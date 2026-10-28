"""Bounded ingestion and disk-backed directory aggregation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from .pricing import GIB, estimate_storage

OBJECT_TYPES = [
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "BOOLEAN",
    "BOOLEAN",
    "UBIGINT",
    "VARCHAR",
    "TIMESTAMPTZ",
    "VARCHAR",
    "DOUBLE",
    "TIMESTAMPTZ",
]
SUM_FIELDS = [
    "total_bytes",
    "current_bytes",
    "noncurrent_bytes",
    "objects",
    "versions",
    "delete_markers",
    "monthly_usd",
    "current_usd",
    "noncurrent_usd",
    "unknown_cost_objects",
    "unknown_cost_bytes",
    "access_observed_objects",
    "stale_bytes",
    "small_objects",
    "multipart_uploads",
    "multipart_bytes",
    "multipart_unknown_sizes",
]
DATE_FIELDS = {"oldest_modified": "min", "last_modified": "max", "last_accessed": "max"}


def parent_for_key(key: str, max_depth: int) -> str:
    parts = key.split("/")[:-1][:max_depth]
    return "/".join(parts) + "/" if parts else ""


def connect(path: Path, memory_limit: str = "512MB", *, read_only: bool = False):
    return duckdb.connect(
        str(path),
        read_only=read_only,
        config={
            "memory_limit": memory_limit,
            "threads": 2,
            "preserve_insertion_order": False,
        },
    )


def initialise(connection) -> None:
    connection.execute("""
        CREATE TABLE metadata (key VARCHAR PRIMARY KEY, value JSON);
        CREATE TABLE objects (
            key VARCHAR, version_id VARCHAR, parent_path VARCHAR, is_latest BOOLEAN,
            is_delete_marker BOOLEAN, size_bytes UBIGINT, storage_class VARCHAR,
            last_modified TIMESTAMPTZ, etag VARCHAR, monthly_usd DOUBLE,
            last_accessed TIMESTAMPTZ
        );
        CREATE TABLE multipart_uploads (
            key VARCHAR, upload_id VARCHAR, parent_path VARCHAR,
            initiated TIMESTAMPTZ, storage_class VARCHAR, size_bytes UBIGINT
        );
    """)


def set_metadata(connection, key: str, value: Any) -> None:
    connection.execute(
        "INSERT OR REPLACE INTO metadata VALUES (?, ?)", [key, json.dumps(value, default=str)]
    )


def metadata(connection) -> dict[str, Any]:
    return {
        key: json.loads(value)
        for key, value in connection.execute("SELECT key, value FROM metadata").fetchall()
    }


def insert_objects(
    connection,
    items: Iterable[Mapping[str, Any]],
    rates: Mapping[str, float],
    max_depth: int,
    *,
    delete_markers: bool = False,
) -> int:
    rows = []
    total_count = 0
    for item in items:
        total_count += 1
        key = item["Key"]
        size = len(key.encode("utf-8")) if delete_markers else int(item.get("Size", 0))
        storage_class = "STANDARD" if delete_markers else item.get("StorageClass", "STANDARD")
        cost = estimate_storage(size, storage_class, rates)
        rows.append(
            (
                key,
                item.get("VersionId"),
                parent_for_key(key, max_depth),
                item.get("IsLatest", True),
                delete_markers,
                size,
                storage_class,
                item.get("LastModified"),
                item.get("ETag"),
                cost,
                None,
            )
        )
        if len(rows) == 1000:
            _insert_batch(connection, rows)
            rows.clear()
    if rows:
        _insert_batch(connection, rows)
    return total_count


def _insert_batch(connection, rows) -> None:
    expressions = ", ".join(f"unnest(?::{column_type}[])" for column_type in OBJECT_TYPES)
    connection.execute(f"INSERT INTO objects SELECT {expressions}", list(zip(*rows, strict=True)))


def import_access_csv(connection, path: Path, bucket: str) -> None:
    connection.execute(
        """
        UPDATE objects SET last_accessed = evidence.last_accessed
        FROM (
            SELECT key, max(CAST(last_accessed AS TIMESTAMPTZ)) AS last_accessed
            FROM read_csv(?, header=true, all_varchar=true)
            WHERE bucket = ? GROUP BY key
        ) evidence
        WHERE objects.key = evidence.key AND objects.is_latest
            AND NOT objects.is_delete_marker
    """,
        [str(path), bucket],
    )


def aggregate(connection, *, now: datetime | None = None) -> None:
    now = now or datetime.now(UTC)
    connection.execute("CREATE TABLE snapshot_clock AS SELECT ?::TIMESTAMPTZ AS captured_at", [now])
    connection.execute("""
        CREATE TEMP TABLE direct AS
        SELECT parent_path AS path,
            coalesce(sum(size_bytes) FILTER (WHERE NOT is_delete_marker), 0)::HUGEINT
                AS total_bytes,
            coalesce(sum(size_bytes) FILTER (WHERE is_latest AND NOT is_delete_marker), 0)
                AS current_bytes,
            coalesce(sum(size_bytes) FILTER (WHERE NOT is_latest AND NOT is_delete_marker), 0)
                AS noncurrent_bytes,
            count(*) FILTER (WHERE is_latest AND NOT is_delete_marker) AS objects,
            count(*) FILTER (WHERE NOT is_delete_marker) AS versions,
            count(*) FILTER (WHERE is_delete_marker) AS delete_markers,
            coalesce(sum(monthly_usd), 0) AS monthly_usd,
            coalesce(sum(monthly_usd) FILTER (WHERE is_latest AND NOT is_delete_marker), 0)
                AS current_usd,
            coalesce(sum(monthly_usd) FILTER (WHERE NOT is_latest AND NOT is_delete_marker), 0)
                AS noncurrent_usd,
            count(*) FILTER (WHERE monthly_usd IS NULL) AS unknown_cost_objects,
            coalesce(sum(size_bytes) FILTER (WHERE monthly_usd IS NULL), 0) AS unknown_cost_bytes,
            count(*) FILTER (WHERE last_accessed IS NOT NULL) AS access_observed_objects,
            coalesce(sum(size_bytes) FILTER (WHERE NOT is_delete_marker AND last_modified <
                (SELECT captured_at FROM snapshot_clock) - INTERVAL '90 days'), 0) AS stale_bytes,
            count(*) FILTER (WHERE NOT is_delete_marker AND size_bytes < 131072) AS small_objects,
            min(last_modified) FILTER (WHERE NOT is_delete_marker) AS oldest_modified,
            max(last_modified) FILTER (WHERE NOT is_delete_marker) AS last_modified,
            max(last_accessed) AS last_accessed
        FROM objects GROUP BY parent_path;
        CREATE TEMP TABLE uploads AS
        SELECT parent_path AS path, count(*) AS multipart_uploads,
            coalesce(sum(size_bytes), 0) AS multipart_bytes,
            count(*) FILTER (WHERE size_bytes IS NULL) AS multipart_unknown_sizes
        FROM multipart_uploads GROUP BY parent_path;
        CREATE TEMP TABLE paths AS
        SELECT '' AS path UNION
        SELECT array_to_string(list_slice(string_split(path, '/'), 1, level), '/') || '/'
        FROM (SELECT path FROM direct UNION SELECT path FROM uploads) parents,
            unnest(range(1, len(string_split(path, '/')))) levels(level);
    """)
    object_fields = [field for field in SUM_FIELDS if not field.startswith("multipart_")]
    columns = [f"coalesce(d.{field}, 0) AS {field}" for field in object_fields]
    columns += [
        f"coalesce(u.{field}, 0) AS {field}"
        for field in SUM_FIELDS
        if field.startswith("multipart_")
    ]
    columns += [f"d.{field}" for field in DATE_FIELDS]
    connection.execute(f"""
        CREATE TABLE prefixes AS SELECT p.path,
            CASE WHEN p.path = '' THEN NULL
                ELSE regexp_replace(p.path, '[^/]*/$', '') END AS parent_path,
            len(p.path) - len(replace(p.path, '/', '')) AS depth,
            {", ".join(columns)}
        FROM paths p LEFT JOIN direct d ON p.path = d.path LEFT JOIN uploads u ON p.path = u.path
    """)
    maximum = connection.execute("SELECT coalesce(max(depth), 0) FROM prefixes").fetchone()[0]
    child_columns = [f"sum({field}) AS {field}" for field in SUM_FIELDS]
    child_columns += [
        f"{operation}({field}) AS {field}" for field, operation in DATE_FIELDS.items()
    ]
    updates = [f"{field} = p.{field} + c.{field}" for field in SUM_FIELDS]
    updates += [
        f"{field} = {'least' if operation == 'min' else 'greatest'}(p.{field}, c.{field})"
        for field, operation in DATE_FIELDS.items()
    ]
    for depth in range(maximum, 0, -1):
        connection.execute(
            f"""
            UPDATE prefixes p SET {", ".join(updates)} FROM (
                SELECT parent_path, {", ".join(child_columns)} FROM prefixes
                WHERE depth = ? GROUP BY parent_path
            ) c WHERE p.path = c.parent_path
        """,
            [depth],
        )
    connection.execute("CREATE INDEX prefix_parent ON prefixes(parent_path)")
    set_metadata(connection, "schema_version", 1)
    set_metadata(connection, "captured_at", now.isoformat())
    set_metadata(connection, "complete", True)
    connection.execute("CHECKPOINT")


def storage_breakdown(connection) -> list[dict[str, Any]]:
    return query_dicts(
        connection,
        """
        SELECT storage_class, sum(size_bytes)::HUGEINT AS total_bytes, count(*) AS versions,
            coalesce(sum(monthly_usd), 0) AS monthly_usd,
            count(*) FILTER (WHERE monthly_usd IS NULL) AS unknown_cost_objects
        FROM objects WHERE NOT is_delete_marker GROUP BY storage_class ORDER BY total_bytes DESC
    """,
    )


def query_dicts(connection, sql: str, parameters=None) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, parameters or [])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def delete_marker_monthly_cost(key: str, rates: Mapping[str, float]) -> float | None:
    return len(key.encode("utf-8")) / GIB * rates["STANDARD"] if "STANDARD" in rates else None
