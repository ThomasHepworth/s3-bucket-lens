"""Read-only current-object listings with bounded, disk-backed prefix scheduling."""

from __future__ import annotations

import json
import resource
import sys
import tempfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import boto3
from botocore.config import Config

from .database import connect
from .inventory import retry_clock_skew

OBJECT_STRUCTURE = json.dumps(
    [
        {
            "Key": "VARCHAR",
            "Size": "UBIGINT",
            "LastModified": "TIMESTAMPTZ",
            "StorageClass": "VARCHAR",
            "ETag": "VARCHAR",
        }
    ]
)


def listing_client(*, profile=None, region=None, workers=8):
    client = boto3.Session(profile_name=profile, region_name=region).client(
        "s3",
        config=Config(
            max_pool_connections=max(10, workers),
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "adaptive", "max_attempts": 5},
        ),
    )
    client.meta.events.register("needs-retry.s3", retry_clock_skew)
    return client


def _fetch_page(client, bucket, task):
    prefix, token, delimiter, depth = task
    arguments = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
    if delimiter:
        arguments["Delimiter"] = "/"
    if token:
        arguments["ContinuationToken"] = token
    started = perf_counter()
    page = client.list_objects_v2(**arguments)
    return page, perf_counter() - started


def _insert_page(database, items):
    if not items:
        return
    payload = json.dumps(items, default=str, ensure_ascii=True, allow_nan=False)
    database.execute(
        """
        INSERT INTO objects
        SELECT record.Key, regexp_extract(record.Key, '[^/]*$', 0),
               regexp_replace(record.Key, '[^/]*$', ''), record.Size,
               record.LastModified, coalesce(record.StorageClass, 'STANDARD'),
               record.ETag, ends_with(record.Key, '/') AND record.Size = 0
        FROM unnest(from_json(?::JSON, ?)) AS entries(record)
        """,
        [payload, OBJECT_STRUCTURE],
    )


def _aggregate_folders(database):
    database.execute("""
        CREATE OR REPLACE TABLE direct_folders AS
        SELECT parent_path AS path, count(*) AS object_count,
               count(*) FILTER (WHERE NOT is_folder_marker) AS file_count,
               sum(size_bytes)::HUGEINT AS total_bytes
        FROM objects GROUP BY parent_path;
        INSERT INTO direct_folders
        SELECT '', 0, 0, 0 WHERE NOT EXISTS (
            SELECT 1 FROM direct_folders WHERE path = ''
        );
        CREATE OR REPLACE TABLE folders AS
        WITH RECURSIVE ancestors AS (
            SELECT * FROM direct_folders
            UNION ALL
            SELECT regexp_replace(left(path, length(path) - 1), '[^/]*$', ''),
                   object_count, file_count, total_bytes
            FROM ancestors WHERE path <> ''
        )
        SELECT path, sum(object_count)::UBIGINT AS object_count,
               sum(file_count)::UBIGINT AS file_count,
               sum(total_bytes)::HUGEINT AS total_bytes
        FROM ancestors GROUP BY path;
        DROP TABLE direct_folders;
    """)


@contextmanager
def _staging_directory(output, checkpoint, resume):
    if not checkpoint:
        with tempfile.TemporaryDirectory(prefix=".s3-catalogue-", dir=output.parent) as temporary:
            yield Path(temporary)
        return
    staging = output.parent / f".{output.name}.in-progress"
    if resume:
        if not (staging / "catalogue.duckdb").is_file():
            raise FileNotFoundError(f"No checkpoint database in {staging}")
    else:
        try:
            staging.mkdir(mode=0o700)
        except FileExistsError:
            raise FileExistsError(f"Checkpoint already exists: {staging}; use --resume") from None
    yield staging


def catalogue(
    bucket,
    output,
    *,
    prefix="",
    workers=16,
    strategy="prefix",
    profile=None,
    region=None,
    memory_limit="256MB",
    max_split_depth=8,
    end_key=None,
    client=None,
    progress=None,
    max_requests=None,
    max_seconds=None,
    allow_partial=False,
    checkpoint=False,
    resume=False,
):
    """Persist current metadata, folders, Parquet and a completion manifest locally.

    Limits are for explicitly partial benchmarks, not complete catalogues. Prefix
    partitioning never parallelises a continuation-token chain. Folder markers
    remain real zero-byte objects; deleted and noncurrent versions are excluded.
    Checkpoints retain the DuckDB WAL and page journal on failure. Resume checks
    the scan configuration before reclaiming unfinished tasks.
    """
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    if not bucket or bucket.endswith("--x-s3") or "/" in bucket or ":" in bucket:
        raise ValueError("a general purpose S3 bucket name is required")
    if not 1 <= workers <= 64 or not 0 <= max_split_depth <= 256:
        raise ValueError("workers must be 1-64 and max_split_depth must be 0-256")
    if strategy not in {"flat", "prefix", "hybrid"}:
        raise ValueError("strategy must be flat, prefix or hybrid")
    if strategy == "flat" and workers != 1:
        raise ValueError("flat listing requires one worker")
    if (max_requests is not None or max_seconds is not None) and not allow_partial:
        raise ValueError("benchmark limits require allow_partial=True")
    if max_requests is not None and max_requests < 1:
        raise ValueError("max_requests must be positive")
    if max_seconds is not None and max_seconds <= 0:
        raise ValueError("max_seconds must be positive")
    checkpoint = checkpoint or resume
    if checkpoint and allow_partial:
        raise ValueError("resumable checkpoints cannot be used for partial benchmarks")
    owned_client = client is None
    client = client or listing_client(profile=profile, region=region, workers=workers)
    configured_region = getattr(getattr(client, "meta", None), "region_name", None)
    bucket_region = region or (configured_region if isinstance(configured_region, str) else None)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = perf_counter()
    statistics = {
        "bucket": bucket,
        "region": bucket_region,
        "prefix": prefix,
        "strategy": strategy,
        "workers": workers,
        "catalogue_kind": "current_objects",
        "end_key": end_key,
        "scope": "key_range" if end_key is not None else "prefix" if prefix else "bucket",
        "started_at": datetime.now(UTC).isoformat(),
        "pages": 0,
        "api_requests": 0,
        "retries": 0,
        "objects": 0,
        "total_bytes": 0,
        "request_seconds": 0.0,
        "write_seconds": 0.0,
        "discovered_prefixes": 0,
        "resumptions": 0,
        "checkpointed": checkpoint,
    }
    configuration = {
        "format_version": 1,
        "bucket": bucket,
        "prefix": prefix,
        "strategy": strategy,
        "region": bucket_region,
        "end_key": end_key,
        "max_split_depth": max_split_depth,
    }
    latencies = []
    elapsed_before_resume = 0.0
    try:
        with _staging_directory(output, checkpoint, resume) as staging:
            with connect(staging / "catalogue.duckdb", memory_limit) as database:
                if resume:
                    saved_configuration, saved_statistics = database.execute(
                        "SELECT configuration, statistics FROM scan_state"
                    ).fetchone()
                    if json.loads(saved_configuration) != configuration:
                        raise ValueError("checkpoint settings do not match this scan")
                    statistics = json.loads(saved_statistics)
                    elapsed_before_resume = statistics.get("listing_seconds", 0.0)
                    statistics["resumptions"] += 1
                    statistics["workers"] = workers
                    latencies = [
                        row[0]
                        for row in database.execute(
                            "SELECT request_seconds FROM processed_pages "
                            "ORDER BY committed_at LIMIT 4096"
                        ).fetchall()
                    ]
                    database.execute("UPDATE listing_tasks SET claimed = false")
                else:
                    database.execute("""
                    BEGIN;
                    CREATE TABLE objects (
                        key VARCHAR, file_name VARCHAR, parent_path VARCHAR,
                        size_bytes UBIGINT, last_modified TIMESTAMPTZ,
                        storage_class VARCHAR, etag VARCHAR, is_folder_marker BOOLEAN
                    );
                    CREATE TABLE listing_tasks (
                        prefix VARCHAR PRIMARY KEY, token VARCHAR,
                        delimiter BOOLEAN, depth UINTEGER, claimed BOOLEAN DEFAULT false
                    );
                    CREATE TABLE scan_state (configuration JSON, statistics JSON);
                    CREATE TABLE processed_pages (
                        prefix VARCHAR, request_token VARCHAR, next_token VARCHAR,
                        delimiter BOOLEAN, depth UINTEGER, object_count UBIGINT,
                        total_bytes HUGEINT, request_seconds DOUBLE, retries UINTEGER,
                        committed_at TIMESTAMPTZ,
                        PRIMARY KEY (prefix, request_token)
                    );
                    """)
                    database.execute(
                        "INSERT INTO listing_tasks VALUES (?, NULL, ?, 0, false)",
                        [prefix, strategy != "flat"],
                    )
                    database.execute(
                        "INSERT INTO scan_state VALUES (?, ?)",
                        [json.dumps(configuration), json.dumps(statistics)],
                    )
                    database.execute("COMMIT")
                submitted = 0
                pending = {}
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    while True:
                        within_budget = (
                            max_seconds is None or perf_counter() - started < max_seconds
                        )
                        available = workers - len(pending)
                        if max_requests is not None:
                            available = min(available, max_requests - submitted)
                        if within_budget and available > 0:
                            tasks = database.execute(
                                "SELECT prefix, token, delimiter, depth FROM listing_tasks "
                                "WHERE NOT claimed ORDER BY delimiter, depth DESC LIMIT ?",
                                [available],
                            ).fetchall()
                            for task in tasks:
                                database.execute(
                                    "UPDATE listing_tasks SET claimed = true WHERE prefix = ?",
                                    [task[0]],
                                )
                                pending[executor.submit(_fetch_page, client, bucket, task)] = task
                                submitted += 1
                        if not pending:
                            break
                        finished, _ = wait(pending, return_when=FIRST_COMPLETED)
                        for future in finished:
                            task = pending.pop(future)
                            page, latency = future.result()
                            task_prefix, token, delimiter, depth = task
                            if end_key is not None:
                                entries = page.get("Contents", [])
                                branches = page.get("CommonPrefixes", [])
                                reached_end = any(
                                    item["Key"] >= end_key for item in entries
                                ) or any(child["Prefix"] > end_key for child in branches)
                                page["Contents"] = [
                                    item for item in entries if item["Key"] <= end_key
                                ]
                                page["CommonPrefixes"] = [
                                    child for child in branches if child["Prefix"] <= end_key
                                ]
                                if reached_end:
                                    page["IsTruncated"] = False
                            next_token = page.get("NextContinuationToken")
                            if page.get("IsTruncated") and (not next_token or next_token == token):
                                raise RuntimeError(
                                    "S3 returned a missing or repeated continuation token"
                                )
                            items = page.get("Contents", [])
                            children = page.get("CommonPrefixes", [])
                            write_started = perf_counter()
                            database.execute("BEGIN")
                            try:
                                _insert_page(database, items)
                                if page.get("IsTruncated"):
                                    database.execute(
                                        "UPDATE listing_tasks SET token = ?, claimed = false "
                                        "WHERE prefix = ?",
                                        [next_token, task_prefix],
                                    )
                                else:
                                    database.execute(
                                        "DELETE FROM listing_tasks WHERE prefix = ?", [task_prefix]
                                    )
                                if children:
                                    if not delimiter or any(
                                        not child["Prefix"].startswith(task_prefix)
                                        or len(child["Prefix"]) <= len(task_prefix)
                                        or not child["Prefix"].endswith("/")
                                        for child in children
                                    ):
                                        raise RuntimeError("S3 returned an invalid child prefix")
                                    task_count = database.execute(
                                        "SELECT count(*) FROM listing_tasks"
                                    ).fetchone()[0]
                                    split = (
                                        strategy == "hybrid"
                                        and depth + 1 < max_split_depth
                                        and task_count < workers * 4
                                    )
                                    database.execute(
                                        "INSERT INTO listing_tasks "
                                        "SELECT child.Prefix, NULL, ?, ?, false "
                                        "FROM unnest(from_json(?::JSON, "
                                        '\'[ {"Prefix":"VARCHAR"} ]\')) AS entries(child)',
                                        [split, depth + 1, json.dumps(children)],
                                    )
                                retries = page.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                                page_bytes = sum(item["Size"] for item in items)
                                database.execute(
                                    "INSERT INTO processed_pages VALUES "
                                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
                                    [
                                        task_prefix,
                                        token or "",
                                        next_token if page.get("IsTruncated") else None,
                                        delimiter,
                                        depth,
                                        len(items),
                                        page_bytes,
                                        latency,
                                        retries,
                                    ],
                                )
                                updated = dict(statistics)
                                updated["pages"] += 1
                                updated["api_requests"] += 1 + retries
                                updated["retries"] += retries
                                updated["objects"] += len(items)
                                updated["total_bytes"] += page_bytes
                                updated["request_seconds"] += latency
                                updated["write_seconds"] += perf_counter() - write_started
                                updated["discovered_prefixes"] += len(children)
                                updated["listing_seconds"] = (
                                    elapsed_before_resume + perf_counter() - started
                                )
                                database.execute(
                                    "UPDATE scan_state SET statistics = ?", [json.dumps(updated)]
                                )
                                database.execute("COMMIT")
                            except BaseException:
                                database.execute("ROLLBACK")
                                raise
                            statistics = updated
                            if len(latencies) < 4096:
                                latencies.append(latency)
                            if progress:
                                progress(
                                    {
                                        **statistics,
                                        "elapsed_seconds": elapsed_before_resume
                                        + perf_counter()
                                        - started,
                                    }
                                )
                statistics["complete"] = (
                    database.execute("SELECT count(*) FROM listing_tasks").fetchone()[0] == 0
                )
                statistics["listing_seconds"] = elapsed_before_resume + perf_counter() - started
                statistics["objects_per_second"] = (
                    statistics["objects"] / statistics["listing_seconds"]
                )
                statistics["pages_per_second"] = statistics["pages"] / statistics["listing_seconds"]
                latencies.sort()
                statistics["request_mean_seconds"] = statistics["request_seconds"] / max(
                    statistics["pages"], 1
                )
                statistics["request_p95_seconds"] = (
                    latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
                    if latencies
                    else None
                )
                validation_started = perf_counter()
                count, distinct, total_bytes = database.execute(
                    "SELECT count(*), count(DISTINCT key), coalesce(sum(size_bytes), 0) "
                    "FROM objects"
                ).fetchone()
                if count != distinct or count != statistics["objects"]:
                    raise RuntimeError("catalogue validation found duplicate or inconsistent keys")
                if total_bytes != statistics["total_bytes"]:
                    raise RuntimeError("catalogue validation found inconsistent byte totals")
                statistics["unique_keys"] = distinct
                statistics["validation_seconds"] = perf_counter() - validation_started
                _aggregate_folders(database)
                database.execute(
                    "COPY objects TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                    [str(staging / "objects.parquet")],
                )
                database.execute(
                    "COPY folders TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                    [str(staging / "folders.parquet")],
                )
            statistics["total_seconds"] = elapsed_before_resume + perf_counter() - started
            statistics["completed_at"] = datetime.now(UTC).isoformat()
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            statistics["peak_rss_mib"] = peak / (1024**2 if sys.platform == "darwin" else 1024)
            statistics["output"] = str(output)
            (staging / "manifest.json").write_text(json.dumps(statistics, indent=2) + "\n")
            staging.rename(output)
    finally:
        if owned_client:
            client.close()
    return statistics
