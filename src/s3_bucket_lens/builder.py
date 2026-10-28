"""Export a credential-free static site without loading the directory tree into RAM."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from urllib.parse import quote

from .database import connect, metadata, query_dicts, set_metadata, storage_breakdown
from .pricing import load_rates

PAGE_SIZE = 100
SHARD_THRESHOLD = 10000
SHARD_PREFIX = 3
SORTS = {"cost": "child.monthly_usd DESC, child.path", "name": "child.path"}


def prefix_id(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()[:32]


@contextmanager
def report_connection(snapshot, memory_limit, temporary):
    if not snapshot.is_dir():
        with connect(snapshot, memory_limit, read_only=True) as connection:
            yield connection
        return
    summary = json.loads((snapshot / "manifest.json").read_text())
    if not summary.get("complete") or summary.get("catalogue_kind") != "current_objects":
        raise ValueError("Not a complete current-object catalogue.")
    if not summary.get("region"):
        raise ValueError("The catalogue must record its AWS region to estimate storage costs.")
    pricing = load_rates(summary["region"])
    with connect(Path(temporary) / "report.duckdb", memory_limit) as connection:
        connection.execute("SET threads = 1")
        connection.read_parquet(str(snapshot / "objects.parquet")).create_view("current_objects")
        connection.read_parquet(str(snapshot / "folders.parquet")).create_view("current_folders")
        connection.execute(
            "CREATE TABLE standard_rate AS SELECT ?::DOUBLE AS monthly_per_gib",
            [pricing["rates"]["STANDARD"]],
        )
        connection.execute("""
            CREATE TABLE metadata (key VARCHAR PRIMARY KEY, value JSON);
            CREATE VIEW objects AS
            SELECT key, NULL::VARCHAR AS version_id, parent_path, true AS is_latest,
                   false AS is_delete_marker, size_bytes, storage_class, last_modified, etag,
                   size_bytes / 1073741824.0 * monthly_per_gib AS monthly_usd,
                   NULL::TIMESTAMPTZ AS last_accessed
            FROM current_objects CROSS JOIN standard_rate;
            CREATE VIEW prefixes AS
            SELECT path,
                CASE WHEN path = '' THEN NULL ELSE
                    regexp_replace(left(path, length(path) - 1), '[^/]*$', '') END AS parent_path,
                length(path) - length(replace(path, '/', '')) AS depth,
                total_bytes, total_bytes AS current_bytes,
                NULL::HUGEINT AS noncurrent_bytes, file_count AS objects,
                NULL::UBIGINT AS versions, NULL::UBIGINT AS delete_markers,
                total_bytes / 1073741824.0 * monthly_per_gib AS monthly_usd,
                total_bytes / 1073741824.0 * monthly_per_gib AS current_usd,
                NULL::DOUBLE AS noncurrent_usd,
                0::UBIGINT AS unknown_cost_objects, 0::HUGEINT AS unknown_cost_bytes,
                0::UBIGINT AS access_observed_objects, NULL::HUGEINT AS stale_bytes,
                NULL::UBIGINT AS small_objects,
                NULL::TIMESTAMPTZ AS oldest_modified, NULL::TIMESTAMPTZ AS last_modified,
                NULL::TIMESTAMPTZ AS last_accessed,
                NULL::UBIGINT AS multipart_uploads, NULL::HUGEINT AS multipart_bytes,
                NULL::UBIGINT AS multipart_unknown_sizes
            FROM current_folders CROSS JOIN standard_rate;
        """)
        set_metadata(connection, "schema_version", 1)
        set_metadata(connection, "complete", True)
        set_metadata(connection, "captured_at", summary["completed_at"])
        set_metadata(connection, "catalogue_kind", "current_objects")
        set_metadata(
            connection,
            "bucket",
            {
                "name": summary["bucket"],
                "region": summary["region"],
                "prefix_filter": summary["prefix"],
                "demo": False,
            },
        )
        set_metadata(
            connection,
            "pricing",
            {
                **pricing,
                "source": "Estimated current storage at the regional S3 Standard rate",
            },
        )
        warnings = [
            "Current objects only. Historical versions, delete markers and incomplete uploads "
            "were not measured; these estimates do not cover the complete S3 bill.",
            "Costs assume S3 Standard for all measured bytes, regardless of actual storage class.",
            "Last modified is not last accessed. "
            "No access history or lifecycle configuration was collected.",
            "S3 listings are not a point-in-time snapshot; "
            "concurrent bucket changes may affect totals.",
            "Folder modification ranges, stale bytes and small-file counts are not aggregated "
            "in this report; individual modification times remain in the object catalogue.",
        ]
        if summary["scope"] != "bucket":
            warnings.append(
                f"Restricted {summary['scope']} scan; totals do not cover the whole bucket."
            )
        set_metadata(connection, "warnings", warnings)
        yield connection


def build(snapshot: Path, output: Path, *, memory_limit: str = "512MB") -> Path:
    if not snapshot.is_file() and not (snapshot / "manifest.json").is_file():
        raise ValueError(f"Snapshot not found: {snapshot}")
    if output.exists():
        raise FileExistsError(f"Report already exists: {output}. Choose a new --output path.")
    html_output = output if output.suffix.lower() in {".html", ".htm"} else None
    site_output = output.with_suffix(".report") if html_output else output
    if site_output.exists():
        raise FileExistsError(f"Report already exists: {site_output}. Choose a new --output path.")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".s3-lens-site-", dir=output.parent) as temporary:
        site = Path(temporary) / "site"
        site.mkdir(mode=0o700)
        data = site / "data"
        data.mkdir()
        with report_connection(snapshot, memory_limit, temporary) as connection:
            info = metadata(connection)
            if info.get("schema_version") != 1 or not info.get("complete"):
                raise ValueError("Not a complete, supported S3 Bucket Lens snapshot.")
            info["storage_classes"] = storage_breakdown(connection)
            info["root"] = query_dicts(connection, "SELECT * FROM prefixes WHERE path = ''")[0]
            info["prefix_count"] = connection.execute("SELECT count(*) FROM prefixes").fetchone()[0]
            info["page_size"] = PAGE_SIZE
            columns = None
            if info["prefix_count"] > SHARD_THRESHOLD:
                columns = [row[0] for row in connection.execute("DESCRIBE prefixes").fetchall()]
                columns.append("child_count")
                info["directory_columns"] = columns
                info["directory_shard_prefix"] = SHARD_PREFIX
            write_data(site / "manifest.js", info)
            for sort, order in SORTS.items():
                export_directories(connection, data, sort, order, columns=columns)
            connection.execute(
                "COPY (SELECT * FROM prefixes ORDER BY path) TO ? (HEADER, DELIMITER ',')",
                [str(site / "prefixes.csv")],
            )
        assets = files("s3_bucket_lens").joinpath("assets")
        for name in (
            "index.html",
            "dashboard.css",
            "dashboard.js",
            "theme.js",
            "icons.js",
            "lucide-LICENCE.txt",
        ):
            (site / name).write_bytes(assets.joinpath(name).read_bytes())
        os.rename(site, site_output)
    return publish_html(site_output, html_output) if html_output else output / "index.html"


def publish_html(site: Path, output: Path, *, replace: bool = False) -> Path:
    if output.exists() and not replace:
        raise FileExistsError(f"HTML already exists: {output}. Choose a new --output path.")
    output.parent.mkdir(parents=True, exist_ok=True)
    prefix = quote(Path(os.path.relpath(site, output.parent)).as_posix(), safe="/") + "/"
    html = (site / "index.html").read_text(encoding="utf-8")
    html = html.replace("<head>", f'<head>\n  <base href="{prefix}">', 1)
    with tempfile.TemporaryDirectory(prefix=".s3-lens-html-", dir=output.parent) as temporary:
        entry = Path(temporary) / "index.html"
        entry.write_text(html, encoding="utf-8")
        entry.chmod(0o600)
        if replace:
            os.replace(entry, output)
        else:
            os.link(entry, output)
    return output


def build_folders(directory: Path, output: Path, *, bucket: str) -> Path:
    directory, output = Path(directory), Path(output)
    if output.exists() or (directory / "index.html").exists():
        raise FileExistsError("Folder report already exists; choose a new output.")
    database_path = directory / "folders.duckdb"
    if database_path.exists():
        raise FileExistsError(database_path)
    info = json.loads((directory / "benchmark.json").read_text())
    info["bucket"] = bucket
    info["extracted_at"] = datetime.fromtimestamp(
        (directory / "benchmark.json").stat().st_mtime, UTC
    ).isoformat()
    with connect(database_path, "128MB") as database:
        database.execute(
            "CREATE TABLE folders AS SELECT DISTINCT path, parent_path, depth::UINTEGER AS depth, "
            "NULL::UBIGINT AS object_count, NULL::UBIGINT AS total_bytes "
            "FROM read_ndjson(?, columns={path:'VARCHAR', parent_path:'VARCHAR', "
            "depth:'UINTEGER'})",
            [str(directory / "directories.ndjson")],
        )
        info["folder_count"] = database.execute("SELECT count(*) FROM folders").fetchone()[0]
        if info["folder_count"] > 51000:
            raise ValueError("Folder search export is limited to 51,000 discovered prefixes.")
        database.execute(
            "COPY (SELECT * FROM folders ORDER BY path) TO ? (HEADER, DELIMITER ',')",
            [str(directory / "folders.csv")],
        )
        database.execute(
            "COPY (SELECT * FROM folders ORDER BY path) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(directory / "folders.parquet")],
        )
        folders = query_dicts(
            database, "SELECT path, parent_path, depth FROM folders ORDER BY path"
        )
        database.execute("CREATE TABLE metadata (value JSON)")
        database.execute("INSERT INTO metadata VALUES (?)", [json.dumps(info)])
    write_data(directory / "folders.js", {**info, "folders": folders})
    return publish_folder_dashboard(directory, output, bucket=bucket)


def publish_folder_dashboard(directory: Path, output: Path, *, bucket: str, replace=False) -> Path:
    directory, output = Path(directory), Path(output)
    if output.exists() and not replace:
        raise FileExistsError(output)
    with connect(directory / "folders.duckdb", read_only=True) as database:
        info = json.loads(database.execute("SELECT value FROM metadata").fetchone()[0])
    root = {
        "path": "",
        "parent_path": None,
        "total_bytes": None,
        "objects": None,
        "monthly_usd": None,
        "delete_markers": None,
        "last_modified": None,
    }
    write_data(
        directory / "manifest.js",
        {
            "schema_version": 1,
            "folder_only": True,
            "bucket": {"name": bucket, "demo": False},
            "captured_at": info["extracted_at"],
            "root": root,
            "page_size": PAGE_SIZE,
            "folder_count": info["folder_count"],
            "extraction": info,
            "pricing": {"source": "Folder-only extraction; storage charges not measured"},
            "storage_classes": [],
        },
    )
    assets = files("s3_bucket_lens").joinpath("assets")
    for name in (
        "index.html",
        "dashboard.css",
        "dashboard.js",
        "theme.js",
        "icons.js",
        "lucide-LICENCE.txt",
    ):
        (directory / name).write_bytes(assets.joinpath(name).read_bytes())
    return publish_html(directory, output, replace=replace)


@contextmanager
def directory_writer(directory, sort, columns):
    handle = None
    current_shard = None
    first = True

    def write_page(node, children, page, total):
        nonlocal handle, current_shard, first
        if columns is None:
            write_directory_page(directory, node, children, sort, page, total)
            return
        identifier = prefix_id(node[0])
        shard = identifier[:SHARD_PREFIX]
        if shard != current_shard:
            if handle:
                handle.write("}}}));\n")
                handle.close()
            path = directory / f"{shard}-{sort}.js"
            handle = path.open("w", encoding="utf-8")
            handle.write('document.dispatchEvent(new CustomEvent("s3-bucket-lens-data", {"detail":')
            handle.write('{"name":' + json.dumps(path.name) + ',"data":{')
            current_shard, first = shard, True
        if not first:
            handle.write(",\n")
        json.dump(f"{identifier}-{sort}-{page}", handle)
        handle.write(":")
        json.dump(
            {
                "node": node,
                "children": children,
                "page": page,
                "total": total,
                "pages": max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
            },
            handle,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        first = False

    try:
        yield write_page
    finally:
        if handle:
            handle.write("}}}));\n")
            handle.close()


def export_directories(connection, directory: Path, sort: str, order: str, *, columns=None) -> None:
    parent_json = "to_json(parent)"
    child_json = "to_json(child)"
    ordering = "parent.path"
    if columns:
        parent_json = "json_array(" + ", ".join(f'parent."{name}"' for name in columns) + ")"
        child_json = "json_array(" + ", ".join(f'child."{name}"' for name in columns) + ")"
        ordering = f"left(sha256(parent.path), {SHARD_PREFIX}), parent.path"
    cursor = connection.execute(f"""
        WITH counts AS (
            SELECT parent_path, count(*) AS child_count FROM prefixes GROUP BY parent_path
        ),
        nodes AS (
            SELECT p.*, coalesce(counts.child_count, 0) AS child_count FROM prefixes p
            LEFT JOIN counts ON p.path = counts.parent_path
        )
        SELECT {parent_json}, CASE WHEN child.path IS NULL THEN NULL ELSE {child_json} END,
            count(child.path) OVER (PARTITION BY parent.path)
        FROM nodes parent LEFT JOIN nodes child ON parent.path = child.parent_path
        ORDER BY {ordering}, {order}
    """)
    current_path = None
    children = []
    page = 1
    node = None
    total = 0
    with directory_writer(directory, sort, columns) as write_page:
        while batch := cursor.fetchmany(1000):
            for node_json, child_json, child_count in batch:
                next_node = json.loads(node_json)
                next_path = next_node[0] if columns else next_node["path"]
                if next_path != current_path:
                    if node is not None and (children or total == 0):
                        write_page(node, children, page, total)
                    current_path = next_path
                    node, children, page, total = next_node, [], 1, child_count
                if child_json is not None:
                    child = json.loads(child_json)
                    if columns is None:
                        child["id"] = prefix_id(child["path"])
                    children.append(child)
                if len(children) == PAGE_SIZE:
                    write_page(node, children, page, total)
                    children = []
                    page += 1
        if node is not None and (children or total == 0):
            write_page(node, children, page, total)


def write_directory_page(directory, node, children, sort, page, total):
    write_data(
        directory / f"{prefix_id(node['path'])}-{sort}-{page}.js",
        {
            "node": node,
            "children": children,
            "page": page,
            "total": total,
            "pages": max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        },
    )


def write_data(path: Path, value) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write('document.dispatchEvent(new CustomEvent("s3-bucket-lens-data", {"detail":')
        json.dump(
            {"name": path.name, "data": value},
            handle,
            default=str,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        handle.write("}));\n")
