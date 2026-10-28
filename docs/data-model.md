# Data Model

## Snapshot Contract

Schema version 1 contains four permanent data tables and a capture-time table:

| Table | Purpose |
| --- | --- |
| `objects` | One record per listed object version or delete marker |
| `multipart_uploads` | One record per unfinished upload; unknown sizes are NULL |
| `prefixes` | Recursive subtree totals, including the root path `''` |
| `metadata` | JSON values keyed by schema version, bucket, prices, scope, warnings and provenance |
| `snapshot_clock` | Timestamp used for reproducible modification-age calculations |

The metadata `complete` flag is written only after successful aggregation. Extraction
uses a private staging directory and atomically publishes a closed database. Failed
extractions leave no final snapshot. Existing destinations are refused.

## Object Records

`key`, `version_id`, `parent_path`, `is_latest`, `is_delete_marker`, `size_bytes`,
`storage_class`, `last_modified`, `etag`, `monthly_usd` and `last_accessed`.

`parent_path` is capped at the configured roll-up depth. `key` is always the full S3
key, with leading/repeated slashes and special characters preserved. Folder markers
are ordinary objects. A current delete marker does not count as a current object;
its historical object versions remain stored and billable.

Delete-marker `size_bytes` holds the UTF-8 key length for billing. Its storage class
is Standard. Prefix `total_bytes` deliberately excludes these marker bytes, so use
`is_delete_marker` when reconciling raw table sizes. Cost includes marker storage.
The marker is retained even when it is not the latest version.

`monthly_usd` is NULL when the class cannot be priced safely. Do not turn this into
zero when reporting cost completeness. `last_accessed` is the latest supplied successful
GET observation for a key, attached only to its current object record. It does not
prove a specific version was accessed or that no later access occurred.

## Directory Totals

`path`, `parent_path` and `depth` identify a directory. The root has a NULL parent.
`total_bytes`, `current_bytes`, `noncurrent_bytes`, `objects`, `versions` and
`delete_markers` describe its entire subtree, not just direct children.

`monthly_usd`, `current_usd` and `noncurrent_usd` are priced subtotals. Their difference
includes delete-marker costs. `unknown_cost_objects` and `unknown_cost_bytes` measure
unpriced records. `access_observed_objects` counts current keys with access evidence.
`stale_bytes` is stored data last modified more than 90 days before capture, not unread data.
`small_objects` counts non-marker versions below 128 KiB. `oldest_modified`,
`last_modified` and `last_accessed` summarise timestamps. `multipart_uploads`,
`multipart_bytes` and `multipart_unknown_sizes` describe incomplete uploads separately.

Never sum every prefix row to calculate a bucket total: parent rows already include
their descendants. Use the root or select a non-overlapping set of siblings. Root
objects are included in root totals even when no child folders exist.

## Python And SQL

```python
from pathlib import Path

from s3_bucket_lens.inventory import extract
from s3_bucket_lens.builder import build

extract("my-bucket", Path("bucket.duckdb"), profile="my-sso-profile")
build(Path("bucket.duckdb"), Path("report"))
```

```python
import duckdb

with duckdb.connect("bucket.duckdb", read_only=True) as database:
    rows = database.execute("""
        SELECT path, noncurrent_bytes, noncurrent_usd, unknown_cost_objects
        FROM prefixes WHERE depth = 1 ORDER BY noncurrent_usd DESC
    """).fetchall()
    database.execute("""
        COPY objects TO 'objects.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
```

Parquet is a useful compressed interchange format for other analytical tools. The
primary database stays DuckDB to preserve several related tables and fast local SQL.
Use `fetchmany()` rather than `fetchall()` for unbounded object-level queries.

Useful investigations include noncurrent retention, storage-class mix, incomplete
uploads, age distributions and minimum-billable-size overhead. ETag comparisons are
only candidate matches; multipart uploads and encryption can change their meaning.
An old modification date or a missing access observation is not permission to delete data.

## Static Site

`manifest.js` contains bucket metadata, root totals, prices, warnings and storage-class
summaries. `data/<sha256-prefix>-<sort>-<page>.js` contains a single directory and at
most 100 children, with child counts for lazy tree expansion. Each local script dispatches
a `s3-bucket-lens-data` event carrying its filename and JSON-encoded payload. This avoids
fetching local JSON, so `index.html` works directly over `file://` without a server.
Loaded script elements and event listeners are removed after delivery.

Paths are hashed for filenames to prevent S3 keys becoming local paths.
Key text is escaped before HTML rendering. The browser never evaluates SQL or
loads an object-level inventory. Cost and name orderings are generated in DuckDB and
written using a bounded cursor; very large sorts may spill to disk.

The export also contains `prefixes.csv`, HTML, CSS, JavaScript, persisted colour-theme
controls and the vendored Lucide licence. Keep these files and the `data` directory
together. The report is entirely static and can also be hosted privately.