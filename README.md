# S3 Bucket Lens

A cleaner, offline view of S3 folders, storage and estimated costs, by Tom Hepworth.
Amazon already provides the [S3 console](https://console.aws.amazon.com/s3/) and
[S3 Storage Lens](https://aws.amazon.com/s3/storage-lens/). This is a lightweight alternative
for exploring and sharing a local snapshot, not a replacement for AWS administration.

*The bundled demo uses synthetic data only.*

## How It Works

- List metadata from a general purpose bucket using your existing AWS profile or SSO.
- Store it in DuckDB; `catalogue` also exports object metadata and folder totals to Parquet.
- Browse an expandable folder tree with sizes, cost estimates, sorting and CSV export.
- Open the generated HTML directly from disk. No server, AWS credentials or internet
	connection is needed by the dashboard. Share the HTML and its companion `.report` folder together.

## Choose A Scan

| | `catalogue` | `extract` |
| --- | --- | --- |
| Current objects and folder sizes | Yes | Yes |
| Older versions and delete markers | Not measured | Measured for versioned buckets |
| Multipart uploads and bucket configuration | Not measured | Included where permitted |
| Listing | Parallel across prefixes | Serial version listing |
| Resume an interrupted scan | `--resume` | Not supported |
| Remaining-time estimate | Optional total-count hint | Not supported |

**Not measured means unknown, not zero.** Use `extract` to investigate markers or
historical storage; rebuilding a current-object catalogue cannot add missing measurements.

## Scan Current Objects

Requires [uv](https://docs.astral.sh/uv/), Python 3.11+ and an authenticated AWS profile.
For SSO, first run `aws sso login --profile my-profile`. From this checkout:

```sh
uvx --no-cache --from . s3-bucket-lens catalogue my-bucket \
	--profile data --region eu-west-1 --workers 16 --memory-limit 2GB \
	--output bucket.catalogue --report-output bucket.html
```

This scans **current objects**, checkpoints each page and opens the report when finished.
Use the bucket's actual region. Add `--prefix analytics/` to restrict the scan, or
`--no-open` to save without opening a browser. Omit `--report-output` to collect data only.
The package is run from this checkout; `--no-cache` ensures local code changes are rebuilt.

## Resume, Refresh Or Rebuild

| Goal | What to do |
| --- | --- |
| Resume an interrupted scan | Repeat the same `catalogue` command with `--resume`. |
| Refresh after uploads or deletions | Run without `--resume`, using new catalogue and report paths. |
| Rebuild the dashboard only | Use `build` against a completed catalogue or DuckDB snapshot. |
| Reopen an existing report | Open its HTML file directly. |

Keep the hidden `.bucket.catalogue.in-progress` directory when resuming. Completed
catalogues and reports are never overwritten. `--resume` does not refresh finished data.
For example, use `bucket-2026-10-28.catalogue` and `bucket-2026-10-28.html` for a new scan.

Rebuild offline, without contacting S3:

```sh
uvx --no-cache --from . s3-bucket-lens build bucket.catalogue \
	--memory-limit 2GB --output bucket-rebuilt.html
```

A rebuild retains the original snapshot's data; it does not reflect later bucket changes.

## Extraction Estimates

Add **one** of these options to `catalogue` when you have an approximate total:

```text
--expected-objects 5000000
--expected-pages 5000
```

Progress includes a rough remaining **listing** time, based on average observed throughput:

```text
Listed 250,000 objects across 250 pages in 60.0s | ETA ~19m 00s (5% of estimated total; listing only)
```

S3's listing API does not provide a total upfront. Without a hint, the log says
`ETA unavailable (total unknown)`. Totals must match the scanned prefix and current objects;
old counts can be wrong after deletions. Page estimates must include prefix discovery and
short pages, not just objects divided by 1,000. Estimates exclude local finalisation and
report generation. Reaching the estimate never stops the scan; listing continues until complete.

## Run A Full Audit

For an example bucket, using profile `my-profile` and the Ireland region, scan current and
older versions, delete markers, multipart uploads and bucket configuration, then open a report:

```sh
uvx --no-cache --from . s3-bucket-lens extract example-bucket \
	--profile my-profile --region eu-west-1 --memory-limit 2GB \
	--output example-audit.duckdb \
	--report-output example-audit.html
```

To save the snapshot first and build the report separately:

```sh
uvx --no-cache --from . s3-bucket-lens extract example-bucket \
	--profile my-profile --region eu-west-1 --memory-limit 2GB --snapshot-only \
	--output example-snapshot.duckdb
uvx --no-cache --from . s3-bucket-lens build example-snapshot.duckdb \
	--memory-limit 2GB --output example-rebuilt.html
```

Choose unused output names for each run. Add `--prefix example-prefix/` to audit only that
literal prefix, or `--no-open` to save a report without launching a browser.
`--skip-multipart-sizes` skips per-upload part-size requests, not version or marker counts.

**The full audit is read-only, serial and not resumable; large version histories can take
hours.** It does not accept `--workers`, `--resume` or the `--expected-*` ETA options.
Bucket configuration and multipart details can be unavailable if your profile lacks permissions.
Optional `--access-csv` or `--access-logs` imports observed access evidence; S3 listings do
not reveal last-access times. Access-log imports download log contents.

## Try Dummy Data

No AWS credentials are needed:

```sh
uvx --no-cache --from . s3-bucket-lens demo --output demo.duckdb
uvx --no-cache --from . s3-bucket-lens build demo.duckdb --output demo.html
```

## Limits And Safety

- Inventory commands are read-only and do not download object contents. Listing incurs
	AWS request charges, and a changing bucket is not captured as an atomic snapshot.
- Costs are USD storage estimates, not invoices. `catalogue` estimates current bytes at
	regional S3 Standard rates; historical versions, markers and uploads are excluded.
	The fuller audit uses known storage-class rates and flags unpriced data. Request,
	retrieval, transfer, account-wide tiers and discounts are not included.
- Snapshots and reports contain sensitive keys and configuration. Share them privately.
- Write-capable cleanup utilities are separate from the read-only scanner and require
	independent review of their targets and confirmation safeguards.

See the [data model and Python API](docs/data-model.md) for more detail.
Use `s3-bucket-lens <command> --help` for all options.

## Development

```sh
uv sync --locked
uv run --frozen pytest
uv run --frozen ruff check src tests
uv run --frozen ruff format --check src tests
uv build
```