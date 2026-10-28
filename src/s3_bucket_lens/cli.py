"""Small command-line interface for uvx."""

import argparse
import json
import sys
import webbrowser
from functools import partial
from pathlib import Path

from .builder import build
from .catalogue import catalogue
from .demo import create_demo
from .inventory import extract


def _positive_count(value: str) -> int:
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return count


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Extract S3 metadata once, explore it offline.")
    commands = root.add_subparsers(dest="command", required=True)
    scan = commands.add_parser("extract", help="Download S3 metadata to a DuckDB snapshot")
    bucket = scan.add_mutually_exclusive_group()
    bucket.add_argument(
        "bucket",
        nargs="?",
        default="example-bucket",
        help="General purpose S3 bucket name (default: example-bucket)",
    )
    bucket.add_argument("--bucket", dest="selected_bucket", help="General purpose S3 bucket name")
    scan.add_argument("--output", "-o", type=Path, default=Path("snapshot.duckdb"))
    scan.add_argument(
        "--report-output",
        type=Path,
        help="HTML file or report directory (default: <snapshot>.html)",
    )
    scan.add_argument("--profile", help="AWS profile, including an authenticated SSO profile")
    scan.add_argument(
        "--region", default="eu-west-2", help="Bucket and pricing region (default: eu-west-2)"
    )
    scan.add_argument("--prefix", default="", help="Restrict extraction to this exact key prefix")
    scan.add_argument(
        "--max-depth",
        type=int,
        default=16,
        help="Directory roll-up depth; all object metadata is retained (default: 16)",
    )
    scan.add_argument("--pricing-file", type=Path, help="Custom regional USD storage rates as JSON")
    evidence = scan.add_mutually_exclusive_group()
    evidence.add_argument(
        "--access-csv", type=Path, help="Observed GET evidence: bucket,key,last_accessed"
    )
    evidence.add_argument(
        "--access-logs", help="Stream existing server access logs: s3://bucket/prefix"
    )
    scan.add_argument(
        "--skip-multipart-sizes",
        action="store_true",
        help="Skip ListParts requests; upload sizes remain explicitly unknown",
    )
    scan.add_argument("--memory-limit", default="512MB", help="DuckDB query-memory limit")
    scan.add_argument(
        "--snapshot-only", action="store_true", help="Save only DuckDB, without an HTML report"
    )
    scan.add_argument("--no-open", action="store_true", help="Do not open the HTML report")
    export = commands.add_parser("build", help="Create and open a static HTML report")
    export.add_argument("snapshot", type=Path)
    export.add_argument("--output", "-o", type=Path, default=Path("s3-bucket-lens.html"))
    export.add_argument("--memory-limit", default="512MB")
    export.add_argument("--no-open", action="store_true", help="Do not open the HTML report")
    demo = commands.add_parser("demo", help="Create a clearly labelled synthetic demo snapshot")
    demo.add_argument("--output", "-o", type=Path, default=Path("demo.duckdb"))
    current = commands.add_parser("catalogue", help="List current objects to DuckDB and Parquet")
    current.add_argument("bucket", help="General purpose S3 bucket name")
    current.add_argument("--output", "-o", type=Path, required=True, help="New output directory")
    current.add_argument("--prefix", default="")
    current.add_argument("--profile")
    current.add_argument(
        "--region", help="Bucket region; otherwise use the AWS credential configuration"
    )
    current.add_argument("--workers", type=int, default=16)
    current.add_argument("--strategy", choices=["prefix", "hybrid", "flat"], default="prefix")
    current.add_argument("--memory-limit", default="256MB")
    current.add_argument("--max-split-depth", type=int, default=8)
    current.add_argument(
        "--resume", action="store_true", help="Continue a retained page checkpoint"
    )
    estimates = current.add_mutually_exclusive_group()
    estimates.add_argument(
        "--expected-objects", type=_positive_count, help="Approximate current-object total for ETA"
    )
    estimates.add_argument(
        "--expected-pages", type=_positive_count, help="Approximate listing-page total for ETA"
    )
    current.add_argument(
        "--report-output", type=Path, help="Build an offline HTML report afterwards"
    )
    current.add_argument("--no-open", action="store_true", help="Do not open the HTML report")
    return root


def main(argv=None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if arguments.command == "extract":
            result = extract(
                arguments.selected_bucket or arguments.bucket,
                arguments.output,
                profile=arguments.profile,
                region=arguments.region,
                prefix=arguments.prefix,
                max_depth=arguments.max_depth,
                pricing_file=arguments.pricing_file,
                access_csv=arguments.access_csv,
                access_logs=arguments.access_logs,
                multipart_sizes=not arguments.skip_multipart_sizes,
                memory_limit=arguments.memory_limit,
            )
            print(f"Snapshot saved: {result}")
            if not arguments.snapshot_only:
                report = build(
                    result,
                    arguments.report_output or result.with_suffix(".html"),
                    memory_limit=arguments.memory_limit,
                )
                open_report(report, no_open=arguments.no_open)
        elif arguments.command == "demo":
            print(f"Synthetic demo saved: {create_demo(arguments.output)}")
        elif arguments.command == "build":
            result = build(
                arguments.snapshot, arguments.output, memory_limit=arguments.memory_limit
            )
            open_report(result, no_open=arguments.no_open)
        elif arguments.command == "catalogue":
            result = catalogue(
                arguments.bucket,
                arguments.output,
                prefix=arguments.prefix,
                profile=arguments.profile,
                region=arguments.region,
                workers=arguments.workers,
                strategy=arguments.strategy,
                memory_limit=arguments.memory_limit,
                max_split_depth=arguments.max_split_depth,
                progress=partial(
                    catalogue_progress,
                    expected_objects=arguments.expected_objects,
                    expected_pages=arguments.expected_pages,
                ),
                checkpoint=True,
                resume=arguments.resume,
            )
            print(json.dumps(result, indent=2))
            if arguments.report_output:
                report = build(
                    arguments.output, arguments.report_output, memory_limit=arguments.memory_limit
                )
                open_report(report, no_open=arguments.no_open)
    except (Exception, KeyboardInterrupt) as exc:
        if arguments.command == "catalogue":
            checkpoint = arguments.output.parent / f".{arguments.output.name}.in-progress"
            if (checkpoint / "catalogue.duckdb").is_file():
                print(
                    f"Checkpoint retained: {checkpoint}. Repeat the command with --resume.",
                    file=sys.stderr,
                )
        if isinstance(exc, KeyboardInterrupt):
            return 130
        print(f"s3-bucket-lens: {exc}", file=sys.stderr)
        return 1
    return 0


def catalogue_progress(statistics, *, expected_objects=None, expected_pages=None):
    if statistics["pages"] == 1 or statistics["pages"] % 10 == 0:
        total = expected_objects if expected_objects is not None else expected_pages
        completed = statistics["objects"] if expected_objects is not None else statistics["pages"]
        elapsed = statistics["elapsed_seconds"]
        if total is None:
            estimate = "ETA unavailable (total unknown)"
        elif completed >= total:
            estimate = "ETA unavailable (estimated total reached; listing continues)"
        elif completed == 0 or elapsed <= 0:
            estimate = "ETA unavailable (collecting throughput)"
        else:
            remaining = max(1, round((total - completed) * elapsed / completed))
            hours, remaining = divmod(remaining, 3600)
            minutes, seconds = divmod(remaining, 60)
            duration = f"{hours}h {minutes:02d}m" if hours else f"{minutes}m {seconds:02d}s"
            estimate = f"ETA ~{duration} ({completed / total:.0%} of estimated total; listing only)"
        print(
            f"Listed {statistics['objects']:,} objects across {statistics['pages']:,} pages "
            f"in {elapsed:.1f}s | {estimate}",
            file=sys.stderr,
        )


def open_report(report: Path, *, no_open: bool) -> None:
    print(f"HTML report saved: {report.resolve()}")
    if not no_open and not webbrowser.open(report.resolve().as_uri()):
        print("Could not open the default browser. Open the HTML file directly.", file=sys.stderr)
