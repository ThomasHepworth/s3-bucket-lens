"""Read-only S3 metadata extraction using the normal boto3 credential chain."""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .access import import_access_logs
from .database import (
    aggregate,
    connect,
    import_access_csv,
    initialise,
    insert_objects,
    parent_for_key,
    set_metadata,
)
from .pricing import load_rates


def extract(
    bucket: str,
    output: Path,
    *,
    profile: str | None = None,
    region: str | None = None,
    prefix: str = "",
    max_depth: int = 16,
    memory_limit: str = "512MB",
    pricing_file: Path | None = None,
    access_csv: Path | None = None,
    access_logs: str | None = None,
    multipart_sizes: bool = True,
    client=None,
    rates_document: dict | None = None,
) -> Path:
    if output.exists():
        raise FileExistsError(f"Snapshot already exists: {output}. Choose a new --output path.")
    if max_depth < 1 or max_depth > 256:
        raise ValueError("--max-depth must be between 1 and 256.")
    if access_csv and access_logs:
        raise ValueError("Choose either --access-csv or --access-logs.")
    if bucket.endswith("--x-s3") or bucket.startswith(("arn:", "s3://")) or "/" in bucket:
        raise ValueError("Use a general purpose bucket name, not an ARN, URI or directory bucket.")
    started = datetime.now(UTC)
    if client is None:
        session = boto3.Session(profile_name=profile, region_name=region)
        client = session.client(
            "s3", config=Config(retries={"mode": "adaptive", "max_attempts": 8})
        )
        client.meta.events.register("needs-retry.s3", retry_clock_skew)
    warnings = []
    location = client.get_bucket_location(Bucket=bucket).get("LocationConstraint")
    location = "eu-west-1" if location == "EU" else location or "us-east-1"
    if region and region != location:
        raise ValueError(f"Bucket is in {location}, not {region}. Rerun with --region {location}.")
    versioning = client.get_bucket_versioning(Bucket=bucket).get("Status", "Disabled")
    pricing = rates_document or load_rates(location, pricing_file)
    if pricing["region"] != location:
        raise ValueError(
            f"Pricing region {pricing['region']} does not match bucket region {location}."
        )
    bucket_info = {
        "name": bucket,
        "region": location,
        "versioning": versioning,
        "prefix_filter": prefix,
        "max_depth": max_depth,
        "demo": False,
    }
    for method, label, absent in [
        ("get_bucket_lifecycle_configuration", "lifecycle", "NoSuchLifecycleConfiguration"),
        ("get_bucket_encryption", "encryption", "ServerSideEncryptionConfigurationNotFoundError"),
        ("get_public_access_block", "public_access", "NoSuchPublicAccessBlockConfiguration"),
    ]:
        try:
            response = getattr(client, method)(Bucket=bucket)
            response.pop("ResponseMetadata", None)
            bucket_info[label] = response
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            bucket_info[label] = None
            if code != absent:
                warnings.append(f"{label}: unavailable ({code})")
        except BotoCoreError as exc:
            bucket_info[label] = None
            warnings.append(f"{label}: unavailable ({type(exc).__name__})")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".s3-lens-", dir=output.parent) as staging:
        temporary = Path(staging) / "snapshot.duckdb"
        connection = connect(temporary, memory_limit)
        try:
            initialise(connection)
            operation = (
                "list_object_versions"
                if versioning in {"Enabled", "Suspended"}
                else "list_objects_v2"
            )
            for page_number, page in enumerate(
                client.get_paginator(operation).paginate(
                    Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}
                ),
                start=1,
            ):
                insert_objects(
                    connection,
                    page.get("Versions", page.get("Contents", [])),
                    pricing["rates"],
                    max_depth,
                )
                insert_objects(
                    connection,
                    page.get("DeleteMarkers", []),
                    pricing["rates"],
                    max_depth,
                    delete_markers=True,
                )
                print(
                    f"\rDownloaded inventory page {page_number:,}",
                    end="",
                    file=sys.stderr,
                    flush=True,
                )
            print(file=sys.stderr)
            collect_multipart(
                client,
                connection,
                bucket,
                prefix,
                max_depth,
                warnings,
                include_sizes=multipart_sizes,
            )
            if access_csv:
                import_access_csv(connection, access_csv, bucket)
            if access_logs:
                print("Downloading observed GET evidence from S3 access logs...", file=sys.stderr)
                evidence = import_access_logs(client, connection, access_logs, bucket)
                set_metadata(connection, "access_evidence", evidence)
                if not evidence["successful_get_events"]:
                    warnings.append("Access logs contained no recognised successful object GETs.")
            set_metadata(connection, "bucket", bucket_info)
            set_metadata(connection, "pricing", pricing)
            set_metadata(connection, "warnings", warnings)
            set_metadata(connection, "scan_started_at", started.isoformat())
            set_metadata(
                connection,
                "access_source",
                access_logs or (str(access_csv.name) if access_csv else None),
            )
            print("Aggregating directory totals in DuckDB...", file=sys.stderr)
            aggregate(connection)
        finally:
            connection.close()
        if output.exists():
            raise FileExistsError(f"Snapshot appeared while extracting: {output}")
        temporary.chmod(0o600)
        os.link(temporary, output)
    return output


def retry_clock_skew(response=None, attempts=1, **kwargs):
    if response and attempts <= 3:
        if response[1].get("Error", {}).get("Code") == "RequestTimeTooSkewed":
            return 0.0
    return None


def collect_multipart(client, connection, bucket, prefix, max_depth, warnings, *, include_sizes):
    try:
        for page in client.get_paginator("list_multipart_uploads").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            for upload in page.get("Uploads", []):
                size = None
                if include_sizes:
                    try:
                        size = sum(
                            part["Size"]
                            for part_page in client.get_paginator("list_parts").paginate(
                                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                            )
                            for part in part_page.get("Parts", [])
                        )
                    except (ClientError, BotoCoreError) as exc:
                        warning = f"Multipart sizes unavailable: {type(exc).__name__}"
                        if warning not in warnings:
                            warnings.append(warning)
                connection.execute(
                    "INSERT INTO multipart_uploads VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        upload["Key"],
                        upload["UploadId"],
                        parent_for_key(upload["Key"], max_depth),
                        upload.get("Initiated"),
                        upload.get("StorageClass", "STANDARD"),
                        size,
                    ],
                )
    except (ClientError, BotoCoreError) as exc:
        warnings.append(f"Multipart inventory unavailable: {type(exc).__name__}")
