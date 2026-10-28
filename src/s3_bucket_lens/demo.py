"""Synthetic metadata for trying the complete workflow without an AWS account."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from .database import aggregate, connect, initialise, insert_objects, set_metadata


def create_demo(output: Path) -> Path:
    if output.exists():
        raise FileExistsError(f"Snapshot already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    rates = {
        "STANDARD": 0.024,
        "STANDARD_IA": 0.0131,
        "ONEZONE_IA": 0.01048,
        "GLACIER_IR": 0.005,
        "GLACIER": 0.00405,
        "DEEP_ARCHIVE": 0.0018,
    }
    folders = [
        "raw/events",
        "raw/imports",
        "curated/analytics",
        "curated/finance",
        "backups/database",
        "backups/legacy",
        "logs/application",
        "logs/audit",
        "exports/monthly",
        "experiments",
        "archive/2022",
        "archive/2023",
    ]
    with connect(output) as connection:
        initialise(connection)
        for index, folder in enumerate(folders):
            storage_class = list(rates)[index % len(rates)]
            rows = [
                {
                    "Key": f"{folder}/partition-{number:04d}.parquet",
                    "Size": (24 - index) * (number % 7 + 1) * 1024**3,
                    "LastModified": now - timedelta(days=index * 37 + number % 12),
                    "StorageClass": storage_class,
                    "IsLatest": True,
                    "VersionId": f"demo-{index}-{number}",
                }
                for number in range(32)
            ]
            insert_objects(connection, rows, rates, 16)
            insert_objects(
                connection,
                [{**row, "IsLatest": False, "Size": row["Size"] // 3} for row in rows[:12]],
                rates,
                16,
            )
        insert_objects(
            connection,
            [
                {
                    "Key": "intelligent/data.parquet",
                    "Size": 200 * 1024**3,
                    "StorageClass": "INTELLIGENT_TIERING",
                    "LastModified": now,
                },
                {"Key": "README.txt", "Size": 1024, "LastModified": now},
            ],
            rates,
            16,
        )
        insert_objects(
            connection,
            [{"Key": "backups/deleted", "LastModified": now}],
            rates,
            16,
            delete_markers=True,
        )
        connection.execute(
            "UPDATE objects SET last_accessed = ? WHERE is_latest AND key LIKE 'raw/%'",
            [now - timedelta(days=2)],
        )
        connection.execute(
            "INSERT INTO multipart_uploads VALUES (?, ?, ?, ?, ?, ?)",
            [
                "exports/monthly/unfinished.parquet",
                "demo-upload",
                "exports/monthly/",
                now - timedelta(days=18),
                "STANDARD",
                64 * 1024**3,
            ],
        )
        set_metadata(
            connection,
            "bucket",
            {
                "name": "data-platform-prod",
                "region": "eu-west-2",
                "versioning": "Enabled",
                "prefix_filter": "",
                "max_depth": 16,
                "demo": True,
                "lifecycle": {
                    "Rules": [
                        {
                            "ID": "expire-application-logs",
                            "Status": "Enabled",
                            "Filter": {"Prefix": "logs/application/"},
                            "Expiration": {"Days": 90},
                        },
                        {
                            "ID": "archive-old-backups",
                            "Status": "Enabled",
                            "Filter": {"Prefix": "backups/"},
                            "Transitions": [{"Days": 90, "StorageClass": "DEEP_ARCHIVE"}],
                        },
                        {
                            "ID": "expire-exports",
                            "Status": "Disabled",
                            "Filter": {"Prefix": "exports/"},
                            "Expiration": {"Days": 30},
                        },
                    ]
                },
                "encryption": {
                    "ServerSideEncryptionConfiguration": {
                        "Rules": [
                            {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"}}
                        ]
                    }
                },
                "public_access": {
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    }
                },
            },
        )
        set_metadata(
            connection,
            "pricing",
            {
                "region": "eu-west-2",
                "currency": "USD",
                "rates": rates,
                "source": "Illustrative demo rates, not a current quotation",
            },
        )
        set_metadata(
            connection, "warnings", ["Synthetic demonstration data. No AWS account was queried."]
        )
        set_metadata(connection, "access_source", "Synthetic successful GET evidence")
        set_metadata(connection, "scan_started_at", now.isoformat())
        aggregate(connection, now=now)
    return output
