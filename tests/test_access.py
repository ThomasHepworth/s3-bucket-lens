import gzip
import io
from unittest.mock import MagicMock

from s3_bucket_lens.access import import_access_logs, parse_access_record
from s3_bucket_lens.database import connect, initialise, insert_objects


def record(key="a%20%2Bb", operation="REST.GET.OBJECT", status=200, bucket="example"):
    return (
        f"owner {bucket} [18/Oct/2026:12:00:00 +0000] ip requester request-id "
        f'{operation} {key} "GET /{key} HTTP/1.1" {status} - 100 100 1 1 "-" "agent"'
    )


def test_only_successful_object_gets_are_access_evidence():
    key, timestamp = parse_access_record(record(), "example")
    assert key == "a +b"
    assert timestamp.day == 18
    assert parse_access_record(record(operation="REST.HEAD.OBJECT"), "example") is None
    assert parse_access_record(record(status=403), "example") is None
    assert parse_access_record(record(bucket="other"), "example") is None
    assert parse_access_record("malformed record", "example") is None


def test_plain_and_gzip_access_logs_are_streamed(tmp_path):
    client = MagicMock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "logs/plain"}]},
        {"Contents": [{"Key": "logs/compressed.gz"}]},
    ]
    client.get_object.side_effect = [
        {"Body": io.BytesIO((record() + "\n").encode())},
        {"Body": io.BytesIO(gzip.compress((record(status=206) + "\n").encode()))},
    ]
    with connect(tmp_path / "snapshot.duckdb") as connection:
        initialise(connection)
        insert_objects(
            connection,
            [{"Key": "a +b", "Size": 10}, {"Key": "unknown", "Size": 10}],
            {"STANDARD": 0.024},
            16,
        )
        result = import_access_logs(client, connection, "s3://logs-bucket/logs/", "example")
        assert result["log_files"] == result["successful_get_events"] == 2
        assert (
            connection.execute(
                "SELECT count(*) FROM objects WHERE last_accessed IS NOT NULL"
            ).fetchone()[0]
            == 1
        )
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="logs-bucket", Prefix="logs/"
    )
