import csv
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.stub import Stubber

from s3_bucket_lens.database import (
    aggregate,
    connect,
    import_access_csv,
    initialise,
    insert_objects,
    metadata,
    query_dicts,
)
from s3_bucket_lens.inventory import extract, retry_clock_skew
from s3_bucket_lens.pricing import GIB

NOW = datetime(2026, 10, 19, tzinfo=UTC)
RATES = {"STANDARD": 0.023, "STANDARD_IA": 0.0125}
PRICING = {"region": "us-east-1", "currency": "USD", "rates": RATES}


def item(key, size=GIB, **kwargs):
    return {"Key": key, "Size": size, "LastModified": NOW, **kwargs}


@pytest.mark.parametrize(
    "code,attempts,expected",
    [
        ("RequestTimeTooSkewed", 1, 0.0),
        ("RequestTimeTooSkewed", 3, 0.0),
        ("RequestTimeTooSkewed", 4, None),
        ("AccessDenied", 1, None),
        ("ExpiredToken", 1, None),
    ],
)
def test_clock_skew_retry_is_bounded_and_specific(code, attempts, expected):
    assert (
        retry_clock_skew(response=(None, {"Error": {"Code": code}}), attempts=attempts) == expected
    )
    assert retry_clock_skew(response=None, attempts=attempts) is None


def test_clock_skew_retry_resigns_the_request(monkeypatch):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    client.meta.events.register("needs-retry.s3", retry_clock_skew)
    signing_times = iter([NOW - timedelta(minutes=20), NOW])
    monkeypatch.setattr("botocore.auth.get_current_datetime", lambda: next(signing_times))
    signed_dates = []

    def send(request):
        signed_dates.append(request.headers["X-Amz-Date"])
        body = Mock()
        if len(signed_dates) == 1:
            body.stream.return_value = [b"<Error><Code>RequestTimeTooSkewed</Code></Error>"]
            return AWSResponse(request.url, 403, {}, body)
        body.stream.return_value = [
            b"<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>"
        ]
        return AWSResponse(request.url, 200, {}, body)

    monkeypatch.setattr(client._endpoint.http_session, "send", send)
    result = client.list_object_versions(Bucket="example")
    assert result["IsTruncated"] is False
    assert len(signed_dates) == 2
    assert signed_dates[0] != signed_dates[1]


def test_rollup_preserves_versions_markers_and_unknown_cost(tmp_path):
    with connect(tmp_path / "test.duckdb") as connection:
        initialise(connection)
        insert_objects(
            connection,
            [
                item("raw/events/a"),
                item("raw/events/a", 2 * GIB, IsLatest=False),
                item("raw/events/", 0),
                item("root", 10),
                item("cold/a", StorageClass="INTELLIGENT_TIERING"),
            ],
            RATES,
            16,
        )
        insert_objects(
            connection, [item("raw/deleted", IsLatest=True)], RATES, 16, delete_markers=True
        )
        aggregate(connection, now=NOW)
        rows = {row["path"]: row for row in query_dicts(connection, "SELECT * FROM prefixes")}
        assert rows[""]["total_bytes"] == 4 * GIB + 10
        assert rows["raw/"]["total_bytes"] == 3 * GIB
        assert rows["raw/"]["noncurrent_bytes"] == 2 * GIB
        assert rows["raw/"]["objects"] == 2
        assert rows[""]["delete_markers"] == 1
        assert rows[""]["unknown_cost_objects"] == 1
        assert rows[""]["monthly_usd"] > 0.069


def test_depth_cap_empty_segments_and_large_batch(tmp_path):
    with connect(tmp_path / "test.duckdb") as connection:
        initialise(connection)
        insert_objects(connection, (item(f"a//b/{number}", 1) for number in range(2101)), RATES, 2)
        aggregate(connection)
        rows = query_dicts(connection, "SELECT path, total_bytes FROM prefixes ORDER BY depth")
        assert [row["path"] for row in rows] == ["", "a/", "a//"]
        assert all(row["total_bytes"] == 2101 for row in rows)


def test_access_is_key_level_evidence_not_modification_time(tmp_path):
    path = tmp_path / "access.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["bucket", "key", "last_accessed"])
        writer.writerow(["example", "a", "2026-10-18T00:00:00Z"])
        writer.writerow(["other", "a", "2026-10-19T00:00:00Z"])
    with connect(tmp_path / "test.duckdb") as connection:
        initialise(connection)
        insert_objects(connection, [item("a"), item("a", IsLatest=False), item("b")], RATES, 16)
        import_access_csv(connection, path, "example")
        aggregate(connection, now=NOW)
        root = query_dicts(connection, "SELECT * FROM prefixes WHERE path = ''")[0]
        assert root["access_observed_objects"] == 1
        assert root["last_accessed"].day == 18
        assert root["last_modified"].day == 19


def test_empty_bucket_and_multipart_only_directory(tmp_path):
    with connect(tmp_path / "test.duckdb") as connection:
        initialise(connection)
        connection.execute(
            "INSERT INTO multipart_uploads VALUES (?, ?, ?, ?, ?, ?)",
            ["uploads/a", "upload", "uploads/", NOW, "STANDARD", None],
        )
        aggregate(connection)
        root = query_dicts(connection, "SELECT * FROM prefixes WHERE path = ''")[0]
        assert root["objects"] == root["total_bytes"] == 0
        assert root["multipart_uploads"] == root["multipart_unknown_sizes"] == 1


def stub_metadata(stubber, versioning):
    stubber.add_response("get_bucket_location", {}, {"Bucket": "example"})
    stubber.add_response("get_bucket_versioning", versioning, {"Bucket": "example"})
    for operation, code in [
        ("get_bucket_lifecycle_configuration", "NoSuchLifecycleConfiguration"),
        ("get_bucket_encryption", "AccessDenied"),
        ("get_public_access_block", "NoSuchPublicAccessBlockConfiguration"),
    ]:
        stubber.add_client_error(operation, code, expected_params={"Bucket": "example"})


@pytest.mark.parametrize("versioning", [{}, {"Status": "Suspended"}, {"Status": "Enabled"}])
def test_extraction_paginates_and_publishes_complete_snapshot(tmp_path, versioning):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stub_metadata(stubber, versioning)
        if versioning:
            stubber.add_response(
                "list_object_versions",
                {
                    "Versions": [item("a", VersionId="v1")],
                    "DeleteMarkers": [
                        {
                            "Key": "deleted/key",
                            "VersionId": "marker",
                            "IsLatest": True,
                            "LastModified": NOW,
                        }
                    ],
                    "IsTruncated": True,
                    "NextKeyMarker": "a",
                    "NextVersionIdMarker": "v1",
                },
                {"Bucket": "example", "Prefix": "", "MaxKeys": 1000},
            )
            stubber.add_response(
                "list_object_versions",
                {"Versions": [item("a", 20, IsLatest=False, VersionId="v0")]},
                {
                    "Bucket": "example",
                    "Prefix": "",
                    "MaxKeys": 1000,
                    "KeyMarker": "a",
                    "VersionIdMarker": "v1",
                },
            )
        else:
            stubber.add_response(
                "list_objects_v2",
                {"Contents": [item("a")], "IsTruncated": True, "NextContinuationToken": "next"},
                {"Bucket": "example", "Prefix": "", "MaxKeys": 1000},
            )
            stubber.add_response(
                "list_objects_v2",
                {"Contents": [item("b", 20)]},
                {"Bucket": "example", "Prefix": "", "MaxKeys": 1000, "ContinuationToken": "next"},
            )
        stubber.add_response("list_multipart_uploads", {}, {"Bucket": "example", "Prefix": ""})
        output = extract(
            "example", tmp_path / "snapshot.duckdb", client=client, rates_document=PRICING
        )
        stubber.assert_no_pending_responses()
    with connect(output, read_only=True) as connection:
        assert metadata(connection)["complete"] is True
        assert "encryption: unavailable (AccessDenied)" in metadata(connection)["warnings"]
        assert (
            connection.execute(
                "SELECT sum(size_bytes) FROM objects WHERE NOT is_delete_marker"
            ).fetchone()[0]
            == GIB + 20
        )
        markers = connection.execute(
            "SELECT size_bytes, monthly_usd FROM objects WHERE is_delete_marker"
        ).fetchall()
        assert len(markers) == (1 if versioning else 0)
        if versioning:
            assert markers[0][0] == len(b"deleted/key")
            assert markers[0][1] > 0


def test_failed_scan_does_not_publish_partial_database(tmp_path):
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    output = tmp_path / "snapshot.duckdb"
    with Stubber(client) as stubber:
        stub_metadata(stubber, {})
        stubber.add_client_error("list_objects_v2", "AccessDenied")
        with pytest.raises(Exception, match="AccessDenied"):
            extract("example", output, client=client, rates_document=PRICING)
    assert not output.exists()
    assert not list(tmp_path.glob(".s3-lens-*"))


def test_ingestion_reports_full_count_across_batch_boundaries(tmp_path):
    with connect(tmp_path / "count.duckdb") as connection:
        initialise(connection)
        total = insert_objects(
            connection, (item(str(number), 1) for number in range(2001)), RATES, 16
        )
        assert total == 2001


def test_selected_region_cannot_silently_misprice_bucket(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-2", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_response("get_bucket_location", {}, {"Bucket": "example"})
        with pytest.raises(ValueError, match="Rerun with --region us-east-1"):
            extract("example", tmp_path / "snapshot.duckdb", region="eu-west-2", client=client)
