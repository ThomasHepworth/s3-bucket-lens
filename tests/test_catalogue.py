import json
import os
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Lock
from unittest.mock import Mock

import boto3
import pytest
from botocore.stub import Stubber

from s3_bucket_lens.catalogue import catalogue
from s3_bucket_lens.database import connect


def object_item(key, size=10):
    return {"Key": key, "Size": size, "LastModified": datetime(2026, 10, 19, tzinfo=UTC)}


def test_hybrid_includes_root_objects_markers_and_nested_prefixes(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "IsTruncated": False,
                "Contents": [object_item("root.txt")],
                "CommonPrefixes": [{"Prefix": "nested/"}],
            },
            {"Bucket": "example", "Prefix": "", "MaxKeys": 1000, "Delimiter": "/"},
        )
        stubber.add_response(
            "list_objects_v2",
            {
                "IsTruncated": False,
                "Contents": [object_item("nested/", 0), object_item("nested/a.parquet")],
            },
            {"Bucket": "example", "Prefix": "nested/", "MaxKeys": 1000, "Delimiter": "/"},
        )
        output = tmp_path / "catalogue"
        result = catalogue("example", output, client=client, workers=1, strategy="hybrid")
        stubber.assert_no_pending_responses()
    assert result["complete"] is True
    assert result["objects"] == 3
    assert result["api_requests"] == 2
    assert json.loads((output / "manifest.json").read_text())["complete"] is True
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute("SELECT count(DISTINCT key) FROM objects").fetchone() == (3,)
        assert database.execute(
            "SELECT object_count, file_count, total_bytes FROM folders WHERE path = ''"
        ).fetchone() == (3, 2, 20)
        assert database.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(output / "objects.parquet")]
        ).fetchone() == (3,)


def test_flat_paginates_without_head_requests(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {"IsTruncated": True, "NextContinuationToken": "next", "Contents": [object_item("a")]},
            {"Bucket": "example", "Prefix": "", "MaxKeys": 1000},
        )
        stubber.add_response(
            "list_objects_v2",
            {"IsTruncated": False, "Contents": [object_item("b")]},
            {"Bucket": "example", "Prefix": "", "MaxKeys": 1000, "ContinuationToken": "next"},
        )
        result = catalogue("example", tmp_path / "flat", workers=1, strategy="flat", client=client)
        stubber.assert_no_pending_responses()
    assert result["complete"] is True
    assert result["objects"] == 2


def test_failed_listing_is_not_published(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_client_error("list_objects_v2", "AccessDenied")
        with pytest.raises(Exception, match="AccessDenied"):
            catalogue("example", tmp_path / "failed", workers=1, client=client)
    assert list(tmp_path.iterdir()) == []


def test_benchmark_limit_is_explicitly_incomplete(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {"IsTruncated": True, "NextContinuationToken": "next", "Contents": [object_item("a")]},
        )
        result = catalogue(
            "example",
            tmp_path / "partial",
            workers=1,
            strategy="flat",
            client=client,
            max_requests=1,
            allow_partial=True,
        )
    assert result["objects"] == 1
    assert result["complete"] is False


def test_key_range_ends_without_another_page(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "IsTruncated": True,
                "NextContinuationToken": "unused",
                "Contents": [object_item("a"), object_item("b"), object_item("c")],
            },
        )
        result = catalogue(
            "example", tmp_path / "range", workers=1, strategy="flat", client=client, end_key="b"
        )
        stubber.assert_no_pending_responses()
    assert result["objects"] == 2
    assert result["complete"] is True


def test_special_keys_and_repeated_slashes_are_preserved(tmp_path):
    client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    keys = ["a//b.bin", "a//", "caf\u00e9/hello+%.txt", "root\nfile", "not-a-marker/"]
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "IsTruncated": False,
                "Contents": [
                    object_item(keys[0], 9),
                    object_item(keys[1], 0),
                    object_item(keys[2], 2),
                    object_item(keys[3], 3),
                    object_item(keys[4], 7),
                ],
            },
        )
        output = tmp_path / "special"
        result = catalogue("example", output, client=client, workers=1, strategy="flat")
    assert result["unique_keys"] == 5
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert {row[0] for row in database.execute("SELECT key FROM objects").fetchall()} == set(
            keys
        )
        assert database.execute(
            "SELECT object_count, file_count, total_bytes FROM folders WHERE path = ''"
        ).fetchone() == (5, 4, 21)
        assert database.execute(
            "SELECT object_count, total_bytes FROM folders WHERE path = 'a//'"
        ).fetchone() == (2, 9)


@pytest.mark.parametrize("resume", [False, True])
def test_current_catalogue_cli_is_separate_from_version_audit(
    tmp_path, monkeypatch, capsys, resume
):
    from s3_bucket_lens import cli

    calls = []

    def collect(bucket, output, **options):
        calls.append((bucket, output, options))
        options["progress"]({"objects": 1000, "pages": 1, "elapsed_seconds": 0.5})
        return {"complete": True, "objects": 1000}

    monkeypatch.setattr(cli, "catalogue", collect)
    assert (
        cli.main(
            [
                "catalogue",
                "example",
                "--output",
                str(tmp_path / "catalogue"),
                "--profile",
                "data",
                "--region",
                "eu-west-1",
                "--prefix",
                "data/",
                *(["--resume"] if resume else []),
            ]
        )
        == 0
    )
    assert calls[0][0] == "example"
    assert calls[0][2]["workers"] == 16
    assert calls[0][2]["strategy"] == "prefix"
    assert calls[0][2]["prefix"] == "data/"
    assert calls[0][2]["checkpoint"] is True
    assert calls[0][2]["resume"] is resume
    output = capsys.readouterr()
    assert json.loads(output.out)["complete"] is True
    assert "1,000 objects" in output.err


@pytest.mark.parametrize("workers", [1, 4])
def test_paginated_common_prefixes_have_no_gaps_or_overlap(tmp_path, workers):
    pages = {
        ("", None): {
            "IsTruncated": True,
            "NextContinuationToken": "root-next",
            "Contents": [object_item("root-a", 1)],
            "CommonPrefixes": [{"Prefix": "a/"}],
        },
        ("", "root-next"): {
            "IsTruncated": False,
            "Contents": [object_item("root-b", 2)],
            "CommonPrefixes": [{"Prefix": "b/"}],
        },
        ("a/", None): {
            "IsTruncated": True,
            "NextContinuationToken": "a-next",
            "Contents": [object_item("a/", 0)],
            "CommonPrefixes": [{"Prefix": "a//"}],
        },
        ("a/", "a-next"): {
            "IsTruncated": False,
            "Contents": [object_item("a/file", 3)],
        },
        ("a//", None): {"IsTruncated": False, "Contents": [object_item("a//file", 4)]},
        ("b/", None): {"IsTruncated": False, "Contents": [object_item("b/file", 5)]},
    }
    calls = Counter()
    lock = Lock()

    def fetch(**arguments):
        identity = (arguments["Prefix"], arguments.get("ContinuationToken"))
        with lock:
            calls[identity] += 1
        assert arguments["MaxKeys"] == 1000
        return pages[identity]

    output = tmp_path / "concurrent"
    result = catalogue(
        "example", output, client=Mock(list_objects_v2=fetch), workers=workers, strategy="hybrid"
    )
    assert result["complete"] is True
    assert result["objects"] == 6
    assert result["unique_keys"] == 6
    assert result["total_bytes"] == 15
    assert calls == Counter(dict.fromkeys(pages, 1))
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute(
            "SELECT object_count, file_count, total_bytes FROM folders WHERE path = ''"
        ).fetchone() == (6, 5, 15)


def test_bad_token_and_duplicate_keys_are_never_published(tmp_path):
    for name, pages, message in [
        ("token", [{"IsTruncated": True, "Contents": [object_item("a")]}], "continuation"),
        (
            "duplicate",
            [{"IsTruncated": False, "Contents": [object_item("a"), object_item("a")]}],
            "duplicate",
        ),
    ]:
        client = Mock()
        client.list_objects_v2.side_effect = pages
        with pytest.raises(RuntimeError, match=message):
            catalogue("example", tmp_path / name, client=client, workers=1, strategy="flat")
        assert not (tmp_path / name).exists()
    assert list(tmp_path.iterdir()) == []


def test_empty_catalogue_and_existing_output(tmp_path):
    client = Mock()
    client.list_objects_v2.return_value = {"IsTruncated": False}
    output = tmp_path / "empty"
    result = catalogue("example", output, client=client, workers=1)
    assert result["complete"] is True
    assert result["objects"] == 0
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute(
            "SELECT object_count, total_bytes FROM folders WHERE path = ''"
        ).fetchone() == (0, 0)
    with pytest.raises(FileExistsError):
        catalogue("example", output, client=client, workers=1)
    assert client.list_objects_v2.call_count == 1


def test_folder_discovery_cap_does_not_claim_complete_tree(tmp_path, monkeypatch):
    import importlib.util
    from pathlib import Path
    from types import SimpleNamespace

    path = Path(__file__).parents[1] / "scripts" / "benchmark-listing.py"
    specification = importlib.util.spec_from_file_location("benchmark_listing", path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": True,
        "NextContinuationToken": "more",
        "CommonPrefixes": [{"Prefix": "child/"}],
    }
    monkeypatch.setattr(module, "listing_client", lambda **options: client)
    arguments = SimpleNamespace(
        output=tmp_path / "folders",
        profile=None,
        region=None,
        prefix="",
        bucket="example",
        max_requests=100,
        seconds=10,
        depth=1,
        max_pages_per_prefix=1,
    )
    result = module.benchmark_discovery(arguments, None)
    assert client.list_objects_v2.call_count == 1
    assert result["capped_prefixes"] == 1
    assert result["complete_discovery_to_requested_depth"] is False
    assert result["complete_object_listing"] is False


@pytest.mark.parametrize("workers", [1, 4, 8, 16, 32])
def test_paginated_prefix_discovery_captures_every_disjoint_task(tmp_path, workers):
    def listing(**arguments):
        assert arguments["MaxKeys"] == 1000
        if arguments["Prefix"] == "":
            assert arguments["Delimiter"] == "/"
            if arguments.get("ContinuationToken") == "next":
                return {"IsTruncated": False, "CommonPrefixes": [{"Prefix": "c/"}]}
            return {
                "IsTruncated": True,
                "NextContinuationToken": "next",
                "Contents": [object_item("root")],
                "CommonPrefixes": [{"Prefix": "a/"}, {"Prefix": "b/"}],
            }
        assert "Delimiter" not in arguments
        return {
            "IsTruncated": False,
            "Contents": [
                object_item(arguments["Prefix"] + "first"),
                object_item(arguments["Prefix"] + "deep/second"),
            ],
        }

    client = Mock()
    client.list_objects_v2.side_effect = listing
    output = tmp_path / "parallel"
    result = catalogue("example", output, workers=workers, client=client)
    assert result["complete"] is True
    assert result["objects"] == result["unique_keys"] == 7
    assert result["api_requests"] == 5
    assert result["total_bytes"] == 70
    assert client.list_objects_v2.call_count == 5
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute("SELECT object_count FROM folders WHERE path = ''").fetchone() == (
            7,
        )


def test_empty_catalogue_has_a_zero_root_and_parquet(tmp_path):
    client = Mock()
    client.list_objects_v2.return_value = {"IsTruncated": False}
    output = tmp_path / "empty"
    result = catalogue("example", output, workers=1, client=client)
    assert result["objects"] == 0
    assert result["complete"] is True
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute("SELECT * FROM folders").fetchall() == [("", 0, 0, 0)]
    assert (output / "objects.parquet").is_file()


def test_duplicate_keys_prevent_publication(tmp_path):
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": False,
        "Contents": [object_item("same"), object_item("same")],
    }
    with pytest.raises(RuntimeError, match="duplicate"):
        catalogue("example", tmp_path / "duplicate", workers=1, client=client)
    assert list(tmp_path.iterdir()) == []


def test_repeated_continuation_token_prevents_publication(tmp_path):
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": True,
        "NextContinuationToken": "same",
        "Contents": [object_item("a")],
    }
    with pytest.raises(RuntimeError, match="continuation"):
        catalogue("example", tmp_path / "repeated", workers=1, strategy="flat", client=client)
    assert list(tmp_path.iterdir()) == []


def test_literal_prefix_and_retry_counts_are_retained(tmp_path):
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": False,
        "Contents": [object_item("literal-file")],
        "ResponseMetadata": {"RetryAttempts": 2},
    }
    result = catalogue("example", tmp_path / "prefix", prefix="literal", workers=1, client=client)
    assert client.list_objects_v2.call_args.kwargs["Prefix"] == "literal"
    assert result["scope"] == "prefix"
    assert result["retries"] == 2
    assert result["api_requests"] == 3


def test_checkpoint_resumes_after_committed_page_without_duplicates(tmp_path):
    output = tmp_path / "recoverable"
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": True,
        "NextContinuationToken": "second-page",
        "Contents": [object_item("a", 3)],
    }

    def interrupt(statistics):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        catalogue(
            "example",
            output,
            workers=1,
            strategy="flat",
            client=client,
            checkpoint=True,
            progress=interrupt,
        )
    staging = tmp_path / ".recoverable.in-progress"
    assert not output.exists()
    with connect(staging / "catalogue.duckdb", read_only=True) as database:
        assert database.execute("SELECT key FROM objects").fetchall() == [("a",)]
        assert database.execute(
            "SELECT request_token, next_token, object_count, total_bytes FROM processed_pages"
        ).fetchall() == [("", "second-page", 1, 3)]
    with pytest.raises(FileExistsError, match="resume"):
        catalogue("example", output, client=client, checkpoint=True)
    client.list_objects_v2.return_value = {
        "IsTruncated": False,
        "Contents": [object_item("b", 7)],
    }
    result = catalogue(
        "example",
        output,
        workers=1,
        strategy="flat",
        client=client,
        resume=True,
    )
    assert client.list_objects_v2.call_args.kwargs["ContinuationToken"] == "second-page"
    assert result["complete"] is True
    assert result["objects"] == result["unique_keys"] == 2
    assert result["total_bytes"] == 10
    assert result["pages"] == 2
    assert not staging.exists()
    with connect(output / "catalogue.duckdb", read_only=True) as database:
        assert database.execute(
            "SELECT request_token FROM processed_pages ORDER BY request_token"
        ).fetchall() == [("",), ("second-page",)]


@pytest.mark.parametrize("crash_point", ["during_insert", "after_commit"])
def test_checkpoint_recovers_wal_after_hard_process_exit(tmp_path, crash_point):
    output = tmp_path / "hard-exit"
    code = """
import os
import sys
from importlib import import_module
from unittest.mock import Mock
module = import_module("s3_bucket_lens.catalogue")
client = Mock()
client.list_objects_v2.return_value = {
    "IsTruncated": True, "NextContinuationToken": "second",
    "Contents": [{"Key": "a", "Size": 3, "LastModified": "2026-10-19T00:00:00Z"}],
}
def interrupt(statistics):
    os._exit(77)
if sys.argv[2] == "during_insert":
    original = module._insert_page
    def insert_then_exit(database, items):
        original(database, items)
        os._exit(77)
    module._insert_page = insert_then_exit
module.catalogue("example", sys.argv[1], workers=1, strategy="flat",
                 client=client, checkpoint=True, progress=interrupt)
"""
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    child = subprocess.run(
        [sys.executable, "-c", code, str(output), crash_point],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 77, child.stderr
    staging = tmp_path / ".hard-exit.in-progress"
    assert (staging / "catalogue.duckdb.wal").is_file()
    with connect(staging / "catalogue.duckdb") as database:
        expected = 1 if crash_point == "after_commit" else 0
        assert database.execute("SELECT count(*) FROM objects").fetchone() == (expected,)
        assert database.execute("SELECT count(*) FROM processed_pages").fetchone() == (expected,)

    def listing(**arguments):
        if arguments.get("ContinuationToken") == "second":
            return {"IsTruncated": False, "Contents": [object_item("b", 7)]}
        return {
            "IsTruncated": True,
            "NextContinuationToken": "second",
            "Contents": [object_item("a", 3)],
        }

    client = Mock(list_objects_v2=Mock(side_effect=listing))
    result = catalogue(
        "example",
        output,
        workers=1,
        strategy="flat",
        client=client,
        resume=True,
    )
    assert client.list_objects_v2.call_count == (1 if expected else 2)
    assert result["objects"] == result["unique_keys"] == 2
    assert result["pages"] == 2
    assert result["total_bytes"] == 10


def test_resume_reclaims_parallel_uncommitted_work(tmp_path):
    output = tmp_path / "parallel-resume"
    entered = Event()
    release = Event()
    calls = Counter()
    lock = Lock()

    def listing(**arguments):
        prefix = arguments["Prefix"]
        with lock:
            calls[prefix] += 1
        if prefix == "":
            return {"IsTruncated": False, "CommonPrefixes": [{"Prefix": "a/"}, {"Prefix": "b/"}]}
        if prefix == "b/":
            entered.set()
            assert release.wait(5)
        else:
            assert entered.wait(5)
        return {"IsTruncated": False, "Contents": [object_item(prefix + "file")]}

    def interrupt(statistics):
        if statistics["objects"]:
            release.set()
            raise KeyboardInterrupt

    client = Mock(list_objects_v2=listing)
    with pytest.raises(KeyboardInterrupt):
        catalogue("example", output, workers=2, client=client, checkpoint=True, progress=interrupt)
    result = catalogue("example", output, workers=2, client=client, resume=True)
    assert result["objects"] == result["unique_keys"] == 2
    assert result["pages"] == 3
    assert calls == Counter({"": 1, "a/": 1, "b/": 2})


@pytest.mark.parametrize(
    "changed",
    [
        {"bucket": "different"},
        {"prefix": "different/"},
        {"strategy": "prefix"},
        {"region": "eu-west-2"},
        {"end_key": "z"},
        {"max_split_depth": 3},
    ],
)
def test_resume_rejects_different_scan_settings(tmp_path, changed):
    output = tmp_path / "settings"
    client = Mock()
    client.list_objects_v2.return_value = {"IsTruncated": False, "Contents": [object_item("a")]}

    def interrupt(statistics):
        raise KeyboardInterrupt

    options = {"bucket": "example", "strategy": "flat", "region": "eu-west-1", "workers": 1}
    with pytest.raises(KeyboardInterrupt):
        catalogue(output=output, client=client, checkpoint=True, progress=interrupt, **options)
    with pytest.raises(ValueError, match="settings"):
        catalogue(output=output, client=client, resume=True, **{**options, **changed})
    assert client.list_objects_v2.call_count == 1
    assert not output.exists()


def test_resume_restarts_finalisation_without_listing_again(tmp_path, monkeypatch):
    from importlib import import_module

    module = import_module("s3_bucket_lens.catalogue")
    aggregate = module._aggregate_folders

    def aggregate_then_fail(database):
        aggregate(database)
        raise RuntimeError("export interrupted")

    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": False,
        "Contents": [object_item("a/file")],
    }
    output = tmp_path / "finalisation"
    monkeypatch.setattr(module, "_aggregate_folders", aggregate_then_fail)
    with pytest.raises(RuntimeError, match="export interrupted"):
        catalogue("example", output, workers=1, client=client, checkpoint=True)
    monkeypatch.setattr(module, "_aggregate_folders", aggregate)
    result = catalogue("example", output, workers=1, client=client, resume=True)
    assert client.list_objects_v2.call_count == 1
    assert result["objects"] == 1
    assert result["complete"] is True
    assert (output / "folders.parquet").is_file()
