import json

import pytest

from s3_bucket_lens.builder import (
    PAGE_SIZE,
    build,
    build_folders,
    prefix_id,
    publish_folder_dashboard,
    publish_html,
)
from s3_bucket_lens.database import (
    aggregate,
    connect,
    initialise,
    insert_objects,
    set_metadata,
)
from s3_bucket_lens.demo import create_demo


def read_report_data(path):
    script = path.read_text()
    prefix = 'document.dispatchEvent(new CustomEvent("s3-bucket-lens-data", {"detail":'
    assert script.startswith(prefix)
    assert script.endswith("}));\n")
    payload = json.loads(script.removeprefix(prefix).removesuffix("}));\n"))
    assert payload["name"] == path.name
    return payload["data"]


@pytest.mark.parametrize("sharded", [False, True])
def test_completed_current_catalogue_builds_original_dashboard(tmp_path, monkeypatch, sharded):
    from datetime import UTC, datetime
    from unittest.mock import Mock

    from s3_bucket_lens.catalogue import catalogue

    directory = tmp_path / "catalogue"
    client = Mock()
    client.list_objects_v2.return_value = {
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "folder/file",
                "Size": 1073741824,
                "LastModified": datetime(2026, 10, 19, tzinfo=UTC),
            },
            {"Key": "folder/", "Size": 0, "LastModified": datetime(2026, 10, 19, tzinfo=UTC)},
        ],
    }
    catalogue("example", directory, region="eu-west-1", workers=1, strategy="flat", client=client)

    def forbid_reaggregation(*arguments, **options):
        raise AssertionError("Current reports must reuse the existing folder roll-ups")

    monkeypatch.setattr("s3_bucket_lens.database.aggregate", forbid_reaggregation)
    if sharded:
        monkeypatch.setattr("s3_bucket_lens.builder.SHARD_THRESHOLD", 0)
    output = tmp_path / "current.html"
    assert build(directory, output) == output
    site = tmp_path / "current.report"
    manifest = read_report_data(site / "manifest.js")
    assert manifest["catalogue_kind"] == "current_objects"
    assert manifest["root"]["total_bytes"] == 1073741824
    assert manifest["root"]["objects"] == 1
    assert manifest["root"]["delete_markers"] is None
    assert manifest["root"]["noncurrent_usd"] is None
    assert manifest["root"]["monthly_usd"] == pytest.approx(
        manifest["pricing"]["rates"]["STANDARD"]
    )
    assert any("S3 Standard" in warning for warning in manifest["warnings"])
    identifier = prefix_id("")
    if sharded:
        records = read_report_data(site / "data" / f"{identifier[:3]}-cost.js")
        root = records[f"{identifier}-cost-1"]
        columns = manifest["directory_columns"]
        root["children"] = [dict(zip(columns, values, strict=True)) for values in root["children"]]
        assert manifest["directory_shard_prefix"] == 3
    else:
        root = read_report_data(site / "data" / f"{identifier}-cost-1.js")
    assert root["children"][0]["path"] == "folder/"
    assert root["children"][0]["total_bytes"] == 1073741824
    assert 'role="treegrid"' in output.read_text()
    assert not list(site.rglob("*.duckdb"))


def test_dashboard_keeps_unknown_measurements_distinct_from_zero():
    import shutil
    import subprocess
    from importlib.resources import files

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the JavaScript contract check")
    script = str(files("s3_bucket_lens").joinpath("assets/dashboard.js"))
    code = """
        const fs = require('fs'), vm = require('vm'), assert = require('assert');
        const source = fs.readFileSync(process.argv[1], 'utf8')
            .replace(/\\ninitialise\\(\\);\\s*$/, '');
        const element = {addEventListener() {}};
        const context = {
            document: {addEventListener() {}, getElementById() {return element;}},
            window: {addEventListener() {}}
        };
        const values = vm.runInNewContext(source + `
            ({unknown: reviewPriority({monthly_usd: null}),
              zero: reviewPriority({monthly_usd: 0}),
              moneyUnknown: money(null), countUnknown: count(null),
              sizeUnknown: size(null), zeroSize: size(0),
              red: reviewPriority({monthly_usd: 21}),
              amber: reviewPriority({monthly_usd: 6})})`, context);
        assert.equal(values.unknown.label, 'Not assessed');
        assert.notEqual(values.unknown.level, 'green');
        assert.equal(values.zero.level, 'green');
        assert.equal(values.red.level, 'red');
        assert.equal(values.amber.level, 'amber');
        assert.equal(values.moneyUnknown, 'Not measured');
        assert.equal(values.countUnknown, 'Not measured');
        assert.equal(values.sizeUnknown, 'Not measured');
        assert.equal(values.zeroSize, '0 B');
    """
    subprocess.run([node, "-e", code, script], check=True, capture_output=True, text=True)


def test_shards_preserve_every_directory_and_both_paginated_sorts(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from s3_bucket_lens.catalogue import catalogue

    monkeypatch.setattr("s3_bucket_lens.builder.SHARD_THRESHOLD", 0)
    monkeypatch.setattr("s3_bucket_lens.builder.SHARD_PREFIX", 1)
    entries = [
        {"Key": f"folder-{number:03d}/file", "Size": number, "LastModified": "2026-10-19T00:00:00Z"}
        for number in range(300)
    ]
    entries.append(
        {"Key": "special//caf\u00e9+%/file", "Size": 1, "LastModified": "2026-10-19T00:00:00Z"}
    )
    client = Mock()
    client.list_objects_v2.return_value = {"IsTruncated": False, "Contents": entries}
    directory = tmp_path / "catalogue"
    catalogue("example", directory, region="eu-west-1", workers=1, strategy="flat", client=client)
    build(directory, tmp_path / "sharded.html")
    site = tmp_path / "sharded.report"
    manifest = read_report_data(site / "manifest.js")
    columns = manifest["directory_columns"]
    expected_paths = {"", "special/", "special//", "special//caf\u00e9+%/"}
    expected_paths.update(f"folder-{number:03d}/" for number in range(300))
    for order in ["cost", "name"]:
        records = {}
        for path in (site / "data").glob(f"*-{order}.js"):
            contents = read_report_data(path)
            assert not records.keys() & contents.keys()
            records.update(contents)
        paths = {record["node"][0] for record in records.values()}
        assert paths == expected_paths
        root_children = []
        for page in range(1, 5):
            record = records[f"{prefix_id('')}-{order}-{page}"]
            assert record["total"] == 301
            assert record["pages"] == 4
            assert record["page"] == page
            assert len(record["children"]) == (100 if page < 4 else 1)
            root_children.extend(
                dict(zip(columns, values, strict=True)) for values in record["children"]
            )
        key = (
            (lambda row: (-row["monthly_usd"], row["path"]))
            if order == "cost"
            else lambda row: row["path"]
        )
        assert root_children == sorted(root_children, key=key)
        assert len({row["path"] for row in root_children}) == 301
        assert records[f"{prefix_id('folder-299/')}-{order}-1"]["children"] == []
    assert len(list((site / "data").glob("*.js"))) <= 32


def test_folder_report_preserves_partial_scope_and_unknown_totals(tmp_path):
    directory = tmp_path / "folders.report"
    directory.mkdir()
    (directory / "benchmark.json").write_text(
        json.dumps(
            {
                "api_requests": 2,
                "duration_seconds": 0.5,
                "depth": 2,
                "complete_discovery_to_requested_depth": False,
                "capped_prefixes": 1,
            }
        )
    )
    folders = [
        {"path": "parent/", "parent_path": "", "depth": 1},
        {"path": "parent/caf\u00e9+%/", "parent_path": "parent/", "depth": 2},
    ]
    (directory / "directories.ndjson").write_text(
        "\n".join(json.dumps(folder) for folder in folders) + "\n"
    )
    output = tmp_path / "folders.html"
    assert build_folders(directory, output, bucket="actual-bucket") == output
    payload = read_report_data(directory / "folders.js")
    assert payload["bucket"] == "actual-bucket"
    assert payload["folder_count"] == 2
    assert payload["complete_discovery_to_requested_depth"] is False
    assert payload["folders"] == folders
    assert '<base href="folders.report/">' in output.read_text()
    dashboard = (directory / "dashboard.js").read_text()
    assert "Recursive sizes and object counts: not measured" in dashboard
    assert 'role="treegrid"' in output.read_text()
    assert 'id="cost-summary"' in output.read_text()
    assert 'href="dashboard.css"' in output.read_text()
    manifest = read_report_data(directory / "manifest.js")
    assert manifest["folder_only"] is True
    assert manifest["root"]["monthly_usd"] is None
    assert manifest["root"]["total_bytes"] is None
    assert manifest["bucket"]["name"] == "actual-bucket"
    assert "connect-src 'none'" in output.read_text()
    with connect(directory / "folders.duckdb", read_only=True) as database:
        assert database.execute(
            "SELECT count(*) FROM folders WHERE object_count IS NULL AND total_bytes IS NULL"
        ).fetchone() == (2,)
        assert database.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(directory / "folders.parquet")]
        ).fetchone() == (2,)
    with pytest.raises(FileExistsError):
        build_folders(directory, output, bucket="actual-bucket")
    with pytest.raises(FileExistsError):
        publish_folder_dashboard(directory, output, bucket="actual-bucket")
    original_csv = (directory / "folders.csv").read_bytes()
    assert (
        publish_folder_dashboard(directory, output, bucket="actual-bucket", replace=True) == output
    )
    assert (directory / "folders.csv").read_bytes() == original_csv


def test_export_has_paged_data_and_no_database_or_credentials(tmp_path):
    snapshot = create_demo(tmp_path / "demo.duckdb")
    site = tmp_path / "site"
    build(snapshot, site)
    manifest = read_report_data(site / "manifest.js")
    root = read_report_data(site / "data" / f"{prefix_id('')}-cost-1.js")
    assert manifest["bucket"]["demo"] is True
    assert root["node"]["total_bytes"] == manifest["root"]["total_bytes"]
    assert root["total"] == len(root["children"])
    assert root["node"]["child_count"] == root["total"]
    assert all("child_count" in child for child in root["children"])
    assert all(child["id"] == prefix_id(child["path"]) for child in root["children"])
    assert not list(site.rglob("*.duckdb"))
    html = (site / "index.html").read_text()
    assert "<script" in html
    assert (site / "theme.js").is_file()
    assert 'role="treegrid"' in html
    assert 'id="cost-summary"' in html
    assert "Cost review" in html
    assert 'aria-label="Selected directory" hidden' in html
    assert "child-cost" not in (site / "dashboard.js").read_text()
    assert "fetch(" not in (site / "dashboard.js").read_text()
    assert "connect-src 'none'" in html
    assert not list(site.rglob("*.json"))


def test_large_directory_pagination_and_safe_key_paths(tmp_path):
    snapshot = tmp_path / "snapshot.duckdb"
    with connect(snapshot) as connection:
        initialise(connection)
        insert_objects(
            connection,
            (
                {"Key": f"folder-{number:04d}/object", "Size": number}
                for number in range(PAGE_SIZE * 2)
            ),
            {"STANDARD": 0.024},
            16,
        )
        insert_objects(
            connection, [{"Key": "../<script>/object", "Size": 1}], {"STANDARD": 0.024}, 16
        )
        set_metadata(connection, "bucket", {"name": "example"})
        aggregate(connection)
    site = tmp_path / "site"
    build(snapshot, site)
    root = prefix_id("")
    first = read_report_data(site / "data" / f"{root}-name-1.js")
    last = read_report_data(site / "data" / f"{root}-name-3.js")
    assert len(first["children"]) == PAGE_SIZE
    assert last["pages"] == 3
    assert len(last["children"]) == 1
    assert all(len(path.stem.split("-")[0]) == 32 for path in (site / "data").iterdir())
    with pytest.raises(FileExistsError):
        build(snapshot, site)


def test_html_output_is_alongside_its_report_folder(tmp_path):
    snapshot = create_demo(tmp_path / "demo.duckdb")
    html = tmp_path / "bucket lens.html"
    assert build(snapshot, html) == html
    assert '<base href="bucket%20lens.report/">' in html.read_text()
    assert (tmp_path / "bucket lens.report" / "manifest.js").is_file()
    assert "Directory tree" in html.read_text()
    with pytest.raises(FileExistsError):
        build(snapshot, html)


def test_root_html_can_be_published_from_a_completed_report(tmp_path):
    site = tmp_path / "bucket.report"
    build(create_demo(tmp_path / "demo.duckdb"), site)
    html = tmp_path / "bucket.html"
    assert publish_html(site, html) == html
    assert '<base href="bucket.report/">' in html.read_text()
    with pytest.raises(FileExistsError):
        publish_html(site, html)
