from pathlib import Path
from unittest.mock import patch

import pytest

from s3_bucket_lens.cli import catalogue_progress, main, parser
from s3_bucket_lens.demo import create_demo


def test_region_default_and_explicit_bucket_selection():
    defaults = parser().parse_args(["extract"])
    assert defaults.bucket == "example-bucket"
    assert defaults.region == "eu-west-2"
    arguments = parser().parse_args(["extract", "--bucket", "example"])
    assert arguments.region == "eu-west-2"
    assert arguments.selected_bucket == "example"
    assert not arguments.no_open
    assert not arguments.snapshot_only
    explicit = parser().parse_args(["extract", "example", "--region", "us-east-1"])
    assert explicit.region == "us-east-1"
    with pytest.raises(SystemExit):
        parser().parse_args(["extract", "example", "--bucket", "other"])


def test_extract_automatically_builds_and_launches_browser(tmp_path):
    snapshot = tmp_path / "snapshot.duckdb"
    html = snapshot.with_suffix(".html")
    with (
        patch("s3_bucket_lens.cli.extract", return_value=snapshot) as extract_mock,
        patch("s3_bucket_lens.cli.build", return_value=html) as build_mock,
        patch("s3_bucket_lens.cli.webbrowser.open") as browser_mock,
    ):
        assert main(["extract"]) == 0
    assert extract_mock.call_args.args[0] == "example-bucket"
    assert extract_mock.call_args.kwargs["region"] == "eu-west-2"
    build_mock.assert_called_once_with(
        snapshot, snapshot.with_suffix(".html"), memory_limit="512MB"
    )
    browser_mock.assert_called_once_with(html.resolve().as_uri())


def test_snapshot_only_retains_two_command_workflow():
    with (
        patch("s3_bucket_lens.cli.extract", return_value=Path("snapshot.duckdb")),
        patch("s3_bucket_lens.cli.build") as build_mock,
        patch("s3_bucket_lens.cli.webbrowser.open") as browser_mock,
    ):
        assert main(["extract", "example", "--snapshot-only"]) == 0
        build_mock.assert_not_called()
        browser_mock.assert_not_called()


def test_build_opens_browser_unless_disabled(tmp_path):
    site = tmp_path / "report"
    with (
        patch("s3_bucket_lens.cli.build", return_value=site / "index.html"),
        patch("s3_bucket_lens.cli.webbrowser.open") as browser_mock,
    ):
        assert main(["build", "snapshot.duckdb", "--output", str(site)]) == 0
        browser_mock.assert_called_once_with((site / "index.html").resolve().as_uri())


@pytest.mark.parametrize("command", ["extract", "build"])
def test_no_open_still_builds_report(tmp_path, command):
    snapshot = tmp_path / "snapshot.duckdb"
    html = tmp_path / "report" / "index.html"
    arguments = [command, "--no-open"]
    if command == "build":
        arguments.append(str(snapshot))
    with (
        patch("s3_bucket_lens.cli.extract", return_value=snapshot),
        patch("s3_bucket_lens.cli.build", return_value=html) as build_mock,
        patch("s3_bucket_lens.cli.webbrowser.open") as browser_mock,
    ):
        assert main(arguments) == 0
        build_mock.assert_called_once()
        browser_mock.assert_not_called()


def test_extract_uses_requested_report_directory(tmp_path):
    snapshot = tmp_path / "snapshot.duckdb"
    site = tmp_path / "chosen-report"
    with (
        patch("s3_bucket_lens.cli.extract", return_value=snapshot),
        patch("s3_bucket_lens.cli.build", return_value=site / "index.html") as build_mock,
    ):
        assert main(["extract", "--report-output", str(site), "--no-open"]) == 0
    build_mock.assert_called_once_with(snapshot, site, memory_limit="512MB")


def test_build_report_persists_after_command_returns(tmp_path):
    snapshot = create_demo(tmp_path / "demo.duckdb")
    site = tmp_path / "report"
    with patch("s3_bucket_lens.cli.webbrowser.open") as browser_mock:
        assert main(["build", str(snapshot), "--output", str(site)]) == 0
    assert (site / "index.html").is_file()
    assert (site / "manifest.js").is_file()
    browser_mock.assert_called_once_with((site / "index.html").resolve().as_uri())


def test_build_defaults_to_an_html_file_in_the_current_directory(tmp_path, monkeypatch):
    snapshot = create_demo(tmp_path / "demo.duckdb")
    monkeypatch.chdir(tmp_path)
    assert main(["build", str(snapshot), "--no-open"]) == 0
    assert (tmp_path / "s3-bucket-lens.html").is_file()
    assert (tmp_path / "s3-bucket-lens.report" / "manifest.js").is_file()


@pytest.mark.parametrize(
    "option,total,remaining",
    [("expected_objects", 1000, "3m 00s"), ("expected_pages", 100, "4m 00s")],
)
def test_catalogue_progress_estimates_remaining_listing_time(capsys, option, total, remaining):
    catalogue_progress({"objects": 250, "pages": 20, "elapsed_seconds": 60.0}, **{option: total})
    message = capsys.readouterr().err
    assert f"ETA ~{remaining}" in message
    assert "listing only" in message


def test_catalogue_progress_does_not_invent_an_unknown_total(capsys):
    catalogue_progress({"objects": 250, "pages": 20, "elapsed_seconds": 60.0})
    assert "ETA unavailable (total unknown)" in capsys.readouterr().err


def test_catalogue_progress_keeps_listing_when_estimated_total_is_reached(capsys):
    catalogue_progress({"objects": 250, "pages": 20, "elapsed_seconds": 60.0}, expected_objects=200)
    assert "estimated total reached; listing continues" in capsys.readouterr().err


@pytest.mark.parametrize("option", ["--expected-objects", "--expected-pages"])
@pytest.mark.parametrize("value", ["0", "-1", "unknown"])
def test_catalogue_estimate_totals_must_be_positive(option, value):
    with pytest.raises(SystemExit):
        parser().parse_args(["catalogue", "example", "--output", "new.catalogue", option, value])


def test_catalogue_accepts_only_one_estimate_basis():
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "catalogue",
                "example",
                "--output",
                "new.catalogue",
                "--expected-objects",
                "1000",
                "--expected-pages",
                "20",
            ]
        )


@pytest.mark.parametrize(
    "option,total,remaining",
    [("--expected-objects", "1000", "3m 00s"), ("--expected-pages", "100", "4m 00s")],
)
def test_catalogue_command_connects_estimate_to_progress(capsys, option, total, remaining):
    with patch("s3_bucket_lens.cli.catalogue", return_value={"complete": True}) as scan:
        assert main(["catalogue", "example", "--output", "new.catalogue", option, total]) == 0
    scan.call_args.kwargs["progress"]({"objects": 250, "pages": 20, "elapsed_seconds": 60.0})
    assert f"ETA ~{remaining}" in capsys.readouterr().err


@pytest.mark.parametrize(
    "statistics",
    [
        {"objects": 0, "pages": 1, "elapsed_seconds": 1.0},
        {"objects": 10, "pages": 1, "elapsed_seconds": 0.0},
    ],
)
def test_catalogue_progress_waits_for_measurable_throughput(capsys, statistics):
    catalogue_progress(statistics, expected_objects=1000)
    assert "collecting throughput" in capsys.readouterr().err


def test_catalogue_progress_formats_hour_scale_estimates(capsys):
    catalogue_progress(
        {"objects": 100, "pages": 10, "elapsed_seconds": 3600.0}, expected_objects=300
    )
    assert "ETA ~2h 00m" in capsys.readouterr().err
