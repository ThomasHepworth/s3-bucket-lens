"""Stream observed successful object GETs from existing S3 server access logs."""

from __future__ import annotations

import gzip
import io
import re
from contextlib import closing
from datetime import datetime
from urllib.parse import unquote, urlsplit

RECORD = re.compile(
    r"^\S+ (?P<bucket>\S+) \[(?P<time>[^\]]+)\] \S+ \S+ \S+ "
    r'(?P<operation>\S+) (?P<key>\S+) ".*?" (?P<status>\d{3}) '
)
GET_OPERATIONS = {"REST.GET.OBJECT", "WEBSITE.GET.OBJECT"}


def parse_access_record(line: str, bucket: str):
    match = RECORD.match(line)
    if not match or match["bucket"] != bucket or match["operation"] not in GET_OPERATIONS:
        return None
    if int(match["status"]) not in {200, 206, 304} or match["key"] == "-":
        return None
    try:
        timestamp = datetime.strptime(match["time"], "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        return None
    return unquote(match["key"]), timestamp


def import_access_logs(client, connection, uri: str, bucket: str) -> dict:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(
            "--access-logs must be an s3://bucket/prefix URI without a query or fragment."
        )
    log_bucket = parsed.netloc
    prefix = unquote(parsed.path[1:])
    connection.execute("CREATE TEMP TABLE access_events (key VARCHAR, accessed TIMESTAMPTZ)")
    batch = []
    file_count = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=log_bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith("/"):
                continue
            with closing(client.get_object(Bucket=log_bucket, Key=item["Key"])["Body"]) as body:
                raw = gzip.GzipFile(fileobj=body) if item["Key"].endswith(".gz") else body
                with io.TextIOWrapper(raw, encoding="utf-8", errors="replace") as stream:
                    for line in stream:
                        event = parse_access_record(line, bucket)
                        if event:
                            batch.append(event)
                        if len(batch) == 1000:
                            insert_access_batch(connection, batch)
                            batch.clear()
            file_count += 1
    if batch:
        insert_access_batch(connection, batch)
    connection.execute("""
        UPDATE objects SET last_accessed = events.accessed FROM (
            SELECT key, max(accessed) AS accessed FROM access_events GROUP BY key
        ) events WHERE objects.key = events.key
            AND objects.is_latest AND NOT objects.is_delete_marker
    """)
    events, first, last = connection.execute(
        "SELECT count(*), min(accessed), max(accessed) FROM access_events"
    ).fetchone()
    connection.execute("DROP TABLE access_events")
    return {
        "source": uri,
        "log_files": file_count,
        "successful_get_events": events,
        "first_observed_get": first,
        "last_observed_get": last,
    }


def insert_access_batch(connection, rows) -> None:
    connection.execute(
        "INSERT INTO access_events SELECT unnest(?::VARCHAR[]), unnest(?::TIMESTAMPTZ[])",
        list(zip(*rows, strict=True)),
    )
