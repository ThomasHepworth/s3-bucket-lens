"""Storage-only estimates, including minimum billable sizes and archive metadata."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import ijson

GIB = 1024**3
MINIMUM_SIZE = {"STANDARD_IA": 128 * 1024, "ONEZONE_IA": 128 * 1024, "GLACIER_IR": 128 * 1024}
ARCHIVE_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}


def validate_rates(rates: Mapping[str, float]) -> dict[str, float]:
    result = {storage_class: float(rate) for storage_class, rate in rates.items()}
    if not result or any(not math.isfinite(rate) or rate < 0 for rate in result.values()):
        raise ValueError("Storage rates must be finite, non-negative USD per GiB-month.")
    return result


def estimate_storage(size: int, storage_class: str, rates: Mapping[str, float]) -> float | None:
    """Return monthly USD, or unknown when the class/tier cannot be priced safely."""
    if storage_class == "INTELLIGENT_TIERING" or storage_class not in rates:
        return None
    billable = max(size, MINIMUM_SIZE.get(storage_class, 0))
    cost = billable / GIB * rates[storage_class]
    if storage_class in ARCHIVE_CLASSES:
        if "STANDARD" not in rates:
            return None
        cost += 32 * 1024 / GIB * rates[storage_class]
        cost += 8 * 1024 / GIB * rates["STANDARD"]
    return cost


def load_rates(region: str, path: Path | None = None) -> dict:
    if path:
        document = json.loads(path.read_text(encoding="utf-8"))
        if document.get("currency") != "USD" or document.get("region") != region:
            raise ValueError("Pricing must specify currency USD and the bucket's region.")
        document["rates"] = validate_rates(document["rates"])
        document["source"] = f"Custom price file: {path.name}"
        return document
    resource = files("s3_bucket_lens").joinpath("data", "pricing.json")
    if resource.is_file():
        catalog = json.loads(resource.read_text(encoding="utf-8"))
    else:
        catalog = json.loads(
            (Path(__file__).resolve().parents[1] / "data" / "pricing.json").read_text(
                encoding="utf-8"
            )
        )
    if catalog.get("schema_version") != 1 or catalog.get("currency") != "USD":
        raise ValueError("Unsupported bundled pricing catalogue.")
    if region not in catalog["regions"]:
        raise ValueError(f"No bundled prices for {region}. Refresh pricing or use --pricing-file.")
    document = dict(catalog["regions"][region])
    document["rates"] = validate_rates(document["rates"])
    document["bundled"] = True
    return document


def fetch_rates(region: str) -> dict:
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-\d+", region):
        raise ValueError("Invalid AWS region.")
    sources = []
    rates = {}
    for service in ("AmazonS3", "AmazonS3GlacierDeepArchive"):
        url = (
            f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/"
            f"{service}/current/{region}/index.json"
        )
        with tempfile.TemporaryFile() as handle:
            try:
                with urlopen(url, timeout=60) as response:
                    while chunk := response.read(1024 * 1024):
                        handle.write(chunk)
            except HTTPError as exc:
                if service == "AmazonS3GlacierDeepArchive" and exc.code == 404:
                    continue
                raise
            handle.seek(0)
            rates.update(parse_offer(handle))
        sources.append(url)
    if "STANDARD" not in rates:
        raise ValueError("AWS price list has no Standard storage rate; use --pricing-file.")
    return {
        "region": region,
        "currency": "USD",
        "rates": validate_rates(rates),
        "source": sources[0],
        "sources": sources,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "basis": "Public on-demand first-volume-band storage rates; no account discounts.",
    }


def parse_offer(handle) -> dict[str, float]:
    usage_classes = {
        "TimedStorage-ByteHrs": "STANDARD",
        "TimedStorage-SIA-ByteHrs": "STANDARD_IA",
        "TimedStorage-ZIA-ByteHrs": "ONEZONE_IA",
        "TimedStorage-GIR-ByteHrs": "GLACIER_IR",
        "TimedStorage-GlacierByteHrs": "GLACIER",
        "TimedStorage-GDA-ByteHrs": "DEEP_ARCHIVE",
        "TimedStorage-RRS-ByteHrs": "REDUCED_REDUNDANCY",
    }
    products = {}
    for sku, product in ijson.kvitems(handle, "products"):
        attributes = product.get("attributes", {})
        usage = attributes.get("usagetype", "")
        storage_class = usage_classes.get(re.sub(r"^[A-Z][A-Z0-9]*-", "", usage))
        if storage_class and product.get("productFamily") in {"Storage", None}:
            products[sku] = storage_class
    handle.seek(0)
    rates = {}
    for sku, terms in ijson.kvitems(handle, "terms.OnDemand"):
        if sku not in products:
            continue
        for term in terms.values():
            for dimension in term.get("priceDimensions", {}).values():
                if dimension.get("unit") == "GB-Mo" and dimension.get("beginRange") == "0":
                    rates[products[sku]] = float(dimension["pricePerUnit"]["USD"])
    if not rates:
        raise ValueError("AWS price list has no recognised storage rates; use --pricing-file.")
    return rates


def refresh_catalogue(output: Path, regions: list[str] | None = None) -> None:
    if not regions:
        url = (
            "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/"
            "AmazonS3/current/region_index.json"
        )
        with urlopen(url, timeout=60) as response:
            regions = sorted(
                region
                for region in json.load(response)["regions"]
                if re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-\d+", region)
            )
        catalog = {"schema_version": 1, "currency": "USD", "regions": {}}
    else:
        catalog = (
            json.loads(output.read_text(encoding="utf-8"))
            if output.exists()
            else {"schema_version": 1, "currency": "USD", "regions": {}}
        )
    for region in regions:
        print(f"Refreshing {region}...", flush=True)
        catalog["regions"][region] = fetch_rates(region)
    catalog["generated_at"] = datetime.now(UTC).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=output.parent, suffix=".json", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(catalog, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, output)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
    print(f"Saved {len(catalog['regions'])} regions to {output}", flush=True)


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description="Refresh packaged AWS regional storage prices.")
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument(
        "--region",
        action="append",
        dest="regions",
        help="Update only this region; repeat to select several. Default: all.",
    )
    options = arguments.parse_args()
    refresh_catalogue(options.output, options.regions)
