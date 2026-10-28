import io
import json
import math

import pytest

from s3_bucket_lens.pricing import (
    GIB,
    estimate_storage,
    load_rates,
    parse_offer,
    refresh_catalogue,
    validate_rates,
)


def test_standard_and_infrequent_access_minimum_size():
    rates = {"STANDARD": 0.023, "STANDARD_IA": 0.0125}
    assert estimate_storage(GIB, "STANDARD", rates) == 0.023
    assert estimate_storage(1, "STANDARD_IA", rates) == 128 * 1024 / GIB * 0.0125


def test_archive_metadata_is_billed_in_two_classes():
    rates = {"STANDARD": 0.023, "DEEP_ARCHIVE": 0.00099}
    expected = (100 + 32 * 1024) / GIB * 0.00099 + 8 * 1024 / GIB * 0.023
    assert estimate_storage(100, "DEEP_ARCHIVE", rates) == expected


def test_unknown_tier_is_not_silently_priced_as_standard():
    assert estimate_storage(GIB, "INTELLIGENT_TIERING", {"STANDARD": 0.023}) is None
    assert estimate_storage(GIB, "FUTURE_CLASS", {"STANDARD": 0.023}) is None


@pytest.mark.parametrize("rate", [-1, math.inf, math.nan])
def test_invalid_rates_are_rejected(rate):
    with pytest.raises(ValueError):
        validate_rates({"STANDARD": rate})


def test_live_price_parser_excludes_archive_staging_and_volume_discounts():
    products = {}
    terms = {}
    for sku, usage, rate in [
        ("standard", "TimedStorage-ByteHrs", "0.024"),
        ("deep", "EUW2-TimedStorage-GDA-ByteHrs", "0.0018"),
        ("staging", "EUW2-TimedStorage-GDA-Staging", "0.022"),
        ("files", "EUW2-Files-TimedStorage-ByteHrs", "0.33"),
    ]:
        products[sku] = {"productFamily": "Storage", "attributes": {"usagetype": usage}}
        terms[sku] = {
            "term": {
                "priceDimensions": {
                    "first": {"unit": "GB-Mo", "beginRange": "0", "pricePerUnit": {"USD": rate}},
                    "discount": {
                        "unit": "GB-Mo",
                        "beginRange": "51200",
                        "pricePerUnit": {"USD": "0.0001"},
                    },
                }
            }
        }
    handle = io.BytesIO(json.dumps({"products": products, "terms": {"OnDemand": terms}}).encode())
    assert parse_offer(handle) == {"STANDARD": 0.024, "DEEP_ARCHIVE": 0.0018}


def test_separate_deep_archive_offer_has_no_product_family():
    offer = {
        "products": {"deep": {"attributes": {"usagetype": "EUW2-TimedStorage-GDA-ByteHrs"}}},
        "terms": {
            "OnDemand": {
                "deep": {
                    "term": {
                        "priceDimensions": {
                            "storage": {
                                "unit": "GB-Mo",
                                "beginRange": "0",
                                "pricePerUnit": {"USD": "0.0018"},
                            }
                        }
                    }
                }
            }
        },
    }
    assert parse_offer(io.BytesIO(json.dumps(offer).encode())) == {"DEEP_ARCHIVE": 0.0018}


def test_bundled_regional_prices_do_not_need_network(monkeypatch):
    def unexpected_network(*args, **kwargs):
        raise AssertionError("Bundled pricing must not contact AWS")

    monkeypatch.setattr("s3_bucket_lens.pricing.urlopen", unexpected_network)
    london = load_rates("eu-west-2")
    virginia = load_rates("us-east-1")
    assert london["bundled"] is True
    assert london["region"] == "eu-west-2"
    assert london["rates"]["STANDARD"] == 0.024
    assert london["rates"]["DEEP_ARCHIVE"] == 0.0018
    assert virginia["rates"]["STANDARD"] == 0.023
    with pytest.raises(ValueError, match="No bundled prices"):
        load_rates("not-a-region")


def test_failed_refresh_preserves_existing_catalogue(tmp_path, monkeypatch):
    output = tmp_path / "pricing.json"
    original = '{"schema_version":1,"currency":"USD","regions":{}}'
    output.write_text(original)

    def fail(region):
        raise RuntimeError("Price download failed")

    monkeypatch.setattr("s3_bucket_lens.pricing.fetch_rates", fail)
    with pytest.raises(RuntimeError, match="Price download failed"):
        refresh_catalogue(output, ["eu-west-2"])
    assert output.read_text() == original
