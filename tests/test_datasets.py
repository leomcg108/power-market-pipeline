from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from power_pipeline.entsoe import datasets

WINDOW_START = datetime(2026, 9, 22, 22, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 9, 23, 22, 0, tzinfo=UTC)


def prices_params(zone="DE_LU", start=WINDOW_START, end=WINDOW_END):
    dataset = datasets.load_datasets()["prices"]
    return datasets.build_params(dataset, datasets.load_zones()[zone], start, end)


def test_load_zones_reads_both_zones_with_their_eic_codes():
    zones = datasets.load_zones()

    assert zones == {
        "CH": datasets.Zone(code="CH", name="Switzerland", eic="10YCH-SWISSGRIDZ"),
        "DE_LU": datasets.Zone(code="DE_LU", name="Germany-Luxembourg", eic="10Y1001A1001A82H"),
    }


def test_build_params_for_de_lu_day_ahead_prices():
    assert prices_params() == {
        "documentType": "A44",
        "in_Domain": "10Y1001A1001A82H",
        "out_Domain": "10Y1001A1001A82H",
        "contract_MarketAgreement.type": "A01",
        "periodStart": "202609222200",
        "periodEnd": "202609232200",
    }


def test_build_params_for_ch_uses_the_swiss_eic_code_for_both_domains():
    params = prices_params(zone="CH")

    assert params["in_Domain"] == params["out_Domain"] == "10YCH-SWISSGRIDZ"


def test_build_params_never_contains_the_token():
    assert "securityToken" not in prices_params()


def test_format_period_converts_to_utc():
    local_midnight = datetime(2026, 9, 23, 0, 0, tzinfo=ZoneInfo("Europe/Zurich"))

    assert datasets.format_period(local_midnight) == "202609222200"


def test_format_period_rejects_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        datasets.format_period(datetime(2026, 9, 22, 22, 0))


def test_format_period_rejects_times_that_are_not_whole_minutes():
    with pytest.raises(ValueError, match="whole minute"):
        datasets.format_period(datetime(2026, 9, 22, 22, 0, 30, tzinfo=UTC))


@pytest.mark.parametrize("end", [WINDOW_START, datetime(2026, 9, 21, 22, 0, tzinfo=UTC)])
def test_build_params_rejects_empty_or_reversed_windows(end):
    with pytest.raises(ValueError, match="must end after it starts"):
        prices_params(end=end)


def test_build_params_rejects_unknown_placeholders():
    dataset = datasets.Dataset(name="flows", description="", params={"in_Domain": "{border}"})
    zone = datasets.load_zones()["CH"]

    with pytest.raises(ValueError, match="Unknown placeholder 'border' in flows param in_Domain"):
        datasets.build_params(dataset, zone, WINDOW_START, WINDOW_END)
