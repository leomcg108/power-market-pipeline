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


def zone_params(dataset_name, zone):
    dataset = datasets.load_datasets()[dataset_name]
    return datasets.build_params(dataset, datasets.load_zones()[zone], WINDOW_START, WINDOW_END)


def test_load_datasets_defines_all_four_datasets_with_their_scope():
    scopes = {name: d.scope for name, d in datasets.load_datasets().items()}

    assert scopes == {"prices": "zone", "load": "zone", "generation": "zone", "flows": "border"}


def test_load_borders_reads_both_directions():
    assert datasets.load_borders() == [
        datasets.Border(from_zone="CH", to_zone="DE_LU"),
        datasets.Border(from_zone="DE_LU", to_zone="CH"),
    ]


def test_build_params_for_de_lu_actual_load():
    assert zone_params("load", "DE_LU") == {
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": "10Y1001A1001A82H",
        "periodStart": "202609222200",
        "periodEnd": "202609232200",
    }


def test_build_params_for_ch_actual_generation():
    assert zone_params("generation", "CH") == {
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": "10YCH-SWISSGRIDZ",
        "periodStart": "202609222200",
        "periodEnd": "202609232200",
    }


@pytest.mark.parametrize(
    ("from_zone", "to_zone", "out_domain", "in_domain"),
    [
        ("CH", "DE_LU", "10YCH-SWISSGRIDZ", "10Y1001A1001A82H"),
        ("DE_LU", "CH", "10Y1001A1001A82H", "10YCH-SWISSGRIDZ"),
    ],
)
def test_build_border_params_for_flows_sets_out_as_from_and_in_as_to(
    from_zone, to_zone, out_domain, in_domain
):
    zones = datasets.load_zones()
    flows = datasets.load_datasets()["flows"]

    params = datasets.build_border_params(
        flows, zones[from_zone], zones[to_zone], WINDOW_START, WINDOW_END
    )

    assert params == {
        "documentType": "A11",
        "out_Domain": out_domain,
        "in_Domain": in_domain,
        "periodStart": "202609222200",
        "periodEnd": "202609232200",
    }


def test_build_params_refuses_a_border_dataset():
    flows = datasets.load_datasets()["flows"]

    with pytest.raises(ValueError, match="flows is requested per border"):
        datasets.build_params(flows, datasets.load_zones()["CH"], WINDOW_START, WINDOW_END)


def test_build_border_params_refuses_a_zone_dataset():
    zones = datasets.load_zones()
    prices = datasets.load_datasets()["prices"]

    with pytest.raises(ValueError, match="prices is requested per zone"):
        datasets.build_border_params(prices, zones["CH"], zones["DE_LU"], WINDOW_START, WINDOW_END)


def test_load_datasets_rejects_an_unknown_scope(tmp_path):
    (tmp_path / "datasets.yaml").write_text(
        "datasets:\n  x:\n    description: X\n    scope: country\n    params: {}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown scope 'country'"):
        datasets.load_datasets(tmp_path)
