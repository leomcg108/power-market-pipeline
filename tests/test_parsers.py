"""Tests for the ENTSO-E parser. Leo writes the implementation, these tests define it.

Fixtures are real API responses. See tests/fixtures/README.md for what each one holds.
Expected values below are copied from the fixture XML by hand.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from lxml import etree

from power_pipeline.entsoe.parsers import PARSED_COLUMNS, ParseError, parse_timeseries

FIXTURES = Path(__file__).parent / "fixtures"
CH = (FIXTURES / "prices_ch_2026-09-23.xml").read_bytes()
DE_LU_2026 = (FIXTURES / "prices_de_lu_2026-09-23.xml").read_bytes()
DE_LU_2024 = (FIXTURES / "prices_de_lu_2024-05-10.xml").read_bytes()
NO_DATA = (FIXTURES / "ack_no_data.xml").read_bytes()
PRICE_DOCUMENTS = {"ch_2026": CH, "de_lu_2026": DE_LU_2026, "de_lu_2024": DE_LU_2024}

HOUR = timedelta(hours=1)
QUARTER = timedelta(minutes=15)
STEPS = {"PT60M": HOUR, "PT15M": QUARTER}


def utc(*parts: int) -> pd.Timestamp:
    return pd.Timestamp(datetime(*parts, tzinfo=UTC))


def one_series(frame: pd.DataFrame, time_series_mrid: str) -> pd.DataFrame:
    rows = frame[frame["time_series_mrid"] == time_series_mrid]
    return rows.sort_values("interval_start_utc").reset_index(drop=True)


def value_at(series: pd.DataFrame, position: int) -> float:
    """Value of a position, counted from 1, in a series sorted by interval start."""
    return series["value"].iloc[position - 1]


# Derived documents: a real fixture with some Points removed or renumbered. They cover
# cases that no real response has shown so far. See tests/fixtures/README.md.
def without_points(xml: bytes, positions: set[int]) -> bytes:
    root = etree.fromstring(xml)
    ns = {"d": etree.QName(root).namespace}
    for point in root.findall(".//d:Point", ns):
        if int(point.findtext("d:position", namespaces=ns)) in positions:
            point.getparent().remove(point)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def point_count(xml: bytes) -> int:
    root = etree.fromstring(xml)
    return len(root.findall(".//{*}Point"))


def test_without_points_removes_only_the_given_positions():
    assert point_count(CH) == 24
    assert point_count(without_points(CH, {1, 24})) == 22


# Output contract


@pytest.mark.parametrize(
    "xml", [*PRICE_DOCUMENTS.values(), NO_DATA], ids=[*PRICE_DOCUMENTS.keys(), "no_data"]
)
def test_output_has_exactly_the_parsed_columns_in_order(xml):
    assert list(parse_timeseries(xml).columns) == PARSED_COLUMNS


def test_no_data_acknowledgement_gives_an_empty_frame():
    frame = parse_timeseries(NO_DATA)

    assert frame.empty
    assert list(frame.columns) == PARSED_COLUMNS


@pytest.mark.parametrize("xml", PRICE_DOCUMENTS.values(), ids=PRICE_DOCUMENTS.keys())
def test_column_types(xml):
    frame = parse_timeseries(xml)

    for column in ("interval_start_utc", "interval_end_utc", "created_datetime_utc"):
        assert isinstance(frame[column].dtype, pd.DatetimeTZDtype), column
        assert str(frame[column].dt.tz) == "UTC", column
    assert frame["value"].dtype == "float64"
    assert pd.api.types.is_integer_dtype(frame["revision_number"])
    assert frame["classification_sequence"].dtype == "Int64"
    for column in ("time_series_mrid", "resolution", "document_mrid"):
        assert all(isinstance(item, str) for item in frame[column]), column


@pytest.mark.parametrize("xml", PRICE_DOCUMENTS.values(), ids=PRICE_DOCUMENTS.keys())
def test_each_interval_ends_one_resolution_after_it_starts(xml):
    frame = parse_timeseries(xml)
    steps = frame["resolution"].map(STEPS)

    assert (frame["interval_end_utc"] - frame["interval_start_utc"] == steps).all()


@pytest.mark.parametrize("xml", PRICE_DOCUMENTS.values(), ids=PRICE_DOCUMENTS.keys())
def test_no_interval_appears_twice_in_a_series(xml):
    frame = parse_timeseries(xml)

    assert not frame.duplicated(["time_series_mrid", "interval_start_utc"]).any()


@pytest.mark.parametrize("xml", PRICE_DOCUMENTS.values(), ids=PRICE_DOCUMENTS.keys())
def test_each_series_covers_its_day_without_gaps(xml):
    frame = parse_timeseries(xml)

    for _, series in frame.groupby("time_series_mrid"):
        series = series.sort_values("interval_start_utc")
        starts = series["interval_start_utc"].reset_index(drop=True)
        ends = series["interval_end_utc"].reset_index(drop=True)
        assert (starts.iloc[1:].reset_index(drop=True) == ends.iloc[:-1]).all()
        assert ends.iloc[-1] - starts.iloc[0] == timedelta(hours=24)


# A full hourly day: CH, one series, all 24 Points present


def test_ch_hourly_day_has_24_rows_in_one_series():
    frame = parse_timeseries(CH)

    assert len(frame) == 24
    assert set(frame["time_series_mrid"]) == {"1"}
    assert set(frame["resolution"]) == {"PT60M"}
    assert frame["classification_sequence"].isna().all()


def test_ch_hourly_day_runs_from_local_midnight_to_local_midnight():
    series = one_series(parse_timeseries(CH), "1")

    assert series["interval_start_utc"].iloc[0] == utc(2026, 9, 22, 22)
    assert series["interval_end_utc"].iloc[-1] == utc(2026, 9, 23, 22)


def test_ch_hourly_day_keeps_the_prices():
    series = one_series(parse_timeseries(CH), "1")

    assert value_at(series, 1) == 194.59
    assert value_at(series, 14) == 78.0
    assert value_at(series, 24) == 190.79


def test_header_fields_are_copied_to_every_row():
    frame = parse_timeseries(CH)

    assert set(frame["document_mrid"]) == {"097f7d92202248c686768bceaa46c380"}
    assert set(frame["revision_number"]) == {1}
    assert set(frame["created_datetime_utc"]) == {utc(2026, 9, 24, 16, 45, 40)}


# A quarter-hourly day: DE-LU, SDAC and EXAA series


def test_de_lu_quarter_hourly_day_has_96_rows_per_series():
    frame = parse_timeseries(DE_LU_2026)

    assert len(frame) == 192
    assert frame.groupby("time_series_mrid").size().to_dict() == {"1": 96, "2": 96}
    assert set(frame["resolution"]) == {"PT15M"}


def test_classification_sequence_tells_sdac_and_exaa_apart():
    frame = parse_timeseries(DE_LU_2026)
    sequences = frame.groupby("time_series_mrid")["classification_sequence"].unique()

    assert list(sequences["1"]) == [2]  # EXAA
    assert list(sequences["2"]) == [1]  # SDAC


def test_de_lu_sdac_quarter_hours_keep_their_prices():
    sdac = one_series(parse_timeseries(DE_LU_2026), "2")

    assert sdac["interval_start_utc"].iloc[0] == utc(2026, 9, 22, 22)
    assert value_at(sdac, 1) == 194.51
    assert value_at(sdac, 96) == 144.2


# Omitted positions: the fill rule on real responses


def test_omitted_quarter_hours_take_the_previous_value():
    # EXAA on 2026-09-23 leaves out positions 9 and 14.
    exaa = one_series(parse_timeseries(DE_LU_2026), "1")

    assert value_at(exaa, 8) == 183.02
    assert value_at(exaa, 9) == 183.02
    assert value_at(exaa, 13) == 180.0
    assert value_at(exaa, 14) == 180.0


def test_points_after_a_gap_keep_their_own_value():
    exaa = one_series(parse_timeseries(DE_LU_2026), "1")

    assert value_at(exaa, 10) == 182.23
    assert value_at(exaa, 15) == 179.35


def test_omitted_hour_takes_the_previous_value():
    # SDAC on 2024-05-10 is hourly and leaves out position 15 (12:00 to 13:00 UTC).
    sdac = one_series(parse_timeseries(DE_LU_2024), "2")

    assert len(sdac) == 24
    assert set(sdac["resolution"]) == {"PT60M"}
    assert sdac["interval_start_utc"].iloc[14] == utc(2024, 5, 10, 12)
    assert value_at(sdac, 14) == 0.02
    assert value_at(sdac, 15) == 0.02
    assert value_at(sdac, 16) == 20.42


def test_hourly_and_quarter_hourly_series_in_one_document():
    frame = parse_timeseries(DE_LU_2024)
    resolutions = frame.groupby("time_series_mrid")["resolution"].unique()

    assert list(resolutions["1"]) == ["PT15M"]  # EXAA
    assert list(resolutions["2"]) == ["PT60M"]  # SDAC
    assert len(frame) == 96 + 24


def test_negative_prices_are_kept():
    exaa = one_series(parse_timeseries(DE_LU_2024), "1")

    assert exaa["value"].min() == -14.9
    assert (exaa["value"] < 0).sum() == 6


# Namespaces


def test_reads_the_namespace_from_the_root_element():
    other_version = CH.replace(b"publicationdocument:7:3", b"publicationdocument:7:9")
    assert other_version != CH

    pd.testing.assert_frame_equal(parse_timeseries(other_version), parse_timeseries(CH))


# Derived edge cases


def test_positions_missing_at_the_end_of_a_period_are_filled():
    # Derived: CH with positions 23 and 24 removed. Position 22 holds 212.1.
    series = one_series(parse_timeseries(without_points(CH, {23, 24})), "1")

    assert len(series) == 24
    assert value_at(series, 22) == 212.1
    assert value_at(series, 23) == 212.1
    assert value_at(series, 24) == 212.1
    assert series["interval_end_utc"].iloc[-1] == utc(2026, 9, 23, 22)


def test_raises_when_position_1_is_missing():
    # Derived: CH with position 1 removed. There is no earlier value to carry forward.
    with pytest.raises(ParseError):
        parse_timeseries(without_points(CH, {1}))


def test_raises_on_a_position_beyond_the_end_of_the_period():
    # Derived: CH with position 24 renumbered to 25, one past the last hour.
    renumbered = CH.replace(b"<position>24</position>", b"<position>25</position>")
    assert renumbered.count(b"<position>25</position>") == 1

    with pytest.raises(ParseError):
        parse_timeseries(renumbered)


def test_reads_quantity_when_a_point_has_no_price():
    # Derived: CH with price.amount renamed to quantity, as in load and generation documents.
    as_quantities = CH.replace(b"price.amount", b"quantity")

    frame = parse_timeseries(as_quantities)

    assert frame["value"].tolist() == parse_timeseries(CH)["value"].tolist()


# Guard rails: each derived document breaks one rule and must raise ParseError.
BROKEN_DOCUMENTS = {
    "not xml": b"<Publication_MarketDocument",
    "missing resolution": CH.replace(b"<resolution>PT60M</resolution>", b""),
    "unsupported resolution": CH.replace(
        b"<resolution>PT60M</resolution>", b"<resolution>P1D</resolution>"
    ),
    "timestamp not in UTC": CH.replace(b"2026-09-22T22:00Z", b"2026-09-23T00:00+02:00"),
    "period not a whole number of hours": CH.replace(b"2026-09-23T22:00Z", b"2026-09-23T22:30Z"),
    "position repeated": CH.replace(b"<position>2</position>", b"<position>1</position>"),
    "point without a value": CH.replace(b"<price.amount>194.59</price.amount>", b""),
}


@pytest.mark.parametrize("xml", BROKEN_DOCUMENTS.values(), ids=BROKEN_DOCUMENTS.keys())
def test_raises_parse_error_on_broken_documents(xml):
    assert xml != CH

    with pytest.raises(ParseError):
        parse_timeseries(xml)


# Load, generation and flows. Fixtures are real responses for 2026-09-23. The DE-LU
# generation file is trimmed to five series. See tests/fixtures/README.md.

LOAD_DE_LU = (FIXTURES / "load_de_lu_2026-09-23.xml").read_bytes()
GENERATION_CH = (FIXTURES / "generation_ch_2026-09-23.xml").read_bytes()
GENERATION_DE_LU = (FIXTURES / "generation_de_lu_2026-09-23_trimmed.xml").read_bytes()
FLOWS_CH_DE_LU = (FIXTURES / "flows_ch_de_lu_2026-09-23.xml").read_bytes()
QUANTITY_DOCUMENTS = {
    "load_de_lu": LOAD_DE_LU,
    "generation_ch": GENERATION_CH,
    "generation_de_lu": GENERATION_DE_LU,
    "flows_ch_de_lu": FLOWS_CH_DE_LU,
}
CH_EIC = "10YCH-SWISSGRIDZ"
DE_LU_EIC = "10Y1001A1001A82H"


def by_psr_type(frame: pd.DataFrame, psr_type: str) -> pd.DataFrame:
    rows = frame[frame["psr_type"] == psr_type]
    return rows.sort_values(["time_series_mrid", "interval_start_utc"]).reset_index(drop=True)


@pytest.mark.parametrize("xml", QUANTITY_DOCUMENTS.values(), ids=QUANTITY_DOCUMENTS.keys())
def test_quantity_documents_have_the_columns_and_megawatt_units(xml):
    frame = parse_timeseries(xml)

    assert list(frame.columns) == PARSED_COLUMNS
    assert set(frame["unit"]) == {"MAW"}
    assert frame["classification_sequence"].isna().all()


@pytest.mark.parametrize("xml", QUANTITY_DOCUMENTS.values(), ids=QUANTITY_DOCUMENTS.keys())
def test_quantity_documents_have_one_row_per_interval_per_series(xml):
    frame = parse_timeseries(xml)
    steps = frame["resolution"].map(STEPS)

    assert not frame.duplicated(["time_series_mrid", "interval_start_utc"]).any()
    assert (frame["interval_end_utc"] - frame["interval_start_utc"] == steps).all()


@pytest.mark.parametrize(
    "xml",
    [*PRICE_DOCUMENTS.values(), *QUANTITY_DOCUMENTS.values()],
    ids=[*PRICE_DOCUMENTS, *QUANTITY_DOCUMENTS],
)
def test_new_text_columns_hold_strings_or_are_missing(xml):
    frame = parse_timeseries(xml)

    for column in ("psr_type", "in_domain", "out_domain", "unit"):
        assert all(isinstance(item, str) or pd.isna(item) for item in frame[column]), column


def test_prices_carry_their_unit_and_both_domains():
    frame = parse_timeseries(DE_LU_2026)

    assert set(frame["unit"]) == {"EUR/MWH"}
    assert set(frame["in_domain"]) == {DE_LU_EIC}
    assert set(frame["out_domain"]) == {DE_LU_EIC}
    assert frame["psr_type"].isna().all()


def test_load_is_one_quarter_hourly_series_tagged_with_the_out_domain():
    frame = parse_timeseries(LOAD_DE_LU)

    assert len(frame) == 96
    assert set(frame["resolution"]) == {"PT15M"}
    assert set(frame["out_domain"]) == {DE_LU_EIC}
    assert frame["in_domain"].isna().all()
    assert frame["psr_type"].isna().all()
    assert frame["value"].min() == 43830.63256
    assert frame["value"].max() == 62836.65418


def test_generation_has_one_hourly_series_per_production_type():
    frame = parse_timeseries(GENERATION_CH)

    assert len(frame) == 6 * 24
    assert frame.groupby("psr_type").size().to_dict() == {
        "B10": 24,
        "B11": 24,
        "B12": 24,
        "B14": 24,
        "B16": 24,
        "B19": 24,
    }
    assert set(frame["resolution"]) == {"PT60M"}
    assert set(frame["in_domain"]) == {CH_EIC}
    assert frame["out_domain"].isna().all()


def test_omitted_night_time_zeros_are_filled():
    # CH solar sends 15 of 24 Points. The repeated night-time zeros are left out.
    solar = by_psr_type(parse_timeseries(GENERATION_CH), "B16")

    assert len(solar) == 24
    assert (solar["value"] == 0).sum() == 11
    assert solar["value"].max() == 3167.783447


def test_generation_and_consumption_are_told_apart_by_domain():
    # DE-LU pumped storage (B10) has two series: generation tagged with the in domain,
    # and pumping consumption tagged with the out domain.
    pumped = by_psr_type(parse_timeseries(GENERATION_DE_LU), "B10")
    generation = pumped[pumped["in_domain"] == DE_LU_EIC]
    consumption = pumped[pumped["out_domain"] == DE_LU_EIC]

    assert len(generation) == 96
    assert len(consumption) == 96
    assert generation["out_domain"].isna().all()
    assert consumption["in_domain"].isna().all()
    assert set(generation["time_series_mrid"]) != set(consumption["time_series_mrid"])


def test_a_gap_between_periods_stays_missing():
    # DE-LU waste (B17) has two Periods, 22:00 to 07:45 and 08:00 to 22:00 UTC.
    waste = by_psr_type(parse_timeseries(GENERATION_DE_LU), "B17")
    starts = set(waste["interval_start_utc"])

    assert len(waste) == 95
    assert utc(2026, 9, 23, 7, 30) in starts
    assert utc(2026, 9, 23, 7, 45) not in starts
    assert utc(2026, 9, 23, 8, 0) in starts


def test_a_series_with_one_point_repeats_it_all_day():
    # DE-LU geothermal (B09) sends a single Point for the whole day.
    geothermal = by_psr_type(parse_timeseries(GENERATION_DE_LU), "B09")

    assert len(geothermal) == 96
    assert set(geothermal["value"]) == {19.00702}


def test_flows_record_their_direction_in_the_domains():
    frame = parse_timeseries(FLOWS_CH_DE_LU)

    assert len(frame) == 96
    assert set(frame["out_domain"]) == {CH_EIC}
    assert set(frame["in_domain"]) == {DE_LU_EIC}
    assert frame["psr_type"].isna().all()
    assert set(frame["resolution"]) == {"PT15M"}


def test_flows_keep_zero_values_including_omitted_repeats():
    # CH to DE-LU sends 92 of 96 Points. Some of the left-out Points repeat a zero.
    frame = parse_timeseries(FLOWS_CH_DE_LU)

    assert (frame["value"] == 0).sum() == 7
    assert frame["value"].max() == 3176.15002
