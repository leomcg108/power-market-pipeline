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
