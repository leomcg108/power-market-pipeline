from datetime import UTC, datetime
from fnmatch import fnmatchcase
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from power_pipeline import ingest

WINDOW_START = datetime(2026, 9, 22, 22, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 9, 23, 22, 0, tzinfo=UTC)
INGESTED_AT = datetime(2026, 9, 24, 14, 30, 12, 345678, tzinfo=UTC)


def relative_path(**overrides):
    args = {
        "dataset": "prices",
        "zone": "DE_LU",
        "window_start_utc": WINDOW_START,
        "window_end_utc": WINDOW_END,
        "ingested_at_utc": INGESTED_AT,
    }
    return ingest.landing_relative_path(**(args | overrides))


def test_landing_relative_path():
    assert relative_path() == (
        "entsoe/prices/DE_LU/2026/09/20260922T2200Z_20260923T2200Z_20260924T143012Z"
    )


def test_raw_and_parsed_volume_paths():
    rel = relative_path()

    assert ingest.raw_volume_path(rel) == (
        "/Volumes/workspace/power_raw/landing/entsoe/prices/DE_LU/2026/09/"
        "20260922T2200Z_20260923T2200Z_20260924T143012Z.xml.gz"
    )
    assert ingest.parsed_volume_path(rel) == (
        "/Volumes/workspace/power_raw/landing/parsed/entsoe/prices/DE_LU/2026/09/"
        "20260922T2200Z_20260923T2200Z_20260924T143012Z.parquet"
    )


def test_landing_relative_path_converts_other_time_zones_to_utc():
    zurich = ZoneInfo("Europe/Zurich")

    rel = relative_path(
        window_start_utc=datetime(2026, 9, 23, 0, 0, tzinfo=zurich),
        window_end_utc=datetime(2026, 9, 24, 0, 0, tzinfo=zurich),
    )

    assert "/2026/09/20260922T2200Z_20260923T2200Z_" in rel


def test_landing_folder_uses_the_utc_month_of_the_window_start():
    # Delivery day 1 October 2026 in Zurich starts at 22:00 UTC on 30 September.
    rel = relative_path(
        window_start_utc=datetime(2026, 9, 30, 22, 0, tzinfo=UTC),
        window_end_utc=datetime(2026, 10, 1, 22, 0, tzinfo=UTC),
    )

    assert rel.startswith("entsoe/prices/DE_LU/2026/09/20260930T2200Z_")


@pytest.mark.parametrize("field", ["window_start_utc", "window_end_utc", "ingested_at_utc"])
def test_landing_relative_path_rejects_naive_datetimes(field):
    with pytest.raises(ValueError, match=f"{field} must be timezone-aware"):
        relative_path(**{field: datetime(2026, 9, 23)})


@pytest.mark.parametrize(("field", "value"), [("dataset", "prices/../x"), ("zone", "DE-LU")])
def test_landing_relative_path_rejects_unsafe_names(field, value):
    with pytest.raises(ValueError, match="letters, digits and underscores"):
        relative_path(**{field: value})


def test_parsed_paths_match_the_copy_into_pattern():
    parsed = ingest.parsed_volume_path(relative_path())
    source_dir = f"{ingest.PARSED_VOLUME_DIR}/entsoe/prices/"

    assert parsed.startswith(source_dir)
    assert fnmatchcase(parsed.removeprefix(source_dir), ingest.PARSED_FILE_PATTERN)


def bronze_prices_frame(rows=2):
    starts = pd.date_range("2026-09-22 22:00", periods=rows, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "dataset": "prices",
            "zone": "DE_LU",
            "psr_type": None,
            "resolution": "PT15M",
            "interval_start_utc": starts,
            "interval_end_utc": starts + pd.Timedelta(minutes=15),
            "value": [-5.25, 101.5][:rows],
            "unit": "EUR/MWH",
            "document_mrid": "doc-1",
            "time_series_mrid": "1",
            "revision_number": 1,
            "created_datetime_utc": pd.Timestamp("2026-09-22 10:57", tz="UTC"),
            "ingested_at_utc": pd.Timestamp(INGESTED_AT),
            "source_file": "/Volumes/workspace/power_raw/landing/entsoe/prices/x.xml.gz",
            "request_params": '{"documentType": "A44"}',
        }
    )


def test_write_parquet_uses_the_bronze_schema(tmp_path):
    path = tmp_path / "nested" / "prices.parquet"

    ingest.write_parquet(bronze_prices_frame(), path, ingest.BRONZE_PRICES_SCHEMA)

    assert pq.read_schema(path).remove_metadata() == ingest.BRONZE_PRICES_SCHEMA


def test_write_parquet_keeps_values_and_utc_microsecond_timestamps(tmp_path):
    path = tmp_path / "prices.parquet"
    frame = bronze_prices_frame()

    ingest.write_parquet(frame, path, ingest.BRONZE_PRICES_SCHEMA)
    table = pq.read_table(path)

    assert table.schema.field("interval_start_utc").type == pa.timestamp("us", tz="UTC")
    assert table.column("value").to_pylist() == [-5.25, 101.5]
    assert table.column("ingested_at_utc").to_pylist()[0] == INGESTED_AT
    assert table.column("psr_type").to_pylist() == [None, None]


def test_write_parquet_rejects_missing_or_extra_columns(tmp_path):
    frame = bronze_prices_frame().drop(columns=["unit"]).assign(surprise=1)

    with pytest.raises(ValueError, match="Missing: {'unit'}. Extra: {'surprise'}"):
        ingest.write_parquet(frame, tmp_path / "x.parquet", ingest.BRONZE_PRICES_SCHEMA)


def test_write_parquet_rejects_naive_timestamps(tmp_path):
    frame = bronze_prices_frame()
    frame["interval_start_utc"] = frame["interval_start_utc"].dt.tz_localize(None)

    with pytest.raises(ValueError, match="interval_start_utc must hold UTC timestamps"):
        ingest.write_parquet(frame, tmp_path / "x.parquet", ingest.BRONZE_PRICES_SCHEMA)


def test_write_parquet_rejects_timestamps_in_other_time_zones(tmp_path):
    frame = bronze_prices_frame()
    frame["created_datetime_utc"] = frame["created_datetime_utc"].dt.tz_convert("Europe/Zurich")

    with pytest.raises(ValueError, match="created_datetime_utc must hold UTC timestamps"):
        ingest.write_parquet(frame, tmp_path / "x.parquet", ingest.BRONZE_PRICES_SCHEMA)


def test_setup_statements_create_schema_then_volume_then_bronze_table():
    schema_sql, volume_sql, table_sql = ingest.setup_statements()

    assert schema_sql.startswith("CREATE SCHEMA IF NOT EXISTS workspace.power_raw ")
    assert volume_sql.startswith("CREATE VOLUME IF NOT EXISTS workspace.power_raw.landing ")
    assert table_sql.startswith("CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_prices (")


def test_bronze_table_has_every_schema_column_and_is_append_only():
    table_sql = ingest.setup_statements()[2]

    for name in ingest.BRONZE_PRICES_SCHEMA.names:
        assert f"    {name} " in table_sql
    assert "'delta.appendOnly' = 'true'" in table_sql


def test_copy_into_bronze_sql_reads_the_dataset_folder():
    assert ingest.copy_into_bronze_sql("prices") == (
        "COPY INTO workspace.power_raw.bronze_prices\n"
        "FROM '/Volumes/workspace/power_raw/landing/parsed/entsoe/prices'\n"
        "FILEFORMAT = PARQUET\n"
        "PATTERN = '*/*/*/*.parquet'"
    )


def test_load_bronze_returns_the_number_of_inserted_rows(monkeypatch):
    calls = []

    def fake_run_sql(statement, warehouse_id, client=None):
        calls.append((statement, warehouse_id))
        return [{"num_affected_rows": "96", "num_inserted_rows": "96"}]

    monkeypatch.setattr(ingest.dbx, "run_sql", fake_run_sql)

    assert ingest.load_bronze("prices", "wh-1") == 96
    assert calls == [(ingest.copy_into_bronze_sql("prices"), "wh-1")]
