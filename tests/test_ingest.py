import gzip
import json
from datetime import UTC, date, datetime, timedelta
from fnmatch import fnmatchcase
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from power_pipeline import ingest
from power_pipeline.entsoe import client as entsoe
from power_pipeline.entsoe.parsers import parse_timeseries

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
            "classification_sequence": pd.array([1] * rows, dtype="Int64"),
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


@pytest.mark.parametrize(
    ("delivery_date", "start", "hours"),
    [
        (date(2026, 9, 23), datetime(2026, 9, 22, 22, 0, tzinfo=UTC), 24),  # summer time
        (date(2026, 1, 15), datetime(2026, 1, 14, 23, 0, tzinfo=UTC), 24),  # winter time
        (date(2024, 3, 31), datetime(2024, 3, 30, 23, 0, tzinfo=UTC), 23),  # clocks go forward
        (date(2024, 10, 27), datetime(2024, 10, 26, 22, 0, tzinfo=UTC), 25),  # clocks go back
        (date(2025, 3, 30), datetime(2025, 3, 29, 23, 0, tzinfo=UTC), 23),
        (date(2025, 10, 26), datetime(2025, 10, 25, 22, 0, tzinfo=UTC), 25),
        (date(2026, 3, 29), datetime(2026, 3, 28, 23, 0, tzinfo=UTC), 23),
        (date(2026, 10, 25), datetime(2026, 10, 24, 22, 0, tzinfo=UTC), 25),
    ],
)
def test_delivery_day_window_utc(delivery_date, start, hours):
    window_start, window_end = ingest.delivery_day_window_utc(delivery_date)

    assert window_start == start
    assert window_end - window_start == timedelta(hours=hours)
    assert window_start.tzinfo == UTC


def test_local_raw_path(tmp_path):
    assert ingest.local_raw_path(tmp_path, relative_path()) == (
        tmp_path
        / "raw/entsoe/prices/DE_LU/2026/09/20260922T2200Z_20260923T2200Z_20260924T143012Z.xml.gz"
    )


def test_write_raw_file_gzips_the_exact_bytes(tmp_path):
    content = b'<?xml version="1.0"?>\r\n<Doc>\xc3\xa9 CRLF and UTF-8 bytes kept as-is</Doc>\n'
    path = tmp_path / "nested" / "response.xml.gz"

    ingest.write_raw_file(content, path)

    assert gzip.decompress(path.read_bytes()) == content


def test_write_raw_file_never_overwrites(tmp_path):
    path = tmp_path / "response.xml.gz"
    ingest.write_raw_file(b"first", path)

    with pytest.raises(FileExistsError):
        ingest.write_raw_file(b"second", path)

    assert gzip.decompress(path.read_bytes()) == b"first"


FIXTURES = Path(__file__).parent / "fixtures"
PRICES_DE_LU = (FIXTURES / "prices_de_lu_2026-09-23.xml").read_bytes()
PRICES_CH = (FIXTURES / "prices_ch_2026-09-23.xml").read_bytes()
NO_DATA = (FIXTURES / "ack_no_data.xml").read_bytes()
REQUEST_PARAMS = {"documentType": "A44", "periodStart": "202609222200"}
RAW_FILE = "/Volumes/workspace/power_raw/landing/entsoe/prices/DE_LU/2026/09/x.xml.gz"


def to_bronze(xml=PRICES_DE_LU, **overrides):
    args = {
        "dataset": "prices",
        "zone": "DE_LU",
        "ingested_at_utc": INGESTED_AT,
        "source_file": RAW_FILE,
        "request_params": REQUEST_PARAMS,
    }
    return ingest.to_bronze_frame(parse_timeseries(xml), **(args | overrides))


def test_bronze_schema_ends_with_the_classification_sequence():
    assert ingest.BRONZE_PRICES_SCHEMA.names[-1] == "classification_sequence"
    assert ingest.BRONZE_PRICES_SCHEMA.field("classification_sequence").type == pa.int32()


def test_to_bronze_frame_has_exactly_the_bronze_columns():
    assert list(to_bronze().columns) == ingest.BRONZE_PRICES_SCHEMA.names


def test_to_bronze_frame_adds_the_lineage_every_raw_record_must_carry():
    frame = to_bronze()

    assert len(frame) == 192
    assert set(frame["dataset"]) == {"prices"}
    assert set(frame["zone"]) == {"DE_LU"}
    assert set(frame["ingested_at_utc"]) == {pd.Timestamp(INGESTED_AT)}
    assert set(frame["source_file"]) == {RAW_FILE}
    assert [json.loads(params) for params in set(frame["request_params"])] == [REQUEST_PARAMS]
    assert frame["psr_type"].isna().all()
    assert frame["unit"].isna().all()


def test_to_bronze_frame_keeps_the_parsed_values_and_sequences():
    parsed = parse_timeseries(PRICES_DE_LU)
    frame = to_bronze()

    assert frame["value"].tolist() == parsed["value"].tolist()
    assert frame["classification_sequence"].tolist() == parsed["classification_sequence"].tolist()
    assert frame["interval_start_utc"].tolist() == parsed["interval_start_utc"].tolist()


def test_to_bronze_frame_refuses_request_params_with_a_token():
    with pytest.raises(ValueError, match="must not contain the token"):
        to_bronze(request_params={**REQUEST_PARAMS, "securityToken": "secret"})


def test_to_bronze_frame_rejects_a_naive_ingestion_time():
    with pytest.raises(ValueError, match="ingested_at_utc must be timezone-aware"):
        to_bronze(ingested_at_utc=datetime(2026, 9, 24, 14, 30))


@pytest.mark.parametrize(
    ("xml", "sequences"), [(PRICES_DE_LU, {1, 2}), (PRICES_CH, {None})], ids=["de_lu", "ch"]
)
def test_bronze_frame_round_trips_through_parquet(tmp_path, xml, sequences):
    path = tmp_path / "prices.parquet"
    frame = to_bronze(xml)

    ingest.write_parquet(frame, path, ingest.BRONZE_PRICES_SCHEMA)
    table = pq.read_table(path)

    assert table.schema.remove_metadata() == ingest.BRONZE_PRICES_SCHEMA
    assert table.num_rows == len(frame)
    assert set(table.column("classification_sequence").to_pylist()) == sequences
    assert table.column("value").to_pylist() == frame["value"].tolist()


def test_ensure_setup_adds_columns_an_existing_table_lacks(monkeypatch):
    ran = []
    monkeypatch.setattr(ingest.dbx, "run_sql", lambda sql, wh, client=None: ran.append(sql))
    existing = set(ingest.BRONZE_PRICES_SCHEMA.names) - {"classification_sequence"}
    monkeypatch.setattr(ingest.dbx, "table_columns", lambda table, wh, client=None: existing)

    statements = ingest.ensure_setup("wh-1")

    alter = (
        "ALTER TABLE workspace.power_raw.bronze_prices ADD COLUMNS (classification_sequence INT)"
    )
    assert statements == [*ingest.setup_statements(), alter]
    assert ran == statements


def test_ensure_setup_alters_nothing_when_the_table_is_up_to_date(monkeypatch):
    ran = []
    monkeypatch.setattr(ingest.dbx, "run_sql", lambda sql, wh, client=None: ran.append(sql))
    up_to_date = set(ingest.BRONZE_PRICES_SCHEMA.names)
    monkeypatch.setattr(ingest.dbx, "table_columns", lambda table, wh, client=None: up_to_date)

    assert ingest.ensure_setup("wh-1") == ingest.setup_statements()
    assert ran == ingest.setup_statements()


@pytest.fixture
def fake_services(monkeypatch):
    """Replace ENTSO-E and Databricks with fakes that record what ingest_day does."""
    calls = {"fetch": [], "uploads": [], "loads": [], "response": None}

    def fake_fetch(params, token):
        calls["fetch"].append((params, token))
        return calls["response"]

    def fake_upload(local_path, volume_path, client=None):
        calls["uploads"].append((volume_path, local_path.read_bytes()))

    def fake_load(dataset, warehouse_id, client=None):
        calls["loads"].append((dataset, warehouse_id))
        return 192

    monkeypatch.setattr(ingest.entsoe, "fetch", fake_fetch)
    monkeypatch.setattr(ingest.dbx, "upload_file", fake_upload)
    monkeypatch.setattr(ingest, "load_bronze", fake_load)
    return calls


def fetch_result(content=PRICES_DE_LU, status="data"):
    root = "Publication_MarketDocument" if status == "data" else "Acknowledgement_MarketDocument"
    return entsoe.FetchResult(
        status=status,
        http_status=200,
        root_element=root,
        content=content,
        request_params={"documentType": "A44"},
    )


def run_ingest_day(tmp_path, dataset="prices"):
    return ingest.ingest_day(
        dataset, "DE_LU", date(2026, 9, 23), token="t", warehouse_id="wh-1", data_dir=tmp_path
    )


def test_ingest_day_requests_the_local_delivery_day(tmp_path, fake_services):
    fake_services["response"] = fetch_result()

    run_ingest_day(tmp_path)

    [(params, token)] = fake_services["fetch"]
    assert params["documentType"] == "A44"
    assert (params["periodStart"], params["periodEnd"]) == ("202609222200", "202609232200")
    assert token == "t"


def test_ingest_day_uploads_raw_then_parquet_and_loads_bronze(tmp_path, fake_services):
    fake_services["response"] = fetch_result()

    result = run_ingest_day(tmp_path)

    (raw_path, raw_bytes), (parsed_path, parsed_bytes) = fake_services["uploads"]
    name_start = "20260922T2200Z_20260923T2200Z_"
    raw_dir = "/Volumes/workspace/power_raw/landing/entsoe/prices/DE_LU/2026/09/"
    parsed_dir = "/Volumes/workspace/power_raw/landing/parsed/entsoe/prices/DE_LU/2026/09/"
    assert raw_path.startswith(raw_dir + name_start)
    assert raw_path.endswith(".xml.gz")
    assert parsed_path.startswith(parsed_dir + name_start)
    assert parsed_path.endswith(".parquet")
    assert gzip.decompress(raw_bytes) == PRICES_DE_LU
    assert parsed_bytes[:4] == b"PAR1"
    assert fake_services["loads"] == [("prices", "wh-1")]
    assert result == ingest.IngestResult("data", 192, 192, raw_path, parsed_path)


def test_ingest_day_parquet_rows_point_back_to_the_raw_file(tmp_path, fake_services):
    fake_services["response"] = fetch_result()

    result = run_ingest_day(tmp_path)

    [parquet_file] = (tmp_path / "parsed").rglob("*.parquet")
    table = pq.read_table(parquet_file)
    assert table.num_rows == 192
    assert set(table.column("source_file").to_pylist()) == {result.raw_volume_path}
    assert set(table.column("request_params").to_pylist()) == {'{"documentType": "A44"}'}
    assert set(table.column("classification_sequence").to_pylist()) == {1, 2}


def test_ingest_day_keeps_a_no_data_answer_as_raw_only(tmp_path, fake_services):
    fake_services["response"] = fetch_result(NO_DATA, status="no_data")

    result = run_ingest_day(tmp_path)

    [(raw_path, raw_bytes)] = fake_services["uploads"]
    assert gzip.decompress(raw_bytes) == NO_DATA
    assert fake_services["loads"] == []
    assert result == ingest.IngestResult("no_data", 0, 0, raw_path, None)
    assert not (tmp_path / "parsed").exists()


def test_ingest_day_refuses_a_dataset_without_a_bronze_table(tmp_path, fake_services):
    with pytest.raises(ValueError, match="no bronze table for load"):
        run_ingest_day(tmp_path, dataset="load")

    assert fake_services["fetch"] == []
