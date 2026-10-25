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
            "in_domain": "10Y1001A1001A82H",
            "out_domain": "10Y1001A1001A82H",
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


def test_setup_statements_create_schema_volume_bronze_tables_then_log():
    statements = ingest.setup_statements()

    assert [s.splitlines()[0] for s in statements[2:]] == [
        "CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_prices (",
        "CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_load (",
        "CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_generation (",
        "CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_flows (",
        "CREATE TABLE IF NOT EXISTS workspace.power_raw.ingestion_log (",
    ]
    assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS workspace.power_raw ")
    assert statements[1].startswith("CREATE VOLUME IF NOT EXISTS workspace.power_raw.landing ")


@pytest.mark.parametrize("dataset", ["prices", "load", "generation", "flows"])
def test_each_bronze_table_has_every_schema_column_and_is_append_only(dataset):
    [table_sql] = [
        s for s in ingest.setup_statements() if f"{ingest.BRONZE_TABLES[dataset]} (" in s
    ]

    for name in ingest.BRONZE_SCHEMAS[dataset].names:
        assert f"    {name} " in table_sql
    assert "'delta.appendOnly' = 'true'" in table_sql


def test_ingestion_log_table_has_every_column_and_is_append_only():
    [log_sql] = [s for s in ingest.setup_statements() if "ingestion_log (" in s]

    for name in ingest.INGESTION_LOG_SCHEMA.names:
        assert f"    {name} " in log_sql
    assert "'delta.appendOnly' = 'true'" in log_sql


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


def test_columns_added_later_sit_at_the_end_in_the_order_they_came():
    for schema in (ingest.ZONE_BRONZE_SCHEMA, ingest.BORDER_BRONZE_SCHEMA):
        assert schema.names[-3:] == ["classification_sequence", "in_domain", "out_domain"]
        assert schema.field("classification_sequence").type == pa.int32()


def test_zone_and_border_schemas_differ_only_in_location_columns():
    zone, border = ingest.ZONE_BRONZE_SCHEMA, ingest.BORDER_BRONZE_SCHEMA

    assert "zone" in zone.names and "zone" not in border.names
    assert {"from_zone", "to_zone"} <= set(border.names)
    assert [n for n in zone.names if n != "zone"] == [
        n for n in border.names if n not in ("from_zone", "to_zone")
    ]
    schemas_by_dataset = ingest.BRONZE_SCHEMAS
    assert schemas_by_dataset == {
        "prices": zone,
        "load": zone,
        "generation": zone,
        "flows": border,
    }


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
    assert set(frame["unit"]) == {"EUR/MWH"}


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


def full_columns(table):
    if table == ingest.INGESTION_LOG_TABLE:
        return set(ingest.INGESTION_LOG_SCHEMA.names)
    dataset = table.rsplit("bronze_", 1)[1]
    return set(ingest.BRONZE_SCHEMAS[dataset].names)


def test_ensure_setup_adds_columns_an_existing_table_lacks(monkeypatch):
    ran = []
    monkeypatch.setattr(
        ingest.dbx, "run_sql", lambda sql, wh, parameters=None, client=None: ran.append(sql)
    )

    def columns_now(table, wh, client=None):
        # bronze_prices as it stood after Phase 00: without the two domain columns.
        if table.endswith("bronze_prices"):
            return full_columns(table) - {"in_domain", "out_domain"}
        return full_columns(table)

    monkeypatch.setattr(ingest.dbx, "table_columns", columns_now)

    statements = ingest.ensure_setup("wh-1")

    alter = (
        "ALTER TABLE workspace.power_raw.bronze_prices "
        "ADD COLUMNS (in_domain STRING, out_domain STRING)"
    )
    assert statements == [*ingest.setup_statements(), alter]
    assert ran == statements


def test_ensure_setup_alters_nothing_when_every_table_is_up_to_date(monkeypatch):
    ran, checked = [], []
    monkeypatch.setattr(
        ingest.dbx, "run_sql", lambda sql, wh, parameters=None, client=None: ran.append(sql)
    )

    def columns_now(table, wh, client=None):
        checked.append(table)
        return full_columns(table)

    monkeypatch.setattr(ingest.dbx, "table_columns", columns_now)

    assert ingest.ensure_setup("wh-1") == ingest.setup_statements()
    assert ran == ingest.setup_statements()
    assert checked == [*ingest.BRONZE_TABLES.values(), ingest.INGESTION_LOG_TABLE]


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
    with pytest.raises(ValueError, match="no bronze table for weather"):
        run_ingest_day(tmp_path, dataset="weather")

    assert fake_services["fetch"] == []


def test_ingest_day_refuses_a_border_dataset(tmp_path, fake_services):
    with pytest.raises(ValueError, match="flows is requested per border direction"):
        run_ingest_day(tmp_path, dataset="flows")

    assert fake_services["fetch"] == []


FLOWS_CH_DE_LU = (FIXTURES / "flows_ch_de_lu_2026-09-23.xml").read_bytes()
GENERATION_DE_LU = (FIXTURES / "generation_de_lu_2026-09-23_trimmed.xml").read_bytes()
FLOWS_RAW_FILE = "/Volumes/workspace/power_raw/landing/entsoe/flows/CH_TO_DE_LU/2026/09/x.xml.gz"


def flows_bronze(**overrides):
    args = {
        "dataset": "flows",
        "from_zone": "CH",
        "to_zone": "DE_LU",
        "ingested_at_utc": INGESTED_AT,
        "source_file": FLOWS_RAW_FILE,
        "request_params": {"documentType": "A11"},
    }
    return ingest.to_bronze_frame(parse_timeseries(FLOWS_CH_DE_LU), **(args | overrides))


def test_flows_bronze_rows_carry_both_zones_and_the_parsed_domains():
    frame = flows_bronze()

    assert list(frame.columns) == ingest.BORDER_BRONZE_SCHEMA.names
    assert len(frame) == 96
    assert set(frame["from_zone"]) == {"CH"}
    assert set(frame["to_zone"]) == {"DE_LU"}
    assert set(frame["out_domain"]) == {"10YCH-SWISSGRIDZ"}
    assert set(frame["in_domain"]) == {"10Y1001A1001A82H"}
    assert set(frame["unit"]) == {"MAW"}


@pytest.mark.parametrize(
    "location",
    [{"zone": "CH"}, {"from_zone": "CH"}, {"zone": "CH", "from_zone": "CH", "to_zone": "DE_LU"}],
)
def test_flows_bronze_rows_need_from_and_to_zones_only(location):
    args = {"from_zone": None, "to_zone": None} | location

    with pytest.raises(ValueError, match="flows rows need from_zone and to_zone"):
        flows_bronze(**args)


def test_zone_bronze_rows_refuse_border_zones():
    with pytest.raises(ValueError, match="prices rows need zone"):
        to_bronze(zone=None, from_zone="CH", to_zone="DE_LU")


def test_generation_bronze_rows_keep_production_type_and_domains():
    frame = ingest.to_bronze_frame(
        parse_timeseries(GENERATION_DE_LU),
        dataset="generation",
        zone="DE_LU",
        ingested_at_utc=INGESTED_AT,
        source_file=RAW_FILE,
        request_params={"documentType": "A75"},
    )

    pumping = frame[(frame["psr_type"] == "B10") & frame["out_domain"].notna()]
    assert len(frame) == 479
    assert set(frame["psr_type"]) == {"B09", "B10", "B16", "B17"}
    assert len(pumping) == 96
    assert pumping["in_domain"].isna().all()


@pytest.mark.parametrize(
    ("dataset", "xml", "location"),
    [
        ("generation", GENERATION_DE_LU, {"zone": "DE_LU"}),
        ("flows", FLOWS_CH_DE_LU, {"from_zone": "CH", "to_zone": "DE_LU"}),
    ],
    ids=["generation", "flows"],
)
def test_new_bronze_frames_round_trip_through_parquet(tmp_path, dataset, xml, location):
    frame = ingest.to_bronze_frame(
        parse_timeseries(xml),
        dataset=dataset,
        ingested_at_utc=INGESTED_AT,
        source_file=RAW_FILE,
        request_params={"documentType": "x"},
        **location,
    )
    path = tmp_path / f"{dataset}.parquet"

    ingest.write_parquet(frame, path, ingest.BRONZE_SCHEMAS[dataset])
    table = pq.read_table(path)

    assert table.schema.remove_metadata() == ingest.BRONZE_SCHEMAS[dataset]
    assert table.num_rows == len(frame)
    assert table.column("value").to_pylist() == frame["value"].tolist()
    assert table.column("psr_type").to_pylist() == [
        None if pd.isna(v) else v for v in frame["psr_type"]
    ]


@pytest.mark.parametrize("dataset", ["load", "generation", "flows"])
def test_copy_into_reads_each_dataset_folder_into_its_own_table(dataset):
    sql = ingest.copy_into_bronze_sql(dataset)

    assert sql.startswith(f"COPY INTO workspace.power_raw.bronze_{dataset}\n")
    assert f"FROM '/Volumes/workspace/power_raw/landing/parsed/entsoe/{dataset}'" in sql


# Ingestion log

WINDOW = (datetime(2024, 4, 30, 22, tzinfo=UTC), datetime(2024, 5, 31, 22, tzinfo=UTC))


def log_entry(**overrides):
    args = {
        "run_id": "run-1",
        "dataset": "prices",
        "zone_or_border": "DE_LU",
        "window_start_utc": WINDOW[0],
        "window_end_utc": WINDOW[1],
        "status": "success",
        "http_status": 200,
        "rows_parsed": 3720,
        "duration_s": 10.2,
        "error_message": None,
        "logged_at_utc": INGESTED_AT,
        "warnings": None,
        "raw_file": RAW_FILE,
    }
    return ingest.LogEntry(**(args | overrides))


def test_log_entry_accepts_only_known_statuses():
    with pytest.raises(ValueError, match="status must be one of"):
        log_entry(status="partial")


def test_insert_log_sql_names_every_column_once():
    sql = ingest.insert_log_sql()
    names = ingest.INGESTION_LOG_SCHEMA.names

    assert sql.startswith("INSERT INTO workspace.power_raw.ingestion_log (")
    assert f"({', '.join(names)})" in sql
    assert f"VALUES ({', '.join(':' + n for n in names)})" in sql


def test_log_request_sends_every_column_as_a_parameter(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ingest.dbx,
        "run_sql",
        lambda sql, wh, parameters=None, client=None: calls.append((sql, wh, parameters)),
    )
    entry = log_entry(status="error", http_status=503, error_message="HTTP 503", rows_parsed=0)

    ingest.log_request(entry, "wh-1")

    [(sql, warehouse_id, parameters)] = calls
    assert sql == ingest.insert_log_sql()
    assert warehouse_id == "wh-1"
    assert list(parameters) == ingest.INGESTION_LOG_SCHEMA.names
    assert parameters["status"] == "error"
    assert parameters["http_status"] == 503
    assert parameters["window_start_utc"] == WINDOW[0]
    assert parameters["warnings"] is None


def test_successful_windows_reads_utc_pairs_and_filters_by_time(monkeypatch):
    calls = []

    def fake_run_sql(sql, wh, parameters=None, client=None):
        calls.append((sql, parameters))
        return [
            {
                "window_start_utc": "2024-04-30T22:00:00.000Z",
                "window_end_utc": "2024-05-31T22:00:00.000Z",
            },
            {"window_start_utc": "2024-03-31T22:00:00Z", "window_end_utc": "2024-04-30T22:00:00Z"},
        ]

    monkeypatch.setattr(ingest.dbx, "run_sql", fake_run_sql)
    not_before = datetime(2026, 10, 25, tzinfo=UTC)

    windows = ingest.successful_windows(
        "prices", "DE_LU", not_before=not_before, warehouse_id="wh-1"
    )

    assert windows == {
        WINDOW,
        (datetime(2024, 3, 31, 22, tzinfo=UTC), datetime(2024, 4, 30, 22, tzinfo=UTC)),
    }
    [(sql, parameters)] = calls
    assert "status = 'success'" in sql
    assert "logged_at_utc >= :not_before" in sql
    assert parameters == {"dataset": "prices", "zone_or_border": "DE_LU", "not_before": not_before}
