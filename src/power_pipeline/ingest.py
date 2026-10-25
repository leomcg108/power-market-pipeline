"""Store and load ENTSO-E data: windows, landing paths, raw and Parquet files, bronze tables."""

import gzip
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sdk import WorkspaceClient

from power_pipeline import databricks_io as dbx
from power_pipeline.entsoe import client as entsoe
from power_pipeline.entsoe import datasets, parsers

_UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")


def _bronze_schema(location_columns: list[str]) -> pa.Schema:
    """Bronze columns. Zone datasets have `zone`. Flows have `from_zone` and `to_zone`.

    One schema defines both the Parquet files and the table, so they cannot drift.
    Microsecond timestamps, because Spark cannot read Parquet timestamps in nanoseconds.
    Columns added after a table first existed sit at the end, in the order they came.
    """
    return pa.schema(
        [
            ("dataset", pa.string()),
            *[(name, pa.string()) for name in location_columns],
            ("psr_type", pa.string()),
            ("resolution", pa.string()),
            ("interval_start_utc", _UTC_TIMESTAMP),
            ("interval_end_utc", _UTC_TIMESTAMP),
            ("value", pa.float64()),
            ("unit", pa.string()),
            ("document_mrid", pa.string()),
            ("time_series_mrid", pa.string()),
            ("revision_number", pa.int32()),
            ("created_datetime_utc", _UTC_TIMESTAMP),
            ("ingested_at_utc", _UTC_TIMESTAMP),
            ("source_file", pa.string()),
            ("request_params", pa.string()),
            # Added 2026-10-13. Prices only: 1 = SDAC, 2 = EXAA, null otherwise.
            ("classification_sequence", pa.int32()),
            # Added 2026-10-25. EIC codes from the series. For generation, an out
            # domain means consumption, such as pumped storage pumping.
            ("in_domain", pa.string()),
            ("out_domain", pa.string()),
        ]
    )


ZONE_BRONZE_SCHEMA = _bronze_schema(["zone"])
BORDER_BRONZE_SCHEMA = _bronze_schema(["from_zone", "to_zone"])
BRONZE_PRICES_SCHEMA = ZONE_BRONZE_SCHEMA

BRONZE_TABLES = {
    name: f"{dbx.RAW_SCHEMA}.bronze_{name}" for name in ("prices", "load", "generation", "flows")
}
BRONZE_SCHEMAS = {
    "prices": ZONE_BRONZE_SCHEMA,
    "load": ZONE_BRONZE_SCHEMA,
    "generation": ZONE_BRONZE_SCHEMA,
    "flows": BORDER_BRONZE_SCHEMA,
}
BRONZE_COMMENTS = {
    "prices": "Day-ahead prices from ENTSO-E A44 documents, SDAC and EXAA series.",
    "load": "Actual total load from ENTSO-E A65 documents, in MW.",
    "generation": "Actual generation per production type from ENTSO-E A75 documents, in MW.",
    "flows": "Cross-border physical flows from ENTSO-E A11 documents, one direction a row, in MW.",
}

INGESTION_LOG_TABLE = f"{dbx.RAW_SCHEMA}.ingestion_log"
INGESTION_LOG_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("dataset", pa.string()),
        ("zone_or_border", pa.string()),
        ("window_start_utc", _UTC_TIMESTAMP),
        ("window_end_utc", _UTC_TIMESTAMP),
        ("status", pa.string()),
        ("http_status", pa.int32()),
        ("rows_parsed", pa.int64()),
        ("duration_s", pa.float64()),
        ("error_message", pa.string()),
        ("logged_at_utc", _UTC_TIMESTAMP),
        # Beyond the spec: data-quality findings, and the raw XML's volume path.
        ("warnings", pa.string()),
        ("raw_file", pa.string()),
    ]
)

RAW_VOLUME_DIR = dbx.LANDING_ROOT
PARSED_VOLUME_DIR = f"{dbx.LANDING_ROOT}/parsed"

# Parsed files sit at parsed/entsoe/<dataset>/<zone>/<yyyy>/<mm>/<file>.parquet.
PARSED_FILE_PATTERN = "*/*/*/*.parquet"

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")

MARKET_TIME_ZONE = ZoneInfo("Europe/Zurich")


def delivery_day_window_utc(delivery_date: date) -> tuple[datetime, datetime]:
    """UTC start and end of one delivery day, which runs midnight to midnight local time.

    Local time is Europe/Zurich, the same as CET/CEST in Germany. The window is 24 hours
    long, except 23 hours on the spring clock change and 25 on the autumn one.
    """
    start = datetime.combine(delivery_date, time(0), tzinfo=MARKET_TIME_ZONE)
    end = datetime.combine(delivery_date + timedelta(days=1), time(0), tzinfo=MARKET_TIME_ZONE)
    return start.astimezone(UTC), end.astimezone(UTC)


def _as_utc(moment: datetime, name: str) -> datetime:
    if moment.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return moment.astimezone(UTC)


def landing_relative_path(
    dataset: str,
    zone: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
    ingested_at_utc: datetime,
) -> str:
    """Path of one request's files inside the landing area, without a file extension.

    Pattern: entsoe/<dataset>/<zone>/<yyyy>/<mm>/<window start>_<window end>_<ingested at>.
    The year and month folders come from the window start in UTC. A local delivery day
    starts at 22:00 or 23:00 UTC the day before, so 1 October lands in the 09 folder.
    """
    for name, value in (("dataset", dataset), ("zone", zone)):
        if not _SAFE_NAME.match(value):
            raise ValueError(f"{name} must use letters, digits and underscores only: {value!r}")
    start = _as_utc(window_start_utc, "window_start_utc")
    end = _as_utc(window_end_utc, "window_end_utc")
    ingested = _as_utc(ingested_at_utc, "ingested_at_utc")
    window = f"{start:%Y%m%dT%H%MZ}_{end:%Y%m%dT%H%MZ}"
    return f"entsoe/{dataset}/{zone}/{start:%Y}/{start:%m}/{window}_{ingested:%Y%m%dT%H%M%SZ}"


def raw_volume_path(relative_path: str) -> str:
    """Volume path of the gzipped raw XML for a landing path."""
    return f"{RAW_VOLUME_DIR}/{relative_path}.xml.gz"


def parsed_volume_path(relative_path: str) -> str:
    """Volume path of the parsed Parquet file for a landing path."""
    return f"{PARSED_VOLUME_DIR}/{relative_path}.parquet"


def local_raw_path(data_dir: Path, relative_path: str) -> Path:
    """Local path of the gzipped raw XML: <data_dir>/raw/<landing path>.xml.gz."""
    return data_dir / "raw" / f"{relative_path}.xml.gz"


def local_parsed_path(data_dir: Path, relative_path: str) -> Path:
    """Local path of the parsed Parquet file: <data_dir>/parsed/<landing path>.parquet."""
    return data_dir / "parsed" / f"{relative_path}.parquet"


def write_raw_file(content: bytes, path: Path) -> None:
    """Save a response body gzipped, byte for byte. Never overwrites an existing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle, gzip.GzipFile(fileobj=handle, mode="wb", mtime=0) as gz:
        gz.write(content)


def write_parquet(frame: pd.DataFrame, path: Path, schema: pa.Schema) -> None:
    """Write a DataFrame to Parquet with exactly the given columns and types.

    Refuses missing or extra columns, and timestamp columns that are not UTC-aware.
    """
    missing = set(schema.names) - set(frame.columns)
    extra = set(frame.columns) - set(schema.names)
    if missing or extra:
        raise ValueError(f"Columns do not match the schema. Missing: {missing}. Extra: {extra}.")
    for field in schema:
        if pa.types.is_timestamp(field.type):
            dtype = frame[field.name].dtype
            if not (isinstance(dtype, pd.DatetimeTZDtype) and str(dtype.tz) == "UTC"):
                raise ValueError(f"Column {field.name} must hold UTC timestamps, got {dtype}.")
    table = pa.Table.from_pandas(frame[schema.names], schema=schema, preserve_index=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def setup_statements() -> list[str]:
    """SQL that creates the raw schema, landing volume, bronze tables and log if missing."""
    statements = [
        dbx.create_schema_sql(dbx.RAW_SCHEMA, "ENTSO-E data exactly as received. Append-only."),
        dbx.create_volume_sql(
            dbx.LANDING_VOLUME, "Raw ENTSO-E XML (gzipped) and the Parquet parsed from it."
        ),
    ]
    for dataset, table in BRONZE_TABLES.items():
        comment = (
            f"{BRONZE_COMMENTS[dataset]} One row per interval per fetch. "
            "Timestamps in UTC. Append-only."
        )
        statements.append(dbx.create_append_only_table_sql(table, BRONZE_SCHEMAS[dataset], comment))
    statements.append(
        dbx.create_append_only_table_sql(
            INGESTION_LOG_TABLE,
            INGESTION_LOG_SCHEMA,
            "One row per ENTSO-E request: status success, no_data or error. Append-only.",
        )
    )
    return statements


def _tables_and_schemas() -> list[tuple[str, pa.Schema]]:
    pairs = [(table, BRONZE_SCHEMAS[dataset]) for dataset, table in BRONZE_TABLES.items()]
    return [*pairs, (INGESTION_LOG_TABLE, INGESTION_LOG_SCHEMA)]


def copy_into_bronze_sql(dataset: str) -> str:
    """COPY INTO statement that loads every new parsed file of a dataset into bronze."""
    return dbx.copy_into_sql(
        BRONZE_TABLES[dataset], f"{PARSED_VOLUME_DIR}/entsoe/{dataset}", PARSED_FILE_PATTERN
    )


def load_bronze(dataset: str, warehouse_id: str, *, client: WorkspaceClient | None = None) -> int:
    """Load new parsed files of a dataset into its bronze table. Returns rows inserted."""
    rows = dbx.run_sql(copy_into_bronze_sql(dataset), warehouse_id, client=client)
    return sum(int(row["num_inserted_rows"]) for row in rows)


def ensure_setup(warehouse_id: str, *, client: WorkspaceClient | None = None) -> list[str]:
    """Create the raw schema, volume, bronze tables and log, then add columns they lack.

    Safe to run any number of times. Returns the statements it ran.
    """
    statements = setup_statements()
    for statement in statements:
        dbx.run_sql(statement, warehouse_id, client=client)
    for table, schema in _tables_and_schemas():
        existing = dbx.table_columns(table, warehouse_id, client=client)
        alter = dbx.add_missing_columns_sql(table, schema, existing)
        if alter:
            dbx.run_sql(alter, warehouse_id, client=client)
            statements.append(alter)
    return statements


def to_bronze_frame(
    parsed: pd.DataFrame,
    *,
    dataset: str,
    ingested_at_utc: datetime,
    source_file: str,
    request_params: dict[str, str],
    zone: str | None = None,
    from_zone: str | None = None,
    to_zone: str | None = None,
) -> pd.DataFrame:
    """Add the columns every raw record must carry (data rule 2) to the parser's output.

    Zone datasets take `zone`. Flows take `from_zone` and `to_zone`. `source_file` is
    the volume path of the raw XML the rows came from. `psr_type`, `unit` and the
    domains come from the parser, which reads them from the XML.
    """
    if entsoe.TOKEN_PARAM in request_params:
        raise ValueError("request_params must not contain the token.")
    schema = BRONZE_SCHEMAS[dataset]
    is_border = "from_zone" in schema.names
    if is_border and not (from_zone and to_zone and zone is None):
        raise ValueError(f"{dataset} rows need from_zone and to_zone, not zone.")
    if not is_border and not (zone and from_zone is None and to_zone is None):
        raise ValueError(f"{dataset} rows need zone, not from_zone and to_zone.")
    frame = parsed.copy()
    frame["dataset"] = dataset
    if is_border:
        frame["from_zone"] = from_zone
        frame["to_zone"] = to_zone
    else:
        frame["zone"] = zone
    frame["ingested_at_utc"] = pd.Timestamp(_as_utc(ingested_at_utc, "ingested_at_utc"))
    frame["source_file"] = source_file
    frame["request_params"] = json.dumps(request_params, sort_keys=True)
    return frame[schema.names]


@dataclass(frozen=True)
class IngestResult:
    status: str
    rows_parsed: int
    rows_loaded: int
    raw_volume_path: str
    parsed_volume_path: str | None


def ingest_day(
    dataset_name: str,
    zone_code: str,
    delivery_date: date,
    *,
    token: str,
    warehouse_id: str,
    data_dir: Path = Path("data"),
    workspace: WorkspaceClient | None = None,
) -> IngestResult:
    """Fetch one delivery day, keep the raw XML, parse it and load it into bronze.

    The raw XML is saved and uploaded first, so the original payload is kept even if a
    later step fails. A "no data" answer is stored as raw only. Every run writes new
    files named by its ingestion time, so nothing is overwritten. Running it twice
    stores two fetches. Staging removes the repeats.
    """
    if dataset_name not in BRONZE_TABLES:
        raise ValueError(f"There is no bronze table for {dataset_name} yet.")
    dataset = datasets.load_datasets()[dataset_name]
    if dataset.scope != "zone":
        raise ValueError(f"{dataset_name} is requested per border direction, not per zone.")
    zone = datasets.load_zones()[zone_code]
    start, end = delivery_day_window_utc(delivery_date)
    result = entsoe.fetch(datasets.build_params(dataset, zone, start, end), token)

    ingested_at = datetime.now(UTC)
    relative_path = landing_relative_path(dataset_name, zone_code, start, end, ingested_at)
    raw_local = local_raw_path(data_dir, relative_path)
    raw_volume = raw_volume_path(relative_path)
    write_raw_file(result.content, raw_local)
    dbx.upload_file(raw_local, raw_volume, client=workspace)
    if result.status == "no_data":
        return IngestResult("no_data", 0, 0, raw_volume, None)

    frame = to_bronze_frame(
        parsers.parse_timeseries(result.content),
        dataset=dataset_name,
        zone=zone_code,
        ingested_at_utc=ingested_at,
        source_file=raw_volume,
        request_params=result.request_params,
    )
    parsed_local = local_parsed_path(data_dir, relative_path)
    parsed_volume = parsed_volume_path(relative_path)
    write_parquet(frame, parsed_local, BRONZE_SCHEMAS[dataset_name])
    dbx.upload_file(parsed_local, parsed_volume, client=workspace)
    rows_loaded = load_bronze(dataset_name, warehouse_id, client=workspace)
    return IngestResult("data", len(frame), rows_loaded, raw_volume, parsed_volume)


LOG_STATUSES = ("success", "no_data", "error")


@dataclass(frozen=True)
class LogEntry:
    """One row of the ingestion log: what happened to one ENTSO-E request."""

    run_id: str
    dataset: str
    zone_or_border: str
    window_start_utc: datetime
    window_end_utc: datetime
    status: str
    http_status: int | None
    rows_parsed: int
    duration_s: float
    error_message: str | None
    logged_at_utc: datetime
    warnings: str | None = None
    raw_file: str | None = None

    def __post_init__(self):
        if self.status not in LOG_STATUSES:
            raise ValueError(f"status must be one of {LOG_STATUSES}, not {self.status!r}.")


def insert_log_sql() -> str:
    """INSERT for one log row. Values are named parameters, one per column."""
    names = INGESTION_LOG_SCHEMA.names
    columns = ", ".join(names)
    markers = ", ".join(f":{name}" for name in names)
    return f"INSERT INTO {INGESTION_LOG_TABLE} ({columns}) VALUES ({markers})"


def log_request(
    entry: LogEntry, warehouse_id: str, *, client: WorkspaceClient | None = None
) -> None:
    """Append one row to the ingestion log."""
    values = {name: getattr(entry, name) for name in INGESTION_LOG_SCHEMA.names}
    dbx.run_sql(insert_log_sql(), warehouse_id, parameters=values, client=client)


def successful_windows(
    dataset: str,
    zone_or_border: str,
    *,
    not_before: datetime,
    warehouse_id: str,
    client: WorkspaceClient | None = None,
) -> set[tuple[datetime, datetime]]:
    """Windows with a `success` row logged at or after `not_before`, as UTC pairs.

    The backfill passes the earliest possible time, so any past success counts. The
    daily job passes the start of the run day, so only today's successes count.
    """
    rows = dbx.run_sql(
        f"SELECT DISTINCT window_start_utc, window_end_utc FROM {INGESTION_LOG_TABLE} "
        "WHERE dataset = :dataset AND zone_or_border = :zone_or_border "
        "AND status = 'success' AND logged_at_utc >= :not_before",
        warehouse_id,
        parameters={
            "dataset": dataset,
            "zone_or_border": zone_or_border,
            "not_before": not_before,
        },
        client=client,
    )
    return {
        (_api_timestamp(row["window_start_utc"]), _api_timestamp(row["window_end_utc"]))
        for row in rows
    }


def _api_timestamp(value: str) -> datetime:
    """Read a timestamp the statement execution API returns, such as 2026-10-11T22:00:00.000Z."""
    return datetime.fromisoformat(value).astimezone(UTC)
