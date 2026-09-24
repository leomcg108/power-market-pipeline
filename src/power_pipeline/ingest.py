"""Store and load ENTSO-E data: landing paths, Parquet files and bronze tables."""

import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sdk import WorkspaceClient

from power_pipeline import databricks_io as dbx

_UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

# One schema for both the Parquet files and the bronze table, so they cannot drift.
# Microsecond timestamps, because Spark cannot read Parquet timestamps in nanoseconds.
BRONZE_PRICES_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("zone", pa.string()),
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
    ]
)

BRONZE_TABLES = {"prices": f"{dbx.RAW_SCHEMA}.bronze_prices"}
BRONZE_SCHEMAS = {"prices": BRONZE_PRICES_SCHEMA}

RAW_VOLUME_DIR = dbx.LANDING_ROOT
PARSED_VOLUME_DIR = f"{dbx.LANDING_ROOT}/parsed"

# Parsed files sit at parsed/entsoe/<dataset>/<zone>/<yyyy>/<mm>/<file>.parquet.
PARSED_FILE_PATTERN = "*/*/*/*.parquet"

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")


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
    """SQL that creates the raw schema, landing volume and bronze tables if missing."""
    return [
        dbx.create_schema_sql(dbx.RAW_SCHEMA, "ENTSO-E data exactly as received. Append-only."),
        dbx.create_volume_sql(
            dbx.LANDING_VOLUME, "Raw ENTSO-E XML (gzipped) and the Parquet parsed from it."
        ),
        dbx.create_append_only_table_sql(
            BRONZE_TABLES["prices"],
            BRONZE_PRICES_SCHEMA,
            "Day-ahead prices parsed from ENTSO-E A44 documents. One row per interval "
            "per fetch. Timestamps in UTC. Append-only.",
        ),
    ]


def copy_into_bronze_sql(dataset: str) -> str:
    """COPY INTO statement that loads every new parsed file of a dataset into bronze."""
    return dbx.copy_into_sql(
        BRONZE_TABLES[dataset], f"{PARSED_VOLUME_DIR}/entsoe/{dataset}", PARSED_FILE_PATTERN
    )


def load_bronze(dataset: str, warehouse_id: str, *, client: WorkspaceClient | None = None) -> int:
    """Load new parsed files of a dataset into its bronze table. Returns rows inserted."""
    rows = dbx.run_sql(copy_into_bronze_sql(dataset), warehouse_id, client=client)
    return sum(int(row["num_inserted_rows"]) for row in rows)
