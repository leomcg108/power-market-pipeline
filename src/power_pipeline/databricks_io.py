"""Helpers for Databricks: secrets, SQL on the warehouse, volume uploads and SQL builders."""

import os
import re
import time
from pathlib import Path

import pyarrow as pa
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

CATALOG = "workspace"
RAW_SCHEMA = f"{CATALOG}.power_raw"
LANDING_VOLUME = f"{RAW_SCHEMA}.landing"
LANDING_ROOT = "/Volumes/workspace/power_raw/landing"

HTTP_PATH_ENV_VAR = "DATABRICKS_HTTP_PATH"
_WAREHOUSE_PATH = re.compile(r"^/sql/1\.0/warehouses/([0-9a-f]+)$")
_UNFINISHED = {StatementState.PENDING, StatementState.RUNNING}


class SqlError(RuntimeError):
    """A SQL statement failed, was canceled or did not finish in time."""


def get_secret(scope: str, key: str) -> str:
    """Read one secret from a Databricks secret scope.

    Works locally, authenticated by DATABRICKS_HOST and DATABRICKS_TOKEN, and inside a
    Databricks job, authenticated as the job's identity.
    """
    return WorkspaceClient().dbutils.secrets.get(scope=scope, key=key)


def warehouse_id_from_http_path(http_path: str) -> str:
    """Return the warehouse ID from an HTTP path like /sql/1.0/warehouses/<id>."""
    match = _WAREHOUSE_PATH.match(http_path.strip())
    if not match:
        raise ValueError("Expected an HTTP path like /sql/1.0/warehouses/<id>.")
    return match.group(1)


def warehouse_id_from_env() -> str:
    """Return the warehouse ID from the DATABRICKS_HTTP_PATH environment variable."""
    http_path = os.environ.get(HTTP_PATH_ENV_VAR, "")
    if not http_path.strip():
        raise ValueError(f"Set {HTTP_PATH_ENV_VAR} or pass a warehouse ID.")
    return warehouse_id_from_http_path(http_path)


def run_sql(
    statement: str,
    warehouse_id: str,
    *,
    client: WorkspaceClient | None = None,
    timeout_s: float = 600,
    poll_interval_s: float = 2,
) -> list[dict[str, str | None]]:
    """Run one SQL statement on a SQL warehouse and return its rows.

    Uses the statement execution API, so no database driver is needed. A stopped
    serverless warehouse starts on the first statement, which can take a minute, so
    this keeps polling until the statement finishes or `timeout_s` passes. Values come
    back as strings, the way the API returns them.
    """
    client = client or WorkspaceClient()
    api = client.statement_execution
    response = api.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    deadline = time.monotonic() + timeout_s
    while response.status.state in _UNFINISHED:
        if time.monotonic() >= deadline:
            api.cancel_execution(response.statement_id)
            raise SqlError(f"Statement did not finish within {timeout_s:.0f} seconds.")
        time.sleep(poll_interval_s)
        response = api.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        message = error.message if error and error.message else "no error message"
        raise SqlError(f"Statement {response.status.state.value}: {message}")

    if not response.manifest or not response.manifest.schema:
        return []
    columns = [column.name for column in response.manifest.schema.columns]
    rows = (response.result.data_array if response.result else None) or []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def upload_file(
    local_path: Path, volume_path: str, *, client: WorkspaceClient | None = None
) -> None:
    """Upload a local file to a Unity Catalog volume. Never overwrites an existing file."""
    client = client or WorkspaceClient()
    with local_path.open("rb") as contents:
        client.files.upload(volume_path, contents, overwrite=False)


def sql_type(data_type: pa.DataType) -> str:
    """Map an Arrow type used in our Parquet files to the matching Databricks SQL type."""
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "STRING"
    if pa.types.is_float64(data_type):
        return "DOUBLE"
    if pa.types.is_int32(data_type):
        return "INT"
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_timestamp(data_type) and data_type.unit == "us" and data_type.tz == "UTC":
        return "TIMESTAMP"
    raise ValueError(f"No SQL type mapping for Arrow type {data_type}.")


def _quote(text: str) -> str:
    """Quote a value as a SQL string literal. Rejects quotes rather than escaping them."""
    if "'" in text or "\\" in text:
        raise ValueError(f"Refusing to quote text with quotes or backslashes: {text!r}")
    return f"'{text}'"


def create_schema_sql(schema: str, comment: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT {_quote(comment)}"


def create_volume_sql(volume: str, comment: str) -> str:
    return f"CREATE VOLUME IF NOT EXISTS {volume} COMMENT {_quote(comment)}"


def create_append_only_table_sql(table: str, schema: pa.Schema, comment: str) -> str:
    """CREATE TABLE IF NOT EXISTS for a Delta table that only accepts appends.

    `delta.appendOnly` makes Delta reject UPDATE and DELETE on the table, which
    enforces the rule that raw data is never changed.
    """
    columns = ",\n".join(f"    {field.name} {sql_type(field.type)}" for field in schema)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n{columns}\n)\n"
        "USING DELTA\n"
        f"COMMENT {_quote(comment)}\n"
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )


def copy_into_sql(table: str, source_dir: str, pattern: str) -> str:
    """COPY INTO statement that loads new Parquet files from a volume folder.

    COPY INTO remembers which files it has loaded into the table, so running the same
    statement again skips them and adds no rows.
    """
    return (
        f"COPY INTO {table}\n"
        f"FROM {_quote(source_dir)}\n"
        "FILEFORMAT = PARQUET\n"
        f"PATTERN = {_quote(pattern)}"
    )
