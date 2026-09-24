from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from databricks.sdk.service.sql import (
    ColumnInfo,
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from power_pipeline import databricks_io


def statement_response(state, *, columns=None, rows=None, error=None, statement_id="stmt-1"):
    manifest = None
    if columns is not None:
        schema = ResultSchema(columns=[ColumnInfo(name=name) for name in columns])
        manifest = ResultManifest(schema=schema)
    return StatementResponse(
        statement_id=statement_id,
        status=StatementStatus(state=state, error=error),
        manifest=manifest,
        result=ResultData(data_array=rows) if rows is not None else None,
    )


def fake_client(*responses):
    """A WorkspaceClient whose first statement returns responses[0], then polls the rest."""
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = responses[0]
    client.statement_execution.get_statement.side_effect = list(responses[1:])
    return client


def test_get_secret_reads_through_workspace_dbutils(monkeypatch):
    workspace = MagicMock()
    workspace.dbutils.secrets.get.return_value = "secret-value"
    monkeypatch.setattr(databricks_io, "WorkspaceClient", lambda: workspace)

    assert databricks_io.get_secret("power-pipeline", "entsoe-api-token") == "secret-value"
    workspace.dbutils.secrets.get.assert_called_once_with(
        scope="power-pipeline", key="entsoe-api-token"
    )


def test_warehouse_id_from_http_path():
    assert databricks_io.warehouse_id_from_http_path("/sql/1.0/warehouses/abc123") == "abc123"


@pytest.mark.parametrize(
    "http_path", ["", "/sql/1.0/endpoints/abc123", "sql/1.0/warehouses/abc123", "/other"]
)
def test_warehouse_id_from_http_path_rejects_other_shapes(http_path):
    with pytest.raises(ValueError, match="HTTP path"):
        databricks_io.warehouse_id_from_http_path(http_path)


def test_warehouse_id_from_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")

    assert databricks_io.warehouse_id_from_env() == "abc123"


def test_warehouse_id_from_env_explains_when_unset():
    with pytest.raises(ValueError, match="DATABRICKS_HTTP_PATH"):
        databricks_io.warehouse_id_from_env()


def test_run_sql_returns_rows_as_dicts():
    client = fake_client(
        statement_response(
            StatementState.SUCCEEDED, columns=["a", "b"], rows=[["1", "x"], ["2", None]]
        )
    )

    rows = databricks_io.run_sql("SELECT 1", "wh-1", client=client)

    assert rows == [{"a": "1", "b": "x"}, {"a": "2", "b": None}]
    call = client.statement_execution.execute_statement.call_args
    assert call.kwargs["statement"] == "SELECT 1"
    assert call.kwargs["warehouse_id"] == "wh-1"


def test_run_sql_polls_until_the_statement_finishes():
    client = fake_client(
        statement_response(StatementState.PENDING),
        statement_response(StatementState.RUNNING),
        statement_response(StatementState.SUCCEEDED, columns=["n"], rows=[["7"]]),
    )

    rows = databricks_io.run_sql("SELECT 7", "wh-1", client=client, poll_interval_s=0)

    assert rows == [{"n": "7"}]
    assert client.statement_execution.get_statement.call_count == 2


def test_run_sql_returns_no_rows_for_statements_without_results():
    client = fake_client(statement_response(StatementState.SUCCEEDED))

    assert databricks_io.run_sql("CREATE SCHEMA x", "wh-1", client=client) == []


def test_run_sql_raises_with_the_error_message_when_a_statement_fails():
    error = ServiceError(message="[TABLE_OR_VIEW_NOT_FOUND] missing")
    client = fake_client(statement_response(StatementState.FAILED, error=error))

    with pytest.raises(databricks_io.SqlError, match="FAILED: \\[TABLE_OR_VIEW_NOT_FOUND\\]"):
        databricks_io.run_sql("SELECT * FROM missing", "wh-1", client=client)


def test_run_sql_cancels_and_raises_after_the_timeout():
    client = fake_client(statement_response(StatementState.PENDING, statement_id="slow"))

    with pytest.raises(databricks_io.SqlError, match="did not finish"):
        databricks_io.run_sql("SELECT 1", "wh-1", client=client, timeout_s=0)

    client.statement_execution.cancel_execution.assert_called_once_with("slow")


def test_upload_file_never_overwrites(tmp_path):
    local = tmp_path / "file.bin"
    local.write_bytes(b"payload")
    client = MagicMock()
    uploaded = {}
    client.files.upload.side_effect = lambda path, contents, overwrite: uploaded.update(
        path=path, data=contents.read(), overwrite=overwrite
    )

    databricks_io.upload_file(local, "/Volumes/c/s/v/file.bin", client=client)

    assert uploaded == {"path": "/Volumes/c/s/v/file.bin", "data": b"payload", "overwrite": False}


@pytest.mark.parametrize(
    ("arrow_type", "sql"),
    [
        (pa.string(), "STRING"),
        (pa.float64(), "DOUBLE"),
        (pa.int32(), "INT"),
        (pa.int64(), "BIGINT"),
        (pa.timestamp("us", tz="UTC"), "TIMESTAMP"),
    ],
)
def test_sql_type_maps_supported_arrow_types(arrow_type, sql):
    assert databricks_io.sql_type(arrow_type) == sql


@pytest.mark.parametrize(
    "arrow_type",
    [pa.timestamp("ns", tz="UTC"), pa.timestamp("us"), pa.timestamp("us", tz="Europe/Zurich")],
)
def test_sql_type_rejects_timestamps_that_are_not_microsecond_utc(arrow_type):
    with pytest.raises(ValueError, match="No SQL type mapping"):
        databricks_io.sql_type(arrow_type)


def test_create_append_only_table_sql():
    schema = pa.schema([("zone", pa.string()), ("value", pa.float64())])

    sql = databricks_io.create_append_only_table_sql("c.s.t", schema, "A table.")

    assert sql == (
        "CREATE TABLE IF NOT EXISTS c.s.t (\n"
        "    zone STRING,\n"
        "    value DOUBLE\n"
        ")\n"
        "USING DELTA\n"
        "COMMENT 'A table.'\n"
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )


def test_create_schema_and_volume_sql_are_idempotent():
    assert databricks_io.create_schema_sql("c.s", "Raw.") == (
        "CREATE SCHEMA IF NOT EXISTS c.s COMMENT 'Raw.'"
    )
    assert databricks_io.create_volume_sql("c.s.v", "Files.") == (
        "CREATE VOLUME IF NOT EXISTS c.s.v COMMENT 'Files.'"
    )


def test_copy_into_sql():
    sql = databricks_io.copy_into_sql("c.s.t", "/Volumes/c/s/v/parsed", "*/*.parquet")

    assert sql == (
        "COPY INTO c.s.t\n"
        "FROM '/Volumes/c/s/v/parsed'\n"
        "FILEFORMAT = PARQUET\n"
        "PATTERN = '*/*.parquet'"
    )


@pytest.mark.parametrize("text", ["it's", "back\\slash"])
def test_sql_builders_refuse_text_with_quotes(text):
    with pytest.raises(ValueError, match="Refusing to quote"):
        databricks_io.create_schema_sql("c.s", text)
