import gzip
from importlib.metadata import version
from pathlib import Path

import httpx
import pytest
import respx
from databricks.sdk.errors import DatabricksError
from typer.testing import CliRunner

from power_pipeline import cli
from power_pipeline.cli import app
from power_pipeline.entsoe import client

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"
AUTH_FAILED = (FIXTURES / "ack_authentication_failed.xml").read_bytes()
NO_DATA = (FIXTURES / "ack_no_data.xml").read_bytes()
PRICES = (FIXTURES / "prices_de_lu_2026-09-23.xml").read_bytes()
TOKEN = "11111111-2222-3333-4444-555555555555"
FETCH_DE_LU = ["fetch", "--dataset", "prices", "--zone", "DE_LU", "--date", "2026-09-23"]


def saved_files(data_dir):
    return sorted((data_dir / "raw").rglob("*.xml.gz")) if (data_dir / "raw").exists() else []


@respx.mock
def test_fetch_saves_the_raw_response_for_the_delivery_day(monkeypatch, tmp_path):
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))
    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)

    result = runner.invoke(app, [*FETCH_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    params = dict(route.calls.last.request.url.params)
    assert params["documentType"] == "A44"
    assert params["periodStart"] == "202609222200"
    assert params["periodEnd"] == "202609232200"
    [saved] = saved_files(tmp_path)
    folder = saved.parent.relative_to(tmp_path).as_posix()
    assert folder == "raw/entsoe/prices/DE_LU/2026/09"
    assert saved.name.startswith("20260922T2200Z_20260923T2200Z_")
    assert gzip.decompress(saved.read_bytes()) == PRICES
    assert "status: data (HTTP 200, Publication_MarketDocument" in result.output
    assert TOKEN not in result.output


@respx.mock
def test_fetch_saves_a_no_data_response_and_succeeds(monkeypatch, tmp_path):
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=NO_DATA))
    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)

    result = runner.invoke(app, [*FETCH_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "status: no_data" in result.output
    [saved] = saved_files(tmp_path)
    assert gzip.decompress(saved.read_bytes()) == NO_DATA


@respx.mock
def test_fetch_reports_api_errors_without_saving_or_leaking_the_token(monkeypatch, tmp_path):
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(401, content=AUTH_FAILED))
    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)

    result = runner.invoke(app, [*FETCH_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 1
    assert "error: HTTP 401, acknowledgement 999: Authentication failed." in result.output
    assert TOKEN not in result.output
    assert saved_files(tmp_path) == []


def test_fetch_explains_a_missing_token(tmp_path):
    result = runner.invoke(app, [*FETCH_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 1
    assert "Set ENTSOE_API_TOKEN" in result.output


def test_fetch_rejects_an_unknown_zone(tmp_path):
    args = ["fetch", "--dataset", "prices", "--zone", "FR", "--date", "2026-09-23"]

    result = runner.invoke(app, [*args, "--data-dir", str(tmp_path)])

    assert result.exit_code == 2
    assert "choose from ['CH', 'DE_LU']" in result.output


def test_version_prints_installed_package_version():
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.output.strip() == version("power-pipeline")


def test_help_lists_the_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "version" in result.output
    assert "diagnose" in result.output


@pytest.fixture
def entsoe_answers_401():
    with respx.mock:
        respx.get(client.BASE_URL).mock(return_value=httpx.Response(401, text="Unauthorized"))
        yield


@pytest.mark.usefixtures("entsoe_answers_401")
def test_diagnose_passes_with_token_in_environment_and_scope(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")
    monkeypatch.setattr(cli.databricks_io, "get_secret", lambda scope, key: "token-from-scope")

    result = runner.invoke(app, ["diagnose", "--secret-scope", "power-pipeline"])

    assert result.exit_code == 0, result.output
    assert "entsoe api: reachable (HTTP 401" in result.output
    assert "token in environment: found" in result.output
    assert "token in secret scope 'power-pipeline': found" in result.output


@pytest.mark.usefixtures("entsoe_answers_401")
def test_diagnose_never_prints_token_values(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")
    monkeypatch.setattr(cli.databricks_io, "get_secret", lambda scope, key: "token-from-scope")

    result = runner.invoke(app, ["diagnose", "--secret-scope", "power-pipeline"])

    assert "token-from-env" not in result.output
    assert "token-from-scope" not in result.output


@pytest.mark.usefixtures("entsoe_answers_401")
def test_diagnose_passes_with_token_only_in_scope(monkeypatch):
    monkeypatch.setattr(cli.databricks_io, "get_secret", lambda scope, key: "token-from-scope")

    result = runner.invoke(app, ["diagnose", "--secret-scope", "power-pipeline"])

    assert result.exit_code == 0, result.output
    assert "token in environment: not set" in result.output


@pytest.mark.usefixtures("entsoe_answers_401")
def test_diagnose_fails_when_no_source_has_a_token():
    result = runner.invoke(app, ["diagnose"])

    assert result.exit_code == 1
    assert "token: not found in any source" in result.output


@pytest.mark.usefixtures("entsoe_answers_401")
def test_diagnose_fails_and_reports_when_the_scope_cannot_be_read(monkeypatch):
    def broken_get_secret(scope, key):
        raise PermissionError("scope does not exist")

    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")
    monkeypatch.setattr(cli.databricks_io, "get_secret", broken_get_secret)

    result = runner.invoke(app, ["diagnose", "--secret-scope", "power-pipeline"])

    assert result.exit_code == 1
    assert "error (PermissionError: scope does not exist)" in result.output


def test_run_job_returns_normally_when_the_command_succeeds(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["power-pipeline-job", "version"])

    assert cli.run_job() is None
    assert capsys.readouterr().out.strip() == version("power-pipeline")


@pytest.mark.usefixtures("entsoe_answers_401")
def test_run_job_returns_normally_when_a_command_exits_with_zero(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")
    monkeypatch.setattr("sys.argv", ["power-pipeline-job", "diagnose"])

    assert cli.run_job() is None


@pytest.mark.usefixtures("entsoe_answers_401")
def test_run_job_raises_system_exit_when_the_command_fails(monkeypatch):
    monkeypatch.setattr("sys.argv", ["power-pipeline-job", "diagnose"])

    with pytest.raises(SystemExit) as exc_info:
        cli.run_job()

    assert exc_info.value.code == 1


def test_setup_uses_the_warehouse_from_the_http_path_and_lists_what_it_ran(monkeypatch):
    calls = []

    def fake_ensure_setup(warehouse_id):
        calls.append(warehouse_id)
        return ["CREATE SCHEMA IF NOT EXISTS x COMMENT 'y'", "ALTER TABLE t ADD COLUMNS (c INT)"]

    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, "ensure_setup", fake_ensure_setup)

    result = runner.invoke(app, ["setup"])

    assert result.exit_code == 0, result.output
    assert calls == ["abc123"]
    assert "ok: CREATE SCHEMA IF NOT EXISTS x COMMENT 'y'" in result.output
    assert "ok: ALTER TABLE t ADD COLUMNS (c INT)" in result.output


@respx.mock
def test_diagnose_fails_when_the_api_is_unreachable(monkeypatch):
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError("connection refused"))
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")

    result = runner.invoke(app, ["diagnose"])

    assert result.exit_code == 1
    assert "entsoe api: NOT reachable (ConnectError" in result.output


def test_fetch_explains_that_border_datasets_are_not_supported_yet(tmp_path):
    args = ["fetch", "--dataset", "flows", "--zone", "CH", "--date", "2026-09-23"]

    result = runner.invoke(app, [*args, "--data-dir", str(tmp_path)])

    assert result.exit_code == 2
    assert "requested per border direction" in result.output


INGEST_DE_LU = ["ingest-day", "--dataset", "prices", "--zone", "DE_LU", "--date", "2026-09-23"]


def test_ingest_day_prints_what_was_stored_and_loaded(monkeypatch, tmp_path):
    calls = []

    def fake_ingest_day(dataset, zone, day, *, token, warehouse_id, data_dir):
        calls.append((dataset, zone, day.isoformat(), token, warehouse_id, data_dir))
        return cli.ingest.IngestResult(
            "data", 192, 192, "/Volumes/raw.xml.gz", "/Volumes/p.parquet"
        )

    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, "ingest_day", fake_ingest_day)

    result = runner.invoke(app, [*INGEST_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert calls == [("prices", "DE_LU", "2026-09-23", TOKEN, "abc123", tmp_path)]
    assert "status: data" in result.output
    assert "raw: /Volumes/raw.xml.gz" in result.output
    assert "parsed: /Volumes/p.parquet" in result.output
    assert "rows loaded into bronze: 192" in result.output
    assert TOKEN not in result.output


def test_ingest_day_reports_api_errors(monkeypatch, tmp_path):
    def failing_ingest_day(*args, **kwargs):
        raise client.EntsoeApiError("HTTP 401, acknowledgement 999: Authentication failed.")

    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, "ingest_day", failing_ingest_day)

    result = runner.invoke(app, [*INGEST_DE_LU, "--data-dir", str(tmp_path)])

    assert result.exit_code == 1
    assert "error: HTTP 401" in result.output


def test_ingest_day_rejects_flows_because_they_are_fetched_per_border(tmp_path):
    args = ["ingest-day", "--dataset", "flows", "--zone", "CH", "--date", "2026-09-23"]

    result = runner.invoke(app, [*args, "--data-dir", str(tmp_path)])

    assert result.exit_code == 2
    assert "choose from ['generation', 'load', 'prices']" in result.output


def test_load_bronze_prints_the_rows_loaded(monkeypatch):
    calls = []

    def fake_load_bronze(dataset, warehouse_id):
        calls.append((dataset, warehouse_id))
        return 0

    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, "load_bronze", fake_load_bronze)

    result = runner.invoke(app, ["load-bronze", "--dataset", "prices"])

    assert result.exit_code == 0, result.output
    assert calls == [("prices", "abc123")]
    assert "rows loaded into bronze: 0" in result.output


def databricks_is_down(*args, **kwargs):
    raise DatabricksError("The request could not be processed by the warehouse.")


@pytest.mark.parametrize(
    ("patched", "args"),
    [
        ("ensure_setup", ["setup"]),
        ("ingest_day", INGEST_DE_LU),
        ("load_bronze", ["load-bronze", "--dataset", "prices"]),
    ],
)
def test_databricks_failures_are_reported_in_one_line(monkeypatch, patched, args):
    monkeypatch.setenv("ENTSOE_API_TOKEN", TOKEN)
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, patched, databricks_is_down)

    result = runner.invoke(app, args)

    assert result.exit_code == 1
    assert "error: Databricks: The request could not be processed by the warehouse." in (
        result.output
    )
    assert "Traceback" not in result.output


def test_sql_failures_are_reported_in_one_line(monkeypatch):
    def failing_load(dataset, warehouse_id):
        raise cli.databricks_io.SqlError("Statement FAILED: [TABLE_OR_VIEW_NOT_FOUND] missing")

    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(cli.ingest, "load_bronze", failing_load)

    result = runner.invoke(app, ["load-bronze", "--dataset", "prices"])

    assert result.exit_code == 1
    assert "error: Databricks: Statement FAILED: [TABLE_OR_VIEW_NOT_FOUND]" in result.output
