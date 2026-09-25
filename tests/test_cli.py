import gzip
from importlib.metadata import version
from pathlib import Path

import httpx
import pytest
import respx
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


def test_setup_runs_every_statement_on_the_warehouse_from_the_http_path(monkeypatch):
    calls = []
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setattr(
        cli.databricks_io, "run_sql", lambda sql, warehouse_id: calls.append((sql, warehouse_id))
    )

    result = runner.invoke(app, ["setup"])

    assert result.exit_code == 0, result.output
    assert calls == [(sql, "abc123") for sql in cli.ingest.setup_statements()]
    assert "ok: CREATE TABLE IF NOT EXISTS workspace.power_raw.bronze_prices (" in result.output


@respx.mock
def test_diagnose_fails_when_the_api_is_unreachable(monkeypatch):
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError("connection refused"))
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")

    result = runner.invoke(app, ["diagnose"])

    assert result.exit_code == 1
    assert "entsoe api: NOT reachable (ConnectError" in result.output
