from importlib.metadata import version

import httpx
import pytest
import respx
from typer.testing import CliRunner

from power_pipeline import cli
from power_pipeline.cli import app
from power_pipeline.entsoe import client

runner = CliRunner()


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


@respx.mock
def test_diagnose_fails_when_the_api_is_unreachable(monkeypatch):
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError("connection refused"))
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")

    result = runner.invoke(app, ["diagnose"])

    assert result.exit_code == 1
    assert "entsoe api: NOT reachable (ConnectError" in result.output
