import httpx
import pytest
import respx

from power_pipeline.entsoe import client


def fail_if_called(*args, **kwargs):
    raise AssertionError("the secret scope should not be read")


def test_get_token_prefers_the_environment_variable(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")
    monkeypatch.setattr(client.databricks_io, "get_secret", fail_if_called)

    assert client.get_token(secret_scope="power-pipeline") == "token-from-env"


def test_get_token_falls_back_to_the_secret_scope(monkeypatch):
    calls = []

    def fake_get_secret(scope, key):
        calls.append((scope, key))
        return "token-from-scope"

    monkeypatch.setattr(client.databricks_io, "get_secret", fake_get_secret)

    assert client.get_token(secret_scope="power-pipeline") == "token-from-scope"
    assert calls == [("power-pipeline", "entsoe-api-token")]


def test_get_token_treats_a_blank_environment_variable_as_unset(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "   ")
    monkeypatch.setattr(client.databricks_io, "get_secret", lambda scope, key: "token-from-scope")

    assert client.get_token(secret_scope="power-pipeline") == "token-from-scope"


def test_get_token_raises_when_no_source_has_a_token():
    with pytest.raises(client.MissingTokenError, match="ENTSOE_API_TOKEN"):
        client.get_token()


def test_get_token_never_logs_the_token(monkeypatch, caplog):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "token-from-env")

    with caplog.at_level("DEBUG"):
        client.get_token()

    assert "token-from-env" not in caplog.text


@respx.mock
def test_check_reachable_counts_an_http_error_status_as_reachable():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(401, text="Unauthorized"))

    result = client.check_reachable()

    assert result.ok
    assert "HTTP 401" in result.detail


@respx.mock
def test_check_reachable_reports_a_connection_error_as_unreachable():
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError("connection refused"))

    result = client.check_reachable()

    assert not result.ok
    assert "ConnectError" in result.detail


@respx.mock
def test_check_reachable_reports_a_timeout_as_unreachable():
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectTimeout("timed out"))

    result = client.check_reachable()

    assert not result.ok
    assert "ConnectTimeout" in result.detail
