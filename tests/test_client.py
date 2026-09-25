import logging
from pathlib import Path

import httpx
import pytest
import respx

from power_pipeline.entsoe import client

FIXTURES = Path(__file__).parent / "fixtures"
AUTH_FAILED = (FIXTURES / "ack_authentication_failed.xml").read_bytes()
NO_DATA = (FIXTURES / "ack_no_data.xml").read_bytes()
PRICES = (FIXTURES / "prices_de_lu_2026-09-23.xml").read_bytes()

TOKEN = "11111111-2222-3333-4444-555555555555"
PARAMS = {
    "documentType": "A44",
    "in_Domain": "10Y1001A1001A82H",
    "out_Domain": "10Y1001A1001A82H",
    "contract_MarketAgreement.type": "A01",
    "periodStart": "202609222200",
    "periodEnd": "202609232200",
}


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


def test_parse_acknowledgement_reads_the_real_no_data_response():
    acknowledgement = client.parse_acknowledgement(NO_DATA)

    [(code, text)] = acknowledgement.reasons
    assert code == "999"
    assert text.startswith("No matching data found for Data item ENERGY_PRICES [12.1.D]")
    assert acknowledgement.is_no_data


def test_parse_acknowledgement_reads_the_real_authentication_failure():
    acknowledgement = client.parse_acknowledgement(AUTH_FAILED)

    assert acknowledgement.reasons == (("999", "Authentication failed."),)
    assert not acknowledgement.is_no_data


def test_is_no_data_needs_the_no_data_text_not_just_code_999():
    assert client.parse_acknowledgement(NO_DATA).is_no_data
    assert not client.parse_acknowledgement(AUTH_FAILED).is_no_data


@pytest.mark.parametrize(
    "reasons",
    [
        (),
        (("999", "No matching data found for X."), ("999", "Authentication failed.")),
        (("998", "No matching data found for X."),),
    ],
)
def test_is_no_data_is_false_unless_every_reason_is_no_data(reasons):
    assert not client.Acknowledgement(reasons=reasons).is_no_data


def test_parse_acknowledgement_reads_the_namespace_from_the_root_element():
    other_version = AUTH_FAILED.replace(
        b"acknowledgementdocument:7:0", b"acknowledgementdocument:8:1"
    )

    acknowledgement = client.parse_acknowledgement(other_version)

    assert acknowledgement.reasons == (("999", "Authentication failed."),)


@pytest.mark.parametrize("content", [PRICES, b"not xml", b""])
def test_parse_acknowledgement_returns_none_for_anything_else(content):
    assert client.parse_acknowledgement(content) is None


@respx.mock
def test_fetch_sends_the_params_and_the_token_as_query_parameters():
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))

    client.fetch(PARAMS, TOKEN)

    assert dict(route.calls.last.request.url.params) == {**PARAMS, "securityToken": TOKEN}


@respx.mock
def test_fetch_returns_the_body_exactly_as_received():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))

    result = client.fetch(PARAMS, TOKEN)

    assert result.status == "data"
    assert result.http_status == 200
    assert result.root_element == "Publication_MarketDocument"
    assert result.content == PRICES
    assert result.request_params == PARAMS


@respx.mock
@pytest.mark.parametrize("http_status", [200, 400])
def test_fetch_treats_a_no_data_acknowledgement_as_no_data(http_status):
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(http_status, content=NO_DATA))

    result = client.fetch(PARAMS, TOKEN)

    assert result.status == "no_data"
    assert result.http_status == http_status
    assert result.content == NO_DATA


@respx.mock
def test_fetch_raises_on_any_other_acknowledgement():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(401, content=AUTH_FAILED))

    with pytest.raises(
        client.EntsoeApiError, match="HTTP 401, acknowledgement 999: Authentication"
    ):
        client.fetch(PARAMS, TOKEN)


@respx.mock
def test_fetch_raises_on_http_errors_without_leaking_the_token():
    body = f"Bad gateway for /api?documentType=A44&securityToken={TOKEN}"
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(502, text=body))

    with pytest.raises(client.EntsoeApiError, match="HTTP 502") as exc_info:
        client.fetch(PARAMS, TOKEN)

    assert TOKEN not in str(exc_info.value)
    assert "securityToken=***" in str(exc_info.value)


@respx.mock
def test_fetch_raises_when_a_200_response_is_not_xml():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, text="<html>oops"))

    with pytest.raises(client.EntsoeApiError, match="not XML"):
        client.fetch(PARAMS, TOKEN)


@respx.mock
def test_fetch_raises_on_network_errors_without_leaking_the_token():
    url = f"{client.BASE_URL}?securityToken={TOKEN}"
    respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError(f"cannot reach {url}"))

    with pytest.raises(client.EntsoeApiError, match="ConnectError") as exc_info:
        client.fetch(PARAMS, TOKEN)

    assert TOKEN not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_fetch_refuses_a_token_inside_params():
    with pytest.raises(ValueError, match="Pass the token separately"):
        client.fetch({**PARAMS, "securityToken": TOKEN}, TOKEN)


@respx.mock
def test_fetch_never_logs_the_token(caplog):
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))
    caplog.set_level(logging.DEBUG)

    client.fetch(PARAMS, TOKEN)

    assert TOKEN not in caplog.text


@respx.mock
def test_httpx_would_log_the_token_if_its_logger_were_left_at_info(caplog):
    # Shows why the client raises the httpx logger to WARNING.
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))
    caplog.set_level(logging.INFO, logger="httpx")

    client.fetch(PARAMS, TOKEN)

    assert TOKEN in caplog.text


def test_redact_removes_the_token_and_any_token_query_parameter():
    text = f"token {TOKEN} and url ?a=1&securityToken=other-token&b=2"

    assert client.redact(text, TOKEN) == "token *** and url ?a=1&securityToken=***&b=2"
