import copy
import logging
from pathlib import Path

import httpx
import pytest
import respx
from lxml import etree

from power_pipeline.entsoe import client

FIXTURES = Path(__file__).parent / "fixtures"
AUTH_FAILED = (FIXTURES / "ack_authentication_failed.xml").read_bytes()
NO_DATA = (FIXTURES / "ack_no_data.xml").read_bytes()
PRICES = (FIXTURES / "prices_de_lu_2026-09-23.xml").read_bytes()
PRICES_CH = (FIXTURES / "prices_ch_2026-09-23.xml").read_bytes()

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
def test_fetch_sends_the_token_as_a_header_and_never_in_the_url():
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))

    client.fetch(PARAMS, TOKEN)

    request = route.calls.last.request
    assert request.headers["SECURITY_TOKEN"] == TOKEN
    assert dict(request.url.params) == PARAMS
    assert TOKEN not in str(request.url)


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
def test_token_stays_out_of_logs_even_with_http_library_logging_turned_on(caplog):
    # The token is in a header, not the URL, so request logging cannot leak it.
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=PRICES))
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    client.fetch(PARAMS, TOKEN)

    assert "HTTP Request: GET" in caplog.text
    assert TOKEN not in caplog.text


def test_redact_removes_the_token_and_any_token_query_parameter():
    text = f"token {TOKEN} and url ?a=1&securityToken=other-token&b=2"

    assert client.redact(text, TOKEN) == "token *** and url ?a=1&securityToken=***&b=2"


# Retries


class RecordingSleep:
    def __init__(self):
        self.calls = []

    def __call__(self, seconds):
        self.calls.append(seconds)


@respx.mock
@pytest.mark.parametrize("first_status", [429, 500, 502, 503, 504])
def test_fetch_retries_rate_limits_and_server_errors(first_status):
    route = respx.get(client.BASE_URL).mock(
        side_effect=[httpx.Response(first_status, text="busy"), httpx.Response(200, content=PRICES)]
    )
    sleep = RecordingSleep()

    result = client.fetch(PARAMS, TOKEN, sleep=sleep)

    assert result.status == "data"
    assert route.call_count == 2
    assert len(sleep.calls) == 1


@respx.mock
def test_fetch_waits_as_long_as_retry_after_asks():
    respx.get(client.BASE_URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, content=PRICES),
        ]
    )
    sleep = RecordingSleep()

    client.fetch(PARAMS, TOKEN, sleep=sleep)

    assert sleep.calls == [7.0]


@respx.mock
def test_fetch_backs_off_exponentially_without_retry_after():
    respx.get(client.BASE_URL).mock(
        side_effect=[httpx.Response(503)] * 3 + [httpx.Response(200, content=PRICES)]
    )
    sleep = RecordingSleep()

    client.fetch(PARAMS, TOKEN, sleep=sleep)

    # About 1, 2 and 4 seconds, each plus up to 1 second of random jitter.
    assert len(sleep.calls) == 3
    for base, waited in zip([1, 2, 4], sleep.calls, strict=True):
        assert base <= waited <= base + 1


@respx.mock
def test_fetch_gives_up_after_max_attempts_and_keeps_the_last_response():
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(503, text="maintenance"))

    with pytest.raises(client.EntsoeApiError, match="HTTP 503") as exc_info:
        client.fetch(PARAMS, TOKEN, max_attempts=3)

    assert route.call_count == 3
    assert exc_info.value.http_status == 503
    assert exc_info.value.content == b"maintenance"


@respx.mock
def test_fetch_retries_network_errors():
    respx.get(client.BASE_URL).mock(
        side_effect=[httpx.ConnectTimeout("slow"), httpx.Response(200, content=PRICES)]
    )

    assert client.fetch(PARAMS, TOKEN).status == "data"


@respx.mock
def test_fetch_gives_up_on_persistent_network_errors():
    route = respx.get(client.BASE_URL).mock(side_effect=httpx.ConnectError("down"))

    with pytest.raises(client.EntsoeApiError, match="ConnectError"):
        client.fetch(PARAMS, TOKEN, max_attempts=4)

    assert route.call_count == 4


@respx.mock
@pytest.mark.parametrize(("status", "content"), [(401, AUTH_FAILED), (400, b"bad request")])
def test_fetch_does_not_retry_client_errors(status, content):
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(status, content=content))

    with pytest.raises(client.EntsoeApiError):
        client.fetch(PARAMS, TOKEN)

    assert route.call_count == 1


@respx.mock
def test_fetch_does_not_retry_a_no_data_answer():
    route = respx.get(client.BASE_URL).mock(return_value=httpx.Response(400, content=NO_DATA))

    assert client.fetch(PARAMS, TOKEN).status == "no_data"
    assert route.call_count == 1


@respx.mock
def test_errors_keep_the_status_and_body_for_raw_storage():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(401, content=AUTH_FAILED))

    with pytest.raises(client.EntsoeApiError) as exc_info:
        client.fetch(PARAMS, TOKEN)

    assert exc_info.value.http_status == 401
    assert exc_info.value.content == AUTH_FAILED


@respx.mock
def test_retry_logs_say_what_failed_without_the_token(caplog):
    respx.get(client.BASE_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, content=PRICES)]
    )
    caplog.set_level(logging.DEBUG)

    client.fetch(PARAMS, TOKEN)

    assert "ENTSO-E request failed (HTTP 503). Retrying in" in caplog.text
    assert "attempt 2 of 5" in caplog.text
    assert TOKEN not in caplog.text


# Rate limiting


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.slept = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_rate_limiter_spaces_requests():
    clock = FakeClock()
    limiter = client.RateLimiter(0.5, clock=clock, sleep=clock.sleep)

    limiter.wait()  # first request: no wait
    clock.now += 0.2
    limiter.wait()  # 0.2 s later: waits the remaining 0.3 s
    clock.now += 1.0
    limiter.wait()  # 1 s later: no wait

    assert clock.slept == [pytest.approx(0.3)]


def test_rate_limiter_with_zero_interval_never_waits():
    clock = FakeClock()
    limiter = client.RateLimiter(0, clock=clock, sleep=clock.sleep)

    for _ in range(3):
        limiter.wait()

    assert clock.slept == []


@respx.mock
def test_fetch_goes_through_the_rate_limiter_on_every_try():
    respx.get(client.BASE_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, content=PRICES)]
    )
    waits = []

    class CountingLimiter:
        def wait(self):
            waits.append(True)

    client.fetch(PARAMS, TOKEN, rate_limiter=CountingLimiter())

    assert len(waits) == 2


def test_default_request_spacing_stays_well_under_the_api_limit():
    requests_per_minute = 60 / client.MIN_REQUEST_INTERVAL_S

    assert requests_per_minute <= 200  # ENTSO-E allows 400 a minute per token


# Truncation guard


def with_time_series(count: int) -> bytes:
    """Derived: the real CH price document with its one TimeSeries copied `count` times."""
    root = etree.fromstring(PRICES_CH)
    [series] = [child for child in root if etree.QName(child).localname == "TimeSeries"]
    for _ in range(count - 1):
        root.append(copy.deepcopy(series))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


@respx.mock
def test_fetch_refuses_a_response_that_may_be_cut_off():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=with_time_series(100)))

    with pytest.raises(client.EntsoeApiError, match="100 TimeSeries, the API maximum"):
        client.fetch(PARAMS, TOKEN)


@respx.mock
def test_fetch_accepts_a_response_below_the_limit():
    respx.get(client.BASE_URL).mock(return_value=httpx.Response(200, content=with_time_series(99)))

    assert client.fetch(PARAMS, TOKEN).status == "data"
