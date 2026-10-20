"""HTTP access to the ENTSO-E Transparency Platform API."""

import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import httpx
from lxml import etree
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from power_pipeline import databricks_io

BASE_URL = "https://web-api.tp.entsoe.eu/api"
TOKEN_ENV_VAR = "ENTSOE_API_TOKEN"
TOKEN_SECRET_KEY = "entsoe-api-token"
# The token travels in this request header, so it is never part of a URL.
TOKEN_HEADER = "SECURITY_TOKEN"
# The API also accepts the token as this query parameter. We never send it that way.
TOKEN_PARAM = "securityToken"

ACKNOWLEDGEMENT_ROOT = "Acknowledgement_MarketDocument"
NO_DATA_REASON_CODE = "999"
NO_DATA_TEXT = "No matching data found"

# Tries per request, counting the first. Retries happen on HTTP 429, HTTP 5xx and
# network errors, with exponential backoff, or the wait the server asks for.
MAX_ATTEMPTS = 5
MAX_RETRY_AFTER_S = 600.0
# ENTSO-E allows 400 requests a minute per token. Two a second stays well under it.
MIN_REQUEST_INTERVAL_S = 0.5
# The API returns at most this many TimeSeries per response and cuts off the rest.
MAX_TIME_SERIES = 100

logger = logging.getLogger(__name__)

# A second guard: keep HTTP library request logs quiet, whatever they may contain.
for _name in ("httpx", "httpcore"):
    logging.getLogger(_name).setLevel(logging.WARNING)

# No entity expansion and no network access while parsing responses.
_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True)
_TOKEN_IN_URL = re.compile(rf"{TOKEN_PARAM}=[^&\s'\"]+")


class MissingTokenError(RuntimeError):
    """No ENTSO-E API token in the environment or in the secret scope."""


class EntsoeApiError(RuntimeError):
    """The request failed, or the API answered with an error. Never holds the token.

    `http_status` and `content` hold the last response, when there was one, so the
    caller can keep the raw body.
    """

    def __init__(
        self, message: str, *, http_status: int | None = None, content: bytes | None = None
    ):
        super().__init__(message)
        self.http_status = http_status
        self.content = content


class RateLimiter:
    """Keeps at least `min_interval_s` between requests made through it."""

    def __init__(
        self,
        min_interval_s: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.min_interval_s = min_interval_s
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last is not None:
            delay = self._last + self.min_interval_s - now
            if delay > 0:
                self._sleep(delay)
                now += delay
        self._last = now


# Shared by every request in this process. Tests replace both with instant versions.
_rate_limiter = RateLimiter(MIN_REQUEST_INTERVAL_S)
_sleep: Callable[[float], None] = time.sleep


def get_token(secret_scope: str | None = None) -> str:
    """Return the ENTSO-E API token without ever logging it.

    Looks in the ENTSOE_API_TOKEN environment variable first. If that is unset or
    blank, reads the `entsoe-api-token` key from the given Databricks secret scope.
    """
    token = os.environ.get(TOKEN_ENV_VAR, "").strip()
    if token:
        logger.info("ENTSO-E token read from environment variable %s", TOKEN_ENV_VAR)
        return token
    if secret_scope:
        token = databricks_io.get_secret(secret_scope, TOKEN_SECRET_KEY)
        logger.info("ENTSO-E token read from secret scope %r", secret_scope)
        return token
    raise MissingTokenError(
        f"Set {TOKEN_ENV_VAR} or pass a secret scope that holds the key '{TOKEN_SECRET_KEY}'."
    )


def redact(text: str, token: str) -> str:
    """Remove the token from text, both on its own and as a URL query parameter."""
    text = _TOKEN_IN_URL.sub(f"{TOKEN_PARAM}=***", text)
    return text.replace(token, "***") if token else text


@dataclass(frozen=True)
class Acknowledgement:
    """ENTSO-E's reply when there is nothing to return: a list of (code, text) reasons."""

    reasons: tuple[tuple[str, str], ...]

    @property
    def is_no_data(self) -> bool:
        """True only if every reason says no matching data was found.

        ENTSO-E also uses code 999 for errors such as "Authentication failed.", so the
        code alone is not enough.
        """
        return bool(self.reasons) and all(
            code == NO_DATA_REASON_CODE and NO_DATA_TEXT in text for code, text in self.reasons
        )

    def describe(self) -> str:
        return "; ".join(f"{code}: {text}" for code, text in self.reasons) or "no reason given"


def _parse_xml(content: bytes) -> etree._Element | None:
    try:
        return etree.fromstring(content, parser=_XML_PARSER)
    except etree.XMLSyntaxError:
        return None


def _read_acknowledgement(root: etree._Element) -> Acknowledgement | None:
    qname = etree.QName(root)
    if qname.localname != ACKNOWLEDGEMENT_ROOT:
        return None
    # Read the namespace from the root element. It differs between document versions.
    ns = {"ack": qname.namespace} if qname.namespace else None
    prefix = "ack:" if ns else ""
    reasons = tuple(
        (
            (reason.findtext(f"{prefix}code", default="", namespaces=ns)).strip(),
            " ".join(reason.findtext(f"{prefix}text", default="", namespaces=ns).split()),
        )
        for reason in root.iterfind(f"{prefix}Reason", namespaces=ns)
    )
    return Acknowledgement(reasons=reasons)


def parse_acknowledgement(content: bytes) -> Acknowledgement | None:
    """Read an Acknowledgement_MarketDocument. Returns None for any other document."""
    root = _parse_xml(content)
    return None if root is None else _read_acknowledgement(root)


@dataclass(frozen=True)
class FetchResult:
    status: Literal["data", "no_data"]
    http_status: int
    root_element: str
    content: bytes
    request_params: dict[str, str]


class _RetryableResponse(Exception):
    """An HTTP answer worth retrying: 429 Too Many Requests or a 5xx server error."""

    def __init__(self, response: httpx.Response):
        super().__init__(f"HTTP {response.status_code}")
        self.response = response


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, (_RetryableResponse, httpx.TransportError))


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """The Retry-After header in seconds, if the server sent one in that form."""
    try:
        return min(max(float(response.headers["retry-after"]), 0.0), MAX_RETRY_AFTER_S)
    except (KeyError, ValueError):
        return None


_backoff = wait_exponential_jitter(initial=1, max=60)


def _wait_before_retry(state: RetryCallState) -> float:
    exc = state.outcome.exception() if state.outcome else None
    if isinstance(exc, _RetryableResponse):
        retry_after = _retry_after_seconds(exc.response)
        if retry_after is not None:
            return retry_after
    return _backoff(state)


def _log_retry(state: RetryCallState, max_attempts: int) -> None:
    exc = state.outcome.exception() if state.outcome else None
    reason = str(exc) if isinstance(exc, _RetryableResponse) else type(exc).__name__
    wait_s = state.next_action.sleep if state.next_action else 0.0
    logger.warning(
        "ENTSO-E request failed (%s). Retrying in %.1f s, attempt %d of %d.",
        reason,
        wait_s,
        state.attempt_number + 1,
        max_attempts,
    )


def _send(
    params: dict[str, str],
    token: str,
    http: httpx.Client | None,
    timeout_s: float,
    max_attempts: int,
    rate_limiter: RateLimiter,
    sleep: Callable[[float], None],
) -> httpx.Response:
    """GET with rate limiting and retries. Raises the last error when tries run out."""
    retrying = Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=_wait_before_retry,
        retry=retry_if_exception(_is_retryable),
        sleep=sleep,
        before_sleep=lambda state: _log_retry(state, max_attempts),
        reraise=True,
    )
    for attempt in retrying:
        with attempt:
            rate_limiter.wait()
            response = (http or httpx).get(
                BASE_URL, params=params, headers={TOKEN_HEADER: token}, timeout=timeout_s
            )
            if response.status_code == 429 or response.status_code >= 500:
                raise _RetryableResponse(response)
            return response
    raise AssertionError("unreachable: tenacity re-raises the last error")


def _time_series_count(root: etree._Element) -> int:
    return sum(1 for child in root if etree.QName(child).localname == "TimeSeries")


def fetch(
    params: dict[str, str],
    token: str,
    *,
    http: httpx.Client | None = None,
    timeout_s: float = 60.0,
    max_attempts: int = MAX_ATTEMPTS,
    rate_limiter: RateLimiter | None = None,
    sleep: Callable[[float], None] | None = None,
) -> FetchResult:
    """Send one request and return the body exactly as received.

    The token is sent in the SECURITY_TOKEN header, never in the URL. Requests are spaced
    by the shared rate limiter. HTTP 429, HTTP 5xx and network errors are retried with
    backoff, up to `max_attempts` tries. An acknowledgement saying "No matching data
    found" is returned as `no_data`. Any other acknowledgement, a remaining HTTP error,
    a network failure or a response that may be cut off raises EntsoeApiError. Messages
    never contain the token, and `request_params` holds exactly the parameters sent.
    """
    if TOKEN_PARAM in params:
        raise ValueError(f"Pass the token separately, not as {TOKEN_PARAM} in params.")
    try:
        response = _send(
            params,
            token,
            http,
            timeout_s,
            max_attempts,
            rate_limiter or _rate_limiter,
            sleep or _sleep,
        )
    except _RetryableResponse as exc:
        response = exc.response  # tries used up: report it below like any HTTP error
    except httpx.TransportError as exc:
        message = f"Request to ENTSO-E failed: {type(exc).__name__}: {exc}"
        raise EntsoeApiError(redact(message, token)) from None

    status, content = response.status_code, response.content
    root = _parse_xml(content)
    acknowledgement = None if root is None else _read_acknowledgement(root)
    if acknowledgement is not None:
        if acknowledgement.is_no_data:
            return FetchResult(
                status="no_data",
                http_status=status,
                root_element=ACKNOWLEDGEMENT_ROOT,
                content=content,
                request_params=dict(params),
            )
        message = f"HTTP {status}, acknowledgement {acknowledgement.describe()}"
        raise EntsoeApiError(redact(message, token), http_status=status, content=content)
    if status != 200:
        snippet = " ".join(response.text[:200].split())
        raise EntsoeApiError(
            redact(f"HTTP {status}: {snippet!r}", token), http_status=status, content=content
        )
    if root is None:
        raise EntsoeApiError(
            "HTTP 200, but the response is not XML.", http_status=status, content=content
        )
    series_count = _time_series_count(root)
    if series_count >= MAX_TIME_SERIES:
        raise EntsoeApiError(
            f"The response holds {series_count} TimeSeries, the API maximum, so it may be "
            "cut off. Request a shorter window.",
            http_status=status,
            content=content,
        )
    return FetchResult(
        status="data",
        http_status=status,
        root_element=etree.QName(root).localname,
        content=content,
        request_params=dict(params),
    )


@dataclass(frozen=True)
class Reachability:
    ok: bool
    detail: str


def check_reachable(timeout_s: float = 15.0) -> Reachability:
    """Send one request without a token to test the network path to the API.

    Any HTTP response counts as reachable, including 401 Unauthorized. A connection
    error or timeout means something between us and ENTSO-E blocks the request.
    """
    try:
        response = httpx.get(BASE_URL, timeout=timeout_s)
    except httpx.TransportError as exc:
        return Reachability(ok=False, detail=f"{type(exc).__name__}: {exc}")
    server = response.headers.get("server", "unknown")
    body_start = " ".join(response.text[:120].split())
    return Reachability(
        ok=True,
        detail=f"HTTP {response.status_code}, server {server!r}, body starts {body_start!r}",
    )
