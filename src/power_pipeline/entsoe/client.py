"""HTTP access to the ENTSO-E Transparency Platform API."""

import logging
import os
import re
from dataclasses import dataclass
from typing import Literal

import httpx
from lxml import etree

from power_pipeline import databricks_io

BASE_URL = "https://web-api.tp.entsoe.eu/api"
TOKEN_ENV_VAR = "ENTSOE_API_TOKEN"
TOKEN_SECRET_KEY = "entsoe-api-token"
TOKEN_PARAM = "securityToken"

ACKNOWLEDGEMENT_ROOT = "Acknowledgement_MarketDocument"
NO_DATA_REASON_CODE = "999"
NO_DATA_TEXT = "No matching data found"

logger = logging.getLogger(__name__)

# httpx logs every request URL at INFO level, and ENTSO-E URLs carry the token.
for _name in ("httpx", "httpcore"):
    logging.getLogger(_name).setLevel(logging.WARNING)

# No entity expansion and no network access while parsing responses.
_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True)
_TOKEN_IN_URL = re.compile(rf"{TOKEN_PARAM}=[^&\s'\"]+")


class MissingTokenError(RuntimeError):
    """No ENTSO-E API token in the environment or in the secret scope."""


class EntsoeApiError(RuntimeError):
    """The request failed, or the API answered with an error. Never holds the token."""


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


def fetch(
    params: dict[str, str],
    token: str,
    *,
    http: httpx.Client | None = None,
    timeout_s: float = 60.0,
) -> FetchResult:
    """Send one request and return the body exactly as received.

    An acknowledgement saying "No matching data found" is returned as `no_data`. Any
    other acknowledgement, HTTP error or network failure raises EntsoeApiError.
    Messages never contain the token. `request_params` holds the parameters without it.
    """
    if TOKEN_PARAM in params:
        raise ValueError(f"Pass the token separately, not as {TOKEN_PARAM} in params.")
    query = {**params, TOKEN_PARAM: token}
    try:
        response = (http or httpx).get(BASE_URL, params=query, timeout=timeout_s)
    except httpx.TransportError as exc:
        message = f"Request to ENTSO-E failed: {type(exc).__name__}: {exc}"
        raise EntsoeApiError(redact(message, token)) from None

    content = response.content
    root = _parse_xml(content)
    acknowledgement = None if root is None else _read_acknowledgement(root)
    if acknowledgement is not None:
        if acknowledgement.is_no_data:
            return FetchResult(
                status="no_data",
                http_status=response.status_code,
                root_element=ACKNOWLEDGEMENT_ROOT,
                content=content,
                request_params=dict(params),
            )
        message = f"HTTP {response.status_code}, acknowledgement {acknowledgement.describe()}"
        raise EntsoeApiError(redact(message, token))
    if response.status_code != 200:
        snippet = " ".join(response.text[:200].split())
        raise EntsoeApiError(redact(f"HTTP {response.status_code}: {snippet!r}", token))
    if root is None:
        raise EntsoeApiError("HTTP 200, but the response is not XML.")
    return FetchResult(
        status="data",
        http_status=response.status_code,
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
