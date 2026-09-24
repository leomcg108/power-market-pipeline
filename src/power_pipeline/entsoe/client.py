"""HTTP access to the ENTSO-E Transparency Platform API."""

import logging
import os
from dataclasses import dataclass

import httpx

from power_pipeline import databricks_io

BASE_URL = "https://web-api.tp.entsoe.eu/api"
TOKEN_ENV_VAR = "ENTSOE_API_TOKEN"
TOKEN_SECRET_KEY = "entsoe-api-token"

logger = logging.getLogger(__name__)


class MissingTokenError(RuntimeError):
    """No ENTSO-E API token in the environment or in the secret scope."""


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
