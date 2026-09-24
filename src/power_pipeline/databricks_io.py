"""Helpers for Databricks: secrets for now, volume uploads and SQL later."""

from databricks.sdk import WorkspaceClient


def get_secret(scope: str, key: str) -> str:
    """Read one secret from a Databricks secret scope.

    Works locally, authenticated by DATABRICKS_HOST and DATABRICKS_TOKEN, and inside a
    Databricks job, authenticated as the job's identity.
    """
    return WorkspaceClient().dbutils.secrets.get(scope=scope, key=key)
