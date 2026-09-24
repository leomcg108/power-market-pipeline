from unittest.mock import MagicMock

from power_pipeline import databricks_io


def test_get_secret_reads_through_workspace_dbutils(monkeypatch):
    workspace = MagicMock()
    workspace.dbutils.secrets.get.return_value = "secret-value"
    monkeypatch.setattr(databricks_io, "WorkspaceClient", lambda: workspace)

    assert databricks_io.get_secret("power-pipeline", "entsoe-api-token") == "secret-value"
    workspace.dbutils.secrets.get.assert_called_once_with(
        scope="power-pipeline", key="entsoe-api-token"
    )
