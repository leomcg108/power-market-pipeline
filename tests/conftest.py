import pytest

SETTING_NAMES = (
    "ENTSOE_API_TOKEN",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_HTTP_PATH",
)


@pytest.fixture(autouse=True)
def isolate_from_real_settings(monkeypatch):
    """Keep every test away from the developer's .env file and real tokens."""
    monkeypatch.setattr("power_pipeline.cli.load_dotenv", lambda *args, **kwargs: False)
    for name in SETTING_NAMES:
        monkeypatch.delenv(name, raising=False)
