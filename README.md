# Power market pipeline

A daily data pipeline for European electricity market data. It collects day-ahead prices, electricity demand, generation by source and cross-border flows for Switzerland (CH) and Germany-Luxembourg (DE-LU) from the ENTSO-E Transparency Platform. It stores the data exactly as received, models it with dbt on Databricks and shows it on a dashboard.

The project is in its first phase. The repo holds the tooling and CI so far. The pipeline itself is not built yet.

## Getting started

### What you need

- [uv](https://docs.astral.sh/uv/) to manage Python and dependencies. uv installs Python 3.12 for you.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install).
- An ENTSO-E Transparency Platform API token. Register at [transparency.entsoe.eu](https://transparency.entsoe.eu), then ask for API access by email to transparency@entsoe.eu.
- A Databricks workspace. Free Edition works. You need a personal access token and the HTTP path of a SQL warehouse.

### Set up

1. Clone the repo and install dependencies:

   ```
   git clone https://github.com/leomcg108/power-market-pipeline.git
   cd power-market-pipeline
   uv sync
   ```

2. Install the pre-commit hooks. They run ruff and a secret scanner (gitleaks) before every commit:

   ```
   uv run pre-commit install
   ```

3. Copy `.env.example` to `.env` and fill in the values. Git ignores `.env`, so your tokens stay on your machine.

   ```
   cp .env.example .env
   ```

### Check that it works

```
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run power-pipeline --help
```

## Data source

Electricity market data comes from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu).
