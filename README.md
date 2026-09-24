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

### Fetch data from ENTSO-E

Fetch one delivery day and save the raw XML, gzipped, under `data/raw/`. The date is the local delivery day in Europe/Zurich time:

```
uv run power-pipeline fetch --dataset prices --zone DE_LU --date 2026-09-23
```

Zones and datasets are defined in `config/zones.yaml` and `config/datasets.yaml`.

### dbt

The dbt project lives in `dbt/`. dbt does not read `.env` itself, so pass it through uv:

```
cd dbt
uv run dbt deps
uv run --env-file ../.env dbt debug
uv run --env-file ../.env dbt build --target dev
```

The `dev` target writes to schemas with a `_dev` suffix, such as `power_staging_dev`, and builds only the last 14 days. To build from a given date, add `--vars '{start_date: 2024-01-01}'`.

### Databricks jobs

The Databricks CLI reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment. It does not read `.env`. The simplest setup is a CLI profile, made once:

```
databricks configure
```

Jobs read the ENTSO-E token from a secret scope called `power-pipeline`. Create it once. The second command asks for the token, so it never lands in your shell history:

```
databricks secrets create-scope power-pipeline
databricks secrets put-secret power-pipeline entsoe-api-token
```

Check that the API, the token and the secret scope are all reachable. The command never prints the token:

```
uv run power-pipeline diagnose --secret-scope power-pipeline
```

Create the raw schema, the landing volume and the bronze tables. The command is safe to run again, since it only creates what is missing:

```
uv run power-pipeline setup
```

Bronze tables are append-only. Delta rejects any `UPDATE` or `DELETE` on them.

Jobs are defined in `databricks.yml` and deployed as a bundle:

```
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run spike -t dev
```

## Data source

Electricity market data comes from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu).
