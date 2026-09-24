"""Command-line entry point: `power-pipeline`."""

import os
import platform
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import typer
from dotenv import find_dotenv, load_dotenv

from power_pipeline import databricks_io, ingest
from power_pipeline.entsoe import client, datasets

app = typer.Typer(
    help="Collect ENTSO-E electricity market data for CH and DE-LU.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Collect ENTSO-E electricity market data for CH and DE-LU."""
    # Local runs read settings from .env. Databricks jobs have no .env, so this does nothing.
    load_dotenv(find_dotenv(usecwd=True))


@app.command("version")
def show_version() -> None:
    """Print the installed package version."""
    typer.echo(version("power-pipeline"))


@app.command()
def diagnose(
    secret_scope: Annotated[
        str | None,
        typer.Option(help="Databricks secret scope that holds the ENTSO-E token."),
    ] = None,
) -> None:
    """Check the Python version, access to the ENTSO-E API and where the token is.

    Never prints the token. Exits with code 1 if the API is unreachable, the token is
    missing everywhere, or the secret scope cannot be read.
    """
    failed = False
    typer.echo(f"python: {platform.python_version()}")

    reach = client.check_reachable()
    typer.echo(f"entsoe api: {'reachable' if reach.ok else 'NOT reachable'} ({reach.detail})")
    failed |= not reach.ok

    token_in_env = bool(os.environ.get(client.TOKEN_ENV_VAR, "").strip())
    typer.echo(f"token in environment: {'found' if token_in_env else 'not set'}")

    token_in_scope = False
    if secret_scope:
        label = f"token in secret scope {secret_scope!r}"
        try:
            token_in_scope = bool(databricks_io.get_secret(secret_scope, client.TOKEN_SECRET_KEY))
        except Exception as exc:  # report any failure: finding it is the point
            typer.echo(f"{label}: error ({type(exc).__name__}: {exc})")
            failed = True
        else:
            typer.echo(f"{label}: {'found' if token_in_scope else 'empty'}")
            failed |= not token_in_scope

    if not (token_in_env or token_in_scope):
        typer.echo("token: not found in any source")
        failed = True

    raise typer.Exit(code=1 if failed else 0)


@app.command()
def fetch(
    dataset: Annotated[str, typer.Option(help="Dataset from config/datasets.yaml.")],
    zone: Annotated[str, typer.Option(help="Zone from config/zones.yaml, for example DE_LU.")],
    delivery_date: Annotated[
        datetime,
        typer.Option("--date", formats=["%Y-%m-%d"], help="Delivery day, local time."),
    ],
    secret_scope: Annotated[
        str | None,
        typer.Option(help="Secret scope with the ENTSO-E token, if ENTSOE_API_TOKEN is unset."),
    ] = None,
    data_dir: Annotated[Path, typer.Option(help="Local data folder.")] = Path("data"),
) -> None:
    """Fetch one delivery day from ENTSO-E and save the raw XML, gzipped, under data/raw/."""
    known_datasets = datasets.load_datasets()
    known_zones = datasets.load_zones()
    if dataset not in known_datasets:
        raise typer.BadParameter(f"choose from {sorted(known_datasets)}", param_hint="--dataset")
    if zone not in known_zones:
        raise typer.BadParameter(f"choose from {sorted(known_zones)}", param_hint="--zone")

    start, end = ingest.delivery_day_window_utc(delivery_date.date())
    params = datasets.build_params(known_datasets[dataset], known_zones[zone], start, end)
    try:
        token = client.get_token(secret_scope)
        result = client.fetch(params, token)
    except (client.MissingTokenError, client.EntsoeApiError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    relative_path = ingest.landing_relative_path(dataset, zone, start, end, datetime.now(UTC))
    path = ingest.local_raw_path(data_dir, relative_path)
    ingest.write_raw_file(result.content, path)
    typer.echo(f"window: {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC")
    typer.echo(
        f"status: {result.status} (HTTP {result.http_status}, "
        f"{result.root_element}, {len(result.content)} bytes)"
    )
    typer.echo(f"saved: {path}")


@app.command()
def setup(
    warehouse_id: Annotated[
        str | None,
        typer.Option(help="SQL warehouse ID. Defaults to the one in DATABRICKS_HTTP_PATH."),
    ] = None,
) -> None:
    """Create the raw schema, landing volume and bronze tables if they do not exist.

    Safe to run any number of times. Existing objects are left as they are.
    """
    warehouse_id = warehouse_id or databricks_io.warehouse_id_from_env()
    for statement in ingest.setup_statements():
        databricks_io.run_sql(statement, warehouse_id)
        typer.echo(f"ok: {statement.splitlines()[0]}")


def run_job() -> None:
    """Entry point for Databricks Python wheel tasks: `power-pipeline-job`.

    Databricks runs wheel tasks inside IPython, which marks the task failed on any
    SystemExit, even exit code 0. Typer always exits, so run it without exiting and
    raise SystemExit only when the command fails.
    """
    exit_code = app(standalone_mode=False)
    if exit_code:
        raise SystemExit(exit_code)
