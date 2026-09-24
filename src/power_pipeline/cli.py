"""Command-line entry point: `power-pipeline`."""

import os
import platform
from importlib.metadata import version
from typing import Annotated

import typer
from dotenv import find_dotenv, load_dotenv

from power_pipeline import databricks_io
from power_pipeline.entsoe import client

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


def run_job() -> None:
    """Entry point for Databricks Python wheel tasks: `power-pipeline-job`.

    Databricks runs wheel tasks inside IPython, which marks the task failed on any
    SystemExit, even exit code 0. Typer always exits, so run it without exiting and
    raise SystemExit only when the command fails.
    """
    exit_code = app(standalone_mode=False)
    if exit_code:
        raise SystemExit(exit_code)
