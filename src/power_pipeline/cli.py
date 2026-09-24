"""Command-line entry point: `power-pipeline`."""

from importlib.metadata import version

import typer

app = typer.Typer(
    help="Collect ENTSO-E electricity market data for CH and DE-LU.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Collect ENTSO-E electricity market data for CH and DE-LU."""


@app.command("version")
def show_version() -> None:
    """Print the installed package version."""
    typer.echo(version("power-pipeline"))
