from importlib.metadata import version

from typer.testing import CliRunner

from power_pipeline.cli import app

runner = CliRunner()


def test_version_prints_installed_package_version():
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.output.strip() == version("power-pipeline")


def test_help_lists_the_version_command():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "version" in result.output
