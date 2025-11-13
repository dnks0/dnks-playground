"""
Entry-point script for the datalake-poc-cli.
"""

import click
import logging

from dbsql_migration_tool.common.variables import CLI_HEADER
from dbsql_migration_tool.common.logging import initialize_logging
from dbsql_migration_tool.result.cli import result
from dbsql_migration_tool.transpile.cli import transpile
from dbsql_migration_tool.experiment.cli import experiment



@click.group()
@click.version_option(message=CLI_HEADER)
@click.option(
    "--log-level",
    "-l",
    "log_level",
    required=False,
    type=click.Choice(logging._nameToLevel.keys(), case_sensitive=False),
    default="INFO",
    help="Verbosity of log messages.",
)
def cli(log_level: str,) -> None:
    """
    A custom `command-line-interface´ to manage useful stuff within the datalab universe.
    """
    initialize_logging(level=logging.getLevelName(log_level.upper()))


cli.add_command(cmd=transpile, name="transpile")
cli.add_command(cmd=experiment, name="experiment")
cli.add_command(cmd=result, name="result")


if __name__ == "__main__":
    cli()
