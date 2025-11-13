import logging
import sys


def initialize_logging(level: int = logging.INFO, fmt: str | None = None) -> None:
    """Set basic logging configuration.

    Configuration of the root logger (existing handlers attached to the root logger are removed and closed) to log
    any message to stdout.

    Configuration of the "project" logger as a child of the root logger, so all messages send via that logger are also
    send to stdout.

    Args:
        level: the log-level to set on the root & project-loggers
        fmt: the format of log messages
    """

    # mute logger
    logging.getLogger("sqlglot").setLevel(logging.ERROR)
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

    # one-shot configuration of root logger
    # log basically everything to stdout
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format=fmt or "%(asctime)s - dbsql-migration-tool - [%(levelname)s] - %(message)s",
    )

    # configure project logger
    # since it's a child of the root logger, everything that is logged via the project logger is also put to stdout
    plogger_name = __name__.split(".", maxsplit=1)[0]  # assuming __name__ is used for module level logger names
    plogger = logging.getLogger(plogger_name)
    plogger.setLevel(level)

    plogger.debug("logging initialized!")
