import platform
from importlib import metadata


# pylint: disable=anomalous-backslash-in-string
CLI_HEADER = f"""\n  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

  ___       _        _        _    _           ___  ___  _       ___      ___ 
 |   \ __ _| |_ __ _| |__ _ _(_)__| |__ ______/ __|/ _ \| |  ___| _ \___ / __|
 | |) / _` |  _/ _` | '_ \ '_| / _| / /(_-<___\__ \ (_) | |_|___|  _/ _ \ (__ 
 |___/\__,_|\__\__,_|_.__/_| |_\__|_\_\/__/   |___/\__\_\____|  |_| \___/\___|
                                                                              

    cli-version:\t{metadata.version(__name__.split('.', maxsplit=1)[0])}
    platform:\t\t{platform.platform()}
\n  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #\n"""
