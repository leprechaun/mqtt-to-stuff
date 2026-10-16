import argparse
import sys

from . import raw_to_delta, to_delta, zigbee_to_delta
from .cli_args import add_common_arguments, validate_common_arguments
from .logging_setup import configure_logging

COMMANDS = [
    zigbee_to_delta.COMMAND,
    raw_to_delta.COMMAND,
    to_delta.COMMAND,
]
COMMANDS_BY_NAME = {command.name: command for command in COMMANDS}


def build_parser():
    parser = argparse.ArgumentParser(prog="mqtt-to-stuff", description="MQTT to Delta Lake collectors.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers_by_name = {}
    for command in COMMANDS:
        subparser = subparsers.add_parser(command.name, help=command.help, description=command.help)
        add_common_arguments(subparser)
        command.add_arguments(subparser)
        subparsers_by_name[command.name] = subparser

    return parser, subparsers_by_name


def parse_args(argv=None):
    parser, subparsers_by_name = build_parser()
    args = parser.parse_args(argv)
    validate_common_arguments(subparsers_by_name[args.command], args)
    COMMANDS_BY_NAME[args.command].normalize(args)

    return args


def cli():
    configure_logging()
    args = parse_args(sys.argv[1:])
    sys.exit(COMMANDS_BY_NAME[args.command].run(args))


if __name__ == "__main__":
    cli()
