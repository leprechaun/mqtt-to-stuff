import sys

from . import raw_to_delta, to_delta, zigbee_to_delta

WORKERS = {
    "zigbee": zigbee_to_delta.main,
    "raw": raw_to_delta.main,
    "hierarchy": to_delta.main,
}


def cli():
    if len(sys.argv) < 2 or sys.argv[1] not in WORKERS:
        sys.stderr.write(f"usage: mqtt-to-stuff {{{','.join(WORKERS)}}} [args...]\n")
        sys.exit(2)

    worker = sys.argv[1]
    sys.exit(WORKERS[worker](sys.argv[2:]))


if __name__ == "__main__":
    cli()
