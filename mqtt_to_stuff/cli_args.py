import os


def add_common_arguments(parser):
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=int(os.environ.get('INTERVAL', "300")))


def validate_common_arguments(parser, args):
    if not args.host:
        parser.error("--host is required (or set the MQTT_HOST environment variable)")
