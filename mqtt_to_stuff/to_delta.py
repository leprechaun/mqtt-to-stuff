import argparse
import logging
import os
import signal
import sys
import threading
import time

import paho.mqtt.client as mqtt
import polars as pl
from delta_client import DeltaLakeClient
from devices import MonitoringPlug, MultiPresenceDetector, PresenceDetector
from mqtt_helpers import generate_on_connect
from register import DeviceRegister, Series

logging.basicConfig(encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)

def write(register, dlc):
    for series_name in register.series:
        series = register.series[series_name]
        df = pl.DataFrame(series.to_list())

        if df.shape[0] > 0:
            logger.info("%s", df)

            df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
            dlc.append(df, series_name, {"partition_by": ["date"]})

            series.clear()


def periodic_batch_writer(register, dlc, interval):
    while True:
        time.sleep(interval)
        write(register, dlc)


def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to DeltaLake.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.")
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=int(os.environ.get('INTERVAL', "300")))

    args = parser.parse_args()

    if not args.topics and (env_topic := os.environ.get('MQTT_TOPIC')):
        args.topics = [env_topic]

    register = DeviceRegister()
    register.add_device_type("plug", MonitoringPlug)
    register.add_device_type("presence", PresenceDetector)
    register.add_device_type("multi-presence", MultiPresenceDetector)


    uptime = Series("iot_device_uptime", 1)
    habitat = Series("habitat", 1)
    presence = Series("presence", 1)
    multi_presence = Series("multi-presence", 0.6)
    electricity = Series("electricity", 1)

    series = [
        uptime,
        electricity,
        presence,
        multi_presence,
        habitat
    ]

    for s in series:
        register.add_series(s)

    if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options = {
            "endpoint_url": S3_ENDPOINT
        }
    else:
        options = {}

    dlc = DeltaLakeClient(args.delta_path, options, compact_at=24)

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            _, zone, area, kind, thing, *rest = msg.topic.split("/")
            key = (("zone", zone), ("area", area), ("thing", thing))
            register.append_data(kind, key, tuple(rest), payload)

        except ValueError:
            logger.exception("exception: %s", msg.topic)
            return


    # Start periodic batch writer thread
    batch_thread = threading.Thread(
        target=periodic_batch_writer,
        args=(register, dlc, args.interval),
        daemon=True
    )
    batch_thread.start()

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(args.topics)
    mqttc.on_message = on_message


    def sigterm_handler(SIGNAL, STACK_FRAME):
        write(register, dlc)
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
