import logging
import os
import signal
import threading
import time

import paho.mqtt.client as mqtt
import polars as pl

from .commands import Command
from .delta_client import DeltaLakeClient
from .devices import MonitoringPlug, MultiPresenceDetector, PresenceDetector
from .mqtt_helpers import generate_on_connect
from .register import DeviceRegister, Series
from .storage_options import build_storage_options

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


def on_message(client, userdata, msg):
    register = userdata
    try:
        payload = msg.payload.decode("utf-8")
        _, zone, area, kind, thing, *rest = msg.topic.split("/")
        key = (("zone", zone), ("area", area), ("thing", thing))
        register.append_data(kind, key, tuple(rest), payload)

    except ValueError:
        logger.exception("failed to parse topic", extra={"topic": msg.topic})
        return


def add_arguments(parser):
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.")


def normalize(args):
    if not args.topics and (env_topic := os.environ.get('MQTT_TOPIC')):
        args.topics = [env_topic]


def run(args):
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

    options = build_storage_options(include_session_token=False)

    dlc = DeltaLakeClient(args.delta_path, options, compact_at=24)

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
    mqttc.user_data_set(register)


    def handle_shutdown(signum, frame):
        logger.info("received signal, shutting down", extra={"signum": signum})
        mqttc.disconnect()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    logger.info("Flushing remaining messages before exit")
    write(register, dlc)

    return 0


COMMAND = Command(
    name="hierarchy",
    help="Copy MQTT events to DeltaLake.",
    add_arguments=add_arguments,
    run=run,
    normalize=normalize,
)
