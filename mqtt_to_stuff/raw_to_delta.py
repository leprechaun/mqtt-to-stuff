import argparse
import datetime
import logging
import signal
import threading
import time

import paho.mqtt.client as mqtt
import polars as pl

from .cli_args import add_common_arguments
from .delta_client import DeltaLakeClient
from .mqtt_helpers import generate_on_connect
from .storage_options import build_storage_options

logger = logging.getLogger(__name__)

buffer = []
buffer_lock = threading.Lock()


def on_message(client, userdata, msg):
    ignored_topics = userdata or []
    if any(mqtt.topic_matches_sub(pattern, msg.topic) for pattern in ignored_topics):
        logger.debug("ignored", extra={"topic": msg.topic})
        return

    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("recv", extra={"topic": msg.topic, "retain": bool(msg.retain)})
    record = {
        "topic": msg.topic,
        "arrival_timestamp": datetime.datetime.now(),
        "payload": payload,
        "retain": bool(msg.retain),
    }
    with buffer_lock:
        buffer.append(record)


def do_flush(dlc):
    with buffer_lock:
        if not buffer:
            return
        batch = buffer.copy()
        buffer.clear()

    try:
        df = pl.DataFrame(batch).with_columns(date=pl.col("arrival_timestamp").dt.date())
        dlc.append(df, "raw-mqtt", {"partition_by": ["date"]})
    except Exception:
        logger.exception("Failed to write batch to Delta Lake", extra={"batch_size": len(batch)})


def flush_buffer(dlc, interval):
    while True:
        time.sleep(interval)
        do_flush(dlc)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Archive all MQTT messages to Delta Lake.")
    add_common_arguments(parser)
    parser.add_argument("--ignore", dest="ignored_topics", action="append", default=[], metavar="TOPIC", help="Topic filter to ignore (repeatable, supports MQTT wildcards)")

    return parser.parse_args(argv)


def main(args):
    args = parse_args(args)

    options = build_storage_options(include_session_token=True)

    dlc = DeltaLakeClient(args.delta_path, options)

    flush_thread = threading.Thread(target=flush_buffer, args=(dlc, args.interval), daemon=True)
    flush_thread.start()

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(["#"])
    mqttc.on_message = on_message
    mqttc.user_data_set(args.ignored_topics)

    def handle_shutdown(signum, frame):
        logger.info("received signal, shutting down", extra={"signum": signum})
        mqttc.disconnect()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    mqttc.connect(args.host, 1883, 60)
    mqttc.loop_forever()

    logger.info("Flushing remaining messages before exit")
    do_flush(dlc)

    return 0
