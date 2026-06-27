import argparse
import sys
import datetime
import logging
import os
import signal
import time
import threading

import paho.mqtt.client as mqtt
import deltalake
import polars as pl

logging.basicConfig(encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)

buffer = []
buffer_lock = threading.Lock()


class DeltaLakeClient:
    def __init__(self, base_path, storage_options):
        self._base_path = base_path
        self._storage_options = storage_options
        self._logger = logging.getLogger(self.__class__.__name__)

    def append(self, df, path, write_options=None):
        if write_options is None:
            write_options = {}

        write_options["writer_properties"] = deltalake.WriterProperties(compression="zstd")

        df.write_delta(
            self._base_path + path,
            delta_write_options=write_options,
            storage_options=self._storage_options,
            mode="append",
        )

        dt = deltalake.DeltaTable(self._base_path + path, storage_options=self._storage_options)
        if len(dt.file_uris()) >= 60:
            dt.optimize.compact()
            dt.create_checkpoint()
            self._logger.info("Compacted %s", path)


def on_message(client, userdata, msg):
    ignored_topics = userdata or []
    if any(mqtt.topic_matches_sub(pattern, msg.topic) for pattern in ignored_topics):
        logger.debug("ignored %s", msg.topic)
        return

    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("recv %s retain=%s %s", msg.topic, bool(msg.retain), payload)
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
        logger.info("Wrote %d records to raw-mqtt", len(df))
    except Exception:
        logger.exception("Failed to write batch to Delta Lake")


def flush_buffer(dlc, interval):
    while True:
        time.sleep(interval)
        do_flush(dlc)


def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)
        logger.info("Connected, subscribed to %s", topics)

    return on_connect


def main(args):
    parser = argparse.ArgumentParser(description="Archive all MQTT messages to Delta Lake.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=os.environ.get('INTERVAL', 60))
    parser.add_argument("--ignore", dest="ignored_topics", action="append", default=[], metavar="TOPIC", help="Topic filter to ignore (repeatable, supports MQTT wildcards)")

    args = parser.parse_args()

    options = {}
    if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options["endpoint_url"] = S3_ENDPOINT
    options["AWS_SESSION_TOKEN"] = os.environ.get('AWS_SESSION_TOKEN', "")

    dlc = DeltaLakeClient(args.delta_path, options)

    flush_thread = threading.Thread(target=flush_buffer, args=(dlc, args.interval), daemon=True)
    flush_thread.start()

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(["#"])
    mqttc.on_message = on_message
    mqttc.user_data_set(args.ignored_topics)

    def handle_shutdown(signum, frame):
        logger.info("Received signal %d, shutting down", signum)
        mqttc.disconnect()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    mqttc.connect(args.host, 1883, 60)
    mqttc.loop_forever()

    logger.info("Flushing remaining messages before exit")
    do_flush(dlc)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
