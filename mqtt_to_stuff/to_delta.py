import argparse
import sys
import paho.mqtt.client as mqtt
import deltalake
import polars as pl
import os
import threading
import time
import signal

from typing import Dict, Tuple
from collections.abc import Callable

from devices import MonitoringPlug, PresenceDetector
from register import DeviceRegister, Series

def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)

    return on_connect

def write(register, base_path):
    print("going to write", base_path)

    write_options = {
        "writer_properties": deltalake.WriterProperties(compression="zstd"),
        "partition_by":['date']
    }

    if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options = {
            "endpoint_url": S3_ENDPOINT
        }
    else:
        options = {}

    for series_name in register.series:
        series = register.series[series_name]
        df = pl.DataFrame(series.to_list())

        if df.shape[0] > 0:
            print(df)

            delta_path = base_path + series_name
            df.with_columns(
                pl.col("timestamp").dt.date().alias("date")
            ).write_delta(
                delta_path,
                mode="append",
                delta_write_options=write_options,
                storage_options=options
            )

            series.clear()

            dt = deltalake.DeltaTable(delta_path,storage_options=options)

            if len(dt.file_uris()) >= 24:
                dt.optimize.compact()
                dt.create_checkpoint()


def periodic_batch_writer(register, base_path, interval):
    while True:
        time.sleep(interval)
        write(register, base_path)


def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to DeltaLake.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.", default=os.environ.get('MQTT_TOPIC'))
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=os.environ.get('INTERVAL', 300))

    args = parser.parse_args()

    register = DeviceRegister()
    register.add_device_type("plug", MonitoringPlug)
    register.add_device_type("presence", PresenceDetector)


    uptime = Series("iot_device_uptime")
    habitat = Series("habitat")
    presence = Series("presence")
    electricity = Series("electricity")

    series = [
        uptime,
        electricity,
        presence,
        habitat
    ]

    for s in series:
        register.add_series(s)

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            _, zone, area, kind, thing, *rest = msg.topic.split("/")
            key = (("zone", zone), ("area", area), ("thing", thing))
            register.append_data(kind, key, tuple(rest), payload)

        except ValueError as e:
            print(e)
            print("exception: " + msg.topic)
            return


    # Start periodic batch writer thread
    batch_thread = threading.Thread(
        target=periodic_batch_writer, 
        args=(register, args.delta_path, args.interval), 
        daemon=True
    )
    batch_thread.start()

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(args.topics)
    mqttc.on_message = on_message


    def sigterm_handler(SIGNAL, STACK_FRAME):
        write(register, args.delta_path)
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
