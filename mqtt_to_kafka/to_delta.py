import argparse
import sys
import paho.mqtt.client as mqtt
import deltalake
import polars
import os
import threading
import time

from typing import Dict, Tuple
from collections.abc import Callable


class MonitoredDevice:
    data = {}
    keys = {}
    changed = False

    records = []

    def __init__(self, location):
        self.location = location

    def set(self, key, value):
        if key in self.keys:
            value = self.keys[key][0](value)

            if self.data.get(key) != value:
                self.data[key] = value
                self.changed = True

    def get_change_record(self):
        if self.changed:
            self.changed = False
            record = self.data.copy()
            record['zone'] = self.location[0]
            record['area'] = self.location[1]
            record['thing'] = self.location[2]
            record['timestamp'] = polars.datetime_now()
            self.records.append(record)
            return record
        return None

    def batch_write_records(self, base_path):
        if not self.records:
            return

        df = polars.DataFrame(self.records, schema=self.schema)
        
        table_path = os.path.join(base_path, f"{self.location[0]}_{self.location[1]}_{self.location[2]}")
        
        deltalake.write_deltatable(
            table_path, 
            df, 
            mode='append'
        )
        
        self.records.clear()


class MonitoringPlug(MonitoredDevice):
    keys = {
        ("sensor", "power"): [float],
        ("sensor", "current"): [float],
        ("sensor", "voltage"): [float],
        ("sensor", "apparent_power"): [float],
        ("sensor", "power_factor"): [float],
        ("sensor", "reactive_power"): [float],
        ("sensor", "energy"): [float],
        ("switch", "switch"): [lambda x: True if x == "ON" else False],
    }


class DeviceRegister:
    type_map = {}
    devices = {}

    def add_device_type(self, kind, klass):
        self.type_map[kind] = klass

    def get_or_create(self, kind, key):
        kind_with_key = (kind, key)

        if kind_with_key in self.devices:
            return self.devices[kind_with_key]
        else:
            if kind in self.type_map:
                device = self.type_map[kind](key)
                self.devices[kind_with_key] = device
                return self.devices[kind_with_key]

        return None

    def append_data(self, kind, key, data_key, value):
        device = self.get_or_create(kind, key)
        if device:
            device.set(data_key, value)


register = DeviceRegister()
register.add_device_type("plug", MonitoringPlug)


def periodic_batch_writer(register, base_path, interval):
    while True:
        time.sleep(interval)
        for device in register.devices.values():
            device.batch_write_records(base_path)


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")

    # format: devices/{zone}/{area}/{type}/{thing}/...
    if msg.topic.startswith("devices/"):
        try:
            _, zone, area, kind, thing, *rest = msg.topic.split("/")
        except ValueError as e:
            print(e)
            print("exception: " + msg.topic)
            return

        key = (zone, area, thing)
        device = register.get_or_create(
            kind,
            key
        )

        if len(rest) > 1 and device:
            device.set((rest[0], rest[1]), payload)

            change = device.get_change_record()

            if change:
                print(key, { k[1]: v for (k, v) in change.items()})

    else:
        print(msg.topic)


def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)

    return on_connect

def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to DeltaLake.")
    parser.add_argument("mqtt_host", help="The MQTT host address.")
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.")
    parser.add_argument("-d", "--delta-path", dest="delta_path", default="/tmp/deltalake", help="Base path for DeltaLake tables")
    parser.add_argument("-i", "--interval", type=int, default=60, help="Batch write interval in seconds")

    args = parser.parse_args()
    print(args)

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

    mqttc.connect(args.mqtt_host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
