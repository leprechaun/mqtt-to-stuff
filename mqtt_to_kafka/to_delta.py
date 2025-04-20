import argparse
import sys
import paho.mqtt.client as mqtt
import deltalake
import polars

from typing import Dict, Tuple
from collections.abc import Callable


# ai! explain this file to me


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


    def get_change_record(self):
        if self.changed == True:
            self.changed = False
            return self.data
        else:
            return None

        as_array = [self.data.get(k) for k in self.keys]

        if sum(1 for x in as_array if x is None) == 0:
            self.records.append(as_array)


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


register = DeviceRegister()
register.add_device_type("plug", MonitoringPlug)

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
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("mqtt_host", help="The MQTT host address.")
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.")

    args = parser.parse_args()
    print(args)

    #mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(args.topics)
    mqttc.on_message = on_message

    mqttc.connect(args.mqtt_host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
