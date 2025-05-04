import argparse
import sys
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Summary, Gauge
import deltalake
import polars

from typing import Dict, Tuple
from collections.abc import Callable


labels = ['zone', 'area', 'thing']

#: Dict[(str, str), Tuple( Gauge | Summary, Callable)]
esphome_metrics = {
    ("sensor", "power"): [Summary('electricity_power', 'Power consumption in watts', labels), float],
    ("sensor", "current"): [Summary('electricity_current', 'Power draw in amps', labels), float],
    ("sensor", "voltage"): [Summary('electricity_voltage', 'Voltage', labels), float],
    ("sensor", "apparent_power"): [Summary('electricity_apparent_power', 'Apparent power', labels), float],
    ("sensor", "power_factor"): [Summary('electricity_power_factor', 'Power factor', labels), float],
    ("sensor", "reactive_power"): [Summary('electricity_reactive_power', 'Reactive power', labels), float],
    ("sensor", "total_energy"): [Summary('electricity_total_energy', 'Cumulative energy consumption in KWh', labels), float],
    ("sensor", "energy"): [Summary('electricity_energy', 'Energy consumption in KWh since device rebooted', labels), float],
    ("switch", "switch"): [Gauge('electricity_relay_status', 'State of the power relay', labels), lambda x: 1 if x == "ON" else 0],


    ("sensor", "light_sensor"): [Gauge("light_level", "Light level in Lux", labels), float],
    ("binary_sensor", "occupancy"): [Gauge("presence_occupancy", "Status of the occupancy sensor", labels), lambda x: 1 if x == "ON" else 0],

    ("sensor", "uptime_sensor"): [Gauge("uptime", "uptime in seconds", labels), int],

    ("sensor", "total_daily_energy"): False,
    ("sensor", "total_daily_energy_two"): False,
    ("sensor", "wifi_signal_percent"): False,
    ("sensor", "wifi_signal_db"): False
}

def handle_esphome(zone, area, kind, thing, rest, payload):
    if rest[0] == 'status':
        return

    sensor_kind = rest[0]
    name = rest[1]

    key = (sensor_kind, name)

    if key in esphome_metrics:
        if esphome_metrics[key]:
            metric = esphome_metrics[key][0]

            if len(esphome_metrics[key]) == 2:
                filter_fn = esphome_metrics[key][1]
            else:
                filter_fn = lambda x: x

            if type(metric) == Summary:
                metric.labels(zone, area, thing).observe(filter_fn(payload))
                return

            elif type(metric) == Gauge:
                metric.labels(zone, area, thing).set(filter_fn(payload))
                return
        else:
            return


    print("ESPHOME", zone, area, kind, thing, rest, payload)


def handle_lgtv(zone, area, kind, thing, rest, payload):
    print("LGTV", zone, area, kind, kind, thing, rest, payload)


def unsupported(zone, area, kind, thing, rest, payload):
    print("unsupported: " + kind)


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

        match kind:
            case "plug":
                handle_esphome(zone, area, kind, thing, rest, payload)

            case "presence":
                handle_esphome(zone, area, kind, thing, rest, payload)

            case "lgtv":
                handle_lgtv(zone, area, kind, thing, rest, payload)

            case _:
                unsupported(zone, area, kind, thing, rest, payload)

    else:
        print(msg.topic)


def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)

    return on_connect

def main(args):
    start_http_server(9101)
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
