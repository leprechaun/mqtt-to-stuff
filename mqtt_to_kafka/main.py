import argparse
import sys
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Summary, Gauge
import re

labels = ['zone', 'area', 'thing']

summaries = {
    "power": Summary('power_watts', 'Power consumption in watts', labels),
    "current": Summary('power_current', 'Power draw in amps', labels),
    "voltage": Summary('power_voltage', 'Power supply voltage', labels),
    "apparent_power": Summary('power_apparent', 'Apparent power', labels),
    "energy": Summary('power_energy', 'Energy consumption', labels)
}

gauges = {
    "occupancy": Gauge('occupancy', 'Occupancy status'),
    "light_sensor": Gauge('light_level', 'Light level')
}


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

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(args.topics)
    mqttc.on_message = on_message

    mqttc.connect(args.mqtt_host, 1883, 60)

    mqttc.loop_forever()

    return 0


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    if topic.startswith("devices/home"):
        parts = topic.split("/")
        zone = parts[2]
        area = parts[3]
        thing = parts[4]

        if thing == "plug" and parts[5] == "sensor":
            sensor_type = parts[6]
            try:
                value = float(payload)
                if sensor_type in summaries:
                    summaries[sensor_type].labels(zone=zone, area=area, thing=thing).observe(value)
            except ValueError:
                print(f"Invalid value for {sensor_type}: {payload}")

        elif thing == "plug" and parts[5] == "binary_sensor":
            if payload.lower() == "on":
                value = 1
            else:
                value = 0
            # Assuming binary sensors represent on/off state
            # You might need to adjust this based on your specific sensors
            # summaries["plug_state"].labels(zone=zone, area=area, thing=thing).observe(value)

        elif thing == "presence" and parts[5] == "binary_sensor":
            if payload.lower() == "on":
                value = 1
            else:
                value = 0
            gauges["occupancy"].labels(zone=zone, area=area).set(value)

        elif thing == "presence" and parts[5] == "sensor":
            try:
                value = float(payload)
                gauges["light_sensor"].labels(zone=zone, area=area).set(value)
            except ValueError:
                print(f"Invalid light sensor value: {payload}")

    else:
        print(f"Unknown topic: {topic} with payload: {payload}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
