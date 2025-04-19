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
    "power_factor": Summary('power_factor', 'Power factor', labels),
    "reactive_power": Summary('power_reactive', 'Reactive power', labels),
    "energy": Summary('power_energy', 'Energy', labels),
}

gauges = {
    "uptime_sensor": Gauge("uptime", "uptime in seconds", labels),
    "relay": Gauge("power_relay_status", "Status of the plug relay", labels),
    "occupancy": Gauge("presence_occupancy", "Status of the occupancy sensor", labels),
    "light_sensor": Gauge("light_level", "Light level in Lux", labels)
}


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    # office/plug/computers/sensor/power/state
    #print(msg.topic, payload)
    # office/plug/test/switch/switch/state

    printed = False

    if groups := re.match(r"devices/home/([\w\-]+)/plug/(\w+)/switch/switch/state", msg.topic):
        groups = groups.groups()
        if payload.lower() == "on":
            gauges['relay'].labels("home", groups[0], groups[1]).set(1)
            printed = True
        else:
            gauges['relay'].labels("home", groups[0], groups[1]).set(0)
            printed = True

    if groups := re.match(r"devices/home/([\w\-]+)/plug/(\w+)/sensor/(\w+)/state", msg.topic):
        groups = groups.groups()

        if groups[2] in summaries.keys():
            summaries[groups[2]].labels("home", groups[0], groups[1]).observe(float(payload))
            printed = True

        elif groups[2] in gauges:
            gauges[groups[2]].labels("home", groups[0], groups[1]).set(float(payload))
            printed = True

    # devices/home/bedroom/presence/bed/binary_sensor/occupancy/state
    if groups := re.match(r"devices/home/([\w\-]+)/presence/(\w+)/binary_sensor/occupancy/state", msg.topic):
        groups = groups.groups()
        if payload.lower() == "on":
            gauges['occupancy'].labels("home", groups[0], groups[1]).set(1)
            printed = True
        else:
            gauges['occupancy'].labels("home", groups[0], groups[1]).set(0)
            printed = True

    if groups := re.match(r"devices/home/([\w\-]+)/presence/(\w+)/sensor/light_sensor/state", msg.topic):
        groups = groups.groups()

        gauges['light_sensor'].labels("home", groups[0], groups[1]).set(float(payload))
        printed = True

    if not printed:
        print(msg.topic + " " + payload)




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
