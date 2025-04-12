import argparse
import sys
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("mqtt_host", help="The MQTT host address.")
    # ai! make the mqtt_topic a flag, -t, that is repeatable
    parser.add_argument("mqtt_topic", help="The MQTT topic to subscribe to.")

    args = parser.parse_args()

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    mqttc.connect(args.mqtt_host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
