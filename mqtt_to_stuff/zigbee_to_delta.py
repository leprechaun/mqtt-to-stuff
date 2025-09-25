import argparse
import sys
import paho.mqtt.client as mqtt
import deltalake
import polars as pl
import datetime
import json
from collections import defaultdict
import logging
import os
import time
import threading


logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

class ZigbeeDeviceRegister:
    def __init__(self):
        self.timeseries = defaultdict(list)
        self.handlers = {}
        self.device_mappings = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def write_all_and_clear(self, base_path):
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


        for name in self.timeseries:
            if len(self.timeseries[name]) > 0:
                df = pl.DataFrame(self.timeseries[name]).with_columns(date=pl.col('timestamp').dt.date())
                try:
                    df.write_delta(
                        base_path + name,
                        mode="append",
                        delta_write_options=write_options,
                        storage_options=options
                    )
                    self.logger.info("wrote %s records to %s/%s" % (len(df), base_path, name))
                    self.timeseries[name].clear()
                except Exception as e:
                    self.logger.warning(e)
                    self.logger.info("Typically a schema mismatch")
                    self.logger.debug(df.schema)

                    print(df)

                    csv_path = "/tmp/" + name + "-" + datetime.datetime.now().isoformat() + ".csv"
                    df.write_csv(csv_path)
                    self.logger.info("wrote %s records to %s" % (len(df), csv_path))
                    self.timeseries[name].clear()

    def register_devices(self, device_definitions):
        for dd in device_definitions:
            result = self.try_registering_device(dd)

            self.logger.info(
                "t: '%s', a: '%s', n: '%s', m: '%s' - supported = %s" % (
                    dd['type'],
                    dd['ieee_address'],
                    dd['friendly_name'],
                    dd.get('model_id'),
                    result
                )
            )

    def try_registering_device(self, device_definition):
        if handler := self._match_device_to_handler(device_definition):
            self.device_mappings[device_definition.get('friendly_name')] = handler
            return True

        else:
            return False


    def _match_device_to_handler(self, device_definition):
        for handler in self.handlers.values():
            if handler.match_device(device_definition):
                return handler

        return None

    def add_handler(self, handler):
        self.handlers[handler.__class__.__name__] = handler

    def append(self, friendly_name, payload):
        if handler := self.device_mappings.get(friendly_name):
            cast_payload = handler.cast_payload(payload)

            if id := handler.friendly_name_to_id(friendly_name):
                id['timestamp'] = datetime.datetime.now()

                id.update(cast_payload)

                self.timeseries[handler.timeseries_name].append(id)
                self.logger.info(["appending", handler.timeseries_name, id])

            else:
                self.logger.info(["not-appending", handler.timeseries_name, "%s didn't match to an id" % friendly_name])
        else:
            self.logger.info(["unsuported", friendly_name, payload])


class ContactSensor:
    timeseries_name = 'contact-sensors'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        if len(split_friendly_name) < 2:
            return None

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }

    def match_device(self, device_definition):
        model_ids = [
            'TS0203'
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        # {'battery': 100, 'battery_low': False, 'contact': True, 'linkquality': 72, 'tamper': False, 'vo
        return payload

class ThermometerAndHygrometer:
    timeseries_name = 'temperature-and-humidity'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        if len(split_friendly_name) < 2:
            return None

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }

    def match_device(self, device_definition):
        model_ids = [
            'TS0201'
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        return {
            "temperature": payload['temperature'],
            "humidity": payload['humidity'],
            "battery": payload['battery'],
            "voltage": payload['voltage'],
            "linkquality": payload['linkquality'],
        }

class TradfriBulbHandler:
    timeseries_name = 'smart-bulbs'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }


    def match_device(self, device_definition):
        model_ids = [
            'TRADFRI bulb E27 WW globe 806lm'
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        return {
            "on": True if payload['state'] == 'ON' else False,
            "brightness": payload['brightness'],
            "linkquality": payload['linkquality']
        }


class ActionButtons:
    timeseries_name = 'buttons'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }


    def match_device(self, device_definition):
        model_ids = [
            'TS004F', # rotary
            'ZG-101ZL' # simple push
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        return payload


class MotionLuminance:
    timeseries_name = 'motion-sensor'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }


    def match_device(self, device_definition):
        model_ids = [
            'ZG-204ZL' # motion & luminance
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        #payload['motion'] = payload.get('occupancy')
        #del payload['occupancy']
        # {'battery': 100, 'illuminance': None, 'illuminance_interval': None, 'keep_time': None, 'linkquality': 84, 'occupancy': None, 'sensitivity': None}
        return payload


ZDR = ZigbeeDeviceRegister()

bulbs = TradfriBulbHandler()
ZDR.add_handler(bulbs)

thermometers = ThermometerAndHygrometer()
ZDR.add_handler(thermometers)

buttons = ActionButtons()
ZDR.add_handler(buttons)

contact_sensors = ContactSensor()
ZDR.add_handler(contact_sensors)

motion_sensors = MotionLuminance()
ZDR.add_handler(motion_sensors)


def on_message(client, userdata, msg):
    if msg.topic.startswith('zigbee2mqtt/bridge/devices'):
        o = json.loads(msg.payload.decode('utf-8'))
        ZDR.register_devices(o)

    elif msg.topic.startswith("zigbee2mqtt/bridge"):
        print("don't care: %s" % msg.topic)
        return

    elif msg.topic.startswith('zigbee2mqtt/'):

        all_split = msg.topic.split("/")
        if all_split[-1] == 'set':
            print(["not an update", all_split])
            return

        split = msg.topic.split("/", 1)
        maybe_friendly_name = split[1]

        try:
            if len(split) == 2:
                o = json.loads(msg.payload.decode('utf-8'))
                ZDR.append(maybe_friendly_name, o)
            else:
                print(["not an update", split])

        except json.decoder.JSONDecodeError as e:
            print(["failed-to-parse", msg.topic, msg.payload.decode("utf-8")])
            print(msg.payload.decode("utf-8"))

    else:
        print("msg", msg.topic, msg.payload.decode("utf-8"))
        pass

def periodic_batch_writer(register, base_path, interval):
    while True:
        time.sleep(interval)
        register.write_all_and_clear(base_path)


def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)

    return on_connect

def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=os.environ.get('INTERVAL', 60))

    args = parser.parse_args()

    # Start periodic batch writer thread
    batch_thread = threading.Thread(
        target=periodic_batch_writer, 
        args=(ZDR, args.delta_path, args.interval), 
        daemon=True
    )
    batch_thread.start()


    topics = ["zigbee2mqtt/#"]

    #mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(topics)
    mqttc.on_message = on_message

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
