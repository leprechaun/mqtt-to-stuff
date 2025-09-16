import argparse
import sys
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server
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
                df.write_delta(
                    base_path + "/" + name,
                    mode="append",
                    delta_write_options=write_options,
                    storage_options=options
                )
                self.logger.info("wrote %s records to %s/%s" % (len(df), base_path, name))
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


            id = handler.friendly_name_to_id(friendly_name)
            id['timestamp'] = datetime.datetime.now()

            id.update(cast_payload)

            #cast_payload['zone'] = 'home'
            #cast_payload['area'] = id['area']
            #cast_payload['thing'] = id['thing']


            self.timeseries[handler.timeseries_name].append(id)
            self.logger.info(["appending", handler.timeseries_name, id])
        else:
            self.logger.info(["unsuported", friendly_name, payload])


class ThermometerAndHygrometer:
    timeseries_name = 'temperature-and-humidity'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split(" ")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[1]
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
        split_friendly_name = friendly_name.split(" ")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[1]
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


ZDR = ZigbeeDeviceRegister()

bulbs = TradfriBulbHandler()
ZDR.add_handler(bulbs)

thermometers = ThermometerAndHygrometer()
ZDR.add_handler(thermometers)

def on_message(client, userdata, msg):
    if msg.topic.startswith('zigbee2mqtt/bridge/devices'):
        o = json.loads(msg.payload.decode('utf-8'))
        ZDR.register_devices(o)

    elif msg.topic.startswith('zigbee2mqtt/'):
        split = msg.topic.split("/")
        maybe_friendly_name = split[1]
        o = json.loads(msg.payload.decode('utf-8'))

        if len(split) == 2:
            ZDR.append(maybe_friendly_name, o)


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
    start_http_server(9101)
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-t", "--topic", dest="topics", action="append", help="The MQTT topic to subscribe to.")
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



    #mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(args.topics)
    mqttc.on_message = on_message

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
