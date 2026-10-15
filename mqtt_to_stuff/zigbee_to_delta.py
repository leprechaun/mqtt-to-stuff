import argparse
import datetime
import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict

import deltalake
import paho.mqtt.client as mqtt
import polars as pl

from .delta_client import DeltaLakeClient
from .devices import (
    VINDSTYRKA,
    ActionButtons,
    ContactSensor,
    MotionLuminance,
    ThermometerAndHygrometer,
    TradfriBulbHandler,
)
from .mqtt_helpers import generate_on_connect

logging.basicConfig(encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)

class ZigbeeDeviceRegister:
    def __init__(self):
        self.timeseries = defaultdict(list)
        self.handlers = {}
        self.device_mappings = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_deltalakeclient(self, dlc):
        self.dlc = dlc

    def write_all_and_clear(self, base_path):
        try:
            for name in self.timeseries:
                if len(self.timeseries[name]) > 0:
                    self._write_timeseries(base_path,name, self.timeseries[name])
        except Exception:
            self.logger.exception("write_all_and_clear failed")

    def _write_timeseries(self, base_path, name, timeseries):
        write_options = {
            "writer_properties": deltalake.WriterProperties(compression="zstd"),
            "partition_by":['date']
        }

        options = {}
        if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
            options["endpoint_url"] = S3_ENDPOINT

        df = pl.DataFrame(timeseries).with_columns(date=pl.col('timestamp').dt.date())

        try:
            self.dlc.append(df, name, write_options)
            timeseries.clear()
            self.logger.info(f"wrote {len(df)} records to {base_path}/{name}")

        except Exception as e: #noqa
            self.logger.warning(e)
            self.logger.info("Typically a schema mismatch")
            self.logger.debug(df.schema)
            self.logger.debug("%s", df)

            csv_path = "/tmp/" + name + "-" + datetime.datetime.now().isoformat() + ".csv"
            df.write_csv(csv_path)
            self.logger.info(f"wrote {len(df)} records to {csv_path}")
            self.timeseries[name].clear()


    def register_devices(self, device_definitions):
        for dd in device_definitions:
            result = self.try_registering_device(dd)

            self.logger.info(
                "t: '{}', a: '{}', n: '{}', m: '{}' - supported = {}".format(
                    dd['type'],
                    dd['ieee_address'],
                    dd['friendly_name'],
                    dd.get('model_id'),
                    result
                )
            )

        self._persist_device_mappings()

    def _persist_device_mappings(self):
        for_df = []

        for (friendly_name, mapping) in self.device_mappings.items():
            now = datetime.datetime.now()
            for_df.append({
                "timestamp": now,
                "address": mapping['address'],
                "manufacturer": mapping['manufacturer'],
                "model": mapping['model'],
                "friendly_name": friendly_name,
            })

        current = pl.DataFrame(for_df)

        previous = self.dlc.get("zigbee-devices")

        if previous is None:
            self.dlc.append(current, "zigbee-devices")
            self.logger.info("had nothing - write all of current")
            self.logger.info("%s", current)

        latest = previous.sort(by=['timestamp']).group_by(["address"], maintain_order=True).last()
        anti = current.join(latest, on=['address','friendly_name'], how='anti')


        self.logger.info("latest")
        self.logger.info("%s", latest)

        if len(anti) > 0:
            self.logger.info("anti: should write this")
            self.logger.info("%s", anti)

            self.dlc.append(anti, 'zigbee-devices')
        else:
            self.logger.info("nothing to write")


    def try_registering_device(self, device_definition):
        if handler := self._match_device_to_handler(device_definition):
            #print(device_definition)
            self.device_mappings[device_definition.get('friendly_name')] = {
                "manufacturer": device_definition.get('manufacturer'),
                "model": device_definition.get('model_id'),
                "address": device_definition.get('ieee_address'),
                "handler": handler
            }
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
        if handler_and_address := self.device_mappings.get(friendly_name):
            handler = handler_and_address['handler']
            address = handler_and_address['address']
            cast_payload = handler.cast_payload(payload)

            if id := handler.friendly_name_to_id(friendly_name):
                id['address'] = address
                id['timestamp'] = datetime.datetime.now()

                id.update(cast_payload)

                self.timeseries[handler.timeseries_name].append(id)
                self.logger.debug(["appending", handler.timeseries_name, id])

            else:
                self.logger.info(["not-appending", handler.timeseries_name, f"{friendly_name} didn't match to an id"])
        else:
            self.logger.info(["unsuported", friendly_name, payload])


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

vindstyrka = VINDSTYRKA()
ZDR.add_handler(vindstyrka)


def on_message(client, userdata, msg):
    zdr = userdata

    if msg.topic.startswith('zigbee2mqtt/bridge/devices'):
        o = json.loads(msg.payload.decode('utf-8'))
        zdr.register_devices(o)

    elif msg.topic.startswith("zigbee2mqtt/bridge"):
        return

    elif msg.topic.startswith('zigbee2mqtt/'):

        all_split = msg.topic.split("/")
        if all_split[-1] == 'set':
            return

        split = msg.topic.split("/", 1)
        maybe_friendly_name = split[1]

        try:
            if len(split) == 2:
                o = json.loads(msg.payload.decode('utf-8'))
                zdr.append(maybe_friendly_name, o)

        except json.decoder.JSONDecodeError:
            logger.exception("failed to parse %s: %s", msg.topic, msg.payload.decode("utf-8"))

def periodic_batch_writer(register, base_path, interval):
    while True:
        time.sleep(interval)
        register.write_all_and_clear(base_path)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("--host", help="The MQTT host address.", default=os.environ.get('MQTT_HOST'))
    parser.add_argument("-d", "--delta-path", dest="delta_path", help="Base path for DeltaLake tables", default=os.environ.get('DELTA_PATH', '/tmp/deltalake/'))
    parser.add_argument("-i", "--interval", type=int, help="Batch write interval in seconds", default=int(os.environ.get('INTERVAL', "60")))

    return parser.parse_args(argv)


def main(args):
    args = parse_args(args)

    # Start periodic batch writer thread
    batch_thread = threading.Thread(
        target=periodic_batch_writer, 
        args=(ZDR, args.delta_path, args.interval), 
        daemon=True
    )
    batch_thread.start()

    options = {}
    if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options["endpoint_url"] = S3_ENDPOINT

    options["AWS_SESSION_TOKEN"] = os.environ.get('AWS_SESSION_TOKEN', "")

    logger.info("%s", options)

    dlc = DeltaLakeClient(args.delta_path, options)

    ZDR.set_deltalakeclient(dlc)



    topics = ["zigbee2mqtt/#"]

    #mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = generate_on_connect(topics)
    mqttc.on_message = on_message
    mqttc.user_data_set(ZDR)

    mqttc.connect(args.host, 1883, 60)

    mqttc.loop_forever()

    return 0

def cli():
    sys.exit(main(sys.argv[1:]))

if __name__ == "__main__":
    cli()
