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
import code

from devices import ActionButtons, ContactSensor, ThermometerAndHygrometer, TradfriBulbHandler, MotionLuminance, VINDSTYRKA

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

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
        except Exception as e:
            print(e)

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
            self.logger.info("wrote %s records to %s/%s" % (len(df), base_path, name))

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

        try:
            previous = self.dlc.get("zigbee-devices")
        except:
            previous = None

        if previous is None:
            self.dlc.append(current, "zigbee-devices")
            print("had nothing - write all of current")
            print(current)

        latest = previous.sort(by=['timestamp']).group_by(["address"], maintain_order=True).last()
        anti = current.join(latest, on=['address','friendly_name'], how='anti')


        print("latest")
        print(latest)

        if len(anti) > 0:
            print("anti: should write this")
            print(anti)

            self.dlc.append(anti, 'zigbee-devices')
        else:
            print("nothing to write")


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
                self.logger.info(["appending", handler.timeseries_name, id])

            else:
                self.logger.info(["not-appending", handler.timeseries_name, "%s didn't match to an id" % friendly_name])
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


class DeltaLakeClient:
    def __init__(self, base_path, storage_options):
        self._base_path = base_path
        self._storage_options = storage_options
        self.logger = logging.getLogger(self.__class__.__name__)

    def get(self, path):
        p = self._base_path + path
        print("going to read", p)
        return pl.read_delta(
            p,
            storage_options = self._storage_options
        )

    # self.dlc.write(df, name, write_options)
    def append(self, df, path, write_options = None):
        if write_options is None:
            write_options = {}

        write_options.update({
            "writer_properties": deltalake.WriterProperties(compression="zstd"),
        })

        df.write_delta(
            self._base_path + path,
            delta_write_options = write_options,
            storage_options = self._storage_options,
            mode = "append"
        )

        dt = deltalake.DeltaTable(
            self._base_path + path,
            storage_options = self._storage_options
        )

        if len(dt.file_uris()) >= 60:
            dt.optimize.compact()
            dt.create_checkpoint()
            self.logger.info("Compacting")


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

    options = {}
    if S3_ENDPOINT := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options["endpoint_url"] = S3_ENDPOINT

    options["AWS_SESSION_TOKEN"] = os.environ.get('AWS_SESSION_TOKEN', "")

    print(options)

    dlc = DeltaLakeClient(args.delta_path, options)

    ZDR.set_deltalakeclient(dlc)



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
