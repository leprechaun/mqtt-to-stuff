from collections import defaultdict
import datetime

class ChangeFilter:
    def __init__(self, cast = lambda x: float(x)):
        self.cast = cast
        self._previous_value = None

    def set(self, value):
        cast = self.cast(value)

        if self._previous_value != cast:
            print("set: return cast value - values different", value, cast, self._previous_value)
            self._previous_value = cast
            return cast

        print("set: return none - values equal", value, cast, self._previous_value)
        self._previous_value = cast
        return None


class MonitoredDevice:
    sensors = {}

    def __init__(self, device_key):
        self.device_key = device_key
        self.series = defaultdict(dict)

    def set(self, key, value):
        if key in self.sensors:
            sensor_config = self.sensors[key]
            cf = sensor_config[0]

            type_cast_value = cf.cast(value)

            series_name = sensor_config[1][0]
            column_name = sensor_config[1][1]

            current_value = self.series[series_name].get(column_name)

            if current_value != type_cast_value:
                self.series[series_name][column_name] = type_cast_value

                record = self.series[series_name]
                series_columns = [x[1][1] for x in self.sensors.values() if x[1][0] == series_name]

                record_keys = set(list(record.keys()))
                column_keys = set(series_columns)


                if len(record) == len(series_columns):
                    return (series_name, list(record.items()))
                else:
                    print(self.device_key, series_name, "specified", series_name + "." + column_name, "=", value, "missing:", column_keys-record_keys)

        return None


class MonitoringPlug(MonitoredDevice):
    sensors = {
        ("sensor", "power", "state"): (
            ChangeFilter(),
            ("electricity", "power")
        ),
        ("sensor", "current", "state"): (
            ChangeFilter(),
            ("electricity", "current")
        ),
        ("sensor", "voltage", "state"): (
            ChangeFilter(),
            ("electricity", "voltage")
        ),
        ("sensor", "apparent_power", "state"): (
            ChangeFilter(),
            ("electricity", "apparent_power")
        ),
        ("sensor", "power_factor", "state"): (
            ChangeFilter(),
            ("electricity", "power_factor")
        ),
        ("sensor", "reactive_power", "state"): (
            ChangeFilter(),
            ("electricity", "reactive_power")
        ),
        ("sensor", "energy", "state"): (
            ChangeFilter(),
            ("electricity", "energy")
        ),
        ("switch", "switch", "state"): (
            ChangeFilter(lambda x: True if x == "ON" else False),
            ("electricity", "switch")
        ),

        ("sensor", "uptime_sensor", "state"): (
            ChangeFilter(int),
            ("iot_device_uptime", "uptime")
        ),
    }

class PresenceDetector(MonitoredDevice):
    sensors = {
        ("binary_sensor", "occupancy", "state"): (
            ChangeFilter(lambda x: True if x == "ON" else False),
            ("presence", "occupancy")
        ),

        ("sensor", "light_sensor", "state"): (
            ChangeFilter(),
            ("habitat", "light_level")
        ),

        ("sensor", "uptime_sensor", "state"): (
            ChangeFilter(),
            ("iot_device_uptime", "uptime")
        ),
    }

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
            'TRADFRI bulb E27 WW globe 806lm',
            'TRADFRI bulb GU10 WW 345lm',
            'CK-BL702-AL-01(7009_Z102LG03-1)',
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

class RainSensor:
    timeseries_name = 'rain'

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }


    def match_device(self, device_definition):
        model_ids = [
            'TS0207'
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        return payload

class VINDSTYRKA:
    timeseries_name: "air-quality"

    def friendly_name_to_id(self, friendly_name):
        split_friendly_name = friendly_name.split("/")

        return {
            "zone": "home",
            "area": split_friendly_name[0],
            "thing": split_friendly_name[2]
        }

    def match_device(self, device_definition):
        model_ids = [
            'VINDSTYRKA'
        ]

        return device_definition.get('model_id') in model_ids

    def cast_payload(self, payload):
        return {
            "humidity": payload["humidity"],
            "temperature": payload["temperature"],
            "pm25": payload["linkquality"],
            "voc_index": payload["voc_index"],
            "linkquality": payload["linkquality"],
        }
