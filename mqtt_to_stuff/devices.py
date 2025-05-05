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


class Series:
    def __init__(self, name):
        self.name = name
        self.records = []

    def append(self, timestamp, source, record):
        thing = (
            ('timestamp', timestamp),
            source,
            record
        )
        print("appended:", self.name, thing[1])
        self.records.append(thing)

    def to_list(self):
        return [ dict(**{"timestamp": r[0][1]}, **dict(r[1]), **dict(r[2])) for r in self.records ]

    def clear(self):
        self.records.clear()


class DeviceRegister:
    type_map = {}
    devices = {}
    series = {}

    def add_device_type(self, kind, klass):
        self.type_map[kind] = klass

    def add_series(self, series):
        self.series[series.name] = series

    def append_data(self, kind, key, rest, value):
        if device := self.get_or_create(kind, key):
            if series_and_record := device.set(rest, value):
                series_name = series_and_record[0]
                record = series_and_record[1]

                if series := self.series.get(series_name):
                    series.append(datetime.datetime.now(), key, record)
                    return
                else:
                    raise Exception("Undefined series: %s" % series_name)
            else:
                print("series_and_record falsy", series_and_record)
        else:
            print("device falsy")

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

    def get_records_by_type(self):
        records = self.records_by_type
        self.records_by_type = defaultdict(list)
        return records


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
                #print(self.device_key, record)

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

