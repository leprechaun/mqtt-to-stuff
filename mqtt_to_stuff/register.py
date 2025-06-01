import datetime

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
