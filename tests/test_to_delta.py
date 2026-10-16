import datetime
from types import SimpleNamespace

from mqtt_to_stuff import to_delta
from mqtt_to_stuff.register import DeviceRegister, Series


class FakeDeltaLakeClient:
    def __init__(self):
        self.append_calls = []

    def append(self, df, path, write_options=None):
        self.append_calls.append((df, path, write_options))


def test_write_appends_non_empty_series():
    register = DeviceRegister()
    series = Series("electricity")
    register.add_series(series)
    series.append(datetime.datetime(2026, 1, 1), (("thing", "plug1"),), {"power": 1.0})

    dlc = FakeDeltaLakeClient()
    to_delta.write(register, dlc)

    assert len(dlc.append_calls) == 1
    df, path, write_options = dlc.append_calls[0]
    assert path == "electricity"
    assert write_options == {"partition_by": ["date"]}
    assert "date" in df.columns
    assert series.records == []


def test_write_skips_empty_series():
    register = DeviceRegister()
    series = Series("electricity")
    register.add_series(series)

    dlc = FakeDeltaLakeClient()
    to_delta.write(register, dlc)

    assert dlc.append_calls == []


def test_on_message_dispatches_well_formed_topic():
    calls = []

    class FakeRegister:
        def append_data(self, kind, key, rest, value):
            calls.append((kind, key, rest, value))

    msg = SimpleNamespace(topic="devices/home/kitchen/plug/plug1/sensor/power/state", payload=b"1.23")
    to_delta.on_message(None, FakeRegister(), msg)

    assert calls == [
        ("plug", (("zone", "home"), ("area", "kitchen"), ("thing", "plug1")), ("sensor", "power", "state"), "1.23")
    ]


def test_on_message_logs_and_swallows_malformed_topic():
    calls = []

    class FakeRegister:
        def append_data(self, kind, key, rest, value):
            calls.append((kind, key, rest, value))

    # Too few segments to unpack - mirrors the zigbee2mqtt topic that leaked
    # into this collector in production before the subscription bug was fixed.
    msg = SimpleNamespace(topic="zigbee2mqtt/bedroom/air-quality/nightstand", payload=b"{}")
    to_delta.on_message(None, FakeRegister(), msg)

    assert calls == []
