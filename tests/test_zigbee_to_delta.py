import datetime
import glob
import json
import os
from types import SimpleNamespace

import polars as pl
import pytest

from mqtt_to_stuff.devices import ThermometerAndHygrometer, TradfriBulbHandler
from mqtt_to_stuff.logging_setup import configure_logging
from mqtt_to_stuff.zigbee_to_delta import ZigbeeDeviceRegister, on_message

DEVICE_DEFINITION = {
    "type": "EndDevice",
    "ieee_address": "0x0000000000000001",
    "friendly_name": "kitchen/sensor/therm-1",
    "manufacturer": "Tuya",
    "model_id": "TS0201",
}


class FakeDeltaLakeClient:
    def __init__(self, existing=None, raise_on_get=False, base_path="s3://bucket/prefix/"):
        self.append_calls = []
        self.existing = existing
        self.raise_on_get = raise_on_get
        self.base_path = base_path

    def get(self, path):
        if self.raise_on_get:
            raise FileNotFoundError("table not found")
        return self.existing

    def append(self, df, path, write_options=None):
        self.append_calls.append((df, path, write_options))


def make_register(dlc=None):
    reg = ZigbeeDeviceRegister()
    reg.add_handler(ThermometerAndHygrometer())
    reg.add_handler(TradfriBulbHandler())
    if dlc is not None:
        reg.set_deltalakeclient(dlc)
    return reg


def test_try_registering_device_known_model():
    reg = make_register()
    assert reg.try_registering_device(DEVICE_DEFINITION) is True
    assert reg.device_mappings["kitchen/sensor/therm-1"]["model"] == "TS0201"


def test_try_registering_device_unknown_model():
    reg = make_register()
    unknown = {**DEVICE_DEFINITION, "model_id": "nope"}
    assert reg.try_registering_device(unknown) is False
    assert reg.device_mappings == {}


def test_register_devices_persists_all_current_devices_on_first_run():
    # First run: no "zigbee-devices" table exists yet, so dlc.get() raises
    # FileNotFoundError. This used to crash (no handling, and even when
    # handled it fell through to previous.sort() on None) - now it should
    # just write everything currently known.
    dlc = FakeDeltaLakeClient(raise_on_get=True)
    reg = make_register(dlc)

    reg.register_devices([DEVICE_DEFINITION])

    assert len(dlc.append_calls) == 1
    df, path, _ = dlc.append_calls[0]
    assert path == "zigbee-devices"
    assert df["friendly_name"].to_list() == ["kitchen/sensor/therm-1"]


def test_register_devices_persists_new_mapping_when_not_already_present():
    existing = pl.DataFrame({
        "timestamp": [datetime.datetime(2026, 1, 1)],
        "address": ["0xdead"],
        "manufacturer": ["Other"],
        "model": ["Other"],
        "friendly_name": ["other/device"],
    })
    dlc = FakeDeltaLakeClient(existing=existing)
    reg = make_register(dlc)

    reg.register_devices([DEVICE_DEFINITION])

    assert len(dlc.append_calls) == 1
    df, path, _ = dlc.append_calls[0]
    assert path == "zigbee-devices"
    assert df["friendly_name"].to_list() == ["kitchen/sensor/therm-1"]


def test_register_devices_writes_nothing_when_already_persisted():
    existing = pl.DataFrame({
        "timestamp": [datetime.datetime(2026, 1, 1)],
        "address": [DEVICE_DEFINITION["ieee_address"]],
        "manufacturer": [DEVICE_DEFINITION["manufacturer"]],
        "model": [DEVICE_DEFINITION["model_id"]],
        "friendly_name": [DEVICE_DEFINITION["friendly_name"]],
    })
    dlc = FakeDeltaLakeClient(existing=existing)
    reg = make_register(dlc)

    reg.register_devices([DEVICE_DEFINITION])

    assert dlc.append_calls == []


def test_append_appends_to_timeseries_for_registered_device():
    reg = make_register()
    reg.try_registering_device(DEVICE_DEFINITION)

    reg.append("kitchen/sensor/therm-1", {
        "temperature": 21.5, "humidity": 50, "battery": 90, "voltage": 3000, "linkquality": 80,
    })

    assert len(reg.timeseries["temperature-and-humidity"]) == 1
    record = reg.timeseries["temperature-and-humidity"][0]
    assert record["temperature"] == 21.5
    assert record["address"] == DEVICE_DEFINITION["ieee_address"]


def test_append_ignores_unregistered_friendly_name():
    reg = make_register()
    reg.append("unknown/device", {"temperature": 1})
    assert reg.timeseries == {}


def test_append_raises_for_unguarded_handler_with_short_friendly_name():
    # TradfriBulbHandler doesn't guard friendly_name_to_id against a short
    # name the way ContactSensor/ThermometerAndHygrometer do - documents the
    # known bug at the ZigbeeDeviceRegister integration point.
    reg = make_register()
    bulb_definition = {
        "type": "EndDevice",
        "ieee_address": "0x2",
        "friendly_name": "just-one-part",
        "manufacturer": "Ikea",
        "model_id": "TRADFRI bulb E27 WW globe 806lm",
    }
    reg.try_registering_device(bulb_definition)

    with pytest.raises(IndexError):
        reg.append("just-one-part", {"state": "ON", "brightness": 100, "linkquality": 90})


def test_write_timeseries_falls_back_to_csv_on_schema_mismatch():
    class RaisingDlc:
        def append(self, df, path, write_options=None):
            raise ValueError("schema mismatch")

    reg = make_register(RaisingDlc())
    reg.timeseries["temperature-and-humidity"] = [{"timestamp": datetime.datetime(2026, 1, 1), "temperature": 1}]

    reg._write_timeseries("temperature-and-humidity", reg.timeseries["temperature-and-humidity"])

    csv_files = glob.glob("/tmp/temperature-and-humidity-*.csv")
    try:
        assert csv_files
        assert reg.timeseries["temperature-and-humidity"] == []
    finally:
        for f in csv_files:
            os.remove(f)


def test_write_timeseries_logs_absolute_path_on_success(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging()

    dlc = FakeDeltaLakeClient(base_path="s3://bucket/prefix/")
    reg = make_register(dlc)
    reg.timeseries["temperature-and-humidity"] = [{"timestamp": datetime.datetime(2026, 1, 1), "temperature": 1}]

    reg._write_timeseries("temperature-and-humidity", reg.timeseries["temperature-and-humidity"])

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["path"] == "s3://bucket/prefix/temperature-and-humidity"


class FakeZDR:
    def __init__(self):
        self.registered = []
        self.appended = []

    def register_devices(self, device_definitions):
        self.registered.append(device_definitions)

    def append(self, friendly_name, payload):
        self.appended.append((friendly_name, payload))


def test_on_message_registers_devices_from_bridge_topic():
    zdr = FakeZDR()
    msg = SimpleNamespace(topic="zigbee2mqtt/bridge/devices", payload=json.dumps([DEVICE_DEFINITION]).encode())

    on_message(None, zdr, msg)

    assert zdr.registered == [[DEVICE_DEFINITION]]


def test_on_message_ignores_other_bridge_topics():
    zdr = FakeZDR()
    msg = SimpleNamespace(topic="zigbee2mqtt/bridge/state", payload=b"online")

    on_message(None, zdr, msg)

    assert zdr.registered == []
    assert zdr.appended == []


def test_on_message_ignores_set_topics():
    zdr = FakeZDR()
    msg = SimpleNamespace(topic="zigbee2mqtt/kitchen/sensor/set", payload=b"{}")

    on_message(None, zdr, msg)

    assert zdr.appended == []


def test_on_message_appends_device_payload():
    zdr = FakeZDR()
    msg = SimpleNamespace(topic="zigbee2mqtt/kitchen/sensor/therm-1", payload=json.dumps({"temperature": 21}).encode())

    on_message(None, zdr, msg)

    assert zdr.appended == [("kitchen/sensor/therm-1", {"temperature": 21})]


def test_on_message_swallows_malformed_json():
    zdr = FakeZDR()
    msg = SimpleNamespace(topic="zigbee2mqtt/kitchen/sensor/therm-1", payload=b"not-json")

    on_message(None, zdr, msg)

    assert zdr.appended == []
