import pytest

from mqtt_to_stuff.devices import (
    VINDSTYRKA,
    ActionButtons,
    ChangeFilter,
    ContactSensor,
    MonitoringPlug,
    MotionLuminance,
    MultiPresenceDetector,
    RainSensor,
    ThermometerAndHygrometer,
    TradfriBulbHandler,
)

PLUG_ELECTRICITY_KEYS = [
    ("sensor", "power", "state"),
    ("sensor", "current", "state"),
    ("sensor", "voltage", "state"),
    ("sensor", "apparent_power", "state"),
    ("sensor", "power_factor", "state"),
    ("sensor", "reactive_power", "state"),
    ("sensor", "energy", "state"),
    ("switch", "switch", "state"),
]


class TestChangeFilter:
    # Note: MonitoredDevice.set() calls cf.cast(value) directly, never cf.set() -
    # ChangeFilter.set()'s own dedup/_previous_value tracking is currently dead
    # code from the production call path. Tested here as a unit in its own right.

    def test_first_value_always_returned(self):
        cf = ChangeFilter()
        assert cf.set("1.0") == 1.0

    def test_same_cast_value_returns_none(self):
        cf = ChangeFilter()
        cf.set("1.0")
        assert cf.set("1.0") is None

    def test_different_raw_same_cast_value_returns_none(self):
        cf = ChangeFilter()
        cf.set("1.50")
        assert cf.set("1.5") is None

    def test_changed_value_returns_cast_value(self):
        cf = ChangeFilter()
        cf.set("1.0")
        assert cf.set("2.0") == 2.0

    def test_custom_cast(self):
        cf = ChangeFilter(lambda x: x == "ON")
        assert cf.set("ON") is True
        assert cf.set("ON") is None
        assert cf.set("OFF") is False


class TestMonitoredDeviceSet:
    def test_unknown_key_ignored(self):
        device = MonitoringPlug(("zone", "area", "thing"))
        assert device.set(("sensor", "does-not-exist", "state"), "1") is None

    def test_returns_none_until_series_complete(self):
        device = MonitoringPlug(("zone", "area", "thing"))
        assert device.set(("sensor", "power", "state"), "1") is None

    def test_returns_record_once_series_complete(self):
        device = MonitoringPlug(("zone", "area", "thing"))

        result = None
        for key in PLUG_ELECTRICITY_KEYS:
            result = device.set(key, "1")

        assert result is not None
        series_name, record = result
        assert series_name == "electricity"
        assert dict(record)["power"] == 1.0

    def test_fires_again_on_changed_value_after_series_complete(self):
        device = MonitoringPlug(("zone", "area", "thing"))
        for key in PLUG_ELECTRICITY_KEYS:
            device.set(key, "1")

        result = device.set(("sensor", "power", "state"), "2")

        assert result is not None
        _series_name, record = result
        assert dict(record)["power"] == 2.0

    def test_resetting_same_value_after_series_complete_returns_none(self):
        # MonitoredDevice.set() dedupes on its own (comparing against the last
        # stored column value), independently of ChangeFilter's own dedup.
        device = MonitoringPlug(("zone", "area", "thing"))
        for key in PLUG_ELECTRICITY_KEYS:
            device.set(key, "1")

        assert device.set(("sensor", "power", "state"), "1") is None


class TestMultiPresenceDetector:
    def test_has_target_sensors_for_each_index(self):
        device = MultiPresenceDetector(("zone", "area", "thing"))
        for target_index in (1, 2, 3):
            for prop in ("x", "y", "distance", "angle", "speed"):
                assert ("sensor", f"target-{target_index}_{prop}", "state") in device.sensors


@pytest.mark.parametrize(
    "handler_cls,model_id",
    [
        (ContactSensor, "TS0203"),
        (ThermometerAndHygrometer, "TS0201"),
        (TradfriBulbHandler, "TRADFRI bulb E27 WW globe 806lm"),
        (ActionButtons, "TS004F"),
        (MotionLuminance, "ZG-204ZL"),
        (RainSensor, "TS0207"),
        (VINDSTYRKA, "VINDSTYRKA"),
    ],
)
def test_match_device_known_model(handler_cls, model_id):
    handler = handler_cls()
    assert handler.match_device({"model_id": model_id}) is True


@pytest.mark.parametrize(
    "handler_cls",
    [ContactSensor, ThermometerAndHygrometer, TradfriBulbHandler, ActionButtons, MotionLuminance, RainSensor, VINDSTYRKA],
)
def test_match_device_unknown_model(handler_cls):
    handler = handler_cls()
    assert handler.match_device({"model_id": "unknown-model"}) is False
    assert handler.match_device({}) is False


def test_thermometer_cast_payload():
    handler = ThermometerAndHygrometer()
    payload = {"temperature": 21.5, "humidity": 55, "battery": 90, "voltage": 3000, "linkquality": 80}
    assert handler.cast_payload(payload) == payload


def test_tradfri_cast_payload():
    handler = TradfriBulbHandler()
    payload = {"state": "ON", "brightness": 200, "linkquality": 90}
    assert handler.cast_payload(payload) == {"on": True, "brightness": 200, "linkquality": 90}


def test_vindstyrka_cast_payload_pm25_bug():
    # Known bug: cast_payload sets "pm25" from payload["linkquality"] instead
    # of payload["pm25"]. This test documents current (wrong) behavior.
    handler = VINDSTYRKA()
    payload = {"humidity": 40, "temperature": 20, "pm25": 5, "voc_index": 100, "linkquality": 70}
    result = handler.cast_payload(payload)
    assert result["pm25"] == payload["linkquality"]


@pytest.mark.parametrize("handler_cls", [ContactSensor, ThermometerAndHygrometer])
def test_friendly_name_to_id_short_name_returns_none(handler_cls):
    handler = handler_cls()
    assert handler.friendly_name_to_id("just-one-part") is None


def test_friendly_name_to_id_valid_name():
    handler = ThermometerAndHygrometer()
    result = handler.friendly_name_to_id("living-room/foo/sensor-1")
    assert result == {"zone": "home", "area": "living-room", "thing": "sensor-1"}


@pytest.mark.parametrize(
    "handler_cls",
    [TradfriBulbHandler, ActionButtons, MotionLuminance, RainSensor, VINDSTYRKA],
)
def test_friendly_name_to_id_short_name_raises_for_unguarded_handlers(handler_cls):
    # Known bug: unlike ContactSensor/ThermometerAndHygrometer, these handlers
    # don't guard against a friendly_name with too few '/'-separated parts.
    handler = handler_cls()
    with pytest.raises(IndexError):
        handler.friendly_name_to_id("just-one-part")
