import datetime

import pytest

from mqtt_to_stuff.register import DeviceRegister, Series


class FakeDevice:
    def __init__(self, key):
        self.key = key
        self.next_result = None

    def set(self, rest, value):
        return self.next_result


@pytest.fixture(autouse=True)
def _reset_device_register_class_state():
    # DeviceRegister.type_map/devices/series are class attributes, not set in
    # __init__, so every instance shares the same dicts (see item #1 from the
    # review - accepted as a known risk, not fixed). Reset between tests so
    # individual tests don't bleed state into each other.
    DeviceRegister.type_map = {}
    DeviceRegister.devices = {}
    DeviceRegister.series = {}
    yield
    DeviceRegister.type_map = {}
    DeviceRegister.devices = {}
    DeviceRegister.series = {}


def test_series_append_without_throttle_always_appends():
    series = Series("test")
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    series.append(t1, "source-a", {"value": 1})
    series.append(t1, "source-a", {"value": 2})
    assert len(series.records) == 2


def test_series_append_with_throttle_drops_within_window():
    series = Series("test", throttle=10)
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    t2 = t1 + datetime.timedelta(seconds=5)
    series.append(t1, "source-a", {"value": 1})
    series.append(t2, "source-a", {"value": 2})
    assert len(series.records) == 1


def test_series_append_with_throttle_allows_after_window():
    series = Series("test", throttle=10)
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    t2 = t1 + datetime.timedelta(seconds=11)
    series.append(t1, "source-a", {"value": 1})
    series.append(t2, "source-a", {"value": 2})
    assert len(series.records) == 2


def test_series_throttle_is_per_source():
    series = Series("test", throttle=10)
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    series.append(t1, "source-a", {"value": 1})
    series.append(t1, "source-b", {"value": 2})
    assert len(series.records) == 2


def test_series_to_list_shape():
    series = Series("test")
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    source = (("zone", "home"), ("area", "kitchen"))
    series.append(t1, source, {"value": 42})

    assert series.to_list() == [{"timestamp": t1, "zone": "home", "area": "kitchen", "value": 42}]


def test_series_clear():
    series = Series("test")
    t1 = datetime.datetime(2026, 1, 1, 12, 0, 0)
    series.append(t1, "source-a", {"value": 1})
    series.clear()
    assert series.records == []


def test_device_register_get_or_create_caches_instance():
    register = DeviceRegister()
    register.add_device_type("plug", FakeDevice)

    first = register.get_or_create("plug", "key-1")
    second = register.get_or_create("plug", "key-1")

    assert first is second


def test_device_register_get_or_create_unregistered_kind_returns_none():
    register = DeviceRegister()
    assert register.get_or_create("unknown", "key-1") is None


def test_device_register_append_data_appends_to_series():
    register = DeviceRegister()
    register.add_device_type("plug", FakeDevice)
    series = Series("electricity")
    register.add_series(series)

    device = register.get_or_create("plug", "key-1")
    device.next_result = ("electricity", [("power", 1.0)])

    register.append_data("plug", "key-1", ("power", "state"), "1")

    assert len(series.records) == 1


def test_device_register_append_data_noop_when_device_returns_none():
    register = DeviceRegister()
    register.add_device_type("plug", FakeDevice)
    series = Series("electricity")
    register.add_series(series)

    register.append_data("plug", "key-1", ("power", "state"), "1")

    assert len(series.records) == 0


def test_device_register_append_data_raises_for_undefined_series():
    register = DeviceRegister()
    register.add_device_type("plug", FakeDevice)

    device = register.get_or_create("plug", "key-1")
    device.next_result = ("does-not-exist", [("power", 1.0)])

    with pytest.raises(Exception):
        register.append_data("plug", "key-1", ("power", "state"), "1")


@pytest.mark.xfail(reason="type_map/devices/series are class attributes shared across all DeviceRegister instances")
def test_device_register_instances_do_not_share_state():
    a = DeviceRegister()
    b = DeviceRegister()

    a.add_device_type("plug", FakeDevice)

    assert b.type_map == {}


def test_get_records_by_type_is_broken():
    # Dead code: references self.records_by_type (never set) and an
    # unimported defaultdict. Documents that it's unusable as-is.
    register = DeviceRegister()
    with pytest.raises(AttributeError):
        register.get_records_by_type()
