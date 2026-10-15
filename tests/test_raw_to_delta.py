import datetime
from types import SimpleNamespace

import pytest

from mqtt_to_stuff import raw_to_delta


class FakeDeltaLakeClient:
    def __init__(self, raise_on_append=False):
        self.append_calls = []
        self.raise_on_append = raise_on_append

    def append(self, df, path, write_options=None):
        if self.raise_on_append:
            raise RuntimeError("boom")
        self.append_calls.append((df, path, write_options))


@pytest.fixture(autouse=True)
def _reset_buffer():
    raw_to_delta.buffer.clear()
    yield
    raw_to_delta.buffer.clear()


def test_on_message_ignores_matching_topic():
    msg = SimpleNamespace(topic="zigbee2mqtt/foo", payload=b"hello", retain=False)
    raw_to_delta.on_message(None, ["zigbee2mqtt/#"], msg)

    assert raw_to_delta.buffer == []


def test_on_message_buffers_non_ignored_topic():
    msg = SimpleNamespace(topic="devices/home/kitchen/plug/plug1/sensor/power/state", payload=b"1.23", retain=True)
    raw_to_delta.on_message(None, [], msg)

    assert len(raw_to_delta.buffer) == 1
    record = raw_to_delta.buffer[0]
    assert record["topic"] == "devices/home/kitchen/plug/plug1/sensor/power/state"
    assert record["payload"] == "1.23"
    assert record["retain"] is True
    assert isinstance(record["arrival_timestamp"], datetime.datetime)


def test_on_message_treats_none_userdata_as_no_ignores():
    msg = SimpleNamespace(topic="anything", payload=b"x", retain=False)
    raw_to_delta.on_message(None, None, msg)

    assert len(raw_to_delta.buffer) == 1


def test_do_flush_noop_on_empty_buffer():
    dlc = FakeDeltaLakeClient()
    raw_to_delta.do_flush(dlc)
    assert dlc.append_calls == []


def test_do_flush_writes_and_clears_buffer():
    raw_to_delta.buffer.append({
        "topic": "t",
        "arrival_timestamp": datetime.datetime(2026, 1, 1),
        "payload": "p",
        "retain": False,
    })

    dlc = FakeDeltaLakeClient()
    raw_to_delta.do_flush(dlc)

    assert len(dlc.append_calls) == 1
    _df, path, write_options = dlc.append_calls[0]
    assert path == "raw-mqtt"
    assert write_options == {"partition_by": ["date"]}
    assert raw_to_delta.buffer == []


def test_do_flush_swallows_append_errors():
    raw_to_delta.buffer.append({
        "topic": "t",
        "arrival_timestamp": datetime.datetime(2026, 1, 1),
        "payload": "p",
        "retain": False,
    })

    dlc = FakeDeltaLakeClient(raise_on_append=True)
    raw_to_delta.do_flush(dlc)

    # Note: the buffer is cleared before the write is attempted, so a failed
    # write currently drops the batch rather than retrying it.
    assert raw_to_delta.buffer == []
