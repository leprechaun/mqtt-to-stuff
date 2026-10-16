import argparse

import pytest

from mqtt_to_stuff.cli_args import add_common_arguments, validate_common_arguments


def test_validate_passes_when_host_present():
    parser = argparse.ArgumentParser()
    add_common_arguments(parser)
    args = parser.parse_args(["--host", "mqtt.example.org"])

    validate_common_arguments(parser, args)  # should not raise


def test_validate_exits_cleanly_when_host_missing(monkeypatch, capsys):
    monkeypatch.delenv("MQTT_HOST", raising=False)
    parser = argparse.ArgumentParser(prog="myprog")
    add_common_arguments(parser)
    args = parser.parse_args([])

    with pytest.raises(SystemExit) as exc_info:
        validate_common_arguments(parser, args)

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "myprog: error: --host is required" in err
