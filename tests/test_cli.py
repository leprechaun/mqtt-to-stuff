import sys

import pytest

from mqtt_to_stuff import cli
from mqtt_to_stuff.commands import Command


@pytest.fixture(autouse=True)
def _mqtt_host_env(monkeypatch):
    monkeypatch.setenv("MQTT_HOST", "mqtt.example.org")


@pytest.mark.parametrize("command_name", ["zigbee", "raw", "hierarchy"])
def test_host_from_cli_flag(command_name):
    args = cli.parse_args([command_name, "--host", "mqtt.flag.example"])
    assert args.host == "mqtt.flag.example"


@pytest.mark.parametrize("command_name", ["zigbee", "raw", "hierarchy"])
def test_host_from_env(command_name, monkeypatch):
    monkeypatch.setenv("MQTT_HOST", "mqtt.env.example")
    args = cli.parse_args([command_name])
    assert args.host == "mqtt.env.example"


@pytest.mark.parametrize("command_name", ["zigbee", "raw", "hierarchy"])
def test_delta_path_default(command_name, monkeypatch):
    monkeypatch.delenv("DELTA_PATH", raising=False)
    args = cli.parse_args([command_name])
    assert args.delta_path == "/tmp/deltalake/"


@pytest.mark.parametrize("command_name", ["zigbee", "raw", "hierarchy"])
def test_interval_default_and_env(command_name, monkeypatch):
    monkeypatch.delenv("INTERVAL", raising=False)
    assert cli.parse_args([command_name]).interval == 300

    monkeypatch.setenv("INTERVAL", "45")
    assert cli.parse_args([command_name]).interval == 45


@pytest.mark.parametrize("command_name", ["zigbee", "raw", "hierarchy"])
def test_missing_host_exits_cleanly_with_subcommand_usage(command_name, monkeypatch, capsys):
    monkeypatch.delenv("MQTT_HOST", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        cli.parse_args([command_name])

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert f"mqtt-to-stuff {command_name}: error: --host is required" in err
    assert f"usage: mqtt-to-stuff {command_name}" in err


def test_no_command_is_an_error():
    with pytest.raises(SystemExit) as exc_info:
        cli.parse_args([])
    assert exc_info.value.code == 2


def test_unknown_command_is_an_error():
    with pytest.raises(SystemExit) as exc_info:
        cli.parse_args(["bogus"])
    assert exc_info.value.code == 2


def test_hierarchy_topic_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("MQTT_TOPIC", "devices/#")
    args = cli.parse_args(["hierarchy"])
    assert args.topics == ["devices/#"]


def test_hierarchy_cli_topic_overrides_env(monkeypatch):
    monkeypatch.setenv("MQTT_TOPIC", "devices/#")
    args = cli.parse_args(["hierarchy", "-t", "custom/topic"])
    assert args.topics == ["custom/topic"]


def test_hierarchy_multiple_cli_topics():
    args = cli.parse_args(["hierarchy", "-t", "a/#", "-t", "b/#"])
    assert args.topics == ["a/#", "b/#"]


def test_hierarchy_no_topic_and_no_env_is_none(monkeypatch):
    monkeypatch.delenv("MQTT_TOPIC", raising=False)
    args = cli.parse_args(["hierarchy"])
    assert args.topics is None


def test_raw_ignore_defaults_to_empty_list():
    args = cli.parse_args(["raw"])
    assert args.ignored_topics == []


def test_raw_ignore_collects_repeated_flags():
    args = cli.parse_args(["raw", "--ignore", "a/#", "--ignore", "b/#"])
    assert args.ignored_topics == ["a/#", "b/#"]


def test_zigbee_has_no_command_specific_arguments():
    args = cli.parse_args(["zigbee"])
    assert args.host == "mqtt.example.org"


def test_commands_registry_covers_all_three():
    assert set(cli.COMMANDS_BY_NAME) == {"zigbee", "raw", "hierarchy"}


def test_cli_dispatches_to_the_selected_commands_run(monkeypatch):
    calls = []

    def fake_run(args):
        calls.append(args)
        return 0

    dummy_command = Command(name="hierarchy", help="test", add_arguments=lambda parser: None, run=fake_run)
    monkeypatch.setitem(cli.COMMANDS_BY_NAME, "hierarchy", dummy_command)
    monkeypatch.setattr(sys, "argv", ["mqtt-to-stuff", "hierarchy", "--host", "mqtt.example.org"])

    with pytest.raises(SystemExit) as exc_info:
        cli.cli()

    assert exc_info.value.code == 0
    assert len(calls) == 1
    assert calls[0].host == "mqtt.example.org"
