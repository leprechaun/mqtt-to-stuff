import json
import logging
import re

from mqtt_to_stuff.logging_setup import configure_logging


def test_json_format_when_log_format_json(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging()

    logging.getLogger("test").info("hello")

    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert payload == {"level": "INFO", "logger": "test", "message": "hello"}


def test_json_format_is_case_insensitive(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "JSON")
    configure_logging()

    logging.getLogger("test").info("hello")

    out = capsys.readouterr().out.strip()
    json.loads(out)  # should not raise


def test_json_format_includes_exception(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging()

    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("test").exception("failed")

    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert payload["message"] == "failed"
    assert "ValueError: boom" in payload["exception"]


def test_plain_text_format_when_log_format_unset(monkeypatch, capsys):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging()

    logging.getLogger("test").info("hello")

    out = capsys.readouterr().out.strip()

    assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ INFO test hello$", out)


def test_plain_text_format_when_log_format_text(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "text")
    configure_logging()

    logging.getLogger("test").info("hello")

    out = capsys.readouterr().out.strip()

    assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ INFO test hello$", out)


def test_logs_go_to_stdout_not_stderr(monkeypatch, capsys):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging()

    logging.getLogger("test").info("hello")

    captured = capsys.readouterr()
    assert "hello" in captured.out
    assert captured.err == ""


def test_log_level_defaults_to_info(monkeypatch, capsys):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    configure_logging()

    logger = logging.getLogger("test")
    logger.debug("should not appear")
    logger.info("should appear")

    out = capsys.readouterr().out
    assert "should not appear" not in out
    assert "should appear" in out


def test_log_level_can_be_lowered_to_debug(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    configure_logging()

    logging.getLogger("test").debug("debug message")

    out = capsys.readouterr().out
    assert "debug message" in out


def test_log_level_can_be_raised_to_warning(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    configure_logging()

    logger = logging.getLogger("test")
    logger.info("should not appear")
    logger.warning("should appear")

    out = capsys.readouterr().out
    assert "should not appear" not in out
    assert "should appear" in out


def test_log_level_is_case_insensitive(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "debug")
    configure_logging()

    logging.getLogger("test").debug("debug message")

    out = capsys.readouterr().out
    assert "debug message" in out


def test_log_level_falls_back_to_info_on_invalid_value(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "not-a-real-level")
    configure_logging()

    logger = logging.getLogger("test")
    logger.debug("should not appear")
    logger.info("should appear")

    out = capsys.readouterr().out
    assert "should not appear" not in out
    assert "should appear" in out
