import json

import polars as pl

from mqtt_to_stuff.delta_client import DeltaLakeClient
from mqtt_to_stuff.logging_setup import configure_logging


def test_append_writes_and_reads_back(tmp_path):
    dlc = DeltaLakeClient(str(tmp_path) + "/", {})
    dlc.append(pl.DataFrame({"value": [1, 2, 3]}), "my-table")

    result = dlc.get("my-table")

    assert result.shape[0] == 3


def test_append_accumulates_rows_across_calls(tmp_path):
    dlc = DeltaLakeClient(str(tmp_path) + "/", {})
    dlc.append(pl.DataFrame({"value": [1]}), "my-table")
    dlc.append(pl.DataFrame({"value": [2]}), "my-table")

    result = dlc.get("my-table")

    assert result.shape[0] == 2


def test_no_compaction_before_threshold(tmp_path):
    dlc = DeltaLakeClient(str(tmp_path) + "/", {})
    dlc.append(pl.DataFrame({"value": [1]}), "my-table")
    dlc.append(pl.DataFrame({"value": [2]}), "my-table")

    checkpoints = list((tmp_path / "my-table" / "_delta_log").glob("*checkpoint*"))

    assert checkpoints == []


def test_compaction_triggers_at_configured_threshold(tmp_path):
    dlc = DeltaLakeClient(str(tmp_path) + "/", {}, compact_at=2)
    dlc.append(pl.DataFrame({"value": [1]}), "my-table")
    dlc.append(pl.DataFrame({"value": [2]}), "my-table")

    checkpoints = list((tmp_path / "my-table" / "_delta_log").glob("*checkpoint*"))

    assert len(checkpoints) > 0


def test_base_path_property_exposes_configured_base_path(tmp_path):
    base_path = str(tmp_path) + "/"
    dlc = DeltaLakeClient(base_path, {})

    assert dlc.base_path == base_path


def test_compaction_log_includes_full_absolute_path(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging()

    dlc = DeltaLakeClient(str(tmp_path) + "/", {}, compact_at=2)
    dlc.append(pl.DataFrame({"value": [1]}), "my-table")
    dlc.append(pl.DataFrame({"value": [2]}), "my-table")

    out = capsys.readouterr().out
    compacted_lines = [json.loads(line) for line in out.splitlines() if '"compacted"' in line]

    assert len(compacted_lines) == 1
    assert compacted_lines[0]["path"] == str(tmp_path) + "/my-table"
