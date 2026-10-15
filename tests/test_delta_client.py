import polars as pl

from mqtt_to_stuff.delta_client import DeltaLakeClient


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
