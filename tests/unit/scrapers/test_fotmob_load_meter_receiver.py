"""Synthetic, offline receiver checks; no Trino installation and no live HTTP required."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from scrapers.fotmob import load_meter as meter
from scrapers.fotmob import load_meter_receiver as receiver


REPOSITORY = Path(__file__).parents[3]
DAY = "2026-11-01"
MATCHES = "iceberg.bronze.fotmob_matches"
MANIFEST = "iceberg.bronze.fotmob_ingest_manifest"
SQUADS = "iceberg.bronze.fotmob_squad_snapshots"
TABLES = [MATCHES, MANIFEST, SQUADS]
UPDATE = "UPDATE iceberg.bronze.fotmob_matches SET synthetic_value = 1"
INSERT = "INSERT INTO iceberg.bronze.fotmob_matches VALUES (1)"
READ = "SELECT * FROM iceberg.bronze.fotmob_matches"
POLL = "SELECT * FROM iceberg.bronze.fotmob_ingest_manifest"
RECEIVED_AT = "2026-11-01T00:00:05Z"


def event(query_id="synthetic_query", query=READ, *, user="src_fotmob", **overrides):
    """Nested Trino 484 QueryCompletedEvent: metadata/context/ioMetadata/failureInfo."""
    payload = {
        "metadata": {"queryId": query_id, "query": query, "queryState": "FINISHED", "updateType": None, "uri": "http://example.invalid"},
        "context": {"user": user, "source": "trino_ui", "catalog": "iceberg", "schema": "bronze",
                    "environment": "production", "remoteClientAddress": "127.0.0.1"},
        "ioMetadata": {"inputs": [], "output": None},
        "failureInfo": None,
        "warnings": [],
        "createTime": "2026-11-01T00:00:00Z", "executionStartTime": "2026-11-01T00:00:01Z",
        "endTime": "2026-11-01T00:00:02Z",
    }
    metadata_keys = {"queryId", "query", "queryState", "updateType", "uri"}
    context_keys = {"user", "source", "catalog", "schema"}
    for key, value in overrides.items():
        if key in metadata_keys:
            payload["metadata"][key] = value
        elif key in context_keys:
            payload["context"][key] = value
        elif key == "errorCode":
            payload["failureInfo"] = {"errorCode": value, "message": "synthetic"} if value is not None else None
        else:
            payload[key] = value
    return payload


def minute_heartbeat(minute):
    return receiver.build_heartbeat(root_tables=[], window_start=meter.utc_text(minute),
                                    window_end=meter.utc_text(minute + timedelta(minutes=1)),
                                    discovered_tables=TABLES)


def build_event_day(queries):
    """1440 receiver heartbeats plus snapshot records derived from events."""
    start, _ = meter.day_bounds(DAY)
    records = [minute_heartbeat(start + timedelta(minutes=index)) for index in range(1440)]
    for index, pairs in queries.items():
        minute = start + timedelta(minutes=index)
        for ordinal, (query, user) in enumerate(pairs):
            ended = meter.utc_text(minute + timedelta(seconds=10))
            record = receiver.event_to_snapshot(
                event(f"q_{index:04d}_{ordinal}", query, user=user, endTime=ended),
                tables=TABLES, received_at=meter.utc_text(minute + timedelta(seconds=11)))
            assert record is not None
            records.append(record)
    return records


def test_event_to_snapshot_full_mapping():
    record = receiver.event_to_snapshot(event(), tables=TABLES, received_at=RECEIVED_AT)
    assert record == {
        "grain": "snapshot", "poll_id": "recv", "query_id": "synthetic_query",
        "user": "src_fotmob", "source": "trino_ui", "query": READ, "state": "FINISHED",
        "created": "2026-11-01T00:00:00Z", "started": "2026-11-01T00:00:01Z",
        "ended": "2026-11-01T00:00:02Z", "stmt_type": "read", "error_code": None,
        "io_tables": [], "observed_at": RECEIVED_AT, "received_via": "http_event_listener",
    }
    written = receiver.event_to_snapshot(event(query=INSERT, errorCode="SYNTAX_ERROR"), tables=TABLES, received_at=RECEIVED_AT)
    assert written["stmt_type"] == "insert" and written["error_code"] == "SYNTAX_ERROR"
    assert receiver.event_to_snapshot(event(source=None), tables=TABLES, received_at=RECEIVED_AT)["source"] is None


def test_event_to_snapshot_timestamp_rules():
    naive = receiver.event_to_snapshot(event(createTime=None, executionStartTime="2026-11-01T00:00:01", endTime=None), tables=TABLES, received_at=RECEIVED_AT)
    assert (naive["started"], naive["created"], naive["ended"]) == ("2026-11-01T00:00:01Z", None, None)
    partial = receiver.event_to_snapshot(event(createTime="yesterday", executionStartTime="2026-11-01T00:00:01Z", endTime=""), tables=TABLES, received_at=RECEIVED_AT)
    assert (partial["created"], partial["started"], partial["ended"]) == (None, "2026-11-01T00:00:01Z", None)
    shifted = receiver.event_to_snapshot(event(executionStartTime="2026-11-01T00:00:01+02:00"), tables=TABLES, received_at=RECEIVED_AT)
    assert shifted["started"] is None and shifted["created"] is not None
    assert receiver.event_to_snapshot(event(createTime=None, executionStartTime=None, endTime=None), tables=TABLES, received_at=RECEIVED_AT) is None
    assert receiver.event_to_snapshot(event(createTime="no", executionStartTime="no", endTime="no"), tables=TABLES, received_at=RECEIVED_AT) is None


def test_event_to_snapshot_rejects_impossible_events():
    for overrides in ({"queryId": ""}, {"queryId": "bad id"}, {"queryId": 123}, {"query": ""}, {"query": None}, {"user": ""}, {"user": None}):
        assert receiver.event_to_snapshot(event(**overrides), tables=TABLES, received_at=RECEIVED_AT) is None


@pytest.mark.parametrize(("query", "expected"), [
    ("SELECT * FROM iceberg.bronze.fotmob_matches", True),
    ("insert into iceberg.bronze.FOTMOB_MATCHES values (1)", True),
    ("SELECT * FROM iceberg.bronze.other_matches", False),
    ("SELECT 1", False),
    (None, False),
    (12345, False),
])
def test_keep_event(query, expected):
    assert receiver.keep_event(event(query=query)) is expected


def test_keep_event_and_io_tables_from_io_metadata():
    io = {"inputs": [{"catalogName": "iceberg", "schema": "bronze", "tableName": "fotmob_matches"}],
          "output": {"catalogName": "iceberg", "schema": "bronze", "tableName": "other_summary"}}
    # SQL text without the fotmob_ substring still kept via ioMetadata table names.
    assert receiver.keep_event(event(query="SELECT * FROM other_summary", ioMetadata=io)) is True
    assert receiver.keep_event(event(query="SELECT * FROM other_summary")) is False
    record = receiver.event_to_snapshot(event(query=INSERT, ioMetadata=io), tables=TABLES, received_at=RECEIVED_AT)
    assert record["io_tables"] == ["iceberg.bronze.fotmob_matches", "iceberg.bronze.other_summary"]
    # Missing/partial ioMetadata never breaks mapping.
    assert receiver.event_to_snapshot(event(ioMetadata=None), tables=TABLES, received_at=RECEIVED_AT)["io_tables"] == []


def test_handle_post_body_outcomes():
    def post(body):
        return receiver.handle_post_body(body, tables=TABLES, received_at=RECEIVED_AT)

    records, outcome = post(json.dumps(event()).encode("utf-8"))
    assert outcome == "kept" and len(records) == 1 and records[0]["query_id"] == "synthetic_query"
    assert post(json.dumps(event(query="SELECT 1")).encode("utf-8")) == ([], "filtered")
    assert post(b"{not json") == ([], "malformed")
    assert post(json.dumps([event()]).encode("utf-8")) == ([], "malformed")
    assert post(b"x" * (receiver.MAX_BODY_BYTES + 1)) == ([], "too_large")
    records, outcome = post(json.dumps(event(queryId="bad id")).encode("utf-8"))
    assert (outcome, records) == ("kept", [])


def test_append_records_writes_collides_and_rotates(tmp_path):
    now = meter.utc_datetime("2026-11-02T00:00:00Z")
    record = receiver.event_to_snapshot(event(), tables=TABLES, received_at=RECEIVED_AT)
    first = receiver.append_records(tmp_path, [record], now=now)
    assert first == tmp_path / "raw" / "20261102-000000-recv.jsonl"
    assert json.loads(first.read_text(encoding="utf-8")) == record
    assert receiver.append_records(tmp_path, [record], now=now).name == "20261102-000000-recv1.jsonl"
    assert receiver.append_records(tmp_path, [record], now=now).name == "20261102-000000-recv2.jsonl"
    old = tmp_path / "raw" / "20260901-000000-recv.jsonl"
    foreign = tmp_path / "raw" / "unrelated.jsonl"
    for path in (old, foreign):
        path.write_text("{}\n", encoding="utf-8")
    receiver.append_records(tmp_path, [record], now=meter.utc_datetime("2026-11-18T00:00:00Z"))
    assert not old.exists()
    assert foreign.exists()


def test_build_heartbeat_fields():
    beat = receiver.build_heartbeat(root_tables=[MATCHES], window_start="2026-11-01T00:00:00Z",
                                    window_end="2026-11-01T00:01:00Z", discovered_tables=[MATCHES, "fotmobX", MANIFEST])
    assert beat == {
        "grain": "heartbeat", "poll_id": "20261101_0001", "polled_at": "2026-11-01T00:01:00Z",
        "window_start": "2026-11-01T00:00:00Z", "window_end": "2026-11-01T00:01:00Z",
        "poll_interval_s": 60, "producer_variant": "event_listener_http",
        "watermark_ok": True, "n5_measurable": True, "status": "ok", "gap_reason": None,
        "discovered_tables": [MANIFEST, MATCHES],
    }
    down = receiver.build_heartbeat(root_tables=[], window_start="2026-11-01T00:00:00Z",
                                    window_end="2026-11-01T00:01:00Z", status="down", discovered_tables=TABLES)
    assert down["status"] == "down" and down["gap_reason"] == "producer_down"


def test_receiver_records_feed_aggregate_pass():
    spec = {0: [(UPDATE, "src_fotmob"), (POLL, "src_fotmob")], 10: [(UPDATE, "src_fotmob")], 60: [(READ, "src_fotmob")]}
    _, report = meter.aggregate_day(build_event_day(spec), DAY, snapshot_counts={MATCHES: 2})
    assert report["pass"] and report["pass_reasons"] == []
    assert report["coverage_minutes_total"] == 1440
    assert report["completeness"] == "full"
    assert report["producer_variant"] == "event_listener_http"
    assert report["n5_measurable"] is True


def test_receiver_records_expose_platform_load():
    _, report = meter.aggregate_day(build_event_day({30: [(INSERT, "airflow")]}), DAY, snapshot_counts={MATCHES: 1})
    assert report["platform_ops_total"] >= 1
    assert "N5" in report["pass_reasons"]
    assert not report["pass"]


def test_event_receiver_post_and_health(tmp_path):
    unit = receiver.EventReceiver(tmp_path)
    assert unit.post(json.dumps(event()).encode("utf-8")) == ("kept", 201)
    assert unit.post(json.dumps(event(query="SELECT 1")).encode("utf-8")) == ("filtered", 201)
    assert unit.post(b"not-json")[0] == "malformed"
    assert unit.post(b"x" * (receiver.MAX_BODY_BYTES + 1))[0] == "too_large"
    health = unit.health()
    assert (health["kept_total"], health["filtered_total"], health["malformed_total"]) == (1, 1, 2)
    assert health["last_event_at"] and health["status"] == "ok"
    assert len(health["files"]) == 1 and health["files"][0].endswith("-recv.jsonl")
    records = meter.read_raw((tmp_path / "raw").glob("*.jsonl"))
    assert [record["received_via"] for record in records] == ["http_event_listener"]


def test_tables_cache_reread_on_mtime_change(tmp_path):
    cache = tmp_path / "tables.json"
    cache.write_text(json.dumps({"discovered_tables": [MATCHES]}), encoding="utf-8")
    os.utime(cache, ns=(0, 1_000_000_000))
    unit = receiver.EventReceiver(tmp_path, tables_cache=cache)
    assert unit.tables() == [MATCHES]
    cache.write_text(json.dumps({"discovered_tables": TABLES}), encoding="utf-8")
    os.utime(cache, ns=(0, 2_000_000_000))
    assert unit.tables() == TABLES
    assert receiver.EventReceiver(tmp_path, tables_cache=tmp_path / "missing.json").tables() == []
    assert receiver.EventReceiver(tmp_path).tables() == []


def test_cli_heartbeat_writes_one_record(tmp_path, capsys):
    discovered = tmp_path / "discovered.json"
    discovered.write_text(json.dumps({"discovered_tables": TABLES}), encoding="utf-8")
    assert receiver.main(["heartbeat", "--root", str(tmp_path / "root"), "--discovered", str(discovered),
                          "--status", "ok", "--window-end", "2026-11-02T00:00:00Z"]) == 0
    files = list((tmp_path / "root" / "raw").glob("*-hb.jsonl"))
    assert [path.name for path in files] == ["20261102-000000-hb.jsonl"]
    assert meter.read_raw(files) == [receiver.build_heartbeat(
        root_tables=[], window_start="2026-11-01T23:59:00Z", window_end="2026-11-02T00:00:00Z", discovered_tables=TABLES)]
    assert json.loads(capsys.readouterr().out)["status"] == "ok"


def test_cli_heartbeat_error_prints_type_only(tmp_path, capsys):
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    assert receiver.main(["heartbeat", "--root", str(tmp_path), "--discovered", str(bad),
                          "--window-end", "2026-11-02T00:00:00Z"]) == 1
    output = capsys.readouterr()
    assert output.err.strip().startswith("fotmob_load_meter_receiver: ")
    assert "{" not in output.err and not output.out
    assert receiver.main(["heartbeat", "--root", str(tmp_path), "--discovered", str(tmp_path / "absent.json")]) == 1


def test_import_under_empty_environment_without_trino(tmp_path):
    script = """
import importlib.abc
import sys
class NoTrino(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'trino' or fullname.startswith('trino.'):
            raise AssertionError('eager Trino import')
sys.meta_path.insert(0, NoTrino())
from scrapers.fotmob import load_meter_receiver
assert load_meter_receiver.keep_event({'query': 'SELECT 1 FROM fotmob_matches'}) is True
assert load_meter_receiver.handle_post_body(b'{}', tables=[], received_at='2026-11-01T00:00:00Z') == ([], 'filtered')
"""
    result = subprocess.run([sys.executable, "-c", script], cwd=tmp_path, env={"PYTHONPATH": str(REPOSITORY), "PYTHONDONTWRITEBYTECODE": "1"}, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
