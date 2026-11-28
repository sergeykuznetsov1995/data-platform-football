import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from threading import Event
from unittest.mock import MagicMock, Mock

import pytest

from deploy.clubelo import trino_load_meter as club
from scrapers.fotmob import load_meter_receiver as receiver


NOW = datetime(2026, 3, 2, 6, tzinfo=timezone.utc)
DAY = (NOW - club.DAY).date()
START = datetime.combine(DAY, datetime.min.time(), timezone.utc)
TABLE = "clubelo_ratings"
SECOND = "clubelo_history"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    current = NOW

    def set_time(value):
        nonlocal current
        current = value

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return current.astimezone(tz)

    monkeypatch.setattr(receiver, "datetime", Clock)
    monkeypatch.setattr(club, "now_utc", lambda: current)
    monkeypatch.setattr(club, "monotonic", lambda: current.timestamp())
    return set_time


def physical(name, catalog="iceberg", schema="bronze"):
    return {"catalogName": catalog, "schema": schema, "table": name}


def event(i=1, *, reads=(TABLE,), output=None, user="src_clubelo", sql=None, end=None):
    if sql is None:
        sql = "select * from iceberg.bronze." + TABLE
    return {
        "metadata": {"queryId": f"20260301_120000_{i:05}_wm2jf", "query": sql, "queryState": "FINISHED"},
        "context": {"user": user, "source": "untrusted-client-label"},
        "endTime": end or START.isoformat(),
        "ioMetadata": {"inputs": [physical(name) for name in reads],
                       "output": physical(output) if output else None},
    }


def send(obj, data):
    return obj.post(json.dumps(data).encode())


def query(root, sql):
    with club.database(root) as db:
        return db.execute(sql).fetchall()


def coverage(root, minutes=1440, *, day=DAY):
    with club.database(root) as db:
        db.executemany("INSERT INTO minutes VALUES (?,1)",
                       [(club.midnight(day) + n * 60,) for n in range(minutes)])


def monitor(tmp_path, counts, *, day=DAY, sentinel=True):
    # A one-argument executable with the production CSV contract. Reject
    # writes, incorrect metadata names and inclusive/offset day boundaries.
    # An int stands for that many Trino snapshots of unknown queries. A list
    # gives the stored rows (operation, engine, trino_user, trino_query_id,
    # dpf.operation, dpf.source) with missing trailing columns read as empty;
    # a dict {"row": ..., "at": ..., "id": ..., "parent": ...} also fixes the
    # snapshot identity. Snapshot ids and parents follow the list order.
    script = tmp_path / "read-only-client"
    rows = {key: [dict(row, row=pad(row["row"])) if isinstance(row, dict) else pad(row) for row in value]
            if isinstance(value, (list, tuple)) else
            [("overwrite", "Trino", "src_clubelo", f"unmatched-{n}", "", "") for n in range(value)]
            for key, value in counts.items()}
    script.write_text(
        f"#!{sys.executable}\n"
        "import csv, sys\n"
        f"counts = {rows!r}\n"
        f"day = {day.isoformat()!r}\n"
        f"sentinel = {sentinel!r}\n"
        "def stored(key, rows):\n"
        "    out, previous = [], ''\n"
        "    for n, row in enumerate(rows):\n"
        "        fixed = row.get('row', ()) if isinstance(row, dict) else row\n"
        "        out.append((key, *fixed, str(row['id']) if isinstance(row, dict) and 'id' in row else str(n + 1),\n"
        "                    row['parent'] if isinstance(row, dict) and 'parent' in row else previous,\n"
        "                    row['at'] if isinstance(row, dict) and 'at' in row\n"
        "                    else f'{day}T{n // 3600:02d}:{n // 60 % 60:02d}:{n % 60:02d}.000Z'))\n"
        "        previous = out[-1][7]\n"
        "    return out\n"
        "assert len(sys.argv) == 2\n"
        "sql = sys.argv[1]\n"
        "out = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)\n"
        f"if sql == {club.DISCOVER!r}:\n"
        "    out.writerows((k,) for k in counts)\n"
        "else:\n"
        "    parts = sql.split(' UNION ALL ')\n"
        f"    queried = [k for k in sorted(counts) if not __import__('re').fullmatch({club.STAGING.pattern!r}, k)]\n"
        "    assert len(parts) == len(queried) + 1 and parts[-1] == \"SELECT '', '', '', '', '', '', '', '', '', ''\"\n"
        "    for part, key in zip(parts, queried):\n"
        "        assert part.startswith(f'''SELECT '{key}', operation, element_at(summary, 'engine-name'),"
        "\\nelement_at(summary, 'trino_user'), element_at(summary, 'trino_query_id'), "
        "element_at(summary, 'dpf.operation'),\\nelement_at(summary, 'dpf.source'), "
        "cast(snapshot_id AS varchar), coalesce(cast(parent_id AS varchar), ''),\\nto_iso8601(committed_at)"
        "\\nFROM iceberg.bronze.\"{key}$snapshots\"''')\n"
        f"        assert \"committed_at >= TIMESTAMP '{day} 00:00:00 UTC'\" in part\n"
        f"        assert \"committed_at < TIMESTAMP '{day + club.DAY} 01:00:00 UTC'\" in part\n"
        "        out.writerows(stored(key, counts[key]))\n"
        "    if sentinel:\n"
        "        out.writerow(('',) * 10)\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    return script


def pad(row):
    # a stored row without its leading table name: six columns
    return tuple(row) + ("",) * (6 - len(row))


def trino_commit(query_id, user="src_clubelo", operation="overwrite"):
    return (operation, "Trino", user, query_id, "", "")


def test_receiver_mixed_primary_equivalence_and_redaction(tmp_path):
    root = tmp_path / "club"
    old = receiver.EventReceiver(tmp_path / "old")
    shared = receiver.EventReceiver(tmp_path / "shared", clubelo_root=root)
    mixed = event(reads=("fotmob_matches", TABLE, TABLE, "private_foreign_table"),
                  sql="select 'sensitive-literal' from iceberg.bronze.fotmob_matches")
    for payload, outcome in ((json.dumps(mixed).encode(), "kept"),
                             (json.dumps(event(2)).encode(), "filtered"),
                             (b"[]", "malformed"), (b"INVALID", "malformed")):
        assert shared.post(payload) == old.post(payload) == (outcome, 201)
    assert shared.health() == old.health()
    assert shared.health()["kept_total"] == 1
    assert shared.health()["filtered_total"] == 1
    assert shared.health()["malformed_total"] == 2
    name, = shared.health()["files"]
    assert (shared.root / "raw" / name).read_bytes() == (old.root / "raw" / name).read_bytes()
    facts = [json.loads(row[0]) for row in query(root, "SELECT facts FROM events")]
    assert sorted(f["reads"][TABLE] for f in facts) == [1, 2]
    assert query(root, "SELECT 1 FROM gaps")
    raw = (root / "clubelo.sqlite3").read_bytes()
    for secret in (b"sensitive-literal", b"private_foreign_table", b"untrusted-client-label",
                   b"20260301_120000", b"fotmob_matches"):
        assert secret not in raw


def test_secondary_failure_preserves_primary_and_withholds_proof(tmp_path, monkeypatch):
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=tmp_path / "club")

    def fail(*args):
        raise OSError("private-storage-detail")

    monkeypatch.setattr(obj._clubelo, "capture", fail)
    data = event(reads=(TABLE, "fotmob_matches"))
    assert send(obj, data) == ("kept", 201)
    assert query(tmp_path / "club", "SELECT 1 FROM gaps")
    monkeypatch.setattr(obj._clubelo, "gap", fail)
    assert send(obj, data) == ("kept", 201)
    assert obj.health()["kept_total"] == 2
    assert obj.clubelo_health()["ready"] is False
    unavailable = receiver.EventReceiver(tmp_path / "other", clubelo_root=tmp_path / "club" / "clubelo.sqlite3")
    assert send(unavailable, data) == ("kept", 201)
    assert unavailable.clubelo_health() == {"enabled": True, "ready": False}


@pytest.mark.parametrize("user,output,verdict", [
    ("src_clubelo", None, "INCOMPLETE"),
    ("airflow", TABLE, "VIOLATION"),
])
def test_locked_late_mixed_event_invalidates_pass_across_restart(tmp_path, clock, user, output, verdict):
    root, primary = tmp_path / "club", tmp_path / "fm"
    obj = receiver.EventReceiver(primary, clubelo_root=root)
    assert send(obj, event()) == ("filtered", 201)
    coverage(root)
    client = monitor(tmp_path, {TABLE: 0})
    assert club.aggregate(root, DAY, client)["verdict"] == "PASS"
    assert " PASS " in club.render(root)
    report_path = root / "daily" / f"{DAY}.json"
    published = report_path.read_bytes()

    mixed = event(2, reads=(TABLE, "fotmob_matches"), user=user, output=output,
                  end="2026-03-02T00:59:59+01:00")
    mixed["createTime"] = "2026-03-01T23:59:54Z"
    with club.database(root) as writer:
        writer.execute("BEGIN IMMEDIATE")
        assert send(obj, mixed) == ("kept", 201)
        assert obj.health()["kept_total"] == 1
        raw_file, = (primary / "raw").glob("*.jsonl")
        assert json.loads(raw_file.read_text())["query_id"] == mixed["metadata"]["queryId"]
        assert query(root, "SELECT count(*) FROM events") == [(1,)]
        assert query(root, "SELECT count(*) FROM gaps") == [(0,)]
        assert report_path.read_bytes() == published
        assert f" {verdict} " in club.render(root)
        assert obj.clubelo_health()["ready"] is True
        restarted = receiver.EventReceiver(primary, clubelo_root=root)
        assert restarted.clubelo_health()["ready"] is True
        assert f" {verdict} " in club.render(root)

    report = club.aggregate(root, DAY, client)
    assert report["verdict"] == verdict
    assert report["incomplete"] == ["dropped_event"]
    assert report["violations"] == (["airflow_table_use"] if output else [])
    assert report["source_queries"] == 1
    assert f" {verdict} " in club.render(root)
    assert send(restarted, event(3)) == ("filtered", 201)
    assert f" {verdict} " in club.render(root)  # Published query count is now stale.

    # The same receiver can collect a healthy new day as soon as the lock clears.
    assert send(obj, event(4, end=NOW.isoformat())) == ("filtered", 201)
    assert obj.clubelo_health()["event_minutes"] == [int(NOW.timestamp())]
    coverage(root, day=NOW.date())
    clock(NOW + club.DAY)
    assert club.aggregate(root, NOW.date(), monitor(tmp_path, {TABLE: 0}, day=NOW.date()))["verdict"] == "PASS"
    assert " PASS " in club.render(root)


def test_concurrent_unknown_losses_cover_retained_days_and_expire(tmp_path, clock):
    root = tmp_path / "club"
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=root)
    received = NOW.date()
    oldest = received - club.RETENTION * club.DAY
    # Include the oldest retained completion and an independently provable
    # violation on that day, alongside repeated bodies with no usable date.
    completion = datetime.combine(oldest, datetime.min.time(), timezone.utc)
    bad = event(4, user="airflow", output=TABLE, end=completion.isoformat())
    with club.database(root) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=4) as workers:
            results = list(workers.map(obj.post, [b"INVALID", json.dumps(bad).encode()] * 4))
        assert results == [("malformed", 201), ("filtered", 201)] * 4
    assert obj.clubelo_health()["ready"] is True
    names = {path.name for path, _, _ in club.loss_markers(root)}
    assert names == {f"clubelo-loss-{received}.unknown_day", f"clubelo-loss-{oldest}.airflow_table_use"}
    assert all((root / name).read_bytes() == b"" for name in names)
    for offset in range(club.RETENTION + 1):
        reasons = club.losses_for_day(root, received - offset * club.DAY)
        assert "dropped_event" in reasons
    assert not club.losses_for_day(root, oldest - club.DAY)
    assert not club.losses_for_day(root, received + club.DAY)
    report = club.aggregate(root, oldest, monitor(tmp_path, {TABLE: 0}, day=oldest))
    assert report["violations"] == ["airflow_table_use"]
    assert report["verdict"] == "VIOLATION" and "dropped_event" in report["incomplete"]

    clock(NOW + club.RETENTION * club.DAY)
    report = club.aggregate(root, received, monitor(tmp_path, {TABLE: 0}, day=received))
    assert report["verdict"] == "INCOMPLETE" and "dropped_event" in report["incomplete"]
    assert [path.name for path, _, _ in club.loss_markers(root)] == [f"clubelo-loss-{received}.unknown_day"]
    clock(NOW + (club.RETENTION + 1) * club.DAY)
    next_day = received + club.DAY
    report = club.aggregate(root, next_day, monitor(tmp_path, {TABLE: 0}, day=next_day))
    assert "dropped_event" not in report["incomplete"]
    assert list(club.loss_markers(root)) == []


def test_primary_http_progress_during_stalled_capture(tmp_path, monkeypatch, clock):
    previous = NOW - club.MINUTE
    clock(previous)
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=tmp_path / "club")
    send(obj, event(3, reads=(), user="airflow", sql="select 1", end=previous.isoformat()))
    clock(NOW)
    entered, release = Event(), Event()
    capture = obj._clubelo.capture
    mixed = event(reads=(TABLE, "fotmob_matches"), end=NOW.isoformat())

    def slow(data, received):
        if data == mixed:
            entered.set()
            if not release.wait(10):
                raise TimeoutError
        capture(data, received)

    def post_handler(data):
        body = json.dumps(data).encode()
        return Mock(receiver=obj, headers={"Content-Length": str(len(body))}, rfile=io.BytesIO(body))

    monkeypatch.setattr(obj._clubelo, "capture", slow)
    first = post_handler(mixed)

    def other_request():
        health = Mock(receiver=obj, path="/healthz", wfile=io.BytesIO())
        receiver._PostHandler.do_GET(health)
        health.send_response.assert_called_once_with(200)
        primary = json.loads(health.wfile.getvalue())
        assert primary["kept_total"] == 1
        filename, = primary["files"]
        assert json.loads((obj.root / "raw" / filename).read_bytes())["query_id"] == mixed["metadata"]["queryId"]
        unrelated = post_handler(event(2, reads=(), user="airflow", sql="select 1", end=NOW.isoformat()))
        receiver._PostHandler.do_POST(unrelated)
        unrelated.send_response.assert_called_once_with(201)
        assert obj.clubelo_health()["event_minutes"] == [int(previous.timestamp())]

    with ThreadPoolExecutor(max_workers=2) as workers:
        pending = workers.submit(receiver._PostHandler.do_POST, first)
        try:
            assert entered.wait(5)
            first.send_response.assert_called_once_with(201)
            first.end_headers.assert_called_once_with()
            workers.submit(other_request).result(timeout=5)
            assert query(obj._clubelo.root, "SELECT id FROM events") == []
        finally:
            release.set()
        pending.result(timeout=5)
    assert query(obj._clubelo.root, "SELECT count(*) FROM events") == [(1,)]
    assert obj.health()["filtered_total"] == 2
    assert obj.clubelo_health()["event_minutes"] == [int(previous.timestamp()), int(NOW.timestamp())]


@pytest.mark.parametrize("sql", ["SELECT 1", "SELECT current_user;", "SHOW CATALOGS",
                                 'SELECT count(*) FROM iceberg.bronze."clubelo_ratings$snapshots"',
                                 'SELECT count(*) FROM iceberg.bronze."clubelo_ratings$snapshots" WHERE snapshot_id > 1',
                                 "SELECT * FROM iceberg.information_schema.tables WHERE table_name LIKE 'clubelo_%'"])
def test_zero_io_probes(sql):
    assert club.classify(event(sql=sql, reads=()), None) is None


def test_physical_only_sql_only_missing_role_and_exact_identity():
    data = event(user="other-user", reads=(TABLE.upper(), TABLE + "$snapshots", "clubelo_"))
    del data["metadata"]["query"]
    data["context"]["source"] = "src_clubelo"
    data["ioMetadata"]["inputs"] += [physical(TABLE, catalog="other"), physical(TABLE, schema="information_schema")]
    facts = club.classify(data, None)
    assert facts["actor"] == "foreign"
    assert facts["reads"] == {TABLE: 1}
    assert not facts["uncertain"]
    assert club.classify(event(reads=()), None)["uncertain"]
    same_table = event(reads=(), output=TABLE,
                       sql=f"INSERT INTO bronze.{TABLE} SELECT * FROM bronze.{TABLE}")
    assert club.classify(same_table, None)["uncertain"]
    partial = event(output=TABLE)
    partial["statistics"] = {"complete": False}
    assert not club.classify(partial, None)["finished"]
    assert club.classify(event(reads=(), user="other", sql=f"SELECT * FROM other.bronze.{TABLE}"), None) is None
    assert club.classify(event(reads=(), user="other", sql=f"SELECT * FROM iceberg.information_schema.{TABLE}"), None) is None


def test_concurrent_dedup_cross_day_conflict_and_offset(tmp_path):
    one, two = club.Sink(tmp_path), club.Sink(tmp_path)
    data = event(reads=(TABLE, TABLE), end="2026-03-02T00:59:59.999999+01:00")
    with ThreadPoolExecutor(max_workers=8) as workers:
        list(workers.map(lambda sink: sink.capture(data, NOW.isoformat()), [one, two] * 8))
    assert query(tmp_path, "SELECT day FROM events") == [(str(DAY),)]
    changed = copy.deepcopy(data)
    changed["endTime"] = "2026-03-02T01:00:00+01:00"
    two.capture(changed, NOW.isoformat())
    assert query(tmp_path, "SELECT day,reason FROM gaps ORDER BY day") == [
        (str(DAY), "conflicting_query_id"), (str(DAY + club.DAY), "conflicting_query_id")]
    two.capture(event(2, end=changed["endTime"]), NOW.isoformat())
    assert query(tmp_path, "SELECT day FROM events ORDER BY day") == [(str(DAY),), (str(DAY + club.DAY),)]


@pytest.mark.parametrize("n", [1440, 1441])
def test_daily_entrypoints_table_limits_and_attribution(tmp_path, n, capsys):
    root = tmp_path / "meter"
    sink = club.Sink(root, "platform_monitor")
    sink.capture(event(reads=(TABLE,) * (n - 1)), NOW.isoformat())
    sink.capture(event(2, user="platform_monitor"), NOW.isoformat())
    sink.capture(event(3, reads=(SECOND,) * 2, sql=f"select * from bronze.{SECOND}"), NOW.isoformat())
    sink.capture(event(4, reads=(), output=TABLE, sql=f"insert into bronze.{TABLE} values (1)"), NOW.isoformat())
    coverage(root)
    client = monitor(tmp_path, {TABLE: [trino_commit("20260301_120000_00004_wm2jf")], SECOND: 0})
    assert club.main(["aggregate", "--monitor", str(client), "--root", str(root), "--date", str(DAY)]) == 0
    report = json.loads((root / "daily" / f"{DAY}.json").read_text())
    verdict = "PASS" if n == 1440 else "VIOLATION"
    assert report["verdict"] == verdict
    assert report["incomplete"] == []
    assert report["covered_minutes"] == 1440
    table = report["tables"][TABLE]
    assert table["input_records_total"] == n
    assert table["input_records"]["source"] == n - 1
    assert table["input_records"]["monitor"] == 1
    assert table["successful_outputs"]["source"] == table["snapshots"] == table["trino_snapshots"] == 1
    assert report["tables"][SECOND]["input_records_total"] == 2
    assert club.main(["render", "--root", str(root)]) == 0
    assert f"Trino clubelo {DAY} {verdict}" in capsys.readouterr().out


@pytest.mark.parametrize("count,verdict", [(1440, "INCOMPLETE"), (1441, "VIOLATION")])
def test_snapshot_threshold_unattributed_failed_output(tmp_path, count, verdict):
    root = tmp_path / "club"
    sink = club.Sink(root)
    failed = event(output=TABLE, sql=f"insert into bronze.{TABLE} select * from bronze.{TABLE}")
    failed["failureInfo"] = {"detail": "private"}
    sink.capture(failed, NOW.isoformat())
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: count}))
    assert report["verdict"] == verdict
    assert report["tables"][TABLE]["input_records_total"] == 1
    assert report["tables"][TABLE]["successful_outputs"]["source"] == 0
    assert "unattributed_snapshot_or_output" in report["incomplete"]
    assert report["covered_minutes"] == 0


def test_gap_sql_uncertainty_missing_table_and_accounts(tmp_path):
    root = tmp_path / "club"
    sink = club.Sink(root)
    coverage(root, 1439)
    sink.capture(event(reads=()), NOW.isoformat())
    sink.capture(event(2, user=None, reads=(SECOND,)), NOW.isoformat())
    sink.capture(event(3, user="another-service"), NOW.isoformat())
    client = monitor(tmp_path, {TABLE: 0})
    report = club.aggregate(root, DAY, client)
    assert report["verdict"] == "INCOMPLETE"
    assert {"uncertain_event", "missing_table_evidence", "no_source_activity", "unknown_use",
            "foreign_use", "minute_coverage"} <= set(report["incomplete"])
    assert report["covered_minutes"] == 1439
    spoof = event(4, user="airflow")
    spoof["context"]["source"] = "src_clubelo"
    sink.capture(spoof, NOW.isoformat())
    report = club.aggregate(root, DAY, client)
    assert report["verdict"] == "INCOMPLETE" and "unattributed_airflow_read" in report["incomplete"]
    assert report["tables"][TABLE]["input_records"]["airflow"] == 1
    assert report["source_queries"] == 0
    sink.capture(event(5, user="another-service", output=TABLE), NOW.isoformat())
    assert "non_source_output" in club.aggregate(root, DAY, client)["violations"]


def test_shared_airflow_never_passes_and_outputs_violate(tmp_path):
    with pytest.raises(ValueError, match="monitor identity"):
        club.Sink(tmp_path, "airflow")
    sink = club.Sink(tmp_path, "dedicated_reader")
    sink.capture(event(), NOW.isoformat())
    sink.capture(event(2, user="airflow"), NOW.isoformat())
    coverage(tmp_path)
    evidence = {"stable": True, "tables": {TABLE: club.empty_evidence()}}
    with club.database(tmp_path) as db:
        report = club.summarize(db, DAY, evidence)
    assert report["verdict"] == "INCOMPLETE"
    assert report["incomplete"] == ["unattributed_airflow_read"] and report["violations"] == []
    counts = report["tables"][TABLE]["input_records"]
    assert (counts["source"], counts["monitor"], counts["airflow"]) == (1, 0, 1)
    sink.capture(event(3, user="airflow", reads=(TABLE,) * 1438, output=TABLE), NOW.isoformat())
    evidence["tables"][TABLE] = {**club.empty_evidence(), "snapshots": 1, "trino": 1, "users": {"airflow": 1},
                                 "query_ids": ["20260301_120000_00003_wm2jf"]}
    with club.database(tmp_path) as db:
        report = club.summarize(db, DAY, evidence)
    assert report["verdict"] == "VIOLATION" and report["violations"] == ["airflow_table_use"]
    assert report["tables"][TABLE]["successful_outputs"]["airflow"] == 1
    sink.capture(event(4, user="airflow"), NOW.isoformat())
    with club.database(tmp_path) as db:
        report = club.summarize(db, DAY, evidence)
    assert report["violations"] == ["airflow_table_use", "input_record_limit"]


def test_missing_completion_missing_query_id_and_disappearing_table(tmp_path):
    root = tmp_path / "club"
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=root)
    invalid = event(end="not-a-time")
    assert send(obj, invalid) == ("filtered", 201)
    del invalid["metadata"]["queryId"]
    invalid["endTime"] = START.isoformat()
    send(obj, invalid)
    assert {reason for reason, in query(root, "SELECT DISTINCT reason FROM gaps")} == {
        "dropped_event", "missing_query_id"}
    club.aggregate(root, DAY, monitor(tmp_path, {TABLE: 0, SECOND: 0}))
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: 0}))
    assert "missing_table_evidence" in report["incomplete"]
    assert report["tables"][SECOND]["snapshots"] is None


def test_staging_lifecycle_and_table_metadata_are_not_table_activity(tmp_path):
    base = "clubelo_result"
    stage = f"{base}__stg_0123456789ab"
    qualified = f"iceberg.bronze.{base}"
    routine = [
        (f"DESCRIBE {qualified}", None), (f"SHOW COLUMNS FROM {qualified}", None),
        (f"SHOW CREATE TABLE {qualified}", None),
        (f"CREATE TABLE iceberg.bronze.{stage} AS SELECT *, CAST(NULL AS VARCHAR) AS \"replace_op\" "
         f"FROM {qualified} WHERE false", stage),
        (f"INSERT INTO iceberg.bronze.{stage} (match_date) VALUES (DATE '2026-11-27')", stage),
        (f"SELECT count(*) FROM iceberg.bronze.{stage}", None),
        (f"DROP TABLE IF EXISTS iceberg.bronze.{stage}", None),
    ]
    for sql, output in routine:
        reads = (stage,) if sql.startswith("SELECT") else ()
        assert club.classify(event(reads=reads, output=output, sql=sql), None) is None, sql
    # The real write: the staging side of the merge is not a Bronze table scan.
    merge = event(reads=(base, stage), output=base,
                  sql=f"MERGE INTO {qualified} t USING iceberg.bronze.{stage} s ON t.match_date = s.match_date "
                      f"WHEN NOT MATCHED THEN INSERT VALUES (1)")
    facts = club.classify(merge, None)
    assert facts["actor"] == "source" and facts["reads"] == {base: 1} and facts["outputs"] == [base]
    assert not facts["uncertain"] and not facts["maintenance"]
    root = tmp_path / "club"
    club.Sink(root).capture(merge, NOW.isoformat())
    assert [name for name, in query(root, "SELECT name FROM inventory")] == [base]
    # A staging copy that really reads the table is a scan.
    loaded = club.classify(event(reads=(base,), output=stage,
                                 sql=f"CREATE TABLE iceberg.bronze.{stage} AS SELECT * FROM {qualified}"), None)
    assert loaded["reads"] == {base: 1} and loaded["outputs"] == []
    # A write into the Bronze table is never dropped, whatever else the statement names.
    into = club.classify(event(reads=(stage,), output=base,
                               sql=f"INSERT INTO {qualified} SELECT * FROM iceberg.bronze.{stage}"), None)
    assert into["outputs"] == [base] and into["reads"] == {} and not into["uncertain"]
    # A statement that names a staging table and reads another data table is not routine.
    mixed = event(reads=(), sql=f"SELECT * FROM iceberg.bronze.{stage} s JOIN iceberg.other.dim_match d ON s.k = d.k")
    mixed["ioMetadata"]["inputs"] = [physical(stage), physical("dim_match", schema="other")]
    assert club.classify(mixed, None)["uncertain"]
    # Dropping a Bronze table is not routine for the source.
    assert club.classify(event(reads=(), sql=f"DROP TABLE {qualified}"), None)["uncertain"]


def test_platform_maintenance_is_ddl_not_airflow_table_use(tmp_path):
    root = tmp_path / "club"
    sink = club.Sink(root)
    expire = "ALTER TABLE iceberg.bronze.\"clubelo_ratings\" EXECUTE expire_snapshots(retention_threshold => '7d')"
    facts = club.classify(event(reads=(), output=TABLE, user="airflow", sql=expire), None)
    assert facts["actor"] == "airflow" and facts["outputs"] == [TABLE] and facts["maintenance"]
    assert not facts["uncertain"] and not club.event_violations(facts)
    sink.capture(event(), NOW.isoformat())
    sink.capture(event(5, reads=(), output=TABLE, user="airflow", sql=expire), NOW.isoformat())
    coverage(root)
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: []}))
    assert report["verdict"] == "PASS" and report["incomplete"] == []
    assert report["tables"][TABLE]["output_queries"]["airflow"] == 1
    # A data write or another DDL by the shared account is still a violation.
    for sql in (f"INSERT INTO iceberg.bronze.{TABLE} VALUES (1)",
                f"ALTER TABLE iceberg.bronze.{TABLE} ADD COLUMN extra varchar"):
        facts = club.classify(event(reads=(), output=TABLE, user="airflow", sql=sql), None)
        assert not facts["maintenance"] and club.event_violations(facts) == {"airflow_table_use"}


def test_snapshots_are_attributed_by_the_committing_engine(tmp_path):
    root = tmp_path / "club"
    sink = club.Sink(root)
    sink.capture(event(), NOW.isoformat())
    coverage(root)
    bulk = [("delete", "", "", "", ""),
            ("append", "", "", "", "replace-identity-partition-batch")]
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: bulk}))
    assert report["verdict"] == "PASS"
    assert (report["tables"][TABLE]["snapshots"], report["tables"][TABLE]["trino_snapshots"],
            report["tables"][TABLE]["catalog_snapshots"]) == (2, 0, 2)
    unattributed = {"unattributed_snapshot_or_output"}
    # ClubElo's plain bulk append carries the source stamp (clubelo_raw_page).
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [("append", "", "", "", "bulk-arrow-append", "clubelo")]}))
    assert report["verdict"] == "PASS" and report["tables"][TABLE]["catalog_snapshots"] == 1
    # Missing evidence, not a verdict: a Trino commit whose query was not stored
    # and a Trino commit without its query id.
    for rows in ([trino_commit("20260301_120000_00009_wm2jf")], [("overwrite", "Trino", "src_clubelo", "", "")]):
        report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: rows}))
        assert report["verdict"] == "INCOMPLETE" and unattributed <= set(report["incomplete"]), rows
    # Not the source's output: an append without the source stamp, a shape no known
    # writer produces, another engine, another principal.
    for rows in ([("append", "", "", "", "")], [("append", "", "", "", "bulk-arrow-append", "other-source")],
                 [("overwrite", "", "", "", "")], [("append", "spark-3.5", "bob", "", "")],
                 [trino_commit("20260301_120000_00009_wm2jf", user="bob")]):
        report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: rows}))
        assert report["verdict"] == "VIOLATION" and "non_source_output" in report["violations"], rows
    # A query id is matched per table: the write to one table cannot stand in for a
    # commit to another. (SECOND is now a known table and needs its evidence too.)
    sink.capture(event(4, reads=(), output=TABLE, sql=f"insert into bronze.{TABLE} values (1)"), NOW.isoformat())
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [], SECOND: [trino_commit("20260301_120000_00004_wm2jf")]}))
    assert unattributed <= set(report["incomplete"])
    assert report["tables"][SECOND]["trino_snapshots"] == 1
    assert not any(report["tables"][SECOND]["successful_outputs"].values())
    # A successful data write is matched to its committed snapshot by query id.
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: bulk, SECOND: []}))
    assert unattributed <= set(report["incomplete"])
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [trino_commit("20260301_120000_00004_wm2jf")], SECOND: []}))
    assert report["verdict"] == "PASS" and report["incomplete"] == []


def test_stored_report_without_a_table_in_its_evidence_never_renders_pass(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    club.aggregate(root, DAY, monitor(tmp_path, {TABLE: []}))
    assert " PASS " in club.render(root)
    path = root / "daily" / f"{DAY}.json"
    for tables in ({}, {SECOND: club.empty_evidence()}):
        report = json.loads(path.read_text())
        report["snapshot_evidence"]["tables"] = tables
        path.write_text(json.dumps(report))
        assert " PASS " not in club.render(root)


def test_describe_hides_no_write():
    assert club.classify(event(reads=(), sql=f"DESCRIBE iceberg.bronze.{TABLE}"), None) is None
    two = f"DESCRIBE iceberg.bronze.{TABLE}; DROP TABLE iceberg.bronze.{TABLE}"
    for user in ("src_clubelo", "airflow"):
        assert club.classify(event(reads=(), sql=two, user=user), None)["uncertain"]
    dropped = club.classify(event(reads=(), output=TABLE, user="airflow", sql=two), None)
    assert dropped["outputs"] == [TABLE] and club.event_violations(dropped) == {"airflow_table_use"}


def test_legacy_facts_with_staging_tables_do_not_break_the_report(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    stage = f"{TABLE}__stg_0123456789ab"
    with club.database(root) as db:
        db.execute("INSERT INTO inventory VALUES (?,?)", (stage, str(DAY)))
        db.execute("INSERT INTO events VALUES (?,?,?,?)", ("f" * 64, str(DAY), "a" * 64, json.dumps(
            {"actor": "source", "reads": {stage: 1}, "outputs": [stage], "finished": True, "uncertain": True})))
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: []}))
    assert stage not in report["tables"] and "missing_table_evidence" not in report["incomplete"]
    assert report["tables"][TABLE]["input_records_total"] == 1


BATCH = ("append", "", "", "", "replace-identity-partition-batch")
DELETE = ("delete", "", "", "", "")


def test_only_the_delete_half_of_a_replace_batch_is_a_catalog_commit(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    non_source = ("VIOLATION", ["non_source_output"])
    # A delete that no replace batch closes is a writer the source cannot own.
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [DELETE]}))
    assert (report["verdict"], report["violations"]) == non_source
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [DELETE, ("append", "", "", "", "bulk-arrow-append", "clubelo")]}))
    assert (report["verdict"], report["violations"]) == non_source
    # A batch may cross midnight: the delete belongs to the day, its append to the next one.
    closing = {"row": BATCH, "at": f"{DAY + club.DAY}T00:00:00.500Z"}
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [{"row": DELETE, "at": f"{DAY}T23:59:59.500Z"}, closing]}))
    assert report["verdict"] == "PASS"
    assert (report["tables"][TABLE]["snapshots"], report["tables"][TABLE]["catalog_snapshots"]) == (1, 1)


def test_a_staging_table_left_in_the_catalog_is_not_a_bronze_table(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    stage = f"{TABLE}__stg_0123456789ab"
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [], stage: []}))
    assert report["verdict"] == "PASS"
    assert stage not in report["tables"] and stage not in report["snapshot_evidence"]["tables"]
    assert " PASS " in club.render(root)


def test_the_empty_staging_copy_drops_nothing_else():
    stage = f"{TABLE}__stg_0123456789ab"
    ctas = f"CREATE TABLE iceberg.bronze.{stage} AS SELECT * FROM iceberg.bronze.{TABLE} WHERE false"
    assert club.classify(event(reads=(), sql=ctas), None) is None
    # a real Bronze output, another data table, a broken input list, another actor
    assert club.classify(event(reads=(), output=TABLE, sql=ctas), None)["outputs"] == [TABLE]
    other = event(reads=(), sql=ctas)
    other["ioMetadata"]["inputs"] = [physical("dim_match", schema="other")]
    assert club.classify(other, None)["uncertain"]
    broken = event(reads=(), sql=ctas)
    broken["ioMetadata"]["inputs"] = "not-a-list"
    assert club.classify(broken, None)["uncertain"]
    foreign = club.classify(event(reads=(), sql=ctas, user="airflow"), None)
    assert foreign["actor"] == "airflow" and foreign["uncertain"]


def test_a_trino_commit_without_its_query_id_keeps_its_principal(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [("overwrite", "Trino", "bob", "", "")]}))
    assert report["verdict"] == "VIOLATION" and "non_source_output" in report["violations"]
    assert "unattributed_snapshot_or_output" in report["incomplete"]
    # Such a day still renders its verdict; it is not hidden as an unavailable report.
    assert " VIOLATION non_source_output " in club.render(root)
    club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [("overwrite", "Trino", "src_clubelo", "", "")]}))
    assert " INCOMPLETE unattributed_snapshot_or_output " in club.render(root)


def test_a_delete_is_closed_only_by_a_replace_batch_of_its_own_table(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    delete = {"row": DELETE, "id": "77", "at": f"{DAY}T10:00:00.000Z"}
    # the same parent id on another table
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [delete], SECOND: [{"row": BATCH, "id": "88", "parent": "77"}]}))
    assert report["tables"][TABLE]["snapshots"] == 1 and report["verdict"] == "VIOLATION"
    assert report["violations"] == ["non_source_output"]
    # a Trino commit that merely carries the stamp
    other = {"row": ("append", "Trino", "src_clubelo", "20260301_120000_00005_wm2jf", "replace-identity-partition-batch"),
             "id": "88", "parent": "77"}
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [delete, other]}))
    assert report["verdict"] == "VIOLATION" and report["violations"] == ["non_source_output"]


def test_snapshot_evidence_needs_the_sentinel_row(tmp_path):
    root = tmp_path / "club"
    club.Sink(root).capture(event(), NOW.isoformat())
    coverage(root)
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: []}, sentinel=False))
    assert report["verdict"] == "INCOMPLETE" and "snapshot_evidence" in report["incomplete"]
    assert report["tables"][TABLE]["snapshots"] is None


def test_stored_evidence_must_be_internally_consistent(tmp_path):
    root = tmp_path / "club"
    sink = club.Sink(root)
    sink.capture(event(), NOW.isoformat())
    sink.capture(event(4, reads=(), output=TABLE, sql=f"insert into bronze.{TABLE} values (1)"), NOW.isoformat())
    coverage(root)
    club.aggregate(root, DAY, monitor(tmp_path, {TABLE: [trino_commit("20260301_120000_00004_wm2jf")]}))
    assert " PASS " in club.render(root)
    path = root / "daily" / f"{DAY}.json"
    for change in ({"snapshots": 0}, {"trino": 0}, {"users": {"src_clubelo": 0}},
                   {"users": {"src_clubelo": 2}}, {"users": {"bob": 1}}):
        report = json.loads(path.read_text())
        report["snapshot_evidence"]["tables"][TABLE].update(change)
        path.write_text(json.dumps(report))
        assert " PASS " not in club.render(root), change


def test_show_tables_with_a_pattern_is_metadata():
    for sql in (f"SHOW TABLES FROM iceberg.bronze LIKE '{TABLE}'",
                "SHOW TABLES FROM iceberg.bronze LIKE 'clubelo\\_r%' ESCAPE '\\'",
                "SHOW SCHEMAS FROM iceberg LIKE 'bronze'", "SHOW TABLES FROM iceberg.bronze"):
        assert club.classify(event(reads=(), sql=sql), None) is None, sql
    two = f"SHOW TABLES FROM iceberg.bronze LIKE '{TABLE}'; DROP TABLE iceberg.bronze.{TABLE}"
    assert club.classify(event(reads=(), sql=two), None)["uncertain"]


def test_maintenance_exempts_one_procedure_call_only():
    expire = f"ALTER TABLE iceberg.bronze.{TABLE} EXECUTE expire_snapshots(retention_threshold => '7d')"
    optimize = (f"ALTER TABLE iceberg.\"bronze\".\"{TABLE}\" EXECUTE optimize(file_size_threshold => '128MB') "
                f"WHERE \"$path\" IN ('s3://a/b.parquet', 's3://a/c.parquet')")
    for sql in (expire, expire + ";", optimize):
        facts = club.classify(event(reads=(), output=TABLE, user="airflow", sql=sql), None)
        assert facts["maintenance"] and not club.event_violations(facts), sql
    for tail in (f"; DROP TABLE iceberg.bronze.{TABLE}",
                 f" WHERE 1=1; INSERT INTO iceberg.bronze.{TABLE} VALUES (1)"):
        facts = club.classify(event(reads=(), output=TABLE, user="airflow", sql=expire + tail), None)
        assert not facts["maintenance"] and club.event_violations(facts) == {"airflow_table_use"}
    for sql in (f"ALTER TABLE iceberg.bronze.{TABLE} EXECUTE drop_table()",
                f"ALTER TABLE iceberg.bronze.{TABLE} ADD COLUMN extra varchar"):
        assert not club.classify(event(reads=(), output=TABLE, user="airflow", sql=sql), None)["maintenance"]


def test_legacy_facts_that_only_touch_a_staging_table_are_no_activity(tmp_path):
    root = tmp_path / "club"
    stage = f"{TABLE}__stg_0123456789ab"
    club.initialize(root)
    coverage(root)
    with club.database(root) as db:
        db.executemany("INSERT INTO events VALUES (?,?,?,?)", [
            (letter * 64, str(DAY), "a" * 64, json.dumps({"actor": actor, "reads": {stage: 1}, "outputs": [],
                                                          "finished": True, "uncertain": False}))
            for letter, actor in (("a", "source"), ("b", "airflow"))])
    report = club.aggregate(root, DAY, monitor(tmp_path, {TABLE: []}))
    assert "no_source_activity" in report["incomplete"]
    assert not {"unattributed_airflow_read", "input_record_limit"} & (set(report["incomplete"]) | set(report["violations"]))


@pytest.fixture
def local_http(monkeypatch):
    sock = MagicMock()
    connection = Mock()
    response = connection.return_value.getresponse.return_value
    response.status = 200
    response.read.return_value = b'{"ready": true}'
    monkeypatch.setattr(club.socket, "socket", sock)
    monkeypatch.setattr(club.http.client, "HTTPConnection", connection)
    return sock, connection


@pytest.mark.parametrize("host,authority,family", [
    ("172.20.0.1", "172.20.0.1", club.socket.AF_INET),
    ("fd00::1", "[fd00::1]", club.socket.AF_INET6),
])
def test_receipt_owned_bridge(local_http, host, authority, family):
    sock, connection = local_http
    assert club.receipt(f"http://{authority}:4849/clubelo-healthz") == {"ready": True}
    sock.assert_called_once_with(family, club.socket.SOCK_STREAM)
    sock.return_value.__enter__.return_value.bind.assert_called_once_with((host, 0))
    sock.return_value.__exit__.assert_called_once()
    connection.assert_called_once_with(host, 4849, timeout=2)
    connection.return_value.request.assert_called_once_with("GET", "/clubelo-healthz")
    connection.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("host", ["172.20.0.2", "8.8.8.8"])
def test_receipt_unowned_address_never_connects(local_http, host):
    sock, connection = local_http
    sock.return_value.__enter__.return_value.bind.side_effect = OSError("unassigned")
    with pytest.raises(ValueError, match="local health URL"):
        club.receipt(f"http://{host}/clubelo-healthz")
    connection.assert_not_called()


@pytest.mark.parametrize("url", [
    "http://meter.example/clubelo-healthz", "http://operator@localhost/clubelo-healthz",
    "http://0.0.0.0/clubelo-healthz", "http://[::]/clubelo-healthz",
    "http://169.254.1.2/clubelo-healthz", "http://[fe80::1]/clubelo-healthz",
    "http://224.0.0.1/clubelo-healthz", "http://[ff02::1]/clubelo-healthz",
    "http://127.0.0.1/healthz", "https://127.0.0.1/clubelo-healthz",
    "http://127.0.0.1/clubelo-healthz?", "http://127.0.0.1/clubelo-healthz#fragment",
])
def test_receipt_rejects_unsafe_url_offline(local_http, url):
    sock, connection = local_http
    with pytest.raises(ValueError):
        club.receipt(url)
    sock.assert_not_called()
    connection.assert_not_called()


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]"])
def test_receipt_loopbacks_and_no_redirects(local_http, host):
    sock, connection = local_http
    response = connection.return_value.getresponse.return_value
    response.status = 302
    with pytest.raises(ValueError, match="health response"):
        club.receipt(f"http://{host}:4849/clubelo-healthz")
    sock.assert_not_called()
    connection.return_value.request.assert_called_once_with("GET", "/clubelo-healthz")


@pytest.mark.parametrize("traffic", ["none", "unrelated", "source"])
def test_midnight_heartbeat_requires_delivered_completion(tmp_path, monkeypatch, clock, traffic):
    stop = START + club.DAY
    start = stop - club.MINUTE
    clock(start)
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=tmp_path / "club")
    received = start + timedelta(seconds=35)
    clock(received)
    if traffic == "source":
        send(obj, event(end=received.isoformat()))
    elif traffic == "unrelated":
        with monkeypatch.context() as patch:
            database = Mock(side_effect=AssertionError("unrelated completion opened the store"))
            patch.setattr(club, "database", database)
            send(obj, event(user="airflow", reads=(), sql="select 1", end=received.isoformat()))
            database.assert_not_called()
    clock(stop)
    monkeypatch.setattr(club, "receipt", lambda url: obj.clubelo_health())
    stamp = int(start.timestamp())
    health = obj.clubelo_health()
    assert health["ready"] is True
    root = obj._clubelo.root
    if traffic == "none":
        assert health["event_minutes"] == []
        with pytest.raises(ValueError, match="unproven minute"):
            club.heartbeat(root, stop, "ok", "mock")
        assert query(root, "SELECT start,ok FROM minutes") == [(stamp, 0)]
    else:
        assert health["event_minutes"] == [stamp]
        club.heartbeat(root, stop, "ok", "mock")
        assert query(root, "SELECT start,ok FROM minutes") == [(stamp, 1)]
    if traffic == "unrelated":
        assert query(root, "SELECT id FROM events") == []
        assert query(root, "SELECT name FROM inventory") == []
        assert query(root, "SELECT reason FROM gaps") == []


def test_restart_and_late_delivery_do_not_backfill(tmp_path, clock, monkeypatch):
    stop = START + club.DAY
    start = stop - club.MINUTE
    clock(start)
    root = tmp_path / "club"
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=root)
    data = event(end=start.isoformat())
    send(obj, data)
    assert obj.clubelo_health()["event_minutes"] == [int(start.timestamp())]
    clock(stop)
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=root)
    send(obj, data)
    assert obj.clubelo_health()["event_minutes"] == []
    send(obj, event(2, end=stop.isoformat()))
    assert obj.clubelo_health()["event_minutes"] == [int(stop.timestamp())]
    monkeypatch.setattr(club, "receipt", lambda url: obj.clubelo_health())
    with pytest.raises(ValueError, match="unproven minute"):
        club.heartbeat(root, stop, "ok", "mock")
    assert query(root, "SELECT start,ok FROM minutes") == [(int(start.timestamp()), 0)]


def test_delivery_receipt_is_bounded_and_expires_when_quiet(tmp_path, clock):
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=tmp_path / "club")
    for n in range(club.RECEIPT_MINUTES + 2):
        current = NOW + n * club.MINUTE
        clock(current)
        data = event(n, reads=(), user="discovery", sql="select 1", end=current.isoformat())
        send(obj, data)
        send(obj, data)
    assert obj.clubelo_health()["event_minutes"] == [
        int((NOW + n * club.MINUTE).timestamp()) for n in range(2, club.RECEIPT_MINUTES + 2)]
    clock(current + club.RECEIPT_MINUTES * club.MINUTE)
    assert obj.clubelo_health()["event_minutes"] == []


@pytest.mark.parametrize("changes", [
    {"metadata": {"queryId": "seems_valid", "query": "select 1"}},
    {"metadata": {"queryId": "!bad", "query": "select 1", "queryState": "FINISHED"}},
    {"metadata": {"queryId": "seems_valid", "query": "", "queryState": "FINISHED"}},
    {"endTime": "invalid"}, {"ioMetadata": {"inputs": [None]}}, {"statistics": {"complete": False}},
])
def test_broken_event_cannot_supply_minute_evidence(tmp_path, changes):
    obj = receiver.EventReceiver(tmp_path / "fm", clubelo_root=tmp_path / "club")
    data = event(user="airflow", reads=(), sql="select 1", end=NOW.isoformat())
    send(obj, dict(data, **changes))
    assert obj.clubelo_health()["event_minutes"] == []


def test_heartbeat_proves_only_last_full_minute_and_preserves_down(tmp_path, monkeypatch):
    root = tmp_path / "club"
    sink = club.Sink(root)
    end = NOW + timedelta(seconds=4)
    monkeypatch.setattr(club, "now_utc", lambda: end)
    minute = int((NOW - club.MINUTE).timestamp())
    health = dict(sink.health(), checked=end.isoformat(), started=(NOW - club.MINUTE).isoformat(), uptime=64,
                  event_minutes=[minute])
    monkeypatch.setattr(club, "receipt", lambda url: health)
    club.heartbeat(root, end, "ok", "mock")
    assert query(root, "SELECT start,ok FROM minutes") == [(minute, 1)]
    for changes in ({"started": (NOW - timedelta(seconds=59)).isoformat()},
                    {"uptime": 63}, {"checked": (NOW - timedelta(seconds=1)).isoformat()},
                    {"ready": False}, {"store": "unrelated"}, {"enabled": False},
                    {"event_minutes": []}, {"event_minutes": [minute + 60]}, {"event_minutes": str(minute)},
                    {"event_minutes": [float(minute)]}):
        monkeypatch.setattr(club, "receipt", lambda url: dict(health, **changes))
        with pytest.raises(ValueError):
            club.heartbeat(root, end, "ok", "mock")
    monkeypatch.setattr(club, "receipt", lambda url: health)
    club.heartbeat(root, end, "ok", "mock")
    assert query(root, "SELECT start,ok FROM minutes") == [(minute, 0)]
    with pytest.raises(ValueError):
        club.heartbeat(root, end - club.MINUTE, "ok", "mock")


def test_primary_heartbeat_unaffected_by_secondary_failure(tmp_path, monkeypatch, capsys):
    discovered = tmp_path / "tables.json"
    discovered.write_text('{"discovered_tables":[]}')
    root = tmp_path / "fm"
    sink = club.Sink(tmp_path / "club")
    monkeypatch.setattr(club, "receipt", lambda url: {"status": "ok"})
    assert receiver.main(["heartbeat", "--discovered", str(discovered), "--root", str(root),
                          "--clubelo-root", str(sink.root), "--window-end", NOW.isoformat()]) == 0
    primary = json.loads(capsys.readouterr().out)
    assert primary["status"] == "ok"
    assert json.loads(Path(primary["raw_file"]).read_text())["status"] == "ok"
    assert query(sink.root, "SELECT ok FROM minutes") == [(0,)]


def test_serve_adapter(tmp_path, monkeypatch):
    received = []
    monkeypatch.setattr(receiver, "_serve", lambda obj, host, port: received.append(obj))
    assert receiver.main(["serve", "--root", str(tmp_path / "fm"), "--clubelo-root", str(tmp_path / "club"),
                          "--clubelo-monitor-user", "dedicated_reader"]) == 0
    obj, = received
    assert obj.clubelo_health()["ready"] is True
    assert send(obj, event(user="dedicated_reader")) == ("filtered", 201)
    facts, = query(tmp_path / "club", "SELECT facts FROM events")
    assert json.loads(facts[0])["actor"] == "monitor"


def test_renderer_rejects_stale_missing_malformed_and_late_events(tmp_path):
    negative = f"• Trino clubelo {DAY} INCOMPLETE report_unavailable"
    assert club.render(tmp_path) == negative
    assert club.render(tmp_path, "../payload") == negative
    sink = club.Sink(tmp_path)
    sink.capture(event(), NOW.isoformat())
    coverage(tmp_path)
    client = monitor(tmp_path, {TABLE: 0})
    club.aggregate(tmp_path, DAY, client)
    assert " PASS " in club.render(tmp_path)
    assert club.render(tmp_path, str(DAY - club.DAY)) == negative
    sink.capture(event(2), NOW.isoformat())
    assert club.render(tmp_path) == negative
    path = tmp_path / "daily" / f"{DAY}.json"
    report = club.aggregate(tmp_path, DAY, client)
    report["generated_at"] = START.isoformat()
    path.write_text(json.dumps(report))
    assert club.render(tmp_path) == negative
    path.write_text('{"raw":"sensitive"')
    assert club.render(tmp_path) == negative


def test_monitor_failure_and_validation(tmp_path):
    root = tmp_path / "club"
    report = club.aggregate(root, DAY, tmp_path / "missing-client")
    assert report["verdict"] == "INCOMPLETE" and "snapshot_evidence" in report["incomplete"]
    client = monitor(tmp_path, {'bad";SELECT 2': 10})
    assert club.aggregate(root, DAY, client)["snapshot_evidence"]["tables"] == {}
    assert club.main(["aggregate", "--root", str(root), "--monitor", str(client), "--date", "';bad"]) == 1


def test_interrupted_report_refresh_invalidates_previous_pass(tmp_path, monkeypatch):
    sink = club.Sink(tmp_path)
    sink.capture(event(), NOW.isoformat())
    coverage(tmp_path)
    client = monitor(tmp_path, {TABLE: 0})
    assert club.aggregate(tmp_path, DAY, client)["verdict"] == "PASS"

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(club, "write_report", fail)
    with pytest.raises(OSError):
        club.aggregate(tmp_path, DAY, client)
    assert club.render(tmp_path).endswith(" INCOMPLETE report_unavailable")


def test_production_morning_hunk_validates_child_output(monkeypatch):
    from unittest.mock import Mock

    patch = Path(club.__file__).with_name("morning-report-2132.patch").read_text().splitlines()
    hunk = patch[3:]
    before = [line[1:] for line in hunk if line.startswith((" ", "-"))]
    after = [line[1:] for line in hunk if line.startswith((" ", "+"))]
    assert f",{len(before)} " in patch[2] and f",{len(after)} @@" in patch[2]
    run = Mock()
    monkeypatch.setattr(subprocess, "run", run)
    namespace = {"datetime": receiver.datetime, "timezone": timezone, "timedelta": timedelta,
                 "Path": Path, "subprocess": subprocess, "_clubelo_daily": lambda now: "old daily",
                 "_clubelo_dq": lambda now: "old dq"}
    exec(compile("\n".join(after), "morning_report.py", "exec"), namespace)
    prefix = f"• Trino clubelo {DAY}"
    valid = f"{prefix} PASS scans=8 snapshots=2 minutes=1440"
    run.return_value.stdout = valid + "\n"
    assert namespace["clubelo_section"]() == ["old daily", "old dq", valid]
    root = "/root/watchdog/clubelo_trino_meter"
    assert run.call_args.args == (["/usr/bin/python3", "-B", root + "/trino_load_meter.py", "render",
                                   "--root", root, "--date", str(DAY)],)
    for invalid in (valid.replace(str(DAY), str(DAY - club.DAY)), "raw client failure", valid + "\nsecret"):
        run.return_value.stdout = invalid
        assert namespace["clubelo_section"]()[-1] == f"{prefix} INCOMPLETE report_unavailable"


def test_standalone_siblings_and_disabled_optional_import(tmp_path):
    for module in (receiver, receiver.meter):
        shutil.copy(module.__file__, tmp_path)
    env = dict(os.environ, PYTHONPATH="", PYTHONDONTWRITEBYTECODE="1")

    def invoke(code):
        return subprocess.run([sys.executable, "-B", "-S", "-c", code], cwd=tmp_path, env=env,
                              check=True, capture_output=True, text=True)

    base = "from pathlib import Path; from load_meter_receiver import EventReceiver; "
    invoke(base + "r=EventReceiver(Path('fm')); assert r.post(b'{}') == ('filtered',201)")
    shutil.copy(club.__file__, tmp_path)
    invoke(base + "r=EventReceiver(Path('fm'), clubelo_root=Path('club')); "
           "assert r.clubelo_health()['ready']; assert r.post(b'{}') == ('filtered',201)")
    result = subprocess.run([sys.executable, "-B", str(tmp_path / "trino_load_meter.py"), "render",
                             "--root", str(tmp_path / "club"), "--date", "bad-input"],
                            env=env, cwd=tmp_path, check=True, capture_output=True, text=True)
    assert result.stdout.endswith(" INCOMPLETE report_unavailable\n")
    assert "bad-input" not in result.stdout
