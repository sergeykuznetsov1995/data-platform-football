"""Offline SQL semantics using DuckDB; no source HTTP, Trino or Airflow."""
import ast
import copy
import io
import json
from types import SimpleNamespace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re
import subprocess

import duckdb
import pytest
import sqlglot

from scrapers.fotmob import c1_inputs as c1

ROOT = Path(__file__).resolve().parents[3]
LIVE = ROOT / "tests/fixtures/fotmob/morning_report_c1_legacy.py"
PATCH = ROOT / "deploy/fotmob/morning-report-c1-inputs.patch"


def functions(source):
    names = {"_c1_sql", "_c1_classify", "_c1_measure", "_c1_streak", "fotmob_daily_criterion"}
    tree = ast.parse(source)
    tree.body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = dict(datetime=datetime, timedelta=timedelta, timezone=timezone,
                     Decimal=Decimal, ROUND_HALF_UP=ROUND_HALF_UP,
                     C1_GAP_MIN_RUNS=2, C1_GAP_MIN_SPREAD_S=7200,
                     C1_LATE_GRACE_S=86400, C1_REFRESH_DAYS=7, C1_SQL_TIMEOUT_S=900,
                     json=json, sys=SimpleNamespace(argv=[]), DAILY_CRITERION_STATE="unused")
    exec(compile(tree, "morning_report.py", "exec"), namespace)
    return namespace


@pytest.fixture
def reports(tmp_path):
    if not LIVE.is_file():
        pytest.fail("#1896 frozen morning report fixture is missing")
    old = LIVE.read_text()
    target = tmp_path / "morning_report.py"
    target.write_text(old)
    result = subprocess.run(["patch", "--batch", "-p1", "-i", str(PATCH)],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    patched = target.read_text()
    # Enforce the brief: SQL only; classifier, rounding and state stay byte exact.
    before = old[:old.index("def _c1_sql(d):")]
    after = old[old.index("def _c1_measure"):]
    assert patched.startswith(before)
    assert patched.endswith(after)
    return functions(old), functions(patched)


class Database:
    def __init__(self):
        self.db = duckdb.connect()
        self.queries = []
        self.db.execute("CREATE MACRO from_iso8601_timestamp(x) AS CAST(x AS TIMESTAMPTZ)")
        self.db.execute("CREATE MACRO to_iso8601(x) AS strftime(x, '%Y-%m-%dT%H:%M:%S')")

    def execute_query(self, sql):
        self.queries.append(sql)
        sql = sql.replace(c1.PREFIX, "")
        sql = re.sub(r" FOR VERSION AS OF \d+", "", sql)
        sql = re.sub(r" WITH \(partitioning = ARRAY\['deadline_day'\]\)", "", sql)
        translated = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        return self.db.execute(translated).fetchall()


@pytest.fixture
def db():
    manager = Database()
    for table in (c1.MATCHES, c1.PAYLOADS, c1.SCOPE, c1.SEASONS):
        columns = {
            c1.MATCHES: "match_id BIGINT, utc_time VARCHAR, cancelled BOOLEAN, finished BOOLEAN",
            c1.PAYLOADS: "match_id BIGINT",
            c1.SCOPE: "decision VARCHAR",
            c1.SEASONS: "is_latest BOOLEAN",
        }[table]
        manager.db.execute(f"CREATE TABLE {table} (competition_id {'BIGINT' if table in (c1.MATCHES, c1.PAYLOADS) else 'VARCHAR'}, source_season_key VARCHAR, "
                           f"{columns}, _target_batch_id VARCHAR, _observed_at TIMESTAMP, _ingested_at TIMESTAMP)")
    manager.db.execute(f"CREATE TABLE {c1.MANIFEST} (competition_id VARCHAR, source_season_key VARCHAR, "
                       "target_type VARCHAR, entity_id VARCHAR, batch_id VARCHAR, parser_version VARCHAR, "
                       "status VARCHAR, completed_at TIMESTAMP, run_id VARCHAR, error_code VARCHAR)")
    return manager


def manifest(db, batch, *, kind="league_season", cid="1", sk="2026", mid=None,
             version=c1.PARSER_VERSION, status="success", when="2026-10-20 12:00:00",
             run="r1", error=None):
    db.db.execute(f"INSERT INTO {c1.MANIFEST} VALUES (?,?,?,?,?,?,?,?,?,?)",
                  [cid, sk, kind, mid, batch, version, status, when, run, error])


def calendar(db, mid=10, batch="cal", *, cid="1", sk="2026", utc="2026-10-19T22:00:00Z",
             finished=True, cancelled=False, observed="2026-10-20 12:00:00", ingested=None):
    db.db.execute(f"INSERT INTO {c1.MATCHES} VALUES (?,?,?,?,?,?,?,?,?)",
                  [cid, sk, mid, utc, cancelled, finished, batch, observed, ingested or observed])


def setup_scope(db, *, cid="1", sk="2026", included=True, latest=True):
    for table, kind, value in ((c1.SCOPE, "competition_profile", "included" if included else "excluded"),
                                (c1.SEASONS, "competition_seasons", latest)):
        batch = kind + cid + sk
        db.db.execute(f"INSERT INTO {table} VALUES (?,?,?,?,?,?)",
                      [cid, sk, value, batch, "2026-10-20 12:00:00", "2026-10-20 12:00:00"])
        manifest(db, batch, kind=kind, cid=cid, sk=sk)


def pinned():
    return dict.fromkeys(c1.SOURCES, 1)


def materialized(db, pair=("1", "2026")):
    return db.execute_query(c1.inputs_sql(pair, pinned(), date(2026, 10, 20),
                                         date(2026, 10, 23)))


@pytest.mark.parametrize("scenario", ["v1", "v2", "empty", "tombstone", "deleted", "moved"])
def test_calendar_selection_before_day_filter(db, scenario):
    manifest(db, "v1", version=c1.LEGACY_PARSER_VERSION)
    calendar(db, batch="v1")
    if scenario != "v1":
        manifest(db, "v2", when="2026-10-19 00:00:00")  # v2 wins even if older
        if scenario not in ("empty", "tombstone"):
            calendar(db, batch="v2", mid=11 if scenario == "deleted" else 10,
                     utc="2026-10-25T22:00:00Z" if scenario == "moved" else "2026-10-19T22:00:00Z")
    if scenario == "tombstone":
        manifest(db, "gone", status="not_available", when="2026-10-21 12:00:00")
    setup_scope(db)
    rows = materialized(db)
    expected = [] if scenario in ("moved", "tombstone") else ["11" if scenario == "deleted" else "10"]
    assert [row[3] for row in rows] == expected


@pytest.mark.parametrize("scenario", ["normal", "gap", "duplicate_run", "late_ft", "boundary", "scope", "old_season"])
def test_materialization_matches_frozen_inputs_and_classification(db, reports, scenario):
    old, new = reports
    setup_scope(db, included=scenario != "scope", latest=scenario != "old_season")
    if scenario == "old_season":
        setup_scope(db, sk="2027")
    manifest(db, "cal")
    calendar(db, finished=scenario not in ("late_ft", "old_season"))
    if scenario == "late_ft":
        # Observe unfinished in the original deadline window; first FT after it.
        calendar(db, batch="historical", finished=False, observed="2026-10-20 10:00:00")
        calendar(db, batch="uncommitted-ft", observed="2026-10-21 03:00:00")
    if scenario in ("gap", "duplicate_run"):
        for hour in (1, 4):
            manifest(db, f"na{hour}", kind="match", mid="10", status="not_available",
                     when=f"2026-10-20 {hour:02d}:00:00", run="same" if scenario == "duplicate_run" else str(hour),
                     error="source_data_not_found")
    else:
        when = "2026-10-21 00:00:00" if scenario == "boundary" else "2026-10-20 20:00:00"
        # Deliberately no success manifest: fi counts uncommitted payloads too.
        db.db.execute(f"INSERT INTO {c1.PAYLOADS} VALUES ('1','2026',10,'p',?,?)", [when, when])
    # Independent expected current calendar/scope/season for the frozen SQL.
    for table in (c1.MATCHES, c1.SCOPE, c1.SEASONS):
        predicate = " WHERE _target_batch_id='cal'" if table == c1.MATCHES else ""
        db.db.execute(f"CREATE VIEW {table}_current AS SELECT * FROM {table}{predicate}")
    expected = db.execute_query(old["_c1_sql"]("2026-10-21"))
    rows = materialized(db)
    actual = [tuple(row[3:]) for row in rows if row[0] == date(2026, 10, 21)]
    # Empty-day marker is supplied by the publisher, not inputs_sql.
    assert actual == ([] if expected[0][0] is None else expected)
    now = datetime(2026, 10, 22)
    assert new["_c1_classify"](actual, now) == old["_c1_classify"](expected, now)


def publish(db, rows=(), *, first="2026-10-20", last="2026-10-23", complete=True):
    db.execute_query(f"CREATE TABLE {c1.TABLE} {c1.DDL}")
    values = []
    day = date.fromisoformat(first)
    while day <= date.fromisoformat(last):
        values.append((day, None, None, None, "day", *([None] * 7), "gen", complete, None))
        day += timedelta(days=1)
    for row in rows:
        values.append((*row[:4], "input", *row[4:], "gen", complete, None))
    c1._stage_rows(db, c1.TABLE, values)


def test_reader_one_scan_four_dates_empty_missing_incomplete(db, reports):
    _, new = reports
    publish(db)
    sql = c1.read_sql("2026-10-20", "2026-10-23", include_day=True)
    assert sql.count(c1.TABLE) == 1
    assert "_current" not in sql
    assert "BETWEEN DATE '2026-10-20' AND DATE '2026-10-23'" in sql
    rows = db.execute_query(sql)
    assert {row[0].date() if isinstance(row[0], datetime) else row[0] for row in rows} == {date(2026, 10, d) for d in range(20, 24)}
    assert new["_c1_classify"](db.execute_query(new["_c1_sql"]("2026-10-20"))) == (0, 0, 0, 0, 0)
    for sql in (new["_c1_sql"]("2026-10-24"), c1.read_sql("2026-10-23", "2026-10-24")):
        with pytest.raises(duckdb.ConversionException, match="C1 incomplete day"):
            db.execute_query(sql)
    db.db.execute("UPDATE fotmob_c1_inputs SET complete=false WHERE deadline_day=DATE '2026-10-21'")
    with pytest.raises(duckdb.ConversionException, match="C1 incomplete day"):
        db.execute_query(new["_c1_sql"]("2026-10-21"))


def test_merge_replaces_removed_and_moved_keys_atomically(db):
    deadline = datetime(2026, 10, 21)
    row = (date(2026, 10, 21), "1", "2026", "10", deadline, None, 0, 0, 0, None, 0)
    publish(db, [row])
    stage = c1.PREFIX + "stage"
    db.execute_query(f"CREATE TABLE {stage} {c1.DDL}")
    removed = (*row[:4], "input", *([None] * 7), "new", False, None)
    moved = (date(2026, 10, 22), *row[1:4], "input", *row[4:], "new", True, None)
    c1._stage_rows(db, stage, [removed, moved])
    db.execute_query(c1.merge_sql(stage))
    assert db.db.execute("SELECT deadline_day FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == [(date(2026, 10, 22),)]


def test_patched_sql_reads_only_materialization(reports):
    _, new = reports
    sql = new["_c1_sql"]("2026-10-21")
    assert sql.count(c1.TABLE) == 1
    assert "deadline_day=DATE '2026-10-21'" in sql
    assert "_current" not in sql
    for table in c1.SOURCES:
        assert c1.PREFIX + table not in sql


def test_rounding_and_pending_use_unchanged_classifier(reports):
    _, new = reports
    deadline = datetime(2026, 10, 21)
    rows = [(str(i), deadline, deadline if i < 397 else None, 0, 0, 0, None, 0) for i in range(400)]
    new["_trino_rows"] = lambda *args, **kwargs: rows
    assert new["_c1_measure"]("2026-10-21", deadline)["pct"] == 99.3
    pending = [("x", deadline, None, 0, 0, 0, None, 1)]
    assert new["_c1_classify"](pending, deadline)[-1] == 1
    assert new["_c1_classify"](pending, deadline + timedelta(days=8))[-1] == 0


def test_partition_change_detection_handles_removed_partitions():
    assert c1.changed_pairs({("1", "old"): "x", ("2", "s"): "a"},
                            {("2", "s"): "b", ("3", "new"): "c"}) == {
                                ("1", "old"), ("2", "s"), ("3", "new")}


class SnapshotHarness:
    """Immutable source versions in DuckDB; metadata reads stay offline."""
    def __init__(self, db, monkeypatch):
        self.db = db
        self.version = 0
        self.inventory = {}
        self.original_execute = db.execute_query

        def execute(sql):
            for table in c1.SOURCES:
                sql = re.sub(rf"{table} FOR VERSION AS OF (\d+)",
                             lambda match: f"{table}_v{match[1]}", sql)
            return self.original_execute(sql)

        db.execute_query = execute
        monkeypatch.setattr(c1, "snapshots", lambda manager: dict.fromkeys(c1.SOURCES, self.version))
        monkeypatch.setattr(c1, "partition_fingerprints", lambda manager, table: self.inventory[table])
        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 28, tzinfo=tz)
        monkeypatch.setattr(c1, "datetime", FrozenDatetime)

    def capture(self):
        self.version += 1
        for table in c1.SOURCES:
            self.db.db.execute(f"CREATE TABLE {table}_v{self.version} AS SELECT * FROM {table}")
            if table == c1.MANIFEST:
                continue
            groups = {}
            for row in self.db.db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall():
                key = (str(row[0]), row[1] if table in (c1.MATCHES, c1.PAYLOADS) else "")
                groups.setdefault(key, []).append(row)
            self.inventory[table] = {key: repr(rows) for key, rows in groups.items()}


def test_refresh_incremental_move_delete_and_checkpoint(db, monkeypatch):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    result = c1.refresh(db, first_day="2026-10-20", last_day="2026-10-27")
    assert result["seasons"] == 1
    assert db.db.execute("SELECT deadline_day FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == [(date(2026, 10, 21),)]
    db.queries.clear()
    c1.refresh(db)
    assert not any("WITH inc AS" in query for query in db.queries)
    manifest(db, "moved", when="2026-10-22 12:00:00")
    calendar(db, batch="moved", utc="2026-10-20T22:00:00Z")
    harness.capture()
    c1.refresh(db)
    assert db.db.execute("SELECT deadline_day FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == [(date(2026, 10, 22),)]
    manifest(db, "gone", status="not_available", when="2026-10-23 12:00:00")
    harness.capture()
    c1.refresh(db)
    assert db.db.execute("SELECT * FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == []
    checkpoint = db.db.execute("SELECT checkpoint_json FROM fotmob_c1_inputs WHERE kind='checkpoint'").fetchone()[0]
    import json
    assert json.loads(checkpoint)["snapshots"][c1.MANIFEST] == harness.version
    assert db.db.execute("SELECT count(*) FROM fotmob_c1_inputs WHERE kind='day' AND complete").fetchone() == (8,)


@pytest.mark.parametrize("failure", ["stage", "merge", "snapshot"])
def test_failed_refresh_cannot_publish_partial_generation(db, monkeypatch, failure):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-27")
    before = db.db.execute("SELECT * FROM fotmob_c1_inputs ORDER BY ALL").fetchall()
    manifest(db, "gone", status="not_available", when="2026-10-23 12:00:00")
    harness.capture()
    execute = db.execute_query
    def fail(sql):
        if failure == "stage" and sql.startswith("INSERT INTO iceberg.bronze.fotmob_c1_inputs__stg_"):
            raise RuntimeError("injected stage failure")
        if failure == "merge" and sql.startswith("MERGE"):
            raise RuntimeError("injected merge failure")
        if failure == "snapshot" and sql.startswith("INSERT INTO iceberg.bronze.fotmob_c1_inputs__stg_"):
            harness.version += 1
        return execute(sql)
    db.execute_query = fail
    with pytest.raises(RuntimeError):
        c1.refresh(db)
    assert db.db.execute("SELECT * FROM fotmob_c1_inputs ORDER BY ALL").fetchall() == before


def test_new_day_reads_only_new_window(db, monkeypatch):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-26")
    db.queries.clear()
    c1.refresh(db)
    queries = [query for query in db.queries if "WITH inc AS" in query]
    assert queries == []
    assert db.db.execute("SELECT count(*) FROM fotmob_c1_inputs WHERE kind='day'").fetchone() == (8,)


def test_shared_match_ids_keep_natural_keys_and_frozen_evidence(db, reports):
    old, _ = reports
    for cid in ("1", "2"):
        setup_scope(db, cid=cid)
        manifest(db, "cal" + cid, cid=cid)
        calendar(db, cid=cid, batch="cal" + cid, finished=False)
        calendar(db, cid=cid, batch="uncommitted" + cid, observed="2026-10-21 01:00:00")
    for table in (c1.MATCHES, c1.SCOPE, c1.SEASONS):
        predicate = " WHERE _target_batch_id IN ('cal1','cal2')" if table == c1.MATCHES else ""
        db.db.execute(f"CREATE VIEW {table}_current AS SELECT * FROM {table}{predicate}")
    pairs = {("1", "2026"), ("2", "2026")}
    rows = db.execute_query(c1.inputs_sql(pairs, pinned(), date(2026, 10, 21), date(2026, 10, 21)))
    assert len(rows) == 2
    assert {row[1] for row in rows} == {"1", "2"}
    expected = db.execute_query(old["_c1_sql"]("2026-10-21"))
    assert [tuple(row[3:]) for row in rows] == expected


def test_normal_task_refuses_bootstrap_conf():
    from types import SimpleNamespace
    with pytest.raises(ValueError, match="approved window"):
        c1.run_refresh(dag_run=SimpleNamespace(conf={"first_day": "2026-10-20"}))


def test_reader_rejects_stale_generation(db, reports):
    _, new = reports
    publish(db)
    db.db.execute("UPDATE fotmob_c1_inputs SET published_at=TIMESTAMP '2026-01-01 00:00:00'")
    with pytest.raises(duckdb.ConversionException, match="C1 incomplete day"):
        db.execute_query(new["_c1_sql"]("2026-10-21"))


def test_manifest_duplicate_success_advances_inputs(db, monkeypatch):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    manifest(db, "success", kind="match", mid="10")
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-27")
    manifest(db, "success", kind="match", mid="10")  # exact duplicate physical row
    harness.capture()
    c1.refresh(db)
    assert db.db.execute("SELECT success_count FROM fotmob_c1_inputs WHERE kind='input'").fetchone() == (2,)


def test_future_inputs_are_updated_before_their_day_opens(db, monkeypatch):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db, utc="2026-10-26T22:00:00Z")  # deadline 28th, beyond bootstrap markers
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-26")
    assert db.db.execute("SELECT deadline_day FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == [(date(2026, 10, 28),)]
    manifest(db, "gone", status="not_available", when="2026-10-27 12:00:00")
    harness.capture()
    c1.refresh(db)
    assert db.db.execute("SELECT * FROM fotmob_c1_inputs WHERE kind='input'").fetchall() == []


def test_physical_partition_predicates_match_trino_types():
    sql = c1.candidates_sql({("1", "2026")}, pinned(), date(2026, 10, 20))
    assert "competition_id=1 AND source_season_key='2026'" in sql
    assert "inc.competition_id=CAST(m.competition_id AS varchar)" in sql
    assert "s.competition_id=CAST(m.competition_id AS varchar)" in sql
    with pytest.raises(ValueError):
        c1.pairs_sql({("bad", "2026")}, physical=True)


def test_refresh_staging_is_owned_by_standard_cleanup(db, monkeypatch):
    from scripts.fotmob_cleanup import STAGING_RE

    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-27")
    stages = [query.split()[2].removeprefix(c1.PREFIX) for query in db.queries
              if query.startswith("CREATE TABLE iceberg.bronze.") and "__stg_" in query]
    assert len(stages) == 2
    assert all(STAGING_RE.fullmatch(stage) for stage in stages)


def test_contract_change_requires_explicit_rebootstrap(db, monkeypatch):
    setup_scope(db)
    manifest(db, "cal")
    calendar(db)
    harness = SnapshotHarness(db, monkeypatch)
    harness.capture()
    c1.refresh(db, first_day="2026-10-20", last_day="2026-10-27")
    monkeypatch.setattr(c1, "CONTRACT_VERSION", c1.CONTRACT_VERSION + 1)
    with pytest.raises(RuntimeError, match="contract changed"):
        c1.refresh(db)


def test_frozen_state_is_unchanged_and_only_pending_days_remeasured(reports):
    _, new = reports
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 28, 9, tzinfo=tz)
    new["datetime"] = FixedDatetime
    final = {"due": 1, "ok": 1, "pct": 100.0, "streak": 1}
    state = {"days": {"2026-10-27": final}}
    saves = []
    new["open"] = lambda *args, **kwargs: io.StringIO(json.dumps(state))
    new["_c1_save"] = lambda value: saves.append(copy.deepcopy(value)) or True
    queries = []
    def query(sql, **kwargs):
        queries.append(sql)
        return [("10", "2026-10-26 00:00:00", "2026-10-26 00:00:00", 0, 0, 0, None, 0)]
    new["_trino_rows"] = query
    assert "1/1 = 100.0%" in new["fotmob_daily_criterion"]()[0]
    assert queries == [] and saves == []
    assert state == {"days": {"2026-10-27": final}}
    state["days"]["2026-10-26"] = {"due": 1, "ok": 0, "pct": 0.0, "pending": 1, "streak": 0}
    new["fotmob_daily_criterion"]()
    assert len(queries) == 1 and "deadline_day=DATE '2026-10-26'" in queries[0]
    assert len(saves) == 1
    assert saves[0]["days"]["2026-10-26"]["ok"] == 1
    assert "pending" not in saves[0]["days"]["2026-10-26"]
    assert saves[0]["days"]["2026-10-27"]["streak"] == 2
    state = saves[0]
    new["fotmob_daily_criterion"]()
    assert len(queries) == 1 and len(saves) == 1
