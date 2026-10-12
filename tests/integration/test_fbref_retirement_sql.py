"""Real PostgreSQL rehearsal in an explicitly labelled, networkless test DB.

Never accepts a DSN or existing production container. Run with the ID of an
owned disposable container via FBREF_RETIREMENT_TEST_CONTAINER.
"""
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deploy.fbref_retirement import prepare as p
from deploy.fbref_retirement import render_commit as renderer


@pytest.fixture
def db():
    cid = os.environ.get("FBREF_RETIREMENT_TEST_CONTAINER")
    if not cid:
        pytest.skip("needs owned isolated PostgreSQL fixture")
    inspected = json.loads(subprocess.check_output(["docker", "inspect", cid]))[0]
    assert inspected["Id"] == cid and inspected["HostConfig"]["NetworkMode"] == "none"
    assert inspected["Config"]["Labels"].get("codex.task") == "1465-fbref-retirement-test"
    assert inspected["Mounts"] == [] and not inspected["HostConfig"]["PortBindings"]

    def run(sql, ok=True):
        result = subprocess.run(["docker", "exec", "-i", cid, "psql", "-X", "-qAt", "-U", "test", "-d", "test", "-v", "ON_ERROR_STOP=1"],
                                input=sql, text=True, capture_output=True, timeout=30)
        assert (result.returncode == 0) == ok, result.stdout + result.stderr
        return result.stdout + result.stderr
    return run


def seed(db):
    db("""
DROP TABLE IF EXISTS task_instance,dag_run,dag,job;
CREATE TABLE dag(dag_id text PRIMARY KEY,is_active boolean,is_paused boolean);
CREATE TABLE dag_run(id integer PRIMARY KEY,dag_id text,run_id text,state text,start_date timestamptz,end_date timestamptz,clear_number integer DEFAULT 0,creating_job_id integer);
CREATE TABLE task_instance(dag_id text,run_id text,task_id text,map_index integer,state text,job_id integer,start_date timestamptz,end_date timestamptz,try_number integer,queued_dttm timestamptz,queued_by_job_id integer,external_executor_id text,trigger_id integer,trigger_timeout timestamptz,next_method text,next_kwargs jsonb,pid integer,duration double precision);
CREATE TABLE job(id integer PRIMARY KEY,job_type text,state text,start_date timestamptz,end_date timestamptz,latest_heartbeat timestamptz);
""")
    dags = p.PRODUCERS + (p.SILVER,) + p.HOLD
    for dag in dags:
        db(f"INSERT INTO dag VALUES ('{dag}',true,true);")
    parents = [{"id": 10+i, "dag_id": producer, "run_id": f"source{i}", "state": "success", "start_date": None, "end_date": None, "clear_number": 0, "creating_job_id": None}
               for i, producer in enumerate(p.PRODUCERS[:2])]
    runs = [{"id": i+1, "dag_id": p.SILVER, "run_id": f"fbref_silver__{parent['dag_id']}__{parent['run_id']}",
             "state": "queued", "start_date": None, "end_date": None, "clear_number": 0, "creating_job_id": None} for i, parent in enumerate(parents)]
    tasks = [{"dag_id": p.SILVER, "run_id": run["run_id"], "task_id": name, "map_index": -1,
              **{k: None for k in p.TASK_UNSET}, "try_number": 0}
             for run in runs for name in ("normal", "quoted'; SELECT 'as data")]
    for table, rows in (("dag_run", parents+runs), ("task_instance", tasks)):
        db(f"INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, {p.sql_literal(rows)});")
    db(f"INSERT INTO dag_run VALUES(99,'{p.SILVER}','old','failed','2026-10-01','2026-10-02',0,NULL);")
    return {"checked_at": "2026-10-12T12:00:00Z", "runs": runs, "tasks": tasks, "parents": parents}


def fingerprints(db):
    return db("""SELECT jsonb_build_object(
 'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM dag_run r),
 'tasks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY run_id,task_id) FROM task_instance t),
 'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM job j),
 'dags',(SELECT jsonb_agg(to_jsonb(d) ORDER BY dag_id) FROM dag d));""")


def test_changes_exact_children_then_rolls_back_everything(db):
    snapshot = seed(db); before = fingerprints(db)
    out = db(p.cancellation_sql(snapshot, "a"*64))
    assert out.count("|failed|") == 2 and "|99|" not in out
    assert fingerprints(db) == before


@pytest.mark.parametrize("mutation,reason", [
    ("UPDATE dag SET is_paused=false WHERE dag_id='dag_ingest_fbref';", "must be paused"),
    ("INSERT INTO dag VALUES ('foreign',true,false);", "must be paused"),
    ("UPDATE dag_run SET state='running' WHERE id=1;", "unfinished DagRun"),
    ("UPDATE dag_run SET start_date=now() WHERE id=1;", "before-image drift"),
    ("UPDATE dag_run SET end_date=now() WHERE id=1;", "before-image drift"),
    ("UPDATE dag_run SET state='running' WHERE id=10;", "unfinished DagRun"),
    ("UPDATE task_instance SET state='running' WHERE task_id='normal';", "tasks before-image drift"),
    ("UPDATE task_instance SET job_id=7 WHERE task_id='normal';", "tasks before-image drift"),
    ("DELETE FROM task_instance WHERE task_id='normal';", "tasks before-image drift"),
    ("INSERT INTO dag_run VALUES (3,'dag_transform_fbref_silver','extra','queued',NULL,NULL,0,NULL);", "unfinished DagRun"),
    ("INSERT INTO dag_run VALUES (88,'other','foreign',NULL,NULL,NULL,0,NULL);", "unfinished DagRun"),
    ("INSERT INTO job VALUES(7,'LocalTaskJob','running',now(),NULL,now());", "LocalTaskJob"),
    ("INSERT INTO job VALUES(7,'LocalTaskJob','success',now()-interval '1 hour',now(),now());", "LocalTaskJob"),
    ("INSERT INTO task_instance(dag_id,run_id,task_id,map_index) VALUES ('other','orphan','a',-1);", "unknown task"),
])
def test_drift_refuses_all_changes(db, mutation, reason):
    snapshot = seed(db); db(mutation); before = fingerprints(db)
    out = db(p.cancellation_sql(snapshot, "b"*64), ok=False)
    assert reason in out
    assert fingerprints(db) == before


def test_repeat_after_terminal_state_refuses(db):
    snapshot = seed(db); db("UPDATE dag_run SET state='failed',end_date=now() WHERE id IN (1,2);")
    before = fingerprints(db)
    assert "before-image drift" in db(p.cancellation_sql(snapshot, "c"*64), ok=False)
    assert fingerprints(db) == before


def test_old_successful_job_and_terminal_history_are_preserved(db):
    snapshot = seed(db)
    db("INSERT INTO job VALUES(7,'LocalTaskJob','success',now()-interval '1 day',now()-interval '1 hour',now()-interval '1 hour');")
    before = fingerprints(db)
    assert db(p.cancellation_sql(snapshot, "d"*64)).count("|failed|") == 2
    assert fingerprints(db) == before


def approved_sql(snapshot, tmp_path, *, now=None):
    now = now or datetime.now(timezone.utc)
    snapshot["checked_at"] = now.isoformat()
    raw = p.canonical(snapshot); (tmp_path / "snapshot.json").write_bytes(raw)
    manifest = {"schema": 1, "kind": "fbref-unstarted-silver-rehearsal", "snapshot_sha256": p.digest(raw),
                "tool_sha256": p.digest(Path(p.__file__).read_bytes()), "run_ids": [1, 2], "run_count": 2, "task_count": 4}
    raw = p.canonical(manifest); (tmp_path / "manifest.json").write_bytes(raw)
    approval = {"action": renderer.ACTION, "manifest_sha256": p.digest(raw),
                "renderer_sha256": p.digest(Path(renderer.__file__).read_bytes()),
                "window_start": (now-timedelta(seconds=30)).isoformat(), "window_end": (now+timedelta(minutes=5)).isoformat(),
                "owner_evidence": "synthetic test authorization on disposable fixture only"}
    return renderer.render(tmp_path, approval, now=now)


def test_approved_commit_changes_only_state_and_end_of_exact_children(db, tmp_path):
    snapshot = seed(db); before = json.loads(fingerprints(db))
    db(approved_sql(snapshot, tmp_path))
    after = json.loads(fingerprints(db))
    assert after["tasks"] == before["tasks"] and after["jobs"] == before["jobs"] and after["dags"] == before["dags"]
    for old, new in zip(before["runs"], after["runs"]):
        if old["id"] in (1, 2):
            assert new["state"] == "failed" and new["end_date"] is not None
            assert {k:v for k,v in new.items() if k not in ("state","end_date")} == {k:v for k,v in old.items() if k not in ("state","end_date")}
        else:
            assert old == new


def test_postgres_itself_rejects_expired_approval(db, tmp_path):
    snapshot = seed(db); before = fingerprints(db)
    sql = approved_sql(snapshot, tmp_path, now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert "outside approved window" in db(sql, ok=False)
    assert fingerprints(db) == before


def test_database_errors_abort_approved_commit_before_any_change(db, tmp_path):
    snapshot = seed(db)
    db("CREATE FUNCTION reject_child_change() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic failure'; END$$;")
    db("CREATE TRIGGER reject_change BEFORE UPDATE ON dag_run FOR EACH ROW EXECUTE FUNCTION reject_child_change();")
    before = fingerprints(db)
    try:
        assert "synthetic failure" in db(approved_sql(snapshot, tmp_path), ok=False)
        assert fingerprints(db) == before
    finally:
        db("DROP FUNCTION reject_child_change() CASCADE;")


@pytest.mark.parametrize("section,key,value,mutation", [
    ("runs", "clear_number", 1, "UPDATE dag_run SET clear_number=1 WHERE id=1;"),
    ("runs", "creating_job_id", 7, "UPDATE dag_run SET creating_job_id=7 WHERE id=1;"),
    ("tasks", "try_number", 4, "UPDATE task_instance SET try_number=4 WHERE task_id='normal';"),
    ("tasks", "queued_by_job_id", 7, "UPDATE task_instance SET queued_by_job_id=7 WHERE task_id='normal';"),
    ("tasks", "external_executor_id", "submitted", "UPDATE task_instance SET external_executor_id='submitted' WHERE task_id='normal';"),
    ("tasks", "trigger_id", 7, "UPDATE task_instance SET trigger_id=7 WHERE task_id='normal';"),
])
def test_sql_rejects_execution_history_even_with_matching_before_images(db, section, key, value, mutation):
    snapshot = seed(db)
    sql = p.cancellation_sql(snapshot, "e"*64)
    bad = json.loads(json.dumps(snapshot))
    for row in bad[section]:
        if (section == "runs" and row["id"] == 1) or (section == "tasks" and row["task_id"] == "normal"):
            row[key] = value
    with pytest.raises(ValueError):
        p.validate_queue(bad)
    # Isolate the SQL defense as well: synthesize matching unsafe before rows.
    sql = sql.replace(p.sql_literal(snapshot), p.sql_literal(bad))
    db(mutation); before = fingerprints(db)
    assert "started Silver work" in db(sql, ok=False)
    assert fingerprints(db) == before
