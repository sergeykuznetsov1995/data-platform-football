"""Offline preparation only: source patches and guarded Silver cancellation SQL.

No database connection, production writer, pause, trigger or service operation.
The emitted transaction always rolls back. Production commit needs a separately
reviewed executor and exact manifest/window approval; do not edit SQL to commit.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

SILVER = "dag_transform_fbref_silver"
PRODUCERS = ("dag_ingest_fbref", "dag_backfill_fbref", "dag_replay_fbref")
HOLD = ("dag_master_pipeline", "dag_sofascore_pipeline", "dag_transform_xref",
        "dag_transform_e3", "dag_transform_e4")
BEFORE = {
    "dags/utils/fbref_current_dag_factory.py": "e6daffedc7b9546bb39e21dceb69f561d40cfe1afb1dfd8983d632ab0a8945a5",
    "dags/dag_backfill_fbref.py": "08cf3e959a9343b0c3384dff67570e30dd0a7e777591d44eb31a1d454a8ec27c",
    "dags/dag_replay_fbref.py": "d960cd6d8b331ecd073b4dcaea878424108cdabf813bdb64fd196f74f1e20e54",
}
SUPPORT = {
    "dags/utils/fbref_pipeline_tasks.py": "c0b13356ef737602c748f651637ed4cb12f84c50556893b6d88335daeceda1c9",
}
PACKAGE = Path(__file__).resolve().parent
TASK_UNSET = ("state", "job_id", "start_date", "end_date", "queued_dttm",
              "queued_by_job_id", "external_executor_id", "trigger_id",
              "trigger_timeout", "next_method", "next_kwargs", "pid", "duration")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_regular(path):
    path = Path(path).absolute()
    require(path.resolve() == path and path.is_file(), f"not a canonical regular file: {path}")
    require(path.stat().st_nlink == 1, f"hardlinked file: {path}")
    return path.read_bytes()


def output_directory(output, source=None):
    output = Path(output).absolute()
    require(output.resolve() == output, "output path contains symlinks")
    # All preparation writes belong to Football, never to an operational root.
    allowed = Path("/root/data-platform-football/.local/tasks")
    require(output.is_relative_to(allowed), "output must be under Football .local/tasks")
    if source is not None:
        source = Path(source).resolve()
        require(not output.is_relative_to(source) and not source.is_relative_to(output), "source/output overlap")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    return output


def prepare_payload(source, output):
    """Copy bytes only. The source is never imported or used as command cwd."""
    source = Path(source).absolute()
    require(source.resolve() == source, "source root contains symlinks")
    originals = {name: read_regular(source / name) for name in BEFORE | SUPPORT}
    for name, expected in (BEFORE | SUPPORT).items():
        require(digest(originals[name]) == expected, f"source drift: {name}")
    patches = {name: read_regular(PACKAGE / "patches" / (Path(name).stem + ".patch")) for name in BEFORE}
    output = output_directory(output, source)
    with tempfile.TemporaryDirectory(dir=output) as tmp:
        staging = Path(tmp)
        for name in BEFORE:
            target = staging / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(originals[name])
        for patch in patches.values():
            subprocess.run(["git", "apply", "--check", "-"], input=patch, cwd=staging, check=True, capture_output=True)
            subprocess.run(["git", "apply", "-"], input=patch, cwd=staging, check=True, capture_output=True)
        entries = []
        for name in BEFORE:
            after = read_regular(staging / name)
            compile(after, name, "exec")
            for phase, data in (("before", originals[name]), ("after", after)):
                target = output / phase / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
            entries.append({"path": name, "before_sha256": BEFORE[name], "after_sha256": digest(after),
                            "patch_sha256": digest(patches[name])})
    manifest = {"schema": 1, "kind": "fbref-silver-source-candidate", "files": entries,
                "unchanged_support": SUPPORT, "execution_qualified": False,
                "production_execution_authorized": False,
                "tool_sha256": digest(read_regular(Path(__file__))),
                "requires": ["exact-image contract/topology/behavior rehearsal", "independent review",
                             "fresh runtime/mount/catalog closure", "owner exact manifest/window approval",
                             "separately reviewed guarded delivery executor"]}
    (output / "manifest.json").write_bytes(canonical(manifest))
    return manifest


def validate_queue(snapshot):
    require(set(snapshot) == {"checked_at", "runs", "tasks", "parents"}, "unexpected snapshot fields")
    require(isinstance(snapshot["checked_at"], str) and snapshot["checked_at"], "missing snapshot time")
    runs, tasks, parents = (snapshot[k] for k in ("runs", "tasks", "parents"))
    require(isinstance(runs, list) and 0 < len(runs) <= 100, "expected 1..100 Silver runs")
    require(isinstance(tasks, list) and isinstance(parents, list), "invalid snapshot arrays")
    ids, names, parent_keys = set(), set(), set()
    for row in runs:
        require(isinstance(row, dict), "invalid DagRun row")
        require(type(row.get("id")) is int and row["id"] > 0 and row["id"] not in ids, "duplicate/invalid run id")
        require(row.get("dag_id") == SILVER and row.get("state") == "queued", "not a queued Silver run")
        require(row.get("start_date") is None and row.get("end_date") is None, "Silver run already started/ended")
        require("start_date" in row and "end_date" in row, "missing run timestamps")
        require(type(row.get("clear_number")) is int and row["clear_number"] == 0, "run was cleared or lacks clear counter")
        require("creating_job_id" in row and row["creating_job_id"] is None, "run has creating job or lacks evidence")
        name = row.get("run_id")
        require(isinstance(name, str) and name not in names, "duplicate/invalid run name")
        pieces = name.split("__", 2)
        require(len(pieces) == 3 and pieces[0] == "fbref_silver" and pieces[1] in PRODUCERS and pieces[2], "unknown Silver producer")
        ids.add(row["id"]); names.add(name); parent_keys.add((pieces[1], pieces[2]))
    found_parents = set()
    for row in parents:
        require(isinstance(row, dict), "invalid parent")
        key = (row.get("dag_id"), row.get("run_id"))
        require(key in parent_keys and key not in found_parents and row.get("state") in {"success", "failed"}, "unknown/nonterminal parent")
        found_parents.add(key)
    require(found_parents == parent_keys, "missing parent evidence")
    seen = set(); counts = {name: 0 for name in names}
    for row in tasks:
        require(isinstance(row, dict) and row.get("dag_id") == SILVER and row.get("run_id") in names, "foreign task")
        require(isinstance(row.get("task_id"), str) and row["task_id"], "missing task id")
        require(type(row.get("map_index")) is int and row["map_index"] >= -1, "invalid map index")
        key = (row["run_id"], row["task_id"], row["map_index"])
        require(key not in seen, "duplicate task"); seen.add(key)
        require(type(row.get("try_number")) is int and row["try_number"] == 0, "task was attempted or lacks attempt counter")
        require(all(k in row and row[k] is None for k in TASK_UNSET), "task started, was queued or has execution evidence")
        counts[row["run_id"]] += 1
    require(all(counts.values()), "missing task evidence for a run")
    return snapshot


def sql_literal(value):
    return "'" + json.dumps(value, sort_keys=True, ensure_ascii=True).replace("'", "''") + "'::jsonb"


def cancellation_sql(snapshot, manifest_sha):
    """Exact before rows and exclusive metadata locks prevent races.

    Conservative job guard may reject old abandoned jobs; never waive it. A
    refusal calls for investigation, not changing states or bypassing guards.
    """
    validate_queue(snapshot)
    require(len(manifest_sha) == 64 and all(c in "0123456789abcdef" for c in manifest_sha), "invalid approval hash")
    required = list(PRODUCERS + (SILVER,) + HOLD)
    return f"""\\set ON_ERROR_STOP on
-- Preparation/rehearsal only. No production executor or COMMIT is supplied.
-- Exact manifest SHA256: {manifest_sha}
BEGIN;
SET LOCAL timezone = 'UTC';
SET LOCAL standard_conforming_strings = on;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1s';
LOCK TABLE dag, dag_run, task_instance, job IN SHARE ROW EXCLUSIVE MODE NOWAIT;
CREATE TEMP TABLE fbref_expected ON COMMIT DROP AS SELECT {sql_literal(snapshot)} AS evidence;
DO $guard$
DECLARE e jsonb; n integer;
BEGIN
 SELECT evidence INTO STRICT e FROM fbref_expected;
 IF EXISTS (SELECT 1 FROM dag WHERE is_active AND NOT is_paused)
 OR (SELECT count(*) FROM dag WHERE dag_id IN
     (SELECT jsonb_array_elements_text({sql_literal(required)})) AND is_active AND is_paused) <> {len(required)}
 THEN RAISE EXCEPTION 'all shared DAGs and required producers/holds must be paused'; END IF;
 IF EXISTS (SELECT 1 FROM dag_run WHERE state IS NULL OR
     (state NOT IN ('success','failed') AND NOT
      (dag_id='{SILVER}' AND state='queued' AND id IN
       (SELECT (r->>'id')::integer FROM jsonb_array_elements(e->'runs') r))))
 THEN RAISE EXCEPTION 'foreign or unknown unfinished DagRun'; END IF;
 IF (SELECT jsonb_agg(to_jsonb(dr) ORDER BY dr.id) FROM dag_run dr WHERE dr.dag_id='{SILVER}' AND dr.state='queued')
    IS DISTINCT FROM (SELECT jsonb_agg(r ORDER BY (r->>'id')::integer) FROM jsonb_array_elements(e->'runs') r)
 THEN RAISE EXCEPTION 'Silver queue before-image drift'; END IF;
 IF (SELECT jsonb_agg(to_jsonb(ti) ORDER BY ti.run_id,ti.task_id,ti.map_index) FROM task_instance ti
     WHERE ti.dag_id='{SILVER}' AND ti.run_id IN
     (SELECT r->>'run_id' FROM jsonb_array_elements(e->'runs') r))
    IS DISTINCT FROM (SELECT jsonb_agg(t ORDER BY t->>'run_id',t->>'task_id',(t->>'map_index')::integer)
     FROM jsonb_array_elements(e->'tasks') t)
 THEN RAISE EXCEPTION 'Silver tasks before-image drift'; END IF;
 IF EXISTS (SELECT 1 FROM jsonb_array_elements(e->'runs') r
      WHERE r->>'state'<>'queued' OR r->>'start_date' IS NOT NULL OR r->>'end_date' IS NOT NULL
      OR r->'clear_number' IS DISTINCT FROM '0'::jsonb OR r->'creating_job_id' IS DISTINCT FROM 'null'::jsonb)
 OR EXISTS (SELECT 1 FROM jsonb_array_elements(e->'tasks') t
      WHERE t->'try_number' IS DISTINCT FROM '0'::jsonb OR EXISTS
      (SELECT 1 FROM jsonb_array_elements_text({sql_literal(list(TASK_UNSET))}) field
       WHERE t->field IS DISTINCT FROM 'null'::jsonb))
 THEN RAISE EXCEPTION 'started Silver work'; END IF;
 IF EXISTS (SELECT 1 FROM jsonb_array_elements(e->'parents') p
      WHERE NOT EXISTS (SELECT 1 FROM dag_run dr WHERE to_jsonb(dr)=p AND dr.state IN ('success','failed')))
 THEN RAISE EXCEPTION 'parent before-image drift'; END IF;
 IF EXISTS (SELECT 1 FROM task_instance ti LEFT JOIN dag_run dr
     ON dr.dag_id=ti.dag_id AND dr.run_id=ti.run_id
     WHERE ti.state IN ('running','queued','restarting','deferred','scheduled','up_for_retry','up_for_reschedule')
       OR (ti.state IS NOT NULL AND ti.state NOT IN ('success','failed','skipped','upstream_failed','removed'))
       OR (ti.state IS NULL AND (ti.job_id IS NOT NULL OR dr.state IS NULL)) )
 THEN RAISE EXCEPTION 'executing, waiting or unknown task/job reference'; END IF;
 IF EXISTS (SELECT 1 FROM job WHERE job_type='LocalTaskJob' AND
     (state IS NULL OR state NOT IN ('success','failed') OR end_date IS NULL
      OR start_date IS NULL OR latest_heartbeat IS NULL
      OR start_date>latest_heartbeat+interval '1 second' OR start_date>end_date
      OR latest_heartbeat>end_date OR latest_heartbeat>clock_timestamp()-interval '5 minutes'
      OR end_date>clock_timestamp()-interval '5 minutes'))
 THEN RAISE EXCEPTION 'unfinished, unknown or recent LocalTaskJob'; END IF;
 UPDATE dag_run SET state='failed',end_date=clock_timestamp()
 WHERE dag_id='{SILVER}' AND state='queued' AND id IN
     (SELECT (r->>'id')::integer FROM jsonb_array_elements(e->'runs') r);
 GET DIAGNOSTICS n = ROW_COUNT;
 IF n <> jsonb_array_length(e->'runs') THEN RAISE EXCEPTION 'cancellation count drift'; END IF;
END $guard$;
SELECT '{manifest_sha}' AS manifest_sha256, id,dag_id,run_id,state,end_date
FROM dag_run WHERE id IN (SELECT (r->>'id')::integer FROM fbref_expected,
jsonb_array_elements(evidence->'runs') r) ORDER BY id;
ROLLBACK;
"""


def prepare_queue(snapshot_path, output):
    original = read_regular(snapshot_path)
    snapshot = validate_queue(json.loads(original))
    output = output_directory(output)
    manifest = {"schema": 1, "kind": "fbref-unstarted-silver-rehearsal", "snapshot_sha256": digest(original),
                "run_ids": sorted(row["id"] for row in snapshot["runs"]),
                "run_count": len(snapshot["runs"]), "task_count": len(snapshot["tasks"]),
                "tool_sha256": digest(read_regular(Path(__file__))),
                "production_execution_authorized": False, "execution_qualified": False,
                "transaction_end": "ROLLBACK", "snapshot_checked_at": snapshot["checked_at"]}
    data = canonical(manifest)
    (output / "snapshot.json").write_bytes(original)
    (output / "manifest.json").write_bytes(data)
    (output / "rehearsal.sql").write_text(cancellation_sql(snapshot, digest(data)))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    payload = sub.add_parser("payload"); payload.add_argument("--source", required=True); payload.add_argument("--output", required=True)
    queue = sub.add_parser("queue"); queue.add_argument("--snapshot", required=True); queue.add_argument("--output", required=True)
    args = parser.parse_args()
    manifest = prepare_payload(args.source, args.output) if args.command == "payload" else prepare_queue(args.snapshot, args.output)
    print(json.dumps({"manifest_sha256": digest(canonical(manifest)), "production_execution_authorized": False}))


if __name__ == "__main__":
    main()
