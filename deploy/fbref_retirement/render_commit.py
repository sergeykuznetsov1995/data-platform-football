"""Render, never execute, a transaction after separate exact owner approval.

Approval must name the queue manifest, this renderer and a <=1h maintenance
window. SQL repeats window and <=5m snapshot-age guards under metadata locks.
Outside-runtime locks, identity checks and delivery remain operator obligations.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from deploy.fbref_retirement import prepare as p

ACTION = "close-exact-never-started-fbref-silver-runs"


def timestamp(value):
    p.require(isinstance(value, str), "timestamp must be text")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    p.require(result.tzinfo is not None, "timestamp needs timezone")
    return result.astimezone(timezone.utc)


def render(bundle, approval, now=None):
    now = now or datetime.now(timezone.utc)
    bundle = Path(bundle)
    raw = p.read_regular(bundle / "manifest.json")
    manifest = json.loads(raw)
    p.require(manifest["kind"] == "fbref-unstarted-silver-rehearsal" and manifest["schema"] == 1, "wrong bundle kind")
    snapshot_raw = p.read_regular(bundle / "snapshot.json")
    p.require(p.digest(snapshot_raw) == manifest["snapshot_sha256"], "snapshot hash drift")
    p.require(manifest["tool_sha256"] == p.digest(p.read_regular(Path(p.__file__))), "preparation tool drift")
    p.require(set(approval) == {"action", "manifest_sha256", "renderer_sha256", "window_start", "window_end", "owner_evidence"}, "unexpected approval fields")
    p.require(approval["action"] == ACTION and approval["manifest_sha256"] == p.digest(raw), "exact action/manifest approval required")
    p.require(approval["renderer_sha256"] == p.digest(p.read_regular(Path(__file__))), "renderer approval drift")
    p.require(isinstance(approval["owner_evidence"], str) and bool(approval["owner_evidence"].strip()), "missing owner approval evidence")
    start, end = timestamp(approval["window_start"]), timestamp(approval["window_end"])
    p.require(0 < (end-start).total_seconds() <= 3600 and now < end, "invalid/expired <=1h window")
    snapshot = p.validate_queue(json.loads(snapshot_raw))
    checked = timestamp(snapshot["checked_at"])
    p.require(-1 <= (now-checked).total_seconds() <= 300, "snapshot must be fresh within five minutes")
    p.require(sorted(r["id"] for r in snapshot["runs"]) == manifest["run_ids"] and
              len(snapshot["runs"]) == manifest["run_count"] and len(snapshot["tasks"]) == manifest["task_count"], "manifest counts/ids drift")
    # ISO values come from datetime, never interpolated raw user input.
    gate = f"""DO $window$
BEGIN
 IF clock_timestamp() < '{start.isoformat()}'::timestamptz
 OR clock_timestamp() >= '{end.isoformat()}'::timestamptz
 OR clock_timestamp() < '{checked.isoformat()}'::timestamptz - interval '1 second'
 OR clock_timestamp() > '{checked.isoformat()}'::timestamptz + interval '5 minutes'
 THEN RAISE EXCEPTION 'outside approved window or stale snapshot'; END IF;
END $window$;
"""
    sql = p.cancellation_sql(snapshot, p.digest(raw))
    sql = sql.replace("-- Preparation/rehearsal only. No production executor or COMMIT is supplied.",
                      "-- Approved SQL rendering only; external runtime/driver/lock checks still required.")
    sql = sql.replace("CREATE TEMP TABLE fbref_expected", gate + "CREATE TEMP TABLE fbref_expected", 1)
    p.require(sql.endswith("ROLLBACK;\n"), "unexpected transaction terminator")
    return sql[:-len("ROLLBACK;\n")] + gate + "COMMIT;\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--approval", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    sql = render(args.bundle, json.loads(p.read_regular(args.approval)))
    output = p.output_directory(args.output)
    (output / "approved.sql").write_text(sql)
    (output / "approval.json").write_bytes(p.read_regular(args.approval))
    print(json.dumps({"sql_sha256": p.digest(sql.encode()), "executed": False}))


if __name__ == "__main__":
    main()
