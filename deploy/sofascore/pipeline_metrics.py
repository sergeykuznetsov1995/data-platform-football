"""Read-only host adapters for #1361. No Airflow import or source requests."""
from __future__ import annotations

import json
import importlib.util
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

def _metric_module(name):
    # dags.utils.__init__ imports Airflow configuration. These two standalone
    # stdlib modules are designed for direct host loading instead.
    path = Path(__file__).resolve().parents[2] / "dags/utils" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pool_wait = _metric_module("sofascore_pool_wait")
red_share = _metric_module("sofascore_red_share")

LANES = {"history": red_share.HISTORY_DAG_ID, "refresh": red_share.REFRESH_DAG_ID}
POOLS = {"history": pool_wait.HISTORY_POOL, "refresh": pool_wait.REFRESH_POOL}


def psql(sql: str) -> str:
    result = subprocess.run(
        ["docker", "exec", "sofascore-airflow-metadb", "psql", "-XqAt", "-U", "airflow", "-d", "airflow",
         "-c", "BEGIN READ ONLY; SET LOCAL statement_timeout = '30s'; " + sql + "; COMMIT;"],
        capture_output=True, text=True, timeout=40, check=False,
    )
    if result.returncode:
        raise RuntimeError("read-only metabase query failed")
    return result.stdout.strip()


def latest_daily(path: Path, day: str):
    selected = None
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            if row["day"] == day:
                selected = row
    return selected


def report_metrics(runtime: Path, now: datetime, day: str, *, result_dir="results") -> dict:
    """Same matches_complete metric as the history report, once per run/scope.

    It measures closed matches in successful publications, NOT unique new
    game_ids across the whole campaign. Never use capture_status_rows (which
    includes incomplete records), or paid-ledger byte chunks as requests.
    End-of-phase timestamps are conservative upper bounds on actual activity.
    """
    d0 = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    d1 = d0 + timedelta(days=1)
    records = {}
    directory = runtime / result_dir
    if not directory.is_dir():
        raise ValueError("history results directory missing")
    for path in directory.glob("*.json"):
        observed = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        if observed > now:
            continue
        payload = json.loads(path.read_text())
        # Reports from other lanes or unidentified legacy reports cannot
        # establish lane activity. run_id + scope_digest survive retries.
        if not payload.get("run_id") or not payload.get("scope_digest"):
            continue
        phase_path = path.with_suffix("") / "matches.json"
        matches = json.loads(phase_path.read_text()) if phase_path.exists() else {}
        traffic = matches.get("traffic", {})
        requests = traffic.get("request_count")
        if requests is None:
            counts = [phase.get("request_count") for phase in payload.get("phases", [])]
            requests = sum(counts) if counts and all(isinstance(n, int) and n >= 0 for n in counts) else None
        closed = matches.get("matches_complete") if payload.get("status") == "success" else 0
        for value in (closed, requests):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError("invalid report count")
        identity = (payload["run_id"], payload["scope_digest"])
        previous = records.get(identity)
        if previous is None or observed > previous[0]:
            records[identity] = (observed, closed, requests)
    result = {}
    usable = [row for row in records.values() if row[1] is not None]
    if usable:
        result["daily_closed"] = sum(closed for at, closed, _ in usable if d0 <= at < d1)
        times = [at for at, closed, _ in usable if closed > 0]
        result["last_progress"] = max(times).isoformat() if times else None
    paid = [row for row in records.values() if row[2] is not None]
    if paid:
        times = [at for at, _, requests in paid if requests > 0]
        result["last_paid"] = max(times).isoformat() if times else None
    return result


def history_queue(runtime: Path) -> int:
    """Remaining legacy wave-1 queue, including parked work (not a ready plan)."""
    snapshot = json.loads((runtime / "snapshot.json").read_text())
    completed = set(json.loads((runtime / "state.json").read_text())["completed"])
    campaign = snapshot["campaign_id"]
    remaining = set()
    for tournament in snapshot["tournaments"]:
        if tournament.get("metadata_status") == "excluded":
            continue
        for season in tournament.get("seasons", []):
            if season.get("metadata_status") == "excluded" or not 0 <= int(season["start_year"]) <= 2025:
                continue
            key = f"{campaign}:{int(tournament['unique_tournament_id'])}:{int(season['source_season_id'])}"
            if key not in completed:
                remaining.add(key)
    return len(remaining)


def lane_sql(lane: str) -> str:
    dag, pool = LANES[lane], POOLS[lane]
    return (
        "SELECT json_build_object("
        "'closed', (SELECT is_paused FROM dag WHERE dag_id = '" + dag + "') OR "
        "(SELECT slots = 0 FROM slot_pool WHERE pool = '" + pool + "'), "
        "'demand_since', (SELECT min(coalesce(t.start_date, d.start_date, t.queued_dttm, d.execution_date)) "
        "FROM task_instance t JOIN dag_run d ON d.dag_id=t.dag_id AND d.run_id=t.run_id "
        "WHERE t.dag_id='" + dag + "' AND d.state IN ('running','queued') "
        "AND (t.task_id LIKE 'run\\_%\\_scope' ESCAPE '\\' OR t.task_id IN "
        "('plan_historical_batch','plan_refresh_batch','refresh_season_schedules','refill_refresh_window')) "
        "AND (t.state IS NULL OR t.state IN ('scheduled','queued','running','up_for_retry'))), "
        "'last_run_end', (SELECT max(end_date) FROM dag_run WHERE dag_id='" + dag + "'), "
        "'last_run_start', (SELECT max(start_date) FROM dag_run WHERE dag_id='" + dag + "'))"
    )


def collect(runtime: Path, coverage_daily: Path, now: datetime, query=psql) -> dict:
    """Collect independent evidence; failed adapters remain explicitly unknown."""
    day = (now.date() - timedelta(days=1)).isoformat()
    t0, t1 = day + "T00:00:00Z", now.date().isoformat() + "T00:00:00Z"
    result = {"observed_at": now.isoformat(), "day": day, "lanes": {},
              "evidence": [str(runtime), str(coverage_daily)], "collection_errors": []}

    def attempt(name, reader):
        try:
            return reader()
        except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired):
            result["collection_errors"].append(name)
            return None

    result["coverage"] = attempt("coverage", lambda: latest_daily(coverage_daily, day))
    result["red_share"] = attempt("red_share", lambda: red_share.parse_rows(query(red_share.red_share_sql(t0, t1))))
    wait = attempt("pool_wait", lambda: pool_wait.parse_rows(
        query(pool_wait.pool_wait_sql(t0, t1)), query(pool_wait.lane_overlap_sql(t0, t1))))
    result["pool_wait"] = wait._asdict() if wait else None
    queue = attempt("history_queue", lambda: history_queue(runtime))
    metrics = attempt("history_reports", lambda: report_metrics(runtime, now, day))
    refresh_metrics = attempt("refresh_reports", lambda: report_metrics(runtime, now, day, result_dir="refresh-results"))
    for lane in LANES:
        sample = attempt(lane + "_door", lambda: json.loads(query(lane_sql(lane))))
        if sample is None:
            continue
        if lane == "history":
            sample.update(metrics or {})
            # A nonempty queue is demand even if no DagRun is alive. Anchor to
            # the last completed run; the first observation is the fallback.
            sample["expected_work"] = queue > 0 if queue is not None else None
            if queue:
                sample["demand_since"] = sample["demand_since"] or sample["last_run_end"] or now.isoformat()
        else:
            # A paused scheduler creates no scope TIs. Detect a missed launch
            # against the actual refresh DAG's three scheduled UTC times.
            candidates = [now.replace(hour=h, minute=30, second=0, microsecond=0) for h in (0, 8, 15)]
            candidates += [at - timedelta(days=1) for at in candidates]
            due = max(at for at in candidates if at <= now - timedelta(minutes=15))
            last_start = datetime.fromisoformat(sample["last_run_start"]) if sample["last_run_start"] else None
            missed = last_start is None or last_start < due
            sample["expected_work"] = sample["demand_since"] is not None or missed
            if missed:
                sample["demand_since"] = sample["demand_since"] or due.isoformat()
            sample.update(refresh_metrics or {})
        marker = runtime.parent / "auto-deliver/sofascore-inflight"
        if marker.exists():
            sample["delivery_since"] = datetime.fromtimestamp(marker.stat().st_mtime, timezone.utc).isoformat()
        result["lanes"][lane] = sample
    return result
