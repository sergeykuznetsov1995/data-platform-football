"""One bounded history slot; canonical scope subprocesses retain their identities."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from scrapers.sofascore import history_controller as controller
from dags.utils import sofascore_all_mens_state as state


def door_open(dag_id, pool):
    """Read-only admission. Missing/failed metadata is never an open door."""
    from airflow.models import DagModel, Pool
    from airflow.utils.session import create_session
    with create_session() as session:
        dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
        lane = session.query(Pool).filter(Pool.pool == pool).one_or_none()
        return dag is not None and not dag.is_paused and lane is not None and lane.slots > 0


def scope_command(env):
    return [sys.executable, "dags/scripts/run_sofascore_scope_cycle.py",
            "--snapshot", env["SOFASCORE_CAMPAIGN_SNAPSHOT"],
            "--tournament-id", env["SOFASCORE_TOURNAMENT_ID"],
            "--source-season-id", env["SOFASCORE_SOURCE_SEASON_ID"],
            "--expected-snapshot-id", env["SOFASCORE_EXPECTED_SNAPSHOT_ID"],
            "--expected-campaign-id", env["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
            "--phase", env.get("SOFASCORE_HISTORY_PHASE", "all"),
            "--season-evidence", env.get("SOFASCORE_HISTORY_SEASON_EVIDENCE", "pages"),
            "--output-dir", env["SOFASCORE_SCOPE_OUTPUT_DIR"],
            "--output", env["SOFASCORE_SCOPE_RESULT_PATH"],
            "--workload-artifact", env["SOFASCORE_WORKLOAD_ARTIFACT"],
            "--run-id", env["SOFASCORE_SCOPE_RUN_ID"]]


def _enable_subreaper():
    """The dedicated worker owns orphaned descendants before they detach."""
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER (Linux)
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


def _process_identity(pid):
    """Linux identity includes start ticks: a recycled PID is never signalled."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return int(fields[1]), fields[19]
    except (OSError, ValueError, IndexError):
        return None


def _kill_owned_tree(process, identity):
    owned = {process.pid: identity} if identity is not None else {}
    # Only a dedicated slot process calls execute_scope. Its children include
    # adopted orphans from this one canonical scope, never another slot.
    anchor = os.getpid()
    # Freeze parents before discovering descendants, including new sessions.
    # All signals require the same start ticks observed under this live child.
    while True:
        for pid, observed in owned.items():
            current = _process_identity(pid)
            if current and current[1] == observed[1]:
                try:
                    os.kill(pid, signal.SIGSTOP)
                except ProcessLookupError:
                    pass
        discovered = {}
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                pid = int(entry.name)
                observed = _process_identity(pid)
                if observed and pid not in owned:
                    parent = observed[0]
                    parent_now = _process_identity(parent) if parent in owned else None
                    parent_owned = (parent_now is not None and parent_now[1] == owned[parent][1])
                    if parent == anchor or parent_owned:
                        discovered[pid] = observed
        if not discovered:
            break
        owned.update(discovered)
    for pid, observed in reversed(list(owned.items())):
        current = _process_identity(pid)
        if current and current[1] == observed[1]:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    return owned


def execute_scope(env, timeout):
    _enable_subreaper()
    process = subprocess.Popen(scope_command(env), env={**os.environ, **env}, start_new_session=True)
    identity = _process_identity(process.pid)
    try:
        return process.wait(timeout=timeout)
    finally:
        # An intermediary may already have exited: its children were adopted
        # by this worker, so ancestry loss does not lose process ownership.
        owned = _kill_owned_tree(process, identity)
        process.wait()
        for _ in range(2):
            for pid in owned:
                if pid != process.pid:
                    try:
                        os.waitpid(pid, 0)
                    except ChildProcessError:
                        pass


def scope_outcome(env, exit_code):
    path = Path(env["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(path.read_text())
        campaign, tournament, season = env["SOFASCORE_SCOPE_KEY"].split(":")
        valid = (result.get("campaign_id") == campaign
                 and int(result.get("tournament_id", 0)) == int(tournament)
                 and int(result.get("source_season_id", 0)) == int(season)
                 and result.get("run_id") == env["SOFASCORE_SCOPE_RUN_ID"])
        if not valid:
            raise ValueError("scope result provenance mismatch")
        if exit_code == 0 and result.get("status") == "success":
            return {"status": "success", "rejected_endpoints": state.read_scope_rejects(path)}
        reason, requests = state.read_scope_outcome(path)
        return {"status": "failed", "reason": reason or "scope cycle failed", "source_requests": requests}
    except (OSError, ValueError, TypeError) as exc:
        return {"status": "failed", "reason": f"scope validation: {exc}", "source_requests": None}


def stamp_result(env, item, outcome, run_id):
    path = Path(env["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(path.read_text())
    except (OSError, ValueError):
        result = {"run_id": env["SOFASCORE_SCOPE_RUN_ID"],
                  "campaign_id": env["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
                  "tournament_id": int(env["SOFASCORE_TOURNAMENT_ID"]),
                  "source_season_id": int(env["SOFASCORE_SOURCE_SEASON_ID"]),
                  "scope_digest": env["SOFASCORE_SCOPE_KEY"], "phases": [],
                  "status": outcome["status"], "errors": [outcome.get("reason", "missing result")]}
    result["history_slot"] = {"slot": int(item["slot"]), "dag_run_id": run_id,
                              "started_at": item["started_at"], "finished_at": item["finished_at"],
                              "terminal_state": outcome["status"]}
    result["history_scope_attempt"] = outcome["status"] != "not_started"
    state._write_document_atomically(path, result)


def run_slot(*, checkpoint, campaign_id, run_id, slot, state_path, failures_path,
             admitted, execute=execute_scope, clock=lambda: datetime.now(timezone.utc),
             sleep=time.sleep, release=None):
    """Validate/account before releasing a slot; independent failures continue FIFO."""
    release = state.current_release() if release is None else release
    failed = False
    terminated = False
    while True:
        report = controller.read_summary(checkpoint, campaign_id)
        run = report["run"]
        active = run.get("slots", {}).get(str(slot))
        if active is None:
            if not admitted() or clock() >= datetime.fromisoformat(run["deadline"]) - timedelta(minutes=10):
                break
        claimed = controller.claim_scope(checkpoint, campaign_id=campaign_id, run_id=run_id, slot=slot, now=clock())
        if claimed is None:
            break
        index, env = claimed
        item = controller.read_summary(checkpoint, campaign_id)["run"]["items"][str(index)]
        outcome = item["outcome"]
        if outcome is None:
            if item["attempts"] > 0 and Path(env["SOFASCORE_SCOPE_RESULT_PATH"]).is_file():
                saved = scope_outcome(env, 0)
                outcome = saved
            while outcome is None or outcome["status"] == "failed":
                if not admitted():
                    outcome = outcome or {"status": "not_started" if item["attempts"] == 0 else "failed",
                                          "reason": "history admission closed before capture",
                                          "source_requests": 0 if item["attempts"] == 0 else None}
                    break
                if item["attempts"] >= 2:
                    outcome = outcome or {"status": "failed", "reason": "history retry budget exhausted after interruption",
                                          "source_requests": None}
                    break
                timeout = min(4 * 3600, (datetime.fromisoformat(run["deadline"]) - clock()).total_seconds() - 600)
                if timeout <= 0:
                    outcome = {"status": "not_started" if item["attempts"] == 0 else "failed", "reason": "history deadline reached",
                               "source_requests": 0 if item["attempts"] == 0 else None}
                    break
                controller.record_scope(checkpoint, campaign_id=campaign_id, run_id=run_id,
                                        slot=slot, index=index, started_attempt=True)
                Path(env["SOFASCORE_SCOPE_RESULT_PATH"]).unlink(missing_ok=True)
                try:
                    code = execute(env, timeout)
                    outcome = scope_outcome(env, code)
                except (Exception, KeyboardInterrupt) as exc:
                    terminated = isinstance(exc, KeyboardInterrupt)
                    outcome = {"status": "failed", "reason": f"scope process: {type(exc).__name__}: {exc}", "source_requests": None}
                item = controller.read_summary(checkpoint, campaign_id)["run"]["items"][str(index)]
                if terminated or outcome["status"] == "success" or item["attempts"] >= 2 or not admitted() or timeout <= 120:
                    break
                try:
                    sleep(120)
                except KeyboardInterrupt:
                    terminated = True
                    break
            controller.record_scope(checkpoint, campaign_id=campaign_id, run_id=run_id,
                                    slot=slot, index=index, outcome=outcome)
        item = controller.read_summary(checkpoint, campaign_id)["run"]["items"][str(index)]
        stamp_result(env, item, outcome, run_id)
        controller.account_scope(env, outcome, state_path=state_path, failures_path=failures_path, release=release)
        controller.record_scope(checkpoint, campaign_id=campaign_id, run_id=run_id,
                                slot=slot, index=index, accounted=True)
        failed |= outcome["status"] == "failed"
        if terminated:
            break
    # A retry of a whole worker remembers failures from earlier scopes too.
    items = controller.read_summary(checkpoint, campaign_id)["run"]["items"].values()
    failed |= any(i["slot"] == str(slot) and i["outcome"] and i["outcome"]["status"] == "failed" for i in items)
    return 1 if failed else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--slot", required=True, type=int)
    parser.add_argument("--state", required=True)
    parser.add_argument("--failures", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--pool", required=True)
    args = parser.parse_args(argv)
    def stopped(signum, frame):
        raise KeyboardInterrupt("history worker termination")
    signal.signal(signal.SIGTERM, stopped)
    return run_slot(checkpoint=args.checkpoint, campaign_id=args.campaign_id, run_id=args.run_id,
                    slot=args.slot, state_path=args.state, failures_path=args.failures,
                    admitted=lambda: door_open(args.dag_id, args.pool))


if __name__ == "__main__":
    raise SystemExit(main())
