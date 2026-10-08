#!/usr/bin/env python3
"""SofaScore lane monitor (#1361). Offline by default; no source transport.

Snapshots are observations, not instructions. Replay never invokes a collector
or publisher. State and its lock must be private to this monitor instance.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Executable by absolute path on a host without installing the project.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.sofascore.pipeline_metrics import pool_wait, red_share  # noqa: E402

LANES = {"history": red_share.HISTORY_DAG_ID, "refresh": red_share.REFRESH_DAG_ID}
STALL_SECONDS = 6 * 3600
STOP_SECONDS = 15 * 60
SNAPSHOT_MAX_AGE = 30 * 60
COVERAGE_THRESHOLD = 99


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(timezone.utc)


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def number(value, *, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("expected a nonnegative number")
    if not math.isfinite(value) or value < 0 or (integer and int(value) != value):
        raise ValueError("expected a finite nonnegative number")
    return value


def pauses_for(pauses: list[dict], lane: str) -> list[tuple[datetime, datetime | None]]:
    intervals = []
    for pause in pauses:
        if pause["lane"] not in LANES and pause["lane"] != "daily":
            raise ValueError("invalid pause lane")
        if not pause.get("reason") or not pause.get("approval"):
            raise ValueError("pause needs reason and approval reference")
        start = timestamp(pause["start"])
        end = timestamp(pause["end"]) if pause.get("end") else None
        if end is None and pause.get("indefinite") is not True:
            raise ValueError("pause without end needs indefinite=true")
        if end is not None and end <= start:
            raise ValueError("pause end must follow start")
        if pause["lane"] == lane:
            intervals.append((start, end))
    return sorted(intervals, key=lambda interval: interval[0])


def paused_during(pauses, lane, start, end):
    """Only a fully covered measurement interval is explained by a pause."""
    cursor = start
    for p0, p1 in pauses_for(pauses, lane):
        if p0 > cursor:
            break
        if p1 is None:
            return True
        cursor = max(cursor, p1)
        if cursor >= end:
            return True
    return False


def evaluate(snapshot: dict, pauses: list[dict], now: datetime, state: dict) -> dict:
    """Return per-rule active/ok/suppressed/unobservable/deferred decisions.

    Unknown data never resolves an episode. Pauses change operational decisions,
    not the measured numbers. Daily windows are closed UTC days from 05:00 UTC.
    """
    rules, lines, missing = {}, [], []
    observed = timestamp(snapshot["observed_at"])
    if not 0 <= (now - observed).total_seconds() <= SNAPSHOT_MAX_AGE:
        raise ValueError("snapshot is stale or from the future")
    for lane in (*LANES, "daily"):
        pauses_for(pauses, lane)  # Validate the entire policy before changing state.

    def rule(key, verdict, detail):
        rules[key] = {"verdict": verdict, "detail": detail}
        if verdict == "unobservable":
            missing.append(key)

    day = snapshot["day"]
    d0 = timestamp(day + "T00:00:00Z")
    d1 = d0 + timedelta(days=1)
    due = d1 + timedelta(hours=5)
    daily_ready = now >= due and day == (now.date() - timedelta(days=1)).isoformat()
    deferred = now < due
    per_dag = snapshot.get("red_share")
    try:
        if not isinstance(per_dag, dict):
            raise ValueError("red share missing")
        if set(per_dag) - set(LANES.values()):
            raise ValueError("unexpected DAG in red share")
        for failed, total in per_dag.values():
            number(failed, integer=True)
            number(total, integer=True)
            if failed > total:
                raise ValueError("failed exceeds total")
        lines.append(red_share.format_line(day, per_dag))
    except (ValueError, TypeError):
        per_dag = None
    for lane, dag in LANES.items():
        key = f"{lane}:red_share"
        if not daily_ready:
            rule(key, "deferred" if deferred else "unobservable", "daily window not ready")
        elif per_dag is None or dag not in per_dag or per_dag[dag][1] == 0:
            rule(key, "unobservable", "no terminal scope TIs")
        else:
            failed, total = per_dag[dag]
            share = red_share.RedShare.of(failed, total)
            detail = f"{day} UTC: failed={failed}, total={total}, pct={share.pct:.2f}; threshold <20%"
            verdict = "active" if share.verdict == "red" else "ok"
            if verdict == "active" and paused_during(pauses, lane, d0, d1):
                verdict = "suppressed"
            rule(key, verdict, detail)

    coverage = snapshot.get("coverage")
    try:
        if coverage["day"] != day:
            raise ValueError("coverage day mismatch")
        complete = number(coverage["core_in24"], integer=True)
        total = number(coverage["core_total"], integer=True)
        if complete > total or total == 0:
            raise ValueError("empty or inconsistent denominator")
        pct = 100 * complete / total
        lines.append(coverage.get("line") or f"core <=24h: {complete}/{total}={pct:.2f}%; threshold >=99%")
        verdict = "active" if pct < COVERAGE_THRESHOLD else "ok"
        # The core denominator also includes daily. A history-only pause cannot
        # explain a coverage failure in either current lane.
        if verdict == "active" and all(paused_during(pauses, lane, d0, d1) for lane in ("refresh", "daily")):
            verdict = "suppressed"
        if not daily_ready:
            verdict = "deferred" if deferred else "unobservable"
        rule("refresh:coverage", verdict, f"{day} UTC: {complete}/{total}={pct:.2f}%; threshold >=99%")
    except (KeyError, TypeError, ValueError):
        rule("refresh:coverage", "deferred" if deferred else "unobservable", "coverage measurement unavailable")

    try:
        wait = pool_wait.PoolWait(**snapshot["pool_wait"])
        for field in wait:
            number(field)
        if wait.waiting_runs > wait.runs:
            raise ValueError("waiting runs exceeds runs")
        lines.append(pool_wait.format_line(wait))
        verdict = {"wait": "active", "ok": "ok", "no_data": "unobservable"}[wait.verdict]
        if verdict == "active" and paused_during(pauses, "refresh", d0, d1):
            verdict = "suppressed"
        if not daily_ready:
            verdict = "deferred" if deferred else "unobservable"
        rule("refresh:pool_wait", verdict, f"{day} UTC: " + pool_wait.format_line(wait))
    except (KeyError, TypeError, ValueError):
        rule("refresh:pool_wait", "deferred" if deferred else "unobservable", "pool measurement unavailable")

    timers = state.setdefault("timers", {})
    demands = state.setdefault("demands", {})
    baselines = state.setdefault("silence_baselines", {})
    for lane in LANES:
        sample = snapshot.get("lanes", {}).get(lane, {})
        intervals = pauses_for(pauses, lane)
        paused = any(p0 <= now and (p1 is None or now < p1) for p0, p1 in intervals)
        resume = max((p1 for _, p1 in intervals if p1 and p1 <= now), default=None)
        delivery = sample.get("delivery_since")
        delivering = delivery is not None and 0 <= (now - timestamp(delivery)).total_seconds() < 3 * 3600
        if paused or delivering:
            timers.pop(lane, None)
            baselines[lane] = stamp(now)
            for name in ("stop", "no_progress", "no_paid"):
                rule(f"{lane}:{name}", "suppressed", "approved pause" if paused else "fresh delivery")
            continue
        expected = sample.get("expected_work")
        if expected is False:
            timers.pop(lane, None)
            demands.pop(lane, None)
            baselines.pop(lane, None)
            for name in ("stop", "no_progress", "no_paid"):
                rule(f"{lane}:{name}", "ok", "no work due")
            continue
        if expected is not True or not sample.get("demand_since"):
            for name in ("stop", "no_progress", "no_paid"):
                rule(f"{lane}:{name}", "unobservable", "work demand unknown")
            continue
        demand = timestamp(sample["demand_since"])
        demand = min(demand, timestamp(demands.setdefault(lane, stamp(demand))))
        demand = max(demand, resume) if resume else demand
        if lane in baselines:
            demand = max(demand, timestamp(baselines[lane]))
        if demand > now:
            raise ValueError("work demand is in the future")
        closed = sample.get("closed")
        if closed is True:
            since = max(timestamp(timers.setdefault(lane, stamp(now))), demand)
            elapsed = (now - since).total_seconds()
            rule(f"{lane}:stop", "active" if elapsed >= STOP_SECONDS else "ok", f"unexplained closed lane for {elapsed:.0f}s; threshold >=900s")
        elif closed is False:
            timers.pop(lane, None)
            rule(f"{lane}:stop", "ok", "lane open")
        else:
            rule(f"{lane}:stop", "unobservable", "lane door unknown")
        for name, field in (("no_progress", "last_progress"), ("no_paid", "last_paid")):
            if name == "no_paid" and sample.get("source_work_expected") is False:
                rule(f"{lane}:{name}", "ok", "saved-raw work needs no paid requests")
                continue
            if field not in sample:
                rule(f"{lane}:{name}", "unobservable", f"{field} unavailable")
                continue
            baseline = max(demand, timestamp(sample[field])) if sample[field] else demand
            if baseline > now:
                raise ValueError(f"{field} is in the future")
            elapsed = (now - baseline).total_seconds()
            rule(f"{lane}:{name}", "active" if elapsed >= STALL_SECONDS else "ok", f"{elapsed:.0f}s with pending work; threshold >=21600s")
        if sample.get("daily_closed") is not None:
            lines.append(f"{lane}: closed matches in reports for {day} UTC: {number(sample['daily_closed'], integer=True)}")
    return {"observed_at": stamp(observed), "day": day, "rules": rules, "lines": lines, "unobservable": missing}


def atomic_state(path: Path, state: dict):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as stream:
        json.dump(state, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def step(snapshot, pauses, now, state, checkpoint=lambda: None):
    previous = state.get("last_observed_at")
    if previous and timestamp(snapshot["observed_at"]) < timestamp(previous):
        raise ValueError("out-of-order observation")
    result = evaluate(snapshot, pauses, now, state)
    episodes = state.setdefault("episodes", {})
    pending = state.setdefault("incidents", {})
    events = []
    for key, decision in result["rules"].items():
        if decision["verdict"] == "ok":
            episodes.pop(key, None)
        if decision["verdict"] != "active":
            continue
        if key not in episodes:
            state["sequence"] = state.get("sequence", 0) + 1
            token = hashlib.sha256(f"sofascore:1361:{key}:{stamp(now)}:{state['sequence']}".encode()).hexdigest()
            incident = {"marker": f"<!-- sofascore-1361:{token} -->", "rule": key,
                        "started_at": stamp(now), "day": now.date().isoformat(),
                        "detail": decision["detail"], "evidence": snapshot.get("evidence", []),
                        "issue": None, "project_added": False}
            # Persist identity BEFORE any external write: an ambiguous create
            # can be reconciled by exact marker after restart.
            pending[token] = incident
            episodes[key] = token
            events.append(token)
        pending[episodes[key]]["detail"] = decision["detail"]
    state["last_observed_at"] = result["observed_at"]
    checkpoint()
    result["new_incidents"] = events
    result["active_incidents"] = dict(episodes)
    return result


def publish(state, publisher, checkpoint):
    errors = []
    for token, incident in state.get("incidents", {}).items():
        try:
            if incident["issue"] is None:
                incident["issue"] = publisher.find(incident["marker"]) or publisher.create(incident)
                checkpoint()
            if not incident["project_added"]:
                publisher.add_project(incident["issue"])
                incident["project_added"] = True
                checkpoint()
        except (OSError, ValueError, RuntimeError) as exc:
            # An old event remains evidence even if the lane recovered before
            # GitHub recovered. Never discard pending incidents on recovery.
            errors.append(f"{token}: {type(exc).__name__}: {exc}")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--snapshot", type=Path)
    source.add_argument("--replay", type=Path)
    source.add_argument("--collect", action="store_true")
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--pauses", type=Path)
    parser.add_argument("--now")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--publish-issues", action="store_true")
    parser.add_argument("--runtime", type=Path, default=Path("/root/sofascore-runtime/all-men"))
    parser.add_argument("--coverage-daily", type=Path, default=Path("/root/watchdog/state/sofascore_veha1/daily.jsonl"))
    args = parser.parse_args(argv)
    if args.publish_issues and (args.replay or args.snapshot or args.now or args.dry_run):
        parser.error("publication requires a current --collect, without replay/snapshot/now/dry-run")
    if args.collect and not args.pauses:
        parser.error("--collect requires an explicit --pauses policy (use [] when none are approved)")
    pauses = json.loads(args.pauses.read_text()) if args.pauses else []
    # Never silently replace malformed or unknown-version durable state.
    state = json.loads(args.state.read_text()) if args.state.exists() else {"version": 1}
    if state.get("version") != 1:
        parser.error("unsupported state version")
    args.state.parent.mkdir(parents=True, exist_ok=True)
    with args.state.with_suffix(args.state.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Read after taking the lock; an overlapping invocation may have saved
        # state between argument parsing and acquiring the lock.
        state = json.loads(args.state.read_text()) if args.state.exists() else {"version": 1}
        if state.get("version") != 1:
            parser.error("unsupported state version")

        def checkpoint():
            atomic_state(args.state, state)
        if args.collect:
            from deploy.sofascore.pipeline_metrics import collect
            snapshots = [collect(args.runtime, args.coverage_daily, datetime.now(timezone.utc))]
        elif args.replay:
            snapshots = [json.loads(line) for line in args.replay.read_text().splitlines() if line.strip()]
        else:
            snapshots = [json.loads(args.snapshot.read_text())]
        errors = []
        for snapshot in snapshots:
            now = timestamp(args.now) if args.now else (timestamp(snapshot["observed_at"]) if args.replay else datetime.now(timezone.utc))
            result = step(snapshot, pauses, now, state, checkpoint)
            if args.publish_issues:
                from deploy.sofascore.pipeline_issues import GitHubIssues
                errors = publish(state, GitHubIssues(), checkpoint)
            result["publication_errors"] = errors
            print(json.dumps(result, ensure_ascii=False))
        return 1 if errors else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"pipeline_watchdog: unobservable: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
