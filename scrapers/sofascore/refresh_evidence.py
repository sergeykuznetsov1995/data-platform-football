"""Offline acceptance evidence from the refresh planner's existing inputs.

No network, scheduler, or database access. A selected plan is deliberately not
accepted as the census read before slicing. Producers must retain that read,
each refill read and the final read, including exclusions and failed attempts.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _instant(value):
    if not isinstance(value, str):
        raise ValueError("timestamp required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)


def _integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("nonnegative integer required")
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def planning_snapshot(identity, partitions, selected, *, core_ids,
                      daily_ids=(), excluded_ids=(), disputed_ids=(), input_complete=False,
                      observed_at=None):
    """Capture ALL six-column pending rows, before any planner filtering.

    Rows have the existing PENDING_PARTITIONS_SQL shape: capture key, canonical
    season, pending count, nearest open deadline, open count, native season ID.
    Identity must come from qualified delivery/readback, never merge time.
    """
    core, daily, excluded, disputed = map(set, (core_ids, daily_ids, excluded_ids, disputed_ids))
    if core & excluded or daily & excluded:
        raise ValueError("conflicting denominator membership")
    rows, seen = [], set()
    for league, canonical, pending, deadline, opened, season in partitions:
        if not str(league).startswith("SS-"):
            raise ValueError("native tournament identity required")
        tid, sid = int(str(league)[3:]), _integer(season, 1)
        _integer(tid, 1)
        pending, opened = _integer(pending, 1), _integer(opened)
        if (opened > pending or (opened > 0 and type(deadline) is not int)
                or (opened == 0 and deadline is not None)):
            raise ValueError("inconsistent open-deadline counts")
        key = tid, sid
        if key in seen:
            raise ValueError("duplicate native season")
        seen.add(key)
        klass = ("daily" if tid in daily else "excluded" if tid in excluded
                 else "core" if tid in core else "disputed" if tid in disputed else "unclassified")
        rows.append(dict(tournament_id=tid, source_season_id=sid,
                         canonical_season=str(canonical), pending_matches=pending,
                         open_matches=opened, overdue_matches=pending - opened,
                         nearest_open_deadline=deadline, classification=klass))
    due = [r for r in rows if r["classification"] == "core"]
    document = dict(schema_version=1, identity=dict(identity), observed_at=observed_at,
                    partitions=rows,
                    selected=list(selected), input_complete=input_complete is True,
                    due_core_scopes=len(due), due_core_matches=sum(r["pending_matches"] for r in due),
                    overdue_core_matches=sum(r["overdue_matches"] for r in due),
                    unclassified_partitions=sum(r["classification"] == "unclassified" for r in rows))
    document["digest"] = _digest(document)
    return document


def write_immutable(path, document):
    """Atomic no-overwrite publication; identical retry is an idempotent read."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(document, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=".refresh-evidence-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != raw:
                raise ValueError("immutable refresh evidence conflict") from None
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _validate_snapshot(doc, revision, identity=None):
    if doc.get("schema_version") != 1 or doc.get("input_complete") is not True:
        raise ValueError("full planning census absent")
    if doc.get("digest") != _digest({k: v for k, v in doc.items() if k != "digest"}):
        raise ValueError("planning evidence digest mismatch")
    current = doc["identity"]
    for name, length in (("code_revision", 40), ("artifact_id", 64),
                         ("denominator_sha256", 64), ("snapshot_id", 64), ("campaign_id", 64)):
        value = current.get(name, "")
        if len(value) != length or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("unqualified identity: " + name)
    if current["code_revision"] != revision or not current.get("run_id"):
        raise ValueError("release/run identity mismatch")
    if identity and any(current[k] != identity[k] for k in identity):
        raise ValueError("mixed planning identity")
    _instant(current["logical_date"])
    _instant(current["started_at"])
    _instant(doc["observed_at"])
    if doc["unclassified_partitions"]:
        raise ValueError("unclassified denominator partitions")
    rows = doc["partitions"]
    native = {(r["tournament_id"], r["source_season_id"]): r for r in rows}
    if len(native) != len(rows):
        raise ValueError("duplicate census identity")
    due = [r for r in rows if r["classification"] == "core"]
    if (doc["due_core_scopes"] != len(due)
            or doc["due_core_matches"] != sum(_integer(r["pending_matches"], 1) for r in due)
            or doc["overdue_core_matches"] != sum(_integer(r["overdue_matches"]) for r in due)):
        raise ValueError("census aggregates differ from retained rows")
    consumed_open, previous_deadline = set(), None
    for selected in doc["selected"]:
        key = selected["tournament_id"], selected["source_season_id"]
        if key not in native:
            raise ValueError("selected season absent from input census")
        row = native[key]
        if row["classification"] not in {"core", "disputed"}:
            raise ValueError("selected excluded/daily scope")
        # Only core priorities form the accepting denominator. A disputed
        # scope cannot overtake an unresolved core open-deadline candidate.
        open_due = {k: r for k, r in native.items() if r["classification"] == "core"
                    and r["open_matches"] > 0 and k not in consumed_open}
        phase = selected.get("phase", "open")
        if phase not in {"open", "debt"}:
            raise ValueError("unknown scope phase")
        if phase == "debt" or row["classification"] == "disputed":
            if open_due:
                raise ValueError("debt/disputed scope overtakes core deadline")
        else:
            deadline = row["nearest_open_deadline"]
            if deadline is None or (open_due and deadline != min(r["nearest_open_deadline"] for r in open_due.values())):
                raise ValueError("earliest core deadline not selected")
            if previous_deadline is not None and deadline < previous_deadline:
                raise ValueError("deadline order reversed")
            previous_deadline = deadline
            consumed_open.add(key)
    return current


def _union_seconds(outcomes, started, deadline):
    intervals = []
    for row in outcomes:
        begin, end = _instant(row["started_at"]), _instant(row["finished_at"])
        if end < begin or begin < started or end > deadline:
            raise ValueError("scope time outside qualified window")
        intervals.append((begin.timestamp(), end.timestamp()))
    total, right = 0.0, None
    for begin, end in sorted(intervals):
        total += max(0, end - max(begin, right if right is not None else begin))
        right = max(end, right if right is not None else end)
    return total


def _tail_status(tail, expected, identity):
    if (not isinstance(expected, list) or len(set(expected)) != 7 or len(expected) != 7
            or tail.get("run_players") is not False
            or tail.get("fotmob_import_errors") != 0
            or tail.get("code_revision") != identity["code_revision"]
            or tail.get("night") != _instant(identity["started_at"]).date().isoformat()
            or not tail.get("run_id")):
        raise ValueError("tail7 identity/players/import evidence incomplete")
    rows = tail["leagues"]
    if len(rows) != 7 or {r["league"] for r in rows} != set(expected):
        raise ValueError("tail7 league denominator incomplete")
    return "pass" if all(r.get("status") == "success" and r.get("capture_complete") is True
                         for r in rows) else "fail"


def assess_run(bundle, expected_revision=None):
    """Return separate check results; incomplete inputs never create a PASS."""
    result = dict(status="unknown", criteria={}, reasons=[], observed_run_state=bundle.get("state"))
    result["criteria"]["dag_run"] = ("pass" if bundle.get("state") == "success" else
                                      "fail" if bundle.get("state") == "failed" else "unknown")
    try:
        revision = expected_revision or bundle["expected_revision"]
        if bundle["expected_revision"] != revision:
            raise ValueError("run belongs to a different release series")
        identity = _validate_snapshot(bundle["initial"], revision)
        for snap in [*bundle["refills"], bundle["final"]]:
            _validate_snapshot(snap, revision, identity)
        outcomes = bundle["outcomes"]
        initial_time = _instant(bundle["initial"]["observed_at"])
        final_time = _instant(bundle["final"]["observed_at"])
        sequence = [initial_time, *[_instant(s["observed_at"]) for s in bundle["refills"]], final_time]
        if sequence != sorted(sequence):
            raise ValueError("planning observations out of order")
        if outcomes and (initial_time > min(_instant(r["started_at"]) for r in outcomes)
                         or final_time < max(_instant(r["finished_at"]) for r in outcomes)):
            raise ValueError("planning snapshots do not bracket execution")
        expected_scopes = [s.get("attempt_id", s["scope_key"]) for snap in [bundle["initial"], *bundle["refills"]]
                           for s in snap["selected"]]
        if len(set(expected_scopes)) != len(expected_scopes):
            raise ValueError("duplicate planned attempt identity")
        if (bundle.get("attempt_history_complete") is not True
                or {r.get("attempt_id", r["scope_key"]) for r in outcomes} != set(expected_scopes)
                or any(r.get("run_id") != identity["run_id"] for r in outcomes)):
            raise ValueError("missing/foreign scope outcomes")
        plans = {s.get("attempt_id", s["scope_key"]): s
                 for snap in [bundle["initial"], *bundle["refills"]] for s in snap["selected"]}
        attempts = {key: [] for key in expected_scopes}
        for row in outcomes:
            key = row.get("attempt_id", row["scope_key"])
            if row["scope_key"] != plans[key]["scope_key"]:
                raise ValueError("physical attempt belongs to a foreign scope")
            attempts[key].append(row)
            _integer(row["remaining_matches"])
            if row["status"] == "partial" and not row.get("stop_reason"):
                raise ValueError("partial scope needs a stop reason")
        terminal = []
        for rows in attempts.values():
            ordered = sorted(rows, key=lambda r: _instant(r["started_at"]))
            for previous, following in zip(ordered, ordered[1:]):
                if (_instant(previous["finished_at"]) > _instant(following["started_at"])
                        or previous["started_at"] == following["started_at"]):
                    raise ValueError("duplicate/overlapping physical attempt")
            terminal.append(ordered[-1])
        result.update(physical_attempts=len(outcomes), physical_outcomes=list(outcomes),
                      failed_physical_attempts=sum(r["status"] not in {"success", "refreshed", "partial"}
                                                   for r in outcomes))
        refill = bundle["refill_result"]
        if refill.get("status") not in {"success", "partial", "failed"} or not refill.get("stop_reason"):
            raise ValueError("terminal refill result absent")
        remaining = bundle["final"]["due_core_matches"]
        exhausted = refill["status"] == "partial" and refill["stop_reason"] in {"time_budget", "wall_deadline"}
        failed = (refill["status"] == "failed" or
                  any(r["status"] not in {"success", "refreshed", "partial"} for r in terminal))
        result["criteria"]["due_scope_completion"] = "fail" if failed or (remaining and not exhausted) else "pass"
        budget = bundle["window_budget_s"]
        if type(budget) not in (int, float) or not math.isfinite(budget) or budget != 7200:
            raise ValueError("qualified window must be7200 seconds")
        window = bundle["capture_window"]
        started, deadline = _instant(window["started_at"]), _instant(window["deadline_at"])
        if started < _instant(identity["started_at"]) or deadline <= started:
            raise ValueError("invalid saved capture window")
        used = _union_seconds(outcomes, started, deadline)
        # No-demand and a short replay are not proof that a busy window can be filled.
        demand = bundle["initial"]["due_core_matches"] > 0
        result.update(run_id=identity["run_id"], started_at=identity["started_at"],
                      due_core_scopes=bundle["initial"]["due_core_scopes"],
                      due_core_matches=bundle["initial"]["due_core_matches"],
                      overdue_core_matches=bundle["initial"]["overdue_core_matches"],
                      remaining_core_matches=remaining, capture_window=dict(window),
                      window_used_s=used, window_use=used / budget)
        result["criteria"]["window_utilization"] = "pass" if demand and used / budget >= .7 else "fail" if demand else "unknown"
        result["criteria"]["tail7"] = _tail_status(bundle["tail"], bundle["expected_tail_leagues"], identity)
        checks = result["criteria"].values()
        result["status"] = "fail" if "fail" in checks else "unknown" if "unknown" in checks else "pass"
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        result["status"] = "fail" if "fail" in result["criteria"].values() else "unknown"
        result["reasons"].append(str(exc))
    return result


def assess_third_day(data, expected_revision):
    try:
        if data["code_revision"] != expected_revision:
            raise ValueError("cross-release coverage")
        # The third-day observation must cover the actual delivery+72h boundary.
        target = _instant(data["delivered_at"]) + timedelta(days=3)
        fact_at = _instant(data.get("measured_at", data["checked_at"]))
        if fact_at != target or _instant(data["checked_at"]) < fact_at:
            raise ValueError("exact third-day boundary not qualified")
        shares = {}
        for name in ("SS", "daily"):
            row = data["lanes"][name]
            closed, total = _integer(row["closed"]), _integer(row["total"], 1)
            if closed > total:
                raise ValueError("invalid coverage denominator")
            shares[name] = closed / total
        return dict(status="pass" if min(shares.values()) >= .9 else "fail", lanes=shares,
                    boundary=target.isoformat())
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        return dict(status="unknown", reason=str(exc))


def assess_nights(expected_runs, bundles, expected_revision=None):
    if not expected_runs or len(set(expected_runs)) != len(expected_runs):
        raise ValueError("complete distinct expected-run roster required")
    revisions = {bundles[key].get("expected_revision") for key in expected_runs if key in bundles}
    pin = expected_revision or (next(iter(revisions)) if len(revisions) == 1 else None)
    rows = [dict(**assess_run(bundles[key], pin), expected_run_id=key) if key in bundles
            else dict(run_id=key, status="unknown", reasons=["expected run missing"])
            for key in expected_runs]
    streak, previous = 0, None
    for row in rows:
        started = _instant(row["started_at"]) if row.get("started_at") else None
        night = started.date() if started and started.hour < 6 else None
        valid = pin and row["status"] == "pass" and row.get("run_id") == row.get("expected_run_id") and night
        streak = (streak + 1 if previous and night == previous + timedelta(days=1) else 1) if valid else 0
        previous = night if valid else None
    return dict(runs=rows, accepted_nights=streak, required_nights=3, expected_revision=pin,
                reasons=[] if pin else ["one qualified release pin required for the series"],
                failed_runs=sum(r["status"] == "fail" for r in rows),
                unknown_runs=sum(r["status"] == "unknown" for r in rows),
                status="pass" if streak >= 3 else "unknown")
