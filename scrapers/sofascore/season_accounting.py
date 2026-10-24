"""Offline, durable native-season closure accounting for #1364.

Consumes saved history_inventory output and finalized slot receipts. This module
never collects inventory, accesses a database, or contacts the source. Observation
times bound transitions; they are never asserted to be exact closure timestamps.
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dags.utils.sofascore_red_share import history_scope_counts

PUBLICATION_DEFINITION = "history_inventory.manifest5+bronze_capture_complete.v1"
UTC = timezone.utc


def digest(value) -> str:
    """Same canonical JSON digest as the existing history controller/receipts."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp requires timezone")
    return parsed.astimezone(UTC)


def _integer(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected nonnegative integer")
    return value


def _key(row) -> str:
    tid, sid = _integer(row["tournament_id"]), _integer(row["season_id"])
    if not tid or not sid:
        raise ValueError("native tournament/season IDs must be positive")
    return f"{tid}:{sid}"


def _identity(value):
    if (not isinstance(value, dict) or set(value) != {"campaign_id", "snapshot_id"}
            or any(not isinstance(v, str) or not v for v in value.values())):
        raise ValueError("explicit campaign/snapshot identity required")
    return value


def _transition_day(after, observed, exact=None):
    """Attribute only an interval contained within one half-open UTC day."""
    if exact is not None:
        stamp = _time(exact)
        if not after < stamp <= observed:
            raise ValueError("closure_at outside observation interval")
        return stamp.date().isoformat()
    return observed.date().isoformat() if after.date() == observed.date() else None


def apply_observation(journal: dict | None, observation: dict) -> dict:
    """Return a new journal; replay is idempotent and mutation of an ID is refused.

    ``inventory.scopes`` uses the native fields produced by history_inventory.
    Optional closed_match_ids contains IDs of published finished matches from the
    *same saved read*. Without IDs, season transitions remain useful, but distinct
    match throughput is unavailable. The publication envelope pins provenance
    and content; a hash is an integrity check, not a substitute for that read.
    """
    identity = _identity(observation["identity"])
    roster = observation["roster"]
    members = {_key(r): r for r in roster}
    if not members or len(members) != len(roster) or any(
            str(r.get("group")) not in {"1", "2", "3", "4", "5", "july"} for r in roster):
        raise ValueError("complete distinct native-season/group roster required")
    roster_hash = digest(roster)
    if journal is not None and (journal.get("schema_version") != 1
                               or journal["identity"] != identity
                               or journal["group_digest"] != roster_hash):
        raise ValueError("journal identity mismatch")
    state = copy.deepcopy(journal) if journal is not None else {
        "schema_version": 1, "identity": identity, "group_digest": roster_hash,
        "roster": roster, "observations": [], "seasons": {}, "events": [], "matches": {},
    }
    observation_id = observation["observation_id"]
    if not isinstance(observation_id, str) or not observation_id:
        raise ValueError("observation_id required")
    content_digest = digest(observation)
    for saved in state["observations"]:
        if saved["observation_id"] == observation_id:
            if saved["digest"] != content_digest:
                raise ValueError("immutable observation ID content mismatch")
            return state
    moment = _time(observation["observed_at"])
    previous_time = _time(state["observations"][-1]["observed_at"]) if state["observations"] else None
    if previous_time is not None and moment <= previous_time:
        raise ValueError("observations must be strictly chronological")
    release = observation["release"]
    if not isinstance(release, str) or not release or release == "unknown":
        raise ValueError("actual release required")
    inventory = observation["inventory"]
    evidence = observation.get("publication_evidence", {})
    qualified = (observation.get("roster_complete") is True
                 and evidence.get("definition") == PUBLICATION_DEFINITION
                 and evidence.get("inventory_digest") == digest(inventory)
                 and _time(inventory["observed_at"]) == moment)
    rows = {_key(r): r for r in inventory["scopes"]}
    if len(rows) != len(inventory["scopes"]) or not rows.keys() <= members.keys():
        raise ValueError("inventory native-season roster mismatch")
    unknown, ids_complete = [], True
    for key, member in members.items():
        row = rows.get(key, {})
        finished, closed = row.get("finished"), row.get("closed")
        counts_valid = (type(finished) is int and type(closed) is int
                        and 0 <= closed <= finished)
        known = (qualified and counts_valid and row.get("schedule_complete") is True
                 and type(row.get("ongoing")) is bool
                 and all(type(row.get(k, 0)) is int and row.get(k, 0) == 0
                         for k in ("unknown", "unknown_matches", "unknown_count", "unknown_scopes")))
        status = ("closed" if finished and finished == closed and row.get("ongoing") is False
                  else "open") if known else "unknown"
        if not known:
            unknown.append(key)
        raw_ids = row.get("closed_match_ids")
        ids = None
        if known and isinstance(raw_ids, list):
            if (any(not isinstance(v, (str, int)) or isinstance(v, bool) or not str(v).isdigit()
                    for v in raw_ids) or len(set(map(str, raw_ids))) != len(raw_ids)
                    or len(raw_ids) != closed):
                raise ValueError("closed_match_ids disagree with native inventory counts")
            ids = set(map(str, raw_ids))
        if ids is None:
            ids_complete = False
        previous = state["seasons"].get(key)
        if previous is not None and previous["status"] != status:
            kind = None
            if status == "closed" and previous["status"] == "open":
                kind = "reclose" if previous["ever_closed"] else "first_closure"
            elif previous["status"] == "closed" and status == "open":
                kind = "reopen"
            elif status == "unknown" or previous["status"] == "unknown":
                kind = "evidence_gap"
            if kind:
                state["events"].append({"kind": kind, "season_key": key,
                    "group": str(member["group"]), "release": release,
                    "after": previous["observed_at"], "observed_at": moment.isoformat(),
                    "day": _transition_day(_time(previous["observed_at"]), moment,
                                           row.get("closure_at") if kind in ("first_closure", "reclose") else None),
                    "finished": finished if counts_valid else None,
                    "observation_id": observation_id})
        # Every previously seen match stays in the journal across reopen/retry.
        # Baseline publications and first sight after an evidence gap are retained
        # but cannot become invented new publications.
        if ids is not None:
            for match_id in sorted(ids):
                match_key = f"{key}:{match_id}"
                if match_key not in state["matches"]:
                    attributable = (previous is not None and previous.get("ids_known") is True
                                    and previous["status"] != "unknown")
                    state["matches"][match_key] = {
                        "season_key": key, "match_id": match_id, "release": release,
                        "group": str(member["group"]), "baseline": not attributable,
                        "after": previous["observed_at"] if previous else None,
                        "observed_at": moment.isoformat(),
                        "publication_run_id": row.get("closed_match_runs", {}).get(match_id),
                        "closed_at": row.get("match_closed_at", {}).get(match_id),
                        "day": _transition_day(_time(previous["observed_at"]), moment,
                                                row.get("match_closed_at", {}).get(match_id)) if attributable else None,
                    }
        state["seasons"][key] = {"status": status, "observed_at": moment.isoformat(),
            "ever_closed": (previous["ever_closed"] if previous else False) or status == "closed",
            "ids_known": ids is not None, "finished": finished, "closed": closed}
    state["observations"].append({"observation_id": observation_id, "digest": content_digest,
        "observed_at": moment.isoformat(), "release": release,
        "unknown_seasons": unknown, "match_ids_complete": ids_complete,
        "roster_complete": observation.get("roster_complete") is True})
    return state


def _union_seconds(spans):
    merged = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return sum((end - start).total_seconds() for start, end in merged)


def summarize_day(journal: dict, saved_day: dict, result_dir: str | Path) -> dict:
    """Evaluate one whole UTC day. Missing evidence yields UNKNOWN, never PASS."""
    d0 = _time(saved_day["day"] + "T00:00:00Z")
    d1 = d0 + timedelta(days=1)
    release = saved_day.get("release")
    missing, attempts = [], []
    identity = _identity(saved_day["identity"])
    if (identity != journal["identity"] or saved_day.get("group_digest") != journal["group_digest"]):
        missing.append("campaign/snapshot/group identity mismatch")
    interval = saved_day.get("delivered_interval", {})
    try:
        if not _time(interval["start"]) <= d0 < d1 <= _time(interval["end"]):
            missing.append("cross-release or incomplete actual delivered interval")
    except (KeyError, ValueError, TypeError):
        missing.append("actual delivered interval missing")
    observations = [o for o in journal["observations"] if d0 <= _time(o["observed_at"]) <= d1]
    if not all(any(_time(o["observed_at"]) == t for o in observations) for t in (d0, d1)):
        missing.append("exact UTC-boundary inventory snapshots missing")
    if any(o["unknown_seasons"] for o in observations):
        missing.append("unknown season inventory within day")
    if not observations or any(not o["match_ids_complete"] for o in observations):
        missing.append("physical closed match IDs unavailable")
    if any(o["release"] != release for o in observations) or not release or release == "unknown":
        missing.append("cross-release inventory evidence")
    expected = saved_day.get("expected_runs", [])
    runs = [r.get("run_id") for r in expected]
    if (saved_day.get("run_roster_complete") is not True
            or saved_day.get("run_roster_digest") != digest(expected)
            or any(not isinstance(r, str) or not r for r in runs) or len(set(runs)) != len(runs)):
        missing.append("whole expected-run roster unqualified")
    if not runs:
        missing.append("empty expected-run roster cannot establish collection coverage")
    result_dir = Path(result_dir)
    spans = {str(slot): [] for slot in range(3)}
    members = {_key(r): str(r["group"]) for r in journal["roster"]}
    for expected_run in expected:
        run_id = expected_run["run_id"]
        try:
            # Reuse the existing canonical digest, finalize, exact report and
            # native identity checks; no second heavyweight collector.
            history_scope_counts(result_dir, d0.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 d1.strftime("%Y-%m-%dT%H:%M:%SZ"), {run_id})
            path = result_dir / ("history-slots-" + hashlib.sha256(run_id.encode()).hexdigest()[:20] + ".json")
            raw = path.read_bytes()
            receipt = json.loads(raw)
            if (expected_run.get("release") != release
                    or expected_run.get("receipt_sha256") != hashlib.sha256(raw).hexdigest()
                    or expected_run.get("plan_digest") != receipt["plan_digest"]
                    or receipt.get("campaign_id") != identity["campaign_id"]):
                raise ValueError("receipt release/content/plan/campaign identity mismatch")
            # A receipt pins outcomes, while a saved controller checkpoint pins
            # the complete plan, slot ledger and selected group. The current
            # live checkpoint must not be substituted for a historical run.
            from scrapers.sofascore.history_controller import _read
            checkpoint_path = result_dir / Path(expected_run["checkpoint_file"]).name
            if hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() != expected_run.get("checkpoint_sha256"):
                raise ValueError("saved checkpoint content mismatch")
            checkpoint = _read(checkpoint_path, identity["campaign_id"])
            reservation = checkpoint["run"]
            if (checkpoint.get("snapshot_id") != identity["snapshot_id"]
                    or reservation["run_id"] != run_id or reservation.get("mode") != "slots"
                    or reservation.get("plan_digest") != receipt["plan_digest"]
                    or reservation.get("slot_digest") != expected_run.get("slot_digest")
                    or reservation.get("finalized") is not True
                    or reservation.get("cursor") != receipt["claimed_count"]
                    or len(reservation["items"]) != receipt["claimed_count"]
                    or len(reservation["plan"]) != receipt["plan_length"]):
                raise ValueError("saved checkpoint run/snapshot/plan/slot identity mismatch")
            active = checkpoint.get("active_group")
            if active not in {"1", "2", "july", "3", "4", "5"}:
                raise ValueError("saved selected history group missing")
            if active not in {"1", "2"}:
                for earlier in ("1", "2"):
                    group = checkpoint["groups"][earlier]
                    if (group["unknown_scopes"] != 0 or group["closed"] * 100 < group["finished"] * 99
                            or group.get("gate_passed") is not True):
                        raise ValueError("saved checkpoint selected group before core gate")
            for item in receipt["items"]:
                ledger_item = {k: v for k, v in item.items() if k not in {"index", "environment"}}
                if reservation["items"].get(str(item["index"])) != ledger_item:
                    raise ValueError("receipt differs from pinned slot ledger")
                slot = item["slot"]
                if type(slot) is not int or slot not in range(3):
                    raise ValueError("slot outside fixed three-slot denominator")
                env = item["environment"]
                key = f'{int(env["SOFASCORE_TOURNAMENT_ID"])}:{int(env["SOFASCORE_SOURCE_SEASON_ID"])}'
                if (env.get("SOFASCORE_EXPECTED_CAMPAIGN_ID") != identity["campaign_id"]
                        or env.get("SOFASCORE_EXPECTED_SNAPSHOT_ID") != identity["snapshot_id"]
                        or env.get("SOFASCORE_HISTORY_GROUP") != members.get(key)):
                    raise ValueError("receipt native-season/group/snapshot identity mismatch")
                if (env.get("SOFASCORE_HISTORY_GROUP") != active
                        or env != reservation["plan"][item["plan_index"]]):
                    raise ValueError("receipt differs from selected checkpoint plan/group")
                status = item["outcome"]["status"]
                record = {"run_id": run_id, "index": item["index"], "slot": slot,
                          "status": status, "attempts": item.get("attempts"),
                          "season_key": key, "group": members[key], "release": release,
                          "scope_run_id": env.get("SOFASCORE_SCOPE_RUN_ID"),
                          "started_at": item.get("started_at"), "finished_at": item.get("finished_at")}
                attempts.append(record)
                if status != "not_started":
                    a, b = _time(item["started_at"]), _time(item["finished_at"])
                    count = _integer(item.get("attempts"))
                    if count < 1:
                        raise ValueError("started scope has no capture attempts")
                    history = expected_run.get("attempt_histories", {}).get(str(item["index"]))
                    if count > 1 and history is None:
                        missing.append(f"run {run_id} item {item['index']}: failed/retry attempt history unavailable")
                    if history is not None:
                        if not isinstance(history, list) or len(history) != count:
                            raise ValueError("retry attempt history count mismatch")
                        last_end = a
                        for detail in history:
                            begin, end = _time(detail["started_at"]), _time(detail["finished_at"])
                            if not (last_end <= begin <= end <= b) or detail["status"] not in ("success", "failed", "waiting"):
                                raise ValueError("invalid saved retry attempt timeline")
                            last_end = end
                        if history[-1]["status"] != status:
                            raise ValueError("retry history terminal outcome mismatch")
                        record["attempt_history"] = history
                    if not _time(interval["start"]) <= a <= b <= _time(interval["end"]):
                        raise ValueError("attempt crosses actual release interval")
                    if max(a, d0) < min(b, d1):
                        spans[str(slot)].append((max(a, d0), min(b, d1)))
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
            missing.append(f"run {run_id}: {error}")
    first_closures = [e for e in journal["events"]
                      if e["kind"] == "first_closure" and e["day"] == saved_day["day"]]
    new_matches = [m for m in journal["matches"].values()
                   if not m["baseline"] and m["day"] == saved_day["day"]]
    successful = {}
    for attempt in attempts:
        if attempt["status"] == "success":
            scope = str(attempt["scope_run_id"]) + "::targets"
            if scope in successful:
                missing.append("duplicate successful history scope identity")
            successful[scope] = attempt
    attributed_seasons = set()
    for match in new_matches:
        attempt = successful.get(match.get("publication_run_id"))
        attributable = (attempt is not None and attempt["season_key"] == match["season_key"]
                        and _time(attempt["started_at"]) < _time(match["observed_at"])
                        and _time(attempt["finished_at"]) > _time(match["after"]))
        if attributable and match.get("closed_at") is not None:
            attributable = (_time(attempt["started_at"]) <= _time(match["closed_at"])
                            <= _time(attempt["finished_at"]))
        if not attributable:
            missing.append(f"native match {match['season_key']}:{match['match_id']}: exact successful history publication association unavailable")
        else:
            attributed_seasons.add(match["season_key"])
    for event in first_closures:
        if event["season_key"] not in attributed_seasons:
            missing.append(f"season {event['season_key']}: first closure lacks successful history publication association")
    # A transition straddling a boundary is honest interval evidence, not a
    # timestamp. It blocks both potentially affected days unless exact saved
    # closure_at/match_closed_at evidence attributes it.
    ambiguous = [e for e in journal["events"] if e["kind"] == "first_closure" and e["day"] is None
                 and _time(e["after"]) < d1 and _time(e["observed_at"]) >= d0]
    ambiguous += [m for m in journal["matches"].values() if not m["baseline"] and m["day"] is None
                  and _time(m["after"]) < d1 and _time(m["observed_at"]) >= d0]
    if ambiguous:
        missing.append("closure interval crosses UTC boundary without exact saved closure evidence")
    if any(e["release"] != release for e in first_closures + new_matches):
        missing.append("cross-release closure evidence")
    occupied = sum(_union_seconds(value) for value in spans.values())
    matches_known = (bool(observations) and all(o["match_ids_complete"] and not o["unknown_seasons"]
                                              for o in observations) and not ambiguous)
    closed_matches = len(new_matches) if matches_known else None
    slots = {slot: {"occupied_seconds": _union_seconds(value), "capacity_seconds": 86400,
                    "occupancy_pct": 100 * _union_seconds(value) / 86400} for slot, value in spans.items()}
    rate = closed_matches / 72 if closed_matches is not None else None
    checks = {"closed_seasons": len(first_closures) >= 67,
              "closed_matches": closed_matches is not None and closed_matches >= 10100,
              "matches_per_hour_per_slot": rate is not None and rate >= 202,
              "slot_occupancy": occupied / (3 * 86400) >= .78}
    return {"schema_version": 1, "day": saved_day["day"], "identity": identity,
            "group_digest": saved_day.get("group_digest"), "release": release,
            "start": d0.isoformat(), "end": d1.isoformat(),
            "verdict": "UNKNOWN" if missing else "PASS" if all(checks.values()) else "FAIL",
            "missing_evidence": missing, "closed_seasons": len(first_closures),
            "closed_matches": closed_matches, "season_denominator": len(journal["roster"]),
            "expected_runs": expected, "attempts": attempts, "slots": slots,
            "slot_capacity_seconds": 3 * 86400, "occupied_slot_seconds": occupied,
            "slot_occupancy_pct": 100 * occupied / (3 * 86400),
            "matches_per_hour_per_slot": rate, "checks": checks,
            "first_closures": first_closures,
            "other_transitions": [e for e in journal["events"] if e["day"] == saved_day["day"]
                                  and e["kind"] != "first_closure"],
            "acceptance_started_at": None, "source_requests": 0, "production_changes": 0}


def summarize_series(reports: list[dict]) -> dict:
    """Only seven consecutive, fully evidenced UTC days can jointly pass."""
    days = sorted(reports, key=lambda r: r["day"])
    reasons = []
    if len(days) != 7 or len({r["day"] for r in days}) != 7:
        reasons.append("exactly seven distinct UTC days required")
    if days:
        first = days[0]
        if any((r["identity"], r["group_digest"], r["release"]) !=
               (first["identity"], first["group_digest"], first["release"]) for r in days):
            reasons.append("cross-release or changed campaign/group identity")
        if any(_time(b["day"] + "T00:00:00Z") - _time(a["day"] + "T00:00:00Z") != timedelta(days=1)
               for a, b in zip(days, days[1:])):
            reasons.append("UTC days are not consecutive")
    if any(r["verdict"] == "UNKNOWN" for r in days):
        reasons.append("unknown day evidence")
    return {"verdict": "UNKNOWN" if reasons else "PASS" if all(r["verdict"] == "PASS" for r in days) else "FAIL",
            "days": [r["day"] for r in days], "missing_evidence": reasons,
            "acceptance_started_at": None}
