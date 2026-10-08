"""Debt-led history waves and an immutable, restart-safe DagRun checkpoint.

No source, browser or database calls. Evidence adapters must identify actual
source seasons; completion memory is never a substitute for published matches.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from datetime import timedelta
from typing import Mapping

from dags.utils import sofascore_all_mens_state as state
from scrapers.sofascore.denominator import Denominator, load_denominator

GROUPS = ("1", "2", "july", "3", "4", "5")
LABELS = {"1": "текущие", "2": "последние завершённые", "july": "июльский хвост",
          "3": "10 предыдущих", "4": "глубже", "5": "спорные"}
SCHEMA_VERSION = 1


def _read(path: Path, campaign: str) -> dict:
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "campaign_id": campaign,
                "positions": {}, "run": None, "retired_runs": []}
    try:
        value = json.loads(path.read_text())
    except (ValueError, OSError) as exc:
        raise state.CampaignPlanningError(f"cannot read history checkpoint: {exc}") from exc
    if (not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION
            or value.get("campaign_id") != campaign or not isinstance(value.get("positions"), dict)):
        raise state.CampaignPlanningError("history checkpoint does not match campaign")
    run = value.get("run")
    if run:
        expected = hashlib.sha256(json.dumps(run.get("plan"), sort_keys=True).encode()).hexdigest()
        if run.get("plan_digest") != expected:
            raise state.CampaignPlanningError("history checkpoint plan digest mismatch")
        if run.get("mode") == "slots":
            digest = hashlib.sha256(json.dumps({k: v for k, v in run.items() if k != "slot_digest"}, sort_keys=True).encode()).hexdigest()
            if run.get("slot_digest") != digest:
                raise state.CampaignPlanningError("history slot ledger digest mismatch")
    return value


def _write(path: Path, document: Mapping) -> None:
    run = document.get("run")
    if run and run.get("mode") == "slots":
        run["slot_digest"] = hashlib.sha256(json.dumps({k: v for k, v in run.items() if k != "slot_digest"}, sort_keys=True).encode()).hexdigest()
    state._write_document_atomically(path, document)
    # Persist the rename as well as the file before accepting a reservation.
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _evidence(inventory: Mapping) -> dict[tuple[int, int], dict]:
    rows = inventory.get("scopes")
    if not isinstance(rows, list) or not inventory.get("observed_at"):
        raise state.CampaignPlanningError("history inventory requires scopes and observed_at")
    result = {}
    for row in rows:
        key = (int(row["tournament_id"]), int(row["season_id"]))
        if key in result:
            raise state.CampaignPlanningError("duplicate source season in history inventory")
        total, closed = row.get("finished", 0), row.get("closed", 0)
        if (isinstance(total, bool) or not isinstance(total, int) or
                isinstance(closed, bool) or not isinstance(closed, int) or not 0 <= closed <= total):
            raise state.CampaignPlanningError("invalid history coverage counts")
        if row.get("ongoing") not in (True, False, None):
            raise state.CampaignPlanningError("invalid season lifecycle")
        result[key] = dict(row)
    return result


def classify(snapshot: Mapping, inventory: Mapping, denominator: Denominator) -> list[dict]:
    evidence = _evidence(inventory)
    scopes = []
    seen = set()
    for tournament in snapshot["tournaments"]:
        tid = int(tournament["unique_tournament_id"])
        priority = denominator.queue_priority(tid)
        if not priority or tournament.get("metadata_status") == "excluded":
            continue
        seasons = sorted((s for s in tournament["seasons"]
                          if s.get("metadata_status") != "excluded"),
                         key=lambda s: (-int(s["start_year"]), -int(s["source_season_id"])))
        historical_depth = 0
        latest_finished = False
        for index, season in enumerate(seasons):
            sid = int(season["source_season_id"])
            if (tid, sid) in seen:
                raise state.CampaignPlanningError("duplicate source season in snapshot")
            seen.add((tid, sid))
            row = evidence.get((tid, sid), {})
            ongoing = row.get("ongoing")
            unknown = ongoing is None or not row.get("schedule_complete", False)
            # An unobserved newest season is bootstrap work of group1, never
            # an excuse to move straight to the previous season's history.
            if ongoing is True or (index == 0 and ongoing is None):
                group, depth = "1", 0
            elif not latest_finished:
                group, depth, latest_finished = "2", 0, True
            else:
                historical_depth += 1
                depth = historical_depth
                group = "3" if depth <= 10 else "4"
            if priority == 9:
                group = "5"
            scopes.append({"group": group, "depth": depth, "tournament": tournament,
                           "season": season, "evidence": row, "unknown": unknown,
                           "key": state.campaign_scope_key(snapshot["campaign_id"], tid, sid)})
    return scopes


def _kind(scope: Mapping, failures: Mapping, release: str, moment, max_attempts: int) -> str:
    t, s = scope["tournament"], scope["season"]
    if t.get("metadata_status") != "ready" or s.get("metadata_status") != "ready":
        return "pending"
    if not s.get("canonical_season") or s.get("season_format") not in {"split_year", "single_year"}:
        return "mapping_required"
    attempts = failures.get(scope["key"], {})
    if attempts.get("completed_rejected_endpoints") and attempts.get("last_release") == release:
        # A green publication can still hold rejected match rows. Give those
        # raw endpoints one replay per new release, not a free endless loop.
        return "quarantined"
    quarantine = state.is_quarantined_record(attempts, max_attempts)
    if quarantine and state._quarantine_applies(
            attempts, release, state.season_alignment_identity(int(t["unique_tournament_id"]), s)):
        return "quarantined"
    if (not quarantine and int(attempts.get("count", 0)) >= max_attempts
            and not state.park_has_cooled(attempts, moment, state.DEFAULT_PARK_COOLDOWN_HOURS)):
        return "parked"
    return "ready"


def coverage(scopes: list[dict], inventory: Mapping) -> dict:
    groups = {g: {"finished": 0, "closed": 0, "remaining": 0, "unknown_scopes": 0,
                  "scopes": 0, "pending": 0, "mapping_required": 0,
                  "parked": 0, "quarantined": 0, "deferred": 0} for g in GROUPS}
    for scope in scopes:
        row = scope["evidence"]
        record = groups[scope["group"]]
        record["scopes"] += 1
        record["finished"] += row.get("finished", 0)
        record["closed"] += row.get("closed", 0)
        record["unknown_scopes"] += int(scope["unknown"])
        kind = scope.get("kind", "ready")
        if kind in record:
            record[kind] += 1
        ids = row.get("july_match_ids", [])
        groups["july"]["finished"] += len(ids)
        groups["july"]["remaining"] += len(ids)
        if ids:
            groups["july"]["scopes"] += 1
            if kind in groups["july"]:
                groups["july"][kind] += 1
    # Closed tail IDs stay in the denominator, rather than making progress
    # disappear as repaired matches leave the pending selection.
    groups["july"]["closed"] = int(inventory.get("july_closed", 0))
    groups["july"]["finished"] += groups["july"]["closed"]
    groups["july"]["unknown_scopes"] = int(inventory.get("july_unresolved", 0))
    for group, record in groups.items():
        record["remaining"] = record["finished"] - record["closed"]
        total = record["finished"]
        record["percent"] = round(100 * record["closed"] / total, 4) if total else None
        record["gate_passed"] = (record["unknown_scopes"] == 0 and
                                 (total == 0 or record["closed"] * 100 >= total * 99))
    return groups


def render_summary(groups: Mapping) -> str:
    parts = []
    for g in GROUPS:
        row = groups[g]
        percent = "нет матчей" if row["percent"] is None else f'{row["percent"]:.2f}%'
        parts.append(f'{g} {LABELS[g]}: {row["closed"]}/{row["finished"]} ({percent}), '
                     f'долг {row["remaining"]}, неизвестно {row["unknown_scopes"]}, '
                     f'pending {row["pending"] + row["mapping_required"]}, '
                     f'parked {row["parked"]}, quarantine {row["quarantined"]}')
    return "• очередь истории SofaScore: " + "; ".join(parts)


def plan(snapshot: Mapping, *, inventory: Mapping, checkpoint_path: str | Path,
         completed=(), failures=None, batch_size=1, dag_run_id="manual",
         denominator=None, release=None, moment=None, max_scope_attempts=3,
         authorized_season_classes=None, slot_count=None, **environment) -> list[dict[str, str]]:
    from datetime import datetime, timezone
    moment = moment or datetime.now(timezone.utc)
    release = state.current_release() if release is None else release
    if snapshot.get("snapshot_id") != state._snapshot_digest(snapshot):
        raise state.CampaignPlanningError("campaign snapshot digest mismatch")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise state.CampaignPlanningError("batch_size must be a positive integer")
    if slot_count is not None and (isinstance(slot_count, bool) or not isinstance(slot_count, int) or not 1 <= slot_count <= 3):
        raise state.CampaignPlanningError("slot_count must be between 1 and 3")
    campaign = snapshot["campaign_id"]
    path = Path(checkpoint_path)
    denominator = denominator or load_denominator()
    with state._state_lock(path):
        checkpoint = _read(path, campaign)
        previous = checkpoint.get("run")
        if previous and previous["run_id"] == dag_run_id:
            return previous.get("workers", previous["plan"])
        run_hash = hashlib.sha256(str(dag_run_id).encode()).hexdigest()
        if run_hash in checkpoint.get("retired_runs", []):
            raise state.CampaignPlanningError("history DagRun reservation was retired")
        if previous and not previous.get("finalized"):
            raise state.CampaignPlanningError("previous history reservation is not finalized")
        if previous:
            checkpoint.setdefault("retired_runs", []).append(hashlib.sha256(previous["run_id"].encode()).hexdigest())
        scopes = classify(snapshot, inventory, denominator)
        for scope in scopes:
            scope["kind"] = _kind(scope, failures or {}, release, moment, max_scope_attempts)
        groups = coverage(scopes, inventory)
        snapshot_ids = {int(s["tournament"]["unique_tournament_id"]) for s in scopes}
        missing_core = sum(denominator.is_core(tid) and tid not in snapshot_ids for tid in denominator.rows)
        for group in ("1", "2"):
            groups[group]["unknown_scopes"] += missing_core
            if missing_core:
                groups[group]["gate_passed"] = False
        active = next((g for g in ("1", "2") if not groups[g]["gate_passed"]), None)
        if active is None:
            active = next((g for g in ("july", "3", "4", "5")
                           if (groups[g]["remaining"] or groups[g]["unknown_scopes"])
                           and (g == "july" or any(s["group"] == g and s["kind"] != "quarantined"
                                                  and (s["unknown"] or s["evidence"].get("closed", 0) < s["evidence"].get("finished", 0))
                                                  for s in scopes))), "5")
        candidates = []
        if isinstance(authorized_season_classes, Mapping):
            raise state.CampaignPlanningError("authorized_season_classes must be class names, not a mapping")
        authorized = set(authorized_season_classes) if authorized_season_classes is not None else None
        for scope in scopes:
            row = scope["evidence"]
            if scope["kind"] != "ready":
                continue
            if active == "july":
                ids = row.get("july_match_ids", [])
                if not ids:
                    continue
            else:
                if scope["group"] != active or (not scope["unknown"] and row.get("closed", 0) == row.get("finished", 0)):
                    continue
                if authorized is not None:
                    from scrapers.sofascore.workload_plan import production_season_shape, season_workload_class, team_count_band
                    s = scope["season"]
                    shape = production_season_shape(season_format={"single_year": "calendar_year"}.get(s["season_format"], s["season_format"]),
                                                    team_count_band=team_count_band(s["team_count"]), max_pages_per_direction=50)
                    if season_workload_class(shape) not in authorized:
                        raise state.CampaignPlanningError("history shape is absent from static policy")
            candidates.append(scope)
        candidates.sort(key=lambda s: (s["depth"], -int(s["season"]["start_year"]),
                                       int(s["tournament"]["unique_tournament_id"]), int(s["season"]["source_season_id"])))
        position = checkpoint["positions"].get(active)
        def rank(scope):
            return [scope["depth"], -int(scope["season"]["start_year"]),
                    int(scope["tournament"]["unique_tournament_id"]), int(scope["season"]["source_season_id"])]
        if position:
            candidates = [s for s in candidates if rank(s) > position] + [s for s in candidates if rank(s) <= position]
        chosen, tournaments, ranks = [], set(), []
        # One tournament per wave, even if batch_size exceeds tournament count.
        for scope in candidates:
            tid = int(scope["tournament"]["unique_tournament_id"])
            if slot_count is None and tid in tournaments:
                continue
            tournaments.add(tid)
            env = state._scope_task_env("capture", snapshot_id=snapshot["snapshot_id"], campaign_id=campaign,
                                        tournament_id=tid, season=scope["season"], lane_env=environment.get("task_env") or {},
                                        snapshot_path=environment.get("snapshot_path", "/opt/airflow/runtime/sofascore/all-men/snapshot.json"),
                                        policy_path=environment.get("policy_path", "/opt/airflow/configs/sofascore/all_mens_campaign.json"),
                                        result_dir=environment.get("result_dir", "/opt/airflow/logs/sofascore-all-men/results"),
                                        workload_artifact=environment.get("workload_artifact", "/opt/airflow/runtime/sofascore/proxy_budget_canary.json"),
                                        dag_run_id=dag_run_id)
            env["SOFASCORE_HISTORY_GROUP"] = active
            if active == "july":
                row_ids = scope["evidence"]["july_match_ids"]
                replay = set(scope["evidence"].get("july_raw_match_ids", []))
                ids = sorted(row_ids, key=lambda i: (i not in replay, int(i)))[:25]
                env.update(SOFASCORE_HISTORY_PHASE="matches", SOFASCORE_HISTORY_SEASON_EVIDENCE="bronze",
                           SOFASCORE_HISTORY_MATCH_IDS_JSON=json.dumps(ids))
            chosen.append(env)
            ranks.append(rank(scope))
            if slot_count is None:
                checkpoint["positions"][active] = rank(scope)
            if slot_count is None and len(chosen) >= batch_size:
                break
        checkpoint.update(snapshot_id=snapshot["snapshot_id"], observed_at=inventory["observed_at"], july_cohort=inventory.get("july_cohort", []),
                          verified_schedules=inventory.get("verified_schedules", []),
                          groups=groups, summary=render_summary(groups), active_group=active,
                          run={"run_id": dag_run_id, "plan": chosen, "finalized": not chosen,
                               "inventory_digest": hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest(),
                               "plan_digest": hashlib.sha256(json.dumps(chosen, sort_keys=True).encode()).hexdigest()})
        if slot_count is not None:
            workers = [dict(env, SOFASCORE_HISTORY_SLOT=str(index),
                            SOFASCORE_HISTORY_CONTROLLER=str(path), SOFASCORE_HISTORY_RUN_ID=dag_run_id)
                       for index, env in enumerate(chosen[:slot_count])]
            checkpoint["run"].update(mode="slots", workers=workers, ranks=ranks,
                                     cursor=0, slots={}, items={},
                                     deadline=min(moment + timedelta(hours=6),
                                                  datetime.fromisoformat(environment["run_deadline"])
                                                  if environment.get("run_deadline") else moment + timedelta(hours=6)).isoformat())
        _write(path, checkpoint)
        return workers if slot_count is not None else chosen


def finalize(path: str | Path, *, campaign_id: str, run_id: str) -> None:
    path = Path(path)
    with state._state_lock(path):
        value = _read(path, campaign_id)
        run = value.get("run")
        if not run or run["run_id"] != run_id:
            raise state.CampaignPlanningError("history finalize reservation mismatch")
        if not run["finalized"]:
            if run.get("mode") == "slots" and (any(i is not None for i in run["slots"].values())
                                                or any(not i["accounted"] for i in run["items"].values())):
                raise state.CampaignPlanningError("history slots are not accounted")
            run["finalized"] = True
            _write(path, value)


def read_summary(path: str | Path, campaign_id: str) -> dict:
    return _read(Path(path), campaign_id)


def _slot_run(document, run_id):
    run = document.get("run")
    if not run or run["run_id"] != run_id or run.get("mode") != "slots" or run["finalized"]:
        raise state.CampaignPlanningError("history slot reservation mismatch")
    return run


def claim_scope(path, *, campaign_id, run_id, slot, now=None):
    """FIFO reservation; a restart of a slot receives its existing scope."""
    from datetime import datetime, timezone
    now = now or datetime.now(timezone.utc)
    path = Path(path)
    with state._state_lock(path):
        document = _read(path, campaign_id)
        run = _slot_run(document, run_id)
        slot = str(slot)
        if slot not in {str(i) for i in range(len(run["workers"]))}:
            raise state.CampaignPlanningError("unknown history slot")
        active = run["slots"].get(slot)
        if active is not None:
            return int(active), run["plan"][run["items"][str(active)]["plan_index"]]
        if now >= datetime.fromisoformat(run["deadline"]):
            return None
        issued = {item["plan_index"] for item in run["items"].values()}
        busy = {run["plan"][run["items"][str(index)]["plan_index"]]["SOFASCORE_TOURNAMENT_ID"]
                for index in run["slots"].values() if index is not None}
        next_plan = next((i for i, env in enumerate(run["plan"])
                          if i not in issued and env["SOFASCORE_TOURNAMENT_ID"] not in busy), None)
        if next_plan is None:
            return None
        index = run["cursor"]
        run["cursor"] += 1
        run["slots"][slot] = index
        run["items"][str(index)] = {"slot": slot, "plan_index": next_plan, "started_at": now.isoformat(),
                                     "attempts": 0, "outcome": None, "accounted": False}
        document["positions"][document["active_group"]] = run["ranks"][next_plan]
        _write(path, document)
        return index, run["plan"][next_plan]


def record_scope(path, *, campaign_id, run_id, slot, index, outcome=None, accounted=False,
                 started_attempt=False):
    """Persist outcome before accounting; retain the slot until accounting succeeds."""
    from datetime import datetime, timezone
    path = Path(path)
    with state._state_lock(path):
        document = _read(path, campaign_id)
        run = _slot_run(document, run_id)
        item = run["items"].get(str(index))
        if item is None or item["slot"] != str(slot):
            raise state.CampaignPlanningError("history slot owner mismatch")
        if item["accounted"]:
            return
        if run["slots"].get(str(slot)) != index:
            raise state.CampaignPlanningError("history slot active scope mismatch")
        if started_attempt:
            item["attempts"] += 1
        if outcome is not None:
            if item["outcome"] is not None and item["outcome"] != outcome:
                raise state.CampaignPlanningError("history scope outcome is immutable")
            item["outcome"] = outcome
            item["finished_at"] = datetime.now(timezone.utc).isoformat()
        if accounted:
            if item["outcome"] is None:
                raise state.CampaignPlanningError("history scope has no outcome")
            item["accounted"] = True
            run["slots"][str(slot)] = None
        _write(path, document)


def account_scope(environment, outcome, *, state_path, failures_path, release):
    """Idempotent recovery of the existing v1 state/failure memory."""
    campaign = environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"]
    scope_key = environment["SOFASCORE_SCOPE_KEY"]
    run_id = environment["SOFASCORE_SCOPE_RUN_ID"]
    if outcome["status"] == "not_started":
        return
    if outcome["status"] == "success":
        if environment.get("SOFASCORE_HISTORY_GROUP") != "july":
            state.mark_completed(state_path, campaign_id=campaign, scope_key=scope_key)
        state.clear_failed(failures_path, campaign_id=campaign, scope_key=scope_key)
        state.mark_completed_rejects(failures_path, campaign_id=campaign, scope_key=scope_key,
                                     rejected_endpoints=outcome.get("rejected_endpoints", 0),
                                     run_id=run_id, release=release)
    else:
        state.mark_failed(failures_path, campaign_id=campaign, scope_key=scope_key,
                          run_id=run_id, reason=outcome.get("reason"),
                          source_requests=outcome.get("source_requests"), release=release,
                          season_identity=environment.get("SOFASCORE_SEASON_ALIGNMENT_IDENTITY"))
