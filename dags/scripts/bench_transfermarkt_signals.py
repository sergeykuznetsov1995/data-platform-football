#!/usr/bin/env python3
"""GET-only #1393 detector probe/parity experiment; never imports a writer.

Run in a disposable source-image container with the worktree mounted read-only
at /workspace and task evidence mounted at /evidence. Only the existing gateway
URL/token are inherited, never a proxy-pool file or production raw-store URL.

Probe: --mode probe --ids 8198,28003 --evidence-dir /evidence/probe
Cohort: --mode cohort --sample /evidence/cohort-template.json --evidence-dir /evidence/cohort
Parity: --mode parity --sample /evidence/cohort/cohort-sample.json
        --evidence-dir /evidence/day1
Repeat the same parity command for the next <=45 minute portion. Different
observations share --experiment-dir (default evidence-dir.parent); budgets apply
across portions AND days. An interrupted process fails closed on resume.

sample.json: {"clubs": [{"club_id":"281", "saison_id":2026,
 "scope":"GB1/2026", "groups":["top_league"], "player_ids":["8198"]}]}
The public tmapi.parse_player_signals contract is grounded in saved 300-ID
source raw and rechecked by the initial probe. Neither missing cost nor national
team membership is guessed from a generic JSON field map.
"""
from __future__ import annotations

from contextlib import contextmanager
import signal

import argparse
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlencode, urlsplit
import uuid
from zoneinfo import ZoneInfo
import re

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.append(str(ROOT / "dags"))

MIB = 1024 * 1024
MAX_ATTEMPTS = 4600
MAX_BYTES = 256 * MIB
MAX_PORTION_SECONDS = 45 * 60
# Existing gateway traffic namespace; the unique run/task labels identify an
# authorized measurement, never an Airflow DagRun or a collector trigger.
MEASUREMENT_DAG_ID = "dag_discover_transfermarkt_registry"
COUNTERS = ("request_attempts", "provider_metered_bytes", "decoded_response_body_bytes")
GROUPS = {"top_league", "lower_league", "calendar", "cup"}


class ProbeGateError(ValueError):
    """Source schema or evidence does not establish detector parity."""


def _write(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n")
    os.replace(temporary, path)


def _read(path: Path) -> dict:
    return json.loads(path.read_text())


def _id(value) -> str:
    value = str(value).strip()
    if not value.isdigit() or int(value) <= 0:
        raise ProbeGateError("expected positive source ID")
    return value


def player_url(ids: list[str]) -> str:
    if not ids or len(ids) > 300 or len(set(ids)) != len(ids):
        raise ProbeGateError("tmapi batch needs 1..300 unique IDs")
    return "https://tmapi.transfermarkt.technology/players?" + urlencode(
        [("ids[]", _id(item)) for item in ids]
    )


def parse_players(payload, expected: list[str]):
    from scrapers.transfermarkt.tmapi import parse_player_signals, TmapiSchemaError
    try:
        return parse_player_signals(payload, expected_ids=expected)
    except TmapiSchemaError as exc:
        raise ProbeGateError(str(exc)) from exc


def recheck_sample(baseline: dict, current: dict, limit: int = 64):
    """A bounded measured repeat of actual changed IDs; never manufactures one."""
    before = {row["player_id"]: row for row in baseline["qualification_observation"]["players"]}
    after = {row["player_id"]: row for row in current["qualification_observation"]["players"]}
    if set(before) != set(after) or baseline["cohort"] != current["cohort"]:
        raise ProbeGateError("recheck needs the same exact measured cohort")
    changed = [player for player in sorted(before, key=int) if before[player]["tmapi"] != after[player]["tmapi"]]
    selected = changed[:limit]
    clubs = {}
    scope = baseline["cohort"]["scopes"][0]
    season = int(scope.split("/")[1])
    for player in selected:
        current_clubs = after[player]["tmapi"]["clubs"]
        if not current_clubs:
            raise ProbeGateError("changed player has no proven current club for plus/1 recheck")
        club = _id(current_clubs[0])
        clubs.setdefault(club, {"club_id": club, "saison_id": season, "scope": scope, "groups": baseline["cohort"]["groups"],
                                "squad_url": f"https://www.transfermarkt.com/club/kader/verein/{club}/plus/1", "player_ids": []})["player_ids"].append(player)
    return {"clubs": list(clubs.values()), "selected_changed_ids": selected, "detected_changed_ids": changed,
            "unrechecked_changed_ids": changed[limit:], "cohort": baseline["cohort"]}


def schema_shape(value, depth: int = 0):
    """Bounded field/type summary; actual source values live only in local raw."""
    if depth >= 4:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(key): schema_shape(item, depth + 1) for key, item in list(value.items())[:80]}
    if isinstance(value, list):
        return {"type": "list", "length": len(value), "item": schema_shape(value[0], depth + 1) if value else None}
    return type(value).__name__


def plan_tasks(sample: dict, *, roster_only=False) -> tuple[list[dict], list[str], set[str]]:
    clubs = sample.get("clubs")
    if not isinstance(clubs, list) or not clubs:
        raise ProbeGateError("sample needs explicit current clubs")
    tasks, players, groups, seen_clubs, participant_tasks = [], [], set(), set(), {}
    for club in clubs:
        club_id = _id(club["club_id"])
        season = int(club["saison_id"])
        if not 1800 <= season <= 2199 or not str(club.get("scope", "")).strip():
            raise ProbeGateError("sample needs valid saison_id and scope")
        if roster_only:
            from scrapers.transfermarkt.denominator import load_denominator
            from scrapers.transfermarkt.tmapi import competition_clubs_url
            scope = str(club["scope"])
            parts = scope.split("/")
            row = load_denominator().row(parts[0]) if len(parts) == 2 else None
            if not row or not row.is_core or parts[1] != str(season) or row.current_saison_id != season:
                raise ProbeGateError("cohort template scope is not a live current denominator edition")
            participant_tasks.setdefault(scope, {
                "key": f"participants/{scope}", "kind": "participants", "scope": scope,
                "competition_id": parts[0], "saison_id": season, "json": True,
                "url": competition_clubs_url(parts[0], season),
            })
        identity = (club_id, season)
        if identity in seen_clubs:
            raise ProbeGateError("duplicate club sample")
        seen_clubs.add(identity)
        groups.update(club.get("groups", []))
        ids = [] if roster_only else [_id(item) for item in club["player_ids"]]
        if (not ids and not roster_only) or len(set(ids)) != len(ids):
            raise ProbeGateError("sample club needs distinct player IDs")
        players.extend(ids)
        url = club.get("squad_url") or (
            f"https://www.transfermarkt.com/club/kader/verein/{club_id}/plus/1" if roster_only else
            f"https://www.transfermarkt.com/club/kader/verein/{club_id}/saison_id/{season}/plus/1"
        )
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.netloc != "www.transfermarkt.com" or parsed.query or parsed.fragment
                or not re.fullmatch(rf"/[^/]+/kader/verein/{club_id}(?:/saison_id/{season})?/plus/1", parsed.path)):
            raise ProbeGateError("sample squad URL does not match official club/season")
        if roster_only and "/saison_id/" in parsed.path:
            raise ProbeGateError("cohort bootstrap requires current club URL without scope saison_id")
        tasks.append({"key": f"squad/{club_id}/{season}", "kind": "squad", "club_id": club_id,
                      "scope": club["scope"], "ids": ids, "json": False,
                      "url": url, "groups": list(club.get("groups", [])), "saison_id": season})
    if roster_only:
        return list(participant_tasks.values()) + tasks, players, groups
    players = list(dict.fromkeys(players))
    for offset in range(0, len(players), 300):
        ids = players[offset:offset + 300]
        tasks.append({"key": f"players/{offset}", "kind": "players", "ids": ids,
                      "scope": "parity/players", "json": True, "url": player_url(ids)})
    for player in players:
        for kind, path in (("mv", "marketValueDevelopment/graph"), ("transfers", "transferHistory/list")):
            tasks.append({"key": f"{kind}/{player}", "kind": kind, "player_id": player,
                          "scope": "parity/careers", "json": True,
                          "url": f"https://www.transfermarkt.com/ceapi/{path}/{player}"})
    return tasks, players, groups


def cohort_report(tasks, results, store, *, player_target=1000, min_scopes=20):
    """Build IDs exclusively from fresh current-club tables, before tmapi/ceapi.

    Club season and the supplied scope season are separate evidence. This
    experiment proves membership through a separate fresh participant answer.
    """
    from bs4 import BeautifulSoup
    from scrapers.transfermarkt.scraper import _parse_squad_page
    from scrapers.transfermarkt.tmapi import parse_competition_clubs, TmapiSchemaError
    candidates, notes, participants, membership_errors = [], [], {}, []
    for task in tasks:
        if task["key"] not in results:
            membership_errors.append({"scope": task["scope"], "reason": "task_raw_not_proven", "key": task["key"]})
            notes.append({"scope": task["scope"], "kind": task["kind"], "key": task["key"], "raw_proven": False})
            continue
        body, record = store.load_capture(results[task["key"]]["raw_capture_id"])
        if record.url != task["url"] or record.status_code != 200:
            raise ProbeGateError("cohort raw identity mismatch")
        if task["kind"] == "participants":
            try:
                ids = parse_competition_clubs(json.loads(body), competition_id=task["competition_id"], saison_id=task["saison_id"])
                participants[task["scope"]] = set(ids)
                notes.append({"scope": task["scope"], "kind": "participants", "participant_ids": list(ids),
                              "raw_capture_id": record.capture_id, "raw_fetched_at": record.fetched_at})
            except (TmapiSchemaError, ValueError):
                membership_errors.append({"scope": task["scope"], "reason": "participants_schema_not_proven"})
            continue
        html = body.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        selected = soup.select_one('select[name="saison_id"] option[selected]')
        headers = {node.get_text(" ", strip=True).lower() for node in soup.select("table.items thead th")}
        rows = _parse_squad_page(html, task["club_id"])
        selected_id = selected.get("value") if selected else None
        membership = task["club_id"] in participants.get(task["scope"], set())
        notes.append({"scope": task["scope"], "club_id": task["club_id"], "scope_saison_id": task["saison_id"],
                      "club_selected_saison_id": selected_id, "club_selected_label": selected.get_text(strip=True) if selected else None,
                      "scope_season_matches_club": str(task["saison_id"]) == selected_id,
                      "raw_capture_id": record.capture_id, "raw_fetched_at": record.fetched_at,
                      "contract_header": "contract" in headers, "roster_players": len(rows),
                      "scope_membership_confirmed": membership})
        if not membership:
            membership_errors.append({"scope": task["scope"], "club_id": task["club_id"], "reason": "template_club_not_a_proven_current_participant"})
            continue
        if not rows or "contract" not in headers or selected_id is None:
            continue
        ids = sorted({str(row["player_id"]) for row in rows}, key=int)
        candidates.append({"club_id": task["club_id"], "saison_id": task["saison_id"], "scope": task["scope"],
                           "groups": task["groups"], "squad_url": task["url"], "player_ids": [],
                           "cohort_raw_capture_id": record.capture_id, "cohort_raw_fetched_at": record.fetched_at,
                           "club_selected_saison_id": selected_id, "available_ids": ids})
    # Round-robin ensures each supplied scope participates before the target
    # truncates the sample; shared players buy only one pair of career GETs.
    seen = set()
    while len(seen) < player_target:
        changed = False
        for candidate in candidates:
            while candidate["available_ids"]:
                player = candidate["available_ids"].pop(0)
                if player in seen:
                    continue
                candidate["player_ids"].append(player)
                seen.add(player)
                changed = True
                break
            if len(seen) >= player_target:
                break
        if not changed:
            break
    clubs = [{key: value for key, value in candidate.items() if key != "available_ids"}
             for candidate in candidates if candidate["player_ids"]]
    scopes = {club["scope"] for club in clubs}
    groups = {group for club in clubs for group in club["groups"]}
    return {"clubs": clubs, "unique_players": len(seen), "scopes": sorted(scopes), "groups": sorted(groups),
            "missing_groups": sorted(GROUPS - groups), "cohort_notes": notes,
            "membership_errors": membership_errors,
            "cohort_gate": len(seen) >= player_target and len(scopes) >= min_scopes and GROUPS <= groups and not membership_errors,
            "detector_gate": False, "scope_membership_freshness_proven": not membership_errors}



def parity_report(tasks: list[dict], results: dict, store, players: list[str], groups: set[str]) -> dict:
    from scrapers.transfermarkt.scraper import _parse_squad_page, _parse_mv_history, _parse_transfers
    tmapi, squads, mv, transfers, observed = {}, {}, {}, {}, []
    api_raw, squad_raw = {}, {}
    for task in tasks:
        proof = results[task["key"]]
        body, record = store.load_capture(proof["raw_capture_id"])
        if record.url != task["url"] or record.status_code != 200:
            raise ProbeGateError("raw evidence identity mismatch")
        payload = json.loads(body) if task["json"] else body.decode("utf-8")
        observed.append(record.fetched_at)
        raw = {"capture_id": proof["raw_capture_id"], "body_sha256": getattr(record, "content_hash", None),
               "url": record.url, "fetched_at": record.fetched_at}
        source_day = datetime.fromisoformat(record.fetched_at.replace("Z", "+00:00")).astimezone(ZoneInfo("Europe/Berlin")).date()
        if task["kind"] == "players":
            tmapi.update(parse_players(payload, task["ids"]))
            api_raw.update({player: raw for player in task["ids"]})
        elif task["kind"] == "squad":
            from bs4 import BeautifulSoup
            header = BeautifulSoup(payload, "html.parser").select("table.items thead th")
            if "contract" not in {item.get_text(" ", strip=True).lower() for item in header}:
                raise ProbeGateError("full current squad lacks contract header")
            rows = {str(row["player_id"]): row for row in _parse_squad_page(payload, task["club_id"])}
            for player in task["ids"]:
                if player not in rows:
                    raise ProbeGateError("sampled player is absent from full squad")
                squads.setdefault(player, []).append((task["club_id"], rows[player]))
                squad_raw[(player, task["club_id"])] = raw
        elif task["kind"] == "mv":
            if not isinstance(payload, dict) or not isinstance(payload.get("list"), list):
                raise ProbeGateError("ceapi market-value list missing")
            mv[task["player_id"]] = (_parse_mv_history(payload, task["player_id"]), source_day, raw, len(payload["list"]))
        else:
            if not isinstance(payload, dict) or not isinstance(payload.get("transfers"), list):
                raise ProbeGateError("ceapi transfers list missing")
            transfers[task["player_id"]] = (_parse_transfers(payload, task["player_id"]), source_day, raw, len(payload["transfers"]))
    failures, comparisons, qualification_rows = [], [], []
    empty_counts = {"mv": 0, "transfers": 0}
    for player in players:
        api = tmapi[player]
        value, contract = api.market_value_eur, api.contract_until
        clubs = api.club_ids
        matching = [(club_id, row) for club_id, row in squads[player] if club_id in clubs]
        issues = []
        if not matching:
            issues.append("tmapi_club_vs_squad")
        else:
            _, row = matching[0]
            if value != row.get("market_value_eur"):
                issues.append("tmapi_value_vs_squad")
            if contract != (row["contract_until"].isoformat() if row.get("contract_until") else None):
                issues.append("tmapi_contract_vs_squad")
        mv_rows, mv_day, mv_raw, mv_raw_count = mv[player]
        transfer_rows, transfer_day, transfer_raw, transfer_raw_count = transfers[player]
        history = [row for row in mv_rows if row.get("mv_date") and row["mv_date"] <= mv_day]
        if history:
            latest_mv = max(history, key=lambda row: row["mv_date"])
            if latest_mv["value_eur"] != value:
                issues.append("tmapi_value_vs_ceapi")
            if latest_mv["mv_date"].isoformat() != api.market_value_date:
                issues.append("tmapi_value_date_vs_ceapi")
        moves = [row for row in transfer_rows if row.get("transfer_date") and row["transfer_date"] <= transfer_day and not row.get("is_upcoming")]
        if moves and max(moves, key=lambda row: row["transfer_date"])["to_club_id"] not in clubs:
            issues.append("tmapi_club_vs_ceapi")
        comparisons.append({"player_id": player, "fields": {"value": value, "value_date": api.market_value_date, "value_present": api.market_value_present, "contract": contract, "clubs": clubs},
                            "mv_rows": len(mv_rows), "transfer_rows": len(transfer_rows), "issues": issues})
        chosen_club, chosen_row = matching[0] if matching else squads[player][0]
        mv_ref = ({"status": "compared", "value": latest_mv["value_eur"], "value_date": latest_mv["mv_date"].isoformat(), "raw_rows": mv_raw_count}
                  if history else {"status": "authoritative_empty" if mv_raw_count == 0 else "unknown", "raw_rows": mv_raw_count})
        transfer_ref = ({"status": "compared", "club_id": max(moves, key=lambda row: row["transfer_date"])["to_club_id"], "raw_rows": transfer_raw_count}
                        if moves else {"status": "authoritative_empty" if transfer_raw_count == 0 else "unknown", "raw_rows": transfer_raw_count})
        empty_counts["mv"] += int(mv_ref["status"] == "authoritative_empty")
        empty_counts["transfers"] += int(transfer_ref["status"] == "authoritative_empty")
        qualification_rows.append({"player_id": player, "tmapi": {**comparisons[-1]["fields"], "clubs": list(clubs)},
                                   "plus1": {"value": chosen_row.get("market_value_eur"), "contract": chosen_row["contract_until"].isoformat() if chosen_row.get("contract_until") else None, "club_id": chosen_club},
                                   "ceapi": {"mv": mv_ref, "transfers": transfer_ref},
                                   "raw": {"tmapi": api_raw[player], "plus1": squad_raw[(player, chosen_club)], "mv": mv_raw, "transfers": transfer_raw}})
        if issues:
            failures.append({"player_id": player, "issues": issues})
    day_msk = datetime.fromisoformat(max(observed).replace("Z", "+00:00")).astimezone(ZoneInfo("Europe/Moscow")).date().isoformat()
    return {"players_checked": len(players), "groups": sorted(groups), "comparisons": comparisons,
            "cohort": {"player_ids": sorted(players, key=int), "groups": sorted(groups),
                       "scope_basis": "configured_denominator", "source_current_editions_proven": False,
                       "scopes": sorted({task["scope"] for task in tasks if task["kind"] == "squad"})},
            "qualification_observation": {"schema_version": "tm-signal-parity-observation-v1", "players": qualification_rows,
                                          "missing_ids": [], "unknown_ids": [], "observed_from": min(observed), "observed_to": max(observed),
                                          "counts": {"expected": len(players), "compared": len(qualification_rows), "missing": 0, "unknown": 0},
                                          "day_msk": day_msk, "ceapi_empty_counts": empty_counts},
            "mismatches": failures, "observed_from": min(observed), "observed_to": max(observed),
            "observation_day_msk": day_msk, "career_date_rule": "source calendar Europe/Berlin at raw fetched_at; original raw is UTC",
            "ceapi_missing_comparisons": {"value": sum(not mv[player][0] for player in players),
                                          "club": sum(not transfers[player][0] for player in players)},
            "field_parity_gate": len(players) >= 1000 and GROUPS <= groups and not failures,
            "detector_gate": False, "remaining_gate": "second-day real changes and freshness evidence required"}


def _client(evidence: Path, remaining: dict, portion_id: str):
    from scrapers.transfermarkt.client import ProxyFilterLeaseProvider, TransfermarktHttpClient
    from scrapers.transfermarkt.models import SharedTrafficLedger
    from scrapers.transfermarkt.raw_store import RawResponseStore
    from scrapers.utils.rate_limiter import RateLimiter
    if not os.environ.get("TM_PROXY_CONTROL_URL") or not os.environ.get("TM_PROXY_CONTROL_TOKEN"):
        raise ProbeGateError("metered TM gateway URL/token required")
    ledger = SharedTrafficLedger(hard_provider_bytes=remaining["provider_metered_bytes"],
                                  soft_provider_bytes=max(1, remaining["provider_metered_bytes"] - MIB))
    client = TransfermarktHttpClient(
        lease_provider=ProxyFilterLeaseProvider(os.environ["TM_PROXY_CONTROL_URL"]),
        traffic_ledger=ledger, raw_store=RawResponseStore.from_uri((evidence / "raw").as_uri()),
        require_raw_store=True, rate_limiter=RateLimiter(max_requests=12, window_seconds=60, burst_size=1),
        request_deadline_monotonic=remaining["deadline_monotonic"],
        lease_metadata={"dag_id": MEASUREMENT_DAG_ID, "run_id": portion_id, "task_id": "signal_probe_1393", "scope": "signal-experiment"},
    )
    client.begin_request_scope(request_attempt_budget=remaining["request_attempts"])
    client.set_cycle_decoded_body_budget(remaining["decoded_response_body_bytes"])
    return client


def _now_utc():
    return datetime.now(timezone.utc)


def run(args, *, client_factory=_client, monotonic=time.monotonic, now_fn=None) -> int:
    if args.mode == "qualify":
        from scrapers.transfermarkt.signal_qualification import build_qualification, QualificationError
        evidence = Path(args.evidence_dir).resolve()
        evidence.mkdir(parents=True, exist_ok=True)
        try:
            wrapper = build_qualification(args.day1, args.day2, evidence / "evidence.json", recheck=getattr(args, "recheck_report", None))
            _write(evidence / "qualification.json", wrapper)
            return 0
        except QualificationError as exc:
            _write(evidence / "report.json", {"detector_gate": False, "qualification_error": str(exc)})
            return 2
    from dags.utils.transfermarkt_current_timetable import available_work_seconds, next_admitted_time
    now = (now_fn or _now_utc)()
    admitted_seconds = available_work_seconds(now, args.portion_seconds)
    evidence = Path(args.evidence_dir).resolve()
    if admitted_seconds == 0:
        evidence.mkdir(parents=True, exist_ok=True)
        _write(evidence / "report.json", {"mode": args.mode, "detector_gate": False, "waiting_for": "delivery_quiet_window",
                                           "resume_after_utc": next_admitted_time(now).isoformat(), "complete": False})
        return 3
    experiment = Path(args.experiment_dir).resolve() if args.experiment_dir else evidence.parent
    evidence.mkdir(parents=True, exist_ok=True)
    experiment.mkdir(parents=True, exist_ok=True)
    with (experiment / "signal-probe.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        ledger_path = experiment / "signal-probe-ledger.json"
        ledger = _read(ledger_path) if ledger_path.exists() else {"version": 1, "active": False, "totals": dict.fromkeys(COUNTERS, 0)}
        if ledger.get("active"):
            raise ProbeGateError("interrupted experiment: reconcile gateway lease bytes before any resume")
        if args.mode != "probe" and not ledger.get("packet_schema_gate"):
            raise ProbeGateError("a successful raw-backed initial player packet probe is required before the large experiment")
        if args.mode == "probe":
            ids = [_id(item) for item in args.ids.split(",") if item]
            tasks = [{"key": "probe", "kind": "players", "scope": "probe", "url": player_url(ids), "json": True}]
            players, groups, mapping = ids, set(), {}
        elif args.mode == "cohort":
            tasks, players, groups = plan_tasks(_read(Path(args.sample)), roster_only=True)
            mapping = {}
        elif args.mode == "recheck":
            selected_sample = recheck_sample(_read(Path(args.day1)), _read(Path(args.day2)), args.recheck_players)
            if not selected_sample["selected_changed_ids"]:
                _write(evidence / "report.json", {"mode": "recheck", "complete": True, "detector_gate": False,
                                                   "actual_changed_count": 0, "qualification_error": "no actual changed IDs; no request was made"})
                return 2
            tasks, players, groups = plan_tasks(selected_sample)
            mapping = {"parser": "scrapers.transfermarkt.tmapi.parse_player_signals", "changed_ids": players}
        else:
            sample = _read(Path(args.sample))
            tasks, players, groups = plan_tasks(sample)
            mapping = {"parser": "scrapers.transfermarkt.tmapi.parse_player_signals"}
        identity = hashlib.sha256(json.dumps({"tasks": tasks, "mapping": mapping}, sort_keys=True).encode()).hexdigest()
        state_path = evidence / "state.json"
        state = _read(state_path) if state_path.exists() else {"identity": identity, "results": {}, "tries": {}, "attempt_records": [], "failures": {}}
        state.setdefault("failures", {})
        if state["identity"] != identity:
            raise ProbeGateError("observation input changed; create a new evidence directory")
        remaining = {key: (args.max_attempts if key == "request_attempts" else args.max_bytes) - ledger["totals"][key] for key in COUNTERS}
        if remaining["request_attempts"] <= 0 or min(remaining["provider_metered_bytes"], remaining["decoded_response_body_bytes"]) <= MIB:
            raise ProbeGateError("experiment budget exhausted")
        portion_id = "tm1393-measurement-" + uuid.uuid4().hex
        started = monotonic()
        remaining["deadline_monotonic"] = started + admitted_seconds
        client = client_factory(evidence, remaining, portion_id)
        ledger["active"] = True
        _write(ledger_path, ledger)
        report = {"mode": args.mode, "identity": identity, "portion_id": portion_id, "detector_gate": False}
        exit_code = 3
        try:
            for task in tasks:
                if task["key"] in state["results"]:
                    continue
                if monotonic() - started >= admitted_seconds - 120:
                    break
                tries = state["tries"].get(task["key"], 0)
                if tries >= args.endpoint_attempts:
                    if args.mode == "cohort":
                        continue
                    raise ProbeGateError("endpoint attempt limit exhausted; no schema gate")
                state["tries"][task["key"]] = tries + 1
                _write(state_path, state)
                outcome = client.fetch(task["url"], as_json=task["json"], max_attempts=1,
                                       label=task["kind"], context={"cycle_id": portion_id, "scope_id": task["scope"]})
                if not outcome.is_success or not outcome.raw_capture_id:
                    report["failure"] = {"key": task["key"], "status": outcome.status.value, "http_status": outcome.status_code}
                    state["failures"][task["key"]] = report["failure"]
                    _write(state_path, state)
                    if args.mode == "cohort":
                        continue
                    break
                state["failures"].pop(task["key"], None)
                state["results"][task["key"]] = {"raw_capture_id": outcome.raw_capture_id,
                                                    "raw_fetched_at": outcome.raw_fetched_at}
                _write(state_path, state)
            terminal = all(task["key"] in state["results"] or (
                args.mode == "cohort" and state["tries"].get(task["key"], 0) >= args.endpoint_attempts
            ) for task in tasks)
            if terminal:
                if args.mode == "probe":
                    body, record = client._raw_store.load_capture(state["results"]["probe"]["raw_capture_id"])
                    payload = json.loads(body)
                    signals = parse_players(payload, players)
                    ledger["packet_schema_gate"] = True
                    ledger["schema_probe"] = {"evidence_dir": str(evidence), "player_ids": players,
                                              "raw_capture_id": state["results"]["probe"]["raw_capture_id"],
                                              "raw_fetched_at": getattr(record, "fetched_at", None)}
                    report.update({"http_status": record.status_code, "packet_schema_gate": True,
                                   "parsed_players": len(signals), "shape": schema_shape(payload),
                                   "remaining_gate": "inspect raw schema; probe is not >=1000-player acceptance"})
                    exit_code = 0 if record.status_code == 200 else 2
                elif args.mode == "cohort":
                    cohort = cohort_report(tasks, state["results"], client._raw_store,
                                           player_target=args.player_target, min_scopes=args.min_scopes)
                    _write(evidence / "cohort-sample.json", cohort)
                    report.update({key: value for key, value in cohort.items() if key != "clubs"})
                    exit_code = 0 if cohort["cohort_gate"] else 2
                else:
                    report.update(parity_report(tasks, state["results"], client._raw_store, players, groups))
                    if args.mode == "recheck":
                        report["original_cohort"] = selected_sample["cohort"]
                        report["unrechecked_changed_ids"] = selected_sample["unrechecked_changed_ids"]
                        report["selected_changed_ids"] = selected_sample["selected_changed_ids"]
                        exit_code = 0 if not report["mismatches"] else 2
                    else:
                        exit_code = 0 if report["field_parity_gate"] else 2
        except Exception as exc:
            report["error_type"] = type(exc).__name__
            from scrapers.transfermarkt.models import ProxyRequiredError
            if isinstance(exc, ProxyRequiredError):
                from scrapers.transfermarkt.client import redact_sensitive
                report["error_detail"] = redact_sensitive(exc)
            exit_code = 2
        finally:
            # Never clear an active marker unless close and provider accounting
            # succeed. A killed process cannot quietly discard paid attempts.
            client.close()
            stats = client.get_traffic_stats()
            if not stats.get("provider_metering_available"):
                raise ProbeGateError("provider accounting unavailable")
            for key in COUNTERS:
                ledger["totals"][key] += int(stats[key])
            state["attempt_records"].extend(client.get_raw_attempt_records())
            state.setdefault("portions", []).append({"portion_id": portion_id, "traffic": stats})
            _write(state_path, state)
            ledger["active"] = False
            _write(ledger_path, ledger)
            report.update({"completed_tasks": len(state["results"]), "planned_tasks": len(tasks),
                           "experiment_totals": ledger["totals"], "traffic": stats,
                           "task_failures": state["failures"],
                           "complete": len(state["results"]) == len(tasks)})
            _write(evidence / "report.json", report)
        return exit_code


@contextmanager
def _cli_portion_alarm(seconds):
    """Interrupt a stuck SQL-free CLI portion, leaving time to settle its lease."""
    previous_handler = signal.getsignal(signal.SIGALRM)
    if signal.getitimer(signal.ITIMER_REAL)[0] > 0:
        raise ProbeGateError('another absolute deadline already owns this process')

    def expired(signum, frame):
        raise ProbeGateError('portion wall clock deadline reached; reconcile any unsettled lease')

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("probe", "cohort", "parity", "recheck", "qualify"), required=True)
    parser.add_argument("--ids", default="")
    parser.add_argument("--sample")
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--experiment-dir")
    parser.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS)
    parser.add_argument("--max-bytes", type=int, default=MAX_BYTES)
    parser.add_argument("--portion-seconds", type=int, default=MAX_PORTION_SECONDS)
    parser.add_argument("--endpoint-attempts", type=int, default=2)
    parser.add_argument("--player-target", type=int, default=1000)
    parser.add_argument("--min-scopes", type=int, default=20)
    parser.add_argument("--day1")
    parser.add_argument("--day2")
    parser.add_argument("--recheck-report")
    parser.add_argument("--recheck-players", type=int, default=64)
    args = parser.parse_args()
    if not 1 <= args.max_attempts <= MAX_ATTEMPTS or not MIB < args.max_bytes <= MAX_BYTES:
        parser.error("limits must fit the agreed 4600 attempts / 256 MiB experiment")
    if not 121 <= args.portion_seconds <= MAX_PORTION_SECONDS or not 1 <= args.endpoint_attempts <= 2:
        parser.error("portion <=45 minutes and endpoint attempts <=2 required")
    if args.mode == "parity" and not args.sample:
        parser.error("parity needs fresh cohort sample")
    if args.mode == "cohort" and not args.sample:
        parser.error("cohort needs known club/scope templates; IDs come from fresh current club pages")
    if args.mode in {"qualify", "recheck"} and (not args.day1 or not args.day2):
        parser.error("qualification and measured recheck need both actual day reports")
    if not 1 <= args.recheck_players <= 64:
        parser.error("changed recheck is limited to 1..64 IDs within the same experiment budget")
    if not 1000 <= args.player_target <= 1100 or not 20 <= args.min_scopes <= 30:
        parser.error("cohort must target 1000..1100 players / 20..30 scopes within the experiment")
    try:
        if args.mode == 'qualify':
            return run(args)
        # The transport's per-read timeout does not bound a continuously
        # streaming body. The isolated CLI also owns an absolute alarm.
        with _cli_portion_alarm(args.portion_seconds - 15):
            return run(args)
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__, "detector_gate": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
