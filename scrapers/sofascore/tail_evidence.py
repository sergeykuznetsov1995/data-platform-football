"""Offline assessment of saved daily-tail producer and scheduler receipts.

No collector lives here. Old result files without physical publication/identity
receipts remain UNKNOWN. File hashes bind the assembler output to saved bytes;
operators still qualify those files against the delivery/scheduler readback.
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from .refresh_evidence import _instant, _integer

DAILY_TAIL_BASIS = "registry:tournaments.json (дейли до #1370)"
DENOMINATOR = Path(__file__).resolve().parents[2] / "configs/sofascore/denominator.tsv"
PRIMARY = "ENG-Premier League"
# Existing runner counters, paired with their actual Bronze MERGE targets.
COUNTERS = {
    "schedule_rows": "sofascore_schedule", "league_table_rows": "sofascore_league_table",
    "rows": "sofascore_player_ratings", "eps_rows": "sofascore_event_player_stats",
    "match_stats_rows": "sofascore_match_stats", "shotmap_rows": "sofascore_event_shotmap",
    "venue_rows": "sofascore_venue", "event_rows": "sofascore_events",
    "participant_rows": "sofascore_event_participants", "incident_rows": "sofascore_incidents",
    "lineup_rows": "sofascore_lineups", "capture_status_rows": "sofascore_match_capture_status",
}


def configured_tournaments(path=DENOMINATOR):
    with Path(path).open(newline="") as stream:
        rows = [r for r in csv.DictReader(stream, delimiter="\t") if r["basis"] == DAILY_TAIL_BASIS]
    result = {r["capture_key"]: int(r["tournament_id"]) for r in rows}
    if len(rows) != 7 or len(result) != 7 or len(set(result.values())) != 7:
        raise ValueError("configured tail denominator must contain exactly seven tournaments")
    return result


def _sha(value):
    if not isinstance(value, str) or len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("qualified full code revision required")
    return value


def _task(prefix, league):
    return prefix if league == PRIMARY else prefix + "_" + league.lower().replace(" ", "_").replace("-", "_")


def _publication(report, phase):
    """Validate retained successful save returns, never a completion boolean."""
    if report.get("manifest_committed") is not True:
        raise ValueError("durable manifest verification receipt absent")
    receipts = report["publication_receipts"]
    by_table = {}
    for receipt in receipts:
        table = receipt["table"]
        if table in by_table:
            raise ValueError("duplicate physical publication receipt")
        if receipt["location"] != "iceberg.bronze." + table:
            raise ValueError("physical publication target mismatch")
        by_table[table] = _integer(receipt["rows"])
    required = {}
    for counter, table in COUNTERS.items():
        if counter in report:
            count = _integer(report[counter])
            if table in by_table and by_table[table] != count:
                raise ValueError("physical publication receipt differs from original row counter")
            if count:
                required[table] = count
    if phase == "season":
        _integer(report["schedule_rows"], 1)
    else:
        _integer(report["capture_status_rows"], 1)
        total = _integer(report["matches_total"], 1)
        complete = _integer(report["matches_complete"])
        skipped = _integer(report["matches_skipped_existing"])
        if complete + skipped != total:
            raise ValueError("match publication does not cover resolved match denominator")
    if not required or any(by_table.get(table) != count for table, count in required.items()):
        raise ValueError("physical publication receipts absent or differ from produced rows")
    # Rejected rows are an existing, separate journal target; never a match publication.
    allowed = {table for counter, table in COUNTERS.items() if counter in report} | {"sofascore_rejected_rows"}
    if set(by_table) - allowed:
        raise ValueError("publication receipt has no produced row counter")
    if (not isinstance(report.get("tables"), list)
            or len(report["tables"]) != len(receipts)
            or set(report["tables"]) != {r["location"] for r in receipts}):
        raise ValueError("publication receipts differ from existing successful save returns")
    if report.get("endpoint_completeness") != 1.0:
        raise ValueError("full committed endpoint closure not proved")


def assess_tail(inputs, expected_revision, expected_night=None, expected_leagues=None):
    """Assess all seven configured tournaments using existing saved inputs.

    ``parent``/``child``/``task_instances`` are scheduler exports, ``reports``
    are unchanged runner JSON plus its passive receipts, ``import_errors`` is
    the saved FotMob isolate readback. Missing facts are never synthesized.
    """
    result = dict(status="unknown", reasons=[], leagues=[], source_requests=0,
                  production_changes=0, acceptance_started=False)
    def problem(message):
        result["reasons"].append(message)
    # Retain known failed physical outcomes even when other envelope files are absent.
    if isinstance(inputs, dict):
        for report in inputs.get("reports") if isinstance(inputs.get("reports"), list) else []:
            if isinstance(report, dict) and (report.get("errors") or report.get("fallback") or report.get("partial")):
                result["status"] = "fail"
        for key in ("parent", "child"):
            if isinstance(inputs.get(key), dict) and inputs[key].get("state") == "failed":
                result["status"] = "fail"
    try:
        revision = _sha(expected_revision)
        configured = configured_tournaments()
        if expected_leagues is not None and (len(expected_leagues) != 7 or set(expected_leagues) != set(configured)):
            raise ValueError("expected leagues differ from configured seven-tournament denominator")
        result["expected_leagues"] = sorted(configured)
        parent, child = inputs["parent"], inputs["child"]
        if not isinstance(parent, dict) or not isinstance(child, dict):
            raise ValueError("parent/child scheduler receipts absent")
        run_id = child["run_id"]
        if (parent["dag_id"] != "dag_trigger_sofascore_daily_tail"
                or child["dag_id"] != "dag_ingest_sofascore"
                or not run_id.startswith("tail__") or parent["child_run_id"] != run_id):
            raise ValueError("tail parent/child linkage absent or foreign")
        result.update(run_id=run_id, code_revision=revision)
        for run in (parent, child):
            if run.get("state") == "failed":
                result["status"] = "fail"
            if run.get("state") != "success":
                problem("parent/child terminal success absent")
            if run.get("code_revision") != revision:
                problem("parent/child cross-release evidence")
        start, finish = _instant(child["start_date"]), _instant(child["end_date"])
        logical = _instant(child["logical_date"])
        night = start.date().isoformat()
        result["night"] = night
        if finish < start or (expected_night and night != expected_night):
            problem("tail outside expected physical night")
        if (child["conf"].get("run_players") is not False
                or len(child["conf"].get("competition_ids", [])) != 7
                or set(child["conf"].get("competition_ids", [])) != set(configured)):
            problem("tail child conf does not pin all seven and no players")
        tasks = inputs["task_instances"]
        if len({t["task_id"] for t in tasks}) != len(tasks):
            problem("duplicate scheduler task receipt")
        task_map = {t["task_id"]: t for t in tasks}
        for t in tasks:
            if t["task_id"].startswith("scrape_player_capture") and t.get("state") != "skipped":
                problem("per-player capture ran or skip proof is absent")
            if t.get("run_id") != run_id or t.get("dag_id") != child["dag_id"]:
                problem("cross-run scheduler task receipt")
        for league in configured:
            player = task_map.get(_task("scrape_player_capture", league), {})
            if player.get("state") != "skipped":
                problem("player branch skip receipt absent: " + league)
            for prefix in ("scrape_sofascore_data", "scrape_match_capture"):
                task = task_map.get(_task(prefix, league), {})
                if task.get("state") in {"failed", "upstream_failed"}:
                    result["status"] = "fail"
                if task.get("state") != "success":
                    problem("producer task not successful: " + league + ":" + prefix)
        errors = inputs["import_errors"]
        if not isinstance(errors, dict):
            problem("FotMob isolate import-error readback absent")
            errors = {}
        if (errors.get("code_revision") != revision or errors.get("isolate") != "fotmob"
                or not isinstance(errors.get("errors"), list)
                or _instant(errors["checked_at"]) < finish):
            problem("qualified FotMob import-error readback absent")
        elif errors["errors"]:
            result["status"] = "fail"
            problem("FotMob isolate import errors")
        reports = inputs.get("reports", [])
        groups = {(league, phase): [] for league in configured for phase in ("season", "match")}
        for report in reports:
            identity = report.get("evidence_identity", {})
            phase = "match" if report.get("entity") == "match_capture" else "season" if report.get("entity") in {"all", "schedule", "league_table"} else None
            key = identity.get("league"), phase
            if key not in groups:
                problem("unbound or foreign physical producer result")
                continue
            groups[key].append(report)
            if report.get("errors") or report.get("fallback") or report.get("partial"):
                result["status"] = "fail"
                problem("failed/fallback/partial physical producer result: " + str(key))
        for league, tournament in configured.items():
            row = dict(league=league, tournament_id=tournament, status="unknown", phases={})
            seasons = set()
            for phase in ("season", "match"):
                physical = groups[league, phase]
                if len(physical) != 1:
                    row["phases"][phase] = "unknown"
                    problem("missing/duplicate physical producer result: " + league + ":" + phase)
                    continue
                report = physical[0]
                identity = report["evidence_identity"]
                try:
                    if (identity["run_id"] != run_id or identity["dag_id"] != child["dag_id"]
                            or identity["code_revision"] != revision
                            or identity["source_tournament_id"] != tournament
                            or _instant(identity["logical_date"]) != logical):
                        raise ValueError("cross-run/release/native tournament producer receipt")
                    sid = _integer(identity["source_season_id"], 1)
                    if not isinstance(identity["season"], str) or not identity["season"]:
                        raise ValueError("canonical season absent")
                    seasons.add((sid, identity["season"]))
                    task_id = _task("scrape_match_capture" if phase == "match" else "scrape_sofascore_data", league)
                    task = task_map[task_id]
                    if identity["try_number"] != task["try_number"] or _integer(identity["try_number"], 1) < 1:
                        raise ValueError("physical attempt differs from scheduler receipt")
                    # Current task state describes the latest attempt only.
                    # A recovered retry needs a separately qualified full history;
                    # neither the last report nor a completeness boolean provides it.
                    if identity["try_number"] > 1:
                        raise ValueError("retry physical history not qualified; first-attempt proof required")
                    if not start <= _instant(identity["started_at"]) <= _instant(identity["finished_at"]) <= finish:
                        raise ValueError("producer receipt outside child run")
                    if report.get("errors") != [] or report.get("fallback") or report.get("partial") or report.get("skipped"):
                        raise ValueError("producer result failed or incomplete")
                    _publication(report, phase)
                    row["phases"][phase] = "pass"
                except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
                    row["phases"][phase] = "unknown"
                    problem(league + ":" + phase + ": " + str(exc))
            if len(seasons) != 1:
                problem("season identity absent/mixed: " + league)
            elif all(row["phases"].get(phase) == "pass" for phase in ("season", "match")):
                row["status"] = "pass"
            result["leagues"].append(row)
        if not result["reasons"] and all(row["status"] == "pass" for row in result["leagues"]):
            result["status"] = "pass"
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        problem(str(exc))
    return result


def assemble_tail(manifest_path):
    """Read only explicitly named saved files and retain byte provenance."""
    manifest_path = Path(manifest_path)
    spec = json.loads(manifest_path.read_text())
    sources, gaps = [], []
    def read(name):
        path = Path(name)
        if not path.is_absolute():
            path = manifest_path.parent / path
        try:
            raw = path.read_bytes()
            sources.append(dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest()))
            return json.loads(raw)
        except (OSError, ValueError) as exc:
            gaps.append(str(path) + ": " + str(exc))
            return None
    inputs = {key: read(spec[key]) if spec.get(key) else None
              for key in ("parent", "child", "task_instances", "import_errors")}
    inputs["reports"] = [doc for name in spec.get("reports", []) if (doc := read(name)) is not None]
    result = assess_tail(inputs, spec["expected_revision"], spec.get("expected_night"))
    result.update(source_files=sources, assembly_gaps=gaps)
    # Keep raw receipts: downstream refresh assessment must recompute, not trust this status.
    result["evidence"] = inputs
    if gaps:
        result["reasons"].extend(gaps)
        if result["status"] == "pass":
            result["status"] = "unknown"
    return result
