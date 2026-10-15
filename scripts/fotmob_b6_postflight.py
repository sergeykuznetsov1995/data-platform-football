#!/usr/bin/env python3
"""Offline qualification of captured ceremony-free B6 delivery and first report.

No collectors, subprocesses, SQL, HTTP, environment or production writes. Inputs
are factual captures, not attestations of PASS. See the operations companion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

# Direct CLI and package imports use the same existing offline contract validator.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))
from scrapers.fotmob.catalog_contract import catalog_contract_from_dict

TARGET = "54717e11888550968c15cf893e2d7aa944fa7368"
DAGS = frozenset({"dag_backfill_fotmob", "dag_collect_fotmob_players",
                  "dag_ingest_fotmob", "dag_orchestrate_fotmob",
                  "dag_refresh_fotmob", "dag_trigger_fotmob_daily"})
OBLIGATIONS = frozenset({"300=2025", "9170=2023", "9669=2023/2024",
                         "10056=2026/2027", "10433=2022/2023", "10840=2025/2026"})
HEX = re.compile(r"[0-9a-f]{64}\Z")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def instant(value, *, runner=False):
    require(isinstance(value, str), "missing timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # The existing runner explicitly strips UTC tzinfo; all other captures must
    # carry their zone. Do not apply this convention to arbitrary evidence.
    if runner and result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    require(result.tzinfo is not None and result.utcoffset() is not None,
            "timestamp has no timezone")
    return result.astimezone(timezone.utc)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    raw = Path(path).read_bytes()
    return json.loads(raw, object_pairs_hook=unique), sha(raw)


def scope(item):
    return f"{item['competition_id']}={item['source_season_key']}"


def tokens(values, label):
    require(isinstance(values, list) and all(isinstance(v, str) and "=" in v for v in values),
            f"invalid {label}")
    require(len(set(values)) == len(values), f"duplicate {label}")
    return set(values)


def checkout_cut(checkout):
    """Derive cut from one retained Git HEAD reflog checkout record."""
    raw = Path(checkout["evidence_path"]).read_bytes()
    require(sha(raw) == checkout.get("evidence_sha256"), "checkout source evidence SHA256 differs")
    line_number = checkout.get("evidence_line")
    require(type(line_number) is int and line_number > 0, "checkout evidence line absent")
    lines = raw.decode("utf-8").splitlines()
    require(line_number <= len(lines), "checkout evidence line out of range")
    match = re.fullmatch(r"([0-9a-f]{40})\|HEAD@\{([^}]+)\}\|checkout: moving from .+ to .+", lines[line_number - 1])
    require(match is not None, "evidence is not a Git HEAD reflog checkout record")
    require(line_number == 1, "checkout is not the latest captured HEAD reflog record")
    require(match[1] == TARGET == checkout.get("head"), "checkout reflog target differs")
    cut = instant(match[2])
    require(cut == instant(checkout.get("actual_checkout_at")), "receipt cut differs from actual Git checkout")
    require(cut <= instant(checkout.get("checked_at")), "checkout receipt precedes checkout")
    return cut


def preserved_environment(capture):
    """Validate the captured environment facts that B6 must preserve."""
    for field in ("cron_sha256", "ignore_sha256"):
        require(isinstance(capture.get(field), str) and HEX.fullmatch(capture[field]),
                f"invalid or absent preserved {field}")
    require(isinstance(capture.get("1185"), str) and capture["1185"].strip(),
            "invalid or absent preserved 1185 service evidence")
    containers = capture.get("containers")
    require(isinstance(containers, dict) and set(containers) == {
        "fotmob-airflow-scheduler", "fotmob-airflow-metadb"}, "invalid preserved containers")
    for container in containers.values():
        require(isinstance(container, dict), "invalid preserved container")
        require(isinstance(container.get("id"), str) and HEX.fullmatch(container["id"]),
                "invalid preserved container id")
        instant(container.get("started"))
        require(isinstance(container.get("image"), str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", container["image"]),
                "invalid preserved container image")
        mounts = container.get("mounts")
        require(isinstance(mounts, list) and mounts, "invalid preserved container mounts")
        for mount in mounts:
            require(isinstance(mount, dict)
                    and isinstance(mount.get("Type"), str) and mount["Type"]
                    and isinstance(mount.get("Source"), str) and mount["Source"].startswith("/")
                    and isinstance(mount.get("Destination"), str) and mount["Destination"].startswith("/")
                    and type(mount.get("RW")) is bool, "invalid preserved container mount")
    return {key: capture[key] for key in ("containers", "cron_sha256", "ignore_sha256", "1185")}


def qualify_delivery(manifest, baseline, probe, checkout):
    require(manifest.get("target") == TARGET, "manifest is not the exact PR1723 target")
    expected = manifest.get("runtime_sha256")
    require(isinstance(expected, dict) and len(expected) == 30
            and all(isinstance(v, str) and HEX.fullmatch(v) for v in expected.values()),
            "manifest must contain 30 runtime SHA256 fingerprints")
    require(probe.get("head") == TARGET, "runtime head is not target (rollback is not delivery)")
    require(probe.get("clean") is True, "runtime is not clean")
    require(probe.get("runtime_sha256") == expected, "runtime fingerprints differ from target")
    require(probe.get("mounted_match") is True, "mounted fingerprints do not match")
    scripts = manifest.get("scripts_sha256")
    require(isinstance(scripts, dict) and set(scripts) == {
        "fotmob-auto-deliver.sh", "b6-fotmob-deliver.sh"}
        and all(isinstance(v, str) and HEX.fullmatch(v) for v in scripts.values())
        and probe.get("scripts") == scripts, "delivery script fingerprints differ")
    require(isinstance(baseline.get("protected"), dict) and baseline["protected"]
            and probe.get("protected") == baseline["protected"], "protected evidence changed or absent")
    require(preserved_environment(baseline) == preserved_environment(probe),
            "preserved containers, cron, ignore or 1185 evidence changed")
    require(checkout.get("head") == TARGET, "checkout evidence target differs")
    cut = checkout_cut(checkout)
    require(cut <= instant(checkout.get("checked_at")) <= instant(probe.get("checked_at")),
            "checkout evidence chronology is invalid")
    require(instant(baseline.get("checked_at")) < cut, "baseline is not before checkout")
    meta = probe["metadb"]
    require(type(meta.get("import_errors")) is int and meta["import_errors"] == 0,
            "import errors are present or unknown")
    checked = instant(meta.get("checked_at"))
    require(cut < checked <= instant(probe["checked_at"]), "metadb capture is not after checkout")
    dags = meta["dags"]
    require(isinstance(dags, list) and len(dags) == 6
            and {d.get("dag_id") for d in dags} == DAGS, "not the exact six DAGs")
    baseline_meta = baseline.get("metadb")
    require(isinstance(baseline_meta, dict), "baseline DAG pause evidence absent")
    baseline_dags = baseline_meta.get("dags")
    require(isinstance(baseline_dags, list) and len(baseline_dags) == 6
            and all(isinstance(d, dict) for d in baseline_dags)
            and {d.get("dag_id") for d in baseline_dags} == DAGS,
            "baseline six DAG pause evidence absent")
    baseline_pauses = {d["dag_id"]: d.get("is_paused") for d in baseline_dags}
    require(all(type(value) is bool for value in baseline_pauses.values()), "invalid baseline DAG pause")
    for dag in dags:
        require(type(dag.get("is_paused")) is bool
                and dag["is_paused"] == baseline_pauses[dag["dag_id"]], "DAG pause changed or absent")
        require(dag.get("is_active") is True
                and cut < instant(dag.get("last_parsed_time")) <= checked,
                f"DAG not parsed strictly after actual checkout: {dag.get('dag_id')}")
    return {"status": "PASS", "target": TARGET, "actual_checkout_at": cut.isoformat(),
            "runtime_fingerprints": 30, "dags_after_cut": sorted(DAGS)}


def qualify_report(report, receipt, natural_runs, cut, report_sha256):
    require(report.get("mode") in {"refresh", "daily", "backfill"}, "report is not a seasonal mode")
    selection = report["selection"]
    require("season" in selection.get("entities", []), "report has no seasonal work")
    run_uuid = report.get("run_id")
    require(isinstance(run_uuid, str) and str(uuid.UUID(run_uuid)) == run_uuid,
            "runner identity is not a canonical UUID")
    run = receipt["run"]
    for key in ("mode", "status", "complete", "errors"):
        require(key not in receipt or receipt[key] == report.get(key), f"receipt {key} differs from report")
    require(run.get("run_id") == "fotmob_orchestrated__" + run_uuid,
            "runner UUID differs from natural DagRun")
    require(run.get("state") in {"success", "failed"}, "DagRun is not terminal")
    started, completed = instant(report["started_at"], runner=True), instant(report["completed_at"], runner=True)
    dag_start, dag_end = instant(run["start_date"]), instant(run["end_date"])
    require(cut < dag_start <= started <= completed <= dag_end <= instant(receipt["checked_at"]),
            "report or DagRun does not lie strictly after actual checkout")
    require(receipt.get("report_sha256") == report_sha256, "captured report SHA256 differs")
    require(type(receipt.get("report_line")) is int and receipt["report_line"] > 0,
            "report log line is absent")
    require(isinstance(receipt.get("log_sha256"), str) and HEX.fullmatch(receipt["log_sha256"]),
            "captured log SHA256 absent")
    require(f"/dag_id=dag_ingest_fotmob/run_id={run['run_id']}/task_id=scrape_fotmob_data/"
            in receipt.get("log_path", ""), "captured log identity differs")
    raw_line = Path(receipt["retained_report_line_path"]).read_bytes()
    require(sha(raw_line) == receipt.get("retained_report_line_sha256"), "retained report line SHA256 differs")
    line = re.fullmatch(r"\[([^]\r\n]+)\] \{subprocess\.py:[0-9]+\} INFO - (\{[^\r\n]+\})\s*", raw_line.decode("utf-8"))
    require(line is not None, "invalid retained Airflow report line")
    captured_report = json.loads(line[2], object_pairs_hook=unique)
    require(json.dumps(captured_report, sort_keys=True) == json.dumps(report, sort_keys=True),
            "retained log line report differs from input report")
    require(completed <= instant(line[1]) <= dag_end, "report log timestamp outside DagRun")
    require(isinstance(natural_runs, list) and natural_runs, "natural run inventory absent")
    eligible = [r for r in natural_runs if instant(r["start_date"]) > cut and r.get("mode") != "transfers"]
    require(len({r["run_id"] for r in natural_runs}) == len(natural_runs), "duplicate natural DagRun")
    selected = [r for r in eligible if r.get("run_id") == run["run_id"]]
    require(len(selected) == 1 and selected[0].get("mode", report["mode"]) == report["mode"],
            "natural DagRun mode differs from report")
    first_start = min(instant(r["start_date"]) for r in eligible)
    first_runs = [r for r in eligible if instant(r["start_date"]) == first_start]
    require(len(first_runs) == 1, "ambiguous first natural seasonal DagRun after cut")
    require(all(first_runs[0].get(k) == run.get(k)
                for k in ("run_id", "state", "start_date", "end_date")),
            "report is not the first captured natural seasonal DagRun after cut")
    planned = tokens(selection["planned_scopes"], "planned scopes")
    completed_scopes = tokens(selection["completed_scopes"], "completed scopes")
    unavailable = tokens(selection["calendar_unavailable_scopes"], "calendar unavailable scopes")
    require(selection.get("planned_scope_count") == len(planned), "planned scope count differs")
    require(completed_scopes <= planned and unavailable <= planned and not unavailable & completed_scopes,
            "completed or unavailable scopes contradict the plan")
    operations = report["operations"]
    plans = [o for o in operations if o.get("entity") == "season_work_plan"]
    require(len(plans) == 1, "season work plan missing or ambiguous")
    metadata = plans[0]["metadata"]
    contract = catalog_contract_from_dict(selection["catalog_contract"])
    require(selection.get("scope_plan_signature") == contract.plan_signature
            and selection.get("entities") == list(contract.entities)
            and metadata.get("scope_plan_signature") == contract.plan_signature,
            "catalog contract signature or entities differ from selection")
    contract_scopes = set(contract.scopes)
    require(metadata.get("obligation_scopes") == len(contract_scopes), "obligation count differs from catalog")
    require(OBLIGATIONS <= contract_scopes, "six obligations disappeared from denominator")
    require(planned <= contract_scopes, "planned scope outside catalog")
    cooling = tokens(metadata.get("cooling_source_gap_scopes", []), "cooling scopes")
    require(cooling <= contract_scopes, "cooldown scope outside denominator")
    attempts = selection.get("scope_attempts")
    require(isinstance(attempts, list), "scope attempts absent")
    attempt_map = {scope(a): a for a in attempts}
    require(len(attempt_map) == len(attempts), "duplicate scope attempt")
    attempt_operations = [o for o in operations if o.get("entity") == "scope_attempts"]
    require(len(attempt_operations) == 1, "scope attempt operation missing or ambiguous")
    outcomes = attempt_operations[0]["metadata"]["outcomes"]
    require({scope(a): a for a in outcomes} == attempt_map and len(outcomes) == len(attempt_map),
            "scope attempt operation differs from selection")
    unavailable_operations = {}
    for operation in operations:
        if operation.get("entity") != "season_bundle":
            continue
        m = operation.get("metadata", {})
        evidence = m.get("calendar_unavailability")
        if evidence is None:
            continue
        identity = scope(m)
        require(identity not in unavailable_operations, "duplicate calendar unavailable operation")
        require(operation.get("attempted") == 1 and operation.get("status") == "retryable"
                and operation.get("succeeded") == 0 and m.get("calendar_available") is False
                and m.get("persisted") is True and operation.get("retryable")
                and identity not in completed_scopes, "calendar unavailability lost retry/incompleteness")
        require(evidence.get("reason") == "source_calendar_unavailable", "calendar reason differs")
        url = evidence.get("url", "")
        parsed = urlsplit(url)
        require(parsed.scheme == "https" and parsed.hostname == "www.fotmob.com"
                and parsed.path == "/api/data/leagues"
                and parse_qs(parsed.query) == {"id": [str(m["competition_id"])],
                                               "season": [m["source_season_key"]]},
                "calendar provenance URL differs from scope")
        require(evidence.get("target_key") == sha(url.encode())
                and isinstance(evidence.get("content_hash"), str) and HEX.fullmatch(evidence["content_hash"])
                and isinstance(evidence.get("raw_uri"), str) and evidence["raw_uri"],
                "calendar provenance hashes or raw URI absent/inconsistent")
        require(evidence.get("http_status") in {200, 304}
                and type(evidence.get("cache_hit")) is bool and evidence.get("stale") is False
                and instant(evidence.get("fetched_at")) <= completed,
                "calendar provenance response invalid/stale/future")
        attempt = attempt_map.get(identity, {})
        require(attempt.get("outcome") == "retryable"
                and attempt.get("reason") == "source_calendar_unavailable"
                and attempt.get("plan_signature") == selection.get("scope_plan_signature")
                and f"{run_uuid}:{identity}" in attempt.get("attempt_identities", [])
                and type(attempt.get("attempt_count")) is int and attempt["attempt_count"] > 0
                and started <= instant(attempt.get("last_attempt_at")) <= completed
                and instant(attempt.get("next_retry_at")) > instant(attempt["last_attempt_at"]),
                "calendar attempt identity, timing or retry evidence differs")
        unavailable_operations[identity] = evidence
    require(set(unavailable_operations) == unavailable, "calendar diagnostic list differs from attempted evidence")
    require(type(report.get("complete")) is bool, "runner complete absent")
    require(not unavailable or report["complete"] is False, "unavailable calendar report cannot be complete")
    return {"status": "PASS", "run_id": run_uuid, "dag_run": run,
            "diagnostics": {"calendar_unavailable_scopes": sorted(unavailable),
                            "calendar_unavailable_count": len(unavailable),
                            "provenance": unavailable_operations},
            "runner": {"status": report.get("status"), "complete": report["complete"],
                       "errors": report.get("errors"), "scope_attempts": attempts,
                       "planned_scopes": selection["planned_scopes"],
                       "completed_scopes": selection["completed_scopes"],
                       "obligation_scopes": metadata["obligation_scopes"],
                       "catalog_contract": contract.as_dict(),
                       "cooling_source_gap_scopes": sorted(cooling)},
            "six_obligations": {"denominator": sorted(OBLIGATIONS),
                                "attempted_unavailable": sorted(OBLIGATIONS & unavailable),
                                "completed_in_report": sorted(OBLIGATIONS & completed_scopes),
                                "unplanned": sorted(OBLIGATIONS - planned)},
            "source_recovery_proven": False, "runtime_full_success_1699_1711": None}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "baseline-probe", "probe", "checkout-evidence", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("report", "report-receipt", "natural-runs"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args(argv)
    result = {"schema": "fotmob-b6-postflight-v1", "status": "FAIL", "inputs": {},
              "scope": "offline captured evidence qualification; no acceptance journal mutation",
              "first_fail_preserved": True}
    try:
        loaded = {}
        for key in ("manifest", "baseline_probe", "probe", "checkout_evidence", "report", "report_receipt", "natural_runs"):
            path = getattr(args, key)
            if path is not None:
                loaded[key], digest = read_json(path)
                result["inputs"][key] = {"path": str(path), "sha256": digest}
        checkout = loaded["checkout_evidence"]
        # Keep the source digest alongside the receipt digest. qualify_delivery
        # derives the cut from the selected actual Git HEAD checkout line.
        evidence_path = Path(checkout["evidence_path"])
        require(sha(evidence_path.read_bytes()) == checkout["evidence_sha256"],
                "checkout source evidence SHA256 differs")
        result["inputs"]["checkout_source"] = {"path": str(evidence_path), "sha256": checkout["evidence_sha256"]}
        result["delivery"] = qualify_delivery(loaded["manifest"], loaded["baseline_probe"], loaded["probe"], checkout)
        optional = [args.report, args.report_receipt, args.natural_runs]
        require(not any(optional) or all(optional), "report, report-receipt and natural-runs are required together")
        if args.report is not None:
            result["captured_runner"] = {key: loaded["report"].get(key) for key in
                                         ("run_id", "mode", "status", "complete", "errors")}
            result["report"] = qualify_report(loaded["report"], loaded["report_receipt"], loaded["natural_runs"],
                                               instant(checkout["actual_checkout_at"]), result["inputs"]["report"]["sha256"])
        else:
            result["report"] = {"status": "NOT_CHECKED"}
        result["status"] = "PASS"
    except (ValueError, TypeError, KeyError, OSError, AttributeError) as exc:
        result["error"] = str(exc)
    # Each result is immutable: a later PASS must use a new path, preserving FAIL.
    try:
        with args.output.open("x", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
    except OSError as exc:
        print(f"cannot create output (existing evidence is preserved): {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": result["status"], "error": result.get("error"), "output": str(args.output)}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
