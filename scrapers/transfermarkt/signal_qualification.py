"""Content-based qualification for the current tmapi detector.

A boolean or a file hash is not qualification. Two exact-cohort source parity
reports and at least one real changed field observed by both reference and
signal on the same Moscow day are required. This is experimental qualification,
not runtime acceptance or permission to skip complete careers.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
import hashlib
import json
from copy import deepcopy
import os
from pathlib import Path
import re
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

SCHEMA = "tm-signal-qualification-v1"
OBSERVATION_SCHEMA = "tm-signal-parity-observation-v1"
GROUPS = {"top_league", "lower_league", "calendar", "cup"}
FIELDS = ("value", "value_date", "value_present", "contract", "clubs")


class QualificationError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise QualificationError(message)


def _time(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise QualificationError("qualification timestamp is invalid") from exc
    _require(result.tzinfo is not None, "qualification timestamp needs timezone")
    return result


def _nullable_date(value):
    if value is not None:
        try:
            _require(isinstance(value, str) and date.fromisoformat(value).isoformat() == value, "compared date is not ISO")
        except ValueError as exc:
            raise QualificationError("compared date is invalid") from exc


def _ids(values):
    _require(isinstance(values, list) and all(isinstance(v, str) and re.fullmatch(r"[1-9][0-9]*", v) for v in values), "qualification IDs are invalid")
    _require(len(values) == len(set(values)), "qualification has duplicate IDs")
    return set(values)


@lru_cache(maxsize=64)
def _packet_ids(url):
    query = parse_qs(urlsplit(url).query)
    _require(set(query) == {"ids[]"} and len(query["ids[]"]) <= 300, "tmapi packet identity is wrong")
    return frozenset(_ids(query["ids[]"]))


def _proof(proof, endpoint, player, club=None):
    _require(isinstance(proof, dict), "source raw proof is missing")
    for field in ("capture_id", "body_sha256"):
        _require(isinstance(proof.get(field), str) and re.fullmatch(r"[a-f0-9]{64}", proof[field]), "source raw identity/hash is invalid")
    stamp = _time(proof.get("fetched_at"))
    url = urlsplit(proof.get("url", ""))
    _require(url.scheme == "https" and not url.username and not url.password and not url.fragment, "source URL is invalid")
    if endpoint == "tmapi":
        _require(url.netloc == "tmapi.transfermarkt.technology" and url.path == "/players", "tmapi proof is another endpoint")
        _require(player in _packet_ids(proof["url"]), "tmapi packet identity is wrong")
    else:
        _require(url.netloc == "www.transfermarkt.com" and not url.query, "reference proof is not an official endpoint")
        if endpoint == "plus1":
            _require(re.fullmatch(rf"/[^/]+/kader/verein/{club}/plus/1", url.path) is not None, "reference is not the full current club page")
        else:
            expected = "marketValueDevelopment/graph" if endpoint == "mv" else "transferHistory/list"
            _require(url.path == f"/ceapi/{expected}/{player}", "career proof belongs to another player")
    return stamp


def _observation(value, cohort):
    _require(isinstance(value, dict) and value.get("schema_version") == OBSERVATION_SCHEMA, "source observation schema is not supported")
    _require(value.get("missing_ids") == [] and value.get("unknown_ids") == [], "source observation has missing or unknown IDs")
    rows = value.get("players")
    _require(isinstance(rows, list), "source observation has no comparisons")
    ids = _ids([row.get("player_id") for row in rows if isinstance(row, dict)])
    _require(len(ids) == len(rows) and ids == cohort, "source comparison cohort differs from expected IDs")
    start, end = _time(value.get("observed_from")), _time(value.get("observed_to"))
    _require(start <= end and end - start <= timedelta(hours=24), "source observation exceeds one day")
    _require(value.get("day_msk") == end.astimezone(ZoneInfo("Europe/Moscow")).date().isoformat(), "source observation Moscow day is wrong")
    indexed, missing = {}, {"mv": 0, "transfers": 0}
    for row in rows:
        player = row["player_id"]
        api, roster, ceapi, raw = (row.get(name) for name in ("tmapi", "plus1", "ceapi", "raw"))
        _require(all(isinstance(item, dict) for item in (api, roster, ceapi, raw)), "source row lacks actual comparisons")
        _require(set(FIELDS) <= set(api) and {"value", "contract", "club_id"} <= set(roster), "source row lacks compared fields")
        clubs = _ids(api["clubs"])
        _require(type(api["value_present"]) is bool, "value presence is not proven")
        _require(api["value"] is None or type(api["value"]) is int and api["value"] >= 0, "source value is invalid")
        _require((api["value_present"] and type(api["value"]) is int) or
                 (not api["value_present"] and api["value"] is None and api["value_date"] is None), "value presence contradicts source fields")
        _nullable_date(api["value_date"])
        _nullable_date(api["contract"])
        _require(api["value"] == roster["value"] and api["contract"] == roster["contract"] and roster["club_id"] in clubs, "tmapi differs from full roster")
        stamps = {"tmapi": _proof(raw.get("tmapi"), "tmapi", player),
                  "plus1": _proof(raw.get("plus1"), "plus1", player, roster["club_id"]),
                  "mv": _proof(raw.get("mv"), "mv", player),
                  "transfers": _proof(raw.get("transfers"), "transfers", player)}
        _require(all(start <= stamp <= end for stamp in stamps.values()), "raw timestamps are outside observation")
        for endpoint in ("mv", "transfers"):
            ref = ceapi.get(endpoint)
            _require(isinstance(ref, dict) and ref.get("status") in {"compared", "authoritative_empty"}, "ceapi outcome is unknown or unproven")
            if ref["status"] == "authoritative_empty":
                _require(ref.get("raw_rows") == 0, "nonempty ceapi was called empty")
                missing[endpoint] += 1
            elif endpoint == "mv":
                _require(type(ref.get("raw_rows")) is int and ref["raw_rows"] > 0 and {"value", "value_date"} <= set(ref), "MV comparison is incomplete")
                _require(ref["value"] == api["value"] and ref["value_date"] == api["value_date"], "tmapi differs from ceapi market value")
            else:
                _require(type(ref.get("raw_rows")) is int and ref["raw_rows"] > 0 and ref.get("club_id") in clubs, "tmapi differs from ceapi current club")
        indexed[player] = (row, stamps)
    _require(value.get("ceapi_empty_counts") == missing, "ceapi empty counts differ from concrete outcomes")
    _require(value.get("counts") == {"expected": len(cohort), "compared": len(indexed), "missing": 0, "unknown": 0}, "comparison counts differ from concrete rows")
    return indexed, start, end


def _linked_report(link, artifact_path):
    _require(isinstance(link, dict) and isinstance(link.get("path"), str), "qualification report link missing")
    report_path = Path(link["path"])
    if not report_path.is_absolute():
        report_path = artifact_path.parent / report_path
    _require(report_path.is_file(), "qualification source report is missing")
    body = report_path.read_bytes()
    _require(hashlib.sha256(body).hexdigest() == link.get("sha256"), "qualification source report hash differs")
    try:
        report = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise QualificationError("source report is not JSON") from exc
    _require(isinstance(report, dict), "source report is not an object")
    _require(report.get("complete") is True and report.get("task_failures") == {}, "source report is incomplete")
    return report


def _with_recheck(before, current, recheck, cohort_value):
    _require(recheck.get("mode") == "recheck" and recheck.get("original_cohort") == cohort_value, "recheck belongs to another original cohort")
    old_rows = {row["player_id"]: row for row in before["players"]}
    current_rows = {row["player_id"]: row for row in current["players"]}
    selected = _ids(recheck.get("selected_changed_ids"))
    _require(1 <= len(selected) <= 64 and selected <= set(old_rows) and set(old_rows) == set(current_rows), "recheck selection is not a bounded original-cohort subset")
    _require(all(old_rows[player]["tmapi"] != current_rows[player]["tmapi"] for player in selected), "recheck did not select actual changed IDs")
    checked, _, _ = _observation(recheck.get("qualification_observation"), selected)
    merged = deepcopy(current)
    merged["players"] = [checked[row["player_id"]][0] if row["player_id"] in checked else row for row in current["players"]]
    for player in selected:
        _require(_time(checked[player][0]["raw"]["tmapi"]["fetched_at"]) >= _time(current_rows[player]["raw"]["tmapi"]["fetched_at"]), "recheck is older than current observation")
    merged["observed_from"] = min(current["observed_from"], recheck["qualification_observation"]["observed_from"])
    merged["observed_to"] = max(current["observed_to"], recheck["qualification_observation"]["observed_to"])
    merged["day_msk"] = _time(merged["observed_to"]).astimezone(ZoneInfo("Europe/Moscow")).date().isoformat()
    merged["ceapi_empty_counts"] = {endpoint: sum(row["ceapi"][endpoint]["status"] == "authoritative_empty" for row in merged["players"]) for endpoint in ("mv", "transfers")}
    return merged


def validate_qualification(qualification: Mapping[str, Any]) -> dict:
    """Validate the wrapper, linked report hashes, all rows and changed evidence."""
    _require(isinstance(qualification, Mapping), "qualification wrapper is not an object")
    path = Path(str(qualification.get("evidence_path", "")))
    _require(path.is_file(), "qualification evidence file is missing")
    body = path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    _require(qualification.get("evidence_sha256") == digest, "qualification evidence hash differs")
    try:
        artifact = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise QualificationError("qualification artifact is not JSON") from exc
    _require(isinstance(artifact, dict) and artifact.get("schema_version") == SCHEMA, "qualification artifact schema is not supported")
    cohort_value = artifact.get("cohort", {})
    cohort = _ids(cohort_value.get("player_ids"))
    _require(len(cohort) >= 1000 and GROUPS <= set(cohort_value.get("groups", [])), "qualification needs >=1000 players and all sample groups")
    scopes = cohort_value.get("scopes")
    _require(isinstance(scopes, list) and len(set(scopes)) >= 20 and all(isinstance(scope, str) and re.fullmatch(r"[A-Za-z0-9_-]+/(?:18|19|20|21)[0-9]{2}", scope) for scope in scopes), "qualification needs >=20 actual scopes")
    reports = artifact.get("reports")
    _require(isinstance(reports, list) and len(reports) == 2, "qualification needs two linked observations")
    source_reports = []
    for link in reports:
        report = _linked_report(link, path)
        _require(report.get("mode") == "parity" and report.get("complete") is True and report.get("task_failures") == {}, "source parity report is incomplete")
        _require(report.get("cohort") == cohort_value, "source report cohort differs from qualification")
        _require(report.get("players_checked") == len(cohort), "source report player count is inconsistent")
        source_reports.append(report)
    first_observation = source_reports[0].get("qualification_observation")
    second_observation = source_reports[1].get("qualification_observation")
    if "recheck" in artifact:
        second_observation = _with_recheck(first_observation, second_observation, _linked_report(artifact["recheck"], path), cohort_value)
    observations = [_observation(first_observation, cohort), _observation(second_observation, cohort)]
    old, _, old_end = observations[0]
    new, new_start, new_end = observations[1]
    _require(old_end < new_start and (new_end.astimezone(ZoneInfo("Europe/Moscow")).date() - old_end.astimezone(ZoneInfo("Europe/Moscow")).date()).days == 1,
             "qualification needs observations on consecutive Moscow days")
    changes = []
    for player in sorted(cohort, key=int):
        before, before_stamps = old[player]
        after, after_stamps = new[player]
        for field, reference, raw_kind in (("value", "value", "mv"), ("value_date", "value_date", "mv"), ("contract", "contract", "plus1"), ("clubs", "club_id", "plus1")):
            if before["tmapi"][field] == after["tmapi"][field]:
                continue
            if raw_kind == "mv":
                prev_ref, next_ref = before["ceapi"]["mv"], after["ceapi"]["mv"]
                if prev_ref["status"] != "compared" or next_ref["status"] != "compared" or prev_ref[reference] == next_ref[reference]:
                    continue
            elif before["plus1"][reference] == after["plus1"][reference]:
                continue
            signal_time, reference_time = after_stamps["tmapi"], after_stamps[raw_kind]
            _require(signal_time.astimezone(ZoneInfo("Europe/Moscow")).date() == reference_time.astimezone(ZoneInfo("Europe/Moscow")).date(), "changed reference and detector were not seen on the same day")
            _require(abs(signal_time - reference_time) <= timedelta(hours=24), "changed reference was not matched within 24 hours")
            changes.append({"player_id": player, "field": field, "before": before["tmapi"][field], "after": after["tmapi"][field],
                            "signal_fetched_at": signal_time.isoformat(), "reference_fetched_at": reference_time.isoformat()})
    _require(changes, "qualification has no real same-day changed observation")
    result = dict(qualification)
    result.update(players_compared=len(cohort), source_parity=True, same_day_changes=True,
                  qualified_at=new_end.isoformat(), changed_observations=changes,
                  schema_version=SCHEMA)
    return result


def build_qualification(day1: str | Path, day2: str | Path, output: str | Path, *, recheck: str | Path | None = None) -> dict:
    """Build from concrete benchmark reports, then use the same runtime validator."""
    paths = [Path(day1).resolve(), Path(day2).resolve()]
    output = Path(output).resolve()
    reports = [json.loads(path.read_bytes()) for path in paths]
    artifact = {"schema_version": SCHEMA, "cohort": reports[0].get("cohort"),
                "reports": [{"path": os.path.relpath(path, output.parent), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in paths]}
    if recheck is not None:
        recheck_path = Path(recheck).resolve()
        artifact["recheck"] = {"path": os.path.relpath(recheck_path, output.parent), "sha256": hashlib.sha256(recheck_path.read_bytes()).hexdigest()}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2) + "\n")
    wrapper = {"evidence_path": str(output), "evidence_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
    return validate_qualification(wrapper)
