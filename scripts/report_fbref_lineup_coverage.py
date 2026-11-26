#!/usr/bin/env python3
"""Prepare a read-only Trino query for the FBref day-14 lineup criterion."""

import argparse
from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers.fbref.daily_report import _scope, instant, match_id_from_url  # noqa: E402
from scrapers.fbref.report_observations import played_score  # noqa: E402


def _observation_time(record, key):
    try:
        return instant(record.get(key))
    except (TypeError, ValueError):
        return None


def prepare(snapshot, control, deployed, cutoff):
    if cutoff != deployed + timedelta(days=14):
        raise ValueError("cutoff must be exactly 14 days after verified deployment")
    coverage = snapshot.get("coverage", {})
    end_day = (cutoff - timedelta(microseconds=1)).date().isoformat()
    issues = set()
    if (instant(coverage.get("as_of")) != cutoff
            or coverage.get("start", "9999") > deployed.date().isoformat()
            or coverage.get("end", "") < end_day):
        issues.add("snapshot_does_not_cover_window")
    if snapshot.get("errors") or not all(isinstance(snapshot.get(key), list) for key in (
        "errors", "schedules", "observations", "competitions", "seasons", "schedule_health"
    )):
        issues.add("snapshot_read_failure_or_unverified")
    adults, pairs, gaps = _scope(snapshot)
    if not pairs:
        issues.add("empty_current_scope")
    direct = {}
    if (instant(control.get("as_of")) != cutoff or not control.get("cutoff_receipt")
            or not isinstance(control.get("editions"), list)):
        issues.add("unverified_cutoff_or_direct_registry")
    for row in control.get("editions", []):
        pair = (str(row["competition_id"]), str(row["season_id"]))
        if pair in pairs:
            if pair in direct:
                issues.add(f"duplicate_direct_edition:{'/'.join(pair)}")
            direct[pair] = row
    for gap in gaps:
        pair = (gap["competition_id"], gap.get("season_id"))
        if gap["reason"] != "missing_or_unfetched_schedule" or pair not in direct:
            issues.add(f"{gap['reason']}:{'/'.join(filter(None, pair))}")

    obs = {(str(r["competition_id"]), str(r["season_id"]), r["match_id"]): r
           for r in snapshot.get("observations", [])}
    fixtures = {}
    linked_fixtures = defaultdict(set)
    unreported = {}
    schedule_keys = set()
    scheduled_played = set()

    def add(pair, mid, key):
        if key in fixtures and fixtures[key][:2] != list(pair):
            issues.add(f"match_scope_conflict:{key}")
            return
        fixtures[key] = [*pair, mid, key]

    for row in snapshot.get("schedules", []):
        pair = (str(row["source_competition_id"]), str(row["source_season_id"]))
        if pair not in pairs:
            if (pair[0] in adults and played_score(row.get("score"), row.get("notes"))
                    and deployed.date().isoformat() <= str(row.get("date") or "") <= end_day):
                issues.add(f"played_edition_outside_current_scope:{'/'.join(pair)}")
            continue
        mid = match_id_from_url(row.get("match_url"))
        if mid:
            schedule_keys.add((*pair, mid))
        if not played_score(row.get("score"), row.get("notes")):
            continue
        record = obs.get((*pair, mid), {})
        read_at = _observation_time(record, "kickoff_observed_at")
        kickoff = _observation_time(record, "kickoff_at") if read_at and read_at <= cutoff else None
        if kickoff:
            if not deployed <= kickoff < cutoff:
                continue
        else:
            try:
                source_day = date.fromisoformat(str(row.get("date") or "")).isoformat()
            except ValueError:
                source_day = None
            # Source-local dates in the padded read can fall across a UTC boundary.
            if source_day and not ((deployed.date() - timedelta(days=1)).isoformat()
                                   <= source_day <= (cutoff.date() + timedelta(days=1)).isoformat()):
                continue
            issues.add("unconfirmed_kickoff")
        key = json.dumps([*pair, row.get("date"), row.get("time"), row.get("home"), row.get("away")])
        if mid:
            scheduled_played.add((*pair, mid))
            linked_fixtures[key].add(mid)
            add(pair, mid, mid)
        else:
            issues.add("unreported_fixture")
            unreported[key] = pair

    for key, pair in unreported.items():
        if len(linked_fixtures[key]) == 1:
            continue
        add(pair, None, key)

    confirmed = set()
    for key, record in obs.items():
        cid, sid, mid = key
        pair = (cid, sid)
        raw_key = record.get("first_completed_raw_key")
        if pair not in pairs or not (record.get("first_completed_seen_at") or raw_key):
            continue  # A preview has neither half of a completed-report receipt.
        kickoff = _observation_time(record, "kickoff_at")
        read_at = _observation_time(record, "kickoff_observed_at")
        seen = _observation_time(record, "first_completed_seen_at")
        dated = kickoff is not None and read_at is not None and read_at <= cutoff
        in_window = kickoff is not None and deployed <= kickoff < cutoff
        if dated and not in_window:
            continue
        if seen and seen > cutoff:
            if in_window:
                issues.add(f"completion_observed_after_cutoff:{mid}")
            continue
        # Without an authenticated kickoff, a pre-deployment completion (or last
        # sighting) can still rule out an older fixture. A contradictory date cannot.
        last_seen = seen or _observation_time(record, "last_seen_at")
        if not in_window and last_seen and last_seen < deployed:
            continue
        add(pair, mid, mid)
        if not (dated and seen and isinstance(raw_key, str) and raw_key.strip()):
            issues.add(f"completed_observation_incomplete:{mid}")
        elif seen < kickoff:
            issues.add(f"completion_before_kickoff:{mid}")
        else:
            confirmed.add(key)
        if key not in scheduled_played:
            if key in schedule_keys:
                issues.add(f"completed_observation_conflicts_with_schedule:{mid}")
            elif pair not in direct:
                issues.add(f"completed_observation_missing_from_schedule:{mid}")

    for pair, edition in sorted(direct.items()):
        targets = {r["match_id"]: r.get("raw_key") for r in edition.get("targets", [])}
        catalog_ids = set(edition.get("catalog_match_ids") or ())
        root = match_id_from_url(edition.get("canonical_url"))
        known_ids = set(targets) | {mid for cid, sid, mid, _ in fixtures.values()
                                    if (cid, sid) == pair and mid}
        if root:
            known_ids.add(root)
        # The accepted source list can identify matches the frontier missed.
        # A known match absent from that list leaves the edition unreconciled.
        if (not root or not edition.get("catalog_raw_key")
                or edition["catalog_raw_key"] != (edition.get("catalog_source") or {}).get("manifest_key")
                or not known_ids <= catalog_ids):
            issues.add(f"direct_catalog_unverified:{'/'.join(pair)}")
        proofs = {r["match_id"]: r for r in edition.get("match_proofs", [])}
        for mid in sorted(known_ids | catalog_ids):
            if not re.fullmatch("[0-9a-f]{8}", mid):
                issues.add(f"invalid_direct_id:{mid}")
                continue
            record = obs.get((*pair, mid), {})
            proof = proofs.get(mid, {})
            trusted = (proof.get("raw_key") and proof["raw_key"] == targets.get(mid)
                       and instant(proof.get("fetched_at")) is not None
                       and instant(proof["fetched_at"]) <= cutoff)
            read_at = _observation_time(record, "kickoff_observed_at")
            kickoff = instant(proof.get("kickoff_at")) if trusted else None
            if kickoff is None and read_at and read_at <= cutoff:
                kickoff = _observation_time(record, "kickoff_at")
            if kickoff and not deployed <= kickoff < cutoff:
                if mid in fixtures:
                    issues.add(f"direct_kickoff_conflict:{mid}")
                continue
            if trusted and played_score(proof.get("score"), proof.get("notes")):
                if kickoff and instant(proof["fetched_at"]) < kickoff:
                    issues.add(f"direct_score_before_kickoff:{mid}")
                add(pair, mid, mid)
                if kickoff:
                    continue
            if trusted and re.search(
                r"postpon|cancel|abandon|award|walkover", str(proof.get("notes") or ""), re.I
            ):
                if mid in fixtures:
                    issues.add(f"direct_played_status_conflict:{mid}")
                continue
            if kickoff and ((*pair, mid) in scheduled_played or (*pair, mid) in confirmed):
                continue
            issues.add(f"direct_match_unconfirmed:{mid}")
    return {"fixtures": [fixtures[key] for key in sorted(fixtures)],
            "pairs": sorted(pairs), "issues": sorted(issues)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--control", type=Path, required=True)
    p.add_argument("--deployment-sha", required=True)
    p.add_argument("--deployed-at", required=True, type=instant)
    p.add_argument("--cutoff", required=True, type=instant)
    args = p.parse_args(argv)
    if not re.fullmatch("[a-f0-9]{40}", args.deployment_sha):
        p.error("deployment-sha must be a full Git SHA from the delivery receipt")
    raw_snapshot = args.snapshot.read_bytes()
    control = json.loads(args.control.read_text())
    result = prepare(json.loads(raw_snapshot), control, args.deployed_at, args.cutoff)
    if sha256(raw_snapshot).hexdigest() != control.get("snapshot_sha256"):
        result["issues"].append("snapshot_digest_unverified")
    packet = json.dumps(result, ensure_ascii=True, separators=(",", ":")).replace("'", "''")
    template = (ROOT / "docs/operations/sql/fbref_lineups_day14.sql").read_text()
    print(template.replace("__DEPLOYED_AT__", args.deployed_at.isoformat())
          .replace("__CUTOFF__", args.cutoff.strftime("%Y-%m-%d %H:%M:%S.%f UTC"))
          .replace("__DEPLOYMENT_SHA__", args.deployment_sha)
          .replace("__COHORT_JSON__", packet))


if __name__ == "__main__":
    main()
