"""Weekly count comparison from the same read-only Bronze snapshot as the daily meter."""

from datetime import timedelta
from typing import Mapping

from scrapers.fbref.daily_report import (
    _edition_token, _fotmob_counts, _scope, _without_reference, build_daily_report, instant,
    match_id_from_url, season_token,
)


def completed_week(as_of):
    end = instant(as_of).date()
    end -= timedelta(days=end.weekday())
    return end - timedelta(days=7), end


def _weekly_identities(*groups):
    """Count each competition/match once; check seasons within each source.

    Groups are the raw weekly rows of each source before played/finished/ready
    filters; ``eligible`` only decides counting, never conflict visibility.
    match_id is a per-source namespace, so seasons are never compared across groups.
    """
    conflict, identities = False, []
    for group in groups:
        seen, seasons = set(), {}
        for cid, mid, season, eligible in group:
            if mid is None:
                continue
            key = (str(cid), str(mid))
            seasons.setdefault(key, set()).add(season)
            if eligible:
                seen.add(key)
        identities.append(seen)
        conflict = conflict or any(len(values) > 1 for values in seasons.values())
    return identities, conflict


def _in_week(value, start, end):
    try:
        kickoff = instant(value)
    except (ValueError, TypeError):
        return False
    return kickoff is not None and start <= kickoff.date() < end


def _week_snapshot(snapshot, mapping, start, end):
    """Select editions with fixtures in the window, retaining current fallback.

    Either source may reveal an edition absent from the other source's fixtures.
    Never infer edition closure from the registry's current flag.
    """
    def in_window(value):
        return _in_week(value, start, end)

    pairs = {(str(r["source_competition_id"]), str(r["source_season_id"]))
             for r in snapshot.get("schedules", []) if start.isoformat() <= str(r.get("date")) < end.isoformat()}
    # Source dates are date-only; observations carry authoritative UTC kickoffs.
    pairs.update((str(r["competition_id"]), str(r["season_id"]))
                 for r in snapshot.get("observations", []) if in_window(r.get("kickoff_at")))
    for cid, entry in mapping.items():
        ids = {str(fid) for fid in entry.get("fotmob_ids", [])}
        for row in snapshot.get("fotmob_matches", []):
            fid = str(row["competition_id"])
            token = _edition_token(row.get("season_key"))
            try:
                kickoff = instant(row.get("utc_time"))
            except (ValueError, TypeError):
                kickoff = None
            if fid not in ids or (kickoff is not None and not in_window(kickoff)):
                continue
            matched = set()
            for season in snapshot["seasons"]:
                if str(season["competition_id"]) != cid:
                    continue
                sid = str(season["season_id"])
                aliases = entry.get("season_keys", {}).get(sid, {}).get(fid, [])
                if (row.get("season_key") in aliases or token is None
                        or token == season_token(sid)):
                    matched.add((cid, sid))
            # Retain editions revealed only by FotMob so missing registry rows
            # become explicit weekly gaps, including editions older than current.
            pairs.update(matched or {(cid, token or str(row.get("season_key") or "unknown"))})
    selected = {cid for cid, _ in pairs}
    return dict(snapshot, seasons=[dict(s, is_current=(
        (str(s["competition_id"]), str(s["season_id"])) in pairs
        if str(s["competition_id"]) in selected else s.get("is_current")))
        for s in snapshot["seasons"]]), pairs


def build_weekly_report(snapshot: Mapping, *, as_of, mapping: Mapping) -> dict:
    as_of = instant(as_of)
    start, end = completed_week(as_of)
    coverage = snapshot.get("coverage", {})
    try:
        covered = (coverage["start"] <= start.isoformat()
                   and coverage["end"] >= (end - timedelta(days=1)).isoformat()
                   and instant(coverage["as_of"]) == as_of)
    except (KeyError, ValueError, TypeError):
        covered = False
    snapshot, window_pairs = _week_snapshot(snapshot, mapping, start, end)
    days = [build_daily_report(snapshot, day=start + timedelta(days=i),
                               as_of=as_of, mapping=mapping) for i in range(7)]
    adults, pairs, gaps = _scope(snapshot)
    expected = {cid for cid, _ in pairs} | {g["competition_id"] for g in gaps}
    gaps.extend({"competition_id": cid, "season_id": sid,
                 "reason": "missing_or_unavailable_week_season"}
                for cid, sid in sorted(window_pairs - pairs) if cid in expected)
    rows, unmapped = [], []
    for cid in sorted(expected):
        entry = mapping.get(cid, {})
        problems = [g for g in gaps if g["competition_id"] == cid]
        if not covered:
            problems.append({"reason": "snapshot_week_not_covered"})
        problems.extend(snapshot.get("errors", []))
        counts, reference_problems = [], []
        for pair in sorted(p for p in pairs if p[0] == cid):
            for i in range(7):
                values, issues = _fotmob_counts(
                    snapshot, mapping, {pair}, start + timedelta(days=i), as_of,
                    registered_seasons=frozenset(
                        season_token(s["season_id"]) for s in snapshot["seasons"]
                        if str(s["competition_id"]) == cid))
                counts.append(values.get(cid))
                reference_problems.extend(issues)
        # A moved fixture can appear in two daily cohorts in a saved snapshot.
        # Conflicting dates are not evidence of two distinct matches.
        fixture_days = {}
        reference_matches = []
        mapped_ids = {str(fid) for fid in entry.get("fotmob_ids", [])}
        for fixture in snapshot.get("fotmob_matches", []):
            fid = str(fixture["competition_id"])
            if fid not in mapped_ids:
                continue
            try:
                kickoff = instant(fixture.get("utc_time"))
            except (ValueError, TypeError):
                kickoff = None
            if kickoff is not None and start <= kickoff.date() < end:
                key = (fid, str(fixture["match_id"]))
                fixture_days.setdefault(key, set()).add(kickoff.date())
                season = next((season_token(sid) for pcid, sid in sorted(pairs)
                               if pcid == cid and fixture.get("season_key") in
                               entry.get("season_keys", {}).get(sid, {}).get(fid, [])),
                              _edition_token(fixture.get("season_key")))
                eligible = (fixture.get("finished") is True
                            and not any(fixture.get(k) is True for k in ("cancelled", "awarded"))
                            and str(fixture.get("postponed") or "").lower() in {"", "false"}
                            and kickoff <= as_of)
                reference_matches.append((cid, fixture["match_id"], season, eligible))
        if any(len(dates) > 1 for dates in fixture_days.values()):
            reference_problems.append({"reason": "conflicting_fotmob_fixture_dates"})
        if not entry.get("fotmob_ids"):
            reason = ("absent_from_fotmob_catalog" if _without_reference(entry, as_of)
                      else "unmapped_fotmob_competition")
            unmapped.append({"competition_id": cid, "name": adults[cid]["name"],
                             "reason": reason, "detail": entry.get("reason")})
            reference_problems.append({"reason": reason})
        matches = [r for d in days for r in d["matches"] if r["competition_id"] == cid]
        if any(not r["date_confirmed"] for r in matches):
            problems.append({"reason": "fbref_fixture_time_unknown"})
        # READINESS_SQL supplies only validated generic + typed manifests with
        # matching hashes. Late arrivals count here; previews and future proof do not.
        ready_keys = {(r["season_id"], r["match_id"]) for r in matches
                      if r["status"] in {"on_time", "late"} and r["bronze_ready_at"]
                      and r["first_completed_seen_at"]
                      and instant(r["bronze_ready_at"]) >= instant(r["first_completed_seen_at"])}
        # Raw week rows (unplayed/unfinished included) so a second-season copy
        # of a match cannot hide behind the played-score filter.
        raw_fbref = {(str(r["source_season_id"]), match_id_from_url(r.get("match_url")))
                     for r in snapshot.get("schedules", [])
                     if str(r["source_competition_id"]) == cid
                     and start.isoformat() <= str(r.get("date")) < end.isoformat()}
        raw_fbref.update((str(r["season_id"]), str(r["match_id"]))
                         for r in snapshot.get("observations", [])
                         if str(r["competition_id"]) == cid and _in_week(r.get("kickoff_at"), start, end))
        identities, season_conflict = _weekly_identities(reference_matches, (
            (cid, mid, season_token(sid), (sid, mid) in ready_keys)
            for sid, mid in sorted(raw_fbref | ready_keys, key=str)))
        if season_conflict:
            problems.append({"reason": "season_conflict"})
        reference = (len(identities[0]) if not problems and not reference_problems
                     and counts and all(n is not None for n in counts) else None)
        ready = None if problems else len(identities[1])
        evidence = []
        for p in [*problems, *reference_problems]:
            if p not in evidence:
                evidence.append(p)
        rows.append({"competition_id": cid, "name": adults[cid]["name"],
                     "fotmob": reference, "bronze_ready": ready,
                     "difference": ready - reference if ready is not None and reference is not None else None,
                     "mapping": {"fotmob_ids": entry.get("fotmob_ids", []),
                                 "basis": "explicit_id_map" if entry.get("fotmob_ids") else None,
                                 "confidence": entry.get("confidence", "not_assessed"),
                                 "evidence": entry.get("reference_evidence"),
                                 "fbref_name": entry.get("fbref_name"),
                                 "fotmob_names": entry.get("fotmob_names", [])},
                     "evidence": evidence})
    return {"schema_version": "fbref-weekly-v1", "start": start.isoformat(), "end_exclusive": end.isoformat(),
            "as_of": as_of.isoformat(), "snapshot_covered": covered,
            "competitions": rows, "unmapped": unmapped,
            "limitations": "Current snapshot cannot reconstruct overwritten Bronze; equal counts do not prove match identity."}


def weekly_lines(report: Mapping) -> list[str]:
    def cell(value):
        return "н/д" if value is None else str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")
    period = f"{report['start']} — {report['end_exclusive']} (конец исключён), UTC"
    coverage = "покрыт" if report["snapshot_covered"] else "н/д: snapshot_week_not_covered"
    lines = [f"FBref: недельная полнота {period}; снимок {coverage}",
             "| Турнир | Период UTC | FotMob N | FBref-Bronze M | M−N | Доказательства; сопоставление |",
             "| --- | --- | ---: | ---: | ---: | --- |"]
    for row in report["competitions"]:
        state = ", ".join(sorted({p["reason"] for p in row["evidence"]})) or "подтверждено"
        mapping = row["mapping"]
        state += f"; {mapping['basis']}; confidence={mapping['confidence']}"
        cells = [row["name"], period, row["fotmob"], row["bronze_ready"], row["difference"], state]
        lines.append("| " + " | ".join(cell(c) for c in cells) + " |")
    lines.append("Без соответствия FotMob: " + ("; ".join(
        f"{cell(r['name'])} ({r['competition_id']}): {cell(r['reason'])}"
        for r in report["unmapped"]) or "нет"))
    lines.append("Ограничения недели: текущий снимок не восстанавливает перезаписанный Bronze; равенство количеств не доказывает совпадение матчей.")
    return lines
