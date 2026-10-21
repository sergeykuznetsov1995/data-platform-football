"""Daily FBref milestone measurement from a fixed, read-only snapshot.

Fixture days use explicit UTC kickoff timestamps. Local times without an
offset are never reinterpreted as UTC. First-seen times are observations, not exact publication timestamps.
"""

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import re
from statistics import median
from typing import Mapping
from zoneinfo import ZoneInfo

from urllib.parse import urlparse
from scrapers.fbref.report_observations import played_score
from scrapers.fbref.policy import CURRENT_ENRICHMENT, PUBLICATION_FRESHNESS_PAGE_KINDS

REPORT_VERSION = "fbref-daily-v1"
EXCLUDED_IDS = {"Big5", "850", "851", "852", "853"}
NON_ADULT = re.compile(
    r"women|female|youth|reserve|academy|junior|primavera|"
    r"\bu[- ]?\d{2}\b|\bunder[- ]?\d{2}\b|premier league 2", re.I
)


def match_id_from_url(value: object) -> str | None:
    if not value:
        return None
    parsed = urlparse(str(value))
    if parsed.netloc and parsed.netloc not in {"fbref.com", "www.fbref.com"}:
        return None
    match = re.match(r"^/en/matches/([0-9a-f]{8})(?:/|$)", parsed.path, re.I)
    return match.group(1).lower() if match else None


def instant(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    result = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamp lacks an explicit timezone")
    return result.astimezone(timezone.utc)


def adult_competition(row: Mapping) -> bool:
    return (
        row.get("gender") == "male"
        and str(row["competition_id"]) not in EXCLUDED_IDS
        and not NON_ADULT.search(" ".join([
            str(row.get("name", "")), str(row.get("classification", "")),
            str((row.get("metadata") or {}).get("source_section", "")),
        ]))
    )


def season_token(value: object) -> str | None:
    value = str(value or "").strip()
    match = re.fullmatch(r"(\d{4})(?:[-/](\d{2}|\d{4}))?", value)
    if not match:
        return None
    first, second = match.groups()
    return first if not second else first + "-" + (
        first[:2] + second if len(second) == 2 else second
    )


def percentile95(values: list[float]) -> float | None:
    """Continuous percentile, matching PostgreSQL percentile_cont(0.95)."""
    if not values:
        return None
    values = sorted(values)
    rank = .95 * (len(values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (rank - lower)


def _stats(values: list[float]) -> dict:
    return {"samples": len(values), "median_hours": median(values) if values else None,
            "p95_hours": percentile95(values)}


def _scope(snapshot: Mapping) -> tuple[dict, set, list]:
    adults = {str(c["competition_id"]): c for c in snapshot["competitions"]
              if adult_competition(c)}
    retired = {
        cid for cid, c in adults.items()
        if c.get("crawl_state") == "skipped"
        and (c.get("metadata") or {}).get("current_scope_lifecycle") == "discontinued"
        and (c.get("metadata") or {}).get("current_scope_reason")
    }
    expected = set(adults) - retired
    active = {cid for cid in expected if adults[cid].get("crawl_state") == "active"
              and adults[cid].get("present")
              and adults[cid].get("lifecycle_state") in {"present", "missing_once"}}
    pairs = {(str(s["competition_id"]), str(s["season_id"]))
             for s in snapshot["seasons"] if str(s["competition_id"]) in expected
             and s.get("is_current") and s.get("present")
             and s.get("lifecycle_state") == "present"}
    health = {(str(s["competition_id"]), str(s["season_id"])): s
              for s in snapshot.get("schedule_health", [])}
    gaps = [{"competition_id": cid, "season_id": sid, "reason": "missing_or_unfetched_schedule"}
            for cid, sid in sorted(pairs)
            if not health.get((cid, sid), {}).get("fetched")]
    gaps.extend({"competition_id": cid, "reason": "missing_current_season"}
                for cid in sorted(expected - {cid for cid, _ in pairs}))
    gaps.extend({"competition_id": cid, "reason": "adult_registry_not_active"}
                for cid in sorted(expected - active))
    return adults, pairs, gaps


def _day_comparator_scope(snapshot: Mapping, pairs: set, day: date) -> tuple[set, list]:
    """Keep every current pair without positive evidence of edition closure.

    A year in the registry, a fresh schedule fetch and empty day windows do
    not establish closure. The snapshot has no authoritative edition bounds
    or complete tournament catalogue, so it cannot justify exclusions.
    """
    return pairs, []


def _edition_token(key: object) -> str | None:
    # Detection only: this never makes an unknown phase an accepted alias.
    return season_token(str(key or "").split(" - ", 1)[0])


def _fotmob_season_keys(entry: Mapping, fid: object, sid: str) -> set[str]:
    # Only exact, competition-specific aliases are allowed. Never strip phases
    # globally or assume that an absent phase has been fetched successfully.
    aliases = entry.get("season_keys", {}).get(sid, {}).get(str(fid))
    return set(aliases) if aliases else set()


def _fotmob_counts(snapshot: Mapping, mapping: Mapping, pairs: set, day: date,
                   as_of: datetime) -> tuple[dict, list]:
    counts = Counter()
    problems = []
    seasons = snapshot.get("fotmob_seasons", [])
    fixtures = snapshot.get("fotmob_matches", [])
    for cid, sid in sorted(pairs):
        canonical_season = season_token(sid)
        if canonical_season is None:
            problems.append({"competition_id": cid, "reason": "unsupported_season_identity"})
            continue
        entry = mapping.get(cid, {})
        ids = entry.get("fotmob_ids", [])
        if not ids:
            problems.append({"competition_id": cid, "reason": "unmapped_fotmob_competition",
                             "detail": entry.get("reason", "unmapped")})
            continue
        seen = set()
        valid = True
        for fid in ids:
            aliases = _fotmob_season_keys(entry, fid, sid)
            def matches_season(key):
                return key in aliases if aliases else season_token(key) == canonical_season
            known_keys = {str(r.get("season_key") or "")
                          for r in [*seasons, *fixtures]
                          if str(r["competition_id"]) == str(fid)}
            newer = sorted(key for key in known_keys
                           if _edition_token(key) and
                           int(_edition_token(key)[-4:]) > int(canonical_season[-4:]))
            if newer:
                problems.append({"competition_id": cid, "season_id": sid,
                                 "fotmob_id": str(fid), "reason": "fbref_registry_behind_fotmob",
                                 "newer_season_keys": newer})
                valid = False
            unknown_phases = sorted(key for key in known_keys
                                    if (_edition_token(key) == canonical_season
                                        or _edition_token(key) is None)
                                    and not matches_season(key))
            if unknown_phases:
                problems.append({"competition_id": cid, "season_id": sid,
                                 "fotmob_id": str(fid), "reason": "unknown_fotmob_season_phase",
                                 "season_keys": unknown_phases})
                valid = False
            # Every explicit phase must have its own fresh confirmation.
            required = [{key} for key in sorted(aliases)] if aliases else [
                {key for key in known_keys if matches_season(key)}]
            for keys in required:
                available = [s for s in seasons if str(s["competition_id"]) == str(fid)
                             and s.get("season_key") in keys]
                confirmations = [instant(s.get("validated_at") or s.get("fetched_at"))
                                 for s in available if not s.get("stale")]
                confirmations = [t for t in confirmations if t is not None and t <= as_of]
                latest = max(confirmations, default=None)
                if latest is None or as_of - latest > timedelta(hours=24):
                    problems.append({"competition_id": cid, "fotmob_id": str(fid), "season_id": sid,
                                     "season_keys": sorted(keys),
                                     "reason": "stale_fotmob_season" if latest else "missing_fotmob_season",
                                     "last_confirmed_at": latest.isoformat() if latest else None,
                                     "age_hours": (as_of-latest).total_seconds()/3600 if latest else None})
                    valid = False
            for row in fixtures:
                if str(row["competition_id"]) != str(fid) or not matches_season(row.get("season_key")):
                    continue
                if row.get("finished") is not True or any(row.get(k) is True for k in ("cancelled", "awarded")) or str(row.get("postponed") or "").lower() not in {"", "false"}:
                    continue
                try:
                    kickoff = instant(row.get("utc_time"))
                except (ValueError, TypeError):
                    kickoff = None
                if kickoff is None:
                    valid = False
                    problems.append({"competition_id": cid, "reason": "fotmob_fixture_time_unknown"})
                elif kickoff <= as_of and kickoff.date() == day:
                    seen.add((str(fid), str(row["match_id"])))
        counts[cid] = len(seen) if valid else None
    return dict(counts), problems


def page_age_metrics(snapshot: Mapping, as_of: datetime) -> dict:
    adults, pairs, _ = _scope(snapshot)
    active = {cid for cid,c in adults.items() if c.get("crawl_state") == "active"
              and c.get("present") and c.get("lifecycle_state") in {"present", "missing_once"}}
    buckets = {kind: [] for kind in PUBLICATION_FRESHNESS_PAGE_KINDS}
    excluded = Counter(snapshot.get("page_age_exclusions") or {})
    seen = set()
    standings = {(r["target_id"], r["content_hash"]) for r in snapshot.get("standings_inventory", [])}
    standings_confirmed = 0
    for row in snapshot.get("page_ages", []):
        if row["target_id"] in seen:
            continue
        seen.add(row["target_id"])
        kind = row["page_kind"]
        buckets.setdefault(kind, [])
        reason = None
        if row.get("state") in {"quarantined", "dead", "skipped"}:
            reason = row["state"]
        elif row.get("refresh_policy") == "historical_once":
            reason = "history"
        elif kind == "squad" and any(part in {"matchlogs", "goallogs", "history", "wages"}
                for part in urlparse(row["canonical_url"]).path.split("/")[4:]):
            reason = "non_roster"
        elif kind != "competition_index" and (not row.get("current_ownership") or not any(
            str(scope["competition_id"]) in active and
            (kind == "competition" or (str(scope["competition_id"]), str(scope["season_id"])) in pairs)
            for scope in row.get("scopes", [])
        )):
            reason = "outside_current_adult_scope"
        if reason:
            excluded[reason] += 1
            continue
        if (kind == "season" and row.get("processed") and
                (row["target_id"], row.get("last_content_hash")) in standings):
            standings_confirmed += 1
        fetched = instant(row.get("last_fetched_at"))
        buckets[kind].append(None if fetched is None else (as_of-fetched).total_seconds()/3600)
    def metrics(values, hours):
        ages = [v for v in values if v is not None and v >= 0]
        fresh = sum(0 <= v < hours for v in ages)
        return {"total": len(values), "sla_hours": hours, "fresh": fresh,
                "fresh_percent": 100*fresh/len(values) if values else None,
                "never": values.count(None), "aged": sum(v >= hours for v in ages),
                "median_hours": median(ages) if ages else None,
                "p95_hours": percentile95(ages), "max_hours": max(ages) if ages else None}
    by_kind = {kind: metrics(values, 672 if kind == "squad" else
                           24 if kind in {"match", "schedule", "competition_index"} else
                           720 if kind in {"player", "matchlog"} else 168)
               for kind, values in sorted(buckets.items())}
    total = sum(by_kind[k]["total"] for k in CURRENT_ENRICHMENT)
    fresh = sum(by_kind[k]["fresh"] for k in CURRENT_ENRICHMENT)
    return {"by_kind": by_kind, "total": total, "fresh": fresh,
            "fresh_percent": 100*fresh/total if total else None, "excluded": dict(excluded),
            "standings_confirmed_seasons": standings_confirmed,
            "errors": list(snapshot.get("page_age_errors", []))}


def page_age_line(report: Mapping) -> str:
    chunks = []
    for kind, row in report["page_ages"]["by_kind"].items():
        pct = f"{row['fresh_percent']:.1f}%" if row["fresh_percent"] is not None else "н/д"
        ages = "/".join(f"{row[k]:.1f}" if row[k] is not None else "н/д"
                        for k in ("median_hours", "p95_hours", "max_hours"))
        chunks.append(f"{kind}: N={row['total']}, fresh{row['sla_hours']//24}={row['fresh']} ({pct}), "
                      f"never={row['never']}, aged={row['aged']}, median/p95/max={ages} ч")
    overall = report["page_ages"]
    pct = f"{overall['fresh_percent']:.1f}%" if overall["fresh_percent"] is not None else "н/д"
    chunks.append(f"season/season_stats/squad: {overall['fresh']}/{overall['total']} ({pct})")
    chunks.append(f"season results*_overall={overall['standings_confirmed_seasons']}")
    chunks.append("исключения=" + str(overall["excluded"]))
    if overall["errors"]:
        chunks.append("н/д: ошибки измерения=" + str(overall["errors"]))
    return "Возраст по видам страниц: " + "; ".join(chunks)


def build_daily_report(snapshot: Mapping, *, day: date, as_of: datetime,
                       mapping: Mapping) -> dict:
    as_of = instant(as_of)
    if as_of is None:
        raise ValueError("as_of is required")
    adults, pairs, gaps = _scope(snapshot)
    observations = {(str(o["competition_id"]), str(o["season_id"]), str(o["match_id"])): o
                    for o in snapshot.get("observations", [])}
    readiness = {(str(o["competition_id"]), str(o["season_id"]), str(o["match_id"])): o
                 for o in snapshot.get("readiness", [])}
    matches = {}
    for row in snapshot.get("schedules", []):
        cid, sid = str(row["source_competition_id"]), str(row["source_season_id"])
        if (cid, sid) not in pairs:
            continue
        if not played_score(row.get("score"), row.get("notes")):
            continue
        url = row.get("match_url")
        mid = match_id_from_url(url)
        kickoff = instant(observations.get((cid, sid, mid), {}).get("kickoff_at"))
        fixture_day = kickoff.date().isoformat() if kickoff else str(row.get("date"))
        if fixture_day != day.isoformat():
            continue
        # A played fixture without a report must stay in the denominator.
        key = (cid, sid, mid or (str(row.get("home")), str(row.get("away")), str(row.get("time"))))
        matches.setdefault(key, {**row, "match_id": mid})
    entries = []
    source_lags, fetch_lags, bronze_lags = [], [], []
    for (cid, sid, _), row in matches.items():
        mid = row["match_id"]
        obs = observations.get((cid, sid, mid), {})
        proof = readiness.get((cid, sid, mid), {})
        first_seen = instant(obs.get("first_seen_at"))
        anchor = instant(obs.get("first_completed_seen_at"))
        ready = instant(proof.get("bronze_ready_at"))
        fetched = instant(proof.get("first_fetch_at"))
        kickoff = instant(obs.get("kickoff_at"))
        first_seen = first_seen if first_seen is None or first_seen <= as_of else None
        anchor = anchor if anchor is None or anchor <= as_of else None
        ready = ready if ready is None or ready <= as_of else None
        fetched = fetched if fetched is None or fetched <= as_of else None
        deadline = anchor + timedelta(hours=24) if anchor else None
        if not mid or anchor is None:
            status = "unknown"
        elif ready is not None and ready >= anchor:
            status = "on_time" if ready <= deadline else "late"
        elif ready is not None:
            status = "unknown"  # A preview fetched before the completed report is not proof.
        else:
            status = "late" if deadline <= as_of else "pending"
        source_lag = None
        if anchor is not None and kickoff is not None and kickoff <= as_of:
            assumed_finish = kickoff + timedelta(hours=2)
            source_lag = (anchor - assumed_finish).total_seconds() / 3600
            # A negative value disproves the two-hour estimate for this match.
            if source_lag >= 0:
                source_lags.append(source_lag)
                if fetched is not None and fetched >= assumed_finish:
                    fetch_lags.append((fetched - assumed_finish).total_seconds() / 3600)
                if ready is not None and ready >= assumed_finish:
                    bronze_lags.append((ready - assumed_finish).total_seconds() / 3600)
            else:
                source_lag = None
        entries.append({
            "competition_id": cid, "season_id": sid, "match_id": mid,
            "match_url": row.get("match_url"), "home": row.get("home"), "away": row.get("away"),
            "status": status, "kickoff_at": kickoff.isoformat() if kickoff else None,
            "date_confirmed": kickoff is not None, "first_seen_at": first_seen.isoformat() if first_seen else None,
            "first_completed_seen_at": anchor.isoformat() if anchor else None,
            "first_seen_raw_key": obs.get("first_seen_raw_key"),
            "first_completed_raw_key": obs.get("first_completed_raw_key"),
            "deadline": deadline.isoformat() if deadline else None,
            "first_fetch_at": fetched.isoformat() if fetched else None,
            "bronze_ready_at": ready.isoformat() if ready else None,
            "source_lag_hours": source_lag,
        })
    totals = Counter(row["status"] for row in entries)
    comparator_pairs, day_scope_exclusions = _day_comparator_scope(snapshot, pairs, day)
    fm_counts, fm_problems = _fotmob_counts(snapshot, mapping, comparator_pairs, day, as_of)
    errors = list(snapshot.get("errors", []))
    population_complete = not gaps and not errors and bool(pairs) and all(r["date_confirmed"] for r in entries)
    completed = population_complete and bool(entries) and not totals["unknown"] and all(
        instant(row["deadline"]) <= as_of for row in entries
    )
    pct = 100 * totals["on_time"] / len(entries) if entries and population_complete and not totals["unknown"] else None
    by_competition = []
    for cid in sorted({cid for cid, _ in comparator_pairs}):
        rows = [r for r in entries if r["competition_id"] == cid]
        count = Counter(r["status"] for r in rows)
        k = fm_counts.get(cid)
        by_competition.append({"competition_id": cid, "name": adults[cid]["name"],
                               "played": len(rows), **{s: count[s] for s in ("on_time", "late", "pending", "unknown")},
                               "fotmob": k, "difference": len(rows) - k if k is not None else None})
    return {
        "schema_version": REPORT_VERSION, "date": day.isoformat(), "as_of": as_of.isoformat(),
        "date_basis": "UTC kickoff date; undated FBref rows retained by source date and marked unconfirmed",
        "adult_universe": len(adults), "active_current_scopes": len(pairs),
        "scope_gaps": gaps, "day_scope_exclusions": day_scope_exclusions,
        "errors": errors, "population_complete": population_complete,
        "played": len(entries), **{s: totals[s] for s in ("on_time", "late", "pending", "unknown")},
        "on_time_percent": pct, "final": completed,
        "milestone_day_pass": completed and pct >= 99 and not fm_problems and all(
            r["difference"] == 0 for r in by_competition
        ),
        "fotmob": sum(fm_counts.values()) if not fm_problems and not errors else None,
        "fotmob_known_count": sum(v for v in fm_counts.values() if v is not None),
        "fotmob_problems": fm_problems,
        "source_lag": {**_stats(source_lags), "unknown": len(entries) - len(source_lags),
                       "basis": "observed completed link minus explicit kickoff + 2h; upper-bound estimate"},
        "match_to_first_fetch": _stats(fetch_lags), "match_to_bronze": _stats(bronze_lags),
        "competitions": by_competition, "matches": sorted(entries, key=lambda r: (r["competition_id"], r["match_id"] or "")),
        "page_ages": page_age_metrics(snapshot, as_of),
        "violators": [r for r in entries if r["status"] == "late"],
    }


def milestone_streak(reports: list[dict]) -> int:
    """Provisional, empty or incomplete dates cannot bridge the series."""
    streak = 0
    previous = None
    for report in sorted(reports, key=lambda r: r["date"], reverse=True):
        if previous is not None and date.fromisoformat(previous) - date.fromisoformat(report["date"]) != timedelta(days=1):
            break
        if not report["final"]:
            if previous is None:
                continue
            break
        if not report["milestone_day_pass"]:
            break
        streak += 1
        previous = report["date"]
    return streak


def summary_line(report: Mapping) -> str:
    pct = report["on_time_percent"]
    percentage = f"{pct:.2f} %" if pct is not None else "н/д"
    k = report["fotmob"] if report["fotmob"] is not None else "н/д"
    state = "окончательный" if report["final"] else "предварительный"
    reasons = Counter(p["reason"] for p in report["fotmob_problems"])
    unconfirmed = sum(not r["date_confirmed"] for r in report["matches"])
    deadlines = sum(instant(r["deadline"]) > instant(report["as_of"])
                    for r in report["matches"] if r["deadline"])
    differences = sum(r["difference"] not in (None, 0) for r in report["competitions"])
    blockers = (f"UTC не подтверждено {unconfirmed}; дедлайн не истёк {deadlines}; "
                f"FotMob причины {dict(reasons)}; известно частично {report['fotmob_known_count']}; "
                f"расхождений {differences}")
    return (f"FBref: матчей {report['date']} UTC {report['played']}, ≤24 ч {report['on_time']} "
            f"({percentage}), у FotMob {k}; {state}; ожидают {report['pending']}, "
            f"неизвестно {report['unknown']}; "
            f"пробелов данных {len(report['scope_gaps']) + len(report['errors'])}; {blockers}")


def render_markdown(report: Mapping) -> str:
    def hours(value):
        return f"{value:.3f} ч" if value is not None else "н/д"
    lag = report["source_lag"]
    output = [f"# FBref — {report['date']}", "", summary_line(report), "", page_age_line(report), "",
              f"Срез: {instant(report['as_of']).astimezone(ZoneInfo('Europe/Moscow')).isoformat()} МСК.", "",
              f"Задержка FBref: медиана {hours(lag['median_hours'])}, 95-й процентиль {hours(lag['p95_hours'])}; "
              f"выборка {lag['samples']}, неизвестно {lag['unknown']}.",
              "Оценка: первое наблюдение завершённой ссылки минус начало матча +2 ч. "
              "Периодический опрос даёт верхнюю оценку появления. Местное время без пояса не используется.", "",
              f"Взрослых турниров: {report['adult_universe']}; активных сезонов: {report['active_current_scopes']}; "
              f"пробелов расписаний: {len(report['scope_gaps'])}.", "",
              "| Турнир | Сыграно | ≤24 ч | Поздно | Ждём | Неизвестно | FotMob | Разница |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in report["competitions"]:
        cells = [row["name"], *[row[k] for k in ("played", "on_time", "late", "pending", "unknown", "fotmob", "difference")]]
        output.append("| " + " | ".join("н/д" if c is None else str(c).replace("|", "\\|") for c in cells) + " |")
    output += ["", "## Нарушители", ""]
    for row in report["violators"]:
        output.append(f"- {row['competition_id']}/{row['season_id']}: [{row['match_id']}]({row['match_url']}), "
                      f"срок {row['deadline']}, Bronze {row['bronze_ready_at'] or 'не готов'}.")
    if not report["violators"]:
        output.append("Подтверждённых нарушителей нет; ожидающие и неизвестные перечислены в JSON.")
    output += ["", "## Ограничения доказательств", ""]
    for problem in [*report["scope_gaps"], *report["fotmob_problems"], *report["errors"]]:
        output.append(f"- {problem}")
    return "\n".join(output) + "\n"
