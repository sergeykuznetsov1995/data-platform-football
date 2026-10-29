"""Read-only inter-day DQ for Bronze snapshots (#1468).

No identity mapping or ingestion changes. A rating date is one observation,
regardless of the number of four-hourly replacements of its partition.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta

COLUMNS = (
    "rating_date", "club_key", "slug", "name", "country", "is_provisional",
    "level_section", "elo_delta_1d_raw",
)
MAX_ROWS = 20000
TABLE = "iceberg.bronze.clubelo_rank_snapshot"


def analyze_snapshots(rows, *, today: date, max_age_days: int = 3):
    """Calculate exact metrics; preserve case, raw delta strings and NULL."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["rating_date"]].append(row)
    if not grouped:
        return {"status": "incomplete", "reason": "нет снимков", "days": []}
    latest = max(grouped)
    expected = [latest - timedelta(days=i) for i in range(4, -1, -1)]
    missing = [day for day in expected if day not in grouped]
    stale = (today - latest).days > max_age_days or latest > today
    days = []
    for day in expected:
        snapshot = grouped.get(day)
        if not snapshot:
            continue
        pairs = defaultdict(set)
        for row in snapshot:
            values = pairs[(row["name"], row["country"])]
            if row["slug"]:
                values.add(row["slug"])
        slugs = {row["slug"] for row in snapshot if row["slug"]}
        regular = [row for row in snapshot if not row["is_provisional"]]
        provisional = sum(bool(row["is_provisional"]) for row in snapshot)
        unmatched = sum(not row["level_section"] for row in regular)
        previous = days[-1] if days and days[-1]["rating_date"] == day - timedelta(days=1) else None
        changes = []
        if previous:
            for pair in sorted(pairs.keys() & previous["pairs"].keys(), key=repr):
                before, after = previous["pairs"][pair], pairs[pair]
                if before != after:
                    changes.append({"name": pair[0], "country": pair[1],
                                    "before": sorted(before), "after": sorted(after)})
        days.append({
            "rating_date": day, "rows": len(snapshot), "slugs": slugs, "pairs": dict(pairs),
            "appeared": sorted(slugs - previous["slugs"]) if previous else None,
            "disappeared": sorted(previous["slugs"] - slugs) if previous else None,
            "slug_changes": changes if previous else None,
            "duplicate_candidates": [
                {"name": pair[0], "country": pair[1], "slugs": sorted(values)}
                for pair, values in sorted(pairs.items(), key=lambda item: repr(item[0]))
                if len(values) > 1
            ],
            "provisional": provisional,
            "provisional_share": provisional / len(snapshot),
            "unmatched_levels": unmatched,
            "regular_rows": len(regular),
            "unmatched_level_share": unmatched / len(regular) if regular else None,
            "without_country": sorted(row["club_key"] for row in snapshot if not row["country"]),
            "afc": sorted(row["club_key"] for row in snapshot if row["country"] == "AFC"),
            "delta_raw": dict(sorted(Counter(row["elo_delta_1d_raw"] for row in snapshot).items(),
                                     key=lambda item: repr(item[0]))),
        })
    return {"status": "incomplete" if missing or stale else "complete",
            "latest": latest, "missing_dates": missing, "stale": stale,
            "five_consecutive_days": not missing, "days": days}


def read_report(trino, *, today: date, max_age_days: int = 3):
    """Two bounded SELECTs. Truncation, races and read failures never look green."""
    try:
        latest_rows = trino.execute_query(f"SELECT max(rating_date) FROM {TABLE}")
        latest = latest_rows[0][0] if latest_rows else None
        if latest is None:
            return analyze_snapshots([], today=today, max_age_days=max_age_days)
        first = latest - timedelta(days=4)
        rows = trino.execute_query(
            f"SELECT {', '.join(COLUMNS)} FROM {TABLE} "
            f"WHERE rating_date BETWEEN DATE '{first.isoformat()}' "
            f"AND DATE '{latest.isoformat()}' LIMIT {MAX_ROWS + 1}"
        )
        if len(rows) > MAX_ROWS:
            return {"status": "unavailable", "reason": "превышен лимит строк", "days": []}
        report = analyze_snapshots([dict(zip(COLUMNS, row)) for row in rows],
                                   today=today, max_age_days=max_age_days)
        if report.get("latest") != latest:
            return {"status": "unavailable", "reason": "снимок изменился при чтении", "days": []}
        return report
    except Exception:
        # Do not put connection details/credentials from driver exceptions in summaries.
        return {"status": "unavailable", "reason": "ошибка чтения Bronze", "days": []}


def _examples(values):
    examples = []
    for value in values[:5]:
        if isinstance(value, dict):
            value = {key: ({"count": len(item), "examples": item[:5]}
                           if isinstance(item, list) else item)
                     for key, item in value.items()}
        examples.append(value)
    return examples


def format_summary(report):
    """Pure formatter. Limit examples, but retain exact counts and daily deltas."""
    lines = [f"ClubElo DQ: {report['status']}"]
    if report.get("reason"):
        lines.append(f"DQ: {report['reason']}")
    if report.get("latest"):
        lines.append(f"DQ даты: latest={report['latest']}; stale={report['stale']}; "
                     f"5 дней подряд={report['five_consecutive_days']}; "
                     f"пропуски={report['missing_dates']}")
    for day in report["days"]:
        lines.append(f"DQ {day['rating_date']}: rows={day['rows']}")
        for key in ("appeared", "disappeared", "slug_changes", "duplicate_candidates",
                    "without_country", "afc"):
            values = day[key]
            lines.append(f"  {key}: нет сравнения" if values is None else
                         f"  {key}: {len(values)}; примеры={_examples(values)!r}")
        lines.append(f"  provisional: {day['provisional']}/{day['rows']} "
                     f"({day['provisional_share']:.2%})")
        share = day['unmatched_level_share']
        lines.append(f"  unmatched_levels: {day['unmatched_levels']}/{day['regular_rows']} "
                     f"({format(share, '.2%') if share is not None else 'нет знаменателя'})")
        lines.append(f"  elo_delta_1d_raw: {day['delta_raw']!r}")
    return lines
