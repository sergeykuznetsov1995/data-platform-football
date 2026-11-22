"""Read-only inter-day DQ for Bronze snapshots (#1468).

No identity mapping or ingestion changes. A rating date is one observation,
regardless of the number of four-hourly replacements of its partition.
The same read-only approach detects /Fixtures gaps by anti-joining raw and parsed dates (#2279).
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
# 30 days keeps a nonfatal parse gap visible for a month: parsed rows for a
# missed date appear only when a later run reparse succeeds, and 04.11 was
# already 18 days old when #2279 landed — a shorter window would hide it.
FIXTURES_GAP_LOOKBACK_DAYS = 30
RAW_PAGE_TABLE = "iceberg.bronze.clubelo_raw_page"
FIXTURES_TABLE = "iceberg.bronze.clubelo_fixture"


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


def _counts_by_date(rows):
    """Per-date counts; a GROUP BY count(*) answer is malformed below 1 (Astra r1)."""
    counts = Counter()
    for day, count in rows:
        value = int(str(count))
        if value < 1:
            raise ValueError(count)
        counts[date.fromisoformat(str(day))] += value
    return counts


def read_fixtures_gap(trino, *, today: date) -> dict:
    """Find completed days with a raw /Fixtures page and no parsed fixture rows."""
    try:
        first = today - timedelta(days=FIXTURES_GAP_LOOKBACK_DAYS)
        window = (f"rating_date >= DATE '{first.isoformat()}' "
                  f"AND rating_date < DATE '{today.isoformat()}'")
        raw_rows = trino.execute_query(
            f"SELECT cast(rating_date AS varchar), count(*) FROM {RAW_PAGE_TABLE} "
            f"WHERE page = '/Fixtures' AND {window} GROUP BY 1"
        )
        parsed_rows = trino.execute_query(
            f"SELECT cast(rating_date AS varchar), count(*) FROM {FIXTURES_TABLE} "
            f"WHERE {window} GROUP BY 1"
        )
    except Exception:
        return {"status": "unavailable", "reason": "ошибка чтения Bronze", "gaps": []}

    # No raw page in the whole window is not "no gaps" (a month without a single
    # /Fixtures fetch must stay visible), and an empty raw answer cannot be told
    # apart from a lost one — both fail closed (Astra r1).
    if not raw_rows:
        return {"status": "unavailable", "reason": "нет raw /Fixtures за окно", "gaps": []}

    try:
        raw = _counts_by_date(raw_rows)
        parsed = _counts_by_date(parsed_rows)
    except Exception:
        return {"status": "unavailable", "reason": "не разобран ответ Bronze", "gaps": []}

    return {
        "status": "ok", "window_from": first, "window_to_exclusive": today,
        "raw_dates": len(raw), "parsed_dates": sum(count > 0 for count in parsed.values()),
        "gaps": [{"rating_date": day, "raw_pages": raw[day]}
                 for day in sorted(raw) if not parsed.get(day)],
    }


def format_fixtures_gap(report: dict) -> str:
    """Format the /Fixtures gap report for the morning summary."""
    if report["status"] != "ok":
        return f"fixtures: raw-без-parsed не посчитано — {report['reason']}"
    gaps = report["gaps"]
    if not gaps:
        return (f"fixtures: raw-без-parsed нет ({report['raw_dates']} дат с raw "
                f"за {FIXTURES_GAP_LOOKBACK_DAYS} дн)")
    dates = ", ".join(f"{gap['rating_date']:%d.%m} ({gap['raw_pages']})" for gap in gaps)
    return f"fixtures: raw-без-parsed {len(gaps)} дат — {dates}"
