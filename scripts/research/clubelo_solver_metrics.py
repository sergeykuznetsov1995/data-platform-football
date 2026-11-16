"""Offline pure-stdlib metrics for ClubElo solver daily aggregates.

solve_fraction is the conditional rate solve_numerator / solve_denominator =
P(solved | detected): it is selection-biased toward challenges that reached
detection and excludes attempts without a detected challenge. The optional
attempts/tunnel_failed/validated counts come from a sanitized aggregate with no
attempt/session join and no full raw log; null means unknown, never zero and
never success or failure, and validated need not be a subset of solved.
"""

import json
import sys
from datetime import date, timedelta

SCHEMA_VERSION = 1
MODE = "daily_aggregates"
POPULATION_FIELDS = (
    "source", "population_id", "counting_unit", "date_basis", "provenance", "revision", "coverage",
)
FUNNEL_FIELDS = ("attempts", "tunnel_failed", "validated")
FAILURE_CATEGORIES = ("solver_timeout", "tunnel", "unknown")
EXCLUDED_KEY = "excluded_successful_run_invalid_clearance"
CENSUS_KEY = "failed_run_census_11_12"


def _nonneg_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer, got {value!r}")
    return value


def _iso_date(value, label):
    if not isinstance(value, str) or len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise ValueError(f"{label} must be an ISO YYYY-MM-DD date string, got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{label} is not a valid calendar date: {value!r}") from None


def _ratio(numerator, denominator):
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _solve_metrics(solved, detected):
    if detected == 0:
        return {"solve_fraction": None, "solve_percent_precise": None, "solve_percent": None}
    fraction = solved / detected
    return {
        "solve_fraction": fraction,
        "solve_percent_precise": round(100.0 * fraction, 2),
        "solve_percent": round(100.0 * fraction),
    }


def _build_day(entry):
    if not isinstance(entry, dict):
        raise ValueError("each day entry must be a JSON object")
    day_utc = _iso_date(entry.get("day_utc"), "day_utc").isoformat()
    if "detected" not in entry or "solved" not in entry:
        raise ValueError(f"day {day_utc} requires detected and solved counts")
    detected = _nonneg_int(entry["detected"], f"{day_utc} detected")
    solved = _nonneg_int(entry["solved"], f"{day_utc} solved")
    if solved > detected:
        raise ValueError(f"solved {solved} exceeds detected {detected} on {day_utc}")
    day = {"day_utc": day_utc, "detected": detected, "solved": solved}
    for field in FUNNEL_FIELDS:
        value = entry.get(field)
        day[field] = None if value is None else _nonneg_int(value, f"{day_utc} {field}")
    attempts = day["attempts"]
    if attempts is not None:
        for field in ("detected", "tunnel_failed", "validated"):
            value = day[field]
            if value is not None and value > attempts:
                raise ValueError(f"{field} {value} exceeds attempts {attempts} on {day_utc}")
        tunnel = day["tunnel_failed"]
        validated = day["validated"]
        if tunnel is not None and validated is not None and tunnel + validated > attempts:
            raise ValueError(f"tunnel_failed + validated exceeds attempts on {day_utc}")
    day["solve_numerator"] = solved
    day["solve_denominator"] = detected
    day.update(_solve_metrics(solved, detected))
    day["validated_over_attempts"] = _ratio(day["validated"], day["attempts"])
    day["tunnel_failed_over_attempts"] = _ratio(day["tunnel_failed"], day["attempts"])
    day["status"] = "complete" if all(day[f] is not None for f in FUNNEL_FIELDS) else "partial"
    return day


def summarize_failure_census(counts):
    """Summarize bootstrap attempts of failed runs, not failed-run counts."""
    if not isinstance(counts, dict):
        raise ValueError("failure census must be a JSON object of counts")
    unknown = sorted(set(counts) - set(FAILURE_CATEGORIES) - {EXCLUDED_KEY})
    if unknown:
        raise ValueError(f"unknown census categories: {unknown}")
    missing = [key for key in FAILURE_CATEGORIES if key not in counts]
    if missing:
        raise ValueError(f"census requires categories {missing}")
    values = {key: _nonneg_int(counts[key], key) for key in FAILURE_CATEGORIES}
    excluded = _nonneg_int(counts.get(EXCLUDED_KEY, 0), EXCLUDED_KEY)
    total = sum(values.values())
    categories = {}
    for key, count in values.items():
        categories[key] = {
            "count": count,
            "fraction": _ratio(count, total),
            "percent": round(100.0 * count / total) if total else None,
        }
    largest = None
    largest_tie = False
    if total:
        top = max(values.values())
        leaders = sorted(key for key, count in values.items() if count == top)
        largest_tie = len(leaders) > 1
        if not largest_tie:
            largest = leaders[0]
    return {
        "total_failed_run_attempts": total,
        "counts": values,
        "categories": categories,
        "largest_category": largest,
        "largest_tie": largest_tie,
        "is_majority": largest is not None and values[largest] * 2 > total,
        EXCLUDED_KEY: excluded,
    }


def build_report(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {payload.get('schema_version')!r}")
    if payload.get("mode") != MODE:
        raise ValueError(f"unsupported mode: {payload.get('mode')!r}")
    population = payload.get("population")
    if not isinstance(population, dict):
        raise ValueError("population metadata mapping is required")
    for field in POPULATION_FIELDS:
        value = population.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"population.{field} must be a nonempty string")
    raw_days = payload.get("days")
    if not isinstance(raw_days, list) or not raw_days:
        raise ValueError("days must be a nonempty list")
    days = [_build_day(entry) for entry in raw_days]
    seen = set()
    for day in days:
        if day["day_utc"] in seen:
            raise ValueError(f"duplicate day_utc: {day['day_utc']}")
        seen.add(day["day_utc"])
    days.sort(key=lambda day: day["day_utc"])
    census = payload.get(CENSUS_KEY)
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": MODE,
        "population": dict(population),
        "day_count": len(days),
        "first_day": days[0]["day_utc"],
        "last_day": days[-1]["day_utc"],
        "days": days,
        "failure_census": None if census is None else summarize_failure_census(census),
    }


def pool_report(report, start_day, end_day):
    days = report.get("days") if isinstance(report, dict) else None
    if not isinstance(days, list) or not days:
        raise ValueError("pool_report requires a report built by build_report")
    start = _iso_date(start_day, "start_day")
    end = _iso_date(end_day, "end_day")
    if start > end:
        raise ValueError("start_day must not be after end_day")
    index = {day["day_utc"]: day for day in days}
    selected = []
    current = start
    while current <= end:
        key = current.isoformat()
        if key not in index:
            raise ValueError(f"day {key} is missing; gaps are never treated as zero")
        selected.append(index[key])
        current += timedelta(days=1)
    detected = sum(day["detected"] for day in selected)
    solved = sum(day["solved"] for day in selected)
    pooled = {
        "population": report["population"],
        "start_day": start_day,
        "end_day": end_day,
        "day_count": len(selected),
        "detected": detected,
        "solved": solved,
        "solve_numerator": solved,
        "solve_denominator": detected,
    }
    pooled.update(_solve_metrics(solved, detected))
    for field in FUNNEL_FIELDS:
        values = [day[field] for day in selected]
        pooled[field] = None if None in values else sum(values)
    pooled["validated_over_attempts"] = _ratio(pooled["validated"], pooled["attempts"])
    pooled["tunnel_failed_over_attempts"] = _ratio(pooled["tunnel_failed"], pooled["attempts"])
    pooled["status"] = "complete" if all(pooled[f] is not None for f in FUNNEL_FIELDS) else "partial"
    return pooled


def percentage_point_change(later_pool, earlier_pool):
    if later_pool["solve_fraction"] is None or earlier_pool["solve_fraction"] is None:
        return None
    return round(100.0 * (later_pool["solve_fraction"] - earlier_pool["solve_fraction"]))


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if len(argv) != 1:
        print("usage: python -m scripts.research.clubelo_solver_metrics INPUT.json", file=sys.stderr)
        return 2
    try:
        with open(argv[0], "r", encoding="utf-8") as handle:
            report = build_report(json.load(handle))
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
