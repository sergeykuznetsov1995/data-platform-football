"""Daily data-quality checks of the ESPN bronze tables (#1505).

Pure ``build_*_sql`` functions for the UTC day D (rows committed that day,
``_ingested_at``) and ``render_quality_lines`` that turns their answers into
morning-report lines — only for a violation; clean data prints nothing.  The
morning report copies ``build_quality_sql`` and ``build_summary_loads_sql``
verbatim (package ``/root/espn-deliveries/1505/summary``): change them only
together with that copy.

Checks (one answer row each: ``check, slug, bad, total``):

* ``valid_empty`` / ``source_malformed`` / ``lineup_anomaly`` — share of the
  played finals of a tournament written on D with that Summary disposition;
  a violation above ``SHARE_LIMIT`` of at least ``SHARE_MIN_MATCHES`` matches;
* ``duplicate_event_id`` — an ``event_id`` on more than one ``espn_match``
  row (the natural key is per partition; a match in two seasons is a bug);
* ``ingested_before_fetched`` — ``_ingested_at < _source_fetched_at`` on any
  row committed on D (R-18: must be 0);
* ``null_score`` — a played final written on D without its score (NULL
  instead of 0);
* ``summary_loads`` — Summary downloads per match on D by the request
  journal (``url_fingerprint`` with more than ``LOADS_LIMIT`` successes);
* ``downgrade_rejected`` — from the recheck (#1506); until then a stub line.
"""

from __future__ import annotations

from typing import Sequence

from .bronze_schema import (
    BRONZE_DATABASE,
    EVENTS_TABLE,
    LINEUP_TABLE,
    MATCH_TABLE,
    TEAM_STATS_TABLE,
)
from .journal import JOURNAL_TABLE

SHARE_LIMIT_PCT = 20
SHARE_MIN_MATCHES = 5
LOADS_LIMIT = 2
SHARE_CHECKS = ("valid_empty", "source_malformed", "lineup_anomaly")
_TABLES = (MATCH_TABLE, LINEUP_TABLE, TEAM_STATS_TABLE, EVENTS_TABLE)


def _iso_day(day: str) -> str:
    parts = day.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"day must be YYYY-MM-DD, got {day!r}")
    return day


def _on(day: str, column: str = "_ingested_at") -> str:
    return (
        f"{column} >= TIMESTAMP '{day} 00:00:00' "
        f"AND {column} < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY"
    )


def _relation(table: str) -> str:
    return f"iceberg.{BRONZE_DATABASE}.{table}"


def build_share_sql(day: str) -> str:
    day = _iso_day(day)
    parts = [
        f"""SELECT '{check}' AS check_name, competition_slug AS slug,
       count_if(disposition = '{check}') AS bad, count(*) AS total
FROM {_relation(MATCH_TABLE)}
WHERE played_final AND disposition IN ('captured', 'valid_empty', 'source_malformed', 'lineup_anomaly')
  AND {_on(day)}
GROUP BY competition_slug"""
        for check in SHARE_CHECKS
    ]
    return "\nUNION ALL\n".join(parts)


def build_duplicates_sql() -> str:
    return f"""SELECT 'duplicate_event_id' AS check_name, '*' AS slug,
       count(*) AS bad, CAST(NULL AS bigint) AS total
FROM (
    SELECT event_id FROM {_relation(MATCH_TABLE)}
    GROUP BY event_id HAVING count(*) > 1
) d"""


def build_ingested_before_fetched_sql(day: str) -> str:
    day = _iso_day(day)
    return "\nUNION ALL\n".join(
        f"""SELECT 'ingested_before_fetched' AS check_name, '{table}' AS slug,
       count_if(_ingested_at < _source_fetched_at) AS bad, count(*) AS total
FROM {_relation(table)}
WHERE {_on(day)}"""
        for table in _TABLES
    )


def build_null_score_sql(day: str) -> str:
    day = _iso_day(day)
    return f"""SELECT 'null_score' AS check_name, competition_slug AS slug,
       count_if(home_score IS NULL OR away_score IS NULL) AS bad, count(*) AS total
FROM {_relation(MATCH_TABLE)}
WHERE played_final AND {_on(day)}
GROUP BY competition_slug"""


def build_quality_sql(day: str) -> str:
    """Every bronze check of day D in one query (``check, slug, bad, total``)."""

    return "\nUNION ALL\n".join(
        (
            build_share_sql(day),
            build_duplicates_sql(),
            build_ingested_before_fetched_sql(day),
            build_null_score_sql(day),
        )
    )


def build_summary_loads_sql(day: str) -> str:
    """Summary downloads per match on D from the request journal (#1500)."""

    day = _iso_day(day)
    return f"""SELECT 'summary_loads' AS check_name, '*' AS slug,
       count_if(loads > {LOADS_LIMIT}) AS bad, count(*) AS total
FROM (
    SELECT url_fingerprint, count(*) AS loads
    FROM {JOURNAL_TABLE}
    WHERE endpoint = 'summary' AND disposition = 'success'
      AND request_date = DATE '{day}'
    GROUP BY url_fingerprint
) l"""


def _violations(rows: Sequence[Sequence]) -> list[str]:
    shares: dict[str, list[str]] = {}
    lines: list[str] = []
    for check, slug, bad, total in rows:
        bad = int(bad or 0)
        total = int(total) if total is not None else None
        if check in SHARE_CHECKS:
            if total and total >= SHARE_MIN_MATCHES and bad * 100 > SHARE_LIMIT_PCT * total:
                shares.setdefault(check, []).append(f"{slug} {bad}/{total}")
        elif check == "duplicate_event_id" and bad:
            lines.append(f"дубли event_id в espn_match: {bad}")
        elif check == "ingested_before_fetched" and bad:
            lines.append(f"_ingested_at < _source_fetched_at в {slug}: {bad} из {total}")
        elif check == "null_score" and bad:
            lines.append(f"сыгранный без счёта (NULL вместо 0) {slug}: {bad} из {total}")
        elif check == "summary_loads" and bad:
            lines.append(f"summary качали > {LOADS_LIMIT} раз за сутки: {bad} матч(ей)")
    for check in SHARE_CHECKS:
        if check in shares:
            lines.insert(
                0,
                f"доля {check} > {SHARE_LIMIT_PCT} % (от {SHARE_MIN_MATCHES} матчей): "
                + ", ".join(sorted(shares[check])),
            )
    return lines


# The recheck (#1506) writes the rejected downgrades; until then a stub line.
DOWNGRADE_REJECTED_LINE = "• ESPN DQ: downgrade_rejected — нет данных до перепроверки (#1506)"


def render_quality_lines(day: str, rows: Sequence[Sequence] | None) -> list[str]:
    """Morning-report lines: one per violation (none when the data is clean),
    then the ``downgrade_rejected`` stub.

    ``rows`` — answers of ``build_quality_sql`` and ``build_summary_loads_sql``;
    ``None`` when the query failed.
    """

    dd = f"{day[8:10]}.{day[5:7]}"
    if rows is None:
        lines = [f"• ESPN DQ {dd}: не посчитано ⚠️"]
    else:
        lines = [f"• ESPN DQ {dd}: {line} ‼️" for line in _violations(rows)]
    return lines + [DOWNGRADE_REJECTED_LINE]


__all__ = [
    "DOWNGRADE_REJECTED_LINE",
    "LOADS_LIMIT",
    "SHARE_CHECKS",
    "SHARE_LIMIT_PCT",
    "SHARE_MIN_MATCHES",
    "build_duplicates_sql",
    "build_ingested_before_fetched_sql",
    "build_null_score_sql",
    "build_quality_sql",
    "build_share_sql",
    "build_summary_loads_sql",
    "render_quality_lines",
]
