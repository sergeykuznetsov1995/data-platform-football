"""Daily data-quality checks of the ESPN bronze tables (#1505).

Pure ``build_*_sql`` functions for the UTC day D (rows committed that day,
``_ingested_at``) and ``render_quality_lines`` that turns their answers into
morning-report lines — only for a violation; clean data prints nothing.  The
morning report copies ``build_quality_sql``, ``build_summary_loads_sql`` and
``build_recheck_sql`` verbatim (package ``/root/espn-deliveries/1506/summary``):
change them only together with that copy.

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
* ``summary_loads_week`` / ``summary_loads_3d`` — Summary downloads from the
  network (``disposition = 'success'``) per match (``url_fingerprint``) by the
  request journal (#1506): on average more than ``LOADS_WEEK_LIMIT_TENTHS``/10
  over the 7 days ending D, or more than ``LOADS_LIMIT`` for one match over
  the 3 days ending D;
* recheck journal (#1506, ``recheck.py``): ``downgrade_rejected`` on D (always
  printed, with leagues) and the recheck fill rate on D per league
  (``recheck_filled``: rechecks that ended ``filled`` of all rechecks).
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
from .recheck import DOWNGRADE_REJECTED, FILLED, RECHECK, RECHECK_TABLE

SHARE_LIMIT_PCT = 20
SHARE_MIN_MATCHES = 5
# Summary downloads per match: at most 2 over 3 days (target ≤ 2.1), on
# average at most 1.1 over 7 days (in tenths, integers only in the SQL).
LOADS_LIMIT = 2
LOADS_WEEK_LIMIT_TENTHS = 11
LOADS_WEEK_DAYS = 7
LOADS_PEAK_DAYS = 3
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


def _loads(day: str, days: int) -> str:
    return f"""SELECT url_fingerprint, count(*) AS loads
    FROM {JOURNAL_TABLE}
    WHERE endpoint = 'summary' AND disposition = 'success'
      AND request_date > DATE '{day}' - INTERVAL '{days}' DAY
      AND request_date <= DATE '{day}'
    GROUP BY url_fingerprint"""


def build_summary_loads_sql(day: str) -> str:
    """Summary downloads per match from the request journal (#1506).

    ``summary_loads_week``: ``bad`` = downloads, ``total`` = matches over the
    7 days ending D; ``summary_loads_3d``: ``bad`` = matches downloaded more
    than ``LOADS_LIMIT`` times, ``total`` = matches over the 3 days ending D.
    """

    day = _iso_day(day)
    return f"""SELECT 'summary_loads_week' AS check_name, '*' AS slug,
       coalesce(sum(loads), 0) AS bad, count(*) AS total
FROM (
    {_loads(day, LOADS_WEEK_DAYS)}
) w
UNION ALL
SELECT 'summary_loads_3d' AS check_name, '*' AS slug,
       count_if(loads > {LOADS_LIMIT}) AS bad, count(*) AS total
FROM (
    {_loads(day, LOADS_PEAK_DAYS)}
) p"""


def build_recheck_sql(day: str) -> str:
    """Recheck journal of day D (#1506): ``downgrade_rejected`` per league
    (``bad``) and ``recheck_filled`` per league (``bad`` filled of ``total``
    rechecks).  The journal exists once the DAG ran (#1507)."""

    day = _iso_day(day)
    return f"""SELECT 'downgrade_rejected' AS check_name, slug,
       count(*) AS bad, CAST(NULL AS bigint) AS total
FROM {RECHECK_TABLE}
WHERE outcome = '{DOWNGRADE_REJECTED}' AND {_on(day, "checked_at")}
GROUP BY slug
UNION ALL
SELECT 'recheck_filled' AS check_name, slug,
       count_if(outcome = '{FILLED}') AS bad, count(*) AS total
FROM {RECHECK_TABLE}
WHERE kind = '{RECHECK}' AND {_on(day, "checked_at")}
GROUP BY slug"""


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
        elif check == "summary_loads_week" and total and bad * 10 > LOADS_WEEK_LIMIT_TENTHS * total:
            lines.append(
                f"summary в среднем {bad / total:.2f} загрузки на матч за {LOADS_WEEK_DAYS} суток "
                f"(> {LOADS_WEEK_LIMIT_TENTHS // 10}.{LOADS_WEEK_LIMIT_TENTHS % 10}): "
                f"{bad} на {total} матч(ей)"
            )
        elif check == "summary_loads_3d" and bad:
            lines.append(
                f"summary качали > {LOADS_LIMIT} раз за {LOADS_PEAK_DAYS} суток: {bad} матч(ей)"
            )
    for check in SHARE_CHECKS:
        if check in shares:
            lines.insert(
                0,
                f"доля {check} > {SHARE_LIMIT_PCT} % (от {SHARE_MIN_MATCHES} матчей): "
                + ", ".join(sorted(shares[check])),
            )
    return lines


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.1f}"


def render_recheck_lines(day: str, rows: Sequence[Sequence] | None) -> list[str]:
    """Two lines from ``build_recheck_sql``: ``downgrade_rejected`` of D (with
    leagues) and the recheck fill rate of D (leagues that got filled)."""

    dd = f"{day[8:10]}.{day[5:7]}"
    if rows is None:
        return [f"• ESPN перепроверка {dd}: не посчитано ⚠️"]
    downgrades: dict[str, int] = {}
    filled: dict[str, tuple[int, int]] = {}
    for check, slug, bad, total in rows:
        if check == "downgrade_rejected" and int(bad or 0):
            downgrades[slug] = int(bad)
        elif check == "recheck_filled":
            filled[slug] = (int(bad or 0), int(total or 0))
    count = sum(downgrades.values())
    line = f"• ESPN перепроверка {dd}: downgrade_rejected за сутки: {count}"
    if count:
        line += " (" + ", ".join(f"{slug} {n}" for slug, n in sorted(downgrades.items())) + ") ‼️"
    lines = [line]
    done = sum(total for _, total in filled.values())
    if not done:
        lines.append(f"• ESPN перепроверка {dd}: перепроверок не было")
        return lines
    got = sum(bad for bad, _ in filled.values())
    line = f"• ESPN перепроверка {dd}: дозаполнено при перепроверке: {got} из {done} ({_pct(got, done)} %)"
    leagues = [f"{slug} {bad}/{total}" for slug, (bad, total) in sorted(filled.items()) if bad]
    if leagues:
        line += "; по лигам: " + ", ".join(leagues)
    return lines + [line]


def render_quality_lines(
    day: str, rows: Sequence[Sequence] | None, recheck_rows: Sequence[Sequence] | None
) -> list[str]:
    """Morning-report lines: one per violation (none when the data is clean),
    then the recheck lines.

    ``rows`` — answers of ``build_quality_sql`` and ``build_summary_loads_sql``,
    ``recheck_rows`` — of ``build_recheck_sql``; ``None`` when a query failed.
    """

    dd = f"{day[8:10]}.{day[5:7]}"
    if rows is None:
        lines = [f"• ESPN DQ {dd}: не посчитано ⚠️"]
    else:
        lines = [f"• ESPN DQ {dd}: {line} ‼️" for line in _violations(rows)]
    return lines + render_recheck_lines(day, recheck_rows)


__all__ = [
    "LOADS_LIMIT",
    "LOADS_PEAK_DAYS",
    "LOADS_WEEK_DAYS",
    "LOADS_WEEK_LIMIT_TENTHS",
    "SHARE_CHECKS",
    "SHARE_LIMIT_PCT",
    "SHARE_MIN_MATCHES",
    "build_duplicates_sql",
    "build_ingested_before_fetched_sql",
    "build_null_score_sql",
    "build_quality_sql",
    "build_recheck_sql",
    "build_share_sql",
    "build_summary_loads_sql",
    "render_quality_lines",
    "render_recheck_lines",
]
