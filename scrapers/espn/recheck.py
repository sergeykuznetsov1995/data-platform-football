"""Recheck of captured matches and the "not worse" rule (#1506).

ESPN sometimes completes a match later (lineups of lower leagues about a week
after it) and sometimes answers with a cut Summary (players without
statistics, no team statistics).  The contour downloads each Summary once;

* **recheck** — one more download at kickoff + 7…10 days (the 00 wave) of a
  played, captured match that is incomplete: ``valid_empty`` /
  ``lineup_anomaly``, or a part (lineup, team statistics, events) not
  captured while at least ``LEAGUE_SHARE`` of the league's matches of the last
  ``LEAGUE_DAYS`` days have it.  ``rechecked_at`` is set whatever the outcome:
  there is no second recheck.  Complete matches are never rechecked.
* **sample** — ``event_id % SAMPLE_MOD == 0`` (5 %) of the captured matches
  once in the first 00 wave after ``first_published_at`` + 24 h and + 72 h:
  when does ESPN complete a match (journal only, ``rechecked_at`` untouched).
* **not worse** (``compare_parts``) — any repeated Summary (recheck, sample,
  a status change after capture) is compared, parsed, with the stored one:
  a part poorer than before where the old one had data keeps the stored
  parse (read back by ``raw_uri``/``raw_sha256``) and journals
  ``downgrade_rejected``.

Journal ``iceberg.ops.espn_recheck_v1``: one row per attempt with the parts
before and after and the outcome; the morning report counts
``downgrade_rejected`` and the recheck fill rate per league from it, the stall
watch alerts on ``downgrade_rejected``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping, Sequence

from .bronze_schema import BRONZE_DATABASE, LINEUP_STAT_COLUMNS, MATCH_TABLE, TEAM_STAT_COLUMNS
from .journal import JOURNAL_SCHEMA, _execute, _literal
from .parser_contracts import SummaryParseResult

RECHECK_TABLE = f"{JOURNAL_SCHEMA}.espn_recheck_v1"
RECHECK_COLUMNS = (
    ("checked_at", "timestamp(6)"),
    ("run_id", "varchar"),
    ("slug", "varchar"),
    ("season_year", "integer"),
    ("event_id", "bigint"),
    ("kind", "varchar"),
    ("before_parts", "varchar"),
    ("after_parts", "varchar"),
    ("outcome", "varchar"),
)

# Kinds of a repeated Summary.
RECHECK = "recheck"
SAMPLE_24H = "sample_24h"
SAMPLE_72H = "sample_72h"
# A status change of a captured match whose stored Summary body changed.
REFRESH = "refresh"
KINDS = (RECHECK, SAMPLE_24H, SAMPLE_72H, REFRESH)

# Outcomes.
FILLED = "filled"
SAME = "same"
CHANGED = "changed"
DOWNGRADE_REJECTED = "downgrade_rejected"
FAILED = "failed"

PARTS = ("lineup", "player_stats", "team_stats", "events")

RECHECK_MIN_AGE = timedelta(days=7)
RECHECK_MAX_AGE = timedelta(days=10)
LEAGUE_DAYS = 30
LEAGUE_SHARE = 0.5
SAMPLE_MOD = 20
SAMPLE_AGES = {SAMPLE_24H: timedelta(hours=24), SAMPLE_72H: timedelta(hours=72)}
# The sample runs in the 00 wave: one day of first publications per window.
SAMPLE_WINDOW = timedelta(days=1)

_MATCH = f"iceberg.{BRONZE_DATABASE}.{MATCH_TABLE}"
_BATCH_ROWS = 500


# ------------------------------------------------------------------ rule


def summary_parts(summary: SummaryParseResult) -> dict[str, int]:
    """Filled units per part: lineup rows, players with statistics, team
    statistic values, events."""

    return {
        "lineup": len(summary.lineup),
        "player_stats": sum(
            any(getattr(row, name) is not None for name in LINEUP_STAT_COLUMNS)
            for row in summary.lineup
        ),
        "team_stats": sum(
            getattr(row, name) is not None
            for row in summary.matchsheet
            for name in TEAM_STAT_COLUMNS
        ),
        "events": len(summary.events),
    }


def _content(summary: SummaryParseResult) -> tuple:
    return (
        summary.disposition,
        summary.anomalies,
        summary.lineup,
        summary.matchsheet,
        summary.events,
        summary.advance_team_id,
    )


def compare_parts(old: SummaryParseResult, new: SummaryParseResult) -> str:
    """Outcome of writing ``new`` over ``old``.

    ``downgrade_rejected`` when any part of ``new`` has fewer filled units
    than ``old`` (a part ``old`` had no data in can only stay or grow);
    otherwise ``filled`` when a part grew, ``same`` for an equal parse and
    ``changed`` for edited values at the same completeness.
    """

    before, after = summary_parts(old), summary_parts(new)
    if any(after[part] < before[part] for part in PARTS):
        return DOWNGRADE_REJECTED
    if any(after[part] > before[part] for part in PARTS):
        return FILLED
    return SAME if _content(old) == _content(new) else CHANGED


# ------------------------------------------------------------------ plan


def _ts(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    naive = value.astimezone(timezone.utc).replace(tzinfo=None)
    return f"TIMESTAMP '{naive.isoformat(sep=' ', timespec='microseconds')}'"


def build_candidates_sql(now: datetime) -> str:
    """``competition_slug, season_year, event_id, kind`` due in the 00 wave at ``now``."""

    missing = " OR ".join(
        f"(m.{column} <> 'captured' AND l.{column}_share >= {LEAGUE_SHARE})"
        for column in ("lineup_state", "team_stats_state", "events_state")
    )
    shares = ",\n           ".join(
        f"avg(IF({column} = 'captured', 1.0, 0.0)) AS {column}_share"
        for column in ("lineup_state", "team_stats_state", "events_state")
    )
    samples = "\nUNION ALL\n".join(
        f"""SELECT competition_slug, season_year, event_id, '{kind}' AS kind
FROM {_MATCH}
WHERE played_final AND first_published_at IS NOT NULL AND event_id % {SAMPLE_MOD} = 0
  AND first_published_at > {_ts(now - age - SAMPLE_WINDOW)}
  AND first_published_at <= {_ts(now - age)}"""
        for kind, age in SAMPLE_AGES.items()
    )
    return f"""WITH league AS (
    SELECT competition_slug,
           {shares}
    FROM {_MATCH}
    WHERE played_final AND lineup_state <> 'pending'
      AND kickoff >= {_ts(now - timedelta(days=LEAGUE_DAYS))} AND kickoff < {_ts(now)}
    GROUP BY competition_slug
)
SELECT m.competition_slug, m.season_year, m.event_id, '{RECHECK}' AS kind
FROM {_MATCH} m JOIN league l ON l.competition_slug = m.competition_slug
WHERE m.played_final AND m.lineup_state <> 'pending' AND m.rechecked_at IS NULL
  AND m.kickoff >= {_ts(now - RECHECK_MAX_AGE)} AND m.kickoff <= {_ts(now - RECHECK_MIN_AGE)}
  AND (m.disposition IN ('valid_empty', 'lineup_anomaly') OR {missing})
UNION ALL
{samples}"""


def select_candidates(rows: Sequence[Sequence[Any]]) -> dict[int, tuple[str, int, str]]:
    """``event_id -> (slug, season_year, kind)``; a recheck wins over a sample."""

    order = {kind: index for index, kind in enumerate(KINDS)}
    chosen: dict[int, tuple[str, int, str]] = {}
    for slug, year, event_id, kind in rows:
        event_id = int(event_id)
        current = chosen.get(event_id)
        if current is None or order[kind] < order[current[2]]:
            chosen[event_id] = (str(slug), int(year), str(kind))
    return chosen


def plan_recheck(trino, now: datetime) -> dict[int, tuple[str, int, str]]:
    """Rechecks and samples due in the 00 wave at ``now`` (``TrinoTableManager``)."""

    return select_candidates(trino.execute_query(build_candidates_sql(now)))


# --------------------------------------------------------------- journal


def journalled(trino, slug: str, season_year: int, rechecks: Mapping[int, str]) -> set[int]:
    """Events of ``rechecks`` (``event_id -> kind``) the journal already has
    with that kind, whatever the outcome: each is downloaded at most once."""

    if not rechecks:
        return set()
    ids = ", ".join(str(int(event_id)) for event_id in sorted(rechecks))
    rows = trino.execute_query(
        f"SELECT DISTINCT event_id, kind FROM {RECHECK_TABLE} "
        f"WHERE slug = ? AND season_year = ? AND event_id IN ({ids})",
        (slug, int(season_year)),
    )
    return {int(event_id) for event_id, kind in rows if rechecks.get(int(event_id)) == kind}


def ensure_recheck_table(conn) -> None:
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {JOURNAL_SCHEMA}")
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in RECHECK_COLUMNS)
    _execute(conn, f"CREATE TABLE IF NOT EXISTS {RECHECK_TABLE} ({columns})")


def recheck_row(
    *,
    checked_at: datetime,
    run_id: str,
    slug: str,
    season_year: int,
    event_id: int,
    kind: str,
    before: Mapping[str, int] | None,
    after: Mapping[str, int] | None,
    outcome: str,
) -> dict[str, Any]:
    if kind not in KINDS:
        raise ValueError(f"unknown recheck kind {kind!r}")
    return {
        "checked_at": checked_at,
        "run_id": run_id,
        "slug": slug,
        "season_year": season_year,
        "event_id": event_id,
        "kind": kind,
        "before_parts": json.dumps(dict(before), sort_keys=True) if before is not None else None,
        "after_parts": json.dumps(dict(after), sort_keys=True) if after is not None else None,
        "outcome": outcome,
    }


def flush_rechecks(conn, rows: Sequence[Mapping[str, Any]]) -> int:
    names = ", ".join(name for name, _ in RECHECK_COLUMNS)
    for start in range(0, len(rows), _BATCH_ROWS):
        values = ", ".join(
            "("
            + ", ".join(_literal(row.get(name), sql_type) for name, sql_type in RECHECK_COLUMNS)
            + ")"
            for row in rows[start : start + _BATCH_ROWS]
        )
        _execute(conn, f"INSERT INTO {RECHECK_TABLE} ({names}) VALUES {values}")
    return len(rows)


__all__ = [
    "CHANGED",
    "DOWNGRADE_REJECTED",
    "FAILED",
    "FILLED",
    "KINDS",
    "PARTS",
    "RECHECK",
    "RECHECK_COLUMNS",
    "RECHECK_TABLE",
    "REFRESH",
    "SAME",
    "SAMPLE_24H",
    "SAMPLE_72H",
    "build_candidates_sql",
    "compare_parts",
    "ensure_recheck_table",
    "flush_rechecks",
    "journalled",
    "plan_recheck",
    "recheck_row",
    "select_candidates",
    "summary_parts",
]
