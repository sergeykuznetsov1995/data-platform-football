"""
Unit tests for ``dags/sql/silver/whoscored_team_match.sql`` (#573).

Strategy
--------
This Silver rollup reads the SPADL-canonical ``silver.whoscored_events_spadl``
and counts ``shots_total`` / ``shots_on_target_proxy`` per team-match. The bug
(#573): ``shots_on_target_proxy`` used ``outcome_success`` (= bronze
``outcome_type='Successful'``) as the on-target signal, but WhoScored marks
``MissedShots`` and ``ShotOnPost`` ``Successful`` too — so the proxy counted
nearly every attempt. The fix redefines on-target as saved shots + scored goals
via the preserved original type in ``_action_source_note`` (``'SavedShot'`` /
``'Goal'``), mirroring ``whoscored_player_match_aggregate.shots_on_target``.

We reuse the ``test_whoscored_player_match_aggregate_sql.py`` DuckDB-bridge: seed
a fixture ``silver_whoscored_events_spadl`` table (already-canonicalised SPADL
rows — the consumer is tested in isolation), rewrite the two Trino-specific
constructs (table ref + ``regexp_like``) into DuckDB spelling, and execute the
SELECT. ``COUNT_IF`` is DuckDB-native (>=1.5).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "whoscored_team_match.sql"


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# DuckDB bridge — translate Trino-specific SQL to DuckDB-compatible form
# ---------------------------------------------------------------------------


def _translate_trino_to_duckdb(sql: str) -> str:
    """Adapt the Silver SQL for execution on DuckDB.

    Two adjustments (same family as the sibling Silver SQL tests):
      * source table ref -> single-namespace fixture table.
      * ``regexp_like`` -> ``regexp_matches`` (identical POSIX-regex semantics).
    ``COUNT_IF`` / ``COALESCE`` / ``ROUND`` are DuckDB-native.
    """
    sql = sql.replace(
        "iceberg.silver.whoscored_events_spadl", "silver_whoscored_events_spadl"
    )
    sql = re.sub(r"\bregexp_like\s*\(", "regexp_matches(", sql, flags=re.IGNORECASE)
    return sql


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


# SPADL columns the team_match SELECT consumes.
_SPADL_COLUMNS: List[str] = [
    "match_id", "team_id_raw", "action_canonical", "_action_source_note",
    "outcome_success", "qualifiers_raw", "x", "y", "league", "season",
]


def _spadl_row(
    *,
    match_id: str = "100",
    team_id_raw: str = "13",
    action_canonical: str = "pass",
    action_source_note: str = "Pass",
    outcome_success: bool = True,
    qualifiers_raw: Optional[str] = None,
    x: float = 90.0,
    y: float = 50.0,
    league: str = "ENG-Premier League",
    season: str = "2526",
) -> Dict[str, Any]:
    """Build a single ``whoscored_events_spadl`` fixture row.

    ``action_source_note`` maps to the physical ``_action_source_note`` column
    (the preserved original WhoScored ``type`` for shot events).
    """
    return {
        "match_id": match_id,
        "team_id_raw": team_id_raw,
        "action_canonical": action_canonical,
        "_action_source_note": action_source_note,
        "outcome_success": outcome_success,
        "qualifiers_raw": qualifiers_raw,
        "x": x,
        "y": y,
        "league": league,
        "season": season,
    }


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


def _seed_and_run(con, fixture_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    con.execute("DROP TABLE IF EXISTS silver_whoscored_events_spadl")
    con.execute(
        """
        CREATE TABLE silver_whoscored_events_spadl (
            match_id            VARCHAR,
            team_id_raw         VARCHAR,
            action_canonical    VARCHAR,
            _action_source_note VARCHAR,
            outcome_success     BOOLEAN,
            qualifiers_raw      VARCHAR,
            x                   DOUBLE,
            y                   DOUBLE,
            league              VARCHAR,
            season              VARCHAR
        )
        """
    )
    placeholders = ", ".join(["?"] * len(_SPADL_COLUMNS))
    insert_sql = (
        f"INSERT INTO silver_whoscored_events_spadl "
        f"({', '.join(_SPADL_COLUMNS)}) VALUES ({placeholders})"
    )
    for row in fixture_rows:
        con.execute(insert_sql, [row[c] for c in _SPADL_COLUMNS])

    sql = _translate_trino_to_duckdb(_read_sql())
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# shots_on_target_proxy = saves + scored goals (#573) — the core regression
# ---------------------------------------------------------------------------


class TestShotsOnTargetSavesPlusGoals:
    """on-target = SavedShot + Goal, NOT every Successful shot. WhoScored marks
    MissedShots / ShotOnPost outcome_type='Successful', so the old
    ``outcome_success`` predicate over-counted off-target attempts."""

    def _shot_corpus(self) -> List[Dict[str, Any]]:
        # One team (13) in one match (100). outcome_success mirrors the live
        # bronze quirk: saves, goals, misses and posts are ALL 'Successful'.
        return [
            _spadl_row(action_canonical="shot", action_source_note="SavedShot",
                       outcome_success=True),                                   # on target
            _spadl_row(action_canonical="shot", action_source_note="Goal",
                       outcome_success=True),                                   # on target (scored)
            _spadl_row(action_canonical="shot", action_source_note="MissedShots",
                       outcome_success=True),                                   # off target, but Successful
            _spadl_row(action_canonical="shot", action_source_note="ShotOnPost",
                       outcome_success=True),                                   # off target, but Successful
            _spadl_row(action_canonical="shot", action_source_note="ChanceMissed",
                       outcome_success=False),                                  # off target
            _spadl_row(action_canonical="shot_penalty", action_source_note="Goal",
                       outcome_success=True),                                   # on target (scored pen)
            _spadl_row(action_canonical="own_goal", action_source_note="Goal",
                       outcome_success=True),                                   # NOT a shot to scorer (#572)
            _spadl_row(action_canonical="pass", action_source_note="Pass",
                       outcome_success=True),                                   # non-shot
        ]

    def test_shots_total_counts_all_shot_family_excl_own_goal(self, duck_conn):
        out = _seed_and_run(duck_conn, self._shot_corpus())
        assert len(out) == 1
        # 5 'shot' + 1 'shot_penalty'; 'own_goal' and 'pass' excluded.
        assert out[0]["shots_total"] == 6

    def test_shots_on_target_is_saves_plus_goals(self, duck_conn):
        out = _seed_and_run(duck_conn, self._shot_corpus())
        # SavedShot + open-play Goal + penalty Goal = 3. MissedShots / ShotOnPost
        # are excluded DESPITE outcome_success=True (the #573 bug).
        assert out[0]["shots_on_target_proxy"] == 3

    def test_missed_shot_successful_is_not_on_target(self, duck_conn):
        """Crisp bug repro: a saved shot + a MissedShots both marked Successful
        → 2 shots, but only 1 on target."""
        out = _seed_and_run(duck_conn, [
            _spadl_row(action_canonical="shot", action_source_note="SavedShot",
                       outcome_success=True),
            _spadl_row(action_canonical="shot", action_source_note="MissedShots",
                       outcome_success=True),
        ])
        assert out[0]["shots_total"] == 2
        assert out[0]["shots_on_target_proxy"] == 1

    def test_own_goal_excluded_from_shots_and_on_target(self, duck_conn):
        """An own-goal carries _action_source_note='Goal' but action_canonical=
        'own_goal' — the shot-family predicate must keep it out of both counts."""
        out = _seed_and_run(duck_conn, [
            _spadl_row(action_canonical="own_goal", action_source_note="Goal",
                       outcome_success=True),
        ])
        assert out[0]["shots_total"] == 0
        assert out[0]["shots_on_target_proxy"] == 0

    def test_on_target_never_exceeds_total(self, duck_conn):
        out = _seed_and_run(duck_conn, self._shot_corpus())
        assert out[0]["shots_on_target_proxy"] <= out[0]["shots_total"]
