"""
Unit tests for ``dags/sql/silver/whoscored_player_match_aggregate.sql`` (#572).

Strategy
--------
This Silver aggregate counts WhoScored ``goals`` / ``shots`` / ``shots_on_target``
directly from ``bronze.whoscored_events`` per Opta ``type``. Own-goals are NOT a
distinct ``type='OwnGoal'`` row — they arrive as ``type='Goal'`` carrying the
qualifier ``"displayName":"OwnGoal"`` (#572 finding). They must NOT be credited
as a goal/shot to the scorer (the defender who deflected into his own net).

We reuse the ``test_spadl_mapping.py`` DuckDB-bridge approach: seed a fixture
``bronze_whoscored_events`` table, rewrite the two Trino-specific constructs the
file uses (``iceberg.bronze.whoscored_events`` ref + ``regexp_like``) into DuckDB
spelling, and execute the SELECT. DuckDB (>=1.5) supports ``COUNT_IF`` natively.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "whoscored_player_match_aggregate.sql"
)


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# DuckDB bridge — translate Trino-specific SQL to DuckDB-compatible form
# ---------------------------------------------------------------------------


def _translate_trino_to_duckdb(sql: str) -> str:
    """Adapt the Silver SQL for execution on DuckDB.

    Same two adjustments as test_spadl_mapping.py:
      * source table ref -> single-namespace fixture table.
      * ``regexp_like`` -> ``regexp_matches`` (identical POSIX-regex semantics).
    ``COUNT_IF`` / ``COALESCE`` / ``ROW_NUMBER OVER`` are DuckDB-native.
    """
    sql = sql.replace("iceberg.bronze.whoscored_events", "bronze_whoscored_events")
    sql = re.sub(r"\bregexp_like\s*\(", "regexp_matches(", sql, flags=re.IGNORECASE)
    return sql


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


# Bronze columns the player_match_aggregate SELECT consumes (dedup natural key
# adds period/minute/second; the rest are projected/counted).
_BRONZE_COLUMNS: List[str] = [
    "game_id", "period", "minute", "second",
    "type", "outcome_type", "team_id", "player_id",
    "qualifiers", "league", "season", "_ingested_at",
]


def _row(
    *,
    game_id: int = 100,
    period: str = "FirstHalf",
    minute: int = 5,
    second: int = 0,
    type_: str = "Pass",
    outcome_type: str = "Successful",
    team_id: int = 13,
    player_id: float = 555.0,
    qualifiers: Optional[str] = None,
    league: str = "ENG-Premier League",
    season: str = "2526",
    ingested_at: str = "2026-05-08 12:00:00",
) -> Dict[str, Any]:
    """Build a single bronze fixture row with sensible defaults."""
    return {
        "game_id": game_id,
        "period": period,
        "minute": minute,
        "second": second,
        "type": type_,
        "outcome_type": outcome_type,
        "team_id": team_id,
        "player_id": player_id,
        "qualifiers": qualifiers,
        "league": league,
        "season": season,
        "_ingested_at": ingested_at,
    }


def _qualifiers_json(types: List[str]) -> str:
    """Render WhoScored qualifiers as the NESTED JSON-string array bronze stores.

    The sub-type label lives in ``type.displayName`` — the mapping SQL matches on
    ``"displayName": "X"`` (NOT a flat ``type`` string).
    """
    return json.dumps([{"type": {"value": 0, "displayName": t}} for t in types])


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


def _seed_and_run(con, fixture_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    con.execute("DROP TABLE IF EXISTS bronze_whoscored_events")
    con.execute(
        """
        CREATE TABLE bronze_whoscored_events (
            game_id       BIGINT,
            period        VARCHAR,
            minute        BIGINT,
            second        BIGINT,
            type          VARCHAR,
            outcome_type  VARCHAR,
            team_id       BIGINT,
            player_id     DOUBLE,
            qualifiers    VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            _ingested_at  TIMESTAMP
        )
        """
    )
    placeholders = ", ".join(["?"] * len(_BRONZE_COLUMNS))
    insert_sql = (
        f"INSERT INTO bronze_whoscored_events "
        f"({', '.join(_BRONZE_COLUMNS)}) VALUES ({placeholders})"
    )
    for row in fixture_rows:
        con.execute(insert_sql, [row[c] for c in _BRONZE_COLUMNS])

    sql = _translate_trino_to_duckdb(_read_sql())
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Own-goal exclusion (#572) — the core regression
# ---------------------------------------------------------------------------


class TestOwnGoalExclusion:
    """Own-goals (type='Goal' + OwnGoal qualifier) must NOT count toward the
    scorer's goals / shots / shots_on_target."""

    def _mixed_player_rows(self) -> List[Dict[str, Any]]:
        # One player (555) with: a real goal, an own-goal, a saved shot,
        # a missed shot. Distinct minutes so dedup keeps all four.
        return [
            _row(type_="Goal", minute=10, qualifiers=None),                       # real goal
            _row(type_="Goal", minute=20, qualifiers=_qualifiers_json(["OwnGoal"])),  # own-goal
            _row(type_="SavedShot", minute=30, qualifiers=None),                  # shot + on target
            _row(type_="MissedShots", minute=40, qualifiers=None),               # shot, off target
        ]

    def test_goals_exclude_own_goal(self, duck_conn):
        out = _seed_and_run(duck_conn, self._mixed_player_rows())
        assert len(out) == 1
        assert out[0]["goals"] == 1, "own-goal must not be credited as a goal"

    def test_shots_exclude_own_goal(self, duck_conn):
        out = _seed_and_run(duck_conn, self._mixed_player_rows())
        # SavedShot + MissedShots + real Goal = 3 (own-goal excluded).
        assert out[0]["shots"] == 3

    def test_shots_on_target_exclude_own_goal(self, duck_conn):
        out = _seed_and_run(duck_conn, self._mixed_player_rows())
        # SavedShot + real Goal = 2 (own-goal excluded).
        assert out[0]["shots_on_target"] == 2

    def test_pure_own_goal_player_has_zero_goals_and_shots(self, duck_conn):
        """A defender whose only shot-family event is an own-goal scores 0/0/0,
        but the bronze row is still seen (no row drop)."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Goal", minute=15, qualifiers=_qualifiers_json(["OwnGoal"])),
        ])
        assert len(out) == 1
        assert out[0]["goals"] == 0
        assert out[0]["shots"] == 0
        assert out[0]["shots_on_target"] == 0
        # Own-goal is still a physical touch — counted in total_events / touches.
        assert out[0]["total_events"] == 1
        assert out[0]["touches"] == 1

    def test_real_goal_still_counted(self, duck_conn):
        """Guard against over-filtering: a plain Goal (no OwnGoal qualifier)
        is still a goal / shot / shot_on_target."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Goal", minute=12, qualifiers=None),
        ])
        assert out[0]["goals"] == 1
        assert out[0]["shots"] == 1
        assert out[0]["shots_on_target"] == 1
