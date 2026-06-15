"""
Unit tests for ``dags/sql/silver/whoscored_player_season_aggregate.sql`` (B1).

Regex / keyword sanity (no Trino engine) verifies the SPADL action enum
coverage, mandatory ``(league, season)`` JOIN predicate against
``silver.xref_player`` (CLAUDE.md fan-out rule), and the GROUP BY contract
that defines the table's PK.

Plus a DuckDB data-test (#573): ``shots_on_target_proxy`` must be saved shots +
scored goals (``_action_source_note IN ('SavedShot','Goal')``), NOT every
``outcome_success`` shot — and that signal must survive the events→joined CTE
plumbing through the xref_player join.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "whoscored_player_season_aggregate.sql"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


def test_sql_file_exists():
    assert SQL_PATH.exists(), f"missing SQL file: {SQL_PATH}"


def test_reads_canonical_silver_sources():
    """Reads silver.whoscored_events_spadl + silver.xref_player only."""
    body = _strip_comments(_read_sql())
    assert "iceberg.silver.whoscored_events_spadl" in body
    assert "iceberg.silver.xref_player" in body
    # No bronze reads — silver-only consumer
    assert "iceberg.bronze." not in body, (
        "Aggregate must consume Silver SPADL, not Bronze events"
    )


def test_xref_join_has_league_season_predicate():
    """CLAUDE.md hard rule: every xref JOIN MUST include (league, season)
    predicate or you get 1.5–4× fan-out from per-(source, season) rows."""
    body = _strip_comments(_read_sql())
    # The xp CTE filters to source='whoscored' and the join clause must
    # carry league + season equality.
    assert re.search(r"xp\.league\s*=\s*e\.league", body), (
        "xref_player JOIN missing league predicate — fan-out risk"
    )
    assert re.search(r"xp\.season\s*=\s*e\.season", body), (
        "xref_player JOIN missing season predicate — fan-out risk"
    )


def test_xref_filtered_to_whoscored_source():
    body = _strip_comments(_read_sql())
    assert re.search(r"WHERE\s+source\s*=\s*'whoscored'", body), (
        "xp CTE must filter xref_player to source='whoscored'"
    )


def test_pk_group_by_canonical_league_season():
    """Table PK = (canonical_id, league, season) — enforced by GROUP BY."""
    body = _strip_comments(_read_sql())
    gb = re.search(r"GROUP\s+BY\s+([^\n;]+)", body, flags=re.IGNORECASE)
    assert gb, "missing GROUP BY clause"
    cols = {c.strip() for c in gb.group(1).split(",")}
    assert "canonical_id" in cols
    assert "league" in cols
    assert "season" in cols


@pytest.mark.parametrize("action", [
    # Core actions B1 reports as columns; SPADL spec freeze.
    "pass", "take_on", "dribble", "bad_touch", "shot",
    "tackle", "interception", "ball_recovery", "clearance", "foul",
    "keeper_save", "keeper_pick_up", "keeper_claim",
])
def test_spadl_action_referenced(action):
    """Every metric column must be backed by a literal action_canonical
    predicate — protects against silent typos that turn a counter into 0."""
    body = _strip_comments(_read_sql())
    assert f"'{action}'" in body, (
        f"SPADL action '{action}' not referenced in COUNT_IF predicates"
    )


def test_spatial_avg_excludes_defensive_actions():
    """avg_x/avg_y must come from on-ball offensive actions only — defensive
    recoveries pull a winger's average back to his own half."""
    body = _strip_comments(_read_sql())
    # The CASE WHEN must include only attacking on-ball actions.
    assert re.search(
        r"action_canonical\s+IN\s*\(\s*'pass'\s*,\s*'take_on'\s*,\s*'shot'\s*,\s*'dribble'\s*\)\s+THEN\s+x",
        body,
    ), "avg_x CASE filter not restricted to (pass, take_on, shot, dribble)"


def test_touches_in_box_uses_spadl_box_polygon():
    """SPADL coords: opposition box ≈ x>=83 AND y∈[21,79]. Catches the
    classic mistake of writing y BETWEEN 30 AND 70."""
    body = _strip_comments(_read_sql())
    assert "x >= 83" in body, "touches_in_box: x>=83 threshold missing"
    assert "y BETWEEN 21 AND 79" in body, (
        "touches_in_box: y BETWEEN 21 AND 79 threshold missing"
    )


def test_partition_columns_are_last():
    """The CTAS layer (run_silver_transform) injects
    ``WITH (partitioning = ARRAY['league','season'])`` — by Iceberg
    convention partition keys must appear as the trailing columns of
    the SELECT list."""
    body = _strip_comments(_read_sql())
    # Find the final SELECT (top-level GROUP BY query).
    # Just ensure the last two emitted columns are league + season.
    # We grep for the literal block `league,\n    season` which the file uses.
    assert re.search(r"\bleague,\s*\n\s*season\s*\n", body), (
        "partition columns league/season must be emitted as last columns"
    )


# ---------------------------------------------------------------------------
# DuckDB data-test (#573) — shots_on_target_proxy = saves + scored goals
# ---------------------------------------------------------------------------
#
# Seeds the two Silver sources (events_spadl + xref_player), runs the SELECT on
# DuckDB and asserts on-target counts SavedShot + Goal only — verifying the
# ``_action_source_note`` signal survives the events->joined CTE plumbing.


def _translate_trino_to_duckdb(sql: str) -> str:
    sql = sql.replace(
        "iceberg.silver.whoscored_events_spadl", "silver_whoscored_events_spadl"
    )
    sql = sql.replace("iceberg.silver.xref_player", "silver_xref_player")
    sql = re.sub(r"\bregexp_like\s*\(", "regexp_matches(", sql, flags=re.IGNORECASE)
    return sql


_SPADL_COLUMNS: List[str] = [
    "match_id", "player_id_raw", "action_canonical", "_action_source_note",
    "outcome_success", "x", "y", "league", "season",
]


def _spadl_row(
    *,
    match_id: str = "100",
    player_id_raw: str = "555",
    action_canonical: str = "pass",
    action_source_note: str = "Pass",
    outcome_success: bool = True,
    x: float = 90.0,
    y: float = 50.0,
    league: str = "ENG-Premier League",
    season: str = "2526",
) -> Dict[str, Any]:
    return {
        "match_id": match_id,
        "player_id_raw": player_id_raw,
        "action_canonical": action_canonical,
        "_action_source_note": action_source_note,
        "outcome_success": outcome_success,
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


def _seed_and_run(con, spadl_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    con.execute("DROP TABLE IF EXISTS silver_whoscored_events_spadl")
    con.execute("DROP TABLE IF EXISTS silver_xref_player")
    con.execute(
        """
        CREATE TABLE silver_whoscored_events_spadl (
            match_id            VARCHAR,
            player_id_raw       VARCHAR,
            action_canonical    VARCHAR,
            _action_source_note VARCHAR,
            outcome_success     BOOLEAN,
            x                   DOUBLE,
            y                   DOUBLE,
            league              VARCHAR,
            season              VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE silver_xref_player (
            canonical_id VARCHAR,
            source_id    VARCHAR,
            source       VARCHAR,
            league       VARCHAR,
            season       VARCHAR
        )
        """
    )
    # One whoscored player (555) -> canonical fb_999, resolvable for 2526 APL.
    con.execute(
        "INSERT INTO silver_xref_player VALUES "
        "('fb_999', '555', 'whoscored', 'ENG-Premier League', '2526')"
    )
    placeholders = ", ".join(["?"] * len(_SPADL_COLUMNS))
    insert_sql = (
        f"INSERT INTO silver_whoscored_events_spadl "
        f"({', '.join(_SPADL_COLUMNS)}) VALUES ({placeholders})"
    )
    for row in spadl_rows:
        con.execute(insert_sql, [row[c] for c in _SPADL_COLUMNS])

    sql = _translate_trino_to_duckdb(_read_sql())
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class TestShotsOnTargetSavesPlusGoals:
    """on-target = SavedShot + Goal, not every Successful shot (#573) — through
    the xref_player join."""

    def _shot_corpus(self) -> List[Dict[str, Any]]:
        return [
            _spadl_row(action_canonical="shot", action_source_note="SavedShot",
                       outcome_success=True),                                   # on target
            _spadl_row(action_canonical="shot", action_source_note="Goal",
                       outcome_success=True),                                   # on target (scored)
            _spadl_row(action_canonical="shot", action_source_note="MissedShots",
                       outcome_success=True),                                   # off target, but Successful
            _spadl_row(action_canonical="shot", action_source_note="ShotOnPost",
                       outcome_success=True),                                   # off target, but Successful
            _spadl_row(action_canonical="shot_penalty", action_source_note="Goal",
                       outcome_success=True),                                   # on target (scored pen)
            _spadl_row(action_canonical="own_goal", action_source_note="Goal",
                       outcome_success=True),                                   # excluded by shot-family
        ]

    def test_shots_total_counts_all_shot_family_excl_own_goal(self, duck_conn):
        out = _seed_and_run(duck_conn, self._shot_corpus())
        assert len(out) == 1
        assert out[0]["shots_total"] == 5

    def test_shots_on_target_is_saves_plus_goals(self, duck_conn):
        out = _seed_and_run(duck_conn, self._shot_corpus())
        # SavedShot + open-play Goal + penalty Goal = 3.
        assert out[0]["shots_on_target_proxy"] == 3

    def test_missed_shot_successful_is_not_on_target(self, duck_conn):
        out = _seed_and_run(duck_conn, [
            _spadl_row(action_canonical="shot", action_source_note="SavedShot",
                       outcome_success=True),
            _spadl_row(action_canonical="shot", action_source_note="MissedShots",
                       outcome_success=True),
        ])
        assert out[0]["shots_total"] == 2
        assert out[0]["shots_on_target_proxy"] == 1
