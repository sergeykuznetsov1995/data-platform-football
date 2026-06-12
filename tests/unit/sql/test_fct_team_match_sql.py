"""
Unit tests for ``dags/sql/gold/fct_team_match.sql`` points/result logic (#452).

Bug: the CASE expressions for ``points``/``result`` had no NULL-guard —
for unplayed matches (``is_completed = FALSE``, goals_for/goals_against
NULL) both comparisons evaluate to NULL, fall through to ELSE, and the
match gets ``points = 0`` / ``result = 'L'`` (a phantom loss).

Expected semantics (mirrors ``result_1x2`` in ``dim_match.sql.j2``):
explicit WHEN branch for ``<`` instead of ELSE — NULL goals fall through
to the implicit NULL.

Two test layers (pattern: ``test_fct_standings_logic.py``):
  * ``TestFctTeamMatchPointsResultLogic`` — Trino → DuckDB transpile via
    sqlglot; fixture: dim_match + fbref_match_enriched with a win, a draw
    and an unplayed match; empty stubs for the 5 cross-source LEFT JOIN
    blocks (understat/whoscored/sofascore/fotmob + xref bridges).
  * ``TestFctTeamMatchNullGuardStructure`` — regex sanity over the raw
    SQL so the guard can't drift even if DuckDB transpile breaks.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_team_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Structural tests — no engine, pure regex over raw SQL.
# ---------------------------------------------------------------------------


class TestFctTeamMatchNullGuardStructure:
    """The points/result CASE blocks must not fall through to a bare ELSE."""

    def test_points_case_has_no_bare_else(self):
        sql = _strip_comments(_read_sql())
        assert not re.search(
            r"ELSE\s+0\s+END\s+AS\s+points", sql, re.IGNORECASE
        ), (
            "points CASE must not use `ELSE 0` — NULL goals (unplayed "
            "matches) would silently become 0 points (#452)"
        )

    def test_result_case_has_no_bare_else(self):
        sql = _strip_comments(_read_sql())
        assert not re.search(
            r"ELSE\s+'L'\s+END\s+AS\s+result", sql, re.IGNORECASE
        ), (
            "result CASE must not use `ELSE 'L'` — NULL goals (unplayed "
            "matches) would silently become a phantom loss (#452)"
        )

    def test_points_case_has_explicit_loss_branch(self):
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"WHEN\s+u\.goals_for\s*<\s*u\.goals_against\s+THEN\s+0",
            sql, re.IGNORECASE,
        ), "points CASE must spell out the `<` branch explicitly (#452)"

    def test_result_case_has_explicit_loss_branch(self):
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"WHEN\s+u\.goals_for\s*<\s*u\.goals_against\s+THEN\s+'L'",
            sql, re.IGNORECASE,
        ), "result CASE must spell out the `<` branch explicitly (#452)"


# ---------------------------------------------------------------------------
# DuckDB-transpile logic tests.
# ---------------------------------------------------------------------------

sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


def _translate(sql_text: str) -> str:
    """Trino → DuckDB transpile + iceberg.<schema>.<tbl> → <schema>.<tbl>."""
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    out = out.replace("iceberg.gold.", "gold.")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # ===== spine: gold.dim_match =====
    # Live-mirrored formats (2026-06-12): match_id = FBref raw hex,
    # season = slug '2425', league 'ENG-Premier League', team_id = slug.
    # is_completed := (home_score IS NOT NULL AND away_score IS NOT NULL)
    # per dim_match.sql.j2 — the unplayed fixture row keeps that contract.
    con.execute("""
        CREATE TABLE gold.dim_match (
            match_id VARCHAR,
            match_date DATE,
            season VARCHAR,
            league VARCHAR,
            gameweek VARCHAR,
            home_team_id VARCHAR,
            away_team_id VARCHAR,
            is_completed BOOLEAN
        )
    """)
    con.execute("""
        INSERT INTO gold.dim_match VALUES
        ('a1b2c3d4', DATE '2024-08-25', '2425', 'ENG-Premier League', '2',
         'liverpool', 'chelsea', TRUE),
        ('b2c3d4e5', DATE '2024-09-21', '2425', 'ENG-Premier League', '5',
         'arsenal', 'tottenham_hotspur', TRUE),
        ('c3d4e5f6', DATE '2025-05-25', '2425', 'ENG-Premier League', '38',
         'liverpool', 'arsenal', FALSE)
    """)

    # ===== spine: silver.fbref_match_enriched =====
    # Unplayed match c3d4e5f6: scores AND match stats all NULL.
    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            match_id VARCHAR,
            home_score INTEGER,
            away_score INTEGER,
            home_shots INTEGER,
            away_shots INTEGER,
            home_sot INTEGER,
            away_sot INTEGER,
            home_possession INTEGER,
            away_possession INTEGER,
            home_yellow_cards INTEGER,
            away_yellow_cards INTEGER,
            home_red_cards INTEGER,
            away_red_cards INTEGER,
            home_saves INTEGER,
            away_saves INTEGER
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_match_enriched VALUES
        ('a1b2c3d4', 3, 1, 23, 10, 12, 4, 61, 39, 1, 0, 0, 0, 3, 9),
        ('b2c3d4e5', 1, 1, 14, 14, 5, 5, 50, 50, 2, 3, 0, 0, 4, 4),
        ('c3d4e5f6', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, NULL, NULL)
    """)

    # ===== empty stubs for the cross-source LEFT JOIN blocks =====
    # Only the columns the SQL references; zero rows — every LEFT JOIN
    # leaves its block NULL, which is irrelevant to points/result.
    con.execute("""
        CREATE TABLE silver.xref_team (
            source VARCHAR, source_id VARCHAR, canonical_id VARCHAR,
            league VARCHAR, season VARCHAR, confidence VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_match (
            source VARCHAR, source_id VARCHAR, canonical_id VARCHAR,
            league VARCHAR, season VARCHAR, confidence VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE bronze.whoscored_schedule (
            home_team_id DOUBLE, home_team VARCHAR,
            away_team_id DOUBLE, away_team VARCHAR,
            league VARCHAR, season VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.understat_team_match (
            match_id VARCHAR, team_id_canonical VARCHAR,
            league VARCHAR, season VARCHAR,
            xg DOUBLE, npxg DOUBLE, xg_against DOUBLE,
            deep_completions INTEGER, ppda DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE silver.whoscored_team_match (
            match_id VARCHAR, team_id VARCHAR,
            league VARCHAR, season VARCHAR,
            pass_total INTEGER, pass_pct DOUBLE, touches_in_box INTEGER,
            tackle_att INTEGER, interceptions INTEGER,
            ball_recoveries INTEGER, key_passes_ws INTEGER,
            takeon_att INTEGER, takeon_won INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE silver.sofascore_team_match (
            match_id VARCHAR, team_id VARCHAR,
            league VARCHAR, season VARCHAR,
            expected_goals DOUBLE, expected_goals_against DOUBLE,
            total_passes INTEGER, accurate_passes_pct DOUBLE,
            total_tackles INTEGER, interceptions INTEGER,
            ground_duels_won_pct DOUBLE, aerial_duels_won_pct DOUBLE,
            fouls INTEGER, corner_kicks INTEGER, offsides INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE silver.fotmob_team_match (
            match_id VARCHAR, team_id VARCHAR,
            league VARCHAR, season VARCHAR,
            shots_inside_box INTEGER, big_chances INTEGER,
            expected_goals DOUBLE, npxg DOUBLE, xgot DOUBLE,
            expected_assists DOUBLE, touches_in_box INTEGER,
            clearances INTEGER, big_chances_missed INTEGER,
            shots_outside_box INTEGER, blocked_shots INTEGER,
            shots_off_target INTEGER
        )
    """)


@pytest.fixture(scope="module")
def fct_rows():
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    try:
        translated = _translate(sql_text)
    except Exception as e:
        pytest.skip(f"sqlglot Trino→DuckDB translation failed: {e}")

    con = duckdb.connect(":memory:")
    try:
        _bootstrap(con)
    except Exception as e:
        pytest.skip(f"DuckDB fixture bootstrap failed: {e}")

    try:
        rows = con.execute(translated).fetchall()
        col_names = [c[0] for c in con.description]
    except Exception as e:
        pytest.skip(f"DuckDB execution of translated fct_team_match SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


def _row(rows, match_id, team_id):
    matched = [
        r for r in rows
        if r["match_id"] == match_id and r["team_id"] == team_id
    ]
    assert len(matched) == 1, (
        f"expected exactly 1 row for ({match_id}, {team_id}), "
        f"got {len(matched)}"
    )
    return matched[0]


class TestFctTeamMatchPointsResultLogic:

    def test_two_rows_per_match(self, fct_rows):
        assert len(fct_rows) == 6  # 3 matches × 2 sides

    def test_home_win_points_and_result(self, fct_rows):
        home = _row(fct_rows, "a1b2c3d4", "liverpool")
        away = _row(fct_rows, "a1b2c3d4", "chelsea")
        assert (home["points"], home["result"]) == (3, "W")
        assert (away["points"], away["result"]) == (0, "L")

    def test_draw_points_and_result(self, fct_rows):
        for team in ("arsenal", "tottenham_hotspur"):
            row = _row(fct_rows, "b2c3d4e5", team)
            assert (row["points"], row["result"]) == (1, "D")

    def test_unplayed_match_yields_null_points_and_result(self, fct_rows):
        """#452: unplayed match (NULL goals) must NOT become a phantom
        0-point loss — points/result must be NULL for both sides."""
        for team in ("liverpool", "arsenal"):
            row = _row(fct_rows, "c3d4e5f6", team)
            assert row["is_completed"] is False
            assert row["points"] is None, (
                f"unplayed match: points must be NULL, got {row['points']!r}"
            )
            assert row["result"] is None, (
                f"unplayed match: result must be NULL, got {row['result']!r}"
            )
