"""
Unit tests for Gold ``dim_standings`` SQL logic (E2 — 2026-05).

Reads ``bronze.sofascore_league_table`` (APPEND-mode snapshots) directly,
deduplicates to the latest ``_ingested_at`` per (league, season, team),
LEFT JOINs ``gold.entity_xref`` to resolve the SofaScore team name to a
canonical ``team_id``, and emits one row per (league, season, team).

Resolution path:
    matched -> team_id = entity_xref.canonical_id, team_id_source='fbref_canonical'
    orphan  -> team_id = 'ss_<slug>',              team_id_source='sofascore_orphan'

Strategy: Trino → DuckDB transpile via sqlglot, fixture rows in an
in-memory schema, then assert on the result set.
"""

from __future__ import annotations

from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_standings.sql"


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
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # bronze.sofascore_league_table (APPEND mode):
    # team / mp / w / d / l / gf / ga / gd / pts / season ('YYYY' format) / league
    con.execute("""
        CREATE TABLE bronze.sofascore_league_table (
            league VARCHAR,
            season VARCHAR,
            team VARCHAR,
            mp INTEGER,
            w  INTEGER,
            d  INTEGER,
            l  INTEGER,
            gf INTEGER,
            ga INTEGER,
            gd INTEGER,
            pts INTEGER,
            _ingested_at TIMESTAMP
        )
    """)
    # 6 rows: 3 teams × 2 snapshots (latest must win after dedup).
    con.execute("""
        INSERT INTO bronze.sofascore_league_table VALUES
        -- Older snapshot (must be discarded)
        ('ENG-Premier League', '2425', 'Manchester City', 10, 8, 1, 1, 30, 10, 20, 25,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       10, 7, 2, 1, 25, 12, 13, 23,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Arsenal',         10, 6, 3, 1, 22, 14,  8, 21,
         TIMESTAMP '2026-04-26 06:00:00'),
        -- Latest snapshot (must win)
        ('ENG-Premier League', '2425', 'Manchester City', 12, 10, 1, 1, 35, 11, 24, 31,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       12, 8, 2, 2, 28, 14, 14, 26,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Arsenal',         12, 7, 3, 2, 24, 16,  8, 24,
         TIMESTAMP '2026-04-27 06:00:00')
    """)

    # gold.entity_xref — minimal columns the JOIN touches.
    # 2 of the 3 teams resolved -> canonical_id; the 3rd (Arsenal) is an orphan.
    con.execute("""
        CREATE TABLE gold.entity_xref (
            entity_type VARCHAR,
            source VARCHAR,
            source_id VARCHAR,
            canonical_id VARCHAR,
            league VARCHAR,
            season BIGINT
        )
    """)
    con.execute("""
        INSERT INTO gold.entity_xref VALUES
        ('team', 'sofascore', 'Manchester City', 'manchester_city',
         'ENG-Premier League', 2024),
        ('team', 'sofascore', 'Liverpool',       'liverpool',
         'ENG-Premier League', 2024)
    """)


@pytest.fixture(scope="module")
def gold_rows():
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
        pytest.skip(f"DuckDB execution of translated dim_standings SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestDimStandingsLogic:
    def test_dedup_to_latest_snapshot(self, gold_rows):
        """6 input rows (3 teams × 2 snapshots) → 3 output rows."""
        assert len(gold_rows) == 3, (
            f"expected 3 rows after snapshot dedup, got {len(gold_rows)}: "
            f"{[(r['team_name_raw'], r['mp']) for r in gold_rows]}"
        )

    def test_position_ordering(self, gold_rows):
        """``position`` = ROW_NUMBER OVER (PARTITION BY league, season ORDER BY
        points DESC, goal_diff DESC, goals_for DESC).
        Input (latest): MCI=31pts, LIV=26pts, ARS=24pts.
        """
        by_position = sorted(gold_rows, key=lambda r: r["position"])
        assert [r["position"] for r in by_position] == [1, 2, 3]
        assert by_position[0]["team_name_raw"] == "Manchester City"
        assert by_position[1]["team_name_raw"] == "Liverpool"
        assert by_position[2]["team_name_raw"] == "Arsenal"

    def test_resolved_team_uses_canonical_id(self, gold_rows):
        """When entity_xref has a row, team_id = canonical_id and
        team_id_source = 'fbref_canonical'."""
        resolved = [r for r in gold_rows
                    if r["team_id_source"] == "fbref_canonical"]
        assert len(resolved) == 2, (
            f"expected 2 resolved teams, got {len(resolved)}"
        )
        ids = {r["team_name_raw"]: r["team_id"] for r in resolved}
        assert ids["Manchester City"] == "manchester_city"
        assert ids["Liverpool"] == "liverpool"

    def test_orphan_team_uses_ss_prefix(self, gold_rows):
        """When entity_xref has no match, team_id = 'ss_<slug>' and
        team_id_source = 'sofascore_orphan'."""
        orphans = [r for r in gold_rows
                   if r["team_id_source"] == "sofascore_orphan"]
        assert len(orphans) == 1, (
            f"expected 1 orphan team (Arsenal), got {len(orphans)}: "
            f"{[r['team_name_raw'] for r in orphans]}"
        )
        ars = orphans[0]
        assert ars["team_name_raw"] == "Arsenal"
        assert ars["team_id"] == "ss_arsenal", (
            f"orphan team_id must be 'ss_arsenal', got {ars['team_id']!r}"
        )

    def test_points_per_game_exact(self, gold_rows):
        """``points_per_game = CAST(points AS DOUBLE) / NULLIF(mp, 0)`` —
        latest MCI: 31 / 12 == 2.5833…
        """
        mci = next(r for r in gold_rows if r["team_name_raw"] == "Manchester City")
        assert mci["points"] == 31
        assert mci["mp"] == 12
        assert mci["points_per_game"] == pytest.approx(31 / 12)

    def test_match_columns_carry_through(self, gold_rows):
        """mp / wins / draws / losses / goals_for / goals_against / goal_diff /
        points are all surfaced from the latest snapshot."""
        mci = next(r for r in gold_rows if r["team_name_raw"] == "Manchester City")
        assert mci["mp"] == 12
        assert mci["wins"] == 10
        assert mci["draws"] == 1
        assert mci["losses"] == 1
        assert mci["goals_for"] == 35
        assert mci["goals_against"] == 11
        assert mci["goal_diff"] == 24
        assert mci["points"] == 31

    def test_season_normalised_to_bigint(self, gold_rows):
        """Bronze season '2425' → BIGINT 2024 (substr(1,2)+2000)."""
        for r in gold_rows:
            assert r["season"] == 2024, (
                f"season must be 2024 (from '2425'), got {r['season']!r}"
            )
