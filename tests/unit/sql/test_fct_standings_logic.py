"""
Unit tests for Gold ``fct_standings`` (ex-dim_standings, #428) SQL logic (E2 — 2026-05).

Reads ``bronze.sofascore_league_table`` (APPEND-mode snapshots) directly,
deduplicates to the latest ``_ingested_at`` per (league, season, team),
LEFT JOINs ``silver.xref_team`` (post-E1.5 cutover) to resolve the
SofaScore team name to a canonical ``team_id``, and emits one row per
(league, season, team).

Resolution path:
    matched -> team_id = xref_team.canonical_id, team_id_source='fbref_canonical'
    orphan  -> team_id = 'ss_<slug>',            team_id_source='sofascore_orphan'

Two test layers:
  * ``TestFctStandingsLogic`` (legacy) — Trino → DuckDB transpile via
    sqlglot; fixture rows in-memory; assert on the result set.
  * ``TestFctStandingsCutoverStructure`` (E1.5 prep) — regex sanity over
    the raw SQL pinning down the silver.xref_team migration so the
    structure can't drift even if DuckDB transpile breaks.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_standings.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


# ---------------------------------------------------------------------------
# E1.5 cutover structural tests — no engine, pure regex over raw SQL.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFctStandingsCutoverStructure:
    """Regex sanity over ``fct_standings.sql`` post-E1.5 cutover."""

    def test_join_uses_silver_xref_team(self):
        """JOIN must hit silver.xref_team, not gold.entity_xref."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql, (
            "fct_standings.sql must JOIN iceberg.silver.xref_team after "
            "the E1.5 cutover"
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "fct_standings.sql must NOT reference gold.entity_xref in "
            "executable SQL after E1.5 cutover"
        )

    def test_join_filters_sofascore_source(self):
        """JOIN must filter `source = 'sofascore'`."""
        sql = _read_sql()
        assert re.search(
            r"\.source\s*=\s*'sofascore'", sql, re.IGNORECASE,
        ) or re.search(
            r"source\s*=\s*'sofascore'", sql, re.IGNORECASE,
        ), (
            "fct_standings.sql must filter `source = 'sofascore'` on the "
            "silver.xref_team join"
        )

    def test_join_includes_league_predicate(self):
        """JOIN must constrain league (xref_team is per-(source, source_id,
        league, season) — without league predicate we get 1.5-4x fan-out)."""
        sql = _read_sql()
        # Allow either `x.league = s.league` or unaliased forms.
        assert re.search(
            r"\.league\s*=\s*s\.league", sql, re.IGNORECASE,
        ) or re.search(
            r"\bleague\s*=\s*s\.league", sql, re.IGNORECASE,
        ), (
            "fct_standings.sql must include a `league = s.league` "
            "predicate in the silver.xref_team join"
        )

    def test_join_includes_season_predicate(self):
        """xref_team.season is varchar — JOIN must use season_slug
        (the raw 'YYYY' label) rather than the bigint season."""
        sql = _read_sql()
        # We accept any predicate that compares xref season to s.season_slug
        # OR a CAST(s.season AS varchar) form.
        has_slug = re.search(
            r"season\s*=\s*s\.season_slug", sql, re.IGNORECASE,
        )
        has_cast = re.search(
            r"season\s*=\s*CAST\s*\(\s*s\.season\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        )
        assert has_slug or has_cast, (
            "fct_standings.sql must include a season predicate on the "
            "silver.xref_team JOIN — either `season = s.season_slug` or "
            "`season = CAST(s.season AS varchar)`"
        )

    def test_join_excludes_orphan_confidence(self):
        """#460: the silver.xref_team JOIN must exclude orphan rows.
        Orphan xref rows carry a non-NULL source-prefixed canonical_id
        ('ss_<slug>'), so without `confidence <> 'orphan'` they would be
        mislabeled team_id_source='fbref_canonical' (xref_team.sql.j2
        contract: orphans are excluded from every cross-source Gold JOIN)."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE,
        ), (
            "fct_standings.sql must filter `confidence <> 'orphan'` on the "
            "silver.xref_team join (#460)"
        )

    def test_orphan_fallback_ss_prefix_regex(self):
        """Orphan fallback regex: `'ss_' || lower(regexp_replace(...))` must
        survive the cutover — silver.xref_team's orphan namespace is
        per-source ('ss_' for SofaScore)."""
        sql = _read_sql()
        # Tolerant matcher — accept any whitespace, single or double-quoted
        # regex literal in regexp_replace.
        pattern = re.compile(
            r"'ss_'\s*\|\|\s*lower\s*\(\s*regexp_replace\s*\(",
            re.IGNORECASE,
        )
        assert pattern.search(sql), (
            "fct_standings.sql must keep the `'ss_' || "
            "lower(regexp_replace(...))` orphan fallback"
        )

    def test_team_id_source_branch_labels(self):
        """The CASE branch labels (fbref_canonical / sofascore_orphan)
        must remain stable — downstream marts/DQ depend on the exact
        string values."""
        sql = _read_sql()
        assert "'fbref_canonical'" in sql, (
            "fct_standings.sql must keep the 'fbref_canonical' label"
        )
        assert "'sofascore_orphan'" in sql, (
            "fct_standings.sql must keep the 'sofascore_orphan' label"
        )

    def test_migration_breadcrumb_in_header(self):
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql, (
            "fct_standings.sql must keep the E1.5 migration breadcrumb"
        )


# ---------------------------------------------------------------------------
# Legacy DuckDB-transpile tests — preserved as-is from E2.
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
    # 8 rows: 4 teams × 2 snapshots (latest must win after dedup).
    con.execute("""
        INSERT INTO bronze.sofascore_league_table VALUES
        -- Older snapshot (must be discarded)
        ('ENG-Premier League', '2425', 'Manchester City', 10, 8, 1, 1, 30, 10, 20, 25,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       10, 7, 2, 1, 25, 12, 13, 23,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Arsenal',         10, 6, 3, 1, 22, 14,  8, 21,
         TIMESTAMP '2026-04-26 06:00:00'),
        ('ENG-Premier League', '2425', 'Burnley',         10, 2, 2, 6,  8, 20, -12, 8,
         TIMESTAMP '2026-04-26 06:00:00'),
        -- Latest snapshot (must win)
        ('ENG-Premier League', '2425', 'Manchester City', 12, 10, 1, 1, 35, 11, 24, 31,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Liverpool',       12, 8, 2, 2, 28, 14, 14, 26,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Arsenal',         12, 7, 3, 2, 24, 16,  8, 24,
         TIMESTAMP '2026-04-27 06:00:00'),
        ('ENG-Premier League', '2425', 'Burnley',         12, 2, 3, 7, 10, 24, -14, 9,
         TIMESTAMP '2026-04-27 06:00:00')
    """)

    # silver.xref_team (post-E1.5 cutover) — minimal columns the JOIN touches.
    # Note: season is VARCHAR (the season_slug — '2425') unlike the legacy
    # gold.entity_xref where it was BIGINT. This is intentional: silver
    # xref tables use the raw season-slug per the documented contract.
    # 2 of the 4 teams resolved -> canonical_id. Two orphan shapes (#460):
    #   * Arsenal — xref ROW EXISTS with confidence='orphan' and a non-NULL
    #     source-prefixed canonical_id ('ss_arsenal'). This mirrors live
    #     xref_team: the orphan fallback in xref_team.sql.j2 always emits a
    #     canonical_id, so a JOIN without `confidence <> 'orphan'` would
    #     mislabel the row as resolved.
    #   * Burnley — no xref row at all (fresh team before xref refresh);
    #     covers the COALESCE 'ss_<slug>' fallback path.
    con.execute("""
        CREATE TABLE silver.xref_team (
            source VARCHAR,
            source_id VARCHAR,
            canonical_id VARCHAR,
            display_name VARCHAR,
            league VARCHAR,
            season VARCHAR,
            confidence VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.xref_team VALUES
        ('sofascore', 'Manchester City', 'manchester_city', 'Manchester City',
         'ENG-Premier League', '2425', 'name_alias'),
        ('sofascore', 'Liverpool',       'liverpool',       'Liverpool',
         'ENG-Premier League', '2425', 'name_alias'),
        ('sofascore', 'Arsenal',         'ss_arsenal',      'Arsenal',
         'ENG-Premier League', '2425', 'orphan')
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
        pytest.skip(f"DuckDB execution of translated fct_standings SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestFctStandingsLogic:
    def test_dedup_to_latest_snapshot(self, gold_rows):
        """8 input rows (4 teams × 2 snapshots) → 4 output rows."""
        assert len(gold_rows) == 4, (
            f"expected 4 rows after snapshot dedup, got {len(gold_rows)}: "
            f"{[(r['team_name_raw'], r['played']) for r in gold_rows]}"
        )

    def test_position_ordering(self, gold_rows):
        """``position`` = ROW_NUMBER OVER (PARTITION BY league, season ORDER BY
        points DESC, goal_diff DESC, goals_for DESC).
        Input (latest): MCI=31pts, LIV=26pts, ARS=24pts, BUR=9pts.
        """
        by_position = sorted(gold_rows, key=lambda r: r["position"])
        assert [r["position"] for r in by_position] == [1, 2, 3, 4]
        assert by_position[0]["team_name_raw"] == "Manchester City"
        assert by_position[1]["team_name_raw"] == "Liverpool"
        assert by_position[2]["team_name_raw"] == "Arsenal"
        assert by_position[3]["team_name_raw"] == "Burnley"

    def test_resolved_team_uses_canonical_id(self, gold_rows):
        """When silver.xref_team has a row, team_id = canonical_id and
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
        """Both orphan shapes must land on team_id = 'ss_<slug>' and
        team_id_source = 'sofascore_orphan' (#460):
          * Arsenal — xref row exists with confidence='orphan' (must be
            excluded by the JOIN filter, NOT counted as resolved);
          * Burnley — no xref row at all (COALESCE fallback path).
        """
        orphans = {r["team_name_raw"]: r for r in gold_rows
                   if r["team_id_source"] == "sofascore_orphan"}
        assert set(orphans) == {"Arsenal", "Burnley"}, (
            f"expected orphans {{'Arsenal', 'Burnley'}}, got "
            f"{sorted(orphans)}"
        )
        assert orphans["Arsenal"]["team_id"] == "ss_arsenal", (
            f"orphan team_id must be 'ss_arsenal', got "
            f"{orphans['Arsenal']['team_id']!r}"
        )
        assert orphans["Burnley"]["team_id"] == "ss_burnley", (
            f"orphan team_id must be 'ss_burnley', got "
            f"{orphans['Burnley']['team_id']!r}"
        )

    def test_points_per_game_exact(self, gold_rows):
        """``points_per_game = CAST(points AS DOUBLE) / NULLIF(played, 0)`` —
        latest MCI: 31 / 12 == 2.5833…
        """
        mci = next(r for r in gold_rows if r["team_name_raw"] == "Manchester City")
        assert mci["points"] == 31
        assert mci["played"] == 12
        assert mci["points_per_game"] == pytest.approx(31 / 12)

    def test_match_columns_carry_through(self, gold_rows):
        """played / wins / draws / losses / goals_for / goals_against / goal_diff /
        points are all surfaced from the latest snapshot."""
        mci = next(r for r in gold_rows if r["team_name_raw"] == "Manchester City")
        assert mci["played"] == 12
        assert mci["wins"] == 10
        assert mci["draws"] == 1
        assert mci["losses"] == 1
        assert mci["goals_for"] == 35
        assert mci["goals_against"] == 11
        assert mci["goal_diff"] == 24
        assert mci["points"] == 31

    def test_season_normalised_to_bigint(self, gold_rows):
        """#404: SofaScore bronze season slug '2425' is emitted as-is (varchar),
        no longer normalised to bigint year-start."""
        for r in gold_rows:
            assert r["season"] == "2425", (
                f"season must be slug '2425', got {r['season']!r}"
            )
