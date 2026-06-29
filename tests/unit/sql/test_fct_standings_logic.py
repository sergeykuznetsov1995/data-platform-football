"""
Unit tests for Gold ``fct_standings`` SQL logic (#702 — multi-source, 2026-06).

Reads TWO Silver standings projections (Gold one-hop — no direct Bronze read):
    * ``silver.sofascore_league_table``  (primary)
    * ``silver.fotmob_team_standings``   (fallback)
Both are already deduped at Silver. Gold LEFT JOINs ``silver.xref_team`` per
source to resolve the team name to a canonical ``team_id``, then merges the two
sources whole-table: SofaScore wins a (league, season); FotMob only fills
(league, season) pairs SofaScore did not cover.

Resolution path:
    matched -> team_id = xref_team.canonical_id,  team_id_source='fbref_canonical'
    ss orphan -> team_id = 'ss_<slug>',           team_id_source='sofascore_orphan'
    fm orphan -> team_id = 'fm_<slug>',           team_id_source='fotmob_orphan'
``standings_source`` ('sofascore'/'fotmob') carries row provenance.

Two test layers:
  * ``TestFctStandingsStructure`` — regex sanity over the raw SQL (no engine),
    pinning the Silver sources, the per-source xref joins and the merge.
  * ``TestFctStandingsLogic`` — Trino → DuckDB transpile via sqlglot; fixture
    rows in-memory; assert on the result set.
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
# Structural tests — no engine, pure regex over raw SQL.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFctStandingsStructure:
    """Regex sanity over ``fct_standings.sql`` (#702 multi-source)."""

    def test_reads_silver_sofascore_not_bronze(self):
        """#702: Gold one-hop — read silver.sofascore_league_table, NOT bronze."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.sofascore_league_table" in sql, (
            "fct_standings.sql must read silver.sofascore_league_table"
        )
        assert "bronze.sofascore_league_table" not in sql, (
            "fct_standings.sql must NOT read bronze.sofascore_league_table "
            "directly (#702 — Gold one-hop from Silver)"
        )

    def test_reads_silver_fotmob_standings(self):
        """#702: FotMob is the 2nd source via silver.fotmob_team_standings."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.fotmob_team_standings" in sql, (
            "fct_standings.sql must read silver.fotmob_team_standings (#702)"
        )

    def test_join_uses_silver_xref_team(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "fct_standings.sql must NOT reference gold.entity_xref in "
            "executable SQL (E1.5 cutover)"
        )

    def test_join_filters_both_sources(self):
        """Each xref JOIN must filter its source (sofascore / fotmob)."""
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'sofascore'", sql, re.IGNORECASE)
        assert re.search(r"source\s*=\s*'fotmob'", sql, re.IGNORECASE)

    def test_joins_include_league_predicate(self):
        """xref footgun: both joins constrain league (xref_team is per-(source,
        source_id, league, season) — without it 1.5-4x fan-out)."""
        sql = _read_sql()
        assert re.search(r"\.league\s*=\s*s\.league", sql, re.IGNORECASE), (
            "SofaScore xref join must include `league = s.league`"
        )
        assert re.search(r"\.league\s*=\s*f\.league", sql, re.IGNORECASE), (
            "FotMob xref join must include `league = f.league`"
        )

    def test_joins_include_season_predicate(self):
        """Both Silver sources carry season slug ('2526', #404) — joins compare
        directly to xref_team.season (no CAST / no season_slug alias)."""
        sql = _read_sql()
        assert re.search(r"\.season\s*=\s*s\.season\b", sql, re.IGNORECASE), (
            "SofaScore xref join must include `season = s.season`"
        )
        assert re.search(r"\.season\s*=\s*f\.season\b", sql, re.IGNORECASE), (
            "FotMob xref join must include `season = f.season`"
        )

    def test_joins_exclude_orphan_confidence(self):
        """#460: both joins must exclude confidence='orphan' xref rows (they
        carry a non-NULL source-prefixed canonical_id → would be mislabeled
        'fbref_canonical')."""
        sql = _read_sql()
        # Two source blocks → expect at least two `confidence <> 'orphan'`.
        hits = re.findall(r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE)
        assert len(hits) >= 2, (
            "both xref joins (sofascore + fotmob) must filter "
            "`confidence <> 'orphan'`"
        )

    def test_orphan_fallback_prefixes(self):
        """Per-source orphan namespaces: 'ss_' (SofaScore) and 'fm_' (FotMob),
        both feeding `|| lower(regexp_replace(...))`."""
        sql = _read_sql()
        assert "'ss_'" in sql, "must keep SofaScore orphan prefix 'ss_'"
        assert "'fm_'" in sql, "must keep FotMob orphan prefix 'fm_'"
        assert re.search(
            r"\|\|\s*lower\s*\(\s*regexp_replace\s*\(", sql, re.IGNORECASE,
        ), "orphan fallback must build '<prefix>' || lower(regexp_replace(...))"

    def test_team_id_source_branch_labels(self):
        """The CASE labels must stay stable — downstream marts/DQ depend on
        the exact strings."""
        sql = _read_sql()
        for label in ("'fbref_canonical'", "'sofascore_orphan'", "'fotmob_orphan'"):
            assert label in sql, f"fct_standings.sql must keep the {label} label"

    def test_standings_source_provenance_column(self):
        """#702: standings_source ('sofascore'/'fotmob') must be projected."""
        sql = _read_sql()
        assert re.search(r"as\s+standings_source", sql, re.IGNORECASE), (
            "fct_standings.sql must emit a standings_source column"
        )

    def test_migration_breadcrumb_in_header(self):
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql


# ---------------------------------------------------------------------------
# DuckDB-transpile tests — Trino SQL executed against in-memory fixtures.
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
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # --- silver.sofascore_league_table (already deduped at Silver) ----------
    # ENG-Premier League 2425, 4 teams. Conformed columns (team_name, played…).
    con.execute("""
        CREATE TABLE silver.sofascore_league_table (
            team_name VARCHAR,
            played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
            goals_for INTEGER, goals_against INTEGER, goal_diff INTEGER,
            points INTEGER,
            _bronze_ingested_at TIMESTAMP,
            league VARCHAR,
            season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.sofascore_league_table VALUES
        ('Manchester City', 12, 10, 1, 1, 35, 11, 24, 31,
         TIMESTAMP '2026-04-27 06:00:00', 'ENG-Premier League', '2425'),
        ('Liverpool',       12, 8, 2, 2, 28, 14, 14, 26,
         TIMESTAMP '2026-04-27 06:00:00', 'ENG-Premier League', '2425'),
        ('Arsenal',         12, 7, 3, 2, 24, 16,  8, 24,
         TIMESTAMP '2026-04-27 06:00:00', 'ENG-Premier League', '2425'),
        ('Burnley',         12, 2, 3, 7, 10, 24, -14, 9,
         TIMESTAMP '2026-04-27 06:00:00', 'ENG-Premier League', '2425')
    """)

    # --- silver.fotmob_team_standings ---------------------------------------
    # Two (league, season) blocks:
    #   * ENG-Premier League 2425 — SAME key as SofaScore: MUST be dropped by
    #     the whole-table fallback (SofaScore wins). The absurd points=999
    #     would be obvious if it leaked.
    #   * ESP-La Liga 2425 — key absent from SofaScore: MUST appear (fallback).
    con.execute("""
        CREATE TABLE silver.fotmob_team_standings (
            team_id BIGINT,
            team_name VARCHAR,
            position BIGINT,
            played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
            goals_for INTEGER, goals_against INTEGER, goal_diff INTEGER,
            points INTEGER,
            _bronze_ingested_at TIMESTAMP,
            league VARCHAR,
            season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fotmob_team_standings VALUES
        -- collides with SofaScore key -> excluded
        (99, 'Manchester City', 1, 12, 12, 0, 0, 99, 0, 99, 999,
         TIMESTAMP '2026-04-27 06:00:00', 'ENG-Premier League', '2425'),
        -- ESP-La Liga 2425 -> SofaScore has no rows here -> FotMob fallback
        (1, 'Real Madrid', 1, 10, 9, 1, 0, 30, 8, 22, 28,
         TIMESTAMP '2026-04-27 06:00:00', 'ESP-La Liga', '2425'),
        (2, 'Barcelona',   2, 10, 8, 1, 1, 28, 10, 18, 25,
         TIMESTAMP '2026-04-27 06:00:00', 'ESP-La Liga', '2425')
    """)

    # --- silver.xref_team ----------------------------------------------------
    # Resolved: MCI, LIV (sofascore), Real Madrid (fotmob).
    # Orphan shapes:
    #   * Arsenal — xref ROW EXISTS with confidence='orphan' (must be excluded
    #     by the JOIN filter, NOT counted as resolved).
    #   * Burnley — no xref row at all (COALESCE 'ss_<slug>' fallback).
    #   * Barcelona — no fotmob xref row ('fm_<slug>' fallback).
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
         'ENG-Premier League', '2425', 'orphan'),
        ('fotmob',    'Real Madrid',     'real_madrid',     'Real Madrid',
         'ESP-La Liga', '2425', 'name_alias')
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


def _by_name(rows):
    return {r["team_name_raw"]: r for r in rows}


@pytest.mark.unit
class TestFctStandingsLogic:
    def test_row_count_merged_sources(self, gold_rows):
        """4 SofaScore (ENG) + 2 FotMob fallback (ESP) = 6. The colliding
        FotMob ENG row (points=999) is dropped."""
        assert len(gold_rows) == 6, (
            f"expected 6 rows, got {len(gold_rows)}: "
            f"{[(r['team_name_raw'], r['standings_source']) for r in gold_rows]}"
        )

    def test_sofascore_primary_wins_shared_key(self, gold_rows):
        """For ENG-Premier League 2425 (covered by both) only SofaScore rows
        survive — MCI points = 31, NOT the FotMob 999."""
        eng = [r for r in gold_rows if r["league"] == "ENG-Premier League"]
        assert len(eng) == 4
        assert all(r["standings_source"] == "sofascore" for r in eng)
        mci = _by_name(eng)["Manchester City"]
        assert mci["points"] == 31

    def test_fotmob_fallback_for_uncovered_key(self, gold_rows):
        """ESP-La Liga 2425 has no SofaScore rows → FotMob fills it."""
        esp = [r for r in gold_rows if r["league"] == "ESP-La Liga"]
        assert len(esp) == 2
        assert all(r["standings_source"] == "fotmob" for r in esp)

    def test_position_ordering_per_source(self, gold_rows):
        """position = ROW_NUMBER OVER (PARTITION BY league, season ORDER BY
        points DESC, goal_diff DESC, goals_for DESC) — within each block."""
        eng = sorted((r for r in gold_rows if r["league"] == "ENG-Premier League"),
                     key=lambda r: r["position"])
        assert [r["team_name_raw"] for r in eng] == [
            "Manchester City", "Liverpool", "Arsenal", "Burnley"]
        esp = sorted((r for r in gold_rows if r["league"] == "ESP-La Liga"),
                     key=lambda r: r["position"])
        assert [r["team_name_raw"] for r in esp] == ["Real Madrid", "Barcelona"]

    def test_resolved_team_uses_canonical_id(self, gold_rows):
        """xref-matched rows (both sources) → team_id_source='fbref_canonical'."""
        resolved = {r["team_name_raw"]: r["team_id"] for r in gold_rows
                    if r["team_id_source"] == "fbref_canonical"}
        assert resolved == {
            "Manchester City": "manchester_city",
            "Liverpool": "liverpool",
            "Real Madrid": "real_madrid",
        }

    def test_sofascore_orphan_uses_ss_prefix(self, gold_rows):
        """ss orphans (confidence='orphan' row, and no-row) → 'ss_<slug>'."""
        orphans = {r["team_name_raw"]: r for r in gold_rows
                   if r["team_id_source"] == "sofascore_orphan"}
        assert set(orphans) == {"Arsenal", "Burnley"}
        assert orphans["Arsenal"]["team_id"] == "ss_arsenal"
        assert orphans["Burnley"]["team_id"] == "ss_burnley"

    def test_fotmob_orphan_uses_fm_prefix(self, gold_rows):
        """fm orphan (no fotmob xref row) → 'fm_<slug>'."""
        orphans = {r["team_name_raw"]: r for r in gold_rows
                   if r["team_id_source"] == "fotmob_orphan"}
        assert set(orphans) == {"Barcelona"}
        assert orphans["Barcelona"]["team_id"] == "fm_barcelona"

    def test_standings_source_enum(self, gold_rows):
        """Every row carries a valid standings_source provenance."""
        assert {r["standings_source"] for r in gold_rows} == {"sofascore", "fotmob"}

    def test_points_per_game_exact(self, gold_rows):
        """points_per_game = CAST(points AS DOUBLE) / NULLIF(played, 0)."""
        mci = _by_name(gold_rows)["Manchester City"]
        assert mci["points"] == 31 and mci["played"] == 12
        assert mci["points_per_game"] == pytest.approx(31 / 12)

    def test_match_columns_carry_through(self, gold_rows):
        mci = _by_name(gold_rows)["Manchester City"]
        assert (mci["wins"], mci["draws"], mci["losses"]) == (10, 1, 1)
        assert (mci["goals_for"], mci["goals_against"], mci["goal_diff"]) == (35, 11, 24)

    def test_season_emitted_as_slug(self, gold_rows):
        """#404: season slug emitted as-is (varchar), not normalised to bigint."""
        assert {r["season"] for r in gold_rows} == {"2425"}
