"""
Unit tests for Gold ``fct_team_elo`` SQL logic (issue #431).

Reads ``bronze.clubelo_ratings`` ∪ ``bronze.clubelo_ratings_historical``
directly (no silver ClubElo layer — mirrors fct_standings reading bronze),
deduplicates to the latest ``_ingested_at`` per (team_id, elo_date), LEFT JOINs
``silver.xref_team`` (source='clubelo') to resolve the ClubElo team name to a
canonical ``team_id``, and emits one row per (team_id, elo_date).

Resolution path:
    matched -> team_id = xref_team.canonical_id, team_id_source='fbref_canonical'
    orphan  -> team_id = 'ce_<slug>',            team_id_source='clubelo_orphan'

Two test layers (pattern: test_fct_standings_logic.py):
  * ``TestFctTeamEloStructure`` — regex sanity over the raw SQL pinning the
    bronze sources, xref_team JOIN, APL scope, orphan fallback and the absence
    of a season predicate (ClubElo has no season).
  * ``TestFctTeamEloLogic`` — Trino → DuckDB transpile via sqlglot; fixture
    rows in-memory; assert on the result set.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_team_elo.sql"


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
class TestFctTeamEloStructure:
    def test_reads_bronze_ratings_tables(self):
        """Source = bronze ratings (rank lives only here; team_history has none).
        No silver ClubElo projection exists — Gold reads bronze directly."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.clubelo_ratings" in sql
        assert "iceberg.bronze.clubelo_ratings_historical" in sql

    def test_does_not_read_clubelo_team_history(self):
        """clubelo_team_history carries neither rank nor league — must not be
        the source for this fact."""
        sql = _strip_comments(_read_sql())
        assert "clubelo_team_history" not in sql

    def test_join_uses_silver_xref_team(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql

    def test_join_filters_clubelo_source(self):
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'clubelo'", sql, re.IGNORECASE)

    def test_join_excludes_orphan_confidence(self):
        """xref contract: orphan rows carry a non-NULL 'ce_<slug>' canonical_id
        and must be excluded, else they'd be mislabeled 'fbref_canonical'."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE)

    def test_league_scope_follows_xref_team(self):
        """League scope is sourced from silver.xref_team's clubelo rows
        (rendered there from competitions.yaml in_scope) — no hardcoded
        league literal. Without the filter every out-of-scope club becomes
        a 'ce_' orphan and inflates the orphan rate."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"league\s+in\s*\(\s*select\s+league\s+from\s+in_scope_leagues\s*\)",
            sql, re.IGNORECASE,
        )
        assert not re.search(
            r"league\s*=\s*'ENG-Premier League'", sql, re.IGNORECASE,
        ), "hardcoded league literal must not return (former TODO(E8b))"

    def test_no_season_predicate(self):
        """ClubElo has NO season — the executable SQL must not reference a
        season column/predicate (xref clubelo rows have season IS NULL)."""
        sql = _strip_comments(_read_sql()).lower()
        assert "season" not in sql

    def test_orphan_fallback_ce_prefix_regex(self):
        sql = _read_sql()
        pattern = re.compile(
            r"'ce_'\s*\|\|\s*lower\s*\(\s*regexp_replace\s*\(", re.IGNORECASE,
        )
        assert pattern.search(sql)

    def test_team_id_source_branch_labels(self):
        sql = _read_sql()
        assert "'fbref_canonical'" in sql
        assert "'clubelo_orphan'" in sql


# ---------------------------------------------------------------------------
# DuckDB-transpile tests.
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

    # bronze.clubelo_ratings / _historical — only the cols the SQL selects.
    # rating_date is a date-only ISO string ('YYYY-MM-DD'), elo DOUBLE, rank INT.
    for tbl in ("clubelo_ratings", "clubelo_ratings_historical"):
        con.execute(f"""
            CREATE TABLE bronze.{tbl} (
                team VARCHAR,
                league VARCHAR,
                rating_date VARCHAR,
                elo DOUBLE,
                rank INTEGER,
                _ingested_at TIMESTAMP
            )
        """)

    # Daily fresh snapshots.
    #   * Manchester City 2026-06-01 — also present (older) in _historical:
    #     dedup must keep this fresher row (elo 2050, rank 1).
    #   * Liverpool on two dates → grain is team×date (2 output rows).
    #   * Arsenal — xref row exists but confidence='orphan' (excluded → 'ce_').
    #   * Burnley — no xref row at all (COALESCE 'ce_' fallback).
    #   * Leeds United — non-APL (ENG-Championship) → filtered out.
    con.execute("""
        INSERT INTO bronze.clubelo_ratings VALUES
        ('Manchester City', 'ENG-Premier League', '2026-06-01', 2050.0, 1,
         TIMESTAMP '2026-06-01 07:00:00'),
        ('Liverpool',       'ENG-Premier League', '2026-06-01', 1980.0, 2,
         TIMESTAMP '2026-06-01 07:00:00'),
        ('Liverpool',       'ENG-Premier League', '2026-06-08', 1990.0, 2,
         TIMESTAMP '2026-06-08 07:00:00'),
        ('Arsenal',         'ENG-Premier League', '2026-06-01', 1950.0, 3,
         TIMESTAMP '2026-06-01 07:00:00'),
        ('Burnley',         'ENG-Premier League', '2026-06-01', 1700.0, 18,
         TIMESTAMP '2026-06-01 07:00:00'),
        ('Leeds United',    'ENG-Championship',   '2026-06-01', 1650.0, 25,
         TIMESTAMP '2026-06-01 07:00:00')
    """)

    # Weekly historical — OLDER snapshot of the SAME (Manchester City, 2026-06-01)
    # with different elo/rank. Must lose to the fresher clubelo_ratings row.
    con.execute("""
        INSERT INTO bronze.clubelo_ratings_historical VALUES
        ('Manchester City', 'ENG-Premier League', '2026-06-01', 2000.0, 2,
         TIMESTAMP '2026-05-25 07:00:00')
    """)

    # silver.xref_team — minimal cols the JOIN touches. season is VARCHAR but
    # NULL for clubelo (no season). MCI/LIV resolved; ARS orphan-row (#460).
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
        ('clubelo', 'Manchester City', 'manchester_city', 'Manchester City',
         'ENG-Premier League', NULL, 'name_alias'),
        ('clubelo', 'Liverpool',       'liverpool',       'Liverpool',
         'ENG-Premier League', NULL, 'name_alias'),
        ('clubelo', 'Arsenal',         'ce_arsenal',      'Arsenal',
         'ENG-Premier League', NULL, 'orphan')
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
        pytest.skip(f"DuckDB execution of translated fct_team_elo SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestFctTeamEloLogic:
    def test_row_count_and_grain(self, gold_rows):
        """7 input rows (incl. 1 non-APL + 1 historical dup) → 5 output rows:
        MCI×1, LIV×2 (two dates), ARS×1, BUR×1. Leeds (non-APL) filtered."""
        assert len(gold_rows) == 5, (
            f"expected 5 rows, got {len(gold_rows)}: "
            f"{[(r['team_id'], str(r['elo_date'])) for r in gold_rows]}"
        )

    def test_pk_is_unique(self, gold_rows):
        """Design PK (team_id, elo_date) must be unique after dedup."""
        pks = [(r["team_id"], str(r["elo_date"])) for r in gold_rows]
        assert len(pks) == len(set(pks))

    def test_dedup_keeps_freshest_snapshot(self, gold_rows):
        """Manchester City 2026-06-01 exists in both bronze tables; the fresher
        clubelo_ratings row (elo 2050, rank 1) must win over the older
        historical row (elo 2000, rank 2)."""
        mci = [r for r in gold_rows if r["team_id"] == "manchester_city"]
        assert len(mci) == 1
        assert mci[0]["elo"] == pytest.approx(2050.0)
        assert mci[0]["rank"] == 1

    def test_resolved_team_uses_canonical_id(self, gold_rows):
        resolved = [r for r in gold_rows
                    if r["team_id_source"] == "fbref_canonical"]
        ids = {r["team_id"] for r in resolved}
        assert "manchester_city" in ids
        assert "liverpool" in ids

    def test_liverpool_has_two_dates(self, gold_rows):
        """Grain is team×date — Liverpool appears once per rating_date."""
        liv = [r for r in gold_rows if r["team_id"] == "liverpool"]
        assert len(liv) == 2
        assert {str(r["elo_date"]) for r in liv} == {"2026-06-01", "2026-06-08"}

    def test_orphan_team_uses_ce_prefix(self, gold_rows):
        """Both orphan shapes land on 'ce_<slug>' / 'clubelo_orphan':
          * Arsenal — xref row with confidence='orphan' (excluded by JOIN);
          * Burnley — no xref row (COALESCE fallback)."""
        orphans = {r["team_id"]: r for r in gold_rows
                   if r["team_id_source"] == "clubelo_orphan"}
        assert set(orphans) == {"ce_arsenal", "ce_burnley"}

    def test_non_apl_team_filtered_out(self, gold_rows):
        """Leeds United (ENG-Championship) must not appear — APL scope only."""
        names = {r["team_name_raw"] for r in gold_rows}
        assert "Leeds United" not in names

    def test_column_types(self, gold_rows):
        """elo is float, rank is int, elo_date is a date."""
        import datetime as _dt
        row = gold_rows[0]
        assert isinstance(row["elo"], float)
        assert isinstance(row["rank"], int)
        assert isinstance(row["elo_date"], _dt.date)
