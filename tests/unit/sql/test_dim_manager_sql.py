"""
Unit tests for Gold ``dim_manager`` SQL logic — star-schema grain (#425).

dim_manager is a plain per-manager dictionary now: spine =
silver.xref_manager GROUP BY canonical_id, manager_name prefers the FBref
display_name, nationality/dob are enriched from FotMob by coachId (issue #434).
The SCD-2 stint logic that used to live here moves to fct_manager_stint
(issue #429) — its tests go with it (`git log -p tests/unit/sql/test_dim_manager_sql.py`).

Strategy: Trino → DuckDB transpile via sqlglot, fixture rows in an
in-memory schema, execute, assert. Skips cleanly if sqlglot cannot
translate a Trino-specific construct.
"""

from __future__ import annotations

from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_manager.sql"


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE silver.xref_manager (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR,
            confidence VARCHAR, match_score DOUBLE
        )
        """
    )
    # One manager seen by BOTH sources across two seasons (4 xref rows ->
    # 1 dim row); one FotMob-only manager (name falls back to FotMob).
    # FotMob source_id = stable coachId (issue #144) — the bridge key to
    # fotmob_manager_profile.player_id (#434).
    con.execute(
        """
        INSERT INTO silver.xref_manager VALUES
        ('mikel_arteta', 'fbref',  'Mikel Arteta', 'Mikel Arteta',
         'ENG-Premier League', '2425', 'name_normalize', NULL),
        ('mikel_arteta', 'fbref',  'Mikel Arteta', 'Mikel Arteta',
         'ENG-Premier League', '2526', 'name_normalize', NULL),
        ('mikel_arteta', 'fotmob', '1001',         'M. Arteta',
         'ENG-Premier League', '2526', 'name_normalize', NULL),
        ('fm_mgr_unai',  'fotmob', '1002',         'U. Emery',
         'ENG-Premier League', '2526', 'orphan', NULL),
        -- FBref-only historical manager (no FotMob) — enriched by Transfermarkt.
        ('sam_allardyce','fbref',  'Sam Allardyce','Sam Allardyce',
         'ENG-Premier League', '2122', 'name_normalize', NULL),
        -- TM bridge rows (xref-improvements): dim_manager reaches TM
        -- dob/nationality through xref_manager (source_id = coach_id), not the
        -- old direct name-join. Allardyce glued via the name_initial tier —
        -- the case the old join could not see.
        ('mikel_arteta', 'transfermarkt', '5672', 'Mikel Arteta',
         'ENG-Premier League', '2526', 'name_normalize', NULL),
        ('sam_allardyce','transfermarkt', '9999', 'Allardyce, Sam',
         'ENG-Premier League', '2122', 'name_initial', NULL),
        -- TM orphan: must NOT mint a manager_id row and must NOT enrich.
        ('unknown_coach','transfermarkt', '7777', 'Unknown Coach',
         'ENG-Premier League', '2526', 'orphan', NULL)
        """
    )
    # issue #434: FotMob coach profile enriches nationality/dob by coachId.
    # Arteta (coachId 1001) has a profile; Emery (1002) does NOT -> stays NULL.
    con.execute(
        """
        CREATE TABLE silver.fotmob_manager_profile (
            player_id VARCHAR, name VARCHAR, date_of_birth VARCHAR,
            nationality VARCHAR, _bronze_ingested_at TIMESTAMP,
            league VARCHAR, season VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.fotmob_manager_profile VALUES
        ('1001', 'Mikel Arteta', '1982-03-26', 'Spain',
         NULL, 'ENG-Premier League', '2526')
        """
    )
    # issue #434: Transfermarkt coach enrichment, keyed on canonical_id.
    # Arteta also appears here (different dob) to prove FotMob WINS the COALESCE;
    # Allardyce is TM-only (no FotMob) -> TM fills his nationality/dob.
    con.execute(
        """
        CREATE TABLE silver.transfermarkt_coaches (
            coach_id VARCHAR, canonical_id VARCHAR, name VARCHAR, role VARCHAR,
            dob DATE, nationality VARCHAR, current_club_id VARCHAR,
            current_club_name VARCHAR, _bronze_ingested_at TIMESTAMP,
            league VARCHAR, season VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.transfermarkt_coaches VALUES
        ('5672', 'mikel_arteta', 'Mikel Arteta', 'Manager',
         DATE '1972-01-01', 'TM-Spain', '11', 'Arsenal', NULL,
         'ENG-Premier League', '2526'),
        -- NB: the DEPRECATED local canonical_id is the mis-normalised
        -- surname-first slug — the old direct join on it would MISS this row;
        -- only the xref bridge (coach_id 9999 → sam_allardyce) reaches it.
        ('9999', 'allardyce_sam', 'Allardyce, Sam', 'Manager',
         DATE '1954-10-19', 'England', '99', 'West Brom', NULL,
         'ENG-Premier League', '2122')
        """
    )


@pytest.fixture(scope="module")
def gold_rows():
    try:
        sql = _translate(SQL_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover — sqlglot version drift
        pytest.skip(f"sqlglot could not translate dim_manager.sql: {exc}")

    con = duckdb.connect(":memory:")
    _bootstrap(con)
    try:
        cur = con.execute(sql)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DuckDB could not execute translated SQL: {exc}")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


pytestmark = pytest.mark.unit


class TestDimManagerDictionary:
    def test_one_row_per_canonical_manager(self, gold_rows):
        """8 xref rows across sources/seasons collapse to 3 dim rows
        (mikel_arteta, fm_mgr_unai, sam_allardyce); the TM orphan is excluded
        from the spine."""
        assert len(gold_rows) == 3, gold_rows
        ids = [r["manager_id"] for r in gold_rows]
        assert len(ids) == len(set(ids)), f"duplicate manager_id: {ids}"

    def test_tm_orphan_does_not_mint_manager(self, gold_rows):
        """An un-glued TM coach (confidence='orphan') is almost always a
        mis-normalised duplicate of an existing canonical — it must not
        become its own manager_id row."""
        assert not [r for r in gold_rows if r["manager_id"] == "unknown_coach"]

    def test_fbref_display_name_preferred(self, gold_rows):
        arteta = [r for r in gold_rows if r["manager_id"] == "mikel_arteta"]
        assert len(arteta) == 1
        assert arteta[0]["manager_name"] == "Mikel Arteta", (
            "FBref display_name must win over FotMob's 'M. Arteta'"
        )

    def test_fotmob_only_manager_falls_back(self, gold_rows):
        emery = [r for r in gold_rows if r["manager_id"] == "fm_mgr_unai"]
        assert len(emery) == 1
        assert emery[0]["manager_name"] == "U. Emery"

    def test_nationality_dob_enriched_from_fotmob(self, gold_rows):
        """issue #434: matched coachId pulls nationality/dob from FotMob."""
        import datetime

        arteta = [r for r in gold_rows if r["manager_id"] == "mikel_arteta"][0]
        assert arteta["nationality"] == "Spain", arteta
        assert arteta["dob"] == datetime.date(1982, 3, 26), arteta

    def test_unmatched_manager_attrs_null(self, gold_rows):
        """No FotMob/TM profile for this coachId -> nationality/dob stay NULL."""
        emery = [r for r in gold_rows if r["manager_id"] == "fm_mgr_unai"][0]
        assert emery["nationality"] is None, emery
        assert emery["dob"] is None, emery

    def test_fotmob_wins_over_transfermarkt(self, gold_rows):
        """issue #434: COALESCE priority FotMob > TM — Arteta keeps FotMob's
        dob/nationality even though TM carries a different dob."""
        import datetime

        arteta = [r for r in gold_rows if r["manager_id"] == "mikel_arteta"][0]
        assert arteta["nationality"] == "Spain", arteta  # not 'TM-Spain'
        assert arteta["dob"] == datetime.date(1982, 3, 26), arteta  # not 1972

    def test_transfermarkt_fills_when_fotmob_absent(self, gold_rows):
        """issue #434 + xref-improvements: a FBref-only historical manager
        gets nationality/dob from Transfermarkt through the xref bridge
        (coach_id → canonical_id) — here via a name_initial-tier link the old
        direct canonical_id join could not see (xref carries the surname-first
        'Allardyce, Sam' spelling)."""
        import datetime

        sam = [r for r in gold_rows if r["manager_id"] == "sam_allardyce"][0]
        assert sam["nationality"] == "England", sam
        assert sam["dob"] == datetime.date(1954, 10, 19), sam
