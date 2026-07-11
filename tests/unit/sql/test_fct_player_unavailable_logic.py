"""
Pure-SQL logic tests for E5 (Player Availability) — Silver + Gold + Feat.

Approach
--------
We transpile the production Trino SQL files to DuckDB via ``sqlglot`` and run
them against in-memory DuckDB tables seeded with hand-crafted fixtures. This
keeps the tests:

* Hermetic — no Trino / HDFS / Iceberg dependencies.
* Fast (<1s each).
* Covering the *actual* SQL logic, not a re-implementation in Python.

Files under test:

* ``dags/sql/silver/whoscored_player_unavailable.sql``
* ``dags/sql/gold/fct_player_unavailable.sql``

Caveats / known transpile mismatches
------------------------------------
None observed (May 2026). ``REGEXP_REPLACE(..., '_')`` is correctly emitted
with the ``'g'`` global flag for DuckDB. Window-function syntax, CTEs and
``COALESCE`` translate one-to-one. If a future SQL change breaks the transpile
the offending test should be marked ``@pytest.mark.skip`` with a note here.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pytest
import sqlglot

REPO_ROOT = Path(__file__).resolve().parents[3]
SILVER_SQL = REPO_ROOT / "dags" / "sql" / "silver" / "whoscored_player_unavailable.sql"
GOLD_SQL = REPO_ROOT / "dags" / "sql" / "gold" / "fct_player_unavailable.sql"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _transpile(path: Path) -> str:
    """Transpile a Trino SQL file to DuckDB-compatible SQL."""
    sql = path.read_text()
    out = sqlglot.transpile(sql, read="trino", write="duckdb")
    if not out:
        raise RuntimeError(f"sqlglot returned no statements for {path}")
    transpiled = out[0]
    # Trino's NORMALIZE(x, NFD) (the diacritic-strip slug idiom, issue #215) has
    # no DuckDB equivalent — DuckDB only ships nfc_normalize. Rewrite it to
    # DuckDB's strip_accents(x), which removes the combining marks directly; the
    # following `\p{Mn}+` REGEXP_REPLACE then matches nothing (harmless no-op).
    transpiled = re.sub(r"NORMALIZE\((.*?),\s*NFD\)", r"strip_accents(\1)", transpiled)
    # Trino's printf-style FORMAT('%02d%02d', ...) (the season year-start↔slug
    # bridge, #388) is left as DuckDB FORMAT() by sqlglot, but DuckDB's FORMAT
    # uses Python `{}` placeholders and chokes on `%02d`. DuckDB's printf() is
    # the printf-spec equivalent — rewrite the call so the bridge evaluates.
    transpiled = re.sub(r"\bFORMAT\(", "printf(", transpiled)
    # Strip any trailing semicolons; we will wrap in SELECT
    return transpiled.rstrip().rstrip(";")


def _make_con() -> duckdb.DuckDBPyConnection:
    """Fresh in-memory DuckDB with iceberg.bronze / iceberg.silver / iceberg.gold schemas."""
    con = duckdb.connect()
    # DuckDB needs an attached catalog called 'iceberg' to mirror Trino's
    # three-part identifiers.
    con.execute("ATTACH ':memory:' AS iceberg")
    for sch in ("bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{sch}")
    return con


def _exec_select(con: duckdb.DuckDBPyConnection, select_sql: str) -> List[Dict[str, Any]]:
    """Run a SELECT and return list-of-dicts."""
    cur = con.execute(select_sql)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

def _create_bronze_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Mirror columns referenced by silver SQL."""
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_missing_players (
            league VARCHAR,
            season VARCHAR,
            game VARCHAR,
            team VARCHAR,
            player_id VARCHAR,
            player VARCHAR,
            reason VARCHAR,
            status VARCHAR,
            _ingested_at TIMESTAMP
        )
        """
    )
    # Production exposes only the batch referenced by the latest successful
    # preview manifest. These logic tests seed one already-current batch, so a
    # passthrough view keeps the fixture focused on Silver filtering semantics.
    con.execute(
        """
        CREATE VIEW iceberg.bronze.whoscored_missing_players_current AS
        SELECT * FROM iceberg.bronze.whoscored_missing_players
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_schedule (
            league VARCHAR,
            season VARCHAR,
            game VARCHAR,
            date DATE,
            home_team VARCHAR,
            away_team VARCHAR,
            _ingested_at TIMESTAMP
        )
        """
    )


def _create_silver_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE iceberg.silver.whoscored_player_unavailable (
            match_id VARCHAR,
            match_date DATE,
            league VARCHAR,
            season VARCHAR,
            team_name VARCHAR,
            ws_player_id VARCHAR,
            player_name VARCHAR,
            reason VARCHAR,
            status VARCHAR,
            _bronze_ingested_at TIMESTAMP
        )
        """
    )


def _create_gold_dims(con: duckdb.DuckDBPyConnection) -> None:
    # #425 star grains: dim_match passport (match_date, no name columns),
    # dim_team one row per club, dim_player one row per player.
    con.execute(
        """
        CREATE TABLE iceberg.gold.dim_match (
            match_id VARCHAR,
            match_date DATE,
            home_team_id VARCHAR,
            away_team_id VARCHAR,
            league VARCHAR,
            season VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.gold.dim_team (
            team_id VARCHAR,
            team_name VARCHAR,
            country VARCHAR,
            short_name VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.gold.dim_player (
            player_id VARCHAR,
            player_name VARCHAR
        )
        """
    )


# ---------------------------------------------------------------------------
# T6.1 — Silver SQL tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSilverWhoscoredPlayerUnavailable:
    """Pure-SQL tests for ``dags/sql/silver/whoscored_player_unavailable.sql``."""

    def _run_silver(self, con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
        select_sql = _transpile(SILVER_SQL)
        return _exec_select(con, select_sql)

    def test_silver_dedup_keeps_latest_snapshot(self):
        """Two snapshots of the same (game, team, player_id) -> only latest survives."""
        con = _make_con()
        _create_bronze_tables(con)

        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_missing_players VALUES
            ('ENG-Premier League','2024','g1','Arsenal','p1','Saka',
                'injured','Out', TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-Premier League','2024','g1','Arsenal','p1','Saka',
                'injured','Out', TIMESTAMP '2025-01-02 10:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_schedule VALUES
            ('ENG-Premier League','2024','g1', DATE '2025-01-03',
                'Arsenal','Liverpool', TIMESTAMP '2024-12-30 00:00:00')
            """
        )

        rows = self._run_silver(con)
        assert len(rows) == 1, f"expected 1 deduped row, got {len(rows)}"
        # latest snapshot timestamp must win
        assert rows[0]["_bronze_ingested_at"] == datetime(2025, 1, 2, 10, 0, 0)

    def test_silver_filters_intl_duty(self):
        """``reason='International duty'`` rows are dropped (D3)."""
        con = _make_con()
        _create_bronze_tables(con)

        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_missing_players VALUES
            ('ENG-PL','2024','g1','Arsenal','p1','A','Injury','Out',
                TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-PL','2024','g1','Arsenal','p2','B','International duty','Out',
                TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-PL','2024','g1','Arsenal','p3','C','Suspension','Out',
                TIMESTAMP '2025-01-01 10:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_schedule VALUES
            ('ENG-PL','2024','g1', DATE '2025-01-02','Arsenal','X',
                TIMESTAMP '2025-01-01 00:00:00')
            """
        )

        rows = self._run_silver(con)
        reasons = sorted(r["reason"] for r in rows)
        assert reasons == ["Injury", "Suspension"]
        assert "International duty" not in reasons

    def test_silver_filters_non_out(self):
        """Only ``LOWER(status)='out'`` rows survive (D5, #393).

        Bronze "missing players" uses 'Out' (confirmed absence) and 'Doubtful'
        (uncertain). Keep only 'Out', case-insensitively; drop 'Doubtful' and
        any other tier.
        """
        con = _make_con()
        _create_bronze_tables(con)

        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_missing_players VALUES
            ('ENG-PL','2024','g1','Arsenal','p1','A','injured','Out',
                TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-PL','2024','g1','Arsenal','p2','B','injured','out',
                TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-PL','2024','g1','Arsenal','p3','C','injured','Doubtful',
                TIMESTAMP '2025-01-01 10:00:00'),
            ('ENG-PL','2024','g1','Arsenal','p4','D','injured','rumor',
                TIMESTAMP '2025-01-01 10:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_schedule VALUES
            ('ENG-PL','2024','g1', DATE '2025-01-02','Arsenal','X',
                TIMESTAMP '2025-01-01 00:00:00')
            """
        )

        rows = self._run_silver(con)
        # case-insensitive: 'Out' + 'out' both pass; 'Doubtful' / 'rumor' rejected
        names = sorted(r["player_name"] for r in rows)
        assert names == ["A", "B"], f"expected only status=out (case-insensitive); got {names}"

    def test_silver_match_date_enrichment(self):
        """Silver row carries ``match_date`` from bronze schedule join."""
        con = _make_con()
        _create_bronze_tables(con)

        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_missing_players VALUES
            ('ENG-PL','2024','g42','Chelsea','p9','Palmer','injured','Out',
                TIMESTAMP '2025-03-01 10:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO iceberg.bronze.whoscored_schedule VALUES
            ('ENG-PL','2024','g42', DATE '2025-03-15','Chelsea','Arsenal',
                TIMESTAMP '2025-02-15 00:00:00')
            """
        )

        rows = self._run_silver(con)
        assert len(rows) == 1
        assert rows[0]["match_date"] == date(2025, 3, 15)
        assert rows[0]["match_id"] == "g42"
        # season passes through as varchar slug (#388), no longer integer-cast
        assert rows[0]["season"] == "2024"


# ---------------------------------------------------------------------------
# T6.1 — Gold SQL tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestGoldFctPlayerUnavailable:
    """Pure-SQL tests for ``dags/sql/gold/fct_player_unavailable.sql``."""

    def _run_gold(self, con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
        select_sql = _transpile(GOLD_SQL)
        return _exec_select(con, select_sql)

    def _seed_silver_one_row(self, con, *, team_name="Arsenal",
                             player_name="Bukayo Saka", ws_player_id="ws_42",
                             match_id="g100", match_date=date(2025, 1, 15)):
        con.execute(
            f"""
            INSERT INTO iceberg.silver.whoscored_player_unavailable VALUES
            ('{match_id}', DATE '{match_date.isoformat()}', 'ENG-PL', '2425',
             '{team_name}', '{ws_player_id}', '{player_name}',
             'Injury', 'confirmed', TIMESTAMP '2025-01-10 00:00:00')
            """
        )

    def test_gold_orphan_player_id_fallback(self):
        """No dim_player match -> player_id = 'ws_<ws_player_id>'."""
        con = _make_con()
        _create_silver_table(con)
        _create_gold_dims(con)

        # silver row
        self._seed_silver_one_row(
            con,
            team_name="Arsenal",
            player_name="Unknown Player",
            ws_player_id="ws_999",
        )

        # canonical slug for "Arsenal" is "arsenal"
        con.execute(
            """
            INSERT INTO iceberg.gold.dim_match VALUES
            ('FBRMATCHX', DATE '2025-01-15','arsenal','liverpool',
             'ENG-PL', '2425')
            """
        )
        con.execute(
            "INSERT INTO iceberg.gold.dim_team VALUES "
            "('arsenal','Arsenal','England','Arsenal')"
        )
        # dim_player has DIFFERENT name -> no match
        con.execute(
            "INSERT INTO iceberg.gold.dim_player VALUES "
            "('fbref_pid_001','Mohamed Salah')"
        )

        rows = self._run_gold(con)
        assert len(rows) == 1
        assert rows[0]["player_id"] == "ws_ws_999"

    def test_gold_match_id_bridge_via_dim_match(self):
        """Silver (match_date, team_slug) -> dim_match.match_id propagates to Gold."""
        con = _make_con()
        _create_silver_table(con)
        _create_gold_dims(con)

        self._seed_silver_one_row(
            con, team_name="Arsenal", match_id="ws_g_xyz",
            match_date=date(2025, 1, 15),
        )

        # team_slug for 'Arsenal' = 'arsenal'; dim_match has matching home_team_id
        con.execute(
            """
            INSERT INTO iceberg.gold.dim_match VALUES
            ('FBR_MATCH_ABC123', DATE '2025-01-15','arsenal','liverpool',
             'ENG-PL', '2425')
            """
        )
        con.execute(
            "INSERT INTO iceberg.gold.dim_team VALUES "
            "('arsenal','Arsenal','England','Arsenal')"
        )

        rows = self._run_gold(con)
        assert len(rows) == 1
        assert rows[0]["match_id"] == "FBR_MATCH_ABC123"
        # team_id resolved via dim_team
        assert rows[0]["team_id"] == "arsenal"

    def test_gold_team_slug_strips_diacritics(self):
        """Accented team_name slugs to the same ASCII slug as dim_match (issue #215).

        A silver team_name carrying diacritics ("Bayern München") must produce
        ``bayern_munchen`` — matching the accent-free dim_match.home_team_id —
        rather than the bare-slug ``bayern_m_nchen`` that would miss the bridge.
        """
        con = _make_con()
        _create_silver_table(con)
        _create_gold_dims(con)

        self._seed_silver_one_row(
            con, team_name="Bayern München", match_id="ws_g_de",
            match_date=date(2025, 1, 15),
        )

        # dim_match stores the accent-free slug 'bayern_munchen'
        con.execute(
            """
            INSERT INTO iceberg.gold.dim_match VALUES
            ('FBR_BAYERN_1', DATE '2025-01-15','bayern_munchen','dortmund',
             'ENG-PL', '2425')
            """
        )
        con.execute(
            "INSERT INTO iceberg.gold.dim_team VALUES "
            "('bayern_munchen','Bayern München','Germany','Bayern')"
        )

        rows = self._run_gold(con)
        assert len(rows) == 1, (
            "accented team_name must slug to the accent-free dim_match slug and "
            "bridge to exactly one match"
        )
        assert rows[0]["match_id"] == "FBR_BAYERN_1"
        assert rows[0]["team_id"] == "bayern_munchen"

    def test_gold_orphan_match_dropped(self):
        """No dim_match bridge -> row is filtered out (WHERE fbref_match_id IS NOT NULL)."""
        con = _make_con()
        _create_silver_table(con)
        _create_gold_dims(con)

        self._seed_silver_one_row(
            con, team_name="Arsenal", match_date=date(2025, 1, 15),
        )

        # dim_match has match on a *different date* — bridge fails
        con.execute(
            """
            INSERT INTO iceberg.gold.dim_match VALUES
            ('FBR_X', DATE '2025-02-01','arsenal','liverpool',
             'ENG-PL', '2425')
            """
        )
        con.execute(
            "INSERT INTO iceberg.gold.dim_team VALUES "
            "('arsenal','Arsenal','England','Arsenal')"
        )

        rows = self._run_gold(con)
        assert rows == [], "orphan-match silver row must be filtered out of Gold"

    def test_no_legacy_entity_xref_in_executable_sql(self):
        """#703: fct_player_unavailable must NOT reference gold.entity_xref in
        executable SQL. Header/inline comments may legitimately mention the
        historical breadcrumb (this file never JOINed entity_xref) — strip
        ``-- ...`` lines first, then assert no executable reference survives."""
        text = GOLD_SQL.read_text(encoding="utf-8")
        non_comment = "\n".join(
            line for line in text.splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "entity_xref" not in non_comment, (
            "fct_player_unavailable.sql references entity_xref in executable "
            "SQL — only comment breadcrumbs are allowed (#703)"
        )
