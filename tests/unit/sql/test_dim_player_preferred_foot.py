"""
Executable unit test for Gold ``dim_player`` preferred_foot logic (issue #663).

``dim_player.sql.j2`` resolves ``preferred_foot`` via
``LOWER(COALESCE(tm.foot, fm.foot, ss.preferred_foot, sf.preferred_foot))``.

Before #663 SoFIFA was absent from the chain (its parser never extracted foot,
so silver had no column). #663 wires it through Bronze → Silver → Gold and adds
``sf.preferred_foot`` as the LAST fallback. This proves the emitted VALUE:

  * player with foot ONLY in SoFIFA ('Right')        -> 'right'
  * player with a higher-priority source (Transfermarkt 'Left') AND a SoFIFA
    foot ('Right')                                    -> 'left' (SoFIFA is LAST)
  * player with no foot in any source                 -> NULL

Strategy mirrors ``test_dim_player_dob`` / ``test_dim_player_nationality``:
substitute the two VALUES placeholders with small hermetic maps, transpile
Trino → DuckDB via sqlglot, materialise fixture silver tables and execute. Skips
cleanly if sqlglot cannot translate a Trino construct on the installed engine —
the authoritative syntax check is EXPLAIN (TYPE VALIDATE) on live Trino.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_player.sql.j2"

_TEST_COUNTRY_MAP = "('ENG', 'England')"
_TEST_NATIONALITY_ALIAS = "('USA', 'United States')"

_PLACEHOLDER_RE = re.compile(
    r"^[ \t]*\{\{\s*country_map_values_sql\s*\}\}[ \t]*$", re.MULTILINE
)
_ALIAS_PLACEHOLDER_RE = re.compile(
    r"^[ \t]*\{\{\s*nationality_alias_values_sql\s*\}\}[ \t]*$", re.MULTILINE
)


def _render(sql_text: str) -> str:
    out = _PLACEHOLDER_RE.sub(lambda _: _TEST_COUNTRY_MAP, sql_text, count=1)
    out = _ALIAS_PLACEHOLDER_RE.sub(
        lambda _: _TEST_NATIONALITY_ALIAS, out, count=1
    )
    return out


def _translate(sql_text: str) -> str:
    out = sqlglot.parse_one(sql_text, read="trino").sql(
        dialect="duckdb", comments=False
    )
    return out.replace("iceberg.silver.", "silver.").replace(
        "iceberg.bronze.", "bronze."
    )


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # FBref spine — three players, only fbref xref rows (no enrichment hop).
    con.execute("""
        CREATE TABLE silver.xref_player (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            confidence VARCHAR, season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.xref_player VALUES
        ('fb_s', 'fbref', 's', 'high', '2425'),   -- foot only in SoFIFA
        ('fb_t', 'fbref', 't', 'high', '2425'),   -- Transfermarkt foot (wins)
        ('fb_z', 'fbref', 'z', 'high', '2425')    -- no foot anywhere
    """)

    con.execute("""
        CREATE TABLE silver.fbref_player_season_profile (
            player_id VARCHAR, season BIGINT, player VARCHAR, nation VARCHAR,
            pos VARCHAR, minutes INTEGER, squad VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_player_season_profile VALUES
        ('s', 2425, 'Sofifa Sam',   'eng ENG', 'DF', 900, 'Club S'),
        ('t', 2425, 'Transfer Tom', 'eng ENG', 'MF', 900, 'Club T'),
        ('z', 2425, 'Zero Zoe',     'eng ENG', 'FW', 900, 'Club Z')
    """)

    # Enrichment sources — fotmob / sofascore empty (no foot from them here).
    con.execute("""
        CREATE TABLE silver.fotmob_player_profile (
            player_id VARCHAR, season BIGINT, player_name VARCHAR,
            date_of_birth VARCHAR, nationality VARCHAR, height_cm INTEGER,
            foot VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.sofascore_player_profile (
            canonical_id VARCHAR, season BIGINT, player_name VARCHAR,
            date_of_birth DATE, nationality VARCHAR, height_cm INTEGER,
            preferred_foot VARCHAR
        )
    """)
    # Transfermarkt carries a foot for player T (must win over SoFIFA).
    con.execute("""
        CREATE TABLE silver.transfermarkt_players (
            canonical_id VARCHAR, season BIGINT, name VARCHAR, dob DATE,
            nationality VARCHAR, height_cm INTEGER, foot VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.transfermarkt_players VALUES
        ('fb_t', 2425, 'Transfer Tom', NULL, 'England', 180, 'Left')
    """)
    # SoFIFA carries a foot for S (only source) and T (proves COALESCE order:
    # Transfermarkt must still win because SoFIFA is LAST).
    con.execute("""
        CREATE TABLE silver.sofifa_player_profile (
            canonical_id VARCHAR, season BIGINT, dob VARCHAR,
            nationality VARCHAR, height_cm INTEGER, position VARCHAR,
            preferred_foot VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.sofifa_player_profile VALUES
        ('fb_s', 2425, NULL, 'England', 180, 'CB', 'Right'),
        ('fb_t', 2425, NULL, 'England', 180, 'CM', 'Right')
    """)


@pytest.fixture(scope="module")
def gold_rows():
    sql_text = _render(SQL_PATH.read_text(encoding="utf-8"))
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
        pytest.skip(f"DuckDB execution of translated dim_player SQL failed: {e}")

    return {r[col_names.index("player_id")]: dict(zip(col_names, r)) for r in rows}


@pytest.mark.unit
class TestDimPlayerPreferredFoot:
    def test_all_three_players_present(self, gold_rows):
        assert set(gold_rows) == {"fb_s", "fb_t", "fb_z"}

    def test_sofifa_foot_fallback(self, gold_rows):
        """#663: foot present ONLY in SoFIFA ('Right') enriches the column,
        lower-cased like every other source."""
        assert gold_rows["fb_s"]["preferred_foot"] == "right"

    def test_higher_priority_source_wins_over_sofifa(self, gold_rows):
        """Transfermarkt foot outranks SoFIFA (SoFIFA is LAST in the COALESCE)."""
        assert gold_rows["fb_t"]["preferred_foot"] == "left"

    def test_no_foot_anywhere_is_null(self, gold_rows):
        """No source carries foot → NULL (graceful, never an error)."""
        assert gold_rows["fb_z"]["preferred_foot"] is None
