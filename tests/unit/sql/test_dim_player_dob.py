"""
Executable unit test for Gold ``dim_player`` dob logic (issue #584).

``dim_player.sql.j2`` resolves ``dob`` via
``COALESCE(TRY_CAST(fm.date_of_birth), ss.date_of_birth, tm.dob,
          TRY(CAST(date_parse(sf.dob, '%b %e, %Y') AS DATE)))``.

SoFIFA carries dob as a ``"Mon D, YYYY"`` string (e.g. ``'Nov 9, 1982'``).
Before #584 the SoFIFA branch was a plain ``TRY_CAST(sf.dob AS DATE)`` which can
NOT parse that format → always NULL, so SoFIFA never enriched dob. The pilot
(FIFA 18) surfaced this: height filled but dob did not. This proves the emitted
dob VALUE:

  * historical player, dob ONLY in SoFIFA ('Nov 9, 1982')  -> 1982-11-09
  * player with a higher-priority source (Transfermarkt)   -> source wins
    (SoFIFA sits LAST in the COALESCE)
  * player with no dob in any source                        -> NULL

Strategy mirrors ``test_dim_player_nationality``: substitute the two VALUES
placeholders with small hermetic maps, transpile Trino → DuckDB via sqlglot,
materialise fixture silver tables and execute. Skips cleanly if sqlglot cannot
translate a Trino construct on the installed engine — the authoritative syntax
check is EXPLAIN (TYPE VALIDATE) on live Trino.
"""

from __future__ import annotations

import datetime as _dt
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
        ('fb_h', 'fbref', 'h', 'high', '1718'),   -- historical, dob only in SoFIFA
        ('fb_p', 'fbref', 'p', 'high', '2425'),   -- has Transfermarkt dob (wins)
        ('fb_n', 'fbref', 'n', 'high', '2425')    -- no dob anywhere
    """)

    con.execute("""
        CREATE TABLE silver.fbref_player_season_profile (
            player_id VARCHAR, season BIGINT, player VARCHAR, nation VARCHAR,
            pos VARCHAR, minutes INTEGER, squad VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_player_season_profile VALUES
        ('h', 1718, 'Historical Hugh', 'eng ENG', 'DF', 900, 'Club H'),
        ('p', 2425, 'Priority Pete',   'eng ENG', 'MF', 900, 'Club P'),
        ('n', 2425, 'Nobody Ned',      'eng ENG', 'FW', 900, 'Club N')
    """)

    # Enrichment sources — fotmob / sofascore empty (no dob from them here).
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
    # Transfermarkt carries a real DATE dob for player P (must win over SoFIFA).
    con.execute("""
        CREATE TABLE silver.transfermarkt_players (
            canonical_id VARCHAR, season BIGINT, name VARCHAR, dob DATE,
            nationality VARCHAR, height_cm INTEGER, foot VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.transfermarkt_players VALUES
        ('fb_p', 2425, 'Priority Pete', DATE '2000-01-01', 'England', 180, 'right')
    """)
    # SoFIFA dob in the live "Mon D, YYYY" string format. P also has a SoFIFA dob
    # to prove the COALESCE order (Transfermarkt must still win).
    con.execute("""
        CREATE TABLE silver.sofifa_player_profile (
            canonical_id VARCHAR, season BIGINT, dob VARCHAR,
            nationality VARCHAR, height_cm INTEGER, position VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.sofifa_player_profile VALUES
        ('fb_h', 1718, 'Nov 9, 1982', 'England', 185, 'CB'),
        ('fb_p', 2425, 'Mar 3, 1995', 'England', 180, 'CM')
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
class TestDimPlayerDob:
    def test_all_three_players_present(self, gold_rows):
        assert set(gold_rows) == {"fb_h", "fb_p", "fb_n"}

    def test_sofifa_dob_fallback_parsed(self, gold_rows):
        """#584: dob present ONLY in SoFIFA ('Nov 9, 1982') is date_parsed to a
        real DATE — the regression a plain CAST AS DATE silently dropped."""
        assert gold_rows["fb_h"]["dob"] == _dt.date(1982, 11, 9)

    def test_higher_priority_source_wins_over_sofifa(self, gold_rows):
        """Transfermarkt dob outranks SoFIFA (SoFIFA is LAST in the COALESCE)."""
        assert gold_rows["fb_p"]["dob"] == _dt.date(2000, 1, 1)

    def test_no_dob_anywhere_is_null(self, gold_rows):
        """No source carries dob → NULL (graceful, never a parse error)."""
        assert gold_rows["fb_n"]["dob"] is None
