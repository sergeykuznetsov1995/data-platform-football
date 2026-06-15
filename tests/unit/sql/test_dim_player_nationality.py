"""
Executable unit test for Gold ``dim_player`` nationality logic (issue #435).

``dim_player.sql.j2`` maps the FBref FIFA 3-letter code fallback to a full
country name via the ``country_map`` CTE (filled from country_codes.yaml at
render time through ``{{ country_map_values_sql }}``). This proves the actual
emitted nationality VALUE:

  * historical player (FBref-only, ``nation='eng ENG'``)  -> 'England'
  * unmapped code (``nation='xyz XYZ'``, absent from map)  -> 'XYZ' (raw)
  * current player (FotMob nationality present)            -> source wins

Strategy mirrors ``test_dim_venue_logic``: substitute the placeholder with a
small hermetic country map, transpile Trino → DuckDB via sqlglot, materialise
fixture silver tables and execute. Skips cleanly if sqlglot cannot translate a
Trino-specific construct (MAX_BY / REGEXP_EXTRACT) on the installed engine —
the authoritative syntax check is EXPLAIN (TYPE VALIDATE) on live Trino.

NB: fixtures carry the LIVE raw FBref format ``'eng ENG'`` (flag + code), not a
pre-extracted ``'ENG'`` — the column is passed through Silver verbatim and the
REGEXP_EXTRACT happens inside dim_player.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_player.sql.j2"

# Hermetic FIFA-code -> name map (independent of the shipped YAML so this logic
# test stays stable). 'XYZ' is deliberately absent → unmapped-fallback case.
_TEST_COUNTRY_MAP = """\
    ('ENG', 'England'),
    ('SCO', 'Scotland'),
    ('ARG', 'Argentina')"""

_PLACEHOLDER_RE = re.compile(
    r"^[ \t]*\{\{\s*country_map_values_sql\s*\}\}[ \t]*$", re.MULTILINE
)


def _render(sql_text: str) -> str:
    """Fill the standalone ``{{ country_map_values_sql }}`` placeholder."""
    return _PLACEHOLDER_RE.sub(lambda _: _TEST_COUNTRY_MAP, sql_text, count=1)


def _translate(sql_text: str) -> str:
    """Trino → DuckDB transpile + iceberg.<schema>.<tbl> → <schema>.<tbl>."""
    out = sqlglot.parse_one(sql_text, read="trino").sql(
        dialect="duckdb", comments=False
    )
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # --- FBref spine + per-season profile (the only source with history) ---
    con.execute("""
        CREATE TABLE silver.xref_player (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            confidence VARCHAR, season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.xref_player VALUES
        ('fb_aaa', 'fbref',  'aaa',    'high', '2324'),
        ('fb_bbb', 'fbref',  'bbb',    'high', '2324'),
        ('fb_ccc', 'fbref',  'ccc',    'high', '2425'),
        -- player C also has a FotMob xref row (current-APL enrichment)
        ('fb_ccc', 'fotmob', 'fm_ccc', 'high', '2425')
    """)

    con.execute("""
        CREATE TABLE silver.fbref_player_season_profile (
            player_id VARCHAR, season BIGINT, player VARCHAR, nation VARCHAR,
            pos VARCHAR, minutes INTEGER, squad VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_player_season_profile VALUES
        -- A: historical, ENG present in map → 'England'
        ('aaa', 2324, 'Danny Rose',    'eng ENG', 'DF', 900,  'Newcastle United'),
        -- B: historical, XYZ absent from map → raw code 'XYZ'
        ('bbb', 2324, 'Test Unmapped', 'xyz XYZ', 'MF', 800,  'Club B'),
        -- C: current, FBref says SCO; FotMob (below) overrides with England
        ('ccc', 2425, 'Kieran Tierney','sco SCO', 'DF', 1000, 'Arsenal')
    """)

    # --- Enrichment sources (mostly empty; FotMob carries player C) ---
    con.execute("""
        CREATE TABLE silver.fotmob_player_profile (
            player_id VARCHAR, season BIGINT, player_name VARCHAR,
            date_of_birth VARCHAR, nationality VARCHAR, height_cm INTEGER,
            foot VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fotmob_player_profile VALUES
        -- Deliberately 'England' (≠ map's 'Scotland' for SCO) to prove the
        -- source wins over the code map in the COALESCE order.
        ('fm_ccc', 2425, 'Kieran Tierney', '1997-06-05', 'England', 180, 'left')
    """)

    con.execute("""
        CREATE TABLE silver.sofascore_player_profile (
            canonical_id VARCHAR, season BIGINT, player_name VARCHAR,
            date_of_birth DATE, nationality VARCHAR, height_cm INTEGER,
            preferred_foot VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.transfermarkt_players (
            canonical_id VARCHAR, season BIGINT, name VARCHAR, dob DATE,
            nationality VARCHAR, height_cm INTEGER, foot VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.sofifa_player_profile (
            canonical_id VARCHAR, season BIGINT, dob VARCHAR,
            nationality VARCHAR, height_cm INTEGER, position VARCHAR
        )
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

    return [dict(zip(col_names, r)) for r in rows]


def _nat(rows, player_id):
    match = [r for r in rows if r["player_id"] == player_id]
    assert len(match) == 1, f"expected 1 row for {player_id}, got {len(match)}"
    return match[0]["nationality"]


@pytest.mark.unit
class TestDimPlayerNationality:
    def test_row_count(self, gold_rows):
        """One row per FBref-spine player — the country_map JOIN must NOT
        fan out the spine (it is unique on fifa_code)."""
        assert len(gold_rows) == 3, (
            f"expected 3 players, got {len(gold_rows)}: "
            f"{[(r['player_id'], r['nationality']) for r in gold_rows]}"
        )

    def test_pk_unique(self, gold_rows):
        ids = [r["player_id"] for r in gold_rows]
        assert len(ids) == len(set(ids)), f"duplicate player_id: {ids}"

    def test_historical_code_mapped_to_full_name(self, gold_rows):
        """FBref 'eng ENG' (no enrichment source) → 'England'."""
        assert _nat(gold_rows, "fb_aaa") == "England"

    def test_unmapped_code_falls_back_to_raw(self, gold_rows):
        """A code absent from the map degrades to the raw 3-letter code
        (graceful — never NULL, no data loss)."""
        assert _nat(gold_rows, "fb_bbb") == "XYZ"

    def test_source_wins_over_code_map(self, gold_rows):
        """FotMob full name takes precedence over the FBref-code map
        (cm.country_name sits AFTER the sources in the COALESCE)."""
        assert _nat(gold_rows, "fb_ccc") == "England"

    def test_no_bare_three_letter_codes_for_mapped(self, gold_rows):
        """The mapped players read as full names, not codes."""
        for pid in ("fb_aaa", "fb_ccc"):
            nat = _nat(gold_rows, pid)
            assert nat != nat.upper() or len(nat) > 3, (
                f"{pid} nationality {nat!r} looks like an unmapped code"
            )
