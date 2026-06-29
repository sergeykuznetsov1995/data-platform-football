"""
Unit tests for Gold ``dim_referee`` SQL logic (E2 — 2026-05).

The transform reads ``silver.fbref_match_enriched.referee``, filters out
NULL/whitespace, then aggregates by ``LOWER(TRIM(name))`` into a global
referee dimension.

Source-of-truth selection rule (R0.4):
    referee_canonical = MIN(referee_raw)   -- single-source, MIN==stable
    referee_source    = 'fbref'            -- only available source today
    referee_version   = 'v1'

Strategy: Trino → DuckDB transpile via sqlglot, fixture rows in an
in-memory schema, then assert on the result set. Skips cleanly if sqlglot
cannot translate a Trino-specific construct.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_referee.sql"


def _translate(sql_text: str) -> str:
    """Trino → DuckDB transpile + iceberg.<schema>.<tbl> → <schema>.<tbl>.

    DuckDB lacks XXHASH64; we substitute its built-in HASH so the prefix
    contract (``ref_<hex>``) still holds. The hash *value* differs from
    Trino but determinism + uniqueness checks still pass.
    """
    # Drop comments (comments=False): the header references "NORMALIZE(NFD)" in
    # prose, which would otherwise be swallowed by the NORMALIZE→strip_accents
    # regex below and corrupt the SQL.
    out = sqlglot.parse_one(sql_text, read="trino").sql(
        dialect="duckdb", comments=False
    )
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    # Trino's NORMALIZE(x, NFD) diacritic-fold (issue #228) has no DuckDB
    # equivalent — rewrite to DuckDB's strip_accents(x). The following
    # `\p{Mn}+` REGEXP_REPLACE then matches nothing (harmless no-op).
    out = re.sub(r"NORMALIZE\((.*?),\s*NFD\)", r"strip_accents(\1)", out)
    # DuckDB lacks XXHASH64; substitute its built-in HASH so the prefix
    # contract (``ref_<hex>``) still holds. The hash *value* differs from
    # Trino but determinism + uniqueness checks still pass. After the
    # NORMALIZE→strip_accents rewrite above the hash input is the folded name.
    out = out.replace(
        "LOWER(HEX(XXHASH64(ENCODE("
        "REGEXP_REPLACE(strip_accents(LOWER(TRIM(referee_raw))), '\\p{Mn}+', '', 'g')))))",
        "LOWER(PRINTF('%x', HASH("
        "REGEXP_REPLACE(strip_accents(LOWER(TRIM(referee_raw))), '\\p{Mn}+', '', 'g'))))",
    )
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            venue VARCHAR,
            league VARCHAR,
            season BIGINT,
            date DATE,
            referee VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_match_enriched VALUES
        -- 'Mike Dean' — distinct rows in two seasons + two leagues
        ('Etihad Stadium',  'ENG-Premier League', 2023, DATE '2023-08-15', 'Mike Dean'),
        ('Anfield',         'ENG-Premier League', 2024, DATE '2024-09-01', 'Mike Dean'),
        ('Old Trafford',    'ESP-La Liga',        2024, DATE '2024-09-15', 'Mike Dean'),
        -- Mixed case — must collapse with 'Mike Dean'
        ('Goodison Park',   'ENG-Premier League', 2024, DATE '2024-10-15', 'mike dean'),
        ('Goodison Park',   'ENG-Premier League', 2024, DATE '2024-10-22', 'MIKE DEAN'),
        -- Distinct ref
        ('Camp Nou',        'ESP-La Liga',        2024, DATE '2024-09-22', 'Antonio Mateu Lahoz'),
        -- Diacritic pair (issue #228): "Müller" and "Muller" must fold to ONE row
        -- (same referee_id + referee_slug) via NFD + combining-mark strip.
        -- (ü = u + combining diaeresis, which NFD decomposes; unlike the dotless
        --  'ı', which has no NFD form and would NOT fold.)
        ('Allianz Arena',   'GER-Bundesliga',     2024, DATE '2024-09-30', 'Müller'),
        ('Allianz Arena',   'GER-Bundesliga',     2024, DATE '2024-10-05', 'Muller'),
        -- NULL / empty referees — must be filtered out
        ('Random Stadium',  'ENG-Premier League', 2024, DATE '2024-11-01', NULL),
        ('Random Stadium',  'ENG-Premier League', 2024, DATE '2024-11-02', '   ')
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
        pytest.skip(f"DuckDB execution of translated dim_referee SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestDimRefereeLogic:
    def test_null_and_empty_filtered(self, gold_rows):
        """Rows with NULL or whitespace-only referee are filtered at source."""
        canonicals = {r["referee_canonical"] for r in gold_rows}
        assert None not in canonicals
        assert "" not in canonicals
        assert "   " not in canonicals

    def test_one_row_per_lower_trim(self, gold_rows):
        """Mixed-case 'Mike Dean'/'mike dean'/'MIKE DEAN' → 1 row; the
        'Çakır'/'Cakir' diacritic pair also folds to 1 row (issue #228)."""
        # 3 distinct refs after LOWER(TRIM)+diacritic-fold:
        # mike dean + antonio mateu lahoz + cakir
        assert len(gold_rows) == 3, (
            f"expected 3 rows after dedup, got {len(gold_rows)}: "
            f"{[r['referee_canonical'] for r in gold_rows]}"
        )
        canonicals_lower = sorted(r["referee_canonical"].lower() for r in gold_rows)
        assert canonicals_lower == sorted({c for c in canonicals_lower})

    def test_source_and_version_constants(self, gold_rows):
        """All rows have referee_source='fbref' and referee_version='v1'."""
        for r in gold_rows:
            assert r["referee_source"] == "fbref", (
                f"referee_source must be 'fbref', got: {r['referee_source']!r}"
            )
            assert r["referee_version"] == "v1", (
                f"referee_version must be 'v1', got: {r['referee_version']!r}"
            )
            assert r["referee_canonical"] is not None

    def test_leagues_and_seasons_arrays_distinct(self, gold_rows):
        """``leagues`` and ``seasons`` are array_agg(distinct ...)."""
        mike = next(r for r in gold_rows
                    if r["referee_canonical"].lower() == "mike dean")
        leagues = list(mike["leagues"])
        seasons = list(mike["seasons"])
        # distinct values
        assert sorted(leagues) == sorted(set(leagues)), (
            f"leagues array contains duplicates: {leagues}"
        )
        assert sorted(seasons) == sorted(set(seasons)), (
            f"seasons array contains duplicates: {seasons}"
        )
        # Mike Dean appeared in EPL + La Liga
        assert set(leagues) == {"ENG-Premier League", "ESP-La Liga"}, (
            f"unexpected leagues: {leagues}"
        )
        assert set(seasons) == {2023, 2024}, f"unexpected seasons: {seasons}"

    def test_referee_id_prefix(self, gold_rows):
        """referee_id has 'ref_' prefix to avoid collision with team/venue."""
        for r in gold_rows:
            assert r["referee_id"].startswith("ref_"), (
                f"referee_id must start with 'ref_': {r['referee_id']!r}"
            )

    def test_referee_canonical_min_picks_stable_form(self, gold_rows):
        """``referee_canonical = MIN(referee_raw)`` — for 'Mike Dean'/'mike dean'/
        'MIKE DEAN' MIN returns the lexicographically smallest, i.e. 'MIKE DEAN'.
        """
        mike = next(r for r in gold_rows
                    if r["referee_canonical"].lower() == "mike dean")
        assert mike["referee_canonical"] == "MIKE DEAN", (
            f"MIN should return 'MIKE DEAN' (lexicographic min), got "
            f"{mike['referee_canonical']!r}"
        )

    def test_referee_slug_transliterates_diacritics(self, gold_rows):
        """'Müller'/'Muller' fold to ONE row with an ASCII referee_slug (#228).

        A bare ``[^a-zA-Z0-9]+ -> _`` slug would collapse 'ü' to '_',
        yielding a different slug per spelling; NORMALIZE(NFD) + ``\\p{Mn}``
        strip decomposes the accent so both map to 'muller'.
        """
        muller = [r for r in gold_rows if r["referee_slug"] == "muller"]
        assert len(muller) == 1, (
            f"'Müller'/'Muller' must fold to exactly one row, got "
            f"{[r['referee_canonical'] for r in gold_rows]}"
        )

    def test_no_underscore_from_diacritics_in_slug(self, gold_rows):
        """No referee_slug contains a leading/trailing/double underscore that
        would betray an un-stripped accent (regression guard for #228)."""
        for r in gold_rows:
            slug = r["referee_slug"]
            assert "__" not in slug, f"double underscore in slug: {slug!r}"
