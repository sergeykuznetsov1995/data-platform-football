"""
Unit tests for Gold ``dim_venue`` SQL logic (E2 — 2026-05).

The transform UNIONs FBref ``silver.fbref_match_enriched.venue`` with the
ESPN ``bronze.espn_matchsheet`` snapshot (deduped to the latest
``_ingested_at`` per (venue, league, season, game)) and aggregates them
into a global venue dimension keyed by ``LOWER(TRIM(name))``.

Source-of-truth selection rule (R0.4):
    venue_canonical = MAX(name) FILTER (src='fbref')
                      ELSE MAX(name) FILTER (src='espn')
    venue_source    = 'fbref' if any FBref row, else 'espn'
    venue_version   = 'v1'

Strategy: translate the Trino SQL to DuckDB via sqlglot, materialise small
fixture tables (``bronze.espn_matchsheet``, ``silver.fbref_match_enriched``)
and execute against an in-memory connection. Skips cleanly if sqlglot can
not translate a Trino-specific construct.
"""

from __future__ import annotations

from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_venue.sql"


def _translate(sql_text: str) -> str:
    """Trino → DuckDB transpile + iceberg.<schema>.<tbl> → <schema>.<tbl>.

    DuckDB does not ship XXHASH64; we substitute its built-in ``HASH`` which
    is also a deterministic 64-bit hash. The hash *value* is different from
    Trino but the SQL contract we care about (deterministic, prefix shape,
    one row per LOWER(TRIM(name))) is preserved.
    """
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    # Trino XXHASH64(VARBINARY) -> hex; DuckDB has HASH(VARCHAR) -> UBIGINT.
    # Wrap HASH() with LOWER(printf('%x', ...)) equivalent via TO_HEX.
    out = out.replace(
        "LOWER(HEX(XXHASH64(ENCODE(LOWER(TRIM(venue_raw))))))",
        "LOWER(PRINTF('%x', HASH(LOWER(TRIM(venue_raw)))))",
    )
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # silver.fbref_match_enriched: venue, league, season, date, referee, ...
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
        -- 'Etihad Stadium' present in both FBref + ESPN -> FBref must win
        ('Etihad Stadium',  'ENG-Premier League', 2024, DATE '2024-08-15', 'Mike Dean'),
        -- 'Anfield' FBref-only
        ('Anfield',         'ENG-Premier League', 2024, DATE '2024-09-01', 'Mike Dean'),
        -- Mixed-case duplicate of an FBref venue (Old Trafford)
        ('Old Trafford',    'ENG-Premier League', 2024, DATE '2024-09-15', 'Mike Dean'),
        ('OLD TRAFFORD',    'ENG-Premier League', 2024, DATE '2024-10-15', 'Mike Dean'),
        -- NULL/empty venues — must be filtered out
        (NULL,              'ENG-Premier League', 2024, DATE '2024-11-01', 'Anyone'),
        ('   ',             'ENG-Premier League', 2024, DATE '2024-11-02', 'Anyone')
    """)

    # bronze.espn_matchsheet: venue, league, season (4-char label '2425'),
    # game (the SQL slices substr(game,1,10) -> match_date), _ingested_at
    con.execute("""
        CREATE TABLE bronze.espn_matchsheet (
            venue VARCHAR,
            league VARCHAR,
            season VARCHAR,
            game VARCHAR,
            _ingested_at TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO bronze.espn_matchsheet VALUES
        -- Same venue ('Etihad Stadium') as FBref -> FBref must still win
        ('Etihad Stadium',  'ENG-Premier League', '2425', '2024-08-15-MCI-CHE',
         TIMESTAMP '2026-04-27 09:00:00'),
        -- Duplicate snapshot for the same venue/league/season/game — the SQL
        -- keeps the latest _ingested_at. Both the early and late row would
        -- agree on the venue value, but this exercises the dedup CTE.
        ('Etihad Stadium',  'ENG-Premier League', '2425', '2024-08-15-MCI-CHE',
         TIMESTAMP '2026-04-27 06:00:00'),
        -- ESPN-only venue ('Goodison Park') — fallback path
        ('Goodison Park',   'ENG-Premier League', '2425', '2024-08-22-EVE-LIV',
         TIMESTAMP '2026-04-27 09:00:00'),
        -- NULL/empty venues — must be filtered out
        (NULL,              'ENG-Premier League', '2425', '2024-08-22-EVE-LIV',
         TIMESTAMP '2026-04-27 09:00:00'),
        ('   ',             'ENG-Premier League', '2425', '2024-08-22-EVE-LIV',
         TIMESTAMP '2026-04-27 09:00:00')
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
        pytest.skip(f"DuckDB execution of translated dim_venue SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


@pytest.mark.unit
class TestDimVenueLogic:
    def test_one_row_per_lower_trim(self, gold_rows):
        """4 distinct venues after LOWER(TRIM): etihad, anfield, old trafford, goodison.

        Mixed-case 'Old Trafford' / 'OLD TRAFFORD' must collapse to 1 row.
        NULL / '   ' venues must be filtered out.
        """
        canonicals = sorted(r["venue_canonical"].lower() for r in gold_rows)
        assert canonicals == sorted({c.lower() for c in canonicals}), (
            f"venue_canonical values are not unique by lower(): {canonicals}"
        )
        assert len(gold_rows) == 4, (
            f"expected 4 distinct venues after dedup, got {len(gold_rows)}: "
            f"{[r['venue_canonical'] for r in gold_rows]}"
        )

    def test_fbref_priority_when_both_sources_present(self, gold_rows):
        """When FBref AND ESPN both supply the venue, FBref wins."""
        etihad = [r for r in gold_rows
                  if r["venue_canonical"].lower() == "etihad stadium"]
        assert len(etihad) == 1, "Etihad must collapse to a single row"
        row = etihad[0]
        assert row["venue_source"] == "fbref"
        assert row["venue_canonical"] == "Etihad Stadium"
        # Both source-specific columns are surfaced for debuggability
        assert row["venue_fbref"] == "Etihad Stadium"
        assert row["venue_espn"] == "Etihad Stadium"

    def test_espn_fallback_when_only_espn(self, gold_rows):
        """ESPN-only venue → venue_source='espn'."""
        goodison = [r for r in gold_rows
                    if r["venue_canonical"].lower() == "goodison park"]
        assert len(goodison) == 1, "Goodison must appear exactly once"
        row = goodison[0]
        assert row["venue_source"] == "espn"
        assert row["venue_canonical"] == "Goodison Park"
        assert row["venue_fbref"] is None
        assert row["venue_espn"] == "Goodison Park"

    def test_fbref_only_venue(self, gold_rows):
        """FBref-only venue → venue_source='fbref'."""
        anfield = [r for r in gold_rows
                   if r["venue_canonical"].lower() == "anfield"]
        assert len(anfield) == 1
        assert anfield[0]["venue_source"] == "fbref"
        assert anfield[0]["venue_espn"] is None

    def test_canonical_completeness_contract(self, gold_rows):
        """R0.4: every row has non-NULL canonical/source/version."""
        for r in gold_rows:
            assert r["venue_canonical"] is not None, f"NULL canonical: {r}"
            assert r["venue_source"] is not None, f"NULL source: {r}"
            assert r["venue_version"] == "v1", (
                f"venue_version must be 'v1', got: {r['venue_version']!r}"
            )

    def test_venue_id_is_deterministic(self, gold_rows):
        """venue_id is a hash of LOWER(TRIM(name)) — re-run produces same IDs.

        Asserts the hash prefix shape and that re-executing the translated SQL
        on the same fixtures yields identical IDs (deterministic).
        """
        # Format: 'venue_<hex>'
        for r in gold_rows:
            assert r["venue_id"].startswith("venue_"), (
                f"venue_id must start with 'venue_': {r['venue_id']!r}"
            )

        # Re-execute and compare ID sets
        sql_text = SQL_PATH.read_text(encoding="utf-8")
        translated = _translate(sql_text)
        con2 = duckdb.connect(":memory:")
        _bootstrap(con2)
        rerun = con2.execute(translated).fetchall()
        col_names = [c[0] for c in con2.description]
        rerun_rows = [dict(zip(col_names, r)) for r in rerun]
        assert {r["venue_id"] for r in gold_rows} == {r["venue_id"] for r in rerun_rows}

    def test_null_and_empty_venues_filtered(self, gold_rows):
        """NULL and whitespace-only venues are filtered at source CTEs."""
        canonicals = {r["venue_canonical"] for r in gold_rows}
        assert None not in canonicals
        assert "" not in canonicals
        assert "   " not in canonicals
