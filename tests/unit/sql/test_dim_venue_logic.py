"""
Unit tests for Gold ``dim_venue`` SQL logic (issue #145).

``dim_venue.sql`` became a Jinja template ``dim_venue.sql.j2`` with a single
``{{ venue_aliases_values_sql }}`` placeholder. Identity is now the explicit
``venue_<slug>`` from ``venue_aliases.yaml`` (curated) instead of a name hash:
different spellings of one stadium ("Gtech Community Stadium" /
"Brentford Community Stadium") merge into ONE ``venue_id``; raw names with no
alias fall back to a normalised-name hash and are marked ``venue_source =
'orphan'``. ``city`` / ``country`` come from the YAML for curated venues.

Strategy: substitute the placeholder with a small HERMETIC alias VALUES set
(independent of the shipped YAML so the SQL-logic test stays stable), transpile
Trino → DuckDB via sqlglot, materialise fixture tables and execute. Skips
cleanly if sqlglot cannot translate a Trino-specific construct.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_venue.sql.j2"

# Hermetic alias VALUES (raw_name, canonical_id, canonical_name, city, country,
# league, capacity). Two Brentford spellings share one canonical_id → merge test.
# capacity (issue #434) is the 7th, UNQUOTED column (NULL allowed).
_TEST_ALIASES = """\
    ('Etihad Stadium', 'venue_etihad', 'Etihad Stadium', 'Manchester', 'England', 'ENG-Premier League', 53400),
    ('Anfield', 'venue_anfield', 'Anfield', 'Liverpool', 'England', 'ENG-Premier League', 61276),
    ('Old Trafford', 'venue_old_trafford', 'Old Trafford', 'Manchester', 'England', 'ENG-Premier League', 74310),
    ('Goodison Park', 'venue_goodison', 'Goodison Park', 'Liverpool', 'England', 'ENG-Premier League', NULL),
    ('Gtech Community Stadium', 'venue_brentford', 'Gtech Community Stadium', 'London', 'England', 'ENG-Premier League', 17250),
    ('Brentford Community Stadium', 'venue_brentford', 'Gtech Community Stadium', 'London', 'England', 'ENG-Premier League', 17250)"""

_PLACEHOLDER_RE = re.compile(
    r"^[ \t]*\{\{\s*venue_aliases_values_sql\s*\}\}[ \t]*$", re.MULTILINE
)


def _render(sql_text: str) -> str:
    """Fill the standalone ``{{ venue_aliases_values_sql }}`` placeholder."""
    return _PLACEHOLDER_RE.sub(lambda _: _TEST_ALIASES, sql_text, count=1)


def _translate(sql_text: str) -> str:
    """Trino → DuckDB transpile + iceberg.<schema>.<tbl> → <schema>.<tbl>.

    Mirrors test_dim_referee_logic: NORMALIZE(x, NFD) → strip_accents(x); the
    ``\\p{Mn}+`` strip then no-ops. XXHASH64(ENCODE(..)) → HASH((..)) so the
    orphan-id ``venue_<hex>`` prefix contract holds (hash value differs from
    Trino but determinism + uniqueness still pass).
    """
    out = sqlglot.parse_one(sql_text, read="trino").sql(
        dialect="duckdb", comments=False
    )
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    out = re.sub(r"NORMALIZE\((.*?),\s*NFD\)", r"strip_accents(\1)", out)
    out = out.replace("XXHASH64(ENCODE(", "HASH((")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            venue VARCHAR, league VARCHAR, season BIGINT, date DATE, referee VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fbref_match_enriched VALUES
        -- curated, present in both feeds
        ('Etihad Stadium',  'ENG-Premier League', 2024, DATE '2024-08-15', 'A'),
        -- curated, FBref-only
        ('Anfield',         'ENG-Premier League', 2024, DATE '2024-09-01', 'A'),
        -- curated, mixed-case duplicate must fold to one venue_id
        ('Old Trafford',    'ENG-Premier League', 2024, DATE '2024-09-15', 'A'),
        ('OLD TRAFFORD',    'ENG-Premier League', 2024, DATE '2024-10-15', 'A'),
        -- curated Brentford: FBref carries the 'Gtech' sponsor spelling
        ('Gtech Community Stadium', 'ENG-Premier League', 2024, DATE '2024-10-20', 'A'),
        -- NOT in aliases → orphan fallback
        ('New Orphan Park', 'ENG-Premier League', 2024, DATE '2024-10-25', 'A'),
        -- filtered out
        (NULL,              'ENG-Premier League', 2024, DATE '2024-11-01', 'A'),
        ('   ',             'ENG-Premier League', 2024, DATE '2024-11-02', 'A')
    """)

    # #735: dim_venue now reads silver.espn_matchsheet (already trimmed, deduped
    # per match, match_date derived) instead of bronze — fixture mirrors that.
    con.execute("""
        CREATE TABLE silver.espn_matchsheet (
            venue VARCHAR, match_date DATE, _bronze_ingested_at TIMESTAMP,
            league VARCHAR, season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.espn_matchsheet VALUES
        -- curated, shared with the FBref feed
        ('Etihad Stadium',  DATE '2024-08-15', TIMESTAMP '2026-04-27 09:00:00', 'ENG-Premier League', '2425'),
        -- curated, ESPN-only
        ('Goodison Park',   DATE '2024-08-22', TIMESTAMP '2026-04-27 09:00:00', 'ENG-Premier League', '2425'),
        -- curated Brentford: ESPN carries the OLD spelling → must merge with 'Gtech'
        ('Brentford Community Stadium', DATE '2024-08-23', TIMESTAMP '2026-04-27 09:00:00', 'ENG-Premier League', '2425'),
        -- filtered out (dim_venue drops NULL/blank venue defensively)
        (NULL,              DATE '2024-08-22', TIMESTAMP '2026-04-27 09:00:00', 'ENG-Premier League', '2425'),
        ('   ',             DATE '2024-08-22', TIMESTAMP '2026-04-27 09:00:00', 'ENG-Premier League', '2425')
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
        pytest.skip(f"DuckDB execution of translated dim_venue SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


def _by_id(rows, vid):
    return [r for r in rows if r["venue_id"] == vid]


@pytest.mark.unit
class TestDimVenueLogic:
    def test_distinct_venue_count(self, gold_rows):
        """6 venues: etihad, anfield, old trafford (case-fold), goodison,
        brentford (2 spellings merge), one orphan. NULL/'   ' filtered."""
        assert len(gold_rows) == 6, (
            f"expected 6 venues, got {len(gold_rows)}: "
            f"{[(r['venue_id'], r['venue_name']) for r in gold_rows]}"
        )

    def test_curated_venue_id_is_yaml_slug(self, gold_rows):
        """Matched venues key on the explicit canonical_id, not a hash."""
        etihad = _by_id(gold_rows, "venue_etihad")
        assert len(etihad) == 1
        assert etihad[0]["venue_source"] == "curated"
        assert etihad[0]["venue_name"] == "Etihad Stadium"
        assert etihad[0]["city"] == "Manchester"
        assert etihad[0]["country"] == "England"
        # capacity (issue #434) flows from the curated alias VALUES.
        assert etihad[0]["capacity"] == 53400

    def test_two_spellings_merge_into_one_venue(self, gold_rows):
        """Core #145: 'Gtech Community Stadium' (FBref) and 'Brentford Community
        Stadium' (ESPN) collapse to a single venue_id."""
        brentford = _by_id(gold_rows, "venue_brentford")
        assert len(brentford) == 1, "two spellings must merge into one row"
        row = brentford[0]
        assert row["venue_name"] == "Gtech Community Stadium"
        assert row["venue_source"] == "curated"
        # capacity survives the merge (both spellings carry 17250).
        assert row["capacity"] == 17250

    def test_orphan_and_uncurated_capacity_null(self, gold_rows):
        """Orphan venues (no alias) and curated venues without a capacity
        value both surface NULL capacity (issue #434)."""
        orphan = [r for r in gold_rows if r["venue_source"] == "orphan"]
        assert orphan, "fixture must produce an orphan venue"
        assert all(r["capacity"] is None for r in orphan), orphan
        goodison = _by_id(gold_rows, "venue_goodison")  # curated, capacity NULL
        assert len(goodison) == 1
        assert goodison[0]["capacity"] is None

    def test_mixed_case_folds(self, gold_rows):
        """'Old Trafford' / 'OLD TRAFFORD' fold to one curated venue."""
        assert len(_by_id(gold_rows, "venue_old_trafford")) == 1

    def test_orphan_fallback(self, gold_rows):
        """Unmatched raw name → venue_source='orphan', NULL city/country,
        hash-based venue_<hex> id (not a YAML slug)."""
        orphans = [r for r in gold_rows if r["venue_source"] == "orphan"]
        assert len(orphans) == 1
        row = orphans[0]
        assert row["venue_name"] == "New Orphan Park"
        assert row["venue_id"].startswith("venue_")
        assert row["venue_id"] not in {
            "venue_etihad", "venue_anfield", "venue_old_trafford",
            "venue_goodison", "venue_brentford",
        }
        assert row["city"] is None
        assert row["country"] is None

    def test_city_country_filled_for_curated(self, gold_rows):
        """Acceptance: city/country populated for every curated venue."""
        for r in gold_rows:
            if r["venue_source"] == "curated":
                assert r["city"] is not None, f"curated venue NULL city: {r}"
                assert r["country"] is not None, f"curated venue NULL country: {r}"

    def test_canonical_completeness_contract(self, gold_rows):
        """Every row has a non-NULL venue_name and a valid venue_source."""
        for r in gold_rows:
            assert r["venue_name"] is not None, f"NULL venue_name: {r}"
            assert r["venue_source"] in {"curated", "orphan"}, f"bad source: {r}"

    def test_venue_id_unique(self, gold_rows):
        """PK: venue_id unique across the dimension."""
        ids = [r["venue_id"] for r in gold_rows]
        assert len(ids) == len(set(ids)), f"duplicate venue_id: {ids}"

    def test_null_and_empty_venues_filtered(self, gold_rows):
        names = {r["venue_name"] for r in gold_rows}
        assert None not in names
        assert "" not in names
        assert "   " not in names
