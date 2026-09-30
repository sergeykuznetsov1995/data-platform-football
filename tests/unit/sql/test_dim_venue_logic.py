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
# league, capacity) — 7-tuple. Two Brentford spellings share one canonical_id →
# merge test. capacity is the 7th, UNQUOTED column and — since #1590 removed the
# FotMob primary (#750) — the only capacity source (Etihad 99999, Goodison 39414).
_TEST_ALIASES = """\
    ('Etihad Stadium', 'venue_etihad', 'Etihad Stadium', 'Manchester', 'England', 'ENG-Premier League', 99999),
    ('Anfield', 'venue_anfield', 'Anfield', 'Liverpool', 'England', 'ENG-Premier League', NULL),
    ('Old Trafford', 'venue_old_trafford', 'Old Trafford', 'Manchester', 'England', 'ENG-Premier League', NULL),
    ('Goodison Park', 'venue_goodison', 'Goodison Park', 'Liverpool', 'England', 'ENG-Premier League', 39414),
    ('Gtech Community Stadium', 'venue_brentford', 'Gtech Community Stadium', 'London', 'England', 'ENG-Premier League', NULL),
    ('Brentford Community Stadium', 'venue_brentford', 'Gtech Community Stadium', 'London', 'England', 'ENG-Premier League', NULL)"""

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

    # #753: SofaScore per-match venue (silver.sofascore_venue) — lookup-only
    # enrichment for city/coords/country (sole source since #1590 removed the
    # FotMob team-profile branch). Etihad: curated city wins over SofaScore's.
    # Goodison: moved ground → SofaScore fills coords. 'Gtech' spelling attaches
    # to venue_brentford. New Orphan Park: orphan → SofaScore fills city/country.
    # 'SofaScore Phantom' is unknown to fbref/espn → must NOT create a venue.
    con.execute("""
        CREATE TABLE silver.sofascore_venue (
            stadium VARCHAR, city VARCHAR, country VARCHAR,
            venue_latitude DOUBLE, venue_longitude DOUBLE,
            _bronze_ingested_at TIMESTAMP, league VARCHAR, season VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.sofascore_venue VALUES
        ('Etihad Stadium',    'SS-Manchester',  'SS-England', 88.0000, 88.0000, TIMESTAMP '2026-06-23 09:00:00', 'ENG-Premier League', '2425'),
        ('Goodison Park',     'Liverpool',      'England',    53.4388, -2.9663, TIMESTAMP '2026-06-23 09:00:00', 'ENG-Premier League', '2425'),
        ('Gtech Community Stadium', 'London',   'England',    51.4906, -0.2889, TIMESTAMP '2026-06-23 09:00:00', 'ENG-Premier League', '2425'),
        ('New Orphan Park',   'SS-Orphanville', 'Orphanland', 7.0000,  8.0000,  TIMESTAMP '2026-06-23 09:00:00', 'ENG-Premier League', '2425'),
        ('SofaScore Phantom', 'Ghost City',     'Ghostland',  30.0000, 40.0000, TIMESTAMP '2026-06-23 09:00:00', 'ENG-Premier League', '2425')
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
        # capacity: curated only since #1590 (FotMob primary removed).
        assert etihad[0]["capacity"] == 99999
        # surface / opened: FotMob-only (#750) → typed NULL since #1590.
        assert etihad[0]["surface"] is None
        assert etihad[0]["opened"] is None

    def test_two_spellings_merge_into_one_venue(self, gold_rows):
        """Core #145: 'Gtech Community Stadium' (FBref) and 'Brentford Community
        Stadium' (ESPN) collapse to a single venue_id."""
        brentford = _by_id(gold_rows, "venue_brentford")
        assert len(brentford) == 1, "two spellings must merge into one row"
        row = brentford[0]
        assert row["venue_name"] == "Gtech Community Stadium"
        assert row["venue_source"] == "curated"
        # no curated capacity for Brentford and no FotMob primary since #1590.
        assert row["capacity"] is None

    def test_capacity_is_curated_only(self, gold_rows):
        """#1590: capacity = curated venue_aliases.yaml value (the FotMob
        primary was removed). Goodison surfaces its curated 39414; an orphan has
        no curated row → NULL."""
        goodison = _by_id(gold_rows, "venue_goodison")
        assert len(goodison) == 1
        assert goodison[0]["capacity"] == 39414
        orphan = [r for r in gold_rows if r["venue_source"] == "orphan"][0]
        assert orphan["capacity"] is None

    def test_mixed_case_folds(self, gold_rows):
        """'Old Trafford' / 'OLD TRAFFORD' fold to one curated venue."""
        assert len(_by_id(gold_rows, "venue_old_trafford")) == 1

    def test_orphan_fallback(self, gold_rows):
        """Unmatched raw name → venue_source='orphan', hash-based venue_<hex> id
        (not a YAML slug). city and country are filled from SofaScore event.venue
        (#753; the FotMob city source was removed in #1590)."""
        orphans = [r for r in gold_rows if r["venue_source"] == "orphan"]
        assert len(orphans) == 1
        row = orphans[0]
        assert row["venue_name"] == "New Orphan Park"
        assert row["venue_id"].startswith("venue_")
        assert row["venue_id"] not in {
            "venue_etihad", "venue_anfield", "venue_old_trafford",
            "venue_goodison", "venue_brentford",
        }
        assert row["city"] == "SS-Orphanville"  # #753: SofaScore fills non-curated city
        assert row["country"] == "Orphanland"  # #753: SofaScore fills venue country

    def test_curated_city_wins_over_sofascore(self, gold_rows):
        """Precedence: curated venue_aliases.yaml city wins over SofaScore.
        Etihad's SofaScore row carries 'SS-Manchester' but the curated
        'Manchester' must surface."""
        etihad = _by_id(gold_rows, "venue_etihad")[0]
        assert etihad["city"] == "Manchester"

    def test_surface_opened_are_typed_nulls(self, gold_rows):
        """#1590: surface/opened were FotMob-only (#750) → NULL for every row."""
        for r in gold_rows:
            assert r["surface"] is None
            assert r["opened"] is None

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

    # ---- stadium coordinates (SofaScore since #1590) -------------------------

    def test_coords_attach_to_curated(self, gold_rows):
        """Coords flow from silver.sofascore_venue onto the matching venue."""
        etihad = _by_id(gold_rows, "venue_etihad")[0]
        assert etihad["latitude"] == pytest.approx(88.0)
        assert etihad["longitude"] == pytest.approx(88.0)

    def test_coords_survive_spelling_merge(self, gold_rows):
        """SofaScore's 'Gtech' spelling normalises onto venue_brentford even
        though the venue is also seen as 'Brentford Community Stadium' via ESPN."""
        brentford = _by_id(gold_rows, "venue_brentford")[0]
        assert brentford["latitude"] == pytest.approx(51.4906)
        assert brentford["longitude"] == pytest.approx(-0.2889)

    def test_sofascore_coords_fill_moved_ground(self, gold_rows):
        """#753: a moved ground (Goodison) gets coords from SofaScore's
        per-match venue. These were NULL before #753."""
        goodison = _by_id(gold_rows, "venue_goodison")[0]
        assert goodison["latitude"] == pytest.approx(53.4388)
        assert goodison["longitude"] == pytest.approx(-2.9663)

    # ---- #753: SofaScore venue enrichment ----------------------------------

    def test_sofascore_fills_country_for_orphan(self, gold_rows):
        """#753: venue country comes solely from SofaScore event.venue. An orphan with a SofaScore row — which
        had NULL country before #753 — now resolves its country."""
        orphan = [r for r in gold_rows if r["venue_source"] == "orphan"][0]
        assert orphan["country"] == "Orphanland"

    def test_sofascore_adds_no_venues(self, gold_rows):
        """'SofaScore Phantom' exists only in SofaScore (not fbref/espn) → must NOT
        appear as a venue. Guards the lookup-only / no-fan-out contract."""
        names = {r["venue_name"] for r in gold_rows}
        assert "SofaScore Phantom" not in names
        assert len(gold_rows) == 6  # unchanged by the SofaScore join
