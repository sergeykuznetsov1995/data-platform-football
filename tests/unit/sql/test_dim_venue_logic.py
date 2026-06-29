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
# merge test. capacity is a CURATED FALLBACK behind FotMob (#750): Etihad carries a
# curated 99999 that FotMob (53400) must override; Goodison carries 39414 with NO
# FotMob row, so the fallback surfaces. capacity is the 7th, UNQUOTED column.
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

    # #719 coords + #750 attributes from silver.fotmob_team_profile, matched by
    # normalised venue name. Lookup-only — must NOT add venues or fan out the grain.
    # 'Gtech' spelling attaches to venue_brentford; Goodison is absent (curated, all
    # FotMob attrs NULL); 'Phantom Arena' is unknown to fbref/espn (must NOT create a
    # venue). #750: 'New Orphan Park' HAS a FotMob row → city/capacity/coords fill the
    # orphan; Anfield's FotMob city differs ('FotMob-Liverpool') to prove curated wins.
    con.execute("""
        CREATE TABLE silver.fotmob_team_profile (
            venue VARCHAR, venue_latitude DOUBLE, venue_longitude DOUBLE,
            venue_city VARCHAR, venue_surface VARCHAR,
            venue_capacity INTEGER, venue_opened INTEGER, league VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO silver.fotmob_team_profile VALUES
        ('Etihad Stadium',          53.4831, -2.2004, 'Manchester',       'Grass',      53400, 2003, 'ENG-Premier League'),
        ('Anfield',                 53.4308, -2.9608, 'FotMob-Liverpool', 'Grass',      61276, 1884, 'ENG-Premier League'),
        ('Old Trafford',            53.4631, -2.2914, 'Manchester',       'Grass',      74310, 1910, 'ENG-Premier League'),
        ('Gtech Community Stadium', 51.4906, -0.2889, 'London',           'Grass',      17250, 2020, 'ENG-Premier League'),
        ('New Orphan Park',          1.0000,  2.0000, 'Orphanville',      'Artificial',  9999, 1999, 'ENG-Premier League'),
        ('Phantom Arena',           10.0000, 20.0000, 'Nowhere',          'Grass',        100, 1900, 'ENG-Premier League')
    """)

    # #753: SofaScore per-match venue (silver.sofascore_venue) — lookup-only
    # enrichment BEHIND FotMob for city/coords, and the SOLE source of venue
    # country. Etihad: FotMob primary must win over SofaScore's coords. Goodison:
    # moved ground with NO FotMob row → SofaScore fills coords (the #753 win).
    # New Orphan Park: orphan → SofaScore fills country FotMob/curated lack.
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
        # capacity (issue #750): FotMob (53400) overrides the curated fallback (99999).
        assert etihad[0]["capacity"] == 53400
        # surface / opened (issue #750): new FotMob attributes.
        assert etihad[0]["surface"] == "Grass"
        assert etihad[0]["opened"] == 2003

    def test_two_spellings_merge_into_one_venue(self, gold_rows):
        """Core #145: 'Gtech Community Stadium' (FBref) and 'Brentford Community
        Stadium' (ESPN) collapse to a single venue_id."""
        brentford = _by_id(gold_rows, "venue_brentford")
        assert len(brentford) == 1, "two spellings must merge into one row"
        row = brentford[0]
        assert row["venue_name"] == "Gtech Community Stadium"
        assert row["venue_source"] == "curated"
        # capacity (from FotMob 'Gtech' row) survives the spelling merge.
        assert row["capacity"] == 17250

    def test_capacity_fotmob_primary_curated_fallback(self, gold_rows):
        """#750: capacity = COALESCE(FotMob, curated fallback). A curated venue
        without a FotMob row (Goodison — moved-ground case) surfaces its curated
        fallback (39414); an ORPHAN with a FotMob row inherits FotMob's capacity."""
        goodison = _by_id(gold_rows, "venue_goodison")  # curated fallback, no FotMob
        assert len(goodison) == 1
        assert goodison[0]["capacity"] == 39414
        orphan = [r for r in gold_rows if r["venue_source"] == "orphan"][0]
        assert orphan["capacity"] == 9999  # filled from FotMob despite being orphan

    def test_mixed_case_folds(self, gold_rows):
        """'Old Trafford' / 'OLD TRAFFORD' fold to one curated venue."""
        assert len(_by_id(gold_rows, "venue_old_trafford")) == 1

    def test_orphan_fallback(self, gold_rows):
        """Unmatched raw name → venue_source='orphan', hash-based venue_<hex> id
        (not a YAML slug). city is filled from FotMob (#750); country is now filled
        from SofaScore event.venue (#753) — FotMob carries team, not venue, country."""
        orphans = [r for r in gold_rows if r["venue_source"] == "orphan"]
        assert len(orphans) == 1
        row = orphans[0]
        assert row["venue_name"] == "New Orphan Park"
        assert row["venue_id"].startswith("venue_")
        assert row["venue_id"] not in {
            "venue_etihad", "venue_anfield", "venue_old_trafford",
            "venue_goodison", "venue_brentford",
        }
        assert row["city"] == "Orphanville"   # #750: FotMob fills non-curated city
        assert row["country"] == "Orphanland"  # #753: SofaScore fills venue country

    def test_curated_city_wins_over_fotmob(self, gold_rows):
        """#750 precedence: curated venue_aliases.yaml city wins over FotMob.
        Anfield's FotMob row carries 'FotMob-Liverpool' but the curated 'Liverpool'
        must surface."""
        anfield = _by_id(gold_rows, "venue_anfield")[0]
        assert anfield["city"] == "Liverpool"

    def test_surface_opened_attached(self, gold_rows):
        """#750: surface/opened flow from FotMob; NULL when no FotMob match."""
        etihad = _by_id(gold_rows, "venue_etihad")[0]
        assert etihad["surface"] == "Grass"
        assert etihad["opened"] == 2003
        goodison = _by_id(gold_rows, "venue_goodison")[0]  # no FotMob row
        assert goodison["surface"] is None
        assert goodison["opened"] is None

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

    # ---- #719: FotMob stadium coordinates ----------------------------------

    def test_coords_attach_to_curated(self, gold_rows):
        """Coords flow from silver.fotmob_team_profile onto the matching venue."""
        etihad = _by_id(gold_rows, "venue_etihad")[0]
        assert etihad["latitude"] == pytest.approx(53.4831)
        assert etihad["longitude"] == pytest.approx(-2.2004)

    def test_coords_survive_spelling_merge(self, gold_rows):
        """FotMob's 'Gtech' spelling normalises onto venue_brentford even though
        the venue is also seen as 'Brentford Community Stadium' via ESPN."""
        brentford = _by_id(gold_rows, "venue_brentford")[0]
        assert brentford["latitude"] == pytest.approx(51.4906)
        assert brentford["longitude"] == pytest.approx(-0.2889)

    def test_sofascore_coords_fill_moved_ground(self, gold_rows):
        """#753: a moved ground with NO FotMob row (Goodison — FotMob's
        current-ground bias mislabels it) now gets coords from SofaScore's
        per-match venue. These were NULL before #753."""
        goodison = _by_id(gold_rows, "venue_goodison")[0]
        assert goodison["latitude"] == pytest.approx(53.4388)
        assert goodison["longitude"] == pytest.approx(-2.9663)

    def test_fotmob_coords_add_no_venues(self, gold_rows):
        """'Phantom Arena' exists only in FotMob (not fbref/espn) → it must NOT
        appear as a venue. Guards the lookup-only / no-fan-out contract."""
        names = {r["venue_name"] for r in gold_rows}
        assert "Phantom Arena" not in names
        assert len(gold_rows) == 6  # unchanged by the coords join

    # ---- #753: SofaScore venue enrichment ----------------------------------

    def test_fotmob_coords_win_over_sofascore(self, gold_rows):
        """#753 precedence: FotMob coords are PRIMARY; SofaScore only fills NULLs.
        Etihad has both — FotMob's 53.4831 must win over SofaScore's 88.0."""
        etihad = _by_id(gold_rows, "venue_etihad")[0]
        assert etihad["latitude"] == pytest.approx(53.4831)
        assert etihad["longitude"] == pytest.approx(-2.2004)

    def test_sofascore_fills_country_for_orphan(self, gold_rows):
        """#753: venue country comes solely from SofaScore event.venue (FotMob
        carries team country, not venue). An orphan with a SofaScore row — which
        had NULL country before #753 — now resolves its country."""
        orphan = [r for r in gold_rows if r["venue_source"] == "orphan"][0]
        assert orphan["country"] == "Orphanland"

    def test_sofascore_adds_no_venues(self, gold_rows):
        """'SofaScore Phantom' exists only in SofaScore (not fbref/espn) → must NOT
        appear as a venue. Guards the lookup-only / no-fan-out contract."""
        names = {r["venue_name"] for r in gold_rows}
        assert "SofaScore Phantom" not in names
        assert len(gold_rows) == 6  # unchanged by the SofaScore join
