"""
DuckDB logic tests for Silver ``xref_referee`` SQL — the #465 norm-collision
guard in the ``aliases`` CTE.

Two alias rows may fold to ONE normalised key within one league while pointing
at DIFFERENT canonicals ('A Madley' / 'A. Madley' → 'amadley'): without the
``GROUP BY (norm, league) + MAX(canonical_*)`` guard (precedent:
``dim_match.venue_aliases`` / #425) the alias JOIN fans out raw_refs rows and
breaks PK (source, source_id, league, season).

Strategy: render the template through the REAL renderer
(``utils.medallion_config.render_sql_template``) with a hermetic ALIAS_VALUES
set, transpile Trino → DuckDB via sqlglot (NORMALIZE → strip_accents hack as
in test_dim_venue_logic), bootstrap the 3 bronze fixture tables, execute,
assert. Also covers the pre-existing contract: cross-source merge via the
curated dictionary, orphan prefix fallback, and (since the #930 native
cutover) the fotmob branch reading fotmob_match_payloads_current — league
reconstructed via league_map(competition_id), season slug derived from
source_season_key, out-of-map competitions scoped out.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_referee.sql.j2"

# ``utils.medallion_config`` lives under ``dags/utils/`` — dags/ is on
# sys.path inside the Airflow container but not on the host.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

LEAGUE = "ENG-Premier League"

# Hermetic 4-tuple VALUES (raw_name, canonical_name, canonical_id, league) —
# the template contract (rendered live by get_referee_alias_sql_values with
# with_canonical_id=True, with_league=True). 'A Madley' and 'A. Madley' fold
# to the SAME norm ('amadley') in the SAME league but point at DIFFERENT
# canonicals — the #465 collision scenario (a YAML curation error the guard
# must absorb without fanning out).
ALIAS_VALUES = (
    "('Michael Oliver', 'Michael Oliver', 'ref_michael_oliver', 'ENG-Premier League'),\n"
    "        ('M Oliver', 'Michael Oliver', 'ref_michael_oliver', 'ENG-Premier League'),\n"
    "        ('A Madley', 'Andrew Madley', 'ref_andrew_madley', 'ENG-Premier League'),\n"
    "        ('A. Madley', 'Aaron Madley', 'ref_aaron_madley', 'ENG-Premier League')"
)


def _render_and_translate() -> str:
    from utils.medallion_config import render_sql_template

    sql_text = render_sql_template(
        TEMPLATE_PATH, referee_aliases_values_sql=ALIAS_VALUES,
    )
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.bronze.", "bronze.")
    # NORMALIZE(x, NFD) → strip_accents(x); the \p{Mn}+ strip then no-ops
    # (same hack as test_dim_venue_logic).
    out = re.sub(r"NORMALIZE\((.*?),\s*NFD\)", r"strip_accents(\1)", out)
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    # Only the columns the template reads; season = year-start BIGINT (live
    # bronze format) → slug '2526' in the output. source_season_id (#913
    # Phase 2) stays NULL → the season CASE falls through to the legacy
    # year-start slug branch. (The column was missing from this fixture
    # since #913, silently skipping EVERY test below — restored with #930.)
    con.execute("""
        CREATE TABLE bronze.fbref_schedule (
            referee VARCHAR, league VARCHAR, season BIGINT,
            source_season_id VARCHAR
        )
    """)
    con.execute(f"""
        INSERT INTO bronze.fbref_schedule VALUES
        ('Michael Oliver', '{LEAGUE}', 2025, NULL),
        ('Joe Orphan',     '{LEAGUE}', 2025, NULL)
    """)

    con.execute("""
        CREATE TABLE bronze.matchhistory_results (
            referee VARCHAR, league VARCHAR, season BIGINT
        )
    """)
    con.execute(f"""
        INSERT INTO bronze.matchhistory_results VALUES
        ('M Oliver', '{LEAGUE}', 2025),
        -- #465 collision: matches BOTH 'A Madley' and 'A. Madley' alias rows
        ('A Madley', '{LEAGUE}', 2025)
    """)

    # #930 native cutover: the fotmob branch reads the *_current view — one
    # committed row per match (manifest identity entity_id = match_id), no
    # league/season columns (league ← league_map(competition_id), season slug
    # ← source_season_key). competition_id is VARCHAR in payloads (live schema).
    con.execute("""
        CREATE TABLE bronze.fotmob_match_payloads_current (
            match_id BIGINT, competition_id VARCHAR,
            source_season_key VARCHAR, match_facts_json VARCHAR
        )
    """)
    # comp 47 = ENG-Premier League, club-style key '2025/2026' → slug '2526'.
    con.execute(
        "INSERT INTO bronze.fotmob_match_payloads_current VALUES (?, ?, ?, ?)",
        (4506553, "47", "2025/2026",
         '{"infoBox": {"Referee": {"text": "Michael Oliver"}}}'),
    )
    # comp 77 = INT-World Cup, single-year key '2026' → 4-digit slug '2026'.
    con.execute(
        "INSERT INTO bronze.fotmob_match_payloads_current VALUES (?, ?, ?, ?)",
        (9900001, "77", "2026",
         '{"infoBox": {"Referee": {"text": "Cup Referee"}}}'),
    )
    # comp 999 is NOT in league_map — the INNER JOIN must scope it out.
    con.execute(
        "INSERT INTO bronze.fotmob_match_payloads_current VALUES (?, ?, ?, ?)",
        (9900002, "999", "2025/2026",
         '{"infoBox": {"Referee": {"text": "Out Of Scope Referee"}}}'),
    )


@pytest.fixture(scope="module")
def xref_rows():
    try:
        translated = _render_and_translate()
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
        pytest.skip(
            f"DuckDB execution of translated xref_referee SQL failed: {e}"
        )

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestXrefRefereeCollisionGuard:
    """#465: a norm collision in the alias dictionary must not break the PK."""

    def test_pk_unique(self, xref_rows):
        pk = [
            (r["source"], r["source_id"], r["league"], r["season"])
            for r in xref_rows
        ]
        assert len(pk) == len(set(pk)), (
            f"duplicate PK rows (alias JOIN fan-out): {sorted(pk)}"
        )

    def test_collision_resolves_to_single_row(self, xref_rows):
        """Membership only — the guard picks ONE deterministic canonical
        (MAX); which one wins is not part of the contract."""
        rows = [r for r in xref_rows if r["source_id"] == "A Madley"]
        assert len(rows) == 1, (
            f"'A Madley' matches 2 colliding alias rows — expected the GROUP "
            f"BY guard to collapse them, got {len(rows)} rows"
        )
        assert rows[0]["canonical_id"] in {
            "ref_andrew_madley", "ref_aaron_madley",
        }
        assert rows[0]["confidence"] == "name_alias"


class TestXrefRefereeExistingContract:
    """Pre-existing behavior must survive the #465 guard."""

    def test_cross_source_merge_via_dictionary(self, xref_rows):
        """FBref 'Michael Oliver', MatchHistory 'M Oliver' and FotMob
        'Michael Oliver' all resolve to ONE canonical_id."""
        oliver = [
            r for r in xref_rows
            if r["source_id"] in {"Michael Oliver", "M Oliver"}
        ]
        assert {r["source"] for r in oliver} == {
            "fbref", "matchhistory", "fotmob",
        }
        assert {r["canonical_id"] for r in oliver} == {"ref_michael_oliver"}

    def test_fotmob_league_from_competition_id(self, xref_rows):
        """#930: league is reconstructed via league_map(competition_id) —
        varchar comp '47' in payloads → 'ENG-Premier League'; the _current
        view is one row per match, so exactly one fotmob row survives with
        no in-SQL snapshot dedup."""
        fm = [
            r for r in xref_rows
            if r["source"] == "fotmob" and r["source_id"] == "Michael Oliver"
        ]
        assert len(fm) == 1, "one committed payload row → one fotmob xref row"
        assert fm[0]["league"] == LEAGUE

    def test_fotmob_out_of_map_competition_scoped_out(self, xref_rows):
        """#930: INNER JOIN to league_map keeps the legacy 14-league scope —
        comp 999 (not in the map) must not leak into Silver."""
        assert [
            r for r in xref_rows if r["source_id"] == "Out Of Scope Referee"
        ] == [], "competition_id outside league_map must be scoped out"

    def test_fotmob_wc_single_year_season_slug(self, xref_rows):
        """#930 / cutover map §2.2: INT-World Cup single-year key '2026' →
        4-digit slug '2026' (WC branch of the legacy season CASE)."""
        wc = [r for r in xref_rows if r["source_id"] == "Cup Referee"]
        assert len(wc) == 1
        assert wc[0]["league"] == "INT-World Cup"
        assert wc[0]["season"] == "2026"
        assert wc[0]["canonical_id"] == "fm_ref_cup_referee"
        assert wc[0]["confidence"] == "orphan"

    def test_orphan_prefix_fallback(self, xref_rows):
        orphan = next(r for r in xref_rows if r["source_id"] == "Joe Orphan")
        assert orphan["canonical_id"] == "fb_ref_joe_orphan"
        assert orphan["confidence"] == "orphan"

    def test_season_slug_format(self, xref_rows):
        """Club season → slug '2526' from BOTH legacy forms (fbref/mh
        year-start BIGINT 2025) and the native form (fotmob
        source_season_key '2025/2026') — #404 idiom, single expression."""
        club = {r["season"] for r in xref_rows if r["league"] == LEAGUE}
        assert club == {"2526"}
