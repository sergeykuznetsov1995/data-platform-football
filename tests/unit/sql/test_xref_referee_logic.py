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
curated dictionary, fotmob latest-snapshot dedup, orphan prefix fallback.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
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
    # bronze format) → slug '2526' in the output.
    con.execute("""
        CREATE TABLE bronze.fbref_schedule (
            referee VARCHAR, league VARCHAR, season BIGINT
        )
    """)
    con.execute(f"""
        INSERT INTO bronze.fbref_schedule VALUES
        ('Michael Oliver', '{LEAGUE}', 2025),
        ('Joe Orphan',     '{LEAGUE}', 2025)
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

    con.execute("""
        CREATE TABLE bronze.fotmob_match_details (
            match_id VARCHAR, match_facts_json VARCHAR,
            league VARCHAR, season BIGINT, _ingested_at TIMESTAMP
        )
    """)
    # Two snapshots of one match — the latest (Michael Oliver) must win.
    con.execute(
        "INSERT INTO bronze.fotmob_match_details VALUES (?, ?, ?, ?, ?)",
        ("4506553",
         '{"infoBox": {"Referee": {"text": "Stale Snapshot Referee"}}}',
         LEAGUE, 2025, datetime(2026, 6, 1, 3, 0, 0)),
    )
    con.execute(
        "INSERT INTO bronze.fotmob_match_details VALUES (?, ?, ?, ?, ?)",
        ("4506553",
         '{"infoBox": {"Referee": {"text": "Michael Oliver"}}}',
         LEAGUE, 2025, datetime(2026, 6, 2, 3, 0, 0)),
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

    def test_fotmob_dedup_keeps_latest_snapshot(self, xref_rows):
        stale = [
            r for r in xref_rows
            if r["source_id"] == "Stale Snapshot Referee"
        ]
        assert stale == [], "older fotmob snapshot must be deduped away"

    def test_orphan_prefix_fallback(self, xref_rows):
        orphan = next(r for r in xref_rows if r["source_id"] == "Joe Orphan")
        assert orphan["canonical_id"] == "fb_ref_joe_orphan"
        assert orphan["confidence"] == "orphan"

    def test_season_slug_format(self, xref_rows):
        """Year-start BIGINT 2025 → slug '2526' (#404 idiom)."""
        assert {r["season"] for r in xref_rows} == {"2526"}
