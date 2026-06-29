"""
Unit tests for ``dags/sql/gold/dim_match.sql.j2`` — star centre (#425).

dim_match carries FKs to every star dim: home/away_team_id (xref_team),
referee_id (xref_referee), venue_id (inline alias VALUES — byte-identical
to dim_venue), home/away_manager_id (bronze.fbref_match_managers +
xref_manager). Context columns renamed to the design: date -> match_date,
time -> kickoff_time. Denormalised extras (team names, total_goals, btts)
moved out — stats belong to facts.

Every xref JOIN must keep the (league, season) predicate — without it the
JOIN fans out 1.5-4× (memory: feedback_xref_join_season_predicate).

Pattern mirrors ``test_xref_team_sql.py``: regex sanity over the raw
template text (no Trino engine).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_match.sql.j2"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimMatchStarStructure:
    def test_two_left_joins_on_silver_xref_team(self):
        sql = _strip_comments(_read_sql())
        joins = re.findall(
            r"LEFT\s+JOIN\s+iceberg\.silver\.xref_team", sql, re.IGNORECASE
        )
        assert len(joins) == 2, (
            f"expected exactly 2 LEFT JOINs on silver.xref_team "
            f"(home + away), found {len(joins)}"
        )

    def test_referee_fk_via_xref_referee(self):
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.xref_referee", sql, re.IGNORECASE
        ), "dim_match must resolve referee_id via silver.xref_referee"
        assert re.search(r"AS\s+referee_id", sql, re.IGNORECASE)

    def test_manager_fks_via_bronze_and_xref_manager(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fbref_match_managers" in sql
        assert "iceberg.silver.xref_manager" in sql
        assert re.search(r"AS\s+home_manager_id", sql, re.IGNORECASE)
        assert re.search(r"AS\s+away_manager_id", sql, re.IGNORECASE)
        # bronze season is a year-start BIGINT — the slug conversion idiom
        # must be present (silent type mismatch would zero the JOIN).
        assert re.search(r"LPAD\(CAST\(MOD\(", sql, re.IGNORECASE), (
            "bronze season bigint -> slug conversion (LPAD/MOD idiom) missing"
        )

    def test_venue_alias_placeholder_present(self):
        """venue_id resolves via the SAME alias VALUES as dim_venue."""
        assert "{{ venue_aliases_values_sql }}" in _read_sql()

    def test_every_xref_join_has_league_and_season_predicate(self):
        """All 4 xref JOIN blocks carry league AND season equality —
        the anti-fan-out contract."""
        sql = _strip_comments(_read_sql())
        # Split on JOIN keywords and inspect each xref block.
        blocks = re.split(r"(?:LEFT\s+)?(?:INNER\s+)?JOIN\s+", sql,
                          flags=re.IGNORECASE)
        xref_blocks = [b for b in blocks if b.startswith("iceberg.silver.xref")]
        assert len(xref_blocks) == 4, (
            f"expected 4 xref JOIN blocks (2×team, referee, manager), "
            f"got {len(xref_blocks)}"
        )
        for block in xref_blocks:
            assert re.search(r"\.league\s*=", block), (
                f"xref JOIN missing league predicate: {block[:120]!r}"
            )
            assert re.search(r"\.season\s*=", block), (
                f"xref JOIN missing season predicate: {block[:120]!r}"
            )
            assert re.search(r"source\s*=\s*'fbref'", block), (
                f"xref JOIN missing source='fbref' filter: {block[:120]!r}"
            )

    def test_design_renames_applied(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"AS\s+match_date", sql, re.IGNORECASE)
        assert re.search(r"AS\s+kickoff_time", sql, re.IGNORECASE)

    def test_denormalised_extras_removed(self):
        """Team names / total_goals / btts left the passport — stats live in
        facts, names in dim_team (#425)."""
        sql = _strip_comments(_read_sql())
        for gone in ("home_team_name", "away_team_name", "total_goals",
                     "btts"):
            assert not re.search(rf"\b{gone}\b", sql, re.IGNORECASE), (
                f"{gone!r} must not be emitted by dim_match anymore"
            )

    def test_home_and_away_canonical_id_selected(self):
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"home_x\.canonical_id\s+AS\s+home_team_id", sql, re.IGNORECASE
        )
        assert re.search(
            r"away_x\.canonical_id\s+AS\s+away_team_id", sql, re.IGNORECASE
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "entity_xref" not in sql

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
        assert "INSERT INTO" not in sql.upper()
