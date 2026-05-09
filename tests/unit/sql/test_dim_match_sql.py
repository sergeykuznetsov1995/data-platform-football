"""
Unit tests for ``dags/sql/gold/dim_match.sql`` after E1.5 cutover (2026-05-09).

T2 migrated dim_match's home/away team resolution from
``gold.entity_xref`` to ``silver.xref_team``. The (league, season)
predicate was added to prevent the 1.5-4x JOIN fan-out documented in
``feedback_xref_join_season_predicate.md`` (silver.xref_team is keyed
per-(source, source_id, league, season)).

This file pins down those invariants without a Trino engine. Pattern
mirrors ``test_xref_team_sql.py`` / ``test_dim_team_sql.py``: regex
sanity over the raw SQL.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimMatchCutoverStructure:
    """Regex sanity over ``dim_match.sql`` post-E1.5 cutover."""

    def test_two_left_joins_on_silver_xref_team(self):
        """Both home and away resolution use silver.xref_team."""
        sql = _strip_comments(_read_sql())
        # We expect at least two occurrences (home + away).
        joins = re.findall(
            r"LEFT\s+JOIN\s+iceberg\.silver\.xref_team",
            sql, re.IGNORECASE,
        )
        assert len(joins) >= 2, (
            "dim_match.sql must LEFT JOIN iceberg.silver.xref_team at "
            f"least twice (home + away); found {len(joins)}"
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        """gold.entity_xref must NOT appear outside header comments."""
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "dim_match.sql must NOT reference gold.entity_xref in "
            "executable SQL after E1.5 cutover"
        )

    def test_each_join_has_fbref_source_filter(self):
        """Every silver.xref_team JOIN must filter source = 'fbref' so
        we don't accidentally match Understat/WhoScored canonicals."""
        sql = _read_sql()
        # Count occurrences of `source ... = ... 'fbref'` near the joins.
        # Tolerant regex — alias-prefixed (home_x.source / away_x.source)
        # is the documented form, but we also accept bare `source`.
        fbref_filters = re.findall(
            r"\.source\s*=\s*'fbref'", sql, re.IGNORECASE,
        )
        assert len(fbref_filters) >= 2, (
            "dim_match.sql must filter `source = 'fbref'` on BOTH "
            f"silver.xref_team joins; found {len(fbref_filters)}"
        )

    def test_each_join_includes_league_predicate(self):
        """Both joins must include `<alias>.league = m.league` to
        prevent the 1.5-4x fan-out (xref_team is per-season)."""
        sql = _read_sql()
        league_predicates = re.findall(
            r"\.league\s*=\s*m\.league", sql, re.IGNORECASE,
        )
        assert len(league_predicates) >= 2, (
            "dim_match.sql must have a `<alias>.league = m.league` "
            "predicate in BOTH home and away joins to prevent the "
            "documented 1.5-4x JOIN fan-out; found "
            f"{len(league_predicates)}"
        )

    def test_each_join_casts_season_to_varchar(self):
        """silver.xref_team.season is varchar; m.season is bigint —
        each JOIN must CAST(m.season AS varchar) to avoid an implicit
        cast trap."""
        sql = _read_sql()
        season_casts = re.findall(
            r"CAST\s*\(\s*m\.season\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        )
        assert len(season_casts) >= 2, (
            "dim_match.sql must CAST(m.season AS varchar) in BOTH "
            "home and away joins (silver.xref_team.season is varchar); "
            f"found {len(season_casts)}"
        )

    def test_home_and_away_canonical_id_selected(self):
        """SELECT must surface canonical_id from both home and away alias."""
        sql = _read_sql()
        # Tolerant of `home_x.canonical_id AS home_team_id` style.
        assert re.search(
            r"home_x\.canonical_id\s+AS\s+home_team_id",
            sql, re.IGNORECASE,
        ), (
            "dim_match.sql must SELECT `home_x.canonical_id AS "
            "home_team_id`"
        )
        assert re.search(
            r"away_x\.canonical_id\s+AS\s+away_team_id",
            sql, re.IGNORECASE,
        ), (
            "dim_match.sql must SELECT `away_x.canonical_id AS "
            "away_team_id`"
        )

    def test_migration_breadcrumb_in_header(self):
        """E1.5 cutover breadcrumb must be present."""
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql, (
            "dim_match.sql must keep the E1.5 migration breadcrumb"
        )

    def test_pure_select_no_create_table(self):
        """gold_tasks wraps in CTAS — file must stay pure SELECT."""
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "dim_match.sql must remain a pure SELECT"
        )
