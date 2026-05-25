"""
Unit tests for ``dags/sql/gold/match_outcomes.sql`` after E1.5 cutover
(2026-05-09).

T2 migrated match_outcomes (ML target table) from ``gold.entity_xref`` to
``silver.xref_team``. Because match_outcomes is the label table for
classification and regression, label leakage is a concern — but the JOINs
only resolve home/away ids, so the cutover is a pure id-mapping swap.
The (league, season) predicate is required to prevent the documented
1.5-4x fan-out.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "match_outcomes.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestMatchOutcomesCutoverStructure:
    """Regex sanity over ``match_outcomes.sql`` post-E1.5 cutover."""

    def test_two_left_joins_on_silver_xref_team(self):
        """Both home and away resolution use silver.xref_team."""
        sql = _strip_comments(_read_sql())
        joins = re.findall(
            r"LEFT\s+JOIN\s+iceberg\.silver\.xref_team",
            sql, re.IGNORECASE,
        )
        assert len(joins) >= 2, (
            "match_outcomes.sql must LEFT JOIN iceberg.silver.xref_team "
            f"at least twice (home + away); found {len(joins)}"
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "match_outcomes.sql must NOT reference gold.entity_xref in "
            "executable SQL after E1.5 cutover"
        )

    def test_each_join_has_fbref_source_filter(self):
        """Both joins must filter `source = 'fbref'`."""
        sql = _read_sql()
        fbref_filters = re.findall(
            r"\.source\s*=\s*'fbref'", sql, re.IGNORECASE,
        )
        assert len(fbref_filters) >= 2, (
            "match_outcomes.sql must filter `source = 'fbref'` on BOTH "
            f"silver.xref_team joins; found {len(fbref_filters)}"
        )

    def test_each_join_includes_league_predicate(self):
        """Both joins must include `<alias>.league = m.league`."""
        sql = _read_sql()
        league_predicates = re.findall(
            r"\.league\s*=\s*m\.league", sql, re.IGNORECASE,
        )
        assert len(league_predicates) >= 2, (
            "match_outcomes.sql must have a `<alias>.league = m.league` "
            "predicate in BOTH home and away joins; found "
            f"{len(league_predicates)}"
        )

    def test_each_join_casts_season_to_varchar(self):
        """Both joins must CAST(m.season AS varchar)."""
        sql = _read_sql()
        season_casts = re.findall(
            r"CAST\s*\(\s*m\.season\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        )
        assert len(season_casts) >= 2, (
            "match_outcomes.sql must CAST(m.season AS varchar) in BOTH "
            f"home and away joins; found {len(season_casts)}"
        )

    def test_home_and_away_canonical_id_selected(self):
        """SELECT must surface canonical_id from both home_x and away_x."""
        sql = _read_sql()
        assert re.search(
            r"home_x\.canonical_id\s+AS\s+home_team_id",
            sql, re.IGNORECASE,
        ), (
            "match_outcomes.sql must SELECT "
            "`home_x.canonical_id AS home_team_id`"
        )
        assert re.search(
            r"away_x\.canonical_id\s+AS\s+away_team_id",
            sql, re.IGNORECASE,
        ), (
            "match_outcomes.sql must SELECT "
            "`away_x.canonical_id AS away_team_id`"
        )

    def test_targets_unchanged(self):
        """Sanity check: ML targets (result_1x2, btts, total_goals etc.)
        must still be projected — the cutover is id-only and must not
        accidentally drop the labels."""
        sql = _read_sql()
        for col in [
            "result_1x2", "home_win", "draw", "away_win",
            "total_goals", "btts", "over_2_5", "over_3_5",
            "is_completed",
        ]:
            assert re.search(rf"\b{col}\b", sql), (
                f"match_outcomes.sql must keep target column {col!r}"
            )

    def test_migration_breadcrumb_in_header(self):
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql, (
            "match_outcomes.sql must keep the E1.5 migration breadcrumb"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "match_outcomes.sql must remain a pure SELECT"
        )
