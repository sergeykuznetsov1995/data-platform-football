"""
Unit tests for ``dags/sql/gold/fct_player_match.sql`` after E1.5 cutover
(2026-05-09).

T2 migrated team resolution from ``gold.entity_xref`` to
``silver.xref_team`` and applied the ``'fb_'`` prefix to player_id (in
line with the silver.xref_player canonical convention used by
dim_player). The xref_team JOIN includes the (league, season) predicate
to prevent the documented 1.5-4x JOIN fan-out.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctPlayerMatchCutoverStructure:
    """Regex sanity over ``fct_player_match.sql`` post-E1.5 cutover."""

    def test_join_on_silver_xref_team(self):
        """LEFT JOIN must hit silver.xref_team, not gold.entity_xref."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.xref_team",
            sql, re.IGNORECASE,
        ), (
            "fct_player_match.sql must LEFT JOIN iceberg.silver.xref_team "
            "after E1.5 cutover"
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "fct_player_match.sql must NOT reference gold.entity_xref "
            "in executable SQL"
        )

    def test_join_filters_fbref_source(self):
        """JOIN must filter `source = 'fbref'`."""
        sql = _read_sql()
        assert re.search(
            r"\.source\s*=\s*'fbref'", sql, re.IGNORECASE,
        ), "fct_player_match.sql must filter `source = 'fbref'` on the xref join"

    def test_join_includes_league_predicate(self):
        """JOIN includes `<alias>.league = pms.league` to prevent fan-out."""
        sql = _read_sql()
        assert re.search(
            r"\.league\s*=\s*pms\.league", sql, re.IGNORECASE,
        ), (
            "fct_player_match.sql JOIN must include "
            "`<alias>.league = pms.league` predicate"
        )

    def test_join_casts_pms_season_to_varchar(self):
        """silver.xref_team.season is varchar; pms.season is bigint."""
        sql = _read_sql()
        assert re.search(
            r"CAST\s*\(\s*pms\.season\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        ), (
            "fct_player_match.sql must CAST(pms.season AS varchar) in the "
            "xref_team JOIN — the silver xref season column is varchar"
        )

    def test_fb_prefix_on_player_id(self):
        """player_id must be projected with the ``'fb_'`` canonical prefix
        for consistency with dim_player + silver.xref_player."""
        sql = _read_sql()
        pipes = re.search(
            r"'fb_'\s*\|\|\s*pms\.player_id\s+AS\s+player_id",
            sql, re.IGNORECASE,
        )
        concat = re.search(
            r"CONCAT\s*\(\s*'fb_'\s*,\s*pms\.player_id\s*\)\s+AS\s+player_id",
            sql, re.IGNORECASE,
        )
        assert pipes or concat, (
            "fct_player_match.sql must apply the silver.xref_player FBref "
            "canonical-id prefix: `'fb_' || pms.player_id AS player_id` "
            "(or CONCAT equivalent)"
        )

    def test_team_id_from_xref_canonical(self):
        """team_id surface must come from the xref alias canonical_id."""
        sql = _read_sql()
        assert re.search(
            r"\.canonical_id\s+AS\s+team_id", sql, re.IGNORECASE,
        ), (
            "fct_player_match.sql must SELECT `<alias>.canonical_id AS team_id`"
        )

    def test_migration_breadcrumb_in_header(self):
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql, (
            "fct_player_match.sql must keep the E1.5 migration breadcrumb"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fct_player_match.sql must remain a pure SELECT"
        )
