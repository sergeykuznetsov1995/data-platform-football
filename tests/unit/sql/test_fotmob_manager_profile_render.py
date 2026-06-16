"""Render-smoke for ``dags/sql/silver/fotmob_manager_profile.sql`` (issue #434).

Mirror of ``fotmob_player_profile.sql`` but for COACHES (``is_coach`` /
``role='coach'``). Feeds gold.dim_manager nationality/dob enrichment. This file
freezes the SQL contract (coach filters, dedup, dob COALESCE, coachId key) so a
refactor cannot silently drop the manager-enrichment source.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_manager_profile.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobManagerProfileSql:

    def test_reads_team_squad_and_player_details(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_team_squad" in sql, (
            "fotmob_manager_profile.sql must read fotmob_team_squad "
            "(source for coach country/dob)"
        )
        assert "iceberg.bronze.fotmob_player_details" in sql, (
            "fotmob_manager_profile.sql must read fotmob_player_details "
            "(driver: coachId matching xref_manager.source_id)"
        )

    def test_filters_for_coaches_only(self):
        """Inverse of fotmob_player_profile: keep coaches, drop players."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"WHERE\s+is_coach", sql, re.IGNORECASE), (
            "must drive on fotmob_player_details WHERE is_coach"
        )
        assert re.search(r"role\s*=\s*'coach'", sql, re.IGNORECASE), (
            "must filter fotmob_team_squad on role='coach'"
        )
        # Guard against accidentally copying the player filter.
        assert not re.search(r"NOT\s+is_coach", sql, re.IGNORECASE), (
            "must NOT carry the player-profile `NOT is_coach` filter"
        )

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+player_id",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY player_id, ...)"

    def test_join_key_casts_player_id(self):
        """team_squad.player_id is bigint; details.player_id is varchar."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\s*\(\s*player_id\s+AS\s+VARCHAR\s*\)",
            sql, re.IGNORECASE,
        ), "must CAST team_squad.player_id AS VARCHAR (bronze type mismatch)"

    def test_dob_coalesces_squad_then_details(self):
        """dob prefers team_squad.date_of_birth, falls back to birth_date."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"COALESCE\s*\(\s*s\.date_of_birth\s*,\s*d\.birth_date\s*\)",
            sql, re.IGNORECASE,
        ), "dob must be COALESCE(squad.date_of_birth, details.birth_date)"

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "player_id",
            "name",
            "date_of_birth",
            "nationality",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_manager_profile.sql must project `{col}`"
            )

    def test_season_slug_conversion(self):
        """FotMob bronze season is year-start bigint -> 4-char slug."""
        sql = _strip_comments(_read_sql())
        assert "LPAD" in sql and "MOD" in sql, (
            "season must be converted to slug (LPAD/MOD idiom)"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS-wrapping is run_silver_transform's job)"
        )
