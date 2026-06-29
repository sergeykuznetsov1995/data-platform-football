"""Render-smoke for ``dags/sql/silver/fotmob_player_profile.sql``.

T4: time-invariant snapshot для FotMob атрибутов. team_squad — основной
источник для height/dob/country/country_code; player_information_json —
ТОЛЬКО для `foot` (единственное поле, отсутствующее в team_squad).

Этот файл фиксирует контракт SQL'я (filters, dedup, JSON-extract idiom) на
случай рефактора.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_player_profile.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobPlayerProfileSql:

    def test_reads_team_squad_and_player_details(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_team_squad" in sql, (
            "fotmob_player_profile.sql must read fotmob_team_squad "
            "(primary source for height_cm/dob/country)"
        )
        assert "iceberg.bronze.fotmob_player_details" in sql, (
            "fotmob_player_profile.sql must read fotmob_player_details "
            "(needed for `foot` extraction from JSON)"
        )

    def test_filters_out_coaches(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"NOT\s+is_coach", sql, re.IGNORECASE), (
            "fotmob_player_profile.sql must filter out coaches "
            "(is_coach=true rows pollute the player snapshot)"
        )

    def test_does_not_filter_by_position(self):
        """Snapshot должен покрывать ВСЕХ игроков (включая вратарей) —
        time-invariant атрибуты не зависят от позиции."""
        sql = _strip_comments(_read_sql())
        # silver.fotmob_player_season_profile / fotmob_keeper_profile делят
        # игроков по primary_position; здесь — единая таблица.
        assert "primary_position" not in sql, (
            "fotmob_player_profile.sql NE должна делить игроков по позиции "
            "— это time-invariant snapshot для ВСЕХ игроков"
        )

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        # Two CTEs (details_dedup + squad_dedup) с ROW_NUMBER на одинаковом
        # ключе (player_id, league, season). Pattern: existing fotmob_*_profile.
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+player_id",
            sql, re.IGNORECASE,
        ), "fotmob_player_profile.sql must dedup via ROW_NUMBER OVER (PARTITION BY player_id, ...)"

    def test_foot_extracted_from_player_information_json(self):
        """foot выкручивается через element_at(map_from_entries(transform(...)))
        по title='Preferred foot'. Это единственный валидный pattern в Trino
        (JSONPath filter и correlated subquery с UNNEST не поддерживаются)."""
        sql = _read_sql()
        # Pattern check: map_from_entries + 'Preferred foot' lookup.
        assert "map_from_entries" in sql, (
            "fotmob_player_profile.sql must use map_from_entries idiom "
            "(JSONPath filter и correlated UNNEST не поддерживаются в Trino)"
        )
        assert "'Preferred foot'" in sql, (
            "fotmob_player_profile.sql must look up by title 'Preferred foot'"
        )
        assert "$.value.fallback" in sql, (
            "fotmob_player_profile.sql must extract value.fallback "
            "(human-readable Right/Left/Both)"
        )

    def test_join_key_includes_cast_for_player_id(self):
        """fotmob_team_squad.player_id — bigint, fotmob_player_details.player_id
        — varchar. JOIN-ключ требует CAST на team_squad-стороне."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\s*\(\s*player_id\s+AS\s+VARCHAR\s*\)",
            sql, re.IGNORECASE,
        ), (
            "fotmob_player_profile.sql must CAST team_squad.player_id "
            "AS VARCHAR (bronze type mismatch with details.player_id)"
        )

    def test_outputs_required_attribute_columns(self):
        """Минимальный контракт колонок: identity + 4 time-invariant attrs."""
        sql = _read_sql()
        for col in (
            "player_id",
            "player_name",
            "date_of_birth",
            "height_cm",
            "nationality",
            "country_code",
            "foot",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_player_profile.sql must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fotmob_player_profile.sql must remain a pure SELECT "
            "(CTAS-wrapping is done by silver_tasks.run_silver_transform)"
        )
