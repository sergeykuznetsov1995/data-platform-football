"""Render-smoke для ``dags/sql/silver/fotmob_match_referee.sql``.

Issue #290: материализуем СТРАНУ судьи из FotMob (FBref/MatchHistory её не дают).
Этот файл фиксирует контракт SQL'я (источник, dedup, JSON-extract пути, колонки)
на случай рефактора.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_match_referee.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobMatchRefereeSql:

    def test_reads_match_details_bronze(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_match_details" in sql, (
            "fotmob_match_referee.sql must read bronze.fotmob_match_details"
        )

    def test_extracts_all_three_referee_json_paths(self):
        """text + country + countryCode — страна судьи это смысл задачи #290."""
        sql = _strip_comments(_read_sql())
        for path in (
            "$.infoBox.Referee.text",
            "$.infoBox.Referee.country",
            "$.infoBox.Referee.countryCode",
        ):
            assert path in sql, (
                f"fotmob_match_referee.sql must extract `{path}`"
            )

    def test_dedup_via_row_number_on_match_grain(self):
        """Bronze хранит N снимков на матч → дедуп до последнего по _ingested_at."""
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+match_id",
            sql, re.IGNORECASE,
        ), (
            "fotmob_match_referee.sql must dedup via "
            "ROW_NUMBER OVER (PARTITION BY match_id, league, season)"
        )
        assert re.search(r"ORDER\s+BY\s+_ingested_at\s+DESC", sql, re.IGNORECASE), (
            "dedup must keep latest snapshot (ORDER BY _ingested_at DESC)"
        )
        assert re.search(r"\brn\s*=\s*1\b", sql), (
            "fotmob_match_referee.sql must filter to the latest snapshot (rn = 1)"
        )

    def test_filters_out_empty_referee_name(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"Referee\.text'\)\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE), (
            "fotmob_match_referee.sql must drop rows without a referee name"
        )

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "match_id",
            "league",
            "season",
            "referee_name",
            "referee_country",
            "referee_country_code",
            "_bronze_ingested_at",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_match_referee.sql must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fotmob_match_referee.sql must remain a pure SELECT "
            "(CTAS-wrapping is done by silver_tasks.run_silver_transform)"
        )
