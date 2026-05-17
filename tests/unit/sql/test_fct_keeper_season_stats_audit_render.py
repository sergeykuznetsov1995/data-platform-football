"""Render-smoke for ``dags/sql/gold/fct_keeper_season_stats_audit.sql``.

T5 audit (keeper variant): 3 audit-diff (matches/minutes/clean_sheets).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_keeper_season_stats_audit.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctKeeperSeasonStatsAuditSql:

    def test_reads_keeper_silver_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.fbref_keeper_profile" in sql
        assert "iceberg.silver.fotmob_keeper_profile" in sql
        assert "iceberg.silver.whoscored_player_season_aggregate" in sql
        # outfield таблицы НЕ читаются
        assert "fbref_player_season_profile" not in sql
        assert "fotmob_player_season_profile" not in sql

    def test_inner_join_keeper_sources_left_join_ws(self):
        sql = _read_sql()
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.fbref_keeper_profile",
            sql, re.IGNORECASE,
        )
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.fotmob_keeper_profile",
            sql, re.IGNORECASE,
        )
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.whoscored_player_season_aggregate",
            sql, re.IGNORECASE,
        ), "audit must LEFT JOIN на WS (не INNER — сохраняем FBref∩FotMob spine)"

    def test_grain_pk_columns(self):
        sql = _read_sql()
        for col in ['player_id_canonical', 'league', 'season']:
            assert re.search(rf"\b{col}\b", sql)

    def test_audit_diff_columns(self):
        """3 FotMob diffs + 1 WhoScored diff = 4 audit columns total."""
        sql = _read_sql()
        for col in ['matches_diff_fotmob', 'minutes_diff_fotmob',
                    'clean_sheets_diff_fotmob', 'saves_diff_whoscored']:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE)

    def test_no_business_metric_columns(self):
        sql = _strip_comments(_read_sql())
        assert "COALESCE" not in sql.upper()
        for col in ['save_pct_fbref', 'save_percentage_fotmob',
                    'goals_prevented', 'fotmob_rating']:
            assert not re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE)

    def test_season_slug_to_year_idiom(self):
        sql = _read_sql()
        assert re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
