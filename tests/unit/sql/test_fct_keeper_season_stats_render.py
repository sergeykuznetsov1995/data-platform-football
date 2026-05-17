"""Render-smoke for ``dags/sql/gold/fct_keeper_season_stats.sql``.

T5: keeper-variant per-season cross-source stats. Структурно идентично
fct_player_season_stats, источники = silver.fbref_keeper_profile +
silver.fotmob_keeper_profile.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_keeper_season_stats.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctKeeperSeasonStatsSql:

    def test_reads_xref_player_and_both_keeper_silver_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.fbref_keeper_profile" in sql
        assert "iceberg.silver.fotmob_keeper_profile" in sql
        # WhoScored event-aggregate подключён за keeper_saves/pickups/claims.
        assert "iceberg.silver.whoscored_player_season_aggregate" in sql
        # outfield-таблицы НЕ должны читаться — keeper-витрина только GK
        assert "fbref_player_season_profile" not in sql
        assert "fotmob_player_season_profile" not in sql

    def test_fbref_spine_filter(self):
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'fbref'", sql, re.IGNORECASE)
        assert re.search(r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE) or \
               re.search(r"confidence\s*!=\s*'orphan'", sql, re.IGNORECASE)

    def test_inner_join_keeper_profile(self):
        """fbref_keeper_profile уже фильтрует pos LIKE '%GK%'; INNER JOIN
        автоматически даёт только вратарей без отдельного WHERE."""
        sql = _read_sql()
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.fbref_keeper_profile",
            sql, re.IGNORECASE,
        ), "fct_keeper_season_stats must use INNER JOIN на fbref_keeper_profile"

    def test_season_slug_to_year_idiom(self):
        sql = _read_sql()
        assert re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        )

    def test_grain_columns_present(self):
        sql = _read_sql()
        for col in ['player_id_canonical', 'league', 'season']:
            assert re.search(rf"\b{col}\b", sql)

    def test_hard_fact_coalesce_columns(self):
        sql = _read_sql()
        # 5 HARD_FACT для keeper: matches/minutes/clean_sheets/yellow_cards/red_cards.
        # Variadic COALESCE — допускает доп. источники (на будущее).
        hard_facts = [
            ('mp', 'matches_played', 'matches'),
            ('minutes', 'minutes_played', 'minutes'),
            ('clean_sheets', 'clean_sheets', 'clean_sheets'),
            ('yellow_cards', 'yellow_cards', 'yellow_cards'),
            ('red_cards', 'red_cards', 'red_cards'),
        ]
        for fb_col, fm_col, alias in hard_facts:
            pattern = (
                rf"COALESCE\s*\(\s*fb\.{fb_col}\s*,\s*fm\.{fm_col}"
                rf"(?:\s*,[^)]+)?\s*\)\s+AS\s+{alias}\b"
            )
            assert re.search(pattern, sql, re.IGNORECASE), (
                f"HARD_FACT `{alias}` must be COALESCE(fb.{fb_col}, fm.{fm_col}, ...)"
            )

    def test_no_audit_diff_columns(self):
        """Audit-diff колонки вынесены в `gold.fct_keeper_season_stats_audit`."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"_diff_fotmob\b", sql), (
            "fct_keeper_season_stats.sql must NOT include audit-diff columns; "
            "they live in fct_keeper_season_stats_audit"
        )

    def test_save_pct_kept_separate(self):
        """FBref save_pct vs FotMob save_percentage хранятся в отдельных колонках
        (без COALESCE) — шкалы могут различаться, COALESCE замаскирует расхождения."""
        sql = _read_sql()
        assert re.search(r"save_pct_fbref\b", sql), (
            "FBref save_pct must be projected as `save_pct_fbref` (no COALESCE)"
        )
        assert re.search(r"save_percentage_fotmob\b", sql), (
            "FotMob save_percentage must be projected as `save_percentage_fotmob`"
        )

    def test_unique_fbref_keeper_columns_present(self):
        sql = _read_sql()
        unique = [
            'goals_against', 'goals_against_per90', 'shots_on_target_against',
            'saves', 'wins', 'draws', 'losses', 'clean_sheet_pct',
            'pk_faced', 'pk_allowed', 'pk_saved', 'pk_missed', 'pk_save_pct',
        ]
        for col in unique:
            assert re.search(rf"fb\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_FBREF keeper column `{col}` must come from fb."
            )

    def test_unique_fotmob_keeper_columns_present(self):
        sql = _read_sql()
        unique = [
            'saves_per_90', 'goals_prevented',
            'accurate_passes_per_90', 'accurate_long_balls_per_90',
            'fotmob_rating',
        ]
        for col in unique:
            assert re.search(rf"fm\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_FOTMOB keeper column `{col}` must come from fm."
            )

    def test_unique_whoscored_keeper_columns_present(self):
        """WhoScored event-aggregate GK-метрики (3 колонки)."""
        sql = _read_sql()
        for col in ['keeper_saves_whoscored', 'keeper_pickups_whoscored',
                    'keeper_claims_whoscored']:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_WHOSCORED keeper column `{col}` must be projected"
            )

    def test_ws_join_uses_season_slug(self):
        """WS LEFT JOIN MUST использовать season_slug (varchar), НЕ season_year."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"ws\.season\s*=\s*xf\.season_slug", sql, re.IGNORECASE), (
            "WhoScored JOIN must use xf.season_slug (varchar), not season_year"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
