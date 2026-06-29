"""Render-smoke for ``dags/sql/gold/fct_player_season_stats_audit.sql``.

T5 audit: DQ-таблица для cross-source согласованности FBref vs FotMob.
INNER JOIN на оба источника → rows только где обе стороны не-NULL.
Не business-витрина: ТОЛЬКО PK + 6 FotMob diff-колонок + lineage.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_season_stats_audit.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctPlayerSeasonStatsAuditSql:

    def test_reads_xref_and_both_silver_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.fbref_player_season_profile" in sql
        assert "iceberg.silver.fotmob_player_season_profile" in sql
        assert "iceberg.silver.whoscored_player_season_aggregate" in sql
        assert "iceberg.silver.understat_player_season_aggregate" in sql

    def test_inner_join_fbref_fotmob_left_join_ws_us(self):
        """FBref + FotMob — INNER JOIN (классический FotMob audit subset).
        WhoScored + Understat — LEFT JOIN: добавочные diffs, не сужают spine."""
        sql = _read_sql()
        # #463: FBref spine идёт через fb_dedup CTE (max-minutes club);
        # raw silver profile читается внутри CTE.
        assert re.search(
            r"INNER\s+JOIN\s+fb_dedup\s+fb\b",
            sql, re.IGNORECASE,
        ), "audit must INNER JOIN fb_dedup (FBref spine)"
        assert "iceberg.silver.fbref_player_season_profile" in sql, (
            "fb_dedup CTE must read silver.fbref_player_season_profile"
        )
        # FotMob INNER-джойнится через CTE fotmob_counts (per-90 → count recompute,
        # issue #174); CTE читает iceberg.silver.fotmob_player_season_profile.
        assert re.search(
            r"INNER\s+JOIN\s+fotmob_counts",
            sql, re.IGNORECASE,
        ), "audit must INNER JOIN на fotmob_counts (FotMob subset)"
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.whoscored_player_season_aggregate",
            sql, re.IGNORECASE,
        ), "audit must LEFT JOIN на WhoScored (не INNER — сохраняем FotMob spine)"
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.understat_player_season_aggregate",
            sql, re.IGNORECASE,
        ), "audit must LEFT JOIN на Understat"

    def test_grain_pk_columns(self):
        """PK совпадает с main fct: (player_id, league, season)."""
        sql = _read_sql()
        for col in ['player_id', 'league', 'season']:
            assert re.search(rf"\b{col}\b", sql)

    def test_audit_diff_columns(self):
        """13 audit-diff: 6 FotMob + 1 WhoScored + 6 Understat."""
        sql = _read_sql()
        audit_cols = [
            # FotMob (6; #564: penalties_won/conceded удалены — FotMob не
            # отдаёт сезонные пенальти, колонки были полностью NULL)
            'matches_diff_fotmob',
            'minutes_diff_fotmob',
            'goals_diff_fotmob',
            'assists_diff_fotmob',
            'yellow_cards_diff_fotmob',
            'red_cards_diff_fotmob',
            # WhoScored (1; только matches есть в event-aggregate)
            'matches_diff_whoscored',
            # Understat (6)
            'matches_diff_understat',
            'minutes_diff_understat',
            'goals_diff_understat',
            'assists_diff_understat',
            'yellow_cards_diff_understat',
            'red_cards_diff_understat',
        ]
        for col in audit_cols:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"audit-diff column `{col}` must be projected"
            )

    def test_no_business_metric_columns(self):
        """Audit-таблица содержит ТОЛЬКО diff + PK + lineage. Никаких
        standalone business-метрик. COALESCE допустим ТОЛЬКО внутри FotMob
        diff-выражений (#564: NULL=«не было события»→0), не как отдельная метрика."""
        sql = _strip_comments(_read_sql())
        for line in sql.splitlines():
            if "COALESCE" in line.upper():
                assert "diff_fotmob" in line.lower(), (
                    "COALESCE разрешён только внутри FotMob diff-выражений, "
                    f"не как business-метрика: {line.strip()}"
                )
        # UNIQUE_FBREF / UNIQUE_FOTMOB не должны быть выпроецированы.
        for col in ['expected_goals', 'fotmob_rating', 'complete_matches',
                    'big_chances_created']:
            assert not re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"audit-таблица не должна содержать business-метрику `{col}`"
            )

    def test_fotmob_count_diffs_coalesce_to_zero(self):
        """#564: разреженные FotMob счётчики COALESCE→0 (иначе diff=NULL
        съедает ~38% пар). penalties_won/conceded удалены — FotMob не отдаёт
        сезонные пенальти (колонки были полностью NULL)."""
        sql = _strip_comments(_read_sql())
        for metric in ['goals', 'assists', 'yellow_cards', 'red_cards']:
            assert re.search(
                rf"COALESCE\(fm\.{metric},\s*0\)", sql, re.IGNORECASE,
            ), f"FotMob `{metric}` diff должен COALESCE→0 (#564)"
        # matches/minutes НЕ coalesce'им (NULL ≠ 0).
        for raw in ['matches_played', 'minutes_played']:
            assert not re.search(
                rf"COALESCE\(fm\.{raw}", sql, re.IGNORECASE,
            ), f"FotMob `{raw}` НЕ должен coalesce'иться (NULL ≠ 0)"
        for dropped in ['penalties_won_diff_fotmob',
                        'penalties_conceded_diff_fotmob']:
            assert not re.search(rf"\bAS\s+{dropped}\b", sql, re.IGNORECASE), (
                f"`{dropped}` должен быть удалён (#564)"
            )

    def test_outfield_filter_excludes_keepers(self):
        """Симметрично с main fct: outfield-only audit, GK исключены."""
        sql = _read_sql()
        assert re.search(
            r"pos\s+NOT\s+LIKE\s+'%GK%'", sql, re.IGNORECASE,
        )

    def test_season_slug_to_year_idiom(self):
        # #404: season is slug end-to-end — the slug→year-start idiom is removed.
        sql = _read_sql()
        assert not re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season",
            sql, re.IGNORECASE,
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
