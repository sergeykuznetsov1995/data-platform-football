"""Render-smoke for ``dags/sql/gold/fct_player_season_stats.sql``.

T5: cross-source per-season stats per canonical player. FBref-spine + FotMob
bridge через silver.xref_player. Outfield only (вратари в
fct_keeper_season_stats). Wide single-column для overlap (FBref primary,
COALESCE FotMob); UNIQUE_FBREF / UNIQUE_FOTMOB — own column; audit
`<metric>_diff_fotmob` для калибровки расхождений.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_season_stats.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctPlayerSeasonStatsSql:

    def test_reads_xref_player_and_both_silver_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.fbref_player_season_profile" in sql
        assert "iceberg.silver.fotmob_player_season_profile" in sql
        assert "iceberg.silver.whoscored_player_season_aggregate" in sql
        assert "iceberg.silver.understat_player_season_aggregate" in sql

    def test_fbref_spine_filter(self):
        """Spine = (canonical_id, league, season) FBref-only из xref_player."""
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'fbref'", sql, re.IGNORECASE), (
            "spine must filter xref_player to source='fbref'"
        )
        assert re.search(r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE) or \
               re.search(r"confidence\s*!=\s*'orphan'", sql, re.IGNORECASE), (
            "spine must exclude FBref orphan rows"
        )

    def test_outfield_filter_excludes_keepers(self):
        """fbref_player_season_profile содержит ВСЕХ игроков (нет filter pos);
        outfield-витрина должна явно исключать GK."""
        sql = _read_sql()
        assert re.search(
            r"pos\s+NOT\s+LIKE\s+'%GK%'", sql, re.IGNORECASE,
        ), "fct_player_season_stats must exclude GK (pos NOT LIKE '%GK%')"

    def test_xref_join_includes_league_and_season_predicate(self):
        """CLAUDE.md «xref JOIN must include (league, season) predicate»:
        silver.xref_player имеет per-(source, source_id, season) rows;
        без season-condition fan-out 1.5-4×."""
        sql = _strip_comments(_read_sql())
        # Ищем bridge-JOIN на xref_fotmob CTE (FotMob bridge)
        # и убеждаемся что там league и season_year предикаты присутствуют.
        # Эвристика: bridge JOIN определяется по слову xref_fotmob+ON+league+season.
        assert re.search(r"xfm\.league", sql), (
            "xref_fotmob bridge JOIN must include league predicate"
        )
        assert re.search(r"xfm\.season_year", sql), (
            "xref_fotmob bridge JOIN must include season_year predicate"
        )

    def test_season_slug_to_year_idiom(self):
        """xref slug '2526' → bigint 2025 идиомой
        `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)`."""
        sql = _read_sql()
        assert re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        ), (
            "fct_player_season_stats.sql must convert xref season slug ('2526') "
            "to bigint year (2025) using SUBSTR idiom"
        )

    def test_grain_columns_present(self):
        sql = _read_sql()
        for col in ['player_id_canonical', 'league', 'season']:
            assert re.search(rf"\b{col}\b", sql), (
                f"PK column `{col}` must be projected"
            )

    def test_eight_hard_fact_coalesce_columns(self):
        """8 HARD_FACT overlap метрик публикуются single-column через COALESCE
        (FBref primary, FotMob+WS+US fallback). Цепочка variadic — regex
        принимает 2-arg (penalties_*) и 3/4-arg для остальных."""
        sql = _read_sql()
        hard_facts = [
            ('mp', 'matches_played', 'matches'),
            ('minutes', 'minutes_played', 'minutes'),
            ('goals', 'goals', 'goals'),
            ('assists', 'assists', 'assists'),
            ('yellow_cards', 'yellow_cards', 'yellow_cards'),
            ('red_cards', 'red_cards', 'red_cards'),
            ('penalties_won', 'penalties_won', 'penalties_won'),
            ('penalties_conceded', 'penalties_conceded', 'penalties_conceded'),
        ]
        for fb_col, fm_col, alias in hard_facts:
            # Variadic COALESCE: FBref primary, FotMob fallback, + optional
            # additional fallbacks (ws.*, us.*). Закрывающая скобка может
            # стоять сразу после fm.<col> или после доп. источников.
            pattern = (
                rf"COALESCE\s*\(\s*fb\.{fb_col}\s*,\s*fm\.{fm_col}"
                rf"(?:\s*,[^)]+)?\s*\)\s+AS\s+{alias}\b"
            )
            assert re.search(pattern, sql, re.IGNORECASE), (
                f"HARD_FACT `{alias}` must be COALESCE(fb.{fb_col}, fm.{fm_col}, ...)"
            )

    def test_no_audit_diff_columns(self):
        """Audit-diff колонки вынесены в отдельную таблицу
        `gold.fct_player_season_stats_audit` чтобы не загромождать business-витрину.
        Основная fct НЕ должна содержать `<metric>_diff_fotmob` (комментарии
        со ссылкой на audit-таблицу — допустимы)."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"_diff_fotmob\b", sql), (
            "fct_player_season_stats.sql must NOT include audit-diff columns; "
            "they live in fct_player_season_stats_audit"
        )

    def test_unique_fbref_columns_present(self):
        """UNIQUE_FBREF метрики должны идти как single column из FBref."""
        sql = _read_sql()
        unique_fbref = [
            'complete_matches', 'starts', 'subs', 'plus_minus',
            'points_per_match', 'on_off_impact',
            'shots', 'shots_on_target', 'goals_per_shot',
            'crosses', 'offsides', 'own_goals',
        ]
        for col in unique_fbref:
            assert re.search(rf"fb\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_FBREF column `{col}` must come from fb."
            )

    def test_unique_fotmob_columns_present(self):
        """UNIQUE_FOTMOB метрики должны идти как single column из FotMob."""
        sql = _read_sql()
        unique_fotmob = [
            'expected_goals', 'expected_assists', 'expected_goals_on_target',
            'big_chances_created', 'big_chances_missed', 'chances_created',
            'fotmob_rating',
            'defensive_actions_per_90', 'clearances_per_90', 'recoveries_per_90',
            'blocks_per_90', 'accurate_passes_per_90', 'accurate_long_balls_per_90',
            'successful_dribbles_per_90',
        ]
        for col in unique_fotmob:
            assert re.search(rf"fm\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_FOTMOB column `{col}` must come from fm."
            )

    def test_unique_whoscored_columns_present(self):
        """UNIQUE_WHOSCORED метрики — single column из silver WS aggregate."""
        sql = _read_sql()
        unique_ws = [
            'dribbles_whoscored', 'take_on_pct_whoscored', 'bad_touches_whoscored',
            'pass_pct_whoscored', 'tackles_won_whoscored', 'tackle_pct_whoscored',
            'interceptions_whoscored', 'ball_recoveries_whoscored',
            'clearances_whoscored', 'fouls_committed_whoscored',
            'touches_in_box_whoscored', 'avg_x_whoscored', 'avg_y_whoscored',
        ]
        for col in unique_ws:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_WHOSCORED column `{col}` must be projected"
            )

    def test_unique_understat_columns_present(self):
        """UNIQUE_UNDERSTAT метрики — xG/xA/build-up. xG/xA с suffix
        `_understat` чтобы не пересекаться с FotMob expected_goals."""
        sql = _read_sql()
        unique_us = [
            'expected_goals_understat', 'expected_assists_understat',
            'non_penalty_goals_understat', 'non_penalty_xg',
            'xg_chain', 'xg_buildup',
            'key_passes_understat', 'shots_understat',
        ]
        for col in unique_us:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_UNDERSTAT column `{col}` must be projected"
            )

    def test_ws_us_join_uses_season_slug(self):
        """WS/US LEFT JOIN'ы MUST использовать season_slug (varchar '2526'),
        НЕ season_year (bigint 2025). Иначе type mismatch и 0 совпадений."""
        sql = _strip_comments(_read_sql())
        # ws.season = xf.season_slug AND ws.league = xf.league
        assert re.search(r"ws\.season\s*=\s*xf\.season_slug", sql, re.IGNORECASE), (
            "WhoScored JOIN must use xf.season_slug (varchar), not season_year"
        )
        assert re.search(r"ws\.league\s*=\s*xf\.league", sql, re.IGNORECASE), (
            "WhoScored JOIN must include league predicate"
        )
        assert re.search(r"us\.season\s*=\s*xf\.season_slug", sql, re.IGNORECASE), (
            "Understat JOIN must use xf.season_slug (varchar), not season_year"
        )
        assert re.search(r"us\.league\s*=\s*xf\.league", sql, re.IGNORECASE), (
            "Understat JOIN must include league predicate"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fct_player_season_stats.sql must remain a pure SELECT "
            "(CTAS wrapping is done by gold_tasks.run_gold_transform)"
        )
