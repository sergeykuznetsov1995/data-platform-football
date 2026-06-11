"""
Guard-тесты инлайна per-source season rollups в fct_team_season_stats (#478).

Epic #478 удалил производный gold-этаж, включая 4 промежуточные таблицы
gold.{understat,whoscored,sofascore,fotmob}_team_season. Их агрегации
инлайнены как CTE прямо в fct_team_season_stats.sql (полные) и
fct_team_season_stats_audit.sql (усечённые до diff-колонок, без ws_penalties).

Тесты фиксируют это решение:

* main и audit НЕ читают удалённые gold.*_team_season;
* оба читают silver.*_team_match напрямую; main дополнительно читает
  bronze.whoscored_events (penalties #161), audit — НЕТ (избегаем второго
  скана event-grain таблицы за DAG-ран);
* оба файла парсятся sqlglot'ом как Trino (синтакс-смоук; Trino-специфику
  ловит EXPLAIN (TYPE VALIDATE) при верификации — memory #426);
* schema-freeze: список выходных колонок main-файла заморожен — CREATE OR
  REPLACE сверяет схему позиционно, молчаливый дрейф типов/состава колонок
  ломает консьюмеров (feedback_silver_create_or_replace_positional_schema).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot

REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_SQL = REPO_ROOT / "dags" / "sql" / "gold" / "fct_team_season_stats.sql"
AUDIT_SQL = REPO_ROOT / "dags" / "sql" / "gold" / "fct_team_season_stats_audit.sql"

# Замороженный контракт выходных колонок main-файла (102 шт., порядок важен —
# CREATE OR REPLACE сверяет схему позиционно). Менять только осознанно,
# синхронно с консьюмерами и OM-описанием.
EXPECTED_MAIN_COLUMNS = [
    'team_id', 'league', 'season', 'matches', 'minutes', 'goals',
    'goals_against', 'assists', 'yellow_cards', 'red_cards',
    'second_yellow_cards', 'total_shots', 'shots_on_target',
    'fouls_committed', 'fouls_drawn', 'offsides', 'crosses',
    'interceptions', 'tackles_won', 'penalties_won', 'penalties_conceded',
    'own_goals', 'expected_goals', 'expected_goals_against',
    'expected_assists', 'xpts', 'npxg', 'npxg_against', 'players_used',
    'avg_age', 'possession_pct', 'goals_per_90', 'goals_assists_per_90',
    'non_penalty_goals_per_90', 'shots_per_90', 'shot_on_target_pct',
    'goals_per_shot', 'goals_per_shot_on_target', 'complete_matches',
    'substitutions', 'unused_subs', 'points_per_match', 'on_field_goals',
    'on_field_goals_against', 'plus_minus', 'plus_minus_per_90',
    'gk_goals_against', 'gk_saves', 'gk_shots_on_target_against',
    'clean_sheets', 'gk_minutes', 'save_pct', 'gk_pk_attempts_faced',
    'gk_pk_allowed', 'gk_pk_saved', 'goals_against_per_90', 'ppda', 'oppda',
    'deep_completions', 'deep_completions_allowed', 'wins', 'draws',
    'losses', 'points', 'pass_total', 'pass_ok', 'pass_pct',
    'key_passes_ws', 'takeon_att', 'takeon_won', 'takeon_pct', 'clearances',
    'ball_recoveries', 'touches_in_box', 'defensive_actions_third',
    'set_piece_share_pct', 'total_passes', 'accurate_passes',
    'accurate_passes_pct', 'possession_pct_avg', 'corner_kicks',
    'ground_duels_won', 'ground_duels_total', 'ground_duels_won_pct',
    'aerial_duels_won', 'aerial_duels_total', 'aerial_duels_won_pct',
    'total_duels_won_pct', 'accurate_long_balls', 'total_long_balls',
    'accurate_long_balls_pct', 'accurate_crosses', 'total_crosses', 'xgot',
    'big_chances', 'big_chances_missed', 'shots_inside_box',
    'shots_outside_box', 'squad_market_value_eur', 'total_wage_bill_gbp',
    'total_wage_bill_eur', '_gold_created_at',
]


def _strip_comments(sql: str) -> str:
    """Убрать `-- ...` комментарии, чтобы grep не ловил упоминания в шапках."""
    return re.sub(r"--[^\n]*", "", sql)


@pytest.mark.unit
class TestInlineSourceRefs:
    """Источники после инлайна #478: silver напрямую, gold-rollups — нет."""

    def test_no_gold_team_season_references(self):
        pattern = re.compile(
            r"gold\s*\.\s*(understat|whoscored|sofascore|fotmob)_team_season"
        )
        for path in (MAIN_SQL, AUDIT_SQL):
            body = _strip_comments(path.read_text(encoding="utf-8"))
            assert not pattern.search(body), (
                f"{path.name} ссылается на удалённую gold.*_team_season (#478)"
            )

    def test_silver_team_match_sources_present(self):
        for path in (MAIN_SQL, AUDIT_SQL):
            body = _strip_comments(path.read_text(encoding="utf-8"))
            for src in ("understat", "whoscored", "sofascore", "fotmob"):
                assert f"iceberg.silver.{src}_team_match" in body, (
                    f"{path.name}: ожидается инлайн-CTE над "
                    f"silver.{src}_team_match"
                )

    def test_bronze_events_scan_only_in_main(self):
        """ws_penalties (#161) сканирует bronze.whoscored_events ТОЛЬКО в
        main — аудит penalties не сравнивает, второй скан event-grain
        таблицы за DAG-ран не нужен."""
        main_body = _strip_comments(MAIN_SQL.read_text(encoding="utf-8"))
        audit_body = _strip_comments(AUDIT_SQL.read_text(encoding="utf-8"))
        assert "iceberg.bronze.whoscored_events" in main_body
        assert "iceberg.bronze.whoscored_events" not in audit_body


@pytest.mark.unit
class TestSqlParses:
    """Синтакс-смоук: sqlglot парсит оба файла как Trino."""

    @pytest.mark.parametrize("path", [MAIN_SQL, AUDIT_SQL],
                             ids=["main", "audit"])
    def test_parses_as_trino(self, path):
        stmts = sqlglot.parse(path.read_text(encoding="utf-8"), read="trino")
        assert len(stmts) == 1


@pytest.mark.unit
class TestMainSchemaFreeze:
    """Выходные колонки main-файла — позиционный контракт CREATE OR REPLACE."""

    def test_output_columns_frozen(self):
        tree = sqlglot.parse_one(MAIN_SQL.read_text(encoding="utf-8"),
                                 read="trino")
        actual = [e.alias_or_name for e in tree.selects]
        assert actual == EXPECTED_MAIN_COLUMNS, (
            "Состав/порядок выходных колонок fct_team_season_stats изменился. "
            "Если это осознанно — обнови EXPECTED_MAIN_COLUMNS и проверь "
            "консьюмеров (CREATE OR REPLACE сверяет схему позиционно)."
        )
