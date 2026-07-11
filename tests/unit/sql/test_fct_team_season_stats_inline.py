"""
Guard-тесты инлайна per-source season rollups в fct_team_season_stats (#478).

Epic #478 удалил производный gold-этаж, включая 4 промежуточные таблицы
gold.{understat,whoscored,sofascore,fotmob}_team_season. Их агрегации
инлайнены как CTE прямо в fct_team_season_stats.sql.j2 (полные) и
fct_team_season_stats_audit.sql (усечённые до diff-колонок, без ws_penalties).

Тесты фиксируют это решение:

* main и audit НЕ читают удалённые gold.*_team_season;
* оба читают silver.*_team_match напрямую; main дополнительно читает
  silver.whoscored_events_spadl (penalties #161), audit — НЕТ (избегаем второго
  скана event-grain таблицы за DAG-ран);
* оба файла парсятся sqlglot'ом как Trino (синтакс-смоук; Trino-специфику
  ловит EXPLAIN (TYPE VALIDATE) при верификации — memory #426);
* schema-freeze: список выходных колонок main-файла заморожен — CREATE OR
  REPLACE сверяет схему позиционно, молчаливый дрейф типов/состава колонок
  ломает консьюмеров (feedback_silver_create_or_replace_positional_schema);
* структурный паритет близнеца (#740): ВСЕ общие CTE main∩audit держатся
  синхронными динамически (TestAuditCteSync, без хардкод-списка), а каждая
  cross-source метрика main имеет diff-колонку в audit либо числится в
  задокументированном allowlist пропусков (TestAuditDiffCoverage).

#542: main теперь .sql.j2 — его cross-source COALESCE рендерятся из
configs/medallion/source_priority.yaml. Рендерим перед проверками
(``_main_body``); audit остаётся обычным .sql (``_audit_body``).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest
import sqlglot

REPO_ROOT = Path(__file__).resolve().parents[3]

_DAGS_DIR = REPO_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(REPO_ROOT / "configs" / "medallion")
)

MAIN_SQL = REPO_ROOT / "dags" / "sql" / "gold" / "fct_team_season_stats.sql.j2"
AUDIT_SQL = REPO_ROOT / "dags" / "sql" / "gold" / "fct_team_season_stats_audit.sql"


def _main_body() -> str:
    """Rendered main SQL (#542: .sql.j2 → source_priority.yaml COALESCE)."""
    from utils.medallion_config import render_fact_sql
    return render_fact_sql(MAIN_SQL, "fct_team_season_stats")


def _audit_body() -> str:
    """Audit stays a plain .sql (out of #542 scope)."""
    return AUDIT_SQL.read_text(encoding="utf-8")


# Замороженный контракт выходных колонок main-файла (117 шт., порядок важен —
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
    'total_wage_bill_eur',
    # Direct Capology team-finance (#643)
    'declared_payroll_gross_gbp', 'declared_payroll_gross_eur',
    'declared_payroll_net_gbp', 'declared_payroll_net_eur',
    'declared_payroll_adjusted_gross_gbp', 'declared_payroll_adjusted_gross_eur',
    'transfer_income_gbp', 'transfer_income_eur',
    'transfer_expense_gbp', 'transfer_expense_eur',
    'transfer_balance_gbp', 'transfer_balance_eur',
    'squad_size', 'avg_squad_age', 'foreign_count',
    '_gold_created_at',
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
        for name, body in (("main", _main_body()), ("audit", _audit_body())):
            assert not pattern.search(_strip_comments(body)), (
                f"{name} ссылается на удалённую gold.*_team_season (#478)"
            )

    def test_silver_team_match_sources_present(self):
        for name, body in (("main", _main_body()), ("audit", _audit_body())):
            stripped = _strip_comments(body)
            for src in ("understat", "whoscored", "sofascore", "fotmob"):
                assert f"iceberg.silver.{src}_team_match" in stripped, (
                    f"{name}: ожидается инлайн-CTE над "
                    f"silver.{src}_team_match"
                )

    def test_ws_penalties_reads_silver_not_bronze(self):
        """#736: ws_penalties (#161) теперь one-hop — читает
        silver.whoscored_events_spadl через audit-колонки (_action_source_note /
        qualifiers_raw), а НЕ bronze.whoscored_events. Audit-файл penalties
        вообще не считает (ни bronze, ни этот silver-скан)."""
        main_body = _strip_comments(_main_body())
        audit_body = _strip_comments(_audit_body())
        assert "iceberg.silver.whoscored_events_spadl" in main_body
        assert "iceberg.bronze.whoscored_events" not in main_body
        assert "iceberg.bronze.whoscored_events" not in audit_body
        assert "iceberg.silver.whoscored_events_spadl" not in audit_body


@pytest.mark.unit
class TestSqlParses:
    """Синтакс-смоук: sqlglot парсит оба файла как Trino."""

    @pytest.mark.parametrize("getter", [_main_body, _audit_body],
                             ids=["main", "audit"])
    def test_parses_as_trino(self, getter):
        stmts = sqlglot.parse(getter(), read="trino")
        assert len(stmts) == 1


@pytest.mark.unit
class TestMainSchemaFreeze:
    """Выходные колонки main-файла — позиционный контракт CREATE OR REPLACE."""

    def test_output_columns_frozen(self):
        tree = sqlglot.parse_one(_main_body(), read="trino")
        actual = [e.alias_or_name for e in tree.selects]
        assert actual == EXPECTED_MAIN_COLUMNS, (
            "Состав/порядок выходных колонок fct_team_season_stats изменился. "
            "Если это осознанно — обнови EXPECTED_MAIN_COLUMNS и проверь "
            "консьюмеров (CREATE OR REPLACE сверяет схему позиционно)."
        )


# CTE, чьи проекции audit обязан держать синхронными с main: xref_*/ws_name_to_id
# копируются байт-в-байт, per-source rollups (us/ws/fm_team_season) — усечены до
# diff-колонок (#478). Раньше это был хардкод-список — сам по себе manual-sync
# риск (#740): добавил общий CTE → забыл внести в список → дрейф не ловится.
# Теперь набор берётся ДИНАМИЧЕСКИ как общие CTE обоих файлов минус явно
# расходящиеся (_DIVERGENT_CTES).
#
# ss_team_season исключён: в main он двухступенчатый (ss_match_rollup →
# ss_team_season), в audit одноступенчатый — построчного соответствия нет
# (docs/decisions/season-audit-inline.md).
_DIVERGENT_CTES = {"ss_team_season"}

# CTE, которые ОБЯЗАНЫ попасть в покрытие — guard против вакуумного схлопывания
# набора при рефакторе парсинга (#740). ws_name_to_id особенно: в #705 там
# вручную правился double-cast в обоих файлах, прежний тест бы это не поймал.
_MUST_COVER_CTES = {
    "xref_fbref", "xref_ws", "xref_ss", "xref_fm", "ws_name_to_id",
}


def _cte_names(sql_body: str) -> set:
    """Имена всех CTE верхнего уровня в SQL-теле (для intersection main∩audit)."""
    tree = sqlglot.parse_one(sql_body, read="trino")
    return {cte.alias_or_name for cte in tree.find_all(sqlglot.exp.CTE)}


def _shared_synced_ctes() -> list:
    """Общие CTE main ∩ audit (паритет обязателен), минус расходящиеся."""
    shared = _cte_names(_main_body()) & _cte_names(_audit_body())
    return sorted(shared - _DIVERGENT_CTES)


# Вычисляем один раз на загрузке модуля — parametrize требует список на этапе
# сбора тестов.
_SYNCED_CTES = _shared_synced_ctes()


def _cte_proj_map(sql_body: str, cte_name: str) -> dict:
    """{alias: <выражение без алиаса, нормализованное sqlglot>} для проекций CTE.

    sqlglot канонизирует форматирование, поэтому сравнение устойчиво к
    выравниванию/пробелам; comments=False отбрасывает `-- ...` внутри CTE.
    """
    tree = sqlglot.parse_one(sql_body, read="trino")
    for cte in tree.find_all(sqlglot.exp.CTE):
        if cte.alias_or_name == cte_name:
            # UNION-bodied CTE (ws_name_to_id): проекции живут на ЛЕВОМ SELECT —
            # оба плеча несут одинаковые алиасы/выражения по построению. find()
            # отдаёт сам узел для plain-SELECT и левый SELECT для set-операции.
            select_node = cte.this.find(sqlglot.exp.Select)
            out = {}
            for e in select_node.expressions:
                expr = e.this if isinstance(e, sqlglot.exp.Alias) else e
                out[e.alias_or_name] = expr.sql(dialect="trino", comments=False)
            return out
    raise AssertionError(f"CTE {cte_name} не найден")


@pytest.mark.unit
class TestAuditCteSync:
    """#478/#556/#740: каждый общий CTE main∩audit — подмножество main,
    выражения общих колонок совпадают. Набор вычисляется динамически
    (_SYNCED_CTES = _shared_synced_ctes()), поэтому новый дословно-копируемый
    CTE покрывается автоматически. Закрепляет ручное ⚠️-предупреждение
    проверяемым инвариантом (docs/decisions/season-audit-inline.md)."""

    def test_synced_set_covers_known_ctes(self):
        """Guard: динамический набор не схлопнулся вакуумно (#740). xref-мосты и
        ws_name_to_id обязаны проверяться — иначе регрессия их рассинхрона
        пройдёт незамеченной (как было бы до #740)."""
        missing = _MUST_COVER_CTES - set(_SYNCED_CTES)
        assert not missing, (
            f"Паритет-набор не покрывает {sorted(missing)} — рефактор парсинга "
            f"CTE схлопнул _SYNCED_CTES? Проверь _cte_names/_shared_synced_ctes "
            f"(#740: xref_*/ws_name_to_id обязаны быть в покрытии)."
        )

    @pytest.mark.parametrize("cte_name", _SYNCED_CTES)
    def test_audit_cte_subset_of_main(self, cte_name):
        main_map = _cte_proj_map(_main_body(), cte_name)
        audit_map = _cte_proj_map(_audit_body(), cte_name)
        assert audit_map, f"{cte_name}: пустой/отсутствующий CTE в audit"
        for alias, audit_expr in audit_map.items():
            assert alias in main_map, (
                f"{cte_name}.{alias} есть в audit, нет в main — дрейф CTE (#478). "
                f"Синхронизировать с fct_team_season_stats.sql.j2."
            )
            assert audit_expr == main_map[alias], (
                f"{cte_name}.{alias} разошёлся: audit={audit_expr!r} "
                f"main={main_map[alias]!r}. Синхронизировать усечённый CTE с "
                f"fct_team_season_stats.sql.j2 (#478)."
            )


# Карта между именами cross-source метрик в source_priority.yaml (ключи
# fct_team_season_stats) и «базами» diff-колонок audit. Audit историч.
# использует короткие имена: total_shots→shots, expected_goals→xg (#740).
_AUDIT_NAME_MAP = {
    "expected_goals": "xg",
    "expected_goals_against": "xg_against",
    "total_shots": "shots",
}

# HARD_FACT/MODELED метрики main, НАМЕРЕННО не аудируемые в близнеце, с
# обоснованием. Превращает текстовый «KNOWN GAP» из шапки audit-файла (#705) в
# проверяемый allowlist: новая метрика без diff упадёт в тесте, пока её сюда не
# внесут осознанно.
_AUDIT_INTENTIONAL_GAPS = {
    "penalties_won": (
        "KNOWN GAP #705: FBref убрал PKwon/PKcon с сезона 25/26 → нет "
        "FBref-baseline для diff (audit-конвенция = FBref − source); audit "
        "Silver-only и намеренно НЕ считает ws_penalties (event-grain скан)."
    ),
    "penalties_conceded": "KNOWN GAP #705: см. penalties_won.",
    "minutes": (
        "не входит в diff-набор audit: ≈ mp×90, низкий самостоятельный "
        "cross-source DQ-сигнал (matches уже аудируется)."
    ),
    "npxg": (
        "не входит в diff-набор audit: xG-семейство сверяется через xg / "
        "xg_against (modeled us-vs-ss / us-vs-fm)."
    ),
    "touches_in_box": (
        "не входит в diff-набор audit: WS NULL текущих сезонов (#120), "
        "FotMob-only fallback — нет FBref-spine колонки для сверки."
    ),
}


def _audit_diff_bases() -> set:
    """Базовые метрики diff-колонок audit: `goals_diff_sofascore` → `goals`,
    `xg_diff_us_vs_ss` → `xg`. Колонки без `_diff_` (PK/lineage) отбрасываются."""
    tree = sqlglot.parse_one(_audit_body(), read="trino")
    return {
        sel.alias_or_name.split("_diff_")[0]
        for sel in tree.selects
        if "_diff_" in sel.alias_or_name
    }


def _audited_metrics() -> set:
    """Cross-source HARD_FACT/MODELED метрики main = ключи fct_team_season_stats
    в source_priority.yaml. Single-source/finance колонки сюда не попадают по
    построению — у них нет cross-source diff."""
    from utils.medallion_config import load_source_priority
    return set(load_source_priority()["fct_team_season_stats"].keys())


@pytest.mark.unit
class TestAuditDiffCoverage:
    """#740: каждая cross-source метрика main (source_priority.yaml) имеет
    diff-колонку в audit-близнеце ИЛИ числится в задокументированном allowlist
    пропусков. Ловит «добавил merge-метрику в main, забыл diff в audit»."""

    def test_every_metric_has_diff_or_documented_gap(self):
        bases = _audit_diff_bases()
        for metric in sorted(_audited_metrics()):
            base = _AUDIT_NAME_MAP.get(metric, metric)
            assert base in bases or metric in _AUDIT_INTENTIONAL_GAPS, (
                f"Метрика main {metric!r} (source_priority.yaml) не имеет "
                f"diff-колонки в fct_team_season_stats_audit.sql и не внесена в "
                f"_AUDIT_INTENTIONAL_GAPS. Добавь {base}_diff_<source> в audit "
                f"ИЛИ задокументируй намеренный пропуск в allowlist (#740)."
            )

    def test_intentional_gaps_are_live_metrics(self):
        """Allowlist не должен пухнуть мёртвыми ключами, которых уже нет в YAML."""
        stale = set(_AUDIT_INTENTIONAL_GAPS) - _audited_metrics()
        assert not stale, (
            f"_AUDIT_INTENTIONAL_GAPS содержит ключи вне source_priority.yaml: "
            f"{sorted(stale)} — удали устаревшие записи (#740)."
        )


@pytest.mark.unit
class TestXrefTmDedup:
    """#814: canonical-keyed мост xref_tm обязан схлопываться до 1 строки на
    (canonical_id, league, season). silver.xref_team несёт 2 transfermarkt-
    алиаса на canonical в сезоне (напр. 'Liverpool'/'Liverpool FC',
    'Sheff Utd'/'Sheffield United') → SELECT DISTINCT по source_id оставляет оба,
    и LEFT JOIN xref_tm двоит грейн (team_id,league,season) ×2 (18 дублей,
    всплывших при пересборке #712). GROUP BY — структурный guard от регресса."""

    def test_xref_tm_grouped_one_row_per_canonical_season(self):
        tree = sqlglot.parse_one(_main_body(), read="trino")
        cte = next((c for c in tree.find_all(sqlglot.exp.CTE)
                    if c.alias_or_name == "xref_tm"), None)
        assert cte is not None, "xref_tm CTE отсутствует в fct_team_season_stats"
        group = cte.this.args.get("group")
        assert group is not None, (
            "xref_tm без GROUP BY → 2 TM-алиаса на canonical двоят грейн "
            "fct_team_season_stats (#814). Сворачивай через MAX+GROUP BY."
        )
        group_cols = {e.sql(dialect="trino") for e in group.expressions}
        assert {"canonical_id", "league", "season"} <= group_cols, (
            f"xref_tm GROUP BY должен включать (canonical_id, league, season), "
            f"получено {sorted(group_cols)}"
        )
        # tm_club_name берётся агрегатом (MAX), а не сырым source_id.
        tm_expr = _cte_proj_map(_main_body(), "xref_tm")["tm_club_name"]
        assert "MAX(" in tm_expr.upper(), (
            f"tm_club_name должен агрегироваться (MAX) для дедупа, получено "
            f"{tm_expr!r}"
        )
