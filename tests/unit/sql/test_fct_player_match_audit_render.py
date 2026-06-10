"""Render-smoke for ``dags/sql/gold/fct_player_match_audit.sql``.

Issue #46 audit: DQ-таблица для cross-source согласованности FBref vs
SofaScore vs Understat vs WhoScored на match-grain. INNER JOIN на
FBref + SofaScore (secondary spine, см. header) → rows только где обе
стороны spine не-NULL. Understat / WhoScored — LEFT JOIN → diff = NULL
когда источник отсутствует.

Не business-витрина: ТОЛЬКО PK + diff-колонки + lineage. Никаких
COALESCE / UNIQUE_* / rating и т.п.

Шаблон — `test_fct_player_season_stats_audit_render.py`, адаптированный
под match-grain + 4 источника (вместо 5 в seasonal).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_match_audit.sql"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctPlayerMatchAuditSql:

    def test_reads_xref_and_all_four_silver_sources(self):
        """Audit читает Silver заново (one-hop правило), а не gold.fct_*."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.xref_match" in sql
        assert "iceberg.silver.fbref_player_match_stats" in sql
        assert "iceberg.silver.sofascore_player_match_aggregate" in sql
        assert "iceberg.silver.understat_player_match_aggregate" in sql
        assert "iceberg.silver.whoscored_player_match_aggregate" in sql

    def test_inner_join_fbref_and_sofascore(self):
        """FBref — primary spine (INNER), SofaScore — secondary spine
        (INNER, поскольку большинство diff-колонок ss-производные)."""
        sql = _read_sql()
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.fbref_player_match_stats",
            sql, re.IGNORECASE,
        ), "audit must INNER JOIN на FBref Silver"
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.sofascore_player_match_aggregate",
            sql, re.IGNORECASE,
        ), "audit must INNER JOIN на SofaScore Silver"

    def test_left_join_understat_and_whoscored(self):
        """Understat / WhoScored — LEFT JOIN: добавочные diffs, не сужают
        spine. Если источника нет → diff = NULL."""
        sql = _read_sql()
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.understat_player_match_aggregate",
            sql, re.IGNORECASE,
        ), "audit must LEFT JOIN на Understat (не INNER — сохраняем spine)"
        assert re.search(
            r"LEFT\s+JOIN\s+iceberg\.silver\.whoscored_player_match_aggregate",
            sql, re.IGNORECASE,
        ), "audit must LEFT JOIN на WhoScored (не INNER — сохраняем spine)"

    def test_grain_pk_columns(self):
        """PK совпадает с main fct: (match_id_canonical, player_id_canonical).
        Natural composite — non-NULL по конструкции INNER spine."""
        sql = _read_sql()
        for col in ["match_id_canonical", "player_id_canonical"]:
            assert re.search(rf"\b{col}\b", sql), (
                f"PK column `{col}` must be projected"
            )

    def test_xref_join_includes_league_and_season_predicate(self):
        """CLAUDE.md xref footgun ([[feedback_xref_join_season_predicate]]):
        каждый xref-bridge JOIN должен иметь league + season predicate.
        Проверяем что во всех 4 source-bridge CTE league + season_slug
        фигурируют в ON-блоке (любой формы)."""
        sql = _strip_comments(_read_sql())
        # league в JOIN-блоках xref bridge — обязателен.
        # season_slug либо season_year — также обязательны хотя бы где-то.
        assert sql.count(".league") >= 4, (
            "audit must reference .league predicate в ≥4 bridge JOINs "
            "(4 источника × bridge'и)"
        )
        assert re.search(r"season_slug|season_year", sql), (
            "audit must reference season_slug / season_year в bridge JOINs"
        )

    def test_season_slug_passthrough(self):
        """#404: xref season is slug — passed straight through as season_year,
        no slug→year SUBSTR conversion."""
        sql = _read_sql()
        assert re.search(r"season\b[^\n]*AS\s+season_year", sql, re.IGNORECASE), (
            "audit must alias xref season directly as season_year "
            "(slug passthrough after #404)"
        )
        assert not re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        ), "slug→year SUBSTR idiom was removed in #404 — season is slug now"

    def test_audit_diff_columns_present(self):
        """Каждый из 4 источников отдаёт >= несколько diff-колонок.
        Все имеют суффикс `_diff_<source>` или `_diff_<src1>_<src2>`."""
        sql = _read_sql()
        # SofaScore (INNER): минимум 5 HARD_FACT diff'ов с FBref spine.
        ss_diffs = [
            "minutes_diff_ss",
            "goals_diff_ss",
            "assists_diff_ss",
            "shots_diff_ss",
            "yellow_cards_diff_ss",
        ]
        for col in ss_diffs:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"SofaScore audit-diff `{col}` must be projected"
            )

        # Understat (LEFT): HARD_FACT diff'ы с FBref spine.
        us_diffs = [
            "minutes_diff_us",
            "goals_diff_us",
            "assists_diff_us",
            "shots_diff_us",
        ]
        for col in us_diffs:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"Understat audit-diff `{col}` must be projected"
            )

        # WhoScored (LEFT): HARD_FACT diff'ы с FBref spine.
        ws_diffs = [
            "goals_diff_ws",
            "shots_diff_ws",
            "yellow_cards_diff_ws",
        ]
        for col in ws_diffs:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"WhoScored audit-diff `{col}` must be projected"
            )

    def test_modeled_xg_xa_cross_source_diff(self):
        """MODELED xG/xA — cross-source diff Understat vs SofaScore
        (разные модели, ожидаемо расходятся). Diff'ы хранятся в audit
        с суффиксом `_diff_us_ss` либо `_diff_understat_sofascore`."""
        sql = _read_sql()
        # Хотя бы один из xg/xa cross-source diff присутствует.
        xg_pattern = re.search(
            r"\bAS\s+xg_diff_(us_ss|understat_sofascore)\b", sql, re.IGNORECASE
        )
        xa_pattern = re.search(
            r"\bAS\s+xa_diff_(us_ss|understat_sofascore)\b", sql, re.IGNORECASE
        )
        assert xg_pattern, (
            "xG cross-source diff (`xg_diff_us_ss` или эквивалент) must "
            "be projected in audit"
        )
        assert xa_pattern, (
            "xA cross-source diff (`xa_diff_us_ss` или эквивалент) must "
            "be projected in audit"
        )

    def test_diff_columns_only_have_diff_suffix(self):
        """Audit-таблица не должна содержать business-метрики (single-column
        без diff). Все НЕ-PK / НЕ-lineage projection'ы должны иметь
        `_diff_*` суффикс. Эвристика: ищем `AS <alias>` после SELECT,
        и каждый alias либо ∈ {PK, lineage}, либо содержит `_diff_`."""
        sql = _strip_comments(_read_sql())
        aliases = re.findall(r"\bAS\s+(\w+)\b", sql, re.IGNORECASE)
        # SQL-типы в CAST(... AS <TYPE>) — не aliases, отфильтровываем.
        sql_types = {
            "bigint", "integer", "int", "smallint", "tinyint",
            "double", "real", "decimal", "numeric",
            "varchar", "char", "string", "text",
            "boolean", "bool",
            "date", "timestamp", "time",
            "json", "array", "map", "row",
        }
        allowed_non_diff = {
            "match_id_canonical",
            "player_id_canonical",
            "team_id_canonical",
            "fbref_player_id",
            "fbref_match_id",
            "ss_player_id",
            "ss_match_id",
            "us_player_id",
            "us_match_id",
            "ws_player_id",
            "ws_match_id",
            "season_slug",
            "season_year",
            "canonical_id",
            "source_id",
            "league",
            "season",
            "_gold_created_at",
            "_gold_loaded_at",
        }
        for alias in aliases:
            if alias.lower() in sql_types:
                continue
            if alias in allowed_non_diff:
                continue
            assert "_diff_" in alias.lower(), (
                f"audit projection alias `{alias}` must contain `_diff_` "
                "suffix (audit-таблица — только diff + PK + lineage)"
            )

    def test_no_business_coalesce_metrics(self):
        """В audit нет business-COALESCE метрик (single-column через
        COALESCE источников — это business-fct идиома). COALESCE здесь
        допустим ТОЛЬКО для bridging PK `match_id_canonical` (FBref
        bridge LEFT JOIN может вернуть NULL → fallback на fb.match_id):
        1× в SELECT + 1× per LEFT-bridge ON-condition (sofascore/understat/
        whoscored) = до 4 вхождений.

        Главное — никаких `COALESCE(...) AS <business_metric>`."""
        sql = _strip_comments(_read_sql())
        # Все COALESCE-вхождения должны быть на bridging match_id_canonical;
        # ни одного `COALESCE(...) AS <business>` (single-column metric idiom).
        # Эвристика: ищем COALESCE NOT-immediately-followed-by-`,`-без-AS-после-`)`.
        # Простая проверка: ни один `AS <alias>` после COALESCE-)`
        # не должен попадать в бизнес-метрики (RX2 stale-list).
        for stale in (
            "expected_goals",
            "expected_assists",
            "rating",
            "primary_team_name",
            "shots_total",
            "minutes",
            "goals",
            "assists",
        ):
            # Ищем COALESCE(...) AS <stale> — business-fct идиома, не
            # должна быть в audit.
            assert not re.search(
                rf"COALESCE\s*\([^)]*\)\s+AS\s+{stale}\b",
                sql, re.IGNORECASE | re.DOTALL,
            ), (
                f"audit не должен содержать business COALESCE-метрику "
                f"`COALESCE(...) AS {stale}`"
            )

    def test_pure_select_no_create_table(self):
        """CTAS-обёртка делается в gold_tasks.run_gold_transform.
        Сам файл — pure SELECT/CTE; никаких CREATE TABLE / INSERT /
        WITH (partitioning=...)."""
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "audit SQL must remain pure SELECT/CTE — без CREATE TABLE"
        )
        assert "INSERT INTO" not in sql.upper(), (
            "audit SQL must remain pure SELECT/CTE — без INSERT"
        )
        assert not re.search(r"WITH\s*\(\s*partitioning\s*=", sql, re.IGNORECASE), (
            "audit SQL must remain pure SELECT/CTE — без WITH(partitioning=...)"
        )

    def test_no_legacy_entity_xref(self):
        """E1.5 cutover: никаких gold.entity_xref."""
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "audit SQL must NOT reference gold.entity_xref"
        )
