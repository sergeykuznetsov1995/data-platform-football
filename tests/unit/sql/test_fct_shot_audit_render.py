"""Render-smoke for ``dags/sql/gold/fct_shot_audit.sql``.

Issue #602 audit: cross-source DQ-таблица для согласованности ударов между
Understat (спайн) и SofaScore (silver.sofascore_shots) на team-match-grain.
Spine = Understat, агрегированный из gold.fct_shot; SofaScore — LEFT JOIN →
diff = NULL когда источника нет (has_sofascore = FALSE).

Не business-витрина: ТОЛЬКО PK + сырые источниковые метрики (us_*/ss_*) + diff +
coverage-флаг. Никаких COALESCE / business-метрик.

Шаблон — ``test_fct_team_match_audit_render.py``, но ассерты адаптированы:

1. Стандартный audit ассертит «reads Silver, NOT Gold» (one-hop правило). Здесь
   наоборот — fct_shot_audit НАМЕРЕННО читает iceberg.gold.fct_shot (gold-on-gold),
   потому что для Understat нет silver.understat_shots — fct_shot и есть каноничная
   проекция ударов Understat (документировано в шапке fct_shot_audit.sql).
2. PK проецируется как ``us.match_id`` / ``us.team_id`` без ``AS``-алиаса, поэтому
   PK-ассерт проверяет qualified-ссылку, а не ``AS <col>``.
3. Финальный SELECT намеренно несёт сырые us_*/ss_* метрики рядом с diff'ами, так
   что строгий «all aliases must be _diff_» из сиблинга здесь не применим.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_shot_audit.sql"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctShotAuditSql:

    def test_reads_gold_fct_shot_and_silver_sofascore(self):
        """Адаптация ассерта «reads Silver, not Gold»: fct_shot_audit НАМЕРЕННО
        читает iceberg.gold.fct_shot (gold-on-gold) — для Understat нет
        silver.understat_shots, fct_shot и есть каноничная проекция ударов
        (см. шапку fct_shot_audit.sql, "Source-read deviation"). SofaScore —
        единственный источник, читаемый из Silver."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.gold.fct_shot" in sql, (
            "fct_shot_audit must читать iceberg.gold.fct_shot как Understat-спайн "
            "(gold-on-gold deviation, документирована в шапке SQL)"
        )
        assert "iceberg.silver.sofascore_shots" in sql, (
            "fct_shot_audit must читать iceberg.silver.sofascore_shots (SofaScore)"
        )

    def test_understat_spine_and_sofascore_left_join(self):
        """Understat — primary spine (FROM us_agg поверх gold.fct_shot),
        SofaScore — LEFT JOIN (ss_agg поверх silver.sofascore_shots) → не сужает
        spine, has_sofascore=FALSE когда записи нет."""
        sql = _read_sql()
        assert re.search(
            r"FROM\s+iceberg\.gold\.fct_shot",
            sql, re.IGNORECASE,
        ), "Understat spine must читать FROM iceberg.gold.fct_shot"
        assert re.search(
            r"FROM\s+iceberg\.silver\.sofascore_shots",
            sql, re.IGNORECASE,
        ), "SofaScore must читать FROM iceberg.silver.sofascore_shots"
        assert re.search(
            r"LEFT\s+JOIN\s+ss_agg",
            sql, re.IGNORECASE,
        ), "SofaScore must присоединяться через LEFT JOIN (сохраняем spine)"

    def test_grain_pk_columns(self):
        """PK совпадает с main fct (fct_shot): (match_id, team_id). В финальной
        проекции идут как qualified `us.match_id` / `us.team_id` БЕЗ AS-алиаса —
        поэтому проверяем qualified-ссылку, а не `AS <col>`."""
        sql = _read_sql()
        for col in ["match_id", "team_id"]:
            assert re.search(rf"\bus\.{col}\b", sql), (
                f"PK column `{col}` must be projected as `us.{col}`"
            )

    def test_diff_columns_present(self):
        """Финальная audit-проекция несёт 4 diff-колонки (US − SofaScore)."""
        sql = _read_sql()
        for col in ["shots_diff", "xg_diff", "goals_diff", "sot_diff"]:
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"audit-diff `{col}` must be projected as final alias"
            )

    def test_coverage_flag_present(self):
        """has_sofascore — coverage-флаг: TRUE когда SofaScore-запись найдена,
        FALSE когда LEFT JOIN пуст (исторические сезоны без shotmap)."""
        sql = _read_sql()
        assert re.search(r"\bAS\s+has_sofascore\b", sql, re.IGNORECASE), (
            "coverage-флаг `has_sofascore` must be projected"
        )

    def test_pure_select_no_create_table(self):
        """CTAS-обёртка делается в gold_tasks.run_gold_transform. Сам файл —
        pure SELECT/CTE; никаких CREATE TABLE / INSERT / WITH(partitioning=)."""
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
