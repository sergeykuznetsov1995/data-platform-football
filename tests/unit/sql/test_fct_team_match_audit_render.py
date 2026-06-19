"""Render-smoke for ``dags/sql/gold/fct_team_match_audit.sql``.

Issue #95 audit: DQ-таблица для cross-source согласованности FBref vs Understat
vs SofaScore vs WhoScored vs FotMob на team-match-grain. Spine = FBref ∩
Understat (INNER JOIN, design doc §8.3). SofaScore / WhoScored / FotMob — LEFT
JOIN → diff = NULL когда источник отсутствует.

Не business-витрина: ТОЛЬКО PK + diff-колонки + lineage. Никаких COALESCE /
UNIQUE_* / rating.

Шаблон — ``test_fct_player_match_audit_render.py``, адаптирован под team-grain +
5 источников и flatten-CTE спайна FBref (fbref_match_enriched home/away). PK
после #442 — plain ids (match_id, team_id), не *_canonical.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_team_match_audit.sql"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


def _final_select(sql: str) -> str:
    """Вернуть только финальную SELECT-проекцию (между последним top-level
    SELECT и ``FROM fb_team``).

    Спайн FBref здесь — flatten-CTE fb_home/fb_away поверх
    silver.fbref_match_enriched — проецирует метрики (goals_for, shots, …) как
    ``AS <alias>``. Это НЕ часть финальной audit-проекции, поэтому скан
    diff-дисциплины ограничиваем именно финальным SELECT, а не всеми CTE
    (в player-audit CTE алиасят только id → там скан по всему файлу ок).
    """
    body = _strip_comments(sql).split("FROM fb_team")[0]
    return body[body.rfind("SELECT"):]


pytestmark = pytest.mark.unit


class TestFctTeamMatchAuditSql:

    def test_reads_xref_and_all_silver_sources(self):
        """Audit читает Silver заново (one-hop правило), а не gold.fct_team_match."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql
        assert "iceberg.silver.xref_match" in sql
        assert "iceberg.silver.fbref_match_enriched" in sql
        assert "iceberg.silver.understat_team_match" in sql
        assert "iceberg.silver.sofascore_team_match" in sql
        assert "iceberg.silver.whoscored_team_match" in sql
        assert "iceberg.silver.fotmob_team_match" in sql
        assert "gold.fct_team_match" not in sql, (
            "audit must читать Silver напрямую (one-hop), не gold.fct_team_match"
        )

    def test_inner_spine_fbref_and_understat(self):
        """FBref — primary spine (FROM flatten-CTE поверх fbref_match_enriched),
        Understat — secondary spine (INNER JOIN, design doc §8.3)."""
        sql = _read_sql()
        assert re.search(
            r"FROM\s+iceberg\.silver\.fbref_match_enriched",
            sql, re.IGNORECASE,
        ), "FBref spine must читать iceberg.silver.fbref_match_enriched"
        assert re.search(
            r"INNER\s+JOIN\s+iceberg\.silver\.understat_team_match",
            sql, re.IGNORECASE,
        ), "audit must INNER JOIN на Understat Silver (secondary spine)"

    def test_left_join_sofascore_whoscored_fotmob(self):
        """SofaScore / WhoScored / FotMob — LEFT JOIN: добавочные diffs, не
        сужают spine. Если источника нет → diff = NULL."""
        sql = _read_sql()
        for src in (
            "sofascore_team_match",
            "whoscored_team_match",
            "fotmob_team_match",
        ):
            assert re.search(
                rf"LEFT\s+JOIN\s+iceberg\.silver\.{src}",
                sql, re.IGNORECASE,
            ), f"audit must LEFT JOIN на {src} (не INNER — сохраняем spine)"

    def test_grain_pk_columns(self):
        """PK совпадает с main fct: (match_id, team_id) — plain ids после #442.
        Natural composite — non-NULL по конструкции INNER spine."""
        sql = _read_sql()
        for col in ["match_id", "team_id"]:
            assert re.search(rf"\bAS\s+{col}\b", sql), (
                f"PK column `{col}` must be projected as final alias"
            )

    def test_xref_join_includes_league_and_season_predicate(self):
        """CLAUDE.md xref footgun ([[feedback_xref_join_season_predicate]]):
        каждый xref-bridge JOIN должен иметь league + season predicate."""
        sql = _strip_comments(_read_sql())
        assert sql.count(".league") >= 4, (
            "audit must reference .league predicate в ≥4 bridge JOINs"
        )
        assert re.search(r"season_slug|season_year", sql), (
            "audit must reference season_slug / season_year в bridge JOINs"
        )

    def test_diff_columns_only_have_diff_suffix(self):
        """Финальная audit-проекция — ТОЛЬКО diff + PK + lineage. Скан
        ограничен финальным SELECT (см. _final_select): спайн-CTE FBref
        проецирует метрики как `AS goals_for` и т.п., но это не часть audit."""
        final = _final_select(_read_sql())
        aliases = re.findall(r"\bAS\s+(\w+)\b", final, re.IGNORECASE)
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
            "match_id",
            "team_id",
            "league",
            "season",
        }
        for alias in aliases:
            if alias.lower() in sql_types:
                continue
            if alias in allowed_non_diff:
                continue
            assert "_diff_" in alias.lower(), (
                f"final audit projection alias `{alias}` must contain `_diff_` "
                "suffix (audit-таблица — только diff + PK + lineage)"
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
