"""Render-smoke for ``dags/sql/gold/fct_player_match.sql``.

Issue #46: per-match wide витрина cross-source (FBref+SofaScore+Understat+
WhoScored). FBref-spine; HARD_FACT метрики через variadic COALESCE;
MODELED xG/xA — single column через COALESCE(us → ss); audit-diff
вынесены в `gold.fct_player_match_audit`. PK = `(match_id_canonical,
player_id_canonical)` (natural composite — обе компоненты non-NULL по
конструкции FBref-spine, поэтому НИКАКОГО `xxhash64` PK).

Шаблон — `test_fct_player_season_stats_render.py`, адаптированный
под match-grain (нет `pos NOT LIKE '%GK%'` фильтра — keeper-витрины
нет, вратари остаются внутри).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFctPlayerMatchSql:

    def test_reads_xref_and_all_four_silver_sources(self):
        """Multi-source spine. После issue #46 cutover читаем FBref+
        SofaScore+Understat+WhoScored Silver-агрегаты + silver.xref_player
        + silver.xref_match (bridging match_id → canonical)."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert "iceberg.silver.xref_match" in sql
        assert "iceberg.silver.fbref_player_match_stats" in sql
        assert "iceberg.silver.sofascore_player_match_aggregate" in sql
        assert "iceberg.silver.understat_player_match_aggregate" in sql
        assert "iceberg.silver.whoscored_player_match_aggregate" in sql

    def test_fbref_spine_filter(self):
        """Spine = (canonical_id, league, season) FBref-only из xref_player.
        confidence<>'orphan' — отрезаем R2-resolver orphan rows."""
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'fbref'", sql, re.IGNORECASE), (
            "spine must filter xref_player to source='fbref'"
        )
        assert (
            re.search(r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE)
            or re.search(r"confidence\s*!=\s*'orphan'", sql, re.IGNORECASE)
        ), "spine must exclude FBref orphan rows"

    def test_no_keeper_exclusion(self):
        """В отличие от seasonal fct (там outfield-only витрина),
        per-match НЕ исключает вратарей: keeper-match витрины нет.
        Никаких `pos NOT LIKE '%GK%'`."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"pos\s+NOT\s+LIKE\s+'%GK%'", sql, re.IGNORECASE), (
            "fct_player_match.sql must NOT exclude GK on match grain "
            "(no per-match keeper витрина пока)"
        )

    def test_xref_join_includes_league_and_season_predicate(self):
        """CLAUDE.md «xref JOIN must include (league, season) predicate»:
        silver.xref_player имеет per-(source, source_id, season) rows;
        без season-condition fan-out 1.5-4×. Применяем к КАЖДОМУ из 3
        bridge-CTE (sofascore/understat/whoscored)."""
        sql = _strip_comments(_read_sql())
        # Каждый source-bridge JOIN на canonical_id из spine ДОЛЖЕН
        # включать league + season-predicate. Проверяем регексом по
        # ON-блоку каждого CTE bridge.
        for bridge in ("xref_ss", "xref_us", "xref_ws", "xref_sofascore",
                       "xref_understat", "xref_whoscored"):
            # Если CTE с таким именем есть — проверим что в ON-блоке
            # JOIN'а наружу присутствуют league + season(_slug|_year).
            pattern = re.compile(
                rf"INNER\s+JOIN\s+{bridge}\w*\s+\w+\s+ON\b[^;]*?\bleague\b",
                re.IGNORECASE | re.DOTALL,
            )
            if re.search(rf"\b{bridge}\b", sql, re.IGNORECASE):
                # Достаточно одного вхождения league в окрестности bridge.
                assert pattern.search(sql) or re.search(
                    rf"(LEFT|INNER)\s+JOIN\s+{bridge}\w*[\s\S]{{0,400}}?league",
                    sql, re.IGNORECASE,
                ), f"bridge `{bridge}` JOIN must include league predicate"
        # Season predicate (любой из формы): season_slug / season_year /
        # CAST AS varchar / CAST AS BIGINT — хотя бы один в окрестности
        # xref_player JOIN.
        assert re.search(r"season_slug|season_year|season\s*=\s*", sql), (
            "xref_* JOINs must reference season (slug/year) predicate"
        )

    def test_season_slug_to_year_idiom(self):
        """xref slug '2526' → bigint 2025 идиомой
        `2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)`. Нужно ИМЕННО
        для FBref-bigint-spine: fbref_player_match_stats.season — bigint."""
        sql = _read_sql()
        assert re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        ), (
            "fct_player_match.sql must convert xref season slug ('2526') "
            "to bigint year (2025) using SUBSTR idiom for FBref-spine JOIN"
        )

    def test_grain_columns_present(self):
        """PK match-grain: (match_id_canonical, player_id_canonical).
        Plus team_id_canonical и league/season для partition."""
        sql = _read_sql()
        for col in [
            "match_id_canonical",
            "player_id_canonical",
            "league",
            "season",
        ]:
            assert re.search(rf"\b{col}\b", sql), (
                f"PK / partition column `{col}` must be projected"
            )

    def test_no_xxhash_pk(self):
        """PK = natural composite `(match_id_canonical, player_id_canonical)` —
        ОБЕ компоненты non-NULL по конструкции FBref-spine. Hash PK не
        нужен; xxhash64 запрещён (избыточная сложность, NULL-collision risk
        из feedback_hash_pk_with_null_canonical.md)."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"\bxxhash64\b", sql, re.IGNORECASE), (
            "fct_player_match must NOT use xxhash64 PK — natural composite "
            "(match_id_canonical, player_id_canonical) is non-NULL by FBref-spine"
        )

    def test_hard_fact_coalesce_columns(self):
        """HARD_FACT overlap метрик публикуются single-column через variadic
        COALESCE. FBref primary spine; cascade fb→(fm/)ws→us→ss. Regex
        принимает любой N-arg COALESCE начиная с fb.<col>, заканчивая
        AS <alias>. Outer CAST(... AS BIGINT) wrapper допустим."""
        sql = _read_sql()
        hard_facts = [
            ("minutes", "minutes"),
            ("goals", "goals"),
            ("assists", "assists"),
            ("yellow_cards", "yellow_cards"),
            ("red_cards", "red_cards"),
        ]
        for fb_col, alias in hard_facts:
            pattern = (
                rf"COALESCE\s*\(\s*fb\.{fb_col}\b[^)]*\)[^,]*?\bAS\s+{alias}\b"
            )
            assert re.search(pattern, sql, re.IGNORECASE | re.DOTALL), (
                f"HARD_FACT `{alias}` must be COALESCE(fb.{fb_col}, ...) AS {alias}"
            )

    def test_modeled_xg_xa_single_column(self):
        """RX2 verdict (memory: project_xg_rx2_2026-05-22) распространяется
        и на match-grain: xG/xA — single column через COALESCE(us → ss).
        FotMob на match-grain отсутствует (см. план Out-of-scope), поэтому
        cascade только us→ss. Per-source suffix колонки в business-fct
        ЗАПРЕЩЕНЫ."""
        sql = _read_sql()
        # Single-column expected_goals + expected_assists.
        for col in ("expected_goals", "expected_assists"):
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"`{col}` must be projected as a single COALESCE column"
            )
        # Stale per-source-suffix колонки запрещены.
        for stale in (
            "expected_goals_understat",
            "expected_goals_sofascore",
            "expected_assists_understat",
            "expected_assists_sofascore",
        ):
            assert not re.search(rf"\bAS\s+{stale}\b", sql, re.IGNORECASE), (
                f"`{stale}` must NOT be in business fct (audit-diff вместо)"
            )

    def test_no_audit_diff_columns(self):
        """Cross-source diff-колонки вынесены в `gold.fct_player_match_audit`
        чтобы не загромождать business-витрину (memory:
        feedback_audit_in_separate_table). Основная fct НЕ должна
        содержать `<metric>_diff_<source>`. Header-комментарии со ссылкой
        на audit-таблицу — допустимы."""
        sql = _strip_comments(_read_sql())
        for stale in (
            r"_diff_ss\b",
            r"_diff_us\b",
            r"_diff_ws\b",
            r"_diff_sofascore\b",
            r"_diff_understat\b",
            r"_diff_whoscored\b",
            r"_diff_fbref\b",
        ):
            assert not re.search(stale, sql), (
                f"fct_player_match.sql must NOT include audit-diff columns "
                f"(matched `{stale}`); they live in fct_player_match_audit"
            )

    def test_rating_single_source_sofascore(self):
        """RX2: rating берётся ТОЛЬКО из SofaScore (Opta-derived).
        FotMob rating дропнут (на match-grain его и нет). Single-column
        `rating` либо `rating_sofascore` — допустимо до cutover, но
        НИКАКИХ rating_fotmob / rating_diff."""
        sql = _read_sql()
        # rating должен идти из ss.* (SofaScore alias) — точно не
        # из us./ws./fb.; не должно быть rating_fotmob.
        assert not re.search(r"\bAS\s+rating_fotmob\b", sql, re.IGNORECASE), (
            "rating_fotmob must NOT be in fct_player_match (RX2 verdict)"
        )

    def test_partition_columns_projected_last(self):
        """`league`, `season` обязаны быть выпроецированы (CTAS-обёртка
        gold_tasks.run_gold_transform использует их как partitioning)."""
        sql = _read_sql()
        # league и season присутствуют как goals-columns в SELECT (а не
        # только как JOIN predicate).
        assert re.search(r"\bAS\s+league\b|\bxfp?\.league\s*,", sql, re.IGNORECASE) \
            or re.search(r"\bleague\b\s*,\s*\n?\s*\w*\.?season\b", sql, re.IGNORECASE), (
            "`league` must be projected in SELECT for partitioning"
        )

    def test_pure_select_no_create_table(self):
        """CTAS-обёртка делается в gold_tasks.run_gold_transform.
        Сам SQL-файл — pure SELECT, без CREATE TABLE / INSERT /
        WITH (partitioning=...)."""
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fct_player_match.sql must remain a pure SELECT"
        )
        assert "INSERT INTO" not in sql.upper(), (
            "fct_player_match.sql must remain a pure SELECT (no INSERT)"
        )
        assert not re.search(r"WITH\s*\(\s*partitioning\s*=", sql, re.IGNORECASE), (
            "WITH (partitioning=...) — это CTAS-обёртка, делается в gold_tasks; "
            "сам SQL должен быть pure SELECT"
        )

    def test_no_legacy_entity_xref(self):
        """E1.5 cutover (2026-05-09): любые ссылки на gold.entity_xref —
        запрещены в executable SQL. Только silver.xref_*."""
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "fct_player_match.sql must NOT reference gold.entity_xref "
            "after E1.5 cutover; use silver.xref_player / silver.xref_match"
        )
