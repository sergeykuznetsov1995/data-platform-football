"""Render-smoke for ``dags/sql/gold/fct_player_match.sql``.

Issue #46: per-match wide витрина cross-source (FBref+SofaScore+Understat+
WhoScored). FBref-spine; HARD_FACT метрики через variadic COALESCE;
MODELED xG/xA — single column через COALESCE(us → ss); audit-diff
вынесены в `gold.fct_player_match_audit`. PK = `(match_id, player_id)`
(#426 star design §4.4; natural composite — обе компоненты non-NULL по
конструкции FBref-spine, поэтому НИКАКОГО `xxhash64` PK).

Шаблон — `test_fct_player_season_stats_render.py`, адаптированный
под match-grain (нет `pos NOT LIKE '%GK%'` фильтра — keeper-витрины
нет, вратари остаются внутри).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
# #437: fct_player_match became a .sql.j2 template — its cross-source COALESCE
# columns render from configs/medallion/source_priority.yaml. We render here so
# the assertions below run against the actually-executed SQL (priority included).
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_player_match.sql.j2"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)


def _read_sql() -> str:
    from utils.medallion_config import render_fact_sql

    return render_fact_sql(SQL_PATH, "fct_player_match")


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
        """#404: season is slug end-to-end — the slug→year-start SUBSTR idiom is
        gone; the xref CTE passes season through and JOINs are slug = slug."""
        sql = _read_sql()
        assert not re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season",
            sql, re.IGNORECASE,
        ), "fct_player_match.sql must NOT convert season slug→year-start (#404)"

    def test_grain_columns_present(self):
        """PK match-grain: (match_id, player_id) — #426 star design §4.4.
        Plus team_id и league/season для partition."""
        sql = _read_sql()
        for col in [
            "match_id",
            "player_id",
            "league",
            "season",
        ]:
            assert re.search(rf"\b{col}\b", sql), (
                f"PK / partition column `{col}` must be projected"
            )

    def test_no_xxhash_pk(self):
        """PK = natural composite `(match_id, player_id)` —
        ОБЕ компоненты non-NULL по конструкции FBref-spine. Hash PK не
        нужен; xxhash64 запрещён (избыточная сложность, NULL-collision risk
        из feedback_hash_pk_with_null_canonical.md)."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"\bxxhash64\b", sql, re.IGNORECASE), (
            "fct_player_match must NOT use xxhash64 PK — natural composite "
            "(match_id, player_id) is non-NULL by FBref-spine"
        )

    def test_hard_fact_coalesce_columns(self):
        """HARD_FACT overlap метрик публикуются single-column через variadic
        COALESCE. FBref primary spine; cascade fb→(fm/)ws→us→ss. Regex
        принимает любой N-arg COALESCE начиная с fb.<col>, заканчивая
        AS <alias>. Outer CAST(... AS BIGINT) wrapper допустим."""
        sql = _read_sql()
        hard_facts = [
            ("minutes", "minutes_played"),
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
        # Single-column xg + xa (#426 design names).
        for col in ("xg", "xa"):
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

    def test_understat_bridge_has_no_row_number_workaround(self):
        """Issue #70: после dedup в silver.xref_player на уровне resolver'а
        Gold-bridge для Understat не должен делать ROW_NUMBER. Симметричен
        с sofascore/whoscored/fotmob bridges."""
        sql = _read_sql()
        # Вычленяем тело CTE `xref_us_player AS (...)` и проверяем его
        # отдельно — другие части файла могут законно содержать ROW_NUMBER
        # (например, dedup внутри SilverWindowFunctions).
        match = re.search(
            r"xref_us_player\s+AS\s*\((.*?)\)\s*,",
            sql, re.IGNORECASE | re.DOTALL,
        )
        assert match, "CTE `xref_us_player` not found in fct_player_match.sql"
        body = match.group(1)
        assert not re.search(r"\bROW_NUMBER\s*\(", body, re.IGNORECASE), (
            "xref_us_player must NOT contain ROW_NUMBER dedup (issue #70 fix). "
            "Silver resolver guarantees ≤1 understat source_id per "
            "(canonical, league, season); bridge becomes a plain SELECT DISTINCT."
        )
        assert re.search(r"\bSELECT\s+DISTINCT\b", body, re.IGNORECASE), (
            "xref_us_player must use SELECT DISTINCT (symmetric with ss/ws bridges)"
        )


# ---------------------------------------------------------------------------
# Issue #70 — fan-out contract via DuckDB synthetic
# ---------------------------------------------------------------------------
class TestUnderstatBridgeFanOutContract:
    """Sanity-prove the understat bridge: it tolerates a well-formed
    `silver.xref_player` (one source_id per canonical+league+season) and
    fans out when that invariant breaks. The fan-out path is the regression
    `silver.xref_player_resolver._dedup_canonical_per_season` prevents.

    This isolates the understat bridge logic in DuckDB instead of trying
    to render the full fct_player_match.sql (too many CTEs / Trino-only
    idioms for an in-process driver).
    """

    @staticmethod
    def _setup_duckdb(con) -> None:
        # Spine: one match, one player_id_canonical = 'fb_harrison_reed'.
        con.execute(
            "CREATE TABLE spine (match_id_canonical VARCHAR, "
            "player_id_canonical VARCHAR, league VARCHAR, season_slug VARCHAR)"
        )
        con.execute(
            "INSERT INTO spine VALUES "
            "('m1', 'fb_harrison_reed', 'ENG-Premier League', '2526')"
        )

    @staticmethod
    def _xref_table(con, rows) -> None:
        con.execute(
            "CREATE TABLE xref_player (canonical_id VARCHAR, source VARCHAR, "
            "source_id VARCHAR, league VARCHAR, season VARCHAR, confidence VARCHAR)"
        )
        for r in rows:
            con.execute(
                "INSERT INTO xref_player VALUES (?, ?, ?, ?, ?, ?)",
                r,
            )

    _BRIDGE_SQL = """
    WITH xref_us_player AS (
        SELECT DISTINCT
            canonical_id,
            source_id AS us_player_id,
            league,
            season AS season_slug
        FROM xref_player
        WHERE source = 'understat'
          AND confidence <> 'orphan'
    )
    SELECT spine.match_id_canonical, spine.player_id_canonical
    FROM spine
    LEFT JOIN xref_us_player b
      ON b.canonical_id = spine.player_id_canonical
     AND b.league = spine.league
     AND b.season_slug = spine.season_slug
    """

    def test_well_formed_xref_one_row_per_pk(self):
        """Когда Silver dedup отработал — xref_player содержит ровно одну
        understat-строку per (canonical, league, season) — bridge даёт 1 row."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect()
        try:
            self._setup_duckdb(con)
            self._xref_table(con, [
                ('fb_harrison_reed', 'understat', '6827',
                 'ENG-Premier League', '2526', 'name_team'),
            ])
            rows = con.execute(self._BRIDGE_SQL).fetchall()
            assert len(rows) == 1
            assert rows[0] == ('m1', 'fb_harrison_reed')
        finally:
            con.close()

    def test_unfixed_xref_fan_out_demonstrates_regression(self):
        """Контракт: ЕСЛИ кто-то обойдёт _dedup_canonical_per_season и
        запишет 2 understat source_id для одного canonical в одном сезоне
        — текущий bridge (без ROW_NUMBER) fan-out 2×. Этот тест защищает
        от reintroduction'а старого hack'а: с настоящим
        ROW_NUMBER-workaround'ом он бы давал 1 row здесь, а наш фикс
        приводит к 2 row — что и фиксируется как ожидаемое поведение."""
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect()
        try:
            self._setup_duckdb(con)
            self._xref_table(con, [
                ('fb_harrison_reed', 'understat', '910',
                 'ENG-Premier League', '2526', 'name_team'),
                ('fb_harrison_reed', 'understat', '6827',
                 'ENG-Premier League', '2526', 'name_team'),
            ])
            rows = con.execute(self._BRIDGE_SQL).fetchall()
            # 2 → подтверждает что Gold SQL полагается на Silver dedup;
            # хак-ROW_NUMBER здесь бы вернул 1 (но мы его убрали).
            assert len(rows) == 2
        finally:
            con.close()
