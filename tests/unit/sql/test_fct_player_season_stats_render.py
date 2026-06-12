"""Render-smoke for ``dags/sql/gold/fct_player_season_stats.sql``.

T6: cross-source per-season stats per canonical player across 5 sources
(FBref/FotMob/WhoScored/Understat/SofaScore). FBref-spine; HARD_FACT
metrics single-column через variadic COALESCE; UNIQUE_<source> single
column; MODELED metrics (xG/xA/rating) keep per-source suffix.
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

    def test_season_slug_passthrough(self):
        """#404: xref season is slug — passed straight through as season_year,
        no slug→year SUBSTR conversion."""
        sql = _read_sql()
        assert re.search(r"season\b[^\n]*AS\s+season_year", sql, re.IGNORECASE), (
            "fct_player_season_stats.sql must alias xref season directly as "
            "season_year (slug passthrough after #404)"
        )
        assert not re.search(
            r"2000\s*\+\s*CAST\s*\(\s*SUBSTR\s*\(\s*season\s*,\s*1\s*,\s*2\s*\)",
            sql, re.IGNORECASE,
        ), "slug→year SUBSTR idiom was removed in #404 — season is slug now"

    def test_grain_columns_present(self):
        """#428 star design §5.2: PK = (player_id, league, season) — plain id."""
        sql = _read_sql()
        for col in ['player_id', 'league', 'season']:
            assert re.search(rf"\b{col}\b", sql), (
                f"PK column `{col}` must be projected"
            )

    def test_fb_dedup_collapses_multi_squad_seasons(self):
        """#463: silver profile grain = (player_id, squad, league, season) —
        winter transfers keep one row per club. Gold PK stays
        (player_id, league, season) via fb_dedup CTE: survivor = max-minutes
        club (§5.2), deterministic tiebreaker squad."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"\bfb_dedup\s+AS\s*\(", sql, re.IGNORECASE), (
            "missing fb_dedup CTE — multi-squad silver rows would fan out gold PK"
        )
        assert re.search(
            r"ORDER\s+BY\s+minutes\s+DESC\s+NULLS\s+LAST\s*,\s*squad",
            sql, re.IGNORECASE,
        ), "fb_dedup must pick max-minutes club deterministically (§5.2)"
        assert re.search(r"INNER\s+JOIN\s+fb_dedup\s+fb\b", sql, re.IGNORECASE), (
            "spine must join fb_dedup, not the raw silver profile"
        )
        assert not re.search(
            r"JOIN\s+iceberg\.silver\.fbref_player_season_profile\s+fb\b",
            sql, re.IGNORECASE,
        ), "raw silver profile must be read only inside fb_dedup"

    def test_team_id_fk_with_orphan_fallback(self):
        """#428 §5.2: team_id FK (fb.squad → xref_team) + 'fb_<slug>' orphan
        fallback — строки не теряются (§6.2)."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql, (
            "team_id bridge must read silver.xref_team"
        )
        assert re.search(r"\bAS\s+team_id\b", sql, re.IGNORECASE), (
            "team_id FK must be projected"
        )
        assert re.search(
            r"'fb_'\s*\|\|\s*lower\s*\(\s*regexp_replace\s*\(", sql,
            re.IGNORECASE,
        ), "team_id must keep the 'fb_<slug>' orphan fallback"

    def test_xref_team_join_includes_league_and_season(self):
        """xref_team JOIN must carry (league, season) — fan-out footgun."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"xt\.league\s*=\s*xf\.league", sql, re.IGNORECASE), (
            "xref_team bridge JOIN must include league predicate"
        )
        assert re.search(r"xt\.season\s*=\s*xf\.season_year", sql, re.IGNORECASE), (
            "xref_team bridge JOIN must include season predicate"
        )

    def test_context_columns_dropped(self):
        """#428: контекст через dims — primary_team_name / position_* /
        player_id_canonical удалены из business-fct."""
        sql = _strip_comments(_read_sql())
        for stale in ('primary_team_name', 'position_fbref',
                      'position_fotmob', 'player_id_canonical'):
            assert not re.search(rf"\bAS\s+{stale}\b", sql, re.IGNORECASE), (
                f"`{stale}` must be dropped (#428 — context via dims)"
            )

    def test_hard_fact_coalesce_columns(self):
        """HARD_FACT overlap метрик публикуются single-column через variadic
        COALESCE. FBref primary spine; cascade fb→fm→ws→us→ss (subset of
        sources per metric — depends on what each source materialises).
        Regex принимает любой N-arg COALESCE начиная с fb.<col>."""
        sql = _read_sql()
        hard_facts = [
            ('mp', 'matches'),
            ('minutes', 'minutes'),
            ('goals', 'goals'),
            ('assists', 'assists'),
            ('yellow_cards', 'yellow_cards'),
            ('red_cards', 'red_cards'),
            ('penalties_won', 'penalties_won'),
            ('penalties_conceded', 'penalties_conceded'),
        ]
        for fb_col, alias in hard_facts:
            # Variadic COALESCE starting with fb.<fb_col>, ending with
            # AS <alias>. Outer CAST(... AS BIGINT) wrapper is fine — the
            # COALESCE-AS span is matched inside.
            pattern = (
                rf"COALESCE\s*\(\s*fb\.{fb_col}\b[^)]*\)[^,]*?\bAS\s+{alias}\b"
            )
            assert re.search(pattern, sql, re.IGNORECASE | re.DOTALL), (
                f"HARD_FACT `{alias}` must be COALESCE(fb.{fb_col}, ...) AS {alias}"
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
        """UNIQUE_FOTMOB метрики, отсутствующие у других источников."""
        sql = _read_sql()
        # issue #154: FotMob silver хранит defensive_actions / poss_won_final_third
        # только в per-90 форме (absolute-полей больше нет) → проецируем `*_per_90`.
        unique_fotmob = [
            'defensive_actions_per_90',
            'big_chances_created', 'big_chances_missed', 'chances_created',
            'poss_won_final_third_per_90',
        ]
        for col in unique_fotmob:
            assert re.search(rf"fm\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_FOTMOB column `{col}` must come from fm."
            )

    def test_unique_whoscored_columns_present(self):
        """UNIQUE_WHOSCORED метрики, отсутствующие у других источников."""
        sql = _read_sql()
        unique_ws = [
            'bad_touches', 'touches_in_box', 'avg_x', 'avg_y',
        ]
        for col in unique_ws:
            assert re.search(rf"\bws\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_WHOSCORED column `{col}` must come from ws."
            )

    def test_unique_sofascore_columns_present(self):
        """UNIQUE_SOFASCORE — ground/aerial/total duels, errors_lead_to_*,
        touches/dispossessed, goal+shot breakdowns. Single-column из ss."""
        sql = _read_sql()
        unique_ss = [
            'ground_duels_won', 'aerial_duels_won', 'total_duels_won',
            'errors_lead_to_goal', 'errors_lead_to_shot',
            'touches', 'dispossessed', 'possession_lost',
            'shots_off_target', 'shots_inside_box', 'shots_outside_box',
            'goals_inside_box', 'goals_outside_box',
            'headed_goals', 'left_foot_goals', 'right_foot_goals',
        ]
        for col in unique_ss:
            assert re.search(rf"\bss\.{col}\b", sql, re.IGNORECASE), (
                f"UNIQUE_SOFASCORE column `{col}` must come from ss."
            )

    def test_xg_single_column_after_rx2(self):
        """RX2 verdict: xG свёрнут в single `expected_goals` через
        COALESCE(us → fm → ss). Per-source suffix колонки удалены."""
        sql = _read_sql()
        # Single-column expected_goals + expected_assists должны присутствовать.
        for col in ('expected_goals', 'expected_assists'):
            assert re.search(
                rf"COALESCE\([^)]*\)\s*,?\s*2?\)?\s+AS\s+{col}\b",
                sql, re.IGNORECASE,
            ) or re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"`{col}` must be projected as a single COALESCE column"
            )
        # Per-source suffix колонки удалены из business-fct.
        for stale in (
            'expected_goals_fotmob',
            'expected_goals_sofascore',
            'expected_assists_fotmob',
        ):
            assert not re.search(rf"\bAS\s+{stale}\b", sql, re.IGNORECASE), (
                f"`{stale}` must be removed from business fct (RX2 verdict)"
            )
        # Understat-unique метрики остаются с суффиксом (нет overlap с другими).
        for col in (
            'non_penalty_xg_understat',
            'xg_chain_understat',
            'xg_buildup_understat',
        ):
            assert re.search(rf"\bAS\s+{col}\b", sql, re.IGNORECASE), (
                f"Understat-unique `{col}` must be projected"
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
