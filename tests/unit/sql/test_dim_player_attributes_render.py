"""Render-smoke for ``dags/sql/gold/dim_player_attributes.sql``.

T4: cross-source snapshot per canonical_id с per-source колонками
(born_year_fbref / dob_fotmob / nationality_fbref / nationality_fotmob /
height_cm_fotmob / foot_fotmob). No winning-value logic — обе значения
живут рядом, потребитель решает сам.

Spine: FBref (source='fbref' AND confidence != 'orphan') → один row per
canonical_id. FotMob атрибуты подцепляются через xref_player bridge
(latest-season source_id per canonical_id).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_player_attributes.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimPlayerAttributesSql:

    def test_reads_xref_player_and_both_silver_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql, (
            "dim_player_attributes.sql must read silver.xref_player "
            "(canonical-id spine)"
        )
        assert "iceberg.silver.fbref_player_season_profile" in sql, (
            "dim_player_attributes.sql must read fbref_player_season_profile "
            "(born_year + nation_fbref source)"
        )
        assert "iceberg.silver.fotmob_player_profile" in sql, (
            "dim_player_attributes.sql must read silver.fotmob_player_profile "
            "(height/dob/foot/nationality FotMob source)"
        )

    def test_fbref_spine_filter(self):
        """Один row per canonical_id обеспечивается фильтром source='fbref' +
        confidence != 'orphan'. Любой другой spine рискует ×N fan-out."""
        sql = _read_sql()
        assert re.search(
            r"source\s*=\s*'fbref'", sql, re.IGNORECASE,
        ), "dim_player_attributes.sql must filter xref_player to source='fbref' for spine"
        assert re.search(
            r"confidence\s*<>\s*'orphan'", sql, re.IGNORECASE,
        ) or re.search(
            r"confidence\s*!=\s*'orphan'", sql, re.IGNORECASE,
        ), "dim_player_attributes.sql must exclude FBref orphan rows from spine"

    def test_fotmob_bridge_latest_season(self):
        """FotMob source_id берётся из latest-season row в xref_player
        (xref хранит per-(source, source_id, season) → fan-out без dedup)."""
        sql = _read_sql()
        # ROW_NUMBER ... ORDER BY season DESC anywhere in fotmob-CTE area.
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\([^)]*season\s+DESC",
            sql, re.IGNORECASE,
        ), (
            "dim_player_attributes.sql must dedup FotMob xref by latest "
            "season (ROW_NUMBER OVER (PARTITION BY canonical_id ORDER BY season DESC))"
        )

    def test_per_source_attribute_columns_named_with_suffix(self):
        """Контракт: атрибутные колонки имеют суффикс _fbref / _fotmob —
        no winning-value logic, обе значения публикуются."""
        sql = _read_sql()
        required_cols = [
            "born_year_fbref",
            "dob_fotmob",
            "nationality_fbref",
            "nationality_fotmob",
            "height_cm_fotmob",
            "foot_fotmob",
            "player_id_canonical",
            "player_name_canonical",
        ]
        for col in required_cols:
            assert re.search(rf"\b{col}\b", sql), (
                f"dim_player_attributes.sql must project `{col}`"
            )

    def test_player_name_uses_coalesce_only_for_label(self):
        """Единственное место где допустим COALESCE между источниками — это
        display label (player_name_canonical). Атрибуты — нет."""
        sql = _read_sql()
        assert re.search(
            r"COALESCE\s*\([^)]*player_name", sql, re.IGNORECASE,
        ), (
            "dim_player_attributes.sql must use COALESCE(fb.player_name, "
            "fm.player_name) AS player_name_canonical"
        )

    def test_no_join_on_season_predicate(self):
        """Snapshot-зерно: JOIN на silver Silver-таблицы НЕ должен включать
        season (берём MAX_BY per player_id внутри CTE). Иначе наследуем
        cross-source season-type discrepancy (bigint 2025 vs varchar '2526')."""
        sql = _strip_comments(_read_sql())
        # Ищем JOIN ... AND ... season — это анти-pattern для этого SQL.
        # Допустимо: ROW_NUMBER OVER (... ORDER BY season DESC) — без AND.
        join_blocks = re.findall(
            r"LEFT\s+JOIN[^;]+?(?=LEFT\s+JOIN|\Z)",
            sql, re.IGNORECASE | re.DOTALL,
        )
        for blk in join_blocks:
            on_clause = re.search(r"\bON\b(.+?)(?=LEFT\s+JOIN|\Z)",
                                  blk, re.IGNORECASE | re.DOTALL)
            if on_clause:
                txt = on_clause.group(1).lower()
                # `season DESC` внутри ORDER BY — это OK; «AND ... season ...»
                # как JOIN predicate — нет.
                # Простая эвристика: если в ON-блоке есть `.season`, это plain
                # column predicate (а не ORDER BY).
                assert ".season" not in txt, (
                    "dim_player_attributes.sql JOIN on clause must NOT include "
                    "season predicate (snapshot-grain; season-type discrepancy "
                    f"между bigint и varchar): {blk[:120]!r}"
                )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "dim_player_attributes.sql must remain a pure SELECT "
            "(CTAS wrapping is done by gold_tasks.run_gold_transform)"
        )

    def test_fbref_profile_dedup_cte(self):
        """#463: silver profile grain = (player_id, squad, league, season) —
        MAX_BY(squad, season) AS current_team над multi-squad сезоном
        недетерминирован. fbref_latest must read a pre-deduped CTE
        (max-minutes club per (player, season), tiebreaker squad)."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"\bfbref_profile_dedup\s+AS\s*\(", sql, re.IGNORECASE), (
            "missing fbref_profile_dedup CTE — current_team would be "
            "nondeterministic for winter transfers (#463)"
        )
        assert re.search(
            r"PARTITION\s+BY\s+player_id\s*,\s*season\s+"
            r"ORDER\s+BY\s+minutes\s+DESC\s+NULLS\s+LAST\s*,\s*squad",
            sql, re.IGNORECASE,
        ), "fbref_profile_dedup must pick max-minutes club per (player, season)"
        assert re.search(r"FROM\s+fbref_profile_dedup", sql, re.IGNORECASE), (
            "fbref_latest must read fbref_profile_dedup, not the raw silver table"
        )
