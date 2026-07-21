"""Render-smoke for ``dags/sql/silver/fotmob_player_profile.sql``.

T4: time-invariant snapshot для FotMob атрибутов. После cutover #930 источники —
native: fotmob_squad_snapshots_current — основной источник для
height/dob/country/country_code; fotmob_player_snapshots_current
(player_information_json) — ТОЛЬКО для `foot` (единственное поле,
отсутствующее в squad-снапшоте). Сезонный скоуп (league, season)
реконструируется каркасом [CUTOVER-FRAMEWORK #930] (season_axis/player_scope).

Этот файл фиксирует контракт SQL'я (sources, filters, dedup, JSON-extract
idiom) на случай рефактора.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_player_profile.sql"
DAG_PATH = PROJECT_ROOT / "dags" / "dag_transform_fotmob_silver.py"
METADATA_PATH = (
    PROJECT_ROOT / "configs" / "openmetadata" / "descriptions"
    / "silver_fotmob_player_profile.yaml"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobPlayerProfileSql:

    def test_reads_native_squad_and_player_snapshots(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_squad_snapshots_current" in sql, (
            "fotmob_player_profile.sql must read fotmob_squad_snapshots_current "
            "(primary source for height_cm/dob/country)"
        )
        assert "iceberg.bronze.fotmob_player_snapshots_current" in sql, (
            "fotmob_player_profile.sql must read fotmob_player_snapshots_current "
            "(needed for `foot` extraction from JSON)"
        )

    def test_no_legacy_bronze_sources(self):
        """Cutover #930: legacy bronze.fotmob_* больше не читается."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_team_squad" not in sql, (
            "legacy fotmob_team_squad must be replaced by "
            "fotmob_squad_snapshots_current (#930 cutover)"
        )
        assert "iceberg.bronze.fotmob_player_details" not in sql, (
            "legacy fotmob_player_details must be replaced by "
            "fotmob_player_snapshots_current (#930 cutover)"
        )
        assert not re.search(r"\b_batch_id\b", sql), (
            "legacy lineage column _batch_id does not exist in native "
            "tables (use _target_batch_id)"
        )
        assert not re.search(r"\b_ingested_at\b", sql), (
            "legacy lineage column _ingested_at does not exist in native "
            "tables (use _observed_at / _target_batch_id)"
        )

    def test_season_scope_from_cutover_framework(self):
        """(league, season) реконструируются ТОЛЬКО каркасом #930:
        season_axis — единственное место вычисления season-слага,
        player_scope/squad_scope — драйверы членства игрока в сезоне."""
        sql = _read_sql()
        assert "[CUTOVER-FRAMEWORK #930]" in sql, (
            "fotmob_player_profile.sql must embed the shared cutover framework "
            "block (season scope for global native snapshots)"
        )
        stripped = _strip_comments(sql)
        for cte in ("league_map", "season_axis", "player_scope", "squad_scope"):
            assert re.search(rf"\b{cte}\b", stripped), (
                f"cutover framework CTE `{cte}` must be present"
            )
        # season-слаг (LPAD/MOD) живёт только в season_axis каркаса — в файле
        # не должно остаться собственного legacy-CASE по d.season.
        assert stripped.count("LPAD") == 3, (
            "season slug must be computed ONLY inside season_axis "
            "(exactly the 3 LPAD calls of the framework block)"
        )

    def test_uses_canonical_league_map_placeholder(self):
        sql = _read_sql()
        assert sql.count("{{ fotmob_league_map_values_sql }}") == 1
        assert "(47,  'ENG-Premier League')" not in sql

    def test_filters_out_coaches(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"NOT\s+(?:ps\.)?is_coach", sql, re.IGNORECASE), (
            "fotmob_player_profile.sql must filter out coaches "
            "(is_coach=true rows pollute the player snapshot)"
        )

    def test_does_not_filter_by_position(self):
        """Snapshot должен покрывать ВСЕХ игроков (включая вратарей) —
        time-invariant атрибуты не зависят от позиции."""
        sql = _strip_comments(_read_sql())
        # silver.fotmob_player_season_profile / fotmob_keeper_profile делят
        # игроков по primary_position; здесь — единая таблица.
        assert "primary_position" not in sql, (
            "fotmob_player_profile.sql NE должна делить игроков по позиции "
            "— это time-invariant snapshot для ВСЕХ игроков"
        )

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        # Two CTEs (details_dedup + squad_dedup) с ROW_NUMBER на одинаковом
        # ключе (player_id, league, season). Pattern: existing fotmob_*_profile.
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+ps\.player_id",
            sql, re.IGNORECASE,
        ), (
            "fotmob_player_profile.sql must dedup via "
            "ROW_NUMBER OVER (PARTITION BY ps.player_id, <league>, <season>)"
        )
        assert re.search(
            r"ORDER\s+BY\s+\w+\._observed_at\s+DESC\s*,\s*\w+\._target_batch_id\s+DESC",
            sql, re.IGNORECASE,
        ), (
            "dedup ORDER BY must use native lineage "
            "(_observed_at DESC, _target_batch_id DESC)"
        )

    def test_foot_extracted_from_player_information_json(self):
        """foot выкручивается через element_at(map_from_entries(transform(...)))
        по title='Preferred foot'. Это единственный валидный pattern в Trino
        (JSONPath filter и correlated subquery с UNNEST не поддерживаются)."""
        sql = _read_sql()
        # Pattern check: map_from_entries + 'Preferred foot' lookup.
        assert "map_from_entries" in sql, (
            "fotmob_player_profile.sql must use map_from_entries idiom "
            "(JSONPath filter и correlated UNNEST не поддерживаются в Trino)"
        )
        assert "'Preferred foot'" in sql, (
            "fotmob_player_profile.sql must look up by title 'Preferred foot'"
        )
        assert "$.value.fallback" in sql, (
            "fotmob_player_profile.sql must extract value.fallback "
            "(human-readable Right/Left/Both)"
        )

    def test_squad_join_casts_team_id_to_bigint(self):
        """fotmob_squad_snapshots_current.team_id — varchar, scope-CTE каркаса
        несут bigint team_id (season_teams). JOIN-ключ требует явный CAST на
        squad-стороне (native type mismatch; cutover-mapping §2.4)."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\s*\(\s*sq\.team_id\s+AS\s+bigint\s*\)",
            sql, re.IGNORECASE,
        ), (
            "fotmob_player_profile.sql must CAST(sq.team_id AS bigint) "
            "when joining squad_snapshots to the bigint team_id scope"
        )

    def test_outputs_required_attribute_columns(self):
        """Минимальный контракт колонок: identity + 4 time-invariant attrs."""
        sql = _read_sql()
        for col in (
            "player_id",
            "player_name",
            "date_of_birth",
            "height_cm",
            "nationality",
            "country_code",
            "foot",
            "is_current_season",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_player_profile.sql must project `{col}`"
            )

    def test_current_season_flag_follows_both_scope_branches(self):
        """Roster and leaderboard rows retain season-axis truth in the output."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"player_scope\s+AS\s*\(.*?is_current_season.*?UNION\s+ALL"
            r".*?is_current_season",
            sql, re.IGNORECASE | re.DOTALL,
        )
        assert re.search(
            r"d\.scope_is_current_season\s+AS\s+is_current_season",
            sql, re.IGNORECASE,
        )

    @pytest.mark.parametrize("column", ["height_cm", "foot"])
    def test_attribute_coverage_is_current_roster_scoped(self, column):
        """Historical leaderboard rows have no historical squad attributes."""
        dag_source = DAG_PATH.read_text(encoding="utf-8")
        match = re.search(
            rf"CHECK\.coverage\(\s*'silver\.fotmob_player_profile'\s*,"
            rf"\s*column='{column}'\s*,(?P<body>.*?)\)",
            dag_source, re.DOTALL,
        )
        assert match, f"missing {column} coverage check"
        assert "where='is_current_season'" in match.group("body"), (
            f"{column} coverage must exclude historical leaderboard rows"
        )

    def test_openmetadata_declares_current_season_boolean(self):
        metadata = METADATA_PATH.read_text(encoding="utf-8")
        assert re.search(
            r"^\s*-\s+name:\s+is_current_season\s*$", metadata, re.MULTILINE
        )
        assert "current" in metadata.lower()

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fotmob_player_profile.sql must remain a pure SELECT "
            "(CTAS-wrapping is done by silver_tasks.run_silver_transform)"
        )
