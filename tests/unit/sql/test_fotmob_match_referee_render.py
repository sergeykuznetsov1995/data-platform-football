"""Render-smoke для ``dags/sql/silver/fotmob_match_referee.sql``.

Issue #290: материализуем СТРАНУ судьи из FotMob (FBref/MatchHistory её не дают).
Issue #930: cutover источника — legacy ``bronze.fotmob_match_details`` →
native ``bronze.fotmob_match_payloads_current`` (league из competition_id через
league_map, season из source_season_key, дедуп делает сама ``_current``-view).
Этот файл фиксирует контракт SQL'я (источник, dedup, JSON-extract пути, колонки)
на случай рефактора.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_match_referee.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobMatchRefereeSql:

    def test_reads_native_match_payloads_current(self):
        """#930: источник — native current-view, legacy bronze больше не читаем."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_match_payloads_current" in sql, (
            "fotmob_match_referee.sql must read bronze.fotmob_match_payloads_current"
        )
        assert "fotmob_match_details" not in sql, (
            "legacy bronze.fotmob_match_details must not be referenced after #930 cutover"
        )

    def test_extracts_all_three_referee_json_paths(self):
        """text + country + countryCode — страна судьи это смысл задачи #290."""
        sql = _strip_comments(_read_sql())
        for path in (
            "$.infoBox.Referee.text",
            "$.infoBox.Referee.country",
            "$.infoBox.Referee.countryCode",
        ):
            assert path in sql, (
                f"fotmob_match_referee.sql must extract `{path}`"
            )

    def test_dedup_delegated_to_current_view(self):
        """Одна (последняя) строка на матч: это гарантирует `_current`-view
        (манифест-гейт + идентичность target_type/match_id), поэтому SQL обязан
        читать view, а не сырую append-only таблицу, и не тащить legacy-идиому
        ROW_NUMBER ... ORDER BY _ingested_at DESC."""
        sql = _strip_comments(_read_sql())
        assert not re.search(r"fotmob_match_payloads\b", sql), (
            "must not read the raw append-only bronze.fotmob_match_payloads "
            "(bypasses the manifest commit-gate and dedup of the _current view)"
        )
        assert not re.search(r"ORDER\s+BY\s+_ingested_at\s+DESC", sql, re.IGNORECASE), (
            "legacy ROW_NUMBER/_ingested_at dedup idiom must be gone: "
            "the _current view already yields one committed row per match"
        )

    def test_league_and_season_derived_from_native_scope(self):
        """#930: league — из competition_id (varchar → bigint → league_map,
        INNER JOIN скоупит прежними 14 лигами); season — год-старта из
        source_season_key ('2025/2026'), далее прежний legacy-CASE слага."""
        sql = _strip_comments(_read_sql())
        assert "league_map" in sql, (
            "fotmob_match_referee.sql must map competition_id -> league via league_map"
        )
        assert re.search(
            r"CAST\s*\(\s*p\.competition_id\s+AS\s+bigint\s*\)", sql, re.IGNORECASE,
        ), (
            "payloads.competition_id is varchar — must be CAST to bigint "
            "for the league_map join"
        )
        assert re.search(
            r"substr\s*\(\s*p\.source_season_key\s*,\s*1\s*,\s*4\s*\)",
            sql, re.IGNORECASE,
        ), (
            "season year-start must come from substr(source_season_key, 1, 4) "
            "(never from the season-key form)"
        )
        assert re.search(r"WHEN\s+league\s*=\s*'INT-World Cup'", sql, re.IGNORECASE), (
            "season slug must keep the legacy INT-World Cup 4-digit CASE branch"
        )

    def test_filters_out_empty_referee_name(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"Referee\.text'\)\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE), (
            "fotmob_match_referee.sql must drop rows without a referee name"
        )
        assert re.search(r"Referee\.text'\)\s*<>\s*''", sql), (
            "fotmob_match_referee.sql must drop rows with an empty referee name"
        )

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "match_id",
            "league",
            "season",
            "referee_name",
            "referee_country",
            "referee_country_code",
            "_bronze_ingested_at",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_match_referee.sql must project `{col}`"
            )

    def test_match_id_kept_varchar(self):
        """Контракт silver: match_id остаётся строкой (Gold/xref завязаны на varchar),
        native payloads отдаёт bigint → обязателен CAST."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\s*\(\s*p\.match_id\s+AS\s+varchar\s*\)", sql, re.IGNORECASE,
        ), (
            "native match_id is bigint — must be CAST to varchar to keep the "
            "silver output contract"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "fotmob_match_referee.sql must remain a pure SELECT "
            "(CTAS-wrapping is done by silver_tasks.run_silver_transform)"
        )
