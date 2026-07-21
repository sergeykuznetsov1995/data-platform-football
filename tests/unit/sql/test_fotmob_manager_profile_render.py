"""Render-smoke for ``dags/sql/silver/fotmob_manager_profile.sql`` (issue #434).

Mirror of ``fotmob_player_profile.sql`` but for COACHES (``is_coach`` /
``member_type='coach'``). Feeds gold.dim_manager nationality/dob enrichment.
This file freezes the SQL contract (coach filters, dedup, dob COALESCE,
coachId key) so a refactor cannot silently drop the manager-enrichment source.

Cutover #930: sources moved from legacy ``bronze.fotmob_player_details`` /
``fotmob_team_squad`` to native ``fotmob_player_snapshots_current`` /
``fotmob_squad_snapshots_current`` with the shared season-scope framework
(``coach_scope`` via ``season_teams_current`` x ``squad_snapshots_current``).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_manager_profile.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestFotmobManagerProfileSql:

    def test_reads_native_snapshots(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_squad_snapshots_current" in sql, (
            "fotmob_manager_profile.sql must read fotmob_squad_snapshots_current "
            "(source for coach country/dob)"
        )
        assert "iceberg.bronze.fotmob_player_snapshots_current" in sql, (
            "fotmob_manager_profile.sql must read fotmob_player_snapshots_current "
            "(coachId matching xref_manager.source_id)"
        )

    def test_no_legacy_bronze_sources(self):
        """Cutover #930: legacy tables must be gone."""
        sql = _strip_comments(_read_sql())
        assert "fotmob_player_details" not in sql, (
            "legacy bronze.fotmob_player_details must not be read after cutover"
        )
        assert "fotmob_team_squad" not in sql, (
            "legacy bronze.fotmob_team_squad must not be read after cutover"
        )
        assert not re.search(r"(?<!_target)_batch_id\b", sql), (
            "legacy _batch_id does not exist in native tables "
            "(use _target_batch_id)"
        )

    def test_season_scope_framework(self):
        """Season scope must come from the shared cutover framework CTEs."""
        sql = _strip_comments(_read_sql())
        for cte in ("league_map", "season_axis", "team_scope", "squad_scope",
                    "coach_scope"):
            assert re.search(rf"\b{cte}\b", sql), (
                f"must contain framework CTE `{cte}` "
                "(see /root/fotmob-runtime/cutover-framework.md)"
            )
        assert "iceberg.bronze.fotmob_season_teams_current" in sql, (
            "season scope must be built from fotmob_season_teams_current"
        )
        assert "iceberg.bronze.fotmob_competition_seasons_current" in sql, (
            "season axis must be built from fotmob_competition_seasons_current"
        )

    def test_filters_for_coaches_only(self):
        """Inverse of fotmob_player_profile: keep coaches, drop players."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"WHERE\s+ps\.is_coach", sql, re.IGNORECASE), (
            "must filter fotmob_player_snapshots_current WHERE ps.is_coach"
        )
        assert re.search(r"member_type\s*=\s*'coach'", sql, re.IGNORECASE), (
            "must filter fotmob_squad_snapshots_current on member_type='coach' "
            "(native replacement of legacy role='coach')"
        )
        # Guard against accidentally copying the player filter.
        assert not re.search(r"NOT\s+(?:ps\.)?is_coach", sql, re.IGNORECASE), (
            "must NOT carry the player-profile `NOT is_coach` filter"
        )

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*"
            r"PARTITION\s+BY\s+(?:\w+\.)?player_id",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY player_id, ...)"
        assert re.search(
            r"ORDER\s+BY\s+\w+\._observed_at\s+DESC\s*,"
            r"\s*\w+\._target_batch_id\s+DESC",
            sql, re.IGNORECASE,
        ), (
            "dedup must order by _observed_at DESC, _target_batch_id DESC "
            "(native lineage; legacy _ingested_at/_batch_id do not exist)"
        )

    def test_join_key_casts_squad_team_id(self):
        """squad_snapshots.team_id is varchar; coach_scope.team_id is bigint."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\s*\(\s*sq\.team_id\s+AS\s+bigint\s*\)",
            sql, re.IGNORECASE,
        ), "must CAST squad_snapshots.team_id AS bigint (native type mismatch)"

    def test_country_from_member_json(self):
        """Native squad snapshot has no typed `country` column."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"json_extract_scalar\s*\(\s*sq\.member_json\s*,\s*'\$\.cname'\s*\)",
            sql, re.IGNORECASE,
        ), "nationality must come from member_json $.cname (cutover-mapping §3.4)"

    def test_dob_coalesces_squad_then_details(self):
        """dob prefers squad date_of_birth, falls back to snapshot birth_date."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"COALESCE\s*\(\s*s\.date_of_birth\s*,\s*d\.birth_date\s*\)",
            sql, re.IGNORECASE,
        ), "dob must be COALESCE(squad.date_of_birth, details.birth_date)"

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "player_id",
            "name",
            "date_of_birth",
            "nationality",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"fotmob_manager_profile.sql must project `{col}`"
            )

    def test_season_slug_only_in_season_axis(self):
        """Season slug (LPAD/MOD idiom) is computed ONLY inside season_axis."""
        sql = _strip_comments(_read_sql())
        assert "LPAD" in sql and "MOD" in sql, (
            "season must be converted to slug (LPAD/MOD idiom, in season_axis)"
        )
        # Framework check: no slug computation outside the framework block —
        # the final SELECT must project season from the scope, not recompute it.
        tail = sql.split("details_dedup", 1)[1]
        assert "LPAD" not in tail and "MOD(" not in tail, (
            "season slug must be computed only in season_axis "
            "(framework check #1: no own LPAD/MOD outside the framework)"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS-wrapping is run_silver_transform's job)"
        )
