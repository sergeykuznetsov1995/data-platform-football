"""Render-smoke for ``dags/sql/silver/fotmob_manager_profile.sql`` (issue #434).

Mirror of ``fotmob_player_profile.sql`` but for COACHES. The current squad row
(``member_type='coach'``) is the mandatory driver; a player snapshot is only a
LEFT attribute fallback. Feeds gold.dim_manager nationality/dob enrichment.
This file freezes the SQL contract so a refactor cannot silently reintroduce
the zero-manager regression.

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
            "fotmob_manager_profile.sql may read fotmob_player_snapshots_current "
            "only as the optional name/dob fallback"
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
        """Squad membership, not a player-snapshot row, selects coaches."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"member_type\s*=\s*'coach'", sql, re.IGNORECASE), (
            "must filter fotmob_squad_snapshots_current on member_type='coach' "
            "(native replacement of legacy role='coach')"
        )
        # Guard against accidentally copying the player filter.
        assert not re.search(r"NOT\s+(?:ps\.)?is_coach", sql, re.IGNORECASE), (
            "must NOT carry the player-profile `NOT is_coach` filter"
        )

    def test_squad_is_driver_and_player_snapshot_is_left_fallback(self):
        """A coach absent from player snapshots must still be emitted."""
        sql = _strip_comments(_read_sql())
        assert re.search(
            r"FROM\s+squad_dedup\s+s\s+LEFT\s+JOIN\s+snapshot_fallback\s+d",
            sql, re.IGNORECASE | re.DOTALL,
        ), "final manager grain must be driven by squad_dedup"
        assert re.search(r"sq\.member_id\s+AS\s+player_id", sql, re.IGNORECASE)
        assert re.search(r"sq\.member_name\s+AS\s+name", sql, re.IGNORECASE)
        assert not re.search(
            r"FROM\s+coach_scope\s+\w+\s+JOIN\s+"
            r"iceberg\.bronze\.fotmob_player_snapshots_current",
            sql, re.IGNORECASE | re.DOTALL,
        ), "player snapshot must never be an INNER requirement"

    def test_dedup_via_row_number(self):
        from scrapers.fotmob.constants import render_fotmob_sql

        sql = render_fotmob_sql(_read_sql())
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
            r"COALESCE\s*\(\s*NULLIF\s*\(\s*TRIM\s*\(s\.date_of_birth\)"
            r".*?NULLIF\s*\(\s*TRIM\s*\(d\.birth_date\)",
            sql, re.IGNORECASE | re.DOTALL,
        ), "dob must prefer squad date_of_birth before snapshot birth_date"

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
        tail = sql.split("snapshot_fallback", 1)[1]
        assert "LPAD" not in tail and "MOD(" not in tail, (
            "season slug must be computed only in season_axis "
            "(framework check #1: no own LPAD/MOD outside the framework)"
        )

    def test_uses_canonical_league_map_placeholder(self):
        sql = _read_sql()
        assert sql.count("{{ fotmob_league_map_values_sql }}") == 1
        assert "(47,  'ENG-Premier League')" not in sql

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS-wrapping is run_silver_transform's job)"
        )


def test_squad_coach_survives_empty_player_snapshot_fixture():
    """Execute the production SQL with one squad coach and zero profile rows.

    This is the exact production failure mode behind #930: an INNER join to
    ``fotmob_player_snapshots_current`` returned zero managers even though the
    current squad carried a complete coach row.
    """
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    try:
        con.execute("""
            CREATE TABLE fotmob_competition_seasons_current (
                competition_id VARCHAR, source_season_key VARCHAR,
                is_selected BOOLEAN, is_latest BOOLEAN
            );
            CREATE TABLE fotmob_season_teams_current (
                competition_id BIGINT, source_season_key VARCHAR,
                team_id BIGINT, team_name VARCHAR
            );
            CREATE TABLE fotmob_squad_snapshots_current (
                team_id VARCHAR, member_id VARCHAR, member_type VARCHAR,
                member_name VARCHAR, member_json VARCHAR,
                date_of_birth VARCHAR, _observed_at TIMESTAMP,
                _target_batch_id VARCHAR
            );
            CREATE TABLE fotmob_player_snapshots_current (
                player_id VARCHAR, name VARCHAR, birth_date VARCHAR,
                is_coach BOOLEAN, _observed_at TIMESTAMP,
                _target_batch_id VARCHAR
            );
            CREATE TABLE fotmob_leaderboards_current (
                competition_id BIGINT, source_season_key VARCHAR,
                team_id BIGINT, participant_id BIGINT,
                participant_type VARCHAR
            );
        """)
        con.execute(
            "INSERT INTO fotmob_competition_seasons_current VALUES "
            "('47', '2025/2026', TRUE, TRUE)"
        )
        con.execute(
            "INSERT INTO fotmob_season_teams_current VALUES "
            "(47, '2025/2026', 100, 'Fixture FC')"
        )
        con.execute(
            "INSERT INTO fotmob_squad_snapshots_current VALUES "
            "('100', '9001', 'coach', 'Fixture Coach', "
            "'{\"cname\":\"Spain\"}', '1975-04-03', "
            "TIMESTAMP '2026-07-21 10:00:00', 'batch-1')"
        )

        from scrapers.fotmob.constants import render_fotmob_sql

        sql = render_fotmob_sql(_read_sql())
        replacements = {
            "iceberg.bronze.fotmob_competition_seasons_current":
                "fotmob_competition_seasons_current",
            "iceberg.bronze.fotmob_season_teams_current":
                "fotmob_season_teams_current",
            "iceberg.bronze.fotmob_squad_snapshots_current":
                "fotmob_squad_snapshots_current",
            "iceberg.bronze.fotmob_player_snapshots_current":
                "fotmob_player_snapshots_current",
            "iceberg.bronze.fotmob_leaderboards_current":
                "fotmob_leaderboards_current",
            "json_extract_scalar": "json_extract_string",
        }
        for source, local in replacements.items():
            sql = sql.replace(source, local)

        rows = con.execute(sql).fetchall()
        assert len(rows) == 1
        assert rows[0][:4] == (
            "9001", "Fixture Coach", "1975-04-03", "Spain",
        )
        assert rows[0][-2:] == ("ENG-Premier League", "2526")
    finally:
        con.close()
