"""Static contract for the two-engine FBref male completeness gate."""

from pathlib import Path
import re

import pytest


SQL_PATH = Path("docs/operations/sql/fbref_male_completeness_control.sql")


@pytest.mark.unit
def test_gate_crosses_active_male_registry_with_both_required_seasons():
    sql = SQL_PATH.read_text(encoding="utf-8")
    lowered = sql.casefold()

    assert "2025-2026" in sql
    assert "2026-2027" in sql
    assert "cross join required_seasons" in lowered
    assert "competition.gender = 'male'" in lowered
    assert "competition.crawl_state = 'active'" in lowered
    assert "competition.lifecycle_state in ('present', 'missing_once')" in lowered
    assert "competition.present" in lowered
    assert "expected_current_male_competitions" not in lowered


@pytest.mark.unit
def test_gate_deduplicates_schedule_match_keys_and_emits_competition_plus_total():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "regexp_extract(match_url, '/matches/([^/]+)'" in sql
    assert "row_number() over" in sql
    assert "partition by source_competition_id, source_season_id, match_id" in sql
    assert "schedule_rank = 1" in sql
    assert "grouping sets" in sql
    assert "total" in sql
    assert "current_run_matches" in sql
    assert "iceberg.bronze.fbref_page_manifest" in sql
    assert "manifest.run_id = cast(:control_run_id as varchar)" in sql


@pytest.mark.unit
def test_gate_requires_all_eight_dataset_decisions_and_direct_or_empty_proof():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()
    datasets = (
        "shot_events",
        "match_events",
        "lineups",
        "match_team_stats",
        "match_managers",
        "match_officials",
        "match_keeper_stats",
        "match_player_stats",
    )

    assert all(f"('{dataset}'" in sql for dataset in datasets)
    assert "availability_decisions = 8" in sql
    assert "current_run_match_rows = 1" in sql
    assert "availability = 'available'" in sql
    assert "typed_rows > 0" in sql
    assert re.search(
        r"availability\s+not\s+in\s*\(\s*'available',\s*'empty',"
        r"\s*'restricted',\s*'not_applicable'\s*\)",
        sql,
    )
    assert re.search(
        r"availability\s+in\s*\('empty', 'restricted', 'not_applicable'\)",
        sql,
    )
    assert "nullif(trim(availability.reason), '') is not null" in sql
    assert all(f"fbref_{dataset}" in sql for dataset in datasets)
    assert "manifest.parser_version = observation.typed_parser_version" in sql
    assert "_batch_id = cast(:control_run_id as varchar)" in sql


@pytest.mark.unit
def test_companion_control_query_proves_success_or_auditable_dead_target():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "engine: postgresql" in sql
    assert "engine: trino" in sql
    assert "fbref_control.observation_processing" in sql
    assert "fbref_control.dataset_manifest" in sql
    assert "observation.frontier_state = 'dead'" in sql
    assert "nullif(trim(observation.last_error_class), '') is not null" in sql
    assert "control_proof in ('durable', 'dead')" in sql
    assert "every returned verdict must be pass" in sql
    assert sql.count("group by grouping sets") == 2
