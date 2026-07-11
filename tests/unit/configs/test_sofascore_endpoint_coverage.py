"""Versioned SofaScore endpoint/table coverage matrix contracts."""

from __future__ import annotations

from pathlib import Path
from collections import Counter

import pytest

from utils.sofascore_dq import (
    COVERAGE_STATUSES,
    MANIFEST_STATES,
    load_coverage_contract,
)


pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def coverage():
    return load_coverage_contract(ROOT / "configs/sofascore/endpoint_coverage.yaml")


def test_only_four_review_outcomes(coverage):
    assert set(coverage["coverage_statuses"]) == COVERAGE_STATUSES
    assert {spec["status"] for spec in coverage["endpoints"].values()} <= COVERAGE_STATUSES


def test_manifest_states_are_exact_and_http_errors_not_success(coverage):
    assert set(coverage["manifest"]["states"]) == MANIFEST_STATES
    assert set(coverage["manifest"]["retryable_http_statuses"]) == {
        403,
        429,
        500,
        502,
        503,
        504,
    }
    assert set(coverage["manifest"]["acceptable_terminal_states"]["required"]) == {
        "success",
        "legitimate_empty",
    }


def test_exact_endpoint_outcomes_match_current_capture_wiring(coverage):
    normalized = {
        "tournament_catalog",
        "discovery_categories",
        "category_tournaments",
        "tournament_seasons",
        "schedule_last",
        "schedule_next",
        "standings_total",
        "event",
        "lineups",
        "statistics",
        "shotmap",
        "incidents",
        "player_profile",
        "player_season_statistics",
    }
    raw_only = {
        "rounds",
        "cup_trees",
        "participants",
        "squads",
        "referee_profile",
    }
    unsupported = {"stages"}
    excluded = {
        "player_event_statistics",
        "event_graph",
        "average_positions",
        "player_heatmap",
        "h2h_events",
        "odds",
        "fan_votes",
        "tv_channels",
    }
    by_status = {
        status: {
            name
            for name, spec in coverage["endpoints"].items()
            if spec["status"] == status
        }
        for status in COVERAGE_STATUSES
    }
    assert by_status == {
        "normalized": normalized,
        "raw-only": raw_only,
        "unsupported": unsupported,
        "intentionally-excluded": excluded,
    }
    assert coverage["coverage_summary"] == {
        "normalized": 14,
        "raw-only": 5,
        "unsupported": 1,
        "intentionally-excluded": 8,
    }
    assert Counter(
        spec["status"] for spec in coverage["endpoints"].values()
    ) == Counter(coverage["coverage_summary"])


def test_lineup_incident_event_bronze_names_are_canonical(coverage):
    endpoints = coverage["endpoints"]
    assert "bronze.sofascore_events" in endpoints["event"]["destination"]
    assert "bronze.sofascore_event_participants" in endpoints["event"]["destination"]
    assert "bronze.sofascore_lineups" in endpoints["lineups"]["destination"]
    assert "bronze.sofascore_incidents" in endpoints["incidents"]["destination"]
    assert not {
        "bronze.sofascore_event_metadata",
        "bronze.sofascore_lineup",
        "bronze.sofascore_incident",
    } & set(coverage["tables"])


def test_new_bronze_tables_require_raw_replay_lineage(coverage):
    for table_name in (
        "bronze.sofascore_events",
        "bronze.sofascore_event_participants",
        "bronze.sofascore_lineups",
        "bronze.sofascore_incidents",
    ):
        required = set(coverage["tables"][table_name]["required_columns"])
        assert {
            "source_tournament_id",
            "source_season_id",
            "raw_content_hash",
            "raw_blob_key",
        } <= required


def test_every_normalized_destination_has_grain_key_dq_and_downstream(coverage):
    tables = coverage["tables"]
    for name, endpoint in coverage["endpoints"].items():
        assert endpoint["grain"], name
        assert endpoint["natural_key"], name
        assert "empty_semantics" in endpoint, name
        if endpoint["status"] == "normalized":
            assert endpoint["dq"], name
            for destination in endpoint["destination"]:
                if destination.startswith(("bronze.", "silver.", "gold.")):
                    assert destination in tables, (name, destination)
                    assert tables[destination]["grain"]
                    assert tables[destination]["natural_key"]
                    assert "downstream" in tables[destination]


def test_every_claimed_table_has_a_real_materializer_file_and_status(coverage):
    for table_name, table in coverage["tables"].items():
        relative_path = table["materialized_by"].split("#", 1)[0]
        materializer = ROOT / relative_path
        assert materializer.is_file(), (table_name, relative_path)
        assert table_name.split(".")[-1] in materializer.read_text(encoding="utf-8")
        assert table["production_write_status"]


def test_raw_only_and_excluded_payloads_have_reason(coverage):
    expected = {
        "event_graph": "intentionally-excluded",
        "average_positions": "intentionally-excluded",
        "player_heatmap": "intentionally-excluded",
        "h2h_events": "intentionally-excluded",
        "odds": "intentionally-excluded",
        "fan_votes": "intentionally-excluded",
        "tv_channels": "intentionally-excluded",
    }
    for endpoint, status in expected.items():
        spec = coverage["endpoints"][endpoint]
        assert spec["status"] == status
        assert spec["reason"]
        assert spec["destination"] == []
        assert spec["preserved_arrays"] == []
    for endpoint in ("rounds", "cup_trees", "participants", "squads"):
        spec = coverage["endpoints"][endpoint]
        assert spec["status"] == "raw-only"
        assert spec["reason"]
        assert spec["destination"] == []
        assert spec["preserved_arrays"]
    referee = coverage["endpoints"]["referee_profile"]
    assert referee["status"] == "raw-only"
    assert referee["reason"]
    assert referee["required_json_paths"] == ["/referee"]


def test_missing_group_playoff_round_and_bracket_tables_are_not_claimed(coverage):
    assert coverage["endpoints"]["standings_total"]["status"] == "normalized"
    assert coverage["endpoints"]["rounds"]["status"] == "raw-only"
    assert coverage["endpoints"]["cup_trees"]["status"] == "raw-only"
    assert coverage["endpoints"]["stages"]["status"] == "unsupported"
    assert {
        "silver.sofascore_standings",
        "silver.sofascore_round",
        "silver.sofascore_cup_bracket",
    }.isdisjoint(coverage["tables"])


def test_post_shot_fact_is_claimed_only_after_real_dag_wiring(coverage):
    table = "gold.fct_sofascore_team_match_post_shot_xg"
    assert (ROOT / "dags/sql/gold/fct_sofascore_team_match_post_shot_xg.sql").is_file()
    assert table in coverage["tables"]
    assert table in coverage["endpoints"]["shotmap"]["destination"]
    assert "fct_sofascore_team_match_post_shot_xg" in (
        ROOT / "dags/dag_transform_e3.py"
    ).read_text(encoding="utf-8")
    fact = coverage["tables"][table]
    assert fact["natural_key"] == ["match_id", "team_id"]
    assert fact["allowed_values"]["metric_source"] == ["sofascore"]


def test_no_nonexistent_silver_contracts_are_advertised(coverage):
    nonexistent = {
        "silver.sofascore_standings",
        "silver.sofascore_event_metadata",
        "silver.sofascore_lineup",
        "silver.sofascore_match_statistics",
        "silver.sofascore_incident",
        "silver.sofascore_round",
        "silver.sofascore_cup_bracket",
        "silver.sofascore_team",
        "silver.sofascore_squad",
        "silver.sofascore_referee",
    }
    assert nonexistent.isdisjoint(coverage["tables"])


def test_claimed_physical_tables_are_the_exact_current_sofascore_set(coverage):
    claimed = set(coverage["tables"])
    assert {name for name in claimed if name.startswith("bronze.")} == {
        "bronze.sofascore_schedule",
        "bronze.sofascore_league_table",
        "bronze.sofascore_events",
        "bronze.sofascore_event_participants",
        "bronze.sofascore_lineups",
        "bronze.sofascore_incidents",
        "bronze.sofascore_player_ratings",
        "bronze.sofascore_event_player_stats",
        "bronze.sofascore_match_stats",
        "bronze.sofascore_event_shotmap",
        "bronze.sofascore_venue",
        "bronze.sofascore_match_capture_status",
        "bronze.sofascore_player_universe",
        "bronze.sofascore_player_profile",
        "bronze.sofascore_player_season_stats",
    }
    assert {name for name in claimed if name.startswith("silver.")} == {
        "silver.sofascore_league_table",
        "silver.sofascore_player_ratings",
        "silver.sofascore_player_match_aggregate",
        "silver.sofascore_team_match",
        "silver.sofascore_shots",
        "silver.sofascore_venue",
        "silver.sofascore_player_profile",
        "silver.sofascore_player_season_aggregate",
    }
    assert {name for name in claimed if name.startswith("gold.")} == {
        "gold.fct_sofascore_team_match_post_shot_xg"
    }


def test_normalized_match_endpoints_are_exactly_pipeline_event_paths(coverage):
    from scrapers.sofascore.pipeline import EVENT_PATHS, PLAYER_PATHS

    normalized_match = {
        name
        for name, spec in coverage["endpoints"].items()
        if spec["status"] == "normalized" and spec["target_type"] == "event"
    }
    assert normalized_match == set(EVENT_PATHS)
    assert set(coverage["manifest"]["raw_replay_endpoints"]) == (
        set(EVENT_PATHS)
        | set(PLAYER_PATHS)
        | {
            "schedule_last",
            "schedule_next",
            "standings_total",
            "rounds",
            "cup_trees",
            "participants",
            "squads",
            "referee_profile",
        }
    )


def test_raw_replay_tournament_and_player_writes_are_explicit(coverage):
    assert coverage["endpoints"]["schedule_last"]["required"] is True
    assert coverage["endpoints"]["schedule_next"]["required"] is True
    assert coverage["endpoints"]["standings_total"]["required"] is False
    assert coverage["endpoints"]["player_profile"]["required"] is True
    assert coverage["endpoints"]["player_season_statistics"]["required"] is False
    for table in (
        "bronze.sofascore_schedule",
        "bronze.sofascore_league_table",
        "bronze.sofascore_player_profile",
        "bronze.sofascore_player_season_stats",
    ):
        assert "replay/no-op wired" in coverage["tables"][table][
            "production_write_status"
        ]


def test_fresh_bootstrap_declares_both_sides_of_every_coalesce(coverage):
    compat = coverage["bronze_compatibility_columns"]
    assert ["home_team", "home_team_name"] in compat["sofascore_schedule"]
    assert ["minute", "time"] in compat["sofascore_event_shotmap"]
    assert ["height_cm", "height"] in compat["sofascore_player_profile"]
    assert ["stat_key", "key", "statistics_type"] in compat[
        "sofascore_match_stats"
    ]
