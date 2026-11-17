"""Versioned SofaScore endpoint/table coverage matrix contracts."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from pathlib import Path

import pytest

from utils.sofascore_dq import (
    COVERAGE_STATUSES,
    MANIFEST_STATES,
    SofaScoreContractError,
    load_coverage_contract,
    validate_coverage_contract,
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
        "not_supported",
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
        "rounds",
        "cup_trees",
        "participants",
        "referee_profile",
    }
    raw_only: set[str] = set()
    unsupported = {"stages", "squads"}
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
        "normalized": 18,
        "raw-only": 0,
        "unsupported": 2,
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
        "bronze.sofascore_rounds",
        "bronze.sofascore_cup_trees",
        "bronze.sofascore_participants",
        "bronze.sofascore_referee_profile",
    ):
        required = set(coverage["tables"][table_name]["required_columns"])
        assert {
            "source_tournament_id",
            "source_season_id",
            "raw_content_hash",
            "raw_blob_key",
        } <= required


def test_participant_gender_is_a_male_only_pre_write_enum(coverage):
    participants = coverage["tables"]["bronze.sofascore_event_participants"]
    assert "gender" in participants["required_columns"]
    assert participants["allowed_values"]["gender"] == ["M"]
    assert participants["allowed_values"]["team_side"] == ["home", "away"]


def test_lineup_jersey_number_has_an_exact_bigint_contract(coverage):
    lineups = coverage["tables"]["bronze.sofascore_lineups"]
    assert lineups["column_types"] == {"jersey_number": "bigint"}


def test_table_column_types_reject_unsupported_sql_types(coverage):
    invalid = deepcopy(coverage)
    invalid["tables"]["bronze.sofascore_lineups"]["column_types"] = {
        "jersey_number": "double"
    }

    with pytest.raises(SofaScoreContractError, match="column_types"):
        validate_coverage_contract(invalid)


@pytest.mark.parametrize("sql_type", [[], {}])
def test_table_column_types_reject_unhashable_sql_types(coverage, sql_type):
    invalid = deepcopy(coverage)
    invalid["tables"]["bronze.sofascore_lineups"]["column_types"] = {
        "jersey_number": sql_type
    }

    with pytest.raises(SofaScoreContractError, match="column_types"):
        validate_coverage_contract(invalid)


@pytest.mark.parametrize(
    "column_types",
    [[], {"jersey-number": "bigint"}],
)
def test_table_column_types_require_a_safe_column_mapping(coverage, column_types):
    invalid = deepcopy(coverage)
    invalid["tables"]["bronze.sofascore_lineups"]["column_types"] = column_types

    with pytest.raises(SofaScoreContractError, match="column_types"):
        validate_coverage_contract(invalid)


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
        claimed = table["materialized_by"]
        # A table may have more than one writer: the refresh lane writes
        # ``bronze.sofascore_schedule`` directly, next to the capture engine
        # (Sol round 20).  Every one of them has to be a real file that
        # actually mentions the table.
        writers = [claimed] if isinstance(claimed, str) else claimed
        assert writers, table_name
        for writer in writers:
            relative_path = writer.split("#", 1)[0]
            materializer = ROOT / relative_path
            assert materializer.is_file(), (table_name, relative_path)
            assert table_name.split(".")[-1] in materializer.read_text(
                encoding="utf-8"
            )
        assert table["production_write_status"]


def test_excluded_and_unsupported_payloads_have_reason(coverage):
    expected = {
        "player_event_statistics": "intentionally-excluded",
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
    squads = coverage["endpoints"]["squads"]
    assert squads["status"] == "unsupported"
    assert squads["reason"]
    assert squads["destination"] == []
    assert squads["preserved_arrays"] == []
    # The runner now publishes every retained season payload: no endpoint
    # may stay raw-only against the real writer.
    assert not {
        name
        for name, spec in coverage["endpoints"].items()
        if spec["status"] == "raw-only"
    }


def test_season_capture_bronze_publications_match_the_real_writer(coverage):
    committed = set(coverage["committed_state_dq"]["duplicate_tables"])
    silver_gold_tables = {
        name
        for name in coverage["tables"]
        if name.startswith(("silver.", "gold."))
    }
    for endpoint_name, table_name, key in (
        ("rounds", "bronze.sofascore_rounds", ["league", "season", "round_id"]),
        (
            "cup_trees",
            "bronze.sofascore_cup_trees",
            ["league", "season", "cup_tree_id", "node_id"],
        ),
        (
            "participants",
            "bronze.sofascore_participants",
            ["league", "season", "team_id"],
        ),
        (
            "referee_profile",
            "bronze.sofascore_referee_profile",
            ["league", "season", "requested_referee_id"],
        ),
    ):
        endpoint = coverage["endpoints"][endpoint_name]
        assert endpoint["status"] == "normalized", endpoint_name
        assert endpoint["destination"] == [table_name], endpoint_name
        table = coverage["tables"][table_name]
        assert table["grain"]
        assert table["natural_key"] == key
        assert set(key) <= set(table["required_columns"])
        assert table["partition_columns"] == ["league", "season"]
        assert table["materialized_by"] == (
            "dags/scripts/run_sofascore_scraper.py#_run_season_capture_engine"
        )
        assert "replay/no-op wired" in table["production_write_status"]
        assert {
            "source_tournament_id",
            "source_season_id",
            "raw_content_hash",
            "raw_blob_key",
            "league",
            "season",
            "_ingested_at",
        } <= set(table["required_columns"])
        assert table_name in committed
        assert not set(table["downstream"]) & silver_gold_tables
    rounds = coverage["tables"]["bronze.sofascore_rounds"]
    assert "is_current" in rounds["required_columns"]
    cup_trees = coverage["tables"]["bronze.sofascore_cup_trees"]
    assert {"parent_node_id", "children_json"} <= set(cup_trees["required_columns"])
    referee = coverage["tables"]["bronze.sofascore_referee_profile"]
    assert {"requested_referee_id", "referee_id", "slug"} <= set(
        referee["required_columns"]
    )
    assert coverage["endpoints"]["referee_profile"]["required_json_paths"] == [
        "/referee"
    ]


def test_season_capture_columns_match_parser_anchors(coverage):
    from scrapers.sofascore.season_fields import (
        parse_cup_trees,
        parse_referee_profile,
    )

    cup_row = parse_cup_trees(
        {"cupTrees": [{"id": 1, "nodes": [{"id": 2}]}]},
        source_tournament_id=17,
        source_season_id=76986,
    )[0]
    referee_row = parse_referee_profile(
        {"referee": {"id": 9, "slug": "canonical-referee"}},
        source_tournament_id=17,
        source_season_id=76986,
        requested_referee_id=8,
    )[0]
    assert "children_json" in cup_row
    assert "children_node_ids" not in cup_row
    assert "slug" in referee_row
    assert "referee_slug" not in referee_row


def test_missing_group_playoff_round_and_bracket_tables_are_not_claimed(coverage):
    assert coverage["endpoints"]["standings_total"]["status"] == "normalized"
    assert coverage["endpoints"]["rounds"]["status"] == "normalized"
    assert coverage["endpoints"]["cup_trees"]["status"] == "normalized"
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
        "bronze.sofascore_rounds",
        "bronze.sofascore_cup_trees",
        "bronze.sofascore_participants",
        "bronze.sofascore_referee_profile",
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


def test_every_normalized_table_has_a_stage_appropriate_duplicate_gate(coverage):
    committed = set(coverage["committed_state_dq"]["duplicate_tables"])
    later = set(
        coverage["committed_state_dq"]["later_stage_duplicate_tables"]
    )
    assert not committed & later
    assert committed | later == set(coverage["tables"])
    assert later == {"silver.sofascore_player_ratings"}
    e4_dq = (ROOT / "dags/utils/e4_dq.py").read_text(encoding="utf-8")
    assert "iceberg.silver.sofascore_player_ratings" in e4_dq
    assert "CHECK.no_duplicates" in e4_dq


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


def test_refresh_lane_reuses_the_measured_season_page_endpoint(coverage):
    # Lane F walks season pages, not a by-date feed: the source answered 404
    # for every by-date shape on 2026-08-25, so the lane rides the class the
    # campaign already measures and the coverage file carries no date feed.
    from scrapers.sofascore.schedule_refresh import (
        SCHEDULE_PAGE_ENDPOINT,
        SCHEDULE_PAGE_TARGET_TYPE,
    )

    assert "scheduled_events" not in coverage["endpoints"]
    spec = coverage["endpoints"][SCHEDULE_PAGE_ENDPOINT]
    assert spec["target_type"] == SCHEDULE_PAGE_TARGET_TYPE == "season_page"
    assert spec["destination"] == ["bronze.sofascore_schedule"]
    assert spec["path"] == (
        "/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}"
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


def test_completeness_journal_is_a_table_free_read_model_contract(coverage):
    journal = coverage["completeness_journal"]
    assert journal["version"] == "schedule_core_x_ops_manifest_v1"
    assert journal["sources"] == {
        "schedule": "bronze.sofascore_schedule",
        "manifest": "iceberg.ops.sofascore_capture_manifest",
    }
    assert journal["read_model"] == "sql_projection"
    assert journal["match_identity"] == "game_id across ENG/SS"
    assert journal["partition_key"] == "SS-<id>"
    # Contract only: никакая materialized таблица не заявлена.
    assert "table" not in journal
    assert "completeness_journal" not in coverage["tables"]


def test_completeness_journal_core_endpoints_equal_endpoint_journal(coverage):
    from deploy.sofascore.endpoint_journal import CORE_ENDPOINTS

    journal = coverage["completeness_journal"]
    assert journal["core_endpoints"] == list(CORE_ENDPOINTS)
    assert journal["core_endpoints"] == coverage["committed_state_dq"][
        "required_event_endpoints"
    ]


def test_completeness_journal_statuses_match_endpoint_journal_semantics(coverage):
    from deploy.sofascore.endpoint_journal import (
        JOURNAL_STATUSES,
        TERMINAL_STATUSES,
    )

    journal = coverage["completeness_journal"]
    assert journal["statuses"] == ["not_collected", "collected", "source_404", "quarantine"]
    assert set(journal["statuses"]) == set(JOURNAL_STATUSES)
    assert journal["complete_statuses"] == ["collected", "source_404"]
    assert set(journal["complete_statuses"]) == set(TERMINAL_STATUSES)
    assert set(journal["complete_statuses"]) <= set(journal["statuses"])
    assert journal["missing_endpoint_status"] == "not_collected"


def test_completeness_journal_manifest_status_mapping_is_consistent(coverage):
    journal = coverage["completeness_journal"]
    mapping = journal["manifest_status_mapping"]
    assert set(mapping) == set(coverage["manifest"]["states"])
    assert mapping == {
        "success": ["collected"],
        "legitimate_empty": ["collected"],
        "not_supported": ["source_404", "not_collected"],
        "retryable_failure": ["not_collected"],
        "schema_error": ["quarantine"],
    }
    for targets in mapping.values():
        assert set(targets) <= set(journal["statuses"])
    reached = {status for targets in mapping.values() for status in targets}
    assert set(journal["complete_statuses"]) <= reached
    assert journal["missing_endpoint_status"] not in journal["complete_statuses"]


def test_completeness_journal_extra_root_key_keeps_validator_green(coverage):
    # Root validator допускает дополнительный key: контракт остаётся валидным.
    validate_coverage_contract(coverage)
