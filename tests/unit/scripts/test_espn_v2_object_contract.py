from __future__ import annotations

from scripts import espn_v2_object_contract as contract


def test_native_object_contract_covers_generations_current_views_and_manifests():
    assert set(contract.GENERATION_TABLES) == {
        "espn_schedule_generation_v2",
        "espn_lineup_generation_v2",
        "espn_matchsheet_generation_v2",
    }
    assert set(contract.CURRENT_VIEWS) == {
        "espn_schedule_current",
        "espn_lineup_current",
        "espn_matchsheet_current",
    }
    assert {
        "espn_ingest_manifest_v2",
        "espn_request_ledger_generation_v2",
        "espn_scope_cutover_v2",
        "espn_catalog_snapshot_v2",
        "espn_legacy_baseline_v2",
    } <= set(contract.REQUIRED_COLUMNS)


def test_native_entities_require_source_ids_and_immutable_lineage():
    for table in (*contract.GENERATION_TABLES, *contract.CURRENT_VIEWS):
        required = contract.REQUIRED_COLUMNS[table]
        assert {
            "scope_id",
            "competition_id",
            "source_season_year",
            "event_id",
        } <= required
        assert {
            "generation_id",
            "generation_signature",
            "registry_signature",
            "plan_signature",
            "raw_uri",
            "raw_sha256",
            "_batch_id",
        } <= required
    for table in (
        "espn_lineup_generation_v2",
        "espn_lineup_current",
    ):
        assert {"team_id", "athlete_id"} <= contract.REQUIRED_COLUMNS[table]
    for table in (
        "espn_matchsheet_generation_v2",
        "espn_matchsheet_current",
    ):
        assert "team_id" in contract.REQUIRED_COLUMNS[table]


def test_lineup_and_matchsheet_empty_state_is_capability_gated():
    assert contract.CAPABILITY_GATED_TABLES == frozenset(
        {
            "espn_lineup_generation_v2",
            "espn_matchsheet_generation_v2",
            "espn_lineup_current",
            "espn_matchsheet_current",
        }
    )
