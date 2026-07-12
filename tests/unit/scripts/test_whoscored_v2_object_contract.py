from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from scrapers.whoscored import repository

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / "scripts" / "whoscored_v2_object_contract.py"
SPEC = importlib.util.spec_from_file_location(
    "whoscored_v2_object_contract_tested", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
contract = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = contract
SPEC.loader.exec_module(contract)


@pytest.mark.unit
def test_static_migration_contract_matches_runtime_repository_inventory():
    assert set(contract.BUSINESS_TABLES) == set(repository.WHOSCORED_BUSINESS_TABLES)
    assert len(contract.BUSINESS_TABLES) == len(repository.WHOSCORED_BUSINESS_TABLES)
    assert set(contract.SCOPE_TABLES) == set(repository.SCOPE_DATASET_TABLES)
    assert set(contract.MATCH_TABLES) == set(repository.MATCH_DATASET_TABLES.values())
    assert set(contract.PREVIEW_TABLES) == set(
        repository.PREVIEW_DATASET_TABLES.values()
    )
    assert contract.MANIFEST_TABLES == (
        repository.CATALOG_MANIFEST_TABLE,
        repository.SCOPE_MANIFEST_TABLE,
        repository.MATCH_MANIFEST_TABLE,
        repository.PREVIEW_MANIFEST_TABLE,
        repository.PROFILE_MANIFEST_TABLE,
    )


@pytest.mark.unit
def test_every_business_table_has_exactly_one_current_publication_contract():
    bronze_current_tables = {
        view.removesuffix("_current")
        for view in contract.BRONZE_VIEWS
        if view.endswith("_current")
    }
    assert bronze_current_tables == set(contract.BUSINESS_TABLES) - {
        "whoscored_player_profile_versions"
    }
    assert contract.SILVER_VIEWS == ("whoscored_player_profile_current",)
    assert set(contract.BATCH_COLUMN_BY_TABLE) == set(contract.BUSINESS_TABLES)
    assert set(contract.BUSINESS_REQUIRED_COLUMNS) == set(contract.BUSINESS_TABLES)


@pytest.mark.unit
def test_deprecated_legacy_names_are_not_business_datasets():
    assert not (set(contract.DEPRECATED_ACTIVE_TABLES) & set(contract.BUSINESS_TABLES))
    assert "whoscored_player_assist_pairs" in contract.DEPRECATED_ACTIVE_TABLES
    assert set(contract.LEGACY_MIGRATION_KEYS) - set(contract.BUSINESS_TABLES) == {
        "whoscored_season_stages",
        "whoscored_player_profile",
    }
    assert set(contract.LEGACY_BUSINESS_TABLES) | set(
        contract.ADDITIVE_V2_TABLES
    ) == set(contract.BUSINESS_TABLES)
    assert set(contract.ROLLBACK_STATE_TABLES) == set(contract.MANIFEST_TABLES) | set(
        contract.ADDITIVE_V2_TABLES
    )


@pytest.mark.unit
def test_every_manifest_requires_content_addressed_raw_provenance():
    for table, columns in contract.MANIFEST_REQUIRED_COLUMNS.items():
        required = set(columns)
        assert "payload_sha256" in required, table
        if table == "whoscored_scope_ingest_manifest":
            assert "raw_uris_json" in required
        else:
            assert "raw_uri" in required
