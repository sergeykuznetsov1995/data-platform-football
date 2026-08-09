from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
DESCRIPTIONS = ROOT / "configs" / "openmetadata" / "descriptions"
PUBLIC_TABLES = {
    "espn_schedule",
    "espn_lineup",
    "espn_matchsheet",
    "espn_ingest_manifest_v2",
    "espn_request_ledger_generation_v2",
    "espn_catalog_snapshot_v2",
}
RETIRED_INTERNAL_DESCRIPTION_TABLES = {
    "espn_schedule_generation_v2",
    "espn_lineup_generation_v2",
    "espn_matchsheet_generation_v2",
    "espn_schedule_current",
    "espn_lineup_current",
    "espn_matchsheet_current",
    "espn_scope_cutover_v2",
    "espn_legacy_baseline_v2",
}


def _load(filename: str) -> dict:
    return yaml.safe_load((DESCRIPTIONS / filename).read_text(encoding="utf-8"))


def _espn_specs() -> tuple[dict, ...]:
    specs = []
    for path in DESCRIPTIONS.glob("bronze_espn_*.yaml"):
        specs.append(yaml.safe_load(path.read_text(encoding="utf-8")))
    return tuple(specs)


def test_openmetadata_exposes_exactly_six_compact6_public_espn_objects():
    specs = _espn_specs()
    targets = {spec["table"]["fullyQualifiedName"] for spec in specs}

    assert targets == {
        f"trino_iceberg.iceberg.bronze.{table}" for table in PUBLIC_TABLES
    }
    assert all("Tier.Bronze" in spec["table"]["tags"] for spec in specs)


def test_canonical_public_entities_document_legacy_native_superset():
    expected = {
        "schedule": {"league", "season", "game", "scope_id", "event_id", "raw_uri"},
        "lineup": {"league", "season", "game", "scope_id", "athlete_id", "raw_uri"},
        "matchsheet": {"league", "season", "game", "scope_id", "team_id", "raw_uri"},
    }
    for entity, columns in expected.items():
        spec = _load(f"bronze_espn_{entity}.yaml")
        documented = {item["name"] for item in spec["columns"]}
        description = spec["table"]["description"].casefold()

        assert columns <= documented
        assert spec["table"]["fullyQualifiedName"].endswith(
            f"bronze.espn_{entity}"
        )
        assert "canonical" in description
        assert "compact6" in description
        assert f"espn_{entity}_current" not in spec["table"]["description"]


def test_openmetadata_does_not_target_acl_restricted_internal_espn_relations():
    for table in RETIRED_INTERNAL_DESCRIPTION_TABLES:
        assert not (DESCRIPTIONS / f"bronze_{table}.yaml").exists()


def test_public_control_descriptions_retain_logical_commit_boundaries():
    expected_phrases = {
        "espn_ingest_manifest_v2": ("manifest_sha256", "complete"),
        "espn_request_ledger_generation_v2": ("raw_uri", "direct-only"),
        "espn_catalog_snapshot_v2": ("registry", "never promote"),
    }
    for table, phrases in expected_phrases.items():
        text = (DESCRIPTIONS / f"bronze_{table}.yaml").read_text(encoding="utf-8")
        assert all(phrase in text.casefold() for phrase in phrases)
