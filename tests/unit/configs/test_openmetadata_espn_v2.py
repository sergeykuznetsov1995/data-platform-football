from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
DESCRIPTIONS = ROOT / "configs" / "openmetadata" / "descriptions"


def _load(filename: str) -> dict:
    return yaml.safe_load((DESCRIPTIONS / filename).read_text(encoding="utf-8"))


def test_all_espn_native_boundaries_have_bronze_descriptions():
    tables = (
        "espn_schedule_generation_v2",
        "espn_lineup_generation_v2",
        "espn_matchsheet_generation_v2",
        "espn_schedule_current",
        "espn_lineup_current",
        "espn_matchsheet_current",
        "espn_ingest_manifest_v2",
        "espn_scope_cutover_v2",
        "espn_request_ledger_generation_v2",
        "espn_catalog_snapshot_v2",
        "espn_legacy_baseline_v2",
    )
    for table in tables:
        path = DESCRIPTIONS / f"bronze_{table}.yaml"
        assert path.is_file(), path.name
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert spec["table"]["fullyQualifiedName"].endswith(f"bronze.{table}")
        assert "Tier.Bronze" in spec["table"]["tags"]


def test_native_entity_metadata_documents_numeric_source_ids_and_lineage():
    expected = {
        "espn_schedule_generation_v2": {
            "scope_id",
            "competition_id",
            "event_id",
            "home_team_id",
            "away_team_id",
            "raw_uri",
            "raw_sha256",
        },
        "espn_lineup_generation_v2": {
            "scope_id",
            "competition_id",
            "event_id",
            "team_id",
            "athlete_id",
            "raw_uri",
            "raw_sha256",
        },
        "espn_matchsheet_generation_v2": {
            "scope_id",
            "competition_id",
            "event_id",
            "team_id",
            "raw_uri",
            "raw_sha256",
        },
    }
    for table, columns in expected.items():
        spec = _load(f"bronze_{table}.yaml")
        documented = {item["name"] for item in spec["columns"]}
        assert columns <= documented
        assert "native" in spec["table"]["description"].casefold()


def test_current_views_and_entities_document_manifest_cutover_and_capabilities():
    for entity in ("schedule", "lineup", "matchsheet"):
        text = (DESCRIPTIONS / f"bronze_espn_{entity}_current.yaml").read_text(
            encoding="utf-8"
        )
        assert "COMPLETE" in text
        assert "cutover" in text.casefold()
        assert "legacy" in text.casefold()
    for entity in ("lineup", "matchsheet"):
        corpus = "\n".join(
            (DESCRIPTIONS / f"bronze_espn_{entity}{suffix}.yaml").read_text(
                encoding="utf-8"
            )
            for suffix in ("_generation_v2", "_current")
        )
        assert "proven" in corpus
        assert "valid_empty" in corpus
        assert "partial" in corpus
        assert "absent" in corpus


def test_manifest_cutover_and_baseline_descriptions_state_safety_boundary():
    manifest = (DESCRIPTIONS / "bronze_espn_ingest_manifest_v2.yaml").read_text(
        encoding="utf-8"
    )
    cutover = (DESCRIPTIONS / "bronze_espn_scope_cutover_v2.yaml").read_text(
        encoding="utf-8"
    )
    baseline = (DESCRIPTIONS / "bronze_espn_legacy_baseline_v2.yaml").read_text(
        encoding="utf-8"
    )
    assert "manifest_sha256" in manifest and "COMPLETE" in manifest
    assert "append-only" in cutover and "rollback" in cutover
    assert "snapshot" in baseline.casefold() and "never" in baseline.casefold()


def test_retained_legacy_descriptions_no_longer_claim_the_production_runtime():
    for entity in ("schedule", "lineup", "matchsheet"):
        text = (DESCRIPTIONS / f"bronze_espn_{entity}.yaml").read_text(encoding="utf-8")
        assert "soccerdata" not in text.casefold()
        assert "retained" in text.casefold()
        assert "rollback" in text.casefold()
