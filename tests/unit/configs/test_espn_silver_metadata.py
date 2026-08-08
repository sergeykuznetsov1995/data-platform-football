"""Focused semantic contracts for ESPN native-v2 Silver catalog metadata."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml


pytestmark = pytest.mark.unit


ROOT = Path(__file__).resolve().parents[3]
DESCRIPTIONS = ROOT / "configs" / "openmetadata" / "descriptions"


def _description(name: str) -> dict:
    return yaml.safe_load((DESCRIPTIONS / f"silver_espn_{name}.yaml").read_text())


def _column(spec: dict, name: str) -> dict:
    return next(column for column in spec["columns"] if column["name"] == name)


def test_child_event_foreign_keys_target_espn_match():
    parent = "trino_iceberg.iceberg.silver.espn_match.event_id"
    for name in ("team_match", "player_match_aggregate", "match_events", "substitutions"):
        assert _description(name)["relationships"] == [{
            "from": "event_id",
            "to": parent,
            "type": "FOREIGN_KEY",
            "description": "N:1 — дочерняя запись относится к одному матчу ESPN.",
        }]


def test_exact_pii_mapping_for_espn_tables_and_name_columns():
    expected_table_pii = {
        "match": "PII.Low",
        "team_match": "PII.None",
        "player_match_aggregate": "PII.Low",
        "match_events": "PII.Low",
        "substitutions": "PII.Low",
        "venue": "PII.None",
    }
    expected_column_pii = {
        "match": {"referee"},
        "team_match": set(),
        "player_match_aggregate": {"player_name"},
        "match_events": {"player_name"},
        "substitutions": {"player_in_name", "player_out_name"},
        "venue": set(),
    }
    for name, pii in expected_table_pii.items():
        spec = _description(name)
        assert {tag for tag in spec["table"]["tags"] if tag.startswith("PII.")} == {pii}
        actual_columns = {
            column["name"]
            for column in spec["columns"]
            if "PII.Low" in column.get("tags", [])
        }
        assert actual_columns == expected_column_pii[name]


def test_metadata_pins_season_and_data_quality_caveats():
    match = _description("match")
    season_slug = _column(match, "season_slug_platform")["description"]
    assert "displayName" in season_slug and "YYZZ" in season_slug
    assert "partition" in season_slug.lower() and "season" in season_slug

    team = _description("team_match")["table"]["description"].lower()
    assert "metric not tracked" in team and "zero" in team

    penalty = _column(_description("match_events"), "is_penalty")["description"]
    assert "penaltyKick" in penalty and "is_shootout" in penalty

    substitutions = _description("substitutions")["table"]["description"]
    assert "played_final" in substitutions and "исключ" in substitutions.lower()
