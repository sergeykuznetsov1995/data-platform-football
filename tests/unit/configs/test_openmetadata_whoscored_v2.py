from __future__ import annotations

from pathlib import Path

import yaml

from scrapers.whoscored.parsers import PARSER_VERSION


ROOT = Path(__file__).resolve().parents[3]
OPENMETADATA = ROOT / "configs" / "openmetadata"
DESCRIPTIONS = OPENMETADATA / "descriptions"


def _fqn(path: Path) -> str:
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    return spec["table"]["fullyQualifiedName"]


def test_whoscored_canonical_views_are_ingested_with_view_lineage():
    metadata = (OPENMETADATA / "trino_ingestion.yaml").read_text()
    lineage = (OPENMETADATA / "trino_lineage.yaml").read_text()

    assert "includeViews: true" in metadata
    assert "processViewLineage: true" in lineage


def test_whoscored_serving_boundaries_have_openmetadata_descriptions():
    expected = {
        "bronze_whoscored_stages_current.yaml": "bronze.whoscored_stages_current",
        "bronze_whoscored_schedule_current.yaml": "bronze.whoscored_schedule_current",
        "bronze_whoscored_match_incidents_current.yaml": (
            "bronze.whoscored_match_incidents_current"
        ),
        "bronze_whoscored_match_bets_current.yaml": (
            "bronze.whoscored_match_bets_current"
        ),
        "bronze_whoscored_events_current.yaml": "bronze.whoscored_events_current",
        "bronze_whoscored_lineups_current.yaml": "bronze.whoscored_lineups_current",
        "bronze_whoscored_missing_players_current.yaml": (
            "bronze.whoscored_missing_players_current"
        ),
    }

    for filename, suffix in expected.items():
        path = DESCRIPTIONS / filename
        assert path.is_file(), filename
        assert _fqn(path).endswith(suffix)


def test_deprecated_season_stages_metadata_is_removed():
    corpus = "\n".join(
        path.read_text(encoding="utf-8") for path in DESCRIPTIONS.glob("*.yaml")
    )

    assert not (DESCRIPTIONS / "bronze_whoscored_season_stages.yaml").exists()
    assert "whoscored_season_stages" not in corpus


def test_whoscored_consumers_document_manifest_backed_schedule_view():
    for filename in (
        "silver_whoscored_player_unavailable.yaml",
        "fct_team_match_audit.yaml",
        "fct_team_season_stats.yaml",
    ):
        text = (DESCRIPTIONS / filename).read_text(encoding="utf-8")
        assert "bronze.whoscored_schedule_current" in text


def test_whoscored_bronze_descriptions_track_the_runtime_parser_version():
    version = PARSER_VERSION.rsplit("-v", 1)[-1]
    for filename in (
        "bronze_whoscored_events.yaml",
        "bronze_whoscored_events_current.yaml",
        "bronze_whoscored_lineups_current.yaml",
        "bronze_whoscored_missing_players.yaml",
        "bronze_whoscored_missing_players_current.yaml",
        "bronze_whoscored_schedule.yaml",
        "bronze_whoscored_schedule_current.yaml",
        "bronze_whoscored_stages_current.yaml",
    ):
        text = (DESCRIPTIONS / filename).read_text(encoding="utf-8")
        assert f"parser v{version}" in text, filename


def test_whoscored_event_id_descriptions_match_bigint_storage_contract():
    text = (DESCRIPTIONS / "bronze_whoscored_events.yaml").read_text(encoding="utf-8")
    assert "Глобально уникальный Opta `id`" in text
    assert "team-local sequence; не является FK" in text
    assert "ID связанного игрока (bigint" in text
