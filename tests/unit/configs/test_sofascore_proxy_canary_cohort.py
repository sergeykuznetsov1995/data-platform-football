import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
COHORT = ROOT / "configs" / "sofascore" / "proxy_canary_cohort.json"
WORLD_CUP_COHORT = (
    ROOT / "configs" / "sofascore" / "proxy_canary_cohort_world_cup.json"
)


def test_paid_canary_cohort_is_fixed_complete_and_unique():
    payload = json.loads(COHORT.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 1
    assert payload["cohort"] == "25_matches_50_players"
    assert payload["source_tournament_id"] == 17
    assert payload["source_season_id"] == 76986
    assert payload["canonical_competition"] == "ENG-Premier League"
    assert payload["canonical_season"] == "2526"
    assert len(payload["match_ids"]) == len(set(payload["match_ids"])) == 25
    assert len(payload["player_ids"]) == len(set(payload["player_ids"])) == 50
    assert all(value.isdigit() for value in payload["match_ids"])
    assert all(value.isdigit() for value in payload["player_ids"])


def test_world_cup_match_and_player_cohorts_are_fixed_and_source_evidenced():
    payload = json.loads(WORLD_CUP_COHORT.read_text(encoding="utf-8"))

    assert payload["source_tournament_id"] == 16
    assert payload["source_season_id"] == 58210
    assert len(payload["match_ids"]) == len(set(payload["match_ids"])) == 25
    assert len(payload["player_ids"]) == len(set(payload["player_ids"])) == 50
    assert all(value.isdigit() for value in payload["player_ids"])
    evidence = payload["player_selection_evidence"]
    assert evidence["type"] == "captured_lineups"
    assert evidence["endpoint"] == "lineups"
    assert evidence["distinct_evidence_matches"] == 25
