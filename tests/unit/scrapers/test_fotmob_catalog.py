from __future__ import annotations

import pytest

from scrapers.fotmob.catalog import (
    CatalogConflictError,
    SelectedSeasonMismatch,
    classify_competition,
    competition_from_league_payload,
    discover_competitions,
    parse_seasons,
    validate_selected_season,
)
from scrapers.fotmob.domain import (
    CompetitionRef,
    ScopeClassification,
    ScopeDecision,
)


def test_all_leagues_deduplicates_by_numeric_id_and_merges_country_context():
    payload = {
        "popular": [
            {"id": "47", "name": "Premier League", "pageUrl": "/leagues/47/overview/premier-league"},
            {"id": 42, "name": "Champions League", "type": "league"},
        ],
        "countries": [
            {
                "ccode": "ENG",
                "name": "England",
                "leagues": [
                    {"id": 47, "name": "Premier League", "gender": "male"},
                    {"id": 48, "name": "Championship"},
                ],
            }
        ],
    }

    result = discover_competitions(payload)

    assert [item.competition_id for item in result.competitions] == [42, 47, 48]
    premier_league = result.by_id[47]
    assert premier_league.country_code == "ENG"
    assert premier_league.country_name == "England"
    assert premier_league.gender == "male"
    assert len(premier_league.source_paths) == 2
    assert result.conflicts == ()


def test_all_leagues_surfaces_conflicting_duplicate_ids_and_strict_mode_fails():
    payload = {
        "international": [
            {"id": 289, "name": "Africa Cup of Nations", "type": "league"},
            {"id": 289, "name": "Asian Cup", "type": "league"},
        ]
    }

    discovery = discover_competitions(payload)
    assert discovery.conflicts[0].competition_id == 289
    assert "name" in discovery.conflicts[0].fields
    with pytest.raises(CatalogConflictError, match="289"):
        discover_competitions(payload, strict_conflicts=True)


@pytest.mark.parametrize(
    ("competition", "expected", "rule"),
    [
        (CompetitionRef(1, "Women's Super League"), ScopeDecision.EXCLUDED, "exclude_female"),
        (CompetitionRef(2, "UEFA U21 Championship"), ScopeDecision.EXCLUDED, "exclude_youth"),
        (CompetitionRef(3, "Premier Reserve League"), ScopeDecision.EXCLUDED, "exclude_reserve"),
        (CompetitionRef(9084, "Premier League 2"), ScopeDecision.EXCLUDED, "exclude_reserve"),
        (CompetitionRef(4, "Club Friendlies"), ScopeDecision.EXCLUDED, "exclude_friendly"),
        (CompetitionRef(47, "Premier League"), ScopeDecision.INCLUDED, "include_male_senior_default"),
        (CompetitionRef(5, "League", gender="mixed"), ScopeDecision.REVIEW_REQUIRED, "review_unknown_gender"),
        (CompetitionRef(6, "League", age_group="academy"), ScopeDecision.EXCLUDED, "exclude_youth"),
    ],
)
def test_male_senior_scope_classifier_is_auditable(competition, expected, rule):
    result = classify_competition(competition)
    assert result.decision is expected
    assert result.policy_rule == rule


def test_scope_classifier_hook_can_override_ambiguous_source_metadata():
    competition = CompetitionRef(999, "Ambiguous Invitational")

    def include_known(item):
        return ScopeClassification(item, ScopeDecision.INCLUDED, "curated official", "curated")

    assert classify_competition(competition, hooks=(include_known,)).policy_rule == "curated"


def test_season_discovery_unions_all_exact_source_lists_without_derivation():
    payload = {
        "details": {"id": 289, "selectedSeason": "2025"},
        "allAvailableSeasons": ["2025", "2023"],
        "stats": {
            "seasonsWithLinks": ["2025", "2017/2019"],
            "seasonStatLinks": [{"Name": "2017/2018"}],
        },
        "seasons": [{"seasonName": "2015"}],
    }

    seasons = parse_seasons(payload, 289)

    assert [item.source_season_key for item in seasons] == [
        "2025",
        "2023",
        "2017/2019",
        "2017/2018",
    ]


def test_irregular_exact_seasons_are_not_reformatted_or_sorted():
    payload = {
        "details": {
            "id": 289,
            "name": "Africa Cup of Nations",
            "selectedSeason": "2025 Morocco",
            "latestSeason": "2025 Morocco",
        },
        "allAvailableSeasons": ["2025 Morocco", "2023", "2021/22", "2023"],
    }

    seasons = parse_seasons(payload, 289)

    assert [item.source_season_key for item in seasons] == [
        "2025 Morocco",
        "2023",
        "2021/22",
    ]
    assert seasons[0].is_selected and seasons[0].is_latest


def test_selected_season_validation_catches_fotmob_current_season_fallback():
    payload = {
        "details": {"id": 289, "name": "AFCON", "selectedSeason": "2025"},
        "allAvailableSeasons": ["2025", "2023"],
    }

    assert validate_selected_season(payload, "2025", competition_id=289) == "2025"
    with pytest.raises(SelectedSeasonMismatch, match="requested exact season"):
        validate_selected_season(payload, "2027", competition_id=289)


def test_competition_from_league_payload_keeps_source_metadata():
    item = competition_from_league_payload({
        "details": {
            "id": 289,
            "name": "Africa Cup of Nations",
            "selectedSeason": "2025",
            "country": "INT",
            "gender": "male",
            "seopath": "africa-cup-nations",
        }
    })
    assert item.competition_id == 289
    assert item.country_code == "INT"
    assert item.presentation_slug == "289-africa-cup-nations"
