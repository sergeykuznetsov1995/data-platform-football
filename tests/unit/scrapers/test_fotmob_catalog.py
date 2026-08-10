from __future__ import annotations

import pytest

from scrapers.fotmob.catalog import (
    CLASSIFIER_VERSION,
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
        (
            CompetitionRef(
                47,
                "Premier League",
                gender="male",
                competition_type="league",
                age_group="adult",
            ),
            ScopeDecision.INCLUDED,
            "include_structural_male_adult",
        ),
        (CompetitionRef(5, "League", gender="mixed"), ScopeDecision.REVIEW_REQUIRED, "review_unknown_gender"),
        (CompetitionRef(6, "League", age_group="academy"), ScopeDecision.EXCLUDED, "exclude_youth"),
    ],
)
def test_male_senior_scope_classifier_is_auditable(competition, expected, rule):
    result = classify_competition(competition, competition)
    assert result.decision is expected
    assert result.policy_rule == rule


def test_scope_classifier_hook_can_override_ambiguous_source_metadata():
    catalog = CompetitionRef(999, "Ambiguous Invitational")
    profile = CompetitionRef(
        999,
        "Ambiguous Invitational",
        gender="male",
        competition_type="unknown",
    )

    def include_known(item):
        return ScopeClassification(item, ScopeDecision.INCLUDED, "curated official", "curated")

    assert classify_competition(catalog, profile, hooks=(include_known,)).policy_rule == "curated"


def test_catalog_without_profile_is_pending_probe():
    catalog = CompetitionRef(70001, "Senior Cup")
    result = classify_competition(catalog)
    assert result.decision is ScopeDecision.PENDING_PROBE
    assert result.policy_rule == "probe_required"


@pytest.mark.parametrize("name", ["Friendlies", "Club Friendlies"])
def test_structurally_male_adult_friendlies_are_included(name):
    catalog = CompetitionRef(70002, name)
    profile = CompetitionRef(
        70002, name, gender="male", competition_type="league", age_group="adult"
    )
    result = classify_competition(catalog, profile)
    assert result.decision is ScopeDecision.INCLUDED
    assert result.policy_rule == "include_structural_male_adult"


@pytest.mark.parametrize(
    "name",
    [
        "Charity Match",
        "Legends Show",
        "Player Testimonial",
        "Player Testimonials",
        "Preseason Exhibition",
        "Preseason Exhibitions",
    ],
)
def test_show_matches_are_excluded_even_when_profile_is_male(name):
    profile = CompetitionRef(70003, name, gender="male", competition_type="league")
    result = classify_competition(profile, profile)
    assert result.decision is ScopeDecision.EXCLUDED
    assert result.policy_rule == "exclude_show"


def test_structural_female_field_beats_an_identical_male_facing_name():
    catalog = CompetitionRef(70004, "Men's Premier League")
    profile = CompetitionRef(
        70004,
        "Men's Premier League",
        gender="female",
        competition_type="league",
        age_group="adult",
    )
    assert classify_competition(catalog, profile).policy_rule == "exclude_female"


@pytest.mark.parametrize(
    ("name", "rule"),
    [
        ("UEFA U16 Championship", "exclude_youth"),
        ("International U24 Tournament", "exclude_youth"),
        ("Under 25 Championship", "exclude_youth"),
        ("UEFA Ｕ－21 Championship", "exclude_youth"),
        ("UEFA U‑21 Championship", "exclude_youth"),
        ("National Development Cup", "exclude_reserve"),
        ("Premier League Ⅱ Teams", "exclude_reserve"),
        ("Premier Réserves", "exclude_reserve"),
    ],
)
def test_youth_and_reserve_signals_are_unicode_normalized(name, rule):
    profile = CompetitionRef(70005, name, gender="male", competition_type="league")
    assert classify_competition(profile, profile).policy_rule == rule


@pytest.mark.parametrize(
    ("profile", "rule"),
    [
        (
            CompetitionRef(70006, "League", competition_type="league"),
            "review_unknown_gender",
        ),
        (
            CompetitionRef(70006, "League", gender="male"),
            "review_unknown_type",
        ),
        (
            CompetitionRef(
                70006, "League", gender="male", competition_type="round-robin-ish"
            ),
            "review_unknown_type",
        ),
    ],
)
def test_unknown_gender_or_type_requires_review(profile, rule):
    result = classify_competition(profile, profile)
    assert result.decision is ScopeDecision.REVIEW_REQUIRED
    assert result.policy_rule == rule


def test_conflicting_structural_metadata_requires_review():
    catalog = CompetitionRef(
        70011, "Senior Cup", gender="male", competition_type="cup"
    )
    profile = CompetitionRef(
        70011, "Senior Cup", gender="male", competition_type="league"
    )
    result = classify_competition(catalog, profile)
    assert result.decision is ScopeDecision.REVIEW_REQUIRED
    assert result.policy_rule == "review_structural_conflict"


@pytest.mark.parametrize(
    ("catalog_gender", "profile_gender"),
    [("male", "female"), ("female", "male")],
)
def test_conflicting_structural_gender_requires_review(
    catalog_gender, profile_gender
):
    catalog = CompetitionRef(
        70012, "Senior Cup", gender=catalog_gender, competition_type="league"
    )
    profile = CompetitionRef(
        70012, "Senior Cup", gender=profile_gender, competition_type="league"
    )
    result = classify_competition(catalog, profile)
    assert result.decision is ScopeDecision.REVIEW_REQUIRED
    assert result.policy_rule == "review_structural_conflict"


def test_profile_identity_mismatch_raises():
    catalog = CompetitionRef(70007, "League")
    profile = CompetitionRef(70008, "League", gender="male", competition_type="league")
    with pytest.raises(ValueError, match="profile.*another competition"):
        classify_competition(catalog, profile)


def test_hook_precedence_cannot_change_competition_identity():
    catalog = CompetitionRef(70009, "Legends Show")
    profile = CompetitionRef(70009, "Legends Show", gender="male", competition_type="league")

    def include_show(item):
        return ScopeClassification(item, ScopeDecision.INCLUDED, "approved", "hook")

    assert classify_competition(catalog, profile, hooks=(include_show,)).policy_rule == "hook"

    def change_identity(_item):
        other = CompetitionRef(70010, "Other", gender="male", competition_type="league")
        return ScopeClassification(other, ScopeDecision.INCLUDED, "wrong", "hook")

    with pytest.raises(ValueError, match="hook.*another competition"):
        classify_competition(catalog, profile, hooks=(change_identity,))


@pytest.mark.parametrize("competition_id", [114, 489])
def test_historical_male_ids_are_included_from_structural_fixtures(competition_id):
    catalog = CompetitionRef(competition_id, "Senior Competition")
    profile = CompetitionRef(
        competition_id,
        "Senior Competition",
        gender="male",
        competition_type="league",
        age_group="adult",
    )
    assert classify_competition(catalog, profile).decision is ScopeDecision.INCLUDED


@pytest.mark.parametrize("competition_id", [10557, 10558])
def test_historical_female_ids_are_excluded_from_structural_fixtures(competition_id):
    catalog = CompetitionRef(competition_id, "Premier League")
    profile = CompetitionRef(
        competition_id,
        "Premier League",
        gender="female",
        competition_type="league",
        age_group="adult",
    )
    assert classify_competition(catalog, profile).decision is ScopeDecision.EXCLUDED


def test_classifier_version_is_bound_to_policy():
    assert CLASSIFIER_VERSION == "fotmob-men-v1"


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
            "ageGroup": "adult",
            "seopath": "africa-cup-nations",
        }
    })
    assert item.competition_id == 289
    assert item.country_code == "INT"
    assert item.age_group == "adult"
    assert item.presentation_slug == "289-africa-cup-nations"
