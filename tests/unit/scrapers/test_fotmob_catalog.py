from __future__ import annotations

import pytest

from utils.fotmob_publication import FOTMOB_DAILY_COMPETITION_IDS

from scrapers.fotmob.catalog import (
    CatalogConflictError,
    DEFAULT_SCOPE_OVERRIDES,
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


@pytest.mark.parametrize(
    ("competition", "rule"),
    [
        # Диакритика снимается перед матчингом (NFKD).
        (CompetitionRef(7001, "Coupe de France Féminine"), "exclude_female"),
        (CompetitionRef(7002, "Première Ligue Féminine"), "exclude_female"),
        # Романские формы: португальская, итальянская, мексиканская.
        (CompetitionRef(7003, "Nacional Feminino"), "exclude_female"),
        (CompetitionRef(7004, "Serie A Femminile"), "exclude_female"),
        (CompetitionRef(7005, "Liga MX Femenil"), "exclude_female"),
        # "Womens" без апострофа ломал \b после women.
        (CompetitionRef(7006, "Womens Asian Cup"), "exclude_female"),
        # Скандинавские и нидерландская формы.
        (CompetitionRef(7007, "1. Division Kvinner"), "exclude_female"),
        (CompetitionRef(7008, "Kvindeliga"), "exclude_female"),
        (CompetitionRef(7009, "Eredivisie Vrouwen"), "exclude_female"),
        # Суффикс (W) — маркер, который использует сам источник.
        (CompetitionRef(7010, "Damallsvenskan (W)"), "exclude_female"),
        (CompetitionRef(7011, "World Cup Qualification (W) UEFA"), "exclude_female"),
        # Португальская молодёжная лига U23.
        (CompetitionRef(7012, "Liga Revelação"), "exclude_youth"),
    ],
)
def test_scope_classifier_catches_localised_women_and_youth_names(competition, rule):
    assert classify_competition(competition).policy_rule == rule


@pytest.mark.parametrize(
    "competition",
    [
        # Вторые дивизионы: токены ii/b — ловушка для широких шаблонов.
        CompetitionRef(9117, "NB II", country_name="Hungary"),
        CompetitionRef(9113, "Liga II", country_name="Romania"),
        CompetitionRef(264, "First Division B", country_name="Belgium"),
        CompetitionRef(146, "2. Bundesliga", country_name="Germany"),
        CompetitionRef(8968, "Primera Federación", country_name="Spain"),
        # "Sudamericana" содержит "damer", "Eliteserien" содержит "elite".
        CompetitionRef(299, "Copa Sudamericana", country_name="International"),
        CompetitionRef(59, "Eliteserien", country_name="Norway"),
        CompetitionRef(47, "Premier League", country_name="England"),
    ],
)
def test_scope_classifier_keeps_mens_competitions_that_look_suspicious(competition):
    result = classify_competition(competition)
    assert result.decision is ScopeDecision.INCLUDED, result.reason


def test_national_team_friendlies_are_in_scope_but_club_and_charity_are_not():
    """Решение владельца 07.08.2026: товарищеские сборных собираем, остальные — нет."""

    friendlies = CompetitionRef(114, "Friendlies", country_name="International")
    assert classify_competition(friendlies).decision is ScopeDecision.INCLUDED
    assert classify_competition(friendlies).policy_rule == "include_national_team_friendlies"

    for competition_id, name in (
        (489, "Club Friendlies"),
        (10312, "Sidemen Charity Match"),
        (10656, "Beta Squad vs Amp Charity"),
    ):
        competition = CompetitionRef(competition_id, name, country_name="International")
        assert classify_competition(competition).policy_rule == "exclude_friendly"

    # Женские и молодёжные товарищеские остаются за бортом по своим правилам.
    assert classify_competition(
        CompetitionRef(293, "Women's Friendlies", country_name="International")
    ).policy_rule == "exclude_female"
    assert classify_competition(
        CompetitionRef(344, "Friendlies U-21", country_name="International")
    ).policy_rule == "exclude_youth"


def test_scope_overrides_decide_by_id_not_by_name():
    """Пары турниров, различающиеся ТОЛЬКО id — имя и страна совпадают побайтово."""

    female_cup = CompetitionRef(11029, "Super Cup", country_name="Netherlands")
    male_cup = CompetitionRef(237, "Super Cup", country_name="Netherlands")
    assert classify_competition(female_cup).decision is ScopeDecision.EXCLUDED
    assert classify_competition(male_cup).decision is ScopeDecision.INCLUDED


def test_scope_overrides_are_unique_and_reviewable():
    ids = [item.competition_id for item in DEFAULT_SCOPE_OVERRIDES]
    assert len(ids) == len(set(ids)), "дубли id в DEFAULT_SCOPE_OVERRIDES"
    assert all(item.name.strip() and item.country.strip() for item in DEFAULT_SCOPE_OVERRIDES)


def test_women_pinned_into_the_sealed_daily_cohort_stay_documented():
    """Растяжка на отложенный долг #1139: 10557/10558 — женские, но опечатаны в когорте.

    Источник отдаёт по ним ``details.gender=female``. Исключить их нельзя, не пересчитав
    оба SHA когорты #930 в четырёх местах, иначе падает ``validate_data``. Если кто-то
    добавит их в overrides без ротации печати — сначала упадёт этот тест и объяснит,
    что нужно сделать, а не соседний фенс с непонятной ошибкой.
    """

    override_ids = {item.competition_id for item in DEFAULT_SCOPE_OVERRIDES}
    for competition_id in (10557, 10558):
        assert competition_id in FOTMOB_DAILY_COMPETITION_IDS
        assert competition_id not in override_ids, (
            f"турнир {competition_id} женский, но вшит в опечатанную дневную когорту #930: "
            "его исключение требует пересчёта FOTMOB_DAILY_SCOPE_SHA256 и "
            "FOTMOB_DAILY_COMPETITION_IDS_SHA256 в dags/utils/fotmob_publication.py, "
            "deploy/fotmob/deploy.py, scripts/fotmob_runtime.py, scripts/fotmob_acceptance.py "
            "и docs/operations/fotmob-production.md"
        )


def test_daily_cohort_stays_inside_the_included_scope():
    """Фенс: исключение турнира из когорты роняет validate_data и дневную волну."""

    for competition_id in FOTMOB_DAILY_COMPETITION_IDS:
        competition = CompetitionRef(competition_id, f"cohort-{competition_id}")
        result = classify_competition(competition)
        assert result.decision is ScopeDecision.INCLUDED, (
            f"турнир {competition_id} из дневной когорты больше не included "
            f"({result.policy_rule}) — нужна согласованная ротация когорты"
        )


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
