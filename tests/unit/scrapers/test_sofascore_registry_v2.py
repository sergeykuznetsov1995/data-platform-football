"""Production eligibility and schema-v2 SofaScore registry contracts."""

from __future__ import annotations

import json
import importlib
from copy import deepcopy
from pathlib import Path

import pytest

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.discovery import merge_registry, parse_seasons_payload
from scrapers.sofascore.registry import (
    ActivationError,
    activation_eligibility,
    approve_tournament,
    classify_tournament_source,
    pending_review,
    set_activation,
)


CASES = json.loads(
    Path("tests/fixtures/sofascore_tournament_classification_cases.json")
    .read_text(encoding="utf-8")
)


def _record(case: dict, classification: dict) -> dict:
    return {
        "unique_tournament_id": case["id"],
        "name": case["name"],
        "slug": "test-league",
        "category": {"id": 1, "name": "Test", "slug": "test"},
        "sport_slug": case.get("sport", "football"),
        "page_path": f"{case.get('sport', 'football')}/test/test-league",
        "canonical_id": None,
        "enabled": False,
        "classification": classification,
        "review": pending_review(),
        "seasons": [{
            "season_id": 1,
            "name": "Test 25/26",
            "source_name": "Test 25/26",
            "year": "25/26",
            "format": "split_year",
            "season_format": "split_year",
            "canonical_season": "2526",
            "start_date": None,
            "end_date": None,
            "aliases": ["25/26", "2526"],
            "evidence": [{"type": "fixture"}],
        }],
    }


@pytest.mark.unit
@pytest.mark.parametrize("case", CASES["positive"], ids=lambda case: case["name"])
def test_adult_men_candidates_require_evidenced_operator_review(case):
    classification = classify_tournament_source(
        case,
        name=case["name"],
        sport_slug="football",
        endpoint="fixture",
    )
    record = _record(case, classification)

    assert classification["gender"] == "male"
    assert classification["age_group"] == "unknown"
    assert classification["team_level"] == "unknown"
    assert activation_eligibility(record).allowed is False

    approved = approve_tournament(
        record,
        canonical_id=f"TEST-{case['id']}",
        reviewed_by="unit-test",
        reviewed_at="2026-07-11",
        evidence=[{"type": "official_rules", "reference": "fixture"}],
    )
    active = set_activation(approved, enabled=True)
    assert active["enabled"] is True
    assert activation_eligibility(active).allowed is True


@pytest.mark.unit
@pytest.mark.parametrize("case", CASES["negative"], ids=lambda case: case["name"])
def test_non_adult_men_source_evidence_cannot_be_overridden(case):
    classification = classify_tournament_source(
        case,
        name=case["name"],
        sport_slug=case.get("sport", "football"),
        endpoint="fixture",
    )
    record = _record(case, classification)

    assert activation_eligibility(record).allowed is False
    with pytest.raises(ActivationError, match="source"):
        approve_tournament(
            record,
            canonical_id=f"TEST-{case['id']}",
            reviewed_by="unit-test",
            reviewed_at="2026-07-11",
            evidence=[{"type": "official_rules", "reference": "fixture"}],
        )


@pytest.mark.unit
def test_source_negative_evidence_overrides_a_stale_approved_review():
    source = CASES["negative"][0]
    classification = classify_tournament_source(
        source,
        name=source["name"],
        sport_slug="football",
        endpoint="fixture",
    )
    record = _record(source, classification)
    record["canonical_id"] = "TEST-WOMEN"
    record["enabled"] = True
    record["review"] = {
        "status": "approved",
        "confirmed": {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        },
        "reviewed_by": "stale",
        "reviewed_at": "2025-01-01",
        "evidence": [{"type": "legacy"}],
        "notes": None,
    }

    assert activation_eligibility(record).allowed is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("age_group", "youth", "source age_group is youth"),
        ("team_level", "reserve", "source team_level is reserve"),
    ],
)
def test_source_youth_or_reserve_cannot_be_hidden_by_empty_exclusions(
    field, value, reason
):
    """The normalized source field is a gate, not merely an explanation."""

    source = CASES["positive"][0]
    classification = classify_tournament_source(
        source,
        name=source["name"],
        sport_slug="football",
        endpoint="fixture",
    )
    classification[field] = value
    classification["status"] = "review_required"
    classification["exclusion_reasons"] = []
    record = _record(source, classification)
    record["canonical_id"] = "TEST-NEGATIVE"
    record["review"] = {
        "status": "approved",
        "confirmed": {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        },
        "reviewed_by": "stale",
        "reviewed_at": "2026-07-11",
        "evidence": [{"type": "official_rules", "reference": "fixture"}],
        "notes": None,
    }

    eligibility = activation_eligibility(record)
    assert eligibility.allowed is False
    assert reason in eligibility.reasons
    with pytest.raises(ActivationError, match=reason):
        set_activation(record, enabled=True)


@pytest.mark.unit
def test_season_v2_split_calendar_named_dates_and_explicit_euro_alias():
    payload = {"seasons": [
        {
            "id": 1,
            "name": "EURO 2020",
            "year": "2020",
            "startDate": "2021-06-11",
            "endTimestamp": 1626048000,
        },
        {"id": 2, "name": "EURO 24/25", "year": "24/25"},
        {"id": 3, "name": "Centenary Edition", "year": "Centenary"},
    ]}

    calendar, split_year, named = parse_seasons_payload(payload, 1)
    assert calendar["format"] == "calendar_year"
    assert calendar["season_format"] == "single_year"
    assert calendar["start_date"] == "2021-06-11"
    assert calendar["end_date"] == "2021-07-12"
    assert "2021" in calendar["aliases"]
    assert split_year["format"] == "split_year"
    assert split_year["canonical_season"] == "2425"
    assert named["format"] == "named"
    assert named["canonical_season"] is None


@pytest.mark.unit
def test_merge_preserves_all_operator_fields_and_is_idempotent():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    prior = next(
        item for item in existing["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    prior["operator_ticket"] = "OPS-123"
    prior["seasons"][0]["aliases"].append("2025/2026")
    discovered = deepcopy(prior)
    discovered["name"] = "Premier League refreshed"
    discovered["canonical_id"] = None
    discovered["enabled"] = False
    discovered["review"] = pending_review()
    discovered["seasons"][0]["aliases"] = ["25/26", "2526"]

    once, _ = merge_registry(existing, [discovered])
    twice, counts = merge_registry(once, [discovered])
    refreshed = next(
        item for item in once["tournaments"]
        if item["unique_tournament_id"] == 17
    )

    assert refreshed["canonical_id"] == "ENG-Premier League"
    assert refreshed["enabled"] is True
    assert refreshed["review"] == prior["review"]
    assert refreshed["operator_ticket"] == "OPS-123"
    assert "2025/2026" in refreshed["seasons"][0]["aliases"]
    assert twice == once
    assert counts["updated_tournaments"] == 0


@pytest.mark.unit
def test_merge_does_not_erase_prior_source_gender_when_refresh_omits_it():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    prior = next(
        item for item in existing["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    discovered = deepcopy(prior)
    discovered["classification"] = classify_tournament_source(
        {},
        name=discovered["name"],
        sport_slug="football",
        endpoint="/unique-tournament/17",
    )

    merged, _ = merge_registry(existing, [discovered])
    refreshed = next(
        item for item in merged["tournaments"]
        if item["unique_tournament_id"] == 17
    )

    assert refreshed["classification"]["gender"] == "male"
    assert refreshed["classification"]["status"] == "review_required"
    assert SofaScoreCatalog.from_mapping(merged).tournament(17).capture_allowed


@pytest.mark.unit
def test_merge_explicit_negative_source_evidence_overrides_prior_male():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    prior = next(
        item for item in existing["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    discovered = deepcopy(prior)
    discovered["name"] = "Women's Premier League"
    discovered["classification"] = classify_tournament_source(
        {"gender": "F"},
        name=discovered["name"],
        sport_slug="football",
        endpoint="/unique-tournament/17",
    )

    merged, _ = merge_registry(existing, [discovered])
    refreshed = next(
        item for item in merged["tournaments"]
        if item["unique_tournament_id"] == 17
    )

    assert refreshed["classification"]["gender"] == "female"
    assert refreshed["classification"]["status"] == "excluded"
    assert not SofaScoreCatalog.from_mapping(merged).tournament(17).capture_allowed


@pytest.mark.unit
@pytest.mark.parametrize(
    ("old_name", "source_fields", "expected_field", "expected_value"),
    [
        ("Premier League U21", {"gender": "male", "isYouth": True},
         "age_group", "youth"),
        ("Premier League Reserves", {"gender": "male", "isReserve": True},
         "team_level", "reserve"),
        ("Premier League Futsal", {"gender": "male"}, "sport", "football"),
    ],
)
def test_merge_rename_keeps_source_exclusions_and_operator_fields(
    old_name, source_fields, expected_field, expected_value
):
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    prior = next(
        item for item in existing["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    prior["name"] = old_name
    prior["classification"] = classify_tournament_source(
        source_fields,
        name=old_name,
        sport_slug="football",
        endpoint="/unique-tournament/17/old",
    )
    prior["operator_ticket"] = "OPS-NEGATIVE-EVIDENCE"
    prior_review = deepcopy(prior["review"])
    prior_enabled = prior["enabled"]

    discovered = deepcopy(prior)
    discovered["name"] = "Renamed Premier League"
    discovered["canonical_id"] = None
    discovered["enabled"] = False
    discovered["review"] = pending_review()
    discovered.pop("operator_ticket")
    discovered["classification"] = classify_tournament_source(
        {"gender": "male"},
        name=discovered["name"],
        sport_slug="football",
        endpoint="/unique-tournament/17/new",
    )

    merged, _ = merge_registry(existing, [discovered])
    refreshed = next(
        item for item in merged["tournaments"]
        if item["unique_tournament_id"] == 17
    )

    assert refreshed["canonical_id"] == "ENG-Premier League"
    assert refreshed["enabled"] is prior_enabled
    assert refreshed["review"] == prior_review
    assert refreshed["operator_ticket"] == "OPS-NEGATIVE-EVIDENCE"
    assert refreshed["classification"][expected_field] == expected_value
    assert refreshed["classification"]["exclusion_reasons"]
    assert refreshed["classification"]["status"] == "excluded"
    assert not SofaScoreCatalog.from_mapping(merged).tournament(17).capture_allowed


@pytest.mark.unit
def test_v1_registry_remains_readable_but_is_not_capture_eligible():
    document = {
        "schema_version": 1,
        "tournaments": [{
            "unique_tournament_id": 17,
            "name": "Premier League",
            "slug": "premier-league",
            "category": {"id": 1, "name": "England", "slug": "england"},
            "sport_slug": "football",
            "page_path": "football/england/premier-league",
            "canonical_id": "ENG-Premier League",
            "enabled": True,
            "seasons": [{
                "season_id": 1,
                "name": "Premier League 25/26",
                "year": "25/26",
                "season_format": "split_year",
                "canonical_season": "2526",
            }],
        }],
    }

    catalog = SofaScoreCatalog.from_mapping(document)
    assert catalog.resolve_season_id(17, "2526") == 1
    assert catalog.tournament(17).capture_allowed is False
    assert catalog.enabled_competition_ids() == ()


@pytest.mark.unit
def test_named_season_can_use_an_explicit_operator_canonical_override():
    document = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    epl = next(
        item for item in document["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    epl["seasons"] = [{
        "season_id": 999,
        "name": "Centenary Edition",
        "source_name": "Centenary Edition",
        "year": "Centenary",
        "format": "named",
        "season_format": "unknown",
        "canonical_season": None,
        "canonical_season_override": "CENTENARY",
        "start_date": None,
        "end_date": None,
        "aliases": ["Centenary", "CENTENARY"],
        "evidence": [{"type": "fixture"}],
    }]
    document["tournaments"] = [epl]

    catalog = SofaScoreCatalog.from_mapping(document)
    season = catalog.resolve_source_season(17, "CENTENARY")
    assert season is not None
    assert season.source_canonical_season is None
    assert season.canonical_season == "CENTENARY"
    assert season.activatable is True
    assert catalog.enabled_competition_ids() == ("ENG-Premier League",)


@pytest.mark.unit
def test_operator_cli_activation_is_atomic_and_fail_closed(tmp_path):
    module = importlib.import_module("dags.scripts.manage_sofascore_registry")
    path = tmp_path / "tournaments.json"
    path.write_bytes(
        Path("configs/sofascore/tournaments.json").read_bytes()
    )

    assert module.main(["--registry", str(path), "17", "disable"]) == 0
    disabled = json.loads(path.read_text(encoding="utf-8"))
    epl = next(
        item for item in disabled["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    assert epl["enabled"] is False

    assert module.main(["--registry", str(path), "17", "enable"]) == 0
    before_rejected_activation = path.read_bytes()
    with pytest.raises(ActivationError, match="operator review"):
        module.main(["--registry", str(path), "8", "enable"])
    assert path.read_bytes() == before_rejected_activation


@pytest.mark.unit
def test_scheduled_discovery_poison_proxy_and_bot_pr_contract():
    workflow = Path(".github/workflows/sofascore-discovery.yml").read_text(
        encoding="utf-8"
    )
    assert 'cron: "17 4 * * 1-6"' in workflow
    assert 'cron: "17 4 * * 0"' in workflow
    assert "HTTP_PROXY: http://127.0.0.1:9" in workflow
    assert "--scope \"$scope\"" in workflow
    assert 'traffic["paid_proxy_bytes"] == 0' in workflow
    assert 'traffic["browser_sessions"] == 0' in workflow
    assert 'traffic["browser_navigations"] == 0' in workflow
    assert "gh pr create" in workflow
