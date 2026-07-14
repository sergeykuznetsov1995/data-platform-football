"""Production eligibility and schema-v2 SofaScore registry contracts."""

from __future__ import annotations

import json
import importlib
from copy import deepcopy
from pathlib import Path

import pytest

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.discovery import (
    DiscoveryConcurrentUpdate,
    merge_registry,
    parse_seasons_payload,
)
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


def _cli():
    return importlib.import_module("dags.scripts.manage_sofascore_registry")


# The shipped registry is a moving target: every onboarding wave flips rows from
# "no source evidence yet" to "approved and enabled" (wave 1 activated
# 8/16/17/23/34/35).  The CLI contracts below never assert *which* leagues are
# live -- they need an eligible row, a row whose source evidence has not landed
# yet, and rows with broken canonical links.  Borrowing whichever rows the
# production registry happened to ship is what silently disarmed the negative
# arm of these gates when La Liga (8) was onboarded, so pin the archetypes here.
#
# The archetypes are built through the production helpers, and the eligible one
# is re-checked by ``set_activation``, so a fixture that stops exercising the
# gate it guards fails loudly here instead of passing vacuously below.
def _undiscovered(row: dict) -> dict:
    """Pin the pre-discovery archetype: the source detail was never fetched."""

    row = deepcopy(row)
    row["enabled"] = False
    row["review"] = pending_review()
    row["seasons"] = []
    row["classification"] = classify_tournament_source(
        {},
        name=row["name"],
        sport_slug="football",
        endpoint=f"/unique-tournament/{row['unique_tournament_id']}",
    )
    assert row["classification"]["gender"] == "unknown"
    assert activation_eligibility(row).allowed is False
    return row


def _approved(row: dict) -> dict:
    """Pin the shipped archetype: source-confirmed male football + sign-off."""

    row = deepcopy(row)
    row["classification"] = classify_tournament_source(
        {"gender": "M"},
        name=row["name"],
        sport_slug="football",
        endpoint=f"/unique-tournament/{row['unique_tournament_id']}",
    )
    approved = approve_tournament(
        row,
        canonical_id=row["canonical_id"],
        reviewed_by="fixture@example.com",
        reviewed_at="2026-07-01T00:00:00+00:00",
        evidence=[
            {"type": "operator_reference", "reference": "https://example.org/rules"}
        ],
    )
    return set_activation(approved, enabled=True)


def _registry_fixture(tmp_path) -> Path:
    """A registry pinned to the archetypes the CLI contracts actually need."""

    document = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    rows = {item["unique_tournament_id"]: item for item in document["tournaments"]}
    # 16/17: eligible (approved, enabled, canonical seasons available).
    rows[16] = _approved(rows[16])
    rows[17] = _approved(rows[17])
    # 8: an adult-men candidate whose source gender evidence has not landed.
    rows[8] = _undiscovered(rows[8])
    # 7: a registry row that carries no canonical_id at all.
    rows[7] = _undiscovered(rows[7])
    rows[7]["canonical_id"] = None
    # 270: a canonical_id that exists in competitions.yaml, but whose only
    # canonical source season is not one of the seasons declared there.
    rows[270] = _undiscovered(rows[270])
    rows[270]["canonical_id"] = "INT-Africa Cup of Nations"
    rows[270]["seasons"] = [{
        "season_id": 1957,
        "name": "Africa Cup of Nations 1957",
        "source_name": "Africa Cup of Nations 1957",
        "year": "1957",
        "format": "calendar_year",
        "season_format": "single_year",
        "canonical_season": "1957",
        "start_date": None,
        "end_date": None,
        "aliases": ["1957"],
        "evidence": [{"type": "fixture"}],
    }]
    document["tournaments"] = list(rows.values())

    path = tmp_path / "tournaments.json"
    path.write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return path


def _tournament(path: Path, tournament_id: int) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    return next(
        item for item in document["tournaments"]
        if item["unique_tournament_id"] == tournament_id
    )


def _batch_file(tmp_path, payload: dict) -> Path:
    path = tmp_path / "review.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.mark.unit
def test_operator_cli_activation_is_atomic_and_fail_closed(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)

    assert module.main(
        ["--registry", str(path), "disable", "--tournament-ids", "17"]
    ) == 0
    assert _tournament(path, 17)["enabled"] is False

    assert module.main(
        ["--registry", str(path), "enable", "--tournament-ids", "17"]
    ) == 0
    before_rejected_activation = path.read_bytes()
    with pytest.raises(ActivationError, match="operator review"):
        module.main(["--registry", str(path), "enable", "--tournament-ids", "8"])
    assert path.read_bytes() == before_rejected_activation


@pytest.mark.unit
def test_batch_enable_is_all_or_nothing_in_one_write(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)

    # 16 is eligible, 8 is not: the eligible row must not be written either.
    before = path.read_bytes()
    with pytest.raises(ActivationError, match="operator review"):
        module.main(
            ["--registry", str(path), "enable", "--tournament-ids", "16,8"]
        )
    assert path.read_bytes() == before

    assert module.main(
        ["--registry", str(path), "enable", "--tournament-ids", "16,17"]
    ) == 0
    assert _tournament(path, 16)["enabled"] is True
    assert _tournament(path, 17)["enabled"] is True


@pytest.mark.unit
def test_approve_batch_applies_the_whole_wave_in_one_write(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    batch = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "reviewed_at": "2026-07-14T00:00:00+00:00",
        "approvals": [
            {
                "tournament_id": 16,
                "canonical_id": "INT-World Cup",
                "evidence": [{
                    "type": "repository",
                    "reference": "configs/medallion/competitions.yaml#INT-World Cup",
                    "note": "adult men's first-team FIFA competition",
                }],
                "notes": "wave 1",
            },
            {
                "tournament_id": 17,
                "canonical_id": "ENG-Premier League",
                "evidence": ["https://www.premierleague.com/"],
            },
        ],
    })

    assert module.main(
        ["--registry", str(path), "approve-batch", "--input", str(batch)]
    ) == 0

    for tournament_id in (16, 17):
        row = _tournament(path, tournament_id)
        assert row["review"]["status"] == "approved"
        assert row["review"]["reviewed_by"] == "operator@example.com"
        assert row["review"]["confirmed"] == {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        }
        # Approval never activates capture on its own.
        assert row["enabled"] is False
    assert _tournament(path, 17)["review"]["evidence"] == [
        {"type": "operator_reference", "reference": "https://www.premierleague.com/"}
    ]


@pytest.mark.unit
def test_approve_batch_rejects_an_unreplaced_evidence_todo(tmp_path):
    # #946 phase-4 review: the evidence gate must fail closed against the
    # prepare-review placeholder. Filling in only reviewed_by and leaving the
    # `"todo": true` stub must NOT activate a competition.
    module = _cli()
    path = _registry_fixture(tmp_path)
    before = path.read_bytes()
    with_todo = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "reviewed_at": "2026-07-14T00:00:00+00:00",
        "approvals": [{
            "tournament_id": 17,
            "canonical_id": "ENG-Premier League",
            "evidence": [{
                "type": "repository",
                "reference": "configs/medallion/competitions.yaml#ENG-Premier League",
                "note": module.REVIEW_TODO,
                "todo": True,
            }],
        }],
    })
    with pytest.raises(
        ActivationError,
        match="evidence TODO must be replaced with an out-of-source reference",
    ):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(with_todo)]
        )
    # The unreplaced stub aborts before the compare-and-swap write.
    assert path.read_bytes() == before

    # Replacing the stub with a real out-of-source reference approves the wave.
    replaced = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "reviewed_at": "2026-07-14T00:00:00+00:00",
        "approvals": [{
            "tournament_id": 17,
            "canonical_id": "ENG-Premier League",
            "evidence": ["https://www.premierleague.com/"],
        }],
    })
    assert module.main(
        ["--registry", str(path), "approve-batch", "--input", str(replaced)]
    ) == 0
    assert _tournament(path, 17)["review"]["status"] == "approved"
    assert _tournament(path, 17)["review"]["evidence"] == [
        {"type": "operator_reference", "reference": "https://www.premierleague.com/"}
    ]


@pytest.mark.unit
def test_approve_batch_aborts_before_writing_when_one_row_is_ineligible(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    before = path.read_bytes()
    batch = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "approvals": [
            {
                "tournament_id": 17,
                "canonical_id": "ENG-Premier League",
                "evidence": ["https://www.premierleague.com/"],
            },
            {
                # Source classification still says gender=unknown: the machine
                # layer has not run, so no operator may approve this row.
                "tournament_id": 8,
                "canonical_id": "ESP-La Liga",
                "evidence": ["https://www.laliga.com/"],
            },
        ],
    })

    with pytest.raises(ActivationError, match="source gender is not confirmed male"):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(batch)]
        )
    assert path.read_bytes() == before


@pytest.mark.unit
def test_approve_batch_cannot_precede_the_source_gender_evidence(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    batch = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "approvals": [{
            "tournament_id": 8,
            "canonical_id": "ESP-La Liga",
            "evidence": ["https://www.laliga.com/"],
        }],
    })
    with pytest.raises(ActivationError, match="source gender is not confirmed male"):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(batch)]
        )

    # Once discovery has written the source evidence, the same batch applies.
    document = json.loads(path.read_text(encoding="utf-8"))
    laliga = next(
        item for item in document["tournaments"]
        if item["unique_tournament_id"] == 8
    )
    laliga["classification"].update({
        "gender": "male",
        "status": "review_required",
        "evidence": [{
            "type": "source_field",
            "endpoint": "/unique-tournament/8",
            "field": "gender",
            "value": "M",
        }],
    })
    path.write_text(json.dumps(document), encoding="utf-8")

    assert module.main(
        ["--registry", str(path), "approve-batch", "--input", str(batch)]
    ) == 0
    assert _tournament(path, 8)["review"]["status"] == "approved"
    assert _tournament(path, 8)["enabled"] is False


@pytest.mark.unit
def test_approve_batch_rejects_a_concurrent_registry_update(tmp_path, monkeypatch):
    module = _cli()
    path = _registry_fixture(tmp_path)
    batch = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "approvals": [{
            "tournament_id": 17,
            "canonical_id": "ENG-Premier League",
            "evidence": ["https://www.premierleague.com/"],
        }],
    })

    original_read = module._read

    def _racing_read(target: Path):
        document = original_read(target)
        concurrent = deepcopy(dict(document))
        other = next(
            item for item in concurrent["tournaments"]
            if item["unique_tournament_id"] == 16
        )
        other["enabled"] = False
        target.write_text(json.dumps(concurrent), encoding="utf-8")
        return document

    monkeypatch.setattr(module, "_read", _racing_read)
    with pytest.raises(DiscoveryConcurrentUpdate, match="registry changed"):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(batch)]
        )
    assert _tournament(path, 17)["review"]["reviewed_by"] != "operator@example.com"


@pytest.mark.unit
def test_approve_batch_refuses_duplicates_and_an_unfilled_reviewer(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    before = path.read_bytes()

    unsigned = _batch_file(tmp_path, {
        "reviewed_by": None,
        "approvals": [{
            "tournament_id": 17,
            "canonical_id": "ENG-Premier League",
            "evidence": ["https://www.premierleague.com/"],
        }],
    })
    with pytest.raises(ActivationError, match="reviewed_by is required"):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(unsigned)]
        )

    duplicated = tmp_path / "duplicated.json"
    duplicated.write_text(json.dumps({
        "reviewed_by": "operator@example.com",
        "approvals": [
            {
                "tournament_id": 17,
                "canonical_id": "ENG-Premier League",
                "evidence": ["https://www.premierleague.com/"],
            },
            {
                "tournament_id": 17,
                "canonical_id": "ENG-Premier League",
                "evidence": ["https://www.premierleague.com/"],
            },
        ],
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate tournament id"):
        module.main(
            ["--registry", str(path), "approve-batch", "--input", str(duplicated)]
        )
    assert path.read_bytes() == before


@pytest.mark.unit
def test_reject_batch_disables_and_records_evidence(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    batch = _batch_file(tmp_path, {
        "reviewed_by": "operator@example.com",
        "rejections": [{
            "tournament_id": 17,
            "evidence": ["https://example.org/out-of-scope"],
            "notes": "out of scope",
        }],
    })

    assert module.main(
        ["--registry", str(path), "reject-batch", "--input", str(batch)]
    ) == 0
    epl = _tournament(path, 17)
    assert epl["review"]["status"] == "rejected"
    assert epl["enabled"] is False
    assert epl["review"]["notes"] == "out of scope"


@pytest.mark.unit
def test_prepare_review_snapshots_source_state_and_blocks_broken_links(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    before = path.read_bytes()
    # 7 has no canonical_id, 270's only canonical season is not in
    # competitions.yaml, 8 is a candidate awaiting targeted discovery.
    output = tmp_path / "review.json"
    assert module.main([
        "--registry", str(path),
        "prepare-review",
        "--tournament-ids", "8,7",
        "--tournament-ids", "270",
        "--output", str(output),
    ]) == 0

    draft = json.loads(output.read_text(encoding="utf-8"))
    assert draft["reviewed_by"] is None
    assert [row["tournament_id"] for row in draft["approvals"]] == [8, 7, 270]
    rows = {row["tournament_id"]: row for row in draft["approvals"]}

    laliga = rows[8]
    assert laliga["canonical_id"] == "ESP-La Liga"
    assert laliga["source_snapshot"]["gender"] == "unknown"
    assert laliga["source_snapshot"]["status"] == "unknown"
    assert laliga["source_snapshot"]["canonical_seasons"] == []
    assert laliga["evidence"] == [{
        "type": "repository",
        "reference": "configs/medallion/competitions.yaml#ESP-La Liga",
        "note": module.REVIEW_TODO,
        "todo": True,
    }]
    assert any("canonical source season" in reason for reason in laliga["blocked"])

    assert rows[7]["evidence"] == []
    assert rows[7]["blocked"] == ["canonical_id is missing from the registry row"]
    assert any(
        "overlaps" in reason for reason in rows[270]["blocked"]
    )
    # The registry itself is read-only for this command.
    assert path.read_bytes() == before


@pytest.mark.unit
def test_prepare_review_blocks_a_canonical_id_outside_competitions(tmp_path):
    module = _cli()
    path = _registry_fixture(tmp_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    laliga = next(
        item for item in document["tournaments"]
        if item["unique_tournament_id"] == 8
    )
    laliga["canonical_id"] = "ESP-Segunda Division"
    path.write_text(json.dumps(document), encoding="utf-8")

    output = tmp_path / "review.json"
    assert module.main([
        "--registry", str(path),
        "prepare-review",
        "--tournament-ids", "8",
        "--output", str(output),
    ]) == 0

    row = json.loads(output.read_text(encoding="utf-8"))["approvals"][0]
    assert row["evidence"] == []
    assert row["blocked"] == [
        "canonical_id 'ESP-Segunda Division' is not in "
        "configs/medallion/competitions.yaml"
    ]


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
