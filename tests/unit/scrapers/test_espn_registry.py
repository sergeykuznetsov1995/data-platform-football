"""Validation and manual-promotion tests for the ESPN source registry."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from scrapers.espn.discovery import CatalogCandidate, CatalogSnapshot
from scrapers.espn.models import (
    ADMITTED_AGE_CLASSES,
    AgeClass,
    CapabilityState,
    EntityCapabilities,
    Gender,
)
from scrapers.espn.registry import (
    DEFAULT_REGISTRY_PATH,
    RegistryError,
    build_discovered_male_registry,
    load_registry,
    promote_candidate,
    validate_registry_document,
)


FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "espn"
    / "catalog_2026-07-31.json"
)


def _document() -> dict:
    return {
        "schema_version": 1,
        "registry_version": "2026-07-31",
        "as_of": "2026-07-31",
        "competitions": [
            {
                "espn_id": 700,
                "slug": "eng.1",
                "name": "English Premier League",
                "enabled": True,
                "gender": "MALE",
                "age_class": "SENIOR",
                "gender_evidence": ["detail.gender=MALE"],
                "age_class_evidence": ["manual: senior"],
                "legacy": {
                    "league": "ENG-Premier League",
                    "league_aliases": ["ENG-Premier League"],
                    "season_aliases": {"2026": ["2627", "2026"]},
                },
                "editions": [
                    {
                        "source_season_year": 2026,
                        "display_name": "2026-27 English Premier League",
                        "start_date": "2026-06-01",
                        "end_date": "2027-06-01",
                        "current": True,
                        "capabilities": {
                            "schedule": "proven",
                            "lineup": "proven",
                            "matchsheet": "partial",
                        },
                    }
                ],
            }
        ],
    }


def _male_candidate() -> CatalogCandidate:
    return CatalogCandidate(
        espn_id=775,
        slug="uefa.champions",
        name="UEFA Champions League",
        group="Europe",
        source_order=2,
        gender=Gender.MALE,
        source_season_year=2026,
        edition_display_name="2026-27 UEFA Champions League",
        start_date="2026-07-01",
        end_date="2027-06-30",
        capabilities=EntityCapabilities(
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
        ),
        gender_evidence=("detail.gender=MALE",),
    )


@pytest.mark.unit
def test_generated_registry_contains_every_and_only_explicit_male_fixture() -> None:
    snapshot = CatalogSnapshot.from_dict(json.loads(FIXTURE.read_text()))
    generated = build_discovered_male_registry(
        snapshot,
        legacy_registry=load_registry(DEFAULT_REGISTRY_PATH),
    )
    expected = {
        (row.espn_id, row.slug, row.source_season_year)
        for row in snapshot.candidates
        if row.gender is Gender.MALE
    }

    assert len(expected) == 181
    assert {
        (row.espn_id, row.slug, row.current_edition.source_season_year)
        for row in generated.promoted
    } == expected
    assert all(row.gender is Gender.MALE for row in generated.promoted)


@pytest.mark.unit
def test_generated_registry_preserves_only_known_legacy_aliases() -> None:
    generated = build_discovered_male_registry(
        CatalogSnapshot.from_dict(json.loads(FIXTURE.read_text())),
        legacy_registry=load_registry(DEFAULT_REGISTRY_PATH),
    )

    assert sum(row.legacy is not None for row in generated.promoted) == 9
    assert generated.by_id[700].legacy.league == "ENG-Premier League"
    assert generated.by_id[775].legacy is None


@pytest.mark.unit
def test_generated_registry_removes_non_configured_prior_legacy_alias() -> None:
    previous_document = _document()
    previous_document["competitions"][0].update(
        espn_id=775,
        slug="uefa.champions",
        name="UEFA Champions League",
        legacy={
            "league": "NON-CONFIGURED",
            "league_aliases": ["NON-CONFIGURED"],
            "season_aliases": {"2026": ["2026"]},
        },
    )
    previous = validate_registry_document(previous_document)

    generated = build_discovered_male_registry(
        CatalogSnapshot("2026-08-02T00:00:00+00:00", (_male_candidate(),)),
        legacy_registry=load_registry(DEFAULT_REGISTRY_PATH),
        previous_registry=previous,
    )

    assert generated.by_id[775].legacy is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("espn_id", None),
        ("gender_evidence", ()),
        ("source_season_year", None),
        ("start_date", None),
        ("end_date", None),
    ],
)
def test_explicit_male_with_incomplete_identity_fails_closed(field, value) -> None:
    candidate = replace(_male_candidate(), **{field: value})

    with pytest.raises(RegistryError, match="explicit MALE candidate"):
        build_discovered_male_registry(
            CatalogSnapshot("2026-08-02T00:00:00+00:00", (candidate,)),
            legacy_registry=load_registry(DEFAULT_REGISTRY_PATH),
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "capabilities",
    [
        EntityCapabilities(
            CapabilityState.PARTIAL,
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
        ),
        EntityCapabilities(
            CapabilityState.ABSENT,
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
        ),
        EntityCapabilities(
            CapabilityState.QUARANTINED,
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
        ),
        EntityCapabilities(
            CapabilityState.UNKNOWN,
            CapabilityState.QUARANTINED,
            CapabilityState.UNKNOWN,
        ),
        EntityCapabilities(
            CapabilityState.UNKNOWN,
            CapabilityState.UNKNOWN,
            CapabilityState.QUARANTINED,
        ),
    ],
)
def test_generated_registry_rejects_unsafe_candidate_capabilities(capabilities) -> None:
    candidate = replace(_male_candidate(), capabilities=capabilities)

    with pytest.raises(RegistryError, match="explicit MALE candidate"):
        build_discovered_male_registry(
            CatalogSnapshot("2026-08-02T00:00:00+00:00", (candidate,)),
            legacy_registry=load_registry(DEFAULT_REGISTRY_PATH),
        )


@pytest.mark.unit
def test_generated_registry_retains_same_year_prior_contract() -> None:
    prior = validate_registry_document(_document())
    candidate = replace(
        _male_candidate(),
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        source_season_year=2026,
        edition_display_name="changed ESPN display name",
        start_date="2026-01-01",
        end_date="2026-12-31",
    )

    generated = build_discovered_male_registry(
        CatalogSnapshot("2026-08-02T00:00:00+00:00", (candidate,)),
        legacy_registry=prior,
        previous_registry=prior,
    )

    competition = generated.by_id[700]
    assert competition.age_class is AgeClass.SENIOR
    assert competition.legacy == prior.by_id[700].legacy
    assert competition.current_edition == prior.by_id[700].current_edition


@pytest.mark.unit
def test_generated_registry_rollover_preserves_prior_editions() -> None:
    prior = validate_registry_document(_document())
    candidate = replace(
        _male_candidate(),
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        source_season_year=2027,
        edition_display_name="2027-28 English Premier League",
        start_date="2027-06-01",
        end_date="2028-06-01",
    )

    generated = build_discovered_male_registry(
        CatalogSnapshot("2026-08-02T00:00:00+00:00", (candidate,)),
        legacy_registry=prior,
        previous_registry=prior,
    )

    competition = generated.by_id[700]
    assert [edition.source_season_year for edition in competition.editions] == [
        2026,
        2027,
    ]
    assert [edition.current for edition in competition.editions] == [False, True]
    assert competition.age_class is AgeClass.SENIOR
    assert competition.legacy == prior.by_id[700].legacy


@pytest.mark.unit
def test_seed_registry_preserves_all_nine_legacy_mappings() -> None:
    registry = load_registry(DEFAULT_REGISTRY_PATH)

    assert len(registry.competitions) == 9
    assert registry.by_slug["eng.1"].legacy.league == "ENG-Premier League"
    assert registry.by_slug["fifa.world"].legacy.league == "INT-World Cup"
    assert registry.by_slug["uefa.euro"].legacy.league == "INT-European Championship"
    assert registry.by_slug["caf.nations"].legacy.league == "INT-Africa Cup of Nations"
    assert registry.by_slug["conmebol.america"].legacy.league == "INT-Copa America"
    assert registry.by_slug["eng.1"].legacy.season_aliases[2026] == ("2627", "2026")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda d: d["competitions"].append(deepcopy(d["competitions"][0])),
            "duplicate espn_id",
        ),
        (
            lambda d: d["competitions"].append(
                {**deepcopy(d["competitions"][0]), "espn_id": 701}
            ),
            "duplicate slug",
        ),
        (lambda d: d["competitions"][0].update(gender="UNKNOWN"), "explicit MALE"),
        (
            lambda d: d["competitions"][0]["editions"][0]["capabilities"].update(
                lineup="sometimes"
            ),
            "unknown capability",
        ),
        (
            lambda d: d["competitions"][0]["editions"][0].update(
                start_date="2027-01-01", end_date="2026-01-01"
            ),
            "date window",
        ),
        (
            lambda d: d["competitions"][0]["editions"][0].update(current=False),
            "exactly one current edition",
        ),
        (
            lambda d: d["competitions"][0].update(gender_evidence=[]),
            "gender evidence",
        ),
        (
            lambda d: d["competitions"][0]["editions"][0]["capabilities"].update(
                schedule="partial"
            ),
            "schedule capability must be proven",
        ),
        (
            lambda d: d["competitions"][0]["editions"][0]["capabilities"].update(
                matchsheet="quarantined"
            ),
            "matchsheet capability must be explicit",
        ),
    ],
)
def test_registry_rejects_unsafe_enabled_entries(mutation, message: str) -> None:
    document = _document()
    mutation(document)

    with pytest.raises(RegistryError, match=message):
        validate_registry_document(document)


@pytest.mark.unit
def test_registry_allows_enabled_explicit_male_with_honest_unknowns() -> None:
    document = _document()
    row = document["competitions"][0]
    row.update(age_class="UNKNOWN", age_class_evidence=[])
    row["editions"][0]["capabilities"] = {
        "schedule": "unknown",
        "lineup": "unknown",
        "matchsheet": "unknown",
    }

    registry = validate_registry_document(document)

    assert registry.promoted[0].age_class is AgeClass.UNKNOWN


@pytest.mark.unit
def test_disabled_unknown_candidate_is_allowed_but_not_promoted() -> None:
    document = _document()
    row = document["competitions"][0]
    row.update(enabled=False, gender="UNKNOWN", age_class="UNKNOWN")
    row["gender_evidence"] = []
    row["age_class_evidence"] = []

    registry = validate_registry_document(document)

    assert registry.competitions[0].enabled is False


@pytest.mark.unit
@pytest.mark.parametrize("gender", [Gender.FEMALE, Gender.UNKNOWN])
def test_female_or_unknown_discovery_cannot_be_promoted(gender: Gender) -> None:
    candidate = CatalogCandidate(
        espn_id=8097,
        slug="eng.w.1",
        name="Women's Super League",
        group="Europe",
        source_order=1,
        gender=gender,
        age_class=AgeClass.UNKNOWN,
        source_season_year=2026,
        edition_display_name="2026-27 Women's Super League",
        start_date="2026-07-01",
        end_date="2027-07-01",
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PROVEN,
            CapabilityState.PROVEN,
        ),
        gender_evidence=(f"detail.gender={gender.value}",),
    )

    with pytest.raises(RegistryError, match="explicit MALE"):
        promote_candidate(
            _document(),
            candidate,
            age_class=AgeClass.SENIOR,
            age_class_evidence=("manual: senior",),
            legacy_league="ENG-Womens Super League",
        )


@pytest.mark.unit
def test_manual_promotion_returns_new_valid_document_without_mutation() -> None:
    document = _document()
    candidate = CatalogCandidate(
        espn_id=775,
        slug="uefa.champions",
        name="UEFA Champions League",
        group="Europe",
        source_order=2,
        gender=Gender.MALE,
        age_class=AgeClass.UNKNOWN,
        source_season_year=2026,
        edition_display_name="2026-27 UEFA Champions League",
        start_date="2026-07-01",
        end_date="2027-06-30",
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PARTIAL,
            CapabilityState.PARTIAL,
        ),
        gender_evidence=("detail.gender=MALE",),
    )

    promoted = promote_candidate(
        document,
        candidate,
        age_class=AgeClass.SENIOR,
        age_class_evidence=("manual review 2026-07-31",),
        legacy_league="UEFA-Champions League",
    )

    assert len(document["competitions"]) == 1
    assert len(promoted["competitions"]) == 2
    assert validate_registry_document(promoted).by_slug["uefa.champions"].enabled


@pytest.mark.unit
@pytest.mark.parametrize(
    "age_class", sorted(ADMITTED_AGE_CLASSES, key=lambda item: item.value)
)
def test_manual_promotion_admits_every_explicit_age_class(age_class: AgeClass) -> None:
    candidate = CatalogCandidate(
        espn_id=775,
        slug="uefa.champions",
        name="UEFA Champions League",
        group="Europe",
        source_order=2,
        gender=Gender.MALE,
        source_season_year=2026,
        edition_display_name="2026 UEFA Champions League",
        start_date="2026-01-01",
        end_date="2026-12-31",
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PARTIAL,
            CapabilityState.ABSENT,
        ),
        gender_evidence=("detail.gender=MALE",),
    )

    promoted = promote_candidate(
        {**_document(), "competitions": []},
        candidate,
        age_class=age_class,
        age_class_evidence=(f"manual: {age_class.value}",),
        legacy_league="UEFA-Champions League",
    )

    assert validate_registry_document(promoted).by_id[775].age_class is age_class


@pytest.mark.unit
def test_manual_promotion_reactivates_existing_disabled_candidate() -> None:
    document = _document()
    row = document["competitions"][0]
    row.update(enabled=False, gender="UNKNOWN", age_class="UNKNOWN")
    row["gender_evidence"] = []
    row["age_class_evidence"] = []
    candidate = CatalogCandidate(
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        group="Europe",
        source_order=1,
        gender=Gender.MALE,
        source_season_year=2026,
        edition_display_name="2026-27 English Premier League",
        start_date="2026-06-01",
        end_date="2027-06-01",
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PARTIAL,
            CapabilityState.ABSENT,
        ),
        gender_evidence=("reviewed detail.gender=MALE",),
    )

    promoted = promote_candidate(
        document,
        candidate,
        age_class=AgeClass.U23,
        age_class_evidence=("manual: U23",),
        legacy_league="ENG-Premier League",
    )
    competition = validate_registry_document(promoted).by_id[700]

    assert document["competitions"][0]["enabled"] is False
    assert competition.enabled is True
    assert competition.gender is Gender.MALE
    assert competition.age_class is AgeClass.U23
    assert competition.gender_evidence == ("reviewed detail.gender=MALE",)
    assert competition.age_class_evidence == ("manual: U23",)
    assert len(competition.editions) == 1


@pytest.mark.unit
def test_manual_rollover_promotion_replaces_current_edition_without_mutation() -> None:
    document = _document()
    candidate = CatalogCandidate(
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        group="Europe",
        source_order=1,
        gender=Gender.MALE,
        source_season_year=2027,
        edition_display_name="2027-28 English Premier League",
        start_date="2027-06-01",
        end_date="2028-06-01",
        capabilities=EntityCapabilities(
            CapabilityState.PROVEN,
            CapabilityState.PARTIAL,
            CapabilityState.ABSENT,
        ),
        gender_evidence=("detail.gender=MALE",),
    )

    promoted = promote_candidate(
        document,
        candidate,
        age_class=AgeClass.SENIOR,
        age_class_evidence=("manual: unchanged senior classification",),
        legacy_league="ENG-Premier League",
    )
    competition = validate_registry_document(promoted).by_id[700]

    assert document["competitions"][0]["editions"][0]["current"] is True
    assert [edition.source_season_year for edition in competition.editions] == [
        2026,
        2027,
    ]
    assert competition.current_edition.source_season_year == 2027
    assert competition.legacy.season_aliases[2027] == ("2728", "2027")


@pytest.mark.unit
def test_registry_rejects_unknown_keys() -> None:
    document = _document()
    document["competitions"][0]["editions"][0]["capabilties"] = {}

    with pytest.raises(RegistryError, match="unknown keys"):
        validate_registry_document(document)


@pytest.mark.unit
@pytest.mark.parametrize("schema_version", [True, False, 1.0, "1"])
def test_registry_schema_version_requires_exact_supported_integer(
    schema_version,
) -> None:
    document = _document()
    document["schema_version"] = schema_version

    with pytest.raises(RegistryError, match="schema_version"):
        validate_registry_document(document)
