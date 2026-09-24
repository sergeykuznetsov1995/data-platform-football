"""Contracts for the repository-owned ESPN ingestion path."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from scrapers.espn.models import (
    ADMITTED_AGE_CLASSES,
    AgeClass,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
    IngestPlan,
    ManifestState,
    RequestDisposition,
    ScopeManifest,
    ScopePlan,
)


def _edition() -> Edition:
    return Edition(
        source_season_year=2026,
        display_name="2026-27 English Premier League",
        start_date=date(2026, 6, 1),
        end_date=date(2027, 6, 1),
        current=True,
        capabilities=EntityCapabilities(
            schedule=CapabilityState.PROVEN,
            lineup=CapabilityState.PROVEN,
            matchsheet=CapabilityState.PARTIAL,
        ),
    )


@pytest.mark.unit
def test_scope_identity_is_native_numeric_id_and_source_year() -> None:
    edition = _edition()
    competition = Competition(
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        enabled=True,
        editions=(edition,),
        gender_evidence=("detail.gender=MALE",),
        age_class_evidence=("manual: senior",),
    )

    assert competition.scope_id(edition) == "700:2026"


@pytest.mark.unit
def test_canonical_json_and_signatures_are_order_independent() -> None:
    scope = ScopePlan(
        scope_id="700:2026",
        espn_id=700,
        slug="eng.1",
        source_season_year=2026,
        start_date=date(2026, 6, 1),
        end_date=date(2027, 6, 1),
        capabilities=_edition().capabilities,
    )
    first = IngestPlan(
        schema_version=1,
        run_id="run-1",
        as_of=date(2026, 7, 31),
        registry_signature="a" * 64,
        scopes=(scope,),
        metadata={"z": 1, "a": {"b": 2, "a": 1}},
    )
    second = IngestPlan(
        schema_version=1,
        run_id="run-1",
        as_of=date(2026, 7, 31),
        registry_signature="a" * 64,
        scopes=(scope,),
        metadata={"a": {"a": 1, "b": 2}, "z": 1},
    )

    assert first.canonical_json() == second.canonical_json()
    assert first.signature() == second.signature()
    assert len(first.signature()) == 64


@pytest.mark.unit
def test_disposition_and_manifest_are_typed_and_canonical() -> None:
    disposition = RequestDisposition(
        endpoint="summary",
        state="captured",
        detail="raw persisted",
        event_id=401234567,
    )
    manifest = ScopeManifest(
        schema_version=1,
        run_id="run-1",
        scope_id="700:2026",
        registry_signature="a" * 64,
        plan_signature="b" * 64,
        state=ManifestState.COMPLETE,
        generated_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
        dispositions=(disposition,),
        row_counts={"schedule": 380, "lineup": 8000},
    )

    assert '"state":"complete"' in manifest.canonical_json()
    assert '"generated_at":"2026-07-31T12:00:00Z"' in manifest.canonical_json()
    assert manifest.signature() == manifest.signature()


@pytest.mark.unit
def test_age_classes_enumerate_every_admitted_source_classification() -> None:
    assert ADMITTED_AGE_CLASSES == frozenset(
        {
            AgeClass.SENIOR,
            AgeClass.U17,
            AgeClass.U19,
            AgeClass.U20,
            AgeClass.U21,
            AgeClass.U23,
            AgeClass.COLLEGE,
        }
    )
    assert AgeClass.YOUTH not in ADMITTED_AGE_CLASSES
    assert AgeClass.UNKNOWN not in ADMITTED_AGE_CLASSES


@pytest.mark.unit
def test_frozen_contracts_copy_and_freeze_nested_mappings() -> None:
    from scrapers.espn.models import LegacyAliases

    aliases = {2026: ["2627", "2026"]}
    legacy = LegacyAliases(
        league="ENG-Premier League",
        league_aliases=["ENG-Premier League"],
        season_aliases=aliases,
    )
    metadata = {"nested": {"items": [1, 2]}}
    scope = ScopePlan(
        scope_id="700:2026",
        espn_id=700,
        slug="eng.1",
        source_season_year=2026,
        start_date=date(2026, 6, 1),
        end_date=date(2027, 6, 1),
        capabilities=_edition().capabilities,
    )
    plan = IngestPlan(
        schema_version=1,
        run_id="run-1",
        as_of=date(2026, 7, 31),
        registry_signature="a" * 64,
        scopes=[scope],
        metadata=metadata,
    )
    counts = {"schedule": 1}
    manifest = ScopeManifest(
        schema_version=1,
        run_id="run-1",
        scope_id="700:2026",
        registry_signature="a" * 64,
        plan_signature="b" * 64,
        state=ManifestState.COMPLETE,
        generated_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
        dispositions=[],
        row_counts=counts,
    )

    aliases[2026].append("changed")
    metadata["nested"]["items"].append(3)
    counts["schedule"] = 99

    assert legacy.league_aliases == ("ENG-Premier League",)
    assert legacy.season_aliases[2026] == ("2627", "2026")
    assert plan.scopes == (scope,)
    assert plan.metadata["nested"]["items"] == (1, 2)
    assert manifest.dispositions == ()
    assert manifest.row_counts["schedule"] == 1
    with pytest.raises(TypeError):
        plan.metadata["new"] = True
    with pytest.raises(TypeError):
        manifest.row_counts["schedule"] = 2


@pytest.mark.unit
@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (
            lambda: Edition(
                2026.0,
                "x",
                date(2026, 1, 1),
                date(2026, 2, 1),
                True,
                _edition().capabilities,
            ),
            "source_season_year",
        ),
        (
            lambda: Competition(
                700.0, "eng.1", "EPL", Gender.MALE, AgeClass.SENIOR, True, (_edition(),)
            ),
            "espn_id",
        ),
        (
            lambda: ScopePlan(
                "700:2026",
                700.0,
                "eng.1",
                2026,
                date(2026, 1, 1),
                date(2026, 2, 1),
                _edition().capabilities,
            ),
            "espn_id",
        ),
        (
            lambda: IngestPlan(2, "run", date(2026, 1, 1), "a" * 64, (), {}),
            "schema_version",
        ),
        (lambda: IngestPlan(1, "", date(2026, 1, 1), "a" * 64, (), {}), "run_id"),
        (
            lambda: IngestPlan(1, "run", date(2026, 1, 1), "not-a-signature", (), {}),
            "registry_signature",
        ),
        (
            lambda: ScopeManifest(
                2,
                "run",
                "700:2026",
                "a" * 64,
                "b" * 64,
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {},
            ),
            "schema_version",
        ),
        (
            lambda: ScopeManifest(
                1,
                "run",
                "700.0:2026",
                "a" * 64,
                "b" * 64,
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {},
            ),
            "scope_id",
        ),
        (
            lambda: ScopeManifest(
                1,
                "run",
                "700:2026",
                "bad",
                "b" * 64,
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {},
            ),
            "registry_signature",
        ),
        (
            lambda: ScopeManifest(
                1,
                "run",
                "700:2026",
                "a" * 64,
                "bad",
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {},
            ),
            "plan_signature",
        ),
        (
            lambda: ScopeManifest(
                1,
                "run",
                "700:2026",
                "a" * 64,
                "b" * 64,
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {"schedule": -1},
            ),
            "non-negative",
        ),
        (
            lambda: ScopeManifest(
                1,
                "run",
                "700:2026",
                "a" * 64,
                "b" * 64,
                ManifestState.COMPLETE,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                (),
                {"schedule": 1.5},
            ),
            "integer",
        ),
    ],
)
def test_native_contracts_reject_invalid_identity_and_integrity_values(
    factory, message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        factory()


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_season_year",
    [True, 2026.0, "2026", "+2026", "2026.0", "02026"],
)
def test_competition_scope_id_rejects_noncanonical_year_values(
    source_season_year,
) -> None:
    competition = Competition(
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        enabled=True,
        editions=(_edition(),),
    )

    with pytest.raises(ValueError, match="source_season_year"):
        competition.scope_id(source_season_year)
