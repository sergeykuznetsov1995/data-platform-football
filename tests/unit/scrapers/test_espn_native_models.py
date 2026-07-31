"""Contracts for the repository-owned ESPN ingestion path."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from scrapers.espn.models import (
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
