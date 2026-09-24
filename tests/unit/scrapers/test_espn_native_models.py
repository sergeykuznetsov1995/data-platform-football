"""Contracts for the repository-owned ESPN ingestion path."""

from __future__ import annotations

from datetime import date

import pytest

from scrapers.espn.models import (
    ADMITTED_AGE_CLASSES,
    AgeClass,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
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
    aliases[2026].append("changed")

    assert legacy.league_aliases == ("ENG-Premier League",)
    assert legacy.season_aliases[2026] == ("2627", "2026")


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
