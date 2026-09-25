"""Contracts for the repository-owned ESPN ingestion path."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from scrapers.espn.models import (
    AgeClass,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
    SeasonType,
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
def test_open_editions_allows_the_new_season_before_the_old_one_closes() -> None:
    # #1501: a new core season opens while the previous one still has open
    # matches; "exactly one current edition" is gone.
    old = replace(_edition(), source_season_year=2025, display_name="2025-26 EPL")
    new = _edition()
    closed = replace(_edition(), source_season_year=2024, display_name="2024-25 EPL",
                     current=False)
    competition = Competition(
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        enabled=True,
        editions=(closed, old, new),
    )

    assert competition.open_editions() == (old, new)
    with pytest.raises(ValueError, match="at least one open edition"):
        replace(competition, editions=(closed,)).open_editions()


@pytest.mark.unit
def test_edition_carries_its_season_types() -> None:
    types = (SeasonType(1, "League Phase"), SeasonType(2, "Knockout Round Playoffs"))
    edition = replace(_edition(), types=types)

    assert edition.types == types
    assert edition.to_dict()["types"][0] == {
        "id": 1, "name": "League Phase", "start_date": None, "end_date": None,
    }
    with pytest.raises(ValueError, match="repeat a type id"):
        replace(_edition(), types=(SeasonType(1), SeasonType(1)))


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
