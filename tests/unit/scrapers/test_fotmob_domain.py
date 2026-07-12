from __future__ import annotations

import pytest

from scrapers.fotmob.domain import (
    CompetitionRef,
    ScopeRef,
    SeasonRef,
    StageRef,
    competition_slug,
)


def test_source_native_identities_do_not_depend_on_names_or_derived_years():
    competition = CompetitionRef(289, "Africa Cup of Nations", source_slug="africa-cup-nations")
    season = SeasonRef(289, "2025")
    stage = StageRef(289, "2025", "playoff:final", name="Final")

    assert competition.identity == 289
    assert season.identity == (289, "2025")
    assert stage.identity == (289, "2025", "playoff:final")
    assert ScopeRef.from_season(season, "playoff:final").identity == (
        289,
        "2025",
        "playoff:final",
    )


def test_competition_slug_is_id_prefixed_presentation_metadata():
    assert competition_slug(42, "UEFA Champions League") == "42-uefa-champions-league"
    assert CompetitionRef(63, "Премьер-лига").presentation_slug == "63"


@pytest.mark.parametrize("value", ["", None, 2025])
def test_exact_source_season_key_must_be_a_nonempty_string(value):
    with pytest.raises(ValueError):
        SeasonRef(47, value)  # type: ignore[arg-type]
