from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scrapers.fotmob.domain import (
    CompetitionRef,
    CompetitionScopeEvidence,
    ProbeStatus,
    ScopeDecision,
    ScopeRef,
    SeasonRef,
    StageRef,
    competition_slug,
)


def test_scope_policy_domain_contract_is_stable():
    observed_at = datetime(2026, 8, 8, tzinfo=timezone.utc)
    evidence = CompetitionScopeEvidence(
        competition_id=47,
        catalog_name="Premier League",
        profile_name="Premier League",
        source_gender="male",
        source_age_group="adult",
        source_type="league",
        probe_status=ProbeStatus.SUCCESS,
        decision=ScopeDecision.INCLUDED,
        reason="structurally confirmed adult men's competition",
        policy_rule="include_structural_male_adult",
        classifier_version="fotmob-men-v1",
        profile_target_key="leagues/47",
        profile_content_hash="profile-sha256",
        catalog_fingerprint="catalog-sha256",
        authoritative_miss_count=0,
        next_probe_at=None,
        observed_at=observed_at,
    )

    assert [status.value for status in ProbeStatus] == [
        "success",
        "pending",
        "not_found",
        "dead",
        "invalid",
    ]
    assert ScopeDecision.PENDING_PROBE.value == "pending_probe"
    assert evidence.competition_id == 47
    assert evidence.observed_at is observed_at


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
