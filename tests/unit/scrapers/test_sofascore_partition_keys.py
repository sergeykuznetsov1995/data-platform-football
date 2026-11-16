import pandas as pd
import pytest

from scrapers.sofascore.partition_keys import (
    assert_canonical_leagues,
    canonical_league,
)


pytestmark = pytest.mark.unit


@pytest.mark.parametrize("league", ["SS-1", "SS-17", "SS-999999999999999999999"])
def test_canonical_league_preserves_valid_keys(league):
    assert canonical_league(league, {}) == league


def test_canonical_league_maps_legacy_without_mutation():
    mapping = {"ENG": 17, "ESP": 8}
    original = mapping.copy()
    assert canonical_league("ENG", mapping) == "SS-17"
    assert canonical_league("ESP", mapping) == "SS-8"
    assert mapping == original


@pytest.mark.parametrize("league", [
    None, 17, True, float("nan"), "", " ", " ENG", "ENG ",
    " SS-17", "SS-17 ", "SS-17\n", "UNKNOWN", "SS", "SS-", "SS-0",
    "SS-00", "SS-017", "SS--1", "SS-+1", "SS-1.0", "SS-1e2",
    "SS-١", "ss-17",
])
def test_canonical_league_rejects_invalid_keys(league):
    with pytest.raises(ValueError):
        canonical_league(league, {"ENG": 17})


@pytest.mark.parametrize("league", ["SS-0", "SS-017", "SS--1", "SS-1.0"])
def test_malformed_ss_keys_cannot_be_rescued_by_mapping(league):
    with pytest.raises(ValueError):
        canonical_league(league, {league: 17})


@pytest.mark.parametrize("tournament_id", [
    None, True, False, 0, -1, 17.0, 1.5, "17", float("nan"), [], {},
])
def test_canonical_league_rejects_invalid_mapping_values(tournament_id):
    with pytest.raises(ValueError):
        canonical_league("ENG", {"ENG": tournament_id})


def test_frame_guard_accepts_canonical_leagues_without_mutation():
    frame = pd.DataFrame({"league": ["SS-17", "SS-8"], "season": ["2526", "2526"]})
    original = frame.copy(deep=True)
    assert assert_canonical_leagues(frame) is None
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("league", [
    "ENG", None, float("nan"), pd.NA, "", " ", "SS-0", "SS-017",
    "SS--1", "SS-1.0", " SS-17", "SS-17\n", 17,
])
def test_frame_guard_rejects_invalid_leagues_without_mutation(league):
    frame = pd.DataFrame({"league": ["SS-17", league], "season": ["2526", "2526"]})
    original = frame.copy(deep=True)
    with pytest.raises(ValueError):
        assert_canonical_leagues(frame)
    pd.testing.assert_frame_equal(frame, original)


def test_frame_guard_rejects_missing_column_without_mutation():
    frame = pd.DataFrame({"season": ["2526"]})
    original = frame.copy(deep=True)
    with pytest.raises(ValueError):
        assert_canonical_leagues(frame)
    pd.testing.assert_frame_equal(frame, original)
