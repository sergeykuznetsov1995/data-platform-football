"""Fail-closed contracts for WhoScored's Detailed statistics catalog."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from scrapers.whoscored.detailed_feeds import (
    DETAILED_FEED_CATALOG,
    DETAILED_FEED_CATALOG_FINGERPRINT,
    DETAILED_FEED_CATALOG_VERSION,
    DETAILED_FEED_OPTIONS,
    PLAYER_DETAILED_STATISTICS_ENDPOINT,
    TEAM_DETAILED_STATISTICS_ENDPOINT,
    DetailedFeedFamily,
    SortByProvenance,
    fingerprint_detailed_feed_catalog,
    validate_detailed_feed_catalog,
)


EXPECTED_OPTIONS = (
    ("shots", "zones"),
    ("shots", "situations"),
    ("shots", "accuracy"),
    ("shots", "bodyparts"),
    ("goals", "zones"),
    ("goals", "situations"),
    ("goals", "bodyparts"),
    ("conversion", "zones"),
    ("conversion", "situations"),
    ("passes", "length"),
    ("passes", "type"),
    ("key-passes", "length"),
    ("key-passes", "type"),
    ("assists", "type"),
    ("blocks", "type"),
    ("offsides", "type"),
    ("fouls", "type"),
    ("cards", "type"),
    ("possession-loss", "type"),
    ("dribbles", "success"),
    ("tackles", "success"),
    ("interception", "success"),
    ("clearances", "success"),
    ("aerial", "success"),
    ("saves", "shotzone"),
)


@pytest.mark.unit
def test_catalog_has_exact_25_unique_options_for_both_endpoint_families():
    assert (
        tuple((option.category, option.subcategory) for option in DETAILED_FEED_OPTIONS)
        == EXPECTED_OPTIONS
    )
    assert len(DETAILED_FEED_OPTIONS) == len(set(DETAILED_FEED_OPTIONS)) == 25
    assert len(DETAILED_FEED_CATALOG) == 50
    assert len({entry.key for entry in DETAILED_FEED_CATALOG}) == 50

    expected_endpoints = {
        DetailedFeedFamily.TEAM: TEAM_DETAILED_STATISTICS_ENDPOINT,
        DetailedFeedFamily.PLAYER: PLAYER_DETAILED_STATISTICS_ENDPOINT,
    }
    expected_options = set(DETAILED_FEED_OPTIONS)
    for family, endpoint in expected_endpoints.items():
        family_entries = [
            entry for entry in DETAILED_FEED_CATALOG if entry.family is family
        ]
        assert len(family_entries) == 25
        assert {entry.option for entry in family_entries} == expected_options
        assert {entry.endpoint for entry in family_entries} == {endpoint}


@pytest.mark.unit
def test_entries_are_immutable_versioned_and_do_not_invent_sort_values():
    entry = DETAILED_FEED_CATALOG[0]
    assert entry.catalog_version == DETAILED_FEED_CATALOG_VERSION
    assert entry.sort_by == ""
    assert entry.sort_by_provenance is SortByProvenance.SAFE_EMPTY_FALLBACK
    assert entry.sort_by_evidence is None
    assert all(
        candidate.sort_by == ""
        and candidate.sort_by_provenance is SortByProvenance.SAFE_EMPTY_FALLBACK
        and candidate.sort_by_evidence is None
        for candidate in DETAILED_FEED_CATALOG
    )
    with pytest.raises(FrozenInstanceError):
        entry.sort_by = "Rating"  # type: ignore[misc]


@pytest.mark.unit
def test_catalog_fingerprint_is_stable_and_order_independent():
    assert DETAILED_FEED_CATALOG_FINGERPRINT == (
        "a58764cb4c3baf5c781690f34b791fccd653cb3279bff14b71793c5e36e72c2b"
    )
    assert fingerprint_detailed_feed_catalog(reversed(DETAILED_FEED_CATALOG)) == (
        DETAILED_FEED_CATALOG_FINGERPRINT
    )


@pytest.mark.unit
def test_startup_validation_rejects_catalog_edit_without_version_bump():
    changed_entry = replace(
        DETAILED_FEED_CATALOG[0],
        sort_by="Rating",
        sort_by_provenance=SortByProvenance.SOURCE_BUNDLE,
        sort_by_evidence="reviewed-bundle.js:fixture",
    )
    changed_catalog = (changed_entry, *DETAILED_FEED_CATALOG[1:])

    with pytest.raises(
        RuntimeError,
        match="fingerprint changed without a reviewed version bump",
    ):
        validate_detailed_feed_catalog(changed_catalog)


@pytest.mark.unit
def test_startup_validation_rejects_incomplete_static_catalog():
    with pytest.raises(RuntimeError, match="must contain 50 entries"):
        validate_detailed_feed_catalog(DETAILED_FEED_CATALOG[:-1])
