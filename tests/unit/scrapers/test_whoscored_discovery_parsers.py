"""Source-preserving WhoScored discovery and structured parser contracts."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

import scrapers.whoscored.parsers as whoscored_parsers
from scrapers.whoscored.catalog import (
    apply_schedule_classification,
    build_technical_exclusion_audit,
    CatalogError,
    classify_tournament,
    DEFAULT_TOURNAMENT_OVERRIDES,
    TournamentOverride,
    WhoScoredCatalog,
)
from scrapers.whoscored.domain import (
    SeasonFormat,
    TournamentEligibility,
    WhoScoredScope,
)
from scrapers.whoscored.parsers import (
    DatasetStatus,
    PlayerStageStatisticsPage,
    WhoScoredParseError,
    extract_matchcentre_data,
    find_source_season_id,
    is_valid_match_page_without_matchcentre,
    merge_player_stage_statistics_pages,
    parse_all_regions,
    parse_matchcentre_data,
    parse_player_stage_statistics,
    parse_player_stage_statistics_page,
    parse_preview_bundle,
    parse_profile_bundle,
    parse_js_literal,
    parse_schedule_json,
    parse_season_page,
    parse_team_stage_statistics,
    parse_tournament_seasons,
    schema_fingerprint,
)
from scrapers.whoscored.repository import canonical_catalog_rows


SCOPE = WhoScoredScope("WS-252-2", "2526", SeasonFormat.SPLIT_YEAR)


@pytest.mark.unit
def test_matchcentre_unavailability_requires_two_matching_source_markers():
    valid = """
    <script>require.config.params['matchheader'] = {
      input: [1, 2, 'Home', 'Away'], matchId: 1973523
    };</script>
    <script>require.config.params["args"] = {
      matchId: 1973523, initialMatchDataForScrappers: [[[1, 2]]]
    };</script>
    """
    assert is_valid_match_page_without_matchcentre(valid, game_id=1973523)
    assert not is_valid_match_page_without_matchcentre(valid, game_id=1973524)

    challenge = """
    <html><title>Just a moment...</title>
    <script src='/cdn-cgi/challenge-platform/x'></script></html>
    """
    error_shell = """
    <html><title>Page not found</title>
    <script>require.config.params['args'] = {matchId: 1973523};</script></html>
    """
    assert not is_valid_match_page_without_matchcentre(challenge, game_id=1973523)
    assert not is_valid_match_page_without_matchcentre(error_shell, game_id=1973523)

    unsupported = valid.replace(
        "initialMatchDataForScrappers:",
        "matchCentreData: JSON.parse('{}'), initialMatchDataForScrappers:",
    )
    with pytest.raises(WhoScoredParseError, match="present but malformed"):
        extract_matchcentre_data(unsupported)
    assert not is_valid_match_page_without_matchcentre(
        unsupported, game_id=1973523
    )


@pytest.mark.unit
def test_all_regions_classifies_every_tournament_without_defaulting_to_men():
    result = parse_all_regions(
        {
            "regions": [
                {
                    "id": 252,
                    "name": "England",
                    "code": "GB-ENG",
                    "tournaments": [
                        {"id": 2, "name": "Premier League"},
                        {"id": 3, "name": "Women's Super League"},
                        {"id": 4, "name": "Premier League U21", "sex": 1},
                        {"id": 5, "name": "FA Cup", "sex": 1},
                        {"id": 6, "name": "Women's Cup", "sex": 1},
                        {"id": 7, "name": "Reserve League", "sex": 1},
                        {"id": 8, "name": "Olympic Games Men", "sex": 1},
                        {"id": 9, "name": "Serie A Femminile"},
                        {"id": 10, "name": "Liga F"},
                        {"id": 11, "name": "NWSL"},
                    ],
                }
            ]
        }
    )
    by_id = {row["tournament_id"]: row for row in result.rows}
    assert by_id[2]["eligibility"] == "quarantined"
    assert by_id[3]["eligibility"] == "excluded_women"
    assert by_id[4]["eligibility"] == "excluded_youth"
    assert by_id[5]["eligibility"] == "included"
    assert by_id[6]["eligibility"] == "quarantined"
    assert by_id[7]["eligibility"] == "excluded_reserve"
    assert by_id[8]["eligibility"] == "excluded_youth"
    assert by_id[9]["eligibility"] == "excluded_women"
    assert by_id[10]["eligibility"] == "excluded_women"
    assert by_id[11]["eligibility"] == "excluded_women"
    assert all(
        row["source_raw_json"] and row["source_schema_fingerprint"]
        for row in result.rows
    )


@pytest.mark.unit
def test_versioned_override_is_an_exception_not_an_allow_list():
    rows = parse_all_regions(
        [
            {
                "id": 1,
                "name": "International",
                "tournaments": [
                    {"id": 10, "name": "Ambiguous Cup"},
                    {"id": 11, "name": "Another Cup"},
                ],
            }
        ],
        overrides=(
            TournamentOverride(
                10,
                TournamentEligibility.INCLUDED,
                "manually verified senior men",
                version="review-7",
                canonical_competition_id="INT-Ambiguous Cup",
            ),
        ),
    ).rows
    assert rows[0]["competition_id"] == "INT-Ambiguous Cup"
    assert rows[0]["override_version"] == "review-7"
    assert rows[0]["eligibility"] == "included"
    assert rows[1]["eligibility"] == "quarantined"


@pytest.mark.unit
def test_reserve_markers_have_a_distinct_disposition_from_youth():
    names = {
        20: "Reserve League",
        21: "National B-Team Cup",
        22: "Development League",
        23: "Premier League 2",
        24: "National U21 League",
    }
    rows = parse_all_regions(
        [
            {
                "id": 1,
                "name": "Test Region",
                "tournaments": [
                    {"id": tournament_id, "name": name, "sex": 1}
                    for tournament_id, name in names.items()
                ],
            }
        ],
        overrides=(),
    ).rows

    by_id = {row["tournament_id"]: row for row in rows}
    assert {
        by_id[tournament_id]["eligibility"] for tournament_id in (20, 21, 22, 23)
    } == {"excluded_reserve"}
    assert by_id[24]["eligibility"] == "excluded_youth"
    assert by_id[20]["classification_reason"] == "name_marks_reserve"


@pytest.mark.unit
def test_technical_exclusion_requires_a_versioned_source_id_override():
    rows = parse_all_regions(
        [
            {
                "id": 1,
                "name": "Test Region",
                "tournaments": [
                    {"id": 30, "name": "Technical Tournament"},
                    {"id": 31, "name": "Technical Tournament"},
                ],
            }
        ],
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "duplicate source shell",
                version="technical-review-1",
                canonical_competition_id="WS-1-31",
            ),
        ),
    ).rows

    by_id = {row["tournament_id"]: row for row in rows}
    assert by_id[30]["eligibility"] == "excluded_technical"
    assert by_id[30]["override_version"] == "technical-review-1"
    assert by_id[30]["classification_reason"] == (
        "explicit_override:duplicate source shell"
    )
    assert by_id[31]["eligibility"] == "quarantined"
    assert by_id[31]["override_version"] is None

    catalog = WhoScoredCatalog.from_rows(
        {"competitions": [by_id[30]], "seasons": [], "stages": []}
    )
    assert (
        catalog.competition(by_id[30]["competition_id"]).eligibility
        is TournamentEligibility.EXCLUDED_TECHNICAL
    )
    assert catalog.quarantined == ()


@pytest.mark.unit
def test_technical_canonical_binding_is_not_used_as_an_identity_alias():
    overrides = (
        TournamentOverride(
            30,
            TournamentEligibility.EXCLUDED_TECHNICAL,
            "duplicate source shell",
            version="technical-review-identity-1",
            canonical_competition_id="WS-1-31",
        ),
    )
    parsed = parse_all_regions(
        [
            {
                "id": 1,
                "name": "Test Region",
                "tournaments": [
                    {"id": 30, "name": "Duplicate Cup", "sex": 1},
                    {"id": 31, "name": "duplicate cup", "sex": 1},
                ],
            }
        ],
        overrides=overrides,
        competition_aliases={(1, 30): "WS-1-31"},
    ).rows
    by_id = {row["tournament_id"]: row for row in parsed}

    assert by_id[30]["competition_id"] == "WS-1-30"
    assert by_id[31]["competition_id"] == "WS-1-31"
    canonical = canonical_catalog_rows(
        {"competitions": parsed, "seasons": (), "stages": ()}
    )
    catalog = WhoScoredCatalog.from_rows(canonical)
    assert len(catalog.competitions) == 2
    audit = build_technical_exclusion_audit(
        canonical,
        source_snapshot_sha256="f" * 64,
        overrides=overrides,
    )
    assert audit["unresolved_candidate_count"] == 0
    assert audit["components"][0]["canonical_competition_id"] == "WS-1-31"


@pytest.mark.unit
def test_technical_audit_is_snapshot_bound_and_requires_versioned_id_review():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-1-30",
                "region_id": 1,
                "tournament_id": 30,
                "tournament_name": "Duplicate Cup",
                "tournament_url": "/Regions/1/Tournaments/31/Show/Cup",
                "eligibility": "excluded_technical",
            },
            {
                "competition_id": "WS-1-31",
                "region_id": 1,
                "tournament_id": 31,
                "tournament_name": "duplicate  cup",
                "tournament_url": "/Regions/1/Tournaments/31/Show/Cup/",
                "eligibility": "included",
            },
        ],
        "seasons": [],
        "stages": [
            {
                "competition_id": "WS-1-30",
                "tournament_id": 30,
                "stage_id": 700,
            },
            {
                "competition_id": "WS-1-31",
                "tournament_id": 31,
                "stage_id": 700,
            },
        ],
    }

    unresolved = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="a" * 64,
        overrides=(),
    )
    assert unresolved["source_snapshot_sha256"] == "a" * 64
    assert unresolved["candidate_count"] == 3
    assert unresolved["unresolved_candidate_count"] == 3
    assert {item["candidate_type"] for item in unresolved["candidates"]} == {
        "normalized_name_within_region",
        "canonical_tournament_link",
        "stage_id_overlap",
    }
    assert {item["review_disposition"] for item in unresolved["candidates"]} == {
        "requires_versioned_source_id_review"
    }

    reviewed = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="a" * 64,
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "duplicate source shell",
                version="technical-review-1",
                canonical_competition_id="WS-1-31",
            ),
        ),
    )
    assert reviewed["candidate_count"] == 3
    assert reviewed["unresolved_candidate_count"] == 0
    assert reviewed["technical_override_registry"] == [
        {
            "tournament_id": 30,
            "reason": "duplicate source shell",
            "version": "technical-review-1",
            "canonical_competition_id": "WS-1-31",
            "present_in_source_snapshot": True,
            "component_id": "component-0001",
        }
    ]
    assert {
        tuple(item["canonical_tournament_ids"]) for item in reviewed["candidates"]
    } == {
        (31,)
    }


@pytest.mark.unit
def test_technical_audit_records_a_reproducible_zero_candidate_result():
    report = build_technical_exclusion_audit(
        {
            "competitions": [
                {
                    "competition_id": "WS-1-31",
                    "region_id": 1,
                    "tournament_id": 31,
                    "tournament_name": "Unique Cup",
                    "tournament_url": "/Regions/1/Tournaments/31/Show/Cup",
                    "eligibility": "included",
                }
            ],
            "seasons": [],
            "stages": [],
        },
        source_snapshot_sha256="b" * 64,
        overrides=(),
    )

    assert report == {
        "schema_version": 1,
        "audit_type": "whoscored_technical_exclusion_audit",
        "source_snapshot_sha256": "b" * 64,
        "classifier_version": "senior-men-v3",
        "technical_override_registry": [],
        "candidate_count": 0,
        "unresolved_candidate_count": 0,
        "component_count": 0,
        "unresolved_component_count": 0,
        "candidates": [],
        "components": [],
    }


@pytest.mark.unit
def test_technical_audit_rejects_a_present_orphan_override():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-1-30",
                "region_id": 1,
                "tournament_id": 30,
                "tournament_name": "Unique Cup",
                "tournament_url": "/Regions/1/Tournaments/30/Show/Unique-Cup",
                "eligibility": "excluded_technical",
            }
        ],
        "seasons": [],
        "stages": [],
    }
    report = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="c" * 64,
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "stale duplicate decision",
                version="technical-review-2",
                canonical_competition_id="WS-1-31",
            ),
        ),
    )

    assert report["candidate_count"] == 1
    assert report["unresolved_candidate_count"] == 1
    assert report["unresolved_component_count"] == 1
    assert report["candidates"][0]["candidate_type"] == (
        "technical_override_without_duplicate_evidence"
    )
    assert "component_has_0_canonical_tournaments" in report["components"][0][
        "validation_failures"
    ]


@pytest.mark.unit
def test_technical_audit_resolves_connected_signals_as_one_component():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-1-30",
                "region_id": 1,
                "tournament_id": 30,
                "tournament_name": "Shared Cup",
                "tournament_url": "/shared-link",
                "eligibility": "excluded_technical",
            },
            {
                "competition_id": "WS-1-31",
                "region_id": 1,
                "tournament_id": 31,
                "tournament_name": "shared cup",
                "tournament_url": "/only-b",
                "eligibility": "included",
            },
            {
                "competition_id": "WS-1-32",
                "region_id": 1,
                "tournament_id": 32,
                "tournament_name": "Other Cup",
                "tournament_url": "/shared-link/",
                "eligibility": "included",
            },
        ],
        "seasons": [],
        "stages": [],
    }
    report = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="d" * 64,
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "duplicate source shell",
                version="technical-review-3",
                canonical_competition_id="WS-1-31",
            ),
        ),
    )

    assert report["candidate_count"] == 2
    assert report["component_count"] == 1
    assert report["unresolved_candidate_count"] == 2
    assert report["unresolved_component_count"] == 1
    assert report["components"][0]["canonical_tournament_ids"] == [31, 32]
    assert "component_has_2_canonical_tournaments" in report["components"][0][
        "validation_failures"
    ]
    assert {item["component_id"] for item in report["candidates"]} == {
        "component-0001"
    }


@pytest.mark.unit
def test_technical_audit_enforces_the_canonical_competition_binding():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-1-30",
                "region_id": 1,
                "tournament_id": 30,
                "tournament_name": "Shared Cup",
                "eligibility": "excluded_technical",
            },
            {
                "competition_id": "WS-1-31",
                "region_id": 1,
                "tournament_id": 31,
                "tournament_name": "shared cup",
                "eligibility": "included",
            },
        ],
        "seasons": [],
        "stages": [],
    }
    report = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="e" * 64,
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "duplicate source shell",
                version="technical-review-4",
                canonical_competition_id="WS-1-999",
            ),
        ),
    )

    assert report["unresolved_candidate_count"] == 1
    assert "override_30_canonical_competition_mismatch" in report["components"][
        0
    ]["validation_failures"]


@pytest.mark.unit
def test_technical_audit_requires_an_included_canonical_survivor():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-1-30",
                "region_id": 1,
                "tournament_id": 30,
                "tournament_name": "Shared Cup",
                "eligibility": "excluded_technical",
            },
            {
                "competition_id": "WS-1-31",
                "region_id": 1,
                "tournament_id": 31,
                "tournament_name": "shared cup",
                "eligibility": "excluded_women",
            },
        ],
        "seasons": [],
        "stages": [],
    }
    report = build_technical_exclusion_audit(
        rows,
        source_snapshot_sha256="1" * 64,
        overrides=(
            TournamentOverride(
                30,
                TournamentEligibility.EXCLUDED_TECHNICAL,
                "duplicate source shell",
                version="technical-review-6",
                canonical_competition_id="WS-1-31",
            ),
        ),
    )

    assert report["unresolved_candidate_count"] == 1
    assert "canonical_tournament_is_not_included" in report["components"][0][
        "validation_failures"
    ]


@pytest.mark.unit
def test_technical_override_requires_a_canonical_competition_binding():
    with pytest.raises(CatalogError, match="has no canonical competition id"):
        TournamentOverride(
            30,
            TournamentEligibility.EXCLUDED_TECHNICAL,
            "duplicate source shell",
            version="technical-review-5",
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    (("reason", ""), ("reason", None), ("version", "  "), ("version", None)),
)
def test_tournament_override_requires_audit_metadata(field, value):
    kwargs = {
        "tournament_id": 30,
        "eligibility": TournamentEligibility.EXCLUDED_TECHNICAL,
        "reason": "duplicate source shell",
        "version": "technical-review-1",
        "canonical_competition_id": "WS-1-31",
    }
    kwargs[field] = value

    with pytest.raises(CatalogError, match=f"has no {field}"):
        TournamentOverride(**kwargs)


@pytest.mark.unit
def test_default_overrides_cover_only_audited_empty_calendar_senior_men():
    audited = (
        (110, 599, "Japan Football League"),
        (196, 416, "Prva Liga"),
        (21, 480, "Premier League Qualification"),
        (245, 252, "1 Division"),
        (252, 23, "Papa John's Trophy"),
    )
    rows = parse_all_regions(
        [
            {
                "id": region_id,
                "name": f"Region {region_id}",
                "tournaments": [{"id": tournament_id, "name": name}],
            }
            for region_id, tournament_id, name in audited
        ]
        + [
            {
                "id": 999,
                "name": "Unknown",
                "tournaments": [{"id": 999999, "name": "Unverified Cup"}],
            }
        ]
    ).rows

    by_id = {row["tournament_id"]: row for row in rows}
    assert set(by_id) == {23, 252, 416, 480, 599, 999999}
    for _region_id, tournament_id, _name in audited:
        assert by_id[tournament_id]["eligibility"] == "included"
        assert by_id[tournament_id]["source_sex"] is None
        assert by_id[tournament_id]["override_version"]
        assert by_id[tournament_id]["classification_reason"].startswith(
            "explicit_override:audited senior men:"
        )

    assert by_id[999999]["eligibility"] == "quarantined"
    assert by_id[999999]["classification_reason"] == "source_sex_not_yet_observed"
    assert {item.tournament_id for item in DEFAULT_TOURNAMENT_OVERRIDES} == {
        23,
        252,
        416,
        480,
        599,
    }

    contradictory = parse_all_regions(
        [
            {
                "id": 110,
                "name": "Japan",
                "tournaments": [
                    {"id": 599, "name": "Women's League", "sex": 0},
                    {"id": 23, "name": "Trophy U21", "sex": 1},
                ],
            }
        ]
    ).rows
    contradictory_by_id = {row["tournament_id"]: row for row in contradictory}
    assert contradictory_by_id[599]["eligibility"] == "excluded_women"
    assert contradictory_by_id[23]["eligibility"] == "excluded_youth"


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_sex",
    (
        1.5,
        0.5,
        Decimal("1.5"),
        float("nan"),
        float("inf"),
        float("-inf"),
        2,
        -1,
        True,
    ),
)
def test_non_exact_source_sex_never_fails_open_or_crashes(source_sex):
    classification = classify_tournament(
        tournament_id=999999,
        tournament_name="Unverified Cup",
        source_sex=source_sex,
        overrides=(),
    )

    assert classification.source_sex is None
    assert classification.eligibility is TournamentEligibility.QUARANTINED
    assert classification.reason in {
        "source_sex_is_boolean",
        "source_sex_is_unknown",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_sex",
    (0, 1, 0.0, 1.0, Decimal("0"), Decimal("1"), "0", "1"),
)
def test_exact_binary_source_sex_is_accepted(source_sex):
    classification = classify_tournament(
        tournament_id=999999,
        tournament_name="Verified Cup",
        source_sex=source_sex,
        overrides=(),
    )

    assert classification.source_sex in {0, 1}
    expected = (
        TournamentEligibility.INCLUDED
        if classification.source_sex == 1
        else TournamentEligibility.EXCLUDED_WOMEN
    )
    assert classification.eligibility is expected


@pytest.mark.unit
def test_season_discovery_retains_unrecognized_labels_as_quarantined():
    competition = parse_all_regions(
        [
            {
                "id": 252,
                "name": "England",
                "tournaments": [{"id": 2, "name": "Premier League", "sex": 1}],
            }
        ]
    ).rows[0]
    seasons = parse_tournament_seasons(
        """
        <select id="seasons">
          <option value="/Regions/252/Tournaments/2/Seasons/100/Stages/1" selected>
            2025/2026
          </option>
          <option value="/Regions/252/Tournaments/2/Seasons/99/Stages/1">Special Edition</option>
        </select>
        """,
        competition_row=competition,
    ).rows
    assert seasons[0]["season_id"] == "2526"
    assert seasons[0]["season_format"] == "split_year"
    assert seasons[0]["source_selected"] is True
    assert seasons[0]["is_active"] is None
    assert seasons[0]["eligibility"] == "included"
    assert seasons[1]["season_id"] is None
    assert seasons[1]["eligibility"] == "quarantined"
    assert seasons[1]["classification_reason"] == "unrecognized_source_season_label"


@pytest.mark.unit
def test_season_discovery_supports_calendar_year_phase_labels():
    competition = parse_all_regions(
        [
            {
                "id": 34,
                "name": "Bulgaria",
                "tournaments": [{"id": 169, "name": "Super Cup", "sex": 1}],
            }
        ]
    ).rows[0]
    html = """
        <select id="seasons">
          <option value="/Regions/34/Tournaments/169/Seasons/1495/Stages/1">
            2008 Fall
          </option>
        </select>
        """
    seasons = parse_tournament_seasons(
        html,
        competition_row=competition,
    ).rows

    assert seasons[0]["season_id"] == "2008"
    assert seasons[0]["season_format"] == "single_year"
    assert seasons[0]["source_label"] == "2008 Fall"
    assert seasons[0]["eligibility"] == "included"
    assert (
        find_source_season_id(
            html,
            WhoScoredScope("WS-34-169", "2008", SeasonFormat.SINGLE_YEAR),
        )
        == 1495
    )


@pytest.mark.unit
def test_season_discovery_losslessly_disambiguates_same_format_collision():
    competition = parse_all_regions(
        [
            {
                "id": 1,
                "name": "Region",
                "tournaments": [{"id": 2, "name": "Cup", "sex": 1}],
            }
        ]
    ).rows[0]
    html = """
        <select id="seasons">
          <option value="/Regions/1/Tournaments/2/Seasons/10/Stages/1">2008</option>
          <option value="/Regions/1/Tournaments/2/Seasons/11/Stages/2">2008 Fall</option>
        </select>
        """
    seasons = parse_tournament_seasons(
        html,
        competition_row=competition,
    ).rows

    assert {row["eligibility"] for row in seasons} == {"included"}
    assert {row["season_id"] for row in seasons} == {
        "2008-single-ws10",
        "2008-single-ws11",
    }
    assert (
        find_source_season_id(
            html,
            WhoScoredScope("WS-1-2", "2008-single-ws11", SeasonFormat.SINGLE_YEAR),
        )
        == 11
    )


@pytest.mark.unit
def test_season_discovery_losslessly_disambiguates_format_collision():
    competition = parse_all_regions(
        [
            {
                "id": 11,
                "name": "Argentina",
                "tournaments": [{"id": 605, "name": "Argentina 4", "sex": 1}],
            }
        ]
    ).rows[0]
    html = """
        <select id="seasons">
          <option value="/Regions/11/Tournaments/605/Seasons/8534/Stages/1">2021</option>
          <option value="/Regions/11/Tournaments/605/Seasons/8426/Stages/2">2020/2021</option>
        </select>
        """
    seasons = parse_tournament_seasons(html, competition_row=competition).rows

    assert {row["eligibility"] for row in seasons} == {"included"}
    assert {row["season_id"] for row in seasons} == {
        "2021-single-ws8534",
        "2021-split-ws8426",
    }
    catalog = WhoScoredCatalog.from_rows(
        {"competitions": [competition], "seasons": seasons, "stages": []}
    )
    assert {scope.scope.spec for scope in catalog.enabled_scopes()} == {
        "WS-11-605=2021-single-ws8534",
        "WS-11-605=2021-split-ws8426",
    }
    assert (
        find_source_season_id(
            html,
            WhoScoredScope("WS-11-605", "2021-split-ws8426", SeasonFormat.SPLIT_YEAR),
        )
        == 8426
    )


@pytest.mark.unit
def test_season_discovery_supports_non_consecutive_multi_year_editions():
    competition = parse_all_regions(
        [
            {
                "id": 248,
                "name": "Africa",
                "tournaments": [
                    {
                        "id": 747,
                        "name": "Africa Cup of Nations Qualification",
                        "sex": 1,
                    }
                ],
            }
        ]
    ).rows[0]
    row = parse_tournament_seasons(
        """
        <select id="seasons"><option selected
          value="/Regions/248/Tournaments/747/Seasons/700/Stages/900">2019/2021</option>
        </select>
        """,
        competition_row=competition,
    ).rows[0]
    assert row["season_id"] == "1921"
    assert row["season_format"] == "multi_year"
    assert row["eligibility"] == "included"


@pytest.mark.unit
def test_discovered_catalog_round_trip_and_active_legacy_compatible_scope():
    competition = dict(
        parse_all_regions(
            [
                {
                    "id": 252,
                    "name": "England",
                    "tournaments": [{"id": 2, "name": "Premier League", "sex": 1}],
                }
            ]
        ).rows[0]
    )
    seasons = list(
        parse_tournament_seasons(
            """
            <select id="seasons"><option selected
              value="/Regions/252/Tournaments/2/Seasons/100/Stages/700">2025/26</option>
            </select>
            """,
            competition_row=competition,
        ).rows
    )
    seasons[0] = {**seasons[0], "is_active": True}
    stages = [
        {
            "competition_id": "WS-252-2",
            "region_id": 252,
            "tournament_id": 2,
            "source_season_id": 100,
            "stage_id": 700,
            "eligibility": "included",
        }
    ]
    rows = {"competitions": [competition], "seasons": seasons, "stages": stages}
    catalog = WhoScoredCatalog.from_rows(rows)
    active = catalog.eligible_scopes(active_only=True)
    assert len(active) == 1
    assert active[0].scope == SCOPE
    assert active[0].stage_ids == (700,)
    assert catalog.resolve_scope("WS-252-2", "2526") == active[0]
    assert WhoScoredCatalog.from_rows(catalog.to_rows()).to_rows() == catalog.to_rows()


@pytest.mark.unit
def test_discovered_catalog_preserves_legacy_exclusion_rows():
    legacy_row = {
        "competition_id": "WS-1-20",
        "region_id": 1,
        "tournament_id": 20,
        "tournament_name": "Reserve League",
        "eligibility": "excluded_youth",
        "classification_reason": "name_marks_youth",
        "classifier_version": "senior-men-v2",
    }

    catalog = WhoScoredCatalog.from_rows(
        {"competitions": [legacy_row], "seasons": [], "stages": []}
    )

    competition = catalog.competition("WS-1-20")
    assert competition.eligibility is TournamentEligibility.EXCLUDED_YOUTH
    assert competition.whoscored_enabled is False
    assert catalog.to_rows()["competitions"] == (legacy_row,)


@pytest.mark.unit
def test_unversioned_persisted_technical_exclusion_is_quarantined_losslessly():
    untrusted_row = {
        "competition_id": "WS-1-30",
        "region_id": 1,
        "tournament_id": 30,
        "tournament_name": "Technical Tournament",
        "eligibility": "excluded_technical",
        "classification_reason": "explicit_override:duplicate source shell",
        "classifier_version": "senior-men-v3",
        "override_version": None,
    }

    catalog = WhoScoredCatalog.from_rows(
        {"competitions": [untrusted_row], "seasons": [], "stages": []}
    )

    competition = catalog.competition("WS-1-30")
    assert competition.eligibility is TournamentEligibility.QUARANTINED
    assert competition.classification_reason == (
        "excluded_technical_without_override_version"
    )
    assert catalog.quarantined[0]["eligibility"] == "quarantined"
    assert catalog.to_rows()["competitions"] == (untrusted_row,)


@pytest.mark.unit
def test_schedule_sex_resolves_provisional_catalog_and_propagates_to_seasons():
    competition = dict(
        parse_all_regions(
            [
                {
                    "id": 252,
                    "name": "England",
                    "tournaments": [{"id": 2, "name": "Premier League"}],
                }
            ]
        ).rows[0]
    )
    season = {
        "competition_id": competition["competition_id"],
        "region_id": 252,
        "tournament_id": 2,
        "season_id": "2526",
        "source_season_id": 100,
        "season_format": "split_year",
        "is_active": True,
        "eligibility": "quarantined",
        "classification_reason": "source_sex_not_yet_observed",
    }
    resolved = apply_schedule_classification(
        {"competitions": [competition], "seasons": [season], "stages": []},
        [{"region_id": 252, "tournament_id": 2, "source_sex": 1}],
    )
    assert resolved["competitions"][0]["eligibility"] == "included"
    assert resolved["seasons"][0]["eligibility"] == "included"
    assert len(WhoScoredCatalog.from_rows(resolved).active_scopes()) == 1


@pytest.mark.unit
def test_active_catalog_fails_closed_when_activity_is_unknown():
    catalog = WhoScoredCatalog.from_rows(
        {
            "competitions": [
                {
                    "competition_id": "WS-1-2",
                    "region_id": 1,
                    "tournament_id": 2,
                    "eligibility": "included",
                }
            ],
            "seasons": [
                {
                    "competition_id": "WS-1-2",
                    "region_id": 1,
                    "tournament_id": 2,
                    "season_id": "2526",
                    "source_season_id": 3,
                    "season_format": "split_year",
                    "eligibility": "included",
                }
            ],
            "stages": [],
        }
    )
    with pytest.raises(CatalogError, match="Cannot determine active status"):
        catalog.active_scopes()


@pytest.mark.unit
def test_tables_push_parses_all_source_season_table_families():
    html = """
      <a href="/Regions/252/Tournaments/2/Seasons/100/Stages/700">Fixtures</a>
      <select id="stages"><option
        value="/Regions/252/Tournaments/2/Seasons/100/Stages/700">League</option></select>
      <script>
        var tables = [];
        tables.push({stageId:700,
          table:[{teamId:26,teamName:'Liverpool',position:1,played:38,points:90}],
          forms3:[{teamId:26,teamName:'Liverpool',form:'WWW'}],
          streaks:[{teamId:26,teamName:'Liverpool',name:'Wins',value:4}],
          performance:[{teamId:26,teamName:'Liverpool',home:50,away:40}]
        });
      </script>
    """
    result = parse_season_page(
        html,
        scope=SCOPE,
        region_id=252,
        tournament_id=2,
        source_season_id=100,
    )
    assert result.standings.rows[0]["team_id"] == 26
    assert result.standings.rows[0]["points"] == 90
    assert result.forms.row_count == 1
    assert result.streaks.row_count == 1
    assert result.performance.row_count == 1
    assert set(result.datasets) == {
        "stages",
        "standings",
        "forms",
        "streaks",
        "performance",
    }


@pytest.mark.unit
def test_positional_season_rows_and_historical_array_elisions_are_preserved():
    assert parse_js_literal("[1,,3,]") == [1, None, 3]
    standings = [700, 26, "Liverpool", 1, 38, 30, 5, 3, 90, 20, 70, 95]
    forms = [700, 26, "Liverpool", 2, 6, 5, 1, 0, 10, 2, 8, 16]
    html = f"""
      <a href="/Regions/252/Tournaments/2/Seasons/100/Stages/700">Fixtures</a>
      <script>var tables=[]; tables.push({{
        stageId:700,standings:{json.dumps([standings])},forms3:{json.dumps([forms])},
        streaksCurrent:[[700,26,'Liverpool',1,5]],
        performance:[[700,26,'Liverpool',1,'33310']]
      }});</script>
    """
    result = parse_season_page(
        html,
        scope=SCOPE,
        region_id=252,
        tournament_id=2,
        source_season_id=100,
    )
    row = result.standings.rows[0]
    assert (row["team_id"], row["team"], row["rank"], row["played"], row["points"]) == (
        26,
        "Liverpool",
        1,
        38,
        95,
    )
    assert row["source_values_json"] == json.dumps(standings, separators=(",", ":"))
    assert (
        result.forms.row_count
        == result.streaks.row_count
        == result.performance.row_count
        == 1
    )


@pytest.mark.unit
def test_season_tables_ignore_rank_coloring_layout_arrays_as_team_rows():
    html = """
      <a href="/Regions/11/Tournaments/68/Seasons/9326/Stages/21111">
        Fixtures
      </a>
      <script>
        var tables = [];
        tables.push({
          stageId: 21111,
          rankColorings: [
            ['standing-zone-top-1', '#00aa00'],
            ['standing-zone-bottom-1', '#aa0000']
          ],
          table: [[21111, 501, 'Senior Team', 1, 10, 7, 2, 1, 20, 8, 12, 23]]
        });
      </script>
    """

    result = parse_season_page(
        html,
        scope=SCOPE,
        region_id=11,
        tournament_id=68,
        source_season_id=9326,
    )

    assert result.standings.row_count == 1
    assert result.standings.rows[0]["stage_id"] == 21111
    assert result.standings.rows[0]["team_id"] == 501
    assert result.standings.rows[0]["team"] == "Senior Team"


def _full_match() -> dict:
    return {
        "attendance": 50000,
        "venueName": "Example Stadium",
        "referee": {"officialId": 9, "name": "Ref Example"},
        "expandedMaxMinute": 95,
        "events": [
            {
                "id": 9_000_001.0,
                "eventId": 1,
                "type": {"displayName": "Pass"},
                "teamId": 26,
                "playerId": 11,
                "isTouch": "false",
                "satisfiedEventsTypes": [1, 2],
            }
        ],
        "playerIdNameDictionary": {"11": "Player A", "12": "Player B"},
        "home": {
            "teamId": 26,
            "name": "Liverpool",
            "scores": {"fulltime": 1},
            "stats": {"possession": {"90": 55.5}},
            "shotZones": {"sixYardBox": {"shots": 2, "goals": 1}},
            "players": [
                {
                    "playerId": 11,
                    "name": "Player A",
                    "isFirstEleven": True,
                    "subbedOutExpandedMinute": 70,
                    "stats": {"ratings": {"70": 7.1}, "passesTotal": {"70": 40}},
                },
                {
                    "playerId": 12,
                    "name": "Player B",
                    "isFirstEleven": False,
                    "subbedInExpandedMinute": 70,
                    "stats": {},
                },
            ],
            "formations": [
                {
                    "formationId": 1,
                    "formationName": "4-3-3",
                    "playerIds": [11, 12],
                    "startMinuteExpanded": 0,
                    "endMinuteExpanded": 95,
                }
            ],
        },
        "away": {
            "teamId": 30,
            "name": "Away",
            "scores": {"fulltime": 0},
            "players": [],
        },
    }


@pytest.mark.unit
def test_match_bundle_covers_match_events_lineups_subs_formations_and_stats():
    result = parse_matchcentre_data(_full_match(), scope=SCOPE, game_id=1, game="g")
    assert result.matches.rows[0]["referee_id"] == 9
    assert result.matches.rows[0]["venue_name"] == "Example Stadium"
    assert result.events.rows[0]["is_touch"] is False
    assert result.events.rows[0]["satisfied_events_types"] == "[1,2]"
    assert result.lineups.row_count == 2
    assert {(row["action"], row["player_id"]) for row in result.substitutions.rows} == {
        ("off", 11),
        ("on", 12),
    }
    assert result.formations.rows[0]["formation_name"] == "4-3-3"
    assert result.team_match_stats.status is DatasetStatus.AVAILABLE
    assert {row["source_path"] for row in result.team_match_stats.rows} >= {
        "possession.90",
        "shotZones.sixYardBox.shots",
        "shotZones.sixYardBox.goals",
    }
    assert result.player_match_stats.status is DatasetStatus.AVAILABLE
    assert all(
        row["source_schema_fingerprint"]
        for dataset in result.datasets.values()
        for row in dataset.rows
    )
    assert all(
        "source_raw_json" not in row
        for dataset in (result.team_match_stats, result.player_match_stats)
        for row in dataset.rows
    )


@pytest.mark.unit
def test_stage_statistics_are_long_form_and_keep_document_shape():
    result = parse_player_stage_statistics(
        {
            "playerTableStats": [
                {
                    "playerId": 11,
                    "playerName": "Player A",
                    "teamId": 26,
                    "teamName": "Liverpool",
                    "apps": 10,
                    "passSuccess": 88.5,
                    "nested": {"per90": 2.2},
                }
            ],
            "paging": {"total": 1},
        },
        scope=SCOPE,
        stage_id=700,
        source_season_id=100,
    )
    assert {row["source_path"] for row in result.rows} >= {
        "apps",
        "passSuccess",
        "nested.per90",
    }
    assert all(row["document_schema_fingerprint"] for row in result.rows)
    assert all("document_raw_json" not in row for row in result.rows)


@pytest.mark.unit
def test_stage_statistics_fingerprint_each_source_record_once(monkeypatch):
    calls = []
    original = whoscored_parsers.schema_fingerprint

    def counted(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(whoscored_parsers, "schema_fingerprint", counted)
    records = [
        {
            "playerId": player_id,
            "teamId": 26,
            **{f"metric{metric}": metric for metric in range(50)},
        }
        for player_id in (11, 12)
    ]

    result = parse_player_stage_statistics(
        {
            "playerTableStats": records,
            "paging": {"totalResults": 2, "totalPages": 1},
        },
        scope=SCOPE,
        stage_id=700,
    )

    assert len(result.rows) == 100
    # One fingerprint for the complete document and one per source record;
    # the former implementation recalculated it for every emitted metric.
    assert len(calls) == 3


@pytest.mark.unit
@pytest.mark.parametrize(
    "parser, list_key, identity",
    (
        (parse_team_stage_statistics, "teamTableStats", {"teamId": 26}),
        (
            parse_player_stage_statistics,
            "playerTableStats",
            {"playerId": 11, "teamId": 26},
        ),
    ),
)
def test_stage_statistics_accept_flaresolverr_body_wrapped_json(
    parser, list_key, identity
):
    record = {**identity, "teamName": "Liverpool", "rating": 7.2}
    document = {list_key: [record], "paging": {"totalResults": 1}}
    payload = (
        "<html><head></head><body>"
        + json.dumps(document, separators=(",", ":"))
        + "</body></html>"
    ).encode()

    result = parser(
        payload,
        scope=SCOPE,
        stage_id=700,
        source_season_id=100,
        source_category="summary",
        source_subcategory="all",
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert {row["source_path"] for row in result.rows} == {"rating"}
    assert all(row["source_category"] == "summary" for row in result.rows)
    assert all(row["source_subcategory"] == "all" for row in result.rows)


@pytest.mark.unit
def test_team_statistics_treat_all_zero_paging_as_unpaginated_sentinel():
    result = parse_team_stage_statistics(
        {
            "teamTableStats": [
                {"teamId": 26, "teamName": "Liverpool", "rating": 7.2},
                {"teamId": 30, "teamName": "Arsenal", "rating": 7.1},
            ],
            "paging": {
                "currentPage": 0,
                "totalPages": 0,
                "resultsPerPage": 0,
                "totalResults": 0,
                "firstRecordIndex": 0,
                "lastRecordIndex": 0,
            },
        },
        scope=SCOPE,
        stage_id=700,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert {row["team_id"] for row in result.rows} == {26, 30}


@pytest.mark.unit
def test_player_statistics_keep_zero_paging_guard_strict():
    payload = {
        "playerTableStats": [
            {"playerId": 11, "teamId": 26, "playerName": "Player", "apps": 1}
        ],
        "paging": {
            "currentPage": 0,
            "totalPages": 0,
            "resultsPerPage": 0,
            "totalResults": 0,
        },
    }

    with pytest.raises(WhoScoredParseError, match="incomplete"):
        parse_player_stage_statistics(payload, scope=SCOPE, stage_id=700)


@pytest.mark.unit
def test_team_zero_paging_accepts_official_xg_current_page_one_sentinel():
    payload = {
        "teamTableStats": [{"teamId": 26, "teamName": "Liverpool", "apps": 1}],
        "paging": {
            "currentPage": 1,
            "totalPages": 0,
            "totalResults": 0,
            "resultsPerPage": 0,
            "firstRecordIndex": 0,
            "lastRecordIndex": 0,
        },
    }

    result = parse_team_stage_statistics(payload, scope=SCOPE, stage_id=700)

    assert result.status is DatasetStatus.AVAILABLE
    assert {row["team_id"] for row in result.rows} == {26}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    (("currentPage", 2), ("resultsPerPage", 1), ("lastRecordIndex", 1)),
)
def test_team_zero_paging_exception_rejects_real_nonzero_page_metadata(field, value):
    paging = {
        "currentPage": 1,
        "totalPages": 0,
        "totalResults": 0,
        "resultsPerPage": 0,
        "firstRecordIndex": 0,
        "lastRecordIndex": 0,
    }
    paging[field] = value
    payload = {
        "teamTableStats": [{"teamId": 26, "teamName": "Liverpool", "apps": 1}],
        "paging": paging,
    }

    with pytest.raises(WhoScoredParseError, match="incomplete"):
        parse_team_stage_statistics(payload, scope=SCOPE, stage_id=700)


@pytest.mark.unit
def test_stage_statistics_reject_malformed_body_wrapped_json():
    with pytest.raises(WhoScoredParseError, match="Discovery JSON is invalid"):
        parse_player_stage_statistics(
            b'<html><body>{"playerTableStats":</body></html>',
            scope=SCOPE,
            stage_id=700,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "paging",
    (
        {"totalResults": 2, "totalPages": 1},
        {"total": 1, "totalPages": 2},
    ),
)
def test_stage_statistics_reject_truncated_or_paginated_response(paging):
    payload = {
        "playerTableStats": [
            {
                "playerId": 11,
                "teamId": 26,
                "playerName": "Player A",
                "apps": 10,
            }
        ],
        "paging": paging,
    }

    with pytest.raises(WhoScoredParseError, match="incomplete|partial first page"):
        parse_player_stage_statistics(
            payload,
            scope=SCOPE,
            stage_id=700,
            source_season_id=100,
        )


@pytest.mark.unit
def test_player_statistics_pages_are_merged_only_after_complete_pagination():
    def payload(page, player_id):
        return {
            "playerTableStats": [
                {
                    "playerId": player_id,
                    "teamId": 26,
                    "playerName": f"Player {player_id}",
                    "apps": page,
                }
            ],
            "paging": {
                "currentPage": page,
                "pageIndex": page - 1,
                "totalPages": 2,
                "totalResults": 2,
                "resultsPerPage": 1,
                "firstRecordIndex": page - 1,
                "lastRecordIndex": page - 1,
            },
        }

    first = parse_player_stage_statistics_page(
        payload(1, 11), scope=SCOPE, stage_id=700, expected_page=1
    )
    second = parse_player_stage_statistics_page(
        payload(2, 12), scope=SCOPE, stage_id=700, expected_page=2
    )
    assert isinstance(first, PlayerStageStatisticsPage)
    assert isinstance(second, PlayerStageStatisticsPage)
    assert first.results_per_page == 1
    assert first.page_index == 0
    assert first.first_record_index == 0
    assert second.last_record_index == 1

    merged = merge_player_stage_statistics_pages(
        [second, first], scope=SCOPE, stage_id=700
    )
    assert merged.status is DatasetStatus.AVAILABLE
    assert {row["player_id"] for row in merged.rows} == {11, 12}
    assert {row["row_index"] for row in merged.rows} == {0, 1}


@pytest.mark.unit
def test_player_statistics_pagination_rejects_repeated_or_wrong_pages():
    payload = {
        "playerTableStats": [{"playerId": 11, "teamId": 26, "apps": 1}],
        "paging": {
            "currentPage": 1,
            "totalPages": 2,
            "totalResults": 2,
            "resultsPerPage": 1,
        },
    }
    with pytest.raises(WhoScoredParseError, match="expected 2"):
        parse_player_stage_statistics_page(
            payload, scope=SCOPE, stage_id=700, expected_page=2
        )

    first = parse_player_stage_statistics_page(
        payload, scope=SCOPE, stage_id=700, expected_page=1
    )
    assert isinstance(first, PlayerStageStatisticsPage)
    repeated = PlayerStageStatisticsPage(
        page_number=2,
        page_index=None,
        total_pages=2,
        total_results=2,
        results_per_page=1,
        first_record_index=None,
        last_record_index=None,
        index_base=None,
        records=first.records,
    )
    with pytest.raises(WhoScoredParseError, match="repeats"):
        merge_player_stage_statistics_pages(
            [first, repeated], scope=SCOPE, stage_id=700
        )


@pytest.mark.unit
@pytest.mark.parametrize("current_page", (0, -1))
def test_player_statistics_requires_positive_current_page_on_single_page(
    current_page,
):
    payload = {
        "playerTableStats": [{"playerId": 11, "teamId": 26, "apps": 1}],
        "paging": {
            "currentPage": current_page,
            "totalPages": 1,
            "totalResults": 1,
            "resultsPerPage": 1,
            "firstRecordIndex": 0,
            "lastRecordIndex": 0,
        },
    }
    with pytest.raises(WhoScoredParseError, match="currentPage must be positive"):
        parse_player_stage_statistics_page(
            payload, scope=SCOPE, stage_id=700, expected_page=1
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("paging_update", "message"),
    (
        ({"totalPages": 101}, "totalPages exceeds"),
        ({"totalResults": 500_001}, "totalResults exceeds"),
        ({"totalPages": 3}, "cardinality contradicts|totalPages disagrees"),
        ({"firstRecordIndex": 0, "lastRecordIndex": 1}, "range cardinality"),
        ({"pageIndex": 1}, "pageIndex disagrees"),
    ),
)
def test_player_statistics_rejects_unbounded_or_inconsistent_page_metadata(
    paging_update, message
):
    paging = {
        "currentPage": 1,
        "totalPages": 2,
        "totalResults": 2,
        "resultsPerPage": 1,
        "firstRecordIndex": 0,
        "lastRecordIndex": 0,
    }
    paging.update(paging_update)
    payload = {
        "playerTableStats": [{"playerId": 11, "teamId": 26, "apps": 1}],
        "paging": paging,
    }
    with pytest.raises(WhoScoredParseError, match=message):
        parse_player_stage_statistics_page(
            payload, scope=SCOPE, stage_id=700, expected_page=1
        )


@pytest.mark.unit
def test_stage_statistics_preserve_distinct_rows_for_the_same_source_entity():
    payload = {
        "playerTableStats": [
            {"playerId": 11, "teamId": 26, "apps": 10},
            {"playerId": 11, "teamId": 26, "apps": 9},
        ],
        "paging": {"totalResults": 2, "totalPages": 1},
    }

    parsed = parse_player_stage_statistics(
        payload,
        scope=SCOPE,
        stage_id=700,
        source_season_id=100,
    )

    assert parsed.status is DatasetStatus.AVAILABLE
    assert [row["row_index"] for row in parsed.rows] == [0, 1]
    assert [row["numeric_value"] for row in parsed.rows] == [10.0, 9.0]


@pytest.mark.unit
def test_team_stage_statistics_preserve_live_repeated_team_grain():
    payload = {
        "teamTableStats": [
            {
                "teamId": 346,
                "teamName": "Argentina",
                "apps": 5,
                "ranking": 3,
                "rating": 6.87,
            },
            {
                "teamId": 346,
                "teamName": "Argentina",
                "apps": 3,
                "ranking": 4,
                "rating": 6.86,
            },
        ],
        # This is the source's exact unpaginated-team sentinel.
        "paging": {
            "currentPage": 1,
            "totalPages": 0,
            "resultsPerPage": 0,
            "totalResults": 0,
            "firstRecordIndex": 0,
            "lastRecordIndex": 0,
        },
    }

    parsed = parse_team_stage_statistics(payload, scope=SCOPE, stage_id=23752)

    assert parsed.status is DatasetStatus.AVAILABLE
    assert {row["row_index"] for row in parsed.rows} == {0, 1}
    assert {row["team_id"] for row in parsed.rows} == {346}
    assert {
        (row["row_index"], row["stat"], row["numeric_value"])
        for row in parsed.rows
        if row["stat"] in {"apps", "ranking", "rating"}
    } == {
        (0, "apps", 5.0),
        (0, "ranking", 3.0),
        (0, "rating", 6.87),
        (1, "apps", 3.0),
        (1, "ranking", 4.0),
        (1, "rating", 6.86),
    }


@pytest.mark.unit
def test_stage_statistics_reject_exact_duplicate_records_that_mask_truncation():
    record = {"playerId": 11, "teamId": 26, "apps": 10}
    payload = {
        "playerTableStats": [record, dict(record)],
        "paging": {"totalResults": 2, "totalPages": 1},
    }

    with pytest.raises(WhoScoredParseError, match="duplicate source records"):
        parse_player_stage_statistics(
            payload,
            scope=SCOPE,
            stage_id=700,
            source_season_id=100,
        )


@pytest.mark.unit
def test_preview_bundle_keeps_predicted_lineups_sections_and_missing_players():
    html = """
      <script>
        var predictedLineups = {home:{formation:'4-3-3',players:[
          {playerId:11,playerName:'Player A',position:{displayName:'FW'},rating:7.2}
        ]}};
        var matchHeaderJson = {venueName:'Ground',predictedScore:'2-1'};
      </script>
      <div id="missing-players"><div><table><tbody><tr>
        <td class="pn"><a href="/Players/12/Show/B">Player B</a></td>
        <td class="reason">Injury</td><td class="confirmed">Confirmed</td>
      </tr></tbody></table></div></div>
    """
    result = parse_preview_bundle(
        html,
        scope=SCOPE,
        game_id=1,
        game="g",
        home_team="Liverpool",
        away_team="Away",
    )
    assert result.preview_lineups.rows[0]["player_id"] == 11
    assert result.preview_lineups.rows[0]["side"] == "home"
    assert {row["section_type"] for row in result.preview_sections.rows} >= {
        "predictedLineups",
        "matchHeaderJson",
    }
    assert result.missing_players.rows[0]["player_id"] == 12


@pytest.mark.unit
def test_preview_rating_accepts_only_the_exact_source_na_placeholder():
    html = """
      <script>
        var predictedLineups = {home:{players:[
          {playerId:11,playerName:'Player A',rating:'N/A'}
        ]}};
      </script>
    """

    result = parse_preview_bundle(
        html,
        scope=SCOPE,
        game_id=1,
        game="g",
        home_team="Home",
        away_team="Away",
    )

    assert result.preview_lineups.rows[0]["rating"] is None

    with pytest.raises(WhoScoredParseError, match="preview.rating is not numeric"):
        parse_preview_bundle(
            html.replace("N/A", "unrated"),
            scope=SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )


@pytest.mark.unit
def test_profile_bundle_keeps_participations_and_latest_matches():
    html = """
      <div><span class="info-label">Name: </span>Player A</div>
      <script>var currentParticipations = [{
        tournamentId:2,seasonId:100,stageId:700,teamId:26,teamName:'Liverpool',
        position:{displayName:'FW'}
      }];</script>
      <div id="latest-matches"><table><tr><td>
        <a href="/Matches/123/Live">Latest</a>
      </td></tr></table></div>
    """
    result = parse_profile_bundle(html, player_id=11)
    assert result.profiles.rows[0]["name"] == "Player A"
    assert any(row["stage_id"] == 700 for row in result.participations.rows)
    assert any(row["game_id"] == 123 for row in result.participations.rows)


@pytest.mark.unit
def test_profile_bundle_reads_live_require_args_and_ignores_tab_shells():
    html = """
      <div><span class="info-label">Name: </span>Player A</div>
      <div id="Top-Tournaments-btn">Top Tournaments</div>
      <div id="All-Tournaments-btn">All Tournaments</div>
      <div id="player-tournament-stats">
        <a href="#player-tournament-stats-summary">Summary</a>
      </div>
      <script>
        require.config.params['args'] = {
          tournaments: [{
            RegionId:247, TournamentId:36, TournamentName:'FIFA World Cup',
            SeasonId:10498, StageId:23752, TeamId:328, TeamName:'Australia',
            PlayerId:11, PositionText:'Defender', GameStarted:2, Rating:6.46
          }],
          playerId: 11,
          currentTeamId: 328
        };
      </script>
    """

    result = parse_profile_bundle(html, player_id=11)

    assert result.participations.status is DatasetStatus.AVAILABLE
    assert len(result.participations.rows) == 1
    row = result.participations.rows[0]
    assert row["source_path"] == "profileArgs.tournaments.0"
    assert row["region_id"] == 247
    assert row["tournament_id"] == 36
    assert row["source_season_id"] == 10498
    assert row["stage_id"] == 23752
    assert row["team_id"] == 328
    assert row["position"] == "Defender"
    assert json.loads(row["source_raw_json"])["Rating"] == 6.46


@pytest.mark.unit
def test_profile_tab_shell_without_source_records_is_not_a_participation():
    html = """
      <div><span class="info-label">Name: </span>Player A</div>
      <div id="Top-Tournaments-btn">Top Tournaments</div>
      <div id="player-tournament-stats">
        <a href="#player-tournament-stats-summary">Summary</a>
      </div>
    """

    result = parse_profile_bundle(html, player_id=11)

    assert result.participations.status is DatasetStatus.NOT_AVAILABLE
    assert result.participations.rows == ()


@pytest.mark.unit
def test_profile_empty_live_tournament_array_is_authoritative_empty():
    html = """
      <div><span class="info-label">Name: </span>Player A</div>
      <div id="player-tournament-stats"></div>
      <script>
        require.config.params['args'] = {
          tournaments: [], playerId: 11, currentTeamId: 328
        };
      </script>
    """

    result = parse_profile_bundle(html, player_id=11)

    assert result.participations.status is DatasetStatus.EMPTY
    assert result.participations.rows == ()


@pytest.mark.unit
def test_schedule_exposes_source_catalog_metadata_and_parses_false_string():
    result = parse_schedule_json(
        json.dumps(
            {
                "tournaments": [
                    {
                        "regionId": 252,
                        "regionName": "England",
                        "tournamentId": 2,
                        "tournamentName": "Premier League",
                        "seasonId": 100,
                        "seasonName": "2025/2026",
                        "sex": 1,
                        "matches": [
                            {
                                "id": 1,
                                "homeTeamName": "Home",
                                "awayTeamName": "Away",
                                "matchIsOpta": "false",
                            }
                        ],
                    }
                ]
            }
        ),
        scope=SCOPE,
        stage_id=700,
    )
    row = result.rows[0]
    assert row["tournament_id"] == 2
    assert row["source_sex"] == 1
    assert row["match_is_opta"] is False


@pytest.mark.unit
def test_schema_fingerprint_tracks_shape_not_values():
    assert schema_fingerprint({"a": 1, "b": ["x"]}) == schema_fingerprint(
        {"a": 999, "b": ["y"]}
    )
    assert schema_fingerprint({"a": 1}) != schema_fingerprint({"a": "1"})
    assert schema_fingerprint([1, "team"]) != schema_fingerprint(["team", 1])
