"""Source-contract tests for WhoScored's positional stage-team feeds."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest

from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.parsers import DatasetStatus, WhoScoredParseError
from scrapers.whoscored.stage_feeds import (
    STAGE_TEAM_FEED_BY_TYPE,
    STAGE_TEAM_FEED_CATALOG,
    STAGE_TEAM_FEED_CATALOG_FINGERPRINT,
    STAGE_TEAM_FEED_CATALOG_VERSION,
    fingerprint_stage_team_feed_catalog,
    parse_stage_team_feed,
    stage_team_feed_url,
)


SCOPE = WhoScoredScope("WS-36-1", "2026", SeasonFormat.SINGLE_YEAR)


@pytest.mark.unit
def test_catalog_is_exact_immutable_and_fingerprinted():
    assert tuple(spec.type_id for spec in STAGE_TEAM_FEED_CATALOG) == (
        2,
        3,
        6,
        7,
        8,
        11,
        18,
        25,
    )
    assert len(STAGE_TEAM_FEED_BY_TYPE) == 8
    assert all(
        spec.catalog_version == STAGE_TEAM_FEED_CATALOG_VERSION
        for spec in STAGE_TEAM_FEED_CATALOG
    )
    assert {
        (spec.field, spec.against)
        for spec in STAGE_TEAM_FEED_CATALOG
        if spec.type_id != 25
    } == {(2, 0)}
    assert (
        STAGE_TEAM_FEED_BY_TYPE[25].field,
        STAGE_TEAM_FEED_BY_TYPE[25].against,
    ) == (-1, -1)
    assert STAGE_TEAM_FEED_CATALOG_FINGERPRINT == (
        "a28172b594d2b5373bf9337678d05a8acc3a90f38ca31d3e6c749642ba66aab3"
    )
    assert fingerprint_stage_team_feed_catalog(reversed(STAGE_TEAM_FEED_CATALOG)) == (
        STAGE_TEAM_FEED_CATALOG_FINGERPRINT
    )
    with pytest.raises(FrozenInstanceError):
        STAGE_TEAM_FEED_CATALOG[0].field = 0  # type: ignore[misc]
    with pytest.raises(TypeError):
        STAGE_TEAM_FEED_BY_TYPE[99] = STAGE_TEAM_FEED_CATALOG[0]  # type: ignore[index]


@pytest.mark.unit
def test_url_uses_the_exact_catalog_default_filters():
    assert stage_team_feed_url(23752, 2) == (
        "https://www.whoscored.com/stagestatfeed/23752/stageteams/"
        "?type=2&stageId=23752&teamId=-1&field=2&against=0"
    )
    assert stage_team_feed_url(23752, 25).endswith(
        "?type=25&stageId=23752&teamId=-1&field=-1&against=-1"
    )


@pytest.mark.unit
def test_touch_channels_parse_the_inline_primed_literal_without_execution():
    html = """
        <script>
          require.config.params['args'] = {
            tournamentId: 36,
            touchChannels: [[[324, 'Austria', 15, [[555, 366, 404]]]]]
          };
        </script>
    """
    result = parse_stage_team_feed(
        html,
        scope=SCOPE,
        stage_id=23752,
        feed_type=2,
        source_season_id=10368,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert result.row_count == 3
    assert [row["stat"] for row in result.rows] == ["left", "centre", "right"]
    assert [row["numeric_value"] for row in result.rows] == [555.0, 366.0, 404.0]
    assert {row["source_category"] for row in result.rows} == {"stagestatfeed"}
    assert {row["source_subcategory"] for row in result.rows} == {
        "type_2_touch_channels"
    }
    assert {row["team_id"] for row in result.rows} == {324}
    assert {row["team"] for row in result.rows} == {"Austria"}
    assert {row["source_season_id"] for row in result.rows} == {10368}
    assert [row["source_path"] for row in result.rows] == [
        "$[0][0][3][0][0]",
        "$[0][0][3][0][1]",
        "$[0][0][3][0][2]",
    ]
    expected_filter = {
        "against": 0,
        "field": 2,
        "stageId": 23752,
        "teamId": -1,
        "type": 2,
    }
    assert all(json.loads(row["filter"]) == expected_filter for row in result.rows)
    raw_rows = [row for row in result.rows if row["source_raw_json"] is not None]
    assert len(raw_rows) == 1
    assert json.loads(raw_rows[0]["source_raw_json"])[0] == 324
    assert len({row["source_schema_fingerprint"] for row in result.rows}) == 1
    assert len({row["document_schema_fingerprint"] for row in result.rows}) == 1


@pytest.mark.unit
def test_live_endpoint_single_quoted_array_is_parsed_without_javascript_eval():
    result = parse_stage_team_feed(
        "[[[324,'Austria',15,[[555,366,404]]]]]",
        scope=SCOPE,
        stage_id=23752,
        feed_type=2,
        source_season_id=10368,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert [row["numeric_value"] for row in result.rows] == [555.0, 366.0, 404.0]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("feed_type", "values", "expected_stats"),
    [
        (
            3,
            [1, 2, 3, 4, 5, 6],
            [
                "for_defense",
                "for_midfield",
                "for_attack",
                "against_defense",
                "against_midfield",
                "against_attack",
            ],
        ),
        (6, [7, 8, 9], ["left", "centre", "right"]),
        (7, [10, 11, 12], ["Isb", "Ib", "Ob"]),
    ],
)
def test_vector_feeds_decode_only_bundle_proven_dimensions(
    feed_type,
    values,
    expected_stats,
):
    result = parse_stage_team_feed(
        [[[1, "Team", 252, [values]]]],
        scope=SCOPE,
        stage_id=700,
        feed_type=feed_type,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert [row["stat"] for row in result.rows] == expected_stats
    assert [row["numeric_value"] for row in result.rows] == [
        float(value) for value in values
    ]


@pytest.mark.unit
def test_goal_types_preserve_labels_and_name_only_proven_dimensions():
    payload = [
        [
            [
                324,
                "Austria",
                15,
                [
                    [
                        [
                            ["goal", "openplay", "rightfoot", [3]],
                            ["miss", "corner", "header", [1]],
                        ]
                    ]
                ],
            ]
        ]
    ]
    result = parse_stage_team_feed(
        json.dumps(payload),
        scope=SCOPE,
        stage_id=23752,
        feed_type=8,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert result.row_count == 8
    assert [row["stat"] for row in result.rows[:4]] == [
        "outcome",
        "situation",
        "body_part",
        "count",
    ]
    assert [row["text_value"] for row in result.rows[:3]] == [
        "goal",
        "openplay",
        "rightfoot",
    ]
    assert result.rows[3]["numeric_value"] == 3.0
    assert json.loads(result.rows[3]["subcategory"]) == {
        "body_part": "rightfoot",
        "outcome": "goal",
        "situation": "openplay",
    }
    assert result.rows[3]["source_path"] == "$[0][0][3][0][0][0][3][0]"


@pytest.mark.unit
def test_pass_types_preserve_opaque_position_and_normalize_source_labels():
    payload = [
        [
            [
                10,
                "Passing Team",
                20,
                [
                    [
                        99,
                        [
                            ["pass", "cross", [12]],
                            ["pass", "throughball", [3]],
                        ],
                    ]
                ],
            ]
        ]
    ]
    result = parse_stage_team_feed(
        payload,
        scope=SCOPE,
        stage_id=701,
        feed_type=11,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert result.row_count == 7
    assert result.rows[0]["stat"] == "position_0"
    assert result.rows[0]["numeric_value"] == 99.0
    count_rows = [row for row in result.rows if row["stat"] == "count"]
    assert [(row["subcategory"], row["numeric_value"]) for row in count_rows] == [
        ("cross", 12.0),
        ("throughball", 3.0),
    ]


@pytest.mark.unit
def test_cards_preserve_source_codes_instead_of_inventing_card_labels():
    payload = [[[20, "Card Team", 30, [[[[1, 5], [3, 2], [9, 1]]]]]]]
    result = parse_stage_team_feed(
        payload,
        scope=SCOPE,
        stage_id=702,
        feed_type=18,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert result.row_count == 6
    assert [row["numeric_value"] for row in result.rows] == [
        1.0,
        5.0,
        3.0,
        2.0,
        9.0,
        1.0,
    ]
    assert [row["subcategory"] for row in result.rows if row["stat"] == "count"] == [
        "source_code_1",
        "source_code_3",
        "source_code_9",
    ]


@pytest.mark.unit
def test_cards_preserve_live_named_metric_tail_alongside_source_codes():
    payload = [
        [
            [
                20,
                "Card Team",
                30,
                [
                    [
                        [
                            [1, 2],
                            [7, 3],
                            [
                                [
                                    ["fk_foul_lost", [47]],
                                    ["total_red_card", [0]],
                                    ["total_yel_card", [5]],
                                ]
                            ],
                        ],
                    ]
                ],
            ]
        ]
    ]

    result = parse_stage_team_feed(
        payload,
        scope=SCOPE,
        stage_id=23752,
        feed_type=18,
    )

    named_counts = [
        (row["subcategory"], row["numeric_value"])
        for row in result.rows
        if row["stat"] == "count" and not row["subcategory"].startswith("source_code")
    ]
    assert named_counts == [
        ("fk_foul_lost", 47.0),
        ("total_red_card", 0.0),
        ("total_yel_card", 5.0),
    ]


@pytest.mark.unit
def test_teams_played_decodes_the_source_json_string_without_eval():
    html = """
      <script>
        require.config.params['args'] = {
          teamsPlayed: [[[324, 'Austria', 15, ['[1,3,4]']]]]
        };
      </script>
    """
    result = parse_stage_team_feed(
        html,
        scope=SCOPE,
        stage_id=23752,
        feed_type=25,
    )

    assert result.status is DatasetStatus.AVAILABLE
    assert [row["stat"] for row in result.rows] == [
        "source_encoded_counts",
        "home",
        "away",
        "overall",
    ]
    assert result.rows[0]["text_value"] == "[1,3,4]"
    assert [row["numeric_value"] for row in result.rows[1:]] == [1.0, 3.0, 4.0]
    assert all(
        json.loads(row["filter"])["field"] == -1
        and json.loads(row["filter"])["against"] == -1
        for row in result.rows
    )

    malicious = [[[324, "Austria", 15, ["[1,2,__import__('os').system('false')]"]]]]
    with pytest.raises(WhoScoredParseError, match="invalid JSON"):
        parse_stage_team_feed(
            malicious,
            scope=SCOPE,
            stage_id=23752,
            feed_type=25,
        )


@pytest.mark.unit
def test_empty_and_unavailable_are_distinct():
    for payload in ([], [[]], "[]", "[[]]"):
        parsed = parse_stage_team_feed(
            payload,
            scope=SCOPE,
            stage_id=700,
            feed_type=2,
        )
        assert parsed.status is DatasetStatus.EMPTY
        assert parsed.reason == "source_stagestatfeed_empty"

    unavailable = parse_stage_team_feed(
        "null",
        scope=SCOPE,
        stage_id=700,
        feed_type=2,
    )
    assert unavailable.status is DatasetStatus.NOT_AVAILABLE
    assert unavailable.reason == "source_stagestatfeed_unavailable"

    primed_absent = parse_stage_team_feed(
        "<html><script>require.config.params['args'] = {};</script></html>",
        scope=SCOPE,
        stage_id=700,
        feed_type=2,
    )
    assert primed_absent.status is DatasetStatus.NOT_AVAILABLE
    assert primed_absent.reason == "source_inline_touchChannels_absent"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("payload", "feed_type", "message"),
    [
        ("{}", 2, "root must be a positional array"),
        ([[[1, "Team", 2, [[1, 2]]]]], 2, "array of length 3"),
        (
            [
                [
                    [1, "Team", 2, [[1, 2, 3]]],
                    [1, "Duplicate", 2, [[1, 2, 3]]],
                ]
            ],
            2,
            "duplicate team identity",
        ),
        (
            [[[1, "Team", 2, [[[["goal", "openplay", "header", [1, 2]]]]]]]],
            8,
            "count vector",
        ),
    ],
)
def test_malformed_or_drifted_positional_shapes_fail_closed(
    payload,
    feed_type,
    message,
):
    with pytest.raises(WhoScoredParseError, match=message):
        parse_stage_team_feed(
            payload,
            scope=SCOPE,
            stage_id=700,
            feed_type=feed_type,
        )


@pytest.mark.unit
def test_unknown_types_and_non_primed_html_never_become_empty_success():
    with pytest.raises(WhoScoredParseError, match="unknown stagestatfeed type 99"):
        parse_stage_team_feed(
            [[]],
            scope=SCOPE,
            stage_id=700,
            feed_type=99,
        )
    with pytest.raises(WhoScoredParseError, match="neither a JSON array"):
        parse_stage_team_feed(
            "<html>not a type-11 JSON response</html>",
            scope=SCOPE,
            stage_id=700,
            feed_type=11,
        )
    with pytest.raises(WhoScoredParseError, match="inline-prime container"):
        parse_stage_team_feed(
            "<html>Cloudflare challenge, not a TeamStatistics page</html>",
            scope=SCOPE,
            stage_id=700,
            feed_type=2,
        )
