"""Small source-native examples; frozen real-card regression is run separately."""

import copy
import json

import pytest

from scrapers.fotmob.silver_matches import (
    MaterializationError,
    load_contract,
    materialize,
)
from scrapers.fotmob.silver_matches.decoder import _Decoder, _conform_array


@pytest.fixture(scope="module")
def contract():
    return load_contract()


def payload():
    return {
        "general": {
            "matchId": "10",
            "parentLeagueId": 42,
            "leagueId": 42,
            "homeTeam": {"id": 11, "name": "Home"},
            "awayTeam": {"id": 22, "name": "Away"},
        },
        "header": {"teams": [{"id": 11, "name": "Home"}, {"id": 22, "name": "Away"}]},
        "content": {
            "matchFacts": {"matchId": 10, "events": {"events": []}},
            "lineup": None,
            "shotmap": {"shots": []},
        },
    }


def record(data=None):
    return [
        "42",
        "2026/2027",
        "10",
        "batch-1",
        "2026-10-24 00:00:00",
        "claimed-hash",
        "s3://raw/blob",
        "fotmob-native-v2",
        "{}",
        "[]",
        json.dumps(data if data is not None else payload()),
    ]


def with_lineup(data, players):
    data["content"]["lineup"] = {
        "matchId": 10,
        "homeTeam": {"id": 11, "starters": players, "subs": []},
        "awayTeam": {"id": 22, "starters": [], "subs": []},
    }
    return data


def test_sparse_match_is_not_dropped_and_calls_do_not_share_state(contract):
    source = record()
    untouched = copy.deepcopy(source)
    first = materialize([source], "pub-a", contract)
    second = materialize([source], "pub-b", contract)
    assert source == untouched
    assert len(first.rows["matches"]) == len(second.rows["matches"]) == 1
    assert len(first.rows["match_teams"]) == 2
    assert first.rows["match_players"] == []
    assert first.rows["shots"] == []
    assert first.rows["matches"][0]["publication_id"] == "pub-a"
    assert second.rows["matches"][0]["publication_id"] == "pub-b"
    first.rows["matches"].clear()
    assert len(second.rows["matches"]) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d["general"].__setitem__("matchId", "999"),
        lambda d: d["general"].__setitem__("parentLeagueId", 99),
        lambda d: d["header"]["teams"][1].__setitem__("id", 11),
        lambda d: d["general"]["homeTeam"].__setitem__("id", 0),
        lambda d: d.__setitem__("content", []),
    ],
)
def test_invalid_identity_or_mandatory_structure_rejects_candidate(contract, mutation):
    data = payload()
    mutation(data)
    with pytest.raises(MaterializationError):
        materialize([record(data)], "pub", contract)


def test_duplicate_selected_context_rejected(contract):
    with pytest.raises(MaterializationError, match="multiple selected versions"):
        materialize([record(), record()], "pub", contract)


def test_unknown_path_blocks_candidate(contract):
    data = payload()
    data["futureField"] = {"score": 9}
    with pytest.raises(MaterializationError, match="unknown_source_path"):
        materialize([record(data)], "pub", contract)


def test_unknown_player_ids_remain_separate(contract):
    data = with_lineup(
        payload(), [{"id": 0, "name": "Unknown A"}, {"id": 0, "name": "Unknown B"}]
    )
    result = materialize([record(data)], "pub", contract)
    players = result.rows["match_players"]
    assert len(players) == 2
    assert all(row["source_player_id"] is None for row in players)
    assert len({row["local_player_record_key"] for row in players}) == 2
    assert all(
        row["null_reason"]["source_player_id"] == "sentinel_id_zero" for row in players
    )


def test_optional_metric_does_not_drop_healthy_match(contract):
    data = with_lineup(
        payload(),
        [{"id": 111, "name": "Player", "performance": {"rating": "not rated"}}],
    )
    result = materialize([record(data)], "pub", contract)
    assert len(result.rows["matches"]) == 1
    metric = result.rows["player_match_metrics"][0]
    assert metric["value_number"] is None
    assert metric["value_text"] == "not rated"
    assert metric["raw_value"]
    assert metric["null_reason"]["value_number"] == "invalid_optional"


def test_full_rebuild_does_not_retain_removed_children(contract):
    populated = with_lineup(payload(), [{"id": 111, "name": "Player"}])
    old = materialize([record(populated)], "old", contract)
    new = materialize([record()], "new", contract)
    assert len(old.rows["match_players"]) == 1
    assert new.rows["match_players"] == []


def test_bigint_conformance_does_not_round_through_float(contract):
    decoder = _Decoder(contract, "pub")
    decoder.common = {"match_id": 10}
    out = decoder.row(
        "shots",
        {"id": 2**63 - 1},
        "$.content.shotmap.shots[*]",
        "/content/shotmap/shots/0",
    )
    assert out["source_shot_id"] == 2**63 - 1
    invalid = decoder.row(
        "shots", {"id": 2**63}, "$.content.shotmap.shots[*]", "/content/shotmap/shots/1"
    )
    assert invalid["source_shot_id"] is None
    assert invalid["null_reason"]["source_shot_id"] == "invalid_optional"


@pytest.mark.parametrize("source", [None, [], [None], [1, None]])
def test_null_array_and_null_element_remain_distinct(contract, source):
    decoder = _Decoder(contract, "pub")
    decoder.common = {"match_id": 10}
    prefix = "$.content.matchFacts.events.events[*].penShootoutScore"
    decoder.registry[prefix + "[*]"] = dict(decoder.registry[prefix])
    out = decoder.row(
        "match_events",
        {"penShootoutScore": source},
        "$.content.matchFacts.events.events[*]",
        "/content/matchFacts/events/events/0",
    )
    assert out["shootout_score"] == source


def test_unsupported_parser_version_is_not_silently_accepted(contract):
    source = record()
    source[7] = "future-parser-v99"
    with pytest.raises(MaterializationError, match="unsupported Bronze parser"):
        materialize([source], "pub", contract)


def test_same_native_match_in_two_seasons_keeps_separate_coverage(contract):
    data = payload()
    data["content"]["matchFacts"]["events"]["penaltyShootoutEvents"] = None
    first = record(data)
    second = record(data)
    second[1] = "2027/2028"
    second[3] = "batch-2"
    result = materialize([first, second], "pub", contract)
    pointer = "/content/matchFacts/events/penaltyShootoutEvents"
    coverage = [
        row
        for row in result.rows["data_coverage"]
        if row["concrete_source_path"] == pointer
    ]
    assert len(coverage) == 2
    assert {row["source_season_key"] for row in coverage} == {"2026/2027", "2027/2028"}
    assert {row["source_batch_id"] for row in coverage} == {"batch-1", "batch-2"}
    assert all(row["source_state"] == "source_null" for row in coverage)
    for projection in result.projections:
        target = result.rows[projection["table"]][projection["row_index"]]
        assert projection["match_context_key"] == target["match_context_key"]
        assert projection["publication_id"] == target["publication_id"]
        assert projection["source_batch_id"] == target["source_batch_id"]


@pytest.mark.parametrize(
    "source,expected,reason",
    [
        (["2", "3"], [2, 3], None),
        ([None, "2", 3], [None, 2, 3], None),
        ([str(2**63 - 1)], [2**63 - 1], None),
        (
            [str(2**63), "not an integer", 1.5, True],
            [None, None, None, None],
            "invalid_optional_array_element",
        ),
        (None, None, None),
        ([], [], None),
    ],
)
def test_bigint_array_elements_are_typed_without_float_rounding(
    source, expected, reason
):
    assert _conform_array(source, "ARRAY(BIGINT)") == (expected, reason)


def test_string_arrays_do_not_stringify_nested_structures():
    assert _conform_array(["x", 2, None, {"a": 1}, [3]], "ARRAY(VARCHAR)") == (
        ["x", "2", None, None, None],
        "invalid_optional_array_element",
    )


def test_related_match_shootout_array_is_typed_and_raw_preserved(contract):
    data = payload()
    data["content"]["h2h"] = {
        "matches": [
            {
                "status": {"reason": {"penalties": ["2", "3"]}},
            }
        ]
    }
    result = materialize([record(data)], "pub", contract)
    related = result.rows["match_related_matches"][0]
    assert related["shootout_score"] == [2, 3]
    raw = related["raw_value"]
    assert raw["/content/h2h/matches/0/status/reason/penalties/0"] == "2"
    assert raw["/content/h2h/matches/0/status/reason/penalties/1"] == "3"


def test_invalid_optional_array_element_keeps_healthy_match_and_raw(contract):
    data = payload()
    raw_score = ["bad", "2", None, str(2**63)]
    data["content"]["h2h"] = {
        "matches": [{"status": {"reason": {"penalties": raw_score}}}]
    }
    result = materialize([record(data)], "pub", contract)
    related = result.rows["match_related_matches"][0]
    assert len(result.rows["matches"]) == 1
    assert related["shootout_score"] == [None, 2, None, None]
    assert related["null_reason"]["shootout_score"] == "invalid_optional_array_element"
    assert related["raw_value"]["invalid_array:shootout_score"] == raw_score
