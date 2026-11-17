"""Unit tests for scrapers.sofascore.season_fields (#1367 paid season fields)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.sofascore.season_fields import (
    assert_natural_keys,
    parse_cup_trees,
    parse_participants,
    parse_referee_profile,
    parse_rounds,
)

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"

TOURNAMENT_ID = 17
SEASON_ID = 76986
TOURNAMENT_ID_STR = "17"
SEASON_ID_STR = "76986"

ROUNDS_FIXTURE = "sofascore_season_76986_rounds.json"
CUP_TREES_FIXTURE = "sofascore_season_76986_cup_trees.json"
PARTICIPANTS_FIXTURE = "sofascore_season_76986_participants.json"
REFEREE_FIXTURE = "sofascore_referee_900_profile.json"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class TestParseRounds:
    def test_fixture_rows(self):
        rows = parse_rounds(
            _load_fixture(ROUNDS_FIXTURE),
            source_tournament_id=TOURNAMENT_ID,
            source_season_id=SEASON_ID,
        )
        assert [row["round_id"] for row in rows] == ["1", "2", "3"]
        for row in rows:
            assert row["source_tournament_id"] == TOURNAMENT_ID_STR
            assert row["source_season_id"] == SEASON_ID_STR
            assert row["round"] == int(row["round_id"])
            assert row["name"] == f"Round {row['round_id']}"
        assert [row["is_current"] for row in rows] == [False, False, True]

    def test_is_current_matches_round_and_name(self):
        matching = parse_rounds(
            {"rounds": [{"round": 7, "name": "Group stage"}], "currentRound": {"round": 7, "name": "Group stage"}},
            source_tournament_id="1",
            source_season_id="2",
        )
        assert matching[0]["is_current"] is True

        name_mismatch = parse_rounds(
            {"rounds": [{"round": 7, "name": "Group stage"}], "currentRound": {"round": 7, "name": "Knockout"}},
            source_tournament_id="1",
            source_season_id="2",
        )
        assert name_mismatch[0]["is_current"] is False

        round_mismatch = parse_rounds(
            {"rounds": [{"round": 7, "name": "Group stage"}], "currentRound": {"round": 8, "name": "Group stage"}},
            source_tournament_id="1",
            source_season_id="2",
        )
        assert round_mismatch[0]["is_current"] is False

    def test_is_current_without_name_in_current_round(self):
        rows = parse_rounds(
            {
                "rounds": [{"round": 7, "name": "Group stage"}, {"round": 8, "name": "Knockout"}],
                "currentRound": {"round": 8},
            },
            source_tournament_id="1",
            source_season_id="2",
        )
        assert [row["is_current"] for row in rows] == [False, True]

    def test_without_current_round_all_false(self):
        rows = parse_rounds(
            {"rounds": [{"round": 1, "name": "Round 1"}]},
            source_tournament_id="1",
            source_season_id="2",
        )
        assert rows[0]["is_current"] is False

    def test_empty_rounds_returns_empty(self):
        assert parse_rounds({"rounds": []}, source_tournament_id=1, source_season_id=2) == []
        assert parse_rounds({}, source_tournament_id=1, source_season_id=2) == []

    def test_missing_or_null_round_raises(self):
        with pytest.raises(ValueError, match=r"rounds\[0\]: missing required field 'round'"):
            parse_rounds({"rounds": [{"name": "Round 1"}]}, source_tournament_id=1, source_season_id=2)
        with pytest.raises(ValueError, match=r"rounds\[1\]: missing required field 'round'"):
            parse_rounds(
                {"rounds": [{"round": 1}, {"round": None}]},
                source_tournament_id=1,
                source_season_id=2,
            )

    def test_duplicate_round_id_raises(self):
        payload = {"rounds": [{"round": 3, "name": "Round 3"}, {"round": 3, "name": "Round 3"}]}
        with pytest.raises(ValueError, match="duplicate natural key"):
            parse_rounds(payload, source_tournament_id=1, source_season_id=2)


class TestParseCupTrees:
    def test_fixture_rows(self):
        rows = parse_cup_trees(
            _load_fixture(CUP_TREES_FIXTURE),
            source_tournament_id=TOURNAMENT_ID,
            source_season_id=SEASON_ID,
        )
        assert [(row["cup_tree_id"], row["node_id"]) for row in rows] == [("9001", "1"), ("9001", "2")]
        for row in rows:
            assert row["source_tournament_id"] == TOURNAMENT_ID_STR
            assert row["source_season_id"] == SEASON_ID_STR
            assert row["parent_node_id"] is None
        assert rows[0]["round"] == 1
        assert rows[0]["event_id"] == 14000001
        assert rows[0]["children_json"] == "[]"
        assert rows[1]["children_json"] == "[1]"

    def test_nested_children_become_rows_with_parent_links(self):
        children = [{"id": 11, "name": "Final", "children": [{"id": 12, "name": "Winner"}]}]
        payload = {"cupTrees": [{"id": 77, "nodes": [{"id": 10, "name": "Semifinal", "children": children}]}]}
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert [(row["cup_tree_id"], row["node_id"], row["parent_node_id"]) for row in rows] == [
            ("77", "10", None),
            ("77", "11", "10"),
            ("77", "12", "11"),
        ]
        assert rows[0]["children_json"] == _canonical_json(children)
        assert rows[1]["children_json"] == _canonical_json(children[0]["children"])
        assert rows[2]["children_json"] == "[]"
        for row in rows:
            assert row["source_tournament_id"] == "1"
            assert row["source_season_id"] == "2"

    def test_scalar_children_do_not_become_rows(self):
        payload = {"cupTrees": [{"id": 5, "nodes": [{"id": 50, "children": [51, 52]}]}]}
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert [row["node_id"] for row in rows] == ["50"]
        assert rows[0]["children_json"] == "[51,52]"

    def test_children_json_is_canonical(self):
        payload = {"cupTrees": [{"id": 6, "nodes": [{"id": 60, "children": [{"z": 1, "a": "Örebro", "id": 61}]}]}]}
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert rows[0]["children_json"] == '[{"a":"Örebro","id":61,"z":1}]'

    def test_does_not_duplicate_revisited_nodes(self):
        payload = {
            "cupTrees": [
                {
                    "id": 9,
                    "nodes": [
                        {"id": 1, "children": [{"id": 2, "children": [{"id": 1}]}]},
                        {"id": 2},
                    ],
                }
            ]
        }
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert [(row["node_id"], row["parent_node_id"]) for row in rows] == [("1", None), ("2", "1")]

    def test_malformed_items_raise(self):
        with pytest.raises(ValueError, match=r"cup_trees\[0\]: missing required field 'id'"):
            parse_cup_trees({"cupTrees": [{"nodes": []}]}, source_tournament_id=1, source_season_id=2)
        with pytest.raises(ValueError, match=r"cup_trees\[0\].nodes\[0\]: missing required field 'id'"):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "nodes": [{"round": 2}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )
        with pytest.raises(
            ValueError,
            match=r"cup_trees\[0\].nodes\[0\].children\[0\]: missing required field 'id'",
        ):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "nodes": [{"id": 2, "children": [{"name": "no id"}]}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )
        with pytest.raises(ValueError, match=r"cup_trees\[0\].nodes\[0\].children\[0\]: child must be"):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "nodes": [{"id": 2, "children": [None]}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )

    def test_empty_cup_trees_returns_empty(self):
        assert parse_cup_trees({"cupTrees": []}, source_tournament_id=1, source_season_id=2) == []
        assert parse_cup_trees({}, source_tournament_id=1, source_season_id=2) == []


class TestParseParticipants:
    def test_fixture_rows(self):
        rows = parse_participants(
            _load_fixture(PARTICIPANTS_FIXTURE),
            source_tournament_id=TOURNAMENT_ID,
            source_season_id=SEASON_ID,
        )
        assert [row["team_id"] for row in rows] == ["42", "44"]
        assert rows[0]["name"] == "Arsenal"
        assert rows[0]["gender"] == "M"
        assert rows[1]["name"] == "Liverpool"
        for row in rows:
            assert row["source_tournament_id"] == TOURNAMENT_ID_STR
            assert row["source_season_id"] == SEASON_ID_STR

    def test_scalars_flattened_snake_case(self):
        payload = {"teams": [{"id": 42, "name": "Arsenal", "userCount": 2500}]}
        rows = parse_participants(payload, source_tournament_id=1, source_season_id=2)
        assert rows[0]["user_count"] == 2500

    def test_duplicate_team_id_raises(self):
        payload = {"teams": [{"id": 42, "name": "Arsenal"}, {"id": 42, "name": "Arsenal FC"}]}
        with pytest.raises(ValueError, match="duplicate natural key"):
            parse_participants(payload, source_tournament_id=1, source_season_id=2)

    def test_missing_team_id_raises(self):
        payload = {"teams": [{"id": 1, "name": "First"}, {"name": "Second"}]}
        with pytest.raises(ValueError, match=r"participants\[1\]: missing required field 'id'"):
            parse_participants(payload, source_tournament_id=1, source_season_id=2)

    def test_empty_teams_returns_empty(self):
        assert parse_participants({"teams": []}, source_tournament_id=1, source_season_id=2) == []
        assert parse_participants({}, source_tournament_id=1, source_season_id=2) == []


class TestParseRefereeProfile:
    def test_fixture_row(self):
        rows = parse_referee_profile(
            _load_fixture(REFEREE_FIXTURE),
            source_tournament_id=TOURNAMENT_ID,
            source_season_id=SEASON_ID,
            requested_referee_id=900,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["requested_referee_id"] == "900"
        assert row["referee_id"] == "900"
        assert row["slug"] is None
        assert row["name"] == "Example Referee"
        assert row["country_alpha2"] == "GB"
        assert row["country_name"] == "England"
        assert row["statistics_matches"] == 100
        assert row["statistics_yellow_cards"] == 350
        assert row["statistics_red_cards"] == 10
        assert row["source_tournament_id"] == TOURNAMENT_ID_STR
        assert row["source_season_id"] == SEASON_ID_STR

    def test_requested_id_differs_from_actual_and_slug_kept(self):
        payload = {"referee": {"id": 148502, "name": "Szymon Marciniak", "slug": "szymon-marciniak"}}
        rows = parse_referee_profile(
            payload,
            source_tournament_id=1,
            source_season_id=2,
            requested_referee_id=999,
        )
        row = rows[0]
        assert row["requested_referee_id"] == "999"
        assert row["referee_id"] == "148502"
        assert row["slug"] == "szymon-marciniak"
        assert row["source_tournament_id"] == "1"
        assert row["source_season_id"] == "2"

    def test_requested_id_stringified(self):
        rows = parse_referee_profile(
            {"referee": {"id": 1}},
            source_tournament_id=1,
            source_season_id=2,
            requested_referee_id=12345,
        )
        assert rows[0]["requested_referee_id"] == "12345"

    def test_null_referee_returns_empty(self):
        kwargs = {"source_tournament_id": 1, "source_season_id": 2, "requested_referee_id": 900}
        assert parse_referee_profile({"referee": None}, **kwargs) == []
        assert parse_referee_profile({}, **kwargs) == []

    def test_malformed_referee_raises(self):
        kwargs = {"source_tournament_id": 1, "source_season_id": 2, "requested_referee_id": 1}
        with pytest.raises(ValueError, match=r"referee_profile\.referee: missing required field 'id'"):
            parse_referee_profile({"referee": {"name": "No id"}}, **kwargs)
        with pytest.raises(ValueError, match=r"referee_profile\.referee: expected an object"):
            parse_referee_profile({"referee": [1, 2]}, **kwargs)
        with pytest.raises(ValueError, match=r"referee_profile: requested_referee_id must not be None"):
            parse_referee_profile(
                {"referee": {"id": 1}},
                source_tournament_id=1,
                source_season_id=2,
                requested_referee_id=None,
            )


class TestAssertNaturalKeys:
    def test_unique_rows_return_none(self):
        rows = [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}, {"a": "3", "b": "x"}]
        assert assert_natural_keys(rows, ("a",)) is None
        assert assert_natural_keys(rows, ("a", "b")) is None
        assert assert_natural_keys([], ("a",)) is None

    def test_missing_or_null_key_raises(self):
        with pytest.raises(ValueError, match=r"rows\[0\]"):
            assert_natural_keys([{"a": None}], ("a",))
        with pytest.raises(ValueError, match=r"rows\[1\]"):
            assert_natural_keys([{"a": "1"}, {"b": "2"}], ("a",))

    def test_duplicate_key_raises(self):
        with pytest.raises(ValueError, match=r"rows\[1\].*duplicate natural key"):
            assert_natural_keys([{"a": "1"}, {"a": "1"}], ("a",))
