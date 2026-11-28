"""Unit tests for scrapers.sofascore.season_fields (#1367 paid season fields)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.sofascore.manifest import ManifestStatus
from scrapers.sofascore.season_fields import (
    assert_natural_keys,
    parse_cup_trees,
    parse_participants,
    parse_referee_profile,
    parse_rounds,
)
from scrapers.sofascore.season_pipeline import (
    REFEREE_PROFILE_ENDPOINT,
    SeasonMaterializationError,
    SeasonPartitionMaterialization,
    SeasonPartitionPlan,
    build_cup_trees_spec,
    build_participants_spec,
    build_referee_profile_spec,
    build_rounds_spec,
    build_schedule_page_spec,
    build_squad_spec,
    build_standings_total_spec,
    materialize_season_partition,
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

    def test_slug_disambiguates_reused_round_number(self):
        rows = parse_rounds(
            _load_fixture("sofascore_season_89063_rounds.json"),
            source_tournament_id=1221,
            source_season_id=89063,
        )
        assert [row["round_id"] for row in rows] == [
            "1", "2", "3", "4", "5", "6", "5:round-of-16", "27:quarterfinals",
        ]
        assert rows[4]["round"] == rows[6]["round"] == 5
        assert rows[4].get("name") is None
        assert rows[6]["name"] == "Round of 16"
        assert rows[-1]["is_current"] is True

    def test_duplicate_round_number_and_slug_still_raises(self):
        payload = {"rounds": [
            {"round": 5, "slug": "round-of-16"},
            {"round": 5, "slug": "round-of-16"},
        ]}
        with pytest.raises(ValueError, match="duplicate natural key"):
            parse_rounds(payload, source_tournament_id=1, source_season_id=2)

    def test_slug_identity_collision_with_bare_id_fails_closed(self):
        payload = {"rounds": [
            {"round": "5:round-of-16"},
            {"round": 5, "slug": "round-of-16"},
        ]}
        with pytest.raises(ValueError, match="duplicate natural key"):
            parse_rounds(payload, source_tournament_id=1, source_season_id=2)

    def test_stage_prefix_disambiguates_restarted_round_number(self):
        # Liga AUF Uruguaya 2026: Apertura, Intermedio and Clausura all start at 1.
        rows = parse_rounds(
            _load_fixture("sofascore_season_89288_rounds.json"),
            source_tournament_id=278,
            source_season_id=89288,
        )
        assert [row["round_id"] for row in rows] == [
            *[f"Apertura:{number}" for number in range(1, 16)],
            *[f"Intermedio:{number}" for number in range(1, 8)],
            "Intermedio:29:final",
            *[f"Clausura:{number}" for number in range(1, 16)],
            "28:semifinals",
        ]
        assert rows[15]["round"] == rows[23]["round"] == 1
        assert rows[15]["prefix"] == "Intermedio"
        assert rows[23]["prefix"] == "Clausura"
        assert [index for index, row in enumerate(rows) if row["is_current"]] == [38]

    def test_stage_prefix_qualifies_only_the_stage_that_carries_it(self):
        # New Zealand National League 2026: the second stage repeats 1..11 under a prefix.
        rows = parse_rounds(
            _load_fixture("sofascore_season_90902_rounds.json"),
            source_tournament_id=594,
            source_season_id=90902,
        )
        assert [row["round_id"] for row in rows] == [
            *[str(number) for number in range(1, 23)],
            *[f"National League:{number}" for number in range(1, 12)],
        ]
        assert rows[9]["round"] == rows[31]["round"] == 10
        # currentRound names its stage, so round 10 of the first stage is not current.
        assert [index for index, row in enumerate(rows) if row["is_current"]] == [31]

    def test_same_stage_and_number_still_raises(self):
        payload = {"rounds": [
            {"round": 1, "prefix": "Apertura"},
            {"round": 1, "prefix": "Apertura"},
        ]}
        with pytest.raises(ValueError, match="duplicate natural key"):
            parse_rounds(payload, source_tournament_id=1, source_season_id=2)

    def test_blank_or_padded_prefix_is_the_same_stage_marker_in_id_and_current(self):
        payload = {
            "currentRound": {"round": 2, "prefix": " Apertura "},
            "rounds": [
                {"round": 1, "prefix": "Apertura"},
                {"round": 2, "prefix": " Apertura "},
                {"round": 2, "prefix": ""},
            ],
        }
        rows = parse_rounds(payload, source_tournament_id=1, source_season_id=2)
        assert [row["round_id"] for row in rows] == ["Apertura:1", "Apertura:2", "2"]
        assert [row["is_current"] for row in rows] == [False, True, False]

    def test_current_round_naming_both_stage_and_number_picks_one_stage(self):
        payload = {
            "currentRound": {"round": 1, "prefix": "Clausura"},
            "rounds": [
                {"round": 1, "prefix": "Apertura"},
                {"round": 1, "prefix": "Clausura"},
            ],
        }
        rows = parse_rounds(payload, source_tournament_id=1, source_season_id=2)
        assert [row["is_current"] for row in rows] == [False, True]

    def test_current_round_without_stage_matches_only_unprefixed_rounds(self):
        # A mixed-stage season repeats 1..11 under a prefix; the currentRound of
        # the unprefixed stage carries no prefix and must not flag the prefixed twin.
        payload = {
            "currentRound": {"round": 10},
            "rounds": [{"round": 10}, {"round": 10, "prefix": "National League"}],
        }
        rows = parse_rounds(payload, source_tournament_id=1, source_season_id=2)
        assert [row["round_id"] for row in rows] == ["10", "National League:10"]
        assert [row["is_current"] for row in rows] == [True, False]

    def test_current_round_without_stage_flags_nothing_in_an_all_staged_season(self):
        payload = {
            "currentRound": {"round": 1},
            "rounds": [{"round": 1, "prefix": "Apertura"}, {"round": 1, "prefix": "Clausura"}],
        }
        rows = parse_rounds(payload, source_tournament_id=1, source_season_id=2)
        assert [row["is_current"] for row in rows] == [False, False]



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


FRESHNESS_KEY = "2026-11-17"
CANONICAL_LEAGUE = "england-premier-league"
CANONICAL_SEASON = "2025-2026"

_PARTITION_BASE = {
    "source_tournament_id": TOURNAMENT_ID_STR,
    "source_season_id": SEASON_ID_STR,
}


class _FakeRaw:
    def __init__(self, target, content_hash, blob_key, http_status):
        self.target = target
        self.content_hash = content_hash
        self.blob_key = blob_key
        self.http_status = http_status


class _FakeDataset:
    def __init__(self, rows):
        self.rows = tuple(rows)

    @property
    def row_count(self):
        return len(self.rows)


class _FakeManifest:
    def __init__(
        self,
        key,
        status,
        *,
        row_count=0,
        raw_content_hash="",
        raw_blob_key="",
        request_url="",
        fetched_at="",
        http_status=None,
        error_type=None,
        is_terminal=True,
    ):
        self.key = key
        self.status = status
        self.row_count = row_count
        self.raw_content_hash = raw_content_hash
        self.raw_blob_key = raw_blob_key
        self.request_url = request_url
        self.fetched_at = fetched_at
        self.http_status = http_status
        self.error_type = error_type
        self.is_terminal = is_terminal


class _FakeCaptureResult:
    def __init__(self, manifest, datasets=(), raw=None):
        self.manifest = manifest
        self.datasets = dict(datasets)
        self.raw = raw


def _common_kwargs():
    return {
        "source_tournament_id": TOURNAMENT_ID,
        "source_season_id": SEASON_ID,
        "freshness_key": FRESHNESS_KEY,
        "paid_proxy": False,
    }


def _result(
    spec,
    *,
    status,
    dataset_name=None,
    rows=(),
    error_type=None,
    http_status=200,
    is_terminal=True,
):
    datasets = {}
    if dataset_name is not None:
        datasets[dataset_name] = _FakeDataset(rows)
    content_hash = f"hash:{spec.key.endpoint}:{spec.key.target_id}"
    blob_key = f"blob:{spec.key.endpoint}:{spec.key.target_id}"
    manifest = _FakeManifest(
        spec.key,
        status,
        row_count=len(rows),
        raw_content_hash=content_hash,
        raw_blob_key=blob_key,
        request_url=spec.url,
        fetched_at="2026-11-17T00:00:00+00:00",
        http_status=http_status,
        error_type=error_type,
        is_terminal=is_terminal,
    )
    raw = _FakeRaw(spec.raw_target, content_hash, blob_key, http_status)
    return _FakeCaptureResult(manifest, datasets, raw)


def _parsed_result(spec, payload, **kwargs):
    rows = [row for parser in spec.parsers.values() for row in parser(payload)]
    assert rows
    dataset_name = next(iter(spec.parsers))
    return _result(spec, status=ManifestStatus.SUCCESS, dataset_name=dataset_name, rows=rows, **kwargs)


def _empty_result(spec, *, http_status=200):
    return _result(spec, status=ManifestStatus.LEGITIMATE_EMPTY, http_status=http_status)


def _partition_specs(referee_ids=("900",)):
    common = _common_kwargs()
    return [
        build_schedule_page_spec(direction="last", page=0, **common),
        build_schedule_page_spec(direction="next", page=0, **common),
        build_standings_total_spec(**common),
        build_rounds_spec(**common),
        build_cup_trees_spec(**common),
        build_participants_spec(**common),
        *(
            build_referee_profile_spec(referee_id=referee_id, **common)
            for referee_id in referee_ids
        ),
    ]


def _season_plan(specs):
    return SeasonPartitionPlan(
        source_tournament_id=TOURNAMENT_ID_STR,
        source_season_id=SEASON_ID_STR,
        freshness_key=FRESHNESS_KEY,
        event_freshness_key=FRESHNESS_KEY,
        specs=tuple(specs),
        pending_keys=(),
        missing_raw_keys=(),
        schedule_event_ids=(),
        team_ids=(),
        referee_ids=(),
    )


def _materialize(specs, results):
    return materialize_season_partition(
        _season_plan(specs),
        results,
        canonical_league=CANONICAL_LEAGUE,
        canonical_season=CANONICAL_SEASON,
    )


def _partition_results(specs, *, referee_result=None):
    fixture_by_endpoint = {
        "rounds": ROUNDS_FIXTURE,
        "cup_trees": CUP_TREES_FIXTURE,
        "participants": PARTICIPANTS_FIXTURE,
    }
    results = []
    for spec in specs:
        endpoint = spec.key.endpoint
        if endpoint.startswith("schedule_"):
            results.append(_empty_result(spec, http_status=404))
        elif endpoint == "standings_total":
            results.append(_empty_result(spec))
        elif endpoint == REFEREE_PROFILE_ENDPOINT:
            results.append(
                referee_result
                or _parsed_result(spec, _load_fixture(REFEREE_FIXTURE))
            )
        else:
            results.append(_parsed_result(spec, _load_fixture(fixture_by_endpoint[endpoint])))
    return results


class TestSeasonFieldSpecsNormalized:
    @pytest.mark.parametrize(
        ("builder", "endpoint", "collection", "fixture_name"),
        [
            (build_rounds_spec, "rounds", "rounds", ROUNDS_FIXTURE),
            (build_cup_trees_spec, "cup_trees", "cupTrees", CUP_TREES_FIXTURE),
            (build_participants_spec, "participants", "teams", PARTICIPANTS_FIXTURE),
        ],
    )
    def test_season_specs_are_parser_backed(
        self, builder, endpoint, collection, fixture_name
    ):
        path_by_endpoint = {
            "rounds": "rounds",
            "cup_trees": "cuptrees",
            "participants": "teams",
        }
        spec = builder(**_common_kwargs())
        assert spec.raw_only is False
        assert set(spec.parsers) == {endpoint}
        assert spec.paid_proxy is False
        assert spec.key.endpoint == endpoint
        assert spec.key.target_type == "season"
        assert spec.key.target_id == SEASON_ID_STR
        assert (
            spec.url
            == f"https://www.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID_STR}"
            f"/season/{SEASON_ID_STR}/{path_by_endpoint[endpoint]}"
        )

        payload = _load_fixture(fixture_name)
        assert spec.schema_validator(payload) is True
        assert spec.schema_validator({}) is False
        assert spec.empty_predicate({collection: []}) is True

        rows = spec.parsers[endpoint](payload)
        assert rows
        for row in rows:
            assert row["source_tournament_id"] == TOURNAMENT_ID_STR
            assert row["source_season_id"] == SEASON_ID_STR

    def test_referee_spec_is_parser_backed_with_requested_id(self):
        spec = build_referee_profile_spec(referee_id=900, **_common_kwargs())
        assert spec.raw_only is False
        assert set(spec.parsers) == {REFEREE_PROFILE_ENDPOINT}
        assert spec.paid_proxy is False
        assert spec.key.endpoint == REFEREE_PROFILE_ENDPOINT
        assert spec.key.target_type == "referee"
        assert spec.key.target_id == "900"
        assert spec.url == "https://www.sofascore.com/api/v1/referee/900"

        payload = _load_fixture(REFEREE_FIXTURE)
        assert spec.schema_validator(payload) is True
        assert spec.schema_validator({"referee": {"id": 999, "slug": "other"}}) is False
        assert spec.empty_predicate({"referee": None}) is True

        rows = spec.parsers[REFEREE_PROFILE_ENDPOINT](payload)
        assert [row["requested_referee_id"] for row in rows] == ["900"]
        assert rows[0]["source_tournament_id"] == TOURNAMENT_ID_STR
        assert rows[0]["source_season_id"] == SEASON_ID_STR

    def test_squads_spec_stays_raw_only(self):
        spec = build_squad_spec(team_id=42, **_common_kwargs())
        assert spec.raw_only is True
        assert spec.parsers == {}
        assert spec.key.endpoint == "squads"

    def test_default_materialization_fields_are_empty_tuples(self):
        materialization = SeasonPartitionMaterialization(
            canonical_league=CANONICAL_LEAGUE,
            canonical_season=CANONICAL_SEASON,
            schedule_rows=(),
            standings_rows=(),
            raw_lineage={},
            endpoint_statuses={},
            deferred_keys=(),
        )
        assert materialization.rounds_rows == ()
        assert materialization.cup_trees_rows == ()
        assert materialization.participants_rows == ()
        assert materialization.referee_profile_rows == ()


class TestSeasonPartitionMaterializationFields:
    def test_all_four_datasets_materialized_with_lineage(self):
        specs = _partition_specs()
        results = _partition_results(specs)
        materialization = _materialize(specs, results)

        assert [row["round_id"] for row in materialization.rounds_rows] == ["1", "2", "3"]
        assert [row["is_current"] for row in materialization.rounds_rows] == [False, False, True]
        assert [(row["cup_tree_id"], row["node_id"]) for row in materialization.cup_trees_rows] == [
            ("9001", "1"),
            ("9001", "2"),
        ]
        assert [row["team_id"] for row in materialization.participants_rows] == ["42", "44"]
        assert len(materialization.referee_profile_rows) == 1
        referee_row = materialization.referee_profile_rows[0]
        assert referee_row["requested_referee_id"] == "900"
        assert referee_row["referee_id"] == "900"

        datasets = {
            "rounds": materialization.rounds_rows,
            "cup_trees": materialization.cup_trees_rows,
            "participants": materialization.participants_rows,
            REFEREE_PROFILE_ENDPOINT: materialization.referee_profile_rows,
        }
        for endpoint, rows in datasets.items():
            assert rows
            for row in rows:
                assert row["league"] == CANONICAL_LEAGUE
                assert row["season"] == CANONICAL_SEASON
                assert row["source_tournament_id"] == TOURNAMENT_ID_STR
                assert row["source_season_id"] == SEASON_ID_STR
                assert row["raw_endpoint"] == endpoint
                assert row["raw_content_hash"]
                assert row["raw_blob_key"]
                assert row["raw_request_url"]
                assert row["raw_fetched_at"]
        assert {row["raw_target_id"] for row in materialization.rounds_rows} == {SEASON_ID_STR}
        assert {row["raw_target_id"] for row in materialization.referee_profile_rows} == {"900"}
        assert set(materialization.raw_lineage) == {spec.key for spec in specs}
        assert materialization.schedule_rows == ()
        assert materialization.deferred_keys == ()

    def test_empty_datasets_materialize_without_schedule_rows(self):
        specs = _partition_specs()
        results = [_empty_result(spec, http_status=404 if spec.key.endpoint.startswith("schedule_") else 200) for spec in specs]
        materialization = _materialize(specs, results)

        assert materialization.schedule_rows == ()
        assert materialization.standings_rows == ()
        assert materialization.rounds_rows == ()
        assert materialization.cup_trees_rows == ()
        assert materialization.participants_rows == ()
        assert materialization.referee_profile_rows == ()
        assert materialization.deferred_keys == ()
        assert materialization.endpoint_completeness == 1.0

    def test_deferred_normalized_referee_profile_is_allowed(self):
        specs = _partition_specs()
        referee_spec = specs[-1]
        referee_rows = referee_spec.parsers[REFEREE_PROFILE_ENDPOINT](
            _load_fixture(REFEREE_FIXTURE)
        )
        deferred_result = _result(
            referee_spec,
            status=ManifestStatus.RETRYABLE_FAILURE,
            dataset_name=REFEREE_PROFILE_ENDPOINT,
            rows=referee_rows,
            error_type="DeferredMaterialization",
            http_status=200,
            is_terminal=False,
        )
        materialization = _materialize(
            specs, _partition_results(specs, referee_result=deferred_result)
        )

        assert materialization.deferred_keys == (referee_spec.key,)
        assert [row["requested_referee_id"] for row in materialization.referee_profile_rows] == ["900"]
        assert materialization.referee_profile_rows[0]["raw_content_hash"]
        assert materialization.referee_profile_rows[0]["raw_blob_key"]
        assert materialization.endpoint_statuses[referee_spec.key] == ManifestStatus.RETRYABLE_FAILURE
        assert materialization.endpoint_completeness == 1.0
        assert materialization.match_phase_ready is True
        assert [row["round_id"] for row in materialization.rounds_rows] == ["1", "2", "3"]

    def test_rejected_referee_schema_error_stays_optional(self):
        specs = _partition_specs()
        referee_spec = specs[-1]
        rejected_result = _result(
            referee_spec,
            status=ManifestStatus.SCHEMA_ERROR,
            error_type="SchemaValidationError",
            http_status=200,
        )
        materialization = _materialize(
            specs, _partition_results(specs, referee_result=rejected_result)
        )

        assert materialization.referee_profile_rows == ()
        assert materialization.endpoint_statuses[referee_spec.key] == ManifestStatus.SCHEMA_ERROR
        assert materialization.match_phase_ready is True
        assert set(materialization.raw_lineage) == {spec.key for spec in specs}

    def test_non_referee_schema_error_blocks_publication(self):
        common = _common_kwargs()
        specs = [
            build_schedule_page_spec(direction="last", page=0, **common),
            build_schedule_page_spec(direction="next", page=0, **common),
            build_rounds_spec(**common),
        ]
        results = [
            _empty_result(specs[0], http_status=404),
            _empty_result(specs[1], http_status=404),
            _result(
                specs[2],
                status=ManifestStatus.SCHEMA_ERROR,
                error_type="SchemaValidationError",
                http_status=200,
            ),
        ]
        with pytest.raises(SeasonMaterializationError):
            _materialize(specs, results)


class TestSeasonFieldDuplicateGuards:
    @pytest.mark.parametrize(
        ("build_spec", "endpoint", "key_row"),
        [
            (
                lambda common: build_rounds_spec(**common),
                "rounds",
                {"round_id": "1"},
            ),
            (
                lambda common: build_cup_trees_spec(**common),
                "cup_trees",
                {"cup_tree_id": "9001", "node_id": "1"},
            ),
            (
                lambda common: build_participants_spec(**common),
                "participants",
                {"team_id": "42"},
            ),
            (
                lambda common: build_referee_profile_spec(referee_id=900, **common),
                REFEREE_PROFILE_ENDPOINT,
                {"requested_referee_id": "900", "referee_id": "900"},
            ),
        ],
    )
    def test_duplicate_natural_keys_fail_publication(self, build_spec, endpoint, key_row):
        common = _common_kwargs()
        specs = [
            build_schedule_page_spec(direction="last", page=0, **common),
            build_schedule_page_spec(direction="next", page=0, **common),
            build_spec(common),
        ]
        duplicate_rows = [
            {**_PARTITION_BASE, **key_row},
            {**_PARTITION_BASE, **key_row},
        ]
        results = [
            _empty_result(specs[0], http_status=404),
            _empty_result(specs[1], http_status=404),
            _result(
                specs[2],
                status=ManifestStatus.SUCCESS,
                dataset_name=endpoint,
                rows=duplicate_rows,
            ),
        ]
        with pytest.raises(SeasonMaterializationError, match="natural key check failed"):
            _materialize(specs, results)


# --- #1367: the rounds/blocks cup-trees payload shape (World Cup 2026) -------

CUP_TREES_ROUNDS_FIXTURE = "sofascore_cup_trees_rounds_shape.json"


def _rounds_fixture() -> dict:
    return _load_fixture(CUP_TREES_ROUNDS_FIXTURE)


class TestParseCupTreesRoundsShape:
    def test_fixture_rows_one_per_block_with_round_context(self):
        rows = parse_cup_trees(
            _rounds_fixture(), source_tournament_id=16, source_season_id=58210
        )
        assert [(row["node_id"], row["block_kind"]) for row in rows] == [
            ("2585779", "round"),
            ("2585780", "round"),
            ("2585775", "final"),
            ("2585806", "third_place"),
        ]
        for row in rows:
            assert row["cup_tree_id"] == "10560975"
            assert row["source_tournament_id"] == "16"
            assert row["source_season_id"] == "58210"
            assert row["parent_node_id"] is None
            assert row["children_json"] == "[]"
        semifinal = rows[0]
        assert semifinal["round_description"] == "Semifinal"
        assert semifinal["round_order"] == 4
        assert semifinal["round_type"] == 16
        assert semifinal["result"] == "1:1"
        assert semifinal["home_team_score"] == "1 (3)"
        assert semifinal["away_team_score"] == "1 (4)"
        assert semifinal["finished"] is True
        assert semifinal["venue_name"] == "MetLife Stadium"
        assert semifinal["venue_city_name"] == "East Rutherford"
        assert semifinal["events_json"] == "[12813014]"
        participants = json.loads(semifinal["participants_json"])
        assert [p["team"]["nameCode"] for p in participants] == ["GER", "PAR"]
        assert participants[1]["sourceBlockId"] == 2218740

    def test_standalone_role_merges_into_repeated_round_block(self):
        # finalMatchCupBlock repeats the Final round block's id 2585775: one
        # row keeps BOTH the round context and the explicit final role
        # (review adjudication — first-seen dedupe must not drop the role).
        rows = parse_cup_trees(
            _rounds_fixture(), source_tournament_id=16, source_season_id=58210
        )
        final_rows = [row for row in rows if row["node_id"] == "2585775"]
        assert len(final_rows) == 1
        assert final_rows[0]["block_kind"] == "final"
        assert final_rows[0]["round_description"] == "Final"
        # review r1: the merged row keeps the round identity, not just the label
        assert final_rows[0]["round_id"] == "56005"
        ids = [row["node_id"] for row in rows]
        assert len(ids) == len(set(ids))

    def test_repeated_standalone_block_still_validates_list_fields(self):
        # review r1: a malformed non-list on the repeated standalone block
        # must raise — the duplicate branch is not a validation bypass.
        payload = _rounds_fixture()
        payload["cupTrees"][0]["finalMatchCupBlock"]["events"] = 12813005
        with pytest.raises(
            ValueError, match=r"finalMatchCupBlock: field 'events' must be a list"
        ):
            parse_cup_trees(payload, source_tournament_id=16, source_season_id=58210)

    def test_empty_block_fields_encode_empty_lists(self):
        rows = parse_cup_trees(
            _rounds_fixture(), source_tournament_id=16, source_season_id=58210
        )
        bye = next(row for row in rows if row["node_id"] == "2585780")
        assert bye["participants_json"] == "[]"
        assert bye["events_json"] == "[]"

    def test_absent_standalone_blocks_are_skipped(self):
        payload = _rounds_fixture()
        tree = payload["cupTrees"][0]
        tree["finalMatchCupBlock"] = None
        del tree["thirdPlaceMatchCupBlock"]
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert [row["block_kind"] for row in rows] == ["round", "round", "round"]

    def test_nonempty_nodes_still_win_over_rounds(self):
        payload = _rounds_fixture()
        payload["cupTrees"][0]["nodes"] = [{"id": 7}]
        rows = parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
        assert [row["node_id"] for row in rows] == ["7"]

    def test_malformed_items_raise(self):
        with pytest.raises(
            ValueError, match=r"cup_trees\[0\].rounds\[0\]: missing required field 'id'"
        ):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "rounds": [{"blocks": []}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )
        with pytest.raises(
            ValueError,
            match=r"cup_trees\[0\].rounds\[0\].blocks\[0\]: missing required field 'id'",
        ):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "rounds": [{"id": 2, "blocks": [{}]}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )
        with pytest.raises(
            ValueError, match=r"field 'blocks' must be a list, got dict"
        ):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "rounds": [{"id": 2, "blocks": {}}]}]},
                source_tournament_id=1,
                source_season_id=2,
            )
        with pytest.raises(
            ValueError, match=r"field 'rounds' must be a list, got dict"
        ):
            parse_cup_trees(
                {"cupTrees": [{"id": 1, "rounds": {}}]},
                source_tournament_id=1,
                source_season_id=2,
            )

    @pytest.mark.parametrize("field", ["participants", "events"])
    def test_non_list_block_fields_raise(self, field):
        block = {"id": 3}
        block[field] = {} if field == "participants" else 12813014
        payload = {"cupTrees": [{"id": 1, "rounds": [{"id": 2, "blocks": [block]}]}]}
        with pytest.raises(
            ValueError, match=rf"field '{field}' must be a list"
        ):
            parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)

    def test_malformed_standalone_block_raises(self):
        payload = _rounds_fixture()
        payload["cupTrees"][0]["finalMatchCupBlock"] = "nope"
        with pytest.raises(
            ValueError, match=r"cup_trees\[0\].finalMatchCupBlock: expected an object"
        ):
            parse_cup_trees(payload, source_tournament_id=1, source_season_id=2)
