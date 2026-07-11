from __future__ import annotations

import pytest

from scrapers.fotmob.catalog import SelectedSeasonMismatch
from scrapers.fotmob.domain import LeaderboardCategoryRef, ScopeRef
from scrapers.fotmob.parsers import (
    inventory_json_paths,
    parse_leaderboards,
    parse_season_bundle,
    parse_transfers,
)


@pytest.fixture
def tournament_payload():
    return {
        "tabs": ["overview", "table", "playoff", "fixtures", "stats", "seasons"],
        "allAvailableSeasons": ["2025", "2023"],
        "details": {
            "id": 289,
            "name": "Africa Cup of Nations",
            "selectedSeason": "2025",
            "latestSeason": "2025",
            "country": "INT",
            "gender": "male",
        },
        "table": [
            {
                "data": {
                    "composite": True,
                    "leagueId": 289,
                    "leagueName": "Africa Cup of Nations",
                    "tables": [
                        {
                            "leagueId": 895039,
                            "leagueName": "Grp. A",
                            "pageUrl": "/leagues/895039/overview/grp-a?season=2025",
                            "legend": [
                                {"title": "Qualification", "tKey": "qualification", "color": "#0f0", "indices": [1]}
                            ],
                            "table": {
                                "all": [
                                    {"id": 1, "name": "Alpha", "idx": 1, "played": 1,
                                     "wins": 0, "draws": 0, "losses": 1,
                                     "scoresStr": "0-1", "pts": 0, "qualColor": "#0f0"},
                                    {"id": 2, "name": "Beta", "idx": 2, "played": 1,
                                     "wins": 1, "draws": 0, "losses": 0,
                                     "scoresStr": "1-0", "pts": 3},
                                ],
                                "home": [
                                    {"id": 1, "name": "Alpha", "idx": 1, "played": 1, "pts": 0}
                                ],
                            },
                        },
                        {
                            "leagueId": 1,
                            "leagueName": "Best 3rd placed teams",
                            "table": {
                                "all": [
                                    {"id": 1, "name": "Alpha", "idx": 1, "played": 1, "pts": 0}
                                ]
                            },
                        },
                    ],
                }
            }
        ],
        "fixtures": {
            "allMatches": [
                {
                    "id": "100",
                    "round": "1",
                    "roundName": 1,
                    "pageUrl": "/matches/alpha-vs-beta/x#100",
                    "home": {"id": "1", "name": "Alpha"},
                    "away": {"id": "2", "name": "Beta"},
                    "status": {"utcTime": "2025-01-01T12:00:00Z", "finished": True,
                               "cancelled": False, "awarded": False, "scoreStr": "0 - 1",
                               "reason": {"long": "Full-Time", "longKey": "finished"}},
                },
                {
                    "id": 100,
                    "home": {"id": 1, "name": "Alpha"},
                    "away": {"id": 2, "name": "Beta"},
                    "status": {"finished": True},
                },
            ],
            "fixtureInfo": {
                "activeRound": {"roundId": "final"},
                "rounds": [
                    {"roundId": "1", "localizedKey": "round_fmt"},
                    {"roundId": "final", "localizedKey": "finalTournament"},
                ],
                "teams": [
                    {"id": 1, "name": "Alpha"},
                    {"id": 2, "name": "Beta"},
                    {"id": 3, "name": "Gamma"},
                ],
            },
        },
        "playoff": {
            "rounds": [
                {
                    "participantCount": 2,
                    "stage": "final",
                    "matchups": [
                        {
                            "homeTeamId": 2,
                            "awayTeamId": 4,
                            "homeTeam": "Beta",
                            "awayTeam": "Delta",
                            "homeScore": 2,
                            "awayScore": 0,
                            "winner": 2,
                            "drawOrder": 1,
                            "aggregatedResult": {"homeScore": 2, "awayScore": 0},
                            "matches": [
                                {"matchId": 101, "home": {"id": 2, "name": "Beta", "score": 2},
                                 "away": {"id": 4, "name": "Delta", "score": 0},
                                 "status": {"finished": True, "scoreStr": "2 - 0"}}
                            ],
                        }
                    ],
                }
            ],
            "special": [],
        },
        "stats": {
            "players": [
                {"name": "goals", "header": "Top scorer", "category": "Top Stat",
                 "order": 1, "fetchAllUrl": "https://data.fotmob.com/goals.json", "topThree": [{}, {}, {}]},
                {"name": "assists", "header": "Assists", "category": "Top Stat",
                 "order": 2, "fetchAllUrl": None},
            ],
            "teams": [
                {"name": "rating_team", "header": "FotMob rating", "category": "General",
                 "fetchAllUrl": "https://data.fotmob.com/rating-team.json"}
            ],
            "seasonStatLinks": [
                {"Name": "2025", "StageId": 0, "TemplateId": 289,
                 "TournamentId": 24309, "RelativePath": "stats/289/season/24309/topstats.json"}
            ],
        },
    }


def test_season_bundle_parses_all_contexts_once_without_collapsing_groups(tournament_payload):
    bundle = parse_season_bundle(tournament_payload, ScopeRef(289, "2025"))

    assert bundle.scope.identity == (289, "2025", None)
    assert {row["match_id"] for row in bundle.matches} == {100, 101}
    assert len([issue for issue in bundle.issues if issue.code == "duplicate_match_id"]) == 1
    assert {(row["table_name"], row["table_type"], row["team_id"]) for row in bundle.standings} == {
        ("Grp. A", "all", 1),
        ("Grp. A", "all", 2),
        ("Grp. A", "home", 1),
        ("Best 3rd placed teams", "all", 1),
    }
    alpha_all = next(
        row for row in bundle.standings
        if row["table_name"] == "Grp. A" and row["table_type"] == "all" and row["team_id"] == 1
    )
    assert alpha_all["points"] == 0
    assert alpha_all["qualification_title"] == "Qualification"
    assert {row["team_id"] for row in bundle.teams} == {1, 2, 3, 4}
    assert {row["stage_id"] for row in bundle.stages} >= {
        "fixture:1", "fixture:final", "table:895039", "table:1",
        "playoff:final", "stats:0:24309",
    }
    assert len(bundle.playoffs) == 1
    assert bundle.playoffs[0]["match_ids"] == (101,)


def test_bundle_retains_advertised_categories_capabilities_and_full_path_inventory(tournament_payload):
    tournament_payload["fixtures"]["allMatches"][1]["lateOptionalField"] = {"nested": 7}
    bundle = parse_season_bundle(tournament_payload, ScopeRef(289, "2025"))

    assert [item.name for item in bundle.player_categories] == ["goals", "assists"]
    assert bundle.player_categories[1].fetch_all_url is None
    assert [item.name for item in bundle.team_categories] == ["rating_team"]
    assert bundle.capabilities["fixtures_advertised"] is True
    assert bundle.capabilities["match_count"] == 2
    assert "$.fixtures.allMatches[].lateOptionalField.nested" in bundle.json_paths


def test_flat_table_shape_and_zero_points_are_supported():
    payload = {
        "details": {"id": 47, "name": "Premier League", "selectedSeason": "2025/2026"},
        "table": [{"data": {"leagueId": 47, "leagueName": "Premier League", "table": {
            "all": [{"id": 10, "name": "Team", "idx": 20, "pts": 0, "scoresStr": "0-10"}]
        }}}],
        "fixtures": {"allMatches": []},
    }
    bundle = parse_season_bundle(payload, ScopeRef(47, "2025/2026"))
    assert bundle.standings[0]["points"] == 0
    assert bundle.standings[0]["goals_for"] == 0
    assert bundle.standings[0]["goals_against"] == 10


def test_direct_standing_list_and_nested_fixture_info_shapes_are_supported():
    payload = {
        "details": {"id": 9, "name": "League", "selectedSeason": "2025"},
        "table": [{"id": 1, "name": "Alpha", "idx": 1, "pts": 0}],
        "fixtures": {
            "data": {
                "allMatches": [],
                "fixtureInfo": {
                    "rounds": [{"roundId": "qualification", "localizedKey": "qualification"}],
                    "teams": [{"id": 1, "name": "Alpha"}],
                },
            }
        },
    }
    bundle = parse_season_bundle(payload, ScopeRef(9, "2025"))
    assert bundle.standings[0]["team_id"] == 1
    assert bundle.standings[0]["points"] == 0
    assert {row["stage_id"] for row in bundle.stages} >= {"fixture:qualification"}
    assert {row["team_id"] for row in bundle.teams} == {1}


def test_season_bundle_rejects_silent_selected_season_fallback(tournament_payload):
    with pytest.raises(SelectedSeasonMismatch):
        parse_season_bundle(tournament_payload, ScopeRef(289, "2027"))


def test_parse_leaderboards_walks_every_toplist_and_team_name_fallback():
    payload = {
        "TopLists": [
            {"Title": "Goals", "StatName": "goals", "Category": "Attack", "StatList": [
                {"ParticiantId": "7", "ParticipantName": "Player", "TeamId": "2",
                 "TeamName": "Beta", "Rank": 1, "StatValue": 0}
            ]},
            {"Title": "Expected goals", "StatName": "xg", "Category": "Attack", "StatList": [
                {"ParticipantId": 7, "ParticipantName": "Player", "TeamId": 2,
                 "Rank": 1, "StatValue": 1.2}
            ]},
        ]
    }
    rows = parse_leaderboards(payload, participant_type="player", scope=ScopeRef(289, "2025"))
    assert [(row["stat_name"], row["stat_value"]) for row in rows] == [("goals", 0), ("xg", 1.2)]
    assert rows[0]["participant_id"] == 7

    team_rows = parse_leaderboards(
        {"TopLists": [{"StatList": [{"ParticipantName": "Alpha", "TeamId": 1}]}]},
        participant_type="team",
        descriptor=LeaderboardCategoryRef("team", "rating_team", "Rating", "General", None),
    )
    assert team_rows[0]["team_name"] == "Alpha"
    assert team_rows[0]["stat_name"] == "rating_team"


def test_transfer_pages_use_correct_fee_fields_and_deduplicate_events():
    transfer = {
        "playerId": 7,
        "name": "Player",
        "position": {"label": "LW", "key": "leftwinger_short"},
        "transferDate": "2026-05-29T11:21:16Z",
        "fromClub": {"id": 1, "name": "Alpha", "fullName": "Alpha FC"},
        "toClub": "Beta",
        "toClubFullName": "Beta FC",
        "toClubId": 2,
        "fee": {"feeText": "€1m", "localizedFeeText": "1 млн €", "value": 1_000_000},
        "marketValue": {"value": 2_000_000},
        "transferType": {"text": "Permanent", "localizationKey": "permanent"},
        "onLoan": False,
    }
    rows = parse_transfers(
        [{"page": 1, "transfers": [transfer]}, {"page": 2, "transfers": [dict(transfer)]}],
        scope=ScopeRef(47, "2025/2026"),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["fee_text"] == "€1m"
    assert row["localized_fee_text"] == "1 млн €"
    assert row["fee_value"] == 1_000_000
    assert row["market_value"] == 2_000_000
    assert row["from_club_full_name"] == "Alpha FC"
    assert len(row["transfer_event_id"]) == 64


def test_json_inventory_scans_all_array_elements_and_empty_containers():
    paths = inventory_json_paths({"items": [{"a": 1}, {"b": []}], "empty": {}})
    assert "$.items[].a" in paths
    assert "$.items[].b[]" in paths
    assert "$.empty" in paths
