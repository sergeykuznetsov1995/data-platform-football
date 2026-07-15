"""Focused contracts for the transport-free WhoScored v2 primitives."""

from __future__ import annotations

import gzip
import io
import json
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyarrow import fs

from scrapers.whoscored.catalog import CatalogError, WhoScoredCatalog
from scrapers.whoscored.domain import (
    SeasonFormat,
    WhoScoredScope,
    base_season_id,
    canonical_season_id,
    disambiguated_season_id,
    source_season_id_hint,
)
from scrapers.whoscored.parsers import (
    DatasetStatus,
    JavaScriptLiteralError,
    WhoScoredParseError,
    extract_js_assignment,
    extract_matchcentre_data,
    find_source_season_id,
    parse_calendar_months,
    parse_js_literal,
    parse_matchcentre_data,
    parse_preview_bundle,
    parse_profile_bundle,
    parse_schedule_bets,
    parse_schedule_incidents,
    parse_schedule_json,
    parse_season_stages,
)
from scrapers.whoscored.raw_store import (
    RawObjectCorrupt,
    RawObjectNotFound,
    RawObjectRecord,
    RawStoreError,
    RawTarget,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    schedule_month_target,
)


CLUB_SCOPE = WhoScoredScope("ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR)
WORLD_CUP_SCOPE = WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR)


@pytest.mark.unit
class TestCanonicalScopes:
    def test_explicit_formats_keep_ambiguous_values_unambiguous(self):
        assert canonical_season_id("2021", SeasonFormat.SPLIT_YEAR) == "2021"
        assert canonical_season_id("2021", SeasonFormat.SINGLE_YEAR) == "2021"
        assert canonical_season_id("1921", SeasonFormat.MULTI_YEAR) == "1921"
        assert WORLD_CUP_SCOPE.spec == "INT-World Cup=2026"

    def test_no_start_year_or_shift_heuristic(self):
        with pytest.raises(ValueError, match="consecutive"):
            WhoScoredScope("ENG-Premier League", "2026", SeasonFormat.SPLIT_YEAR)
        with pytest.raises(ValueError, match="calendar year"):
            WhoScoredScope("INT-World Cup", "2526", SeasonFormat.SINGLE_YEAR)
        with pytest.raises(ValueError, match="must not be an adjacent"):
            WhoScoredScope("INT-Cup", "2526", SeasonFormat.MULTI_YEAR)
        with pytest.raises(ValueError, match="four decimal digits"):
            WhoScoredScope("ENG-Premier League", "2025-26", SeasonFormat.SPLIT_YEAR)

    def test_collision_identity_is_explicit_strict_and_reversible(self):
        token = disambiguated_season_id("2021", SeasonFormat.SINGLE_YEAR, 8534)
        assert token == "2021-single-ws8534"
        assert base_season_id(token) == "2021"
        assert source_season_id_hint(token) == 8534
        assert source_season_id_hint("2021") is None
        assert WhoScoredScope("WS-11-605", token, SeasonFormat.SINGLE_YEAR).spec == (
            "WS-11-605=2021-single-ws8534"
        )
        with pytest.raises(ValueError, match="encodes 'single'"):
            canonical_season_id(token, SeasonFormat.SPLIT_YEAR)
        with pytest.raises(ValueError, match="positive integer"):
            disambiguated_season_id("2021", SeasonFormat.SINGLE_YEAR, 0)

    def test_catalog_resolves_format_and_source_ids_from_sources_mapping(self):
        catalog = WhoScoredCatalog.from_mapping(
            {
                "competitions": [
                    {
                        "id": "ENG-Premier League",
                        "seasons": [{"id": 2526, "start": "2025-08-15"}],
                        "sources": {
                            "fallback": ["whoscored"],
                            "whoscored": {
                                "region_id": 252,
                                "tournament_id": 2,
                            },
                        },
                    },
                    {
                        "id": "INT-World Cup",
                        "seasons": [{"id": 2026, "season_format": "single_year"}],
                        "sources": {"primary": ["whoscored"]},
                    },
                ]
            }
        )
        club = catalog.parse_scope_spec("ENG-Premier League=2526")
        world_cup = catalog.resolve_scope("INT-World Cup", 2026)
        assert club.scope == CLUB_SCOPE
        assert world_cup.scope == WORLD_CUP_SCOPE
        assert catalog.competition("ENG-Premier League").region_id == 252
        assert catalog.competition("ENG-Premier League").tournament_id == 2
        with pytest.raises(CatalogError, match="not configured"):
            catalog.resolve_scope("INT-World Cup", 2526)


@pytest.mark.unit
class TestEmbeddedJavaScript:
    def test_strict_json_then_json5_without_rewriting_strings(self):
        assert parse_js_literal('{"text":"a:b,{c}","value":1}') == {
            "text": "a:b,{c}",
            "value": 1,
        }
        assert parse_js_literal(
            "{/* comment */ name:'Papa John\\'s', numeric:0x10, values:[1,2,],}"
        ) == {"name": "Papa John's", "numeric": 16, "values": [1, 2]}

    def test_balanced_assignment_handles_all_quote_styles_and_comments(self):
        source = """
            const wsCalendar = {
              single: '}', double: "]", template: `a{b}`,
              /* } */ mask: {2026:{0:{1:1}}}
            };
        """
        # Backticks are scanner-safe but not JSON5 values, so the unsupported
        # expression is rejected rather than executed.
        with pytest.raises(JavaScriptLiteralError):
            extract_js_assignment(source, "wsCalendar")
        safe = source.replace("`a{b}`", "'a{b}'")
        assert extract_js_assignment(safe, "wsCalendar")["mask"] == {
            "2026": {"0": {"1": 1}}
        }

    def test_calendar_date_extension_is_targeted_and_opt_in(self):
        html = """
        <script>var wsCalendar = {
          min: (new Date(2025, 7, 15)).toString(),
          mask: {2025:{7:{15:1}}, 2026:{0:{1:1}}}
        };</script>
        """
        with pytest.raises(JavaScriptLiteralError):
            extract_js_assignment(html, "wsCalendar")
        parsed = extract_js_assignment(html, "wsCalendar", allow_date_expressions=True)
        assert parsed["min"] is None
        assert [month.token for month in parse_calendar_months(html)] == [
            "202508",
            "202601",
        ]

    def test_matchcentre_extraction_is_string_aware_and_strict(self):
        html = """
        <script>require.config.params.args = {
          matchCentreData: {"events":[{"eventId":1,"qualifiers":
            [{"value":"brace } in string"}]}],"home":{},"away":{}}
        };</script>
        """
        assert extract_matchcentre_data(html)["events"][0]["eventId"] == 1
        with pytest.raises(JavaScriptLiteralError, match="not found"):
            extract_matchcentre_data("<html>no payload</html>")


def _matchcentre() -> dict:
    return {
        "expandedMaxMinute": 94,
        "playerIdNameDictionary": {"11": "Starter"},
        "events": [
            {
                "id": 4_100_001.0,
                "eventId": 41,
                "relatedEventId": 40,
                "minute": 10,
                "second": 2,
                "expandedMinute": 10,
                "period": {"displayName": "FirstHalf"},
                "type": {"displayName": "Pass"},
                "outcomeType": {"displayName": "Successful"},
                "teamId": 26,
                "playerId": 11,
                "x": 25,
                "y": 50,
                "qualifiers": [{"type": {"value": 2, "displayName": "Cross"}}],
            },
            {
                "id": 4_100_002.0,
                "eventId": 42,
                "minute": 11,
                "teamId": 26,
                "playerId": 999,
                "type": {"displayName": "BallTouch"},
            },
        ],
        "home": {
            "teamId": 26,
            "name": "Liverpool",
            "incidentEvents": [],
            "players": [
                {
                    "playerId": 11,
                    "name": "Starter",
                    "shirtNo": 4,
                    "position": "DC",
                    "isFirstEleven": True,
                    "subbedOutExpandedMinute": 70,
                    "stats": {"ratings": {"45": 6.9, "70": 7.2}},
                },
                {
                    "playerId": 12,
                    "name": "Sub",
                    "isFirstEleven": False,
                    "subbedInExpandedMinute": 70,
                    "stats": {},
                },
            ],
        },
        "away": {"teamId": 167, "name": "Manchester City", "players": []},
    }


@pytest.mark.unit
class TestMatchParser:
    def test_live_team_local_event_id_collision_preserves_both_actions(self):
        fixture = (
            Path(__file__).resolve().parents[3]
            / "tests/fixtures/whoscored/world_cup_duplicate_team_event_ids.json"
        )
        data = json.loads(fixture.read_text(encoding="utf-8"))

        events = parse_matchcentre_data(
            data,
            scope=WORLD_CUP_SCOPE,
            game_id=data["game_id"],
        ).events.rows

        assert [row["source_event_id"] for row in events] == [
            2_944_339_597,
            2_944_339_663,
        ]
        assert [row["team_event_id"] for row in events] == [2, 2]
        assert [row["team_id"] for row in events] == [972, 485]

    def test_preserves_source_event_id_and_related_source_id(self):
        result = parse_matchcentre_data(
            _matchcentre(), scope=CLUB_SCOPE, game_id=1903117, game="fixture"
        )
        first = result.events.rows[0]
        assert first["source_event_id"] == 4_100_001
        assert first["opta_event_id"] == 4_100_001
        assert first["team_event_id"] == 41
        assert first["related_team_event_id"] == 40
        assert first["team"] == "Liverpool"
        assert first["player"] == "Starter"
        assert first["qualifiers"] == ('[{"type":{"displayName":"Cross","value":2}}]')
        assert result.events.rows[1]["player"] is None
        assert result.events.status is DatasetStatus.AVAILABLE
        assert result.matches.rows[0]["source_raw_json"] is None
        assert result.matches.rows[0]["source_schema_fingerprint"]

    def test_lineup_minutes_and_latest_rating_are_derived_without_more_io(self):
        lineups = parse_matchcentre_data(
            _matchcentre(), scope=CLUB_SCOPE, game_id=1903117
        ).lineups
        by_player = {row["player_id"]: row for row in lineups.rows}
        assert by_player[11]["minutes_played"] == 70
        assert by_player[11]["rating"] == pytest.approx(7.2)
        assert by_player[12]["minutes_played"] == 24

    def test_absent_lineups_are_not_confused_with_available_empty_lineups(self):
        data = {
            "events": [],
            "home": {"teamId": 1},
            "away": {"teamId": 2},
        }
        result = parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)
        assert result.events.status is DatasetStatus.EMPTY
        assert result.lineups.status is DatasetStatus.NOT_AVAILABLE

    def test_match_metadata_requires_both_source_team_identities(self):
        with pytest.raises(WhoScoredParseError, match="home has no team identity"):
            parse_matchcentre_data(
                {"events": [], "home": {}, "away": {"teamId": 2}},
                scope=CLUB_SCOPE,
                game_id=1,
            )
        with pytest.raises(WhoScoredParseError, match="away has no team identity"):
            parse_matchcentre_data(
                {"events": [], "home": {"teamId": 1}, "away": {}},
                scope=CLUB_SCOPE,
                game_id=1,
            )

    def test_absent_events_do_not_discard_available_match_datasets(self):
        data = _matchcentre()
        data.pop("events")

        result = parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1903117)

        assert result.events.status is DatasetStatus.NOT_AVAILABLE
        assert result.events.reason == "source_events_absent"
        assert result.matches.status is DatasetStatus.AVAILABLE
        assert result.matches.rows[0]["home_team_id"] == 26
        assert result.lineups.status is DatasetStatus.AVAILABLE
        assert result.player_match_stats.status is DatasetStatus.AVAILABLE

        extracted = extract_matchcentre_data(
            "<script>var matchCentreData = "
            "{home:{teamId:26},away:{teamId:167}};</script>"
        )
        assert extracted["home"]["teamId"] == 26
        assert "events" not in extracted

    def test_duplicate_source_event_id_gets_stable_composite_identity(self):
        data = _matchcentre()
        data["events"][1]["id"] = data["events"][0]["id"]
        events = parse_matchcentre_data(
            data, scope=CLUB_SCOPE, game_id=1
        ).events.rows

        assert len({row["source_event_id"] for row in events}) == 2
        assert {row["opta_event_id"] for row in events} == {4_100_001}

        data["events"][1].pop("id")
        with pytest.raises(WhoScoredParseError, match=r"events\[1\]\.id is required"):
            parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)

    def test_team_local_event_ids_can_repeat_only_across_teams(self):
        data = _matchcentre()
        data["events"][1]["eventId"] = data["events"][0]["eventId"]
        data["events"][1]["teamId"] = 167

        events = parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1).events.rows

        assert [row["source_event_id"] for row in events] == [4_100_001, 4_100_002]
        assert [row["team_event_id"] for row in events] == [41, 41]

        data["events"][1]["teamId"] = 26
        with pytest.raises(WhoScoredParseError, match="team-local event identity"):
            parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)

    def test_global_event_id_uses_bigint_without_float_rounding(self):
        data = _matchcentre()
        data["events"] = [data["events"][0]]
        data["events"][0]["id"] = 9_223_372_036_854_775_807

        row = parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1).events.rows[0]

        assert row["source_event_id"] == 9_223_372_036_854_775_807

        data["events"][0]["id"] = float(2**53)
        with pytest.raises(WhoScoredParseError, match="representable integer"):
            parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)


@pytest.mark.unit
class TestDocumentParsers:
    def test_profile_is_global_and_keyed_by_source_player(self):
        html = """
        <div><span class="info-label">Name: </span>Alice Example</div>
        <div><span class="info-label">Current Team: </span>
          <a class="team-link" href="/Teams/26/Show/Liverpool">Liverpool</a></div>
        <div><span class="info-label">Age: </span>25 <i>(06-06-2001)</i></div>
        <div><span class="info-label">Height: </span>180cm</div>
        <div><span class="info-label">Nationality: </span>
          <span class="iconize">England<span class="country flg-gb-eng"></span></span></div>
        <div><span class="info-label">Positions: </span>Forward, Midfielder</div>
        """
        row = parse_profile_bundle(html, player_id="355401").profiles.rows[0]
        assert row["player_id"] == 355401
        assert row["current_team_id"] == 26
        assert row["date_of_birth"] == "2001-06-06"
        assert row["height_cm"] == 180
        assert row["country_code"] == "gb-eng"
        assert "league" not in row and "season" not in row

    def test_preview_rows_are_side_aware_and_zero_is_valid(self):
        html = """
        <div id="missing-players">
          <div><table><tbody><tr>
            <td class="pn"><a href="/Players/10/Show/A">Player A</a></td>
            <td class="reason"><span title="Hamstring"></span></td>
            <td class="confirmed">Confirmed</td>
          </tr></tbody></table></div>
          <div><table><tbody><tr>
            <td class="pn"><a href="/Players/20/Show/B">Player B</a></td>
            <td class="reason">Suspended</td><td class="confirmed"></td>
          </tr></tbody></table></div>
        </div>
        """
        result = parse_preview_bundle(
            html,
            scope=CLUB_SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )
        assert [
            (row["team"], row["player_id"]) for row in result.missing_players.rows
        ] == [
            ("Home", 10),
            ("Away", 20),
        ]
        unavailable = parse_preview_bundle(
            "<html><body><h1>Preview</h1></body></html>",
            scope=CLUB_SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )
        assert all(
            dataset.status is DatasetStatus.NOT_AVAILABLE
            for dataset in unavailable.datasets.values()
        )

    def test_absent_missing_players_section_is_not_an_authoritative_empty(self):
        html = """
        <script>
          var predictedLineups = {home: {players: []}, away: {players: []}};
        </script>
        """
        result = parse_preview_bundle(
            html,
            scope=CLUB_SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )

        assert result.missing_players.status is DatasetStatus.NOT_AVAILABLE
        assert result.preview_lineups.status is DatasetStatus.EMPTY
        assert result.preview_sections.status is DatasetStatus.AVAILABLE

    def test_schedule_projects_stable_schema_and_nested_json(self):
        result = parse_schedule_json(
            {
                "tournaments": [
                    {
                        "matches": [
                            {
                                "id": 100,
                                "startTimeUtc": "2026-06-11T19:00:00Z",
                                "homeTeamName": "Mexico",
                                "awayTeamName": "South Africa",
                                "homeTeamId": 1,
                                "awayTeamId": 2,
                                "status": 6,
                                "matchIsOpta": True,
                                "hasPreview": True,
                                "incidents": [{"type": "goal", "minute": 1}],
                            }
                        ]
                    }
                ]
            },
            scope=WORLD_CUP_SCOPE,
            stage_id=999,
            stage="Group Stage",
        )
        row = result.rows[0]
        assert row["game_id"] == 100
        assert row["game"] == "2026-06-11 Mexico-South Africa"
        assert row["season"] == "2026"
        assert row["status"] == 6
        assert row["incidents"] == '[{"minute":1,"type":"goal"}]'

        incidents = parse_schedule_incidents(result)
        assert incidents.status is DatasetStatus.AVAILABLE
        assert incidents.rows == (
            {
                "league": "INT-World Cup",
                "season": "2026",
                "game_id": 100,
                "game": "2026-06-11 Mexico-South Africa",
                "stage_id": 999,
                "stage": "Group Stage",
                "match_is_opta": True,
                "entity_key": "100:incidents[0]",
                "source_ordinal": 0,
                "source_path": "incidents[0]",
                "source_incident_id": None,
                "incident_type": "goal",
                "incident_subtype": None,
                "minute": 1,
                "expanded_minute": None,
                "period": None,
                "field": None,
                "team_id": None,
                "team": None,
                "player_id": None,
                "player": None,
                "participating_player_id": None,
                "participating_player": None,
                "source_raw_json": '{"minute":1,"type":"goal"}',
                "source_schema_fingerprint": incidents.rows[0][
                    "source_schema_fingerprint"
                ],
            },
        )

    def test_non_opta_schedule_incident_preserves_source_participants(self):
        schedule = parse_schedule_json(
            {
                "tournaments": [
                    {
                        "matches": [
                            {
                                "id": 101,
                                "homeTeamName": "Home",
                                "awayTeamName": "Away",
                                "matchIsOpta": False,
                                "hasIncidentsSummary": True,
                                "incidents": [
                                    {
                                        "id": "summary-7",
                                        "type": {"displayName": "Goal"},
                                        "subType": "Penalty",
                                        "minute": 45,
                                        "period": {"displayName": "FirstHalf"},
                                        "teamId": 9,
                                        "teamName": "Home",
                                        "playerId": 77,
                                        "playerName": "Scorer",
                                        "participatingPlayerId": 88,
                                        "participatingPlayerName": "Assist",
                                    }
                                ],
                            }
                        ]
                    }
                ]
            },
            scope=WORLD_CUP_SCOPE,
            stage_id=999,
        )

        result = parse_schedule_incidents(schedule)

        assert result.status is DatasetStatus.AVAILABLE
        row = result.rows[0]
        assert row["match_is_opta"] is False
        assert row["source_incident_id"] == "summary-7"
        assert row["incident_type"] == "Goal"
        assert row["incident_subtype"] == "Penalty"
        assert row["team_id"] == 9
        assert row["team"] == "Home"
        assert row["player_id"] == 77
        assert row["player"] == "Scorer"
        assert row["participating_player_id"] == 88
        assert row["participating_player"] == "Assist"

    def test_schedule_bets_normalizes_stable_market_and_provider_ids(self):
        schedule = parse_schedule_json(
            {
                "tournaments": [
                    {
                        "matches": [
                            {
                                "id": 102,
                                "homeTeamName": "Home",
                                "awayTeamName": "Away",
                                "bets": {
                                    "home": {
                                        "betName": "Home",
                                        "betId": "market-1",
                                        "offers": [
                                            {
                                                "oddsDecimal": "2.25",
                                                "oddsFractional": "5/4",
                                                "oddsUS": "+125",
                                                "clickOutUrl": "https://book.test/bet",
                                                "bettingProvider": "B3",
                                                "providerId": 23,
                                            }
                                        ],
                                    }
                                },
                            }
                        ]
                    }
                ]
            },
            scope=WORLD_CUP_SCOPE,
            stage_id=999,
            stage="Group Stage",
        )

        result = parse_schedule_bets(schedule)

        assert result.status is DatasetStatus.AVAILABLE
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row["entity_key"] == "102:home:market-1:23"
        assert row["source_outcome"] == "home"
        assert row["source_bet_id"] == "market-1"
        assert row["provider_id"] == 23
        assert row["betting_provider"] == "B3"
        assert row["odds_decimal"] == 2.25
        assert row["odds_fractional"] == "5/4"
        assert row["odds_us"] == "+125"
        assert row["clickout_url"] == "https://book.test/bet"
        assert row["source_path"] == "bets.home.offers[0]"
        assert '"bet_id":"market-1"' in row["source_raw_json"]

    def test_schedule_without_bets_is_authoritative_empty(self):
        schedule = parse_schedule_json(
            {
                "tournaments": [
                    {
                        "matches": [
                            {
                                "id": 103,
                                "homeTeamName": "Home",
                                "awayTeamName": "Away",
                            }
                        ]
                    }
                ]
            },
            scope=WORLD_CUP_SCOPE,
            stage_id=999,
        )

        result = parse_schedule_bets(schedule)

        assert result.status is DatasetStatus.EMPTY
        assert result.rows == ()

    def test_season_and_stage_discovery_use_explicit_scope_format(self):
        seasons = """
        <select id="seasons"><option value="/Regions/247/Tournaments/36/Seasons/9001">
          2026</option></select>
        """
        assert find_source_season_id(seasons, WORLD_CUP_SCOPE) == 9001
        stages = """
        <a href="/Regions/247/Tournaments/36/Seasons/9001/Stages/700">Fixtures</a>
        <select id="stages"><option value="/Regions/247/Tournaments/36/Seasons/9001/Stages/700">
          Group Stage</option><option value="/Regions/247/Tournaments/36/Seasons/9001/Stages/701">
          Final Stage</option></select>
        """
        parsed = parse_season_stages(
            stages,
            scope=WORLD_CUP_SCOPE,
            region_id=247,
            tournament_id=36,
            source_season_id=9001,
        )
        assert [(row["stage_id"], row["stage"]) for row in parsed.rows] == [
            (700, "Group Stage"),
            (701, "Final Stage"),
        ]


@pytest.mark.unit
class TestRawStore:
    def _store(self, tmp_path) -> WhoScoredRawStore:
        return WhoScoredRawStore.from_uri(tmp_path.as_uri())

    def test_s3_store_prefers_dedicated_whoscored_credentials(
        self, monkeypatch
    ):
        captured = {}
        sentinel = object()

        def filesystem(**kwargs):
            captured.update(kwargs)
            return sentinel

        monkeypatch.setattr("scrapers.whoscored.raw_store.fs.S3FileSystem", filesystem)
        monkeypatch.setenv("S3_ACCESS_KEY", "platform-access")
        monkeypatch.setenv("S3_SECRET_KEY", "platform-secret")
        monkeypatch.setenv("WHOSCORED_RAW_S3_ACCESS_KEY", "whoscored-access")
        monkeypatch.setenv("WHOSCORED_RAW_S3_SECRET_KEY", "whoscored-secret")

        store = WhoScoredRawStore.from_uri("s3://warehouse/raw/whoscored")

        assert store.filesystem is sentinel
        assert captured["access_key"] == "whoscored-access"
        assert captured["secret_key"] == "whoscored-secret"

    @pytest.mark.parametrize(
        "name,value",
        [
            ("WHOSCORED_RAW_S3_ACCESS_KEY", "dedicated-only"),
            ("WHOSCORED_RAW_S3_SECRET_KEY", "dedicated-only"),
            ("S3_ACCESS_KEY", "platform-only"),
            ("S3_SECRET_KEY", "platform-only"),
        ],
    )
    def test_s3_store_rejects_partial_credential_pairs(
        self, monkeypatch, name, value
    ):
        for variable in (
            "WHOSCORED_RAW_S3_ACCESS_KEY",
            "WHOSCORED_RAW_S3_SECRET_KEY",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
        ):
            monkeypatch.delenv(variable, raising=False)
        monkeypatch.setenv(name, value)

        with pytest.raises(RawStoreError, match="must be set together"):
            WhoScoredRawStore.from_uri("s3://warehouse/raw/whoscored")

    def test_content_addressed_round_trip_and_cache_hit(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903117)
        calls = []

        def load(url):
            calls.append(url)
            return "<html>match</html>"

        raw, first, hit = store.get_or_fetch(target, load, content_type="text/html")
        cached, second, second_hit = store.get_or_fetch(
            target,
            lambda _: pytest.fail("loader called on raw cache hit"),
            content_type="text/html",
        )
        assert raw == cached == b"<html>match</html>"
        assert hit is False and second_hit is True
        assert first.content_hash == second.content_hash
        assert calls == ["https://www.whoscored.com/Matches/1903117/Live"]
        assert gzip.decompress(store._read_bytes(first.blob_key)) == raw

    def test_concurrent_identical_target_fetches_source_once(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903118)
        calls = 0
        calls_lock = threading.Lock()
        both_ready = threading.Barrier(2)

        def loader(_url):
            nonlocal calls
            with calls_lock:
                calls += 1
            time.sleep(0.05)
            return b"<html>singleflight</html>"

        def fetch():
            both_ready.wait(timeout=1)
            return store.get_or_fetch(target, loader, content_type="text/html")

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [
                future.result()
                for future in (executor.submit(fetch), executor.submit(fetch))
            ]

        assert calls == 1
        assert sorted(result[2] for result in results) == [False, True]
        assert {result[0] for result in results} == {b"<html>singleflight</html>"}

    def test_new_commit_is_append_only_and_never_calls_move(self, tmp_path):
        class NoMoveFilesystem:
            def __init__(self, delegate):
                self.delegate = delegate

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def move(self, *_args, **_kwargs):
                pytest.fail("append-only raw commits must not call move")

        store = WhoScoredRawStore(
            NoMoveFilesystem(fs.LocalFileSystem()), str(tmp_path)
        )
        target = match_page_target(1903120)
        record = store.store_text(target, "<html>append-only</html>")

        assert not store._exists(store._target_manifest_key(target))
        receipt_key = store._target_receipt_key(target, asdict(record))
        assert store._exists(receipt_key)
        assert store.load_text(target)[0] == "<html>append-only</html>"

    def test_delayed_older_writer_cannot_replace_newer_receipt(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903121)
        newest = store.store_text(
            target,
            "<html>new</html>",
            fetched_at="2026-07-14T08:00:00+00:00",
        )
        older = store.store_text(
            target,
            "<html>old</html>",
            fetched_at="2026-07-14T07:00:00+00:00",
        )

        body, loaded = store.load_text(target)

        assert body == "<html>new</html>"
        assert loaded == newest
        assert loaded != older

    def test_same_commit_is_idempotent_and_creates_one_receipt(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903122)
        kwargs = {"fetched_at": "2026-07-14T08:00:00+00:00"}

        first = store.store_text(target, "<html>same</html>", **kwargs)
        second = store.store_text(target, "<html>same</html>", **kwargs)

        assert first == second
        receipt_root = tmp_path / store._target_receipt_prefix(target)
        assert len(list(receipt_root.glob("*.json"))) == 1

    def test_transient_direct_put_is_retried_and_verified(
        self, tmp_path, monkeypatch
    ):
        class TransientWriteFilesystem:
            def __init__(self, delegate, failures):
                self.delegate = delegate
                self.failures = failures

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def open_output_stream(self, *args, **kwargs):
                if self.failures:
                    self.failures -= 1
                    raise OSError("temporary object-store outage")
                return self.delegate.open_output_stream(*args, **kwargs)

        filesystem = TransientWriteFilesystem(fs.LocalFileSystem(), failures=2)
        store = WhoScoredRawStore(filesystem, str(tmp_path))
        monkeypatch.setenv("WHOSCORED_RAW_RETRY_BASE_SECONDS", "0")

        record = store.store_text(match_page_target(1903123), "<html>ok</html>")

        assert filesystem.failures == 0
        assert gzip.decompress(store._read_bytes(record.blob_key)) == b"<html>ok</html>"

    def test_read_after_write_mismatch_retries_full_put(self, tmp_path, monkeypatch):
        class StaleReadFilesystem:
            def __init__(self, delegate):
                self.delegate = delegate
                self.stale_reads = 1

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def open_input_file(self, *args, **kwargs):
                if self.stale_reads:
                    self.stale_reads -= 1
                    return io.BytesIO(b"partial-object")
                return self.delegate.open_input_file(*args, **kwargs)

        filesystem = StaleReadFilesystem(fs.LocalFileSystem())
        store = WhoScoredRawStore(filesystem, str(tmp_path))
        monkeypatch.setenv("WHOSCORED_RAW_RETRY_BASE_SECONDS", "0")

        record = store.store_text(match_page_target(1903126), "<html>ok</html>")

        assert filesystem.stale_reads == 0
        assert store.load_text(match_page_target(1903126))[1] == record

    def test_blob_survives_receipt_failure_and_retry_resumes_without_refetch(
        self, tmp_path, monkeypatch
    ):
        class ReceiptFailureFilesystem:
            def __init__(self, delegate):
                self.delegate = delegate
                self.fail_receipt = True
                self.blob_writes = 0

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def open_output_stream(self, path, *args, **kwargs):
                if path.endswith(".raw.gz"):
                    self.blob_writes += 1
                if self.fail_receipt and "target-history-v2" in path:
                    raise OSError("crash before receipt publish")
                return self.delegate.open_output_stream(path, *args, **kwargs)

        filesystem = ReceiptFailureFilesystem(fs.LocalFileSystem())
        store = WhoScoredRawStore(filesystem, str(tmp_path))
        target = match_page_target(1903127)
        monkeypatch.setenv("WHOSCORED_RAW_IO_ATTEMPTS", "1")

        with pytest.raises(RawStoreError, match="write/verify failed"):
            store.store_text(target, "<html>resume</html>")

        assert filesystem.blob_writes == 1
        assert not store.has(target)
        filesystem.fail_receipt = False

        record = store.store_text(target, "<html>resume</html>")

        assert filesystem.blob_writes == 1
        assert store.load_text(target)[1] == record

    def test_corrupt_newer_receipt_falls_back_to_last_valid_version(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903124)
        older = store.store_text(
            target,
            "<html>valid</html>",
            fetched_at="2026-07-14T07:00:00+00:00",
        )
        newer = store.store_text(
            target,
            "<html>corrupt-receipt</html>",
            fetched_at="2026-07-14T08:00:00+00:00",
        )
        store._write_bytes(
            store._target_receipt_key(target, asdict(newer)), b"{not-json"
        )

        body, loaded = store.load_text(target)

        assert body == "<html>valid</html>"
        assert loaded == older

    def test_legacy_mutable_manifest_remains_readable(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903125)
        record = store.store_text(
            target,
            "<html>legacy</html>",
            fetched_at="2026-07-14T08:00:00+00:00",
        )
        receipt_key = store._target_receipt_key(target, asdict(record))
        store.filesystem.delete_file(store._path(receipt_key))
        store._write_json(store._target_manifest_key(target), asdict(record))

        assert store.load_text(target) == ("<html>legacy</html>", record)
        assert (
            store.quarantine(target, reason="legacy replay", record=record) is not None
        )
        assert not store.has(target)

    def test_corrupt_v2_receipt_falls_back_to_valid_legacy_manifest(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903130)
        record = store.store_text(target, "<html>legacy-fallback</html>")
        receipt_key = store._target_receipt_key(target, asdict(record))
        store._write_json(store._target_manifest_key(target), asdict(record))
        store._write_bytes(receipt_key, b"{not-json")

        assert store.load_text(target) == ("<html>legacy-fallback</html>", record)

    def test_missing_blob_v2_receipt_falls_back_to_valid_legacy_manifest(
        self, tmp_path
    ):
        store = self._store(tmp_path)
        target = match_page_target(1903131)
        legacy = store.store_text(
            target,
            "<html>legacy-fallback</html>",
            fetched_at="2026-07-14T07:00:00+00:00",
        )
        legacy_receipt = store._target_receipt_key(target, asdict(legacy))
        store.filesystem.delete_file(store._path(legacy_receipt))
        store._write_json(store._target_manifest_key(target), asdict(legacy))
        missing = {
            **asdict(legacy),
            "content_hash": "f" * 64,
            "blob_key": store._blob_key("f" * 64),
            "fetched_at": "2026-07-14T08:00:00+00:00",
        }
        missing_receipt = store._target_receipt_key(target, missing)
        store._write_json(missing_receipt, store._receipt_payload(missing))

        assert store.load_text(target) == ("<html>legacy-fallback</html>", legacy)

    def test_newest_observation_wins_across_legacy_and_v2_layouts(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903132)
        older_v2 = store.store_text(
            target,
            "<html>older-v2</html>",
            fetched_at="2026-07-14T07:00:00+00:00",
        )
        newer_legacy = store.store_text(
            target,
            "<html>newer-legacy</html>",
            fetched_at="2026-07-14T08:00:00+00:00",
        )
        newer_receipt = store._target_receipt_key(target, asdict(newer_legacy))
        store.filesystem.delete_file(store._path(newer_receipt))
        store._write_json(store._target_manifest_key(target), asdict(newer_legacy))

        assert store.load_text(target) == ("<html>newer-legacy</html>", newer_legacy)

        newest_v2 = store.store_text(
            target,
            "<html>newest-v2</html>",
            fetched_at="2026-07-14T09:00:00+00:00",
        )
        assert store.load_text(target) == ("<html>newest-v2</html>", newest_v2)
        assert older_v2 != newest_v2

    def test_malformed_legacy_timestamp_cannot_poison_valid_v2(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903134)
        valid = store.store_text(target, "<html>valid-v2</html>")
        store._write_json(
            store._target_manifest_key(target),
            {**asdict(valid), "fetched_at": "not-a-timestamp"},
        )

        assert store.load_text(target) == ("<html>valid-v2</html>", valid)

    @pytest.mark.parametrize(
        "fetched_at",
        ["0999-01-01T00:00:00+00:00", "2026-01-01T00:00:00.1234567+00:00"],
    )
    def test_receipt_order_rejects_non_fixed_width_timestamps(
        self, tmp_path, fetched_at
    ):
        store = self._store(tmp_path)

        with pytest.raises(RawStoreError, match="fetched_at"):
            store.store_text(
                match_page_target(1903133),
                "<html>x</html>",
                fetched_at=fetched_at,
            )

    def test_quarantine_invalidates_only_the_record_that_failed(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903128)
        failed = store.store_text(
            target,
            "<html>failed</html>",
            fetched_at="2026-07-14T07:00:00+00:00",
        )
        replacement = store.store_text(
            target,
            "<html>healthy</html>",
            fetched_at="2026-07-14T08:00:00+00:00",
        )

        store.quarantine(target, reason="late validation failure", record=failed)

        assert store.load_text(target) == ("<html>healthy</html>", replacement)
        assert store._exists(
            store._target_invalidation_key(target, store._record_sha256(failed))
        )
        assert not store._exists(
            store._target_invalidation_key(target, store._record_sha256(replacement))
        )

    def test_evidence_only_quarantine_cannot_invalidate_concurrent_writer(
        self, tmp_path
    ):
        store = self._store(tmp_path)
        target = match_page_target(1903129)
        replacement = store.store_text(target, "<html>healthy</html>")

        assert store.quarantine(target, reason="malformed superseded receipt")

        assert store.load_text(target) == ("<html>healthy</html>", replacement)

    def test_invalidated_observation_cannot_be_reported_as_repaired(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903135)
        observed_at = "2026-07-14T07:00:00+00:00"
        failed = store.store_text(
            target,
            "<html>failed</html>",
            fetched_at=observed_at,
        )
        store.quarantine(target, reason="invalid", record=failed)

        with pytest.raises(RawStoreError, match="invalidated raw observation"):
            store.store_text(
                target,
                "<html>failed</html>",
                fetched_at=observed_at,
            )

        repaired = store.store_text(target, "<html>failed</html>")
        assert store.load_text(target)[1] == repaired

    def test_store_fails_if_observation_is_invalidated_during_receipt_commit(
        self, tmp_path, monkeypatch
    ):
        store = self._store(tmp_path)
        target = match_page_target(1903136)
        original = store._write_immutable_json

        def write_with_invalidation(relative, payload):
            original(relative, payload)
            if relative.startswith(store._target_receipt_prefix(target)):
                record = RawObjectRecord(**payload["record"])
                record_sha = store._record_sha256(record)
                original(
                    store._target_invalidation_key(target, record_sha),
                    {
                        "invalidation_version": "whoscored-target-invalidation-v1",
                        "record_sha256": record_sha,
                        "target_id": target.target_id,
                        "fetched_at": record.fetched_at,
                        "content_hash": record.content_hash,
                    },
                )

        monkeypatch.setattr(store, "_write_immutable_json", write_with_invalidation)

        with pytest.raises(RawStoreError, match="invalidated while"):
            store.store_text(target, "<html>raced</html>")
        assert not store.has(target)

    @pytest.mark.parametrize("timeout", ["nan", "inf", "-inf"])
    def test_target_lock_rejects_non_finite_timeout_before_waiting(
        self, tmp_path, monkeypatch, timeout
    ):
        store = self._store(tmp_path)
        target = match_page_target(1903119)
        monkeypatch.setenv("WHOSCORED_RAW_LOCK_TIMEOUT_SECONDS", timeout)

        with pytest.raises(ValueError, match="finite non-negative"):
            with store.target_lock(target):
                pytest.fail("invalid timeout must fail before acquiring the lock")

    def test_targets_are_route_specific(self):
        assert match_page_target(1).target_id != preview_page_target(1).target_id
        assert schedule_month_target(22, 2026, 1).canonical_url.endswith("d=202601")

    def test_raw_target_manifest_binds_url_kind_and_source_ids(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1)
        store.store_text(target, "<html>x</html>")
        changed = RawTarget(
            source="whoscored",
            page_kind=target.page_kind,
            target_id=target.target_id,
            canonical_url="https://www.whoscored.com/Matches/2/Live",
            source_ids=target.source_ids,
        )

        with pytest.raises(RawObjectCorrupt, match="Target mismatch"):
            store.load_bytes(changed)

    def test_missing_and_corrupt_objects_fail_loudly(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1)
        with pytest.raises(RawObjectNotFound):
            store.load_bytes(target)
        assert store.quarantine(target, reason="missing") is None
        record = store.store_text(target, "<html>x</html>")
        store._write_bytes(record.blob_key, b"not-gzip")
        with pytest.raises(RawObjectCorrupt):
            store.load_bytes(target)
        assert not store.is_fresh(target, max_age=timedelta(hours=6))

    def test_freshness_normalizes_timezone_offsets_and_expires_once(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(2)
        store.store_text(
            target,
            "<html>x</html>",
            fetched_at="2026-07-11T08:00:00+02:00",
        )

        assert store.is_fresh(
            target,
            max_age=timedelta(hours=1),
            now=datetime(2026, 7, 11, 6, 30, tzinfo=timezone.utc),
        )
        assert not store.is_fresh(
            target,
            max_age=timedelta(hours=1),
            now=datetime(
                2026,
                7,
                11,
                9,
                1,
                tzinfo=timezone(timedelta(hours=2)),
            ),
        )
        with pytest.raises(ValueError, match="timezone-aware"):
            store.is_fresh(
                target,
                max_age=timedelta(hours=1),
                now=datetime(2026, 7, 11, 6, 30),
            )

    def test_freshness_reads_manifest_without_double_loading_blob(
        self, tmp_path, monkeypatch
    ):
        store = self._store(tmp_path)
        target = match_page_target(22)
        store.store_text(
            target,
            "<html>x</html>",
            fetched_at="2026-07-11T06:00:00+00:00",
        )
        monkeypatch.setattr(
            store,
            "load_bytes",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("freshness must not load the raw blob")
            ),
        )

        assert store.is_fresh(
            target,
            max_age=timedelta(hours=1),
            now=datetime(2026, 7, 11, 6, 30, tzinfo=timezone.utc),
        )

    def test_freshness_fails_closed_for_corrupt_manifest_metadata(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(3)
        record = store.store_text(target, "<html>x</html>")
        receipt_key = store._target_receipt_key(target, asdict(record))
        malformed_record = {**asdict(record), "fetched_at": "not-a-timestamp"}
        store._write_json(
            receipt_key,
            store._receipt_payload(malformed_record),
        )

        assert not store.is_fresh(target, max_age=timedelta(hours=6))

        store._write_bytes(receipt_key, b"{not-json")
        assert not store.is_fresh(target, max_age=timedelta(hours=6))

        repaired = store.store_text(target, "<html>x</html>")
        repaired_key = store._target_receipt_key(target, asdict(repaired))
        escaped_record = {**asdict(repaired), "blob_key": "../outside.raw.gz"}
        store._write_json(
            repaired_key,
            store._receipt_payload(escaped_record),
        )
        assert not store.is_fresh(target, max_age=timedelta(hours=6))
        with pytest.raises(RawObjectCorrupt):
            store.load_bytes(target)
        quarantine_root = tmp_path / "quarantine" / "targets" / "match"
        assert list(quarantine_root.rglob("*.json"))

    def test_corrupt_blob_is_quarantined_refetched_and_repaired(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(4)
        record = store.store_text(target, "<html>old</html>")
        store._write_bytes(record.blob_key, b"not-gzip")
        calls = []

        payload, repaired, cache_hit = store.get_or_fetch(
            target,
            lambda url: calls.append(url) or "<html>old</html>",
            content_type="text/html",
        )

        assert cache_hit is False
        assert calls == [target.canonical_url]
        assert payload == b"<html>old</html>"
        assert gzip.decompress(store._read_bytes(repaired.blob_key)) == payload
        assert list((tmp_path / "quarantine").rglob("*.json"))

    def test_malformed_manifest_is_quarantined_and_refetched(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(5)
        record = store.store_text(target, "<html>old</html>")
        receipt_key = store._target_receipt_key(target, asdict(record))
        malformed_record = {**asdict(record), "source_ids": None}
        store._write_json(
            receipt_key,
            store._receipt_payload(malformed_record),
        )
        calls = []

        payload, repaired, cache_hit = store.get_or_fetch(
            target,
            lambda url: calls.append(url) or "<html>fresh</html>",
            content_type="text/html",
        )

        assert cache_hit is False
        assert calls == [target.canonical_url]
        assert payload == b"<html>fresh</html>"
        assert dict(repaired.source_ids) == dict(target.source_ids)
        assert list((tmp_path / "quarantine").rglob("*.json"))

    def test_corrupt_deflate_blob_is_quarantined_refetched_and_repaired(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(6)
        source = b"a" * 1000
        record = store.store_bytes(target, source, content_type="text/html")
        encoded = bytearray(store._read_bytes(record.blob_key))
        encoded[10] ^= 0xFF
        with pytest.raises(zlib.error):
            gzip.decompress(encoded)
        store._write_bytes(record.blob_key, bytes(encoded))
        calls = []

        payload, repaired, cache_hit = store.get_or_fetch(
            target,
            lambda url: calls.append(url) or source,
            content_type="text/html",
        )

        assert cache_hit is False
        assert calls == [target.canonical_url]
        assert payload == source
        assert gzip.decompress(store._read_bytes(repaired.blob_key)) == source
        assert list((tmp_path / "quarantine").rglob("*.json"))
