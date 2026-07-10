"""Focused contracts for the transport-free WhoScored v2 primitives."""

from __future__ import annotations

import gzip

import pytest

from scrapers.whoscored.catalog import CatalogError, WhoScoredCatalog
from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope, canonical_season_id
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
    parse_preview_html,
    parse_profile_html,
    parse_schedule_json,
    parse_season_stages,
)
from scrapers.whoscored.raw_store import (
    RawObjectCorrupt,
    RawObjectNotFound,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    schedule_month_target,
)


CLUB_SCOPE = WhoScoredScope(
    "ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR
)
WORLD_CUP_SCOPE = WhoScoredScope(
    "INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR
)


@pytest.mark.unit
class TestCanonicalScopes:
    def test_explicit_formats_keep_ambiguous_values_unambiguous(self):
        assert canonical_season_id("2021", SeasonFormat.SPLIT_YEAR) == "2021"
        assert canonical_season_id("2021", SeasonFormat.SINGLE_YEAR) == "2021"
        assert WORLD_CUP_SCOPE.spec == "INT-World Cup=2026"

    def test_no_start_year_or_shift_heuristic(self):
        with pytest.raises(ValueError, match="consecutive"):
            WhoScoredScope("ENG-Premier League", "2026", SeasonFormat.SPLIT_YEAR)
        with pytest.raises(ValueError, match="calendar year"):
            WhoScoredScope("INT-World Cup", "2526", SeasonFormat.SINGLE_YEAR)
        with pytest.raises(ValueError, match="four decimal digits"):
            WhoScoredScope("ENG-Premier League", "2025-26", SeasonFormat.SPLIT_YEAR)

    def test_catalog_resolves_format_and_source_ids_from_sources_mapping(self):
        catalog = WhoScoredCatalog.from_mapping(
            {
                "competitions": [
                    {
                        "id": "ENG-Premier League",
                        "seasons": [{"id": 2526, "start": "2025-08-15"}],
                        "sources": {
                            "fallback": ["whoscored"],
                            "whoscored": {"region_id": 252, "league_id": 2},
                        },
                    },
                    {
                        "id": "INT-World Cup",
                        "seasons": [
                            {"id": 2026, "season_format": "single_year"}
                        ],
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
        parsed = extract_js_assignment(
            html, "wsCalendar", allow_date_expressions=True
        )
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
                "qualifiers": [
                    {"type": {"value": 2, "displayName": "Cross"}}
                ],
            },
            {
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
    def test_preserves_source_event_id_and_related_source_id(self):
        result = parse_matchcentre_data(
            _matchcentre(), scope=CLUB_SCOPE, game_id=1903117, game="fixture"
        )
        first = result.events.rows[0]
        assert first["source_event_id"] == 41
        assert first["related_event_id"] == 40
        assert first["team"] == "Liverpool"
        assert first["player"] == "Starter"
        assert first["qualifiers"] == (
            '[{"type":{"displayName":"Cross","value":2}}]'
        )
        assert result.events.rows[1]["player"] is None
        assert result.events.status is DatasetStatus.AVAILABLE

    def test_lineup_minutes_and_latest_rating_are_derived_without_more_io(self):
        lineups = parse_matchcentre_data(
            _matchcentre(), scope=CLUB_SCOPE, game_id=1903117
        ).lineups
        by_player = {row["player_id"]: row for row in lineups.rows}
        assert by_player[11]["minutes_played"] == 70
        assert by_player[11]["rating"] == pytest.approx(7.2)
        assert by_player[12]["minutes_played"] == 24

    def test_absent_lineups_are_not_confused_with_available_empty_lineups(self):
        data = {"events": [], "home": {}, "away": {}}
        result = parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)
        assert result.events.status is DatasetStatus.EMPTY
        assert result.lineups.status is DatasetStatus.NOT_AVAILABLE

    def test_duplicate_or_missing_source_event_id_fails_contract(self):
        data = _matchcentre()
        data["events"][1]["eventId"] = 41
        with pytest.raises(WhoScoredParseError, match="Duplicate source event"):
            parse_matchcentre_data(data, scope=CLUB_SCOPE, game_id=1)
        data["events"][1].pop("eventId")
        with pytest.raises(WhoScoredParseError, match="eventId is required"):
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
        row = parse_profile_html(html, player_id="355401").rows[0]
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
        result = parse_preview_html(
            html,
            scope=CLUB_SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )
        assert [(row["team"], row["player_id"]) for row in result.rows] == [
            ("Home", 10),
            ("Away", 20),
        ]
        empty = parse_preview_html(
            "<html><body><h1>Preview</h1></body></html>",
            scope=CLUB_SCOPE,
            game_id=1,
            game="g",
            home_team="Home",
            away_team="Away",
        )
        assert empty.status is DatasetStatus.EMPTY

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

    def test_content_addressed_round_trip_and_cache_hit(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1903117)
        calls = []

        def load(url):
            calls.append(url)
            return "<html>match</html>"

        raw, first, hit = store.get_or_fetch(
            target, load, content_type="text/html"
        )
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

    def test_targets_are_route_specific(self):
        assert match_page_target(1).target_id != preview_page_target(1).target_id
        assert schedule_month_target(22, 2026, 1).canonical_url.endswith("d=202601")

    def test_missing_and_corrupt_objects_fail_loudly(self, tmp_path):
        store = self._store(tmp_path)
        target = match_page_target(1)
        with pytest.raises(RawObjectNotFound):
            store.load_bytes(target)
        record = store.store_text(target, "<html>x</html>")
        store._write_bytes(record.blob_key, b"not-gzip")
        with pytest.raises(RawObjectCorrupt):
            store.load_bytes(target)
