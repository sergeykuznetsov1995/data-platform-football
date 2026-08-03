"""Frozen, network-free contracts for ESPN native offline parsing."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
from datetime import date, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.espn import summary_parser as summary_parser_module
from scrapers.espn.models import (
    AgeClass,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
    LegacyAliases,
)
from scrapers.espn.parsers import (
    EntityParseState,
    EspnParseError,
    parse_competition_detail_bytes,
    parse_scoreboard_calendar,
    parse_scoreboards,
    parse_soccer_dropdown_bytes,
    parse_summary,
)


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _raw(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _scope(
    *,
    espn_id: int = 730,
    slug: str = "ita.1",
    year: int = 2020,
    start: date = date(2020, 8, 1),
    end: date = date(2021, 7, 31),
    lineup: CapabilityState = CapabilityState.PROVEN,
    matchsheet: CapabilityState = CapabilityState.PROVEN,
) -> tuple[Competition, Edition]:
    edition = Edition(
        year,
        f"{year} test edition",
        start,
        end,
        True,
        EntityCapabilities(CapabilityState.PROVEN, lineup, matchsheet),
    )
    competition = Competition(
        espn_id,
        slug,
        "Test Competition",
        Gender.MALE,
        AgeClass.SENIOR,
        True,
        (edition,),
        ("detail.gender=MALE",),
        ("manual: senior",),
        LegacyAliases("LEG-Test", (), {year: (str(year),)}),
    )
    return competition, edition


def _schedule(payload: dict | None = None, **scope_kwargs):
    competition, edition = _scope(**scope_kwargs)
    rows = parse_scoreboards(
        [_raw(payload or _load("native_scoreboard.json"))],
        competition=competition,
        edition=edition,
        query_start=edition.start_date,
        query_end=edition.end_date,
    )
    return competition, edition, rows


@pytest.mark.unit
def test_catalog_facade_accepts_raw_bytes_only_and_retains_native_identity() -> None:
    dropdown = _raw({"leagues": [{"id": "730", "slug": "ita.1", "name": "Serie A"}]})
    detail = _raw(
        {
            "id": "730",
            "slug": "ita.1",
            "name": "Serie A",
            "gender": "MALE",
            "genderEvidence": "reviewed",
            "season": {
                "year": 2020,
                "displayName": "2020-21",
                "startDate": "2020-08-01T00:00Z",
                "endDate": "2021-07-31T23:59Z",
            },
            "capabilities": {
                "schedule": "proven",
                "lineup": "partial",
                "matchsheet": "proven",
            },
        }
    )

    assert parse_soccer_dropdown_bytes(dropdown)[0].espn_id == 730
    assert parse_competition_detail_bytes(detail).source_season_year == 2020
    with pytest.raises(TypeError, match="raw payload must be bytes"):
        parse_soccer_dropdown_bytes(json.loads(dropdown))  # type: ignore[arg-type]


@pytest.mark.unit
def test_dropdown_raw_bytes_facade_accepts_the_html_contract_from_discovery() -> None:
    html = b"""<html><script>window['__espnfitt__']={"navigation":{"leagueTeams":{"groups":[{"name":"Europe","columns":[{"teams":[{"n":"Serie A","id":"730","lk":[{"u":"/soccer/league/_/name/ita.1"}]}]}]}]}}};</script></html>"""

    rows = parse_soccer_dropdown_bytes(html)

    assert [(row.espn_id, row.slug, row.name) for row in rows] == [
        (730, "ita.1", "Serie A")
    ]


@pytest.mark.unit
def test_schedule_normalizes_native_ids_status_scores_and_legacy_fields() -> None:
    _, _, rows = _schedule()

    assert len(rows) == 1
    row = rows[0]
    assert (row.competition_id, row.event_id) == (730, 401000001)
    assert (row.home_team_id, row.away_team_id) == (10, 20)
    assert (row.home_score, row.away_score) == (2, 1)
    assert row.kickoff.tzinfo is timezone.utc
    assert row.status == "STATUS_FULL_TIME"
    assert row.terminal and row.played_final and not row.terminal_nonplayed
    assert (row.venue_id, row.venue, row.attendance_value) == (
        99,
        "Native Ground",
        1000,
    )
    assert row.attendance == "1000"
    assert (row.home_goals, row.away_goals) == ("2", "1")
    assert (row.league, row.season, row.game_id) == (
        "LEG-Test",
        "2020",
        401000001,
    )
    assert '"optionalCompetitionField":"kept"' in row.extra_json
    assert '"optionalEventField":{"a":1,"b":2}' in row.extra_json
    assert '"abbreviation":"HOM"' in row.extra_json


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status", "terminal", "played", "nonplayed"),
    [
        ("STATUS_SCHEDULED", False, False, False),
        ("STATUS_IN_PROGRESS", False, False, False),
        ("STATUS_FULL_TIME", True, True, False),
        ("STATUS_POSTPONED", False, False, False),
        ("STATUS_SUSPENDED", False, False, False),
        ("STATUS_CANCELED", True, False, True),
    ],
)
def test_versioned_status_map_separates_final_from_terminal_nonplayed(
    status: str, terminal: bool, played: bool, nonplayed: bool
) -> None:
    payload = _load("native_scoreboard.json")
    event = payload["events"][0]
    event["status"]["type"]["name"] = status
    if not played:
        for competitor in event["competitions"][0]["competitors"]:
            competitor.pop("score", None)

    _, _, rows = _schedule(payload)

    assert (rows[0].terminal, rows[0].played_final, rows[0].terminal_nonplayed) == (
        terminal,
        played,
        nonplayed,
    )
    assert rows[0].summary_required is played


@pytest.mark.unit
def test_postponed_native_event_can_transition_to_rescheduled_final() -> None:
    postponed = _load("native_scoreboard.json")
    postponed_event = postponed["events"][0]
    postponed_event["status"]["type"]["name"] = "STATUS_POSTPONED"
    for side in postponed_event["competitions"][0]["competitors"]:
        side.pop("score")
    _, _, postponed_rows = _schedule(postponed)

    final = _load("native_scoreboard.json")
    final["events"][0]["date"] = "2020-10-19T18:45Z"
    _, _, final_rows = _schedule(final)

    assert postponed_rows[0].event_id == final_rows[0].event_id
    assert not postponed_rows[0].terminal
    assert not postponed_rows[0].terminal_nonplayed
    assert final_rows[0].terminal
    assert final_rows[0].played_final


@pytest.mark.unit
def test_unknown_status_and_required_schema_drift_fail_closed() -> None:
    payload = _load("native_scoreboard.json")
    payload["events"][0]["status"]["type"]["name"] = "STATUS_NEW_FROM_UPSTREAM"
    with pytest.raises(EspnParseError, match="unknown ESPN status"):
        _schedule(payload)

    payload = _load("native_scoreboard.json")
    payload["events"][0]["competitions"][0]["competitors"] = "drift"
    with pytest.raises(EspnParseError, match="competitors"):
        _schedule(payload)

    with pytest.raises(EspnParseError, match="valid JSON"):
        parse_scoreboards(
            [b"{"],
            competition=_scope()[0],
            edition=_scope()[1],
            query_start=date(2020, 8, 1),
            query_end=date(2021, 7, 31),
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("leagues", "message"),
    [
        (None, "scoreboard.leagues"),
        ("drift", "scoreboard.leagues"),
        ([], "exactly one root league"),
        ([{"id": "740", "slug": "esp.1"}], "promoted league"),
        (
            [
                {"id": "730", "slug": "ita.1"},
                {"id": "730", "slug": "ita.1"},
            ],
            "exactly one root league",
        ),
        (
            [
                {"id": "730", "slug": "ita.1"},
                {"id": "740", "slug": "esp.1"},
            ],
            "exactly one root league",
        ),
        (
            [
                {"id": "740", "slug": "esp.1"},
                {"id": "730", "slug": "ita.1"},
            ],
            "exactly one root league",
        ),
        ([{"id": "730", "slug": "wrong.slug"}], "slug"),
    ],
)
def test_every_scoreboard_document_is_bound_to_one_promoted_root_league(
    leagues: object, message: str
) -> None:
    payload = _load("native_scoreboard.json")
    if leagues is None:
        payload.pop("leagues")
    else:
        payload["leagues"] = leagues

    with pytest.raises(EspnParseError, match=message):
        _schedule(payload)


@pytest.mark.unit
def test_schedule_joins_sides_by_home_away_and_deduplicates_native_event_id() -> None:
    payload = _load("native_scoreboard.json")
    payload["events"][0]["competitions"][0]["competitors"].reverse()
    duplicate = deepcopy(payload)
    _, _, rows = _schedule(payload)
    competition, edition = _scope()
    doubled = parse_scoreboards(
        [_raw(payload), _raw(duplicate)],
        competition=competition,
        edition=edition,
        query_start=edition.start_date,
        query_end=edition.end_date,
    )

    assert (rows[0].home_team_id, rows[0].away_team_id) == (10, 20)
    assert len(doubled) == 1

    duplicate["events"][0]["date"] = "2020-09-20T18:45Z"
    with pytest.raises(EspnParseError, match="conflicting duplicate event_id"):
        parse_scoreboards(
            [_raw(payload), _raw(duplicate)],
            competition=competition,
            edition=edition,
            query_start=edition.start_date,
            query_end=edition.end_date,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("espn_id", "slug", "year", "start", "end", "kickoff"),
    [
        (730, "ita.1", 2020, date(2020, 8, 1), date(2021, 7, 31), "2020-09-19T18:45Z"),
        (740, "esp.1", 2020, date(2020, 8, 1), date(2021, 7, 31), "2020-09-12T18:45Z"),
        (740, "esp.1", 2023, date(2023, 6, 1), date(2024, 6, 30), "2023-08-12T18:45Z"),
        (
            3908,
            "caf.nations",
            2025,
            date(2025, 12, 21),
            date(2026, 12, 31),
            "2026-01-18T18:45Z",
        ),
    ],
)
def test_event_season_identity_and_exact_edition_window_remove_contamination(
    espn_id: int,
    slug: str,
    year: int,
    start: date,
    end: date,
    kickoff: str,
) -> None:
    payload = _load("native_scoreboard.json")
    good = payload["events"][0]
    good["season"]["year"] = year
    good["date"] = kickoff
    contaminant = deepcopy(good)
    contaminant["id"] = "401000002"
    contaminant["season"]["year"] = year - 1
    payload["events"] = [contaminant, good]
    payload["leagues"][0].update(id=str(espn_id), slug=slug)

    _, _, rows = _schedule(
        payload,
        espn_id=espn_id,
        slug=slug,
        year=year,
        start=start,
        end=end,
    )

    assert [row.event_id for row in rows] == [401000001]


@pytest.mark.unit
def test_world_cup_stage_ranges_expand_without_outer_shell_or_heuristic() -> None:
    competition, edition = _scope(
        espn_id=606,
        slug="fifa.world",
        year=2022,
        start=date(2022, 11, 20),
        end=date(2022, 12, 18),
    )
    payload = {
        "leagues": [
            {
                "id": "606",
                "calendar": [
                    {
                        "startDate": "2022-01-01T00:00Z",
                        "endDate": "2022-12-31T23:59Z",
                        "entries": [
                            {
                                "startDate": "2022-11-20T00:00Z",
                                "endDate": "2022-11-22T23:59Z",
                            },
                            {
                                "startDate": "2022-12-18T00:00Z",
                                "endDate": "2022-12-18T23:59Z",
                            },
                        ],
                    }
                ],
            }
        ],
        "events": [],
    }

    days = parse_scoreboard_calendar(_raw(payload), competition, edition)

    assert days == (
        date(2022, 11, 20),
        date(2022, 11, 21),
        date(2022, 11, 22),
        date(2022, 12, 18),
    )


@pytest.mark.unit
def test_summary_is_parsed_once_and_joins_reordered_sections_by_native_team_id() -> (
    None
):
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["header"]["competitions"][0]["competitors"].reverse()
    payload["rosters"].reverse()
    assert "form" not in payload["boxscore"]

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup_state is EntityParseState.CAPTURED
    assert result.matchsheet_state is EntityParseState.CAPTURED
    assert {(row.team_id, row.home_away, row.athlete_id) for row in result.lineup} == {
        (10, "home", 101),
        (20, "away", 201),
    }
    assert [(row.team_id, row.home_away) for row in result.matchsheet] == [
        (10, "home"),
        (20, "away"),
    ]
    assert result.lineup[0].jersey is not None
    assert result.lineup[0].is_home
    assert result.lineup[0].position == "Midfielder"
    assert result.lineup[0].formation_place == "7"
    assert result.lineup[0].sub_in is None
    assert result.lineup[0].sub_out is None
    assert result.lineup[0].statistics_json.startswith("[")
    assert result.matchsheet[0].venue_id == 99
    assert result.matchsheet[0].is_home
    assert result.matchsheet[0].referee_id == 77
    assert '"optionalSummaryField":{"a":1,"z":2}' in result.extra_json


@pytest.mark.unit
def test_summary_missing_optionals_still_normalizes_rows() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload.pop("gameInfo")
    for roster in payload["rosters"]:
        athlete = roster["roster"][0]["athlete"]
        athlete.pop("jersey", None)
        roster["roster"][0].pop("captain", None)
        roster["roster"][0].pop("statistics", None)

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert all(row.jersey is None and row.captain is None for row in result.lineup)
    assert all(
        row.venue_id is None and row.referee_id is None for row in result.matchsheet
    )


@pytest.mark.unit
def test_consumed_nested_optionals_are_retained_in_canonical_extra_json() -> None:
    scoreboard = _load("native_scoreboard.json")
    scoreboard["events"][0]["competitions"][0]["venue"].update(
        {"capacity": 42000, "address": {"city": "Rome", "country": "IT"}}
    )
    scoreboard["events"][0]["competitions"][0]["competitors"][0]["team"]["color"] = (
        "112233"
    )
    competition, edition, schedule = _schedule(scoreboard)
    schedule_extra = json.loads(schedule[0].extra_json)

    summary = _load("native_summary.json")
    summary["gameInfo"]["venue"].update(
        {"capacity": 42000, "address": {"country": "IT", "city": "Rome"}}
    )
    summary["gameInfo"]["officials"][0].update(
        {"order": 1, "position": {"name": "REFEREE", "rank": 3}}
    )
    result = parse_summary(
        _raw(summary), competition=competition, edition=edition, event=schedule[0]
    )
    summary_extra = json.loads(result.extra_json)

    assert schedule_extra["venue"] == {
        "address": {"city": "Rome", "country": "IT"},
        "capacity": 42000,
    }
    assert schedule_extra["sides"]["home"]["team"]["color"] == "112233"
    assert schedule_extra["source"]["league"]["sourceOptional"] == {"a": 1, "z": 2}
    assert summary_extra["gameInfo"]["venue"] == {
        "address": {"city": "Rome", "country": "IT"},
        "capacity": 42000,
    }
    assert summary_extra["gameInfo"]["officials"][0]["order"] == 1
    assert summary_extra["gameInfo"]["officials"][0]["position"] == {"rank": 3}
    assert '"address":{"city":"Rome","country":"IT"}' in result.extra_json


@pytest.mark.unit
def test_summary_official_position_is_optional_and_unclassified_rows_are_retained() -> (
    None
):
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"] = [
        {
            "id": "88",
            "fullName": "Fourth Official",
            "displayName": "Fourth Official",
            "order": 4,
        },
        {
            "id": "89",
            "fullName": "Video Official",
            "position": {"name": "VIDEO_ASSISTANT", "rank": 2},
            "providerExtra": {"code": "VAR"},
        },
        {
            "id": "90",
            "fullName": "Reserve Official",
            "position": None,
        },
    ]

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert all(
        row.referee_id is None and row.referee is None for row in result.matchsheet
    )
    officials = json.loads(result.extra_json)["gameInfo"]["officials"]
    assert officials == payload["gameInfo"]["officials"]


@pytest.mark.unit
def test_multiple_primary_referees_are_preserved_without_guessing_a_scalar() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"] = [
        {
            "fullName": "First Referee",
            "displayName": "First Referee",
            "order": 1,
            "position": {
                "id": "1",
                "name": "Referee",
                "displayName": "Referee",
            },
        },
        {
            "fullName": "Second Referee",
            "displayName": "Second Referee",
            "order": 2,
            "position": {
                "id": "1",
                "name": "Referee",
                "displayName": "Referee",
            },
        },
    ]

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert all(
        row.referee_id is None and row.referee is None for row in result.matchsheet
    )
    assert (
        json.loads(result.extra_json)["gameInfo"]["officials"]
        == payload["gameInfo"]["officials"]
    )
    assert all(
        json.loads(row.extra_json)["summaryGameInfo"]["officials"]
        == payload["gameInfo"]["officials"]
        for row in result.matchsheet
    )


@pytest.mark.unit
def test_multiple_primary_referees_still_reject_a_malformed_source_row() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"].append(
        {"fullName": None, "position": {"name": "REFEREE"}}
    )

    with pytest.raises(EspnParseError, match=r"officials\[1\].fullName"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )


@pytest.mark.unit
def test_multiple_primary_referees_reject_extra_json_key_collision() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"].append(
        {"fullName": "Second Referee", "position": {"name": "REFEREE"}}
    )
    payload["boxscore"]["teams"][0]["summaryGameInfo"] = {"source": True}

    with pytest.raises(EspnParseError, match="collides"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )


@pytest.mark.unit
def test_summary_official_position_rejects_non_object_when_non_null() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"][0]["position"] = "REFEREE"

    with pytest.raises(EspnParseError, match=r"officials\[0\].position"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )


@pytest.mark.unit
def test_malformed_athlete_and_one_sided_sections_fail_not_valid_empty() -> None:
    competition, edition, schedule = _schedule()
    malformed = _load("native_summary.json")
    malformed["rosters"][0]["roster"][0]["athlete"].pop("id")
    with pytest.raises(EspnParseError, match="athlete.id"):
        parse_summary(
            _raw(malformed),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    one_sided = _load("native_summary.json")
    one_sided["rosters"] = one_sided["rosters"][:1]
    with pytest.raises(EspnParseError, match="both event teams"):
        parse_summary(
            _raw(one_sided),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_structurally_valid_prematch_stub_is_valid_empty_only_when_permitted() -> None:
    competition, edition = _scope(
        lineup=CapabilityState.PARTIAL, matchsheet=CapabilityState.ABSENT
    )
    _, _, schedule = _schedule(
        lineup=CapabilityState.PARTIAL, matchsheet=CapabilityState.ABSENT
    )
    stub = {
        "header": {
            "id": str(schedule[0].event_id),
            "competitions": [
                {
                    "date": schedule[0].kickoff.isoformat(),
                    "competitors": [
                        {
                            "homeAway": "home",
                            "team": {"id": "10", "displayName": "Home FC"},
                        },
                        {
                            "homeAway": "away",
                            "team": {"id": "20", "displayName": "Away FC"},
                        },
                    ],
                }
            ],
        }
    }

    result = parse_summary(
        _raw(stub), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY

    proven_competition, proven_edition = _scope()
    with pytest.raises(EspnParseError, match="proven lineup"):
        parse_summary(
            _raw(stub),
            competition=proven_competition,
            edition=proven_edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_structurally_valid_prematch_stub_allows_unknown_capabilities() -> None:
    competition, edition = _scope(
        lineup=CapabilityState.UNKNOWN, matchsheet=CapabilityState.UNKNOWN
    )
    _, _, schedule = _schedule(
        lineup=CapabilityState.UNKNOWN, matchsheet=CapabilityState.UNKNOWN
    )
    stub = {
        "header": {
            "id": str(schedule[0].event_id),
            "competitions": [
                {
                    "date": schedule[0].kickoff.isoformat(),
                    "competitors": [
                        {
                            "homeAway": "home",
                            "team": {"id": "10", "displayName": "Home FC"},
                        },
                        {
                            "homeAway": "away",
                            "team": {"id": "20", "displayName": "Away FC"},
                        },
                    ],
                }
            ],
        }
    }

    result = parse_summary(
        _raw(stub), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY


@pytest.mark.unit
def test_matchsheet_without_team_statistics_is_valid_empty_only_when_permitted() -> (
    None
):
    competition, edition, schedule = _schedule(matchsheet=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team.pop("statistics")

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup_state is EntityParseState.CAPTURED
    assert result.matchsheet == ()
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY

    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="proven matchsheet"):
        parse_summary(
            _raw(payload),
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_matchsheet_rejects_one_sided_missing_team_statistics() -> None:
    competition, edition, schedule = _schedule(matchsheet=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    payload["boxscore"]["teams"][0].pop("statistics")

    with pytest.raises(EspnParseError, match="both or neither"):
        parse_summary(
            _raw(payload),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_matchsheet_with_bilateral_empty_statistics_is_valid_empty_only_when_permitted() -> (
    None
):
    competition, edition, schedule = _schedule(matchsheet=CapabilityState.PARTIAL)
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team["statistics"] = []

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.matchsheet == ()
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY

    payload["boxscore"]["teams"][0]["statistics"] = [{"name": "shots", "value": 1}]
    with pytest.raises(EspnParseError, match="empty for both or neither"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )

    for team in payload["boxscore"]["teams"]:
        team["statistics"] = []
    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="proven matchsheet"):
        parse_summary(
            _raw(payload),
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_lineup_without_team_rosters_is_valid_empty_only_when_permitted() -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.PARTIAL)
    payload = _load("native_summary.json")
    for team in payload["rosters"]:
        team.pop("roster")

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED

    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="proven lineup"):
        parse_summary(
            _raw(payload),
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_lineup_rejects_one_sided_missing_team_roster() -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.PARTIAL)
    payload = _load("native_summary.json")
    payload["rosters"][0].pop("roster")

    with pytest.raises(EspnParseError, match="both or neither"):
        parse_summary(
            _raw(payload),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_only_reviewed_one_sided_lineup_degrades_to_valid_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    payload["rosters"][0].pop("roster")
    raw = _raw(payload)

    with pytest.raises(EspnParseError, match="both or neither"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    lineup_source = {"rosters": payload["rosters"]}
    lineup_source_sha256 = hashlib.sha256(
        json.dumps(
            lineup_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    identity = (schedule[0].scope_id, schedule[0].event_id)
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_ONE_SIDED_LINEUPS",
        {lineup_source_sha256: identity},
    )

    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )
    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY

    changed = deepcopy(payload)
    changed["rosters"][1]["roster"][0]["athlete"]["displayName"] = "Changed"
    with pytest.raises(EspnParseError, match="both or neither"):
        parse_summary(
            _raw(changed),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="proven lineup"):
        parse_summary(
            raw,
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_conventional_xi_requires_22_unique_starters() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        for offset in range(1, 12):
            player = deepcopy(seed)
            player["starter"] = True
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )
    assert sum(row.starter is True for row in result.lineup) == 22

    payload["rosters"][0]["roster"][0]["starter"] = False
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )

    asymmetric = _load("native_summary.json")
    for roster in asymmetric["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        count = 11 if roster["homeAway"] == "home" else 10
        for offset in range(1, count + 1):
            player = deepcopy(seed)
            player["starter"] = True
            player["athlete"]["id"] = str(base_id + offset)
            roster["roster"].append(player)
    with pytest.raises(EspnParseError, match="starter|conventional"):
        parse_summary(
            _raw(asymmetric),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_sparse_explicit_participant_roster_discards_the_whole_lineup() -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        is_home = roster["homeAway"] == "home"
        base_id = 100 if is_home else 200
        row_count = 1 if is_home else 20
        starter_count = 1 if is_home else 11
        roster["roster"] = []
        for offset in range(1, row_count + 1):
            player = deepcopy(seed)
            player["starter"] = offset <= starter_count
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED

    seven_player_boundary = deepcopy(payload)
    sparse_roster = next(
        roster
        for roster in seven_player_boundary["rosters"]
        if roster["homeAway"] == "home"
    )
    seed = sparse_roster["roster"][0]
    for offset in range(2, 8):
        player = deepcopy(seed)
        player["athlete"]["id"] = str(100 + offset)
        player["athlete"]["displayName"] = f"Player {100 + offset}"
        sparse_roster["roster"].append(player)
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(seven_player_boundary),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    malformed = deepcopy(payload)
    malformed["rosters"][1]["roster"][0]["athlete"].pop("id")
    with pytest.raises(EspnParseError, match="athlete.id"):
        parse_summary(
            _raw(malformed),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(payload),
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_complete_rosters_with_bad_starter_counts_still_fail() -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        is_home = roster["homeAway"] == "home"
        base_id = 100 if is_home else 200
        starter_count = 11 if is_home else 10
        roster["roster"] = []
        for offset in range(1, 21):
            player = deepcopy(seed)
            player["starter"] = offset <= starter_count
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)

    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(payload),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_reviewed_truncated_lineup_identity_is_exact_and_immutable() -> None:
    assert dict(summary_parser_module._REVIEWED_TRUNCATED_LINEUPS) == {
        "41c1ce43ba84ebb976040b5fb748f7a54c01d1d47ae5eff36193d74d9f289aad": (
            "18481:2025",
            761072,
            ((347, 11), (3802, 10)),
        ),
        "0d8f88f7e3486d1b40328e62f67c7484fe31373894c59a38490711b32f4960ef": (
            "19834:2026",
            401872737,
            ((124, 11), (3384, 10)),
        ),
        "7e5ceeae758c411a2be8e7693ba728d9ad6ff9f8b92fe5506ef3f71247887f3d": (
            "3922:2026",
            401856621,
            ((580, 11), (11678, 12)),
        ),
        "d97c47b2e2e437033280a2182cad85c540e3cd622c2db9c1e2d68ffaac750378": (
            "3922:2026",
            401863500,
            ((449, 11), (624, 12)),
        ),
        "58d2a9f9d41a3edd58af0795f389b6e718b5abc44e12654d7f8ba2bf45521726": (
            "3922:2026",
            401864003,
            ((205, 12), (2659, 11)),
        ),
        "7ea0b551a150097dcd84d46a14b1e6bcfed57115931ef0982f721b119361b613": (
            "3922:2026",
            401867105,
            ((4214, 10), (4277, 11)),
        ),
        "252ff0807fb86e598fce6433aab55898d8d535161047eb38b3769035982862af": (
            "3922:2026",
            401871169,
            ((479, 12), (2850, 11)),
        ),
        "7b15e1d008506a2e495627b7d1da7347bff968a8ca70afefd053c3efb7c0681c": (
            "3922:2026",
            401874051,
            ((4214, 10), (4385, 12)),
        ),
        "190cfb44654474b30825a9b001f59917e9c20fc10812481aaf117629943c79d4": (
            "3928:2026",
            401879512,
            ((7251, 8), (7257, 11)),
        ),
        "31e4165e594c8166526c254535ccdc529c41911437ba7ea93aa225b1d1d94f90": (
            "3928:2026",
            401879513,
            ((7254, 10), (7870, 9)),
        ),
        "b9e6120e51f6562faf02edcd10d6d0e02dc77574841128b95fa951276cbe2970": (
            "3928:2026",
            401879514,
            ((17516, 9), (131213, 9)),
        ),
        "9af01563ee246de907407877a8048b47f25209eabbc1e34b06713d64306f7a89": (
            "3928:2026",
            401879517,
            ((7252, 10), (7253, 11)),
        ),
        "3adb6bbe510e7a520a5deb6d8d99d540f27e343b8177f0f4eab4cedbbb06d061": (
            "3928:2026",
            401879623,
            ((7259, 11), (131835, 6)),
        ),
        "7db63748d32060a7de06ec08c7ba8f7cf00bb2e41f7803b5696ed8ef4904e0a6": (
            "3929:2026",
            401898685,
            ((7243, 4), (7245, 11)),
        ),
        "f02ab1944137c5e8218238dd271ea404bb698f3f3fd80bc020c08b91e5e77b60": (
            "3945:2026",
            401842743,
            ((2720, 11), (20856, 10)),
        ),
        "79983143b5562e8aa233fdc1caa0b977f88857f35bac3b9d50e1d6bb0ce39a6d": (
            "3945:2026",
            401842746,
            ((20856, 10), (22163, 11)),
        ),
        "8caabc860d5894856e51b2e4a22d49455d083fe65f3eb06dc36d97e6532ec928": (
            "3945:2026",
            401842760,
            ((2495, 11), (20856, 10)),
        ),
    }
    assert dict(summary_parser_module._REVIEWED_CONTRADICTORY_LINEUPS) == {
        "287b2052375fe3ef2fc4fc24f8c69f0be23d20adac832d86a098fc194275985f": (
            "19778:2025",
            734179,
            ((2664, 11), (2728, 11)),
        ),
        "dc4b54fc66f6d2ce7b7004c8fcb6411e59ca9470ae6386be6c745a4dc933788c": (
            "19778:2025",
            734184,
            ((214, 11), (2641, 11)),
        ),
        "e3a51e4590879e092163e1fad6a377467b4f7cc361fd56d3c91fac4e7965c2f9": (
            "19831:2020",
            565756,
            ((2829, 11), (18210, 12)),
        ),
        "6083361093816508832ea3be234c8cf475e4a5725d9d47871e7ca262c2594345": (
            "19831:2020",
            599000,
            ((2875, 11), (2888, 11)),
        ),
        "d42c97067cc2ac708e6e0085dd5735b7ef8c899a06d0658ffd7b2061cb412274": (
            "19834:2026",
            401867393,
            ((367, 11), (22344, 11)),
        ),
        "3698912fea6e1545167896f6aa1571491f7e6210c5f2f1e2ec0f895384236b30": (
            "19915:2026",
            401841831,
            ((20684, 11), (22525, 11)),
        ),
        "b1b068b7e2c931527efaf57dd574f953483544cc4046b87aa23099d1937cc535": (
            "2272:2026",
            762013,
            ((1936, 11), (7388, 11)),
        ),
        "1105bf97b43fe4a3733e41e8f0ad303f82a9746d5ac7ad536c4f98399b6b6ec4": (
            "3903:2026",
            401843855,
            ((2, 11), (7845, 11)),
        ),
        "64345c28d330aadb8a8ac6a483b931563f35bfb25749c085e299f8e064cc3d82": (
            "3904:2026",
            401844958,
            ((2635, 11), (14074, 8)),
        ),
        "4e1cb30f5e297aa0d409790db4e14d553bcfefc25a10f5a897019a2454a0b0c2": (
            "3904:2026",
            401844963,
            ((10052, 9), (10105, 9)),
        ),
        "9d8e0a3cb88a5e5a06aac1cfc8a3ea7f1b37fdd81e64243f599133d340189dd7": (
            "3911:2012",
            340346,
            ((2650, 17), (9632, 16)),
        ),
        "39b01ed6ba1b65b835bf0a8237aaed33fafaf704e5fa1c3b40c0f6426674b864": (
            "3911:2012",
            340348,
            ((2873, 17), (9632, 13)),
        ),
        "7e4203120f22150fe68dcee99f47dcf75d632b216e0b3ba27ac808d1ea1050fb": (
            "3911:2012",
            340350,
            ((2874, 11), (2875, 12)),
        ),
        "916d34e732a06275cc6e5f5b3acd06d75625eddaa85318b0a7dd0ca2f1aed851": (
            "3911:2012",
            340351,
            ((2888, 11), (11790, 16)),
        ),
        "6208068f6165e7f090ecd6a25f732af081bc3c5c25e849bf984f4ae9e9f8a6bc": (
            "3911:2012",
            340352,
            ((2874, 15), (11790, 15)),
        ),
        "960c13658d2be1a4c8fd9f82cf07d5c9f6db7f50bf8ab2a3de3dd7fe86adb436": (
            "3911:2012",
            340357,
            ((2875, 12), (2888, 11)),
        ),
        "7141b48dd8adb4790be50a7fa8d7c4c6b4f2e5d6374de2b45efea7d000b881f1": (
            "3922:2026",
            401867936,
            ((657, 8), (4214, 14)),
        ),
        "17d0e8226c3d20f947bfbe5d0b2578730a7f069a34cce259711300df0902c6a5": (
            "3922:2026",
            401872547,
            ((469, 11), (7368, 11)),
        ),
        "b2febcd86b77adeb0227ce853ee7ae0cbb5dcd4d8b249b70625253f84d784304": (
            "3922:2026",
            401874104,
            ((657, 10), (1038, 11)),
        ),
        "9836f00b83e2ce9a2c28090537dd686ead41118f4ab8a95295ba2db726c6ab18": (
            "3934:2026",
            762422,
            ((5584, 11), (21313, 11)),
        ),
        "a78cf5d47e0096a10e96ef19465feb3c854b895e2c8150d8c256ecf986296a98": (
            "3940:2026",
            401878593,
            ((259, 11), (422, 11)),
        ),
    }
    assert summary_parser_module._REVIEWED_PARTIAL_CONVENTIONAL_LINEUP_SCOPES == (
        frozenset({"3904:2026"})
    )
    assert summary_parser_module._REVIEWED_DUPLICATE_LINEUP_SCOPES == frozenset(
        {"3911:2012"}
    )
    assert dict(summary_parser_module._REVIEWED_ONE_SIDED_LINEUPS) == {
        "54c233a36e49dee961703a659ef03de013d445659614f3e3450bad0e63ad9ced": (
            "19834:2026",
            401897918,
        )
    }
    with pytest.raises(TypeError):
        summary_parser_module._REVIEWED_TRUNCATED_LINEUPS["0" * 64] = (  # type: ignore[index]
            "18481:2025",
            761072,
            ((347, 11), (3802, 10)),
        )
    with pytest.raises(TypeError):
        summary_parser_module._REVIEWED_CONTRADICTORY_LINEUPS["0" * 64] = (  # type: ignore[index]
            "19831:2020",
            565756,
            ((2829, 11), (18210, 12)),
        )
    with pytest.raises(TypeError):
        summary_parser_module._REVIEWED_ONE_SIDED_LINEUPS["0" * 64] = (  # type: ignore[index]
            "19834:2026",
            401897918,
        )


@pytest.mark.unit
def test_reviewed_scope_discards_only_partial_conventional_lineups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        starter_count = 11 if roster["homeAway"] == "home" else 6
        roster["roster"] = []
        for offset in range(1, 21):
            player = deepcopy(seed)
            player["starter"] = offset <= starter_count
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)

    raw = _raw(payload)
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_PARTIAL_CONVENTIONAL_LINEUP_SCOPES",
        frozenset({schedule[0].scope_id}),
    )
    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )
    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED

    contradictory = deepcopy(payload)
    contradictory["rosters"][0]["roster"][0]["subbedIn"] = True
    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(
            _raw(contradictory),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    complete = deepcopy(payload)
    away = next(
        roster for roster in complete["rosters"] if roster["homeAway"] == "away"
    )
    for player in away["roster"][:11]:
        player["starter"] = True
    for player in away["roster"][11:]:
        player["starter"] = False
    captured = parse_summary(
        _raw(complete),
        competition=competition,
        edition=edition,
        event=schedule[0],
    )
    assert captured.lineup_state is EntityParseState.CAPTURED
    assert len(captured.lineup) == 40

    balanced_small_sided = deepcopy(payload)
    balanced_small_sided["format"] = {"startersPerTeam": 5}
    for roster in balanced_small_sided["rosters"]:
        for index, player in enumerate(roster["roster"]):
            player["starter"] = index < 5
    small_sided_result = parse_summary(
        _raw(balanced_small_sided),
        competition=competition,
        edition=edition,
        event=schedule[0],
    )
    assert small_sided_result.lineup_state is EntityParseState.CAPTURED

    unbalanced_small_sided = deepcopy(balanced_small_sided)
    unbalanced_small_sided["rosters"][0]["roster"][4]["starter"] = False
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(unbalanced_small_sided),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    proven_competition, proven_edition, proven_schedule = _schedule()
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_PARTIAL_CONVENTIONAL_LINEUP_SCOPES",
        frozenset({proven_schedule[0].scope_id}),
    )
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            raw,
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_reviewed_scope_discards_duplicate_lineup_after_full_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    duplicate = deepcopy(payload["rosters"][0]["roster"][0])
    duplicate["captain"] = not duplicate["captain"]
    payload["rosters"][0]["roster"].append(duplicate)
    raw = _raw(payload)

    with pytest.raises(EspnParseError, match="duplicate event/team/athlete"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_DUPLICATE_LINEUP_SCOPES",
        frozenset({schedule[0].scope_id}),
    )
    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )
    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED

    contradictory = deepcopy(payload)
    for roster in contradictory["rosters"]:
        for player in roster["roster"]:
            player["starter"] = True
            player["subbedIn"] = False
            player["subbedOut"] = False
    contradictory["rosters"][0]["roster"][0]["subbedIn"] = True
    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(
            _raw(contradictory),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    malformed = deepcopy(payload)
    malformed["rosters"][1]["roster"][0]["athlete"].pop("id")
    with pytest.raises(EspnParseError, match="athlete.id"):
        parse_summary(
            _raw(malformed),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    mixed_starter_semantics = deepcopy(payload)
    mixed_starter_semantics["rosters"][0]["roster"][0]["starter"] = True
    with pytest.raises(EspnParseError, match="starter flag for every athlete"):
        parse_summary(
            _raw(mixed_starter_semantics),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    invalid_small_sided_format = deepcopy(payload)
    invalid_small_sided_format["format"] = {"startersPerTeam": 8}
    with pytest.raises(EspnParseError, match="integer from 1 to 7"):
        parse_summary(
            _raw(invalid_small_sided_format),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    unique = deepcopy(payload)
    unique["rosters"][0]["roster"].pop()
    captured = parse_summary(
        _raw(unique),
        competition=competition,
        edition=edition,
        event=schedule[0],
    )
    assert captured.lineup_state is EntityParseState.CAPTURED

    proven_competition, proven_edition, proven_schedule = _schedule()
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_DUPLICATE_LINEUP_SCOPES",
        frozenset({proven_schedule[0].scope_id}),
    )
    with pytest.raises(EspnParseError, match="duplicate event/team/athlete"):
        parse_summary(
            raw,
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_only_reviewed_truncated_conventional_lineup_degrades_to_valid_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        starter_count = 11 if roster["homeAway"] == "home" else 10
        roster["roster"] = []
        for offset in range(1, 21):
            player = deepcopy(seed)
            player["starter"] = offset <= starter_count
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)

    raw = _raw(payload)
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    identity = (
        schedule[0].scope_id,
        schedule[0].event_id,
        ((10, 11), (20, 10)),
    )
    lineup_source = {"rosters": payload["rosters"]}
    if "format" in payload:
        lineup_source["format"] = payload["format"]
    lineup_source_sha256 = hashlib.sha256(
        json.dumps(
            lineup_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_TRUNCATED_LINEUPS",
        {lineup_source_sha256: identity},
    )
    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )

    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED

    # Unrelated response-byte drift must not change an identical roster section.
    drifted = parse_summary(
        raw + b"\n",
        competition=competition,
        edition=edition,
        event=schedule[0],
    )
    assert drifted.lineup_state is EntityParseState.VALID_EMPTY

    # A roster-section change with the same starter counts must not match.
    changed_roster = deepcopy(payload)
    changed_roster["rosters"][1]["roster"][0]["athlete"]["displayName"] = "Changed"
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(changed_roster),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    # A lineup-relevant format change with identical rosters must not match.
    changed_format = deepcopy(payload)
    changed_format["format"] = {"startersPerTeam": 5}
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(changed_format),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    # The same roster hash must still match scope, event and exact team counts.
    wrong_identities = (
        ("other:2025", schedule[0].event_id, ((10, 11), (20, 10))),
        (schedule[0].scope_id, schedule[0].event_id + 1, ((10, 11), (20, 10))),
        (schedule[0].scope_id, schedule[0].event_id, ((10, 11), (20, 9))),
    )
    for wrong_identity in wrong_identities:
        monkeypatch.setattr(
            summary_parser_module,
            "_REVIEWED_TRUNCATED_LINEUPS",
            {lineup_source_sha256: wrong_identity},
        )
        with pytest.raises(EspnParseError, match="11 starters"):
            parse_summary(
                raw,
                competition=competition,
                edition=edition,
                event=schedule[0],
            )

    # Even the exact reviewed identity remains forbidden for PROVEN capability.
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_TRUNCATED_LINEUPS",
        {lineup_source_sha256: identity},
    )
    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            raw,
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_only_reviewed_contradictory_lineup_degrades_to_valid_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        for offset in range(1, 12):
            player = deepcopy(seed)
            player["starter"] = True
            player["subbedIn"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)
    payload["rosters"][0]["roster"][0]["subbedIn"] = True
    raw = _raw(payload)

    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    lineup_source = {"rosters": payload["rosters"]}
    if "format" in payload:
        lineup_source["format"] = payload["format"]
    lineup_source_sha256 = hashlib.sha256(
        json.dumps(
            lineup_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    identity = (
        schedule[0].scope_id,
        schedule[0].event_id,
        ((10, 11), (20, 11)),
    )
    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_CONTRADICTORY_LINEUPS",
        {lineup_source_sha256: identity},
    )

    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )
    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY

    changed = deepcopy(payload)
    changed["rosters"][1]["roster"][0]["athlete"]["displayName"] = "Changed"
    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(
            _raw(changed),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )

    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_CONTRADICTORY_LINEUPS",
        {lineup_source_sha256: (schedule[0].scope_id, schedule[0].event_id, ())},
    )
    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(raw, competition=competition, edition=edition, event=schedule[0])

    monkeypatch.setattr(
        summary_parser_module,
        "_REVIEWED_CONTRADICTORY_LINEUPS",
        {lineup_source_sha256: identity},
    )
    proven_competition, proven_edition, proven_schedule = _schedule()
    with pytest.raises(EspnParseError, match="proven lineup"):
        parse_summary(
            raw,
            competition=proven_competition,
            edition=proven_edition,
            event=proven_schedule[0],
        )


@pytest.mark.unit
def test_unreviewed_bench_player_marked_only_as_subbed_out_is_rejected() -> None:
    competition, edition, schedule = _schedule(lineup=CapabilityState.UNKNOWN)
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        for offset in range(1, 12):
            player = deepcopy(seed)
            player["starter"] = True
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            roster["roster"].append(player)
    bench = deepcopy(payload["rosters"][0]["roster"][0])
    bench["athlete"]["id"] = "999"
    bench["starter"] = False
    bench["subbedIn"] = False
    bench["subbedOut"] = True
    payload["rosters"][0]["roster"].append(bench)

    with pytest.raises(EspnParseError, match="contradictory"):
        parse_summary(
            _raw(payload),
            competition=competition,
            edition=edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_balanced_small_sided_explicit_lineup_is_genuinely_non_conventional() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["format"] = {"startersPerTeam": 5}
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        for offset in range(1, 6):
            player = deepcopy(seed)
            player["starter"] = True
            player["athlete"]["id"] = str(base_id + offset)
            roster["roster"].append(player)

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert sum(row.starter is True for row in result.lineup) == 10

    unknown_competition, unknown_edition, unknown_schedule = _schedule(
        lineup=CapabilityState.UNKNOWN
    )
    unbalanced = deepcopy(payload)
    next(
        roster for roster in unbalanced["rosters"] if roster["homeAway"] == "home"
    )["roster"].pop()
    with pytest.raises(EspnParseError, match="11 starters"):
        parse_summary(
            _raw(unbalanced),
            competition=unknown_competition,
            edition=unknown_edition,
            event=unknown_schedule[0],
        )


@pytest.mark.unit
@pytest.mark.parametrize("bad_value", [None, True, [12], {"value": 12}, "twelve"])
def test_matchsheet_rejects_malformed_stat_values(bad_value: object) -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    payload["boxscore"]["teams"][0]["statistics"][0]["value"] = bad_value

    with pytest.raises(EspnParseError, match="statistic.*value|scalar"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )


@pytest.mark.unit
def test_matchsheet_uses_numeric_display_value_when_value_is_null() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team["statistics"] = [{"name": "shots", "value": None, "displayValue": "12"}]

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert [row.total_shots for row in result.matchsheet] == ["12", "12"]


@pytest.mark.unit
def test_unknown_structured_matchsheet_stats_remain_canonical_without_failing() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team["statistics"].append(
            {
                "name": "newProviderShape",
                "value": {"segments": [1, 2], "label": "experimental"},
            }
        )

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert all(row.total_shots is not None for row in result.matchsheet)
    assert all("newProviderShape" in row.statistics_json for row in result.matchsheet)


@pytest.mark.unit
def test_dual_lineup_stat_sources_and_mapping_shapes_populate_legacy_fields() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        player = roster["roster"][0]
        player["stats"] = [{"name": "totalShots", "value": 4}]
        player["statistics"] = {
            "appearances": {"displayValue": "3"},
            "foulsCommitted": {"value": 2},
            "goalAssists": 1,
        }

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    assert all(row.total_shots == 4.0 for row in result.lineup)
    assert all(row.appearances == 3.0 for row in result.lineup)
    assert all(row.fouls_committed == 2.0 for row in result.lineup)
    assert all(row.goal_assists == 1.0 for row in result.lineup)
    assert all('"statistics"' in row.statistics_json for row in result.lineup)
    assert all('"stats"' in row.statistics_json for row in result.lineup)


@pytest.mark.unit
def test_conflicting_dual_lineup_stat_sources_fail_closed() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        player = roster["roster"][0]
        player["stats"] = [{"name": "totalShots", "value": 4}]
        player["statistics"] = {"totalShots": 5}

    with pytest.raises(EspnParseError, match="conflicting.*total_shots"):
        parse_summary(
            _raw(payload), competition=competition, edition=edition, event=schedule[0]
        )


@pytest.mark.unit
def test_parser_rows_explicitly_cover_every_existing_legacy_bronze_column() -> None:
    schema = json.loads(
        (
            Path(__file__).resolve().parents[2] / "fixtures" / "bronze_schemas.json"
        ).read_text()
    )["tables"]
    metadata = {"_batch_id", "_entity_type", "_ingested_at", "_source"}
    from scrapers.espn.parser_contracts import LineupRow, MatchsheetRow, ScheduleRow

    actual = {
        "bronze.espn_schedule": {field.name for field in fields(ScheduleRow)},
        "bronze.espn_lineup": {field.name for field in fields(LineupRow)},
        "bronze.espn_matchsheet": {field.name for field in fields(MatchsheetRow)},
    }
    for table, row_fields in actual.items():
        expected = set(schema[table]["columns"]) - metadata
        assert expected <= row_fields, (
            f"{table} missing {sorted(expected - row_fields)}"
        )


@pytest.mark.unit
def test_versioned_stat_name_maps_populate_full_legacy_surfaces() -> None:
    from scrapers.espn.parser_contracts import (
        LINEUP_STAT_MAP_VERSION,
        MATCHSHEET_STAT_MAP_VERSION,
    )

    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    lineup_names = {
        "appearances": "appearances",
        "foulsCommitted": "fouls_committed",
        "foulsSuffered": "fouls_suffered",
        "goalAssists": "goal_assists",
        "goalsConceded": "goals_conceded",
        "offsides": "offsides",
        "ownGoals": "own_goals",
        "redCards": "red_cards",
        "saves": "saves",
        "shotsFaced": "shots_faced",
        "shotsOnTarget": "shots_on_target",
        "subIns": "sub_ins",
        "totalGoals": "total_goals",
        "totalShots": "total_shots",
        "yellowCards": "yellow_cards",
    }
    matchsheet_names = {
        "accurateCrosses": "accurate_crosses",
        "accurateLongBalls": "accurate_long_balls",
        "accuratePasses": "accurate_passes",
        "blockedShots": "blocked_shots",
        "crossPct": "cross_pct",
        "effectiveClearance": "effective_clearance",
        "effectiveTackles": "effective_tackles",
        "foulsCommitted": "fouls_committed",
        "goalAssists": "goal_assists",
        "goalDifference": "goal_difference",
        "goalsConceded": "goals_conceded",
        "interceptions": "interceptions",
        "longballPct": "longball_pct",
        "offsides": "offsides",
        "passPct": "pass_pct",
        "penaltyKickGoals": "penalty_kick_goals",
        "penaltyKickShots": "penalty_kick_shots",
        "possessionPct": "possession_pct",
        "redCards": "red_cards",
        "saves": "saves",
        "shotPct": "shot_pct",
        "shotsOnTarget": "shots_on_target",
        "tacklePct": "tackle_pct",
        "totalClearance": "total_clearance",
        "totalCrosses": "total_crosses",
        "totalGoals": "total_goals",
        "totalLongBalls": "total_long_balls",
        "totalPasses": "total_passes",
        "totalShots": "total_shots",
        "totalTackles": "total_tackles",
        "wonCorners": "won_corners",
        "yellowCards": "yellow_cards",
    }
    for roster in payload["rosters"]:
        roster["roster"][0]["stats"] = [
            {"name": name, "value": index + 0.5}
            for index, name in enumerate(lineup_names)
        ]
        roster["roster"][0].pop("statistics", None)
    for team in payload["boxscore"]["teams"]:
        team["statistics"] = [
            {"name": name, "value": index + 1}
            for index, name in enumerate(matchsheet_names)
        ]

    result = parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )

    lineup = result.lineup[0]
    matchsheet = result.matchsheet[0]
    assert LINEUP_STAT_MAP_VERSION == "espn-lineup-stat-map-v1"
    assert MATCHSHEET_STAT_MAP_VERSION == "espn-matchsheet-stat-map-v1"
    assert all(
        isinstance(getattr(lineup, target), float) for target in lineup_names.values()
    )
    assert all(
        isinstance(getattr(matchsheet, target), str)
        for target in matchsheet_names.values()
    )
    assert matchsheet.won_corners == "31"
