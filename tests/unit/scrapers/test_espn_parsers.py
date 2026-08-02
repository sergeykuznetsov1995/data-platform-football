"""Frozen, network-free contracts for ESPN native offline parsing."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
from datetime import date, timezone
import json
from pathlib import Path

import pytest

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
