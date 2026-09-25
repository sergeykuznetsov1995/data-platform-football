"""Frozen, network-free contracts for ESPN native offline parsing."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re

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
from scrapers.espn.denominator import DEFAULT_DENOMINATOR_PATH, load_denominator
from scrapers.espn.parsers import (
    EntityParseState,
    EspnParseError,
    LINEUP_STAT_NAME_MAP,
    MATCHSHEET_STAT_NAME_MAP,
    SummaryDisposition,
    espn_day,
    parse_all_scoreboard_day,
    parse_competition_detail_bytes,
    parse_scoreboard_calendar,
    parse_scoreboards,
    parse_soccer_dropdown_bytes,
    parse_summary,
    stale_open_events,
)
from scrapers.espn.parser_contracts import ScheduleParseState
from scrapers.espn.parser_common import source_day_bounds


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
def test_unknown_status_quarantines_the_match_and_schema_drift_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # #1501 (R-11): an unknown status name holds back one match, not the
    # whole tournament; required-schema drift still fails closed.
    payload = _load("native_scoreboard.json")
    known = deepcopy(payload["events"][0])
    known["id"] = "401000002"
    payload["events"][0]["status"]["type"]["name"] = "STATUS_NEW_FROM_UPSTREAM"
    payload["events"].append(known)
    with caplog.at_level("WARNING", logger="scrapers.espn.schedule_parser"):
        _, _, rows = _schedule(payload)

    by_id = {row.event_id: row for row in rows}
    quarantined = by_id[401000001]
    assert quarantined.status == "STATUS_NEW_FROM_UPSTREAM"
    assert quarantined.parse_state is ScheduleParseState.QUARANTINED
    assert not quarantined.terminal and not quarantined.played_final
    assert not quarantined.summary_required
    assert by_id[401000002].parse_state is ScheduleParseState.PARSED
    assert by_id[401000002].played_final
    assert "STATUS_NEW_FROM_UPSTREAM" in caplog.text

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
def test_bare_empty_scoreboard_event_is_skipped_but_any_field_stays_strict() -> None:
    _, _, baseline = _schedule()

    padded = _load("native_scoreboard.json")
    padded["events"].append({})
    _, _, rows = _schedule(padded)
    assert rows == baseline

    for drift in ({"id": "0"}, {"id": ""}, {"id": None}, {"date": "2020-09-19T18:45Z"}):
        polluted = _load("native_scoreboard.json")
        polluted["events"].append(drift)
        with pytest.raises(EspnParseError, match="canonical positive native ID"):
            _schedule(polluted)


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
def test_aug_5_leagues_cup_source_day_keeps_aug_6_utc_kickoffs() -> None:
    payload = _load("native_scoreboard.json")
    template = payload["events"][0]
    event_ids = (401863559, 401863560, 401863562, 401863563, 401863564)
    payload["events"] = []
    for event_id in event_ids:
        event = deepcopy(template)
        event["id"] = str(event_id)
        event["date"] = "2026-08-06T00:00Z"
        event["season"]["year"] = 2026
        payload["events"].append(event)
    payload["leagues"][0].update(id="19425", slug="concacaf.leagues.cup")
    competition, edition = _scope(
        espn_id=19425,
        slug="concacaf.leagues.cup",
        year=2026,
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    rows = parse_scoreboards(
        _raw(payload),
        competition=competition,
        edition=edition,
        query_start=date(2026, 8, 5),
        query_end=date(2026, 8, 5),
    )

    assert tuple(row.event_id for row in rows) == event_ids


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kickoff", "admitted"),
    [
        ("2026-08-04T23:59Z", True),
        ("2026-08-05T12:00Z", True),
        ("2026-08-06T00:00Z", True),
        ("2026-08-03T23:59Z", False),
        ("2026-08-07T00:00Z", False),
    ],
)
def test_source_calendar_day_buffer_is_exactly_one_utc_day(
    kickoff: str, admitted: bool
) -> None:
    payload = _load("native_scoreboard.json")
    payload["events"][0]["date"] = kickoff
    payload["events"][0]["season"]["year"] = 2026
    competition, edition = _scope(
        year=2026,
        start=date(2026, 8, 5),
        end=date(2026, 8, 5),
    )

    rows = parse_scoreboards(
        _raw(payload),
        competition=competition,
        edition=edition,
        query_start=date(2026, 8, 5),
        query_end=date(2026, 8, 5),
    )

    assert bool(rows) is admitted


@pytest.mark.unit
def test_source_calendar_buffer_is_ordinal_safe_at_date_sentinels() -> None:
    assert source_day_bounds(date.min, date.min) == (
        date.min,
        date.min + date.resolution,
    )
    assert source_day_bounds(date.max, date.max) == (
        date.max - date.resolution,
        date.max,
    )


@pytest.mark.unit
def test_source_calendar_buffer_never_weakens_source_season_identity() -> None:
    payload = _load("native_scoreboard.json")
    payload["events"][0]["date"] = "2026-08-06T00:00Z"
    payload["events"][0]["season"]["year"] = 2025
    competition, edition = _scope(
        year=2026,
        start=date(2026, 8, 5),
        end=date(2026, 8, 5),
    )

    rows = parse_scoreboards(
        _raw(payload),
        competition=competition,
        edition=edition,
        query_start=date(2026, 8, 5),
        query_end=date(2026, 8, 5),
    )

    assert rows == ()


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


def _summary(payload: dict, **scope_kwargs):
    competition, edition, schedule = _schedule(**scope_kwargs)
    return parse_summary(
        _raw(payload), competition=competition, edition=edition, event=schedule[0]
    )


def _full_rosters(payload: dict, counts: dict[str, tuple[int, int]]) -> None:
    """Replace each roster with ``rows`` athletes, the first ``starters`` start."""

    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        rows, starters = counts[roster["homeAway"]]
        roster["roster"] = []
        for offset in range(1, rows + 1):
            player = deepcopy(seed)
            player["starter"] = offset <= starters
            player["subbedIn"] = False
            player["subbedOut"] = False
            player["athlete"]["id"] = str(base_id + offset)
            player["athlete"]["displayName"] = f"Player {base_id + offset}"
            roster["roster"].append(player)


@pytest.mark.unit
def test_summary_is_parsed_once_and_joins_reordered_sections_by_native_team_id() -> (
    None
):
    payload = _load("native_summary.json")
    payload["header"]["competitions"][0]["competitors"].reverse()
    payload["rosters"].reverse()
    assert "form" not in payload["boxscore"]

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.CAPTURED
    assert result.reason is None and result.anomalies == ()
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
    assert result.matchsheet[0].referee == "Ref Example"
    assert [row.score for row in result.matchsheet] == [2, 1]
    assert result.parser_version == "espn-native-parser-v5"


@pytest.mark.unit
def test_summary_missing_optionals_still_normalizes_rows() -> None:
    payload = _load("native_summary.json")
    payload.pop("gameInfo")
    for roster in payload["rosters"]:
        roster["roster"][0]["athlete"].pop("jersey", None)
        roster["roster"][0].pop("statistics", None)

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.CAPTURED
    assert all(row.jersey is None for row in result.lineup)
    assert all(
        row.venue_id is None and row.referee is None and row.attendance is None
        for row in result.matchsheet
    )


@pytest.mark.unit
def test_jersey_is_read_from_the_roster_entry_before_the_athlete() -> None:
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        roster["roster"][0]["jersey"] = "21"

    result = _summary(payload)

    assert {row.jersey for row in result.lineup} == {"21"}
    assert all('"jersey"' not in row.extra_json for row in result.lineup)


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
    assert summary_extra["gameInfo"]["officials"] == [
        {"id": "77", "position": {"rank": 3}}
    ]


@pytest.mark.unit
def test_dropped_summary_blocks_never_reach_extra_json() -> None:
    payload = _load("native_summary.json")
    for block in (
        "news",
        "article",
        "videos",
        "odds",
        "pickcenter",
        "lastFiveGames",
        "seasonseries",
        "standings",
        "broadcasts",
        "leaders",
    ):
        payload[block] = {"sentinel": block}
    payload["boxscore"]["newBoxscoreField"] = 1

    result = _summary(payload)

    assert "sentinel" not in result.extra_json
    # Root-level keys outside the parsed blocks are not kept either.
    assert "optionalSummaryField" not in result.extra_json
    assert json.loads(result.extra_json)["boxscore"] == {"newBoxscoreField": 1}


@pytest.mark.unit
def test_summary_official_position_is_optional_and_unclassified_rows_are_retained() -> (
    None
):
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
        {"id": "90", "fullName": "Reserve Official", "position": None},
    ]

    result = _summary(payload)

    assert all(row.referee is None for row in result.matchsheet)
    officials = json.loads(result.extra_json)["gameInfo"]["officials"]
    assert officials == payload["gameInfo"]["officials"]


@pytest.mark.unit
def test_referee_is_the_official_ordered_first() -> None:
    # eng.1 2010 lists one referee three times with order 1-3; the old parser
    # refused to pick one and left the column empty.
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"] = [
        {
            "fullName": "Second Referee",
            "order": 2,
            "position": {"name": "Referee"},
        },
        {"fullName": "First Referee", "order": 1},
    ]

    result = _summary(payload)

    assert {row.referee for row in result.matchsheet} == {"First Referee"}
    officials = json.loads(result.extra_json)["gameInfo"]["officials"]
    assert officials[0] == payload["gameInfo"]["officials"][0]


@pytest.mark.unit
def test_referee_falls_back_to_the_first_labelled_referee() -> None:
    payload = _load("native_summary.json")
    payload["gameInfo"]["officials"] = [
        {"fullName": "Assistant", "position": {"name": "Assistant Referee"}},
        {"fullName": "Main Referee", "position": {"displayName": "Match Referee"}},
        {"fullName": "Other Referee", "position": {"name": "Referee"}},
    ]

    result = _summary(payload)

    assert {row.referee for row in result.matchsheet} == {"Main Referee"}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda p: p["gameInfo"]["officials"][0].update(position="REFEREE"),
            r"officials\[0\].position",
        ),
        (
            lambda p: p["rosters"][0]["roster"][0]["athlete"].pop("id"),
            r"athlete.id",
        ),
        (lambda p: p.update(rosters={"home": []}), r"summary.rosters must be an array"),
        (
            lambda p: p["header"]["competitions"][0]["competitors"].append(
                deepcopy(p["header"]["competitions"][0]["competitors"][0])
            ),
            "exactly two competitors",
        ),
        (lambda p: p["header"].update(id="401000002"), "header.id"),
        (
            lambda p: p["header"]["competitions"][0]["competitors"][1].update(
                homeAway="home"
            ),
            "unique home and away",
        ),
        (
            lambda p: p["rosters"].append(deepcopy(p["rosters"][0])),
            "duplicate team ID",
        ),
        (lambda p: p.update(keyEvents={"id": 1}), r"summary.keyEvents must be an array"),
        (
            lambda p: p["boxscore"]["teams"][0]["statistics"][0].update(value="twelve"),
            "value",
        ),
    ],
)
def test_unknown_shape_is_source_malformed_and_the_next_match_still_publishes(
    mutate, reason: str
) -> None:
    payload = _load("native_summary.json")
    mutate(payload)

    broken = _summary(payload)

    assert broken.disposition is SummaryDisposition.SOURCE_MALFORMED
    assert broken.reason is not None
    assert re.search(reason, broken.reason)
    assert broken.lineup == () and broken.matchsheet == () and broken.events == ()
    assert broken.lineup_state is EntityParseState.MALFORMED
    assert broken.matchsheet_state is EntityParseState.MALFORMED

    neighbour = _summary(_load("native_summary.json"))

    assert neighbour.disposition is SummaryDisposition.CAPTURED
    assert neighbour.lineup and neighbour.matchsheet


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "reason"),
    [(b"not json", "valid JSON"), (b"[]", "root must be an object")],
)
def test_non_object_body_is_source_malformed(raw: bytes, reason: str) -> None:
    competition, edition, schedule = _schedule()

    result = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )

    assert result.disposition is SummaryDisposition.SOURCE_MALFORMED
    assert reason in (result.reason or "")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("literal", "reason"),
    [
        ("1" + "0" * 400, "OverflowError"),
        ("1" * 5000, "ValueError"),
        ("[" * 100_000 + "]" * 100_000, "RecursionError"),
    ],
    ids=["float_overflow", "too_many_digits", "too_deep"],
)
def test_content_numeric_and_depth_errors_are_source_malformed(
    literal: str, reason: str
) -> None:
    payload = _load("native_summary.json")
    payload["rosters"][0]["roster"][0]["statistics"] = [
        {"name": "totalGoals", "value": "__LITERAL__"}
    ]
    raw = _raw(payload).replace(b'"__LITERAL__"', literal.encode())
    competition, edition, schedule = _schedule()

    broken = parse_summary(
        raw, competition=competition, edition=edition, event=schedule[0]
    )

    assert broken.disposition is SummaryDisposition.SOURCE_MALFORMED
    assert reason in (broken.reason or "")
    assert broken.lineup == () and broken.matchsheet == ()
    neighbour = _summary(_load("native_summary.json"))
    assert neighbour.disposition is SummaryDisposition.CAPTURED


@pytest.mark.unit
def test_caller_bugs_still_raise() -> None:
    competition, edition, schedule = _schedule()
    with pytest.raises(TypeError, match="raw payload must be bytes"):
        parse_summary(
            _load("native_summary.json"),  # type: ignore[arg-type]
            competition=competition,
            edition=edition,
            event=schedule[0],
        )
    other_competition, other_edition = _scope(espn_id=700, slug="eng.1")
    with pytest.raises(ValueError, match="context does not match"):
        parse_summary(
            _raw(_load("native_summary.json")),
            competition=other_competition,
            edition=other_edition,
            event=schedule[0],
        )


@pytest.mark.unit
def test_schedule_kickoff_and_venue_are_not_identity() -> None:
    # A rescheduled match or a matchday placeholder kickoff (#1501) must not
    # cost the Summary; the Summary venue wins over the schedule one.
    payload = _load("native_summary.json")
    payload["header"]["competitions"][0]["date"] = "2020-09-20T12:00Z"
    payload["gameInfo"]["venue"]["id"] = "98"

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.CAPTURED
    assert {row.venue_id for row in result.matchsheet} == {98}


@pytest.mark.unit
@pytest.mark.parametrize(
    "capability",
    (CapabilityState.PROVEN, CapabilityState.UNKNOWN, CapabilityState.QUARANTINED),
)
def test_structurally_valid_prematch_stub_is_valid_empty_whatever_the_capability(
    capability: CapabilityState,
) -> None:
    # The parser no longer reads edition capabilities (C8-F4).
    _, _, schedule = _schedule()
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

    result = _summary(stub, lineup=capability, matchsheet=capability)

    assert result.disposition is SummaryDisposition.VALID_EMPTY
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY
    assert result.reason is None


@pytest.mark.unit
@pytest.mark.parametrize("empty", ("absent", "empty"))
def test_matchsheet_without_team_statistics_is_valid_empty(empty: str) -> None:
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        if empty == "absent":
            team.pop("statistics")
        else:
            team["statistics"] = []

    result = _summary(payload)

    assert result.lineup_state is EntityParseState.CAPTURED
    assert result.matchsheet == ()
    assert result.matchsheet_state is EntityParseState.VALID_EMPTY
    # Only both entities empty make the match VALID_EMPTY.
    assert result.disposition is SummaryDisposition.CAPTURED


def _zero_skeleton() -> list[dict[str, object]]:
    """The shape ESPN actually sends: displayValue strings, no ``value`` key."""

    return [
        {"name": "foulsCommitted", "displayName": "Fouls", "displayValue": "0"},
        {"name": "possessionPct", "displayName": "Possession", "displayValue": "0.0"},
        {"name": "totalShots", "displayName": "Shots", "displayValue": "0"},
    ]


@pytest.mark.unit
@pytest.mark.parametrize("empty_side", ("absent", "empty", "missing_team"))
def test_one_sided_statistics_write_the_other_side_and_flag_the_match(
    empty_side: str,
) -> None:
    # ESPN answered 19834:2026 event 401908161 with ``statistics: []`` for one
    # team and a zero skeleton for the other.
    payload = _load("native_summary.json")
    if empty_side == "absent":
        payload["boxscore"]["teams"][0].pop("statistics")
    elif empty_side == "empty":
        payload["boxscore"]["teams"][0]["statistics"] = []
    else:
        payload["boxscore"]["teams"].pop(0)
    payload["boxscore"]["teams"][-1]["statistics"] = _zero_skeleton()

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.LINEUP_ANOMALY
    assert result.anomalies == ("one_sided_statistics",)
    assert [row.team_id for row in result.matchsheet] == [10]
    assert result.matchsheet[0].total_shots == "0"
    assert result.lineup_state is EntityParseState.CAPTURED


@pytest.mark.unit
@pytest.mark.parametrize("empty", ("absent", "empty"))
def test_lineup_without_any_player_is_valid_empty(empty: str) -> None:
    payload = _load("native_summary.json")
    for team in payload["rosters"]:
        if empty == "absent":
            team.pop("roster")
        else:
            team["roster"] = []

    result = _summary(payload)

    assert result.lineup == ()
    assert result.lineup_state is EntityParseState.VALID_EMPTY
    assert result.matchsheet_state is EntityParseState.CAPTURED
    assert result.disposition is SummaryDisposition.CAPTURED


@pytest.mark.unit
@pytest.mark.parametrize("empty_side", ("absent", "empty", "missing_block"))
def test_one_sided_roster_writes_the_other_team(empty_side: str) -> None:
    payload = _load("native_summary.json")
    away = next(block for block in payload["rosters"] if block["homeAway"] == "away")
    if empty_side == "absent":
        away.pop("roster")
    elif empty_side == "empty":
        away["roster"] = []
    else:
        payload["rosters"].remove(away)

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.LINEUP_ANOMALY
    assert result.anomalies == ("one_sided_roster",)
    assert [row.team_id for row in result.lineup] == [10]


@pytest.mark.unit
def test_starters_not_11_writes_rows_and_flags_the_match() -> None:
    payload = _load("native_summary.json")
    _full_rosters(payload, {"home": (11, 11), "away": (11, 11)})

    result = _summary(payload)
    assert result.disposition is SummaryDisposition.CAPTURED
    assert sum(row.starter is True for row in result.lineup) == 22

    for counts in (
        {"home": (11, 10), "away": (11, 11)},
        {"home": (20, 11), "away": (20, 13)},
        # A sparse participant list (only the scorer) is written as well.
        {"home": (1, 1), "away": (20, 11)},
    ):
        payload = _load("native_summary.json")
        _full_rosters(payload, counts)

        result = _summary(payload)

        assert result.disposition is SummaryDisposition.LINEUP_ANOMALY
        assert result.anomalies == ("starters_not_11",)
        assert len(result.lineup) == counts["home"][0] + counts["away"][0]
        assert result.lineup_state is EntityParseState.CAPTURED


@pytest.mark.unit
def test_contradictory_flags_write_rows_and_flag_the_match() -> None:
    payload = _load("native_summary.json")
    _full_rosters(payload, {"home": (11, 11), "away": (11, 11)})
    bench = deepcopy(payload["rosters"][0]["roster"][0])
    bench["athlete"]["id"] = "999"
    bench.update(starter=False, subbedIn=False, subbedOut=True)
    payload["rosters"][0]["roster"].append(bench)

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.LINEUP_ANOMALY
    assert result.anomalies == ("contradictory_flags",)
    assert 999 in {row.athlete_id for row in result.lineup}


@pytest.mark.unit
def test_duplicate_player_keeps_the_first_row() -> None:
    payload = _load("native_summary.json")
    twin = deepcopy(payload["rosters"][0]["roster"][0])
    twin["position"] = {"name": "Goalkeeper"}
    payload["rosters"][0]["roster"].append(twin)

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.LINEUP_ANOMALY
    assert result.anomalies == ("duplicate_player",)
    away = [row for row in result.lineup if row.team_id == 20]
    assert [(row.athlete_id, row.position) for row in away] == [(201, "Forward")]


@pytest.mark.unit
def test_formation_place_missing_only_when_formation_is_declared() -> None:
    def _payload(formation: str | None, places: list[str | None]) -> dict:
        payload = _load("native_summary.json")
        _full_rosters(payload, {"home": (11, 11), "away": (11, 11)})
        for block in payload["rosters"]:
            if formation is not None:
                block["formation"] = formation
            for player, place in zip(block["roster"], places * 11):
                player.pop("formationPlace", None)
                if place is not None:
                    player["formationPlace"] = place
        return payload

    partial = _summary(_payload("4-4-2", ["1", None]))
    assert partial.anomalies == ("formation_place_missing",)
    assert {row.formation for row in partial.matchsheet} == {"4-4-2"}

    # 2005-2016 bodies: a formation and no formationPlace at all.
    assert _summary(_payload("4-4-2", [None])).anomalies == ()
    # No formation: nothing to check against.
    no_formation = _summary(_payload(None, ["1", None]))
    assert no_formation.anomalies == ()
    assert {row.formation for row in no_formation.matchsheet} == {None}


@pytest.mark.unit
def test_balanced_small_sided_explicit_lineup_is_genuinely_non_conventional() -> None:
    payload = _load("native_summary.json")
    payload["format"] = {"startersPerTeam": 5}
    _full_rosters(payload, {"home": (5, 5), "away": (5, 5)})

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.CAPTURED
    assert sum(row.starter is True for row in result.lineup) == 10

    _full_rosters(payload, {"home": (4, 4), "away": (5, 5)})
    unbalanced = _summary(payload)
    assert unbalanced.anomalies == ("starters_not_11",)


@pytest.mark.unit
def test_match_level_fields_come_from_the_same_summary() -> None:
    # No recorded Summary has a shootout or a two-legged tie: the shapes are
    # the C6-F6 keys on the competitor/competition form of the recorded bodies.
    payload = _load("native_summary.json")
    competition_raw = payload["header"]["competitions"][0]
    competition_raw["leg"] = {"value": 2, "displayValue": "2nd Leg"}
    home, away = competition_raw["competitors"]
    home.update(
        linescores=[
            {"displayValue": "1"},
            {"displayValue": "0"},
            {"displayValue": "1"},
            {"displayValue": "0"},
        ],
        shootoutScore=4,
        aggregateScore=2.0,
        advance=True,
    )
    away.update(
        linescores=[{"displayValue": "0"}, {"value": 1.0, "displayValue": "1"}],
        shootoutScore="3",
        aggregateScore=2,
        advance=False,
    )
    payload["gameInfo"]["attendance"] = 0
    payload["rosters"][0]["formation"] = "4-4-2"

    result = _summary(payload)

    by_side = {row.home_away: row for row in result.matchsheet}
    assert (by_side["home"].score_h1, by_side["home"].score_h2) == (1, 0)
    assert by_side["home"].score_et == 1
    assert (by_side["away"].score_h1, by_side["away"].score_h2) == (0, 1)
    assert by_side["away"].score_et is None
    assert (by_side["home"].shootout_score, by_side["away"].shootout_score) == (4, 3)
    assert (by_side["home"].aggregate_score, by_side["away"].aggregate_score) == (2, 2)
    assert (by_side["home"].advance, by_side["away"].advance) == (True, False)
    assert {row.leg for row in result.matchsheet} == {2}
    assert result.advance_team_id == 10
    assert {row.attendance for row in result.matchsheet} == {None}
    assert (by_side["away"].formation, by_side["home"].formation) == ("4-4-2", None)
    extra = json.loads(result.extra_json)
    assert "leg" not in extra.get("headerSections", {}).get("competition", {})
    assert "headerSections" not in extra


@pytest.mark.unit
def test_unplayed_match_has_null_score() -> None:
    scoreboard = _load("native_scoreboard.json")
    scoreboard["events"][0]["status"] = {
        "type": {"name": "STATUS_SCHEDULED", "completed": False}
    }
    competition, edition, schedule = _schedule(scoreboard)
    assert not schedule[0].played_final

    result = parse_summary(
        _raw(_load("native_summary.json")),
        competition=competition,
        edition=edition,
        event=schedule[0],
    )

    assert [row.score for row in result.matchsheet] == [None, None]


@pytest.mark.unit
def test_key_event_flags_and_scores_are_read_when_present() -> None:
    payload = _load("native_summary.json")
    payload["keyEvents"] = [
        {
            "id": "7001",
            "type": {"id": "94", "text": "Red Card", "type": "red-card"},
            "period": {"number": 2},
            "clock": {"value": 5460.0, "displayValue": "90'+1'"},
            "team": {"id": "10", "displayName": "Home FC"},
            "participants": [{"athlete": {"id": "101", "displayName": "Home Player"}}],
            "scoringPlay": False,
            "redCard": True,
            "yellowCard": False,
            "penaltyKick": False,
            "ownGoal": False,
            "homeScore": 2,
            "awayScore": 1,
            "fieldPositionX": 0.5,
            "fieldPositionY": 0.25,
            "wallclock": "2020-09-19T20:40:00Z",
        }
    ]
    payload["commentary"] = [
        {"sequence": 0, "time": {"value": 0.0, "displayValue": ""}, "text": "Hello"}
    ]

    result = _summary(payload)

    key_event, commentary = result.events
    assert (key_event.kind, key_event.play_id, key_event.period) == (
        "key_event",
        "7001",
        2,
    )
    assert key_event.clock_display == "90'+1'"
    assert key_event.team_id == 10 and json.loads(key_event.athlete_ids) == [101]
    assert (key_event.red_card, key_event.yellow_card) == (True, False)
    assert (key_event.home_score, key_event.away_score) == (2, 1)
    assert (key_event.x, key_event.y) == (0.5, 0.25)
    assert json.loads(key_event.extra_json) == {
        "type": {"type": "red-card"},
        "wallclock": "2020-09-19T20:40:00Z",
    }
    assert (commentary.kind, commentary.sequence, commentary.text) == (
        "commentary",
        0,
        "Hello",
    )
    assert commentary.clock_display is None and commentary.play_id is None


@pytest.mark.unit
def test_matchsheet_uses_numeric_display_value_when_value_is_null() -> None:
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team["statistics"] = [{"name": "shots", "value": None, "displayValue": "12"}]

    result = _summary(payload)

    assert [row.total_shots for row in result.matchsheet] == ["12", "12"]


@pytest.mark.unit
@pytest.mark.parametrize("bad_value", [None, True, [12], {"value": 12}, "twelve"])
def test_matchsheet_malformed_stat_values_are_source_malformed(
    bad_value: object,
) -> None:
    payload = _load("native_summary.json")
    payload["boxscore"]["teams"][0]["statistics"][0]["value"] = bad_value

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.SOURCE_MALFORMED
    assert re.search("statistic.*value|scalar", result.reason or "")


@pytest.mark.unit
def test_unknown_structured_matchsheet_stats_remain_canonical_without_failing() -> None:
    payload = _load("native_summary.json")
    for team in payload["boxscore"]["teams"]:
        team["statistics"].append(
            {
                "name": "newProviderShape",
                "value": {"segments": [1, 2], "label": "experimental"},
            }
        )

    result = _summary(payload)

    assert all(row.total_shots is not None for row in result.matchsheet)
    assert all("newProviderShape" in row.statistics_json for row in result.matchsheet)


@pytest.mark.unit
def test_dual_lineup_stat_sources_and_mapping_shapes_populate_legacy_fields() -> None:
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        player = roster["roster"][0]
        player["stats"] = [{"name": "totalShots", "value": 4}]
        player["statistics"] = {
            "appearances": {"displayValue": "3"},
            "foulsCommitted": {"value": 2},
            "goalAssists": 1,
        }

    result = _summary(payload)

    assert all(row.total_shots == 4.0 for row in result.lineup)
    assert all(row.appearances == 3.0 for row in result.lineup)
    assert all(row.fouls_committed == 2.0 for row in result.lineup)
    assert all(row.goal_assists == 1.0 for row in result.lineup)
    assert all('"statistics"' in row.statistics_json for row in result.lineup)
    assert all('"stats"' in row.statistics_json for row in result.lineup)


@pytest.mark.unit
def test_conflicting_dual_lineup_stat_sources_are_source_malformed() -> None:
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        player = roster["roster"][0]
        player["stats"] = [{"name": "totalShots", "value": 4}]
        player["statistics"] = {"totalShots": 5}

    result = _summary(payload)

    assert result.disposition is SummaryDisposition.SOURCE_MALFORMED
    assert re.search("conflicting.*total_shots", result.reason or "")


@pytest.mark.unit
def test_versioned_stat_name_maps_populate_full_legacy_surfaces() -> None:
    from scrapers.espn.parser_contracts import (
        LINEUP_STAT_MAP_VERSION,
        MATCHSHEET_STAT_MAP_VERSION,
        MatchsheetRow,
    )

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

    result = _summary(payload)

    lineup = result.lineup[0]
    matchsheet = result.matchsheet[0]
    assert LINEUP_STAT_MAP_VERSION == "espn-lineup-stat-map-v1"
    assert MATCHSHEET_STAT_MAP_VERSION == "espn-matchsheet-stat-map-v2"
    assert set(LINEUP_STAT_NAME_MAP) == set(lineup_names)
    assert set(MATCHSHEET_STAT_NAME_MAP) == set(matchsheet_names) | {
        "fouls",
        "possession",
        "shots",
        "cornerKicks",
    }
    assert all(
        isinstance(getattr(lineup, target), float) for target in lineup_names.values()
    )
    assert all(
        isinstance(getattr(matchsheet, target), str)
        for target in matchsheet_names.values()
    )
    assert matchsheet.won_corners == "27"
    # Dead in ESPN itself (C6-F5): never reintroduce.
    dead = {
        "capacity",
        "referee_id",
        "goal_assists",
        "goal_difference",
        "goals_conceded",
        "total_goals",
        "corner_kicks",
    }
    assert not dead & set(MatchsheetRow.__dataclass_fields__)
    assert "captain" not in type(lineup).__dataclass_fields__


# --------------------------------------------------------------------------
# #1501: kickoff confirmation, all/scoreboard day, ESPN day, stale open.

PROBES = FIXTURES / "probes"


def _probe_bytes(name: str) -> bytes:
    return (PROBES / name).read_bytes()


def _esp1_scope():
    return _scope(
        espn_id=740,
        slug="esp.1",
        year=2026,
        start=date(2026, 6, 1),
        end=date(2027, 5, 31),
    )


@pytest.mark.unit
def test_matchday_placeholder_kickoff_is_not_confirmed() -> None:
    # Frozen schedule of 20.09.2026 (Trino, V2 review): all 10 esp.1 matches
    # of the round sit on "2026-09-20 18:00". Rebuilt as a scoreboard from the
    # real esp.1 day body with the placeholder time and a scheduled status.
    real = json.loads(_probe_bytes("scoreboard_esp1_20260920.json"))
    template = real["events"][0]
    events = []
    with (FIXTURES / "schedule_esp1_20260920_placeholder.csv").open() as handle:
        for line in handle:
            event_id, slug, kickoff = (part.strip('"') for part in line.strip().split(","))
            assert slug == "esp.1"
            event = deepcopy(template)
            event["id"] = event_id
            event["date"] = kickoff[:16].replace(" ", "T") + "Z"
            event["status"]["type"]["name"] = "STATUS_SCHEDULED"
            for side in event["competitions"][0]["competitors"]:
                side["score"] = None
            events.append(event)
    assert len(events) == 10
    placeholder = {**real, "events": events}
    competition, edition = _esp1_scope()

    rows = parse_scoreboards(
        [_raw(placeholder)],
        competition=competition,
        edition=edition,
        query_start=date(2026, 9, 20),
        query_end=date(2026, 9, 20),
    )

    assert len(rows) == 10
    assert not any(row.kickoff_confirmed for row in rows)

    # The same ten simultaneous kickoffs once the matches are under way are
    # factual, not a placeholder; three scheduled ones stay below the rule.
    for event in events:
        event["status"]["type"]["name"] = "STATUS_FIRST_HALF"
    for event in events[:3]:
        event["status"]["type"]["name"] = "STATUS_SCHEDULED"
    live = parse_scoreboards(
        [_raw({**real, "events": events})],
        competition=competition,
        edition=edition,
        query_start=date(2026, 9, 20),
        query_end=date(2026, 9, 20),
    )
    assert all(row.kickoff_confirmed for row in live)


@pytest.mark.unit
def test_real_esp1_day_with_distinct_kickoffs_is_confirmed() -> None:
    competition, edition = _esp1_scope()

    rows = parse_scoreboards(
        [_probe_bytes("scoreboard_esp1_20260920.json")],
        competition=competition,
        edition=edition,
        query_start=date(2026, 9, 20),
        query_end=date(2026, 9, 20),
    )

    assert len(rows) == 5
    assert len({row.kickoff for row in rows}) == 4
    assert all(row.kickoff_confirmed for row in rows)


@pytest.mark.unit
def test_time_valid_false_event_is_not_confirmed() -> None:
    # Event 732409 cut from all/scoreboard?dates=20260924 (the only
    # timeValid=false of 100 events that day).
    competition, edition = _scope(
        espn_id=20114, slug="x.20114", year=2025,
        start=date(2025, 1, 1), end=date(2026, 12, 31),
    )

    rows = parse_all_scoreboard_day(
        _probe_bytes("all_scoreboard_event_timevalid_false.json"),
        {20114: competition},
        date(2026, 9, 24),
    )

    (row,) = rows["x.20114"]
    assert row.event_id == 732409
    assert row.kickoff_confirmed is False


def _target_competitions() -> dict[int, Competition]:
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    targets: dict[int, Competition] = {}
    for row in denominator.rows.values():
        if not (row.in_target and row.live and row.espn_id):
            continue
        editions = tuple(
            Edition(
                year,
                f"{year} {row.slug}",
                date(year, 1, 1),
                date(year + 1, 12, 31),
                True,
                EntityCapabilities(
                    CapabilityState.UNKNOWN,
                    CapabilityState.UNKNOWN,
                    CapabilityState.UNKNOWN,
                ),
            )
            for year in (2025, 2026)
        )
        targets[row.espn_id] = Competition(
            row.espn_id, row.slug, row.name, Gender.MALE, AgeClass.SENIOR, True, editions
        )
    return targets


@pytest.mark.unit
def test_all_scoreboard_day_binds_events_to_targets_through_uid() -> None:
    # all/scoreboard?dates=20260923&limit=1000: 78 events, leagues[0] without
    # an id; 23 belong to 12 target tournaments, 55 to women's, NCAA and
    # friendly leagues outside the target set.
    raw = _probe_bytes("all_scoreboard_20260923.json")
    document = json.loads(raw)
    assert len(document["events"]) == 78
    assert "id" not in document["leagues"][0]
    targets = _target_competitions()
    assert len(targets) == 161

    by_slug = parse_all_scoreboard_day(raw, targets, date(2026, 9, 23))

    assert set(by_slug) == {competition.slug for competition in targets.values()}
    counts = {slug: len(rows) for slug, rows in by_slug.items() if rows}
    assert counts == {
        "ned.cup": 6, "concacaf.nations.league": 3, "chi.copa_chi": 3,
        "slv.1": 2, "global.gulf_cup": 2, "usa.1": 1, "usa.usl.1": 1,
        "eng.fa_qual": 1, "sco.challenge": 1, "col.1": 1, "per.1": 1, "gua.1": 1,
    }
    rows = [row for group in by_slug.values() for row in group]
    for row in rows:
        assert row.competition_slug in counts
        assert f"~l:{row.competition_id}~e:{row.event_id}" in json.dumps(document)
    assert {row.status for row in rows} <= {"STATUS_FULL_TIME", "STATUS_FINAL_PEN"}
    # Every kickoff falls on ESPN day 23.09 (US Eastern), not on UTC 23.09.
    assert {espn_day(row.kickoff) for row in rows} == {date(2026, 9, 23)}


@pytest.mark.unit
def test_all_scoreboard_uid_without_league_fails_closed() -> None:
    document = json.loads(_probe_bytes("all_scoreboard_event_timevalid_false.json"))
    document["events"][0]["uid"] = "s:600~e:732409"
    with pytest.raises(EspnParseError, match="has no league"):
        parse_all_scoreboard_day(_raw(document), {}, date(2026, 9, 24))


@pytest.mark.unit
def test_espn_day_is_the_us_eastern_calendar_day() -> None:
    assert espn_day(datetime(2026, 9, 21, 2, 30, tzinfo=timezone.utc)) == date(2026, 9, 20)
    assert espn_day(datetime(2026, 9, 23, 4, 0, tzinfo=timezone.utc)) == date(2026, 9, 23)
    assert espn_day(datetime(2026, 1, 10, 4, 30, tzinfo=timezone.utc)) == date(2026, 1, 9)


@pytest.mark.unit
def test_postponed_matches_become_stale_after_three_days() -> None:
    # eng.1 13.08.2005: events 184196 and 184203 stayed STATUS_POSTPONED.
    competition, edition = _scope(
        espn_id=700, slug="eng.1", year=2005,
        start=date(2005, 8, 1), end=date(2006, 5, 31),
    )
    rows = parse_scoreboards(
        [_probe_bytes("scoreboard_eng1_20050813_postponed.json")],
        competition=competition,
        edition=edition,
        query_start=date(2005, 8, 13),
        query_end=date(2005, 8, 13),
    )
    kickoff = datetime(2005, 8, 13, 14, 0, tzinfo=timezone.utc)
    postponed = [row for row in rows if row.status == "STATUS_POSTPONED"]
    assert [row.event_id for row in postponed] == [184196, 184203]
    assert all(row.kickoff_confirmed for row in rows)

    assert stale_open_events(rows, kickoff + timedelta(days=2)) == ()
    assert stale_open_events(rows, kickoff + timedelta(days=3, seconds=1)) == (
        184196,
        184203,
    )
