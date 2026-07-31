"""Frozen, network-free contracts for ESPN native offline parsing."""

from __future__ import annotations

from copy import deepcopy
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
    assert (row.venue_id, row.venue, row.attendance) == (99, "Native Ground", 1000)
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
        ("STATUS_POSTPONED", True, False, True),
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
    assert result.lineup[0].sub_in == "start"
    assert result.lineup[0].sub_out == "end"
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
def test_conventional_xi_requires_22_unique_starters() -> None:
    competition, edition, schedule = _schedule()
    payload = _load("native_summary.json")
    for roster in payload["rosters"]:
        seed = roster["roster"][0]
        base_id = 100 if roster["homeAway"] == "home" else 200
        roster["roster"] = []
        for offset in range(1, 12):
            player = deepcopy(seed)
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
