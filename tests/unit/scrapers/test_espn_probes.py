"""ESPN core parsers against real recorded ESPN responses (#1498, #1502).

Bodies are verbatim copies of the 24.09.2026 review probes; the source URL of
every file is listed in ``tests/fixtures/espn/probes/README.md``.  No network.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

import pytest

from scrapers.espn.models import CapabilityState
from scrapers.espn.parsers import (
    EntityParseState,
    SummaryDisposition,
    parse_scoreboards,
    parse_summary,
)
from tests.unit.scrapers.test_espn_parsers import _raw, _scope


PROBES = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes"


def _bytes(name: str) -> bytes:
    return (PROBES / name).read_bytes()


def _json(name: str) -> dict:
    return json.loads(_bytes(name))


def _summary_context(payload: dict):
    """Competition/Edition/ScheduleRow for the event described by the header.

    The schedule row goes through the real scoreboard parser: a one-event
    scoreboard is assembled from the summary header fields only.
    """
    header = payload["header"]
    competition_raw = header["competitions"][0]
    year = header["season"]["year"]
    scoreboard = {
        "leagues": [
            {"id": header["league"]["id"], "slug": header["league"]["slug"]}
        ],
        "events": [
            {
                "id": header["id"],
                "date": competition_raw["date"],
                "season": {"year": year},
                "status": competition_raw["status"],
                "competitions": [
                    {
                        "competitors": [
                            {
                                "homeAway": side["homeAway"],
                                "score": side.get("score"),
                                "team": {
                                    "id": side["team"]["id"],
                                    "displayName": side["team"]["displayName"],
                                },
                            }
                            for side in competition_raw["competitors"]
                        ]
                    }
                ],
            }
        ],
    }
    competition, edition = _scope(
        espn_id=int(header["league"]["id"]),
        slug=header["league"]["slug"],
        year=year,
        start=date(year, 1, 1),
        end=date(year + 1, 12, 31),
        lineup=CapabilityState.UNKNOWN,
        matchsheet=CapabilityState.UNKNOWN,
    )
    (event,) = parse_scoreboards(
        [_raw(scoreboard)],
        competition=competition,
        edition=edition,
        query_start=edition.start_date,
        query_end=edition.end_date,
    )
    return competition, edition, event


def _parse(name: str):
    raw = _bytes(name)
    competition, edition, event = _summary_context(json.loads(raw))
    return event, parse_summary(
        raw, competition=competition, edition=edition, event=event
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "event_id", "score", "lineup_rows"),
    [
        # eng.1 2020-21, Arsenal 2-0 Brighton (top league).
        ("summary_eng1_2020.json", 578281, (2, 0), 40),
        # ger.2 2016-17, Greuther Fürth 1-0 Nürnberg (second tier).
        ("summary_ger2_2016.json", 456996, (1, 0), 36),
        # eng.1 2005-06, Everton 0-2 Manchester United.
        ("summary_eng1_2005.json", 184188, (0, 2), 32),
    ],
)
def test_real_summary_is_captured_with_two_starting_elevens(
    name: str, event_id: int, score: tuple[int, int], lineup_rows: int
) -> None:
    event, result = _parse(name)

    assert event.status == "STATUS_FULL_TIME"
    assert event.played_final
    assert (event.home_score, event.away_score) == score
    assert result.event_id == event_id
    assert result.disposition is SummaryDisposition.CAPTURED
    assert result.lineup_state is EntityParseState.CAPTURED
    assert len(result.lineup) == lineup_rows
    starters: dict[int, int] = {}
    for row in result.lineup:
        starters[row.team_id] = starters.get(row.team_id, 0) + (row.starter is True)
    assert sorted(starters.values()) == [11, 11]
    assert result.matchsheet_state is EntityParseState.CAPTURED
    assert len(result.matchsheet) == 2


# Nine recorded Summary bodies (#1502): five history probes c3/recon and four
# C6-F1 cases; every one gets a disposition, none raises.
_DISPOSITIONS = [
    # name, disposition, anomalies, lineup state, matchsheet state,
    # lineup rows, starters by team, formation on both teams
    ("summary_eng1_2005.json", "captured", (), "captured", "captured", 32,
     {368: 11, 360: 11}, False),
    # Fulham has 13 rows flagged starter.
    ("summary_eng1_2010.json", "lineup_anomaly", ("starters_not_11",), "captured",
     "captured", 38, {382: 11, 370: 13}, True),
    # Auxerre has 12 rows flagged starter.
    ("summary_ucl_2010.json", "lineup_anomaly", ("starters_not_11",), "captured",
     "captured", 37, {103: 11, 172: 12}, True),
    # World Cup 2010: ``roster: []`` for both teams, statistics present.
    ("summary_fifaworld_2010.json", "captured", (), "valid_empty", "captured", 0,
     {}, True),
    ("summary_ger2_2016.json", "captured", (), "captured", "captured", 36,
     {3070: 11, 269: 11}, True),
    ("summary_eng1_2020.json", "captured", (), "captured", "captured", 40,
     {359: 11, 331: 11}, True),
    # uru.1 401905201: 10 + 11 starters, no formationPlace, no statistics.
    ("summary_uru1_2026_ten_starters.json", "lineup_anomaly", ("starters_not_11",),
     "captured", "valid_empty", 40, {9902: 10, 8416: 11}, False),
    # jpn.1 401877180: FC Tokyo fields 10 starters.
    ("summary_jpn1_2026_ten_starters.json", "lineup_anomaly", ("starters_not_11",),
     "captured", "captured", 39, {3384: 10, 22167: 11}, True),
    # arg.2 401844030: athlete 408183 is both starter and subbed in.
    ("summary_arg2_2026_contradictory_flag.json", "lineup_anomaly",
     ("contradictory_flags",), "captured", "captured", 39, {10743: 11, 236: 11},
     True),
    # gua.1 401879625: no ``roster`` key, no statistics - the honest empty.
    ("summary_gua1_2026_no_roster.json", "valid_empty", (), "valid_empty",
     "valid_empty", 0, {}, False),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    (
        "name",
        "disposition",
        "anomalies",
        "lineup_state",
        "matchsheet_state",
        "lineup_rows",
        "starters",
        "formation",
    ),
    _DISPOSITIONS,
)
def test_real_summary_gets_a_disposition_without_raising(
    name: str,
    disposition: str,
    anomalies: tuple[str, ...],
    lineup_state: str,
    matchsheet_state: str,
    lineup_rows: int,
    starters: dict[int, int],
    formation: bool,
) -> None:
    event, result = _parse(name)
    payload = _json(name)

    assert result.disposition is SummaryDisposition(disposition)
    assert result.reason is None
    assert result.anomalies == anomalies
    assert result.lineup_state is EntityParseState(lineup_state)
    assert result.matchsheet_state is EntityParseState(matchsheet_state)
    assert len(result.lineup) == lineup_rows
    counted: dict[int, int] = {}
    for row in result.lineup:
        counted[row.team_id] = counted.get(row.team_id, 0) + (row.starter is True)
    assert counted == starters
    # Shirt numbers live on the roster entry, not on the athlete (C6-F5).
    assert sum(row.jersey is not None for row in result.lineup) == lineup_rows
    assert len(result.matchsheet) == (2 if matchsheet_state == "captured" else 0)
    for row in result.matchsheet:
        assert (row.formation is not None) is formation
        assert row.score == (
            event.home_score if row.is_home else event.away_score
        )
        if row.score_h1 is not None:
            assert row.score_h1 + row.score_h2 == row.score
    # Every keyEvents and commentary record becomes one event row.
    assert len(result.events) == len(payload.get("keyEvents") or []) + len(
        payload.get("commentary") or []
    )
    # Commentary lines share a play (a foul and the free kick it wins), so
    # their key is the sequence; bodies without sequence fall back to play_id.
    keys = {
        (
            row.kind,
            row.play_id
            if row.kind == "key_event" or row.sequence is None
            else row.sequence,
        )
        for row in result.events
    }
    assert len(keys) == len(result.events)
    # Only unknown keys inside parsed blocks; news/videos/odds/... are dropped.
    assert set(json.loads(result.extra_json)) <= {
        "header",
        "headerSections",
        "boxscore",
        "rosters",
        "gameInfo",
    }


@pytest.mark.unit
def test_real_summary_referee_linescores_and_attendance() -> None:
    _, eng1_2010 = _parse("summary_eng1_2010.json")
    # Three officials, all "Referee", order 1-3: the first ordered one wins.
    assert {row.referee for row in eng1_2010.matchsheet} == {"Peter Walton"}
    assert [(row.score_h1, row.score_h2) for row in eng1_2010.matchsheet] == [
        (1, 0),
        (0, 1),
    ]

    _, eng1_2005 = _parse("summary_eng1_2005.json")
    assert {row.referee for row in eng1_2005.matchsheet} == {"G Poll"}
    assert {(row.score_h1, row.score_h2, row.score_et) for row in eng1_2005.matchsheet} == {
        (None, None, None)
    }
    assert {row.formation for row in eng1_2005.matchsheet} == {None}

    _, ucl_2010 = _parse("summary_ucl_2010.json")
    # ESPN reports attendance 0 when it does not know it.
    assert {row.attendance for row in ucl_2010.matchsheet} == {None}

    _, eng1_2020 = _parse("summary_eng1_2020.json")
    assert {row.attendance for row in eng1_2020.matchsheet} == {10000}
    assert {row.formation for row in eng1_2020.matchsheet} == {"4-2-3-1", "4-3-3"}
    goal = next(
        row
        for row in eng1_2020.events
        if row.kind == "key_event" and row.type_text == "Goal"
    )
    assert goal.scoring_play is True
    assert goal.team_id is not None and json.loads(goal.athlete_ids)
    assert goal.clock_display is not None
    # Summary events carry no card/penalty/own-goal flags (core plays do).
    assert {row.red_card for row in eng1_2020.events} == {None}
    commentary = [row for row in eng1_2020.events if row.kind == "commentary"]
    assert {row.team_id for row in commentary} == {None}
    assert any('"participants"' in row.extra_json for row in commentary)


@pytest.mark.unit
def test_real_day_scoreboard_yields_every_event_of_the_day() -> None:
    competition, edition = _scope(
        espn_id=700,
        slug="eng.1",
        year=2026,
        start=date(2026, 6, 1),
        end=date(2027, 5, 31),
    )
    rows = parse_scoreboards(
        [_bytes("scoreboard_eng1_day.json")],
        competition=competition,
        edition=edition,
        query_start=date(2026, 9, 20),
        query_end=date(2026, 9, 20),
    )

    assert len(rows) == 4
    assert {row.status for row in rows} == {"STATUS_FULL_TIME"}
    assert all(row.played_final for row in rows)
    assert [row.event_id for row in rows] == [
        401879272,
        401879273,
        401879276,
        401878777,
    ]
    first = rows[0]
    assert first.kickoff == datetime(2026, 9, 20, 13, 0, tzinfo=timezone.utc)
    assert (first.home_team, first.away_team) == ("Manchester City", "Sunderland")
    assert (first.home_score, first.away_score) == (5, 3)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "count", "ref_prefix"),
    [
        (
            "core_events_eng1_2025.json",
            380,
            "http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/",
        ),
        (
            "core_events_ucl_2010_type5.json",
            96,
            "http://sports.core.api.espn.com/v2/sports/soccer/leagues/"
            "uefa.champions/events/",
        ),
        (
            "leagues_core.json",
            219,
            "http://sports.core.api.espn.com/v2/sports/soccer/leagues/",
        ),
    ],
)
def test_real_core_list_page_shape(name: str, count: int, ref_prefix: str) -> None:
    # Contract for #1499/#1501: one page carries the whole list as $ref links.
    # Lists longer than one page: test_espn_core_lists.py (eng.fa, pageCount 9).
    page = _json(name)

    assert page["count"] == count == len(page["items"])
    assert page["pageIndex"] == 1
    assert page["pageCount"] == 1
    assert page["pageSize"] >= count
    refs = [item["$ref"] for item in page["items"]]
    assert all(set(item) == {"$ref"} for item in page["items"])
    assert all(ref.startswith(ref_prefix) for ref in refs)
    assert len(set(refs)) == count
