"""ESPN core parsers against real recorded ESPN responses (#1498).

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
    EspnParseError,
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
    assert result.lineup_state is EntityParseState.CAPTURED
    assert len(result.lineup) == lineup_rows
    starters: dict[int, int] = {}
    for row in result.lineup:
        starters[row.team_id] = starters.get(row.team_id, 0) + (row.starter is True)
    assert sorted(starters.values()) == [11, 11]
    assert result.matchsheet_state is EntityParseState.CAPTURED
    assert len(result.matchsheet) == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "message"),
    [
        # eng.1 2010-11, Man City 1-1 Fulham: Fulham has 13 rows flagged starter.
        (
            "summary_eng1_2010.json",
            r"11 starters per team for event 292828; got \{382: 11, 370: 13\}",
        ),
        # UCL 2010-11, AC Milan 2-0 Auxerre: Auxerre has 12 rows flagged starter.
        (
            "summary_ucl_2010.json",
            r"11 starters per team for event 307787; got \{103: 11, 172: 12\}",
        ),
        # World Cup 2010, South Africa 1-1 Mexico: both roster lists are empty.
        ("summary_fifaworld_2010.json", "team roster must not be empty"),
    ],
)
def test_real_2010_summary_fails_strictly_until_disposition(
    name: str, message: str
) -> None:
    # #1502 moves these real 2010 shapes to source_malformed / lineup_anomaly
    # instead of an exception; until then the strict parser must refuse them.
    with pytest.raises(EspnParseError, match=message):
        _parse(name)


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
    # A body with pageCount > 1 has not been recorded yet (#1501).
    page = _json(name)

    assert page["count"] == count == len(page["items"])
    assert page["pageIndex"] == 1
    assert page["pageCount"] == 1
    assert page["pageSize"] >= count
    refs = [item["$ref"] for item in page["items"]]
    assert all(set(item) == {"$ref"} for item in page["items"])
    assert all(ref.startswith(ref_prefix) for ref in refs)
    assert len(set(refs)) == count
