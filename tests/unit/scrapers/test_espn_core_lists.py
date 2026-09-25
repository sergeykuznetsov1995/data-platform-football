"""Core list pages, seasons, types and event status on recorded ESPN bodies (#1501)."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pytest

from scrapers.espn.core_lists import (
    collect_refs,
    event_id_from_ref,
    event_ids,
    parse_event_status,
    parse_league_current_season,
    parse_ref_page,
    parse_season,
    parse_types,
)
from scrapers.espn.parser_common import EspnParseError
from scrapers.espn.schedule_parser import STATUS_MAP

PROBES = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes"
ENG_FA_SEASONS = "seasons_eng.fa_page0_limit3.json"


def _bytes(name: str) -> bytes:
    return (PROBES / name).read_bytes()


def _json(name: str):
    return json.loads(_bytes(name))


def _raw(value) -> bytes:
    return json.dumps(value).encode()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "count"),
    [
        ("seasons_uefa.champions.json", 26),
        ("types_uefa.champions_2026.json", 6),
        ("type_events_uefa.champions_2026_t1.json", 144),
        ("type_events_fifa.world_2010_t1.json", 48),
        ("events_window_eng1_30d.json", 30),
        ("core_events_eng1_2025.json", 380),
        ("core_events_ucl_2010_type5.json", 96),
        ("leagues_core.json", 219),
        ("types_eng.fa_2026_empty.json", 0),
    ],
)
def test_every_recorded_single_page_list_is_complete(name: str, count: int) -> None:
    page = parse_ref_page(_json(name))
    assert page.count == count == len(page.refs)
    assert collect_refs(lambda number: _bytes(name)) == page.refs


def _eng_fa_pages() -> dict[int, bytes]:
    """Page 1 is the recorded body; pages 2..9 are synthetic of the same form."""

    first = _json(ENG_FA_SEASONS)
    assert (first["count"], first["pageSize"], first["pageCount"]) == (25, 3, 9)
    template = first["items"][0]["$ref"]
    pages = {1: _bytes(ENG_FA_SEASONS)}
    years = iter(range(2022, 2022 - 22, -1))
    for number in range(2, 10):
        size = 3 if number < 9 else 1
        items = [
            {"$ref": template.replace("/seasons/2025", f"/seasons/{next(years)}")}
            for _ in range(size)
        ]
        pages[number] = _raw({**first, "pageIndex": number, "items": items})
    return pages


@pytest.mark.unit
def test_list_longer_than_one_page_is_walked_to_the_last_page() -> None:
    pages = _eng_fa_pages()
    asked: list[int] = []

    def fetch(number: int) -> bytes:
        asked.append(number)
        return pages[number]

    refs = collect_refs(fetch)

    assert asked == list(range(1, 10))
    assert len(refs) == 25
    assert refs[0].endswith("/eng.fa/seasons/2025?lang=en&region=us")


@pytest.mark.unit
def test_missing_page_or_short_total_fails_closed() -> None:
    pages = _eng_fa_pages()
    # A page that repeats page 1 instead of page 5.
    with pytest.raises(EspnParseError, match="pageIndex"):
        collect_refs(lambda number: pages[1] if number == 5 else pages[number])
    # The last page lost its item: count=25 but 24 refs.
    short = {**pages, 9: _raw({**json.loads(pages[9]), "items": []})}
    with pytest.raises(EspnParseError, match="count=25"):
        collect_refs(lambda number: short[number])
    # A page changes the total mid-walk.
    moved = {**pages, 3: _raw({**json.loads(pages[3]), "count": 26})}
    with pytest.raises(EspnParseError, match="changed count"):
        collect_refs(lambda number: moved[number])
    # One page that does not hold its count.
    single = {**_json("seasons_uefa.champions.json"), "count": 27}
    with pytest.raises(EspnParseError, match="count=27"):
        parse_ref_page(single)


@pytest.mark.unit
def test_empty_types_list_is_legitimate() -> None:
    # eng.fa season 2026 is not opened by ESPN yet: count 0, pageCount 0.
    assert parse_types(_json("types_eng.fa_2026_empty.json")) == ()
    assert collect_refs(lambda number: _bytes("types_eng.fa_2026_empty.json")) == ()


@pytest.mark.unit
def test_league_detail_gives_the_current_season_and_stage() -> None:
    season = parse_league_current_season(_json("league_detail_uefa.champions.json"))

    assert season.year == 2026
    assert season.display_name == "2026-27 UEFA Champions League"
    assert (season.type_id, season.type_name) == (1, "League Phase")
    # 2026-07-01T04:00Z .. 2027-07-01T03:59Z are ESPN (US Eastern) days.
    assert (season.start, season.end) == (date(2026, 7, 1), date(2027, 6, 30))

    autumn_spring = parse_league_current_season(_json("league_detail_sui.1.json"))
    assert (autumn_spring.year, autumn_spring.display_name) == (
        2025,
        "2025-26 Swiss Super League",
    )


@pytest.mark.unit
def test_season_2010_embeds_nine_types_and_2026_lists_six() -> None:
    season, types = parse_season(_json("season_uefa.champions_2010.json"))

    assert season.year == 2010
    assert [item.id for item in types] == list(range(1, 10))
    assert types[0].name == "Qualifying First Round"
    assert types[0].start_date == date(2010, 6, 29)
    assert types[-1].name == "Final"

    current = parse_types(_json("types_uefa.champions_2026.json"))
    assert [item.id for item in current] == [1, 2, 3, 4, 5, 6]
    assert all(item.name is None for item in current)  # $ref-only list


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "count", "first"),
    [
        ("type_events_uefa.champions_2026_t1.json", 144, 401915264),
        ("type_events_fifa.world_2010_t1.json", 48, 264031),
    ],
)
def test_type_events_yield_native_event_ids(name: str, count: int, first: int) -> None:
    ids = event_ids(collect_refs(lambda number: _bytes(name)))

    assert len(ids) == len(set(ids)) == count
    assert ids[0] == first


@pytest.mark.unit
def test_event_ref_parsing_is_strict() -> None:
    assert event_id_from_ref(
        "http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/740780?lang=en"
    ) == 740780
    with pytest.raises(EspnParseError, match="event"):
        event_id_from_ref("http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1")


@pytest.mark.unit
def test_event_status_name_is_read_and_known() -> None:
    name = parse_event_status(_json("event_status_first_half.json"))

    assert name == "STATUS_FIRST_HALF"
    assert name in STATUS_MAP


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "parser", "code"),
    [
        ("events_nodtype_404.json", parse_ref_page, "404"),
        ("events_window_eng1_395d_400.json", parse_ref_page, "400"),
        ("scoreboard_range_400.json", parse_ref_page, "400"),
        ("events_nodtype_404.json", parse_league_current_season, "404"),
        ("events_nodtype_404.json", parse_season, "404"),
        ("events_nodtype_404.json", parse_event_status, "404"),
    ],
)
def test_espn_error_bodies_fail_with_their_code(name, parser, code) -> None:
    with pytest.raises(EspnParseError, match=f"ESPN error {code}"):
        parser(_json(name))
