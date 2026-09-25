"""Edition lifecycle from recorded ESPN core bodies: no manual season switch (#1501)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
from pathlib import Path

import pytest

from scrapers.espn import editions as editions_module
from scrapers.espn.core_lists import (
    collect_refs,
    event_ids,
    parse_league_current_season,
    parse_season,
    parse_types,
)
from scrapers.espn.editions import (
    EditionState,
    dedupe_events,
    duplicate_of,
    edition_list_requests,
    fill_edition,
    plan_editions,
)
from scrapers.espn.models import SeasonType
from scrapers.espn.parser_common import EspnParseError

PROBES = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes"


def _json(name: str):
    return json.loads((PROBES / name).read_text(encoding="utf-8"))


def _state(slug: str, year: int, **kwargs) -> EditionState:
    return EditionState(slug, year, f"{year} {slug}", date(year, 1, 1), date(year, 12, 31), **kwargs)


@pytest.mark.unit
def test_calendar_league_opens_the_new_year_from_the_core_detail() -> None:
    # Recorded calendar-year detail: season 2025, 2025-01-01T05:00Z ..
    # 2026-01-01T04:59Z (the ET year 2025). The registry knows only 2024.
    current = parse_league_current_season(_json("league_detail_fifa.world.u20.json"))
    known = [_state("fifa.world.u20", 2024)]

    unfinished = plan_editions(
        current, known, {2024: False}, competition_slug="fifa.world.u20"
    )
    assert [(state.year, state.display_name, state.open) for state in unfinished.open_now] == [
        (2025, "2025 FIFA U20 World Cup", True)
    ]
    assert (unfinished.open_now[0].start, unfinished.open_now[0].end) == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )
    assert unfinished.close_now == ()
    assert [state.year for state in unfinished.keep] == [2024]

    finished = plan_editions(current, known, {2024: True}, competition_slug="fifa.world.u20")
    assert [state.year for state in finished.close_now] == [2024]
    assert not finished.close_now[0].open


@pytest.mark.unit
def test_autumn_spring_league_rolls_over_and_closes_only_when_all_terminal() -> None:
    detail = _json("league_detail_sui.1.json")
    recorded = parse_league_current_season(detail)
    assert recorded.year == 2025
    known = [
        EditionState(
            "sui.1", 2025, recorded.display_name, recorded.start, recorded.end
        )
    ]
    # Same season: nothing to do.
    same = plan_editions(recorded, known, {}, competition_slug="sui.1")
    assert (same.open_now, same.close_now, same.keep) == ((), (), tuple(known))

    # ESPN moves the detail to 2026 (only ``season`` changed in the body).
    detail["season"] = {
        **detail["season"],
        "year": 2026,
        "displayName": "2026-27 Swiss Super League",
        "startDate": "2026-06-01T04:00Z",
        "endDate": "2027-06-01T03:59Z",
    }
    new = parse_league_current_season(detail)

    open_both = plan_editions(new, known, {2025: False}, competition_slug="sui.1")
    assert [state.key for state in open_both.open_now] == ["sui.1:2026"]
    assert open_both.open_now[0].display_name == "2026-27 Swiss Super League"
    assert open_both.close_now == ()
    assert [state.key for state in open_both.keep] == ["sui.1:2025"]

    rolled = plan_editions(new, known, {2025: True}, competition_slug="sui.1")
    assert [state.key for state in rolled.close_now] == ["sui.1:2025"]
    # A year missing from the terminal map is not finished.
    assert plan_editions(new, known, {}, competition_slug="sui.1").close_now == ()


@pytest.mark.unit
def test_closed_edition_that_core_still_calls_current_reopens() -> None:
    current = parse_league_current_season(_json("league_detail_sui.1.json"))
    closed = _state("sui.1", 2025, open=False)

    plan = plan_editions(current, [closed], {}, competition_slug="sui.1")

    assert [(state.key, state.open) for state in plan.open_now] == [("sui.1:2025", True)]
    assert plan.keep == ()


@pytest.mark.unit
def test_cup_edition_lists_every_type_and_never_the_typeless_season() -> None:
    _, types_2010 = parse_season(_json("season_uefa.champions_2010.json"))
    assert len(types_2010) == 9
    requests = edition_list_requests("uefa.champions", 2010, types_2010)
    assert len(requests) == 10
    assert all("/seasons/2010/events" not in request.url for request in requests)
    assert [request.url.rsplit("/types/", 1)[-1] for request in requests[1:]] == [
        f"{number}/events" for number in range(1, 10)
    ]

    types_2026 = parse_types(_json("types_uefa.champions_2026.json"))
    assert len(types_2026) == 6
    league_phase = event_ids(
        collect_refs(lambda n: (PROBES / "type_events_uefa.champions_2026_t1.json").read_bytes())
    )
    # Types 2..6 (knockout rounds) are not drawn yet: empty lists.
    events_by_type = {1: league_phase, **{item.id: () for item in types_2026[1:]}}
    edition = fill_edition(_state("uefa.champions", 2026), types_2026, events_by_type)

    assert edition.types == types_2026
    assert len(edition.event_ids) == 144

    group_stage = event_ids(
        collect_refs(lambda n: (PROBES / "core_events_ucl_2010_type5.json").read_bytes())
    )
    filled_2010 = fill_edition(
        _state("uefa.champions", 2010),
        types_2010,
        {item.id: (group_stage if item.id == 5 else ()) for item in types_2010},
    )
    assert len(filled_2010.event_ids) == 96

    with pytest.raises(EspnParseError, match="no event list for types"):
        fill_edition(_state("uefa.champions", 2026), types_2026, {1: league_phase})


@pytest.mark.unit
def test_season_without_types_yet_is_open_with_an_empty_denominator() -> None:
    types = parse_types(_json("types_eng.fa_2026_empty.json"))

    edition = fill_edition(_state("eng.fa", 2026), types, {})

    assert edition.open and edition.types == () and edition.event_ids == frozenset()
    assert len(edition_list_requests("eng.fa", 2026, types)) == 1


@pytest.mark.unit
def test_same_event_in_main_and_qualifying_slug_is_a_duplicate() -> None:
    # In 2010 the qualifying rounds lived inside uefa.champions; today they are
    # a separate slug. Synthetic overlap on real 2010 group-stage ids.
    group_stage = event_ids(
        collect_refs(lambda n: (PROBES / "core_events_ucl_2010_type5.json").read_bytes())
    )
    qualifying = _state("uefa.champions_qual", 2010, event_ids=frozenset(group_stage[:3]) | {9})
    main = _state("uefa.champions", 2010, event_ids=frozenset(group_stage))

    owners = dedupe_events([qualifying, main])  # order given: qualifying first

    assert owners == {event_id: "uefa.champions:2010" for event_id in sorted(group_stage[:3])}
    assert duplicate_of(qualifying, group_stage[0], owners) == "uefa.champions:2010"
    assert duplicate_of(main, group_stage[0], owners) is None
    assert duplicate_of(qualifying, 9, owners) is None


@pytest.mark.unit
def test_no_frozen_scope_constants_remain() -> None:
    source = Path(editions_module.__file__).read_text(encoding="utf-8")
    assert "181" not in source and "300" not in source


@pytest.mark.unit
def test_known_editions_must_not_repeat_a_year() -> None:
    current = parse_league_current_season(_json("league_detail_sui.1.json"))
    twin = _state("sui.1", 2024)
    with pytest.raises(EspnParseError, match="repeat a year"):
        plan_editions(current, [twin, replace(twin)], {}, competition_slug="sui.1")


@pytest.mark.unit
def test_season_type_ids_are_positive() -> None:
    with pytest.raises(ValueError):
        SeasonType(0)
