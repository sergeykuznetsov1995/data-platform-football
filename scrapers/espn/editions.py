"""Edition lifecycle from ESPN core, without a manual season switch (#1501).

Every run reads the league's current season from ``leagues/{slug}`` (R-38).
A season year the registry has not seen opens a new edition; an older open
edition closes only once the caller reports every one of its matches terminal.
Both can be open at once.  A season is split into stages (``types``): the
edition's match list is the union of the event ids of all its types — ESPN has
no season-level event list without a type (404, R-32).  An event id is unique
across ESPN: the same match listed under a second tournament (a qualifying
round inside the main competition) is marked as a duplicate of the first.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from typing import Iterable, Mapping, Sequence

from . import urls
from .core_lists import CoreSeason
from .models import SeasonType
from .parser_common import EspnParseError


@dataclass(frozen=True, slots=True)
class EditionState:
    competition_slug: str
    year: int
    display_name: str
    start: date
    end: date
    types: tuple[SeasonType, ...] = ()
    event_ids: frozenset[int] = field(default_factory=frozenset)
    open: bool = True

    @property
    def key(self) -> str:
        return f"{self.competition_slug}:{self.year}"


@dataclass(frozen=True, slots=True)
class EditionPlan:
    open_now: tuple[EditionState, ...]
    close_now: tuple[EditionState, ...]
    keep: tuple[EditionState, ...]


def plan_editions(
    current: CoreSeason,
    known: Sequence[EditionState],
    schedule_terminal: Mapping[int, bool],
    *,
    competition_slug: str,
) -> EditionPlan:
    """Open the core's current season if new; close older ones once finished.

    ``schedule_terminal[year]`` is True when every match of that edition is
    terminal; a missing year counts as not finished.
    """

    known = [state for state in known if state.competition_slug == competition_slug]
    if len({state.year for state in known}) != len(known):
        raise EspnParseError(f"{competition_slug}: known editions repeat a year")
    open_now: list[EditionState] = []
    close_now: list[EditionState] = []
    keep: list[EditionState] = []
    same = next((state for state in known if state.year == current.year), None)
    if same is None:
        open_now.append(
            EditionState(
                competition_slug,
                current.year,
                current.display_name,
                current.start,
                current.end,
            )
        )
    elif not same.open:
        # Core still calls it current: it is not finished after all.
        open_now.append(replace(same, open=True))
    for state in known:
        if state is same and not state.open:
            continue
        if state.open and state.year < current.year and schedule_terminal.get(
            state.year, False
        ):
            close_now.append(replace(state, open=False))
        else:
            keep.append(state)
    return EditionPlan(tuple(open_now), tuple(close_now), tuple(keep))


def edition_list_requests(
    slug: str, year: int, types: Iterable[SeasonType]
) -> tuple[urls.EspnRequest, ...]:
    """Requests that list one edition: its types, then events per type.

    Never ``seasons/{year}/events`` without a type — ESPN answers it with 404.
    """

    return (urls.season_types(slug, year),) + tuple(
        urls.type_events(slug, year, item.id) for item in types
    )


def fill_edition(
    state: EditionState,
    types: Sequence[SeasonType],
    events_by_type: Mapping[int, Iterable[int]],
) -> EditionState:
    """Edition with its types and the union of their event ids.

    Zero types is legitimate (ESPN has not opened the stages yet); a type
    without its event list is not.
    """

    missing = [item.id for item in types if item.id not in events_by_type]
    if missing:
        raise EspnParseError(f"{state.key}: no event list for types {missing}")
    extra = set(events_by_type) - {item.id for item in types}
    if extra:
        raise EspnParseError(f"{state.key}: event lists for unknown types {extra}")
    ids: set[int] = set()
    for item in types:
        ids.update(events_by_type[item.id])
    return replace(state, types=tuple(types), event_ids=frozenset(ids))


def _priority(indexed: tuple[int, EditionState]) -> tuple[bool, int]:
    index, state = indexed
    return state.competition_slug.endswith("_qual"), index


def dedupe_events(editions: Sequence[EditionState]) -> Mapping[int, str]:
    """``event_id -> owner edition key`` for ids listed by several editions.

    The owner is the first edition in the given order, a main competition
    before its qualifying slug; every other edition's row for that id gets
    ``duplicate_of = owner``.
    """

    owners: dict[int, str] = {}
    duplicated: set[int] = set()
    for _, state in sorted(enumerate(editions), key=_priority):
        for event_id in state.event_ids:
            if event_id in owners:
                duplicated.add(event_id)
            else:
                owners[event_id] = state.key
    return {event_id: owners[event_id] for event_id in sorted(duplicated)}


def duplicate_of(state: EditionState, event_id: int, owners: Mapping[int, str]) -> str | None:
    owner = owners.get(event_id)
    return owner if owner is not None and owner != state.key else None


__all__ = [
    "EditionPlan",
    "EditionState",
    "dedupe_events",
    "duplicate_of",
    "edition_list_requests",
    "fill_edition",
    "plan_editions",
]
