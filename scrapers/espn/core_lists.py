"""Strict parsing of ESPN core list and season objects (#1501).

Core lists are paged (``count/pageIndex/pageSize/pageCount`` + ``items`` of
``$ref`` links).  ``collect_refs`` walks every page and checks the total, so a
list longer than one page is never silently cut (R-10).  An empty list
(``count = 0, pageCount = 0``) is legitimate: the Cup season ESPN has not
opened yet returns exactly that.  ESPN error bodies (400/404) fail with their
code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
import re
from typing import Any, Callable, Mapping

from .models import SeasonType
from .parser_common import (
    EspnParseError,
    decode_object,
    espn_day,
    native_id,
    required_list,
    required_mapping,
    required_string,
    source_year,
    utc_datetime,
)
from .schedule_parser import STATUS_MAP

logger = logging.getLogger(__name__)

_EVENT_REF_RE = re.compile(r"/events/(\d+)(?:[/?#]|$)")
_TYPE_REF_RE = re.compile(r"/types/(\d+)(?:[/?#]|$)")


@dataclass(frozen=True, slots=True)
class RefPage:
    count: int
    page_index: int
    page_count: int
    refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoreSeason:
    year: int
    display_name: str
    start: date  # ESPN (US Eastern) day
    end: date
    type_id: int | None
    type_name: str | None


def raise_on_error_body(payload: Any, field: str) -> None:
    """ESPN answers 400/404 with a small JSON body; name its code."""

    if not isinstance(payload, Mapping):
        return
    error = payload.get("error")
    if isinstance(error, Mapping) and "code" in error:
        raise EspnParseError(
            f"{field}: ESPN error {error.get('code')}: {error.get('message')}"
        )
    if set(payload) == {"code", "message"}:
        raise EspnParseError(
            f"{field}: ESPN error {payload.get('code')}: {payload.get('message')}"
        )


def _count(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise EspnParseError(f"{field} must be a non-negative integer")
    return value


def parse_ref_page(payload: Any, field: str = "core list") -> RefPage:
    raise_on_error_body(payload, field)
    page = required_mapping(payload, field)
    items = page.get("items")
    if not isinstance(items, list):
        raise EspnParseError(f"{field} has no items array")
    count = _count(page.get("count"), f"{field}.count")
    page_index = _count(page.get("pageIndex"), f"{field}.pageIndex")
    page_count = _count(page.get("pageCount"), f"{field}.pageCount")
    if page_count == 0:
        if count != 0 or items or page_index != 0:
            raise EspnParseError(f"{field} with pageCount=0 must be empty")
    elif not 1 <= page_index <= page_count:
        raise EspnParseError(
            f"{field} pageIndex={page_index} outside 1..{page_count}"
        )
    elif page_count == 1 and count != len(items):
        raise EspnParseError(
            f"{field} count={count} differs from {len(items)} items"
        )
    refs: list[str] = []
    for index, item in enumerate(items):
        ref = item.get("$ref") if isinstance(item, Mapping) else None
        refs.append(required_string(ref, f"{field}.items[{index}].$ref"))
    return RefPage(count, page_index, page_count, tuple(refs))


def collect_refs(
    fetch_page: Callable[[int], bytes], field: str = "core list"
) -> tuple[str, ...]:
    """Every ``$ref`` of a paged core list, pages 1..pageCount."""

    first = parse_ref_page(decode_object(fetch_page(1), f"{field} page 1"), field)
    refs = list(first.refs)
    for number in range(2, first.page_count + 1):
        page = parse_ref_page(
            decode_object(fetch_page(number), f"{field} page {number}"), field
        )
        if page.page_index != number:
            raise EspnParseError(
                f"{field} page {number} reports pageIndex={page.page_index}"
            )
        if (page.count, page.page_count) != (first.count, first.page_count):
            raise EspnParseError(f"{field} page {number} changed count or pageCount")
        refs.extend(page.refs)
    if first.count != len(refs):
        raise EspnParseError(
            f"{field} count={first.count} differs from {len(refs)} collected refs"
        )
    if len(set(refs)) != len(refs):
        raise EspnParseError(f"{field} repeats a $ref")
    return tuple(refs)


def event_id_from_ref(ref: str) -> int:
    match = _EVENT_REF_RE.search(ref) if isinstance(ref, str) else None
    if match is None:
        raise EspnParseError(f"not an ESPN event $ref: {ref!r}")
    return native_id(match.group(1), "event $ref id")


def _day(value: Any, field: str) -> date:
    return espn_day(utc_datetime(value, field))


def _season_type(item: Any, field: str) -> SeasonType:
    node = required_mapping(item, field)
    if "id" in node:
        type_id = native_id(node["id"], f"{field}.id")
    else:
        ref = required_string(node.get("$ref"), f"{field}.$ref")
        match = _TYPE_REF_RE.search(ref)
        if match is None:
            raise EspnParseError(f"{field}.$ref is not a season type: {ref!r}")
        type_id = native_id(match.group(1), f"{field}.$ref id")
    name = node.get("name")
    return SeasonType(
        id=type_id,
        name=required_string(name, f"{field}.name") if name is not None else None,
        start_date=_day(node["startDate"], f"{field}.startDate")
        if node.get("startDate") is not None
        else None,
        end_date=_day(node["endDate"], f"{field}.endDate")
        if node.get("endDate") is not None
        else None,
    )


def parse_types(payload: Any, field: str = "season types") -> tuple[SeasonType, ...]:
    """Stages of one season; embedded or ``$ref``-only, one page only."""

    page = parse_ref_page(payload, field)
    if page.page_count > 1:
        raise EspnParseError(f"{field} spans {page.page_count} pages")
    types = tuple(
        _season_type(item, f"{field}.items[{index}]")
        for index, item in enumerate(payload["items"])
    )
    if len({item.id for item in types}) != len(types):
        raise EspnParseError(f"{field} repeats a type id")
    return types


def _core_season(season: Mapping[str, Any], field: str) -> CoreSeason:
    year = source_year(season.get("year"), f"{field}.year")
    display_name = required_string(season.get("displayName"), f"{field}.displayName")
    start = _day(season.get("startDate"), f"{field}.startDate")
    end = _day(season.get("endDate"), f"{field}.endDate")
    if start > end:
        raise EspnParseError(f"{field} starts after it ends")
    type_id = type_name = None
    if season.get("type") is not None:
        current_type = _season_type(season["type"], f"{field}.type")
        type_id, type_name = current_type.id, current_type.name
    return CoreSeason(year, display_name, start, end, type_id, type_name)


def parse_league_current_season(detail: Any) -> CoreSeason:
    """The season ESPN calls current in ``leagues/{slug}`` — read every run."""

    raise_on_error_body(detail, "league detail")
    node = required_mapping(detail, "league detail")
    return _core_season(
        required_mapping(node.get("season"), "league detail.season"),
        "league detail.season",
    )


def parse_season(payload: Any) -> tuple[CoreSeason, tuple[SeasonType, ...]]:
    raise_on_error_body(payload, "season")
    node = required_mapping(payload, "season")
    return _core_season(node, "season"), parse_types(node.get("types"), "season.types")


def parse_event_status(payload: Any) -> str:
    """Status name of ``competitions/{id}/status``; unknown names only warn."""

    raise_on_error_body(payload, "event status")
    node = required_mapping(payload, "event status")
    status_type = required_mapping(node.get("type"), "event status.type")
    name = required_string(status_type.get("name"), "event status.type.name")
    if name not in STATUS_MAP:
        logger.warning("ESPN event status %r is not in the status map", name)
    return name


def event_ids(refs: Any) -> tuple[int, ...]:
    ids = tuple(event_id_from_ref(ref) for ref in required_list(list(refs), "refs"))
    if len(set(ids)) != len(ids):
        raise EspnParseError("event list repeats an event id")
    return ids


__all__ = [
    "CoreSeason",
    "RefPage",
    "collect_refs",
    "event_id_from_ref",
    "event_ids",
    "parse_event_status",
    "parse_league_current_season",
    "parse_ref_page",
    "parse_season",
    "parse_types",
    "raise_on_error_body",
]
