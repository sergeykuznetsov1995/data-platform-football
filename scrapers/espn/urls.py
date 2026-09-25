"""Every ESPN request address of the new contour, in one place (#1501).

Lists come from the core API only; scoreboard takes exactly one ``date`` —
ESPN answers ``scoreboard?dates=A-B`` with 400 since August 2026 (R-04), so a
range to scoreboard cannot be expressed through this module.  Origins are the
#1500 transport constants; the transport picks the live origin per cluster.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlencode

from .transport_contracts import (
    ESPN_CORE_API_ORIGIN,
    ESPN_SITE_WEB_API_ORIGIN,
    EndpointType,
)

CORE_SOCCER = f"{ESPN_CORE_API_ORIGIN}/v2/sports/soccer"
SITE_SOCCER = f"{ESPN_SITE_WEB_API_ORIGIN}/apis/site/v2/sports/soccer"
LIST_LIMIT = 1000
# ESPN core refuses a longer ``events?dates=`` window with 400
# "The dates range specified is too large" (395 days refused, 365 accepted).
MAX_WINDOW_DAYS = 365
_CORE_LOCALE = (("lang", "en"), ("region", "us"))


@dataclass(frozen=True, slots=True)
class EspnRequest:
    url: str
    params: Mapping[str, str]
    endpoint: EndpointType

    @property
    def full_url(self) -> str:
        return f"{self.url}?{urlencode(list(self.params.items()))}"


def _core(path: str, endpoint: EndpointType, *pairs: tuple[str, str]) -> EspnRequest:
    return EspnRequest(
        f"{CORE_SOCCER}/{path}",
        MappingProxyType(dict(pairs + _CORE_LOCALE)),
        endpoint,
    )


def _page(page: int) -> tuple[tuple[str, str], ...]:
    if type(page) is not int or page < 1:
        raise ValueError("core list page must be a positive integer")
    return (("page", str(page)),) if page > 1 else ()


def _day(value: date) -> str:
    if type(value) is not date:
        raise TypeError("ESPN day must be a date")
    return value.strftime("%Y%m%d")


def league_detail(slug: str) -> EspnRequest:
    return _core(f"leagues/{slug}", EndpointType.CATALOG)


def league_seasons(slug: str, page: int = 1, *, limit: int = LIST_LIMIT) -> EspnRequest:
    return _core(
        f"leagues/{slug}/seasons",
        EndpointType.CATALOG,
        ("limit", str(limit)),
        *_page(page),
    )


def season(slug: str, year: int) -> EspnRequest:
    return _core(f"leagues/{slug}/seasons/{year}", EndpointType.CATALOG)


def season_types(slug: str, year: int) -> EspnRequest:
    return _core(f"leagues/{slug}/seasons/{year}/types", EndpointType.CATALOG)


def type_events(
    slug: str, year: int, type_id: int, page: int = 1, *, limit: int = LIST_LIMIT
) -> EspnRequest:
    return _core(
        f"leagues/{slug}/seasons/{year}/types/{type_id}/events",
        EndpointType.SCOREBOARD,
        ("limit", str(limit)),
        *_page(page),
    )


def events_window(
    slug: str, start: date, end: date, page: int = 1, *, limit: int = LIST_LIMIT
) -> EspnRequest:
    """Core event list for ESPN days ``start..end`` (one day: ``dates=D``)."""

    first, last = _day(start), _day(end)
    if start > end:
        raise ValueError("events window starts after it ends")
    if (end - start).days + 1 > MAX_WINDOW_DAYS:
        raise ValueError(f"events window exceeds {MAX_WINDOW_DAYS} days")
    dates = first if start == end else f"{first}-{last}"
    return _core(
        f"leagues/{slug}/events",
        EndpointType.SCOREBOARD,
        ("dates", dates),
        ("limit", str(limit)),
        *_page(page),
    )


def event_status(slug: str, event_id: int) -> EspnRequest:
    return _core(
        f"leagues/{slug}/events/{event_id}/competitions/{event_id}/status",
        EndpointType.SCOREBOARD,
    )


def league_scoreboard_day(slug: str, day: date) -> EspnRequest:
    return EspnRequest(
        f"{SITE_SOCCER}/{slug}/scoreboard",
        MappingProxyType({"dates": _day(day), "limit": str(LIST_LIMIT)}),
        EndpointType.SCOREBOARD,
    )


def all_scoreboard_day(day: date) -> EspnRequest:
    return league_scoreboard_day("all", day)


def summary(slug: str, event_id: int) -> EspnRequest:
    """Match Summary on site.web.api, as in the recorded probes (#1504)."""

    if type(event_id) is not int or event_id < 1:
        raise ValueError("ESPN event id must be a positive integer")
    return EspnRequest(
        f"{SITE_SOCCER}/{slug}/summary",
        MappingProxyType({"event": str(event_id)}),
        EndpointType.SUMMARY,
    )


__all__ = [
    "EspnRequest",
    "MAX_WINDOW_DAYS",
    "all_scoreboard_day",
    "event_status",
    "events_window",
    "league_detail",
    "league_scoreboard_day",
    "league_seasons",
    "season",
    "season_types",
    "summary",
    "type_events",
]
