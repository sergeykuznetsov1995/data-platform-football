"""Transfermarkt's service API (tmapi) for competition participants (#1392).

Cup, continental and national-team competition pages are rendered by a script
(``<tm-competition-homepage>``): their HTML carries no participant table, so a
listing fetch sees an empty shell.  tmapi answers the same question directly:

* ``/competition/{id}/club?season={saison_id}`` — participant club ids
  (about 0.6 KB against 29.5 KB of the ``/teilnehmer/`` HTML page);
* ``/competition/{id}/regulation`` — every edition with ``isCurrentSeason``.

``season`` is the source ``saison_id`` (``season.py``): Copa do Brasil "2026"
is ``season=2025``.  Requests go through the scraper's own transport (proxy
lease, raw store); this module only builds URLs and reads payloads, failing
closed on anything it does not recognise.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

TMAPI_BASE = "https://tmapi.transfermarkt.technology"
_ID_RE = re.compile(r"[A-Za-z0-9_-]+")
_SAISON_RE = re.compile(r"(?:18|19|20|21)\d{2}")


class TmapiSchemaError(ValueError):
    """A tmapi payload does not have the shape this module relies on."""


@dataclass(frozen=True)
class RegulationSeason:
    saison_id: int
    display: str
    is_current: bool


def _competition(competition_id: Any) -> str:
    value = str(competition_id or "").strip()
    if not _ID_RE.fullmatch(value):
        raise ValueError(f"invalid competition id: {competition_id!r}")
    return value


def _saison(saison_id: Any) -> int:
    value = str(saison_id).strip()
    if not _SAISON_RE.fullmatch(value):
        raise ValueError(f"invalid saison_id: {saison_id!r}")
    return int(value)


def competition_clubs_url(competition_id: Any, saison_id: Any) -> str:
    return (
        f"{TMAPI_BASE}/competition/{_competition(competition_id)}/club"
        f"?season={_saison(saison_id)}"
    )


def competition_regulation_url(competition_id: Any) -> str:
    return f"{TMAPI_BASE}/competition/{_competition(competition_id)}/regulation"


def _data(payload: Any) -> Any:
    if not isinstance(payload, Mapping) or payload.get("success") is not True:
        raise TmapiSchemaError("tmapi payload is not a successful envelope")
    if "data" not in payload:
        raise TmapiSchemaError("tmapi payload has no data")
    return payload["data"]


def parse_competition_clubs(
    payload: Any, *, competition_id: Any, saison_id: Any,
) -> tuple[str, ...]:
    """Participant club ids; an empty tuple is the source's own answer."""

    data = _data(payload)
    if not isinstance(data, Mapping):
        raise TmapiSchemaError("competition clubs data is not an object")
    if str(data.get("competitionId")) != _competition(competition_id):
        raise TmapiSchemaError("competition clubs answer another competition")
    if data.get("seasonId") != _saison(saison_id):
        raise TmapiSchemaError("competition clubs answer another season")
    raw = data.get("clubIds")
    if not isinstance(raw, list):
        raise TmapiSchemaError("competition clubs has no clubIds list")
    ids = tuple(str(item).strip() for item in raw)
    if any(not item.isdigit() for item in ids):
        raise TmapiSchemaError("competition clubs has a non-numeric club id")
    if len(set(ids)) != len(ids):
        raise TmapiSchemaError("competition clubs has duplicate club ids")
    return ids


def parse_regulation(
    payload: Any, *, competition_id: Any,
) -> tuple[RegulationSeason, ...]:
    """Every edition the source lists, newest first."""

    data = _data(payload)
    if not isinstance(data, list) or not data:
        raise TmapiSchemaError("regulation data is not a non-empty list")
    expected = _competition(competition_id)
    seasons = []
    for item in data:
        season = item.get("season") if isinstance(item, Mapping) else None
        if (
            not isinstance(season, Mapping)
            or str(item.get("competitionId")) != expected
            or not isinstance(item.get("isCurrentSeason"), bool)
        ):
            raise TmapiSchemaError("regulation entry has an unexpected shape")
        seasons.append(
            RegulationSeason(
                saison_id=_saison(season.get("id")),
                display=str(season.get("display") or ""),
                is_current=item["isCurrentSeason"],
            )
        )
    if sum(item.is_current for item in seasons) != 1:
        raise TmapiSchemaError("regulation must mark exactly one current season")
    return tuple(sorted(seasons, key=lambda item: item.saison_id, reverse=True))


def current_saison_id(seasons: tuple[RegulationSeason, ...]) -> int:
    """The edition the source itself calls current.

    For a national-team tournament that is the last played one: AFCN lists
    2026 ("2027") ahead but marks 2024 ("2025") current.
    """

    return next(item.saison_id for item in seasons if item.is_current)


def competition_clubs(
    fetch_json: Callable[[str], Optional[Any]],
    competition_id: Any,
    saison_id: Any,
) -> Optional[tuple[str, ...]]:
    """Participants through the caller's transport; ``None`` = not proven.

    A failed fetch or a payload of unknown shape is not an empty answer.
    """

    payload = fetch_json(competition_clubs_url(competition_id, saison_id))
    if payload is None:
        return None
    try:
        return parse_competition_clubs(
            payload, competition_id=competition_id, saison_id=saison_id,
        )
    except TmapiSchemaError:
        return None


__all__ = [
    "TMAPI_BASE",
    "RegulationSeason",
    "TmapiSchemaError",
    "competition_clubs",
    "competition_clubs_url",
    "competition_regulation_url",
    "current_saison_id",
    "parse_competition_clubs",
    "parse_regulation",
]
