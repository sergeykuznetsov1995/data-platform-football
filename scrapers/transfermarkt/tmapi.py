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
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlencode

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


@dataclass(frozen=True)
class PlayerSignal:
    """Current player fields measured in the source's 23 September payload.

    A signal is a detector, never a replacement for a complete career or a
    tournament squad. National-team assignments remain separate from clubs.
    """

    player_id: str
    market_value_eur: int | None
    market_value_date: str | None
    market_value_present: bool
    contract_until: str | None
    last_contract_renewal: tuple[int | None, int | None, int | None]
    club_ids: tuple[str, ...]
    assignments: tuple[tuple[str, str, str | None, str | None, bool], ...]
    attributes_json: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def signature(self) -> str:
        return hashlib.sha256(json.dumps(
            self.as_dict(), sort_keys=True, separators=(',', ':'), allow_nan=False,
        ).encode('utf-8')).hexdigest()


def _source_id(value: Any) -> str:
    result = str(value)
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]+', result) or int(result) <= 0:
        raise TmapiSchemaError('tmapi has an invalid source ID')
    return result


def _optional_date(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TmapiSchemaError('tmapi date is not a nullable ISO date')
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise TmapiSchemaError('tmapi date is invalid') from exc


def parse_player_signals(payload: Any, *, expected_ids) -> dict[str, PlayerSignal]:
    """Fail closed on missing IDs or fields; explicit null remains observable."""
    expected = tuple(_source_id(item) for item in expected_ids)
    if not 1 <= len(expected) <= 300 or len(set(expected)) != len(expected):
        raise TmapiSchemaError('signal batch needs 1..300 distinct IDs')
    data = _data(payload)
    if not isinstance(data, list):
        raise TmapiSchemaError('player signal data is not a list')
    result = {}
    for row in data:
        if not isinstance(row, Mapping):
            raise TmapiSchemaError('player signal row is not an object')
        player_id = _source_id(row.get('id'))
        attrs, mv, memberships = row.get('attributes'), row.get('marketValueDetails'), row.get('clubAssignments')
        if (
            not isinstance(attrs, Mapping) or not {'contractUntil', 'lastContractRenewal'} <= attrs.keys()
            or (mv is not None and (not isinstance(mv, Mapping) or 'current' not in mv))
            or not isinstance(memberships, list)
        ):
            raise TmapiSchemaError('player signal lacks contract, value or club evidence')
        current = None if mv is None else mv['current']
        if current is None:
            value, determined = None, None
        else:
            if not isinstance(current, Mapping) or not {'value', 'currency', 'determined'} <= current.keys():
                raise TmapiSchemaError('player market value has an unknown shape')
            value = current['value']
            if type(value) is not int or value < 0 or current['currency'] != 'EUR':
                raise TmapiSchemaError('player market value is not a nonnegative EUR amount')
            determined = _optional_date(current['determined'])
        renewal = attrs['lastContractRenewal']
        if not isinstance(renewal, Mapping) or not {'year', 'month', 'day'} <= renewal.keys():
            raise TmapiSchemaError('contract renewal has an unknown shape')
        renewal_values = tuple(renewal[key] for key in ('year', 'month', 'day'))
        for item, minimum, maximum in zip(renewal_values, (1800, 1, 1), (2199, 12, 31)):
            if item is not None and (type(item) is not int or not minimum <= item <= maximum):
                raise TmapiSchemaError('contract renewal component is invalid')
        assignments = []
        for assignment in memberships:
            if not isinstance(assignment, Mapping) or not {'clubId', 'type', 'shirtNumber', 'isCaptain'} <= assignment.keys():
                raise TmapiSchemaError('club assignment is incomplete')
            kind = assignment['type']
            if kind not in {'current', 'additional', 'nationalTeam'} or type(assignment['isCaptain']) is not bool:
                raise TmapiSchemaError('club assignment has an unknown type')
            number = assignment['shirtNumber']
            if number is not None and not re.fullmatch(r'[0-9]+', str(number)):
                raise TmapiSchemaError('shirt number is invalid')
            assignments.append((_source_id(assignment['clubId']), kind, _optional_date(assignment.get('start')),
                                None if number is None else str(number), assignment['isCaptain']))
        if len(set(assignments)) != len(assignments):
            raise TmapiSchemaError('duplicate player assignment')
        club_ids = tuple(sorted({item[0] for item in assignments if item[1] in {'current', 'additional'}}, key=int))
        # Website presentation/preferences and premium-agency metadata are not
        # player changes. Preserve the bio and agency identity in the signature.
        stable_attrs = {key: attrs.get(key) for key in (
            'height', 'preferredFootId', 'positionId', 'firstSidePositionId',
            'secondSidePositionId', 'consultantAgencyId',
        )}
        stable_attrs.update(lifeDates=row.get('lifeDates'), nationalityDetails=row.get('nationalityDetails'))
        try:
            attrs_json = json.dumps(stable_attrs, sort_keys=True, separators=(',', ':'), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise TmapiSchemaError('player attributes are not valid JSON') from exc
        if player_id in result:
            raise TmapiSchemaError('duplicate player signal ID')
        result[player_id] = PlayerSignal(
            player_id, value, determined, current is not None, _optional_date(attrs['contractUntil']),
            renewal_values, club_ids, tuple(sorted(assignments, key=lambda item: (int(item[0]), item[1]))), attrs_json,
        )
    if set(result) != set(expected):
        raise TmapiSchemaError('player signal packet identity differs from requested IDs')
    return result


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


def players_url(player_ids) -> str:
    """Public player signal packet: at most 300 distinct positive IDs."""
    ids = tuple(str(value).strip() for value in player_ids)
    if (
        not 1 <= len(ids) <= 300 or len(set(ids)) != len(ids)
        or any(not re.fullmatch(r'[0-9]+', value) or int(value) <= 0 for value in ids)
    ):
        raise ValueError('players packet requires 1..300 unique positive IDs')
    return f'{TMAPI_BASE}/players?' + urlencode([('ids[]', value) for value in ids])


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
    "players_url",
    "PlayerSignal",
    "parse_player_signals",
]
