"""Parse saved tmapi club stadium responses. See docs/operations/transfermarkt-club-stadiums.md."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal, TypeVar


PARSER_REVISION = 1
SCHEMA_REVISION = 1
T = TypeVar('T')


class ClubStadiumError(ValueError):
    def __init__(self, reason: str, field: str) -> None:
        self.reason = reason
        self.field = field
        super().__init__(f'{reason} at {field}')


@dataclass(frozen=True)
class Stadium:
    stadium_id: str | None
    name: str
    capacity: int | None
    international_capacity: int | None
    website: str | None
    address_line_1: str | None
    address_line_2: str | None
    country_id: str | None
    country_name: str | None
    latitude: float | None
    longitude: float | None
    build_year: int | None
    renovation_year: int | None
    field_length: int | None
    field_width: int | None
    field_surface: str | None
    images: tuple[str, ...] | None
    source_is_historical: bool | None


@dataclass(frozen=True)
class ClubStadiumRelation:
    club_id: str
    stadium_id: str | None
    observed_at: str
    valid_from: None = None
    valid_to: None = None


@dataclass(frozen=True)
class ClubStadiumObservation:
    status: Literal['observed_present', 'observed_empty']
    club_id: str
    observed_at: str
    stadium: Stadium | None
    completeness_proof: str | None

    @property
    def relation(self) -> ClubStadiumRelation | None:
        if self.stadium is None:
            return None
        return ClubStadiumRelation(self.club_id, self.stadium.stadium_id, self.observed_at)

    def canonical_json(self) -> str:
        document = asdict(self)
        relation = self.relation
        document['relation'] = asdict(relation) if relation is not None else None
        document['parser_revision'] = PARSER_REVISION
        document['schema_revision'] = SCHEMA_REVISION
        return json.dumps(
            document, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False,
        )


def _object(value: object, path: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise ClubStadiumError('invalid_object', path)
    return value


def _optional(obj: Mapping, key: str, path: str, parse: Callable[[object, str], T]) -> T | None:
    if key in obj:
        return parse(obj[key], f'{path}.{key}')
    return None


def _id(value: object, path: str) -> str:
    if type(value) is int and value > 0:
        try:
            return str(value)
        except ValueError:
            pass
    if isinstance(value, str) and re.fullmatch(r'[1-9][0-9]*', value):
        return value
    raise ClubStadiumError('invalid_id', path)


def _timestamp(value: object) -> str:
    # fromisoformat also accepts compact dates, spaces and offsets with seconds.
    if isinstance(value, str) and re.fullmatch(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
        r'(?:\.[0-9]{6})?(?:Z|[+-][0-9]{2}:[0-5][0-9])', value
    ):
        try:
            canonical = datetime.fromisoformat(value).isoformat()
        except ValueError:
            pass
        else:
            expected = value[:-1] + '+00:00' if value.endswith('Z') else value
            if canonical == expected:
                return canonical
    raise ClubStadiumError('invalid_observed_at', 'observed_at')


def _text(value: object, path: str) -> str:
    if not isinstance(value, str):
        raise ClubStadiumError('invalid_text', path)
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise ClubStadiumError('invalid_text', path) from exc
    return value


def _nonblank(value: object, path: str) -> str:
    result = _text(value, path)
    if not result.strip():
        raise ClubStadiumError('blank_text', path)
    return result


def _address(value: object, path: str) -> str | None:
    return _text(value, path).strip() or None


def _integer(value: object, path: str) -> int:
    if type(value) is not int or value < 0:
        raise ClubStadiumError('invalid_integer', path)
    return value


def _coordinate(value: object, path: str, limit: int) -> float:
    if type(value) not in (int, float) or not -limit <= value <= limit:
        raise ClubStadiumError('invalid_coordinate', path)
    return float(value)


def _boolean(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        raise ClubStadiumError('invalid_boolean', path)
    return value


def _images(value: object, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ClubStadiumError('invalid_images', path)
    urls = []
    for index, item in enumerate(value):
        entry_path = f'{path}[{index}]'
        entry = _object(item, entry_path)
        urls.append(_nonblank(entry.get('url'), f'{entry_path}.url'))
        _optional(entry, 'source', entry_path, _text)
    return tuple(dict.fromkeys(urls))


def _country_name(country_id: str | None, names: Mapping | None) -> str | None:
    if names is None:
        return None
    result = None
    for key, value in _object(names, 'country_names').items():
        identifier = _id(key, 'country_names.key')
        name = _nonblank(value, f'country_names.{identifier}')
        if identifier == country_id:
            if result is not None and name != result:
                raise ClubStadiumError('conflicting_country_name', 'country_names')
            result = name
    return result


def parse_club_stadium(
    payload: Mapping, *, club_id: str | int, observed_at: str,
    country_names: Mapping[str | int, str] | None = None,
) -> ClubStadiumObservation:
    """Observe one club's stadium from a complete successful saved envelope."""
    club = _id(club_id, 'club_id')
    timestamp = _timestamp(observed_at)
    envelope = _object(payload, 'payload')
    if envelope.get('success') is not True:
        raise ClubStadiumError('unsuccessful_envelope', 'success')
    _optional(envelope, 'message', 'payload', _text)
    data = _object(envelope.get('data'), 'data')
    if not data:
        raise ClubStadiumError('empty_data', 'data')
    source_club = _optional(data, 'clubId', 'data', _id)
    if source_club is not None and source_club != club:
        raise ClubStadiumError('club_mismatch', 'data.clubId')
    name = _nonblank(data.get('name'), 'data.name').strip()
    location_path = 'data.location'
    location = _object(data.get('location', {}), location_path)
    building_path = 'data.buildingDetails'
    building = _object(data.get('buildingDetails', {}), building_path)
    postcode = _optional(location, 'postcode', location_path, _address)
    city = _optional(location, 'city', location_path, _address)
    country_id = _optional(location, 'countryId', location_path, _id)

    stadium = Stadium(
        stadium_id=_optional(data, 'id', 'data', _id),
        name=name,
        capacity=_optional(data, 'capacity', 'data', _integer),
        international_capacity=_optional(data, 'internationalCapacity', 'data', _integer),
        website=_optional(data, 'url', 'data', _text),
        address_line_1=_optional(location, 'street', location_path, _address),
        address_line_2=' '.join(part for part in (postcode, city) if part) or None,
        country_id=country_id,
        country_name=_country_name(country_id, country_names),
        latitude=_optional(location, 'latitude', location_path, lambda v, p: _coordinate(v, p, 90)),
        longitude=_optional(location, 'longitude', location_path, lambda v, p: _coordinate(v, p, 180)),
        build_year=_optional(building, 'buildYear', building_path, _integer),
        renovation_year=_optional(building, 'renovationYear', building_path, _integer),
        field_length=_optional(building, 'fieldLength', building_path, _integer),
        field_width=_optional(building, 'fieldWidth', building_path, _integer),
        field_surface=_optional(building, 'fieldSurface', building_path, _text),
        images=_optional(data, 'images', 'data', _images),
        source_is_historical=_optional(data, 'isHistorical', 'data', _boolean),
    )
    return ClubStadiumObservation('observed_present', club, timestamp, stadium, None)


def confirmed_empty_club_stadium(
    *, club_id: str | int, observed_at: str, completeness_proof: str | None = None,
) -> ClubStadiumObservation:
    """Record external evidence of completeness for this club and capture."""
    club = _id(club_id, 'club_id')
    timestamp = _timestamp(observed_at)
    if not isinstance(completeness_proof, str) or not completeness_proof.strip():
        raise ClubStadiumError('missing_completeness_proof', 'completeness_proof')
    proof = _text(completeness_proof, 'completeness_proof')
    return ClubStadiumObservation('observed_empty', club, timestamp, None, proof)
