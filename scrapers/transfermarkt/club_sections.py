"""T1 club staff and T7 club profile, stadium and club–stadium observations."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from functools import cache
import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, SoupStrainer

from . import club_stadiums, staff
from .section_contracts import ObservationTable, make_outcome


_CLUB = ('club_id',)
_VENUE_KEY = ('stadium_observation_key', 'club_id', 'observed_at')
_VENUE_TYPES = {
    **dict.fromkeys(('capacity', 'international_capacity', 'build_year', 'renovation_year',
                     'field_length', 'field_width'), 'bigint'),
    'source_is_historical': 'boolean', 'latitude': 'double', 'longitude': 'double',
}
TABLES = {
    'club_staff_observations': ObservationTable(tuple(staff.STAFF_COLUMNS), staff.STAFF_KEY, _CLUB,
                                              {'appointed_date': 'date', 'observed_day': 'date'}),
    'club_profile_observations': ObservationTable(
        ('club_id', 'url', 'name', 'short_name', 'abbreviation', 'club_code', 'official_name', 'image',
         'address_line_1', 'address_line_2', 'address_line_3', 'country_id', 'colors', 'is_national_team'),
        ('club_id', 'observed_at'), _CLUB, {'is_national_team': 'boolean'}),
    'stadium_observations': ObservationTable(
        ('club_id', 'stadium_observation_key', 'stadium_id', 'name', 'capacity', 'international_capacity',
         'website', 'address_line_1', 'address_line_2', 'country_id', 'country_name', 'latitude',
         'longitude', 'build_year', 'renovation_year', 'field_length', 'field_width', 'field_surface',
         'images', 'source_is_historical'), _VENUE_KEY, _CLUB, _VENUE_TYPES),
    'club_stadium_relations': ObservationTable(
        ('club_id', 'stadium_observation_key', 'stadium_id', 'valid_from', 'valid_to'),
        ('club_id', 'stadium_observation_key', 'observed_at'), _CLUB,
        {'valid_from': 'timestamp', 'valid_to': 'timestamp'}),
}
ENDPOINT_ENTITIES = {
    'club_staff': ('club_staff_observations',),
    'club_profile': ('club_profile_observations',),
    'club_stadium': ('stadium_observations', 'club_stadium_relations'),
}
JSON_ENDPOINTS = frozenset({'club_profile', 'club_stadium'})
PAGINATED = frozenset()
_ROUTES = {
    'club_staff': re.compile(r'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/mitarbeiter/verein/([1-9][0-9]*)/?'),
    'club_profile': re.compile(r'https://tmapi\.transfermarkt\.technology/club/([1-9][0-9]*)'),
    'club_stadium': re.compile(r'https://tmapi\.transfermarkt\.technology/club/([1-9][0-9]*)/stadium'),
}


def scope_from_url(endpoint, url, *, edition_id='', competition_id=''):
    match = _ROUTES[endpoint].fullmatch(url)
    if not match:
        raise ValueError(f'{endpoint}:invalid_club_url')
    return {'club_id': match[1]}


def _staff_club_matches(html, url, club_id):
    markers = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer(['link', 'meta', 'tm-subnavigation']))
    for tag in markers.select('link[rel~=canonical], meta[property="og:url"]'):
        label = tag.get('href') if tag.name == 'link' else tag.get('content')
        match = _ROUTES['club_staff'].fullmatch(urljoin(url, label)) if label else None
        if not match or match[1] != club_id:
            return False
    return all(nav.get('controller') == 'verein' and nav.get('id') == club_id
               for nav in markers.select('tm-subnavigation'))


@cache
def _country_names():
    from .discovery import load_countries

    return {country.country_id: country.country for country in load_countries()}


def _is_club_profile(data, profile):
    # Both source documents can publish a name and the same numeric ID.
    if {'clubId', 'capacity', 'internationalCapacity', 'buildingDetails', 'isHistorical'}.intersection(data):
        return False
    link = re.fullmatch(r'/[A-Za-z0-9_-]+/startseite/verein/([1-9][0-9]*)/?', profile.url or '')
    if link is not None:
        return link[1] == profile.club_id
    source_text = (profile.short_name, profile.abbreviation, profile.club_code, profile.official_name, profile.image,
                   profile.address_line_1, profile.address_line_2, profile.country_id)
    return (profile.is_national_team is not None or bool(profile.colors)
            or any(value and value.strip() for value in source_text))


def _is_club_stadium(data, stadium):
    if {'baseDetails', 'preferences', 'squadDetails', 'crestUrl', 'relativeUrl', 'isSpecialClub'}.intersection(data):
        return False
    measures = (stadium.capacity, stadium.international_capacity, stadium.build_year, stadium.renovation_year,
                stadium.field_length, stadium.field_width)
    if any(value is not None for value in measures) or (stadium.field_surface or '').strip():
        return True
    # A sparse venue can publish its owning club alongside its location, images or historical flag.
    return 'clubId' in data and (
        any(value is not None for value in (stadium.latitude, stadium.longitude, stadium.source_is_historical))
        or bool(stadium.address_line_1 or stadium.address_line_2 or stadium.country_id or stadium.images))


def confirm_stadium_absence(outcomes, profile):
    """Pair a durable stadium 404 with the verified profile in this run."""
    stadium = outcomes[0].capture
    if (stadium is None or stadium.status_code != 404 or profile is None
            or profile.status != 'observed_present' or profile.scope != outcomes[0].scope):
        return outcomes
    source = profile.capture
    if (source is None or source.status_code != 200 or source.capture_id == stadium.capture_id
            or stadium.url != source.url + '/stadium'):
        return outcomes
    extracted_at = max(profile.extracted_at, outcomes[0].extracted_at)
    if extracted_at < max(source.captured_at, stadium.captured_at):
        return outcomes

    def evidence(capture):
        return {'raw_capture_id': capture.capture_id, 'body_hash': capture.body_hash,
                'captured_at': capture.captured_at, 'status_code': capture.status_code, 'url': capture.url}

    proof = json.dumps({'kind': 'club_profile_200_stadium_404', 'club_id': profile.scope.club_id,
                        'scope_id': profile.scope.scope_id, 'cycle_id': profile.scope.cycle_id,
                        'profile': evidence(source), 'stadium': evidence(stadium)},
                       sort_keys=True, separators=(',', ':'))
    return tuple(make_outcome(
        item.entity, stadium, item.scope, status='observed_empty', reason='club_stadium:paired_404', proof=proof,
        parser_revision=item.parser_revision, extracted_at=extracted_at, universe_version=item.universe_version,
    ) for item in outcomes)


def extract(captures, scope, *, extracted_at, universe_version):
    capture, = captures
    endpoint = capture.endpoint
    entities = ENDPOINT_ENTITIES[endpoint]
    revision = staff.STAFF_PARSER_REVISION if endpoint == 'club_staff' else 'club-stadiums-v2'

    def outcomes(status, reason, tables=None):
        return tuple(make_outcome(
            entity, capture, scope, status=status, reason=reason,
            records=(tables or {}).get(entity, ()), parser_revision=revision,
            extracted_at=extracted_at, universe_version=universe_version,
        ) for entity in entities)

    if capture.status_code != 200:
        return outcomes('unavailable', f'{endpoint}:http_{capture.status_code}')
    club_id = scope_from_url(endpoint, capture.url)['club_id']
    if scope.club_id and scope.club_id != club_id:
        return outcomes('error', f'{endpoint}:club_mismatch')
    clock = datetime.fromisoformat(capture.captured_at)
    try:
        if endpoint == 'club_staff':
            html = capture.text
            if not _staff_club_matches(html, capture.url, club_id):
                return outcomes('error', 'club_staff:club_mismatch')
            members = staff.parse_staff(html, club_id)
            return outcomes('observed_present', 'staff:complete_sections', {
                entities[0]: [dict(member, observed_day=clock.date()) for member in members],
            })
        payload = json.loads(capture.body)
        if endpoint == 'club_profile':
            profile = club_stadiums.parse_club_profile(payload, club_id=club_id, observed_at=clock.isoformat(),
                                                       country_names=_country_names())
            if not _is_club_profile(payload['data'], profile):
                return outcomes('error', 'club_profile:unverified_response_type')
            record = asdict(profile)
            record['colors'] = json.dumps(profile.colors, ensure_ascii=False, separators=(',', ':'))
            return outcomes('observed_present', 'club_profile:published', {entities[0]: [record]})
        observation = club_stadiums.parse_club_stadium(payload, club_id=club_id, observed_at=clock.isoformat(),
                                                       country_names=_country_names())
        if not _is_club_stadium(payload['data'], observation.stadium):
            return outcomes('error', 'club_stadium:unverified_response_type')
        venue = asdict(observation.stadium)
        stadium_id = observation.stadium.stadium_id
        # Without a source ID this key identifies only this club's observation.
        key = f'stadium:{stadium_id}' if stadium_id else f'club:{club_id}:unidentified'
        venue.update(club_id=club_id, stadium_observation_key=key)
        if venue['images'] is not None:
            venue['images'] = json.dumps(venue['images'], ensure_ascii=False, separators=(',', ':'))
        relation = dict(asdict(observation.relation), stadium_observation_key=key)
        return outcomes('observed_present', 'club_stadium:published', {
            'stadium_observations': [venue], 'club_stadium_relations': [relation],
        })
    except (ValueError, UnicodeError) as exc:
        return outcomes('error', f'{endpoint}:{getattr(exc, "reason", "invalid_layout")}')
