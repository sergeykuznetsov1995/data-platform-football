from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from scrapers.transfermarkt.club_stadiums import (
    PARSER_REVISION,
    SCHEMA_REVISION,
    ClubStadiumError,
    ClubStadiumRelation,
    Stadium,
    confirmed_empty_club_stadium,
    parse_club_stadium,
)


pytestmark = pytest.mark.unit
CAPTURE = '2026-10-03T08:05:00+02:00'
IMAGE_1 = 'https://img.example/stadium-1.jpg'
IMAGE_2 = 'https://img.example/stadium-2.jpg'


@pytest.fixture
def envelope():
    return {
        'success': True,
        'message': 'OK',
        'data': {
            'id': '56',
            'name': 'Spotify Camp Nou',
            'url': 'www.fcbarcelona.com/web/index_idiomes.html',
            'clubId': '131',
            'isHistorical': False,
            'capacity': 62657,
            'internationalCapacity': 62000,
            'images': [
                {'url': IMAGE_1, 'source': 'IMAGO'},
                {'url': IMAGE_2, 'source': 'IMAGO'},
                {'url': IMAGE_1, 'source': 'Other'},
            ],
            'location': {
                'countryId': 157,
                'street': ' Carrer d´Aristides Maillol, entrance no 7  ',
                'postcode': '08028 ',
                'city': ' Barcelona',
                'latitude': 41.380896,
                'longitude': 2.1228198,
            },
            'buildingDetails': {
                'buildYear': 1957,
                'renovationYear': 1994,
                'fieldWidth': 68,
                'fieldLength': 105,
                'fieldSurface': 'Hybridrasen',
            },
        },
    }


def test_full_observation(envelope):
    result = parse_club_stadium(
        envelope, club_id=131, observed_at=CAPTURE, country_names={'157': 'España'},
    )
    assert (result.status, result.club_id, result.observed_at, result.completeness_proof) == (
        'observed_present', '131', CAPTURE, None,
    )
    assert result.stadium == Stadium(
        stadium_id='56', name='Spotify Camp Nou', capacity=62657, international_capacity=62000,
        website='www.fcbarcelona.com/web/index_idiomes.html',
        address_line_1='Carrer d´Aristides Maillol, entrance no 7', address_line_2='08028 Barcelona',
        country_id='157', country_name='España', latitude=41.380896, longitude=2.1228198,
        build_year=1957, renovation_year=1994, field_length=105, field_width=68,
        field_surface='Hybridrasen', images=(IMAGE_1, IMAGE_2), source_is_historical=False,
    )
    assert result.relation == ClubStadiumRelation('131', '56', CAPTURE)
    with pytest.raises(FrozenInstanceError):
        result.stadium.capacity = 10


def test_null_renovation_year_preserves_the_stadium(envelope):
    envelope['data']['buildingDetails']['renovationYear'] = None
    result = parse_club_stadium(envelope, club_id=131, observed_at=CAPTURE)

    assert result.status == 'observed_present'
    assert result.stadium.renovation_year is None
    assert (result.stadium.build_year, result.stadium.capacity) == (1957, 62657)
    assert result.relation == ClubStadiumRelation('131', '56', CAPTURE)


def test_confirmed_empty():
    result = confirmed_empty_club_stadium(
        club_id=131, observed_at=CAPTURE, completeness_proof='capture/report-17',
    )
    assert json.loads(result.canonical_json()) == {
        'schema_revision': SCHEMA_REVISION, 'parser_revision': PARSER_REVISION,
        'club_id': '131', 'observed_at': CAPTURE, 'status': 'observed_empty',
        'completeness_proof': 'capture/report-17', 'stadium': None, 'relation': None,
    }


@pytest.mark.parametrize('kwargs', [{}, {'completeness_proof': None}, {'completeness_proof': '  '}])
def test_confirmed_empty_requires_external_proof(kwargs):
    with pytest.raises(ClubStadiumError) as error:
        confirmed_empty_club_stadium(club_id=131, observed_at=CAPTURE, **kwargs)
    assert error.value.reason == 'missing_completeness_proof'


@pytest.mark.parametrize(('payload', 'reason'), [
    ([], 'invalid_object'),
    ({'clubId': '131', 'name': 'Ground'}, 'unsuccessful_envelope'),
    ({'success': False, 'data': {'name': 'Ground'}}, 'unsuccessful_envelope'),
    ({'success': 1, 'data': {'name': 'Ground'}}, 'unsuccessful_envelope'),
    ({'success': True, 'message': []}, 'invalid_text'),
    ({'success': True}, 'invalid_object'),
    ({'success': True, 'data': None}, 'invalid_object'),
    ({'success': True, 'data': []}, 'invalid_object'),
    ({'success': True, 'data': {}}, 'empty_data'),
    ({'success': True, 'data': {'unrecognized': True}}, 'invalid_text'),
])
def test_broken_envelope_raises(payload, reason):
    with pytest.raises(ClubStadiumError) as error:
        parse_club_stadium(payload, club_id='131', observed_at=CAPTURE)
    assert error.value.reason == reason


@pytest.mark.parametrize(('attributes', 'reason', 'field'), [
    ({'clubId': 132}, 'club_mismatch', 'data.clubId'),
    ({'clubId': None}, 'invalid_id', 'data.clubId'),
    ({'id': True}, 'invalid_id', 'data.id'),
    ({'name': '  '}, 'blank_text', 'data.name'),
    ({'capacity': True}, 'invalid_integer', 'data.capacity'),
    ({'internationalCapacity': '90'}, 'invalid_integer', 'data.internationalCapacity'),
    ({'isHistorical': 1}, 'invalid_boolean', 'data.isHistorical'),
    ({'url': []}, 'invalid_text', 'data.url'),
    ({'location': []}, 'invalid_object', 'data.location'),
    ({'location': {'countryId': 3.5}}, 'invalid_id', 'data.location.countryId'),
    ({'location': {'street': None}}, 'invalid_text', 'data.location.street'),
    ({'location': {'postcode': 8028}}, 'invalid_text', 'data.location.postcode'),
    ({'location': {'latitude': float('nan')}}, 'invalid_coordinate', 'data.location.latitude'),
    ({'location': {'longitude': float('inf')}}, 'invalid_coordinate', 'data.location.longitude'),
    ({'location': {'latitude': False}}, 'invalid_coordinate', 'data.location.latitude'),
    ({'location': {'longitude': -181}}, 'invalid_coordinate', 'data.location.longitude'),
    ({'buildingDetails': None}, 'invalid_object', 'data.buildingDetails'),
    ({'buildingDetails': {'buildYear': None}}, 'invalid_integer', 'data.buildingDetails.buildYear'),
    ({'buildingDetails': {'renovationYear': False}}, 'invalid_integer', 'data.buildingDetails.renovationYear'),
    ({'buildingDetails': {'renovationYear': -1}}, 'invalid_integer', 'data.buildingDetails.renovationYear'),
    ({'buildingDetails': {'renovationYear': 1994.5}}, 'invalid_integer', 'data.buildingDetails.renovationYear'),
    ({'buildingDetails': {'renovationYear': '1994'}}, 'invalid_integer', 'data.buildingDetails.renovationYear'),
    ({'buildingDetails': {'fieldLength': 3.5}}, 'invalid_integer', 'data.buildingDetails.fieldLength'),
    ({'images': {}}, 'invalid_images', 'data.images'),
    ({'images': [IMAGE_1]}, 'invalid_object', 'data.images[0]'),
    ({'images': [{}]}, 'invalid_text', 'data.images[0].url'),
    ({'images': [{'url': 42}]}, 'invalid_text', 'data.images[0].url'),
    ({'images': [{'url': ' '}]}, 'blank_text', 'data.images[0].url'),
    ({'images': [{'url': IMAGE_1, 'source': []}]}, 'invalid_text', 'data.images[0].source'),
])
def test_invalid_attributes_raise(attributes, reason, field):
    data = {'name': 'Ground', **attributes}
    with pytest.raises(ClubStadiumError) as error:
        parse_club_stadium({'success': True, 'data': data}, club_id=131, observed_at=CAPTURE)
    assert (error.value.reason, error.value.field) == (reason, field)


@pytest.mark.parametrize('timestamp', [
    None, 'yesterday', '2026-10-03T08:05:00', '2026-10-03 08:05:00+00:00',
    '2026-02-30T12:00:00Z', '20261003T080500Z', '2026-10-03T08:05:00+01:60',
])
def test_rejects_invalid_capture_time(envelope, timestamp):
    with pytest.raises(ClubStadiumError) as error:
        parse_club_stadium(envelope, club_id=131, observed_at=timestamp)
    assert error.value.reason == 'invalid_observed_at'


@pytest.mark.parametrize('club_id', [True, 0, -1, 1.5, '0131', '1e2'])
def test_rejects_invalid_caller_id(envelope, club_id):
    with pytest.raises(ClubStadiumError) as error:
        parse_club_stadium(envelope, club_id=club_id, observed_at=CAPTURE)
    assert error.value.reason == 'invalid_id'


def test_replay_and_json_round_trip(envelope):
    args = {'club_id': '131', 'observed_at': '2026-10-03T06:05:00Z', 'country_names': {157: 'España'}}
    first = parse_club_stadium(envelope, **args)
    second = parse_club_stadium(json.loads(json.dumps(envelope)), **args)
    assert first == second
    encoded = first.canonical_json().encode('utf-8')
    assert encoded == second.canonical_json().encode('utf-8')
    doc = json.loads(encoded)
    assert encoded == json.dumps(
        doc, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False,
    ).encode('utf-8')
    assert doc['club_id'] == doc['relation']['club_id'] == '131'
    assert doc['stadium']['stadium_id'] == doc['relation']['stadium_id'] == '56'
    assert doc['observed_at'] == doc['relation']['observed_at'] == '2026-10-03T06:05:00+00:00'
    assert doc['stadium']['country_id'] == '157'
    assert doc['stadium']['images'] == [IMAGE_1, IMAGE_2]


def test_missing_id_preserves_club_scoped_observation():
    result = parse_club_stadium(
        {'success': True, 'data': {'name': 'Local Ground'}}, club_id='131', observed_at=CAPTURE,
    )
    assert result.status == 'observed_present'
    assert result.stadium.stadium_id is None
    assert result.relation == ClubStadiumRelation('131', None, CAPTURE)
    assert all(value is None for key, value in json.loads(result.canonical_json())['stadium'].items()
               if key != 'name')


def test_shared_venue_has_separate_club_relations(envelope):
    first = parse_club_stadium(envelope, club_id=131, observed_at=CAPTURE)
    envelope['data']['clubId'] = '132'
    second = parse_club_stadium(envelope, club_id=132, observed_at=CAPTURE)
    assert first.stadium == second.stadium
    assert first.relation == ClubStadiumRelation('131', '56', CAPTURE)
    assert second.relation == ClubStadiumRelation('132', '56', CAPTURE)


@pytest.mark.parametrize('historical', [True, False])
def test_no_published_period(envelope, historical):
    envelope['data']['isHistorical'] = historical
    result = parse_club_stadium(envelope, club_id=131, observed_at=CAPTURE)
    assert result.stadium.source_is_historical is historical
    assert (result.stadium.build_year, result.stadium.renovation_year) == (1957, 1994)
    assert result.relation.observed_at == CAPTURE
    assert result.relation.valid_from is None
    assert result.relation.valid_to is None


@pytest.mark.parametrize(('parts', 'line_2'), [
    ({'postcode': '08028'}, '08028'), ({'city': ' Barcelona '}, 'Barcelona'),
    ({'postcode': ' ', 'city': ''}, None),
])
def test_partial_address_and_unresolved_country(parts, line_2):
    location = {'street': ' ', 'countryId': 157, **parts}
    result = parse_club_stadium(
        {'success': True, 'data': {'name': 'Ground', 'location': location}},
        club_id='131', observed_at=CAPTURE, country_names={5: 'Country Five'},
    )
    assert result.stadium.address_line_1 is None
    assert result.stadium.address_line_2 == line_2
    assert result.stadium.country_id == '157'
    assert result.stadium.country_name is None


def _club_profile_payload():
    return {'success': True, 'data': {
        'id': 17, 'name': 'Example FC', 'relativeUrl': '/example/startseite/verein/17',
        'crestUrl': 'https://images.example/17.png',
        'preferences': {'clubCode': 'EFC'},
        'baseDetails': {
            'shortName': 'Example', 'abbreviation': 'EX', 'isNationalTeam': False,
            'superiorClub': {
                'name': 'Example Football Club',
                'colors': {'second': 'blue', 'unset': False, 'blank': '', 'first': 'red'},
                'location': {'street': '  Main Road  ', 'postcode': ' 020 ',
                             'city': ' Example City ', 'countryId': 4},
            },
        },
    }}


def _parse_profile_fixture(payload, **kwargs):
    from scrapers.transfermarkt.club_stadiums import parse_club_profile

    return parse_club_profile(payload, club_id='17', observed_at='2025-06-17T00:00:00Z',
                              **kwargs)


def test_profile_official_fields_and_deterministic_json():
    import json
    from dataclasses import FrozenInstanceError

    profile = _parse_profile_fixture(_club_profile_payload(), country_names={'4': 'Exampleland'})
    assert profile.colors == ('blue', 'red')
    assert profile.is_national_team is False
    actual = json.loads(profile.canonical_json())
    assert actual.pop('observed_at')
    assert actual == {
        'club_id': '17', 'url': '/example/startseite/verein/17', 'name': 'Example FC',
        'short_name': 'Example', 'abbreviation': 'EX', 'club_code': 'EFC',
        'official_name': 'Example Football Club', 'image': 'https://images.example/17.png',
        'address_line_1': 'Main Road', 'address_line_2': '020 Example City',
        'address_line_3': 'Exampleland', 'country_id': '4', 'colors': ['blue', 'red'],
        'is_national_team': False,
    }
    assert profile.canonical_json() == _parse_profile_fixture(
        _club_profile_payload(), country_names={'4': 'Exampleland'}).canonical_json()
    with pytest.raises(FrozenInstanceError):
        profile.name = 'changed'


def test_profile_national_team_and_missing_optional_fields():
    payload = _club_profile_payload()
    payload['data']['baseDetails']['isNationalTeam'] = True
    result = _parse_profile_fixture(payload)
    assert result.is_national_team is True
    assert result.official_name is None
    assert result.address_line_3 is None
    minimal = _parse_profile_fixture({'success': True, 'data': {'id': '17', 'name': 'X'}})
    assert minimal.colors == ()
    for key, value in minimal.as_dict().items():
        if key not in {'club_id', 'observed_at', 'colors', 'name'}:
            assert value is None, key


@pytest.mark.parametrize(('path', 'value'), [
    (('success',), 1), (('data',), []), (('data', 'id'), '18'),
    (('data', 'name'), ' '), (('data', 'relativeUrl'), 23),
    (('data', 'baseDetails'), False), (('data', 'preferences'), []),
    (('data', 'baseDetails', 'isNationalTeam'), 1),
    (('data', 'baseDetails', 'superiorClub', 'colors'), []),
    (('data', 'baseDetails', 'superiorClub', 'colors', 'second'), True),
    (('data', 'baseDetails', 'superiorClub', 'location', 'postcode'), 20),
])
def test_profile_rejects_malformed_present_values(path, value):
    from scrapers.transfermarkt.club_stadiums import ClubStadiumError

    payload = _club_profile_payload()
    parent = payload
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(ClubStadiumError):
        _parse_profile_fixture(payload)
