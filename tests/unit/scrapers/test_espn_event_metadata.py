"""Recorded core event identity and date validation; no network."""
from copy import deepcopy
from datetime import date, datetime, timezone
import json

import pytest

from scrapers.espn import urls, wave
from scrapers.espn.event_metadata import PARSER_VERSION, parse_event_metadata
from scrapers.espn.parser_common import EspnParseError
from scrapers.espn.schedule_parser import parse_scoreboards
from tests.unit.scrapers.test_espn_wave import PROBES, _event

pytestmark = pytest.mark.unit


@pytest.fixture
def context():
    competition, edition = wave.TournamentWork(
        slug='eng.1', season_year=2026, event_ids=(401879276,), espn_id=700,
        name='Premier League', display_name='2026', start=date(2026, 7, 1),
        end=date(2027, 6, 30), days=(date(2026, 9, 19),)).context()
    event = _event('eng.1', when='2026-09-19T13:00Z', event_id=401879276, status='STATUS_SCHEDULED')
    event['season']['year'] = 2026
    for side, identity in zip(event['competitions'][0]['competitors'], (349, 364)):
        side['team']['id'] = str(identity)
    row, = parse_scoreboards(json.dumps(dict(leagues=[dict(id='700', slug='eng.1')], events=[event])).encode(),
                             competition=competition, edition=edition,
                             query_start=date(2026, 9, 19), query_end=date(2026, 9, 19))
    body = json.loads((PROBES / 'core_event_eng1_2026_401879276.json').read_bytes())
    return competition, edition, row, body


def parse(context, body):
    competition, edition, event, _ = context
    return parse_event_metadata(json.dumps(body).encode(), competition=competition, edition=edition, event=event)


def test_recorded_core_event_updates_date_without_changing_status_or_identity(context):
    value = parse(context, context[-1])
    row = value.apply(context[2])
    assert row.kickoff == datetime(2026, 9, 20, 13, tzinfo=timezone.utc)
    assert row.date == row.match_date == row.kickoff
    assert row.event_id == 401879276 and row.source_season_year == 2026
    assert row.status == 'STATUS_SCHEDULED' and not row.played_final
    assert row.parser_version == PARSER_VERSION
    assert row.game.startswith('2026-09-20 ')
    assert row.kickoff_confirmed
    request = urls.event_metadata('eng.1', row.event_id)
    assert request.url == 'https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/401879276'


@pytest.mark.parametrize('bad', ['event', 'uid', 'league', 'season', 'competition', 'team',
                               'homeaway', 'date_disagrees', 'outside_edition', 'invalid_timevalid'])
def test_conflicting_core_metadata_never_changes_kickoff(context, bad):
    body = deepcopy(context[-1])
    node = body['competitions'][0]
    if bad == 'event': body['id'] = '1'
    elif bad == 'uid': body['uid'] = 's:600~l:999~e:401879276'
    elif bad == 'league': body['league']['$ref'] = 'https://evil.example/v2/sports/soccer/leagues/eng.1'
    elif bad == 'season': body['season']['$ref'] = body['season']['$ref'].replace('2026', '2025')
    elif bad == 'competition': node['id'] = '1'
    elif bad == 'team': node['competitors'][0]['team']['$ref'] = node['competitors'][0]['team']['$ref'].replace('/349', '/999')
    elif bad == 'homeaway': node['competitors'][0]['homeAway'] = 'away'
    elif bad == 'date_disagrees': node['date'] = '2026-10-01T13:00Z'
    elif bad == 'outside_edition': body['date'] = node['date'] = '2030-01-01T13:00Z'
    elif bad == 'invalid_timevalid': node['timeValid'] = 'true'
    with pytest.raises(EspnParseError):
        parse(context, body)


def test_unconfirmed_source_time_stays_unconfirmed(context):
    body = deepcopy(context[-1])
    body['competitions'][0]['timeValid'] = False
    assert not parse(context, body).apply(context[2]).kickoff_confirmed


@pytest.mark.parametrize('missing', ['event', 'competition', 'both'])
def test_missing_timevalid_does_not_confirm_metadata_date(context, missing):
    body = deepcopy(context[-1])
    if missing in ('event', 'both'):
        body.pop('timeValid')
    if missing in ('competition', 'both'):
        body['competitions'][0].pop('timeValid')
    assert not parse(context, body).kickoff_confirmed
