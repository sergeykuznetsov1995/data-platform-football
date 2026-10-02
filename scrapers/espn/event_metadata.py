"""Small core event metadata, bound to an existing Bronze match.

Refs are identity assertions only: no URL from a response is ever followed.
The separate fixed status endpoint remains the authority for status. Team
names and other existing schedule facts are carried, not fetched recursively.
"""
from dataclasses import dataclass, replace
from datetime import datetime
from urllib.parse import urlsplit

from .parser_common import (EspnParseError, decode_object, native_id, optional_bool,
                            required_list, required_mapping, source_day_contains,
                            source_year, utc_datetime)

PARSER_VERSION = 'espn-core-event-metadata-v1'


def _ref(value, path, field):
    if not isinstance(value, str):
        raise EspnParseError(f'{field} needs a core identity ref')
    ref = urlsplit(value)
    if (ref.scheme not in ('http', 'https') or ref.netloc != 'sports.core.api.espn.com'
            or ref.path != '/v2/sports/soccer/' + path or ref.fragment):
        raise EspnParseError(f'{field} does not match the requested core identity')


@dataclass(frozen=True)
class EventMetadata:
    kickoff: datetime
    kickoff_confirmed: bool

    def apply(self, schedule):
        return replace(schedule, kickoff=self.kickoff, date=self.kickoff, match_date=self.kickoff,
                       kickoff_confirmed=self.kickoff_confirmed, parser_version=PARSER_VERSION,
                       game=f'{self.kickoff.date().isoformat()} {schedule.home_team}-{schedule.away_team}')


def parse_event_metadata(raw, *, competition, edition, event):
    """Validate ID/league/year/two sides/date before adopting a changed kickoff."""
    payload = decode_object(raw, 'core event metadata')
    slug, event_id = competition.slug, event.event_id
    path = f'leagues/{slug}/events/{event_id}'
    if (event.competition_id != competition.espn_id or event.competition_slug != slug
            or event.source_season_year != edition.source_season_year):
        raise EspnParseError('core event context does not match stored match')
    if native_id(payload.get('id'), 'core event.id') != event_id:
        raise EspnParseError('core event ID differs from stored match')
    if payload.get('uid') != f's:600~l:{competition.espn_id}~e:{event_id}':
        raise EspnParseError('core event UID differs from league/event identity')
    if '$ref' in payload:
        _ref(payload['$ref'], path, 'core event')
    league = required_mapping(payload.get('league'), 'core event.league')
    _ref(league.get('$ref'), f'leagues/{slug}', 'core event.league')
    season = required_mapping(payload.get('season'), 'core event.season')
    _ref(season.get('$ref'), f'leagues/{slug}/seasons/{edition.source_season_year}', 'core event.season')
    if 'year' in season and source_year(season['year'], 'core event.season.year') != edition.source_season_year:
        raise EspnParseError('core event season year differs from stored match')
    nodes = required_list(payload.get('competitions'), 'core event.competitions')
    if len(nodes) != 1:
        raise EspnParseError('core event must contain exactly one competition')
    node = required_mapping(nodes[0], 'core event.competition')
    if native_id(node.get('id'), 'core competition.id') != event_id:
        raise EspnParseError('core competition ID differs from event')
    if '$ref' in node:
        _ref(node['$ref'], path + f'/competitions/{event_id}', 'core competition')
    if 'uid' in node and node['uid'] != f's:600~l:{competition.espn_id}~e:{event_id}~c:{event_id}':
        raise EspnParseError('core competition UID differs from event')
    expected = {'home': event.home_team_id, 'away': event.away_team_id}
    sides = {}
    for raw_side in required_list(node.get('competitors'), 'core competition.competitors'):
        side = required_mapping(raw_side, 'core competitor')
        home_away = side.get('homeAway')
        if home_away not in expected or home_away in sides:
            raise EspnParseError('core competitors need unique home and away sides')
        team_id = native_id(side.get('id'), 'core competitor.id')
        if team_id != expected[home_away]:
            raise EspnParseError('core competitor differs from stored team')
        team = required_mapping(side.get('team'), 'core competitor.team')
        _ref(team.get('$ref'), f'leagues/{slug}/seasons/{edition.source_season_year}/teams/{team_id}',
             'core competitor.team')
        if 'id' in team and native_id(team['id'], 'core team.id') != team_id:
            raise EspnParseError('core team ID differs from competitor')
        sides[home_away] = team_id
    if sides != expected:
        raise EspnParseError('core event must contain both stored teams')
    kickoff = utc_datetime(payload.get('date'), 'core event.date')
    if utc_datetime(node.get('date'), 'core competition.date') != kickoff:
        raise EspnParseError('core event and competition dates disagree')
    if not source_day_contains(kickoff.date(), edition.start_date, edition.end_date):
        raise EspnParseError('core kickoff is outside the stored edition')
    confirmed = (optional_bool(payload.get('timeValid'), 'core event.timeValid') is True
                 and optional_bool(node.get('timeValid'), 'core competition.timeValid') is True)
    return EventMetadata(kickoff, confirmed)
