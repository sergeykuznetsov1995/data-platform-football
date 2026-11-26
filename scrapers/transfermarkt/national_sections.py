"""T9 national career aggregates and their independently captured team references.

Aggregate fields and enum follow felipeall/transfermarkt-api,
transfermarkt_api/services/players/national_career.py at 71aa75ecd708e5c8c283c4af5968dd370777b3f8,
adapted for offline parsing in Football #2289 (03ad0078). See licenses/transfermarkt-api-felipeall-MIT.txt.
"""

from __future__ import annotations

from datetime import date
import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .section_contracts import ObservationTable, make_outcome


_AGGREGATES = 'national_career_aggregate_observations'
_TEAMS = 'national_team_level_observations'
_FLAGS = 'national_callup_flag_observations'
_SQUADS = 'national_tournament_squad_observations'
_CALLUPS = 'national_callup_observations'
_PARTICIPANTS = 'national_tournament_participant_observations'
_ROSTER_SCOPE = ('competition_id', 'edition_id', 'national_team_id')
TABLES = {
    _PARTICIPANTS: ObservationTable(
        (*_ROSTER_SCOPE, 'national_team_name', 'published_squad_size', 'participation_section'),
        (*_ROSTER_SCOPE, 'observed_at'), ('competition_id', 'edition_id'), {'published_squad_size': 'bigint'}),
    _FLAGS: ObservationTable(('club_id', 'player_id', 'source_flag', 'callup_identity_status'),
                             ('club_id', 'player_id', 'observed_at'), ('club_id',)),
    _AGGREGATES: ObservationTable(
        ('player_id', 'national_team_id', 'period', 'metric', 'metric_value', 'shirt_number', 'is_captain',
         'national_debut', 'national_last_game', 'national_career_status'),
        ('player_id', 'national_team_id', 'period', 'metric', 'observed_at'), ('player_id',),
        {'metric_value': 'bigint', 'national_debut': 'date', 'national_last_game': 'date'}),
    _TEAMS: ObservationTable(
        ('team_packet', 'national_team_id', 'national_team_name', 'national_team_type_id', 'main_national_team_id'),
        ('national_team_id', 'observed_at'), ('team_packet',)),
    _SQUADS: ObservationTable((*_ROSTER_SCOPE, 'player_id'), (*_ROSTER_SCOPE, 'player_id', 'observed_at'),
                              _ROSTER_SCOPE),
    _CALLUPS: ObservationTable((*_ROSTER_SCOPE, 'player_id', 'callup_id'), ('callup_id', 'observed_at'),
                               _ROSTER_SCOPE),
}
ENDPOINT_ENTITIES = {
    'national_career': (_AGGREGATES,), 'national_team_reference': (_TEAMS,),
    'national_squad': (_SQUADS, _CALLUPS), 'national_callup_flags': (_FLAGS,),
    'national_tournament_participants': (_PARTICIPANTS,),
}
JSON_ENDPOINTS = frozenset({'national_career', 'national_team_reference'})
PAGINATED = frozenset()
_ID = r'[1-9][0-9]*'
_API = r'https://tmapi\.transfermarkt\.technology'
_CLUB_PAGE = re.compile(rf'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/startseite/verein/({_ID})/?')
_PLAYER_LINK = re.compile(rf'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/profil/spieler/({_ID})/?')
_CAREER = re.compile(rf'{_API}/player/({_ID})/national-career-history')
_PACKET_PARAM = rf'ids(?:\[\]|%5[Bb]%5[Dd])={_ID}'
_TEAMS_URL = re.compile(rf'{_API}/clubs\?({_PACKET_PARAM}(?:&{_PACKET_PARAM})*)')
_ROSTER = re.compile(rf'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/kader/verein/({_ID})'
                     r'/saison_id/([1-9][0-9]{3})(?:/plus/1)?/?')
# FIWC identifies the national-team World Cup. Club competitions use the same URL pattern.
_PARTICIPANT_URL = re.compile(r'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/teilnehmer/pokalwettbewerb/'
                              r'(FIWC)/saison_id/([1-9][0-9]{3})/?')
_STATUS = {
    'CURRENT_NATIONAL_PLAYER': 'current',
    'RECENT_NATIONAL_PLAYER': 'recent',
    'FORMER_NATIONAL_PLAYER': 'former',
}


def scope_from_url(endpoint, url, *, edition_id='', competition_id=''):
    if endpoint == 'national_tournament_participants' and (match := _PARTICIPANT_URL.fullmatch(url)):
        if competition_id and match[1] != competition_id or edition_id and match[2] != edition_id:
            raise ValueError('national_participants_requested_scope_mismatch')
        return {'competition_id': match[1], 'edition_id': match[2]}
    if endpoint == 'national_callup_flags' and (match := _CLUB_PAGE.fullmatch(url)):
        return {'club_id': match[1]}
    if endpoint == 'national_career' and (match := _CAREER.fullmatch(url)):
        return {'player_id': match[1]}
    if endpoint == 'national_team_reference' and (match := _TEAMS_URL.fullmatch(url)):
        ids = [term.split('=')[1] for term in match[1].split('&')]
        if len(ids) != len(set(ids)):
            raise ValueError('national_reference_duplicate_requested_id')
        return {'team_packet': ','.join(sorted(ids))}
    if endpoint == 'national_squad' and (match := _ROSTER.fullmatch(url)):
        if not re.fullmatch(r'[A-Z0-9]{2,6}', competition_id) or (edition_id and edition_id != match[2]):
            raise ValueError('national_squad_requested_edition_invalid')
        return {'competition_id': competition_id, 'edition_id': match[2], 'national_team_id': match[1]}
    raise ValueError('national_source_url_invalid')


def _identifier(value):
    if type(value) not in (int, str) or not re.fullmatch(_ID, str(value)):
        raise ValueError('national_source_identifier_invalid')
    return str(value)


def _count(value):
    if value is not None and (type(value) is not int or not 0 <= value <= 2 ** 63 - 1):
        raise ValueError('national_career_count_invalid')
    return value


def _date(value):
    if value is not None and (not isinstance(value, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value)
                              or date.fromisoformat(value).isoformat() != value):
        raise ValueError('national_career_date_invalid')
    return value


def _career(data, player_id):
    if not isinstance(data, dict) or not isinstance(data.get('history'), list):
        return 'unknown', 'history_not_published', (), None
    if 'playerId' in data and _identifier(data['playerId']) != player_id:
        raise ValueError('national_career_player_mismatch')
    teams = {}
    for entry in data['history']:
        if not isinstance(entry, dict):
            raise ValueError('national_career_row_invalid')
        if 'playerId' in entry and _identifier(entry['playerId']) != player_id:
            raise ValueError('national_career_player_mismatch')
        team_id = _identifier(entry.get('clubId'))
        state = entry.get('careerState')
        captain = entry.get('isCaptain')
        if not isinstance(state, str) or state not in _STATUS or (captain is not None and type(captain) is not bool):
            raise ValueError('national_career_attributes_invalid')
        fields = dict(player_id=player_id, national_team_id=team_id, period='career',
                      national_debut=_date(entry.get('debut')), national_last_game=_date(entry.get('lastGame')),
                      national_career_status=_STATUS[state], is_captain=captain, shirt_number=_count(entry.get('shirtNumber')))
        metrics = [dict(fields, metric=metric, metric_value=_count(entry.get(key)))
                   for metric, key in (('appearances', 'gamesPlayed'), ('goals', 'goalsScored'))]
        if teams.setdefault(team_id, metrics) != metrics:
            raise ValueError('national_career_conflicting_team')
    if 'clubIds' in data:
        inventory = data['clubIds']
        if not isinstance(inventory, list) or {_identifier(i) for i in inventory} != set(teams):
            raise ValueError('national_career_team_inventory_mismatch')
    if not teams:
        return 'observed_empty', 'published_empty_history', (), f'player/{player_id}:data.history=[]'
    records = [row for team in teams.values() for row in team]
    if any(row['metric_value'] is None for row in records):
        return 'unknown', 'career_metric_not_published', (), None
    return 'observed_present', 'published_career_metrics', records, None


def _references(data, team_packet):
    if not isinstance(data, list):
        raise ValueError('national_reference_list_missing')
    records = {}
    for row in data:
        if not isinstance(row, dict) or not isinstance(row.get('baseDetails'), dict):
            raise ValueError('national_reference_invalid')
        team_id = _identifier(row.get('id'))
        details = row['baseDetails']
        if (details.get('isNationalTeam') is not True or not isinstance(row.get('name'), str)
                or not row['name'].strip() or team_id in records):
            raise ValueError('national_reference_team_invalid')
        main = details.get('mainClubId')
        records[team_id] = dict(team_packet=team_packet, national_team_id=team_id,
                                national_team_name=row['name'], national_team_type_id=_identifier(details.get('clubTypeId')),
                                main_national_team_id=_identifier(main) if main is not None else None)
    if set(records) != set(team_packet.split(',')):
        raise ValueError('national_reference_inventory_mismatch')
    return 'observed_present', 'published_national_team_types', tuple(records.values()), None


def _club_callup_flags(capture, club_id):
    soup = BeautifulSoup(capture.text, 'html.parser')
    identities = [tag.get('content') or tag.get('href') for tag in soup.select('meta[property="og:url"], link[rel~=canonical]')]
    if not identities:
        return 'unknown', 'club_profile_identity_missing', ()
    if any(not isinstance(url, str) or not (owner := _CLUB_PAGE.fullmatch(url)) or owner[1] != club_id
           for url in identities):
        raise ValueError('national_callup_club_mismatch')
    flags = {}
    for span in soup.select('table.items span.ausfall-table[title]'):
        label = span['title'].strip()
        if not re.fullmatch(r'Called up to national team(?:\s*-.*)?', label, re.I):
            continue
        link = span.find_parent('a', href=True)
        href = link['href'] if link else ''
        if not (href.startswith('/') and not href.startswith('//') or href.startswith('https://www.transfermarkt.com/')):
            raise ValueError('callup_flag_player_missing')
        match = _PLAYER_LINK.fullmatch(urljoin(capture.url, href))
        if not match or flags.setdefault(match[1], label) != label:
            raise ValueError('callup_flag_player_conflict')
    if not flags:
        return 'unknown', 'no_published_callup_flag', ()
    # A club may flag a player's absence without publishing the identity, team or date of the call-up itself.
    return 'observed_present', 'source_explicitly_flags_national_callup', [
        dict(club_id=club_id, player_id=player_id, source_flag=label, callup_identity_status='unknown')
        for player_id, label in flags.items()]


def _tournament_participants(capture, scope):
    soup = BeautifulSoup(capture.text, 'html.parser')
    headers = soup.select('tm-competition-header')
    if (len(headers) != 1 or headers[0].get('competition-id') != scope['competition_id']
            or headers[0].get('season-id') != scope['edition_id']):
        raise ValueError('national_participants_competition_header_mismatch')
    seasons = soup.select('select[name="saison_id"] option[selected]')
    if len(seasons) != 1 or seasons[0].get('value') != scope['edition_id']:
        raise ValueError('national_participants_edition_mismatch')
    canonical = soup.select('meta[property="og:url"], link[rel~=canonical]')
    if not canonical or any(scope_from_url(capture.endpoint, tag.get('href') or tag.get('content') or '') != scope
                            for tag in canonical):
        raise ValueError('national_participants_page_mismatch')
    # This published section has rows for participating teams. Another grid on the same
    # page lists teams explicitly described as "Not qualified (yet)" with its own pager.
    section = 'Clubs starting into tournament at a later point'
    headings = [h for h in soup.select('div.box > h2.content-box-headline') if h.get_text(' ', strip=True) == section]
    if len(headings) != 1:
        return 'unknown', 'participant_section_not_published', ()
    tables = headings[0].parent.select('.grid-view > table.items')
    if len(tables) != 1:
        raise ValueError('national_participants_grid_missing')
    table = tables[0]
    grid = table.parent
    if grid.select_one('.pager, .tm-pagination'):
        return 'unknown', 'participating_group_pagination_unresolved', ()
    headers = [h.get_text(' ', strip=True).casefold() for h in table.select(':scope > thead th')]
    if headers.count('club') != 1 or headers.count('squad') != 1:
        raise ValueError('national_participants_headers_missing')
    records = []
    for tr in table.select(':scope > tbody > tr'):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(headers):
            raise ValueError('national_participants_row_invalid')
        anchors = cells[headers.index('club')].select('a[href]')
        if len(anchors) != 1:
            raise ValueError('national_participants_team_link_missing')
        name = anchors[0].get_text(' ', strip=True)
        href = anchors[0]['href']
        if not href.startswith('/') or href.startswith('//') or not name:
            raise ValueError('national_participants_team_link_invalid')
        team = _CLUB_PAGE.fullmatch('https://www.transfermarkt.com' + href)
        if not team:
            raise ValueError('national_participants_team_link_invalid')
        raw_size = cells[headers.index('squad')].get_text(strip=True)
        if raw_size not in {'', '-', '?'} and not re.fullmatch(r'(?:0|[1-9][0-9]{0,3})', raw_size):
            raise ValueError('national_participants_squad_size_invalid')
        records.append(dict(**scope, national_team_id=team[1], national_team_name=name,
                            participation_section=section, published_squad_size=int(raw_size) if raw_size.isdigit() else None))
    keys = grid.select(':scope > .keys[title]')
    ids = [r['national_team_id'] for r in records]
    if (len(keys) != 1 or not ids or len(ids) != len(set(ids))
            or scope_from_url(capture.endpoint, 'https://www.transfermarkt.com' + keys[0]['title']) != scope
            or ids != [t.get_text(strip=True) for t in keys[0].select(':scope > span')]):
        raise ValueError('national_participants_inventory_mismatch')
    return 'observed_present', 'published_participating_teams_and_squad_sizes', records


def extract(captures, scope, *, extracted_at, universe_version):
    capture, = captures
    endpoint = capture.endpoint
    entities = ENDPOINT_ENTITIES[endpoint]

    def outcome(entity, status, reason, records=(), proof=None):
        return make_outcome(entity, capture, scope, status=status, reason=f'{endpoint}:{reason}', records=records,
                            proof=proof, parser_revision='national-sections-v2', extracted_at=extracted_at,
                            universe_version=universe_version)

    if capture.status_code != 200:
        return tuple(outcome(entity, 'unavailable', f'http_{capture.status_code}') for entity in entities)
    try:
        fields = scope_from_url(endpoint, capture.url, edition_id=scope.edition_id or '',
                                competition_id=scope.competition_id or '')
        if endpoint == 'national_tournament_participants':
            return (outcome(_PARTICIPANTS, *_tournament_participants(capture, fields)),)
        if endpoint == 'national_callup_flags':
            if scope.club_id and scope.club_id != fields['club_id']:
                raise ValueError('national_callup_club_mismatch')
            return (outcome(_FLAGS, *_club_callup_flags(capture, fields['club_id'])),)
        if endpoint == 'national_squad':
            # This route names a team and season. Even a complete season roster cannot establish
            # registrations for a particular tournament or a separately published call-up object.
            return (outcome(_SQUADS, 'unknown', 'season_roster_lacks_tournament_registration'),
                    outcome(_CALLUPS, 'unknown', 'callup_object_not_published_by_roster'))
        payload = json.loads(capture.body)
        if not isinstance(payload, dict) or payload.get('success') is not True:
            return (outcome(entities[0], 'unknown', 'successful_source_envelope_missing'),)
        if endpoint == 'national_career':
            if scope.player_id and scope.player_id != fields['player_id']:
                raise ValueError('national_career_player_mismatch')
            result = _career(payload.get('data'), fields['player_id'])
        else:
            result = _references(payload.get('data'), fields['team_packet'])
        return (outcome(entities[0], *result),)
    except (ValueError, UnicodeError) as exc:
        return tuple(outcome(entity, 'error', str(exc)) for entity in entities)
