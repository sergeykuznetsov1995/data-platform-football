"""T3 representative profiles, explicit personnel links and complete agency client lists.

The roster fields, page route, 25-row fallback and published total follow
thomas-u-grund/exclusive-affiliation-ergm-replication at 69ed21b6d70180057239f8b23ddf826cc3023c6d
(enrichment/{parse_agency_clients,scrape_tm,build_tables}.py).
Football #2283 (e31c6076) adapted the client rows for missing fields and conflicting duplicates.
See licenses/exclusive-affiliation-ergm-replication-MIT.txt.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import re
from urllib.parse import parse_qsl

from bs4 import BeautifulSoup


from .section_contracts import ObservationTable, make_outcome


_PROFILE = 'representative_profile_observations'
_AFFILIATIONS = 'agent_agency_observations'
_CLIENTS = 'agency_client_observations'
_PAGES = 'agency_client_page_observations'
_DIRECTORY = 'agency_directory_observations'
_STATISTICS = 'agency_statistics_page_observations'
_PROFILE_SCOPE = ('entity_type', 'entity_id')
TABLES = {
    _STATISTICS: ObservationTable(
        ('statistics_page', 'page_number', 'published_land_id', 'agency_id', 'agency_name', 'agency_country',
         'agency_profile_url', 'published_player_count', 'next_page_url', 'directory_completeness'),
        ('statistics_page', 'agency_id', 'observed_at'), ('statistics_page',), {'published_player_count': 'bigint'}),
    _DIRECTORY: ObservationTable(('lookup_query', 'agency_id', 'agency_name', 'listed_agents', 'agent_ids_json',
                                  'agent_identity_status', 'page_number'), ('lookup_query', 'agency_id', 'observed_at'), ('lookup_query',)),
    _PROFILE: ObservationTable((*_PROFILE_SCOPE, 'name', 'client_count', 'staff_listing_json'), (*_PROFILE_SCOPE, 'observed_at'),
                               _PROFILE_SCOPE, {'client_count': 'bigint'}),
    _AFFILIATIONS: ObservationTable((*_PROFILE_SCOPE, 'agent_id', 'agency_id', 'agent_name', 'relation_type'),
                                    ('agent_id', 'agency_id', 'relation_type', 'observed_at'), _PROFILE_SCOPE),
    _CLIENTS: ObservationTable(
        ('agency_id', 'player_id', 'relation_type', 'club_id', 'club_name', 'nationality1',
         'nationality2', 'age', 'market_value_eur', 'page_number'),
        ('agency_id', 'player_id', 'observed_at'), ('agency_id',)),
    _PAGES: ObservationTable(
        ('agency_id', 'page_number', 'client_count', 'source_row_count', 'new_client_count', 'page_client_ids',
         'next_page_url'), ('agency_id', 'page_number', 'observed_at'), ('agency_id',),
        {'client_count': 'bigint', 'source_row_count': 'bigint', 'new_client_count': 'bigint'}),
}
ENDPOINT_ENTITIES = {
    'agency_profile': (_PROFILE, _AFFILIATIONS), 'agent_profile': (_PROFILE,),
    'agency_clients': (_CLIENTS, _PAGES, _PROFILE, _AFFILIATIONS), 'agency_directory': (_DIRECTORY,),
    'agency_statistics': (_STATISTICS,),
}
JSON_ENDPOINTS = frozenset()
PAGINATED = frozenset({'agency_clients', 'agency_directory'})
_BASE = 'https://www.transfermarkt.com'
_PATH = r'https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/'
_ID = r'[1-9][0-9]*'
# A statistics page is addressed by its land filter, or since 2026-11-28 by
# the page of the all-countries grid alone.
_STATISTICS_URL = re.compile(r'https://www\.transfermarkt\.com/berater/beraterfirmenuebersicht/berater'
                             r'(?:\?landid=(0|[1-9][0-9]{0,3})(?:&page=([1-9][0-9]{0,4}))?'
                             r'|\?page=([1-9][0-9]{0,4}))?')
_AGENCY = re.compile(rf'({_PATH}beraterfirma/berater/({_ID}))(?:/page/({_ID}))?/?')
_AGENT = re.compile(rf'{_PATH}profil/berater/({_ID})/?')
_PLAYER = re.compile(rf'{_PATH}profil/spieler/({_ID})/?')
_CLUB = re.compile(rf'{_PATH}startseite/verein/({_ID})/?')
_COUNT = re.compile(r'\bKey Facts\s+Players\s*:\s*((?:[1-9][0-9]{0,2}(?:,[0-9]{3})+|[0-9]+))(?=\s|$)', re.I)
_MONEY = re.compile(r'€\s*([0-9]+(?:[.,][0-9]{1,2})?)\s*(k|m|mio|bn)?\.?', re.I)
_MAGNITUDES = {'': 1, 'k': 1000, 'm': 1000000, 'mio': 1000000, 'bn': 1000000000}
_MISSING = {'', '-', '?'}
_COLUMN_LABELS = {
    'player': {'player', 'player / position'}, 'age': {'age'}, 'flag': {'nat.', 'nationality', 'citizenship'},
    'club': {'current club', 'club'}, 'value': {'market value', 'market value (eur)'},
}


class AgencyError(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


@dataclass
class _WalkBounds:
    highest_seen: int = 1
    declared_last: int | None = None

    def check(self, number, following, highest, declared):
        self.highest_seen = max(self.highest_seen, highest, number)
        if declared is not None:
            if self.declared_last is not None and self.declared_last != declared:
                raise AgencyError('pager_last_changed_during_walk')
            self.declared_last = declared
        if self.declared_last is not None and self.highest_seen > self.declared_last:
            raise AgencyError('pager_extends_beyond_declared_last')
        if following is None and number < self.highest_seen:
            raise AgencyError('walk_ended_before_announced_page')


def scope_from_url(endpoint, url, *, edition_id='', competition_id=''):
    if endpoint == 'agency_statistics':
        land, number = agency_statistics_route(url)
        return {'statistics_page': f'{land or "default"}/{number}'}
    if endpoint == 'agency_directory':
        return {'lookup_query': agency_directory_route(url)[0]}
    if endpoint == 'agent_profile' and (match := _AGENT.fullmatch(url)):
        return {'entity_type': 'person', 'entity_id': match[1]}
    match = _AGENCY.fullmatch(url)
    if match and endpoint == 'agency_profile' and not match[3]:
        return {'entity_type': 'agency', 'entity_id': match[2]}
    if match and endpoint == 'agency_clients':
        return {'agency_id': match[2]}
    raise AgencyError('invalid_representative_url')


def scope_for_entity(entity, endpoint, values):
    if endpoint == 'agency_clients' and entity in {_PROFILE, _AFFILIATIONS}:
        return {'entity_type': 'agency', 'entity_id': values['agency_id']}
    return values


def page_number(endpoint, url):
    if endpoint == 'agency_directory':
        return agency_directory_route(url)[1]
    scope_from_url(endpoint, url)
    if endpoint not in PAGINATED:
        raise AgencyError('not_paginated')
    return int(_AGENCY.fullmatch(url)[3] or 1)


def _absolute(href):
    if not isinstance(href, str) or any(ch.isspace() for ch in href):
        raise AgencyError('source_link_invalid')
    if href.startswith('/') and not href.startswith('//'):
        return _BASE + href
    if href.startswith(_BASE + '/'):
        return href
    raise AgencyError('source_link_origin_invalid')


def _document(capture):
    soup = BeautifulSoup(capture.text, 'html.parser')
    expected = scope_from_url(capture.endpoint, capture.url)
    canonicals = soup.select('link[rel~=canonical], meta[property="og:url"]')
    for canonical in canonicals:
        url = _absolute(canonical.get('href') or canonical.get('content'))
        if scope_from_url(capture.endpoint, url) != expected:
            raise AgencyError('representative_page_identity_mismatch')
    if capture.endpoint in {'agency_profile', 'agency_clients', 'agent_profile'}:
        agency = capture.endpoint != 'agent_profile'
        owner = expected.get('agency_id') or expected['entity_id']
        navigations = soup.select('tm-subnavigation')
        if any(nav.get('controller') != 'berater' or nav.get('section', 'berater') != 'berater' or nav.get('id') != owner
               for nav in navigations):
            raise AgencyError('representative_navigation_identity_mismatch')
        title = soup.title.get_text(' ', strip=True) if soup.title else ''
        agency_header = soup.select_one('h2.advisor-header__headline')
        if (re.search(r'[-|]\s*(?:referee|player|manager|coach) profile\b', title, re.I)
                or agency and re.search(r'[-|]\s*agent profile\b', title, re.I)
                or not agency and agency_header is not None):
            raise AgencyError('representative_page_type_mismatch')
        if not canonicals and not (agency and agency_header is not None and navigations):
            raise AgencyError('representative_page_identity_missing')
    return soup


def _count(soup):
    values = {int(n.replace(',', '')) for n in _COUNT.findall(soup.get_text(' ', strip=True))}
    if len(values) > 1:
        raise AgencyError('conflicting_client_counts')
    return next(iter(values), None)


def _table(soup):
    candidates = []
    for table in soup.select('table.items'):
        thead = table.find('thead', recursive=False)
        if thead is None:
            continue
        headers = [' '.join(th.stripped_strings).strip().casefold() for th in thead.select('th')]
        if set(headers) & _COLUMN_LABELS['player']:
            indices = {}
            for key, labels in _COLUMN_LABELS.items():
                matches = [i for i, title in enumerate(headers) if title in labels]
                if len(matches) != 1:
                    raise AgencyError('clients_table_headers_changed')
                indices[key] = matches[0]
            candidates.append((table, indices, len(headers)))
    if len(candidates) != 1:
        raise AgencyError('clients_table_missing_or_ambiguous')
    return candidates[0]


def _pagination(capture, number, soup, table, count):
    if page_number(capture.endpoint, capture.url) != number:
        raise AgencyError('page_number_mismatch')
    if count is None:
        raise AgencyError('published_client_count_missing')
    agency_id = scope_from_url(capture.endpoint, capture.url)['agency_id']
    region = table.find_parent(class_='grid-view') or table.find_parent(class_='box') or soup
    # A pager can expose only a window. The published client count supplies the
    # missing tail on the donor's standard 25-row layout.
    last = max(1, (count + 24) // 25)
    next_links = set()
    declared_last = set()
    following_links = set()
    for link in region.select('.pager a[href], .tm-pagination a[href]'):
        url = _absolute(link['href'])
        if scope_from_url('agency_clients', url)['agency_id'] != agency_id:
            raise AgencyError('pager_agency_mismatch')
        page = page_number('agency_clients', url)
        item = link.find_parent('li')
        classes = set(item.get('class', ())) if item is not None else set()
        if classes & {'next', 'tm-pagination__list-item--icon-next', 'tm-pagination__list-item--icon-next-page'} or 'next' in (link.get('rel') or ()):
            if page != number + 1:
                raise AgencyError('invalid_next_page')
            next_links.add(url)
        if (classes & {'last', 'tm-pagination__list-item--icon-last', 'tm-pagination__list-item--icon-last-page'}
                or 'last' in (link.get('rel') or ())):
            declared_last.add(page)
        if page == number + 1:
            following_links.add(url)
        last = max(last, page)
    if count == 0 and (number != 1 or last != 1):
        raise AgencyError('zero_client_pager_conflict')
    if len(declared_last) > 1 or (declared_last and last not in declared_last):
        raise AgencyError('pager_last_mismatch')
    if len(next_links or following_links) > 1:
        raise AgencyError('ambiguous_next_page')
    following = None
    if number < last or next_links:
        match = _AGENCY.fullmatch(capture.url)
        following = next(iter(next_links or following_links), f'{match[1]}/page/{number + 1}')
    return following, last, next(iter(declared_last), None)


def next_page(capture, page_number):
    if capture.endpoint == 'agency_directory':
        return _directory_page(capture, page_number)[2][0]
    soup = _document(capture)
    table, _, _ = _table(soup)
    return _pagination(capture, page_number, soup, table, _count(soup))[0]


def _links(cell, route, marker):
    by_id = {}
    for link in cell.find_all('a', href=True):
        if marker not in link['href']:
            continue
        match = route.fullmatch(_absolute(link['href']))
        if match is None:
            raise AgencyError('entity_link_malformed')
        title = (link.get('title') or link.get_text(' ', strip=True)).strip()
        previous = by_id.get(match[1])
        if title and previous and previous != title:
            raise AgencyError('entity_link_conflict')
        by_id[match[1]] = previous or title
    if len(by_id) > 1:
        raise AgencyError('entity_link_conflict')
    return next(iter(by_id.items()), (None, None))


def _client_row(tr, cols, width):
    cells = tr.find_all('td', recursive=False)
    if len(cells) != width or not {'odd', 'even'}.intersection(tr.get('class', ())):
        raise AgencyError('client_row_shape_changed')
    player_id, _ = _links(cells[cols['player']], _PLAYER, '/profil/spieler/')
    if not player_id:
        raise AgencyError('client_id_missing')
    club_cell = cells[cols['club']]
    club_id, club_name = _links(club_cell, _CLUB, '/startseite/verein/')
    if not club_id and (club_cell.get_text(strip=True) not in _MISSING or club_cell.select_one('a[href], img')):
        raise AgencyError('club_link_missing')
    flag_cell = cells[cols['flag']]
    flags = [str(img.get('title', '')).strip() for img in flag_cell.select('img.flaggenrahmen')]
    if (len(flags) > 2 or any(not name for name in flags)
            or (not flags and (flag_cell.get_text(strip=True) not in _MISSING or flag_cell.select_one('img')))):
        raise AgencyError('nationality_invalid')
    flags.extend([None] * (2 - len(flags)))
    age = cells[cols['age']].get_text(strip=True)
    if age not in _MISSING and not re.fullmatch('[0-9]{1,3}', age):
        raise AgencyError('age_invalid')
    amount = cells[cols['value']].get_text(' ', strip=True)
    value = None
    if amount not in _MISSING:
        match = _MONEY.fullmatch(amount)
        if not match:
            raise AgencyError('client_market_value_invalid')
        euros = Decimal(match[1].replace(',', '.')) * _MAGNITUDES[(match[2] or '').lower()]
        if euros != euros.to_integral_value() or euros > 2 ** 63 - 1:
            raise AgencyError('client_market_value_invalid')
        value = int(euros)
    return dict(player_id=player_id, club_id=club_id, club_name=club_name, nationality1=flags[0],
                nationality2=flags[1], age=None if age in _MISSING else int(age), market_value_eur=value)


def _client_walk(captures):
    expected_count = None
    next_url = captures[0].url
    bounds = _WalkBounds()
    remaining = []
    by_player = {}
    seen_captures = set()
    for number, capture in enumerate(captures, 1):
        if capture.endpoint != 'agency_clients' or capture.url != next_url or capture.capture_id in seen_captures:
            raise AgencyError('client_page_chain_broken')
        seen_captures.add(capture.capture_id)
        soup = _document(capture)
        table, cols, width = _table(soup)
        count = _count(soup)
        pager = _pagination(capture, number, soup, table, count)
        bounds.check(number, *pager)
        next_url = pager[0]
        if number > 1 and count != expected_count:
            raise AgencyError('client_inventory_changed')
        expected_count = count
        body = table.find('tbody', recursive=False)
        if body is None:
            raise AgencyError('client_table_body_missing')
        rows = [_client_row(tr, cols, width) for tr in body.find_all('tr', recursive=False)]
        if count and not rows:
            raise AgencyError('unproven_empty_page')
        ids = [row['player_id'] for row in rows]
        grid = table.find_parent(class_='grid-view')
        inventories = grid.select(':scope > .keys') if grid is not None else []
        if len(inventories) > 1:
            raise AgencyError('client_page_inventory_ambiguous')
        if inventories:
            published = [tag.get_text(strip=True) for tag in inventories[0].select(':scope > span')]
            if published != list(inventories[0].stripped_strings) or published != ids:
                raise AgencyError('client_page_player_inventory_mismatch')
        new_rows = []
        for row in rows:
            player = row['player_id']
            if player in by_player:
                if by_player[player] != row:
                    raise AgencyError('conflicting_duplicate_client')
            else:
                by_player[player] = row
                new_rows.append(row)
        page = dict(page_number=number, client_count=count, source_row_count=len(rows), new_client_count=len(new_rows),
                    page_client_ids=json.dumps(ids, separators=(',', ':')), next_page_url=next_url)
        remaining.append((capture, page, new_rows))
    if next_url is not None or len(by_player) != expected_count:
        raise AgencyError('incomplete_agency_client_roster')
    return remaining


def _staff_identity(row, listed_name):
    agent_id, link_name = _links(row, _AGENT, '/profil/berater/')
    name = listed_name if listed_name not in _MISSING else link_name
    if not agent_id or not name or name in _MISSING:
        return agent_id, None
    for link in row.select('a[href*="/profil/berater/"]'):
        captions = [link.get_text(' ', strip=True), link.get('title', ''), link.get('aria-label', '')]
        captions.extend(img['alt'] for img in link.select('img[alt]'))
        if any(caption.strip() and caption.strip() != name for caption in captions):
            raise AgencyError('conflicting_agency_staff')
    return agent_id, name


def _staff(soup, agency_id):
    tables = []
    for box in soup.select('div.box'):
        heading = box.find('h2', class_='content-box-headline')
        if heading and heading.get_text(' ', strip=True).casefold() in {'staff', 'agents', 'mitarbeiter', 'berater'}:
            tables.extend(box.select('table.items'))
    if len(tables) != 1:
        return (), None, False
    body = tables[0].find('tbody', recursive=False) or tables[0]
    members = {}
    listings = []
    unresolved = False
    for tr in body.find_all('tr', recursive=False):
        labels = tr.select('strong.employee-label')
        listed_name = labels[0].get_text(' ', strip=True) if len(labels) == 1 else ''
        agent_id, name = _staff_identity(tr, listed_name)
        if listed_name not in _MISSING:
            cell = labels[0].find_parent('td') or labels[0].parent
            roles = [match[1] for span in cell.find_all('span', recursive=False)
                     if (match := re.fullmatch(r'\(([^()]+)\)', span.get_text(' ', strip=True)))]
            licensed = cell.select_one('.licensed__label[title="Officially licensed"]')
            listings.append(dict(source_name=listed_name, source_roles=roles, licensed_label=licensed['title'] if licensed else None,
                                 agent_id=agent_id))
        if agent_id and name:
            if members.setdefault(agent_id, name) != name:
                raise AgencyError('conflicting_agency_staff')
        else:
            unresolved = True
    relations = tuple(dict(entity_type='agency', entity_id=agency_id, agency_id=agency_id, agent_id=agent_id,
                           agent_name=name, relation_type='agent_agency') for agent_id, name in members.items())
    return relations, json.dumps(listings, ensure_ascii=False, separators=(',', ':')) if listings else None, unresolved


def _profiles(capture, identity, outcome):
    soup = _document(capture)
    agency = identity['entity_type'] == 'agency'
    headings = soup.select('h1.data-header__headline-wrapper, h2.advisor-header__headline' if agency
                           else 'h1.data-header__headline-wrapper')
    name = headings[0].get_text(' ', strip=True) if len(headings) == 1 else ''
    results = []
    staff = ()
    unresolved = False
    listing = staff_error = None
    if agency:
        try:
            staff, listing, unresolved = _staff(soup, identity['entity_id'])
        except AgencyError as exc:
            staff_error = exc.reason
    if name and name not in _MISSING:
        count = _count(soup) if agency else None
        results.append(outcome(_PROFILE, capture, 'observed_present', 'published_profile',
                               [dict(identity, name=name, client_count=count, staff_listing_json=listing)]))
    else:
        results.append(outcome(_PROFILE, capture, 'unknown', 'profile_identity_or_name_not_published'))
    if agency:
        if staff_error:
            results.append(outcome(_AFFILIATIONS, capture, 'error', staff_error))
        else:
            reason = 'published_personnel_links_partial_identity' if unresolved else 'published_personnel_links'
            results.append(outcome(_AFFILIATIONS, capture, 'observed_present' if staff else 'unknown',
                                   reason if staff else 'personnel_identity_not_published', staff))
    return tuple(results)


def _client_outcomes(captures, identity, outcome):
    results = []
    try:
        pages = _client_walk(captures)
    except (ValueError, UnicodeError) as exc:
        return tuple(outcome(entity, captures[0], 'error', getattr(exc, 'reason', 'invalid_source_data'))
                     for entity in (_CLIENTS, _PAGES))
    for capture, page, records in pages:
        if records:
            rows = [dict(identity, **record, relation_type='agency_client', page_number=page['page_number'])
                    for record in records]
            results.append(outcome(_CLIENTS, capture, 'observed_present', 'published_clients', rows))
        elif page['client_count'] == 0:
            results.append(outcome(_CLIENTS, capture, 'observed_empty', 'published_zero_clients',
                                   proof='Key Facts Players: 0 and a recognized empty client table'))
        results.append(outcome(_PAGES, capture, 'observed_present', 'reconciled_client_page', [dict(identity, **page)]))
    return tuple(results)


def extract(captures, scope, *, extracted_at, universe_version):
    first = captures[0]
    endpoint = first.endpoint
    entities = ENDPOINT_ENTITIES[endpoint]

    def outcome(entity, capture, status, reason, records=(), proof=None):
        return make_outcome(entity, capture, scope, status=status, reason=f'{endpoint}:{reason}',
                            records=records, proof=proof, parser_revision='agency-sections-v6',
                            extracted_at=extracted_at, universe_version=universe_version)

    for capture in captures:
        if capture.status_code != 200:
            return tuple(outcome(entity, capture, 'unavailable', f'http_{capture.status_code}') for entity in entities)
    try:
        identity = scope_from_url(endpoint, first.url)
        if endpoint == 'agency_statistics':
            if len(captures) != 1:
                raise AgencyError('statistics_page_has_multiple_captures')
            rows = _statistics_page(first, identity['statistics_page'])
            return (outcome(_STATISTICS, first, 'observed_present', 'published_directory_page', rows),)
        if endpoint == 'agency_directory':
            walked = _directory_walk(captures)
            if not walked[0][1]:
                return (outcome(_DIRECTORY, first, 'observed_empty', 'search_declared_zero_hits',
                                proof='Search results for agents: 0 Hits and an empty identified results table'),)
            return tuple(outcome(_DIRECTORY, cap, 'observed_present', 'complete_search_query', rows)
                         for cap, rows in walked)
        if endpoint == 'agency_clients':
            if page_number(endpoint, first.url) != 1:
                raise AgencyError('agency_walk_must_start_on_first_page')
            profiles = _profiles(first, scope_for_entity(_PROFILE, endpoint, identity), outcome)
            return _client_outcomes(captures, identity, outcome) + profiles
        if len(captures) != 1:
            raise AgencyError('unexpected_profile_page')
        return _profiles(first, identity, outcome)
    except (ValueError, UnicodeError) as exc:
        reason = getattr(exc, 'reason', 'invalid_source_data')
        status = 'unknown' if reason == 'representative_page_identity_missing' else 'error'
        return tuple(outcome(entity, first, status, reason) for entity in entities)


def agency_directory_route(url):
    """A public agency lookup by name, restricted to the agent results' own page parameter."""
    prefix = _BASE + '/schnellsuche/ergebnis/schnellsuche?'
    if not isinstance(url, str) or not url.startswith(prefix):
        raise AgencyError('directory_url_invalid')
    query = url[len(prefix):]
    if (not query or any(ord(c) < 33 or ord(c) == 127 for c in query) or '#' in query or '?' in query
            or re.search(r'%(?![0-9a-fA-F]{2})', query)):
        raise AgencyError('directory_query_invalid')
    try:
        entries = parse_qsl(query, strict_parsing=True, keep_blank_values=True, errors='strict', max_num_fields=4)
    except ValueError:
        raise AgencyError('directory_query_invalid') from None
    params = dict(entries)
    if (len(params) != len(entries) or params.keys() - {'query', 'x', 'y', 'page'}
            or (('x' in params) != ('y' in params)) or params.get('x', '0') != '0' or params.get('y', '0') != '0'):
        raise AgencyError('directory_parameters_invalid')
    term = params.get('query', '')
    if (not 1 <= len(term) <= 80 or term.strip() != term or not any(c.isalnum() for c in term)
            or not all(c.isalpha() or c.isdecimal() or c in " .'-’" for c in term)):
        raise AgencyError('directory_name_invalid')
    page = params.get('page', '1')
    if not re.fullmatch(_ID, page):
        raise AgencyError('directory_page_invalid')
    return term, int(page)


def agency_statistics_route(url):
    """One explicitly requested page of the source's agency statistics grid."""
    match = _STATISTICS_URL.fullmatch(url)
    if not match:
        raise AgencyError('agency_statistics_url_invalid')
    return match[1], int(match[2] or match[3] or '1')


def _statistics_page(capture, statistics_page):
    requested_land, number = agency_statistics_route(capture.url)
    soup = BeautifulSoup(capture.text, 'html.parser')
    grids = [box for box in soup.select('div.box') if (h := box.find('h2', class_='content-box-headline'))
             and h.get_text(' ', strip=True).casefold() == 'players agency']
    if len(grids) != 1 or len(tables := grids[0].select('.grid-view table.items')) != 1:
        raise AgencyError('agency_statistics_grid_missing')
    table = tables[0]
    grid = table.find_parent(class_='grid-view')
    keys = grid.select(':scope > .keys[title]')
    if len(keys) != 1:
        raise AgencyError('agency_statistics_keys_missing')
    land, published_page = agency_statistics_route(_absolute(keys[0]['title']))
    # The all-countries page publishes no land filter; since 2026-11-28 its
    # keys, canonical link and pager all agree on that.
    if requested_land not in (None, land) or number != published_page:
        raise AgencyError('agency_statistics_scope_mismatch')
    canonical = soup.select('meta[property="og:url"]')
    if len(canonical) != 1 or agency_statistics_route(_absolute(canonical[0].get('content'))) not in {(land, 1), (land, number)}:
        raise AgencyError('agency_statistics_scope_mismatch')

    following = set()
    active = set()
    later = False
    for link in grid.select('.tm-pagination li a[href]'):
        url = _absolute(link['href'])
        page_land, page = agency_statistics_route(url)
        classes = link.find_parent('li').get('class', ())
        if page_land != land:
            raise AgencyError('agency_statistics_pager_scope_mismatch')
        if 'tm-pagination__list-item--active' in classes:
            active.add(page)
        if 'tm-pagination__list-item--icon-next-page' in classes:
            if page != number + 1:
                raise AgencyError('agency_statistics_next_page_invalid')
            following.add(url)
        later |= page > number
    if ((grid.select_one('.pager, .tm-pagination') is not None and active != {number})
            or len(following) != int(later)):
        raise AgencyError('agency_statistics_pager_incomplete')
    next_url = next(iter(following), None)
    heads = [h.get_text(' ', strip=True).casefold() for h in table.select(':scope > thead th')]
    if heads.count('players agency') != 1 or heads.count('player') != 1:
        raise AgencyError('agency_statistics_headers_missing')
    records = []
    for tr in table.select(':scope > tbody > tr'):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(heads):
            raise AgencyError('agency_statistics_row_invalid')
        company = cells[heads.index('players agency')]
        anchors = company.select('a[href*="/beraterfirma/berater/"]')
        if len(anchors) != 1 or not (name := anchors[0].get_text(' ', strip=True)):
            raise AgencyError('agency_statistics_agency_missing')
        profile = _absolute(anchors[0]['href'])
        agency_id = scope_from_url('agency_profile', profile)['entity_id']
        count = cells[heads.index('player')]
        raw_count = count.get_text(strip=True)
        if not re.fullmatch(r'0|[1-9][0-9]{0,8}', raw_count):
            raise AgencyError('agency_statistics_player_count_invalid')
        if any(scope_from_url('agency_profile', _absolute(a['href']))['entity_id'] != agency_id for a in count.select('a[href]')):
            raise AgencyError('agency_statistics_player_count_mismatch')
        flags = company.select('img.flaggenrahmen[title]')
        if len(flags) > 1:
            raise AgencyError('agency_statistics_country_ambiguous')
        records.append(dict(statistics_page=statistics_page, page_number=number, published_land_id=land, agency_id=agency_id,
                            agency_name=name, agency_country=flags[0]['title'] if flags else None, agency_profile_url=profile,
                            published_player_count=int(raw_count), next_page_url=next_url, directory_completeness='unknown'))
    ids = [r['agency_id'] for r in records]
    if not ids or len(ids) != len(set(ids)) or [e.get_text(strip=True) for e in keys[0].select(':scope > span')] != ids:
        raise AgencyError('agency_statistics_inventory_mismatch')
    return records


def _directory_page(capture, number):
    query, current = agency_directory_route(capture.url)
    if number != current:
        raise AgencyError('directory_page_number_mismatch')
    soup = _document(capture)
    rows, count, region = _directory_rows(soup, query, number)
    links = set()
    highest = number
    declared = set()
    for link in region.select('.pager a[href], .tm-pagination a[href]'):
        url = _absolute(link['href'])
        term, page = agency_directory_route(url)
        item = link.find_parent('li')
        classes = item.get('class', ()) if item else ()
        rel = link.get('rel') or ()
        if term != query or (('next' in rel or 'next' in classes or 'tm-pagination__list-item--icon-next-page' in classes)
                             and page != number + 1):
            raise AgencyError('directory_pager_scope_mismatch')
        if 'last' in rel or {'last', 'tm-pagination__list-item--icon-last', 'tm-pagination__list-item--icon-last-page'}.intersection(classes):
            declared.add(page)
        if page == number + 1:
            links.add(url)
        highest = max(highest, page)
    if len(links) > 1 or (highest > number and not links):
        raise AgencyError('directory_pager_incomplete')
    if len(declared) > 1 or declared and highest not in declared:
        raise AgencyError('directory_pager_last_mismatch')
    return rows, count, (next(iter(links), None), highest, next(iter(declared), None))


def _directory_rows(soup, query, number):
    candidates = []
    for heading in soup.select('.box > .table-header, .box > h2.content-box-headline'):
        title = ' '.join(heading.stripped_strings)
        if match := re.fullmatch(r'Search results for agents\s*[-–]\s*([0-9]+)\s+Hits?', title, re.I):
            candidates.append((heading.parent, int(match[1])))
    if len(candidates) != 1:
        raise AgencyError('agency_search_results_not_identified')
    region, count = candidates[0]
    tables = region.select('table.items')
    if len(tables) != 1:
        raise AgencyError('directory_table_missing_or_ambiguous')
    table = tables[0]
    heads = [' '.join(th.stripped_strings).casefold() for th in table.select('thead th')]
    if heads.count('company') != 1 or heads.count('agents') != 1:
        raise AgencyError('directory_columns_missing')
    body = table.find('tbody', recursive=False)
    if body is None:
        raise AgencyError('directory_body_missing')
    rows = []
    for tr in body.find_all('tr', recursive=False):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(heads):
            raise AgencyError('directory_row_changed')
        agencies = {}
        for link in cells[heads.index('company')].select('a[href]'):
            if '/beraterfirma/berater/' in link['href']:
                url = _absolute(link['href'])
                agency = scope_from_url('agency_profile', url)['entity_id']
                name = link.get_text(' ', strip=True)
                if name and agencies.setdefault(agency, name) != name:
                    raise AgencyError('directory_agency_conflict')
        if len(agencies) != 1:
            raise AgencyError('directory_agency_missing')
        agency, name = next(iter(agencies.items()))
        agents = cells[heads.index('agents')]
        ids = set()
        anchor_texts = []
        for link in agents.select('a[href]'):
            if '/profil/berater/' in link['href']:
                ids.add(scope_from_url('agent_profile', _absolute(link['href']))['entity_id'])
                anchor_texts.append(link.get_text(' ', strip=True))
        listed = agents.get_text(' ', strip=True)
        all_identified = bool(ids) and ' '.join(anchor_texts) == listed
        rows.append(dict(lookup_query=query, agency_id=agency, agency_name=name,
                         listed_agents=listed or None, agent_identity_status='identified' if all_identified else 'unknown',
                         agent_ids_json=json.dumps(sorted(ids), separators=(',', ':')) if ids else None,
                         page_number=number))
    keys = region.select('.grid-view .keys > span')
    if keys and [tag.get_text(strip=True) for tag in keys] != [r['agency_id'] for r in rows]:
        raise AgencyError('directory_published_keys_mismatch')
    return rows, count, region


def _directory_walk(captures):
    following = captures[0].url
    bounds = _WalkBounds()
    total = None
    agencies = set()
    pages = []
    capture_ids = set()
    for number, capture in enumerate(captures, 1):
        if capture.endpoint != 'agency_directory' or capture.url != following or capture.capture_id in capture_ids:
            raise AgencyError('directory_walk_broken')
        capture_ids.add(capture.capture_id)
        rows, expected, pager = _directory_page(capture, number)
        bounds.check(number, *pager)
        following = pager[0]
        if (number > 1 and total != expected) or (not rows and (expected or number != 1 or following is not None)):
            raise AgencyError('directory_hits_changed')
        total = expected
        for row in rows:
            if row['agency_id'] in agencies:
                raise AgencyError('directory_duplicate_agency')
            agencies.add(row['agency_id'])
        pages.append((capture, rows))
    if following is not None or len(agencies) != total:
        raise AgencyError('directory_query_incomplete')
    return pages
