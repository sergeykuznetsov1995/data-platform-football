"""Manual HTML examples based on the pinned donor's English source and Football #2283."""

import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import agency_sections as lane, core


pytestmark = pytest.mark.unit

BASE = 'https://www.transfermarkt.com'
AGENCY = BASE + '/alpha/beraterfirma/berater/47'
PERSON = BASE + '/alex/profil/berater/47'
SCOPE = core.ScopeContext('GB1', '2627', 'agents', 'offline')
EXTRACTED = '2026-11-26T10:00:00.000000+00:00'
FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures/transfermarkt/agency'
FPA = BASE + '/fpa-fairplay-agency-ltd/beraterfirma/berater/770'
STATISTICS = BASE + '/berater/beraterfirmenuebersicht/berater'


def capture(endpoint, url, html, minute=0, status=200):
    body = html.encode()
    return core.SourceCapture(url, endpoint, body, hashlib.sha256(url.encode() + body).hexdigest(),
                              hashlib.sha256(body).hexdigest(), f'2026-10-03T07:{minute:02d}:00.000000+00:00', status)


def observe(*captures):
    return lane.extract(captures, SCOPE, extracted_at=EXTRACTED, universe_version='agency-fixture-v1')


def row(player, *, age='29', amount='€2.50m'):
    return (f'<tr class="odd"><td><a href="/player/profil/spieler/{player}">Client {player}</a></td>'
            f'<td class="zentriert">{age}</td><td class="zentriert"><img class="flaggenrahmen" '
            'title="Italy" alt="Italy"></td><td><a title="FC Alpha" href="/alpha/startseite/verein/11">A</a>'
            f'</td><td class="zentriert hauptlink">{amount}</td></tr>')


def client_page(rows, *, total=3, pager='', name='Alpha'):
    return (f'<h1 class="data-header__headline-wrapper">{name}</h1><h2>Key Facts</h2><span>Players: {total}</span>'
            '<div class="grid-view"><table class="items"><thead><tr><th>Player</th><th>Age</th><th>Nat.</th>'
            '<th>Current club</th><th>Market value</th></tr></thead><tbody>'
            + rows + '</tbody></table>' + pager + '</div>')


NEXT = '<div class="pager"><ul><li class="next"><a href="/alpha/beraterfirma/berater/47/page/2">›</a></li></ul></div>'
PREV = '<div class="pager"><ul><li class="previous"><a href="/alpha/beraterfirma/berater/47">‹</a></li></ul></div>'


def pair():
    return (capture('agency_clients', AGENCY, client_page(row('101') + row('102'), pager=NEXT)),
            capture('agency_clients', AGENCY + '/page/2', client_page(row('102') + row('103'), pager=PREV), 1))


def test_complete_walk_records_three_clients_from_four_occurrences_and_per_page_evidence():
    p1, p2 = pair()
    assert lane.next_page(p1, 1) == p2.url and lane.next_page(p2, 2) is None
    results = observe(p1, p2)
    clients = [row for out in results if out.entity == 'agency_client_observations' for row in out.rows]
    pages = [row for out in results if out.entity == 'agency_client_page_observations' for row in out.rows]
    assert [(r['agency_id'], r['player_id'], r['source_capture_id']) for r in clients] == [
        ('47', '101', p1.capture_id), ('47', '102', p1.capture_id), ('47', '103', p2.capture_id),
    ]
    assert all((r['age'], r['nationality1'], r['club_id'], r['market_value_eur']) ==
               (29, 'Italy', '11', 2_500_000) for r in clients)
    assert [(r['page_number'], r['source_row_count'], r['new_client_count']) for r in pages] == [(1, 2, 2), (2, 2, 1)]
    assert observe(p1, p2) == results


@pytest.mark.parametrize('bad', ['truncated', 'gap', 'different_agency', 'same_capture', 'count', 'conflict',
                                  'foreign_next', 'wrong_next', 'foreign_client', 'layout', 'money', 'only_p2'])
def test_incomplete_or_contradictory_walk_has_no_partial_client_rows(bad):
    first, second = pair()
    html = first.text
    if bad == 'truncated':
        caps = [first]
    elif bad == 'only_p2':
        caps = [second]
    elif bad in {'gap', 'different_agency', 'same_capture'}:
        url = second.url.replace('/page/2', '/page/3') if bad == 'gap' else second.url.replace('/47/', '/48/')
        caps = [first, first if bad == 'same_capture' else capture('agency_clients', url, second.text, 1)]
    else:
        prev = html
        nxt = second.text
        if bad == 'count':
            nxt = nxt.replace('Players: 3', 'Players: 4')
        elif bad == 'conflict':
            nxt = nxt.replace('€2.50m', '€3m', 1)
        elif bad == 'foreign_next':
            prev = prev.replace('href="/alpha/beraterfirma/', 'href="https://wrong.test/alpha/beraterfirma/')
        elif bad == 'wrong_next':
            prev = prev.replace('/47/page/2', '/47/page/3')
        elif bad == 'foreign_client':
            prev = prev.replace('href="/player/', 'href="https://wrong.test/player/', 1)
        elif bad == 'layout':
            prev = prev.replace('class="items"', 'class="renamed"')
        else:
            prev = prev.replace('€2.50m', '£2.50m', 1)
        caps = [capture('agency_clients', first.url, prev), capture('agency_clients', second.url, nxt, 1)]
    results = observe(*caps)
    facts = [o for o in results if o.entity in {'agency_client_observations', 'agency_client_page_observations'}]
    assert facts and all(out.status in {'error', 'unknown'} and not out.rows for out in facts)


def test_published_zero_needs_a_recognizable_clients_table_and_receipt_proof():
    empty = client_page('', total=0)
    clients, page, profile, staff = observe(capture('agency_clients', AGENCY, empty))
    assert profile.rows[0]['client_count'] == 0 and staff.status == 'unknown'
    assert clients.status == 'observed_empty' and clients.proof and not clients.rows
    assert page.rows[0]['source_row_count'] == 0
    for body in (empty.replace('class="items"', 'class="renamed"'), empty.replace('Players: 0', '')):
        facts = observe(capture('agency_clients', AGENCY, body))[:2]
        assert all(not o.rows and o.status in {'unknown', 'error'} for o in facts)


def test_person_and_agency_with_same_number_are_distinct_and_staff_requires_person_profile_link():
    html = (client_page('', total=0) + '<div class="box"><h2 class="content-box-headline">Staff</h2>'
            '<table class="items"><tbody><tr class="odd"><td class="hauptlink">'
            '<a href="/alex/profil/berater/47">Alex</a></td></tr></tbody></table></div>')
    agency, staff = observe(capture('agency_profile', AGENCY, html))
    person, = observe(capture('agent_profile', PERSON, f'<link rel="canonical" href="{PERSON}">'
                             '<h1 class="data-header__headline-wrapper">Alex</h1>'))
    assert (agency.rows[0]['entity_type'], agency.rows[0]['entity_id'], agency.rows[0]['name']) == ('agency', '47', 'Alpha')
    assert (person.rows[0]['entity_type'], person.rows[0]['entity_id'], person.rows[0]['name']) == ('person', '47', 'Alex')
    assert [(r['agent_id'], r['agency_id'], r['relation_type']) for r in staff.rows] == [('47', '47', 'agent_agency')]
    for broken in (html.replace('/alex/profil/berater/47', '/alpha/beraterfirma/berater/47'),
                   html.replace('>Staff<', '>Clients<')):
        _, missing = observe(capture('agency_profile', AGENCY, broken))
        assert missing.status == 'unknown' and not missing.rows


@pytest.mark.parametrize('endpoint,url', [('agent_profile', AGENCY), ('agency_profile', PERSON),
    ('agency_clients', AGENCY + '?page=2'), ('agency_clients', AGENCY.replace('47', '0')),
    ('agency_clients', AGENCY + '/page/00'), ('agency_profile', AGENCY + '/page/2'),
    ('agency_clients', AGENCY.replace('www.', 'admin@www.')), ('agency_clients', AGENCY + '#x')])
def test_distinct_routes_and_full_authority(endpoint, url):
    with pytest.raises(ValueError):
        lane.scope_from_url(endpoint, url)


def test_failed_or_wrong_profile_never_becomes_authoritative_empty():
    for result in observe(capture('agency_profile', AGENCY, '', status=429)):
        assert result.status == 'unavailable' and not result.rows and not result.proof
    wrong = ('<link rel="canonical" href="https://www.transfermarkt.com/other/profil/berater/999">'
             '<h1 class="data-header__headline-wrapper">Alex</h1>')
    assert observe(capture('agent_profile', PERSON, wrong))[0].status == 'error'


# The original .co.uk search archive at SchubmannM/footy-world-xi-validator-web
# c332da0e, footy_validator/tests/fixtures.py:267–364, has 22 hits, 10 on page 1.
# This separately authored two-row fixture uses its published agency IDs and names;
# the stateless parser also runs under the permitted .com search target.
SEARCH = BASE + '/schnellsuche/ergebnis/schnellsuche?query=Thomas%20Muller&x=0&y=0'


def search_page(entries, *, hits, pager=''):
    rows = ''.join(f'<tr><td><a href="/company/beraterfirma/berater/{aid}">{company}</a></td>'
                   f'<td>{name}</td></tr>' for aid, company, name in entries)
    return (f'<div class="box"><div class="table-header">Search results for agents - {hits} Hits</div>'
            f'<div class="grid-view"><table class="items"><thead><tr><th>Company</th><th>Agents</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>{pager}</div></div>')


def test_directory_facts_preserve_plain_agent_names_with_unresolved_person_ids():
    entries = [('65', 'Soccertalk GmbH', 'Alen Augustincic'), ('770', 'FPA Fairplay Agency Ltd', 'Dino Lamberti')]
    p1 = capture('agency_directory', SEARCH, search_page(entries, hits=2))
    (result,) = observe(p1)
    assert result.entity == 'agency_directory_observations' and result.status == 'observed_present'
    assert [(r['agency_id'], r['agency_name'], r['listed_agents'], r['agent_ids_json']) for r in result.rows] == [
        ('65', 'Soccertalk GmbH', 'Alen Augustincic', None), ('770', 'FPA Fairplay Agency Ltd', 'Dino Lamberti', None)]
    assert all('agent_id' not in r and r['lookup_query'] == 'Thomas Muller' and r['agent_identity_status'] == 'unknown'
               for r in result.rows)
    assert core.plan_for_outcome(result, captured_at=p1.captured_at, scope_values={'lookup_query': 'Thomas Muller'}).rows


def test_search_reconciles_only_its_own_pager_and_full_query_count():
    entries = [('65', 'Soccertalk GmbH', 'Alen Augustincic'), ('770', 'FPA Fairplay Agency Ltd', 'Dino Lamberti')]
    href = '/schnellsuche/ergebnis/schnellsuche?query=Thomas+Muller&amp;x=0&amp;y=0&amp;page=2'
    next_link = f'<div class="pager"><a href="{href}" rel="next">2</a></div>'
    p1 = capture('agency_directory', SEARCH, search_page(entries[:1], hits=2, pager=next_link))
    next_url = lane.next_page(p1, 1)
    assert next_url.endswith('&page=2') and lane.page_number('agency_directory', next_url) == 2
    assert all(not o.rows for o in observe(p1))
    p2 = capture('agency_directory', next_url, search_page(entries[1:], hits=2), minute=1)
    outcomes = observe(p1, p2)
    assert [o.rows[0]['agency_id'] for o in outcomes] == ['65', '770']
    assert all(o.status == 'error' for o in observe(p1, capture('agency_directory', next_url, search_page(entries[:1], hits=2))))
    assert all(not o.rows for o in observe(capture('agency_directory', SEARCH, search_page(entries, hits=22))))


@pytest.mark.parametrize('bad', [SEARCH.replace('.com', '.co.uk'), SEARCH + '&sort=foo', SEARCH + '&page=00',
    SEARCH + '&query=q', SEARCH.replace('x=0','x=7'), SEARCH.replace('%20', '%0a'), SEARCH + '#',
    SEARCH.replace('Thomas%20Muller', '%2Flogin'), SEARCH.replace('x=0&y=0', 'x=0')])
def test_search_url_is_limited_to_name_query_and_agent_pages(bad):
    with pytest.raises(ValueError):
        lane.scope_from_url('agency_directory', bad)


def test_client_write_plans_keep_each_page_source_and_use_capture_clock():
    captures = pair()
    outcomes = observe(*captures)
    plans = [core.plan_for_outcome(outcome, captured_at=outcome.capture.captured_at,
                                   scope_values=lane.scope_for_entity(outcome.entity, 'agency_clients', {'agency_id': '47'}))
             for outcome in outcomes if outcome.status in core.CONCLUSIVE_CORE_STATUSES]
    assert {p.capture_id for p in plans} == {c.capture_id for c in captures}
    for plan in plans:
        assert all(r['raw_capture_id'] == plan.capture_id and r['observed_at'] == plan.captured_at for r in plan.rows)
    assert all(p.natural_keys == ('agency_id', 'player_id', 'observed_at')
               for p in plans if p.entity == 'agency_client_observations')
    assert core.build_core_dq(outcomes)['passed']


def test_fpa_has_13_player_clients_and_published_staff_names_without_person_ids():
    source = capture('agency_clients', FPA, (FIXTURES / 'fpa-770.html').read_text())
    clients, page, profile, relations = observe(source)
    assert lane.next_page(source, 1) is None
    assert (len(clients.rows), page.rows[0]['source_row_count'], page.rows[0]['next_page_url']) == (13, 13, None)
    assert [(r['player_id'], r['club_id'], r['market_value_eur']) for r in clients.rows[:2]] == [
        ('707759', '434', 1_000_000), ('464190', '260', 600_000)]
    assert clients.rows[-1]['player_id'] == '1396729' and clients.rows[-1]['market_value_eur'] is None
    assert all(r['player_id'] != '5965' for r in clients.rows)
    assert (profile.rows[0]['name'], profile.rows[0]['client_count']) == ('FPA Fairplay Agency Ltd', 13)
    staff = json.loads(profile.rows[0]['staff_listing_json'])
    assert len(staff) == 7 and all(r['agent_id'] is None for r in staff)
    assert staff[0] == dict(source_name='Dino Lamberti', source_roles=['Owner'], licensed_label='Officially licensed', agent_id=None)
    assert staff[5] == dict(source_name='Marcelo Sander', source_roles=['Agent'], licensed_label=None, agent_id=None)
    assert relations.status == 'unknown' and not relations.rows
    assert all(o.status == 'unavailable' and not o.rows for o in observe(capture('agency_clients', FPA, 'Checking...', status=202)))


def test_statistics_rows_keep_the_published_filter_next_url_and_only_this_page():
    result, = observe(capture('agency_statistics', STATISTICS, (FIXTURES / 'statistics-page.html').read_text()))
    assert result.status == 'observed_present' and result.entity == 'agency_statistics_page_observations'
    assert [(r['agency_id'], r['agency_country'], r['published_player_count']) for r in result.rows] == [
        ('440', 'United States', 1539), ('5024', 'France', 26)]
    assert all((r['published_land_id'], r['statistics_page'], r['directory_completeness'], r['next_page_url']) ==
               ('12', 'default/1', 'unknown', STATISTICS + '?landid=12&page=2') for r in result.rows)
    assert core.plan_for_outcome(result, captured_at=result.capture.captured_at, scope_values={'statistics_page': 'default/1'}).rows
    assert lane.scope_from_url('agency_statistics', result.rows[0]['next_page_url']) == {'statistics_page': '12/2'}


@pytest.mark.parametrize('old,new', [
    ('&amp;page=2', '&amp;page=3'), ('<span>5024</span>', '<span>440</span>'),
    ('<span>440</span>', ''), ('&amp;page=2', '&amp;sort=name'), ('landid=12&amp;page=2', 'landid=1&amp;page=2'),
    ('>26</a>', '>-</a>'), ('<th>Player</th>', '<th>Rumours</th>'),
])
def test_damaged_statistics_page_never_reports_an_empty_directory(old, new):
    html = (FIXTURES / 'statistics-page.html').read_text()
    result, = observe(capture('agency_statistics', STATISTICS, html.replace(old, new)))
    assert result.status == 'error' and not result.rows


@pytest.mark.parametrize('suffix', ['?sort=firma', '?page=2', '?landid=12&page=00', '?landid=12&landid=3',
                                     '?landid=12&x=1', '?landid=12#fragment'])
def test_statistics_query_allowlist_includes_only_documented_pagination(suffix):
    from scrapers.transfermarkt import raw_store

    with pytest.raises(ValueError):
        lane.scope_from_url('agency_statistics', STATISTICS + suffix)
    with pytest.raises(ValueError):
        raw_store._safe_url(STATISTICS + suffix)
    assert raw_store._safe_url(STATISTICS + '?landid=12&page=2') == STATISTICS + '?landid=12&page=2'
