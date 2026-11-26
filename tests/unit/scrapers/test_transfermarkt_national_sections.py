"""Expected fields from felipeall/transfermarkt-api 71aa75e's two archived tmapi responses."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import core, national_sections as lane


pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures/transfermarkt/national'
API = 'https://tmapi.transfermarkt.technology'
CAREER_URL = API + '/player/28003/national-career-history'
TEAMS_URL = API + '/clubs?ids%5B%5D=11940&ids%5B%5D=3437&ids%5B%5D=53819'
ROSTER_URL = 'https://www.transfermarkt.com/argentina/kader/verein/3437/saison_id/2025/plus/1'
PARTICIPANTS_URL = 'https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/FIWC/saison_id/2025'
SCOPE = core.ScopeContext('FIWC', '2526', 'argentina', 'offline', competition_id='FIWC', edition_id='2025')
CLOCK = '2026-10-14T10:00:00.000000+00:00'
EXTRACTED = '2026-11-26T10:00:00.000000+00:00'


def recorded(name):
    return json.loads(gzip.decompress((FIXTURES / name).read_bytes()))


def observe(endpoint, url, raw, status=200):
    body = raw if isinstance(raw, bytes) else json.dumps(raw).encode()
    capture = core.SourceCapture(url, endpoint, body, hashlib.sha256(url.encode() + body).hexdigest(),
                                 hashlib.sha256(body).hexdigest(), CLOCK, status_code=status)
    return lane.extract((capture,), SCOPE, extracted_at=EXTRACTED, universe_version='national-fixture-v1')


def test_published_career_counts_for_three_teams_are_not_rosters_or_callups():
    raw = recorded('player-28003-national-career.json.gz')
    result, = observe('national_career', CAREER_URL, raw)
    assert (result.entity, result.status) == ('national_career_aggregate_observations', 'observed_present')
    assert [(r['national_team_id'], r['metric'], r['metric_value']) for r in result.rows] == [
        ('3437', 'appearances', 207), ('3437', 'goals', 125), ('11940', 'appearances', 18),
        ('11940', 'goals', 14), ('53819', 'appearances', 5), ('53819', 'goals', 2),
    ]
    assert [r['national_career_status'] for r in result.rows[::2]] == ['current', 'former', 'former']
    assert result.rows[0]['is_captain'] is True and result.rows[0]['shirt_number'] == 10
    assert str(result.rows[0]['national_debut']) == '2005-08-17'
    assert all(r['period'] == 'career' and r['observed_at'] == CLOCK for r in result.rows)
    assert all('match_id' not in r and 'callup_id' not in r for r in result.rows)
    assert result == observe('national_career', CAREER_URL, raw)[0]


def test_team_lookup_retains_three_independent_published_level_ids_and_names():
    results = observe('national_team_reference', TEAMS_URL, recorded('argentina-national-teams.json.gz'))
    assert all(outcome.status == 'observed_present' and len(outcome.rows) == 1 for outcome in results)
    rows = [outcome.rows[0] for outcome in results]
    assert [(r['national_team_id'], r['national_team_name'], r['national_team_type_id']) for r in rows] == [
        ('11940', 'Argentina U20', '4'), ('3437', 'Argentina', '1'), ('53819', 'Argentina Olympic Team', '14'),
    ]
    assert all(r['source_url'] == TEAMS_URL and r['main_national_team_id'] == '3437' for r in rows)


def test_overlap_at_same_clock_cannot_replace_another_packets_team(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
    from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan, SectionTarget
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    teams = {str(row['id']): row for row in recorded('argentina-national-teams.json.gz')['data']}
    db = CoreDB()
    def run(ids, cycle, clock=CLOCK):
        url = API + '/clubs?' + '&'.join('ids%5B%5D=' + team for team in ids)
        value = {'success': True, 'data': [teams[team] for team in ids]}
        body = json.dumps(value).encode()
        digest = hashlib.sha256(body).hexdigest()
        def fetch(requested, **options):
            assert requested == url and options['as_json']
            return FetchOutcome(status=FetchStatus.OK, value=value, raw_capture_id=digest, raw_body_hash=digest,
                                raw_fetched_at=clock, status_code=200)
        def load(capture_id, requested):
            assert (capture_id, requested) == (digest, url)
            return body, SimpleNamespace(url=url, endpoint='national_team_reference', content_hash=digest, fetched_at=clock)
        runner = CoreRun(fetch, request_raw_loader=load, extracted_at=EXTRACTED)
        plan = CoreScopePlan('FIWC', '2526', 'national', cycle, section_targets=(SectionTarget('national_team_reference', url),))
        return runner.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    read = 'SELECT national_team_id,team_packet FROM transfermarkt_national_team_level_observations ORDER BY 1,2'
    first = run(('11940', '3437'), 'first')
    before = db.sql.execute(read).fetchall()
    assert first.receipt['receipt_readback'] and len(before) == 2
    with pytest.raises(RuntimeError):
        run(('53819', '3437'), 'second')
    assert db.sql.execute(read).fetchall() == before
    assert run(('11940', '3437'), 'first').receipts == first.receipts
    later = run(('53819', '3437'), 'later', '2026-10-15T10:00:00.000000+00:00')
    assert later.receipt['receipt_readback'] and len(db.sql.execute(read).fetchall()) == 4


@pytest.mark.parametrize('endpoint,url', [
    ('national_career', CAREER_URL + '?a=1'), ('national_career', CAREER_URL.replace('28003', '028003')),
    ('national_career', CAREER_URL.replace('technology', 'technology:444')),
    ('national_career', CAREER_URL + '#x'), ('national_career', CAREER_URL.replace('https://', 'http://')),
    ('national_team_reference', TEAMS_URL + '&ids[]=3437'), ('national_team_reference', TEAMS_URL + '&x=1'),
    ('national_squad', ROSTER_URL.replace('www.', 'x@www.')),
    ('national_squad', ROSTER_URL.replace('2025', '2024')),
])
def test_source_ownership_and_identity_are_strict(endpoint, url):
    with pytest.raises(ValueError):
        lane.scope_from_url(endpoint, url, competition_id='FIWC', edition_id='2025')


def test_seasonal_membership_cannot_certify_a_tournament_roster_or_a_callup():
    results = observe('national_squad', ROSTER_URL, b'<html><h1>Squad Argentina 25/26</h1>'
                      b'<a href="/lionel-messi/profil/spieler/28003">Lionel Messi</a></html>')
    assert [(o.entity, o.status, o.rows) for o in results] == [
        ('national_tournament_squad_observations', 'unknown', ()),
        ('national_callup_observations', 'unknown', ()),
    ]
    assert all(o.proof is None for o in results)


@pytest.mark.parametrize('raw', [
    {}, {'success': False, 'data': {'history': []}}, {'success': True, 'data': {}},
    {'success': True, 'data': {'history': None}}, {'history': []},
    {'success': True, 'data': {'history': [], 'clubIds': ['3437']}},
])
def test_missing_failed_partial_career_is_not_a_conclusive_empty(raw):
    result, = observe('national_career', CAREER_URL, raw)
    assert result.status in {'error', 'unknown', 'unavailable'} and result.proof is None and not result.rows


def test_explicit_empty_is_scoped_and_rate_limit_is_unavailable():
    empty = {'success': True, 'data': {'history': [], 'clubIds': []}}
    result, = observe('national_career', CAREER_URL, empty)
    assert result.status == 'observed_empty' and 'history' in result.proof and not result.rows
    result, = observe('national_career', CAREER_URL, empty, 429)
    assert result.status == 'unavailable' and not result.rows


def test_null_metric_does_not_record_zero_wrong_player_and_conflicts_fail_closed():
    raw = recorded('player-28003-national-career.json.gz')
    rows = raw['data']['history']
    rows[0]['goalsScored'] = None
    result, = observe('national_career', CAREER_URL, raw)
    assert result.status == 'unknown' and not result.rows
    rows[0]['goalsScored'] = 0
    assert observe('national_career', CAREER_URL, raw)[0].rows[1]['metric_value'] == 0
    rows.append(dict(rows[0], gamesPlayed=200))
    assert observe('national_career', CAREER_URL, raw)[0].status == 'error'
    rows.pop()
    rows[0]['playerId'] = '1'
    assert observe('national_career', CAREER_URL, raw)[0].status == 'error'


def test_missing_requested_team_or_domestic_club_never_certifies_the_packet():
    raw = recorded('argentina-national-teams.json.gz')
    raw['data'][0]['baseDetails']['isNationalTeam'] = False
    assert observe('national_team_reference', TEAMS_URL, raw)[0].status == 'error'
    raw['data'].pop(0)
    assert observe('national_team_reference', TEAMS_URL, raw)[0].status == 'error'


def test_native_plan_carries_metric_types_keys_and_separate_capture_lineage():
    result, = observe('national_career', CAREER_URL, recorded('player-28003-national-career.json.gz'))
    career_plan = core.plan_for_outcome(result, captured_at=CLOCK, scope_values={'player_id': '28003'})
    assert career_plan.natural_keys == ('player_id', 'national_team_id', 'period', 'metric', 'observed_at')
    assert 'metric_value bigint' in career_plan.ddl and 'national_debut date' in career_plan.ddl
    assert all(r['raw_capture_id'] == result.capture.capture_id and r['source_url'] == CAREER_URL for r in career_plan.rows)
    references = observe('national_team_reference', TEAMS_URL, recorded('argentina-national-teams.json.gz'))
    assert len(references) == 3 and core.build_core_dq((result, *references))['passed']
    for item in references:
        row, = item.rows
        plan = core.plan_for_outcome(item, captured_at=CLOCK, scope_values={'national_team_id': row['national_team_id']})
        assert plan.scope_columns == ('national_team_id',)
        assert row['raw_capture_id'] != career_plan.rows[0]['raw_capture_id']
    with pytest.raises(ValueError, match='row_scope_mismatch'):
        core.plan_for_outcome(references[0], captured_at=CLOCK, scope_values={'national_team_id': '999'})
    first = references[0]
    first.rows[0]['team_packet'] = '999'
    with pytest.raises(ValueError, match='row_scope_mismatch'):
        core.plan_for_outcome(first, captured_at=CLOCK, scope_values={'national_team_id': '11940'})


def test_explicit_club_tooltip_preserves_documented_callup_flag_without_inventing_a_callup_id():
    # tanzhijian/fusion-stat@854ed182, tests/data/transfermarkt/arsenal-fc_startseite_verein_11.html,
    # Git blob ebd42bffcebc5556526c2dcb38468ed4784b81c1. Source lines 1095 and 1180.
    url = 'https://www.transfermarkt.com/arsenal-fc/startseite/verein/11'
    label = 'Called up to national team -  - cross-competition'
    body = (f'<meta property="og:url" content="{url}"><table class="items"><tbody><tr><td>'
            f'<a href="/takehiro-tomiyasu/profil/spieler/331560"><span class="ausfall-table" title="{label}"></span></a>'
            '</td></tr><tr><td><a href="/player/profil/spieler/2"><span title="Injured"></span></a>'
            '</td></tr></tbody></table>')
    result, = observe('national_callup_flags', url, body.encode())
    assert result.entity == 'national_callup_flag_observations'
    assert [(r['club_id'], r['player_id'], r['source_flag'], r['callup_identity_status']) for r in result.rows] == [
        ('11', '331560', label, 'unknown')]
    assert 'callup_id' not in result.rows[0]
    assert core.plan_for_outcome(result, captured_at=CLOCK, scope_values={'club_id': '11'}).rows
    assert observe('national_callup_flags', url, body.replace(label, 'Absent').encode())[0].status == 'unknown'
    wrong = body.replace('content="' + url, 'content="' + url[:-2] + '99')
    assert observe('national_callup_flags', url, wrong.encode())[0].status == 'error'


def test_tournament_teams_and_counts_exclude_unqualified_teams_and_player_registrations():
    html = (FIXTURES / 'fiwc-2026-participants.html').read_bytes()
    result, = observe('national_tournament_participants', PARTICIPANTS_URL, html)
    assert result.status == 'observed_present'
    assert [(r['competition_id'], r['edition_id'], r['national_team_id'], r['national_team_name'], r['published_squad_size'])
            for r in result.rows] == [('FIWC', '2025', '3299', 'England', 26), ('FIWC', '2025', '3375', 'Spain', 27)]
    assert all('player_id' not in r and 'callup_id' not in r for r in result.rows)
    plan = core.plan_for_outcome(result, captured_at=CLOCK, scope_values={'competition_id': 'FIWC', 'edition_id': '2025'})
    assert plan.natural_keys == ('competition_id', 'edition_id', 'national_team_id', 'observed_at')
    assert 'published_squad_size bigint' in plan.ddl
    missing, = observe('national_tournament_participants', PARTICIPANTS_URL, html.replace(b'<td>26</td>', b'<td>-</td>'))
    assert missing.rows[0]['published_squad_size'] is None


@pytest.mark.parametrize('old,new', [
    ('competition-id="FIWC"', 'competition-id="CL"'), ('season-id="2025"', 'season-id="2024"'),
    ('value="2025" selected', 'value="2024" selected'), ('<span>3375</span>', '<span>3376</span>'),
    ('<td>26</td>', '<td>26 players</td>'), ('/england/startseite/verein/3299', 'https://foreign.test/england/startseite/verein/3299'),
    ('Clubs starting into tournament at a later point', 'Not qualified (yet)'),
    ('<span>3299</span>', '<span>3299</span><div class="pager"></div>'),
])
def test_damaged_or_paginated_participant_group_is_inconclusive(old, new):
    html = (FIXTURES / 'fiwc-2026-participants.html').read_text().replace(old, new)
    result, = observe('national_tournament_participants', PARTICIPANTS_URL, html.encode())
    assert result.status in {'unknown', 'error'} and not result.rows and not result.proof


@pytest.mark.parametrize('url', [PARTICIPANTS_URL.replace('/FIWC/', '/CL/'), PARTICIPANTS_URL + '/page/2',
                                 PARTICIPANTS_URL + '?saison_id=2025', PARTICIPANTS_URL.replace('/2025', '/2024')])
def test_competition_route_cannot_accept_a_club_tournament_or_unrelated_page(url):
    with pytest.raises(ValueError):
        lane.scope_from_url('national_tournament_participants', url, competition_id='FIWC', edition_id='2025')


def test_agencies_and_national_teams_full_core_run_readback_and_replay(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
    from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan, SectionTarget
    from tests.unit.scrapers import test_transfermarkt_agency_sections as agencies
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    agency_pages = agencies.pair()
    search = agencies.capture('agency_directory', agencies.SEARCH, agencies.search_page(
        [('65', 'Soccertalk GmbH', 'Alen Augustincic'), ('770', 'FPA Fairplay Agency Ltd', 'Dino Lamberti')], hits=2))
    sources = {page.url: page for page in (*agency_pages, search)}
    for endpoint, url, name in (('national_career', CAREER_URL, 'player-28003-national-career.json.gz'),
                                ('national_team_reference', TEAMS_URL, 'argentina-national-teams.json.gz')):
        raw = gzip.decompress((FIXTURES / name).read_bytes())
        digest = hashlib.sha256(raw).hexdigest()
        sources[url] = core.SourceCapture(url, endpoint, raw, digest, digest, CLOCK)
    sources[ROSTER_URL] = agencies.capture('national_squad', ROSTER_URL, '<h1>Squad Argentina</h1>')
    for endpoint, url, fixture in (
        ('agency_clients', agencies.FPA, agencies.FIXTURES / 'fpa-770.html'),
        ('agency_statistics', agencies.STATISTICS, agencies.FIXTURES / 'statistics-page.html'),
        ('national_tournament_participants', PARTICIPANTS_URL, FIXTURES / 'fiwc-2026-participants.html'),
    ):
        sources[url] = agencies.capture(endpoint, url, fixture.read_text())

    fetched = []
    def fetch(url, **options):
        source = sources[url]
        assert (options['label'], options['as_json']) == (source.endpoint, source.endpoint in lane.JSON_ENDPOINTS)
        fetched.append(url)
        value = json.loads(source.body) if options['as_json'] else source.text
        return FetchOutcome(status=FetchStatus.OK, value=value, raw_capture_id=source.capture_id,
                            raw_body_hash=source.body_hash, raw_fetched_at=source.captured_at, status_code=200)

    def load(capture_id, url):
        c = sources[url]
        assert c.capture_id == capture_id
        return c.body, SimpleNamespace(url=url, endpoint=c.endpoint, fetched_at=c.captured_at, content_hash=c.body_hash)

    plan = CoreScopePlan('FIWC', '2526', 'agencies-and-national', 'fixture-test', edition_id='2025',
                         section_targets=tuple(SectionTarget(s.endpoint, s.url) for s in sources.values()
                                               if s is not agency_pages[1]))
    db = CoreDB()
    run = CoreRun(fetch, request_raw_loader=load, extracted_at=EXTRACTED, universe_version='archived-fixtures')
    first_receipts = None
    for iteration in (1, 2):
        result = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
        assert result.fetched == len(sources) and fetched == list(sources) * iteration
        assert result.dq['passed'] and result.receipt['receipt_readback'] and result.receipt['verified']
        assert all(r['receipt_readback'] and r['verified'] for r in result.receipts)
        if iteration == 1:
            first_receipts = result.receipts
        else:
            assert result.receipts == first_receipts
        statuses = {o['entity']: o['status'] for o in result.outcomes}
        assert statuses['national_tournament_squad_observations'] == statuses['national_callup_observations'] == 'unknown'
        assert statuses['agent_agency_observations'] == 'unknown'
        assert db.sql.execute('SELECT player_id, page_number FROM transfermarkt_agency_client_observations '
                              "WHERE agency_id='47' ORDER BY player_id").fetchall() == [('101', 1), ('102', 1), ('103', 2)]
        assert db.sql.execute('SELECT page_number, source_row_count, new_client_count '
                              "FROM transfermarkt_agency_client_page_observations WHERE agency_id='47' ORDER BY page_number").fetchall() == [
            (1, 2, 2), (2, 2, 1)]
        assert db.sql.execute('SELECT entity_type, entity_id FROM transfermarkt_representative_profile_observations '
                              'ORDER BY entity_id').fetchall() == [('agency', '47'), ('agency', '770')]
        assert db.sql.execute("SELECT count(*) FROM transfermarkt_agency_client_observations WHERE agency_id='770'").fetchone() == (13,)
        assert db.sql.execute('SELECT agency_id, published_player_count, directory_completeness FROM transfermarkt_agency_statistics_page_observations '
                              'ORDER BY agency_id').fetchall() == [('440', 1539, 'unknown'), ('5024', 26, 'unknown')]
        assert db.sql.execute('SELECT national_team_id, published_squad_size FROM transfermarkt_national_tournament_participant_observations '
                              'ORDER BY national_team_id').fetchall() == [('3299', 26), ('3375', 27)]
        assert db.sql.execute('SELECT agency_id, agent_identity_status FROM transfermarkt_agency_directory_observations '
                              'ORDER BY agency_id').fetchall() == [('65', 'unknown'), ('770', 'unknown')]
        assert db.sql.execute('SELECT national_team_id, metric_value FROM transfermarkt_national_career_aggregate_observations '
                              "WHERE metric='goals' ORDER BY national_team_id").fetchall() == [
            ('11940', 14), ('3437', 125), ('53819', 2)]
        assert db.sql.execute('SELECT national_team_id, national_team_type_id FROM transfermarkt_national_team_level_observations '
                              'ORDER BY national_team_id').fetchall() == [('11940', '4'), ('3437', '1'), ('53819', '14')]
        assert db.sql.execute('SELECT count(*) FROM transfermarkt_core_receipts_v1').fetchone() == (15,)
