from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import club_sections, core
from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan, SectionTarget
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

pytestmark = pytest.mark.unit

CLOCK = '2026-11-26T09:00:00.000000+00:00'
API = 'https://tmapi.transfermarkt.technology'
STAFF = 'https://www.transfermarkt.com/sc-paderborn-07/mitarbeiter/verein/127'
SCOPE = core.ScopeContext('L1', '2627', 'club-sections', 'cycle', club_id='127')


def capture(endpoint, url, data):
    body = data if isinstance(data, bytes) else json.dumps(data).encode()
    return core.SourceCapture(url, endpoint, body, sha256(url.encode() + body).hexdigest(),
                              sha256(body).hexdigest(), CLOCK)


def extract(source):
    return club_sections.extract([source], SCOPE, extracted_at=CLOCK, universe_version='test')


def test_staff_saved_page_keeps_director_role_and_its_own_club():
    body = Path('tests/fixtures/transfermarkt/staff/sc-paderborn-07.html').read_bytes()
    source = capture('club_staff', STAFF, body)
    outcome, = extract(source)
    director, = [row for row in outcome.rows if row['person_id'] == '112401' and row['role'] == 'Sporting Director']
    assert (director['name'], director['club_id'], director['observed_day']) == (
        'Robin Trost', '127', '2026-11-26',
    )
    plan = core.plan_for_outcome(outcome, captured_at=CLOCK, scope_values={'club_id': '127'})
    assert plan.natural_keys == ('club_id', 'person_id', 'section', 'role', 'observed_day')
    assert director['source_capture_id'] == source.capture_id
    with pytest.raises(ValueError):
        core.plan_for_outcome(outcome, captured_at=CLOCK, scope_values={'club_id': '999'})


def test_missing_staff_layout_and_empty_stadium_envelope_are_unresolved():
    outcome, = extract(capture('club_staff', STAFF, b'<p>No table</p>'))
    assert outcome.status == 'error'
    outcomes = extract(capture('club_stadium', f'{API}/club/127/stadium', {'success': True, 'data': {}}))
    assert [item.status for item in outcomes] == ['error', 'error']


def test_stadium_and_relation_survive_offline_write_and_idempotent_replay(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    url = f'{API}/club/127/stadium'
    source = capture('club_stadium', url, {'success': True, 'data': {
        'name': 'Home', 'clubId': '127', 'capacity': 15000, 'location': {'latitude': 51.7},
    }})
    target = SectionTarget(source.endpoint, source.url)
    plan = CoreScopePlan('L1', '2627', 'club-sections', 'cycle', section_targets=(target,))
    def fetch(url, **options):
        assert (url, options['as_json']) == (source.url, True)
        return FetchOutcome(status=FetchStatus.OK, value=json.loads(source.body), raw_capture_id=source.capture_id,
                            raw_body_hash=source.body_hash, raw_fetched_at=CLOCK, status_code=200)

    def load(capture_id, url):
        from types import SimpleNamespace
        assert (capture_id, url) == (source.capture_id, source.url)
        return source.body, SimpleNamespace(url=url, endpoint=source.endpoint, fetched_at=CLOCK,
                                            content_hash=source.body_hash)

    run = CoreRun(fetch, request_raw_loader=load, extracted_at=CLOCK)
    db = CoreDB()
    writer = CoreWriter(db)
    report = run.run(plan, writer=writer, connection_factory=lambda: db)
    assert report.receipt['receipt_readback'] and report.dq['passed']
    assert db.sql.execute('SELECT club_id, stadium_id, stadium_observation_key, capacity, latitude FROM '
                          'transfermarkt_stadium_observations').fetchall() == [
        ('127', None, 'club:127:unidentified', 15000, 51.7)]
    assert db.sql.execute('SELECT club_id, stadium_id, valid_from, valid_to FROM '
                          'transfermarkt_club_stadium_relations').fetchall() == [('127', None, None, None)]
    assert run.run(plan, writer=writer, connection_factory=lambda: db).receipt['receipt_readback']
    assert db.sql.execute('SELECT count(*) FROM transfermarkt_club_stadium_relations').fetchone() == (1,)


def test_club_profile_checks_identity_and_keeps_reported_fields():
    url = f'{API}/club/127'
    source = capture('club_profile', url, {'success': True, 'data': {
        'id': 127, 'name': 'Paderborn', 'baseDetails': {'isNationalTeam': False},
    }})
    outcome, = extract(source)
    row, = outcome.rows
    assert (row['club_id'], row['name'], row['is_national_team']) == ('127', 'Paderborn', False)
    assert extract(replace(source, url=f'{API}/club/128'))[0].status == 'error'


@pytest.mark.parametrize('url', [f'{API}/club/127/stadium?x=1', f'{API}:444/club/127/stadium',
                                 'https://www.transfermarkt.com/club/127/stadium'])
def test_wrong_stadium_source_is_rejected_before_fetch(url):
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=(SectionTarget('club_stadium', url),))
    with pytest.raises(ValueError):
        CoreRun(lambda *a, **kw: pytest.fail('unexpected fetch')).run(plan)


def test_offline_cli_runs_saved_section_and_explicit_first_wave_target(tmp_path, monkeypatch):
    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_orchestrator import _write_replay_manifest
    from tests.unit.scrapers.test_transfermarkt_sections import _club_value_capture

    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    staff = capture('club_staff', STAFF, Path('tests/fixtures/transfermarkt/staff/sc-paderborn-07.html').read_bytes())
    _write_replay_manifest(tmp_path, [staff, _club_value_capture()])
    plan_file = tmp_path / 'targets.json'
    plan_file.write_text(json.dumps({'club_value_targets': [{'competition_id': 'VIE1', 'slug': 'v-league-1'}]}))
    report_file = tmp_path / 'report.json'
    db = CoreDB()
    monkeypatch.setattr(cli, '_write_options', lambda _: {'writer': CoreWriter(db), 'connection_factory': lambda: db})
    assert cli.main(['offline', '--fixtures-root', str(tmp_path), '--section-plan', str(plan_file),
                     '--league', 'L1', '--season', '2627', '--write', '--report-out', str(report_file)]) == 0
    report = json.loads(report_file.read_text())
    assert report['fetched'] == 2 and len(report['receipts']) == 2 and report['dq']['passed']
    assert report['dq']['row_counts']['club_staff_observations'] > 0
    assert report['dq']['row_counts']['club_value_observations'] > 0
    assert all(item['receipt_readback'] for item in report['receipts'])


def test_live_planner_can_select_only_one_section(tmp_path):
    from dags.scripts import run_transfermarkt_core as cli
    from scrapers.transfermarkt.orchestrator import planned_urls

    path = tmp_path / 'plan.json'
    path.write_text(json.dumps({'section_targets': [{'endpoint': 'club_staff', 'url': STAFF}]}))
    args = cli._parser().parse_args(['live', '--league', 'L1', '--season', '2627', '--scope-id', 's',
                                    '--cycle-id', 'c', '--section-plan', str(path)])
    assert planned_urls(cli._live_plan(args)) == (STAFF,)


def test_section_pagination_limit_stops_before_another_fetch(monkeypatch):
    from scrapers.transfermarkt import agency_sections, orchestrator

    url = 'https://www.transfermarkt.com/example/beraterfirma/berater/770'
    source = capture('agency_clients', url, b'<p>page one</p>')
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=(SectionTarget(source.endpoint, url),))
    monkeypatch.setattr(orchestrator, '_SECTION_PAGE_LIMIT', 1)
    monkeypatch.setattr(agency_sections, 'next_page', lambda page, number: url + '/page/2')
    def fetch(request_url, **options):
        assert request_url == url
        return FetchOutcome(status=FetchStatus.OK, value=source.text, raw_capture_id=source.capture_id,
                            raw_body_hash=source.body_hash, raw_fetched_at=CLOCK, status_code=200)
    report = CoreRun(fetch, extracted_at=CLOCK).run(plan)
    assert report.fetched == 1 and not report.receipts and report.dq['verified_write_coverage'] == 0
    assert all(item['status'] == 'error' for item in report.outcomes)


def test_new_raw_routes_keep_query_parameters_scoped_to_their_endpoint():
    from scrapers.transfermarkt.raw_store import _safe_url

    lookup = 'https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche'
    permitted = (lookup + '?query=Soccer+Talk&page=2', f'{API}/player/28003/injury',
                 f'{API}/player/28003/national-career-history')
    assert tuple(map(_safe_url, permitted)) == permitted
    for url in (STAFF + '?query=Soccer', lookup + '?query=Soccer&x=1&y=0', lookup + '?query=A&query=B',
                lookup + '?query=Soccer&token=test', permitted[1] + '?query=Soccer',
                f'{API}/player/28003/unknown-route'):
        with pytest.raises(ValueError):
            _safe_url(url)


def test_section_cli_releases_the_transport_after_a_failed_fetch(tmp_path, monkeypatch):
    from dags.scripts import run_transfermarkt_core as cli
    from scrapers.transfermarkt import client

    closed = []

    class OfflineTransport:
        def fetch(self, *args, **kwargs):
            raise OSError('simulated read failure')

        def close(self):
            closed.append(True)

    monkeypatch.setattr(client, 'TransfermarktHttpClient', OfflineTransport)
    path = tmp_path / 'targets.json'
    path.write_text(json.dumps({'section_targets': [{'endpoint': 'club_staff', 'url': STAFF}]}))
    with pytest.raises(SystemExit) as error:
        cli.main(['live', '--league', 'L1', '--season', '2627', '--scope-id', 's', '--cycle-id', 'c',
                  '--i-have-transport-permission', '--section-plan', str(path)])
    assert error.value.code == 2 and closed == [True]


@pytest.mark.parametrize(('country_id', 'name'), [(40, 'Germany'), (99999, None)])
def test_t7_country_names_use_the_static_country_file(country_id, name):
    url = f'{API}/club/127'
    location = {'countryId': country_id}
    profile, = extract(capture('club_profile', url, {'success': True, 'data': {
        'id': 127, 'name': 'SC Paderborn', 'baseDetails': {'superiorClub': {'location': location}},
    }}))
    stadium, _ = extract(capture('club_stadium', url + '/stadium', {'success': True, 'data': {
        'name': 'Home', 'capacity': 15000, 'location': location,
    }}))
    assert (profile.rows[0]['country_id'], profile.rows[0]['address_line_3']) == (str(country_id), name)
    assert (stadium.rows[0]['country_id'], stadium.rows[0]['country_name']) == (str(country_id), name)


def _stadium_pair(tmp_path, *, status=404, profile_id=127, profile_url_id=127, same_capture=False):
    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_orchestrator import _write_replay_manifest

    url = f'{API}/club/127'
    profile = capture('club_profile', f'{API}/club/{profile_url_id}', {
        'success': True, 'data': {'id': profile_id, 'name': 'SC Paderborn', 'baseDetails': {'isNationalTeam': False}}})
    payload = {'success': True, 'data': {'name': 'Home', 'capacity': 15000}} if status == 200 else b'Not found'
    stadium = replace(capture('club_stadium', url + '/stadium', profile.body if same_capture else payload),
                      captured_at='2026-11-26T08:59:59.000000+00:00', status_code=status)
    if same_capture:
        stadium = replace(stadium, capture_id=profile.capture_id)
    _write_replay_manifest(tmp_path, [profile, stadium])
    return cli._load_records(tmp_path), profile, stadium


@pytest.mark.parametrize('stadium_first', [False, True])
def test_paired_stadium_404_has_two_source_proofs_on_zero_row_readback(tmp_path, monkeypatch, stadium_first):
    from dags.scripts import run_transfermarkt_core as cli

    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    records, profile, stadium = _stadium_pair(tmp_path)
    sources = (stadium, profile) if stadium_first else (profile, stadium)
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=tuple(SectionTarget(src.endpoint, src.url) for src in sources))
    fetch, requested = cli._fixture_fetch(records), []

    def recorded(url, **options):
        requested.append(url)
        return fetch(url, **options)

    runner = CoreRun(recorded, request_raw_loader=cli._fixture_raw_loader(records), extracted_at=CLOCK)
    db = CoreDB()
    writer = CoreWriter(db)
    result = runner.run(plan, writer=writer, connection_factory=lambda: db)
    assert requested == [src.url for src in sources] and result.dq['passed']
    assert len(result.receipts) == 2 and all(r['receipt_readback'] for r in result.receipts)
    for table in ('stadium_observations', 'club_stadium_relations'):
        assert db.sql.execute(f'SELECT COUNT(*) FROM transfermarkt_{table}').fetchone() == (0,)
        status, count, expected, raw_proof = db.sql.execute(
            'SELECT observation_status, row_count, expected_rows, status_proof '
            'FROM transfermarkt_core_receipts_v1 WHERE entity=?', (table,)).fetchone()
        assert (status, count, expected) == ('observed_empty', 0, 0)
        proof = json.loads(raw_proof)
        assert (proof['club_id'], proof['scope_id'], proof['cycle_id']) == ('127', 's', 'c')
        for key, src in [('profile', profile), ('stadium', stadium)]:
            assert proof[key] == {'raw_capture_id': src.capture_id, 'body_hash': src.body_hash, 'url': src.url,
                                  'captured_at': src.captured_at, 'status_code': src.status_code}
    assert db.sql.execute('SELECT club_id FROM transfermarkt_club_profile_observations').fetchall() == [('127',)]
    writes = list(writer.writes)
    assert runner.run(plan, writer=writer, connection_factory=lambda: db).receipts == result.receipts
    assert writer.writes == writes and db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone() == (3,)


@pytest.mark.parametrize('damage', ['unplanned-profile', 'missing-profile', 'profile-club', 'stadium-rate-limit',
                                    'stadium-scope', 'profile-cycle', 'stadium-status', 'profile-id', 'profile-clock',
                                    'profile-payload', 'same-capture', 'early-extraction', 'foreign-profile-target',
                                    'stadium-bytes', 'profile-raw-missing'])
def test_club_stadium_absence_requires_an_exact_durable_pair(tmp_path, damage):
    from dags.scripts import run_transfermarkt_core as cli

    records, profile, stadium = _stadium_pair(
        tmp_path, status=429 if damage == 'stadium-rate-limit' else 404,
        profile_id=999 if damage in ('profile-club', 'foreign-profile-target') else 127,
        profile_url_id=999 if damage == 'foreign-profile-target' else 127, same_capture=damage == 'same-capture')
    sources = [stadium] if damage == 'unplanned-profile' else [stadium, profile]
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=tuple(SectionTarget(src.endpoint, src.url) for src in sources))
    if damage == 'missing-profile':
        del records[profile.url]
    fetch, loader, requested = cli._fixture_fetch(records), cli._fixture_raw_loader(records), []

    def recorded(url, **options):
        requested.append(url)
        result = fetch(url, **options)
        if damage == 'profile-payload' and url == profile.url:
            return replace(result, value={'success': True, 'data': {'id': 999, 'name': 'Other'}})
        return result

    changes = {'stadium-scope': (stadium.url, 'scope_id', 'elsewhere'),
               'profile-cycle': (profile.url, 'cycle_id', 'other'),
               'stadium-status': (stadium.url, 'status_code', 500),
               'profile-id': (profile.url, 'capture_id', 'a' * 64),
               'profile-clock': (profile.url, 'fetched_at', stadium.captured_at)}

    def load(capture_id, url):
        if damage == 'profile-raw-missing' and url == profile.url:
            raise FileNotFoundError('missing profile capture')
        body, manifest = loader(capture_id, url)
        if damage == 'stadium-bytes' and url == stadium.url:
            body = b'other'
        if damage in changes:
            target, field, value = changes[damage]
            if url == target:
                setattr(manifest, field, value)
        return body, manifest

    extracted_at = stadium.captured_at if damage == 'early-extraction' else CLOCK
    report = CoreRun(recorded, request_raw_loader=load, extracted_at=extracted_at).run(plan)
    assert requested == [src.url for src in sources]
    pair = [outcome for outcome in report.outcomes if outcome['entity'] in ('stadium_observations', 'club_stadium_relations')]
    assert len(pair) == 2 and all(item['status'] in ('unavailable', 'error') and not item['rows'] for item in pair)


def test_stadium_404_processed_first_waits_for_profile_extraction_clock(tmp_path, monkeypatch):
    from datetime import datetime
    from dags.scripts import run_transfermarkt_core as cli
    from scrapers.transfermarkt import orchestrator

    records, profile, stadium = _stadium_pair(tmp_path)
    first = datetime.fromisoformat(stadium.captured_at)
    second = datetime.fromisoformat(profile.captured_at)
    times = iter((first, second))

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(times)

    monkeypatch.setattr(orchestrator, 'datetime', Clock)
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=(
        SectionTarget(stadium.endpoint, stadium.url), SectionTarget(profile.endpoint, profile.url)))
    report = CoreRun(cli._fixture_fetch(records), request_raw_loader=cli._fixture_raw_loader(records)).run(plan)
    assert [o['status'] for o in report.outcomes] == ['observed_empty', 'observed_empty', 'observed_present']
    assert {o['extracted_at'] for o in report.outcomes} == {profile.captured_at}
    assert len({o['run_id'] for o in report.outcomes}) == 1


@pytest.mark.parametrize('raw_json_value', [False, True])
def test_stadium_200_in_an_explicit_pair_keeps_the_published_venue(tmp_path, raw_json_value):
    from dags.scripts import run_transfermarkt_core as cli

    records, profile, stadium = _stadium_pair(tmp_path, status=200)
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=(
        SectionTarget(profile.endpoint, profile.url), SectionTarget(stadium.endpoint, stadium.url)))
    fixture = cli._fixture_fetch(records)

    def fetch(url, **options):
        result = fixture(url, **options)
        return replace(result, value=json.dumps(result.value)) if raw_json_value else result

    report = CoreRun(fetch, request_raw_loader=cli._fixture_raw_loader(records),
                     extracted_at=CLOCK).run(plan)
    assert [(o['status'], o['rows']) for o in report.outcomes] == [('observed_present', 1)] * 3


@pytest.mark.parametrize('foreign', [None, 'canonical', 'og', 'navigation'])
def test_staff_rejects_a_contradictory_published_club(foreign):
    body = Path('tests/fixtures/transfermarkt/staff/sc-paderborn-07.html').read_bytes()
    ids = {marker: '999' if marker == foreign else '127' for marker in ('canonical', 'og', 'navigation')}
    identity = (
        f'<link rel="canonical" href="/another/mitarbeiter/verein/{ids["canonical"]}">'
        f'<meta property="og:url" content="https://www.transfermarkt.com/another/mitarbeiter/verein/{ids["og"]}">'
        f'<tm-subnavigation controller="verein" id="{ids["navigation"]}"></tm-subnavigation>'
    )
    outcome, = extract(capture('club_staff', STAFF, identity.encode() + body))
    if foreign:
        assert (outcome.status, outcome.status_reason, outcome.rows) == ('error', 'club_staff:club_mismatch', ())
    else:
        assert outcome.status == 'observed_present' and {row['club_id'] for row in outcome.rows} == {'127'}


@pytest.mark.parametrize(('endpoint', 'fields'), [
    ('club_stadium', {'baseDetails': {'isNationalTeam': False}}),
    ('club_profile', {'capacity': 15000, 'buildingDetails': {'fieldWidth': 68}}),
    ('club_stadium', {}),
    ('club_profile', {}),
    ('club_stadium', {'buildingDetails': {}}),
    ('club_profile', {'baseDetails': {}}),
    ('club_stadium', {'baseDetails': {'isNationalTeam': False}, 'capacity': 15000}),
    ('club_profile', {'baseDetails': {'isNationalTeam': False}, 'capacity': 15000}),
])
def test_t7_unproven_or_conflicting_response_type_cannot_write(tmp_path, monkeypatch, endpoint, fields):
    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_orchestrator import _write_replay_manifest

    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    club_url = f'{API}/club/127'
    stadium_url = club_url + '/stadium'
    url = club_url if endpoint == 'club_profile' else stadium_url
    sources = [capture(endpoint, url, {'success': True, 'data': {'id': 127, 'name': 'Paderborn', **fields}})]
    if endpoint == 'club_profile':
        sources.append(replace(capture('club_stadium', stadium_url, b'Not found'), status_code=404))
    _write_replay_manifest(tmp_path, sources)
    records = cli._load_records(tmp_path)
    plan = CoreScopePlan('L1', '2627', 's', 'c', section_targets=tuple(
        SectionTarget(source.endpoint, source.url) for source in sources))
    db = CoreDB()
    writer = CoreWriter(db)
    result = CoreRun(cli._fixture_fetch(records), request_raw_loader=cli._fixture_raw_loader(records),
                     extracted_at=CLOCK).run(plan, writer=writer, connection_factory=lambda: db)
    assert not result.receipts and not writer.writes
    assert all(item['status'] in {'error', 'unknown', 'unavailable'} and item['rows'] == 0 for item in result.outcomes)


@pytest.mark.parametrize(('endpoint', 'fields'), [
    ('club_profile', {'relativeUrl': '/sc-paderborn/startseite/verein/127'}),
    ('club_stadium', {'buildingDetails': {'fieldLength': 105}}),
    ('club_stadium', {'clubId': 127, 'location': {'countryId': 40}}),
])
def test_t7_sparse_payload_with_published_club_or_venue_fields_remains_conclusive(endpoint, fields):
    url = f'{API}/club/127' + ('/stadium' if endpoint == 'club_stadium' else '')
    outcomes = extract(capture(endpoint, url, {'success': True, 'data': {'id': 127, 'name': 'Home', **fields}}))
    assert all(item.status == 'observed_present' and len(item.rows) == 1 for item in outcomes)
