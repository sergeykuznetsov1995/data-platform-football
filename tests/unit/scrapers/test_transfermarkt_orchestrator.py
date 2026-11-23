"""Offline orchestration tests for the stage-B core run (#2404)."""
import gzip
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from scrapers.transfermarkt import orchestrator
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.orchestrator import (
    ClubTarget,
    CoreRun,
    CoreRunError,
    CoreScopePlan,
    coach_history_url,
    coach_profile_url,
    mv_history_url,
    squad_url,
    transfers_url,
)
from tests.unit.scrapers.test_transfermarkt_core import EXTRACTED_AT, UNIVERSE, _load
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

pytestmark = pytest.mark.unit

FIXTURES = Path('tests/fixtures/transfermarkt/stage_a_rewrite')
YEAR = 2026


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)


def _plan(**overrides):
    values = dict(
        league='GB1', season='2627', scope_id='tm-scope-test', cycle_id='tm-cycle-test',
        edition_id='2026',
        clubs=(ClubTarget(club_id='11', club_slug='arsenal', club_name='Arsenal',
                          squad_url=squad_url('arsenal', '11', YEAR)),),
        market_value_player_ids=(), transfer_player_ids=(), coach_profile_ids=(),
    )
    values.update(overrides)
    return CoreScopePlan(**values)


class _ReplayFetch:
    """Serve the recorded sample through the client fetch contract."""

    def __init__(self, endpoints):
        self.records = json.loads((FIXTURES / 'manifest.json').read_text())
        self.endpoints = endpoints
        self.calls = []

    def _fixture(self, url):
        for item in self.records['fixtures']:
            if item['endpoint'] not in self.endpoints:
                continue
            record = json.loads((FIXTURES / item['capture_file']).read_text())
            if record['url'] == url:
                return record, FIXTURES / item['blob_file']
        return None

    def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
        self.calls.append((label, url))
        found = self._fixture(url)
        if found is None:
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='fixture_missing')
        record, blob = found
        text = gzip.decompress(blob.read_bytes()).decode('utf-8')
        value = json.loads(text) if as_json else text
        return FetchOutcome(
            status=FetchStatus.OK, value=value,
            raw_capture_id=record['capture_id'], raw_body_hash=record['content_hash'],
            raw_fetched_at=record['fetched_at'], cache_hit=False,
            status_code=record['status_code'],
        )

    def raw_loader(self, capture_id):
        for item in self.records['fixtures']:
            if item['capture_id'] != capture_id:
                continue
            record = json.loads((FIXTURES / item['capture_file']).read_text())
            return gzip.decompress((FIXTURES / item['blob_file']).read_bytes()), _Record(record)
        raise KeyError(capture_id)


class _Record:
    def __init__(self, record):
        self.content_hash = record['content_hash']
        self.url = record['url']
        self.fetched_at = record['fetched_at']


def test_squad_scope_run_produces_outcomes_dq_and_one_bundle_per_capture():
    replay = _ReplayFetch({'squad'})
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club',
                                   squad_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['entity'] for item in report.outcomes] == [
        'squad_memberships', 'player_attribute_observations', 'player_contract_observations']
    assert all(item['status'] == 'observed_present' for item in report.outcomes)
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    for table in ('transfermarkt_squad_memberships',
                  'transfermarkt_player_attribute_observations',
                  'transfermarkt_player_contract_observations'):
        assert db.sql.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == report.outcomes[0]['rows']
    # Idempotent replay of the same unit returns the same verified receipt.
    again = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert again.receipts == report.receipts
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] \
        == report.outcomes[0]['rows']


def test_career_pair_writes_one_bundle_per_capture():
    replay = _ReplayFetch({'market_value_points'})
    capture = _load('market_value_points')
    player_id = capture.url.rsplit('/', 1)[-1]
    plan = _plan(clubs=(), market_value_player_ids=(player_id,))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes[0]['entity'] == 'market_value_points'
    assert len(report.receipts) == 1
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_market_value_points').fetchone()[0] \
        == report.outcomes[0]['rows']


def test_unavailable_fetch_records_captureless_outcomes_without_bundles():
    def dead_fetch(url, **kwargs):
        return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')

    plan = _plan()
    run = CoreRun(dead_fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    report = run.run(plan)
    assert [item['status'] for item in report.outcomes] == ['unavailable'] * 3
    assert all(item['source_capture_id'] is None for item in report.outcomes)
    assert report.receipts == () and report.receipt is None
    assert report.dq['passed'] is True  # structural DQ; coverage stays 0


def test_not_applicable_transport_outcome_keeps_its_proof_and_status():
    def fetch(url, *, as_json, label, context=None, validator=None, **kwargs):
        return FetchOutcome(status=FetchStatus.NOT_APPLICABLE, status_code=200)

    run = CoreRun(fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    report = run.run(_plan())
    assert [item['status'] for item in report.outcomes] == ['not_applicable'] * 3
    assert report.dq['outcome_status_counts'] == {'not_applicable': 3}
    assert report.receipts == ()


def test_json_capture_without_raw_loader_fails_closed():
    replay = _ReplayFetch({'market_value_points'})
    capture = _load('market_value_points')
    player_id = capture.url.rsplit('/', 1)[-1]
    plan = _plan(market_value_player_ids=(player_id,))
    run = CoreRun(replay.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    with pytest.raises(CoreRunError, match='raw body loader'):
        run.run(plan)


def test_planned_urls_cover_every_target_kind():
    plan = _plan(
        clubs=(ClubTarget('11', 'arsenal', squad_url=squad_url('arsenal', '11', YEAR),
                          coach_history_url=coach_history_url('arsenal', '11')),),
        market_value_player_ids=('110',),
        transfer_player_ids=('110',),
        coach_profile_ids=(('42', 'mikel-arteta'),),
    )
    assert orchestrator.planned_urls(plan) == (
        squad_url('arsenal', '11', YEAR),
        coach_history_url('arsenal', '11'),
        mv_history_url('110'),
        transfers_url('110'),
        coach_profile_url('mikel-arteta', '42'),
    )


def test_coach_history_writes_the_club_scoped_stint_bundle():
    replay = _ReplayFetch({'coach_history'})
    capture = _load('coach_history')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club',
                                   coach_history_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes[0]['entity'] == 'coach_stints'
    assert report.dq['verified_write_coverage'] == 1.0
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_coach_stints').fetchone()[0] == \
        report.outcomes[0]['rows']


def test_dq_reports_verified_coverage_only_after_receipts():
    replay = _ReplayFetch({'squad'})
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club', squad_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    unwritten = run.run(plan)
    assert unwritten.dq['receipt_diagnostics'] == ['receipt_absent']


def test_offline_cli_acceptance_over_the_recorded_sample(tmp_path, capsys):
    from dags.scripts import run_transfermarkt_core as cli

    report_path = tmp_path / 'report.json'
    exit_code = cli.main([
        'offline', '--fixtures-root', str(FIXTURES),
        '--league', 'GB1', '--season', '2627', '--report-out', str(report_path),
    ])
    assert exit_code == 0
    report = json.loads(report_path.read_text())
    statuses = {(item['entity'], item['status']) for item in report['outcomes']}
    assert ('squad_memberships', 'observed_present') in statuses
    assert ('squad_memberships', 'observed_empty') in statuses
    assert ('market_value_points', 'observed_present') in statuses
    assert ('transfer_events', 'observed_present') in statuses
    assert ('coach_stints', 'observed_present') in statuses  # full club history now
    assert report['dq']['passed'] is True and report['dq']['errors'] == []
    assert report['receipt'] is None and report['receipts'] == []
    # The sample mixes seasons: this run owns the three 2026 squads plus the
    # season-independent career and coach captures.
    assert report['fetched'] == 19 and report['cache_hits'] == 0

    other_season = tmp_path / 'report-2223.json'
    assert cli.main([
        'offline', '--fixtures-root', str(FIXTURES),
        '--league', 'GB1', '--season', '2223', '--report-out', str(other_season),
    ]) == 0
    second = json.loads(other_season.read_text())
    squad_rows = {item['rows'] for item in second['outcomes']
                  if item['entity'] == 'squad_memberships'}
    assert squad_rows == {55}  # the 2022 Japan squad alone


def test_offline_cli_fails_fast_on_missing_planned_fixture(tmp_path):
    from dags.scripts import run_transfermarkt_core as cli

    root = tmp_path / 'fixtures'
    root.mkdir()
    (root / 'manifest.json').write_text(json.dumps({'fixtures': []}))
    with pytest.raises(SystemExit) as caught:
        cli.main(['offline', '--fixtures-root', str(root), '--league', 'GB1', '--season', '2627'])
    assert caught.value.code == 2


def test_live_cli_refuses_without_explicit_transport_permission(capsys):
    from dags.scripts import run_transfermarkt_core as cli

    with pytest.raises(SystemExit) as caught:
        cli.main(['live', '--league', 'GB1', '--season', '2627',
                  '--scope-id', 's', '--cycle-id', 'c', '--club', '11:arsenal'])
    assert caught.value.code == 2


def test_finding_16_offline_squad_record_cannot_be_relabelled_as_coach_history():
    # Round 7 finding 16: both dispatch and raw loading bind a recorded endpoint to its route.
    from dags.scripts import run_transfermarkt_core as cli

    capture = _load('squad')
    args = cli._parser().parse_args(['offline', '--fixtures-root', str(FIXTURES),
                                     '--league', 'GB1', '--season', '2627'])
    record, blob = cli._load_records(FIXTURES)[capture.url]
    records = {capture.url: ({**record, 'endpoint': 'coach_history'}, blob)}
    with pytest.raises(ValueError) as refused:
        cli._offline_plan(args, records)
    assert str(refused.value) == f"Fixture endpoint 'coach_history' does not match its URL {capture.url!r}"

    replay = _ReplayFetch({'squad'})
    request = orchestrator._Request('squad', capture.url)
    result = replay.fetch(capture.url, as_json=False, label=request.label)
    with pytest.raises(CoreRunError, match='^squad raw capture endpoint differs$'):
        orchestrator._capture(request, result, cli._fixture_raw_loader(records), {})
    assert orchestrator._capture(request, result, replay.raw_loader, {}) == capture


def _write_replay_manifest(root, captures):
    fixtures = []
    for capture in captures:
        folder = root / 'captures' / capture.endpoint
        folder.mkdir(parents=True, exist_ok=True)
        record = {
            'endpoint': capture.endpoint, 'url': capture.url, 'capture_id': capture.capture_id,
            'content_hash': capture.body_hash, 'fetched_at': capture.captured_at,
            'status_code': capture.status_code, 'scope_id': 's', 'cycle_id': 'c',
        }
        (folder / f'{capture.capture_id}.json').write_text(json.dumps(record))
        blob_file = f'{capture.capture_id}.gz'
        (root / blob_file).write_bytes(gzip.compress(capture.body))
        fixtures.append({'capture_id': capture.capture_id, 'endpoint': capture.endpoint, 'blob_file': blob_file})
    (root / 'manifest.json').write_text(json.dumps({'fixtures': fixtures}))


@pytest.mark.parametrize('malformed_first', [False, True])
def test_offline_malformed_transfer_cannot_hide_capture_conflict(malformed_first, tmp_path, monkeypatch, capsys):
    # Round 9 finding 8
    from dags.scripts import run_transfermarkt_core as cli

    squad = _load('squad')
    body = b'{"transfers":'
    transfer = replace(_load('transfer_events'), capture_id=squad.capture_id,
                       body=body, body_hash=hashlib.sha256(body).hexdigest())
    _write_replay_manifest(tmp_path, [transfer, squad])
    # Give the two endpoint records separate blobs, even though they claim one ID.
    (tmp_path / 'transfer.gz').write_bytes(gzip.compress(body))
    path = tmp_path / 'manifest.json'
    manifest = json.loads(path.read_text())
    manifest['fixtures'][0]['blob_file'] = 'transfer.gz'
    path.write_text(json.dumps({'fixtures': manifest['fixtures'][:1]}))
    fetch = cli._fixture_fetch(cli._load_records(tmp_path))
    malformed = fetch(transfer.url, as_json=True, label='transfer_events')
    assert (malformed.status, malformed.error, malformed.raw_capture_id, malformed.raw_body_hash) == (
        FetchStatus.SCHEMA_ERROR, 'invalid_json', transfer.capture_id, transfer.body_hash)
    assert fetch.requested == fetch.reached == {transfer.url}
    if not malformed_first:
        manifest['fixtures'].reverse()
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(cli, '_write_options', lambda args: pytest.fail('unexpected writer setup'))
    with pytest.raises(SystemExit) as caught:
        cli.main(['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1',
                  '--season', '2627', '--write'])
    assert caught.value.code == 2
    assert capsys.readouterr().err.rstrip().endswith(': capture_evidence_conflict')


@pytest.mark.parametrize('edition', ['2026', '2025', None])
def test_offline_attendance_respects_explicit_edition(edition, tmp_path, monkeypatch, capsys):
    # Round 9 finding 10
    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_sections import (
        ATTENDANCE_PAGE_2, ATTENDANCE_START, PAGE_1_CAPTURED, PAGE_2_CAPTURED, _attendance_capture,
    )

    captures = [_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
                _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)]
    if edition != '2025':
        captures.insert(0, _load('squad'))  # 2026 squad and 2025 attendance
    _write_replay_manifest(tmp_path, captures)
    path = tmp_path / 'report.json'
    args = ['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1',
            '--season', '2526' if edition == '2025' else '2627', '--report-out', str(path)]
    if edition is not None:
        args.extend(['--edition-id', edition])
    if edition == '2026':
        monkeypatch.setattr(cli, '_fixture_fetch', lambda *a: pytest.fail('unexpected fetch setup'))
        monkeypatch.setattr(cli, '_write_options', lambda *a: pytest.fail('unexpected writer setup'))
        with pytest.raises(SystemExit) as caught:
            cli.main([*args, '--write'])
        assert caught.value.code == 2
        assert capsys.readouterr().err.rstrip().endswith(': attendance_edition_mismatch')
        return
    assert cli.main(args) == 0
    report = json.loads(path.read_text())
    assert report['fetched'] == len(captures)
    attendance, = [item for item in report['outcomes'] if item['entity'] == 'attendance_observations']
    assert (attendance['status'], attendance['rows']) == ('observed_present', 2)


def test_live_cli_refuses_foreign_attendance_before_transport(monkeypatch, capsys):
    # Round 9 finding 10
    from dags.scripts import run_transfermarkt_core as cli
    from scrapers.transfermarkt import client

    plan = _plan(attendance_editions=(orchestrator.AttendanceEditionTarget('GB1', 2026),
                                      orchestrator.AttendanceEditionTarget('GB1', 2025)))
    monkeypatch.setattr(cli, '_live_plan', lambda args: plan)
    monkeypatch.setattr(client, 'TransfermarktHttpClient', lambda: pytest.fail('unexpected transport setup'))
    with pytest.raises(SystemExit) as caught:
        cli.main(['live', '--league', 'GB1', '--season', '2627', '--edition-id', '2026',
                  '--scope-id', 's', '--cycle-id', 'c', '--club', '11:arsenal',
                  '--i-have-transport-permission'])
    assert caught.value.code == 2
    assert capsys.readouterr().err.rstrip().endswith(': attendance_edition_mismatch')


@pytest.mark.parametrize('different_body', [False, True], ids=['aliases', 'conflict'])
def test_shared_empty_squad_capture_keeps_each_requested_club(different_body):
    # Round 9 finding 12
    from scrapers.transfermarkt.native_writer import _timestamp

    empty = _load('squad', 3)
    clubs = tuple(ClubTarget(club_id, slug, squad_url=squad_url(slug, club_id, YEAR))
                  for club_id, slug in [('11', 'first'), ('12', 'second')])
    second_body = empty.body + b' ' if different_body else empty.body
    second_clock = '2026-11-21T11:00:00.000000+00:00'
    assert second_clock != empty.captured_at
    responses = {clubs[0].squad_url: (empty.body, empty.captured_at),
                 clubs[1].squad_url: (second_body, second_clock)}

    def fetch(url, **kwargs):
        body, clock = responses[url]
        return FetchOutcome(status=FetchStatus.OK, value=body.decode(), status_code=200,
                            raw_capture_id=empty.capture_id, raw_body_hash=hashlib.sha256(body).hexdigest(),
                            raw_fetched_at=clock)

    plan = _plan(clubs=clubs)
    run = CoreRun(fetch, extracted_at=EXTRACTED_AT)
    if different_body:
        with pytest.raises(CoreRunError, match='^capture_evidence_conflict$'):
            run.run(plan, writer=object(), connection_factory=lambda: pytest.fail('unexpected connection'))
        return
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.dq['errors'] == []
    assert report.dq['outcome_status_counts'] == {'observed_empty': 6}
    assert len(report.receipts) == 2 and all(r['verified'] is True for r in report.receipts)
    rows = db.sql.execute(
        "SELECT scope_values_json, captured_at, source_capture_id, source_body_hash "
        "FROM transfermarkt_core_receipts_v1 WHERE entity = 'squad_memberships'")
    assert {(dict(json.loads(scope))['club_id'], _timestamp(clock), capture_id, body_hash)
            for scope, clock, capture_id, body_hash in rows} == {
        (club.club_id, _timestamp(responses[club.squad_url][1]), empty.capture_id, empty.body_hash)
        for club in clubs}


@pytest.mark.parametrize('case', ['complete', 'club_values', 'unreached', 'missing_start', 'duplicate_start'])
def test_finding_17_offline_cli_accounts_for_every_manifest_record(case, tmp_path, monkeypatch, capsys):
    # Round 7 finding 17: success requires a representable edition and every fixture to be reached.
    from dataclasses import replace

    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_sections import (
        ATTENDANCE_PAGE_2, ATTENDANCE_START, PAGE_1_CAPTURED, PAGE_2_CAPTURED,
        _attendance_capture, _club_value_capture,
    )

    first = _attendance_capture('page_1.html', ATTENDANCE_START + '?page=01', PAGE_1_CAPTURED)
    second = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    if case == 'unreached':
        # An unavailable first page leaves the saved second page unrequested.
        first = replace(first, status_code=503)
    captures = [_load('squad'), second]
    if case != 'missing_start':
        captures.append(first)
    if case == 'duplicate_start':
        captures.append(replace(first, url=ATTENDANCE_START, capture_id='a' * 64))
    if case == 'club_values':
        captures.append(_club_value_capture())
    _write_replay_manifest(tmp_path, captures)
    report_path = tmp_path / 'report.json'
    args = ['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1', '--season', '2627',
            '--write', '--report-out', str(report_path)]

    db = CoreDB()
    writer = CoreWriter(db)
    monkeypatch.setattr(cli, '_write_options', lambda args: {'writer': writer, 'connection_factory': lambda: db})
    plan_errors = {
        'club_values': "The offline replay plan cannot represent 'club_values' fixtures yet",
        'missing_start': 'attendance_first_page_missing',
        'duplicate_start': 'attendance_duplicate_first_page',
    }
    if case in plan_errors:
        with pytest.raises(SystemExit) as caught:
            cli.main(args)
        assert caught.value.code == 2
        assert plan_errors[case] in capsys.readouterr().err
        assert writer.writes == []
        return

    if case == 'unreached':
        assert cli.main(args) == 2
        assert capsys.readouterr().err.splitlines() == ['Unreached fixture records:', second.url]
        report = json.loads(report_path.read_text())
        assert len(report['receipts']) == 1 and report['receipts'][0]['verified'] is True
        return

    assert cli.main(args) == 0
    report = json.loads(report_path.read_text())
    assert report['fetched'] == 3
    assert len(report['receipts']) == 2 and all(item['verified'] for item in report['receipts'])
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone() == (38,)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_attendance_observations').fetchone() == (2,)


def test_r8_manifest_endpoint_must_match_the_record(tmp_path, monkeypatch, capsys):
    # Round 8 finding 20
    from dags.scripts import run_transfermarkt_core as cli

    squad = _load('squad')
    _write_replay_manifest(tmp_path, [squad])
    path = tmp_path / 'manifest.json'
    manifest = json.loads(path.read_text())
    manifest['fixtures'][0]['endpoint'] = 'coach_history'
    path.write_text(json.dumps(manifest))
    (tmp_path / 'captures/squad').rename(tmp_path / 'captures/coach_history')
    monkeypatch.setattr(cli, '_offline_plan', lambda *args: pytest.fail('unexpected dispatch'))
    with pytest.raises(SystemExit) as caught:
        cli.main(['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1', '--season', '2627'])
    assert caught.value.code == 2
    record_path = tmp_path / 'captures/coach_history' / f'{squad.capture_id}.json'
    assert capsys.readouterr().err.rstrip().endswith(f'Fixture endpoint mismatch in {record_path}')


@pytest.mark.parametrize('case', ['conflicting_squad', 'missing_continuation', 'unreached_core'])
def test_r8_offline_replay_requires_every_request_and_eligible_record(case, tmp_path, monkeypatch, capsys):
    # Round 8 finding 21
    from dataclasses import replace

    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_sections import (
        ATTENDANCE_PAGE_2, ATTENDANCE_START, PAGE_1_CAPTURED, _attendance_capture,
    )

    squad = _load('squad')
    club_id = squad.url.split('/verein/')[1].split('/')[0]
    if case == 'conflicting_squad':
        extra = replace(squad, url=squad_url('another-slug', club_id, YEAR), capture_id='a' * 64)
    elif case == 'missing_continuation':
        extra = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    else:
        extra = _load('coach_profile')
        offline_plan = cli._offline_plan

        def skip_coach(*args):
            return replace(offline_plan(*args), coach_profile_ids=())

        monkeypatch.setattr(cli, '_offline_plan', skip_coach)
    _write_replay_manifest(tmp_path, [squad, extra])
    report_path = tmp_path / 'report.json'
    args = ['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1', '--season', '2627',
            '--write', '--report-out', str(report_path)]
    db = CoreDB()
    writer = CoreWriter(db)
    monkeypatch.setattr(cli, '_write_options', lambda args: {'writer': writer, 'connection_factory': lambda: db})

    if case == 'conflicting_squad':
        with pytest.raises(SystemExit) as caught:
            cli.main(args)
        assert caught.value.code == 2
        assert capsys.readouterr().err.rstrip().endswith(
            f'Conflicting squad fixture URLs for club {club_id}: {squad.url}, {extra.url}')
        assert writer.writes == []
        return

    assert cli.main(args) == 2
    expected = (['Missing fixture responses:', ATTENDANCE_PAGE_2] if case == 'missing_continuation'
                else ['Unreached fixture records:', extra.url])
    assert capsys.readouterr().err.splitlines() == expected
    receipt, = json.loads(report_path.read_text())['receipts']
    assert receipt['verified'] is True
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone() == (38,)


def test_offline_native_connection_submits_once(tmp_path, monkeypatch):
    from inspect import signature
    from unittest.mock import Mock

    import trino.dbapi

    from dags.scripts import run_transfermarkt_core as cli
    from scrapers.transfermarkt import writer

    connect = Mock()
    monkeypatch.setattr(trino.dbapi, 'connect', connect)
    monkeypatch.setattr(writer, 'TransfermarktIcebergWriter', Mock())
    monkeypatch.delenv('TRINO_PASSWORD', raising=False)
    args = cli._parser().parse_args(['offline', '--fixtures-root', str(tmp_path), '--league', 'GB1',
                                     '--season', '2627', '--write', '--trino-host', 'offline.invalid',
                                     '--trino-port', '8080', '--trino-user', 'replay'])
    cli._write_options(args)['connection_factory']()
    expected = dict(host='offline.invalid', port=8080, user='replay', catalog='iceberg', schema='bronze',
                    http_scheme='http', auth=None, max_attempts=1)
    if 'legacy_prepared_statements' in signature(trino.dbapi.Connection).parameters:
        expected['legacy_prepared_statements'] = False
    connect.assert_called_once_with(**expected)
