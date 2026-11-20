from __future__ import annotations

import hashlib
import itertools
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dags.scripts import run_transfermarkt_scope_cycle as cycle
from scrapers.transfermarkt.models import FetchStatus
from scrapers.transfermarkt.registry import deterministic_scope_id, resolve_competition
from tests.unit.scripts.test_run_transfermarkt_scope_cycle import (
    _approved_args,
    _fake_subprocess,
    _parse_args,
    _payload,
)

pytestmark = pytest.mark.unit

START = 'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'
CAPTURED = '2026-11-20T12:00:00+00:00'


def test_history_attendance_extends_existing_cycle_and_noop_resumes(tmp_path, monkeypatch):
    from scrapers.transfermarkt import history_portion

    payload = _payload(tmp_path)
    payload['edition_record'].update(current=False, active=False)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    args.write_mode = 'native-only'
    args.refresh_mode = 'historical'
    args.standing_policy_sha256 = 'a' * 64
    monkeypatch.setenv('TM_DAG_ID', 'dag_backfill_transfermarkt')
    monkeypatch.setenv('TM_ATTENDANCE_ENABLED', 'true')
    monkeypatch.setenv('TM_HISTORY_DEADLINE_AT', '2026-11-22T18:45:00+00:00')
    monkeypatch.setenv('TM_HISTORY_PORTION_ID', 'attendance-one')
    monkeypatch.setattr(history_portion, 'remaining_seconds', lambda: 2700)
    monkeypatch.setattr(cycle, '_approval_mode', lambda *a: 'standing_policy')
    grant = cycle.PolicyGrant('history-grant', 'a' * 64, 1)
    monkeypatch.setattr(
        cycle, '_consume_approvals',
        lambda *a, **k: {'paid_proxy': grant, 'production_write': grant},
    )
    monkeypatch.setattr(
        cycle, '_enforce_standing_policy',
        lambda *a, **k: {'paid_proxy': grant, 'production_write': grant},
    )
    monkeypatch.setattr(cycle, '_record_standing_policy_authorization', lambda *a: None)
    monkeypatch.setattr(cycle, '_verify_adopted_raw_attempts', lambda *a: None)
    monkeypatch.setattr(cycle, '_history_complete_result', lambda identity, entity, result, path: result)

    def native_attendance(command, result):
        result['native_write_manifest'] = result['batch_manifest']
        result['native_write_manifest_complete'] = True
        if result['entity'] != cycle.ATTENDANCE_ENTITY:
            return result
        expected = [str(value) for value in range(1, 21)]
        output = result['outputs']['attendance']
        output['rows'] = 20
        row = result['native_write_manifest']['rows'][0]
        row['native_rows'] = 20
        result['attendance_receipt'] = {
            'verified': True,
            'receipt_readback': True,
            'final_season': True,
            'final_season_proof': 'history-backfill:registry-snapshot-20260711',
            'expected_club_ids': expected,
            'observed_club_ids': expected,
            'observed_at': CAPTURED,
            'row_count': 20,
            'physical_hash': row['native_hash'],
        }
        return result

    calls = []
    manifests = []
    ledgers = []
    runner = _fake_subprocess(calls, mutate=native_attendance)
    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=(),
        subprocess_runner=runner,
        manifest_writer=manifests.append,
        parent_ledger_writer=ledgers.append,
        monotonic_ns=itertools.count(start=0, step=1_000_000).__next__,
    )
    assert [command[command.index('--entity') + 1] for command, _ in calls] == [
        *cycle.ENTITY_ORDER, cycle.ATTENDANCE_ENTITY,
    ]
    attendance_command, attendance_options = calls[-1]
    assert json.loads(attendance_options['env']['TM_EXPECTED_CLUB_IDS_JSON']) == [
        str(value) for value in range(1, 21)
    ]
    assert attendance_command[attendance_command.index('--write-mode') + 1] == 'native-only'
    assert manifest['dq']['expected_entities'] == list(cycle._expected_entities(True))
    assert manifest['entities'][-1]['entity'] == cycle.ATTENDANCE_OUTPUT_ENTITY
    assert manifest['entities'][-1]['raw_rows'] == 20
    assert set(ledgers[-1]['by_entity']) == set(cycle._expected_entities(True))
    result_path = Path(payload['result_paths']['entity_staging_dir']) / 'attendance.json'
    assert json.loads(result_path.read_text())['attendance_receipt']['row_count'] == 20

    resumed = cycle.run_scope_cycle(
        args,
        operation_argv=(),
        subprocess_runner=runner,
        manifest_writer=manifests.append,
        parent_ledger_writer=ledgers.append,
    )
    assert resumed['manifest_digest'] == manifest['manifest_digest']
    assert len(calls) == 5
    assert len(manifests) == len(ledgers) == 1


def test_attendance_activation_preserves_base_checkpoint_identity(tmp_path):
    payload = _payload(tmp_path)
    argv = _approved_args(tmp_path, payload)[0]
    args = _parse_args(argv)
    identity = cycle._scope_identity(args)
    base = cycle._entity_limits(args)
    extended = cycle._entity_limits(args, include_attendance=True)
    assert cycle._checkpoint_identity(identity, args, base) == cycle._checkpoint_identity(
        identity, args, extended,
    )
    assert set(extended) == set(cycle._entity_order(True))


def test_backfill_policy_authorizes_attendance_only_for_enabled_history():
    required = cycle.required_write_tables('native-only', include_attendance=True)
    policy = cycle.load_standing_policy(
        Path('dags/configs/transfermarkt/standing_backfill_policy.json'))
    assert required <= set(policy.allowed_write_tables)
    attendance_tables = {
        'iceberg.bronze.transfermarkt_attendance_observations',
        'iceberg.ops.transfermarkt_attendance_receipts_v1',
    }
    assert not attendance_tables & cycle.required_write_tables('native-only')


def test_history_capture_uses_raw_pages_and_real_parser(tmp_path, monkeypatch):
    from dags.utils import transfermarkt_attendance_history as history

    body = (Path('tests/fixtures/transfermarkt/attendance/single_page.html')).read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    competition = resolve_competition('GB1')
    scope_id = deterministic_scope_id('GB1', '2025')
    monkeypatch.setenv('TM_SCOPE_ID', scope_id)
    outcome = SimpleNamespace(
        status=FetchStatus.OK,
        raw_capture_id='capture-attendance',
        raw_fetched_at=CAPTURED,
        raw_body_hash=digest,
    )
    raw = SimpleNamespace(
        url=START,
        capture_id='capture-attendance',
        content_hash=digest,
        fetched_at=CAPTURED,
        status_code=200,
        scope_id=scope_id,
    )
    calls = []

    def fetch(url, **kwargs):
        calls.append((url, kwargs))
        return outcome

    scraper = SimpleNamespace(
        _fetch_endpoint_outcome=fetch,
        _http_client=SimpleNamespace(
            _raw_store=SimpleNamespace(load_capture=lambda capture_id: (body, raw))),
    )
    written = []

    def writer(instance, extraction, expected, preflight, **identity):
        written.append((extraction, expected, preflight, identity))
        return {
            'verified': True,
            'receipt_readback': True,
            'expected_club_ids': sorted(expected, key=int),
            'observed_at': identity['observed_at'],
            'final_season': True,
            'physical_hash': 'a' * 64,
            'row_count': len(extraction.rows),
        }

    monkeypatch.setattr(
        'dags.utils.transfermarkt_attendance.write_attendance_observations', writer)
    extraction, receipt = history.capture_attendance_history(
        scraper,
        competition_record=competition,
        edition_id='2025',
        expected_club_ids=('11', '631'),
        run_key='history-child',
        write_mode='native-only',
        reader_revision=7,
    )
    assert receipt['verified'] is True
    assert calls[0][0] == START
    assert calls[0][1]['label'] == 'attendance'
    assert extraction.final_season is True
    assert extraction.final_season_proof.startswith('history-backfill:')
    assert {row['club_id'] for row in extraction.rows} == {'11', '631'}
    assert {row['observed_at'] for row in extraction.rows} == {CAPTURED}
    assert written[0][1] == ('11', '631')
    assert written[0][2]['write_mode'] == 'native-only'
    assert written[0][3]['competition_id'] == 'GB1'


def test_history_capture_rejects_mismatched_writer_receipt(tmp_path, monkeypatch):
    from dags.utils import transfermarkt_attendance_history as history

    body = Path('tests/fixtures/transfermarkt/attendance/single_page.html').read_bytes()
    monkeypatch.setattr(
        'dags.utils.transfermarkt_attendance.write_attendance_observations',
        lambda *a, **k: {'verified': False},
    )
    digest = hashlib.sha256(body).hexdigest()
    outcome = SimpleNamespace(
        status=FetchStatus.OK,
        raw_capture_id='capture-attendance',
        raw_fetched_at=CAPTURED,
        raw_body_hash=digest,
    )
    raw = SimpleNamespace(
        url=START,
        capture_id='capture-attendance',
        content_hash=digest,
        fetched_at=CAPTURED,
        status_code=200,
        scope_id=deterministic_scope_id('GB1', '2025'),
    )
    monkeypatch.setenv('TM_SCOPE_ID', raw.scope_id)
    scraper = SimpleNamespace(
        _fetch_endpoint_outcome=lambda *a, **k: outcome,
        _http_client=SimpleNamespace(
            _raw_store=SimpleNamespace(load_capture=lambda capture_id: (body, raw))),
    )
    with pytest.raises(RuntimeError, match='receipt differs'):
        history.capture_attendance_history(
            scraper,
            competition_record=resolve_competition('GB1'),
            edition_id='2025',
            expected_club_ids=('11', '631'),
            run_key='history-child',
            write_mode='native-only',
            reader_revision=7,
        )


def test_history_empty_participants_commit_proof_without_http(monkeypatch):
    from dags.utils import transfermarkt_attendance_history as history

    monkeypatch.setenv('TM_SCOPE_ID', deterministic_scope_id('GB1', '2025'))
    monkeypatch.setenv('TM_EXPECTED_CLUB_PROOF_JSON', json.dumps({
        'listing_status': 'authoritative_empty',
        'expected_team_ids': [],
        'listing_source_body_hash': 'b' * 64,
        'fetched_at': CAPTURED,
    }))
    written = []

    def writer(instance, extraction, expected, preflight, **identity):
        written.append((extraction, expected, identity))
        return {
            'verified': True,
            'receipt_readback': True,
            'expected_club_ids': [],
            'observed_at': identity['observed_at'],
            'final_season': True,
            'physical_hash': 'a' * 64,
            'row_count': 0,
        }

    monkeypatch.setattr(
        'dags.utils.transfermarkt_attendance.write_attendance_observations', writer)
    extraction, receipt = history.capture_attendance_history(
        SimpleNamespace(),
        competition_record=resolve_competition('GB1'),
        edition_id='2025',
        expected_club_ids=(),
        run_key='history-empty',
        write_mode='native-only',
        reader_revision=7,
    )
    assert extraction.result.status == 'observed_empty'
    assert extraction.rows == ()
    assert receipt['row_count'] == 0
    assert written[0][0].result.proof == 'players-listing:' + 'b' * 64
    assert written[0][2]['observed_at'] == CAPTURED
