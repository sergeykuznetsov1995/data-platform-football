"""Durable cohort accounting and read-only current admission."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_discovery_continuation as cohorts
from dags.utils import transfermarkt_current_timetable as timetable

NOW = datetime(2026, 11, 5, 3, tzinfo=timezone.utc)


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def identity(**changes):
    return {**cohorts.identity_for(previous_snapshot_id='old', mode='daily',
        policy_hash='policy', request_limit=100, retry_limit=10,
        byte_cap_bytes=10000, day=cohorts.cohort_day(NOW)), **changes}


def prepared(tmp_path):
    path = tmp_path / 'active.json'
    job = cohorts.select_job(path, identity(), now=NOW, write=write)
    return path, job


def test_resume_later_slot_retains_grant_and_checkpoint(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='paused', portion=2, traffic={'requests': 12})
    cohorts.write_job(path, job, write)
    resumed = cohorts.select_job(path, identity(),
                                now=NOW + timedelta(hours=20), write=write)
    assert resumed == job
    assert resumed['identity']['request_limit'] == 100


@pytest.mark.parametrize('change', [dict(policy_hash='changed'), dict(request_limit=101),
    dict(previous_snapshot_id='other'), dict(parser_revision='other')])
def test_resume_rejects_identity_drift(tmp_path, change):
    path, _ = prepared(tmp_path)
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(**change), now=NOW, write=write)


def test_publication_ack_releases_slots_without_skipping_tomorrow(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='source_complete', final_manifest={'hash': 'manifest', 'snapshot_id': 'new'})
    cohorts.write_job(path, job, write)
    assert len(cohorts.reservations(NOW, path)) == 18
    with pytest.raises(ValueError, match='publication'):
        cohorts.mark_published(path, manifest_hash='wrong', snapshot_id='new', now=NOW, write=write)
    cohorts.mark_published(path, manifest_hash='manifest', snapshot_id='new', now=NOW, write=write)
    slots = cohorts.reservations(NOW, path)
    assert slots == ((NOW + timedelta(days=1), NOW + timedelta(days=1, minutes=55)),)
    done = cohorts.select_job(path, identity(previous_snapshot_id='new'), now=NOW, write=write)
    assert done['status'] == 'published'
    next_day = cohorts.select_job(path, identity(previous_snapshot_id='new', cohort_day='2026-11-06'),
                                 now=NOW + timedelta(days=1), write=write)
    assert next_day['status'] == 'ready'
    assert next_day['job_id'] != done['job_id']


def test_corrupt_marker_only_reserves_bootstrap_and_discovery_fails_closed(tmp_path):
    path, _ = prepared(tmp_path)
    path.write_text('{}')
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] == [3, 3]
    with pytest.raises(ValueError, match='integrity'):
        cohorts.select_job(path, identity(), now=NOW, write=write)


def test_current_stops_before_slot_and_backfill_unchanged(tmp_path, monkeypatch):
    path, _ = prepared(tmp_path)
    monkeypatch.setattr(cohorts, 'JOB_PATH', path)
    before = NOW.replace(hour=4, minute=45)
    assert timetable.available_work_seconds(before, reserve_discovery=True) == 900
    assert timetable.available_work_seconds(before) == 2700
    assert timetable.next_admitted_time(NOW.replace(hour=5), reserve_discovery=True) == NOW.replace(hour=5, minute=55)
    assert timetable.next_admitted_time(NOW.replace(hour=2), reserve_discovery=True) == NOW.replace(hour=3, minute=55)


def test_three_delivery_gaps_keep_drain_and_rollback_headroom(tmp_path):
    path, _ = prepared(tmp_path)
    windows = cohorts.reservations(NOW, path)
    gaps = [(b[0] - a[1]).total_seconds() for a, b in zip(windows, windows[1:])]
    assert sum(gap >= 5260 + 3600 + 600 for gap in gaps[:9]) == 3
    assert all(start.hour not in (1, 2) for start, _ in windows)


def test_expired_cohort_never_renews_its_paid_grant(tmp_path):
    path, job = prepared(tmp_path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='expired'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert [start.hour for start, _ in cohorts.reservations(NOW + timedelta(days=1), path)] == [3, 3]


@pytest.mark.parametrize('payload', [None, 12, 'broken', [], True])
def test_nonobject_marker_falls_back_without_crashing_current(tmp_path, payload):
    path = tmp_path / 'active.json'
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match='object'):
        cohorts.read_job(path)
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] == [3, 3]


@pytest.mark.parametrize('field,value', [
    ('identity', []), ('status', []), ('started_at', {}), ('updated_at', None),
    ('final_manifest', []), ('artifacts', []), ('sizing', []), ('traffic', []),
    ('traffic', {'by_entity': []}), ('traffic', {'by_entity': {'bad': None}}),
    ('identity', {'cohort_day': []}), ('portion', '0'),
    ('final_manifest', {'snapshot_id': []}), ('artifacts', {'file': None}),
])
def test_malformed_nested_marker_fails_closed_and_releases_current(tmp_path, field, value):
    path, job = prepared(tmp_path)
    job[field] = value
    cohorts.write_job(path, job, write)
    with pytest.raises(ValueError):
        cohorts.read_job(path)
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] == [3, 3]


@pytest.mark.parametrize('value', ['broken', '', '-1', 'null', 'NaN', '999999999999999999999'])
def test_manual_invalid_drain_refuses_instead_of_starting(tmp_path, value):
    path = tmp_path / 'drain'
    path.write_text(value)
    with pytest.raises(ValueError, match='invalid'):
        cohorts.ensure_manual_delivery_admission(NOW, path)


def test_manual_active_drain_blocks_delayed_worker_but_expired_flag_does_not(tmp_path):
    path = tmp_path / 'drain'
    cohorts.ensure_manual_delivery_admission(NOW, path)
    path.write_text(str(int((NOW + timedelta(minutes=5)).timestamp())))
    with pytest.raises(ValueError, match='during delivery drain'):
        cohorts.ensure_manual_delivery_admission(NOW, path)
    cohorts.ensure_manual_delivery_admission(NOW + timedelta(minutes=6), path)


def test_continuation_marks_running_only_on_explicit_claim(tmp_path):
    from scrapers.transfermarkt.models import LeaseTrafficSnapshot, SharedTrafficLedger

    def ledger_with_traffic():
        ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                     soft_provider_bytes=9000, retry_limit=10)
        ledger.observe_lease('lease-1', LeaseTrafficSnapshot(up_bytes=10, down_bytes=90))
        ledger.record_attempt(entity='competition_registry', decoded_bytes=100,
                              provider_up_bytes=10, provider_down_bytes=90,
                              retry=True, duration_seconds=0.5)
        return ledger

    path, job = prepared(tmp_path)
    job.update(status='paused', portion=1, traffic=ledger_with_traffic().snapshot())
    cohorts.write_job(path, job, write)
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    continuation = cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                                 now=lambda: NOW, run_id='run-2')
    # Construction validates and restores cumulative traffic but must not
    # claim the slot: a later setup failure leaves the cohort resumable.
    assert cohorts.read_job(path)['status'] == 'paused'
    assert ledger.snapshot()['requests'] == 1
    assert ledger.snapshot()['retries'] == 1
    continuation.mark_running()
    saved = cohorts.read_job(path)
    assert saved['status'] == 'running'
    assert saved['portion'] == 2
    assert saved['traffic']['requests'] == 1
    assert saved['traffic']['retries'] == 1
    with pytest.raises(ValueError, match='already running'):
        continuation.mark_running()


@pytest.mark.parametrize('status', ['running', 'unsettled', 'failed', 'writing'])
def test_continuation_still_rejects_unsettled_or_unknown_states(tmp_path, status):
    from scrapers.transfermarkt.models import SharedTrafficLedger

    path, job = prepared(tmp_path)
    job.update(status=status)
    cohorts.write_job(path, job, write)
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not settled and resumable'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert cohorts.read_job(path)['status'] == status


# --- One-time pinned recovery of the single audited legacy 'running' marker ---

import hashlib
from pathlib import Path

from scrapers.transfermarkt.models import (LeaseTrafficSnapshot,
                                           SharedTrafficLedger,
                                           stable_payload_hash)


def _traffic():
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    ledger.observe_lease('lease-1', LeaseTrafficSnapshot(up_bytes=10, down_bytes=90))
    ledger.record_attempt(entity='competition_registry', decoded_bytes=100,
                          provider_up_bytes=10, provider_down_bytes=90,
                          retry=True, duration_seconds=0.5)
    return ledger.snapshot()


def _paused_manifest(traffic):
    return {'status': 'paused', 'cycle_id': 'cycle-1', 'run_id': 'run-1',
            'portion': 1, 'traffic': traffic, 'source_status': 'incomplete',
            'writes': [], 'error': 'discovery portion exhausted; checkpoint retained'}


def _failed_manifest():
    return {'status': 'failed', 'cycle_id': 'cycle-2', 'run_id': 'run-2',
            'error': {'type': 'ValueError',
                      'message': 'retry_budget differs from the shared traffic ledger limit'},
            'source_status': 'not_started', 'writes': [],
            'write_status': {'status': 'not_started', 'authorized': False,
                             'attempted': False, 'completed': False,
                             'affected_tables': []}}


def _write_manifest(root, name, manifest):
    digest = stable_payload_hash(manifest)
    path = root / 'runs' / f'{name}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'manifest_hash': digest, 'manifest': manifest}))
    return {'path': str(path.relative_to(root)), 'hash': digest}


def stranded(tmp_path, monkeypatch, *, paused=None, failed=None):
    """Legacy marker: claimed 'running' at construction, then setup failed."""
    path, job = prepared(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps({'https://example.test/a': {'hash': 'x'}}))
    failures = checkpoint.with_suffix('.failures.json')
    failures.write_text(json.dumps({'https://example.test/a': {'status': 'ok'}}))
    traffic = _traffic()
    job.update(status='running', portion=2, traffic=traffic, last_run_id='run-2',
               artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (checkpoint, failures)})
    cohorts.write_job(path, job, write)
    root = path.parent.parent
    paused_pin = _write_manifest(root, 'paused', paused or _paused_manifest(traffic))
    failed_pin = _write_manifest(root, 'failed', failed or _failed_manifest())
    monkeypatch.setattr(cohorts, 'KNOWN_SETUP_FAILURE_RECOVERY', {
        'marker_hash': stable_payload_hash(job),
        'paused_manifest': paused_pin, 'failed_manifest': failed_pin})
    return path, job


def test_pinned_recovery_preserves_accounting_and_resumes(tmp_path, monkeypatch):
    path, job = stranded(tmp_path, monkeypatch)
    resumed = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=2), write=write)
    assert resumed['status'] == 'paused'
    for field in ('portion', 'started_at', 'identity', 'traffic', 'artifacts',
                  'job_id', 'checkpoint_path', 'last_run_id'):
        assert resumed[field] == job[field]
    recovery = resumed['recovery']
    assert 'setup failure' in recovery['reason']
    assert recovery['marker_hash'] == stable_payload_hash(job)
    assert len(recovery['paused_manifest_hash']) == 64
    assert len(recovery['failed_manifest_hash']) == 64
    # The re-settled cohort is genuinely resumable by the normal continuation.
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    continuation = cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                                 now=lambda: NOW + timedelta(hours=2),
                                                 run_id='run-3')
    assert continuation.portion == 3
    assert ledger.snapshot()['requests'] == 1
    # Repeat selection is a no-op: the marker is simply 'paused' now.
    before = path.read_bytes()
    again = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=3), write=write)
    assert again['status'] == 'paused'
    assert path.read_bytes() == before


def test_unrelated_running_marker_stays_rejected(tmp_path, monkeypatch):
    path, job = prepared(tmp_path)
    job.update(status='running', portion=1, last_run_id='run-x')
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='unresolved traffic or writes'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


def test_recovery_rejects_changed_artifact_without_writing(tmp_path, monkeypatch):
    path, job = stranded(tmp_path, monkeypatch)
    Path(job['checkpoint_path']).write_text(json.dumps({'tampered': True}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='changed after settlement'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'running'


def test_recovery_rejects_tampered_manifest_without_writing(tmp_path, monkeypatch):
    path, job = stranded(tmp_path, monkeypatch)
    pinned = cohorts.KNOWN_SETUP_FAILURE_RECOVERY['paused_manifest']
    envelope_path = path.parent.parent / pinned['path']
    envelope = json.loads(envelope_path.read_text())
    envelope['manifest']['portion'] = 99
    envelope_path.write_text(json.dumps(envelope))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='differs from the audited pin'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


@pytest.mark.parametrize('kind,manifest', [
    ('failed', {**_failed_manifest(),
                'error': {'type': 'ValueError', 'message': 'some other setup error'}}),
    ('failed', {**_failed_manifest(), 'error': {'type': 'RuntimeError',
                'message': 'retry_budget differs from the shared traffic ledger limit'}}),
    ('failed', {**_failed_manifest(), 'source_status': 'failed'}),
    ('failed', {**_failed_manifest(), 'writes': [{'table': 'x', 'rows': 1}]}),
    ('failed', {**_failed_manifest(),
                'write_status': {'attempted': True}}),
    ('failed', {**_failed_manifest(), 'run_id': 'run-1'}),
    ('paused', {**_paused_manifest(_traffic()), 'status': 'failed'}),
    ('paused', {**_paused_manifest(_traffic()), 'portion': 2}),
    ('paused', {**_paused_manifest({**_traffic(), 'requests': 2})}),
    ('paused', {**_paused_manifest({**_traffic(), 'provider_metered_bytes': 0})}),
    ('paused', {**_paused_manifest({**_traffic(), 'cache_hits': 9})}),
])
def test_recovery_rejects_wrong_evidence_semantics(tmp_path, monkeypatch, kind, manifest):
    kwargs = {'paused' if kind == 'paused' else 'failed': manifest}
    path, _ = stranded(tmp_path, monkeypatch, **kwargs)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='recovery .* evidence does not match'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'running'


def test_recovery_never_bypasses_age_or_identity(tmp_path, monkeypatch):
    path, job = stranded(tmp_path, monkeypatch)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='expired'):
        cohorts.select_job(path, identity(), now=NOW + timedelta(days=1), write=write)
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=101), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'running'


def test_recovery_fails_closed_on_missing_manifest(tmp_path, monkeypatch):
    path, _ = stranded(tmp_path, monkeypatch)
    (path.parent.parent / cohorts.KNOWN_SETUP_FAILURE_RECOVERY['failed_manifest']['path']).unlink()
    before = path.read_bytes()
    with pytest.raises(ValueError, match='missing or unreadable'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
