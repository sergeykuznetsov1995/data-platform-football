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
    assert len(cohorts.reservations(NOW, path)) == 26
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
    # 10:45 sits in the 07:55→11:00 delivery gap of the 13-slot timetable (#2145).
    before = NOW.replace(hour=10, minute=45)
    assert timetable.available_work_seconds(before, reserve_discovery=True) == 900
    assert timetable.available_work_seconds(before) == 2700
    assert timetable.next_admitted_time(NOW.replace(hour=5), reserve_discovery=True) == NOW.replace(hour=5, minute=55)
    assert timetable.next_admitted_time(NOW.replace(hour=2), reserve_discovery=True) == NOW.replace(hour=3, minute=55)


def test_three_delivery_gaps_keep_drain_and_rollback_headroom(tmp_path):
    path, _ = prepared(tmp_path)
    windows = cohorts.reservations(NOW, path)
    # Thirteen slots: 03,04,05,06,07,11,12,13,14,15,19,21,23 UTC (#2145,
    # owner authorization 2026-11-07), preserving 07→11, 15→19 and 23→03.
    assert [start.hour for start, _ in windows[:13]] == [3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 19, 21, 23]
    gaps = [(b[0] - a[1]).total_seconds() for a, b in zip(windows, windows[1:])]
    assert sum(gap >= 5260 + 3600 + 600 for gap in gaps[:13]) == 3
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


# --- Expired paused cohort rollover (#2145, owner authorization 2026-11-07) ---

def _settled_paused(tmp_path, **changes):
    """A settled 'paused' marker with real checkpoint evidence on disk."""
    path, job = prepared(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps({'https://example.test/a': {'hash': 'x'}}))
    failures = checkpoint.with_suffix('.failures.json')
    failures.write_text(json.dumps({'https://example.test/a': {'status': 'ok'}}))
    job.update(status='paused', portion=1, traffic=_traffic(), last_run_id='run-1',
               artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (checkpoint, failures)}, **changes)
    cohorts.write_job(path, job, write)
    return path, job


def test_expired_paused_cohort_rolls_over_with_evidence_preserved(tmp_path):
    path, old = _settled_paused(tmp_path)
    later = NOW + timedelta(days=1, hours=2)

    new = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                             now=later, write=write)

    # A genuinely new cohort: fresh started_at/job_id/checkpoint, no traffic.
    assert new['status'] == 'ready'
    assert new['portion'] == 0
    assert new['traffic'] is None
    assert new['job_id'] != old['job_id']
    assert new['started_at'] == later.isoformat()
    assert new['checkpoint_path'] != old['checkpoint_path']
    # The exact old marker is preserved as <job_id>/expired.json: checkpoint,
    # started_at and cumulative traffic untouched, nothing renewed or reset.
    archived = cohorts.read_job(path.parent / old['job_id'] / 'expired.json')
    assert archived == old
    assert archived['started_at'] == old['started_at']
    assert archived['traffic'] == old['traffic']
    assert archived['portion'] == old['portion']
    # The active marker is the fresh cohort, not a renewed old one.
    active = cohorts.read_job(path)
    assert active['job_id'] == new['job_id']
    assert active['started_at'] == later.isoformat()
    # A same-day call resumes the fresh cohort without touching the archive.
    again = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                               now=later + timedelta(minutes=30), write=write)
    assert again['job_id'] == new['job_id']


def test_expired_paused_same_identity_collision_refuses_without_archiving(tmp_path):
    path, job = _settled_paused(tmp_path)
    later = NOW + timedelta(days=1, hours=2)
    # The caller supplies the identity unchanged: it hashes to the same
    # job_id as the expired paused cohort.  Rollover would archive and then
    # recreate a cohort in the same job_id/checkpoint namespace, so select_job
    # must refuse before any artifact read or archive write.
    before = path.read_bytes()
    with pytest.raises(ValueError, match='collides with the expired paused cohort'):
        cohorts.select_job(path, identity(), now=later, write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()
    # A genuinely changed identity (new cohort_day and/or policy) still rolls over.
    new = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                             now=later, write=write)
    assert new['job_id'] != job['job_id']
    assert (path.parent / job['job_id'] / 'expired.json').exists()


@pytest.mark.parametrize('status', ['ready', 'running', 'source_complete', 'failed', 'unsettled', 'writing'])
def test_expired_non_paused_cohort_never_rolls_over(tmp_path, status):
    path, job = _settled_paused(tmp_path)
    job.update(status=status)
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='expired'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_with_incomplete_identity_refuses_without_archiving(tmp_path):
    path, job = _settled_paused(tmp_path)
    # Structural gap: a required identity key is missing from the expired
    # marker.  Fail closed before any artifact read or archive write; the
    # active marker stays byte-identical and no archive is created.
    del job['identity']['parser_revision']
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='incomplete key set'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_artifact_drift_refuses_without_archiving(tmp_path):
    path, job = _settled_paused(tmp_path)
    Path(job['checkpoint_path']).write_text(json.dumps({'tampered': True}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='changed after settlement'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_missing_artifact_refuses_without_archiving(tmp_path):
    path, job = _settled_paused(tmp_path)
    Path(job['checkpoint_path']).with_suffix('.failures.json').unlink()
    before = path.read_bytes()
    with pytest.raises(ValueError, match='missing or unreadable'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_without_settled_evidence_refuses(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='paused', portion=1, traffic=_traffic())
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='no settled artifact evidence'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_paused_marker_with_future_start_time_fails_closed(tmp_path):
    path, job = _settled_paused(tmp_path, started_at=(NOW + timedelta(hours=1)).isoformat())
    before = path.read_bytes()
    with pytest.raises(ValueError, match='expired'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_paused_marker_just_under_24h_still_resumes_without_rollover(tmp_path):
    path, job = _settled_paused(tmp_path)
    before = path.read_bytes()
    resumed = cohorts.select_job(path, identity(), now=NOW + timedelta(days=1, microseconds=-1),
                                 write=write)
    assert resumed['job_id'] == job['job_id']
    assert resumed['started_at'] == job['started_at']
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def _malformed_traffic_variants():
    full = _traffic()
    entity = next(iter(full['by_entity']))
    no_entity_duration = json.loads(json.dumps(full))
    del no_entity_duration['by_entity'][entity]['duration_seconds']
    # A positive aggregate with an empty per-entity ledger is not settled:
    # an empty by_entity is valid only when every aggregate counter is zero.
    empty_ledger_positive = {**full, 'by_entity': {}}
    # Aggregate/entity sum mismatch: the aggregate drifts from by_entity.
    aggregate_mismatch = json.loads(json.dumps(full))
    aggregate_mismatch['requests'] += 1
    down_mismatch = json.loads(json.dumps(full))
    down_mismatch['provider_down_bytes'] -= 1
    # provider_metered_bytes must equal aggregate provider up + down.
    metered_mismatch = {**full,
                        'provider_metered_bytes': full['provider_metered_bytes'] + 1}
    # Per-entity durations must be finite and non-negative.
    inf_duration = json.loads(json.dumps(full))
    inf_duration['by_entity'][entity]['duration_seconds'] = float('inf')
    nan_duration = json.loads(json.dumps(full))
    nan_duration['by_entity'][entity]['duration_seconds'] = float('nan')
    # Entity keys must be non-empty strings.
    empty_entity_key = json.loads(json.dumps(full))
    empty_entity_key['by_entity'] = {'': empty_entity_key['by_entity'][entity]}
    return [
        None,
        {'requests': 12},
        {k: v for k, v in full.items() if k != 'by_entity'},
        {k: v for k, v in full.items() if k != 'provider_metered_bytes'},
        {**full, 'requests': -1},
        {**full, 'cache_hits': True},
        no_entity_duration,
        empty_ledger_positive,
        aggregate_mismatch,
        down_mismatch,
        metered_mismatch,
        inf_duration,
        nan_duration,
        empty_entity_key,
    ]


def test_expired_paused_with_zeroed_empty_ledger_still_rolls_over(tmp_path):
    """An empty by_entity ledger is valid when every aggregate counter is zero."""
    zeroed = {'requests': 0, 'retries': 0, 'provider_up_bytes': 0,
              'provider_down_bytes': 0, 'provider_metered_bytes': 0,
              'decoded_bytes': 0, 'cache_hits': 0, 'by_entity': {}}
    path, old = _settled_paused(tmp_path)
    old['traffic'] = zeroed
    cohorts.write_job(path, old, write)
    later = NOW + timedelta(days=1, hours=2)

    new = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                             now=later, write=write)
    assert new['status'] == 'ready'
    assert new['job_id'] != old['job_id']
    archived = cohorts.read_job(path.parent / old['job_id'] / 'expired.json')
    assert archived == old
    assert archived['traffic'] == zeroed


@pytest.mark.parametrize('job_id', [
    '../escape',
    'tm-discovery-job-../escape',
    'tm-discovery-job-' + '0' * 24,
])
def test_expired_paused_changed_or_traversal_job_id_refuses_first(tmp_path, job_id):
    """job_id must equal tm-discovery-job- + stable_payload_hash(identity)[:24]
    before any artifact is read or any archive path is touched."""
    path, job = _settled_paused(tmp_path)
    original_job_id = job['job_id']
    job['job_id'] = job_id
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='not bound to its identity'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    # No archive was written anywhere: neither under the traversal target nor
    # under the original cohort directory.
    assert not (tmp_path / 'escape' / 'expired.json').exists()
    assert not (path.parent / original_job_id / 'expired.json').exists()


@pytest.mark.parametrize('traffic', _malformed_traffic_variants())
def test_expired_paused_without_complete_traffic_refuses(tmp_path, traffic):
    """Valid artifact files on disk do not prove the accounting was settled."""
    path, job = _settled_paused(tmp_path)
    job['traffic'] = traffic
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='complete settled traffic'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_with_missing_traffic_key_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    del job['traffic']
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='complete settled traffic'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_artifact_outside_cohort_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    outside = tmp_path / 'elsewhere' / 'checkpoint.json'
    outside.parent.mkdir(parents=True)
    outside.write_text(json.dumps({'planted': True}))
    job['artifacts'] = {**job['artifacts'],
                        str(outside): hashlib.sha256(outside.read_bytes()).hexdigest()}
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='do not match its checkpoint'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_relative_artifact_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    digest = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
    # Relative artifact entries survive read_job (they are strings) but are
    # not bound to the cohort and must never be read or archived.
    job['artifacts'] = {'checkpoint.json': digest,
                        'checkpoint.failures.json': digest}
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='do not match its checkpoint'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_checkpoint_outside_cohort_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    job['checkpoint_path'] = str(tmp_path / 'other-job' / 'checkpoint.json')
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='not bound to its cohort'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_expired_paused_missing_failures_artifact_entry_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    # Only the checkpoint is listed: the .failures.json sibling is dropped.
    job['artifacts'] = {str(checkpoint): job['artifacts'][str(checkpoint)]}
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='do not match its checkpoint'):
        cohorts.select_job(path, identity(cohort_day='2026-11-06'),
                           now=NOW + timedelta(days=1), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_rollover_retry_after_active_write_crash_does_not_rewrite_archive(tmp_path):
    path, old = _settled_paused(tmp_path)
    archive = path.parent / old['job_id'] / 'expired.json'
    later = NOW + timedelta(days=1, hours=2)
    calls = []

    crashed = []

    def flaky_write(target, payload):
        calls.append(str(target))
        if target == path and archive.exists() and not crashed:
            crashed.append(True)
            raise OSError('simulated crash before active-marker write')
        write(target, payload)

    with pytest.raises(OSError, match='simulated crash'):
        cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                           now=later, write=flaky_write)
    # The archive was written but the old active marker remains untouched.
    assert cohorts.read_job(archive) == old
    assert cohorts.read_job(path)['job_id'] == old['job_id']

    calls.clear()
    new = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                             now=later, write=flaky_write)
    assert new['job_id'] != old['job_id']
    assert cohorts.read_job(path)['job_id'] == new['job_id']
    # The retry never rewrote the archive: no write call touched it.
    assert str(archive) not in calls
    assert cohorts.read_job(archive) == old


def test_conflicting_existing_archive_refuses_without_overwriting(tmp_path):
    path, old = _settled_paused(tmp_path)
    archive = path.parent / old['job_id'] / 'expired.json'
    conflicting = {**old, 'portion': old['portion'] + 1}
    cohorts.write_job(archive, conflicting, write)
    archive_before = archive.read_bytes()
    active_before = path.read_bytes()

    with pytest.raises(ValueError, match='conflicts'):
        cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(NOW + timedelta(days=1))),
                           now=NOW + timedelta(days=1, hours=2), write=write)
    assert archive.read_bytes() == archive_before
    assert path.read_bytes() == active_before


def test_corrupt_existing_archive_refuses_without_overwriting(tmp_path):
    path, old = _settled_paused(tmp_path)
    archive = path.parent / old['job_id'] / 'expired.json'
    archive.write_text('not json')
    active_before = path.read_bytes()

    with pytest.raises(ValueError, match='corrupt'):
        cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(NOW + timedelta(days=1))),
                           now=NOW + timedelta(days=1, hours=2), write=write)
    assert archive.read_text() == 'not json'
    assert path.read_bytes() == active_before
