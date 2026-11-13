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


@pytest.mark.parametrize('delay', [
    timedelta(hours=20),
    timedelta(days=1, minutes=1),
    timedelta(days=7),
])
def test_resume_later_slot_retains_grant_and_checkpoint(tmp_path, delay):
    path, job = prepared(tmp_path)
    job.update(status='paused', portion=2, traffic={'requests': 12})
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    resumed = cohorts.select_job(path, identity(), now=NOW + delay, write=write)
    assert resumed == job
    assert resumed['identity']['request_limit'] == 100
    assert path.read_bytes() == before


@pytest.mark.parametrize('change', [dict(policy_hash='changed'), dict(request_limit=101),
    dict(previous_snapshot_id='other'), dict(parser_revision='other')])
def test_resume_rejects_identity_drift(tmp_path, change):
    path, _ = prepared(tmp_path)
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(**change), now=NOW, write=write)


@pytest.mark.parametrize('delay', [timedelta(0), timedelta(days=2)])
def test_identity_drift_rejected_at_any_age(tmp_path, delay):
    path, _ = prepared(tmp_path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=101), now=NOW + delay, write=write)
    assert path.read_bytes() == before


def test_publication_ack_releases_slots_without_skipping_tomorrow(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='source_complete', final_manifest={'hash': 'manifest', 'snapshot_id': 'new'})
    cohorts.write_job(path, job, write)
    assert len(cohorts.reservations(NOW, path)) == 26
    with pytest.raises(ValueError, match='publication'):
        cohorts.mark_published(path, manifest_hash='wrong', snapshot_id='new', now=NOW, write=write)
    cohorts.mark_published(path, manifest_hash='manifest', snapshot_id='new', now=NOW, write=write)
    slots = cohorts.reservations(NOW, path)
    assert slots == ((NOW + timedelta(days=1), NOW + timedelta(days=1, minutes=57)),)
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
    # 10:45 sits in the 07:57→11:00 delivery gap of the 13-slot timetable (#2145).
    before = NOW.replace(hour=10, minute=45)
    assert timetable.available_work_seconds(before, reserve_discovery=True) == 900
    assert timetable.available_work_seconds(before) == 2700
    assert timetable.next_admitted_time(NOW.replace(hour=5), reserve_discovery=True) == NOW.replace(hour=5, minute=57)
    assert timetable.next_admitted_time(NOW.replace(hour=2), reserve_discovery=True) == NOW.replace(hour=3, minute=57)


def test_three_delivery_gaps_keep_drain_and_rollback_headroom(tmp_path):
    path, _ = prepared(tmp_path)
    windows = cohorts.reservations(NOW, path)
    # Thirteen slots: 03,04,05,06,07,11,12,13,14,15,19,21,23 UTC (#2145,
    # owner authorization 2026-11-10), preserving the four-hour delivery
    # gaps 07→11, 15→19 and 23→03.
    assert [start.hour for start, _ in windows[:13]] == [3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 19, 21, 23]
    gaps = [(b[0] - a[1]).total_seconds() for a, b in zip(windows, windows[1:])]
    # Strict delivery contract: every four-hour gap leaves 183 minutes
    # (4h minus the 57-minute run) after discovery — above the mandatory
    # 9460 s of drain 3600 s, delivery budget 5260 s and rollback 600 s
    # combined. Two days are reserved, so five of the six daily gaps appear.
    delivery_gaps = [gap for gap in gaps if gap >= 3600 + 5260 + 600]
    assert delivery_gaps == [10980] * 5
    assert all(start.hour not in (1, 2) for start, _ in windows)


def test_aged_cohort_never_renews_its_paid_grant_but_persists(tmp_path):
    path, job = prepared(tmp_path)
    before = path.read_bytes()
    later = NOW + timedelta(days=2)
    # An aged cohort is never renewed, rolled over or reset: the same grant,
    # started_at and caps persist byte-for-byte at any age.
    resumed = cohorts.select_job(path, identity(), now=later, write=write)
    assert resumed == job
    assert path.read_bytes() == before
    assert resumed['identity']['request_limit'] == 100
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] != [3, 3]


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


def test_continuation_rejects_exhausted_state_without_renewal(tmp_path):
    from scrapers.transfermarkt.models import SharedTrafficLedger

    path, job = prepared(tmp_path)
    job.update(status='exhausted')
    cohorts.write_job(path, job, write)
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    # The explicit exhausted branch precedes the generic resumable gate.
    with pytest.raises(ValueError, match='budget exhausted'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert cohorts.read_job(path)['status'] == 'exhausted'


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


def test_recovery_never_bypasses_identity_but_ignores_age(tmp_path, monkeypatch):
    path, job = stranded(tmp_path, monkeypatch)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=101), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'running'
    # Age never blocks the pinned recovery: an aged-out marker re-settles as
    # 'paused' with its accounting preserved; nothing is rolled over.
    aged = NOW + timedelta(days=3)
    resumed = cohorts.select_job(path, identity(), now=aged, write=write)
    assert resumed['status'] == 'paused'
    assert resumed['traffic'] == job['traffic']
    assert resumed['started_at'] == job['started_at']
    assert not (path.parent / job['job_id'] / 'expired.json').exists()


def test_recovery_fails_closed_on_missing_manifest(tmp_path, monkeypatch):
    path, _ = stranded(tmp_path, monkeypatch)
    (path.parent.parent / cohorts.KNOWN_SETUP_FAILURE_RECOVERY['failed_manifest']['path']).unlink()
    before = path.read_bytes()
    with pytest.raises(ValueError, match='missing or unreadable'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


# --- Settled paused markers used by resume/portion-limit tests ---

def _settled_paused(tmp_path, **changes):
    """A settled 'paused' marker with real checkpoint evidence on disk."""
    path, job = prepared(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps({'https://example.test/a': {'hash': 'x'}}))
    failures = checkpoint.with_suffix('.failures.json')
    failures.write_text(json.dumps({'https://example.test/a': {'status': 'ok'}}))
    changes.setdefault('portion', 1)
    job.update(status='paused', traffic=_traffic(), last_run_id='run-1',
               artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (checkpoint, failures)}, **changes)
    cohorts.write_job(path, job, write)
    return path, job


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


# --- One-time pinned recovery of the single audited 'failed' paid-I/O marker ---

def _paid_traffics():
    """Portion-7 (paused) and portion-8 (active) cumulative snapshots.

    One ledger grows between the snapshots: portion 8 really made paid
    requests before the raw-store commit failed closed, so every counter
    delta from paused to active is non-negative (requests strictly grow).
    """
    ledger = SharedTrafficLedger(hard_provider_bytes=1000000,
                                 soft_provider_bytes=900000, retry_limit=10)
    ledger.observe_lease('lease-1', LeaseTrafficSnapshot(up_bytes=10, down_bytes=90))
    ledger.record_attempt(entity='competition_registry', decoded_bytes=100,
                          provider_up_bytes=10, provider_down_bytes=90,
                          retry=True, duration_seconds=0.5)
    paused = ledger.snapshot()
    ledger.observe_lease('lease-2', LeaseTrafficSnapshot(up_bytes=20, down_bytes=180))
    ledger.record_attempt(entity='competition_registry', decoded_bytes=200,
                          provider_up_bytes=20, provider_down_bytes=180,
                          retry=False, duration_seconds=0.7)
    return paused, ledger.snapshot()


_PAID_PAUSED_TRAFFIC, _PAID_ACTIVE_TRAFFIC = _paid_traffics()


def _failed_paid_manifest(traffic):
    return {'status': 'failed', 'cycle_id': 'cycle-2', 'run_id': 'run-2',
            'error': {'type': 'RawStoreError',
                      'message': 'Transfermarkt raw response commit failed closed'},
            'source_status': 'failed', 'writes': [],
            'write_status': {'status': 'failed', 'authorized': True,
                             'attempted': False, 'completed': False,
                             'affected_tables': []},
            'traffic': traffic}


def _paused_portion_manifest(traffic):
    return {'status': 'paused', 'cycle_id': 'cycle-1', 'run_id': 'run-1',
            'portion': 7, 'traffic': traffic, 'source_status': 'incomplete',
            'writes': [], 'error': 'discovery portion exhausted; checkpoint retained'}


def _settled_traffic_by_entity(by_entity):
    """A settled-consistent traffic dict built from per-entity specs."""
    counters = ('requests', 'retries', 'provider_up_bytes',
                'provider_down_bytes', 'decoded_bytes', 'cache_hits')
    traffic = {field: 0 for field in counters}
    traffic['provider_metered_bytes'] = 0
    traffic['by_entity'] = {}
    for name, item in by_entity.items():
        entry = {field: 0 for field in counters}
        entry.update(item)
        entry.setdefault('duration_seconds', 0.0)
        traffic['by_entity'][name] = entry
        for field in counters:
            traffic[field] += entry[field]
    traffic['provider_metered_bytes'] = (traffic['provider_up_bytes']
                                         + traffic['provider_down_bytes'])
    return traffic


def _bump(traffic, field, amount):
    """Settled-consistent drift: the same delta on aggregate and entities."""
    drifted = json.loads(json.dumps(traffic))
    drifted[field] += amount
    for item in drifted['by_entity'].values():
        if field in item:
            item[field] += amount
    return drifted


def failed_stranded(tmp_path, monkeypatch, *, marker_traffic=None, paused=None, failed=None):
    """Audited marker: portion 8 paid I/O, then RawStoreError failed the run."""
    path, job = prepared(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps({'https://example.test/a': {'hash': 'x'}}))
    failures = checkpoint.with_suffix('.failures.json')
    failures.write_text(json.dumps({'https://example.test/a': {'status': 'ok'}}))
    active_traffic = _PAID_ACTIVE_TRAFFIC if marker_traffic is None else marker_traffic
    job.update(status='failed', portion=8, traffic=active_traffic,
               last_run_id='run-2',
               artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (checkpoint, failures)})
    cohorts.write_job(path, job, write)
    root = path.parent.parent
    paused_pin = _write_manifest(root, 'paused-7',
                                 paused if paused is not None else _paused_portion_manifest(_PAID_PAUSED_TRAFFIC))
    failed_pin = _write_manifest(root, 'failed-8',
                                 failed if failed is not None else _failed_paid_manifest(active_traffic))
    monkeypatch.setattr(cohorts, 'KNOWN_PAID_IO_FAILURE_RECOVERY', {
        'marker_hash': stable_payload_hash(job),
        'cohort_job_id': job['job_id'],
        'paused_manifest': paused_pin, 'failed_manifest': failed_pin})
    return path, job


def test_paid_io_recovery_preserves_accounting_and_resumes(tmp_path, monkeypatch):
    # The delta between the paused baseline and the marker is real paid work.
    assert (_PAID_ACTIVE_TRAFFIC['requests']
            == _PAID_PAUSED_TRAFFIC['requests'] + 1)
    path, job = failed_stranded(tmp_path, monkeypatch)
    resumed = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=2), write=write)
    assert resumed['status'] == 'paused'
    for field in ('portion', 'started_at', 'identity', 'traffic', 'artifacts',
                  'job_id', 'checkpoint_path', 'last_run_id'):
        assert resumed[field] == job[field]
    recovery = resumed['recovery']
    assert 'paid' in recovery['reason']
    assert recovery['marker_hash'] == stable_payload_hash(job)
    assert len(recovery['paused_manifest_hash']) == 64
    assert len(recovery['failed_manifest_hash']) == 64
    # The re-settled cohort is genuinely resumable: the normal continuation
    # restores the paid-inclusive cumulative traffic and proceeds to portion 9.
    ledger = SharedTrafficLedger(hard_provider_bytes=1000000,
                                 soft_provider_bytes=900000, retry_limit=10)
    continuation = cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                                 now=lambda: NOW + timedelta(hours=2),
                                                 run_id='run-3')
    assert continuation.portion == 9
    snapshot = ledger.snapshot()
    assert snapshot['requests'] == 2
    assert snapshot['provider_up_bytes'] == 30
    assert snapshot['provider_down_bytes'] == 270
    # Repeat selection is a no-op: the marker is simply 'paused' now.
    before = path.read_bytes()
    again = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=3), write=write)
    assert again['status'] == 'paused'
    assert path.read_bytes() == before


def test_unrelated_failed_marker_stays_rejected(tmp_path, monkeypatch):
    path, job = prepared(tmp_path)
    job.update(status='failed', portion=1, last_run_id='run-x')
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='unresolved traffic or writes'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_negative_aggregate_delta(tmp_path, monkeypatch):
    inflated = _bump(_PAID_PAUSED_TRAFFIC, 'requests', 2)
    path, _ = failed_stranded(tmp_path, monkeypatch, paused=_paused_portion_manifest(inflated))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='recovery traffic delta'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_negative_entity_delta(tmp_path, monkeypatch):
    # Aggregate deltas all stay non-negative, but one entity loses counted
    # work: compensation by another entity must not hide the regression.
    marker = _settled_traffic_by_entity({
        'competition_registry': {'requests': 1, 'retries': 0,
                                 'provider_up_bytes': 20, 'provider_down_bytes': 180,
                                 'decoded_bytes': 200},
        'league_registry': {'requests': 3, 'retries': 1,
                            'provider_up_bytes': 60, 'provider_down_bytes': 540,
                            'decoded_bytes': 600}})
    baseline = _settled_traffic_by_entity({
        'competition_registry': {'requests': 2, 'retries': 1,
                                 'provider_up_bytes': 30, 'provider_down_bytes': 270,
                                 'decoded_bytes': 300},
        'league_registry': {'requests': 2, 'retries': 0,
                            'provider_up_bytes': 30, 'provider_down_bytes': 270,
                            'decoded_bytes': 300}})
    path, _ = failed_stranded(tmp_path, monkeypatch, marker_traffic=marker,
                              paused=_paused_portion_manifest(baseline))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='per-entity'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_disappeared_entity(tmp_path, monkeypatch):
    # Aggregate counters match exactly, but the paused entity set is not a
    # subset of the active one: an entity must never vanish between portions.
    marker = _settled_traffic_by_entity({
        'competition_registry': {'requests': 2, 'retries': 1,
                                 'provider_up_bytes': 30, 'provider_down_bytes': 270,
                                 'decoded_bytes': 300}})
    baseline = _settled_traffic_by_entity({
        'other_registry': {'requests': 2, 'retries': 1,
                           'provider_up_bytes': 30, 'provider_down_bytes': 270,
                           'decoded_bytes': 300}})
    path, _ = failed_stranded(tmp_path, monkeypatch, marker_traffic=marker,
                              paused=_paused_portion_manifest(baseline))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='lost a paused entity'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_failed_traffic_mismatch(tmp_path, monkeypatch):
    drifted = _bump(_PAID_ACTIVE_TRAFFIC, 'requests', 1)
    path, _ = failed_stranded(tmp_path, monkeypatch, failed=_failed_paid_manifest(drifted))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='failed traffic differs from the marker'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_failed_entity_mismatch(tmp_path, monkeypatch):
    # Same aggregates, renamed entity: the failed traffic must be the
    # marker's exact per-entity ledger, not a look-alike.
    renamed = _settled_traffic_by_entity({'other_registry': {
        'requests': 2, 'retries': 1, 'provider_up_bytes': 30,
        'provider_down_bytes': 270, 'decoded_bytes': 300}})
    path, _ = failed_stranded(tmp_path, monkeypatch, failed=_failed_paid_manifest(renamed))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='entities differ from the marker'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


@pytest.mark.parametrize('kwargs', [
    {'marker_traffic': {'requests': 12}},
    {'failed': _failed_paid_manifest(None)},
    {'paused': _paused_portion_manifest(None)},
])
def test_paid_io_recovery_requires_settled_traffic_snapshots(tmp_path, monkeypatch, kwargs):
    path, _ = failed_stranded(tmp_path, monkeypatch, **kwargs)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='settled snapshot'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


@pytest.mark.parametrize('kind,manifest', [
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC),
                'error': {'type': 'RawStoreError', 'message': 'some other storage error'}}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC),
                'error': {'type': 'ValueError',
                          'message': 'Transfermarkt raw response commit failed closed'}}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC), 'status': 'paused'}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC), 'source_status': 'not_started'}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC), 'writes': [{'table': 'x', 'rows': 1}]}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC), 'write_status': {'attempted': True}}),
    ('failed', {**_failed_paid_manifest(_PAID_ACTIVE_TRAFFIC), 'run_id': 'run-1'}),
    ('paused', {**_paused_portion_manifest(_PAID_PAUSED_TRAFFIC), 'status': 'failed'}),
    ('paused', {**_paused_portion_manifest(_PAID_PAUSED_TRAFFIC), 'portion': 8}),
])
def test_paid_io_recovery_rejects_wrong_evidence_semantics(tmp_path, monkeypatch, kind, manifest):
    kwargs = {'paused' if kind == 'paused' else 'failed': manifest}
    path, _ = failed_stranded(tmp_path, monkeypatch, **kwargs)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='recovery .* evidence does not match'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_changed_artifact_without_writing(tmp_path, monkeypatch):
    path, job = failed_stranded(tmp_path, monkeypatch)
    Path(job['checkpoint_path']).write_text(json.dumps({'tampered': True}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='changed after settlement'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_rejects_tampered_manifest_without_writing(tmp_path, monkeypatch):
    path, job = failed_stranded(tmp_path, monkeypatch)
    pinned = cohorts.KNOWN_PAID_IO_FAILURE_RECOVERY['paused_manifest']
    envelope_path = path.parent.parent / pinned['path']
    envelope = json.loads(envelope_path.read_text())
    envelope['manifest']['portion'] = 99
    envelope_path.write_text(json.dumps(envelope))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='differs from the audited pin'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_fails_closed_on_missing_manifest(tmp_path, monkeypatch):
    path, _ = failed_stranded(tmp_path, monkeypatch)
    (path.parent.parent / cohorts.KNOWN_PAID_IO_FAILURE_RECOVERY['failed_manifest']['path']).unlink()
    before = path.read_bytes()
    with pytest.raises(ValueError, match='missing or unreadable'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'


def test_paid_io_recovery_never_bypasses_identity_but_ignores_age(tmp_path, monkeypatch):
    path, job = failed_stranded(tmp_path, monkeypatch)
    before = path.read_bytes()
    # Identity drift is still rejected first: a drifted caller never reaches
    # the pinned recovery, whatever the marker's age.
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=101), now=NOW, write=write)
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'failed'
    # Age never blocks the pinned recovery and never rolls the cohort over:
    # the aged failed marker re-settles as 'paused' with its accounting,
    # started_at and job_id preserved verbatim; no expired.json exists.
    aged = NOW + timedelta(days=3)
    resumed = cohorts.select_job(path, identity(), now=aged, write=write)
    assert resumed['status'] == 'paused'
    for field in ('portion', 'started_at', 'identity', 'traffic', 'artifacts',
                  'job_id', 'checkpoint_path', 'last_run_id'):
        assert resumed[field] == job[field]
    assert resumed['updated_at'] == aged.isoformat()
    assert not (path.parent / job['job_id'] / 'expired.json').exists()
    # Retry idempotency: repeat selection is a no-op on the paused marker.
    paused_bytes = path.read_bytes()
    again = cohorts.select_job(path, identity(), now=aged + timedelta(minutes=30), write=write)
    assert again['status'] == 'paused'
    assert path.read_bytes() == paused_bytes


# --- One-time pinned rollover of the owner-authorized unused 'ready' marker ---
# (#2145, owner authorization 2026-11-11): production marker job_id
# tm-discovery-job-e8e97a7722721d97cb20d271, payload hash 761daa37…; tests
# monkeypatch the pin to a tmp-path-bound variant via the unused_ready helper.

def unused_ready(tmp_path, monkeypatch):
    """The pinned unused 'ready' marker: portion 0, traffic None, no
    evidence on the marker and no checkpoint files on disk."""
    path, job = prepared(tmp_path)
    monkeypatch.setattr(cohorts, 'KNOWN_UNUSED_READY_ROLLOVER', {
        'marker_hash': stable_payload_hash(job), 'cohort_job_id': job['job_id']})
    return path, job


def test_unused_ready_rollover_before_24h_new_caps_fresh_identity(tmp_path, monkeypatch):
    path, old = unused_ready(tmp_path, monkeypatch)
    # New caps before 24h previously rejected as identity drift; the exact
    # unused pin rolls the marker over instead.
    early = NOW + timedelta(hours=2)
    new = cohorts.select_job(path, identity(request_limit=200), now=early, write=write)
    assert new['status'] == 'ready'
    assert new['portion'] == 0
    assert new['traffic'] is None
    assert new['job_id'] != old['job_id']
    assert new['started_at'] == early.isoformat()
    assert new['checkpoint_path'] != old['checkpoint_path']
    assert new['identity']['request_limit'] == 200
    # The exact old marker is archived untouched as <job_id>/unused.json.
    assert cohorts.read_job(path.parent / old['job_id'] / 'unused.json') == old
    assert cohorts.read_job(path)['job_id'] == new['job_id']
    # A repeat call resumes the fresh cohort; the archive stays exact.
    again = cohorts.select_job(path, identity(request_limit=200),
                               now=early + timedelta(minutes=30), write=write)
    assert again['job_id'] == new['job_id']
    assert cohorts.read_job(path.parent / old['job_id'] / 'unused.json') == old


def test_unused_ready_rollover_aged_ready_fresh_identity(tmp_path, monkeypatch):
    path, old = unused_ready(tmp_path, monkeypatch)
    # The unused pin rolls the marker over into a genuinely fresh cohort at
    # any age; only the pin (never age) authorizes this rollover.
    later = NOW + timedelta(days=1, hours=2)
    new = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                             now=later, write=write)
    assert new['status'] == 'ready'
    assert new['portion'] == 0
    assert new['traffic'] is None
    assert new['job_id'] != old['job_id']
    assert new['started_at'] == later.isoformat()
    assert new['checkpoint_path'] != old['checkpoint_path']
    assert cohorts.read_job(path.parent / old['job_id'] / 'unused.json') == old
    assert cohorts.read_job(path)['job_id'] == new['job_id']


def test_unused_ready_retry_idempotent_and_never_rewrites_archive(tmp_path, monkeypatch):
    path, old = unused_ready(tmp_path, monkeypatch)
    archive = path.parent / old['job_id'] / 'unused.json'
    caller = identity(request_limit=200)
    calls, crashed = [], []

    def flaky_write(target, payload):
        calls.append(str(target))
        if target == path and archive.exists() and not crashed:
            crashed.append(True)
            raise OSError('simulated crash before active-marker write')
        write(target, payload)

    with pytest.raises(OSError, match='simulated crash'):
        cohorts.select_job(path, caller, now=NOW + timedelta(hours=2), write=flaky_write)
    # Archive→active ordering: the archive landed first, the old marker is
    # still active untouched.
    assert cohorts.read_job(archive) == old
    assert cohorts.read_job(path)['job_id'] == old['job_id']
    # The retry finds the equal archive, treats it as idempotent success
    # (no rewrite) and writes the fresh active cohort.
    calls.clear()
    new = cohorts.select_job(path, caller, now=NOW + timedelta(hours=2), write=flaky_write)
    assert new['job_id'] != old['job_id']
    assert str(archive) not in calls
    assert cohorts.read_job(archive) == old
    assert cohorts.read_job(path)['job_id'] == new['job_id']


def test_unused_ready_conflicting_archive_refuses_without_overwriting(tmp_path, monkeypatch):
    path, old = unused_ready(tmp_path, monkeypatch)
    archive = path.parent / old['job_id'] / 'unused.json'
    conflicting = {**old, 'portion': 1}
    cohorts.write_job(archive, conflicting, write)
    archive_before = archive.read_bytes()
    active_before = path.read_bytes()
    with pytest.raises(ValueError, match='conflicts'):
        cohorts.select_job(path, identity(request_limit=200),
                           now=NOW + timedelta(hours=2), write=write)
    assert archive.read_bytes() == archive_before
    assert path.read_bytes() == active_before


def test_unused_ready_job_id_collision_refuses_before_writes(tmp_path, monkeypatch):
    path, job = unused_ready(tmp_path, monkeypatch)
    before = path.read_bytes()
    # The unchanged identity hashes to the pinned job_id: rollover would
    # archive and recreate a cohort in the same job_id/checkpoint namespace,
    # so select_job refuses before any write at all.
    with pytest.raises(ValueError, match='collides with the unused ready cohort'):
        cohorts.select_job(path, identity(), now=NOW + timedelta(hours=2), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'unused.json').exists()


@pytest.mark.parametrize('name', ['checkpoint.json', 'checkpoint.failures.json'])
def test_unused_ready_with_checkpoint_file_present_stays_rejected(tmp_path, monkeypatch, name):
    path, job = unused_ready(tmp_path, monkeypatch)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    target = checkpoint if name == 'checkpoint.json' else checkpoint.with_suffix('.failures.json')
    target.write_text(json.dumps({'https://example.test/a': {'hash': 'x'}}))
    before = path.read_bytes()
    # Any progress evidence on disk keeps the fail-closed behavior in
    # both windows, and nothing is archived.
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=200),
                           now=NOW + timedelta(hours=2), write=write)
    assert path.read_bytes() == before
    later = NOW + timedelta(days=1, hours=2)
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=200),
                           now=later, write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'unused.json').exists()


def test_unused_ready_off_pin_marker_continues_at_any_age(tmp_path, monkeypatch):
    path, job = prepared(tmp_path)
    # The pin names a different payload hash: this ready marker is off-pin.
    monkeypatch.setattr(cohorts, 'KNOWN_UNUSED_READY_ROLLOVER', {
        'marker_hash': '0' * 64, 'cohort_job_id': job['job_id']})
    before = path.read_bytes()
    later = NOW + timedelta(days=1, hours=2)
    resumed = cohorts.select_job(path, identity(cohort_day=cohorts.cohort_day(later)),
                                 now=later, write=write)
    # Off-pin simply continues: the cohort persists at any age and nothing
    # is archived or renewed.
    assert resumed == job
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'unused.json').exists()


def test_unused_ready_literal_production_pin():
    assert cohorts.KNOWN_UNUSED_READY_ROLLOVER == {
        'marker_hash': '761daa3756cd37f6e30fbf98d5a84b209f073fec16790c52156dbed95fb7fac7',
        'cohort_job_id': 'tm-discovery-job-e8e97a7722721d97cb20d271',
    }


@pytest.mark.parametrize('changes', [
    {'portion': 1},
    {'traffic': _traffic()},
    {'last_run_id': 'run-1'},
    {'artifacts': {'checkpoint.json': 'x'}},
    {'final_manifest': {'path': 'x'}},
    {'recovery': {'reason': 'x'}},
    {'writes': [{'table': 'x'}]},
    {'write_status': {'attempted': True}},
])
def test_unused_ready_progress_evidence_stays_rejected_even_if_pinned(tmp_path, monkeypatch, changes):
    path, job = prepared(tmp_path)
    job.update(**changes)
    cohorts.write_job(path, job, write)
    # Even a pin naming this drifted payload is refused: the unused
    # invariants (portion 0, traffic None, no write evidence) are enforced
    # beyond the hash, so any progress keeps the old rejection.
    monkeypatch.setattr(cohorts, 'KNOWN_UNUSED_READY_ROLLOVER', {
        'marker_hash': stable_payload_hash(job), 'cohort_job_id': job['job_id']})
    before = path.read_bytes()
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=200),
                           now=NOW + timedelta(hours=2), write=write)
    assert path.read_bytes() == before
    assert not (path.parent / job['job_id'] / 'unused.json').exists()


def test_unused_ready_relative_checkpoint_refused_even_with_matching_pin(tmp_path, monkeypatch):
    path, job = prepared(tmp_path)
    job['checkpoint_path'] = str(Path(job['job_id']) / 'checkpoint.json')
    monkeypatch.setattr(cohorts, 'KNOWN_UNUSED_READY_ROLLOVER', {
        'marker_hash': stable_payload_hash(job), 'cohort_job_id': job['job_id']})
    monkeypatch.chdir(tmp_path)
    relative_path = Path('active.json')
    cohorts.write_job(relative_path, job, write)
    before = relative_path.read_bytes()
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(relative_path, identity(request_limit=200),
                           now=NOW + timedelta(hours=2), write=write)
    assert relative_path.read_bytes() == before
    assert not (tmp_path / job['job_id'] / 'unused.json').exists()


# --- One-time pinned recovery of the audited AWS WAF failed cohort ---
_WAF_URL = 'https://www.transfermarkt.com/navigation/wettbewerbe'
_WAF_ENVELOPES = [
    '3ec6700925ae3eaaf8a2002ad87f4c537f42dc9c9d67e37a612593bc49a63048',
    '195ee3f40365eff0531b1a1ff8a007f9285cbb1aa098da0ba1d6cd586c0f6238',
]


def _waf_failure_document():
    return {_WAF_URL: {
        'status': 'blocked', 'status_code': 405, 'attempts': 2, 'error': 'HTTP 405',
        'raw_capture_id': '7bacc8a74379e55f903a9e029348d6b6ca6d745421e0d513f2da60aac6be1185',
        'raw_body_hash': '807f282e72f4a318018f98f6bb39a8166b996fe2817c94974c6b6bdff11a364b',
        'raw_attempt_envelope_ids': _WAF_ENVELOPES,
        'value': None,
    }}


def _failed_waf_manifest(traffic):
    manifest_traffic = json.loads(json.dumps(traffic))
    for field in ('remaining_hard_bytes', 'remaining_retries', 'retry_limit'):
        manifest_traffic.pop(field, None)
    return {
        'status': 'failed', 'run_id': 'scheduled__2026-11-11T11:00:00+00:00',
        'error': {'type': 'DiscoveryPageUnavailable',
                  'message': 'required discovery page failed: url=https://www.transfermarkt.com/navigation/wettbewerbe, status=blocked, http=405'},
        'source_status': 'failed', 'writes': [],
        'write_status': {'attempted': False, 'authorized': False, 'completed': False},
        'traffic': manifest_traffic,
    }


def waf_failed(tmp_path, monkeypatch, *, failures=None, manifest=None, traffic=None):
    path, job = prepared(tmp_path)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    failure_document = _waf_failure_document() if failures is None else failures
    failure_path = checkpoint.with_suffix('.failures.json')
    failure_path.write_text(json.dumps(failure_document))
    active_traffic = _PAID_ACTIVE_TRAFFIC if traffic is None else traffic
    job.update(status='failed', portion=1,
               last_run_id='scheduled__2026-11-11T11:00:00+00:00',
               traffic=active_traffic,
               artifacts={str(failure_path): hashlib.sha256(failure_path.read_bytes()).hexdigest()})
    cohorts.write_job(path, job, write)
    failed_pin = _write_manifest(path.parent.parent, 'waf-failed',
        manifest if manifest is not None else _failed_waf_manifest(active_traffic))
    monkeypatch.setattr(cohorts, 'KNOWN_WAF_FAILURE_RECOVERY', {
        'marker_hash': stable_payload_hash(job),
        'cohort_job_id': job['job_id'],
        'failure_sha256': hashlib.sha256(failure_path.read_bytes()).hexdigest(),
        'failed_manifest': failed_pin,
        'url': _WAF_URL,
        'raw_capture_id': '7bacc8a74379e55f903a9e029348d6b6ca6d745421e0d513f2da60aac6be1185',
        'raw_body_hash': '807f282e72f4a318018f98f6bb39a8166b996fe2817c94974c6b6bdff11a364b',
        'raw_attempt_envelope_ids': _WAF_ENVELOPES,
    })
    return path, job, failure_document


def test_waf_recovery_quarantines_failure_and_resumes_with_traffic(tmp_path, monkeypatch):
    path, job, original_failures = waf_failed(tmp_path, monkeypatch)
    resumed = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=2), write=write)
    assert resumed['status'] == 'paused'
    for field in ('portion', 'started_at', 'identity', 'traffic',
                  'job_id', 'checkpoint_path', 'last_run_id'):
        assert resumed[field] == job[field]
    checkpoint = Path(job['checkpoint_path'])
    failures = checkpoint.with_suffix('.failures.json')
    quarantine = checkpoint.with_suffix('.failures.blocked.json')
    assert json.loads(failures.read_text()) == {}
    assert resumed['artifacts'] == {str(failures): hashlib.sha256(failures.read_bytes()).hexdigest()}
    retained = json.loads(quarantine.read_text())
    assert retained['failed_marker'] == job
    assert retained['failures'] == original_failures
    assert retained['original_marker_hash'] == stable_payload_hash(job)
    assert retained['original_failures_sha256'] == job['artifacts'][str(failures)]
    recovery = resumed['recovery']
    assert recovery['quarantine_path'] == str(quarantine)
    assert recovery['quarantine_hash'] == hashlib.sha256(quarantine.read_bytes()).hexdigest()
    assert recovery['raw_attempt_envelope_ids'] == _WAF_ENVELOPES
    ledger = SharedTrafficLedger(hard_provider_bytes=1000000,
                                 soft_provider_bytes=900000, retry_limit=10)
    continuation = cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                                 now=lambda: NOW + timedelta(hours=2),
                                                 run_id='next-run')
    assert continuation.portion == 2
    assert ledger.snapshot()['requests'] == 2
    assert ledger.snapshot()['retries'] == 1
    before = path.read_bytes()
    again = cohorts.select_job(path, identity(), now=NOW + timedelta(hours=3), write=write)
    assert again['status'] == 'paused'
    assert path.read_bytes() == before


def test_waf_recovery_completes_after_quarantine_only_crash(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    checkpoint = Path(job['checkpoint_path'])
    quarantine = checkpoint.with_suffix('.failures.blocked.json')
    failures = checkpoint.with_suffix('.failures.json')
    original = failures.read_bytes()
    def interrupted(target, payload):
        if target == failures:
            raise RuntimeError('crash after quarantine')
        write(target, payload)
    with pytest.raises(RuntimeError, match='after quarantine'):
        cohorts.select_job(path, identity(), now=NOW, write=interrupted)
    assert quarantine.exists() and failures.read_bytes() == original
    resumed = cohorts.select_job(path, identity(), now=NOW, write=write)
    assert resumed['status'] == 'paused'
    assert json.loads(failures.read_text()) == {}


def test_waf_recovery_completes_after_empty_failures_crash(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    failures = Path(job['checkpoint_path']).with_suffix('.failures.json')
    def interrupted(target, payload):
        if target == path:
            raise RuntimeError('crash before marker')
        write(target, payload)
    with pytest.raises(RuntimeError, match='before marker'):
        cohorts.select_job(path, identity(), now=NOW, write=interrupted)
    assert cohorts.read_job(path)['status'] == 'failed'
    assert json.loads(failures.read_text()) == {}
    resumed = cohorts.select_job(path, identity(), now=NOW, write=write)
    assert resumed['status'] == 'paused'


@pytest.mark.parametrize('contents', ['{broken', '{"conflict": true}'])
def test_waf_recovery_refuses_invalid_quarantine_without_overwrite(tmp_path, monkeypatch, contents):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    quarantine = Path(job['checkpoint_path']).with_suffix('.failures.blocked.json')
    quarantine.write_text(contents)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='quarantine'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert quarantine.read_text() == contents
    assert cohorts.read_job(path)['status'] == 'failed'


def test_waf_recovery_refuses_empty_failures_without_quarantine(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    failures = Path(job['checkpoint_path']).with_suffix('.failures.json')
    failures.write_text('{}')
    quarantine = failures.with_suffix('.blocked.json')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='quarantine'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert not quarantine.exists()


def test_waf_recovery_rejects_checkpoint_and_changed_artifact(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    checkpoint = Path(job['checkpoint_path'])
    checkpoint.write_text('{}')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='checkpoint'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert not checkpoint.with_suffix('.failures.blocked.json').exists()


def test_waf_recovery_rejects_manifest_traffic_mismatch(tmp_path, monkeypatch):
    drifted = _bump(_PAID_ACTIVE_TRAFFIC, 'requests', 1)
    path, _, _ = waf_failed(tmp_path, monkeypatch, manifest=_failed_waf_manifest(drifted))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='traffic'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


@pytest.mark.parametrize('changes', [
    {'status': 'paused'},
    {'source_status': 'not_started'},
    {'writes': [{'table': 'x', 'rows': 1}]},
    {'write_status': {'attempted': True, 'authorized': False, 'completed': False}},
    {'error': {'type': 'RawStoreError', 'message': 'other'}},
])
def test_waf_recovery_rejects_wrong_manifest_semantics(tmp_path, monkeypatch, changes):
    manifest = {**_failed_waf_manifest(_PAID_ACTIVE_TRAFFIC), **changes}
    path, _, _ = waf_failed(tmp_path, monkeypatch, manifest=manifest)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='evidence'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


def test_waf_recovery_rejects_changed_failure_evidence(tmp_path, monkeypatch):
    changed = _waf_failure_document()
    changed[_WAF_URL]['raw_capture_id'] = '0' * 64
    path, _, _ = waf_failed(tmp_path, monkeypatch, failures=changed)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='failure evidence'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


def test_waf_recovery_never_bypasses_identity_or_pin_but_ignores_age(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='drift'):
        cohorts.select_job(path, identity(request_limit=101), now=NOW, write=write)
    assert path.read_bytes() == before
    # Age never blocks the pinned WAF recovery: the aged failed marker still
    # quarantines its cached blocked seed and re-settles as 'paused'.
    resumed = cohorts.select_job(path, identity(), now=NOW + timedelta(days=2), write=write)
    assert resumed['status'] == 'paused'
    assert resumed['started_at'] == job['started_at']
    assert resumed['traffic'] == job['traffic']
    job['portion'] = 2
    cohorts.write_job(path, job, write)
    off_pin = path.read_bytes()
    with pytest.raises(ValueError, match='unresolved traffic or writes'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == off_pin


def test_waf_recovery_literal_production_pin():
    pin = cohorts.KNOWN_WAF_FAILURE_RECOVERY
    assert pin['cohort_job_id'] == 'tm-discovery-job-75915fb274a0ddc406940e83'
    assert pin['marker_hash'] == 'a716819d35272452911d20c2ad10c11b8999e0da53bdabff33346ddac74797f1'
    assert pin['failure_sha256'] == '5853ce0dfa67b0f0666af232de6731442654a8c7c10c83ebdafc6b3d1ad41d4a'
    assert pin['failed_manifest']['hash'] == 'f32dcea613f131574f3191853d3a23ffcc601185e5ec93254decba5e15bab763'
    assert pin['url'] == _WAF_URL
    assert pin['raw_attempt_envelope_ids'] == _WAF_ENVELOPES



def test_waf_recovery_rejects_changed_artifact_without_writing(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    failures = Path(job['checkpoint_path']).with_suffix('.failures.json')
    failures.write_text('{"changed": true}')
    quarantine = failures.with_suffix('.blocked.json')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='active failures'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert not quarantine.exists()


def test_waf_recovery_rejects_tampered_manifest_without_writing(tmp_path, monkeypatch):
    path, job, _ = waf_failed(tmp_path, monkeypatch)
    pinned = cohorts.KNOWN_WAF_FAILURE_RECOVERY['failed_manifest']
    manifest_path = path.parent.parent / pinned['path']
    envelope = json.loads(manifest_path.read_text())
    envelope['manifest']['run_id'] = 'different-run'
    manifest_path.write_text(json.dumps(envelope))
    quarantine = Path(job['checkpoint_path']).with_suffix('.failures.blocked.json')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='differs from the audited pin'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before
    assert not quarantine.exists()


@pytest.mark.parametrize('scope', ['aggregate', 'entity'])
def test_waf_recovery_rejects_duration_drift(tmp_path, monkeypatch, scope):
    manifest = _failed_waf_manifest(_PAID_ACTIVE_TRAFFIC)
    if scope == 'aggregate':
        manifest['traffic']['duration_seconds'] += 1
    else:
        manifest['traffic']['by_entity']['competition_registry']['duration_seconds'] += 1
    path, _, _ = waf_failed(tmp_path, monkeypatch, manifest=manifest)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='traffic differs'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


@pytest.mark.parametrize('location', ['active', 'failed'])
@pytest.mark.parametrize('value', [None, -1, float('inf'), float('nan')])
def test_waf_recovery_rejects_invalid_aggregate_duration(tmp_path, monkeypatch, location, value):
    traffic = json.loads(json.dumps(_PAID_ACTIVE_TRAFFIC))
    traffic['duration_seconds'] = value
    kwargs = {'traffic': traffic} if location == 'active' else {
        'manifest': _failed_waf_manifest(traffic)}
    path, _, _ = waf_failed(tmp_path, monkeypatch, **kwargs)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='aggregate duration is invalid'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


# --- Persistent cohort: started-time sanity, portion limit, resume guards ---

def test_max_cohort_portions_pin():
    # 104 saved portions of progress bound the persistent cohort; the count
    # promises nothing about any calendar duration.
    assert cohorts.MAX_COHORT_PORTIONS == 104 == 13 * 8


@pytest.mark.parametrize('status', ['ready', 'paused', 'source_complete', 'failed'])
def test_future_start_time_fails_closed_on_any_status(tmp_path, status):
    path, job = _settled_paused(tmp_path)
    job.update(status=status, started_at=(NOW + timedelta(hours=2)).isoformat())
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='invalid or future start time'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    assert path.read_bytes() == before


def test_start_time_within_clock_skew_is_accepted(tmp_path):
    path, job = _settled_paused(tmp_path, started_at=(NOW + timedelta(minutes=30)).isoformat())
    before = path.read_bytes()
    resumed = cohorts.select_job(path, identity(), now=NOW, write=write)
    assert resumed == job
    assert path.read_bytes() == before


def test_aged_marker_is_never_migrated_or_rewritten(tmp_path):
    path, job = _settled_paused(tmp_path)
    before = path.read_bytes()
    later = NOW + timedelta(days=2)
    resumed = cohorts.select_job(path, identity(), now=later, write=write)
    assert resumed == job
    assert path.read_bytes() == before
    assert set(resumed) == set(job)


def test_stale_expired_json_archive_is_inert(tmp_path):
    path, job = _settled_paused(tmp_path)
    stale = path.parent / job['job_id'] / 'expired.json'
    stale.parent.mkdir(parents=True, exist_ok=True)
    cohorts.write_job(stale, job, write)
    before = path.read_bytes()
    later = NOW + timedelta(days=2)
    resumed = cohorts.select_job(path, identity(), now=later, write=write)
    assert resumed == job
    assert path.read_bytes() == before


def test_reservations_follow_updated_freshness_not_started_age(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='paused', portion=1, traffic=_traffic(),
               started_at=(NOW - timedelta(days=5)).isoformat())
    cohorts.write_job(path, job, write)
    # A five-day-old cohort that a slot just refreshed still reserves both
    # days: every bounded portion refreshes updated_at, age never releases.
    assert len(cohorts.reservations(NOW, path)) == 26


def test_reservations_stale_marker_keeps_only_daily_bootstrap(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='paused', portion=1,
               updated_at=(NOW - timedelta(hours=25)).isoformat())
    cohorts.write_job(path, job, write)
    # A marker no slot refreshed for over a day releases everything except
    # the guaranteed daily bootstrap slot, which can resume it.
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] == [3, 3]


@pytest.mark.parametrize('traffic', _malformed_traffic_variants())
def test_continuation_resume_without_complete_traffic_refuses(tmp_path, traffic):
    """Valid artifact files on disk do not prove the accounting was settled."""
    path, job = _settled_paused(tmp_path)
    job['traffic'] = traffic
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not a complete settled snapshot'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'paused'


def test_continuation_resume_with_missing_traffic_key_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    del job['traffic']
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not a complete settled snapshot'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before


def test_continuation_construction_is_idempotent_noop(tmp_path):
    path, job = _settled_paused(tmp_path)
    before = path.read_bytes()
    for _ in range(2):
        ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                     soft_provider_bytes=9000, retry_limit=10)
        continuation = cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                                     now=lambda: NOW, run_id='run-2')
        assert continuation.portion == 2
        assert ledger.snapshot()['requests'] == 1
    assert path.read_bytes() == before


def test_exhausted_marker_is_terminal_everywhere(tmp_path):
    path, job = _settled_paused(tmp_path)
    job.update(status='exhausted')
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    with pytest.raises(ValueError, match='no budget renewal'):
        cohorts.select_job(path, identity(), now=NOW, write=write)
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='no budget renewal'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    with pytest.raises(ValueError, match='publication'):
        cohorts.mark_published(path, manifest_hash='x', snapshot_id='new',
                               now=NOW, write=write)
    assert path.read_bytes() == before
    # Terminal exhausted releases every reservation except the daily bootstrap.
    assert [start.hour for start, _ in cohorts.reservations(NOW, path)] == [3, 3]


def test_continuation_portion_overflow_settles_terminal_exhausted(tmp_path):
    path, job = _settled_paused(tmp_path, portion=cohorts.MAX_COHORT_PORTIONS)
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='portion limit exhausted'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    settled = cohorts.read_job(path)
    # Terminal settle with no renewal: portion, traffic, artifacts and
    # started_at verbatim; only the status and update time changed.
    assert settled['status'] == 'exhausted'
    assert settled['portion'] == cohorts.MAX_COHORT_PORTIONS
    assert settled['traffic'] == job['traffic']
    assert settled['artifacts'] == job['artifacts']
    assert settled['started_at'] == job['started_at']
    with pytest.raises(ValueError, match='no budget renewal'):
        cohorts.select_job(path, identity(), now=NOW, write=write)


def test_continuation_portion_overflow_with_damaged_evidence_stays_paused(tmp_path):
    path, job = _settled_paused(tmp_path, portion=cohorts.MAX_COHORT_PORTIONS)
    Path(job['checkpoint_path']).write_text(json.dumps({'tampered': True}))
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    # No terminal settle on damaged evidence: the marker stays paused,
    # byte-identical, for operator forensics.
    with pytest.raises(ValueError, match='changed after settlement'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'paused'


def test_continuation_portion_overflow_without_settled_traffic_stays_paused(tmp_path):
    path, job = _settled_paused(tmp_path, portion=cohorts.MAX_COHORT_PORTIONS)
    job['traffic'] = {'requests': 12}
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not a complete settled snapshot'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before
    assert cohorts.read_job(path)['status'] == 'paused'


@pytest.mark.parametrize('job_id', [
    '../escape',
    'tm-discovery-job-../escape',
    'tm-discovery-job-' + '0' * 24,
])
def test_continuation_resume_changed_or_traversal_job_id_refuses(tmp_path, job_id):
    """job_id must equal tm-discovery-job- + stable_payload_hash(identity)[:24]
    before any artifact is read."""
    path, job = _settled_paused(tmp_path)
    job['job_id'] = job_id
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not bound to its identity'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before


def test_continuation_resume_checkpoint_outside_cohort_refuses(tmp_path):
    path, job = _settled_paused(tmp_path)
    job['checkpoint_path'] = str(tmp_path / 'other-job' / 'checkpoint.json')
    cohorts.write_job(path, job, write)
    before = path.read_bytes()
    ledger = SharedTrafficLedger(hard_provider_bytes=10000,
                                 soft_provider_bytes=9000, retry_limit=10)
    with pytest.raises(ValueError, match='not bound to its cohort'):
        cohorts.DiscoveryContinuation(path, identity(), ledger, write,
                                      now=lambda: NOW, run_id='run-2')
    assert path.read_bytes() == before


def test_published_cohort_starts_fresh_cohort_next_operational_day(tmp_path):
    path, job = prepared(tmp_path)
    job.update(status='published', final_manifest={'hash': 'manifest', 'snapshot_id': 'new'},
               published_at=NOW.isoformat())
    cohorts.write_job(path, job, write)
    later = NOW + timedelta(days=2)
    fresh = cohorts.select_job(path, identity(previous_snapshot_id='new',
                                              cohort_day=cohorts.cohort_day(later)),
                               now=later, write=write)
    assert fresh['status'] == 'ready'
    assert fresh['portion'] == 0
    assert fresh['traffic'] is None
    assert fresh['job_id'] != job['job_id']
    assert fresh['started_at'] == later.isoformat()
    assert cohorts.read_job(path.parent / job['job_id'] / 'completed.json') == job
