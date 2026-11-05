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
