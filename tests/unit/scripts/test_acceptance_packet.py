from __future__ import annotations

import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scripts import acceptance_packet as packet


NOW = '2026-10-16T12:00:00+00:00'
PAST = '2026-10-15T12:00:00+00:00'
FUTURE = '2026-10-17T12:00:00+00:00'
OWNER = '01a134fb-4197-7690-83fb-2d89fb84793a'
OTHER = '01a14091-f921-7522-b131-69d772ba9241'
WATCH = 'acceptance-duty'


def row(key='task-1', **changes):
    value = {
        'id': key, 'revision': 3, 'series': 1, 'source': 'fotmob',
        'status': 'observing', 'started_at': PAST, 'definition_confirmed': True,
        'impact_hold': False, 'close_hold': False, 'monitoring_stale': False,
        'next_check_at': PAST, 'criteria': [], 'days': [],
        'monitoring': {'owner_thread_id': OWNER, 'watch_key': WATCH,
                       'bound_series': 1, 'next_check_at': FUTURE,
                       'evidence_plan': [{'criterion': 'data', 'producer': 'source',
                                          'path': '/never/open/this.json'}]},
    }
    value.update(changes)
    return value


def snapshot(*rows):
    return {'schema': 1, 'revision': 10, 'generated_at': NOW, 'sources': {},
            'acceptances': list(rows), 'handoffs': {
                'fotmob': {'revision': 9, 'checked_at': PAST, 'path': '/handoff.md',
                           'summary': 'MUST NOT COPY', 'history': ['large'] * 500}}}


def build(value, **kwargs):
    return packet.build_packet(value, now=NOW, owner_thread_id=OWNER, watch_key=WATCH,
                               **kwargs)


def invoke(tmp_path, value, capsys, *extra, raw=None, dirname='out'):
    source = tmp_path / 'source.json'
    source.write_bytes(raw if raw is not None else json.dumps(value).encode())
    result = packet.main(['--snapshot', str(source), '--output-dir', str(tmp_path / dirname),
                          '--owner-thread', OWNER, '--watch-key', WATCH, '--now', NOW,
                          *extra])
    return result, capsys.readouterr(), source


def test_all_open_rows_are_indexed_and_closed_rows_are_counted():
    result = build(snapshot(row(), row('task-2', status='closed')))
    assert result['counts'] == {'total': 2, 'open': 1, 'closed': 1,
                                'candidates': 0, 'attention': 0}
    assert [item['id'] for item in result['index']] == ['task-1']
    item = result['index'][0]
    assert item['next_check_at'] == PAST
    assert item['monitoring']['next_check_at'] == FUTURE
    assert item['phase'] == 'started'
    assert set(result['handoffs'][0]) == {'source', 'revision', 'checked_at', 'path'}
    assert 'MUST NOT COPY' not in json.dumps(result)
    assert 'evidence_plan' not in json.dumps(result)


def test_future_operational_deadline_defers_old_derived_deadline():
    result = build(snapshot(row()), reconciliation_due_at=FUTURE,
                   owner_status_due_at=FUTURE)
    assert result['candidates'] == []
    assert result['next_check_at'] == FUTURE
    assert result['work_state'] == 'waiting'


@pytest.mark.parametrize('deadline', [PAST, NOW, '2026-10-16T15:00:00+03:00'])
def test_operational_due_includes_exact_boundary(deadline):
    value = row()
    value['monitoring']['next_check_at'] = deadline
    result = build(snapshot(value))
    assert result['candidates'][0]['reasons'] == ['operational_due']
    assert result['candidates'][0]['action'] == 'check_evidence'


@pytest.mark.parametrize('field', ['impact_hold', 'close_hold'])
def test_holds_never_remove_due_observation(field):
    value = row(**{field: True}, status='on_hold')
    value['monitoring']['next_check_at'] = NOW
    result = build(snapshot(value))
    assert result['index'][0][field] is True
    assert result['candidates'][0]['action'] == 'check_evidence'


@pytest.mark.parametrize('started_at', [None, FUTURE])
def test_unstarted_window_selects_readiness_only(started_at):
    value = row(started_at=started_at, status='waiting')
    value['monitoring']['next_check_at'] = NOW
    result = build(snapshot(value))
    assert result['index'][0]['phase'] == 'waiting'
    assert result['candidates'][0]['action'] == 'check_readiness'
    assert 'start' not in result['candidates'][0]


def test_unstarted_future_operational_window_waits():
    assert build(snapshot(row(started_at=None)))['candidates'] == []


@pytest.mark.parametrize('change,reason', [
    ({'monitoring': None}, 'missing_monitoring'),
    ({'definition_confirmed': False}, 'unconfirmed_definition'),
    ({'monitoring_stale': True}, 'stale_binding'),
])
def test_missing_unconfirmed_and_stale_are_explicit(change, reason):
    result = build(snapshot(row(**change)))
    assert reason in result['candidates'][0]['reasons']
    assert reason in result['attention'][0]['reasons']
    assert result['candidates'][0]['action'] == 'lead_check'


def test_bound_series_mismatch_detected_even_with_false_snapshot_flag():
    value = row()
    value['monitoring']['bound_series'] = 2
    result = build(snapshot(value))
    assert 'stale_binding' in result['candidates'][0]['reasons']
    assert result['index'][0]['monitoring']['bound_series'] == 2


@pytest.mark.parametrize('deadline', [None, '', 'not-a-date', '2026-10-17T12:00:00'])
def test_invalid_operational_deadline_requires_lead(deadline):
    value = row()
    value['monitoring']['next_check_at'] = deadline
    result = build(snapshot(value))
    assert 'invalid_operational_deadline' in result['candidates'][0]['reasons']
    assert result['needs_lead_check'] is True


def test_absent_operational_deadline_is_not_hidden_by_derived_date():
    value = row()
    del value['monitoring']['next_check_at']
    assert 'missing_operational_deadline' in build(snapshot(value))['candidates'][0]['reasons']


@pytest.mark.parametrize('field,value', [('owner_thread_id', OTHER), ('watch_key', 'other-watch')])
def test_foreign_assignments_are_visible_and_not_own_measurements(field, value):
    item = row()
    item['monitoring'].update({field: value, 'next_check_at': NOW})
    result = build(snapshot(item))
    assert result['index'][0]['assigned_to_owner'] is False
    assert result['candidates'][0]['action'] == 'review_foreign_assignment'
    assert result['candidates'][0]['reasons'] == ['foreign_assignment']
    assert result['attention'][0]['reasons'] == ['foreign_assignment']


def test_unknown_global_deadlines_never_claim_no_work():
    result = build(snapshot())
    assert result['work_state'] == 'needs_lead_check'
    assert result['needs_lead_check'] is True
    assert result['global_checks'] == {
        'reconciliation': {'status': 'unknown', 'due_at': None},
        'owner_status': {'status': 'unknown', 'due_at': None}}


def test_global_due_and_next_deadline_are_separate():
    result = build(snapshot(row()), reconciliation_due_at=NOW,
                   owner_status_due_at=FUTURE)
    assert result['global_checks']['reconciliation']['status'] == 'due'
    assert result['global_checks']['owner_status']['status'] == 'scheduled'
    assert result['next_check_at'] == NOW
    assert result['work_state'] == 'due'


def test_datetime_now_accepted_and_input_is_unchanged():
    value = snapshot(row())
    before = copy.deepcopy(value)
    result = packet.build_packet(value, now=datetime.fromisoformat(NOW),
                                 owner_thread_id=OWNER, watch_key=WATCH)
    result['index'][0]['monitoring']['owner_thread_id'] = 'changed'
    assert value == before


@pytest.mark.parametrize('mutator', [
    lambda s: s.pop('acceptances'), lambda s: s.pop('handoffs'),
    lambda s: s.pop('revision'), lambda s: s.pop('generated_at'),
    lambda s: s.update(schema=2), lambda s: s.update(schema=True),
    lambda s: s.update(revision=-1), lambda s: s.update(acceptances={}),
    lambda s: s['acceptances'].append(copy.deepcopy(s['acceptances'][0])),
    lambda s: s['acceptances'][0].pop('monitoring_stale'),
    lambda s: s['acceptances'][0].update(series=True),
    lambda s: s['acceptances'][0].update(revision=11),
    lambda s: s['acceptances'][0].update(status='not-a-status'),
    lambda s: s['acceptances'][0].update(started_at='bad-date'),
    lambda s: s['acceptances'][0].update(next_check_at='bad-date'),
    lambda s: s['handoffs']['fotmob'].update(checked_at='bad-date'),
    lambda s: s.update(generated_at='2026-10-16T12:00:00'),
])
def test_malformed_incomplete_input_fails_closed(mutator):
    value = snapshot(row())
    mutator(value)
    with pytest.raises(packet.PacketError):
        build(value)


def test_raw_snapshot_is_preserved_and_packet_references_hash(tmp_path, capsys):
    raw = ('  ' + json.dumps(snapshot(row()), ensure_ascii=False, indent=3) + '\n').encode()
    rc, captured, source = invoke(tmp_path, None, capsys, raw=raw)
    assert rc == 0, captured.err
    result = json.loads(captured.out)
    artifact = result['raw_snapshot']
    assert Path(artifact['path']).read_bytes() == raw
    assert artifact['sha256'] == hashlib.sha256(raw).hexdigest()
    assert artifact['bytes'] == len(raw)
    assert source.read_bytes() == raw
    assert json.loads((tmp_path / 'out/packet.json').read_bytes()) == result
    assert (tmp_path / 'out/snapshot.json').stat().st_mode & 0o222 == 0


def test_budget_pointer_keeps_full_packet_and_all_candidates(tmp_path, capsys):
    value = snapshot(*(row(f'task-{i}', monitoring=None) for i in range(100)))
    rc, captured, _ = invoke(tmp_path, value, capsys, '--max-output-bytes', '512')
    assert rc == 0, captured.err
    assert len(captured.out.encode()) <= 512
    pointer = json.loads(captured.out)
    assert pointer['requires_full_read'] is True
    assert pointer['candidate_count'] == 100
    raw = Path(pointer['path']).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == pointer['sha256']
    assert len(raw) == pointer['bytes']
    assert len(json.loads(raw)['candidates']) == 100


def test_too_small_budget_is_an_explicit_error_without_artifacts(tmp_path, capsys):
    rc, captured, _ = invoke(tmp_path, snapshot(row()), capsys, '--max-output-bytes', '1')
    assert rc != 0
    assert captured.out == ''
    assert 'budget' in captured.err
    assert not (tmp_path / 'out').exists()


@pytest.mark.parametrize('raw', [b'{bad json', b'{"schema":1,"schema":1}', b'{"schema":NaN}', b'[]'])
def test_bad_json_never_creates_success_packet(tmp_path, capsys, raw):
    rc, captured, _ = invoke(tmp_path, None, capsys, raw=raw)
    assert rc != 0
    assert captured.out == ''
    assert not (tmp_path / 'out').exists()


def test_existing_output_directory_is_never_overwritten(tmp_path, capsys):
    out = tmp_path / 'out'
    out.mkdir()
    marker = out / 'packet.json'
    marker.write_text('existing artifact')
    rc, captured, _ = invoke(tmp_path, snapshot(row()), capsys)
    assert rc != 0
    assert captured.out == ''
    assert marker.read_text() == 'existing artifact'


@pytest.mark.parametrize('ancestor', [False, True])
def test_output_symlinks_are_rejected(tmp_path, capsys, ancestor):
    target = tmp_path / 'target'
    target.mkdir()
    (tmp_path / 'alias').symlink_to(target, target_is_directory=True)
    dirname = 'alias/out' if ancestor else 'alias'
    rc, captured, _ = invoke(tmp_path, snapshot(row()), capsys, dirname=dirname)
    assert rc != 0
    assert captured.out == ''
    assert list(target.iterdir()) == []


def test_snapshot_data_never_executes_commands_or_opens_evidence(tmp_path, capsys, monkeypatch):
    import socket
    import sqlite3
    import subprocess

    def forbidden(*args, **kwargs):
        raise AssertionError('network/database/process access is forbidden')

    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    value = snapshot(row())
    marker = tmp_path / 'should-not-exist'
    value['acceptances'][0]['monitoring']['evidence_plan'][0]['producer'] = f'touch {marker}'
    value['handoffs']['fotmob']['summary'] = f'$(touch {marker})'
    rc, captured, _ = invoke(tmp_path, value, capsys)
    assert rc == 0, captured.err
    assert not marker.exists()
