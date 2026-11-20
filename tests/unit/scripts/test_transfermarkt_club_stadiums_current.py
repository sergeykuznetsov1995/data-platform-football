from __future__ import annotations

import pytest

from dags.scripts import run_transfermarkt_current as current
from tests.unit.scripts.test_run_transfermarkt_current import Offline
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row

pytestmark = pytest.mark.unit


@pytest.fixture
def offline(tmp_path, monkeypatch):
    return Offline(tmp_path, monkeypatch)


def writer_for(offline, *, fail=None, stop=None):
    writes = []

    def writer(scraper, scope, preflight, cycle_id, *, club_id, profile_outcome,
               stadium_outcome, **kwargs):
        if club_id == fail:
            raise RuntimeError('read-back failed')
        writes.append(club_id)
        if club_id == stop:
            offline.clock.advance(2350)
        return {
            **offline.proof(), 'club_id': club_id, 'receipt_id': 'receipt-' + club_id,
            'profile_raw_capture_id': profile_outcome.raw_capture_id,
            'profile_raw_body_hash': profile_outcome.raw_body_hash,
            'stadium_raw_capture_id': stadium_outcome.raw_capture_id,
            'stadium_raw_body_hash': stadium_outcome.raw_body_hash,
            'stadium_status': ('observed_empty' if stadium_outcome.status_code == 404
                               else 'observed_present'),
        }

    return writer, writes


def data(offline):
    return next(iter(offline.state()['scopes'].values()))


def stadium_calls(offline):
    return [url for _, url in offline.calls
            if url.startswith('https://tmapi.transfermarkt.technology/club/')]


def test_monthly_capture_and_fresh_cursor_noop(offline):
    writer, writes = writer_for(offline)
    offline.run_cold('cold', stadium_writer=writer)
    assert writes == ['10', '20'], data(offline).get('stadium_sweep')
    assert len(stadium_calls(offline)) == 4
    saved = data(offline)
    assert saved['cursor']['stadium_completed_at']
    assert saved['stadium_last_sweep']['completed'] == {
        '10': {'receipt_id': 'receipt-10', 'status': 'observed_present'},
        '20': {'receipt_id': 'receipt-20', 'status': 'observed_present'},
    }
    offline.calls.clear()
    offline.run('fresh', stadium_writer=writer)
    assert stadium_calls(offline) == []
    assert writes == ['10', '20']
    offline.clock.advance(30 * 86400)
    offline.run('next-month', stadium_writer=writer)
    assert writes == ['10', '20', '10', '20']


def test_confirmed_404_is_a_conclusive_empty_receipt(offline):
    offline.stadium_status = {'10': 404}
    writer, writes = writer_for(offline)
    offline.run_cold('empty', stadium_writer=writer)
    assert writes == ['10', '20']
    completed = data(offline)['stadium_last_sweep']['completed']
    assert completed['10']['status'] == 'observed_empty'
    assert completed['20']['status'] == 'observed_present'


@pytest.mark.parametrize('failure', ['write', 'proof'])
def test_partial_sweep_resumes_only_pending(offline, failure):
    good, writes = writer_for(offline, fail='10' if failure == 'write' else None)
    if failure == 'proof':
        def bad(*args, **kwargs):
            proof = good(*args, **kwargs)
            if kwargs['club_id'] == '10':
                proof['verified'] = False
            return proof
        writer = bad
    else:
        writer = good
    offline.run_cold('partial', stadium_writer=writer)
    saved = data(offline)
    assert saved['stadium_sweep']['pending'] == ['10']
    assert saved['stadium_sweep']['deferred']['10']
    assert saved['cursor']['stadium_pending']
    assert saved['cursor']['stadium_completed_at'] is None
    resumed, resumed_writes = writer_for(offline)
    offline.run('resume', stadium_writer=resumed)
    assert resumed_writes == ['10']
    assert 'stadium_sweep' not in data(offline)
    assert data(offline)['cursor']['stadium_completed_at']


def test_deadline_retains_remainder(offline):
    writer, writes = writer_for(offline, stop='10')
    offline.run('deadline', stadium_writer=writer)
    assert writes == ['10']
    assert data(offline)['stadium_sweep']['pending'] == ['20']
    resumed, resumed_writes = writer_for(offline)
    offline.run('resume', stadium_writer=resumed)
    assert resumed_writes == ['20']


def test_old_cursor_migrates_additive_monthly_fields():
    cursor = current.ScopeCursor('scope', generation='old').as_dict()
    cursor.pop('stadium_completed_at')
    cursor.pop('stadium_pending')
    migrated = current.ScopeCursor.from_mapping(cursor)
    assert migrated.stadium_completed_at is None
    assert migrated.stadium_pending is False


def test_season_close_preserves_unfinished_sweep(offline):
    writer, _ = writer_for(offline, fail='10')
    offline.run('partial', stadium_writer=writer)
    pending = data(offline)['stadium_sweep']
    rows = [_joined_row(_competition('GB1'), _edition('GB1', '2026', current=False)),
            _joined_row(_competition('GB1'), _edition('GB1', '2027', current=True))]
    report = current.run_current_portion(
        rows, {'paid_io_allowed': True, 'write_mode': 'dual', 'revision': 1}, 'close',
        denominator_rows=[{'competition_id': 'GB1', 'live': True,
                           'competition_class': 'core_club', 'tier': 1}],
        state_dir=offline.root / 'state', qualification=offline.qualification,
        max_seconds=2400, max_scopes=1, scraper_factory=offline.factory,
        roster_writer=offline.roster_writer, career_writer=offline.career_writer,
        coach_writer=offline.coach_writer, stadium_writer=writer,
        now_fn=lambda: offline.clock.wall, monotonic_fn=lambda: offline.clock.elapsed,
    )
    result, = report['scopes']
    assert result['kind'] == 'season_close' and result['status'] == 'complete'
    saved = data(offline)
    assert saved['stadium_sweep_at_close'] == pending
    assert 'stadium_sweep' not in saved
    assert saved['cursor']['stadium_pending'] is False
