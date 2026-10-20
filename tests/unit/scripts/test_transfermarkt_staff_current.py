"""Weekly staff work uses the real offline client and durable current journal."""
import json

import pytest

from dags.scripts import run_transfermarkt_current as current
from scrapers.transfermarkt.models import TrafficBudgetExceeded
from tests.unit.scripts.test_run_transfermarkt_current import offline
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row

pytestmark = pytest.mark.unit


def writer_for(offline, *, fail=None, stop=None):
    writes = []
    def writer(scraper, scope, preflight, cycle_id, *, club_id, outcome, source_url):
        if club_id == fail:
            raise RuntimeError('read-back failed')
        writes.append(club_id)
        if club_id == stop:
            offline.clock.advance(2350)
        return {**offline.proof(), 'club_id': club_id, 'raw_capture_id': outcome.raw_capture_id,
                'source_body_hash': outcome.raw_body_hash}
    return writer, writes


def data(offline):
    return next(iter(offline.state()['scopes'].values()))


def staff_calls(offline):
    return [url.rsplit('/', 1)[-1] for _, url in offline.calls if '/mitarbeiter/' in url]


def test_production_current_wires_independent_staff_sweep(offline, monkeypatch):
    writer, writes = writer_for(offline)
    monkeypatch.setattr(current, '_production_factory', offline.factory)
    monkeypatch.setattr('dags.utils.transfermarkt_staff.write_current_staff', writer)
    # Keep the public call compatible with the pre-fix runner. Only the actual
    # production factory is replaced; no new staff injection argument is used.
    current.run_current_portion(
        [_joined_row(_competition('GB1'), _edition('GB1', '2026'))],
        {'paid_io_allowed': True, 'write_mode': 'dual', 'revision': 1}, 'production-wiring',
        denominator_rows=[{'competition_id': 'GB1', 'live': True, 'competition_class': 'core_club', 'tier': 1}],
        state_dir=offline.root / 'state', qualification=offline.qualification,
        max_seconds=2400, roster_writer=offline.roster_writer,
        career_writer=offline.career_writer, coach_writer=offline.coach_writer,
        now_fn=lambda: offline.clock.wall, monotonic_fn=lambda: offline.clock.elapsed,
    )
    assert writes == ['10', '20']
    assert data(offline)['cursor']['staff_completed_at']


def test_staff_runs_on_seventh_day_independently_of_roster_and_coach_ttl(offline):
    writer, writes = writer_for(offline)
    offline.run('cold', staff_writer=writer)
    assert writes == ['10', '20']
    assert data(offline)['cursor']['staff_completed_at']
    assert data(offline)['cursor']['resume_json'] == '{}'
    offline.clock.advance(6 * 86400)
    offline.run('six-days', staff_writer=writer)
    assert writes == ['10', '20']
    offline.clock.advance(86400)
    # A fresh roster and fresh coach TTL must not mask the separate staff clock.
    state = offline.state()
    row = next(iter(state['scopes'].values()))
    row['cursor']['roster_completed_at'] = offline.clock.wall.isoformat()
    row['cursor']['resume_json'] = '{}'
    row['coaches_checked_at'] = offline.clock.wall.isoformat()
    offline.write_state(state)
    report = offline.run('seven-days', staff_writer=writer)
    assert 'weekly_staff' in [item['kind'] for item in report['scopes']]
    assert writes == ['10', '20', '10', '20']
    assert not json.loads(data(offline)['cursor']['resume_json']).get('staff_pending')


@pytest.mark.parametrize('failure', ['page', 'write', 'proof'])
def test_first_failed_club_does_not_block_second_and_restarts_only_pending(offline, failure):
    if failure == 'page':
        offline.staff_html = {'10': '<html>broken staff</html>'}
    writer, writes = writer_for(offline, fail='10' if failure == 'write' else None)
    good_writer = writer
    if failure == 'proof':
        def writer(*args, **kwargs):
            proof = good_writer(*args, **kwargs)
            return {**proof, 'verified': kwargs['club_id'] != '10'}
    offline.run('partial', staff_writer=writer)
    saved = data(offline)
    assert saved['staff_sweep']['pending'] == ['10']
    assert saved['staff_sweep']['deferred']['10']
    assert saved['cursor']['staff_completed_at'] is None
    assert json.loads(saved['cursor']['resume_json'])['staff_pending']
    assert '20' in writes
    offline.staff_html = {}
    resumed_writer, resumed = writer_for(offline)
    offline.run('resume', staff_writer=resumed_writer)
    assert resumed == ['10']
    assert 'staff_sweep' not in data(offline)
    assert data(offline)['cursor']['staff_completed_at']
    assert not json.loads(data(offline)['cursor']['resume_json']).get('staff_pending')


def test_deadline_stops_before_next_club_and_restart_keeps_remainder(offline):
    writer, writes = writer_for(offline, stop='10')
    offline.run('deadline', staff_writer=writer)
    assert writes == ['10']
    assert staff_calls(offline) == ['10']
    assert data(offline)['staff_sweep']['pending'] == ['20']
    resumed_writer, resumed = writer_for(offline)
    offline.run('resume', staff_writer=resumed_writer)
    assert resumed == ['20']
    assert not json.loads(data(offline)['cursor']['resume_json']).get('staff_pending')


def test_budget_exhaustion_keeps_all_unwritten_clubs(offline):
    def writer(*args, **kwargs):
        raise TrafficBudgetExceeded('scope budget exhausted')
    offline.run('budget', staff_writer=writer)
    assert staff_calls(offline) == ['10']
    assert data(offline)['staff_sweep']['pending'] == ['10', '20']
    assert data(offline)['cursor']['staff_completed_at'] is None


def test_old_cursor_migrates_additive_staff_clock_without_losing_resume():
    cursor = current.ScopeCursor('scope', generation='old', resume_json='{"clubs": {}}')
    old = cursor.as_dict()
    old.pop('staff_completed_at')
    migrated = current.ScopeCursor.from_mapping(old)
    assert migrated.staff_completed_at is None
    assert migrated.resume_json == cursor.resume_json
