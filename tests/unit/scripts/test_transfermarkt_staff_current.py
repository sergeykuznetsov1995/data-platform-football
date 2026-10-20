"""Weekly staff work uses the real offline client and durable current journal."""
import json

import pytest

from dags.scripts import run_transfermarkt_current as current
from scrapers.transfermarkt.models import TrafficBudgetExceeded
from tests.unit.scripts.test_run_transfermarkt_current import offline, production_current
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


def test_production_current_wires_independent_staff_sweep(offline, production_current):
    from dags.utils.transfermarkt_staff import STAFF_TABLE, STAFF_RECEIPTS_TABLE

    policy = json.loads(production_current.policy_path.read_text())
    policy['allowed_write_tables'] += [STAFF_TABLE, STAFF_RECEIPTS_TABLE]
    production_current.policy_path.write_text(json.dumps(policy))
    production_current.run('production-wiring')
    assert production_current.writes == ['10', '20']
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
    assert saved['cursor']['staff_pending']
    assert saved['cursor']['cold_complete']
    assert saved['cursor']['resume_json'] == '{}'
    assert '20' in writes
    offline.staff_html = {}
    resumed_writer, resumed = writer_for(offline)
    offline.run('resume', staff_writer=resumed_writer)
    assert resumed == ['10']
    assert 'staff_sweep' not in data(offline)
    assert data(offline)['cursor']['staff_completed_at']
    assert not data(offline)['cursor']['staff_pending']


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
    old.pop('staff_pending')
    migrated = current.ScopeCursor.from_mapping(old)
    assert migrated.staff_completed_at is None
    assert migrated.resume_json == cursor.resume_json
    assert not migrated.staff_pending


@pytest.mark.parametrize('fresh_roster', [False, True])
def test_permanent_staff_failure_does_not_block_weekly_roster_or_full_staff_sweep(offline, fresh_roster):
    writer, writes = writer_for(offline, fail='10')
    offline.run('partial', staff_writer=writer)
    assert writes == ['20']
    offline.clock.advance(7 * 86400)
    if fresh_roster:
        state = offline.state()
        cursor = next(iter(state['scopes'].values()))['cursor']
        # Isolate the staff clock from the separate legacy-resume regression.
        cursor.update(cold_complete=True, resume_json='{}', roster_completed_at=offline.clock.wall.isoformat())
        offline.write_state(state)
    offline.calls.clear()
    report = offline.run('next-week', staff_writer=writer)
    assert ('weekly_staff' if fresh_roster else 'weekly_roster') in [item['kind'] for item in report['scopes']]
    assert sorted(url.split('/verein/')[1].split('/')[0]
                  for _, url in offline.calls if '/kader/' in url) == ([] if fresh_roster else ['10', '20'])
    assert staff_calls(offline) == ['10', '20']
    assert writes == ['20', '20']
    assert data(offline)['staff_sweep']['pending'] == ['10']


def test_old_staff_pending_does_not_displace_weekly_roster(offline):
    offline.run('cold')
    offline.clock.advance(7 * 86400)
    state = offline.state()
    saved = next(iter(state['scopes'].values()))
    saved['cursor'].pop('staff_pending', None)
    saved['cursor']['resume_json'] = json.dumps({
        'staff_pending': True, 'clubs': saved['listing_clubs'], 'squad_year': 2026,
        'required': [], 'careers': {'market_value_points': [], 'transfer_events': []},
    })
    offline.write_state(state)
    writer, writes = writer_for(offline)
    report = offline.run('old-staff-pending', staff_writer=writer)
    assert 'weekly_roster' in [item['kind'] for item in report['scopes']]
    assert writes == ['10', '20']


@pytest.mark.parametrize('work', [
    {'required': ['10']},
    {'careers': {'market_value_points': ['110']}},
    {'coaches_pending': True, 'coach_club_ids': ['10']},
    {'verified_pages': ['raw-1'], 'remaining_players': ['110']},
])
def test_old_staff_pending_migration_preserves_roster_continuation(work):
    cursor = current.ScopeCursor('scope', generation='old', cold_complete=True).as_dict()
    cursor.pop('staff_pending', None)
    cursor['resume_json'] = json.dumps({'staff_pending': True, **work})
    migrated = current.ScopeCursor.from_mapping(cursor)
    assert json.loads(migrated.resume_json) == work
    assert migrated.staff_pending


def test_old_staff_pending_migration_preserves_membership_removal(offline):
    offline.club_ids = tuple(str(club) for club in range(10, 20))
    offline.run('cold')
    offline.club_ids = offline.club_ids[:-1]
    offline.clock.advance(21 * 3600)
    offline.run('participants')
    state = offline.state()
    saved = next(iter(state['scopes'].values()))
    resume = json.loads(saved['cursor']['resume_json'])
    assert set(resume['clubs']) == set(offline.club_ids)
    assert not resume['required']
    resume['staff_pending'] = True
    saved['cursor'].pop('staff_pending', None)
    saved['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.calls.clear()
    writer, writes = writer_for(offline)
    report = offline.run('participants-continue', staff_writer=writer)
    assert report['scopes'][0]['kind'] == 'resume'
    assert report['scopes'][0]['status'] == 'complete'
    assert offline.writes[-1].expected_team_ids == offline.club_ids
    assert not any('/kader/' in url for _, url in offline.calls)
    assert writes == list(offline.club_ids)


@pytest.mark.parametrize('failure', ['write', 'deadline'])
def test_season_close_preserves_staff_remainder_without_blocking_handoff(offline, failure):
    writer, writes = writer_for(offline, fail='10' if failure == 'write' else None,
                               stop='10' if failure == 'deadline' else None)
    offline.run('partial', staff_writer=writer)
    pending = data(offline)['staff_sweep']
    assert pending['pending'] == (['10'] if failure == 'write' else ['20'])
    offline.calls.clear()
    rows = [_joined_row(_competition('GB1'), _edition('GB1', '2026', current=False)),
            _joined_row(_competition('GB1'), _edition('GB1', '2027', current=True))]
    report = current.run_current_portion(
        rows, {'paid_io_allowed': True, 'write_mode': 'dual', 'revision': 1}, 'close',
        denominator_rows=[{'competition_id': 'GB1', 'live': True, 'competition_class': 'core_club', 'tier': 1}],
        state_dir=offline.root / 'state', qualification=offline.qualification,
        max_seconds=2400, max_scopes=1, scraper_factory=offline.factory,
        roster_writer=offline.roster_writer, career_writer=offline.career_writer,
        coach_writer=offline.coach_writer, staff_writer=writer,
        now_fn=lambda: offline.clock.wall, monotonic_fn=lambda: offline.clock.elapsed)
    result, = report['scopes']
    assert result['kind'] == 'season_close'
    assert result['status'] == 'complete'
    assert result.get('historical_handoff') is True
    assert not offline.state()['season_closures']
    assert staff_calls(offline) == []
    saved = data(offline)
    assert saved['staff_sweep_at_close'] == pending
    assert 'staff_sweep' not in saved
    assert saved['cursor']['resume_json'] == '{}'
    assert not saved['cursor']['staff_pending']
