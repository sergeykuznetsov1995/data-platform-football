"""Retry admission across real current portions, with synthetic source responses."""
from datetime import timedelta

import pytest

from tests.unit.scripts.test_run_transfermarkt_current import Offline


pytestmark = pytest.mark.unit


@pytest.fixture
def offline(tmp_path, monkeypatch):
    return Offline(tmp_path, monkeypatch)


def rows(report, kind):
    return [item for item in report['scopes'] if item['kind'] == kind]


def one_scope(offline):
    return next(iter(offline.state()['scopes'].values()))


def test_same_player_generation_releases_apply_slot_until_backoff_expires(offline):
    offline.run_cold('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.player_values['110'] = 2_000_000
    offline.clock.advance(21 * 3600)
    offline.run('detect')
    writes = len(offline.writes)
    for attempt in range(3):
        offline.clock.advance(60)
        failures = rows(offline.run(f'confirm-{attempt}'), 'apply_change')
        assert len(failures) == 1 and 'source_mismatch' in failures[0]['error']
    failed = one_scope(offline)['signals']['player:110']
    assert failed['status'] == 'failed'
    assert failed['observation']['signature'] != failed['applied_signature']
    assert len(offline.writes) == writes

    offline.calls.clear()
    offline.clock.advance(4 * 3600 - 1)
    assert not rows(offline.run('deferred'), 'apply_change')
    assert not offline.calls
    offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(1)
    assert rows(offline.run('retry'), 'apply_change')
    assert one_scope(offline)['signals']['player:110']['status'] == 'applied'
    assert len(offline.writes) == writes + 1


def test_recovery_uncertain_streak_releases_resume_slot(offline):
    offline.run('cold')
    offline.clock.advance(60)
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['recovery_signal_refresh_required'] = {
        'required_checks': ['players'], 'reconciled_at': offline.clock.wall.isoformat(),
    }
    data['cursor']['players_checked_at'] = None
    offline.write_state(state)
    offline.packet_missing = True
    for attempt in range(3):
        offline.clock.advance(60)
        report = offline.run(f'uncertain-{attempt}', max_scopes=1)
        assert rows(report, 'signals')[0]['status'] == 'uncertain'
        assert 'SignalRefreshRequired' in rows(report, 'resume')[0]['retained']
    data = one_scope(offline)
    assert data['cursor']['players_checked_at'] is None
    assert data['recovery_signal_refresh_required']
    report = offline.run('other-work', competitions=('GB1', 'ES1'), max_scopes=1)
    assert not rows(report, 'resume')
    assert rows(report, 'cold_roster')
    assert not rows(offline.run('no-other-work', max_scopes=1), 'resume')

    offline.packet_missing = False
    offline.clock.advance(timedelta(hours=4).total_seconds())
    offline.run('recover')
    assert not one_scope(offline).get('recovery_signal_refresh_required')


@pytest.mark.parametrize('source_change', ['same', 'superseding_player', 'second_player', 'still_contradictory'])
def test_daily_detection_survives_mismatch_backoff(offline, source_change):
    offline.run_cold('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.clock.advance(21 * 3600)
    offline.player_values['110'] = 2_000_000
    offline.run('detect')
    offline.clock.advance(19 * 3600)
    for attempt in range(3):
        offline.clock.advance(60)
        failed = rows(offline.run(f'lag-{attempt}'), 'apply_change')
        assert len(failed) == 1 and 'source_mismatch' in failed[0]['error']
    old = one_scope(offline)['signals']['player:110']
    writes = len(offline.writes)

    if source_change == 'superseding_player':
        offline.player_values['110'] = 3_000_000
        offline.squad_values['10'] = '€3.0m'
    elif source_change in {'second_player', 'still_contradictory'}:
        offline.player_values['120'] = 3_000_000
        offline.squad_values['20'] = '€3.0m'
        if source_change == 'second_player':
            offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(3600)
    report = offline.run('due-again')
    assert rows(report, 'signals') and not rows(report, 'signals')[0].get('check_failures')
    after_check = one_scope(offline)['signals']['player:110']
    assert after_check['observation']['checked_at'] > old['observation']['checked_at']
    offline.calls.clear()
    next_report = offline.run('apply-new-evidence')
    if source_change == 'same':
        assert not rows(report, 'apply_change') and not rows(next_report, 'apply_change')
        assert after_check['observation']['signature'] == old['observation']['signature']
        assert after_check['status'] == 'failed' and not offline.calls
    elif source_change == 'still_contradictory':
        assert any('source_mismatch' in item.get('error', '')
                   for item in rows(report, 'apply_change') + rows(next_report, 'apply_change'))
        data = one_scope(offline)
        assert len(offline.writes) == writes
        assert all(data['signals'][f'player:{p}']['status'] != 'applied' for p in ('110', '120'))
    else:
        data = one_scope(offline)
        assert len(offline.writes) == writes + 1
        assert set(offline.writes[-1].expected_team_ids) == {'10', '20'}
        assert data['signals']['player:110']['status'] == data['signals']['player:120']['status'] == 'applied'
        if source_change == 'superseding_player':
            assert after_check['observation']['signature'] != old['observation']['signature']
            assert any(old['observation']['signature'] in event for event in after_check['superseded_evidence'])


def test_stale_career_confirmation_also_releases_slot(offline):
    offline.run_cold('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.player_dates['110'] = '2026-09-30'
    offline.mv_payloads['110'] = {'list': [{'datum_mw': 'Aug 30, 2026', 'y': 1000000, 'mw': '€1.0m'}]}
    offline.clock.advance(21 * 3600)
    offline.run('changed-date')
    for attempt in range(3):
        offline.clock.advance(60)
        assert rows(offline.run(f'stale-graph-{attempt}'), 'apply_change')
        proof = one_scope(offline)['career_proofs']['market_value_points']['collector_window']
        assert proof['source_mismatch_ids'] == ['110']
    offline.calls.clear()
    assert not rows(offline.run('no-paid-retry'), 'apply_change')
    assert not offline.calls
    offline.clock.advance(4 * 3600)
    offline.mv_payloads['110'] = {'list': [{'datum_mw': 'Sep 30, 2026', 'y': 1000000, 'mw': '€1.0m'}]}
    assert rows(offline.run('new-graph'), 'apply_change')
    assert one_scope(offline)['signals']['player:110']['status'] == 'applied'


def test_uncertain_recovery_releases_single_slot_to_next_unfinished_edition(offline):
    from scrapers.transfermarkt.registry import deterministic_scope_id

    competitions = ('GB1', 'ES1')
    offline.run('two-rosters', competitions=competitions, max_scopes=2)
    offline.clock.advance(60)
    state = offline.state()
    first = deterministic_scope_id('GB1', '2026')
    second = deterministic_scope_id('ES1', '2026')
    data = state['scopes'][first]
    data['recovery_signal_refresh_required'] = {
        'required_checks': ['players'], 'reconciled_at': offline.clock.wall.isoformat(),
    }
    data['cursor']['players_checked_at'] = None
    offline.write_state(state)
    offline.packet_missing = True
    for attempt in range(3):
        offline.clock.advance(60)
        report = offline.run(f'blocked-first-{attempt}', competitions=competitions, max_scopes=1)
        assert [r['scope_id'] for r in rows(report, 'resume')] == [first]
        assert 'SignalRefreshRequired' in rows(report, 'resume')[0]['retained']
    report = offline.run('give-slot-to-peer', competitions=competitions, max_scopes=1)
    assert [r['scope_id'] for r in rows(report, 'resume')] == [second]


def test_required_check_progress_resets_streak_while_another_remains_uncertain(offline):
    offline.run('cold')
    offline.clock.advance(60)
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['recovery_signal_refresh_required'] = {
        'required_checks': ['injury', 'players'], 'reconciled_at': offline.clock.wall.isoformat(),
    }
    data['cursor']['players_checked_at'] = data['cursor']['injury_checked_at'] = None
    offline.write_state(state)
    offline.packet_missing = True
    good = offline.injury_html
    offline.injury_html = '<html>partial</html>'
    for attempt in range(6):
        if attempt == 2:
            offline.injury_html = good
        offline.clock.advance(60)
        report = offline.run(f'recovery-{attempt}')
        assert 'SignalRefreshRequired' in rows(report, 'resume')[0]['retained']
        assert ('injury' in rows(report, 'signals')[0]['check_failures']) == (attempt < 2)
    assert not rows(offline.run('deferred-after-new-streak'), 'resume')


def test_recovery_deadlines_alone_do_not_start_uncertain_backoff(offline, monkeypatch):
    from dags.scripts import run_transfermarkt_current as current

    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['recovery_signal_refresh_required'] = {'required_checks': ['players'], 'reconciled_at': offline.clock.wall.isoformat()}
    data['cursor']['players_checked_at'] = None
    offline.write_state(state)

    def deadline(scope):
        raise current.PortionDeadlineError('portion deadline before player check')

    monkeypatch.setattr(current._Scope, 'players', deadline)
    for attempt in range(4):
        offline.clock.advance(60)
        report = offline.run(f'deadline-{attempt}')
        assert 'deadline' in rows(report, 'signals')[0]['check_failures']['players']
        assert 'SignalRefreshRequired' in rows(report, 'resume')[0]['retained']
