"""Offline checks for the qualification CAPTCHA policy and saved history."""
from datetime import datetime, timedelta, timezone
import json

import pytest
from dags.scripts.transfermarkt_signal_guard import QualificationGuard, GuardHalt
from dags.scripts.bench_transfermarkt_signals import _write

NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)


def observe(guard, url, status):
    guard.reserve(url)
    return guard.observe(url, 1, {str(status): 1}, [{'status_code': status}], None, None)


def test_streak_resets_on_response_and_survives_restart(tmp_path):
    guard = QualificationGuard(tmp_path, NOW, _write)
    assert observe(guard, 'url', 405)
    assert not observe(guard, 'url', 404)
    assert observe(guard, 'url', 403)
    guard = QualificationGuard(tmp_path, NOW, _write)
    assert observe(guard, 'url', 429)
    with pytest.raises(GuardHalt, match='captcha_streak'):
        observe(guard, 'url', 405)
    assert guard.counts('url')['responses'] == 1


@pytest.mark.parametrize('captchas,total,halt', [(2, 99, False), (1, 100, False), (2, 100, True)])
def test_rate_boundary_excludes_transport(tmp_path, captchas, total, halt):
    guard = QualificationGuard(tmp_path, NOW, _write)
    for i in range(captchas):
        observe(guard, f'captcha-{i}', 405)
    observe(guard, 'transport', 0)
    for i in range(total - captchas - 1):
        observe(guard, f'ordinary-{i}', 404)
    guard = QualificationGuard(tmp_path, NOW, _write)
    if halt:
        with pytest.raises(GuardHalt, match='captcha_rate'):
            observe(guard, 'last', 404)
    else:
        observe(guard, 'last', 404)
    assert len(guard.data['response_window']) == total


def test_hour_window_expires_at_3600_seconds(tmp_path):
    guard = QualificationGuard(tmp_path, NOW, _write)
    observe(guard, 'captcha-1', 405)
    observe(guard, 'captcha-2', 405)
    guard = QualificationGuard(tmp_path, NOW + timedelta(seconds=3600), _write)
    for i in range(100):
        observe(guard, f'ordinary-{i}', 404)
    assert len(guard.data['response_window']) == 100
    assert not any(e['captcha'] for e in guard.data['response_window'])


@pytest.mark.parametrize('stop', [None, {'reason': 'captcha', 'url': 'url'}])
def test_v1_migration_keeps_quota_and_existing_stop(tmp_path, stop):
    data = {'version': 1, 'days': {'2026-10-08': {'url': {'responses': 2, 'transport': 1}}}, 'stop': stop}
    (tmp_path/'qualification-guard.json').write_text(json.dumps(data))
    guard = QualificationGuard(tmp_path, NOW, _write)
    assert guard.data['version'] == 2
    assert guard.counts('url') == {'responses': 2, 'transport': 1}
    assert guard.data['stop'] == stop


def test_rebuild_preserves_unknown_reservation_after_confirmed_captcha(tmp_path):
    state = {'identity': 'a' * 64, 'qualification_day_msk': '2026-10-08',
             'attempt_records': [{'envelope_id': 'captcha', 'url': 'url', 'status_code': 405,
                                  'observed_at': NOW.isoformat(), 'qualification_captcha': True}],
             'url_attempts': {'url': {'responses': 1, 'transport': 0}}}
    (tmp_path/'state.json').write_text(json.dumps(state))
    guard = QualificationGuard(tmp_path, NOW, _write)
    assert guard.counts('url')['responses'] == 1
    assert len(guard.data['response_window']) == 1
    assert guard.data['captcha_streaks']['url'] == 1
    guard = QualificationGuard(tmp_path, NOW, _write)
    assert guard.counts('url')['responses'] == 1
    assert len(guard.data['response_window']) == 1
