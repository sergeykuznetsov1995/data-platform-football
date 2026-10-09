"""Offline observations and persistent notification dedup for #1466."""
import gzip
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scrapers.clubelo import watch
from scrapers.clubelo.parse import LayoutChanged
from scrapers.clubelo.transport import ClubEloBlocked, ClubEloFetchError

FIXTURES = Path(__file__).parents[2] / 'fixtures' / 'clubelo'


def body(name, directory='20260924'):
    return gzip.decompress((FIXTURES / directory / (name + '.html.gz')).read_bytes())


class Transport:
    def __init__(self, login=None, fixtures=None, status=200):
        self.answers = {'/login/': login if login is not None else body('login'),
                        '/Fixtures': fixtures if fixtures is not None else body('Fixtures')}
        self.status = status
        self.paths = []
        self.requests = self.wire_bytes = 0

    def get(self, path):
        self.paths.append(path)
        self.requests += 1
        value = self.answers[path]
        if isinstance(value, Exception):
            raise value
        self.wire_bytes += len(gzip.compress(value))
        return SimpleNamespace(status=self.status, body=value)


def run(tmp_path, transport=None, notifier=None, **kwargs):
    return watch.run_watch(transport or Transport(), state_file=tmp_path / 'state.json',
                           notifier=notifier if notifier is not None else Mock(return_value=True), **kwargs)


def test_closed_login_and_empty_fixtures_create_silent_persistent_baseline(tmp_path):
    notify = Mock(return_value=True)
    result = run(tmp_path, notifier=notify)
    assert result['status'] == 'success'
    assert result['requests'] == 2 and result['wire_bytes'] > 0
    assert result['observed']['login']['registration_unavailable'] is True
    assert result['observed']['login']['pricing_links'] == []  # CSS isn't evidence.
    assert result['observed']['fixtures'] == {'has_rows': False}  # Country tables ignored.
    assert not result['notified']
    notify.assert_not_called()
    saved = json.loads((tmp_path / 'state.json').read_text())
    assert saved['observed'] == saved['notified']


@pytest.mark.parametrize('name', ['login_open', 'login_prices'])
def test_registration_or_prices_notified_once_across_invocations(tmp_path, name):
    notify = Mock(return_value=True)
    run(tmp_path, notifier=notify)
    changed = body(name, 'watch')
    assert run(tmp_path, Transport(login=changed), notify)['notified'] is True
    assert run(tmp_path, Transport(login=changed), notify)['notified'] is False
    assert notify.call_count == 1


def test_missing_closed_sentence_is_itself_a_signal(tmp_path):
    notify = Mock(return_value=True)
    opened = body('login').replace(b'Account registration is not available yet.', b'')
    assert run(tmp_path, Transport(login=opened), notify)['notified']
    assert not run(tmp_path, Transport(login=opened), notify)['notified']
    assert notify.call_count == 1


def test_first_interesting_state_notified_and_combined_with_fixtures(tmp_path):
    notify = Mock(return_value=True)
    transport = Transport(body('login_open', 'watch'), body('Fixtures_rows', 'watch'))
    assert run(tmp_path, transport, notify)['notified']
    assert not run(tmp_path, transport, notify)['notified']
    assert notify.call_count == 1
    message = notify.call_args.args[0]
    assert '/login/' in message and 'Fixtures: появились строки матчей.' in message


def test_csrf_and_generation_time_and_match_count_do_not_change_state(tmp_path):
    notify = Mock(return_value=True)
    opened = body('login_open', 'watch')
    fixtures = body('Fixtures_rows', 'watch')
    run(tmp_path, Transport(opened, fixtures), notify)
    changed = re.sub(br'(name="csrfmiddlewaretoken" value=")[^"]+', br'\1changedtoken', opened)
    changed = changed.replace(b'href="/register/"', b'href="/register/?token=random#fragment"')
    fixtures = fixtures.replace(b'2026-09-24 15:55:45', b'2026-10-01 08:00:00')
    fixtures = fixtures.replace(b'</table><p><small>Page created',
                                b'<tr><td>2026-09-26</td><td>Other home</td><td>Other away</td></tr>'
                                b'</table><p><small>Page created', 1)
    assert not run(tmp_path, Transport(changed, fixtures), notify)['notified']
    assert notify.call_count == 1


def test_visible_price_change_is_notified_once(tmp_path):
    notify = Mock(return_value=True)
    priced = body('login_prices', 'watch')
    run(tmp_path, Transport(login=priced), notify)
    assert run(tmp_path, Transport(login=priced.replace(b'9.00', b'12.00')), notify)['notified']
    assert not run(tmp_path, Transport(login=priced.replace(b'9.00', b'12.00')), notify)['notified']
    assert notify.call_count == 2


@pytest.mark.parametrize('currency', ['€', '$', '£', 'EUR', 'USD', 'GBP'])
def test_price_suffix_change_is_detected(tmp_path, currency):
    notify = Mock(return_value=True)
    priced = body('login').replace(b'</body>', f'<p>API subscription: 9.00 {currency}</p></body>'.encode())
    assert run(tmp_path, Transport(login=priced), notify)['notified']
    assert run(tmp_path, Transport(login=priced.replace(b'9.00', b'12.00')), notify)['notified']
    assert notify.call_count == 2


@pytest.mark.parametrize('style', ['display:none', 'display: none !important', 'VISIBILITY: hidden'])
def test_inline_hidden_registration_and_price_are_not_evidence(style):
    original = body('login')
    block = (f'<div style="{style}"><a href="/register/">Create account</a><p>€9.00</p></div>').encode()
    changed = original.replace(b'</body>', block + b'</body>')
    assert watch.parse_login(changed) == watch.parse_login(original)


def test_truncated_login_keeps_previous_state_and_does_not_notify(tmp_path):
    notify = Mock(return_value=True)
    run(tmp_path, notifier=notify)
    original = body('login')
    truncated = original[:original.index(b'<p style="font-size:13px')]
    result = run(tmp_path, Transport(login=truncated), notify)
    assert result['status'] == 'error'
    assert result['observed']['login']['registration_unavailable'] is True
    notify.assert_not_called()


def test_return_to_closed_or_empty_state_is_a_new_transition(tmp_path):
    notify = Mock(return_value=True)
    run(tmp_path, Transport(body('login_open', 'watch'), body('Fixtures_rows', 'watch')), notify)
    assert run(tmp_path, notifier=notify)['notified']
    assert not run(tmp_path, notifier=notify)['notified']
    assert notify.call_count == 2


@pytest.mark.parametrize('name,labels', [
    ('login_open', ['Ссылки регистрации']),
    ('login_prices', ['Видимые цены', 'Ссылки цен/подписки']),
])
def test_notification_names_disappeared_signals(tmp_path, name, labels):
    notify = Mock(return_value=True)
    run(tmp_path, Transport(login=body(name, 'watch')), notify)
    assert run(tmp_path, notifier=notify)['notified']
    message = notify.call_args.args[0]
    for label in labels:
        assert label + ': больше не обнаружены.' in message
    assert not run(tmp_path, notifier=notify)['notified']
    assert notify.call_count == 2


@pytest.mark.parametrize('answer,status', [
    (ClubEloFetchError('network/5xx exhausted'), 200),
    (b'<html><title>Error</title><body>Server Error</body></html>', 200),
    (body('login'), 302),
    (body('login'), 500),
])
def test_bad_check_preserves_valid_observations(tmp_path, answer, status):
    notify = Mock(return_value=True)
    run(tmp_path, notifier=notify)
    before = json.loads((tmp_path / 'state.json').read_text())['observed']
    result = run(tmp_path, Transport(login=answer, status=status), notify)
    assert result['status'] == 'error' and result['errors']
    assert json.loads((tmp_path / 'state.json').read_text())['observed'] == before
    notify.assert_not_called()


def test_block_stops_all_remaining_source_requests(tmp_path):
    transport = Transport(login=ClubEloBlocked('HTTP 429'))
    result = run(tmp_path, transport)
    assert result['status'] == 'error' and transport.paths == ['/login/']


def test_one_page_failure_does_not_discard_other_page_change(tmp_path):
    notify = Mock(return_value=True)
    run(tmp_path, notifier=notify)
    result = run(tmp_path, Transport(body('login_open', 'watch'), ClubEloFetchError('timeout')), notify)
    assert result['status'] == 'error' and result['notified']
    assert result['observed']['fixtures'] == {'has_rows': False}
    assert notify.call_count == 1


def test_failed_notification_remains_pending(tmp_path):
    notify = Mock(side_effect=[False, True])
    changed = Transport(login=body('login_open', 'watch'))
    assert run(tmp_path, changed, notify)['status'] == 'error'
    saved = json.loads((tmp_path / 'state.json').read_text())
    assert 'login' not in saved['notified']
    assert run(tmp_path, changed, notify)['notified']
    assert not run(tmp_path, changed, notify)['notified']
    assert notify.call_count == 2


def test_notifier_exception_is_soft_and_retried(tmp_path):
    notify = Mock(side_effect=RuntimeError('TG down'))
    transport = Transport(login=body('login_open', 'watch'))
    assert run(tmp_path, transport, notify)['status'] == 'error'
    assert run(tmp_path, transport, Mock(return_value=True))['notified']


@pytest.mark.parametrize('content', ['not json', '{}', '{"version":1,"observed":[],"notified":{}}'])
def test_corrupt_state_is_not_overwritten_and_no_notification(tmp_path, content):
    path = tmp_path / 'state.json'
    path.write_text(content)
    transport = Transport(login=body('login_open', 'watch'))
    notify = Mock(return_value=True)
    assert run(tmp_path, transport, notify)['status'] == 'error'
    assert path.read_text() == content and transport.requests == 0
    notify.assert_not_called()


def test_unwritable_state_prevents_notification(tmp_path, monkeypatch):
    monkeypatch.setattr(watch, '_save', Mock(side_effect=PermissionError('state unwritable')))
    notify = Mock(return_value=True)
    assert run(tmp_path, Transport(login=body('login_open', 'watch')), notify)['status'] == 'error'
    notify.assert_not_called()


def test_atomic_write_keeps_old_json_on_replace_failure(tmp_path, monkeypatch):
    path = tmp_path / 'state.json'
    path.write_text('{"old":true}')
    monkeypatch.setattr(watch.os, 'replace', Mock(side_effect=OSError('disk')))
    with pytest.raises(OSError):
        watch._save(path, {'new': True})
    assert path.read_text() == '{"old":true}'
    assert list(tmp_path.iterdir()) == [path]


def test_busy_state_lock_prevents_requests_and_send(tmp_path):
    with (tmp_path / 'state.json.lock').open('a') as lock:
        watch.fcntl.flock(lock, watch.fcntl.LOCK_EX | watch.fcntl.LOCK_NB)
        transport, notify = Transport(), Mock()
        assert run(tmp_path, transport, notify)['status'] == 'error'
        assert transport.requests == 0
        notify.assert_not_called()


def test_api_switch_remains_stub_and_does_not_make_requests(tmp_path):
    transport = Transport()
    result = run(tmp_path, transport, source='api')
    assert result['status'] == 'error' and 'NotImplementedError' in result['errors'][0]
    assert transport.requests == 0


def test_changed_fixtures_layout_is_unknown_not_empty():
    with pytest.raises(LayoutChanged):
        watch.parse_fixtures(b'<html><h1>Fixtures</h1><table></table></html>')
