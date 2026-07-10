"""Unit tests for FlareSolverrClient (scrapers/base/flaresolverr_client.py)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrTimeout,
    describe_proxy_mode,
    is_chromium_error_page,
)


# -----------------------------------------------------------------------------
# describe_proxy_mode (issue #616 — proxy mode visibility)
# -----------------------------------------------------------------------------


class TestDescribeProxyMode:
    def test_none_is_proxy_less(self):
        assert describe_proxy_mode(None).startswith('PROXY-LESS')

    def test_empty_string_is_proxy_less(self):
        assert describe_proxy_mode('').startswith('PROXY-LESS')

    def test_filter_url_named_as_filter(self):
        assert 'filter' in describe_proxy_mode('http://proxy_filter:8899')

    def test_residential_url_named_without_leaking_creds(self):
        url = 'http://user:secret@residential.example.com:8080'
        desc = describe_proxy_mode(url)
        assert desc == 'via residential proxy'
        assert 'secret' not in desc


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _ok_response(json_payload: dict, status_code: int = 200) -> MagicMock:
    """Build a MagicMock that mimics a successful requests.Response."""
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.json.return_value = json_payload
    resp.text = ''
    resp.raise_for_status = MagicMock()
    return resp


def _ok_solution_payload() -> dict:
    return {
        'status': 'ok',
        'message': '',
        'solution': {
            'response': '<html>hello</html>',
            'cookies': [{'name': 'cf_clearance', 'value': 'abc'}],
            'userAgent': 'Mozilla/5.0',
            'status': 200,
        },
    }


# -----------------------------------------------------------------------------
# request.get tests
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrGet:
    def test_get_happy_path(self):
        client = FlareSolverrClient(url='http://fs:8191')
        with patch.object(
            client, 'session', new=MagicMock()
        ) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())

            out = client.get('https://example.com', session_id='s1')

        assert out['html'] == '<html>hello</html>'
        assert out['cookies'] == [{'name': 'cf_clearance', 'value': 'abc'}]
        assert out['userAgent'] == 'Mozilla/5.0'
        assert out['status'] == 200

        sess.post.assert_called_once()
        call_args = sess.post.call_args
        assert call_args.args[0] == 'http://fs:8191/v1'
        payload = call_args.kwargs['json']
        assert payload['cmd'] == 'request.get'
        assert payload['url'] == 'https://example.com'
        assert payload['session'] == 's1'
        assert payload['maxTimeout'] == 60_000
        assert 'returnOnlyCookies' not in payload

    def test_get_returns_only_cookies(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get('https://x.com', 's1', return_only_cookies=True)

        payload = sess.post.call_args.kwargs['json']
        assert payload['returnOnlyCookies'] is True

    def test_get_can_disable_media_per_navigation(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get('https://x.com', 's1', disable_media=True)

        payload = sess.post.call_args.kwargs['json']
        assert payload['disableMedia'] is True

    def test_post_commands_are_not_retried_by_http_adapter(self):
        client = FlareSolverrClient()

        retries = client.session.get_adapter('http://').max_retries

        assert retries.total == 2
        assert retries.allowed_methods == frozenset({'GET'})

    @pytest.mark.parametrize(
        'message',
        [
            'Cloudflare challenge could not be solved',
            'Turnstile timed out',
            'Failed to bypass challenge',
            'CLOUDFLARE error',
            'cloudflare detected',
        ],
    )
    def test_get_raises_cf_challenge(self, message):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {'status': 'error', 'message': message}
            )
            with pytest.raises(FlareSolverrCFChallengeFailed):
                client.get('https://x.com', 's1')

    def test_get_raises_generic_error(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {'status': 'error', 'message': 'Session not found'}
            )
            with pytest.raises(FlareSolverrError) as exc_info:
                client.get('https://x.com', 's1')
            # Must be the base error, NOT the CF subclass.
            assert not isinstance(exc_info.value, FlareSolverrCFChallengeFailed)

    def test_get_raises_timeout_on_requests_timeout(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.Timeout('slow')
            with pytest.raises(FlareSolverrTimeout):
                client.get('https://x.com', 's1')

    def test_get_raises_timeout_on_connection_error(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError(
                'refused'
            )
            with pytest.raises(FlareSolverrTimeout):
                client.get('https://x.com', 's1')

    def test_get_raises_on_non_2xx(self):
        client = FlareSolverrClient()
        bad_resp = MagicMock()
        bad_resp.ok = False
        bad_resp.status_code = 500
        bad_resp.text = 'Internal Server Error'
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = bad_resp
            with pytest.raises(FlareSolverrError) as exc_info:
                client.get('https://x.com', 's1')
            assert not isinstance(
                exc_info.value, FlareSolverrCFChallengeFailed
            )
            assert not isinstance(exc_info.value, FlareSolverrTimeout)

    def test_get_max_timeout_default(self):
        client = FlareSolverrClient(default_max_timeout_ms=60_000)
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get('https://x.com', 's1')
        payload = sess.post.call_args.kwargs['json']
        assert payload['maxTimeout'] == 60_000

    def test_get_max_timeout_custom(self):
        client = FlareSolverrClient(default_max_timeout_ms=60_000)
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get('https://x.com', 's1', max_timeout_ms=30_000)
        payload = sess.post.call_args.kwargs['json']
        assert payload['maxTimeout'] == 30_000


# -----------------------------------------------------------------------------
# session lifecycle tests
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrSessions:
    def test_create_session_payload(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            client.create_session('my-id')

        payload = sess.post.call_args.kwargs['json']
        assert payload == {'cmd': 'sessions.create', 'session': 'my-id'}

    def test_create_session_with_proxy_splits_credentials(self):
        # Chromium rejects creds embedded in the proxy URL
        # (ERR_NO_SUPPORTED_PROXIES, #647) — auth must go in separate
        # username/password fields, with a credential-free url.
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            client.create_session('id', proxy_url='http://u:p@h:1')

        payload = sess.post.call_args.kwargs['json']
        assert payload['cmd'] == 'sessions.create'
        assert payload['session'] == 'id'
        assert payload['proxy'] == {
            'url': 'http://h:1', 'username': 'u', 'password': 'p',
        }

    def test_create_session_proxy_without_credentials_unchanged(self):
        # A credential-free proxy URL is passed through verbatim.
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            client.create_session('id', proxy_url='http://h:1')

        payload = sess.post.call_args.kwargs['json']
        assert payload['proxy'] == {'url': 'http://h:1'}

    def test_create_session_decodes_percent_encoded_lease_credentials(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            client.create_session(
                'id', proxy_url='http://lease:s%2Fecret%2Btoken@proxy_filter:8900'
            )

        payload = sess.post.call_args.kwargs['json']
        assert payload['proxy'] == {
            'url': 'http://proxy_filter:8900',
            'username': 'lease',
            'password': 's/ecret+token',
        }

    def test_create_session_log_never_contains_proxy_credentials(self, caplog):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            with caplog.at_level('INFO'):
                client.create_session(
                    'id', proxy_url='http://lease:top-secret@proxy_filter:8900'
                )

        assert 'top-secret' not in caplog.text
        assert 'ad-tech filter' in caplog.text

    def test_destroy_session_idempotent(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {'status': 'error', 'message': 'Session not found'}
            )
            # Must NOT raise.
            client.destroy_session('ghost')

    def test_destroy_session_swallows_timeout(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError('x')
            # FlareSolverrTimeout is a subclass of FlareSolverrError → swallowed.
            client.destroy_session('whatever')

    def test_list_sessions(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {'status': 'ok', 'sessions': ['a', 'b']}
            )
            assert client.list_sessions() == ['a', 'b']


# -----------------------------------------------------------------------------
# /health
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrHealth:
    def test_health_returns_true(self):
        client = FlareSolverrClient(url='http://fs:8191')
        with patch.object(client, 'session', new=MagicMock()) as sess:
            resp = MagicMock()
            resp.status_code = 200
            sess.get.return_value = resp
            assert client.health() is True
        sess.get.assert_called_once()
        assert sess.get.call_args.args[0] == 'http://fs:8191/health'

    def test_health_returns_false_on_error(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.get.side_effect = requests.exceptions.ConnectionError('nope')
            # Must NOT raise.
            assert client.health() is False

    def test_health_returns_false_on_non_200(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            resp = MagicMock()
            resp.status_code = 503
            sess.get.return_value = resp
            assert client.health() is False


# -----------------------------------------------------------------------------
# Context manager
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrContextManager:
    def test_context_manager_creates_and_destroys(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})

            with client as (c, session_id):
                assert c is client
                assert session_id.startswith('fs-')
                # 'fs-' + 8 hex chars = 11 chars total.
                assert len(session_id) == 11

        # Two posts: sessions.create then sessions.destroy.
        assert sess.post.call_count == 2
        cmds = [call.kwargs['json']['cmd'] for call in sess.post.call_args_list]
        sessions = [
            call.kwargs['json']['session'] for call in sess.post.call_args_list
        ]
        assert cmds == ['sessions.create', 'sessions.destroy']
        # Both calls used the same auto-generated session id.
        assert sessions[0] == sessions[1]
        assert sessions[0].startswith('fs-')

    def test_context_manager_destroys_on_exception(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})

            with pytest.raises(RuntimeError):
                with client as (_c, _sid):
                    raise RuntimeError('boom')

        # destroy still happened.
        cmds = [call.kwargs['json']['cmd'] for call in sess.post.call_args_list]
        assert 'sessions.destroy' in cmds

    def test_context_manager_resets_auto_session_after_exit(self):
        client = FlareSolverrClient()
        with patch.object(client, 'session', new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({'status': 'ok'})
            with client as (_c, _sid):
                assert client._auto_session_id is not None
            # Cleared after __exit__.
            assert client._auto_session_id is None


# -----------------------------------------------------------------------------
# is_chromium_error_page tests (#655)
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestIsChromiumErrorPage:
    """FlareSolverr returns Chromium net-error pages as HTTP 200 HTML; these
    must be detected so callers refuse to cache them (#655)."""

    @pytest.mark.parametrize(
        'html',
        [
            '<html><body>chrome-error://dino/</body></html>',
            '<html id="neterror"><body>...</body></html>',
            '<html><body>ERR_NO_SUPPORTED_PROXIES</body></html>',
            # Realistic shape: title is the host, body is the chrome error page.
            '<html><head><title>sofifa.com</title></head>'
            '<body class="neterror"><div id="main-frame-error"></div></body></html>',
        ],
    )
    def test_detects_error_pages(self, html):
        assert is_chromium_error_page(html) is True

    @pytest.mark.parametrize(
        'html',
        [
            '',
            '<html><body><p>real content</p></body></html>',
            '<select id="select-version"><option>FC 26</option></select>',
        ],
    )
    def test_passes_real_html(self, html):
        assert is_chromium_error_page(html) is False
