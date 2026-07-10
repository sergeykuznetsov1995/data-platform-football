"""Unit tests for FBref curl_cffi HTTP fast-path traffic audit (issue #124).

Covers:
- Content-Type → CDP-style resource_type mapping helper.
- `_fetch_page_http()` increments `_stats['http_*']` counters on 200 success.
- Counters increment on non-200 too (proxy spent bytes regardless of status).
"""

import pytest
from unittest.mock import MagicMock


class TestContentTypeMapping:
    """`_content_type_to_resource_type` must mirror NodriverBypass._rtype_name."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "content_type,expected",
        [
            ('text/html; charset=utf-8', 'Document'),
            ('text/html', 'Document'),
            ('application/json', 'XHR'),
            ('text/css', 'Stylesheet'),
            ('application/javascript', 'Script'),
            ('text/javascript; charset=UTF-8', 'Script'),
            ('image/png', 'Image'),
            ('font/woff2', 'Font'),
            ('application/octet-stream', 'Other'),
            ('', 'Other'),
        ],
    )
    def test_mapping(self, content_type, expected):
        from scrapers.fbref.browser_manager import _content_type_to_resource_type

        assert _content_type_to_resource_type(content_type) == expected


def _make_scraper_stub():
    """Build a minimal FBrefScraper without running the heavy __init__.

    Only sets the attributes `_fetch_page_http` and `_record_http_diag` touch:
    `_http_session`, `_http_request_count`, `_http_cookies_time`, `_stats`.
    """
    from scrapers.fbref.scraper import FBrefScraper

    scraper = FBrefScraper.__new__(FBrefScraper)
    scraper._http_session = MagicMock()
    scraper._http_request_count = 0
    scraper._http_cookies_time = None
    # Issue #624: `_record_http_diag` now reads the current nodriver proxy
    # and the proxy the curl session was minted on. Mirror the real __init__
    # defaults so failure-path recording can't raise.
    scraper._nodriver_browser = None
    scraper._http_proxy_minted = None
    scraper._stats = {
        'http_bytes_downloaded': 0,
        'http_html_bytes_downloaded': 0,
        'http_requests_count': 0,
        'http_bytes_by_resource_type': {},
        'http_requests_by_resource_type': {},
    }
    return scraper


def _make_response(
    status_code: int,
    body: str,
    content_type: str = 'text/html',
    *,
    wire_overhead: int = 0,
):
    """Build a curl_cffi-like response mock."""
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    response.content = body.encode('utf-8')
    response.headers = {'content-type': content_type}
    response.download_size = len(response.content)
    response.header_size = wire_overhead
    response.request_size = 0
    response.upload_size = 0
    return response


class TestFetchPageHttpAudit:
    """`_fetch_page_http()` must always bump `http_*` counters per request."""

    @pytest.mark.unit
    def test_increments_counters_on_success(self):
        # Body must contain '<table' to pass the page-validation gate and return
        # the HTML rather than falling through to `incomplete_no_tables`.
        body = '<html><body><table id="x"></table></body></html>'
        scraper = _make_scraper_stub()
        scraper._http_session.get.return_value = _make_response(
            200, body, 'text/html; charset=utf-8'
        )

        result = scraper._fetch_page_http('https://fbref.com/test')

        assert result == body
        assert scraper._stats['http_requests_count'] == 1
        assert scraper._stats['http_bytes_downloaded'] == len(body)
        assert scraper._stats['http_bytes_by_resource_type'] == {
            'Document': len(body),
        }
        assert scraper._stats['http_html_bytes_downloaded'] == len(body)
        assert scraper._stats['http_requests_by_resource_type'] == {
            'Document': 1,
        }

    @pytest.mark.unit
    def test_increments_counters_on_non_200(self):
        # 403 returns None, but proxy already transferred bytes — they must
        # still be attributed so the audit reflects real quota usage.
        body = '<html>blocked</html>'
        scraper = _make_scraper_stub()
        scraper._http_session.get.return_value = _make_response(
            403, body, 'text/html'
        )

        result = scraper._fetch_page_http('https://fbref.com/test')

        assert result is None
        assert scraper._stats['http_requests_count'] == 1
        assert scraper._stats['http_bytes_downloaded'] == len(body)
        assert scraper._stats['http_bytes_by_resource_type'] == {
            'Document': len(body),
        }

    @pytest.mark.unit
    def test_counts_curl_wire_overhead_not_decoded_characters(self):
        body = '<html><body><table>Ł</table></body></html>'
        scraper = _make_scraper_stub()
        response = _make_response(200, body, wire_overhead=137)
        scraper._http_session.get.return_value = response

        assert scraper._fetch_page_http('https://fbref.com/test') == body
        assert scraper._stats['http_bytes_downloaded'] == (
            len(body.encode('utf-8')) + 137
        )
        assert scraper._stats['http_html_bytes_downloaded'] == len(
            body.encode('utf-8')
        )

    @pytest.mark.unit
    def test_accumulates_across_multiple_calls(self):
        body1 = '<html><body><table id="a"></table></body></html>'
        body2 = '<html><body><table id="b"></table></body></html>'
        scraper = _make_scraper_stub()
        scraper._http_session.get.side_effect = [
            _make_response(200, body1, 'text/html'),
            _make_response(200, body2, 'text/html'),
        ]

        scraper._fetch_page_http('https://fbref.com/a')
        scraper._fetch_page_http('https://fbref.com/b')

        assert scraper._stats['http_requests_count'] == 2
        assert (
            scraper._stats['http_bytes_downloaded']
            == len(body1) + len(body2)
        )
        assert scraper._stats['http_bytes_by_resource_type']['Document'] == (
            len(body1) + len(body2)
        )
        assert scraper._stats['http_requests_by_resource_type']['Document'] == 2


class TestHttpDiagProxyField:
    """Issue #624: `_record_http_diag` records the current nodriver proxy
    (sanitized) so proxy-mismatch fallbacks can be told apart from TLS/expiry."""

    @pytest.mark.unit
    def test_diag_records_none_proxy_without_browser(self):
        scraper = _make_scraper_stub()  # _nodriver_browser = None
        scraper._http_session.get.return_value = _make_response(
            403, '<html>blocked</html>'
        )

        scraper._fetch_page_http('https://fbref.com/test')

        diag = scraper._stats['http_fetch_diag']
        assert len(diag) == 1
        assert diag[0]['reason'] == 'non_200'
        assert diag[0]['proxy'] is None

    @pytest.mark.unit
    def test_diag_records_sanitized_proxy(self):
        scraper = _make_scraper_stub()
        browser = MagicMock()
        browser.proxy = 'http://user:secret@pool.example.io:10587'
        scraper._nodriver_browser = browser
        scraper._http_session.get.return_value = _make_response(
            403, '<html>blocked</html>'
        )

        scraper._fetch_page_http('https://fbref.com/test')

        diag = scraper._stats['http_fetch_diag']
        assert len(diag) == 1
        # Credentials must be stripped — host:port only.
        assert diag[0]['proxy'] == 'pool.example.io:10587'

    @pytest.mark.unit
    def test_diag_records_proxy_minted_matching_current(self):
        # No drift: the session was minted on the same proxy the browser is on
        # now — proxy == proxy_minted, so this is NOT a proxy-mismatch fallback.
        scraper = _make_scraper_stub()
        scraper._http_proxy_minted = 'pool.example.io:10587'
        browser = MagicMock()
        browser.proxy = 'http://user:secret@pool.example.io:10587'
        scraper._nodriver_browser = browser
        scraper._http_session.get.return_value = _make_response(
            403, '<html>blocked</html>'
        )

        scraper._fetch_page_http('https://fbref.com/test')

        diag = scraper._stats['http_fetch_diag']
        assert diag[0]['proxy_minted'] == 'pool.example.io:10587'
        assert diag[0]['proxy'] == diag[0]['proxy_minted']

    @pytest.mark.unit
    def test_diag_records_proxy_mismatch(self):
        # Drift: cf_clearance was minted on one proxy but the browser has since
        # rotated to another — proxy != proxy_minted flags an IP-bound mismatch
        # (not a TLS / cookie-expiry failure).
        scraper = _make_scraper_stub()
        scraper._http_proxy_minted = 'minted.example.io:1'
        browser = MagicMock()
        browser.proxy = 'http://user:secret@rotated.example.io:2'
        scraper._nodriver_browser = browser
        scraper._http_session.get.return_value = _make_response(
            403, '<html>blocked</html>'
        )

        scraper._fetch_page_http('https://fbref.com/test')

        diag = scraper._stats['http_fetch_diag']
        assert diag[0]['proxy'] == 'rotated.example.io:2'
        assert diag[0]['proxy_minted'] == 'minted.example.io:1'
        assert diag[0]['proxy'] != diag[0]['proxy_minted']


class TestEnvTunables:
    """Issue #624: cold-start / fallback thresholds are env-tunable so ops can
    dampen browser restart amplification on a bad-proxy day without a code
    change. `_env_num` reads the override, or falls back to the default."""

    @pytest.mark.unit
    def test_env_num_default_when_absent(self, monkeypatch):
        from scrapers.fbref.scraper import _env_num

        monkeypatch.delenv('FBREF_TEST_TUNABLE', raising=False)
        assert _env_num('FBREF_TEST_TUNABLE', 15.0, float) == 15.0

    @pytest.mark.unit
    def test_env_num_reads_override(self, monkeypatch):
        from scrapers.fbref.scraper import _env_num

        monkeypatch.setenv('FBREF_TEST_TUNABLE', '30')
        assert _env_num('FBREF_TEST_TUNABLE', 15.0, float) == 30.0
        monkeypatch.setenv('FBREF_TEST_RETRIES', '8')
        assert _env_num('FBREF_TEST_RETRIES', 5, int) == 8

    @pytest.mark.unit
    def test_env_num_falls_back_on_invalid_or_empty(self, monkeypatch):
        from scrapers.fbref.scraper import _env_num

        monkeypatch.setenv('FBREF_TEST_TUNABLE', 'not-a-number')
        assert _env_num('FBREF_TEST_TUNABLE', 15.0, float) == 15.0
        monkeypatch.setenv('FBREF_TEST_TUNABLE', '')
        assert _env_num('FBREF_TEST_TUNABLE', 15.0, float) == 15.0


# Body must contain '<table' and no CF keywords to pass the `_fetch_page`
# validation gates and be returned as a successful (non-match) page.
_VALID_HTML = '<html><body><table id="x">' + 'x' * 200 + '</table></body></html>'


def _make_fetch_stub():
    """Stub for driving `_fetch_page` orchestration (issue #624 decouple).

    `_fetch_page` is the main fetch loop; here every heavy collaborator
    (`_fetch_page_http`/`_fetch_page_nodriver`, restart, re-mint, proxy
    manager) is mocked and only the branching state the loop reads is set.
    Mirrors the real __init__ defaults for the #624 knobs.
    """
    from scrapers.fbref.scraper import FBrefScraper

    scraper = FBrefScraper.__new__(FBrefScraper)
    scraper.use_nodriver = True
    scraper._page_cache = {}
    scraper._rate_limiter = MagicMock()
    scraper._proxy_manager = None
    scraper._current_proxy_obj = None
    scraper._consecutive_fetch_failures = 0
    scraper.MAX_SLOW_PROXY_RETRIES = 1
    scraper.MAX_CONSECUTIVE_FAILURES = 15
    # HTTP fast-path state (#624)
    scraper._http_session = MagicMock(name='http_session')
    scraper._http_cookies_expired = MagicMock(return_value=False)
    scraper._http_proxy_minted = 'minted.example.io:1'
    scraper._http_consecutive_fallbacks = 0
    scraper.HTTP_MAX_FALLBACKS_BEFORE_REMINT = 2
    scraper.RESET_HTTP_ON_RESTART = 0
    scraper._stats = {
        'failures': 0,
        'successes': 0,
        'bytes_downloaded': 0,
        'http_fetch_ok': 0,
        'http_fetch_fallback': 0,
    }
    # Mock collaborators so only the orchestration under test executes.
    scraper._fetch_page_http = MagicMock(return_value=None)
    scraper._fetch_page_nodriver = MagicMock(return_value=_VALID_HTML)
    scraper._fetch_page_selenium = MagicMock(return_value=_VALID_HTML)
    scraper._track_download = MagicMock()
    scraper._manage_cache_size = MagicMock()
    scraper._maybe_restart_browser = MagicMock()
    scraper._try_init_http_session = MagicMock()
    scraper._try_change_proxy_nodriver = MagicMock(return_value=False)
    scraper._close_browser = MagicMock()
    return scraper


class TestColdStartDecouple:
    """Issue #624: a slow/dead *nodriver* proxy must NOT drop the *curl* fast-path
    session — it is bound to its own proxy and survives browser restarts. The
    restart sites pass `reset_http=False` by default; FBREF_RESET_HTTP_ON_RESTART
    restores the old reset-on-restart behaviour."""

    @pytest.mark.unit
    def test_slow_proxy_restart_keeps_http_session(self):
        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        scraper = _make_fetch_stub()
        scraper._fetch_page_nodriver = MagicMock(side_effect=SlowProxyError('slow'))

        result = scraper._fetch_page('https://fbref.com/x', page_type='other')

        assert result is None
        scraper._close_browser.assert_called_once_with(
            reset_http=False, reason='slow_proxy'
        )

    @pytest.mark.unit
    def test_consecutive_failures_restart_keeps_http_session(self):
        scraper = _make_fetch_stub()
        scraper.MAX_CONSECUTIVE_FAILURES = 1
        scraper._fetch_page_nodriver = MagicMock(side_effect=RuntimeError('boom'))

        result = scraper._fetch_page('https://fbref.com/x', page_type='other')

        assert result is None
        scraper._close_browser.assert_called_once_with(
            reset_http=False, reason='consecutive_failures'
        )

    @pytest.mark.unit
    def test_env_hatch_restores_reset_on_slow_proxy(self):
        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        scraper = _make_fetch_stub()
        scraper.RESET_HTTP_ON_RESTART = 1  # opt back into old reset-on-restart
        scraper._fetch_page_nodriver = MagicMock(side_effect=SlowProxyError('slow'))

        scraper._fetch_page('https://fbref.com/x', page_type='other')

        scraper._close_browser.assert_called_once_with(
            reset_http=True, reason='slow_proxy'
        )

    @pytest.mark.unit
    def test_warm_path_never_drops_session_or_restarts(self):
        scraper = _make_fetch_stub()
        sentinel = scraper._http_session
        scraper._fetch_page_http = MagicMock(return_value=_VALID_HTML)  # fast-path hit

        for i in range(5):
            scraper._page_cache.clear()
            assert (
                scraper._fetch_page(f'https://fbref.com/{i}', page_type='other')
                == _VALID_HTML
            )

        assert scraper._stats['http_fetch_ok'] == 5
        assert scraper._stats['http_fetch_fallback'] == 0
        assert scraper._http_consecutive_fallbacks == 0
        assert scraper._http_session is sentinel
        scraper._close_browser.assert_not_called()
        scraper._try_init_http_session.assert_not_called()


class TestFallbackRemintCounter:
    """Issue #624: the curl path never reports proxy failures to the proxy-manager,
    so a dead pinned proxy is bounded by a fallback counter — after N consecutive
    fallbacks the session is dropped to re-mint on the next nodriver fetch. The
    counter resets on any fast-path success."""

    @pytest.mark.unit
    def test_counter_increments_below_threshold(self):
        scraper = _make_fetch_stub()
        sentinel = scraper._http_session
        # fast-path miss (stub default), nodriver succeeds → counter +1, no drop

        result = scraper._fetch_page('https://fbref.com/x', page_type='other')

        assert result == _VALID_HTML
        assert scraper._stats['http_fetch_fallback'] == 1
        assert scraper._http_consecutive_fallbacks == 1
        assert scraper._http_session is sentinel  # below threshold → not dropped
        scraper._try_init_http_session.assert_not_called()

    @pytest.mark.unit
    def test_counter_resets_on_fast_path_success(self):
        scraper = _make_fetch_stub()
        scraper._http_consecutive_fallbacks = 3  # pre-existing streak
        scraper._fetch_page_http = MagicMock(return_value=_VALID_HTML)  # hit

        result = scraper._fetch_page('https://fbref.com/x', page_type='other')

        assert result == _VALID_HTML
        assert scraper._stats['http_fetch_ok'] == 1
        assert scraper._http_consecutive_fallbacks == 0

    @pytest.mark.unit
    def test_remint_fires_at_threshold(self):
        scraper = _make_fetch_stub()
        scraper.HTTP_MAX_FALLBACKS_BEFORE_REMINT = 2
        scraper._http_consecutive_fallbacks = 1  # one miss away from threshold

        result = scraper._fetch_page('https://fbref.com/x', page_type='other')

        assert result == _VALID_HTML
        # at threshold the curl session is dropped + counter reset; the nodriver
        # success then re-mints via the lazy-init hook.
        assert scraper._http_consecutive_fallbacks == 0
        assert scraper._http_proxy_minted is None
        scraper._try_init_http_session.assert_called_once()


class TestRemintTunableDefaults:
    """Issue #624: the new re-mint / rollback knobs default to the documented
    values (re-mint after 2 fallbacks; rollback hatch off)."""

    @pytest.mark.unit
    def test_defaults_when_env_absent(self, monkeypatch):
        from scrapers.fbref.scraper import _env_num

        monkeypatch.delenv('FBREF_HTTP_MAX_FALLBACKS_BEFORE_REMINT', raising=False)
        monkeypatch.delenv('FBREF_RESET_HTTP_ON_RESTART', raising=False)
        assert _env_num('FBREF_HTTP_MAX_FALLBACKS_BEFORE_REMINT', 2, int) == 2
        assert _env_num('FBREF_RESET_HTTP_ON_RESTART', 0, int) == 0

    @pytest.mark.unit
    def test_overrides_applied(self, monkeypatch):
        from scrapers.fbref.scraper import _env_num

        monkeypatch.setenv('FBREF_HTTP_MAX_FALLBACKS_BEFORE_REMINT', '4')
        monkeypatch.setenv('FBREF_RESET_HTTP_ON_RESTART', '1')
        assert _env_num('FBREF_HTTP_MAX_FALLBACKS_BEFORE_REMINT', 2, int) == 4
        assert _env_num('FBREF_RESET_HTTP_ON_RESTART', 0, int) == 1
