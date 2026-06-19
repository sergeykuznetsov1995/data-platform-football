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
        'http_requests_count': 0,
        'http_bytes_by_resource_type': {},
        'http_requests_by_resource_type': {},
    }
    return scraper


def _make_response(status_code: int, body: str, content_type: str = 'text/html'):
    """Build a curl_cffi-like response mock."""
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    response.headers = {'content-type': content_type}
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
