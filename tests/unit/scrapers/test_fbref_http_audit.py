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
