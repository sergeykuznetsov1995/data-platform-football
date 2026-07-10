"""SofaScore residential-proxy byte counter (issue #789 Phase 2 + #879).

Counts response-body bytes of the tls REST path (`_fetch_json_endpoint`) per
host, plus — since #879 — the rx+tx bytes of each Camoufox capture session,
folded in at session teardown by `_camoufox_session`. The counter must count
every status, aggregate by host, and never raise on a malformed response.
"""

from unittest.mock import MagicMock, patch

import pytest

from scrapers.sofascore import SofaScoreScraper


class _FakeResp:
    def __init__(self, body: bytes, status: int = 200):
        self.content = body
        self.status_code = status


@pytest.fixture
def scraper():
    return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])


@pytest.mark.unit
def test_record_proxy_bytes_accumulates_by_host(scraper):
    scraper._record_proxy_bytes('https://api.sofascore.com/a', _FakeResp(b'a' * 1000))
    scraper._record_proxy_bytes('https://api.sofascore.com/b', _FakeResp(b'b' * 2000))

    stats = scraper.get_traffic_stats()

    assert stats['proxy_response_bytes'] == 3000
    assert stats['proxy_response_mb'] == round(3000 / 1024 / 1024, 4)
    assert stats['top_traffic_urls'][0] == {
        'url': 'api.sofascore.com',
        'bytes': 3000,
        'mb': round(3000 / 1024 / 1024, 4),
    }


@pytest.mark.unit
def test_counts_error_pages_too(scraper):
    scraper._record_proxy_bytes(
        'https://api.sofascore.com/z', _FakeResp(b'x' * 500, status=403)
    )

    assert scraper.get_traffic_stats()['proxy_response_bytes'] == 500


@pytest.mark.unit
def test_counter_never_raises_on_bad_response(scraper):
    class _Bad:
        @property
        def content(self):
            raise RuntimeError('stream gone')

    scraper._record_proxy_bytes('https://x', _Bad())  # must not raise

    assert scraper.get_traffic_stats()['proxy_response_bytes'] == 0


@pytest.mark.unit
def test_zero_traffic_shape(scraper):
    assert scraper.get_traffic_stats() == {
        'proxy_response_bytes': 0,
        'proxy_response_mb': 0.0,
        'camoufox_bytes': 0,
        'camoufox_mb': 0.0,
        'browser_sessions': 0,
        'browser_navigations': 0,
        'browser_api_fetches': 0,
        'browser_fallback_navigations': 0,
        'browser_blocked_requests': 0,
        'requests': 0,
        'top_traffic_urls': [],
    }


@pytest.mark.unit
def test_camoufox_session_bytes_fold_into_totals(scraper):
    # #879: a capture session's rx+tx bytes must land in BOTH the camoufox
    # breakdown and the proxy_response totals (they are billable residential
    # bytes), plus a per-host row for the domain breakdown.
    fake = MagicMock()
    fake.__enter__.return_value._bytes_total = 3 * 1024 * 1024

    with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
               return_value=fake):
        with scraper._camoufox_session(None):
            pass

    stats = scraper.get_traffic_stats()
    assert stats['camoufox_bytes'] == 3 * 1024 * 1024
    assert stats['camoufox_mb'] == 3.0
    assert stats['browser_sessions'] == 1
    assert stats['proxy_response_bytes'] == 3 * 1024 * 1024
    assert stats['top_traffic_urls'][0]['url'] == 'camoufox:www.sofascore.com'


@pytest.mark.unit
def test_camoufox_operation_counters_fold_into_stats(scraper):
    fake = MagicMock()
    entered = fake.__enter__.return_value
    entered._bytes_total = 0
    entered._navigation_count = 1
    entered._api_fetch_count = 7
    entered._blocked_count = 12

    with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
               return_value=fake):
        with scraper._camoufox_session(None):
            pass

    stats = scraper.get_traffic_stats()
    assert stats['browser_sessions'] == 1
    assert stats['browser_navigations'] == 1
    assert stats['browser_api_fetches'] == 7
    assert stats['browser_blocked_requests'] == 12


@pytest.mark.unit
def test_camoufox_bytes_sum_with_tls_bytes(scraper):
    fake = MagicMock()
    fake.__enter__.return_value._bytes_total = 2 * 1024 * 1024
    scraper._record_proxy_bytes('https://api.sofascore.com/a', _FakeResp(b'a' * 1000))

    with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
               return_value=fake):
        with scraper._camoufox_session(None):
            pass

    stats = scraper.get_traffic_stats()
    assert stats['proxy_response_bytes'] == 2 * 1024 * 1024 + 1000
    assert stats['camoufox_bytes'] == 2 * 1024 * 1024


@pytest.mark.unit
def test_camoufox_bytes_folded_even_when_session_body_raises(scraper):
    # The accumulation lives in finally — a crash inside the session must not
    # lose the bytes already spent through the proxy.
    fake = MagicMock()
    fake.__enter__.return_value._bytes_total = 1024

    with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
               return_value=fake):
        with pytest.raises(RuntimeError):
            with scraper._camoufox_session(None):
                raise RuntimeError('mid-session crash')

    assert scraper.get_traffic_stats()['camoufox_bytes'] == 1024


@pytest.mark.unit
def test_camoufox_fake_without_counter_is_ignored(scraper):
    # Test fakes (and a session that failed to start) have no _bytes_total —
    # the fold must silently count zero, never raise.
    class _Bare:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    with patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture',
               return_value=_Bare()):
        with scraper._camoufox_session(None):
            pass

    assert scraper.get_traffic_stats()['camoufox_bytes'] == 0
