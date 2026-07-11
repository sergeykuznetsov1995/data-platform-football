"""SofaScore Camoufox traffic diagnostics (issue #879).

Provider-authoritative budget accounting belongs to the common capture engine;
this compatibility counter covers browser rx+tx and operation counts only.
"""

from unittest.mock import MagicMock, patch

import pytest

from scrapers.sofascore import SofaScoreScraper


@pytest.fixture
def scraper():
    return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2025])


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
