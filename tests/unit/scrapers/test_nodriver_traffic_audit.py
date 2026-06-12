"""
Unit tests for NodriverBypass traffic-audit instrumentation (issue #44).

Covers:
- Counter initialisation in __init__.
- get_real_traffic_stats() shape (per-resource-type / CF / restart).
- restart_browser(reason=...) / close_sync(reason=...) increment.
- Resource-type detection via CDP requestWillBeSent/responseReceived/
  loadingFinished events (issue #116 fix).
"""

import asyncio
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestNodriverTrafficCountersInit:
    """The new counters introduced for issue #44 must be zero-initialised."""

    @pytest.mark.unit
    def test_init_zero_counters(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()

        assert bypass._real_bytes_downloaded == 0
        assert bypass._real_requests_count == 0
        # New issue-#44 fields
        assert dict(bypass._real_bytes_by_resource_type) == {}
        assert dict(bypass._real_requests_by_resource_type) == {}
        assert bypass._request_resource_types == {}
        assert bypass._cf_challenge_attempts == 0
        assert bypass._cf_challenges_passed == 0
        assert bypass._cf_challenges_failed == 0
        assert dict(bypass._restart_reasons) == {}


class TestGetRealTrafficStatsShape:
    """get_real_traffic_stats() must surface all audit fields."""

    @pytest.mark.unit
    def test_returns_extended_dict(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        # Simulate observed traffic.
        bypass._real_bytes_downloaded = 1024
        bypass._real_requests_count = 3
        bypass._real_bytes_by_resource_type['Document'] = 700
        bypass._real_bytes_by_resource_type['Script'] = 324
        bypass._real_requests_by_resource_type['Document'] = 1
        bypass._real_requests_by_resource_type['Script'] = 2
        bypass._cf_challenge_attempts = 4
        bypass._cf_challenges_passed = 3
        bypass._cf_challenges_failed = 1
        bypass._restart_reasons['slow_proxy'] = 2
        bypass._restart_reasons['page_limit'] = 1

        stats = bypass.get_real_traffic_stats()

        assert stats['real_bytes_downloaded'] == 1024
        assert stats['real_requests_count'] == 3
        assert stats['real_bytes_by_resource_type'] == {
            'Document': 700, 'Script': 324,
        }
        assert stats['real_requests_by_resource_type'] == {
            'Document': 1, 'Script': 2,
        }
        assert stats['cf_challenge_attempts'] == 4
        assert stats['cf_challenges_passed'] == 3
        assert stats['cf_challenges_failed'] == 1
        assert stats['restart_reasons'] == {
            'slow_proxy': 2, 'page_limit': 1,
        }


class TestRestartReasonPropagation:
    """restart_browser(reason='X') must bump the counter on close()."""

    @pytest.mark.unit
    def test_close_increments_restart_reason_when_browser_present(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        # Fake a live browser so close() increments and then runs teardown.
        fake_browser = MagicMock()
        fake_browser.connection = None
        fake_browser._process = None
        bypass._browser = fake_browser

        async def _run():
            await bypass.close(reason='slow_proxy')

        asyncio.run(_run())

        assert dict(bypass._restart_reasons) == {'slow_proxy': 1}

    @pytest.mark.unit
    def test_close_skips_counter_when_browser_already_none(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        bypass._browser = None  # already torn down

        async def _run():
            await bypass.close(reason='slow_proxy')

        asyncio.run(_run())

        # No browser to close → no reason recorded (avoids double counting
        # when context manager fires close() after an explicit close_sync).
        assert dict(bypass._restart_reasons) == {}

    @pytest.mark.unit
    def test_restart_browser_propagates_reason(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        fake_browser = MagicMock()
        fake_browser.connection = None
        fake_browser._process = None
        bypass._browser = fake_browser

        bypass.restart_browser(reason='consecutive_failures')

        assert dict(bypass._restart_reasons) == {'consecutive_failures': 1}

    @pytest.mark.unit
    def test_close_sync_propagates_reason(self):
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        fake_browser = MagicMock()
        fake_browser.connection = None
        fake_browser._process = None
        bypass._browser = fake_browser

        bypass.close_sync(reason='page_limit')

        assert dict(bypass._restart_reasons) == {'page_limit': 1}


class _FakeType:
    """Mimics nodriver ResourceType enum — only `.name` is read by handlers."""

    def __init__(self, name):
        self.name = name


def _evt(**kw):
    """Build a duck-typed CDP event (handlers use getattr exclusively)."""
    return SimpleNamespace(**kw)


def _setup_handlers():
    """Drive _enable_network_tracking() against a mocked Page and return
    the three captured callbacks keyed by CDP event class name."""
    from scrapers.base.browser.nodriver_bypass import NodriverBypass

    bypass = NodriverBypass()
    bypass._page = MagicMock()
    bypass._page.send = AsyncMock()

    captured: dict = {}

    def fake_add_handler(event_cls, fn):
        captured[event_cls.__name__] = fn

    bypass._page.add_handler = fake_add_handler

    asyncio.run(bypass._enable_network_tracking())
    return bypass, captured


class TestResourceTypeDetection:
    """Issue #116: _real_bytes_by_resource_type must break down traffic
    by Document/Script/XHR/... instead of dumping everything into 'Other'."""

    @pytest.mark.unit
    def test_request_will_be_sent_fills_cache_before_loading_finished(self):
        bypass, h = _setup_handlers()

        h['RequestWillBeSent'](_evt(request_id='R1', type_=_FakeType('Document')))
        h['LoadingFinished'](_evt(request_id='R1', encoded_data_length=1000))

        assert dict(bypass._real_bytes_by_resource_type) == {'Document': 1000}
        assert dict(bypass._real_requests_by_resource_type) == {'Document': 1}
        assert bypass._resource_type_cache_misses == 0

    @pytest.mark.unit
    def test_loading_finished_without_request_misses_to_other(self):
        bypass, h = _setup_handlers()

        h['LoadingFinished'](_evt(request_id='R2', encoded_data_length=500))

        assert dict(bypass._real_bytes_by_resource_type) == {'Other': 500}
        assert bypass._resource_type_cache_misses == 1

    @pytest.mark.unit
    def test_response_received_overrides_request_will_be_sent(self):
        # Redirect chains: requestWillBeSent reports Document, but the final
        # response carries XHR. Last writer must win.
        bypass, h = _setup_handlers()

        h['RequestWillBeSent'](_evt(request_id='R3', type_=_FakeType('Document')))
        h['ResponseReceived'](_evt(request_id='R3', type_=_FakeType('XHR')))
        h['LoadingFinished'](_evt(request_id='R3', encoded_data_length=200))

        assert dict(bypass._real_bytes_by_resource_type) == {'XHR': 200}
        assert bypass._resource_type_cache_misses == 0

    @pytest.mark.unit
    def test_enum_normalisation_strips_dotted_path(self):
        # Cover both shapes nodriver may emit: enum-with-.name and bare
        # 'ResourceType.X' string.
        bypass, h = _setup_handlers()

        h['RequestWillBeSent'](_evt(request_id='R4', type_=_FakeType('Script')))
        h['LoadingFinished'](_evt(request_id='R4', encoded_data_length=10))

        h['RequestWillBeSent'](_evt(request_id='R5', type_='ResourceType.Stylesheet'))
        h['LoadingFinished'](_evt(request_id='R5', encoded_data_length=20))

        assert dict(bypass._real_bytes_by_resource_type) == {
            'Script': 10,
            'Stylesheet': 20,
        }

    @pytest.mark.unit
    def test_cache_miss_counter_published_in_stats(self):
        bypass, h = _setup_handlers()

        h['RequestWillBeSent'](_evt(request_id='R6', type_=_FakeType('Document')))
        h['LoadingFinished'](_evt(request_id='R6', encoded_data_length=100))
        h['LoadingFinished'](_evt(request_id='R7', encoded_data_length=50))  # miss

        stats = bypass.get_real_traffic_stats()
        assert stats['resource_type_cache_misses'] == 1
        assert stats['real_bytes_by_resource_type'] == {'Document': 100, 'Other': 50}


# Mock IcebergWriter at module level to prevent Trino connection attempts
# during BaseScraper.__init__() (same pattern as test_nodriver_fbref_scraper).
ICEBERG_WRITER_PATCH = 'scrapers.base.base_scraper.IcebergWriter'


def _nonzero_traffic_stats() -> dict:
    """Realistic non-zero NodriverBypass.get_real_traffic_stats() payload."""
    return {
        'real_bytes_downloaded': 1_500_000,
        'real_requests_count': 12,
        'real_bytes_by_resource_type': {'Document': 1_200_000, 'Script': 300_000},
        'real_requests_by_resource_type': {'Document': 1, 'Script': 11},
        'resource_type_cache_misses': 2,
        'cf_challenge_attempts': 3,
        'cf_challenges_passed': 2,
        'cf_challenges_failed': 1,
        'restart_reasons': {'page_limit': 1},
    }


class TestNodriverFBrefTrafficWiring:
    """Issue #131: NodriverFBrefScraper must flush NodriverBypass traffic
    counters into _stats so the runner diagnostics report non-zero MB on the
    production DAG path (previously always 0)."""

    @pytest.mark.unit
    @patch(ICEBERG_WRITER_PATCH)
    def test_update_real_traffic_stats_reads_live_browser(self, mock_iceberg):
        from scrapers.nodriver_fbref import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        scraper._browser = MagicMock()
        scraper._browser.get_real_traffic_stats.return_value = _nonzero_traffic_stats()

        scraper._update_real_traffic_stats()

        assert scraper._stats['real_bytes_downloaded'] == 1_500_000
        assert scraper._stats['real_requests_count'] == 12
        assert scraper._stats['real_bytes_by_resource_type'] == {
            'Document': 1_200_000, 'Script': 300_000,
        }
        assert scraper._stats['cf_challenge_attempts'] == 3
        assert scraper._stats['cf_challenges_passed'] == 2
        assert scraper._stats['cf_challenges_failed'] == 1
        assert scraper._stats['restart_reasons'] == {'page_limit': 1}
        assert scraper._stats['resource_type_cache_misses'] == 2

    @pytest.mark.unit
    @patch(ICEBERG_WRITER_PATCH)
    def test_end_to_end_diagnostics_nonzero(self, mock_iceberg):
        # Full wiring: browser counters → _update_real_traffic_stats() (called
        # by the runner via hasattr) → _stats → _get_traffic_diagnostics().
        from dags.scripts.run_fbref_scraper import _get_traffic_diagnostics
        from scrapers.nodriver_fbref import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        scraper._browser = MagicMock()
        scraper._browser.get_real_traffic_stats.return_value = _nonzero_traffic_stats()

        result = _get_traffic_diagnostics(scraper)

        assert result['real_proxy_mb'] > 0
        assert result['real_proxy_bytes'] == 1_500_000
        assert result['real_proxy_requests'] == 12
        assert result['cf_challenge_attempts'] == 3
        assert result['real_proxy_mb_by_resource_type']  # non-empty breakdown

    @pytest.mark.unit
    @patch(ICEBERG_WRITER_PATCH)
    def test_close_browser_flushes_to_base(self, mock_iceberg):
        from scrapers.nodriver_fbref import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        scraper._browser = MagicMock()
        scraper._browser.get_real_traffic_stats.return_value = _nonzero_traffic_stats()

        # Closing the browser must flush counters into the persistent base so
        # they survive the teardown (mid-scrape restart scenario).
        scraper._close_browser()
        assert scraper._browser is None

        # With no live browser, a fresh snapshot still reports the base totals.
        scraper._update_real_traffic_stats()
        assert scraper._stats['real_bytes_downloaded'] == 1_500_000
        assert scraper._stats['cf_challenge_attempts'] == 3
        assert scraper._stats['restart_reasons'] == {'page_limit': 1}
