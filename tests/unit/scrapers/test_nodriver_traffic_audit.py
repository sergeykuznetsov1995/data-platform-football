"""
Unit tests for NodriverBypass traffic-audit instrumentation (issue #44).

Covers:
- Counter initialisation in __init__.
- get_real_traffic_stats() shape (per-resource-type / CF / restart).
- restart_browser(reason=...) / close_sync(reason=...) increment.
"""

import asyncio

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
