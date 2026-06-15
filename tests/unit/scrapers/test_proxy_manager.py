"""
Tests for ProxyManager utility.
"""

import pytest
from unittest.mock import MagicMock, patch
import tempfile
import os
import time

from scrapers.utils.proxy_manager import (
    Proxy,
    ProxyType,
    ProxyManager,
    ProxyManagerConfig,
    ErrorType,
    create_proxy_manager,
    classify_error,
)


class TestProxy:
    """Tests for Proxy dataclass."""

    def test_default_proxy(self):
        proxy = Proxy(host='127.0.0.1', port=8080)
        assert proxy.host == '127.0.0.1'
        assert proxy.port == 8080
        assert proxy.proxy_type == ProxyType.HTTP

    def test_proxy_url(self):
        proxy = Proxy(host='proxy.example.com', port=8080)
        assert proxy.url == 'http://proxy.example.com:8080'

    def test_proxy_url_with_auth(self):
        proxy = Proxy(
            host='proxy.example.com',
            port=8080,
            username='user',
            password='pass'
        )
        assert proxy.url == 'http://user:pass@proxy.example.com:8080'

    def test_socks5_proxy_url(self):
        proxy = Proxy(
            host='127.0.0.1',
            port=9050,
            proxy_type=ProxyType.SOCKS5
        )
        assert proxy.url == 'socks5://127.0.0.1:9050'

    def test_requests_proxies(self):
        proxy = Proxy(host='proxy.example.com', port=8080)
        proxies = proxy.requests_proxies
        assert 'http' in proxies
        assert 'https' in proxies
        assert proxies['http'] == 'http://proxy.example.com:8080'

    def test_selenium_args(self):
        proxy = Proxy(host='proxy.example.com', port=8080)
        args = proxy.selenium_args
        assert '--proxy-server=http://proxy.example.com:8080' in args

    def test_success_rate(self):
        proxy = Proxy(host='test', port=8080)
        assert proxy.success_rate == 1.0  # No requests yet

        proxy.success_count = 8
        proxy.failure_count = 2
        assert proxy.success_rate == 0.8

    def test_record_success(self):
        proxy = Proxy(host='test', port=8080)
        proxy.record_success()
        assert proxy.success_count == 1
        assert proxy.last_used > 0

    def test_record_failure(self):
        proxy = Proxy(host='test', port=8080)
        proxy.record_failure()
        assert proxy.failure_count == 1

    def test_record_failure_with_error_type(self):
        """Test recording failure with error type classification."""
        proxy = Proxy(host='test', port=8080)
        proxy.record_failure(error_type='rate_limit')
        proxy.record_failure(error_type='rate_limit')
        proxy.record_failure(error_type='cloudflare')

        assert proxy.failure_count == 3
        assert proxy.error_counts['rate_limit'] == 2
        assert proxy.error_counts['cloudflare'] == 1

    def test_record_response_time(self):
        """Test recording response time for performance tracking."""
        proxy = Proxy(host='test', port=8080)
        proxy.record_response_time(1.5)
        proxy.record_response_time(2.0)
        proxy.record_response_time(2.5)

        assert len(proxy.response_times) == 3
        assert proxy.avg_response_time == 2.0

    def test_get_error_summary(self):
        """Test error summary generation."""
        proxy = Proxy(host='test', port=8080)
        assert proxy.get_error_summary() == "no errors"

        proxy.record_failure(error_type='forbidden')
        proxy.record_failure(error_type='forbidden')
        proxy.record_failure(error_type='cloudflare')

        summary = proxy.get_error_summary()
        assert 'forbidden:2' in summary
        assert 'cloudflare:1' in summary

    def test_mark_banned(self):
        proxy = Proxy(host='test', port=8080)
        assert proxy.is_banned is False
        proxy.mark_banned()
        assert proxy.is_banned is True

    def test_mark_banned_records_timestamp(self):
        """mark_banned() stamps banned_at; a later success clears it (#552)."""
        proxy = Proxy(host='test', port=8080)
        assert proxy.banned_at is None

        proxy.mark_banned()
        assert proxy.banned_at is not None

        proxy.record_success()
        assert proxy.is_banned is False
        assert proxy.banned_at is None


class TestProxyManager:
    """Tests for ProxyManager."""

    def test_init(self):
        manager = ProxyManager()
        assert manager.total_count == 0

    def test_add_proxy(self):
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        assert manager.total_count == 1

    def test_add_proxy_url(self):
        manager = ProxyManager()
        manager.add_proxy_url('http://user:pass@proxy.example.com:8080')
        assert manager.total_count == 1

    def test_add_proxy_url_socks5(self):
        manager = ProxyManager()
        manager.add_proxy_url('socks5://127.0.0.1:9050')
        assert manager.total_count == 1

    def test_get_proxy_round_robin(self):
        manager = ProxyManager(rotation_strategy='round_robin')
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        proxy1 = manager.get_proxy()
        proxy2 = manager.get_proxy()

        assert proxy1.host == 'proxy1.example.com'
        assert proxy2.host == 'proxy2.example.com'

    def test_get_proxy_random(self):
        manager = ProxyManager(rotation_strategy='random')
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        proxy = manager.get_proxy()
        assert proxy is not None

    def test_get_proxy_no_proxies(self):
        manager = ProxyManager()
        proxy = manager.get_proxy()
        assert proxy is None

    def test_record_result_success(self):
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        manager.record_result(proxy, success=True)

        assert proxy.success_count == 1

    def test_record_result_failure_bans_proxy(self):
        manager = ProxyManager()
        manager.config.ban_threshold = 3
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()

        # Record failures
        for _ in range(3):
            manager.record_result(proxy, success=False)

        assert proxy.is_banned is True

    def test_record_result_single_failure_does_not_ban_fresh_proxy(self):
        """A fresh proxy must survive ONE transient failure.

        Regression for #470 bug 1: success_rate = 0/1 = 0 < min_success_rate(0.5)
        used to permaban on the very first failure, making ban_threshold dead
        code and letting a single transient site outage wipe the whole pool in
        one rotation pass.
        """
        manager = ProxyManager()  # defaults: ban_threshold=5, min_success_rate=0.5
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        manager.record_result(proxy, success=False)  # single transient failure

        assert proxy.is_banned is False
        assert proxy.failure_count == 1

    def test_record_result_success_rate_ban_after_min_attempts(self):
        """Once a proxy reaches ban_threshold attempts, a sub-threshold success
        rate still bans it — the guard only suppresses *premature* bans."""
        manager = ProxyManager()
        manager.config.ban_threshold = 5
        manager.config.min_success_rate = 0.5
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        # 2 successes, then 3 failures: 5 attempts, rate 2/5=0.4 < 0.5, but only
        # 3 consecutive failures (< ban_threshold) so the consecutive path is idle.
        manager.record_result(proxy, success=True)
        manager.record_result(proxy, success=True)
        for _ in range(3):
            manager.record_result(proxy, success=False)

        assert proxy.is_banned is True

    def test_available_count(self):
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        assert manager.available_count == 2
        assert manager.total_count == 2

        # Ban one
        proxy = manager._proxies[0]
        proxy.mark_banned()

        assert manager.available_count == 1
        assert manager.total_count == 2

    def test_unban_all(self):
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        # Ban all
        for proxy in manager._proxies:
            proxy.mark_banned()

        assert manager.available_count == 0

        manager.unban_all()
        assert manager.available_count == 2

    def test_get_stats(self):
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)

        stats = manager.get_stats()
        assert 'total' in stats
        assert 'available' in stats
        assert 'proxies' in stats
        assert 'error_type_totals' in stats
        assert 'in_cooldown' in stats

    def test_cooldown_respected(self):
        """Test that cooldown is respected between proxy uses."""
        manager = ProxyManager(cooldown_seconds=0.5)  # Short cooldown for test
        manager.add_proxy('proxy1.example.com', 8080)

        proxy1 = manager.get_proxy()
        proxy1.last_used = time.time()

        # Immediately try to get proxy - should still return same one (only one available)
        proxy2 = manager.get_proxy(respect_cooldown=True)
        assert proxy2.host == proxy1.host

        # Wait for cooldown
        time.sleep(0.6)
        proxy3 = manager.get_proxy(respect_cooldown=True)
        assert proxy3.host == proxy1.host

    def test_cooldown_multiple_proxies(self):
        """Test cooldown with multiple proxies."""
        manager = ProxyManager(cooldown_seconds=1.0)
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        # Get first proxy and mark as used
        proxy1 = manager.get_proxy(respect_cooldown=True)
        proxy1.last_used = time.time()

        # Get second proxy (should be different because first is in cooldown)
        proxy2 = manager.get_proxy(respect_cooldown=True)
        assert proxy2.host != proxy1.host

    def test_record_result_with_error_type(self):
        """Test recording result with error type classification."""
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        manager.record_result(proxy, success=False, error_type='rate_limit')
        manager.record_result(proxy, success=False, error_type='cloudflare')

        assert proxy.error_counts['rate_limit'] == 1
        assert proxy.error_counts['cloudflare'] == 1

    def test_cloudflare_ban_threshold(self):
        """Test that proxy is banned quickly for Cloudflare blocks."""
        manager = ProxyManager()
        manager.config.cloudflare_ban_threshold = 2
        manager.config.min_success_rate = 0.0  # Disable success rate banning for this test
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()

        # First Cloudflare failure - should not trigger ban yet
        manager.record_result(proxy, success=False, error_type='cloudflare')
        # Note: After first failure, success_rate drops to 0 which would trigger ban
        # So we need to set min_success_rate to 0 to test cloudflare_ban_threshold

        # Second Cloudflare failure - should trigger ban
        manager.record_result(proxy, success=False, error_type='cloudflare')
        assert proxy.is_banned is True
        assert proxy.error_counts.get('cloudflare', 0) == 2

    def test_banned_proxy_auto_unbanned_after_cooldown(self):
        """A banned proxy returns to the pool once its cooldown elapses (#552).

        banned_at is set into the past to simulate elapsed time — no real
        sleep / freezegun, mirroring how the cooldown tests set last_used.
        """
        manager = ProxyManager(unban_cooldown_seconds=600.0)
        manager.add_proxy('proxy1.example.com', 8080)
        proxy = manager._proxies[0]

        proxy.mark_banned()
        proxy.banned_at = time.time() - 601.0  # cooldown elapsed

        returned = manager.get_proxy()
        assert returned is proxy
        assert proxy.is_banned is False
        assert proxy.banned_at is None

    def test_banned_proxy_stays_banned_within_cooldown(self):
        """A banned proxy is NOT returned before its cooldown elapses (#552)."""
        manager = ProxyManager(unban_cooldown_seconds=600.0)
        manager.add_proxy('proxy1.example.com', 8080)
        proxy = manager._proxies[0]

        proxy.mark_banned()  # banned_at = now, well within cooldown

        assert manager.get_proxy() is None
        assert proxy.is_banned is True

    def test_auto_unban_resets_consecutive_failures(self):
        """Auto-unban clears the consecutive-failure counter so the proxy gets
        a fresh ban_threshold budget, mirroring unban_all (#552)."""
        manager = ProxyManager(unban_cooldown_seconds=600.0)
        manager.config.ban_threshold = 3
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        for _ in range(3):
            manager.record_result(proxy, success=False)
        assert proxy.is_banned is True

        proxy.banned_at = time.time() - 601.0
        manager.get_proxy()

        assert proxy.is_banned is False
        assert manager._consecutive_failures['proxy1.example.com:8080'] == 0

    def test_auto_unban_disabled_when_cooldown_zero(self):
        """unban_cooldown_seconds=0 keeps the old permanent-ban behaviour (#552)."""
        manager = ProxyManager(unban_cooldown_seconds=0.0)
        manager.add_proxy('proxy1.example.com', 8080)
        proxy = manager._proxies[0]

        proxy.mark_banned()
        proxy.banned_at = time.time() - 10_000.0  # long past, but auto-unban off

        assert manager.get_proxy() is None
        assert proxy.is_banned is True

    def test_unban_all_clears_banned_at(self):
        """unban_all() also clears banned_at, not just the is_banned flag (#552)."""
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        proxy = manager._proxies[0]
        proxy.mark_banned()
        assert proxy.banned_at is not None

        manager.unban_all()
        assert proxy.is_banned is False
        assert proxy.banned_at is None

    def test_cloudflare_ban_re_bans_after_auto_unban_probe(self):
        """DoD regression: a dead cloudflare proxy re-bans on its first probe
        after auto-unban — cumulative error_counts is preserved (#552)."""
        manager = ProxyManager(unban_cooldown_seconds=600.0)
        manager.config.cloudflare_ban_threshold = 2
        manager.config.min_success_rate = 0.0  # isolate the cloudflare path
        manager.add_proxy('proxy1.example.com', 8080)

        proxy = manager.get_proxy()
        for _ in range(2):
            manager.record_result(proxy, success=False, error_type='cloudflare')
        assert proxy.is_banned is True

        # Cooldown elapses -> proxy returns for one probe.
        proxy.banned_at = time.time() - 601.0
        assert manager.get_proxy() is proxy
        assert proxy.is_banned is False

        # One more cloudflare failure re-bans immediately (count 2->3 >= 2).
        manager.record_result(proxy, success=False, error_type='cloudflare')
        assert proxy.is_banned is True

    def test_get_best_proxies(self):
        """Test getting best performing proxies."""
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)
        manager.add_proxy('proxy3.example.com', 8080)

        # Set different success rates
        manager._proxies[0].success_count = 8
        manager._proxies[0].failure_count = 2  # 80% success
        manager._proxies[1].success_count = 5
        manager._proxies[1].failure_count = 5  # 50% success
        manager._proxies[2].success_count = 9
        manager._proxies[2].failure_count = 1  # 90% success

        best = manager.get_best_proxies(limit=2)
        assert len(best) == 2
        assert best[0].host == 'proxy3.example.com'  # 90% success
        assert best[1].host == 'proxy1.example.com'  # 80% success

    def test_get_cooldown_status(self):
        """Test getting cooldown status for proxies."""
        manager = ProxyManager(cooldown_seconds=60.0)
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        # Mark first proxy as recently used
        manager._proxies[0].last_used = time.time()

        status = manager.get_cooldown_status()
        assert 'proxy1.example.com:8080' in status
        assert status['proxy1.example.com:8080'] > 0  # In cooldown
        assert status['proxy2.example.com:8080'] == 0  # Not in cooldown

    def test_get_http_proxy_url_with_auth(self):
        """Test get_http_proxy_url returns proper HTTP format with auth."""
        manager = ProxyManager()
        manager.add_proxy(
            'proxy.example.com',
            8080,
            username='user',
            password='pass'
        )

        http_url = manager.get_http_proxy_url()
        assert http_url == 'http://user:pass@proxy.example.com:8080'

    def test_get_http_proxy_url_without_auth(self):
        """Test get_http_proxy_url returns proper HTTP format without auth."""
        manager = ProxyManager()
        manager.add_proxy('proxy.example.com', 8080)

        http_url = manager.get_http_proxy_url()
        assert http_url == 'http://proxy.example.com:8080'

    def test_get_http_proxy_url_no_proxies(self):
        """Test get_http_proxy_url returns None when no proxies."""
        manager = ProxyManager()

        http_url = manager.get_http_proxy_url()
        assert http_url is None

    def test_get_http_proxy_url_all_banned(self):
        """Test get_http_proxy_url returns None when all proxies banned."""
        manager = ProxyManager()
        manager.add_proxy('proxy.example.com', 8080)

        # Ban the proxy
        manager._proxies[0].mark_banned()

        http_url = manager.get_http_proxy_url()
        assert http_url is None

    def test_load_from_file(self):
        manager = ProxyManager()

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('http://proxy1.example.com:8080\n')
            f.write('http://proxy2.example.com:8080\n')
            f.write('# This is a comment\n')
            f.write('socks5://127.0.0.1:9050\n')
            temp_path = f.name

        try:
            count = manager.load_from_file(temp_path)
            assert count == 3
            assert manager.total_count == 3
        finally:
            os.unlink(temp_path)


class TestCreateProxyManager:
    """Tests for create_proxy_manager function."""

    def test_create_empty(self):
        manager = create_proxy_manager()
        assert manager.total_count == 0

    def test_create_with_urls(self):
        manager = create_proxy_manager(
            proxy_urls=[
                'http://proxy1.example.com:8080',
                'http://proxy2.example.com:8080',
            ]
        )
        assert manager.total_count == 2

    def test_create_with_tor(self):
        manager = create_proxy_manager(use_tor=True)
        assert manager.total_count == 1


class TestProxyType:
    """Tests for ProxyType enum."""

    def test_proxy_types(self):
        assert ProxyType.HTTP.value == 'http'
        assert ProxyType.HTTPS.value == 'https'
        assert ProxyType.SOCKS4.value == 'socks4'
        assert ProxyType.SOCKS5.value == 'socks5'
        assert ProxyType.TOR.value == 'tor'


class TestErrorType:
    """Tests for ErrorType enum."""

    def test_error_types(self):
        assert ErrorType.RATE_LIMIT.value == 'rate_limit'
        assert ErrorType.FORBIDDEN.value == 'forbidden'
        assert ErrorType.CLOUDFLARE.value == 'cloudflare'
        assert ErrorType.TIMEOUT.value == 'timeout'
        assert ErrorType.CONNECTION.value == 'connection'
        assert ErrorType.UNKNOWN.value == 'unknown'


class TestClassifyError:
    """Tests for classify_error function."""

    def test_rate_limit_errors(self):
        assert classify_error("HTTP 429 Too Many Requests") == 'rate_limit'
        assert classify_error("Rate limit exceeded") == 'rate_limit'
        assert classify_error("too many requests") == 'rate_limit'

    def test_forbidden_errors(self):
        assert classify_error("HTTP 403 Forbidden") == 'forbidden'
        assert classify_error("Access denied") == 'forbidden'
        assert classify_error("FORBIDDEN") == 'forbidden'

    def test_cloudflare_errors(self):
        assert classify_error("Cloudflare challenge") == 'cloudflare'
        assert classify_error("CAPTCHA required") == 'cloudflare'
        assert classify_error("Checking your browser") == 'cloudflare'
        assert classify_error("Just a moment") == 'cloudflare'
        assert classify_error("turnstile challenge") == 'cloudflare'

    def test_timeout_errors(self):
        assert classify_error("Request timeout") == 'timeout'
        assert classify_error("Connection timed out") == 'timeout'

    def test_connection_errors(self):
        assert classify_error("Connection refused") == 'connection'
        assert classify_error("Network unreachable") == 'connection'
        assert classify_error("Connection reset") == 'connection'

    def test_unknown_errors(self):
        assert classify_error("Some random error") == 'unknown'
        assert classify_error("") == 'unknown'
