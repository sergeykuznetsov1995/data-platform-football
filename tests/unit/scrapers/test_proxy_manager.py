"""
Tests for ProxyManager utility.
"""

import pytest
from unittest.mock import MagicMock, patch
import tempfile
import os

from scrapers.utils.proxy_manager import (
    Proxy,
    ProxyType,
    ProxyManager,
    ProxyManagerConfig,
    create_proxy_manager,
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

    def test_mark_banned(self):
        proxy = Proxy(host='test', port=8080)
        assert proxy.is_banned is False
        proxy.mark_banned()
        assert proxy.is_banned is True


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
