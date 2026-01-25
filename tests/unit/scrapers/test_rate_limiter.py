"""
Tests for RateLimiter utility.
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from scrapers.utils.rate_limiter import (
    RateLimiter,
    RateLimiterConfig,
    get_rate_limiter,
    RATE_LIMITS,
)


class TestRateLimiterConfig:
    """Tests for RateLimiterConfig."""

    def test_default_config(self):
        config = RateLimiterConfig()
        assert config.max_requests == 10
        assert config.window_seconds == 60.0
        assert config.burst_size == 10

    def test_custom_config(self):
        config = RateLimiterConfig(
            max_requests=20,
            window_seconds=30.0,
            burst_size=5
        )
        assert config.max_requests == 20
        assert config.window_seconds == 30.0
        assert config.burst_size == 5

    def test_burst_size_defaults_to_max_requests(self):
        config = RateLimiterConfig(max_requests=15)
        assert config.burst_size == 15


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_init(self):
        limiter = RateLimiter(max_requests=10, window_seconds=60)
        assert limiter.config.max_requests == 10
        assert limiter.config.window_seconds == 60

    def test_try_acquire_success(self):
        limiter = RateLimiter(max_requests=10, window_seconds=60)
        assert limiter.try_acquire() is True
        assert limiter.available_tokens < 10

    def test_try_acquire_exhausted(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60, burst_size=2)

        # Exhaust tokens
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False

    def test_acquire_with_timeout(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60, burst_size=1)

        # First should succeed
        assert limiter.acquire(timeout=0.1) is True

        # Second should fail due to timeout
        assert limiter.acquire(timeout=0.1) is False

    def test_token_refill(self):
        limiter = RateLimiter(max_requests=60, window_seconds=1.0)  # 1 per second

        # Use a token
        limiter.try_acquire()
        initial_tokens = limiter.available_tokens

        # Wait for refill
        time.sleep(0.1)

        # Should have more tokens
        assert limiter.available_tokens > initial_tokens

    def test_is_limited(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60, burst_size=1)

        assert limiter.is_limited is False
        limiter.try_acquire()
        assert limiter.is_limited is True

    def test_reset(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60, burst_size=2)

        limiter.try_acquire()
        limiter.try_acquire()
        assert limiter.try_acquire() is False

        limiter.reset()
        assert limiter.try_acquire() is True

    def test_wait_time_seconds(self):
        limiter = RateLimiter(max_requests=60, window_seconds=60)  # 1 per second

        # With tokens available
        assert limiter.wait_time_seconds() == 0.0

        # Exhaust tokens
        for _ in range(60):
            limiter.try_acquire()

        # Should need to wait
        assert limiter.wait_time_seconds() > 0

    def test_thread_safety(self):
        limiter = RateLimiter(max_requests=100, window_seconds=60, burst_size=100)
        acquired_count = [0]
        lock = threading.Lock()

        def worker():
            if limiter.try_acquire():
                with lock:
                    acquired_count[0] += 1

        # Run many threads concurrently
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(worker) for _ in range(200)]
            for f in futures:
                f.result()

        # Should have acquired exactly 100 (burst size)
        assert acquired_count[0] == 100


class TestGetRateLimiter:
    """Tests for get_rate_limiter function."""

    def test_get_known_source(self):
        limiter = get_rate_limiter('fbref')
        assert limiter.config.max_requests == RATE_LIMITS['fbref'].max_requests

    def test_get_unknown_source(self):
        limiter = get_rate_limiter('unknown_source')
        assert limiter.config.max_requests == 10  # Default

    def test_predefined_limits(self):
        assert 'fbref' in RATE_LIMITS
        assert 'understat' in RATE_LIMITS
        assert 'whoscored' in RATE_LIMITS
