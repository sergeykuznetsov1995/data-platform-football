"""
Token Bucket Rate Limiter
=========================

Thread-safe rate limiter implementation using token bucket algorithm.
Supports configurable request limits per time window.
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimiterConfig:
    """Configuration for rate limiter."""
    max_requests: int = 10
    window_seconds: float = 60.0
    burst_size: Optional[int] = None  # If None, equals max_requests

    def __post_init__(self):
        if self.burst_size is None:
            self.burst_size = self.max_requests


class RateLimiter:
    """
    Token bucket rate limiter with thread-safe operations.

    Usage:
        limiter = RateLimiter(max_requests=10, window_seconds=60)

        if limiter.acquire():
            # Make request
            pass
    """

    def __init__(
        self,
        max_requests: int = 10,
        window_seconds: float = 60.0,
        burst_size: Optional[int] = None
    ):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed per window
            window_seconds: Time window in seconds
            burst_size: Maximum burst capacity (defaults to max_requests)
        """
        self.config = RateLimiterConfig(
            max_requests=max_requests,
            window_seconds=window_seconds,
            burst_size=burst_size
        )

        # Token bucket state
        self._tokens = float(self.config.burst_size)
        self._last_update = time.monotonic()
        self._lock = threading.Lock()

        # Refill rate: tokens per second
        self._refill_rate = self.config.max_requests / self.config.window_seconds

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_update

        # Add tokens based on elapsed time
        new_tokens = elapsed * self._refill_rate
        self._tokens = min(self.config.burst_size, self._tokens + new_tokens)
        self._last_update = now

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token, blocking if necessary.

        Args:
            timeout: Maximum time to wait for a token (None = wait forever)

        Returns:
            True if token acquired, False if timeout exceeded
        """
        start_time = time.monotonic()

        while True:
            if self.try_acquire():
                return True

            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= timeout:
                    return False

            # Calculate wait time until next token is available
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    continue

                wait_time = (1 - self._tokens) / self._refill_rate

            # Sleep for a portion of the wait time
            sleep_time = min(wait_time, 0.1)  # Check every 100ms max
            if timeout is not None:
                remaining = timeout - (time.monotonic() - start_time)
                sleep_time = min(sleep_time, max(0, remaining))

            time.sleep(sleep_time)

    def try_acquire(self) -> bool:
        """
        Try to acquire a token without blocking.

        Returns:
            True if token acquired, False otherwise
        """
        with self._lock:
            self._refill()

            if self._tokens >= 1:
                self._tokens -= 1
                return True

            return False

    @property
    def available_tokens(self) -> float:
        """Get current number of available tokens."""
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def is_limited(self) -> bool:
        """Check if rate limit is currently active."""
        return self.available_tokens < 1

    def reset(self) -> None:
        """Reset the rate limiter to full capacity."""
        with self._lock:
            self._tokens = float(self.config.burst_size)
            self._last_update = time.monotonic()

    def wait_time_seconds(self) -> float:
        """
        Get estimated wait time until a token is available.

        Returns:
            Seconds to wait, or 0 if token available now
        """
        with self._lock:
            self._refill()

            if self._tokens >= 1:
                return 0.0

            return (1 - self._tokens) / self._refill_rate


# Preset configurations for known sources
RATE_LIMITS = {
    'fbref': RateLimiterConfig(max_requests=20, window_seconds=60),
    'understat': RateLimiterConfig(max_requests=30, window_seconds=60),
    'whoscored': RateLimiterConfig(max_requests=10, window_seconds=60),
    'fotmob': RateLimiterConfig(max_requests=30, window_seconds=60),
    'sofascore': RateLimiterConfig(max_requests=20, window_seconds=60),
    # Registry discovery walks thousands of small public JSON documents through
    # one metered residential lease at a time. Keep it at the SofaScore pace and
    # deny it a burst, so a long scan stays an even, human-plausible trickle.
    'sofascore_discovery': RateLimiterConfig(
        max_requests=20, window_seconds=60, burst_size=1
    ),
    'sofifa': RateLimiterConfig(max_requests=30, window_seconds=60),
    'clubelo': RateLimiterConfig(max_requests=60, window_seconds=60),
    'espn': RateLimiterConfig(max_requests=30, window_seconds=60),
    'matchhistory': RateLimiterConfig(max_requests=30, window_seconds=60),
}


def get_rate_limiter(source: str) -> RateLimiter:
    """
    Get a rate limiter configured for a specific source.

    Args:
        source: Data source name (e.g., 'fbref', 'understat')

    Returns:
        Configured RateLimiter instance
    """
    config = RATE_LIMITS.get(source, RateLimiterConfig())
    return RateLimiter(
        max_requests=config.max_requests,
        window_seconds=config.window_seconds,
        burst_size=config.burst_size
    )
