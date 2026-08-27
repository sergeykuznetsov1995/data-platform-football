"""SofaScore request pacing for the history lane (#1218, owner decision 5).

The daily ingest keeps the production ``RATE_LIMITS["sofascore"]`` pace (20/60)
that the paid canary was measured with.  The history lane may ask for a faster
pace through ``SOFASCORE_RATE_LIMIT_PER_MINUTE``; the first HTTP 429 *from the
source* drops that limiter back to the production pace for the rest of the
process.  Gateway 429s (lease concurrency) never touch the limiter.
"""

from __future__ import annotations

import logging
import os
from typing import Mapping

from scrapers.utils.rate_limiter import (
    RATE_LIMITS,
    RateLimiter,
    RateLimiterConfig,
    get_rate_limiter,
)

logger = logging.getLogger(__name__)

RATE_LIMIT_ENV = "SOFASCORE_RATE_LIMIT_PER_MINUTE"
_MAX_PER_MINUTE = 60


class AdaptiveRateLimiter(RateLimiter):
    """Token bucket with a one-way fallback to the production SofaScore pace."""

    def __init__(
        self,
        max_requests: int,
        window_seconds: float = 60.0,
        burst_size: int | None = None,
        *,
        fallback_config: RateLimiterConfig = RATE_LIMITS["sofascore"],
    ) -> None:
        super().__init__(max_requests, window_seconds, burst_size)
        self._fallback_config = fallback_config
        self.fell_back = False

    def fallback(self) -> bool:
        """Drop to the production pace; return False when already there."""
        with self._lock:
            if self.fell_back:
                return False
            # Credit the time elapsed at the old pace before switching.
            self._refill()
            previous = self.config
            self.config = RateLimiterConfig(
                max_requests=self._fallback_config.max_requests,
                window_seconds=self._fallback_config.window_seconds,
                burst_size=self._fallback_config.burst_size,
            )
            self._refill_rate = self.config.max_requests / self.config.window_seconds
            self._tokens = min(self._tokens, float(self.config.burst_size))
            self.fell_back = True
        logger.warning(
            "SofaScore answered 429: rate limit %s/%ss falls back to %s/%ss "
            "for the rest of this process",
            previous.max_requests,
            previous.window_seconds,
            self.config.max_requests,
            self.config.window_seconds,
        )
        return True


def production_rate_limiter(environ: Mapping[str, str] = os.environ) -> RateLimiter:
    """Build the engine limiter from ``SOFASCORE_RATE_LIMIT_PER_MINUTE``.

    Unset/empty keeps the plain production limiter; a value must be an integer
    in ``1..60`` requests per minute, anything else fails closed.
    """
    raw = (environ.get(RATE_LIMIT_ENV) or "").strip()
    if not raw:
        return get_rate_limiter("sofascore")
    try:
        per_minute = int(raw)
    except ValueError:
        per_minute = 0
    if not 1 <= per_minute <= _MAX_PER_MINUTE:
        raise ValueError(
            f"{RATE_LIMIT_ENV} must be an integer in 1..{_MAX_PER_MINUTE}, got {raw!r}"
        )
    return AdaptiveRateLimiter(max_requests=per_minute, window_seconds=60.0)
