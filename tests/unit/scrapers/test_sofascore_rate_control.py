from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from scrapers.sofascore.pipeline import build_capture_runtime
from scrapers.sofascore.rate_control import (
    AdaptiveRateLimiter,
    production_rate_limiter,
)
from scrapers.utils import rate_limiter as rate_limiter_module
from scrapers.utils.rate_limiter import RATE_LIMITS, RateLimiter


pytestmark = pytest.mark.unit


@pytest.fixture
def clock(monkeypatch):
    """Freeze the token bucket clock so refill maths is exact."""

    state = {"now": 1000.0}

    def monotonic():
        return state["now"]

    monkeypatch.setattr(
        rate_limiter_module,
        "time",
        SimpleNamespace(monotonic=monotonic, sleep=lambda _seconds: None),
    )
    return state


def test_env_sets_the_history_lane_pace(clock):
    limiter = production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": "60"})

    assert isinstance(limiter, AdaptiveRateLimiter)
    assert limiter.config.max_requests == 60
    assert limiter.config.window_seconds == 60
    assert limiter.config.burst_size == 60
    assert limiter.available_tokens == 60
    assert limiter.fell_back is False


def test_empty_env_keeps_the_production_limiter():
    limiter = production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": ""})
    expected = RATE_LIMITS["sofascore"]

    assert type(limiter) is RateLimiter
    assert limiter.config.max_requests == expected.max_requests
    assert limiter.config.window_seconds == expected.window_seconds
    assert limiter.config.burst_size == expected.burst_size
    assert type(production_rate_limiter({})) is RateLimiter


@pytest.mark.parametrize("value", ["abc", "0", "61", "1.5", "-20"])
def test_invalid_env_fails_closed(value):
    with pytest.raises(ValueError, match="SOFASCORE_RATE_LIMIT_PER_MINUTE"):
        production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": value})


def test_fallback_drops_to_the_production_rate(clock):
    limiter = production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": "60"})
    for _ in range(60):
        assert limiter.try_acquire()
    assert limiter.wait_time_seconds() == pytest.approx(1.0)

    assert limiter.fallback() is True

    expected = RATE_LIMITS["sofascore"]
    assert limiter.fell_back is True
    assert limiter.config.max_requests == expected.max_requests
    assert limiter.config.window_seconds == expected.window_seconds
    assert limiter.config.burst_size == expected.burst_size
    # One token at 20/60 takes three seconds, not one.
    assert limiter.wait_time_seconds() == pytest.approx(3.0)
    clock["now"] += 60.0
    assert limiter.available_tokens == expected.burst_size


def test_fallback_clamps_the_burst_to_the_production_bucket(clock):
    limiter = production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": "60"})
    assert limiter.available_tokens == 60

    limiter.fallback()

    assert limiter.available_tokens == RATE_LIMITS["sofascore"].burst_size


def test_fallback_is_one_way_and_logs_once(clock, caplog):
    limiter = production_rate_limiter({"SOFASCORE_RATE_LIMIT_PER_MINUTE": "60"})
    with caplog.at_level(logging.WARNING, logger="scrapers.sofascore.rate_control"):
        assert limiter.fallback() is True
        assert limiter.fallback() is False

    assert limiter.fell_back is True
    assert limiter.config.max_requests == RATE_LIMITS["sofascore"].max_requests
    assert [r for r in caplog.records if "429" in r.getMessage()] and len(
        caplog.records
    ) == 1


def test_capture_runtime_engine_uses_the_env_pace(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_RATE_LIMIT_PER_MINUTE", "60")
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_ARTIFACT", raising=False)
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_LEDGER", raising=False)
    monkeypatch.setenv("SOFASCORE_MANIFEST_PATH", str(tmp_path / "manifest.json"))

    runtime = build_capture_runtime(
        run_id="history-run",
        task_id="capture",
        raw_store_uri=f"file://{tmp_path / 'raw'}",
        manifest_backend="json",
    )

    limiter = runtime.engine.rate_limiter
    assert isinstance(limiter, AdaptiveRateLimiter)
    assert limiter.config.max_requests == 60
