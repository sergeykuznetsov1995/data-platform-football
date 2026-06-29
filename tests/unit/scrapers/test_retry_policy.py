"""
Tests for RetryPolicy utility.
"""

import pytest
from unittest.mock import MagicMock, patch
from tenacity import RetryError

from scrapers.utils.retry_policy import (
    RetryPolicy,
    get_retry_policy,
    with_retry,
    RETRY_POLICIES,
    RETRYABLE_EXCEPTIONS,
)


class TestRetryPolicy:
    """Tests for RetryPolicy."""

    def test_init_defaults(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.max_delay == 60.0
        assert policy.min_wait == 1.0
        assert policy.max_wait == 30.0

    def test_init_custom(self):
        policy = RetryPolicy(
            max_attempts=5,
            max_delay=120,
            min_wait=0.5,
            max_wait=15,
        )
        assert policy.max_attempts == 5
        assert policy.max_delay == 120

    def test_execute_success(self):
        policy = RetryPolicy(max_attempts=3)

        def success_func():
            return "success"

        result = policy.execute(success_func)
        assert result == "success"

    def test_execute_with_retry(self):
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        call_count = [0]

        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ConnectionError("Temporary failure")
            return "success"

        result = policy.execute(flaky_func)
        assert result == "success"
        assert call_count[0] == 3

    def test_execute_max_retries_exceeded(self):
        policy = RetryPolicy(max_attempts=2, min_wait=0.01, max_wait=0.01)

        def always_fails():
            raise ConnectionError("Always fails")

        with pytest.raises(ConnectionError):
            policy.execute(always_fails)

    def test_decorator(self):
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        call_count = [0]

        @policy.decorator
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise TimeoutError("Temporary")
            return "done"

        result = flaky_func()
        assert result == "done"
        assert call_count[0] == 2

    def test_non_retryable_exception(self):
        policy = RetryPolicy(max_attempts=3, min_wait=0.01)

        def raises_value_error():
            raise ValueError("Not retryable")

        # ValueError is not in RETRYABLE_EXCEPTIONS by default
        with pytest.raises(ValueError):
            policy.execute(raises_value_error)

    def test_custom_retryable_exceptions(self):
        policy = RetryPolicy(
            max_attempts=2,
            min_wait=0.01,
            retryable_exceptions=(ValueError,),
        )
        call_count = [0]

        def raises_value_error():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("Retryable now")
            return "success"

        result = policy.execute(raises_value_error)
        assert result == "success"
        assert call_count[0] == 2


class TestGetRetryPolicy:
    """Tests for get_retry_policy function."""

    def test_get_standard(self):
        policy = get_retry_policy('standard')
        assert policy.max_attempts == 3

    def test_get_aggressive(self):
        policy = get_retry_policy('aggressive')
        assert policy.max_attempts == 5

    def test_get_conservative(self):
        policy = get_retry_policy('conservative')
        assert policy.max_attempts == 2

    def test_get_unknown(self):
        policy = get_retry_policy('unknown')
        # Should return standard
        assert policy.max_attempts == 3


class TestWithRetryDecorator:
    """Tests for with_retry decorator factory."""

    def test_decorator_factory(self):
        call_count = [0]

        @with_retry(max_attempts=2, max_delay=10)
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ConnectionError("Fail")
            return "ok"

        result = flaky_func()
        assert result == "ok"


class TestRetryableExceptions:
    """Tests for RETRYABLE_EXCEPTIONS."""

    def test_default_exceptions(self):
        assert ConnectionError in RETRYABLE_EXCEPTIONS
        assert TimeoutError in RETRYABLE_EXCEPTIONS
        assert OSError in RETRYABLE_EXCEPTIONS


class TestHttpErrorRetry:
    """HTTPError is retried only for transient/server statuses, never 4xx (#470).

    requests.exceptions.HTTPError subclasses OSError, so a blanket type-based
    retry would otherwise hammer 403/404 — burning attempts and accelerating
    proxy bans on anti-bot sources.
    """

    @staticmethod
    def _http_error(status):
        import requests
        resp = MagicMock()
        resp.status_code = status
        return requests.exceptions.HTTPError(response=resp)

    def test_http_403_not_retried(self):
        import requests
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        calls = [0]

        def f():
            calls[0] += 1
            raise self._http_error(403)

        with pytest.raises(requests.exceptions.HTTPError):
            policy.execute(f)
        assert calls[0] == 1  # no retry on 403

    def test_http_404_not_retried(self):
        import requests
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        calls = [0]

        def f():
            calls[0] += 1
            raise self._http_error(404)

        with pytest.raises(requests.exceptions.HTTPError):
            policy.execute(f)
        assert calls[0] == 1

    def test_http_503_retried(self):
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        calls = [0]

        def f():
            calls[0] += 1
            if calls[0] < 3:
                raise self._http_error(503)
            return "ok"

        assert policy.execute(f) == "ok"
        assert calls[0] == 3

    def test_http_429_retried(self):
        policy = RetryPolicy(max_attempts=2, min_wait=0.01, max_wait=0.01)
        calls = [0]

        def f():
            calls[0] += 1
            raise self._http_error(429)

        with pytest.raises(Exception):
            policy.execute(f)
        assert calls[0] == 2  # retried up to max_attempts

    def test_connection_error_still_retried(self):
        """Regression guard: non-HTTP retryables keep retrying."""
        policy = RetryPolicy(max_attempts=3, min_wait=0.01, max_wait=0.01)
        calls = [0]

        def f():
            calls[0] += 1
            if calls[0] < 3:
                raise ConnectionError("transient")
            return "ok"

        assert policy.execute(f) == "ok"
        assert calls[0] == 3
