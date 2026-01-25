"""
Tests for CircuitBreaker utility.
"""

import pytest
from unittest.mock import MagicMock, patch
import pybreaker

from scrapers.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    get_circuit_breaker,
    CIRCUIT_BREAKER_CONFIGS,
    PyBreakerError,
)


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig."""

    def test_default_config(self):
        config = CircuitBreakerConfig()
        assert config.fail_max == 5
        assert config.reset_timeout == 60
        assert config.success_threshold == 2

    def test_custom_config(self):
        config = CircuitBreakerConfig(
            fail_max=3,
            reset_timeout=120,
            success_threshold=1,
        )
        assert config.fail_max == 3
        assert config.reset_timeout == 120


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_init(self):
        cb = CircuitBreaker(fail_max=5, reset_timeout=60)
        assert cb.config.fail_max == 5
        assert cb.state == 'closed'

    def test_call_success(self):
        cb = CircuitBreaker(fail_max=3)

        def success_func():
            return "success"

        result = cb.call(success_func)
        assert result == "success"
        assert cb.failure_count == 0

    def test_call_failure(self):
        cb = CircuitBreaker(fail_max=3, reset_timeout=60)

        def failing_func():
            raise ValueError("Error")

        with pytest.raises(ValueError):
            cb.call(failing_func)

        assert cb.failure_count == 1

    def test_circuit_opens_after_failures(self):
        cb = CircuitBreaker(fail_max=2, reset_timeout=60)

        def failing_func():
            raise ValueError("Error")

        # First failure - still closed
        with pytest.raises(ValueError):
            cb.call(failing_func)
        assert cb.failure_count == 1

        # Second failure - pybreaker opens circuit and raises CircuitBreakerError
        with pytest.raises(pybreaker.CircuitBreakerError):
            cb.call(failing_func)

        # Circuit should be open now
        assert cb.is_open is True

        # Subsequent calls also raise CircuitBreakerError
        with pytest.raises(pybreaker.CircuitBreakerError):
            cb.call(failing_func)

    def test_state_property(self):
        cb = CircuitBreaker()
        assert cb.state == 'closed'
        assert cb.state_enum == CircuitState.CLOSED

    def test_is_closed(self):
        cb = CircuitBreaker()
        assert cb.is_closed is True
        assert cb.is_open is False

    def test_decorator(self):
        cb = CircuitBreaker(fail_max=5)

        @cb.decorator
        def my_func(x):
            return x * 2

        result = my_func(5)
        assert result == 10

    def test_reset_method_exists(self):
        """Test that reset method exists and is callable."""
        cb = CircuitBreaker(fail_max=5, reset_timeout=60)

        # Verify reset method exists and can be called
        cb.reset()  # Should not raise

    def test_force_open_method_exists(self):
        """Test that force_open method exists and is callable."""
        cb = CircuitBreaker()
        assert cb.is_closed is True

        # Verify force_open method exists and can be called
        cb.force_open()  # Should not raise


class TestGetCircuitBreaker:
    """Tests for get_circuit_breaker function."""

    def test_get_default(self):
        cb = get_circuit_breaker('default')
        assert cb.config.fail_max == 5

    def test_get_strict(self):
        cb = get_circuit_breaker('strict')
        assert cb.config.fail_max == 3
        assert cb.config.reset_timeout == 120

    def test_get_lenient(self):
        cb = get_circuit_breaker('lenient')
        assert cb.config.fail_max == 10

    def test_get_whoscored(self):
        cb = get_circuit_breaker('whoscored')
        assert cb.config.fail_max == 3
        assert cb.config.reset_timeout == 300  # Longer for selenium

    def test_get_with_name(self):
        cb = get_circuit_breaker('default', name='my_breaker')
        assert cb.config.name == 'my_breaker'


class TestCircuitState:
    """Tests for CircuitState enum."""

    def test_states(self):
        assert CircuitState.CLOSED.value == 'closed'
        assert CircuitState.OPEN.value == 'open'
        assert CircuitState.HALF_OPEN.value == 'half-open'
