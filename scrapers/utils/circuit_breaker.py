"""
Circuit Breaker
===============

PyBreaker-based circuit breaker for handling service failures.
Prevents cascading failures by temporarily blocking requests to failing services.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, Type, Tuple

import pybreaker

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = 'closed'      # Normal operation
    OPEN = 'open'          # Blocking requests
    HALF_OPEN = 'half-open'  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    fail_max: int = 5
    reset_timeout: int = 60
    success_threshold: int = 2
    exclude_exceptions: Tuple[Type[Exception], ...] = ()
    name: Optional[str] = None


class CircuitBreakerListener(pybreaker.CircuitBreakerListener):
    """Custom listener for circuit breaker state changes."""

    def __init__(self, name: str):
        self.name = name

    def state_change(self, cb: pybreaker.CircuitBreaker, old_state, new_state):
        """Log state changes."""
        logger.warning(
            f"Circuit breaker '{self.name}' state changed: "
            f"{old_state.name} -> {new_state.name}"
        )

    def failure(self, cb: pybreaker.CircuitBreaker, exc: Exception):
        """Log failures."""
        logger.debug(
            f"Circuit breaker '{self.name}' recorded failure: {exc}"
        )

    def success(self, cb: pybreaker.CircuitBreaker):
        """Log successes."""
        logger.debug(f"Circuit breaker '{self.name}' recorded success")


class CircuitBreaker:
    """
    Circuit breaker wrapper around pybreaker.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service failing, requests blocked for reset_timeout seconds
    - HALF_OPEN: Testing if service recovered, limited requests allowed

    Usage:
        cb = CircuitBreaker(fail_max=5, reset_timeout=60)

        result = cb.call(some_function, arg1, arg2)

        # Or use as decorator
        @cb.decorator
        def my_function():
            ...
    """

    def __init__(
        self,
        fail_max: int = 5,
        reset_timeout: int = 60,
        success_threshold: int = 2,
        exclude_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
        name: Optional[str] = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            fail_max: Number of failures before opening circuit
            reset_timeout: Seconds to wait before half-open state
            success_threshold: Successes needed to close from half-open
            exclude_exceptions: Exceptions that don't count as failures
            name: Optional name for logging
        """
        self.config = CircuitBreakerConfig(
            fail_max=fail_max,
            reset_timeout=reset_timeout,
            success_threshold=success_threshold,
            exclude_exceptions=exclude_exceptions or (),
            name=name or 'default',
        )

        self._breaker = pybreaker.CircuitBreaker(
            fail_max=fail_max,
            reset_timeout=reset_timeout,
            success_threshold=success_threshold,
            # CircuitBreakerError must never count as a failure; merge in any
            # caller-supplied exclusions instead of dropping them (#470 bug 3).
            exclude=[pybreaker.CircuitBreakerError, *self.config.exclude_exceptions],
            listeners=[CircuitBreakerListener(self.config.name)],
            name=self.config.name,
        )

    @property
    def state(self) -> str:
        """Get current circuit state as string."""
        state_name = self._breaker.current_state
        # pybreaker returns state name like 'closed', 'open', 'half-open'
        if hasattr(state_name, 'name'):
            # It's a state object
            return state_name.name.lower().replace('_', '-')
        return str(state_name).lower()

    @property
    def state_enum(self) -> CircuitState:
        """Get current circuit state as enum."""
        state_map = {
            'closed': CircuitState.CLOSED,
            'open': CircuitState.OPEN,
            'half-open': CircuitState.HALF_OPEN,
        }
        return state_map.get(self.state, CircuitState.CLOSED)

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests)."""
        return self.state == 'open'

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == 'closed'

    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._breaker.fail_counter

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            pybreaker.CircuitBreakerError: If circuit is open
            Exception: Original exception from function
        """
        return self._breaker.call(func, *args, **kwargs)

    def decorator(self, func: Callable) -> Callable:
        """
        Decorator to wrap function with circuit breaker.

        Usage:
            @cb.decorator
            def my_function():
                ...
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper

    def reset(self) -> None:
        """
        Reset circuit breaker.

        Note: pybreaker doesn't expose a public reset method.
        This recreates the internal breaker.
        """
        self._breaker = pybreaker.CircuitBreaker(
            fail_max=self.config.fail_max,
            reset_timeout=self.config.reset_timeout,
            success_threshold=self.config.success_threshold,
            exclude=[pybreaker.CircuitBreakerError, *self.config.exclude_exceptions],
            listeners=[CircuitBreakerListener(self.config.name)],
            name=self.config.name,
        )
        logger.info(f"Circuit breaker '{self.config.name}' manually reset")

    def force_open(self) -> None:
        """
        Force circuit to open state by simulating max failures.

        Note: This creates a temporary function that always fails
        and calls it until the circuit opens.
        """
        def always_fail():
            raise Exception("Forced failure")

        for _ in range(self.config.fail_max):
            try:
                self._breaker.call(always_fail)
            except (Exception, pybreaker.CircuitBreakerError):
                pass

        logger.warning(f"Circuit breaker '{self.config.name}' forced open")


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


# Re-export pybreaker's CircuitBreakerError for convenience
PyBreakerError = pybreaker.CircuitBreakerError


# Preset configurations for different sources
CIRCUIT_BREAKER_CONFIGS = {
    'default': CircuitBreakerConfig(fail_max=5, reset_timeout=60),
    'strict': CircuitBreakerConfig(fail_max=3, reset_timeout=120),
    'lenient': CircuitBreakerConfig(fail_max=10, reset_timeout=30),
    'whoscored': CircuitBreakerConfig(fail_max=3, reset_timeout=300),  # Strict for selenium
}


def get_circuit_breaker(
    source: str = 'default',
    name: Optional[str] = None
) -> CircuitBreaker:
    """
    Get a circuit breaker configured for a specific source.

    Args:
        source: Configuration preset name
        name: Optional circuit breaker name

    Returns:
        Configured CircuitBreaker instance
    """
    config = CIRCUIT_BREAKER_CONFIGS.get(source, CIRCUIT_BREAKER_CONFIGS['default'])
    return CircuitBreaker(
        fail_max=config.fail_max,
        reset_timeout=config.reset_timeout,
        success_threshold=config.success_threshold,
        name=name or source,
    )
