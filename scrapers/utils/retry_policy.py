"""
Retry Policy
============

Tenacity-based retry policy with exponential backoff.
Provides decorators and context managers for resilient operations.
"""

import logging
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, Union

from tenacity import (
    RetryError,
    Retrying,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_random_exponential,
    before_sleep_log,
    after_log,
)

logger = logging.getLogger(__name__)

# Common retryable exceptions
RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)

try:
    import requests
    RETRYABLE_EXCEPTIONS = RETRYABLE_EXCEPTIONS + (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
    )
except ImportError:
    pass


class RetryPolicy:
    """
    Configurable retry policy using tenacity.

    Usage:
        policy = RetryPolicy(max_attempts=3, max_delay=60)

        @policy.decorator
        def fetch_data():
            ...

        # Or use as context manager
        for attempt in policy.attempts():
            with attempt:
                fetch_data()
    """

    def __init__(
        self,
        max_attempts: int = 3,
        max_delay: float = 60.0,
        min_wait: float = 1.0,
        max_wait: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
        log_level: int = logging.WARNING,
    ):
        """
        Initialize retry policy.

        Args:
            max_attempts: Maximum number of retry attempts
            max_delay: Maximum total delay across all retries
            min_wait: Minimum wait time between retries
            max_wait: Maximum wait time between retries
            exponential_base: Base for exponential backoff
            jitter: Add random jitter to wait times
            retryable_exceptions: Exceptions that trigger retry
            log_level: Logging level for retry attempts
        """
        self.max_attempts = max_attempts
        self.max_delay = max_delay
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or RETRYABLE_EXCEPTIONS
        self.log_level = log_level

    def _get_wait_strategy(self):
        """Get the wait strategy based on configuration."""
        if self.jitter:
            return wait_random_exponential(
                multiplier=self.min_wait,
                max=self.max_wait,
            )
        return wait_exponential(
            multiplier=self.min_wait,
            max=self.max_wait,
            exp_base=self.exponential_base,
        )

    def _get_stop_strategy(self):
        """Get the stop strategy based on configuration."""
        return (
            stop_after_attempt(self.max_attempts) |
            stop_after_delay(self.max_delay)
        )

    def _get_retry_condition(self):
        """Get the retry condition based on configuration."""
        return retry_if_exception_type(self.retryable_exceptions)

    @property
    def decorator(self) -> Callable:
        """
        Get a decorator that applies this retry policy.

        Usage:
            @policy.decorator
            def my_function():
                ...
        """
        return retry(
            wait=self._get_wait_strategy(),
            stop=self._get_stop_strategy(),
            retry=self._get_retry_condition(),
            before_sleep=before_sleep_log(logger, self.log_level),
            after=after_log(logger, self.log_level),
            reraise=True,
        )

    def attempts(self) -> Retrying:
        """
        Get a Retrying object for use in for loops.

        Usage:
            for attempt in policy.attempts():
                with attempt:
                    do_something()
        """
        return Retrying(
            wait=self._get_wait_strategy(),
            stop=self._get_stop_strategy(),
            retry=self._get_retry_condition(),
            before_sleep=before_sleep_log(logger, self.log_level),
            after=after_log(logger, self.log_level),
            reraise=True,
        )

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with retry policy.

        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function

        Returns:
            Function result

        Raises:
            RetryError: If all retries exhausted
        """
        decorated = self.decorator(func)
        return decorated(*args, **kwargs)


# Preset policies for different use cases
RETRY_POLICIES = {
    'aggressive': RetryPolicy(
        max_attempts=5,
        max_delay=120,
        min_wait=0.5,
        max_wait=30,
    ),
    'standard': RetryPolicy(
        max_attempts=3,
        max_delay=60,
        min_wait=1,
        max_wait=15,
    ),
    'conservative': RetryPolicy(
        max_attempts=2,
        max_delay=30,
        min_wait=2,
        max_wait=10,
    ),
    'quick': RetryPolicy(
        max_attempts=2,
        max_delay=10,
        min_wait=0.5,
        max_wait=5,
    ),
}


def with_retry(
    max_attempts: int = 3,
    max_delay: float = 60.0,
    exceptions: Optional[Tuple[Type[Exception], ...]] = None,
) -> Callable:
    """
    Decorator factory for applying retry policy.

    Usage:
        @with_retry(max_attempts=3)
        def fetch_data():
            ...
    """
    policy = RetryPolicy(
        max_attempts=max_attempts,
        max_delay=max_delay,
        retryable_exceptions=exceptions,
    )
    return policy.decorator


def get_retry_policy(preset: str = 'standard') -> RetryPolicy:
    """
    Get a preset retry policy.

    Args:
        preset: Policy preset name ('aggressive', 'standard', 'conservative', 'quick')

    Returns:
        Configured RetryPolicy instance
    """
    return RETRY_POLICIES.get(preset, RETRY_POLICIES['standard'])
