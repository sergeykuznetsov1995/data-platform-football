"""Dependency-light lazy exports for scraper utilities."""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "RateLimiter": ("scrapers.utils.rate_limiter", "RateLimiter"),
    "RetryPolicy": ("scrapers.utils.retry_policy", "RetryPolicy"),
    "CircuitBreaker": ("scrapers.utils.circuit_breaker", "CircuitBreaker"),
    "ProxyManager": ("scrapers.utils.proxy_manager", "ProxyManager"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    """Load an export only when a caller explicitly requests it."""
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
