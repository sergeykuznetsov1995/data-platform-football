"""Dependency-light exports for direct-first WhoScored ingestion."""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "SeasonFormat": ("scrapers.whoscored.domain", "SeasonFormat"),
    "WhoScoredScope": ("scrapers.whoscored.domain", "WhoScoredScope"),
    "WhoScoredIngestService": (
        "scrapers.whoscored.service",
        "WhoScoredIngestService",
    ),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    """Load public domain/service objects only when explicitly requested."""

    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
