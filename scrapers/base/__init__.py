"""Dependency-light lazy exports for shared scraper base components."""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "BaseScraper": ("scrapers.base.base_scraper", "BaseScraper"),
    "ReplaceGuardError": ("scrapers.base.base_scraper", "ReplaceGuardError"),
    "SeleniumScraper": ("scrapers.base.base_scraper", "SeleniumScraper"),
    "IcebergWriter": ("scrapers.base.iceberg_writer", "IcebergWriter"),
    "HDFSClient": ("scrapers.base.hdfs_client", "HDFSClient"),
    "HDFSError": ("scrapers.base.hdfs_client", "HDFSError"),
    "TrinoTableManager": ("scrapers.base.trino_manager", "TrinoTableManager"),
    "TrinoError": ("scrapers.base.trino_manager", "TrinoError"),
    "CloudflareBypass": ("scrapers.base.browser", "CloudflareBypass"),
    "BrowserConfig": ("scrapers.base.browser", "BrowserConfig"),
    "browser_session": ("scrapers.base.browser", "browser_session"),
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
