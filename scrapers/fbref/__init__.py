"""FBref ingestion package with lazy compatibility exports.

The production path is ``pipeline`` → ``fetcher`` → immutable raw storage →
offline generic/typed parsing. Imports stay lazy so Airflow DAG parsing does
not initialize pandas, PyArrow, Trino, or browser components.
"""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "BASE_URL": ("scrapers.fbref.constants", "BASE_URL"),
    "LEAGUE_IDS": ("scrapers.fbref.constants", "LEAGUE_IDS"),
    "PLAYER_STAT_TYPES": ("scrapers.fbref.constants", "PLAYER_STAT_TYPES"),
    "TEAM_STAT_TYPES": ("scrapers.fbref.constants", "TEAM_STAT_TYPES"),
    "DEFAULT_RATE_LIMIT": ("scrapers.fbref.constants", "DEFAULT_RATE_LIMIT"),
    "extract_tables_from_comments": (
        "scrapers.fbref.html_parser",
        "extract_tables_from_comments",
    ),
    "extract_player_ids_from_table": (
        "scrapers.fbref.html_parser",
        "extract_player_ids_from_table",
    ),
    "extract_team_ids_from_table": (
        "scrapers.fbref.html_parser",
        "extract_team_ids_from_table",
    ),
    "parse_table": ("scrapers.fbref.html_parser", "parse_table"),
    "find_schedule_table": (
        "scrapers.fbref.html_parser",
        "find_schedule_table",
    ),
    "find_team_stats_table": (
        "scrapers.fbref.html_parser",
        "find_team_stats_table",
    ),
    "find_player_stats_table": (
        "scrapers.fbref.html_parser",
        "find_player_stats_table",
    ),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
