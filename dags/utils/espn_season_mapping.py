"""Dependency-light ESPN source-season to platform-season promotion boundary.

Bronze owns source identity.  This module is used only when a downstream
transform promotes a reviewed ESPN scope into platform ``(league, season)``
identity.  There is deliberately no year-conversion formula here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping

import yaml


SCHEMA_VERSION = 1
CONVENTIONS = frozenset({"split_year", "calendar_year", "tournament"})
VALUES_MARKER = "__ESPN_DOWNSTREAM_SCOPE_VALUES__"
FILTER_MARKER = "__ESPN_DOWNSTREAM_SCOPE_FILTER__"
_SCOPE_RE = re.compile(r"([1-9][0-9]*):([1-9][0-9]{3})")
_SEASON_RE = re.compile(r"[0-9]{4}")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTAINER_CONFIG_ROOT = Path("/opt/airflow/configs")
_CONFIG_ROOT = (
    _CONTAINER_CONFIG_ROOT
    if _CONTAINER_CONFIG_ROOT.is_dir()
    else _REPO_ROOT / "configs"
)
DEFAULT_MAPPING_PATH = Path(
    os.environ.get("ESPN_SEASON_MAPPING_PATH")
    or _CONFIG_ROOT / "espn" / "season_mapping.yaml"
)
DEFAULT_COMPETITIONS_PATH = Path(
    os.environ.get("MEDALLION_CONFIG_DIR")
    or _CONFIG_ROOT / "medallion"
) / "competitions.yaml"
DEFAULT_REGISTRY_PATH = _CONFIG_ROOT / "espn" / "registry.yaml"


class SeasonMappingError(ValueError):
    """The explicit ESPN downstream promotion map is unsafe or ambiguous."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise SeasonMappingError(f"duplicate YAML key {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True, slots=True)
class Approval:
    approved_by: str
    approved_at: date
    reference: str


@dataclass(frozen=True, slots=True)
class SeasonMapping:
    scope_id: str
    espn_id: int
    source_season_year: int
    platform_league: str | None
    platform_season_slug: str
    convention: str
    effective_start_date: date
    effective_end_date: date
    approval: Approval
    downstream_enabled: bool = False


@dataclass(frozen=True, slots=True)
class SeasonMappingCatalog:
    schema_version: int
    mapping_version: str
    frozen_scope_reference: str
    mappings: tuple[SeasonMapping, ...]

    @property
    def by_scope(self) -> Mapping[str, SeasonMapping]:
        return MappingProxyType({item.scope_id: item for item in self.mappings})

    @property
    def enabled(self) -> tuple[SeasonMapping, ...]:
        return tuple(item for item in self.mappings if item.downstream_enabled)


def _reject_unknown_keys(value: Mapping[str, Any], allowed: set[str], field: str) -> None:
    extras = set(value) - allowed
    if extras:
        raise SeasonMappingError(f"{field} has unknown keys {sorted(extras)}")


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SeasonMappingError(f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SeasonMappingError(f"{field} must be a positive integer")
    return value


def _iso_date(value: Any, field: str) -> date:
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise SeasonMappingError(f"{field} must be an ISO-8601 date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SeasonMappingError(f"{field} must be an ISO-8601 date") from exc


def _approval(value: Any, field: str) -> Approval:
    if not isinstance(value, Mapping):
        raise SeasonMappingError(f"{field} approval must be an object")
    _reject_unknown_keys(value, {"approved_by", "approved_at", "reference"}, field)
    return Approval(
        approved_by=_required_string(value.get("approved_by"), f"{field}.approved_by"),
        approved_at=_iso_date(value.get("approved_at"), f"{field}.approved_at"),
        reference=_required_string(value.get("reference"), f"{field}.reference"),
    )


def _mapping(scope_id: Any, value: Any) -> SeasonMapping:
    field = f"mappings[{scope_id!r}]"
    if not isinstance(scope_id, str) or _SCOPE_RE.fullmatch(scope_id) is None:
        raise SeasonMappingError(f"{field} key must be '<espn_id>:<source_year>'")
    if not isinstance(value, Mapping):
        raise SeasonMappingError(f"{field} must be an object")
    _reject_unknown_keys(
        value,
        {
            "espn_id",
            "source_season_year",
            "platform_league",
            "platform_season_slug",
            "convention",
            "effective_start_date",
            "effective_end_date",
            "approval",
            "downstream_enabled",
        },
        field,
    )
    espn_id = _positive_int(value.get("espn_id"), f"{field}.espn_id")
    source_year = _positive_int(
        value.get("source_season_year"), f"{field}.source_season_year"
    )
    suffix_id, suffix_year = (int(part) for part in scope_id.split(":"))
    if (espn_id, source_year) != (suffix_id, suffix_year):
        raise SeasonMappingError(
            f"{field} scope suffix must equal espn_id/source_season_year"
        )
    convention = _required_string(value.get("convention"), f"{field}.convention")
    if convention not in CONVENTIONS:
        raise SeasonMappingError(
            f"{field}.convention must be one of {sorted(CONVENTIONS)}"
        )
    season_slug = _required_string(
        value.get("platform_season_slug"), f"{field}.platform_season_slug"
    )
    if _SEASON_RE.fullmatch(season_slug) is None:
        raise SeasonMappingError(
            f"{field}.platform_season_slug must be an explicit four-digit slug"
        )
    league_value = value.get("platform_league")
    league = (
        _required_string(league_value, f"{field}.platform_league")
        if league_value is not None
        else None
    )
    enabled = value.get("downstream_enabled", False)
    if not isinstance(enabled, bool):
        raise SeasonMappingError(f"{field}.downstream_enabled must be boolean")
    if enabled and league is None:
        raise SeasonMappingError(
            f"{field}.platform_league is required when downstream is enabled"
        )
    start = _iso_date(value.get("effective_start_date"), f"{field}.effective_start_date")
    end = _iso_date(value.get("effective_end_date"), f"{field}.effective_end_date")
    if start > end:
        raise SeasonMappingError(f"{field} has an invalid date window")
    return SeasonMapping(
        scope_id=scope_id,
        espn_id=espn_id,
        source_season_year=source_year,
        platform_league=league,
        platform_season_slug=season_slug,
        convention=convention,
        effective_start_date=start,
        effective_end_date=end,
        approval=_approval(value.get("approval"), field),
        downstream_enabled=enabled,
    )


def _validate_unique_routes(mappings: tuple[SeasonMapping, ...]) -> None:
    platform_pairs: dict[tuple[str, str], str] = {}
    by_league: dict[str, list[SeasonMapping]] = {}
    for item in mappings:
        if item.platform_league is None:
            continue
        pair = (item.platform_league, item.platform_season_slug)
        owner = platform_pairs.get(pair)
        if owner is not None:
            raise SeasonMappingError(
                f"platform pair {pair!r} is ambiguous between {owner} and {item.scope_id}"
            )
        platform_pairs[pair] = item.scope_id
        if item.downstream_enabled:
            by_league.setdefault(item.platform_league, []).append(item)
    for league, items in by_league.items():
        ordered = sorted(items, key=lambda item: (item.effective_start_date, item.scope_id))
        for previous, current in zip(ordered, ordered[1:]):
            if current.effective_start_date <= previous.effective_end_date:
                raise SeasonMappingError(
                    f"enabled date windows overlap for {league!r}: "
                    f"{previous.scope_id} and {current.scope_id}"
                )


def _platform_season(
    competitions_document: Mapping[str, Any], item: SeasonMapping
) -> Mapping[str, Any]:
    competitions = competitions_document.get("competitions")
    if not isinstance(competitions, list):
        raise SeasonMappingError("competitions document is missing competitions")
    for competition in competitions:
        if not isinstance(competition, Mapping):
            continue
        if competition.get("id") != item.platform_league:
            continue
        for season in competition.get("seasons") or []:
            if isinstance(season, Mapping) and str(season.get("id")) == item.platform_season_slug:
                return {**season, "competition_format": competition.get("competition_format")}
        raise SeasonMappingError(
            f"in_scope platform pair {(item.platform_league, item.platform_season_slug)!r} "
            "is missing from competitions.yaml"
        )
    raise SeasonMappingError(
        f"in_scope platform league {item.platform_league!r} is missing from competitions.yaml"
    )


def _validate_scope_agreement(
    catalog: SeasonMappingCatalog,
    competitions_document: Mapping[str, Any],
    registry_document: Mapping[str, Any] | None,
) -> None:
    competitions = competitions_document.get("competitions")
    if not isinstance(competitions, list):
        raise SeasonMappingError("competitions document is missing competitions")
    in_scope = [
        row
        for row in competitions
        if isinstance(row, Mapping) and row.get("in_scope") is True
    ]
    expected = {row.get("id") for row in in_scope}
    actual = {item.platform_league for item in catalog.enabled}
    if actual != expected:
        raise SeasonMappingError(
            "enabled ESPN mappings must exactly equal competitions.yaml in_scope leagues; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )

    expected_pairs: set[tuple[str, str]] = set()
    for competition in in_scope:
        league = _required_string(competition.get("id"), "competition id")
        seasons = competition.get("seasons")
        if not isinstance(seasons, list) or not seasons:
            raise SeasonMappingError(f"in_scope competition {league!r} has no seasons")
        latest = max(
            seasons,
            key=lambda season: _iso_date(
                season.get("start") if isinstance(season, Mapping) else None,
                f"{league} season start",
            ),
        )
        latest_id = latest.get("id")
        if isinstance(latest_id, bool) or not isinstance(latest_id, (int, str)):
            raise SeasonMappingError(f"{league} season id must be a four-digit slug")
        latest_slug = str(latest_id)
        if _SEASON_RE.fullmatch(latest_slug) is None:
            raise SeasonMappingError(f"{league} season id must be a four-digit slug")
        expected_pairs.add((league, latest_slug))
    actual_pairs = {
        (item.platform_league or "", item.platform_season_slug)
        for item in catalog.enabled
    }
    if actual_pairs != expected_pairs:
        raise SeasonMappingError(
            "enabled ESPN mappings must exactly equal the latest in_scope "
            "competition seasons; "
            f"missing={sorted(expected_pairs - actual_pairs)}, "
            f"extra={sorted(actual_pairs - expected_pairs)}"
        )

    registry_by_id: dict[int, Mapping[str, Any]] = {}
    if registry_document is not None:
        rows = registry_document.get("competitions")
        if not isinstance(rows, list):
            raise SeasonMappingError("registry document is missing competitions")
        for row in rows:
            if isinstance(row, Mapping) and isinstance(row.get("espn_id"), int):
                registry_by_id[row["espn_id"]] = row

    for item in catalog.enabled:
        season = _platform_season(competitions_document, item)
        if (
            _iso_date(season.get("start"), "competitions season start")
            != item.effective_start_date
            or _iso_date(season.get("end"), "competitions season end")
            != item.effective_end_date
        ):
            raise SeasonMappingError(
                f"{item.scope_id} effective dates disagree with competitions.yaml"
            )
        season_format = season.get("season_format", "split_year")
        expected_convention = (
            "tournament"
            if season.get("competition_format") == "group_knockout"
            else "calendar_year"
            if season_format == "single_year"
            else "split_year"
        )
        if item.convention != expected_convention:
            raise SeasonMappingError(
                f"{item.scope_id} convention disagrees with competitions.yaml"
            )

        if registry_document is None:
            continue
        registry_row = registry_by_id.get(item.espn_id)
        if registry_row is None:
            raise SeasonMappingError(f"{item.scope_id} is missing from registry.yaml")
        legacy = registry_row.get("legacy")
        if not isinstance(legacy, Mapping) or legacy.get("league") != item.platform_league:
            raise SeasonMappingError(
                f"{item.scope_id} platform league disagrees with registry.yaml"
            )
        season_aliases = legacy.get("season_aliases")
        if not isinstance(season_aliases, Mapping):
            raise SeasonMappingError(
                f"{item.scope_id} registry legacy season_aliases are missing"
            )
        allowed_aliases = season_aliases.get(item.source_season_year)
        if allowed_aliases is None:
            allowed_aliases = season_aliases.get(str(item.source_season_year))
        if not isinstance(allowed_aliases, list) or item.platform_season_slug not in {
            str(alias) for alias in allowed_aliases
        }:
            raise SeasonMappingError(
                f"{item.scope_id} platform season is missing from registry "
                "legacy season_aliases"
            )
        editions = [
            edition
            for edition in registry_row.get("editions") or []
            if isinstance(edition, Mapping)
            and edition.get("source_season_year") == item.source_season_year
        ]
        if not editions:
            raise SeasonMappingError(
                f"{item.scope_id} source season is missing from registry.yaml"
            )
        if not any(edition.get("current") is True for edition in editions):
            raise SeasonMappingError(
                f"{item.scope_id} source season is not current in registry.yaml"
            )


def validate_mapping_document(
    document: Any,
    *,
    competitions_document: Mapping[str, Any] | None,
    registry_document: Mapping[str, Any] | None,
    require_scope_agreement: bool = True,
) -> SeasonMappingCatalog:
    if not isinstance(document, Mapping):
        raise SeasonMappingError("season mapping document must be an object")
    _reject_unknown_keys(
        document,
        {
            "schema_version",
            "mapping_version",
            "default_downstream_enabled",
            "frozen_scope_reference",
            "mappings",
        },
        "season mapping document",
    )
    if document.get("schema_version") != SCHEMA_VERSION:
        raise SeasonMappingError(f"schema_version must be {SCHEMA_VERSION}")
    if document.get("default_downstream_enabled") is not False:
        raise SeasonMappingError("default_downstream_enabled must be false")
    raw_mappings = document.get("mappings")
    if not isinstance(raw_mappings, Mapping):
        raise SeasonMappingError("mappings must be an object keyed by scope_id")
    mappings = tuple(
        sorted(
            (_mapping(scope_id, value) for scope_id, value in raw_mappings.items()),
            key=lambda item: item.scope_id,
        )
    )
    _validate_unique_routes(mappings)
    mapping_version = _required_string(
        document.get("mapping_version"), "mapping_version"
    )
    mapping_version_date = _iso_date(mapping_version, "mapping_version")
    for item in mappings:
        if item.approval.approved_at > mapping_version_date:
            raise SeasonMappingError(
                f"{item.scope_id} approval date is after mapping_version"
            )
    catalog = SeasonMappingCatalog(
        schema_version=SCHEMA_VERSION,
        mapping_version=mapping_version,
        frozen_scope_reference=_required_string(
            document.get("frozen_scope_reference"), "frozen_scope_reference"
        ),
        mappings=mappings,
    )
    if require_scope_agreement:
        if competitions_document is None:
            raise SeasonMappingError(
                "competitions document is required for in_scope agreement"
            )
        _validate_scope_agreement(catalog, competitions_document, registry_document)
    return catalog


def _load_yaml(path: Path) -> Mapping[str, Any]:
    try:
        document = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeySafeLoader)
    except OSError as exc:
        raise SeasonMappingError(f"cannot read season mapping dependency {path}") from exc
    except yaml.YAMLError as exc:
        raise SeasonMappingError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(document, Mapping):
        raise SeasonMappingError(f"YAML document must be an object: {path}")
    return document


def load_season_mapping(
    path: Path | str = DEFAULT_MAPPING_PATH,
    *,
    competitions_path: Path | str | None = DEFAULT_COMPETITIONS_PATH,
    registry_path: Path | str | None = DEFAULT_REGISTRY_PATH,
    require_scope_agreement: bool = True,
) -> SeasonMappingCatalog:
    mapping_document = _load_yaml(Path(path))
    competitions_document = (
        _load_yaml(Path(competitions_path)) if competitions_path is not None else None
    )
    registry_document = _load_yaml(Path(registry_path)) if registry_path is not None else None
    return validate_mapping_document(
        mapping_document,
        competitions_document=competitions_document,
        registry_document=registry_document,
        require_scope_agreement=require_scope_agreement,
    )


def _sql_string(value: str) -> str:
    if any(marker in value for marker in ("\x00", "\n", "\r", ";", "--", "/*", "*/")):
        raise SeasonMappingError(f"unsafe SQL mapping value {value!r}")
    return "'" + value.replace("'", "''") + "'"


def downstream_scope_values_sql(catalog: SeasonMappingCatalog | None = None) -> str:
    catalog = catalog or load_season_mapping()
    if not catalog.enabled:
        raise SeasonMappingError("no downstream-enabled ESPN season mappings")
    return ",\n".join(
        "    ("
        + ", ".join(
            (
                _sql_string(item.scope_id),
                str(item.espn_id),
                str(item.source_season_year),
                _sql_string(item.platform_league or ""),
                _sql_string(item.platform_season_slug),
                _sql_string(item.convention),
                f"DATE '{item.effective_start_date.isoformat()}'",
                f"DATE '{item.effective_end_date.isoformat()}'",
            )
        )
        + ")"
        for item in catalog.enabled
    )


def downstream_scope_filter_sql() -> str:
    """One exact native-or-legacy join plus the reviewed edition date fence."""

    return """(
        (
            es_source.scope_id = espn_scope.scope_id
            AND es_source.competition_id = espn_scope.espn_id
            AND es_source.source_season_year = espn_scope.source_season_year
        )
        OR (
            es_source.scope_id IS NULL
            AND es_source.competition_id IS NULL
            AND es_source.source_season_year IS NULL
            AND es_source.league = espn_scope.platform_league
            AND CAST(es_source.season AS varchar) = espn_scope.platform_season_slug
        )
    )
    AND TRY_CAST(SUBSTR(CAST(es_source.game AS varchar), 1, 10) AS date)
        BETWEEN espn_scope.effective_start_date AND espn_scope.effective_end_date"""


def render_espn_downstream_sql(
    sql: str, *, catalog: SeasonMappingCatalog | None = None
) -> str:
    """Render both mapping markers or reject a half-wired downstream query."""

    if not isinstance(sql, str):
        raise TypeError("SQL must be a string")
    has_values = VALUES_MARKER in sql
    has_filter = FILTER_MARKER in sql
    if has_values != has_filter:
        raise SeasonMappingError(
            "SQL must contain both ESPN downstream markers or neither"
        )
    if not has_values:
        return sql
    rendered = sql.replace(VALUES_MARKER, downstream_scope_values_sql(catalog))
    rendered = rendered.replace(FILTER_MARKER, downstream_scope_filter_sql())
    if VALUES_MARKER in rendered or FILTER_MARKER in rendered:
        raise SeasonMappingError("ESPN downstream SQL markers leaked after render")
    return rendered


__all__ = [
    "Approval",
    "CONVENTIONS",
    "DEFAULT_MAPPING_PATH",
    "FILTER_MARKER",
    "SCHEMA_VERSION",
    "SeasonMapping",
    "SeasonMappingCatalog",
    "SeasonMappingError",
    "VALUES_MARKER",
    "downstream_scope_filter_sql",
    "downstream_scope_values_sql",
    "load_season_mapping",
    "render_espn_downstream_sql",
    "validate_mapping_document",
]
