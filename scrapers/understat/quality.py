"""Exact in-memory quality gates for an Understat league-season scope.

Validation happens before any Bronze partition is replaced.  It checks the
seven-table contract, exact scope, natural keys and cross-table game/player
coverage.  Row floors are deliberately derived from the schedule rather than
from a whole historical table, so old data cannot mask a broken current scope.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

from scrapers.understat.catalog import LEAGUE_BY_CANONICAL
from scrapers.understat.manifest import (
    CONTRACT_VERSION,
    SCHEDULE_ENTITY,
    UNDERSTAT_ENTITIES,
    ManifestStatus,
    ScopeAttempt,
    ScopeKey,
    new_attempt_id,
    utc_now_iso,
)
from scrapers.understat.contracts import TABLE_CONTRACTS
from scrapers.understat.parsers import TEAM_BREAKDOWN_DIMENSIONS


NATURAL_KEYS: Mapping[str, tuple[str, ...]] = {
    contract.table_name: contract.natural_key for contract in TABLE_CONTRACTS
}
REQUIRED_COLUMNS: Mapping[str, tuple[str, ...]] = {
    contract.table_name: contract.required_columns for contract in TABLE_CONTRACTS
}

if tuple(NATURAL_KEYS) != UNDERSTAT_ENTITIES:
    raise RuntimeError("Understat manifest and table contract entity order diverged")

GAME_ENTITIES = (
    "understat_team_match_stats",
    "understat_shots",
    "understat_player_match_stats",
)

TEAM_MATCH_CORE_COLUMNS = tuple(
    f"{side}_{metric}"
    for side in ("home", "away")
    for metric in (
        "team_id",
        "points",
        "expected_points",
        "goals",
        "xg",
        "np_xg",
        "np_xg_difference",
        "ppda_att",
        "ppda_def",
        "ppda_allowed_att",
        "ppda_allowed_def",
        "deep_completions",
        "deep_allowed",
    )
)


class UnderstatQualityError(RuntimeError):
    """A scope cannot safely be published."""


@dataclass(frozen=True)
class QualityIssue:
    code: str
    message: str
    status: Optional[ManifestStatus] = None
    entity: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "status": self.status.value if self.status else "warning",
            "entity": self.entity,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class QualityReport:
    scope: ScopeKey
    active: bool
    status: ManifestStatus
    entity_statuses: Mapping[str, ManifestStatus]
    row_counts: Mapping[str, int]
    natural_key_counts: Mapping[str, int]
    payload_hashes: Mapping[str, str]
    completed_game_count: int
    issues: Sequence[QualityIssue] = ()
    batch_id: Optional[str] = None

    @property
    def publishable(self) -> bool:
        return self.status is ManifestStatus.COMPLETE

    @property
    def passed(self) -> bool:
        return self.status in {
            ManifestStatus.COMPLETE,
            ManifestStatus.UPSTREAM_PENDING,
            ManifestStatus.NOT_PUBLISHED,
        }

    def raise_for_failure(self) -> None:
        if self.passed:
            return
        messages = "; ".join(issue.message for issue in self.issues)
        raise UnderstatQualityError(
            f"{self.scope.league}/{self.scope.season}: "
            f"{self.status.value}: {messages or 'quality gate failed'}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "active": self.active,
            "status": self.status.value,
            "entity_statuses": {
                key: value.value for key, value in self.entity_statuses.items()
            },
            "row_counts": dict(self.row_counts),
            "natural_key_counts": dict(self.natural_key_counts),
            "payload_hashes": dict(self.payload_hashes),
            "completed_game_count": self.completed_game_count,
            "batch_id": self.batch_id,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def _jsonable(value: Any) -> Any:
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) else value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {
            str(key): _jsonable(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _jsonable(item())
        except (TypeError, ValueError):
            pass
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def dataframe_content_hash(frame: pd.DataFrame) -> str:
    """Stable content checksum excluding volatile ingestion metadata."""
    columns = sorted(column for column in frame.columns if not column.startswith("_"))
    header = json.dumps(columns, ensure_ascii=False, separators=(",", ":"))
    rows = []
    for values in frame.loc[:, columns].itertuples(index=False, name=None):
        rows.append(
            json.dumps(
                [_jsonable(value) for value in values],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    digest = hashlib.sha256()
    digest.update(header.encode("utf-8"))
    for row in sorted(rows):
        digest.update(b"\n")
        digest.update(row.encode("utf-8"))
    return digest.hexdigest()


def _as_frame(value: Optional[pd.DataFrame]) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if not isinstance(value, pd.DataFrame):
        raise TypeError("Understat scope entities must be pandas DataFrames or None")
    return value


def _normalized_values(series: pd.Series) -> set[str]:
    return {str(value).strip() for value in series.dropna().tolist()}


def _normalized_ids(values: Iterable[Any]) -> set[str]:
    return {str(value).strip() for value in values if not pd.isna(value)}


def _truth_value(value: Any) -> Optional[bool]:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if value is True or value == 1:
        return True
    if value is False or value == 0:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return None


def _coverage_allowlist(
    value: object,
) -> tuple[set[str], set[str]]:
    """Normalize ``{missing: {id: reason}, extra: ...}`` coverage exceptions.

    A flat mapping/iterable remains a convenient shorthand for missing IDs.
    Reasons are intentionally not interpreted here; callers retain them in
    config/code review while the report records the affected IDs.
    """
    if value is None:
        return set(), set()
    if isinstance(value, Mapping) and ({"missing", "extra"} & set(value)):
        missing = value.get("missing", ())
        extra = value.get("extra", ())
    else:
        missing, extra = value, ()

    def ids(raw: object) -> set[str]:
        if isinstance(raw, Mapping):
            raw = raw.keys()
        if isinstance(raw, (str, int)):
            raw = (raw,)
        return _normalized_ids(raw or ())

    return ids(missing), ids(extra)


def _issue(
    issues: list[QualityIssue],
    code: str,
    message: str,
    *,
    status: Optional[ManifestStatus],
    entity: Optional[str] = None,
    **details: Any,
) -> None:
    issues.append(
        QualityIssue(
            code=code,
            message=message,
            status=status,
            entity=entity,
            details=details,
        )
    )


def _sample(values: Iterable[str], limit: int = 10) -> list[str]:
    return sorted(set(values))[:limit]


def _is_missing_scalar(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def validate_understat_scope(
    frames: Mapping[str, Optional[pd.DataFrame]],
    *,
    scope: ScopeKey,
    active: bool,
    previous_row_counts: Optional[Mapping[str, int]] = None,
    payload_hashes: Optional[Mapping[str, str]] = None,
    batch_id: Optional[str] = None,
    coverage_exceptions: Optional[Mapping[str, object]] = None,
) -> QualityReport:
    """Validate all seven entity frames for exactly one league-season scope.

    ``previous_row_counts`` normally comes from the latest usable source
    observation (complete or upstream-pending). A source response may be
    legitimately empty before publication, but an entity that was previously
    non-empty is never allowed to disappear.
    """
    if not isinstance(scope, ScopeKey):
        raise TypeError("scope must be a ScopeKey")
    supplied = {str(key): value for key, value in dict(frames).items()}
    previous = {
        str(key): int(value)
        for key, value in dict(previous_row_counts or {}).items()
    }
    supplied_hashes = {
        str(key): str(value or "").strip()
        for key, value in dict(payload_hashes or {}).items()
    }
    exceptions = dict(coverage_exceptions or {})
    issues: list[QualityIssue] = []

    missing_entities = set(UNDERSTAT_ENTITIES) - set(supplied)
    extra_entities = set(supplied) - set(UNDERSTAT_ENTITIES)
    for entity in sorted(missing_entities):
        _issue(
            issues,
            "missing_entity",
            f"{entity}: missing from the seven-table scope contract",
            status=ManifestStatus.SCHEMA_DRIFT,
            entity=entity,
        )
    if extra_entities:
        _issue(
            issues,
            "unexpected_entities",
            f"unexpected Understat entities: {sorted(extra_entities)}",
            status=ManifestStatus.SCHEMA_DRIFT,
            extra=sorted(extra_entities),
        )

    normalized_frames = {
        entity: _as_frame(supplied.get(entity)) for entity in UNDERSTAT_ENTITIES
    }
    row_counts = {entity: len(frame) for entity, frame in normalized_frames.items()}
    key_counts: dict[str, int] = {}
    hashes: dict[str, str] = {}
    entity_schema_broken: set[str] = set()
    league_definition = LEAGUE_BY_CANONICAL.get(scope.league)
    if league_definition is None:
        _issue(
            issues,
            "unsupported_scope_league",
            f"scope league is not a production Understat league: {scope.league}",
            status=ManifestStatus.SCHEMA_DRIFT,
        )

    for entity, frame in normalized_frames.items():
        hashes[entity] = supplied_hashes.get(entity) or (
            dataframe_content_hash(frame) if not frame.empty else ""
        )
        key = NATURAL_KEYS[entity]
        if frame.empty:
            key_counts[entity] = 0
            if previous.get(entity, 0) > 0:
                _issue(
                    issues,
                    "previously_populated_entity_empty",
                    f"{entity}: current result is empty but the latest usable "
                    f"source observation had {previous[entity]} rows",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    previous_rows=previous[entity],
                )
            continue

        required = set(REQUIRED_COLUMNS[entity])
        missing_columns = sorted(required - set(frame.columns))
        if batch_id and "_batch_id" not in frame.columns:
            missing_columns.append("_batch_id")
        if missing_columns:
            entity_schema_broken.add(entity)
            _issue(
                issues,
                "missing_columns",
                f"{entity}: required columns are missing: {missing_columns}",
                status=ManifestStatus.SCHEMA_DRIFT,
                entity=entity,
                columns=missing_columns,
            )

        if set(key).issubset(frame.columns):
            key_frame = frame.loc[:, list(key)]
            non_null_keys = key_frame.dropna(how="any")
            key_counts[entity] = len(non_null_keys.drop_duplicates())
            null_rows = len(frame) - len(non_null_keys)
            if null_rows:
                _issue(
                    issues,
                    "null_natural_key",
                    f"{entity}: {null_rows} rows have a null natural key",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    null_rows=null_rows,
                    key=list(key),
                )
            duplicate_rows = len(non_null_keys) - key_counts[entity]
            if duplicate_rows:
                _issue(
                    issues,
                    "duplicate_natural_key",
                    f"{entity}: {duplicate_rows} duplicate natural-key rows",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    duplicate_rows=duplicate_rows,
                    key=list(key),
                )
        else:
            key_counts[entity] = 0

        if {"league", "season"}.issubset(frame.columns):
            actual_leagues = _normalized_values(frame["league"])
            actual_seasons = _normalized_values(frame["season"])
            actual_source_seasons = (
                _normalized_values(frame["source_season_id"])
                if "source_season_id" in frame.columns
                else set()
            )
            scope_columns = [
                column
                for column in ("league", "season", "source_season_id")
                if column in frame.columns
            ]
            null_scope_rows = int(
                frame.loc[:, scope_columns].isna().any(axis=1).sum()
            )
            if (
                actual_leagues != {scope.league}
                or actual_seasons != {scope.season}
                or actual_source_seasons != {str(scope.source_season_id)}
                or null_scope_rows
            ):
                _issue(
                    issues,
                    "scope_mismatch",
                    f"{entity}: rows escape requested scope "
                    f"{scope.league}/{scope.season}",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    leagues=sorted(actual_leagues),
                    seasons=sorted(actual_seasons),
                    source_season_ids=sorted(actual_source_seasons),
                    null_scope_rows=null_scope_rows,
                )

        if "league_id" in frame.columns and league_definition is not None:
            actual_league_ids = _normalized_ids(frame["league_id"])
            null_league_id_rows = int(frame["league_id"].isna().sum())
            expected_league_id = str(league_definition.source_league_id)
            if actual_league_ids != {expected_league_id} or null_league_id_rows:
                _issue(
                    issues,
                    "league_id_mismatch",
                    f"{entity}: league_id does not match {scope.league} "
                    f"(expected {expected_league_id})",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    league_ids=sorted(actual_league_ids),
                    expected_league_id=expected_league_id,
                    null_scope_rows=null_league_id_rows,
                )

        if batch_id and "_batch_id" in frame.columns:
            actual_batches = _normalized_values(frame["_batch_id"])
            null_batch_rows = int(frame["_batch_id"].isna().sum())
            if actual_batches != {batch_id} or null_batch_rows:
                _issue(
                    issues,
                    "batch_mismatch",
                    f"{entity}: rows do not all carry batch_id={batch_id}",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    batches=sorted(actual_batches),
                    null_batch_rows=null_batch_rows,
                )

        if entity == "understat_shots" and "xg" in frame.columns:
            numeric_xg = pd.to_numeric(frame["xg"], errors="coerce")
            invalid_xg = int(
                (numeric_xg.isna() | ~numeric_xg.between(0.0, 1.0)).sum()
            )
            if invalid_xg:
                _issue(
                    issues,
                    "invalid_xg",
                    f"{entity}: {invalid_xg} xg values are null, non-numeric or "
                    "outside [0, 1]",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    invalid_rows=invalid_xg,
                )
            for coordinate in ("location_x", "location_y"):
                if coordinate not in frame.columns:
                    continue
                values = pd.to_numeric(frame[coordinate], errors="coerce")
                invalid_coordinates = int(
                    (values.isna() | ~values.between(0.0, 1.0)).sum()
                )
                if invalid_coordinates:
                    _issue(
                        issues,
                        "invalid_shot_coordinate",
                        f"{entity}: {invalid_coordinates} {coordinate} values are "
                        "null, non-numeric or outside [0, 1]",
                        status=ManifestStatus.CONTRACT_FAILURE,
                        entity=entity,
                        column=coordinate,
                        invalid_rows=invalid_coordinates,
                    )

    schedule = normalized_frames[SCHEDULE_ENTITY]
    completed_ids: set[str] = set()
    if not schedule.empty and SCHEDULE_ENTITY not in entity_schema_broken:
        forecast_columns = (
            "forecast_home_win",
            "forecast_draw",
            "forecast_away_win",
        )
        forecasts = schedule.loc[:, list(forecast_columns)].apply(
            pd.to_numeric, errors="coerce"
        )
        supplied_forecasts = forecasts.notna().sum(axis=1)
        partial_forecasts = int(supplied_forecasts.isin((1, 2)).sum())
        forecast_sum_tolerance = 0.02
        forecast_total_lower_bound = 1.0 - forecast_sum_tolerance
        forecast_total_upper_bound = 1.0 + forecast_sum_tolerance
        forecast_sums = forecasts.sum(axis=1)
        # Three Float64 addends can accumulate one unit-scale ULP each.
        forecast_sum_epsilon = math.ulp(1.0) * len(forecast_columns)
        forecast_sums_below_tolerance = (
            forecast_sums < forecast_total_lower_bound
        ) & ~forecast_sums.map(
            lambda total: math.isclose(
                float(total),
                forecast_total_lower_bound,
                rel_tol=0.0,
                abs_tol=forecast_sum_epsilon,
            )
        )
        forecast_sums_above_tolerance = (
            forecast_sums > forecast_total_upper_bound
        ) & ~forecast_sums.map(
            lambda total: math.isclose(
                float(total),
                forecast_total_upper_bound,
                rel_tol=0.0,
                abs_tol=forecast_sum_epsilon,
            )
        )
        forecast_sums_outside_tolerance = (
            forecast_sums_below_tolerance | forecast_sums_above_tolerance
        )
        invalid_forecasts = int(
            (
                (supplied_forecasts == 3)
                & (
                    (~((forecasts >= 0.0) & (forecasts <= 1.0)).all(axis=1))
                    | forecast_sums_outside_tolerance
                )
            ).sum()
        )
        if partial_forecasts or invalid_forecasts:
            _issue(
                issues,
                "invalid_forecast",
                f"{SCHEDULE_ENTITY}: {partial_forecasts} partial and "
                f"{invalid_forecasts} invalid probability forecasts",
                status=ManifestStatus.CONTRACT_FAILURE,
                entity=SCHEDULE_ENTITY,
                partial_rows=partial_forecasts,
                invalid_rows=invalid_forecasts,
            )
        result_values = schedule["is_result"].map(_truth_value)
        data_values = schedule["has_data"].map(_truth_value)
        invalid = int(result_values.isna().sum() + data_values.isna().sum())
        if invalid:
            entity_schema_broken.add(SCHEDULE_ENTITY)
            _issue(
                issues,
                "invalid_schedule_flags",
                f"{SCHEDULE_ENTITY}: {invalid} is_result/has_data values are not boolean",
                status=ManifestStatus.SCHEMA_DRIFT,
                entity=SCHEDULE_ENTITY,
                invalid_rows=invalid,
            )
        else:
            completed_ids = _normalized_ids(
                schedule.loc[result_values.astype(bool), "game_id"]
            )
            data_without_result = _normalized_ids(
                schedule.loc[
                    data_values.astype(bool) & ~result_values.astype(bool), "game_id"
                ]
            )
            if data_without_result:
                _issue(
                    issues,
                    "data_without_result",
                    f"{SCHEDULE_ENTITY}: has_data is true for games not marked result",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=SCHEDULE_ENTITY,
                    game_ids=_sample(data_without_result),
                    count=len(data_without_result),
                )

    non_schedule_rows = sum(
        row_counts[entity] for entity in UNDERSTAT_ENTITIES[1:]
    )
    if schedule.empty:
        if non_schedule_rows:
            _issue(
                issues,
                "facts_without_schedule",
                "Understat facts exist while the scope schedule is empty",
                status=ManifestStatus.CONTRACT_FAILURE,
                entity=SCHEDULE_ENTITY,
                non_schedule_rows=non_schedule_rows,
            )
        elif not active:
            _issue(
                issues,
                "closed_scope_not_published",
                "closed Understat scope has no schedule and cannot be treated "
                "as an expected future-season publication delay",
                status=ManifestStatus.CONTRACT_FAILURE,
                entity=SCHEDULE_ENTITY,
            )
    elif not entity_schema_broken and not completed_ids:
        if not active:
            _issue(
                issues,
                "closed_scope_without_results",
                "closed Understat scope has a schedule but no completed games",
                status=ManifestStatus.CONTRACT_FAILURE,
                entity=SCHEDULE_ENTITY,
            )
        elif non_schedule_rows:
            _issue(
                issues,
                "schedule_only_scope_has_facts",
                "active scope has no completed games but non-schedule facts exist",
                status=ManifestStatus.CONTRACT_FAILURE,
                entity=SCHEDULE_ENTITY,
                non_schedule_rows=non_schedule_rows,
            )

    if completed_ids:
        for entity in UNDERSTAT_ENTITIES[1:]:
            if row_counts[entity] == 0:
                _issue(
                    issues,
                    "completed_scope_entity_empty",
                    f"{entity}: empty despite {len(completed_ids)} completed games",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    completed_games=len(completed_ids),
                )

        for entity in GAME_ENTITIES:
            frame = normalized_frames[entity]
            if frame.empty or "game_id" not in frame.columns:
                continue
            actual = _normalized_ids(frame["game_id"])
            allowed_missing, allowed_extra = _coverage_allowlist(
                exceptions.get(entity)
            )
            missing = completed_ids - actual
            extra = actual - completed_ids
            unallowed_missing = missing - allowed_missing
            unallowed_extra = extra - allowed_extra
            if unallowed_missing:
                _issue(
                    issues,
                    "missing_completed_games",
                    f"{entity}: missing {len(unallowed_missing)} completed games",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    game_ids=_sample(unallowed_missing),
                    count=len(unallowed_missing),
                )
            if unallowed_extra:
                _issue(
                    issues,
                    "unexpected_game_coverage",
                    f"{entity}: contains {len(unallowed_extra)} games not marked complete",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity=entity,
                    game_ids=_sample(unallowed_extra),
                    count=len(unallowed_extra),
                )
            allowlisted = (missing & allowed_missing) | (extra & allowed_extra)
            if allowlisted:
                _issue(
                    issues,
                    "allowlisted_game_coverage",
                    f"{entity}: {len(allowlisted)} source coverage exceptions allowed",
                    status=None,
                    entity=entity,
                    game_ids=_sample(allowlisted),
                    count=len(allowlisted),
                )

        player_match = normalized_frames["understat_player_match_stats"]
        roster_columns = {"game_id", "team_id"}
        schedule_team_columns = {"game_id", "home_team_id", "away_team_id"}
        if (
            not player_match.empty
            and roster_columns.issubset(player_match.columns)
            and schedule_team_columns.issubset(schedule.columns)
        ):
            allowed_missing, _ = _coverage_allowlist(
                exceptions.get("understat_player_match_stats")
            )
            actual_teams_by_game = {
                game_id: _normalized_ids(group["team_id"])
                for game_id, group in player_match.assign(
                    _normalized_game_id=player_match["game_id"].map(
                        lambda value: str(value).strip()
                    )
                ).groupby("_normalized_game_id")
            }
            actual_side_teams_by_game: dict[str, set[tuple[str, str]]] = {}
            if "team_side" in player_match.columns:
                for game_id, side, team_id in player_match.loc[
                    :, ["game_id", "team_side", "team_id"]
                ].itertuples(index=False, name=None):
                    if _is_missing_scalar(side) or _is_missing_scalar(team_id):
                        continue
                    actual_side_teams_by_game.setdefault(
                        str(game_id).strip(), set()
                    ).add((str(side).strip(), str(team_id).strip()))
            roster_gaps: dict[str, list[str]] = {}
            roster_side_mismatches: dict[str, dict[str, list[str]]] = {}
            for row in schedule.loc[
                :, ["game_id", "home_team_id", "away_team_id"]
            ].itertuples(index=False, name=None):
                game_id = str(row[0]).strip()
                if game_id not in completed_ids or game_id in allowed_missing:
                    continue
                expected_teams = _normalized_ids(row[1:])
                missing_teams = expected_teams - actual_teams_by_game.get(
                    game_id, set()
                )
                if missing_teams:
                    roster_gaps[game_id] = _sample(missing_teams)
                expected_side_teams = {
                    ("h", str(row[1]).strip()),
                    ("a", str(row[2]).strip()),
                }
                actual_side_teams = actual_side_teams_by_game.get(game_id, set())
                if actual_side_teams != expected_side_teams:
                    roster_side_mismatches[game_id] = {
                        "expected": sorted(
                            f"{side}:{team_id}"
                            for side, team_id in expected_side_teams
                        ),
                        "actual": sorted(
                            f"{side}:{team_id}"
                            for side, team_id in actual_side_teams
                        ),
                    }
            if roster_gaps:
                _issue(
                    issues,
                    "player_match_team_coverage_mismatch",
                    "understat_player_match_stats must contain both schedule "
                    "team IDs for every completed, non-allowlisted game",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity="understat_player_match_stats",
                    games=dict(list(sorted(roster_gaps.items()))[:10]),
                    affected_game_count=len(roster_gaps),
                )
            if roster_side_mismatches:
                _issue(
                    issues,
                    "player_match_side_team_mismatch",
                    "understat_player_match_stats team_side/team_id pairs must "
                    "match the schedule home and away teams exactly",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity="understat_player_match_stats",
                    games=dict(
                        list(sorted(roster_side_mismatches.items()))[:10]
                    ),
                    affected_game_count=len(roster_side_mismatches),
                )

        team_match = normalized_frames["understat_team_match_stats"]
        core_columns = {"game_id", *TEAM_MATCH_CORE_COLUMNS}
        if not team_match.empty and core_columns.issubset(team_match.columns):
            incomplete_games: dict[str, list[str]] = {}
            for row in team_match.loc[
                :, ["game_id", *TEAM_MATCH_CORE_COLUMNS]
            ].itertuples(index=False, name=None):
                game_id = str(row[0]).strip()
                if game_id not in completed_ids:
                    continue
                missing_columns = [
                    column
                    for column, value in zip(TEAM_MATCH_CORE_COLUMNS, row[1:])
                    if _is_missing_scalar(value)
                ]
                if missing_columns:
                    incomplete_games[game_id] = missing_columns
            if incomplete_games:
                _issue(
                    issues,
                    "team_match_side_metrics_incomplete",
                    "understat_team_match_stats must contain non-null core "
                    "metrics for both sides of every completed game",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity="understat_team_match_stats",
                    games=dict(list(sorted(incomplete_games.items()))[:10]),
                    affected_game_count=len(incomplete_games),
                )

        players = normalized_frames["understat_players"]
        if not players.empty and "player_id" in players.columns:
            player_ids = _normalized_ids(players["player_id"])
            for entity in (
                "understat_shots",
                "understat_player_match_stats",
                "understat_player_team_season_stats",
            ):
                frame = normalized_frames[entity]
                if frame.empty or "player_id" not in frame.columns:
                    continue
                missing_players = _normalized_ids(frame["player_id"]) - player_ids
                if missing_players:
                    _issue(
                        issues,
                        "player_reference_gap",
                        f"{entity}: {len(missing_players)} player IDs are absent "
                        "from understat_players",
                        status=ManifestStatus.CONTRACT_FAILURE,
                        entity=entity,
                        player_ids=_sample(missing_players),
                        count=len(missing_players),
                    )

        player_team = normalized_frames["understat_player_team_season_stats"]
        pair_columns = {"team_id", "player_id"}
        if (
            not player_match.empty
            and not player_team.empty
            and pair_columns.issubset(player_match.columns)
            and pair_columns.issubset(player_team.columns)
        ):
            known_pairs = {
                (str(team), str(player))
                for team, player in player_team.loc[
                    :, ["team_id", "player_id"]
                ].dropna().itertuples(index=False, name=None)
            }
            match_pairs = {
                (str(team), str(player))
                for team, player in player_match.loc[
                    :, ["team_id", "player_id"]
                ].dropna().itertuples(index=False, name=None)
            }
            missing_pairs = match_pairs - known_pairs
            if missing_pairs:
                _issue(
                    issues,
                    "player_team_reference_gap",
                    "understat_player_match_stats contains player/team pairs absent "
                    "from understat_player_team_season_stats",
                    status=ManifestStatus.CONTRACT_FAILURE,
                    entity="understat_player_match_stats",
                    pairs=[list(pair) for pair in sorted(missing_pairs)[:10]],
                    count=len(missing_pairs),
                )

        breakdown_entity = "understat_team_season_breakdowns"
        breakdowns = normalized_frames[breakdown_entity]
        schedule_team_columns = {"home_team_id", "away_team_id"}
        if schedule_team_columns.issubset(schedule.columns):
            expected_team_ids = _normalized_ids(schedule["home_team_id"]) | (
                _normalized_ids(schedule["away_team_id"])
            )
            if not breakdowns.empty and {"team_id", "dimension"}.issubset(
                breakdowns.columns
            ):
                actual_team_ids = _normalized_ids(breakdowns["team_id"])
                missing_teams = expected_team_ids - actual_team_ids
                extra_teams = actual_team_ids - expected_team_ids
                if missing_teams or extra_teams:
                    _issue(
                        issues,
                        "team_breakdown_coverage_mismatch",
                        f"{breakdown_entity}: source-team coverage is not exact",
                        status=ManifestStatus.CONTRACT_FAILURE,
                        entity=breakdown_entity,
                        missing_team_ids=_sample(missing_teams),
                        extra_team_ids=_sample(extra_teams),
                    )

                observed_pairs = {
                    (str(team_id), str(dimension))
                    for team_id, dimension in breakdowns.loc[
                        :, ["team_id", "dimension"]
                    ].dropna().itertuples(index=False, name=None)
                }
                dimension_gaps: dict[str, dict[str, list[str]]] = {}
                for team_id in sorted(expected_team_ids):
                    observed = {
                        dimension
                        for observed_team, dimension in observed_pairs
                        if observed_team == team_id
                    }
                    missing_dimensions = TEAM_BREAKDOWN_DIMENSIONS - observed
                    extra_dimensions = observed - TEAM_BREAKDOWN_DIMENSIONS
                    if missing_dimensions or extra_dimensions:
                        dimension_gaps[team_id] = {
                            "missing": sorted(missing_dimensions),
                            "extra": sorted(extra_dimensions),
                        }
                if dimension_gaps:
                    _issue(
                        issues,
                        "team_breakdown_dimension_mismatch",
                        f"{breakdown_entity}: every source team must contain "
                        "exactly the seven known dimensions",
                        status=ManifestStatus.CONTRACT_FAILURE,
                        entity=breakdown_entity,
                        teams=dict(list(dimension_gaps.items())[:10]),
                        affected_team_count=len(dimension_gaps),
                    )

    hard_issues = [issue for issue in issues if issue.status is not None]
    if any(issue.status is ManifestStatus.SCHEMA_DRIFT for issue in hard_issues):
        status = ManifestStatus.SCHEMA_DRIFT
    elif hard_issues:
        status = ManifestStatus.CONTRACT_FAILURE
    elif schedule.empty:
        status = ManifestStatus.NOT_PUBLISHED
    elif not completed_ids:
        status = ManifestStatus.UPSTREAM_PENDING
    else:
        status = ManifestStatus.COMPLETE

    if status is ManifestStatus.COMPLETE:
        entity_statuses = {
            entity: ManifestStatus.COMPLETE for entity in UNDERSTAT_ENTITIES
        }
    elif status is ManifestStatus.NOT_PUBLISHED:
        entity_statuses = {
            entity: ManifestStatus.NOT_PUBLISHED for entity in UNDERSTAT_ENTITIES
        }
    elif status is ManifestStatus.UPSTREAM_PENDING:
        entity_statuses = {
            SCHEDULE_ENTITY: ManifestStatus.COMPLETE,
            **{
                entity: ManifestStatus.UPSTREAM_PENDING
                for entity in UNDERSTAT_ENTITIES[1:]
            },
        }
    else:
        global_failure = any(
            issue.status is status and issue.entity is None for issue in hard_issues
        )
        entity_statuses = {}
        for entity in UNDERSTAT_ENTITIES:
            entity_issues = [issue for issue in hard_issues if issue.entity == entity]
            if any(
                issue.status is ManifestStatus.SCHEMA_DRIFT
                for issue in entity_issues
            ):
                entity_statuses[entity] = ManifestStatus.SCHEMA_DRIFT
            elif entity_issues:
                entity_statuses[entity] = ManifestStatus.CONTRACT_FAILURE
            elif global_failure:
                entity_statuses[entity] = status
            elif row_counts[entity] > 0:
                entity_statuses[entity] = ManifestStatus.COMPLETE
            else:
                entity_statuses[entity] = status

    return QualityReport(
        scope=scope,
        active=bool(active),
        status=status,
        entity_statuses=entity_statuses,
        row_counts=row_counts,
        natural_key_counts=key_counts,
        payload_hashes=hashes,
        completed_game_count=len(completed_ids),
        issues=tuple(issues),
        batch_id=batch_id,
    )


def build_scope_attempt(
    report: QualityReport,
    *,
    batch_id: str,
    run_id: str,
    mode: str,
    parser_version: str,
    attempt_id: Optional[str] = None,
    attempt_no: int = 1,
    contract_version: str = CONTRACT_VERSION,
    started_at: Optional[str] = None,
    completed_at: Optional[str] = None,
) -> ScopeAttempt:
    """Convert a quality report to the single append-only publication row."""
    if report.batch_id and report.batch_id != batch_id:
        raise ValueError(
            f"quality report batch_id={report.batch_id} does not match {batch_id}"
        )
    hard = [issue for issue in report.issues if issue.status is not None]
    error_message = "; ".join(issue.message for issue in hard) or None
    return ScopeAttempt(
        scope=report.scope,
        status=report.status,
        batch_id=batch_id,
        run_id=run_id,
        attempt_id=attempt_id or new_attempt_id(),
        attempt_no=attempt_no,
        mode=mode,
        parser_version=parser_version,
        contract_version=contract_version,
        entity_statuses=report.entity_statuses,
        row_counts=report.row_counts,
        natural_key_counts=report.natural_key_counts,
        payload_hashes=report.payload_hashes,
        quality=report.to_dict(),
        started_at=started_at or utc_now_iso(),
        completed_at=completed_at or utc_now_iso(),
        error_type=report.status.value if hard else None,
        error_message=error_message,
    )


def build_failure_attempt(
    *,
    scope: ScopeKey,
    status: ManifestStatus | str,
    batch_id: str,
    run_id: str,
    mode: str,
    parser_version: str,
    error_message: str,
    error_type: Optional[str] = None,
    attempt_id: Optional[str] = None,
    attempt_no: int = 1,
    contract_version: str = CONTRACT_VERSION,
    entity_statuses: Optional[Mapping[str, ManifestStatus | str]] = None,
    row_counts: Optional[Mapping[str, int]] = None,
    natural_key_counts: Optional[Mapping[str, int]] = None,
    payload_hashes: Optional[Mapping[str, str]] = None,
    started_at: Optional[str] = None,
    completed_at: Optional[str] = None,
) -> ScopeAttempt:
    """Build an auditable terminal attempt when extraction/DQ cannot report.

    The runner owns exception classification.  This helper only accepts the
    three failure states and fills all seven maps, preventing an early HTTP or
    schema exception from producing an incomplete manifest row.
    """
    status = status if isinstance(status, ManifestStatus) else ManifestStatus(status)
    if status not in {
        ManifestStatus.RETRYABLE_FAILURE,
        ManifestStatus.CONTRACT_FAILURE,
        ManifestStatus.SCHEMA_DRIFT,
    }:
        raise ValueError("build_failure_attempt requires a failure status")

    def full_map(values: Optional[Mapping[str, Any]], default: Any) -> dict[str, Any]:
        supplied = dict(values or {})
        unknown = set(supplied) - set(UNDERSTAT_ENTITIES)
        if unknown:
            raise ValueError(f"unknown Understat entities: {sorted(unknown)}")
        return {
            entity: supplied.get(entity, default) for entity in UNDERSTAT_ENTITIES
        }

    statuses = full_map(entity_statuses, status)
    rows = full_map(row_counts, 0)
    keys = full_map(natural_key_counts, 0)
    hashes = full_map(payload_hashes, "")
    return ScopeAttempt(
        scope=scope,
        status=status,
        batch_id=batch_id,
        run_id=run_id,
        attempt_id=attempt_id or new_attempt_id(),
        attempt_no=attempt_no,
        mode=mode,
        parser_version=parser_version,
        contract_version=contract_version,
        entity_statuses=statuses,
        row_counts=rows,
        natural_key_counts=keys,
        payload_hashes=hashes,
        quality={
            "scope": scope.to_dict(),
            "status": status.value,
            "error_type": error_type or status.value,
            "error_message": str(error_message),
        },
        started_at=started_at or utc_now_iso(),
        completed_at=completed_at or utc_now_iso(),
        error_type=error_type or status.value,
        error_message=str(error_message),
    )


__all__ = [
    "GAME_ENTITIES",
    "NATURAL_KEYS",
    "REQUIRED_COLUMNS",
    "QualityIssue",
    "QualityReport",
    "UnderstatQualityError",
    "build_failure_attempt",
    "build_scope_attempt",
    "dataframe_content_hash",
    "validate_understat_scope",
]
