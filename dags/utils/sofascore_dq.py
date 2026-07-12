"""Fail-closed SofaScore coverage and data-quality contracts.

The capture engine owns transport; this module owns the boundary between an
observed JSON payload and data that is safe to publish.  It is intentionally
network-free and mostly pure so the same checks run after a live capture, an
offline raw replay and in unit tests.

The versioned source of truth is ``configs/sofascore/endpoint_coverage.yaml``.
No endpoint may be fetched without one of the four explicit coverage outcomes
and no normalized table may omit a grain or natural key.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import yaml


_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COVERAGE_FILE = _REPO_ROOT / "configs" / "sofascore" / "endpoint_coverage.yaml"

COVERAGE_STATUSES = frozenset(
    {"normalized", "raw-only", "unsupported", "intentionally-excluded"}
)
MANIFEST_STATES = frozenset(
    {
        "success",
        "legitimate_empty",
        "not_supported",
        "retryable_failure",
        "schema_error",
    }
)
REQUIRED_ACCEPTABLE_STATES = frozenset({"success", "legitimate_empty"})
OPTIONAL_ACCEPTABLE_STATES = frozenset({"success", "legitimate_empty", "not_supported"})
RETRYABLE_HTTP_STATUSES = frozenset({403, 429, 500, 502, 503, 504})

_SAFE_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


class SofaScoreContractError(ValueError):
    """The versioned endpoint/table declaration is internally inconsistent."""


class SofaScoreDQViolation(RuntimeError):
    """Publication must stop because a fail-closed SofaScore gate failed."""


@dataclass(frozen=True)
class DQFinding:
    code: str
    message: str
    count: int = 1
    examples: tuple[Any, ...] = ()


@dataclass
class DQReport:
    """A composable result for pre-commit and post-transform checks."""

    findings: list[DQFinding] = field(default_factory=list)
    metrics: dict[str, float | int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.findings

    def add(
        self,
        code: str,
        message: str,
        *,
        count: int = 1,
        examples: Iterable[Any] = (),
    ) -> None:
        self.findings.append(DQFinding(code, message, int(count), tuple(examples)))

    def extend(self, other: "DQReport") -> "DQReport":
        self.findings.extend(other.findings)
        self.metrics.update(other.metrics)
        return self

    def require(self) -> "DQReport":
        if self.findings:
            detail = "; ".join(f"{f.code}: {f.message}" for f in self.findings[:10])
            raise SofaScoreDQViolation(detail)
        return self


@dataclass(frozen=True)
class CaptureExpectation:
    endpoint: str
    target_type: str
    target_id: str
    source_tournament_id: str | int | None = None
    source_season_id: str | int | None = None
    freshness_key: str | None = None


@dataclass(frozen=True)
class DQQuery:
    name: str
    sql: str
    expected_value: float | int
    comparator: str = "eq"


@dataclass(frozen=True)
class ActiveRegistryPartition:
    league: str
    season: str
    source_tournament_id: int
    source_season_id: int


def _coverage_path(path: str | os.PathLike[str] | None = None) -> Path:
    if path is not None:
        return Path(path)
    override = os.environ.get("SOFASCORE_ENDPOINT_COVERAGE_FILE")
    return Path(override) if override else DEFAULT_COVERAGE_FILE


@lru_cache(maxsize=8)
def _load_coverage_cached(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    validate_coverage_contract(doc)
    return doc


def load_coverage_contract(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Load and validate the endpoint matrix.

    A shallow copy protects the cached top-level mapping from accidental key
    replacement.  Callers must treat nested declarations as immutable.
    """

    return dict(_load_coverage_cached(str(_coverage_path(path).resolve())))


def reset_coverage_cache() -> None:
    _load_coverage_cached.cache_clear()


def _require_non_empty_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise SofaScoreContractError(f"{label} must be a non-empty mapping")
    return value


def _require_string_list(
    value: Any, label: str, *, allow_empty: bool = False
) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise SofaScoreContractError(
            f"{label} must be {'a' if allow_empty else 'a non-empty'} list"
        )
    if any(not isinstance(item, str) or not item for item in value):
        raise SofaScoreContractError(f"{label} must contain non-empty strings")
    return value


def validate_coverage_contract(doc: Any) -> None:
    """Validate all endpoint/table declarations, not only the requested one."""

    root = _require_non_empty_mapping(doc, "coverage document")
    if root.get("schema_version") != 1:
        raise SofaScoreContractError("endpoint coverage schema_version must be 1")
    if root.get("source") != "sofascore":
        raise SofaScoreContractError("endpoint coverage source must be sofascore")

    declared_statuses = frozenset(
        _require_string_list(root.get("coverage_statuses"), "coverage_statuses")
    )
    if declared_statuses != COVERAGE_STATUSES:
        raise SofaScoreContractError(
            f"coverage statuses must be exactly {sorted(COVERAGE_STATUSES)}"
        )

    summary = _require_non_empty_mapping(
        root.get("coverage_summary"), "coverage_summary"
    )
    if set(summary) != COVERAGE_STATUSES:
        raise SofaScoreContractError(
            "coverage_summary must contain exactly the four coverage statuses"
        )
    if any(not isinstance(value, int) or value < 0 for value in summary.values()):
        raise SofaScoreContractError(
            "coverage_summary values must be non-negative integers"
        )

    manifest = _require_non_empty_mapping(root.get("manifest"), "manifest")
    states = frozenset(_require_string_list(manifest.get("states"), "manifest.states"))
    if states != MANIFEST_STATES:
        raise SofaScoreContractError(
            f"manifest states must be exactly {sorted(MANIFEST_STATES)}"
        )
    if (
        frozenset(manifest.get("acceptable_terminal_states", {}).get("required", []))
        != REQUIRED_ACCEPTABLE_STATES
    ):
        raise SofaScoreContractError("required terminal states changed")
    if (
        frozenset(manifest.get("acceptable_terminal_states", {}).get("optional", []))
        != OPTIONAL_ACCEPTABLE_STATES
    ):
        raise SofaScoreContractError("optional terminal states changed")
    if (
        frozenset(manifest.get("retryable_http_statuses", []))
        != RETRYABLE_HTTP_STATUSES
    ):
        raise SofaScoreContractError("retryable HTTP status contract changed")

    gates = _require_non_empty_mapping(root.get("quality_gates"), "quality_gates")
    for zero_gate in (
        "duplicate_natural_keys",
        "season_mismatches",
        "skeleton_schedule_rows",
        "partition_key_loss",
        "raw_array_loss",
        "required_field_loss",
    ):
        if gates.get(zero_gate) != 0:
            raise SofaScoreContractError(f"quality_gates.{zero_gate} must be 0")
    for full_gate in ("required_endpoint_completeness",):
        if float(gates.get(full_gate, -1)) != 1.0:
            raise SofaScoreContractError(f"quality_gates.{full_gate} must be 1.0")
    for coverage_gate in (
        "player_profile_coverage",
        "player_rating_coverage",
        "silver_gold_attach_rate",
    ):
        value = gates.get(coverage_gate)
        if not isinstance(value, (int, float)) or not 0.0 < float(value) <= 1.0:
            raise SofaScoreContractError(
                f"quality_gates.{coverage_gate} must be in (0, 1]"
            )

    tables = _require_non_empty_mapping(root.get("tables"), "tables")
    for table_name, table in tables.items():
        if not isinstance(table_name, str) or not _SAFE_SQL_IDENT_RE.fullmatch(
            table_name
        ):
            raise SofaScoreContractError(f"invalid table name: {table_name!r}")
        spec = _require_non_empty_mapping(table, f"tables.{table_name}")
        if not isinstance(spec.get("grain"), str) or not spec["grain"].strip():
            raise SofaScoreContractError(f"tables.{table_name}.grain is required")
        key = _require_string_list(
            spec.get("natural_key"), f"tables.{table_name}.natural_key"
        )
        cols = _require_string_list(
            spec.get("required_columns"), f"tables.{table_name}.required_columns"
        )
        missing_key_columns = sorted(set(key) - set(cols))
        if missing_key_columns:
            raise SofaScoreContractError(
                f"tables.{table_name} natural-key columns missing from required_columns: "
                + ", ".join(missing_key_columns)
            )
        _require_string_list(
            spec.get("partition_columns"),
            f"tables.{table_name}.partition_columns",
            allow_empty=True,
        )
        _require_string_list(
            spec.get("downstream"),
            f"tables.{table_name}.downstream",
            allow_empty=True,
        )
        materialized_by = spec.get("materialized_by")
        if not isinstance(materialized_by, str) or not materialized_by.strip():
            raise SofaScoreContractError(
                f"tables.{table_name}.materialized_by is required"
            )
        materializer_path = materialized_by.split("#", 1)[0]
        if not (_REPO_ROOT / materializer_path).is_file():
            raise SofaScoreContractError(
                f"tables.{table_name}.materialized_by path does not exist: "
                f"{materializer_path}"
            )
        write_status = spec.get("production_write_status")
        if not isinstance(write_status, str) or not write_status.strip():
            raise SofaScoreContractError(
                f"tables.{table_name}.production_write_status is required"
            )

    committed_dq = _require_non_empty_mapping(
        root.get("committed_state_dq"), "committed_state_dq"
    )
    dq_tables = _require_string_list(
        committed_dq.get("duplicate_tables"),
        "committed_state_dq.duplicate_tables",
    )
    if len(dq_tables) != len(set(dq_tables)):
        raise SofaScoreContractError(
            "committed_state_dq.duplicate_tables contains duplicates"
        )
    unknown_dq_tables = set(dq_tables) - set(tables)
    if unknown_dq_tables:
        raise SofaScoreContractError(
            "committed-state DQ references unknown tables: "
            + ", ".join(sorted(unknown_dq_tables))
        )
    later_dq_tables = _require_string_list(
        committed_dq.get("later_stage_duplicate_tables"),
        "committed_state_dq.later_stage_duplicate_tables",
        allow_empty=True,
    )
    if len(later_dq_tables) != len(set(later_dq_tables)):
        raise SofaScoreContractError(
            "committed_state_dq.later_stage_duplicate_tables contains duplicates"
        )
    if set(dq_tables) & set(later_dq_tables):
        raise SofaScoreContractError(
            "committed-state and later-stage duplicate tables overlap"
        )
    unknown_later_tables = set(later_dq_tables) - set(tables)
    if unknown_later_tables:
        raise SofaScoreContractError(
            "later-stage DQ references unknown tables: "
            + ", ".join(sorted(unknown_later_tables))
        )
    required_event_endpoints = _require_string_list(
        committed_dq.get("required_event_endpoints"),
        "committed_state_dq.required_event_endpoints",
    )

    compat = _require_non_empty_mapping(
        root.get("bronze_compatibility_columns"), "bronze_compatibility_columns"
    )
    for table_name, groups in compat.items():
        if not isinstance(groups, list) or not groups:
            raise SofaScoreContractError(
                f"bronze_compatibility_columns.{table_name} must be non-empty"
            )
        for index, group in enumerate(groups):
            _require_string_list(
                group,
                f"bronze_compatibility_columns.{table_name}[{index}]",
            )

    endpoints = _require_non_empty_mapping(root.get("endpoints"), "endpoints")
    normalized_destinations: set[str] = set()
    for endpoint_name, endpoint in endpoints.items():
        if not isinstance(endpoint_name, str) or not endpoint_name:
            raise SofaScoreContractError("endpoint names must be non-empty strings")
        spec = _require_non_empty_mapping(endpoint, f"endpoints.{endpoint_name}")
        for required_field in (
            "path",
            "target_type",
            "status",
            "required",
            "raw_collection",
            "preserved_arrays",
            "destination",
            "grain",
            "natural_key",
            "empty_semantics",
            "dq",
        ):
            if required_field not in spec:
                raise SofaScoreContractError(
                    f"endpoints.{endpoint_name}.{required_field} is required"
                )
        status = spec["status"]
        if status not in COVERAGE_STATUSES:
            raise SofaScoreContractError(
                f"endpoints.{endpoint_name}.status invalid: {status!r}"
            )
        if not isinstance(spec["required"], bool):
            raise SofaScoreContractError(
                f"endpoints.{endpoint_name}.required must be boolean"
            )
        if spec["required"] and status in {"unsupported", "intentionally-excluded"}:
            raise SofaScoreContractError(
                f"required endpoint {endpoint_name} cannot be {status}"
            )
        _require_string_list(
            spec["preserved_arrays"],
            f"endpoints.{endpoint_name}.preserved_arrays",
            allow_empty=True,
        )
        destinations = _require_string_list(
            spec["destination"],
            f"endpoints.{endpoint_name}.destination",
            allow_empty=True,
        )
        _require_string_list(
            spec["natural_key"], f"endpoints.{endpoint_name}.natural_key"
        )
        _require_string_list(
            spec["dq"], f"endpoints.{endpoint_name}.dq", allow_empty=True
        )
        if status == "normalized":
            if not destinations:
                raise SofaScoreContractError(
                    f"normalized endpoint {endpoint_name} needs a destination"
                )
            normalized_destinations.update(
                d for d in destinations if d.startswith(("bronze.", "silver.", "gold."))
            )
        else:
            if not isinstance(spec.get("reason"), str) or not spec["reason"].strip():
                raise SofaScoreContractError(
                    f"non-normalized endpoint {endpoint_name} needs a reason"
                )
            if destinations:
                raise SofaScoreContractError(
                    f"non-normalized endpoint {endpoint_name} cannot claim a destination"
                )
            if (
                status == "raw-only"
                and not spec["preserved_arrays"]
                and not spec.get("required_json_paths")
            ):
                raise SofaScoreContractError(
                    f"raw-only endpoint {endpoint_name} must declare preserved "
                    "arrays or required JSON paths"
                )
            if (
                status in {"unsupported", "intentionally-excluded"}
                and spec["preserved_arrays"]
            ):
                raise SofaScoreContractError(
                    f"uncaptured endpoint {endpoint_name} cannot claim preserved arrays"
                )

    unknown_destinations = normalized_destinations - set(tables)
    if unknown_destinations:
        raise SofaScoreContractError(
            "normalized destinations have no table contract: "
            + ", ".join(sorted(unknown_destinations))
        )

    invalid_required_endpoints = [
        name
        for name in required_event_endpoints
        if name not in endpoints
        or endpoints[name].get("status") != "normalized"
        or endpoints[name].get("target_type") != "event"
        or not endpoints[name].get("required")
    ]
    if invalid_required_endpoints:
        raise SofaScoreContractError(
            "committed-state DQ event endpoints are not required normalized "
            "event endpoints: " + ", ".join(invalid_required_endpoints)
        )

    observed_summary = {
        status: sum(spec["status"] == status for spec in endpoints.values())
        for status in COVERAGE_STATUSES
    }
    if dict(summary) != observed_summary:
        raise SofaScoreContractError(
            f"coverage_summary drift: declared={dict(summary)}, "
            f"observed={observed_summary}"
        )


def manifest_state_for_response(
    http_status: int | None,
    *,
    parsed: bool,
    row_count: int | None,
    supported: bool = True,
) -> str:
    """Map an endpoint outcome to the only legal manifest state.

    In particular, 403/429/5xx can never become success or legitimate_empty.
    A 404/410 becomes ``not_supported`` only when the caller has positively
    classified the endpoint as optional for this competition format.
    """

    if http_status in RETRYABLE_HTTP_STATUSES or http_status is None:
        return "retryable_failure"
    if http_status in {404, 410}:
        return "not_supported" if not supported else "retryable_failure"
    if not 200 <= int(http_status) < 300:
        return "retryable_failure"
    if not parsed or row_count is None or row_count < 0:
        return "schema_error"
    return "success" if row_count > 0 else "legitimate_empty"


def _key_tuple(row: Mapping[str, Any], fields: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in fields)


def validate_table_rows(
    table_name: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any] | None = None,
) -> DQReport:
    """Check required columns, non-null natural keys and duplicate keys."""

    doc = contract or load_coverage_contract()
    try:
        table = doc["tables"][table_name]
    except KeyError as exc:
        raise SofaScoreContractError(
            f"unknown normalized table {table_name!r}"
        ) from exc
    materialized = list(rows)
    required_columns = list(table["required_columns"])
    key_fields = list(table["natural_key"])
    report = DQReport()

    missing_rows: list[tuple[int, tuple[str, ...]]] = []
    null_keys: list[tuple[int, tuple[Any, ...]]] = []
    seen: dict[tuple[Any, ...], int] = {}
    duplicates: list[tuple[Any, ...]] = []
    invalid_enums: list[tuple[int, str, Any]] = []
    allowed_values = {
        column: set(values)
        for column, values in (table.get("allowed_values") or {}).items()
    }
    for index, row in enumerate(materialized):
        missing = tuple(col for col in required_columns if col not in row)
        if missing:
            missing_rows.append((index, missing))
        key = _key_tuple(row, key_fields)
        if any(value is None or value == "" for value in key):
            null_keys.append((index, key))
        elif key in seen:
            duplicates.append(key)
        else:
            seen[key] = index
        for column, allowed in allowed_values.items():
            value = row.get(column)
            if value is not None and value not in allowed:
                invalid_enums.append((index, column, value))

    if missing_rows:
        report.add(
            "required_field_loss",
            f"{len(missing_rows)} {table_name} rows omit required columns",
            count=len(missing_rows),
            examples=missing_rows[:5],
        )
    if null_keys:
        report.add(
            "null_natural_key",
            f"{len(null_keys)} {table_name} rows have an empty natural key",
            count=len(null_keys),
            examples=null_keys[:5],
        )
    if duplicates:
        unique_duplicates = list(dict.fromkeys(duplicates))
        report.add(
            "duplicate_natural_key",
            f"{len(unique_duplicates)} duplicate {table_name} natural keys",
            count=len(unique_duplicates),
            examples=unique_duplicates[:5],
        )
    if invalid_enums:
        report.add(
            "invalid_enum_value",
            f"{len(invalid_enums)} {table_name} rows violate an enum contract",
            count=len(invalid_enums),
            examples=invalid_enums[:5],
        )
    report.metrics[f"{table_name}.rows"] = len(materialized)
    report.metrics[f"{table_name}.duplicate_keys"] = len(set(duplicates))
    return report


def _first_non_empty(row: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            value = value.get("id") or value.get("name")
        if value is not None and value != "":
            return value
    return None


def validate_schedule_rows(rows: Iterable[Mapping[str, Any]]) -> DQReport:
    """Reject issue-#900 skeletons while permitting unplayed scoreless games."""

    materialized = list(rows)
    report = DQReport()
    skeletons: list[tuple[int, str]] = []
    for index, row in enumerate(materialized):
        event_id = _first_non_empty(row, ("game_id", "event_id", "id"))
        home = _first_non_empty(row, ("home_team_id", "home_team", "home_team_name"))
        away = _first_non_empty(row, ("away_team_id", "away_team", "away_team_name"))
        kickoff = _first_non_empty(
            row, ("start_timestamp", "start_time", "date", "match_date")
        )
        if event_id is None:
            skeletons.append((index, "missing event id"))
        elif home is None or away is None:
            skeletons.append((index, "missing home or away team"))
        elif str(home) == str(away):
            skeletons.append((index, "home and away team are identical"))
        elif kickoff is None:
            skeletons.append((index, "missing kickoff"))
    if skeletons:
        report.add(
            "skeleton_schedule_row",
            f"{len(skeletons)} schedule rows are analytically empty or invalid",
            count=len(skeletons),
            examples=skeletons[:5],
        )
    report.metrics["schedule.rows"] = len(materialized)
    report.metrics["schedule.skeleton_rows"] = len(skeletons)
    return report


def validate_lineup_semantics(rows: Iterable[Mapping[str, Any]]) -> DQReport:
    """Prove starters, bench and unused substitutes are explicit and coherent."""

    materialized = list(rows)
    invalid: list[tuple[int, str]] = []
    for index, row in enumerate(materialized):
        starter = row.get("is_starter")
        bench = row.get("is_bench")
        unused = row.get("is_unused_substitute")
        status = row.get("participation_status")
        if not isinstance(starter, bool) or not isinstance(bench, bool):
            invalid.append((index, "starter/bench flags must be boolean"))
            continue
        if starter == bench:
            invalid.append((index, "exactly one of starter/bench must be true"))
        if unused is not False and unused is not True:
            invalid.append((index, "unused-substitute flag must be boolean"))
        elif unused and (not bench or starter):
            invalid.append((index, "unused substitute must be on the bench"))
        expected_status = (
            "starter"
            if starter
            else ("unused_substitute" if unused else "substitute_used")
        )
        if status != expected_status:
            invalid.append(
                (
                    index,
                    f"participation_status={status!r}, expected {expected_status!r}",
                )
            )
    report = DQReport()
    if invalid:
        report.add(
            "lineup_semantics",
            f"{len(invalid)} lineup rows have contradictory participation semantics",
            count=len(invalid),
            examples=invalid[:10],
        )
    report.metrics["lineup.rows"] = len(materialized)
    report.metrics["lineup.unused_substitutes"] = sum(
        row.get("is_unused_substitute") is True for row in materialized
    )
    return report


def validate_event_participants(rows: Iterable[Mapping[str, Any]]) -> DQReport:
    """Require exactly one home and one away team for every full event."""

    by_event: dict[Any, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_event.setdefault(row.get("match_id") or row.get("event_id"), []).append(row)
    invalid: list[tuple[Any, tuple[Any, ...]]] = []
    for event_id, event_rows in by_event.items():
        sides = tuple(
            sorted(row.get("team_side") for row in event_rows if row.get("team_side"))
        )
        team_ids = {
            row.get("team_id") for row in event_rows if row.get("team_id") is not None
        }
        if event_id is None or sides != ("away", "home") or len(team_ids) != 2:
            invalid.append((event_id, sides))
    report = DQReport()
    if invalid:
        report.add(
            "event_participant_shape",
            f"{len(invalid)} events do not have exactly one distinct home and away team",
            count=len(invalid),
            examples=invalid[:10],
        )
    report.metrics["event_participants.events"] = len(by_event)
    return report


def validate_season_alignment(
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_source_season_id: str | int,
    expected_canonical_season: str,
) -> DQReport:
    """Require both source and canonical season identities to match the run."""

    expected_source = str(expected_source_season_id)
    expected_canonical = str(expected_canonical_season)
    mismatches: list[tuple[int, Any, Any]] = []
    materialized = list(rows)
    for index, row in enumerate(materialized):
        # Do not let lineage metadata mask a conflicting season embedded in
        # the payload itself.  Every populated source/canonical identity on a
        # row must agree with the requested registry season.
        source_values = {
            str(row.get(field))
            for field in (
                "season_id",
                "sofascore_season_id",
                "source_season_id",
            )
            if row.get(field) not in (None, "")
        }
        canonical_values = {
            str(row.get(field))
            for field in ("canonical_season", "season")
            if row.get(field) not in (None, "")
        }
        if source_values != {expected_source} or canonical_values != {
            expected_canonical
        }:
            mismatches.append(
                (index, tuple(sorted(source_values)), tuple(sorted(canonical_values)))
            )
    report = DQReport()
    if mismatches:
        report.add(
            "season_mismatch",
            f"{len(mismatches)} rows do not match source season "
            f"{expected_source!r} / canonical season {expected_canonical!r}",
            count=len(mismatches),
            examples=mismatches[:5],
        )
    report.metrics["season.rows"] = len(materialized)
    report.metrics["season.mismatches"] = len(mismatches)
    return report


def validate_partition_replacement(
    existing_keys: Iterable[Sequence[Any]],
    candidate_keys: Iterable[Sequence[Any]],
    *,
    allow_removed_keys: Iterable[Sequence[Any]] = (),
) -> DQReport:
    """Pre-commit partition-shrink gate.

    This validates the complete candidate snapshot, not an incremental MERGE
    batch.  A caller doing incremental upsert must union existing+staged keys
    first.  Explicit removals are allowed only when the operator passes their
    exact natural keys (for example a source-deleted duplicate).
    """

    old = {tuple(key) for key in existing_keys}
    new = {tuple(key) for key in candidate_keys}
    allowed = {tuple(key) for key in allow_removed_keys}
    lost = sorted(old - new - allowed, key=repr)
    report = DQReport()
    if lost:
        report.add(
            "partition_shrink",
            f"candidate partition loses {len(lost)} previously published keys",
            count=len(lost),
            examples=lost[:10],
        )
    report.metrics["partition.existing_keys"] = len(old)
    report.metrics["partition.candidate_keys"] = len(new)
    report.metrics["partition.lost_keys"] = len(lost)
    return report


def _observation_sort_key(row: Mapping[str, Any]) -> tuple[str, int]:
    timestamp = str(
        row.get("finished_at") or row.get("updated_at") or row.get("started_at") or ""
    )
    try:
        attempt = int(row.get("attempt") or row.get("attempt_no") or 0)
    except (TypeError, ValueError):
        attempt = 0
    return timestamp, attempt


def validate_manifest_completeness(
    expectations: Iterable[CaptureExpectation],
    observations: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any] | None = None,
) -> DQReport:
    """Require an acceptable latest state for every expected endpoint target."""

    doc = contract or load_coverage_contract()
    endpoints = doc["endpoints"]
    observed = list(observations)
    failures: list[tuple[str, str, str | None]] = []
    total = 0
    complete = 0

    for expected in expectations:
        total += 1
        spec = endpoints.get(expected.endpoint)
        if spec is None:
            raise SofaScoreContractError(
                f"manifest expectation references unknown endpoint {expected.endpoint!r}"
            )
        candidates = []
        for row in observed:
            if row.get("endpoint") != expected.endpoint:
                continue
            if row.get("target_type") != expected.target_type:
                continue
            if str(row.get("target_id")) != str(expected.target_id):
                continue
            if expected.source_tournament_id is not None and str(
                row.get("source_tournament_id")
            ) != str(expected.source_tournament_id):
                continue
            if expected.source_season_id is not None and str(
                row.get("source_season_id")
            ) != str(expected.source_season_id):
                continue
            if expected.freshness_key is not None and str(
                row.get("freshness_key")
            ) != str(expected.freshness_key):
                continue
            candidates.append(row)
        latest = max(candidates, key=_observation_sort_key) if candidates else None
        state = latest.get("state") if latest else None
        accepted = (
            REQUIRED_ACCEPTABLE_STATES
            if spec["required"]
            else OPTIONAL_ACCEPTABLE_STATES
        )
        if state in accepted:
            complete += 1
        else:
            failures.append((expected.endpoint, str(expected.target_id), state))

    report = DQReport()
    ratio = 1.0 if total == 0 else complete / total
    if failures:
        report.add(
            "endpoint_incomplete",
            f"{len(failures)}/{total} expected endpoint targets lack an acceptable terminal state",
            count=len(failures),
            examples=failures[:10],
        )
    report.metrics["manifest.expected"] = total
    report.metrics["manifest.complete"] = complete
    report.metrics["manifest.completeness"] = ratio
    return report


def validate_minimum_coverage(
    label: str,
    covered_ids: Iterable[Any],
    universe_ids: Iterable[Any],
    *,
    threshold: float = 0.95,
) -> DQReport:
    if not 0.0 < threshold <= 1.0:
        raise ValueError("threshold must be in (0, 1]")
    universe = {value for value in universe_ids if value is not None}
    covered = {value for value in covered_ids if value is not None} & universe
    # An empty denominator is missing evidence, not perfect coverage.  The
    # only defensible floor is semantic: at least one expected entity must
    # exist.  Competition-size guesses would make small valid tournaments fail.
    ratio = 0.0 if not universe else len(covered) / len(universe)
    missing = sorted(universe - covered, key=repr)
    report = DQReport(
        metrics={
            f"{label}.coverage": ratio,
            f"{label}.expected": len(universe),
            f"{label}.covered": len(covered),
        }
    )
    if not universe:
        report.add(
            f"{label}_coverage",
            f"{label} expected universe is empty; coverage is unproven",
        )
        return report
    if ratio < threshold:
        report.add(
            f"{label}_coverage",
            f"{label} coverage {ratio:.2%} ({len(covered)}/{len(universe)}) "
            f"is below {threshold:.2%}",
            count=len(missing),
            examples=missing[:10],
        )
    return report


def validate_player_coverage(
    *,
    squad_player_ids: Iterable[Any],
    lineup_player_ids: Iterable[Any],
    statistics_player_ids: Iterable[Any],
    incident_player_ids: Iterable[Any],
    profile_player_ids: Iterable[Any],
    rated_player_event_ids: Iterable[Any],
    appeared_player_event_ids: Iterable[Any],
    silver_player_event_ids: Iterable[Any],
    gold_player_event_ids: Iterable[Any],
    threshold: float = 0.95,
) -> DQReport:
    """Apply the 95% gates to the complete player universe (#724/#869)."""

    universe = (
        set(squad_player_ids)
        | set(lineup_player_ids)
        | set(statistics_player_ids)
        | set(incident_player_ids)
    )
    report = DQReport()
    report.extend(
        validate_minimum_coverage(
            "player_profile", profile_player_ids, universe, threshold=threshold
        )
    )
    report.extend(
        validate_minimum_coverage(
            "player_rating",
            rated_player_event_ids,
            appeared_player_event_ids,
            threshold=threshold,
        )
    )
    report.extend(
        validate_minimum_coverage(
            "silver_gold_attach",
            gold_player_event_ids,
            silver_player_event_ids,
            threshold=threshold,
        )
    )
    report.metrics["player_universe.size"] = len(
        {value for value in universe if value is not None}
    )
    return report


def _decode_json_pointer_segment(segment: str) -> str:
    return segment.replace("~1", "/").replace("~0", "~")


def resolve_json_pointer(payload: Any, pointer: str) -> list[Any]:
    """Resolve RFC-6901-like pointers with ``*`` fan-out for arrays/maps."""

    if pointer == "":
        return [payload]
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise SofaScoreContractError(f"invalid JSON pointer {pointer!r}")
    values = [payload]
    for raw_segment in pointer[1:].split("/"):
        segment = _decode_json_pointer_segment(raw_segment)
        next_values: list[Any] = []
        for value in values:
            if segment == "*":
                if isinstance(value, Mapping):
                    next_values.extend(value.values())
                elif isinstance(value, list):
                    next_values.extend(value)
            elif isinstance(value, Mapping) and segment in value:
                next_values.append(value[segment])
            elif isinstance(value, list) and segment.isdigit():
                index = int(segment)
                if 0 <= index < len(value):
                    next_values.append(value[index])
        values = next_values
        if not values:
            break
    return values


def _pointer_is_vacuous_below_empty_collection(payload: Any, pointer: str) -> bool:
    """True when a wildcard path has no children because its parent is empty."""

    segments = pointer[1:].split("/") if pointer.startswith("/") else []
    for index, segment in enumerate(segments):
        if segment != "*":
            continue
        parent = "/" + "/".join(segments[:index])
        parents = resolve_json_pointer(payload, parent)
        if parents and all(
            isinstance(value, (list, Mapping)) and len(value) == 0 for value in parents
        ):
            return True
    return False


def validate_raw_payload(
    endpoint_name: str,
    payload: Any,
    *,
    normalized_row_count: int | None = None,
    contract: Mapping[str, Any] | None = None,
) -> DQReport:
    """Check required fields and prove declared arrays survived raw capture."""

    doc = contract or load_coverage_contract()
    try:
        endpoint = doc["endpoints"][endpoint_name]
    except KeyError as exc:
        raise SofaScoreContractError(f"unknown endpoint {endpoint_name!r}") from exc
    report = DQReport()
    missing_paths: list[str] = []
    for pointer in endpoint.get("required_json_paths", []):
        if not resolve_json_pointer(payload, pointer):
            missing_paths.append(pointer)
    if missing_paths:
        report.add(
            "schema_drift",
            f"{endpoint_name} is missing {len(missing_paths)} required JSON paths",
            count=len(missing_paths),
            examples=missing_paths,
        )

    lost_arrays: list[str] = []
    array_lengths: dict[str, int] = {}
    for pointer in endpoint.get("preserved_arrays", []):
        values = resolve_json_pointer(payload, pointer)
        if not values and _pointer_is_vacuous_below_empty_collection(payload, pointer):
            array_lengths[pointer] = 0
        elif not values or any(not isinstance(value, list) for value in values):
            lost_arrays.append(pointer)
        else:
            array_lengths[pointer] = sum(len(value) for value in values)
    if lost_arrays:
        report.add(
            "raw_array_loss",
            f"{endpoint_name} lost or changed {len(lost_arrays)} declared arrays",
            count=len(lost_arrays),
            examples=lost_arrays,
        )

    cardinality_paths = endpoint.get("normalized_cardinality_paths", [])
    if normalized_row_count is not None and cardinality_paths:
        raw_count = 0
        for pointer in cardinality_paths:
            values = resolve_json_pointer(payload, pointer)
            if not values and _pointer_is_vacuous_below_empty_collection(
                payload, pointer
            ):
                continue
            if not values or any(not isinstance(value, list) for value in values):
                report.add(
                    "raw_cardinality_unavailable",
                    f"cannot derive normalized cardinality from {pointer}",
                    examples=(pointer,),
                )
                continue
            raw_count += sum(len(value) for value in values)
        report.metrics[f"{endpoint_name}.raw_cardinality"] = raw_count
        report.metrics[f"{endpoint_name}.normalized_cardinality"] = normalized_row_count
        if raw_count != normalized_row_count:
            report.add(
                "normalized_cardinality_drift",
                f"{endpoint_name} normalized {normalized_row_count} rows from "
                f"{raw_count} raw array elements",
                count=abs(raw_count - normalized_row_count),
            )

    report.metrics[f"{endpoint_name}.declared_arrays"] = len(
        endpoint.get("preserved_arrays", [])
    )
    return report


def schema_fingerprint(payload: Any) -> dict[str, str]:
    """Return a deterministic JSON-path/type fingerprint (array values use *)."""

    result: dict[str, str] = {}

    def walk(value: Any, pointer: str) -> None:
        if value is None:
            kind = "null"
        elif isinstance(value, bool):
            kind = "boolean"
        elif isinstance(value, int):
            kind = "integer"
        elif isinstance(value, float):
            kind = "number"
        elif isinstance(value, str):
            kind = "string"
        elif isinstance(value, list):
            kind = "array"
        elif isinstance(value, Mapping):
            kind = "object"
        else:
            kind = type(value).__name__
        result[pointer or "/"] = kind
        if isinstance(value, Mapping):
            for key in sorted(value, key=str):
                escaped = str(key).replace("~", "~0").replace("/", "~1")
                walk(value[key], f"{pointer}/{escaped}")
        elif isinstance(value, list):
            for item in value:
                walk(item, f"{pointer}/*")

    walk(payload, "")
    return dict(sorted(result.items()))


def compare_schema_fingerprints(
    baseline: Mapping[str, str],
    observed: Mapping[str, str],
) -> DQReport:
    """Fail on removed fields/type changes; tolerate additive source fields."""

    missing = sorted(set(baseline) - set(observed))
    changed = sorted(
        path
        for path in set(baseline) & set(observed)
        if baseline[path] != observed[path]
        and "null" not in {baseline[path], observed[path]}
    )
    report = DQReport()
    if missing:
        report.add(
            "schema_field_removed",
            f"{len(missing)} baseline JSON paths disappeared",
            count=len(missing),
            examples=missing[:10],
        )
    if changed:
        report.add(
            "schema_type_changed",
            f"{len(changed)} baseline JSON paths changed type",
            count=len(changed),
            examples=[(p, baseline[p], observed[p]) for p in changed[:10]],
        )
    report.metrics["schema.baseline_paths"] = len(baseline)
    report.metrics["schema.observed_paths"] = len(observed)
    return report


def raw_payload_sha256(payload_bytes: bytes) -> str:
    if not isinstance(payload_bytes, bytes):
        raise TypeError("raw payload must be bytes")
    return hashlib.sha256(payload_bytes).hexdigest()


def validate_offline_replay(raw_bytes: bytes, replay_payload: Any) -> DQReport:
    """Prove replay parsed the exact stored JSON bytes, without a network fetch."""

    report = DQReport()
    try:
        original = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        report.add("raw_json_invalid", f"stored payload is not valid JSON: {exc}")
        return report
    if original != replay_payload:
        report.add(
            "replay_payload_mismatch",
            "offline replay object differs from the exact stored JSON payload",
        )
    report.metrics["raw.bytes"] = len(raw_bytes)
    return report


def validate_bronze_compatibility_columns(
    observed_columns: Mapping[str, Iterable[str]],
    *,
    contract: Mapping[str, Any] | None = None,
) -> DQReport:
    """Prevent fresh-bootstrap failures in legacy/new Silver COALESCE bridges."""

    doc = contract or load_coverage_contract()
    report = DQReport()
    missing: list[tuple[str, str]] = []
    for table, groups in doc["bronze_compatibility_columns"].items():
        columns = set(observed_columns.get(table, ()))
        for group in groups:
            for column in group:
                if column not in columns:
                    missing.append((table, column))
    if missing:
        report.add(
            "bronze_compatibility_column_missing",
            f"fresh Bronze schema omits {len(missing)} legacy/source-key compatibility columns",
            count=len(missing),
            examples=missing[:10],
        )
    return report


def active_registry_partitions(
    registry_path: str | os.PathLike[str] | None = None,
) -> tuple[ActiveRegistryPartition, ...]:
    """Return the scheduled production season for each active tournament.

    Discovery may add historical seasons to an already-enabled tournament.
    Those metadata rows are not implicitly activated for daily capture/DQ;
    otherwise a registry refresh would make E3 demand an unrequested backfill.
    Historical partitions are validated by the explicit backfill workflow.
    """

    from scrapers.sofascore.catalog import SofaScoreCatalog
    from utils.medallion_config import (
        get_active_season,
        is_single_year_competition,
    )

    catalog = SofaScoreCatalog.load(registry_path)
    partitions = []
    for tournament in catalog.tournaments:
        if not tournament.capture_allowed or tournament.canonical_id is None:
            continue
        scheduled = get_active_season(tournament.canonical_id)
        if scheduled is None:
            continue
        scheduled = int(scheduled)
        canonical = (
            str(scheduled)
            if is_single_year_competition(tournament.canonical_id)
            else f"{scheduled % 100:02d}{(scheduled + 1) % 100:02d}"
        )
        season = catalog.resolve_source_season(
            tournament.unique_tournament_id,
            canonical,
        )
        if season is None or not season.activatable or season.canonical_season is None:
            raise SofaScoreContractError(
                f"active {tournament.canonical_id} season {canonical!r} "
                "has no activatable SofaScore metadata"
            )
        partitions.append(
            ActiveRegistryPartition(
                league=tournament.canonical_id,
                season=str(season.canonical_season),
                source_tournament_id=tournament.unique_tournament_id,
                source_season_id=season.season_id,
            )
        )
    return tuple(
        sorted(
            partitions,
            key=lambda item: (
                item.league,
                item.season,
                item.source_tournament_id,
                item.source_season_id,
            ),
        )
    )


def _sql_literal(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("SQL scope value must be a non-empty string")
    if any(token in value for token in (";", "--", "/*", "*/", "\x00", "\n", "\r")):
        raise ValueError(f"unsafe SQL scope value: {value!r}")
    return "'" + value.replace("'", "''") + "'"


def _numeric_source_id_mismatch(column: str, expected: str | int) -> str:
    """Render a type-stable source-ID mismatch predicate.

    Historical Bronze tables inferred SofaScore IDs as ``DOUBLE``. Casting
    those values directly to varchar produces scientific notation in Trino
    (for example ``76986`` becomes ``7.6986E4``), which made every otherwise
    correct season fail DQ. Numeric comparison accepts bigint/varchar/double
    storage while still treating null, non-numeric and fractional IDs as a
    mismatch.
    """

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?", column
    ):
        raise ValueError(f"unsafe source-ID column: {column!r}")
    if isinstance(expected, bool):
        raise ValueError("expected source ID must be a positive integer")
    try:
        expected_id = int(expected)
    except (TypeError, ValueError) as exc:
        raise ValueError("expected source ID must be a positive integer") from exc
    if expected_id <= 0 or str(expected).strip() != str(expected_id):
        raise ValueError("expected source ID must be a positive integer")
    numeric = f"TRY_CAST({column} AS double)"
    return f"{numeric} IS NULL OR {numeric} <> CAST({expected_id} AS double)"


def build_partition_dq_queries(
    league: str,
    season: str,
    source_tournament_id: str | int,
    source_season_id: str | int,
    *,
    contract: Mapping[str, Any] | None = None,
) -> list[DQQuery]:
    """Build Trino-ready gates for the production post-normalization callback.

    The engine executes each scalar query and compares the first value with the
    declared expectation.  The pure validators above remain the pre-write and
    replay path; these SQL checks protect the committed Iceberg state.
    """

    doc = contract or load_coverage_contract()
    league_sql = _sql_literal(league)
    season_sql = _sql_literal(str(season))
    source_tournament_sql = _sql_literal(str(source_tournament_id))
    source_season_sql = _sql_literal(str(source_season_id))
    schedule_season_mismatch = _numeric_source_id_mismatch(
        "season_id", source_season_id
    )
    event_lineage_season_mismatch = _numeric_source_id_mismatch(
        "source_season_id", source_season_id
    )
    event_payload_season_mismatch = _numeric_source_id_mismatch(
        "season_id", source_season_id
    )
    scope = f"league = {league_sql} AND season = {season_sql}"
    ss_scope = f"ss.league = {league_sql} AND ss.season = {season_sql}"
    schedule_scope = f"s.league = {league_sql} AND s.season = {season_sql}"
    queries: list[DQQuery] = []

    manifest_key_sql = ", ".join(
        f'"{column}"' for column in doc["manifest"]["natural_key"]
    )
    queries.append(
        DQQuery(
            name="duplicate_natural_key[ops.sofascore_capture_manifest]",
            sql=(
                "SELECT COUNT(*) FROM ("
                f"SELECT {manifest_key_sql}, COUNT(*) AS n "
                f"FROM {doc['manifest']['table']} "
                f"GROUP BY {manifest_key_sql} HAVING COUNT(*) > 1)"
            ),
            expected_value=0,
        )
    )

    for table_name in doc["committed_state_dq"]["duplicate_tables"]:
        spec = doc["tables"][table_name]
        if spec.get("partition_columns") != ["league", "season"]:
            raise SofaScoreContractError(
                "committed-state duplicate table is not league/season "
                f"partitioned: {table_name}"
            )
        key_sql = ", ".join(f'"{column}"' for column in spec["natural_key"])
        queries.append(
            DQQuery(
                name=f"duplicate_natural_key[{table_name}]",
                sql=(
                    "SELECT COUNT(*) FROM ("
                    f"SELECT {key_sql}, COUNT(*) AS n FROM iceberg.{table_name} "
                    f"WHERE {scope} GROUP BY {key_sql} HAVING COUNT(*) > 1)"
                ),
                expected_value=0,
            )
        )
        null_predicate = " OR ".join(
            f'"{column}" IS NULL OR trim(CAST("{column}" AS varchar)) = \'\''
            for column in spec["natural_key"]
        )
        queries.append(
            DQQuery(
                name=f"null_natural_key[{table_name}]",
                sql=(
                    f"SELECT COUNT(*) FROM iceberg.{table_name} "
                    f"WHERE {scope} AND ({null_predicate})"
                ),
                expected_value=0,
            )
        )

    queries.extend(
        [
            DQQuery(
                name="skeleton_schedule_rows",
                sql=(
                    "SELECT COUNT(*) FROM iceberg.bronze.sofascore_schedule "
                    f"WHERE {scope} AND (game_id IS NULL OR "
                    "COALESCE(NULLIF(TRIM(CAST(home_team_name AS varchar)), ''), "
                    "NULLIF(TRIM(CAST(home_team AS varchar)), '')) IS NULL OR "
                    "COALESCE(NULLIF(TRIM(CAST(away_team_name AS varchar)), ''), "
                    "NULLIF(TRIM(CAST(away_team AS varchar)), '')) IS NULL OR "
                    "(start_timestamp IS NULL AND date IS NULL) OR "
                    "COALESCE(NULLIF(TRIM(CAST(home_team_name AS varchar)), ''), "
                    "NULLIF(TRIM(CAST(home_team AS varchar)), '')) = "
                    "COALESCE(NULLIF(TRIM(CAST(away_team_name AS varchar)), ''), "
                    "NULLIF(TRIM(CAST(away_team AS varchar)), '')))"
                ),
                expected_value=0,
            ),
            DQQuery(
                name="schedule_season_mismatches",
                sql=(
                    "SELECT COUNT(*) FROM iceberg.bronze.sofascore_schedule "
                    f"WHERE {scope} AND ({schedule_season_mismatch})"
                ),
                expected_value=0,
            ),
            DQQuery(
                name="event_season_mismatches",
                sql=(
                    "SELECT COUNT(*) FROM iceberg.bronze.sofascore_events "
                    f"WHERE {scope} AND (({event_lineage_season_mismatch}) OR "
                    f"({event_payload_season_mismatch}))"
                ),
                expected_value=0,
            ),
            *[
                DQQuery(
                    name=f"{check_name}_season_mismatches",
                    sql=(
                        f"SELECT COUNT(*) FROM iceberg.bronze.{table_name} c "
                        "LEFT JOIN iceberg.bronze.sofascore_schedule s ON "
                        "CAST(s.game_id AS varchar) = CAST(c.match_id AS varchar) "
                        f"WHERE c.league = {league_sql} AND "
                        f"c.season = {season_sql} AND (s.game_id IS NULL OR "
                        "CAST(s.season AS varchar) IS DISTINCT FROM "
                        "CAST(c.season AS varchar) OR "
                        "s.league IS DISTINCT FROM c.league)"
                    ),
                    expected_value=0,
                )
                for check_name, table_name in (
                    ("player_ratings", "sofascore_player_ratings"),
                    ("event_player_stats", "sofascore_event_player_stats"),
                    ("match_stats", "sofascore_match_stats"),
                    ("event_shotmap", "sofascore_event_shotmap"),
                )
            ],
            DQQuery(
                name="player_profile_expected_universe_nonempty",
                sql=(
                    "SELECT COUNT(DISTINCT player_id) FROM "
                    "iceberg.bronze.sofascore_player_universe "
                    f"WHERE {scope} AND player_id IS NOT NULL"
                ),
                expected_value=1,
                comparator="gte",
            ),
            DQQuery(
                name="player_profile_coverage",
                sql=(
                    "WITH universe AS ("
                    "SELECT player_id FROM "
                    "iceberg.bronze.sofascore_player_universe "
                    f"WHERE {scope} AND player_id IS NOT NULL), "
                    "profiles AS (SELECT DISTINCT player_id FROM "
                    f"iceberg.silver.sofascore_player_profile WHERE {scope}) "
                    "SELECT CASE WHEN COUNT(*) = 0 THEN 0e0 ELSE "
                    "CAST(COUNT_IF(p.player_id IS NOT NULL) AS double) / COUNT(*) END "
                    "FROM universe u LEFT JOIN profiles p ON p.player_id = u.player_id"
                ),
                expected_value=float(doc["quality_gates"]["player_profile_coverage"]),
                comparator="gte",
            ),
            DQQuery(
                name="player_rating_expected_appearances_nonempty",
                sql=(
                    "SELECT COUNT(*) FROM "
                    "iceberg.silver.sofascore_player_match_aggregate "
                    f"WHERE {scope} AND minutes_played > 0"
                ),
                expected_value=1,
                comparator="gte",
            ),
            DQQuery(
                name="player_rating_coverage",
                sql=(
                    "SELECT CASE WHEN COUNT(*) = 0 THEN 0e0 ELSE "
                    "CAST(COUNT_IF(rating IS NOT NULL AND rating > 0) AS double) / "
                    "COUNT(*) END FROM iceberg.silver.sofascore_player_match_aggregate "
                    f"WHERE {scope} AND minutes_played > 0"
                ),
                expected_value=float(doc["quality_gates"]["player_rating_coverage"]),
                comparator="gte",
            ),
            DQQuery(
                name="silver_gold_expected_candidates_nonempty",
                sql=(
                    "SELECT COUNT(*) FROM "
                    "iceberg.silver.sofascore_player_match_aggregate ss "
                    f"WHERE {ss_scope}"
                ),
                expected_value=1,
                comparator="gte",
            ),
            DQQuery(
                name="silver_gold_attach_rate",
                sql=(
                    "WITH candidates AS (SELECT ss.match_id AS source_match_id, "
                    "ss.player_id AS source_player_id, xm.canonical_id AS match_id, "
                    "xp.canonical_id AS player_id FROM "
                    "iceberg.silver.sofascore_player_match_aggregate ss "
                    "LEFT JOIN iceberg.silver.xref_match xm ON xm.source = 'sofascore' "
                    "AND xm.source_id = CAST(ss.match_id AS varchar) "
                    "AND xm.league = ss.league AND xm.season = ss.season "
                    "LEFT JOIN iceberg.silver.xref_player xp ON xp.source = 'sofascore' "
                    "AND xp.source_id = CAST(ss.player_id AS varchar) "
                    "AND xp.league = ss.league AND xp.season = ss.season "
                    f"WHERE {ss_scope}), attached AS (SELECT c.*, "
                    "g.match_id AS gold_match_id FROM candidates c LEFT JOIN "
                    "iceberg.gold.fct_lineup g ON g.match_id = c.match_id "
                    "AND g.player_id = c.player_id) SELECT CASE WHEN COUNT(*) = 0 "
                    "THEN 0e0 ELSE CAST(COUNT_IF(gold_match_id IS NOT NULL) AS double) "
                    "/ COUNT(*) END FROM attached"
                ),
                expected_value=float(doc["quality_gates"]["silver_gold_attach_rate"]),
                comparator="gte",
            ),
        ]
    )
    required_endpoints = doc["committed_state_dq"]["required_event_endpoints"]
    endpoint_values = ", ".join(
        f"({_sql_literal(endpoint)})" for endpoint in required_endpoints
    )
    endpoint_names = ", ".join(
        _sql_literal(endpoint) for endpoint in required_endpoints
    )
    queries.append(
        DQQuery(
            name="required_endpoint_completeness",
            sql=(
                "WITH required_endpoints(endpoint) AS (VALUES "
                f"{endpoint_values}), expected AS (SELECT DISTINCT "
                "CAST(s.game_id AS varchar) AS target_id, e.endpoint FROM "
                "iceberg.bronze.sofascore_schedule s CROSS JOIN "
                f"required_endpoints e WHERE {schedule_scope} AND "
                "s.status_type = 'finished'), ranked_manifest AS (SELECT "
                "target_id, endpoint, status, ROW_NUMBER() OVER (PARTITION BY "
                "target_id, endpoint ORDER BY updated_at DESC, attempts DESC) AS rn "
                "FROM iceberg.ops.sofascore_capture_manifest WHERE "
                f"source_tournament_id = {source_tournament_sql} AND "
                f"source_season_id = {source_season_sql} AND "
                "target_type = 'event' AND freshness_key = 'final' AND "
                f"endpoint IN ({endpoint_names})), latest_manifest AS (SELECT "
                "target_id, endpoint, status FROM ranked_manifest WHERE rn = 1) "
                "SELECT COUNT_IF(m.status IS NULL OR m.status NOT IN "
                "('success', 'legitimate_empty')) FROM expected x LEFT JOIN "
                "latest_manifest m ON m.target_id = x.target_id AND "
                "m.endpoint = x.endpoint"
            ),
            expected_value=0,
        )
    )
    return queries


def _dq_query_passed(query: DQQuery, observed: Any) -> bool:
    if observed is None or isinstance(observed, bool):
        return False
    try:
        actual = float(observed)
        expected = float(query.expected_value)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(actual):
        return False
    if query.comparator == "eq":
        return actual == expected
    if query.comparator == "gte":
        return actual >= expected
    raise SofaScoreContractError(
        f"unknown DQ comparator {query.comparator!r} for {query.name}"
    )


def run_committed_partition_dq(
    partition: ActiveRegistryPartition,
    connection: Any,
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute every committed-state gate for one active registry partition."""

    if not isinstance(partition, ActiveRegistryPartition):
        raise TypeError("partition must be ActiveRegistryPartition")
    queries = build_partition_dq_queries(
        partition.league,
        partition.season,
        partition.source_tournament_id,
        partition.source_season_id,
        contract=contract,
    )
    results = []
    violations = []
    for query in queries:
        cursor = connection.cursor()
        try:
            cursor.execute(query.sql)
            row = cursor.fetchone()
        except Exception as exc:
            raise SofaScoreDQViolation(
                f"{partition.league}/{partition.season} {query.name} query "
                f"failed: {exc}"
            ) from exc
        finally:
            cursor.close()
        observed = row[0] if row else None
        passed = _dq_query_passed(query, observed)
        result = {
            "name": query.name,
            "observed": observed,
            "expected": query.expected_value,
            "comparator": query.comparator,
            "passed": passed,
        }
        results.append(result)
        if not passed:
            violations.append(result)

    if violations:
        detail = "; ".join(
            f"{item['name']} observed={item['observed']!r} "
            f"expected {item['comparator']} {item['expected']!r}"
            for item in violations[:10]
        )
        raise SofaScoreDQViolation(
            f"{partition.league}/{partition.season} committed-state DQ "
            f"failed {len(violations)}/{len(results)} checks: {detail}"
        )
    return {
        "league": partition.league,
        "season": partition.season,
        "source_tournament_id": partition.source_tournament_id,
        "source_season_id": partition.source_season_id,
        "checks": len(results),
        "results": results,
    }


def run_active_registry_committed_dq(
    *,
    registry_path: str | os.PathLike[str] | None = None,
    connection: Any | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run committed-state DQ for all capture-allowed registry partitions."""

    partitions = active_registry_partitions(registry_path)
    owns_connection = connection is None
    if owns_connection:
        from utils.data_quality import _get_conn

        connection = _get_conn()
    results = []
    try:
        for partition in partitions:
            results.append(
                run_committed_partition_dq(
                    partition,
                    connection,
                    contract=contract,
                )
            )
    finally:
        if owns_connection and connection is not None:
            connection.close()
    return {
        "status": "success",
        "partitions": len(results),
        "checks": sum(result["checks"] for result in results),
        "results": results,
    }


__all__ = [
    "COVERAGE_STATUSES",
    "MANIFEST_STATES",
    "REQUIRED_ACCEPTABLE_STATES",
    "OPTIONAL_ACCEPTABLE_STATES",
    "RETRYABLE_HTTP_STATUSES",
    "ActiveRegistryPartition",
    "CaptureExpectation",
    "DQFinding",
    "DQQuery",
    "DQReport",
    "SofaScoreContractError",
    "SofaScoreDQViolation",
    "active_registry_partitions",
    "build_partition_dq_queries",
    "compare_schema_fingerprints",
    "load_coverage_contract",
    "manifest_state_for_response",
    "raw_payload_sha256",
    "run_active_registry_committed_dq",
    "run_committed_partition_dq",
    "reset_coverage_cache",
    "resolve_json_pointer",
    "schema_fingerprint",
    "validate_bronze_compatibility_columns",
    "validate_coverage_contract",
    "validate_event_participants",
    "validate_lineup_semantics",
    "validate_manifest_completeness",
    "validate_minimum_coverage",
    "validate_offline_replay",
    "validate_partition_replacement",
    "validate_player_coverage",
    "validate_raw_payload",
    "validate_schedule_rows",
    "validate_season_alignment",
    "validate_table_rows",
]
