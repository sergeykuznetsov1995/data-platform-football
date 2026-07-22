"""Immutable acceptance evidence for the staged WhoScored daily rollout.

The module deliberately has no Airflow dependency.  The DAG supplies the
current run context and the immediately preceding terminal scheduled DagRun;
the production-admission command can independently replay the same receipt
validation from the operational object store.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping, Optional, Sequence


ACCEPTANCE_SCHEMA_VERSION = 1
ACCEPTANCE_PREFIX = "production/whoscored-rollout/v1"
REQUIRED_CONSECUTIVE_SUCCESSES = 2
REQUIRED_PARSER_VERSION = "whoscored-parser-v8"
WAVE_ORDER = ("wave-20", "wave-70", "wave-all")
WAVE_LIMITS = {"wave-20": 20, "wave-70": 70, "wave-all": 2_000}
ROLLOUT_GENESIS_PROOF_SHA256 = hashlib.sha256(
    b"whoscored-rollout-promotion-genesis-v1"
).hexdigest()
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_DIGEST = re.compile(r"[0-9a-f]{64}")

_SCOPE_EXACT_MANIFEST_PAIRS = (
    ("scope", "expected_scope_batches", "exact_scope_manifests"),
    ("match", "expected_match_batches", "exact_match_manifests"),
    (
        "match_not_available",
        "expected_match_not_available_batches",
        "exact_match_not_available_manifests",
    ),
    ("preview", "expected_preview_batches", "exact_preview_manifests"),
    (
        "preview_not_available",
        "expected_preview_not_available_batches",
        "exact_preview_not_available_manifests",
    ),
)
_SCOPE_PHYSICAL_CURRENT_PAIRS = (
    ("schedule", "schedule_rows", "schedule_games"),
    ("events", "manifest_event_rows", "current_event_rows"),
    ("lineups", "manifest_lineup_rows", "current_lineup_rows"),
    ("matches", "manifest_match_rows", "current_match_rows"),
    (
        "substitutions",
        "manifest_substitution_rows",
        "current_substitution_rows",
    ),
    ("formations", "manifest_formation_rows", "current_formation_rows"),
    ("team_stats", "manifest_team_stat_rows", "current_team_stat_rows"),
    ("player_stats", "manifest_player_stat_rows", "current_player_stat_rows"),
    (
        "missing_players",
        "manifest_missing_player_rows",
        "current_missing_player_rows",
    ),
    (
        "preview_lineups",
        "manifest_preview_lineup_rows",
        "current_preview_lineup_rows",
    ),
    (
        "preview_sections",
        "manifest_preview_section_rows",
        "current_preview_section_rows",
    ),
)
_SCOPE_DUPLICATE_COUNTERS = (
    "exact_match_outcome_duplicates",
    "exact_preview_outcome_duplicates",
    "duplicate_source_event_ids",
    "duplicate_team_event_ids",
)
_SCOPE_ZERO_MISMATCH_COUNTERS = (
    "exact_scope_dataset_mismatches",
    "exact_match_dataset_mismatches",
    "exact_match_not_available_physical_rows",
    "exact_preview_dataset_mismatches",
    "exact_preview_not_available_physical_rows",
    "scope_manifest_mismatches",
)
_SCOPE_OUTCOMES = (
    ("match", "exact_match_outcome_count", "exact_match_outcome_payload_sha256"),
    (
        "preview",
        "exact_preview_outcome_count",
        "exact_preview_outcome_payload_sha256",
    ),
)
_PROFILE_EXACT_MANIFEST_PAIRS = (
    ("profile", "expected_profile_batches", "exact_profile_manifests"),
    (
        "profile_not_available",
        "expected_profile_not_available_batches",
        "exact_profile_not_available_manifests",
    ),
)
_PROFILE_PHYSICAL_CURRENT_PAIRS = (
    ("profiles", "current_profile_manifests", "current_profile_rows"),
    (
        "participations",
        "manifest_participation_rows",
        "current_participation_rows",
    ),
)
_PROFILE_DUPLICATE_COUNTERS = ("exact_profile_outcome_duplicates",)
_PROFILE_ZERO_MISMATCH_COUNTERS = (
    "exact_profile_row_mismatches",
    "exact_profile_participation_mismatches",
    "exact_profile_not_available_physical_rows",
    "uncovered_profiles",
    "stale_profiles",
)


class WhoScoredRolloutAcceptanceError(RuntimeError):
    """The rollout evidence is missing, inconsistent, or ineligible."""


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout evidence is not canonical JSON"
        ) from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _digest(value: Any, *, label: str) -> str:
    result = str(value or "")
    if _DIGEST.fullmatch(result) is None:
        raise WhoScoredRolloutAcceptanceError(
            f"invalid WhoScored rollout {label} digest"
        )
    return result


def _positive_int(value: Any, *, label: str, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WhoScoredRolloutAcceptanceError(f"invalid WhoScored rollout {label}")
    return value


def _utc_iso(value: Any, *, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise WhoScoredRolloutAcceptanceError(
            f"WhoScored rollout {label} must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(value: Any, *, label: str) -> datetime:
    raw = str(value or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WhoScoredRolloutAcceptanceError(
            f"invalid WhoScored rollout {label}"
        ) from exc
    if parsed.tzinfo is None or _utc_iso(parsed, label=label) != raw:
        raise WhoScoredRolloutAcceptanceError(
            f"non-canonical WhoScored rollout {label}"
        )
    return parsed.astimezone(timezone.utc)


def _run_type(value: Any) -> str:
    return str(value or "").lower().split(".")[-1]


def is_countable_scheduled_run(
    *,
    run_id: Any,
    run_type: Any,
    external_trigger: Any,
    conf: Any,
) -> bool:
    """Only an unmodified scheduler-created daily instance can count."""

    return (
        isinstance(run_id, str)
        and run_id.startswith("scheduled__")
        and _run_type(run_type) == "scheduled"
        and external_trigger is False
        and type(conf) is dict
        and conf == {}
    )


def receipts_prefix(rollout_id: str) -> str:
    if _TOKEN.fullmatch(rollout_id) is None:
        raise WhoScoredRolloutAcceptanceError("invalid WhoScored rollout id")
    return f"{ACCEPTANCE_PREFIX}/{rollout_id}/receipts"


def _scope_identity(scope_plan: Mapping[str, Any]) -> dict[str, Any]:
    rollout_id = str(scope_plan.get("rollout_id") or "")
    wave_id = str(scope_plan.get("wave_id") or "")
    if _TOKEN.fullmatch(rollout_id) is None or wave_id not in WAVE_ORDER:
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored signed rollout identity"
        )
    max_scopes = _positive_int(scope_plan.get("max_scopes"), label="max scopes")
    if max_scopes != WAVE_LIMITS[wave_id]:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout wave limit differs from the production contract"
        )
    require_full_active = scope_plan.get("require_full_active")
    if type(require_full_active) is not bool or require_full_active != (
        wave_id == "wave-all"
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout full-active flag differs from its wave"
        )
    active_count = _positive_int(
        scope_plan.get("active_scope_count"), label="selected scope count"
    )
    catalog_count = _positive_int(
        scope_plan.get("catalog_active_scope_count"),
        label="catalog active scope count",
    )
    deferred_count = _positive_int(
        scope_plan.get("deferred_scope_count"),
        label="deferred scope count",
        allow_zero=True,
    )
    if active_count + deferred_count != catalog_count:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored selected and deferred scopes do not partition the catalog"
        )
    expected_selected = min(max_scopes, catalog_count)
    if active_count != expected_selected:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout did not select its exact cumulative wave"
        )
    if require_full_active and (active_count != catalog_count or deferred_count != 0):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored final rollout wave is not the full active catalog"
        )
    selected_scopes = scope_plan.get("active_scopes")
    if (
        not isinstance(selected_scopes, list)
        or len(selected_scopes) != active_count
        or any(not isinstance(item, str) or not item for item in selected_scopes)
        or len(selected_scopes) != len(set(selected_scopes))
        or hashlib.sha256(
            ("\n".join(selected_scopes) + "\n").encode("utf-8")
        ).hexdigest()
        != scope_plan.get("active_scopes_sha256")
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout selected scope preimage is invalid"
        )
    if require_full_active and (
        scope_plan.get("ranked_scope_ids_sha256")
        != hashlib.sha256(
            ("\n".join(selected_scopes) + "\n").encode("utf-8")
        ).hexdigest()
        or scope_plan.get("catalog_active_scopes_sha256")
        != hashlib.sha256(
            ("\n".join(sorted(selected_scopes)) + "\n").encode("utf-8")
        ).hexdigest()
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored final wave does not reveal the frozen rank/catalog preimage"
        )
    runtime_sha256 = _digest(scope_plan.get("runtime_sha256"), label="runtime")
    classifier_sha256 = _digest(scope_plan.get("classifier_sha256"), label="classifier")
    promotion_acceptance_sha256 = _digest(
        scope_plan.get("promotion_acceptance_sha256"),
        label="promotion acceptance",
    )
    promotion_terminal_receipt_sha256 = _digest(
        scope_plan.get("promotion_terminal_receipt_sha256"),
        label="promotion terminal receipt",
    )
    if (
        wave_id == "wave-20"
        and (
            promotion_acceptance_sha256 != ROLLOUT_GENESIS_PROOF_SHA256
            or promotion_terminal_receipt_sha256 != ROLLOUT_GENESIS_PROOF_SHA256
        )
    ) or (
        wave_id != "wave-20"
        and (
            promotion_acceptance_sha256 == ROLLOUT_GENESIS_PROOF_SHA256
            or promotion_terminal_receipt_sha256 == ROLLOUT_GENESIS_PROOF_SHA256
        )
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout promotion proof differs from its wave"
        )
    return {
        "rollout_id": rollout_id,
        "wave_id": wave_id,
        "max_scopes": max_scopes,
        "require_full_active": require_full_active,
        "selected_scope_count": active_count,
        "selected_scopes": list(selected_scopes),
        "selected_scopes_sha256": _digest(
            scope_plan.get("active_scopes_sha256"), label="selected scopes"
        ),
        "catalog_active_scope_count": catalog_count,
        "catalog_active_scopes_sha256": _digest(
            scope_plan.get("catalog_active_scopes_sha256"),
            label="catalog active scopes",
        ),
        "deferred_scope_count": deferred_count,
        "deferred_scopes_sha256": _digest(
            scope_plan.get("deferred_scopes_sha256"), label="deferred scopes"
        ),
        "cohort_sha256": _digest(scope_plan.get("cohort_sha256"), label="cohort"),
        "workload_sha256": _digest(scope_plan.get("workload_sha256"), label="workload"),
        "ranked_scope_ids_sha256": _digest(
            scope_plan.get("ranked_scope_ids_sha256"), label="ranked scope ids"
        ),
        "ranked_workload_sha256": _digest(
            scope_plan.get("ranked_workload_sha256"), label="ranked workload"
        ),
        "runtime_sha256": runtime_sha256,
        "classifier_sha256": classifier_sha256,
        "promotion_acceptance_sha256": promotion_acceptance_sha256,
        "promotion_terminal_receipt_sha256": promotion_terminal_receipt_sha256,
    }


def normalized_scope_plan_authority(
    scope_plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Public strict projection used to bind receipts to the persisted DAG XCom."""

    return _scope_identity(scope_plan)


def scope_plan_sha256(scope_plan: Mapping[str, Any]) -> str:
    return _sha256_json(normalized_scope_plan_authority(scope_plan))


def scope_plan_authority_sha256(scope_plan: Mapping[str, Any]) -> str:
    """Compatibility alias for callers using the longer authority name."""

    return scope_plan_sha256(scope_plan)


def _release_identity(runtime_preflight: Mapping[str, Any]) -> dict[str, str]:
    if (
        runtime_preflight.get("status") != "success"
        or runtime_preflight.get("transport_policy") != "direct_then_paid"
        or runtime_preflight.get("direct_only") is not False
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout requires admitted direct_then_paid runtime"
        )
    contract = runtime_preflight.get("runtime_contract")
    if not isinstance(contract, Mapping):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout runtime contract is missing"
        )
    parser_version = str(contract.get("parser_version") or "")
    if parser_version != REQUIRED_PARSER_VERSION:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout is not running parser-v8"
        )
    return {
        "parser_version": parser_version,
        "manifest_sha256": _digest(
            contract.get("manifest_sha256"), label="runtime manifest"
        ),
        "code_tree_sha256": _digest(
            contract.get("code_tree_sha256"), label="runtime code tree"
        ),
    }


def terminal_task_states_evidence(
    states: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Canonical hash/count witness for every successful TaskInstance."""

    canonical: list[dict[str, Any]] = []
    identities: set[tuple[str, int]] = set()
    for raw in states:
        if not isinstance(raw, Mapping) or set(raw) != {
            "task_id",
            "map_index",
            "state",
        }:
            raise WhoScoredRolloutAcceptanceError(
                "invalid WhoScored terminal task-state evidence"
            )
        task_id = str(raw.get("task_id") or "")
        map_index = raw.get("map_index")
        state = str(raw.get("state") or "").lower().split(".")[-1]
        if (
            not task_id
            or len(task_id) > 250
            or isinstance(map_index, bool)
            or not isinstance(map_index, int)
            or map_index < -1
            or state != "success"
            or (task_id, map_index) in identities
        ):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored terminal task-state witness is not all-success"
            )
        identities.add((task_id, map_index))
        canonical.append({"task_id": task_id, "map_index": map_index, "state": state})
    canonical.sort(key=lambda item: (item["task_id"], item["map_index"]))
    if not canonical or not any(
        item["task_id"] == "final_success_gate" for item in canonical
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored terminal witness lacks the successful final gate"
        )
    return {"count": len(canonical), "sha256": _sha256_json(canonical)}


def mapped_scope_dq_evidence(
    values: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    materialized: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored mapped scope DQ evidence is not an object"
            )
        materialized.append(dict(value))
    scopes = [str(value.get("scope") or "") for value in materialized]
    scopes_sha256 = hashlib.sha256(
        ("\n".join(scopes) + ("\n" if scopes else "")).encode("utf-8")
    ).hexdigest()
    if any(not scope for scope in scopes) or len(scopes) != len(set(scopes)):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored mapped scope DQ evidence is not exact and green"
        )
    return {
        "count": len(materialized),
        "sha256": _sha256_json(materialized),
        "scopes_sha256": scopes_sha256,
    }


def _dq_counter(value: Mapping[str, Any], field: str, *, label: str) -> int:
    counter = value.get(field)
    if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
        raise WhoScoredRolloutAcceptanceError(
            f"invalid WhoScored idempotency {label} counter: {field}"
        )
    return counter


def _dq_pairs(
    value: Mapping[str, Any],
    pairs: Sequence[tuple[str, str, str]],
    *,
    label: str,
) -> tuple[dict[str, Any], int]:
    projection: dict[str, Any] = {}
    violations = 0
    for name, left_field, right_field in pairs:
        left = _dq_counter(value, left_field, label=label)
        right = _dq_counter(value, right_field, label=label)
        projection[name] = {left_field: left, right_field: right}
        violations += left != right
    return projection, violations


def _dq_zero_counters(
    value: Mapping[str, Any], fields: Sequence[str], *, label: str
) -> tuple[dict[str, int], int]:
    projection = {
        field: _dq_counter(value, field, label=label) for field in fields
    }
    return projection, sum(counter != 0 for counter in projection.values())


def idempotency_evidence(
    *,
    scope_dq: Iterable[Mapping[str, Any]],
    profile_dq: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a compact, replayable proof of exact idempotent Bronze state."""

    scope_values = list(scope_dq)
    if not 1 <= len(scope_values) <= WAVE_LIMITS["wave-all"]:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored idempotency scope evidence is empty or oversized"
        )
    scope_projection: list[dict[str, Any]] = []
    scope_identities: set[str] = set()
    scope_violations = 0
    for raw in scope_values:
        if not isinstance(raw, Mapping):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored idempotency scope evidence is not an object"
            )
        scope = raw.get("scope")
        if (
            not isinstance(scope, str)
            or not scope
            or scope != scope.strip()
            or len(scope) > 512
            or scope in scope_identities
        ):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored idempotency scope identity is invalid"
            )
        scope_identities.add(scope)
        exact_manifests, exact_violations = _dq_pairs(
            raw, _SCOPE_EXACT_MANIFEST_PAIRS, label="scope"
        )
        physical_current, parity_violations = _dq_pairs(
            raw, _SCOPE_PHYSICAL_CURRENT_PAIRS, label="scope"
        )
        duplicates, duplicate_violations = _dq_zero_counters(
            raw, _SCOPE_DUPLICATE_COUNTERS, label="scope"
        )
        zero_mismatches, mismatch_violations = _dq_zero_counters(
            raw, _SCOPE_ZERO_MISMATCH_COUNTERS, label="scope"
        )
        outcomes: dict[str, Any] = {}
        for name, count_field, digest_field in _SCOPE_OUTCOMES:
            outcomes[name] = {
                "count": _dq_counter(raw, count_field, label="scope"),
                "payload_sha256": _digest(
                    raw.get(digest_field), label=f"idempotency scope {name} outcome"
                ),
            }
        scope_violations += (
            exact_violations
            + parity_violations
            + duplicate_violations
            + mismatch_violations
        )
        scope_projection.append(
            {
                "scope": scope,
                "exact_manifests": exact_manifests,
                "duplicates": duplicates,
                "physical_current": physical_current,
                "zero_mismatches": zero_mismatches,
                "outcomes": outcomes,
            }
        )

    if not isinstance(profile_dq, Mapping) or profile_dq.get("status") != "success":
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored idempotency profile evidence is not green"
        )
    profile_exact, profile_exact_violations = _dq_pairs(
        profile_dq, _PROFILE_EXACT_MANIFEST_PAIRS, label="profile"
    )
    profile_physical_current, profile_parity_violations = _dq_pairs(
        profile_dq, _PROFILE_PHYSICAL_CURRENT_PAIRS, label="profile"
    )
    profile_duplicates, profile_duplicate_violations = _dq_zero_counters(
        profile_dq, _PROFILE_DUPLICATE_COUNTERS, label="profile"
    )
    profile_zero_mismatches, profile_mismatch_violations = _dq_zero_counters(
        profile_dq, _PROFILE_ZERO_MISMATCH_COUNTERS, label="profile"
    )
    profile_projection = {
        "roster_players": _dq_counter(
            profile_dq, "roster_players", label="profile"
        ),
        "exact_manifests": profile_exact,
        "duplicates": profile_duplicates,
        "physical_current": profile_physical_current,
        "zero_mismatches": profile_zero_mismatches,
        "outcome": {
            "count": _dq_counter(
                profile_dq, "exact_profile_outcome_count", label="profile"
            ),
            "payload_sha256": _digest(
                profile_dq.get("exact_profile_outcome_payload_sha256"),
                label="idempotency profile outcome",
            ),
        },
    }
    profile_violations = (
        profile_exact_violations
        + profile_parity_violations
        + profile_duplicate_violations
        + profile_mismatch_violations
    )
    if scope_violations or profile_violations:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored idempotency evidence is not green"
        )
    return {
        "schema_version": 1,
        "status": "green",
        "scope": {
            "scope_count": len(scope_projection),
            "exact_manifest_pair_count": len(scope_projection)
            * len(_SCOPE_EXACT_MANIFEST_PAIRS),
            "duplicate_counter_count": len(scope_projection)
            * len(_SCOPE_DUPLICATE_COUNTERS),
            "physical_current_pair_count": len(scope_projection)
            * len(_SCOPE_PHYSICAL_CURRENT_PAIRS),
            "zero_mismatch_counter_count": len(scope_projection)
            * len(_SCOPE_ZERO_MISMATCH_COUNTERS),
            "violation_count": 0,
            "evidence_sha256": _sha256_json(scope_projection),
        },
        "profile": {
            "exact_manifest_pair_count": len(_PROFILE_EXACT_MANIFEST_PAIRS),
            "duplicate_counter_count": len(_PROFILE_DUPLICATE_COUNTERS),
            "physical_current_pair_count": len(_PROFILE_PHYSICAL_CURRENT_PAIRS),
            "zero_mismatch_counter_count": len(_PROFILE_ZERO_MISMATCH_COUNTERS),
            "violation_count": 0,
            "evidence_sha256": _sha256_json(profile_projection),
        },
    }


def _validated_idempotency_witness(
    value: Any, *, expected_scope_count: int
) -> dict[str, Any]:
    top_fields = {"schema_version", "status", "scope", "profile"}
    scope_fields = {
        "scope_count",
        "exact_manifest_pair_count",
        "duplicate_counter_count",
        "physical_current_pair_count",
        "zero_mismatch_counter_count",
        "violation_count",
        "evidence_sha256",
    }
    profile_fields = scope_fields - {"scope_count"}
    if (
        not isinstance(value, Mapping)
        or set(value) != top_fields
        or value.get("schema_version") != 1
        or value.get("status") != "green"
        or not isinstance(value.get("scope"), Mapping)
        or not isinstance(value.get("profile"), Mapping)
        or set(value["scope"]) != scope_fields
        or set(value["profile"]) != profile_fields
    ):
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored idempotency witness schema"
        )
    scope = value["scope"]
    profile = value["profile"]
    expected_scope = {
        "scope_count": expected_scope_count,
        "exact_manifest_pair_count": expected_scope_count
        * len(_SCOPE_EXACT_MANIFEST_PAIRS),
        "duplicate_counter_count": expected_scope_count
        * len(_SCOPE_DUPLICATE_COUNTERS),
        "physical_current_pair_count": expected_scope_count
        * len(_SCOPE_PHYSICAL_CURRENT_PAIRS),
        "zero_mismatch_counter_count": expected_scope_count
        * len(_SCOPE_ZERO_MISMATCH_COUNTERS),
        "violation_count": 0,
    }
    expected_profile = {
        "exact_manifest_pair_count": len(_PROFILE_EXACT_MANIFEST_PAIRS),
        "duplicate_counter_count": len(_PROFILE_DUPLICATE_COUNTERS),
        "physical_current_pair_count": len(_PROFILE_PHYSICAL_CURRENT_PAIRS),
        "zero_mismatch_counter_count": len(_PROFILE_ZERO_MISMATCH_COUNTERS),
        "violation_count": 0,
    }
    if any(scope.get(field) != expected for field, expected in expected_scope.items()) or any(
        profile.get(field) != expected
        for field, expected in expected_profile.items()
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored idempotency witness is not green"
        )
    for witness in (scope, profile):
        _digest(witness.get("evidence_sha256"), label="idempotency evidence")
    return {
        "schema_version": 1,
        "status": "green",
        "scope": dict(scope),
        "profile": dict(profile),
    }


def _validate_green_evidence(
    *,
    scope: Mapping[str, Any],
    runtime_preflight: Mapping[str, Any],
    catalog_dq: Mapping[str, Any],
    profile_dq: Mapping[str, Any],
    traffic_dq: Mapping[str, Any],
    daily_slo: Mapping[str, Any],
    alert_preflight: Mapping[str, Any],
    scope_dq: Iterable[Mapping[str, Any]],
    terminal_task_states: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    if catalog_dq.get("status") != "success":
        raise WhoScoredRolloutAcceptanceError("WhoScored catalog DQ is not green")
    if catalog_dq.get("active_scopes") != scope["catalog_active_scope_count"]:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored catalog DQ scope count differs from the signed rollout"
        )
    if profile_dq.get("status") != "success":
        raise WhoScoredRolloutAcceptanceError("WhoScored profile DQ is not green")
    if daily_slo.get("status") != "success":
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rolling daily SLO is not green"
        )
    if (
        set(alert_preflight)
        != {
            "status",
            "campaign_id",
            "approval_id",
            "approval_sha256",
            "transport_policy",
        }
        or alert_preflight.get("status") != "delivered"
        or alert_preflight.get("transport_policy") != "direct_then_paid"
        or alert_preflight.get("campaign_id") != runtime_preflight.get("campaign_id")
        or alert_preflight.get("approval_id") != runtime_preflight.get("approval_id")
        or alert_preflight.get("approval_sha256")
        != runtime_preflight.get("approval_sha256")
        or _TOKEN.fullmatch(str(alert_preflight.get("campaign_id") or "")) is None
        or _TOKEN.fullmatch(str(alert_preflight.get("approval_id") or "")) is None
        or _DIGEST.fullmatch(str(alert_preflight.get("approval_sha256") or "")) is None
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored paid alert delivery is not green"
        )
    if traffic_dq.get("schema_version") != 1:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored traffic reconciliation evidence is invalid"
        )
    paid = traffic_dq.get("paid_proxy_bytes")
    if isinstance(paid, bool) or not isinstance(paid, int) or paid < 0:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored traffic paid-byte total is invalid"
        )
    for field in (
        "reported_paid_proxy_bytes",
        "request_ledger_paid_proxy_bytes",
        "durable_paid_proxy_bytes",
        "campaign_paid_proxy_bytes",
    ):
        if traffic_dq.get(field) != paid:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored traffic reconciliation is not exact"
            )
    _digest(traffic_dq.get("artifact_sha256"), label="traffic artifact")
    _positive_int(traffic_dq.get("artifact_bytes"), label="traffic artifact bytes")
    scope_dq_values = list(scope_dq)
    scope_dq_witness = mapped_scope_dq_evidence(scope_dq_values)
    if (
        scope_dq_witness["count"] != scope["selected_scope_count"]
        or scope_dq_witness["scopes_sha256"] != scope["selected_scopes_sha256"]
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored mapped scope DQ evidence is not exact and green"
        )
    return {
        "catalog_dq_sha256": _sha256_json(dict(catalog_dq)),
        "scope_dq": scope_dq_witness,
        "idempotency": idempotency_evidence(
            scope_dq=scope_dq_values,
            profile_dq=profile_dq,
        ),
        "profile_dq_sha256": _sha256_json(dict(profile_dq)),
        "traffic_dq_sha256": _sha256_json(dict(traffic_dq)),
        "daily_slo_sha256": _sha256_json(dict(daily_slo)),
        "alert_preflight_sha256": _sha256_json(dict(alert_preflight)),
        "terminal_task_states": terminal_task_states_evidence(terminal_task_states),
    }


def _receipt_digest_from_key(key: str) -> str:
    digest = PurePosixPath(key).stem
    return _digest(digest, label="receipt key")


def _identity_payload(receipt: Mapping[str, Any]) -> dict[str, Any]:
    scope = receipt["scope"]
    release = receipt["release"]
    return {
        "rollout_id": scope["rollout_id"],
        "wave_id": scope["wave_id"],
        "max_scopes": scope["max_scopes"],
        "require_full_active": scope["require_full_active"],
        "selected_scope_count": scope["selected_scope_count"],
        "selected_scopes_sha256": scope["selected_scopes_sha256"],
        "catalog_active_scope_count": scope["catalog_active_scope_count"],
        "catalog_active_scopes_sha256": scope["catalog_active_scopes_sha256"],
        "cohort_sha256": scope["cohort_sha256"],
        "ranked_scope_ids_sha256": scope["ranked_scope_ids_sha256"],
        "runtime_sha256": scope["runtime_sha256"],
        "classifier_sha256": scope["classifier_sha256"],
        "promotion_acceptance_sha256": scope["promotion_acceptance_sha256"],
        "promotion_terminal_receipt_sha256": scope["promotion_terminal_receipt_sha256"],
        "parser_version": release["parser_version"],
        "manifest_sha256": release["manifest_sha256"],
        "code_tree_sha256": release["code_tree_sha256"],
    }


def _validate_receipt(key: str, value: Mapping[str, Any]) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "receipt_type",
        "dag_id",
        "run_id",
        "logical_date",
        "conf_sha256",
        "scope",
        "release",
        "evidence",
        "identity_sha256",
        "previous_scheduled_run",
        "previous_run_receipt_sha256",
        "prior_wave_receipt_sha256",
        "consecutive_successes",
        "wave_accepted",
    }
    receipt = dict(value)
    if (
        set(receipt) != expected_fields
        or receipt.get("schema_version") != ACCEPTANCE_SCHEMA_VERSION
        or receipt.get("receipt_type") != "whoscored-rollout-run-success"
        or receipt.get("dag_id") != "dag_ingest_whoscored"
        or not isinstance(receipt.get("run_id"), str)
        or not receipt["run_id"].startswith("scheduled__")
        or receipt.get("conf_sha256") != _sha256_json({})
        or not isinstance(receipt.get("scope"), Mapping)
        or not isinstance(receipt.get("release"), Mapping)
        or not isinstance(receipt.get("evidence"), Mapping)
    ):
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout acceptance receipt"
        )
    _parse_utc_iso(receipt["logical_date"], label="receipt logical date")
    scope = receipt["scope"]
    release = receipt["release"]
    evidence = receipt["evidence"]
    if set(scope) != {
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "selected_scope_count",
        "selected_scopes",
        "selected_scopes_sha256",
        "catalog_active_scope_count",
        "catalog_active_scopes_sha256",
        "deferred_scope_count",
        "deferred_scopes_sha256",
        "cohort_sha256",
        "workload_sha256",
        "ranked_scope_ids_sha256",
        "ranked_workload_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    }:
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout receipt scope schema"
        )
    rollout_id = str(scope.get("rollout_id") or "")
    wave_id = str(scope.get("wave_id") or "")
    if _TOKEN.fullmatch(rollout_id) is None or wave_id not in WAVE_ORDER:
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout receipt scope identity"
        )
    if not key.startswith(f"{receipts_prefix(rollout_id)}/"):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout receipt key differs from its rollout id"
        )
    max_scopes = _positive_int(scope.get("max_scopes"), label="receipt max scopes")
    require_full_active = scope.get("require_full_active")
    selected_count = _positive_int(
        scope.get("selected_scope_count"), label="receipt selected scope count"
    )
    selected_scopes = scope.get("selected_scopes")
    catalog_count = _positive_int(
        scope.get("catalog_active_scope_count"), label="receipt catalog scope count"
    )
    deferred_count = _positive_int(
        scope.get("deferred_scope_count"),
        label="receipt deferred scope count",
        allow_zero=True,
    )
    if (
        max_scopes != WAVE_LIMITS[wave_id]
        or type(require_full_active) is not bool
        or require_full_active != (wave_id == "wave-all")
        or selected_count != min(max_scopes, catalog_count)
        or not isinstance(selected_scopes, list)
        or len(selected_scopes) != selected_count
        or any(not isinstance(item, str) or not item for item in selected_scopes)
        or len(selected_scopes) != len(set(selected_scopes))
        or hashlib.sha256(
            ("\n".join(selected_scopes) + "\n").encode("utf-8")
        ).hexdigest()
        != scope.get("selected_scopes_sha256")
        or selected_count + deferred_count != catalog_count
        or (
            require_full_active
            and (selected_count != catalog_count or deferred_count != 0)
        )
    ):
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout receipt wave scope"
        )
    if require_full_active and (
        scope.get("ranked_scope_ids_sha256")
        != hashlib.sha256(
            ("\n".join(selected_scopes) + "\n").encode("utf-8")
        ).hexdigest()
        or scope.get("catalog_active_scopes_sha256")
        != hashlib.sha256(
            ("\n".join(sorted(selected_scopes)) + "\n").encode("utf-8")
        ).hexdigest()
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored final receipt rank/catalog preimage mismatch"
        )
    for field in (
        "selected_scopes_sha256",
        "catalog_active_scopes_sha256",
        "deferred_scopes_sha256",
        "cohort_sha256",
        "workload_sha256",
        "ranked_scope_ids_sha256",
        "ranked_workload_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    ):
        _digest(scope.get(field), label=f"receipt {field}")
    if (
        set(release) != {"parser_version", "manifest_sha256", "code_tree_sha256"}
        or release.get("parser_version") != REQUIRED_PARSER_VERSION
    ):
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout receipt release"
        )
    _digest(release.get("manifest_sha256"), label="receipt runtime manifest")
    _digest(release.get("code_tree_sha256"), label="receipt runtime code tree")
    if scope.get("runtime_sha256") != release.get("code_tree_sha256"):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored receipt release differs from its signed runtime pin"
        )
    if set(evidence) != {
        "catalog_dq_sha256",
        "scope_dq",
        "idempotency",
        "profile_dq_sha256",
        "traffic_dq_sha256",
        "daily_slo_sha256",
        "alert_preflight_sha256",
        "terminal_task_states",
    }:
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout receipt evidence schema"
        )
    for field in (
        "catalog_dq_sha256",
        "profile_dq_sha256",
        "traffic_dq_sha256",
        "daily_slo_sha256",
        "alert_preflight_sha256",
    ):
        _digest(evidence.get(field), label="evidence")
    for label, expected_witness_fields in (
        ("scope_dq", {"count", "sha256", "scopes_sha256"}),
        ("terminal_task_states", {"count", "sha256"}),
    ):
        witness = evidence.get(label)
        if not isinstance(witness, Mapping) or set(witness) != expected_witness_fields:
            raise WhoScoredRolloutAcceptanceError(
                f"invalid WhoScored rollout {label} witness"
            )
        _positive_int(witness.get("count"), label=f"{label} count")
        _digest(witness.get("sha256"), label=f"{label} witness")
        if label == "scope_dq":
            _digest(witness.get("scopes_sha256"), label="scope DQ identities")
    if (
        evidence["scope_dq"]["count"] != selected_count
        or evidence["scope_dq"]["scopes_sha256"] != scope["selected_scopes_sha256"]
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout scope DQ witness differs from its frozen wave"
        )
    _validated_idempotency_witness(
        evidence.get("idempotency"), expected_scope_count=selected_count
    )
    previous = receipt.get("previous_scheduled_run")
    if previous is not None:
        if (
            not isinstance(previous, Mapping)
            or set(previous) != {"run_id", "state", "logical_date"}
            or not isinstance(previous.get("run_id"), str)
            or not previous["run_id"].startswith("scheduled__")
            or previous.get("state") not in {"success", "failed"}
        ):
            raise WhoScoredRolloutAcceptanceError(
                "invalid WhoScored previous scheduled run evidence"
            )
        _parse_utc_iso(
            previous.get("logical_date"), label="previous scheduled logical date"
        )
    prior_wave = receipt.get("prior_wave_receipt_sha256")
    if wave_id == WAVE_ORDER[0]:
        if (
            prior_wave is not None
            or scope.get("promotion_acceptance_sha256") != ROLLOUT_GENESIS_PROOF_SHA256
            or scope.get("promotion_terminal_receipt_sha256")
            != ROLLOUT_GENESIS_PROOF_SHA256
        ):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored first rollout wave must carry exact genesis authority"
            )
    else:
        _digest(prior_wave, label="prior wave receipt")
        if (
            scope.get("promotion_acceptance_sha256") == ROLLOUT_GENESIS_PROOF_SHA256
            or scope.get("promotion_terminal_receipt_sha256")
            == ROLLOUT_GENESIS_PROOF_SHA256
            or prior_wave != scope.get("promotion_terminal_receipt_sha256")
        ):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored promoted receipt differs from authorized prior terminal"
            )
    identity_sha256 = _sha256_json(_identity_payload(receipt))
    if receipt.get("identity_sha256") != identity_sha256:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout receipt identity hash mismatch"
        )
    streak = receipt.get("consecutive_successes")
    if streak not in (1, REQUIRED_CONSECUTIVE_SUCCESSES) or receipt.get(
        "wave_accepted"
    ) is not (streak == REQUIRED_CONSECUTIVE_SUCCESSES):
        raise WhoScoredRolloutAcceptanceError(
            "invalid WhoScored rollout acceptance streak"
        )
    previous_receipt_sha256 = receipt.get("previous_run_receipt_sha256")
    if streak == REQUIRED_CONSECUTIVE_SUCCESSES:
        _digest(previous_receipt_sha256, label="previous run receipt")
    elif previous_receipt_sha256 is not None:
        raise WhoScoredRolloutAcceptanceError(
            "non-accepted WhoScored receipt cannot link a previous run receipt"
        )
    if streak == REQUIRED_CONSECUTIVE_SUCCESSES and (
        previous is None or previous.get("state") != "success"
    ):
        raise WhoScoredRolloutAcceptanceError(
            "accepted WhoScored rollout receipt lacks previous success"
        )
    expected_key = f"{receipts_prefix(rollout_id)}/{_sha256_json(receipt)}.json"
    if key != expected_key:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout receipt content-address mismatch"
        )
    return receipt


def validated_receipts(
    records: Iterable[tuple[str, Mapping[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    return _materialize_validated_receipts(records)


def _materialize_validated_receipts(
    records: Iterable[tuple[str, Mapping[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    materialized = list(records)
    return sorted(
        ((key, _validate_receipt(key, value)) for key, value in materialized),
        key=lambda item: (item[1]["logical_date"], item[0]),
    )


def _record_index(
    records: Sequence[tuple[str, Mapping[str, Any]]],
) -> dict[str, tuple[str, Mapping[str, Any]]]:
    return {_receipt_digest_from_key(key): (key, receipt) for key, receipt in records}


def _same_rollout_basis(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_scope = left["scope"]
    right_scope = right["scope"]
    return (
        left_scope["rollout_id"] == right_scope["rollout_id"]
        and left["release"] == right["release"]
        and left_scope["catalog_active_scope_count"]
        == right_scope["catalog_active_scope_count"]
        and left_scope["catalog_active_scopes_sha256"]
        == right_scope["catalog_active_scopes_sha256"]
        and left_scope["ranked_scope_ids_sha256"]
        == right_scope["ranked_scope_ids_sha256"]
        and left_scope["runtime_sha256"] == right_scope["runtime_sha256"]
        and left_scope["classifier_sha256"] == right_scope["classifier_sha256"]
    )


def _validate_accepted_chain(
    *,
    key: str,
    receipt: Mapping[str, Any],
    by_digest: Mapping[str, tuple[str, Mapping[str, Any]]],
    visiting: Optional[set[str]] = None,
) -> dict[str, tuple[str, Mapping[str, Any]]]:
    """Prove both same-wave successes and every cumulative prior wave."""

    digest = _receipt_digest_from_key(key)
    active_visiting = set(visiting or ())
    if digest in active_visiting or receipt.get("wave_accepted") is not True:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored accepted rollout chain is cyclic or incomplete"
        )
    active_visiting.add(digest)
    previous_digest = receipt.get("previous_run_receipt_sha256")
    previous_record = by_digest.get(previous_digest)
    if previous_record is None:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored accepted run is not linked to its first success"
        )
    _previous_key, previous_receipt = previous_record
    previous_scheduled = receipt.get("previous_scheduled_run")
    current_logical = _parse_utc_iso(
        receipt.get("logical_date"), label="accepted run logical date"
    )
    previous_logical = _parse_utc_iso(
        previous_receipt.get("logical_date"), label="linked run logical date"
    )
    if (
        previous_scheduled is None
        or previous_scheduled["state"] != "success"
        or previous_scheduled["run_id"] != previous_receipt["run_id"]
        or previous_scheduled["logical_date"] != previous_receipt["logical_date"]
        or previous_receipt["identity_sha256"] != receipt["identity_sha256"]
        or previous_receipt["prior_wave_receipt_sha256"]
        != receipt["prior_wave_receipt_sha256"]
        or (current_logical.date() - previous_logical.date()).days != 1
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored accepted run has an invalid same-wave success link"
        )
    wave_id = receipt["scope"]["wave_id"]
    wave_index = WAVE_ORDER.index(wave_id)
    chain: dict[str, tuple[str, Mapping[str, Any]]] = {}
    if wave_index:
        prior_digest = receipt["prior_wave_receipt_sha256"]
        prior_record = by_digest.get(prior_digest)
        if prior_record is None:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored accepted wave is not linked to its prior wave"
            )
        prior_key, prior_receipt = prior_record
        prior_selected_count = prior_receipt["scope"]["selected_scope_count"]
        prior_logical = _parse_utc_iso(
            prior_receipt["logical_date"], label="prior wave logical date"
        )
        if (
            prior_receipt["scope"]["wave_id"] != WAVE_ORDER[wave_index - 1]
            or receipt["scope"]["promotion_terminal_receipt_sha256"] != prior_digest
            or not _same_rollout_basis(receipt, prior_receipt)
            or receipt["scope"]["selected_scopes"][:prior_selected_count]
            != prior_receipt["scope"]["selected_scopes"]
            or prior_logical >= previous_logical
        ):
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored accepted waves do not share frozen rollout identity"
            )
        chain.update(
            _validate_accepted_chain(
                key=prior_key,
                receipt=prior_receipt,
                by_digest=by_digest,
                visiting=active_visiting,
            )
        )
    chain[wave_id] = (key, receipt)
    return chain


def _terminal_run_witness(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": receipt["run_id"],
        "logical_date": receipt["logical_date"],
        "scope_plan_sha256": _sha256_json(receipt["scope"]),
        "evidence_sha256": _sha256_json(
            {
                "scope": receipt["scope"],
                "release": receipt["release"],
                "evidence": receipt["evidence"],
            }
        ),
        "scope_dq": dict(receipt["evidence"]["scope_dq"]),
        "idempotency": _validated_idempotency_witness(
            receipt["evidence"]["idempotency"],
            expected_scope_count=receipt["scope"]["selected_scope_count"],
        ),
        "task_states": dict(receipt["evidence"]["terminal_task_states"]),
    }


def promotion_acceptance_evidence(
    records: Iterable[tuple[str, Mapping[str, Any]]],
    *,
    rollout_id: str,
    source_wave_id: str,
    expected_terminal_receipt_sha256: str,
) -> dict[str, Any]:
    """Replay the exact accepted predecessor chain used to authorize promotion.

    The caller supplies the expected content address so an arbitrary object-store
    listing cannot silently select a different accepted branch.  Only wave-20 and
    wave-70 are promotion sources: their selected chains contain exactly two and
    four scheduled-run receipts respectively.
    """

    receipts_prefix(rollout_id)
    if source_wave_id not in WAVE_ORDER[:-1]:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion source wave is invalid"
        )
    terminal_digest = _digest(
        expected_terminal_receipt_sha256,
        label="promotion terminal receipt",
    )
    validated = _materialize_validated_receipts(records)
    by_digest: dict[str, tuple[str, Mapping[str, Any]]] = {}
    seen_run_ids: set[str] = set()
    for key, receipt in validated:
        digest = _receipt_digest_from_key(key)
        if receipt["scope"]["rollout_id"] != rollout_id:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored promotion evidence contains a foreign rollout receipt"
            )
        if digest in by_digest or receipt["run_id"] in seen_run_ids:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored promotion evidence contains duplicate receipts"
            )
        by_digest[digest] = (key, receipt)
        seen_run_ids.add(receipt["run_id"])
    terminal_record = by_digest.get(terminal_digest)
    if terminal_record is None:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion terminal receipt is absent"
        )
    terminal_key, terminal_receipt = terminal_record
    if (
        terminal_receipt["scope"]["wave_id"] != source_wave_id
        or terminal_receipt["wave_accepted"] is not True
    ):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion terminal receipt is not the accepted source wave"
        )
    chain = _validate_accepted_chain(
        key=terminal_key,
        receipt=terminal_receipt,
        by_digest=by_digest,
    )
    expected_waves = WAVE_ORDER[: WAVE_ORDER.index(source_wave_id) + 1]
    if tuple(chain) != expected_waves:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion accepted predecessor chain is incomplete"
        )
    selected_digests: set[str] = set()
    for wave_id in expected_waves:
        accepted_key, accepted_receipt = chain[wave_id]
        accepted_digest = _receipt_digest_from_key(accepted_key)
        previous_digest = _digest(
            accepted_receipt.get("previous_run_receipt_sha256"),
            label="promotion previous run receipt",
        )
        if previous_digest not in by_digest:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored promotion predecessor run receipt is absent"
            )
        selected_digests.update((previous_digest, accepted_digest))
    expected_receipt_count = 2 * len(expected_waves)
    if len(selected_digests) != expected_receipt_count:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion predecessor chain is not exact"
        )
    selected = [
        (key, receipt)
        for key, receipt in validated
        if _receipt_digest_from_key(key) in selected_digests
    ]
    if len(selected) != expected_receipt_count:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored promotion predecessor receipt selection is incomplete"
        )
    receipt_digests = [_receipt_digest_from_key(key) for key, _receipt in selected]
    terminal_scope = terminal_receipt["scope"]
    proof = {
        "schema_version": 1,
        "rollout_id": rollout_id,
        "source_wave_id": source_wave_id,
        "source_cohort_sha256": terminal_scope["cohort_sha256"],
        "runtime_sha256": terminal_scope["runtime_sha256"],
        "classifier_sha256": terminal_scope["classifier_sha256"],
        "release": dict(terminal_receipt["release"]),
        "receipt_sha256s": receipt_digests,
        "terminal_receipt_sha256": terminal_digest,
    }
    return {
        **proof,
        "promotion_acceptance_sha256": _sha256_json(proof),
        "terminal_runs": [_terminal_run_witness(receipt) for _key, receipt in selected],
    }


def _prior_wave_receipt(
    *,
    current: Mapping[str, Any],
    records: Sequence[tuple[str, Mapping[str, Any]]],
) -> Optional[str]:
    wave_id = current["scope"]["wave_id"]
    wave_index = WAVE_ORDER.index(wave_id)
    if wave_index == 0:
        return None
    required_wave = WAVE_ORDER[wave_index - 1]
    by_digest = _record_index(records)
    candidates: list[tuple[str, Mapping[str, Any]]] = []
    for key, receipt in records:
        if (
            receipt["scope"]["wave_id"] == required_wave
            and receipt["wave_accepted"] is True
            and current["scope"]["promotion_terminal_receipt_sha256"]
            == _receipt_digest_from_key(key)
            and _same_rollout_basis(current, receipt)
            and current["scope"]["selected_scopes"][
                : receipt["scope"]["selected_scope_count"]
            ]
            == receipt["scope"]["selected_scopes"]
            and _parse_utc_iso(receipt["logical_date"], label="prior wave logical date")
            < _parse_utc_iso(current["logical_date"], label="current wave logical date")
        ):
            _validate_accepted_chain(
                key=key,
                receipt=receipt,
                by_digest=by_digest,
            )
            candidates.append((key, receipt))
    if not candidates:
        raise WhoScoredRolloutAcceptanceError(
            f"WhoScored rollout {wave_id} requires accepted {required_wave} evidence"
        )
    return _receipt_digest_from_key(candidates[-1][0])


def normalized_run_evidence(
    *,
    scope_plan: Mapping[str, Any],
    runtime_preflight: Mapping[str, Any],
    catalog_dq: Mapping[str, Any],
    profile_dq: Mapping[str, Any],
    traffic_dq: Mapping[str, Any],
    daily_slo: Mapping[str, Any],
    alert_preflight: Mapping[str, Any],
    scope_dq: Iterable[Mapping[str, Any]],
    terminal_task_states: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Public pure replay of every authority and green-evidence XCom/TI input."""

    scope = _scope_identity(scope_plan)
    release = _release_identity(runtime_preflight)
    if scope["runtime_sha256"] != release["code_tree_sha256"]:
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout runtime pin differs from admitted release"
        )
    evidence = _validate_green_evidence(
        scope=scope,
        runtime_preflight=runtime_preflight,
        catalog_dq=catalog_dq,
        profile_dq=profile_dq,
        traffic_dq=traffic_dq,
        daily_slo=daily_slo,
        alert_preflight=alert_preflight,
        scope_dq=scope_dq,
        terminal_task_states=terminal_task_states,
    )
    return {"scope": scope, "release": release, "evidence": evidence}


def run_evidence_sha256(
    *,
    scope_plan: Mapping[str, Any],
    runtime_preflight: Mapping[str, Any],
    catalog_dq: Mapping[str, Any],
    profile_dq: Mapping[str, Any],
    traffic_dq: Mapping[str, Any],
    daily_slo: Mapping[str, Any],
    alert_preflight: Mapping[str, Any],
    scope_dq: Iterable[Mapping[str, Any]],
    terminal_task_states: Iterable[Mapping[str, Any]],
) -> str:
    return _sha256_json(
        normalized_run_evidence(
            scope_plan=scope_plan,
            runtime_preflight=runtime_preflight,
            catalog_dq=catalog_dq,
            profile_dq=profile_dq,
            traffic_dq=traffic_dq,
            daily_slo=daily_slo,
            alert_preflight=alert_preflight,
            scope_dq=scope_dq,
            terminal_task_states=terminal_task_states,
        )
    )


def build_success_receipt(
    *,
    run_id: str,
    logical_date: datetime,
    scope_plan: Mapping[str, Any],
    runtime_preflight: Mapping[str, Any],
    catalog_dq: Mapping[str, Any],
    profile_dq: Mapping[str, Any],
    traffic_dq: Mapping[str, Any],
    daily_slo: Mapping[str, Any],
    alert_preflight: Mapping[str, Any],
    scope_dq: Iterable[Mapping[str, Any]],
    terminal_task_states: Iterable[Mapping[str, Any]],
    previous_terminal_run: Optional[Mapping[str, Any]],
    existing_records: Iterable[tuple[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    """Build the deterministic receipt for one fully green scheduled run."""

    if not isinstance(run_id, str) or not run_id.startswith("scheduled__"):
        raise WhoScoredRolloutAcceptanceError(
            "WhoScored rollout receipt requires a scheduled run id"
        )
    logical_iso = _utc_iso(logical_date, label="logical date")
    normalized = normalized_run_evidence(
        scope_plan=scope_plan,
        runtime_preflight=runtime_preflight,
        catalog_dq=catalog_dq,
        profile_dq=profile_dq,
        traffic_dq=traffic_dq,
        daily_slo=daily_slo,
        alert_preflight=alert_preflight,
        scope_dq=scope_dq,
        terminal_task_states=terminal_task_states,
    )
    scope = normalized["scope"]
    release = normalized["release"]
    evidence = normalized["evidence"]
    records = _materialize_validated_receipts(existing_records)
    receipt: dict[str, Any] = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "receipt_type": "whoscored-rollout-run-success",
        "dag_id": "dag_ingest_whoscored",
        "run_id": run_id,
        "logical_date": logical_iso,
        "conf_sha256": _sha256_json({}),
        "scope": scope,
        "release": release,
        "evidence": evidence,
        "identity_sha256": "",
        "previous_scheduled_run": None,
        "previous_run_receipt_sha256": None,
        "prior_wave_receipt_sha256": None,
        "consecutive_successes": 1,
        "wave_accepted": False,
    }
    receipt["identity_sha256"] = _sha256_json(_identity_payload(receipt))
    receipt["prior_wave_receipt_sha256"] = _prior_wave_receipt(
        current=receipt,
        records=records,
    )

    previous_receipt: Optional[Mapping[str, Any]] = None
    previous_receipt_key: Optional[str] = None
    previous_evidence: Optional[dict[str, Any]] = None
    if previous_terminal_run is not None:
        previous_run_id = str(previous_terminal_run.get("run_id") or "")
        previous_state = _run_type(previous_terminal_run.get("state"))
        previous_logical = previous_terminal_run.get("logical_date")
        if not isinstance(previous_logical, datetime):
            raise WhoScoredRolloutAcceptanceError(
                "invalid previous scheduled DagRun logical date"
            )
        previous_logical_iso = _utc_iso(
            previous_logical, label="previous scheduled logical date"
        )
        previous_evidence = {
            "run_id": previous_run_id,
            "state": previous_state,
            "logical_date": previous_logical_iso,
        }
        for candidate_key, candidate in records:
            if candidate["run_id"] == previous_run_id:
                if previous_receipt is not None:
                    raise WhoScoredRolloutAcceptanceError(
                        "duplicate WhoScored rollout receipt for one DagRun"
                    )
                previous_receipt = candidate
                previous_receipt_key = candidate_key
        current_day = logical_date.astimezone(timezone.utc).date()
        previous_day = previous_logical.astimezone(timezone.utc).date()
        consecutive_day = (current_day - previous_day).days == 1
        if (
            previous_state == "success"
            and consecutive_day
            and previous_receipt is not None
            and previous_receipt["identity_sha256"] == receipt["identity_sha256"]
        ):
            receipt["consecutive_successes"] = min(
                REQUIRED_CONSECUTIVE_SUCCESSES,
                previous_receipt["consecutive_successes"] + 1,
            )
            assert previous_receipt_key is not None
            receipt["previous_run_receipt_sha256"] = _receipt_digest_from_key(
                previous_receipt_key
            )
    receipt["previous_scheduled_run"] = previous_evidence
    receipt["wave_accepted"] = (
        receipt["consecutive_successes"] == REQUIRED_CONSECUTIVE_SUCCESSES
    )
    return receipt


def record_success_receipt(
    *,
    ops_store: Any,
    run_id: str,
    logical_date: datetime,
    scope_plan: Mapping[str, Any],
    runtime_preflight: Mapping[str, Any],
    catalog_dq: Mapping[str, Any],
    profile_dq: Mapping[str, Any],
    traffic_dq: Mapping[str, Any],
    daily_slo: Mapping[str, Any],
    alert_preflight: Mapping[str, Any],
    scope_dq: Iterable[Mapping[str, Any]],
    terminal_task_states: Iterable[Mapping[str, Any]],
    previous_terminal_run: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Persist and read back one content-addressed run receipt."""

    rollout_id = str(scope_plan.get("rollout_id") or "")
    prefix = receipts_prefix(rollout_id)
    records = list(ops_store.iter_content_addressed_json(prefix))
    receipt = build_success_receipt(
        run_id=run_id,
        logical_date=logical_date,
        scope_plan=scope_plan,
        runtime_preflight=runtime_preflight,
        catalog_dq=catalog_dq,
        profile_dq=profile_dq,
        traffic_dq=traffic_dq,
        daily_slo=daily_slo,
        alert_preflight=alert_preflight,
        scope_dq=scope_dq,
        terminal_task_states=terminal_task_states,
        previous_terminal_run=previous_terminal_run,
        existing_records=records,
    )
    candidate_key = f"{prefix}/{_sha256_json(receipt)}.json"
    receipt = _validate_receipt(candidate_key, receipt)
    existing_for_run = [
        value
        for _key, value in _materialize_validated_receipts(records)
        if value["run_id"] == run_id
    ]
    if existing_for_run:
        if len(existing_for_run) != 1 or existing_for_run[0] != receipt:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored rollout DagRun receipt is immutable and conflicts"
            )
        artifact = {
            "key": next(key for key, value in records if value == receipt),
            "sha256": _sha256_json(receipt),
            "bytes": len(_canonical_json_bytes(receipt)),
        }
        artifact["uri"] = ops_store.object_uri(artifact["key"])
    else:
        artifact = ops_store.put_content_addressed_json(prefix, receipt)
        persisted = ops_store.read_content_addressed_json(
            artifact["key"],
            expected_sha256=artifact["sha256"],
            expected_bytes=artifact["bytes"],
        )
        persisted = _validate_receipt(artifact["key"], persisted)
        if persisted != receipt:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored rollout receipt failed durable read-back"
            )
    return {
        "status": "accepted" if receipt["wave_accepted"] else "pending",
        "rollout_id": receipt["scope"]["rollout_id"],
        "wave_id": receipt["scope"]["wave_id"],
        "consecutive_successes": receipt["consecutive_successes"],
        "required_consecutive_successes": REQUIRED_CONSECUTIVE_SUCCESSES,
        "receipt": artifact,
    }


def rollout_acceptance_status(
    records: Iterable[tuple[str, Mapping[str, Any]]], *, rollout_id: str
) -> dict[str, Any]:
    """Replay the immutable ledger and report whether every wave is accepted."""

    receipts_prefix(rollout_id)
    validated = _materialize_validated_receipts(records)
    seen_run_ids: set[str] = set()
    by_digest: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for key, receipt in validated:
        if receipt["scope"]["rollout_id"] != rollout_id:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored rollout ledger contains a foreign rollout receipt"
            )
        if receipt["run_id"] in seen_run_ids:
            raise WhoScoredRolloutAcceptanceError(
                "WhoScored rollout ledger contains duplicate DagRun receipts"
            )
        seen_run_ids.add(receipt["run_id"])
        by_digest[_receipt_digest_from_key(key)] = (key, receipt)
    accepted: dict[str, tuple[str, Mapping[str, Any]]] = {}
    complete_chains: list[dict[str, tuple[str, Mapping[str, Any]]]] = []
    for key, receipt in validated:
        if receipt["wave_accepted"] is not True:
            continue
        chain = _validate_accepted_chain(
            key=key,
            receipt=receipt,
            by_digest=by_digest,
        )
        accepted.update(chain)
        if receipt["scope"]["wave_id"] == WAVE_ORDER[-1]:
            complete_chains.append(chain)
    if complete_chains:
        accepted = complete_chains[-1]
    missing = [wave for wave in WAVE_ORDER if wave not in accepted]
    accepted_release: Optional[dict[str, str]] = None
    accepted_catalog: Optional[dict[str, Any]] = None
    accepted_authority: Optional[dict[str, Any]] = None
    final_wave_receipt_sha256: Optional[str] = None
    terminal_runs: list[dict[str, Any]] = []
    if not missing:
        final_key, final_receipt = accepted[WAVE_ORDER[-1]]
        accepted_release = dict(final_receipt["release"])
        accepted_catalog = {
            "active_scope_count": final_receipt["scope"]["catalog_active_scope_count"],
            "active_scopes_sha256": final_receipt["scope"][
                "catalog_active_scopes_sha256"
            ],
        }
        accepted_authority = {
            field: final_receipt["scope"][field]
            for field in (
                "rollout_id",
                "wave_id",
                "max_scopes",
                "require_full_active",
                "cohort_sha256",
                "ranked_scope_ids_sha256",
                "runtime_sha256",
                "classifier_sha256",
                "promotion_acceptance_sha256",
                "promotion_terminal_receipt_sha256",
            )
        }
        final_wave_receipt_sha256 = _receipt_digest_from_key(final_key)
        witness_by_run: dict[str, dict[str, Any]] = {}
        for wave in WAVE_ORDER:
            _key, receipt = accepted[wave]
            previous_record = by_digest[receipt["previous_run_receipt_sha256"]]
            for run_receipt in (previous_record[1], receipt):
                witness_by_run[run_receipt["run_id"]] = _terminal_run_witness(
                    run_receipt
                )
        terminal_runs = sorted(
            witness_by_run.values(), key=lambda item: item["logical_date"]
        )
    return {
        "schema_version": 1,
        "status": "accepted" if not missing else "pending",
        "rollout_id": rollout_id,
        "accepted_waves": [wave for wave in WAVE_ORDER if wave in accepted],
        "missing_waves": missing,
        "release": accepted_release,
        "catalog": accepted_catalog,
        "authority": accepted_authority,
        "final_wave_receipt_sha256": final_wave_receipt_sha256,
        "terminal_runs": terminal_runs,
    }
