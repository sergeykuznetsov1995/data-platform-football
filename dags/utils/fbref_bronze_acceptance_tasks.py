"""Strict task boundaries for the manual FBref Bronze acceptance DAGs.

The acceptance DAG deliberately uses an explicit, evidence-backed cohort.
It must never fall through to the crawler's ordinary due-frontier selection:
doing so would make the paid sample depend on scheduler timing and could grow
the sample after discovery.  Selection is pure and deterministic here; the
control store owns the atomic cohort freeze and all durable evidence.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any


logger = logging.getLogger(__name__)

ACCEPTANCE_REQUEST_LIMIT = 100
ACCEPTANCE_BYTE_LIMIT_MB = 50
ACCEPTANCE_SHARD_SIZE = 25
ACCEPTANCE_RESERVATION_MB = 3
ACCEPTANCE_MAX_BATCHES = 1
# The DAG itself times out after three hours.  A four-hour lock preserves the
# writer fence for the whole run while bounding a hard-kill orphan to hours,
# not the eight-day publication TTL used across downstream transforms.
ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS = 4 * 60 * 60

ACCEPTANCE_PAGE_KINDS = (
    "competition_index",
    "competition",
    "season",
    "season_stats",
    "schedule",
    "standings",
    "squad",
    "player",
    "matchlog",
    "match",
)
SEASON_STAT_ROUTES = (
    "standard",
    "shooting",
    "playingtime",
    "misc",
    "keepers",
)
EXPLICIT_EMPTY_STATES = frozenset(
    {"empty", "restricted", "not_applicable"}
)
MATCH_TYPED_DATASETS = frozenset(
    {
        "typed:shot_events",
        "typed:match_events",
        "typed:lineups",
        "typed:match_team_stats",
        "typed:match_managers",
        "typed:match_officials",
        "typed:match_keeper_stats",
        "typed:match_player_stats",
    }
)
SEASON_ROUTE_TYPED_DATASETS = {
    "standard": frozenset({"typed:player_stats", "typed:team_stats"}),
    "shooting": frozenset(
        {"typed:player_shooting", "typed:team_shooting"}
    ),
    "playingtime": frozenset(
        {"typed:player_playingtime", "typed:team_playingtime"}
    ),
    "misc": frozenset({"typed:player_misc", "typed:team_misc"}),
    "keepers": frozenset({"typed:keeper_keeper"}),
}

_CURRENT_SIMPLE_SLOTS = (
    ("competition_index", "competition_index"),
    ("competition", "competition"),
    ("season", "season"),
    ("schedule", "schedule"),
    ("standings", "standings"),
    ("squad", "squad"),
    ("matchlog", "matchlog"),
)
_HISTORY_SIMPLE_SLOTS = tuple(
    item
    for item in _CURRENT_SIMPLE_SLOTS
    if item[0] not in {"competition_index", "competition"}
)
_EVIDENCE_SLOTS = (
    ("player_populated", "player", "populated_player"),
    ("player_empty", "player", "empty_player"),
    ("match_full", "match", "full_match"),
    ("match_sparse", "match", "sparse_match"),
)


class FBrefAcceptanceError(RuntimeError):
    """The acceptance contract cannot be proven from durable evidence."""


def _normalized_scope(scope: object) -> str:
    value = str(scope or "").strip().casefold()
    if value not in {"current", "history"}:
        raise ValueError("FBref acceptance scope must be current or history")
    return value


def _run_type(scope: object) -> str:
    return "current" if _normalized_scope(scope) == "current" else "backfill"


def _control_run_id(*, airflow_run_id: str, dag_id: str) -> str:
    from scrapers.fbref.control import make_control_run_id

    return make_control_run_id(airflow_run_id, dag_id=dag_id)


def _control_store():
    from scrapers.fbref.control import ControlStore

    return ControlStore.from_env()


def _pipeline():
    from scrapers.fbref.pipeline import FBrefPipeline

    return FBrefPipeline.from_env()


def _acceptance_settings(scope: object):
    from scrapers.fbref.pipeline import PipelineSettings

    return PipelineSettings.acceptance(scope=_normalized_scope(scope))


def _required_text(row: Mapping[str, Any], name: str) -> str:
    value = str(row.get(name) or "").strip()
    if not value:
        raise FBrefAcceptanceError(
            f"FBref acceptance candidate is missing {name}"
        )
    return value


def _normalize_candidate(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise FBrefAcceptanceError(
            "FBref acceptance candidate must be a mapping"
        )
    source_ids = raw.get("source_ids")
    if not isinstance(source_ids, Mapping):
        raise FBrefAcceptanceError(
            "FBref acceptance candidate source_ids are missing"
        )
    page_kind = _required_text(raw, "page_kind").casefold()
    if page_kind not in ACCEPTANCE_PAGE_KINDS:
        raise FBrefAcceptanceError(
            f"Unsupported FBref acceptance page kind: {page_kind}"
        )
    state = _required_text(raw, "state").casefold()
    if state not in {"queued", "retry", "fetched"}:
        raise FBrefAcceptanceError(
            f"Acceptance target is not crawlable: state={state}"
        )
    gender = (
        None
        if raw.get("gender") is None
        else str(raw.get("gender")).strip().casefold()
    )
    if page_kind != "competition_index" and gender != "male":
        raise FBrefAcceptanceError(
            "FBref acceptance candidates must resolve to male scope"
        )
    evidence_class = (
        None
        if raw.get("evidence_class") is None
        else str(raw.get("evidence_class")).strip().casefold()
    )
    return {
        "target_id": _required_text(raw, "target_id"),
        "page_kind": page_kind,
        "canonical_url": _required_text(raw, "canonical_url"),
        "source_ids": {
            str(key): str(value)
            for key, value in sorted(source_ids.items())
            if value is not None and str(value).strip()
        },
        "refresh_policy": _required_text(raw, "refresh_policy").casefold(),
        "state": state,
        "gender": gender,
        "competition_id": (
            None
            if raw.get("competition_id") is None
            else str(raw.get("competition_id")).strip() or None
        ),
        "season_id": (
            None
            if raw.get("season_id") is None
            else str(raw.get("season_id")).strip() or None
        ),
        "is_current": raw.get("is_current"),
        "evidence_class": evidence_class,
    }


def _candidate_slot(candidate: Mapping[str, Any]) -> str | None:
    page_kind = candidate["page_kind"]
    if page_kind == "season_stats":
        route = str(candidate["source_ids"].get("stat_route") or "").casefold()
        if route in SEASON_STAT_ROUTES:
            return f"season_stats_{route}"
        return None
    for slot, required_kind in _CURRENT_SIMPLE_SLOTS:
        if page_kind == required_kind:
            return slot
    evidence = candidate.get("evidence_class")
    for slot, required_kind, required_evidence in _EVIDENCE_SLOTS:
        if page_kind == required_kind and evidence == required_evidence:
            return slot
    return None


def _required_slots(scope: str) -> tuple[str, ...]:
    simple = (
        _CURRENT_SIMPLE_SLOTS
        if scope == "current"
        else _HISTORY_SIMPLE_SLOTS
    )
    return (
        *(slot for slot, _ in simple),
        *(f"season_stats_{route}" for route in SEASON_STAT_ROUTES),
        *(slot for slot, _, _ in _EVIDENCE_SLOTS),
    )


def _select_slots(
    candidates: Sequence[Mapping[str, Any]], *, scope: str
) -> list[dict[str, Any]]:
    by_slot: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        slot = _candidate_slot(candidate)
        if slot is not None:
            by_slot[slot].append(dict(candidate))
    for values in by_slot.values():
        values.sort(key=lambda item: item["target_id"])

    missing = [slot for slot in _required_slots(scope) if not by_slot[slot]]
    if missing:
        raise FBrefAcceptanceError(
            "FBref acceptance evidence is incomplete: missing="
            + ",".join(missing)
        )

    selected = []
    selected_ids = set()
    for slot in _required_slots(scope):
        candidate = next(
            (
                item
                for item in by_slot[slot]
                if item["target_id"] not in selected_ids
            ),
            None,
        )
        if candidate is None:
            raise FBrefAcceptanceError(
                f"No distinct FBref target is available for slot {slot}"
            )
        selected_ids.add(candidate["target_id"])
        selected.append({"slot": slot, **candidate})
    return selected


def select_acceptance_cohort(
    raw_candidates: Sequence[Mapping[str, Any]], *, scope: object
) -> dict[str, Any]:
    """Return one deterministic, representative cohort or fail closed."""

    normalized_scope = _normalized_scope(scope)
    candidates = [_normalize_candidate(row) for row in raw_candidates]
    target_ids = [row["target_id"] for row in candidates]
    if len(target_ids) != len(set(target_ids)):
        raise FBrefAcceptanceError(
            "FBref acceptance candidates contain duplicate target IDs"
        )

    if normalized_scope == "current":
        invalid_current = [
            row["target_id"]
            for row in candidates
            if row["season_id"] is not None and row["is_current"] is not True
        ]
        if invalid_current:
            raise FBrefAcceptanceError(
                "Current acceptance candidates contain non-current seasons"
            )
        selected = _select_slots(candidates, scope=normalized_scope)
        season = None
    else:
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in candidates:
            competition_id = row["competition_id"]
            season_id = row["season_id"]
            if (
                competition_id
                and season_id
                and row["is_current"] is False
            ):
                groups[(competition_id, season_id)].append(row)
        selected = []
        season = None
        failures = []
        for key in sorted(groups):
            try:
                selected = _select_slots(groups[key], scope=normalized_scope)
            except FBrefAcceptanceError as exc:
                failures.append(f"{key[0]}/{key[1]}: {exc}")
                continue
            season = {"competition_id": key[0], "season_id": key[1]}
            break
        if not selected:
            detail = "; ".join(failures[:5]) or "no historical season groups"
            raise FBrefAcceptanceError(
                "No one male historical season has complete acceptance "
                f"evidence: {detail}"
            )

    if not selected or len(selected) > ACCEPTANCE_SHARD_SIZE:
        raise FBrefAcceptanceError(
            "FBref acceptance cohort must contain between 1 and 25 targets"
        )
    target_ids = [item["target_id"] for item in selected]
    cohort_hash = hashlib.sha256(
        json.dumps(
            target_ids,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    return {
        "scope": normalized_scope,
        "cohort_hash": cohort_hash,
        "cohort_size": len(selected),
        "season": season,
        "target_ids": target_ids,
        "members": selected,
    }


def validate_fbref_acceptance_readiness(*, scope: object) -> dict[str, Any]:
    """Validate the fixed live profile before creating a control run."""

    from utils.fbref_pipeline_tasks import validate_fbref_production_readiness

    normalized_scope = _normalized_scope(scope)
    result = validate_fbref_production_readiness(
        run_type=_run_type(normalized_scope),
        request_limit=ACCEPTANCE_REQUEST_LIMIT,
        byte_limit_mb=ACCEPTANCE_BYTE_LIMIT_MB,
        shard_size=ACCEPTANCE_SHARD_SIZE,
    )
    return {**result, "acceptance_scope": normalized_scope}


def validate_fbref_acceptance_replay_readiness() -> dict[str, Any]:
    """Validate replay dependencies using read-only probes exclusively."""

    import os

    from utils.alerts import validate_alert_environment
    from utils.fbref_pipeline_tasks import validate_fbref_runtime_limits

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.fbref.raw_store import RawPageStore
    from scrapers.fbref.readiness import (
        check_raw_store_read_access,
        check_trino_read_access,
        validate_raw_store_uri,
    )

    limits = validate_fbref_runtime_limits(
        run_type="replay",
        request_limit=0,
        byte_limit_mb=0,
        shard_size=ACCEPTANCE_SHARD_SIZE,
    )
    alert = validate_alert_environment("prod")
    raw_uri = validate_raw_store_uri(os.environ.get("FBREF_RAW_STORE_URI"))
    migrations = _control_store().validate_migrations()
    raw_health = check_raw_store_read_access(RawPageStore.from_uri(raw_uri))
    trino_health = check_trino_read_access(TrinoTableManager())
    return {
        **alert,
        **limits,
        "execution_mode": "acceptance_replay_nonpublishing",
        "publication_eligible": False,
        "dependencies": {
            "control_migrations": migrations,
            "raw_store": raw_health,
            "trino": trino_health,
            "camoufox": {"status": "not_required"},
            "proxy_meter": {"status": "not_required"},
        },
    }


def acquire_fbref_acceptance_publication_lock(
    *, airflow_run_id: str, dag_id: str
) -> dict[str, Any]:
    """Acquire the global writer fence with a crash-bounded acceptance TTL."""

    from utils.fbref_pipeline_tasks import acquire_fbref_publication_lock

    return acquire_fbref_publication_lock(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        ttl_seconds=ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS,
    )


def initialize_fbref_acceptance_run(
    *, airflow_run_id: str, dag_id: str, scope: object
) -> str:
    """Create a durable run that is physically ineligible for publication."""

    normalized_scope = _normalized_scope(scope)
    return _pipeline().initialize_acceptance_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        settings=_acceptance_settings(normalized_scope),
    )


def initialize_fbref_acceptance_replay_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    source_control_run_id: object,
) -> str:
    """Create a zero-budget replay run with nonpublication evidence."""

    from scrapers.fbref.pipeline import PipelineSettings

    source = str(source_control_run_id or "").strip()
    if not source:
        raise ValueError("Replay requires source_control_run_id")
    return _pipeline().initialize_acceptance_replay_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        source_control_run_id=source,
        settings=PipelineSettings.acceptance_replay(
            shard_size=ACCEPTANCE_SHARD_SIZE
        ),
    )


def prepare_fbref_acceptance_cohort(
    *, airflow_run_id: str, dag_id: str, scope: object
) -> dict[str, Any]:
    """Select and atomically freeze the exact paid acceptance cohort."""

    normalized_scope = _normalized_scope(scope)
    control = _control_store()
    candidates = control.list_acceptance_candidates(
        scope=normalized_scope, limit=1000
    )
    cohort = select_acceptance_cohort(candidates, scope=normalized_scope)
    run_id = _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id)
    persisted = _pipeline().seed_acceptance_cohort(
        run_id=run_id,
        target_ids=cohort["target_ids"],
        settings=_acceptance_settings(normalized_scope),
        required_page_kinds=tuple(
            kind
            for kind in ACCEPTANCE_PAGE_KINDS
            if normalized_scope == "current"
            or kind not in {"competition_index", "competition"}
        ),
        required_routes=SEASON_STAT_ROUTES,
        coverage_slots={
            item["slot"]: item["target_id"] for item in cohort["members"]
        },
    )
    if (
        list(persisted.get("target_ids") or ()) != cohort["target_ids"]
        or int(persisted.get("cohort_size") or 0) != cohort["cohort_size"]
        or str(persisted.get("cohort_sha256") or "")
        != cohort["cohort_hash"]
    ):
        raise FBrefAcceptanceError(
            "Frozen FBref acceptance cohort differs from selected evidence"
        )
    logger.info(
        "FBref acceptance cohort frozen: scope=%s size=%s sha256=%s",
        normalized_scope,
        cohort["cohort_size"],
        cohort["cohort_hash"],
    )
    return cohort


def run_fbref_acceptance_live_wave(
    *, airflow_run_id: str, dag_id: str, scope: object
) -> dict[str, Any]:
    """Fetch exactly the frozen shard; never admit a second due cohort."""

    from utils.fbref_pipeline_tasks import run_fbref_live_waves

    normalized_scope = _normalized_scope(scope)
    return run_fbref_live_waves(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        worker_id=f"bronze-acceptance:{normalized_scope}:{airflow_run_id}",
        page_kinds=ACCEPTANCE_PAGE_KINDS,
        run_type=_run_type(normalized_scope),
        request_limit=ACCEPTANCE_REQUEST_LIMIT,
        byte_limit_mb=ACCEPTANCE_BYTE_LIMIT_MB,
        shard_size=ACCEPTANCE_SHARD_SIZE,
        reservation_mb=ACCEPTANCE_RESERVATION_MB,
        max_batches=ACCEPTANCE_MAX_BATCHES,
    )


def audit_fbref_acceptance_raw(
    *, airflow_run_id: str, dag_id: str, scope: object
) -> dict[str, Any]:
    from utils.fbref_pipeline_tasks import audit_fbref_raw_integrity

    return audit_fbref_raw_integrity(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        run_type=_run_type(scope),
    )


def parse_fbref_acceptance_replay(
    *, airflow_run_id: str, dag_id: str, source_control_run_id: object
) -> dict[str, Any]:
    """Parse one acceptance source cohort with a physically zero budget."""

    from utils.fbref_pipeline_tasks import parse_fbref_wave

    source = str(source_control_run_id or "").strip()
    if not source:
        raise ValueError("Replay requires source_control_run_id")
    return parse_fbref_wave(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        page_kinds=ACCEPTANCE_PAGE_KINDS,
        run_type="replay",
        source_control_run_id=source,
        request_limit=0,
        byte_limit_mb=0,
        shard_size=ACCEPTANCE_SHARD_SIZE,
        reservation_mb=ACCEPTANCE_RESERVATION_MB,
        acceptance_replay=True,
    )


def _validate_expected_cohort(
    evidence: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    target_ids = list(expected.get("target_ids") or ())
    members = list(expected.get("members") or ())
    if not target_ids or len(target_ids) > ACCEPTANCE_SHARD_SIZE:
        raise FBrefAcceptanceError("Expected acceptance cohort is invalid")
    if (
        len(members) != len(target_ids)
        or [str(item.get("target_id") or "") for item in members]
        != target_ids
        or len({str(item.get("slot") or "") for item in members})
        != len(members)
    ):
        raise FBrefAcceptanceError(
            "Expected acceptance coverage slots are invalid"
        )
    expected_hash = str(expected.get("cohort_hash") or "")
    actual_hash = hashlib.sha256(
        json.dumps(
            target_ids,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    if actual_hash != expected_hash:
        raise FBrefAcceptanceError("Acceptance cohort hash is invalid")

    durable_targets = evidence.get("targets")
    if not isinstance(durable_targets, Sequence):
        raise FBrefAcceptanceError("Acceptance target evidence is missing")
    durable_ids = [str(item.get("target_id") or "") for item in durable_targets]
    if durable_ids != target_ids:
        raise FBrefAcceptanceError(
            "Acceptance target evidence differs from the frozen cohort"
        )
    bad_targets = [
        item
        for item in durable_targets
        if str(item.get("status") or "").casefold() != "succeeded"
        or int(item.get("http_status") or 0) not in {200, 304}
        or not item.get("raw_manifest_key")
        or not item.get("content_hash")
    ]
    if bad_targets:
        raise FBrefAcceptanceError(
            f"Acceptance has {len(bad_targets)} unsuccessful target(s)"
        )


def _validate_dataset_evidence(evidence: Mapping[str, Any]) -> None:
    datasets = evidence.get("datasets")
    if not isinstance(datasets, Sequence) or not datasets:
        raise FBrefAcceptanceError("Acceptance dataset evidence is missing")
    failures = []
    for item in datasets:
        availability = str(item.get("availability") or "").casefold()
        statuses = {
            str(item.get(name) or "").casefold()
            for name in (
                "parse_status",
                "persistence_status",
                "validation_status",
            )
        }
        absence_reason = str(
            item.get("absence_reason")
            or item.get("empty_reason")
            or item.get("error_message")
            or ""
        ).strip()
        if statuses - {"succeeded", "skipped"}:
            failures.append(item)
        elif availability in {"unknown", "error", "pending", ""}:
            failures.append(item)
        elif availability in EXPLICIT_EMPTY_STATES and not absence_reason:
            failures.append(item)
    if failures:
        raise FBrefAcceptanceError(
            f"Acceptance has {len(failures)} invalid dataset manifest(s)"
        )


def _safe_dataset(item: Mapping[str, Any]) -> bool:
    return (
        str(item.get("parse_status") or "").casefold()
        in {"succeeded", "skipped"}
        and str(item.get("persistence_status") or "").casefold()
        in {"succeeded", "skipped"}
        and str(item.get("validation_status") or "").casefold()
        in {"succeeded", "skipped"}
        and str(item.get("availability") or "").casefold()
        not in {"", "unknown", "error", "pending"}
    )


def _validate_fresh_coverage(
    evidence: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    """Re-prove semantic slots from this run, not selection-time history."""

    targets = {
        str(item.get("target_id") or ""): item
        for item in evidence.get("targets") or ()
    }
    datasets_by_target: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in evidence.get("datasets") or ():
        datasets_by_target[str(item.get("target_id") or "")].append(item)

    failures = []
    for member in expected.get("members") or ():
        slot = str(member.get("slot") or "")
        target_id = str(member.get("target_id") or "")
        target = targets.get(target_id)
        datasets = datasets_by_target.get(target_id, [])
        if target is None:
            failures.append(f"{slot}:target_missing")
            continue
        page_kind = str(target.get("page_kind") or "").casefold()
        if slot.startswith("season_stats_"):
            expected_route = slot.removeprefix("season_stats_")
            source_ids = target.get("source_ids")
            actual_route = (
                str(source_ids.get("stat_route") or "").casefold()
                if isinstance(source_ids, Mapping)
                else ""
            )
            if page_kind != "season_stats" or actual_route != expected_route:
                failures.append(f"{slot}:route_changed")
            elif not _has_typed_completion(
                datasets,
                SEASON_ROUTE_TYPED_DATASETS[expected_route],
            ):
                failures.append(f"{slot}:typed_datasets_incomplete")
            continue
        if slot in {name for name, _ in _CURRENT_SIMPLE_SLOTS}:
            expected_kind = dict(_CURRENT_SIMPLE_SLOTS)[slot]
            if page_kind != expected_kind:
                failures.append(f"{slot}:page_kind_changed")
            elif slot == "season" and not _has_typed_completion(
                datasets, SEASON_ROUTE_TYPED_DATASETS["standard"]
            ):
                failures.append(f"{slot}:typed_datasets_incomplete")
            elif slot == "schedule" and not _has_typed_completion(
                datasets, frozenset({"typed:schedule"})
            ):
                failures.append(f"{slot}:typed_dataset_incomplete")
            continue
        if slot in {"player_populated", "player_empty"}:
            pages = [item for item in datasets if item.get("dataset") == "__page__"]
            if page_kind != "player" or len(pages) != 1:
                failures.append(f"{slot}:page_evidence_missing")
                continue
            page = pages[0]
            availability = str(page.get("availability") or "").casefold()
            rows = int(page.get("row_count") or 0)
            if slot == "player_populated" and not (
                _safe_dataset(page)
                and availability == "available"
                and rows > 0
            ):
                failures.append(f"{slot}:not_populated")
            elif slot == "player_empty" and not (
                _safe_dataset(page)
                and availability == "empty"
                and rows == 0
                and str(page.get("empty_reason") or "").strip()
            ):
                failures.append(f"{slot}:not_explicitly_empty")
            continue
        if slot in {"match_full", "match_sparse"}:
            typed = {
                str(item.get("dataset") or ""): item
                for item in datasets
                if str(item.get("dataset") or "")
                in MATCH_TYPED_DATASETS | {"typed:__complete__"}
            }
            completion = typed.get("typed:__complete__")
            player_stats = typed.get("typed:match_player_stats")
            if (
                page_kind != "match"
                or set(typed) != MATCH_TYPED_DATASETS | {"typed:__complete__"}
                or completion is None
                or not _safe_dataset(completion)
                or any(
                    not _safe_dataset(typed[name])
                    for name in MATCH_TYPED_DATASETS
                )
                or player_stats is None
            ):
                failures.append(f"{slot}:typed_evidence_incomplete")
                continue
            availability = str(
                player_stats.get("availability") or ""
            ).casefold()
            rows = int(player_stats.get("row_count") or 0)
            if slot == "match_full" and not (
                availability == "available" and rows > 0
            ):
                failures.append(f"{slot}:not_full")
            elif slot == "match_sparse" and not (
                availability in EXPLICIT_EMPTY_STATES
                and rows == 0
                and str(player_stats.get("empty_reason") or "").strip()
            ):
                failures.append(f"{slot}:not_sparse")
            continue
        failures.append(f"{slot}:unknown_slot")
    if failures:
        raise FBrefAcceptanceError(
            "Fresh acceptance coverage failed: " + ",".join(failures)
        )


def _has_typed_completion(
    datasets: Sequence[Mapping[str, Any]], required: frozenset[str]
) -> bool:
    typed = {
        str(item.get("dataset") or ""): item
        for item in datasets
        if str(item.get("dataset") or "").startswith("typed:")
    }
    expected = required | {"typed:__complete__"}
    return expected <= set(typed) and all(
        _safe_dataset(typed[name]) for name in expected
    )


def validate_fbref_acceptance_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    expected_cohort: Mapping[str, Any] | None = None,
    source_control_run_id: object | None = None,
    replay: bool = False,
) -> dict[str, Any]:
    """Apply strict evidence gates, then finish the non-publishing run."""

    run_id = _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id)
    control = _control_store()
    evidence = control.get_acceptance_run_evidence(run_id)
    if not isinstance(evidence, Mapping):
        raise FBrefAcceptanceError("FBref acceptance evidence is missing")

    if replay:
        source = str(source_control_run_id or "").strip()
        if not source:
            raise ValueError("Replay requires source_control_run_id")
        summary = evidence.get("summary")
        if not isinstance(summary, Mapping):
            raise FBrefAcceptanceError("Replay summary evidence is missing")
        traffic = summary.get("traffic_totals")
        if not isinstance(traffic, Mapping):
            raise FBrefAcceptanceError("Replay traffic evidence is missing")
        if (
            not {"requests_used", "bytes_used"} <= set(summary)
            or "network_attempts" not in traffic
            or int(summary.get("request_limit", -1)) != 0
            or int(summary.get("byte_limit", -1)) != 0
            or int(summary.get("requests_used") or 0) != 0
            or int(summary.get("bytes_used") or 0) != 0
            or int(traffic.get("network_attempts") or 0) != 0
        ):
            raise FBrefAcceptanceError("Acceptance replay is not zero-network")
    else:
        if not isinstance(expected_cohort, Mapping):
            raise FBrefAcceptanceError("Expected acceptance cohort is missing")
        _validate_expected_cohort(evidence, expected_cohort)
    _validate_dataset_evidence(evidence)
    if not replay:
        _validate_fresh_coverage(evidence, expected_cohort)

    summary = _pipeline().validate_and_finish(
        run_id,
        replay_source_run_id=(
            str(source_control_run_id).strip()
            if replay and source_control_run_id is not None
            else None
        ),
        publication_eligible=False,
        acceptance=True,
        acceptance_replay=replay,
    )
    return {
        "control_run_id": run_id,
        "source_control_run_id": (
            str(source_control_run_id).strip() if replay else None
        ),
        "cohort_hash": (
            None if replay else expected_cohort.get("cohort_hash")
        ),
        "summary": summary,
        "target_count": len(evidence["targets"]),
        "dataset_manifest_count": len(evidence["datasets"]),
    }


__all__ = [
    "ACCEPTANCE_BYTE_LIMIT_MB",
    "ACCEPTANCE_MAX_BATCHES",
    "ACCEPTANCE_PAGE_KINDS",
    "ACCEPTANCE_PUBLICATION_LOCK_TTL_SECONDS",
    "ACCEPTANCE_REQUEST_LIMIT",
    "ACCEPTANCE_RESERVATION_MB",
    "ACCEPTANCE_SHARD_SIZE",
    "FBrefAcceptanceError",
    "acquire_fbref_acceptance_publication_lock",
    "audit_fbref_acceptance_raw",
    "initialize_fbref_acceptance_replay_run",
    "initialize_fbref_acceptance_run",
    "parse_fbref_acceptance_replay",
    "prepare_fbref_acceptance_cohort",
    "run_fbref_acceptance_live_wave",
    "select_acceptance_cohort",
    "validate_fbref_acceptance_readiness",
    "validate_fbref_acceptance_replay_readiness",
    "validate_fbref_acceptance_run",
]
