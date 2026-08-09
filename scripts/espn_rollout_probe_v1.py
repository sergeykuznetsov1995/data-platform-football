#!/usr/bin/env python3
"""Versioned, strictly observational ESPN rollout health evaluator.

The evaluator receives dependency-injected readers, invokes every reader once,
and emits one result for every rollout invariant even when an earlier reader or
check fails.  It deliberately owns no database, artifact, Airflow, Docker, or
catalog mutation API.  A host observer can adapt its existing read paths to the
``READ_METHODS`` contract and render the returned JSON.

For an offline replay, pass a JSON object containing one key per reader (the
reader name without ``read_``)::

    python scripts/espn_rollout_probe_v1.py --snapshot snapshot.json

Only the report is printed to stdout; the snapshot and platform state are not
modified.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from scrapers.espn.layout import (  # noqa: E402
    COMPACT6,
    LEGACY14,
    LayoutError,
    validate_catalog_layout,
)


UTC = timezone.utc
PROBE_KIND = "espn-rollout-probe-v1"
SCHEMA_VERSION = 1
EXPECTED_METADB_CONTAINER = "espn-airflow-airflow-metadb-1"
EXPECTED_HEALTH_URL = "http://127.0.0.1:8086/health"
EXPECTED_SCOPE_COUNT = 181
FRESHNESS_LIMIT_HOURS = 36
EXPECTED_PARSER_VERSION = "espn-native-parser-v3"
EXPECTED_RUNTIME_VERSION = "espn-native-runtime-v4"
KNOWN_LEAGUES_CUP_SCOPE = "19425:2026"
KNOWN_LEAGUES_CUP_EVENT_IDS = (
    401863559,
    401863560,
    401863562,
    401863563,
    401863564,
)

ESPN_DAG_IDS = (
    "dag_ingest_espn",
    "dag_repair_espn",
    "dag_backfill_espn",
    "dag_replay_espn",
    "dag_discover_espn_registry",
    "dag_monitor_espn",
    "dag_trigger_espn_daily",
)
ARM_ORDER = (
    "dag_ingest_espn",
    "dag_monitor_espn",
    "dag_discover_espn_registry",
    "dag_trigger_espn_daily",
)
ALWAYS_PAUSED_DAGS = frozenset(ESPN_DAG_IDS) - frozenset(ARM_ORDER)
PARENT_DAG_ID = "dag_trigger_espn_daily"
CHILD_DAG_ID = "dag_ingest_espn"
CHILD_RUN_PREFIX = f"espn_daily__{PARENT_DAG_ID}__"
ARM_START_UTC = time(13, 50)
ARM_END_UTC = time(14, 15)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_SCOPE_ID = re.compile(r"[1-9][0-9]*:[1-9][0-9]*")
_RECEIPT_FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "dag_id",
        "run_id",
        "attempt",
        "mode",
        "as_of",
        "logical_date",
        "parent",
        "scope_ids",
        "registry_ref",
        "registry_signature",
        "release",
        "canary_campaign",
        "qualification",
        "admission_ref",
        "plan_index_ref",
        "durable_manifest_ref",
        "published_dq_refs",
        "verdict_ref",
        "health_ref",
        "lease_release_ref",
        "receipt_sha256",
    }
)
_RELEASE_FIELDS = frozenset(
    {
        "release_commit",
        "release_tree_sha256",
        "registry_signature",
        "target_scope_sha256",
        "target_scope_ids",
        "campaign_id",
        "parser_version",
        "runtime_version",
    }
)
_PARENT_ENVELOPE_FIELDS = frozenset(
    {
        "schema",
        "owner_profile",
        "parent_dag_id",
        "parent_task_id",
        "parent_run_id",
        "parent_run_type",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "child_dag_id",
        "child_run_id",
    }
)

READ_METHODS = (
    "read_container",
    "read_ui_health",
    "read_dags",
    "read_parent_child",
    "read_receipt",
    "read_registry",
    "read_target",
    "read_scope_heads",
    "read_dispositions",
    "read_active_leases",
    "read_known_events",
    "read_layout",
)

RESULT_CODES = (
    "runtime.metadb_container",
    "runtime.ui_health",
    "airflow.dag_inventory",
    "airflow.pause_posture",
    "airflow.parent_child",
    "artifact.final_receipt",
    "registry.frozen",
    "target.exact_181",
    "heads.physical_versions",
    "qualification.dispositions",
    "freshness.per_scope",
    "leases.zero_active",
    "events.leagues_cup_five",
    "serving.layout_parity",
)


@dataclass(frozen=True, slots=True)
class _Observation:
    value: Any = None
    error_type: str | None = None


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _json_value(value: Any) -> Any:
    """Return a deterministic JSON-safe diagnostic without leaking objects."""

    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    if isinstance(value, datetime):
        return _utc_iso(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return f"<{type(value).__name__}>"


def _canonical_bytes(value: object, *, newline: bool = False) -> bytes:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if newline:
        encoded += "\n"
    return encoded.encode("utf-8")


def _target_scope_sha256(scope_ids: Sequence[str]) -> str:
    return hashlib.sha256(_canonical_bytes(list(scope_ids))).hexdigest()


def _result(
    code: str,
    status: str,
    summary: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if status not in {"ok", "fail", "unknown"}:
        raise ValueError(f"invalid probe result status {status!r}")
    return {
        "code": code,
        "status": status,
        "severity": "hard",
        "summary": summary,
        "details": _json_value(details or {}),
    }


def _unavailable(code: str, observation: _Observation, subject: str) -> dict[str, Any]:
    details = {}
    if observation.error_type is not None:
        details["reader_error_type"] = observation.error_type
    return _result(code, "unknown", f"{subject} observation is unavailable", details)


def _mapping(observation: _Observation) -> Mapping[str, Any] | None:
    return observation.value if isinstance(observation.value, Mapping) else None


def _sequence(value: object) -> Sequence[Any] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return None


def _scope_ids(value: object) -> tuple[str, ...] | None:
    raw = _sequence(value)
    if raw is None or not all(
        type(item) is str and _SCOPE_ID.fullmatch(item) is not None for item in raw
    ):
        return None
    scopes = tuple(raw)
    if len(set(scopes)) != len(scopes) or tuple(sorted(scopes)) != scopes:
        return None
    return scopes


def _sha(value: object) -> str | None:
    return value if isinstance(value, str) and _SHA256.fullmatch(value) else None


def _read_all(readers: object) -> dict[str, _Observation]:
    observations: dict[str, _Observation] = {}
    for name in READ_METHODS:
        try:
            reader = getattr(readers, name)
            if not callable(reader):
                raise TypeError(f"{name} is not callable")
            observations[name] = _Observation(value=reader())
        except Exception as exc:  # every other reader still runs
            observations[name] = _Observation(error_type=type(exc).__name__)
    return observations


def _check_container(observation: _Observation) -> dict[str, Any]:
    code = "runtime.metadb_container"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "metadatabase container")
    actual = {
        "name": value.get("name"),
        "status": value.get("status"),
        "health": value.get("health"),
    }
    expected = {
        "name": EXPECTED_METADB_CONTAINER,
        "status": "running",
        "health": "healthy",
    }
    if actual != expected:
        return _result(
            code,
            "fail",
            "dedicated ESPN metadatabase container identity or health differs",
            {"expected": expected, "observed": actual},
        )
    return _result(
        code, "ok", "dedicated ESPN metadatabase container is healthy", actual
    )


def _component_status(body: Mapping[str, Any], name: str) -> str | None:
    value = body.get(name)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        return "malformed"
    status = value.get("status")
    if status is None:
        return None
    return status if isinstance(status, str) else "malformed"


def _check_ui_health(observation: _Observation) -> dict[str, Any]:
    code = "runtime.ui_health"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "Airflow UI health")
    body = value.get("body")
    if not isinstance(body, Mapping):
        return _result(code, "unknown", "Airflow health response body is malformed")
    observed = {
        "url": value.get("url"),
        "status_code": value.get("status_code"),
        "metadatabase": _component_status(body, "metadatabase"),
        "scheduler": _component_status(body, "scheduler"),
        "triggerer": _component_status(body, "triggerer"),
        "dag_processor": _component_status(body, "dag_processor"),
    }
    optional_healthy = all(
        observed[name] in {None, "healthy"} for name in ("triggerer", "dag_processor")
    )
    healthy = (
        observed["url"] == EXPECTED_HEALTH_URL
        and type(observed["status_code"]) is int
        and observed["status_code"] == 200
        and observed["metadatabase"] == "healthy"
        and observed["scheduler"] == "healthy"
        and optional_healthy
    )
    if not healthy:
        return _result(
            code,
            "fail",
            "Airflow UI bind, HTTP response, or component health differs",
            {"expected_url": EXPECTED_HEALTH_URL, "observed": observed},
        )
    return _result(
        code, "ok", "Airflow UI and required components are healthy", observed
    )


def _dag_mapping(observation: _Observation) -> Mapping[str, Any] | None:
    value = _mapping(observation)
    if value is None or not all(type(item) is bool for item in value.values()):
        return None
    return value


def _check_dag_inventory(observation: _Observation) -> dict[str, Any]:
    code = "airflow.dag_inventory"
    value = _dag_mapping(observation)
    if value is None:
        return _unavailable(code, observation, "ESPN DAG inventory")
    actual = set(value)
    expected = set(ESPN_DAG_IDS)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        return _result(
            code,
            "fail",
            "isolated ESPN DagBag inventory is not exact",
            {
                "expected_count": len(ESPN_DAG_IDS),
                "observed_count": len(actual),
                "missing": missing,
                "unexpected": unexpected,
            },
        )
    return _result(
        code,
        "ok",
        "isolated ESPN DagBag contains exactly seven DAGs",
        {"dag_ids": list(ESPN_DAG_IDS)},
    )


def _inside_arm_window(observed_at: datetime) -> bool:
    current = observed_at.astimezone(UTC).time().replace(tzinfo=None)
    return ARM_START_UTC <= current < ARM_END_UTC


def _derived_child_run_id(parent_run_id: str) -> str:
    return f"{CHILD_RUN_PREFIX}{parent_run_id}"


def _scheduled_parent_identity(parent_child: Mapping[str, Any]) -> bool:
    run_id = parent_child.get("parent_run_id")
    logical_date = _parse_timestamp(parent_child.get("parent_logical_date"))
    interval_start = _parse_timestamp(parent_child.get("parent_data_interval_start"))
    interval_end = _parse_timestamp(parent_child.get("parent_data_interval_end"))
    if (
        not isinstance(run_id, str)
        or not run_id.startswith("scheduled__")
        or parent_child.get("parent_run_type") != "scheduled"
        or logical_date is None
        or interval_start is None
        or interval_end is None
    ):
        return False
    return (
        run_id == f"scheduled__{logical_date.isoformat()}"
        and logical_date == interval_start
        and interval_end - interval_start == timedelta(days=1)
        and (
            logical_date.hour,
            logical_date.minute,
            logical_date.second,
            logical_date.microsecond,
        )
        == (14, 0, 0, 0)
    )


def _exact_child_is_draining(parent_child: Mapping[str, Any]) -> bool:
    parent_run_id = parent_child.get("parent_run_id")
    return (
        _scheduled_parent_identity(parent_child)
        and isinstance(parent_run_id, str)
        and bool(parent_run_id)
        and parent_child.get("parent_dag_id") == PARENT_DAG_ID
        and parent_child.get("child_dag_id") == CHILD_DAG_ID
        and parent_child.get("child_run_id") == _derived_child_run_id(parent_run_id)
        and parent_child.get("child_state") in {"queued", "running"}
    )


def _check_pause_posture(
    dags_observation: _Observation,
    parent_child_observation: _Observation,
    observed_at: datetime,
) -> dict[str, Any]:
    code = "airflow.pause_posture"
    dags = _dag_mapping(dags_observation)
    if dags is None or set(dags) != set(ESPN_DAG_IDS):
        return _unavailable(code, dags_observation, "exact ESPN pause posture")
    unpaused = tuple(dag_id for dag_id in ESPN_DAG_IDS if dags[dag_id] is False)
    if not unpaused:
        return _result(
            code,
            "ok",
            "all seven ESPN DAGs are paused",
            {"phase": "rest", "unpaused": []},
        )
    if not _inside_arm_window(observed_at):
        return _result(
            code,
            "fail",
            "an ESPN DAG is unpaused outside the UTC arm window",
            {
                "arm_window_utc": "[13:50,14:15)",
                "unpaused": list(unpaused),
            },
        )
    if set(unpaused) & ALWAYS_PAUSED_DAGS:
        return _result(
            code,
            "fail",
            "repair, backfill, and replay must stay paused during daily arming",
            {"unpaused": list(unpaused)},
        )
    arm_unpaused = tuple(dag_id for dag_id in ARM_ORDER if not dags[dag_id])
    legal_prefixes = tuple(ARM_ORDER[:size] for size in range(len(ARM_ORDER) + 1))
    if arm_unpaused not in legal_prefixes:
        return _result(
            code,
            "fail",
            "daily arming DAGs are not an ordered prefix",
            {"arm_order": list(ARM_ORDER), "unpaused": list(arm_unpaused)},
        )
    parent_child = _mapping(parent_child_observation)
    if parent_child is None or type(parent_child.get("parent_created")) is not bool:
        return _unavailable(
            code,
            parent_child_observation,
            "daily parent creation prerequisite",
        )
    if parent_child["parent_created"] is False:
        return _result(
            code,
            "ok",
            "daily DAGs form a legal pre-parent arm prefix",
            {"phase": "arming", "unpaused": list(arm_unpaused)},
        )
    if not dags[PARENT_DAG_ID]:
        return _result(
            code,
            "fail",
            "daily parent must be paused immediately after its run is created",
            {"unpaused": list(arm_unpaused)},
        )
    pre_parent_prefixes = tuple(ARM_ORDER[:size] for size in range(4))
    if arm_unpaused not in pre_parent_prefixes:
        return _result(
            code,
            "fail",
            "post-parent unpaused DAGs are not a legal three-DAG prefix",
            {"unpaused": list(arm_unpaused)},
        )
    if not _exact_child_is_draining(parent_child):
        return _result(
            code,
            "fail",
            "post-parent arm posture lacks the exact draining derived child",
            {
                "parent_run_id": parent_child.get("parent_run_id"),
                "child_run_id": parent_child.get("child_run_id"),
                "child_state": parent_child.get("child_state"),
            },
        )
    return _result(
        code,
        "ok",
        "daily parent is re-paused while its exact child drains",
        {"phase": "child_draining", "unpaused": list(arm_unpaused)},
    )


def _check_parent_child(
    observation: _Observation,
    observed_at: datetime,
) -> dict[str, Any]:
    code = "airflow.parent_child"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "scheduler parent/child identity")
    parent_created = value.get("parent_created")
    if type(parent_created) is not bool:
        return _result(code, "unknown", "scheduler parent creation state is unknown")
    if parent_created is False:
        residual = [
            name
            for name in (
                "parent_run_id",
                "parent_run_type",
                "parent_logical_date",
                "parent_data_interval_start",
                "parent_data_interval_end",
                "parent_state",
                "child_run_id",
                "child_state",
            )
            if value.get(name) is not None
        ]
        if residual:
            return _result(
                code,
                "fail",
                "absent parent claims partial parent/child run state",
                {"unexpected_fields": residual},
            )
        if not _inside_arm_window(observed_at):
            return _result(
                code,
                "unknown",
                "no scheduler parent/child evidence exists outside the arm window",
                {"parent_created": False},
            )
        return _result(
            code,
            "ok",
            "daily parent has not yet been created",
            {"parent_created": False},
        )
    parent_run_id = value.get("parent_run_id")
    if not isinstance(parent_run_id, str) or not parent_run_id:
        return _result(code, "unknown", "created parent run identity is unavailable")
    expected_child_run_id = _derived_child_run_id(parent_run_id)
    observed = {
        "parent_dag_id": value.get("parent_dag_id"),
        "parent_run_id": parent_run_id,
        "parent_run_type": value.get("parent_run_type"),
        "parent_logical_date": value.get("parent_logical_date"),
        "parent_data_interval_start": value.get("parent_data_interval_start"),
        "parent_data_interval_end": value.get("parent_data_interval_end"),
        "parent_state": value.get("parent_state"),
        "child_dag_id": value.get("child_dag_id"),
        "child_run_id": value.get("child_run_id"),
        "child_state": value.get("child_state"),
    }
    if observed["child_state"] is None:
        return _result(
            code,
            "unknown",
            "created parent has no observable derived child state",
            observed,
        )
    identity_ok = (
        _scheduled_parent_identity(value)
        and observed["parent_dag_id"] == PARENT_DAG_ID
        and observed["child_dag_id"] == CHILD_DAG_ID
        and observed["child_run_id"] == expected_child_run_id
    )
    states_ok = observed["parent_state"] in {
        "queued",
        "running",
        "success",
    } and observed["child_state"] in {"queued", "running", "success"}
    if not identity_ok or not states_ok:
        return _result(
            code,
            "fail",
            "scheduler parent/derived-child identity or state is invalid",
            {"expected_child_run_id": expected_child_run_id, "observed": observed},
        )
    return _result(
        code,
        "ok",
        "scheduler parent owns the exact derived child",
        observed,
    )


def _cross_value(observation: _Observation, name: str) -> object:
    value = _mapping(observation)
    return None if value is None else value.get(name)


def _receipt_artifact(observation: _Observation) -> Mapping[str, Any] | None:
    value = _mapping(observation)
    if value is None:
        return None
    if "artifact" not in value:
        return value
    artifact = value.get("artifact")
    return artifact if isinstance(artifact, Mapping) else None


def _receipt_value(observation: _Observation, name: str) -> object:
    artifact = _receipt_artifact(observation)
    return None if artifact is None else artifact.get(name)


def _receipt_completed_at(
    observation: _Observation,
    artifact: Mapping[str, Any],
) -> datetime | None:
    value = _mapping(observation)
    if value is not None and "artifact" in value:
        return _parse_timestamp(value.get("completed_at"))
    return _parse_timestamp(artifact.get("logical_date"))


def _qualification_has_failures(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == "failures" and item != []:
                return True
            if _qualification_has_failures(item):
                return True
        return False
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_qualification_has_failures(item) for item in value)
    return False


def _release_campaign_id(release: Mapping[str, Any]) -> str | None:
    identity = {
        "release_commit": release.get("release_commit"),
        "release_tree_sha256": release.get("release_tree_sha256"),
        "registry_signature": release.get("registry_signature"),
        "target_scope_sha256": release.get("target_scope_sha256"),
    }
    if (
        not isinstance(identity["release_commit"], str)
        or _COMMIT.fullmatch(identity["release_commit"]) is None
        or any(
            _sha(identity[name]) is None
            for name in identity
            if name != "release_commit"
        )
    ):
        return None
    return hashlib.sha256(_canonical_bytes(identity)).hexdigest()


def _check_receipt(
    observation: _Observation,
    parent_child_observation: _Observation,
    registry_observation: _Observation,
    target_observation: _Observation,
    observed_at: datetime,
) -> dict[str, Any]:
    code = "artifact.final_receipt"
    artifact = _receipt_artifact(observation)
    if artifact is None:
        return _unavailable(code, observation, "final success receipt")
    completed_at = _receipt_completed_at(observation, artifact)
    if completed_at is None:
        return _result(code, "unknown", "final receipt completion time is invalid")
    age_hours = (observed_at - completed_at).total_seconds() / 3600
    receipt_scopes = _scope_ids(artifact.get("scope_ids"))
    release = artifact.get("release")
    release_map = release if isinstance(release, Mapping) else {}
    parent = artifact.get("parent")
    parent_map = parent if isinstance(parent, Mapping) else {}
    receipt_parent_identity = {
        "parent_run_id": parent_map.get("parent_run_id"),
        "parent_run_type": parent_map.get("parent_run_type"),
        "parent_logical_date": parent_map.get("logical_date"),
        "parent_data_interval_start": parent_map.get("data_interval_start"),
        "parent_data_interval_end": parent_map.get("data_interval_end"),
    }
    qualification = artifact.get("qualification")
    release_scopes = (
        _scope_ids(release_map.get("target_scope_ids")) if release_map else None
    )
    qualification_scopes = None
    if isinstance(qualification, Mapping):
        raw_qualification_scopes = _sequence(qualification.get("scopes"))
        if raw_qualification_scopes is not None and all(
            isinstance(item, Mapping) for item in raw_qualification_scopes
        ):
            qualification_scopes = _scope_ids(
                [item.get("scope_id") for item in raw_qualification_scopes]
            )
    target = _mapping(target_observation)
    target_scopes = None if target is None else _scope_ids(target.get("scope_ids"))
    parent_child = _mapping(parent_child_observation)
    exact_child = None
    if parent_child is not None and parent_child.get("parent_created") is True:
        exact_child = parent_child.get("child_run_id")
    registry = _mapping(registry_observation)
    frozen_signature = None if registry is None else registry.get("frozen_signature")
    target_sha = None if target is None else target.get("target_scope_sha256")
    unsigned_receipt = {
        key: item for key, item in artifact.items() if key != "receipt_sha256"
    }
    expected_receipt_sha = hashlib.sha256(
        _canonical_bytes(unsigned_receipt, newline=True)
    ).hexdigest()
    release_campaign_id = _release_campaign_id(release_map) if release_map else None
    qualification_count = (
        qualification.get("scope_count") if isinstance(qualification, Mapping) else None
    )
    complete_new = (
        qualification.get("complete_new")
        if isinstance(qualification, Mapping)
        else None
    )
    noop_revalidated = (
        qualification.get("noop_revalidated")
        if isinstance(qualification, Mapping)
        else None
    )
    malformed = (
        set(artifact) != _RECEIPT_FIELDS
        or artifact.get("kind") != "espn-run-success-receipt-v1"
        or artifact.get("schema_version") != 1
        or artifact.get("dag_id") != CHILD_DAG_ID
        or type(artifact.get("attempt")) is not int
        or artifact.get("attempt") < 1
        or _parse_timestamp(artifact.get("logical_date")) is None
        or artifact.get("receipt_sha256") != expected_receipt_sha
        or receipt_scopes is None
        or set(parent_map) != _PARENT_ENVELOPE_FIELDS
        or parent_map.get("schema") != "espn-daily-parent-v2"
        or parent_map.get("owner_profile") != "espn-isolated-v1"
        or parent_map.get("parent_dag_id") != PARENT_DAG_ID
        or parent_map.get("parent_task_id") != "trigger_espn_ingest"
        or parent_map.get("child_dag_id") != CHILD_DAG_ID
        or parent_map.get("child_run_id") != artifact.get("run_id")
        or not _scheduled_parent_identity(receipt_parent_identity)
        or artifact.get("logical_date") != parent_map.get("logical_date")
        or not isinstance(release, Mapping)
        or set(release_map) != _RELEASE_FIELDS
        or release_scopes is None
        or release_map.get("target_scope_sha256")
        != _target_scope_sha256(release_scopes)
        or release_map.get("campaign_id") != release_campaign_id
        or release_map.get("parser_version") != EXPECTED_PARSER_VERSION
        or release_map.get("runtime_version") != EXPECTED_RUNTIME_VERSION
        or not isinstance(qualification, Mapping)
        or type(qualification_count) is not int
        or qualification_count != len(receipt_scopes)
        or type(complete_new) is not int
        or type(noop_revalidated) is not int
        or complete_new < 0
        or noop_revalidated < 0
        or complete_new + noop_revalidated != len(receipt_scopes)
        or qualification_scopes != receipt_scopes
        or _qualification_has_failures(qualification)
    )
    drift = (
        (exact_child is not None and artifact.get("run_id") != exact_child)
        or (
            parent_child is not None
            and parent_child.get("parent_created") is True
            and (
                parent_map.get("parent_run_id") != parent_child.get("parent_run_id")
                or parent_map.get("logical_date")
                != parent_child.get("parent_logical_date")
                or parent_map.get("data_interval_start")
                != parent_child.get("parent_data_interval_start")
                or parent_map.get("data_interval_end")
                != parent_child.get("parent_data_interval_end")
            )
        )
        or (target_scopes is not None and receipt_scopes != target_scopes)
        or release_scopes != receipt_scopes
        or (
            frozen_signature is not None
            and (
                artifact.get("registry_signature") != frozen_signature
                or release_map.get("registry_signature") != frozen_signature
            )
        )
        or (
            target_sha is not None
            and release_map.get("target_scope_sha256") != target_sha
        )
    )
    if malformed or drift or age_hours < 0 or age_hours >= FRESHNESS_LIMIT_HOURS:
        return _result(
            code,
            "fail",
            "final success receipt is stale, malformed, or identity-drifted",
            {
                "age_hours": round(age_hours, 3),
                "kind": artifact.get("kind"),
                "dag_id": artifact.get("dag_id"),
                "run_id": artifact.get("run_id"),
                "scope_count": None if receipt_scopes is None else len(receipt_scopes),
                "checksum_matches": artifact.get("receipt_sha256")
                == expected_receipt_sha,
            },
        )
    return _result(
        code,
        "ok",
        "final success receipt is fresh and identity-bound",
        {"age_hours": round(age_hours, 3), "scope_count": len(receipt_scopes)},
    )


def _check_registry(
    observation: _Observation,
    receipt_observation: _Observation,
) -> dict[str, Any]:
    code = "registry.frozen"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "frozen registry")
    configured_signature = _sha(value.get("configured_signature"))
    frozen_signature = _sha(value.get("frozen_signature"))
    configured_scopes = _scope_ids(value.get("configured_scope_ids"))
    frozen_scopes = _scope_ids(value.get("frozen_scope_ids"))
    target_sha = _sha(value.get("target_scope_sha256"))
    frozen_target_sha = _sha(value.get("frozen_target_scope_sha256"))
    receipt_signature = _receipt_value(receipt_observation, "registry_signature")
    configured_target_sha = (
        _target_scope_sha256(configured_scopes)
        if configured_scopes is not None
        else None
    )
    frozen_derived_sha = (
        _target_scope_sha256(frozen_scopes) if frozen_scopes is not None else None
    )
    valid = (
        configured_signature is not None
        and configured_signature == frozen_signature
        and configured_scopes is not None
        and configured_scopes == frozen_scopes
        and target_sha is not None
        and target_sha == frozen_target_sha
        and target_sha == configured_target_sha
        and frozen_target_sha == frozen_derived_sha
        and (receipt_signature is None or receipt_signature == frozen_signature)
    )
    if not valid:
        return _result(
            code,
            "fail",
            "configured registry differs from the frozen rollout registry",
            {
                "configured_signature": configured_signature,
                "frozen_signature": frozen_signature,
                "configured_scope_count": (
                    None if configured_scopes is None else len(configured_scopes)
                ),
                "frozen_scope_count": None
                if frozen_scopes is None
                else len(frozen_scopes),
                "target_hash_matches": target_sha is not None
                and target_sha == frozen_target_sha,
            },
        )
    return _result(
        code,
        "ok",
        "configured registry exactly matches the frozen rollout target",
        {"registry_signature": frozen_signature, "scope_count": len(frozen_scopes)},
    )


def _check_target(
    observation: _Observation,
    registry_observation: _Observation,
    receipt_observation: _Observation,
) -> dict[str, Any]:
    code = "target.exact_181"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "target scope set")
    scopes = _scope_ids(value.get("scope_ids"))
    target_sha = _sha(value.get("target_scope_sha256"))
    registry_scopes = _scope_ids(_cross_value(registry_observation, "frozen_scope_ids"))
    receipt_scopes = _scope_ids(_receipt_value(receipt_observation, "scope_ids"))
    derived_target_sha = _target_scope_sha256(scopes) if scopes is not None else None
    valid = (
        scopes is not None
        and len(scopes) == EXPECTED_SCOPE_COUNT
        and KNOWN_LEAGUES_CUP_SCOPE in scopes
        and target_sha is not None
        and target_sha == derived_target_sha
        and (registry_scopes is None or scopes == registry_scopes)
        and (receipt_scopes is None or scopes == receipt_scopes)
    )
    if not valid:
        return _result(
            code,
            "fail",
            "rollout target is not the exact identity-bound 181-scope set",
            {
                "expected_count": EXPECTED_SCOPE_COUNT,
                "observed_count": None if scopes is None else len(scopes),
                "unique": scopes is not None,
                "target_scope_sha256": target_sha,
            },
        )
    return _result(
        code,
        "ok",
        "rollout target contains exactly 181 unique scopes",
        {"scope_count": len(scopes), "target_scope_sha256": target_sha},
    )


def _head_rows(observation: _Observation) -> tuple[list[Mapping[str, Any]], bool]:
    raw = _sequence(observation.value)
    if raw is None:
        return [], False
    rows = []
    for item in raw:
        if not isinstance(item, Mapping):
            return [], False
        rows.append(item)
    return rows, True


def _heads_by_scope(
    observation: _Observation,
) -> tuple[dict[str, Mapping[str, Any]], list[str], bool]:
    rows, valid = _head_rows(observation)
    if not valid:
        return {}, [], False
    heads: dict[str, Mapping[str, Any]] = {}
    duplicates = []
    for row in rows:
        scope_id = row.get("scope_id")
        if not isinstance(scope_id, str) or not scope_id:
            return {}, [], False
        if scope_id in heads:
            duplicates.append(scope_id)
        heads[scope_id] = row
    return heads, sorted(set(duplicates)), True


def _check_heads(
    observation: _Observation,
    target_observation: _Observation,
    registry_observation: _Observation,
) -> dict[str, Any]:
    code = "heads.physical_versions"
    heads, duplicates, structurally_valid = _heads_by_scope(observation)
    if not structurally_valid:
        return _unavailable(code, observation, "physical scope heads")
    target_scopes = _scope_ids(_cross_value(target_observation, "scope_ids"))
    registry_signature = _cross_value(registry_observation, "frozen_signature")
    target_sha = _cross_value(target_observation, "target_scope_sha256")
    if target_scopes is None:
        return _result(
            code, "unknown", "exact target is unavailable for head comparison"
        )
    expected = set(target_scopes)
    missing = sorted(expected - set(heads))
    unexpected = sorted(set(heads) - expected)
    mismatched = []
    for scope_id, head in sorted(heads.items()):
        if (
            head.get("state") != "complete"
            or head.get("parser_version") != EXPECTED_PARSER_VERSION
            or head.get("runtime_version") != EXPECTED_RUNTIME_VERSION
            or head.get("physical_verified") is not True
            or head.get("registry_signature") != registry_signature
            or head.get("target_scope_sha256") != target_sha
        ):
            mismatched.append(scope_id)
    if (
        len(heads) != EXPECTED_SCOPE_COUNT
        or duplicates
        or missing
        or unexpected
        or mismatched
    ):
        return _result(
            code,
            "fail",
            "physical scope heads are incomplete, mixed, or not v3/v4",
            {
                "expected_count": EXPECTED_SCOPE_COUNT,
                "observed_count": len(heads),
                "duplicates": duplicates,
                "missing": missing,
                "unexpected": unexpected,
                "mismatched": mismatched,
                "expected_parser_version": EXPECTED_PARSER_VERSION,
                "expected_runtime_version": EXPECTED_RUNTIME_VERSION,
            },
        )
    return _result(
        code,
        "ok",
        "all 181 physical heads are COMPLETE parser-v3/runtime-v4",
        {
            "scope_count": len(heads),
            "parser_version": EXPECTED_PARSER_VERSION,
            "runtime_version": EXPECTED_RUNTIME_VERSION,
        },
    )


def _check_dispositions(
    observation: _Observation,
    target_observation: _Observation,
) -> dict[str, Any]:
    code = "qualification.dispositions"
    qualification = _mapping(observation)
    if qualification is None:
        return _unavailable(code, observation, "qualification dispositions")
    target_scopes = _scope_ids(_cross_value(target_observation, "scope_ids"))
    if target_scopes is None:
        return _result(
            code,
            "unknown",
            "exact target is unavailable for disposition comparison",
        )
    raw_scopes = _sequence(qualification.get("scopes"))
    if raw_scopes is None:
        return _result(code, "unknown", "qualification scope details are unavailable")
    invalid_scopes = []
    invalid_events = []
    observed_scope_ids = []
    outcome_counts = {"complete_new": 0, "noop_revalidated": 0}
    known_event_dispositions = set()
    for index, scope in enumerate(raw_scopes):
        if not isinstance(scope, Mapping):
            invalid_scopes.append(f"index:{index}")
            continue
        scope_id = scope.get("scope_id")
        if not isinstance(scope_id, str):
            invalid_scopes.append(f"index:{index}")
            continue
        observed_scope_ids.append(scope_id)
        outcome = scope.get("outcome")
        if outcome not in outcome_counts:
            invalid_scopes.append(scope_id)
        else:
            outcome_counts[outcome] += 1
        schedule = scope.get("schedule")
        if (
            not isinstance(schedule, Mapping)
            or schedule.get("state") not in {"captured", "valid_empty"}
            or schedule.get("failures") != []
            or (
                schedule.get("state") == "valid_empty"
                and not isinstance(schedule.get("proof"), Mapping)
            )
            or scope.get("failures") != []
        ):
            invalid_scopes.append(scope_id)
        events = _sequence(scope.get("events"))
        if events is None:
            invalid_scopes.append(scope_id)
            continue
        raw_evidence = _sequence(scope.get("raw_evidence"))
        raw_request_ids = []
        if not raw_evidence:
            invalid_scopes.append(scope_id)
        else:
            for raw in raw_evidence:
                if (
                    not isinstance(raw, Mapping)
                    or set(raw)
                    != {
                        "request_id",
                        "endpoint",
                        "event_id",
                        "state",
                        "raw_uri",
                        "raw_sha256",
                    }
                    or not isinstance(raw.get("request_id"), str)
                    or not raw.get("request_id")
                    or not isinstance(raw.get("endpoint"), str)
                    or not raw.get("endpoint")
                    or (
                        raw.get("event_id") is not None
                        and (
                            type(raw.get("event_id")) is not int
                            or raw.get("event_id") <= 0
                        )
                    )
                    or raw.get("state") != "captured"
                    or not isinstance(raw.get("raw_uri"), str)
                    or not raw.get("raw_uri")
                    or _sha(raw.get("raw_sha256")) is None
                ):
                    invalid_scopes.append(scope_id)
                    continue
                raw_request_ids.append(raw["request_id"])
            if len(raw_request_ids) != len(set(raw_request_ids)):
                invalid_scopes.append(scope_id)
        event_ids = []
        for event in events:
            if not isinstance(event, Mapping):
                invalid_events.append(f"{scope_id}:malformed")
                continue
            event_id = event.get("event_id")
            played_final = event.get("played_final")
            summary_required = event.get("summary_required")
            entities = event.get("entities")
            if (
                type(event_id) is not int
                or event_id <= 0
                or type(played_final) is not bool
                or type(summary_required) is not bool
                or event.get("failures") != []
                or not isinstance(entities, Mapping)
                or set(entities) != {"lineup", "matchsheet"}
            ):
                invalid_events.append(f"{scope_id}:{event_id}")
                continue
            event_ids.append(event_id)
            expected_states = (
                {"captured", "valid_empty"}
                if played_final and summary_required
                else {"not_applicable"}
            )
            for entity_name in ("lineup", "matchsheet"):
                entity = entities.get(entity_name)
                if (
                    not isinstance(entity, Mapping)
                    or entity.get("state") not in expected_states
                    or entity.get("failures") != []
                ):
                    invalid_events.append(f"{scope_id}:{event_id}:{entity_name}")
            if scope_id == KNOWN_LEAGUES_CUP_SCOPE:
                known_event_dispositions.add(event_id)
        if len(event_ids) != len(set(event_ids)):
            invalid_scopes.append(scope_id)
    observed_scopes = _scope_ids(observed_scope_ids)
    missing_scopes = (
        list(target_scopes)
        if observed_scopes is None
        else sorted(set(target_scopes) - set(observed_scopes))
    )
    unexpected_scopes = (
        []
        if observed_scopes is None
        else sorted(set(observed_scopes) - set(target_scopes))
    )
    missing_known_events = sorted(
        set(KNOWN_LEAGUES_CUP_EVENT_IDS) - known_event_dispositions
    )
    summary_valid = (
        qualification.get("scope_count") == EXPECTED_SCOPE_COUNT
        and qualification.get("complete_new") == outcome_counts["complete_new"]
        and qualification.get("noop_revalidated") == outcome_counts["noop_revalidated"]
        and sum(outcome_counts.values()) == EXPECTED_SCOPE_COUNT
        and qualification.get("failures") == []
        and not _qualification_has_failures(qualification)
    )
    if (
        not summary_valid
        or observed_scopes != target_scopes
        or invalid_scopes
        or invalid_events
        or missing_scopes
        or unexpected_scopes
        or missing_known_events
    ):
        return _result(
            code,
            "fail",
            "qualification lacks exact terminal scope/event/entity dispositions",
            {
                "scope_count": len(raw_scopes),
                "invalid_scopes": sorted(set(invalid_scopes)),
                "invalid_events": sorted(set(invalid_events)),
                "missing_scopes": missing_scopes,
                "unexpected_scopes": unexpected_scopes,
                "missing_known_events": missing_known_events,
            },
        )
    return _result(
        code,
        "ok",
        "all 181 scopes have terminal schedule and event/entity dispositions",
        {
            "scope_count": len(observed_scopes),
            "complete_new": outcome_counts["complete_new"],
            "noop_revalidated": outcome_counts["noop_revalidated"],
            "known_event_count": len(known_event_dispositions),
        },
    )


def _check_freshness(
    observation: _Observation,
    target_observation: _Observation,
    observed_at: datetime,
) -> dict[str, Any]:
    code = "freshness.per_scope"
    heads, duplicates, structurally_valid = _heads_by_scope(observation)
    if not structurally_valid:
        return _unavailable(code, observation, "per-scope freshness")
    target_scopes = _scope_ids(_cross_value(target_observation, "scope_ids"))
    if target_scopes is None:
        return _result(code, "unknown", "exact target is unavailable for freshness")
    scopes = []
    for scope_id in target_scopes:
        head = heads.get(scope_id)
        if head is None:
            scopes.append(
                {"scope_id": scope_id, "status": "fail", "reason": "missing_head"}
            )
            continue
        completed_at = _parse_timestamp(head.get("last_complete_at"))
        if completed_at is None:
            scopes.append(
                {"scope_id": scope_id, "status": "unknown", "reason": "invalid_time"}
            )
            continue
        age_hours = (observed_at - completed_at).total_seconds() / 3600
        if head.get("state") != "complete":
            status, reason = "fail", "head_not_complete"
        elif age_hours < 0:
            status, reason = "fail", "future_completion"
        elif age_hours >= FRESHNESS_LIMIT_HOURS:
            status, reason = "fail", "stale"
        else:
            status, reason = "ok", "fresh"
        scopes.append(
            {
                "scope_id": scope_id,
                "status": status,
                "reason": reason,
                "age_hours": round(age_hours, 3),
            }
        )
    unexpected = sorted(set(heads) - set(target_scopes))
    statuses = {item["status"] for item in scopes}
    if duplicates or unexpected:
        status = "fail"
    elif "fail" in statuses:
        status = "fail"
    elif "unknown" in statuses:
        status = "unknown"
    else:
        status = "ok"
    summary = {
        "ok": "every target scope has fresh qualified evidence",
        "fail": "one or more target scopes are stale or missing",
        "unknown": "one or more target scope freshness values are unknown",
    }[status]
    return _result(
        code,
        status,
        summary,
        {
            "freshness_limit_hours": FRESHNESS_LIMIT_HOURS,
            "duplicates": duplicates,
            "unexpected": unexpected,
            "scopes": scopes,
        },
    )


def _check_leases(observation: _Observation) -> dict[str, Any]:
    code = "leases.zero_active"
    leases = _sequence(observation.value)
    if leases is None:
        return _unavailable(code, observation, "active ESPN leases")
    if leases:
        identities = []
        for lease in leases:
            if isinstance(lease, Mapping):
                identities.append(
                    {
                        "scope_id": lease.get("scope_id"),
                        "owner_id": lease.get("owner_id"),
                    }
                )
            else:
                identities.append({"malformed": True})
        return _result(
            code,
            "fail",
            "one or more ESPN control leases are active",
            {"active_count": len(leases), "leases": identities},
        )
    return _result(code, "ok", "no ESPN control leases are active", {"active_count": 0})


def _check_known_events(observation: _Observation) -> dict[str, Any]:
    code = "events.leagues_cup_five"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "known Leagues Cup events")
    event_ids = _sequence(value.get("event_ids"))
    actual = tuple(event_ids) if event_ids is not None else ()
    valid_items = all(type(event_id) is int and event_id > 0 for event_id in actual)
    missing = sorted(set(KNOWN_LEAGUES_CUP_EVENT_IDS) - set(actual))
    unexpected = sorted(set(actual) - set(KNOWN_LEAGUES_CUP_EVENT_IDS))
    valid = (
        value.get("scope_id") == KNOWN_LEAGUES_CUP_SCOPE
        and valid_items
        and len(actual) == len(set(actual))
        and not missing
        and not unexpected
    )
    if not valid:
        return _result(
            code,
            "fail",
            "the exact five Leagues Cup events are not present in scope 19425:2026",
            {
                "expected_scope_id": KNOWN_LEAGUES_CUP_SCOPE,
                "observed_scope_id": value.get("scope_id"),
                "missing": missing,
                "unexpected": unexpected,
            },
        )
    return _result(
        code,
        "ok",
        "all five Leagues Cup boundary events are present",
        {"scope_id": KNOWN_LEAGUES_CUP_SCOPE, "event_ids": list(actual)},
    )


def _check_layout(observation: _Observation) -> dict[str, Any]:
    code = "serving.layout_parity"
    value = _mapping(observation)
    if value is None:
        return _unavailable(code, observation, "serving layout")
    mode = value.get("layout_mode")
    if mode not in {LEGACY14, COMPACT6}:
        return _result(
            code,
            "unknown" if mode in {None, ""} else "fail",
            "serving layout mode is absent or unknown",
            {"layout_mode": mode},
        )
    inventory = _sequence(value.get("inventory"))
    if inventory is None:
        return _result(code, "unknown", "serving catalog inventory is unavailable")
    try:
        inventory_result = validate_catalog_layout(mode, inventory)
    except (LayoutError, TypeError, ValueError) as exc:
        return _result(
            code,
            "fail",
            "catalog inventory does not match the declared serving layout",
            {"layout_mode": mode, "inventory_error_type": type(exc).__name__},
        )
    expected_relation = (
        "iceberg.bronze.espn_schedule_current"
        if mode == LEGACY14
        else "iceberg.bronze.espn_schedule"
    )
    relation_ok = (
        value.get("serving_relation") == expected_relation
        and value.get("serving_readable") is True
    )
    parity = value.get("parity")
    parity_ok = True
    if mode == COMPACT6:
        parity_ok = (
            isinstance(parity, Mapping)
            and set(parity) == {"schedule", "lineup", "matchsheet"}
            and all(item is True for item in parity.values())
        )
    if not relation_ok or not parity_ok:
        return _result(
            code,
            "fail",
            "serving relation probe or internal/public parity failed",
            {
                **inventory_result,
                "expected_serving_relation": expected_relation,
                "observed_serving_relation": value.get("serving_relation"),
                "serving_readable": value.get("serving_readable"),
                "parity": parity,
            },
        )
    return _result(
        code,
        "ok",
        f"{mode} inventory and serving probe are exact",
        {
            **inventory_result,
            "serving_relation": expected_relation,
            "parity": parity,
        },
    )


def _safe_check(code: str, check: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        result = check()
    except Exception as exc:
        return _result(
            code,
            "unknown",
            "probe check could not evaluate its observation",
            {"check_error_type": type(exc).__name__},
        )
    if result.get("code") != code:
        return _result(
            code, "unknown", "probe check returned the wrong result identity"
        )
    return result


def run_probe(readers: object, *, observed_at: datetime) -> dict[str, Any]:
    """Run all observational checks without short-circuiting.

    ``readers`` must expose the callables named by ``READ_METHODS``.  The
    evaluator has no fallback that reaches a live service directly, which keeps
    every external boundary injectable and auditable.
    """

    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    now = observed_at.astimezone(UTC)
    observations = _read_all(readers)
    get = observations.__getitem__
    checks: tuple[tuple[str, Callable[[], dict[str, Any]]], ...] = (
        (RESULT_CODES[0], lambda: _check_container(get("read_container"))),
        (RESULT_CODES[1], lambda: _check_ui_health(get("read_ui_health"))),
        (RESULT_CODES[2], lambda: _check_dag_inventory(get("read_dags"))),
        (
            RESULT_CODES[3],
            lambda: _check_pause_posture(
                get("read_dags"), get("read_parent_child"), now
            ),
        ),
        (
            RESULT_CODES[4],
            lambda: _check_parent_child(get("read_parent_child"), now),
        ),
        (
            RESULT_CODES[5],
            lambda: _check_receipt(
                get("read_receipt"),
                get("read_parent_child"),
                get("read_registry"),
                get("read_target"),
                now,
            ),
        ),
        (
            RESULT_CODES[6],
            lambda: _check_registry(get("read_registry"), get("read_receipt")),
        ),
        (
            RESULT_CODES[7],
            lambda: _check_target(
                get("read_target"), get("read_registry"), get("read_receipt")
            ),
        ),
        (
            RESULT_CODES[8],
            lambda: _check_heads(
                get("read_scope_heads"), get("read_target"), get("read_registry")
            ),
        ),
        (
            RESULT_CODES[9],
            lambda: _check_dispositions(get("read_dispositions"), get("read_target")),
        ),
        (
            RESULT_CODES[10],
            lambda: _check_freshness(get("read_scope_heads"), get("read_target"), now),
        ),
        (RESULT_CODES[11], lambda: _check_leases(get("read_active_leases"))),
        (RESULT_CODES[12], lambda: _check_known_events(get("read_known_events"))),
        (RESULT_CODES[13], lambda: _check_layout(get("read_layout"))),
    )
    results = [_safe_check(code, check) for code, check in checks]
    counts = {
        status: sum(item["status"] == status for item in results)
        for status in ("ok", "fail", "unknown")
    }
    return {
        "kind": PROBE_KIND,
        "schema_version": SCHEMA_VERSION,
        "observed_at": _utc_iso(now),
        "status": "ok"
        if counts == {"ok": len(results), "fail": 0, "unknown": 0}
        else "fail",
        "counts": counts,
        "results": results,
    }


class SnapshotReaders:
    """Read-only adapter for a pre-collected JSON observation snapshot."""

    def __init__(self, snapshot: Mapping[str, Any]):
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot must be a mapping")
        self._snapshot = snapshot

    def _get(self, key: str) -> Any:
        if key not in self._snapshot:
            raise KeyError(key)
        return self._snapshot[key]

    def read_container(self) -> Any:
        return self._get("container")

    def read_ui_health(self) -> Any:
        return self._get("ui_health")

    def read_dags(self) -> Any:
        return self._get("dags")

    def read_parent_child(self) -> Any:
        return self._get("parent_child")

    def read_receipt(self) -> Any:
        return self._get("receipt")

    def read_registry(self) -> Any:
        return self._get("registry")

    def read_target(self) -> Any:
        return self._get("target")

    def read_scope_heads(self) -> Any:
        return self._get("scope_heads")

    def read_dispositions(self) -> Any:
        return self._get("dispositions")

    def read_active_leases(self) -> Any:
        return self._get("active_leases")

    def read_known_events(self) -> Any:
        return self._get("known_events")

    def read_layout(self) -> Any:
        return self._get("layout")


def _load_snapshot(path: str) -> Mapping[str, Any]:
    if path == "-":
        value = json.load(sys.stdin)
    else:
        with open(path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    if not isinstance(value, Mapping):
        raise ValueError("snapshot root must be a JSON object")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default="-", help="JSON snapshot path or -")
    parser.add_argument(
        "--observed-at",
        help="aware ISO-8601 timestamp (defaults to the current UTC time)",
    )
    args = parser.parse_args(argv)
    observed_at = (
        datetime.now(UTC)
        if args.observed_at is None
        else _parse_timestamp(args.observed_at)
    )
    if observed_at is None:
        parser.error("--observed-at must be a timezone-aware ISO-8601 timestamp")
    report = run_probe(
        SnapshotReaders(_load_snapshot(args.snapshot)), observed_at=observed_at
    )
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARM_ORDER",
    "ESPN_DAG_IDS",
    "EXPECTED_PARSER_VERSION",
    "EXPECTED_RUNTIME_VERSION",
    "KNOWN_LEAGUES_CUP_EVENT_IDS",
    "READ_METHODS",
    "RESULT_CODES",
    "SnapshotReaders",
    "run_probe",
]
