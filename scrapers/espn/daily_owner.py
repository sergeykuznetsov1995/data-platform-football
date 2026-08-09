"""Frozen ownership contract for isolated daily ESPN ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from types import MappingProxyType
from typing import Mapping


UTC = timezone.utc
ISOLATED_STACK_ENV = "ESPN_ISOLATED_STACK"
SCHEDULED_RUN_TYPE = "scheduled"
ACTIVE_TRIGGER_STATES = frozenset({"running", "deferred", "up_for_reschedule"})


class DailyOwnerError(ValueError):
    """The isolated daily owner identity is incomplete or forged."""


@dataclass(frozen=True, slots=True)
class DailyOwnerProfile:
    """Immutable identities admitted to own one daily ESPN child run."""

    name: str
    parent_dag_id: str
    trigger_task_id: str
    child_dag_id: str
    child_run_prefix: str
    envelope_schema: str


ESPN_ISOLATED_V1 = DailyOwnerProfile(
    name="espn-isolated-v1",
    parent_dag_id="dag_trigger_espn_daily",
    trigger_task_id="trigger_espn_ingest",
    child_dag_id="dag_ingest_espn",
    child_run_prefix="espn_daily",
    envelope_schema="espn-daily-parent-v2",
)
_OWNER_PROFILES: Mapping[str, DailyOwnerProfile] = MappingProxyType(
    {ESPN_ISOLATED_V1.name: ESPN_ISOLATED_V1}
)
DAILY_PARENT_FIELDS = frozenset(
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


def _required(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DailyOwnerError(f"{field} must be a non-empty string")
    return value


def _enum_value(value: object) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").lower().split(".")[-1]


def _utc(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DailyOwnerError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def resolve_daily_owner_profile(name: object) -> DailyOwnerProfile:
    """Resolve only the reviewed profile; arbitrary identities are rejected."""

    if not isinstance(name, str) or name not in _OWNER_PROFILES:
        raise DailyOwnerError("unknown ESPN daily owner profile")
    return _OWNER_PROFILES[name]


def daily_child_run_id(parent_run_id: object) -> str:
    """Return the one deterministic child identity for a parent run."""

    run_id = _required(parent_run_id, "parent_run_id")
    profile = ESPN_ISOLATED_V1
    return f"{profile.child_run_prefix}__{profile.parent_dag_id}__{run_id}"


def standard_scheduled_run_id(logical_date: object) -> str:
    """Mirror Airflow 2.11's standard scheduled DagRun identity."""

    logical = _utc(logical_date, "logical_date")
    return f"{SCHEDULED_RUN_TYPE}__{logical.isoformat()}"


def daily_parent_envelope(
    *,
    parent_run_id: object,
    logical_date: object,
    data_interval_start: object,
    data_interval_end: object,
) -> dict[str, str]:
    """Build the exact v2 envelope without accepting identity configuration."""

    profile = ESPN_ISOLATED_V1
    run_id = _required(parent_run_id, "parent_run_id")
    return {
        "schema": profile.envelope_schema,
        "owner_profile": profile.name,
        "parent_dag_id": profile.parent_dag_id,
        "parent_task_id": profile.trigger_task_id,
        "parent_run_id": run_id,
        "parent_run_type": SCHEDULED_RUN_TYPE,
        "logical_date": _required(logical_date, "logical_date"),
        "data_interval_start": _required(data_interval_start, "data_interval_start"),
        "data_interval_end": _required(data_interval_end, "data_interval_end"),
        "child_dag_id": profile.child_dag_id,
        "child_run_id": daily_child_run_id(run_id),
    }


def validate_scheduled_owner(
    *,
    environ: Mapping[str, str] | None = None,
    **context: object,
) -> dict[str, str]:
    """Fail unless this task is inside the exact isolated scheduled owner."""

    runtime_env = os.environ if environ is None else environ
    if runtime_env.get(ISOLATED_STACK_ENV) != "1":
        raise DailyOwnerError("ESPN daily owner requires ESPN_ISOLATED_STACK=1")

    profile = ESPN_ISOLATED_V1
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    if dag_run is None or dag is None:
        raise DailyOwnerError("ESPN daily owner requires an Airflow DagRun context")
    if (
        getattr(dag, "dag_id", None) != profile.parent_dag_id
        or getattr(dag_run, "dag_id", None) != profile.parent_dag_id
    ):
        raise DailyOwnerError("ESPN daily owner DAG identity mismatch")
    if _enum_value(getattr(dag_run, "run_type", None)) != SCHEDULED_RUN_TYPE:
        raise DailyOwnerError("ESPN daily owner requires a scheduled run type")
    if _enum_value(getattr(dag_run, "state", None)) != "running":
        raise DailyOwnerError("ESPN daily owner DagRun is not active")

    run_id = _required(getattr(dag_run, "run_id", None), "parent run ID")
    if context.get("run_id") != run_id:
        raise DailyOwnerError("ESPN daily owner context run ID mismatch")
    logical_date = _utc(getattr(dag_run, "logical_date", None), "logical_date")
    interval_start = _utc(
        getattr(dag_run, "data_interval_start", None), "data_interval_start"
    )
    interval_end = _utc(
        getattr(dag_run, "data_interval_end", None), "data_interval_end"
    )
    context_logical = _utc(context.get("logical_date"), "context logical_date")
    context_start = _utc(
        context.get("data_interval_start"), "context data_interval_start"
    )
    context_end = _utc(context.get("data_interval_end"), "context data_interval_end")
    if (logical_date, interval_start, interval_end) != (
        context_logical,
        context_start,
        context_end,
    ):
        raise DailyOwnerError("ESPN daily owner context interval mismatch")
    if (
        logical_date != interval_start
        or interval_end - interval_start != timedelta(days=1)
        or (
            interval_start.hour,
            interval_start.minute,
            interval_start.second,
            interval_start.microsecond,
        )
        != (14, 0, 0, 0)
    ):
        raise DailyOwnerError("ESPN daily owner interval is not the 14:00 UTC day")
    if run_id != standard_scheduled_run_id(logical_date):
        raise DailyOwnerError("ESPN daily owner run ID is not standard scheduled form")
    return {
        "owner_profile": profile.name,
        "parent_dag_id": profile.parent_dag_id,
        "parent_run_id": run_id,
    }
