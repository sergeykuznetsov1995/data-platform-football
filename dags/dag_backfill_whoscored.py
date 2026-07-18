"""Bounded, S3-resumable WhoScored historical backfill.

Every DagRun executes at most 100 dynamically mapped work items.  The frozen
plan and immutable success receipts are stored beside the WhoScored raw data,
so a task/container/host restart resumes from durable evidence rather than a
mutable file under Airflow logs.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/dags/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import base64
import hashlib
import json
import math
import os
import re
import shlex
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags.scripts.whoscored_identity import stable_safe_token
from dags.scripts.whoscored_proxy_runtime import (
    PaidRuntime,
    WhoScoredProxyRuntimeError,
    paid_campaign_gateway_call,
    paid_alert_source_guard_command,
    projected_paid_alert_environment,
    projected_transport_environment,
    resolve_paid_runtime,
    validate_paid_alert_for_source,
    validate_transport_alert_delivery,
)
from scrapers.whoscored.runtime_limits import (
    SOURCE_PAGE_REQUESTS_PER_MINUTE,
    source_page_request_hard_ceiling_per_day,
    source_pool_slots,
)
from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS


RUN_ROOT = "/opt/airflow/logs/whoscored_runs"
BACKFILL_CHUNK_SIZE = 25
PROFILE_CHUNK_SIZE = 200
MAX_WORK_ITEMS_PER_RUN = 100
BACKFILL_DEADLINE_DAYS = 30
try:
    BACKFILL_SOURCE_POOL_SLOTS = source_pool_slots()
except ValueError as exc:
    raise AirflowException(str(exc)) from exc
BACKFILL_PAGE_REQUESTS_PER_MINUTE_PER_SLOT = SOURCE_PAGE_REQUESTS_PER_MINUTE
BACKFILL_CAPACITY_HARD_CEILING_REQUEST_UNITS_PER_DAY = (
    source_page_request_hard_ceiling_per_day(BACKFILL_SOURCE_POOL_SLOTS)
)
BACKFILL_OBSERVED_MIN_ELAPSED_SECONDS = 6 * 60 * 60
BACKFILL_OBSERVED_MIN_COMPLETED_REQUEST_UNITS = 1_000
DIRECT_POOL = os.environ.get("WHOSCORED_DIRECT_POOL", "whoscored_direct_pool")
BACKFILL_POOL = os.environ.get("WHOSCORED_BACKFILL_POOL", "whoscored_direct_pool")
DQ_POOL = os.environ.get("WHOSCORED_DQ_POOL", "whoscored_dq_pool")
_QUEUE_ID = re.compile(r"^[A-Za-z0-9_.-]{0,120}$")
_PLAN_ID = re.compile(r"^[0-9a-f]{64}$")
FROZEN_DQ_POPULATION_VERSION = 1
FROZEN_SCOPE_QUERY_CHUNK_SIZE = 500


def _transport_runtime(
    context: Mapping[str, Any],
    *,
    task_id: Optional[str] = None,
    work_item_id: Optional[str] = None,
) -> PaidRuntime:
    try:
        return resolve_paid_runtime(
            context,
            task_id=task_id,
            work_item_id=work_item_id,
        )
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(str(exc)) from exc


def _bind_transport_allocation(
    transport: PaidRuntime,
    *,
    task_id: str,
    work_item_id: str,
) -> PaidRuntime:
    try:
        return transport.for_allocation(
            task_id=task_id,
            work_item_id=work_item_id,
        )
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(str(exc)) from exc


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _scope_chunks(
    scope_pairs: list[tuple[str, str]],
) -> list[list[tuple[str, str]]]:
    """Split frozen scope identities into planner-safe Trino batches."""

    return [
        scope_pairs[index : index + FROZEN_SCOPE_QUERY_CHUNK_SIZE]
        for index in range(0, len(scope_pairs), FROZEN_SCOPE_QUERY_CHUNK_SIZE)
    ]


def _scope_values_sql(
    scope_pairs: list[tuple[str, str]],
    *,
    alias: str = "frozen",
) -> str:
    """Build one bounded ``VALUES`` relation for exact scope joins."""

    if not scope_pairs or len(scope_pairs) > FROZEN_SCOPE_QUERY_CHUNK_SIZE:
        raise AirflowException(
            "WhoScored frozen scope SQL batch must contain 1.."
            f"{FROZEN_SCOPE_QUERY_CHUNK_SIZE} scopes"
        )
    values = ",".join(
        f"({_sql_string(league)}, {_sql_string(season)})"
        for league, season in scope_pairs
    )
    return f"SELECT * FROM (VALUES {values}) AS {alias}(league, season)"


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _frozen_match_identity_key(item: Mapping[str, Any]) -> tuple[str, str, int]:
    """Order staged keys exactly as the snapshot-pinned Trino fingerprint."""

    return str(item["league"]), str(item["season"]), int(item["game_id"])


def _frozen_dq_population(
    state: Any,
    plan: Mapping[str, Any],
    progress: Mapping[str, Any],
) -> Dict[str, Any]:
    """Materialise exact receipt-derived DQ keys as an immutable artifact.

    Schedule and roster tables are mutable during a long crawl.  The terminal
    gate therefore derives its population only from the validated latest
    checkpoint and proves that the match/profile receipts cover that frozen
    population exactly before querying Bronze.
    """

    queue_id = str(plan["queue_id"])
    plan_id = str(plan["plan_id"])
    checkpoint = state.latest_checkpoint(queue_id, plan_id)
    checkpoint_artifact = checkpoint.get("artifact")
    if not isinstance(checkpoint_artifact, Mapping):
        raise AirflowException("backfill DQ checkpoint has no immutable artifact")
    progress_artifact = progress.get("checkpoint")
    if isinstance(progress_artifact, Mapping) and any(
        progress_artifact.get(field) != checkpoint_artifact.get(field)
        for field in ("key", "sha256", "bytes")
    ):
        raise AirflowException("backfill DQ checkpoint changed after progress proof")

    receipts = checkpoint.get("receipts")
    if not isinstance(receipts, list):
        raise AirflowException("backfill DQ checkpoint has no validated receipts")
    by_kind: dict[str, list[Mapping[str, Any]]] = {
        kind: [] for kind in ("schedule", "matches", "roster", "profiles")
    }
    for receipt in receipts:
        if not isinstance(receipt, Mapping) or receipt.get("kind") not in by_kind:
            raise AirflowException("backfill DQ checkpoint contains an invalid receipt")
        by_kind[str(receipt["kind"])].append(receipt)

    plan_scopes = sorted(str(value) for value in plan.get("scopes", []))
    match_chunk_size = plan.get("match_chunk_size")
    profile_chunk_size = plan.get("profile_chunk_size")
    if (
        not plan_scopes
        or type(match_chunk_size) is not int
        or match_chunk_size <= 0
        or type(profile_chunk_size) is not int
        or profile_chunk_size <= 0
    ):
        raise AirflowException("backfill DQ plan has an invalid frozen work policy")

    def require_exact_work_items(
        kind: str,
        expected: list[dict[str, Any]],
    ) -> None:
        actual = [dict(receipt.get("work_item") or {}) for receipt in by_kind[kind]]
        expected_sorted = sorted(
            expected,
            key=lambda item: str(item.get("work_id") or ""),
        )
        actual_sorted = sorted(
            actual,
            key=lambda item: str(item.get("work_id") or ""),
        )
        if actual_sorted != expected_sorted:
            expected_ids = [str(item.get("work_id") or "") for item in expected_sorted]
            actual_ids = [str(item.get("work_id") or "") for item in actual_sorted]
            raise AirflowException(
                f"backfill DQ {kind} receipts do not exactly match the frozen "
                f"work items: expected={expected_ids[:20]}, actual={actual_ids[:20]}"
            )

    schedule_by_scope = {
        str(receipt["scope"]): receipt for receipt in by_kind["schedule"]
    }
    roster_by_scope = {str(receipt["scope"]): receipt for receipt in by_kind["roster"]}
    if (
        len(schedule_by_scope) != len(by_kind["schedule"])
        or sorted(schedule_by_scope) != plan_scopes
        or len(roster_by_scope) != len(by_kind["roster"])
        or sorted(roster_by_scope) != plan_scopes
    ):
        raise AirflowException(
            "backfill DQ requires exactly one schedule and roster freeze per scope"
        )
    require_exact_work_items(
        "schedule",
        [state._schedule_work(plan, scope) for scope in plan_scopes],
    )
    require_exact_work_items(
        "roster",
        [state._roster_work(scope) for scope in plan_scopes],
    )

    matches: list[dict[str, Any]] = []
    scope_stages: list[dict[str, Any]] = []
    expected_match_work: list[dict[str, Any]] = []
    candidate_keys: set[tuple[str, int]] = set()
    preview_keys: set[tuple[str, int]] = set()
    global_game_ids: set[int] = set()
    for scope in plan_scopes:
        league, separator, season = scope.rpartition("=")
        if not separator or not league or not season:
            raise AirflowException(f"invalid frozen scope {scope!r}")
        outcome = schedule_by_scope[scope].get("outcome")
        if not isinstance(outcome, Mapping):
            raise AirflowException("schedule receipt has no frozen outcome")
        candidate_ids = list(outcome.get("candidate_game_ids") or [])
        preview_ids = set(outcome.get("preview_game_ids") or [])
        source_stage_ids = list(outcome.get("source_stage_ids") or [])
        if (
            not source_stage_ids
            or source_stage_ids != sorted(set(source_stage_ids))
            or any(
                type(stage_id) is not int or stage_id <= 0
                for stage_id in source_stage_ids
            )
        ):
            raise AirflowException(
                f"schedule receipt has invalid frozen stage IDs for {scope}"
            )
        catalog_stage_ids = list(plan.get("schedule_stage_ids", {}).get(scope) or [])
        if source_stage_ids != catalog_stage_ids:
            raise AirflowException(
                "backfill DQ detected source-stage identity drift for "
                f"{scope}: catalog={catalog_stage_ids}, observed={source_stage_ids}"
            )
        scope_stages.append(
            {
                "scope": scope,
                "league": league,
                "season": season,
                "stage_ids": source_stage_ids,
            }
        )
        for index in range(0, len(candidate_ids), match_chunk_size):
            chunk = candidate_ids[index : index + match_chunk_size]
            expected_match_work.append(
                state._match_work(
                    scope,
                    chunk,
                    sorted(preview_ids & set(chunk)),
                    index // match_chunk_size,
                )
            )
        for game_id in candidate_ids:
            key = (scope, int(game_id))
            if key in candidate_keys or int(game_id) in global_game_ids:
                raise AirflowException(
                    f"frozen game_id is not globally unique: {game_id}"
                )
            candidate_keys.add(key)
            global_game_ids.add(int(game_id))
            if int(game_id) in preview_ids:
                preview_keys.add(key)
            matches.append(
                {
                    "scope": scope,
                    "league": league,
                    "season": season,
                    "game_id": int(game_id),
                    "preview_required": int(game_id) in preview_ids,
                }
            )

    require_exact_work_items("matches", expected_match_work)
    receipt_match_keys = [
        (str(receipt["scope"]), int(game_id))
        for receipt in by_kind["matches"]
        for game_id in receipt.get("outcome", {}).get("game_ids", [])
    ]
    receipt_preview_keys = [
        (str(receipt["scope"]), int(game_id))
        for receipt in by_kind["matches"]
        for game_id in receipt.get("work_item", {}).get("preview_game_ids", [])
    ]
    if (
        len(receipt_match_keys) != len(candidate_keys)
        or set(receipt_match_keys) != candidate_keys
        or len(receipt_preview_keys) != len(preview_keys)
        or set(receipt_preview_keys) != preview_keys
    ):
        raise AirflowException(
            "backfill match receipts do not exactly cover the frozen schedule population"
        )

    frozen_player_ids = sorted(
        {
            int(player_id)
            for receipt in by_kind["roster"]
            for player_id in receipt.get("outcome", {}).get("profile_player_ids", [])
        }
    )
    owner_scope = plan_scopes[0]
    expected_profile_work = [
        state._profile_work(
            owner_scope,
            frozen_player_ids[index : index + profile_chunk_size],
            index // profile_chunk_size,
        )
        for index in range(0, len(frozen_player_ids), profile_chunk_size)
    ]
    require_exact_work_items("profiles", expected_profile_work)
    receipt_player_ids = [
        int(player_id)
        for receipt in by_kind["profiles"]
        for player_id in receipt.get("outcome", {}).get("player_ids", [])
    ]
    if len(receipt_player_ids) != len(frozen_player_ids) or set(
        receipt_player_ids
    ) != set(frozen_player_ids):
        raise AirflowException(
            "backfill profile receipts do not exactly cover the frozen roster population"
        )

    population = {
        "schema_version": 1,
        "population_version": FROZEN_DQ_POPULATION_VERSION,
        "queue_id": queue_id,
        "plan_id": plan_id,
        "checkpoint": {
            field: checkpoint_artifact[field] for field in ("key", "sha256", "bytes")
        },
        "matches": sorted(
            matches,
            key=_frozen_match_identity_key,
        ),
        "scope_stages": scope_stages,
        "player_ids": frozen_player_ids,
        "counts": {
            "scopes": len(plan_scopes),
            "matches": len(matches),
            "previews": len(preview_keys),
            "players": len(frozen_player_ids),
            "stages": sum(len(item["stage_ids"]) for item in scope_stages),
        },
    }
    prefix = f"backfill/{queue_id}/dq-populations/{plan_id}"
    artifact = state.store.put_content_addressed_json(prefix, population)
    persisted = state.store.read_content_addressed_json(
        str(artifact["key"]),
        expected_sha256=str(artifact["sha256"]),
        expected_bytes=int(artifact["bytes"]),
    )
    if persisted != population:
        raise AirflowException("backfill frozen DQ population readback mismatch")
    return {
        **population,
        "population_sha256": hashlib.sha256(
            _canonical_json_bytes(population)
        ).hexdigest(),
        "artifact": artifact,
    }


def _frozen_scope_feed_integrity(
    population: Mapping[str, Any],
    *,
    staged_relation: Mapping[str, Any],
) -> Dict[str, int]:
    """Validate the exact 68-feed contract for every receipt-frozen stage.

    The catalog can contain thousands of historical scopes.  One join against
    the snapshot-pinned staged relation replaces cardinality-sized ``VALUES``
    SQL while the feed-key validation remains scope-local.
    """

    from dags.dag_ingest_whoscored import _feed_state_integrity_summary
    from scrapers.whoscored.parsers import PARSER_VERSION
    from scrapers.base.trino_manager import get_trino_connection

    counters = {
        "frozen_scope_stage_count": 0,
        "missing_scope_feed_manifests": 0,
        "expected_feed_state_count": 0,
        "actual_feed_state_count": 0,
        "missing_feed_state_count": 0,
        "extra_feed_state_count": 0,
        "malformed_feed_state_count": 0,
        "unavailable_feed_count": 0,
    }
    raw_scope_stages = population.get("scope_stages")
    if not isinstance(raw_scope_stages, list):
        raise AirflowException("frozen scope feed population is malformed")
    scope_stages: dict[tuple[str, str], list[int]] = {}
    for item in raw_scope_stages:
        if not isinstance(item, Mapping):
            raise AirflowException("frozen scope feed population is malformed")
        key = (str(item.get("league") or ""), str(item.get("season") or ""))
        stage_ids = item.get("stage_ids")
        if (
            not all(key)
            or not isinstance(stage_ids, list)
            or not stage_ids
            or any(type(stage_id) is not int or stage_id <= 0 for stage_id in stage_ids)
            or stage_ids != sorted(set(stage_ids))
            or (
                item.get("scope") is not None
                and item.get("scope") != f"{key[0]}={key[1]}"
            )
            or key in scope_stages
        ):
            raise AirflowException("frozen scope feed population is malformed")
        scope_stages[key] = list(stage_ids)
        counters["frozen_scope_stage_count"] += len(stage_ids)

    from dags.scripts.whoscored_frozen_dq import (
        FrozenDQError,
        staged_scope_relation_sql,
    )

    try:
        eligible_sql = staged_scope_relation_sql(staged_relation)
    except FrozenDQError as exc:
        raise AirflowException(str(exc)) from exc
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH eligible AS (
                    {eligible_sql}
                ),
                ranked AS (
                    SELECT m.league, m.season, m.dataset_states_json,
                           ROW_NUMBER() OVER (
                               PARTITION BY m.league, m.season
                               ORDER BY m.completed_at DESC,
                                        m._ingested_at DESC,
                                        m.batch_id DESC
                           ) AS rn
                    FROM iceberg.bronze.whoscored_scope_ingest_manifest m
                    JOIN eligible e
                      ON e.league=m.league AND e.season=m.season
                    WHERE m.entity_group='season' AND m.state='success'
                      AND m.parser_version={_sql_string(PARSER_VERSION)}
                      AND m.batch_id LIKE 'wss2-%'
                      AND m.raw_uris_json IS NOT NULL
                )
                SELECT e.league, e.season, r.dataset_states_json
                FROM eligible e
                LEFT JOIN ranked r
                  ON r.league=e.league AND r.season=e.season AND r.rn=1
                ORDER BY e.league, e.season
                """
            )
            rows = cur.fetchall()
            returned: dict[tuple[str, str], Any] = {}
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) != 3:
                    raise AirflowException(
                        "frozen scope feed query returned a malformed row"
                    )
                key = (str(row[0]), str(row[1]))
                if key not in scope_stages or key in returned:
                    raise AirflowException(
                        "frozen scope feed query returned unexpected identities"
                    )
                returned[key] = row[2]
            if set(returned) != set(scope_stages):
                raise AirflowException(
                    "frozen scope feed query did not round-trip every identity"
                )
            for key in sorted(scope_stages):
                payload = returned[key]
                if payload is None:
                    counters["missing_scope_feed_manifests"] += 1
                    continue
                integrity = _feed_state_integrity_summary(
                    payload,
                    scope_stages[key],
                )
                for counter in (
                    "expected_feed_state_count",
                    "actual_feed_state_count",
                    "missing_feed_state_count",
                    "extra_feed_state_count",
                    "malformed_feed_state_count",
                    "unavailable_feed_count",
                ):
                    counters[counter] += int(integrity[counter])
        finally:
            cur.close()
    finally:
        conn.close()
    return counters


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _catalog_as_of_date(context: Mapping[str, Any]) -> date:
    logical_date = context.get("logical_date")
    if not isinstance(logical_date, datetime) or logical_date.tzinfo is None:
        raise AirflowException(
            "WhoScored catalog discovery requires a timezone-aware logical_date"
        )
    return logical_date.date()


def _initial_backfill_started_at(context: Mapping[str, Any]) -> datetime:
    dag_run = context.get("dag_run")
    for value in (
        getattr(dag_run, "start_date", None),
        getattr(context.get("task_instance"), "start_date", None),
    ):
        if isinstance(value, datetime):
            return _as_utc(value)
    return datetime.now(timezone.utc)


def _parse_plan_time(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise AirflowException(f"invalid backfill {field}: {value!r}") from exc
    return _as_utc(parsed)


def _validate_backfill_plan_deadline(
    plan: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
) -> tuple[datetime, datetime, datetime]:
    """Validate immutable timing before any source work is constructed."""

    provenance = plan.get("provenance")
    if not isinstance(provenance, Mapping):
        raise AirflowException("backfill plan has no provenance")
    started_at = _parse_plan_time(
        provenance.get("backfill_started_at"), field="started_at"
    )
    deadline_at = _parse_plan_time(
        provenance.get("backfill_deadline_at"), field="deadline_at"
    )
    current = _as_utc(now or datetime.now(timezone.utc))
    contract_duration = deadline_at - started_at
    if not timedelta(0) < contract_duration <= timedelta(days=BACKFILL_DEADLINE_DAYS):
        raise AirflowException("backfill plan deadline must be within 30 days")
    if current > deadline_at:
        raise AirflowException(
            "WhoScored backfill deadline expired before source work: "
            f"deadline={deadline_at.isoformat()}, now={current.isoformat()}"
        )
    return started_at, deadline_at, current


def _backfill_slo_summary(
    plan: Mapping[str, Any],
    progress: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    enforce_capacity: bool = True,
) -> Dict[str, Any]:
    """Enforce the immutable deadline against an honest capacity assumption.

    The configured value is an SLO planning assumption, not a throttle.  Its
    hard maximum is the validated deployed source-pool slot count multiplied
    by the slower 30-request/minute page limiter used by matches, previews,
    and profiles.
    Observed wall-clock throughput is reported only after a useful sample and
    remains advisory so a planned pause cannot create a second false gate.
    """
    started_at, deadline_at, current = _validate_backfill_plan_deadline(
        plan,
        now=now,
    )
    capacity_env = "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY"
    try:
        capacity = int(
            os.environ.get(
                capacity_env,
                str(BACKFILL_CAPACITY_HARD_CEILING_REQUEST_UNITS_PER_DAY),
            )
        )
    except ValueError as exc:
        raise AirflowException(f"invalid {capacity_env}") from exc
    if not 1_000 <= capacity <= BACKFILL_CAPACITY_HARD_CEILING_REQUEST_UNITS_PER_DAY:
        raise AirflowException(
            f"{capacity_env} must be in 1000.."
            f"{BACKFILL_CAPACITY_HARD_CEILING_REQUEST_UNITS_PER_DAY}"
        )
    remaining = progress.get("remaining_request_units")
    exact = remaining is not None
    if remaining is None:
        remaining = progress.get("remaining_request_units_lower_bound", 0)
    if type(remaining) is not int or remaining < 0:
        raise AirflowException(f"invalid backfill remaining work: {progress}")
    estimated_completed = progress.get("estimated_completed_request_units", 0)
    actual_completed = progress.get("actual_completed_request_units", 0)
    stage_drifts = progress.get("schedule_stage_cardinality_drifts", 0)
    if (
        type(estimated_completed) is not int
        or estimated_completed < 0
        or type(actual_completed) is not int
        or actual_completed < estimated_completed
        or type(stage_drifts) is not int
        or stage_drifts < 0
    ):
        raise AirflowException(f"invalid backfill request-unit accounting: {progress}")
    seconds_remaining = (deadline_at - current).total_seconds()
    days_remaining = max(0.0, seconds_remaining / 86400)
    required_per_day = (
        0.0
        if remaining == 0
        else math.inf
        if not days_remaining
        else remaining / days_remaining
    )
    projected_days = remaining / capacity
    capacity_breached = required_per_day > capacity
    elapsed_seconds = max(0.0, (current - started_at).total_seconds())
    observed_available = (
        elapsed_seconds >= BACKFILL_OBSERVED_MIN_ELAPSED_SECONDS
        and actual_completed >= BACKFILL_OBSERVED_MIN_COMPLETED_REQUEST_UNITS
    )
    observed_per_day: Optional[float] = None
    observed_projected_days: Optional[float] = None
    observed_status = "insufficient_sample"
    if observed_available:
        observed_per_day = actual_completed / (elapsed_seconds / 86400)
        observed_projected_days = (
            0.0 if remaining == 0 else remaining / observed_per_day
        )
        observed_status = (
            "complete"
            if remaining == 0
            else "lagging"
            if observed_projected_days > days_remaining
            else "on_track"
        )
    summary = {
        "started_at": started_at.isoformat(),
        "deadline_at": deadline_at.isoformat(),
        "deadline_days": BACKFILL_DEADLINE_DAYS,
        "days_remaining": round(days_remaining, 6),
        "remaining_request_units": remaining,
        "remaining_is_exact": exact,
        "estimated_completed_request_units": estimated_completed,
        "actual_completed_request_units": actual_completed,
        "accounted_request_units_to_deadline": actual_completed + remaining,
        "schedule_stage_cardinality_drifts": stage_drifts,
        "request_unit_accounting": (
            "actual-completed-plus-exact-match-preview-estimated-remaining-v2"
        ),
        "capacity_assumption": "slo-planning-only-not-a-runtime-throttle",
        "capacity_hard_ceiling_basis": (
            f"{BACKFILL_SOURCE_POOL_SLOTS}-source-pool-slots-x-"
            f"{BACKFILL_PAGE_REQUESTS_PER_MINUTE_PER_SLOT}-"
            "page-requests-per-minute-x-1440"
        ),
        "source_pool_slots": BACKFILL_SOURCE_POOL_SLOTS,
        "capacity_hard_ceiling_request_units_per_day": (
            BACKFILL_CAPACITY_HARD_CEILING_REQUEST_UNITS_PER_DAY
        ),
        "assumed_capacity_request_units_per_day": capacity,
        "required_request_units_per_day": (
            None if not math.isfinite(required_per_day) else round(required_per_day, 6)
        ),
        "assumed_projected_days_remaining": round(projected_days, 6),
        "capacity_status": "breach" if capacity_breached else "sufficient",
        "capacity_blocker": (
            "required_request_units_per_day_exceed_assumed_capacity"
            if capacity_breached
            else None
        ),
        "observed_projection_status": observed_status,
        "observed_sample_elapsed_hours": round(elapsed_seconds / 3600, 6),
        "observed_sample_completed_request_units": actual_completed,
        "observed_request_units_per_day": (
            round(observed_per_day, 6) if observed_per_day is not None else None
        ),
        "observed_projected_days_remaining": (
            round(observed_projected_days, 6)
            if observed_projected_days is not None
            else None
        ),
        "observed_projection_is_advisory": True,
    }
    if enforce_capacity and capacity_breached:
        raise AirflowException(f"WhoScored 30-day backfill SLO failed: {summary}")
    return summary


BACKFILL_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key
        not in {
            "pool",
            "retries",
            "retry_delay",
            "execution_timeout",
            "on_failure_callback",
        }
    },
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=90),
}

_RUN_DIR_TEMPLATE = (
    RUN_ROOT + "/{{ dag.dag_id | stable_safe_token }}/{{ run_id | stable_safe_token }}"
)
_TASK_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_SCHEMA_READY": "1",
    # Airflow does not consistently export map_index as an AIRFLOW_CTX value.
    # Render it explicitly so request and paid ledgers retain exact attribution.
    "AIRFLOW_CTX_DAG_ID": "{{ dag.dag_id }}",
    "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
    "AIRFLOW_CTX_TASK_ID": "{{ task.task_id }}",
    "AIRFLOW_CTX_TRY_NUMBER": "{{ ti.try_number }}",
    "AIRFLOW_CTX_MAP_INDEX": "{{ ti.map_index }}",
    "WHOSCORED_REQUEST_LEDGER_PATH": (
        _RUN_DIR_TEMPLATE
        + "/requests_{{ task.task_id | replace('.', '_') }}_"
        + "{{ ti.map_index }}_try{{ ti.try_number }}.jsonl"
    ),
}


def _runtime_params(context: Mapping[str, Any]) -> Dict[str, Any]:
    params = dict(context.get("params") or {})
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if isinstance(conf, Mapping):
        for key in (
            "scopes",
            "game_ids",
            "date_from",
            "date_to",
            "queue_id",
            "plan_id",
            "all_catalog",
            "require_zero_paid",
            "direct_only",
            "transport_policy",
            "paid_approval_id",
            "paid_approval_sha256",
            "full_history_catalog",
        ):
            if key in conf:
                params[key] = conf[key]
    return params


def _discovery_traffic_identity(
    context: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve the exact PythonOperator attempt owning catalog traffic."""

    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    ti = context.get("ti")
    task = context.get("task")
    task_id = (
        getattr(ti, "task_id", None)
        or getattr(task, "task_id", None)
        or "prepare_backfill_plan"
    )
    try:
        try_number = int(getattr(ti, "try_number", 1))
        map_index = int(getattr(ti, "map_index", -1))
    except (TypeError, ValueError) as exc:
        raise AirflowException("invalid WhoScored discovery task identity") from exc
    if not dag_id or not run_id or try_number < 1 or map_index < -1:
        raise AirflowException(
            "WhoScored full-history discovery requires dag/run/try identity"
        )
    return {
        "dag_id": str(dag_id),
        "run_id": str(run_id),
        "task_id": str(task_id),
        "map_index": map_index,
        "try_number": try_number,
    }


def _discovery_request_ledger_path(
    context: Mapping[str, Any],
) -> tuple[Path, dict[str, Any]]:
    identity = _discovery_traffic_identity(context)
    run_dir = (
        Path(RUN_ROOT)
        / stable_safe_token(identity["dag_id"])
        / stable_safe_token(identity["run_id"])
    )
    name = (
        f"requests_{stable_safe_token(identity['task_id'])}_"
        f"{identity['map_index']}_try{identity['try_number']}.jsonl"
    )
    return run_dir / name, identity


def _initialise_discovery_request_ledger(path: Path) -> None:
    """Create an fsynced zero-event proof before constructing transport."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _persist_discovery_request_ledger(
    *,
    path: Path,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Copy exact fsynced events to immutable ops storage for terminal DQ."""

    events: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    digest = hashlib.sha256()
    source_bytes = 0
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise AirflowException(
            f"cannot read WhoScored discovery request ledger {path}: {exc}"
        ) from exc
    with handle:
        line_number = 0
        while True:
            raw = handle.readline(256 * 1024 + 1)
            if not raw:
                break
            line_number += 1
            if len(raw) > 256 * 1024:
                raise AirflowException(
                    "oversized WhoScored discovery ledger event at "
                    f"{path}:{line_number}"
                )
            if not raw.endswith(b"\n"):
                raise AirflowException(
                    "truncated WhoScored discovery request ledger at "
                    f"{path}:{line_number}"
                )
            digest.update(raw)
            source_bytes += len(raw)
            try:
                event = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise AirflowException(
                    "corrupt WhoScored discovery request ledger at "
                    f"{path}:{line_number}: {exc}"
                ) from exc
            event_id = event.get("event_id") if isinstance(event, dict) else None
            if (
                not isinstance(event, dict)
                or event.get("event_version") != "whoscored-request-v1"
                or not isinstance(event_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", event_id) is None
                or event_id in event_ids
                or any(event.get(key) != value for key, value in identity.items())
            ):
                raise AirflowException(
                    "invalid WhoScored discovery request identity at "
                    f"{path}:{line_number}"
                )
            event_ids.add(event_id)
            events.append(event)

    request_events = [event for event in events if event.get("status") != "accounted"]

    def counter(event: Mapping[str, Any], field: str) -> int:
        value = event.get(field, 0)
        if type(value) is not int or value < 0:
            raise AirflowException(
                f"invalid {field} in WhoScored discovery request ledger"
            )
        return value

    if any(counter(event, "paid_proxy_bytes") for event in request_events):
        raise AirflowException(
            "WhoScored discovery ledger contains unaccounted paid bytes"
        )
    paid_proxy_bytes = sum(
        counter(event, "paid_proxy_bytes")
        for event in events
        if event.get("status") == "accounted"
    )
    evidence = {
        "schema_version": 1,
        "evidence_type": "whoscored_request_ledger",
        **dict(identity),
        "source_name": path.name,
        "source_sha256": digest.hexdigest(),
        "source_bytes": source_bytes,
        "event_count": len(events),
        "request_count": len(request_events),
        "wire_bytes": sum(
            counter(event, "request_bytes") + counter(event, "response_bytes")
            for event in request_events
        ),
        "paid_proxy_bytes": paid_proxy_bytes,
        "events": events,
    }
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - guarded by optional=False
        raise AirflowException("WhoScored operational S3 store is required")
    prefix = (
        "traffic/"
        f"{stable_safe_token(identity['dag_id'])}/"
        f"{stable_safe_token(identity['run_id'])}/request-ledgers"
    )
    artifact = ops_store.put_content_addressed_json(prefix, evidence)
    return {
        **artifact,
        "event_count": evidence["event_count"],
        "request_count": evidence["request_count"],
        "wire_bytes": evidence["wire_bytes"],
        "paid_proxy_bytes": evidence["paid_proxy_bytes"],
    }


def _persist_discovery_report_attribution(
    *,
    traffic: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist report-owned paid attribution independently of request events."""

    from dags.dag_ingest_whoscored import _canonical_traffic_url

    raw_paid = traffic.get("paid_proxy_bytes", 0)
    raw_urls = traffic.get("paid_proxy_bytes_by_url", {})
    if type(raw_paid) is not int or raw_paid < 0 or not isinstance(raw_urls, Mapping):
        raise AirflowException(
            "WhoScored discovery returned invalid paid report attribution"
        )
    urls: dict[str, int] = {}
    for raw_url, raw_count in raw_urls.items():
        url = _canonical_traffic_url(raw_url)
        if not url or type(raw_count) is not int or raw_count <= 0 or url in urls:
            raise AirflowException(
                "WhoScored discovery returned invalid paid URL attribution"
            )
        urls[url] = raw_count
    if sum(urls.values()) != raw_paid:
        raise AirflowException("WhoScored discovery paid report URL counters differ")
    task_key = f"{identity['task_id']}[{identity['map_index']}]"
    task_try_key = f"{task_key}/try{identity['try_number']}"
    evidence = {
        "schema_version": 1,
        "evidence_type": "whoscored_report_paid_attribution",
        **dict(identity),
        "paid_proxy_bytes": raw_paid,
        "paid_proxy_bytes_by_url": dict(sorted(urls.items())),
        "paid_proxy_bytes_by_task": {task_key: raw_paid} if raw_paid else {},
        "paid_proxy_bytes_by_task_try": ({task_try_key: raw_paid} if raw_paid else {}),
    }
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - guarded by optional=False
        raise AirflowException("WhoScored operational S3 store is required")
    prefix = (
        "traffic/"
        f"{stable_safe_token(identity['dag_id'])}/"
        f"{stable_safe_token(identity['run_id'])}/report-attribution"
    )
    artifact = ops_store.put_content_addressed_json(prefix, evidence)
    return {**artifact, "paid_proxy_bytes": raw_paid}


def validate_backfill_params(**context: Any) -> Dict[str, Any]:
    params = _runtime_params(context)
    executor = os.environ.get("AIRFLOW__CORE__EXECUTOR", "").strip()
    if not executor.endswith("LocalExecutor"):
        raise AirflowException(
            "WhoScored backfill requires AIRFLOW__CORE__EXECUTOR=LocalExecutor; "
            f"got {executor or 'unset'}"
        )
    transport = _transport_runtime(context)
    if not transport.is_paid and (
        not bool(params.get("direct_only", True))
        or not bool(params.get("require_zero_paid", True))
    ):
        raise AirflowException(
            "legacy booleans cannot authorize paid WhoScored traffic; use a "
            "signed direct_then_paid DagRun configuration"
        )
    try:
        _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract()
        _WHOSCORED_RUNTIME_CONTRACT.validate_airflow_source_pool(
            direct_pool=DIRECT_POOL,
            backfill_pool=BACKFILL_POOL,
        )
    except _WHOSCORED_RUNTIME_CONTRACT.RuntimeContractError as exc:
        raise AirflowException(str(exc)) from exc
    raw_scopes = params.get("scopes") or []
    game_ids = params.get("game_ids") or []
    all_catalog = bool(params.get("all_catalog", False))
    queue_id = str(params.get("queue_id") or "")
    plan_id = str(params.get("plan_id") or "")
    if plan_id:
        if not queue_id or not _QUEUE_ID.fullmatch(queue_id):
            raise AirflowException("resuming a plan requires a valid queue_id")
        if not _PLAN_ID.fullmatch(plan_id):
            raise AirflowException("invalid WhoScored backfill plan_id")
        if (
            raw_scopes
            or game_ids
            or all_catalog
            or params.get("date_from")
            or params.get("date_to")
        ):
            raise AirflowException(
                "plan resume accepts only queue_id+plan_id, not mutable selectors"
            )
        return {
            "scopes": [],
            "game_ids": [],
            "all_catalog": False,
            "queue_id": queue_id,
            "plan_id": plan_id,
            "chunk_size": BACKFILL_CHUNK_SIZE,
            "profile_chunk_size": PROFILE_CHUNK_SIZE,
            "max_work_items": MAX_WORK_ITEMS_PER_RUN,
        }
    if not raw_scopes and not all_catalog:
        raise AirflowException(
            "WhoScored backfill requires explicit scopes or all_catalog=true"
        )
    if raw_scopes and all_catalog:
        raise AirflowException(
            "WhoScored scopes and all_catalog are mutually exclusive"
        )
    if not _QUEUE_ID.fullmatch(queue_id):
        raise AirflowException("invalid WhoScored backfill queue_id")

    from dags.scripts.run_whoscored_scraper import RunnerScope

    try:
        scopes = [RunnerScope.parse(str(value)).spec for value in raw_scopes]
    except ValueError as exc:
        raise AirflowException(str(exc)) from exc
    if len(scopes) != len(set(scopes)):
        raise AirflowException("duplicate WhoScored backfill scopes")
    if any(isinstance(value, bool) or int(value) <= 0 for value in game_ids):
        raise AirflowException("WhoScored game_ids must be positive integers")
    if game_ids and (all_catalog or len(scopes) != 1):
        raise AirflowException(
            "explicit WhoScored game_ids require exactly one explicit scope"
        )

    date_from = params.get("date_from") or None
    date_to = params.get("date_to") or None
    if date_from and date_to and str(date_to) < str(date_from):
        raise AirflowException("date_to must not precede date_from")
    return {
        "scopes": scopes,
        "game_ids": sorted({int(value) for value in game_ids}),
        "all_catalog": all_catalog,
        "queue_id": queue_id or None,
        "plan_id": None,
        "chunk_size": BACKFILL_CHUNK_SIZE,
        "profile_chunk_size": PROFILE_CHUNK_SIZE,
        "max_work_items": MAX_WORK_ITEMS_PER_RUN,
    }


def prepare_backfill_plan(
    *,
    alert_metadata: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    """Freeze the exact eligible scope set as one immutable S3 plan."""

    validated = validate_backfill_params(**context)
    params = _runtime_params(context)
    from dags.scripts import run_whoscored_scraper as runner
    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    state = WhoScoredBackfillState.from_env()
    if validated["plan_id"]:
        queue_id = str(validated["queue_id"])
        plan_id = str(validated["plan_id"])
        plan = state.load_plan(queue_id, plan_id)
        _validate_backfill_plan_deadline(plan)
        artifact = state.plan_reference(queue_id, plan_id)
        return {
            "queue_id": queue_id,
            "plan_id": plan_id,
            "plan_uri": artifact["uri"],
            "plan_sha256": artifact["sha256"],
            "scopes": len(plan["scopes"]),
            "resumed": True,
            "catalog_generation": plan.get("provenance", {}).get("catalog_batch_id"),
            "discovery_traffic_required": False,
            "discovery_traffic_evidence": None,
            "discovery_report_attribution": None,
        }

    repository = runner._new_repository()
    repository.ensure_schema()
    full_history_catalog = bool(
        validated["all_catalog"] or params.get("full_history_catalog", False)
    )
    if full_history_catalog:
        discovery_transport = _transport_runtime(
            context,
            task_id="prepare_backfill_plan",
            work_item_id="catalog-discovery",
        )
        discovery_alert_metadata = validate_paid_alert_for_source(
            discovery_transport,
            alert_metadata,
            context,
        )
        service_cls = runner._load_runtime()
        discovery_report = runner._new_report("discover", ())
        ledger_path, traffic_identity = _discovery_request_ledger_path(context)
        _initialise_discovery_request_ledger(ledger_path)
        discovery_environment = {
            "AIRFLOW_CTX_DAG_ID": traffic_identity["dag_id"],
            "AIRFLOW_CTX_DAG_RUN_ID": traffic_identity["run_id"],
            "AIRFLOW_CTX_TASK_ID": traffic_identity["task_id"],
            "AIRFLOW_CTX_MAP_INDEX": str(traffic_identity["map_index"]),
            "AIRFLOW_CTX_TRY_NUMBER": str(traffic_identity["try_number"]),
            "WHOSCORED_REQUEST_LEDGER_PATH": str(ledger_path),
        }
        previous_environment = {
            key: os.environ.get(key) for key in discovery_environment
        }
        with projected_paid_alert_environment(discovery_alert_metadata):
            with projected_transport_environment(discovery_transport, context):
                os.environ.update(discovery_environment)
                try:
                    result = runner._run_discover(
                        service_cls,
                        repository,
                        discovery_report,
                        full_history=True,
                        as_of_date=_catalog_as_of_date(context),
                    )
                finally:
                    try:
                        discovery_traffic_evidence = _persist_discovery_request_ledger(
                            path=ledger_path,
                            identity=traffic_identity,
                        )
                    finally:
                        for key, value in previous_environment.items():
                            if value is None:
                                os.environ.pop(key, None)
                            else:
                                os.environ[key] = value
        reported_traffic = getattr(result, "traffic", {})
        raw_reported_paid = (
            reported_traffic.get("paid_proxy_bytes", 0)
            if isinstance(reported_traffic, Mapping)
            else 0
        )
        if type(raw_reported_paid) is not int or raw_reported_paid < 0:
            raise AirflowException(
                "WhoScored discovery returned invalid paid traffic evidence"
            )
        reported_paid = raw_reported_paid
        discovery_report_attribution = _persist_discovery_report_attribution(
            traffic=(reported_traffic if isinstance(reported_traffic, Mapping) else {}),
            identity=traffic_identity,
        )
        if reported_paid != discovery_traffic_evidence["paid_proxy_bytes"]:
            raise AirflowException(
                "WhoScored discovery paid traffic ledger/report mismatch: "
                f"ledger={discovery_traffic_evidence['paid_proxy_bytes']}, "
                f"report={reported_paid}"
            )
        if not discovery_transport.is_paid and reported_paid:
            raise AirflowException(
                f"WhoScored backfill discovery used paid proxy: {reported_paid} bytes"
            )
        if getattr(result, "errors", None):
            raise AirflowException(
                "WhoScored full-history discovery failed: "
                + "; ".join(str(item) for item in result.errors)
            )
    else:
        discovery_traffic_evidence = None
        discovery_report_attribution = None

    provenance, catalog_snapshot = repository.load_catalog_generation_snapshot()
    catalog_discovery_mode = str(provenance.get("catalog_discovery_mode") or "")
    if full_history_catalog and catalog_discovery_mode != "full_history":
        raise AirflowException(
            "WhoScored full-history discovery was not proven by its exact "
            f"catalog manifest: mode={catalog_discovery_mode or 'missing'}"
        )
    requested = [runner.RunnerScope.parse(item) for item in validated["scopes"]]
    selected = runner._select_catalog_snapshot_scopes(
        catalog_snapshot,
        requested,
        active_only=False,
    )
    scopes = [scope.spec for scope, _runtime in selected]
    schedule_stage_ids = {
        scope.spec: sorted(
            {int(stage_id) for stage_id in getattr(runtime, "stage_ids", ())}
        )
        for scope, runtime in selected
    }
    catalog_scopes = [
        scope.spec
        for scope, _runtime in runner._select_catalog_snapshot_scopes(
            catalog_snapshot,
            [],
            active_only=False,
        )
    ]
    selector = {
        "requested_scopes": sorted(validated["scopes"]),
        "game_ids": validated["game_ids"],
        "all_catalog": validated["all_catalog"],
        "date_from": params.get("date_from") or None,
        "date_to": params.get("date_to") or None,
        "full_history_catalog": full_history_catalog,
    }
    from dags.scripts.run_whoscored_scraper import _selector_identity

    date_from = (
        datetime.strptime(str(params["date_from"]), "%Y-%m-%d").date()
        if params.get("date_from")
        else None
    )
    date_to = (
        datetime.strptime(str(params["date_to"]), "%Y-%m-%d").date()
        if params.get("date_to")
        else None
    )
    _selector_hash, default_queue_id = _selector_identity(
        [scope for scope, _runtime in selected],
        validated["game_ids"],
        validated["all_catalog"],
        date_from,
        date_to,
    )
    queue_id = validated["queue_id"] or default_queue_id
    provenance["full_history_discovery"] = catalog_discovery_mode == "full_history"
    provenance["catalog_eligible_scope_count"] = len(catalog_scopes)
    provenance["catalog_eligible_scopes_sha256"] = hashlib.sha256(
        ("\n".join(sorted(catalog_scopes)) + "\n").encode("utf-8")
    ).hexdigest()
    started_at = _initial_backfill_started_at(context)
    provenance["backfill_started_at"] = started_at.isoformat()
    provenance["backfill_deadline_at"] = (
        started_at + timedelta(days=BACKFILL_DEADLINE_DAYS)
    ).isoformat()
    plan = state.create_plan(
        queue_id=queue_id,
        selector=selector,
        scopes=scopes,
        provenance=provenance,
        schedule_stage_ids=schedule_stage_ids,
    )
    return {
        "queue_id": queue_id,
        "plan_id": plan["plan_id"],
        "plan_uri": plan["artifact"]["uri"],
        "plan_sha256": plan["artifact"]["sha256"],
        "scopes": len(scopes),
        "resumed": False,
        "catalog_generation": provenance["catalog_batch_id"],
        "discovery_traffic_required": full_history_catalog,
        "discovery_traffic_evidence": discovery_traffic_evidence,
        "discovery_report_attribution": discovery_report_attribution,
    }


def _work_output_path(context: Mapping[str, Any], work_id: str) -> Path:
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown"
    run_id = context.get("run_id")
    if not run_id and context.get("dag_run") is not None:
        run_id = context["dag_run"].run_id
    return (
        Path(RUN_ROOT)
        / stable_safe_token(dag_id)
        / stable_safe_token(run_id)
        / f"backfill_{stable_safe_token(work_id)}.json"
    )


def _controller_batch_id(context: Mapping[str, Any]) -> str:
    run_id = context.get("run_id")
    if not run_id and context.get("dag_run") is not None:
        run_id = context["dag_run"].run_id
    if not run_id:
        raise AirflowException("backfill controller requires a DagRun identity")
    return str(run_id)


def build_backfill_commands(
    *,
    plan_ref: Mapping[str, Any],
    alert_metadata: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> list[str]:
    """Return at most 100 independent commands from durable pending state."""

    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    queue_id = str(plan_ref["queue_id"])
    plan_id = str(plan_ref["plan_id"])
    state = WhoScoredBackfillState.from_env()
    plan = state.load_plan(queue_id, plan_id)
    _validate_backfill_plan_deadline(plan)
    # Re-evaluate the durable frontier before materialising any source work.
    # Schedule receipts reveal the exact match/preview population and roster
    # receipts later reveal profiles; each newly exact lower bound can stop
    # the crawl before the next network batch if the 30-day target is no
    # longer achievable under the deployed source ceiling.
    progress = state.checkpoint_progress(queue_id, plan_id)
    _backfill_slo_summary(plan, progress)
    try:
        request_unit_limit = int(
            os.environ.get("WHOSCORED_BACKFILL_REQUEST_UNITS_PER_RUN", "3000")
        )
    except ValueError as exc:
        raise AirflowException(
            "invalid WHOSCORED_BACKFILL_REQUEST_UNITS_PER_RUN"
        ) from exc
    batch = state.create_batch(
        queue_id,
        plan_id,
        batch_id=_controller_batch_id(context),
        limit=MAX_WORK_ITEMS_PER_RUN,
        request_unit_limit=request_unit_limit,
    )
    base_transport = _transport_runtime(context)
    commands: list[str] = []
    for item in batch["work_items"]:
        work_item_id = str(item["work_id"])
        transport = _bind_transport_allocation(
            base_transport,
            task_id="run_whoscored_backfill_item",
            work_item_id=work_item_id,
        )
        alert_guard = paid_alert_source_guard_command(
            transport,
            alert_metadata,
            context,
        )
        encoded = (
            base64.urlsafe_b64encode(
                json.dumps(item, separators=(",", ":"), sort_keys=True).encode("utf-8")
            )
            .decode("ascii")
            .rstrip("=")
        )
        output = _work_output_path(context, work_item_id)
        output_argument = (
            shlex.quote(str(output.with_suffix("")) + "_try")
            + '"${AIRFLOW_CTX_TRY_NUMBER}"'
            + shlex.quote(output.suffix)
        )
        commands.append(
            "cd /opt/airflow && "
            f"{alert_guard}"
            "python dags/scripts/run_whoscored_backfill_item.py "
            f"--queue-id {shlex.quote(queue_id)} "
            f"--plan-id {shlex.quote(plan_id)} "
            f"--work-item {shlex.quote(encoded)} "
            f"--output {output_argument} "
            f"{transport.cli_args(work_item_id=work_item_id)}"
        )
    return commands


def validate_backfill_batch(
    *,
    plan_ref: Mapping[str, Any],
    **_context: Any,
) -> Dict[str, Any]:
    """Return compact durable progress; incomplete plans continue next DagRun."""

    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    state = WhoScoredBackfillState.from_env()
    queue_id = str(plan_ref["queue_id"])
    plan_id = str(plan_ref["plan_id"])
    progress = state.advance_batch(
        queue_id,
        plan_id,
        batch_id=_controller_batch_id(_context),
    )
    if progress["next_work_items"] > MAX_WORK_ITEMS_PER_RUN:
        raise AirflowException(f"unbounded WhoScored backfill controller: {progress}")
    plan = state.load_plan(queue_id, plan_id)
    return {**progress, "slo": _backfill_slo_summary(plan, progress)}


def _continuation_transport_conf(transport: PaidRuntime) -> Dict[str, Any]:
    if transport.is_paid:
        raise AirflowException(
            "paid authority is bound to one exact DagRun and cannot be continued"
        )
    return {
        "transport_policy": transport.policy,
        "direct_only": True,
        "require_zero_paid": True,
    }


_CONTINUATION_REQUIRED_TASK_IDS = frozenset(
    {
        "run_whoscored_backfill_item",
        "validate_whoscored_backfill_batch",
        "validate_global_historical_dq",
        "report_whoscored_backfill_traffic",
    }
)


def _require_successful_continuation_upstreams(context: Mapping[str, Any]) -> None:
    """Refuse a new DagRun unless every source and terminal DQ task succeeded."""
    dag_run = context.get("dag_run")
    if dag_run is None or not callable(getattr(dag_run, "get_task_instances", None)):
        raise AirflowException("continuation requires DagRun task-state context")
    observed: dict[str, list[str]] = {
        task_id: [] for task_id in _CONTINUATION_REQUIRED_TASK_IDS
    }
    for item in dag_run.get_task_instances():
        if item.task_id not in observed:
            continue
        observed[item.task_id].append(str(item.state or "none").lower().split(".")[-1])
    failures = [
        f"{task_id}={states or ['missing']}"
        for task_id, states in sorted(observed.items())
        if not states or any(state != "success" for state in states)
    ]
    if failures:
        raise AirflowException(
            "WhoScored continuation blocked by unsuccessful upstreams: "
            + ", ".join(failures)
        )


def schedule_backfill_continuation(
    *,
    plan_ref: Mapping[str, Any],
    progress_ref: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    """Idempotently queue the next bounded DagRun for an incomplete plan."""
    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    state = WhoScoredBackfillState.from_env()
    queue_id = str(plan_ref["queue_id"])
    plan_id = str(plan_ref["plan_id"])
    progress = dict(progress_ref or state.checkpoint_progress(queue_id, plan_id))
    plan = state.load_plan(queue_id, plan_id)
    # A capacity breach is not remediated by consuming an impossible queue.
    # Stop the continuation chain until measured throughput or the explicitly
    # enforced topology changes; the immutable frontier remains resumable.
    slo = _backfill_slo_summary(plan, progress, enforce_capacity=False)
    if slo["capacity_status"] == "breach":
        raise AirflowException(
            f"WhoScored continuation stopped by 30-day capacity preflight: {slo}"
        )
    if progress["status"] == "complete":
        return {**progress, "slo": slo, "continuation": "not_required"}
    if not progress["next_work_items"]:
        raise AirflowException(
            f"incomplete WhoScored plan has no schedulable work: {progress}"
        )
    _require_successful_continuation_upstreams(context)

    transport = _transport_runtime(context)
    if transport.is_paid:
        if transport.approval is None:
            raise AirflowException("paid continuation has no verified approval")
        try:
            campaign = paid_campaign_gateway_call(transport.approval, "snapshot")
        except WhoScoredProxyRuntimeError as exc:
            raise AirflowException(
                f"WhoScored paid continuation campaign gateway is invalid: {exc}"
            ) from exc
        campaign_status = str(campaign.get("status") or "")
        if campaign_status == "revoked":
            raise AirflowException("WhoScored paid campaign is revoked")
        if campaign_status not in {"active", "awaiting_approval", "complete"}:
            raise AirflowException(
                f"WhoScored paid campaign has invalid state: {campaign_status!r}"
            )
        # Campaign state is bound to the current signed DagRun. Reusing the
        # approval in the deterministic continuation would either violate the
        # replay boundary or fail its run-id claim, so a new approval is always
        # required for the next DagRun.
        return {
            **progress,
            "slo": slo,
            "continuation": "awaiting_approval",
            "campaign_id": transport.campaign_id,
            "approval_id": transport.approval_id,
            "campaign_status": campaign_status,
            "awaiting_reason": "new_dagrun_requires_new_approval",
        }

    dag_run = context.get("dag_run")
    parent_run_id = str(context.get("run_id") or getattr(dag_run, "run_id", "") or "")
    if not parent_run_id:
        raise AirflowException("continuation requires the parent DagRun identity")
    raw_parent_conf = getattr(dag_run, "conf", None)
    parent_conf = dict(raw_parent_conf) if isinstance(raw_parent_conf, Mapping) else {}
    start_receipts = int(parent_conf.get("start_receipts") or 0)
    made_progress = int(progress["successful_receipts"]) > start_receipts
    no_progress_runs = (
        0 if made_progress else int(parent_conf.get("no_progress_runs") or 0) + 1
    )
    try:
        maximum_no_progress = int(
            os.environ.get("WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS", "3")
        )
    except ValueError as exc:
        raise AirflowException(
            "invalid WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS"
        ) from exc
    if not 1 <= maximum_no_progress <= 10:
        raise AirflowException(
            "WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS must be in 1..10"
        )
    if no_progress_runs > maximum_no_progress:
        raise AirflowException(
            "WhoScored automatic continuation stopped after "
            f"{no_progress_runs - 1} no-progress runs"
        )
    ti = context.get("ti")
    if not made_progress and ti is not None and int(getattr(ti, "try_number", 1)) == 1:
        raise AirflowException(
            "WhoScored no-progress continuation is backing off before retry"
        )
    parent_digest = hashlib.sha256(parent_run_id.encode("utf-8")).hexdigest()[:20]
    run_id = f"backfill__{queue_id}__{plan_id[:16]}__after_{parent_digest}"
    conf = {
        "queue_id": queue_id,
        "plan_id": plan_id,
        "parent_run_id": parent_run_id,
        "start_receipts": int(progress["successful_receipts"]),
        "no_progress_runs": no_progress_runs,
        **_continuation_transport_conf(transport),
    }
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models.dagrun import DagRun

    try:
        trigger_dag(
            dag_id="dag_backfill_whoscored",
            run_id=run_id,
            conf=conf,
            replace_microseconds=False,
        )
        continuation = "scheduled"
    except DagRunAlreadyExists:
        existing = DagRun.find(dag_id="dag_backfill_whoscored", run_id=run_id)
        if len(existing) != 1 or dict(existing[0].conf or {}) != conf:
            raise AirflowException(f"continuation DagRun identity conflict: {run_id}")
        continuation = "already_scheduled"
    return {
        **progress,
        "slo": slo,
        "continuation": continuation,
        "continuation_run_id": run_id,
        "made_progress": made_progress,
        "no_progress_runs": no_progress_runs,
    }


def _global_historical_integrity_summary(
    scopes: Optional[list[str]] = None,
    *,
    catalog_batch_id: Optional[str] = None,
    frozen_population: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Validate all or an explicit frozen scope set without one giant plan."""

    from scrapers.whoscored.parsers import MATCH_AVAILABILITY_VERSION, PARSER_VERSION
    from scrapers.whoscored.repository import (
        MATCH_DATASET_TABLES,
        PREVIEW_DATASET_TABLES,
        SCOPE_DATASET_TABLES,
    )
    from scrapers.base.trino_manager import get_trino_connection

    parser_sql = "'" + PARSER_VERSION.replace("'", "''") + "'"
    availability_sql = "'" + MATCH_AVAILABILITY_VERSION.replace("'", "''") + "'"
    scope_pairs: list[tuple[str, str]] = []
    if scopes:
        from dags.scripts.run_whoscored_scraper import RunnerScope

        scope_pairs = [
            (item.competition_id, item.season_id)
            for item in (RunnerScope.parse(value) for value in scopes)
        ]
    if len(scope_pairs) != len(set(scope_pairs)):
        raise AirflowException("WhoScored historical DQ scopes are not unique")
    if frozen_population is not None and not scope_pairs:
        raise AirflowException("frozen WhoScored historical DQ requires scopes")

    def scope_filter(league: str, season: str) -> str:
        if not scope_pairs:
            return ""
        values = " OR ".join(
            f"({league}={_sql_string(scope_league)} AND "
            f"{season}={_sql_string(scope_season)})"
            for scope_league, scope_season in scope_pairs
        )
        return f" AND ({values})"

    if scope_pairs and frozen_population is None:
        eligible_sql = (
            "SELECT * FROM (VALUES "
            + ",".join(
                f"({_sql_string(league)}, {_sql_string(season)})"
                for league, season in scope_pairs
            )
            + ") AS frozen(league, season)"
        )
    elif scope_pairs:
        from dags.scripts.whoscored_frozen_dq import (
            FrozenDQError,
            staged_scope_relation_sql,
        )

        staged_relation = frozen_population.get("staged_relation")
        if not isinstance(staged_relation, Mapping):
            raise AirflowException(
                "frozen WhoScored scope DQ requires a staged relation"
            )
        try:
            eligible_sql = staged_scope_relation_sql(staged_relation)
        except FrozenDQError as exc:
            raise AirflowException(str(exc)) from exc
    else:
        eligible_filter = scope_filter("c.competition_id", "s.season_id")
        eligible_sql = f"""
            SELECT DISTINCT c.competition_id AS league, s.season_id AS season
            FROM iceberg.bronze.whoscored_competitions_current c
            JOIN iceberg.bronze.whoscored_seasons_current s
              ON s.competition_id = c.competition_id
            WHERE c.eligibility = 'included'
              AND s.eligibility = 'included'
              {eligible_filter}
        """
    manifest_filter = (
        "" if frozen_population is not None else scope_filter("league", "season")
    )
    dynamic_population_filter = " AND 1=0" if frozen_population is not None else ""
    entity_manifest_filter = manifest_filter + dynamic_population_filter
    roster_filter = (
        "" if frozen_population is not None else scope_filter("league", "season")
    ) + dynamic_population_filter
    catalog_manifest_filter = (
        f"WHERE batch_id={_sql_string(catalog_batch_id)} AND state='success'"
        if catalog_batch_id
        else "WHERE state='success' ORDER BY completed_at DESC LIMIT 1"
    )
    frozen_summary: dict[str, int] = {}
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            # The mutable-history query is retained for the standalone global
            # audit. A terminal backfill instead gets every match/profile
            # counter from the immutable receipt population below, so only
            # the bounded scope-coverage proof is needed here.
            coverage_sql = f"""
                WITH eligible AS (
                    {eligible_sql}
                ),
                scope_success AS (
                    SELECT DISTINCT league, season
                    FROM iceberg.bronze.whoscored_scope_ingest_latest_success
                    WHERE entity_group = 'season'
                      AND parser_version = {parser_sql}
                      AND batch_id LIKE 'wss2-%'
                      AND raw_uris_json IS NOT NULL
                ),
                completed AS (
                    SELECT DISTINCT s.league, s.season,
                           CAST(s.game_id AS BIGINT) AS game_id
                    FROM iceberg.bronze.whoscored_schedule_current s
                    JOIN eligible e ON e.league=s.league AND e.season=s.season
                    WHERE s.status = 6 OR (
                        s.status = 1 AND s.home_score IS NOT NULL
                        AND s.away_score IS NOT NULL
                        AND s.date <= CAST(
                            CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                        )
                    )
                    {dynamic_population_filter}
                ),
                eligible_players AS (
                    SELECT DISTINCT CAST(r.player_id AS BIGINT) AS player_id
                    FROM iceberg.bronze.whoscored_player_roster r
                    JOIN eligible e ON e.league=r.league AND e.season=r.season
                    WHERE r.player_id IS NOT NULL {dynamic_population_filter}
                ),
                latest_match AS (
                    SELECT m.*
                    FROM iceberg.bronze.whoscored_match_ingest_latest m
                    JOIN eligible e ON e.league=m.league AND e.season=m.season
                ),
                valid_match AS (
                    SELECT league, season, game_id
                    FROM latest_match
                    WHERE parser_version = {parser_sql}
                      AND availability_version = {availability_sql} AND (
                        (
                            state = 'success' AND batch_id LIKE 'ws2-%'
                            AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL
                        ) OR (
                            state = 'not_available'
                            AND failure_code IS NOT NULL
                            AND (raw_uri IS NOT NULL OR http_status IN (404, 410))
                        )
                    )
                ),
                required_preview AS (
                    SELECT DISTINCT s.league, s.season,
                           CAST(s.game_id AS BIGINT) AS game_id
                    FROM iceberg.bronze.whoscored_schedule_current s
                    JOIN completed c
                      ON c.league=s.league AND c.season=s.season
                     AND c.game_id=CAST(s.game_id AS BIGINT)
                    WHERE s.has_preview=TRUE
                ),
                latest_preview AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY m.league, m.season, m.game_id
                            ORDER BY COALESCE(
                                m.completed_at, m.fetched_at, m._ingested_at
                            ) DESC, COALESCE(m.batch_id, '') DESC
                        ) rn
                        FROM iceberg.bronze.whoscored_preview_ingest_manifest m
                        JOIN eligible e
                          ON e.league=m.league AND e.season=m.season
                    ) WHERE rn=1
                ),
                valid_preview AS (
                    SELECT league, season, CAST(game_id AS BIGINT) AS game_id
                    FROM latest_preview
                    WHERE parser_version={parser_sql} AND (
                        (state='success'
                         AND batch_id LIKE 'wsp2-%'
                         AND raw_uri IS NOT NULL
                         AND payload_sha256 IS NOT NULL)
                        OR
                        (state='not_available'
                         AND availability_version={availability_sql}
                         AND failure_code IS NOT NULL
                         AND (raw_uri IS NOT NULL OR http_status IN (404, 410)))
                    )
                ),
                latest_profile AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY m.player_id ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, COALESCE(_profile_batch_id, '') DESC
                        ) rn
                        FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                        JOIN eligible_players e
                          ON e.player_id=CAST(m.player_id AS BIGINT)
                    ) WHERE rn = 1
                ),
                valid_profile AS (
                    SELECT CAST(player_id AS BIGINT) AS player_id
                    FROM latest_profile
                    WHERE parser_version = {parser_sql} AND (
                        (state = 'success'
                         AND _profile_batch_id LIKE 'wspr2-%'
                         AND raw_uri IS NOT NULL
                         AND payload_sha256 IS NOT NULL)
                        OR
                        (state='not_available'
                         AND availability_version={availability_sql}
                         AND failure_code IS NOT NULL
                         AND (raw_uri IS NOT NULL OR http_status IN (404, 410)))
                    )
                )
                SELECT
                    (SELECT COUNT(*) FROM eligible),
                    (SELECT COUNT(*) FROM eligible e LEFT JOIN scope_success s
                     ON s.league = e.league AND s.season = e.season
                     WHERE s.league IS NULL),
                    (SELECT COUNT(*) FROM completed),
                    (SELECT COUNT(*) FROM completed c LEFT JOIN valid_match m
                     ON m.league = c.league AND m.season = c.season
                    AND m.game_id = c.game_id WHERE m.game_id IS NULL),
                    (SELECT COUNT(*) FROM latest_match WHERE state = 'parse_failed'),
                    (SELECT COUNT(*) FROM latest_match WHERE state = 'retryable'),
                    (SELECT COUNT(*) FROM latest_match WHERE state = 'terminal'),
                    (SELECT COUNT(*) FROM latest_match
                     WHERE state = 'not_available' AND (
                        availability_version IS DISTINCT FROM {availability_sql}
                        OR failure_code IS NULL
                        OR (raw_uri IS NULL AND (
                            http_status IS NULL OR http_status NOT IN (404, 410)
                        ))
                     )),
                    (SELECT COUNT(*) FROM required_preview),
                    (SELECT COUNT(*) FROM required_preview r LEFT JOIN valid_preview p
                     ON p.league=r.league AND p.season=r.season
                    AND p.game_id=r.game_id WHERE p.game_id IS NULL),
                    (SELECT COUNT(*) FROM latest_preview WHERE state='parse_failed'),
                    (SELECT COUNT(*) FROM latest_preview WHERE state='retryable'),
                    (SELECT COUNT(*) FROM latest_preview WHERE state='terminal'),
                    (SELECT COUNT(*) FROM latest_profile p JOIN eligible_players e
                     ON e.player_id=CAST(p.player_id AS BIGINT)
                     WHERE p.state = 'parse_failed'),
                    (SELECT COUNT(*) FROM latest_profile p JOIN eligible_players e
                     ON e.player_id=CAST(p.player_id AS BIGINT)
                     WHERE p.state = 'retryable'),
                    (SELECT COUNT(*) FROM latest_profile p JOIN eligible_players e
                     ON e.player_id=CAST(p.player_id AS BIGINT)
                     WHERE p.state = 'terminal'),
                    (SELECT COUNT(*) FROM eligible_players e LEFT JOIN valid_profile p
                     ON p.player_id=e.player_id WHERE p.player_id IS NULL)
                """
            if frozen_population is not None:
                cur.execute(
                    f"""
                    WITH eligible AS (
                        {eligible_sql}
                    ),
                    scope_success AS (
                        SELECT DISTINCT m.league, m.season
                        FROM
                          iceberg.bronze.whoscored_scope_ingest_latest_success m
                        JOIN eligible e
                          ON e.league=m.league AND e.season=m.season
                        WHERE m.entity_group='season'
                          AND m.parser_version={parser_sql}
                          AND m.batch_id LIKE 'wss2-%'
                          AND m.raw_uris_json IS NOT NULL
                    )
                    SELECT
                        (SELECT COUNT(*) FROM eligible),
                        (SELECT COUNT(*) FROM eligible e
                         LEFT JOIN scope_success s
                           ON s.league=e.league AND s.season=e.season
                         WHERE s.league IS NULL),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT), CAST(0 AS BIGINT),
                        CAST(0 AS BIGINT)
                    """
                )
                values = cur.fetchall()
                if len(values) != 1 or len(values[0]) != 17:
                    raise AirflowException(
                        "frozen scope coverage query returned an invalid row"
                    )
                row = tuple(int(value or 0) for value in values[0])
            else:
                cur.execute(coverage_sql)
                row = cur.fetchall()[0]

            parity: dict[str, dict[str, int]] = {}

            # Split per dataset on purpose. A single UNION/CTE across all 25
            # datasets exceeded Trino's query-stage limit in production.
            for table in sorted(SCOPE_DATASET_TABLES):
                key = table
                eligible_cte = ""
                eligible_join = ""
                if frozen_population is not None:
                    eligible_cte = f"eligible AS ({eligible_sql}),"
                    eligible_join = (
                        "JOIN eligible e ON e.league=m.league AND e.season=m.season"
                    )
                cur.execute(
                    f"""
                    WITH {eligible_cte}
                    latest AS (
                        SELECT m.*
                        FROM
                          iceberg.bronze.whoscored_scope_ingest_latest_success m
                        {eligible_join}
                        WHERE m.entity_group = 'season'
                          AND m.parser_version = {parser_sql}
                          AND m.batch_id LIKE 'wss2-%'
                          AND m.raw_uris_json IS NOT NULL
                          {manifest_filter}
                    ), owners AS (
                        SELECT league,season,batch_id,
                               COALESCE(TRY_CAST(json_extract_scalar(
                                   TRY(json_parse(entity_counts_json)),
                                   '$.{key}'
                               ) AS BIGINT),0) expected_rows
                        FROM latest
                    ), physical AS (
                        SELECT m.league,m.season,m.batch_id,
                               COUNT(d._scope_batch_id) actual_rows
                        FROM owners m
                        LEFT JOIN iceberg.bronze.{table} d
                          ON m.league=d.league AND m.season=d.season
                         AND m.batch_id=d._scope_batch_id
                        GROUP BY 1,2,3
                    ), current_rows AS (
                        SELECT m.league,m.season,m.batch_id,
                               COUNT(d._scope_batch_id) actual_rows
                        FROM owners m
                        LEFT JOIN iceberg.bronze.{table}_current d
                          ON m.league=d.league AND m.season=d.season
                         AND m.batch_id=d._scope_batch_id
                        GROUP BY 1,2,3
                    )
                    SELECT
                        COALESCE(SUM(m.expected_rows),0),
                        COALESCE(SUM(p.actual_rows),0),
                        COALESCE(SUM(c.actual_rows),0),
                        COUNT_IF(m.expected_rows<>COALESCE(p.actual_rows,0)
                              OR m.expected_rows<>COALESCE(c.actual_rows,0))
                    FROM owners m
                    LEFT JOIN physical p
                      ON p.league=m.league AND p.season=m.season
                     AND p.batch_id=m.batch_id
                    LEFT JOIN current_rows c
                      ON c.league=m.league AND c.season=m.season
                     AND c.batch_id=m.batch_id
                    """
                )
                values = cur.fetchall()
                if len(values) != 1 or len(values[0]) != 4:
                    raise AirflowException(
                        "frozen scope parity query returned an invalid row"
                    )
                parity[table] = {
                    "manifest": int(values[0][0] or 0),
                    "physical": int(values[0][1] or 0),
                    "current": int(values[0][2] or 0),
                    "owner_mismatches": int(values[0][3] or 0),
                }

            for entity, table in (
                () if frozen_population is not None else MATCH_DATASET_TABLES.items()
            ):
                cur.execute(
                    f"""
                    WITH latest AS (
                        SELECT *
                        FROM iceberg.bronze.whoscored_match_ingest_latest_success
                        WHERE parser_version = {parser_sql}
                          AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                          {entity_manifest_filter}
                    )
                    SELECT
                        COALESCE(SUM(CAST(json_extract_scalar(
                            json_parse(entity_counts_json), '$.{entity}'
                        ) AS BIGINT)), 0),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table} d JOIN latest m
                         ON m.league=d.league AND m.season=d.season
                        AND m.game_id=CAST(d.game_id AS BIGINT)
                        AND m.batch_id=d._game_batch_id),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table}_current d
                         JOIN latest m ON m.league=d.league AND m.season=d.season
                        AND m.game_id=CAST(d.game_id AS BIGINT)
                        AND m.batch_id=d._game_batch_id)
                    FROM latest
                    """
                )
                values = cur.fetchall()[0]
                parity[table] = {
                    "manifest": int(values[0] or 0),
                    "physical": int(values[1] or 0),
                    "current": int(values[2] or 0),
                }

            for entity, table in (
                () if frozen_population is not None else PREVIEW_DATASET_TABLES.items()
            ):
                cur.execute(
                    f"""
                    WITH latest AS (
                        SELECT * FROM
                            iceberg.bronze.whoscored_preview_ingest_latest_success
                        WHERE parser_version = {parser_sql}
                          AND batch_id LIKE 'wsp2-%' AND raw_uri IS NOT NULL
                          {entity_manifest_filter}
                    )
                    SELECT
                        COALESCE(SUM(CAST(json_extract_scalar(
                            json_parse(entity_counts_json), '$.{entity}'
                        ) AS BIGINT)), 0),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table} d JOIN latest m
                         ON m.league=d.league AND m.season=d.season
                        AND m.game_id=CAST(d.game_id AS BIGINT)
                        AND m.batch_id=d._preview_batch_id),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table}_current d
                         JOIN latest m ON m.league=d.league AND m.season=d.season
                        AND m.game_id=CAST(d.game_id AS BIGINT)
                        AND m.batch_id=d._preview_batch_id)
                    FROM latest
                    """
                )
                values = cur.fetchall()[0]
                parity[table] = {
                    "manifest": int(values[0] or 0),
                    "physical": int(values[1] or 0),
                    "current": int(values[2] or 0),
                }

            # Catalog datasets are one atomic manifest-backed snapshot.
            for entity, table in (
                ("competitions", "whoscored_competitions"),
                ("seasons", "whoscored_seasons"),
                ("stages", "whoscored_stages"),
            ):
                cur.execute(
                    f"""
                    WITH latest AS (
                        SELECT * FROM iceberg.bronze.whoscored_catalog_manifest
                        {catalog_manifest_filter}
                    )
                    SELECT
                        COALESCE(MAX({entity}_count), 0),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table} d JOIN latest m
                         ON m.batch_id=d._catalog_batch_id),
                        (SELECT COUNT(*) FROM iceberg.bronze.{table} d JOIN latest m
                         ON m.batch_id=d._catalog_batch_id)
                    FROM latest
                    """
                )
                values = cur.fetchall()[0]
                parity[table] = {
                    "manifest": int(values[0] or 0),
                    "physical": int(values[1] or 0),
                    "current": int(values[2] or 0),
                }

            profile_sql = f"""
                WITH latest AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY player_id ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, _profile_batch_id DESC
                        ) rn
                        FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                        WHERE state='success' AND parser_version={parser_sql}
                          AND _profile_batch_id LIKE 'wspr2-%'
                          AND raw_uri IS NOT NULL AND payload_sha256 IS NOT NULL
                          AND player_id IN (
                              SELECT DISTINCT CAST(player_id AS BIGINT)
                              FROM iceberg.bronze.whoscored_player_roster
                              WHERE player_id IS NOT NULL {roster_filter}
                          )
                    ) WHERE rn=1
                )
                SELECT
                    (SELECT COUNT(*) FROM latest),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_player_profile_versions p
                     JOIN latest m ON m.player_id=p.player_id
                      AND m._profile_batch_id=p._profile_batch_id),
                    (SELECT COUNT(*)
                     FROM iceberg.silver.whoscored_player_profile_current
                     WHERE player_id IN (
                         SELECT DISTINCT CAST(player_id AS BIGINT)
                         FROM iceberg.bronze.whoscored_player_roster
                         WHERE player_id IS NOT NULL {roster_filter}
                     )),
                    (SELECT COALESCE(SUM(participations_count),0) FROM latest),
                    (SELECT COUNT(*) FROM
                       iceberg.bronze.whoscored_player_stage_participations p
                     JOIN latest m ON m.player_id=p.player_id
                      AND m._profile_batch_id=p._profile_batch_id),
                    (SELECT COUNT(*) FROM
                       iceberg.bronze.whoscored_player_stage_participations_current
                     WHERE player_id IN (
                         SELECT DISTINCT CAST(player_id AS BIGINT)
                         FROM iceberg.bronze.whoscored_player_roster
                         WHERE player_id IS NOT NULL {roster_filter}
                     ))
                """
            if frozen_population is None:
                cur.execute(profile_sql)
                profile_row = cur.fetchall()[0]
            else:
                profile_row = (0, 0, 0, 0, 0, 0)
            parity["whoscored_player_profile_versions"] = {
                "manifest": int(profile_row[0] or 0),
                "physical": int(profile_row[1] or 0),
                "current": int(profile_row[2] or 0),
            }
            parity["whoscored_player_stage_participations"] = {
                "manifest": int(profile_row[3] or 0),
                "physical": int(profile_row[4] or 0),
                "current": int(profile_row[5] or 0),
            }
            if frozen_population is not None:
                from dags.scripts.whoscored_frozen_dq import (
                    FrozenDQError,
                    frozen_historical_integrity,
                )

                try:
                    frozen_summary, frozen_parity = frozen_historical_integrity(
                        cur,
                        frozen_population,
                        parser_version=PARSER_VERSION,
                        availability_version=MATCH_AVAILABILITY_VERSION,
                        match_dataset_tables=MATCH_DATASET_TABLES,
                        preview_dataset_tables=PREVIEW_DATASET_TABLES,
                    )
                except FrozenDQError as exc:
                    raise AirflowException(str(exc)) from exc
                parity.update(frozen_parity)
        finally:
            cur.close()
    finally:
        conn.close()

    mismatches = sorted(
        table
        for table, counts in parity.items()
        if counts.get("manifest") != counts.get("physical")
        or counts.get("manifest") != counts.get("current")
        or int(counts.get("owner_mismatches") or 0) != 0
    )
    summary = {
        "eligible_scopes": int(row[0] or 0),
        "uncovered_eligible_scopes": int(row[1] or 0),
        "completed_matches": int(row[2] or 0),
        "uncovered_completed_matches": int(row[3] or 0),
        "parse_failed_matches": int(row[4] or 0),
        "retryable_matches": int(row[5] or 0),
        "terminal_matches": int(row[6] or 0),
        "unproven_not_available_matches": int(row[7] or 0),
        "required_previews": int(row[8] or 0),
        "uncovered_previews": int(row[9] or 0),
        "parse_failed_previews": int(row[10] or 0),
        "retryable_previews": int(row[11] or 0),
        "terminal_previews": int(row[12] or 0),
        "failed_previews": sum(int(row[index] or 0) for index in (10, 11, 12)),
        "parse_failed_profiles": int(row[13] or 0),
        "retryable_profiles": int(row[14] or 0),
        "terminal_profiles": int(row[15] or 0),
        "uncovered_eligible_profiles": int(row[16] or 0),
        "failed_profiles": sum(int(row[index] or 0) for index in (13, 14, 15)),
        "dq_scope": f"{len(scope_pairs)} frozen scopes",
        "dataset_count": len(parity),
        "dataset_parity_mismatches": len(mismatches),
        "mismatched_datasets": mismatches,
    }
    summary.update(frozen_summary)
    return summary


def _catalog_snapshot_scope_summary(plan: Mapping[str, Any]) -> Dict[str, Any]:
    """Prove the complete physical catalog payload pinned by the plan."""

    provenance = plan.get("provenance")
    if not isinstance(provenance, Mapping):
        raise AirflowException("backfill plan has no catalog provenance")
    batch_id = str(provenance.get("catalog_batch_id") or "")
    if not batch_id:
        raise AirflowException("backfill plan has no catalog batch id")
    from dags.scripts import run_whoscored_scraper as runner
    from scrapers.whoscored.repository import catalog_payload_sha256

    repository = runner._new_repository()
    catalog = repository.load_discovered_catalog(batch_id=batch_id)
    payload_sha256 = catalog_payload_sha256(catalog.to_rows())
    expected_payload_sha256 = str(provenance.get("catalog_payload_sha256") or "")
    if not expected_payload_sha256 or payload_sha256 != expected_payload_sha256:
        raise AirflowException(
            "WhoScored frozen catalog payload proof failed: "
            f"batch={batch_id}, expected={expected_payload_sha256 or 'missing'}, "
            f"actual={payload_sha256}"
        )
    frozen_catalog_scopes = {
        item.scope.spec for item in catalog.eligible_scopes(active_only=False)
    }
    plan_scopes = set(str(value) for value in plan.get("scopes", []))
    missing = sorted(plan_scopes - frozen_catalog_scopes)
    extra = sorted(frozen_catalog_scopes - plan_scopes)
    all_catalog = bool(plan.get("selector", {}).get("all_catalog"))
    digest = hashlib.sha256(
        ("\n".join(sorted(frozen_catalog_scopes)) + "\n").encode("utf-8")
    ).hexdigest()
    expected_count = provenance.get("catalog_eligible_scope_count")
    expected_digest = provenance.get("catalog_eligible_scopes_sha256")
    if (
        missing
        or (all_catalog and extra)
        or type(expected_count) is not int
        or expected_count != len(frozen_catalog_scopes)
        or expected_digest != digest
    ):
        raise AirflowException(
            "WhoScored frozen catalog scope proof failed: "
            f"batch={batch_id}, missing={missing[:20]}, extra={extra[:20]}, "
            f"expected_count={expected_count}, actual_count={len(frozen_catalog_scopes)}"
        )
    return {
        "catalog_batch_id": batch_id,
        "catalog_eligible_scopes": len(frozen_catalog_scopes),
        "catalog_eligible_scopes_sha256": digest,
        "catalog_payload_sha256": payload_sha256,
        "plan_scopes": len(plan_scopes),
        "catalog_scope_mode": "exact" if all_catalog else "subset",
    }


def validate_global_historical_dq(
    *,
    plan_ref: Mapping[str, Any],
    progress_ref: Optional[Mapping[str, Any]] = None,
    **_context: Any,
) -> Dict[str, Any]:
    """Run global cutover DQ after the all-catalog plan is complete."""

    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    state = WhoScoredBackfillState.from_env()
    queue_id = str(plan_ref["queue_id"])
    plan_id = str(plan_ref["plan_id"])
    progress = dict(progress_ref or state.checkpoint_progress(queue_id, plan_id))
    plan = state.load_plan(queue_id, plan_id)
    if progress["status"] != "complete":
        return {**progress, "status": "deferred"}
    all_catalog = bool(plan.get("selector", {}).get("all_catalog"))
    provenance = plan.get("provenance", {})
    if all_catalog and (
        not bool(provenance.get("full_history_discovery"))
        or provenance.get("catalog_discovery_mode") != "full_history"
        or not provenance.get("catalog_batch_id")
    ):
        raise AirflowException(
            "all-catalog historical DQ requires full-history catalog provenance"
        )

    population = _frozen_dq_population(state, plan, progress)
    from dags.scripts.whoscored_frozen_dq import (
        FrozenDQError,
        cleanup_staged_frozen_populations,
        stage_frozen_population,
    )

    try:
        staged_relation = stage_frozen_population(population)
    except FrozenDQError as exc:
        raise AirflowException(str(exc)) from exc
    snapshot = _catalog_snapshot_scope_summary(plan)
    # Release the multi-million-key Python objects before the set-based Trino
    # scans. Correctness is now pinned to one immutable Iceberg snapshot and
    # the content-addressed population artifact retained below.
    population.pop("matches", None)
    population.pop("player_ids", None)
    feed_summary = _frozen_scope_feed_integrity(
        population,
        staged_relation=staged_relation,
    )
    population.pop("scope_stages", None)
    summary = _global_historical_integrity_summary(
        list(plan.get("scopes", [])),
        catalog_batch_id=str(provenance["catalog_batch_id"]),
        frozen_population={"staged_relation": staged_relation},
    )
    summary.update(snapshot)
    summary.update(feed_summary)
    summary.update(
        {
            "frozen_population_sha256": population["population_sha256"],
            "frozen_population_uri": population["artifact"]["uri"],
            "frozen_population_matches": population["counts"]["matches"],
            "frozen_population_previews": population["counts"]["previews"],
            "frozen_population_players": population["counts"]["players"],
        }
    )
    failures = {
        key: value
        for key, value in summary.items()
        if key
        in {
            "uncovered_eligible_scopes",
            "uncovered_completed_matches",
            "parse_failed_matches",
            "retryable_matches",
            "terminal_matches",
            "unproven_not_available_matches",
            "uncovered_previews",
            "parse_failed_previews",
            "retryable_previews",
            "terminal_previews",
            "failed_previews",
            "failed_profiles",
            "parse_failed_profiles",
            "retryable_profiles",
            "terminal_profiles",
            "uncovered_eligible_profiles",
            "missing_frozen_schedule_matches",
            "event_game_mismatches",
            "lineup_game_mismatches",
            "incomplete_final_opta_games",
            "uncovered_incident_summaries",
            "uncovered_bet_matches",
            "incomplete_match_snapshots",
            "invalid_event_identity_rows",
            "duplicate_source_event_ids",
            "duplicate_team_event_ids",
            "inconsistent_match_dataset_states",
            "incomplete_preview_snapshots",
            "inconsistent_preview_dataset_states",
            "unproven_not_available_profiles",
            "missing_scope_feed_manifests",
            "missing_feed_state_count",
            "extra_feed_state_count",
            "malformed_feed_state_count",
            "dataset_parity_mismatches",
        }
        and int(value)
    }
    if summary["dataset_count"] != 25:
        failures["dataset_count"] = summary["dataset_count"]
    if summary["eligible_scopes"] != len(plan.get("scopes", [])):
        failures["eligible_scopes"] = summary["eligible_scopes"]
    if int(progress.get("schedule_stage_cardinality_drifts") or 0):
        failures["schedule_stage_cardinality_drifts"] = int(
            progress["schedule_stage_cardinality_drifts"]
        )
    for actual_key, population_key in (
        ("completed_matches", "matches"),
        ("required_previews", "previews"),
        ("frozen_profile_players", "players"),
    ):
        expected = int(population["counts"][population_key])
        if int(summary.get(actual_key, -1)) != expected:
            failures[f"{actual_key}_population_mismatch"] = {
                "actual": summary.get(actual_key),
                "expected": expected,
            }
    if failures:
        raise AirflowException(
            f"WhoScored global historical DQ failed: {failures}; {summary}"
        )
    from scrapers.base.trino_manager import get_trino_connection

    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            try:
                cleaned = cleanup_staged_frozen_populations(
                    cur,
                    keep_population_sha256=str(population["population_sha256"]),
                )
            except FrozenDQError as exc:
                raise AirflowException(str(exc)) from exc
        finally:
            cur.close()
    finally:
        conn.close()
    summary["expired_frozen_dq_partitions"] = cleaned
    return {"status": "success", **summary}


def report_backfill_traffic(
    *,
    plan_ref: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    from dags.dag_ingest_whoscored import aggregate_traffic_reports

    discovery_required = bool(plan_ref and plan_ref.get("discovery_traffic_required"))
    expected_request = plan_ref.get("discovery_traffic_evidence") if plan_ref else None
    expected_report = plan_ref.get("discovery_report_attribution") if plan_ref else None
    request_reference_fields = {
        "uri",
        "key",
        "sha256",
        "bytes",
        "event_count",
        "request_count",
        "wire_bytes",
        "paid_proxy_bytes",
    }
    report_reference_fields = {
        "uri",
        "key",
        "sha256",
        "bytes",
        "paid_proxy_bytes",
    }
    if discovery_required and (
        not isinstance(expected_request, Mapping)
        or set(expected_request) != request_reference_fields
        or not isinstance(expected_report, Mapping)
        or set(expected_report) != report_reference_fields
    ):
        raise AirflowException(
            "WhoScored backfill discovery traffic reference is invalid"
        )
    if not discovery_required:
        return aggregate_traffic_reports(allow_empty=True, **context)
    assert isinstance(expected_request, Mapping)  # validated above
    assert isinstance(expected_report, Mapping)  # validated above
    identity = _discovery_traffic_identity(context)
    request_prefix = (
        "traffic/"
        f"{stable_safe_token(identity['dag_id'])}/"
        f"{stable_safe_token(identity['run_id'])}/request-ledgers/"
    )
    report_prefix = (
        "traffic/"
        f"{stable_safe_token(identity['dag_id'])}/"
        f"{stable_safe_token(identity['run_id'])}/report-attribution/"
    )
    request_key = expected_request.get("key")
    report_key = expected_report.get("key")
    if (
        not isinstance(request_key, str)
        or not request_key.startswith(request_prefix)
        or not isinstance(report_key, str)
        or not report_key.startswith(report_prefix)
    ):
        raise AirflowException(
            "WhoScored discovery traffic artifact belongs to another DagRun"
        )
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - guarded by optional=False
        raise AirflowException("WhoScored operational S3 store is required")
    request_evidence = ops_store.read_content_addressed_json(
        request_key,
        expected_sha256=str(expected_request.get("sha256") or ""),
        expected_bytes=int(expected_request.get("bytes") or 0),
    )
    report_evidence = ops_store.read_content_addressed_json(
        report_key,
        expected_sha256=str(expected_report.get("sha256") or ""),
        expected_bytes=int(expected_report.get("bytes") or 0),
    )
    for field in ("event_count", "request_count", "wire_bytes", "paid_proxy_bytes"):
        if request_evidence.get(field) != expected_request.get(field):
            raise AirflowException(
                "WhoScored discovery traffic artifact summary mismatch: " + field
            )
    if (
        report_evidence.get("paid_proxy_bytes")
        != expected_report.get("paid_proxy_bytes")
        or report_evidence.get("paid_proxy_bytes")
        != request_evidence.get("paid_proxy_bytes")
        or report_evidence.get("dag_id") != identity["dag_id"]
        or report_evidence.get("run_id") != identity["run_id"]
        or report_evidence.get("task_id") != "prepare_backfill_plan"
    ):
        raise AirflowException(
            "WhoScored discovery report/request attribution mismatch"
        )
    external_paid = report_evidence.get("paid_proxy_bytes")
    if type(external_paid) is not int or external_paid < 0:
        raise AirflowException(
            "WhoScored backfill discovery paid traffic reference is invalid"
        )
    if not _transport_runtime(context).is_paid and external_paid != 0:
        raise AirflowException("WhoScored backfill discovery used paid proxy")
    summary = aggregate_traffic_reports(
        allow_empty=True,
        additional_reported_paid_attribution=report_evidence,
        **context,
    )
    if int(summary.get("durable_request_ledgers") or 0) < 1:
        raise AirflowException(
            "WhoScored terminal traffic DQ omitted discovery evidence"
        )
    return summary


def enforce_backfill_gate(
    *,
    plan_ref: Optional[Mapping[str, Any]] = None,
    progress_ref: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    ti = context.get("ti")
    if dag_run is None:
        raise AirflowException("backfill terminal gate requires dag_run context")
    current = getattr(ti, "task_id", "final_success_gate")
    complete = False
    if plan_ref:
        from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

        progress = dict(progress_ref or {})
        if not progress:
            progress = WhoScoredBackfillState.from_env().checkpoint_progress(
                str(plan_ref["queue_id"]),
                str(plan_ref["plan_id"]),
            )
        complete = progress["status"] == "complete"
    failures = []
    for item in dag_run.get_task_instances():
        if item.task_id == current:
            continue
        state = str(item.state or "none").lower().split(".")[-1]
        if (
            complete
            and item.task_id == "run_whoscored_backfill_item"
            and state == "skipped"
        ):
            continue
        if state != "success":
            suffix = (
                f"[{item.map_index}]" if getattr(item, "map_index", -1) >= 0 else ""
            )
            failures.append(f"{item.task_id}{suffix}={state}")
    if failures:
        raise AirflowException(
            "WhoScored backfill tasks were not successful: " + ", ".join(failures)
        )
    return {"status": "success"}


with DAG(
    dag_id="dag_backfill_whoscored",
    default_args=BACKFILL_ARGS,
    description="Bounded WhoScored backfill with immutable S3 plan and receipts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    # Deduplicate mapped-worker failures into one notification per DagRun.
    on_failure_callback=SCRAPER_ARGS.get("on_failure_callback"),
    params={
        "scopes": Param(default=[], type="array", items={"type": "string"}),
        "game_ids": Param(default=[], type="array", items={"type": "integer"}),
        "date_from": Param(default=None, type=["null", "string"], format="date"),
        "date_to": Param(default=None, type=["null", "string"], format="date"),
        "queue_id": Param(default="", type="string", pattern="^[A-Za-z0-9_.-]{0,120}$"),
        "plan_id": Param(default="", type="string", pattern="^$|^[0-9a-f]{64}$"),
        "all_catalog": Param(default=False, type="boolean"),
        "transport_policy": Param(
            default="direct_only",
            type="string",
            enum=["direct_only", "direct_then_paid"],
        ),
        "paid_approval_id": Param(default="", type="string"),
        "paid_approval_sha256": Param(
            default="",
            type="string",
            pattern="^$|^[0-9a-f]{64}$",
        ),
        "require_zero_paid": Param(default=True, type="boolean"),
        "direct_only": Param(default=True, type="boolean"),
        "full_history_catalog": Param(default=False, type="boolean"),
    },
    user_defined_filters={"stable_safe_token": stable_safe_token},
    tags=DAG_TAGS.get("whoscored", ["scraping", "whoscored", "backfill"]),
    doc_md="""
    Supply explicit scopes or `all_catalog=true`. A DagRun commits at most 100
    immutable work receipts and queues a deterministic continuation with the
    exact `queue_id + plan_id` until all 25-match and 200-player chunks have
    durable success evidence. `all_catalog=true` always performs full-history
    discovery and binds its catalog generation, start, and hard 30-day
    deadline into the immutable plan. The controller fails closed when the
    exact request-unit lower bound exceeds the deployed source ceiling; the
    capacity setting is an SLO assumption and does not change runtime rate or
    concurrency limits.
    """,
) as dag:
    validate_params = PythonOperator(
        task_id="validate_backfill_selectors",
        python_callable=validate_backfill_params,
        execution_timeout=timedelta(minutes=5),
    )
    paid_alert_preflight = PythonOperator(
        task_id="validate_whoscored_paid_alert_delivery",
        python_callable=validate_transport_alert_delivery,
        retries=0,
        execution_timeout=timedelta(minutes=1),
    )
    prepare_plan = PythonOperator(
        task_id="prepare_backfill_plan",
        python_callable=prepare_backfill_plan,
        op_kwargs={"alert_metadata": paid_alert_preflight.output},
        pool=BACKFILL_POOL,
        # Discovery is serialized against mapped source workers by holding the
        # full validated source-pool capacity.
        pool_slots=BACKFILL_SOURCE_POOL_SLOTS,
        # In-process discovery persists one exact task/request report. Airflow
        # retry would retain provider bytes but replace that report; a later
        # manually approved DagRun resumes safely from raw storage instead.
        retries=0,
        execution_timeout=timedelta(hours=8),
    )
    build_commands = PythonOperator(
        task_id="build_backfill_work",
        python_callable=build_backfill_commands,
        op_kwargs={
            "plan_ref": prepare_plan.output,
            "alert_metadata": paid_alert_preflight.output,
        },
        execution_timeout=timedelta(minutes=10),
    )
    run_backfill = BashOperator.partial(
        task_id="run_whoscored_backfill_item",
        env=_TASK_ENV,
        append_env=True,
        pool=BACKFILL_POOL,
        pool_slots=1,
        priority_weight=10,
        retries=1,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=90),
        do_xcom_push=False,
    ).expand(bash_command=build_commands.output)
    backfill_dq = PythonOperator(
        task_id="validate_whoscored_backfill_batch",
        python_callable=validate_backfill_batch,
        op_kwargs={"plan_ref": prepare_plan.output},
        trigger_rule="all_done",
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=10),
    )
    historical_dq = PythonOperator(
        task_id="validate_global_historical_dq",
        python_callable=validate_global_historical_dq,
        op_kwargs={
            "plan_ref": prepare_plan.output,
            "progress_ref": backfill_dq.output,
        },
        trigger_rule="all_done",
        # Hold every production direct-writer slot for the complete multi-query
        # cut. This prevents daily manifests/business tables from changing
        # between the snapshot-pinned key proof and parity statements.
        pool=DIRECT_POOL,
        pool_slots=BACKFILL_SOURCE_POOL_SLOTS,
        execution_timeout=timedelta(hours=2),
    )
    traffic_dq = PythonOperator(
        task_id="report_whoscored_backfill_traffic",
        python_callable=report_backfill_traffic,
        op_kwargs={"plan_ref": prepare_plan.output},
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=10),
    )
    continue_backfill = PythonOperator(
        task_id="schedule_next_whoscored_backfill_batch",
        python_callable=schedule_backfill_continuation,
        op_kwargs={
            "plan_ref": prepare_plan.output,
            "progress_ref": backfill_dq.output,
        },
        trigger_rule="all_done",
        pool=DQ_POOL,
        retries=2,
        retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(minutes=10),
    )
    final_gate = PythonOperator(
        task_id="final_success_gate",
        python_callable=enforce_backfill_gate,
        op_kwargs={
            "plan_ref": prepare_plan.output,
            "progress_ref": backfill_dq.output,
        },
        trigger_rule="all_done",
    )

    validate_params >> paid_alert_preflight >> prepare_plan
    prepare_plan >> build_commands >> run_backfill
    run_backfill >> [backfill_dq, traffic_dq]
    backfill_dq >> historical_dq
    [run_backfill, backfill_dq, historical_dq, traffic_dq] >> continue_backfill
    continue_backfill >> final_gate
