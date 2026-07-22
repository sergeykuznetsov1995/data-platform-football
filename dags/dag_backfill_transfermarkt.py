"""Continuous, checkpointed Native-Bronze historical Transfermarkt backfill."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from scrapers.transfermarkt.models import (
    DEFAULT_ENTITY_TIMEOUT_SECONDS,
    MAX_ROSTER_WINDOW,
    MAX_SCOPE_BATCH,
    PARENT_DAILY_HARD_PROVIDER_BYTE_CAP,
    PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP,
    PARENT_REQUEST_LIMIT,
    PARENT_RETRY_LIMIT,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
    SCOPE_REQUEST_LIMIT,
    SCOPE_RETRY_LIMIT,
    SCOPE_SOFT_PROVIDER_BYTE_STOP,
    SCOPE_WALL_CLOCK_TIMEOUT_SECONDS,
)
from utils.default_args import SCRAPER_ARGS
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_backfill_attempts import (
    has_matching_scope_attempt_result,
)
from utils.transfermarkt_backfill_runtime import (
    BACKFILL_DAG_ID,
    BackfillRuntimeError,
    BackfillStateRepository,
    build_campaign_from_registry,
    claim_and_plan,
    plan_existing_batch,
    read_promoted_registry,
    select_recoverable_batch,
    strict_cutover_preflight,
)


STANDING_BACKFILL_POLICY_PATH = (
    "/opt/airflow/dags/configs/transfermarkt/standing_backfill_policy.json"
)
CHECKPOINT_TTL_DAYS = 35
COACH_HISTORY_TTL_DAYS = 28
BACKFILL_CONTROL_POOL = "transfermarkt_backfill_control"
ACTIVE_RUN_COOLDOWN = timedelta(seconds=30)
IDLE_POLL_INTERVAL = timedelta(hours=1)
FAILED_RUN_COOLDOWN = timedelta(minutes=5)


def _publish_next_poll(
    ti: Any,
    *,
    now: datetime,
    scopes: tuple[state.BackfillScopeState, ...] = (),
    idle: bool = False,
) -> None:
    target = now + (IDLE_POLL_INTERVAL if idle else ACTIVE_RUN_COOLDOWN)
    retry_times = tuple(
        item.next_retry_at
        for item in scopes
        if item.next_retry_at is not None and item.next_retry_at > now
    )
    if retry_times:
        target = min(target, min(retry_times))
    target = max(target, now + ACTIVE_RUN_COOLDOWN)
    push = getattr(ti, "xcom_push", None)
    if callable(push):
        push(key="next_poll_at", value=target.isoformat())


def _backfill_poll_ready(**context: Any) -> bool:
    ti = context["ti"]
    raw_target = ti.xcom_pull(
        task_ids="plan_historical_batch", key="next_poll_at"
    )
    if raw_target:
        try:
            target = datetime.fromisoformat(str(raw_target).replace("Z", "+00:00"))
        except ValueError as exc:
            raise AirflowException("backfill next-poll timestamp is invalid") from exc
        if target.tzinfo is None or target.utcoffset() is None:
            raise AirflowException("backfill next-poll timestamp lacks timezone")
        target = target.astimezone(timezone.utc)
    else:
        dag_run = context.get("dag_run")
        started_at = getattr(dag_run, "start_date", None)
        if not isinstance(started_at, datetime):
            started_at = datetime.now(timezone.utc)
        elif started_at.tzinfo is None or started_at.utcoffset() is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        target = started_at.astimezone(timezone.utc) + FAILED_RUN_COOLDOWN
    if datetime.now(timezone.utc) < target:
        return False
    dag_run = context.get("dag_run")
    get_instances = getattr(dag_run, "get_task_instances", None)
    if callable(get_instances):
        failed = tuple(
            item
            for item in get_instances()
            if str(getattr(item, "task_id", ""))
            != "wait_before_next_continuous_run"
            and str(getattr(item, "state", "")).lower()
            in {"failed", "upstream_failed"}
        )
        if failed:
            task_ids = ", ".join(
                sorted(str(getattr(item, "task_id", "")) for item in failed)
            )
            raise AirflowException(
                f"backfill upstream task failure after cooldown: {task_ids}"
            )
    return True


def _strict_backfill_preflight() -> dict[str, Any]:
    try:
        return strict_cutover_preflight()
    except Exception as exc:  # Airflow should show one stable task failure class
        if isinstance(exc, AirflowException):
            raise
        raise AirflowException(str(exc)) from exc


def _load_backfill_policy():
    try:
        from dags.scripts.run_transfermarkt_scope_cycle import (
            validate_standing_policy_for_scope_cycle,
        )
    except ModuleNotFoundError:
        from scripts.run_transfermarkt_scope_cycle import (
            validate_standing_policy_for_scope_cycle,
        )
    from utils.transfermarkt_approval import load_standing_policy

    policy = load_standing_policy(STANDING_BACKFILL_POLICY_PATH)
    validate_standing_policy_for_scope_cycle(
        policy,
        write_mode="native-only",
        cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
        request_limit=SCOPE_REQUEST_LIMIT,
        retry_limit=SCOPE_RETRY_LIMIT,
        expected_dag_id=BACKFILL_DAG_ID,
    )
    return policy


def _environment_for_scope(
    *,
    payload: Mapping[str, Any],
    preflight: Mapping[str, Any],
    policy_hash: str,
    run_id: str,
    batch_id: str,
    lease_id: str,
    claim_generation: int,
    attempt_sequence: int = 1,
    finalize_only: bool = False,
) -> dict[str, str]:
    scope_id = str(payload["scope_id"])
    return {
        "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
        "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
        "TM_REQUIRE_METERED_PROXY": "true",
        "TRANSFERMARKT_REQUIRE_RAW_STORE": "true",
        "TM_DAG_ID": BACKFILL_DAG_ID,
        "TM_RUN_ID": run_id,
        "TM_TASK_ID": "run_historical_scope",
        "TM_SCOPE_ID": scope_id,
        "TM_SCOPE_PAYLOAD_JSON": json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ),
        "TM_READER_REVISION": str(int(preflight["revision"])),
        "TM_CANDIDATE_SLOT": str(preflight["candidate_slot"]),
        "TM_WRITE_MODE": "native-only",
        "TM_PARSER_VERSION": "v2",
        "TM_SCHEMA_VERSION": "2",
        "TM_APPROVAL_MODE": "standing_policy",
        "TM_STANDING_POLICY_PATH": STANDING_BACKFILL_POLICY_PATH,
        "TM_STANDING_POLICY_SHA256": policy_hash,
        "TM_MV_TRANSFERS_LIMIT": str(MAX_ROSTER_WINDOW),
        "TM_REFRESH_MODE": "historical",
        "TM_COACH_HISTORY_TTL_DAYS": str(COACH_HISTORY_TTL_DAYS),
        "TM_PROXY_LEASE_TTL_SECONDS": str(DEFAULT_ENTITY_TIMEOUT_SECONDS),
        "TM_CHECKPOINT_TTL_DAYS": str(CHECKPOINT_TTL_DAYS),
        "TM_ENTITY_TIMEOUT_SECONDS": str(DEFAULT_ENTITY_TIMEOUT_SECONDS),
        "TM_PROVIDER_HARD_CAP_BYTES": str(SCOPE_HARD_PROVIDER_BYTE_CAP),
        "TM_PROVIDER_SOFT_STOP_BYTES": str(SCOPE_SOFT_PROVIDER_BYTE_STOP),
        "TM_PROXY_REQUEST_LIMIT": str(SCOPE_REQUEST_LIMIT),
        "TM_PROXY_RETRY_LIMIT": str(SCOPE_RETRY_LIMIT),
        # These are batch-local in this DAG. There is intentionally no UTC-day
        # or campaign application cap on historical work.
        "TM_PARENT_BYTE_BUDGET": str(PARENT_DAILY_HARD_PROVIDER_BYTE_CAP),
        "TM_PARENT_SOFT_BYTE_STOP": str(PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP),
        "TM_PARENT_REQUEST_LIMIT": str(PARENT_REQUEST_LIMIT),
        "TM_PARENT_RETRY_LIMIT": str(PARENT_RETRY_LIMIT),
        "TM_BACKFILL_CAMPAIGN_ID": str(payload["resume_cycle_id"]),
        "TM_BACKFILL_BATCH_ID": batch_id,
        "TM_BACKFILL_LEASE_ID": lease_id,
        "TM_BACKFILL_CLAIM_GENERATION": str(int(claim_generation)),
        "TM_BACKFILL_ATTEMPT_SEQUENCE": str(int(attempt_sequence)),
        "TM_BACKFILL_FINALIZE_ONLY": "true" if finalize_only else "false",
    }


def _render_planned_environments(
    *,
    ti: Any,
    now: datetime,
    campaign: state.BackfillCampaign,
    scopes: tuple[state.BackfillScopeState, ...],
    batch: state.BackfillBatch | None,
    attempts: tuple[state.BackfillAttempt, ...],
    payloads: tuple[dict[str, Any], ...],
    preflight: Mapping[str, Any],
    policy_hash: str,
    run_id: str,
) -> list[dict[str, str]]:
    if batch is None:
        _publish_next_poll(ti, now=now, scopes=scopes, idle=True)
        return []
    _publish_next_poll(ti, now=now)
    scopes_by_id = {item.target.scope_id: item for item in scopes}
    generations = dict(
        zip(
            batch.scope_ids,
            batch.scope_claim_generations,
            strict=True,
        )
    )
    persisted_attempt_ids = {item.attempt_id for item in attempts}
    if tuple(item["scope_id"] for item in payloads) != batch.scope_ids:
        raise AirflowException("mapped plan differs from durable batch membership")
    planned_environments: list[dict[str, str]] = []
    for payload in payloads:
        scope_id = str(payload["scope_id"])
        scope = scopes_by_id[scope_id]
        generation = generations[scope_id]
        attempt_sequence = scope.attempt_count + (
            1 if scope.status.value == "running" else 0
        )
        paths = payload.get("result_paths")
        local_result = False
        forced_finalize = (
            scope.status.value != "running"
            or batch.status.value in {"dq_pending", "complete", "blocked_platform"}
        )
        if not forced_finalize:
            if not isinstance(paths, Mapping):
                raise AirflowException(f"{scope_id}: result_paths is invalid")
            try:
                local_result = has_matching_scope_attempt_result(
                    result_base_dir=str(paths.get("base_dir") or ""),
                    entity_dir=str(paths.get("entity_staging_dir") or ""),
                    campaign_id=campaign.campaign_id,
                    child_cycle_id=str(payload.get("child_cycle_id") or ""),
                    scope_id=scope_id,
                    batch_id=batch.batch_id,
                    claim_generation=generation,
                    attempt_sequence=attempt_sequence,
                )
            except Exception as exc:
                raise AirflowException(
                    f"{scope_id}: local attempt fence is invalid"
                ) from exc
        planned_environments.append(_environment_for_scope(
            payload=payload,
            preflight=preflight,
            policy_hash=policy_hash,
            run_id=run_id,
            batch_id=batch.batch_id,
            lease_id=str(scope.lease_id or batch.batch_id),
            claim_generation=generation,
            attempt_sequence=attempt_sequence,
            finalize_only=(
                forced_finalize
                or local_result
                or state.stable_attempt_id(
                    campaign.campaign_id,
                    scope_id,
                    scope.attempt_count + 1,
                    claim_generation=generation,
                )
                in persisted_attempt_ids
            ),
        ))
    return planned_environments


def _persist_planner_platform_incident(
    *,
    campaign_id: str,
    batch_id: str,
    exc: BaseException,
) -> None:
    """Bind every post-claim planner failure before another run may replay it."""

    from utils.transfermarkt_backfill_artifacts import BackfillArtifactStore
    from utils.transfermarkt_backfill_finalize import (
        _persist_batch_platform_incident,
    )

    with BackfillStateRepository.connect() as repository:
        campaign = repository.load_campaign(campaign_id)
        batch = repository.load_batch(batch_id)
        if batch.open_platform_incident_id is not None:
            return
        _persist_batch_platform_incident(
            repository,
            campaign=campaign,
            batch=batch,
            artifact_store=BackfillArtifactStore.from_env(),
            phase="post_claim_planning",
            error_class=f"post_claim_planning:{type(exc).__name__}"[:200],
            now=datetime.now(timezone.utc),
            raw_evidence_ids=batch.raw_evidence_ids,
        )


def _plan_historical_batch(**context: Any) -> list[dict[str, str]]:
    ti = context["ti"]
    preflight = ti.xcom_pull(task_ids="strict_cutover_preflight") or {}
    if preflight.get("paid_io_allowed") is not True:
        raise AirflowException("strict preflight did not authorize paid I/O")
    if str(context["dag"].dag_id) != BACKFILL_DAG_ID:
        raise AirflowException("backfill planner is attached to the wrong DAG")
    run_id = str(context.get("run_id") or "").strip()
    if not run_id:
        raise AirflowException("Airflow run_id is required")
    params = dict(context.get("params") or {})
    limit = int(params.get("max_batch", MAX_SCOPE_BATCH))
    if not 1 <= limit <= MAX_SCOPE_BATCH:
        raise AirflowException(f"max_batch must be in 1..{MAX_SCOPE_BATCH}")
    policy = _load_backfill_policy()
    now = datetime.now(timezone.utc)
    resumed_incident_batch_id: str | None = None
    campaign: state.BackfillCampaign | None = None
    batch: state.BackfillBatch | None = None

    try:
        with BackfillStateRepository.connect() as repository:
            repository.ensure_schema()
            campaign = repository.open_campaign()
            if campaign is None:
                rows = read_promoted_registry()
                campaign = build_campaign_from_registry(
                    rows,
                    policy_sha256=policy.policy_hash,
                    now=now,
                    previous_campaigns=repository.load_campaigns(),
                )
                if campaign is None:
                    _publish_next_poll(ti, now=now, idle=True)
                    return []
                campaign, scopes = repository.initialise_campaign(campaign)
            elif campaign.status.value == "waiting_prerequisite":
                campaign, scopes = repository.resume_waiting_campaign(campaign)
            else:
                scopes = repository.load_scopes(campaign.campaign_id)

            if campaign.policy_sha256 != policy.policy_hash:
                _publish_next_poll(ti, now=now, scopes=scopes, idle=True)
                raise BackfillRuntimeError(
                    "standing policy changed during the frozen campaign; "
                    "the campaign remains blocked from paid I/O"
                )

            incident_batch = None
            if campaign.status in {
                state.CampaignStatus.ACTIVE,
                state.CampaignStatus.BLOCKED_PLATFORM,
            }:
                campaign, incident_batch = repository.reconcile_open_platform_incident(
                    campaign,
                    now=now,
                )
            if campaign.status is state.CampaignStatus.BLOCKED_PLATFORM:
                if params.get("resume_platform_block") is not True:
                    _publish_next_poll(ti, now=now, scopes=scopes, idle=True)
                    repository.reconcile_platform_block(campaign, now=now)
                    raise BackfillRuntimeError(
                        f"campaign {campaign.campaign_id} is platform-blocked; "
                        "repair the prerequisite, then trigger with "
                        "resume_platform_block=true"
                    )
                campaign, scopes, resumed_batch = repository.resume_platform_campaign(
                    campaign,
                    lease_owner=f"{BACKFILL_DAG_ID}:{run_id}",
                    now=now,
                )
                if incident_batch is not None:
                    resumed_incident_batch_id = resumed_batch.batch_id
            rows = read_promoted_registry(
                registry_snapshot_id=campaign.registry_snapshot_id,
            )
            batches = repository.load_batches(campaign.campaign_id)
            batch = select_recoverable_batch(campaign, scopes, batches)
            if batch is not None:
                if (
                    batch.status.value in {"claimed", "running"}
                    and batch.batch_id != resumed_incident_batch_id
                ):
                    scopes = repository.recover_batch_claim(
                        batch,
                        scopes,
                        lease_owner=f"{BACKFILL_DAG_ID}:{run_id}",
                        now=now,
                    )
                payloads = plan_existing_batch(
                    campaign,
                    batch,
                    registry_rows=rows,
                    run_id=run_id,
                    now=now,
                )
            else:
                claim, payloads = claim_and_plan(
                    campaign,
                    scopes,
                    registry_rows=rows,
                    run_id=run_id,
                    lease_owner=f"{BACKFILL_DAG_ID}:{run_id}",
                    now=now,
                    limit=limit,
                )
                # The batch MERGE is the first durable claim mutation.  Keep
                # its identity before persistence so a later scope-CAS/readback
                # failure can be bound to an incident instead of replayed.
                batch = claim.batch
                repository.persist_claim(scopes, claim)
                scopes = claim.scopes
            attempts = repository.load_attempts(campaign.campaign_id)
    except Exception as exc:
        if campaign is not None and batch is not None:
            try:
                _persist_planner_platform_incident(
                    campaign_id=campaign.campaign_id,
                    batch_id=batch.batch_id,
                    exc=exc,
                )
            except Exception as incident_exc:
                raise AirflowException(
                    "post-claim planning failed and its platform incident "
                    "could not be persisted"
                ) from incident_exc
        if isinstance(exc, AirflowException):
            raise
        raise AirflowException(str(exc)) from exc
    assert campaign is not None
    try:
        return _render_planned_environments(
            ti=ti,
            now=now,
            campaign=campaign,
            scopes=scopes,
            batch=batch,
            attempts=attempts,
            payloads=payloads,
            preflight=preflight,
            policy_hash=policy.policy_hash,
            run_id=run_id,
        )
    except Exception as exc:
        if batch is not None:
            try:
                _persist_planner_platform_incident(
                    campaign_id=campaign.campaign_id,
                    batch_id=batch.batch_id,
                    exc=exc,
                )
            except Exception as incident_exc:
                raise AirflowException(
                    "post-claim environment validation failed and its platform "
                    "incident could not be persisted"
                ) from incident_exc
        if isinstance(exc, AirflowException):
            raise
        raise AirflowException(str(exc)) from exc


def _finalize_historical_batch(**context: Any) -> dict[str, Any]:
    planned = context["ti"].xcom_pull(task_ids="plan_historical_batch") or []
    if not planned:
        from utils.transfermarkt_backfill_finalize import (
            reconcile_campaign_completion,
        )

        return reconcile_campaign_completion()
    from utils.transfermarkt_backfill_finalize import finalize_backfill_batch

    try:
        result = finalize_backfill_batch(planned)
    except Exception as exc:
        raise AirflowException(str(exc)) from exc
    if result.get("silver_trigger_allowed") is not False:
        raise AirflowException("backfill finalizer attempted to authorize Silver")
    return result


with DAG(
    dag_id=BACKFILL_DAG_ID,
    default_args=SCRAPER_ARGS,
    description="Continuous frozen-snapshot Transfermarkt historical Bronze backfill",
    schedule="@continuous",
    start_date=datetime(2026, 7, 21),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["scraping", "transfermarkt", "bronze", "historical", "backfill"],
    max_active_runs=1,
    params={
        "max_batch": Param(
            default=MAX_SCOPE_BATCH,
            type="integer",
            minimum=1,
            maximum=MAX_SCOPE_BATCH,
        ),
        "resume_platform_block": Param(
            default=False,
            type="boolean",
            description=(
                "Explicitly resume the one evidence-bound platform-blocked batch"
            ),
        ),
    },
    doc_md="""
    Historical senior-men scopes only. A campaign freezes one fresh promoted
    registry snapshot and is drained in batches of at most eight. Every paid
    attempt is raw-first, uses the dedicated backfill proxy pool/quota, writes
    Native Bronze only, and is finalized with snapshot-pinned DQ. The DAG never
    triggers Silver; current editions remain owned by dag_ingest_transfermarkt.
    """,
) as dag:
    preflight_task = PythonOperator(
        task_id="strict_cutover_preflight",
        python_callable=_strict_backfill_preflight,
        pool=BACKFILL_CONTROL_POOL,
    )
    plan_task = PythonOperator(
        task_id="plan_historical_batch",
        python_callable=_plan_historical_batch,
        pool=BACKFILL_CONTROL_POOL,
    )
    run_task = BashOperator.partial(
        task_id="run_historical_scope",
        bash_command=r"""set -uo pipefail
if [[ "${TM_BACKFILL_FINALIZE_ONLY:-false}" == "true" ]]; then
  exit 0
fi
cd /opt/airflow
python dags/scripts/run_transfermarkt_scope_cycle.py \
  --payload-json "$TM_SCOPE_PAYLOAD_JSON" \
  --reader-revision "$TM_READER_REVISION" \
  --candidate-slot "$TM_CANDIDATE_SLOT" \
  --write-mode native-only \
  --standing-policy "$TM_STANDING_POLICY_PATH" \
  --standing-policy-sha256 "$TM_STANDING_POLICY_SHA256" \
  --career-window-limit "$TM_MV_TRANSFERS_LIMIT" \
  --refresh-mode historical \
  --coach-history-ttl-days "$TM_COACH_HISTORY_TTL_DAYS" \
  --checkpoint-ttl-days "$TM_CHECKPOINT_TTL_DAYS" \
  --lease-ttl-seconds "$TM_PROXY_LEASE_TTL_SECONDS" \
  --entity-timeout-seconds "$TM_ENTITY_TIMEOUT_SECONDS" \
  --cycle-budget-bytes "$TM_PROVIDER_HARD_CAP_BYTES" \
  --soft-byte-stop-bytes "$TM_PROVIDER_SOFT_STOP_BYTES" \
  --request-limit "$TM_PROXY_REQUEST_LIMIT" \
  --retry-limit "$TM_PROXY_RETRY_LIMIT" \
  --parent-byte-budget "$TM_PARENT_BYTE_BUDGET" \
  --parent-soft-byte-stop "$TM_PARENT_SOFT_BYTE_STOP" \
  --parent-request-limit "$TM_PARENT_REQUEST_LIMIT" \
  --parent-retry-limit "$TM_PARENT_RETRY_LIMIT"
# The durable finalizer classifies the immutable result/raw evidence. It must
# run for source failures too; a missing evidence envelope becomes a platform
# block there rather than being silently retried here.
exit 0""",
        append_env=True,
        retries=0,
        pool="transfermarkt_backfill_proxy",
        pool_slots=1,
        priority_weight=10,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS),
        do_xcom_push=False,
    ).expand(env=plan_task.output)
    finalize_task = PythonOperator(
        task_id="finalize_historical_batch",
        python_callable=_finalize_historical_batch,
        trigger_rule="all_done",
        pool=BACKFILL_CONTROL_POOL,
    )
    cooldown_task = PythonSensor(
        task_id="wait_before_next_continuous_run",
        python_callable=_backfill_poll_ready,
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=2).total_seconds(),
        trigger_rule="all_done",
        retries=0,
        pool=BACKFILL_CONTROL_POOL,
    )

    preflight_task >> plan_task >> run_task >> finalize_task >> cooldown_task


__all__ = [
    "BACKFILL_DAG_ID",
    "BACKFILL_CONTROL_POOL",
    "STANDING_BACKFILL_POLICY_PATH",
    "_environment_for_scope",
    "_backfill_poll_ready",
    "_finalize_historical_batch",
    "_plan_historical_batch",
    "_strict_backfill_preflight",
    "dag",
]
