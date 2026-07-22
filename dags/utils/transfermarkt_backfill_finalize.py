"""Idempotent attempt accounting and snapshot-pinned DQ for backfill batches."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from scrapers.transfermarkt.raw_store import RawResponseStore
from utils import transfermarkt_backfill_state as state
from utils import transfermarkt_native_v2 as tm_v2
from utils.transfermarkt_backfill_artifacts import BackfillArtifactStore
from utils.transfermarkt_backfill_attempts import (
    ClassifiedScopeAttempt,
    collect_scope_attempt_evidence,
    verify_envelope_set,
)
from utils.transfermarkt_backfill_dq import (
    pin_iceberg_snapshots,
    run_backfill_batch_dq,
)
from utils.transfermarkt_backfill_runtime import (
    BACKFILL_DAG_ID,
    BackfillStateRepository,
)
from utils.transfermarkt_scope_state import ScopeManifest


class BackfillFinalizeError(RuntimeError):
    """A mapped batch could not be reconciled and quality-gated exactly."""


def _load_object(path: str) -> dict[str, Any]:
    candidate = Path(path)
    if not candidate.is_absolute() or not candidate.is_file():
        raise BackfillFinalizeError(f"required result artifact is missing: {path}")
    try:
        value = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillFinalizeError(f"result artifact is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise BackfillFinalizeError(f"result artifact must be an object: {path}")
    return value


def _payload_from_environment(environment: Mapping[str, str]) -> dict[str, Any]:
    if environment.get("TM_DAG_ID") != BACKFILL_DAG_ID:
        raise BackfillFinalizeError("mapped environment has the wrong DAG identity")
    if environment.get("TM_WRITE_MODE") != "native-only":
        raise BackfillFinalizeError("backfill mapped environment is not Native-only")
    try:
        payload = json.loads(environment["TM_SCOPE_PAYLOAD_JSON"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise BackfillFinalizeError("mapped scope payload is unreadable") from exc
    if not isinstance(payload, dict):
        raise BackfillFinalizeError("mapped scope payload must be an object")
    return payload


def _validated_manifest(
    value: Mapping[str, Any],
    *,
    policy_sha256: str,
    campaign: state.BackfillCampaign,
    scope: state.BackfillScopeState,
    parent_cycle_id: str | None = None,
    child_cycle_id: str | None = None,
    capture_revision: str | None = None,
) -> ScopeManifest:
    manifest = ScopeManifest.from_mapping(value)
    manifest.validate(tm_v2.NATIVE_ENTITIES)
    if value.get("manifest_digest") != manifest.digest:
        raise BackfillFinalizeError("scope manifest digest mismatch")
    if manifest.dq_evidence.get("silver_trigger_allowed") is not False:
        raise BackfillFinalizeError("historical manifest authorizes Silver")
    if manifest.dq_evidence.get("standing_policy_hash") != policy_sha256:
        raise BackfillFinalizeError(
            "scope manifest authorization differs from frozen campaign policy"
        )
    expected_identity = {
        "scope_id": scope.target.scope_id,
        "competition_id": scope.target.competition_id,
        "edition_id": scope.target.edition_id,
        "canonical_competition_id": scope.target.canonical_competition_id,
        "canonical_season": scope.target.canonical_season,
        "registry_snapshot_id": campaign.registry_snapshot_id,
        "parser_revision": "v2",
        "schema_revision": "2",
    }
    for field, expected in expected_identity.items():
        if str(getattr(manifest, field)) != str(expected):
            raise BackfillFinalizeError(
                f"scope manifest {field} differs from durable scope"
            )
    if parent_cycle_id is not None and manifest.parent_cycle_id != parent_cycle_id:
        raise BackfillFinalizeError("scope manifest parent cycle differs from batch")
    if child_cycle_id is not None and manifest.child_cycle_id != child_cycle_id:
        raise BackfillFinalizeError("scope manifest child cycle differs from plan")
    if capture_revision is not None and manifest.capture_revision != capture_revision:
        raise BackfillFinalizeError("scope manifest capture revision differs from plan")
    return manifest


def _manifest(
    path: str,
    *,
    policy_sha256: str,
    campaign: state.BackfillCampaign,
    scope: state.BackfillScopeState,
    parent_cycle_id: str,
    child_cycle_id: str,
    capture_revision: str,
) -> ScopeManifest:
    return _validated_manifest(
        _load_object(path),
        policy_sha256=policy_sha256,
        campaign=campaign,
        scope=scope,
        parent_cycle_id=parent_cycle_id,
        child_cycle_id=child_cycle_id,
        capture_revision=capture_revision,
    )


def _persisted_manifest(
    artifact_store: BackfillArtifactStore,
    scope: state.BackfillScopeState,
    *,
    policy_sha256: str,
    campaign: state.BackfillCampaign,
    parent_cycle_id: str | None = None,
    child_cycle_id: str | None = None,
    capture_revision: str | None = None,
) -> ScopeManifest:
    if scope.scope_manifest_uri is None or scope.scope_manifest_sha256 is None:
        raise BackfillFinalizeError("captured scope lacks a durable manifest")
    return _validated_manifest(
        artifact_store.load_json(
            scope.scope_manifest_uri,
            expected_sha256=scope.scope_manifest_sha256,
        ),
        policy_sha256=policy_sha256,
        campaign=campaign,
        scope=scope,
        parent_cycle_id=parent_cycle_id,
        child_cycle_id=child_cycle_id,
        capture_revision=capture_revision,
    )


def _claim_generation(environment: Mapping[str, str], *, scope_id: str) -> int:
    try:
        generation = int(environment["TM_BACKFILL_CLAIM_GENERATION"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BackfillFinalizeError(
            f"{scope_id}: claim generation is invalid"
        ) from exc
    if generation < 1:
        raise BackfillFinalizeError(f"{scope_id}: claim generation is invalid")
    return generation


def _attempt_sequence(environment: Mapping[str, str], *, scope_id: str) -> int:
    try:
        sequence = int(environment["TM_BACKFILL_ATTEMPT_SEQUENCE"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BackfillFinalizeError(
            f"{scope_id}: attempt sequence is invalid"
        ) from exc
    if sequence < 1:
        raise BackfillFinalizeError(f"{scope_id}: attempt sequence is invalid")
    return sequence


def _block_platform(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    now: datetime,
) -> tuple[state.BackfillCampaign, state.BackfillBatch]:
    """Converge both independently persisted control rows to blocked state."""

    current_batch = batch
    if current_batch.status not in {
        state.BatchStatus.BLOCKED_PLATFORM,
        state.BatchStatus.COMPLETE,
    }:
        blocked_batch = current_batch.transition(
            state.BatchStatus.BLOCKED_PLATFORM,
            now=now,
        )
        repository.persist_batch_transition(current_batch, blocked_batch)
        current_batch = blocked_batch
    current_campaign = campaign
    if current_campaign.status is state.CampaignStatus.ACTIVE:
        blocked_campaign = current_campaign.transition(
            state.CampaignStatus.BLOCKED_PLATFORM,
            now=now,
        )
        repository.persist_campaign_transition(current_campaign, blocked_campaign)
        current_campaign = blocked_campaign
    return current_campaign, current_batch


def _persist_batch_platform_incident(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    artifact_store: BackfillArtifactStore,
    phase: str,
    error_class: str,
    now: datetime,
    raw_evidence_ids: Sequence[str] = (),
    cause_artifact: Any | None = None,
) -> tuple[state.BackfillCampaign, state.BackfillBatch]:
    """Publish evidence before durably blocking a post-attempt batch phase."""

    if batch.open_platform_incident_id is not None:
        raise BackfillFinalizeError("batch already has an open platform incident")
    evidence = tuple(sorted(set(raw_evidence_ids)))
    incident_id = state.stable_platform_incident_id(
        batch,
        phase=phase,
        error_class=error_class[:200],
        raw_evidence_ids=evidence,
    )
    cause = (
        {
            "uri": str(cause_artifact.uri),
            "sha256": str(cause_artifact.sha256),
        }
        if cause_artifact is not None
        else None
    )
    incident_artifact = artifact_store.publish_json(
        {
            "contract_version": 1,
            "incident_id": incident_id,
            "campaign_id": campaign.campaign_id,
            "batch_id": batch.batch_id,
            "batch_revision": batch.revision,
            "blocked_from_status": batch.status.value,
            "phase": phase,
            "error_class": error_class[:200],
            "raw_evidence_ids": list(evidence),
            "snapshot_pins": batch.snapshot_pins,
            "cause_artifact": cause,
        },
        kind="platform_incident",
        owner_id=campaign.campaign_id,
    )
    incident = state.BackfillPlatformIncident.build(
        batch=batch,
        phase=phase,
        error_class=error_class[:200],
        report_uri=incident_artifact.uri,
        report_sha256=incident_artifact.sha256,
        raw_evidence_ids=evidence,
        now=now,
    )
    blocked_batch = batch.record_platform_incident(incident, now=now)
    repository.persist_batch_transition(batch, blocked_batch)
    blocked_campaign, converged_batch = _block_platform(
        repository,
        campaign=campaign,
        batch=blocked_batch,
        now=now,
    )
    return blocked_campaign, converged_batch


def _platform_error_class(phase: str, exc: BaseException) -> str:
    return f"{phase}:{type(exc).__name__}"[:200]


def _matching_attempt(
    attempts: Sequence[state.BackfillAttempt],
    *,
    scope: state.BackfillScopeState,
    batch: state.BackfillBatch,
    sequence: int,
) -> state.BackfillAttempt | None:
    attempt_id = state.stable_attempt_id(
        scope.campaign_id,
        scope.target.scope_id,
        sequence,
        claim_generation=scope.claim_generation,
    )
    matches = [item for item in attempts if item.attempt_id == attempt_id]
    if len(matches) > 1:
        raise BackfillFinalizeError("durable attempt journal contains duplicates")
    if not matches:
        return None
    attempt = matches[0]
    if (
        attempt.batch_id != batch.batch_id
        or attempt.scope_id != scope.target.scope_id
        or attempt.claim_generation != scope.claim_generation
        or attempt.sequence != sequence
    ):
        raise BackfillFinalizeError("durable attempt journal identity drifted")
    return attempt


def _safe_finished_at(scope: state.BackfillScopeState, now: datetime) -> datetime:
    started = scope.leased_at or now
    return max(started, now)


def _persist_platform_attempt(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    scope: state.BackfillScopeState,
    artifact_store: BackfillArtifactStore,
    now: datetime,
    phase: str,
    error_class: str,
    raw_evidence_ids: Sequence[str] = (),
) -> tuple[state.BackfillCampaign, state.BackfillScopeState, state.BackfillBatch]:
    blocked_campaign, blocked_batch = _persist_batch_platform_incident(
        repository,
        campaign=campaign,
        batch=batch,
        artifact_store=artifact_store,
        phase=phase,
        error_class=error_class[:200],
        now=now,
        raw_evidence_ids=raw_evidence_ids,
    )
    attempt = state.BackfillAttempt.build(
        scope=scope,
        batch_id=blocked_batch.batch_id,
        outcome=state.AttemptOutcome.PLATFORM_ERROR,
        started_at=scope.leased_at or batch.claimed_at,
        finished_at=_safe_finished_at(scope, now),
        raw_evidence_ids=raw_evidence_ids,
        error_class=error_class[:200],
        error_message=None,
    )
    transition = repository.persist_attempt(blocked_campaign, scope, attempt)
    return transition.campaign, transition.scope, blocked_batch


def _complete_campaign_if_ready(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    artifact_store: BackfillArtifactStore,
    raw_store: RawResponseStore,
    now: datetime,
) -> tuple[state.CompletionAccounting, bool, str | None]:
    scopes = repository.load_scopes(campaign.campaign_id)
    accounting = state.accounting_for(campaign, scopes)
    if not accounting.is_complete:
        return accounting, False, campaign.report_uri
    attempts = repository.load_attempts(campaign.campaign_id)
    batches = repository.load_batches(campaign.campaign_id)
    scope_by_id = {item.target.scope_id: item for item in scopes}
    attempts_by_batch: dict[str, list[state.BackfillAttempt]] = {}
    for attempt in attempts:
        attempts_by_batch.setdefault(attempt.batch_id, []).append(attempt)
    complete_batches = tuple(
        item for item in batches if item.status is state.BatchStatus.COMPLETE
    )
    if not complete_batches:
        raise BackfillFinalizeError(
            "terminal campaign has no completed batch evidence"
        )
    for complete_batch in complete_batches:
        _verify_completed_batch_evidence(
            repository,
            campaign=campaign,
            batch=complete_batch,
            artifact_store=artifact_store,
            raw_store=raw_store,
            attempts=attempts_by_batch.get(complete_batch.batch_id, ()),
            scopes=scope_by_id,
            # Each batch proved envelopes/captures/bodies before its COMPLETE
            # CAS.  Final campaign closure rereads the small content-addressed
            # attestation and exact state indexes, avoiding a multi-million
            # object audit in one Airflow task.
            verify_raw_objects=False,
        )
    report = state.completion_report(
        campaign,
        scopes,
        attempts=attempts,
        batches=batches,
        require_complete=True,
    )
    report_artifact = artifact_store.publish_bytes(
        report.canonical_json.encode("utf-8"),
        kind="campaign_report",
        owner_id=campaign.campaign_id,
    )
    if report_artifact.sha256 != report.sha256:
        raise BackfillFinalizeError("campaign report artifact hash drift")
    if campaign.status is state.CampaignStatus.COMPLETE:
        if (
            campaign.report_uri != report_artifact.uri
            or campaign.report_sha256 != report.sha256
        ):
            raise BackfillFinalizeError("completed campaign report identity drift")
        return accounting, True, campaign.report_uri
    if campaign.status is not state.CampaignStatus.ACTIVE:
        raise BackfillFinalizeError("only an active campaign can complete")
    completed_campaign, recomputed = state.complete_campaign(
        campaign,
        scopes,
        attempts=attempts,
        batches=batches,
        report_uri=report_artifact.uri,
        now=now,
    )
    if recomputed.sha256 != report.sha256:
        raise BackfillFinalizeError("campaign completion report drift")
    repository.persist_campaign_transition(campaign, completed_campaign)
    return accounting, True, report_artifact.uri


def _finish_completed_batch(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    artifact_store: BackfillArtifactStore,
    raw_store: RawResponseStore,
    now: datetime,
) -> tuple[state.CompletionAccounting, bool, str | None]:
    if batch.status is not state.BatchStatus.COMPLETE:
        raise BackfillFinalizeError("scope DQ commit requires a complete batch")
    _verify_completed_batch_evidence(
        repository,
        campaign=campaign,
        batch=batch,
        artifact_store=artifact_store,
        raw_store=raw_store,
    )
    scopes = {
        item.target.scope_id: item
        for item in repository.load_scopes(campaign.campaign_id)
    }
    expected_generation = dict(zip(
        batch.scope_ids,
        batch.scope_claim_generations,
        strict=True,
    ))
    for scope_id in batch.scope_ids:
        current = scopes.get(scope_id)
        if current is None:
            raise BackfillFinalizeError("complete batch scope is missing")
        if (
            current.batch_id != batch.batch_id
            or current.claim_generation != expected_generation[scope_id]
        ):
            raise BackfillFinalizeError("complete batch scope identity drifted")
        if current.status is state.ScopeStatus.CAPTURED_PENDING_DQ:
            complete_scope = state.mark_scope_dq_complete(
                current,
                scope_manifest_uri=str(current.scope_manifest_uri),
                scope_manifest_sha256=str(current.scope_manifest_sha256),
                raw_evidence_ids=current.raw_evidence_ids,
                now=now,
            )
            repository.persist_scope_transition(current, complete_scope)
        elif current.status is state.ScopeStatus.RUNNING:
            raise BackfillFinalizeError("complete batch still has a running scope")
    refreshed_campaign = repository.load_campaign(campaign.campaign_id)
    return _complete_campaign_if_ready(
        repository,
        campaign=refreshed_campaign,
        artifact_store=artifact_store,
        raw_store=raw_store,
        now=now,
    )


def _finish_completed_batch_or_block(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    artifact_store: BackfillArtifactStore,
    raw_store: RawResponseStore,
    now: datetime,
) -> tuple[state.CompletionAccounting, bool, str | None]:
    """Make completion-phase platform failures explicitly repairable."""

    try:
        return _finish_completed_batch(
            repository,
            campaign=campaign,
            batch=batch,
            artifact_store=artifact_store,
            raw_store=raw_store,
            now=now,
        )
    except Exception as exc:
        if campaign.status is state.CampaignStatus.ACTIVE:
            _persist_batch_platform_incident(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                phase="campaign_completion",
                error_class=_platform_error_class("campaign_completion", exc),
                now=datetime.now(timezone.utc),
                raw_evidence_ids=batch.raw_evidence_ids,
            )
        raise


def _blocked_result(campaign_id: str, batch_id: str) -> dict[str, Any]:
    return {
        "status": "blocked_platform",
        "campaign_id": campaign_id,
        "batch_id": batch_id,
        "silver_trigger_allowed": False,
    }


def _verify_completed_batch_evidence(
    repository: BackfillStateRepository,
    *,
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    artifact_store: BackfillArtifactStore,
    raw_store: RawResponseStore,
    attempts: Sequence[state.BackfillAttempt] | None = None,
    scopes: Mapping[str, state.BackfillScopeState] | None = None,
    verify_raw_objects: bool = True,
) -> None:
    """Verify a COMPLETE batch or its compact immutable final attestation."""

    if (
        batch.dq_report_uri is None
        or batch.dq_report_sha256 is None
        or batch.snapshot_pins is None
        or not batch.raw_evidence_ids
    ):
        raise BackfillFinalizeError("complete batch evidence is incomplete")
    payload = artifact_store.load_json(
        batch.dq_report_uri,
        expected_sha256=batch.dq_report_sha256,
    )
    expected = {
        "campaign_id": campaign.campaign_id,
        "batch_id": batch.batch_id,
        "registry_snapshot_id": campaign.registry_snapshot_id,
        "snapshot_ids": dict(sorted(batch.snapshot_pins.items())),
        "passed": True,
    }
    if any(
        state.canonical_json(payload.get(key)) != state.canonical_json(value)
        for key, value in expected.items()
    ):
        raise BackfillFinalizeError("complete batch DQ report identity drifted")
    declared_attestation = str(payload.get("batch_attestation_sha256") or "")
    attested_payload = dict(payload)
    attested_payload.pop("batch_attestation_sha256", None)
    if declared_attestation != state.canonical_sha256(attested_payload):
        raise BackfillFinalizeError("complete batch attestation hash drifted")
    envelope_payload = payload.get("attempt_envelopes")
    if (
        not isinstance(envelope_payload, Mapping)
        or envelope_payload.get("all_verified") is not True
        or tuple(envelope_payload.get("ids") or ()) != batch.raw_evidence_ids
        or int(envelope_payload.get("count", -1)) != len(batch.raw_evidence_ids)
    ):
        raise BackfillFinalizeError("complete batch envelope report drifted")
    envelope_records = (
        verify_envelope_set(raw_store, batch.raw_evidence_ids)
        if verify_raw_objects else ()
    )

    batch_attempts = tuple(attempts) if attempts is not None else tuple(
        item for item in repository.load_attempts(campaign.campaign_id)
        if item.batch_id == batch.batch_id
    )
    attempt_evidence = tuple(sorted({
        envelope_id
        for attempt in batch_attempts
        for envelope_id in attempt.raw_evidence_ids
    }))
    if attempt_evidence != batch.raw_evidence_ids:
        raise BackfillFinalizeError("complete batch attempt evidence drifted")
    scope_by_id = dict(scopes) if scopes is not None else {
        item.target.scope_id: item
        for item in repository.load_scopes(campaign.campaign_id)
    }
    raw_lineage = payload.get("raw_lineage")
    if not isinstance(raw_lineage, Mapping):
        raise BackfillFinalizeError("complete batch raw-lineage report is absent")
    partial_inventory = raw_lineage.get("partial_capture_inventory")
    if not isinstance(partial_inventory, list):
        raise BackfillFinalizeError("complete batch partial inventory is invalid")
    attempts_by_scope: dict[str, list[state.BackfillAttempt]] = {}
    for attempt in batch_attempts:
        attempts_by_scope.setdefault(attempt.scope_id, []).append(attempt)
    for item in partial_inventory:
        if not isinstance(item, Mapping):
            raise BackfillFinalizeError("complete batch partial inventory is invalid")
        partial_scope = scope_by_id.get(str(item.get("scope_id") or ""))
        reported_status = str(item.get("scope_status") or "")
        progressed_retryable = (
            reported_status == state.ScopeStatus.RETRYABLE_ERROR.value
            and partial_scope is not None
            and partial_scope.status in {
                state.ScopeStatus.RETRYABLE_ERROR,
                state.ScopeStatus.RUNNING,
                state.ScopeStatus.CAPTURED_PENDING_DQ,
                state.ScopeStatus.COMPLETE,
                state.ScopeStatus.UNAVAILABLE,
                state.ScopeStatus.TERMINAL_ERROR,
            }
            and any(
                attempt.outcome in {
                    state.AttemptOutcome.SOURCE_ERROR,
                    state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
                }
                for attempt in attempts_by_scope.get(
                    str(item.get("scope_id") or ""), ()
                )
            )
        )
        if (
            partial_scope is None
            or partial_scope.target.scope_id not in batch.scope_ids
            or (
                reported_status != partial_scope.status.value
                and not progressed_retryable
            )
        ):
            raise BackfillFinalizeError(
                "complete batch partial inventory differs from durable scope"
            )
    for scope_id in batch.scope_ids:
        scope = scope_by_id.get(scope_id)
        if scope is None:
            raise BackfillFinalizeError("complete batch scope is missing")
        if verify_raw_objects and scope.status in {
            state.ScopeStatus.CAPTURED_PENDING_DQ,
            state.ScopeStatus.COMPLETE,
        }:
            manifest = _persisted_manifest(
                artifact_store,
                scope,
                policy_sha256=campaign.policy_sha256,
                campaign=campaign,
                parent_cycle_id=batch.batch_id,
            )
            scope_records = tuple(
                item for item in envelope_records if item.scope_id == scope_id
            )
            if not scope_records or {
                item.cycle_id for item in scope_records
            } != {manifest.child_cycle_id}:
                raise BackfillFinalizeError(
                    "complete batch manifest cycle differs from raw evidence"
                )


def finalize_backfill_batch(
    planned_environments: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    """Reconcile attempts, DQ and completion across arbitrary crash boundaries."""

    if not planned_environments or len(planned_environments) > 8:
        raise BackfillFinalizeError("finalizer requires one bounded non-empty batch")
    payloads = tuple(_payload_from_environment(item) for item in planned_environments)
    campaign_ids = {str(item.get("resume_cycle_id") or "") for item in payloads}
    batch_ids = {
        str(item.get("TM_BACKFILL_BATCH_ID") or "")
        for item in planned_environments
    }
    if len(campaign_ids) != 1 or "" in campaign_ids:
        raise BackfillFinalizeError("mapped scopes disagree on campaign_id")
    if len(batch_ids) != 1 or "" in batch_ids:
        raise BackfillFinalizeError("mapped scopes disagree on batch_id")
    campaign_id = next(iter(campaign_ids))
    batch_id = next(iter(batch_ids))
    raw_store = RawResponseStore.from_env()
    assert raw_store is not None
    artifact_store = BackfillArtifactStore(raw_store)
    now = datetime.now(timezone.utc)

    with BackfillStateRepository.connect() as repository:
        campaign = repository.load_campaign(campaign_id)
        policy_hashes = {
            str(item.get("TM_STANDING_POLICY_SHA256") or "")
            for item in planned_environments
        }
        if policy_hashes != {campaign.policy_sha256}:
            raise BackfillFinalizeError(
                "mapped authorization differs from frozen campaign policy"
            )
        batch = repository.load_batch(batch_id)
        if batch.campaign_id != campaign_id:
            raise BackfillFinalizeError("batch belongs to another campaign")
        payload_scope_ids = tuple(str(item.get("scope_id") or "") for item in payloads)
        if payload_scope_ids != batch.scope_ids:
            raise BackfillFinalizeError("mapped scopes differ from durable batch")
        expected_generation = dict(zip(
            batch.scope_ids,
            batch.scope_claim_generations,
            strict=True,
        ))
        for environment, scope_id in zip(
            planned_environments,
            payload_scope_ids,
            strict=True,
        ):
            if _claim_generation(environment, scope_id=scope_id) != expected_generation[scope_id]:
                raise BackfillFinalizeError(f"{scope_id}: claim generation drift")

        if (
            campaign.status is state.CampaignStatus.BLOCKED_PLATFORM
            or batch.status is state.BatchStatus.BLOCKED_PLATFORM
            or batch.open_platform_incident_id is not None
        ):
            _block_platform(
                repository,
                campaign=campaign,
                batch=batch,
                now=now,
            )
            return _blocked_result(campaign_id, batch_id)
        if batch.status is state.BatchStatus.COMPLETE:
            if campaign.status not in {
                state.CampaignStatus.ACTIVE,
                state.CampaignStatus.COMPLETE,
            }:
                raise BackfillFinalizeError(
                    "complete batch belongs to a non-recoverable campaign"
                )
            accounting, campaign_completed, campaign_report_uri = (
                _finish_completed_batch_or_block(
                    repository,
                    campaign=campaign,
                    batch=batch,
                    artifact_store=artifact_store,
                    raw_store=raw_store,
                    now=now,
                )
            )
            return {
                "status": "campaign_complete" if campaign_completed else "batch_complete",
                "campaign_id": campaign_id,
                "batch_id": batch_id,
                "accounting": accounting.as_dict(),
                "dq_report_uri": batch.dq_report_uri,
                "campaign_report_uri": campaign_report_uri,
                "silver_trigger_allowed": False,
            }
        if campaign.status is not state.CampaignStatus.ACTIVE:
            raise BackfillFinalizeError("campaign is not active at batch finalization")
        if batch.status is state.BatchStatus.CLAIMED:
            running_batch = batch.transition(state.BatchStatus.RUNNING, now=now)
            repository.persist_batch_transition(batch, running_batch)
            batch = running_batch
        elif batch.status not in {
            state.BatchStatus.RUNNING,
            state.BatchStatus.DQ_PENDING,
        }:
            raise BackfillFinalizeError("batch is not recoverable at finalization")

        scopes = {
            item.target.scope_id: item
            for item in repository.load_scopes(campaign_id)
        }
        attempts = list(repository.load_attempts(campaign_id))
        captured: list[tuple[state.BackfillScopeState, ScopeManifest]] = []
        for environment, payload in zip(
            planned_environments,
            payloads,
            strict=True,
        ):
            scope_id = str(payload["scope_id"])
            previous = scopes.get(scope_id)
            if previous is None:
                raise BackfillFinalizeError(f"{scope_id}: durable scope is missing")
            generation = expected_generation[scope_id]
            if previous.claim_generation != generation or previous.batch_id != batch_id:
                raise BackfillFinalizeError(f"{scope_id}: durable claim identity drift")
            expected_sequence = previous.attempt_count + (
                1 if previous.status is state.ScopeStatus.RUNNING else 0
            )
            if (
                _attempt_sequence(environment, scope_id=scope_id)
                != expected_sequence
            ):
                raise BackfillFinalizeError(f"{scope_id}: attempt sequence drift")
            manifest: ScopeManifest | None = None
            accounted_attempt: state.BackfillAttempt
            if previous.status is state.ScopeStatus.RUNNING:
                existing = _matching_attempt(
                    attempts,
                    scope=previous,
                    batch=batch,
                    sequence=previous.attempt_count + 1,
                )
                if existing is not None:
                    accounted_attempt = existing
                    transition = repository.resume_persisted_attempt(
                        campaign,
                        previous,
                        existing,
                    )
                    campaign = transition.campaign
                    current = transition.scope
                else:
                    if environment.get("TM_BACKFILL_FINALIZE_ONLY") == "true":
                        campaign, current, batch = _persist_platform_attempt(
                            repository,
                            campaign=campaign,
                            batch=batch,
                            scope=previous,
                            artifact_store=artifact_store,
                            now=now,
                            phase="finalize_only_missing_attempt",
                            error_class="missing_persisted_attempt_for_finalize_only",
                        )
                        scopes[scope_id] = current
                        return _blocked_result(campaign_id, batch_id)
                    paths = payload.get("result_paths")
                    if not isinstance(paths, Mapping):
                        raise BackfillFinalizeError(f"{scope_id}: result_paths is invalid")
                    classified: ClassifiedScopeAttempt | None = None
                    try:
                        classified = collect_scope_attempt_evidence(
                            result_base_dir=str(paths.get("base_dir") or ""),
                            entity_dir=str(paths.get("entity_staging_dir") or ""),
                            scope_manifest_path=str(paths.get("scope_manifest") or ""),
                            scope_id=scope_id,
                            raw_store=raw_store,
                            campaign_id=campaign_id,
                            child_cycle_id=str(payload.get("child_cycle_id") or ""),
                            batch_id=batch_id,
                            claim_generation=generation,
                            attempt_sequence=expected_sequence,
                        )
                        checkpoint_artifact = (
                            artifact_store.publish_file(
                                classified.checkpoint_path,
                                kind="checkpoint",
                                owner_id=campaign_id,
                            )
                            if classified.checkpoint_path else None
                        )
                        manifest_artifact = (
                            artifact_store.publish_file(
                                classified.manifest_path,
                                kind="scope_manifest",
                                owner_id=campaign_id,
                            )
                            if classified.manifest_path else None
                        )
                        if classified.outcome is state.AttemptOutcome.CAPTURED:
                            assert classified.manifest_path is not None
                            manifest = _manifest(
                                classified.manifest_path,
                                policy_sha256=campaign.policy_sha256,
                                campaign=campaign,
                                scope=previous,
                                parent_cycle_id=batch.batch_id,
                                child_cycle_id=str(payload["child_cycle_id"]),
                                capture_revision=str(payload["capture_revision"]),
                            )
                    except Exception as exc:
                        campaign, current, batch = _persist_platform_attempt(
                            repository,
                            campaign=campaign,
                            batch=batch,
                            scope=previous,
                            artifact_store=artifact_store,
                            now=now,
                            phase="scope_attempt_evidence",
                            error_class=f"finalizer_evidence_{type(exc).__name__}",
                            raw_evidence_ids=(
                                classified.raw_evidence_ids if classified else ()
                            ),
                        )
                        scopes[scope_id] = current
                        raise BackfillFinalizeError(
                            f"{scope_id}: immutable attempt evidence is invalid"
                        ) from exc
                    if classified.outcome is state.AttemptOutcome.PLATFORM_ERROR:
                        campaign, current, batch = _persist_platform_attempt(
                            repository,
                            campaign=campaign,
                            batch=batch,
                            scope=previous,
                            artifact_store=artifact_store,
                            now=now,
                            phase="scope_attempt_platform_error",
                            error_class=(
                                classified.error_class or "platform_unknown"
                            ),
                            raw_evidence_ids=classified.raw_evidence_ids,
                        )
                        scopes[scope_id] = current
                        return _blocked_result(campaign_id, batch_id)
                    attempt = state.BackfillAttempt.build(
                        scope=previous,
                        batch_id=batch_id,
                        outcome=classified.outcome,
                        started_at=previous.leased_at or batch.claimed_at,
                        finished_at=_safe_finished_at(previous, now),
                        raw_evidence_ids=classified.raw_evidence_ids,
                        source_observed_at=classified.observed_at,
                        error_class=classified.error_class,
                        error_message=classified.error_message,
                        retry_after_seconds=classified.retry_after_seconds,
                        checkpoint_uri=(
                            checkpoint_artifact.uri if checkpoint_artifact else None
                        ),
                        checkpoint_sha256=(
                            checkpoint_artifact.sha256 if checkpoint_artifact else None
                        ),
                        scope_manifest_uri=(
                            manifest_artifact.uri if manifest_artifact else None
                        ),
                        scope_manifest_sha256=(
                            manifest_artifact.sha256 if manifest_artifact else None
                        ),
                    )
                    transition = repository.persist_attempt(campaign, previous, attempt)
                    attempts.append(attempt)
                    accounted_attempt = attempt
                    campaign = transition.campaign
                    current = transition.scope
            else:
                if previous.status is state.ScopeStatus.COMPLETE:
                    raise BackfillFinalizeError("non-complete batch contains complete scope")
                existing = _matching_attempt(
                    attempts,
                    scope=previous,
                    batch=batch,
                    sequence=previous.attempt_count,
                )
                if existing is None:
                    raise BackfillFinalizeError(
                        f"{scope_id}: accounted scope lacks its attempt journal"
                    )
                accounted_attempt = existing
                current = previous

            scopes[scope_id] = current
            if accounted_attempt.outcome is state.AttemptOutcome.PLATFORM_ERROR:
                if batch.open_platform_incident_id is None:
                    raise BackfillFinalizeError(
                        "platform attempt lacks an evidence-bound batch incident"
                    )
                return _blocked_result(campaign_id, batch_id)
            if current.status is state.ScopeStatus.CAPTURED_PENDING_DQ:
                try:
                    manifest = manifest or _persisted_manifest(
                        artifact_store,
                        current,
                        policy_sha256=campaign.policy_sha256,
                        campaign=campaign,
                        parent_cycle_id=batch.batch_id,
                        child_cycle_id=str(payload["child_cycle_id"]),
                        capture_revision=str(payload["capture_revision"]),
                    )
                except Exception as exc:
                    _persist_batch_platform_incident(
                        repository,
                        campaign=campaign,
                        batch=batch,
                        artifact_store=artifact_store,
                        phase="persisted_manifest_verification",
                        error_class=_platform_error_class(
                            "persisted_manifest_verification",
                            exc,
                        ),
                        now=now,
                        raw_evidence_ids=current.raw_evidence_ids,
                    )
                    raise BackfillFinalizeError(
                        f"{scope_id}: persisted manifest cannot be verified"
                    ) from exc
                captured.append((current, manifest))
            if campaign.status is state.CampaignStatus.BLOCKED_PLATFORM:
                _block_platform(
                    repository,
                    campaign=campaign,
                    batch=batch,
                    now=now,
                )
                return _blocked_result(campaign_id, batch_id)

        current_attempts = repository.load_attempts(campaign_id)
        batch_attempts = tuple(
            item for item in current_attempts if item.batch_id == batch_id
        )
        unique_evidence = tuple(sorted({
            envelope_id
            for item in batch_attempts
            for envelope_id in item.raw_evidence_ids
        }))
        if not unique_evidence:
            _persist_batch_platform_incident(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                phase="batch_evidence_verification",
                error_class="batch_evidence_verification:missing_raw_evidence",
                now=now,
            )
            raise BackfillFinalizeError("batch has no immutable raw evidence")
        try:
            envelope_records = verify_envelope_set(raw_store, unique_evidence)
        except Exception as exc:
            _persist_batch_platform_incident(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                phase="batch_evidence_verification",
                error_class=_platform_error_class(
                    "batch_evidence_verification",
                    exc,
                ),
                now=datetime.now(timezone.utc),
                raw_evidence_ids=unique_evidence,
            )
            raise
        if batch.status is state.BatchStatus.RUNNING:
            try:
                pins = pin_iceberg_snapshots(repository.cursor)
            except Exception as exc:
                _persist_batch_platform_incident(
                    repository,
                    campaign=campaign,
                    batch=batch,
                    artifact_store=artifact_store,
                    phase="snapshot_pinning",
                    error_class=_platform_error_class("snapshot_pinning", exc),
                    now=datetime.now(timezone.utc),
                    raw_evidence_ids=unique_evidence,
                )
                raise
            dq_pending = batch.transition(
                state.BatchStatus.DQ_PENDING,
                now=now,
                snapshot_pins=pins,
                raw_evidence_ids=unique_evidence,
            )
            repository.persist_batch_transition(batch, dq_pending)
            batch = dq_pending
        else:
            try:
                if not batch.snapshot_pins:
                    raise BackfillFinalizeError("DQ-pending batch lacks snapshot pins")
                if tuple(batch.raw_evidence_ids) != unique_evidence:
                    raise BackfillFinalizeError("DQ-pending batch evidence drifted")
                pins = {key: int(value) for key, value in batch.snapshot_pins.items()}
            except Exception as exc:
                _persist_batch_platform_incident(
                    repository,
                    campaign=campaign,
                    batch=batch,
                    artifact_store=artifact_store,
                    phase="dq_resume_validation",
                    error_class=_platform_error_class(
                        "dq_resume_validation",
                        exc,
                    ),
                    now=datetime.now(timezone.utc),
                    raw_evidence_ids=unique_evidence,
                )
                raise
        try:
            dq_report = run_backfill_batch_dq(
                repository.cursor,
                campaign_id=campaign_id,
                batch_id=batch_id,
                registry_snapshot_id=campaign.registry_snapshot_id,
                manifests=[item[1] for item in captured],
                child_cycle_ids=[
                    str(payload["child_cycle_id"]) for payload in payloads
                ],
                scope_bindings=[
                    (
                        str(payload["child_cycle_id"]),
                        str(payload["scope_id"]),
                        str(payload["competition_id"]),
                        str(payload["edition_id"]),
                    )
                    for payload in payloads
                ],
                raw_store=raw_store,
                attempt_envelopes=envelope_records,
                scope_statuses={
                    scope_id: scopes[scope_id].status.value
                    for scope_id in batch.scope_ids
                },
                pins=pins,
            )
            dq_payload = dq_report.as_dict()
            dq_payload["attempt_envelopes"] = {
                "count": len(envelope_records),
                "ids": list(unique_evidence),
                "all_verified": True,
            }
            dq_payload["batch_attestation_sha256"] = state.canonical_sha256(
                dq_payload
            )
            dq_artifact = artifact_store.publish_json(
                dq_payload,
                kind="batch_dq",
                owner_id=campaign_id,
            )
        except Exception as exc:
            _persist_batch_platform_incident(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                phase="batch_dq_execution",
                error_class=_platform_error_class("batch_dq_execution", exc),
                now=datetime.now(timezone.utc),
                raw_evidence_ids=unique_evidence,
            )
            raise
        if not dq_report.passed:
            _persist_batch_platform_incident(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                phase="batch_dq_failed",
                error_class="batch_dq_failed:quality_gate",
                now=datetime.now(timezone.utc),
                raw_evidence_ids=unique_evidence,
                cause_artifact=dq_artifact,
            )
            raise BackfillFinalizeError("backfill batch DQ did not pass")
        completed_batch = batch.transition(
            state.BatchStatus.COMPLETE,
            now=datetime.now(timezone.utc),
            snapshot_pins=pins,
            dq_report_uri=dq_artifact.uri,
            dq_report_sha256=dq_artifact.sha256,
            raw_evidence_ids=unique_evidence,
        )
        repository.persist_batch_transition(batch, completed_batch)
        batch = completed_batch
        accounting, campaign_completed, campaign_report_uri = (
            _finish_completed_batch_or_block(
                repository,
                campaign=campaign,
                batch=batch,
                artifact_store=artifact_store,
                raw_store=raw_store,
                now=datetime.now(timezone.utc),
            )
        )

    return {
        "status": "campaign_complete" if campaign_completed else "batch_complete",
        "campaign_id": campaign_id,
        "batch_id": batch_id,
        "accounting": accounting.as_dict(),
        "dq_report_uri": batch.dq_report_uri,
        "campaign_report_uri": campaign_report_uri,
        "silver_trigger_allowed": False,
    }


def reconcile_campaign_completion() -> dict[str, Any]:
    """Complete an exact terminal campaign even when no batch is newly mapped."""

    raw_store = RawResponseStore.from_env()
    assert raw_store is not None
    artifact_store = BackfillArtifactStore(raw_store)
    with BackfillStateRepository.connect() as repository:
        campaign = repository.open_campaign()
        if campaign is None:
            return {"status": "idle", "silver_trigger_allowed": False}
        if campaign.status is not state.CampaignStatus.ACTIVE:
            return {
                "status": campaign.status.value,
                "campaign_id": campaign.campaign_id,
                "silver_trigger_allowed": False,
            }
        try:
            accounting, completed, report_uri = _complete_campaign_if_ready(
                repository,
                campaign=campaign,
                artifact_store=artifact_store,
                raw_store=raw_store,
                now=datetime.now(timezone.utc),
            )
        except Exception as exc:
            batches = repository.load_batches(campaign.campaign_id)
            complete_batches = tuple(
                item for item in batches
                if item.status is state.BatchStatus.COMPLETE
                and item.open_platform_incident_id is None
            )
            if complete_batches:
                batch = max(
                    complete_batches,
                    key=lambda item: (item.completed_at, item.batch_id),
                )
                _persist_batch_platform_incident(
                    repository,
                    campaign=campaign,
                    batch=batch,
                    artifact_store=artifact_store,
                    phase="campaign_completion",
                    error_class=_platform_error_class("campaign_completion", exc),
                    now=datetime.now(timezone.utc),
                    raw_evidence_ids=batch.raw_evidence_ids,
                )
            raise
    return {
        "status": "campaign_complete" if completed else "idle",
        "campaign_id": campaign.campaign_id,
        "accounting": accounting.as_dict(),
        "campaign_report_uri": report_uri,
        "silver_trigger_allowed": False,
    }


__all__ = [
    "BackfillFinalizeError",
    "finalize_backfill_batch",
    "reconcile_campaign_completion",
]
