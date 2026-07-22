from __future__ import annotations

import dataclasses
import json
from datetime import date, datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_backfill_state as state
from scrapers.transfermarkt.models import (
    MAX_SCOPE_BATCH,
    SCOPE_WALL_CLOCK_TIMEOUT_SECONDS,
)


UTC = timezone.utc
NOW = datetime(2026, 7, 21, 8, 30, tzinfo=UTC)
POLICY_HASH = "a" * 64
RAW_ONE = "b" * 64
RAW_TWO = "c" * 64
MANIFEST_HASH = "d" * 64
CHECKPOINT_HASH = "e" * 64


def _target(index: int) -> state.HistoricalScopeTarget:
    return state.HistoricalScopeTarget(
        scope_id=f"tm-scope-{index:03d}",
        competition_id=f"C{index:03d}",
        edition_id=f"E{index:03d}",
        canonical_competition_id=f"TM-C{index:03d}",
        canonical_season=f"20{index:02d}/20{index + 1:02d}",
        registry_snapshot_id="registry-frozen-1",
    )


def _campaign(count: int = 3) -> state.BackfillCampaign:
    campaign = state.BackfillCampaign.build(
        registry_snapshot_id="registry-frozen-1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser-v3",
        schema_revision="schema-v2",
        targets=[_target(index) for index in reversed(range(count))],
        now=NOW,
    )
    return campaign.transition(state.CampaignStatus.ACTIVE, now=NOW)


def _scopes(campaign: state.BackfillCampaign):
    return tuple(
        state.BackfillScopeState.initial(campaign, target, now=NOW)
        for target in campaign.targets
    )


def _one_claim(
    campaign: state.BackfillCampaign,
    scope: state.BackfillScopeState,
    at: datetime,
) -> state.BackfillScopeState:
    result = state.claim_scopes(
        campaign, [scope], lease_owner="worker-1", now=at
    )
    assert result.batch is not None
    return result.scopes[0]


def _attempt(
    scope: state.BackfillScopeState,
    outcome: state.AttemptOutcome,
    *,
    at: datetime,
    raw=(RAW_ONE,),
    retry_after_seconds=None,
) -> state.BackfillAttempt:
    kwargs = {}
    if outcome in {
        state.AttemptOutcome.SOURCE_ERROR,
        state.AttemptOutcome.PLATFORM_ERROR,
    }:
        kwargs.update(error_class="http_503", error_message="source failed")
    if outcome is state.AttemptOutcome.CAPTURED:
        kwargs.update(
            scope_manifest_uri="s3://ops/scope.json",
            scope_manifest_sha256=MANIFEST_HASH,
            checkpoint_uri="s3://ops/checkpoint.json",
            checkpoint_sha256=CHECKPOINT_HASH,
        )
    return state.BackfillAttempt.build(
        scope=scope,
        batch_id=scope.batch_id,
        outcome=outcome,
        started_at=at - timedelta(minutes=5),
        finished_at=at,
        raw_evidence_ids=raw,
        source_observed_at=(
            None if outcome is state.AttemptOutcome.PLATFORM_ERROR else at
        ),
        retry_after_seconds=retry_after_seconds,
        **kwargs,
    )


def _complete_batch(
    batch: state.BackfillBatch,
    attempt: state.BackfillAttempt,
    *,
    at: datetime,
) -> state.BackfillBatch:
    running = batch.transition(state.BatchStatus.RUNNING, now=at)
    dq_pending = running.transition(
        state.BatchStatus.DQ_PENDING, now=at + timedelta(seconds=1)
    )
    return dq_pending.transition(
        state.BatchStatus.COMPLETE,
        now=at + timedelta(seconds=2),
        snapshot_pins={"registry": 101, "native_bronze": 202},
        dq_report_uri=f"s3://ops/dq/{batch.batch_id}.json",
        dq_report_sha256=MANIFEST_HASH,
        raw_evidence_ids=attempt.raw_evidence_ids,
    )


def _direct_claim(scope: state.BackfillScopeState, *, at: datetime):
    generation = scope.claim_generation + 1
    batch = state.BackfillBatch.build(
        scope.campaign_id,
        [scope.target.scope_id],
        scope_claim_generations=[generation],
        now=at,
    )
    lease_id = state.canonical_sha256({
        "batch_id": batch.batch_id,
        "generation": generation,
    })
    leased = dataclasses.replace(
        scope,
        status=state.ScopeStatus.RUNNING,
        batch_id=batch.batch_id,
        lease_id=lease_id,
        lease_owner="fixture-worker",
        leased_at=at,
        heartbeat_at=at,
        next_retry_at=None,
        claim_generation=generation,
        updated_at=at,
        revision=scope.revision + 1,
    )
    return leased, batch


def test_campaign_and_target_hashes_are_order_and_timezone_stable():
    targets = [_target(2), _target(0), _target(1)]
    first = state.BackfillCampaign.build(
        registry_snapshot_id="registry-frozen-1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser-v3",
        schema_revision="schema-v2",
        targets=targets,
        now=NOW,
    )
    second = state.BackfillCampaign.build(
        registry_snapshot_id="registry-frozen-1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser-v3",
        schema_revision="schema-v2",
        targets=reversed(targets),
        now=NOW.astimezone(timezone(timedelta(hours=2))),
    )

    assert first.campaign_id == second.campaign_id
    assert first.target_sha256 == second.target_sha256
    assert [item.scope_id for item in first.targets] == sorted(
        item.scope_id for item in targets
    )
    assert state.canonical_json(first).startswith('{"campaign_id":')
    assert ": " not in state.canonical_json(first)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"current": True}, "historical"),
        ({"gender": "women"}, "senior men"),
        ({"age_category": "youth"}, "senior men"),
        ({"active": False}, "active"),
        ({"classification_status": "unknown"}, "eligible"),
    ],
)
def test_frozen_target_rejects_non_historical_or_non_senior_men(changes, message):
    values = dataclasses.asdict(_target(1))
    values.update(changes)
    with pytest.raises(state.BackfillStateError, match=message):
        state.HistoricalScopeTarget(**values)


def test_frozen_target_rejects_duplicate_or_wrong_snapshot():
    with pytest.raises(state.BackfillStateError, match="duplicate scope_id"):
        state.freeze_historical_targets([_target(1), _target(1)])

    with pytest.raises(state.BackfillStateError, match="frozen registry"):
        state.freeze_historical_targets(
            [_target(1)], registry_snapshot_id="another-snapshot"
        )


def test_claim_is_deterministic_bounded_and_binds_batch_to_ordered_scopes():
    campaign = _campaign(MAX_SCOPE_BATCH + 2)
    scopes = list(reversed(_scopes(campaign)))

    result = state.claim_scopes(
        campaign,
        scopes,
        lease_owner="worker-a",
        now=NOW + timedelta(minutes=1),
    )

    assert result.batch is not None
    assert len(result.batch.scope_ids) == MAX_SCOPE_BATCH
    assert result.batch.scope_ids == tuple(
        sorted(item.target.scope_id for item in scopes)[:MAX_SCOPE_BATCH]
    )
    assert result.batch.batch_id == state.stable_batch_id(
        campaign.campaign_id, result.batch.scope_ids
    )
    assert sum(item.status is state.ScopeStatus.RUNNING for item in result.scopes) == 8
    assert len({item.lease_id for item in result.scopes if item.lease_id}) == 1

    with pytest.raises(state.BackfillStateError, match="claim limit"):
        state.claim_scopes(
            campaign, scopes, lease_owner="x", now=NOW, limit=MAX_SCOPE_BATCH + 1
        )


def test_stale_lease_reclaims_only_after_scope_timeout_plus_fifteen_minutes():
    campaign = _campaign(1)
    leased = _one_claim(campaign, _scopes(campaign)[0], NOW)
    before = NOW + timedelta(
        seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS + (14 * 60) + 59
    )
    boundary = NOW + timedelta(
        seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS, minutes=15
    )

    assert state.STALE_LEASE_AFTER == timedelta(
        seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS, minutes=15
    )
    assert not state.is_stale_lease(leased, now=before)
    assert state.is_stale_lease(leased, now=boundary)
    reclaimed = state.reclaim_stale_lease(leased, now=boundary)
    assert reclaimed.status is state.ScopeStatus.PENDING
    assert reclaimed.lease_id is None
    assert reclaimed.checkpoint_uri == leased.checkpoint_uri


def test_interrupted_batch_claim_is_recovered_without_new_generation_or_batch():
    campaign = _campaign(2)
    initial = _scopes(campaign)
    claim = state.claim_scopes(
        campaign,
        initial,
        lease_owner="original-worker",
        now=NOW,
    )
    assert claim.batch is not None
    claimed_by_id = {item.target.scope_id: item for item in claim.scopes}
    partial = (
        claimed_by_id[claim.batch.scope_ids[0]],
        next(item for item in initial if item.target.scope_id == claim.batch.scope_ids[1]),
    )

    recovered = state.recover_batch_scopes(
        claim.batch,
        partial,
        lease_owner="recovery-worker",
        now=NOW + timedelta(minutes=5),
    )

    assert all(item.status is state.ScopeStatus.RUNNING for item in recovered)
    assert all(item.batch_id == claim.batch.batch_id for item in recovered)
    assert tuple(item.claim_generation for item in recovered) == (
        claim.batch.scope_claim_generations
    )
    assert len({item.lease_id for item in recovered}) == 1
    assert len({item.lease_owner for item in recovered}) == 1
    assert recovered[0].heartbeat_at == NOW + timedelta(minutes=5)


def test_explicit_platform_resume_keeps_batch_generation_and_advances_attempt_sequence():
    campaign = _campaign(1)
    claim = state.claim_scopes(
        campaign,
        _scopes(campaign),
        lease_owner="worker",
        now=NOW,
    )
    assert claim.batch is not None
    leased = claim.scopes[0]
    platform_attempt = _attempt(
        leased,
        state.AttemptOutcome.PLATFORM_ERROR,
        at=NOW + timedelta(minutes=1),
        raw=(),
    )
    failed = state.apply_attempt(leased, platform_attempt)
    blocked_batch = claim.batch.transition(
        state.BatchStatus.BLOCKED_PLATFORM,
        now=NOW + timedelta(minutes=1),
    )

    resumed = state.resume_platform_scopes(
        blocked_batch,
        [failed],
        platform_scope_ids=[failed.target.scope_id],
        lease_owner="explicit-resume",
        now=NOW + timedelta(minutes=2),
    )[0]

    assert resumed.status is state.ScopeStatus.RUNNING
    assert resumed.batch_id == blocked_batch.batch_id
    assert resumed.claim_generation == leased.claim_generation
    assert resumed.attempt_count == 1
    assert state.stable_attempt_id(
        resumed.campaign_id,
        resumed.target.scope_id,
        resumed.attempt_count + 1,
        claim_generation=resumed.claim_generation,
    ) != platform_attempt.attempt_id


def test_stale_reclaim_is_persisted_as_cas_and_next_claim_has_unique_batch_id():
    campaign = _campaign(1)
    initial = _scopes(campaign)
    first_claim = state.claim_scopes(
        campaign, initial, lease_owner="worker-a", now=NOW
    )
    first_batch_id = first_claim.batch.batch_id
    stale_at = NOW + state.STALE_LEASE_AFTER

    second_claim = state.claim_scopes(
        campaign,
        first_claim.scopes,
        lease_owner="worker-b",
        now=stale_at,
    )

    assert second_claim.reclaimed_scope_ids == (initial[0].target.scope_id,)
    assert len(second_claim.reclaimed_scopes) == 1
    assert second_claim.batch.batch_id != first_batch_id
    assert second_claim.batch.scope_claim_generations == (2,)
    assert second_claim.scopes[0].claim_generation == 2
    assert state.stable_attempt_id(
        campaign.campaign_id,
        initial[0].target.scope_id,
        1,
        claim_generation=1,
    ) != state.stable_attempt_id(
        campaign.campaign_id,
        initial[0].target.scope_id,
        1,
        claim_generation=2,
    )
    statements = state.claim_merge_statements(first_claim.scopes, second_claim)
    assert len(statements) == 3
    assert f"MERGE INTO {state.SCOPE_TABLE}" in statements[0]
    assert f"MERGE INTO {state.BATCH_TABLE}" in statements[1]
    assert f"MERGE INTO {state.SCOPE_TABLE}" in statements[2]


def test_unavailable_requires_two_confirmations_on_distinct_utc_days():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    first = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            at=datetime(2026, 7, 21, 23, 30, tzinfo=UTC),
        ),
    )
    assert first.status is state.ScopeStatus.RETRYABLE_ERROR
    assert first.unavailable_confirmation_days == (date(2026, 7, 21),)
    assert first.next_retry_at == datetime(2026, 7, 22, tzinfo=UTC)

    scope = _one_claim(campaign, first, first.next_retry_at)
    second = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            at=datetime(2026, 7, 22, 0, 5, tzinfo=UTC),
            raw=(RAW_TWO,),
        ),
    )
    assert second.status is state.ScopeStatus.UNAVAILABLE
    assert second.unavailable_confirmation_days == (
        date(2026, 7, 21),
        date(2026, 7, 22),
    )
    assert second.raw_evidence_ids == (RAW_ONE, RAW_TWO)


def test_replayed_unavailable_observation_does_not_become_a_new_day():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    observed_at = datetime(2026, 7, 21, 23, 30, tzinfo=UTC)
    first = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            at=observed_at,
        ),
    )
    scope = _one_claim(campaign, first, first.next_retry_at)
    replay_finished_at = datetime(2026, 7, 22, 1, 0, tzinfo=UTC)
    replay = state.BackfillAttempt.build(
        scope=scope,
        batch_id=scope.batch_id,
        outcome=state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
        started_at=replay_finished_at - timedelta(minutes=1),
        finished_at=replay_finished_at,
        source_observed_at=observed_at,
        raw_evidence_ids=(RAW_TWO,),
    )

    with pytest.raises(state.BackfillStateError, match="distinct UTC day"):
        state.apply_attempt(scope, replay)


def test_final_source_slot_cannot_leave_one_unavailable_confirmation_pending():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    for offset in (1, 2):
        failed = state.apply_attempt(
            scope,
            _attempt(
                scope,
                state.AttemptOutcome.SOURCE_ERROR,
                at=NOW + timedelta(days=offset),
            ),
        )
        scope = _one_claim(campaign, failed, failed.next_retry_at)

    final_attempt = _attempt(
        scope,
        state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
        at=NOW + timedelta(days=4),
        raw=(RAW_TWO,),
    )
    terminal = state.apply_attempt(scope, final_attempt)

    assert final_attempt.outcome is state.AttemptOutcome.SOURCE_ERROR
    assert final_attempt.error_class == "unavailable_confirmation_exhausted"
    assert terminal.status is state.ScopeStatus.TERMINAL_ERROR
    assert terminal.source_attempt_count == state.MAX_SOURCE_ATTEMPTS
    assert terminal.last_error_class == "unavailable_confirmation_exhausted"


def test_source_error_retries_one_hour_then_twenty_four_and_terminalizes_third():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    first_at = NOW + timedelta(minutes=10)
    first = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.SOURCE_ERROR,
            at=first_at,
            retry_after_seconds=7200,
        ),
    )
    assert first.status is state.ScopeStatus.RETRYABLE_ERROR
    assert first.next_retry_at == first_at + timedelta(hours=2)

    scope = _one_claim(campaign, first, first.next_retry_at)
    second_at = first.next_retry_at + timedelta(minutes=5)
    second = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.SOURCE_ERROR,
            at=second_at,
            retry_after_seconds=60,
        ),
    )
    assert second.next_retry_at == second_at + timedelta(hours=24)

    scope = _one_claim(campaign, second, second.next_retry_at)
    third = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.SOURCE_ERROR,
            at=second.next_retry_at + timedelta(minutes=1),
        ),
    )
    assert third.status is state.ScopeStatus.TERMINAL_ERROR
    assert third.source_error_count == 3
    assert third.next_retry_at is None


def test_third_total_source_attempt_terminalizes_even_with_prior_unavailable_probe():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    unavailable = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            at=NOW + timedelta(minutes=1),
        ),
    )
    assert unavailable.source_attempt_count == 1

    scope = _one_claim(campaign, unavailable, unavailable.next_retry_at)
    first_error = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.SOURCE_ERROR,
            at=unavailable.next_retry_at + timedelta(minutes=1),
        ),
    )
    assert first_error.source_attempt_count == 2
    assert first_error.source_error_count == 1

    scope = _one_claim(campaign, first_error, first_error.next_retry_at)
    terminal = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.SOURCE_ERROR,
            at=first_error.next_retry_at + timedelta(minutes=1),
        ),
    )
    assert terminal.status is state.ScopeStatus.TERMINAL_ERROR
    assert terminal.source_attempt_count == 3
    assert terminal.source_error_count == 2


def test_platform_error_blocks_campaign_but_does_not_spend_source_retry_budget():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    attempt = _attempt(
        scope,
        state.AttemptOutcome.PLATFORM_ERROR,
        at=NOW + timedelta(minutes=1),
        raw=(),
    )
    failed = state.apply_attempt(scope, attempt)

    assert state.attempt_blocks_campaign(attempt)
    assert failed.status is state.ScopeStatus.RETRYABLE_ERROR
    assert failed.source_error_count == 0
    assert failed.next_retry_at is None
    plan = state.plan_attempt_transition(campaign, scope, attempt)
    assert plan.scope == failed
    assert plan.campaign.status is state.CampaignStatus.BLOCKED_PLATFORM
    assert len(plan.statements) == 3
    assert f"MERGE INTO {state.CAMPAIGN_TABLE}" in plan.statements[-1]
    with pytest.raises(state.BackfillStateError, match="campaign blocking CAS"):
        state.attempt_transition_merge_statements(scope, attempt, failed)
    assert state.attempt_transition_merge_statements(
        scope, attempt, failed, campaign=campaign
    ) == plan.statements


def test_checkpoint_and_raw_evidence_survive_attempt_and_dq_transition():
    campaign = _campaign(1)
    scope = _one_claim(campaign, _scopes(campaign)[0], NOW)
    captured = state.apply_attempt(
        scope,
        _attempt(
            scope,
            state.AttemptOutcome.CAPTURED,
            at=NOW + timedelta(minutes=30),
        ),
    )
    assert captured.status is state.ScopeStatus.CAPTURED_PENDING_DQ
    assert captured.checkpoint_sha256 == CHECKPOINT_HASH
    complete = state.mark_scope_dq_complete(
        captured,
        scope_manifest_uri="s3://ops/final-scope.json",
        scope_manifest_sha256=MANIFEST_HASH,
        raw_evidence_ids=(RAW_TWO,),
        now=NOW + timedelta(minutes=31),
    )
    assert complete.status is state.ScopeStatus.COMPLETE
    assert complete.raw_evidence_ids == (RAW_ONE, RAW_TWO)


def test_exact_completion_accounting_rejects_missing_extra_and_open_scope():
    campaign = _campaign(2)
    scopes = _scopes(campaign)
    accounting = state.accounting_for(campaign, scopes)
    assert accounting.target_total == 2
    assert accounting.open_total == 2
    assert not accounting.is_complete

    with pytest.raises(state.BackfillStateError, match="missing"):
        state.accounting_for(campaign, scopes[:1])

    with pytest.raises(state.BackfillStateError, match="non-terminal"):
        state.completion_report(campaign, scopes)


def test_completion_report_is_canonical_and_partitions_every_target_once():
    campaign = _campaign(3)
    base = _scopes(campaign)
    attempts = []
    batches = []

    leased, batch = _direct_claim(base[0], at=NOW)
    complete_attempt = _attempt(
        leased,
        state.AttemptOutcome.CAPTURED,
        at=NOW + timedelta(minutes=1),
        raw=(f"{1:064x}",),
    )
    captured = state.apply_attempt(leased, complete_attempt)
    complete_scope = state.mark_scope_dq_complete(
        captured,
        scope_manifest_uri="s3://ops/complete.json",
        scope_manifest_sha256=MANIFEST_HASH,
        now=NOW + timedelta(minutes=2),
    )
    attempts.append(complete_attempt)
    batches.append(_complete_batch(
        batch, complete_attempt, at=NOW + timedelta(minutes=2)
    ))

    unavailable_scope = base[1]
    for offset, raw in ((3, f"{2:064x}"), (24 * 60 + 3, f"{3:064x}")):
        leased, batch = _direct_claim(
            unavailable_scope, at=NOW + timedelta(minutes=offset)
        )
        unavailable_attempt = _attempt(
            leased,
            state.AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            at=NOW + timedelta(minutes=offset + 1),
            raw=(raw,),
        )
        unavailable_scope = state.apply_attempt(
            leased, unavailable_attempt
        )
        attempts.append(unavailable_attempt)
        batches.append(_complete_batch(
            batch,
            unavailable_attempt,
            at=NOW + timedelta(minutes=offset + 2),
        ))

    terminal_scope = base[2]
    terminal_at = NOW + timedelta(minutes=5)
    for number in range(3):
        leased, batch = _direct_claim(terminal_scope, at=terminal_at)
        terminal_attempt = _attempt(
            leased,
            state.AttemptOutcome.SOURCE_ERROR,
            at=terminal_at + timedelta(minutes=1),
            raw=(f"{number + 4:064x}",),
        )
        terminal_scope = state.apply_attempt(leased, terminal_attempt)
        attempts.append(terminal_attempt)
        batches.append(_complete_batch(
            batch,
            terminal_attempt,
            at=terminal_at + timedelta(minutes=2),
        ))
        if terminal_scope.next_retry_at is not None:
            terminal_at = terminal_scope.next_retry_at

    finished = (complete_scope, unavailable_scope, terminal_scope)
    completion_incident = state.BackfillPlatformIncident.build(
        batches[0],
        phase="completion_report",
        error_class="ObjectStoreTimeout",
        report_uri="s3://ops/incidents/completion-report.json",
        report_sha256="f" * 64,
        raw_evidence_ids=batches[0].raw_evidence_ids,
        now=NOW + timedelta(days=3),
    )
    open_completed_batch = batches[0].record_platform_incident(
        completion_incident,
        now=NOW + timedelta(days=3),
    )
    with pytest.raises(state.BackfillStateError, match="open platform incident"):
        state.completion_report(
            campaign,
            finished,
            attempts=attempts,
            batches=(open_completed_batch, *batches[1:]),
        )
    batches[0] = open_completed_batch.resolve_platform_incident(
        now=NOW + timedelta(days=3, seconds=1)
    )
    left = state.completion_report(
        campaign, finished, attempts=attempts, batches=batches
    )
    right = state.completion_report(
        campaign,
        reversed(finished),
        attempts=reversed(attempts),
        batches=reversed(batches),
    )
    assert left == right
    assert left.payload["accounting"] == {
        "target_total": 3,
        "pending": 0,
        "running": 0,
        "captured_pending_dq": 0,
        "retryable_error": 0,
        "complete": 1,
        "unavailable": 1,
        "terminal_error": 1,
        "terminal_total": 3,
        "open_total": 0,
        "is_complete": True,
    }
    assert json.loads(left.canonical_json) == left.payload
    completed, artifact = state.complete_campaign(
        campaign,
        finished,
        attempts=attempts,
        batches=batches,
        report_uri="s3://ops/final-report.json",
        now=NOW + timedelta(days=1),
    )
    assert artifact.sha256 == completed.report_sha256
    assert completed.status is state.CampaignStatus.COMPLETE
    state.verify_completion_report(
        completed, finished, attempts, batches, artifact
    )

    with pytest.raises(state.BackfillStateError, match="missing batch"):
        state.completion_report(
            campaign,
            finished,
            attempts=attempts,
            batches=batches[:-1],
        )
    incomplete = dataclasses.replace(
        batches[0],
        status=state.BatchStatus.DQ_PENDING,
        completed_at=None,
    )
    with pytest.raises(state.BackfillStateError, match="incomplete batch"):
        state.completion_report(
            campaign,
            finished,
            attempts=attempts,
            batches=(incomplete, *batches[1:]),
        )
    wrong_raw = dataclasses.replace(
        batches[0], raw_evidence_ids=(f"{63:064x}",)
    )
    with pytest.raises(state.BackfillStateError, match="omits attempt evidence"):
        state.completion_report(
            campaign,
            finished,
            attempts=attempts,
            batches=(wrong_raw, *batches[1:]),
        )


def test_batch_transition_graph_and_complete_evidence_are_fail_closed():
    campaign = _campaign(1)
    batch = state.BackfillBatch.build(
        campaign.campaign_id,
        [campaign.targets[0].scope_id],
        scope_claim_generations=[1],
        now=NOW,
    )
    with pytest.raises(state.BackfillStateError, match="invalid batch transition"):
        batch.transition(state.BatchStatus.COMPLETE, now=NOW)

    running = batch.transition(state.BatchStatus.RUNNING, now=NOW)
    pending = running.transition(state.BatchStatus.DQ_PENDING, now=NOW)
    with pytest.raises(state.BackfillStateError, match="pins, raw evidence"):
        pending.transition(state.BatchStatus.COMPLETE, now=NOW)

    complete = pending.transition(
        state.BatchStatus.COMPLETE,
        now=NOW,
        snapshot_pins={"registry": 1, "native": 2},
        dq_report_uri="s3://ops/dq.json",
        dq_report_sha256=MANIFEST_HASH,
        raw_evidence_ids=(RAW_ONE,),
    )
    assert complete.status is state.BatchStatus.COMPLETE
    with pytest.raises(state.BackfillStateError, match="invalid batch transition"):
        complete.transition(state.BatchStatus.RUNNING, now=NOW)


def test_dq_pending_platform_incident_resume_preserves_pins_and_evidence():
    campaign = _campaign(1)
    claimed = state.BackfillBatch.build(
        campaign.campaign_id,
        [campaign.targets[0].scope_id],
        scope_claim_generations=[1],
        now=NOW,
    )
    running = claimed.transition(
        state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=1)
    )
    pending = running.transition(
        state.BatchStatus.DQ_PENDING,
        now=NOW + timedelta(seconds=2),
        snapshot_pins={"native_bronze": 42, "registry": 7},
        dq_report_uri="s3://ops/dq/pending.json",
        dq_report_sha256=MANIFEST_HASH,
        raw_evidence_ids=(RAW_TWO, RAW_ONE),
    )
    incident_id = state.stable_platform_incident_id(
        pending,
        phase="batch_dq",
        error_class="TrinoUnavailable",
        raw_evidence_ids=(RAW_TWO,),
    )
    incident = state.BackfillPlatformIncident.build(
        pending,
        phase="batch_dq",
        error_class="TrinoUnavailable",
        report_uri=f"s3://ops/incidents/{incident_id}.json",
        report_sha256="f" * 64,
        raw_evidence_ids=(RAW_TWO,),
        now=NOW + timedelta(seconds=3),
    )

    assert incident.incident_id == incident_id
    assert state.BackfillPlatformIncident.build(
        pending,
        phase="batch_dq",
        error_class="TrinoUnavailable",
        report_uri="s3://ops/incidents/re-published.json",
        report_sha256="d" * 64,
        raw_evidence_ids=(RAW_TWO,),
        now=NOW + timedelta(seconds=3),
    ).incident_id == incident_id
    assert state.stable_platform_incident_id(
        dataclasses.replace(pending, revision=pending.revision + 1),
        phase="batch_dq",
        error_class="TrinoUnavailable",
        raw_evidence_ids=(RAW_TWO,),
    ) != incident_id
    assert state.stable_platform_incident_id(
        pending,
        phase="campaign_report",
        error_class="TrinoUnavailable",
        raw_evidence_ids=(RAW_TWO,),
    ) != incident_id
    blocked = pending.record_platform_incident(
        incident, now=NOW + timedelta(seconds=4)
    )
    assert blocked.status is state.BatchStatus.BLOCKED_PLATFORM
    assert blocked.platform_incidents == (incident,)
    assert blocked.open_platform_incident_id == incident.incident_id
    assert blocked.snapshot_pins == pending.snapshot_pins
    assert blocked.dq_report_sha256 == pending.dq_report_sha256
    assert blocked.raw_evidence_ids == pending.raw_evidence_ids
    with pytest.raises(state.BackfillStateError, match="resolved explicitly"):
        blocked.transition(
            state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=5)
        )

    resumed = blocked.resolve_platform_incident(
        now=NOW + timedelta(seconds=5)
    )
    assert resumed.status is state.BatchStatus.DQ_PENDING
    assert resumed.open_platform_incident_id is None
    assert resumed.platform_incidents == (incident,)
    assert resumed.snapshot_pins == pending.snapshot_pins
    assert resumed.dq_report_uri == pending.dq_report_uri
    assert resumed.dq_report_sha256 == pending.dq_report_sha256
    assert resumed.raw_evidence_ids == pending.raw_evidence_ids
    assert resumed.revision == pending.revision + 2


def test_claimed_incident_resumes_running_and_source_block_transition_remains():
    campaign = _campaign(1)
    claimed = state.BackfillBatch.build(
        campaign.campaign_id, [campaign.targets[0].scope_id], now=NOW
    )
    incident = state.BackfillPlatformIncident.build(
        claimed,
        phase="claim_persist",
        error_class="StateWriteTimeout",
        report_uri="s3://ops/incidents/claim.json",
        report_sha256="f" * 64,
        now=NOW + timedelta(seconds=1),
    )
    blocked = claimed.record_platform_incident(
        incident, now=NOW + timedelta(seconds=2)
    )
    resumed = blocked.resolve_platform_incident(
        now=NOW + timedelta(seconds=3)
    )
    assert resumed.status is state.BatchStatus.RUNNING

    source_blocked = claimed.transition(
        state.BatchStatus.BLOCKED_PLATFORM,
        now=NOW + timedelta(seconds=1),
    )
    assert source_blocked.open_platform_incident_id is None
    assert source_blocked.transition(
        state.BatchStatus.RUNNING,
        now=NOW + timedelta(seconds=2),
    ).status is state.BatchStatus.RUNNING


def test_complete_batch_incident_resolution_keeps_completion_and_report_refs():
    campaign = _campaign(1)
    scope = _scopes(campaign)[0]
    claimed = state.BackfillBatch.build(
        campaign.campaign_id, [scope.target.scope_id], now=NOW
    )
    pending = claimed.transition(
        state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=1)
    ).transition(
        state.BatchStatus.DQ_PENDING, now=NOW + timedelta(seconds=2)
    )
    complete = pending.transition(
        state.BatchStatus.COMPLETE,
        now=NOW + timedelta(seconds=3),
        snapshot_pins={"native_bronze": 99, "registry": 11},
        dq_report_uri="s3://ops/dq/complete.json",
        dq_report_sha256=MANIFEST_HASH,
        raw_evidence_ids=(RAW_ONE,),
    )
    incident = state.BackfillPlatformIncident.build(
        complete,
        phase="campaign_report_publish",
        error_class="ObjectStoreTimeout",
        report_uri="s3://ops/incidents/report-publish.json",
        report_sha256="f" * 64,
        raw_evidence_ids=(RAW_TWO,),
        now=NOW + timedelta(seconds=4),
    )
    recorded = complete.record_platform_incident(
        incident, now=NOW + timedelta(seconds=5)
    )

    assert recorded.status is state.BatchStatus.COMPLETE
    assert recorded.completed_at == complete.completed_at
    assert recorded.dq_report_sha256 == complete.dq_report_sha256
    open_report = state.completion_report(
        campaign,
        [scope],
        batches=[recorded],
        require_complete=False,
    )
    batch_report = open_report.payload["batches"][0]
    assert batch_report["open_platform_incident_id"] == incident.incident_id
    assert batch_report["platform_incidents"] == [
        {
            "incident_id": incident.incident_id,
            "phase": incident.phase,
                "error_class": incident.error_class,
                "blocked_from_status": state.BatchStatus.COMPLETE.value,
                "pre_incident_batch_revision": complete.revision,
                "report_uri": incident.report_uri,
            "report_sha256": incident.report_sha256,
            "raw_evidence_ids": [RAW_TWO],
            "created_at": "2026-07-21T08:30:04.000000Z",
        }
    ]

    resolved = recorded.resolve_platform_incident(
        now=NOW + timedelta(seconds=6)
    )
    assert resolved.status is state.BatchStatus.COMPLETE
    assert resolved.completed_at == complete.completed_at
    assert resolved.snapshot_pins == complete.snapshot_pins
    assert resolved.dq_report_uri == complete.dq_report_uri
    assert resolved.dq_report_sha256 == complete.dq_report_sha256
    assert resolved.raw_evidence_ids == complete.raw_evidence_ids
    assert resolved.platform_incidents == (incident,)
    assert resolved.open_platform_incident_id is None


def test_platform_incident_validation_rejects_cross_batch_duplicate_and_bad_open():
    campaign = _campaign(2)
    first = state.BackfillBatch.build(
        campaign.campaign_id, [campaign.targets[0].scope_id], now=NOW
    ).transition(state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=1))
    second = state.BackfillBatch.build(
        campaign.campaign_id, [campaign.targets[1].scope_id], now=NOW
    ).transition(state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=1))
    first_incident = state.BackfillPlatformIncident.build(
        first,
        phase="raw_verify",
        error_class="ArtifactMismatch",
        report_uri="s3://ops/incidents/first.json",
        report_sha256="f" * 64,
        now=NOW + timedelta(seconds=2),
    )
    second_incident = state.BackfillPlatformIncident.build(
        second,
        phase="raw_verify",
        error_class="ArtifactMismatch",
        report_uri="s3://ops/incidents/second.json",
        report_sha256="e" * 64,
        now=NOW + timedelta(seconds=2),
    )
    blocked = first.record_platform_incident(
        first_incident, now=NOW + timedelta(seconds=3)
    )

    with pytest.raises(state.BackfillStateError, match="another batch"):
        first.record_platform_incident(
            second_incident, now=NOW + timedelta(seconds=3)
        )
    with pytest.raises(state.BackfillStateError, match="duplicate"):
        dataclasses.replace(
            blocked,
            platform_incidents=(first_incident, first_incident),
        )
    with pytest.raises(state.BackfillStateError, match="absent from batch history"):
        dataclasses.replace(
            blocked,
            open_platform_incident_id="0" * 64,
        )
    with pytest.raises(state.BackfillStateError, match="another batch"):
        dataclasses.replace(
            first,
            status=state.BatchStatus.BLOCKED_PLATFORM,
            platform_incidents=(second_incident,),
            open_platform_incident_id=second_incident.incident_id,
        )


def test_platform_incident_batch_roundtrip_hash_and_cas_sql_are_exact():
    campaign = _campaign(1)
    original = state.BackfillBatch.build(
        campaign.campaign_id, [campaign.targets[0].scope_id], now=NOW
    ).transition(state.BatchStatus.RUNNING, now=NOW + timedelta(seconds=1))
    incident = state.BackfillPlatformIncident.build(
        original,
        phase="state_write",
        error_class="CasReadbackMismatch",
        report_uri="s3://ops/incidents/state-write.json",
        report_sha256="f" * 64,
        raw_evidence_ids=(RAW_ONE,),
        now=NOW + timedelta(seconds=2),
    )
    blocked = original.record_platform_incident(
        incident, now=NOW + timedelta(seconds=3)
    )
    row = _persisted_row(
        blocked,
        batch_id=blocked.batch_id,
        campaign_id=blocked.campaign_id,
        status=blocked.status.value,
        revision=blocked.revision,
    )

    assert state.parse_batch_row(row) == blocked
    assert state.record_sha256(blocked) != state.record_sha256(original)
    sql = state.batch_transition_merge_sql(original, blocked)
    assert incident.incident_id in sql
    assert f"BIGINT '{original.revision}'" in sql

    corrupt_payload = state.record_payload(blocked)
    corrupt_payload["platform_incidents"][0][
        "pre_incident_batch_revision"
    ] += 1
    with pytest.raises(state.BackfillStateError, match="incident content"):
        state.batch_from_mapping(corrupt_payload)

    legacy_payload = state.record_payload(original)
    legacy_payload.pop("platform_incidents")
    legacy_payload.pop("open_platform_incident_id")
    assert state.batch_from_mapping(legacy_payload) == original


def test_ddl_and_merge_builders_are_deterministic_cas_and_escape_values():
    campaign = _campaign(1)
    initial = state.BackfillCampaign.build(
        registry_snapshot_id="registry-frozen-1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser's-v3",
        schema_revision="schema-v2",
        targets=[_target(0)],
        now=NOW,
    )
    scope = state.BackfillScopeState.initial(initial, initial.targets[0], now=NOW)
    batch = state.BackfillBatch.build(
        initial.campaign_id, [scope.target.scope_id], now=NOW
    )

    ddl = state.ddl_statements()
    assert len(ddl) == 4
    assert all("CREATE TABLE IF NOT EXISTS iceberg.ops" in item for item in ddl)
    campaign_sql = state.campaign_merge_sql(initial)
    assert "source.expected_revision = BIGINT '-1'" in campaign_sql
    assert "parser''s-v3" in campaign_sql
    assert state.scope_merge_sql(scope) == state.scope_merge_sql(scope)
    assert "target.revision = source.expected_revision" in state.batch_merge_sql(batch)

    leased = state.claim_scopes(
        campaign, _scopes(campaign), lease_owner="worker", now=NOW
    ).scopes[0]
    attempt = _attempt(
        leased, state.AttemptOutcome.SOURCE_ERROR, at=NOW + timedelta(minutes=1)
    )
    attempt_sql = state.attempt_merge_sql(attempt)
    assert f"MERGE INTO {state.ATTEMPT_TABLE}" in attempt_sql
    assert "target.record_sha256 = source.record_sha256" in attempt_sql


def test_record_hash_binds_checkpoint_and_raw_evidence():
    campaign = _campaign(1)
    scope = _scopes(campaign)[0]
    changed = dataclasses.replace(
        scope,
        checkpoint_uri="s3://ops/checkpoint.json",
        checkpoint_sha256=CHECKPOINT_HASH,
        raw_evidence_ids=(RAW_ONE,),
    )
    assert state.record_sha256(scope) != state.record_sha256(changed)


def _persisted_row(record, **projections):
    payload = state.record_payload(record)
    return {
        "record_json": state.canonical_json(payload),
        "record_sha256": state.canonical_sha256(payload),
        **projections,
    }


def test_row_parsers_restore_exact_records_and_reject_projection_or_hash_drift():
    campaign = _campaign(1)
    scope = _scopes(campaign)[0]
    batch = state.BackfillBatch.build(
        campaign.campaign_id, [scope.target.scope_id], now=NOW
    )
    leased = state.claim_scopes(
        campaign, [scope], lease_owner="worker", now=NOW
    ).scopes[0]
    attempt = _attempt(
        leased, state.AttemptOutcome.SOURCE_ERROR, at=NOW + timedelta(minutes=1)
    )

    assert state.parse_campaign_row(_persisted_row(
        campaign,
        campaign_id=campaign.campaign_id,
        status=campaign.status.value,
        revision=campaign.revision,
    )) == campaign
    assert state.parse_scope_row(_persisted_row(
        scope,
        campaign_id=scope.campaign_id,
        scope_id=scope.target.scope_id,
        status=scope.status.value,
        revision=scope.revision,
    )) == scope
    assert state.parse_batch_row(_persisted_row(
        batch,
        batch_id=batch.batch_id,
        campaign_id=batch.campaign_id,
        status=batch.status.value,
        revision=batch.revision,
    )) == batch
    assert state.parse_attempt_row(_persisted_row(
        attempt,
        attempt_id=attempt.attempt_id,
        campaign_id=attempt.campaign_id,
        scope_id=attempt.scope_id,
        sequence=attempt.sequence,
        outcome=attempt.outcome.value,
    )) == attempt

    bad_projection = _persisted_row(
        scope, scope_id="another", revision=scope.revision
    )
    with pytest.raises(state.BackfillStateError, match="projected scope_id"):
        state.parse_scope_row(bad_projection)
    bad_hash = _persisted_row(scope)
    bad_hash["record_sha256"] = "0" * 64
    with pytest.raises(state.BackfillStateError, match="mismatch"):
        state.parse_scope_row(bad_hash)


def test_claim_and_attempt_transition_sql_bind_prior_revision_and_readback():
    campaign = _campaign(1)
    previous = _scopes(campaign)
    claim = state.claim_scopes(
        campaign, previous, lease_owner="worker", now=NOW
    )
    statements = state.claim_merge_statements(previous, claim)
    assert len(statements) == 2
    assert f"MERGE INTO {state.BATCH_TABLE}" in statements[0]
    assert "source.expected_revision = BIGINT '-1'" in statements[1]

    leased = claim.scopes[0]
    attempt = _attempt(
        leased, state.AttemptOutcome.SOURCE_ERROR, at=NOW + timedelta(minutes=1)
    )
    current = state.apply_attempt(leased, attempt)
    attempt_sql, scope_sql = state.attempt_transition_merge_statements(
        leased, attempt, current
    )
    assert f"MERGE INTO {state.ATTEMPT_TABLE}" in attempt_sql
    assert f"MERGE INTO {state.SCOPE_TABLE}" in scope_sql
    assert state.record_readback_sql(current).endswith(
        f"scope_id = '{current.target.scope_id}'"
    )


def test_readback_verifier_requires_exactly_one_exact_hash_and_revision():
    campaign = _campaign(1)
    good = {
        "record_sha256": state.canonical_sha256(
            state.campaign_storage_payload(campaign)
        ),
        "revision": campaign.revision,
    }
    state.verify_record_readback(campaign, [good])

    with pytest.raises(state.BackfillStateError, match="exactly one"):
        state.verify_record_readback(campaign, [])
    with pytest.raises(state.BackfillStateError, match="exactly one"):
        state.verify_record_readback(campaign, [good, good])
    with pytest.raises(state.BackfillStateError, match="hash mismatch"):
        state.verify_record_readback(
            campaign, [{**good, "record_sha256": "0" * 64}]
        )
    with pytest.raises(state.BackfillStateError, match="revision mismatch"):
        state.verify_record_readback(campaign, [{**good, "revision": 99}])
