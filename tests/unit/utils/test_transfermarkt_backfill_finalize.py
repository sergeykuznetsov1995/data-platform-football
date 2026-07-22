from __future__ import annotations

from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from utils import transfermarkt_backfill_finalize as finalize
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_backfill_artifacts import BackfillArtifactError
from utils.transfermarkt_backfill_attempts import ClassifiedScopeAttempt
from scrapers.transfermarkt.raw_store import RawResponseStore


NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _dq_report(campaign, batch, pins, *, passed=True):
    return SimpleNamespace(
        passed=passed,
        snapshot_ids=pins,
        as_dict=lambda: {
            "campaign_id": campaign.campaign_id,
            "batch_id": batch.batch_id,
            "registry_snapshot_id": campaign.registry_snapshot_id,
            "snapshot_ids": dict(sorted(pins.items())),
            "child_cycle_ids": ["child-cycle"],
            "bronze_checks": [],
            "raw_lineage": {
                "capture_count": 1,
                "capture_set_hash": "f" * 64,
                "partial_capture_count": 0,
                "partial_capture_inventory": [],
                "rows_by_table": {},
            },
            "passed": passed,
        },
    )


def _planned_environment(campaign, scope, batch, base, *, finalize_only=False):
    payload = {
        "parent_cycle_id": batch.batch_id,
        "resume_cycle_id": campaign.campaign_id,
        "child_cycle_id": "child-cycle",
        "scope_id": scope.target.scope_id,
        "competition_id": scope.target.competition_id,
        "edition_id": scope.target.edition_id,
        "canonical_competition_id": scope.target.canonical_competition_id,
        "canonical_season": scope.target.canonical_season,
        "registry_snapshot_id": campaign.registry_snapshot_id,
        "capture_revision": "v3",
        "result_paths": {
            "base_dir": str(base),
            "entity_staging_dir": str(base / "entities"),
            "scope_manifest": str(base / "scope-manifest.json"),
        },
    }
    return {
        "TM_DAG_ID": "dag_backfill_transfermarkt",
        "TM_WRITE_MODE": "native-only",
        "TM_STANDING_POLICY_SHA256": campaign.policy_sha256,
        "TM_BACKFILL_BATCH_ID": batch.batch_id,
        "TM_BACKFILL_CLAIM_GENERATION": str(scope.claim_generation),
        "TM_BACKFILL_ATTEMPT_SEQUENCE": str(
            scope.attempt_count
            + (1 if scope.status is state.ScopeStatus.RUNNING else 0)
        ),
        "TM_BACKFILL_FINALIZE_ONLY": "true" if finalize_only else "false",
        "TM_SCOPE_PAYLOAD_JSON": json.dumps(payload),
    }


def _campaign_and_claim():
    target = state.HistoricalScopeTarget(
        scope_id="GB1__2020",
        competition_id="GB1",
        edition_id="2020",
        canonical_competition_id="TM-GB1",
        canonical_season="2021",
        registry_snapshot_id="registry-1",
    )
    waiting = state.BackfillCampaign.build(
        registry_snapshot_id="registry-1",
        policy_sha256="a" * 64,
        parser_revision="parser-v1",
        schema_revision="schema-v1",
        targets=[target],
        now=NOW,
    )
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    scope = state.BackfillScopeState.initial(campaign, target, now=NOW)
    claim = state.claim_scopes(
        campaign,
        [scope],
        lease_owner="worker",
        now=NOW,
    )
    assert claim.batch is not None
    return campaign, claim.scopes[0], claim.batch


def test_manifest_from_another_scope_is_rejected(monkeypatch):
    campaign, scope, _batch = _campaign_and_claim()
    swapped = SimpleNamespace(
        digest="d" * 64,
        validate=lambda _entities: None,
        dq_evidence={
            "silver_trigger_allowed": False,
            "standing_policy_hash": campaign.policy_sha256,
        },
        scope_id="GB1__2019",
        competition_id=scope.target.competition_id,
        edition_id="2019",
        canonical_competition_id=scope.target.canonical_competition_id,
        canonical_season="2020",
        registry_snapshot_id=campaign.registry_snapshot_id,
        parser_revision="v2",
        schema_revision="2",
        parent_cycle_id="other-batch",
        child_cycle_id="other-child",
        capture_revision="v3",
    )
    monkeypatch.setattr(
        finalize.ScopeManifest,
        "from_mapping",
        staticmethod(lambda _value: swapped),
    )

    with pytest.raises(finalize.BackfillFinalizeError, match="scope_id"):
        finalize._validated_manifest(
            {"manifest_digest": swapped.digest},
            policy_sha256=campaign.policy_sha256,
            campaign=campaign,
            scope=scope,
        )


class _Repository:
    def __init__(self, campaign, scope, batch):
        self.campaign = campaign
        self.scopes = {scope.target.scope_id: scope}
        self.batch = batch
        self.attempts = []
        self.cursor = object()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def load_campaign(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return self.campaign

    def load_scopes(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return tuple(self.scopes.values())

    def load_batch(self, batch_id):
        assert batch_id == self.batch.batch_id
        return self.batch

    def load_attempts(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return tuple(self.attempts)

    def load_batches(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return (self.batch,)

    def persist_batch_transition(self, previous, current):
        assert previous == self.batch
        self.batch = current

    def persist_campaign_transition(self, previous, current):
        assert previous == self.campaign
        self.campaign = current

    def persist_scope_transition(self, previous, current):
        assert previous == self.scopes[previous.target.scope_id]
        self.scopes[current.target.scope_id] = current

    def persist_attempt(self, campaign, previous, attempt):
        plan = state.plan_attempt_transition(campaign, previous, attempt)
        self.attempts.append(attempt)
        self.scopes[plan.scope.target.scope_id] = plan.scope
        self.campaign = plan.campaign
        return plan

    def resume_persisted_attempt(self, campaign, previous, attempt):
        plan = state.plan_attempt_transition(campaign, previous, attempt)
        self.scopes[plan.scope.target.scope_id] = plan.scope
        self.campaign = plan.campaign
        return plan


def test_successful_batch_is_dq_gated_and_completes_exact_campaign(
    tmp_path,
    monkeypatch,
):
    campaign, scope, batch = _campaign_and_claim()
    repository = _Repository(campaign, scope, batch)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    capture = raw_store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"raw",
        status_code=200,
        headers={"content-type": "text/html"},
        fetched_at=NOW.isoformat(),
        cycle_id="child-cycle",
        scope_id=scope.target.scope_id,
        endpoint="players",
        attempt=1,
    )
    envelope = raw_store.store_response_envelope(capture)
    base = tmp_path / "result"
    entity_dir = base / "entities"
    entity_dir.mkdir(parents=True)
    manifest_path = base / "scope-manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    planned = [_planned_environment(campaign, scope, batch, base)]
    monkeypatch.setattr(
        finalize.BackfillStateRepository,
        "connect",
        lambda: repository,
    )
    monkeypatch.setattr(
        finalize.RawResponseStore,
        "from_env",
        lambda optional=False: raw_store,
    )
    monkeypatch.setattr(
        finalize,
        "collect_scope_attempt_evidence",
        lambda **_kwargs: ClassifiedScopeAttempt(
            outcome=state.AttemptOutcome.CAPTURED,
            raw_evidence_ids=(envelope.envelope_id,),
            error_class=None,
            error_message=None,
            retry_after_seconds=None,
            manifest_path=str(manifest_path),
            checkpoint_path=None,
            observed_at=NOW,
        ),
    )
    manifest = SimpleNamespace(
        child_cycle_id="child-cycle",
        dq_evidence={"silver_trigger_allowed": False},
    )
    monkeypatch.setattr(finalize, "_manifest", lambda _path, **_kwargs: manifest)
    monkeypatch.setattr(
        finalize, "_persisted_manifest", lambda *_args, **_kwargs: manifest
    )
    dq_report = _dq_report(
        campaign, batch, {"iceberg.bronze.table": 7}
    )
    monkeypatch.setattr(
        finalize,
        "run_backfill_batch_dq",
        lambda *_args, **_kwargs: dq_report,
    )
    monkeypatch.setattr(
        finalize,
        "pin_iceberg_snapshots",
        lambda _cursor: {"iceberg.bronze.table": 7},
    )

    result = finalize.finalize_backfill_batch(planned)

    assert result["status"] == "campaign_complete"
    assert result["silver_trigger_allowed"] is False
    assert result["accounting"]["target_total"] == 1
    assert result["accounting"]["complete"] == 1
    assert repository.batch.status is state.BatchStatus.COMPLETE
    assert repository.campaign.status is state.CampaignStatus.COMPLETE
    assert repository.campaign.report_uri == result["campaign_report_uri"]

    monkeypatch.setattr(
        finalize,
        "collect_scope_attempt_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("source reread")),
    )
    monkeypatch.setattr(
        finalize,
        "run_backfill_batch_dq",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("DQ rerun")),
    )

    retried = finalize.finalize_backfill_batch(planned)

    assert retried == result


def test_attempt_journal_is_resumed_without_running_source_again(tmp_path, monkeypatch):
    campaign, scope, batch = _campaign_and_claim()
    repository = _Repository(campaign, scope, batch)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    capture = raw_store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"source failed",
        status_code=503,
        headers={"content-type": "text/html"},
        fetched_at=NOW.isoformat(),
        cycle_id="child-cycle",
        scope_id=scope.target.scope_id,
        endpoint="players",
        attempt=1,
    )
    envelope = raw_store.store_response_envelope(capture)
    persisted = state.BackfillAttempt.build(
        scope=scope,
        batch_id=batch.batch_id,
        outcome=state.AttemptOutcome.SOURCE_ERROR,
        started_at=NOW,
        finished_at=NOW,
        raw_evidence_ids=(envelope.envelope_id,),
        source_observed_at=NOW,
        error_class="http_503",
    )
    repository.attempts.append(persisted)
    planned = [_planned_environment(
        campaign, scope, batch, tmp_path / "missing", finalize_only=True,
    )]
    monkeypatch.setattr(finalize.BackfillStateRepository, "connect", lambda: repository)
    monkeypatch.setattr(
        finalize.RawResponseStore,
        "from_env",
        lambda optional=False: raw_store,
    )
    monkeypatch.setattr(
        finalize,
        "collect_scope_attempt_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("source reread")),
    )
    monkeypatch.setattr(finalize, "pin_iceberg_snapshots", lambda _cursor: {"table": 7})
    dq_report = _dq_report(campaign, batch, {"table": 7})
    monkeypatch.setattr(
        finalize,
        "run_backfill_batch_dq",
        lambda *_args, **_kwargs: dq_report,
    )

    result = finalize.finalize_backfill_batch(planned)

    assert result["status"] == "batch_complete"
    assert repository.scopes[scope.target.scope_id].status is state.ScopeStatus.RETRYABLE_ERROR
    assert repository.batch.status is state.BatchStatus.COMPLETE
    assert repository.attempts == [persisted]


def test_dq_pending_batch_reuses_pins_and_finishes_scope_and_campaign(
    tmp_path,
    monkeypatch,
):
    campaign, scope, batch = _campaign_and_claim()
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    capture = raw_store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"captured",
        status_code=200,
        headers={"content-type": "text/html"},
        fetched_at=NOW.isoformat(),
        cycle_id="child-cycle",
        scope_id=scope.target.scope_id,
        endpoint="players",
        attempt=1,
    )
    envelope = raw_store.store_response_envelope(capture)
    artifact = finalize.BackfillArtifactStore(raw_store).publish_json(
        {"placeholder": True},
        kind="scope_manifest",
        owner_id=campaign.campaign_id,
    )
    attempt = state.BackfillAttempt.build(
        scope=scope,
        batch_id=batch.batch_id,
        outcome=state.AttemptOutcome.CAPTURED,
        started_at=NOW,
        finished_at=NOW,
        raw_evidence_ids=(envelope.envelope_id,),
        source_observed_at=NOW,
        scope_manifest_uri=artifact.uri,
        scope_manifest_sha256=artifact.sha256,
    )
    captured_scope = state.apply_attempt(scope, attempt)
    running = batch.transition(state.BatchStatus.RUNNING, now=NOW)
    dq_pending = running.transition(
        state.BatchStatus.DQ_PENDING,
        now=NOW,
        snapshot_pins={"table": 17},
        raw_evidence_ids=(envelope.envelope_id,),
    )
    repository = _Repository(campaign, captured_scope, dq_pending)
    repository.attempts.append(attempt)
    planned = [_planned_environment(
        campaign, captured_scope, batch, tmp_path / "missing", finalize_only=True,
    )]
    monkeypatch.setattr(finalize.BackfillStateRepository, "connect", lambda: repository)
    monkeypatch.setattr(
        finalize.RawResponseStore,
        "from_env",
        lambda optional=False: raw_store,
    )
    manifest = SimpleNamespace(
        child_cycle_id="child-cycle",
        dq_evidence={"silver_trigger_allowed": False},
    )
    monkeypatch.setattr(
        finalize, "_persisted_manifest", lambda *_args, **_kwargs: manifest
    )
    observed_pins = []
    dq_report = _dq_report(campaign, batch, {"table": 17})

    def _dq(*_args, **kwargs):
        observed_pins.append(kwargs["pins"])
        return dq_report

    monkeypatch.setattr(finalize, "run_backfill_batch_dq", _dq)

    result = finalize.finalize_backfill_batch(planned)

    assert observed_pins == [{"table": 17}]
    assert result["status"] == "campaign_complete"
    assert repository.batch.status is state.BatchStatus.COMPLETE
    assert repository.scopes[scope.target.scope_id].status is state.ScopeStatus.COMPLETE


def test_failed_dq_is_published_as_resumable_batch_incident(
    tmp_path,
    monkeypatch,
):
    campaign, scope, batch = _campaign_and_claim()
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    capture = raw_store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"captured",
        status_code=200,
        headers={"content-type": "text/html"},
        fetched_at=NOW.isoformat(),
        cycle_id="child-cycle",
        scope_id=scope.target.scope_id,
        endpoint="players",
        attempt=1,
    )
    envelope = raw_store.store_response_envelope(capture)
    artifact_store = finalize.BackfillArtifactStore(raw_store)
    artifact = artifact_store.publish_json(
        {"placeholder": True},
        kind="scope_manifest",
        owner_id=campaign.campaign_id,
    )
    attempt = state.BackfillAttempt.build(
        scope=scope,
        batch_id=batch.batch_id,
        outcome=state.AttemptOutcome.CAPTURED,
        started_at=NOW,
        finished_at=NOW,
        raw_evidence_ids=(envelope.envelope_id,),
        source_observed_at=NOW,
        scope_manifest_uri=artifact.uri,
        scope_manifest_sha256=artifact.sha256,
    )
    captured_scope = state.apply_attempt(scope, attempt)
    dq_pending = batch.transition(state.BatchStatus.RUNNING, now=NOW).transition(
        state.BatchStatus.DQ_PENDING,
        now=NOW,
        snapshot_pins={"table": 17},
        raw_evidence_ids=(envelope.envelope_id,),
    )
    repository = _Repository(campaign, captured_scope, dq_pending)
    repository.attempts.append(attempt)
    planned = [_planned_environment(
        campaign, captured_scope, batch, tmp_path / "missing", finalize_only=True,
    )]
    monkeypatch.setattr(finalize.BackfillStateRepository, "connect", lambda: repository)
    monkeypatch.setattr(
        finalize.RawResponseStore,
        "from_env",
        lambda optional=False: raw_store,
    )
    manifest = SimpleNamespace(
        child_cycle_id="child-cycle",
        dq_evidence={"silver_trigger_allowed": False},
    )
    monkeypatch.setattr(
        finalize, "_persisted_manifest", lambda *_args, **_kwargs: manifest
    )
    observed_pins = []
    failed_report = _dq_report(campaign, batch, {"table": 17}, passed=False)

    def _dq(*_args, **kwargs):
        observed_pins.append(kwargs["pins"])
        return failed_report

    monkeypatch.setattr(finalize, "run_backfill_batch_dq", _dq)
    monkeypatch.setattr(
        finalize,
        "collect_scope_attempt_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("source reread")),
    )

    with pytest.raises(finalize.BackfillFinalizeError, match="DQ did not pass"):
        finalize.finalize_backfill_batch(planned)

    assert observed_pins == [{"table": 17}]
    assert repository.campaign.status is state.CampaignStatus.BLOCKED_PLATFORM
    assert repository.batch.status is state.BatchStatus.BLOCKED_PLATFORM
    assert repository.batch.snapshot_pins == {"table": 17}
    incident_id = repository.batch.open_platform_incident_id
    assert incident_id is not None
    incident = repository.batch.platform_incidents[-1]
    assert incident.incident_id == incident_id
    assert incident.blocked_from_status is state.BatchStatus.DQ_PENDING
    report = artifact_store.load_json(
        incident.report_uri,
        expected_sha256=incident.report_sha256,
    )
    assert report["incident_id"] == incident_id
    assert report["cause_artifact"] is not None


def test_complete_batch_finishes_captured_scope_without_source_or_dq(
    tmp_path,
    monkeypatch,
):
    campaign, scope, batch = _campaign_and_claim()
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    capture = raw_store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"captured",
        status_code=200,
        headers={"content-type": "text/html"},
        fetched_at=NOW.isoformat(),
        cycle_id="child-cycle",
        scope_id=scope.target.scope_id,
        endpoint="players",
        attempt=1,
    )
    envelope = raw_store.store_response_envelope(capture)
    artifact_store = finalize.BackfillArtifactStore(raw_store)
    manifest_artifact = artifact_store.publish_json(
        {"placeholder": True},
        kind="scope_manifest",
        owner_id=campaign.campaign_id,
    )
    attempt = state.BackfillAttempt.build(
        scope=scope,
        batch_id=batch.batch_id,
        outcome=state.AttemptOutcome.CAPTURED,
        started_at=NOW,
        finished_at=NOW,
        raw_evidence_ids=(envelope.envelope_id,),
        source_observed_at=NOW,
        scope_manifest_uri=manifest_artifact.uri,
        scope_manifest_sha256=manifest_artifact.sha256,
    )
    captured_scope = state.apply_attempt(scope, attempt)
    dq_payload = _dq_report(campaign, batch, {"table": 17}).as_dict()
    dq_payload["attempt_envelopes"] = {
        "count": 1,
        "ids": [envelope.envelope_id],
        "all_verified": True,
    }
    dq_payload["batch_attestation_sha256"] = state.canonical_sha256(dq_payload)
    dq_artifact = artifact_store.publish_json(
        dq_payload,
        kind="batch_dq",
        owner_id=campaign.campaign_id,
    )
    complete_batch = (
        batch.transition(state.BatchStatus.RUNNING, now=NOW)
        .transition(
            state.BatchStatus.DQ_PENDING,
            now=NOW,
            snapshot_pins={"table": 17},
            raw_evidence_ids=(envelope.envelope_id,),
        )
        .transition(
            state.BatchStatus.COMPLETE,
            now=NOW,
            dq_report_uri=dq_artifact.uri,
            dq_report_sha256=dq_artifact.sha256,
        )
    )
    repository = _Repository(campaign, captured_scope, complete_batch)
    repository.attempts.append(attempt)
    planned = [_planned_environment(
        campaign, captured_scope, batch, tmp_path / "missing", finalize_only=True,
    )]
    monkeypatch.setattr(finalize.BackfillStateRepository, "connect", lambda: repository)
    monkeypatch.setattr(
        finalize.RawResponseStore,
        "from_env",
        lambda optional=False: raw_store,
    )
    monkeypatch.setattr(
        finalize,
        "collect_scope_attempt_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("source reread")),
    )
    monkeypatch.setattr(
        finalize,
        "run_backfill_batch_dq",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("DQ rerun")),
    )
    manifest = SimpleNamespace(child_cycle_id="child-cycle")
    monkeypatch.setattr(
        finalize, "_persisted_manifest", lambda *_args, **_kwargs: manifest
    )

    result = finalize.finalize_backfill_batch(planned)

    assert result["status"] == "campaign_complete"
    assert repository.batch == complete_batch
    assert repository.scopes[scope.target.scope_id].status is state.ScopeStatus.COMPLETE


def test_campaign_completion_failure_blocks_with_complete_batch_incident(
    tmp_path,
    monkeypatch,
):
    campaign, scope, batch = _campaign_and_claim()
    complete_batch = (
        batch.transition(state.BatchStatus.RUNNING, now=NOW)
        .transition(
            state.BatchStatus.DQ_PENDING,
            now=NOW,
            snapshot_pins={"table": 17},
            raw_evidence_ids=("b" * 64,),
        )
        .transition(
            state.BatchStatus.COMPLETE,
            now=NOW,
            dq_report_uri="s3://raw/dq.json",
            dq_report_sha256="d" * 64,
        )
    )
    repository = _Repository(campaign, scope, complete_batch)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    artifact_store = finalize.BackfillArtifactStore(raw_store)
    monkeypatch.setattr(
        finalize,
        "_finish_completed_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            finalize.BackfillFinalizeError("campaign report corrupt")
        ),
    )

    with pytest.raises(finalize.BackfillFinalizeError, match="report corrupt"):
        finalize._finish_completed_batch_or_block(
            repository,
            campaign=campaign,
            batch=complete_batch,
            artifact_store=artifact_store,
            raw_store=raw_store,
            now=NOW,
        )

    assert repository.campaign.status is state.CampaignStatus.BLOCKED_PLATFORM
    assert repository.batch.status is state.BatchStatus.COMPLETE
    incident = repository.batch.platform_incidents[-1]
    assert repository.batch.open_platform_incident_id == incident.incident_id
    assert incident.blocked_from_status is state.BatchStatus.COMPLETE
    assert incident.phase == "campaign_completion"


def test_corrupt_old_complete_batch_attestation_blocks_final_campaign_close(
    tmp_path,
    monkeypatch,
):
    targets = tuple(
        state.HistoricalScopeTarget(
            scope_id=f"GB1__20{year}",
            competition_id="GB1",
            edition_id=f"20{year}",
            canonical_competition_id="TM-GB1",
            canonical_season=f"20{year + 1}",
            registry_snapshot_id="registry-1",
        )
        for year in (19, 20)
    )
    waiting = state.BackfillCampaign.build(
        registry_snapshot_id="registry-1",
        policy_sha256="a" * 64,
        parser_revision="parser-v1",
        schema_revision="schema-v1",
        targets=targets,
        now=NOW,
    )
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    scopes = tuple(
        state.BackfillScopeState.initial(campaign, target, now=NOW)
        for target in campaign.targets
    )

    attempts = []
    batches = []
    finished = []
    current_scopes = scopes
    for index, raw_id in enumerate(("a" * 64, "b" * 64), start=1):
        claimed = state.claim_scopes(
            campaign,
            current_scopes,
            lease_owner=f"worker-{index}",
            now=NOW,
            limit=1,
        )
        assert claimed.batch is not None
        running_scope = next(
            item for item in claimed.scopes
            if item.batch_id == claimed.batch.batch_id
        )
        attempt = state.BackfillAttempt.build(
            scope=running_scope,
            batch_id=claimed.batch.batch_id,
            outcome=state.AttemptOutcome.CAPTURED,
            started_at=NOW,
            finished_at=NOW,
            raw_evidence_ids=(raw_id,),
            source_observed_at=NOW,
            scope_manifest_uri=f"s3://raw/scope-{index}.json",
            scope_manifest_sha256="c" * 64,
        )
        captured = state.apply_attempt(running_scope, attempt)
        completed_scope = state.mark_scope_dq_complete(
            captured,
            scope_manifest_uri=str(captured.scope_manifest_uri),
            scope_manifest_sha256=str(captured.scope_manifest_sha256),
            now=NOW,
        )
        current_scopes = tuple(
            completed_scope
            if item.target.scope_id == completed_scope.target.scope_id
            else item
            for item in claimed.scopes
        )
        complete_batch = (
            claimed.batch.transition(state.BatchStatus.RUNNING, now=NOW)
            .transition(
                state.BatchStatus.DQ_PENDING,
                now=NOW,
                snapshot_pins={"table": 17},
                raw_evidence_ids=(raw_id,),
            )
            .transition(
                state.BatchStatus.COMPLETE,
                now=NOW,
                dq_report_uri=(
                    f"{(tmp_path / 'raw').as_uri()}/backfill/v1/"
                    f"batch_dq/{campaign.campaign_id}/{index:064x}.json"
                ),
                dq_report_sha256=f"{index:064x}",
            )
        )
        attempts.append(attempt)
        batches.append(complete_batch)
        finished.append(completed_scope)

    class _TwoBatchRepository(_Repository):
        def load_batches(self, campaign_id):
            assert campaign_id == self.campaign.campaign_id
            return tuple(batches)

    repository = _TwoBatchRepository(campaign, finished[-1], batches[-1])
    repository.scopes = {
        item.target.scope_id: item for item in current_scopes
    }
    repository.attempts = list(attempts)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    artifact_store = finalize.BackfillArtifactStore(raw_store)
    original_verify = finalize._verify_completed_batch_evidence

    def _verify(repository_arg, *, batch, **kwargs):
        if batch.batch_id == batches[-1].batch_id:
            return None
        return original_verify(repository_arg, batch=batch, **kwargs)

    monkeypatch.setattr(finalize, "_verify_completed_batch_evidence", _verify)

    with pytest.raises(BackfillArtifactError, match="cannot be read"):
        finalize._finish_completed_batch_or_block(
            repository,
            campaign=campaign,
            batch=batches[-1],
            artifact_store=artifact_store,
            raw_store=raw_store,
            now=NOW,
        )

    assert repository.campaign.status is state.CampaignStatus.BLOCKED_PLATFORM
    assert repository.campaign.report_uri is None
    assert repository.batch.status is state.BatchStatus.COMPLETE
    assert repository.batch.platform_incidents[-1].phase == "campaign_completion"
