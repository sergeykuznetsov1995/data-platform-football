from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from utils import transfermarkt_backfill_runtime as runtime
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_backfill_artifacts import BackfillArtifactStore
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
    CompetitionRecord,
    CompetitionType,
    EditionRecord,
    EvidenceOrigin,
    Gender,
    SeasonFormat,
    TeamType,
    canonical_season,
)


NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
POLICY_HASH = "a" * 64


class _Cursor:
    def close(self):
        pass


class _Connection:
    def cursor(self):
        return _Cursor()

    def close(self):
        pass


class _HealthResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            "transfermarkt_backfill_paid_enabled": True,
            "transfermarkt_requests_per_minute": 12,
            "transfermarkt_backfill_uses_production_daily_budget": False,
            "transfermarkt_request_permit_consume_required": True,
            "transfermarkt_request_permit_state_durable": True,
            "transfermarkt_request_permit_pending_ttl_seconds": 90,
            "transfermarkt_backfill_max_queue_seconds": 55,
        }


def _competition(snapshot: str = "registry-1") -> CompetitionRecord:
    evidence = ClassificationEvidence(
        source_field="structured_metadata",
        source_value="senior men club league",
        source_url="https://www.transfermarkt.com/gb1",
        origin=EvidenceOrigin.STRUCTURED,
        competition_type=CompetitionType.DOMESTIC_LEAGUE,
        gender=Gender.MEN,
        team_type=TeamType.CLUB,
        age_category=AgeCategory.SENIOR,
        season_format=SeasonFormat.SPLIT_YEAR,
    )
    return CompetitionRecord(
        competition_id="GB1",
        slug="premier-league",
        name="Premier League",
        country="England",
        confederation="UEFA",
        competition_type=CompetitionType.DOMESTIC_LEAGUE,
        gender=Gender.MEN,
        team_type=TeamType.CLUB,
        age_category=AgeCategory.SENIOR,
        season_format=SeasonFormat.SPLIT_YEAR,
        active=True,
        source_url="https://www.transfermarkt.com/gb1",
        discovered_at=NOW,
        canonical_competition_id="TM-GB1",
        evidence=(evidence,),
        registry_snapshot_id=snapshot,
        source_body_hash="competition-hash",
    )


def _edition(
    edition_id: str,
    *,
    current: bool,
    snapshot: str = "registry-1",
) -> EditionRecord:
    return EditionRecord(
        competition_id="GB1",
        edition_id=edition_id,
        edition_label=edition_id,
        canonical_season=canonical_season(edition_id, SeasonFormat.SPLIT_YEAR),
        season_format=SeasonFormat.SPLIT_YEAR,
        start_date=None,
        end_date=None,
        active=True,
        current=current,
        participant_count=20,
        participant_hash=f"participants-{edition_id}",
        source_url=f"https://www.transfermarkt.com/gb1/{edition_id}",
        discovered_at=NOW,
        registry_snapshot_id=snapshot,
        source_body_hash=f"edition-hash-{edition_id}",
    )


def _row(competition: CompetitionRecord, edition: EditionRecord) -> dict:
    return {
        "competition_id": competition.competition_id,
        "slug": competition.slug,
        "name": competition.name,
        "country": competition.country,
        "confederation": competition.confederation,
        "competition_type": competition.competition_type.value,
        "gender": competition.gender.value,
        "team_type": competition.team_type.value,
        "age_category": competition.age_category.value,
        "competition_season_format": competition.season_format.value,
        "competition_active": competition.active,
        "competition_source_url": competition.source_url,
        "competition_discovered_at": competition.discovered_at.isoformat(),
        "canonical_competition_id": competition.canonical_competition_id,
        "classification_status": competition.classification_status.value,
        "classification_evidence": json.dumps(
            [item.as_dict() for item in competition.evidence]
        ),
        "competition_source_body_hash": competition.source_body_hash,
        "competition_parser_revision": competition.parser_revision,
        "competition_schema_revision": competition.schema_revision,
        "edition_id": edition.edition_id,
        "edition_label": edition.edition_label,
        "canonical_season": edition.canonical_season,
        "edition_season_format": edition.season_format.value,
        "start_date": None,
        "end_date": None,
        "edition_active": edition.active,
        "is_current": edition.current,
        "participant_count": edition.participant_count,
        "participant_hash": edition.participant_hash,
        "edition_source_url": edition.source_url,
        "edition_discovered_at": edition.discovered_at.isoformat(),
        "edition_source_body_hash": edition.source_body_hash,
        "edition_parser_revision": edition.parser_revision,
        "edition_schema_revision": edition.schema_revision,
        "registry_snapshot_id": edition.registry_snapshot_id,
        "last_success_at": None,
        "career_fetches_pending": 0,
    }


def _state_row(record, **projections):
    payload = (
        state.campaign_storage_payload(record)
        if isinstance(record, state.BackfillCampaign)
        else state.record_payload(record)
    )
    return {
        "record_json": state.canonical_json(payload),
        "record_sha256": state.canonical_sha256(payload),
        **projections,
    }


def test_waiting_pointer_recovers_old_snapshot_delta_after_canonical_advances(
    monkeypatch,
):
    competition = _competition("snapshot-s1")
    registry_s1 = [
        _row(competition, _edition(year, current=False, snapshot="snapshot-s1"))
        for year in ("2018", "2019", "2020")
    ]
    all_targets = runtime.historical_targets_from_registry(registry_s1)
    prior_waiting = state.BackfillCampaign.build(
        registry_snapshot_id="snapshot-prior",
        policy_sha256=POLICY_HASH,
        parser_revision="parser",
        schema_revision="schema",
        targets=(replace(all_targets[0], registry_snapshot_id="snapshot-prior"),),
        now=NOW - timedelta(days=1),
    )
    prior = prior_waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    prior_scope = state.BackfillScopeState.initial(
        prior, prior.targets[0], now=prior.created_at,
    )
    waiting = state.BackfillCampaign.build(
        registry_snapshot_id="snapshot-s1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser",
        schema_revision="schema",
        targets=all_targets[1:],
        now=NOW,
    )
    partial = state.BackfillScopeState.initial(
        waiting, waiting.targets[0], now=waiting.created_at,
    )
    pointer_row = _state_row(
        waiting,
        campaign_id=waiting.campaign_id,
        target_sha256=waiting.target_sha256,
        target_count=2,
        status=waiting.status.value,
        revision=waiting.revision,
    )
    partial_row = _state_row(
        partial,
        campaign_id=waiting.campaign_id,
        scope_id=partial.target.scope_id,
        status=partial.status.value,
        revision=partial.revision,
    )
    prior_row = _state_row(
        prior_scope,
        campaign_id=prior.campaign_id,
        scope_id=prior_scope.target.scope_id,
        status=prior_scope.status.value,
        revision=prior_scope.revision,
    )

    requested = []

    registry_s2 = tuple(registry_s1[:1])

    def _pinned_registry(*, registry_snapshot_id=None, **_kwargs):
        requested.append(registry_snapshot_id)
        if registry_snapshot_id is None:
            return registry_s2
        assert registry_snapshot_id == "snapshot-s1"
        return tuple(registry_s1)

    monkeypatch.setattr(runtime, "read_promoted_registry", _pinned_registry)

    class _Repository(runtime.BackfillStateRepository):
        def __init__(self):
            self.missing = ()
            self.transitions = []
            self._prefetched_scopes = {}

        def query(self, sql):
            if "campaign_id <>" in sql:
                assert f"JOIN {state.CAMPAIGN_TABLE}" in sql
                return [prior_row]
            if f"FROM {state.CAMPAIGN_TABLE}" in sql:
                return [pointer_row]
            if f"FROM {state.SCOPE_TABLE}" in sql:
                assert waiting.campaign_id in sql
                return [partial_row]
            raise AssertionError(f"unexpected SQL: {sql}")

        def persist_initial_scopes(self, scopes):
            self.missing = tuple(scopes)

        def persist(self, record, _statement):
            self.transitions.append(record.status)

    repository = _Repository()
    recovered = repository.open_campaign()
    assert recovered is not None
    active, scopes = runtime.BackfillStateRepository.resume_waiting_campaign(
        repository, recovered
    )

    assert requested == ["snapshot-s1"]
    assert recovered.targets == waiting.targets
    assert tuple(item.target for item in repository.missing) == (waiting.targets[1],)
    assert tuple(item.target for item in scopes) == waiting.targets
    assert active.status is state.CampaignStatus.ACTIVE


def test_initialise_campaign_publishes_pointer_before_first_scope_chunk():
    target = state.HistoricalScopeTarget(
        scope_id="GB1__2020",
        competition_id="GB1",
        edition_id="2020",
        canonical_competition_id="TM-GB1",
        canonical_season="2021",
        registry_snapshot_id="snapshot-s1",
    )
    campaign = state.BackfillCampaign.build(
        registry_snapshot_id="snapshot-s1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser",
        schema_revision="schema",
        targets=(target,),
        now=NOW,
    )

    class _Repository(runtime.BackfillStateRepository):
        def __init__(self):
            self.events = []

        def _query_scopes(self, campaign_id):
            assert campaign_id == campaign.campaign_id
            return ()

        def persist(self, record, _statement):
            self.events.append(f"campaign:{record.status.value}")

        def persist_initial_scopes(self, scopes):
            assert len(tuple(scopes)) == 1
            self.events.append("scope_chunk")
            raise RuntimeError("injected scope chunk failure")

    repository = _Repository()
    with pytest.raises(RuntimeError, match="scope chunk failure"):
        repository.initialise_campaign(campaign)

    assert repository.events == [
        "campaign:waiting_prerequisite",
        "scope_chunk",
    ]


@pytest.mark.parametrize("kind", ("duplicate", "extra"))
def test_resume_waiting_campaign_rejects_non_exact_partial_denominator(kind):
    target = state.HistoricalScopeTarget(
        scope_id="GB1__2020",
        competition_id="GB1",
        edition_id="2020",
        canonical_competition_id="TM-GB1",
        canonical_season="2021",
        registry_snapshot_id="snapshot-s1",
    )
    campaign = state.BackfillCampaign.build(
        registry_snapshot_id="snapshot-s1",
        policy_sha256=POLICY_HASH,
        parser_revision="parser",
        schema_revision="schema",
        targets=(target,),
        now=NOW,
    )
    expected = state.BackfillScopeState.initial(campaign, target, now=NOW)
    if kind == "duplicate":
        persisted = (expected, expected)
        error = "duplicate"
    else:
        extra_target = replace(
            target,
            scope_id="GB1__2019",
            edition_id="2019",
            canonical_season="2020",
        )
        persisted = (
            expected,
            replace(expected, target=extra_target),
        )
        error = "extra"

    class _Repository(runtime.BackfillStateRepository):
        def __init__(self):
            pass

        def load_scopes(self, campaign_id):
            assert campaign_id == campaign.campaign_id
            return persisted

    with pytest.raises(runtime.BackfillRuntimeError, match=error):
        _Repository().resume_waiting_campaign(campaign)


def test_large_denominator_uses_bounded_bulk_chunks_with_exact_readback(
    monkeypatch,
):
    targets = tuple(
        state.HistoricalScopeTarget(
            scope_id=f"C{index:05d}__E{index:05d}",
            competition_id=f"C{index:05d}",
            edition_id=f"E{index:05d}",
            canonical_competition_id=f"TM-C{index:05d}",
            canonical_season=str(1900 + index),
            registry_snapshot_id="snapshot-scale",
        )
        for index in range(9_700)
    )
    campaign = state.BackfillCampaign.build(
        registry_snapshot_id="snapshot-scale",
        policy_sha256=POLICY_HASH,
        parser_revision="parser",
        schema_revision="schema",
        targets=targets,
        now=NOW,
    )
    scopes = tuple(
        state.BackfillScopeState.initial(campaign, target, now=NOW)
        for target in campaign.targets
    )
    current_chunk = []
    chunk_sizes = []
    original_readback = state.initial_scope_chunk_readback_sql

    def _readback(items):
        current_chunk[:] = tuple(items)
        chunk_sizes.append(len(current_chunk))
        return original_readback(current_chunk)

    monkeypatch.setattr(state, "initial_scope_chunk_readback_sql", _readback)

    class _RecordingCursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

    repository = object.__new__(runtime.BackfillStateRepository)
    repository.cursor = _RecordingCursor()
    repository.query = lambda _statement: [
        {
            "scope_id": item.target.scope_id,
            "record_sha256": state.record_sha256(item),
            "revision": item.revision,
        }
        for item in current_chunk
    ]

    repository.persist_initial_scopes(scopes)

    assert len(repository.cursor.statements) == 76
    assert len(chunk_sizes) == 76
    assert max(chunk_sizes) == runtime.INITIAL_SCOPE_CHUNK_SIZE
    assert max(
        len(statement.encode("utf-8"))
        for statement in repository.cursor.statements
    ) < runtime.MAX_STATE_MUTATION_SQL_BYTES
    assert len(state.campaign_merge_sql(campaign).encode("utf-8")) < 20_000


def test_strict_preflight_requires_v2_legacy_shutdown_raw_and_durable_permits(
    monkeypatch,
):
    from utils import transfermarkt_native_v2 as tm_v2

    monkeypatch.setenv("TM_NATIVE_V2_ENABLED", "true")
    monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
    monkeypatch.setenv("TM_PROXY_CONTROL_TOKEN", "p" * 32)
    monkeypatch.setenv("TM_BACKFILL_PROXY_CONTROL_URL", "http://proxy:8899")
    monkeypatch.setenv("TM_BACKFILL_PROXY_CONTROL_TOKEN", "b" * 32)
    reader = SimpleNamespace(
        exists=True,
        active_version="v2",
        active_slot="a",
        revision=7,
        legacy_writers_disabled_at=NOW,
        cleanup_completed_at=None,
    )
    monkeypatch.setattr(tm_v2, "read_reader_state", lambda *_a, **_k: reader)
    monkeypatch.setattr(
        tm_v2,
        "verify_reader_views",
        lambda *_a, **_k: {"passed": True},
    )
    monkeypatch.setattr(tm_v2, "inactive_slot", lambda _reader: "b")

    result = runtime.strict_cutover_preflight(
        connection_factory=_Connection,
        raw_store_factory=lambda: SimpleNamespace(uri_prefix="s3://raw/tm"),
        proxy_health_get=lambda *_a, **_k: _HealthResponse(),
    )

    assert result["paid_io_allowed"] is True
    assert result["write_mode"] == "native-only"
    assert result["active_slot"] == "a"
    assert result["candidate_slot"] == "b"


def test_registry_freshness_checks_every_row_not_only_latest():
    rows = [
        {
            "registry_snapshot_id": "one",
            "competition_discovered_at": NOW.isoformat(),
            "edition_discovered_at": NOW.isoformat(),
        },
        {
            "registry_snapshot_id": "one",
            "competition_discovered_at": (NOW - timedelta(days=2)).isoformat(),
            "edition_discovered_at": NOW.isoformat(),
        },
    ]

    with pytest.raises(runtime.BackfillRuntimeError, match="fresh full discovery"):
        runtime.validate_fresh_registry_snapshot(rows, now=NOW)


def test_initial_campaign_is_historical_only_and_future_snapshot_is_delta():
    competition = _competition()
    rows = [
        _row(competition, _edition("2020", current=False)),
        _row(competition, _edition("2026", current=True)),
    ]
    campaign = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )

    assert campaign is not None
    assert [item.edition_id for item in campaign.targets] == ["2020"]
    assert all(not item.current for item in campaign.targets)

    competition_v2 = replace(
        competition,
        registry_snapshot_id="registry-2",
        discovered_at=NOW,
    )
    rows_v2 = [
        _row(
            competition_v2,
            replace(
                _edition("2020", current=False),
                registry_snapshot_id="registry-2",
            ),
        ),
        _row(
            competition_v2,
            _edition("2019", current=False, snapshot="registry-2"),
        ),
    ]
    delta = runtime.build_campaign_from_registry(
        rows_v2,
        policy_sha256=POLICY_HASH,
        now=NOW,
        previous_campaign=campaign,
    )

    assert delta is not None
    assert [item.edition_id for item in delta.targets] == ["2019"]
    assert delta.registry_snapshot_id == "registry-2"

    competition_v3 = replace(
        competition,
        registry_snapshot_id="registry-3",
        discovered_at=NOW,
    )
    rows_v3 = [
        _row(
            competition_v3,
            _edition(edition_id, current=False, snapshot="registry-3"),
        )
        for edition_id in ("2020", "2019", "2018")
    ]
    second_delta = runtime.build_campaign_from_registry(
        rows_v3,
        policy_sha256=POLICY_HASH,
        now=NOW,
        previous_campaigns=(campaign, delta),
    )

    assert second_delta is not None
    assert [item.edition_id for item in second_delta.targets] == ["2018"]


def test_claimed_payload_is_campaign_stable_and_batch_bounded():
    competition = _competition()
    rows = [_row(competition, _edition("2020", current=False))]
    waiting = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )
    assert waiting is not None
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    scopes = tuple(
        state.BackfillScopeState.initial(campaign, item, now=NOW)
        for item in campaign.targets
    )

    claim, payloads = runtime.claim_and_plan(
        campaign,
        scopes,
        registry_rows=rows,
        run_id="scheduled__batch-1",
        lease_owner="worker-1",
        now=NOW,
        limit=8,
    )

    assert claim.batch is not None
    assert len(claim.batch.scope_ids) == 1
    assert payloads[0]["resume_cycle_id"] == campaign.campaign_id
    assert payloads[0]["parent_cycle_id"] == claim.batch.batch_id
    assert payloads[0]["edition_record"]["current"] is False


def test_interrupted_batch_is_selected_before_any_new_claim_and_replanned_stably():
    competition = _competition()
    rows = [_row(competition, _edition("2020", current=False))]
    waiting = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )
    assert waiting is not None
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    initial = tuple(
        state.BackfillScopeState.initial(campaign, item, now=NOW)
        for item in campaign.targets
    )
    claim, first_payloads = runtime.claim_and_plan(
        campaign,
        initial,
        registry_rows=rows,
        run_id="scheduled__batch-1",
        lease_owner="worker-1",
        now=NOW,
    )
    assert claim.batch is not None

    selected = runtime.select_recoverable_batch(
        campaign,
        claim.scopes,
        [claim.batch],
    )
    recovered_payloads = runtime.plan_existing_batch(
        campaign,
        selected,
        registry_rows=rows,
        run_id="scheduled__batch-2",
        now=NOW + timedelta(minutes=5),
    )

    assert selected == claim.batch
    assert recovered_payloads[0]["parent_cycle_id"] == claim.batch.batch_id
    assert recovered_payloads[0]["parent_ledger"] == first_payloads[0]["parent_ledger"]
    assert recovered_payloads[0]["result_paths"] == first_payloads[0]["result_paths"]
    assert (
        recovered_payloads[0]["child_cycle_id"] == first_payloads[0]["child_cycle_id"]
    )


def test_complete_batch_is_recovered_only_while_captured_scope_needs_dq_commit():
    competition = _competition()
    rows = [_row(competition, _edition("2020", current=False))]
    waiting = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )
    assert waiting is not None
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    scope = state.BackfillScopeState.initial(
        campaign,
        campaign.targets[0],
        now=NOW,
    )
    claim = state.claim_scopes(
        campaign,
        [scope],
        lease_owner="worker",
        now=NOW,
    )
    assert claim.batch is not None
    leased = claim.scopes[0]
    attempt = state.BackfillAttempt.build(
        scope=leased,
        batch_id=claim.batch.batch_id,
        outcome=state.AttemptOutcome.CAPTURED,
        started_at=NOW,
        finished_at=NOW + timedelta(minutes=1),
        raw_evidence_ids=("b" * 64,),
        source_observed_at=NOW + timedelta(minutes=1),
        scope_manifest_uri="s3://raw/manifest.json",
        scope_manifest_sha256="c" * 64,
    )
    captured = state.apply_attempt(leased, attempt)
    running = claim.batch.transition(state.BatchStatus.RUNNING, now=NOW)
    dq_pending = running.transition(
        state.BatchStatus.DQ_PENDING,
        now=NOW + timedelta(minutes=1),
        snapshot_pins={"table": 7},
    )
    complete_batch = dq_pending.transition(
        state.BatchStatus.COMPLETE,
        now=NOW + timedelta(minutes=2),
        dq_report_uri="s3://raw/dq.json",
        dq_report_sha256="d" * 64,
        raw_evidence_ids=attempt.raw_evidence_ids,
    )

    assert (
        runtime.select_recoverable_batch(
            campaign,
            [captured],
            [complete_batch],
        )
        == complete_batch
    )
    complete_scope = state.mark_scope_dq_complete(
        captured,
        scope_manifest_uri=str(captured.scope_manifest_uri),
        scope_manifest_sha256=str(captured.scope_manifest_sha256),
        raw_evidence_ids=captured.raw_evidence_ids,
        now=NOW + timedelta(minutes=3),
    )
    assert (
        runtime.select_recoverable_batch(
            campaign,
            [complete_scope],
            [complete_batch],
        )
        is None
    )
    incident = state.BackfillPlatformIncident.build(
        complete_batch,
        phase="campaign_completion",
        error_class="ReportPublishError",
        report_uri="s3://raw/platform-incident.json",
        report_sha256="e" * 64,
        raw_evidence_ids=complete_batch.raw_evidence_ids,
        now=NOW + timedelta(minutes=4),
    )
    complete_with_incident = complete_batch.record_platform_incident(
        incident,
        now=NOW + timedelta(minutes=4),
    )
    assert complete_with_incident.status is state.BatchStatus.COMPLETE
    assert (
        runtime.select_recoverable_batch(
            campaign,
            [complete_scope],
            [complete_with_incident],
        )
        == complete_with_incident
    )


class _IncidentRepository(runtime.BackfillStateRepository):
    def __init__(self, campaign, scope, batch):
        self.campaign = campaign
        self.scope = scope
        self.batch = batch
        self.transitions = []

    def load_batches(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return (self.batch,)

    def load_scopes(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return (self.scope,)

    def load_attempts(self, campaign_id):
        assert campaign_id == self.campaign.campaign_id
        return ()

    def persist_campaign_transition(self, previous, current):
        assert previous == self.campaign
        self.transitions.append("campaign")
        self.campaign = current

    def persist_batch_transition(self, previous, current):
        assert previous == self.batch
        self.transitions.append("batch")
        self.batch = current


def _dq_platform_incident(tmp_path):
    competition = _competition()
    rows = [_row(competition, _edition("2020", current=False))]
    waiting = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )
    assert waiting is not None
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    initial = state.BackfillScopeState.initial(
        campaign,
        campaign.targets[0],
        now=NOW,
    )
    claim = state.claim_scopes(
        campaign,
        [initial],
        lease_owner="worker",
        now=NOW,
    )
    assert claim.batch is not None
    scope = claim.scopes[0]
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
    attempt = state.BackfillAttempt.build(
        scope=scope,
        batch_id=claim.batch.batch_id,
        outcome=state.AttemptOutcome.CAPTURED,
        started_at=NOW,
        finished_at=NOW + timedelta(minutes=1),
        raw_evidence_ids=(envelope.envelope_id,),
        source_observed_at=NOW + timedelta(minutes=1),
        scope_manifest_uri="s3://raw/manifest.json",
        scope_manifest_sha256="c" * 64,
    )
    captured_scope = state.apply_attempt(scope, attempt)
    running = claim.batch.transition(
        state.BatchStatus.RUNNING,
        now=NOW + timedelta(minutes=1),
    )
    dq_pending = running.transition(
        state.BatchStatus.DQ_PENDING,
        now=NOW + timedelta(minutes=2),
        snapshot_pins={"iceberg.bronze.transfermarkt_players_v2_a": 17},
        raw_evidence_ids=(envelope.envelope_id,),
    )
    phase = "batch_dq"
    error_class = "BackfillDQError"
    incident_id = state.stable_platform_incident_id(
        dq_pending,
        phase=phase,
        error_class=error_class,
        raw_evidence_ids=(envelope.envelope_id,),
    )
    artifact_store = BackfillArtifactStore(raw_store)
    cause_artifact = artifact_store.publish_json(
        {
            "campaign_id": campaign.campaign_id,
            "batch_id": dq_pending.batch_id,
            "passed": False,
        },
        kind="batch_dq",
        owner_id=campaign.campaign_id,
    )
    artifact = artifact_store.publish_json(
        {
            "contract_version": state.CONTRACT_VERSION,
            "campaign_id": campaign.campaign_id,
            "batch_id": dq_pending.batch_id,
            "incident_id": incident_id,
            "batch_revision": dq_pending.revision,
            "phase": phase,
            "error_class": error_class,
            "blocked_from_status": state.BatchStatus.DQ_PENDING.value,
            "snapshot_pins": dict(dq_pending.snapshot_pins or {}),
            "raw_evidence_ids": [envelope.envelope_id],
            "cause_artifact": {
                "uri": cause_artifact.uri,
                "sha256": cause_artifact.sha256,
            },
        },
        kind="platform_incident",
        owner_id=campaign.campaign_id,
    )
    incident = state.BackfillPlatformIncident.build(
        dq_pending,
        phase=phase,
        error_class=error_class,
        report_uri=artifact.uri,
        report_sha256=artifact.sha256,
        raw_evidence_ids=(envelope.envelope_id,),
        now=NOW + timedelta(minutes=3),
    )
    assert incident.incident_id == incident_id
    blocked_batch = dq_pending.record_platform_incident(
        incident,
        now=NOW + timedelta(minutes=3),
    )
    return (
        campaign,
        captured_scope,
        blocked_batch,
        raw_store,
        artifact_store,
    )


def test_dq_incident_explicit_resume_preserves_scopes_and_snapshot_pins(tmp_path):
    campaign, scope, batch, raw_store, artifact_store = _dq_platform_incident(tmp_path)
    blocked_campaign = campaign.transition(
        state.CampaignStatus.BLOCKED_PLATFORM,
        now=NOW + timedelta(minutes=3),
    )
    repository = _IncidentRepository(blocked_campaign, scope, batch)

    active, scopes, resumed = repository.resume_platform_campaign(
        blocked_campaign,
        lease_owner="unused-for-batch-incident",
        now=NOW + timedelta(minutes=4),
        raw_store=raw_store,
        artifact_store=artifact_store,
    )

    assert repository.transitions == ["campaign", "batch"]
    assert active.status is state.CampaignStatus.ACTIVE
    assert scopes == (scope,)
    assert repository.scope == scope
    assert resumed.status is state.BatchStatus.DQ_PENDING
    assert resumed.snapshot_pins == batch.snapshot_pins
    assert resumed.raw_evidence_ids == batch.raw_evidence_ids
    assert resumed.open_platform_incident_id is None


def test_active_campaign_with_open_incident_converges_to_blocked(tmp_path):
    campaign, scope, batch, _raw_store, _artifact_store = _dq_platform_incident(
        tmp_path
    )
    repository = _IncidentRepository(campaign, scope, batch)

    blocked, incident_batch = repository.reconcile_open_platform_incident(
        campaign,
        now=NOW + timedelta(minutes=4),
    )

    assert repository.transitions == ["campaign"]
    assert blocked.status is state.CampaignStatus.BLOCKED_PLATFORM
    assert incident_batch == batch
    assert repository.batch == batch


def test_incident_artifact_is_verified_before_any_resume_cas(tmp_path):
    campaign, scope, batch, raw_store, artifact_store = _dq_platform_incident(tmp_path)
    blocked_campaign = campaign.transition(
        state.CampaignStatus.BLOCKED_PLATFORM,
        now=NOW + timedelta(minutes=3),
    )
    repository = _IncidentRepository(blocked_campaign, scope, batch)
    wrong_store = BackfillArtifactStore(
        RawResponseStore.from_uri((tmp_path / "other-raw").as_uri())
    )

    with pytest.raises(runtime.BackfillRuntimeError, match="cannot be verified"):
        repository.resume_platform_campaign(
            blocked_campaign,
            lease_owner="unused-for-batch-incident",
            now=NOW + timedelta(minutes=4),
            raw_store=raw_store,
            artifact_store=wrong_store,
        )

    assert repository.transitions == []
    assert repository.campaign == blocked_campaign
    assert repository.batch == batch


def test_post_claim_incident_resume_repairs_partial_scope_claim(tmp_path):
    competition = _competition()
    rows = [
        _row(competition, _edition(year, current=False))
        for year in ("2019", "2020")
    ]
    waiting = runtime.build_campaign_from_registry(
        rows,
        policy_sha256=POLICY_HASH,
        now=NOW,
    )
    assert waiting is not None
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    initial = tuple(
        state.BackfillScopeState.initial(campaign, target, now=NOW)
        for target in campaign.targets
    )
    claim = state.claim_scopes(
        campaign,
        initial,
        lease_owner="planner-before-crash",
        now=NOW,
        limit=2,
    )
    assert claim.batch is not None
    partial_scopes = (claim.scopes[0], initial[1])
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    artifact_store = BackfillArtifactStore(raw_store)
    phase = "post_claim_planning"
    error_class = "post_claim_planning:OSError"
    incident_id = state.stable_platform_incident_id(
        claim.batch,
        phase=phase,
        error_class=error_class,
        raw_evidence_ids=(),
    )
    artifact = artifact_store.publish_json(
        {
            "contract_version": state.CONTRACT_VERSION,
            "campaign_id": campaign.campaign_id,
            "batch_id": claim.batch.batch_id,
            "incident_id": incident_id,
            "batch_revision": claim.batch.revision,
            "phase": phase,
            "error_class": error_class,
            "blocked_from_status": claim.batch.status.value,
            "snapshot_pins": None,
            "raw_evidence_ids": [],
            "cause_artifact": None,
        },
        kind="platform_incident",
        owner_id=campaign.campaign_id,
    )
    incident = state.BackfillPlatformIncident.build(
        claim.batch,
        phase=phase,
        error_class=error_class,
        report_uri=artifact.uri,
        report_sha256=artifact.sha256,
        raw_evidence_ids=(),
        now=NOW + timedelta(minutes=1),
    )
    blocked_batch = claim.batch.record_platform_incident(
        incident,
        now=NOW + timedelta(minutes=1),
    )
    blocked_campaign = campaign.transition(
        state.CampaignStatus.BLOCKED_PLATFORM,
        now=NOW + timedelta(minutes=1),
    )

    class _PartialClaimRepository(runtime.BackfillStateRepository):
        def __init__(self):
            self.campaign = blocked_campaign
            self.scopes = list(partial_scopes)
            self.batch = blocked_batch
            self.transitions = []

        def load_batches(self, campaign_id):
            assert campaign_id == self.campaign.campaign_id
            return (self.batch,)

        def load_scopes(self, campaign_id):
            assert campaign_id == self.campaign.campaign_id
            return tuple(self.scopes)

        def load_attempts(self, campaign_id):
            assert campaign_id == self.campaign.campaign_id
            return ()

        def persist_scope_transition(self, previous, current):
            index = self.scopes.index(previous)
            self.scopes[index] = current
            self.transitions.append(f"scope:{current.target.scope_id}")

        def persist_campaign_transition(self, previous, current):
            assert previous == self.campaign
            self.campaign = current
            self.transitions.append("campaign")

        def persist_batch_transition(self, previous, current):
            assert previous == self.batch
            self.batch = current
            self.transitions.append("batch")

    repository = _PartialClaimRepository()
    active, recovered, resumed_batch = repository.resume_platform_campaign(
        blocked_campaign,
        lease_owner="planner-after-restart",
        now=NOW + timedelta(minutes=2),
        raw_store=raw_store,
        artifact_store=artifact_store,
    )

    assert active.status is state.CampaignStatus.ACTIVE
    assert resumed_batch.status is state.BatchStatus.RUNNING
    assert resumed_batch.open_platform_incident_id is None
    assert {item.status for item in recovered} == {state.ScopeStatus.RUNNING}
    assert {item.batch_id for item in recovered} == {claim.batch.batch_id}
    assert tuple(item.claim_generation for item in recovered) == (
        claim.batch.scope_claim_generations
    )
    assert repository.transitions[-2:] == ["campaign", "batch"]
