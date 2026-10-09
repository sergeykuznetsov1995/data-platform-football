from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_backfill_runtime import BackfillStateRepository
from utils.transfermarkt_history_circuit import HistoryTransportCircuit
from utils.transfermarkt_history_authority import authorize_historical_writer, validate_batch_policy
from utils.transfermarkt_approval import load_standing_policy
from utils.transfermarkt_season_handoff import closed_targets
from scrapers.transfermarkt.history_pool import validate_history_pool_files
from scrapers.transfermarkt.models import SCOPE_HARD_PROVIDER_BYTE_CAP, SCOPE_REQUEST_LIMIT, SCOPE_RETRY_LIMIT

NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
POLICY_PATH = Path(__file__).resolve().parents[3] / 'dags/configs/transfermarkt/standing_backfill_policy.json'


def claim():
    policy = load_standing_policy(POLICY_PATH)
    target = state.HistoricalScopeTarget('GB1__2025', 'GB1', '2025', 'TM-GB1', '2025/26', 'original')
    campaign = state.BackfillCampaign.build(registry_snapshot_id='original', policy_sha256=policy.policy_hash,
        parser_revision='v2', schema_revision='2', targets=[target], now=NOW)
    campaign = campaign.transition(state.CampaignStatus.ACTIVE, now=NOW)
    result = state.claim_scopes(campaign, [state.BackfillScopeState.initial(campaign, target, now=NOW)],
                                lease_owner='offline', now=NOW)
    batch = replace(result.batch, registry_snapshot_id='latest', standing_policy=policy.payload(),
                    stream_id='history-0', scope_registry_snapshot_ids={target.scope_id: 'original'})
    return policy, campaign, result.scopes[0], batch


def test_transport_incidents_do_not_consume_terminal_source_budget():
    _, campaign, scope, batch = claim()
    for index in range(5):
        attempt = state.BackfillAttempt.build(scope=scope, batch_id=scope.batch_id,
            outcome=state.AttemptOutcome.TRANSPORT_ERROR, started_at=NOW, finished_at=NOW + timedelta(minutes=index),
            source_observed_at=NOW, raw_evidence_ids=['a' * 64], error_class='transport_timeout')
        scope = state.apply_attempt(scope, attempt)
        assert scope.status is state.ScopeStatus.RETRYABLE_ERROR
        assert scope.source_attempt_count == scope.source_error_count == 0
        result = state.claim_scopes(campaign, [scope], lease_owner='offline', now=scope.next_retry_at)
        scope = result.scopes[0]
    assert scope.attempt_count == 5


def test_circuit_distinct_scopes_pause_probe_recurrence_and_idempotence(tmp_path):
    circuit = HistoryTransportCircuit(tmp_path)
    for index in range(3):
        circuit.feedback('history-0', attempt_id=str(index), scope_id=str(index), transport_error=True,
                         now=NOW + timedelta(minutes=index))
    assert circuit.admit('history-0', now=NOW + timedelta(minutes=3))[0] is False
    assert circuit.admit('history-1', now=NOW + timedelta(minutes=3))[0] is True
    recovery = NOW + timedelta(minutes=17)
    assert circuit.admit('history-0', now=recovery, reserve=True, owner='batch-1')[:2] == (True, True)
    assert circuit.admit('history-0', now=recovery)[0] is False
    assert circuit.authorized('history-0', owner='batch-1', now=recovery)
    assert not circuit.authorized('history-0', owner='batch-2', now=recovery)
    pause = circuit.feedback('history-0', attempt_id='probe', scope_id='0', transport_error=True, now=recovery)
    assert datetime.fromisoformat(pause) == recovery + timedelta(minutes=15)
    assert circuit.feedback('history-0', attempt_id='probe', scope_id='0', transport_error=True,
                            now=recovery + timedelta(minutes=1)) == pause
    recovery += timedelta(minutes=15)
    circuit.admit('history-0', now=recovery, reserve=True, owner='batch-2')
    circuit.feedback('history-0', attempt_id='success', scope_id='0', transport_error=False, now=recovery)
    assert circuit.admit('history-0', now=recovery)[:2] == (True, False)


@pytest.mark.parametrize('scopes,times', [(['same'] * 4, [0, 1, 2, 3]), (['a', 'b', 'c'], [0, 11, 22])])
def test_circuit_threshold_requires_three_distinct_scopes_in_ten_minutes(tmp_path, scopes, times):
    circuit = HistoryTransportCircuit(tmp_path)
    for index, (scope, minute) in enumerate(zip(scopes, times)):
        circuit.feedback('history-0', attempt_id=str(index), scope_id=scope, transport_error=True,
                         now=NOW + timedelta(minutes=minute))
    assert circuit.admit('history-0', now=NOW + timedelta(minutes=times[-1]))[0]


def test_batch_policy_version_moves_without_campaign_mutation_but_restrictions_hold():
    policy, campaign, _, batch = claim()
    updated = replace(policy, policy_version=policy.policy_version + 1, expires_at=policy.expires_at + timedelta(days=1))
    kwargs = dict(write_mode='native-only', cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
                  request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT, now=NOW)
    validate_batch_policy(policy, updated, **kwargs)
    assert batch.policy_sha256 == policy.policy_hash
    assert campaign.policy_sha256 == policy.policy_hash
    with pytest.raises(Exception, match='advance policy_version'):
        validate_batch_policy(policy, replace(updated, policy_version=policy.policy_version), **kwargs)
    with pytest.raises(Exception, match='expired'):
        validate_batch_policy(policy, replace(updated, expires_at=NOW), **kwargs)
    with pytest.raises(Exception, match='omits write tables'):
        validate_batch_policy(policy, replace(updated, allowed_write_tables=('iceberg.ops.proxy_traffic_runs',)), **kwargs)
    parsed = state.batch_from_mapping(state.record_payload(batch))
    assert parsed == batch
    legacy = replace(batch, registry_snapshot_id=None, standing_policy=None, stream_id=None,
                     scope_registry_snapshot_ids=None)
    assert 'standing_policy' not in state.record_payload(legacy)


def test_running_historical_writer_authority_is_independent_of_reader_state(monkeypatch, tmp_path):
    policy, campaign, scope, batch = claim()
    monkeypatch.setenv('TM_HISTORY_CIRCUIT_DIR', str(tmp_path))
    monkeypatch.setattr("utils.transfermarkt_backfill_runtime.current_signal_qualification", lambda: {"evidence_sha256": "f" * 64})
    repository = SimpleNamespace(load_batch=lambda _: batch, load_scopes=lambda _: [scope], load_campaign=lambda _: campaign)
    env = {'TM_DAG_ID': 'dag_backfill_transfermarkt', 'TM_REFRESH_MODE': 'historical',
           'TM_APPROVAL_MODE': 'standing_policy', 'TM_BACKFILL_CAMPAIGN_ID': campaign.campaign_id,
           'TM_BACKFILL_BATCH_ID': batch.batch_id, 'TM_SCOPE_ID': scope.target.scope_id,
           'TM_BACKFILL_LEASE_ID': scope.lease_id, 'TM_BACKFILL_CLAIM_GENERATION': '1',
           'TM_BACKFILL_ATTEMPT_SEQUENCE': '1', 'TM_STANDING_POLICY_PATH': str(POLICY_PATH),
           'TM_STANDING_POLICY_SHA256': policy.policy_hash, 'TM_STREAM_ID': 'history-0',
           'TM_SCOPE_PAYLOAD_JSON': json.dumps(scope.target.identity_payload()),
           'TM_HISTORY_CURRENT_QUALIFICATION_SHA256': 'f' * 64}
    result = authorize_historical_writer(repository, environment=env, write_mode='native-only', now=NOW)
    assert result['historical_native_authority']
    for key, invalid in [('TM_BACKFILL_LEASE_ID', 'stale'), ('TM_BACKFILL_ATTEMPT_SEQUENCE', '2'),
                         ('TM_STREAM_ID', 'current-0'), ('TM_DAG_ID', 'dag_ingest_transfermarkt')]:
        with pytest.raises(ValueError):
            authorize_historical_writer(repository, environment={**env, key: invalid}, write_mode='native-only', now=NOW)
    with pytest.raises(ValueError):
        authorize_historical_writer(repository, environment=env, write_mode='dual', now=NOW)


def test_queue_skips_old_unavailable_retry_until_it_is_due():
    _, campaign, scope, _ = claim()
    old = replace(scope, status=state.ScopeStatus.RETRYABLE_ERROR, next_retry_at=NOW + timedelta(days=1),
                  lease_id=None, lease_owner=None, leased_at=None, heartbeat_at=None)
    new_campaign = state.BackfillCampaign.build(registry_snapshot_id='new', policy_sha256=campaign.policy_sha256,
        parser_revision='v2', schema_revision='2', targets=[replace(scope.target, edition_id='2024', scope_id='GB1__2024', registry_snapshot_id='new')], now=NOW)
    new_campaign = new_campaign.transition(state.CampaignStatus.ACTIVE, now=NOW)
    new_scope = state.BackfillScopeState.initial(new_campaign, new_campaign.targets[0], now=NOW)
    repo = object.__new__(BackfillStateRepository)
    repo.load_campaigns = lambda: (campaign, new_campaign)
    repo.load_scopes = lambda id: (old,) if id == campaign.campaign_id else (new_scope,)
    repo.load_batches = lambda _: ()
    assert repo.select_queue_campaign(now=NOW) == new_campaign
    assert repo.select_queue_campaign(now=NOW + timedelta(days=1)) == campaign


def test_season_close_requires_new_current_edition_and_preserves_old_scope():
    prior = [{'scope_id': 'GB1__2025', 'competition_id': 'GB1', 'edition_id': '2025'}]
    rows = [{'competition_id': 'GB1', 'edition_id': '2025', 'is_current': False, 'registry_snapshot_id': 'new'},
            {'competition_id': 'GB1', 'edition_id': '2026', 'is_current': True, 'registry_snapshot_id': 'new'}]
    assert closed_targets(prior, rows)[0].scope_id == 'GB1__2025'
    assert not closed_targets(prior, rows[:1])


def test_deployment_pool_preflight_rejects_overlapping_exits_without_secret_output(tmp_path):
    current = tmp_path / 'current.json'
    history = tmp_path / 'history.json'
    record = {'host': 'proxy.invalid', 'port': 8000, 'username': 'private-user', 'password': 'private-password'}
    current.write_text(json.dumps([record]))
    history.write_text(json.dumps([record]))
    with pytest.raises(ValueError, match='overlap') as error:
        validate_history_pool_files(current, history)
    assert 'private' not in str(error.value)
    history.write_text(json.dumps([{**record, 'username': 'history-user'}]))
    validate_history_pool_files(current, history)


def test_real_current_qualification_missing_blocks_history_without_new_activation_file():
    from utils.transfermarkt_backfill_runtime import current_signal_qualification, BackfillRuntimeError
    with pytest.raises(BackfillRuntimeError, match='qualification is required'):
        current_signal_qualification()


def test_invalid_probe_repauses_instead_of_clearing_circuit(tmp_path):
    circuit = HistoryTransportCircuit(tmp_path)
    for index in range(3):
        circuit.feedback('history-0', attempt_id=str(index), scope_id=str(index), transport_error=True, now=NOW)
    later = NOW + timedelta(minutes=15)
    circuit.admit('history-0', now=later, reserve=True, owner='probe')
    circuit.feedback('history-0', attempt_id='invalid', scope_id='0', transport_error=False,
                     recovery_valid=False, now=later)
    assert circuit.admit('history-0', now=later)[0] is False


def test_probe_batch_roundtrip_keeps_writer_and_capture_pins():
    _, _, scope, batch = claim()
    batch = replace(batch, recovery_probe=True,
                    scope_writer_pins={scope.target.scope_id: {'revision': 7, 'candidate_slot': 'a'}},
                    scope_stream_ids={scope.target.scope_id: 'history-0'})
    payload = state.record_payload(batch)
    assert state.batch_from_mapping(payload) == batch
    assert state.record_sha256(state.batch_from_mapping(payload)) == state.record_sha256(batch)


def test_expired_unfinished_batch_stays_visible_while_other_entries_run():
    from utils.transfermarkt_backfill_runtime import select_recoverable_batch
    _, old_campaign, old_scope, old_batch = claim()
    original_scope_hash = state.record_sha256(old_scope)
    held = old_batch.transition(state.BatchStatus.WAITING_POLICY, now=NOW)
    target = replace(old_scope.target, scope_id='GB1__2024', edition_id='2024', registry_snapshot_id='new')
    new_campaign = state.BackfillCampaign.build(registry_snapshot_id='new', policy_sha256=old_campaign.policy_sha256,
        parser_revision='v2', schema_revision='2', targets=[target], now=NOW)
    new_campaign = new_campaign.transition(state.CampaignStatus.ACTIVE, now=NOW)
    new_scope = state.BackfillScopeState.initial(new_campaign, target, now=NOW)
    repo = object.__new__(BackfillStateRepository)
    repo.load_campaigns = lambda: (old_campaign, new_campaign)
    repo.load_scopes = lambda id: (old_scope,) if id == old_campaign.campaign_id else (new_scope,)
    repo.load_batches = lambda id: (held,) if id == old_campaign.campaign_id else ()
    assert repo.select_queue_campaign(now=NOW) == new_campaign
    assert select_recoverable_batch(old_campaign, [old_scope], [held]) is None
    unclaimed = state.claim_scopes(old_campaign, [old_scope], lease_owner='new', now=NOW + timedelta(days=2),
                                   eligible_scope_ids=set())
    assert unclaimed.batch is None
    assert state.record_sha256(unclaimed.scopes[0]) == original_scope_hash
    assert state.batch_from_mapping(state.record_payload(held)) == held
