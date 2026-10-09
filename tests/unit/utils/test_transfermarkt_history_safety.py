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


@pytest.mark.parametrize('outcome', [state.AttemptOutcome.TRANSPORT_ERROR, state.AttemptOutcome.CONTINUATION])
def test_transport_incidents_do_not_consume_terminal_source_budget(outcome):
    _, campaign, scope, batch = claim()
    for index in range(5):
        attempt = state.BackfillAttempt.build(scope=scope, batch_id=scope.batch_id,
            outcome=outcome, started_at=NOW, finished_at=NOW + timedelta(minutes=index),
            source_observed_at=NOW, raw_evidence_ids=['a' * 64],
            error_class='transport_timeout' if outcome is state.AttemptOutcome.TRANSPORT_ERROR else None)
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


@pytest.mark.parametrize('transport,status,outcome,error_class,expected', [
    (False,200,state.AttemptOutcome.CAPTURED,None,True),
    (False,200,state.AttemptOutcome.CONTINUATION,None,True),
    (False,503,state.AttemptOutcome.SOURCE_ERROR,'http_503',True),
    (False,200,state.AttemptOutcome.SOURCE_ERROR,'http_200',False),
    (True,None,state.AttemptOutcome.TRANSPORT_ERROR,'transport_timeout',False),
])
def test_probe_verifies_real_raw_envelope_time_and_semantics(tmp_path,transport,status,outcome,error_class,expected):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    from utils.transfermarkt_backfill_finalize import _probe_has_verified_response
    _,_,scope,batch=claim()
    batch=replace(batch,recovery_probe=True)
    store=RawResponseStore.from_uri((tmp_path/'raw').as_uri())
    fields=dict(url='https://www.transfermarkt.com/page',fetched_at=NOW.isoformat(),
                cycle_id='original-child',scope_id=scope.target.scope_id,endpoint='listing',attempt=1)
    if transport:
        envelope=store.store_transport_error(**fields,error_kind='timeout',error_type='TimeoutError')
    else:
        capture=store.store_attempt(**fields,body=b'synthetic offline source response',status_code=status,headers={})
        envelope=store.store_response_envelope(capture)
    extra={'scope_manifest_uri':'s3://synthetic/manifest.json','scope_manifest_sha256':'a'*64} if outcome is state.AttemptOutcome.CAPTURED else {}
    attempt=state.BackfillAttempt.build(scope=scope,batch_id=batch.batch_id,outcome=outcome,
        started_at=NOW,finished_at=NOW,source_observed_at=NOW,error_class=error_class,
        raw_evidence_ids=[envelope.envelope_id],**extra)
    assert _probe_has_verified_response(batch,attempt,store) is expected
    # Replay after persist_attempt reads the same real immutable envelope type.
    assert _probe_has_verified_response(batch,attempt,store) is expected


def test_corrected_registry_identity_does_not_starve_new_campaign():
    from utils.transfermarkt_backfill_runtime import _semantic_target_identity
    _,old_campaign,old_scope,_=claim()
    old_scope=state.BackfillScopeState.initial(old_campaign,old_scope.target,now=NOW)
    corrected=replace(old_scope.target,canonical_competition_id='TM-CORRECTED',registry_snapshot_id='new')
    new_campaign=state.BackfillCampaign.build(registry_snapshot_id='new',policy_sha256=old_campaign.policy_sha256,
        parser_revision='v2',schema_revision='2',targets=[corrected],now=NOW)
    new_campaign=new_campaign.transition(state.CampaignStatus.ACTIVE,now=NOW)
    new_scope=state.BackfillScopeState.initial(new_campaign,corrected,now=NOW)
    old_hash=state.record_sha256(old_scope)
    repo=object.__new__(BackfillStateRepository)
    repo.load_campaigns=lambda:(old_campaign,new_campaign)
    repo.load_scopes=lambda id:(old_scope,) if id==old_campaign.campaign_id else (new_scope,)
    repo.load_batches=lambda _:()
    assert repo.select_queue_campaign(now=NOW,allowed_ids={corrected.scope_id},registry_targets=[corrected])==new_campaign
    assert state.record_sha256(old_scope)==old_hash


def test_authoritative_empty_delete_rechecks_revoked_historical_authority(monkeypatch):
    from dags.scripts import run_transfermarkt_scraper as runner
    monkeypatch.setenv('TM_DAG_ID','dag_backfill_transfermarkt')
    monkeypatch.setenv('TM_READER_REVISION','7')
    calls=[]
    def denied(mode,revision):
        calls.append((mode,revision))
        raise ValueError('historical policy revoked')
    monkeypatch.setattr(runner,'_authorize_write_mode',denied)
    monkeypatch.setattr(runner,'_canonical_scope_season',lambda *_:'2025/26')
    class Scraper:
        def _bronze_connection(self):
            raise AssertionError('revoked authority must fail before any DELETE connection')
    spec=runner._spec_for_write_mode(runner.ENTITY_SPECS['market_value_history'],'native-only')
    with pytest.raises(ValueError,match='revoked'):
        runner._delete_valid_empty_rows(Scraper(),spec,['1'],'TM-GB1',2025)
    assert calls==[('native-only',7)]


def test_empty_native_manifest_rechecks_historical_authority_before_ops_write(monkeypatch):
    from dags.scripts import run_transfermarkt_scraper as runner
    monkeypatch.setenv('TM_DAG_ID','dag_backfill_transfermarkt')
    monkeypatch.setenv('TM_READER_REVISION','7')
    def denied(*_):
        raise ValueError('historical claim revoked')
    monkeypatch.setattr(runner,'_authorize_write_mode',denied)
    class Scraper:
        def _bronze_connection(self):
            raise AssertionError('revoked authority must fail before manifest mutation')
    spec=runner._spec_for_write_mode(runner.ENTITY_SPECS['market_value_history'],'native-only')
    results={'outputs':{'market_value_points':{'table':'iceberg.bronze.transfermarkt_market_value_points'}}}
    with pytest.raises(ValueError,match='revoked'):
        runner._persist_native_write_manifest(Scraper(),spec,{},results,'original-child','TM-GB1',2025,7)


@pytest.mark.parametrize('migration', ['career', 'season', 'both'])
def test_additive_ops_migration_keeps_exact_original_grants_and_restrictions(migration):
    from dags.scripts.run_transfermarkt_scope_cycle import (
        CAREER_SAFETY_OPS_TABLES, SEASON_HANDOFF_OPS_TABLES,
        standing_policy_for_hash, standing_policy_hash_compatible,
    )
    current = load_standing_policy(POLICY_PATH)
    additions = {'career': CAREER_SAFETY_OPS_TABLES, 'season': SEASON_HANDOFF_OPS_TABLES,
                 'both': CAREER_SAFETY_OPS_TABLES | SEASON_HANDOFF_OPS_TABLES}[migration]
    original = replace(current, allowed_write_tables=tuple(
        table for table in current.allowed_write_tables if table not in additions))
    kwargs = dict(write_mode='native-only', cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
                  request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT, now=NOW)
    assert standing_policy_for_hash(current, original.policy_hash) == original
    assert validate_batch_policy(original, current, **kwargs) == current
    assert original.policy_hash != current.policy_hash
    restricted = replace(current, allowed_write_tables=tuple(
        table for table in current.allowed_write_tables if table != 'iceberg.bronze.transfermarkt_transfer_events'))
    assert not standing_policy_hash_compatible(restricted, original.policy_hash)
    with pytest.raises(Exception, match='omits write tables'):
        validate_batch_policy(original, restricted, **kwargs)
    assert not standing_policy_hash_compatible(replace(current, expires_at=current.expires_at + timedelta(days=1)), original.policy_hash)
    with pytest.raises(Exception, match='expired'):
        validate_batch_policy(replace(original, expires_at=NOW), current, **kwargs)


def test_historical_sql_mutation_rechecks_authority_before_cursor(monkeypatch):
    from dags.scripts import run_transfermarkt_scraper as runner
    monkeypatch.setenv('TM_DAG_ID', 'dag_backfill_transfermarkt')
    monkeypatch.setenv('TM_READER_REVISION', '7')
    def denied(*_):
        raise ValueError('historical claim revoked')
    monkeypatch.setattr(runner, '_authorize_write_mode', denied)
    connection = SimpleNamespace(cursor=lambda: pytest.fail('no cursor after revoked authority'))
    with pytest.raises(ValueError, match='revoked'):
        runner._execute_cursor(connection, 'MERGE INTO iceberg.ops.transfermarkt_fetch_state USING data ON true WHEN NOT MATCHED THEN INSERT VALUES (1)')


def test_historical_frame_authority_rechecks_after_waiting_for_writer_lock(monkeypatch):
    from contextlib import contextmanager
    from dags.scripts import run_transfermarkt_scraper as runner
    monkeypatch.setenv('TM_DAG_ID', 'dag_backfill_transfermarkt')
    events = []
    @contextmanager
    def acquired():
        events.append('lock_acquired')
        yield
    def denied(*_):
        events.append('authority_checked')
        raise ValueError('historical claim expired while waiting')
    monkeypatch.setattr(runner, 'writer_lock', acquired)
    monkeypatch.setattr(runner, '_authorize_write_mode', denied)
    with pytest.raises(ValueError, match='expired while waiting'):
        runner._save_frames(SimpleNamespace(), SimpleNamespace(outputs=()), {}, False, {})
    assert events == ['lock_acquired', 'authority_checked']


@pytest.mark.parametrize('dag_id,filename,original_version,write_mode', [
    ('dag_ingest_transfermarkt', 'standing_approval_policy.json', 3, 'dual'),
    ('dag_backfill_transfermarkt', 'standing_backfill_policy.json', 1, 'native-only'),
])
def test_known_handoff_version_migration_preserves_original_grant(monkeypatch, dag_id, filename, original_version, write_mode):
    from dags.scripts import run_transfermarkt_scope_cycle as cycle
    path = POLICY_PATH.with_name(filename)
    current = load_standing_policy(path)
    original = replace(current, policy_version=original_version, allowed_write_tables=tuple(
        table for table in current.allowed_write_tables if table not in cycle.SEASON_HANDOFF_OPS_TABLES))
    assert cycle.standing_policy_for_hash(current, original.policy_hash) == original
    monkeypatch.setenv('TM_DAG_ID', dag_id)
    monkeypatch.setenv(cycle.STANDING_POLICY_ENV_GATE, 'true')
    monkeypatch.delenv('TM_BACKFILL_BATCH_POLICY_JSON', raising=False)
    args = SimpleNamespace(standing_policy=str(path), standing_policy_sha256=original.policy_hash,
        write_mode=write_mode, cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
        request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT)
    grant = cycle._enforce_standing_policy(args)['paid_proxy']
    assert grant.packet_hash == original.policy_hash
    assert grant.policy_version == original_version
    assert grant.packet_id == f'standing-policy-v{original_version}'
    for changed in (
        replace(current, policy_version=current.policy_version + 1),
        replace(current, expires_at=current.expires_at + timedelta(days=1)),
        replace(current, allowed_write_tables=current.allowed_write_tables + ('iceberg.ops.unrelated_permission',)),
        replace(current, allowed_write_tables=tuple(table for table in current.allowed_write_tables
                                                  if table != 'iceberg.bronze.transfermarkt_transfer_events')),
    ):
        assert not cycle.standing_policy_hash_compatible(changed, original.policy_hash)
    assert not cycle.standing_policy_hash_compatible(current, replace(original, policy_version=original_version + 2).policy_hash)
