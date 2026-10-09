"""Historical Native Bronze permission independent of reader cutover.

A caller needs the standing policy AND an exact persisted running batch claim.
Current/manual/legacy paths retain the original reader lifecycle guard.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from .transfermarkt_approval import StandingPolicy

BACKFILL_DAG_ID = 'dag_backfill_transfermarkt'


def validate_batch_policy(bound, current, *, write_mode, cycle_budget_bytes,
                          request_limit, retry_limit, now=None):
    """An old batch cannot use expired or more permissive replay privileges."""
    from dags.scripts.run_transfermarkt_scope_cycle import validate_standing_policy_for_scope_cycle
    instant = now or datetime.now(timezone.utc)
    for policy in (bound, current):
        policy.assert_not_expired(instant)
        validate_standing_policy_for_scope_cycle(
            policy, write_mode=write_mode, cycle_budget_bytes=cycle_budget_bytes,
            request_limit=request_limit, retry_limit=retry_limit,
            expected_dag_id=BACKFILL_DAG_ID)
    if bound.policy_hash != current.policy_hash and current.policy_version <= bound.policy_version:
        raise ValueError('changed historical policy must advance policy_version')


def authorize_historical_writer(repository, *, environment, write_mode, now=None):
    """Read the durable fence before paid work and again before Bronze DML."""
    from . import transfermarkt_backfill_state as state
    from .transfermarkt_approval import load_standing_policy
    from scrapers.transfermarkt.models import SCOPE_HARD_PROVIDER_BYTE_CAP, SCOPE_REQUEST_LIMIT, SCOPE_RETRY_LIMIT
    instant = now or datetime.now(timezone.utc)
    if (environment.get('TM_DAG_ID') != BACKFILL_DAG_ID or write_mode != 'native-only'
            or environment.get('TM_REFRESH_MODE') != 'historical'
            or environment.get('TM_APPROVAL_MODE') != 'standing_policy'):
        raise ValueError('historical native authority requires the historical standing-policy DAG')
    batch = repository.load_batch(environment['TM_BACKFILL_BATCH_ID'])
    if (batch.campaign_id != environment['TM_BACKFILL_CAMPAIGN_ID']
            or batch.status not in {state.BatchStatus.CLAIMED, state.BatchStatus.RUNNING}
            or batch.open_platform_incident_id):
        raise ValueError('historical batch is not authorized to write')
    probe_flag = environment.get('TM_HISTORY_RECOVERY_PROBE', 'false').lower() in {'true', '1', 'yes', 'on'}
    if probe_flag != batch.recovery_probe:
        raise ValueError('historical recovery request limit differs from frozen batch')
    campaign = repository.load_campaign(batch.campaign_id)
    if campaign.status is not state.CampaignStatus.ACTIVE:
        raise ValueError('historical campaign is not active')
    scope_id = environment['TM_SCOPE_ID']
    scopes = [item for item in repository.load_scopes(batch.campaign_id) if item.target.scope_id == scope_id]
    if len(scopes) != 1:
        raise ValueError('historical write requires one exact persisted scope')
    scope = scopes[0]
    if (scope.status is not state.ScopeStatus.RUNNING or scope.batch_id != batch.batch_id
            or scope.lease_id != environment['TM_BACKFILL_LEASE_ID']
            or scope.claim_generation != int(environment['TM_BACKFILL_CLAIM_GENERATION'])
            or scope.attempt_count + 1 != int(environment['TM_BACKFILL_ATTEMPT_SEQUENCE'])
            or scope_id not in batch.scope_ids):
        raise ValueError('historical running claim fence drifted')
    if state.is_stale_lease(scope, now=instant):
        raise ValueError('historical writer lease is stale')
    payload = json.loads(environment['TM_SCOPE_PAYLOAD_JSON'])
    snapshot = (batch.scope_registry_snapshot_ids or {}).get(scope_id, batch.registry_snapshot_id or scope.target.registry_snapshot_id)
    if (payload.get('scope_id') != scope_id or payload.get('competition_id') != scope.target.competition_id
            or payload.get('edition_id') != scope.target.edition_id
            or payload.get('canonical_competition_id') != scope.target.canonical_competition_id
            or payload.get('canonical_season') != scope.target.canonical_season
            or payload.get('registry_snapshot_id') != snapshot):
        raise ValueError('historical capture identity differs from durable claim')
    pin = (batch.scope_writer_pins or {}).get(scope_id)
    if pin and (int(environment['TM_READER_REVISION']) != pin['revision']
                or environment['TM_CANDIDATE_SLOT'] != pin['candidate_slot']):
        raise ValueError('historical original writer proof drifted')
    from .transfermarkt_backfill_runtime import current_signal_qualification
    qualification = current_signal_qualification()
    if environment.get('TM_HISTORY_CURRENT_QUALIFICATION_SHA256') != qualification['evidence_sha256']:
        raise ValueError('current qualification changed after historical preflight')
    current = load_standing_policy(environment['TM_STANDING_POLICY_PATH'])
    bound = StandingPolicy(**batch.standing_policy) if batch.standing_policy else current
    expected = batch.policy_sha256 or campaign.policy_sha256
    if bound.policy_hash != expected or environment['TM_STANDING_POLICY_SHA256'] != expected:
        raise ValueError('historical batch policy proof drifted')
    validate_batch_policy(bound, current, write_mode=write_mode,
                          cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
                          request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT,
                          now=instant)
    from .transfermarkt_history_circuit import HistoryTransportCircuit
    stream = (batch.scope_stream_ids or {}).get(scope_id, batch.stream_id or environment.get('TM_STREAM_ID', 'history-0'))
    if environment.get('TM_STREAM_ID') != stream:
        raise ValueError('historical batch stream identity drifted')
    # A reserved recovery probe remains allowed for its own exact batch.
    circuit = HistoryTransportCircuit()
    if not circuit.authorized(stream, owner=batch.batch_id, now=instant):
        raise ValueError('historical transport circuit is paused')
    return {'write_mode': write_mode, 'historical_native_authority': True,
            'batch_id': batch.batch_id, 'policy_sha256': expected, 'stream_id': stream}
