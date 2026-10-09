"""Behavioral checks for signal acknowledgements and current queue fairness."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_current_state as state
from dags.utils import transfermarkt_scope_planner as scope_planner
from scrapers.transfermarkt.registry import deterministic_scope_id
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row


NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
SCOPE_ID = deterministic_scope_id('GB1', '2026')


def observation(signature='a', *, at=NOW, **changes):
    return state.SignalObservation(**{
        'scope_id': SCOPE_ID, 'entity': 'club', 'entity_id': '1',
        'signature': signature * 64, 'checked_at': at, 'signal_version': 'club-v1',
        'raw_capture_id': 'raw-1', 'raw_fetched_at': at - timedelta(seconds=1),
        'source_url': 'https://www.transfermarkt.com/listing', 'source_body_hash': 'b' * 64,
        **changes,
    })


def check(*, kind='listing', status='complete', at=NOW, **changes):
    return state.EditionCheck(**{
        'scope_id': SCOPE_ID, 'check_kind': kind, 'cycle_id': 'cycle-1',
        'registry_snapshot_id': 'registry-1', 'denominator_hash': 'c' * 64,
        'checked_at': at, 'status': status, 'result': '' if status == 'complete' else 'timeout',
        'raw_capture_ids': ('raw-1',), 'expected_ids': 20, 'observed_ids': 20,
        **changes,
    })


def test_signal_packet_commits_in_one_guarded_merge_with_escaped_values():
    packet = [state.mark_seen(None, observation(entity_id=str(index), raw_capture_id="raw'packet"))
              for index in range(300)]
    sql = state.build_signals_merge(packet)
    assert sql.count('MERGE INTO') == 1
    assert sql.count("raw''packet") == 300
    assert 's.last_checked_at >= t.last_checked_at' in sql
    assert 't.observed_signature = s.observed_signature' in sql
    assert 't.signal_version = s.signal_version' in sql
    assert 'USING (VALUES' in sql


@pytest.mark.parametrize('count', [0, 301])
def test_signal_packet_rejects_unbounded_sizes(count):
    with pytest.raises(state.CurrentStateError, match='1..300'):
        state.build_signals_merge(state.mark_seen(None, observation(entity_id=str(index)))
                                  for index in range(count))


def test_signal_packet_rejects_duplicate_identity():
    first = state.mark_seen(None, observation())
    with pytest.raises(state.CurrentStateError, match='duplicate'):
        state.build_signals_merge([first, first])


def test_signal_is_pending_until_matching_bronze_proof():
    pending = state.mark_seen(None, observation())
    assert pending.pending
    assert pending.applied_signature is None
    applied = state.mark_applied(pending, signature='a' * 64, bronze_manifest='manifest-1', committed_at=NOW)
    assert not applied.pending
    assert applied.applied_signature == 'a' * 64
    assert applied.bronze_manifest == 'manifest-1'
    unchanged = state.mark_seen(applied, observation(at=NOW + timedelta(hours=20)))
    assert unchanged.status == 'applied'
    assert unchanged.committed_at == NOW


def test_unchanged_pending_or_failed_signal_keeps_original_detection():
    first = state.mark_seen(None, observation())
    failed = state.mark_failed(first, 'Bronze commit failed')
    again = state.mark_seen(failed, observation(at=NOW + timedelta(hours=20)))
    assert again.first_detected_at == NOW
    assert again.status == 'failed'
    assert again.result == 'Bronze commit failed'
    assert again.observation.checked_at == NOW + timedelta(hours=20)


def test_superseding_change_preserves_failed_evidence_without_claiming_success():
    failed = state.mark_failed(state.mark_seen(None, observation()), 'incomplete roster')
    changed = state.mark_seen(failed, observation('d', at=NOW + timedelta(hours=1)))
    assert changed.status == 'pending'
    assert changed.first_detected_at == NOW + timedelta(hours=1)
    assert len(changed.superseded_evidence) == 1
    assert 'incomplete roster' in changed.superseded_evidence[0]
    assert 'failed' in changed.superseded_evidence[0]
    with pytest.raises(state.CurrentStateError, match='latest signal'):
        state.mark_applied(changed, signature='a' * 64, bronze_manifest='stale-manifest', committed_at=NOW)


def test_return_to_applied_value_remains_pending_after_unacknowledged_intermediate_change():
    applied = state.mark_applied(state.mark_seen(None, observation()), signature='a' * 64,
                                 bronze_manifest='first', committed_at=NOW)
    intermediate = state.mark_failed(state.mark_seen(applied, observation('d', at=NOW + timedelta(hours=1))),
                                     'ack failed after possible write')
    returned = state.mark_seen(intermediate, observation(at=NOW + timedelta(hours=2)))
    assert returned.applied_signature == returned.observation.signature
    assert returned.pending


def test_signal_version_change_triggers_recheck_even_when_signature_matches():
    applied = state.mark_applied(state.mark_seen(None, observation()), signature='a' * 64,
                                 bronze_manifest='first', committed_at=NOW)
    changed = state.mark_seen(applied, observation(at=NOW + timedelta(hours=1), signal_version='club-v2'))
    assert changed.pending
    assert changed.first_detected_at == NOW + timedelta(hours=1)


def test_signal_rejects_out_of_order_or_different_identity():
    initial = state.mark_seen(None, observation())
    with pytest.raises(state.CurrentStateError, match='out-of-order'):
        state.mark_seen(initial, observation(at=NOW - timedelta(hours=1)))
    with pytest.raises(state.CurrentStateError, match='identity'):
        state.mark_seen(initial, observation(entity_id='2'))


@pytest.mark.parametrize('changes', [
    {'signature': 'not-a-hash'}, {'source_body_hash': 'A' * 64},
    {'entity': 'career_debt'}, {'scope_id': ''}, {'checked_at': NOW.replace(tzinfo=None)},
    {'raw_fetched_at': NOW + timedelta(seconds=1)}, {'source_url': 'http://example.test'},
    {'source_url': 'https://user:pass@example.test'},
])
def test_observation_rejects_unverified_fields(changes):
    with pytest.raises(state.CurrentStateError):
        observation(**changes)


def test_mapping_roundtrips_and_refuses_unknown_contract_fields():
    pending = state.mark_failed(state.mark_seen(None, observation()), 'bad scope')
    assert state.SignalState.from_mapping(pending.as_dict()) == pending
    assert state.SignalObservation.from_mapping(observation().as_dict()) == observation()
    with pytest.raises(state.CurrentStateError, match='fields differ'):
        state.SignalObservation.from_mapping({**observation().as_dict(), 'extra': True})
    assert state.EditionCheck.from_mapping(check().as_dict()) == check()


def test_ops_row_load_preserves_detection_and_requires_zoned_timestamps():
    row = {
        'scope_id': SCOPE_ID, 'entity': 'club', 'entity_id': '1',
        'observed_signature': 'a' * 64, 'applied_signature': None,
        'first_detected_at': NOW - timedelta(hours=1), 'last_checked_at': NOW,
        'signal_version': 'club-v1', 'raw_capture_id': 'raw-1',
        'raw_fetched_at': NOW - timedelta(seconds=1),
        'source_url': 'https://www.transfermarkt.com/listing', 'source_body_hash': 'b' * 64,
        'status': 'failed', 'result': 'incomplete roster',
        'bronze_manifest': None, 'committed_at': None, 'superseded_evidence_json': '[]',
    }
    loaded = state.SignalState.from_ops_row(row)
    assert loaded.first_detected_at == NOW - timedelta(hours=1)
    assert loaded.status == 'failed'
    assert loaded.pending
    with pytest.raises(state.CurrentStateError, match='timezone'):
        state.SignalState.from_ops_row({**row, 'last_checked_at': NOW.replace(tzinfo=None)})
    with pytest.raises(state.CurrentStateError, match='detection evidence'):
        state.SignalState.from_ops_row({**row, 'first_detected_at': None})


def test_same_timestamp_conflicting_signature_is_not_ordered_as_a_newer_signal():
    with pytest.raises(state.CurrentStateError, match='same check timestamp'):
        state.mark_seen(state.mark_seen(None, observation()), observation('d'))


@pytest.mark.parametrize('proof', [
    {'signature': 'd' * 64, 'bronze_manifest': 'manifest', 'committed_at': NOW},
    {'signature': 'a' * 64, 'bronze_manifest': '', 'committed_at': NOW},
    {'signature': 'a' * 64, 'bronze_manifest': 'manifest', 'committed_at': NOW - timedelta(seconds=1)},
])
def test_no_acknowledgement_without_matching_timely_bronze_manifest(proof):
    with pytest.raises(state.CurrentStateError):
        state.mark_applied(state.mark_seen(None, observation()), **proof)


def test_sql_contract_has_full_evidence_and_does_not_execute():
    ddl = state.build_current_state_tables()
    assert len(ddl) == 2
    assert all('CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_current_' in sql for sql in ddl)
    failed = state.mark_failed(state.mark_seen(None, observation()), "parser's error")
    sql = state.build_signal_merge(failed)
    assert "parser''s error" in sql
    assert 'first_detected_at' in sql and 'applied_signature' in sql
    assert 's.last_checked_at >= t.last_checked_at' in sql
    assert 't.observed_signature = s.observed_signature' in sql
    assert 't.signal_version = s.signal_version' in sql
    assert 'superseded_evidence_json' in sql
    assert "TIMESTAMP '2026-10-08 12:00:00'" in sql
    sql = state.build_check_merge(check())
    assert 't.check_kind = s.check_kind' in sql
    assert 't.cycle_id = s.cycle_id' in sql
    assert 'denominator_hash' in sql and 'registry_snapshot_id' in sql


@pytest.mark.parametrize('changes', [
    {'raw_capture_ids': ()}, {'observed_ids': 19}, {'expected_ids': -1}, {'observed_ids': True},
])
def test_complete_check_requires_full_coverage_and_raw(changes):
    with pytest.raises(state.CurrentStateError):
        check(**changes)


def test_daily_coverage_needs_all_three_checks_and_uses_separate_20h_planning_target():
    cursor = state.ScopeCursor(SCOPE_ID)
    for kind in ('listing', 'players'):
        cursor = state.record_check(cursor, check(kind=kind))
    assert not state.daily_complete(cursor, NOW)
    cursor = state.record_check(cursor, check(kind='injury'))
    assert state.daily_complete(cursor, NOW + timedelta(hours=23))
    assert cursor.daily_due(NOW + timedelta(hours=19)) == ()
    assert cursor.daily_due(NOW + timedelta(hours=20)) == state.DAILY_CHECKS
    assert not state.daily_complete(cursor, NOW + timedelta(hours=24, seconds=1))


def test_failed_check_records_attempt_without_resetting_success_freshness():
    cursor = state.record_check(state.ScopeCursor(SCOPE_ID), check())
    later = NOW + timedelta(hours=20)
    cursor = state.record_check(cursor, check(status='uncertain', at=later, observed_ids=19))
    assert cursor.listing_checked_at == NOW
    assert cursor.last_work_at == later
    assert 'listing' in cursor.daily_due(later)


def test_weekly_roster_due_only_after_cold_and_at_seven_days():
    assert not state.ScopeCursor(SCOPE_ID).roster_due(NOW)
    cursor = state.ScopeCursor(SCOPE_ID, cold_complete=True, roster_completed_at=NOW)
    assert not cursor.roster_due(NOW + timedelta(days=6, hours=23))
    assert cursor.roster_due(NOW + timedelta(days=7))


def test_cursor_roundtrip_and_policy_resume_validation():
    cursor = state.ScopeCursor(SCOPE_ID, generation='club-signature',
                               resume_json='{"verified_raw_refs":["raw-1"]}', listing_checked_at=NOW)
    assert state.ScopeCursor.from_mapping(cursor.as_dict()) == cursor
    with pytest.raises(state.CurrentStateError, match='migration'):
        replace(cursor, policy_version='old-mode')
    with pytest.raises(state.CurrentStateError, match='generation'):
        replace(cursor, generation='')


def denominator(*ids):
    return [{'competition_id': cid, 'live': True, 'competition_class': 'core_club', 'tier': tier}
            for cid, tier in ids]


def registry(*ids):
    return [_joined_row(_competition(cid), _edition(cid, '2026')) for cid in ids]


def test_missing_and_quarantined_current_scopes_remain_in_denominator():
    result = state.current_scope_targets(denominator(('GB1', 1), ('L1', 1), ('ES1', 1)),
                                          registry('GB1', 'L1'), quarantined={'L1': 'conflict'})
    assert result.denominator_competition_ids == ('ES1', 'GB1', 'L1')
    assert [target.scope_id for target in result.targets] == [SCOPE_ID]
    assert dict(result.blocked_competitions) == {
        'ES1': 'missing current edition in promoted registry', 'L1': 'conflict',
    }


def test_target_order_top_leagues_then_numeric_tier_and_invalid_tier_last():
    ids = ('LOW10', 'LOW2', 'FR1', 'GB1', 'ES1', 'IT1', 'L1', 'CUP')
    result = state.current_scope_targets(denominator(*[(cid, cid[3:] if cid.startswith('LOW') else 1)
                                                     for cid in ids[:-1]], ('CUP', 'cup')),
                                          registry(*ids))
    assert [target.competition_id for target in result.targets] == [
        'GB1', 'ES1', 'IT1', 'L1', 'FR1', 'LOW2', 'LOW10', 'CUP',
    ]


def test_new_eligible_competition_keeps_tail_without_changing_core_denominator():
    result = state.current_scope_targets(denominator(('GB1', 1)), registry('NEW1', 'GB1'))
    assert result.denominator_competition_ids == ('GB1',)
    assert [(target.competition_id, target.queue_rank) for target in result.targets] == [
        ('GB1', 0), ('NEW1', 1),
    ]


def test_tail_keeps_registry_crawl_gate_and_does_not_admit_amateur():
    rows = denominator(('GB1', 1)) + [
        {'competition_id': 'AM1', 'live': True, 'competition_class': 'amateur'},
        {'competition_id': 'RS1', 'live': True, 'competition_class': 'reserve'},
    ]
    registry_rows = registry('GB1', 'AM1', 'RS1', 'NEW1')
    registry_rows[-1]['classification_status'] = 'unknown'
    result = state.current_scope_targets(rows, registry_rows)
    assert [target.competition_id for target in result.targets] == ['GB1', 'RS1']
    assert 'NEW1' in dict(result.blocked_competitions)


@pytest.mark.parametrize('changes', [
    {'classification_status': 'unknown'}, {'competition_active': False},
    {'edition_active': False}, {'registry_snapshot_id': ''},
])
def test_target_builder_fails_closed_without_shrinking_denominator(changes):
    result = state.current_scope_targets(denominator(('GB1', 1)), [{**registry('GB1')[0], **changes}])
    assert result.targets == ()
    assert result.denominator_competition_ids == ('GB1',)
    assert dict(result.blocked_competitions)['GB1']


def test_conflicting_registry_rows_block_only_their_competition():
    rows = registry('GB1', 'L1')
    rows.append({**rows[0], 'registry_snapshot_id': 'conflicting'})
    result = state.current_scope_targets(denominator(('GB1', 1), ('L1', 1)), rows)
    assert [target.competition_id for target in result.targets] == ['L1']
    assert dict(result.blocked_competitions)['GB1'] == 'conflicting current registry rows'


def test_single_slot_turn_prevents_signal_or_cold_starvation():
    targets = state.current_scope_targets(denominator(('GB1', 1), ('ES1', 1)), registry('GB1', 'ES1')).targets
    assert state.plan_current_work(targets, {}, now=NOW, limit=1, turn=0)[0].kind == 'signals'
    assert state.plan_current_work(targets, {}, now=NOW, limit=1, turn=1)[0].kind == 'resume'
    first = state.plan_current_work(targets, {}, now=NOW, limit=4)
    assert [(work.kind, work.target.competition_id) for work in first] == [
        ('signals', 'GB1'), ('resume', 'GB1'), ('signals', 'ES1'), ('resume', 'ES1'),
    ]


def test_failed_first_check_cannot_starve_due_successful_scope():
    targets = state.current_scope_targets(denominator(('GB1', 1), ('ES1', 1)), registry('GB1', 'ES1')).targets
    by_id = {target.competition_id: target.scope_id for target in targets}
    cursors = {
        by_id['GB1']: state.ScopeCursor(by_id['GB1'], last_work_at=NOW),
        by_id['ES1']: state.ScopeCursor(
            by_id['ES1'], listing_checked_at=NOW - timedelta(hours=23),
            players_checked_at=NOW - timedelta(hours=23), injury_checked_at=NOW - timedelta(hours=23),
            last_work_at=NOW - timedelta(hours=23),
        ),
    }
    work, = state.plan_current_work(targets, cursors, now=NOW, limit=1, turn=0)
    assert work.target.competition_id == 'ES1'


def test_failed_scope_attempt_does_not_starve_others():
    targets = state.current_scope_targets(denominator(('GB1', 1), ('ES1', 1)), registry('GB1', 'ES1')).targets
    failed_cursor = state.record_check(state.ScopeCursor(SCOPE_ID), check(status='failed'))
    works = state.plan_current_work(targets, {SCOPE_ID: failed_cursor}, now=NOW, limit=2)
    assert [(work.kind, work.target.competition_id) for work in works] == [('signals', 'ES1'), ('resume', 'ES1')]


def test_successful_daily_checks_do_not_remove_weekly_roster_work():
    target = state.current_scope_targets(denominator(('GB1', 1)), registry('GB1')).targets[0]
    cursor = state.ScopeCursor(target.scope_id, cold_complete=True, roster_completed_at=NOW - timedelta(days=7))
    for kind in state.DAILY_CHECKS:
        cursor = state.record_check(cursor, check(kind=kind))
    work, = state.plan_current_work([target], {target.scope_id: cursor}, now=NOW)
    assert work.kind == 'weekly_roster'
    assert work.checks_due == ()


def test_ongoing_verified_resume_survives_current_daily_checks():
    target = state.current_scope_targets(denominator(('GB1', 1)), registry('GB1')).targets[0]
    cursor = state.ScopeCursor(target.scope_id, cold_complete=True, generation='generation-1',
                               resume_json='{"verified_pages":["raw-1"],"remaining_players":[42]}',
                               roster_completed_at=NOW)
    for kind in state.DAILY_CHECKS:
        cursor = state.record_check(cursor, check(kind=kind))
    work, = state.plan_current_work([target], {target.scope_id: cursor}, now=NOW)
    assert work.kind == 'resume'
    assert cursor.generation == 'generation-1'
    assert 'remaining_players' in cursor.resume_json


def test_current_targets_match_actual_promoted_registry_and_planner_scope_identity():
    competition, edition = _competition('GB1'), _edition('GB1', '2026')
    row = _joined_row(competition, edition)
    targets = state.current_scope_targets(denominator(('GB1', 1)), [row])
    eligible, = scope_planner.eligible_registry_scopes([row])
    plan = scope_planner.plan_transfermarkt_scopes(
        {'scopes': ['GB1:2026']}, parent_cycle_id='current-integration-test',
        competitions=[competition], editions=[edition], now=NOW,
    )
    assert targets.targets[0].scope_id == eligible.scope_id == plan.mapped_payloads[0]['scope_id']
    assert targets.targets[0].scope_id.startswith('tm-')
    assert targets.targets[0].registry_snapshot_id == plan.mapped_payloads[0]['registry_snapshot_id']


def test_current_target_does_not_trust_eligible_label_without_classification_proof():
    row = registry('GB1')[0]
    row['classification_evidence'] = '[]'
    targets = state.current_scope_targets(denominator(('GB1', 1)), [row])
    assert targets.targets == ()
    assert targets.denominator_competition_ids == ('GB1',)
    assert dict(targets.blocked_competitions)['GB1']
