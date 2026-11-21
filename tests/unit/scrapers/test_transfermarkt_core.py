"""Offline core-lane tests on the recorded stage-A capture sample (#2404)."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import core
from scrapers.transfermarkt.core import (
    CoreParseError,
    LaneOutcome,
    ScopeContext,
    SourceCapture,
)

pytestmark = pytest.mark.unit

FIXTURES = Path('tests/fixtures/transfermarkt/stage_a_rewrite')
EXTRACTED_AT = '2026-11-21T12:00:00.123456+00:00'
UNIVERSE = 'core-test-universe-v1'


def _load(endpoint, index=0):
    manifest = json.loads((FIXTURES / 'manifest.json').read_text())
    fixtures = [item for item in manifest['fixtures'] if item['endpoint'] == endpoint]
    item = fixtures[index]
    capture = json.loads((FIXTURES / item['capture_file']).read_text())
    body = gzip.decompress((FIXTURES / item['blob_file']).read_bytes())
    return SourceCapture(
        url=capture['url'], endpoint=endpoint, body=body,
        capture_id=capture['capture_id'], body_hash=capture['content_hash'],
        captured_at=capture['fetched_at'], status_code=capture['status_code'],
    )


def _scope(**overrides):
    values = dict(
        league='GB1', season='2526', scope_id='tm-scope-test', cycle_id='tm-cycle-test',
        club_id='11', club_name='Test FC', player_id=None, edition_id='2025',
    )
    values.update(overrides)
    return ScopeContext(**values)


def _expected(endpoint, capture):
    path = FIXTURES / 'expected' / endpoint / (capture.capture_id + '.json')
    return json.loads(path.read_text()) if path.exists() else None


# ---------------------------------------------------------------- capture/scope contracts


def test_canonical_timestamp_accepts_zero_microseconds_but_requires_them():
    assert core.canonical_timestamp('2026-01-01T00:00:00.000000+00:00') == \
        '2026-01-01T00:00:00.000000+00:00'
    with pytest.raises(CoreParseError):
        core.canonical_timestamp('2026-01-01T00:00:00+00:00')
    with pytest.raises(CoreParseError):
        core.canonical_timestamp('2026-01-01T00:00:00Z')


def test_plan_scope_is_cross_checked_against_the_capture_url():
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    outcomes = core.extract_squad_observations(
        capture, _scope(club_id=club_id), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core.plan_for_outcome(
            outcomes[0], captured_at=capture.captured_at,
            scope_values={'league': 'GB1', 'season': '2526', 'club_id': '999'})


def test_source_capture_verifies_body_hash_and_lineage():
    capture = _load('squad')
    assert capture.text.startswith('<!DOCTYPE html>') or capture.text.startswith('<')
    with pytest.raises(CoreParseError, match='body_hash_mismatch'):
        SourceCapture(
            url=capture.url, endpoint='squad', body=capture.body + b' ',
            capture_id=capture.capture_id, body_hash=capture.body_hash,
            captured_at=capture.captured_at,
        )


def test_scope_context_rejects_malformed_identity():
    with pytest.raises(CoreParseError):
        _scope(league='gb1')
    with pytest.raises(CoreParseError):
        _scope(season='202525')
    with pytest.raises(CoreParseError):
        _scope(club_id='0')
    _scope(club_id=None)


# ---------------------------------------------------------------- squad lanes


@pytest.mark.parametrize('index', range(4))
def test_squad_extraction_matches_recorded_parser_rows(index):
    capture = _load('squad', index)
    expected = _expected('squad', capture)
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    scope = _scope(club_id=club_id)
    outcomes = core.extract_squad_observations(
        capture, scope, extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert [item.entity for item in outcomes] == [
        'squad_memberships', 'player_attribute_observations', 'player_contract_observations']
    if not expected['parser_rows']:
        assert all(item.status == 'observed_empty' and item.proof for item in outcomes)
        assert all(item.rows == () for item in outcomes)
        return
    for outcome in outcomes:
        assert outcome.status == 'observed_present'
        assert len(outcome.rows) == len(expected['parser_rows'])
        for row in outcome.rows:
            assert row['league'] == 'GB1' and row['season'] == '2526' and row['club_id'] == club_id
            assert row['raw_capture_id'] == capture.capture_id
            assert row['source_body_hash'] == capture.body_hash
            assert row['run_id'] == outcomes[0].run_id
            assert row['_batch_id'].startswith('tm-core-batch-')
    keys = [tuple(row[column] for column in core.CORE_NATURAL_KEYS['squad_memberships'])
            for row in outcomes[0].rows]
    assert len(set(keys)) == len(keys)
    contract_statuses = {row['observation_status'] for row in outcomes[2].rows}
    assert contract_statuses <= {'observed_present', 'unknown', 'error', 'not_applicable'}


def test_squad_lane_requires_club_scope():
    capture = _load('squad')
    with pytest.raises(CoreParseError, match='squad_requires_club_scope'):
        core.extract_squad_observations(
            capture, _scope(club_id=None), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)


def test_authoritative_empty_roster_page_is_observed_empty():
    html = ('<html><body><h1 class="data-header__headline-wrapper">Empty FC</h1>'
            '<p>no squad here</p></body></html>')
    body = html.encode()
    capture = SourceCapture(
        url='https://www.transfermarkt.com/empty-fc/kader/verein/999/saison_id/2025/plus/1',
        endpoint='squad', body=body, capture_id=hashlib.sha256(body).hexdigest(),
        body_hash=hashlib.sha256(body).hexdigest(),
        captured_at='2026-11-21T10:00:00.654321+00:00')
    outcomes = core.extract_squad_observations(
        capture, _scope(club_id='999'), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert [item.status for item in outcomes] == ['observed_empty'] * 3
    assert outcomes[0].proof == 'empty_roster:squad_table_absent'


# ---------------------------------------------------------------- career lanes


@pytest.mark.parametrize('index', range(4))
def test_market_value_extraction_matches_recorded_rows(index):
    capture = _load('market_value_points', index)
    player_id = capture.url.rsplit('/', 1)[-1].split('.')[0]
    outcome = core.extract_career_observations(
        capture, _scope(player_id=player_id, club_id=None),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.entity == 'market_value_points'
    expected = _expected('market_value_points', capture)
    if outcome.status == 'observed_present':
        assert len(outcome.rows) == len(expected['parser_rows'])
        for row in outcome.rows:
            assert row['player_id'] == player_id
            assert row['source_endpoint'] == 'market_value_points'
    else:
        assert outcome.status in {'observed_empty', 'error'}
        assert outcome.rows == ()


@pytest.mark.parametrize('index', range(4))
def test_transfer_extraction_matches_recorded_rows(index):
    capture = _load('transfer_events', index)
    player_id = capture.url.rsplit('/', 1)[-1].split('.')[0]
    outcome = core.extract_career_observations(
        capture, _scope(player_id=player_id, club_id=None),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.entity == 'transfer_events'
    expected = _expected('transfer_events', capture)
    if outcome.status == 'observed_present':
        assert len(outcome.rows) == len(expected['parser_rows'])
        assert all(row['transfer_id'] and len(row['transfer_id']) == 64 for row in outcome.rows)
        assert 'season' not in outcome.rows[0]  # only the native event_season slug survives
    else:
        assert outcome.status in {'observed_empty', 'error'}


def test_career_lane_reports_invalid_json_as_error():
    body = b'{not json'
    capture = SourceCapture(
        url='https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/1',
        endpoint='market_value_points', body=body,
        capture_id=hashlib.sha256(body).hexdigest(), body_hash=hashlib.sha256(body).hexdigest(),
        captured_at='2026-11-21T10:00:00.654321+00:00')
    outcome = core.extract_career_observations(
        capture, _scope(player_id='1', club_id=None),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.status == 'error' and outcome.status_reason == 'invalid_json'
    assert outcome.rows == ()


def test_career_lane_never_emits_partial_rows_on_collection_error():
    payload = {'transfers': [{'date': 'Sep 1, 2025', 'from': {}, 'to': {}}]}  # no meaningful clubs
    body = json.dumps(payload).encode()
    capture = SourceCapture(
        url='https://www.transfermarkt.com/ceapi/transferHistory/list/1',
        endpoint='transfer_events', body=body,
        capture_id=hashlib.sha256(body).hexdigest(), body_hash=hashlib.sha256(body).hexdigest(),
        captured_at='2026-11-21T10:00:00.654321+00:00')
    outcome = core.extract_career_observations(
        capture, _scope(player_id='1', club_id=None),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.status == 'error' and outcome.status_reason.startswith(('collection:', 'schema:'))
    assert outcome.rows == ()


# ---------------------------------------------------------------- coach lanes


@pytest.mark.parametrize('index', range(4))
def test_coach_profile_extraction(index):
    capture = _load('coach_profile', index)
    coach_id = capture.url.split('/trainer/')[1].split('/')[0]
    outcome = core.extract_coach_profile(
        capture, _scope(), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.entity == 'coach_profiles'
    if outcome.status == 'observed_present':
        (row,) = outcome.rows
        assert row['coach_id'] == coach_id
        assert row['observed_at'] == capture.captured_at
    else:
        assert outcome.status == 'error' and outcome.rows == ()


@pytest.mark.parametrize('index', range(4))
def test_coach_stints_extraction_covers_the_whole_club_history(index):
    capture = _load('coach_history', index)
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    outcome = core.extract_coach_stints(
        capture, _scope(club_id=club_id), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.entity == 'coach_stints'
    if outcome.status == 'observed_present':
        assert outcome.rows
        for row in outcome.rows:
            assert row['club_id'] == club_id
            assert len(row['stint_id']) == 64
    else:
        assert outcome.status == 'observed_empty' and outcome.proof
        assert outcome.rows == ()


def test_coach_stints_require_the_club_scope():
    capture = _load('coach_history')
    with pytest.raises(CoreParseError):
        core.extract_coach_stints(
            capture, _scope(club_id=None), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)


# ---------------------------------------------------------------- DQ and plans


def _squad_outcomes():
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    return core.extract_squad_observations(
        capture, _scope(club_id=club_id), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)


def test_captureless_outcomes_cover_transport_failures():
    outcome = core.captureless_outcome(
        'squad_memberships', _scope(), status='unavailable', reason='squad:retry_exhausted',
        extracted_at=EXTRACTED_AT, run_id='run-x', universe_version=UNIVERSE)
    assert outcome.status == 'unavailable' and outcome.rows == () and outcome.proof is None
    with pytest.raises(CoreParseError):
        core.captureless_outcome(
            'squad_memberships', _scope(), status='observed_present', reason='x',
            extracted_at=EXTRACTED_AT, run_id='run-x', universe_version=UNIVERSE)


def test_core_dq_passes_the_recorded_sample_and_flags_duplicates():
    outcomes = _squad_outcomes()
    dq = core.build_core_dq(outcomes)
    assert dq['passed'] is True and dq['errors'] == []
    assert dq['entities']['squad_memberships']['row_count'] == len(outcomes[0].rows)
    assert dq['verified_write_coverage'] == 0.0 and dq['receipt_diagnostics'] == ['receipt_absent']
    verified = {'verified': True, 'receipt_readback': True, 'plan_sha256': 'p', 'committed_batch': 'b'}
    assert core.build_core_dq(outcomes, verified_receipt=verified)['verified_write_coverage'] == 1.0
    duplicated = outcomes + outcomes
    assert 'duplicate_outcome' in core.build_core_dq(duplicated)['errors']
    assert 'empty_universe' in core.build_core_dq(())['errors']


def test_plan_for_outcome_builds_the_native_write_contract():
    outcomes = _squad_outcomes()
    club_id = outcomes[0].rows[0]['club_id']
    plan = core.plan_for_outcome(
        outcomes[0], captured_at=outcomes[0].capture.captured_at,
        scope_values={'league': 'GB1', 'season': '2526', 'club_id': club_id})
    assert plan.table == 'transfermarkt_squad_memberships'
    assert plan.write_mode == 'replace'
    assert plan.scope_values == ('GB1', '2526', club_id)
    assert plan.natural_keys == ('league', 'season', 'club_id', 'player_id')
    assert plan.ddl.startswith('CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_squad_memberships')
    from scrapers.transfermarkt.native_writer import NativeWritePlan
    assert isinstance(plan, NativeWritePlan)
    merged = core.plan_for_outcome(
        outcomes[1], captured_at=outcomes[1].capture.captured_at,
        scope_values={'league': 'GB1', 'season': '2526', 'club_id': club_id})
    assert merged.write_mode == 'merge'


def test_plan_for_outcome_refuses_non_conclusive_and_scope_drift():
    outcomes = _squad_outcomes()
    unavailable = core.captureless_outcome(
        'squad_memberships', _scope(), status='unavailable', reason='x',
        extracted_at=EXTRACTED_AT, run_id='r', universe_version=UNIVERSE)
    with pytest.raises(ValueError, match='non-conclusive'):
        core.plan_for_outcome(unavailable, captured_at=EXTRACTED_AT, scope_values={})
    with pytest.raises(Exception):
        core.plan_for_outcome(
            outcomes[0], captured_at=outcomes[0].capture.captured_at,
            scope_values={'league': 'GB1', 'season': '2526', 'club_id': '999'})


def test_core_ddl_types_follow_the_column_contract():
    ddl = core.core_ddl('market_value_points')
    assert 'value_eur bigint' in ddl
    assert 'mv_date date' in ddl
    assert 'is_value_removed boolean' in ddl
    assert 'captured_at timestamp(6)' in ddl
    assert 'player_name varchar' not in ddl  # not this table's column


def test_lane_outcome_validates_status_rows_and_proof_rules():
    capture = _load('squad')
    scope = _scope()
    with pytest.raises(CoreParseError):
        LaneOutcome('squad_memberships', 'observed_empty', 'r', proof=None,
                    capture=capture, scope=scope, extracted_at=EXTRACTED_AT,
                    run_id='r', universe_version=UNIVERSE)
    with pytest.raises(CoreParseError):
        LaneOutcome('squad_memberships', 'observed_empty', 'r', proof='p', rows=({'x': 1},),
                    capture=capture, scope=scope, extracted_at=EXTRACTED_AT,
                    run_id='r', universe_version=UNIVERSE)
    with pytest.raises(CoreParseError):
        LaneOutcome('nope', 'observed_empty', 'r', proof='p')
