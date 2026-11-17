from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import pandas as pd
import pytest

from dags.utils import transfermarkt_representation as representation
from scrapers.transfermarkt.representation import (
    REPRESENTATION_COLUMNS,
    REPRESENTATION_KEY,
    RepresentationUniverse,
    UniversePlayer,
    build_representation_dq,
    extract_representation_observations,
)
from scrapers.transfermarkt.tmapi import players_url
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)

pytestmark = pytest.mark.unit

EXTRACTED = datetime(2026, 11, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
COMMITTED = datetime(2026, 11, 17, 13, 0, 0, 654321, tzinfo=timezone.utc)
CAPTURED = datetime(2026, 11, 8, 4, 57, 13, 167286, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in (
        'TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID',
        'TM_WRITE_MODE', 'TM_READER_REVISION',
    ):
        monkeypatch.delenv(name, raising=False)


class RepresentationCursor(MemoryCursor):
    def execute(self, sql, params=()):
        params = tuple(value.isoformat(timespec='microseconds') if isinstance(value, datetime)
                       else value for value in params)
        sql = sql.replace(
            'transfermarkt_player_representation_receipts_v1',
            'transfermarkt_native_write_manifest_v2',
        )
        target_literal = "'" + representation.REPRESENTATION_TABLE + "'"
        placeholder = '__representation_target_table__'
        sql = sql.replace(target_literal, "'" + placeholder + "'")
        result = super().execute(sql, params)
        if placeholder in sql:
            self.db.sql.execute(
                'UPDATE transfermarkt_native_write_manifest_v2 SET target_table=? '
                'WHERE target_table=?',
                (representation.REPRESENTATION_TABLE, placeholder),
            )
        return result


class RepresentationDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.mutation = None

    def cursor(self):
        return RepresentationCursor(self)


class RepresentationWriter(MemoryWriter):
    def _write_to_iceberg(self, *args, **kwargs):
        result = super()._write_to_iceberg(*args, **kwargs)
        if self.db.mutation == 'extra':
            self.db.sql.execute(
                'INSERT INTO transfermarkt_player_representation_observations '
                'SELECT * FROM transfermarkt_player_representation_observations LIMIT 1'
            )
        elif self.db.mutation == 'missing':
            self.db.sql.execute(
                'DELETE FROM transfermarkt_player_representation_observations '
                'WHERE rowid = (SELECT MIN(rowid) FROM transfermarkt_player_representation_observations)'
            )
        return result


@pytest.fixture
def representation_policy(monkeypatch, tmp_path):
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] += [
        representation.REPRESENTATION_TABLE,
        representation.REPRESENTATION_RECEIPTS_TABLE,
    ]
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'representation-policy.json'
    path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, representation_policy):  # noqa: F811
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = RepresentationDB()
    original_cell = representation.current._cell

    def fake_cell(column, value):
        if column in representation.TIMESTAMP_COLUMNS:
            return representation._canonical_cell(column, value)
        return original_cell(column, value)

    monkeypatch.setattr(representation.current, '_cell', fake_cell)
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = RepresentationWriter(db)
    return scraper


def extraction(instance, *, value=123, players=('1',), clubs=('10',), at=EXTRACTED, scope='scope-1'):
    ids = tuple(str(item) for item in players if item == '1' or item == 1)
    body = json.dumps({
        'success': True,
        'data': [{
            'id': '1',
            'attributes': {'consultantAgencyId': value},
            'clubAssignments': [
                {'clubId': club, 'type': 'current', 'shirtNumber': None, 'isCaptain': False}
                for club in clubs
            ],
        }],
    }).encode()
    raw = instance.test_store.store_attempt(
        url=players_url(ids), body=body, status_code=200,
        headers={'Content-Type': 'application/json'}, fetched_at=CAPTURED.isoformat(),
        cycle_id='raw-cycle', scope_id=scope, endpoint='current_players', attempt=0,
    )
    roster = RepresentationUniverse(
        'registry-writer-test',
        tuple(UniversePlayer(str(player), club_ids=tuple(clubs)) for player in players),
        (raw.capture_id,),
    )
    return extract_representation_observations(instance.test_store, roster, extracted_at=at)


def write(instance, result, *, at=COMMITTED):
    return representation.write_representation_observations(
        instance, result, PREFLIGHT, committed_at=at,
    )


def business_table(instance):
    return pd.read_sql_query(
        'SELECT * FROM transfermarkt_player_representation_observations', instance.test_db.sql,
    )


def receipt_table(instance):
    return pd.read_sql_query(
        'SELECT * FROM transfermarkt_native_write_manifest_v2', instance.test_db.sql,
    )


def restrict_policy(path, missing):
    policy = json.loads(path.read_text())
    policy['allowed_write_tables'] = [
        table for table in policy['allowed_write_tables'] if table not in missing
    ]
    path.write_text(json.dumps(policy))


def test_ddl_contains_complete_dod_schema_and_disjoint_targets():
    ddl = representation.representation_ddl()
    assert len(ddl) == 2
    assert representation.REPRESENTATION_TABLE in ddl[0]
    assert representation.REPRESENTATION_RECEIPTS_TABLE in ddl[1]
    for column in REPRESENTATION_COLUMNS:
        assert re.search(rf'\b{re.escape(column)}\b', ddl[0])
    for column in representation.RECEIPT_COLUMNS:
        assert re.search(rf'\b{column}\b', ddl[1])
    assert 'DELETE' not in '\n'.join(ddl)


def test_writer_returns_full_physical_and_receipt_readback(physical):
    result = extraction(physical)
    receipt = write(physical, result)
    assert receipt['verified'] is True
    assert receipt['receipt_readback'] is True
    assert receipt['expected_players'] == receipt['expected_groups'] == 1
    assert receipt['row_count'] == 1
    assert receipt['status_counts'] == {'assigned': 1}
    assert receipt['natural_key_columns'] == list(REPRESENTATION_KEY)
    assert receipt['raw_capture_ids'] == [result.observations[0].raw_capture_id]
    assert receipt['assessment_manifest_id'] == result.universe.assessment_manifest_id
    assert receipt['universe_version'] == 'registry-writer-test'
    assert receipt['run_id'] == result.run_id
    assert receipt['committed_batch'] == result.batch_id
    assert len(receipt['plan_sha256']) == len(receipt['physical_hash']) == 64
    assert receipt['committed_at'] == COMMITTED.isoformat()
    rows = business_table(physical)
    assert len(rows) == 1
    assert list(rows.columns) == REPRESENTATION_COLUMNS
    assert rows.iloc[0]['representative_key'] == 'agency:123'
    assert pd.Timestamp(rows.iloc[0]['captured_at']).tz_localize('UTC') == CAPTURED
    persisted_receipt = receipt_table(physical).iloc[0]
    assert persisted_receipt['receipt_id'] == receipt['receipt_id']
    assert persisted_receipt['physical_hash'] == receipt['physical_hash']
    dq = build_representation_dq(result, verified_receipt=receipt)
    assert dq['verified_write_coverage'] == 1.0


def test_identical_replay_is_noop_and_later_replay_preserves_source_clock(physical):
    result = extraction(physical)
    first = write(physical, result)
    repeat = write(physical, result)
    assert first['receipt_id'] == repeat['receipt_id']
    assert len(business_table(physical)) == len(receipt_table(physical)) == 1
    later_result = extraction(physical, at=EXTRACTED + timedelta(hours=2))
    later = write(physical, later_result, at=COMMITTED + timedelta(hours=2))
    assert later['receipt_id'] != first['receipt_id']
    assert len(business_table(physical)) == 1
    assert len(receipt_table(physical)) == 2
    row = business_table(physical).iloc[0]
    assert pd.Timestamp(row['extracted_at']).tz_localize('UTC') == pd.Timestamp(later_result.extracted_at)
    assert pd.Timestamp(row['captured_at']).tz_localize('UTC') == CAPTURED
    assert pd.Timestamp(row['observed_at']).tz_localize('UTC') == CAPTURED
    with pytest.raises(Exception, match='older|stale'):
        write(physical, result)


def test_multiple_representatives_and_multi_club_membership_are_not_overwritten(physical):
    result = extraction(physical, clubs=('10', '20'))
    first = result.observations[0]
    second = replace(
        first, representative_key='agency:456', representative_id='456',
        evidence_type='independent_test_proof', evidence_locator='saved-proof-2',
    )
    multi = replace(result, observations=(first, second))
    receipt = write(physical, multi)
    rows = business_table(physical)
    assert len(rows) == 2
    assert set(rows['representative_id']) == {'123', '456'}
    assert not rows.duplicated(list(REPRESENTATION_KEY)).any()
    assert receipt['expected_players'] == 1 and receipt['row_count'] == 2
    assert receipt['status_counts'] == {'assigned': 2}


def test_unknown_and_missing_status_keys_replay_without_duplicates(physical):
    unknown = extraction(physical, value=0)
    write(physical, unknown)
    write(physical, unknown)
    assert len(business_table(physical)) == 1
    missing = extraction(physical, players=('1', '2'))
    write(physical, missing)
    write(physical, missing)
    rows = business_table(physical)
    assert len(rows) == 3
    assert set(rows['observation_status']) == {'unknown', 'assigned', 'missing_packet'}
    missing_row = rows.loc[rows.observation_status == 'missing_packet'].iloc[0]
    assert missing_row['source_capture_id'].startswith('missing:')
    assert pd.isna(missing_row['representative_id'])


def test_writer_rejects_contradictory_existing_group_before_second_merge(physical):
    result = extraction(physical, value=0)
    write(physical, result)
    first = result.observations[0]
    contradictory = replace(
        first, observation_status='assigned', status_reason='positive_id',
        representative_key='agency:123', representative_id='123',
    )
    with pytest.raises(Exception, match='contradict|conflict'):
        write(physical, replace(result, observations=(contradictory,)))
    assert len(business_table(physical)) == 1


def test_writer_rejects_changed_immutable_payload_at_same_key(physical):
    result = extraction(physical)
    write(physical, result)
    changed = replace(result.observations[0], status_reason='changed')
    with pytest.raises(Exception, match='immutable|conflict|differs'):
        write(physical, replace(result, observations=(changed,)))
    assert business_table(physical).iloc[0]['status_reason'] == 'positive_id'


@pytest.mark.parametrize('missing', [
    (representation.REPRESENTATION_TABLE,),
    (representation.REPRESENTATION_RECEIPTS_TABLE,),
    (representation.REPRESENTATION_TABLE, representation.REPRESENTATION_RECEIPTS_TABLE),
])
def test_missing_grants_fail_before_any_sql(physical, representation_policy, missing):
    restrict_policy(representation_policy, missing)
    with pytest.raises(Exception, match='authorize|policy|grant'):
        write(physical, extraction(physical))
    assert physical.test_db.events == []


@pytest.mark.parametrize('failure', ['business', 'business_readback', 'receipt', 'receipt_readback'])
def test_failed_write_or_readback_never_returns_verified(physical, monkeypatch, failure):
    if failure == 'business':
        monkeypatch.setattr(
            physical._iceberg_writer, 'write_dataframe',
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError('write failed')),
        )
    elif failure == 'business_readback':
        physical.test_db.corrupt_table = 'transfermarkt_player_representation_observations'
    elif failure == 'receipt':
        physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    else:
        original = representation.current.run._query_dataframe

        def corrupt_receipt(connection, sql, params):
            result = original(connection, sql, params)
            if 'player_representation_receipts' in sql:
                result['physical_hash'] = 'corrupt'
            return result

        monkeypatch.setattr(representation.current.run, '_query_dataframe', corrupt_receipt)
    with pytest.raises(Exception):
        write(physical, extraction(physical))


def test_same_plan_recovers_receipt_after_business_commit_without_duplicates(physical):
    result = extraction(physical)
    physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    with pytest.raises(Exception):
        write(physical, result)
    assert len(business_table(physical)) == 1
    assert receipt_table(physical).empty
    physical.test_db.fail_sql = None
    recovered = write(physical, result)
    assert recovered['verified'] and recovered['receipt_readback']
    assert len(business_table(physical)) == 1
    assert len(receipt_table(physical)) == 1


@pytest.mark.parametrize('mutation', ['extra', 'missing'])
def test_missing_or_extra_physical_keys_fail_readback(physical, mutation):
    physical.test_db.mutation = mutation
    with pytest.raises(Exception, match='physical|read-back|key|differs'):
        write(physical, extraction(physical))


def test_existing_receipt_cannot_conceal_physical_damage(physical):
    result = extraction(physical)
    write(physical, result)
    physical.test_db.sql.execute(
        "UPDATE transfermarkt_player_representation_observations SET source_body_hash='damaged'"
    )
    with pytest.raises(Exception, match='physical|differs|read-back'):
        write(physical, result)


def test_empty_extraction_and_structural_dq_failure_do_not_write(physical):
    empty = extract_representation_observations(
        physical.test_store, RepresentationUniverse('empty', (), ()), extracted_at=EXTRACTED,
    )
    with pytest.raises(Exception, match='empty|denominator|DQ'):
        write(physical, empty)
    assert physical.test_db.events == []


def test_writer_mutations_are_limited_to_t2_tables_and_use_no_delete(physical):
    write(physical, extraction(physical))
    mutations = [sql for sql in physical.test_db.events
                 if sql.startswith(('CREATE', 'MERGE', 'INSERT', 'DELETE', 'UPDATE', 'ALTER', 'DROP'))]
    assert mutations
    assert not any(sql.startswith('DELETE') for sql in mutations)
    forbidden = ('squad_memberships', 'player_attribute_observations', 'club_staff', 'transfermarkt_players')
    assert not any(any(table in sql for table in forbidden) for sql in mutations)
    allowed = ('transfermarkt_player_representation_observations', 'transfermarkt_native_write_manifest_v2')
    assert all(any(table in sql for table in allowed) for sql in mutations)


def test_dq_rejects_receipt_from_another_plan(physical):
    result = extraction(physical)
    receipt = write(physical, result)
    wrong_plan = dict(receipt, plan_sha256='0' * 64)
    report = build_representation_dq(result, verified_receipt=wrong_plan)
    assert report['verified_write_coverage'] == 0.0
    assert 'receipt_plan_sha256_mismatch' in report['receipt_diagnostics']


def test_stored_receipt_physical_hash_mismatch_never_returns_verified(physical):
    result = extraction(physical)
    write(physical, result)
    physical.test_db.sql.execute(
        "UPDATE transfermarkt_native_write_manifest_v2 SET physical_hash='0' || physical_hash"
    )
    with pytest.raises(Exception, match='stored|differs'):
        write(physical, result)
