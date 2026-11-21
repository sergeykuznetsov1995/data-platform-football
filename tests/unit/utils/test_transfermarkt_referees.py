from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import pandas as pd
import pytest

from dags.utils import transfermarkt_referees as referees
from scrapers.transfermarkt.referee_capture import (
    REFEREE_AGGREGATE_PHYSICAL_COLUMNS,
    REFEREE_PROFILE_COLUMNS,
    capture_referee_directory,
    capture_referee_profiles,
    referee_directory_url,
    referee_profile_url,
)
from tests.unit.scrapers.test_transfermarkt_referee_capture import (
    DIRECTORY, PROFILE, FakeClient, scope,
)
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)

pytestmark = pytest.mark.unit
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in (
        'TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID',
        'TM_WRITE_MODE', 'TM_READER_REVISION',
    ):
        monkeypatch.delenv(name, raising=False)


class RefereeCursor(MemoryCursor):
    def execute(self, sql, params=()):
        params = tuple(
            value.isoformat(timespec='microseconds') if isinstance(value, datetime) else value
            for value in params
        )
        sql = sql.replace(
            'transfermarkt_referee_receipts_v1', 'transfermarkt_native_write_manifest_v2',
        )
        placeholder = '__referee_target_table__'
        for target in (referees.REFEREE_PROFILE_TABLE, referees.REFEREE_AGGREGATE_TABLE):
            sql = sql.replace(f"'{target}'", f"'{placeholder}'")
        result = super().execute(sql, params)
        if placeholder in sql:
            target = (
                referees.REFEREE_PROFILE_TABLE if 'run-profiles' in sql
                else referees.REFEREE_AGGREGATE_TABLE
            )
            self.db.sql.execute(
                'UPDATE transfermarkt_native_write_manifest_v2 SET target_table=? '
                'WHERE target_table=?', (target, placeholder),
            )
        return result


class RefereeDB(MemoryDB):
    def cursor(self):
        return RefereeCursor(self)


class RefereeWriter(MemoryWriter):
    def _write_to_iceberg(self, *args, **kwargs):
        result = super()._write_to_iceberg(*args, **kwargs)
        if self.db.mutation == 'extra':
            self.db.sql.execute(
                'INSERT INTO transfermarkt_referee_season_aggregates '
                'SELECT * FROM transfermarkt_referee_season_aggregates LIMIT 1',
            )
        elif self.db.mutation == 'missing':
            self.db.sql.execute(
                'DELETE FROM transfermarkt_referee_season_aggregates '
                'WHERE rowid = (SELECT MIN(rowid) FROM transfermarkt_referee_season_aggregates)',
            )
        return result


@pytest.fixture
def referee_policy(monkeypatch, tmp_path):
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] = list(dict.fromkeys([
        *policy['allowed_write_tables'],
        referees.REFEREE_PROFILE_TABLE,
        referees.REFEREE_AGGREGATE_TABLE,
        referees.REFEREE_RECEIPTS_TABLE,
    ]))
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'referee-policy.json'
    path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, referee_policy):  # noqa: F811
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = RefereeDB()
    db.mutation = None
    original_cell = referees.current._cell

    def fake_cell(column, value):
        if column in referees.TIMESTAMP_COLUMNS:
            return referees._canonical_cell(column, value)
        return original_cell(column, value)

    monkeypatch.setattr(referees.current, '_cell', fake_cell)
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = RefereeWriter(db)
    scraper._http_client._raw_store = scraper.test_store
    return scraper


def aggregate_bundle(instance, *, at=NOW):
    value = scope()
    url = referee_directory_url(value)
    client = FakeClient(instance.test_store.root, {url: DIRECTORY}, scope_id=value['scope_id'])
    return capture_referee_directory(
        client, value, run_id='run-aggregates', universe_version='registry-test', extracted_at=at,
    )


def profile_bundle(instance, *, at=NOW):
    value = scope()
    url = referee_profile_url('101')
    client = FakeClient(instance.test_store.root, {url: PROFILE}, scope_id=value['scope_id'])
    return capture_referee_profiles(
        client, ['101'], scope=value,
        run_id='run-profiles', universe_version='registry-test', extracted_at=at,
    )


def write(instance, bundle):
    return referees.write_referee_bundle(instance, bundle, PREFLIGHT)


def table(instance, name):
    return pd.read_sql_query(f'SELECT * FROM {name}', instance.test_db.sql)


def receipts(instance):
    return table(instance, 'transfermarkt_native_write_manifest_v2')


def restrict_policy(path, missing):
    policy = json.loads(path.read_text())
    policy['allowed_write_tables'] = [
        table for table in policy['allowed_write_tables'] if table not in missing
    ]
    path.write_text(json.dumps(policy))


def test_ddl_covers_both_nonmatch_keys_lineage_and_receipt():
    ddl = referees.referee_ddl()
    assert len(ddl) == 3
    for column in REFEREE_PROFILE_COLUMNS:
        assert re.search(rf'\b{re.escape(column)}\b', ddl[0])
    for column in REFEREE_AGGREGATE_PHYSICAL_COLUMNS:
        assert re.search(rf'\b{re.escape(column)}\b', ddl[1])
    for column in referees.RECEIPT_COLUMNS:
        assert re.search(rf'\b{column}\b', ddl[2])
    assert 'DELETE' not in '\n'.join(ddl)


def test_aggregate_write_returns_directory_metric_status_and_physical_proof(physical):
    bundle = aggregate_bundle(physical)
    receipt = write(physical, bundle)
    assert receipt['verified'] is True and receipt['receipt_readback'] is True
    assert receipt['expected_directory_pages'] == 1
    assert receipt['expected_profiles'] == 2
    assert receipt['expected_metrics'] == receipt['row_count'] == 10
    assert receipt['status_counts']['observed_present'] == 9
    assert receipt['status_counts']['explicit_none'] == 1
    assert receipt['natural_key_columns'] == list(referees.REFEREE_AGGREGATE_KEY)
    assert receipt['raw_capture_ids'] == list(bundle.raw_capture_ids)
    assert len(receipt['plan_sha256']) == len(receipt['physical_hash']) == 64
    rows = table(physical, 'transfermarkt_referee_season_aggregates')
    assert len(rows) == 10
    assert not {'match_id', 'game_id', 'home_team', 'away_team', 'event'} & set(rows.columns)
    assert not rows.duplicated(list(referees.REFEREE_AGGREGATE_KEY)).any()
    dq = referees.build_referee_dq(bundle, verified_receipt=receipt)
    assert dq['structural_passed'] is True and dq['match_row_count'] == 0


def test_profile_write_has_separate_counts_and_same_receipt_contract(physical):
    bundle = profile_bundle(physical)
    receipt = write(physical, bundle)
    assert receipt['expected_directory_pages'] == 0
    assert receipt['expected_profiles'] == 1 and receipt['expected_metrics'] == 0
    assert receipt['row_count'] == 1
    assert receipt['natural_key_columns'] == list(referees.REFEREE_PROFILE_KEY)
    rows = table(physical, 'transfermarkt_referee_profiles')
    assert len(rows) == 1 and rows.iloc[0]['referee_id'] == '101'
    assert rows.iloc[0]['birth_date'] == '1990-03-10'


def test_identical_replay_is_noop_and_source_clock_does_not_advance(physical):
    bundle = aggregate_bundle(physical)
    first = write(physical, bundle)
    repeat = write(physical, bundle)
    assert first['receipt_id'] == repeat['receipt_id']
    assert first['physical_hash'] == repeat['physical_hash']
    assert len(table(physical, 'transfermarkt_referee_season_aggregates')) == 10
    assert len(receipts(physical)) == 1
    later = aggregate_bundle(physical, at=NOW + timedelta(hours=1))
    # The source capture clock, not extraction time, remains the observation key.
    assert later.aggregates[0]['observed_at'] == bundle.aggregates[0]['observed_at']


def test_changed_business_value_at_same_observation_key_fails(physical):
    bundle = aggregate_bundle(physical)
    write(physical, bundle)
    changed = dict(bundle.aggregates[0])
    changed['value'] = '999'
    with pytest.raises(Exception, match='immutable|differs|conflict'):
        write(physical, replace(bundle, aggregates=(changed, *bundle.aggregates[1:])))
    assert table(physical, 'transfermarkt_referee_season_aggregates').iloc[0]['value'] != 999


@pytest.mark.parametrize('missing', [
    (referees.REFEREE_AGGREGATE_TABLE,),
    (referees.REFEREE_RECEIPTS_TABLE,),
    (referees.REFEREE_PROFILE_TABLE, referees.REFEREE_RECEIPTS_TABLE),
])
def test_missing_grants_fail_before_sql(physical, referee_policy, missing):
    bundle = profile_bundle(physical) if referees.REFEREE_PROFILE_TABLE in missing else aggregate_bundle(physical)
    restrict_policy(referee_policy, missing)
    with pytest.raises(Exception, match='authorize|policy|grant'):
        write(physical, bundle)
    assert physical.test_db.events == []


@pytest.mark.parametrize('failure', ['business', 'business_readback', 'receipt', 'receipt_readback'])
def test_failed_write_or_readback_never_returns_verified(physical, monkeypatch, failure):
    if failure == 'business':
        monkeypatch.setattr(
            physical._iceberg_writer, 'write_dataframe',
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError('write failed')),
        )
    elif failure == 'business_readback':
        physical.test_db.corrupt_table = 'transfermarkt_referee_season_aggregates'
    elif failure == 'receipt':
        physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    else:
        original = referees.current.run._query_dataframe

        def corrupt_receipt(connection, sql, params):
            result = original(connection, sql, params)
            if 'referee_receipts' in sql:
                result['physical_hash'] = 'corrupt'
            return result

        monkeypatch.setattr(referees.current.run, '_query_dataframe', corrupt_receipt)
    with pytest.raises(Exception):
        write(physical, aggregate_bundle(physical))


def test_business_commit_without_receipt_recovers_same_plan(physical):
    bundle = aggregate_bundle(physical)
    physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    with pytest.raises(Exception):
        write(physical, bundle)
    assert len(table(physical, 'transfermarkt_referee_season_aggregates')) == 10
    assert receipts(physical).empty
    physical.test_db.fail_sql = None
    recovered = write(physical, bundle)
    assert recovered['verified'] and recovered['receipt_readback']
    assert len(table(physical, 'transfermarkt_referee_season_aggregates')) == 10
    assert len(receipts(physical)) == 1


@pytest.mark.parametrize('mutation', ['extra', 'missing'])
def test_missing_or_extra_physical_keys_fail(physical, mutation):
    physical.test_db.mutation = mutation
    with pytest.raises(Exception, match='physical|read-back|key|differs'):
        write(physical, aggregate_bundle(physical))


def test_null_lineage_and_match_fields_fail_structural_dq_before_write(physical):
    bundle = aggregate_bundle(physical)
    broken = dict(bundle.aggregates[0])
    broken['raw_capture_id'] = None
    with pytest.raises(Exception, match='lineage|DQ|structural'):
        write(physical, replace(bundle, aggregates=(broken, *bundle.aggregates[1:])))
    assert physical.test_db.events == []


def test_writer_mutations_are_limited_to_referee_tables_and_use_no_delete(physical):
    write(physical, aggregate_bundle(physical))
    mutations = [
        sql for sql in physical.test_db.events
        if sql.startswith(('CREATE', 'MERGE', 'INSERT', 'DELETE', 'UPDATE', 'ALTER', 'DROP'))
    ]
    assert mutations and not any(sql.startswith('DELETE') for sql in mutations)
    allowed = (
        'transfermarkt_referee_profiles', 'transfermarkt_referee_season_aggregates',
        'transfermarkt_native_write_manifest_v2',
    )
    assert all(any(table in sql for table in allowed) for sql in mutations)
    forbidden = ('squad_memberships', 'club_staff', 'transfermarkt_players', 'spielbericht')
    assert not any(any(table in sql for table in forbidden) for sql in mutations)


def test_physical_dq_reads_lineage_duplicates_and_run_receipts(physical):
    write(physical, aggregate_bundle(physical))
    report = referees.run_referee_physical_dq(physical.test_db, run_id='run-aggregates')
    assert report['status'] == 'GREEN' and report['completed'] is True
    assert report['receipt_count'] == 1 and report['match_row_count'] == 0
    assert report['tables'][referees.REFEREE_AGGREGATE_TABLE]['row_count'] == 10


def test_physical_dq_turns_null_lineage_or_duplicate_keys_red(physical):
    write(physical, aggregate_bundle(physical))
    physical.test_db.sql.execute(
        'UPDATE transfermarkt_referee_season_aggregates SET raw_capture_id=NULL WHERE rowid=1',
    )
    report = referees.run_referee_physical_dq(physical.test_db)
    assert report['status'] == 'RED' and 'null_lineage' in report['errors'][0]


def test_partial_business_commit_is_completed_by_same_plan(physical):
    bundle = aggregate_bundle(physical)
    physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    with pytest.raises(Exception):
        write(physical, bundle)
    physical.test_db.sql.execute(
        'DELETE FROM transfermarkt_referee_season_aggregates WHERE rowid=1',
    )
    physical.test_db.fail_sql = None
    recovered = write(physical, bundle)
    assert recovered['verified'] is True
    assert len(table(physical, 'transfermarkt_referee_season_aggregates')) == 10
    assert len(receipts(physical)) == 1
