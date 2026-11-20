from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

import pandas as pd
import pytest

from dags.utils import transfermarkt_attendance as attendance
from scrapers.transfermarkt.attendance import (
    ATTENDANCE_COLUMNS,
    ATTENDANCE_KEY,
    AttendanceExtraction,
    AttendancePage,
    AttendanceResult,
    confirmed_empty_attendance,
    parse_attendance,
)
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).parents[2] / 'fixtures' / 'transfermarkt' / 'attendance'
START = 'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'
CAPTURED = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
EXTRACTED = datetime(2026, 11, 20, 12, 1, 0, 123456, tzinfo=timezone.utc)
COMMITTED = datetime(2026, 11, 20, 13, 0, 0, 654321, tzinfo=timezone.utc)


class AttendanceCursor(MemoryCursor):
    def execute(self, sql, params=()):
        params = tuple(value.isoformat(timespec='microseconds') if isinstance(value, datetime)
                       else value for value in params)
        sql = sql.replace(
            'transfermarkt_attendance_receipts_v1',
            'transfermarkt_native_write_manifest_v2',
        )
        target_literal = "'" + attendance.ATTENDANCE_TABLE + "'"
        placeholder = '__attendance_target_table__'
        sql = sql.replace(target_literal, "'" + placeholder + "'")
        result = super().execute(sql, params)
        if placeholder in sql:
            self.db.sql.execute(
                'UPDATE transfermarkt_native_write_manifest_v2 SET target_table=? '
                'WHERE target_table=?',
                (attendance.ATTENDANCE_TABLE, placeholder),
            )
        return result


class AttendanceDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.mutation = None

    def cursor(self):
        return AttendanceCursor(self)


class AttendanceWriter(MemoryWriter):
    def _write_to_iceberg(self, *args, **kwargs):
        result = super()._write_to_iceberg(*args, **kwargs)
        if self.db.mutation == 'extra':
            self.db.sql.execute(
                'INSERT INTO transfermarkt_attendance_observations '
                'SELECT * FROM transfermarkt_attendance_observations LIMIT 1')
        elif self.db.mutation == 'missing':
            self.db.sql.execute(
                'DELETE FROM transfermarkt_attendance_observations '
                'WHERE rowid = (SELECT MIN(rowid) FROM transfermarkt_attendance_observations)')
        return result


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in ('TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID',
                 'TM_WRITE_MODE', 'TM_READER_REVISION'):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def attendance_policy(monkeypatch, tmp_path):
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] += [
        attendance.ATTENDANCE_TABLE,
        attendance.ATTENDANCE_RECEIPTS_TABLE,
    ]
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'attendance-policy.json'
    path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, attendance_policy):  # noqa: F811
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = AttendanceDB()
    original_cell = attendance.current._cell

    def fake_cell(column, value):
        if column in attendance.TIMESTAMP_COLUMNS:
            return attendance._canonical_cell(column, value)
        return original_cell(column, value)

    monkeypatch.setattr(attendance.current, '_cell', fake_cell)
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = AttendanceWriter(db)
    return scraper


def source_result() -> AttendanceResult:
    body = (FIXTURES / 'single_page.html').read_bytes()
    page = AttendancePage(
        START, body, 'capture-single', hashlib.sha256(body).hexdigest(), CAPTURED.isoformat(),
    )
    return parse_attendance([page], start_url=START, competition_id='GB1', edition_id=2025)


def extraction(*, at=EXTRACTED, result=None) -> AttendanceExtraction:
    return AttendanceExtraction(
        result or source_result(), 'scope-1', 'run-1', 'registry-writer-test', at.isoformat(),
        final_season=True, final_season_proof='registry:GB1:2025:closed',
    )


def write(instance, result, *, at=COMMITTED, clubs=('11', '631'), observed=CAPTURED):
    return attendance.write_attendance_observations(
        instance, result, clubs, PREFLIGHT,
        competition_id='GB1', edition_id='2025', observed_at=observed.isoformat(), committed_at=at,
    )


def business_table(instance):
    return pd.read_sql_query('SELECT * FROM transfermarkt_attendance_observations', instance.test_db.sql)


def receipt_table(instance):
    return pd.read_sql_query('SELECT * FROM transfermarkt_native_write_manifest_v2', instance.test_db.sql)


def restrict_policy(path, missing):
    policy = json.loads(path.read_text())
    policy['allowed_write_tables'] = [
        table for table in policy['allowed_write_tables'] if table not in missing
    ]
    path.write_text(json.dumps(policy))


def test_ddl_contains_complete_schema_and_disjoint_targets():
    ddl = attendance.attendance_ddl()
    assert len(ddl) == 2
    assert attendance.ATTENDANCE_TABLE in ddl[0]
    assert attendance.ATTENDANCE_RECEIPTS_TABLE in ddl[1]
    for column in ATTENDANCE_COLUMNS:
        assert re.search(rf'\b{re.escape(column)}\b', ddl[0])
    for column in attendance.RECEIPT_COLUMNS:
        assert re.search(rf'\b{column}\b', ddl[1])
    assert 'DELETE' not in '\n'.join(ddl)


def test_writer_returns_full_edition_physical_and_receipt_readback(physical):
    result = extraction()
    receipt = write(physical, result)
    assert receipt['verified'] is True and receipt['receipt_readback'] is True
    assert receipt['expected_clubs'] == receipt['row_count'] == 2
    assert receipt['expected_club_ids'] == ['11', '631']
    assert receipt['observed_club_ids'] == ['11', '631']
    assert receipt['status_counts'] == {'observed_present': 2}
    assert receipt['capture_status'] == 'observed_present'
    assert receipt['final_season'] is True
    assert receipt['final_season_proof'] == 'registry:GB1:2025:closed'
    assert receipt['natural_key_columns'] == list(ATTENDANCE_KEY)
    assert receipt['raw_capture_ids'] == ['capture-single']
    assert receipt['run_id'] == result.run_id
    assert receipt['committed_batch'] == result.batch_id
    assert len(receipt['plan_sha256']) == len(receipt['physical_hash']) == 64
    assert receipt['committed_at'] == COMMITTED.isoformat()
    rows = business_table(physical)
    assert len(rows) == 2
    assert list(rows.columns) == list(ATTENDANCE_COLUMNS)
    assert set(rows['club_id']) == {'11', '631'}
    assert not rows.duplicated(list(ATTENDANCE_KEY)).any()
    persisted_receipt = receipt_table(physical).iloc[0]
    assert persisted_receipt['receipt_id'] == receipt['receipt_id']
    assert persisted_receipt['physical_hash'] == receipt['physical_hash']


def test_identical_replay_is_noop_and_later_replay_preserves_source_clock(physical):
    result = extraction()
    first = write(physical, result)
    repeat = write(physical, result)
    assert first['receipt_id'] == repeat['receipt_id']
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 1
    later_result = extraction(at=EXTRACTED + timedelta(hours=2))
    later = write(physical, later_result, at=COMMITTED + timedelta(hours=2))
    assert later['receipt_id'] != first['receipt_id']
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 2
    assert set(business_table(physical)['extracted_at']) == {later_result.extracted_at.replace('+00:00', '')}
    assert set(business_table(physical)['observed_at']) == {'2026-11-20T12:00:00.000000'}
    with pytest.raises(Exception, match='stale'):
        write(physical, result)


def test_timestamp_offsets_are_normalized_before_stale_and_replay_checks(physical):
    result = extraction()
    write(physical, result)
    india = timezone(timedelta(hours=5, minutes=30))
    later_at = (EXTRACTED + timedelta(hours=2)).astimezone(india)
    later = write(physical, extraction(at=later_at), at=COMMITTED + timedelta(hours=2))
    assert later['verified'] is True
    assert set(business_table(physical)['extracted_at']) == {'2026-11-20T14:01:00.123456'}
    with pytest.raises(Exception, match='stale'):
        write(physical, result)


def test_writer_rejects_changed_immutable_payload_at_same_key(physical):
    result = extraction()
    write(physical, result)
    parsed = source_result()
    changed = replace(parsed.rows[0], capacity=1, capacity_raw='1')
    conflicting = AttendanceResult('observed_present', (changed, parsed.rows[1]))
    with pytest.raises(Exception, match='immutable|differs'):
        write(physical, extraction(result=conflicting))
    assert set(business_table(physical)['capacity']) == {60704, 40173}


@pytest.mark.parametrize('mutation', ['unexpected', 'duplicate'])
def test_invalid_existing_edition_rows_fail_before_business_write(physical, mutation):
    result = extraction()
    write(physical, result)
    frame = attendance._frame(result).iloc[[0]].copy()
    if mutation == 'unexpected':
        frame['club_id'] = '999'
    physical._iceberg_writer.write_dataframe(
        df=frame, database='bronze', table='transfermarkt_attendance_observations',
        merge_keys=[], add_metadata=False, source='transfermarkt',
    )
    assert len(business_table(physical)) == 3
    with pytest.raises(Exception, match='unexpected existing|duplicate existing'):
        write(physical, result)
    assert len(business_table(physical)) == 3
    assert len(receipt_table(physical)) == 1


def test_mixed_observation_times_cannot_form_one_edition_snapshot(physical):
    parsed = source_result()
    later = CAPTURED + timedelta(seconds=1)
    changed = replace(parsed.rows[0], observed_at=later.isoformat(), captured_at=later.isoformat())
    result = AttendanceResult('observed_present', (changed, parsed.rows[1]))
    with pytest.raises(Exception, match='edition identity'):
        write(physical, extraction(result=result))
    assert physical.test_db.events == []


@pytest.mark.parametrize('missing', [
    (attendance.ATTENDANCE_TABLE,),
    (attendance.ATTENDANCE_RECEIPTS_TABLE,),
    (attendance.ATTENDANCE_TABLE, attendance.ATTENDANCE_RECEIPTS_TABLE),
])
def test_missing_grants_fail_before_any_sql(physical, attendance_policy, missing):
    restrict_policy(attendance_policy, missing)
    with pytest.raises(Exception, match='authorize|policy|grant'):
        write(physical, extraction())
    assert physical.test_db.events == []


def test_structural_dq_failure_does_not_write(physical):
    with pytest.raises(Exception, match='DQ|participants'):
        write(physical, extraction(), clubs=('11',))
    assert physical.test_db.events == []


def test_duplicate_expected_club_ids_do_not_write(physical):
    with pytest.raises(Exception, match='duplicates'):
        write(physical, extraction(), clubs=('11', '11', '631'))
    assert physical.test_db.events == []


@pytest.mark.parametrize('failure', ['business', 'business_readback', 'receipt', 'receipt_readback'])
def test_failed_write_or_readback_never_returns_verified(physical, monkeypatch, failure):
    if failure == 'business':
        monkeypatch.setattr(
            physical._iceberg_writer, 'write_dataframe',
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError('write failed')),
        )
    elif failure == 'business_readback':
        physical.test_db.corrupt_table = 'transfermarkt_attendance_observations'
    elif failure == 'receipt':
        physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    else:
        original = attendance.current.run._query_dataframe

        def corrupt_receipt(connection, sql, params):
            result = original(connection, sql, params)
            if 'attendance_receipts' in sql:
                result['physical_hash'] = 'corrupt'
            return result

        monkeypatch.setattr(attendance.current.run, '_query_dataframe', corrupt_receipt)
    with pytest.raises(Exception):
        write(physical, extraction())


def test_same_plan_recovers_receipt_after_business_commit_without_duplicates(physical):
    result = extraction()
    physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    with pytest.raises(Exception):
        write(physical, result)
    assert len(business_table(physical)) == 2
    assert receipt_table(physical).empty
    physical.test_db.fail_sql = None
    recovered = write(physical, result)
    assert recovered['verified'] and recovered['receipt_readback']
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 1


@pytest.mark.parametrize('mutation', ['extra', 'missing'])
def test_missing_or_extra_physical_rows_fail_readback(physical, mutation):
    physical.test_db.mutation = mutation
    with pytest.raises(Exception, match='physical|read-back|differs'):
        write(physical, extraction())


def test_existing_receipt_cannot_conceal_physical_damage(physical):
    result = extraction()
    write(physical, result)
    physical.test_db.sql.execute(
        "UPDATE transfermarkt_attendance_observations SET source_body_hash='damaged'")
    with pytest.raises(Exception, match='physical|differ|read-back'):
        write(physical, result)


def test_empty_edition_writes_receipt_without_fabricated_rows(physical):
    result = AttendanceExtraction(
        confirmed_empty_attendance(completeness_proof='capture:empty-layout-v1'),
        'scope-empty', 'run-empty', 'registry-writer-test', EXTRACTED.isoformat(),
    )
    receipt = write(physical, result, clubs=())
    assert receipt['verified'] and receipt['row_count'] == 0
    assert receipt['expected_club_ids'] == receipt['observed_club_ids'] == []
    assert receipt['status_counts'] == {'observed_empty': 1}
    assert receipt['capture_status'] == 'observed_empty'
    assert business_table(physical).empty
    assert len(receipt_table(physical)) == 1


def test_writer_mutations_are_limited_to_attendance_tables_and_use_no_delete(physical):
    write(physical, extraction())
    mutations = [sql for sql in physical.test_db.events
                 if sql.startswith(('CREATE', 'MERGE', 'INSERT', 'DELETE', 'UPDATE', 'ALTER', 'DROP'))]
    assert mutations
    assert not any(sql.startswith('DELETE') for sql in mutations)
    forbidden = ('squad_memberships', 'player_attribute_observations', 'club_staff', 'transfermarkt_players')
    assert not any(any(table in sql for table in forbidden) for sql in mutations)
    allowed = ('transfermarkt_attendance_observations', 'transfermarkt_native_write_manifest_v2')
    assert all(any(table in sql for table in allowed) for sql in mutations)


def test_stored_receipt_physical_hash_mismatch_never_returns_verified(physical):
    result = extraction()
    write(physical, result)
    physical.test_db.sql.execute(
        "UPDATE transfermarkt_native_write_manifest_v2 SET physical_hash='0' || physical_hash")
    with pytest.raises(Exception, match='stored|differs'):
        write(physical, result)
