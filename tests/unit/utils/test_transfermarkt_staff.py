"""Offline parser, physical writer, and read-only role projection regressions."""
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pandas as pd
import pytest

from dags.utils import transfermarkt_staff as staff
from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.staff import STAFF_KEY, parse_staff
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, SCOPE, scraper,
)

pytestmark = pytest.mark.unit
FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'staff'
NOW = datetime(2026, 10, 20, 23, 30, 0, 123456, tzinfo=timezone.utc)
URL = 'https://www.transfermarkt.com/club-10/mitarbeiter/verein/10'


def page(people=(('1', 'Manager'),), section='Coaching Staff'):
    return ('<div class="box"><h2 class="content-box-headline">' + section + '</h2>'
            '<table><thead><tr><th>Name/Position</th><th>Appointed</th></tr></thead><tbody>'
            + ''.join('<tr><td><table class="inline-table"><tr><td>'
                      f'<a href="/person-{person}/profil/trainer/{person}">Person {person}</a>'
                      f'</td></tr><tr><td>{role}</td></tr></table></td><td>Jul 1, 2026</td></tr>'
                      for person, role in people) + '</tbody></table></div>')


@pytest.mark.parametrize('name,count,sections', [
    ('ca-boca-juniors', 18, 6), ('fc-bayern-munchen', 108, 8), ('sc-paderborn-07', 41, 8),
])
def test_parser_keeps_all_sections_and_nested_rows(name, count, sections):
    rows = parse_staff((FIXTURES / (name + '.html')).read_text(), '10')
    assert len(rows) == count
    assert len({row['section'] for row in rows}) == sections
    assert all(row['club_id'] == '10' and row['role'] and row['name'] for row in rows)
    assert any(row['role'] != 'Manager' for row in rows)


def test_parser_keeps_multiple_roles_of_one_person():
    rows = parse_staff(page((('1', 'Manager'), ('1', 'Sporting Director'))), '10')
    assert Counter(row['person_id'] for row in rows) == {'1': 2}
    assert {row['role'] for row in rows} == {'Manager', 'Sporting Director'}


@pytest.mark.parametrize('html', [
    '<html>blocked</html>', page().replace('<th>Appointed</th>', ''),
    page().replace('Person 1', ''), page().replace('Manager', '-'),
    page().replace('<tbody>', '<tbody><tr><td>broken</td></tr>'),
    page() + '<div class="box"><h2 class="content-box-headline">Medical department</h2></div>',
    page().replace('class="content-box-headline"', ''),
])
def test_partial_or_empty_staff_is_an_error_for_parser_and_http_validator(html):
    with pytest.raises(ValueError):
        parse_staff(html, '10')
    validator = tm.TransfermarktScraper._endpoint_validator('club_staff', False)
    assert validator(html)


class StaffCursor(MemoryCursor):
    def execute(self, sql, params=()):
        sql = re.sub(r"DATE '([^']*)'", r"'\1'", sql)
        sql = re.sub(r"TIMESTAMP '([^']*)'", lambda m: "TIMESTAMP '" + m[1].replace(' ', 'T') + "'", sql)
        params = tuple(value.isoformat(timespec='microseconds') if isinstance(value, datetime)
                       else value for value in params)
        # Extend the existing physical fake only for the new independent receipt.
        sql = sql.replace('transfermarkt_club_staff_receipts_v1', 'transfermarkt_native_write_manifest_v2')
        return super().execute(sql, params)


class StaffDB(MemoryDB):
    def cursor(self):
        return StaffCursor(self)


@pytest.fixture
def staff_policy(monkeypatch, tmp_path):
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] += [staff.STAFF_TABLE, staff.STAFF_RECEIPTS_TABLE]
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'staff-policy.json'
    path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, staff_policy):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = StaffDB()
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = MemoryWriter(db)
    scraper._http_client._raw_store = scraper.test_store
    return scraper


def capture(instance, html=None, at=NOW, club_id='10', scope=None):
    scope = scope or staff.current._scope(instance, SCOPE)
    url = URL.replace('/10', '/' + club_id)
    raw = instance.test_store.store_attempt(
        url=url, body=(html or page()).encode(), status_code=200, headers={},
        fetched_at=at.isoformat(), cycle_id='staff-test', scope_id=scope['scope_id'],
        endpoint='club_staff', attempt=0,
    )
    return FetchOutcome(status=FetchStatus.OK, value=html or page(),
                        raw_capture_id=raw.capture_id, raw_body_hash=raw.content_hash,
                        raw_fetched_at=raw.fetched_at)


def write(instance, html=None, at=NOW, **kwargs):
    outcome = capture(instance, html, at)
    return staff.write_current_staff(instance, SCOPE, PREFLIGHT, 'portion-test',
                                      club_id='10', outcome=outcome, source_url=URL, **kwargs)


def table(instance):
    return pd.read_sql_query('SELECT * FROM transfermarkt_club_staff', instance.test_db.sql)


def restrict_staff_policy(path, missing):
    policy = json.loads(path.read_text())
    policy['allowed_write_tables'] = [table for table in policy['allowed_write_tables'] if table not in missing]
    path.write_text(json.dumps(policy))


@pytest.mark.parametrize('missing', [
    (staff.STAFF_TABLE,), (staff.STAFF_RECEIPTS_TABLE,),
    (staff.STAFF_TABLE, staff.STAFF_RECEIPTS_TABLE),
])
def test_writer_rejects_missing_staff_grants_before_raw_read_or_sql(physical, staff_policy, monkeypatch, missing):
    outcome = capture(physical)
    restrict_staff_policy(staff_policy, missing)
    def unexpected_read(*args):
        pytest.fail('raw read before staff policy authorization')
    monkeypatch.setattr(physical.test_store, 'load_capture', unexpected_read)
    with pytest.raises(staff.current.CurrentWriteError, match='authorize staff tables'):
        staff.write_current_staff(physical, SCOPE, PREFLIGHT, 'denied',
                                  club_id='10', outcome=outcome, source_url=URL)
    assert physical.test_db.events == []


@pytest.mark.parametrize('missing', [
    (), (staff.STAFF_TABLE,), (staff.STAFF_RECEIPTS_TABLE,),
    (staff.STAFF_TABLE, staff.STAFF_RECEIPTS_TABLE),
])
def test_production_preflight_requires_staff_grants_before_scraper_construction(staff_policy, monkeypatch, missing):
    from dags.scripts import run_transfermarkt_current as runner
    from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row

    restrict_staff_policy(staff_policy, missing)
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    monkeypatch.setenv('TM_PROXY_CONTROL_URL', 'http://offline.invalid')
    monkeypatch.setenv('TRANSFERMARKT_REQUIRE_RAW_STORE', 'true')
    monkeypatch.setenv('TM_WRITE_MODE', 'dual')
    constructed = []
    def create(**kwargs):
        constructed.append(kwargs)
        return 'scraper'
    monkeypatch.setattr(tm, 'TransfermarktScraper', create)
    target = SimpleNamespace(competition_id='GB1', edition_id='2026', scope_id='scope')
    row = _joined_row(_competition('GB1'), _edition('GB1', '2026'))
    if missing:
        with pytest.raises(staff.current.CurrentWriteError, match='authorize staff tables'):
            runner._production_factory(target, row, {}, 1000, {})
        assert constructed == []
    else:
        assert runner._production_factory(target, row, {}, 1000, {}) == 'scraper'
        assert len(constructed) == 1


@pytest.mark.parametrize('phase', ['business', 'receipt'])
def test_writer_rechecks_staff_grants_before_each_write(physical, staff_policy, monkeypatch, phase):
    if phase == 'business':
        original = staff._staff_frame
        def frame(*args):
            result = original(*args)
            restrict_staff_policy(staff_policy, (staff.STAFF_TABLE,))
            return result
        monkeypatch.setattr(staff, '_staff_frame', frame)
    else:
        original = staff.current._read_back
        def read_back(*args):
            result = original(*args)
            restrict_staff_policy(staff_policy, (staff.STAFF_RECEIPTS_TABLE,))
            return result
        monkeypatch.setattr(staff.current, '_read_back', read_back)
    with pytest.raises(staff.current.CurrentWriteError, match='authorize staff tables'):
        write(physical)
    if phase == 'business':
        assert physical.test_db.events == []
    else:
        assert not any(sql.startswith('MERGE INTO iceberg.ops.') for sql in physical.test_db.events)


def test_native_day_merge_is_idempotent_and_keeps_history_without_coach_writes(physical):
    first = write(physical, page((('1', 'Head Coach'), ('2', 'Assistant Manager'))))
    repeat = write(physical, page((('1', 'Head Coach'), ('2', 'Assistant Manager'))))
    assert first['verified'] and first['bronze_manifest'] == repeat['bronze_manifest']
    assert first['physical']['physical_hash'] == repeat['physical']['physical_hash']
    assert len(table(physical)) == 2
    # Raw time +02:00 still belongs to the same UTC day.
    write(physical, page((('1', 'Head Coach'), ('2', 'Assistant Manager'))),
          at=NOW.astimezone(timezone(timedelta(hours=2))) + timedelta(minutes=1))
    assert len(table(physical)) == 2
    assert set(table(physical).observed_day) == {'2026-10-20'}
    write(physical, page((('1', 'Head Coach'), ('2', 'Assistant Manager'))), at=NOW + timedelta(days=1))
    assert len(table(physical)) == 4
    assert not table(physical).duplicated(list(STAFF_KEY)).any()
    mutations = [sql for sql in physical.test_db.events if sql.startswith(('MERGE', 'CREATE', 'DELETE', 'INSERT'))]
    assert all('coach' not in sql and 'dual_write' not in sql for sql in mutations)
    assert all('DELETE' not in sql for sql in mutations)


@pytest.mark.parametrize('failure', ['business', 'business_readback', 'receipt', 'receipt_readback'])
def test_failed_physical_write_or_readback_never_returns_ack(physical, monkeypatch, failure):
    if failure == 'business':
        monkeypatch.setattr(physical._iceberg_writer, 'write_dataframe',
                            lambda **kw: (_ for _ in ()).throw(RuntimeError('write failed')))
    elif failure == 'business_readback':
        physical.test_db.corrupt_table = 'transfermarkt_club_staff'
    elif failure == 'receipt':
        physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    else:
        original = staff.current.run._query_dataframe
        def query(connection, sql, params):
            result = original(connection, sql, params)
            if 'staff_receipts' in sql:
                result['physical_hash'] = 'corrupt'
            return result
        monkeypatch.setattr(staff.current.run, '_query_dataframe', query)
    with pytest.raises((RuntimeError, staff.current.CurrentWriteError)):
        write(physical)
    if failure in {'business', 'business_readback', 'receipt'}:
        assert physical.test_db.sql.execute('SELECT COUNT(*) FROM transfermarkt_native_write_manifest_v2').fetchone() == (0,)


def test_stale_capture_and_partial_snapshot_cannot_replace_committed_staff(physical):
    write(physical, page((('1', 'Manager'), ('2', 'Assistant'))))
    before = table(physical)
    with pytest.raises(staff.current.CurrentWriteError, match='90%'):
        write(physical, at=NOW + timedelta(days=7))
    with pytest.raises(staff.current.CurrentWriteError, match='newer'):
        write(physical, page((('1', 'Manager'), ('2', 'Assistant'))), at=NOW - timedelta(minutes=1))
    pd.testing.assert_frame_equal(before, table(physical))


def roles(instance, stints):
    rows = []
    for stint in stints:
        rows.append({**dict.fromkeys(tm.COACH_STINT_COLUMNS + tm._METADATA_COLUMNS),
                     'club_id': '10', 'coach_id': '1', 'role': 'Manager',
                     'appointed_date': '2026-07-01', **stint})
    pd.DataFrame(rows).to_sql('transfermarkt_coach_stints', instance.test_db.sql, if_exists='replace', index=False)
    sql = staff.coach_roles_sql().replace('iceberg.bronze.', '').replace('iceberg.ops.', '')
    sql = sql.replace('transfermarkt_club_staff_receipts_v1', 'transfermarkt_native_write_manifest_v2')
    return pd.read_sql_query(sql, instance.test_db.sql)


def test_select_only_enriches_matching_open_dated_stint(physical):
    write(physical, page((('1', 'Head Coach'),)))
    result = roles(physical, [
        {}, {'left_date': '2026-10-20'}, {'appointed_date': None},
        {'appointed_date': '2026-10-21'}, {'club_id': '20'}, {'coach_id': '2'},
    ])
    assert result.role.tolist() == ['Head Coach', 'Manager', 'Manager', 'Manager', 'Manager', 'Manager']
    assert physical.test_db.sql.execute('SELECT DISTINCT role FROM transfermarkt_coach_stints').fetchall() == [('Manager',)]


def test_latest_club_snapshot_prevents_reviving_disappeared_person(physical):
    write(physical, page((('1', 'Head Coach'),)))
    write(physical, page((('2', 'New Head Coach'),)), at=NOW + timedelta(days=7))
    assert roles(physical, [{}]).role.tolist() == ['Manager']


def test_conflicting_roles_are_not_enriched(physical):
    write(physical, page((('1', 'Head Coach'), ('1', 'Assistant Manager'))))
    assert roles(physical, [{}]).role.tolist() == ['Manager']


def test_unconfirmed_same_day_update_does_not_resurrect_older_personal_role(physical):
    write(physical, page((('1', 'Head Coach'), ('2', 'Assistant'))))
    physical.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_native_write_manifest_v2'
    with pytest.raises(RuntimeError, match='late physical'):
        write(physical, page((('2', 'Assistant'), ('3', 'Head Coach'))), at=NOW + timedelta(minutes=1))
    assert roles(physical, [{}]).role.tolist() == ['Manager']
    assert len(table(physical)) == 3  # The failed capture deleted no previous key.
