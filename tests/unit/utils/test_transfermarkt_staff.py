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


@pytest.mark.parametrize(
    'fixture_token,club_id,expected_roles,directors,multiple_roles,extra_sections',
    [
        pytest.param(
            'boca', '189',
            {
                'Manager': 1, 'Assistant Manager': 3, 'Goalkeeping Coach': 1,
                'Fitness coach': 1, 'Sporting Director': 2, 'President': 1,
                'Vice-President': 1, 'Head of Youth Scouting': 1,
                'Club Doctor': 1, 'Physiotherapist': 1, 'Academy manager': 1,
                'Youth Coach': 4,
            },
            {'14855', '22804'}, {}, set(), id='boca',
        ),
        pytest.param(
            'bayern', '27',
            {
                'Manager': 1, 'Assistant Manager': 4, 'Goalkeeping Coach': 2,
                'Performance Manager': 1, 'Athletic Coach': 4, 'Rehab coach': 3,
                'Individual Coach': 1, 'Chief Analyst': 2, 'Match Analyst': 6,
                'Sporting CEO': 1, 'Sporting Director': 1,
                'Director of Development': 2, 'Director of Finance': 1,
                'Director of Marketing and Sales': 1, 'Executive Assistant': 1,
                'President': 1, 'Vice-President': 1,
                'Chairman of the Supervisory Board': 1, 'Chairman': 1,
                'Deputy chairman of the board': 2,
                'Member of the supervisory board': 5,
                'Chief Marketing Officer': 1, 'Member of administrative board': 1,
                'Honorary President': 1, 'Head of Scouting': 1, 'Chief Scout': 1,
                'Loan Player Manager': 1, 'Scout': 9, 'Youth Chief Scout': 1,
                'Youth Scouting Co-ordinator': 2, 'Youth Scout': 6,
                'Club Doctor': 3, 'Medical Director Physiotherapy': 1,
                'Physiotherapist': 6, 'Sports Scientist': 1, 'Nutritionist': 1,
                'Director of Media and Communications': 1, 'Press officer': 1,
                'Team Manager': 3, 'Kit Manager': 4, 'Bus driver': 2,
                'Stadium Announcer': 1, 'Club representative': 12,
                'Safety Officer': 1, 'Head of Marketing': 1, 'Academy manager': 1,
                'Coordinator of talent management': 1,
                'Academy Goalkeeping coordinator': 1, 'Head of Soccer School': 1,
            },
            {'39281'},
            {
                '74617': {('Coaching Staff', 'Athletic Coach'),
                          ('Coaching Staff', 'Rehab coach')},
                '51283': {('Coaching Staff', 'Athletic Coach'),
                          ('Coaching Staff', 'Rehab coach')},
                '6965': {('Board of Directors', 'President'),
                         ('Board of Directors', 'Chairman of the Supervisory Board')},
                '5327': {('Board of Directors', 'Vice-President'),
                         ('Board of Directors', 'Member of the supervisory board')},
                '5249': {('Board of Directors', 'Member of the supervisory board'),
                         ('Board of Directors', 'Honorary President')},
                '135056': {('Other', 'Team Manager'), ('Other', 'Safety Officer')},
            },
            {'Other', 'Public Relations'}, id='bayern',
        ),
        pytest.param(
            'paderborn', '127',
            {
                'Manager': 1, 'Assistant Manager': 2, 'Goalkeeping Coach': 1,
                'Athletic Coach': 1, 'Rehab coach': 1, 'Individual Coach': 1,
                'Video Analyst': 1, 'Match Analyst': 1, 'Managing Director Sport': 1,
                'Chief Executive Officer': 1, 'Sporting Director': 1,
                'Head of first team football': 1, 'Director of Development': 1,
                'President': 1, 'Vice-President': 2,
                'Chairman of the Supervisory Board': 1,
                'Deputy chairman of the board': 1,
                'Member of the supervisory board': 3, 'Economic council': 3,
                'Counselor of administrative board': 1, 'Chief Scout': 2, 'Scout': 1,
                'Club Doctor': 4, 'Medical Director Physiotherapy': 1,
                'Physiotherapist': 2, 'Head of Media and Communications': 1,
                'Medienmitarbeiter': 1, 'Team Manager': 1, 'Kit Manager': 1,
                'Academy Manager': 1,
            },
            {'112401'},
            {
                '80401': {('Coaching Staff', 'Video Analyst'),
                          ('Coaching Staff', 'Match Analyst')},
                '131277': {('Board of Directors', 'Chairman of the Supervisory Board'),
                           ('Board of Directors', 'Economic council')},
                '131278': {('Board of Directors', 'Deputy chairman of the board'),
                           ('Board of Directors', 'Economic council')},
                '131280': {('Board of Directors', 'Member of the supervisory board'),
                           ('Board of Directors', 'Economic council')},
            },
            {'Other', 'Public Relations'}, id='paderborn',
        ),
    ],
)
def test_real_fixtures_preserve_every_raw_section_and_role(
    fixture_token, club_id, expected_roles, directors, multiple_roles, extra_sections,
):
    fixture_path = FIXTURES / {
        'boca': 'ca-boca-juniors.html',
        'bayern': 'fc-bayern-munchen.html',
        'paderborn': 'sc-paderborn-07.html',
    }[fixture_token]
    rows = parse_staff(fixture_path.read_text(encoding='utf-8'), club_id)

    assert {row['section'] for row in rows} == {
        'Board of Directors', 'Coaching Staff', 'Management',
        'Medical department', 'Scouting', 'Youth',
    } | extra_sections
    assert Counter(row['role'] for row in rows) == Counter(expected_roles)
    director_rows = [row for row in rows if row['role'] == 'Sporting Director']
    assert {row['person_id'] for row in director_rows} == directors
    assert {row['section'] for row in director_rows} == {'Management'}
    for person_id in directors:
        assert {
            (row['section'], row['role']) for row in rows
            if row['person_id'] == person_id
        } == {('Management', 'Sporting Director')}
    for person_id, expected_pairs in multiple_roles.items():
        assert {
            (row['section'], row['role']) for row in rows
            if row['person_id'] == person_id
        } == expected_pairs


def test_sporting_director_is_optional_for_conclusive_staff_observation(physical):
    html = page((('1', 'Manager'),))
    rows = parse_staff(html, '10')
    assert len(rows) == 1
    assert rows[0]['club_id'] == '10'
    assert rows[0]['person_id'] == '1'
    assert rows[0]['person_slug'] == 'person-1'
    assert rows[0]['name'] == 'Person 1'
    assert rows[0]['section'] == 'Coaching Staff'
    assert rows[0]['role'] == 'Manager'
    assert 'appointed_date' in rows[0]

    result = write(physical, html)
    assert result['verified'] is True
    stored = table(physical)
    assert len(stored) == 1
    assert stored['role'].tolist() == ['Manager']


def test_dated_role_change_keeps_both_natural_keys_and_receipts(physical):
    first = write(physical, page((('1', 'Head Coach'),)))
    second_at = NOW + timedelta(days=7)
    second_html = page((('1', 'Sporting Director'),))
    second = write(physical, second_html, at=second_at)
    replay = write(physical, second_html, at=second_at)

    assert first['verified'] is True
    assert second['verified'] is True
    assert replay['verified'] is True
    for field in ('bronze_manifest', 'raw_capture_id'):
        assert replay[field] == second[field]
    assert replay['physical']['physical_hash'] == second['physical']['physical_hash']

    first_day = NOW.date().isoformat()
    second_day = second_at.date().isoformat()
    stored = table(physical)
    assert len(stored) == 2
    assert not stored.duplicated(subset=list(STAFF_KEY)).any()
    assert set(stored[
        ['person_id', 'section', 'role', 'observed_day']
    ].itertuples(index=False, name=None)) == {
        ('1', 'Coaching Staff', 'Head Coach', first_day),
        ('1', 'Coaching Staff', 'Sporting Director', second_day),
    }

    receipts = pd.read_sql_query(
        'SELECT observed_day, row_count, physical_hash '
        'FROM transfermarkt_native_write_manifest_v2 WHERE club_id = ?',
        physical.test_db.sql, params=('10',),
    )
    assert len(receipts) == 2
    assert Counter(receipts['observed_day']) == Counter({first_day: 1, second_day: 1})
    assert set(receipts.itertuples(index=False, name=None)) == {
        (first_day, 1, first['physical']['physical_hash']),
        (second_day, 1, second['physical']['physical_hash']),
    }


def test_staff_physical_readback_uses_every_natural_key(physical, monkeypatch):
    from inspect import signature

    original = staff.current.run._query_dataframe
    query_signature = signature(original)
    queries = []

    def query_dataframe(*args, **kwargs):
        bound = query_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        queries.append((bound.arguments['sql'], tuple(bound.arguments['params'] or ())))
        return original(*args, **kwargs)

    monkeypatch.setattr(staff.current.run, '_query_dataframe', query_dataframe)
    result = write(physical, page((
        ('1', 'Manager'), ('2', 'Assistant Manager'), ('3', 'Goalkeeping Coach'),
    )))

    business_queries = [
        (sql, params) for sql, params in queries
        if 'FROM iceberg.bronze.transfermarkt_club_staff WHERE _batch_id = ?'
        in ' '.join(sql.split())
    ]
    assert len(business_queries) == 1
    sql, params = business_queries[0]
    compact_sql = ''.join(sql.split())
    assert '(club_id,person_id,section,role,observed_day)IN' in compact_sql
    assert compact_sql.count('(?,?,?,?,?)') == 3
    assert len(params) == 1 + 3 * 5
    assert params[0] == result['bronze_manifest']

    stored = table(physical)
    assert len(stored) == 3
    assert not stored.duplicated(subset=list(STAFF_KEY)).any()
    assert set(stored[['person_id', 'section', 'role']].itertuples(
        index=False, name=None,
    )) == {
        ('1', 'Coaching Staff', 'Manager'),
        ('2', 'Coaching Staff', 'Assistant Manager'),
        ('3', 'Coaching Staff', 'Goalkeeping Coach'),
    }
    assert {
        tuple(params[offset:offset + 5]) for offset in range(1, len(params), 5)
    } == set(stored[list(STAFF_KEY)].itertuples(index=False, name=None))
    assert result['verified'] is True
    assert result['physical']['physical_hash']


def test_staff_readback_rejects_unexpected_same_batch_row(physical, monkeypatch):
    original = physical._iceberg_writer.write_dataframe

    def write_with_extra_row(**kwargs):
        result = original(**kwargs)
        extra = kwargs['df'].iloc[[0]].copy()
        extra['person_id'] = 'unexpected'
        extra['name'] = 'Unexpected Person'
        original(**{**kwargs, 'df': extra})
        return result

    monkeypatch.setattr(physical._iceberg_writer, 'write_dataframe', write_with_extra_row)
    with pytest.raises(staff.current.CurrentWriteError, match='physical business/lineage mismatch'):
        write(physical)


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


@pytest.mark.parametrize('key', STAFF_KEY)
@pytest.mark.parametrize('invalid', ('missing', 'null', 'duplicate_column'))
def test_staff_readback_rejects_invalid_natural_key_columns(monkeypatch, key, invalid):
    frame = pd.DataFrame([{
        'club_id': '10', 'person_id': '1', 'section': 'Coaching Staff',
        'role': 'Manager', 'observed_day': '2026-10-20', '_batch_id': 'staff-test',
    }])
    if invalid == 'missing':
        frame = frame.drop(columns=[key])
    elif invalid == 'null':
        frame.loc[0, key] = None
    else:
        frame = pd.concat([frame, frame[[key]]], axis=1)
    output = SimpleNamespace(key='club_staff', table_name='transfermarkt_club_staff', replace_keys=())

    def unexpected_query(*args, **kwargs):
        pytest.fail('invalid natural keys must fail before querying physical rows')

    monkeypatch.setattr(staff.current.run, '_query_dataframe', unexpected_query)
    with pytest.raises(staff.current.CurrentWriteError, match='natural key'):
        staff.current._read_back(SimpleNamespace(), output, frame, 'staff-test', natural_keys=STAFF_KEY)


def test_staff_readback_rejects_duplicate_natural_keys(monkeypatch):
    row = {
        'club_id': '10', 'person_id': '1', 'section': 'Coaching Staff',
        'role': 'Manager', 'observed_day': '2026-10-20', '_batch_id': 'staff-test',
    }
    frame = pd.DataFrame([row, row])
    output = SimpleNamespace(key='club_staff', table_name='transfermarkt_club_staff', replace_keys=())

    def unexpected_query(*args, **kwargs):
        pytest.fail('duplicate natural keys must fail before querying physical rows')

    monkeypatch.setattr(staff.current.run, '_query_dataframe', unexpected_query)
    with pytest.raises(staff.current.CurrentWriteError, match='natural key'):
        staff.current._read_back(SimpleNamespace(), output, frame, 'staff-test', natural_keys=STAFF_KEY)


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
def test_production_factory_allows_current_without_staff_grants(staff_policy, monkeypatch, missing):
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
        def read_back(*args, **kwargs):
            result = original(*args, **kwargs)
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
