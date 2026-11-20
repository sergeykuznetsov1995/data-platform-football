from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from dags.utils import transfermarkt_club_values as club_values
from scrapers.transfermarkt.club_values import (
    CLUB_VALUE_COLUMNS, CLUB_VALUE_KEY, ClubValueCapture, parse_club_values,
)
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)


pytestmark = pytest.mark.unit
EXTRACTED = datetime(2026, 11, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
COMMITTED = datetime(2026, 11, 17, 13, 0, 0, 654321, tzinfo=timezone.utc)
CAPTURED = datetime(2026, 11, 8, 4, 57, 13, 167286, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in ('TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID', 'TM_WRITE_MODE', 'TM_READER_REVISION'):
        monkeypatch.delenv(name, raising=False)


class ClubValueCursor(MemoryCursor):
    def execute(self, sql, params=()):
        params = tuple(value.isoformat(timespec='microseconds') if isinstance(value, datetime) else value
                       for value in params)
        sql = sql.replace('transfermarkt_club_value_receipts_v1', 'transfermarkt_native_write_manifest_v2')
        target_literal = "'" + club_values.CLUB_VALUE_TABLE + "'"
        placeholder = '__club_value_target_table__'
        sql = sql.replace(target_literal, "'" + placeholder + "'")
        result = super().execute(sql, params)
        if placeholder in sql:
            self.db.sql.execute(
                'UPDATE transfermarkt_native_write_manifest_v2 SET target_table=? WHERE target_table=?',
                (club_values.CLUB_VALUE_TABLE, placeholder),
            )
        return result


class ClubValueDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.mutation = None

    def cursor(self):
        return ClubValueCursor(self)


class ClubValueWriter(MemoryWriter):
    def _write_to_iceberg(self, *args, **kwargs):
        result = super()._write_to_iceberg(*args, **kwargs)
        if self.db.mutation == 'extra':
            self.db.sql.execute(
                'INSERT INTO transfermarkt_club_value_observations '
                'SELECT * FROM transfermarkt_club_value_observations LIMIT 1')
        elif self.db.mutation == 'missing':
            self.db.sql.execute(
                'DELETE FROM transfermarkt_club_value_observations '
                'WHERE rowid = (SELECT MIN(rowid) FROM transfermarkt_club_value_observations)')
        return result


@pytest.fixture
def club_value_policy(monkeypatch, tmp_path):
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] = list(dict.fromkeys(
        policy['allowed_write_tables'] + [club_values.CLUB_VALUE_TABLE, club_values.CLUB_VALUE_RECEIPTS_TABLE]
    ))
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'club-value-policy.json'
    path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, club_value_policy):  # noqa: F811
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = ClubValueDB()
    original_cell = club_values.current._cell

    def fake_cell(column, value):
        if column in club_values.TIMESTAMP_COLUMNS or column in club_values.DATE_COLUMNS:
            return club_values._canonical_cell(column, value)
        return original_cell(column, value)

    monkeypatch.setattr(club_values.current, '_cell', fake_cell)
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = ClubValueWriter(db)
    scraper._http_client._raw_store = scraper.test_store
    return scraper


def page(club_id='10', value=150_000, current=None):
    current = current or value
    return f'''<table class="items"><thead><tr><th>#</th><th>wappen</th><th>Club</th><th>League</th><th>Value 01/01/2020</th><th>Current value</th><th>%</th></tr></thead><tbody><tr><td>1</td><td></td><td><a href="/club-{club_id}/startseite/verein/{club_id}">Club {club_id}</a></td><td>League</td><td>-</td><td>€{current / 1000:.1f}K</td><td>-</td></tr></tbody></table>
<select name="stichtag"><option value="2020-01-01" selected>01/01/2020</option><option value="2020-01-15">15/01/2020</option></select>
<script>Highcharts.Chart({{'xAxis':{{'categories':['01\\x2F01\\x2F2020','15\\x2F01\\x2F2020']}},'series':[{{'name':'Club {club_id}','data':[{{'y':0}},{{'y':{value}}}]}}]}})</script>'''


def extraction(instance, club_ids=('10',), *, value=150_000, current=None, observed=CAPTURED, extracted=EXTRACTED):
    captures = []
    url = 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1/stichtag/2020-01-01'
    raw = instance.test_store.store_attempt(
        url=url, body=page(club_ids[0], value, current).encode(), status_code=200, headers={},
        fetched_at=observed.isoformat(), cycle_id='raw-' + observed.isoformat(),
        scope_id='scope-1', endpoint='club_values', attempt=0,
    )
    capture = ClubValueCapture(
        competition_id='VIE1', source_url=url, raw_capture_id=raw.capture_id,
        source_body_hash=raw.content_hash, captured_at=raw.fetched_at,
        extracted_at=extracted.isoformat(), scope_id='scope-1', run_id='run-1',
        cycle_id='cycle-1', universe_version='registry-test',
    )
    for club_id in club_ids:
        captures.append(parse_club_values(page(club_id, value, current), capture))
    rows = tuple(row for result in captures for row in result.rows)
    first = captures[0]
    return type(first)(
        capture=first.capture, available_dates=first.available_dates,
        value_dates=first.value_dates, club_ids=tuple(club_ids), rows=rows,
        reconciled_table_points=sum(result.reconciled_table_points for result in captures),
        differing_table_points=sum(result.differing_table_points for result in captures),
        full_available_history=all(result.full_available_history for result in captures),
    )


def write(instance, result, *, at=COMMITTED):
    return club_values.write_club_values(instance, result, PREFLIGHT, committed_at=at)


def business_table(instance):
    return pd.read_sql_query('SELECT * FROM transfermarkt_club_value_observations', instance.test_db.sql)


def receipt_table(instance):
    return pd.read_sql_query('SELECT * FROM transfermarkt_native_write_manifest_v2', instance.test_db.sql)


def test_ddl_has_full_contract_and_no_deletes():
    ddl = club_values.club_value_ddl()
    assert len(ddl) == 2
    for column in CLUB_VALUE_COLUMNS:
        assert column in ddl[0]
    for column in club_values.RECEIPT_COLUMNS:
        assert column in ddl[1]
    assert 'DELETE' not in '\n'.join(ddl)


def test_writer_returns_per_club_receipts_and_exact_physical_rows(physical):
    result = extraction(physical, ('10', '20'))
    receipt = write(physical, result)
    assert receipt['verified'] and receipt['receipt_readback']
    assert len(receipt['receipts']) == 2
    assert len(business_table(physical)) == 4
    assert len(receipt_table(physical)) == 2
    for club_id in ('10', '20'):
        club_receipt = next(item for item in receipt['receipts'] if item['club_id'] == club_id)
        assert club_receipt['expected_points'] == 2
        assert club_receipt['status_counts'] == {'explicit_none': 1, 'observed_present': 1}
        assert club_receipt['differing_table_points'] == 0
        assert len(club_receipt['physical_hash']) == len(club_receipt['plan_sha256']) == 64
        assert club_receipt['committed_at'] == COMMITTED.isoformat()
    assert list(business_table(physical).columns) == list(CLUB_VALUE_COLUMNS)
    assert set(business_table(physical)['value_amount'].dropna()) == {150_000}


def test_identical_replay_is_noop(physical):
    result = extraction(physical)
    first = write(physical, result)
    repeat = write(physical, result)
    assert first['plan_sha256'] == repeat['plan_sha256']
    assert first['receipts'][0]['receipt_id'] == repeat['receipts'][0]['receipt_id']
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 1


def test_replay_rejects_deletion_of_a_later_observation_outside_the_plan(physical):
    first = write(physical, extraction(physical))
    write(physical, extraction(
        physical, value=160_000, observed=CAPTURED + timedelta(days=7),
        extracted=EXTRACTED + timedelta(days=7),
    ), at=COMMITTED + timedelta(days=7))
    physical.test_db.sql.execute(
        'DELETE FROM transfermarkt_club_value_observations WHERE value_amount = 160000')
    with pytest.raises(Exception, match='receipt read-back differs: physical_hash'):
        write(physical, extraction(physical))
    assert first['receipts'][0]['receipt_id']


def test_later_correction_appends_and_does_not_overwrite_history(physical):
    first = write(physical, extraction(physical))
    later_observed = CAPTURED + timedelta(days=7)
    later = write(physical, extraction(
        physical, value=160_000, observed=later_observed, extracted=EXTRACTED + timedelta(days=7),
    ), at=COMMITTED + timedelta(days=7))
    assert later['receipts'][0]['receipt_id'] != first['receipts'][0]['receipt_id']
    assert len(business_table(physical)) == 4
    assert len(receipt_table(physical)) == 2
    assert set(business_table(physical)['value_amount'].dropna()) == {150_000, 160_000}
    assert len({tuple(row) for row in business_table(physical)[list(CLUB_VALUE_KEY)].itertuples(index=False, name=None)}) == 4


def test_same_observation_conflict_and_stale_extraction_are_rejected(physical):
    result = extraction(physical)
    write(physical, result)
    with pytest.raises(Exception, match='immutable club-value fields differ'):
        write(physical, extraction(physical, value=160_000))
    with pytest.raises(Exception, match='stale club-value extraction'):
        write(physical, extraction(physical, extracted=EXTRACTED - timedelta(seconds=1)))


def test_value_differs_is_counted_and_both_values_are_committed(physical):
    result = extraction(physical, current=140_000)
    receipt = write(physical, result)
    assert receipt['receipts'][0]['differing_table_points'] == 1
    assert set(business_table(physical)['value_amount'].dropna()) == {140_000, 150_000}


@pytest.mark.parametrize('missing', [
    (club_values.CLUB_VALUE_TABLE,), (club_values.CLUB_VALUE_RECEIPTS_TABLE,),
    (club_values.CLUB_VALUE_TABLE, club_values.CLUB_VALUE_RECEIPTS_TABLE),
])
def test_writer_rejects_missing_policy_grants_before_sql(physical, club_value_policy, missing):
    policy = json.loads(club_value_policy.read_text())
    policy['allowed_write_tables'] = [table for table in policy['allowed_write_tables'] if table not in missing]
    club_value_policy.write_text(json.dumps(policy))
    with pytest.raises(Exception, match='authorize club-value writes'):
        write(physical, extraction(physical))
    assert physical.test_db.events == []


@pytest.mark.parametrize('mutation', ['extra', 'missing'])
def test_physical_mutation_prevents_receipt(physical, mutation):
    physical.test_db.mutation = mutation
    with pytest.raises(Exception, match='physical read-back differs|physical business/lineage mismatch'):
        write(physical, extraction(physical))
    assert len(receipt_table(physical)) == 0


def test_receipt_readback_mismatch_prevents_success(physical, monkeypatch):
    original = club_values.current.run._query_dataframe

    def query(connection, sql, params):
        result = original(connection, sql, params)
        if 'club_value_receipts' in sql:
            result['physical_hash'] = 'corrupt'
        return result

    monkeypatch.setattr(club_values.current.run, '_query_dataframe', query)
    with pytest.raises(Exception, match='receipt read-back differs'):
        write(physical, extraction(physical))
