from datetime import timedelta
import json
from pathlib import Path
import re

import pytest

from dags.utils import transfermarkt_rumours as writer
from scrapers.transfermarkt.rumour_observations import RUMOUR_COLUMNS, build_rumour_dq, digest
from tests.unit.scrapers.test_transfermarkt_rumour_observations import (
    CAPTURED, EXTRACTED, feed, player_row,
)
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)


pytestmark = pytest.mark.unit
COMMITTED = EXTRACTED + timedelta(hours=1)
BRONZE = writer.RUMOUR_TABLE.split('.')[-1]
RECEIPTS = writer.RUMOUR_RECEIPTS_TABLE.split('.')[-1]


class RumourCursor:
    """Execute the writer's INSERT-only MERGE predicates and projections in SQLite."""

    def __init__(self, db):
        self.db = db
        self.cursor = db.sql.cursor()

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, params=()):
        self.db.events.append(sql)
        sql = sql.replace(writer.RUMOUR_TABLE, BRONZE).replace(writer.RUMOUR_RECEIPTS_TABLE, RECEIPTS)
        sql = sql.removesuffix(" WITH (format = 'PARQUET')")
        sql = re.sub(r'CAST\(\? AS (timestamp\(6\)|date|bigint|double|varchar)\)', '?', sql)
        sql = sql.replace('timestamp(6)', 'text')
        if sql.startswith('MERGE'):
            match = re.fullmatch(
                r'MERGE INTO (\w+) t USING \(VALUES (.+)\) s\(([^)]+)\) ON (.+?) '
                r'WHEN NOT MATCHED THEN INSERT \(([^)]+)\) VALUES \(([^)]+)\)', sql)
            assert match is not None, sql
            table, records, source, predicate, columns, projection = match.groups()
            assert table in (BRONZE, RECEIPTS)
            sql = (f'WITH s({source}) AS (VALUES {records}) INSERT INTO {table} ({columns}) '
                   f'SELECT {projection} FROM s WHERE NOT EXISTS '
                   f'(SELECT 1 FROM {table} t WHERE {predicate})')
            self.cursor.execute(sql, params)
            if self.db.after_insert:
                self.db.after_insert(self.db.sql, table)
        else:
            self.cursor.execute(sql, params)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cursor.close()


class RumourDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.after_insert = None

    def cursor(self):
        return RumourCursor(self)


@pytest.fixture
def policy(monkeypatch, tmp_path):
    for name in ('TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID',
                 'TM_WRITE_MODE', 'TM_READER_REVISION'):
        monkeypatch.delenv(name, raising=False)
    config = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    for table in (writer.RUMOUR_TABLE, writer.RUMOUR_RECEIPTS_TABLE):
        if table not in config['allowed_write_tables']:
            config['allowed_write_tables'].append(table)
    config['expires_at'] = '2999-01-01T00:00:00Z'
    path = tmp_path / 'policy.json'
    path.write_text(json.dumps(config))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(path))
    return path


@pytest.fixture
def physical(scraper, policy, monkeypatch, tmp_path):  # noqa: F811
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    db = RumourDB()
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = MemoryWriter(db)
    return scraper


def write(instance, extraction, at=COMMITTED):
    return writer.write_rumours(instance, extraction, PREFLIGHT, committed_at=at)


def contents(instance, table):
    cursor = instance.test_db.sql.execute(f'SELECT * FROM {table} ORDER BY rowid')
    columns = [entry[0] for entry in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def test_ddl_types_and_fixed_targets():
    business, receipt = writer.rumour_ddl()
    assert business.startswith('CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_rumour_observations (')
    assert receipt.startswith('CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_rumour_receipts_v1 (')
    for definition in ('last_source_date date', 'last_reply_date date', 'observed_at timestamp(6)',
                       'extracted_at timestamp(6)', 'age bigint', 'page_number bigint',
                       'probability_percent double'):
        assert definition in business
    for column in writer.RECEIPT_COLUMNS:
        assert column in receipt
    assert 'expected_subjects bigint' in receipt and 'committed_at timestamp(6)' in receipt


def test_exact_page_payloads_and_receipts_including_empty(physical):
    result = feed()
    proof = write(physical, result)
    assert proof['verified'] and proof['receipt_readback']
    assert proof['reconciliation']['reconciliation_passed']
    assert proof['reconciliation']['receipt_coverage'] == {1: True, 2: True}
    stored, = contents(physical, BRONZE)
    expected, = result.pages[0].rows
    assert tuple(stored) == RUMOUR_COLUMNS
    assert stored == {k: v.replace('T', ' ').removesuffix('Z') if k in {
        'captured_at', 'extracted_at', 'observed_at', '_ingested_at'} else v for k, v in expected.items()}
    receipts = contents(physical, RECEIPTS)
    assert len(receipts) == 2
    for page, physical_receipt, receipt in zip(result.pages, receipts, proof['receipts']):
        assert receipt['receipt_id'] == physical_receipt['receipt_id']
        assert receipt['committed_batch'] == page.batch_id
        assert receipt['plan_sha256'] == result.plan_sha256()
        assert receipt['source_body_hash'] == page.source_body_hash
        assert receipt['raw_capture_id'] == page.raw_capture_id
        assert receipt['committed_at'] == COMMITTED.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        assert receipt['page_number'] == page.page_number
        assert receipt['expected_assertions'] == receipt['expected_subjects'] == len(page.rows)
        assert json.loads(receipt['status_counts_json']) == {page.capture.status: 1}
        assert json.loads(receipt['pagination_json'])['last_page'] == 2
    assert receipts[1]['capture_status'] == 'observed_empty'
    assert receipts[1]['physical_hash'] == digest([])
    assert all('transfer_events' not in sql and 'transfermarkt_match' not in sql
               for sql in physical.test_db.events)


def test_identical_replay_rereads_pages_and_uses_original_commit(physical):
    result = feed(second=player_row(19))
    first = write(physical, result)
    before = contents(physical, BRONZE), contents(physical, RECEIPTS)
    physical.test_db.events.clear()
    assert write(physical, result, COMMITTED + timedelta(hours=4)) == first
    assert (contents(physical, BRONZE), contents(physical, RECEIPTS)) == before
    assert not any(sql.startswith('MERGE') for sql in physical.test_db.events)
    assert sum(sql.startswith('SELECT') and writer.RUMOUR_TABLE in sql
               for sql in physical.test_db.events) == 2


def test_history_appends_at_later_clock_and_for_same_clock_correction(physical):
    original = feed()
    write(physical, original)
    row, = contents(physical, BRONZE)
    correction = feed(probability='75%')
    write(physical, correction)
    later = feed(captured_at=CAPTURED + timedelta(days=1), extracted_at=EXTRACTED + timedelta(days=1))
    write(physical, later, COMMITTED + timedelta(days=1))
    rows = contents(physical, BRONZE)
    assert len(rows) == 3 and rows[0] == row
    assert len(contents(physical, RECEIPTS)) == 6
    assert len({tuple(r[k] for k in writer.RUMOUR_KEY) for r in rows}) == 3
    assert rows[1]['observed_at'] == row['observed_at']
    assert rows[1]['rumour_identity'] == row['rumour_identity']
    assert rows[1]['correction_identity'] != row['correction_identity']
    assert rows[2]['observed_at'] != row['observed_at']
    assert rows[2]['content_hash'] == row['content_hash']
    write(physical, original)
    assert contents(physical, BRONZE) == rows


def test_same_key_source_conflict_and_stale_extraction_preserve_commits(physical):
    write(physical, feed())
    before = contents(physical, BRONZE), contents(physical, RECEIPTS)
    with pytest.raises(writer.current.CurrentWriteError, match='immutable'):
        write(physical, feed(raw_capture_id='a' * 64))
    with pytest.raises(writer.current.CurrentWriteError, match='stale'):
        write(physical, feed(extracted_at=EXTRACTED - timedelta(seconds=1)))
    assert (contents(physical, BRONZE), contents(physical, RECEIPTS)) == before


@pytest.mark.parametrize('missing', [writer.RUMOUR_TABLE, writer.RUMOUR_RECEIPTS_TABLE])
def test_both_policy_grants_required_before_sql(physical, policy, missing):
    config = json.loads(policy.read_text())
    config['allowed_write_tables'] = [t for t in config['allowed_write_tables'] if t != missing]
    policy.write_text(json.dumps(config))
    with pytest.raises(writer.current.CurrentWriteError, match='authorize rumour writes'):
        write(physical, feed())
    assert physical.test_db.events == []


@pytest.mark.parametrize('mutation', ['extra', 'missing', 'duplicate', 'payload'])
def test_post_merge_physical_corruption_blocks_receipt(physical, mutation):
    def damage(db, table):
        if table != BRONZE:
            return
        if mutation in ('extra', 'duplicate'):
            db.execute(f'INSERT INTO {table} SELECT * FROM {table} LIMIT 1')
            if mutation == 'extra':
                db.execute(f"UPDATE {table} SET subject_id='unexpected' WHERE rowid=last_insert_rowid()")
        elif mutation == 'missing':
            db.execute(f'DELETE FROM {table}')
        else:
            db.execute(f"UPDATE {table} SET player_name='incorrect'")

    physical.test_db.after_insert = damage
    with pytest.raises(writer.current.CurrentWriteError, match='unexpected|duplicate|missing|immutable'):
        write(physical, feed())
    assert contents(physical, RECEIPTS) == []


@pytest.mark.parametrize('replay', [False, True])
def test_zero_rows_are_verified_on_initial_empty_and_replay(physical, replay):
    result = feed()

    def inject(db, table):
        if table == RECEIPTS:
            db.execute(f"INSERT INTO {BRONZE} (_batch_id, subject_id) VALUES (?, 'unexpected')",
                       [result.pages[1].batch_id])

    if replay:
        write(physical, result)
        inject(physical.test_db.sql, RECEIPTS)
    else:
        physical.test_db.after_insert = inject
    with pytest.raises(writer.current.CurrentWriteError, match='unexpected'):
        write(physical, result)
    assert len(contents(physical, RECEIPTS)) == (2 if replay else 1)


@pytest.mark.parametrize('mutation', ['missing', 'hash', 'clock', 'duplicate'])
def test_receipt_readback_must_match_exactly(physical, mutation):
    def damage(db, table):
        if table != RECEIPTS:
            return
        if mutation == 'missing':
            db.execute(f'DELETE FROM {table}')
        elif mutation == 'duplicate':
            db.execute(f'INSERT INTO {table} SELECT * FROM {table}')
        elif mutation == 'clock':
            db.execute(f"UPDATE {table} SET committed_at='2026-01-01 00:00:00'")
        else:
            db.execute(f"UPDATE {table} SET physical_hash='incorrect'")

    physical.test_db.after_insert = damage
    with pytest.raises(writer.current.CurrentWriteError, match='receipt'):
        write(physical, feed())


def test_replay_detects_changed_stored_commit_clock(physical):
    result = feed()
    write(physical, result)
    physical.test_db.sql.execute(f"UPDATE {RECEIPTS} SET committed_at='2026-01-01 00:00:00'")
    with pytest.raises(writer.current.CurrentWriteError, match='receipt'):
        write(physical, result)


@pytest.mark.parametrize('mutation', ['missing', 'moved_batch', 'extra_key'])
def test_replay_requires_original_global_keys_and_complete_batch(physical, mutation):
    result = feed()
    write(physical, result)
    db = physical.test_db.sql
    if mutation == 'missing':
        db.execute(f'DELETE FROM {BRONZE}')
    elif mutation == 'moved_batch':
        db.execute(f"UPDATE {BRONZE} SET _batch_id='different'")
    else:
        db.execute(f'INSERT INTO {BRONZE} SELECT * FROM {BRONZE}')
        db.execute(f"UPDATE {BRONZE} SET subject_id='unexpected' WHERE rowid=last_insert_rowid()")
    with pytest.raises(writer.current.CurrentWriteError, match='missing|immutable|unexpected'):
        write(physical, result)


def test_final_reread_detects_disappearing_earlier_receipt(physical):
    def remove_previous(db, table):
        if table == RECEIPTS:
            db.execute(f'DELETE FROM {table} WHERE page_number=1 AND EXISTS '
                       f'(SELECT 1 FROM {table} WHERE page_number=2)')

    physical.test_db.after_insert = remove_previous
    with pytest.raises(writer.current.CurrentWriteError, match='receipt set'):
        write(physical, feed())


def test_partial_inputs_unverified_receipts_and_structural_dq(physical):
    result = feed()
    with pytest.raises(writer.current.CurrentWriteError, match='complete typed'):
        write(physical, result.pages[:1])
    with pytest.raises(writer.current.CurrentWriteError, match='structural DQ'):
        write(physical, feed(second=player_row(17)))
    assert physical.test_db.events == []
    one, two = write(physical, result)['receipts']
    for receipts in ((one,), (one, one), (one, two, two), (one, dict(two, expected_subjects=2)),
                     (one, dict(two, receipt_readback=False)), (one, dict(two, raw_capture_id='wrong'))):
        dq = build_rumour_dq(result, receipts)
        assert not dq['reconciliation_passed']
    assert build_rumour_dq(result, (one,))['receipt_coverage'] == {1: True, 2: False}
