"""Unit tests for the stage-B mono-native writer commit path (#2404)."""
import hashlib
from dataclasses import asdict
from dataclasses import replace as dataclasses_replace
from datetime import datetime, timezone
import json
import re
import shutil

import pytest

from scrapers.transfermarkt import core, native_writer, write_journal
from scrapers.transfermarkt.native_writer import NativeWritePlan, StaleTransfermarktWrite
from tests.unit.utils.test_transfermarkt_current_write import MemoryCursor, MemoryDB
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoAmbiguousWriteError

pytestmark = pytest.mark.unit

RECEIPTS = 'transfermarkt_core_receipts_v1'
UTC = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
# Plan rows state a clock in its one canonical spelling (UTC, microseconds).
CLOCK = UTC.isoformat(timespec='microseconds')
LATER = datetime(2026, 11, 21, 13, 0, 0, tzinfo=timezone.utc).isoformat(timespec='microseconds')
EARLIER = datetime(2026, 11, 21, 11, 0, 0, tzinfo=timezone.utc).isoformat(timespec='microseconds')

# A plan carries its entity's registered DDL and exactly its column set.
MEMBERSHIPS_DDL = core.core_ddl('squad_memberships')
MV_DDL = core.core_ddl('market_value_points')

_SQUAD_URL = 'https://www.transfermarkt.com/x/kader/verein/11/saison_id/2025/plus/1'
_MV_URL = 'https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/1'


def _placeholder(column):
    """A value of the declared type for a business column the test does not name."""
    kind = core.COLUMN_TYPES[column]
    if kind == 'bigint':
        return 1
    if kind == 'boolean':
        return False
    if kind == 'date':
        return None
    return 'x'


def _contract_row(entity, endpoint, url, *, capture, clock, run, ingested, **business):
    """A row shaped exactly as the entity's contract says, like core._render.

    The lineage is the capture's and the run's: the writer refuses a row
    that disagrees with either.
    """
    values = {column: _placeholder(column) for column in core._BUSINESS_COLUMNS[entity]}
    values.update(business)
    row = {column: values[column] for column in core._BUSINESS_COLUMNS[entity]}
    row.update(
        source_url=url, source_endpoint=endpoint, raw_capture_id=capture,
        source_capture_id=capture, source_body_hash='b' * 64,
        scope_id='tm-scope', cycle_id='tm-cycle', run_id=run, universe_version='u',
        captured_at=clock, observed_at=clock, extracted_at=ingested,
        parser_revision='core-v1', schema_revision='1',
        observation_status='observed_present', status_reason='squad_page',
        _source='transfermarkt', _entity_type=entity,
        _batch_id=native_writer._batch_id_for_run(run), _ingested_at=ingested,
    )
    return row


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)


_COLUMN_PROBE = re.compile(
    r'^\s*SELECT \* FROM (?:iceberg\.bronze\.)?(transfermarkt_\w+) WHERE 1 = 0\s*$', re.I)


class CoreCursor(MemoryCursor):
    def execute(self, sql, params=()):
        if 'FROM system.runtime.queries' in sql:
            sql = sql.replace('system.runtime.queries', 'test_query_history')
        super().execute(sql, params)
        # sqlite reports no column types: the schema preflight's probe gets the
        # declared ones, as Trino reports them.
        probe = _COLUMN_PROBE.match(sql)
        if probe and self.description:
            declared = {row[1]: (row[2] or '').lower() for row in self.db.sql.execute(
                f'PRAGMA table_info({probe[1]})')}
            self.description = [(column[0], declared.get(column[0])) for column in self.description]
        return self.description


class CoreDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.sql.execute('CREATE TABLE test_query_history (query_id TEXT, state TEXT, error_code TEXT)')

    def rejected(self, cursor, client_error):
        # The client lost its response. A separate coordinator read can prove
        # that this particular simulated submission was rejected before execution.
        row = self.sql.execute("INSERT INTO test_query_history (state, error_code) "
                               "VALUES ('FAILED', 'QUERY_QUEUE_FULL')").lastrowid
        cursor.query_id = f'query-{row}'
        self.sql.execute('UPDATE test_query_history SET query_id = ? WHERE rowid = ?',
                         (cursor.query_id, row))
        return client_error

    def cursor(self):
        return CoreCursor(self)


def _rejected_target(db, error):
    class RejectedCursor(CoreCursor):
        def execute(self, sql, params=()):
            raise db.rejected(self, error)

    with native_writer._target_dispatch() as statement:
        native_writer._execute_once(RejectedCursor(db), 'MERGE INTO target', None, statement)


class CoreWriter(IcebergWriter):
    """Store canonical writer frames in sqlite without donor cell rewriting."""

    def __init__(self, db):
        super().__init__()
        self.db = db
        self.writes = []
        self.replacements = []  # (table, delete_filter) of every write

    def _write_to_iceberg(self, df, database, table, partition_spec, mode='append',
                          delete_filter=None, merge_keys=None, bulk_arrow=False):
        df = self._pandas_to_arrow(df).to_pandas()
        self.writes.append((table, len(df), tuple(merge_keys or ())))
        self.replacements.append((table, delete_filter))
        cur = self.db.sql.cursor()
        columns = list(df.columns)
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(col + " TEXT" for col in columns)})')
        existing = {row[1] for row in cur.execute(f'PRAGMA table_info({table})')}
        for column in set(columns) - existing:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN {column} TEXT')
        staged = list(df.itertuples(index=False, name=None))
        if delete_filter:
            # Like the Transfermarkt writer's single-statement replacement:
            # the scope's live rows leave only once the new rows are staged.
            cur.execute(f'DELETE FROM {table} WHERE {delete_filter}')
        marks = ', '.join('?' for _ in columns)
        for row in staged:
            if merge_keys:
                idx = [columns.index(key) for key in merge_keys]
                cur.execute('DELETE FROM ' + table + ' WHERE ' + ' AND '.join(key + ' = ?' for key in merge_keys),
                            tuple(row[i] for i in idx))
            cur.execute(f'INSERT INTO {table} ({", ".join(columns)}) VALUES ({marks})', tuple(row))
        return 'iceberg.' + database + '.' + table


def membership_plan(players, *, clock=CLOCK, capture='cap-1', mode='replace',
                    status='observed_present', proof='', run='run-1', ingested=None):
    rows = tuple(_contract_row(
        'squad_memberships', 'squad', _SQUAD_URL, capture=capture, clock=clock, run=run,
        ingested=ingested or clock, league='GB1', season='2526', club_id='11',
        player_id=pid, player_name='Player ' + pid) for pid in players)
    return NativeWritePlan(
        entity='squad_memberships', table='transfermarkt_squad_memberships',
        ddl=MEMBERSHIPS_DDL, natural_keys=('league', 'season', 'club_id', 'player_id'),
        scope_columns=('league', 'season', 'club_id'), scope_values=('GB1', '2526', '11'),
        rows=tuple(rows), captured_at=clock, write_mode=mode,
        status=status, status_reason='squad_page' if status == 'observed_present'
        else 'empty_roster', proof=proof, capture_id=capture,
        evidence=(
            ('source_url', _SQUAD_URL),
            ('source_endpoint', 'squad'), ('raw_capture_id', capture),
            ('source_capture_id', capture), ('source_body_hash', 'b' * 64),
            ('scope_id', 'tm-scope'), ('cycle_id', 'tm-cycle'),
            ('parser_revision', 'core-v1'), ('schema_revision', '1'),
        ) if not rows else (),
    )


def mv_plan(points, *, clock=CLOCK, capture='cap-mv'):
    rows = tuple(_contract_row(
        'market_value_points', 'market_value_points', _MV_URL, capture=capture, clock=clock,
        run='run-1', ingested=clock, player_id='1', mv_date=day, value_eur=value)
        for day, value in points)
    return NativeWritePlan(
        entity='market_value_points', table='transfermarkt_market_value_points',
        ddl=MV_DDL, natural_keys=('player_id', 'mv_date'),
        scope_columns=('player_id',), scope_values=('1',),
        rows=tuple(rows), captured_at=clock, write_mode='merge',
        status_reason='career_page', capture_id=capture,
    )


def write(db, plans, *, run_id='run-1', clock=CLOCK):
    return native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id=run_id,
        universe_version='core-universe-v1', committed_at=clock,
    )


# ---------------------------------------------------------------- journal


def test_intent_lifecycle_uses_the_index_instead_of_a_directory_scan():
    path = write_journal.save_intent({'run_id': 'r1'}, {'plans': []}, cycle='c1')
    matches = write_journal.pending_intents({'run_id': 'r1'})
    assert [item[0] for item in matches] == [path]
    assert matches[0][1]['payload'] == {'plans': []}
    assert write_journal.pending_intents({'run_id': 'other'}) == []
    write_journal.finish_intent(path)
    assert write_journal.pending_intents({'run_id': 'r1'}) == []
    assert not path.exists()


def test_index_loss_adopts_only_checksum_verified_orphans():
    path = write_journal.save_intent({'run_id': 'r1'}, {'plans': [1]})
    index = write_journal._index_path()
    index.unlink()  # crash between the file write and the index append
    matches = write_journal.pending_intents({'run_id': 'r1'})
    assert [item[0] for item in matches] == [path]
    path.write_text(path.read_text() + ' ')
    with pytest.raises(RuntimeError, match='orphan checksum'):
        write_journal.pending_intents({'run_id': 'r1'})


def test_finished_entry_with_leftover_file_never_resurrects():
    path = write_journal.save_intent({'run_id': 'r1'}, {'plans': []})
    body = path.read_bytes()
    write_journal.finish_intent(path)
    path.write_bytes(body)  # crash between the index append and the unlink
    assert write_journal.pending_intents({'run_id': 'r1'}) == []


def test_torn_index_tail_is_discarded_and_appending_heals():
    write_journal._index_path().parent.mkdir(parents=True)
    write_journal._index_path().write_text('{"digest": "broken')  # torn line, no newline
    path = write_journal.save_intent({'run_id': 'r1'}, {'plans': []})
    assert [item[0] for item in write_journal.pending_intents({'run_id': 'r1'})] == [path]


def test_snapshot_anchors_are_immutable_and_reject_table_drift(tmp_path):
    class AnchorCursor:
        def __init__(self, rows):
            self.rows = rows
            self.description = None
        def execute(self, sql, params=None):
            self.last = sql
        def fetchall(self):
            if 'transfermarkt_squad_memberships$snapshots' in self.last:
                return self.rows
            raise RuntimeError('TABLE_NOT_FOUND: missing')
        def close(self):
            pass
    class AnchorConnection:
        def __init__(self, rows):
            self.rows = rows
        def cursor(self):
            return AnchorCursor(self.rows)

    connection = AnchorConnection([(77,)])
    anchors = write_journal.snapshot_anchors('a' * 64, connection, {'transfermarkt_squad_memberships'})
    assert anchors == {'transfermarkt_squad_memberships': 77}
    # A missing table anchors as None; re-read is stable.
    assert write_journal.snapshot_anchors('b' * 64, AnchorConnection([]), {'transfermarkt_market_value_points'}) == {
        'transfermarkt_market_value_points': None}
    assert write_journal.read_snapshot_anchors('a' * 64) == anchors
    with pytest.raises(RuntimeError, match='anchor tables differ'):
        write_journal.snapshot_anchors('a' * 64, connection, {'transfermarkt_squad_memberships', 'transfermarkt_transfer_events'})


def test_receipt_requires_verified_flag_and_round_trips():
    with pytest.raises(RuntimeError, match='unverified'):
        write_journal.record_receipt('c' * 64, {'verified': False})
    write_journal.record_receipt('c' * 64, {'verified': True, 'plan_sha256': 'x'})
    assert write_journal.read_receipt('c' * 64)['plan_sha256'] == 'x'
    with pytest.raises(RuntimeError, match='differs'):
        write_journal.record_receipt('c' * 64, {'verified': True, 'plan_sha256': 'y'})


# ---------------------------------------------------------------- plans


def test_plan_validation_rejects_broken_contracts():
    plan = membership_plan(['1'], capture='c')
    base = dict(
        entity=plan.entity, table=plan.table, ddl=plan.ddl, natural_keys=plan.natural_keys,
        scope_columns=plan.scope_columns, scope_values=plan.scope_values, rows=plan.rows,
        captured_at=plan.captured_at, status_reason='squad_page', capture_id='c',
        evidence=plan.evidence)
    NativeWritePlan(**base)
    with pytest.raises(ValueError, match='table name'):
        NativeWritePlan(**{**base, 'table': 'transfermarkt_squad_memberships; DROP TABLE x'})
    with pytest.raises(ValueError, match='write_mode'):
        NativeWritePlan(**{**base, 'write_mode': 'upsert'})
    with pytest.raises(ValueError, match='natural_keys'):
        NativeWritePlan(**{**base, 'natural_keys': ()})
    with pytest.raises(ValueError, match='outside its replacement scope'):
        NativeWritePlan(**{**base, 'scope_values': ('GB1', '2526', '12')})
    with pytest.raises(ValueError, match='newest row capture'):
        NativeWritePlan(**{**base, 'captured_at': EARLIER})
    with pytest.raises(ValueError, match='duplicate core natural key'):
        NativeWritePlan(**{**base, 'rows': base['rows'] + base['rows']})
    # A roster lane holds exactly one capture, and is never written as a merge:
    # only the attendance edition snapshot spans pages (review r2 finding 3).
    with pytest.raises(ValueError, match='share one source_capture_id'):
        NativeWritePlan(**{**base, 'rows': (
            base['rows'][0],
            {**base['rows'][0], 'player_id': '2',
             'source_capture_id': 'other', 'raw_capture_id': 'other'})})
    with pytest.raises(ValueError, match='page evidence belongs to a multi-page'):
        NativeWritePlan(**{**base, 'pages': (('u', 'c', CLOCK, 'h' * 64, ''),)})
    with pytest.raises(ValueError, match='entity contract'):
        NativeWritePlan(**{**base, 'write_mode': 'merge'})


def test_merge_and_replace_readbacks_use_different_predicates():
    replace = membership_plan(['1'])
    assert replace.readback_sql() == ('league = ? AND season = ? AND club_id = ?', ('GB1', '2526', '11'))
    merge = mv_plan([('2025-01-01', 10), ('2025-02-01', 20)])
    predicate, params = merge.readback_sql()
    # Every bound parameter stays varchar: date/timestamp key columns never
    # receive string casts against typed columns.
    assert predicate == 'player_id = ? AND source_capture_id = ?'
    assert params == ('1', 'cap-mv')


# ---------------------------------------------------------------- guard


class GuardCursor:
    def __init__(self, rows=None, pages=None, fail=False, manifest=None):
        self.rows = rows
        self.pages = pages or []
        # Accepted page evidence of the scope, (url, capture id, clock, hash):
        # one acknowledged walk, logged under its receipt.
        self.manifest = manifest or []
        self.fail = fail
        self.description = None
    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError('TABLE_NOT_FOUND: nope')
        self.sql = sql
    def fetchall(self):
        if 'transfermarkt_core_pages_v1' in self.sql:
            return [('receipt-walk', '', '', number, len(self.manifest), url, capture_id, clock,
                     body_hash, 0, '', CLOCK)
                    for number, (url, capture_id, clock, body_hash)
                    in enumerate(self.manifest, start=1)]
        if 'SELECT receipt_id FROM' in self.sql:
            return [('receipt-walk',)] if self.manifest else []
        if 'SELECT receipt_id, committed_at, source_capture_id' in self.sql:
            if not self.manifest:
                return []
            _, capture, clock, body = self.manifest[-1]
            return [('receipt-walk', CLOCK, capture, body, clock)]
        return self.pages if 'GROUP BY source_url' in self.sql else self.rows
    def close(self):
        pass


def test_guard_refuses_older_bundle_and_accepts_equal_or_missing():
    plan = membership_plan(['1'])
    for committed, refused in ((LATER, True), (CLOCK, False), (None, False)):
        cursor = GuardCursor(rows=[(committed,)])
        connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
        if refused:
            with pytest.raises(StaleTransfermarktWrite):
                native_writer.guard_native_bundle(connection, plan)
        else:
            native_writer.guard_native_bundle(connection, plan)
    missing = type('C', (), {'cursor': lambda self: GuardCursor(fail=True)})()
    native_writer.guard_native_bundle(missing, plan)  # table absent: nothing to guard


# ---------------------------------------------------------------- stage-C review (#2405)

SIBLING_CLOCK = datetime(2026, 11, 21, 12, 30, 0, tzinfo=timezone.utc).isoformat(timespec='microseconds')
NEWEST = datetime(2026, 11, 21, 14, 0, 0, tzinfo=timezone.utc).isoformat(timespec='microseconds')


def _snapshot(first_clock, first_capture):
    """An attendance edition snapshot of two pages captured at different clocks."""
    from tests.unit.scrapers.test_transfermarkt_sections import (
        ATTENDANCE_FIXTURES, ATTENDANCE_PAGE_2, ATTENDANCE_START, EXTRACTED_AT, _attendance_plan,
        _capture,
    )
    def page(url, fixture, clock, name):
        # A later capture of the same page has its own 64-hex capture identity.
        return _capture(url, (ATTENDANCE_FIXTURES / fixture).read_bytes(), clock,
                        capture_id=hashlib.sha256(name.encode()).hexdigest())
    page_1 = page(ATTENDANCE_START, 'page_1.html', first_clock, first_capture)
    page_2 = page(ATTENDANCE_PAGE_2, 'page_2.html', SIBLING_CLOCK, 'cap-2')
    return _attendance_plan((page_1, page_2), EXTRACTED_AT)


def test_guard_refuses_a_stale_page_hidden_behind_a_fresh_sibling_page():
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2, ATTENDANCE_START
    plan = _snapshot(EARLIER, 'cap-old')
    cursor = GuardCursor(rows=[(SIBLING_CLOCK,)],
                         pages=[(ATTENDANCE_START, CLOCK), (ATTENDANCE_PAGE_2, SIBLING_CLOCK)])
    connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
    # The scope maximum equals the plan's own clock: only the per-page
    # comparison sees that page 1 would regress a fresher committed page.
    with pytest.raises(StaleTransfermarktWrite):
        native_writer.guard_native_bundle(connection, plan)


def test_guard_accepts_equal_newer_and_unrelated_pages_of_a_snapshot():
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2, ATTENDANCE_START
    # A committed page the snapshot no longer contains is replaced by design.
    cursor = GuardCursor(
        rows=[(SIBLING_CLOCK,)],
        pages=[(ATTENDANCE_START, CLOCK), (ATTENDANCE_PAGE_2, SIBLING_CLOCK),
               (ATTENDANCE_START + '?page=3', SIBLING_CLOCK)])
    connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
    native_writer.guard_native_bundle(connection, _snapshot(CLOCK, 'cap-1'))
    # A newer page 1 beside the unchanged page 2 is a legitimate refresh.
    native_writer.guard_native_bundle(connection, _snapshot(NEWEST, 'cap-new'))


def test_guard_counts_a_page_that_contributed_no_row():
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2, ATTENDANCE_START
    plan = _snapshot(NEWEST, 'cap-new')
    # Page 1 of the new walk is an older capture that rendered no row.
    rowless_older = dataclasses_replace(
        plan, rows=tuple(row for row in plan.rows if row['source_url'] == ATTENDANCE_PAGE_2),
        captured_at=SIBLING_CLOCK, capture_id='', evidence=(),
        pages=((ATTENDANCE_START, 'cap-rowless', EARLIER, hashlib.sha256(b'rowless').hexdigest(),
                ATTENDANCE_PAGE_2),
               (ATTENDANCE_PAGE_2, hashlib.sha256(b'cap-2').hexdigest(), SIBLING_CLOCK,
                plan.rows[1]['source_body_hash'], '')))
    cursor = GuardCursor(rows=[(SIBLING_CLOCK,)],
                         pages=[(ATTENDANCE_START, CLOCK), (ATTENDANCE_PAGE_2, SIBLING_CLOCK)])
    connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
    # The rows alone are fresh; the walked page 1 evidence is not.
    with pytest.raises(StaleTransfermarktWrite):
        native_writer.guard_native_bundle(connection, rowless_older)


def test_guard_keeps_scope_freshness_for_clockless_merge_lanes():
    # market_value_points keys carry no capture clock: an older capture could
    # overwrite fresher values and stays refused by the scope maximum.
    plan = mv_plan([('2025-01-01', 10)], clock=EARLIER, capture='cap-old')
    cursor = GuardCursor(rows=[(LATER,)])
    connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
    with pytest.raises(StaleTransfermarktWrite):
        native_writer.guard_native_bundle(connection, plan)


# ---------------------------------------------------------------- bundles


def test_replace_bundle_removes_departed_players_and_receipts_each_plan():
    db = CoreDB()
    first = write(db, [membership_plan(['1', '2'], capture='cap-1')])
    assert first['verified'] is True and first['receipt_readback'] is True
    assert [proof['rows'] for proof in first['plans']] == [2]
    second = write(db, [membership_plan(['1'], capture='cap-2', clock=LATER)],
                   run_id='run-2', clock=LATER)
    rows = db.sql.execute('SELECT player_id FROM transfermarkt_squad_memberships ORDER BY player_id').fetchall()
    assert rows == [('1',)]
    assert [proof['rows'] for proof in second['plans']] == [1]
    receipts = db.sql.execute(f'SELECT row_count, expected_rows FROM {RECEIPTS} ORDER BY rowid').fetchall()
    assert receipts == [(2, 2), (1, 1)]


def test_merge_bundle_keeps_earlier_rows_and_updates_same_natural_key():
    db = CoreDB()
    write(db, [mv_plan([('2025-01-01', 10), ('2025-02-01', 20)], capture='cap-mv')])
    second = write(db, [mv_plan([('2025-01-01', 11), ('2025-03-01', 30)], capture='cap-mv2', clock=LATER)],
                   run_id='run-2', clock=LATER)
    rows = dict(db.sql.execute('SELECT mv_date, value_eur FROM transfermarkt_market_value_points').fetchall())
    assert rows == {'2025-01-01': 11, '2025-02-01': 20, '2025-03-01': 30}
    assert second['plans'][0]['rows'] == 2  # the plan's own rows only


def test_bundle_replay_is_idempotent_through_the_journal_receipt():
    db = CoreDB()
    writer = CoreWriter(db)
    plans = [membership_plan(['1'], capture='cap-1')]
    first = native_writer.write_core_bundle(
        lambda: db, writer, plans, run_id='r', universe_version='u', committed_at=CLOCK)
    writes_before = len(writer.writes)
    again = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)
    assert again == first and len(writer.writes) == writes_before
    count = db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0]
    assert count == 1


def test_replay_with_drifted_committed_rows_fails_closed():
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-1')])
    db.sql.execute("UPDATE transfermarkt_squad_memberships SET player_name = 'mutated'")
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [membership_plan(['1'], capture='cap-1')])


def test_empty_replace_bundle_deletes_the_scope_and_receipts_zero_rows():
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-1')])
    empty = NativeWritePlan(
        entity='squad_memberships', table='transfermarkt_squad_memberships',
        ddl=MEMBERSHIPS_DDL, natural_keys=('league', 'season', 'club_id', 'player_id'),
        scope_columns=('league', 'season', 'club_id'), scope_values=('GB1', '2526', '11'),
        rows=(), captured_at=LATER, status='observed_empty', status_reason='empty_roster',
        proof='empty_roster:no_data_rows', capture_id='cap-empty',
        evidence=(
            ('source_url', _SQUAD_URL), ('source_endpoint', 'squad'),
            ('raw_capture_id', 'cap-empty'), ('source_capture_id', 'cap-empty'),
            ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
            ('parser_revision', 'p'), ('schema_revision', 's'),
        ),
    )
    receipt = write(db, [empty], run_id='run-2', clock=LATER)
    assert receipt['plans'][0]['rows'] == 0
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 0


def test_stale_bundle_refuses_before_mutation_and_leaves_the_intent_pending():
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-new', clock=LATER)], run_id='run-2', clock=LATER)
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [membership_plan(['1', '9'], capture='cap-old', clock=EARLIER)],
              run_id='run-3', clock=EARLIER)
    pending = write_journal.pending_intents({'run_id': 'run-3'})
    assert len(pending) == 1 and 'cap-old' in json.dumps(pending[0][1])
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 1


def test_physical_readback_mismatch_fails_the_bundle():
    db = CoreDB()
    writer = CoreWriter(db)

    original = writer._write_to_iceberg

    def drop_one(df, *args, **kwargs):
        original(df.iloc[1:], *args, **kwargs)

    writer._write_to_iceberg = drop_one
    with pytest.raises(RuntimeError, match='physical readback'):
        native_writer.write_core_bundle(
            lambda: db, writer, [membership_plan(['1', '2'], capture='cap-1')],
            run_id='r', universe_version='u', committed_at=CLOCK)
    pending = write_journal.pending_intents({'run_id': 'r'})
    assert len(pending) == 1


# ---------------------------------------------------------------- review round-1 regressions


def test_query_rows_fetches_when_the_cursor_execute_returns_the_cursor():
    class SelfReturningCursor:
        description = [('n',)]
        def __init__(self):
            self.fetched = False
        def execute(self, sql, params=None):
            return self
        def fetchall(self):
            self.fetched = True
            return [(4,)]
    cursor = SelfReturningCursor()
    assert native_writer._query_rows(cursor, 'SELECT n FROM t') == [(4,)]
    assert cursor.fetched


def test_empty_replacement_keeps_the_fresher_capture_clock_via_receipts():
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-new', clock=LATER)], run_id='run-2', clock=LATER)
    db.sql.execute('DELETE FROM transfermarkt_squad_memberships')  # proven empty scope
    older = membership_plan(['1', '9'], capture='cap-old', clock=EARLIER)
    with pytest.raises(StaleTransfermarktWrite):
        native_writer.guard_native_bundle(db, older)


def test_reobservation_of_the_same_capture_refreshes_run_lineage():
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-1')], run_id='run-1')
    second = write(db, [membership_plan(['1'], capture='cap-1', run='run-2')], run_id='run-2')
    assert second['verified'] is True
    rows = db.sql.execute('SELECT run_id, _batch_id FROM transfermarkt_squad_memberships').fetchall()
    assert len(rows) == 1 and rows[0][0] == 'run-2'


def test_bundle_rejects_two_replacements_of_one_scope():
    plans = [membership_plan(['1']), membership_plan(['2'])]
    with pytest.raises(ValueError, match='duplicate core replacement scope'):
        native_writer.write_core_bundle(
            lambda: CoreDB(), CoreWriter(CoreDB()), plans,
            run_id='r', universe_version='u', committed_at=CLOCK)


def test_receipt_batch_names_the_same_batch_as_the_rows():
    db = CoreDB()
    receipt = write(db, [membership_plan(['1'], capture='cap-1', run='run-9')], run_id='run-9')
    (row_batch,) = db.sql.execute('SELECT _batch_id FROM transfermarkt_squad_memberships').fetchone()
    assert receipt['committed_batch'] == row_batch
    stored = db.sql.execute('SELECT committed_batch FROM transfermarkt_core_receipts_v1').fetchall()
    assert stored == [(row_batch,)]


def test_pending_intent_replays_after_a_crash_without_another_request():
    db = CoreDB()
    writer = CoreWriter(db)
    plans = [membership_plan(['1'], capture='cap-1')]
    broken = CoreWriter(db)
    broken.dispatches_journalled_target = True
    broken._write_to_iceberg = lambda *a, **k: _rejected_target(db, RuntimeError('crash mid-write'))
    with pytest.raises(RuntimeError, match='crash mid-write'):
        native_writer.write_core_bundle(
            lambda: db, broken, plans, run_id='r', universe_version='u', committed_at=CLOCK)
    assert write_journal.pending_intents({'run_id': 'r'})
    receipts = native_writer.replay_pending_intents(lambda: db, writer)
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert write_journal.pending_intents({'run_id': 'r'}) == []
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 1


def test_acknowledgement_rewrites_a_deleted_warehouse_receipt_row():
    db = CoreDB()
    plans = [membership_plan(['1'], capture='cap-1')]
    first = write(db, plans)
    db.sql.execute('DELETE FROM transfermarkt_core_receipts_v1')
    again = write(db, plans)
    assert again == first
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 1


def test_acknowledgement_finishes_a_leftover_intent(monkeypatch):
    db = CoreDB()
    plans = [membership_plan(['1'], capture='cap-1')]
    crashed = {"once": True}

    def crash_after_receipt(path):
        if crashed["once"]:
            crashed["once"] = False
            raise RuntimeError('crash between record_receipt and finish_intent')

    original = write_journal.finish_intent
    monkeypatch.setattr(write_journal, 'finish_intent', crash_after_receipt)
    with pytest.raises(RuntimeError, match='crash between'):
        write(db, plans)
    monkeypatch.setattr(write_journal, 'finish_intent', original)
    assert write_journal.pending_intents({'run_id': 'run-1'})
    again = write(db, plans)
    assert again['verified'] is True
    assert write_journal.pending_intents({'run_id': 'run-1'}) == []


# ---------------------------------------------------------------- review round-2 regressions


def test_reobservation_with_a_later_extraction_time_refreshes_lineage():
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-1', run='run-1')], run_id='run-1')
    # Same capture bytes, later extraction: only the run lineage may differ.
    second = write(db, [membership_plan(['1'], capture='cap-1', run='run-2', ingested=LATER)],
                   run_id='run-2', clock=LATER)
    assert second['verified'] is True
    rows = db.sql.execute(
        'SELECT run_id, _ingested_at FROM transfermarkt_squad_memberships').fetchall()
    assert len(rows) == 1 and rows[0][0] == 'run-2' and rows[0][1] == LATER


def test_empty_merge_plan_after_populated_scope_keeps_earlier_rows():
    db = CoreDB()
    write(db, [mv_plan([('2025-01-01', 10)], capture='cap-mv')])
    empty = NativeWritePlan(
        entity='market_value_points', table='transfermarkt_market_value_points',
        ddl=MV_DDL, natural_keys=('player_id', 'mv_date'),
        scope_columns=('player_id',), scope_values=('1',),
        rows=(), captured_at=LATER, write_mode='merge',
        status='observed_empty', status_reason='empty_collection',
        proof='empty_collection:market_value_points', capture_id='cap-mv2',
        evidence=(
            ('source_url', _MV_URL), ('source_endpoint', 'market_value_points'),
            ('raw_capture_id', 'cap-mv2'), ('source_capture_id', 'cap-mv2'),
            ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
            ('parser_revision', 'p'), ('schema_revision', 's'),
        ),
    )
    receipt = write(db, [empty], run_id='run-2', clock=LATER)
    assert receipt['plans'][0]['rows'] == 0
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_market_value_points').fetchone()[0] == 1


def test_empty_plan_receipt_carries_full_capture_evidence():
    db = CoreDB()
    empty = NativeWritePlan(
        entity='squad_memberships', table='transfermarkt_squad_memberships',
        ddl=MEMBERSHIPS_DDL, natural_keys=('league', 'season', 'club_id', 'player_id'),
        scope_columns=('league', 'season', 'club_id'), scope_values=('GB1', '2526', '11'),
        rows=(), captured_at=CLOCK, status='observed_empty', status_reason='empty_roster',
        proof='empty_roster:no_data_rows', capture_id='cap-empty',
        evidence=(
            ('source_url', 'https://www.transfermarkt.com/x/kader/verein/11/saison_id/2025/plus/1'),
            ('source_endpoint', 'squad'), ('raw_capture_id', 'cap-empty'),
            ('source_capture_id', 'cap-empty'), ('source_body_hash', 'h' * 64),
            ('scope_id', 'tm-scope'), ('cycle_id', 'tm-cycle'),
            ('parser_revision', 'core-v1'), ('schema_revision', '1'),
        ),
    )
    write(db, [empty])
    row = db.sql.execute(
        'SELECT source_capture_id, source_body_hash, observation_status, status_proof '
        'FROM transfermarkt_core_receipts_v1').fetchone()
    assert row == ('cap-empty', 'h' * 64, 'observed_empty', 'empty_roster:no_data_rows')


# ---------------------------------------------------------------- review round-3 regressions


def test_populated_plan_derives_its_capture_id_from_the_rows():
    plan = membership_plan(['1'], capture='cap-1')
    rebuilt = NativeWritePlan(
        entity=plan.entity, table=plan.table, ddl=plan.ddl,
        natural_keys=plan.natural_keys, scope_columns=plan.scope_columns,
        scope_values=plan.scope_values, rows=plan.rows, captured_at=plan.captured_at,
        status_reason=plan.status_reason,
    )
    assert rebuilt.capture_id == 'cap-1'


def test_plan_evidence_must_name_the_same_capture_and_carry_real_values():
    def empty_plan(**overrides):
        values = dict(
            entity='squad_memberships', table='transfermarkt_squad_memberships',
            ddl=MEMBERSHIPS_DDL, natural_keys=('league', 'season', 'club_id', 'player_id'),
            scope_columns=('league', 'season', 'club_id'), scope_values=('GB1', '2526', '11'),
            rows=(), captured_at=CLOCK, status='observed_empty', status_reason='empty_roster',
            proof='p', capture_id='cap-B',
            evidence=(
                ('source_url', _SQUAD_URL), ('source_endpoint', 'squad'),
                ('raw_capture_id', 'cap-B'), ('source_capture_id', 'cap-B'),
                ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
                ('parser_revision', 'p'), ('schema_revision', 's'),
            ),
        )
        values.update(overrides)
        return NativeWritePlan(**values)

    empty_plan()  # a consistent plan validates
    mismatched = (
        ('raw_capture_id', 'cap-A'), ('source_capture_id', 'cap-B'),
        ('source_url', _SQUAD_URL), ('source_endpoint', 'squad'),
        ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
        ('parser_revision', 'p'), ('schema_revision', 's'),
    )
    with pytest.raises(ValueError, match='different capture'):
        empty_plan(evidence=mismatched)
    valid = empty_plan()
    hollow_evidence = tuple(
        (column, '' if column == 'source_body_hash' else value)
        for column, value in valid.evidence)
    with pytest.raises(ValueError, match='non-empty'):
        empty_plan(evidence=hollow_evidence)


def test_same_capture_merge_keeps_its_exact_rule_outside_multi_capture_lanes():
    # Round-2 self-review: only pages of one unit re-own rows (finding 7); a
    # capture of any other merge lane is still exactly its committed rows.
    db = CoreDB()
    write(db, [mv_plan([('2025-01-01', 10)], capture='cap-mv')])
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [mv_plan([('2025-01-01', 10), ('2025-02-01', 20)], capture='cap-mv')],
              run_id='run-2', clock=LATER)


# ---------------------------------------------------------------- stage-C review round 3
def _empty_roster_plan(clock=CLOCK, capture='cap-empty'):
    return NativeWritePlan(
        entity='squad_memberships', table='transfermarkt_squad_memberships',
        ddl=MEMBERSHIPS_DDL, natural_keys=('league', 'season', 'club_id', 'player_id'),
        scope_columns=('league', 'season', 'club_id'), scope_values=('GB1', '2526', '11'),
        rows=(), captured_at=clock, status='observed_empty', status_reason='empty_roster',
        proof='empty_roster:no_data_rows', capture_id=capture,
        evidence=(
            ('source_url', _SQUAD_URL), ('source_endpoint', 'squad'),
            ('raw_capture_id', capture), ('source_capture_id', capture),
            ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
            ('parser_revision', 'p'), ('schema_revision', 's'),
        ),
    )


def test_schema_preflight_also_guards_a_rowless_replacement():
    # Round 3 finding 9: a proven-empty replacement deletes the scope, so the
    # table must be this plan's before that delete runs.
    db = CoreDB()
    db.sql.execute('CREATE TABLE transfermarkt_squad_memberships (league TEXT, season TEXT, '
                   'club_id TEXT, donor_extra TEXT)')
    db.sql.execute("INSERT INTO transfermarkt_squad_memberships VALUES ('GB1', '2526', '11', 'x')")
    with pytest.raises(RuntimeError, match='schema differs'):
        write(db, [_empty_roster_plan()], run_id='run-2')
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 1


def _stage_b_identity(plan, run_id):
    """The bundle identity of the stage-B writer (#2404), before page evidence
    and rowless capture evidence entered the plan digest."""
    plan_hash = native_writer._digest({
        'entity': plan.entity, 'table': plan.table, 'natural_keys': plan.natural_keys,
        'scope': list(zip(plan.scope_columns, plan.scope_values)),
        'write_mode': plan.write_mode, 'captured_at': plan.captured_at,
        'status': plan.status, 'status_reason': plan.status_reason, 'proof': plan.proof,
        'rows': [],
    })
    return plan_hash, native_writer._digest({'plans': [plan_hash], 'run_id': run_id})


def _legacy_intent(plan, digest):
    """Save a plan the way stage B journalled it: no page evidence field."""
    descriptor = asdict(plan)
    del descriptor['pages']
    descriptor['rows'] = native_writer._sorted_rows(plan.rows, plan.natural_keys)
    return write_journal.save_intent(
        {'run_id': 'run-old', 'digest': digest}, {'plans': [descriptor]},
        run_id='run-old', universe_version='u', committed_at=CLOCK)


def test_plan_rejects_a_column_of_mixed_value_types_before_mutation():
    # Round-3 finding 4, same class: Arrow refuses a mixed column only inside
    # the insertion, after a replacement has deleted the scope.
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-1')])
    plan = membership_plan(['1', '2'], capture='cap-2', clock=LATER)
    mixed = list(plan.rows)
    mixed[1] = {**mixed[1], 'player_name': 7}
    with pytest.raises(ValueError, match='mixes value types'):
        write(db, [dataclasses_replace(plan, rows=tuple(mixed), capture_id='')], run_id='run-2', clock=LATER)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 2


# ---------------------------------------------------------------- stage-C review round 4

def test_rowless_replacement_refuses_a_table_with_a_foreign_column():
    # Round 4 finding 6: a table of this plan's own columns plus a donor
    # column is not this plan's table; a proven-empty replacement must not
    # delete from it.
    db = CoreDB()
    db.sql.execute(core.core_ddl('squad_memberships').split(' WITH ')[0].replace(
        'iceberg.bronze.', '').rstrip(')') + ', donor_extra varchar)')
    db.sql.execute("INSERT INTO transfermarkt_squad_memberships (league, season, club_id, donor_extra) "
                   "VALUES ('GB1', '2526', '11', 'x')")
    with pytest.raises(RuntimeError, match='schema differs'):
        write(db, [_empty_roster_plan()], run_id='run-2')
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 1

def test_plan_rows_carry_the_evidence_the_plan_names():
    # Round 4 finding 13: only the capture ids were compared, so a row's body
    # hash could differ from the evidence and still reach the receipt.
    plan = membership_plan(['1'], capture='cap-1')
    row = plan.rows[0]
    evidenced = (
        ('source_url', row['source_url']), ('source_endpoint', 'squad'),
        ('raw_capture_id', 'cap-1'), ('source_capture_id', 'cap-1'),
        ('source_body_hash', 'e' * 64), ('scope_id', 'tm-scope'), ('cycle_id', 'tm-cycle'),
        ('parser_revision', 'core-v1'), ('schema_revision', '1'),
    )
    with pytest.raises(ValueError, match='lineage differs'):
        dataclasses_replace(plan, evidence=evidenced)
    # The row's own evidence is accepted.
    own = tuple(
        (column, row['source_body_hash'] if column == 'source_body_hash' else value)
        for column, value in evidenced)
    assert dataclasses_replace(plan, evidence=own).evidence == own

def test_empty_replacement_cannot_erase_its_own_capture():
    # Round 4 finding 3, single-page lane: a capture is never both populated
    # and empty; the same-capture integrity check used to be skipped.
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-1')])
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [_empty_roster_plan(clock=LATER, capture='cap-1')], run_id='run-2', clock=LATER)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 2

def test_guard_reads_the_page_manifest_of_a_rowless_committed_page():
    # Round 4 finding 1: a page the last walk left without rows exists only
    # in the manifest; the data rows cannot prove its clock.
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2, ATTENDANCE_START
    plan = _snapshot(EARLIER, 'cap-old')
    manifest = [(ATTENDANCE_START, 'empty', SIBLING_CLOCK, 'a' * 64),
               (ATTENDANCE_PAGE_2, 'cap-2', SIBLING_CLOCK, 'b' * 64)]

    def guard(pages):
        cursor = GuardCursor(rows=[(SIBLING_CLOCK,)], pages=pages, manifest=manifest)
        connection = type('C', (), {'cursor': lambda self, c=cursor: c})()
        native_writer.guard_native_bundle(connection, plan)

    with pytest.raises(StaleTransfermarktWrite):
        guard([(ATTENDANCE_PAGE_2, SIBLING_CLOCK)])
    # Without the manifest the rows alone would let the stale page through.
    cursor = GuardCursor(rows=[(SIBLING_CLOCK,)], pages=[(ATTENDANCE_PAGE_2, SIBLING_CLOCK)])
    native_writer.guard_native_bundle(
        type('C', (), {'cursor': lambda self, c=cursor: c})(), plan)


def _players(db):
    return sorted(row[0] for row in db.sql.execute(
        'SELECT player_id FROM transfermarkt_squad_memberships'))


def test_failed_insertion_keeps_the_committed_scope():
    # Round 4 finding 2: the writer committed its own DELETE before calling
    # the insertion, so a failed insertion left the committed scope empty.
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-1')])

    class TimedOut(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _rejected_target(db, TimeoutError('insertion timeout'))

    with pytest.raises(TimeoutError, match='insertion timeout'):
        native_writer.write_core_bundle(
            lambda: db, TimedOut(db), [membership_plan(['1', '9'], capture='cap-2', clock=LATER)],
            run_id='run-2', universe_version='core-universe-v1', committed_at=LATER)
    assert _players(db) == ['1', '2']

    # A populated replacement hands its scope to the writer's staged
    # replacement and issues no DELETE of its own.
    events, writer = len(db.events), CoreWriter(db)
    receipt = native_writer.write_core_bundle(
        lambda: db, writer, [membership_plan(['1', '9'], capture='cap-3', clock=LATER)],
        run_id='run-3', universe_version='core-universe-v1', committed_at=LATER)
    assert receipt['verified'] is True
    assert writer.replacements == [
        ('transfermarkt_squad_memberships', "league = 'GB1' AND season = '2526' AND club_id = '11'")]
    assert not [sql for sql in db.events[events:]
                if sql.startswith('DELETE FROM iceberg.bronze.transfermarkt_squad_memberships')]
    assert _players(db) == ['1', '9']
    # A proven-empty replacement has nothing to stage: it stays one DELETE.
    events = len(db.events)
    write(db, [_empty_roster_plan(clock=NEWEST, capture='cap-empty')], run_id='run-4', clock=NEWEST)
    assert [sql for sql in db.events[events:] if sql.startswith('DELETE')] == [
        'DELETE FROM iceberg.bronze.transfermarkt_squad_memberships '
        'WHERE league = ? AND season = ? AND club_id = ?']
    assert _players(db) == []


def test_populated_replacement_reaches_the_transfermarkt_writer_as_its_delete_filter(monkeypatch):
    # Round 4 finding 2, the wiring: the real writer class with only its
    # storage replaced. The manager behind it turns a delete filter into the
    # staged single-statement replacement (test_transfermarkt_writer_safety).
    from scrapers.transfermarkt import writer as transfermarkt_writer
    seen = {}

    def storage(self, df, database, table, partition_spec, mode='append', **kwargs):
        seen.update(database=database, table=table, mode=mode, **kwargs)
        return 'iceberg.bronze.' + table

    monkeypatch.setattr(IcebergWriter, '_write_to_iceberg', storage)
    native_writer._mutate_plan(
        CoreDB(), transfermarkt_writer.TransfermarktIcebergWriter(trino_host='trino.invalid', trino_port=8080),
        membership_plan(['1']))
    assert seen == {
        'database': 'bronze', 'table': 'transfermarkt_squad_memberships', 'mode': 'append',
        'delete_filter': "league = 'GB1' AND season = '2526' AND club_id = '11'",
        'merge_keys': None, 'bulk_arrow': False}


def test_plan_rows_are_read_only_after_validation():
    # Round 4 finding 7: a plan's rows stayed writable through plan.rows, so
    # the writer could verify content the plan never validated.
    plan = _snapshot(CLOCK, 'cap-1')
    digest = plan.plan_sha256()
    row = plan.rows[0]
    with pytest.raises(TypeError):
        row['observation_status'] = 'unknown'
    with pytest.raises(TypeError):
        row['parser_revision'] = 'wrong'
    # A mapping with no mutating method at all (round 5 finding 14): there is
    # no initialiser or update left to refill a validated row through.
    for attempt in (lambda: row.update(stadium_name='Ashburton Grove'),
                    lambda: row.pop('club_name'), lambda: row.setdefault('club_name', 'x'),
                    lambda: row.clear()):
        with pytest.raises(AttributeError):
            attempt()
    with pytest.raises(TypeError):
        del row['club_name']
    with pytest.raises(TypeError):
        row |= {'club_name': 'x'}
    assert plan.plan_sha256() == digest
    # Still a mapping for every consumer: copyable and serialisable as dicts.
    from collections.abc import Mapping
    assert isinstance(row, Mapping) and not isinstance(row, dict)
    assert row['observation_status'] == 'observed_present'
    assert {**row, 'club_name': 'x'}['club_name'] == 'x'
    assert {json.dumps(item, sort_keys=True)
            for item in native_writer._sorted_rows(plan.rows, plan.natural_keys)} == {
        json.dumps(dict(item), sort_keys=True) for item in plan.rows}
    assert [dict(item) for item in asdict(plan)['rows']] == [dict(item) for item in plan.rows]
    # Another plan is the way to change rows.
    changed = dataclasses_replace(plan, rows=tuple({**item, 'stadium_name': 'x'} for item in plan.rows))
    assert changed.plan_sha256() != digest and plan.plan_sha256() == digest


def test_key_cells_compare_the_moment_not_its_spelling():
    # Round 4 finding 8: an engine returns a clock as a datetime, a plan
    # holds it as text; both spellings of one moment are one key.
    columns = ('referee_id', 'observed_at')
    engine = native_writer._key_cells(('101', datetime(2026, 11, 20, 15, 0, tzinfo=timezone.utc)), columns)
    assert engine == native_writer._key_cells(('101', '2026-11-20T15:00:00.000000+00:00'), columns)
    assert engine == native_writer._key_cells(('101', '2026-11-20T15:00:00Z'), columns)
    assert engine != native_writer._key_cells(('101', '2026-11-20T15:00:01.000000+00:00'), columns)
    # Only clock columns are read as moments; text stays text.
    assert native_writer._key_cells(('2026-11-20T15:00:00Z', 'x'), ('note', 'x')) == (
        '2026-11-20T15:00:00Z', 'x')


def test_pre_upgrade_intent_is_finished_after_verified_recovery():
    # Round 3 finding 10: an intent saved under an earlier plan digest must
    # not stay pending after its verified recovery.
    db = CoreDB()
    plan_hash, old_digest = _stage_b_identity(_empty_roster_plan(), 'run-old')
    _legacy_intent(_empty_roster_plan(), old_digest)
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert write_journal.pending_intents({'run_id': 'run-old'}) == []
    assert write_journal.read_receipt(old_digest)['verified'] is True
    # The receipt the stage-B writer never reached is completed under its identity.
    assert db.sql.execute(
        'SELECT plan_sha256 FROM transfermarkt_core_receipts_v1').fetchall() == [(plan_hash,)]
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db)) == []


def test_recovery_finishes_the_exact_intent_file_it_replayed():
    # Round 4 finding 11: a populated stage-B intent kept its digest, was
    # re-saved in the current format, and only that copy was finished.
    db = CoreDB()
    plan = membership_plan(['1'], capture='cap-1')
    digest = native_writer._digest({'plans': [plan.plan_sha256()], 'run_id': 'run-old'})
    path = _legacy_intent(plan, digest)
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert not path.exists()
    assert write_journal.pending_intents({'run_id': 'run-old'}) == []
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db)) == []
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] == 1


def test_legacy_recovery_keeps_the_identity_of_its_warehouse_receipt():
    # Round 4 finding 10: recovery committed under the new digest before it
    # looked at the saved identity, so a legacy write that had already
    # produced its warehouse receipt was acknowledged a second time.
    db = CoreDB()
    plan = _empty_roster_plan()
    plan_hash, legacy_digest = _stage_b_identity(plan, 'run-old')
    path = _legacy_intent(plan, legacy_digest)
    # The stage-B writer crashed after its warehouse receipt was verified.
    native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
    legacy_receipt = native_writer._plan_receipt(
        plan, {'entity': plan.entity, 'table': plan.table, 'rows': 0,
               'physical_hash': native_writer._digest([]), 'expected_rows': 0, 'status_counts': {}},
        plan_hash, legacy_digest, 'run-old', 'u', native_writer._batch_id_for_run('run-old'), CLOCK)
    native_writer._merge_receipts(db, [legacy_receipt])
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    # One warehouse receipt for the run, scope and capture: the legacy one.
    assert db.sql.execute(
        'SELECT receipt_id, plan_sha256 FROM transfermarkt_core_receipts_v1').fetchall() == [
        (legacy_receipt['receipt_id'], legacy_receipt['plan_sha256'])]
    assert write_journal.read_receipt(legacy_digest)['plan_sha256'] == legacy_digest
    assert not path.exists() and write_journal.pending_intents({'run_id': 'run-old'}) == []


def test_unrecognised_intent_digest_is_refused_before_mutation():
    # Round 4 finding 10: a digest that is neither this writer's nor the one
    # before it says nothing about what the intent committed.
    db = CoreDB()
    plan = _empty_roster_plan()
    path = _legacy_intent(plan, 'f' * 64)
    with pytest.raises(RuntimeError, match='not recognised'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert path.exists() and len(write_journal.pending_intents({'run_id': 'run-old'})) == 1
    assert write_journal.read_receipt('f' * 64) is None
    assert not db.sql.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'transfermarkt_core_receipts_v1'"
    ).fetchall()


# ---------------------------------------------------------------- stage-C review round 5

def test_page_evidence_is_validated_field_by_field():
    # Round 5 finding 11: the page-tuple checks sat inside all(...), where the
    # truthy map object short-circuited them, and the journal rebuilds plans
    # through the same constructor.
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_START
    plan = _snapshot(CLOCK, 'cap-1')
    first = plan.pages[0]
    with pytest.raises(ValueError, match='page evidence'):
        dataclasses_replace(plan, pages=plan.pages + (first,))
    with pytest.raises(ValueError, match='page evidence'):
        dataclasses_replace(plan, pages=(
            *plan.pages, (ATTENDANCE_START + '?page=9', first[1], first[2], 'invalid', '')))
    with pytest.raises(ValueError, match='page evidence'):
        dataclasses_replace(plan, pages=(
            *plan.pages, (ATTENDANCE_START + '?page=9', first[1], first[2], 'f' * 64, '')))
    with pytest.raises(ValueError, match='page evidence'):
        dataclasses_replace(plan, pages=(
            *plan.pages[:-1], (*plan.pages[-1][:2], '', plan.pages[-1][3], '')))
    # A page clock is stated one way only, like the clock of a row.
    with pytest.raises(ValueError, match='page evidence'):
        dataclasses_replace(plan, pages=(
            (*plan.pages[0][:2], plan.pages[0][2].replace('+00:00', 'Z'), *plan.pages[0][3:]),
            plan.pages[1]))


def test_a_plan_rebuilt_at_the_write_boundary_must_be_one_whole_walk():
    # Round 5 finding 3, the write boundary: the journal rebuilds a plan
    # without an outcome, so the walk is checked here as well.
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2, ATTENDANCE_START
    plan = _snapshot(CLOCK, 'cap-1')
    assert [page[4] for page in plan.pages] == [ATTENDANCE_PAGE_2, '']
    with pytest.raises(ValueError, match='one walk'):
        dataclasses_replace(plan, pages=plan.pages[:1])
    with pytest.raises(ValueError, match='one walk'):
        dataclasses_replace(plan, pages=plan.pages[::-1])
    with pytest.raises(ValueError, match='one walk'):
        dataclasses_replace(plan, pages=(
            (*plan.pages[0][:4], ATTENDANCE_START + '?page=3'), plan.pages[1]))
    with pytest.raises(ValueError, match='one walk'):
        dataclasses_replace(plan, pages=(plan.pages[0], (*plan.pages[1][:4], plan.pages[0][0])))


def test_a_capture_committed_as_empty_cannot_acquire_rows():
    # Round 5 finding 4, single-page lane: the zero-row receipt of the scope
    # names the capture that was accepted as empty.
    db = CoreDB()
    write(db, [_empty_roster_plan(capture='cap-x')])
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [membership_plan(['1'], capture='cap-x')], run_id='run-2', clock=CLOCK)
    assert _players(db) == []


def test_the_writer_accepts_only_a_conclusive_plan():
    # Round 5 finding 5: a rowless plan with an inconclusive status reached
    # the public writer, deleted the committed scope and was acknowledged.
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-1')])
    for status in ('unknown', 'unavailable', 'error', 'explicit_none'):
        with pytest.raises(ValueError, match='status'):
            dataclasses_replace(_empty_roster_plan(clock=LATER), status=status, proof='x')
    # Rows need a status that carries rows; an absence needs a proof.
    for status in ('observed_empty', 'not_applicable'):
        with pytest.raises(ValueError, match='proof'):
            dataclasses_replace(_empty_roster_plan(clock=LATER), status=status, proof='')
    with pytest.raises(ValueError, match='status'):
        dataclasses_replace(membership_plan(['1']), status='observed_empty', proof='empty_roster:x')
    with pytest.raises(ValueError, match='status'):
        dataclasses_replace(membership_plan(['1']), status='not_applicable', proof='x')
    assert _players(db) == ['1', '2']
    # The conclusive combinations stay accepted.
    dataclasses_replace(_empty_roster_plan(clock=LATER), status='not_applicable')
    assert membership_plan(['1']).status == 'observed_present'


def test_validated_rows_cannot_be_reinitialised():
    # Round 5 finding 14: the read-only row was still a dictionary, and the
    # initialiser it inherited refilled it in place; the writer then verified
    # an observed_present receipt over rows marked unknown.
    plan = _snapshot(CLOCK, 'cap-1')
    digest = plan.plan_sha256()
    row = plan.rows[0]
    with pytest.raises(TypeError):
        row.__init__(observation_status='unknown', parser_revision='wrong')
    with pytest.raises(TypeError):
        row.__init__({'observation_status': 'unknown'})
    with pytest.raises((TypeError, AttributeError)):
        row._values['observation_status'] = 'unknown'
    with pytest.raises((TypeError, AttributeError)):
        row._values = {'observation_status': 'unknown'}
    assert plan.plan_sha256() == digest
    assert row['observation_status'] == 'observed_present' and row['parser_revision'] == 'attendance-v1'
    db = CoreDB()
    receipt = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [plan], run_id='run-14', universe_version='u',
        committed_at=plan.captured_at)
    assert receipt['plans'][0]['status_counts'] == {'observed_present': 2}


# ---------------------------------------------------------------- round 5: journal and receipts

_EMPTY_PROOF = {'entity': 'squad_memberships', 'table': 'transfermarkt_squad_memberships',
                'rows': 0, 'physical_hash': None, 'expected_rows': 0, 'status_counts': {}}


def _receipt_count(db):
    return db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0]


def test_an_existing_receipt_is_never_overwritten_with_another_capture():
    # Round 5 finding 8: stage B digests a rowless plan without its capture,
    # so two captures of one scope at one clock share a receipt id. Recovery
    # of the second overwrote the first one's evidence through MERGE UPDATE.
    db = CoreDB()
    native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
    first = _empty_roster_plan(capture='cap-a')
    second = _empty_roster_plan(capture='cap-b')
    plan_hash, legacy_digest = _stage_b_identity(first, 'run-old')
    assert _stage_b_identity(second, 'run-old') == (plan_hash, legacy_digest)
    seeded = native_writer._plan_receipt(
        first, {**_EMPTY_PROOF, 'physical_hash': native_writer._digest([])}, plan_hash,
        legacy_digest, 'run-old', 'u', native_writer._batch_id_for_run('run-old'), CLOCK)
    native_writer._merge_receipts(db, [seeded])
    _legacy_intent(second, legacy_digest)
    with pytest.raises(RuntimeError, match='receipt'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert db.sql.execute(
        'SELECT source_capture_id FROM transfermarkt_core_receipts_v1').fetchall() == [('cap-a',)]


def test_an_identical_acknowledgement_rewrites_no_receipt(monkeypatch):
    # Round 5 finding 8: every acknowledged replay issued a receipt MERGE
    # ... UPDATE, an overwrite in principle.
    db = CoreDB()
    plans = [membership_plan(['1'], capture='cap-1')]
    issued = []
    real = CoreCursor.execute

    def spy(self, sql, params=()):
        # Every write the writer sends to the receipts table, whichever path
        # issues it (round 6 moved the INSERT behind a read-back).
        if ('transfermarkt_core_receipts_v1' in sql
                and not sql.lstrip().startswith(('SELECT', 'CREATE'))):
            issued.append(sql.split('(')[0].strip())
        return real(self, sql, params)

    monkeypatch.setattr(CoreCursor, 'execute', spy)

    def commit():
        return native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)

    first = commit()
    assert issued and 'MERGE' not in issued[0]
    issued.clear()
    assert commit() == first
    assert issued == []
    assert _receipt_count(db) == 1


def test_the_legacy_identity_serves_only_what_stage_b_could_journal():
    # Round 5 finding 9: a stage-C snapshot journalled with its page evidence
    # was accepted under the stage-B digest of a plan without that evidence.
    plan = _snapshot(CLOCK, 'cap-1')
    descriptor = asdict(plan)
    descriptor['rows'] = native_writer._sorted_rows(plan.rows, plan.natural_keys)
    digest = native_writer._digest({
        'plans': [native_writer._stage_b_plan_sha256(plan)], 'run_id': 'run-old'})
    path = write_journal.save_intent(
        {'run_id': 'run-old', 'digest': digest}, {'plans': [descriptor]},
        run_id='run-old', universe_version='u', committed_at=plan.captured_at)
    db = CoreDB()
    with pytest.raises(RuntimeError, match='not recognised'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert path.exists()
    assert not db.sql.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'transfermarkt_core_receipts_v1'"
    ).fetchall()


def test_a_retry_after_a_migrated_recovery_reuses_its_acknowledgement():
    # Round 5 finding 10: the migration was acknowledged under the legacy
    # digest only, so the ordinary writer retry of the same plan committed
    # again under the current digest and wrote a second warehouse receipt.
    db = CoreDB()
    plan = _empty_roster_plan()
    plan_hash, legacy_digest = _stage_b_identity(plan, 'run-old')
    _legacy_intent(plan, legacy_digest)
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    events = len(db.events)
    again = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [plan], run_id='run-old', universe_version='u',
        committed_at=CLOCK)
    assert again['verified'] is True
    assert _receipt_count(db) == 1
    assert db.sql.execute(
        'SELECT plan_sha256 FROM transfermarkt_core_receipts_v1').fetchall() == [(plan_hash,)]
    assert not [sql for sql in db.events[events:] if sql.lstrip().startswith('DELETE')]


# ---------------------------------------------------------------- stage-C review round 6

def _empty_merge_plan():
    """A proven-empty market-value plan: a merge lane with no row."""
    return NativeWritePlan(
        entity='market_value_points', table='transfermarkt_market_value_points',
        ddl=MV_DDL, natural_keys=('player_id', 'mv_date'),
        scope_columns=('player_id',), scope_values=('1',),
        rows=(), captured_at=LATER, write_mode='merge',
        status='observed_empty', status_reason='empty_collection',
        proof='empty_collection:market_value_points', capture_id='cap-mv2',
        evidence=(
            ('source_url', _MV_URL), ('source_endpoint', 'market_value_points'),
            ('raw_capture_id', 'cap-mv2'), ('source_capture_id', 'cap-mv2'),
            ('source_body_hash', 'b'), ('scope_id', 's'), ('cycle_id', 'c'),
            ('parser_revision', 'p'), ('schema_revision', 's'),
        ),
    )


def _referee_profile_plan():
    from tests.unit.scrapers import test_transfermarkt_sections as lanes
    profile, = lanes.sections.extract_referee_profile_observations(
        lanes._referee_profile_capture(), lanes._referee_scope(), referee_id='101',
        extracted_at=lanes.REFEREE_EXTRACTED_AT, universe_version=lanes.UNIVERSE)
    return core.plan_for_outcome(profile, captured_at=lanes.REFEREE_CAPTURED,
                                 scope_values={'referee_id': '101'})


def test_a_plan_is_bound_to_its_entity_contract():
    # Round 6 finding 15: the public constructor accepted a plan that
    # departed from the registered entity: a business column missing from
    # every row, or a merge lane switched to replace.
    plan = membership_plan(['1'])
    for change in ({'write_mode': 'merge'}, {'natural_keys': ('league', 'season', 'club_id')},
                   {'scope_columns': ('league', 'season', 'player_id')},
                   {'ddl': plan.ddl.replace('    player_name varchar,\n', '')}):
        with pytest.raises(ValueError, match='entity contract'):
            dataclasses_replace(plan, **change)
    with pytest.raises(ValueError, match='entity contract'):
        dataclasses_replace(_empty_merge_plan(), write_mode='replace')
    with pytest.raises(ValueError, match='column set'):
        dataclasses_replace(plan, rows=tuple(
            {column: value for column, value in row.items() if column != 'player_name'}
            for row in plan.rows))
    snapshot = _snapshot(CLOCK, 'cap-1')
    with pytest.raises(ValueError, match='column set'):
        dataclasses_replace(snapshot, rows=tuple(
            {column: value for column, value in row.items() if column != 'capacity'}
            for row in snapshot.rows))


def test_a_direct_plan_cannot_contradict_the_row_verdict_policy():
    # Round 6 finding 16: a plan reconstructed by hand carried rows marked
    # unknown under an observed_present status with a proof on top.
    plan = _snapshot(CLOCK, 'cap-1')
    with pytest.raises(ValueError, match='status'):
        dataclasses_replace(plan, rows=tuple(
            {**row, 'observation_status': 'unknown'} for row in plan.rows))
    with pytest.raises(ValueError, match='proof'):
        dataclasses_replace(plan, proof='no rows')


def test_a_snapshot_offered_to_the_writer_starts_on_its_first_page():
    # Round 6 finding 14: only the terminal tuple and its row reached the
    # writer, which replaced the whole edition with them.
    from tests.unit.scrapers.test_transfermarkt_sections import ATTENDANCE_PAGE_2
    plan = _snapshot(CLOCK, 'cap-1')
    with pytest.raises(ValueError, match='first_page'):
        dataclasses_replace(
            plan, rows=tuple(row for row in plan.rows if row['source_url'] == ATTENDANCE_PAGE_2),
            pages=plan.pages[1:], captured_at=plan.pages[1][2],
            capture_id=plan.pages[1][1], evidence=())


def test_named_capture_evidence_comes_from_the_manifest_and_rows_agree_with_the_run():
    # Round 6 finding 17: the receipt took a forged body hash from the plan's
    # evidence, and rows carrying another batch or extraction clock verified.
    plan = _snapshot(CLOCK, 'cap-1')
    forged = tuple(
        ('source_body_hash', 'e' * 64) if column == 'source_body_hash' else (column, value)
        for column, value in plan.evidence)
    with pytest.raises(ValueError, match='evidence'):
        dataclasses_replace(plan, evidence=forged)
    with pytest.raises(ValueError, match='run'):
        dataclasses_replace(plan, rows=tuple(
            {**row, '_batch_id': 'wrong'} if row is plan.rows[0] else row for row in plan.rows))
    with pytest.raises(ValueError, match='run'):
        dataclasses_replace(plan, rows=tuple(
            {**row, 'extracted_at': EARLIER} if row is plan.rows[0] else row for row in plan.rows))


def test_plan_clocks_and_dates_are_canonical():
    # Round 6 finding 18: a date-only plan clock verified, and a DATE column
    # accepted any text.
    with pytest.raises(ValueError, match='canonical'):
        dataclasses_replace(_empty_roster_plan(), captured_at='2026-11-21')
    profile = _referee_profile_plan()
    with pytest.raises(ValueError, match='date'):
        dataclasses_replace(profile, rows=({**profile.rows[0], 'birth_date': 'yesterday'},))


def _receipt_count(db):
    return db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0]


def test_a_receipt_contradiction_is_refused_before_any_mutation():
    # Round 6 finding 1: the contradiction between a bundle's receipt and the
    # one the warehouse already holds under that id was found by the receipts
    # step, after the bundle had replaced live data and inserted the receipts
    # of its other plans.
    db = CoreDB()
    write(db, [membership_plan(['1', '2'], capture='cap-pop')])
    receipts_before = _receipt_count(db)
    first, second = _empty_roster_plan(capture='cap-a'), _empty_roster_plan(capture='cap-b')
    plan_hash, legacy_digest = _stage_b_identity(first, 'run-old')
    seeded = native_writer._plan_receipt(
        first, {**_EMPTY_PROOF, 'physical_hash': native_writer._digest([])}, plan_hash,
        legacy_digest, 'run-old', 'u', native_writer._batch_id_for_run('run-old'), CLOCK)
    native_writer._merge_receipts(db, [seeded])
    _legacy_intent(second, legacy_digest)
    with pytest.raises(RuntimeError, match='contradicts'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert _players(db) == ['1', '2']  # the roster was not deleted
    assert _receipt_count(db) == receipts_before + 1

    plans = [membership_plan(['9'], capture='cap-9'), mv_plan([('2025-01-01', 10)])]
    plan_hashes, digest = native_writer._bundle_identity(plans, 'r')
    forged = native_writer._plan_receipt(
        plans[1], {'entity': plans[1].entity, 'table': plans[1].table, 'rows': 1,
                   'physical_hash': 'h', 'expected_rows': 1, 'status_counts': {}},
        plan_hashes[1], digest, 'r', 'u', native_writer._batch_id_for_run('r'), CLOCK)
    native_writer._merge_receipts(db, [{**forged, 'source_body_hash': 'e' * 64}])
    receipts_before = _receipt_count(db)
    with pytest.raises(RuntimeError, match='contradicts'):
        native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)
    # Neither the first plan's receipt nor any data was written.
    assert _receipt_count(db) == receipts_before
    assert '9' not in _players(db)
    assert not db.sql.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'transfermarkt_market_value_points'"
    ).fetchall()


def test_an_ambiguous_receipt_insert_is_resolved_by_its_finished_query_and_readback():
    # Round 6 finding 3: the commit-conflict retry ladder repeated the receipt
    # INSERT after a commit whose outcome was unknown, and the read-back saw
    # two physical rows for one receipt id.
    class AmbiguousCursor(CoreCursor):
        fired = False

        def execute(self, sql, params=()):
            insert = sql.startswith('INSERT INTO iceberg.ops.transfermarkt_core_receipts_v1')
            if insert and not AmbiguousCursor.fired:
                AmbiguousCursor.fired = True
                super().execute(sql, params)
                self.query_id = 'q_ambiguous_receipt'
                raise RuntimeError('ICEBERG_COMMIT_ERROR: commit state unknown')
            return super().execute(sql, params)

    class AmbiguousDB(CoreDB):
        def cursor(self):
            return AmbiguousCursor(self)

    db = AmbiguousDB()
    with pytest.raises(RuntimeError, match='commit state unknown'):
        native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), [membership_plan(['1'], capture='cap-1')], run_id='r',
            universe_version='u', committed_at=CLOCK)
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='r')
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_ambiguous_receipt','FINISHED',NULL)")
    receipt, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='r')
    assert receipt['verified'] is True
    assert _receipt_count(db) == 1


# ---------------------------------------------------------------- round 6: journal and identity

def _stored_clocks(db):
    return sorted(native_writer._timestamp(value[0]).isoformat() for value in db.sql.execute(
        'SELECT committed_at FROM transfermarkt_core_receipts_v1'))


def test_a_receipt_only_repair_leaves_a_recoverable_intent():
    # Round 6 finding 8: with every data plan already written, a repair that
    # only re-inserts deleted receipts saved no intent, so an interrupted
    # repair had nothing left to replay.
    class RepairDB(CoreDB):
        flaky = False
        inserts = 0

        def cursor(self):
            if not self.flaky:
                return CoreCursor(self)

            class FlakyCursor(CoreCursor):
                def execute(self, sql, params=()):
                    if sql.startswith('INSERT INTO iceberg.ops.transfermarkt_core_receipts_v1'):
                        RepairDB.inserts += 1
                        if RepairDB.inserts == 2:
                            raise self.db.rejected(self, RuntimeError('crash during receipt repair'))
                    return super().execute(sql, params)
            return FlakyCursor(self)

    db = RepairDB()
    plans = [membership_plan(['1'], capture='cap-1'), mv_plan([('2025-01-01', 10)])]

    def commit():
        return native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)

    assert commit()['verified'] is True
    db.sql.execute('DELETE FROM transfermarkt_core_receipts_v1')
    db.flaky = True
    with pytest.raises(RuntimeError, match='crash during receipt repair'):
        commit()
    assert write_journal.pending_intents({'run_id': 'r'})
    db.flaky = False
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert _receipt_count(db) == 2 and not write_journal.pending_intents({'run_id': 'r'})


def test_an_interrupted_migration_never_leaves_an_intent_the_reader_rejects(monkeypatch):
    # Round 6 finding 9: the migration saved a second intent under the legacy
    # digest with a current-format descriptor; an interruption left it behind,
    # and every later recovery stopped at "digest is not recognised".
    db = CoreDB()
    plan = _empty_roster_plan()
    _plan_hash, legacy_digest = _stage_b_identity(plan, 'run-old')
    _legacy_intent(plan, legacy_digest)
    real = native_writer._commit_plans

    def crash(*args, **kwargs):
        raise RuntimeError('crash during migration')

    monkeypatch.setattr(native_writer, '_commit_plans', crash)
    with pytest.raises(RuntimeError, match='crash during migration'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    monkeypatch.setattr(native_writer, '_commit_plans', real)
    receipts = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert write_journal.pending_intents({'run_id': 'run-old'}) == []


def test_an_ordinary_retry_reuses_a_completed_stage_b_acknowledgement():
    # Round 6 findings 10 and 11: a unit the stage-B writer acknowledged
    # completely (journal receipt and warehouse receipt under the old digest,
    # no pending intent, no alias) was committed a second time under today's
    # identity by the ordinary retry; an alias published later carried the
    # post-write anchors instead of the acknowledged ones.
    db = CoreDB()
    plan = _empty_roster_plan()
    plan_hash, legacy_digest = _stage_b_identity(plan, 'run-old')
    _current, current_digest = native_writer._bundle_identity([plan], 'run-old')
    batch = native_writer._batch_id_for_run('run-old')
    write_journal.snapshot_anchors(legacy_digest, db, {'transfermarkt_squad_memberships', 'ops.transfermarkt_core_receipts_v1'})
    acknowledged = write_journal.read_snapshot_anchors(legacy_digest)
    native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
    native_writer.execute_statement(db.cursor(), plan.ddl)
    native_writer._merge_receipts(db, [native_writer._plan_receipt(
        plan, {**_EMPTY_PROOF, 'physical_hash': native_writer._digest([])}, plan_hash,
        legacy_digest, 'run-old', 'u', batch, CLOCK)])
    write_journal.record_receipt(legacy_digest, {
        'verified': True, 'receipt_readback': True, 'run_id': 'run-old', 'universe_version': 'u',
        'committed_at': CLOCK, 'plan_sha256': legacy_digest, 'committed_batch': batch,
        'plans': [], 'manifests': []})
    again = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [plan], run_id='run-old', universe_version='u',
        committed_at=LATER)
    assert again['verified'] is True
    assert _receipt_count(db) == 1
    assert db.sql.execute('SELECT plan_sha256 FROM transfermarkt_core_receipts_v1').fetchall() == [
        (plan_hash,)]
    assert write_journal.read_receipt(current_digest)['verified'] is True
    assert write_journal.read_snapshot_anchors(current_digest) == acknowledged != {}


def test_a_retry_reuses_the_commit_clock_of_its_pending_intent(monkeypatch):
    # Round 6 finding 12: a retry that named a later commit clock saved a
    # second intent for the same digest; its receipt contradicted the one the
    # interrupted write had already stored, and the replay stopped there.
    db = CoreDB()
    plans = [membership_plan(['1'], capture='cap-1')]
    real = write_journal.record_receipt

    def crash(*args, **kwargs):
        raise RuntimeError('crash before the local acknowledgement')

    monkeypatch.setattr(write_journal, 'record_receipt', crash)
    with pytest.raises(RuntimeError, match='crash before'):
        native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)
    monkeypatch.setattr(write_journal, 'record_receipt', real)
    assert _stored_clocks(db) == [native_writer._timestamp(CLOCK).isoformat()]
    again = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=LATER)
    assert again['verified'] is True
    assert _receipt_count(db) == 1
    assert _stored_clocks(db) == [native_writer._timestamp(CLOCK).isoformat()]
    assert not write_journal.pending_intents({'run_id': 'r'})


def test_a_receipt_repair_is_bound_to_the_acknowledged_universe():
    # Round 6 finding 13: a retry under another universe repaired the deleted
    # warehouse receipt with that universe and still returned the original
    # acknowledgement.
    db = CoreDB()
    plans = [membership_plan(['1'], capture='cap-1')]

    def commit(universe):
        return native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version=universe,
            committed_at=CLOCK)

    assert commit('u')['verified'] is True
    db.sql.execute('DELETE FROM transfermarkt_core_receipts_v1')
    with pytest.raises(RuntimeError, match='universe'):
        commit('different')
    assert _receipt_count(db) == 0
    assert commit('u')['verified'] is True
    assert _receipt_count(db) == 1


def _warehouse_receipt(db, plan, run_id, clock):
    native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
    plan_hashes, digest = native_writer._bundle_identity([plan], run_id)
    native_writer._merge_receipts(db, [native_writer._plan_receipt(
        plan, {**_EMPTY_PROOF, 'physical_hash': native_writer._digest([])}, plan_hashes[0],
        digest, run_id, 'u', native_writer._batch_id_for_run(run_id), clock)])


def test_a_displaced_capture_cannot_return_as_empty():
    # Round 6 finding 6: a roster capture that a newer capture displaced came
    # back as a proven-empty roster and erased its successor; one capture id
    # was also acknowledged twice with different bodies and clocks.
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-a')])
    write(db, [membership_plan(['2'], capture='cap-b')], run_id='run-2', clock=LATER)
    assert _players(db) == ['2']
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [_empty_roster_plan(capture='cap-a')], run_id='run-3')
    assert _players(db) == ['2']
    _warehouse_receipt(db, _empty_roster_plan(capture='cap-x'), 'run-old', CLOCK)
    # The same empty response may be observed at another clock without
    # becoming a different capture; only its lineage changes.
    assert write(db, [_empty_roster_plan(clock=LATER, capture='cap-x')], run_id='run-4', clock=LATER)[
        'verified'] is True
    assert _receipt_count(db) == 4  # cap-a, cap-b and two observations of cap-x


def test_an_acknowledged_empty_merge_capture_cannot_acquire_rows():
    # Round 6 finding 7: a merge lane answered "pending" as soon as its data
    # read was empty, without looking at the capture's zero-row receipt.
    db = CoreDB()
    write(db, [_empty_merge_plan()])
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [mv_plan([('2025-01-01', 10)], capture='cap-mv2', clock=LATER)],
              run_id='run-2', clock=LATER)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_market_value_points').fetchone()[0] == 0


def test_run_preflight_refuses_capture_contradictions_between_valid_plans():
    # Round 6 finding 21: two plans that are valid on their own contradicted
    # each other about one capture: populated and proven-empty, or one capture
    # id naming two different responses.
    with pytest.raises(ValueError, match='duplicate core capture'):
        native_writer.validate_run_plans(
            [mv_plan([('2025-01-01', 10)], capture='cap-mv2'), _empty_merge_plan()])
    first = mv_plan([('2025-01-01', 10)], capture='cap-1')
    second = dataclasses_replace(mv_plan([('2025-02-01', 20)], capture='cap-1'), rows=tuple(
        {**row, 'source_body_hash': 'e' * 64} for row in
        mv_plan([('2025-02-01', 20)], capture='cap-1').rows))
    with pytest.raises(ValueError, match='duplicate core capture'):
        native_writer.validate_run_plans([first, second])
    # Fan-out of one capture over several scopes stays legitimate while every
    # plan names the same response.
    from tests.unit.scrapers import test_transfermarkt_sections as lanes
    capture = lanes._club_value_capture()
    outcomes = lanes.sections.extract_club_value_observations(
        capture, lanes._club_value_scope(), competition_id='VIE1',
        extracted_at=lanes.EXTRACTED_AT, universe_version=lanes.UNIVERSE)
    plans = [core.plan_for_outcome(
        outcome, captured_at=lanes.CLUB_CAPTURED,
        scope_values={'club_id': outcome.rows[0]['club_id']}) for outcome in outcomes[:2]]
    assert len(plans) == 2
    native_writer.validate_run_plans(plans)

# ---------------------------------------------------------------- round 6: an uncertain replacement


def _submit_lost_merge(db, apply=None, *, query_id='q_lost_merge'):
    class LostCursor(CoreCursor):
        def execute(self, sql, params=()):
            self.query_id = query_id
            if apply is not None:
                apply()
            state = 'FINISHED' if apply is not None else 'QUEUED'
            db.sql.execute('INSERT INTO test_query_history VALUES (?, ?, NULL)', (self.query_id, state))
            raise TrinoAmbiguousWriteError('Connection lost while executing SQL')

    with native_writer._target_dispatch() as statement:
        native_writer._execute_once(LostCursor(db), 'MERGE INTO target', None, statement)


class _LostResponse(CoreWriter):
    """A staged replacement that committed before the connection dropped."""

    dispatches_journalled_target = True

    def _write_to_iceberg(self, *args, **kwargs):
        apply = super()._write_to_iceberg
        _submit_lost_merge(self.db, lambda: apply(*args, **kwargs))


@pytest.mark.parametrize('entry', ['retry', 'replay'])
def test_an_uncertain_replacement_is_settled_by_reading_the_scope_back(entry):
    # Round 6 finding 2. The replacement MERGE may commit before its response
    # is lost. The bundle is then neither acknowledged nor written again: the
    # rows the journal's replay reads back are the plan's own.
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-1')])
    plans = [membership_plan(['1', '2'], capture='cap-2', clock=LATER)]
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(
            lambda: db, _LostResponse(db), plans, run_id='run-2',
            universe_version='core-universe-v1', committed_at=LATER)
    assert _players(db) == ['1', '2']
    assert write_journal.pending_intents({'run_id': 'run-2'})
    assert _receipt_count(db) == 1  # nothing acknowledged the unverified write
    settled = CoreWriter(db)
    if entry == 'retry':
        receipt = write(db, plans, run_id='run-2', clock=LATER)
    else:
        receipt = native_writer.replay_pending_intents(lambda: db, settled)[0]
    assert receipt['verified'] is True and receipt['receipt_readback'] is True
    assert settled.writes == []
    assert _players(db) == ['1', '2'] and _receipt_count(db) == 2
    assert write_journal.pending_intents({'run_id': 'run-2'}) == []


def test_an_uncertain_snapshot_finishes_its_walk_without_repeating_the_target():
    # The recorded MERGE finished and its rows are intact. Only ops evidence is missing.
    db = CoreDB()
    older = _snapshot(EARLIER, 'cap-old')
    native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [older], run_id='run-a', universe_version='u',
        committed_at=older.captured_at)
    snapshot = _snapshot(CLOCK, 'cap-1')
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(
            lambda: db, _LostResponse(db), [snapshot], run_id='run-b', universe_version='u',
            committed_at=LATER)
    settled = CoreWriter(db)
    receipt = native_writer.replay_pending_intents(lambda: db, settled)[0]
    assert receipt['verified'] is True
    assert settled.writes == []
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, snapshot), snapshot.rows)
    # Its walk is logged once, under the receipt that acknowledges it.
    plan_hashes, digest = native_writer._bundle_identity([snapshot], 'run-b')
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_core_pages_v1 WHERE receipt_id = ?',
        (native_writer._receipt_id(plan_hashes[0], 'run-b', digest),)).fetchone() == (len(snapshot.pages),)
    assert write_journal.pending_intents({'run_id': 'run-b'}) == []


def test_the_production_writer_never_replays_the_replacement_of_a_core_write(monkeypatch):
    # Round 6 finding 2, the wiring. The commit boundary marks the thread, the
    # production writer hands the scope to its manager, and the manager runs
    # the replacement MERGE it generates once.
    from scrapers.transfermarkt import writer as transfermarkt_writer
    from tests.unit.scrapers.test_transfermarkt_writer_safety import (
        _TombstoneWarehouse, _scripted,
    )

    plan = membership_plan(['B'], capture='cap-b')
    warehouse = _TombstoneWarehouse(
        plan.table, [dict(membership_plan(['A'], capture='cap-a').rows[0])])
    production = transfermarkt_writer.TransfermarktIcebergWriter(
        trino_host='trino.invalid', trino_port=8080)
    production._trino_manager = _scripted(warehouse)
    # The core writer created the table and checked its columns itself.
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(IcebergWriter, '_evolve_schema', lambda *args: None)

    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(
            lambda: CoreDB(), production, [plan], run_id='run-1',
            universe_version='core-universe-v1', committed_at=CLOCK)

    assert len(warehouse.merges) == 1
    assert warehouse.players() == ['B']
    assert write_journal.pending_intents({'run_id': 'run-1'})
    assert not native_writer.write_is_journalled()  # the boundary closed with the failure


# ---------------------------------------------------------------- round 7: writer pins


def _walk_pages():
    """Page captures of the attendance edition at this file's clocks."""
    from tests.unit.scrapers.test_transfermarkt_sections import (
        ATTENDANCE_FIXTURES, ATTENDANCE_PAGE_2, ATTENDANCE_START, _capture,
    )

    def page(url, fixture, clock, name):
        return _capture(url, (ATTENDANCE_FIXTURES / fixture).read_bytes(), clock,
                        capture_id=hashlib.sha256(name.encode()).hexdigest())
    return ATTENDANCE_START, ATTENDANCE_PAGE_2, page


class _UnknownOpsCursor(CoreCursor):
    """Every INSERT into an ops table answers "commit state unknown" and lands nothing."""

    inserts = 0

    def execute(self, sql, params=()):
        if sql.startswith('INSERT INTO iceberg.ops.'):
            type(self).inserts += 1
            raise self.db.rejected(self, RuntimeError('ICEBERG_COMMIT_ERROR: commit state unknown'))
        return super().execute(sql, params)


class _UnknownOpsDB(CoreDB):
    healthy = False

    def cursor(self):
        return CoreCursor(self) if self.healthy else _UnknownOpsCursor(self)


def test_an_ops_insert_with_an_unknown_outcome_is_never_repeated_at_once():
    # Round 7 finding 2 (in-process part): an INSERT whose commit was unknown
    # was repeated at once because the read-back saw no row. The first
    # statement can still land after that read: a receipt twice under one id,
    # or a walk logged twice. Replay learns the rejection from the coordinator.
    for plans in ([membership_plan(['1'], capture='cap-1')], [_snapshot(CLOCK, 'cap-1')]):
        db = _UnknownOpsDB()
        _UnknownOpsCursor.inserts = 0
        run_id = plans[0].rows[0]['run_id']
        with pytest.raises(RuntimeError, match='commit state unknown'):
            native_writer.write_core_bundle(
                lambda: db, CoreWriter(db), plans, run_id=run_id,
                universe_version='core-universe-v1', committed_at=plans[0].captured_at)
        assert _UnknownOpsCursor.inserts == 1 and _receipt_count(db) == 0
        if plans[0].pages:
            assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (0,)
        assert write_journal.pending_intents({'run_id': run_id})
        db.healthy = True
        settled = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        assert len(settled) == 1 and settled[0]['verified'] is True and _receipt_count(db) == 1
    # The replay logged the two-page walk once.
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (2,)


def _directory_plans(referee, capture_id):
    from tests.unit.scrapers.test_transfermarkt_sections import (
        REFEREE_CAPTURED, REFEREE_DIRECTORY_URL, _referee_body, _referee_directory, _referee_page,
        _referee_plans,
    )
    page = dataclasses_replace(
        _referee_page(REFEREE_DIRECTORY_URL, _referee_body([(referee, 'Ref ' + referee)])),
        capture_id=capture_id)
    return _referee_plans(_referee_directory((page,)), {capture_id: REFEREE_CAPTURED})


def test_a_capture_cannot_be_two_responses_in_the_referee_directory():
    # Round 7 finding 11: the same directory URL and capture clock answered
    # twice with different bodies under one capture id. The rows of the first
    # response were already there, so the capture's receipt evidence was never
    # compared and both responses were acknowledged.
    from tests.unit.scrapers.test_transfermarkt_sections import REFEREE_EXTRACTED_AT
    db = CoreDB()
    first, second = _directory_plans('101', 'a' * 64), _directory_plans('303', 'a' * 64)
    assert first[0].capture_id == second[0].capture_id and first[0].rows != second[0].rows
    write(db, first, run_id=first[0].rows[0]['run_id'], clock=REFEREE_EXTRACTED_AT)
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, second, run_id=second[0].rows[0]['run_id'], clock=REFEREE_EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT COUNT(DISTINCT source_body_hash) FROM transfermarkt_core_receipts_v1 '
        'WHERE source_capture_id = ?', ('a' * 64,)).fetchone() == (1,)
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_referee_aggregate_observations WHERE referee_id = ?',
        ('303',)).fetchone() == (0,)


def _other_cycle(plan, cycle):
    """The same capture as a later cycle observed it: only the cycle differs."""
    return dataclasses_replace(
        plan, rows=tuple({**row, 'cycle_id': cycle} for row in plan.rows),
        evidence=tuple((key, cycle) if key == 'cycle_id' else (key, value)
                       for key, value in plan.evidence))


def test_a_cached_capture_observed_in_another_cycle_is_a_lineage_refresh():
    # Round 7 finding 13: the cycle names the run that observed a capture, as
    # its run id does. Compared as a business fact, a cached page could never
    # return in a later cycle.
    from tests.unit.scrapers.test_transfermarkt_sections import (
        LATER_EXTRACTED_AT, _attendance_plan,
    )
    start, page_2_url, page = _walk_pages()
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-1')])
    refreshed = _other_cycle(
        membership_plan(['1'], capture='cap-1', run='run-2', ingested=LATER), 'tm-cycle-2')
    assert write(db, [refreshed], run_id='run-2', clock=LATER)['verified'] is True
    assert db.sql.execute('SELECT DISTINCT cycle_id FROM transfermarkt_squad_memberships').fetchall() \
        == [('tm-cycle-2',)]
    older = _snapshot(EARLIER, 'cap-old')
    write(db, [older], run_id='run-a', clock=older.captured_at)
    # A later cycle walks a fresher first page and the very capture of page 2
    # the earlier cycle acknowledged: the cache served it again.
    later = _other_cycle(_attendance_plan(
        (page(start, 'page_1.html', NEWEST, 'cap-late'),
         page(page_2_url, 'page_2.html', SIBLING_CLOCK, 'cap-2')), LATER_EXTRACTED_AT), 'tm-cycle-2')
    assert write(db, [later], run_id=later.rows[0]['run_id'], clock=later.captured_at)['verified'] is True
    assert db.sql.execute('SELECT DISTINCT cycle_id FROM transfermarkt_attendance_observations').fetchall() \
        == [('tm-cycle-2',)]
    # One receipt per acknowledgement: two rosters and two walks.
    assert _receipt_count(db) == 4


def test_an_acknowledgement_superseded_at_an_equal_clock_cannot_be_retried():
    # Round 7 finding 5: two different captures of one page at one clock are
    # ordered by their acknowledgement. The retry of the earlier one restored
    # the rows the later one had left out, and returned verified.
    from tests.unit.scrapers.test_transfermarkt_sections import (
        EXTRACTED_AT, LATER_EXTRACTED_AT, ROWLESS_TERMINAL_PAGE_2, _attendance_plan, _capture,
    )
    start, page_2_url, page = _walk_pages()
    page_1 = page(start, 'page_1.html', CLOCK, 'cap-1')
    empty_2 = _capture(page_2_url, ROWLESS_TERMINAL_PAGE_2, SIBLING_CLOCK,
                       capture_id=hashlib.sha256(b'empty').hexdigest())
    first = _attendance_plan(
        (page_1, page(page_2_url, 'page_2.html', SIBLING_CLOCK, 'cap-2')), EXTRACTED_AT)
    second = _attendance_plan((page_1, empty_2), LATER_EXTRACTED_AT)
    db = CoreDB()

    def clubs():
        return sorted(row[0] for row in db.sql.execute(
            'SELECT club_id FROM transfermarkt_attendance_observations'))

    write(db, [first], run_id=first.rows[0]['run_id'], clock=first.captured_at)
    assert write(db, [second], run_id=second.rows[0]['run_id'], clock=LATER)['verified'] is True
    assert clubs() == ['11']
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [first], run_id=first.rows[0]['run_id'], clock=first.captured_at)
    assert clubs() == ['11']
    # The later acknowledgement is still the one in force.
    assert write(db, [second], run_id=second.rows[0]['run_id'], clock=LATER)['verified'] is True
    assert clubs() == ['11']


def test_an_acknowledgement_restores_its_own_partly_lost_walk():
    # Round 7 finding 6 (own walk): a walk that lost one of its rows was
    # refused as corrupt even to the acknowledgement that logged it, so
    # nothing could ever make it whole again.
    plan = _snapshot(CLOCK, 'cap-1')
    db = CoreDB()
    first = write(db, [plan], run_id=plan.rows[0]['run_id'], clock=plan.captured_at)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE page_number = 2')
    again = write(db, [plan], run_id=plan.rows[0]['run_id'], clock=plan.captured_at)
    assert again['plan_sha256'] == first['plan_sha256'] and again['verified'] is True
    assert db.sql.execute(
        'SELECT page_number, pages_count FROM transfermarkt_core_pages_v1 ORDER BY page_number'
    ).fetchall() == [(1, 2), (2, 2)]


def test_the_whole_retried_unit_may_restore_its_evidence():
    # Round 7 finding 6, the unit: a retried unit is one acknowledgement. The
    # plans of the unit being retried may all restore what they lost; an
    # acknowledgement outside the unit still waits.
    older, newer = _snapshot(EARLIER, 'cap-old'), _snapshot(CLOCK, 'cap-1')

    def receipt_of(plan, run_id):
        plan_hashes, digest = native_writer._bundle_identity([plan], run_id)
        return native_writer._receipt_id(plan_hashes[0], run_id, digest)

    db = CoreDB()
    write(db, [older], run_id='run-a', clock=older.captured_at)
    write(db, [newer], run_id='run-b', clock=LATER)
    lost, kept = receipt_of(older, 'run-a'), receipt_of(newer, 'run-b')
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id = ?', (lost,))
    with pytest.raises(RuntimeError, match='manifest lost'):
        native_writer._replay_state(db, newer, kept, LATER)
    assert native_writer._replay_state(db, newer, kept, LATER, own={kept, lost}) \
        == 'written'


@pytest.mark.parametrize('after_copy', [False, True])
def test_an_alias_carries_the_anchors_of_the_acknowledgement_it_aliases(monkeypatch, after_copy):
    # Round 7 finding 9: the alias receipt became durable before its anchors
    # were copied. The interrupted copy was never completed, and the next
    # retry anchored the alias to the warehouse as it was after the write.
    db = CoreDB()
    write(db, [membership_plan(['1'], capture='cap-old')], run_id='run-0')
    plan = _empty_roster_plan(clock=LATER, capture='cap-empty')
    _legacy_digest = _stage_b_identity(plan, 'run-old')[1]
    _legacy_intent(plan, _legacy_digest)
    # The warehouse moves on after the legacy acknowledgement was anchored.
    db.snapshots['transfermarkt_core_receipts_v1'] = [(34, None, '2026-11-21 10:00:00')]
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db))[0]['verified'] is True
    db.snapshots['transfermarkt_core_receipts_v1'] = [(86, 34, '2026-11-21 13:00:00')]
    original = write_journal.read_snapshot_anchors(_legacy_digest)
    current = native_writer._bundle_identity([plan], 'run-old')[1]

    copy = write_journal.copy_snapshot_anchors

    def interrupted(*args):
        # Round 12 finding 2. A proven alias copy has an accounted-for source.
        if after_copy:
            copy(*args)
        raise RuntimeError('crash between alias and anchor')

    with monkeypatch.context() as patched:
        patched.setattr(write_journal, 'copy_snapshot_anchors', interrupted)
        with pytest.raises(RuntimeError, match='crash between alias and anchor'):
            native_writer.write_core_bundle(
                lambda: db, CoreWriter(db), [plan], run_id='run-old', universe_version='u',
                committed_at=CLOCK)
    again = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [plan], run_id='run-old', universe_version='u',
        committed_at=CLOCK)
    assert again['verified'] is True
    # The alias is anchored where the acknowledgement it stands for was: before its write emptied the roster.
    assert write_journal.read_snapshot_anchors(current) == original


def test_a_lost_historical_walk_is_restored_from_its_acknowledgement():
    # Round 7 finding 6 (historical walk): the walk of an older
    # acknowledgement was deleted. Nothing could restore it: its own retry is
    # stale, the current acknowledgement and every fresh walk were refused.
    # The journal keeps the acknowledged walk: it is logged again under its
    # own receipt, the data is not touched, and the scope goes on.
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan
    start, _page_2_url, page = _walk_pages()

    def single(clock, name):
        return _attendance_plan((page(start, 'single_page.html', clock, name),))

    def receipt_of(plan, run_id):
        plan_hashes, digest = native_writer._bundle_identity([plan], run_id)
        return native_writer._receipt_id(plan_hashes[0], run_id, digest)

    older, newer, fresh = single(EARLIER, 'cap-old'), single(CLOCK, 'cap-1'), single(NEWEST, 'cap-new')
    db = CoreDB()
    write(db, [older], run_id='run-a', clock=older.captured_at)
    write(db, [newer], run_id='run-b', clock=newer.captured_at)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id = ?', (receipt_of(older, 'run-a'),))
    events = len(db.events)
    assert write(db, [fresh], run_id='run-c', clock=fresh.captured_at)['verified'] is True
    assert db.sql.execute(
        'SELECT receipt_id, source_capture_id FROM transfermarkt_core_pages_v1 ORDER BY captured_at'
    ).fetchall() == [(receipt_of(older, 'run-a'), older.pages[0][1]),
                     (receipt_of(newer, 'run-b'), newer.pages[0][1]),
                     (receipt_of(fresh, 'run-c'), fresh.pages[0][1])]
    # The repair logged the lost walk and nothing else: two walk INSERTs in
    # all, the restored one and that of the fresh snapshot, and no write to
    # the data beyond the fresh snapshot's own.
    inserts = [sql for sql in db.events[events:]
               if sql.startswith('INSERT INTO iceberg.ops.transfermarkt_core_pages_v1')]
    assert len(inserts) == 2
    # A receipt this journal never acknowledged stays lost evidence.
    db.sql.execute('INSERT INTO transfermarkt_core_receipts_v1 SELECT * FROM '
                   'transfermarkt_core_receipts_v1 WHERE receipt_id = ?', (receipt_of(older, 'run-a'),))
    db.sql.execute("UPDATE transfermarkt_core_receipts_v1 SET receipt_id = 'tm-core-receipt-unknown' "
                   'WHERE rowid = (SELECT MAX(rowid) FROM transfermarkt_core_receipts_v1)')
    with pytest.raises(RuntimeError, match='manifest lost'):
        write(db, [single(LATER, 'cap-later')], run_id='run-d', clock=LATER)


def test_the_journal_finds_the_walk_an_acknowledgement_logged():
    db = CoreDB()
    walk, = write(db, [_snapshot(CLOCK, 'logged')])['walks']
    other = membership_plan(['1'])
    ack = write(db, [other], clock=LATER)
    other_id = native_writer._receipt_id(other.plan_sha256(), ack['run_id'], ack['plan_sha256'])
    assert write_journal.acknowledged_walk(walk['receipt_id']) == (walk, CLOCK)
    assert write_journal.acknowledged_walk(other_id) is None


def _empty_squad_plans(clock=LATER):
    roster = _empty_roster_plan(clock=clock)
    return [roster, *(dataclasses_replace(
        roster, entity=entity, table=core.CORE_TABLES[entity], ddl=core.core_ddl(entity),
        natural_keys=core.CORE_NATURAL_KEYS[entity], write_mode=core.CORE_WRITE_MODES[entity])
        for entity in ('player_attribute_observations', 'player_contract_observations'))]


def _stage_b_squad_intent(plans):
    hashes = [native_writer._stage_b_plan_sha256(plan) for plan in plans]
    digest = native_writer._digest({'plans': hashes, 'run_id': 'run-old'})
    descriptors = native_writer._descriptors(plans)
    for descriptor in descriptors:
        del descriptor['pages']
    path = write_journal.save_intent(
        {'run_id': 'run-old', 'digest': digest}, {'plans': descriptors},
        run_id='run-old', committed_at=CLOCK, universe_version='u')
    return digest, path


def test_lost_empty_squad_receipt_cannot_reopen_the_roster():
    # Round 7 finding 4: the two surviving squad receipts belong to other data tables.
    db = CoreDB()
    populated = membership_plan(['1', '2'])
    write(db, [populated])
    write(db, _empty_squad_plans(), run_id='empty', clock=LATER)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE run_id = ? AND entity = ?',
                   ('empty', populated.entity))
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [populated])
    assert _players(db) == [] and _receipt_count(db) == 3


def test_recovered_walk_evidence_participates_in_capture_checks():
    # Round 7 finding 6: a displaced page with its original ID cannot return with other rows.
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan
    db = CoreDB()
    original = _snapshot(CLOCK, 'cap-1')
    start, _, page = _walk_pages()
    shorter = _attendance_plan((page(start, 'single_page.html', LATER, 'shorter'),))
    write(db, [original], run_id='first')
    write(db, [shorter], run_id='shorter', clock=LATER)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE pages_count = 2')
    fresh = _snapshot(NEWEST, 'fresh')
    changed = dataclasses_replace(fresh, rows=tuple(
        {**row, 'spectators_total': row['spectators_total'] + 1}
        if row['club_id'] == '631' else row for row in fresh.rows))
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [changed], run_id='changed', clock=NEWEST)
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, shorter), shorter.rows)
    assert _receipt_count(db) == 2


@pytest.mark.parametrize('anchored_stage_b', [True, False])
def test_ordinary_retry_settles_the_pending_stage_b_squad(monkeypatch, anchored_stage_b):
    # Round 7 finding 7: all three receipts landed before the legacy intent was finished.
    db, plans = CoreDB(), _empty_squad_plans()
    legacy, path = _stage_b_squad_intent(plans)

    def crash(*args):
        raise RuntimeError('lost local acknowledgement')

    if anchored_stage_b:
        # This handover predates dispatch fences. Only complete physical
        # evidence can settle it, through the read-only recovery path.
        write_journal.snapshot_anchors(legacy, db, native_writer._anchored_tables(plans))
        native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
        for plan in plans:
            native_writer.execute_statement(db.cursor(), plan.ddl)
        native_writer._merge_receipts(db, [native_writer._plan_receipt(
            plan, native_writer._physical_proof(db, plan), native_writer._stage_b_plan_sha256(plan),
            legacy, 'run-old', 'u', native_writer._batch_id_for_run('run-old'), CLOCK)
            for plan in plans])
    else:
        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'record_receipt', crash)
            with pytest.raises(RuntimeError, match='lost local acknowledgement'):
                native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    before = db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall()
    assert len(before) == 3 and path.exists()
    receipt = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id='run-old', universe_version='u', committed_at=NEWEST)
    assert receipt['verified'] and receipt['identity']['digest'] == legacy
    assert receipt['committed_at'] == CLOCK
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall() == before
    if anchored_stage_b:
        assert [pending for pending, _ in write_journal.pending_intents({})] == [path]
    else:
        assert not path.exists() and write_journal.pending_intents({}) == []


def test_interrupted_legacy_squad_repair_uses_replayable_descriptors(monkeypatch):
    # Round 7 finding 8: descriptor identity and the original warehouse identity survive repair.
    db, plans = CoreDB(), _empty_squad_plans()
    legacy, _ = _stage_b_squad_intent(plans)
    original, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    current = native_writer._bundle_identity(plans, 'run-old')[1]
    anchors = write_journal.read_snapshot_anchors(legacy)
    assert anchors and write_journal.read_receipt(current) is None
    before = db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall()
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE entity = ?', (plans[0].entity,))

    def interrupt(*args, **kwargs):
        raise RuntimeError('interrupted before INSERT')

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_insert_once', interrupt)
        with pytest.raises(RuntimeError, match='interrupted before INSERT'):
            native_writer.write_core_bundle(lambda: db, CoreWriter(db), plans, run_id='run-old',
                                            universe_version='u', committed_at=NEWEST)
    (_, body), = write_journal.pending_intents({})
    assert body['identity']['digest'] == current
    assert body['payload']['warehouse_identity'] == original['identity']
    replayed, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert replayed['verified'] and replayed['identity'] == original['identity']
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall() == before
    assert write_journal.read_snapshot_anchors(current) == write_journal.read_snapshot_anchors(legacy) == anchors
    assert write_journal.pending_intents({}) == []


@pytest.mark.parametrize('has_binding', [True, False])
def test_missing_legacy_market_value_receipt_cannot_change_body(monkeypatch, has_binding):
    # Round 7 finding 10: whitespace changes the response evidence even though both parses are empty.
    def empty(body):
        body_hash = hashlib.sha256(body).hexdigest()
        plan = _empty_merge_plan()
        evidence = {**dict(plan.evidence), 'raw_capture_id': body_hash,
                    'source_capture_id': body_hash, 'source_body_hash': body_hash}
        return dataclasses_replace(plan, capture_id=body_hash, evidence=tuple(evidence.items()))

    first, other = empty(b'{"list":[]}'), empty(b'{"list": []}')
    db = CoreDB()
    _, legacy = _stage_b_identity(first, 'run-old')
    assert _stage_b_identity(other, 'run-old')[1] == legacy
    _legacy_intent(first, legacy)
    with monkeypatch.context() as patch:
        if not has_binding:
            # An acknowledgement written by the previous version, before it
            # recorded the descriptor digest. Publish it once in that format.
            record = write_journal.record_receipt
            patch.setattr(write_journal, 'record_receipt', lambda digest, receipt: record(
                digest, {k: v for k, v in receipt.items() if k != 'descriptor_digest'}))
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    original = write_journal.read_receipt(legacy)
    before = db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall()
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    start = len(db.events)
    with pytest.raises(RuntimeError, match='capture evidence'):
        native_writer.write_core_bundle(lambda: db, CoreWriter(db), [other], run_id='run-old',
                                        universe_version='u', committed_at=CLOCK)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert _receipt_count(db) == 0 and write_journal.read_receipt(legacy) == original
    assert write_journal.read_receipt(native_writer._bundle_identity([other], 'run-old')[1]) is None
    def retry_original():
        return native_writer.write_core_bundle(lambda: db, CoreWriter(db), [first], run_id='run-old',
                                               universe_version='u', committed_at=CLOCK)

    if has_binding:
        assert retry_original()['verified']
        assert db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall() == before
    else:
        with pytest.raises(RuntimeError, match='independent capture evidence'):
            retry_original()
        assert _receipt_count(db) == 0


class _QueuedReplacement(CoreWriter):
    dispatches_journalled_target = True

    def _write_to_iceberg(self, df, database, table, partition_spec, mode='append',
                          delete_filter=None, merge_keys=None, bulk_arrow=False):
        columns = list(df.columns)
        tombstones = self.db.sql.execute(
            f'SELECT {", ".join(columns)} FROM {table} WHERE {delete_filter}').fetchall()
        staged = df.copy()

        def land():
            # The retained stage deletes only its original row images.
            # Each staged insert takes the unconditional insert branch.
            predicate = ' AND '.join(f'{col} IS ?' for col in columns)
            self.db.sql.executemany(f'DELETE FROM {table} WHERE {predicate}', tombstones)
            CoreWriter._write_to_iceberg(self, staged, database, table, partition_spec)
            self.db.sql.execute("UPDATE test_query_history SET state = 'FINISHED' WHERE query_id = 'q_lost_merge'")

        self.land = land
        _submit_lost_merge(self.db)


def test_queued_attendance_merge_fences_replay_and_newer_capture():
    # Round 7 finding 1: execute the original from its retained stage after replay returns.
    db = CoreDB()
    older = _other_cycle(_snapshot(EARLIER, 'old'), 'older-cycle')
    write(db, [older], run_id='old', clock=older.captured_at)
    pending = _snapshot(CLOCK, 'pending')
    newest = _snapshot(NEWEST, 'newest')
    delayed, retry = _QueuedReplacement(db), CoreWriter(db)
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, delayed, [pending], run_id='pending',
                                        universe_version='u', committed_at=LATER)
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, older), older.rows)
    try:
        early = native_writer.replay_pending_intents(lambda: db, retry, run_id='pending')
    except RuntimeError as exc:
        assert 'still pending' in str(exc)
        with pytest.raises(RuntimeError, match='still pending'):
            write(db, [newest], run_id='newest', clock=NEWEST)
        early = []
    finally:
        delayed.land()
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, pending), pending.rows)
    assert not early
    assert not retry.writes and _receipt_count(db) == 1
    assert write_journal.pending_intents({'run_id': 'pending'})
    settled, = native_writer.replay_pending_intents(lambda: db, retry, run_id='pending')
    assert settled['verified'] and _receipt_count(db) == 2
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, pending), pending.rows)
    assert write(db, [newest], run_id='newest', clock=NEWEST)['verified']
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, newest), newest.rows)


@pytest.mark.parametrize('target', [RECEIPTS, 'transfermarkt_core_pages_v1'])
def test_queued_ops_insert_lands_after_the_replay_attempt(target):
    # Round 7 finding 2: one accepted INSERT may remain queued across a new connection.
    plan = _snapshot(CLOCK, 'pending')

    class QueuedCursor(CoreCursor):
        def execute(self, sql, params=()):
            if sql.startswith(f'INSERT INTO iceberg.ops.{target}') and self.db.queued is None:
                self.db.queued = (sql, tuple(params))
                error = RuntimeError('commit state unknown')
                if target == RECEIPTS:
                    self.query_id = 'queued-receipt'
                    self.db.sql.execute('INSERT INTO test_query_history VALUES (?, ?, ?)',
                                        (self.query_id, 'QUEUED', None))
                raise error
            return super().execute(sql, params)

    class QueuedDB(CoreDB):
        queued = None

        def cursor(self):
            return QueuedCursor(self)

    db = QueuedDB()
    with pytest.raises(RuntimeError, match='commit state unknown'):
        write(db, [plan], run_id='queued', clock=plan.captured_at)
    assert db.sql.execute(f'SELECT count(*) FROM {target}').fetchone() == (0,)
    try:
        early = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    except RuntimeError as exc:
        assert 'still pending' in str(exc)
        early = []
    finally:
        # Even a replay that erroneously returns verified is followed by
        # the original, exactly as accepted, including its bound rows.
        CoreCursor(db).execute(*db.queued)
    expected_count = 1 if target == RECEIPTS else len(plan.pages)
    assert db.sql.execute(f'SELECT count(*) FROM {target}').fetchone() == (expected_count,)
    assert not early
    assert write_journal.pending_intents({'run_id': 'queued'})
    if target == RECEIPTS:
        with pytest.raises(RuntimeError, match='still pending'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='queued-receipt'")
    settled, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert settled['verified'] and write_journal.pending_intents({}) == []
    assert _receipt_count(db) == 1
    assert db.sql.execute('SELECT page_number FROM transfermarkt_core_pages_v1 '
                          'ORDER BY page_number').fetchall() == [(1,), (2,)]


def _copy_warehouse_scope(warehouse, db, plan):
    """Make the actual DuckDB MERGE result visible to the SQLite readback harness."""
    columns = ', '.join(plan.rows[0])
    rows = warehouse.engine.execute(f'SELECT {columns} FROM {plan.table} WHERE {plan.scope_sql()}',
                                    plan.scope_values).fetchall()
    db.sql.execute(f'DELETE FROM {plan.table} WHERE {plan.scope_sql()}', plan.scope_values)
    marks = ', '.join('?' for _ in plan.rows[0])
    db.sql.executemany(f'INSERT INTO {plan.table} ({columns}) VALUES ({marks})', rows)


@pytest.mark.parametrize('fault', ['values', 'tombstones', 'count', 'describe'])
def test_stage_failure_leaves_target_undispatched_and_retry_uses_a_fresh_stage(monkeypatch, fault):
    # Round 8 finding 3. A delayed stage INSERT lands after the retry's count.
    # Round 8 finding 7. A failed count must leave the target free to replay.
    # Round 9 finding 13. Replay must also remove the first physical stage.
    from scrapers.base.trino_manager import TrinoError
    from scrapers.transfermarkt import writer as tm_writer
    from tests.unit.scrapers.test_transfermarkt_writer_safety import _TombstoneWarehouse

    db = CoreDB()
    old = membership_plan(['A'])
    plan = membership_plan(['B'], capture='cap-b', clock=LATER, run='capture-b')
    write(db, [old])
    warehouse = _TombstoneWarehouse(plan.table, [dict(old.rows[0])])
    warehouse.lost = True  # this test loses only the selected staging response
    manager = tm_writer.TransfermarktTrinoManager()
    manager._conn = warehouse.connection()
    manager._create_connection = warehouse.connection
    production = tm_writer.TransfermarktIcebergWriter()
    production._trino_manager = manager
    monkeypatch.setattr(manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(IcebergWriter, '_evolve_schema', lambda *args: None)
    held, stages = [], []
    run = warehouse.run

    def stage_fault(sql):
        if sql.startswith('CREATE TABLE ') and '__stg_' in sql:
            stages.append(sql.split()[2])
        if not held:
            insert = sql.startswith('INSERT INTO ') and '__stg_' in sql
            if ((fault == 'values' and insert and 'VALUES' in sql)
                    or (fault == 'tombstones' and insert and 'SELECT DISTINCT' in sql)):
                held.append(sql)
                raise ConnectionResetError('Connection reset after accepted stage INSERT')
            if fault == 'describe' and sql.startswith('DESCRIBE '):
                held.append(None)
                raise RuntimeError('stage describe unavailable')
            if fault == 'count' and sql.startswith('SELECT count(*)'):
                held.append(None)
                raise RuntimeError('stage count unavailable')
        if len(stages) == 1 and held and sql.startswith('DROP TABLE') and stages[0] in sql:
            return []  # keep the first stage available to its queued query
        if held and sql.startswith('MERGE INTO') and held[0] is not None:
            run(held.pop())
            held.append(None)
        result = run(sql)
        if sql.startswith('MERGE INTO'):
            _copy_warehouse_scope(warehouse, db, plan)
        return result

    monkeypatch.setattr(warehouse, 'run', stage_fault)
    scope = (plan.full_table, native_writer._scope_json(plan))
    with pytest.raises(TrinoError if fault in ('count', 'describe') else TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, production, [plan], run_id='capture-b',
                                        universe_version='u', committed_at=LATER)
    assert len(stages) == 1 and not warehouse.merges and warehouse.players() == ['A']
    assert write_journal.pending_statement(scope) is None
    replayed, = native_writer.replay_pending_intents(lambda: db, production, run_id='capture-b')
    assert replayed['verified'] and _players(db) == ['B']
    assert len(stages) == len(set(stages)) == 2
    assert len(warehouse.merges) == 1 and stages[1].split('.')[-1] in warehouse.merges[0]
    assert held == [None] and warehouse.players() == ['B']
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]
    assert write_journal.pending_intents({}) == []
    assert write_journal.pending_statement(scope) is None


@pytest.mark.parametrize('local_ack', [False, True])
def test_verified_legacy_empty_delete_still_fences_newer_rosters(local_ack):
    # Round 8 finding 4. The physical receipts already exist while a repeated
    # stage-B DELETE waits at the coordinator. Its empty postimage has no ID.
    db, plans = CoreDB(), _empty_squad_plans()
    digest, path = _stage_b_squad_intent(plans)
    write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables(plans))
    native_writer.execute_statement(db.cursor(), native_writer.core_receipts_ddl())
    hashes = [native_writer._stage_b_plan_sha256(plan) for plan in plans]
    proofs = []
    for plan in plans:
        native_writer.execute_statement(db.cursor(), plan.ddl)
        proofs.append(native_writer._physical_proof(db, plan))
    batch = native_writer._batch_id_for_run('run-old')
    native_writer._merge_receipts(db, [
        native_writer._plan_receipt(plan, proof, plan_hash, digest, 'run-old', 'u', batch, CLOCK)
        for plan, proof, plan_hash in zip(plans, proofs, hashes, strict=True)])
    if local_ack:
        # r7 could mark the legacy handover adopted after readback, before
        # retiring the intent. That marker cannot prove a pre-DML handoff.
        write_journal.adopt_intent(path)
        write_journal.record_receipt(digest, {
            'verified': True, 'receipt_readback': True, 'run_id': 'run-old', 'universe_version': 'u',
            'plan_sha256': digest, 'committed_batch': batch, 'committed_at': CLOCK, 'plans': proofs,
            'manifests': []})
    roster = plans[0]
    queued = (f'DELETE FROM {roster.full_table} WHERE {roster.scope_sql()}', roster.scope_values)
    start = len(db.events)
    verified, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-old')
    assert verified['verified']
    assert native_writer.write_core_bundle(lambda: db, CoreWriter(db), plans, run_id='run-old',
                                          universe_version='u', committed_at=CLOCK)['verified']
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [membership_plan(['new'], clock=NEWEST, capture='fresh')], clock=NEWEST)
    db.cursor().execute(*queued)
    assert _players(db) == [] and _receipt_count(db) == len(plans)


def _transfer_plan(player, name, clock):
    entity = 'transfer_events'
    row = _contract_row(entity, entity, 'https://www.transfermarkt.com/ceapi/transferHistory/list/' + player,
                        capture=name, clock=clock, run=name, ingested=clock, player_id=player,
                        transfer_id='transfer-' + player, to_club_name=name)
    return NativeWritePlan(
        entity=entity, table=core.CORE_TABLES[entity], ddl=core.core_ddl(entity),
        natural_keys=core.CORE_NATURAL_KEYS[entity], scope_columns=('player_id',),
        scope_values=(player,), rows=(row,), captured_at=clock, write_mode='merge',
        status_reason='career_page', capture_id=name)


def test_delayed_keyed_transfer_merge_serializes_only_its_player(monkeypatch):
    # Round 8 finding 5. Queue the real single-row manager's target SQL and
    # apply it later, after an overlapping corrected capture tries to publish.
    from scrapers.transfermarkt import writer as tm_writer
    from tests.unit.scrapers.test_transfermarkt_writer_safety import _TombstoneWarehouse, _scripted

    db = CoreDB()
    old, fresh = _transfer_plan('1', 'old', CLOCK), _transfer_plan('1', 'corrected', LATER)
    initial = _transfer_plan('1', 'initial', EARLIER)
    write(db, [initial], run_id='initial', clock=EARLIER)
    warehouse = _TombstoneWarehouse(old.table, [dict(initial.rows[0])])
    production = tm_writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(IcebergWriter, '_evolve_schema', lambda *args: None)
    held = []
    run = warehouse.run

    def queue(sql):
        if sql.startswith('MERGE INTO'):
            held.append(sql)
            raise ConnectionResetError('Connection reset after accepted keyed MERGE')
        return run(sql)

    monkeypatch.setattr(warehouse, 'run', queue)
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, production, [old], run_id='old',
                                        universe_version='u', committed_at=CLOCK)
    assert len(held) == 1 and 'WHEN MATCHED THEN UPDATE SET' in held[0]
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [fresh], run_id='corrected', clock=LATER)
    other = _transfer_plan('2', 'unrelated', LATER)
    assert write(db, [other], run_id='unrelated', clock=LATER)['verified']
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='old')

    run(held[0])
    _copy_warehouse_scope(warehouse, db, old)
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge', 'FINISHED', NULL)")
    # Round 9 finding 13. The queued target needed its stage through land().
    receipt, = native_writer.replay_pending_intents(lambda: db, production, run_id='old')
    assert receipt['verified'] and len(held) == 1 and len(warehouse.merges) == 1
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(old.table,)]
    assert write(db, [fresh], run_id='corrected', clock=LATER)['verified']
    assert db.sql.execute(f'SELECT player_id, to_club_name FROM {old.table} ORDER BY player_id').fetchall() == [
        ('1', 'corrected'), ('2', 'unrelated')]


def test_first_response_delete_rejection_requires_its_coordinator_record(monkeypatch):
    # Round 8 finding 6. process() raises a real TrinoUserError before the
    # cursor has assigned query_id. The admission response is from the DELETE.
    import requests
    from trino import dbapi
    from trino.exceptions import TrinoUserError
    from tests.unit.scrapers.test_transfermarkt_writer_safety import _trino_response

    posts = []
    class Admission(requests.Session):
        def send(self, request, **kwargs):
            assert request.method == 'POST'
            posts.append(request.body.decode())
            return _trino_response('q_delete', error={
                'errorName': 'QUERY_QUEUE_FULL', 'message': 'queue full',
                'errorCode': 1, 'errorType': 'USER_ERROR',
            })

    transport = native_writer._journal_connection(lambda: dbapi.connect(
        host='offline.invalid', user='test', http_session=Admission()))
    db = CoreDB()
    write(db, [membership_plan(['1'])])
    plan = _empty_roster_plan(clock=LATER)
    execute = CoreCursor.execute
    def reject_delete(self, sql, params=()):
        if sql.startswith('DELETE FROM ' + plan.full_table):
            return transport.cursor().execute(sql, params)
        return execute(self, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', reject_delete)
        with pytest.raises(TrinoUserError, match='q_delete'):
            write(db, [plan], run_id='empty', clock=LATER)
    assert len(posts) == 1 and posts[0].startswith("EXECUTE IMMEDIATE 'DELETE FROM ")
    key = (plan.full_table, native_writer._scope_json(plan))
    assert write_journal.statement_query(write_journal.pending_statement(key)) == 'q_delete'
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='empty')
    assert _players(db) == ['1']
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_delete', 'FAILED', 'QUERY_QUEUE_FULL')")
    verified, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='empty')
    assert verified['verified'] and _players(db) == [] and write_journal.pending_statement(key) is None


@pytest.mark.parametrize('conflict', ['body', 'clock'])
def test_referee_capture_evidence_survives_receipt_loss_on_another_key(conflict):
    # Round 8 finding 17. Even rows no longer owned by the incoming page
    # identify the response whose receipt disappeared.
    from tests.unit.scrapers.test_transfermarkt_sections import REFEREE_EXTRACTED_AT
    db = CoreDB()
    first, = _directory_plans('101', 'a' * 64)
    incoming, = _directory_plans('303', first.capture_id)
    if conflict == 'clock':
        body = first.rows[0]['source_body_hash']
        incoming = dataclasses_replace(
            incoming, captured_at=LATER,
            evidence=tuple((col, body if col == 'source_body_hash' else value)
                           for col, value in incoming.evidence),
            rows=tuple({**row, 'captured_at': LATER, 'observed_at': LATER,
                        'source_body_hash': body} for row in incoming.rows))
    write(db, [first], run_id=first.rows[0]['run_id'], clock=REFEREE_EXTRACTED_AT)
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    before = db.sql.execute(f'SELECT * FROM {first.table}').fetchall()
    writer = CoreWriter(db)
    if conflict == 'body':
        with pytest.raises(RuntimeError, match='replay differs'):
            native_writer.write_core_bundle(lambda: db, writer, [incoming], run_id=incoming.rows[0]['run_id'],
                                            universe_version='u', committed_at=LATER)
        assert not writer.writes and _receipt_count(db) == 0
        assert db.sql.execute(f'SELECT * FROM {first.table}').fetchall() == before
    else:
        # A clock is observation lineage, not a different response body.
        assert native_writer.write_core_bundle(
            lambda: db, writer, [incoming], run_id=incoming.rows[0]['run_id'],
            universe_version='u', committed_at=LATER)['verified'] is True
        assert db.sql.execute(
            f'SELECT referee_id, COUNT(*) FROM {first.table} GROUP BY referee_id'
        ).fetchall() == [('101', 5), ('303', 5)]


def test_queued_merge_from_before_single_submit_still_guards_its_scope():
    # Round 8 finding 5. r7's protocol bit on an anchored keyed intent did
    # not mean its target was fenced, even when no statement file survives.
    db = CoreDB()
    old = _transfer_plan('1', 'queued', CLOCK)
    fresh = _transfer_plan('1', 'corrected', LATER)
    _, digest = native_writer._bundle_identity([old], 'queued')
    write_journal.save_intent(
        {'run_id': 'queued', 'digest': digest, 'statement_protocol': 1},
        {'plans': native_writer._descriptors([old])}, run_id='queued', universe_version='u',
        committed_at=CLOCK)
    write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables([old]))
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [fresh], run_id='corrected', clock=LATER)
    assert not any(native_writer.is_mutation(sql) for sql in db.events)


def test_pre_upgrade_cleanup_id_cannot_release_an_insert():
    # Round 8 finding 2. The ID saved on an earlier attempt can belong to
    # DEALLOCATE. Even a definite rejection of that ID cannot authorize replay.
    db = CoreDB()
    key = (native_writer.CORE_RECEIPTS_TABLE, 'old-receipt')
    expected = native_writer._digest(('old-receipt',))
    old = write_journal.begin_statement(key, expected)
    write_journal.remember_query(old, 'q_deallocate')
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_deallocate', 'FAILED', 'QUERY_QUEUE_FULL')")
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer._settle_statement(db, key, expected, lambda: True)
    assert write_journal.pending_statement(key) == old


@pytest.mark.parametrize('mismatch', ['season', 'origin'])
def test_adoption_rechecks_the_saved_route_before_any_mutation(mismatch):
    # Round 8 finding 8
    from tests.unit.scrapers.test_transfermarkt_core import _scope

    body = (b'<html><body><h1 class="data-header__headline-wrapper">Empty FC</h1>'
            b'<p>no squad here</p></body></html>')
    digest = hashlib.sha256(body).hexdigest()
    capture = core.SourceCapture(_SQUAD_URL, 'squad', body, digest, digest, CLOCK)
    outcome, *_ = core.extract_squad_observations(capture, _scope(), extracted_at=LATER, universe_version='u')
    plan = core.plan_for_outcome(outcome, captured_at=CLOCK, scope_values={
        'league': 'GB1', 'season': '2526', 'club_id': '11'})
    assert plan.status == 'observed_empty'
    descriptor, = native_writer._descriptors([plan])
    del descriptor['pages']
    if mismatch == 'season':
        descriptor['scope_values'] = ('GB1', '2627', '11')
        # The old hash bound the scope but never validated it against the route.
        valid_url = _SQUAD_URL.replace('/2025/', '/2026/')
        plan = dataclasses_replace(plan, scope_values=descriptor['scope_values'], evidence=tuple(
            (k, valid_url if k == 'source_url' else v) for k, v in plan.evidence))
    else:
        descriptor['evidence'] = [(k, v.replace('www.', 'other.') if k == 'source_url' else v)
                                  for k, v in plan.evidence]
    _, legacy = _stage_b_identity(plan, 'run-old')
    path = write_journal.save_intent({'run_id': 'run-old', 'digest': legacy}, {'plans': [descriptor]},
                                     run_id='run-old', universe_version='u', committed_at=LATER)
    db = CoreDB()
    with pytest.raises(core.CoreParseError, match='row_scope_mismatch'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    with pytest.raises(core.CoreParseError, match='row_scope_mismatch'):
        native_writer.write_core_bundle(lambda: db, CoreWriter(db), [plan], run_id='run-old',
                                        universe_version='u', committed_at=LATER)
    assert path.exists() and not write_journal.read_snapshot_anchors(legacy)
    assert not write_journal._record_path('single-submit-adoptions', path.stem).exists()
    assert not db.events


@pytest.mark.parametrize('damage', [None, 'receipt', 'target'])
def test_completed_pre_fence_intent_only_verifies_its_original_ack(damage):
    # Round 8 finding 9
    db, plan = CoreDB(), _empty_roster_plan()
    original = write(db, [plan])
    path = write_journal.save_intent(
        {'run_id': 'run-1', 'digest': original['plan_sha256'], 'statement_protocol': 1},
        {'plans': native_writer._descriptors([plan])}, run_id='run-1',
        universe_version=original['universe_version'], committed_at=CLOCK)
    if damage == 'receipt':
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
    elif damage == 'target':
        db.sql.execute(f"INSERT INTO {plan.table} (league, season, club_id) VALUES ('GB1','2526','11')")
    start = len(db.events)
    if damage == 'target':
        with pytest.raises(RuntimeError, match='readback differs'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    else:
        assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db)) == [original]
        assert write(db, [plan], clock=NEWEST) == original
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [membership_plan(['1'], clock=LATER)], run_id='later', clock=LATER)
    # Round 11 finding 5. A missing immutable receipt alone can be inserted
    # after verifying the original physical proof, with its target guard intact.
    mutations = [sql for sql in db.events[start:] if native_writer.is_mutation(sql)]
    assert path.exists() and len(mutations) == int(damage == 'receipt')
    assert all(sql.startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE) for sql in mutations)
    assert _receipt_count(db) == 1


def test_existing_capture_sensitive_alias_repairs_original_receipt(monkeypatch):
    # Round 8 finding 10
    db, plan = CoreDB(), _empty_merge_plan()
    _, legacy = _stage_b_identity(plan, 'run-old')
    _legacy_intent(plan, legacy)
    record = write_journal.record_receipt
    with monkeypatch.context() as patch:
        # Published aliases originally had the current key and original warehouse
        # identity, before the redundant descriptor_digest field was introduced.
        patch.setattr(write_journal, 'record_receipt', lambda key, receipt: record(
            key, {k: v for k, v in receipt.items() if k != 'descriptor_digest'}))
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        native_writer.write_core_bundle(lambda: db, CoreWriter(db), [plan], run_id='run-old',
                                        universe_version='u', committed_at=NEWEST)
    current = native_writer._bundle_identity([plan], 'run-old')[1]
    alias = write_journal.read_receipt(current)
    assert alias['identity']['digest'] == legacy and write_journal.pending_intents({}) == []
    assert write_journal.read_snapshot_anchors(current) == write_journal.read_snapshot_anchors(legacy)
    before = db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall()
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    assert native_writer.write_core_bundle(lambda: db, CoreWriter(db), [plan], run_id='run-old',
                                          universe_version='u', committed_at=NEWEST) == alias
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall() == before


@pytest.mark.parametrize('empty_first', [True, False])
def test_single_page_order_survives_receipt_loss_and_refuses_ties(empty_first):
    # Round 8 finding 11
    db = CoreDB()
    empty, populated = _empty_roster_plan(), membership_plan(['1'])
    first, later = (empty, populated) if empty_first else (populated, empty)
    write(db, [first], run_id='first', clock=CLOCK)
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [later], run_id='collision', clock=CLOCK)
    assert _players(db) == ([] if empty_first else ['1'])
    newer = write(db, [later], run_id='later', clock=LATER)
    assert newer['plans'][0]['committed_at'] == LATER
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE run_id = ?', ('later',))
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [first], run_id='first', clock=NEWEST)
    assert _players(db) == (['1'] if empty_first else [])
    assert _receipt_count(db) == 1
    assert write(db, [later], run_id='later', clock=NEWEST) == newer


@pytest.mark.parametrize('scoped_proof', [True, False])
def test_finished_pre_fence_adoption_keeps_guard_after_intent_removal(monkeypatch, scoped_proof):
    # Round 9 finding 1
    db, plan = CoreDB(), _empty_roster_plan()
    with monkeypatch.context() as old_writer:
        if not scoped_proof:
            record = write_journal.record_receipt
            def old_receipt(digest, receipt):
                proof = {k: v for k, v in receipt['plans'][0].items()
                         if k in ('entity', 'table', 'rows', 'expected_rows', 'physical_hash', 'status_counts')}
                record(digest, {**receipt, 'plans': [proof]})
            old_writer.setattr(write_journal, 'record_receipt', old_receipt)
        written = write(db, [plan])
    original = write_journal.read_receipt(written['plan_sha256'])
    old = write_journal.save_intent(
        {'run_id': 'run-1', 'digest': original['plan_sha256']},
        {'plans': native_writer._descriptors([plan])}, universe_version=original['universe_version'],
        committed_at=CLOCK)
    write_journal.adopt_intent(old)
    write_journal._append_index({'digest': old.stem, 'status': 'finished'})
    old.unlink()
    assert write_journal.pending_intents({}) == []
    before = len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [membership_plan(['1'], clock=LATER)], run_id='new', clock=LATER)
    assert write(db, [plan]) == original
    assert not any(native_writer.is_mutation(sql) for sql in db.events[before:])
    db.cursor().execute(f'DELETE FROM {plan.full_table} WHERE {plan.scope_sql()}', plan.scope_values)
    assert _players(db) == [] and _receipt_count(db) == 1


def test_row_loss_cannot_witness_a_queued_replacement():
    # Round 9 finding 2
    db = CoreDB()
    wanted = membership_plan(['1'], clock=LATER, capture='wanted')
    extra = membership_plan(['2'], capture='earlier')
    native_writer.execute_statement(db.cursor(), wanted.ddl)
    native_writer._mutate_plan(db, CoreWriter(db), wanted)
    row = dict(extra.rows[0])
    db.sql.execute(f'INSERT INTO {wanted.table} ({", ".join(row)}) '
                   f'VALUES ({", ".join("?" for _ in row)})', tuple(row.values()))
    assert _players(db) == ['1', '2']
    queued = CoreWriter(db)
    def lost(*args, **kwargs):
        raise TrinoAmbiguousWriteError('queued replacement')
    queued._write_to_iceberg = lost
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, queued, [wanted], run_id='wanted',
                                        universe_version='u', committed_at=LATER)
    db.sql.execute(f"DELETE FROM {wanted.table} WHERE player_id = '2'")
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert _players(db) == ['1'] and _receipt_count(db) == 0


def test_new_rows_witness_a_strictly_additive_merge_without_query_history():
    # Round 9 finding 2
    db, plan = CoreDB(), membership_plan(['1'])

    class Applied(CoreWriter):
        def _write_to_iceberg(self, *args, **kwargs):
            super()._write_to_iceberg(*args, **kwargs)
            raise TrinoAmbiguousWriteError('response lost after commit')

    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, Applied(db), [plan], run_id='add',
                                        universe_version='u', committed_at=CLOCK)
    retry = CoreWriter(db)
    receipt, = native_writer.replay_pending_intents(lambda: db, retry)
    assert receipt['verified'] and not retry.writes and _players(db) == ['1']


@pytest.mark.parametrize('interruption', ['walk', 'walk-tie', 'receipt'])
def test_target_remains_fenced_until_its_ops_insert_lands(monkeypatch, interruption):
    # Round 9 finding 3
    db = CoreDB()
    if interruption != 'receipt':
        plan = _snapshot(CLOCK, 'first')
        competitor = _snapshot(NEWEST, 'second')
        if interruption == 'walk-tie':
            competitor = plan
            plan = dataclasses_replace(plan, rows=tuple(
                row for row in plan.rows if row['source_capture_id'] == plan.pages[1][1]))
        table = native_writer.CORE_PAGES_TABLE
    else:
        competitor = membership_plan(['1'])
        write(db, [competitor], run_id='competitor')
        plan = _empty_roster_plan(clock=LATER)
        table = native_writer.CORE_RECEIPTS_TABLE
    held = []
    execute = CoreCursor.execute
    def queue(self, sql, params=()):
        if sql.startswith('INSERT INTO ' + table):
            held.append((sql, tuple(params)))
            if interruption == 'receipt':
                self.query_id = 'q_ops_receipt'
                db.sql.execute("INSERT INTO test_query_history VALUES ('q_ops_receipt','QUEUED',NULL)")
            raise RuntimeError('queued ops insert')
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', queue)
        with pytest.raises(RuntimeError, match='queued ops insert'):
            write(db, [plan], run_id='queued', clock=LATER)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [competitor], run_id='competitor', clock=LATER if interruption == 'walk-tie' else NEWEST)
    assert len(held) == 1
    db.cursor().execute(*held[0])
    if interruption == 'receipt':
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_ops_receipt'")
    receipt, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='queued')
    assert receipt['verified']
    if interruption == 'walk':
        assert write(db, [competitor], run_id='competitor', clock=NEWEST)['verified']
    elif interruption == 'walk-tie':
        with pytest.raises(RuntimeError, match='replay differs'):
            write(db, [competitor], run_id='competitor', clock=LATER)
        assert db.sql.execute('SELECT row_count FROM transfermarkt_core_pages_v1 '
                              'WHERE page_number = 1').fetchall() == [(0,)]
    else:
        assert _players(db) == []
        with pytest.raises(StaleTransfermarktWrite):
            write(db, [competitor], run_id='competitor')


def test_finished_delete_requires_its_empty_postcondition(monkeypatch):
    # Round 9 finding 4
    import requests
    from trino import dbapi
    from trino.exceptions import TrinoConnectionError
    from tests.unit.scrapers.test_transfermarkt_writer_safety import _trino_response

    posts = []
    class Coordinator(requests.Session):
        def send(self, request, **kwargs):
            if request.method == 'GET':
                raise requests.ConnectionError('lost DELETE poll')
            if request.method == 'DELETE':
                response = _trino_response('q_delete')
                response.status_code = 204
                return response
            assert request.method == 'POST'
            posts.append(request.body.decode())
            return _trino_response('q_delete', nextUri='http://offline.invalid/results/q_delete')

    transport = native_writer._journal_connection(lambda: dbapi.connect(
        host='offline.invalid', user='test', http_session=Coordinator()))
    db = CoreDB()
    write(db, [membership_plan(['1'])])
    plan = _empty_roster_plan(clock=LATER)
    class DeleteCursor(CoreCursor):
        physical = None

        @property
        def query_id(self):
            return self.physical.query_id if self.physical is not None else None

        def execute(self, sql, params=()):
            if sql.startswith('DELETE FROM ' + plan.full_table):
                self.physical = transport.cursor()
                return self.physical.execute(sql, params)
            return super().execute(sql, params)

        def fetchall(self):
            return self.physical.fetchall() if self.physical is not None else super().fetchall()

        def close(self):
            if self.physical is not None:
                self.physical.close()
            super().close()

    with monkeypatch.context() as patch:
        patch.setattr(db, 'cursor', lambda: DeleteCursor(db))
        with pytest.raises(TrinoConnectionError):
            write(db, [plan], run_id='empty', clock=LATER)
    assert len(posts) == 1 and posts[0].startswith("EXECUTE IMMEDIATE 'DELETE FROM ")
    statement = write_journal.pending_statement((plan.full_table, native_writer._scope_json(plan)))
    assert write_journal.statement_query(statement) == 'q_delete'
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_delete', 'FINISHED', NULL)")
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    db.sql.execute(f'DELETE FROM {plan.table}')
    for state, code in [('QUEUED', None), ('FAILED', 'GENERIC_INTERNAL_ERROR')]:
        db.sql.execute('UPDATE test_query_history SET state = ?, error_code = ?', (state, code))
        with pytest.raises(RuntimeError, match='still pending'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        assert _receipt_count(db) == 1
    db.sql.execute("UPDATE test_query_history SET state = 'FINISHED', error_code = NULL")
    start = len(db.events)
    recovered, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert recovered['verified'] and _players(db) == [] and _receipt_count(db) == 2
    assert not any(sql.startswith('DELETE FROM') for sql in db.events[start:])


def test_filtered_replay_finishes_independent_player_before_reporting_pending():
    # Round 9 finding 5
    db = CoreDB()
    units = []
    for player in ('1', '2'):
        plan = _transfer_plan(player, 'capture' + player, CLOCK)
        hashes, digest = native_writer._bundle_identity([plan], 'batch')
        path = write_journal.save_intent(
            {'run_id': 'batch', 'digest': digest, 'statement_protocol': 1, 'single_submit': True},
            {'plans': native_writer._descriptors([plan])}, universe_version='u', committed_at=CLOCK)
        units.append((path, plan, native_writer._receipt_id(hashes[0], 'batch', digest)))
    (_, blocked, key), (_, ready, _) = sorted(units)
    native_writer.execute_statement(db.cursor(), blocked.ddl)
    write_journal.begin_statement((blocked.full_table, native_writer._scope_json(blocked)),
                                  {'single_submit': key}, observable=False)
    for completed in (1, 0):
        with pytest.raises(RuntimeError, match='still pending') as unresolved:
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='batch')
        assert len(unresolved.value.receipts) == completed
        assert write_journal.read_receipt(native_writer._bundle_identity([ready], 'batch')[1])['verified']
        assert [p for p, _ in write_journal.pending_intents({})] == [sorted(units)[0][0]]
        assert _receipt_count(db) == 1


@pytest.mark.parametrize('version', [6, 7])
def test_historical_walk_without_journal_cells_reverifies_all_cells(monkeypatch, version):
    # Round 9 finding 6
    db, plan = CoreDB(), _snapshot(CLOCK, 'original')
    digest = native_writer._page_digest
    with monkeypatch.context() as old:
        old.setattr(native_writer, '_page_digest', lambda p, page, rows, *args: digest(p, page, rows, version))
        record = write_journal.record_receipt
        old.setattr(write_journal, 'record_receipt', lambda key, receipt: record(
            key, {k: value for k, value in receipt.items() if k != 'walks'}))
        write(db, [plan])
    before = db.sql.execute('SELECT * FROM transfermarkt_core_pages_v1 ORDER BY page_number').fetchall()
    assert write(db, [plan])['verified']
    assert db.sql.execute('SELECT * FROM transfermarkt_core_pages_v1 ORDER BY page_number').fetchall() == before
    page = plan.pages[0]
    rows = native_writer._page_rows(plan, page[1])
    mixed = digest(plan, page, rows, 7 if version == 6 else 6)
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET rows_sha256 = ? WHERE page_number = 1', (mixed,))
    with pytest.raises(RuntimeError, match='manifest'):
        write(db, [plan])
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET rows_sha256 = ? WHERE page_number = 1',
                   (digest(plan, page, rows, version),))
    db.sql.execute("UPDATE transfermarkt_core_pages_v1 SET acknowledged_at = ? WHERE page_number = 1", (LATER,))
    with pytest.raises(RuntimeError, match='manifest'):
        write(db, [plan])


@pytest.mark.parametrize('conflict', ['body', 'clock', None])
def test_referee_capture_read_precedes_competition_filter(conflict):
    # Round 9 finding 7
    db = CoreDB()
    first, = _directory_plans('101', 'a' * 64)
    second, = _directory_plans('303' if conflict == 'body' else '101', first.capture_id)
    url = second.rows[0]['source_url'].replace('/XY1/', '/GB1/')
    stamp = LATER if conflict == 'clock' else second.captured_at
    second = dataclasses_replace(second, scope_values=('GB1', '2025'), captured_at=stamp,
        rows=tuple({**r, 'competition_id': 'GB1', 'source_url': url, 'observed_at': stamp,
                    'captured_at': stamp} for r in second.rows),
        evidence=tuple((c, url if c == 'source_url' else v) for c, v in second.evidence))
    write(db, [first])
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    if conflict == 'body':
        with pytest.raises(RuntimeError, match='replay differs'):
            write(db, [second])
        assert _receipt_count(db) == 0
    else:
        # The same body is a valid alias even at another observation clock.
        assert write(db, [second])['verified']
    competitions = db.sql.execute(f'SELECT DISTINCT competition_id FROM {first.table} ORDER BY 1').fetchall()
    assert competitions == ([('XY1',)] if conflict == 'body' else [('GB1',), ('XY1',)])


def test_pending_universe_mismatch_refused_before_connection():
    # Round 9 finding 11
    db, plan = CoreDB(), membership_plan(['1'])
    def unavailable():
        raise RuntimeError('offline')
    def commit(factory, universe):
        return native_writer.write_core_bundle(factory, CoreWriter(db), [plan], run_id='r',
                                              universe_version=universe, committed_at=CLOCK)
    with pytest.raises(RuntimeError, match='offline'):
        commit(unavailable, 'u')
    pending = write_journal.pending_intents({})
    with pytest.raises(RuntimeError, match='universe'):
        commit(lambda: pytest.fail('unexpected warehouse connection'), 'other')
    assert write_journal.pending_intents({}) == pending
    assert commit(lambda: db, 'u')['universe_version'] == 'u'


@pytest.mark.parametrize('different_body', [False, True])
def test_empty_club_capture_aliases_compare_body_only(different_body):
    # Round 9 finding 12
    capture = hashlib.sha256(b'<p>no squad here</p>').hexdigest()
    db, first = CoreDB(), _empty_roster_plan(capture=capture)
    first = dataclasses_replace(first, evidence=tuple(
        (k, capture if k == 'source_body_hash' else v) for k, v in first.evidence))
    evidence = dict(first.evidence)
    evidence['source_url'] = evidence['source_url'].replace('/verein/11/', '/verein/22/')
    if different_body:
        evidence['source_body_hash'] = 'a' * 64
    second = dataclasses_replace(first, scope_values=('GB1', '2526', '22'), captured_at=LATER,
                                 evidence=tuple(evidence.items()))
    if different_body:
        with pytest.raises(ValueError, match='capture evidence'):
            write(db, [first, second])
        assert db.events == [] and write_journal.pending_intents({}) == []
    else:
        assert write(db, [first, second])['verified']
        assert _receipt_count(db) == 2


def _historical_single_submit(plan, run, clock, *, finished):
    _, digest = native_writer._bundle_identity([plan], run)
    path = write_journal.save_intent(
        {'run_id': run, 'digest': digest, 'single_submit': True, 'statement_protocol': 1},
        {'plans': native_writer._descriptors([plan])}, universe_version='core-universe-v1',
        committed_at=clock)
    if finished:
        write_journal.finish_intent(path)
    return path


@pytest.mark.parametrize('scope_protocol', [False, True])
def test_finished_single_submit_walk_migrates_its_guard(monkeypatch, scope_protocol):
    # Round 10 finding 1
    db, old = CoreDB(), _snapshot(CLOCK, 'old')
    receipt = write(db, [old])
    identity = {'run_id': 'run-1', 'digest': receipt['plan_sha256'], 'single_submit': True,
                'statement_protocol': 1}
    if scope_protocol:
        identity['scope_protocol'] = 1
    path = write_journal.save_intent(identity, {'plans': native_writer._descriptors([old])})
    write_journal.finish_intent(path)
    fresh = _snapshot(NEWEST, 'fresh')
    persisted = write_journal._immutable_record
    def interrupt(path, value):
        persisted(path, value)
        if 'legacy-scopes' in path.parts:
            raise RuntimeError('after guard publication')
    before = len(db.events)
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_immutable_record', interrupt)
        with pytest.raises(RuntimeError, match='after guard publication'):
            write(db, [fresh], clock=NEWEST)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [fresh], clock=NEWEST)
    assert write(db, [old]) == receipt
    assert not any(native_writer.is_mutation(sql) for sql in db.events[before:])


@pytest.mark.parametrize('local_ack', [True, False])
def test_walk_order_uses_the_bound_receipt_clock(local_ack, monkeypatch):
    # Round 10 finding 2
    db, old = CoreDB(), _snapshot(CLOCK, 'first')
    late = '2026-11-21T12:35:00.000000+00:00'
    early = '2026-11-21T12:10:00.000000+00:00'
    record = write_journal.record_receipt
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', lambda key, receipt: record(
            key, {k: value for k, value in receipt.items() if k != 'walks'}))
        receipt = write(db, [old], clock=late)
    if local_ack:
        # Isolate the clock still present in the old local proof.
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
    else:
        write_journal._record_path('receipts', receipt['plan_sha256']).unlink()
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET acknowledged_at = ?', (early,))
    with pytest.raises(RuntimeError, match='acknowledgement clock'):
        write(db, [_snapshot(CLOCK, 'conflicting')], clock='2026-11-21T12:15:00.000000+00:00')
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, old), old.rows)


@pytest.mark.parametrize('interrupt_settlement', [False, True])
def test_bound_finished_delete_recovers_ahead_of_competing_anchored_intent(monkeypatch, interrupt_settlement):
    # Round 10 finding 3
    db = CoreDB()
    write(db, [membership_plan(['1'])])
    owner = _empty_roster_plan(clock=LATER)
    path = _historical_single_submit(owner, 'delete', LATER, finished=False)
    owner_hashes, owner_digest = native_writer._bundle_identity([owner], 'delete')
    write_journal.snapshot_anchors(owner_digest, db, native_writer._anchored_tables([owner]))
    execute = CoreCursor.execute
    def lose(self, sql, params=()):
        if sql.startswith('DELETE FROM ' + owner.full_table):
            self.query_id = 'q_delete'
            execute(self, sql, params)
            raise TrinoAmbiguousWriteError('lost DELETE poll')
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', lose)
        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    other = membership_plan(['2'], clock=NEWEST, capture='competing')
    competitor = _historical_single_submit(other, 'competing', NEWEST, finished=False)
    hashes, digest = native_writer._bundle_identity([other], 'competing')
    write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables([other]))
    other_id = native_writer._receipt_id(hashes[0], 'competing', digest)
    unknown = write_journal.begin_statement((native_writer.CORE_RECEIPTS_TABLE, other_id),
                                            {'single_submit': 'unknown-postimage'}, observable=False)
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_delete', 'FINISHED', NULL)")
    before = len(db.events)
    if interrupt_settlement:
        remember = write_journal.remember_terminal_target
        def interrupt(*args):
            remember(*args)
            raise RuntimeError('after terminal witness')
        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'remember_terminal_target', interrupt)
            with pytest.raises(RuntimeError, match='after terminal witness'):
                native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
        db.sql.execute('DELETE FROM test_query_history')
    with pytest.raises(RuntimeError, match='still pending') as unresolved:
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    recovered, = unresolved.value.receipts
    assert recovered['verified'] and not path.exists() and competitor.exists()
    owner_id = native_writer._receipt_id(owner_hashes[0], 'delete', owner_digest)
    assert write_journal.terminal_target(path.stem, owner_id)
    assert unresolved.value.unresolved == (competitor.stem,)
    assert write_journal.pending_statement(unknown['key']) == unknown
    assert any('test_query_history' in sql for sql in db.events[before:])
    assert not any(sql.startswith('DELETE FROM') for sql in db.events[before:])
    assert _players(db) == []


def test_latest_bound_walk_verifies_through_two_finished_guards():
    # Round 10 finding 4
    db = CoreDB()
    first, latest = _snapshot(CLOCK, 'first'), _snapshot(NEWEST, 'latest')
    write(db, [first], run_id='first')
    receipt = write(db, [latest], run_id='latest', clock=NEWEST)
    for plan, run, clock in ((first, 'first', CLOCK), (latest, 'latest', NEWEST)):
        _historical_single_submit(plan, run, clock, finished=True)
    start = len(db.events)
    assert write(db, [latest], run_id='latest', clock=NEWEST) == receipt
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [first], run_id='first')
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [_snapshot(NEWEST, 'third')], run_id='third', clock=NEWEST)
    assert len(list((write_journal._root() / 'legacy-scopes' / first.table).glob('*.json'))) == 2
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('boundary', ['reserving', 'between_members'])
def test_packet_reserves_both_players_before_dispatch(monkeypatch, boundary):
    # Round 10 finding 5
    db = CoreDB()
    packet = [_transfer_plan(player, 'initial-' + player, CLOCK) for player in ('1', '2')]
    calls = []
    name = 'hold_scope' if boundary == 'reserving' else '_write_plan'
    module = write_journal if boundary == 'reserving' else native_writer
    original = getattr(module, name)
    def interrupt(*args):
        if calls:
            raise RuntimeError('interrupted packet')
        calls.append(args)
        return original(*args)
    with monkeypatch.context() as patch:
        patch.setattr(module, name, interrupt)
        with pytest.raises(RuntimeError, match='interrupted packet'):
            write(db, packet)
    newer = _transfer_plan('2', 'new-2', LATER)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [newer], run_id='new', clock=LATER)
    result, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert result['verified'] and write(db, [newer], run_id='new', clock=LATER)['verified']
    assert write(db, [_transfer_plan('1', 'new-1', LATER)], run_id='next', clock=LATER)['verified']
    assert db.sql.execute(f'SELECT player_id, to_club_name FROM {newer.table} ORDER BY 1').fetchall() == [
        ('1', 'new-1'), ('2', 'new-2')]
    assert not write_journal.pending_intents({})


def _club_plan(club, capture, clock):
    plan = membership_plan([club], capture=capture, clock=clock)
    return dataclasses_replace(plan, scope_values=('GB1', '2526', club), rows=tuple({
        **r, 'club_id': club, 'source_url': r['source_url'].replace('/verein/11/', f'/verein/{club}/')
    } for r in plan.rows))


def test_recovery_finishes_partial_release_after_superseding_one_club(monkeypatch):
    # Round 10 finding 6
    db = CoreDB()
    packet = [_club_plan(c, 'old-' + c, CLOCK) for c in ('11', '22')]
    release = write_journal.release_scope
    def interrupt(scope, owner):
        release(scope, owner)
        raise RuntimeError('after one release')
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'release_scope', interrupt)
        with pytest.raises(RuntimeError, match='after one release'):
            write(db, packet)
    fresh = [_club_plan(c, 'new-' + c, LATER) for c in ('11', '22')]
    assert write(db, fresh[:1], run_id='new-11', clock=LATER)['verified']
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, fresh[1:], run_id='new-22', clock=LATER)
    before = list(db.events)
    recovered, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert recovered['verified'] and db.events == before
    assert write(db, fresh[1:], run_id='new-22', clock=LATER)['verified']
    with pytest.raises(StaleTransfermarktWrite):
        write(db, packet)
    assert not write_journal.pending_intents({})


@pytest.mark.parametrize('rules', [(6, 6), (7, 7), (6, 7), (7, 6)])
def test_foreign_bundle_requires_one_digest_rule_for_restored_walk(monkeypatch, rules):
    # Round 10 finding 7
    db, plan = CoreDB(), _snapshot(CLOCK, 'head')
    digest, journal_walks = native_writer._page_digest, native_writer._journal_walks
    def historical_digest(plan, page, rows, *args):
        return digest(plan, page, rows, rules[plan.pages.index(page)])
    def unversioned(plans, ids):
        walks = journal_walks(plans, ids)
        for walk in walks:
            del walk['digest_version']
        return walks
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_page_digest', historical_digest)
        patch.setattr(native_writer, '_journal_walks', unversioned)
        old = write(db, [plan], run_id='historical')
    key = old['walks'][0]['receipt_id']
    cells = native_writer._stored_walk(db, key)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE page_number = 2')
    cached = _other_cycle(plan, 'another-cycle')
    if rules[0] == rules[1]:
        assert write(db, [cached], run_id='later', clock=LATER)['verified']
        assert native_writer._stored_walk(db, key) == cells
    else:
        with pytest.raises(RuntimeError, match='replay differs'):
            write(db, [cached], run_id='later', clock=LATER)
        assert native_writer._stored_walk(db, key) == cells[:1]
        assert _receipt_count(db) == 1


def _referee_observation(plan, competition, clock):
    url = plan.rows[0]['source_url'].replace('/XY1/', '/' + competition + '/')
    return dataclasses_replace(plan, scope_values=(competition, '2025'), captured_at=clock,
        rows=tuple({**r, 'competition_id': competition, 'source_url': url, 'observed_at': clock,
                    'captured_at': clock} for r in plan.rows),
        evidence=tuple((c, url if c == 'source_url' else v) for c, v in plan.evidence))


@pytest.mark.parametrize('evidence', ['receipt', 'queued', 'lost_binding', 'historical_queue'])
def test_referee_body_is_bound_across_scopes_before_target_dispatch(evidence):
    # Round 10 finding 8
    db = CoreDB()
    first, = _directory_plans('101', 'a' * 64)
    conflict, = _directory_plans('303', first.capture_id)
    conflict = _referee_observation(conflict, 'GB1', LATER)
    key = (first.full_table, first.capture_id)
    if evidence == 'receipt':
        write(db, [first])
        db.sql.execute(f'DELETE FROM {first.table}')
        # A historical writer left only the warehouse witness.
        write_journal._record_path('capture-bodies', native_writer._digest(key)).unlink()
    else:
        class Queued(CoreWriter):
            dispatches_journalled_target = True
            def _write_to_iceberg(self, *args, **kwargs):
                self.land = lambda: CoreWriter._write_to_iceberg(self, *args, **kwargs)
                _submit_lost_merge(db)
        queued = Queued(db)
        if evidence == 'historical_queue':
            _historical_single_submit(first, 'run-1', CLOCK, finished=False)
        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.write_core_bundle(lambda: db, queued, [first], run_id='run-1',
                                            universe_version='core-universe-v1', committed_at=CLOCK)
        if evidence in ('historical_queue', 'lost_binding'):
            write_journal._record_path('capture-bodies', native_writer._digest(key)).unlink()
    rejected = CoreWriter(db)
    with pytest.raises(RuntimeError, match='replay differs'):
        native_writer.write_core_bundle(lambda: db, rejected, [conflict], run_id='conflicting',
                                        universe_version='u', committed_at=LATER)
    assert not rejected.writes
    if evidence != 'receipt':
        queued.land()
        db.sql.execute("UPDATE test_query_history SET state = 'FINISHED' WHERE query_id = 'q_lost_merge'")
        assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')[0]['verified']
    assert write(db, [first])['verified']
    assert db.sql.execute(f'SELECT DISTINCT source_body_hash FROM {first.table}').fetchall() == [
        (first.rows[0]['source_body_hash'],)]
    assert _receipt_count(db) == 1


def _squad_observation(entity, clock, capture='squad-body'):
    fields = {'contract_status': 'ok'} if entity == 'player_contract_observations' else {}
    row = _contract_row(entity, 'squad', _SQUAD_URL, capture=capture, clock=clock, run='squad',
                        ingested=LATER, league='GB1', season='2526', club_id='11', player_id='1', **fields)
    return NativeWritePlan(entity=entity, table=core.CORE_TABLES[entity], ddl=core.core_ddl(entity),
                           natural_keys=core.CORE_NATURAL_KEYS[entity], scope_columns=core.CORE_SCOPE_COLUMNS[entity],
                           scope_values=('GB1', '2526', '11'), rows=(row,), captured_at=clock,
                           write_mode='merge', status_reason='squad_page')


def test_accumulating_bundle_verifies_distinct_clocks_and_competitions_together():
    # Round 10 finding 14
    db = CoreDB()
    referee, = _directory_plans('101', 'a' * 64)
    plans = [referee, _referee_observation(referee, 'XY1', LATER),
             _referee_observation(referee, 'GB1', LATER)]
    for entity in ('player_attribute_observations', 'player_contract_observations'):
        plans.extend(_squad_observation(entity, clock) for clock in (CLOCK, LATER))
    receipt = write(db, plans)
    assert len(receipt['plans']) == _receipt_count(db) == 7
    assert write(db, plans) == receipt
    conflict = dataclasses_replace(plans[-1], rows=tuple({**row, 'source_body_hash': 'e' * 64}
                                                         for row in plans[-1].rows))
    with pytest.raises(ValueError, match='capture evidence'):
        write(db, [plans[-2], conflict])
    collision = _squad_observation(plans[-1].entity, LATER, capture='other')
    with pytest.raises(ValueError, match='natural key'):
        write(db, [plans[-1], collision])


@pytest.mark.parametrize('entity', ['player_attribute_observations', 'player_contract_observations'])
def test_newest_first_keeps_unseen_squad_observation_keys(entity):
    # Round 10 finding 15
    db = CoreDB()
    newer = _squad_observation(entity, LATER, 'newer')
    older = _squad_observation(entity, CLOCK, 'older')
    latest = write(db, [newer], run_id='latest', clock=LATER)
    historical = write(db, [older], run_id='history', clock=CLOCK)
    assert write(db, [newer], run_id='latest') == latest
    assert write(db, [older], run_id='history') == historical
    assert db.sql.execute(f'SELECT source_capture_id FROM {older.table} ORDER BY observed_at').fetchall() == [
        ('older',), ('newer',)]
    with pytest.raises(RuntimeError, match='natural key'):
        write(db, [_squad_observation(entity, CLOCK, 'collision')])


@pytest.mark.parametrize('damage', ['index', 'marker', 'one_guard', 'before_migration'])
def test_archived_guard_inventory_survives_loss(damage):
    # Round 11 finding 1. Both replacement tables belong to the archived unit.
    db = CoreDB()
    old = [_snapshot(CLOCK, 'first'), _club_plan('11', 'old-club', CLOCK)]
    receipt = write(db, old)
    identity = {'run_id': 'run-1', 'digest': receipt['plan_sha256'], 'single_submit': True}
    old_path = write_journal.save_intent(identity, {'plans': native_writer._descriptors(old)})
    write_journal.finish_intent(old_path)
    fresh = _club_plan('11', 'new-club', NEWEST)
    if damage != 'before_migration':
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, [fresh], run_id='fresh', clock=NEWEST)
    guard = write_journal._record_path('legacy-scopes/' + fresh.table, old_path.stem)
    if damage == 'marker':
        write_journal._record_path('migrated-scopes', old_path.stem).unlink()
    if damage in ('one_guard', 'index'):
        guard.unlink()
    if damage in ('index', 'before_migration'):
        write_journal._index_path().unlink()
    if damage == 'before_migration':
        # Only the old acknowledgement and its anchors survive this upgrade.
        for path in (write_journal._root() / 'finished-identities').glob('*.json'):
            path.unlink()
    start = len(db.events)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [fresh], run_id='fresh', clock=NEWEST)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, old) == receipt
    for member in old:
        assert list((write_journal._root() / 'legacy-scopes' / member.table).glob('*.json'))


def test_historical_named_page_clock_must_match_warehouse_receipt(monkeypatch):
    # Round 11 finding 2. A fresh sibling hides the regressed, rowless replacement.
    from tests.unit.scrapers import test_transfermarkt_sections as lanes
    db = CoreDB()
    # Name page one unambiguously at a later clock than the other populated page.
    old = _snapshot(LATER, 'named')
    record = write_journal.record_receipt
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', lambda k, r: record(
            k, {key: val for key, val in r.items() if key != 'walks'}))
        write(db, [old], clock=LATER)
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET captured_at=? WHERE source_capture_id=?',
                   (EARLIER, old.capture_id))
    start, tail, page = _walk_pages()
    stale = lanes._page(start, lanes.ROWLESS_PAGE_1, CLOCK, 'stale-empty')
    fresh = page(tail, 'page_2.html', NEWEST, 'fresh-sibling')
    with pytest.raises(RuntimeError, match='named capture'):
        write(db, [lanes._attendance_plan((stale, fresh))], run_id='incoming', clock=NEWEST)
    assert _receipt_count(db) == 1
    assert native_writer._target_matches(db, old)


@pytest.mark.parametrize('interrupt_ack', [False, True])
def test_archived_guard_requires_terminal_delete_before_ack(monkeypatch, interrupt_ack):
    # Round 11 finding 3. Losing rows independently cannot settle q_delete.
    db = CoreDB()
    old = membership_plan(['1'])
    write(db, [old])
    owner = _empty_roster_plan(clock=LATER)
    path = _historical_single_submit(owner, 'delete', LATER, finished=False)
    execute = CoreCursor.execute
    submissions = []
    def queue(self, sql, params=()):
        if sql.startswith('DELETE FROM ' + owner.full_table):
            submissions.append(sql)
            self.query_id = 'q_delete'
            db.sql.execute("INSERT INTO test_query_history VALUES ('q_delete','QUEUED',NULL)")
            raise TrinoAmbiguousWriteError('queued DELETE')
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', queue)
        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    _historical_single_submit(old, 'run-1', CLOCK, finished=True)
    db.sql.execute('DELETE FROM ' + owner.table)
    _, digest = native_writer._bundle_identity([owner], 'delete')
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    assert path.exists() and write_journal.read_receipt(digest) is None
    assert _receipt_count(db) == 1
    statement = write_journal.pending_statement(native_writer._target_scope(owner))
    assert write_journal.statement_query(statement) == 'q_delete'
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_delete'")
    if interrupt_ack:
        record = write_journal.record_receipt
        def interrupt(key, receipt):
            record(key, receipt)
            raise RuntimeError('after local acknowledgement')
        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'record_receipt', interrupt)
            with pytest.raises(RuntimeError, match='after local acknowledgement'):
                native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
        db.sql.execute('DELETE FROM test_query_history')
    recovered, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    assert recovered == write_journal.read_receipt(digest) and not path.exists()
    assert len(submissions) == 1 and _receipt_count(db) == 2
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [_club_plan('11', 'third', NEWEST)], run_id='third', clock=NEWEST)


def test_disjoint_retries_cannot_certify_mixed_rule_walk(monkeypatch):
    # Round 11 finding 4. Each candidate has original rows for only one page.
    from tests.unit.scrapers import test_transfermarkt_sections as lanes
    db = CoreDB()
    start, tail, page = _walk_pages()
    head = page(start, 'page_1.html', CLOCK, 'original-head')
    end = page(tail, 'page_2.html', CLOCK, 'original-tail')
    plan = lanes._attendance_plan((head, end))
    digest, walks = native_writer._page_digest, native_writer._journal_walks
    def old_digest(plan, cell, rows, *args):
        return digest(plan, cell, rows, 6 if cell[0] == start else 7)
    def old_walks(plans, keys):
        return [{k: v for k, v in walk.items() if k != 'digest_version'} for walk in walks(plans, keys)]
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_page_digest', old_digest)
        patch.setattr(native_writer, '_journal_walks', old_walks)
        receipt = write(db, [plan])
    db.sql.execute('DELETE FROM ' + plan.table)
    retries = [(head, page(tail, 'page_2.html', LATER, 'next-tail')),
               (page(start, 'page_1.html', LATER, 'next-head'), end)]
    for index, pages in enumerate(retries):
        with pytest.raises(RuntimeError, match='original row evidence'):
            write(db, [lanes._attendance_plan(pages)], run_id=str(index), clock=LATER)
    assert _receipt_count(db) == 1
    assert not write_journal._record_path('verified-walk-rules', receipt['walks'][0]['receipt_id']).exists()


def _historical_packet(plans, *, clock=CLOCK, run='run-1'):
    hashes, digest = native_writer._bundle_identity(plans, run)
    path = write_journal.save_intent(
        {'run_id': run, 'digest': digest, 'statement_protocol': 1, 'single_submit': True, 'scope_protocol': 1},
        {'plans': native_writer._descriptors(plans)}, committed_at=clock, universe_version='core-universe-v1')
    return path, native_writer._receipt_id(hashes[0], run, digest)


def test_partial_old_reservation_fences_other_member():
    # Round 11 finding 6. Scope protocol 1 predates atomic packet reservation.
    db = CoreDB()
    packet = [_transfer_plan(player, 'first-' + player, CLOCK) for player in ('1', '2')]
    path, owner = _historical_packet(packet)
    write_journal.hold_scope(native_writer._target_scope(packet[0]), owner)
    fresh = _transfer_plan('2', 'next', LATER)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [fresh], run_id='next', clock=LATER)
    assert not db.events and path.exists()
    assert write_journal.scope_owner(native_writer._target_scope(packet[1])) == owner
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')[0]['verified']
    assert write(db, [fresh], run_id='next', clock=LATER)['verified']


@pytest.mark.parametrize('old_dml_uncertain', [False, True])
def test_old_post_ack_partial_release_finishes_after_supersession(monkeypatch, old_dml_uncertain):
    # Round 11 finding 7. The older release sequence predates completed-intents.
    db = CoreDB()
    packet = [_club_plan(club, 'old-' + club, CLOCK) for club in ('11', '22')]
    path, owner = _historical_packet(packet)
    if old_dml_uncertain:
        _, digest = native_writer._bundle_identity(packet, 'run-1')
        write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables(packet))
    release = write_journal.release_scope
    def interrupt(scope, owner):
        release(scope, owner)
        raise RuntimeError('old writer stopped after release')
    with monkeypatch.context() as old:
        old.setattr(write_journal, 'complete_intent', lambda *args: None)
        old.setattr(write_journal, 'release_scope', interrupt)
        with pytest.raises(RuntimeError, match='stopped after release'):
            write(db, packet)
    assert write_journal.scope_owner(native_writer._target_scope(packet[1])) == owner
    first = _club_plan('11', 'new-11', LATER)
    if old_dml_uncertain:
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, [first], run_id='new-11', clock=LATER)
    else:
        assert write(db, [first], run_id='new-11', clock=LATER)['verified']
    start = len(db.events)
    result, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert result['verified'] and not path.exists() and len(db.events) == start
    assert write_journal.scope_owner(native_writer._target_scope(packet[1])) is None
    fresh = _club_plan('22', 'new-22', LATER)
    if old_dml_uncertain:
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, [fresh], run_id='new-22', clock=LATER)
    else:
        assert write(db, [fresh], run_id='new-22', clock=LATER)['verified']


@pytest.mark.parametrize('entity', ['player_attribute_observations', 'player_contract_observations'])
@pytest.mark.parametrize('damage', ['different_key', 'drifted_clock'])
def test_observation_proof_counts_whole_capture_at_clock(entity, damage):
    # Round 11 finding 9. An extra player must not disappear in a key filter.
    db = CoreDB()
    first = _squad_observation(entity, CLOCK)
    first = dataclasses_replace(first, rows=tuple({**first.rows[0], 'player_id': str(i)} for i in range(38)))
    write(db, [first])
    extra = {**first.rows[0], 'player_id': 'extra'}
    if damage == 'drifted_clock':
        extra['captured_at'] = EARLIER
    columns = ', '.join(extra)
    marks = ', '.join('?' for _ in extra)
    db.sql.execute(f'INSERT INTO {first.table} ({columns}) VALUES ({marks})', tuple(extra.values()))
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [first], run_id='retry')
    assert db.sql.execute(f'SELECT COUNT(*) FROM {first.table}').fetchone() == (39,)
    assert _receipt_count(db) == 1


def test_rowless_observation_clocks_share_body_and_proof():
    # Round 11 finding 11. Each absence is a distinct immutable observation.
    db = CoreDB()
    plans = [p for clock in (CLOCK, LATER) for p in _empty_squad_plans(clock)[1:]]
    written = write(db, plans)
    assert [p['rows'] for p in written['plans']] == [0] * 4
    assert _receipt_count(db) == 4 and write(db, plans) == written
    with pytest.raises(ValueError, match='receipt identity'):
        write(db, [plans[0], plans[0]])
    with pytest.raises(ValueError, match='duplicate core capture'):
        write(db, [plans[0], dataclasses_replace(plans[2], proof='different evidence')])
    evidence = {**dict(plans[2].evidence), 'source_body_hash': 'd' * 64}
    with pytest.raises(ValueError, match='capture evidence'):
        write(db, [plans[0], dataclasses_replace(plans[2], evidence=tuple(evidence.items()))])


@pytest.mark.parametrize('lost_anchors', [False, True])
def test_missing_archived_ack_is_confined_to_bound_targets(lost_anchors):
    # Round 11 finding 12. The bound anchor lists the whole attendance packet.
    db = CoreDB()
    attendance = _snapshot(CLOCK, 'archived')
    ack = write(db, [attendance])
    _historical_single_submit(attendance, 'run-1', CLOCK, finished=True)
    path = write_journal._record_path('receipts', ack['plan_sha256'])
    anchors = write_journal._record_path('anchors', ack['plan_sha256'])
    original = anchors.read_bytes()
    if lost_anchors:
        anchors.unlink()
    path.unlink()
    assert write(db, [_club_plan('11', 'squad', LATER)], clock=LATER)['verified']
    with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
        write(db, [_snapshot(LATER, 'next')], run_id='next', clock=LATER)
    write_journal.record_receipt(ack['plan_sha256'], ack)
    if lost_anchors:
        anchors.write_bytes(original)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [_snapshot(LATER, 'next')], run_id='next', clock=LATER)
    assert write(db, [attendance]) == ack


def test_receipt_only_repair_replays_under_archived_guard(monkeypatch):
    # Round 11 finding 5. The repair intent survives independently of target ownership.
    db, plan = CoreDB(), _snapshot(CLOCK, 'original')
    ack = write(db, [plan])
    archive = _historical_single_submit(plan, 'run-1', CLOCK, finished=True)
    expected = db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall()
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    execute, queued = CoreCursor.execute, []
    def delay(self, sql, params=()):
        if sql.startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE):
            queued.append((sql, tuple(params)))
            self.query_id = 'q_repair'
            raise TrinoAmbiguousWriteError('delayed receipt repair')
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', delay)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
        with pytest.raises(RuntimeError, match='still pending'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    submission, = queued
    db.cursor().execute(*submission)
    # Round 12 finding 5. Identical receipt cells can be restored independently.
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_repair', 'QUEUED', NULL)")
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall() == expected
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_repair'")
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1') == [ack]
    assert not write_journal.pending_intents({})
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS}').fetchall() == expected
    assert write_journal._record_path('legacy-scopes/' + plan.table, archive.stem).exists()
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [_snapshot(NEWEST, 'fresh')], run_id='fresh', clock=NEWEST)


def _queue_empty_packet(db, monkeypatch, plans):
    execute = CoreCursor.execute
    queued = []

    def queue(cursor, sql, params=()):
        if sql.startswith('DELETE FROM ' + plans[0].full_table):
            queued.append((sql, tuple(params)))
            cursor.query_id = 'q_roster_delete'
            db.sql.execute("INSERT INTO test_query_history VALUES ('q_roster_delete', 'QUEUED', NULL)")
            raise TrinoAmbiguousWriteError('queued roster DELETE')
        return execute(cursor, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', queue)
        with pytest.raises(TrinoAmbiguousWriteError, match='queued roster DELETE'):
            write(db, plans, run_id='delete', clock=LATER)
    submission, = queued
    return submission


def test_lost_statement_recovers_the_bound_queued_delete(monkeypatch):
    # Round 12 finding 1
    db, plans = CoreDB(), _empty_squad_plans()
    write(db, [membership_plan(['1'])])
    submission = _queue_empty_packet(db, monkeypatch, plans)
    scope = native_writer._target_scope(plans[0])
    statement = write_journal.pending_statement(scope)
    write_journal._statement_path(scope).unlink()
    db.sql.execute('DELETE FROM ' + plans[0].table)
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    assert write_journal.pending_statement(scope) == statement
    assert write_journal.statement_query(statement) == 'q_roster_delete'
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [membership_plan(['2'], clock=NEWEST, capture='replacement')], run_id='later', clock=NEWEST)
    assert _receipt_count(db) == 1 and list(write_journal.bundle_reservations())
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_roster_delete'")
    result, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    assert result['verified'] and _receipt_count(db) == 4
    assert write_journal.pending_statement(scope) is None


@pytest.mark.parametrize('lost_warehouse', [False, True])
def test_orphan_anchor_fences_replacement_after_local_history_loss(lost_warehouse):
    # Round 12 finding 2
    db, plan = CoreDB(), _snapshot(CLOCK, 'historical')
    ack = write(db, [plan])
    write_journal._index_path().unlink()
    write_journal._record_path('receipts', ack['plan_sha256']).unlink()
    for path in (write_journal._root() / 'finished-identities').glob('*.json'):
        path.unlink()
    if lost_warehouse:
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
        db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    before = len(db.events)
    with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
        write(db, [_snapshot(NEWEST, 'fresh')], run_id='fresh', clock=NEWEST)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[before:])
    assert native_writer._target_matches(db, plan)
    assert write(db, [_club_plan('11', 'unrelated', NEWEST)], run_id='squad', clock=NEWEST)['verified']


def test_foreign_rowless_page_clock_needs_independent_walk(monkeypatch):
    # Round 12 finding 3
    from tests.unit.scrapers import test_transfermarkt_sections as lanes

    db = CoreDB()
    start, tail, page = _walk_pages()
    old = lanes._attendance_plan((lanes._page(start, lanes.ROWLESS_PAGE_1, LATER, 'old-empty'),
                                  page(tail, 'page_2.html', SIBLING_CLOCK, 'named-tail')))
    assert old.capture_id == old.pages[1][1]
    record = write_journal.record_receipt
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', lambda key, ack: record(
            key, {k: v for k, v in ack.items() if k != 'walks'}))
        write(db, [old], clock=LATER)
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET captured_at=? WHERE page_number=1', (EARLIER,))
    incoming = lanes._attendance_plan((page(start, 'page_1.html', CLOCK, 'older-head'),
                                       page(tail, 'page_2.html', NEWEST, 'newer-tail')))
    with pytest.raises(StaleTransfermarktWrite, match='independent page clocks'):
        write(db, [incoming], run_id='incoming', clock=NEWEST)
    assert _receipt_count(db) == 1 and native_writer._target_matches(db, old)


def test_finished_delete_settles_historical_packet_with_rowless_merges(monkeypatch):
    # Round 12 finding 4
    db, plans = CoreDB(), _empty_squad_plans()
    old = [membership_plan(['1']), *_empty_squad_plans(CLOCK)[1:]]
    ack = write(db, old)
    path, owner = _historical_packet(plans, run='delete', clock=LATER)
    submission = _queue_empty_packet(db, monkeypatch, plans)
    archived = write_journal.save_intent({'run_id': 'run-1', 'digest': ack['plan_sha256']},
                                         {'plans': native_writer._descriptors(old)})
    write_journal.finish_intent(archived)
    db.cursor().execute(*submission)
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_roster_delete'")
    result, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')
    assert [proof['rows'] for proof in result['plans']] == [0, 0, 0]
    assert write_journal.read_receipt(result['plan_sha256']) == result and not path.exists()
    assert write_journal.terminal_target(path.stem, owner)
    assert _receipt_count(db) == 6 and not list(write_journal.bundle_reservations())


@pytest.mark.parametrize('anchored', [False, True])
def test_replay_after_last_historical_owner_unlink(monkeypatch, anchored):
    # Round 12 finding 6
    db = CoreDB()
    packet = [_club_plan(club, 'old-' + club, CLOCK) for club in ('11', '22')]
    path, _ = _historical_packet(packet)
    _, digest = native_writer._bundle_identity(packet, 'run-1')
    if anchored:
        write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables(packet))
    last_scope = native_writer._target_scope(packet[-1])
    release = write_journal.release_scope

    def interrupt(scope, owner):
        release(scope, owner)
        if scope == last_scope:
            raise RuntimeError('last owner unlinked')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'complete_intent', lambda *args: None)
        patch.setattr(write_journal, 'release_scope', interrupt)
        with pytest.raises(RuntimeError, match='last owner unlinked'):
            write(db, packet)
    assert path.exists() and all(write_journal.scope_owner(native_writer._target_scope(p)) is None for p in packet)
    if not anchored:
        assert write(db, [_club_plan('22', 'next', NEWEST)], run_id='next', clock=NEWEST)['verified']
    start = len(db.events)
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1') == [
        write_journal.read_receipt(digest)]
    assert not path.exists() and len(db.events) == start
    if anchored:
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, [_club_plan('22', 'next', NEWEST)], run_id='next', clock=NEWEST)


def _attendance_edition(plan, edition):
    def url(value):
        return value.replace('/2025', '/' + edition)
    return dataclasses_replace(
        plan, scope_values=('GB1', edition),
        rows=tuple({**row, 'edition_id': edition, 'source_url': url(row['source_url'])} for row in plan.rows),
        evidence=tuple((k, url(v) if k == 'source_url' else v) for k, v in plan.evidence),
        pages=tuple((url(start), capture, clock, body, url(following))
                    for start, capture, clock, body, following in plan.pages))


def test_missing_ack_and_conflicting_restore_are_scoped_to_archived_edition():
    # Round 12 finding 7
    db, plan = CoreDB(), _snapshot(CLOCK, 'old')
    ack = write(db, [plan])
    _historical_single_submit(plan, 'run-1', CLOCK, finished=True)
    path = write_journal._record_path('receipts', ack['plan_sha256'])
    original = path.read_bytes()
    path.unlink()
    other = _attendance_edition(plan, '2024')
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET scope_values_json=?', (native_writer._scope_json(other),))
    with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
        write(db, [other], run_id='other')
    db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET scope_values_json=?', (native_writer._scope_json(plan),))
    assert write(db, [other], run_id='other')['verified']
    fresh = _snapshot(NEWEST, 'next')
    with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
        write(db, [fresh], run_id='next', clock=NEWEST)
    path.write_bytes(original)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [fresh], run_id='next', clock=NEWEST)
    path.unlink()
    write_journal.record_receipt(ack['plan_sha256'], {**ack, 'committed_at': NEWEST})
    with pytest.raises(RuntimeError, match='acknowledgement differs'):
        write(db, [fresh], run_id='next', clock=NEWEST)
    assert write(db, [_club_plan('11', 'independent', NEWEST)], run_id='squad', clock=NEWEST)['verified']


def test_single_page_run_refuses_a_warehouse_only_walk_after_journal_rollback(tmp_path):
    # Round 13 finding 2
    from tests.unit.scrapers import test_transfermarkt_sections as lanes

    db = CoreDB()
    scope = lanes.CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t10', cycle_id='tm-cycle-t10', edition_id='2025',
        attendance_editions=(lanes.AttendanceEditionTarget('GB1', 2025, 'premier-league'),))
    body = (lanes.ATTENDANCE_FIXTURES / 'single_page.html').read_bytes()

    def run(name, clock, writer=None):
        fetcher = lanes._SectionFetch({lanes.ATTENDANCE_START: body + name.encode()})

        def fetch(*args, **kwargs):
            return dataclasses_replace(fetcher.fetch(*args, **kwargs), raw_fetched_at=clock)

        return lanes.CoreRun(fetch, universe_version=lanes.UNIVERSE, extracted_at=clock).run(
            scope, writer=writer or CoreWriter(db), connection_factory=lambda: db)

    assert run('A', EARLIER).receipts[0]['verified']
    root = write_journal._root()
    backup = tmp_path / 'after-a'
    shutil.copytree(root, backup)
    table = core.CORE_TABLES['attendance_observations']
    db.sql.execute(f'CREATE TABLE walk_a AS SELECT * FROM {table}')
    ack = run('B', CLOCK).receipts[0]
    db.sql.execute(f'DELETE FROM {table}')
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM walk_a')
    repair = _QueuedReplacement(db)
    with pytest.raises(TrinoAmbiguousWriteError):
        run('B', CLOCK, repair)
    root.rename(tmp_path / 'in-flight-b')
    shutil.copytree(backup, root)
    with pytest.raises(StaleTransfermarktWrite, match='local dispatch history'):
        run('C', NEWEST)
    assert db.sql.execute(f'SELECT * FROM {table}').fetchall() == db.sql.execute('SELECT * FROM walk_a').fetchall()
    assert _receipt_count(db) == 2
    assert write(db, [_club_plan('77', 'independent', NEWEST)], clock=NEWEST)['verified']
    root.rename(tmp_path / 'rolled-back-journal')
    (tmp_path / 'in-flight-b').rename(root)
    repair.land()
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id=ack['run_id']) == [ack]
    assert db.sql.execute(f'SELECT COUNT(*), COUNT(DISTINCT club_id) FROM {table}').fetchone() == (2, 2)


@pytest.mark.parametrize('damage', ['copied_ack', 'descriptor', 'run', 'owner'])
def test_misfiled_terminal_ack_cannot_restore_superseded_roster(damage):
    # Round 13 finding 3
    db = CoreDB()
    old = write(db, [membership_plan(['1'], clock=EARLIER, capture='old')], clock=EARLIER)
    roster = membership_plan([str(i) for i in range(38)])
    write(db, [roster])
    empty = write(db, _empty_squad_plans(), run_id='empty', clock=LATER)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE run_id=? AND entity=?', ('empty', roster.entity))
    path = write_journal._record_path('receipts', empty['plan_sha256'])
    correct = path.read_bytes()
    entry, = [entry for entry in write_journal.guard_history() if entry['identity'].get('run_id') == 'empty']
    completion_path = write_journal._record_path('completed-intents', entry['digest'])
    completion = write_journal._read_record(completion_path)
    # The deployed terminal record had no independent hash of the acknowledgement.
    legacy = {k: completion[k] for k in ('intent', 'digest', 'terminal', 'owner')}
    completion_path.unlink()
    write_journal._immutable_record(completion_path, legacy)
    if damage == 'owner':
        completion_path.unlink()
        write_journal._immutable_record(completion_path, {**legacy, 'owner': 'other-owner'})
    elif damage == 'copied_ack':
        path.write_bytes(write_journal._record_path('receipts', old['plan_sha256']).read_bytes())
    else:
        change = {'descriptor_digest': old['descriptor_digest']} if damage == 'descriptor' else {'run_id': 'run-1'}
        path.unlink()
        write_journal.record_receipt(path.stem, {**empty, **change})
    before = len(db.events)
    with pytest.raises(RuntimeError, match='acknowledgement differs'):
        write(db, [roster])
    assert _players(db) == [] and not any(native_writer.is_mutation(sql) for sql in db.events[before:])
    path.write_bytes(correct)
    completion_path.unlink()
    write_journal._immutable_record(completion_path, legacy)
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [roster])


def _archived_delete_with_terminal_outcome(db, monkeypatch):
    plans = _empty_squad_plans()
    old = [membership_plan(['1']), *_empty_squad_plans(CLOCK)[1:]]
    ack = write(db, old)
    path, owner = _historical_packet(plans, run='delete', clock=LATER)
    submission = _queue_empty_packet(db, monkeypatch, plans)
    archived = write_journal.save_intent({'run_id': 'run-1', 'digest': ack['plan_sha256']},
                                         {'plans': native_writer._descriptors(old)})
    write_journal.finish_intent(archived)
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_roster_delete'")
    return plans, path, owner


def test_owner_replay_settles_bound_delete_with_two_pending_descriptors(monkeypatch):
    db = CoreDB()
    write(db, [membership_plan(['1'])])
    plans = _empty_squad_plans()
    submission = _queue_empty_packet(db, monkeypatch, plans)
    (current, body), = write_journal.pending_intents({'run_id': 'delete'})
    digest = body['identity']['digest']
    assert body['identity']['packet_attempt'] and write_journal.read_snapshot_anchors(digest)
    owner = native_writer._receipt_id(plans[0].plan_sha256(), 'delete', digest)

    older = _empty_squad_plans(CLOCK)
    archive, older_owner = _historical_packet(older)
    _, older_digest = native_writer._bundle_identity(older, 'run-1')
    write_journal.snapshot_anchors(older_digest, db, native_writer._anchored_tables(older))
    native_writer._release_packet(plans, owner, ())
    scopes = [native_writer._target_scope(plan) for plan in plans]
    statement = write_journal.pending_statement(scopes[0])
    assert statement['expected'] == {'single_submit': owner} and owner != older_owner
    assert all(write_journal.scope_owner(scope) is None for scope in scopes)
    assert not list(write_journal.bundle_reservations())

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')

    db.cursor().execute(*submission)
    with pytest.raises(RuntimeError, match='statement outcome is still pending'):
        replay()
    assert write_journal.pending_statement(scopes[0]) == statement
    assert not write_journal.terminal_target(current.stem, owner)
    assert write_journal.read_receipt(digest) is None and _receipt_count(db) == 1

    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_roster_delete'")
    receipt, = replay()
    witness = write_journal.terminal_witness(current.stem, owner, statement)
    assert witness['query_id'] == 'q_roster_delete' and witness['outcome'] == 'FINISHED'
    assert receipt['verified'] and receipt == write_journal.read_receipt(digest)
    assert write_journal.pending_statement(scopes[0]) is None and _receipt_count(db) == 4
    assert not current.exists() and archive.exists() and write_journal.read_receipt(older_digest) is None


def test_historical_receipt_repair_requires_finished_before_first_publication(monkeypatch):
    # Round 13 finding 4
    db = CoreDB()
    plans, path, owner = _archived_delete_with_terminal_outcome(db, monkeypatch)
    _, digest = native_writer._bundle_identity(plans, 'delete')

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')

    def interrupt(*args):
        raise RuntimeError('before local publication')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', interrupt)
        with pytest.raises(RuntimeError, match='before local publication'):
            replay()
    assert _receipt_count(db) == 6 and write_journal.terminal_target(path.stem, owner)
    db.sql.execute(f'CREATE TABLE original_receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    execute, queued = CoreCursor.execute, []

    def delay(cursor, sql, params=()):
        if sql.startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE):
            queued.append((sql, tuple(params)))
            cursor.query_id = 'q_historical_receipt'
            db.sql.execute("INSERT INTO test_query_history VALUES ('q_historical_receipt','QUEUED',NULL)")
            raise TrinoAmbiguousWriteError('queued historical INSERT')
        return execute(cursor, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', delay)
        with pytest.raises(TrinoAmbiguousWriteError):
            replay()
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM original_receipt')
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
    assert path.exists() and write_journal.read_receipt(digest) is None
    key = (native_writer.CORE_RECEIPTS_TABLE, owner)
    assert write_journal.statement_query(write_journal.pending_statement(key)) == 'q_historical_receipt'
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_historical_receipt'")
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    submission, = queued
    db.cursor().execute(*submission)
    assert replay()[0]['verified'] and not path.exists() and _receipt_count(db) == 6
    assert write_journal.pending_statement(key) is None


@pytest.mark.parametrize('old_observable_fence', [False, True])
def test_current_packet_receipt_needs_finished_and_exact_cells_before_publication(monkeypatch, old_observable_fence):
    # Round 14 finding 2
    db = CoreDB()
    plans = [membership_plan(['1']), _club_plan('42', 'sibling', CLOCK)]

    def interrupt(*args):
        raise RuntimeError('before local publication')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', interrupt)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, plans)
    (path, body), = write_journal.pending_intents({'run_id': 'run-1'})
    digest = body['identity']['digest']
    assert body['identity']['packet_attempt'] and write_journal.read_receipt(digest) is None
    key = native_writer._receipt_id(plans[0].plan_sha256(), 'run-1', digest)
    fence = (native_writer.CORE_RECEIPTS_TABLE, key)
    assert not write_journal.terminal_target(path.stem, key)
    expected = db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall()
    assert len(expected) == 2
    db.sql.execute(f'CREATE TABLE saved_receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (key,))
    delete = f'DELETE FROM {RECEIPTS} WHERE receipt_id=?'
    db.sql.execute(delete, (key,))

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    execute, submissions = CoreCursor.execute, []

    def queue(cursor, sql, params=()):
        if sql.startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE):
            submissions.append((sql, tuple(params)))
            cursor.query_id = 'q_current_receipt'
            db.sql.execute("INSERT INTO test_query_history VALUES ('q_current_receipt','QUEUED',NULL)")
            raise TrinoAmbiguousWriteError('lost receipt INSERT response')
        return execute(cursor, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', queue)
        if old_observable_fence:
            # The deployed writer marked current-packet INSERTs observable.
            begin = write_journal.begin_statement

            def begin_prior_statement(statement_key, expected, **options):
                if statement_key == fence:
                    options['observable'] = True
                return begin(statement_key, expected, **options)

            patch.setattr(write_journal, 'begin_statement', begin_prior_statement)
        with pytest.raises(TrinoAmbiguousWriteError):
            replay()
        statement = write_journal.pending_statement(fence)
        assert statement['observable'] is old_observable_fence
        assert write_journal.statement_query(statement) == 'q_current_receipt'
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_receipt')
        assert db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall() == expected
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        db.sql.execute(delete, (key,))
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_current_receipt'")
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        assert write_journal.pending_statement(fence) == statement
        assert path.exists() and write_journal.read_receipt(digest) is None
    submission, = submissions
    restored = db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_receipt').lastrowid
    db.cursor().execute(*submission)
    assert db.sql.execute(f'SELECT COUNT(*) FROM {RECEIPTS} WHERE receipt_id=?', (key,)).fetchone() == (2,)
    with pytest.raises(RuntimeError, match='receipt readback differs'):
        replay()
    assert write_journal.pending_statement(fence) == statement and write_journal.read_receipt(digest) is None
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE rowid=?', (restored,))
    ack, = replay()
    assert ack['verified'] and write_journal.read_receipt(digest) == ack
    assert not path.exists() and write_journal.pending_statement(fence) is None
    assert db.sql.execute(f'SELECT * FROM {RECEIPTS} ORDER BY receipt_id').fetchall() == expected


@pytest.mark.parametrize('conflict', [False, True])
def test_lost_ack_after_last_unlink_fences_only_the_archived_clubs(monkeypatch, conflict):
    # Round 13 finding 5
    db = CoreDB()
    donor = write(db, [_club_plan('99', 'donor', CLOCK)], run_id='donor')
    packet = [_club_plan(c, 'old-' + c, CLOCK) for c in ('11', '42')]
    path, owner = _historical_packet(packet)
    _, digest = native_writer._bundle_identity(packet, 'run-1')
    write_journal.snapshot_anchors(digest, db, native_writer._anchored_tables(packet))

    def stop(path):
        raise RuntimeError('after final unlink')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'complete_intent', lambda *args: None)
        patch.setattr(write_journal, 'finish_intent', stop)
        with pytest.raises(RuntimeError, match='after final unlink'):
            write(db, packet)
    ack = write_journal._record_path('receipts', digest)
    saved = ack.read_bytes()
    ack.unlink()
    if conflict:
        ack.write_bytes(write_journal._record_path('receipts', donor['plan_sha256']).read_bytes())
    scopes = [native_writer._target_scope(p) for p in packet]
    assert all(write_journal.scope_owner(scope) is None for scope in scopes)
    assert all(write_journal.pending_statement(scope) is None for scope in scopes)
    with pytest.raises(RuntimeError, match='acknowledgement|receipt plan_sha256'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert not write_journal.completed_intent(path.stem)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, [_club_plan('42', 'later', NEWEST)], run_id='later', clock=NEWEST)
    assert path.exists() and native_writer._target_matches(db, packet[1])
    assert write(db, [_club_plan('77', 'writable', NEWEST)], run_id='other', clock=NEWEST)['verified']
    ack.write_bytes(saved)
    assert native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')[0]['verified']
    assert not path.exists()


@pytest.mark.parametrize('legacy', [False, True])
def test_terminal_delete_witness_recovers_only_its_missing_query_mapping(monkeypatch, legacy):
    # Round 13 finding 6
    db = CoreDB()
    plans, path, owner = _archived_delete_with_terminal_outcome(db, monkeypatch)
    scope = native_writer._target_scope(plans[0])
    remember = write_journal.remember_terminal_target

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='delete')

    def interrupt(*args):
        remember(*args)
        raise RuntimeError('after terminal witness')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'remember_terminal_target', interrupt)
        with pytest.raises(RuntimeError, match='after terminal witness'):
            replay()
    statement = write_journal.pending_statement(scope)
    assert write_journal.terminal_target(path.stem, owner, statement)
    if legacy:
        witness = write_journal._record_path(f'terminal-targets/{path.stem}/{owner}', statement['attempt'])
        original = write_journal._read_record(witness)
        witness.unlink()
        write_journal._immutable_record(witness, {k: original[k] for k in ('intent', 'receipt_id', 'attempt', 'query_id')})
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    mapping.unlink()
    write_journal.remember_query(statement, 'q_unrelated')
    with pytest.raises(RuntimeError, match='terminal target binding differs'):
        replay()
    mapping.unlink()
    db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id='q_roster_delete'")
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert not mapping.exists() and path.exists()
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_roster_delete'")
    db.sql.execute(f"INSERT INTO {plans[0].table} (league,season,club_id) VALUES ('GB1','2526','11')")
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert not mapping.exists()
    db.sql.execute(f'DELETE FROM {plans[0].table}')
    if not legacy:
        db.sql.execute("DELETE FROM test_query_history WHERE query_id='q_roster_delete'")
    assert replay()[0]['verified'] and not path.exists() and _receipt_count(db) == 6
    assert write_journal.pending_statement(scope) is None


def test_conflicting_archived_ack_before_scope_migration_is_confined():
    # Round 13 finding 7
    db = CoreDB()
    donor = write(db, [_club_plan('11', 'donor', CLOCK)], run_id='donor')
    plan = _snapshot(CLOCK, 'archive')
    ack = write(db, [plan])
    archive = _historical_single_submit(plan, 'run-1', CLOCK, finished=True)
    assert not write_journal._record_path('guard-inventories', archive.stem).exists()
    path = write_journal._record_path('receipts', ack['plan_sha256'])
    donor_bytes = write_journal._record_path('receipts', donor['plan_sha256']).read_bytes()
    path.write_bytes(donor_bytes)
    fresh = _snapshot(NEWEST, 'next')
    with pytest.raises(RuntimeError, match='acknowledgement differs'):
        write(db, [fresh], run_id='next', clock=NEWEST)
    assert write(db, [_attendance_edition(fresh, '2024')], run_id='another-edition', clock=NEWEST)['verified']
    assert write(db, [_club_plan('77', 'squad', NEWEST)], run_id='squad', clock=NEWEST)['verified']
    assert native_writer._target_matches(db, plan) and path.read_bytes() == donor_bytes


@pytest.mark.parametrize('damage', ['none', 'changed_row', 'missing_sibling'])
def test_missing_single_club_ack_requires_complete_physical_member(damage):
    # Round 13 finding 8
    from tests.unit.scrapers.test_transfermarkt_core import _squad_outcomes

    db = CoreDB()
    outcome = _squad_outcomes()[0]
    values = {col: outcome.rows[0][col] for col in core.CORE_SCOPE_COLUMNS[outcome.entity]}
    plan = core.plan_for_outcome(outcome, captured_at=outcome.capture.captured_at, scope_values=values)
    assert len(plan.rows) == 38 and values['club_id'] == '132785'
    packet = [plan]
    if damage == 'missing_sibling':
        packet.append(_club_plan('42', 'sibling', CLOCK))
    ack = write(db, packet)
    path, _ = _historical_packet(packet)
    write_journal.finish_intent(path)
    receipt = write_journal._record_path('receipts', ack['plan_sha256'])
    saved = receipt.read_bytes()
    receipt.unlink()
    if damage == 'missing_sibling':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE plan_sha256=?', (packet[1].plan_sha256(),))
    elif damage == 'changed_row':
        db.sql.execute(f'UPDATE {plan.table} SET player_name=? WHERE player_id=?',
                       ('changed', plan.rows[0]['player_id']))
    other = _club_plan('77', 'disjoint', NEWEST)
    other = dataclasses_replace(other, scope_values=('GB1', '2627', '77'), rows=tuple({
        **row, 'season': '2627', 'source_url': row['source_url'].replace('/2025/', '/2026/')
    } for row in other.rows))
    if damage == 'none':
        assert write(db, [other], run_id='77', clock=NEWEST)['verified']
        with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
            write(db, [plan], run_id='another')
        assert native_writer._target_matches(db, plan)
    else:
        with pytest.raises(RuntimeError, match='acknowledgement is still pending'):
            write(db, [other], run_id='77', clock=NEWEST)
    receipt.write_bytes(saved)
    assert write(db, [other], run_id='77', clock=NEWEST)['verified']


def _interrupt_ops_insert(patch, db, table, query_id, *, landed=False, member=None):
    """Keep the exact bound INSERT for delivery after a writer restart."""
    submissions = []
    execute = CoreCursor.execute

    def interrupt(cursor, sql, params=()):
        if sql.startswith('INSERT INTO ' + table) and (member is None or params[0] == member):
            submissions.append((sql, tuple(params)))
            if landed:
                execute(cursor, sql, params)
            cursor.query_id = query_id
            db.sql.execute('INSERT INTO test_query_history VALUES (?, ?, NULL)',
                           (query_id, 'FINISHED' if landed else 'QUEUED'))
            raise TrinoAmbiguousWriteError('lost INSERT response')
        return execute(cursor, sql, params)

    patch.setattr(CoreCursor, 'execute', interrupt)
    return submissions


def _interrupt_publication(*args):
    raise RuntimeError('before local publication')


@pytest.mark.parametrize('earlier', ['target', 'receipt'])
def test_finished_original_operation_cannot_publish_a_queued_target_repair(tmp_path, monkeypatch, earlier):
    # Round 15 finding 2
    db = CoreDB()
    plan, old = _snapshot(CLOCK, 'B'), _snapshot(EARLIER, 'A')
    write(db, [old], run_id='A', clock=EARLIER)
    db.sql.execute(f'CREATE TABLE saved_old AS SELECT * FROM {old.table}')

    class OriginalTarget(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            write_rows = super()._write_to_iceberg
            _submit_lost_merge(db, lambda: write_rows(*args, **kwargs), query_id='original_target')

    def commit(writer):
        return native_writer.write_core_bundle(lambda: db, writer, [plan], run_id='B',
                                               universe_version='u', committed_at=CLOCK)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='B')

    with monkeypatch.context() as patch:
        if earlier == 'receipt':
            _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'original_receipt', landed=True)
        with pytest.raises(TrinoAmbiguousWriteError):
            commit(OriginalTarget(db) if earlier == 'target' else CoreWriter(db))
    root = write_journal._root()
    backup = tmp_path / 'original-finished'
    shutil.copytree(root, backup)
    ack, = replay()
    _, digest = native_writer._bundle_identity([plan], 'B')
    db.sql.execute(f'CREATE TABLE saved_target AS SELECT * FROM {plan.table}')
    db.sql.execute(f'DELETE FROM {plan.table}')
    db.sql.execute(f'INSERT INTO {plan.table} SELECT * FROM saved_old')
    delayed = _QueuedReplacement(db)
    with pytest.raises(TrinoAmbiguousWriteError):
        commit(delayed)
    db.sql.execute(f'DELETE FROM {plan.table}')
    db.sql.execute(f'INSERT INTO {plan.table} SELECT * FROM saved_target')
    root.rename(tmp_path / 'repair-pending')
    shutil.copytree(backup, root)
    for _ in range(2):
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            replay()
        assert write_journal.read_receipt(digest) is None
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [_snapshot(NEWEST, 'C')], run_id='C', clock=NEWEST)
    # Independent restoration and the already accepted original can themselves
    # produce duplicates. Recovery must leave them visible and refuse publication.
    delayed.land()
    assert db.sql.execute(f'SELECT count(*) FROM {plan.table}').fetchone()[0] > len(plan.rows)
    with pytest.raises(RuntimeError):
        replay()
    assert write_journal.read_receipt(digest) is None


@pytest.mark.parametrize('historical', [False, True])
@pytest.mark.parametrize('earlier_finished', [False, True])
def test_receipt_repair_survives_restoring_prepublication_backup(tmp_path, monkeypatch, historical, earlier_finished):
    # Round 15 finding 3
    db = CoreDB()
    if historical:
        plans, path, member = _archived_delete_with_terminal_outcome(db, monkeypatch)
        run = 'delete'
    else:
        plans, run = [membership_plan(['1']), _club_plan('42', 'sibling', CLOCK)], 'run-1'
        path = None
    _, digest = native_writer._bundle_identity(plans, run)
    member = native_writer._receipt_id(plans[0].plan_sha256(), run, digest)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id=run)

    root = write_journal._root()
    backup = tmp_path / 'pre-publication'
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        if earlier_finished:
            execute = native_writer._execute_once

            def snapshot_original(cursor, sql, bound, statement):
                execute(cursor, sql, bound, statement)
                if statement and statement['key'] == [native_writer.CORE_RECEIPTS_TABLE, member]:
                    write_journal.remember_query(statement, 'earlier_insert')
                    db.sql.execute("INSERT INTO test_query_history VALUES ('earlier_insert','FINISHED',NULL)")
                    shutil.copytree(root, backup)

            patch.setattr(native_writer, '_execute_once', snapshot_original)
        with pytest.raises(RuntimeError, match='before local publication'):
            if historical:
                replay()
            else:
                write(db, plans)
    if not earlier_finished:
        shutil.copytree(root, backup)
    db.sql.execute(f'CREATE TABLE saved_member AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'replacement', member=member)
        with pytest.raises(TrinoAmbiguousWriteError):
            replay()
    root.rename(tmp_path / 'repair')
    shutil.copytree(backup, root)
    if earlier_finished:
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_member')
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert write_journal.read_receipt(digest) is None
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='replacement'")
    assert db.sql.execute(f'SELECT count(*) FROM {RECEIPTS} WHERE receipt_id=?', (member,)).fetchone() == (
        2 if earlier_finished else 1,)
    with pytest.raises(RuntimeError):
        replay()
    assert write_journal.read_receipt(digest) is None
    if not earlier_finished:
        root.rename(tmp_path / 'older')
        (tmp_path / 'repair').rename(root)
        assert replay()[0]['verified'] and (path is None or not path.exists())


def test_prepublication_page_repair_requires_its_own_finished_insert(monkeypatch):
    # Round 15 finding 4
    db, plan = CoreDB(), _snapshot(CLOCK, 'two-pages')
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    table = 'transfermarkt_core_pages_v1'
    db.sql.execute(f'CREATE TABLE saved_pages AS SELECT * FROM {table}')
    db.sql.execute(f'DELETE FROM {table}')

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, 'repair_pages')
        with pytest.raises(TrinoAmbiguousWriteError):
            replay()
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_pages')
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='repair_pages'")
    assert db.sql.execute(f'SELECT page_number, count(*) FROM {table} GROUP BY page_number '
                          'ORDER BY page_number').fetchall() == [(1, 2), (2, 2)]
    with pytest.raises(RuntimeError):
        replay()
    _, digest = native_writer._bundle_identity([plan], 'run-1')
    assert write_journal.read_receipt(digest) is None


@pytest.mark.parametrize('boundary', ['retention', 'unlink'])
def test_verified_receipt_survives_retention_and_unlink(monkeypatch, boundary):
    # Round 15 finding 6
    db, plan = CoreDB(), membership_plan(['1'])
    _, digest = native_writer._bundle_identity([plan], 'run-1')
    with monkeypatch.context() as patch:
        writes = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'verified_receipt', landed=True)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    outcome, finish = native_writer._query_outcome, write_journal.finish_statement
    lookups = []

    def expire(connection, statement, **options):
        result = outcome(connection, statement, **options)
        if statement['key'][0] == native_writer.CORE_RECEIPTS_TABLE:
            lookups.append(result)
            db.sql.execute("DELETE FROM test_query_history WHERE query_id='verified_receipt'")
        return result

    def after_unlink(statement):
        finish(statement)
        if statement['key'][0] == native_writer.CORE_RECEIPTS_TABLE:
            raise RuntimeError('after receipt unlink')

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_query_outcome', expire)
        if boundary == 'unlink':
            patch.setattr(write_journal, 'finish_statement', after_unlink)
            with pytest.raises(RuntimeError, match='after receipt unlink'):
                replay()
        else:
            assert replay()[0]['verified']
    assert lookups == ['finished'] and len(writes) == 1
    if boundary == 'unlink':
        member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
        assert write_journal.pending_statement((native_writer.CORE_RECEIPTS_TABLE, member)) is None
        db.sql.execute(f'CREATE TABLE exact AS SELECT * FROM {RECEIPTS}')
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        assert _receipt_count(db) == 0 and write_journal.read_receipt(digest) is None
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM exact')
        assert replay()[0]['verified']
    assert _receipt_count(db) == 1 and not write_journal.pending_intents({})
