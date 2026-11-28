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
    assert row['observation_status'] == 'observed_present' and row['parser_revision'] == 'attendance-v2'
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

    mappings = _capture_returned_queries(monkeypatch, db, [native_writer.CORE_RECEIPTS_TABLE])
    monkeypatch.setattr(write_journal, 'record_receipt', crash)
    with pytest.raises(RuntimeError, match='crash before'):
        native_writer.write_core_bundle(
            lambda: db, CoreWriter(db), plans, run_id='r', universe_version='u', committed_at=CLOCK)
    monkeypatch.setattr(write_journal, 'record_receipt', real)
    for path, contents in mappings.items():
        path.write_bytes(contents)
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


@pytest.mark.parametrize('interrupted', [False, True])
def test_a_lost_historical_walk_is_restored_from_its_acknowledgement(monkeypatch, interrupted):
    # Round 29 finding 2
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
    if interrupted:
        _r28_interrupt(monkeypatch, write_journal, 'retire_operations',
                       lambda: write(db, [fresh], run_id='run-c', clock=fresh.captured_at))
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
            mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
            patch.setattr(write_journal, 'record_receipt', crash)
            with pytest.raises(RuntimeError, match='lost local acknowledgement'):
                native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        for mapping, contents in mappings.items():
            mapping.write_bytes(contents)
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
    write_journal.start_submissions(native_writer._receipt_id(owner_hashes[0], 'delete', owner_digest))
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
    assert recovered['verified'] and db.events[len(before):]
    assert not any(native_writer.is_mutation(sql) for sql in db.events[len(before):])
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
        write_journal.start_submissions(owner)
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
    assert result['verified'] and not path.exists() and db.events[start:]
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
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
    path, owner = _historical_packet(packet)
    _, digest = native_writer._bundle_identity(packet, 'run-1')
    if anchored:
        write_journal.start_submissions(owner)
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
            _submit_legacy_unpublished_repair(db, run='delete')
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
            _submit_legacy_unpublished_repair(db)
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
    write_journal.start_submissions(owner)
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


def _capture_returned_queries(patch, db, tables):
    """Preserve genuine coordinator responses at the statement cleanup boundary."""
    execute, unlink = CoreCursor.execute, write_journal._unlink_record
    mappings = {}

    def with_query(cursor, sql, params=()):
        result = execute(cursor, sql, params)
        if any(sql.startswith('INSERT INTO ' + table) for table in tables):
            cursor.query_id = f'returned_{len(mappings)}'
            db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FINISHED', NULL)", (cursor.query_id,))
        return result

    def snapshot(path):
        if path.parent.name == 'statement-queries' and path.exists():
            mappings[path] = path.read_bytes()
        unlink(path)

    patch.setattr(CoreCursor, 'execute', with_query)
    patch.setattr(write_journal, '_unlink_record', snapshot)
    return mappings


def _submit_legacy_unpublished_repair(db, *, run='run-1', pages=False):
    """Reproduce a POST already made by r16, before r17 required operator review."""
    (path, body), = write_journal.pending_intents({'run_id': run})
    plans, hashes, digest = native_writer._intent_unit(body)
    ids = [native_writer._receipt_id(h, run, digest) for h in hashes]
    clock = body['evidence']['committed_at']
    with native_writer.writer_lock(), native_writer.journalled_write():
        native_writer._LOCAL.packet = (ids[0], [path], dict(zip(ids, hashes)), [])
        if pages:
            plan, = plans
            native_writer._record_pages(db, plan, ids[0], clock, True)
        else:
            receipts = [native_writer._plan_receipt(
                plan, native_writer._physical_proof(db, plan, clock), h, digest, run,
                body['evidence']['universe_version'], native_writer._batch_id_for_run(run), clock)
                for plan, h in zip(plans, hashes, strict=True)]
            native_writer._merge_receipts(db, receipts)


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
            _submit_legacy_unpublished_repair(db, run=run)
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
            _submit_legacy_unpublished_repair(db, pages=True)
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


@pytest.mark.parametrize('boundary', ['retention', 'unlink', 'restored_query', 'legacy_query'])
def test_verified_receipt_survives_retention_and_unlink(monkeypatch, boundary):
    # Round 15 finding 6
    db, plan = CoreDB(), membership_plan(['1'])
    _, digest = native_writer._bundle_identity([plan], 'run-1')
    with monkeypatch.context() as patch:
        writes = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'verified_receipt', landed=True)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    statement = write_journal.pending_statement(key)
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    saved = mapping.read_bytes()
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
        if boundary != 'retention':
            patch.setattr(write_journal, 'finish_statement', after_unlink)
            with pytest.raises(RuntimeError, match='after receipt unlink'):
                replay()
        else:
            assert replay()[0]['verified']
    assert lookups == ['finished'] and len(writes) == 1
    if boundary != 'retention':
        assert write_journal.pending_statement(key) is None
        if boundary in ('restored_query', 'legacy_query'):
            # Round 16 finding 6
            write_journal._immutable_record(mapping, {'query_id': 'different'})
            with pytest.raises(RuntimeError, match='still pending'):
                replay()
            other = _snapshot(LATER, 'disjoint')
            assert write(db, [other], run_id='disjoint', clock=LATER)['verified']
            assert mapping.exists() and write_journal.read_receipt(digest) is None
            mapping.unlink()
            if boundary == 'restored_query':
                mapping.write_bytes(saved)
            else:
                write_journal._immutable_record(mapping, {'query_id': 'verified_receipt'})
            assert write(db, [other], run_id='disjoint', clock=LATER)['verified']
        db.sql.execute(f'CREATE TABLE exact AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (member,))
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        assert not native_writer._stored_receipts(db, [member]) and write_journal.read_receipt(digest) is None
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM exact')
        assert replay()[0]['verified']
    assert len(native_writer._stored_receipts(db, [member])) == 1 and not write_journal.pending_intents({})
    assert not mapping.exists()


@pytest.mark.parametrize('legacy_marker', [False, True])
def test_restored_settlement_marker_preserves_queued_receipt_attempt(monkeypatch, legacy_marker):
    # Round 16 finding 3
    db, plan = CoreDB(), membership_plan(['7'])
    hashes, digest = native_writer._bundle_identity([plan], 'run-1')
    member = native_writer._receipt_id(hashes[0], 'run-1', digest)
    scope = native_writer._target_scope(plan)
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    marker = write_journal._record_path('settled-statements', write_journal._statement_path(key).stem)
    unlink = write_journal._unlink_record
    saved = []

    def keep_marker(path):
        if path == marker:
            saved.append(path.read_bytes())
        unlink(path)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', keep_marker)
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    db.sql.execute(f'CREATE TABLE original AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, key[0], 'replacement')
        with pytest.raises(TrinoAmbiguousWriteError):
            _submit_legacy_unpublished_repair(db)
    latest = write_journal.latest_operation(scope, key, member)
    assert latest['state'] == 'dispatched'
    original_marker, = saved
    marker.write_bytes(original_marker)
    if legacy_marker:
        original_statement = write_journal._read_record(marker)['statement']
        marker.unlink()
        write_journal._immutable_record(marker, original_statement)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM original')
    with pytest.raises(RuntimeError, match='settled statement binding differs'):
        replay()
    assert write_journal.latest_operation(scope, key, member) == latest
    marker.unlink()  # the queued attempt had no settlement marker
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert write_journal.pending_statement(key) == latest['statement']
    assert write_journal.statement_query(latest['statement']) == 'replacement'
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='replacement'")
    with pytest.raises(RuntimeError, match='receipt readback differs'):
        replay()
    assert _receipt_count(db) == 2 and write_journal.read_receipt(digest) is None


def _representation_packet(players, clock):
    from tests.unit.scrapers import test_transfermarkt_sections as lanes

    capture = lanes._representation_capture({
        'success': True, 'data': [{'id': p, 'attributes': {'consultantAgencyId': 55},
                                   'clubAssignments': [{'clubId': 11, 'type': 'current'}]} for p in players]})
    capture = dataclasses_replace(capture, captured_at=clock,
                                  url=lanes.REPRESENTATION_URL.partition('?')[0] + '?' +
                                  '&'.join('ids[]=' + p for p in players))
    outcomes = lanes.sections.extract_representation_observations(
        capture, lanes._representation_scope(), expected_ids=players, extracted_at=clock,
        universe_version=lanes.UNIVERSE)
    return [core.plan_for_outcome(o, captured_at=clock, scope_values={'player_id': p})
            for o, p in zip(outcomes, players, strict=True)]


@pytest.mark.parametrize('boundary', ['queued', 'between_members'])
def test_pre_reservation_restore_fences_entire_representation_packet(tmp_path, monkeypatch, boundary):
    # Round 16 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root, backup = write_journal._root(), tmp_path / 'pre-reservation'
    reserve, write_plan = write_journal.reserve_scopes, native_writer._write_plan
    land = []

    def before_reservation(scopes, owner):
        shutil.copytree(root, backup)
        return reserve(scopes, owner)

    def between(*args):
        if args[2] == plans[1]:
            raise RuntimeError('between players')
        return write_plan(*args)

    class Queued(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            apply = super()._write_to_iceberg
            land.append(lambda: apply(*args, **kwargs))
            _submit_lost_merge(db)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_scopes', before_reservation)
        if boundary == 'queued':
            with pytest.raises(TrinoAmbiguousWriteError):
                native_writer.write_core_bundle(lambda: db, Queued(db), plans, run_id='packet',
                                               universe_version='u', committed_at=CLOCK)
        else:
            patch.setattr(native_writer, '_write_plan', between)
            with pytest.raises(RuntimeError, match='between players'):
                native_writer.write_core_bundle(lambda: db, CoreWriter(db), plans, run_id='packet',
                                               universe_version='u', committed_at=CLOCK)
    scopes = [native_writer._target_scope(plan) for plan in plans]
    statement = write_journal.pending_statement(scopes[0])
    owner = write_journal.scope_owner(scopes[0])
    root.rename(tmp_path / 'in-flight')
    shutil.copytree(backup, root)
    assert all(write_journal.scope_owner(s) is None for s in scopes)
    competitor = _representation_packet(('9',), LATER)
    start = len(db.events)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, competitor, run_id='another', clock=LATER)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert all(write_journal.scope_owner(s) == owner for s in scopes)
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='packet')

    if boundary == 'queued':
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            replay()
        write_journal._immutable_record(write_journal._attempt_path(scopes[0]), statement)
        write_journal.remember_query(statement, 'q_lost_merge')
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        deliver, = land
        deliver()
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_lost_merge'")
    result, = replay()
    assert result['verified'] and write(db, competitor, run_id='another', clock=LATER)['verified']
    assert not write_journal.pending_intents({})


def _journal_pair():
    root = write_journal._root()
    return root, root.with_name(root.name + '.attempts')


def _backup_pair(directory):
    directory.mkdir()
    for tree in _journal_pair():
        if tree.exists():
            shutil.copytree(tree, directory / tree.name)


def _restore_pair(directory, displaced):
    displaced.mkdir()
    for tree in _journal_pair():
        if tree.exists():
            tree.rename(displaced / tree.name)
        if (directory / tree.name).exists():
            shutil.copytree(directory / tree.name, tree)


@pytest.mark.parametrize('historical', [False, True])
@pytest.mark.parametrize('boundary', ['absent', 'older-id', 'older-finished-witness'])
def test_pair_restore_fences_squad_receipt(tmp_path, monkeypatch, historical, boundary):
    # Round 16 finding 2
    db = CoreDB()
    if historical:
        write(db, [membership_plan(['1']), *_empty_squad_plans(CLOCK)[1:]])
        plans, run, clock = _empty_squad_plans(), 'delete', LATER
    else:
        plans, run, clock = [membership_plan(['1']), _club_plan('42', 'sibling', CLOCK)], 'run-1', CLOCK
    hashes, digest = native_writer._bundle_identity(plans, run)
    member = native_writer._receipt_id(hashes[0], run, digest)
    backup = tmp_path / 'original'
    publish, execute, save = write_journal.record_receipt, native_writer._execute_once, write_journal.save_intent

    def historical_save(identity, payload, **evidence):
        return save({k: v for k, v in identity.items() if k not in ('packet_attempt', 'submission_protocol')}, payload, **evidence)

    def before_publication(*args):
        if boundary == 'absent':
            _backup_pair(backup)
        return publish(*args)

    def before_return(cursor, sql, params, statement):
        execute(cursor, sql, params, statement)
        if historical and sql.startswith('DELETE FROM ' + plans[0].full_table):
            write_journal.remember_query(statement, 'original_delete')
            db.sql.execute("INSERT INTO test_query_history VALUES ('original_delete','FINISHED',NULL)")
            assert native_writer._target_matches(db, plans[0])
            write_journal.remember_terminal_target(statement['operation']['intent'], member, statement)
            write_journal.verified_operation(statement, 'original_delete')
        if statement and statement['key'] == [native_writer.CORE_RECEIPTS_TABLE, member]:
            write_journal.remember_query(statement, 'original_receipt')
            db.sql.execute("INSERT INTO test_query_history VALUES ('original_receipt','FINISHED',NULL)")
            if boundary == 'older-finished-witness':
                expected = native_writer._statement_expected(statement)
                assert native_writer._settle_statement(
                    db, statement['key'], expected,
                    lambda: native_writer._digest(native_writer._stored_receipts(db, [member])[member]) == expected,
                    require_finished=True, release=False)
            if boundary != 'absent':
                _backup_pair(backup)

    with monkeypatch.context() as patch:
        if historical:
            patch.setattr(write_journal, 'save_intent', historical_save)
        patch.setattr(write_journal, 'record_receipt', before_publication)
        patch.setattr(native_writer, '_execute_once', before_return)
        assert write(db, plans, run_id=run, clock=clock)['verified']
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'replacement', member=member)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans, run_id=run, clock=clock)
    _restore_pair(backup, tmp_path / 'queued')
    if historical:
        # Deployed records predate the independent submission watermark.
        write_journal._submission_path(member).unlink()
    if boundary != 'absent':
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    (intent, body), = write_journal.pending_intents({'run_id': run})
    assert bool(body['identity'].get('packet_attempt')) != historical
    if historical:
        assert write_journal.terminal_target(intent.stem, member, scope=native_writer._target_scope(plans[0]))

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id=run)

    start = len(db.events)
    for call in (replay, replay, lambda: write(db, plans, run_id=run, clock=clock),
                 lambda: write(db, [_club_plan('11', 'new', NEWEST)], run_id='fresh', clock=NEWEST)):
        with pytest.raises(RuntimeError, match='still pending'):
            call()
        assert intent.exists() and write_journal.read_receipt(digest) is None
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, [_club_plan('77', 'elsewhere', LATER)], run_id='disjoint', clock=LATER)['verified']
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='replacement'")
    with pytest.raises(RuntimeError):
        replay()
    assert write_journal.read_receipt(digest) is None
    assert db.sql.execute(f'SELECT COUNT(*) FROM {RECEIPTS} WHERE receipt_id=?', (member,)).fetchone() == (
        1 if boundary == 'absent' else 2,)


def test_pair_restore_preserves_late_duplicate_walk(tmp_path, monkeypatch):
    # Round 16 finding 2
    db, plan = CoreDB(), _snapshot(CLOCK, 'two-pages')
    backup = tmp_path / 'original'
    publish = write_journal.record_receipt

    def before_publication(*args):
        _backup_pair(backup)
        return publish(*args)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', before_publication)
        ack = write(db, [plan])
    member = ack['walks'][0]['receipt_id']
    table = 'transfermarkt_core_pages_v1'
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {table}')
    db.sql.execute(f'DELETE FROM {table}')
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, 'replacement_pages')
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    _restore_pair(backup, tmp_path / 'queued')
    start = len(db.events)
    for expected_pages in (0, 2):
        if expected_pages:
            db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
        for _ in range(2):
            with pytest.raises(RuntimeError, match='still pending'):
                native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
        with pytest.raises(StaleTransfermarktWrite):
            write(db, [_snapshot(NEWEST, 'fresh')], run_id='new', clock=NEWEST)
        assert db.sql.execute(f'SELECT COUNT(*) FROM {table}').fetchone() == (expected_pages,)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='replacement_pages'")
    with pytest.raises(RuntimeError):
        write(db, [plan])
    assert db.sql.execute(f'SELECT page_number, COUNT(*) FROM {table} WHERE receipt_id=? '
                          'GROUP BY page_number ORDER BY page_number', (member,)).fetchall() == [(1, 2), (2, 2)]
    assert write_journal.read_receipt(ack['plan_sha256']) is None


@pytest.mark.parametrize('intact_sibling', [False, True])
def test_reviewed_receipt_and_intact_later_repair(tmp_path, monkeypatch, intact_sibling):
    # Round 16 finding 2
    db, plan = CoreDB(), membership_plan(['7'])
    backup, publish = tmp_path / 'original', write_journal.record_receipt

    def before_publication(*args):
        _backup_pair(backup)
        return publish(*args)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', before_publication)
        ack = write(db, [plan])
    digest = ack['plan_sha256']
    member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    exact = native_writer._stored_receipts(db, [member])[member]
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    with monkeypatch.context() as patch:
        if not intact_sibling:
            # A deployed repair statement did not name the published intent.
            operation = native_writer._operation

            def legacy_operation(scope, member):
                return {k: v for k, v in operation(scope, member).items() if k != 'after_publication'}

            patch.setattr(native_writer, '_operation', legacy_operation)
        queued = _interrupt_ops_insert(patch, db, key[0], 'reviewed_repair')
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    statement = write_journal.pending_statement(key)
    query = write_journal.statement_query(statement)
    _, attempts = _journal_pair()
    _restore_pair(backup, tmp_path / 'queued')
    (path, _), = write_journal.pending_intents({'run_id': 'run-1'})
    scope = native_writer._target_scope(plan)
    if intact_sibling:
        attempts.rename(tmp_path / 'old-attempts')
        (tmp_path / 'queued' / attempts.name).rename(attempts)
        write_journal._immutable_record(write_journal._attempt_path(key), statement)
        write_journal.remember_query(statement, query)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
    assert db.sql.execute('SELECT state FROM test_query_history WHERE query_id=?', (query,)).fetchall() == [
        ('FINISHED',)]
    assert native_writer._stored_receipts(db, [member]) == {member: exact}
    assert native_writer._target_matches(db, plan)
    start = len(db.events)
    if intact_sibling:
        finish = write_journal.finish_statement

        def after_unlink(resolved):
            finish(resolved)
            if resolved == statement:
                raise RuntimeError('after verified receipt unlink')

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'finish_statement', after_unlink)
            with pytest.raises(RuntimeError, match='after verified receipt unlink'):
                replay()
        db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
        assert replay() == [ack]
    else:
        with pytest.raises(RuntimeError, match='operator reconciliation'):
            replay()
        restored = write_journal.latest_operation(scope, key, member)
        assert restored['statement']['attempt'] != statement['attempt']
        # Independent incident evidence restores the actual statement/query binding.
        # It still needs an operator decision to publish this pre-publication packet.
        head = write_journal._operation_path(scope, key, member)
        head.write_bytes((tmp_path / 'queued' / attempts.name / head.relative_to(attempts)).read_bytes())
        write_journal._immutable_record(write_journal._attempt_path(key), statement)
        write_journal.remember_query(statement, query)
        with pytest.raises(RuntimeError, match='operator reconciliation'):
            replay()
        with native_writer.writer_lock():
            decision = write_journal.record_reconciliation(
                path.stem, statement, query, reviewer='incident operator')

        def reconcile(decisions):
            return native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), path.stem, decisions)

        with pytest.raises(RuntimeError, match='explicit reconciliation'):
            replay()
        with pytest.raises(RuntimeError, match='members differ'):
            reconcile({'another-member': decision})
        record = write_journal._record_path('operator-reconciliations/' + path.stem, member)
        saved = record.read_bytes()
        for field, value in [('attempt', restored['statement']['attempt']),
                             ('operation', {**statement['operation'], 'scope': ['other', 'scope']})]:
            conflicting = {**decision, 'statement': {**statement, field: value}}
            write_journal._store_record(record, conflicting, replace=True)
            with pytest.raises(RuntimeError, match='attempt differs'):
                reconcile({member: conflicting})
            assert write_journal.read_receipt(digest) is None
        record.write_bytes(saved)
        db.sql.execute('UPDATE test_query_history SET state=? WHERE query_id=?', ('QUEUED', query))
        with pytest.raises(RuntimeError, match='still pending'):
            reconcile({member: decision})
        db.sql.execute('UPDATE test_query_history SET state=? WHERE query_id=?', ('FINISHED', query))
        physical_hash = exact[native_writer.RECEIPT_COLUMNS.index('physical_hash')]
        db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', ('wrong', member))
        with pytest.raises(RuntimeError, match='postimage differs'):
            reconcile({member: decision})
        db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', (physical_hash, member))
        assert write_journal.read_receipt(digest) is None and path.exists()
        assert reconcile({member: decision}) == ack
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.read_receipt(digest) == ack and not path.exists()


def test_rewound_same_intent_waits_for_review_even_after_restoring_the_latest_attempt(tmp_path, monkeypatch):
    db, plan = CoreDB(), membership_plan(['7'])
    scope = native_writer._target_scope(plan)
    backup = tmp_path / 'before-publication'

    def replay():
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    (intent, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    _backup_pair(backup)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, key[0], 'same_intent_repair')
        with pytest.raises(TrinoAmbiguousWriteError):
            _submit_legacy_unpublished_repair(db)
    replacement = write_journal.pending_statement(key)
    assert replacement['operation']['intent'] == intent.stem
    _restore_pair(backup, tmp_path / 'latest')
    earlier = write_journal.latest_operation(scope, key, member)['statement']
    assert earlier['attempt'] != replacement['attempt']
    start = len(db.events)
    for _ in range(2):
        with pytest.raises(RuntimeError, match='operator reconciliation'):
            replay()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])

    # Returning the newer files after detecting the rewind identifies the
    # reviewed POST. Only the explicit entry point may release this incident.
    _restore_pair(tmp_path / 'latest', tmp_path / 'older')
    submission, = queued
    db.cursor().execute(*submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='same_intent_repair'")
    start = len(db.events)
    with pytest.raises(RuntimeError, match='operator reconciliation'):
        replay()
    with pytest.raises(StaleTransfermarktWrite):
        write(db, [membership_plan(['8'], clock=LATER)], run_id='new', clock=LATER)
    assert write_journal.read_receipt(digest) is None
    with native_writer.writer_lock():
        decision = write_journal.record_reconciliation(intent.stem, replacement, 'same_intent_repair',
                                                       reviewer='incident operator')
    receipt = native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), intent.stem, {member: decision})
    assert receipt['verified'] and not intent.exists()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('damage', ['missing', 'copied'])
def test_unavailable_submission_watermark_fences_only_its_pending_packet(tmp_path, monkeypatch, damage):
    db, plan = CoreDB(), membership_plan(['7'])
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    (intent, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    owner = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    watermark = write_journal._submission_path(owner)
    if damage == 'missing':
        watermark.unlink()
    else:
        displaced = tmp_path / 'displaced-watermark'
        watermark.rename(displaced)
        shutil.copy2(displaced, watermark)
    start = len(db.events)
    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        write(db, [plan])
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, [membership_plan(['8'], clock=LATER)], run_id='new', clock=LATER)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.read_receipt(digest) is None and intent.exists()
    assert write(db, [_club_plan('77', 'elsewhere', LATER)], run_id='other', clock=LATER)['verified']


@pytest.mark.parametrize('historical', [False, True])
def test_missing_head_holds_published_and_anchored_receipt_recovery(tmp_path, monkeypatch, historical):
    # Round 17 finding 3
    db, plan = CoreDB(), membership_plan(['7'])
    hashes, digest = native_writer._bundle_identity([plan], 'run-1')
    member = native_writer._receipt_id(hashes[0], 'run-1', digest)
    backup = tmp_path / 'before-repair'
    if historical:
        _historical_packet([plan])
        original = native_writer._write_plan

        def pre_target(*args):
            _backup_pair(backup)
            return original(*args)

        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_write_plan', pre_target)
            assert write(db, [plan])['verified']
    else:
        assert write(db, [plan])['verified']
        _backup_pair(backup)
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'missing_head')
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    _restore_pair(backup, tmp_path / 'in-flight')
    write_journal._submission_path(member).unlink()
    start = len(db.events)
    for _ in range(2):
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, [plan])
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, [_club_plan('77', 'other', LATER)], run_id='other', clock=LATER)['verified']
    db.cursor().execute(*queued[0])
    assert db.sql.execute(f'SELECT count(*) FROM {RECEIPTS} WHERE receipt_id=?', (member,)).fetchone() == (1,)


@pytest.mark.parametrize('legacy_replacement', [False, True])
@pytest.mark.parametrize('restore_cells', [False, True])
def test_original_submission_directory_cannot_authorize_obsolete_receipt(
        tmp_path, monkeypatch, restore_cells, legacy_replacement):
    # Round 17 finding 4
    db, plan = CoreDB(), membership_plan(['7'])
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    (intent, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    directory = write_journal._submission_path(member).parent
    binding = write_journal._watermark_binding(member)
    backup = tmp_path / 'before-second'
    _backup_pair(backup)
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    save = write_journal._save_submission

    def preserve_original(head):
        directory.rename(tmp_path / 'original-submissions')
        save(head)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_save_submission', preserve_original)
        if legacy_replacement:
            # The deployed r16 replacement left the old inode untouched.
            patch.setattr(write_journal, '_replace_submission', preserve_original)
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'hidden')
        with pytest.raises(TrinoAmbiguousWriteError):
            _submit_legacy_unpublished_repair(db)
    _restore_pair(backup, tmp_path / 'second')
    directory.rename(tmp_path / 'newer-submissions')
    (tmp_path / 'original-submissions').rename(directory)
    if legacy_replacement:
        assert write_journal._watermark_binding(member) == binding
    else:
        assert write_journal.submission_head(member)['superseded']
    if restore_cells:
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    start = len(db.events)
    for call in (lambda: write(db, [plan]),
                 lambda: native_writer.replay_pending_intents(lambda: db, CoreWriter(db))):
        with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
            call()
    assert intent.exists() and write_journal.read_receipt(digest) is None
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.cursor().execute(*queued[0])
    assert db.sql.execute(f'SELECT count(*) FROM {RECEIPTS}').fetchone() == (1 + restore_cells,)


@pytest.mark.parametrize('explicit', [False, True])
def test_returned_query_recovery_requires_finished_and_exact_packet(monkeypatch, explicit):
    # Round 17 finding 6
    db, plan = CoreDB(), membership_plan(['7'])
    with monkeypatch.context() as patch:
        mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    (intent, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    member = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    scope = native_writer._target_scope(plan)
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    operation = write_journal.latest_operation(scope, key, member)
    assert operation['state'] == 'returned'
    (mapping, contents), = mappings.items()
    mapping.write_bytes(contents)
    if explicit:
        with native_writer.writer_lock():
            decision = write_journal.record_reconciliation(intent.stem, operation['statement'], operation['query_id'],
                                                           reviewer='incident review')

    def recover():
        if explicit:
            return native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), intent.stem, {member: decision})
        return native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')[0]

    db.sql.execute("UPDATE test_query_history SET state='QUEUED'")
    with pytest.raises(RuntimeError, match='still pending'):
        recover()
    db.sql.execute("UPDATE test_query_history SET state='FINISHED'")
    db.sql.execute(f'CREATE TABLE exact AS SELECT * FROM {plan.table}')
    db.sql.execute(f'DELETE FROM {plan.table}')
    before = len(db.events)
    with pytest.raises(RuntimeError):
        recover()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[before:])
    assert write_journal.latest_operation(scope, key, member) == operation and mapping.exists()
    assert write(db, [_club_plan('77', 'other', LATER)], run_id='other', clock=LATER)['verified']
    db.sql.execute(f'INSERT INTO {plan.table} SELECT * FROM exact')
    assert recover()['verified'] and not mapping.exists() and not intent.exists()


@pytest.mark.parametrize('boundary', ['before-head', 'head', 'binding', 'intent', 'index'])
def test_submission_initialization_crash_at_each_write(monkeypatch, boundary):
    # Round 17 finding 8
    db, plan = CoreDB(), membership_plan(['7'])
    hashes, digest = native_writer._bundle_identity([plan], 'run-1')
    member = native_writer._receipt_id(hashes[0], 'run-1', digest)
    store, write_once, append = write_journal._store_record, write_journal._write_once, write_journal._append_index

    def stop():
        raise OSError('crash at ' + boundary)

    def record(path, *args, **kwargs):
        if boundary == 'before-head' and path == write_journal._submission_path(member):
            stop()
        store(path, *args, **kwargs)
        if path == {'head': write_journal._submission_path(member),
                    'binding': write_journal._packet_watermark(member)}.get(boundary):
            stop()

    def intent(path, *args, **kwargs):
        write_once(path, *args, **kwargs)
        if boundary == 'intent' and path.parent == write_journal._root():
            stop()

    def index(entry):
        append(entry)
        if boundary == 'index':
            stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_store_record', record)
        patch.setattr(write_journal, '_write_once', intent)
        patch.setattr(write_journal, '_append_index', index)
        with pytest.raises(OSError, match='crash at'):
            write(db, [plan])
    assert db.events == [] and not list(write_journal.scope_operations(native_writer._target_scope(plan)))
    assert bool(write_journal.pending_intents({})) == (boundary in ('intent', 'index'))
    assert write(db, [plan])['verified'] and write(db, [plan])['verified'] and _receipt_count(db) == 1


def test_operator_decisions_follow_each_members_last_submission(monkeypatch):
    # Round 17 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    targets = {}

    class LabelledWriter(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            with native_writer._target_dispatch() as statement:
                member = statement['operation']['member']
                query = 'target_' + member
                write_journal.remember_query(statement, query)
                targets[member] = (statement, query)
                super()._write_to_iceberg(*args, **kwargs)
                db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FINISHED', NULL)", (query,))

    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            native_writer.write_core_bundle(lambda: db, LabelledWriter(db), plans, run_id='packet',
                                            universe_version='u', committed_at=CLOCK)
    first, second = targets
    (intent, _), = write_journal.pending_intents({})
    with native_writer.writer_lock():
        with pytest.raises(RuntimeError, match='attempt differs'):
            write_journal.record_reconciliation(intent.stem, *targets[first], reviewer='incident review')
        decisions = {}
        for member, plan in zip((first, second), plans, strict=True):
            receipt = write_journal.latest_operation(native_writer._target_scope(plan),
                                                     (native_writer.CORE_RECEIPTS_TABLE, member), member)
            decisions[member] = write_journal.record_reconciliation(
                intent.stem, receipt['statement'], receipt['query_id'], reviewer='incident review')
    start = len(db.events)
    with pytest.raises(RuntimeError, match='members differ'):
        native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), intent.stem, {second: decisions[second]})
    assert native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), intent.stem, decisions)['verified']
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('damage', ['wrong-inode', 'retired', 'checksum'])
def test_submission_initialization_preserves_conflicting_binding(damage):
    # Round 17 finding 8
    db, plan = CoreDB(), membership_plan(['7'])
    hashes, digest = native_writer._bundle_identity([plan], 'run-1')
    member = native_writer._receipt_id(hashes[0], 'run-1', digest)
    write_journal.start_submissions(member)
    binding = write_journal._packet_watermark(member)
    if damage == 'checksum':
        binding.write_bytes(b'corrupted')
    else:
        value = write_journal._watermark_binding(member)
        if damage == 'wrong-inode':
            value['inode'] += 1
        else:
            value['retired'] = 'f' * 32
        write_journal._store_record(binding, value, replace=True)
    before = binding.read_bytes()
    with pytest.raises(RuntimeError):
        write(db, [plan])
    assert binding.read_bytes() == before and not db.events and not write_journal.pending_intents({})


@pytest.mark.parametrize('before_first_post', [True, False])
def test_reservation_without_descriptors_guards_untouched_packet_member(tmp_path, monkeypatch, before_first_post):
    # Round 17 finding 2
    db = CoreDB()
    plans = _representation_packet(('7', '9'), CLOCK)
    backup = tmp_path / 'before-packet'
    _backup_pair(backup)
    write_plan = native_writer._write_plan

    def stop(*args):
        if args[2] == plans[0 if before_first_post else 1]:
            raise RuntimeError('after packet reservation')
        return write_plan(*args)

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', stop)
        with pytest.raises(RuntimeError, match='after packet reservation'):
            write(db, plans, run_id='packet')
    _restore_pair(backup, tmp_path / 'packet')
    assert not write_journal.pending_intents({})
    start = len(db.events)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']


def test_missing_submission_can_be_reestablished_after_exact_review(monkeypatch):
    # Round 17 finding 4
    db, plan = CoreDB(), membership_plan(['7'])
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        patch.setattr(write_journal, 'record_receipt', _interrupt_publication)
        with pytest.raises(RuntimeError, match='before local publication'):
            write(db, [plan])
    (path, _), = write_journal.pending_intents({})
    scope = native_writer._target_scope(plan)
    receipt, = [r for r in write_journal.scope_operations(scope) if r['key'][0] == native_writer.CORE_RECEIPTS_TABLE]
    owner = receipt['operation']['owner']
    write_journal._submission_path(owner).unlink()
    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        write(db, [plan])
    start = len(db.events)
    with native_writer.writer_lock():
        decision = write_journal.record_reconciliation(path.stem, receipt['statement'], receipt['query_id'],
                                                       reviewer='independent operator')
    ack = native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), path.stem, {owner: decision})
    assert ack['verified'] and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.submission_is_current(write_journal.submission_head(owner))
    assert write(db, [plan]) == ack and _receipt_count(db) == 1


def test_acknowledged_restore_hold_releases_only_on_explicit_exact_review(monkeypatch):
    # Round 17 finding 4
    db, plan = CoreDB(), membership_plan(['7'])
    ack = write(db, [plan])
    digest = ack['plan_sha256']
    owner = native_writer._receipt_id(plan.plan_sha256(), 'run-1', digest)
    scope = native_writer._target_scope(plan)
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        submissions = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'repair')
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, [plan])
    (path, _), = write_journal.pending_intents({})
    statement = write_journal.pending_statement((native_writer.CORE_RECEIPTS_TABLE, owner))
    # A storage operator can fence an opaque full-volume restore even when
    # all three locations agree. The live query still needs independent review.
    with native_writer.writer_lock():
        write_journal.hold_restored_packet(owner, digest, [scope])
        decision = write_journal.record_reconciliation(path.stem, statement, 'repair', reviewer='incident operator')
    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        write(db, [plan])
    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        write(db, [membership_plan(['8'], clock=LATER)], run_id='overlap', clock=LATER)
    assert write(db, [_club_plan('77', 'other', LATER)], run_id='disjoint', clock=LATER)['verified']

    def reconcile():
        return native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), path.stem, {owner: decision})

    start = len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        reconcile()
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='repair'")
    with pytest.raises(RuntimeError, match='postimage differs'):
        reconcile()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.cursor().execute(*submissions[0])
    start = len(db.events)
    assert reconcile() == ack
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert not list(write_journal.restored_packet_hold({scope})) and not path.exists()
    assert write(db, [plan]) == ack


@pytest.mark.parametrize('reserved', [False, True])
def test_crash_before_scope_owners_preserves_packet_reservation(monkeypatch, reserved):
    # Round 18 finding 1
    db = CoreDB()
    plans = _representation_packet(('7', '9'), CLOCK)

    def stop(*args):
        raise OSError('before individual scope owners')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_scopes' if reserved else 'reserve_submission', stop)
        with pytest.raises(OSError, match='before individual'):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner = native_writer._receipt_id(hashes[0], 'run-1', digest)
    scopes = [native_writer._target_scope(plan) for plan in plans]
    head = write_journal.submission_head(owner)
    assert [write_journal.scope_owner(scope) for scope in scopes] == [None, None]
    assert list(write_journal.bundle_reservations()) == []
    if reserved:
        assert head['reservation'] == path.stem and set(map(tuple, head['scopes'])) == set(scopes)
        start = len(db.events)
        for player in ('9', '7'):
            with pytest.raises(StaleTransfermarktWrite, match='still pending'):
                write(db, _representation_packet((player,), LATER), run_id='overlap', clock=LATER)
        assert db.events[start:] == []
    else:
        assert head == {'owner': owner}
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']
    receipt, = native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    assert receipt['verified'] and not path.exists()
    assert db.sql.execute(f'SELECT count(*) FROM {RECEIPTS} WHERE run_id=?', ('run-1',)).fetchone() == (2,)


@pytest.mark.parametrize('restored_dispatch', [(), (1,), (0,), (0, 1)])
def test_original_publication_restore_hold_requires_exact_packet_review(monkeypatch, restored_dispatch):
    # Round 18 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)

    def stop(*args):
        raise OSError('after acknowledgement')

    with monkeypatch.context() as patch:
        # Round 19 finding 3
        mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        dispatched = {}
        finish = write_journal.finish_statement

        def capture(statement, **options):
            if statement['key'][0] == native_writer.CORE_RECEIPTS_TABLE:
                op = statement['operation']
                file = write_journal._operation_path(op['scope'], statement['key'], op['member'])
                dispatched[op['member']] = file, file.read_bytes()
            finish(statement, **options)

        patch.setattr(write_journal, 'finish_statement', capture)
        patch.setattr(native_writer, '_complete_packet', stop)
        with pytest.raises(OSError, match='after acknowledgement'):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    ack = write_journal.read_receipt(digest)
    members = [native_writer._receipt_id(p.plan_sha256(), 'run-1', digest) for p in plans]
    scopes = [native_writer._target_scope(p) for p in plans]
    # Round 20 finding 5
    for index in restored_dispatch:
        file, snapshot = dispatched[members[index]]
        file.write_bytes(snapshot)
        statement = write_journal._read_record(file)['statement']
        mapping = write_journal._record_path('statement-queries', statement['attempt'])
        mapping.write_bytes(mappings[mapping])
    with native_writer.writer_lock():
        write_journal.hold_restored_packet(members[0], digest, scopes)
        reviews = {}
        for scope, member in zip(scopes, members, strict=True):
            op = write_journal.latest_operation(scope, (native_writer.CORE_RECEIPTS_TABLE, member), member)
            reviews[member] = write_journal.record_reconciliation(
                path.stem, op['statement'], op.get('query_id') or write_journal.statement_query(op['statement']), reviewer='restore operator')

    def reconcile(decisions):
        return native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), path.stem, decisions)

    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        native_writer.replay_pending_intents(lambda: db, CoreWriter(db), run_id='run-1')
    with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
        write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']

    start = len(db.events)
    with pytest.raises(RuntimeError, match='members differ'):
        reconcile({members[0]: reviews[members[0]]})
    if restored_dispatch:
        mapping.unlink()
        with pytest.raises(RuntimeError):
            reconcile(reviews)
        write_journal.remember_query(statement, reviews[members[1 - index]]['query_id'])
        with pytest.raises(RuntimeError):
            reconcile(reviews)
        mapping.write_bytes(mappings[mapping])
    query = reviews[members[1]]['query_id']
    db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query,))
    with pytest.raises(RuntimeError, match='still pending'):
        reconcile(reviews)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (members[0],))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[0],))
    with pytest.raises(RuntimeError, match='postimage differs'):
        reconcile(reviews)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    table = plans[1].table
    db.sql.execute(f'CREATE TABLE target AS SELECT * FROM {table} WHERE player_id=?', ('9',))
    db.sql.execute(f'DELETE FROM {table} WHERE player_id=?', ('9',))
    with pytest.raises(RuntimeError):
        reconcile(reviews)
    assert list(write_journal.restored_packet_hold(set(scopes))) and path.exists()
    for index in restored_dispatch:
        assert write_journal._read_record(dispatched[members[index]][0])['state'] == 'dispatched'
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', lambda *args: pytest.fail('second acknowledgement'))
        assert reconcile(reviews) == ack
    assert not list(write_journal.restored_packet_hold(set(scopes))) and not path.exists()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    with pytest.raises(RuntimeError, match='exact pending intent'):
        reconcile(reviews)
    assert write(db, plans) == ack
    assert write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)['verified']


def _partial_representation_receipts(patch, db, plans, *, historical, boundary):
    run = 'run-1'
    if historical:
        _historical_packet(plans)
    hashes, digest = native_writer._bundle_identity(plans, run)
    members = [native_writer._receipt_id(h, run, digest) for h in hashes]
    first_key = [native_writer.CORE_RECEIPTS_TABLE, members[0]]
    mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
    transition, finish = write_journal._operation_transition, write_journal.finish_statement

    def stop_after_return(statement, state, *args):
        result = transition(statement, state, *args)
        if state == 'returned' and statement['key'] == first_key:
            raise OSError('first receipt returned')
        return result

    def stop_after_cleanup(statement, **options):
        finish(statement, **options)
        if statement['key'] == first_key:
            raise OSError('first receipt returned')

    if boundary in ('returned', 'returned-mapped'):
        patch.setattr(write_journal, '_operation_transition', stop_after_return)
    else:
        patch.setattr(write_journal, 'finish_statement', stop_after_cleanup)
    with pytest.raises(OSError, match='first receipt returned'):
        write(db, plans)
    first = write_journal.latest_operation(native_writer._target_scope(plans[0]), first_key, members[0])
    assert first['state'] == 'returned' and first['query_id']
    mapping = write_journal._record_path('statement-queries', first['statement']['attempt'])
    if boundary == 'restored-map':
        mapping.write_bytes(mappings[mapping])
    elif boundary == 'returned':
        mapping.unlink()  # only the returned sibling retains the genuine coordinator ID
    assert write_journal.latest_operation(native_writer._target_scope(plans[1]),
                                          (native_writer.CORE_RECEIPTS_TABLE, members[1]), members[1]) is None
    return digest, members, first, mapping


@pytest.mark.parametrize('historical', [False, True])
@pytest.mark.parametrize('boundary', ['returned', 'cleaned', 'restored-map'])
def test_partial_receipt_packet_continues_only_after_exact_first_return(monkeypatch, historical, boundary):
    # Round 18 finding 3
    # Round 18 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    with monkeypatch.context() as patch:
        digest, members, first, mapping = _partial_representation_receipts(
            patch, db, plans, historical=historical, boundary=boundary)
    writer = CoreWriter(db)

    def recover():
        if historical:
            return native_writer.replay_pending_intents(lambda: db, writer, run_id='run-1')[0]
        return native_writer.write_core_bundle(lambda: db, writer, plans, run_id='run-1',
                                               universe_version='core-universe-v1', committed_at=CLOCK)

    start = len(db.events)
    query = first['query_id']
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    with pytest.raises(RuntimeError, match='still pending'):
        recover()
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'QUEUED', NULL)", (query,))
    with pytest.raises(RuntimeError, match='still pending'):
        recover()
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
    if mapping.exists():
        exact = mapping.read_bytes()
        write_journal._store_record(mapping, {'statement': first['statement'], 'query_id': 'other'}, replace=True)
        with pytest.raises(RuntimeError):
            recover()
        mapping.write_bytes(exact)
    (physical_hash,) = db.sql.execute(f'SELECT physical_hash FROM {RECEIPTS} WHERE receipt_id=?',
                                      (members[0],)).fetchone()
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', ('0' * 64, members[0]))
    with pytest.raises(RuntimeError):
        recover()
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', (physical_hash, members[0]))
    table = plans[1].table
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {table} WHERE player_id=?', ('9',))
    db.sql.execute(f'DELETE FROM {table} WHERE player_id=?', ('9',))
    with pytest.raises(RuntimeError, match='physical readback differs'):
        recover()
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
    assert write_journal.read_receipt(digest) is None
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert recover()['verified'] and write(db, plans) == write_journal.read_receipt(digest)
    assert len([sql for sql in db.events[start:] if sql.startswith('INSERT INTO ')]) == 1
    assert writer.writes == [] and not mapping.exists() and not write_journal.pending_intents({})
    assert db.sql.execute(f'SELECT receipt_id, count(*) FROM {RECEIPTS} GROUP BY receipt_id ORDER BY receipt_id'
                          ).fetchall() == [(m, 1) for m in sorted(members)]


@pytest.mark.parametrize('historical', [False, True])
@pytest.mark.parametrize('restored_cells', [False, True])
def test_partial_packet_cannot_repeat_a_late_second_receipt(monkeypatch, historical, restored_cells):
    # Round 18 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    with monkeypatch.context() as patch:
        digest, members, _, _ = _partial_representation_receipts(
            patch, db, plans, historical=historical, boundary='restored-map')
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'late', member=members[1])
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    submission, = queued
    start = len(db.events)
    for _ in range(2):
        with pytest.raises(RuntimeError, match='still pending'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.read_receipt(digest) is None
    db.cursor().execute(*submission)  # Exact cells alone cannot settle the queued INSERT.
    start = len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        write(db, plans)
    if restored_cells:
        db.cursor().execute(*submission)  # The original now lands alongside the restored row.
        start = len(db.events)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='late'")
    if restored_cells:
        with pytest.raises(RuntimeError, match='receipt readback differs'):
            write(db, plans)
        assert write_journal.read_receipt(digest) is None and _receipt_count(db) == 3
    else:
        assert write(db, plans)['verified'] and _receipt_count(db) == 2
    assert not any(sql.startswith('INSERT INTO ') for sql in db.events[start:])


@pytest.mark.parametrize('historical', [False, True])
def test_restored_first_receipt_cannot_hide_a_later_sibling_insert(tmp_path, monkeypatch, historical):
    # Round 18 finding 3
    # Round 18 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    with monkeypatch.context() as patch:
        digest, members, _, _ = _partial_representation_receipts(
            patch, db, plans, historical=historical, boundary='cleaned')
    backup = tmp_path / 'first-receipt'
    _backup_pair(backup)
    with monkeypatch.context() as patch:
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'later', member=members[1])
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    submission, = queued
    _restore_pair(backup, tmp_path / 'second-receipt')
    for landed in (False, True):
        if landed:
            db.cursor().execute(*submission)
            db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='later'")
        start = len(db.events)
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)
        assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
        assert write_journal.read_receipt(digest) is None and _receipt_count(db) == 1 + landed
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']


def test_corrupt_initial_watermark_routes_by_exact_pending_descriptor(monkeypatch):
    # Round 18 finding 5
    db, plans = CoreDB(), _representation_packet(('7',), CLOCK)

    def stop(*args):
        raise OSError('before first warehouse connection')

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_journal_connection', stop)
        with pytest.raises(OSError, match='before first warehouse'):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    digest = body['identity']['digest']
    owner = native_writer._receipt_id(plans[0].plan_sha256(), 'run-1', digest)
    binding = write_journal._packet_watermark(owner)
    binding.write_bytes(b'torn')
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_journal_connection', lambda *args: pytest.fail('admitted corrupt packet'))
        for attempt in (lambda: write(db, plans),
                        lambda: write(db, _representation_packet(('7',), LATER), run_id='overlap', clock=LATER)):
            with pytest.raises(RuntimeError, match='checksum'):
                attempt()
    assert write(db, _representation_packet(('17',), LATER), run_id='disjoint', clock=LATER)['verified']
    assert write(db, [_club_plan('77', 'elsewhere', LATER)], run_id='other', clock=LATER)['verified']
    with pytest.raises(RuntimeError, match='checksum'):
        native_writer.reconcile_pending_intent(lambda: db, CoreWriter(db), path.stem, {owner: {}})
    assert binding.read_bytes() == b'torn' and path.exists() and write_journal.read_receipt(digest) is None


@pytest.mark.parametrize('historical', [False, True])
@pytest.mark.parametrize('players', [('7', '9'), ('7', '9', '17')])
def test_dispatched_receipt_prefix_waits_for_query_and_all_targets(monkeypatch, historical, players):
    # Round 19 finding 1
    db, plans = CoreDB(), _representation_packet(players, CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    members = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    if historical:
        _historical_packet(plans)
    with monkeypatch.context() as patch:
        mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        queued = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'last-prefix',
                                       member=members[-2])
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    for path, content in mappings.items():
        path.write_bytes(content)
    key = (native_writer.CORE_RECEIPTS_TABLE, members[-2])
    statement = write_journal.pending_statement(key)
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    saved = mapping.read_bytes()
    writer = CoreWriter(db)

    def recover():
        if historical:
            return native_writer.replay_pending_intents(lambda: db, writer, run_id='run-1')[0]
        return native_writer.write_core_bundle(lambda: db, writer, plans, run_id='run-1',
                                              universe_version='core-universe-v1', committed_at=CLOCK)

    start = len(db.events)
    with pytest.raises(RuntimeError):
        recover()
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:]) and not writer.writes
    db.cursor().execute(*queued[0])
    start = len(db.events)
    with pytest.raises(RuntimeError):
        recover()
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='last-prefix'")
    mapping.unlink()
    with pytest.raises(RuntimeError):
        recover()
    mapping.write_bytes(saved)
    for path in mappings:
        query = write_journal._read_record(path)['query_id']
        db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query,))
        with pytest.raises(RuntimeError):
            recover()
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
    table = plans[-1].table
    db.sql.execute(f'CREATE TABLE tail_target AS SELECT * FROM {table} WHERE player_id=?', (players[-1],))
    db.sql.execute(f'DELETE FROM {table} WHERE player_id=?', (players[-1],))
    with pytest.raises(RuntimeError):
        recover()
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.read_receipt(digest) is None
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM tail_target')
    assert recover()['verified'] and not writer.writes
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]
    assert _receipt_count(db) == len(plans) and not write_journal.pending_intents({})


@pytest.mark.parametrize('boundary', ['returned-mapped', 'returned', 'restored-map', 'cleaned'])
def test_repair_prefix_uses_current_intent_before_restoring_sibling(monkeypatch, boundary):
    # Round 19 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        _, members, first, _ = _partial_representation_receipts(
            patch, db, plans, historical=False, boundary=boundary)
    (path, _), = write_journal.pending_intents({})
    query = first['query_id']
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    writer = CoreWriter(db)
    start = len(db.events)

    def recover():
        return native_writer.replay_pending_intents(lambda: db, writer, run_id='run-1')

    with pytest.raises(RuntimeError):
        recover()
    assert path.exists() and _receipt_count(db) == 1
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FINISHED', NULL)", (query,))
    original = db.sql.execute(f'SELECT physical_hash FROM {RECEIPTS} WHERE receipt_id=?', (members[0],)).fetchone()
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=?', ('wrong',))
    with pytest.raises(RuntimeError):
        recover()
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=?', original)
    db.sql.execute(f'CREATE TABLE tail AS SELECT * FROM {plans[1].table} WHERE player_id=?', ('9',))
    db.sql.execute(f'DELETE FROM {plans[1].table} WHERE player_id=?', ('9',))
    with pytest.raises(RuntimeError):
        recover()
    assert path.exists() and not writer.writes
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {plans[1].table} SELECT * FROM tail')
    assert recover() == [ack] and not path.exists() and not writer.writes
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]
    assert native_writer._stored_receipts(db, members).keys() == set(members)


def test_receipt_repair_keeps_members_outside_its_worklist_read_only(monkeypatch):
    # Round 19 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9', '17'), CLOCK)
    ack = write(db, plans)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    first, held, tail = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    db.sql.execute(f'CREATE TABLE saved_held AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (held,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id<>?', (held,))
    with monkeypatch.context() as patch:
        _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'repair-first', member=first, landed=True)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    assert write(db, _representation_packet(('23',), LATER), run_id='disjoint', clock=LATER)['verified']
    start = len(db.events)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (held,))
    with pytest.raises(RuntimeError, match='receipt readback differs'):
        write(db, plans)
    assert not native_writer._stored_receipts(db, [tail])
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_held')
    assert write(db, plans) == ack and not write_journal.pending_intents({})
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


def _r20_recover(db, writer, plans, replay):
    if replay:
        ack, = native_writer.replay_pending_intents(lambda: db, writer, run_id='run-1')
        return ack
    return native_writer.write_core_bundle(lambda: db, writer, plans, run_id='run-1',
                                           universe_version='core-universe-v1', committed_at=CLOCK)


@pytest.mark.parametrize('restored_map', [False, True])
@pytest.mark.parametrize('keep_ack', [False, True])
def test_retired_original_with_rewound_receipt_prefix_is_read_only(tmp_path, monkeypatch, restored_map, keep_ack):
    # Round 20 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    with monkeypatch.context() as patch:
        digest, members, _, mapping = _partial_representation_receipts(
            patch, db, plans, historical=False, boundary='restored-map' if restored_map else 'cleaned')
    root = write_journal._root()
    backup = tmp_path / 'prefix'
    shutil.copytree(root, backup)
    execute = CoreCursor.execute
    tail = []

    def capture(cursor, sql, params=()):
        if sql.startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE):
            tail.append((sql, tuple(params)))
        return execute(cursor, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', capture)
        ack = write(db, plans)
    submission, = tail
    root.rename(tmp_path / 'published')
    shutil.copytree(backup, root)
    if keep_ack:
        write_journal.record_receipt(digest, ack)
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE saved_target AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[1],))
    writer = CoreWriter(db)
    start = len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write_journal.read_receipt(digest) == (ack if keep_ack else None) and _receipt_count(db) == 1
    db.cursor().execute(*submission)
    start = len(db.events)
    if restored_map:
        with pytest.raises(RuntimeError, match='unbound'):
            _r20_recover(db, writer, plans, True)
        assert mapping.exists()
    else:
        assert _r20_recover(db, writer, plans, True) == ack
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert _receipt_count(db) == 2


@pytest.mark.parametrize('drift', [False, True])
def test_active_receipt_repair_requires_its_pending_descriptor(tmp_path, monkeypatch, drift):
    # Round 20 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    root = write_journal._root()
    shutil.copytree(root, tmp_path / 'ack')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        _, members, first, _ = _partial_representation_receipts(patch, db, plans, historical=False, boundary='cleaned')
    root.rename(tmp_path / 'repair')
    shutil.copytree(tmp_path / 'ack', root)
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (first['query_id'],))
    if drift:
        db.sql.execute(f"CREATE TABLE saved_target AS SELECT * FROM {plans[1].table} WHERE player_id='9'")
        db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    start = len(db.events)
    writer = CoreWriter(db)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FINISHED', NULL)", (first['query_id'],))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert not write_journal.pending_intents({}) and _receipt_count(db) == 1
    root.rename(tmp_path / 'restored')
    (tmp_path / 'repair').rename(root)
    if drift:
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, True)
        db.sql.execute(f'INSERT INTO {plans[1].table} SELECT * FROM saved_target')
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert _r20_recover(db, writer, plans, True) == ack
    assert not writer.writes and native_writer._stored_receipts(db, members).keys() == set(members)
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


def test_receipt_repair_descriptor_bounds_work_before_reservation(monkeypatch):
    # Round 20 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9', '17'), CLOCK)
    ack = write(db, plans)
    members = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    owner, _, outside = members
    before = write_journal.submission_head(owner)['members']
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id<>?', (outside,))

    def stop(*args, **kwargs):
        raise OSError('before receipt reservation')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', stop)
        with pytest.raises(OSError, match='before receipt reservation'):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    assert body['payload']['receipt_only'] is True
    assert body['payload']['receipt_worklist'] == {'missing': members[:2], 'before': before}
    db.sql.execute(f'CREATE TABLE saved_receipt AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    table = plans[-1].table
    db.sql.execute(f"CREATE TABLE saved_target AS SELECT * FROM {table} WHERE player_id='17'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='17'")
    start = len(db.events)
    writer = CoreWriter(db)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert native_writer._stored_receipts(db, members).keys() == set(members) and not writer.writes
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE] * 2
    assert not path.exists()


@pytest.mark.parametrize('repair', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_acknowledged_retirement_resumes_after_first_member_unlink(monkeypatch, repair, replay):
    # Round 20 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    members = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    scopes = [native_writer._target_scope(plan) for plan in plans]
    if repair:
        write(db, plans)
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
    keys = [(native_writer.CORE_RECEIPTS_TABLE, m) for m in members]
    first = write_journal._operation_path(scopes[0], keys[0], members[0])
    unlink, retire = write_journal._unlink_record, write_journal.retire_operations

    def stop(path):
        unlink(path)
        if path == first:
            raise OSError('retired first member')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', lambda _, owner: retire(scopes, owner))
        patch.setattr(write_journal, '_unlink_record', stop)
        with pytest.raises(OSError, match='retired first member'):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    ack = write_journal.read_receipt(digest)
    assert write_journal.completed_intent(path.stem)['acknowledgement'] == native_writer._digest(ack)
    survivor = write_journal._operation_path(scopes[1], keys[1], members[1])
    assert not first.exists() and survivor.exists()
    start = len(db.events)
    writer = CoreWriter(db)
    genuine = survivor.read_bytes()
    record = write_journal._read_record(survivor)
    write_journal._store_record(survivor, {**record, 'state': 'rejected'}, replace=True)
    with pytest.raises(RuntimeError, match='completed receipt binding differs'):
        _r20_recover(db, writer, plans, replay)
    survivor.write_bytes(genuine)
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (members[0],))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[0],))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    table = plans[-1].table
    db.sql.execute(f"CREATE TABLE target AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    with pytest.raises(RuntimeError):
        write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)
    assert path.exists() and write_journal.latest_operation(scopes[1], keys[1], members[1])
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert not path.exists() and not list(write_journal.bundle_reservations())
    assert write(db, _representation_packet(('9',), LATER), run_id='overlap', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [False, True])
def test_final_rejected_receipt_with_restored_sibling_mapping(monkeypatch, replay):
    # Round 20 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    members = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    with monkeypatch.context() as patch:
        mappings = _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'final', member=members[-1])
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    (mapping, saved), = mappings.items()
    mapping.write_bytes(saved)
    start = len(db.events)
    writer = CoreWriter(db)
    db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code='UNKNOWN' WHERE query_id='final'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    db.sql.execute("UPDATE test_query_history SET error_code='QUERY_QUEUE_FULL' WHERE query_id='final'")
    table = plans[0].table
    db.sql.execute(f"CREATE TABLE target AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    assert _r20_recover(db, writer, plans, replay)['verified'] and not writer.writes
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]
    assert _receipt_count(db) == 2 and not write_journal.pending_intents({})


@pytest.mark.parametrize('finished', [False, True])
@pytest.mark.parametrize('keep_ack', [False, True])
def test_restored_original_cannot_extend_surviving_receipt_worklist(tmp_path, monkeypatch, finished, keep_ack):
    # Round 21 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root = write_journal._root()
    publish = write_journal.record_receipt

    def backup_original(digest, receipt):
        shutil.copytree(root, tmp_path / 'original')
        publish(digest, receipt)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', backup_original)
        ack = write(db, plans)
    digest = ack['plan_sha256']
    first, tail = [native_writer._receipt_id(h, 'run-1', digest) for h in ack['identity']['plan_hashes']]
    db.sql.execute(f'CREATE TABLE original_receipts AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    if finished:
        with monkeypatch.context() as patch:
            _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'repair-tail', member=tail, landed=True)
            with pytest.raises(TrinoAmbiguousWriteError):
                write(db, plans)
        finish = write_journal.finish_statement

        def stop_after_finish(statement, **kwargs):
            finish(statement, **kwargs)
            if statement['key'] == [native_writer.CORE_RECEIPTS_TABLE, tail]:
                raise OSError('finished repair')

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'finish_statement', stop_after_finish)
            with pytest.raises(OSError, match='finished repair'):
                write(db, plans)
        key = [native_writer.CORE_RECEIPTS_TABLE, tail]
        assert write_journal.latest_operation(native_writer._target_scope(plans[1]), key, tail)['state'] == 'FINISHED'
    else:
        def stop_before_insert(*args, **kwargs):
            raise OSError('reserved repair')

        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_merge_receipts', stop_before_insert)
            with pytest.raises(OSError, match='reserved repair'):
                write(db, plans)
    head = write_journal.submission_head(first)
    assert write_journal.submission_is_current(head) and head['receipt_repair']['missing'] == [tail]
    root.rename(tmp_path / 'repair')
    shutil.copytree(tmp_path / 'original', root)
    (path, _), = write_journal.pending_intents({})
    assert path.stem != head['receipt_repair']['intent']
    if keep_ack:
        write_journal.record_receipt(digest, ack)
    table = plans[0].table
    db.sql.execute(f"CREATE TABLE original_target AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (first,))
    start = len(db.events)
    writer = CoreWriter(db)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM original_target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and _receipt_count(db) == int(finished)
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:]) and not writer.writes
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM original_receipts WHERE receipt_id=?', (first,))
    if not finished:
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, True)
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM original_receipts WHERE receipt_id=?', (tail,))
    assert _r20_recover(db, writer, plans, True) == ack
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:]) and not writer.writes


def test_rowless_rumour_member_freezes_only_its_missing_receipt(monkeypatch):
    # Round 21 finding 2
    from tests.unit.scrapers import test_transfermarkt_sections as lanes

    captures = (lanes._rumour_capture(1, lanes.RUMOUR_PAGE_1_CAPTURED),
                lanes._rumour_capture(2, lanes.RUMOUR_PAGE_2_CAPTURED, body=lanes._no_entries_html(2, 2)))
    outcomes = lanes.sections.extract_rumour_observations(
        captures, lanes._rumour_scope(), competition=lanes._rumour_competition(),
        extracted_at=lanes.RUMOUR_EXTRACTED_AT, universe_version=lanes.UNIVERSE)
    plans = [core.plan_for_outcome(outcome, captured_at=outcome.capture.captured_at,
                                   scope_values={'competition_id': 'GB1'}) for outcome in outcomes]
    assert plans[0].rows and not plans[1].rows
    run = outcomes[0].run_id
    db = CoreDB()
    writer = CoreWriter(db)

    def retry():
        return native_writer.write_core_bundle(lambda: db, writer, plans, run_id=run,
                                               universe_version=lanes.UNIVERSE, committed_at=lanes.RUMOUR_EXTRACTED_AT)

    ack = retry()
    first, empty = [native_writer._receipt_id(h, run, ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    prior = write_journal.submission_head(first)['members']
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (empty,))

    def interrupt(*args, **kwargs):
        raise OSError('before reservation')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', interrupt)
        with pytest.raises(OSError, match='before reservation'):
            retry()
    (path, body), = write_journal.pending_intents({})
    assert body['payload']['receipt_only']
    assert body['payload']['receipt_worklist'] == {'missing': [empty], 'before': prior}
    writer.writes.clear()
    table = plans[0].table
    db.sql.execute(f'CREATE TABLE populated AS SELECT * FROM {table}')
    db.sql.execute(f'CREATE TABLE original_receipt AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {table}')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    start = len(db.events)
    with pytest.raises(RuntimeError):
        native_writer.replay_pending_intents(lambda: db, writer, run_id=run)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM populated')
    with pytest.raises(RuntimeError):
        retry()
    assert path.exists() and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM original_receipt')
    assert native_writer.replay_pending_intents(lambda: db, writer, run_id=run) == [ack]
    assert not writer.writes and native_writer._stored_receipts(db, [first, empty]).keys() == {first, empty}
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('repair', [False, True])
def test_last_operation_unlink_requires_live_readback_before_scope_release(monkeypatch, repair):
    # Round 21 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner, tail = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    if repair:
        write(db, plans)
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))

    def interrupt(*args):
        raise OSError('before release')

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_release_packet', interrupt)
        with pytest.raises(OSError, match='before release'):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    ack = write_journal.read_receipt(digest)
    scopes = [native_writer._target_scope(plan) for plan in plans]
    assert write_journal.completed_intent(path.stem)['acknowledgement'] == native_writer._digest(ack)
    assert write_journal.submission_retired(write_journal.submission_head(owner))
    assert all(not list(write_journal.scope_operations(scope)) for scope in scopes)
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE target AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (tail,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and all(write_journal.scope_owner(scope) == owner for scope in scopes)
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=(SELECT physical_hash FROM saved) WHERE receipt_id=?', (tail,))
    assert _r20_recover(db, writer, plans, True) == ack
    assert not path.exists() and all(write_journal.scope_owner(scope) is None for scope in scopes)
    assert db.events[start:] and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert not writer.writes


def _restore_rejected_tail_mapping(monkeypatch, db, plans, member, *, cleanup=False):
    key = (native_writer.CORE_RECEIPTS_TABLE, member)
    with monkeypatch.context() as patch:
        _interrupt_ops_insert(patch, db, key[0], 'rejected-tail', member=member)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    statement = write_journal.pending_statement(key)
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    saved = {mapping: mapping.read_bytes()}
    unlink = write_journal._unlink_record

    def capture_cleanup(path):
        if path == mapping and cleanup:
            pending = write_journal._statement_path(key)
            for record in (pending, write_journal._attempt_path(key),
                           write_journal._record_path('settled-statements', pending.stem)):
                saved[record] = record.read_bytes()
        unlink(path)
    db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code='QUERY_QUEUE_FULL' "
                   "WHERE query_id='rejected-tail'")
    finish = write_journal.finish_statement

    def stop_after_rejection(current, **options):
        finish(current, **options)
        if current == statement and options.get('outcome') == 'rejected':
            raise OSError('durable rejection')

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', capture_cleanup)
        patch.setattr(write_journal, 'finish_statement', stop_after_rejection)
        with pytest.raises(OSError, match='durable rejection'):
            write(db, plans)
    record = write_journal.latest_operation(native_writer._target_scope(plans[-1]), key, member)
    assert record['state'] == 'rejected' and record['query_id'] == 'rejected-tail'
    assert not mapping.exists() and write_journal.pending_statement(key) is None
    for path, data in saved.items():
        path.write_bytes(data)
    return mapping


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('cleanup', [False, True])
def test_rejected_tail_reauthenticates_restored_query_mapping(monkeypatch, replay, cleanup):
    # Round 22 finding 3
    # Round 21 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner, tail = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    mapping = _restore_rejected_tail_mapping(monkeypatch, db, plans, tail, cleanup=cleanup)
    original = mapping.read_bytes()
    authentic = write_journal._read_record(mapping)
    assert authentic['statement'] == write_journal.submission_head(owner)['statement']
    start, writer = len(db.events), CoreWriter(db)
    write_journal._store_record(mapping, {**authentic, 'query_id': 'other-query'}, replace=True)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    mapping.write_bytes(original)
    db.sql.execute("DELETE FROM test_query_history WHERE query_id='rejected-tail'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert mapping.read_bytes() == original
    assert any('test_query_history' in sql for sql in db.events[start:])
    db.sql.execute("INSERT INTO test_query_history VALUES ('rejected-tail', 'QUEUED', NULL)")
    for state, error in [('FAILED', 'UNKNOWN'), ('FINISHED', None), ('QUEUED', None)]:
        db.sql.execute('UPDATE test_query_history SET state=?, error_code=? WHERE query_id=?',
                       (state, error, 'rejected-tail'))
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert mapping.read_bytes() == original
    db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code='QUERY_QUEUE_FULL'")
    table = plans[0].table
    db.sql.execute(f"CREATE TABLE saved AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert mapping.read_bytes() == original and _receipt_count(db) == 1
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert not mapping.exists() and not write_journal.pending_intents({}) and not writer.writes
    assert _receipt_count(db) == 2
    assert [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


def _r22_stop(*args, **kwargs):
    raise OSError('interrupted reservation')


@pytest.mark.parametrize('prior_worklist', [False, True])
def test_general_repair_preceding_a_retired_worklist_only_verifies(tmp_path, monkeypatch, prior_worklist):
    # Round 22 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    first, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    table, root = plans[0].table, write_journal._root()
    if prior_worklist:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        assert write(db, plans) == ack
    predecessor = write_journal.submission_head(first)
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    (_, body), = write_journal.pending_intents({})
    assert body['payload']['predecessor_head'] == native_writer._digest(predecessor)
    shutil.copytree(root, tmp_path / 'general')
    assert write(db, plans) == ack
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_release_packet', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    head = write_journal.submission_head(first)
    assert write_journal.submission_retired(head) and head['receipt_repair']['missing'] == [tail]
    root.rename(tmp_path / 'latest')
    shutil.copytree(tmp_path / 'general', root)
    (path, _), = write_journal.pending_intents({})
    db.sql.execute(f"CREATE TABLE saved_target AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    db.sql.execute(f'CREATE TABLE saved_receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (first,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (first,))
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and _receipt_count(db) == 1
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved_receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert not path.exists()


@pytest.mark.parametrize('release_started,missing', [(False, '7'), (False, '9'), (True, '9')])
def test_completed_packet_verifies_scopes_it_still_owns(tmp_path, monkeypatch, release_started, missing):
    # Round 22 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root = write_journal._root()
    scopes = [native_writer._target_scope(plan) for plan in plans]
    hold, release = write_journal.hold_scope, write_journal.release_scope

    def snapshot(scope, owner):
        hold(scope, owner)
        if not (tmp_path / 'first').exists():
            shutil.copytree(root / 'scope-owners', tmp_path / 'first')

    def release_one(scope, owner):
        release(scope, owner)
        _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'hold_scope', snapshot)
        if release_started:
            patch.setattr(write_journal, 'release_scope', release_one)
        else:
            patch.setattr(native_writer, '_release_packet', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    if not release_started:
        (root / 'scope-owners').rename(tmp_path / 'full')
        shutil.copytree(tmp_path / 'first', root / 'scope-owners')
    assert bool(list(write_journal.bundle_reservations())) != release_started
    (path, body), = write_journal.pending_intents({})
    ack = write_journal.read_receipt(body['identity']['digest'])
    members = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    owned_scope, released_scope = (scopes[1], scopes[0]) if release_started else scopes
    assert write_journal.scope_owner(owned_scope) == members[0]
    assert write_journal.scope_owner(released_scope) is None
    key = members[0 if missing == '7' else 1]
    table = plans[0].table
    db.sql.execute(f'CREATE TABLE target AS SELECT * FROM {table} WHERE player_id=?', (missing,))
    db.sql.execute(f'DELETE FROM {table} WHERE player_id=?', (missing,))
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (key,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (key,))
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    assert path.exists() and write_journal.scope_owner(owned_scope) == members[0]
    assert bool(list(write_journal.bundle_reservations())) != release_started
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert not path.exists() and not list(write_journal.bundle_reservations())


@pytest.mark.parametrize('replay', [False, True])
def test_no_post_completion_releases_only_its_exact_reservation(monkeypatch, replay):
    # Round 22 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    table = plans[-1].table
    db.sql.execute(f"CREATE TABLE saved AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_release_packet', _r22_stop)
        with pytest.raises(OSError):
            native_writer.replay_pending_intents(lambda: db, CoreWriter(db))
    (path, _), = write_journal.pending_intents({})
    owner = native_writer._receipt_id(ack['identity']['plan_hashes'][0], 'run-1', ack['plan_sha256'])
    head = write_journal.submission_head(owner)
    assert head['reservation'] == path.stem and 'statement' not in head
    assert write_journal.completed_intent(path.stem)['acknowledgement'] == native_writer._digest(ack)
    writer, start = CoreWriter(db), len(db.events)
    write_journal._replace_submission({**head, 'reservation': '0' * 64})
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    write_journal._replace_submission(head)
    scope = native_writer._target_scope(plans[0])
    pending = write_journal.begin_statement(scope, owner)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert write_journal.pending_statement(scope) == pending and path.exists()
    write_journal.finish_statement(pending, outcome='rejected')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not list(write_journal.bundle_reservations())
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('release_crash', [False, True])
def test_released_receipt_reservation_without_post_allows_exact_retries(tmp_path, monkeypatch, release_crash):
    # Round 22 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root = write_journal._root()
    publish = write_journal.record_receipt

    def save(digest, receipt):
        shutil.copytree(root, tmp_path / 'original')
        publish(digest, receipt)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', save)
        ack = write(db, plans)
    owner, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    db.sql.execute(f'CREATE TABLE saved AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_merge_receipts', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    root.rename(tmp_path / 'repair')
    shutil.copytree(tmp_path / 'original', root)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM saved')
    writer, start = CoreWriter(db), len(db.events)
    if release_crash:
        # Round 23 finding 4
        (publication, _), = write_journal.pending_intents({})
        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_release_packet', _r22_stop)
            with pytest.raises(OSError):
                _r20_recover(db, writer, plans, True)
        head = write_journal.submission_head(owner)
        completion = write_journal.completed_intent(publication.stem)
        assert completion['reservation_head'] == native_writer._digest(head)
        assert head['reservation'] != publication.stem and publication.stem in head['after_publication']
        assert write_journal._read_record(write_journal._packet_watermark(owner))['released']
        record = write_journal._record_path('completed-intents', publication.stem)
        write_journal._store_record(record, {**completion, 'reservation_head': '0' * 64}, replace=True)
        with pytest.raises(RuntimeError, match='binding differs'):
            _r20_recover(db, writer, plans, False)
        assert publication.exists()
        write_journal._store_record(record, completion, replace=True)
    assert _r20_recover(db, writer, plans, True) == ack
    head = write_journal.submission_head(owner)
    assert 'statement' not in head and write_journal._read_record(write_journal._packet_watermark(owner))['released']
    # The watermark is written before the guard unlinks. A surviving full
    # reservation still requires the missing descriptor to finish its release.
    reservation = write_journal._record_path('reserving-bundles', owner)
    saved_reservation = tmp_path / 'repair' / reservation.relative_to(root)
    shutil.copyfile(saved_reservation, reservation)
    with pytest.raises(RuntimeError, match='descriptor is still pending'):
        _r20_recover(db, writer, plans, False)
    assert reservation.read_bytes() == saved_reservation.read_bytes()
    reservation.unlink()
    for _ in range(2):
        start_retry = len(db.events)
        assert _r20_recover(db, writer, plans, False) == ack
        assert db.events[start_retry:] and write_journal.submission_head(owner) == head
    table = plans[0].table
    db.sql.execute(f"CREATE TABLE target AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM target')
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert not write_journal.pending_intents({}) and not list(write_journal.bundle_reservations())
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


def _r23_general_reservation(monkeypatch, *, before_local_guards=False):
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE saved_target AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        if before_local_guards:
            patch.setattr(write_journal, 'reserve_scopes', _r22_stop)
        else:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    members = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    head = write_journal.submission_head(members[0])
    assert write_journal.submission_is_current(head) and 'statement' not in head
    return db, plans, ack, members


@pytest.mark.parametrize('player', ['7', '9'])
def test_unreleased_head_owns_every_scope_before_local_guards(monkeypatch, player):
    # Round 23 finding 1
    db, plans, ack, members = _r23_general_reservation(monkeypatch, before_local_guards=True)
    scopes = [native_writer._target_scope(plan) for plan in plans]
    owner = members[0]
    writer = CoreWriter(db)
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', _r22_stop)
        with pytest.raises(OSError):
            _r20_recover(db, writer, plans, True)
    (path, _), = write_journal.pending_intents({})
    assert write_journal.completed_intent(path.stem) and not list(write_journal.bundle_reservations())
    assert all(write_journal.scope_owner(scope) is None for scope in scopes)
    table = plans[0].table
    member = members[0 if player == '7' else 1]
    db.sql.execute(f'CREATE TABLE exact AS SELECT * FROM {table} WHERE player_id=?', (player,))
    db.sql.execute(f'DELETE FROM {table} WHERE player_id=?', (player,))
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    start = len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError, match='physical readback'):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM exact')
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, True)
    assert path.exists() and not write_journal._read_record(write_journal._packet_watermark(owner)).get('released')
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert not path.exists() and write_journal.reservation_released(write_journal.submission_head(owner))
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet((player,), LATER), run_id='overlap', clock=LATER)['verified']


@pytest.mark.parametrize('later_reserved', [False, True])
def test_stale_general_descriptor_after_second_target_repair(tmp_path, monkeypatch, later_reserved):
    # Round 23 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    table, root = plans[0].table, write_journal._root()
    owner = native_writer._receipt_id(ack['identity']['plan_hashes'][0], 'run-1', ack['plan_sha256'])
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    shutil.copytree(root, tmp_path / 'first-repair')
    assert write(db, plans) == ack
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    if later_reserved:
        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
            with pytest.raises(OSError):
                write(db, plans)
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    else:
        assert write(db, plans) == ack
    root.rename(tmp_path / 'second-repair')
    shutil.copytree(tmp_path / 'first-repair', root)
    (path, body), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    if later_reserved:
        assert head['reservation'] != path.stem and 'statement' not in head
    else:
        assert write_journal.submission_retired(head) and head['statement']['key'][0] == plans[1].full_table
    watermark = write_journal._packet_watermark(owner).read_bytes()
    assert body['payload']['predecessor_head'] != native_writer._digest(head)
    db.sql.execute(f"CREATE TABLE exact AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    writer, start = CoreWriter(db), len(db.events)
    for replay in (False, True):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM exact')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists()
    assert write_journal._packet_watermark(owner).read_bytes() == watermark
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert not path.exists() and write_journal.submission_head(owner) == head
    if later_reserved:
        # Round 30 finding 6. Exact readback of the listed predecessor now
        # releases the no-POST reservation while preserving the head binding.
        assert path.stem in head['after_publication']
        assert write_journal._read_record(write_journal._packet_watermark(owner)) == {
            **write_journal._decode_record(watermark), 'released': True}
    else:
        assert write_journal._packet_watermark(owner).read_bytes() == watermark
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('reserved', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_matching_undispatched_receipt_reservation_accepts_only_exact_restoration(monkeypatch, reserved, replay):
    # Round 25 finding 5
    # Round 23 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    db.sql.execute(f'CREATE TABLE receipts AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        if reserved:
            patch.setattr(native_writer, '_merge_receipts', _r22_stop)
        else:
            patch.setattr(write_journal, 'reserve_submission', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    watermark = write_journal._packet_watermark(owner).read_bytes()
    assert write_journal.submission_is_current(head)
    if reserved:
        assert head['reservation'] == path.stem and head['receipt_repair']['missing'] == [tail]
    else:
        assert body['payload']['predecessor_head'] == native_writer._digest(head)
        assert body['payload']['receipt_worklist']['missing'] == [tail]
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipts WHERE receipt_id=?', (tail,))
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='conflict' WHERE receipt_id=?", (tail,))
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, True)
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=(SELECT physical_hash FROM receipts WHERE receipt_id=?) '
                   'WHERE receipt_id=?', (tail, tail))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists()
    expected_owner = owner if reserved else None
    assert all(write_journal.scope_owner(scope) == expected_owner for scope in head['scopes'])
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipts WHERE receipt_id=?', (owner,))
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and write_journal.submission_head(owner) == head
    if reserved:
        assert write_journal.reservation_released(head)
    else:
        assert write_journal._packet_watermark(owner).read_bytes() == watermark
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('replay', [True, False])
@pytest.mark.parametrize('posted', [False, True])
def test_release_rechecks_remaining_owner_with_foreign_receipt_queued(monkeypatch, replay, posted):
    # Round 23 finding 5
    # Round 24 finding 6
    db, plans, ack, members = _r23_general_reservation(monkeypatch)
    if posted:
        db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    scopes = [native_writer._target_scope(plan) for plan in plans]
    release = write_journal.release_scope

    def release_first(scope, owner):
        release(scope, owner)
        _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'release_scope', release_first)
        with pytest.raises(OSError):
            write(db, plans)
    assert write_journal.submission_retired(write_journal.submission_head(members[0])) == posted
    with monkeypatch.context() as patch:
        _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'foreign-seven')
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, _representation_packet(('7',), LATER), run_id='new-seven', clock=LATER)
    foreign, = [r for r in write_journal.scope_operations(scopes[0])
                if r['key'][0] == native_writer.CORE_RECEIPTS_TABLE]
    statement = write_journal.pending_statement(foreign['key'])
    assert statement == foreign['statement'] and write_journal.statement_query(statement) == 'foreign-seven'
    assert write_journal.scope_owner(scopes[1]) == members[0]
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    mapping_bytes = mapping.read_bytes()
    table = plans[1].table
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='physical readback'):
        _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    db.sql.execute(f'CREATE TABLE remaining AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (members[1],))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[1],))
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert write_journal.scope_owner(scopes[1]) == members[0]
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM remaining')
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        # After releasing 9, an ordinary whole-packet write encounters 7's fence.
        with pytest.raises(StaleTransfermarktWrite):
            _r20_recover(db, writer, plans, False)
    assert not write_journal.pending_intents({'run_id': 'run-1'}) and write_journal.scope_owner(scopes[1]) is None
    assert write_journal.pending_statement(foreign['key']) == statement
    assert write_journal.statement_query(statement) == 'foreign-seven' and mapping.read_bytes() == mapping_bytes
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='new-nine', clock=LATER)['verified']
    assert write_journal.pending_statement(foreign['key']) == statement


def test_released_general_reservation_retries_only_read_exact_packet(tmp_path, monkeypatch):
    # Round 23 finding 6
    db, plans, ack, members = _r23_general_reservation(monkeypatch)
    root = write_journal._root()
    shutil.copytree(root, tmp_path / 'reserved')
    writer = CoreWriter(db)
    assert _r20_recover(db, writer, plans, True) == ack
    head = write_journal.submission_head(members[0])
    assert write_journal.reservation_released(head) and 'receipt_repair' not in head
    start = len(db.events)
    for _ in range(2):
        before = len(db.events)
        assert _r20_recover(db, writer, plans, False) == ack and len(db.events) > before
    table = plans[1].table
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved_target')
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[1],))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert not write_journal.pending_intents({}) and write_journal.submission_head(members[0]) == head
    # The independently released head also fences a restore of its own
    # pre-completion descriptor and local owners.
    root.rename(tmp_path / 'released')
    shutil.copytree(tmp_path / 'reserved', root)
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, True)
    assert len(write_journal.pending_intents({})) == 1
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize(('published', 'repair'), [
    (False, 'retired'), (True, 'retired'), (True, 'returned'), (False, 'FINISHED')])
def test_original_publication_behind_later_target_repair(tmp_path, monkeypatch, published, repair):
    # Round 25 finding 1
    # Round 25 finding 2
    # Round 24 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root, record = write_journal._root(), write_journal.record_receipt

    def snapshot(digest, receipt):
        if published:
            record(digest, receipt)
        shutil.copytree(root, tmp_path / 'original')
        if not published:
            record(digest, receipt)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'record_receipt', snapshot)
        ack = write(db, plans)
    table = plans[0].table
    owner = native_writer._receipt_id(ack['identity']['plan_hashes'][0], 'run-1', ack['plan_sha256'])
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    if repair == 'FINISHED':
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, _LostResponse(db), plans, False)
    if repair == 'retired':
        assert write(db, plans) == ack
    else:
        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'retire_operations', _r22_stop)
            with pytest.raises(OSError):
                _r20_recover(db, CoreWriter(db), plans, repair == 'FINISHED')
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head)
    assert write_journal.submission_retired(head) == (repair == 'retired')
    if repair != 'retired':
        last = head['statement']
        assert write_journal.latest_operation(last['operation']['scope'], last['key'],
                                              last['operation']['member'])['state'] == repair
    root.rename(tmp_path / 'latest')
    (tmp_path / 'original').rename(root)
    (path, _), = write_journal.pending_intents({})
    assert path.stem in head['statement']['operation']['after_publication']
    db.sql.execute(f"CREATE TABLE saved AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    guards = [write_journal._record_path('reserving-bundles', owner), write_journal._packet_watermark(owner),
              write_journal._submission_path(owner), *(write_journal._scope_owner_path(native_writer._target_scope(p))
                                                        for p in plans)]
    original_guards = {guard: guard.read_bytes() for guard in guards}
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
    for replay in (False, True):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    assert {guard: guard.read_bytes() for guard in guards} == original_guards
    assert path.exists() and bool(write_journal.read_receipt(ack['plan_sha256'])) == published
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    assert _r20_recover(db, writer, plans, True) == ack
    assert not path.exists() and write_journal.submission_head(owner) == head
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


def test_old_worklist_cannot_extend_released_general_reservation(tmp_path, monkeypatch):
    # Round 24 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    predecessor = write_journal.submission_head(owner)
    root = write_journal._root()
    shutil.copytree(root, tmp_path / 'ack')
    db.sql.execute(f'CREATE TABLE tail AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    root.rename(tmp_path / 'worklist')
    (tmp_path / 'ack').rename(root)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM tail')
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    assert write(db, plans) == ack
    head = write_journal.submission_head(owner)
    assert head['members'] == predecessor['members'] and write_journal.reservation_released(head)
    watermark = write_journal._packet_watermark(owner).read_bytes()
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    root.rename(tmp_path / 'released')
    (tmp_path / 'worklist').rename(root)
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
    assert _receipt_count(db) == 1 and write_journal.submission_head(owner) == head
    assert write_journal._packet_watermark(owner).read_bytes() == watermark
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize(('backup', 'replay'), [('reserved', True), ('completed', True), ('partial', True),
                                               ('mixed', False), ('mixed', True)])
@pytest.mark.parametrize('posted', [False, True])
def test_verified_stale_repair_releases_its_restored_guards(tmp_path, monkeypatch, backup, posted, replay):
    # Round 25 finding 3
    # Round 24 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    table, root = plans[1].table, write_journal._root()
    owner = native_writer._receipt_id(ack['identity']['plan_hashes'][0], 'run-1', ack['plan_sha256'])
    db.sql.execute(f"CREATE TABLE saved AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        if backup == 'completed':
            patch.setattr(write_journal, 'retire_operations', _r22_stop)
        else:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    assert bool(write_journal.completed_intent(path.stem)) == (backup == 'completed')
    scopes = [native_writer._target_scope(p) for p in plans]
    first_reservation, = write_journal.bundle_reservations()
    assert first_reservation['intent'] == path.stem
    assert all(write_journal._read_record(write_journal._scope_owner_path(s))['intent'] == path.stem for s in scopes)
    shutil.copytree(root, tmp_path / 'first')
    assert write(db, plans) == ack
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    if not posted:
        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
            with pytest.raises(OSError):
                write(db, plans)
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM saved')
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_release_packet', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    later_guard = write_journal._scope_owner_path(scopes[1])
    later_bytes = later_guard.read_bytes()
    assert write_journal._read_record(later_guard)['intent'] != path.stem
    assert _r20_recover(db, CoreWriter(db), plans, True) == ack
    root.rename(tmp_path / 'second')
    (tmp_path / 'first').rename(root)
    if backup == 'partial':
        later_guard.unlink()
    elif backup == 'mixed':
        original_guard = later_guard.read_bytes()
        later_guard.write_bytes(later_bytes)
    head = write_journal.submission_head(owner)
    watermark = write_journal._packet_watermark(owner).read_bytes()
    assert write_journal.submission_retired(head) == posted
    if not posted:
        assert write_journal.reservation_release_started(head)
    writer, start = CoreWriter(db), len(db.events)
    db.sql.execute(f'CREATE TABLE receipts AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and list(write_journal.bundle_reservations())
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipts')
    if backup == 'mixed':
        # Round 33 finding 4
        # Round 32 finding 5
        protected = _r26_file_bytes([path, *_r27_guards(plans, owner)])
        with pytest.raises(RuntimeError, match='guard generation'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
        _r28_no_dml(db, writer, start)
        later_guard.write_bytes(original_guard)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not list(write_journal.bundle_reservations())
    assert write_journal.scope_owner(scopes[0]) is None
    assert not later_guard.exists()
    assert write_journal.submission_head(owner) == head and write_journal._packet_watermark(owner).read_bytes() == watermark
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='next', clock=LATER)['verified']


def test_verified_obsolete_squad_repair_records_terminal_completion(tmp_path, monkeypatch):
    # Round 24 finding 4
    db, plans = CoreDB(), [_club_plan(club, 'original-' + club, CLOCK) for club in ('11', '22')]
    ack = write(db, plans)
    root, table = write_journal._root(), plans[0].table
    owner = native_writer._receipt_id(ack['identity']['plan_hashes'][0], 'run-1', ack['plan_sha256'])
    db.sql.execute(f"DELETE FROM {table} WHERE club_id='22'")
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_scopes', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    shutil.copytree(root, tmp_path / 'early')
    assert write(db, plans) == ack
    db.sql.execute(f"DELETE FROM {table} WHERE club_id='22'")
    assert write(db, plans) == ack
    root.rename(tmp_path / 'latest')
    (tmp_path / 'early').rename(root)
    (path, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert write_journal.submission_retired(head)
    writer, start = CoreWriter(db), len(db.events)
    db.sql.execute(f'CREATE TABLE receipts AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (owner,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert path.exists() and write_journal.completed_intent(path.stem) is None
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=(SELECT physical_hash FROM receipts WHERE receipt_id=?) '
                   'WHERE receipt_id=?', (owner, owner))
    assert _r20_recover(db, writer, plans, True) == ack
    completion = write_journal.completed_intent(path.stem)
    assert completion['terminal'] and completion['acknowledgement'] == native_writer._digest(ack)
    assert not path.exists() and write_journal.submission_head(owner) == head
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, [_club_plan('22', 'next', LATER)], run_id='next', clock=LATER)['verified']


def test_preexisting_original_completion_authenticates_later_publication_link(tmp_path, monkeypatch):
    # Round 24 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    root = write_journal._root()
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    assert 'reservation_head' not in write_journal.completed_intent(path.stem)
    shutil.copytree(root, tmp_path / 'completed')
    ack = write(db, plans)
    owner, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_merge_receipts', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    root.rename(tmp_path / 'reserved')
    (tmp_path / 'completed').rename(root)
    head = write_journal.submission_head(owner)
    assert path.stem in head['after_publication'] and 'statement' not in head
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, False)
    assert 'reservation_head' not in write_journal.completed_intent(path.stem) and path.exists()
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_release_packet', _r22_stop)
        with pytest.raises(OSError):
            _r20_recover(db, writer, plans, True)
    assert write_journal.completed_intent(path.stem)['reservation_head'] == native_writer._digest(head)
    assert _r20_recover(db, writer, plans, True) == ack
    assert not path.exists() and write_journal.reservation_released(head)
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('replay', [False, True])
def test_linked_obsolete_receipt_worklist_releases_only_after_full_readback(tmp_path, monkeypatch, replay):
    # Round 25 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]
    root = write_journal._root()
    db.sql.execute(f'CREATE TABLE receipts AS SELECT * FROM {RECEIPTS}')
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_merge_receipts', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    assert write_journal.submission_head(owner)['reservation'] == path.stem
    shutil.copytree(root, tmp_path / 'worklist')
    assert write(db, plans) == ack
    db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    assert write(db, plans) == ack
    root.rename(tmp_path / 'latest')
    (tmp_path / 'worklist').rename(root)
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head) and write_journal.submission_retired(head)
    assert path.stem in head['statement']['operation']['after_publication']
    assert native_writer._digest(head) != body['payload']['predecessor_head']
    watermark = write_journal._packet_watermark(owner).read_bytes()
    writer, start = CoreWriter(db), len(db.events)
    for member in (owner, tail):
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert path.exists() and list(write_journal.bundle_reservations())
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipts WHERE receipt_id=?', (member,))
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not list(write_journal.bundle_reservations())
    assert all(write_journal.scope_owner(native_writer._target_scope(p)) is None for p in plans)
    assert write_journal.submission_head(owner) == head and write_journal._packet_watermark(owner).read_bytes() == watermark
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='next', clock=LATER)['verified']


@pytest.mark.parametrize('posted', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_partial_release_leaves_foreign_target_settlement_to_its_owner(monkeypatch, posted, replay):
    # Round 25 finding 6
    db, plans, ack, members = _r23_general_reservation(monkeypatch)
    seven, nine = [native_writer._target_scope(p) for p in plans]
    if posted:
        db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    release = write_journal.release_scope

    def stop_after_seven(scope, owner):
        release(scope, owner)
        assert scope == seven
        _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'release_scope', stop_after_seven)
        with pytest.raises(OSError):
            write(db, plans)
    assert write_journal.submission_retired(write_journal.submission_head(members[0])) == posted
    marker = write_journal._record_path('settled-statements', write_journal._statement_path(seven).stem)
    saved = []
    unlink = write_journal._unlink_record

    def save_marker(path):
        if path == marker and path.exists():
            saved.append(path.read_bytes())
        unlink(path)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', save_marker)
        foreign_ack = write(db, _representation_packet(('7',), LATER), run_id='first-seven', clock=LATER)
    marker_bytes, = saved
    first = native_writer._receipt_id(foreign_ack['identity']['plan_hashes'][0], 'first-seven', foreign_ack['plan_sha256'])
    assert write_journal._decode_record(marker_bytes)['statement']['operation']['owner'] == first

    class QueuedWriter(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _submit_lost_merge(self.db, query_id='queued-seven')

    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, QueuedWriter(db), _representation_packet(('7',), NEWEST),
                                        run_id='second-seven', universe_version='core-universe-v1', committed_at=NEWEST)
    foreign = write_journal.pending_statement(seven)
    mapping = write_journal._record_path('statement-queries', foreign['attempt'])
    mapping_bytes = mapping.read_bytes()
    assert write_journal.statement_query(foreign) == 'queued-seven'
    marker.write_bytes(marker_bytes)
    writer, start = CoreWriter(db), len(db.events)
    db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    with pytest.raises(RuntimeError, match='physical readback'):
        _r20_recover(db, writer, plans, replay)
    assert write_journal.scope_owner(nine) == members[0]
    db.sql.execute(f'INSERT INTO {plans[1].table} SELECT * FROM saved_target')
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(StaleTransfermarktWrite) as conflict:
            _r20_recover(db, writer, plans, False)
        assert conflict.value.owner == foreign['operation']['owner']
    assert not write_journal.pending_intents({'run_id': 'run-1'}) and write_journal.scope_owner(nine) is None
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']
    assert mapping.read_bytes() == mapping_bytes and marker.read_bytes() == marker_bytes
    assert write_journal._read_record(write_journal._statement_path(seven)) == foreign


def _r26_snapshot(monkeypatch, directory, module, method, run):
    original = getattr(module, method)

    def capture(*args, **kwargs):
        if not directory.exists():
            shutil.copytree(write_journal._root(), directory)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(module, method, capture)
        return run()


def _r26_members(ack):
    return [native_writer._receipt_id(h, 'run-1', ack['plan_sha256']) for h in ack['identity']['plan_hashes']]


def _r26_restore(snapshot):
    root = write_journal._root()
    displaced = snapshot.with_name('displaced')
    root.rename(displaced)
    snapshot.rename(root)
    return displaced


def _r26_file_bytes(paths):
    return {path: path.read_bytes() if path.exists() else None for path in paths}


@pytest.mark.parametrize('snapshot', ['publication', 'clean'])
def test_active_target_requires_repair_descriptor_before_dml(tmp_path, monkeypatch, snapshot):
    # Round 26 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup = tmp_path / 'original'
    if snapshot == 'publication':
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
        write_journal._index_path().unlink()
    else:
        ack = write(db, plans)
        shutil.copytree(write_journal._root(), backup)
    owner, _ = _r26_members(ack)
    table = plans[0].table
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head) and not write_journal.submission_retired(head)
    assert bool(head['statement']['operation'].get('after_publication')) == (snapshot == 'clean')
    _r26_restore(backup)
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    protected = _r26_file_bytes([write_journal._submission_path(owner), write_journal._packet_watermark(owner),
                                 *(path for path, _ in write_journal.pending_intents({}))])
    writer, start = CoreWriter(db), len(db.events)
    for replay in ([True, False] if snapshot == 'publication' else [False]):
        with pytest.raises(StaleTransfermarktWrite, match='descriptor'):
            _r20_recover(db, writer, plans, replay)
    assert protected == _r26_file_bytes(protected)
    assert _receipt_count(db) == 1 and not writer.writes
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('before_reservation', [False, True])
def test_linked_target_verifies_siblings_before_local_cleanup(tmp_path, monkeypatch, before_reservation):
    # Round 26 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup, scopes = tmp_path / 'original', [native_writer._target_scope(p) for p in plans]
    if before_reservation:
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'reserve_scopes', lambda: write(db, plans))
    else:
        release = write_journal.release_scope

        def snapshot_release(scope, owner):
            release(scope, owner)
            if scope == scopes[0]:
                shutil.copytree(write_journal._root(), backup)

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'release_scope', snapshot_release)
            ack = write(db, plans)
    owner, tail = _r26_members(ack)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, _LostResponse(db), plans, False)
    statement = write_journal.pending_statement(scopes[0])
    displaced = _r26_restore(backup)
    for directory in ('pending-statements', 'dispatch-attempts', 'statement-queries'):
        shutil.copytree(displaced / directory, write_journal._root() / directory, dirs_exist_ok=True)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    protected = _r26_file_bytes([
        write_journal._record_path('reserving-bundles', owner), *(write_journal._scope_owner_path(s) for s in scopes),
        write_journal._statement_path(scopes[0]), write_journal._attempt_path(scopes[0]),
        write_journal._record_path('statement-queries', statement['attempt']),
        write_journal._operation_path(scopes[0], scopes[0], owner),
    ])
    assert write_journal.statement_query(statement) == 'q_lost_merge'
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('later_post', [False, True])
@pytest.mark.parametrize('completed', [False, True])
def test_linked_receipt_worklist_routes_before_completion_or_predecessor(tmp_path, monkeypatch, later_post, completed):
    # Round 26 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    backup = tmp_path / 'worklist'
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    module, method = (write_journal, 'retire_operations') if completed else (native_writer, '_merge_receipts')
    assert _r26_snapshot(monkeypatch, backup, module, method, lambda: write(db, plans)) == ack
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE exact AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    with monkeypatch.context() as patch:
        if later_post:
            patch.setattr(write_journal, 'retire_operations', _r22_stop)
        else:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    if not later_post:
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM exact')
        assert write(db, plans) == ack
    _r26_restore(backup)
    (path, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert path.stem in (head['statement']['operation'] if later_post else head)['after_publication']
    assert bool(write_journal.completed_intent(path.stem)) == completed
    db.sql.execute(f'CREATE TABLE exact_receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert path.exists() and len(list(write_journal.bundle_reservations())) == 1
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM exact_receipt')
    assert _r20_recover(db, writer, plans, completed) == ack
    assert not path.exists() and not list(write_journal.bundle_reservations())
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)['verified']


@pytest.mark.parametrize('worklist', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_interrupted_obsolete_release_checks_only_remaining_tagged_member(tmp_path, monkeypatch, worklist, replay):
    # Round 26 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    members = _r26_members(ack)
    table = plans[1].table
    backup = tmp_path / 'obsolete'
    if worklist:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[1],))
    else:
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    assert _r26_snapshot(monkeypatch, backup, native_writer, '_merge_receipts' if worklist else '_write_plan',
                         lambda: write(db, plans)) == ack
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    assert write(db, plans) == ack
    _r26_restore(backup)
    scopes = [native_writer._target_scope(p) for p in plans]
    guards = [write_journal._scope_owner_path(s) for s in scopes]
    unlink = write_journal._unlink_record

    def stop_after_owner(path):
        unlink(path)
        if path in guards:
            _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', stop_after_owner)
        with pytest.raises(OSError):
            _r20_recover(db, CoreWriter(db), plans, True)
    released, = [i for i, guard in enumerate(guards) if not guard.exists()]
    remaining = 1 - released

    class QueuedWriter(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _submit_lost_merge(self.db, query_id='foreign-target')

    later = _representation_packet(plans[released].scope_values, LATER)
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, QueuedWriter(db), later, run_id='foreign-target',
                                        universe_version='core-universe-v1', committed_at=LATER)
    foreign = write_journal.pending_statement(scopes[released])
    protected = _r26_file_bytes([write_journal._statement_path(scopes[released]),
                                 write_journal._attempt_path(scopes[released]),
                                 write_journal._record_path('statement-queries', foreign['attempt'])])
    db.sql.execute(f'CREATE TABLE remaining_receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?',
                   (members[remaining],))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[remaining],))
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert guards[remaining].exists() and _r26_file_bytes(protected) == protected
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM remaining_receipt')
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(StaleTransfermarktWrite) as conflict:
            _r20_recover(db, writer, plans, False)
        assert conflict.value.owner == foreign['operation']['owner']
    assert not guards[remaining].exists() and not write_journal.pending_intents({'run_id': 'run-1'})
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(plans[remaining].scope_values, LATER), run_id='next', clock=LATER)['verified']
    assert _r26_file_bytes(protected) == protected


@pytest.mark.parametrize('replay', [True, False])
def test_retired_target_remnants_verified_before_finishing_publication(tmp_path, monkeypatch, replay):
    # Round 26 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    members = _r26_members(ack)
    scopes = [native_writer._target_scope(p) for p in plans]
    records = [write_journal._operation_path(s, s, m) for s, m in zip(scopes, members)]
    unlink = write_journal._unlink_record

    def stop_retired(path):
        if path in records and write_journal.submission_retired(write_journal.submission_head(members[0])):
            _r22_stop()
        unlink(path)

    db.sql.execute(f'DELETE FROM {plans[0].table}')
    with monkeypatch.context() as patch:
        retire = write_journal.retire_operations
        order = list(reversed(scopes)) if replay else scopes
        patch.setattr(write_journal, 'retire_operations', lambda requested, owner: retire(order, owner))
        patch.setattr(write_journal, '_unlink_record', stop_retired)
        with pytest.raises(OSError):
            write(db, plans)
    _r26_restore(backup)
    assert records[-1].exists()
    (path, _), = write_journal.pending_intents({})
    writer, start = CoreWriter(db), len(db.events)
    # The retired head still authenticates its last operation. An unknown
    # outcome for that same attempt must retain both fences.
    terminal = write_journal._read_record(records[-1])
    write_journal._store_record(records[-1], {**terminal, 'state': 'dispatched'}, replace=True)
    with pytest.raises(StaleTransfermarktWrite):
        _r20_recover(db, writer, plans, replay)
    assert path.exists() and records[-1].exists()
    write_journal._store_record(records[-1], terminal, replace=True)
    if replay:
        sibling = write_journal._read_record(records[0])
        earlier = {**sibling['operation'], 'intent': path.stem}
        conflict = {**sibling, 'operation': earlier, 'statement': {**sibling['statement'], 'operation': earlier}}
        write_journal._store_record(records[0], conflict, replace=True)
        protected = _r26_file_bytes([path, *records])
        with pytest.raises(RuntimeError, match='retiring target operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
        write_journal._store_record(records[0], sibling, replace=True)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not any(r.exists() for r in records)
    assert not writer.writes and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


def _r27_dispatch_files(statement):
    key = statement['key']
    pending = write_journal._statement_path(key)
    return [pending, write_journal._attempt_path(key), write_journal._record_path('settled-statements', pending.stem),
            write_journal._record_path('statement-queries', statement['attempt'])]


def _r27_guards(plans, owner):
    return [write_journal._record_path('reserving-bundles', owner), write_journal._submission_path(owner),
            write_journal._packet_watermark(owner), *(write_journal._scope_owner_path(native_writer._target_scope(p))
                                                       for p in plans)]


def _r27_restore_dispatch(snapshot, statement):
    root = write_journal._root()
    displaced = _r26_restore(snapshot)
    for path in _r27_dispatch_files(statement):
        source = displaced / path.relative_to(root)
        if source.exists():
            path.parent.mkdir(exist_ok=True)
            shutil.copyfile(source, path)


@pytest.mark.parametrize('snapshot', ['publication', 'clean'])
def test_r27_active_attendance_walk_requires_current_descriptor(tmp_path, monkeypatch, snapshot):
    # Round 27 finding 1
    from tests.unit.scrapers import test_transfermarkt_sections as attendance

    captures = [attendance._attendance_capture(name, url, clock) for name, url, clock in (
        ('page_1.html', attendance.ATTENDANCE_START, attendance.PAGE_1_CAPTURED),
        ('page_2.html', attendance.ATTENDANCE_PAGE_2, attendance.PAGE_2_CAPTURED))]
    old = []
    for capture in captures:
        body = capture.body + b'<!-- earlier edition -->'
        digest = hashlib.sha256(body).hexdigest()
        old.append(dataclasses_replace(capture, body=body, body_hash=digest, capture_id=digest,
                                       captured_at=capture.captured_at.replace('T12:', 'T09:')))
    earlier = attendance._attendance_plan(old, extracted_at=attendance.EXTRACTED_AT.replace('T12:', 'T10:'))
    plan = attendance._attendance_plan(captures)
    db, backup = CoreDB(), tmp_path / 'published'
    writer = CoreWriter(db)

    def commit(edition):
        return native_writer.write_core_bundle(lambda: db, writer, [edition], run_id=edition.rows[0]['run_id'],
                                               universe_version=attendance.UNIVERSE,
                                               committed_at=edition.rows[0]['extracted_at'])

    commit(earlier)
    db.sql.execute(f'CREATE TABLE earlier AS SELECT * FROM {plan.table}')
    if snapshot == 'publication':
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: commit(plan))
        write_journal._index_path().unlink()  # the later repair has no publication link
    else:
        ack = commit(plan)
        shutil.copytree(write_journal._root(), backup)
    run = ack['run_id']
    member = native_writer._receipt_id(plan.plan_sha256(), run, ack['plan_sha256'])
    db.sql.execute(f'DELETE FROM {plan.table}')
    db.sql.execute(f'INSERT INTO {plan.table} SELECT * FROM earlier')
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (member,))
    start = len(db.events)
    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', _r22_stop)
        with pytest.raises(OSError, match='interrupted reservation'):
            commit(plan)
    (path, _), = write_journal.pending_intents({'run_id': run})
    head = write_journal.submission_head(member)
    assert write_journal.submission_is_current(head) and not write_journal.submission_retired(head)
    assert head['statement']['key'] == [native_writer.CORE_PAGES_TABLE, member]
    assert head['statement']['operation']['intent'] == path.stem
    assert bool(head['statement']['operation'].get('after_publication')) == (snapshot == 'clean')
    assert any(q.startswith('INSERT INTO ' + native_writer.CORE_PAGES_TABLE) for q in db.events[start:])
    assert native_writer._target_matches(db, earlier) and not native_writer._target_matches(db, plan)
    assert len(native_writer._stored_walk(db, member)) == len(captures)
    _r26_restore(backup)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    protected = _r26_file_bytes(_r27_guards([plan], member) + [p for p, _ in write_journal.pending_intents({})])
    start = len(db.events)
    writer.writes.clear()
    if snapshot == 'publication':
        with pytest.raises(StaleTransfermarktWrite, match='descriptor'):
            native_writer.replay_pending_intents(lambda: db, writer, run_id=run)
    with pytest.raises(StaleTransfermarktWrite, match='descriptor'):
        commit(plan)
    assert _r26_file_bytes(protected) == protected and native_writer._target_matches(db, earlier)
    assert not native_writer._stored_receipts(db, [member])
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])


def test_r27_active_target_refuses_genuine_conflicting_sibling(tmp_path, monkeypatch):
    # Round 27 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    scope = native_writer._target_scope(plans[0])
    backup = tmp_path / 'publication'
    retire = write_journal.retire_operations
    original = {}

    def save_original(scopes, owner):
        record = write_journal._operation_path(scope, scope, owner)
        original[record] = record.read_bytes()
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', save_original)
        ack = _r26_snapshot(patch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, _ = _r26_members(ack)
    db.sql.execute(f'DELETE FROM {plans[0].table}')
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    _r26_restore(backup)
    record, = original
    record.write_bytes(original[record])
    head = write_journal.submission_head(owner)
    earlier = write_journal._read_record(record)
    (path, _), = write_journal.pending_intents({})
    assert write_journal.submission_is_current(head) and not write_journal.submission_retired(head)
    assert earlier['state'] == 'returned' and earlier['statement']['attempt'] != head['members'][owner]
    assert earlier['operation']['intent'] == path.stem != head['statement']['operation']['intent']
    assert path.stem in head['statement']['operation']['after_publication']
    protected = _r26_file_bytes([record, path, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    for replay in (False, True):
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
    with pytest.raises(StaleTransfermarktWrite):
        write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])


@pytest.mark.parametrize('lane', ['target', 'receipt', 'current-receipt'])
def test_r27_packet_verification_leaves_dispatch_and_guards_intact(tmp_path, monkeypatch, lane):
    # Round 27 finding 3
    db = CoreDB()
    plans = _representation_packet(('7',) if lane == 'target' else ('7', '9'), CLOCK)
    backup = tmp_path / 'before-repair'
    if lane == 'target':
        ack = _r26_snapshot(monkeypatch, backup, native_writer, '_write_plan', lambda: write(db, plans))
        db.sql.execute(f'DELETE FROM {plans[0].table}')
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, _LostResponse(db), plans, False)
    else:
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[0],))
        with monkeypatch.context() as patch:
            _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
            unlink = write_journal._unlink_record

            def stop_before_mapping(path):
                if path.parent.name == 'statement-queries' and path.exists():
                    _r22_stop()
                unlink(path)

            patch.setattr(write_journal, '_unlink_record', stop_before_mapping)
            with pytest.raises(OSError):
                write(db, plans)
    members = _r26_members(ack)
    statement = write_journal.submission_head(members[0])['statement']
    if lane != 'current-receipt':
        _r27_restore_dispatch(backup, statement)
    (path, _), = write_journal.pending_intents({})
    if lane == 'current-receipt':
        assert path.stem == statement['operation']['intent']
    else:
        assert path.stem in statement['operation']['after_publication']
    assert native_writer._query_outcome(db, statement) == 'finished'
    dispatch = _r27_dispatch_files(statement)
    assert all(p.exists() for p in (dispatch if lane != 'target' else [dispatch[0], dispatch[1], dispatch[3]]))
    assert all(native_writer._target_matches(db, p) for p in plans)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (members[-1],))
    record = write_journal._operation_path(statement['operation']['scope'], statement['key'], members[0])
    protected = _r26_file_bytes([*dispatch, *_r27_guards(plans, members[0]), record, path])
    writer, start = CoreWriter(db), len(db.events)
    for replay in (True, False):
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])


@pytest.mark.parametrize('lane', ['receipt', 'reservation'])
@pytest.mark.parametrize('replay', [False, True])
def test_r27_linked_completed_worklist_before_predecessor(tmp_path, monkeypatch, lane, replay):
    # Round 27 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    backup, table = tmp_path / 'worklist', plans[0].table
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r26_snapshot(monkeypatch, backup, write_journal, 'retire_operations', lambda: write(db, plans))
    if lane == 'receipt':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
        with monkeypatch.context() as patch:
            _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
            patch.setattr(write_journal, 'retire_operations', _r22_stop)
            with pytest.raises(OSError):
                write(db, plans)
    else:
        db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
        with monkeypatch.context() as patch:
            patch.setattr(native_writer, '_write_plan', _r22_stop)
            with pytest.raises(OSError):
                write(db, plans)
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    _r26_restore(backup)
    (path, body), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert write_journal.completed_intent(path.stem) and write_journal.submission_is_current(head)
    assert native_writer._digest(head) != body['payload']['predecessor_head']
    if lane == 'receipt':
        assert not write_journal.submission_retired(head)
        last = head['statement']
        assert last['key'] == [native_writer.CORE_RECEIPTS_TABLE, owner]
        operation = write_journal.latest_operation(last['operation']['scope'], last['key'], owner)
        assert native_writer._query_outcome(db, last, query_id=operation['query_id']) == 'finished'
        assert path.stem in last['operation']['after_publication']
    else:
        assert 'statement' not in head and not write_journal.reservation_release_started(head)
        assert path.stem in head['after_publication']
    protected = _r26_file_bytes([path, *_r27_guards(plans, owner)])
    db.sql.execute(f'CREATE TABLE sibling AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM sibling')
    assert _r20_recover(db, writer, plans, replay) == ack and not path.exists()
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])
    assert write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)['verified']


@pytest.mark.parametrize('lane', ['receipt', 'general', 'rejection'])
@pytest.mark.parametrize('replay', [True, False])
def test_r27_terminal_remnants_retire_before_linked_descriptor(tmp_path, monkeypatch, lane, replay):
    # Round 27 finding 5
    # Round 27 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup, table = tmp_path / 'linked', plans[0].table
    if lane == 'general':
        ack = write(db, plans)
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
        _r26_snapshot(monkeypatch, backup, native_writer, '_write_plan', lambda: write(db, plans))
    else:
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, tail = _r26_members(ack)
    scopes = [native_writer._target_scope(p) for p in plans]
    key = (native_writer.CORE_RECEIPTS_TABLE, tail) if lane == 'receipt' else scopes[1]
    remnant = write_journal._operation_path(scopes[1], key, tail)
    if lane == 'receipt':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    elif lane == 'general':
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    else:
        class Rejected(CoreWriter):
            dispatches_journalled_target = True

            def _write_to_iceberg(self, *args, **kwargs):
                _rejected_target(db, TrinoAmbiguousWriteError('lost rejection'))

        db.sql.execute(f"CREATE TABLE seven AS SELECT * FROM {table} WHERE player_id='7'")
        db.sql.execute(f'DELETE FROM {table}')
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, Rejected(db), plans, False)
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM seven')
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        retire, unlink = write_journal.retire_operations, write_journal._unlink_record

        def interrupt(path):
            if path == remnant and write_journal.submission_retired(write_journal.submission_head(owner)):
                _r22_stop()
            unlink(path)

        patch.setattr(write_journal, '_unlink_record', interrupt)
        patch.setattr(write_journal, 'retire_operations', lambda requested, packet: retire(reversed(scopes), packet))
        with pytest.raises(OSError):
            write(db, plans)
    _r26_restore(backup)
    (path, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head) and write_journal.submission_retired(head)
    assert path.stem in head['statement']['operation']['after_publication']
    assert write_journal._read_record(remnant)['state'] == 'returned'
    writer, start = CoreWriter(db), len(db.events)
    if lane == 'rejection':
        rejected = write_journal.latest_operation(scopes[0], scopes[0], owner)
        assert rejected['state'] == 'rejected' and rejected['statement']['attempt'] == head['members'][owner]
        assert rejected['operation']['intent'] == head['statement']['operation']['intent']
        query = rejected['query_id']
        protected = _r26_file_bytes([path, remnant, write_journal._operation_path(scopes[0], scopes[0], owner),
                                     *_r27_guards(plans, owner)])
        db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query,))
        with pytest.raises(StaleTransfermarktWrite, match='outcome is still pending'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
        db.sql.execute("UPDATE test_query_history SET state='FAILED' WHERE query_id=?", (query,))
        assert native_writer._query_outcome(db, rejected['statement'], query_id=query) == 'rejected'
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not any(list(write_journal.scope_operations(s)) for s in scopes)
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [True, False])
def test_r27_verified_obsolete_target_never_reacquires_released_scope(tmp_path, monkeypatch, replay):
    # Round 27 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    seven, nine = [native_writer._target_scope(p) for p in plans]
    backup = tmp_path / 'partial'
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    release = write_journal.release_scope

    def snapshot(scope, packet):
        release(scope, packet)
        if scope == seven:
            shutil.copytree(write_journal._root(), backup)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'release_scope', snapshot)
        assert write(db, plans) == ack
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, _LostResponse(db), plans, False)
    statement = write_journal.submission_head(owner)['statement']
    _r27_restore_dispatch(backup, statement)
    (path, body), = write_journal.pending_intents({})
    assert body['payload']['receipt_worklist'] and path.stem in statement['operation']['after_publication']
    assert native_writer._query_outcome(db, statement) == 'finished'
    assert write_journal.scope_owner(seven) is None and write_journal.scope_owner(nine) == owner
    writer, start = CoreWriter(db), len(db.events)
    with monkeypatch.context() as patch:
        hold = write_journal.hold_scope

        def check_scope(scope, packet):
            assert scope != seven
            hold(scope, packet)

        patch.setattr(write_journal, 'hold_scope', check_scope)
        assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not write_journal.statement_query(statement)
    assert all(write_journal.scope_owner(s) is None for s in (seven, nine))
    assert not writer.writes and not any(native_writer.is_mutation(q) for q in db.events[start:])
    assert write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)['verified']


def _r28_interrupt(monkeypatch, module, method, run):
    with monkeypatch.context() as patch:
        patch.setattr(module, method, _r22_stop)
        with pytest.raises(OSError, match='interrupted reservation'):
            run()


def _r28_no_dml(db, writer, start):
    assert not writer.writes
    assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])


@pytest.mark.parametrize('completed', [False, True])
def test_r28_completed_current_target_checks_each_surviving_attempt(tmp_path, monkeypatch, completed):
    # Round 29 finding 1
    # Round 28 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    seven = native_writer._target_scope(plans[0])
    saved = {}

    def capture(scopes, owner):
        record = write_journal._operation_path(seven, seven, owner)
        saved[record] = record.read_bytes()
        retire(scopes, owner)

    retire = write_journal.retire_operations
    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', capture)
        ack = write(db, plans)
    owner, _ = _r26_members(ack)
    db.sql.execute(f'DELETE FROM {plans[0].table}')
    module, boundary = ((write_journal, 'retire_operations') if completed else (native_writer, '_complete_packet'))
    _r28_interrupt(monkeypatch, module, boundary, lambda: write(db, plans))
    (path, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert bool(write_journal.completed_intent(path.stem)) == completed
    assert head['statement']['operation']['intent'] == path.stem
    record, = saved
    genuine = record.read_bytes()
    record.write_bytes(saved[record])
    earlier = write_journal._read_record(record)
    assert earlier['state'] == 'returned' and earlier['operation']['intent'] != path.stem
    assert earlier['statement']['attempt'] != head['members'][owner]
    protected = _r26_file_bytes([record, path, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    for replay in (False, True):
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert protected == _r26_file_bytes(protected)
    with pytest.raises(StaleTransfermarktWrite):
        write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)
    _r28_no_dml(db, writer, start)
    assert bool(write_journal.completed_intent(path.stem)) == completed
    record.write_bytes(genuine)
    assert _r20_recover(db, writer, plans, True) == ack
    assert not writer.writes and not path.exists() and not record.exists()


@pytest.mark.parametrize('completed', [False, True])
def test_r28_receipt_cleanup_waits_for_whole_packet(monkeypatch, completed):
    # Round 28 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner, tail = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    if completed:
        write(db, plans)
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    snapshot = {}
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        unlink = write_journal._unlink_record

        def settlement(path):
            if path.parent.name == 'statement-queries' and path.exists():
                statement = write_journal._read_record(path)['statement']
                snapshot.update(_r26_file_bytes(_r27_dispatch_files(statement)))
                if not completed:
                    _r22_stop()
            unlink(path)

        patch.setattr(write_journal, '_unlink_record', settlement)
        if completed:
            patch.setattr(write_journal, 'retire_operations', _r22_stop)
        with pytest.raises(OSError):
            write(db, plans)
    assert len(snapshot) == 4 and all(snapshot.values())
    if completed:
        for path, data in snapshot.items():
            path.write_bytes(data)
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    else:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='9'")
    (descriptor, _), = write_journal.pending_intents({})
    assert bool(write_journal.completed_intent(descriptor.stem)) == completed
    protected = _r26_file_bytes([descriptor, *snapshot, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    for replay in (False, True):
        with pytest.raises(RuntimeError, match='readback differs'):
            _r20_recover(db, writer, plans, replay)
        assert protected == _r26_file_bytes(protected)
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('snapshot', ['general', 'completed-general', 'original'])
@pytest.mark.parametrize('replay', [True, False])
def test_r28_reservation_links_precede_completion_and_guard_materialization(tmp_path, monkeypatch, snapshot, replay):
    # Round 28 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup, table = tmp_path / 'older', plans[0].table
    if snapshot == 'original':
        hold = write_journal.hold_scope

        def first_guard(scope, owner):
            hold(scope, owner)
            if not backup.exists():
                shutil.copytree(write_journal._root(), backup)

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'hold_scope', first_guard)
            ack = write(db, plans)
    else:
        ack = write(db, plans)
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
        module, method = ((native_writer, '_write_plan') if snapshot == 'general'
                          else (write_journal, 'retire_operations'))
        assert _r26_snapshot(monkeypatch, backup, module, method, lambda: write(db, plans)) == ack
    owner, tail = _r26_members(ack)
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    _r28_interrupt(monkeypatch, native_writer, '_write_plan', lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    displaced = _r26_restore(backup)
    (descriptor, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head) and 'statement' not in head
    assert descriptor.stem in head['after_publication']
    guards = _r27_guards(plans, owner)
    assert sum(path.exists() for path in guards[-2:]) == (1 if snapshot == 'original' else 2)
    db.sql.execute(f'CREATE TABLE receipt AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    protected = _r26_file_bytes([descriptor, *guards])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert protected == _r26_file_bytes(protected)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipt')
    # A pre-POST link cannot release a guard acquired by the missing descriptor.
    guard = next(g for g in guards[-2:] if g.exists())
    original = guard.read_bytes()
    shutil.copyfile(displaced / guard.relative_to(write_journal._root()), guard)
    mixed = _r26_file_bytes([descriptor, *guards])
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(mixed) == mixed
    guard.write_bytes(original)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not descriptor.exists() and not list(write_journal.bundle_reservations())
    _r28_no_dml(db, writer, start)
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('entry', ['write', 'replay', 'partial-release', 'retired-write', 'retired-replay',
                                   'general-retired-write', 'general-retired-replay'])
def test_r28_last_linked_worklist_accounts_for_active_generation_guards(tmp_path, monkeypatch, entry):
    # Round 30 finding 5
    # Round 28 finding 4
    # Round 29 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    backup = tmp_path / 'obsolete'
    if entry.startswith('general-'):
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='9'")
    else:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r26_snapshot(monkeypatch, backup, write_journal, 'retire_operations', lambda: write(db, plans))
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    if 'retired-' in entry:
        _r28_interrupt(monkeypatch, native_writer, '_release_packet', lambda: write(db, plans))
        assert write_journal.submission_retired(write_journal.submission_head(owner))
        assert all(not list(write_journal.scope_operations(native_writer._target_scope(p))) for p in plans)
    else:
        _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    displaced = _r26_restore(backup)
    for directory in ('reserving-bundles', 'scope-owners'):
        shutil.copytree(displaced / directory, write_journal._root() / directory, dirs_exist_ok=True)
    (old, _), = write_journal.pending_intents({})
    op = write_journal.submission_head(owner)['statement']['operation']
    assert old.stem in op['after_publication']
    assert all(write_journal._read_record(g)['intent'] == op['intent']
               for g in _r27_guards(plans, owner) if g.parent.name in ('scope-owners', 'reserving-bundles'))
    writer, start = CoreWriter(db), len(db.events)
    if entry == 'partial-release':
        unlink = write_journal._unlink_record

        def stop_after_guard(path):
            unlink(path)
            if path.parent.name == 'scope-owners':
                _r22_stop()

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, '_unlink_record', stop_after_guard)
            with pytest.raises(OSError):
                _r20_recover(db, writer, plans, True)
        assert old.exists() and write_journal.submission_retired(write_journal.submission_head(owner))
        released, = [p for p in plans if write_journal.scope_owner(native_writer._target_scope(p)) is None]
        _r28_no_dml(db, writer, start)
        assert write(db, _representation_packet(released.scope_values, LATER), run_id='released', clock=LATER)['verified']
        start = len(db.events)
    assert _r20_recover(db, writer, plans, not entry.endswith('write')) == ack
    assert not old.exists() and not list(write_journal.bundle_reservations())
    _r28_no_dml(db, writer, start)
    next_scope = ('7',) if entry == 'partial-release' and released.scope_values == ('9',) else ('9',)
    assert write(db, _representation_packet(next_scope, LATER), run_id='next', clock=LATER)['verified']


@pytest.mark.parametrize('prior_head', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_r28_bound_target_then_receipt_both_finished(tmp_path, monkeypatch, replay, prior_head):
    # Round 28 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    # Exercise the persisted r27 head format. Its reservation is the only
    # independent binding for the target once the receipt POST advances the member.
    save = write_journal._save_submission

    def prior_format(head):
        save({key: value for key, value in head.items() if key != 'target_attempts'})

    if prior_head:
        monkeypatch.setattr(write_journal, '_save_submission', prior_format)
    backup = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, tail = _r26_members(ack)
    nine = native_writer._target_scope(plans[1])
    db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, _LostResponse(db), plans, False)
    with monkeypatch.context() as patch:
        posts = _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'lost_receipt',
                                      landed=True, member=tail)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    assert len(posts) == 1
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    _r26_restore(backup)
    (path, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    target = write_journal.latest_operation(nine, nine, tail)
    receipt = write_journal.latest_operation(nine, (native_writer.CORE_RECEIPTS_TABLE, tail), tail)
    assert target and receipt and target['state'] == receipt['state'] == 'FINISHED'
    assert head['receipt_repair']['before'][tail] == target['statement']['attempt']
    assert head['members'][tail] == receipt['statement']['attempt'] != target['statement']['attempt']
    assert path.stem in head['statement']['operation']['after_publication']
    assert all(native_writer._query_outcome(db, r['statement'], query_id=r['query_id']) == 'finished'
               for r in (target, receipt))
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not list(write_journal.scope_operations(nine))
    _r28_no_dml(db, writer, start)
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('descriptor', ['linked', 'retired-linked', 'retired-current', 'retired-current-second-crash'])
def test_r28_linked_dispatched_target_settles_coordinator_rejection(tmp_path, monkeypatch, replay, descriptor):
    # Round 30 finding 8
    # Round 29 finding 8
    # Round 28 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup, table = tmp_path / 'publication', plans[0].table
    ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, _ = _r26_members(ack)
    db.sql.execute(f"CREATE TABLE seven AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")

    class Rejected(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _rejected_target(db, TrinoAmbiguousWriteError('lost rejection'))

    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, Rejected(db), plans, False)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM seven')
    last = write_journal.submission_head(owner)['statement']
    scope = native_writer._target_scope(plans[0])
    record = write_journal._operation_path(scope, scope, owner)
    if descriptor.startswith('retired-'):
        if descriptor.startswith('retired-current'):
            backup = tmp_path / 'current-repair'
            shutil.copytree(write_journal._root(), backup)
        saved = _r26_file_bytes([record, *_r27_dispatch_files(last)])
        assert write(db, plans) == ack
        assert write_journal.submission_retired(write_journal.submission_head(owner))
        _r26_restore(backup)
        for file, data in saved.items():
            if data is not None:
                file.parent.mkdir(exist_ok=True, parents=True)
                file.write_bytes(data)
    else:
        _r27_restore_dispatch(backup, last)
    (path, _), = write_journal.pending_intents({})
    if descriptor.startswith('retired-current'):
        assert path.stem == last['operation']['intent'] and write_journal.completed_intent(path.stem) is None
    else:
        assert path.stem in last['operation']['after_publication']
    assert write_journal._read_record(record)['state'] == 'dispatched'
    query = write_journal.statement_query(last)
    assert query and native_writer._query_outcome(db, last) == 'rejected'
    protected = _r26_file_bytes([path, record, *_r27_dispatch_files(last), *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    db.sql.execute("UPDATE test_query_history SET error_code='UNKNOWN' WHERE query_id=?", (query,))
    with pytest.raises(StaleTransfermarktWrite, match='outcome is still pending'):
        _r20_recover(db, writer, plans, replay)
    assert protected == _r26_file_bytes(protected)
    db.sql.execute("UPDATE test_query_history SET error_code='QUERY_QUEUE_FULL' WHERE query_id=?", (query,))
    if descriptor.startswith('retired-current'):
        _, tail = _r26_members(ack)
        db.sql.execute(f'CREATE TABLE held AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert protected == _r26_file_bytes(protected)
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM held')
    if descriptor == 'retired-current-second-crash':
        # Round 31 finding 9
        immutable = write_journal._immutable_record

        def interrupt_marker(file, data):
            if file.parent.name == 'settled-statements':
                assert write_journal._read_record(record)['state'] == 'rejected'
                _r22_stop()
            immutable(file, data)

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, '_immutable_record', interrupt_marker)
            with pytest.raises(OSError, match='interrupted reservation'):
                _r20_recover(db, writer, plans, replay)
        saved = _r26_file_bytes(protected)
        assert write_journal.read_statement(scope) == last and write_journal.statement_query(last) == query
        db.sql.execute("UPDATE test_query_history SET state='RUNNING' WHERE query_id=?", (query,))
        with pytest.raises(RuntimeError, match='outcome is still pending'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(saved) == saved
        db.sql.execute("UPDATE test_query_history SET state='FAILED' WHERE query_id=?", (query,))
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations',
                   lambda: _r20_recover(db, writer, plans, replay))
    rejected = write_journal._read_record(record)
    assert rejected['state'] == 'rejected' and rejected['query_id'] == query
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not record.exists() and not any(f.exists() for f in _r27_dispatch_files(last))
    _r28_no_dml(db, writer, start)
    assert write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)['verified']


def _r28_overlapping_worklists(tmp_path, monkeypatch, db, plans, predecessor='worklist', *, general=False):
    backup = tmp_path / 'predecessor'
    if predecessor == 'original':
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
    else:
        ack = write(db, plans)
    owner, tail = _r26_members(ack)
    if predecessor == 'worklist':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r26_snapshot(monkeypatch, backup, native_writer, '_merge_receipts', lambda: write(db, plans))
    elif predecessor == 'general':
        db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='9'")
        _r26_snapshot(monkeypatch, backup, write_journal, 'retire_operations', lambda: write(db, plans))
    if general:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    (current, body), = write_journal.pending_intents({})
    for path in [*backup.glob('*.json'), backup / write_journal._index_path().name]:
        shutil.copyfile(path, write_journal._root() / path.name)
    head = write_journal.submission_head(owner)
    assert head['reservation'] == current.stem == head['receipt_repair']['intent']
    if general:
        assert 'receipt_worklist' not in body['payload'] and 'warehouse_identity' in body['payload']
        assert head['statement']['operation']['intent'] == current.stem
    else:
        assert head['receipt_repair'] == {'intent': current.stem, **body['payload']['receipt_worklist']}
    assert head['receipt_repair']['missing'] == [owner]
    old, = [p for p, _ in write_journal.pending_intents({}) if p != current]
    assert old.stem in head['after_publication']
    return ack


@pytest.mark.parametrize('replay', [True, False])
@pytest.mark.parametrize('predecessor', ['worklist', 'original', 'general'])
@pytest.mark.parametrize('general', [False, True])
def test_r28_current_worklist_precedes_linked_descriptor(tmp_path, monkeypatch, replay, predecessor, general):
    # Round 30 finding 9
    # Round 29 finding 9
    # Round 28 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = _r28_overlapping_worklists(tmp_path, monkeypatch, db, plans, predecessor, general=general)
    writer, start = CoreWriter(db), len(db.events)
    # A second missing receipt is outside the current worklist.
    owner, tail = _r26_members(ack)
    db.sql.execute(f'CREATE TABLE held AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    protected = _r26_file_bytes([p for p, _ in write_journal.pending_intents({})] + _r27_guards(plans, owner))
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM held')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not write_journal.pending_intents({}) and not list(write_journal.bundle_reservations())
    assert not writer.writes and [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('linked', [False, True])
@pytest.mark.parametrize('replay', [False, True])
def test_r29_page_then_receipt_uses_each_bound_attempt(tmp_path, monkeypatch, linked, replay):
    # Round 29 finding 2
    from tests.unit.scrapers import test_transfermarkt_sections as attendance

    captures = [attendance._attendance_capture(name, url, clock) for name, url, clock in (
        ('page_1.html', attendance.ATTENDANCE_START, attendance.PAGE_1_CAPTURED),
        ('page_2.html', attendance.ATTENDANCE_PAGE_2, attendance.PAGE_2_CAPTURED))]
    plan = attendance._attendance_plan(captures)
    run = plan.rows[0]['run_id']
    db, backup = CoreDB(), tmp_path / 'publication'
    writer = CoreWriter(db)

    def commit():
        return native_writer.write_core_bundle(lambda: db, writer, [plan], run_id=run,
                                               universe_version=attendance.UNIVERSE, committed_at=attendance.EXTRACTED_AT)

    scope = native_writer._target_scope(plan)
    original_page = []
    retire = write_journal.retire_operations

    def capture_original(scopes, owner):
        key = (native_writer.CORE_PAGES_TABLE, owner)
        original_page.append(write_journal._operation_path(scope, key, owner).read_bytes())
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', capture_original)
        ack = _r26_snapshot(patch, backup, write_journal, 'complete_intent', commit)
    member = native_writer._receipt_id(plan.plan_sha256(), run, ack['plan_sha256'])
    page_record = write_journal._operation_path(scope, (native_writer.CORE_PAGES_TABLE, member), member)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (member,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (member,))
    with monkeypatch.context() as patch:
        # Exercise the persisted r28 format, where the reservation is the
        # only independent binding for this page after the receipt advances.
        save = write_journal._save_submission
        patch.setattr(write_journal, '_save_submission',
                      lambda head: save({k: v for k, v in head.items() if k != 'page_attempts'}))
        _capture_returned_queries(patch, db, [native_writer.CORE_PAGES_TABLE, native_writer.CORE_RECEIPTS_TABLE])
        _r28_interrupt(patch, write_journal, 'retire_operations', commit)
    genuine = page_record.read_bytes()
    head = write_journal.submission_head(member)
    assert head['receipt_repair']['before'][member] == write_journal._read_record(page_record)['statement']['attempt']
    assert head['members'][member] == head['statement']['attempt'] != head['receipt_repair']['before'][member]
    if linked:
        _r26_restore(backup)
    (descriptor, _), = write_journal.pending_intents({})
    assert bool(write_journal.completed_intent(descriptor.stem)) != linked
    writer.writes.clear()
    start = len(db.events)

    def recover():
        if replay:
            result, = native_writer.replay_pending_intents(lambda: db, writer, run_id=run)
            return result
        return commit()

    if not linked:
        page_record.write_bytes(original_page[0])
        protected = _r26_file_bytes([descriptor, page_record, *_r27_guards([plan], member)])
        with pytest.raises(RuntimeError, match='operation binding differs'):
            recover()
        assert _r26_file_bytes(protected) == protected
        page_record.write_bytes(genuine)
    assert recover() == ack and not descriptor.exists() and not page_record.exists()
    _r28_no_dml(db, writer, start)


def _r29_snapshot_receipt_settlements(patch):
    snapshots = {}
    unlink = write_journal._unlink_record

    def capture(path):
        if path.parent.name == 'statement-queries' and path.exists():
            statement = write_journal._read_record(path)['statement']
            op = statement['operation']
            if statement['key'] == [native_writer.CORE_RECEIPTS_TABLE, op['member']]:
                record = write_journal._operation_path(op['scope'], statement['key'], op['member'])
                snapshots[op['member']] = _r26_file_bytes([record, *_r27_dispatch_files(statement)])
        unlink(path)

    patch.setattr(write_journal, '_unlink_record', capture)
    return snapshots


def _r29_restore_files(snapshot):
    for path, data in snapshot.items():
        assert data is not None
        path.write_bytes(data)


@pytest.mark.parametrize('replay', [False, True])
def test_r29_finished_receipt_evidence_survives_sibling_read_error(monkeypatch, replay):
    # Round 29 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner = native_writer._receipt_id(hashes[0], 'run-1', digest)
    with monkeypatch.context() as patch:
        _interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'finished-seven', landed=True, member=owner)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    with monkeypatch.context() as patch:
        settlements = _r29_snapshot_receipt_settlements(patch)
        _r28_interrupt(patch, write_journal, 'complete_intent', lambda: write(db, plans))
    saved = settlements[owner]
    _r29_restore_files(saved)
    receipt_key = (native_writer.CORE_RECEIPTS_TABLE, owner)
    assert write_journal.latest_operation(native_writer._target_scope(plans[0]), receipt_key, owner)['state'] == 'FINISHED'
    (descriptor, _), = write_journal.pending_intents({})
    assert not write_journal.completed_intent(descriptor.stem) and write_journal.read_receipt(digest)
    protected = _r26_file_bytes([descriptor, *saved, *_r27_guards(plans, owner)])
    reads = 0
    execute = CoreCursor.execute

    def lose_connection(cursor, sql, params=()):
        nonlocal reads
        if sql.startswith(f'SELECT * FROM {plans[1].full_table} WHERE source_capture_id = ?'):
            reads += 1
            if reads >= 2:
                raise ConnectionError('lost second target read')
        return execute(cursor, sql, params)

    writer, start = CoreWriter(db), len(db.events)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', lose_connection)
        with pytest.raises(ConnectionError, match='second target'):
            _r20_recover(db, writer, plans, replay)
    assert reads >= 2 and _r26_file_bytes(protected) == protected
    _r28_no_dml(db, writer, start)
    assert _r20_recover(db, writer, plans, replay)['verified'] and not descriptor.exists()


@pytest.mark.parametrize('replay', [False, True])
def test_r29_two_receipt_settlements_bind_before_unlink(monkeypatch, replay):
    # Round 29 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        snapshots = _r29_snapshot_receipt_settlements(patch)
        ack = write(db, plans)
        owner, tail = _r26_members(ack)
        original = snapshots[owner]
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
        _r28_interrupt(patch, write_journal, 'retire_operations', lambda: write(db, plans))
    _r29_restore_files(original)
    _r29_restore_files(snapshots[tail])
    (descriptor, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert write_journal.submission_is_current(head) and write_journal.completed_intent(descriptor.stem)
    writer, start = CoreWriter(db), len(db.events)
    protected = _r26_file_bytes([descriptor, *original, *snapshots[tail], *_r27_guards(plans, owner)])
    with pytest.raises(RuntimeError, match='receipt binding differs'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    # Restore the current member with its own query mapping.
    for path in original.keys() - snapshots[owner].keys():
        path.unlink()
    _r29_restore_files(snapshots[owner])
    assert _r20_recover(db, writer, plans, replay) == ack and not descriptor.exists()
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('general', [False, True])
def test_r29_linked_worklist_releases_reservation_without_owners(tmp_path, monkeypatch, replay, general):
    # Round 30 finding 6
    # Round 29 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    if general:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    else:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    backup, table = tmp_path / 'unreserved', plans[1].table
    _r26_snapshot(monkeypatch, backup, write_journal, 'reserve_submission', lambda: write(db, plans))
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    _r28_interrupt(monkeypatch, native_writer, '_write_plan', lambda: write(db, plans))
    _r26_restore(backup)
    (descriptor, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert descriptor.stem in head['after_publication'] and 'statement' not in head
    assert not write_journal.reservation_release_started(head)
    assert not any(write_journal.scope_owner(native_writer._target_scope(p)) for p in plans)
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='physical readback'):
        _r20_recover(db, writer, plans, replay)
    assert descriptor.exists() and not write_journal.reservation_release_started(head)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not descriptor.exists() and write_journal.reservation_released(head)
    assert not list(write_journal.bundle_reservations())
    _r28_no_dml(db, writer, start)
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('players', [('7',), ('7', '9')])
def test_r29_original_r27_head_requires_exact_target_witness(monkeypatch, replay, players):
    # Round 30 finding 7
    # Round 29 finding 7
    db, plans = CoreDB(), _representation_packet(players, CLOCK)
    prior = _representation_packet(players, EARLIER)
    write(db, prior, run_id=prior[0].rows[0]['run_id'], clock=EARLIER)
    save = write_journal._save_submission

    def prior_format(head):
        save({key: value for key, value in head.items() if key != 'target_attempts'})

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_save_submission', prior_format)
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, _LostResponse(db), plans, False)
    with monkeypatch.context() as patch:
        if len(players) == 1:
            patch.setattr(write_journal, '_save_submission', prior_format)
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        _r28_interrupt(patch, write_journal, 'retire_operations', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    ack = write_journal.read_receipt(native_writer._bundle_identity(plans, 'run-1')[1])
    member, *siblings = _r26_members(ack)
    scope = native_writer._target_scope(plans[0])
    target = write_journal.latest_operation(scope, scope, member)
    head = write_journal.submission_head(member)
    assert set(head.get('target_attempts', {})) == set(siblings)
    assert head['members'][member] != target['statement']['attempt'] and 'receipt_repair' not in head
    witness = write_journal._record_path(f'terminal-targets/{descriptor.stem}/{member}', target['statement']['attempt'])
    original = witness.read_bytes()
    writer, start = CoreWriter(db), len(db.events)
    witness.unlink()
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    conflicting = {**write_journal._decode_record(original),
                   'statement_sha256': native_writer._digest(head['statement'])}
    write_journal._store_record(witness, conflicting)
    with pytest.raises(RuntimeError, match='terminal target binding differs'):
        _r20_recover(db, writer, plans, replay)
    witness.write_bytes(original)
    db.sql.execute("UPDATE test_query_history SET state='RUNNING' WHERE query_id=?", (target['query_id'],))
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    assert descriptor.exists() and write_journal.scope_owner(scope) == member
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (target['query_id'],))
    if siblings:
        attempts = head['target_attempts']
        write_journal._replace_submission({**head, 'target_attempts': {**attempts, member: attempts[siblings[0]]}})
        protected = _r26_file_bytes([descriptor, *_r27_guards(plans, member)])
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
        write_journal._replace_submission(head)
    assert _r20_recover(db, writer, plans, replay) == ack and not descriptor.exists()
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
def test_r29_original_receipt_prefix_defers_missing_owner(tmp_path, monkeypatch, replay):
    # Round 29 finding 10
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup = tmp_path / 'one-owner'
    hold, unlink = write_journal.hold_scope, write_journal._unlink_record

    def first_owner(scope, owner):
        hold(scope, owner)
        if not backup.exists():
            shutil.copytree(write_journal._root() / 'scope-owners', backup)

    def stop_at_receipt_marker(path):
        if path.parent.name == 'statement-queries' and path.exists():
            _r22_stop()
        unlink(path)

    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        patch.setattr(write_journal, 'hold_scope', first_owner)
        patch.setattr(write_journal, '_unlink_record', stop_at_receipt_marker)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    owners = write_journal._root() / 'scope-owners'
    owners.rename(tmp_path / 'both')
    backup.rename(owners)
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner = native_writer._receipt_id(hashes[0], 'run-1', digest)
    head = write_journal.submission_head(owner)
    (descriptor, _), = write_journal.pending_intents({})
    files = _r27_dispatch_files(head['statement'])
    assert all(file.exists() for file in files)
    assert [write_journal.scope_owner(native_writer._target_scope(p)) for p in plans] == [owner, None]
    protected = _r26_file_bytes([descriptor, *files, *_r27_guards(plans, owner)])
    table = plans[1].table
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='9'")
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='physical readback'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'INSERT INTO {table} SELECT * FROM nine')
    assert _r20_recover(db, writer, plans, replay)['verified'] and not descriptor.exists()
    assert not writer.writes and [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql)] == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('replay', [False, True])
def test_r30_original_authenticates_target_before_first_receipt(monkeypatch, replay):
    # Round 30 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    scope = native_writer._target_scope(plans[0])
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    owner = native_writer._receipt_id(hashes[0], 'run-1', digest)
    record = write_journal._operation_path(scope, scope, owner)
    write_plan = native_writer._write_plan

    def stop_before_nine(connection, writer, plan, *args):
        if plan.scope_values == ('9',):
            _r22_stop()
        return write_plan(connection, writer, plan, *args)

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', stop_before_nine)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    first = record.read_bytes()
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    current = record.read_bytes()
    head = write_journal.submission_head(owner)
    (descriptor, _), = write_journal.pending_intents({})
    assert write_journal._read_record(record)['statement']['attempt'] == head['target_attempts'][owner]
    record.write_bytes(first)
    earlier = write_journal._read_record(record)
    assert earlier['state'] == 'returned' and earlier['operation']['intent'] == descriptor.stem
    assert earlier['statement']['attempt'] != head['target_attempts'][owner]
    protected = _r26_file_bytes([descriptor, record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected and write_journal.read_receipt(digest) is None
    assert _receipt_count(db) == 0
    _r28_no_dml(db, writer, start)
    record.write_bytes(current)
    assert _r20_recover(db, writer, plans, replay)['verified'] and _receipt_count(db) == 2
    assert not writer.writes and not descriptor.exists()


@pytest.mark.parametrize('replay', [False, True])
def test_r30_receipt_reservation_refuses_restored_target(monkeypatch, replay):
    # Round 30 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    seven = native_writer._target_scope(plans[0])
    saved = []
    retire = write_journal.retire_operations

    def capture(scopes, owner):
        record = write_journal._operation_path(seven, seven, owner)
        saved.append(record.read_bytes())
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', capture)
        ack = write(db, plans)
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
        assert write(db, plans) == ack
    owner, tail = _r26_members(ack)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert head['reservation'] == descriptor.stem and 'statement' not in head
    assert head['receipt_repair']['before'][owner] == write_journal._decode_record(saved[1])['statement']['attempt']
    record = write_journal._operation_path(seven, seven, owner)
    record.write_bytes(saved[0])
    assert write_journal._read_record(record)['statement']['attempt'] != head['receipt_repair']['before'][owner]
    protected = _r26_file_bytes([record, descriptor, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    record.unlink()
    assert _r20_recover(db, writer, plans, replay) == ack and _receipt_count(db) == 2
    assert not writer.writes and not descriptor.exists()


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('rewrite', [True, False])
def test_r30_unchanged_attendance_conflicts_with_same_intent_rewrite(monkeypatch, replay, rewrite):
    # Round 30 finding 3
    db, attendance = CoreDB(), _snapshot(CLOCK, 'r30-attendance')
    write(db, [attendance])
    plans = [attendance, membership_plan(['1'])]
    _r28_interrupt(monkeypatch, native_writer, '_record_pages', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    scope = native_writer._target_scope(attendance)
    owner = write_journal.scope_owner(scope)
    record = write_journal._operation_path(scope, scope, owner)
    noop = record.read_bytes()
    assert write_journal._read_record(record)['state'] == 'unchanged'
    if rewrite:
        db.sql.execute(f'DELETE FROM {attendance.table}')
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    writer, start = CoreWriter(db), len(db.events)
    if not rewrite:
        assert record.read_bytes() == noop and owner not in write_journal.submission_head(owner)['target_attempts']
        assert _r20_recover(db, writer, plans, replay)['verified'] and not descriptor.exists()
        _r28_no_dml(db, writer, start)
        return
    rewritten = record.read_bytes()
    assert write_journal._read_record(record)['statement']['attempt'] == write_journal.submission_head(
        owner)['target_attempts'][owner]
    record.write_bytes(noop)
    assert write_journal._read_record(record)['operation']['intent'] == descriptor.stem
    protected = _r26_file_bytes([record, descriptor, *_r27_guards(plans, owner)])
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    record.write_bytes(rewritten)
    assert _r20_recover(db, writer, plans, replay)['verified'] and not descriptor.exists()
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
def test_r30_expired_rejection_preserves_both_receipt_settlements(monkeypatch, replay):
    # Round 30 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    db.sql.execute(f'DELETE FROM {RECEIPTS}')
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        settlements = _r29_snapshot_receipt_settlements(patch)
        _restore_rejected_tail_mapping(patch, db, plans, tail, cleanup=True)
    _r29_restore_files(settlements[owner])
    last = write_journal.submission_head(owner)['statement']
    query = write_journal.statement_query(last)
    assert query == 'rejected-tail'
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    (descriptor, _), = write_journal.pending_intents({})
    files = [*settlements[owner], *settlements[tail], descriptor, *_r27_guards(plans, owner)]
    protected = _r26_file_bytes(files)
    assert all(protected.values())
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='rejected receipt outcome is still pending'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(protected) == protected
    _r28_no_dml(db, writer, start)
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FAILED', 'QUERY_QUEUE_FULL')", (query,))
    assert _r20_recover(db, writer, plans, replay) == ack and not descriptor.exists()
    assert not writer.writes and _receipt_count(db) == 2


def test_r30_page_only_repair_starts_its_own_target_history(monkeypatch):
    # Round 30 finding 3
    db, plan = CoreDB(), _snapshot(CLOCK, 'single-scope-repair')
    ack = write(db, [plan])
    member, = _r26_members(ack)
    original = write_journal.submission_head(member)
    assert member in original['target_attempts']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, [plan]))
    head = write_journal.submission_head(member)
    assert head['statement']['operation']['intent'] != original['statement']['operation']['intent']
    assert head['page_attempts'][member] == head['statement']['attempt']
    assert member not in head.get('target_attempts', {})
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, [plan], True) == ack
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('rewind', [None, 'unpublished', 'acknowledged'])
def test_r31_mapped_target_rejection_requires_live_query(tmp_path, monkeypatch, replay, rewind):
    # Round 31 finding 1
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)

    if rewind == 'acknowledged':
        write(db, plans)
        db.sql.execute(f'DELETE FROM {plans[0].table}')

    class Rejected(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _rejected_target(db, TrinoAmbiguousWriteError('lost rejection'))

    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, Rejected(db), plans, False)
    scope = native_writer._target_scope(plans[0])
    statement = write_journal.read_statement(scope)
    owner = statement['operation']['owner']
    query = write_journal.statement_query(statement)
    dispatch = _r27_dispatch_files(statement)
    unlink = write_journal._unlink_record

    def interrupt_mapping(path):
        if path == dispatch[-1]:
            _r22_stop()
        unlink(path)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', interrupt_mapping)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    (path, body), = write_journal.pending_intents({})
    record = write_journal._operation_path(scope, scope, owner)
    assert write_journal._read_record(record)['state'] == 'rejected'
    assert all(p.exists() for p in dispatch)
    saved = _r26_file_bytes([path, record, *dispatch, *_r27_guards(plans, owner)])
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    writer, start = CoreWriter(db), len(db.events)
    if rewind:
        watermark = write_journal._packet_watermark(owner)
        watermark.rename(tmp_path / 'watermark')
        with pytest.raises(StaleTransfermarktWrite, match='still pending'):
            _r20_recover(db, writer, plans, replay)
        (tmp_path / 'watermark').rename(watermark)
        db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FAILED', 'QUERY_QUEUE_FULL')", (query,))
        with pytest.raises(StaleTransfermarktWrite, match='operator reconciliation'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(saved) == saved
        _r28_no_dml(db, writer, start)
        return
    with pytest.raises(RuntimeError, match='rejected target outcome'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 0
    assert write_journal.read_receipt(body['identity']['digest']) is None
    assert not db.sql.execute(f'SELECT * FROM {plans[0].table}').fetchall()
    _r28_no_dml(db, writer, start)
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FAILED', 'QUERY_QUEUE_FULL')", (query,))
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert len(writer.writes) == 2 and _receipt_count(db) == 2
    assert not path.exists() and not any(p.exists() for p in dispatch)


def _r32_repaired_packet_missing_receipt(monkeypatch, db=None):
    db = CoreDB() if db is None else db
    plans = _representation_packet(('7', '9'), CLOCK)
    seven = native_writer._target_scope(plans[0])
    retire = write_journal.retire_operations
    attempts = []

    def capture(scopes, owner):
        record = write_journal._operation_path(seven, seven, owner)
        if record.exists():
            attempts.append(record.read_bytes())
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', capture)
        ack = write(db, plans)
        for player in ('7', '9'):
            db.sql.execute(f'DELETE FROM {plans[0].table} WHERE player_id=?', (player,))
            assert write(db, plans) == ack
    owner, tail = _r26_members(ack)
    record = write_journal._operation_path(seven, seven, owner)
    db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    return db, plans, ack, owner, record, attempts


@pytest.mark.parametrize('replay', [False, True])
def test_r31_unreserved_worklist_authenticates_predecessor(monkeypatch, replay):
    # Round 31 finding 2
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    (path, body), = write_journal.pending_intents({})
    before = body['payload']['receipt_worklist']['before'][owner]
    assert write_journal._decode_record(attempts[1])['statement']['attempt'] == before
    assert write_journal._decode_record(attempts[0])['statement']['attempt'] != before
    record.write_bytes(attempts[0])
    saved = _r26_file_bytes([path, record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='predecessor operation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    record.write_bytes(attempts[1])
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: _r20_recover(db, writer, plans, replay))
    assert not record.exists() and write_journal.submission_head(owner)['reservation'] == path.stem
    _r28_no_dml(db, writer, start)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and not writer.writes and _receipt_count(db) == 2


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('r28_head', [False, True])
def test_r31_original_page_attempt_checked_before_first_receipt(monkeypatch, replay, r28_head):
    # Round 32 finding 2
    if r28_head:
        save = write_journal._save_submission
        monkeypatch.setattr(write_journal, '_save_submission',
                            lambda head: save({k: v for k, v in head.items() if k not in ('page_attempts', 'before_target_tracking')}))
    # Round 31 finding 3
    db = CoreDB()
    plans = [_snapshot(CLOCK, 'before-receipt'), *_representation_packet(('7',), CLOCK)]

    def stop_before_receipts():
        _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))

    stop_before_receipts()
    scope = native_writer._target_scope(plans[0])
    owner = write_journal.scope_owner(scope)
    record = write_journal._operation_path(scope, (native_writer.CORE_PAGES_TABLE, owner), owner)
    first = record.read_bytes()
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    stop_before_receipts()
    current = record.read_bytes()
    db.sql.execute(f'DELETE FROM {plans[1].table}')
    stop_before_receipts()
    head = write_journal.submission_head(owner)
    assert head['statement']['operation']['member'] != owner
    assert ('page_attempts' in head) != r28_head
    assert head['members'][owner] == write_journal._decode_record(current)['statement']['attempt']
    record.write_bytes(first)
    (path, _), = write_journal.pending_intents({})
    saved = _r26_file_bytes([path, record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 0
    record.write_bytes(current)
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert not path.exists() and not writer.writes and _receipt_count(db) == 2


def _r31_r27_head(patch):
    save = write_journal._save_submission
    new_fields = {'page_attempts', 'target_attempts', 'before_target_tracking'}
    patch.setattr(write_journal, '_save_submission',
                  lambda head: save({key: value for key, value in head.items() if key not in new_fields}))


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('rewrite', [False, True])
def test_r31_mixed_generation_requires_positive_unchanged_history(monkeypatch, replay, rewrite):
    # Round 31 finding 4
    db, attendance = CoreDB(), _snapshot(CLOCK, 'r27-noop')
    plans = [attendance, *_representation_packet(('7',), CLOCK)]
    with monkeypatch.context() as patch:
        _r31_r27_head(patch)
        write(db, [attendance])
        _r28_interrupt(patch, native_writer, '_record_pages', lambda: write(db, plans))
        scope = native_writer._target_scope(attendance)
        owner = write_journal.scope_owner(scope)
        record = write_journal._operation_path(scope, scope, owner)
        noop = record.read_bytes()
        assert write_journal._read_record(record)['state'] == 'unchanged'
        if rewrite:
            db.sql.execute(f'DELETE FROM {attendance.table}')
            _r28_interrupt(patch, native_writer, '_record_pages', lambda: write(db, plans))
            assert write_journal._read_record(record)['state'] == 'returned'
            rewritten = record.read_bytes()
    db.sql.execute(f'DELETE FROM {plans[1].table}')
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    head = write_journal.submission_head(owner)
    assert head['target_attempts'] and owner not in head['target_attempts']
    (path, _), = write_journal.pending_intents({})
    writer, start = CoreWriter(db), len(db.events)
    if not rewrite:
        assert record.read_bytes() == noop
        assert _r20_recover(db, writer, plans, replay)['verified'] and not path.exists()
        _r28_no_dml(db, writer, start)
        return
    record.write_bytes(noop)
    saved = _r26_file_bytes([path, record, *_r27_guards(plans, owner)])
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(saved) == saved
    record.write_bytes(rewritten)
    assert _r20_recover(db, writer, plans, replay)['verified'] and not path.exists()
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('later_bundle_guard', [False, True])
def test_r31_released_reservation_finishes_guard_unlinks(tmp_path, monkeypatch, replay, later_bundle_guard):
    # Round 31 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    backup = tmp_path / 'general'
    _r26_snapshot(monkeypatch, backup, write_journal, 'retire_operations', lambda: write(db, plans))
    db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    _r28_interrupt(monkeypatch, native_writer, '_release_packet', lambda: write(db, plans))
    head = write_journal.submission_head(owner)
    assert write_journal.reservation_release_started(head) and 'statement' not in head
    displaced = _r26_restore(backup)
    # Round 32 finding 5
    (path, _), = write_journal.pending_intents({})
    nine_guard = write_journal._scope_owner_path(native_writer._target_scope(plans[1]))
    shutil.copyfile(displaced / nine_guard.relative_to(write_journal._root()), nine_guard)
    assert write_journal._read_record(nine_guard)['intent'] == head['reservation']
    saved = _r26_file_bytes([path, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    # Both released owners authenticate the generation even if the linked
    # descriptor still holds its own bundle reservation.
    shutil.copytree(displaced / 'scope-owners', write_journal._root() / 'scope-owners', dirs_exist_ok=True)
    if later_bundle_guard:
        shutil.copytree(displaced / 'reserving-bundles', write_journal._root() / 'reserving-bundles', dirs_exist_ok=True)
    (path, _), = write_journal.pending_intents({})
    assert path.stem in head['after_publication']
    writer, start = CoreWriter(db), len(db.events)
    unlink = write_journal._unlink_record

    def interrupt_first_owner(file):
        unlink(file)
        if file.parent.name == 'scope-owners':
            _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', interrupt_first_owner)
        with pytest.raises(OSError, match='interrupted reservation'):
            _r20_recover(db, writer, plans, replay)
    assert path.exists() and write_journal.completed_intent(path.stem)['terminal_guard_generation'] == head['reservation']
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and write_journal.reservation_released(head)
    _r28_no_dml(db, writer, start)
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('stop', ['receipt-merge', 'receipt-reservation', 'target'])
def test_r31_lone_general_descriptor_checks_guard_generation(tmp_path, monkeypatch, replay, stop):
    # Round 32 finding 1
    # Round 31 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    backup = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, backup, write_journal, 'retire_operations', lambda: write(db, plans))
    owner, _ = _r26_members(ack)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    if stop == 'receipt-reservation':
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    else:
        method = '_write_plan' if stop == 'target' else '_merge_receipts'
        _r28_interrupt(monkeypatch, native_writer, method, lambda: write(db, plans))
    (path, _), = write_journal.pending_intents({})
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[0]))
    current = guard.read_bytes()
    shutil.copyfile(backup / guard.relative_to(write_journal._root()), guard)
    assert write_journal._read_record(guard)['intent'] != path.stem
    saved = _r26_file_bytes([path, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    guard.write_bytes(current)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not path.exists() and len(writer.writes) == int(stop == 'target') and _receipt_count(db) == 2


@pytest.mark.parametrize('replay', [False, True])
def test_r31_general_worklist_defers_owner_until_sibling_readback(tmp_path, monkeypatch, replay):
    # Round 31 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    hold = write_journal.hold_scope
    owners = write_journal._root() / 'scope-owners'

    def snapshot_first_owner(scope, owner):
        hold(scope, owner)
        intent = write_journal._read_record(write_journal._scope_owner_path(scope))['intent']
        snapshot = tmp_path / intent
        if not snapshot.exists():
            shutil.copytree(owners, snapshot)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'hold_scope', snapshot_first_owner)
        ack = _r28_overlapping_worklists(tmp_path, patch, db, plans, 'original', general=True)
    owner, tail = _r26_members(ack)
    owners.rename(tmp_path / 'both')
    (tmp_path / write_journal.submission_head(owner)['receipt_repair']['intent']).rename(owners)
    assert len(list(owners.iterdir())) == 1
    db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    saved = _r26_file_bytes([*(p for p, _ in write_journal.pending_intents({})), *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='receipt readback'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 0
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not write_journal.pending_intents({}) and not writer.writes and _receipt_count(db) == 2


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('advance_sibling', [False, True])
def test_r31_r27_target_witness_before_first_receipt(monkeypatch, replay, advance_sibling):
    # Round 32 finding 8
    # Round 31 finding 8
    db, plans = CoreDB(), [_snapshot(CLOCK, 'attendance')]
    if advance_sibling:
        plans.extend(_representation_packet(('7',), CLOCK))
    with monkeypatch.context() as patch:
        _r31_r27_head(patch)
        write(db, [_snapshot(EARLIER, 'earlier')], run_id='earlier', clock=EARLIER)
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, _LostResponse(db), plans, False)
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
        if advance_sibling:
            db.sql.execute(f'DELETE FROM {plans[1].table}')
            _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    scope = native_writer._target_scope(plans[0])
    owner = write_journal.scope_owner(scope)
    head = write_journal.submission_head(owner)
    page = write_journal.latest_operation(scope, (native_writer.CORE_PAGES_TABLE, owner), owner)['statement']
    assert head['members'][owner] == page['attempt']
    assert (head['statement'] == page) != advance_sibling
    (path, _), = write_journal.pending_intents({})
    target = write_journal.latest_operation(scope, scope, owner)
    witness = write_journal.terminal_witness(path.stem, owner, target['statement'])
    assert witness['query_id'] == target['query_id'] and witness['outcome'] == 'FINISHED'
    write_journal._replace_submission({**head, 'target_attempts': {owner: head['statement']['attempt']}})
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert path.exists()
    write_journal._replace_submission(head)
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert not writer.writes and not path.exists() and _receipt_count(db) == 1 + len(plans)


def _r32_stop_before_receipt_reservation(monkeypatch, run):
    reserve = write_journal.reserve_submission

    def interrupt(*args, **kwargs):
        if 'missing_receipts' in kwargs:
            _r22_stop()
        reserve(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'reserve_submission', interrupt)
        with pytest.raises(OSError, match='interrupted reservation'):
            run()


def test_r32_fresh_receipt_worklist_checks_frozen_attempt_before_reserving(monkeypatch):
    # Round 32 finding 3
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    before = write_journal.submission_head(owner)['members'][owner]
    assert before == write_journal._decode_record(attempts[1])['statement']['attempt']
    assert before != write_journal._decode_record(attempts[0])['statement']['attempt']
    record.write_bytes(attempts[0])
    protected = _r26_file_bytes([record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='predecessor operation'):
        _r20_recover(db, writer, plans, False)
    assert _r26_file_bytes(protected) == protected and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, False) == ack
    assert not record.exists() and not writer.writes and _receipt_count(db) == 2


@pytest.mark.parametrize('replay', [False, True])
def test_r32_mapped_page_rejection_requires_live_coordinator_row(monkeypatch, replay):
    # Round 32 finding 4
    db, plans = CoreDB(), [_snapshot(CLOCK, 'rejected-page')]
    query = 'rejected-page'
    with monkeypatch.context() as patch:
        posts = _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, query)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    assert len(posts) == 1
    owner = write_journal.scope_owner(native_writer._target_scope(plans[0]))
    statement = write_journal.submission_head(owner)['statement']
    assert statement['key'] == [native_writer.CORE_PAGES_TABLE, owner]
    files = _r27_dispatch_files(statement)
    db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code='QUERY_QUEUE_FULL' WHERE query_id=?", (query,))
    unlink = write_journal._unlink_record

    def stop_mapping(path):
        if path == files[-1]:
            _r22_stop()
        unlink(path)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', stop_mapping)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    (descriptor, _), = write_journal.pending_intents({})
    record = write_journal._operation_path(statement['operation']['scope'], statement['key'], owner)
    assert write_journal._read_record(record)['state'] == 'rejected' and all(p.exists() for p in files)
    saved = _r26_file_bytes([record, descriptor, *files, *_r27_guards(plans, owner)])
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='rejected page outcome'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 0
    _r28_no_dml(db, writer, start)
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FAILED', 'QUERY_QUEUE_FULL')", (query,))
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert not writer.writes and _receipt_count(db) == 1 and not descriptor.exists()
    assert not any(p.exists() for p in files)
    assert db.sql.execute('SELECT count(*) FROM transfermarkt_core_pages_v1').fetchone() == (len(plans[0].pages),)


@pytest.mark.parametrize('replay', [False, True])
def test_r32_general_repair_leaves_one_owner_on_failed_sibling_read(tmp_path, monkeypatch, replay):
    # Round 32 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, sibling = _r26_members(ack)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    owners = write_journal._root() / 'scope-owners'
    snapshot = tmp_path / 'one-owner'
    hold = write_journal.hold_scope

    def first_owner(scope, packet):
        hold(scope, packet)
        if not snapshot.exists():
            shutil.copytree(owners, snapshot)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'hold_scope', first_owner)
        _r32_stop_before_receipt_reservation(patch, lambda: write(db, plans))
    owners.rename(tmp_path / 'both')
    snapshot.rename(owners)
    assert len(list(owners.iterdir())) == 1
    (descriptor, _), = write_journal.pending_intents({})
    saved = _r26_file_bytes([descriptor, *_r27_guards(plans, owner)])
    execute = CoreCursor.execute

    def disconnect(cursor, sql, params=()):
        if sql.startswith('SELECT receipt_id,') and sibling in params:
            raise ConnectionError('sibling receipt read disconnected')
        return execute(cursor, sql, params)

    writer, start = CoreWriter(db), len(db.events)
    with monkeypatch.context() as patch:
        patch.setattr(CoreCursor, 'execute', disconnect)
        with pytest.raises(ConnectionError, match='sibling receipt'):
            _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not writer.writes and _receipt_count(db) == 2 and not list(owners.iterdir()) and not descriptor.exists()


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore_receipt', [False, True])
def test_r32_unreserved_worklist_retires_exact_predecessor(monkeypatch, replay, restore_receipt):
    # Round 32 finding 7
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    (descriptor, body), = write_journal.pending_intents({})
    record.write_bytes(attempts[1])
    assert body['payload']['receipt_worklist']['before'][owner] == write_journal._read_record(record)['statement']['attempt']
    if restore_receipt:
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack
    mutations = [q.split(' (')[0] for q in db.events[start:] if native_writer.is_mutation(q)]
    assert mutations == ([] if restore_receipt else ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE])
    assert not descriptor.exists() and not record.exists() and not writer.writes and _receipt_count(db) == 2
    assert write(db, _representation_packet(('7',), LATER), run_id='next-seven', clock=LATER)['verified']


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('legacy', [False, True])
def test_r32_original_noop_with_accounted_sequence(monkeypatch, replay, legacy):
    # Round 32 finding 9
    db, attendance = CoreDB(), _snapshot(CLOCK, 'unchanged')
    write(db, [attendance])
    plans = [attendance, *_representation_packet(('7',), CLOCK)]
    with monkeypatch.context() as patch:
        if legacy:
            save = write_journal._save_submission
            patch.setattr(write_journal, '_save_submission',
                          lambda head: save({k: v for k, v in head.items() if k != 'before_target_tracking'}))
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    scope, sibling_scope = map(native_writer._target_scope, plans)
    owner = write_journal.scope_owner(scope)
    head = write_journal.submission_head(owner)
    assert head['statement']['sequence'] == 2 and head['statement']['key'] == [native_writer.CORE_PAGES_TABLE, owner]
    assert write_journal.latest_operation(scope, scope, owner)['state'] == 'unchanged'
    sibling, = head['target_attempts']
    target = write_journal._operation_path(sibling_scope, sibling_scope, sibling)
    assert write_journal._read_record(target)['statement']['sequence'] == 1
    writer, start = CoreWriter(db), len(db.events)
    if legacy:
        assert 'before_target_tracking' not in head
        previous = target.read_bytes()
        target.unlink()
        (descriptor, _), = write_journal.pending_intents({})
        protected = _r26_file_bytes([descriptor, *_r27_guards(plans, owner)])
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(protected) == protected
        _r28_no_dml(db, writer, start)
        target.write_bytes(previous)
        conflicting = {**head['members'], sibling: head['statement']['attempt']}
        write_journal._replace_submission({**head, 'members': conflicting})
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        _r28_no_dml(db, writer, start)
        write_journal._replace_submission(head)
    assert _r20_recover(db, writer, plans, replay)['verified'] and _receipt_count(db) == 3
    assert not writer.writes and not write_journal.pending_intents({})


def _r33_data_mutations(db, start, plans):
    ddl = {native_writer.core_receipts_ddl(), *(plan.ddl for plan in plans)}
    if any(plan.pages for plan in plans):
        ddl.add(native_writer.core_pages_ddl())
    return [sql.split(' (')[0] for sql in db.events[start:] if native_writer.is_mutation(sql) and sql not in ddl]


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore_target', [False, True])
@pytest.mark.parametrize('boundary', ['reserve_submission', 'reserve_scopes'])
def test_r33_unreserved_general_predecessor(monkeypatch, replay, restore_target, boundary):
    # Round 34 finding 1
    # Round 33 finding 1
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    table = plans[0].table
    db.sql.execute(f"CREATE TABLE seven AS SELECT * FROM {table} WHERE player_id='7'")
    db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
    _r28_interrupt(monkeypatch, write_journal, boundary, lambda: write(db, plans))
    (path, body), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    if boundary == 'reserve_submission':
        assert body['payload']['predecessor_head'] == native_writer._digest(head)
    else:
        assert head['reservation'] == path.stem and 'statement' not in head
        assert head['members'][owner] == write_journal._decode_record(attempts[1])['statement']['attempt']
    assert not body['payload'].get('receipt_only')
    if restore_target:
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM seven')
    record.write_bytes(attempts[0])
    saved = _r26_file_bytes([path, record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='predecessor operation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1
    _r28_no_dml(db, writer, start)
    with pytest.raises(StaleTransfermarktWrite, match='still pending'):
        write(db, _representation_packet(('9',), LATER), run_id='competing-nine', clock=LATER)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, replay) == ack
    assert len(writer.writes) == int(not restore_target)
    assert _r33_data_mutations(db, start, plans) == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]
    assert not record.exists() and not path.exists() and _receipt_count(db) == 2


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore_receipt', [False, True])
def test_r33_independent_worklist_guard(tmp_path, monkeypatch, replay, restore_receipt):
    # Round 33 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    publication = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, publication, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, tail = _r26_members(ack)
    db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    (path, body), = write_journal.pending_intents({})
    assert body['payload']['reserve_scopes']
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[0]))
    shutil.copyfile(publication / guard.relative_to(write_journal._root()), guard)
    original = write_journal._read_record(guard)
    assert original['intent'] != path.stem and original['owner'] == owner
    if restore_receipt:
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    saved = _r26_file_bytes([path, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1 + int(restore_receipt)
    write_journal._store_record(guard, {**original, 'intent': path.stem}, replace=True)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not writer.writes and _receipt_count(db) == 2 and not path.exists() and not guard.exists()
    assert _r33_data_mutations(db, start, plans) == (
        [] if restore_receipt else ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE])


@pytest.mark.parametrize('replay', [False, True])
def test_r33_sibling_physical_proof_before_target(monkeypatch, replay):
    # Round 33 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, sibling = _r26_members(ack)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    hold = write_journal.hold_scope

    def stop_after_first(scope, packet):
        hold(scope, packet)
        _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'hold_scope', stop_after_first)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    owners = write_journal._root() / 'scope-owners'
    assert len(list(owners.iterdir())) == 1
    saved = _r26_file_bytes([path, *_r27_guards(plans, owner)])
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='conflicting' WHERE receipt_id=?", (sibling,))
    receipt = native_writer._stored_receipts(db, [sibling])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='contradicts'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved and native_writer._stored_receipts(db, [owner, sibling]) == receipt
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', (ack['plans'][1]['physical_hash'], sibling))
    assert _r20_recover(db, writer, plans, replay) == ack and not path.exists()
    assert len(writer.writes) == 1 and not list(owners.iterdir()) and _receipt_count(db) == 2
    assert _r33_data_mutations(db, start, plans) == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('hide_page', [False, True])
def test_r33_page_binding_across_receipt_upgrade(monkeypatch, replay, legacy, hide_page):
    # Round 34 finding 7
    # Round 33 finding 5
    db = CoreDB()
    plans = [_snapshot(CLOCK, 'r28-page'), *_representation_packet(('7',), CLOCK)]
    with monkeypatch.context() as patch:
        if legacy:
            save = write_journal._save_submission
            patch.setattr(write_journal, '_save_submission',
                          lambda head: save({k: v for k, v in head.items() if k not in ('page_attempts', 'before_target_tracking')}))
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
        scope = native_writer._target_scope(plans[0])
        owner = write_journal.scope_owner(scope)
        page = write_journal._operation_path(scope, (native_writer.CORE_PAGES_TABLE, owner), owner)
        old_page = page.read_bytes()
        db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
        if hide_page:
            db.sql.execute(f'DELETE FROM {plans[1].table}')
            _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    genuine = page.read_bytes()
    head = write_journal.submission_head(owner)
    assert head['members'][owner] == write_journal._read_record(page)['statement']['attempt']
    assert ('page_attempts' not in head) == legacy
    if hide_page:
        assert head['statement']['operation']['member'] != owner
        page.unlink()
    writer = CoreWriter(db)
    _r28_interrupt(monkeypatch, write_journal, 'complete_intent', lambda: _r20_recover(db, writer, plans, replay))
    (path, _), = write_journal.pending_intents({})
    assert _receipt_count(db) == 2 and write_journal.submission_head(owner)['members'][owner] != head['members'][owner]
    start = len(db.events)
    page.write_bytes(old_page)
    saved = _r26_file_bytes([path, page, *_r27_guards(plans, owner)])
    with pytest.raises(RuntimeError, match='operation binding differs'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    page.write_bytes(genuine)
    assert _r20_recover(db, writer, plans, replay)['verified'] and not path.exists()
    assert not writer.writes and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize(('unlink_sequence', 'restore_noop'), [*((i, False) for i in range(1, 9)), (1, True)])
def test_r33_noop_retirement_after_each_supporting_unlink(monkeypatch, replay, unlink_sequence, restore_noop):
    # Round 34 finding 8
    # Round 33 finding 6
    db, attendance = CoreDB(), _snapshot(CLOCK, 'accounted-noop')
    write(db, [attendance])
    plans = [attendance, *_representation_packet(('7', '8', '9'), CLOCK)]
    with monkeypatch.context() as patch:
        save = write_journal._save_submission
        patch.setattr(write_journal, '_save_submission',
                      lambda head: save({k: v for k, v in head.items() if k != 'before_target_tracking'}))
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    scope = native_writer._target_scope(attendance)
    owner = write_journal.scope_owner(scope)
    assert write_journal.latest_operation(scope, scope, owner)['state'] == 'unchanged'
    assert write_journal.submission_head(owner)['statement']['sequence'] == 4
    noop = write_journal._operation_path(scope, scope, owner)
    original = noop.read_bytes()
    unlink = write_journal._unlink_record

    def crash_after_unlink(path):
        if path.parent.parent == write_journal._operation_directory(scope).parent and path.exists():
            record = write_journal._read_record(path)
            if record.get('statement', {}).get('sequence') == unlink_sequence:
                unlink(path)
                _r22_stop()
        unlink(path)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', crash_after_unlink)
        with pytest.raises(OSError, match='interrupted reservation'):
            write(db, plans)
    (path, _), = write_journal.pending_intents({})
    assert write_journal.completed_intent(path.stem) and _receipt_count(db) == 5
    writer, start = CoreWriter(db), len(db.events)
    if restore_noop:
        assert not noop.exists()
        noop.write_bytes(original)
        witness, = (write_journal._root() / 'verified-noops').glob('*.json')
        proof = witness.read_bytes()
        witness.unlink()
        with pytest.raises(RuntimeError, match='operation binding differs'):
            _r20_recover(db, writer, plans, replay)
        assert noop.read_bytes() == original and path.exists()
        _r28_no_dml(db, writer, start)
        witness.write_bytes(proof)
    assert _r20_recover(db, writer, plans, replay)['verified']
    assert not path.exists() and _receipt_count(db) == 5 and not list(write_journal.bundle_reservations())
    _r28_no_dml(db, writer, start)


def _r33_restore_step_predecessor(db, plans, monkeypatch, *, unreserved):
    scope = native_writer._target_scope(plans[0])
    saved = {}
    retire = write_journal.retire_operations

    def save_target(scopes, owner):
        target = write_journal._operation_path(scope, scope, owner)
        saved[target] = target.read_bytes()
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', save_target)
        ack = write(db, plans)
    owner, tail = _r26_members(ack)
    head = write_journal.submission_head(owner)
    record, = saved
    attempt = write_journal._decode_record(saved[record])['statement']['attempt']
    assert head['target_attempts'][owner] == attempt != head['members'][owner]
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    if unreserved:
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(saved[record])
    return ack, record


@pytest.mark.parametrize('entry', ['fresh', 'ordinary', 'replay'])
def test_r33_publication_step_head_predecessor(monkeypatch, entry):
    # Round 33 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack, record = _r33_restore_step_predecessor(db, plans, monkeypatch, unreserved=entry != 'fresh')
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, entry == 'replay') == ack
    assert not record.exists() and not write_journal.pending_intents({}) and not writer.writes
    assert _r33_data_mutations(db, start, plans) == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize(('linked', 'boundary'), [
    (False, 'mapped'), (True, 'mapped'), (True, 'dispatched'),
    *((True, cut) for cut in ('mapping', 'statement', 'attempt', 'marker')),
    *((True, subset) for subset in range(15)),
    *((True, ('dispatched', subset)) for subset in range(8, 16)),
])
def test_r33_page_rejection_packet_readback(tmp_path, monkeypatch, replay, linked, boundary):
    # Round 35 finding 9
    # Round 34 finding 9
    # Round 33 finding 8
    db, plans = CoreDB(), [_snapshot(CLOCK, 'rejected-repair')]
    publication = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, publication, write_journal, 'complete_intent', lambda: write(db, plans))
    owner, = _r26_members(ack)
    db.sql.execute('CREATE TABLE pages AS SELECT * FROM transfermarkt_core_pages_v1')
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    query = 'page-repair-rejected'
    with monkeypatch.context() as patch:
        _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, query)
        with pytest.raises(TrinoAmbiguousWriteError):
            write(db, plans)
    db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code='QUERY_QUEUE_FULL' WHERE query_id=?", (query,))
    statement = write_journal.submission_head(owner)['statement']
    files = _r27_dispatch_files(statement)
    unlink = write_journal._unlink_record

    def stop_mapping(path):
        if path == files[-1]:
            _r22_stop()
        unlink(path)

    record = write_journal._operation_path(statement['operation']['scope'], statement['key'], owner)
    dispatched = record.read_bytes()
    if boundary != 'dispatched':
        with monkeypatch.context() as patch:
            patch.setattr(write_journal, '_unlink_record', stop_mapping)
            with pytest.raises(OSError, match='interrupted reservation'):
                write(db, plans)
    if linked:
        _r27_restore_dispatch(publication, statement)
    db.sql.execute('INSERT INTO transfermarkt_core_pages_v1 SELECT * FROM pages')
    (path, _), = write_journal.pending_intents({})
    assert (path.stem in statement['operation']['after_publication']) == linked
    if boundary == 'dispatched':
        assert write_journal._read_record(record)['state'] == 'dispatched' and not files[2].exists()
    else:
        assert write_journal._read_record(record)['state'] == 'rejected' and all(f.exists() for f in files)
    if isinstance(boundary, tuple):
        _, boundary = boundary
        record.write_bytes(dispatched)
    if isinstance(boundary, int):
        for index, file in enumerate(files):
            if not boundary & (1 << index):
                file.unlink()
    db.sql.execute('DELETE FROM test_query_history WHERE query_id=?', (query,))
    saved = _r26_file_bytes([path, record, *files, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='outcome is still pending'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    db.sql.execute("INSERT INTO test_query_history VALUES (?, 'FAILED', 'QUERY_QUEUE_FULL')", (query,))
    if linked:
        db.sql.execute(f'CREATE TABLE receipts AS SELECT * FROM {RECEIPTS}')
        db.sql.execute(f'DELETE FROM {RECEIPTS}')
        with pytest.raises(RuntimeError, match='receipt readback'):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(saved) == saved
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM receipts')
    cuts = dict(zip(('statement', 'attempt', 'marker', 'mapping'), files, strict=True))
    if boundary in cuts:
        def interrupt_cleanup(file):
            unlink(file)
            if file == cuts[boundary]:
                _r22_stop()

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, '_unlink_record', interrupt_cleanup)
            with pytest.raises(OSError, match='interrupted reservation'):
                _r20_recover(db, writer, plans, replay)
        assert path.exists() and not cuts[boundary].exists()
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not any(f.exists() for f in [path, record, *files]) and _receipt_count(db) == 1
    assert not writer.writes and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('boundary', ['retired', 'reserved', 'released'])
def test_r34_unlinked_retired_head_checks_every_guard(tmp_path, monkeypatch, replay, boundary):
    # Round 35 finding 1
    # Round 34 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, _ = _r26_members(ack)
    root = write_journal._root()
    published = tmp_path / 'published'
    obsolete = tmp_path / 'first-repair'
    shutil.copytree(root, published)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    assert _r26_snapshot(monkeypatch, obsolete, native_writer, '_write_plan', lambda: write(db, plans)) == ack
    root.rename(tmp_path / 'first-finished')
    published.rename(root)
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {plans[0].table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='9'")
    stop = '_release_packet' if boundary == 'retired' else '_write_plan'
    _r28_interrupt(monkeypatch, native_writer, stop, lambda: write(db, plans))
    if boundary != 'retired':
        db.sql.execute(f'INSERT INTO {plans[0].table} SELECT * FROM nine')
        if boundary == 'released':
            _r28_interrupt(monkeypatch, native_writer, '_release_packet', lambda: write(db, plans))
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[1]))
    later_guard = guard.read_bytes()
    head = write_journal.submission_head(owner)
    if boundary == 'retired':
        assert write_journal.submission_retired(head) and write(db, plans) == ack
    else:
        assert 'statement' not in head and write_journal.reservation_release_started(head) == (boundary == 'released')
    _r26_restore(obsolete)
    (descriptor, _), = write_journal.pending_intents({})
    assert descriptor.stem not in head.get('statement', {}).get('operation', head)['after_publication']
    first_guard = guard.read_bytes()
    guard.write_bytes(later_guard)
    saved = _r26_file_bytes([descriptor, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    guard.write_bytes(first_guard)
    if boundary != 'retired':
        complete = write_journal.complete_intent

        def stop_after_completion(*args, **kwargs):
            complete(*args, **kwargs)
            _r22_stop()

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'complete_intent', stop_after_completion)
            with pytest.raises(OSError, match='interrupted reservation'):
                _r20_recover(db, writer, plans, replay)
        assert write_journal.completed_intent(descriptor.stem)['obsolete_release_head'] == native_writer._digest(head)
        assert descriptor.exists() and guard.read_bytes() == first_guard
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not descriptor.exists() and not guard.exists() and not writer.writes
    assert not _r33_data_mutations(db, start, plans)
    if boundary == 'reserved':
        # The independent later reservation still awaits its own descriptor.
        with pytest.raises(RuntimeError, match='still pending'):
            write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)
        return
    assert write(db, _representation_packet(('9',), LATER), run_id='next-nine', clock=LATER)['verified']


@pytest.mark.parametrize('drift', [False, True])
def test_r34_ordinary_retry_all_receipts_checks_predecessor(monkeypatch, drift):
    # Round 34 finding 3
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    if drift:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    record.write_bytes(attempts[0])
    saved = _r26_file_bytes([record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='predecessor operation'):
        _r20_recover(db, writer, plans, False)
    assert _r26_file_bytes(saved) == saved and not write_journal.pending_intents({})
    _r28_no_dml(db, writer, start)
    record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, False) == ack
    assert not record.exists() and len(writer.writes) == int(drift) and _receipt_count(db) == 2
    assert not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('remove_reservation', [False, True])
@pytest.mark.parametrize('original', [False, True])
def test_r34_competing_packet_leaves_both_generations_intact(tmp_path, monkeypatch, replay, remove_reservation, original):
    # Round 35 finding 4
    # Round 34 finding 4
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    overlapping = _representation_packet(('9',), LATER)
    if not original:
        ack = write(db, plans)
        owner, sibling = _r26_members(ack)
    if replay:
        _r28_interrupt(monkeypatch, write_journal, 'snapshot_anchors',
                       lambda: write(db, overlapping, run_id='next', clock=LATER))
    if original:
        backup = tmp_path / 'publication'
        ack = _r26_snapshot(monkeypatch, backup, write_journal, 'complete_intent', lambda: write(db, plans))
        owner, sibling = _r26_members(ack)
        _r26_restore(backup)
        write_journal._scope_owner_path(native_writer._target_scope(plans[1])).unlink()
    else:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
        hold = write_journal.hold_scope

        def stop(scope, packet):
            hold(scope, packet)
            _r22_stop()

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'hold_scope', stop)
            with pytest.raises(OSError, match='interrupted reservation'):
                write(db, plans)
    guards = _r27_guards(plans, owner)
    if remove_reservation:
        guards[0].unlink()
    assert sum(g.exists() for g in guards[-2:]) == 1
    conflict = owner if original else sibling
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (conflict,))
    saved = _r26_file_bytes([*guards, *(p for p, _ in write_journal.pending_intents({}))])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        if replay:
            native_writer.replay_pending_intents(lambda: db, writer, run_id='next')
        else:
            native_writer.write_core_bundle(lambda: db, writer, overlapping, run_id='next',
                                            universe_version='core-universe-v1', committed_at=LATER)
    assert _r26_file_bytes(saved) == saved and _receipt_count(db) == 1 + int(original)
    _r28_no_dml(db, writer, start)


def _r34_restore_receipt_predecessor(db, plans, monkeypatch, *, drift=False):
    ack = write(db, plans)
    owner, sibling = _r26_members(ack)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (sibling,))
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    record = write_journal._operation_path(native_writer._target_scope(plans[1]),
                                           (native_writer.CORE_RECEIPTS_TABLE, sibling), sibling)
    previous = record.read_bytes()
    assert write(db, plans) == ack and not record.exists()
    if drift:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (owner,))
    _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    record.write_bytes(previous)
    return ack, descriptor, record


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('drift', [False, True])
def test_r34_restored_retired_worklist_allows_successor(monkeypatch, replay, drift):
    # Round 34 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack, descriptor, record = _r34_restore_receipt_predecessor(db, plans, monkeypatch, drift=drift)
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not descriptor.exists() and _receipt_count(db) == 2
    assert len(writer.writes) == int(drift)
    assert _r33_data_mutations(db, start, plans) == ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('replay', [False, True])
def test_r34_finished_target_is_settled_before_later_drift(monkeypatch, replay):
    # Round 34 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, sibling = _r26_members(ack)
    scope = native_writer._target_scope(plans[0])
    record = write_journal._operation_path(scope, scope, owner)
    remove_target = f"DELETE FROM {plans[0].table} WHERE player_id='7'"
    db.sql.execute(remove_target)
    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, _LostResponse(db), plans, False)
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    previous = record.read_bytes()
    assert write_journal._read_record(record)['state'] == 'FINISHED'
    assert write(db, plans) == ack and not record.exists()
    db.sql.execute(remove_target)
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (sibling,))
    _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    record.write_bytes(previous)
    (descriptor, _), = write_journal.pending_intents({})
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not descriptor.exists() and _receipt_count(db) == 2
    assert len(writer.writes) == 1
    assert _r33_data_mutations(db, start, plans) == ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('drift', ['exact', 'missing', 'dropped'])
def test_r35_descriptor_free_guard_preflight(tmp_path, monkeypatch, drift):
    # Round 35 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    publication = tmp_path / 'publication'
    ack = _r26_snapshot(monkeypatch, publication, write_journal, 'retire_operations', lambda: write(db, plans))
    owner, _ = _r26_members(ack)
    scope = native_writer._target_scope(plans[0])
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[1]))
    record = write_journal._operation_path(scope, scope, owner)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    exact = record.read_bytes()
    assert write(db, plans) == ack
    record.write_bytes(exact)
    shutil.copyfile(publication / guard.relative_to(write_journal._root()), guard)
    if drift == 'missing':
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    elif drift == 'dropped':
        db.sql.execute(f'DROP TABLE {plans[0].table}')
    saved = _r26_file_bytes([record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='guard generation'):
        _r20_recover(db, writer, plans, False)
    assert _r26_file_bytes(saved) == saved and not write_journal.pending_intents({})
    _r28_no_dml(db, writer, start)
    guard.unlink()
    assert _r20_recover(db, writer, plans, False) == ack
    assert len(writer.writes) == {'exact': 0, 'missing': 1, 'dropped': 2}[drift]
    assert not record.exists() and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('step', ['target', 'receipt'])
@pytest.mark.parametrize('boundary', ['pending-statements', 'dispatch-attempts', 'settled-statements'])
def test_r35_predecessor_settlement_marker(monkeypatch, replay, step, boundary):
    # Round 36 finding 5
    # Round 35 finding 3
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    scope = native_writer._target_scope(plans[0])
    saved = {}
    unlink = write_journal._unlink_record

    def capture(path):
        if path.parent.name == boundary and path.exists():
            obj = write_journal._read_record(path)
            st = obj.get('statement', obj)
            key, op = st['key'], st['operation']
            expected = list(scope) if step == 'target' else [native_writer.CORE_RECEIPTS_TABLE, op['member']]
            if op['scope'] == list(scope) and key == expected:
                record = write_journal._operation_path(scope, key, op['member'])
                saved.update(_r26_file_bytes([record, *_r27_dispatch_files(st)]))
        unlink(path)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', capture)
        ack = write(db, plans)
    _, tail = _r26_members(ack)
    if replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record, _, _, marker, _ = saved
    for path, data in saved.items():
        if data is not None:
            path.write_bytes(data)
    exact = write_journal._decode_record(saved[marker])
    write_journal._store_record(marker, {**exact, 'query_id': 'wrong'}, replace=True)
    writer, start = CoreWriter(db), len(db.events)
    conflict = _r26_file_bytes(saved)
    with pytest.raises(RuntimeError, match='settled query binding'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == conflict
    _r28_no_dml(db, writer, start)
    marker.write_bytes(saved[marker])
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not marker.exists() and not writer.writes
    assert len(_r33_data_mutations(db, start, plans)) == int(replay)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restored', [False, True])
def test_r35_reserved_worklist_predecessor(monkeypatch, replay, restored):
    # Round 35 finding 5
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    head = write_journal.submission_head(owner)
    assert head['receipt_repair']['before'][owner] == write_journal._decode_record(attempts[1])['statement']['attempt']
    if restored:
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    record.write_bytes(attempts[0])
    descriptor, = [p for p, _ in write_journal.pending_intents({})]
    saved = _r26_file_bytes([descriptor, record, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='reserved operation outcome'):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, replay) == ack and not record.exists() and not descriptor.exists()
    assert not writer.writes and _r33_data_mutations(db, start, plans) == (
        [] if restored else ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE])


@pytest.mark.parametrize('replay', [False, True])
def test_r35_reserved_step_predecessor(monkeypatch, replay):
    # Round 35 finding 6
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack, record = _r33_restore_step_predecessor(db, plans, monkeypatch, unreserved=False)
    previous = record.read_bytes()
    record.unlink()
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    _r28_interrupt(monkeypatch, write_journal, 'reserve_scopes', lambda: write(db, plans))
    record.write_bytes(previous)
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack and not record.exists()
    assert len(writer.writes) == 1 and _r33_data_mutations(db, start, plans) == [
        'INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE]


@pytest.mark.parametrize('entry', ['ordinary', 'worklist', 'replay'])
def test_r35_completed_noop_without_descriptor(monkeypatch, entry):
    # Round 35 finding 7
    db, attendance = CoreDB(), _snapshot(CLOCK, 'accounted-noop')
    write(db, [attendance])
    plans = [attendance, *_representation_packet(('7', '8', '9'), CLOCK)]
    with monkeypatch.context() as patch:
        save = write_journal._save_submission
        patch.setattr(write_journal, '_save_submission',
                      lambda head: save({k: v for k, v in head.items() if k != 'before_target_tracking'}))
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    scope = native_writer._target_scope(attendance)
    record = write_journal._operation_path(scope, scope, write_journal.scope_owner(scope))
    unchanged = record.read_bytes()
    ack = write(db, plans)
    if entry != 'ordinary':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[-1],))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(unchanged)
    witness, = (write_journal._root() / 'verified-noops').glob('*.json')
    completion = write_journal._record_path('completed-intents', write_journal._read_record(record)['operation']['intent'])
    writer, start = CoreWriter(db), len(db.events)
    for proof in (witness, completion):
        data = proof.read_bytes()
        proof.unlink()
        with pytest.raises(RuntimeError, match='predecessor operation'):
            _r20_recover(db, writer, plans, entry == 'replay')
        assert record.read_bytes() == unchanged
        _r28_no_dml(db, writer, start)
        proof.write_bytes(data)
    assert _r20_recover(db, writer, plans, entry == 'replay') == ack
    assert not writer.writes and not record.exists() and not write_journal.pending_intents({})
    assert len(_r33_data_mutations(db, start, plans)) == int(entry != 'ordinary')


@pytest.mark.parametrize('replay', [False, True])
def test_r35_finished_predecessor_mapping(monkeypatch, replay):
    # Round 35 finding 8
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    owner, tail = _r26_members(ack)
    scope = native_writer._target_scope(plans[0])
    record = write_journal._operation_path(scope, scope, owner)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    with pytest.raises(TrinoAmbiguousWriteError):
        _r20_recover(db, _LostResponse(db), plans, False)
    statement = write_journal.read_statement(scope)
    mapping = write_journal._record_path('statement-queries', statement['attempt'])
    mapped = mapping.read_bytes()
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    exact = record.read_bytes()
    assert write_journal._decode_record(exact)['state'] == 'FINISHED' and write(db, plans) == ack
    if replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(exact)
    write_journal.remember_query(statement, 'wrong-query')
    saved = _r26_file_bytes([record, mapping])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    mapping.write_bytes(mapped)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not mapping.exists() and not record.exists() and not writer.writes
    assert len(_r33_data_mutations(db, start, plans)) == int(replay)


@pytest.mark.parametrize('replay', [False, True])
def test_r35_unknown_sibling_preserves_predecessor(monkeypatch, replay):
    # Round 35 finding 10
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    first, sibling = map(native_writer._target_scope, plans)
    files = {}
    finish = write_journal.finish_statement

    def capture(statement, **kwargs):
        if statement['key'] == list(sibling):
            files.update(_r26_file_bytes(_r27_dispatch_files(statement)[:2]))
        return finish(statement, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'finish_statement', capture)
        ack = write(db, plans)
    owner, tail = _r26_members(ack)
    record = write_journal._operation_path(first, first, owner)
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    _r28_interrupt(monkeypatch, write_journal, 'retire_operations', lambda: write(db, plans))
    exact = record.read_bytes()
    assert write(db, plans) == ack
    if replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(exact)
    for path, data in files.items():
        path.write_bytes(data)
    assert write_journal.latest_operation(sibling, sibling, tail) is None
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='pending'):
        _r20_recover(db, writer, plans, replay)
    assert record.read_bytes() == exact and _r26_file_bytes(files) == files
    _r28_no_dml(db, writer, start)
    for path in files:
        path.unlink()
    assert _r20_recover(db, writer, plans, replay) == ack and not record.exists()
    assert not writer.writes and len(_r33_data_mutations(db, start, plans)) == int(replay)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore_record', [False, True])
def test_r35_completed_unreserved_worklist(monkeypatch, replay, restore_record):
    # Round 35 finding 11
    db, plans, ack, _, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    _r28_interrupt(monkeypatch, write_journal, 'release_obsolete_guards', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    writer, start = CoreWriter(db), len(db.events)
    if restore_record:
        record.write_bytes(attempts[0])
        with pytest.raises(RuntimeError, match='predecessor operation'):
            _r20_recover(db, writer, plans, replay)
        assert record.read_bytes() == attempts[0] and descriptor.exists()
        record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not descriptor.exists() and not record.exists()
    assert not writer.writes and not _r33_data_mutations(db, start, plans)


def _r36_capture_target(monkeypatch, db, plans, index):
    scope = native_writer._target_scope(plans[index])
    saved = {}
    retire = write_journal.retire_operations

    def capture(scopes, owner):
        for record in write_journal.scope_operations(scope):
            if record['key'] == list(scope):
                path = write_journal._operation_path(scope, scope, record['operation']['member'])
                saved[path] = path.read_bytes()
        retire(scopes, owner)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'retire_operations', capture)
        ack = write(db, plans)
    (path, data), = saved.items()
    return ack, path, data


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore_record', [False, True])
@pytest.mark.parametrize('drift', ['missing', 'sibling', 'target'])
def test_r36_completed_worklist_cannot_submit(monkeypatch, replay, restore_record, drift):
    # Round 36 finding 1
    db, plans, ack, owner, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    tail = _r26_members(ack)[-1]
    _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    _r28_interrupt(monkeypatch, write_journal, 'release_obsolete_guards', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    assert write_journal.completed_intent(descriptor.stem)
    if restore_record:
        record.write_bytes(attempts[1])
    if drift == 'target':
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    else:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail if drift == 'missing' else owner,))
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not descriptor.exists()
    assert len(writer.writes) == int(not replay and drift == 'target')
    assert _r33_data_mutations(db, start, plans) == (
        ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE] if not replay and drift != 'target' else [])
    assert write_journal.submission_head(owner)['statement']['operation']['intent'] != descriptor.stem
    if replay:
        _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('drift', ['exact', 'missing', 'drop', 'receipt'])
def test_r36_release_exact_retired_guard(tmp_path, monkeypatch, drift):
    # Round 36 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    snapshot = tmp_path / 'publication'
    ack, record, data = _r26_snapshot(monkeypatch, snapshot, write_journal, 'complete_intent',
                                      lambda: _r36_capture_target(monkeypatch, db, plans, 0))
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[1]))
    shutil.copyfile(snapshot / guard.relative_to(write_journal._root()), guard)
    record.write_bytes(data)
    owner, tail = _r26_members(ack)
    assert write_journal._read_record(guard)['intent'] == write_journal.submission_head(owner)['statement']['operation']['intent']
    if drift == 'drop':
        db.sql.execute(f'DROP TABLE {plans[0].table}')
    elif drift == 'missing':
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    elif drift == 'receipt':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    writer, start = CoreWriter(db), len(db.events)
    assert _r20_recover(db, writer, plans, False) == ack
    assert not record.exists() and not guard.exists() and not write_journal.pending_intents({})
    assert len(writer.writes) == {'drop': 2, 'missing': 1}.get(drift, 0)
    assert _r33_data_mutations(db, start, plans) == (
        ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE] if drift == 'receipt' else [])


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('restore', ['historical', 'current', 'clean'])
def test_r36_historical_page_dispatch(monkeypatch, replay, restore):
    # Round 36 finding 3
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    url, _, page = _walk_pages()
    older = _attendance_plan((page(url, 'single_page.html', EARLIER, 'old'),))
    db = CoreDB()
    old = write(db, [older], run_id='run-a', clock=EARLIER)
    historical = old['walks'][0]['receipt_id']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'fresh'),)), *_representation_packet(('7',), CLOCK)]
    saved = {}
    finish = write_journal.finish_statement

    def capture(statement, **kwargs):
        if statement['key'][0] == native_writer.CORE_PAGES_TABLE:
            saved[statement['key'][1]] = _r26_file_bytes(_r27_dispatch_files(statement)[:2])
        return finish(statement, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'finish_statement', capture)
        ack, record, data = _r36_capture_target(patch, db, plans, 1)
    owner, tail = _r26_members(ack)
    if replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(data)
    writer, start = CoreWriter(db), len(db.events)
    if restore != 'clean':
        files = saved[historical if restore == 'historical' else owner]
        for path, content in files.items():
            path.write_bytes(content)
        with pytest.raises(RuntimeError, match='pending'):
            _r20_recover(db, writer, plans, replay)
        assert record.read_bytes() == data and _r26_file_bytes(files) == files
        _r28_no_dml(db, writer, start)
        for path in files:
            path.unlink()
    assert _r20_recover(db, writer, plans, replay) == ack and not record.exists()
    assert not writer.writes and len(_r33_data_mutations(db, start, plans)) == int(replay)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('drift', ['schema', 'newer', 'page'])
def test_r36_predecessor_warehouse_preflight(monkeypatch, replay, drift):
    # Round 36 finding 4
    db = CoreDB()
    plans = ([_snapshot(CLOCK, 'base'), *_representation_packet(('7',), CLOCK)] if drift == 'page'
             else _representation_packet(('7', '9'), CLOCK))
    ack, record, data = _r36_capture_target(monkeypatch, db, plans, -1)
    if replay:
        db.sql.execute(f'DELETE FROM {plans[-1].table} WHERE player_id=?', (plans[-1].scope_values[0],))
        _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    if drift == 'schema':
        db.sql.execute(f'ALTER TABLE {plans[0].table} RENAME COLUMN scope_id TO unexpected')
    elif drift == 'page':
        db.sql.execute("UPDATE transfermarkt_core_pages_v1 SET source_body_hash='conflicting'")
    else:
        db.sql.execute(f"UPDATE {plans[0].table} SET captured_at=?, observed_at=? WHERE player_id='7'", (LATER, LATER))
    record.write_bytes(data)
    writer, start = CoreWriter(db), len(db.events)
    if drift == 'newer':
        assert _r20_recover(db, writer, plans, replay) == ack and not record.exists()
        assert len(writer.writes) == 1 + int(replay)
    else:
        with pytest.raises(RuntimeError, match='differs'):
            _r20_recover(db, writer, plans, replay)
        assert record.read_bytes() == data
        _r28_no_dml(db, writer, start)
        assert not any(native_writer.is_mutation(q) for q in db.events[start:])


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('released', [False, True])
def test_r36_completed_reservation_frozen_attempt(monkeypatch, replay, released):
    # Round 36 finding 6
    db, plans, ack, _, record, attempts = _r32_repaired_packet_missing_receipt(monkeypatch)
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    boundary = (native_writer, '_release_packet') if released else (write_journal, 'retire_operations')
    _r28_interrupt(monkeypatch, *boundary, lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    writer, start = CoreWriter(db), len(db.events)
    record.write_bytes(attempts[0])
    with pytest.raises(RuntimeError, match='pending'):
        _r20_recover(db, writer, plans, replay)
    assert record.read_bytes() == attempts[0] and descriptor.exists()
    record.write_bytes(attempts[1])
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not descriptor.exists()
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('descriptor', [False, True])
def test_r36_modern_noop_witness(tmp_path, monkeypatch, replay, descriptor):
    # Round 36 finding 7
    db, attendance = CoreDB(), _snapshot(CLOCK, 'modern')
    write(db, [attendance])
    plans = [attendance, *_representation_packet(('7', '8', '9'), CLOCK)]
    snapshot = tmp_path / 'original'
    ack, record, data = _r26_snapshot(monkeypatch, snapshot, write_journal, 'retire_operations',
                                      lambda: _r36_capture_target(monkeypatch, db, plans, 0))
    op = write_journal._decode_record(data)
    assert op['state'] == 'unchanged'
    owner, *_, tail = _r26_members(ack)
    proof = {'head': native_writer._digest(write_journal.submission_head(owner)), 'operation': op}
    witness = write_journal._record_path('verified-noops', native_writer._digest(proof))
    assert write_journal._read_record(witness) == proof
    if descriptor:
        _r26_restore(snapshot)
        (pending, _), = write_journal.pending_intents({})
        assert write_journal.completed_intent(pending.stem)
    elif replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    record.write_bytes(data)
    writer, start = CoreWriter(db), len(db.events)
    if not descriptor:
        content = witness.read_bytes()
        witness.unlink()
        with pytest.raises(RuntimeError, match='predecessor operation'):
            _r20_recover(db, writer, plans, replay)
        assert record.read_bytes() == data
        _r28_no_dml(db, writer, start)
        witness.write_bytes(content)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not writer.writes
    assert len(_r33_data_mutations(db, start, plans)) == int(replay and not descriptor)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize(('window', 'legacy'), [
    ('unreserved', False), ('reserved', False), ('release', False), ('general', False),
    ('reserved', True), ('release', True), ('general', True)])
def test_r36_noop_across_reservation(monkeypatch, replay, window, legacy):
    # Round 37 finding 8
    # Round 36 finding 8
    db, attendance = CoreDB(), _snapshot(CLOCK, 'accounted-noop')
    write(db, [attendance])
    plans = [attendance, *_representation_packet(('7', '8', '9'), CLOCK)]
    with monkeypatch.context() as patch:
        save = write_journal._save_submission
        patch.setattr(write_journal, '_save_submission',
                      lambda head: save({k: v for k, v in head.items() if k != 'before_target_tracking'}))
        _r28_interrupt(patch, native_writer, '_merge_receipts', lambda: write(db, plans))
    scope = native_writer._target_scope(attendance)
    owner = write_journal.scope_owner(scope)
    record = write_journal._operation_path(scope, scope, owner)
    original = record.read_bytes()
    ack = write(db, plans)
    retired = write_journal.submission_head(owner)
    tail = _r26_members(ack)[-1]
    db.sql.execute(f'CREATE TABLE tail AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    if legacy:
        def save_old_reservation(head):
            if 'retired_steps' in head:
                steps = {k: v for k, v in head['retired_steps'].items() if k != 'head'}
                head = {**head, 'retired_steps': steps}
            save(head)
        monkeypatch.setattr(write_journal, '_save_submission', save_old_reservation)
    if window == 'unreserved':
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    elif window == 'general':
        db.sql.execute(f"DELETE FROM {plans[1].table} WHERE player_id='7'")
        _r28_interrupt(monkeypatch, write_journal, 'reserve_scopes', lambda: write(db, plans))
    else:
        _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
        if window == 'release':
            db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM tail')
            _r28_interrupt(monkeypatch, native_writer, '_release_packet', lambda: write(db, plans))
    record.write_bytes(original)
    writer, start = CoreWriter(db), len(db.events)
    if window != 'unreserved':
        head = write_journal.submission_head(owner)
        (descriptor, body), = write_journal.pending_intents({})
        assert body['payload']['predecessor_head'] == native_writer._digest(retired)
        if legacy:
            assert 'head' not in head['retired_steps']
            content = descriptor.read_bytes()
            descriptor.unlink()
            with pytest.raises(RuntimeError):
                _r20_recover(db, writer, plans, replay)
            assert record.read_bytes() == original
            _r28_no_dml(db, writer, start)
            descriptor.write_bytes(content)
        else:
            assert head['retired_steps']['head'] == native_writer._digest(retired)
        proof = {'head': native_writer._digest(retired), 'operation': write_journal._read_record(record)}
        witness = write_journal._record_path('verified-noops', native_writer._digest(proof))
        content = witness.read_bytes()
        write_journal._store_record(witness, {**proof, 'head': native_writer._digest(head)}, replace=True)
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert record.read_bytes() == original
        _r28_no_dml(db, writer, start)
        witness.write_bytes(content)
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not write_journal.pending_intents({})
    assert len(writer.writes) == int(window == 'general')
    assert _r33_data_mutations(db, start, plans) == (
        [] if window == 'release' else ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE])


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('released', [False, True])
@pytest.mark.parametrize(('completed', 'receipt_only', 'restore_receipt'), [
    (completed, receipt_only, False) for completed in (False, True) for receipt_only in (False, True)
] + [(completed, True, state) for completed in (False, True) for state in ('returned', 'promotion')])
def test_r36_obsolete_retired_generation(tmp_path, monkeypatch, replay, released, completed, receipt_only, restore_receipt):
    # Round 39 finding 10
    # Round 39 finding 11
    # Round 38 finding 8
    # Round 37 finding 9
    # Round 36 finding 9
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    if restore_receipt == 'promotion':
        write(db, [plans[0]], run_id='earlier-sibling')
    ack = write(db, plans)
    owner, _ = _r26_members(ack)
    root = write_journal._root()
    published, obsolete = tmp_path / 'published', tmp_path / 'obsolete'
    shutil.copytree(root, published)
    if receipt_only:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[-1],))
    else:
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
    boundary = '_release_packet' if completed else ('_merge_receipts' if receipt_only else '_write_plan')
    with monkeypatch.context() as patch:
        if restore_receipt:
            tail = _r26_members(ack)[-1]
            if restore_receipt == 'promotion':
                _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
            directory = 'statement-queries' if restore_receipt == 'promotion' else 'pending-statements'
            saved = _r37_capture_settlement(patch, directory, lambda key: key == [native_writer.CORE_RECEIPTS_TABLE, tail])
        _r26_snapshot(patch, obsolete, native_writer, boundary, lambda: write(db, plans))
    root.rename(tmp_path / 'first-finished')
    published.rename(root)
    db.sql.execute(f"CREATE TABLE nine AS SELECT * FROM {plans[0].table} WHERE player_id='9'")
    db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='9'")
    _r28_interrupt(monkeypatch, native_writer, '_write_plan', lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {plans[0].table} SELECT * FROM nine')
    if released:
        _r28_interrupt(monkeypatch, native_writer, '_release_packet', lambda: write(db, plans))
    _r26_restore(obsolete)
    (descriptor, _), = write_journal.pending_intents({})
    head = write_journal.submission_head(owner)
    assert descriptor.stem not in head['after_publication'] and descriptor.stem == head['retired_steps']['intent']
    assert bool(write_journal.completed_intent(descriptor.stem)) == completed
    writer, start = CoreWriter(db), len(db.events)
    if restore_receipt:
        record = next(iter(saved))
        exact = write_journal._decode_record(saved[record])
        statement = exact['statement']
        assert head['members'][tail] == statement['attempt']
        write_journal._store_record(record, {**exact, 'statement': {**statement, 'attempt': 'wrong'}})
        evidence = _r26_file_bytes([descriptor, record, *_r27_guards(plans, owner)])
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        record.write_bytes(saved[record])
        # A verified receipt attempt cannot retire before its sibling's cells match.
        db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (owner,))
        evidence = _r26_file_bytes(evidence)
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', (ack['plans'][0]['physical_hash'], owner))
    if restore_receipt == 'promotion':
        # Round 40 finding 6
        _, pending, attempt, marker, mapping = saved
        mapping.parent.mkdir(exist_ok=True)
        for path in (pending, attempt, mapping):
            path.write_bytes(saved[path])
        db.sql.execute(f"UPDATE {RECEIPTS} SET row_count=0 WHERE run_id='earlier-sibling'")
        for window in ('before-marker', 'marker', 'mapping-unlink'):
            if window == 'marker':
                marker.write_bytes(saved[marker])
            if window == 'mapping-unlink':
                pending.unlink()
                attempt.unlink()
            evidence = _r26_file_bytes([descriptor, *saved, *_r27_guards(plans, owner)])
            with pytest.raises(RuntimeError, match='differs from committed capture'):
                _r20_recover(db, writer, plans, replay)
            assert _r26_file_bytes(evidence) == evidence
            _r28_no_dml(db, writer, start)
        db.sql.execute(f"UPDATE {RECEIPTS} SET row_count=1 WHERE run_id='earlier-sibling'")
        marker.unlink()
        for path in (pending, attempt):
            path.write_bytes(saved[path])
        promote = write_journal.verified_operation

        def stop_after_promotion(statement, query):
            promote(statement, query)
            _r22_stop()

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, 'verified_operation', stop_after_promotion)
            with pytest.raises(OSError):
                _r20_recover(db, writer, plans, replay)
        assert write_journal._read_record(record)['state'] == 'FINISHED' and not marker.exists()
        evidence = _r26_file_bytes([descriptor, *saved, *_r27_guards(plans, owner)])
        query = exact['query_id']
        db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query,))
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
    assert _r20_recover(db, writer, plans, replay) == ack
    if restore_receipt:
        assert not record.exists()
    assert not descriptor.exists() and write_journal.submission_head(owner) == head
    assert write_journal.reservation_release_started(head) == released
    assert not write_journal.obsolete_guard_scopes(set(map(tuple, head['scopes'])), owner, descriptor.stem)
    _r28_no_dml(db, writer, start)


def _r37_capture_settlement(patch, directory, matches):
    saved = {}
    unlink = write_journal._unlink_record

    def capture(path):
        if not saved and path.parent.name == directory and path.exists():
            data = write_journal._read_record(path)
            statement = data.get('statement', data)
            if matches(statement['key']):
                op = statement['operation']
                record = write_journal._operation_path(op['scope'], statement['key'], op['member'])
                saved.update(_r26_file_bytes([record, *_r27_dispatch_files(statement)]))
        unlink(path)

    patch.setattr(write_journal, '_unlink_record', capture)
    return saved


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('damage', ['dispatch', 'schema', 'receipt'])
def test_r37_completed_worklist_conflict_without_operations(monkeypatch, replay, damage):
    # Round 37 finding 1
    with monkeypatch.context() as patch:
        saved = _r37_capture_settlement(patch, 'pending-statements', lambda k: k[0] == native_writer.CORE_RECEIPTS_TABLE)
        db, plans, ack, owner, _, _ = _r32_repaired_packet_missing_receipt(patch)
    _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    _r28_interrupt(monkeypatch, write_journal, 'release_obsolete_guards', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    assert write_journal.completed_intent(descriptor.stem)
    _, pending, attempt, _, _ = saved
    if damage == 'dispatch':
        for path in (pending, attempt):
            path.write_bytes(saved[path])
    elif damage == 'schema':
        db.sql.execute(f'ALTER TABLE {plans[0].table} RENAME COLUMN scope_id TO unexpected')
    else:
        db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (owner,))
    evidence = _r26_file_bytes([descriptor, *saved, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(evidence) == evidence
    if damage == 'dispatch':
        pending.unlink()
        attempt.unlink()
    elif damage == 'schema':
        db.sql.execute(f'ALTER TABLE {plans[0].table} RENAME COLUMN unexpected TO scope_id')
    else:
        db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=? WHERE receipt_id=?', (ack['plans'][0]['physical_hash'], owner))
    assert _r20_recover(db, writer, plans, replay) == ack and not descriptor.exists()
    assert not writer.writes and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
def test_r37_capture_conflict_before_sibling_repair(tmp_path, monkeypatch, replay):
    # Round 37 finding 2
    db = CoreDB()
    plans = [_snapshot(CLOCK, 'first'), *_representation_packet(('7',), CLOCK)]
    snapshot = tmp_path / 'published'
    ack, record, data = _r26_snapshot(monkeypatch, snapshot, write_journal, 'complete_intent',
                                      lambda: _r36_capture_target(monkeypatch, db, plans, -1))
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[-1]))
    if replay:
        db.sql.execute(f'DELETE FROM {plans[-1].table}')
        _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    else:
        shutil.copyfile(snapshot / guard.relative_to(write_journal._root()), guard)
    record.write_bytes(data)
    evidence = _r26_file_bytes([record, guard, *(p for p, _ in write_journal.pending_intents({}))])
    db.sql.execute(f'DROP TABLE {plans[-1].table}')
    capture, body = (plans[0].rows[0][key] for key in ('source_capture_id', 'source_body_hash'))
    db.sql.execute(f"UPDATE {plans[0].table} SET source_body_hash='wrong' WHERE source_capture_id=?", (capture,))
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError, match='differs from committed capture'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(evidence) == evidence
    assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (plans[-1].table,)).fetchone()
    db.sql.execute(f'UPDATE {plans[0].table} SET source_body_hash=? WHERE source_capture_id=?', (body, capture))
    assert _r20_recover(db, writer, plans, replay) == ack and len(writer.writes) == 1
    assert not record.exists() and not guard.exists() and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('window', ['orphan', 'settled', 'legacy', 'dispatched', 'dispatched-marker', 'reserved-legacy'])
def test_r37_historical_page_settlement(monkeypatch, replay, window):
    # Round 38 finding 2
    # Round 38 finding 6
    # Round 39 finding 3
    # Round 39 finding 8
    dispatched = window.startswith('dispatched')
    legacy = window in ('legacy', 'reserved-legacy')
    orphan = dispatched or window == 'orphan'
    # Round 37 finding 3
    # Round 37 finding 4
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    older = _attendance_plan((page(url, 'single_page.html', EARLIER, 'old'),))
    first = write(db, [older], run_id='early', clock=EARLIER)
    historical = first['walks'][0]['receipt_id']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'current'),)), *_representation_packet(('7',), CLOCK)]
    with monkeypatch.context() as patch:
        if legacy:
            replace_head = write_journal._replace_submission

            def legacy_head(head):
                replace_head({key: value for key, value in head.items()
                              if key not in ('before_target_tracking', 'page_attempts')})

            patch.setattr(write_journal, '_replace_submission', legacy_head)
        saved = _r37_capture_settlement(patch, 'pending-statements', lambda k: k == [native_writer.CORE_PAGES_TABLE, historical])
        ack = write(db, plans)
    record, pending, attempt, marker, _ = saved
    if dispatched:
        db.sql.execute(f'DELETE FROM {plans[-1].table}')
        _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
        (descriptor, _), = write_journal.pending_intents({})
        last = write_journal.submission_head(_r26_members(ack)[0])['statement']
        assert last['operation']['intent'] == descriptor.stem and last['key'] == list(native_writer._target_scope(plans[-1]))
    elif window == 'reserved-legacy':
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[-1],))
        _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    elif replay:
        if orphan:
            db.sql.execute(f'DELETE FROM {plans[-1].table}')
            _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
        else:
            db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[-1],))
            _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    files = (pending, attempt) if orphan else (record, pending, attempt, marker)
    if window == 'dispatched-marker':
        files = (marker,)
    for path in files:
        path.write_bytes(saved[path])
    writer, start = CoreWriter(db), len(db.events)
    if orphan:
        dropped = plans[0] if dispatched else plans[-1]
        db.sql.execute(f'ALTER TABLE {dropped.table} RENAME TO held_table')
    else:
        db.sql.execute("UPDATE transfermarkt_core_pages_v1 SET source_body_hash='wrong' WHERE receipt_id=?", (historical,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(files) == {path: saved[path] for path in files}
    _r28_no_dml(db, writer, start)
    if orphan:
        assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (dropped.table,)).fetchone()
        for path in files:
            path.unlink()
        if dispatched:
            db.sql.execute(f'ALTER TABLE held_table RENAME TO {dropped.table}')
    else:
        db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET source_body_hash=? WHERE receipt_id=?',
                       (older.pages[0][3], historical))
    if legacy:
        # With identical walk bytes, a different frozen member attempt is
        # still insufficient to authenticate the surviving page operation.
        head = write_journal.submission_head(_r26_members(ack)[0])
        assert 'page_attempts' not in head.get('retired_steps', head)
        earlier = write_journal._decode_record(saved[record])
        statement = {**earlier['statement'], 'attempt': 'other-attempt'}
        write_journal._store_record(record, {**earlier, 'statement': statement}, replace=True)
        for path in (pending, attempt):
            write_journal._store_record(path, statement, replace=True)
        write_journal._store_record(marker, {'statement': statement, 'query_id': None}, replace=True)
        conflict = _r26_file_bytes(files)
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(files) == conflict
        _r28_no_dml(db, writer, start)
        for path in files:
            path.write_bytes(saved[path])
    assert _r20_recover(db, writer, plans, replay) == ack and not any(path.exists() for path in saved)
    assert len(writer.writes) == int(window == 'orphan')
    assert _r33_data_mutations(db, start, plans) == (
        ['INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE] if (replay and not orphan or window == 'reserved-legacy') else [])


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('window', ['marker', 'before-marker', 'mapping-unlink', 'after-marker-unlink',
                                    'partial-before-marker', 'partial-mapping-unlink'])
def test_r37_returned_receipt_before_mapping_unlink(monkeypatch, replay, window):
    # Round 39 finding 9
    # Round 38 finding 7
    # Round 37 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    partial = window.startswith('partial-')
    window = window.removeprefix('partial-')
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    tail = native_writer._receipt_id(hashes[-1], 'run-1', digest)
    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_RECEIPTS_TABLE])
        saved = _r37_capture_settlement(patch, 'statement-queries', lambda k: k[0] == native_writer.CORE_RECEIPTS_TABLE
                                         and (not partial or k[1] == tail))
        ack = write(db, plans)
    record, pending, attempt, marker, mapping = saved
    query = write_journal._decode_record(saved[mapping])
    assert write_journal._decode_record(saved[record])['state'] == 'returned'
    if partial:
        _r38_partial_release(monkeypatch, db, plans, ack)
    elif replay:
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(ack)[-1],))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
    # The operation transition precedes the immutable marker. At the other
    # boundary both dispatch files have already been removed.
    if window == 'before-marker':
        saved[marker] = None
    elif window in ('mapping-unlink', 'after-marker-unlink'):
        saved[pending] = saved[attempt] = None
        if window == 'after-marker-unlink':
            saved[marker] = None
    for path, data in saved.items():
        if data is not None:
            path.write_bytes(data)
    writer, start = CoreWriter(db), len(db.events)
    write_journal._store_record(mapping, {**query, 'query_id': 'wrong'}, replace=True)
    mismatch = 'reserved operation outcome|binding|unbound core statement' if partial else 'binding|unbound core statement'
    with pytest.raises(RuntimeError, match=mismatch):
        _r20_recover(db, writer, plans, replay)
    mapping.write_bytes(saved[mapping])
    db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query['query_id'],))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(saved) == saved
    _r28_no_dml(db, writer, start)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query['query_id'],))
    if window == 'mapping-unlink':
        unlink = write_journal._unlink_record

        def interrupt_mapping(path):
            if path == mapping:
                _r22_stop()
            unlink(path)

        with monkeypatch.context() as patch:
            patch.setattr(write_journal, '_unlink_record', interrupt_mapping)
            with pytest.raises(OSError):
                _r20_recover(db, writer, plans, replay)
        assert marker.exists() and mapping.exists()
    assert _r20_recover(db, writer, plans, replay) == ack and not any(path.exists() for path in saved)
    assert not writer.writes and len(_r33_data_mutations(db, start, plans)) == int(replay and not partial)


def _r38_partial_release(monkeypatch, db, plans, ack):
    owner, tail = _r26_members(ack)
    db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
    release = write_journal.release_scope

    def release_first(scope, packet):
        release(scope, packet)
        _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, 'release_scope', release_first)
        with pytest.raises(OSError):
            write(db, plans)
    seven, nine = map(native_writer._target_scope, plans)
    assert write_journal.scope_owner(seven) is None and write_journal.scope_owner(nine) == owner
    return seven, nine


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('foreign_target', [False, True])
def test_r37_partial_release_with_restored_sibling(monkeypatch, replay, foreign_target):
    # Round 37 finding 6
    # Round 37 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack, record, data = _r36_capture_target(monkeypatch, db, plans, 1)
    owner, tail = _r26_members(ack)
    seven, nine = _r38_partial_release(monkeypatch, db, plans, ack)
    foreign_files = {}
    if foreign_target:
        class Queued(CoreWriter):
            dispatches_journalled_target = True

            def _write_to_iceberg(self, *args, **kwargs):
                _submit_lost_merge(db, query_id='foreign-target')

        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.write_core_bundle(lambda: db, Queued(db), _representation_packet(('7',), LATER),
                                            run_id='foreign', universe_version='core-universe-v1', committed_at=LATER)
        statement = write_journal.read_statement(seven)
        foreign_files = _r26_file_bytes([*_r27_dispatch_files(statement), write_journal._scope_owner_path(seven),
                                         write_journal._operation_path(seven, seven, statement['operation']['member'])])
        assert write_journal.statement_query(statement) == 'foreign-target'
    else:
        db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (owner,))
    record.write_bytes(data)
    writer, start = CoreWriter(db), len(db.events)
    (descriptor, _), = write_journal.pending_intents({'run_id': 'run-1'})
    held = _r26_file_bytes([descriptor, record, write_journal._scope_owner_path(nine)])
    db.sql.execute(f"UPDATE {RECEIPTS} SET physical_hash='wrong' WHERE receipt_id=?", (tail,))
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(held) == held and _r26_file_bytes(foreign_files) == foreign_files
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'UPDATE {RECEIPTS} SET physical_hash=(SELECT physical_hash FROM nine) WHERE receipt_id=?', (tail,))
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, False)
    assert not record.exists() and not write_journal.pending_intents({'run_id': 'run-1'})
    assert write_journal.scope_owner(nine) is None and _r26_file_bytes(foreign_files) == foreign_files
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('completed', [False, True])
def test_r38_missing_target_freshness(tmp_path, monkeypatch, replay, completed):
    # Round 38 finding 1
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'old'),)), *_representation_packet(('7',), CLOCK)]
    publication = tmp_path / 'publication'
    ack, record, data = _r26_snapshot(monkeypatch, publication, write_journal, 'complete_intent',
                                      lambda: _r36_capture_target(monkeypatch, db, plans, -1))
    owner, tail = _r26_members(ack)
    if completed:
        db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
        _r28_interrupt(monkeypatch, write_journal, 'release_obsolete_guards', lambda: write(db, plans))
    elif replay:
        db.sql.execute(f'DELETE FROM {plans[-1].table}')
        _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))

    snapshot = tmp_path / 'before-newer'
    shutil.copytree(write_journal._root(), snapshot)
    warehouse = db.sql.serialize()
    newer = _attendance_plan((page(url, 'single_page.html', LATER, 'new'),))
    later = write(db, [newer], run_id='later', clock=LATER)
    if completed:
        # The newer walk still orders this edition when its receipt row is lost.
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (_r26_members(later)[0],))
    db.sql.execute(f'DROP TABLE {plans[0].table}')

    def restore_predecessor():
        if not completed:
            if not replay:
                guard = write_journal._scope_owner_path(native_writer._target_scope(plans[-1]))
                shutil.copyfile(publication / guard.relative_to(write_journal._root()), guard)
            record.write_bytes(data)

    restore_predecessor()
    descriptors = [p for p, _ in write_journal.pending_intents({})]
    evidence = _r26_file_bytes([record, *descriptors, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(native_writer.StaleTransfermarktWrite, match='newer capture'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(evidence) == evidence
    assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (plans[0].table,)).fetchone()

    _r26_restore(snapshot)
    db.sql.deserialize(warehouse)
    restore_predecessor()
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not write_journal.pending_intents({})


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('damage', ['orphan-marker', 'released-body'])
def test_r38_partial_release_evidence(monkeypatch, replay, damage):
    # Round 38 finding 3
    # Round 38 finding 5
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    seven, nine = map(native_writer._target_scope, plans)
    with monkeypatch.context() as patch:
        saved = _r37_capture_settlement(patch, 'settled-statements', lambda key: key == list(seven))
        ack, sibling, data = _r36_capture_target(patch, db, plans, 1)
    _r38_partial_release(monkeypatch, db, plans, ack)
    sibling.write_bytes(data)
    _, _, _, marker, _ = saved
    table = plans[0].table
    if damage == 'orphan-marker':
        marker.write_bytes(saved[marker])
    else:
        assert plans[0].capture_id == plans[1].capture_id
        db.sql.execute(f"UPDATE {table} SET source_body_hash='wrong' WHERE player_id IN ('7', '9')")
    (descriptor, _), = write_journal.pending_intents({})
    evidence = _r26_file_bytes([descriptor, marker, sibling, write_journal._scope_owner_path(nine)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(evidence) == evidence
    _r28_no_dml(db, writer, start)
    if damage == 'orphan-marker':
        marker.unlink()
    else:
        db.sql.execute(f"UPDATE {table} SET source_body_hash=? WHERE player_id='9'", (plans[1].rows[0]['source_body_hash'],))
    if damage == 'released-body' and not replay:
        with pytest.raises(RuntimeError, match='differs'):
            _r20_recover(db, writer, plans, False)
    else:
        assert _r20_recover(db, writer, plans, replay) == ack
    assert not sibling.exists() and not descriptor.exists() and write_journal.scope_owner(nine) is None
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('step', ['target', 'historical-page', 'older-historical-page'])
def test_r38_foreign_dispatch_on_released_key(monkeypatch, replay, step):
    # Round 39 finding 6
    # Round 38 finding 4
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    plans = _representation_packet(('7', '9'), CLOCK)
    historical = None
    if step == 'older-historical-page':
        older = _attendance_plan((page(url, 'single_page.html', EARLIER, 'earlier'),))
        historical = write(db, [older], run_id='early', clock=EARLIER)['walks'][0]['receipt_id']
        db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    if step != 'target':
        plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'old'),)), plans[-1]]
    ack, record, data = _r36_capture_target(monkeypatch, db, plans, 0)
    member = _r26_members(ack)[0]
    seven, nine = _r38_partial_release(monkeypatch, db, plans, ack)
    if step == 'target':
        class Queued(CoreWriter):
            dispatches_journalled_target = True

            def _write_to_iceberg(self, *args, **kwargs):
                _submit_lost_merge(db, query_id='foreign-target')

        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.write_core_bundle(lambda: db, Queued(db), _representation_packet(('7',), LATER),
                                            run_id='foreign', universe_version='core-universe-v1', committed_at=LATER)
        key = seven
        record.write_bytes(data)
    else:
        if historical:
            assert historical in write_journal.submission_head(member)['members']
        key = (native_writer.CORE_PAGES_TABLE, historical or member)
        db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (key[1],))
        newer = _attendance_plan((page(url, 'single_page.html', LATER, 'new'),))
        with monkeypatch.context() as patch:
            _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, 'foreign-history', landed=False)
            with pytest.raises(TrinoAmbiguousWriteError):
                write(db, [newer], run_id='foreign', clock=LATER)
    statement = write_journal.read_statement(key)
    op = statement['operation']
    assert op['owner'] != member and op['scope'] == list(seven)
    foreign = _r26_file_bytes([*_r27_dispatch_files(statement), write_journal._scope_owner_path(seven),
                               write_journal._operation_path(seven, key, op['member'])])
    writer, start = CoreWriter(db), len(db.events)
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(RuntimeError, match='pending'):
            _r20_recover(db, writer, plans, False)
    assert _r26_file_bytes(foreign) == foreign
    assert not record.exists() and not write_journal.pending_intents({'run_id': 'run-1'})
    assert write_journal.scope_owner(nine) is None
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
def test_r39_dispatched_repair_freshness(monkeypatch, tmp_path, replay):
    # Round 39 finding 1
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'old'),)), *_representation_packet(('7',), CLOCK)]
    ack = write(db, plans)
    root = write_journal._root()
    publication = tmp_path / 'publication'
    shutil.copytree(root, publication)
    original = db.sql.serialize()
    newer = _attendance_plan((page(url, 'single_page.html', LATER, 'new'),))
    write(db, [newer], run_id='later', clock=LATER)
    newer_warehouse = db.sql.serialize()
    later_journal = _r26_restore(publication)

    db.sql.deserialize(original)
    db.sql.execute(f'DELETE FROM {plans[-1].table}')
    _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
    (descriptor, _), = write_journal.pending_intents({})
    owner = _r26_members(ack)[0]
    assert write_journal.submission_head(owner)['statement']['operation']['intent'] == descriptor.stem
    repaired = db.sql.serialize()
    active = tmp_path / 'active'
    shutil.copytree(root, active)

    db.sql.deserialize(newer_warehouse)
    for path in later_journal.rglob('*.json'):
        destination = root / path.relative_to(later_journal)
        if not destination.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
    index = write_journal._index_path()
    existing = set(index.read_bytes().splitlines(keepends=True))
    with index.open('ab') as stream:
        stream.writelines(line for line in (later_journal / index.name).read_bytes().splitlines(keepends=True)
                          if line not in existing)
    db.sql.execute(f'DROP TABLE {plans[0].table}')
    evidence = _r26_file_bytes([descriptor, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(native_writer.StaleTransfermarktWrite, match='newer capture'):
        _r20_recover(db, writer, plans, replay)
    _r28_no_dml(db, writer, start)
    assert _r26_file_bytes(evidence) == evidence
    assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (plans[0].table,)).fetchone()

    db.sql.deserialize(repaired)
    root.rename(tmp_path / 'stale')
    active.rename(root)
    assert _r20_recover(db, writer, plans, replay) == ack and not descriptor.exists()
    assert not writer.writes and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('completed', [False, True])
def test_r39_missing_merge_target_receipt(tmp_path, monkeypatch, replay, completed):
    # Round 40 finding 1
    # Round 39 finding 2
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    prior = write(db, [plans[0]], run_id='older')
    historical = native_writer._receipt_id(prior['identity']['plan_hashes'][0], 'older', prior['plan_sha256'])
    snapshot = tmp_path / 'publication'
    ack, record, data = _r26_snapshot(monkeypatch, snapshot, write_journal, 'complete_intent',
                                      lambda: _r36_capture_target(monkeypatch, db, plans, -1))
    owner, tail = _r26_members(ack)
    if completed:
        db.sql.execute(f'CREATE TABLE nine AS SELECT * FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        db.sql.execute(f'DELETE FROM {RECEIPTS} WHERE receipt_id=?', (tail,))
        _r32_stop_before_receipt_reservation(monkeypatch, lambda: write(db, plans))
        db.sql.execute(f'INSERT INTO {RECEIPTS} SELECT * FROM nine')
        _r28_interrupt(monkeypatch, write_journal, 'release_obsolete_guards', lambda: write(db, plans))
    elif replay:
        db.sql.execute(f"DELETE FROM {plans[-1].table} WHERE player_id='9'")
        _r28_interrupt(monkeypatch, write_journal, 'reserve_submission', lambda: write(db, plans))
    else:
        guard = write_journal._scope_owner_path(native_writer._target_scope(plans[-1]))
        shutil.copyfile(snapshot / guard.relative_to(write_journal._root()), guard)
    record.write_bytes(data)
    evidence = _r26_file_bytes([record, *(p for p, _ in write_journal.pending_intents({})), *_r27_guards(plans, owner)])
    table = plans[0].table
    db.sql.execute(f'ALTER TABLE {table} RENAME TO held_table')
    writer, start = CoreWriter(db), len(db.events)
    for column, wrong, exact in [('source_body_hash', 'a' * 64, plans[0].rows[0]['source_body_hash']),
                                 ('row_count', 0, 1), ('row_count', 2, 1), ('expected_rows', 2, 1)]:
        if column == 'expected_rows':
            # Even internally consistent counts must agree with this exact plan SHA.
            db.sql.execute(f'UPDATE {RECEIPTS} SET row_count=2 WHERE receipt_id=?', (historical,))
        db.sql.execute(f'UPDATE {RECEIPTS} SET {column}=? WHERE receipt_id=?', (wrong, historical))
        with pytest.raises(RuntimeError, match='differs from committed capture'):
            _r20_recover(db, writer, plans, replay)
        _r28_no_dml(db, writer, start)
        assert _r26_file_bytes(evidence) == evidence
        assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (table,)).fetchone()
        db.sql.execute(f'UPDATE {RECEIPTS} SET {column}=? WHERE receipt_id=?', (exact, historical))
    db.sql.execute(f'UPDATE {RECEIPTS} SET row_count=1 WHERE receipt_id=?', (historical,))
    db.sql.execute(f'ALTER TABLE held_table RENAME TO {table}')
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not record.exists() and not write_journal.pending_intents({})
    assert len(writer.writes) == int(replay and not completed) and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('window', ['orphan', 'legacy-precompletion', 'modern-precompletion', 'released', 'held'])
def test_r39_publication_historical_marker(monkeypatch, replay, window):
    # Round 40 finding 4
    # Round 40 finding 8
    # Round 39 finding 4
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    older = _attendance_plan((page(url, 'single_page.html', EARLIER, 'old'),))
    historical = write(db, [older], run_id='early', clock=EARLIER)['walks'][0]['receipt_id']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    plans = [_attendance_plan((page(url, 'single_page.html', CLOCK, 'current'),)), *_representation_packet(('7',), CLOCK)]
    if window == 'legacy-precompletion':
        _historical_packet(plans)
    if window == 'held':
        plans.reverse()
    release = write_journal.release_scope

    def stop_after_release(scope, owner):
        release(scope, owner)
        _r22_stop()

    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_PAGES_TABLE])
        saved = _r37_capture_settlement(patch, 'settled-statements',
                                         lambda key: key == [native_writer.CORE_PAGES_TABLE, historical])
        patch.setattr(write_journal, 'release_scope', stop_after_release)
        with pytest.raises(OSError):
            write(db, plans)
    record, _, _, marker, _ = saved
    marker.write_bytes(saved[marker])
    (descriptor, body), = write_journal.pending_intents({})
    if window.endswith('precompletion'):
        write_journal._record_path('completed-intents', descriptor.stem).unlink()
    guard = write_journal._scope_owner_path(native_writer._target_scope(plans[-1]))
    assert guard.exists()
    writer, start = CoreWriter(db), len(db.events)
    if window in ('released', 'held'):
        exact = write_journal._decode_record(saved[record])
        statement = {**exact['statement'], 'attempt': 'other'}
        write_journal._store_record(record, {**exact, 'statement': statement})
        write_journal._store_record(marker, {'statement': statement, 'query_id': exact['query_id']}, replace=True)
    evidence = _r26_file_bytes([marker, descriptor, guard, record])
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert _r26_file_bytes(evidence) == evidence
    _r28_no_dml(db, writer, start)
    if window in ('released', 'held'):
        for path in (record, marker):
            path.write_bytes(saved[path])
        evidence = _r26_file_bytes(evidence)
        query = exact['query_id']
        db.sql.execute("UPDATE test_query_history SET state='QUEUED' WHERE query_id=?", (query,))
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id=?", (query,))
        db.sql.execute("UPDATE transfermarkt_core_pages_v1 SET source_body_hash='wrong' WHERE receipt_id=?", (historical,))
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        _r28_no_dml(db, writer, start)
        db.sql.execute('UPDATE transfermarkt_core_pages_v1 SET source_body_hash=? WHERE receipt_id=?',
                       (older.pages[0][3], historical))
    else:
        marker.unlink()
    assert _r20_recover(db, writer, plans, replay) == write_journal.read_receipt(body['identity']['digest'])
    assert not any(path.exists() for path in evidence) and not writer.writes
    assert not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('damage', ['other-target', 'held-receipt', 'released-receipt', 'released-body'])
def test_r39_obsolete_release_capture_readback(tmp_path, monkeypatch, replay, damage):
    # Round 40 finding 5
    # Round 39 finding 5
    db = CoreDB()
    seven, nine, other = _representation_packet(('7', '9', '23'), CLOCK)
    plans = [seven, nine]
    write(db, [other], run_id='third')
    for index, plan in enumerate(plans):
        write(db, [plan], run_id=f'prior-{index}')
    ack = write(db, plans)
    backup = tmp_path / 'obsolete'
    db.sql.execute(f"DELETE FROM {nine.table} WHERE player_id='9'")
    assert _r26_snapshot(monkeypatch, backup, native_writer, '_write_plan', lambda: write(db, plans)) == ack
    db.sql.execute(f"DELETE FROM {nine.table} WHERE player_id='9'")
    assert write(db, plans) == ack
    _r26_restore(backup)
    guards = [write_journal._scope_owner_path(native_writer._target_scope(p)) for p in plans]
    unlink = write_journal._unlink_record

    def stop_after_guard(path):
        unlink(path)
        if path in guards:
            _r22_stop()

    with monkeypatch.context() as patch:
        patch.setattr(write_journal, '_unlink_record', stop_after_guard)
        with pytest.raises(OSError):
            _r20_recover(db, CoreWriter(db), plans, True)
    assert sum(p.exists() for p in guards) == 1
    writer, start = CoreWriter(db), len(db.events)
    if damage == 'other-target':
        db.sql.execute(f"UPDATE {nine.table} SET source_body_hash=? WHERE player_id='23'", ('a' * 64,))
    else:
        index = next(i for i, g in enumerate(guards) if g.exists() == (damage == 'held-receipt'))
        if damage == 'released-body':
            assert seven.capture_id == nine.capture_id
            db.sql.execute(f'UPDATE {RECEIPTS} SET source_body_hash=? WHERE run_id=?', ('a' * 64, f'prior-{index}'))
        else:
            db.sql.execute(f'UPDATE {RECEIPTS} SET row_count=0 WHERE run_id=?', (f'prior-{index}',))
        if damage == 'held-receipt':
            evidence = _r26_file_bytes([*guards, *(p for p, _ in write_journal.pending_intents({}))])
            with pytest.raises(RuntimeError, match='differs from committed capture'):
                _r20_recover(db, writer, plans, replay)
            assert _r26_file_bytes(evidence) == evidence
            db.sql.execute(f'UPDATE {RECEIPTS} SET row_count=1 WHERE run_id=?', (f'prior-{index}',))
    if replay or damage == 'held-receipt':
        assert _r20_recover(db, writer, plans, replay) == ack
    else:
        with pytest.raises(RuntimeError, match='differs from committed capture'):
            _r20_recover(db, writer, plans, False)
    _r28_no_dml(db, writer, start)
    assert not any(path.exists() for path in guards)
    db.sql.execute(f"UPDATE {nine.table} SET source_body_hash=? WHERE player_id='23'", (other.rows[0]['source_body_hash'],))
    if damage == 'released-receipt':
        db.sql.execute(f'UPDATE {RECEIPTS} SET row_count=1 WHERE run_id=?', (f'prior-{index}',))
    if damage == 'released-body':
        db.sql.execute(f'UPDATE {RECEIPTS} SET source_body_hash=? WHERE run_id=?',
                       (plans[index].rows[0]['source_body_hash'], f'prior-{index}'))
    assert _r20_recover(db, writer, plans, False) == ack


@pytest.mark.parametrize('replay', [False, True])
def test_r39_foreign_marker_on_released_target(monkeypatch, replay):
    # Round 39 finding 7
    db, plans = CoreDB(), _representation_packet(('7', '9'), CLOCK)
    ack = write(db, plans)
    seven, nine = _r38_partial_release(monkeypatch, db, plans, ack)
    other = _representation_packet(('7',), LATER)

    class Rejected(CoreWriter):
        dispatches_journalled_target = True

        def _write_to_iceberg(self, *args, **kwargs):
            _rejected_target(db, TrinoAmbiguousWriteError('lost rejection'))

    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(lambda: db, Rejected(db), other, run_id='foreign',
                                        universe_version='core-universe-v1', committed_at=LATER)
    with monkeypatch.context() as patch:
        saved = _r37_capture_settlement(patch, 'settled-statements', lambda key: key == list(seven))
        assert write(db, other, run_id='foreign', clock=LATER)['verified']
    record, _, _, marker, _ = saved
    assert write_journal._decode_record(saved[record])['state'] == 'rejected'
    for path in (record, marker):
        path.write_bytes(saved[path])
    evidence = _r26_file_bytes(saved)
    writer, start = CoreWriter(db), len(db.events)
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(native_writer.StaleTransfermarktWrite, match='released target'):
            _r20_recover(db, writer, plans, False)
    assert write_journal.scope_owner(nine) is None and not write_journal.pending_intents({})
    assert _r26_file_bytes(saved) == evidence
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('damage', ['landed-merge', 'publisher-marker'])
def test_r40_dispatched_repair_before_table_creation(monkeypatch, replay, damage):
    # Round 40 finding 2
    # Round 40 finding 3
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    earlier = _attendance_plan((page(url, 'single_page.html', EARLIER, 'earlier'),))
    with monkeypatch.context() as patch:
        saved = _r37_capture_settlement(patch, 'settled-statements', lambda key: key[0] == native_writer.CORE_PAGES_TABLE)
        historical = write(db, [earlier], run_id='early', clock=EARLIER)['walks'][0]['receipt_id']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    att = _attendance_plan((page(url, 'single_page.html', CLOCK, 'current'),))
    rep, = _representation_packet(('7',), CLOCK)
    plans = [att, rep]
    write(db, [rep], run_id='prior')
    ack = write(db, plans)
    db.sql.execute(f'DELETE FROM {rep.table}')
    if damage == 'landed-merge':
        with pytest.raises(TrinoAmbiguousWriteError):
            _r20_recover(db, _LostResponse(db), plans, False)
        pending = write_journal.read_statement(native_writer._target_scope(rep))
        assert write_journal.statement_query(pending) == 'q_lost_merge'
        op = pending['operation']
        files = [*_r27_dispatch_files(pending), write_journal._operation_path(op['scope'], pending['key'], op['member'])]
        db.sql.execute(f"UPDATE {RECEIPTS} SET row_count=0 WHERE run_id='prior'")
    else:
        _r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: write(db, plans))
        record, _, _, marker, _ = saved
        marker.write_bytes(saved[marker])
        assert not record.exists()
        files = list(saved)
    db.sql.execute(f'ALTER TABLE {att.table} RENAME TO held_attendance')
    (descriptor, _), = write_journal.pending_intents({})
    owner = _r26_members(ack)[0]
    evidence = _r26_file_bytes([*files, descriptor, *_r27_guards(plans, owner)])
    writer, start = CoreWriter(db), len(db.events)
    with pytest.raises(RuntimeError):
        _r20_recover(db, writer, plans, replay)
    assert not db.sql.execute('SELECT name FROM sqlite_master WHERE name=?', (att.table,)).fetchone()
    assert _r26_file_bytes(evidence) == evidence
    _r28_no_dml(db, writer, start)
    db.sql.execute(f'ALTER TABLE held_attendance RENAME TO {att.table}')
    if damage == 'landed-merge':
        db.sql.execute(f"UPDATE {RECEIPTS} SET row_count=1 WHERE run_id='prior'")
    else:
        marker.unlink()
    assert _r20_recover(db, writer, plans, replay) == ack
    assert not descriptor.exists() and not writer.writes and not _r33_data_mutations(db, start, plans)


@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('queued', [False, True])
def test_r40_foreign_historical_marker_after_release(monkeypatch, replay, queued):
    # Round 40 finding 7
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()

    def attendance(clock, tag):
        return _attendance_plan((page(url, 'single_page.html', clock, tag),))

    old = write(db, [attendance(EARLIER, 'initial')], run_id='original', clock=EARLIER)
    historical = old['walks'][0]['receipt_id']
    key = [native_writer.CORE_PAGES_TABLE, historical]
    remove_walk = 'DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?'
    db.sql.execute(remove_walk, (historical,))
    plans = [attendance(CLOCK, 'current'), *_representation_packet(('9',), CLOCK)]
    ack = write(db, plans)
    released, held = _r38_partial_release(monkeypatch, db, plans, ack)
    db.sql.execute(remove_walk, (historical,))
    with monkeypatch.context() as patch:
        saved = _r37_capture_settlement(patch, 'settled-statements', lambda k: k == key)
        write(db, [attendance(LATER, 'later')], run_id='later', clock=LATER)
    record, _, _, marker, _ = saved
    assert not record.exists()
    files = [*saved, write_journal._scope_owner_path(released)]
    if queued:
        clock = '2026-11-21T14:00:00.000000+00:00'
        db.sql.execute(remove_walk, (historical,))
        with monkeypatch.context() as patch:
            _interrupt_ops_insert(patch, db, native_writer.CORE_PAGES_TABLE, 'queued-walk', landed=False)
            with pytest.raises(TrinoAmbiguousWriteError):
                write(db, [attendance(clock, 'next')], run_id='next', clock=clock)
        pending = write_journal.read_statement(key)
        assert pending['operation']['owner'] == write_journal.scope_owner(released)
        files.extend(_r27_dispatch_files(pending))
    marker.write_bytes(saved[marker])
    evidence = _r26_file_bytes(files)
    writer, start = CoreWriter(db), len(db.events)
    if replay:
        assert _r20_recover(db, writer, plans, True) == ack
    else:
        with pytest.raises(StaleTransfermarktWrite, match='released target'):
            _r20_recover(db, writer, plans, False)
    assert _r26_file_bytes(evidence) == evidence
    assert write_journal.scope_owner(held) is None and not write_journal.pending_intents({'run_id': 'run-1'})
    _r28_no_dml(db, writer, start)


@pytest.mark.parametrize('replay', [False, True], ids=['direct', 'replay'])
@pytest.mark.parametrize('conflict', [False, True], ids=['exact', 'held-conflict'])
def test_closing_historical_page_checks_held_capture(monkeypatch, replay, conflict):
    from tests.unit.scrapers.test_transfermarkt_sections import _attendance_plan

    db = CoreDB()
    url, _, page = _walk_pages()
    earlier = _attendance_plan((page(url, 'single_page.html', EARLIER, 'earlier'),))
    historical = write(db, [earlier], run_id='earlier', clock=EARLIER)['walks'][0]['receipt_id']
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id=?', (historical,))
    attendance = _attendance_plan((page(url, 'single_page.html', CLOCK, 'current'),))
    representation, = _representation_packet(('7',), CLOCK)
    write(db, [representation], run_id='prior-sibling')
    plans = [attendance, representation]
    release = write_journal.release_scope

    def stop_after_first_release(scope, owner):
        release(scope, owner)
        _r22_stop()

    with monkeypatch.context() as patch:
        _capture_returned_queries(patch, db, [native_writer.CORE_PAGES_TABLE])
        saved = _r37_capture_settlement(patch, 'settled-statements',
                                        lambda key: key == [native_writer.CORE_PAGES_TABLE, historical])
        patch.setattr(write_journal, 'release_scope', stop_after_first_release)
        with pytest.raises(OSError):
            write(db, plans)
    (descriptor, body), = write_journal.pending_intents({})
    ack = write_journal.read_receipt(body['identity']['digest'])
    operation, _, _, marker, _ = saved
    for path in (operation, marker):
        path.write_bytes(saved[path])
    assert write_journal.scope_owner(native_writer._target_scope(attendance)) is None
    guard = write_journal._scope_owner_path(native_writer._target_scope(representation))
    evidence = _r26_file_bytes([guard, descriptor, operation, marker])
    assert all(evidence.values())
    writer, start = CoreWriter(db), len(db.events)
    if conflict:
        db.sql.execute(f"UPDATE {RECEIPTS} SET row_count=0 WHERE run_id='prior-sibling'")
        with pytest.raises(RuntimeError):
            _r20_recover(db, writer, plans, replay)
        assert _r26_file_bytes(evidence) == evidence
        _r28_no_dml(db, writer, start)
    else:
        assert _r20_recover(db, writer, plans, replay) == ack
        assert not any(path.exists() for path in evidence)
        assert not writer.writes and not _r33_data_mutations(db, start, plans)
