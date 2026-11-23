"""Unit tests for the stage-B mono-native writer commit path (#2404)."""
import hashlib
from dataclasses import asdict
from dataclasses import replace as dataclasses_replace
from datetime import datetime, timezone
import json
import re

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
        if sql.startswith('MERGE INTO iceberg.ops.transfermarkt_core_pages_v1 '):
            # One statement per page: insert the page, or replace its
            # evidence when the snapshot's clock is not older.
            bound = tuple(params)
            columns = ', '.join(native_writer.PAGES_COLUMNS)
            marks = ', '.join('?' for _ in native_writer.PAGES_COLUMNS)
            cur = self.db.sql.cursor()
            accepted = cur.execute(
                'SELECT captured_at FROM transfermarkt_core_pages_v1 '
                'WHERE target_table = ? AND scope_values_json = ? AND source_url = ?',
                bound[:3]).fetchone()
            if accepted is None:
                cur.execute(
                    f'INSERT INTO transfermarkt_core_pages_v1 ({columns}) VALUES ({marks})', bound)
            elif native_writer._timestamp(bound[4]) >= native_writer._timestamp(accepted[0]):
                cur.execute(
                    'UPDATE transfermarkt_core_pages_v1 SET source_capture_id = ?, '
                    'captured_at = ?, source_body_hash = ? '
                    'WHERE target_table = ? AND scope_values_json = ? AND source_url = ?',
                    (*bound[3:], *bound[:3]))
            self.description = None
            return
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
    def cursor(self):
        return CoreCursor(self)


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
        self.manifest = manifest or []  # accepted page evidence of the scope
        self.fail = fail
        self.description = None
    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError('TABLE_NOT_FOUND: nope')
        self.sql = sql
    def fetchall(self):
        if 'transfermarkt_core_pages_v1' in self.sql:
            return self.manifest
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
            ('source_url', 'u'), ('source_endpoint', 'squad'),
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
    broken._write_to_iceberg = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('crash mid-write'))
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
            ('source_url', 'u'), ('source_endpoint', 'market_value_points'),
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
                ('source_url', 'u'), ('source_endpoint', 'squad'),
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
        ('source_url', 'u'), ('source_endpoint', 'squad'),
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
            ('source_url', 'u'), ('source_endpoint', 'squad'),
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
        def _write_to_iceberg(self, *args, **kwargs):
            raise TimeoutError('insertion timeout')

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
            ('source_url', 'u'), ('source_endpoint', 'market_value_points'),
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


def test_an_ambiguous_receipt_insert_is_resolved_by_reading_the_id_back():
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
                raise RuntimeError('ICEBERG_COMMIT_ERROR: commit state unknown')
            return super().execute(sql, params)

    class AmbiguousDB(CoreDB):
        def cursor(self):
            return AmbiguousCursor(self)

    db = AmbiguousDB()
    receipt = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [membership_plan(['1'], capture='cap-1')], run_id='r',
        universe_version='u', committed_at=CLOCK)
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
                            raise RuntimeError('crash during receipt repair')
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
    write(db, [membership_plan(['2'], capture='cap-b')], run_id='run-2')
    assert _players(db) == ['2']
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [_empty_roster_plan(capture='cap-a')], run_id='run-3')
    assert _players(db) == ['2']
    _warehouse_receipt(db, _empty_roster_plan(capture='cap-x'), 'run-old', CLOCK)
    with pytest.raises(RuntimeError, match='replay differs'):
        write(db, [_empty_roster_plan(clock=LATER, capture='cap-x')], run_id='run-4', clock=LATER)
    assert _receipt_count(db) == 3  # cap-a, cap-b and one cap-x


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


class _LostResponse(CoreWriter):
    """A staged replacement that committed before the connection dropped."""

    def _write_to_iceberg(self, *args, **kwargs):
        super()._write_to_iceberg(*args, **kwargs)
        raise TrinoAmbiguousWriteError('Connection lost while executing SQL')


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


def test_an_uncertain_snapshot_is_replaced_once_more_and_acknowledged():
    # A snapshot's replay finds its rows but not its page manifest, which
    # follows the data: the replacement is issued once more, from a stage built
    # on the rows the scope holds now, and the unit is acknowledged.
    db = CoreDB()
    older = _snapshot(EARLIER, 'cap-old')
    native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [older], run_id='run-a', universe_version='u',
        committed_at=older.captured_at)
    snapshot = _snapshot(CLOCK, 'cap-1')
    with pytest.raises(TrinoAmbiguousWriteError):
        native_writer.write_core_bundle(
            lambda: db, _LostResponse(db), [snapshot], run_id='run-b', universe_version='u',
            committed_at=snapshot.captured_at)
    settled = CoreWriter(db)
    receipt = native_writer.replay_pending_intents(lambda: db, settled)[0]
    assert receipt['verified'] is True
    assert settled.writes == [(snapshot.table, len(snapshot.rows), ())]
    assert native_writer._rows_equal(native_writer._read_plan_rows(db, snapshot), snapshot.rows)
    native_writer._verify_pages(db, snapshot)
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
