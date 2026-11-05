from __future__ import annotations

import re
import sqlite3

import pytest

from dags.utils import silver_slots as slots


SPEC = slots.SlotSpec(
    source='fotmob', schema='iceberg.silver', tables=('match', 'lineup'),
)


class FakeCursor:
    """DB-API stand-in that emulates the reader-state row and the CAS UPDATE.

    A state row is ``[active_slot, revision, updated_by]``.

    ``built`` lists existing slot tables (default: every name asked for);
    ``hook(cur, sql)`` runs before each statement and may raise or mutate rows.
    A mutation result must be fetched before the next statement.
    """

    def __init__(self, rows=None, *, missing=False, cas_fails=False, built=None, hook=None,
                 views=None, columns=None):
        self.rows = [list(row) for row in (rows or [])]
        self.missing = missing
        self.cas_fails = cas_fails
        self.built = built
        self.hook = hook
        self.views = dict(views or {})
        self.columns = columns
        self.sql: list[str] = []
        self._result: list[tuple] = []
        self._unfetched = False

    def execute(self, sql):
        assert not self._unfetched, 'mutation result was not fetched'
        self.sql.append(sql)
        if self.hook:
            self.hook(self, sql)
        if 'system.runtime.nodes' in sql:
            self._result = [(1,)]
            return
        if sql == 'SELECT to_iso8601(current_timestamp)':
            self._result = [('2026-11-05T13:00:00Z',)]
            return
        if 'system.runtime.transactions' in sql:
            self._result = []
            return
        head = sql.split(None, 1)[0].upper()
        if sql.startswith('CREATE TABLE'):
            self.missing = False
        elif self.missing and 'VIEW' not in sql:
            raise RuntimeError('TABLE_NOT_FOUND: iceberg.ops.fotmob_reader_state does not exist')
        if 'information_schema.views' in sql:
            name = re.search(r"table_name = '([^']*)'", sql).group(1)
            self._result = [(self.views[name],)] if name in self.views else []
        elif 'information_schema.columns' in sql:
            names = re.findall(r"'([a-z0-9_]+)'", sql.split(' IN ', 1)[1])
            self._result = [
                (name, col, dtype, pos) for name in names
                for col, dtype, pos in (
                    self.columns.get(name, []) if self.columns is not None
                    else [('id', 'bigint', 1), ('label', 'varchar', 2)]
                )
            ]
        elif 'information_schema' in sql:
            names = re.findall(r"'([a-z0-9_]+)'", sql.split(' IN ', 1)[1])
            self._result = [(n,) for n in names if self.built is None or n in self.built]
        elif head == 'SELECT':
            self._result = [(slot, rev, None, by) for slot, rev, by in self.rows]
        elif head == 'UPDATE':
            expected = int(re.search(r'AND revision = (\d+)', sql).group(1))
            slot = re.search(r"SET active_slot = '([ab])'", sql).group(1)
            actor = re.search(r"updated_by = '([^']*)'", sql).group(1)
            matched = 0
            for row in self.rows:
                if not self.cas_fails and row[1] == expected and row[0] != slot:
                    row[:] = [slot, row[1] + 1, actor]
                    matched += 1
            self._result = [(matched,)]
        elif head == 'MERGE':
            if not self.rows:
                self.rows.append([None, 0, 'bootstrap'])
            self._result = [(len(self.rows),)]
        elif sql.startswith('CREATE OR REPLACE VIEW'):
            view, query = sql.split(' AS\n', 1)
            self.views[view.rsplit('.', 1)[1]] = query
            self._result = [(True,)]
        else:
            self._result = [(True,)]
        self._unfetched = head in {'UPDATE', 'CREATE', 'MERGE'}

    def fetchall(self):
        result, self._result = self._result, []
        self._unfetched = False
        return result

    def kinds(self):
        return [
            'EXISTS' if 'information_schema' in sql else sql.split(None, 1)[0].upper()
            for sql in self.sql
        ]


def bootstrap_cursor(**kwargs):
    return FakeCursor([[None, 0, 'bootstrap']], **kwargs)


def fail_once(fragment):
    fired = []

    def hook(cur, sql):
        if fragment in sql and not fired:
            fired.append(sql)
            raise RuntimeError(f'boom: {fragment}')
    return hook


SWITCH = ['SELECT', 'EXISTS', 'UPDATE', 'SELECT', 'CREATE', 'CREATE', 'SELECT']


def test_slotted_relation():
    assert slots.slotted_relation('iceberg.silver.x', 'a') == 'iceberg.silver.x_a'
    assert slots.slotted_relation('iceberg.silver.x', ' B ') == 'iceberg.silver.x_b'
    with pytest.raises(ValueError):
        slots.slotted_relation('iceberg.silver.x', 'c')


def test_inactive_and_other_slot_accept_tm_reader_state():
    from dags.utils import transfermarkt_native_v2 as tm_v2

    assert slots.inactive_slot(slots.ReaderState(exists=False, source='x')) == 'a'
    assert slots.inactive_slot(slots.ReaderState(exists=True, source='x', active_slot='a')) == 'b'
    assert slots.inactive_slot(slots.ReaderState(exists=True, source='x', active_slot='b')) == 'a'
    assert slots.inactive_slot(tm_v2.ReaderState(exists=True, active_slot='a')) == 'b'
    assert slots.other_slot('a') == 'b' and slots.other_slot('b') == 'a'
    with pytest.raises(ValueError):
        slots.other_slot(None)


def test_first_switch_from_bootstrap_row():
    cur = bootstrap_cursor()
    result = slots.advance(cur, SPEC, expected_revision=0)
    assert result == {'switched': True, 'from_slot': None, 'to_slot': 'a', 'revision': 1}
    assert cur.kinds() == SWITCH
    assert cur.sql[1] == slots.slot_tables_exist_sql(SPEC, 'a')
    assert cur.sql[4:6] == slots.reader_view_sql_all(SPEC, 'a')
    assert cur.rows == [['a', 1, 'silver_slots']]


def test_missing_or_empty_state_table():
    state = slots.read_reader_state(FakeCursor(missing=True), SPEC)
    assert state == slots.ReaderState(exists=False, source='fotmob', revision=0)
    assert not slots.read_reader_state(FakeCursor(), SPEC).exists
    with pytest.raises(RuntimeError, match='TABLE_NOT_FOUND'):
        slots.read_reader_state(FakeCursor(missing=True), SPEC, allow_missing=False)
    with pytest.raises(slots.StateInvariantError):
        slots.read_reader_state(FakeCursor(), SPEC, allow_missing=False)
    # No bootstrap row: the CAS UPDATE matches nothing and the re-read rejects it.
    cur = FakeCursor()
    with pytest.raises(slots.RevisionConflict):
        slots.advance(cur, SPEC, expected_revision=0)
    assert 'CREATE' not in cur.kinds()


def test_bootstrap_then_first_switch():
    cur = FakeCursor(missing=True)
    slots.bootstrap(cur, SPEC)
    assert cur.sql == slots.state_table_ddl(SPEC)
    assert slots.advance(cur, SPEC, expected_revision=0)['revision'] == 1


def test_advance_then_rollback():
    cur = bootstrap_cursor()
    assert slots.advance(cur, SPEC, expected_revision=0)['to_slot'] == 'a'
    second = slots.advance(cur, SPEC, expected_revision=1)
    assert second == {'switched': True, 'from_slot': 'a', 'to_slot': 'b', 'revision': 2}
    back = slots.rollback(cur, SPEC, expected_revision=2)
    assert back == {'switched': True, 'from_slot': 'b', 'to_slot': 'a', 'revision': 3}
    assert cur.rows == [['a', 3, 'silver_slots']]
    assert cur.sql[-3:-1] == slots.reader_view_sql_all(SPEC, 'a')


def test_retry_onto_active_slot_rebuilds_views_without_update():
    cur = FakeCursor([['a', 1, None]])
    result = slots.switch_reader(cur, SPEC, to_slot='a', expected_revision=1)
    assert result == {'switched': False, 'from_slot': 'a', 'to_slot': 'a', 'revision': 1}
    assert cur.kinds() == ['SELECT', 'CREATE', 'CREATE', 'SELECT']
    assert cur.rows == [['a', 1, None]]


def test_retry_after_failed_view_completes_the_switch():
    cur = FakeCursor([['a', 1, None]], hook=fail_once('VIEW iceberg.silver.lineup'))
    with pytest.raises(RuntimeError, match='boom'):
        slots.advance(cur, SPEC, expected_revision=1, actor='run-1')
    assert cur.rows == [['b', 2, 'run-1']]
    cur.sql.clear()
    result = slots.advance(cur, SPEC, expected_revision=1, actor='run-1')
    assert result == {'switched': False, 'from_slot': 'a', 'to_slot': 'b', 'revision': 2}
    assert cur.kinds() == ['SELECT', 'CREATE', 'CREATE', 'SELECT']
    assert cur.sql[1:3] == slots.reader_view_sql_all(SPEC, 'b')


def test_own_retry_does_not_switch_back():
    cur = FakeCursor([['b', 2, 'run-1']])
    assert slots.advance(cur, SPEC, expected_revision=1, actor='run-1')['to_slot'] == 'b'
    assert slots.rollback(cur, SPEC, expected_revision=1, actor='run-1')['to_slot'] == 'b'
    assert 'UPDATE' not in cur.kinds() and cur.rows == [['b', 2, 'run-1']]
    retry = slots.switch_reader(cur, SPEC, to_slot='b', expected_revision=1, actor='run-1')
    assert retry['switched'] is False
    with pytest.raises(slots.RevisionConflict, match='not a'):
        slots.switch_reader(cur, SPEC, to_slot='a', expected_revision=1, actor='run-1')


def test_foreign_switch_at_next_revision_is_a_conflict():
    # (a) someone else promoted b from bootstrap while our build went to a.
    cur = bootstrap_cursor()
    slots.switch_reader(cur, SPEC, to_slot='b', expected_revision=0, actor='other')
    with pytest.raises(slots.RevisionConflict, match="expected=0, actual=1 switched by 'other'"):
        slots.advance(cur, SPEC, expected_revision=0, actor='run-1')
    # (b) a rollback from bootstrap cannot have landed: rev 1 is someone else's.
    cur = FakeCursor([['a', 1, 'other']])
    with pytest.raises(slots.RevisionConflict, match="switched by 'other'"):
        slots.rollback(cur, SPEC, expected_revision=0)
    assert cur.kinds() == ['SELECT'] and cur.rows == [['a', 1, 'other']]
    # (c) same direction, different run: still a conflict, not a silent retry.
    cur = FakeCursor([['b', 2, 'run-1']])
    with pytest.raises(slots.RevisionConflict, match="not 'run-2'"):
        slots.advance(cur, SPEC, expected_revision=1, actor='run-2')


def test_assert_reader_revision_is_strict():
    cur = FakeCursor([['b', 2, 'run-1']])
    state = slots.assert_reader_revision(cur, SPEC, 2)
    assert (state.active_slot, state.revision, state.updated_by) == ('b', 2, 'run-1')
    with pytest.raises(slots.RevisionConflict):
        slots.assert_reader_revision(cur, SPEC, 1)


def test_unbuilt_slot_is_refused_before_update():
    cur = FakeCursor([['a', 1, None]], built={'match_b'})
    with pytest.raises(slots.StateInvariantError, match='slot b is not built: missing lineup_b'):
        slots.rollback(cur, SPEC, expected_revision=1)
    assert cur.kinds() == ['SELECT', 'EXISTS']
    assert cur.rows == [['a', 1, None]]


def test_concurrent_switch_during_views_is_detected():
    def hook(cur, sql):
        if 'VIEW iceberg.silver.lineup' in sql:
            cur.rows[0] = ['a', cur.rows[0][1] + 1, 'other']
    cur = FakeCursor([['a', 1, None]], hook=hook)
    with pytest.raises(slots.RevisionConflict, match='moved during the view rebuild'):
        slots.advance(cur, SPEC, expected_revision=1)


def test_empty_table_list_switches_state_only():
    spec = slots.SlotSpec(source='fotmob', schema='iceberg.silver', tables=())
    cur = bootstrap_cursor()
    result = slots.switch_reader(cur, spec, to_slot='b', expected_revision=0)
    assert result['switched'] and result['revision'] == 1
    assert cur.kinds() == ['SELECT', 'UPDATE', 'SELECT', 'SELECT']


def test_revision_conflicts():
    cur = FakeCursor([['a', 1, None]])
    with pytest.raises(slots.RevisionConflict):
        slots.switch_reader(cur, SPEC, to_slot='b', expected_revision=0)
    assert cur.kinds() == ['SELECT']
    cur = FakeCursor([['a', 1, None]], cas_fails=True)
    with pytest.raises(slots.RevisionConflict, match='CAS failed'):
        slots.advance(cur, SPEC, expected_revision=1)
    assert cur.kinds() == ['SELECT', 'EXISTS', 'UPDATE', 'SELECT']


def test_exact_sql():
    assert SPEC.state_table == 'iceberg.ops.fotmob_reader_state'
    assert slots.reader_view_sql(SPEC, 'match', 'b') == """
CREATE OR REPLACE VIEW iceberg.silver.match AS
SELECT * FROM iceberg.silver.match_b
""".strip()
    assert slots.switch_sql(SPEC, to_slot='a', expected_revision=4) == """
UPDATE iceberg.ops.fotmob_reader_state
SET active_slot = 'a',
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = 'silver_slots'
WHERE source = 'fotmob'
  AND state_key = 'canonical'
  AND revision = 4
  AND active_slot IS DISTINCT FROM 'a'
""".strip()
    assert slots.read_state_sql(SPEC) == """
SELECT active_slot, revision, updated_at, updated_by
FROM iceberg.ops.fotmob_reader_state
WHERE source = 'fotmob'
  AND state_key = 'canonical'
""".strip()
    assert slots.slot_tables_exist_sql(SPEC, 'b') == """
SELECT table_name
FROM iceberg.information_schema.tables
WHERE table_schema = 'silver'
  AND table_type = 'BASE TABLE'
  AND table_name IN ('match_b', 'lineup_b')
""".strip()
    assert slots.state_table_ddl(SPEC) == [
        """
CREATE TABLE IF NOT EXISTS iceberg.ops.fotmob_reader_state (
    source varchar,
    state_key varchar,
    active_slot varchar,
    revision bigint,
    updated_at timestamp(6),
    updated_by varchar
) WITH (format = 'PARQUET')
""".strip(),
        """
MERGE INTO iceberg.ops.fotmob_reader_state t
USING (VALUES ('fotmob', 'canonical')) s(source, state_key)
ON t.source = s.source AND t.state_key = s.state_key
WHEN NOT MATCHED THEN INSERT (
    source, state_key, active_slot, revision, updated_at, updated_by
) VALUES (
    s.source, s.state_key, CAST(NULL AS varchar), CAST(0 AS bigint),
    CURRENT_TIMESTAMP, 'bootstrap'
)
""".strip(),
    ]


@pytest.mark.parametrize('kwargs', [
    {'tables': ('match; DROP TABLE x',)},
    {'tables': ("match'",)},
    {'source': 'Fot-Mob'},
    {'schema': 'iceberg.silver;'},
    {'schema': 'silver'},
    {'schema': 'iceberg.silver.extra'},
    {'tables': 'match'},
    {'state_table': "iceberg.ops.x'"},
])
def test_spec_rejects_unsafe_names(kwargs):
    base = {'source': 'fotmob', 'schema': 'iceberg.silver', 'tables': ('match',)}
    with pytest.raises(ValueError):
        slots.SlotSpec(**{**base, **kwargs})


def test_state_invariants():
    with pytest.raises(ValueError):
        slots.reader_view_sql(SPEC, "match'", 'a')
    with pytest.raises(slots.StateInvariantError, match='singleton'):
        slots.read_reader_state(FakeCursor([[None, 0, None], ['a', 1, None]]), SPEC)
    with pytest.raises(slots.StateInvariantError, match='active_slot'):
        slots.read_reader_state(FakeCursor([['c', 0, None]]), SPEC)
    cur = bootstrap_cursor()
    with pytest.raises(slots.StateInvariantError, match='nothing to roll back'):
        slots.rollback(cur, SPEC, expected_revision=0)
    assert cur.kinds() == ['SELECT']


def test_transfermarkt_delegates_to_shared_module():
    from dags.utils import transfermarkt_native_v2 as tm_v2

    for slot in ('a', 'B'):
        assert tm_v2.slotted_relation('iceberg.silver.x', slot) == (
            slots.slotted_relation('iceberg.silver.x', slot)
        )
    with pytest.raises(ValueError):
        tm_v2.slotted_relation('iceberg.silver.x', 'c')
    for active in (None, 'a', 'b'):
        state = tm_v2.ReaderState(exists=True, active_slot=active)
        assert tm_v2.inactive_slot(state) == slots.inactive_slot(state)


STATE_SPEC = slots.SlotSpec(
    source='fotmob', schema='iceberg.silver', tables=('match', 'lineup'), state_selected=True,
)


def legacy_views(slot='a'):
    return {name: f'SELECT * FROM iceberg.silver.{name}_{slot}' for name in STATE_SPEC.tables}


def test_state_selected_sql_uses_singleton_selector_for_both_slots():
    sql = slots.reader_view_sql(STATE_SPEC, 'match', 'a')
    assert sql == slots.reader_view_sql(STATE_SPEC, 'match', 'b')
    assert sql.count('CASE WHEN (count(*) = 1) THEN max(active_slot) END') == 2
    assert sql.count("(source = 'fotmob') AND (state_key = 'canonical')") == 2
    assert 'UNION ALL' in sql
    assert 'FROM iceberg.silver.match_a' in sql and 'FROM iceberg.silver.match_b' in sql


@pytest.mark.parametrize(('active_rows', 'expected'), [
    ([], []), ([(None,)], []), ([('x',)], []),
    ([('a',)], [('old',)]), ([('b',)], [('new',)]),
    ([('a',), ('a',)], []), ([('a',), ('b',)], []),
])
def test_selector_returns_only_a_valid_singleton(active_rows, expected):
    # SQLite executes this portable SELECT shape to verify predicate semantics;
    # it does not establish Trino's cross-table snapshot/planning guarantees.
    query = slots.reader_view_sql(STATE_SPEC, 'match', 'a').split(' AS\n', 1)[1]
    query = query.replace('iceberg.silver.', '').replace('iceberg.ops.', '')
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE match_a (value TEXT)')
        db.execute('CREATE TABLE match_b (value TEXT)')
        db.execute("INSERT INTO match_a VALUES ('old')")
        db.execute("INSERT INTO match_b VALUES ('new')")
        db.execute('CREATE TABLE fotmob_reader_state (source TEXT, state_key TEXT, active_slot TEXT)')
        db.executemany("INSERT INTO fotmob_reader_state VALUES ('fotmob', 'canonical', ?)", active_rows)
        db.execute("INSERT INTO fotmob_reader_state VALUES ('other', 'canonical', 'a')")
        db.execute("INSERT INTO fotmob_reader_state VALUES ('fotmob', 'other', 'a')")
        assert db.execute(query).fetchall() == expected


def test_state_selected_bootstrap_prepares_views_before_cas_and_retry_has_no_ddl():
    cur = bootstrap_cursor()
    result = slots.advance(cur, STATE_SPEC, expected_revision=0, actor='first')
    assert result['to_slot'] == 'a' and result['revision'] == 1
    update = cur.kinds().index('UPDATE')
    assert cur.kinds()[:update].count('CREATE') == 2
    assert 'CREATE' not in cur.kinds()[update:]
    cur.sql.clear()
    assert not slots.advance(cur, STATE_SPEC, expected_revision=0, actor='first')['switched']
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()
    with pytest.raises(slots.RevisionConflict, match="not 'other'"):
        slots.advance(cur, STATE_SPEC, expected_revision=0, actor='other')


def test_state_selected_migrates_current_static_views_before_cas():
    cur = FakeCursor([['a', 1, None]], views=legacy_views())
    slots.advance(cur, STATE_SPEC, expected_revision=1, actor='next')
    update = cur.kinds().index('UPDATE')
    assert cur.kinds()[:update].count('CREATE') == 2
    assert 'CREATE' not in cur.kinds()[update:]
    assert cur.rows == [['b', 2, 'next']]


def test_preparation_failure_keeps_old_state_and_partial_migration_is_retryable():
    cur = FakeCursor([['a', 1, None]], views=legacy_views(),
                     hook=fail_once('CREATE OR REPLACE VIEW iceberg.silver.lineup'))
    with pytest.raises(RuntimeError, match='boom'):
        slots.advance(cur, STATE_SPEC, expected_revision=1, actor='next')
    assert cur.rows == [['a', 1, None]]
    assert 'UPDATE' not in cur.kinds()
    assert 'UNION ALL' in cur.views['match']
    assert cur.views['lineup'].endswith('lineup_a')
    cur.sql.clear()
    slots.advance(cur, STATE_SPEC, expected_revision=1, actor='next')
    assert cur.kinds().count('CREATE') == 1
    assert cur.rows == [['b', 2, 'next']]


def test_failed_response_after_state_selected_cas_is_retryable_without_ddl():
    fired = []

    def hook(cur, sql):
        if sql == slots.read_state_sql(STATE_SPEC) and cur.rows[0][1] == 2 and not fired:
            fired.append(True)
            raise RuntimeError('lost confirmation')

    cur = FakeCursor([['a', 1, None]], views=legacy_views(), hook=hook)
    with pytest.raises(RuntimeError, match='lost confirmation'):
        slots.advance(cur, STATE_SPEC, expected_revision=1, actor='next')
    assert cur.rows == [['b', 2, 'next']]
    cur.sql.clear()
    slots.advance(cur, STATE_SPEC, expected_revision=1, actor='next')
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()


@pytest.mark.parametrize('unsafe', [
    None,
    'SELECT * FROM iceberg.silver.lineup_b',
    'SELECT * FROM iceberg.silver.lineup_a WHERE id > 0',
    "SELECT * FROM iceberg.silver.lineup_a WHERE 'A' = 'a'",
])
def test_unsafe_or_missing_existing_view_fails_before_any_ddl(unsafe):
    views = legacy_views()
    if unsafe is None:
        views.pop('lineup')
    else:
        views['lineup'] = unsafe
    cur = FakeCursor([['a', 1, None]], views=views)
    with pytest.raises(slots.StateInvariantError, match='unsafe reader view'):
        slots.advance(cur, STATE_SPEC, expected_revision=1)
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()
    assert cur.rows == [['a', 1, None]]


@pytest.mark.parametrize('missing', ['match_a', 'lineup_b'])
def test_state_selected_requires_both_physical_slots(missing):
    built = {f'{name}_{slot}' for name in STATE_SPEC.tables for slot in slots.SLOTS} - {missing}
    cur = FakeCursor([['a', 1, None]], views=legacy_views(), built=built)
    with pytest.raises(slots.StateInvariantError, match='is not built'):
        slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=1)
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()


@pytest.mark.parametrize('bad_columns', [
    [], [('other', 'bigint', 1)], [('id', 'varchar', 1)],
    [('label', 'varchar', 1), ('id', 'bigint', 2)],
])
def test_state_selected_checks_column_names_types_and_order(bad_columns):
    columns = {f'{name}_{slot}': [('id', 'bigint', 1), ('label', 'varchar', 2)]
               for name in STATE_SPEC.tables for slot in slots.SLOTS}
    columns['lineup_b'] = bad_columns
    cur = FakeCursor([['a', 1, None]], views=legacy_views(), columns=columns)
    with pytest.raises(slots.StateInvariantError, match='physical slot columns'):
        slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=1)
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()


def test_prepare_is_idempotent_and_revision_is_strict():
    cur = bootstrap_cursor()
    state = slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=0)
    assert state.active_slot is None and cur.rows == [[None, 0, 'bootstrap']]
    cur.sql.clear()
    slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=0)
    assert 'CREATE' not in cur.kinds() and 'UPDATE' not in cur.kinds()
    with pytest.raises(slots.RevisionConflict):
        slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=1)
    with pytest.raises(ValueError, match='state_selected'):
        slots.prepare_reader_views(cur, SPEC, expected_revision=0)
    with pytest.raises(slots.StateInvariantError, match='singleton'):
        slots.prepare_reader_views(FakeCursor(), STATE_SPEC, expected_revision=0)


def test_view_normalization_preserves_literals_and_accepts_identifier_quotes():
    assert slots._normalise_view_sql('select * from "iceberg"."silver"."match_a"') == (
        slots._normalise_view_sql('SELECT * FROM iceberg.silver.match_a')
    )
    assert slots._normalise_view_sql("SELECT 'A'") != slots._normalise_view_sql("SELECT 'a'")


def test_preparation_accepts_actual_trino_formatted_view_definition():
    # Captured from Trino's information_schema.views in the isolated #2020
    # probe on 2026-11-05; only schema/source/table names are replaced here.
    template = """SELECT *
FROM
  iceberg.silver.{name}_a
WHERE ((SELECT (CASE WHEN (count(*) = 1) THEN max(active_slot) END)
FROM
  iceberg.ops.fotmob_reader_state
WHERE ((source = 'fotmob') AND (state_key = 'canonical'))
) = 'a')
UNION ALL SELECT *
FROM
  iceberg.silver.{name}_b
WHERE ((SELECT (CASE WHEN (count(*) = 1) THEN max(active_slot) END)
FROM
  iceberg.ops.fotmob_reader_state
WHERE ((source = 'fotmob') AND (state_key = 'canonical'))
) = 'b')
"""
    views = {name: template.format(name=name) for name in STATE_SPEC.tables}
    cur = FakeCursor([['a', 1, None]], views=views)
    slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=1)
    assert 'CREATE' not in cur.kinds()
    # Do not erase structural parentheses or accept a changed selector.
    cur.views['lineup'] = cur.views['lineup'].replace('count(*) = 1', 'count(*) >= 1')
    with pytest.raises(slots.StateInvariantError, match='unsafe reader view'):
        slots.prepare_reader_views(cur, STATE_SPEC, expected_revision=1)
