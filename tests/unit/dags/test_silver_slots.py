from __future__ import annotations

import re

import pytest

from dags.utils import silver_slots as slots


SPEC = slots.SlotSpec(
    source='fotmob', schema='iceberg.silver', tables=('match', 'lineup'),
)


class FakeCursor:
    """DB-API stand-in that emulates the reader-state row and the CAS UPDATE.

    ``built`` lists existing slot tables (default: every name asked for);
    ``hook(cur, sql)`` runs before each statement and may raise or mutate rows.
    A mutation result must be fetched before the next statement.
    """

    def __init__(self, rows=None, *, missing=False, cas_fails=False, built=None, hook=None):
        self.rows = [list(row) for row in (rows or [])]
        self.missing = missing
        self.cas_fails = cas_fails
        self.built = built
        self.hook = hook
        self.sql: list[str] = []
        self._result: list[tuple] = []
        self._unfetched = False

    def execute(self, sql):
        assert not self._unfetched, 'mutation result was not fetched'
        self.sql.append(sql)
        if self.hook:
            self.hook(self, sql)
        head = sql.split(None, 1)[0].upper()
        if sql.startswith('CREATE TABLE'):
            self.missing = False
        elif self.missing and 'VIEW' not in sql:
            raise RuntimeError('TABLE_NOT_FOUND: iceberg.ops.fotmob_reader_state does not exist')
        if 'information_schema' in sql:
            names = re.findall(r"'([a-z0-9_]+)'", sql.split(' IN ', 1)[1])
            self._result = [(n,) for n in names if self.built is None or n in self.built]
        elif head == 'SELECT':
            self._result = [tuple(row) for row in self.rows]
        elif head == 'UPDATE':
            expected = int(re.search(r'AND revision = (\d+)', sql).group(1))
            slot = re.search(r"SET active_slot = '([ab])'", sql).group(1)
            matched = 0
            for row in self.rows:
                if not self.cas_fails and row[1] == expected and row[0] != slot:
                    row[0], row[1] = slot, row[1] + 1
                    matched += 1
            self._result = [(matched,)]
        elif head == 'MERGE':
            if not self.rows:
                self.rows.append([None, 0, None])
            self._result = [(len(self.rows),)]
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
    return FakeCursor([[None, 0, None]], **kwargs)


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
    assert cur.rows == [['a', 1, None]]


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
    assert cur.rows == [['a', 3, None]]
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
        slots.advance(cur, SPEC, expected_revision=1)
    assert cur.rows == [['b', 2, None]]
    cur.sql.clear()
    result = slots.advance(cur, SPEC, expected_revision=1)
    assert result == {'switched': False, 'from_slot': 'b', 'to_slot': 'b', 'revision': 2}
    assert cur.kinds() == ['SELECT', 'CREATE', 'CREATE', 'SELECT']
    assert cur.sql[1:3] == slots.reader_view_sql_all(SPEC, 'b')


def test_stale_advance_does_not_switch_back():
    cur = FakeCursor([['b', 2, None]])
    assert slots.advance(cur, SPEC, expected_revision=1)['to_slot'] == 'b'
    assert slots.rollback(cur, SPEC, expected_revision=1)['to_slot'] == 'b'
    assert 'UPDATE' not in cur.kinds() and cur.rows == [['b', 2, None]]
    assert slots.switch_reader(cur, SPEC, to_slot='b', expected_revision=1)['switched'] is False
    with pytest.raises(slots.RevisionConflict, match='not a'):
        slots.switch_reader(cur, SPEC, to_slot='a', expected_revision=1)


def test_unbuilt_slot_is_refused_before_update():
    cur = FakeCursor([['a', 1, None]], built={'match_b'})
    with pytest.raises(slots.StateInvariantError, match='slot b is not built: missing lineup_b'):
        slots.rollback(cur, SPEC, expected_revision=1)
    assert cur.kinds() == ['SELECT', 'EXISTS']
    assert cur.rows == [['a', 1, None]]


def test_concurrent_switch_during_views_is_detected():
    def hook(cur, sql):
        if 'VIEW iceberg.silver.lineup' in sql:
            cur.rows[0] = ['a', cur.rows[0][1] + 1, None]
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
SELECT active_slot, revision, updated_at
FROM iceberg.ops.fotmob_reader_state
WHERE source = 'fotmob'
  AND state_key = 'canonical'
""".strip()
    assert slots.slot_tables_exist_sql(SPEC, 'b') == """
SELECT table_name
FROM iceberg.information_schema.tables
WHERE table_schema = 'silver'
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
