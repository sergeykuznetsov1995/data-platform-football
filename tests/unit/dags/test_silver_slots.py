from __future__ import annotations

import re

import pytest

from dags.utils import silver_slots as slots


SPEC = slots.SlotSpec(
    source='fotmob', schema='iceberg.silver', tables=('match', 'lineup'),
)


class FakeCursor:
    """DB-API stand-in that emulates the reader-state row and the CAS UPDATE."""

    def __init__(self, rows=None, *, missing=False, cas_fails=False):
        self.rows = [list(row) for row in (rows or [])]
        self.missing = missing
        self.cas_fails = cas_fails
        self.sql: list[str] = []
        self._result: list[tuple] = []

    def execute(self, sql):
        self.sql.append(sql)
        if self.missing:
            raise RuntimeError('TABLE_NOT_FOUND: iceberg.ops.fotmob_reader_state does not exist')
        head = sql.split(None, 1)[0].upper()
        if head == 'SELECT':
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
        else:
            self._result = [(True,)]

    def fetchall(self):
        result, self._result = self._result, []
        return result

    def kinds(self):
        return [sql.split(None, 1)[0].upper() for sql in self.sql]


def bootstrap_cursor(**kwargs):
    return FakeCursor([[None, 0, None]], **kwargs)


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
    assert cur.kinds() == ['SELECT', 'UPDATE', 'SELECT', 'CREATE', 'CREATE']
    assert cur.sql[3:] == slots.reader_view_sql_all(SPEC, 'a')
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


def test_advance_then_rollback():
    cur = bootstrap_cursor()
    assert slots.advance(cur, SPEC, expected_revision=0)['to_slot'] == 'a'
    second = slots.advance(cur, SPEC, expected_revision=1)
    assert second == {'switched': True, 'from_slot': 'a', 'to_slot': 'b', 'revision': 2}
    back = slots.rollback(cur, SPEC, expected_revision=2)
    assert back == {'switched': True, 'from_slot': 'b', 'to_slot': 'a', 'revision': 3}
    assert cur.rows == [['a', 3, None]]
    assert cur.sql[-2:] == slots.reader_view_sql_all(SPEC, 'a')


def test_retry_onto_active_slot_rebuilds_views_without_update():
    cur = FakeCursor([['a', 1, None]])
    result = slots.switch_reader(cur, SPEC, to_slot='a', expected_revision=1)
    assert result == {'switched': False, 'from_slot': 'a', 'to_slot': 'a', 'revision': 1}
    assert cur.kinds() == ['SELECT', 'CREATE', 'CREATE']
    assert cur.rows == [['a', 1, None]]


def test_empty_table_list_switches_state_only():
    spec = slots.SlotSpec(source='fotmob', schema='iceberg.silver', tables=())
    cur = bootstrap_cursor()
    result = slots.switch_reader(cur, spec, to_slot='b', expected_revision=0)
    assert result['switched'] and result['revision'] == 1
    assert cur.kinds() == ['SELECT', 'UPDATE', 'SELECT']


def test_revision_conflicts():
    cur = FakeCursor([['a', 1, None]])
    with pytest.raises(slots.RevisionConflict):
        slots.switch_reader(cur, SPEC, to_slot='b', expected_revision=0)
    assert cur.kinds() == ['SELECT']
    cur = FakeCursor([['a', 1, None]], cas_fails=True)
    with pytest.raises(slots.RevisionConflict, match='CAS failed'):
        slots.advance(cur, SPEC, expected_revision=1)
    assert cur.kinds() == ['SELECT', 'UPDATE', 'SELECT']


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
