"""Shared A/B slot switching for Silver readers (Transfermarkt, FotMob).

This module deliberately has no Airflow, Trino-client or scraper imports.
Every operation takes a DB-API cursor and emits plain Trino SQL, so DAG tasks,
operator CLIs and host-side unit tests share one contract.

The model is two physical copies of each Silver table (``<table>_a`` and
``<table>_b``) plus one reader-state row per source:

* a new build always targets :func:`inactive_slot`;
* :func:`switch_reader` refuses a slot whose tables are not all built, moves
  the reader with a compare-and-set on ``revision`` and only then re-points
  ``CREATE OR REPLACE VIEW <table>`` at the chosen slot;
* a retry with the same ``expected_revision`` after the CAS landed (state is
  already at ``expected + 1``) skips the UPDATE and rebuilds the views on the
  active slot, so a failed view step is completed, never reversed;
* the state row is created by :func:`bootstrap`, never inferred by a switch.

The reader state is the source of truth; views follow it.  The set is
re-pointed sequentially, so while the ``CREATE OR REPLACE VIEW`` loop runs
(seconds) a reader joining two views may see a mix of slots.  Slot tables are
immutable during the switch, so no data is corrupted.  A concurrent switch
detected after the loop raises :class:`RevisionConflict`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


SLOTS = ('a', 'b')
STATE_KEY = 'canonical'
DEFAULT_ACTOR = 'silver_slots'

_NAME = re.compile(r'[a-z0-9_]+')
_QUALIFIED = re.compile(r'[a-z0-9_.]+')
_CATALOG_SCHEMA = re.compile(r'[a-z0-9_]+\.[a-z0-9_]+')
_MUTATION = re.compile(r'\s*(CREATE|UPDATE|MERGE|INSERT|DELETE|ALTER)\b', re.I)


class RevisionConflict(RuntimeError):
    """A compare-and-set revision no longer matches persisted state."""


class StateInvariantError(RuntimeError):
    """The persisted reader state is missing, ambiguous or malformed."""


@dataclass(frozen=True)
class ReaderState:
    exists: bool
    source: str
    active_slot: str | None = None
    revision: int = 0
    updated_at: Any = None


@dataclass(frozen=True)
class SlotSpec:
    """One source's slotted Silver tables and its reader-state table."""

    source: str
    schema: str
    tables: tuple[str, ...]
    state_table: str = ''

    def __post_init__(self) -> None:
        _check_name(self.source, 'source')
        if not isinstance(self.schema, str) or not _CATALOG_SCHEMA.fullmatch(self.schema):
            raise ValueError(f'schema must be <catalog>.<schema>, got {self.schema!r}')
        if isinstance(self.tables, str):
            raise ValueError('tables must be a sequence of names, not a string')
        tables = tuple(self.tables)
        for table in tables:
            _check_name(table, 'table')
        object.__setattr__(self, 'tables', tables)
        state_table = self.state_table or f'iceberg.ops.{self.source}_reader_state'
        _check_qualified(state_table, 'state_table')
        object.__setattr__(self, 'state_table', state_table)


def _check_name(value: str, what: str) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise ValueError(f'{what} must match [a-z0-9_]+, got {value!r}')
    return value


def _check_qualified(value: str, what: str) -> str:
    if not isinstance(value, str) or not _QUALIFIED.fullmatch(value):
        raise ValueError(f'{what} must match [a-z0-9_.]+, got {value!r}')
    return value


def _sql_literal(value: Any) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_revision(revision: int) -> int:
    try:
        value = int(revision)
    except (TypeError, ValueError) as exc:
        raise ValueError('expected_revision must be an integer') from exc
    if value < 0:
        raise ValueError('expected_revision must be non-negative')
    return value


def _table_missing(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ('table_not_found', 'table not found', 'does not exist')
    )


def _execute(cur, sql: str) -> None:
    """Run one statement; Trino mutations only commit once results are consumed."""
    cur.execute(sql)
    if _MUTATION.match(sql):
        cur.fetchall()


def normalise_slot(slot: str) -> str:
    value = str(slot or '').strip().lower()
    if value not in SLOTS:
        raise ValueError(f'candidate/active slot must be one of {SLOTS}, got {slot!r}')
    return value


def slotted_relation(relation: str, slot: str) -> str:
    return f'{relation}_{normalise_slot(slot)}'


def inactive_slot(state: Any) -> str:
    """Return the only slot a new build may target.

    Duck-typed on ``.active_slot`` so any reader-state shape works.  A
    never-switched source deterministically starts with slot A.
    """
    return 'b' if state.active_slot == 'a' else 'a'


def other_slot(slot: str) -> str:
    return 'b' if normalise_slot(slot) == 'a' else 'a'


def state_table_ddl(spec: SlotSpec) -> list[str]:
    """Idempotent reader-state DDL plus a never-switched singleton seed."""
    return [
        f"""
CREATE TABLE IF NOT EXISTS {spec.state_table} (
    source varchar,
    state_key varchar,
    active_slot varchar,
    revision bigint,
    updated_at timestamp(6),
    updated_by varchar
) WITH (format = 'PARQUET')
""".strip(),
        f"""
MERGE INTO {spec.state_table} t
USING (VALUES ({_sql_literal(spec.source)}, '{STATE_KEY}')) s(source, state_key)
ON t.source = s.source AND t.state_key = s.state_key
WHEN NOT MATCHED THEN INSERT (
    source, state_key, active_slot, revision, updated_at, updated_by
) VALUES (
    s.source, s.state_key, CAST(NULL AS varchar), CAST(0 AS bigint),
    CURRENT_TIMESTAMP, 'bootstrap'
)
""".strip(),
    ]


def bootstrap(cur, spec: SlotSpec) -> None:
    """Create the state table and seed its singleton (idempotent)."""
    for sql in state_table_ddl(spec):
        _execute(cur, sql)


def read_state_sql(spec: SlotSpec) -> str:
    return f"""
SELECT active_slot, revision, updated_at
FROM {spec.state_table}
WHERE source = {_sql_literal(spec.source)}
  AND state_key = '{STATE_KEY}'
""".strip()


def switch_sql(
    spec: SlotSpec,
    *,
    to_slot: str,
    expected_revision: int,
    actor: str = DEFAULT_ACTOR,
) -> str:
    slot = _sql_literal(normalise_slot(to_slot))
    return f"""
UPDATE {spec.state_table}
SET active_slot = {slot},
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE source = {_sql_literal(spec.source)}
  AND state_key = '{STATE_KEY}'
  AND revision = {_normalise_revision(expected_revision)}
  AND active_slot IS DISTINCT FROM {slot}
""".strip()


def slot_tables_exist_sql(spec: SlotSpec, slot: str) -> str:
    catalog, schema = spec.schema.split('.')
    names = ', '.join(
        _sql_literal(slotted_relation(table, slot)) for table in spec.tables
    )
    return f"""
SELECT table_name
FROM {catalog}.information_schema.tables
WHERE table_schema = {_sql_literal(schema)}
  AND table_name IN ({names})
""".strip()


def reader_view_sql(spec: SlotSpec, table: str, slot: str) -> str:
    """Pin the stable reader view to one physical slot."""
    view = f'{spec.schema}.{_check_name(table, "table")}'
    return f"""
CREATE OR REPLACE VIEW {view} AS
SELECT * FROM {slotted_relation(view, slot)}
""".strip()


def reader_view_sql_all(spec: SlotSpec, slot: str) -> list[str]:
    return [reader_view_sql(spec, table, slot) for table in spec.tables]


def read_reader_state(cur, spec: SlotSpec, *, allow_missing: bool = True) -> ReaderState:
    try:
        _execute(cur, read_state_sql(spec))
        rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001 - connector exception hierarchy varies
        if allow_missing and _table_missing(exc):
            return ReaderState(exists=False, source=spec.source)
        raise
    if not rows:
        if allow_missing:
            return ReaderState(exists=False, source=spec.source)
        raise StateInvariantError(f'{spec.source} reader singleton is missing')
    if len(rows) != 1:
        raise StateInvariantError(
            f'{spec.source} reader state is not a singleton: rows={len(rows)}'
        )
    active_slot, revision, updated_at = rows[0]
    if active_slot is not None and active_slot not in SLOTS:
        raise StateInvariantError(f'{spec.source} active_slot is malformed: {active_slot!r}')
    if revision is None or int(revision) < 0:
        raise StateInvariantError(f'{spec.source} revision is malformed: {revision!r}')
    return ReaderState(
        exists=True, source=spec.source, active_slot=active_slot,
        revision=int(revision), updated_at=updated_at,
    )


def assert_reader_revision(cur, spec: SlotSpec, expected_revision: int) -> ReaderState:
    expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, spec, allow_missing=(expected == 0))
    if state.revision != expected:
        raise RevisionConflict(
            f'{spec.source} reader revision drift: expected={expected}, '
            f'actual={state.revision}'
        )
    return state


def _read_for_switch(cur, spec: SlotSpec, expected_revision: int) -> tuple[ReaderState, bool]:
    """Return the state and whether this caller's CAS has already landed."""
    expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, spec, allow_missing=(expected == 0))
    if state.revision == expected + 1 and state.active_slot in SLOTS:
        return state, True
    if state.revision != expected:
        raise RevisionConflict(
            f'{spec.source} reader revision drift: expected={expected}, '
            f'actual={state.revision}'
        )
    return state, False


def _missing_slot_tables(cur, spec: SlotSpec, slot: str) -> list[str]:
    if not spec.tables:
        return []
    _execute(cur, slot_tables_exist_sql(spec, slot))
    built = {str(row[0]) for row in cur.fetchall()}
    return [
        name for name in (slotted_relation(table, slot) for table in spec.tables)
        if name not in built
    ]


def _switch_from(
    cur, spec: SlotSpec, state: ReaderState, slot: str, *, applied: bool, actor: str,
) -> dict[str, Any]:
    switched = not applied and state.active_slot != slot
    revision = state.revision
    if switched:
        missing = _missing_slot_tables(cur, spec, slot)
        if missing:
            raise StateInvariantError(
                f'slot {slot} is not built: missing {", ".join(missing)}'
            )
        _execute(cur, switch_sql(
            spec, to_slot=slot, expected_revision=state.revision, actor=actor,
        ))
        # Trino UPDATE row counts are not reliable: confirm the CAS by reading.
        after = read_reader_state(cur, spec)
        if after.revision != state.revision + 1 or after.active_slot != slot:
            raise RevisionConflict(
                f'{spec.source} reader CAS failed: expected revision='
                f'{state.revision + 1} slot={slot}, actual revision='
                f'{after.revision} slot={after.active_slot}'
            )
        revision = after.revision
    for sql in reader_view_sql_all(spec, slot):
        _execute(cur, sql)
    final = read_reader_state(cur, spec)
    if final.revision != revision:
        raise RevisionConflict(
            f'{spec.source} reader moved during the view rebuild: expected '
            f'revision={revision}, actual={final.revision}; views may be stale'
        )
    return {
        'switched': switched,
        'from_slot': state.active_slot,
        'to_slot': slot,
        'revision': revision,
    }


def switch_reader(
    cur,
    spec: SlotSpec,
    *,
    to_slot: str,
    expected_revision: int,
    actor: str = DEFAULT_ACTOR,
) -> dict[str, Any]:
    """CAS the reader state onto ``to_slot``, then re-point every view.

    Re-switching onto the active slot, or retrying after this CAS already
    landed, skips the UPDATE but still rebuilds the views.
    """
    slot = normalise_slot(to_slot)
    state, applied = _read_for_switch(cur, spec, expected_revision)
    if applied and state.active_slot != slot:
        raise RevisionConflict(
            f'{spec.source} reader moved to slot {state.active_slot}, not {slot}'
        )
    return _switch_from(cur, spec, state, slot, applied=applied, actor=actor)


def advance(cur, spec: SlotSpec, *, expected_revision: int) -> dict[str, Any]:
    """Promote a finished build in the inactive slot."""
    state, applied = _read_for_switch(cur, spec, expected_revision)
    slot = state.active_slot if applied else inactive_slot(state)
    return _switch_from(cur, spec, state, slot, applied=applied, actor=DEFAULT_ACTOR)


def rollback(cur, spec: SlotSpec, *, expected_revision: int) -> dict[str, Any]:
    """Return readers to the previously active slot."""
    state, applied = _read_for_switch(cur, spec, expected_revision)
    if state.active_slot is None:
        raise StateInvariantError(f'{spec.source}: nothing to roll back')
    slot = state.active_slot if applied else other_slot(state.active_slot)
    return _switch_from(cur, spec, state, slot, applied=applied, actor=DEFAULT_ACTOR)
