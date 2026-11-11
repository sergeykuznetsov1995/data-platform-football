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
  active slot, so a failed view step is completed, never reversed.  The retry
  is honoured only when the landed CAS carries the caller's ``actor``
  (``updated_by``): pass a unique ``actor`` per run (e.g. the Airflow run_id)
  for safe retries; with a shared actor a retry is indistinguishable from
  someone else's switch;
* the state row is created by :func:`bootstrap`, never inferred by a switch.

By default views are re-pointed sequentially; a reader joining two views may
see a mix of slots during that loop. With ``SlotSpec.state_selected=True``,
permanent views select A/B through the state row. They are prepared before
the CAS and never rebuilt after it. Both physical slots must keep compatible
schemas. This alone does not synchronize snapshots of the physical tables:
callers also need to serialize builds and publication against reader queries.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .silver_reader_barrier import drain_readers


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
    updated_by: str | None = None


@dataclass(frozen=True)
class SlotSpec:
    """One source's slotted Silver tables and its reader-state table."""

    source: str
    schema: str
    tables: tuple[str, ...]
    state_table: str = ''
    state_selected: bool = False

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
SELECT active_slot, revision, updated_at, updated_by
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
  AND table_type = 'BASE TABLE'
  AND table_name IN ({names})
""".strip()


def reader_view_sql(spec: SlotSpec, table: str, slot: str) -> str:
    """Render a static view, or a permanent state-selected view when opted in."""
    normalise_slot(slot)
    view = f'{spec.schema}.{_check_name(table, "table")}'
    if spec.state_selected:
        # Match Trino's persisted expression parentheses so validation needs
        # only token/whitespace normalization, never expression rewriting.
        selector = f"""SELECT (CASE WHEN (count(*) = 1) THEN max(active_slot) END)
FROM {spec.state_table}
WHERE ((source = {_sql_literal(spec.source)}) AND (state_key = '{STATE_KEY}'))"""
        branches = [
            f"SELECT * FROM {slotted_relation(view, candidate)}\n"
            f"WHERE (({selector}) = '{candidate}')"
            for candidate in SLOTS
        ]
        return f"CREATE OR REPLACE VIEW {view} AS\n" + '\nUNION ALL\n'.join(branches)
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
    active_slot, revision, updated_at, updated_by = rows[0]
    if active_slot is not None and active_slot not in SLOTS:
        raise StateInvariantError(f'{spec.source} active_slot is malformed: {active_slot!r}')
    if revision is None or int(revision) < 0:
        raise StateInvariantError(f'{spec.source} revision is malformed: {revision!r}')
    return ReaderState(
        exists=True, source=spec.source, active_slot=active_slot,
        revision=int(revision), updated_at=updated_at,
        updated_by=(str(updated_by) if updated_by is not None else None),
    )


def assert_reader_revision(cur, spec: SlotSpec, expected_revision: int) -> ReaderState:
    """Strict revision check, e.g. to pin a build slot under a revision.

    No retry allowance: ``expected + 1`` is a conflict here.  The switch
    operations use :func:`_read_for_switch`, which accepts the caller's own
    landed CAS.
    """
    expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, spec, allow_missing=(expected == 0))
    if state.revision != expected:
        raise RevisionConflict(
            f'{spec.source} reader revision drift: expected={expected}, '
            f'actual={state.revision}'
        )
    return state


def _read_for_switch(
    cur, spec: SlotSpec, expected_revision: int, actor: str,
) -> tuple[ReaderState, bool]:
    """Return the state and whether this caller's CAS has already landed."""
    expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, spec, allow_missing=(expected == 0))
    if state.revision == expected + 1 and state.active_slot in SLOTS:
        if state.updated_by != actor:
            raise RevisionConflict(
                f'{spec.source} reader revision drift: expected={expected}, '
                f'actual={state.revision} switched by {state.updated_by!r}, '
                f'not {actor!r}'
            )
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


def _normalise_view_sql(sql: str) -> tuple[str, ...]:
    """Compare SQL tokens, preserving literals and expression structure."""
    tokens = re.findall(
        r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+|[^\s]", sql,
    )
    return tuple(
        token if token.startswith("'") else token.removeprefix('"').removesuffix('"').lower()
        for token in tokens
    )


def prepare_reader_views(cur, spec: SlotSpec, *, expected_revision: int) -> ReaderState:
    """Validate all relations, then install state-selected views before a CAS.

    A partially completed migration may be retried: each view must already be
    state-selected or statically pinned to the current active slot. Missing
    views are allowed only before the first publication. No reader state is
    changed. Callers must serialize builds/switches for a source.
    """
    if not spec.state_selected:
        raise ValueError('prepare_reader_views requires state_selected=True')
    state = assert_reader_revision(cur, spec, expected_revision)
    if not state.exists:
        raise StateInvariantError(f'{spec.source} reader singleton is missing')
    for slot in SLOTS:
        missing = _missing_slot_tables(cur, spec, slot)
        if missing:
            raise StateInvariantError(f'slot {slot} is not built: missing {", ".join(missing)}')
    catalog, schema = spec.schema.split('.')
    pending = []
    for table in spec.tables:
        names = ', '.join(_sql_literal(slotted_relation(table, slot)) for slot in SLOTS)
        _execute(cur, f"""
SELECT table_name, column_name, data_type, ordinal_position
FROM {catalog}.information_schema.columns
WHERE table_schema = {_sql_literal(schema)} AND table_name IN ({names})
ORDER BY table_name, ordinal_position
""".strip())
        columns = {slotted_relation(table, slot): [] for slot in SLOTS}
        for name, column, data_type, ordinal in cur.fetchall():
            columns[name].append((int(ordinal), column, data_type))
        left, right = (sorted(columns[slotted_relation(table, slot)]) for slot in SLOTS)
        if not left or left != right:
            raise StateInvariantError(f'{table}: incompatible or missing physical slot columns')
        _execute(cur, f"""
SELECT view_definition
FROM {catalog}.information_schema.views
WHERE table_schema = {_sql_literal(schema)} AND table_name = {_sql_literal(table)}
""".strip())
        rows = cur.fetchall()
        desired = reader_view_sql(spec, table, 'a')
        desired_query = desired.split(' AS\n', 1)[1]
        if len(rows) == 1 and _normalise_view_sql(rows[0][0]) == _normalise_view_sql(desired_query):
            continue
        if not rows and state.active_slot is None:
            pending.append(desired)
            continue
        legacy = (
            f'SELECT * FROM {spec.schema}.{slotted_relation(table, state.active_slot)}'
            if state.active_slot else None
        )
        if (len(rows) != 1 or legacy is None
                or _normalise_view_sql(rows[0][0]) != _normalise_view_sql(legacy)):
            raise StateInvariantError(f'{table}: unsafe reader view for active slot {state.active_slot}')
        pending.append(desired)
    # All validation precedes DDL; drift also invalidates legacy migrations.
    assert_reader_revision(cur, spec, state.revision)
    for sql in pending:
        _execute(cur, sql)
    return assert_reader_revision(cur, spec, state.revision)


def _switch_from(
    cur, spec: SlotSpec, state: ReaderState, slot: str, *, applied: bool, actor: str,
) -> dict[str, Any]:
    switched = not applied and state.active_slot != slot
    revision = state.revision
    if spec.state_selected:
        prepare_reader_views(cur, spec, expected_revision=state.revision)
        if switched:
            drain_readers(cur)
            assert_reader_revision(cur, spec, state.revision)
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
    if not spec.state_selected:
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
        'from_slot': other_slot(state.active_slot) if applied else state.active_slot,
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
    """CAS the reader state onto ``to_slot``.

    Re-switching onto the active slot, or retrying after this CAS already
    landed, skips the UPDATE. Static mode rebuilds views after the CAS;
    state-selected mode validates/prepares permanent views before it.
    """
    slot = normalise_slot(to_slot)
    state, applied = _read_for_switch(cur, spec, expected_revision, actor)
    if applied and state.active_slot != slot:
        raise RevisionConflict(
            f'{spec.source} reader moved to slot {state.active_slot}, not {slot}'
        )
    return _switch_from(cur, spec, state, slot, applied=applied, actor=actor)


def advance(
    cur, spec: SlotSpec, *, expected_revision: int, actor: str = DEFAULT_ACTOR,
) -> dict[str, Any]:
    """Promote a finished build in the inactive slot."""
    state, applied = _read_for_switch(cur, spec, expected_revision, actor)
    slot = state.active_slot if applied else inactive_slot(state)
    return _switch_from(cur, spec, state, slot, applied=applied, actor=actor)


def rollback(
    cur, spec: SlotSpec, *, expected_revision: int, actor: str = DEFAULT_ACTOR,
) -> dict[str, Any]:
    """Return readers to the previously active slot."""
    state, applied = _read_for_switch(cur, spec, expected_revision, actor)
    if state.active_slot is None:
        raise StateInvariantError(f'{spec.source}: nothing to roll back')
    slot = state.active_slot if applied else other_slot(state.active_slot)
    return _switch_from(cur, spec, state, slot, applied=applied, actor=actor)
