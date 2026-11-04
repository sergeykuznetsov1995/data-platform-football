"""FotMob Silver core build into an inactive A/B slot (#2020, epic #1677).

Like :mod:`utils.silver_slots`, this module has no Airflow or Trino-client
imports: every step takes a DB-API cursor and emits plain Trino SQL, so the DAG
``dag_transform_fotmob_silver_v2``, a host-side pilot and the unit tests share
one contract.

* The 22 core tables (``dags/sql/silver/fotmob_*.sql.j2``) are built into
  ``<table>_<slot>`` of the slot readers do not see
  (:func:`silver_slots.inactive_slot`); :func:`switch_reader` then moves every
  reader view at once with :func:`silver_slots.advance` (also when nothing
  changed: the build slot then equals bronze).
* Context tables (13, partitioned by ``competition_id, source_season_key``) are
  rebuilt per partition with ``DELETE`` + ``INSERT``, in batches of one
  competition.  A partition is rebuilt when its change signal (max
  ``_observed_at``, max ``_ingested_at`` and row count of its bronze
  ``_current`` views) differs from the signal recorded for *this slot* in the
  build-state table; a batch that
  runs out of memory is split in half.  Match tables read the whole competition
  calendar and the cards of every season key the batch's matches carry
  (``keys(S)``: a split-season card lies under the parent key).
* Global tables (9) are rebuilt whole when their signal changes, optionally in
  ``mod(<key>, N)`` buckets (players do not fit the 2 GB query limit).
* Build state is written after each batch, so a failed run resumes where it
  stopped.  Both slots start empty: the first two runs are full builds.  The
  state carries the sha256 of the table's template: a changed template drops
  the slot table and rebuilds it in full.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .silver_slots import (
    SlotSpec,
    advance,
    assert_reader_revision,
    bootstrap,
    inactive_slot,
    other_slot,
    read_reader_state,
    rollback,
    slotted_relation,
)

_LOG = logging.getLogger(__name__)

MATCH_TABLES = (
    'fotmob_matches', 'fotmob_match_teams', 'fotmob_match_team_stats',
    'fotmob_match_lineups', 'fotmob_match_player_stats', 'fotmob_match_events',
    'fotmob_shots', 'fotmob_match_momentum',
)
SEASON_CONTEXT_TABLES = (
    'fotmob_season_stages', 'fotmob_standings', 'fotmob_season_teams',
    'fotmob_leaderboards', 'fotmob_playoff_brackets',
)
GLOBAL_TABLES = (
    'fotmob_competitions', 'fotmob_competition_seasons',
    'fotmob_competition_season_history', 'fotmob_players', 'fotmob_player_career',
    'fotmob_player_market_values', 'fotmob_teams', 'fotmob_team_squads',
    'fotmob_transfers',
)
ALL_TABLES = MATCH_TABLES + SEASON_CONTEXT_TABLES + GLOBAL_TABLES
FAMILIES = {'match': MATCH_TABLES, 'season': SEASON_CONTEXT_TABLES, 'global': GLOBAL_TABLES}
PARTITION_COLUMNS = ('competition_id', 'source_season_key')

CALENDAR_VIEW = 'fotmob_matches_current'
PAYLOAD_VIEW = 'fotmob_match_payloads_current'
# Bronze views read by each table; their max(_observed_at)/count(*) is the change signal.
SIGNAL_SOURCES = {
    **{table: (PAYLOAD_VIEW, CALENDAR_VIEW) for table in MATCH_TABLES},
    'fotmob_season_stages': ('fotmob_season_stages_current',),
    'fotmob_standings': ('fotmob_standings_current',),
    'fotmob_season_teams': ('fotmob_season_teams_current',),
    'fotmob_leaderboards': ('fotmob_leaderboards_current', 'fotmob_leaderboard_categories_current'),
    'fotmob_playoff_brackets': ('fotmob_playoff_brackets_current',),
    'fotmob_competitions': ('fotmob_competitions_current',),
    'fotmob_competition_seasons': ('fotmob_competition_seasons_current',),
    'fotmob_competition_season_history': ('fotmob_competition_season_history_current',),
    'fotmob_players': ('fotmob_player_snapshots_current',),
    'fotmob_player_career': ('fotmob_player_snapshots_current',),
    'fotmob_player_market_values': ('fotmob_player_snapshots_current',),
    'fotmob_teams': ('fotmob_team_snapshots_current',),
    'fotmob_team_squads': ('fotmob_squad_snapshots_current',),
    'fotmob_transfers': ('fotmob_transfer_events_current',),
}
# Bronze views whose raw competition_id is varchar (the rest are bigint).
VARCHAR_COMPETITION_VIEWS = frozenset({
    PAYLOAD_VIEW, 'fotmob_leaderboard_categories_current', 'fotmob_competitions_current',
    'fotmob_competition_seasons_current', 'fotmob_competition_season_history_current',
})
# Global tables with fotmob_season_scope: built whole, narrowed only by competition.
SEASON_GLOBAL_TABLES = GLOBAL_TABLES[:3]
BUCKET_KEY = {
    'fotmob_players': 'player_id',
    'fotmob_player_career': 'player_id',
    'fotmob_player_market_values': 'player_id',
    'fotmob_teams': 'team_id',
    'fotmob_team_squads': 'team_id',
    'fotmob_transfers': 'from_base(substr(transfer_event_id, 1, 6), 16)',
}
DEFAULT_BUCKETS = {
    'fotmob_players': 8, 'fotmob_player_career': 4, 'fotmob_player_market_values': 4,
    'fotmob_teams': 2,
}
# Merged by the DAG over silver_tasks._PARTITION_STAGED_SESSION_PROPERTIES.
SESSION_PROPERTIES = {
    'query_max_memory': '2GB',
    'query_max_total_memory': '2GB',
    'query_max_run_time': '30m',
    'enable_dynamic_filtering': 'false',
}

_MEMORY_ERROR = re.compile(
    r'EXCEEDED_LOCAL_MEMORY_LIMIT|EXCEEDED_GLOBAL_MEMORY_LIMIT|exceeded .*memory limit', re.I,
)
_PLACEHOLDER = re.compile(r'\{\{\s*[A-Za-z_][A-Za-z0-9_]*\s*\}\}')
_CATALOG_SCHEMA = re.compile(r'[a-z0-9_]+\.[a-z0-9_]+')
_QUALIFIED = re.compile(r'[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+')

# A change signal: (max(_observed_at), max(_ingested_at) as Trino varchar or None, row count).
Signal = tuple
EMPTY_SIGNAL = (None, None, 0)
# Build-state row marking "a build of this run touches the slot" (rollback guard).
BUILD_MARKER = '__build__'
ROLLED_BACK_SHA = 'rolled_back'


@dataclass(frozen=True)
class BuildConfig:
    schema: str = 'iceberg.silver'
    reader_state_table: str = 'iceberg.ops.fotmob_reader_state'
    build_state_table: str = 'iceberg.ops.fotmob_silver_build_state'
    sql_dir: Path = Path('/opt/airflow/dags/sql/silver')
    competitions: tuple[int, ...] = ()
    tables: tuple[str, ...] = ALL_TABLES
    max_partitions_per_batch: int = 10
    entity_buckets: dict[str, int] | None = None
    full_rebuild: bool = False

    def __post_init__(self) -> None:
        if not _CATALOG_SCHEMA.fullmatch(self.schema):
            raise ValueError(f'schema must be <catalog>.<schema>, got {self.schema!r}')
        for name in (self.reader_state_table, self.build_state_table):
            if not _QUALIFIED.fullmatch(name):
                raise ValueError(f'state table must be <catalog>.<schema>.<table>, got {name!r}')
        object.__setattr__(self, 'sql_dir', Path(self.sql_dir))
        object.__setattr__(self, 'competitions', tuple(int(c) for c in self.competitions))
        tables = tuple(self.tables)
        if not tables:
            raise ValueError('tables must name at least one FotMob Silver table')
        unknown = sorted(set(tables) - set(ALL_TABLES))
        if unknown:
            raise ValueError(f'unknown FotMob Silver tables: {unknown}')
        object.__setattr__(self, 'tables', tables)
        if int(self.max_partitions_per_batch) < 1:
            raise ValueError('max_partitions_per_batch must be >= 1')
        for table, buckets in (self.entity_buckets or {}).items():
            if table not in GLOBAL_TABLES or int(buckets) < 1:
                raise ValueError(f'entity_buckets: bad entry {table!r}={buckets!r}')
            if int(buckets) > 1 and table not in BUCKET_KEY:
                raise ValueError(f'entity_buckets: {table} has no bucket key')

    def buckets(self, table: str) -> int:
        return int((self.entity_buckets or {}).get(table, DEFAULT_BUCKETS.get(table, 1)))


def slot_spec(cfg: BuildConfig) -> SlotSpec:
    return SlotSpec(
        source='fotmob', schema=cfg.schema, tables=ALL_TABLES,
        state_table=cfg.reader_state_table,
    )


# --- SQL helpers ------------------------------------------------------------

def _run(cur, sql: str) -> list:
    """Run one statement and consume its result (Trino commits on fetch)."""
    cur.execute(sql)
    return cur.fetchall()


def _int(value: Any) -> str:
    return str(int(value))


def _str(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _in(values: Iterable[Any]) -> str:
    return ', '.join(_str(v) for v in sorted(values))


def _is_memory_error(exc: BaseException) -> bool:
    return bool(_MEMORY_ERROR.search(str(exc)))


def render_scoped_sql(
    table: str,
    sql_dir: Path,
    *,
    calendar_scope: str = 'TRUE',
    payload_scope: str = 'TRUE',
    season_scope: str = 'TRUE',
    entity_scope: str = 'TRUE',
) -> str:
    """Render one core template; every scope placeholder is always passed."""
    from .medallion_config import render_sql_template

    sql = render_sql_template(
        Path(sql_dir) / f'{table}.sql.j2',
        fotmob_calendar_scope=calendar_scope,
        fotmob_payload_scope=payload_scope,
        fotmob_season_scope=season_scope,
        fotmob_entity_scope=entity_scope,
    )
    code = re.sub(r'/\*.*?\*/', '', re.sub(r'--[^\n]*', '', sql), flags=re.S)
    left = _PLACEHOLDER.search(code)
    if left:  # an inline placeholder render_sql_template leaves as-is
        raise ValueError(f'{table}: placeholder left unrendered: {left.group(0)}')
    return sql.strip().rstrip(';').strip()


def wrap_select(sql: str, where: str = 'TRUE') -> str:
    return (
        'SELECT *, CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS _silver_created_at\n'
        f'FROM (\n{sql}\n) src\nWHERE {where}'
    )


def _partition_where(comp: int, keys: Iterable[str], *, text_competition: bool = False) -> str:
    competition = _str(int(comp)) if text_competition else _int(comp)
    return f'competition_id = {competition} AND source_season_key IN ({_in(keys)})'


def _competition_filter(view: str, competitions: Iterable[int]) -> str:
    render = (lambda c: _str(int(c))) if view in VARCHAR_COMPETITION_VIEWS else _int
    return f'competition_id IN ({", ".join(render(c) for c in sorted(competitions))})'


def _table_exists(cur, schema: str, name: str) -> bool:
    catalog, schema_name = schema.split('.')
    rows = _run(cur, f"""
SELECT table_name
FROM {catalog}.information_schema.tables
WHERE table_schema = {_str(schema_name)}
  AND table_name = {_str(name)}
""".strip())
    return bool(rows)


# --- change signals and partition plan --------------------------------------

def _merge(a: Signal | None, b: Signal) -> Signal:
    if a is None:
        return b

    def latest(x, y):
        stamps = [v for v in (x, y) if v is not None]
        return max(stamps) if stamps else None
    return (latest(a[0], b[0]), latest(a[1], b[1]), a[2] + b[2])


def signal_sql(view: str, competitions: Iterable[int] = (), *, per_partition: bool = True) -> str:
    competitions = tuple(competitions)
    stamps = 'CAST(max(_observed_at) AS VARCHAR), CAST(max(_ingested_at) AS VARCHAR), count(*)'
    if not per_partition:
        where = f'\nWHERE {_competition_filter(view, competitions)}' if competitions else ''
        return f'SELECT {stamps}\nFROM iceberg.bronze.{view}{where}'
    where = [
        'TRY_CAST(competition_id AS BIGINT) IS NOT NULL',
        '  AND source_season_key IS NOT NULL',
        "  AND source_season_key <> ''",
    ]
    if competitions:
        where.append(f'  AND {_competition_filter(view, competitions)}')
    return '\n'.join([
        'SELECT TRY_CAST(competition_id AS BIGINT), source_season_key,',
        f'       {stamps}',
        f'FROM iceberg.bronze.{view}',
        'WHERE ' + '\n'.join(where),
        'GROUP BY 1, 2',
    ])


def _view_signals(cur, view: str, competitions: tuple, per_partition: bool) -> dict:
    if not per_partition:
        (observed, ingested, rows), = _run(cur, signal_sql(view, competitions, per_partition=False))
        return {(None, None): (observed, ingested, int(rows or 0))}
    return {
        (int(comp), str(key)): (observed, ingested, int(rows))
        for comp, key, observed, ingested, rows in _run(cur, signal_sql(view, competitions))
    }


def read_signals(cur, table: str, competitions: Iterable[int] = (),
                 cache: dict | None = None) -> dict:
    """``{(competition_id, season_key): signal}``; global tables: ``{(None, None): signal}``.

    ``cache`` keeps each bronze view's scan for the run (match tables share
    their views; players/career/market values share one snapshot view).
    """
    cache = {} if cache is None else cache
    per_partition = table not in GLOBAL_TABLES
    scope = tuple(competitions) if per_partition or table in SEASON_GLOBAL_TABLES else ()
    signals: dict = {}
    for view in SIGNAL_SOURCES[table]:
        key = ('signal', view, per_partition, scope)
        if key not in cache:
            cache[key] = _view_signals(cur, view, scope, per_partition)
        for part, signal in cache[key].items():
            signals[part] = _merge(signals.get(part), signal)
    return signals


def plan_competitions(cur, competitions: Iterable[int] = ()) -> dict:
    """Calendar plan ``{competition_id: {S: keys(S)}}``.

    ``keys(S)`` is the union of the calendar keys of every match listed under
    ``S``: the card of a split-season match may lie under any of them.  One
    row per (S, key) pair, no ARRAY (the Trino CLI JSON output drops arrays).
    """
    competitions = tuple(competitions)
    scope = f'\n      AND {_competition_filter(CALENDAR_VIEW, competitions)}' if competitions else ''
    rows = _run(cur, f"""
WITH cal AS (
    SELECT DISTINCT competition_id, match_id, source_season_key
    FROM iceberg.bronze.{CALENDAR_VIEW}
    WHERE competition_id IS NOT NULL
      AND match_id IS NOT NULL
      AND source_season_key IS NOT NULL
      AND source_season_key <> ''{scope}
)
SELECT DISTINCT a.competition_id, a.source_season_key AS output_key,
       b.source_season_key AS card_key
FROM cal a
JOIN cal b
  ON b.competition_id = a.competition_id
 AND b.match_id = a.match_id
""".strip())
    plan: dict = {}
    for comp, key, card_key in rows:
        plan.setdefault(int(comp), {}).setdefault(str(key), set()).add(str(card_key))
    return {comp: {key: frozenset(keys) for key, keys in parts.items()}
            for comp, parts in plan.items()}


def partition_signal(keys: Iterable[str], signals: dict, comp: int) -> Signal:
    signal = None
    for key in sorted(keys):
        if (comp, key) in signals:
            signal = _merge(signal, signals[(comp, key)])
    return signal if signal is not None else EMPTY_SIGNAL


def context_targets(cur, cfg: BuildConfig, table: str, cache: dict | None = None) -> dict:
    """``{competition_id: {S: (keys(S), signal)}}`` for one context table."""
    cache = {} if cache is None else cache
    signals = read_signals(cur, table, cfg.competitions, cache)
    if table in MATCH_TABLES:
        if 'plan' not in cache:
            cache['plan'] = plan_competitions(cur, cfg.competitions)
        plan = {comp: dict(parts) for comp, parts in cache['plan'].items()}
        for comp, key in signals:  # cards without a calendar row: output under their own key
            plan.setdefault(comp, {}).setdefault(key, frozenset({key}))
    else:
        plan = {}
        for comp, key in signals:
            plan.setdefault(comp, {})[key] = frozenset({key})
    return {
        comp: {key: (keys, partition_signal(keys, signals, comp)) for key, keys in parts.items()}
        for comp, parts in plan.items()
    }


# --- build state (per slot) -------------------------------------------------

def build_state_ddl(cfg: BuildConfig) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.build_state_table} (
    slot varchar,
    table_name varchar,
    competition_id bigint,
    source_season_key varchar,
    signal_max_observed_at timestamp(6),
    signal_max_ingested_at timestamp(6),
    signal_rows bigint,
    sql_sha varchar,
    built_at timestamp(6),
    run_id varchar
) WITH (format = 'PARQUET')
""".strip()


def template_sha(cfg: BuildConfig, table: str) -> str:
    return hashlib.sha256((cfg.sql_dir / f'{table}.sql.j2').read_bytes()).hexdigest()


def read_build_state(cur, cfg: BuildConfig, slot: str, table: str) -> tuple[dict, set]:
    """``({(competition_id, key): signal}, {sql_sha, ...})`` of one slot table."""
    rows = _run(cur, f"""
SELECT competition_id, source_season_key, CAST(signal_max_observed_at AS VARCHAR),
       CAST(signal_max_ingested_at AS VARCHAR), signal_rows, sql_sha
FROM {cfg.build_state_table}
WHERE slot = {_str(slot)}
  AND table_name = {_str(table)}
""".strip())
    state = {
        (None if comp is None else int(comp), key): (observed, ingested, int(count))
        for comp, key, observed, ingested, count, _ in rows
    }
    return state, {row[5] for row in rows}


def _slot_state(cur, cfg: BuildConfig, table: str, slot: str,
                log: Callable[[str], Any]) -> dict:
    """Build state of a slot table; a changed template drops the table and its state."""
    state, shas = read_build_state(cur, cfg, slot, table)
    if shas - {template_sha(cfg, table)}:
        log(f'{table}: template changed since slot {slot} was built, rebuilding in full')
        _run(cur, f'DROP TABLE IF EXISTS {_relation(cfg, table, slot)}')
        _delete_state(cur, cfg, slot, [table])
        return {}
    return state


def _delete_state(cur, cfg: BuildConfig, slot: str, tables: Iterable[str],
                  comp: int | None = None, keys: Iterable[str] = ()) -> None:
    where = f'slot = {_str(slot)}\n  AND table_name IN ({_in(tables)})'
    if comp is not None:
        where += f'\n  AND {_partition_where(comp, keys)}'
    _run(cur, f'DELETE FROM {cfg.build_state_table}\nWHERE {where}')


def write_build_state(cur, cfg: BuildConfig, slot: str, table: str, comp: int | None,
                      signals: dict, run_id: str) -> None:
    """Replace the state rows of ``signals`` (``{key: signal}``; global: ``{None: signal}``)."""
    _delete_state(cur, cfg, slot, [table], comp, [k for k in signals if k is not None])

    def stamp(value):
        return 'NULL' if value is None else f'TIMESTAMP {_str(value)}'
    sha = _str(template_sha(cfg, table))
    values = ',\n'.join(
        '    ({slot}, {table}, {comp}, {key}, {observed}, {ingested}, {rows}, {sha}, '
        'CURRENT_TIMESTAMP, {run})'.format(
            slot=_str(slot), table=_str(table),
            comp='NULL' if comp is None else _int(comp),
            key='NULL' if key is None else _str(key),
            observed=stamp(observed), ingested=stamp(ingested),
            rows=_int(rows), sha=sha, run=_str(run_id),
        )
        for key, (observed, ingested, rows) in sorted(signals.items(), key=lambda kv: str(kv[0]))
    )
    _run(cur, (
        f'INSERT INTO {cfg.build_state_table} (slot, table_name, competition_id, '
        'source_season_key, signal_max_observed_at, signal_max_ingested_at, signal_rows, '
        'sql_sha, built_at, run_id)\n'
        f'VALUES\n{values}'
    ))


# --- build --------------------------------------------------------------------

def _relation(cfg: BuildConfig, table: str, slot: str) -> str:
    return f'{cfg.schema}.{slotted_relation(table, slot)}'


def ensure_context_table(cur, cfg: BuildConfig, table: str, slot: str) -> bool:
    """Create the empty partitioned slot table; state without its table is stale.

    Returns True when the table had to be created (its state was cleared).
    """
    if _table_exists(cur, cfg.schema, slotted_relation(table, slot)):
        return False
    _delete_state(cur, cfg, slot, [table])
    sql = render_scoped_sql(
        table, cfg.sql_dir, calendar_scope='FALSE', payload_scope='FALSE',
        season_scope='FALSE', entity_scope='FALSE',
    )
    partitioning = ', '.join(_str(c) for c in PARTITION_COLUMNS)
    _run(cur, (
        f'CREATE TABLE IF NOT EXISTS {_relation(cfg, table, slot)}\n'
        f"WITH (format = 'PARQUET', partitioning = ARRAY[{partitioning}])\n"
        f'AS\n{wrap_select(sql, "FALSE")}\nWITH NO DATA'
    ))
    return True


def _batch_sql(cfg: BuildConfig, table: str, comp: int, batch: list, parts: dict) -> str:
    if table in MATCH_TABLES:
        card_keys = set().union(*(parts[key][0] for key in batch))
        sql = render_scoped_sql(
            table, cfg.sql_dir,
            calendar_scope=f'competition_id = {_int(comp)}',
            payload_scope=_partition_where(comp, card_keys, text_competition=True),
        )
    else:
        sql = render_scoped_sql(table, cfg.sql_dir, season_scope=_partition_where(comp, batch))
    return wrap_select(sql, _partition_where(comp, batch))


def _build_batch(cur, cfg: BuildConfig, table: str, slot: str, comp: int, batch: list,
                 parts: dict, run_id: str, log: Callable[[str], Any]) -> int:
    relation = _relation(cfg, table, slot)
    try:
        _run(cur, f'DELETE FROM {relation}\nWHERE {_partition_where(comp, batch)}')
        _run(cur, f'INSERT INTO {relation}\n{_batch_sql(cfg, table, comp, batch, parts)}')
    except Exception as exc:  # noqa: BLE001 - connector exception hierarchy varies
        if len(batch) == 1 or not _is_memory_error(exc):
            raise
        half = len(batch) // 2
        log(f'{table} {comp}: memory limit on {len(batch)} partitions, splitting in half')
        return (
            _build_batch(cur, cfg, table, slot, comp, batch[:half], parts, run_id, log)
            + _build_batch(cur, cfg, table, slot, comp, batch[half:], parts, run_id, log)
        )
    write_build_state(cur, cfg, slot, table, comp, {key: parts[key][1] for key in batch}, run_id)
    return len(batch)


def build_context_table(cur, cfg: BuildConfig, table: str, slot: str, run_id: str, *,
                        log: Callable[[str], Any] = _LOG.info, cache: dict | None = None) -> int:
    """Rebuild changed partitions of one context table; return partitions touched."""
    state = _slot_state(cur, cfg, table, slot, log)
    if ensure_context_table(cur, cfg, table, slot):
        state = {}
    targets = context_targets(cur, cfg, table, cache)
    in_scope = (lambda c: c in cfg.competitions) if cfg.competitions else (lambda c: True)
    rebuilt = 0
    for comp in sorted(set(targets) | {c for c, _ in state if c is not None and in_scope(c)}):
        parts = targets.get(comp, {})
        gone = sorted(key for c, key in state if c == comp and key not in parts)
        if gone:
            log(f'{table} {comp}: dropping vanished partitions {gone}')
            _run(cur, f'DELETE FROM {_relation(cfg, table, slot)}\n'
                      f'WHERE {_partition_where(comp, gone)}')
            _delete_state(cur, cfg, slot, [table], comp, gone)
            rebuilt += len(gone)
        changed = sorted(key for key, (_, signal) in parts.items() if state.get((comp, key)) != signal)
        size = int(cfg.max_partitions_per_batch)
        for start in range(0, len(changed), size):
            rebuilt += _build_batch(
                cur, cfg, table, slot, comp, changed[start:start + size], parts, run_id, log,
            )
    return rebuilt


def build_global_table(cur, cfg: BuildConfig, table: str, slot: str, run_id: str, *,
                       log: Callable[[str], Any] = _LOG.info, cache: dict | None = None) -> int:
    """Rebuild one global table whole (in buckets) if its signal changed; return 0 or 1."""
    signal = read_signals(cur, table, cfg.competitions, cache)[(None, None)]
    state = _slot_state(cur, cfg, table, slot, log)
    exists = _table_exists(cur, cfg.schema, slotted_relation(table, slot))
    if exists and state.get((None, None)) == signal:
        return 0
    season_scope = 'TRUE'
    if table in SEASON_GLOBAL_TABLES and cfg.competitions:
        season_scope = f'competition_id IN ({", ".join(_int(c) for c in cfg.competitions)})'
    relation = _relation(cfg, table, slot)
    buckets = cfg.buckets(table)
    for bucket in range(buckets):
        entity_scope = f'mod({BUCKET_KEY[table]}, {buckets}) = {bucket}' if buckets > 1 else 'TRUE'
        select = wrap_select(render_scoped_sql(
            table, cfg.sql_dir, season_scope=season_scope, entity_scope=entity_scope,
        ))
        if bucket == 0:
            _run(cur, f"CREATE OR REPLACE TABLE {relation}\nWITH (format = 'PARQUET')\nAS\n{select}")
        else:
            _run(cur, f'INSERT INTO {relation}\n{select}')
        log(f'{table}: bucket {bucket + 1}/{buckets} written')
    write_build_state(cur, cfg, slot, table, None, {None: signal}, run_id)
    return 1


def mark_build(cur, cfg: BuildConfig, slot: str, run_id: str) -> None:
    """Stamp the slot as touched by ``run_id`` before its first write (once per run).

    A failed build may leave a hole (DELETE before INSERT, partial buckets, a
    table dropped for a changed template) without any state row; the marker
    is what the rollback guard sees.  Table state reads filter it out by name.
    """
    rows = _run(cur, f"""
SELECT run_id
FROM {cfg.build_state_table}
WHERE slot = {_str(slot)}
  AND table_name = {_str(BUILD_MARKER)}
""".strip())
    if [row[0] for row in rows] == [run_id]:
        return
    _delete_state(cur, cfg, slot, [BUILD_MARKER])
    _run(cur, (
        f'INSERT INTO {cfg.build_state_table} (slot, table_name, built_at, run_id)\n'
        f'VALUES ({_str(slot)}, {_str(BUILD_MARKER)}, CURRENT_TIMESTAMP, {_str(run_id)})'
    ))


def build_family(cur, cfg: BuildConfig, family: str, slot: str, run_id: str, *,
                 expected_revision: int, log: Callable[[str], Any] = _LOG.info) -> int:
    """Build one family into ``slot``; stop if readers moved. Return units rebuilt."""
    spec = slot_spec(cfg)
    cache: dict = {}
    rebuilt = 0
    for index, table in enumerate(t for t in FAMILIES[family] if t in cfg.tables):
        state = assert_reader_revision(cur, spec, expected_revision)
        if inactive_slot(state) != slot:
            raise RuntimeError(f'slot {slot} is not inactive (reader on {state.active_slot})')
        if index == 0:
            mark_build(cur, cfg, slot, run_id)
        if family == 'global':
            count = build_global_table(cur, cfg, table, slot, run_id, log=log, cache=cache)
        else:
            count = build_context_table(cur, cfg, table, slot, run_id, log=log, cache=cache)
        log(f'{table}: rebuilt {count} in slot {slot}')
        rebuilt += count
    return rebuilt


def drop_slot(cur, cfg: BuildConfig, slot: str) -> None:
    """Forget a slot (``full_rebuild``): drop its tables and its build state."""
    for table in cfg.tables:
        _run(cur, f'DROP TABLE IF EXISTS {_relation(cfg, table, slot)}')
    _delete_state(cur, cfg, slot, (*cfg.tables, BUILD_MARKER))


def prepare_run(cur, cfg: BuildConfig) -> dict:
    """Bootstrap reader and build state; pick the build slot for this run."""
    spec = slot_spec(cfg)
    bootstrap(cur, spec)
    _run(cur, build_state_ddl(cfg))
    state = read_reader_state(cur, spec, allow_missing=False)
    slot = inactive_slot(state)
    if cfg.full_rebuild:
        drop_slot(cur, cfg, slot)
    return {'active_slot': state.active_slot, 'revision': state.revision, 'build_slot': slot}


def _written_since_switch_sql(cfg: BuildConfig, slot: str) -> str:
    spec = slot_spec(cfg)
    return f"""
SELECT count(*)
FROM {cfg.build_state_table}
WHERE slot = {_str(slot)}
  AND built_at > (
    SELECT updated_at
    FROM {spec.state_table}
    WHERE source = {_str(spec.source)}
      AND state_key = 'canonical'
  )
""".strip()


def switch_reader(cur, cfg: BuildConfig, *, mode: str, expected_revision: int,
                  actor: str) -> dict:
    """Advance onto the build slot or roll back.

    ``build`` always advances: with nothing rebuilt the build slot already
    matches bronze, so it is not older than the active one; ``advance``
    refuses a slot with a missing table.  It requires ``cfg.tables`` to be all
    22 tables; a ``competitions`` subset is allowed here and is the caller's
    responsibility: the slot then holds the other competitions from its
    previous build (the production DAG always builds all competitions).

    ``rollback`` must run before the next build: it is refused once a build
    has started on the target slot after the last switch (its marker or state
    rows are newer than the switch; a failed build may have left a hole).
    After it the build state of the abandoned slot is invalidated (``sql_sha``
    = ``rolled_back``), so the next build drops and rebuilds every table there.
    """
    spec = slot_spec(cfg)
    if mode == 'rollback':
        state = read_reader_state(cur, spec, allow_missing=False)
        if state.active_slot is not None and state.revision == int(expected_revision):
            target = other_slot(state.active_slot)
            (written,), = _run(cur, _written_since_switch_sql(cfg, target))
            if int(written):
                raise RuntimeError(
                    f'rollback refused: a build started on slot {target} after the last '
                    f'switch ({written} state rows); finish a build and advance instead'
                )
        result = rollback(cur, spec, expected_revision=expected_revision, actor=actor)
        # Invalidate, not delete: the next build drops and rebuilds every table of the
        # abandoned slot (vanished partitions go with the DROP); rolling back again
        # before that build still finds its tables intact.
        _run(cur, (
            f'UPDATE {cfg.build_state_table}\n'
            f'SET sql_sha = {_str(ROLLED_BACK_SHA)}\n'
            f'WHERE slot = {_str(other_slot(result["to_slot"]))}'
        ))
        return result
    if mode != 'build':
        raise ValueError(f'mode must be build or rollback, got {mode!r}')
    if set(cfg.tables) != set(ALL_TABLES):
        raise ValueError(
            'switch refused: a build switch needs all 22 FotMob Silver tables, '
            f'this run built {len(cfg.tables)}: {", ".join(cfg.tables)}'
        )
    return advance(cur, spec, expected_revision=expected_revision, actor=actor)
