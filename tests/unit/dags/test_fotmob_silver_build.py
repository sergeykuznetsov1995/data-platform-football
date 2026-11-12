"""FotMob Silver core build (#2020) on a scripted DB-API cursor: slots, increment, batches."""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest
import yaml

from dags.utils import fotmob_silver_build as build
from dags.utils.silver_slots import RevisionConflict, StateInvariantError, reader_view_sql

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = ROOT / 'dags' / 'sql' / 'silver'
PAYLOADS = 'fotmob_match_payloads_current'
CALENDAR = 'fotmob_matches_current'
TS1 = '2026-11-04 03:54:00.000000'
TS2 = '2026-11-05 03:54:00.000000'
_TOKEN = re.compile(r"TIMESTAMP '[^']*'|'(?:[^']|'')*'|NULL|CURRENT_TIMESTAMP|-?\d+")


def _signal(value):
    return value if len(value) == 3 else (value[0], value[0], value[1])


def _value(token):
    if token == 'NULL':
        return None
    if token.startswith('TIMESTAMP '):
        return token[len("TIMESTAMP '"):-1]
    if token.startswith("'"):
        return token[1:-1].replace("''", "'")
    return token if token == 'CURRENT_TIMESTAMP' else int(token)


class FakeCursor:
    """Scripted Trino stand-in.

    ``calendar`` — ``{(comp, S): keys(S)}`` (answer of the calendar plan query);
    ``signals`` — ``{view: {(comp, key): (max_observed_at[, max_ingested_at], rows)}}``;
    ``global_signals`` — ``{view: (max_observed_at[, max_ingested_at], rows)}``
    (a 2-tuple means ingested = observed). ``competition_id IN (…)`` filters apply.
    Emulates the reader-state row (as ``test_silver_slots``), the build-state table and
    which tables exist; slot DELETE/INSERT are only logged. ``clock`` ticks per statement
    and stamps ``built_at`` and the reader's ``updated_at``. ``hook(cur, sql)`` may raise.
    Every result must be fetched before the next statement.
    """

    def __init__(self, *, calendar=None, signals=None, global_signals=None, hook=None):
        self.calendar = calendar or {}
        self.signals = signals or {}
        self.global_signals = global_signals or {}
        self.hook = hook
        self.reader = [None, 0, 'bootstrap']
        self.reader_updated_at = 0
        self.clock = 0
        self.state: list[dict] = []
        self.markers: dict[str, dict] = {}  # slot -> build-marker row (table_name '__build__')
        self.tables: set[str] = set()
        self.views = {}
        self.columns = {}
        self.output_columns = [('id', 'bigint'), ('value', 'varchar')]
        self.sql: list[str] = []
        self._result = None

    def execute(self, sql):
        assert self._result is None, 'previous result was not fetched'
        self.clock += 1
        self.sql.append(sql)
        if self.hook:
            self.hook(self, sql)
        self._result = self._answer(sql)

    def fetchall(self):
        result, self._result = self._result, None
        return result

    def _answer(self, sql):
        head = sql.split(None, 1)[0].upper()
        if sql == 'SELECT count(*) FROM system.runtime.nodes WHERE coordinator':
            return [(1,)]
        if sql == 'SELECT to_iso8601(current_timestamp)':
            return [('2026-11-05T13:00:00Z',)]
        if sql.startswith('SELECT transaction_id FROM system.runtime.transactions'):
            return []
        if sql.startswith('DESCRIBE OUTPUT'):
            return [(name, '', '', '', typ, 0, False) for name, typ in self.output_columns]
        if 'information_schema.columns' in sql:
            names = re.findall(r"'(fotmob_[a-z_]+)'", sql)
            if sql.startswith('SELECT column_name'):
                return self.columns.get(names[0], [('id', 'bigint'), ('value', 'varchar')])
            return [(n, c, t, i) for n in names if n in self.tables
                    for i, (c, t) in enumerate(self.columns.get(n, [('id', 'bigint'), ('value', 'varchar')]), 1)]
        if 'information_schema.views' in sql:
            name = re.search(r"table_name = '([^']+)'", sql).group(1)
            return [(self.views[name],)] if name in self.views else []
        if head == 'CREATE' and ' VIEW ' in sql:
            name = re.search(r'VIEW ([\w.]+)', sql).group(1).rsplit('.', 1)[1]
            self.views[name] = sql.split(' AS\n', 1)[1]
            return [(0,)]
        if 'information_schema.tables' in sql:
            names = re.findall(r"'([a-z0-9_]+)'", sql.split('AND table_name', 1)[1])
            return [(n,) for n in names if n in self.tables]
        if head == 'CREATE' and ' VIEW ' not in sql:
            name = re.search(r'TABLE (?:IF NOT EXISTS )?([\w.]+)', sql).group(1)
            self.tables.add(name.rsplit('.', 1)[1])
            return [(0,)]
        if head == 'DROP':
            self.tables.discard(sql.split()[-1].rsplit('.', 1)[1])
            return [(0,)]
        if 'built_at >' in sql:
            slot = re.search(r"slot = '([ab])'", sql).group(1)
            rows = [r for r in self.state if r['slot'] == slot] + [
                m for m in self.markers.values() if m['slot'] == slot]
            return [(sum(1 for r in rows if r['built_at'] > self.reader_updated_at),)]
        if 'fotmob_reader_state' in sql:
            return self._reader(head, sql)
        if 'fotmob_silver_build_state' in sql:
            return self._state(head, sql)
        comps = re.search(r'competition_id IN \(([^)]*)\)', sql)
        comps = {int(c.strip(" '")) for c in comps.group(1).split(',')} if comps else None
        if 'card_key' in sql:
            return [(c, s, k) for (c, s), keys in sorted(self.calendar.items()) for k in sorted(keys)
                    if comps is None or c in comps]
        view = re.search(r'FROM iceberg\.bronze\.(\w+)', sql)
        if head == 'SELECT' and view:
            view = view.group(1)
            if 'GROUP BY' not in sql:
                return [_signal(self.global_signals.get(view, (None, 0)))]
            return [(c, k, *_signal(v)) for (c, k), v in sorted(self.signals.get(view, {}).items())
                    if comps is None or c in comps]
        return [(0,)]

    def _reader(self, head, sql):
        if head == 'SELECT':
            slot, rev, by = self.reader
            return [(slot, rev, None, by)]
        if head == 'UPDATE':
            expected = int(re.search(r'AND revision = (\d+)', sql).group(1))
            slot = re.search(r"SET active_slot = '([ab])'", sql).group(1)
            actor = re.search(r"updated_by = '([^']*)'", sql).group(1)
            if self.reader[1] == expected and self.reader[0] != slot:
                self.reader = [slot, expected + 1, actor]
                self.reader_updated_at = self.clock
        return [(1,)]

    def _state_filter(self, sql):
        where = sql.split('WHERE', 1)[1]
        slot = re.search(r"slot = '([ab])'", where).group(1)
        tables = set(re.findall(r"'([a-z_]+)'", re.search(r'table_name (?:IN \(|= )([^)\n]*)', where).group(1)))
        comp = re.search(r'competition_id = (\d+)', where)
        keys = re.search(r'source_season_key IN \(([^)]*)\)', where)
        keys = {_value(t) for t in _TOKEN.findall(keys.group(1))} if keys else None

        def match(row):
            return (row['slot'] == slot and row['table_name'] in tables
                    and (comp is None or row['competition_id'] == int(comp.group(1)))
                    and (keys is None or row['source_season_key'] in keys))
        return match

    def _state(self, head, sql):
        if head == 'MERGE':
            values = re.search(r'USING \(VALUES \((.*?)\)\)', sql).group(1)
            slot, table, run_id = map(_value, _TOKEN.findall(values))
            self.markers[slot] = {'slot': slot, 'table_name': table,
                                  'run_id': run_id, 'built_at': self.clock}
            return [(1,)]
        if sql.startswith('SELECT run_id'):
            marker = self.markers.get(re.search(r"slot = '([ab])'", sql).group(1))
            return [(marker['run_id'],)] if marker else []
        if head == 'UPDATE':
            slot = re.search(r"WHERE slot = '([ab])'", sql).group(1)
            sha = re.search(r"SET sql_sha = '([^']*)'", sql).group(1)
            for row in [r for r in self.state if r['slot'] == slot] + list(self.markers.values()):
                if row['slot'] == slot:
                    row['sql_sha'] = sha
            return [(0,)]
        if head == 'SELECT':
            match = self._state_filter(sql)
            return [(r['competition_id'], r['source_season_key'], r['signal_max_observed_at'],
                     r['signal_max_ingested_at'], r['signal_rows'], r['sql_sha'])
                    for r in self.state if match(r)]
        if head == 'DELETE':
            match = self._state_filter(sql)
            self.state = [r for r in self.state if not match(r)]
            for slot in [k for k, m in self.markers.items() if match(m)]:
                del self.markers[slot]
        if head == 'INSERT':
            cols = re.search(r'\(([^)]*)\)\s*VALUES', sql).group(1).replace(' ', '').split(',')
            for line in sql.split('VALUES', 1)[1].strip().splitlines():
                row = dict(zip(cols, map(_value, _TOKEN.findall(line))))
                row['built_at'] = self.clock
                if row['table_name'] == build.BUILD_MARKER:
                    self.markers[row['slot']] = row
                else:
                    self.state.append(row)
        return [(0,)]

    # --- views on the log -------------------------------------------------
    def writes(self, table=None, slot=None):
        pattern = re.compile(r'(DELETE FROM|INSERT INTO|CREATE (?:OR REPLACE )?TABLE(?: IF NOT EXISTS)?) '
                             r'iceberg\.silver\.(fotmob_\w+)_([ab])\b')
        out = []
        for sql in self.sql:
            m = pattern.match(sql)
            if m and (table is None or m.group(2) == table) and (slot is None or m.group(3) == slot):
                out.append(sql)
        return out

    def state_keys(self, table, slot='a'):
        return sorted((r['competition_id'], r['source_season_key']) for r in self.state
                      if r['table_name'] == table and r['slot'] == slot)


def _match_cursor(**kwargs):
    """47 with seasons 2025/2026 and 2026/2027 in calendar and cards."""
    signals = {
        PAYLOADS: {(47, '2025/2026'): (TS1, 380), (47, '2026/2027'): (TS1, 100)},
        CALENDAR: {(47, '2025/2026'): (TS1, 380), (47, '2026/2027'): (TS1, 380)},
    }
    calendar = {(47, '2025/2026'): {'2025/2026'}, (47, '2026/2027'): {'2026/2027'}}
    return FakeCursor(calendar=calendar, signals=signals, **kwargs)


def _cfg(tables=('fotmob_matches',), **kwargs):
    return build.BuildConfig(sql_dir=SQL_DIR, tables=tables, **kwargs)


FULL = build.BuildConfig(sql_dir=SQL_DIR)


def _switch(cur, revision, actor, slot):
    """Advance with the full table set (the fake only knows the tables it built)."""
    cur.tables |= {f'{t}_{s}' for t in build.ALL_TABLES for s in ('a', 'b')}
    for table in build.ALL_TABLES:
        cur.views.setdefault(table, reader_view_sql(build.slot_spec(FULL), table, 'a').split(' AS\n', 1)[1])
    return build.switch_reader(cur, FULL, mode='build', expected_revision=revision, actor=actor)


def _run_family(cur, cfg, family, run_id='run-1'):
    run = build.prepare_run(cur, cfg)
    return run, build.build_family(
        cur, cfg, family, run['build_slot'], run_id, expected_revision=run['revision'],
    )


def _deletes_inserts(cur, table, slot='a'):
    return [s.split(None, 1)[0] for s in cur.writes(table, slot) if not s.startswith('CREATE')]


# 1. first build ---------------------------------------------------------------

def test_first_build_creates_slot_table_and_writes_partition_batch():
    cur = _match_cursor()
    run, rebuilt = _run_family(cur, _cfg(), 'match')

    assert run == {'active_slot': None, 'revision': 0, 'build_slot': 'a'}
    assert rebuilt == 2
    create, delete, insert = cur.writes('fotmob_matches', 'a')
    assert create.startswith('CREATE TABLE IF NOT EXISTS iceberg.silver.fotmob_matches_a')
    assert "partitioning = ARRAY['competition_id', 'source_season_key']" in create
    assert create.endswith('WITH NO DATA')
    assert delete == ("DELETE FROM iceberg.silver.fotmob_matches_a\n"
                      "WHERE competition_id = 47 AND source_season_key IN ('2025/2026', '2026/2027')")
    assert insert.startswith('INSERT INTO iceberg.silver.fotmob_matches_a\nSELECT *, CAST(current_timestamp')
    assert 'competition_id = 47\n' in insert  # calendar scope over BIGINT
    assert "competition_id = '47' AND source_season_key IN ('2025/2026', '2026/2027')" in insert
    assert insert.endswith("WHERE competition_id = 47 AND source_season_key IN ('2025/2026', '2026/2027')")
    assert cur.state_keys('fotmob_matches') == [(47, '2025/2026'), (47, '2026/2027')]
    rows = {r['source_season_key']: r for r in cur.state}
    assert rows['2026/2027']['signal_rows'] == 480  # payloads + calendar of keys(S)
    assert rows['2026/2027']['signal_max_observed_at'] == TS1
    assert rows['2026/2027']['signal_max_ingested_at'] == TS1
    assert rows['2026/2027']['sql_sha'] == build.template_sha(_cfg(), 'fotmob_matches')
    assert rows['2026/2027']['run_id'] == 'run-1'


def test_split_season_batch_reads_cards_of_the_parent_key():
    signals = {
        PAYLOADS: {(112, '2026'): (TS1, 900)},
        CALENDAR: {(112, '2026'): (TS1, 900), (112, '2026 - Clausura'): (TS1, 500)},
    }
    calendar = {(112, '2026'): {'2026', '2026 - Clausura'},
                (112, '2026 - Clausura'): {'2026', '2026 - Clausura'}}
    cur = FakeCursor(calendar=calendar, signals=signals)
    _, rebuilt = _run_family(cur, _cfg(max_partitions_per_batch=1), 'match')

    assert rebuilt == 2
    inserts = [s for s in cur.writes('fotmob_matches') if s.startswith('INSERT')]
    clausura = next(s for s in inserts if s.endswith("source_season_key IN ('2026 - Clausura')"))
    assert "competition_id = '112' AND source_season_key IN ('2026', '2026 - Clausura')" in clausura


# 2–4, 6. increment per slot --------------------------------------------------------

def test_repeat_with_unchanged_signals_writes_nothing():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    before = len(cur.writes())
    run = build.prepare_run(cur, _cfg())
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)
    assert run['build_slot'] == 'a' and rebuilt == 0
    assert len(cur.writes()) == before


def test_new_rows_rebuild_only_their_partition():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.sql.clear()
    cur.signals[PAYLOADS][(47, '2026/2027')] = (TS1, 110)
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)

    assert rebuilt == 1
    delete, insert = cur.writes('fotmob_matches')
    assert delete.endswith("source_season_key IN ('2026/2027')")
    assert insert.endswith("source_season_key IN ('2026/2027')")
    assert {r['source_season_key']: r['run_id'] for r in cur.state} == {
        '2025/2026': 'run-1', '2026/2027': 'run-2'}


def test_card_fix_same_count_newer_observed_rebuilds_partition_and_key_neighbours():
    signals = {
        PAYLOADS: {(112, '2025'): (TS1, 10), (112, '2026'): (TS1, 900)},
        CALENDAR: {(112, '2025'): (TS1, 10), (112, '2026'): (TS1, 900),
                   (112, '2026 - Clausura'): (TS1, 500)},
    }
    calendar = {(112, '2025'): {'2025'}, (112, '2026'): {'2026', '2026 - Clausura'},
                (112, '2026 - Clausura'): {'2026', '2026 - Clausura'}}
    cur = FakeCursor(calendar=calendar, signals=signals)
    _run_family(cur, _cfg(), 'match')
    cur.sql.clear()
    cur.signals[PAYLOADS][(112, '2026')] = (TS2, 900)
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)

    assert rebuilt == 2
    delete, _ = cur.writes('fotmob_matches')
    assert delete.endswith("source_season_key IN ('2026', '2026 - Clausura')")
    assert {r['source_season_key']: r['signal_max_observed_at'] for r in cur.state} == {
        '2025': TS1, '2026': TS2, '2026 - Clausura': TS2}


def test_other_slot_with_empty_state_is_built_in_full():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    assert _switch(cur, 0, 'run-1', 'a')['to_slot'] == 'a'
    cur.sql.clear()
    run, rebuilt = _run_family(cur, _cfg(), 'match', run_id='run-2')

    assert run == {'active_slot': 'a', 'revision': 1, 'build_slot': 'b'}
    assert rebuilt == 2
    assert _deletes_inserts(cur, 'fotmob_matches', 'b') == ['DELETE', 'INSERT']
    assert cur.writes(slot='a') == []
    assert cur.state_keys('fotmob_matches', 'b') == [(47, '2025/2026'), (47, '2026/2027')]


def test_build_refuses_when_reader_moved():
    cur = _match_cursor()
    build.prepare_run(cur, _cfg())
    cur.reader = ['a', 1, 'someone']
    cur.sql.clear()
    with pytest.raises(RevisionConflict):
        build.build_family(cur, _cfg(), 'match', 'a', 'run-1', expected_revision=0)
    assert cur.writes() == []


# 5. switch: empty input still advances; rollback ---------------------------------------------------------------

def test_empty_input_still_advances_onto_equal_slot():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    _switch(cur, 0, 'run-1', 'a')
    _run_family(cur, _cfg(), 'match', run_id='run-2')  # builds b in full
    _switch(cur, 1, 'run-2', 'b')
    assert cur.reader == ['b', 2, 'run-2']

    run = build.prepare_run(cur, _cfg())
    cur.sql.clear()
    rebuilt = build.build_family(cur, _cfg(), 'match', run['build_slot'], 'run-3',
                                 expected_revision=run['revision'])
    assert (run['build_slot'], rebuilt) == ('a', 0) and cur.writes() == []
    result = _switch(cur, 2, 'run-3', 'a')
    assert result['to_slot'] == 'a' and result['switched'] is True
    assert cur.reader == ['a', 3, 'run-3']


def test_rollback_mode_uses_rollback():
    cur = _match_cursor()
    build.prepare_run(cur, _cfg())
    cur.reader = ['b', 4, 'run-0']
    cur.tables |= {f'{t}_{s}' for t in build.ALL_TABLES for s in ('a', 'b')}
    for table in build.ALL_TABLES:
        cur.views[table] = f'SELECT * FROM iceberg.silver.{table}_b'
    cur.sql.clear()
    result = build.switch_reader(cur, FULL, mode='rollback', expected_revision=4, actor='run-9')
    assert result['to_slot'] == 'a' and result['revision'] == 5
    assert cur.reader == ['a', 5, 'run-9']
    views = [s for s in cur.sql if 'VIEW' in s]
    assert len(views) == 22 and all('UNION ALL' in s for s in views)


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        build.switch_reader(FakeCursor(), FULL, mode='nope', expected_revision=0, actor='x')


# 7. vanished partition -------------------------------------------------------------

def test_vanished_partition_is_deleted_without_insert():
    cur = _match_cursor()
    cur.calendar[(47, '2024/2025')] = {'2024/2025'}
    cur.signals[CALENDAR][(47, '2024/2025')] = (TS1, 380)
    _run_family(cur, _cfg(), 'match')
    assert (47, '2024/2025') in cur.state_keys('fotmob_matches')
    del cur.calendar[(47, '2024/2025')]
    del cur.signals[CALENDAR][(47, '2024/2025')]
    cur.sql.clear()
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)

    assert rebuilt == 1
    assert cur.writes('fotmob_matches') == [
        "DELETE FROM iceberg.silver.fotmob_matches_a\n"
        "WHERE competition_id = 47 AND source_season_key IN ('2024/2025')"]
    assert cur.state_keys('fotmob_matches') == [(47, '2025/2026'), (47, '2026/2027')]


# 8. memory split -----------------------------------------------------------------

def _four_season_cursor(hook):
    keys = ['2022/2023', '2023/2024', '2024/2025', '2025/2026']
    return FakeCursor(
        calendar={(47, k): {k} for k in keys},
        signals={PAYLOADS: {(47, k): (TS1, 1) for k in keys}, CALENDAR: {}},
        hook=hook,
    )


def test_memory_error_splits_batch_in_half():
    fired = []

    def hook(cur, sql):
        if sql.startswith('INSERT INTO iceberg.silver') and not fired:
            fired.append(sql)
            raise RuntimeError('Query exceeded per-node memory limit: EXCEEDED_LOCAL_MEMORY_LIMIT')

    cur = _four_season_cursor(hook)
    _, rebuilt = _run_family(cur, _cfg(), 'match')

    assert rebuilt == 4
    inserts = [s for s in cur.writes('fotmob_matches') if s.startswith('INSERT')]
    assert [s.rsplit('IN ', 1)[1] for s in inserts] == [
        "('2022/2023', '2023/2024', '2024/2025', '2025/2026')",
        "('2022/2023', '2023/2024')",
        "('2024/2025', '2025/2026')",
    ]
    assert len(cur.state_keys('fotmob_matches')) == 4


def test_memory_error_on_single_partition_raises_and_keeps_state():
    def hook(cur, sql):
        if sql.startswith('INSERT INTO iceberg.silver') and "'2024/2025'" in sql:
            raise RuntimeError('EXCEEDED_LOCAL_MEMORY_LIMIT')

    cur = _four_season_cursor(hook)
    with pytest.raises(RuntimeError, match='EXCEEDED_LOCAL_MEMORY_LIMIT'):
        _run_family(cur, _cfg(), 'match')
    assert cur.state_keys('fotmob_matches') == [(47, '2022/2023'), (47, '2023/2024')]


def test_other_errors_are_not_split():
    def hook(cur, sql):
        if sql.startswith('INSERT INTO iceberg.silver'):
            raise RuntimeError('TABLE_NOT_FOUND')

    cur = _four_season_cursor(hook)
    with pytest.raises(RuntimeError, match='TABLE_NOT_FOUND'):
        _run_family(cur, _cfg(), 'match')
    assert len([s for s in cur.sql if s.startswith('INSERT INTO iceberg.silver')]) == 1
    assert cur.state_keys('fotmob_matches') == []


# season context family ------------------------------------------------------------

def test_leaderboards_partition_signal_merges_both_views_and_scopes_by_season():
    cur = FakeCursor(signals={
        'fotmob_leaderboards_current': {(47, '2025/2026'): (TS1, 50)},
        'fotmob_leaderboard_categories_current': {(47, '2025/2026'): (TS2, 5)},
    })
    _, rebuilt = _run_family(cur, _cfg(('fotmob_leaderboards',)), 'season')

    assert rebuilt == 1
    _, delete, insert = cur.writes('fotmob_leaderboards', 'a')
    scope = "competition_id = 47 AND source_season_key IN ('2025/2026')"
    assert insert.count(scope) == 3  # placeholder twice + wrapper
    assert cur.state[0]['signal_rows'] == 55 and cur.state[0]['signal_max_observed_at'] == TS2


# 9. global tables --------------------------------------------------------------

def test_players_build_in_eight_buckets_then_skip_then_full_rebuild_clears():
    cur = FakeCursor(global_signals={'fotmob_player_snapshots_current': (TS1, 1000)})
    cfg = _cfg(('fotmob_players',))
    _, rebuilt = _run_family(cur, cfg, 'global')

    writes = [s for s in cur.writes('fotmob_players') if s.startswith('INSERT')]
    assert rebuilt == 1 and len(writes) == 8
    assert all(w.startswith('INSERT INTO') for w in writes)
    assert [re.search(r'mod\(player_id, 8\) = (\d)', w).group(1) for w in writes] == list('01234567')
    assert cur.state_keys('fotmob_players') == [(None, None)]

    cur.sql.clear()
    assert build.build_family(cur, cfg, 'global', 'a', 'run-2', expected_revision=0) == 0
    assert cur.writes() == []

    cur.sql.clear()
    full = _cfg(('fotmob_players',), full_rebuild=True)
    _, rebuilt = _run_family(cur, full, 'global', run_id='run-3')
    assert cur.writes('fotmob_players')[0] == 'DELETE FROM iceberg.silver.fotmob_players_a'
    assert rebuilt == 1 and len(cur.writes('fotmob_players')) == 10


def test_entity_buckets_override_and_single_bucket_default():
    cur = FakeCursor(global_signals={'fotmob_team_snapshots_current': (TS1, 10),
                                     'fotmob_transfer_events_current': (TS1, 10)})
    cfg = _cfg(('fotmob_teams', 'fotmob_transfers'), entity_buckets={'fotmob_teams': 3})
    _run_family(cur, cfg, 'global')
    inserts = [s for s in cur.writes('fotmob_teams') if s.startswith('INSERT')]
    assert len(inserts) == 3
    assert 'mod(team_id, 3) = 2' in inserts[2]
    transfers, = [s for s in cur.writes('fotmob_transfers') if s.startswith('INSERT')]
    assert 'mod(' not in transfers


def test_missing_slot_table_is_rebuilt_despite_matching_state():
    cur = FakeCursor(global_signals={'fotmob_squad_snapshots_current': (TS1, 10)})
    cfg = _cfg(('fotmob_team_squads',))
    _run_family(cur, cfg, 'global')
    cur.tables.discard('fotmob_team_squads_a')
    with pytest.raises(StateInvariantError, match='missing'):
        build.build_family(cur, cfg, 'global', 'a', 'run-2', expected_revision=0)
    build.prepare_run(cur, cfg)
    assert build.build_family(cur, cfg, 'global', 'a', 'run-2', expected_revision=0) == 1


# 10. table list, templates, scopes -----------------------------------------------------

def test_all_tables_match_schema_yaml_and_sql_files():
    schema = yaml.safe_load((ROOT / 'configs/medallion/fotmob_silver/core_schema.yaml').read_text())
    tables = schema['tables']
    files = {p.name[:-len('.sql.j2')] for p in SQL_DIR.glob('fotmob_*.sql.j2')}
    assert set(build.ALL_TABLES) == set(tables) == files
    assert len(build.ALL_TABLES) == 22
    context = {t for t, spec in tables.items() if spec['scope'] == 'context'}
    assert context == set(build.MATCH_TABLES) | set(build.SEASON_CONTEXT_TABLES)
    assert set(build.GLOBAL_TABLES) == set(tables) - context
    for table in context:
        assert tables[table]['partitioning'] == list(build.PARTITION_COLUMNS)
    for table, spec in tables.items():
        assert set(spec['sources']) <= set(build.SIGNAL_SOURCES[table]), table


@pytest.mark.parametrize('table', build.ALL_TABLES)
def test_every_template_renders_with_private_scopes(table):
    sql = build.render_scoped_sql(
        table, SQL_DIR, calendar_scope='CAL_SCOPE', payload_scope='PAY_SCOPE',
        season_scope='SEASON_SCOPE', entity_scope='ENTITY_SCOPE',
    )
    assert '{{' not in sql and not sql.endswith(';')
    used = {s for s in ('CAL_SCOPE', 'PAY_SCOPE', 'SEASON_SCOPE', 'ENTITY_SCOPE') if s in sql}
    if table in build.MATCH_TABLES:
        assert used == {'CAL_SCOPE', 'PAY_SCOPE'}
    elif table in build.SEASON_CONTEXT_TABLES or table in build.SEASON_GLOBAL_TABLES:
        assert used == {'SEASON_SCOPE'}
    else:
        assert used == {'ENTITY_SCOPE'}


@pytest.mark.parametrize('table', ['fotmob_competitions', 'fotmob_competition_season_history'])
def test_seasonless_globals_are_scoped_by_competition_only(table):
    view = build.SIGNAL_SOURCES[table][0]
    cur = FakeCursor(global_signals={view: (TS1, 3)})
    _run_family(cur, _cfg((table,), competitions=(47, 112)), 'global')
    create, = [s for s in cur.writes(table) if s.startswith('INSERT')]
    assert 'competition_id IN (47, 112)' in create
    assert 'source_season_key IN' not in create
    signal = next(s for s in cur.sql if s.startswith('SELECT') and f'FROM iceberg.bronze.{view}' in s)
    assert "competition_id IN ('47', '112')" in signal  # raw varchar id in bronze


def test_competition_filter_narrows_signals_and_plan():
    cur = _match_cursor()
    _run_family(cur, _cfg(competitions=(47,)), 'match')
    reads = [s for s in cur.sql if 'FROM iceberg.bronze' in s]
    assert any("competition_id IN ('47')" in s for s in reads)  # payloads: varchar
    assert any('competition_id IN (47)' in s and 'card_key' in s for s in reads)  # calendar: bigint


def test_config_rejects_unknown_tables_and_bad_buckets():
    with pytest.raises(ValueError):
        build.BuildConfig(tables=('fotmob_nope',))
    with pytest.raises(ValueError):
        build.BuildConfig(entity_buckets={'fotmob_competitions': 4})
    with pytest.raises(ValueError):
        build.BuildConfig(max_partitions_per_batch=0)
    with pytest.raises(ValueError):
        build.BuildConfig(tables=())


# review round 1 ------------------------------------------------------------------

def _two_competition_cursor():
    cur = _match_cursor()
    cur.calendar[(112, '2026')] = {'2026'}
    cur.signals[CALENDAR][(112, '2026')] = (TS1, 10)
    cur.signals[PAYLOADS][(112, '2026')] = (TS1, 10)
    return cur


def test_competition_subset_keeps_other_competitions_in_slot():
    cur = _two_competition_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.signals[PAYLOADS][(47, '2026/2027')] = (TS2, 120)
    cur.sql.clear()
    rebuilt = build.build_family(cur, _cfg(competitions=(47,)), 'match', 'a', 'run-2',
                                 expected_revision=0)

    assert rebuilt == 1
    assert not any('competition_id = 112' in s for s in cur.writes('fotmob_matches'))
    assert (112, '2026') in cur.state_keys('fotmob_matches')


def test_missing_context_slot_table_resets_state_and_rebuilds():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.tables.discard('fotmob_matches_a')
    cur.sql.clear()
    with pytest.raises(StateInvariantError, match='missing'):
        build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)
    build.prepare_run(cur, _cfg())
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)

    assert rebuilt == 2
    reset = next(i for i, s in enumerate(cur.sql)
                 if s.startswith('DELETE FROM iceberg.ops.fotmob_silver_build_state')
                 and 'competition_id' not in s)
    create = next(i for i, s in enumerate(cur.sql)
                  if s.startswith('CREATE TABLE IF NOT EXISTS iceberg.silver.fotmob_matches_a'))
    assert reset < create
    assert _deletes_inserts(cur, 'fotmob_matches') == ['DELETE', 'INSERT']


def test_global_signal_change_rebuilds_and_unchanged_writes_nothing():
    cur = FakeCursor(global_signals={'fotmob_squad_snapshots_current': (TS1, 10)})
    cfg = _cfg(('fotmob_team_squads',))
    _run_family(cur, cfg, 'global')
    cur.sql.clear()
    assert build.build_family(cur, cfg, 'global', 'a', 'run-2', expected_revision=0) == 0
    assert cur.writes() == []
    cur.global_signals['fotmob_squad_snapshots_current'] = (TS2, 10)
    assert build.build_family(cur, cfg, 'global', 'a', 'run-3', expected_revision=0) == 1
    delete, insert = cur.writes('fotmob_team_squads')
    assert delete == 'DELETE FROM iceberg.silver.fotmob_team_squads_a'
    assert insert.startswith('INSERT INTO iceberg.silver.fotmob_team_squads_a')


def test_player_snapshot_signal_is_scanned_once_per_family():
    cur = FakeCursor(global_signals={'fotmob_player_snapshots_current': (TS1, 1000)})
    cfg = _cfg(('fotmob_players', 'fotmob_player_career', 'fotmob_player_market_values'))
    _run_family(cur, cfg, 'global')
    scans = [s for s in cur.sql if 'FROM iceberg.bronze.fotmob_player_snapshots_current' in s
             and s.startswith('SELECT')]
    assert len(scans) == 1


@pytest.mark.parametrize('mode', ['build', 'rollback'])
def test_switch_refuses_partial_table_set(mode):
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.tables |= {f'{t}_a' for t in build.ALL_TABLES}
    cur.sql.clear()
    with pytest.raises(ValueError, match='all 22'):
        build.switch_reader(cur, _cfg(), mode=mode, expected_revision=0, actor='run-1')
    assert cur.sql == [] and cur.reader == [None, 0, 'bootstrap']


@pytest.mark.parametrize('table, family', [('fotmob_matches', 'match'), ('fotmob_team_squads', 'global')])
def test_changed_template_clears_slot_table_and_rebuilds_in_full(table, family):
    cur = _match_cursor()
    cur.global_signals['fotmob_squad_snapshots_current'] = (TS1, 10)
    _run_family(cur, _cfg((table,)), family)
    for row in cur.state:
        row['sql_sha'] = 'old'
    cur.sql.clear()
    rebuilt = build.build_family(cur, _cfg((table,)), family, 'a', 'run-2', expected_revision=0)

    assert cur.writes(table)[0] == f'DELETE FROM iceberg.silver.{table}_a'
    assert {f'{table}_a', f'{table}_b'} <= cur.tables
    assert rebuilt == (2 if family == 'match' else 1)
    assert {r['sql_sha'] for r in cur.state} == {build.template_sha(_cfg(), table)}


def test_rollback_refused_after_a_build_wrote_into_target_slot():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    _switch(cur, 0, 'run-1', 'a')
    _run_family(cur, _cfg(), 'match', run_id='run-2')
    _switch(cur, 1, 'run-2', 'b')
    cur.signals[PAYLOADS][(47, '2026/2027')] = (TS2, 120)
    _run_family(cur, _cfg(), 'match', run_id='run-3')  # writes into a after the switch to b
    with pytest.raises(RuntimeError, match='rollback refused: a build started on slot a'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='run-4')
    assert cur.reader == ['b', 2, 'run-2']


def _two_switches(cur):
    _run_family(cur, _cfg(), 'match')
    _switch(cur, 0, 'run-1', 'a')
    _run_family(cur, _cfg(), 'match', run_id='run-2')
    _switch(cur, 1, 'run-2', 'b')


def test_rollback_invalidates_abandoned_slot_so_next_build_clears_and_rebuilds():
    cur = _match_cursor()
    cur.calendar[(47, '2024/2025')] = {'2024/2025'}
    cur.signals[CALENDAR][(47, '2024/2025')] = (TS1, 380)
    _two_switches(cur)
    result = build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='run-3')

    assert result['to_slot'] == 'a' and cur.reader == ['a', 3, 'run-3']
    update, = [s for s in cur.sql if s.startswith('UPDATE iceberg.ops.fotmob_silver_build_state')]
    assert update.endswith("WHERE slot = 'b'")
    assert {r['sql_sha'] for r in cur.state if r['slot'] == 'b'} == {'rolled_back'}
    assert {r['sql_sha'] for r in cur.state if r['slot'] == 'a'} == {
        build.template_sha(_cfg(), 'fotmob_matches')}

    del cur.calendar[(47, '2024/2025')]  # season vanishes from bronze meanwhile
    del cur.signals[CALENDAR][(47, '2024/2025')]
    cur.sql.clear()
    run, rebuilt = _run_family(cur, _cfg(), 'match', run_id='run-4')
    assert run['build_slot'] == 'b' and rebuilt == 2
    assert cur.writes('fotmob_matches', 'b')[0] == 'DELETE FROM iceberg.silver.fotmob_matches_b'
    assert not any(s.startswith('CREATE TABLE') for s in cur.writes())
    assert cur.state_keys('fotmob_matches', 'b') == [(47, '2025/2026'), (47, '2026/2027')]


def test_rollback_of_rollback_before_next_build_is_allowed():
    cur = _match_cursor()
    _two_switches(cur)
    build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='run-3')
    result = build.switch_reader(cur, FULL, mode='rollback', expected_revision=3, actor='run-4')
    assert result['to_slot'] == 'b' and cur.reader == ['b', 4, 'run-4']


def _fail_on(fragment):
    def hook(cur, sql):
        if sql.startswith(fragment[0]) and all(f in sql for f in fragment[1:]):
            raise RuntimeError('worker died')
    return hook


def test_rollback_refused_after_build_failed_before_any_state_row():
    cur = _match_cursor()
    _two_switches(cur)
    cur.signals[PAYLOADS][(47, '2026/2027')] = (TS2, 120)
    cur.hook = _fail_on(('INSERT INTO iceberg.silver.fotmob_matches_a',))
    with pytest.raises(RuntimeError, match='worker died'):
        _run_family(cur, _cfg(), 'match', run_id='run-3')  # DELETE landed, INSERT did not
    cur.hook = None
    assert cur.markers['a']['run_id'] == 'run-3'
    with pytest.raises(RuntimeError, match='rollback refused'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='run-4')
    assert cur.reader == ['b', 2, 'run-2']


def test_rollback_refused_after_partial_global_buckets():
    cur = _match_cursor(global_signals={'fotmob_player_snapshots_current': (TS1, 1000)})
    cfg = _cfg(('fotmob_players',))
    _run_family(cur, cfg, 'global')
    _switch(cur, 0, 'run-1', 'a')
    _run_family(cur, cfg, 'global', run_id='run-2')
    _switch(cur, 1, 'run-2', 'b')
    cur.global_signals['fotmob_player_snapshots_current'] = (TS2, 1001)
    cur.hook = _fail_on(('INSERT INTO iceberg.silver.fotmob_players_a', 'mod(player_id, 8) = 3'))
    with pytest.raises(RuntimeError, match='worker died'):
        _run_family(cur, cfg, 'global', run_id='run-3')
    cur.hook = None
    with pytest.raises(RuntimeError, match='rollback refused'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='run-4')


def test_build_marker_once_per_run_and_resume_unaffected():
    cur = _match_cursor(global_signals={'fotmob_squad_snapshots_current': (TS1, 10)})
    cfg = _cfg(('fotmob_matches', 'fotmob_shots', 'fotmob_team_squads'))
    run = build.prepare_run(cur, cfg)
    for family in ('match', 'global'):
        build.build_family(cur, cfg, family, 'a', 'run-1', expected_revision=0)
    marker_inserts = [s for s in cur.sql if s.startswith('MERGE INTO iceberg.ops')
                      and build.BUILD_MARKER in s]
    assert run['build_slot'] == 'a' and len(marker_inserts) == 1
    assert cur.markers['a']['run_id'] == 'run-1'
    assert all(r['table_name'] != build.BUILD_MARKER for r in cur.state)
    cur.sql.clear()
    assert build.build_family(cur, cfg, 'match', 'a', 'run-2', expected_revision=0) == 0
    assert cur.writes() == [] and cur.markers['a']['run_id'] == 'run-2'


def test_full_rebuild_replaces_build_marker_before_clearing():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.markers['a']['run_id'] == '__full_rebuild__'
    assert cur.state_keys('fotmob_matches') == []


def test_only_ingested_at_change_rebuilds_partition():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.signals[PAYLOADS][(47, '2025/2026')] = (TS1, TS2, 380)
    cur.sql.clear()
    rebuilt = build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)

    assert rebuilt == 1
    delete, _ = cur.writes('fotmob_matches')
    assert delete.endswith("source_season_key IN ('2025/2026')")
    row = next(r for r in cur.state if r['source_season_key'] == '2025/2026')
    assert (row['signal_max_observed_at'], row['signal_max_ingested_at']) == (TS1, TS2)


@pytest.mark.parametrize('table', ['fotmob_matches', 'fotmob_players'])
def test_bootstrap_creates_both_empty_schemas_before_views(table):
    cur = FakeCursor()
    cfg = _cfg((table,))
    build.prepare_run(cur, cfg)
    creates = cur.writes(table)
    assert len(creates) == 2
    assert all(s.startswith('CREATE TABLE IF NOT EXISTS') and s.endswith('WITH NO DATA') for s in creates)
    assert all(('partitioning' in s) == (table == 'fotmob_matches') for s in creates)
    view_index = next(i for i, s in enumerate(cur.sql) if s.startswith('CREATE OR REPLACE VIEW'))
    assert all(cur.sql.index(s) < view_index for s in creates)
    assert build.slot_spec(cfg).state_selected is True
    assert build.slot_spec(cfg).tables == (table,)


def test_missing_active_table_fails_before_any_slot_creation():
    cur = FakeCursor()
    cur.reader = ['b', 3, 'old']
    with pytest.raises(RuntimeError, match='active slot table missing'):
        build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.writes() == []
    assert cur.reader == ['b', 3, 'old']


@pytest.mark.parametrize('columns', [
    [('renamed', 'bigint'), ('value', 'varchar')],
    [('id', 'varchar'), ('value', 'varchar')],
    [('value', 'varchar'), ('id', 'bigint')],
])
def test_template_schema_change_fails_before_clear_even_with_same_column_count(columns):
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    before = list(cur.state)
    tables = set(cur.tables)
    cur.output_columns = columns
    cur.sql.clear()
    with pytest.raises(RuntimeError, match='explicit migration required'):
        build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.writes() == [] and cur.state == before and cur.tables == tables
    assert any(s.startswith('DESCRIBE OUTPUT (') for s in cur.sql)


def test_incompatible_physical_slots_block_rebuild_before_delete():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.columns['fotmob_matches_b'] = [('wrong', 'bigint')]
    cur.sql.clear()
    with pytest.raises(StateInvariantError, match='incompatible'):
        build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.writes() == []


@pytest.mark.parametrize('full_rebuild', [False, True])
def test_legacy_view_on_inactive_slot_blocks_all_data_writes(full_rebuild):
    cur = _match_cursor()
    _two_switches(cur)
    cur.views['fotmob_matches'] = 'SELECT * FROM iceberg.silver.fotmob_matches_a'
    cur.sql.clear()
    with pytest.raises(StateInvariantError, match='unsafe reader view'):
        if full_rebuild:
            build.prepare_run(cur, _cfg(full_rebuild=True))
        else:
            build.build_family(cur, _cfg(), 'match', 'a', 'run-3', expected_revision=2)
    assert cur.writes() == []
    assert cur.reader == ['b', 2, 'run-2']


def test_full_rebuild_failure_preserves_tables_and_prevents_rollback():
    cur = _match_cursor()
    _two_switches(cur)
    before = set(cur.tables)
    cur.hook = _fail_on(('DELETE FROM iceberg.silver.fotmob_matches_a',))
    with pytest.raises(RuntimeError, match='worker died'):
        build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.tables == before
    assert cur.markers['a']['run_id'] == '__full_rebuild__'
    cur.hook = None
    with pytest.raises(RuntimeError, match='rollback refused'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='rollback')
    assert not any(s.startswith(('DROP TABLE', 'CREATE OR REPLACE TABLE')) for s in cur.sql)


@pytest.mark.parametrize('full_rebuild', [False, True])
def test_failed_marker_refresh_keeps_prior_partial_build_protected(full_rebuild):
    cur = _match_cursor()
    _two_switches(cur)
    build.mark_build(cur, _cfg(), 'a', 'partial-build')
    previous = dict(cur.markers['a'])
    cur.hook = _fail_on(('MERGE INTO iceberg.ops.fotmob_silver_build_state',))
    with pytest.raises(RuntimeError, match='worker died'):
        if full_rebuild:
            build.prepare_run(cur, _cfg(full_rebuild=True))
        else:
            build.build_family(cur, _cfg(), 'match', 'a', 'new-run', expected_revision=2)
    assert cur.markers['a'] == previous
    cur.hook = None
    with pytest.raises(RuntimeError, match='rollback refused'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='rollback')


@pytest.mark.parametrize('table,family', [('fotmob_matches', 'match'), ('fotmob_players', 'global')])
def test_failed_rebuild_never_removes_physical_tables(table, family):
    cur = _match_cursor(global_signals={'fotmob_player_snapshots_current': (TS1, 1000)})
    cfg = _cfg((table,))
    _run_family(cur, cfg, family)
    for row in cur.state:
        row['sql_sha'] = 'old'
    before = set(cur.tables)
    cur.hook = _fail_on((f'INSERT INTO iceberg.silver.{table}_a',))
    with pytest.raises(RuntimeError, match='worker died'):
        build.build_family(cur, cfg, family, 'a', 'run-2', expected_revision=0)
    assert cur.tables == before
    assert not any(s.startswith(('DROP TABLE', 'CREATE OR REPLACE TABLE')) for s in cur.sql)


def test_rollback_after_first_publication_refuses_empty_bootstrap_slot():
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    _switch(cur, 0, 'run-1', 'a')
    with pytest.raises(RuntimeError, match='not been published yet'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=1, actor='rollback')
    assert cur.reader == ['a', 1, 'run-1']


def test_recreated_inactive_table_is_marked_ineligible_for_rollback():
    cur = _match_cursor()
    _two_switches(cur)
    cur.tables.remove('fotmob_matches_a')
    build.prepare_run(cur, _cfg())
    assert 'fotmob_matches_a' in cur.tables
    assert cur.markers['a']['run_id'] == '__schema_recovery__'
    with pytest.raises(RuntimeError, match='rollback refused'):
        build.switch_reader(cur, FULL, mode='rollback', expected_revision=2, actor='rollback')


def test_template_varchar_bounds_match_iceberg_storage_including_nested_types():
    cur = FakeCursor()
    cur.output_columns = [('id', 'bigint'), ('value', 'row(label varchar(4), tags array(varchar(6)))')]
    for slot in ('a', 'b'):
        cur.columns[f'fotmob_matches_{slot}'] = [
            ('id', 'bigint'), ('value', 'row(label varchar, tags array(varchar))')]
    build.prepare_run(cur, _cfg())


@pytest.mark.parametrize('full_rebuild', [False, True])
def test_reader_barrier_failure_blocks_data_mutations(monkeypatch, full_rebuild):
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.sql.clear()
    before = set(cur.tables)

    def fail_barrier(cursor):
        raise TimeoutError('older transaction still active')

    monkeypatch.setattr(build, 'drain_readers', fail_barrier)
    with pytest.raises(TimeoutError, match='older transaction'):
        if full_rebuild:
            build.prepare_run(cur, _cfg(full_rebuild=True))
        else:
            build.build_family(cur, _cfg(), 'match', 'a', 'run-2', expected_revision=0)
    assert cur.writes() == [] and cur.tables == before


def test_full_rebuild_rechecks_revision_after_reader_barrier(monkeypatch):
    cur = _match_cursor()
    _run_family(cur, _cfg(), 'match')
    cur.sql.clear()

    def moved(cursor):
        cursor.reader = ['a', 1, 'other']

    monkeypatch.setattr(build, 'drain_readers', moved)
    with pytest.raises(RevisionConflict):
        build.prepare_run(cur, _cfg(full_rebuild=True))
    assert cur.writes() == []


@pytest.mark.parametrize('stop_after_insert', [1, 2])
def test_night_window_interruption_keeps_completed_batches_and_resumes(monkeypatch, stop_after_insert):
    from datetime import datetime
    from utils.fotmob_silver_admission import AdmittedCursor, AdmissionClosed, LOAD_SQL, MSK

    raw = _match_cursor()
    answer = raw._answer
    monkeypatch.setattr(raw, '_answer', lambda sql: [(0,)] if sql == LOAD_SQL else
                        [] if sql.startswith('SET SESSION') else answer(sql))
    clock = [datetime(2026, 11, 6, 1, 0, tzinfo=MSK)]
    inserts = []

    def expire_after_insert(cur, sql):
        if sql.startswith('INSERT INTO iceberg.silver.fotmob_matches_a'):
            inserts.append(sql)
            if len(inserts) == stop_after_insert:
                clock[0] = datetime(2026, 11, 6, 2, 45, tzinfo=MSK)

    raw.hook = expire_after_insert
    cur = AdmittedCursor(raw, now=lambda: clock[0])
    cfg = _cfg(max_partitions_per_batch=1)
    build.prepare_run(cur, cfg)
    with pytest.raises(AdmissionClosed):
        build.build_family(cur, cfg, 'match', 'a', 'night-1', expected_revision=0)
    # INSERT may have committed, but no state was recorded for its interrupted batch.
    assert len(raw.state_keys('fotmob_matches')) == stop_after_insert - 1
    assert raw.reader[:2] == [None, 0]
    raw.hook = None
    raw.sql.clear()
    clock[0] = datetime(2026, 11, 7, 0, 30, tzinfo=MSK)
    build.prepare_run(cur, cfg)
    assert build.build_family(cur, cfg, 'match', 'a', 'night-2', expected_revision=0) == 3 - stop_after_insert
    assert raw.state_keys('fotmob_matches') == [(47, '2025/2026'), (47, '2026/2027')]
    assert len([s for s in raw.sql if s.startswith('INSERT INTO iceberg.silver.fotmob_matches_a')]) == 3 - stop_after_insert
    assert raw.reader[:2] == [None, 0]


# standalone path import (shared scheduler) ------------------------------------------

STANDALONE_PATHS = {
    'fotmob_silver_build': ROOT / 'dags' / 'utils' / 'fotmob_silver_build.py',
    'silver_slots': ROOT / 'dags' / 'utils' / 'silver_slots.py',
}
STANDALONE_EXPORTS = {
    'fotmob_silver_build': ('BuildConfig', 'switch_reader'),
    'silver_slots': ('SlotSpec', 'switch_reader'),
}


@pytest.mark.parametrize('name', sorted(STANDALONE_PATHS))
def test_util_module_imports_standalone_from_file_path(monkeypatch, name):
    """The shared scheduler reads utility files as standalone DAG files
    (``.airflowignore`` contains only ``^$``): loading by path without a
    parent package must not raise a relative-import ``ImportError``."""
    path = STANDALONE_PATHS[name]
    assert path.exists()
    spec = importlib.util.spec_from_file_location(f'standalone_{name}', path)
    module = importlib.util.module_from_spec(spec)
    # dataclasses consult sys.modules[cls.__module__]; monkeypatch restores sys.modules.
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    for attr in STANDALONE_EXPORTS[name]:
        assert hasattr(module, attr)
