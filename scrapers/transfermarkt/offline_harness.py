"""Owned-file/process exercises and deliberately synthetic interval fixtures.

No Iceberg/Trino connection or source transport exists in this module. The local
JSON store exercises locking/intent durability, not warehouse commit semantics.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
import signal
from pathlib import Path
import tempfile
import time

from .offline_acceptance import KINDS, TABLES, digest, validate


def _write(path, value):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as output:
        json.dump(value, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def _wait_owned(pid, expected=0, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            if os.waitstatus_to_exitcode(status) != expected:
                raise RuntimeError(f'owned child {pid} exited unexpectedly: {os.waitstatus_to_exitcode(status)}')
            return
        time.sleep(.01)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise RuntimeError(f'owned child {pid} exceeded local harness deadline')


@contextmanager
def _owned_processes():
    """Reap exactly the children created by this block, including error paths."""
    children = []
    try:
        yield children
    finally:
        for pid in children:
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
                if not done:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
            except ChildProcessError:
                pass  # Already reaped by _wait_owned().


def local_exercises(scratch_parent):
    """Fork only owned children; keep durable proof before temporary cleanup."""
    from dags.utils.transfermarkt_history_circuit import HistoryTransportCircuit
    from .career_refs import CapturePredicate, apply_physical_refs
    from .write_intents import pending_intents, unpack_frames
    receipts = []
    with tempfile.TemporaryDirectory(prefix='tm-d4-owned-', dir=scratch_parent) as directory:
        root = Path(directory)
        lock = root / 'writer.lock'
        lock.touch()
        stat = lock.stat()
        lock_id = f'{stat.st_dev}:{stat.st_ino}'
        original = '2026-10-01T00:00:00+00:00'
        store = root / 'store.json'
        initial = {table: {'version': 0, 'rows': [{'key': k, 'clock': original, 'value': 0} for k in ('p1', 'p2')]} for table in TABLES}
        _write(store, initial)
        # Pipe barrier starts two independent writer processes together.
        read_fd, write_fd = os.pipe()
        with _owned_processes() as children:
            for index in (1, 2):
                pid = os.fork()
                if pid == 0:
                    try:
                        os.close(write_fd)
                        os.read(read_fd, 1)
                        os.close(read_fd)
                        os.environ['TM_WRITER_LOCK_PATH'] = str(lock)
                        os.environ['TM_WRITER_LOCK_SHARED_FILE_ID'] = lock_id
                        from .writer import shared_writer_lock_ready, writer_lock
                        output = []
                        for table in sorted(TABLES):
                            with writer_lock():
                                assert shared_writer_lock_ready()
                                started = time.monotonic_ns()
                                data = json.loads(store.read_text())
                                before = digest(data[table])
                                data[table]['version'] += 1
                                for row in data[table]['rows']:
                                    row['value'] += index
                                _write(store, data)
                                after = digest(data[table])
                                output.append({'writer_id': f'writer-{index}', 'table': table, 'lock_file_id': lock_id,
                                               'before_snapshot': before, 'after_snapshot': after,
                                               'keys': [r['key'] for r in data[table]['rows']],
                                               'clocks': {r['key']: r['clock'] for r in data[table]['rows']},
                                               'entered_ns': started, 'exited_ns': time.monotonic_ns()})
                        _write(root / f'writer-{index}.json', output)
                        os._exit(0)
                    except BaseException:
                        os._exit(90)
                children.append(pid)
            os.close(read_fd)
            os.write(write_fd, b'12')
            os.close(write_fd)
            child_errors = []
            for pid in children:
                try:
                    _wait_owned(pid)
                except RuntimeError as exc:
                    child_errors.append(str(exc))
            if child_errors:
                raise RuntimeError('; '.join(child_errors))
        for index in (1, 2):
            receipts.extend(json.loads((root / f'writer-{index}.json').read_text()))
        ordered = sorted(receipts, key=lambda r: r['entered_ns'])
        if any(a['exited_ns'] > b['entered_ns'] for a, b in zip(ordered, ordered[1:])):
            raise RuntimeError('owned writer critical sections overlap')
        final = json.loads(store.read_text())
        if any(d['version'] != 2 or len(d['rows']) != 2 or any(r['value'] != 3 or r['clock'] != original for r in d['rows']) for d in final.values()):
            raise RuntimeError('owned parallel file update lost keys/clocks')
        # A real child leaves an fsynced immutable parsed bundle and exits abruptly.
        crash_pid = os.fork()
        if crash_pid == 0:
            try:
                os.environ['TM_WRITE_INTENT_DIR'] = str(root / 'intents')
                import pandas as pd
                from .write_intents import save_intent
                frames = {table: pd.DataFrame(final[table]['rows']) for table in sorted(TABLES)}
                save_intent({'fixture': 'owned-crash'}, frames, original_capture_at=original)
                os._exit(17)
            except BaseException:
                os._exit(91)
        _wait_owned(crash_pid, expected=17)
        # Override only while reading our private intent directory; restore caller env.
        previous = os.environ.get('TM_WRITE_INTENT_DIR')
        try:
            os.environ['TM_WRITE_INTENT_DIR'] = str(root / 'intents')
            pending = pending_intents({'fixture': 'owned-crash'})
            if len(pending) != 1:
                raise RuntimeError('crash lost original immutable intent')
            intent_path, payload = pending[0]
            restored = unpack_frames(payload['frames'])
            replay = {table: frame.to_dict('records') for table, frame in restored.items()}
            if any(replay[table] != final[table]['rows'] for table in TABLES):
                raise RuntimeError('crash replay changed original data')
            _write(root / 'replayed.json', replay)
            immutable_intent_sha = intent_path.stem
            if digest(json.loads((root / 'replayed.json').read_text())) != digest(replay):
                raise RuntimeError('offline replay readback differs')
        finally:
            if previous is None:
                os.environ.pop('TM_WRITE_INTENT_DIR', None)
            else:
                os.environ['TM_WRITE_INTENT_DIR'] = previous
        # Maintenance uses the exact same writer helper in its own child.
        maintenance_pid = os.fork()
        if maintenance_pid == 0:
            try:
                os.environ['TM_WRITER_LOCK_PATH'] = str(lock)
                os.environ['TM_WRITER_LOCK_SHARED_FILE_ID'] = lock_id
                from .writer import shared_writer_lock_ready, writer_lock
                with writer_lock():
                    assert shared_writer_lock_ready()
                    data = json.loads(store.read_text())
                    _write(root / 'compacted.json', data)
                    _write(root / 'maintenance.json', {'lock_file_id': lock_id, 'tables': sorted(data), 'files': 1,
                                                       'bytes': (root / 'compacted.json').stat().st_size,
                                                       'keys_unchanged': digest(data) == digest(final)})
                os._exit(0)
            except BaseException:
                os._exit(92)
        _wait_owned(maintenance_pid)
        maintenance = json.loads((root / 'maintenance.json').read_text())
        # Real persistent circuit API, driven by explicit fixture time. No GET.
        circuit = HistoryTransportCircuit(root / 'circuit')
        now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        for index in range(3):
            circuit.feedback('history-0', attempt_id=f'failure-{index}', scope_id=f'scope-{index}',
                             transport_error=True, now=now + timedelta(seconds=index))
        blocked = circuit.admit('history-0', now=now + timedelta(seconds=3))
        recovery = circuit.admit('history-0', now=now + timedelta(seconds=902), reserve=True, owner='owned-probe')
        second_probe = circuit.admit('history-0', now=now + timedelta(seconds=902), reserve=True, owner='other')
        circuit.feedback('history-0', attempt_id='recovery-1', scope_id='scope-0', transport_error=False,
                         recovery_valid=True, now=now + timedelta(seconds=903))
        healed = circuit.admit('history-0', now=now + timedelta(seconds=904))
        if blocked[:2] != (False, False) or recovery[:2] != (True, True) or second_probe[0] or healed[:2] != (True, False):
            raise RuntimeError('local circuit scenario failed')
        # Existing physical-ref renderer only; no cursor or SQL execution.
        predicate = CapturePredicate(snapshot_id=11, legacy_snapshot_id=12)
        predicate.text = "(player_id = 'p1' AND _batch_id = 'original')"
        rendered = apply_physical_refs("SELECT * FROM iceberg.bronze.transfermarkt_market_value_points WHERE _batch_id = 'logical'",
                                       table='transfermarkt_market_value_points', batch='logical', predicate=predicate)
        if 'FOR VERSION AS OF 11' not in rendered or "_batch_id = 'original'" not in rendered:
            raise RuntimeError('physical ref renderer changed original identity')
        return {'scope': 'owned_local_files_processes_only', 'runtime_status': 'UNKNOWN', 'actual_acceptance_start': None,
                'lock_file_id': lock_id, 'writer_receipts': receipts, 'writer_count': 2,
                'critical_sections_nonoverlapping': True, 'maintenance': maintenance,
                'crash_exit_code': 17, 'immutable_intent_sha256': immutable_intent_sha,
                'replay_data_sha256': digest(replay), 'source_http_attempts': 0,
                'circuit_fixture_time': now.isoformat(), 'circuit_journal': json.loads((root / 'circuit/history-0.json').read_text()),
                'limitations': ['JSON file store is not an Iceberg commit.', 'Circuit time and successful recovery feedback are synthetic.',
                                'No live fault, Trino, lease TTL or season transition qualification.',
                                'No production compaction or protected-anchor mutation.']}


def fixture(revision='b1be619f4b7ba9aa4a908370ae98c0e5339f8551'):
    """A synthetic complete shape; elapsed fixture time never represents runtime."""
    origin = datetime(2026, 10, 1, tzinfo=timezone.utc)
    def at(seconds):
        return (origin + timedelta(seconds=seconds)).isoformat()
    workload = [{'id': f'w{i}', 'actor': f'actor-{i % 2}', 'lane': 'current' if i % 2 == 0 else 'history',
                 'policy': f'policy-{i % 2}', 'scope': f'scope-{i}', 'url': f'https://fixture.invalid/{i}',
                 'original_capture_at': at(0)} for i in range(20)]
    manifest = {'schema': 1, 'provenance': 'synthetic_fixture', 'revision': revision,
                'workload': workload, 'workload_sha256': digest(workload), 'windows': [],
                'writer_ids': ['writer-1', 'writer-2'], 'lock_file_id': '1:2',
                'writer_manifest': {table: {'keys': ['p1', 'p2'], 'original_clocks': {'p1': at(0), 'p2': at(0)}} for table in sorted(TABLES)},
                'lease_manifest': {}, 'batch_manifest': {}, 'sampling_seconds': 60,
                'producer': {'id': 'synthetic-fixture-generator-v1', 'sequence_start': 1, 'sequence_end': 0}}
    events = []
    for wi, role in enumerate(('baseline', 'canary1', 'canary2', 'writer', 'history')):
        offset = wi * 86400
        window = {'id': role, 'role': role, 'start': at(offset), 'end': at(offset + 86400),
                  'total_streams': 1 if role == 'baseline' else 2, 'attempt_owners': {}, 'inventory': {kind: [] for kind in KINDS}}
        manifest['windows'].append(window)
        local = []
        def add(kind, identity, **values):
            row = dict(kind=kind, window=role, id=identity, **values)
            window['inventory'][kind].append(identity)
            local.append(row)
        if wi < 3:
            for i in range(10 if role == 'baseline' else 20):
                add('attempt', f'a{i}', at=at(offset + i + 1), work_id=f'w{i}', outcome='success', provider_bytes=100, portion_id=None)
                add('completion', f'c{i}', at=at(offset + i + 2), work_id=f'w{i}', mode='paid')
            add('query', 'q1', at=at(offset + 1), query_id=f'{role}-q1', peak_memory_bytes=64)
        if role == 'writer':
            for table in sorted(TABLES):
                expected = manifest['writer_manifest'][table]
                for writer in ('writer-1', 'writer-2', 'maintenance'):
                    add('writer', table + writer, at=at(offset + 1), writer_id=writer,
                        participant='maintenance' if writer == 'maintenance' else 'writer', lock_file_id='1:2', table=table,
                        before_snapshot=digest([table, 0]), after_snapshot=digest([table, writer]),
                        expected_keys=expected['keys'], observed_keys=expected['keys'],
                        expected_clocks=expected['original_clocks'], observed_clocks=expected['original_clocks'])
            for name in ('crash_replay', 'cache_replay', 'empty', 'multiunit', 'compaction'):
                keys = [] if name == 'empty' else ['p1', 'p2']
                clocks = {k: at(0) for k in keys}
                payload = {'before_keys': keys, 'after_keys': keys,
                    'original_clocks': clocks, 'observed_clocks': clocks, 'files': 2, 'bytes': 128,
                    'protected_before': ['original-anchor'], 'protected_after': ['original-anchor']}
                if name == 'compaction':
                    payload['files'] = {table: 2 for table in TABLES}
                    payload['bytes'] = {table: 128 for table in TABLES}
                    payload['wall_seconds'] = .125
                add('scenario', name, name=name, payload=payload)
        if role == 'history':
            for i in range(3):
                sec = offset + i * 3000
                add('portion', f'p{i}', start=at(sec), end=at(sec + (1000 if i == 0 else 100)))
                add('task', f't{i}', start=at(sec), end=at(sec + (1000 if i == 0 else 100)), portion_id=f'p{i}')
                add('attempt', f'a{i}', at=at(sec + 1), work_id=f'w{i}', outcome='success', provider_bytes=100, portion_id=f'p{i}')
                original = {'actor': f'actor-{i}', 'grant_id': f'grant-{i}', 'acquired_at': at(sec),
                            'expires_at': at(sec + (1200 if i == 0 else 120)), 'grant_sha256': digest(['grant', i])}
                manifest['lease_manifest'][f'l{i}'] = original
                add('lease', f'l{i}', **original, closed_at=at(sec + (999 if i == 0 else 99)), reconciliation_actor=original['actor'])
                add('current', f'current{i}', enqueued_at=at(sec + 1), started_at=at(sec + 1), history_wait_seconds=0)
                units = [dict(work_id='w0', raw_id='original-raw-0', raw_sha256=digest('raw-0'), capture_id='capture-0',
                              physical_unit_id='snapshot11:original-batch-0', native_batch_id='original-batch-0',
                              table='transfermarkt_market_value_points', rows=2, kind='rows', empty_type=None, fetched_at=at(0), captured_at=at(0)),
                         dict(work_id='w2', raw_id='original-raw-2', raw_sha256=digest('raw-2'), capture_id='capture-2',
                              physical_unit_id='empty-proof:original-batch-2', native_batch_id='original-batch-2',
                              table='transfermarkt_transfer_events', rows=0, kind='empty', empty_type='transfer_events', fetched_at=at(0), captured_at=at(0))]
                manifest['batch_manifest'][f'b{i}'] = json.loads(json.dumps(units))
                add('batch', f'b{i}', at=at(sec + (1000 if i == 0 else 100)), sequence=i + 1, checkpoint_before=digest(['cp', i]),
                    checkpoint_after=digest(['cp', i + 1]), resumed_from=digest(['cp', i]), expected_remainder=2-i,
                    observed_remainder=2-i, expected_rows=2, observed_rows=2, duplicates=0, missing=0, raw_times=[u['fetched_at'] for u in units], capture_units=units)
            for i in range(3):
                add('attempt', f'f{i}', at=at(offset + i), work_id=f'w{i * 2}', outcome='transport', provider_bytes=10, portion_id='p0')
            add('attempt', 'recovery-paid', at=at(offset + 903), work_id='w0', outcome='success', provider_bytes=100, portion_id='p0')
            add('scenario', 'transport', name='transport', payload={
                'stream_id': 'history-0', 'actor': 'actor-0',
                'failures': [{'scope': f'scope-{i * 2}', 'at': at(offset + i), 'classification': 'transport', 'attempt_id': f'f{i}'} for i in range(3)],
                'paused_until': at(offset + 902), 'alert_at': at(offset + 2), 'source_before': 0, 'source_after': 0,
                'terminal_scopes': [], 'recovery_attempts': ['recovery-paid'], 'recovery_raw_at': at(offset + 903),
                'recovery_raw_sha256': digest('new fixture raw')})
            add('scenario', 'season_close', name='season_close', payload={
                'registry_before': [{'scope': 'old', 'competition': 'league', 'season': 2025}],
                'registry_after': [{'scope': 'new', 'competition': 'league', 'season': 2026}], 'old_scope': 'old',
                'expected_players': ['p1'], 'observed_players': ['p1'], 'career_players': ['p1'], 'coach_players': ['p1'],
                'completed_at': at(offset + 1), 'history_claim_at': at(offset + 2)})
        for minute in range(1440):
            start, end = at(offset + minute * 60), at(offset + (minute + 1) * 60)
            attempts = [a for a in local if a['kind'] == 'attempt' and start <= a['at'] < end]
            completions = [a for a in local if a['kind'] == 'completion' and start <= a['at'] < end]
            queries = [a for a in local if a['kind'] == 'query' and start <= a['at'] < end]
            add('coverage', f's{minute}', start=start, end=end, attempts=len(attempts), provider_bytes=sum(a['provider_bytes'] for a in attempts),
                completions=len(completions), rss_process_bytes=128, cgroup_memory_bytes=256, memory_limit_bytes=1024,
                process_identity='boot-id:pid42:startticks100', cgroup_identity='mount-id:device:inode',
                swap_bytes=0, trino_memory_bytes=max((q['peak_memory_bytes'] for q in queries), default=0), current_waiting=0,
                writer_observations={'writer-1': '1:2', 'writer-2': '1:2', 'maintenance': '1:2'} if role == 'writer' else {},
                table_checks={table: {'rows': len(v['keys']), 'duplicates': 0, 'key_sha256': digest(sorted(v['keys'])),
                                     'clocks_sha256': digest(v['original_clocks'])} for table, v in manifest['writer_manifest'].items()} if role == 'writer' else {})
        events.extend(local)
    owners = {w['id']: w['attempt_owners'] for w in manifest['windows']}
    for sequence, row in enumerate(events, 1):
        row['producer_sequence'] = sequence
        if row['kind'] == 'attempt':
            work = next(w for w in workload if w['id'] == row['work_id'])
            row['actor'] = work['actor']
            row['stream_id'] = 'history-0' if row['window'] == 'history' or row['window'] != 'baseline' and work['lane'] == 'history' else 'current-0'
            owners[row['window']][row['id']] = {k: row[k] for k in ('work_id', 'actor', 'stream_id')}
    manifest['producer']['sequence_end'] = len(events)
    return manifest, events


def write_fixture(path, manifest, events):
    with Path(path).open('x') as output:
        for row in (manifest, *events):
            output.write(json.dumps(row, sort_keys=True, allow_nan=False) + '\n')
