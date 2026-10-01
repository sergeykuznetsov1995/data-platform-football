"""Explicit isolated Iceberg benchmark using the production ESPN batch writer."""
from datetime import date, datetime, timezone
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import time
import uuid

from . import urls
from .bronze_rows import MatchPayload, RawRef
from .bronze_schema import BRONZE_DATABASE, PARTITION_SPEC, TABLES
from .bronze_writer import TournamentBatch, write_tournament_batch
from .denominator import load_denominator
from .editions import EditionState
from .raw_store import EspnRawStore
from .schedule_parser import schedule_row_from_header
from .summary_parser import parse_summary
from .transport import canonicalize_target
from .wave import build_competition


class IsolatedManager:
    """Only the native four-table write operation may enter the temporary schema."""
    def __init__(self, trino, schema):
        if re.fullmatch(r'espn_pace_bench_[0-9a-f]{32}', schema) is None:
            raise ValueError('unsafe benchmark schema')
        self.trino, self.schema = trino, schema

    def arrow_schema_to_trino(self, schema):
        return self.trino.arrow_schema_to_trino(schema)

    def insert_dataframe_atomic(self, database, table, frame, **kwargs):
        if database != BRONZE_DATABASE or table not in TABLES:
            raise ValueError('benchmark write outside native ESPN table set')
        if kwargs.get('single_statement_replace') is not True:
            raise ValueError('benchmark must use the native atomic write path')
        return self.trino.insert_dataframe_atomic(self.schema, table, frame, **kwargs)


def cached_payloads(raw, ids, records=None):
    competition, edition = build_competition(load_denominator().row('eng.1'),
        EditionState('eng.1', 2015, '2015', date(2015, 1, 1), date(2016, 12, 31)))
    payloads = []
    for event_id in ids:
        request = urls.summary('eng.1', event_id)
        target = canonicalize_target(request.url, request.params)
        if records is None:
            body, record = raw.load(target)
        else:
            from .raw_store import RawJsonRecord
            record = RawJsonRecord(**records[str(event_id)])
            body = raw.load_exact(record.raw_uri, record.content_hash)
        schedule = schedule_row_from_header(body, competition=competition, edition=edition)
        if schedule.event_id != event_id or not schedule.played_final:
            raise ValueError('benchmark payload differs from accepted final ID')
        payloads.append(MatchPayload(schedule, parse_summary(body, competition=competition,
                                                           edition=edition, event=schedule),
                                     RawRef(record.raw_uri, record.content_hash,
                                            datetime.fromisoformat(record.fetched_at))))
    return payloads


def run_benchmark(trino, writer, store, payloads, *, schema=None, repeats=3,
                  monotonic=time.monotonic, now=lambda: datetime.now(timezone.utc), step=None):
    schema = schema or 'espn_pace_bench_' + uuid.uuid4().hex
    adapter = IsolatedManager(trino, schema)
    if not payloads or type(repeats) is not int or repeats < 1:
        raise ValueError('benchmark requires payloads and positive repeat count')
    created = []
    owned = False
    timings = []
    store.record('benchmark', now().timestamp(), event='start', schema=schema)
    try:
        # No IF NOT EXISTS on the namespace: never adopt somebody else's tables.
        trino.execute_query(f'CREATE SCHEMA iceberg.{schema}')
        owned = True
        for table, arrow_schema in TABLES.items():
            created.append(table)
            writer.create_table_if_not_exists(schema, table, arrow_schema, partition_spec=PARTITION_SPEC)
        for _ in range(repeats):
            started = monotonic()
            success = False
            write_seconds = None
            try:
                receipt = write_tournament_batch(TournamentBatch('eng.1', 2015, payloads), trino=adapter)
                write_seconds = monotonic() - started
                for table, count in receipt.rows_per_table.items():
                    result = trino.execute_query(f'SELECT count(*) FROM iceberg.{schema}.{table}')
                    if int(result[0][0]) != count:
                        raise RuntimeError('isolated benchmark row count mismatch')
                success = True
            finally:
                seconds = monotonic() - started
                timings.append(write_seconds if write_seconds is not None else seconds)
                store.record('write', now().timestamp(), run_id=schema, slug='eng.1', year=2015,
                             step=step, matches=len(payloads), write_seconds=write_seconds if write_seconds is not None else seconds,
                             batch_seconds=seconds, success=success, isolated=True)
        from .pace_report import percentile
        return dict(schema=schema, batches=repeats, matches=len(payloads), p95_seconds=percentile(timings),
                    isolated=True, limitation='production mass-write throughput remains #1511')
    finally:
        # Only names created/owned by this invocation can be removed.
        for table in reversed(created):
            trino.execute_query(f'DROP TABLE IF EXISTS iceberg.{schema}.{table}')
        if owned:
            trino.execute_query(f'DROP SCHEMA iceberg.{schema}')
        store.record('benchmark', now().timestamp(), event='finished', schema=schema)


def benchmark(trino, store, state_dir: Path, ids, *, count=20, step=None):
    state = store.get()
    if not state or not state.get('measurement_id'):
        raise ValueError('run measurement first to capture the exact benchmark payloads')
    if type(count) is not int or not 1 <= count <= len(ids):
        raise ValueError('invalid benchmark payload count')
    raw = EspnRawStore.from_uri((state_dir / 'measurement-raw' / state['measurement_id']).as_uri())
    sample_path = state_dir / 'measurement-raw' / state['measurement_id'] / 'benchmark-sample.json'
    selected = list(ids[:count])
    if sample_path.exists():
        sample = json.loads(sample_path.read_text())
        if sample.get('ids') != selected or sample.get('measurement_id') != state['measurement_id']:
            raise ValueError('benchmark sample identity changed')
    else:
        records = {}
        for event_id in selected:
            request = urls.summary('eng.1', event_id)
            _, record = raw.load(canonicalize_target(request.url, request.params))
            records[str(event_id)] = asdict(record)
        sample = dict(ids=selected, measurement_id=state['measurement_id'], records=records)
        # The controller lock serializes creation; flush before future aliases change.
        temporary = sample_path.with_suffix('.tmp')
        with temporary.open('w') as stream:
            json.dump(sample, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, sample_path)
    payloads = cached_payloads(raw, selected, sample['records'])
    from scrapers.base.iceberg_writer import IcebergWriter
    result = run_benchmark(trino, IcebergWriter(), store, payloads, step=step)
    from .measure_pace import digest
    result['sample_sha256'] = digest(sample)
    result['step'] = step
    return result
