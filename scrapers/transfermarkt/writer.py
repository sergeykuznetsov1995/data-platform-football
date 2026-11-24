"""Transfermarkt-local committing SQL and cross-process write boundary.

The lock/commit-retry primitives moved verbatim into ``native_writer.py``
(stage B, #2404); this donor module keeps the Trino managers and the legacy
guard surface of the old stack and re-exports the moved names so existing
imports keep working. The lock protects warehouse work only. HTTP and parsing
never hold it. All TM processes on the scheduler must share its path (the
shared Airflow logs volume).
"""
from __future__ import annotations

import time  # noqa: F401 — donor tests patch the shared time module through here
from contextlib import contextmanager, suppress
from uuid import uuid4

from scrapers.base import trino_manager as base_trino
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.transfermarkt import write_journal
from scrapers.transfermarkt.native_writer import (  # noqa: F401 — donor facade
    CommittingConnection,
    CommittingCursor,
    StaleTransfermarktWrite,
    TrinoConnection,
    TrinoQueryError,
    _CTAS_ANALYSIS_ERRORS,
    _LOCAL,
    _MUTATION,
    _definite_rejection,
    _execute_committing,
    _execute_once,
    _target_dispatch,
    execute_statement,
    is_mutation,
    locked_write,
    shared_writer_lock_ready,
    write_is_journalled,
    writer_deadline,
    writer_lock,
)

CAREER_TABLES = frozenset({
    'transfermarkt_market_value_points', 'transfermarkt_transfer_events',
    'transfermarkt_market_value_history', 'transfermarkt_transfers',
})


@contextmanager
def _stage_create(stage):
    statement = write_journal.begin_statement(('stage-create', stage), {'single_submit': stage})
    try:
        yield statement
    except Exception as exc:
        coordinator = exc.__cause__
        analysis_rejected = (isinstance(coordinator, TrinoQueryError)
                             and coordinator.error_name in _CTAS_ANALYSIS_ERRORS
                             and coordinator.query_id
                             and coordinator.query_id == write_journal.statement_query(statement))
        if _definite_rejection(exc) or analysis_rejected:
            write_journal.finish_statement(statement)
        raise
    write_journal.finish_statement(statement)


class TransfermarktTrinoManager(TrinoTableManager):
    @property
    def connection(self):
        # The base manager caches a connection across calls. A donor read can
        # warm it before a journalled write, and donor work can follow either.
        with self._conn_lock:
            if isinstance(self._conn, TrinoConnection):
                strict = self._conn.max_attempts == 1 and self._conn.legacy_prepared_statements is False
                if strict != write_is_journalled():
                    self._reset_connection()
            return super().connection

    def _create_connection(self):
        transport = {}
        if write_is_journalled():
            transport.update(max_attempts=1, legacy_prepared_statements=False)
        if self._password:
            return base_trino.trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme='https',
                auth=base_trino.trino.auth.BasicAuthentication(self.user, self._password),
                verify=False,
                session_properties={'enable_dynamic_filtering': 'false'},
                **transport,
            )
        return base_trino.trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            session_properties={'enable_dynamic_filtering': 'false'},
            **transport,
        )

    def arrow_schema_to_trino(self, arrow_schema):
        types = super().arrow_schema_to_trino(arrow_schema)
        if 'first_seen_at' in types:
            # The ops registry uses UTC timestamp(6). Never let the generic
            # default TIMESTAMP precision round an immutable microsecond clock.
            types['first_seen_at'] = 'TIMESTAMP(6)'
        from scrapers.transfermarkt.scraper import CAREER_DATE_COLUMNS, SQUAD_DATE_COLUMNS
        for column in SQUAD_DATE_COLUMNS:
            if types.get(column) == 'VARCHAR':
                # An all-NULL first batch arrives as an Arrow null column.
                types[column] = 'DATE'
        for column in CAREER_DATE_COLUMNS:
            if column in types:
                types[column] = 'DATE'
        return types

    def _execute(self, sql, fetch=False, params=None, *, replay_safe=True):
        parent = super()._execute
        mutation = _MUTATION.match(sql)
        if fetch or not mutation:
            return parent(sql, fetch=fetch, params=params)
        if write_is_journalled():
            verb = mutation[1].upper()
            stage = getattr(_LOCAL, 'fresh_stage', None)
            if stage and sql == 'DROP TABLE IF EXISTS ' + stage:
                # The shared helper clears caller-supplied stable names. This
                # name was allocated for a single attempt and has no predecessor.
                return
            if stage and sql.startswith('CREATE TABLE ' + stage + ' AS '):
                _LOCAL.fresh_stage = None
                return self._execute_target(sql, params, dispatch=_stage_create(stage))
            if verb == 'INSERT':
                # Includes every VALUES batch and the replacement tombstones.
                return parent(sql, fetch=fetch, params=params, replay_safe=False)
            if verb == 'MERGE':
                stage = getattr(_LOCAL, 'merge_stage', None)
                return self._execute_target(sql, params, dispatch=_target_dispatch(
                    stage=stage if stage and stage in sql else None))
        if not replay_safe:
            return parent(sql, fetch=fetch, params=params, replay_safe=False)
        return _execute_committing(lambda: parent(sql, fetch=fetch, params=params))

    def _execute_target(self, sql, params, *, dispatch=None):
        with self._conn_lock:
            cursor = self.connection.cursor()
            try:
                with (dispatch if dispatch is not None else _target_dispatch()) as statement:
                    try:
                        _execute_once(cursor, sql, params, statement)
                    except Exception as exc:
                        if any(message in str(exc) for message in self._CONNECTION_ERRORS):
                            with suppress(Exception):
                                self._reset_connection()
                            raise base_trino.TrinoAmbiguousWriteError(
                                f'Connection lost while executing SQL: {exc}') from exc
                        raise base_trino.TrinoError(f'SQL execution failed: {exc}') from exc
            finally:
                with suppress(Exception):
                    cursor.close()

    def _execute_committing(self, sql, *, replay_safe=True):
        # Avoid the common manager's independent retry loop and nested jitter.
        if write_is_journalled() or not replay_safe:
            # The strict ladder retries a definite rejected commit only.
            return super()._execute_committing(sql, replay_safe=False)
        return self._execute(sql)

    def _fast_merge_column_types(self, schema, table, columns, provided):
        # The base single-row fast path falls back to another MERGE on a
        # TrinoError, including an ambiguous response to its first MERGE.
        if write_is_journalled():
            return None
        return super()._fast_merge_column_types(schema, table, columns, provided)

    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        with writer_lock():
            stage_record = None
            previous_stage = getattr(_LOCAL, 'fresh_stage', None)
            previous_merge_stage = getattr(_LOCAL, 'merge_stage', None)
            if write_is_journalled():
                kwargs.pop('staging_id', None)  # a delayed insert belongs only to its original stage
                target = getattr(_LOCAL, 'target', None)
                if target is not None:
                    scope, receipt_id, _, _ = target
                    if scope[0] != f'{self.catalog}.{schema}.{table}':
                        raise RuntimeError('core staging target differs')
                    suffix = 'tm_' + uuid4().hex
                    stage = table + '__stg_' + suffix
                    stage_record = write_journal.stage_record(receipt_id, stage)
                    write_journal._immutable_record(stage_record, {
                        'receipt_id': receipt_id, 'table': scope[0], 'stage': stage, 'fenced_create': True})
                    kwargs['staging_id'] = suffix
            if kwargs.get('delete_filter'):
                kwargs['single_statement_replace'] = True
            try:
                _LOCAL.merge_stage = stage if stage_record else None
                _LOCAL.fresh_stage = f'{self.catalog}.{schema}.{stage}' if stage_record else None
                count = super().insert_dataframe_atomic(schema, table, df, **kwargs)
            finally:
                _LOCAL.fresh_stage = previous_stage
                _LOCAL.merge_stage = previous_merge_stage
            if stage_record is not None:
                write_journal.forget_stage(stage_record)
            return count


class TransfermarktIcebergWriter(IcebergWriter):
    dispatches_journalled_target = True

    def drop_journalled_stage(self, stage):
        manager = self._get_trino_manager()
        if manager.catalog != 'iceberg':
            raise RuntimeError('core retained stage catalog differs')
        with base_trino._COMMIT_LOCK:
            manager.drop_table('bronze', stage, if_exists=True)

    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = TransfermarktTrinoManager(host=self.trino_host, port=self.trino_port, catalog=self.catalog)
        return self._trino_manager
    def write_dataframe(self, df=None, **kwargs):
        if df is not None:
            kwargs['df'] = df
        with writer_lock():
            # Preserve original parser lineage and batch across Bronze and ops.
            kwargs['add_metadata'] = False
            return super().write_dataframe(**kwargs)


def guard_frames(scraper, outputs, frames):
    """Compare original capture clocks before the first bundle mutation.

    One grouped query per output keeps the lock short even for 500 careers.
    Global native careers deliberately compare by player, across source scopes.
    """
    import pandas as pd
    connection = scraper._bronze_connection()
    try:
        for output in outputs:
            frame = frames.get(output.key)
            if frame is None or frame.empty or not output.replace_keys:
                continue
            if output.is_legacy and any(not item.is_legacy for item in outputs):
                continue
            clock_column = 'fetched_at' if 'fetched_at' in frame else '_ingested_at'
            if clock_column not in frame:
                if output.table_name in CAREER_TABLES:
                    raise StaleTransfermarktWrite('replacement lacks original capture time')
                continue
            keys = list(output.replace_keys)
            incoming = {}
            for identity, part in frame.groupby(keys, dropna=False):
                identity = identity if isinstance(identity, tuple) else (identity,)
                stamps = pd.to_datetime(part[clock_column], utc=True)
                if stamps.isna().any():
                    raise StaleTransfermarktWrite('replacement capture time is incomplete')
                incoming[tuple(str(value) for value in identity)] = max(stamps.max(), pd.Timestamp(frame.attrs.get('tm_bundle_fetched_at', stamps.max())))
            predicate = scraper._build_partition_delete_filter(frame, keys)
            cursor = connection.cursor()
            try:
                execute_statement(cursor, 'SELECT ' + ', '.join(keys) + ', MAX(' + clock_column + ') FROM iceberg.bronze.'
                    + output.table_name + ' WHERE ' + predicate + ' GROUP BY ' + ', '.join(keys))
                rows = cursor.fetchall()
            except Exception as exc:
                if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                    rows = []
                else:
                    raise
            finally:
                cursor.close()
            if output.table_name in {'transfermarkt_market_value_points', 'transfermarkt_transfer_events'}:
                from scrapers.transfermarkt.career_refs import latest_capture_times
                for player, stamp in latest_capture_times(connection, output.table_name, sorted(set(frame.player_id.astype(str)))).items():
                    latest = pd.to_datetime(stamp, utc=True)
                    if latest > incoming[(player,)]:
                        raise StaleTransfermarktWrite('newer complete capture already recorded for ' + output.table_name)
            for row in rows:
                identity = tuple(str(value) for value in row[:-1])
                latest = row[-1]
                if latest is None:
                    continue
                stamp = pd.Timestamp(latest)
                stamp = stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')
                if identity not in incoming or stamp > incoming[identity]:
                    raise StaleTransfermarktWrite('newer capture already committed to ' + output.table_name)
    finally:
        connection.close()


def guard_cached_frames(scraper, outputs, frames):
    """Cached physical references must still exist before legacy hydration."""
    import pandas as pd
    connection = scraper._bronze_connection()
    try:
        for output in outputs:
            frame = frames.get(output.key)
            refs = frame.attrs.get('tm_original_capture_refs', []) if frame is not None else []
            if output.is_legacy or not refs:
                continue
            original = {item['player_id']: item['batch_id'] for item in refs}
            expected = frame[frame.player_id.astype(str).isin(original)].copy()
            expected['_batch_id'] = expected.player_id.astype(str).map(original)
            predicate = ' OR '.join('(player_id = ? AND _batch_id = ?)' for _ in refs)
            params = tuple(value for item in refs for value in (item['player_id'], item['batch_id']))
            cursor = connection.cursor()
            try:
                from scrapers.transfermarkt.revisions import CAREER_NULLABLE_ADDITIONS
                # Discover the physical projection from the read itself, as in
                # IcebergWriter.read_table. New nullable columns may not exist
                # until the subsequent write evolves the table schema.
                execute_statement(cursor, 'SELECT * FROM iceberg.bronze.'
                    + output.table_name + ' WHERE ' + predicate, params)
                physical = [column[0] for column in cursor.description]
                actual = pd.DataFrame(cursor.fetchall(), columns=physical)
                missing = set(frame.columns) - set(physical)
                if (missing - CAREER_NULLABLE_ADDITIONS
                    or any(not expected[column].isna().all() for column in missing)):
                    raise StaleTransfermarktWrite('cached career physical projection differs: ' + output.table_name)
                projection = [column for column in frame.columns if column in physical]
                actual = actual[projection]
            finally:
                cursor.close()
            from dags.utils.transfermarkt_current_write import _rows
            if _rows(actual) != _rows(expected[projection]):
                raise StaleTransfermarktWrite('cached career physical captures changed: ' + output.table_name)
    finally:
        connection.close()
