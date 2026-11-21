"""Authorized native writes and physical receipts for nonmatch referee observations."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from dags.utils.transfermarkt_current_state import _sql
from scrapers.transfermarkt.referee_capture import (
    REFEREE_AGGREGATE_PHYSICAL_COLUMNS,
    REFEREE_AGGREGATE_TABLE,
    REFEREE_PROFILE_COLUMNS,
    REFEREE_PROFILE_TABLE,
    REFEREE_RECEIPTS_TABLE,
    RefereeCaptureBundle,
)
from scrapers.transfermarkt.referees import (
    REFEREE_AGGREGATE_OBSERVATION_KEY,
    REFEREE_OBSERVATION_STATUSES,
    REFEREE_PARSER_REVISION,
    REFEREE_SCHEMA_REVISION,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock

REFEREE_PROFILE_KEY = ('referee_id', 'observed_at')
REFEREE_AGGREGATE_KEY = REFEREE_AGGREGATE_OBSERVATION_KEY
TIMESTAMP_COLUMNS = frozenset({'observed_at', 'captured_at', 'extracted_at', '_ingested_at'})
RECEIPT_COLUMNS = (
    'receipt_id', 'capture_kind', 'target_table', 'scope_id', 'competition_id', 'edition_id',
    'run_id', 'universe_version', 'parser_revision', 'schema_revision',
    'natural_key_columns_json', 'expected_directory_pages', 'expected_profiles',
    'expected_metrics', 'row_count', 'status_counts_json', 'raw_capture_ids_json',
    'raw_body_hashes_json', 'plan_sha256', 'physical_hash', 'committed_batch', 'committed_at',
)
_RECEIPT_COUNTS = frozenset({'expected_directory_pages', 'expected_profiles', 'expected_metrics', 'row_count'})
_RECEIPT_ID_STAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)
_RAW_LINEAGE = (
    'source_url', 'raw_capture_id', 'source_body_hash', 'source_endpoint', 'scope_id',
    'cycle_id', 'captured_at', 'observed_at', 'extracted_at', 'parser_revision',
    'schema_revision', 'run_id', 'universe_version',
)


def validate_referee_write_policy(policy) -> None:
    allowed = set(policy.allowed_write_tables)
    missing = tuple(table for table in (
        REFEREE_PROFILE_TABLE, REFEREE_AGGREGATE_TABLE, REFEREE_RECEIPTS_TABLE,
    ) if table not in allowed)
    if missing:
        raise current.CurrentWriteError(
            'standing policy does not authorize referee writes to: ' + ', '.join(missing),
        )


def referee_ddl() -> tuple[str, str, str]:
    profile_types = {'birth_date': 'date', **{column: 'timestamp(6)' for column in TIMESTAMP_COLUMNS}}
    aggregate_types = {
        'value': 'double', 'source_row': 'bigint',
        **{column: 'timestamp(6)' for column in TIMESTAMP_COLUMNS},
    }
    profile_columns = ',\n    '.join(
        f'{column} {profile_types.get(column, "varchar")}' for column in REFEREE_PROFILE_COLUMNS
    )
    aggregate_columns = ',\n    '.join(
        f'{column} {aggregate_types.get(column, "varchar")}'
        for column in REFEREE_AGGREGATE_PHYSICAL_COLUMNS
    )
    receipt_columns = ',\n    '.join(
        f'{column} timestamp(6)' if column == 'committed_at'
        else f'{column} bigint' if column in _RECEIPT_COUNTS else f'{column} varchar'
        for column in RECEIPT_COLUMNS
    )
    return (
        f'CREATE TABLE IF NOT EXISTS {REFEREE_PROFILE_TABLE} '
        f'(\n    {profile_columns}\n)\nWITH (format = \'PARQUET\')',
        f'CREATE TABLE IF NOT EXISTS {REFEREE_AGGREGATE_TABLE} '
        f'(\n    {aggregate_columns}\n)\nWITH (format = \'PARQUET\')',
        f'CREATE TABLE IF NOT EXISTS {REFEREE_RECEIPTS_TABLE} '
        f'(\n    {receipt_columns}\n)\nWITH (format = \'PARQUET\')',
    )


def build_referee_dq(
    bundle: RefereeCaptureBundle, *, verified_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    errors = []
    rows = [*bundle.profiles, *bundle.aggregates]
    if not rows:
        errors.append('empty_extraction')
    key_columns = REFEREE_PROFILE_KEY if bundle.capture_kind == 'profiles' else REFEREE_AGGREGATE_KEY
    keys = [tuple(row.get(column) for column in key_columns) for row in rows]
    if len(set(keys)) != len(keys):
        errors.append('duplicate_natural_key')
    for index, row in enumerate(rows):
        for column in _RAW_LINEAGE:
            if row.get(column) in (None, ''):
                errors.append(f'null_lineage:{index}:{column}')
        if row.get('observation_status') not in REFEREE_OBSERVATION_STATUSES and bundle.capture_kind == 'aggregates':
            errors.append(f'status_invalid:{index}')
    forbidden = {'match_id', 'game_id', 'home_team', 'away_team', 'event', 'appointment'}
    match_rows = sum(bool(forbidden & set(row)) for row in rows)
    if match_rows:
        errors.append('match_rows_forbidden')
    if verified_receipt is not None and verified_receipt.get('plan_sha256') != referee_plan_sha256(bundle):
        errors.append('receipt_plan_sha256_mismatch')
    return {
        'structural_passed': not errors,
        'errors': sorted(set(errors)),
        'row_count': len(rows),
        'match_row_count': match_rows,
        'status_counts': _status_counts(bundle),
        'receipt_verified': verified_receipt is not None and 'receipt_plan_sha256_mismatch' not in errors,
    }


def referee_plan_sha256(bundle: RefereeCaptureBundle) -> str:
    plan = {
        'capture_kind': bundle.capture_kind,
        'scope_id': bundle.scope_id,
        'competition_id': bundle.competition_id,
        'edition_id': bundle.edition_id,
        'run_id': bundle.run_id,
        'universe_version': bundle.universe_version,
        'parser_revision': REFEREE_PARSER_REVISION,
        'schema_revision': REFEREE_SCHEMA_REVISION,
        'profiles': [_canonical_row(row, REFEREE_PROFILE_COLUMNS) for row in bundle.profiles],
        'aggregates': [_canonical_row(row, REFEREE_AGGREGATE_PHYSICAL_COLUMNS) for row in bundle.aggregates],
        'expected_directory_pages': bundle.expected_directory_pages,
    }
    return hashlib.sha256(_canonical_json(plan)).hexdigest()


def write_referee_bundle(
    scraper: Any, bundle: RefereeCaptureBundle, preflight: Mapping[str, Any], *,
    committed_at: datetime | None = None,
) -> dict[str, Any]:
    _authorize(preflight)
    if not isinstance(bundle, RefereeCaptureBundle):
        raise current.CurrentWriteError('bundle must be a RefereeCaptureBundle')
    dq = build_referee_dq(bundle)
    if not dq['structural_passed']:
        raise current.CurrentWriteError('referee structural DQ failed: ' + ', '.join(dq['errors']))
    if committed_at is None:
        committed_at = datetime.now(timezone.utc)
    if not isinstance(committed_at, datetime) or committed_at.tzinfo is None or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    plan_hash = referee_plan_sha256(bundle)
    batch_id = 'tm-referee-' + hashlib.sha256(plan_hash.encode()).hexdigest()[:32]
    frames = _frames(bundle, batch_id)
    receipt_id = _receipt_id(bundle)
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in referee_ddl():
                current.run._execute_cursor(connection, ddl)
            stored = _stored_receipt(connection, receipt_id)
            if len(stored) > 1:
                raise current.CurrentWriteError('multiple referee receipts for one plan')
            physical_by_table = {}
            for table, key_columns, columns, frame in frames:
                existing = _read_existing(connection, table, columns, key_columns, frame)
                _validate_existing(existing, frame, key_columns, columns)
                output = SimpleNamespace(
                    table_name=table.rsplit('.', 1)[-1], key=bundle.capture_kind,
                    is_legacy=False, replace_keys=(),
                )
                if existing.empty or len(existing) != len(frame):
                    scraper._iceberg_writer.write_dataframe(
                        df=frame, database='bronze', table=output.table_name,
                        merge_keys=list(key_columns), add_metadata=False, source='transfermarkt',
                    )
                physical = current._read_back(scraper, output, frame, bundle.run_id)
                committed = _read_existing(connection, table, columns, key_columns, frame)
                _verify_physical(committed, frame, key_columns, columns)
                physical_by_table[table] = physical
            physical_hash = hashlib.sha256(_canonical_json({
                table: physical_by_table[table]['physical_hash'] for table in sorted(physical_by_table)
            })).hexdigest()
            _authorize(preflight)
            values = _receipt_values(bundle, physical_hash, committed_at, batch_id)
            if len(stored) == 1:
                expected = dict(values)
                expected['committed_at'] = _receipt_row_values(stored.iloc[0])['committed_at']
                _require_equal_receipts(_receipt_row_values(stored.iloc[0]), expected, 'stored')
                return _return_receipt(_receipt_row_values(stored.iloc[0]), physical_by_table)
            _merge_receipt(connection, values)
            readback = _stored_receipt(connection, receipt_id)
            if len(readback) != 1:
                raise current.CurrentWriteError('referee receipt read-back is missing')
            _require_equal_receipts(_receipt_row_values(readback.iloc[0]), values, 'read-back')
            return _return_receipt(values, physical_by_table)
        finally:
            connection.close()


def run_referee_physical_dq(connection, *, run_id: str | None = None) -> dict[str, Any]:
    errors = []
    tables = {}
    for table, key_columns in (
        (REFEREE_PROFILE_TABLE, REFEREE_PROFILE_KEY),
        (REFEREE_AGGREGATE_TABLE, REFEREE_AGGREGATE_KEY),
    ):
        null_checks = ' + '.join(
            f'CASE WHEN {column} IS NULL THEN 1 ELSE 0 END' for column in _RAW_LINEAGE
        )
        summary = current.run._execute_cursor(
            connection,
            f'SELECT COUNT(*), COALESCE(SUM({null_checks}), 0) FROM {table}',
            fetch=True,
        )
        duplicates = current.run._execute_cursor(
            connection,
            f'SELECT COUNT(*) FROM (SELECT {", ".join(key_columns)} FROM {table} '
            f'GROUP BY {", ".join(key_columns)} HAVING COUNT(*) > 1) d',
            fetch=True,
        )
        row_count, null_lineage = (int(value) for value in summary[0])
        duplicate_count = int(duplicates[0][0])
        tables[table] = {
            'row_count': row_count, 'null_lineage_count': null_lineage,
            'duplicate_natural_keys': duplicate_count,
        }
        if null_lineage:
            errors.append(f'null_lineage:{table}')
        if duplicate_count:
            errors.append(f'duplicate_natural_key:{table}')
    receipt_count = None
    if run_id is not None:
        rows = current.run._execute_cursor(
            connection,
            f'SELECT COUNT(*) FROM {REFEREE_RECEIPTS_TABLE} WHERE run_id = ?',
            [run_id], fetch=True,
        )
        receipt_count = int(rows[0][0])
    return {
        'completed': True,
        'status': 'GREEN' if not errors else 'RED',
        'errors': errors,
        'tables': tables,
        'run_id': run_id,
        'receipt_count': receipt_count,
        'match_row_count': 0,
    }


def _authorize(preflight):
    policy = load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH'))
    validate_referee_write_policy(policy)
    return current._authorize(preflight)


def _frames(bundle, batch_id):
    frames = []
    if bundle.profiles:
        frames.append((
            REFEREE_PROFILE_TABLE, REFEREE_PROFILE_KEY, REFEREE_PROFILE_COLUMNS,
            _frame(bundle.profiles, REFEREE_PROFILE_COLUMNS, batch_id),
        ))
    if bundle.aggregates:
        frames.append((
            REFEREE_AGGREGATE_TABLE, REFEREE_AGGREGATE_KEY, REFEREE_AGGREGATE_PHYSICAL_COLUMNS,
            _frame(bundle.aggregates, REFEREE_AGGREGATE_PHYSICAL_COLUMNS, batch_id),
        ))
    return frames


def _frame(rows, columns, batch_id):
    frame = pd.DataFrame([dict(row) for row in rows], columns=list(columns))
    frame['_batch_id'] = batch_id
    for column in TIMESTAMP_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
    if 'value' in frame:
        frame['value'] = pd.to_numeric(frame['value'], errors='raise')
    if 'source_row' in frame:
        frame['source_row'] = pd.to_numeric(frame['source_row'], errors='raise').astype('int64')
    return frame


def _canonical_row(row, columns):
    return {
        column: _canonical_cell(column, row.get(column))
        for column in columns if column != '_batch_id'
    }


def _canonical_cell(column, value):
    if value is None or pd.isna(value):
        return None
    if column in TIMESTAMP_COLUMNS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert('UTC').tz_localize(None)
        return stamp.isoformat(timespec='microseconds')
    if column == 'value':
        return float(value)
    if column in {'source_row', *_RECEIPT_COUNTS}:
        return int(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _read_existing(connection, table, columns, key_columns, frame):
    keys = sorted({_key(row, key_columns) for row in frame.to_dict('records')})
    if not keys:
        return pd.DataFrame(columns=list(columns))
    pieces = []
    for start in range(0, len(keys), 100):
        chunk = keys[start:start + 100]
        predicate = ' OR '.join(
            '(' + ' AND '.join(f'{column} = ?' for column in key_columns) + ')'
            for _ in chunk
        )
        parameters = [
            pd.Timestamp(value).to_pydatetime() if column in TIMESTAMP_COLUMNS else value
            for key in chunk for column, value in zip(key_columns, key)
        ]
        try:
            piece = current.run._query_dataframe(
                connection, f'SELECT {", ".join(columns)} FROM {table} WHERE {predicate}', parameters,
            )
        except Exception as exc:
            if not current._missing_table(exc):
                raise
            piece = pd.DataFrame(columns=list(columns))
        pieces.append(piece)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(columns=list(columns))


def _key(row, key_columns):
    return tuple(_canonical_cell(column, row[column]) for column in key_columns)


def _validate_existing(existing, frame, key_columns, columns):
    if existing.empty:
        return
    incoming = {_key(row, key_columns): row for row in frame.to_dict('records')}
    for physical in existing.to_dict('records'):
        key = _key(physical, key_columns)
        if key not in incoming:
            continue
        changed = [
            column for column in columns if column != '_batch_id'
            and _canonical_cell(column, incoming[key][column]) != _canonical_cell(column, physical[column])
        ]
        if changed:
            raise current.CurrentWriteError(
                f'immutable referee fields differ for {key}: {", ".join(changed)}',
            )


def _verify_physical(existing, frame, key_columns, columns):
    incoming = {_key(row, key_columns): row for row in frame.to_dict('records')}
    counts = {}
    physical_rows = {}
    for physical in existing.to_dict('records'):
        key = _key(physical, key_columns)
        counts[key] = counts.get(key, 0) + 1
        physical_rows[key] = physical
    for key, row in incoming.items():
        if counts.get(key, 0) != 1:
            raise current.CurrentWriteError(
                f'physical read-back key {key}: expected once, found {counts.get(key, 0)}',
            )
        changed = [
            column for column in columns if column != '_batch_id'
            and _canonical_cell(column, row[column])
            != _canonical_cell(column, physical_rows[key][column])
        ]
        if changed:
            details = ', '.join(
                f'{column}: {row[column]!r} != {physical_rows[key][column]!r}'
                for column in changed
            )
            raise current.CurrentWriteError(
                f'physical read-back differs for {key}: {details}',
            )


def _status_counts(bundle):
    counts = {}
    for row in (*bundle.profiles, *bundle.aggregates):
        status = row.get('observation_status', 'observed_present')
        counts[status] = counts.get(status, 0) + 1
    return counts


def _receipt_values(bundle, physical_hash, committed_at, batch_id):
    if not isinstance(committed_at, datetime) or committed_at.tzinfo is None or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    key = REFEREE_PROFILE_KEY if bundle.capture_kind == 'profiles' else REFEREE_AGGREGATE_KEY
    body_hashes = {
        row['raw_capture_id']: row['source_body_hash'] for row in (*bundle.profiles, *bundle.aggregates)
    }
    plan_hash = referee_plan_sha256(bundle)
    return {
        'receipt_id': 'tm-referee-receipt-' + hashlib.sha256(plan_hash.encode()).hexdigest()[:32],
        'capture_kind': bundle.capture_kind,
        'target_table': REFEREE_PROFILE_TABLE if bundle.capture_kind == 'profiles' else REFEREE_AGGREGATE_TABLE,
        'scope_id': bundle.scope_id,
        'competition_id': bundle.competition_id,
        'edition_id': bundle.edition_id,
        'run_id': bundle.run_id,
        'universe_version': bundle.universe_version,
        'parser_revision': REFEREE_PARSER_REVISION,
        'schema_revision': REFEREE_SCHEMA_REVISION,
        'natural_key_columns_json': _canonical_json(list(key)).decode(),
        'expected_directory_pages': bundle.expected_directory_pages,
        'expected_profiles': len(bundle.directory_referees) if bundle.capture_kind == 'aggregates' else len(bundle.profiles),
        'expected_metrics': len(bundle.aggregates),
        'row_count': len(bundle.profiles) + len(bundle.aggregates),
        'status_counts_json': _canonical_json(_status_counts(bundle)).decode(),
        'raw_capture_ids_json': _canonical_json(sorted(body_hashes)).decode(),
        'raw_body_hashes_json': _canonical_json(body_hashes).decode(),
        'plan_sha256': plan_hash,
        'physical_hash': physical_hash,
        'committed_batch': batch_id,
        'committed_at': committed_at.astimezone(timezone.utc).replace(tzinfo=None),
    }


def _receipt_id(bundle):
    return _receipt_values(bundle, '', _RECEIPT_ID_STAMP, '')['receipt_id']


def _stored_receipt(connection, receipt_id):
    return current.run._query_dataframe(
        connection,
        f'SELECT {", ".join(RECEIPT_COLUMNS)} FROM {REFEREE_RECEIPTS_TABLE} WHERE receipt_id = ?',
        [receipt_id],
    )


def _merge_receipt(connection, values):
    columns = ', '.join(RECEIPT_COLUMNS)
    literals = ', '.join(
        _sql(value.replace(tzinfo=timezone.utc)) if column == 'committed_at' else _sql(value)
        for column, value in values.items()
    )
    updates = ', '.join(
        f'{column} = s.{column}' for column in RECEIPT_COLUMNS if column != 'receipt_id'
    )
    current.run._execute_cursor(
        connection,
        f'MERGE INTO {REFEREE_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) '
        'ON t.receipt_id = s.receipt_id '
        f'WHEN MATCHED THEN UPDATE SET {updates} '
        f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
        + ', '.join(f's.{column}' for column in RECEIPT_COLUMNS) + ')',
    )


def _receipt_row_values(row):
    values = {}
    for column in RECEIPT_COLUMNS:
        value = row[column]
        if column == 'committed_at':
            if value is None or pd.isna(value):
                raise current.CurrentWriteError('receipt committed_at is missing')
            values[column] = pd.Timestamp(value).to_pydatetime()
        elif value is None or pd.isna(value):
            values[column] = None
        elif column in _RECEIPT_COUNTS:
            values[column] = int(value)
        else:
            values[column] = str(value)
    return values


def _require_equal_receipts(actual, expected, stage):
    changed = [
        column for column in RECEIPT_COLUMNS
        if _canonical_cell(column, actual[column]) != _canonical_cell(column, expected[column])
    ]
    if changed:
        raise current.CurrentWriteError(
            f'referee receipt {stage} differs: {", ".join(changed)}',
        )


def _return_receipt(values, physical):
    result = dict(values)
    result['verified'] = True
    result['receipt_readback'] = True
    result['physical'] = physical
    result['natural_key_columns'] = json.loads(values['natural_key_columns_json'])
    result['status_counts'] = json.loads(values['status_counts_json'])
    result['raw_capture_ids'] = json.loads(values['raw_capture_ids_json'])
    result['raw_body_hashes'] = json.loads(values['raw_body_hashes_json'])
    result['committed_at'] = values['committed_at'].replace(tzinfo=timezone.utc).isoformat()
    return result


def _canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
        allow_nan=False, default=str,
    ).encode('utf-8')


__all__ = [
    'RECEIPT_COLUMNS', 'REFEREE_AGGREGATE_KEY', 'REFEREE_AGGREGATE_TABLE',
    'REFEREE_PROFILE_KEY', 'REFEREE_PROFILE_TABLE', 'REFEREE_RECEIPTS_TABLE',
    'TIMESTAMP_COLUMNS', 'build_referee_dq', 'referee_ddl', 'referee_plan_sha256',
    'run_referee_physical_dq', 'validate_referee_write_policy', 'write_referee_bundle',
]
