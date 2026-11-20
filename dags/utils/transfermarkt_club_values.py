"""Isolated physical writer for published club-value observations."""
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
from scrapers.transfermarkt.club_values import (
    CLUB_VALUE_COLUMNS, CLUB_VALUE_KEY, CLUB_VALUE_PARSER_REVISION,
    CLUB_VALUE_SCHEMA_REVISION, ClubValueExtraction, build_club_value_dq, canonical_json,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock


CLUB_VALUE_TABLE = 'iceberg.bronze.transfermarkt_club_value_observations'
CLUB_VALUE_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_club_value_receipts_v1'
CLUB_VALUE_CONTRACT = {
    'table': CLUB_VALUE_TABLE, 'key_columns': CLUB_VALUE_KEY,
    'parser_revision': CLUB_VALUE_PARSER_REVISION,
    'schema_revision': CLUB_VALUE_SCHEMA_REVISION, 'write_mode': 'native_merge',
}
TIMESTAMP_COLUMNS = ('observed_at', 'captured_at', 'extracted_at', '_ingested_at')
DATE_COLUMNS = ('value_date',)
RECEIPT_COLUMNS = (
    'receipt_id', 'competition_id', 'club_id', 'observed_at', 'raw_capture_id',
    'source_body_hash', 'parser_revision', 'schema_revision', 'expected_points',
    'status_counts_json', 'differing_table_points', 'plan_sha256', 'physical_hash',
    'committed_batch', 'committed_at',
)
_COUNT_COLUMNS = frozenset({'expected_points', 'differing_table_points'})
_IMMUTABLE_EXCLUDED = frozenset({'extracted_at', 'run_id', 'cycle_id', '_batch_id', '_ingested_at'})
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def validate_club_value_write_policy(policy) -> None:
    missing = tuple(table for table in (CLUB_VALUE_TABLE, CLUB_VALUE_RECEIPTS_TABLE)
                    if table not in set(policy.allowed_write_tables))
    if missing:
        raise current.CurrentWriteError(
            'standing policy does not authorize club-value writes to: ' + ', '.join(missing))


def club_value_ddl() -> tuple[str, str]:
    business = ',\n    '.join(
        f'{column} timestamp(6)' if column in TIMESTAMP_COLUMNS
        else f'{column} date' if column in DATE_COLUMNS
        else f'{column} bigint' if column == 'value_amount'
        else f'{column} varchar'
        for column in CLUB_VALUE_COLUMNS)
    receipt = ',\n    '.join(
        f'{column} timestamp(6)' if column in {'observed_at', 'committed_at'}
        else f'{column} bigint' if column in _COUNT_COLUMNS
        else f'{column} varchar'
        for column in RECEIPT_COLUMNS)
    return (
        f'CREATE TABLE IF NOT EXISTS {CLUB_VALUE_TABLE} (\n    {business}\n) WITH (format = \'PARQUET\')',
        f'CREATE TABLE IF NOT EXISTS {CLUB_VALUE_RECEIPTS_TABLE} (\n    {receipt}\n) WITH (format = \'PARQUET\')',
    )


def _authorize(preflight: Mapping[str, Any]) -> Any:
    policy = load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH'))
    validate_club_value_write_policy(policy)
    return current._authorize(preflight)


def _frame(rows: tuple[Mapping[str, Any], ...]) -> pd.DataFrame:
    if not rows:
        raise current.CurrentWriteError('club-value frame must not be empty')
    frame = pd.DataFrame([dict(row) for row in rows], columns=CLUB_VALUE_COLUMNS)
    for column in TIMESTAMP_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
    frame['value_date'] = pd.to_datetime(frame['value_date']).dt.date
    return frame


def _canonical_cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in TIMESTAMP_COLUMNS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert('UTC').tz_localize(None)
        return stamp.isoformat(timespec='microseconds')
    if column in DATE_COLUMNS:
        return pd.Timestamp(value).date().isoformat()
    return current._cell(column, value)


def _read_club(connection, club_id: str) -> pd.DataFrame:
    try:
        result = current.run._query_dataframe(
            connection,
            f'SELECT {", ".join(CLUB_VALUE_COLUMNS)} FROM {CLUB_VALUE_TABLE} WHERE club_id = ?',
            [club_id])
    except Exception as exc:
        if not current._missing_table(exc):
            raise
        result = pd.DataFrame(columns=list(CLUB_VALUE_COLUMNS))
    return result.sort_values(list(CLUB_VALUE_KEY), kind='stable').reset_index(drop=True)


def _natural_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_canonical_cell(column, row[column]) for column in CLUB_VALUE_KEY)


def _rows_by_key(frame: pd.DataFrame) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    result: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for values in frame.itertuples(index=False, name=None):
        row = {column: _canonical_cell(column, value)
               for column, value in zip(frame.columns, values)}
        result.setdefault(_natural_key(row), []).append(row)
    return result


def _verify_committed(existing: pd.DataFrame, rows: tuple[Mapping[str, Any], ...]) -> None:
    physical = _rows_by_key(existing)
    for row in rows:
        key = _natural_key(row)
        committed = physical.get(key, [])
        if len(committed) != 1:
            raise current.CurrentWriteError(
                f'physical read-back differs for club-value key {key}: '
                f'expected one row, found {len(committed)}')
        changed = tuple(column for column in CLUB_VALUE_COLUMNS
                        if _canonical_cell(column, row[column]) != committed[0][column])
        if changed:
            raise current.CurrentWriteError(
                f'physical read-back differs for club-value key {key}: ' + ', '.join(changed))


def _validate_existing(existing: pd.DataFrame, rows: tuple[Mapping[str, Any], ...]) -> None:
    physical = _rows_by_key(existing)
    for row in rows:
        key = _natural_key(row)
        if key not in physical:
            continue
        if len(physical[key]) != 1:
            raise current.CurrentWriteError('duplicate club-value natural key in Bronze')
        stored = physical[key][0]
        changed = tuple(column for column in CLUB_VALUE_COLUMNS if column not in _IMMUTABLE_EXCLUDED
                        and _canonical_cell(column, row[column]) != stored[column])
        if changed:
            raise current.CurrentWriteError(
                f'immutable club-value fields differ for {key}: ' + ', '.join(changed))
        incoming = pd.Timestamp(row['extracted_at'])
        previous = pd.Timestamp(stored['extracted_at'])
        if incoming.tzinfo is not None:
            incoming = incoming.tz_convert('UTC').tz_localize(None)
        if previous.tzinfo is not None:
            previous = previous.tz_convert('UTC').tz_localize(None)
        if incoming < previous:
            raise current.CurrentWriteError('stale club-value extraction for existing natural key')


def _receipt_id(extraction: ClubValueExtraction, club_id: str) -> str:
    identity = [extraction.plan_sha256(), club_id]
    return 'tm-club-values-receipt-' + hashlib.sha256(canonical_json(identity)).hexdigest()[:32]


def _receipt_values(extraction: ClubValueExtraction, club_id: str,
                    physical_hash: str, committed_at: datetime) -> dict[str, Any]:
    if committed_at.tzinfo is None or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    first = extraction.club_rows(club_id)[0]
    return {
        'receipt_id': _receipt_id(extraction, club_id),
        'competition_id': extraction.capture.competition_id,
        'club_id': club_id,
        'observed_at': pd.Timestamp(first['observed_at']).to_pydatetime(),
        'raw_capture_id': first['raw_capture_id'],
        'source_body_hash': first['source_body_hash'],
        'parser_revision': CLUB_VALUE_PARSER_REVISION,
        'schema_revision': CLUB_VALUE_SCHEMA_REVISION,
        'expected_points': len(extraction.club_rows(club_id)),
        'status_counts_json': canonical_json(extraction.club_status_counts(club_id)).decode('utf-8'),
        'differing_table_points': len({row['value_date'] for row in extraction.club_rows(club_id)
                                       if row['reconciliation_status'] == 'value_differs'}),
        'plan_sha256': extraction.plan_sha256(),
        'physical_hash': physical_hash,
        'committed_batch': first['_batch_id'],
        'committed_at': committed_at.astimezone(timezone.utc).replace(tzinfo=None),
    }


def _return_receipt(values: Mapping[str, Any], physical: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(values)
    receipt.update(verified=True, receipt_readback=True, physical=physical,
                   status_counts=json.loads(values['status_counts_json']))
    receipt['committed_at'] = values['committed_at'].replace(tzinfo=timezone.utc).isoformat()
    receipt['observed_at'] = values['observed_at'].replace(tzinfo=timezone.utc).isoformat()
    return receipt


def _canonical_receipt_cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in {'observed_at', 'committed_at'}:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert('UTC').tz_localize(None)
        return stamp
    if column in _COUNT_COLUMNS:
        return int(value)
    return str(value)


def _receipt_row_values(row: Mapping[str, Any]) -> dict[str, Any]:
    values = {}
    for column in RECEIPT_COLUMNS:
        value = row[column]
        if column in {'observed_at', 'committed_at'}:
            if value is None or pd.isna(value):
                raise current.CurrentWriteError('club-value receipt timestamp is missing')
            values[column] = pd.Timestamp(value).to_pydatetime()
        elif value is None or pd.isna(value):
            values[column] = None
        elif column in _COUNT_COLUMNS:
            values[column] = int(value)
        else:
            values[column] = str(value)
    return values


def _require_equal_receipts(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    changed = tuple(column for column in RECEIPT_COLUMNS
                    if _canonical_receipt_cell(column, actual[column])
                    != _canonical_receipt_cell(column, expected[column]))
    if changed:
        raise current.CurrentWriteError('club-value receipt read-back differs: ' + ', '.join(changed))


def _merge_receipt(connection, values: Mapping[str, Any]) -> None:
    columns = ', '.join(RECEIPT_COLUMNS)
    literals = ', '.join(
        _sql(value.replace(tzinfo=timezone.utc)) if column in {'observed_at', 'committed_at'}
        else _sql(value)
        for column, value in values.items())
    updates = ', '.join(f'{column} = s.{column}' for column in RECEIPT_COLUMNS if column != 'receipt_id')
    current.run._execute_cursor(
        connection,
        f'MERGE INTO {CLUB_VALUE_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) '
        'ON t.receipt_id = s.receipt_id '
        f'WHEN MATCHED THEN UPDATE SET {updates} '
        f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
        + ', '.join(f's.{column}' for column in RECEIPT_COLUMNS) + ')')


def _stored_receipt(connection, receipt_id: str) -> pd.DataFrame:
    return current.run._query_dataframe(
        connection,
        f'SELECT {", ".join(RECEIPT_COLUMNS)} FROM {CLUB_VALUE_RECEIPTS_TABLE} WHERE receipt_id = ?',
        [receipt_id])


def write_club_values(scraper, extraction: ClubValueExtraction, preflight: Mapping[str, Any],
                      *, committed_at: datetime | None = None) -> dict[str, Any]:
    """Write every club with append-only observations and per-club receipts."""
    _authorize(preflight)
    if not isinstance(extraction, ClubValueExtraction):
        raise current.CurrentWriteError('extraction must be a ClubValueExtraction')
    if not build_club_value_dq(extraction)['structural_passed']:
        raise current.CurrentWriteError('club-value extraction did not pass structural DQ')
    committed_at = committed_at or datetime.now(timezone.utc)
    if committed_at.tzinfo is None or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    output = SimpleNamespace(
        table_name='transfermarkt_club_value_observations', key='club_values',
        is_legacy=False, replace_keys=('club_id',))
    receipts = []
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in club_value_ddl():
                current.run._execute_cursor(connection, ddl)
            for club_id in extraction.club_ids:
                rows = extraction.club_rows(club_id)
                frame = _frame(rows)
                existing = _read_club(connection, club_id)
                receipt_id = _receipt_id(extraction, club_id)
                stored = _stored_receipt(connection, receipt_id)
                if len(stored) > 1:
                    raise current.CurrentWriteError('multiple receipts for one club-value plan')
                if len(stored) == 1:
                    _verify_committed(existing, rows)
                    _validate_existing(existing, rows)
                    physical = current._read_back(scraper, output, frame, extraction.capture.run_id)
                    stored_values = _receipt_row_values(stored.iloc[0])
                    expected = _receipt_values(
                        extraction, club_id, physical['physical_hash'],
                        stored_values['committed_at'].replace(tzinfo=timezone.utc))
                    _require_equal_receipts(stored_values, expected)
                    receipts.append(_return_receipt(stored_values, physical))
                    continue
                _validate_existing(existing, rows)
                scraper._iceberg_writer.write_dataframe(
                    df=frame, database='bronze', table='transfermarkt_club_value_observations',
                    merge_keys=list(CLUB_VALUE_KEY), add_metadata=False, source='transfermarkt')
                physical = current._read_back(scraper, output, frame, extraction.capture.run_id)
                _verify_committed(_read_club(connection, club_id), rows)
                _validate_existing(_read_club(connection, club_id), rows)
                _authorize(preflight)
                values = _receipt_values(extraction, club_id, physical['physical_hash'], committed_at)
                _merge_receipt(connection, values)
                readback = _stored_receipt(connection, receipt_id)
                if len(readback) != 1:
                    raise current.CurrentWriteError('club-value receipt read-back is missing')
                _require_equal_receipts(_receipt_row_values(readback.iloc[0]), values)
                receipts.append(_return_receipt(values, physical))
        finally:
            connection.close()
    reconciliation = build_club_value_dq(extraction, tuple(receipts))
    if not reconciliation['reconciliation_passed']:
        raise current.CurrentWriteError('club-value receipt coverage is incomplete')
    return {
        'verified': True, 'receipt_readback': True, 'receipts': receipts,
        'plan_sha256': extraction.plan_sha256(),
        'full_available_history': extraction.full_available_history,
        'reconciliation': reconciliation,
    }


__all__ = [
    'CLUB_VALUE_CONTRACT', 'CLUB_VALUE_RECEIPTS_TABLE', 'CLUB_VALUE_TABLE',
    'RECEIPT_COLUMNS', 'club_value_ddl', 'validate_club_value_write_policy',
    'write_club_values',
]
