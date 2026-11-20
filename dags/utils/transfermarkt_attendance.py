"""Immutable attendance writes with edition receipts and physical readback."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import re
from typing import Any, Iterable, Mapping

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from dags.utils.transfermarkt_current_state import _sql
from scrapers.transfermarkt.attendance import (
    ATTENDANCE_COLUMNS,
    ATTENDANCE_KEY,
    ATTENDANCE_PARSER_REVISION,
    ATTENDANCE_SCHEMA_REVISION,
    ATTENDANCE_TABLE,
    AttendanceExtraction,
    build_attendance_dq,
    canonical_club_id,
    canonical_timestamp,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock

ATTENDANCE_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_attendance_receipts_v1'
ATTENDANCE_CONTRACT = {
    'table': ATTENDANCE_TABLE,
    'key_columns': ATTENDANCE_KEY,
    'parser_revision': ATTENDANCE_PARSER_REVISION,
    'schema_revision': ATTENDANCE_SCHEMA_REVISION,
    'write_mode': 'native_merge',
}
TIMESTAMP_COLUMNS = frozenset({'captured_at', 'observed_at', 'extracted_at', '_ingested_at'})
INTEGER_COLUMNS = frozenset({'capacity', 'spectators_total', 'spectators_average', 'matches', 'sold_out'})
ASSOCIATION_FIELDS = frozenset({
    'universe_version', 'run_id', 'scope_id', 'extracted_at',
    '_batch_id', '_ingested_at', 'final_season', 'final_season_proof',
})
RECEIPT_COLUMNS = (
    'receipt_id', 'target_table', 'competition_id', 'edition_id', 'observed_at',
    'capture_status', 'universe_version', 'run_id', 'cycle_id', 'parser_revision', 'schema_revision',
    'natural_key_columns_json', 'expected_club_ids_json', 'observed_club_ids_json',
    'expected_clubs', 'row_count', 'status_counts_json', 'final_season',
    'final_season_proof', 'raw_capture_ids_json', 'raw_body_hashes_json',
    'plan_sha256', 'physical_hash', 'committed_batch', 'committed_at',
)
_RECEIPT_TIMESTAMPS = frozenset({'observed_at', 'committed_at'})
_RECEIPT_COUNTS = frozenset({'expected_clubs', 'row_count'})
_RECEIPT_ID_STAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def validate_attendance_write_policy(policy) -> None:
    missing = tuple(table for table in (ATTENDANCE_TABLE, ATTENDANCE_RECEIPTS_TABLE)
                    if table not in set(policy.allowed_write_tables))
    if missing:
        raise current.CurrentWriteError(
            'standing policy does not authorize attendance writes to: ' + ', '.join(missing))


def attendance_ddl() -> tuple[str, str]:
    business = ',\n    '.join(
        f'{column} timestamp(6)' if column in TIMESTAMP_COLUMNS
        else f'{column} bigint' if column in INTEGER_COLUMNS
        else f'{column} boolean' if column == 'final_season'
        else f'{column} varchar'
        for column in ATTENDANCE_COLUMNS)
    receipts = ',\n    '.join(
        f'{column} timestamp(6)' if column in _RECEIPT_TIMESTAMPS
        else f'{column} bigint' if column in _RECEIPT_COUNTS
        else f'{column} boolean' if column == 'final_season'
        else f'{column} varchar'
        for column in RECEIPT_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {ATTENDANCE_TABLE} (\n    {business}\n) WITH (format = 'PARQUET')",
        f"CREATE TABLE IF NOT EXISTS {ATTENDANCE_RECEIPTS_TABLE} (\n    {receipts}\n) WITH (format = 'PARQUET')",
    )


def _authorize(preflight: Mapping[str, Any]) -> Any:
    policy = load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH'))
    validate_attendance_write_policy(policy)
    return current._authorize(preflight)


def _identity(competition_id: Any, edition_id: Any, observed_at: Any) -> tuple[str, str, str]:
    if isinstance(competition_id, bool) or not isinstance(competition_id, (str, int)) \
            or re.fullmatch(r'[A-Za-z0-9_-]+', str(competition_id)) is None:
        raise current.CurrentWriteError('attendance competition identity is invalid')
    if isinstance(edition_id, bool) or not isinstance(edition_id, (str, int)) \
            or re.fullmatch(r'[0-9]+', str(edition_id)) is None \
            or not 1800 <= int(edition_id) <= 2199:
        raise current.CurrentWriteError('attendance edition identity is invalid')
    try:
        observed = canonical_timestamp(observed_at)
    except Exception as exc:
        raise current.CurrentWriteError('attendance observed_at is invalid') from exc
    return str(competition_id), str(int(edition_id)), observed


def _expected_ids(expected_club_ids: Iterable[Any]) -> tuple[str, ...]:
    try:
        values = tuple(canonical_club_id(value) for value in expected_club_ids)
    except Exception as exc:
        raise current.CurrentWriteError('attendance expected club IDs are invalid') from exc
    if len(set(values)) != len(values):
        raise current.CurrentWriteError('attendance expected club IDs contain duplicates')
    return tuple(sorted(values, key=int))


def _validate_extraction(extraction: AttendanceExtraction, expected: tuple[str, ...],
                         identity: tuple[str, str, str]) -> None:
    if not isinstance(extraction, AttendanceExtraction):
        raise current.CurrentWriteError('extraction must be an AttendanceExtraction')
    dq = build_attendance_dq(extraction, expected)
    if dq['structural_passed'] is not True:
        raise current.CurrentWriteError(
            'attendance extraction did not pass structural DQ: ' + ', '.join(dq['errors']))
    competition, edition, observed = identity
    keys = set()
    for row in extraction.rows:
        key = tuple(row[column] for column in ATTENDANCE_KEY)
        if key in keys:
            raise current.CurrentWriteError('attendance extraction has duplicate natural keys')
        keys.add(key)
        if key != (competition, edition, row['club_id'], observed):
            raise current.CurrentWriteError('attendance rows differ from edition identity')


def _frame(extraction: AttendanceExtraction) -> pd.DataFrame:
    frame = pd.DataFrame([dict(row) for row in extraction.rows], columns=ATTENDANCE_COLUMNS)
    for column in TIMESTAMP_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
    return frame


def _canonical_cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in TIMESTAMP_COLUMNS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert('UTC').tz_localize(None)
        return stamp.isoformat(timespec='microseconds')
    if column in INTEGER_COLUMNS:
        return int(value)
    if column == 'final_season':
        return bool(value)
    return str(value)


def _canonical_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    canonical = [
        {column: _canonical_cell(column, row[column]) for column in ATTENDANCE_COLUMNS}
        for row in rows]
    return sorted(canonical, key=lambda row: tuple(row[column] for column in ATTENDANCE_KEY))


def _physical(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    canonical = _canonical_rows(rows)
    return {
        'rows': len(canonical),
        'physical_hash': hashlib.sha256(_canonical_json(canonical)).hexdigest(),
    }


def _read_existing(connection, identity: tuple[str, str, str]) -> pd.DataFrame:
    competition, edition, observed = identity
    try:
        return current.run._query_dataframe(
            connection,
            f'SELECT {", ".join(ATTENDANCE_COLUMNS)} FROM {ATTENDANCE_TABLE} '
            'WHERE competition_id = ? AND edition_id = ? AND observed_at = ?',
            [competition, edition,
             pd.Timestamp(observed).tz_convert('UTC').tz_localize(None).to_pydatetime()])
    except Exception as exc:
        if current._missing_table(exc):
            return pd.DataFrame(columns=list(ATTENDANCE_COLUMNS))
        raise


def _natural_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row['competition_id']), str(row['edition_id']), str(row['club_id']),
        _canonical_cell('observed_at', row['observed_at']),
    )


def _validate_existing(existing: pd.DataFrame, extraction: AttendanceExtraction) -> None:
    if existing.empty:
        return
    immutable = tuple(column for column in ATTENDANCE_COLUMNS if column not in ASSOCIATION_FIELDS)
    incoming = {_natural_key(row): row for row in extraction.rows}
    for values in existing.itertuples(index=False, name=None):
        old = {column: value for column, value in zip(existing.columns, values)}
        key = _natural_key(old)
        new = incoming.get(key)
        if new is None:
            continue
        changed = tuple(column for column in immutable
                        if _canonical_cell(column, new[column]) != _canonical_cell(column, old[column]))
        if changed:
            raise current.CurrentWriteError(
                f'immutable attendance fields differ for {key}: {", ".join(changed)}')
        incoming_stamp = _canonical_cell('extracted_at', new['extracted_at'])
        existing_stamp = _canonical_cell('extracted_at', old['extracted_at'])
        if incoming_stamp < existing_stamp:
            raise current.CurrentWriteError(f'stale attendance extraction for {key}')
        if incoming_stamp == existing_stamp:
            changed = tuple(column for column in ASSOCIATION_FIELDS
                            if _canonical_cell(column, new[column]) != _canonical_cell(column, old[column]))
            if changed:
                raise current.CurrentWriteError(
                    f'conflicting attendance association for {key}: {", ".join(changed)}')


def _verify_physical(existing: pd.DataFrame, extraction: AttendanceExtraction) -> dict[str, Any]:
    actual = _physical(
        {column: value for column, value in zip(existing.columns, values)}
        for values in existing.itertuples(index=False, name=None))
    expected = _physical(extraction.rows)
    if actual != expected:
        raise current.CurrentWriteError('attendance physical read-back differs from the plan')
    return actual


def attendance_plan_sha256(extraction: AttendanceExtraction, expected: tuple[str, ...],
                           identity: tuple[str, str, str]) -> str:
    plan = {
        'target_table': ATTENDANCE_TABLE,
        'competition_id': identity[0],
        'edition_id': identity[1],
        'observed_at': identity[2],
        'expected_club_ids': list(expected),
        'rows': _canonical_rows(extraction.rows),
        'universe_version': extraction.universe_version,
        'run_id': extraction.run_id,
        'scope_id': extraction.scope_id,
        'extracted_at': extraction.extracted_at,
        'batch_id': extraction.batch_id,
        'final_season': extraction.final_season,
        'final_season_proof': extraction.final_season_proof,
    }
    return hashlib.sha256(_canonical_json(plan)).hexdigest()


def _receipt_values(extraction: AttendanceExtraction, expected: tuple[str, ...],
                    identity: tuple[str, str, str], physical_hash: str,
                    committed_at: datetime) -> dict[str, Any]:
    if not isinstance(committed_at, datetime) or committed_at.tzinfo is None \
            or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    competition, edition, observed = identity
    plan = attendance_plan_sha256(extraction, expected, identity)
    statuses = {}
    hashes = {}
    for row in extraction.rows:
        status = row['observation_status']
        statuses[status] = statuses.get(status, 0) + 1
        hashes[row['source_capture_id']] = row['source_body_hash']
    if not statuses:
        statuses[extraction.result.status] = 1
    return {
        'receipt_id': 'tm-attendance-receipt-' + hashlib.sha256(plan.encode()).hexdigest()[:32],
        'target_table': ATTENDANCE_TABLE,
        'competition_id': competition,
        'edition_id': edition,
        'observed_at': pd.Timestamp(observed).tz_convert('UTC').tz_localize(None).to_pydatetime(),
        'capture_status': extraction.result.status,
        'universe_version': extraction.universe_version,
        'run_id': extraction.run_id,
        'cycle_id': extraction.run_id,
        'parser_revision': ATTENDANCE_PARSER_REVISION,
        'schema_revision': ATTENDANCE_SCHEMA_REVISION,
        'natural_key_columns_json': _canonical_json(list(ATTENDANCE_KEY)).decode(),
        'expected_club_ids_json': _canonical_json(list(expected)).decode(),
        'observed_club_ids_json': _canonical_json(sorted({row['club_id'] for row in extraction.rows}, key=int)).decode(),
        'expected_clubs': len(expected),
        'row_count': len(extraction.rows),
        'status_counts_json': _canonical_json(statuses).decode(),
        'final_season': extraction.final_season,
        'final_season_proof': extraction.final_season_proof,
        'raw_capture_ids_json': _canonical_json(sorted(hashes)).decode(),
        'raw_body_hashes_json': _canonical_json(hashes).decode(),
        'plan_sha256': plan,
        'physical_hash': physical_hash,
        'committed_batch': extraction.batch_id,
        'committed_at': committed_at.astimezone(timezone.utc).replace(tzinfo=None),
    }


def _receipt_id(extraction: AttendanceExtraction, expected: tuple[str, ...],
                identity: tuple[str, str, str]) -> str:
    return _receipt_values(extraction, expected, identity, '', _RECEIPT_ID_STAMP)['receipt_id']


def _stored_receipt(connection, receipt_id: str) -> pd.DataFrame:
    return current.run._query_dataframe(
        connection,
        f'SELECT {", ".join(RECEIPT_COLUMNS)} FROM {ATTENDANCE_RECEIPTS_TABLE} '
        'WHERE receipt_id = ?', [receipt_id])


def _receipt_row(row: Mapping[str, Any]) -> dict[str, Any]:
    values = {}
    for column in RECEIPT_COLUMNS:
        value = row[column]
        if column in _RECEIPT_TIMESTAMPS:
            if value is None or pd.isna(value):
                raise current.CurrentWriteError(f'attendance receipt {column} is missing')
            values[column] = pd.Timestamp(value).to_pydatetime()
        elif value is None or pd.isna(value):
            values[column] = None
        elif column in _RECEIPT_COUNTS:
            values[column] = int(value)
        elif column == 'final_season':
            values[column] = bool(value)
        else:
            values[column] = str(value)
    return values


def _canonical_receipt_cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in _RECEIPT_TIMESTAMPS:
        return pd.Timestamp(value).isoformat()
    if column in _RECEIPT_COUNTS:
        return int(value)
    if column == 'final_season':
        return bool(value)
    return str(value)


def _require_equal_receipts(actual: Mapping[str, Any], expected: Mapping[str, Any], stage: str) -> None:
    changed = tuple(column for column in RECEIPT_COLUMNS
                    if _canonical_receipt_cell(column, actual[column])
                    != _canonical_receipt_cell(column, expected[column]))
    if changed:
        raise current.CurrentWriteError(
            f'attendance receipt {stage} differs: ' + ', '.join(changed))


def _merge_receipt(connection, values: Mapping[str, Any]) -> None:
    columns = ', '.join(RECEIPT_COLUMNS)
    literals = ', '.join(
        _sql(value.replace(tzinfo=timezone.utc)) if column in _RECEIPT_TIMESTAMPS
        else _sql(value)
        for column, value in values.items())
    updates = ', '.join(f'{column} = s.{column}' for column in RECEIPT_COLUMNS
                        if column != 'receipt_id')
    current.run._execute_cursor(
        connection,
        f'MERGE INTO {ATTENDANCE_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) '
        'ON t.receipt_id = s.receipt_id '
        f'WHEN MATCHED THEN UPDATE SET {updates} '
        f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
        + ', '.join(f's.{column}' for column in RECEIPT_COLUMNS) + ')')


def _return_receipt(values: Mapping[str, Any], physical: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(values)
    receipt.update({
        'verified': True,
        'receipt_readback': True,
        'physical': dict(physical),
        'natural_key_columns': json.loads(values['natural_key_columns_json']),
        'expected_club_ids': json.loads(values['expected_club_ids_json']),
        'observed_club_ids': json.loads(values['observed_club_ids_json']),
        'status_counts': json.loads(values['status_counts_json']),
        'raw_capture_ids': json.loads(values['raw_capture_ids_json']),
        'raw_body_hashes': json.loads(values['raw_body_hashes_json']),
        'committed_at': values['committed_at'].replace(tzinfo=timezone.utc).isoformat(),
        'observed_at': values['observed_at'].replace(tzinfo=timezone.utc).isoformat(),
    })
    return receipt


def write_attendance_observations(
    scraper,
    extraction: AttendanceExtraction,
    expected_club_ids: Iterable[Any],
    preflight: Mapping[str, Any],
    *,
    competition_id: Any,
    edition_id: Any,
    observed_at: Any,
    committed_at: datetime | None = None,
) -> dict[str, Any]:
    """Commit one complete edition snapshot and read back rows and receipt."""
    identity = _identity(competition_id, edition_id, observed_at)
    expected = _expected_ids(expected_club_ids)
    _authorize(preflight)
    _validate_extraction(extraction, expected, identity)
    if committed_at is None:
        committed_at = datetime.now(timezone.utc)
    if not isinstance(committed_at, datetime) or committed_at.tzinfo is None \
            or committed_at.utcoffset() is None:
        raise current.CurrentWriteError('committed_at must be timezone-aware')
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in attendance_ddl():
                current.run._execute_cursor(connection, ddl)
            existing = _read_existing(connection, identity)
            stored = _stored_receipt(connection, _receipt_id(extraction, expected, identity))
            if len(stored) > 1:
                raise current.CurrentWriteError('attendance receipts contain duplicate plan IDs')
            if len(stored) == 1:
                _validate_existing(existing, extraction)
                physical = _verify_physical(existing, extraction)
                stored_values = _receipt_row(stored.iloc[0])
                expected_values = _receipt_values(
                    extraction, expected, identity, physical['physical_hash'],
                    stored_values['committed_at'].replace(tzinfo=timezone.utc))
                _require_equal_receipts(stored_values, expected_values, 'stored')
                return _return_receipt(stored_values, physical)
            _validate_existing(existing, extraction)
            frame = _frame(extraction)
            if not frame.empty:
                scraper._iceberg_writer.write_dataframe(
                    df=frame,
                    database='bronze',
                    table='transfermarkt_attendance_observations',
                    merge_keys=list(ATTENDANCE_KEY),
                    add_metadata=False,
                    source='transfermarkt',
                )
            committed = _read_existing(connection, identity)
            physical = _verify_physical(committed, extraction)
            _validate_existing(committed, extraction)
            _authorize(preflight)
            values = _receipt_values(
                extraction, expected, identity, physical['physical_hash'], committed_at)
            _merge_receipt(connection, values)
            readback = _stored_receipt(connection, values['receipt_id'])
            if len(readback) != 1:
                raise current.CurrentWriteError('attendance receipt read-back is missing')
            _require_equal_receipts(_receipt_row(readback.iloc[0]), values, 'read-back')
            return _return_receipt(values, physical)
        finally:
            connection.close()


__all__ = [
    'ASSOCIATION_FIELDS', 'ATTENDANCE_CONTRACT', 'ATTENDANCE_RECEIPTS_TABLE',
    'RECEIPT_COLUMNS', 'TIMESTAMP_COLUMNS', 'attendance_ddl',
    'attendance_plan_sha256', 'validate_attendance_write_policy',
    'write_attendance_observations',
]
