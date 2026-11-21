"""Append-only Bronze persistence for a complete Transfermarkt rumour feed."""
from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Mapping

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from scrapers.transfermarkt.rumour_observations import (
    RUMOUR_COLUMNS, RUMOUR_KEY, RumourFeedExtraction, RumourPageExtraction, build_rumour_dq,
    digest, page_receipt_fields, physical_hash, utc_clock,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock


RUMOUR_TABLE = 'iceberg.bronze.transfermarkt_rumour_observations'
RUMOUR_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_rumour_receipts_v1'
RECEIPT_COLUMNS = (
    'receipt_id', 'competition_id', 'page_number', 'source_page_url', 'raw_capture_id',
    'source_body_hash', 'captured_at', 'extracted_at', 'parser_revision', 'schema_revision',
    'scope_id', 'run_id', 'cycle_id', 'universe_version', 'plan_sha256', 'pagination_json',
    'capture_status', 'duplicate_count', 'source_rows', 'expected_assertions', 'expected_subjects',
    'status_counts_json', 'physical_hash', 'committed_batch', 'committed_at', 'receipt_hash',
)
_TIMESTAMPS = {'observed_at', 'captured_at', 'extracted_at', '_ingested_at', 'committed_at'}
_DATES = {'last_source_date', 'last_reply_date'}
_INTEGERS = {'age', 'page_number', 'duplicate_count', 'source_rows', 'expected_assertions',
             'expected_subjects'}


def validate_rumour_write_policy(policy) -> None:
    missing = [table for table in (RUMOUR_TABLE, RUMOUR_RECEIPTS_TABLE)
               if table not in policy.allowed_write_tables]
    if missing:
        raise current.CurrentWriteError('standing policy does not authorize rumour writes to '
                                        + ', '.join(missing))


def _authorize(preflight: Mapping[str, Any]) -> None:
    validate_rumour_write_policy(load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH')))
    current._authorize(preflight)


def _type(column: str) -> str:
    if column in _TIMESTAMPS:
        return 'timestamp(6)'
    if column in _DATES:
        return 'date'
    if column in _INTEGERS:
        return 'bigint'
    if column == 'probability_percent':
        return 'double'
    return 'varchar'


def rumour_ddl() -> tuple[str, str]:
    def ddl(table, columns):
        definitions = ', '.join(f'{column} {_type(column)}' for column in columns)
        return f"CREATE TABLE IF NOT EXISTS {table} ({definitions}) WITH (format = 'PARQUET')"

    return ddl(RUMOUR_TABLE, RUMOUR_COLUMNS), ddl(RUMOUR_RECEIPTS_TABLE, RECEIPT_COLUMNS)


def _cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in _TIMESTAMPS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is None:
            stamp = stamp.tz_localize('UTC')
        return utc_clock(stamp.to_pydatetime())
    if column in _DATES:
        return pd.Timestamp(value).date().isoformat()
    if column in _INTEGERS:
        if int(value) != value:
            raise current.CurrentWriteError(f'invalid stored integer in {column}')
        return int(value)
    if column == 'probability_percent':
        return float(value)
    return value


def _parameter(column: str, value: Any) -> Any:
    # Trino timestamp(6) stores UTC wall time. Cast from text to preserve microseconds.
    if column in _TIMESTAMPS:
        return value.removesuffix('Z').replace('T', ' ')
    return value


def _insert_missing(connection, table, columns, keys, rows) -> None:
    if not rows:
        return
    record = '(' + ', '.join(f'CAST(? AS {_type(column)})' for column in columns) + ')'
    names = ', '.join(columns)
    condition = ' AND '.join(f't.{key} = s.{key}' for key in keys)
    projection = ', '.join(f's.{column}' for column in columns)
    sql = (f'MERGE INTO {table} t USING (VALUES ' + ', '.join([record] * len(rows))
           + f') s({names}) ON {condition} WHEN NOT MATCHED THEN INSERT ({names}) '
           + f'VALUES ({projection})')
    params = [_parameter(column, row[column]) for row in rows for column in columns]
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        cursor.fetchall()
    finally:
        cursor.close()


def _read(connection, table, columns, where, params) -> list[dict[str, Any]]:
    frame = current.run._query_dataframe(
        connection, f'SELECT {", ".join(columns)} FROM {table} WHERE {where}', params)
    if len(frame.index) == 0:
        return []
    return [{column: _cell(column, value) for column, value in zip(columns, values)}
            for values in frame.loc[:, list(columns)].itertuples(index=False, name=None)]


def _key(row):
    return tuple(row[k] for k in RUMOUR_KEY)


def _read_page(connection, page, rows):
    # The batch detects unexpected keys (also on empty pages). Global natural
    # keys detect collisions even when the committed batch or provenance differs.
    where = '_batch_id = ?'
    params = [page.batch_id]
    if rows:
        record = '(' + ', '.join(f'CAST(? AS {_type(k)})' for k in RUMOUR_KEY) + ')'
        where += f' OR ({", ".join(RUMOUR_KEY)}) IN (' + ', '.join([record] * len(rows)) + ')'
        params.extend(_parameter(k, row[k]) for row in rows for k in RUMOUR_KEY)
    return _read(connection, RUMOUR_TABLE, RUMOUR_COLUMNS, where, params)


def _missing_rows(existing, expected, *, require_all):
    by_key = {_key(row): row for row in expected}
    seen = set()
    for row in existing:
        key = _key(row)
        if key in seen or key not in by_key:
            raise current.CurrentWriteError('unexpected or duplicate physical rumour row')
        incoming = by_key[key]
        if row['extracted_at'] and row['extracted_at'] > incoming['extracted_at']:
            raise current.CurrentWriteError('stale rumour extraction')
        if incoming != row:
            raise current.CurrentWriteError('immutable rumour payload or lineage differs')
        seen.add(key)
    missing = [row for key, row in by_key.items() if key not in seen]
    if require_all and missing:
        raise current.CurrentWriteError('missing physical rumour rows')
    return missing


def _receipt(connection, receipt_id):
    rows = _read(connection, RUMOUR_RECEIPTS_TABLE, RECEIPT_COLUMNS, 'receipt_id = ?', [receipt_id])
    if len(rows) > 1:
        raise current.CurrentWriteError('duplicate rumour receipt')
    return rows[0] if rows else None


def _write_page(connection, page: RumourPageExtraction, plan, committed_at, preflight):
    fields = page_receipt_fields(page, plan)
    stored = _receipt(connection, fields['receipt_id'])
    rows = page.rows
    existing = _read_page(connection, page, rows)
    missing = _missing_rows(existing, rows, require_all=stored is not None)
    if stored is None:
        _authorize(preflight)
        _insert_missing(connection, RUMOUR_TABLE, RUMOUR_COLUMNS, RUMOUR_KEY, missing)
        existing = _read_page(connection, page, rows)
        _missing_rows(existing, rows, require_all=True)
    if physical_hash(existing) != fields['physical_hash']:
        raise current.CurrentWriteError('physical rumour page hash differs')
    fields['committed_at'] = stored['committed_at'] if stored is not None else committed_at
    if fields['committed_at'] is None:
        raise current.CurrentWriteError('missing rumour receipt commit clock')
    fields['receipt_hash'] = digest(fields)
    if stored is None:
        _authorize(preflight)
        _insert_missing(connection, RUMOUR_RECEIPTS_TABLE, RECEIPT_COLUMNS, ('receipt_id',), [fields])
    elif stored != fields:
        raise current.CurrentWriteError('rumour receipt read-back differs')
    if _receipt(connection, fields['receipt_id']) != fields:
        raise current.CurrentWriteError('rumour receipt missing or read-back differs')
    return fields


def write_rumours(scraper, extraction: RumourFeedExtraction, preflight: Mapping[str, Any],
                  *, committed_at: datetime | None = None) -> dict[str, Any]:
    _authorize(preflight)
    if not isinstance(extraction, RumourFeedExtraction):
        raise current.CurrentWriteError('complete typed rumour feed required')
    if not build_rumour_dq(extraction)['structural_passed']:
        raise current.CurrentWriteError('rumour feed failed structural DQ')
    clock = utc_clock(committed_at if committed_at is not None else datetime.now(timezone.utc))
    plan = extraction.plan_sha256()
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in rumour_ddl():
                current.run._execute_cursor(connection, ddl)
            expected = [_write_page(connection, page, plan, clock, preflight) for page in extraction.pages]
            stored = _read(connection, RUMOUR_RECEIPTS_TABLE, RECEIPT_COLUMNS, 'plan_sha256 = ?', [plan])
            if sorted(stored, key=lambda r: r['page_number']) != expected:
                raise current.CurrentWriteError('rumour feed receipt set differs')
        finally:
            connection.close()
    receipts = [dict(row, verified=True, receipt_readback=True) for row in expected]
    dq = build_rumour_dq(extraction, receipts)
    if not dq['reconciliation_passed']:
        raise current.CurrentWriteError('rumour feed receipt reconciliation failed')
    return dict(verified=True, receipt_readback=True, plan_sha256=plan, receipts=receipts,
                reconciliation=dq)
