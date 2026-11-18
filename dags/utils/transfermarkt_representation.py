"""Isolated writer contract for offline player representation observations.

No DAG or current runner imports this module. Production policy deliberately does
not grant these tables in this task; tests use a temporary explicit policy and an
in-memory physical target.
"""
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
from scrapers.transfermarkt.representation import (
    ASSOCIATION_FIELDS,
    GROUP_KEY,
    REPRESENTATION_COLUMNS,
    REPRESENTATION_KEY,
    REPRESENTATION_PARSER_REVISION,
    REPRESENTATION_SCHEMA_REVISION,
    TIMESTAMP_COLUMNS,
    RepresentationExtraction,
    build_representation_dq,
    canonical_json,
    representation_plan_sha256,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock

REPRESENTATION_TABLE = 'iceberg.bronze.transfermarkt_player_representation_observations'
REPRESENTATION_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_player_representation_receipts_v1'
REPRESENTATION_CONTRACT = {
    'table': REPRESENTATION_TABLE,
    'key_columns': REPRESENTATION_KEY,
    'parser_revision': REPRESENTATION_PARSER_REVISION,
    'schema_revision': REPRESENTATION_SCHEMA_REVISION,
    'write_mode': 'native_merge',
}
RECEIPT_COLUMNS = (
    'receipt_id', 'target_table', 'assessment_manifest_id', 'universe_version',
    'run_id', 'cycle_id', 'parser_revision', 'schema_revision',
    'natural_key_columns_json', 'expected_players', 'expected_groups',
    'row_count', 'status_counts_json', 'raw_capture_ids_json', 'raw_body_hashes_json',
    'plan_sha256', 'physical_hash', 'committed_batch', 'committed_at',
)
_RECEIPT_COUNT_COLUMNS = frozenset({'expected_players', 'expected_groups', 'row_count'})
_RECEIPT_ID_STAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)


def validate_representation_write_policy(policy) -> None:
    allowed = set(policy.allowed_write_tables)
    missing = tuple(
        table for table in (REPRESENTATION_TABLE, REPRESENTATION_RECEIPTS_TABLE)
        if table not in allowed
    )
    if missing:
        raise current.CurrentWriteError(
            'standing policy does not authorize representation writes to: '
            + ', '.join(missing))


def representation_ddl() -> tuple[str, str]:
    business_columns = ',\n    '.join(
        f'{column} timestamp(6)' if column in TIMESTAMP_COLUMNS else f'{column} varchar'
        for column in REPRESENTATION_COLUMNS
    )
    receipt_bigint = frozenset({'expected_players', 'expected_groups', 'row_count'})
    receipt_timestamp = frozenset({'committed_at'})
    receipt_columns = ',\n    '.join(
        f'{column} timestamp(6)' if column in receipt_timestamp
        else f'{column} bigint' if column in receipt_bigint
        else f'{column} varchar'
        for column in RECEIPT_COLUMNS
    )
    return (
        (
            f'CREATE TABLE IF NOT EXISTS {REPRESENTATION_TABLE} '
            f'(\n    {business_columns}\n)\n'
            f"WITH (format = 'PARQUET')"
        ),
        (
            f'CREATE TABLE IF NOT EXISTS {REPRESENTATION_RECEIPTS_TABLE} '
            f'(\n    {receipt_columns}\n)\n'
            f"WITH (format = 'PARQUET')"
        ),
    )


def _authorize(preflight: Mapping[str, Any]) -> Any:
    """Authorize one representation write under the standing policy."""
    policy = load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH'))
    validate_representation_write_policy(policy)
    return current._authorize(preflight)


def _frame(extraction: RepresentationExtraction) -> pd.DataFrame:
    """Validate one extraction and render its Trino frame in column order."""
    if not isinstance(extraction, RepresentationExtraction):
        raise current.CurrentWriteError('extraction must be a RepresentationExtraction')
    rows = extraction.rows
    if not rows:
        raise current.CurrentWriteError('extraction has no observation rows')
    if build_representation_dq(extraction)['structural_passed'] is not True:
        raise current.CurrentWriteError('extraction did not pass structural data quality')
    keys = [tuple(row[column] for column in REPRESENTATION_KEY) for row in rows]
    if len(set(keys)) != len(keys):
        raise current.CurrentWriteError('extraction has duplicate representation keys')
    frame = pd.DataFrame([dict(row) for row in rows], columns=REPRESENTATION_COLUMNS)
    for column in TIMESTAMP_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
    return frame


def _canonical_cell(column: str, value: Any) -> Any:
    """Canonical scalar for exact plan/physical comparison; UTC microsecond stamps."""
    if value is None or pd.isna(value):
        return None
    if column in TIMESTAMP_COLUMNS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert('UTC').tz_localize(None)
        return stamp.isoformat(timespec='microseconds')
    return current._cell(column, value)


def _read_existing_groups(connection, extraction: RepresentationExtraction) -> pd.DataFrame:
    """Read every existing physical row of any incoming observation group."""
    groups = sorted({tuple(row[column] for column in GROUP_KEY) for row in extraction.rows})
    pieces: list[pd.DataFrame] = []
    for start in range(0, len(groups), 100):
        chunk = groups[start:start + 100]
        predicate = ' OR '.join(
            '(' + ' AND '.join(f'{column} = ?' for column in GROUP_KEY) + ')'
            for _ in chunk)
        parameters = [value for group in chunk for value in group]
        try:
            piece = current.run._query_dataframe(
                connection,
                f'SELECT {", ".join(REPRESENTATION_COLUMNS)} FROM {REPRESENTATION_TABLE} '
                f'WHERE {predicate}',
                parameters)
        except Exception as exc:
            if not current._missing_table(exc):
                raise
            piece = pd.DataFrame(columns=list(REPRESENTATION_COLUMNS))
        pieces.append(piece)
    existing = (pd.concat(pieces, ignore_index=True) if pieces
                else pd.DataFrame(columns=list(REPRESENTATION_COLUMNS)))
    return existing.sort_values(list(REPRESENTATION_KEY), kind='stable').reset_index(drop=True)


def _validate_existing_groups(existing: pd.DataFrame, extraction: RepresentationExtraction) -> None:
    """Validate every incoming group that already exists physically; never writes."""
    if existing.empty:
        return
    immutable_columns = tuple(
        column for column in REPRESENTATION_COLUMNS if column not in ASSOCIATION_FIELDS)
    incoming_rows = [
        {column: _canonical_cell(column, row[column]) for column in REPRESENTATION_COLUMNS}
        for row in extraction.rows]
    existing_rows = [
        {column: _canonical_cell(column, value) for column, value in zip(existing.columns, row)}
        for row in existing.itertuples(index=False, name=None)]
    incoming_by_key = {
        tuple(row[column] for column in REPRESENTATION_KEY): row for row in incoming_rows}
    planned_groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in incoming_rows:
        planned_groups.setdefault(tuple(row[column] for column in GROUP_KEY), []).append(row)
    existing_groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in existing_rows:
        existing_groups.setdefault(tuple(row[column] for column in GROUP_KEY), []).append(row)
    for group_key in sorted(existing_groups):
        members = existing_groups[group_key]
        for existing_row in members:
            natural_key = tuple(existing_row[column] for column in REPRESENTATION_KEY)
            incoming_row = incoming_by_key.get(natural_key)
            if incoming_row is None:
                continue
            changed = tuple(column for column in immutable_columns
                            if incoming_row[column] != existing_row[column])
            if changed:
                raise current.CurrentWriteError(
                    'immutable representation fields differ for existing natural key '
                    f'{natural_key}: {", ".join(changed)}')
            incoming_stamp = incoming_row['extracted_at']
            existing_stamp = existing_row['extracted_at']
            if incoming_stamp != existing_stamp and (
                    incoming_stamp is None or existing_stamp is None
                    or incoming_stamp < existing_stamp):
                raise current.CurrentWriteError(
                    'stale representation extraction for existing natural key '
                    f'{natural_key}: incoming extracted_at {incoming_stamp} '
                    f'precedes existing {existing_stamp}')
            if incoming_stamp == existing_stamp:
                differing = tuple(column for column in ASSOCIATION_FIELDS
                                  if column != 'extracted_at'
                                  and incoming_row[column] != existing_row[column])
                if differing:
                    raise current.CurrentWriteError(
                        'conflicting representation association for existing natural key '
                        f'{natural_key} at equal extracted_at: {", ".join(differing)}')
        combined = list(planned_groups.get(group_key, [])) + [
            row for row in members
            if tuple(row[column] for column in REPRESENTATION_KEY) not in incoming_by_key]
        statuses = sorted({row['observation_status'] for row in combined})
        if len(statuses) > 1:
            raise current.CurrentWriteError(
                f'contradictory representation group {group_key}: {", ".join(statuses)}')
        agencies: dict[str, set[str | None]] = {}
        for row in combined:
            if row['observation_status'] == 'assigned':
                agencies.setdefault(row['evidence_locator'], set()).add(row['representative_id'])
        for locator in sorted(item for item, ids in agencies.items() if len(ids) > 1):
            raise current.CurrentWriteError(
                f'ambiguous representation agency for group {group_key} '
                f'at evidence_locator {locator}')


def _plan_sha256(extraction: RepresentationExtraction) -> str:
    """Deterministic identity of one immutable write plan; no commit clock."""
    return representation_plan_sha256(extraction)


def _receipt_values(
    extraction: RepresentationExtraction,
    physical_hash: str,
    committed_at: datetime,
) -> dict[str, Any]:
    """Receipt column values for one committed plan, in RECEIPT_COLUMNS order."""
    if (not isinstance(committed_at, datetime)
            or committed_at.tzinfo is None or committed_at.utcoffset() is None):
        raise current.CurrentWriteError('committed_at must be a timezone-aware datetime')
    committed_utc = committed_at.astimezone(timezone.utc).replace(tzinfo=None)
    plan_sha256 = _plan_sha256(extraction)
    status_counts: dict[str, int] = {}
    body_hashes: dict[str, str] = {}
    for row in extraction.rows:
        status = row['observation_status']
        status_counts[status] = status_counts.get(status, 0) + 1
        if row['raw_capture_id'] is not None:
            body_hashes[row['raw_capture_id']] = row['source_body_hash']
    return {
        'receipt_id': 'tm-representation-receipt-'
                      + hashlib.sha256(plan_sha256.encode('utf-8')).hexdigest()[:32],
        'target_table': REPRESENTATION_TABLE,
        'assessment_manifest_id': extraction.universe.assessment_manifest_id,
        'universe_version': extraction.universe.universe_version,
        'run_id': extraction.run_id,
        'cycle_id': extraction.run_id,
        'parser_revision': REPRESENTATION_PARSER_REVISION,
        'schema_revision': REPRESENTATION_SCHEMA_REVISION,
        'natural_key_columns_json': canonical_json(list(REPRESENTATION_KEY)).decode('utf-8'),
        'expected_players': len(extraction.universe.players),
        'expected_groups': len(extraction.expected_groups),
        'row_count': len(extraction.rows),
        'status_counts_json': canonical_json(status_counts).decode('utf-8'),
        'raw_capture_ids_json': canonical_json(sorted(body_hashes)).decode('utf-8'),
        'raw_body_hashes_json': canonical_json(body_hashes).decode('utf-8'),
        'plan_sha256': plan_sha256,
        'physical_hash': physical_hash,
        'committed_batch': extraction.batch_id,
        'committed_at': committed_utc,
    }


def _return_receipt(
    values: Mapping[str, Any],
    physical: Mapping[str, Any],
) -> dict[str, Any]:
    """Readback receipt: stored values plus decoded JSON and verification flags."""
    receipt: dict[str, Any] = dict(values)
    receipt['verified'] = True
    receipt['receipt_readback'] = True
    receipt['physical'] = physical
    receipt['physical_hash'] = physical['physical_hash']
    receipt['natural_key_columns'] = json.loads(values['natural_key_columns_json'])
    receipt['status_counts'] = json.loads(values['status_counts_json'])
    receipt['raw_capture_ids'] = json.loads(values['raw_capture_ids_json'])
    receipt['raw_body_hashes'] = json.loads(values['raw_body_hashes_json'])
    receipt['committed_at'] = values['committed_at'].replace(tzinfo=timezone.utc).isoformat()
    return receipt


def _receipt_id(extraction: RepresentationExtraction) -> str:
    """Deterministic receipt identity of one plan; no commit clock or proof input."""
    return _receipt_values(extraction, '', _RECEIPT_ID_STAMP)['receipt_id']


def _verify_committed_groups(
    existing: pd.DataFrame,
    extraction: RepresentationExtraction,
) -> None:
    """Physical proof that every planned row is committed exactly once, unchanged."""
    if existing.empty:
        raise current.CurrentWriteError(
            'physical read-back differs: no committed representation rows for the plan')
    counts: dict[tuple[str, ...], int] = {}
    cells_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in existing.itertuples(index=False, name=None):
        cells = {column: _canonical_cell(column, value)
                 for column, value in zip(existing.columns, row)}
        key = tuple(cells[column] for column in REPRESENTATION_KEY)
        counts[key] = counts.get(key, 0) + 1
        cells_by_key[key] = cells
    incoming = {tuple(row[column] for column in REPRESENTATION_KEY): row
                for row in extraction.rows}
    for key in sorted(incoming):
        if counts.get(key, 0) != 1:
            raise current.CurrentWriteError(
                f'physical read-back differs for natural key {key}: '
                f'expected exactly one committed row, found {counts.get(key, 0)}')
        committed = cells_by_key[key]
        changed = tuple(
            column for column in REPRESENTATION_COLUMNS
            if column not in ASSOCIATION_FIELDS
            and _canonical_cell(column, incoming[key][column]) != committed[column])
        if changed:
            raise current.CurrentWriteError(
                f'physical read-back differs for natural key {key}: '
                + ', '.join(changed))


def _canonical_receipt_cell(column: str, value: Any) -> Any:
    """Canonical receipt scalar; DBAPI stamps compare through pd.Timestamp."""
    if value is None or pd.isna(value):
        return None
    if column == 'committed_at':
        return pd.Timestamp(value)
    if column in _RECEIPT_COUNT_COLUMNS:
        return int(value)
    return str(value)


def _receipt_row_values(row: Mapping[str, Any]) -> dict[str, Any]:
    """Python scalar values of one physical receipt row, in RECEIPT_COLUMNS order."""
    values: dict[str, Any] = {}
    for column in RECEIPT_COLUMNS:
        value = row[column]
        if column == 'committed_at':
            if value is None or pd.isna(value):
                raise current.CurrentWriteError(
                    'representation receipt committed_at is missing')
            values[column] = pd.Timestamp(value).to_pydatetime()
        elif value is None or pd.isna(value):
            values[column] = None
        elif column in _RECEIPT_COUNT_COLUMNS:
            values[column] = int(value)
        else:
            values[column] = str(value)
    return values


def _require_equal_receipts(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    stage: str,
) -> None:
    """Require every receipt column of two value mappings to match exactly."""
    differing = tuple(
        column for column in RECEIPT_COLUMNS
        if _canonical_receipt_cell(column, actual[column])
        != _canonical_receipt_cell(column, expected[column]))
    if differing:
        raise current.CurrentWriteError(
            f'representation receipt {stage} differs: ' + ', '.join(differing))


def _merge_receipt(connection, values: Mapping[str, Any]) -> None:
    """Upsert exactly one receipt row; the deterministic receipt_id is the merge key."""
    columns = ', '.join(RECEIPT_COLUMNS)
    literals = ', '.join(
        _sql(value.replace(tzinfo=timezone.utc)) if column == 'committed_at'
        else _sql(value)
        for column, value in values.items())
    updates = ', '.join(f'{column} = s.{column}' for column in RECEIPT_COLUMNS
                        if column != 'receipt_id')
    current.run._execute_cursor(
        connection,
        f'MERGE INTO {REPRESENTATION_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) '
        'ON t.receipt_id = s.receipt_id '
        f'WHEN MATCHED THEN UPDATE SET {updates} '
        f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
        + ', '.join(f's.{column}' for column in RECEIPT_COLUMNS) + ')')


def _stored_receipt(connection, receipt_id: str) -> pd.DataFrame:
    """Parameterized lookup of one deterministic receipt by its identity."""
    return current.run._query_dataframe(
        connection,
        f'SELECT {", ".join(RECEIPT_COLUMNS)} FROM {REPRESENTATION_RECEIPTS_TABLE} '
        'WHERE receipt_id = ?', [receipt_id])


def write_representation_observations(
    scraper,
    extraction: RepresentationExtraction,
    preflight: Mapping[str, Any],
    *,
    committed_at: datetime | None = None,
) -> dict[str, Any]:
    """Write one immutable extraction plan, with physical and receipt readback."""
    _authorize(preflight)
    if committed_at is None:
        committed_at = datetime.now(timezone.utc)
    if (not isinstance(committed_at, datetime)
            or committed_at.tzinfo is None or committed_at.utcoffset() is None):
        raise current.CurrentWriteError('committed_at must be a timezone-aware datetime')
    if not isinstance(extraction, RepresentationExtraction):
        raise current.CurrentWriteError('extraction must be a RepresentationExtraction')
    dq = build_representation_dq(extraction)
    if dq['structural_passed'] is not True:
        raise current.CurrentWriteError(
            'representation extraction did not pass structural DQ: '
            + ', '.join(sorted(dq['errors'])))
    frame = _frame(extraction)
    output = SimpleNamespace(
        table_name='transfermarkt_player_representation_observations',
        key='representation', is_legacy=False, replace_keys=())
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in representation_ddl():
                current.run._execute_cursor(connection, ddl)
            existing = _read_existing_groups(connection, extraction)
            stored = _stored_receipt(connection, _receipt_id(extraction))
            if len(stored) > 1:
                raise current.CurrentWriteError(
                    'representation receipts hold more than one row for one plan')
            if len(stored) == 1:
                _verify_committed_groups(existing, extraction)
                _validate_existing_groups(existing, extraction)
                physical = current._read_back(scraper, output, frame, extraction.run_id)
                stored_values = _receipt_row_values(stored.iloc[0])
                expected = _receipt_values(
                    extraction, physical['physical_hash'],
                    stored_values['committed_at'].replace(tzinfo=timezone.utc))
                _require_equal_receipts(stored_values, expected, 'stored')
                return _return_receipt(stored_values, physical)
            _validate_existing_groups(existing, extraction)
            scraper._iceberg_writer.write_dataframe(
                df=frame,
                database='bronze',
                table='transfermarkt_player_representation_observations',
                merge_keys=list(REPRESENTATION_KEY),
                add_metadata=False,
                source='transfermarkt',
            )
            physical = current._read_back(scraper, output, frame, extraction.run_id)
            merged = _read_existing_groups(connection, extraction)
            _verify_committed_groups(merged, extraction)
            _validate_existing_groups(merged, extraction)
            _authorize(preflight)
            values = _receipt_values(extraction, physical['physical_hash'], committed_at)
            _merge_receipt(connection, values)
            readback = _stored_receipt(connection, values['receipt_id'])
            if len(readback) != 1:
                raise current.CurrentWriteError(
                    'representation receipt read-back after merge is missing')
            _require_equal_receipts(_receipt_row_values(readback.iloc[0]), values, 'read-back')
            return _return_receipt(values, physical)
        finally:
            connection.close()
