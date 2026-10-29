"""Independent native staff writes and a read-only coach-role projection.

Staff is outside the coach dual-write bundle and its readiness manifests.
Only a physically verified full club snapshot receives its own receipt.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import re
from types import SimpleNamespace
from urllib.parse import urlsplit

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from dags.utils.transfermarkt_current_state import _sql
from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.models import FetchStatus
from scrapers.transfermarkt.staff import (
    STAFF_COLUMNS, STAFF_KEY, STAFF_PARSER_REVISION, STAFF_TABLE,
    STAFF_RECEIPTS_TABLE, parse_staff,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock


STAFF_CONTRACT = {
    'table': STAFF_TABLE, 'key_columns': STAFF_KEY,
    'parser_revision': STAFF_PARSER_REVISION, 'schema_revision': '1',
    'empty_policy': 'reject', 'write_mode': 'native_merge',
}
_COLUMNS = STAFF_COLUMNS + ['raw_capture_id'] + tm._SCOPE_LINEAGE_COLUMNS + tm._METADATA_COLUMNS
_RECEIPT_COLUMNS = (
    'club_id', 'observed_day', 'observed_at', 'raw_capture_id', 'source_body_hash',
    'parser_revision', 'bronze_manifest', 'row_count', 'physical_hash', 'committed_at',
)


def validate_staff_write_policy(policy):
    if not {STAFF_TABLE, STAFF_RECEIPTS_TABLE} <= set(policy.allowed_write_tables):
        raise current.CurrentWriteError('standing policy does not authorize staff tables')


def _authorize(preflight):
    path = os.environ.get('TM_STANDING_POLICY_PATH', '/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json')
    validate_staff_write_policy(load_standing_policy(path))
    return current._authorize(preflight)


def staff_ddl():
    types = {'appointed_date': 'date', 'observed_day': 'date',
             'observed_at': 'timestamp(6)', 'fetched_at': 'timestamp(6)',
             '_ingested_at': 'timestamp(6)'}
    columns = ', '.join(f'{col} {types.get(col, "varchar")}' for col in _COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {STAFF_TABLE} ({columns}) WITH (format = 'PARQUET')",
        f'''CREATE TABLE IF NOT EXISTS {STAFF_RECEIPTS_TABLE} (
club_id varchar, observed_day date, observed_at timestamp(6), raw_capture_id varchar,
source_body_hash varchar, parser_revision varchar, bronze_manifest varchar,
row_count bigint, physical_hash varchar, committed_at timestamp(6))
WITH (format = 'PARQUET')''',
    )


def _staff_frame(scraper, scope, cycle_id, club_id, outcome, source_url):
    if not re.fullmatch(r'\d+', str(club_id)):
        raise current.CurrentWriteError('staff club must be a numeric source ID')
    url = urlsplit(source_url)
    if (url.scheme != 'https' or url.netloc != 'www.transfermarkt.com'
        or not re.fullmatch(r'/[^/]+/mitarbeiter/verein/' + str(club_id), url.path)
        or url.query or url.fragment):
        raise current.CurrentWriteError('staff source URL differs from club')
    if outcome.status != FetchStatus.OK or not outcome.raw_capture_id:
        raise current.CurrentWriteError('staff requires successful raw capture')
    body, raw = scraper._http_client._raw_store.load_capture(outcome.raw_capture_id)
    observed = pd.Timestamp(raw.fetched_at)
    if (observed.tzinfo is None or raw.url != source_url or raw.scope_id != scope['scope_id']
        or raw.status_code != 200 or hashlib.sha256(body).hexdigest() != outcome.raw_body_hash
        or pd.Timestamp(outcome.raw_fetched_at) != observed):
        raise current.CurrentWriteError('staff raw lineage differs')
    observed = observed.tz_convert('UTC')
    stamp = observed.tz_localize(None).to_pydatetime()
    rows = parse_staff(body.decode('utf-8'), club_id)
    identity = [STAFF_PARSER_REVISION, str(club_id), raw.capture_id, outcome.raw_body_hash]
    batch = 'tm-staff-' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]
    for row in rows:
        row.update(
            observed_day=observed.date(), observed_at=stamp, fetched_at=stamp,
            raw_capture_id=raw.capture_id, source_url=source_url,
            source_body_hash=outcome.raw_body_hash, parser_revision=STAFF_PARSER_REVISION,
            schema_revision='1', cycle_id=raw.cycle_id, scope_id=scope['scope_id'],
            source_competition_id=scope['competition_id'], source_edition_id=scope['edition_id'],
        )
    frame = tm._with_metadata(rows, STAFF_COLUMNS + ['raw_capture_id'] + tm._SCOPE_LINEAGE_COLUMNS,
                              entity_type='club_staff', batch_id=batch, ingested_at=stamp)
    return frame


@current._bounded_current_writer
def write_current_staff(scraper, scope, preflight, cycle_id, *, club_id, outcome, source_url):
    """Merge one full club capture; never delete an observation or write coaches."""
    _authorize(preflight)
    resolved = current._scope(scraper, scope)
    frame = _staff_frame(scraper, resolved, cycle_id, club_id, outcome, source_url)
    first = frame.iloc[0]
    batch = first['_batch_id']
    output = SimpleNamespace(table_name='transfermarkt_club_staff', key='staff',
                             is_legacy=False, replace_keys=())
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in staff_ddl():
                current.run._execute_cursor(connection, ddl)
            previous = current.run._execute_cursor(connection,
                f'SELECT row_count FROM {STAFF_RECEIPTS_TABLE} WHERE club_id = ? '
                'ORDER BY observed_at DESC LIMIT 1', [str(club_id)], fetch=True)
            if previous and len(frame) < .9 * int(previous[0][0]):
                raise current.CurrentWriteError('staff snapshot below 90% completeness guard')
            # Serialize same-day updates across editions and reject stale captures,
            # including a newer write whose receipt failed before acknowledgement.
            latest = current.run._execute_cursor(connection,
                f'SELECT MAX(observed_at) FROM {STAFF_TABLE} WHERE club_id = ? AND observed_day = ?',
                [str(club_id), first['observed_day']], fetch=True)
            if latest and latest[0][0] is not None:
                at = pd.Timestamp(latest[0][0])
                incoming = pd.Timestamp(first['observed_at'])
                if at > incoming:
                    raise current.CurrentWriteError('newer staff observation already exists')
                if at == incoming:
                    hashes = current.run._execute_cursor(connection,
                        f'SELECT DISTINCT source_body_hash FROM {STAFF_TABLE} '
                        'WHERE club_id = ? AND observed_day = ? AND observed_at = ?',
                        [str(club_id), first['observed_day'], first['observed_at'].to_pydatetime()], fetch=True)
                    if any(row[0] != first['source_body_hash'] for row in hashes):
                        raise current.CurrentWriteError('conflicting staff observations at the same time')
            scraper._iceberg_writer.write_dataframe(
                df=frame, database='bronze', table='transfermarkt_club_staff',
                merge_keys=list(STAFF_KEY), add_metadata=False, source='transfermarkt',
            )
            physical = current._read_back(scraper, output, frame, cycle_id)
            _authorize(preflight)
            values = {
                'club_id': str(club_id), 'observed_day': first['observed_day'],
                'observed_at': first['observed_at'], 'raw_capture_id': first['raw_capture_id'],
                'source_body_hash': first['source_body_hash'], 'parser_revision': STAFF_PARSER_REVISION,
                'bronze_manifest': batch, 'row_count': len(frame),
                'physical_hash': physical['physical_hash'],
                'committed_at': datetime.now(timezone.utc).replace(tzinfo=None),
            }
            columns = ', '.join(values)
            literals = ', '.join("DATE '" + value.isoformat() + "'" if col == 'observed_day'
                                 else _sql(value.replace(tzinfo=timezone.utc)) if isinstance(value, datetime)
                                 else _sql(value) for col, value in values.items())
            updates = ', '.join(f'{col} = s.{col}' for col in values
                                if col not in {'club_id', 'observed_day'})
            current.run._execute_cursor(connection,
                f'MERGE INTO {STAFF_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) '
                'ON t.club_id = s.club_id AND t.observed_day = s.observed_day '
                f'WHEN MATCHED THEN UPDATE SET {updates} '
                f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
                + ', '.join(f's.{col}' for col in values) + ')')
            receipt = current.run._query_dataframe(connection,
                f'SELECT {", ".join(_RECEIPT_COLUMNS)} FROM {STAFF_RECEIPTS_TABLE} '
                'WHERE club_id = ? AND observed_day = ?', [str(club_id), first['observed_day']])
            expected = pd.DataFrame([values], columns=_RECEIPT_COLUMNS)
            # Canonicalize dates/timestamps independently of DBAPI date wrappers.
            def canonical(data):
                return sorted(tuple(pd.Timestamp(value).isoformat() if col in {
                    'observed_day', 'observed_at', 'committed_at'} else str(value)
                    for col, value in zip(data.columns, row))
                    for row in data.itertuples(index=False, name=None))
            if canonical(receipt) != canonical(expected):
                raise current.CurrentWriteError('staff receipt read-back differs')
            return {'verified': True, 'bronze_manifest': batch,
                    'committed_at': values['committed_at'].replace(tzinfo=timezone.utc).isoformat(),
                    'club_id': str(club_id), 'raw_capture_id': first['raw_capture_id'],
                    'source_body_hash': first['source_body_hash'], 'physical': physical}
        finally:
            connection.close()


def coach_roles_sql():
    """SELECT only: last confirmed CLUB capture, unique role, open dated stint.

    A failed newer same-day merge can invalidate the old receipt. Count/lineage
    checks then suppress enrichment until a complete snapshot is confirmed.
    No older personal role or older club receipt is used as a fallback.
    """
    columns = list(dict.fromkeys(tm.COACH_STINT_COLUMNS + tm._METADATA_COLUMNS))
    projection = ', '.join('COALESCE(r.role, c.role) AS role' if col == 'role'
                           else 'c.' + col for col in columns)
    return f'''WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY club_id ORDER BY observed_at DESC) AS rn
    FROM {STAFF_RECEIPTS_TABLE} WHERE parser_revision = '{STAFF_PARSER_REVISION}'
), snapshots AS (
    SELECT s.*, m.row_count,
           COUNT(*) OVER (PARTITION BY s.club_id) AS physical_rows
    FROM ranked m JOIN {STAFF_TABLE} s
      ON s.club_id = m.club_id AND s.observed_day = m.observed_day
     AND s._batch_id = m.bronze_manifest AND s.raw_capture_id = m.raw_capture_id
     AND s.source_body_hash = m.source_body_hash AND s.observed_at = m.observed_at
     AND s.parser_revision = m.parser_revision
    WHERE m.rn = 1
), roles AS (
    SELECT club_id, person_id, observed_day, MIN(role) AS role
    FROM snapshots
    WHERE physical_rows = row_count AND section = 'Coaching Staff'
      AND (appointed_date IS NULL OR appointed_date <= observed_day)
    GROUP BY club_id, person_id, observed_day HAVING COUNT(DISTINCT role) = 1
)
SELECT {projection}
FROM iceberg.bronze.transfermarkt_coach_stints c
LEFT JOIN roles r ON c.club_id = r.club_id AND c.coach_id = r.person_id
 AND c.left_date IS NULL AND c.appointed_date IS NOT NULL
 AND c.appointed_date <= r.observed_day'''
