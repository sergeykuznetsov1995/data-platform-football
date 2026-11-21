"""Read-only club profile/stadium DQ over current journals and physical Bronze."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from dags.utils.transfermarkt_club_stadiums import (
    CLUB_PROFILE_TABLE, KEYS, RECEIPTS_TABLE, RELATION_TABLE, STADIUM_TABLE,
)

BUSINESS_TABLES = (CLUB_PROFILE_TABLE, STADIUM_TABLE, RELATION_TABLE)
LINEAGE = (
    'raw_capture_id', 'raw_body_hash', 'source_url', 'captured_at', 'observed_at',
    'extracted_at', 'parser_revision', 'schema_revision', 'cycle_id', 'scope_id',
    'run_id', 'universe_version', '_batch_id',
)


def _time(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('DQ timestamps must be timezone-aware')
    return value.astimezone(timezone.utc)


def load_universe(state_dir: Path) -> dict[str, Any]:
    root = Path(state_dir)
    index = json.loads((root / 'cursor-v1.json').read_text())
    targets = {row['scope_id']: row for row in index.get('current_targets', [])}
    scopes: dict[str, Any] = {}
    clubs: dict[str, set[str]] = {}
    errors: list[str] = []
    for scope_id in sorted(set(targets) | set(index.get('scopes', {}))):
        entry = index.get('scopes', {}).get(scope_id)
        if entry is None:
            scopes[scope_id] = {'status': 'unattempted', 'expected_club_ids': []}
            continue
        path = root / entry['scope_file']
        if not path.exists():
            scopes[scope_id] = {'status': 'missing_journal', 'expected_club_ids': []}
            errors.append(f'{scope_id}:missing_journal')
            continue
        data = json.loads(path.read_text())
        listing = data.get('listing_clubs') or {}
        snapshot = data.get('snapshot') or {}
        expected = set(snapshot.get('expected_team_ids') or ())
        if not listing or set(listing) != expected:
            scopes[scope_id] = {'status': 'listing_incomplete', 'expected_club_ids': []}
            continue
        expected_ids = tuple(sorted(expected, key=int))
        sweep = data.get('stadium_sweep') or data.get('stadium_last_sweep') or {}
        scopes[scope_id] = {
            'status': 'partial' if data.get('stadium_sweep') else 'listed',
            'expected_club_ids': list(expected_ids),
            'pending_club_ids': list(sweep.get('pending') or []),
            'deferred': dict(sweep.get('deferred') or {}),
            'completed': dict(sweep.get('completed') or {}),
            'sweep_started_at': sweep.get('started_at'),
            'sweep_completed_at': sweep.get('completed_at'),
        }
        for club_id in expected_ids:
            clubs.setdefault(club_id, set()).add(scope_id)
    enumerated = [scope for scope in scopes.values() if scope['expected_club_ids']]
    return {
        'scope_count': len(scopes),
        'enumerated_scope_count': len(enumerated),
        'enumeration_complete': bool(scopes) and len(enumerated) == len(scopes),
        'expected_club_count': len(clubs) if len(enumerated) == len(scopes) else None,
        'clubs': {key: sorted(value) for key, value in sorted(clubs.items())},
        'scopes': scopes,
        'errors': sorted(errors),
    }


def _count(query, sql: str) -> int:
    frame = query(sql)
    if len(frame) != 1:
        raise ValueError('DQ count query must return one row')
    return int(frame.iloc[0, 0])


def physical_dq(query) -> dict[str, Any]:
    errors: list[str] = []
    duplicates: dict[str, int] = {}
    null_lineage: dict[str, int] = {}
    for table in BUSINESS_TABLES:
        keys = ', '.join(KEYS[table])
        duplicates[table] = _count(
            query,
            f'SELECT COUNT(*) FROM (SELECT {keys} FROM {table} GROUP BY {keys} '
            'HAVING COUNT(*) > 1) d',
        )
        required = (*KEYS[table], *LINEAGE)
        predicate = ' OR '.join(f'{column} IS NULL' for column in required)
        null_lineage[table] = _count(query, f'SELECT COUNT(*) FROM {table} WHERE {predicate}')
        if duplicates[table]:
            errors.append(f'duplicates:{table}')
        if null_lineage[table]:
            errors.append(f'null_lineage:{table}')
    orphans = _count(query, f'''
        SELECT COUNT(*) FROM {RELATION_TABLE} r
        LEFT JOIN {STADIUM_TABLE} s
          ON s.stadium_observation_key = r.stadium_observation_key
         AND s.club_id = r.club_id AND s.observed_at = r.observed_at
        WHERE s.club_id IS NULL''')
    receipt_mismatches = _count(query, f'''
        SELECT COUNT(*) FROM {RECEIPTS_TABLE} r
        WHERE r.expected_profile_count <> (SELECT COUNT(*) FROM {CLUB_PROFILE_TABLE} p WHERE p._batch_id = r.batch_id)
           OR r.expected_stadium_count <> (SELECT COUNT(*) FROM {STADIUM_TABLE} s WHERE s._batch_id = r.batch_id)
           OR r.expected_relation_count <> (SELECT COUNT(*) FROM {RELATION_TABLE} c WHERE c._batch_id = r.batch_id)''')
    if orphans:
        errors.append('orphan_relations')
    if receipt_mismatches:
        errors.append('receipt_physical_mismatch')
    receipts = query(f'''
        SELECT club_id, receipt_id, stadium_status, stadium_observed_at, batch_id,
               scope_id, run_id, universe_version
        FROM {RECEIPTS_TABLE}''')
    return {
        'duplicates': duplicates,
        'null_lineage': null_lineage,
        'orphan_relations': orphans,
        'receipt_mismatches': receipt_mismatches,
        'receipts': receipts,
        'errors': sorted(errors),
    }


def evaluate_dq(query, universe: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    current = _time(now)
    physical = physical_dq(query)
    latest: dict[str, Mapping[str, Any]] = {}
    for row in physical.pop('receipts').to_dict('records'):
        observed = _time(row['stadium_observed_at'])
        previous = latest.get(row['club_id'])
        if previous is None or observed > _time(previous['stadium_observed_at']):
            latest[row['club_id']] = {**row, 'stadium_observed_at': observed}
    fresh = {
        club_id: row for club_id, row in latest.items()
        if timedelta(0) <= current - row['stadium_observed_at'] <= timedelta(days=30)
        and row['stadium_status'] in {'observed_present', 'observed_empty'}
    }
    expected_clubs = set(universe['clubs'])
    conclusive = expected_clubs & set(fresh)
    scope_reports: dict[str, Any] = {}
    unresolved_shares: list[float] = []
    for scope_id, scope in universe['scopes'].items():
        expected = set(scope['expected_club_ids'])
        if not expected:
            unresolved_shares.append(1.0)
            scope_reports[scope_id] = {
                'status': scope['status'], 'expected': 0, 'conclusive': 0,
                'coverage': None, 'unresolved_share': 1.0,
            }
            continue
        good = expected & conclusive
        unresolved = len(expected - conclusive) / len(expected)
        unresolved_shares.append(unresolved)
        scope_reports[scope_id] = {
            'status': scope['status'], 'expected': len(expected), 'conclusive': len(good),
            'coverage': len(good) / len(expected), 'unresolved_share': unresolved,
            'pending_club_ids': scope['pending_club_ids'], 'deferred': scope['deferred'],
        }
    expected_count = len(expected_clubs)
    coverage = len(conclusive) / expected_count if expected_count else None
    errors = list(physical['errors']) + list(universe['errors'])
    if not universe['enumeration_complete']:
        errors.append('enumeration_incomplete')
    if coverage is None or coverage < .95:
        errors.append('club_conclusive_coverage_below_95')
    if any(share > .05 for share in unresolved_shares):
        errors.append('scope_unresolved_above_5')
    present_ids = {
        club_id for club_id, row in fresh.items() if row['stadium_status'] == 'observed_present'
    }
    return {
        'passed': not errors,
        'checked_at': current.isoformat(),
        'expected_scopes': universe['scope_count'],
        'enumerated_scopes': universe['enumerated_scope_count'],
        'enumeration_complete': universe['enumeration_complete'],
        'expected_clubs': universe['expected_club_count'],
        'fresh_conclusive_clubs': len(conclusive),
        'club_conclusive_coverage': coverage if universe['enumeration_complete'] else None,
        'fresh_present_clubs': len(present_ids),
        'fresh_empty_clubs': len(fresh) - len(present_ids),
        'unresolved_clubs': expected_count - len(conclusive),
        'max_scope_unresolved_share': max(unresolved_shares, default=1.0),
        'scopes': scope_reports,
        'physical': physical,
        'errors': sorted(set(errors)),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--now')
    args = parser.parse_args(argv)
    now = _time(args.now) if args.now else datetime.now(timezone.utc)
    from dags.utils import transfermarkt_native_v2 as native

    connection = native.connect()
    try:
        def query(sql):
            return pd.read_sql_query(sql, connection)
        report = evaluate_dq(query, load_universe(args.state_dir), now=now)
    finally:
        connection.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2, default=str) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
