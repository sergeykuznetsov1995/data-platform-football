"""Resumable weekly/monthly referee ingest and separate historical directory backfill."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Mapping

from scrapers.transfermarkt.referee_capture import (
    RefereeCaptureError,
    capture_referee_directory,
    capture_referee_profiles,
    referee_scopes,
)

WEEKLY = timedelta(days=7)
MONTHLY = timedelta(days=30)
PREFLIGHT = {'write_mode': 'native-only', 'revision': 0, 'paid_io_allowed': True}


def run_referee_sweep(
    registry_rows: list[Mapping[str, Any]], *, mode: str, run_id: str,
    state_dir: Any, selected_scope_ids=(), max_scopes: int = 100,
    scraper_factory=None, writer=None, now_fn=None, monotonic_fn=None,
    max_seconds: int = 3600, preflight=None,
) -> dict[str, Any]:
    if mode not in {'current', 'history'}:
        raise RefereeCaptureError('mode_invalid')
    if isinstance(max_scopes, bool) or not isinstance(max_scopes, int) or max_scopes < 1:
        raise RefereeCaptureError('max_scopes_invalid')
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    monotonic_fn = monotonic_fn or time.monotonic
    preflight = preflight or PREFLIGHT
    now = _utc(now_fn())
    started_monotonic = monotonic_fn()
    historical = mode == 'history'
    available = referee_scopes(registry_rows, historical=historical)
    selected = tuple(str(value) for value in selected_scope_ids)
    known = {row['scope_id'] for row in available}
    if selected and not set(selected) <= known:
        raise RefereeCaptureError('selected_scope_unknown')
    scopes = [row for row in available if not selected or row['scope_id'] in selected]
    if not scopes:
        raise RefereeCaptureError('referee_scope_denominator_empty')
    snapshots = {row['registry_snapshot_id'] for row in available}
    if len(snapshots) != 1 or not next(iter(snapshots)):
        raise RefereeCaptureError('registry_snapshot_invalid')
    universe_version = next(iter(snapshots))
    selection_id = hashlib.sha256(json.dumps(sorted(scopes_ids(scopes))).encode()).hexdigest()
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True)
    state_path = directory / f'{mode}-state.json'
    state = _load_state(state_path, mode, universe_version, selection_id)
    scraper_factory = scraper_factory or production_referee_scraper
    if writer is None:
        from dags.utils.transfermarkt_referees import write_referee_bundle
        writer = write_referee_bundle

    worked = 0
    processed = 0
    continuation = False
    for scope in scopes:
        scope_id = scope['scope_id']
        saved = state['scopes'].setdefault(scope_id, _empty_scope(scope))
        directory_due = _due(saved.get('directory_completed_at'), now, WEEKLY if mode == 'current' else None)
        profile_due = (
            mode == 'current' and bool(saved.get('referee_ids'))
            and _due(saved.get('profiles_completed_at'), now, MONTHLY)
        )
        if not directory_due and not profile_due:
            continue
        if processed >= max_scopes or monotonic_fn() - started_monotonic >= max_seconds:
            continuation = True
            break
        processed += 1
        runtime = scraper_factory(scope)
        if directory_due:
            try:
                bundle = capture_referee_directory(
                    runtime._http_client, scope, run_id=run_id,
                    universe_version=universe_version, extracted_at=now,
                )
                proof = writer(runtime, bundle, preflight)
                _verify_proof(bundle, proof)
            except Exception as exc:
                saved['deferred'] = _error(exc)
                _save_state(state_path, state)
                continue
            saved['directory_completed_at'] = now.isoformat()
            saved['referee_ids'] = sorted(
                {row.referee_id for row in bundle.directory_referees}, key=int,
            )
            saved.pop('deferred', None)
            worked += 1
            _save_state(state_path, state)
        profile_due = (
            mode == 'current' and bool(saved.get('referee_ids'))
            and _due(saved.get('profiles_completed_at'), now, MONTHLY)
        )
        if not profile_due:
            continue
        try:
            bundle = capture_referee_profiles(
                runtime._http_client, saved['referee_ids'], scope=scope,
                run_id=run_id, universe_version=universe_version, extracted_at=now,
            )
            proof = writer(runtime, bundle, preflight)
            _verify_proof(bundle, proof)
        except Exception as exc:
            saved['deferred'] = _error(exc)
            _save_state(state_path, state)
            continue
        saved['profiles_completed_at'] = now.isoformat()
        saved.pop('deferred', None)
        worked += 1
        _save_state(state_path, state)

    report = _coverage_report(
        state, scopes, len(available), now, mode, bool(selected), continuation, run_id,
    )
    report['work_items'] = worked
    if report['unresolved_scopes']:
        report['status'] = 'error'
    elif continuation:
        report['status'] = 'continuation'
    elif not worked:
        report['status'] = 'no_op'
    else:
        report['status'] = 'complete'
    _atomic_json(directory / f'{mode}-last-report.json', report)
    return report


def production_referee_scraper(scope: Mapping[str, Any]):
    from dags.scripts.run_transfermarkt_scope_cycle import validate_standing_policy_for_scope_cycle
    from dags.utils.transfermarkt_approval import load_standing_policy
    from dags.utils.transfermarkt_referees import validate_referee_write_policy
    from dags.utils.transfermarkt_scope_planner import _competition_from_joined_row
    from scrapers.transfermarkt.models import (
        SCOPE_HARD_PROVIDER_BYTE_CAP, SCOPE_REQUEST_LIMIT, SCOPE_RETRY_LIMIT,
        SCOPE_SOFT_PROVIDER_BYTE_STOP, SharedTrafficLedger,
    )
    from scrapers.transfermarkt.scraper import TransfermarktScraper

    if os.environ.get('TM_STANDING_POLICY_ENABLED', '').lower() not in {'true', '1'}:
        raise RefereeCaptureError('standing_policy_gate_required')
    if not os.environ.get('TM_PROXY_CONTROL_URL'):
        raise RefereeCaptureError('metered_proxy_required')
    if os.environ.get('TRANSFERMARKT_REQUIRE_RAW_STORE', '').lower() not in {'true', '1'}:
        raise RefereeCaptureError('raw_store_required')
    policy = load_standing_policy(os.environ.get('TM_STANDING_POLICY_PATH'))
    validate_referee_write_policy(policy)
    validate_standing_policy_for_scope_cycle(
        policy, write_mode=os.environ.get('TM_WRITE_MODE', 'native-only'),
        cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
        request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT,
    )
    return TransfermarktScraper(
        leagues=[scope['competition_id']], seasons=[int(scope['edition_id'])],
        canonical_season=scope['canonical_season'],
        competition_records=(_competition_from_joined_row(scope['_registry_row']),),
        traffic_ledger=SharedTrafficLedger(
            hard_provider_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
            soft_provider_bytes=SCOPE_SOFT_PROVIDER_BYTE_STOP,
            retry_limit=SCOPE_RETRY_LIMIT,
        ),
        lease_metadata={
            'dag_id': os.environ.get('TM_DAG_ID', 'dag_ingest_transfermarkt_referees'),
            'run_id': os.environ.get('TM_RUN_ID', ''),
            'task_id': 'referee_sweep', 'scope': scope['scope_id'],
        },
    )


def read_promoted_referee_rows() -> list[dict[str, Any]]:
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_scope_planner import build_promoted_registry_query

    connection = native.connect()
    cursor = connection.cursor()
    try:
        cursor.execute(build_promoted_registry_query())
        rows = list(cursor.fetchall())
        columns = [item[0] for item in cursor.description or ()]
    finally:
        cursor.close()
        connection.close()
    if not rows or not columns:
        raise RefereeCaptureError('promoted_registry_empty')
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _load_state(path, mode, universe_version, selection_id):
    if not path.exists():
        return {
            'mode': mode, 'universe_version': universe_version,
            'selection_id': selection_id, 'scopes': {},
        }
    value = json.loads(path.read_text())
    if value.get('mode') != mode:
        raise RefereeCaptureError('state_mode_mismatch')
    if value.get('universe_version') != universe_version or value.get('selection_id') != selection_id:
        return {
            'mode': mode, 'universe_version': universe_version,
            'selection_id': selection_id, 'scopes': {},
        }
    if not isinstance(value.get('scopes'), dict):
        raise RefereeCaptureError('state_invalid')
    return value


def _empty_scope(scope):
    return {
        'scope': {key: value for key, value in scope.items() if not key.startswith('_')},
        'directory_completed_at': None,
        'profiles_completed_at': None,
        'referee_ids': [],
    }


def _due(completed_at, now, interval):
    if completed_at is None:
        return True
    if interval is None:
        return False
    return now - _utc(completed_at) >= interval


def _coverage_report(state, scopes, universe_count, now, mode, explicit, continuation, run_id):
    expected = len(scopes)
    deferred = sum('deferred' in row for row in state['scopes'].values())
    if mode == 'history':
        completed = sum(
            row.get('directory_completed_at') is not None
            for row in state['scopes'].values()
        )
        aggregate_fresh = completed
        profile_fresh = 0
    else:
        aggregate_fresh = sum(
            not _due(row.get('directory_completed_at'), now, WEEKLY)
            for row in state['scopes'].values()
        )
        profile_fresh = sum(
            not _due(row.get('profiles_completed_at'), now, MONTHLY)
            for row in state['scopes'].values()
        )
    complete = (
        aggregate_fresh == expected and deferred == 0 and not continuation
        and (mode == 'history' or profile_fresh == expected)
    )
    report = {
        'mode': mode, 'run_id': run_id,
        'universe_version': state['universe_version'],
        'universe_scopes': universe_count,
        'expected_scopes': expected,
        'fresh_aggregate_scopes': aggregate_fresh,
        'fresh_profile_scopes': profile_fresh,
        'aggregate_coverage': aggregate_fresh / expected,
        'profile_coverage': profile_fresh / expected if mode == 'current' else None,
        'unresolved_scopes': deferred,
        'unresolved_ratio': deferred / expected,
        'continuation_required': continuation or deferred > 0,
        'coverage_complete': complete and not explicit,
        'explicit_selection': explicit,
        'match_row_count': 0,
    }
    if mode == 'history':
        report['completed_history_scopes'] = completed
        report['history_coverage'] = completed / expected
    return report


def _verify_proof(bundle, proof):
    expected_rows = len(bundle.profiles) + len(bundle.aggregates)
    if (
        not isinstance(proof, Mapping) or proof.get('verified') is not True
        or proof.get('receipt_readback') is not True
        or proof.get('row_count') != expected_rows
        or tuple(proof.get('raw_capture_ids') or ()) != bundle.raw_capture_ids
    ):
        raise RefereeCaptureError('writer_receipt_mismatch')


def _save_state(path, state):
    _atomic_json(path, state)


def _atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(
        value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False,
    ))
    temporary.replace(path)


def _utc(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except ValueError as exc:
        raise RefereeCaptureError('timestamp_invalid') from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RefereeCaptureError('timestamp_invalid')
    return parsed.astimezone(timezone.utc)


def _error(exc):
    return f'{type(exc).__name__}: {str(exc)[:300]}'


def scopes_ids(scopes):
    return [row['scope_id'] for row in scopes]


def read_referee_preflight():
    from dags.utils import transfermarkt_native_v2 as native

    connection = native.connect()
    cursor = connection.cursor()
    try:
        state = native.read_reader_state(cursor, allow_missing=False)
    finally:
        cursor.close()
        connection.close()
    return {
        'write_mode': 'native-only' if state.legacy_writers_disabled_at is not None else 'dual',
        'revision': state.revision,
        'paid_io_allowed': True,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('current', 'history'), required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--scope', action='append', default=[])
    parser.add_argument('--scopes-json', default='[]')
    parser.add_argument('--max-scopes', type=int, default=100)
    parser.add_argument('--max-seconds', type=int, default=3600)
    args = parser.parse_args(argv)
    selected = json.loads(args.scopes_json)
    if not isinstance(selected, list) or not all(isinstance(value, str) for value in selected):
        parser.error('--scopes-json must be an array of scope IDs')
    report = run_referee_sweep(
        read_promoted_referee_rows(), mode=args.mode, run_id=args.run_id,
        state_dir=args.state_dir, selected_scope_ids=[*args.scope, *selected],
        max_scopes=args.max_scopes, max_seconds=args.max_seconds,
        preflight=read_referee_preflight(),
    )
    print(json.dumps(report, sort_keys=True, ensure_ascii=False))
    return 0 if report['status'] in {'complete', 'no_op', 'continuation'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
