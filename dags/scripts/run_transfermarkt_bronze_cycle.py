"""Publish the planned scopes of one Airflow cycle from its original raw responses."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace

from dags.scripts import run_transfermarkt_core as cli
from dags.utils.transfermarkt_approval import load_standing_policy
from scrapers.transfermarkt import core, native_writer
from scrapers.transfermarkt.capture_replay import capture_replay, read_input_json, verify_attempt_journals
from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan, planned_urls
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.write_journal import _write_once


@dataclass(frozen=True)
class CapturedScope:
    plan: CoreScopePlan
    envelopes: tuple[str, ...]


def validate_inventory(inventory, cycle_id):
    if not isinstance(inventory, dict) or inventory.keys() != {'cycle_id', 'registry_snapshot_id', 'scopes'}:
        raise ValueError('Invalid Transfermarkt plan fields')
    if inventory['cycle_id'] != cycle_id:
        raise ValueError('Capture index does not match this cycle and its original plan')
    universe = inventory['registry_snapshot_id']
    if not isinstance(universe, str) or not universe.strip() or universe != universe.strip():
        raise ValueError('Missing published registry snapshot')
    if not isinstance(inventory['scopes'], list) or not inventory['scopes']:
        raise ValueError('Empty Transfermarkt scope inventory')
    result = {}
    for scope in inventory['scopes']:
        if not isinstance(scope, dict):
            raise ValueError('Invalid Transfermarkt scope')
        scope_id = scope.get('scope_id')
        if not isinstance(scope_id, str) or not scope_id.strip() or scope_id != scope_id.strip() or scope_id in result:
            raise ValueError('Invalid or repeated planned scope')
        if set(scope) - {'scope_id', 'league', 'season', 'edition_id', 'clubs', 'players', 'coaches', 'sections'}:
            raise ValueError('Unknown Transfermarkt scope fields')
        for field in ('clubs', 'players', 'coaches'):
            if field in scope and (not isinstance(scope[field], list)
                                   or not all(isinstance(x, str) and x for x in scope[field])):
                raise ValueError(f'Invalid {field} targets')
        if not all(isinstance(scope.get(field), str) and scope[field] for field in ('league', 'season', 'edition_id')):
            raise ValueError('An explicit competition, season and edition are required')
        plan = cli.build_scope_plan(
            league=scope['league'], season=scope['season'], edition_id=scope['edition_id'],
            scope_id=scope_id, cycle_id=cycle_id, clubs=scope.get('clubs', []),
            players=scope.get('players', []), coaches=scope.get('coaches', []),
            options=cli.section_options(scope['sections']) if 'sections' in scope else None,
        )
        if not planned_urls(plan):
            raise ValueError(f'No targets for {scope_id}')
        result[scope_id] = plan
    return universe, list(result.values())


def _load_cycle(directory: Path, cycle_id: str, store):
    # The expected inventory is published independently of the responses.
    plan_bytes = (directory / 'plan.json').read_bytes()
    universe, plans = validate_inventory(read_input_json(plan_bytes), cycle_id)
    captures = read_input_json((directory / 'captures.json').read_bytes())
    if not isinstance(captures, dict) or captures.keys() != {
        'cycle_id', 'plan_sha256', 'envelopes_by_scope', 'attempt_journals', 'traffic_sha256'
    }:
        raise ValueError('Invalid Transfermarkt capture index fields')
    if (captures['cycle_id'] != cycle_id
            or captures['plan_sha256'] != hashlib.sha256(plan_bytes).hexdigest()):
        raise ValueError('Capture index does not match this cycle and its original plan')
    by_scope = captures['envelopes_by_scope']
    if not isinstance(by_scope, dict):
        raise ValueError('Invalid captured scope index')
    result = []
    for plan in plans:
        scope_id = plan.scope_id
        envelopes = by_scope.get(scope_id)
        if not isinstance(envelopes, list) or not envelopes:
            raise ValueError(f'Missing source responses for {scope_id}')
        result.append(CapturedScope(plan, tuple(envelopes)))
    if len(result) != len(by_scope):
        raise ValueError('Captured and planned scopes differ')
    digests = captures['attempt_journals']
    if (not isinstance(digests, list) or not digests or not all(
            isinstance(d, str) and re.fullmatch('[0-9a-f]{64}', d) for d in digests)
            or len(digests) != len(set(digests))):
        raise ValueError('Missing or invalid original attempt journal identities')
    traffic = (directory / 'traffic.json').read_bytes()
    if hashlib.sha256(traffic).hexdigest() != captures['traffic_sha256']:
        raise ValueError('Bounded traffic receipt digest differs')
    journals = []
    for digest in digests:
        data = (directory / 'evidence' / (digest + '.json')).read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError('Original attempt journal digest differs')
        journals.append(read_input_json(data))
    observed = verify_attempt_journals(store, cycle_id, by_scope, journals, read_input_json(traffic))
    if observed != by_scope:
        raise ValueError('Capture index differs from the complete bounded attempt journal')
    return universe, result


def prepare_scheduled_plan(root: Path, *, cycle_id: str, started_at: datetime) -> Path:
    """Materialize the entire admitted inventory before collection, pinned outside the run."""
    directory = cycle_directory(root, cycle_id)
    digest = os.environ.get('TM_CORE_INVENTORY_SHA256', '')
    if not re.fullmatch(r'[0-9a-f]{64}', digest):
        raise ValueError('Missing approved Transfermarkt Bronze inventory digest')
    data = (root / 'registry' / (digest + '.json')).read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError('Approved Transfermarkt Bronze inventory digest mismatch')
    snapshot = read_input_json(data)
    if not isinstance(snapshot, dict) or snapshot.keys() != {'valid_from', 'valid_until', 'scopes'}:
        raise ValueError('Invalid Transfermarkt Bronze inventory')
    first, last = (datetime.fromisoformat(snapshot[field]) for field in ('valid_from', 'valid_until'))
    now = datetime.now(timezone.utc)
    if (not isinstance(started_at, datetime) or any(value.utcoffset() is None for value in (started_at, first, last))
            or not first <= started_at <= now < last):
        raise ValueError('Inventory is not current for this Airflow run')
    plan = {'cycle_id': cycle_id, 'registry_snapshot_id': digest, 'scopes': snapshot['scopes']}
    validate_inventory(plan, cycle_id)
    target = directory / 'plan.json'
    canonical = json.dumps(plan, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    _write_once(target, canonical, 'Cycle plan differs from the approved Transfermarkt Bronze inventory')
    return target


def _parse(scope, store, *, universe, started_at, write_options):
    plan = scope.plan
    replay = capture_replay(store, scope.envelopes, scope_id=plan.scope_id, cycle_id=plan.cycle_id)
    now = datetime.now(timezone.utc)
    source_times = [datetime.fromisoformat(value) for value in replay.fetched_at]
    if not all(started_at <= value <= now for value in source_times):
        raise ValueError(f'Source response outside the current Airflow run: {plan.scope_id}')
    # The same closed batch must reproduce its original row and receipt identities.
    batch_time = max(source_times).astimezone(timezone.utc).isoformat(timespec='microseconds')
    run = CoreRun(replay.fetch, request_raw_loader=replay.load, universe_version=universe,
                  extracted_at=batch_time)
    report = run.run(plan, **write_options)
    replay.assert_consumed()
    failure = cli.completion_error(report, write_options.get('connection_factory'))
    if failure:
        raise ValueError(f'{plan.scope_id}: {failure}')
    return report


def _check_grant(policy, tables):
    now = datetime.now(timezone.utc)
    if policy.dag_id != 'dag_ingest_transfermarkt' or policy.approved_at > now:
        raise ValueError('Standing policy does not grant this DAG permission to write')
    policy.assert_not_expired(now)
    missing = tables.difference(policy.allowed_write_tables)
    if missing:
        raise ValueError('Standing policy does not cover: ' + ', '.join(sorted(missing)))


def cycle_directory(root: Path, cycle_id: str) -> Path:
    if not isinstance(cycle_id, str) or not cycle_id or cycle_id != cycle_id.strip():
        raise ValueError('Missing Airflow run identity')
    return root / 'cycles' / hashlib.sha256(cycle_id.encode()).hexdigest()


def require_scheduled_writes():
    if os.getenv('TM_STANDING_POLICY_ENABLED', '').strip().lower() != 'true':
        raise ValueError('Scheduled Transfermarkt Bronze writes are disabled')


def publish_cycle(root, policy_file, *, cycle_id, started_at):
    require_scheduled_writes()
    policy = load_standing_policy(policy_file)
    tables = {native_writer.CORE_RECEIPTS_TABLE, native_writer.CORE_PAGES_TABLE}
    _check_grant(policy, tables)
    if not cycle_id or started_at is None or started_at.utcoffset() is None:
        raise ValueError('Missing Airflow run identity or start time')
    plan_file = prepare_scheduled_plan(root, cycle_id=cycle_id, started_at=started_at)
    store = RawResponseStore.from_env()
    if store.uri_prefix != 's3://warehouse/raw/transfermarkt':
        raise ValueError('Scheduled ingestion requires the Transfermarkt source store')
    universe, scopes = _load_cycle(plan_file.parent, cycle_id, store)

    # Validate every original, result and table grant before the first write.
    cycle_plans = []
    for scope in scopes:
        report = _parse(scope, store, universe=universe, started_at=started_at,
                        write_options={'collect_plans': cycle_plans})
        tables.update('iceberg.bronze.' + core.CORE_TABLES[item['entity']] for item in report.outcomes)
    native_writer.validate_run_plans(cycle_plans)
    del cycle_plans  # Release the parsed rows before replaying each scope for publication.
    _check_grant(policy, tables)
    write_options = cli._write_options(SimpleNamespace(trino_host=None, trino_port=None, trino_user=None))
    rows = receipts = 0
    for scope in scopes:
        _check_grant(policy, tables)
        report = _parse(scope, store, universe=universe, started_at=started_at, write_options=write_options)
        rows += sum(item['rows'] for item in report.outcomes)
        receipts += sum(len(receipt['plans']) for receipt in report.receipts)
    return {'cycle_id': cycle_id, 'registry_snapshot_id': universe, 'scopes': len(scopes),
            'rows': rows, 'receipts': receipts, 'tables': sorted(tables)}
