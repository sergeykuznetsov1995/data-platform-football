"""Read-only Transfermarkt paid-traffic report and history step-1 acceptance (#1404).

Paid bytes come only from the gateway ledger (``paid_requests.jsonl`` + rotated
archives). Ops tables and signal-lane receipts explain requests, retries, cache
hits, decoded/wire bytes and duration of the day's runs; their coverage of those
runs' ledger bytes is the reconciliation row (per run, not per day).
No Trino or Airflow imports here: SQL builders return ``(sql, params)``.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import re
from typing import Any, Iterable, Iterator, Mapping
import zlib

from dags.utils.transfermarkt_backfill_state import (CAMPAIGN_TABLE, SCOPE_TABLE,
                                                     UNAVAILABLE_CONFIRMATIONS_REQUIRED)
from dags.utils.transfermarkt_daily_measure import MeasurementError, utc

VERSION = 'traffic-report-v1'
TRAFFIC_RECONCILIATION_TOLERANCE_PCT = 2.0
# ok needs at least this share of the covered lanes' day bytes in settled runs.
TRAFFIC_RECONCILIATION_MIN_COVERAGE_PCT = 98.0
# A run is unsettled while its last bytes are younger than the gateway lease TTL.
LEASE_TTL = timedelta(seconds=3600)
LANES = {'dag_discover_transfermarkt_registry': 'registry', 'dag_ingest_transfermarkt': 'current',
         'dag_backfill_transfermarkt': 'history'}
# The registry lane has no ops writer by design: outside the ±2 % reconciliation.
NOT_COVERED_LANES = ('registry',)
LANE_NAMES = {'registry': 'реестр', 'current': 'актуалка', 'history': 'история', 'other': 'прочее'}
OPS_EVENTS_TABLE = 'iceberg.ops.transfermarkt_proxy_ledger_events_v1'
OPS_RUNS_TABLE = 'iceberg.ops.proxy_traffic_runs'
LEDGER_FILE = 'paid_requests.jsonl'
_ARCHIVE = re.compile(r'^paid_requests\.(\d{8}T\d{6}Z)\.jsonl\.gz$')
# A run+entity is explained by the first source present; sources never sum.
_OPS_PRIORITY = ('events_v1', 'proxy_traffic_runs', 'current_receipts')
_COUNTERS = ('requests', 'retries', 'cache_hits', 'decoded_bytes', 'provider_metered_bytes')
# Measured by events_v1 and receipts only; None when no chosen source reports them.
_OPTIONAL = ('wire_bytes', 'duration_ms')


class TrafficReportError(ValueError):
    """The gateway ledger cannot be read; there is no report without it."""


# Label/name -> output entity group, so the ledger, receipts and ops tables meet.
# Sources: _ENTITY_LABELS (run_transfermarkt_current.py, client labels = ledger and
# receipt entity, client.py:1014), ENTITY_OUTPUTS (run_transfermarkt_scope_cycle.py,
# events_v1 entity), proxy_traffic_runs entity (run_transfermarkt_scraper.py: the
# group names), scraper labels listing/club_staff (scraper.py). Drift is guarded by
# tests; run scripts are not imported here.
ENTITY_GROUPS = {
    'players': {'players', 'listing', 'participants_api', 'teilnehmer', 'squad', 'current_players',
                'current_injury', 'squad_memberships', 'player_attribute_observations',
                'player_contract_observations'},
    'market_value_history': {'market_value_history', 'market_value_points', 'mv_history'},
    'transfers': {'transfers', 'transfer_events'},
    'coaches': {'coaches', 'coach_history', 'coach_profile', 'club_staff', 'coach_profiles', 'coach_stints'},
}
_ENTITY_GROUP = {name: group for group, names in ENTITY_GROUPS.items() for name in names}


def entity_group(name: Any) -> str:
    """Canonical group of an entity name; an unknown name is its own group."""
    name = str(name or '')
    return _ENTITY_GROUP.get(name, name)


def lane(dag_id: Any) -> str:
    return LANES.get(str(dag_id or ''), 'other')


def _int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def iter_ledger_events(ledger_dir: Path | str, day_start: datetime,
                       stats: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    """Yield every well-formed event of the ledger files that may hold the day.

    An archive stamped before ``day_start`` holds only earlier events and is
    skipped; later archives and the current file are read whole, so runs of the
    day are seen beyond the day too. An unreadable archive is listed in
    ``stats``; the current file is mandatory. Malformed lines are counted.
    """
    stats = {} if stats is None else stats
    stats.update(malformed_lines=0, files_read=[], archives_unreadable=[], older_archive=False, earliest_at=None)
    directory = Path(ledger_dir)
    if not directory.is_dir():
        raise TrafficReportError('ledger directory is missing')
    archives = []
    for path in sorted(directory.iterdir()):
        match = _ARCHIVE.match(path.name)
        if not match:
            continue
        if datetime.strptime(match.group(1), '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc) >= day_start:
            archives.append(path)
        else:
            stats['older_archive'] = True
    current = directory / LEDGER_FILE
    if not current.is_file() and not archives:
        raise TrafficReportError('ledger file is missing')
    for path in archives + ([current] if current.is_file() else []):
        try:
            handle = (gzip.open(path, 'rt', encoding='utf-8', errors='replace') if path is not current
                      else path.open(encoding='utf-8', errors='replace'))
            with handle:
                stats['files_read'].append(path.name)
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                        event['_at'] = utc(event['occurred_at'])
                    except (ValueError, TypeError, KeyError, MeasurementError):
                        stats['malformed_lines'] += 1
                        continue
                    if stats['earliest_at'] is None or event['_at'] < stats['earliest_at']:
                        stats['earliest_at'] = event['_at']
                    yield event
        except (OSError, EOFError, zlib.error) as exc:
            if path is current:
                raise TrafficReportError('ledger file is unreadable') from exc
            stats['archives_unreadable'].append(path.name)


def summarize_ledger(events: Iterable[Mapping[str, Any]], day_start: datetime,
                     day_end: datetime, as_of: datetime) -> dict[str, Any]:
    """Day totals plus every run of the day over all opened files.

    Paid bytes come from ``bytes`` events only: leases cross midnight. Day
    totals (``by_lane``, ``by_stream``, ``by_lane_entity``) are strictly the
    day. ``runs`` holds each run with bytes in the day; its ``bytes_window`` is
    every ``bytes`` event of the opened files, so a run started before the first
    opened file is counted only partly. ``in_progress`` marks a run whose last bytes are younger than
    ``LEASE_TTL`` at ``as_of``: bytes after ``day_end`` alone do not, since a
    past day's files are read whole. A gateway counter (``dagrun_total_bytes``) is not used: it lives
    in gateway memory and resets on restart.
    """
    provider = {'total': 0, 'up': 0, 'down': 0}
    by_lane: dict[str, int] = {}
    by_stream: dict[str, int] = {}
    by_lane_entity: dict[str, dict[str, int]] = {}
    tunnels_by_lane: dict[str, int] = {}
    rejected: dict[str, int] = {}
    opened = closed = events_in_day = 0
    window: dict[str, dict[str, Any]] = {}
    for event in events:
        kind, name, at = event.get('event_type'), lane(event.get('dag_id')), event['_at']
        in_day = day_start <= at < day_end
        events_in_day += in_day
        run_id = str(event.get('run_id') or '')
        if kind == 'bytes':
            size, entity = _int(event.get('bytes')), entity_group(event.get('entity'))
            run = window.setdefault(run_id, {'lane': name, 'bytes_in_day': 0, 'bytes_window': 0,
                                             'first_at': at, 'last_at': at, 'started_before_day': False,
                                             'in_progress': False, 'tunnels_window': 0, 'groups': {}})
            run['bytes_window'] += size
            run['groups'][entity] = run['groups'].get(entity, 0) + size
            run['first_at'], run['last_at'] = min(run['first_at'], at), max(run['last_at'], at)
            run['started_before_day'] |= at < day_start
            if not in_day:
                continue
            run['bytes_in_day'] += size
            provider['total'] += size
            if event.get('direction') in ('up', 'down'):
                provider[event['direction']] += size
            by_lane[name] = by_lane.get(name, 0) + size
            stream = str(event.get('stream_id') or '')
            by_stream[stream] = by_stream.get(stream, 0) + size
            entities = by_lane_entity.setdefault(name, {})
            entities[entity] = entities.get(entity, 0) + size
        elif kind == 'lease_closed':
            if run_id in window:
                window[run_id]['tunnels_window'] += _int(event.get('tunnels_total'))
            if in_day:
                closed += 1
                tunnels_by_lane[name] = tunnels_by_lane.get(name, 0) + _int(event.get('tunnels_total'))
        elif kind == 'lease_created' and in_day:
            opened += 1
        elif kind == 'connect_rejected' and in_day:
            reason = str(event.get('reason') or 'unknown')
            rejected[reason] = rejected.get(reason, 0) + 1
    runs = {run_id: {**run, 'in_progress': run['last_at'] > as_of - LEASE_TTL,
                     'first_at': run['first_at'].isoformat(), 'last_at': run['last_at'].isoformat()}
            for run_id, run in sorted(window.items()) if run['bytes_in_day']}
    run_ids: dict[str, list[str]] = {}
    for run_id, run in runs.items():
        run_ids.setdefault(run['lane'], []).append(run_id)
    tunnels = sum(tunnels_by_lane.values())
    return {'provider_bytes': provider, 'by_lane': by_lane, 'by_stream': by_stream,
            'by_lane_entity': by_lane_entity, 'runs': runs, 'leases_opened': opened,
            'leases_closed': closed, 'tunnels': tunnels, 'tunnels_by_lane': tunnels_by_lane,
            'connect_rejected': rejected,
            'bytes_per_tunnel': provider['total'] / tunnels if tunnels else None,
            'run_ids': run_ids, 'events_in_day': events_in_day}


def read_ledger(ledger_dir: Path | str, day_start: datetime, day_end: datetime,
                as_of: datetime) -> dict[str, Any]:
    """Ledger summary with file stats; ``complete`` is False on an unreadable
    archive or when the files do not reach back to the day."""
    stats: dict[str, Any] = {}
    summary = summarize_ledger(iter_ledger_events(ledger_dir, day_start, stats), day_start, day_end, as_of)
    earliest = stats.pop('earliest_at')
    older = stats.pop('older_archive')
    summary.update(stats)
    summary['earliest_event_at'] = earliest.isoformat() if earliest else None
    reason = None
    if stats['archives_unreadable']:
        reason = 'archives_unreadable'
    elif not summary['events_in_day'] and (earliest is None or (not older and earliest > day_start)):
        reason = 'day_not_covered'
    summary['complete'], summary['incomplete_reason'] = reason is None, reason
    return summary


def _runs_values(run_ids: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    params = tuple(sorted({str(item) for item in run_ids if item}))
    if not params:
        raise MeasurementError('no ledger run ids')
    return ', '.join('(CAST(? AS varchar))' for _ in params), params


def build_ops_events_sql(run_ids: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    """Committed child deltas of scope-cycle current and history runs."""
    values, params = _runs_values(run_ids)
    sql = f'''WITH runs(run_id) AS (VALUES {values})
SELECT 'events_v1' AS ops_source, e.parent_cycle_id AS run_id, e.entity,
 sum(e.requests) AS requests, sum(e.retries) AS retries, sum(e.cache_hits) AS cache_hits,
 sum(e.decoded_bytes) AS decoded_bytes, sum(e.wire_bytes) AS wire_bytes,
 sum(e.provider_metered_bytes) AS provider_metered_bytes, sum(e.duration_ms) AS duration_ms
FROM {OPS_EVENTS_TABLE} e JOIN runs r ON e.parent_cycle_id = r.run_id
GROUP BY e.parent_cycle_id, e.entity'''
    return sql, params


def build_ops_runs_sql(run_ids: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    """Legacy daily-lane rows; no cache hits are recorded there."""
    values, params = _runs_values(run_ids)
    sql = f'''WITH runs(run_id) AS (VALUES {values})
SELECT 'proxy_traffic_runs' AS ops_source, p.dag_run_id AS run_id, p.entity,
 sum(p.request_count) AS requests, sum(p.retry_count) AS retries, 0 AS cache_hits,
 sum(p.decoded_response_body_bytes) AS decoded_bytes,
 sum(p.provider_metered_bytes) AS provider_metered_bytes, sum(p.failure_count) AS failure_count
FROM {OPS_RUNS_TABLE} p JOIN runs r ON p.dag_run_id = r.run_id
WHERE p.source = 'transfermarkt'
GROUP BY p.dag_run_id, p.entity'''
    return sql, params


def read_current_receipts(state_dir: Path | str, run_ids: Iterable[str]) -> dict[str, Any]:
    """Signal-lane receipts of the day's runs; the ledger run_id is their child_cycle_id.

    Receipts are matched by run, not by ``captured_at``: the run is compared
    with its ledger bytes over the same span.
    """
    scopes = Path(state_dir) / 'scopes'
    if not scopes.is_dir():
        return {'status': 'not_available', 'reason': 'signal-lane state directory is missing'}
    wanted = set(run_ids)
    seen, rows, malformed = set(), {}, 0
    for path in sorted(scopes.glob('*.json')):
        try:
            receipts = json.loads(path.read_text(encoding='utf-8')).get('traffic_receipts') or []
        except (OSError, ValueError, AttributeError):
            malformed += 1
            continue
        for receipt in receipts:
            try:
                key = (str(receipt['cycle_id']), str(receipt['child_cycle_id']))
                traffic = receipt.get('traffic') or {}
                by_entity = traffic.get('shared_traffic_ledger', traffic).get('by_entity') or {}
            except (KeyError, TypeError, AttributeError):
                malformed += 1
                continue
            if key in seen or key[1] not in wanted:
                continue
            seen.add(key)
            for name, counters in by_entity.items():
                entity = entity_group(name)
                row = rows.setdefault((key[1], entity), {
                    'ops_source': 'current_receipts', 'run_id': key[1], 'entity': entity,
                    **{item: 0 for item in _COUNTERS + _OPTIONAL}})
                for item, source in (('requests', 'requests'), ('retries', 'retries'),
                                     ('cache_hits', 'cache_hits'), ('decoded_bytes', 'decoded_bytes'),
                                     ('provider_metered_bytes', 'provider_bytes'), ('wire_bytes', 'wire_bytes')):
                    row[item] += _int(counters.get(source))
                seconds = counters.get('duration_seconds')
                row['duration_ms'] += round(seconds * 1000) if isinstance(seconds, (int, float)) else 0
    by_entity: dict[str, dict[str, int]] = {}
    for row in rows.values():
        target = by_entity.setdefault(row['entity'], {item: 0 for item in _COUNTERS + _OPTIONAL})
        for item in _COUNTERS + _OPTIONAL:
            target[item] += row[item]
    return {'status': 'ok' if rows else 'empty', 'receipts': len(seen), 'malformed': malformed,
            'by_entity': by_entity, 'rows': list(rows.values())}


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def _add_optional(target: dict[str, Any], row: Mapping[str, Any]) -> None:
    """Sum a measure only from sources that report it; never measured stays None."""
    for item in _OPTIONAL:
        value = row.get(item)
        if isinstance(value, int) and not isinstance(value, bool):
            target[item] = (target[item] or 0) + value


def merge_ops(ledger_summary: Mapping[str, Any], ops_rows: Iterable[Mapping[str, Any]],
              receipts: Mapping[str, Any], ops_sources: Mapping[str, str]) -> dict[str, Any]:
    """Lane/entity table plus run-level reconciliation.

    Ops counters belong to the day's runs as a whole, so they are compared with
    each run's ``bytes_window``; unsettled runs and the registry lane (no ops
    writer) are excluded. ``ok`` also needs settled runs to carry at least
    ``TRAFFIC_RECONCILIATION_MIN_COVERAGE_PCT`` of the covered lanes' day bytes.
    ``ops_sources`` maps each source to ok|empty|not_available.
    """
    runs = ledger_summary['runs']
    # Names of one group sum within a source; across sources the first present wins.
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in [*ops_rows, *(receipts.get('rows') or ())]:
        key = (str(row.get('run_id') or ''), entity_group(row.get('entity')))
        total = grouped.setdefault(key, {}).setdefault(row['ops_source'], {
            'ops_source': row['ops_source'], **{item: 0 for item in _COUNTERS},
            **{item: None for item in _OPTIONAL}})
        for item in _COUNTERS:
            total[item] += _int(row.get(item))
        _add_optional(total, row)
    chosen = {key: next(sources[item] for item in _OPS_PRIORITY if item in sources)
              for key, sources in grouped.items()}
    lanes: dict[str, dict[str, Any]] = {}
    entities: dict[str, dict[str, dict[str, Any]]] = {}

    def slot(table, name):
        return table.setdefault(name, {
            'ledger_provider_bytes': 0, 'runs_ledger_bytes': 0, 'ops_provider_bytes': 0,
            'reconciliation_ledger_bytes': 0, 'reconciliation_ops_bytes': 0, 'reconciled_day_bytes': 0,
            **{item: 0 for item in _COUNTERS if item != 'provider_metered_bytes'},
            **{item: None for item in _OPTIONAL}})

    def reconciled(run_id):
        run = runs.get(run_id)
        return run is not None and run['lane'] not in NOT_COVERED_LANES and not run['in_progress']

    for name, value in ledger_summary['by_lane'].items():
        slot(lanes, name)['ledger_provider_bytes'] = value
    for name, values in ledger_summary['by_lane_entity'].items():
        for entity, value in values.items():
            slot(entities.setdefault(name, {}), entity)['ledger_provider_bytes'] = value
    for run_id, run in runs.items():
        lane_slot = slot(lanes, run['lane'])
        lane_slot['runs_tunnels'] = lane_slot.get('runs_tunnels', 0) + run['tunnels_window']
        if reconciled(run_id):
            lane_slot['reconciled_day_bytes'] += run['bytes_in_day']
        for entity, value in run['groups'].items():
            for target in (lane_slot, slot(entities.setdefault(run['lane'], {}), entity)):
                target['runs_ledger_bytes'] += value
                if reconciled(run_id):
                    target['reconciliation_ledger_bytes'] += value
    for (run_id, entity), row in chosen.items():
        name = runs[run_id]['lane'] if run_id in runs else 'current'
        provider = _int(row.get('provider_metered_bytes'))
        for target in (slot(lanes, name), slot(entities.setdefault(name, {}), entity)):
            target['ops_provider_bytes'] += provider
            if reconciled(run_id):
                target['reconciliation_ops_bytes'] += provider
            for counter in ('requests', 'retries', 'cache_hits', 'decoded_bytes'):
                target[counter] += _int(row.get(counter))
            _add_optional(target, row)
    for name, values in lanes.items():
        values['tunnels'] = ledger_summary['tunnels_by_lane'].get(name, 0)
        values['runs_tunnels'] = values.get('runs_tunnels', 0)
        values['requests_per_tunnel'] = _ratio(values['requests'], values['runs_tunnels'])
    for values in [*lanes.values(), *(item for table in entities.values() for item in table.values())]:
        values['kib_per_request'] = _ratio(values['runs_ledger_bytes'] / 1024, values['requests'])

    trino_down = [item for item in ('events_v1', 'proxy_traffic_runs') if ops_sources.get(item) != 'ok']
    available = [item for item, value in ops_sources.items() if value in ('ok', 'empty')]
    if not ledger_summary.get('complete', True):
        forced = ('not_measured', 'ledger_incomplete')
    elif not available:
        forced = ('not_measured', 'no_ops_sources')
    else:
        forced = None

    def status_of(day_bytes, settled_day_bytes, ledger_bytes, ops_bytes):
        """(delta_pct, coverage_pct, status, reason) for one lane or the covered total."""
        delta = ((ops_bytes - ledger_bytes) / ledger_bytes * 100 if ledger_bytes
                 else (0.0 if not ops_bytes else None))
        coverage = settled_day_bytes / day_bytes * 100 if day_bytes else None
        if forced:
            return (delta, coverage, *forced)
        if not day_bytes:
            return delta, coverage, 'not_measured', 'no_covered_traffic'
        if not settled_day_bytes:
            return delta, coverage, 'not_measured', 'no_settled_runs'
        if trino_down:
            return delta, coverage, 'partial', 'unavailable: ' + ', '.join(trino_down)
        if coverage < TRAFFIC_RECONCILIATION_MIN_COVERAGE_PCT:
            return delta, coverage, 'partial', 'coverage_below_threshold'
        ok = delta is not None and abs(delta) <= TRAFFIC_RECONCILIATION_TOLERANCE_PCT
        return delta, coverage, 'ok' if ok else 'gap', None

    covered = [name for name in ('current', 'history', 'other') if name in lanes]
    for name, values in lanes.items():
        values['reconciliation_status'] = 'not_covered' if name in NOT_COVERED_LANES else status_of(
            values['ledger_provider_bytes'], values['reconciled_day_bytes'],
            values['reconciliation_ledger_bytes'], values['reconciliation_ops_bytes'])[2]
    unexplained = sorted(
        ({'lane': run['lane'], 'entity': entity, 'run_id': run_id, 'ledger_bytes': value}
         for run_id, run in runs.items() if reconciled(run_id)
         for entity, value in run['groups'].items() if value and (run_id, entity) not in chosen),
        key=lambda item: (-item['ledger_bytes'], item['lane'], item['run_id'], item['entity']))
    in_progress = [{'run_id': run_id, 'lane': run['lane'], 'bytes_in_day': run['bytes_in_day']}
                   for run_id, run in runs.items() if run['in_progress'] and run['lane'] not in NOT_COVERED_LANES]
    total = {key: sum(lanes[name][key] for name in covered) for key in (
        'ledger_provider_bytes', 'reconciled_day_bytes', 'reconciliation_ledger_bytes', 'reconciliation_ops_bytes')}
    delta, coverage, status, reason = status_of(*total.values())
    return {'lanes': lanes, 'entities': entities, 'reconciliation': {
        'ledger_provider_bytes': total['reconciliation_ledger_bytes'],
        'ops_provider_bytes': total['reconciliation_ops_bytes'], 'delta_pct': delta,
        'day_covered_bytes': total['ledger_provider_bytes'], 'reconciled_ledger_bytes': total['reconciled_day_bytes'],
        'coverage_pct': coverage, 'min_coverage_pct': TRAFFIC_RECONCILIATION_MIN_COVERAGE_PCT,
        'tolerance_pct': TRAFFIC_RECONCILIATION_TOLERANCE_PCT, 'status': status, 'reason': reason,
        'unavailable_sources': trino_down if forced is None else [],
        'covered_lanes': covered, 'not_covered_lanes': list(NOT_COVERED_LANES),
        'in_progress_runs': in_progress,
        'unexplained_bytes': sum(item['ledger_bytes'] for item in unexplained),
        'unexplained_count': len(unexplained), 'unexplained': unexplained[:10]}}


def build_history_editions_sql(campaign_id: str | None = None) -> tuple[str, tuple[str, ...]]:
    """Every campaign scope with its step and acceptance category.

    Step 1 is the newest numeric edition of each competition within the
    campaign (``transfermarkt_history_order`` step 0); non-numeric editions are
    step ``unknown``. Categories need the same proofs as the state invariants.
    """
    where, params = ('WHERE s.campaign_id = CAST(? AS varchar)', (campaign_id,)) if campaign_id else ('', ())
    sql = f'''WITH scopes AS (
 SELECT s.campaign_id, s.scope_id, s.status,
  json_extract_scalar(s.target_json, '$.competition_id') AS competition_id,
  json_extract_scalar(s.target_json, '$.edition_id') AS edition_id,
  coalesce(s.scope_manifest_uri, '') <> '' AS has_manifest,
  coalesce(cardinality(TRY_CAST(json_parse(s.raw_evidence_ids_json) AS array(varchar))), 0) > 0 AS has_evidence,
  coalesce(json_array_length(json_extract(s.record_json, '$.unavailable_confirmation_days')), 0) AS confirmation_days
 FROM {SCOPE_TABLE} s {where}
),
numbered AS (
 SELECT *, CASE WHEN regexp_like(edition_id, '^[0-9]+$') THEN TRY_CAST(edition_id AS integer) END AS season
 FROM scopes
),
categorized AS (
 SELECT n.*, max(season) OVER (PARTITION BY campaign_id, competition_id) AS step1_season,
  CASE
   WHEN status = 'complete' AND has_manifest AND has_evidence THEN 'loaded'
   WHEN status = 'unavailable' AND has_evidence AND confirmation_days >= {UNAVAILABLE_CONFIRMATIONS_REQUIRED}
    THEN 'unavailable_with_evidence'
   WHEN status = 'terminal_error' AND has_evidence THEN 'failed_with_evidence'
   ELSE 'undetermined'
  END AS category
 FROM numbered n
)
SELECT campaign_id, scope_id, competition_id, edition_id, status, category,
 CASE WHEN season IS NULL THEN 'unknown' WHEN season = step1_season THEN 'step1' ELSE 'later' END AS step,
 category = 'undetermined' AND status IN ('complete', 'unavailable', 'terminal_error') AS evidence_missing
FROM categorized
ORDER BY campaign_id, competition_id, edition_id, scope_id'''
    return sql, params


def build_history_campaigns_sql(campaign_id: str | None = None) -> tuple[str, tuple[str, ...]]:
    """Frozen target count of each campaign against its stored scope rows."""
    where, params = ('WHERE c.campaign_id = CAST(? AS varchar)', (campaign_id,)) if campaign_id else ('', ())
    sql = f'''SELECT c.campaign_id, c.status, c.target_count, count(s.scope_id) AS scope_count
FROM {CAMPAIGN_TABLE} c LEFT JOIN {SCOPE_TABLE} s ON s.campaign_id = c.campaign_id
{where}
GROUP BY c.campaign_id, c.status, c.target_count
ORDER BY c.campaign_id'''
    return sql, params


_EDITION_COUNTERS = ('step1_total', 'loaded', 'unavailable_with_evidence', 'failed_with_evidence',
                     'undetermined', 'evidence_missing')


def summarize_editions(rows: Iterable[Mapping[str, Any]], campaigns: Iterable[Mapping[str, Any]],
                       campaign_id: str | None = None) -> dict[str, Any]:
    """Step-1 acceptance per campaign; a missing campaign or target set blocks it."""
    result: dict[str, dict[str, Any]] = {}
    for row in campaigns:
        target, scopes = _int(row['target_count']), _int(row['scope_count'])
        result[str(row['campaign_id'])] = {
            'campaign_status': row['status'], 'target_count': target, 'scope_count': scopes,
            'complete_set': scopes == target, **{name: 0 for name in _EDITION_COUNTERS},
            'undetermined_scope_ids': []}
    if not result:
        return {'status': 'not_available', 'reason': 'кампания не найдена' if campaign_id else 'кампаний нет'}
    for row in rows:
        item = result.get(str(row['campaign_id']))
        if item is None or row['step'] != 'step1':
            continue
        item['step1_total'] += 1
        item[row['category']] += 1
        if row['category'] == 'undetermined':
            item['evidence_missing'] += bool(row['evidence_missing'])
            if len(item['undetermined_scope_ids']) < 20:
                item['undetermined_scope_ids'].append(row['scope_id'])
    totals = {name: sum(item[name] for item in result.values()) for name in _EDITION_COUNTERS}
    status = 'ok' if all(item['complete_set'] for item in result.values()) else 'incomplete'
    return {'status': status, 'campaigns': result, 'step1': totals}


def _mib(value: int) -> str:
    return f'{value / 1048576:.2f}'.replace('.', ',')


def _number(value: float | None) -> str:
    return '—' if value is None else f'{value:.1f}'.replace('.', ',')


_NOT_MEASURED = {'ledger_incomplete': ' (леджер неполный)', 'no_settled_runs': ' (нет завершённых ранов)',
                 'no_covered_traffic': ' (нет трафика актуалки и истории)', 'no_ops_sources': ' (нет ops-источников)'}


def format_traffic_summary(report: Mapping[str, Any]) -> str:
    ledger, lanes = report['ledger'], report['lanes']
    order = [name for name in ('registry', 'current', 'history', 'other')
             if name != 'other' or ledger['by_lane'].get('other')]
    split = ' / '.join(f'{LANE_NAMES[name]} {_mib(ledger["by_lane"].get(name, 0))}' for name in order)
    rec = report['reconciliation']
    requests = '—' if rec['status'] == 'not_measured' else sum(item['requests'] for item in lanes.values())
    per_request = ', '.join(f'{LANE_NAMES[name]} {_number(lanes.get(name, {}).get("kib_per_request"))}'
                            for name in order)
    rejected = ', '.join(f'{reason}: {count}' for reason, count in sorted(ledger['connect_rejected'].items()))
    settled = f'сверено {_number(rec["coverage_pct"])} % байт дня'
    if rec['status'] == 'not_measured':
        coverage = 'ops не измерено' + _NOT_MEASURED.get(rec['reason'], '')
    elif rec['status'] == 'partial' and rec['unavailable_sources']:
        coverage = f'ops частично (недоступно: {", ".join(rec["unavailable_sources"])}); {settled}'
    elif rec['status'] == 'partial':
        coverage = f'ops частично: {settled} (нужно ≥ {_number(rec["min_coverage_pct"])} %)'
    else:
        share = _ratio(rec['ops_provider_bytes'] * 100, rec['ledger_provider_bytes'])
        coverage = (f'ops покрывает {_number(share)} % актуалки и истории '
                    f'({"ok" if rec["status"] == "ok" else "разрыв"}), {settled}; реестр в ops не пишется')
    lines = [f'Transfermarkt трафик {report["report_day"]} МСК: оплачено {_mib(ledger["provider_bytes"]["total"])} МиБ '
             f'({split}), запросов {requests}, КиБ/запрос: {per_request}, туннелей {ledger["tunnels"]} '
             f'(CONNECT-отказы {rejected or "нет"}), {coverage}']
    if ledger.get('incomplete_reason') == 'day_not_covered':
        lines.append('ТРЕВОГА: леджер не покрывает день')
    if ledger.get('archives_unreadable'):
        lines.append('ТРЕВОГА: леджер: нечитаемые архивы: ' + ', '.join(ledger['archives_unreadable']))
    if ledger.get('malformed_lines'):
        lines.append(f'ТРЕВОГА: леджер: битых строк {ledger["malformed_lines"]}')
    return '\n'.join(lines)


def format_editions_summary(report: Mapping[str, Any]) -> str:
    editions = report['history_editions']
    if editions.get('status') == 'incomplete':
        gaps = ', '.join(f'scope {item["scope_count"]} из {item["target_count"]} ({campaign})'
                         for campaign, item in editions['campaigns'].items() if not item['complete_set'])
        return f'история шаг 1: набор целей неполный: {gaps}'
    if editions.get('status') != 'ok':
        return f'история шаг 1: не измерено — {editions.get("reason", "нет данных")}'
    totals = editions['step1']
    return (f'история шаг 1: изданий {totals["step1_total"]}, загружено {totals["loaded"]}, '
            f'недоступно с доказательством {totals["unavailable_with_evidence"]}, '
            f'ошибка с доказательством {totals["failed_with_evidence"]}, не определено {totals["undetermined"]}')
