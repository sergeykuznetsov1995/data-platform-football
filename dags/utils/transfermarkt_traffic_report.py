"""Read-only Transfermarkt paid-traffic report and history step-1 acceptance (#1404).

Paid bytes come only from the gateway ledger (``paid_requests.jsonl`` + rotated
archives). Ops tables and signal-lane receipts explain requests, retries, cache
hits and decoded bytes; their coverage of the ledger is the reconciliation row.
No Trino or Airflow imports here: SQL builders return ``(sql, params)``.
"""
from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import re
from typing import Any, Iterable, Iterator, Mapping
import zlib

from dags.utils.transfermarkt_backfill_state import SCOPE_TABLE, UNAVAILABLE_CONFIRMATIONS_REQUIRED
from dags.utils.transfermarkt_daily_measure import MeasurementError, utc

VERSION = 'traffic-report-v1'
TRAFFIC_RECONCILIATION_TOLERANCE_PCT = 2.0
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


class TrafficReportError(ValueError):
    """The gateway ledger cannot be read; there is no report without it."""


# One entity under different names: ledger/receipt labels (client.py), events_v1, proxy_traffic_runs.
ENTITY_GROUPS = {
    'coaches': {'coach_history', 'coach_profile', 'coach_profiles', 'coaches'},
    'transfers': {'transfer_events', 'transfers'},
    'market_values': {'market_value_points', 'market_value_history'},
    'squads': {'participants_api', 'squad', 'squad_memberships', 'players', 'current_players', 'teilnehmer'},
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


def iter_ledger_events(ledger_dir: Path | str, day_start: datetime, day_end: datetime,
                       stats: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    """Yield ledger events with ``day_start <= occurred_at < day_end``.

    An archive stamped before ``day_start`` holds only earlier events and is
    skipped. An unreadable archive is listed in ``stats``; the current file is
    mandatory. Malformed lines are counted, never fatal.
    """
    stats = {} if stats is None else stats
    stats.update(malformed_lines=0, files_read=[], archives_unreadable=[])
    directory = Path(ledger_dir)
    if not directory.is_dir():
        raise TrafficReportError('ledger directory is missing')
    archives = []
    for path in sorted(directory.iterdir()):
        match = _ARCHIVE.match(path.name)
        if match and datetime.strptime(match.group(1), '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc) >= day_start:
            archives.append(path)
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
                        occurred = utc(event['occurred_at'])
                    except (ValueError, TypeError, KeyError, MeasurementError):
                        stats['malformed_lines'] += 1
                        continue
                    if day_start <= occurred < day_end:
                        yield event
        except (OSError, EOFError, zlib.error) as exc:
            if path is current:
                raise TrafficReportError('ledger file is unreadable') from exc
            stats['archives_unreadable'].append(path.name)


def summarize_ledger(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Paid bytes from ``bytes`` events only: registry leases cross midnight."""
    provider = {'total': 0, 'up': 0, 'down': 0}
    by_lane: dict[str, int] = {}
    by_stream: dict[str, int] = {}
    by_lane_entity: dict[str, dict[str, int]] = {}
    by_run: dict[str, dict[str, dict[str, int]]] = {}
    tunnels_by_lane: dict[str, int] = {}
    rejected: dict[str, int] = {}
    opened = closed = 0
    for event in events:
        kind, name = event.get('event_type'), lane(event.get('dag_id'))
        if kind == 'bytes':
            size = _int(event.get('bytes'))
            entity, run_id = entity_group(event.get('entity')), str(event.get('run_id') or '')
            provider['total'] += size
            if event.get('direction') in ('up', 'down'):
                provider[event['direction']] += size
            by_lane[name] = by_lane.get(name, 0) + size
            stream = str(event.get('stream_id') or '')
            by_stream[stream] = by_stream.get(stream, 0) + size
            entities = by_lane_entity.setdefault(name, {})
            entities[entity] = entities.get(entity, 0) + size
            run = by_run.setdefault(name, {}).setdefault(run_id, {})
            run[entity] = run.get(entity, 0) + size
        elif kind == 'lease_created':
            opened += 1
        elif kind == 'lease_closed':
            closed += 1
            tunnels_by_lane[name] = tunnels_by_lane.get(name, 0) + _int(event.get('tunnels_total'))
        elif kind == 'connect_rejected':
            reason = str(event.get('reason') or 'unknown')
            rejected[reason] = rejected.get(reason, 0) + 1
    tunnels = sum(tunnels_by_lane.values())
    return {'provider_bytes': provider, 'by_lane': by_lane, 'by_stream': by_stream,
            'by_lane_entity': by_lane_entity, 'by_run': by_run, 'leases_opened': opened,
            'leases_closed': closed, 'tunnels': tunnels, 'tunnels_by_lane': tunnels_by_lane,
            'connect_rejected': rejected,
            'bytes_per_tunnel': provider['total'] / tunnels if tunnels else None,
            'run_ids': {name: sorted(runs) for name, runs in by_run.items()}}


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


def read_current_receipts(state_dir: Path | str, day_start: datetime, day_end: datetime) -> dict[str, Any]:
    """Signal-lane receipts by ``captured_at``; the ledger run_id is their child_cycle_id."""
    scopes = Path(state_dir) / 'scopes'
    if not scopes.is_dir():
        return {'status': 'not_available', 'reason': 'signal-lane state directory is missing'}
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
                captured = utc(receipt['captured_at'])
                traffic = receipt.get('traffic') or {}
                by_entity = traffic.get('shared_traffic_ledger', traffic).get('by_entity') or {}
            except (KeyError, TypeError, AttributeError, MeasurementError):
                malformed += 1
                continue
            if key in seen or not day_start <= captured < day_end:
                continue
            seen.add(key)
            for name, counters in by_entity.items():
                entity = entity_group(name)
                row = rows.setdefault((key[1], entity), {
                    'ops_source': 'current_receipts', 'run_id': key[1], 'entity': entity,
                    **{name: 0 for name in _COUNTERS}})
                for name, source in (('requests', 'requests'), ('retries', 'retries'),
                                     ('cache_hits', 'cache_hits'), ('decoded_bytes', 'decoded_bytes'),
                                     ('provider_metered_bytes', 'provider_bytes')):
                    row[name] += _int(counters.get(source))
    by_entity: dict[str, dict[str, int]] = {}
    for row in rows.values():
        target = by_entity.setdefault(row['entity'], {name: 0 for name in _COUNTERS})
        for name in _COUNTERS:
            target[name] += row[name]
    return {'status': 'ok', 'receipts': len(seen), 'malformed': malformed,
            'by_entity': by_entity, 'rows': list(rows.values())}


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def merge_ops(ledger_summary: Mapping[str, Any], ops_rows: Iterable[Mapping[str, Any]] | None,
              receipts: Mapping[str, Any]) -> dict[str, Any]:
    """Lane/entity table plus reconciliation; ``ops_rows=None`` means Trino was not read."""
    run_lane = {run_id: name for name, runs in ledger_summary['run_ids'].items() for run_id in runs}
    # Names of one group sum within a source; across sources the first present wins.
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in [*(ops_rows or ()), *(receipts.get('rows') or ())]:
        key = (str(row.get('run_id') or ''), entity_group(row.get('entity')))
        total = grouped.setdefault(key, {}).setdefault(row['ops_source'], {
            'ops_source': row['ops_source'], **{name: 0 for name in _COUNTERS}})
        for name in _COUNTERS:
            total[name] += _int(row.get(name))
    chosen = {key: next(sources[name] for name in _OPS_PRIORITY if name in sources)
              for key, sources in grouped.items()}
    lanes: dict[str, dict[str, Any]] = {}
    entities: dict[str, dict[str, dict[str, Any]]] = {}

    def slot(table, name):
        return table.setdefault(name, {'ledger_provider_bytes': 0, 'ops_provider_bytes': 0,
                                       **{item: 0 for item in _COUNTERS if item != 'provider_metered_bytes'}})

    for name, value in ledger_summary['by_lane'].items():
        slot(lanes, name)['ledger_provider_bytes'] = value
    for name, values in ledger_summary['by_lane_entity'].items():
        for entity, value in values.items():
            slot(entities.setdefault(name, {}), entity)['ledger_provider_bytes'] = value
    for (run_id, entity), row in chosen.items():
        name = run_lane.get(run_id, 'current' if row['ops_source'] == 'current_receipts' else 'other')
        provider = _int(row.get('provider_metered_bytes'))
        for target in (slot(lanes, name), slot(entities.setdefault(name, {}), entity)):
            target['ops_provider_bytes'] += provider
            for counter in ('requests', 'retries', 'cache_hits', 'decoded_bytes'):
                target[counter] += _int(row.get(counter))
    for name, values in lanes.items():
        values['tunnels'] = ledger_summary['tunnels_by_lane'].get(name, 0)
        values['requests_per_tunnel'] = _ratio(values['requests'], values['tunnels'])
    for values in [*lanes.values(), *(item for table in entities.values() for item in table.values())]:
        values['kib_per_request'] = _ratio(values['ledger_provider_bytes'] / 1024, values['requests'])
    def status_of(ledger_bytes, ops_bytes):
        delta = ((ops_bytes - ledger_bytes) / ledger_bytes * 100 if ledger_bytes
                 else (0.0 if not ops_bytes else None))
        if ops_rows is None:
            return delta, 'not_measured'
        ok = delta is not None and abs(delta) <= TRAFFIC_RECONCILIATION_TOLERANCE_PCT
        return delta, 'ok' if ok else 'gap'

    covered = [name for name in ('current', 'history', 'other') if name in lanes]
    for name, values in lanes.items():
        values['reconciliation_status'] = ('not_covered' if name in NOT_COVERED_LANES else
                                           status_of(values['ledger_provider_bytes'], values['ops_provider_bytes'])[1])
    unexplained = sorted(
        ({'lane': name, 'entity': entity, 'run_id': run_id, 'ledger_bytes': value}
         for name, runs in ledger_summary['by_run'].items() if name not in NOT_COVERED_LANES
         for run_id, values in runs.items()
         for entity, value in values.items() if value and (run_id, entity) not in chosen),
        key=lambda item: (-item['ledger_bytes'], item['lane'], item['run_id'], item['entity']))
    ledger_total = sum(lanes[name]['ledger_provider_bytes'] for name in covered)
    ops_total = sum(lanes[name]['ops_provider_bytes'] for name in covered)
    delta, status = status_of(ledger_total, ops_total)
    return {'lanes': lanes, 'entities': entities, 'reconciliation': {
        'ledger_provider_bytes': ledger_total, 'ops_provider_bytes': ops_total, 'delta_pct': delta,
        'tolerance_pct': TRAFFIC_RECONCILIATION_TOLERANCE_PCT, 'status': status,
        'covered_lanes': covered, 'not_covered_lanes': list(NOT_COVERED_LANES),
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


_EDITION_COUNTERS = ('step1_total', 'loaded', 'unavailable_with_evidence', 'failed_with_evidence',
                     'undetermined', 'evidence_missing')


def summarize_editions(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    campaigns: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row['step'] != 'step1':
            continue
        item = campaigns.setdefault(str(row['campaign_id']), {
            **{name: 0 for name in _EDITION_COUNTERS}, 'undetermined_scope_ids': []})
        item['step1_total'] += 1
        item[row['category']] += 1
        if row['category'] == 'undetermined':
            item['evidence_missing'] += bool(row['evidence_missing'])
            if len(item['undetermined_scope_ids']) < 20:
                item['undetermined_scope_ids'].append(row['scope_id'])
    totals = {name: sum(item[name] for item in campaigns.values()) for name in _EDITION_COUNTERS}
    return {'status': 'ok', 'campaigns': campaigns, 'step1': totals}


def _mib(value: int) -> str:
    return f'{value / 1048576:.2f}'.replace('.', ',')


def _number(value: float | None) -> str:
    return '—' if value is None else f'{value:.1f}'.replace('.', ',')


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
    if rec['status'] == 'not_measured':
        coverage = 'ops не измерено'
    else:
        share = _ratio(rec['ops_provider_bytes'] * 100, rec['ledger_provider_bytes'])
        coverage = (f'ops покрывает {_number(share)} % актуалки и истории '
                    f'({"ok" if rec["status"] == "ok" else "разрыв"}); реестр в ops не пишется')
    lines = [f'Transfermarkt трафик {report["report_day"]} МСК: оплачено {_mib(ledger["provider_bytes"]["total"])} МиБ '
             f'({split}), запросов {requests}, КиБ/запрос: {per_request}, туннелей {ledger["tunnels"]} '
             f'(CONNECT-отказы {rejected or "нет"}), {coverage}']
    if ledger.get('archives_unreadable'):
        lines.append('ТРЕВОГА: леджер: нечитаемые архивы: ' + ', '.join(ledger['archives_unreadable']))
    if ledger.get('malformed_lines'):
        lines.append(f'ТРЕВОГА: леджер: битых строк {ledger["malformed_lines"]}')
    return '\n'.join(lines)


def format_editions_summary(report: Mapping[str, Any]) -> str:
    editions = report['history_editions']
    if editions.get('status') != 'ok':
        return f'история шаг 1: не измерено — {editions.get("reason", "нет данных")}'
    totals = editions['step1']
    return (f'история шаг 1: изданий {totals["step1_total"]}, загружено {totals["loaded"]}, '
            f'недоступно с доказательством {totals["unavailable_with_evidence"]}, '
            f'ошибка с доказательством {totals["failed_with_evidence"]}, не определено {totals["undetermined"]}')
