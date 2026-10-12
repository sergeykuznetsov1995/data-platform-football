"""Read-only, once-per-Moscow-day native Bronze audit (#1395).

A completed RED audit is still a completed audit. Query failures are incomplete
and retried; neither cached RED nor incomplete evidence is ever reported green.
The local deployment marker records the first actual invocation, not a guessed
historical date. Iceberg reads are pinned to the invocation's cutoff, including
ops evidence and promoted-registry inputs used by the existing full DQ suite.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from utils import transfermarkt_bronze_dq as dq

VERSION = 'tm-daily-bronze-dq-v1'
MOSCOW = ZoneInfo('Europe/Moscow')
DEFAULT_STATE_ROOT = Path('/opt/airflow/logs/transfermarkt-native-v2/daily-dq')
FETCH_STATE = 'iceberg.ops.transfermarkt_fetch_state'
CAREER_REFS = 'iceberg.ops.transfermarkt_career_capture_refs_v1'
FIRST_SEEN_REGISTRY = 'iceberg.ops.transfermarkt_first_seen_v1'
AUDIT_TABLES = tuple(sorted({
    *dq.NATIVE_BRONZE_KEYS, *dq.LEGACY_BRONZE_KEYS,
    dq.COMPETITIONS_REGISTRY_TABLE, dq.EDITIONS_REGISTRY_TABLE,
    dq.REGISTRY_STATE_TABLE, dq.SCOPE_MANIFEST_TABLE, FETCH_STATE, CAREER_REFS,
}))
LINEAGE = ('source_url', 'source_body_hash', 'fetched_at', '_ingested_at',
           'parser_revision', 'schema_revision', 'cycle_id', 'scope_id', '_batch_id')
# Descriptive fill counters have no invented numerical pass threshold. Only the
# contract table encodes not_applicable; numeric facts retain raw applicability.
FIELDS = {
    'competitions': ('competition_id', 'name', 'country', 'competition_type'),
    'competition_editions': ('competition_id', 'edition_id', 'edition_label',
                            'canonical_season', 'participant_count'),
    'squad_memberships': ('player_id', 'club_id', 'player_name', 'club_name'),
    'player_attribute_observations': ('player_id', 'name', 'position', 'dob',
                                     'height_cm', 'foot', 'nationality'),
    'player_contract_observations': ('player_id', 'team_id', 'contract_until'),
    'market_value_points': ('player_id', 'mv_date', 'value_eur'),
    'transfer_events': ('transfer_id', 'player_id', 'transfer_date', 'fee_eur'),
    'coach_profiles': ('coach_id', 'name', 'dob', 'nationality'),
    'coach_stints': ('club_id', 'coach_id', 'appointed_date', 'role'),
}
PLACEHOLDERS = ('-', '—', '?', 'n/a', 'unknown')


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError('daily DQ timestamps must include timezone')
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    # Bronze clocks are timestamp(6) without zone, with UTC semantics.
    return "TIMESTAMP '" + _utc(value).replace(tzinfo=None).isoformat(' ') + "'"


def _atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as output:
        output.write(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                indent=2, default=str) + '\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _row(cur, sql: str) -> tuple:
    cur.execute(sql)
    rows = list(cur.fetchall())
    if len(rows) != 1:
        raise ValueError('daily DQ aggregate must return exactly one row')
    return tuple(rows[0])


def pin_audit_snapshots(cur, cutoff: datetime) -> dict[str, int]:
    """Pin all inputs at one cutoff; missing/empty snapshots fail explicitly."""
    pins = {}
    for table in AUDIT_TABLES:
        catalog, schema, name = table.split('.')
        row = _row(cur, f'SELECT snapshot_id FROM {catalog}.{schema}."{name}$snapshots" '
                   f'WHERE committed_at <= from_iso8601_timestamp({dq._sql_literal(_utc(cutoff).isoformat())}) '
                   'ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1')
        if isinstance(row[0], bool) or int(row[0]) <= 0:
            raise ValueError(f'invalid audit snapshot for {table}')
        pins[table] = int(row[0])
    exists = _row(cur, "SELECT COUNT(*) FROM iceberg.information_schema.tables WHERE table_schema = 'ops' AND table_name = 'transfermarkt_first_seen_v1'")[0]
    if not int(exists):
        return pins
    cur.execute('SELECT snapshot_id FROM iceberg.ops.\"transfermarkt_first_seen_v1$snapshots\" '
                f'WHERE committed_at <= from_iso8601_timestamp({dq._sql_literal(_utc(cutoff).isoformat())}) '
                'ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1')
    optional = list(cur.fetchall())
    if optional:
        if len(optional) != 1 or isinstance(optional[0][0], bool) or int(optional[0][0]) <= 0:
            raise ValueError('invalid first_seen registry snapshot')
        pins[FIRST_SEEN_REGISTRY] = int(optional[0][0])
    return pins


class _PinnedCursor:
    """Also freeze live ops joins in the older full DQ SQL builders."""
    def __init__(self, cursor, pins):
        self.cursor, self.pins = cursor, pins

    def execute(self, sql):
        for table, snapshot in sorted(self.pins.items(), key=lambda item: -len(item[0])):
            sql = re.sub(r'\b(FROM|JOIN)\s+' + re.escape(table)
                         + r'(?![\w.]|\s+FOR\s+VERSION\s+AS\s+OF)',
                         lambda match: f'{match.group(1)} {table} FOR VERSION AS OF {snapshot}',
                         sql, flags=re.IGNORECASE)
        return self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()


def _placeholder(column: str) -> str:
    values = ', '.join(dq._sql_literal(item) for item in PLACEHOLDERS)
    return f'LOWER(TRIM({column})) IN ({values})'


def _applicable(entity: str, field: str) -> str:
    if entity == 'player_contract_observations':
        return "applicability_status = 'ok'"
    raw = {'value_eur': 'mv_raw', 'fee_eur': 'fee_text'}.get(field)
    if raw:
        return f'({raw} IS NOT NULL AND TRIM({raw}) <> \'\' AND NOT ({_placeholder(raw)}))'
    return 'TRUE'


def build_field_metrics_sql(entity: str, implementation_at: datetime) -> str:
    """One bounded aggregate per entity, with a separate old/unknown cohort."""
    if entity not in FIELDS:
        raise ValueError('unknown daily DQ entity')
    table = ('iceberg.bronze.transfermarkt_competitions' if entity == 'competitions'
             else 'iceberg.bronze.transfermarkt_competition_editions'
             if entity == 'competition_editions' else dq.ENTITY_BRONZE_TABLES[entity])
    if entity == 'competitions':
        native = '(competition_id IS NOT NULL)'
    else:
        comp, ed = dq._entity_scope_columns(table)
        native = f'({comp} IS NOT NULL AND {ed} IS NOT NULL)'
    after = f'(_ingested_at >= {_timestamp(implementation_at)} OR fetched_at >= {_timestamp(implementation_at)})'
    lineage_null = ' OR '.join(f'{field} IS NULL' for field in LINEAGE)
    counts = [
        'COUNT(*) AS total_rows',
        f'COUNT_IF(NOT {native}) AS legacy_rows',
        f'COUNT_IF({native} AND NOT COALESCE({after}, FALSE)) AS before_cutoff_or_unknown_rows',
        f'COUNT_IF({native} AND {after}) AS after_cutoff_rows',
        f'COUNT_IF({native} AND {after} AND ({lineage_null})) AS lineage_null_after_cutoff',
        f'COUNT_IF({native} AND _ingested_at IS NULL AND fetched_at IS NULL) AS unknown_clock_rows',
    ]
    for field in FIELDS[entity]:
        applies = _applicable(entity, field)
        known_value = (f'{field} IS NOT NULL AND '
                       f'TRIM(CAST({field} AS varchar)) <> \'\' AND '
                       f'NOT ({_placeholder("CAST(" + field + " AS varchar)")})')
        counts.extend((
            f'COUNT_IF({native} AND {applies}) AS {field}_applicable',
            f'COUNT_IF({native} AND {applies} AND {known_value}) AS {field}_filled',
            f'COUNT_IF({native} AND NOT COALESCE({applies}, FALSE)) AS {field}_not_applicable_or_unknown',
        ))
    return 'SELECT ' + ',\n       '.join(counts) + f'\nFROM {table}'


def build_placeholder_zero_sql() -> str:
    """A genuine free transfer is legal zero; only raw placeholders prove dirt."""
    return f"""SELECT
    (SELECT COUNT(*) FROM iceberg.bronze.transfermarkt_market_value_points
     WHERE value_eur = 0 AND {_placeholder('mv_raw')}) AS market_value_placeholder_zero,
    (SELECT COUNT(*) FROM iceberg.bronze.transfermarkt_transfer_events
     WHERE fee_eur = 0 AND {_placeholder('fee_text')}) AS transfer_fee_placeholder_zero"""


def build_empty_routes_sql(routes: Mapping[str, str]) -> str:
    """Latest listing outcome per edition; missing status is unknown, not empty."""
    if not routes or any(route not in {'pokal', 'wettbewerb'} for route in routes.values()):
        raise ValueError('daily DQ routes require the denominator contract')
    values = ', '.join('(' + dq._sql_literal(key) + ', ' + dq._sql_literal(route) + ')'
                       for key, route in sorted(routes.items()))
    return f"""WITH routes(competition_id, route) AS (VALUES {values}), latest AS (
    SELECT competition_id, edition_id,
           json_extract_scalar(entity_manifest_json, '$.dq_evidence.scope_capture.listing_status') listing_status,
           ROW_NUMBER() OVER (PARTITION BY competition_id, edition_id
                              ORDER BY committed_at DESC, child_cycle_id DESC) rn
    FROM {dq.SCOPE_MANIFEST_TABLE} WHERE status = 'complete'
) SELECT COALESCE(r.route, 'unknown'), COUNT(*),
         COUNT_IF(listing_status = 'authoritative_empty'),
         COUNT_IF(listing_status IS NULL OR listing_status NOT IN ('ok', 'authoritative_empty'))
  FROM latest l LEFT JOIN routes r ON l.competition_id = r.competition_id
  WHERE rn = 1 GROUP BY 1"""


def collect_first_seen_metrics(cur, implementation_at: datetime,
                               pins: Mapping[str, int], progress=None) -> tuple[dict, list]:
    """Use the writer's canonical natural keys; no schema migration in audits."""
    from scrapers.transfermarkt.first_seen import KEYS

    metrics, checks = {}, []
    for name, keys in KEYS.items():
        table = 'iceberg.bronze.' + name
        cur.execute("SELECT column_name FROM iceberg.information_schema.columns "
                    "WHERE table_schema = 'bronze' AND table_name = " + dq._sql_literal(name))
        columns = {str(row[0]) for row in cur.fetchall()}
        if 'first_seen_at' not in columns:
            metrics[name] = {'status': 'UNKNOWN', 'missing_column': True,
                             'new_rows': None, 'null_new_rows': None,
                             'registry_mismatches': None}
            continue
        after = f'(b._ingested_at >= {_timestamp(implementation_at)} OR b.fetched_at >= {_timestamp(implementation_at)})'
        row = _row(cur, f'SELECT COUNT_IF({after}), COUNT_IF({after} AND b.first_seen_at IS NULL) FROM {table} b')
        new_rows, nulls = map(int, row)
        checks.append(dq.BronzeCheckResult(
            name=f'tm_daily_first_seen_completeness[{name}]', kind='daily_first_seen_completeness',
            severity='ERROR', passed=nulls == 0, value=nulls))
        metrics[name] = {'status': 'RED' if nulls else 'UNKNOWN',
                         'missing_column': False, 'new_rows': new_rows,
                         'null_new_rows': nulls, 'registry_mismatches': None}
        if progress is not None:
            progress(metrics, checks)
        mismatch = None
        if FIRST_SEEN_REGISTRY in pins:
            joins = []
            for index, key in enumerate(keys):
                value = (f'CAST(CAST(b.{key} AS date) AS varchar)'
                         if key in {'mv_date', 'appointed_date', 'left_date'}
                         else f'CAST(b.{key} AS varchar)')
                joins.append(f"TRY(json_extract_scalar(r.key_json, '$[{index}]')) IS NOT DISTINCT FROM {value}")
            predicate = ' AND '.join(joins)
            mismatch = int(_row(cur, f'SELECT COUNT(*) FROM {table} b LEFT JOIN {FIRST_SEEN_REGISTRY} r '
                                 f'ON r.table_name = {dq._sql_literal(name)} AND {predicate} '
                                 'WHERE b.first_seen_at IS NOT NULL '
                                 'AND (r.first_seen_at IS NULL OR b.first_seen_at <> r.first_seen_at)')[0])
            checks.append(dq.BronzeCheckResult(
                name=f'tm_daily_first_seen_invariance[{name}]', kind='daily_first_seen_invariance',
                severity='ERROR', passed=mismatch == 0, value=mismatch))
        metrics[name] = {'status': 'RED' if nulls or mismatch else 'GREEN' if mismatch is not None else 'UNKNOWN',
                         'missing_column': False, 'new_rows': new_rows,
                         'null_new_rows': nulls, 'registry_mismatches': mismatch}
        if progress is not None:
            progress(metrics, checks)
    return metrics, checks


def collect_daily_metrics(cur, implementation_at: datetime,
                          routes: Mapping[str, str], progress=None) -> tuple[dict, list]:
    metrics: dict[str, Any] = {'fields': {}, 'cohorts': {}, 'empty_by_route': {}}
    results = []
    for entity, fields in FIELDS.items():
        row = _row(cur, build_field_metrics_sql(entity, implementation_at))
        if len(row) != 6 + 3 * len(fields):
            raise ValueError(f'invalid field audit shape for {entity}')
        counters = [int(value) for value in row]
        metrics['cohorts'][entity] = dict(zip(
            ('total', 'legacy_unknown', 'before_cutoff_or_unknown', 'after_cutoff',
             'lineage_null_after_cutoff', 'unknown_clock'), counters[:6]))
        metrics['fields'][entity] = {}
        for index, field in enumerate(fields):
            applicable, filled, excluded = counters[6 + 3 * index:9 + 3 * index]
            metrics['fields'][entity][field] = {
                'applicable': applicable, 'filled': filled,
                'not_applicable_or_unknown': excluded,
                'fill_ratio': filled / applicable if applicable else None,
            }
        results.append(dq.BronzeCheckResult(
            name=f'tm_daily_null_lineage[{entity}]', kind='daily_null_lineage',
            severity='ERROR', passed=counters[4] == 0, value=counters[4],
            details=f'post-deployment cutoff {_utc(implementation_at).isoformat()}'))
        if progress is not None:
            progress(metrics, results)
    zeros = _row(cur, build_placeholder_zero_sql())
    if len(zeros) != 2:
        raise ValueError('invalid placeholder audit shape')
    for label, value in zip(('market_value', 'transfer_fee'), zeros):
        results.append(dq.BronzeCheckResult(
            name=f'tm_daily_placeholder_zero[{label}]', kind='daily_placeholder_zero',
            severity='ERROR', passed=int(value) == 0, value=int(value)))
    if progress is not None:
        progress(metrics, results)
    cur.execute(build_empty_routes_sql(routes))
    rows = list(cur.fetchall())
    for route, total, empty, unknown in rows:
        total, empty, unknown = int(total), int(empty), int(unknown)
        ratio = empty / total if total else None
        metrics['empty_by_route'][str(route)] = {
            'editions': total, 'authoritative_empty': empty, 'unknown': unknown,
            'ratio': ratio,
        }
        if route == 'pokal':
            results.append(dq.BronzeCheckResult(
                name='tm_daily_pokal_authoritative_empty', kind='daily_empty_route',
                severity='ERROR', passed=empty * 20 <= total,
                value=metrics['empty_by_route'][route], details='RED strictly above 5%'))
        if progress is not None:
            progress(metrics, results)
    for route in ('pokal', 'wettbewerb'):
        metrics['empty_by_route'].setdefault(route, {
            'editions': 0, 'authoritative_empty': None, 'unknown': None, 'ratio': None,
        })
    return metrics, results


def _routes() -> dict[str, str]:
    from scrapers.transfermarkt.denominator import load_denominator
    return {key: item.route for key, item in load_denominator().rows.items()}


def _unknown(metrics: Mapping[str, Any], checks: list) -> bool:
    if any(item.get('status') == 'UNKNOWN' for item in metrics.get('first_seen', {}).values()):
        return True
    if any(item.get('unknown_clock', 0) for item in metrics.get('cohorts', {}).values()):
        return True
    if any(item.get('unknown') or item.get('ratio') is None
           for item in metrics.get('empty_by_route', {}).values()):
        return True
    debt = next((item for item in checks if item.kind == 'career_debt'), None)
    return not debt or not isinstance(debt.value, dict) or bool(
        debt.value.get('null_evidence_manifests') or not debt.value.get('complete_manifests'))


def run_daily_bronze_dq(cur, *, state_root: Path = DEFAULT_STATE_ROOT,
                        run_id: str, now: datetime | None = None,
                        routes: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Persist one completed full audit/day; incomplete attempts remain retryable."""
    clock = _utc(now or datetime.now(timezone.utc))
    day = clock.astimezone(MOSCOW).date().isoformat()
    root = Path(state_root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'audit.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        marker = root / 'implementation.json'
        if not marker.exists():
            _atomic(marker, {'implementation_at': clock.isoformat(),
                             'first_run_id': str(run_id), 'version': VERSION})
        deployed = _utc(datetime.fromisoformat(json.loads(marker.read_text())['implementation_at']))
        if deployed > clock:
            raise ValueError('audit clock precedes actual deployment marker')
        destination = root / f'{day}.json'
        if destination.exists():
            previous = json.loads(destination.read_text())
            if previous.get('completed') and previous.get('version') == VERSION:
                return {**previous, 'cached': True}
        report: dict[str, Any] = {
            'version': VERSION, 'day_msk': day, 'run_id': str(run_id),
            'cutoff_at': clock.isoformat(), 'implementation_at': deployed.isoformat(),
            'completed': False, 'status': 'UNKNOWN', 'cached': False,
            'input_snapshot_ids': {}, 'checks': [], 'metrics': {},
            'paid_requests': 0,
        }
        def remember(metrics, checks):
            # Keep each completed check before a subsequent query can fail.
            report.update(metrics=metrics, checks=[asdict(check) for check in checks])
            report['errors'] = [check.name for check in checks if check.severity == 'ERROR' and not check.passed]
            if report['errors']:
                report['status'] = 'RED'
        try:
            pins = pin_audit_snapshots(cur, clock)
            report['input_snapshot_ids'] = pins
            frozen = _PinnedCursor(cur, pins)
            checks = dq.run_bronze_dq(frozen, registry_snapshot_id=None,
                                      pins=pins, zone='full')
            remember({}, checks)
            metrics, daily_checks = collect_daily_metrics(
                frozen, deployed, routes or _routes(),
                progress=lambda m, c: remember(m, checks + c))
            checks += daily_checks
            remember(metrics, checks)
            first_seen, first_seen_checks = collect_first_seen_metrics(
                frozen, deployed, pins,
                progress=lambda m, c: remember({**metrics, 'first_seen': m}, checks + c))
            metrics['first_seen'] = first_seen
            checks += first_seen_checks
            report.update(checks=[asdict(check) for check in checks], metrics=metrics)
            execution_errors = [check.name for check in checks if check.error]
            errors = [check.name for check in checks if check.severity == 'ERROR' and not check.passed]
            debt = next((check.value for check in checks if check.kind == 'career_debt'), None)
            report['metrics']['career_debt'] = debt
            report['errors'] = errors
            report['execution_errors'] = execution_errors
            report['completed'] = not execution_errors
            report['status'] = 'RED' if errors else ('UNKNOWN' if execution_errors or _unknown(metrics, checks) else 'GREEN')
        except Exception as exc:  # retain failed evidence, then retry on next invocation
            report['execution_errors'] = [f'{type(exc).__name__}: {exc}']
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        attempt = hashlib.sha256((str(run_id) + report['finished_at']).encode()).hexdigest()
        attempts = root / 'attempts'
        attempts.mkdir(exist_ok=True)
        _atomic(attempts / f'{day}-{attempt}.json', report)
        _atomic(destination, report)
        return report


def read_daily_result(*, state_root: Path = DEFAULT_STATE_ROOT,
                      now: datetime | None = None) -> dict[str, Any]:
    clock = _utc(now or datetime.now(timezone.utc))
    day = clock.astimezone(MOSCOW).date().isoformat()
    path = Path(state_root) / f'{day}.json'
    if not path.exists():
        return {'status': 'UNKNOWN', 'completed': False, 'stale': True, 'day_msk': day}
    report = json.loads(path.read_text())
    report['stale'] = report.get('day_msk') != day or report.get('version') != VERSION
    if report['stale']:
        report['status'] = 'UNKNOWN'
    return report


def format_daily_dq_summary(report: Mapping[str, Any]) -> str:
    status = report.get('status', 'UNKNOWN')
    if report.get('stale') or (not report.get('completed') and status != 'RED'):
        status = 'UNKNOWN'
    if not report.get('completed') and status == 'RED':
        status += ' (неполная проверка)'
    errors = report.get('errors')
    error_count = str(len(errors)) if isinstance(errors, list) else 'unknown'
    debt = report.get('metrics', {}).get('career_debt')
    pending = debt.get('career_fetches_pending') if isinstance(debt, dict) else None
    return f'Bronze DQ {report.get("day_msk", "?")} МСК: {status}; ошибок {error_count}; карьерный долг {pending if pending is not None else "unknown"}'
