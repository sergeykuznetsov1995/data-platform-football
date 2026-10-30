"""Read-only daily Transfermarkt measurements (#1395).

One SELECT reads the #1393 journal. The denominator is supplied from the same
promoted-registry contract as the current runner; blocked competitions remain
in it. Site sampling imports existing raw/read-back evidence, never makes HTTP.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from dags.utils.transfermarkt_backfill_state import ATTEMPT_TABLE, BATCH_TABLE, SCOPE_TABLE
from dags.utils.transfermarkt_current_state import (CHECKS_TABLE, SIGNALS_TABLE, current_scope_targets,
                                                  denominator_definition_hash, edition_measurement_fingerprint)
from scrapers.transfermarkt.current_capture import semantic_signature

VERSION = 'daily-measure-v1'
MOSCOW = ZoneInfo('Europe/Moscow')


class MeasurementError(ValueError):
    """Evidence cannot establish the measurement."""


def utc(value: datetime | str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except (ValueError, TypeError) as exc:
        raise MeasurementError('timestamp must be ISO8601 with timezone') from exc
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise MeasurementError('timestamp must include timezone')
    return result.astimezone(timezone.utc)


def day_bounds(day: date | str) -> tuple[datetime, datetime]:
    day = date.fromisoformat(day) if isinstance(day, str) else day
    if not isinstance(day, date) or isinstance(day, datetime):
        raise MeasurementError('report day must be a date')
    start = datetime.combine(day, datetime.min.time(), MOSCOW).astimezone(timezone.utc)
    return start, start + timedelta(days=1)


@dataclass(frozen=True)
class DailyDenominator:
    # competition, scope, edition, snapshot, team type. Blockers have NULL scope.
    rows: tuple[tuple[str, str | None, str | None, str | None, str | None, str | None], ...]
    denominator_hash: str
    denominator_version: str
    blocked_competitions: tuple[tuple[str, str], ...]
    denominator_definition_hash: str

    @property
    def competition_count(self) -> int:
        return len({row[0] for row in self.rows})


def daily_denominator(denominator_rows: Iterable[Any], registry_rows: Iterable[Mapping[str, Any]], *,
                      denominator_version: str) -> DailyDenominator:
    """Use promoted rows returned by build_registry_scope_query, not a stale TSV edition."""
    registry_rows = tuple(registry_rows)
    denominator_rows = tuple(denominator_rows)
    targets = current_scope_targets(denominator_rows, registry_rows)
    if not targets.denominator_competition_ids or not denominator_version.strip():
        raise MeasurementError('live core denominator and version are required')
    current_targets = [{'scope_id': t.scope_id, 'competition_id': t.competition_id,
                        'edition_id': t.edition_id, 'registry_snapshot_id': t.registry_snapshot_id,
                        'queue_rank': t.queue_rank} for t in targets.targets]
    # Byte-for-byte compatible with run_current_portion's check provenance.
    digest = semantic_signature({'competition_ids': targets.denominator_competition_ids,
                                 'current_editions': [t for t in current_targets if t['queue_rank'] == 0],
                                 'blocked_competitions': targets.blocked_competitions})
    teams = {(str(r.get('competition_id', '')), str(r.get('edition_id', ''))): r.get('team_type') for r in registry_rows}
    fingerprints = {(str(r.get('competition_id', '')), str(r.get('edition_id', ''))): edition_measurement_fingerprint(r) for r in registry_rows}
    rows = []
    for competition in targets.denominator_competition_ids:
        matches = [t for t in targets.targets if t.competition_id == competition and t.queue_rank == 0]
        rows.extend((competition, t.scope_id, t.edition_id, t.registry_snapshot_id,
                     teams[competition, t.edition_id], fingerprints[competition, t.edition_id]) for t in matches)
        if not matches:
            rows.append((competition, None, None, None, None, None))
    return DailyDenominator(tuple(rows), digest, denominator_version, targets.blocked_competitions,
                            denominator_definition_hash(denominator_rows))


def build_daily_measure_sql(denominator: DailyDenominator, day: date | str, *, as_of: datetime | str
                            ) -> tuple[str, tuple[Any, ...]]:
    """A single parameterized Trino SELECT; timestamp(6) ops clocks are UTC.

    Freshness is a detection-day cohort. Only deadlines already elapsed at as_of
    enter the rate. Immature changes and old unknown provenance remain explicit.
    Re-run after day_end+24h to finalize the cohort.
    """
    start, end = day_bounds(day)
    observed = utc(as_of)
    if observed < start:
        raise MeasurementError('as_of predates the report day')
    if not denominator.rows:
        raise MeasurementError('empty denominator')
    values = ',\n'.join('(CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar))'
                        for _ in denominator.rows)
    params = (start.replace(tzinfo=None), end.replace(tzinfo=None), observed.replace(tzinfo=None),
              denominator.denominator_hash, denominator.denominator_version, VERSION, denominator.denominator_definition_hash,
              *(cell for row in denominator.rows for cell in row))
    sql = f'''WITH
args(day_start, day_end, as_of, denominator_hash, denominator_version, measurement_version, denominator_definition_hash) AS (
 VALUES (CAST(? AS timestamp(6)), CAST(? AS timestamp(6)), CAST(? AS timestamp(6)),
         CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar), CAST(? AS varchar))
),
core(competition_id, scope_id, edition_id, registry_snapshot_id, team_type, edition_fingerprint) AS (VALUES {values}),
valid_checks AS (
 SELECT c.scope_id, c.check_kind FROM {CHECKS_TABLE} c
 JOIN core d ON c.scope_id = d.scope_id
 CROSS JOIN args a
 WHERE ((c.registry_snapshot_id = d.registry_snapshot_id AND c.denominator_hash = a.denominator_hash)
        OR (c.edition_fingerprint = d.edition_fingerprint
            AND c.denominator_definition_hash = a.denominator_definition_hash
            AND c.registry_snapshot_id IS NOT NULL AND length(c.denominator_hash) = 64))
   AND c.status = 'complete' AND c.expected_ids = c.observed_ids
   AND c.checked_at >= a.day_start AND c.checked_at < a.day_end AND c.checked_at <= a.as_of
   AND cardinality(CAST(json_parse(c.raw_capture_ids_json) AS array(varchar))) > 0
   AND c.check_kind IN ('listing', 'players', 'injury')
   AND (c.check_kind <> 'injury' OR d.team_type <> 'national_team'
        OR (json_extract_scalar(TRY(json_parse(c.result)), '$.applicability_status') = 'not_applicable'
            AND json_extract_scalar(TRY(json_parse(c.result)), '$.rule') = 'league_injury_listing:club_competitions'
            AND json_extract_scalar(TRY(json_parse(c.result)), '$.registry_snapshot_id') = c.registry_snapshot_id))
 GROUP BY c.scope_id, c.check_kind
),
edition_coverage AS (
 SELECT d.competition_id, d.scope_id, count(DISTINCT c.check_kind) = 3 AS checked
 FROM core d LEFT JOIN valid_checks c ON d.scope_id = c.scope_id
 GROUP BY d.competition_id, d.scope_id
),
competition_coverage AS (
 SELECT competition_id, bool_and(checked) AS checked FROM edition_coverage GROUP BY competition_id
),
signal_events AS (
 SELECT scope_id, entity, entity_id, observed_signature AS signature, signal_version,
 first_detected_at, status, result, bronze_manifest, committed_at,
 COALESCE(measurement_kind, 'unknown') AS measurement_kind FROM {SIGNALS_TABLE}
 UNION ALL
 SELECT s.scope_id, s.entity, s.entity_id, json_extract_scalar(json_parse(e), '$.signature'),
 json_extract_scalar(json_parse(e), '$.signal_version'),
 CAST(at_timezone(from_iso8601_timestamp(json_extract_scalar(json_parse(e), '$.first_detected_at')), 'UTC') AS timestamp(6)),
 json_extract_scalar(json_parse(e), '$.status'), json_extract_scalar(json_parse(e), '$.result'),
 json_extract_scalar(json_parse(e), '$.bronze_manifest'),
 CAST(at_timezone(from_iso8601_timestamp(json_extract_scalar(json_parse(e), '$.committed_at')), 'UTC') AS timestamp(6)),
 COALESCE(json_extract_scalar(json_parse(e), '$.measurement_kind'), 'unknown')
 FROM {SIGNALS_TABLE} s CROSS JOIN UNNEST(CAST(json_parse(s.superseded_evidence_json) AS array(varchar))) AS u(e)
),
cohort AS (
 SELECT DISTINCT e.* FROM signal_events e JOIN core d ON d.scope_id = e.scope_id CROSS JOIN args a
 WHERE e.first_detected_at >= a.day_start AND e.first_detected_at < a.day_end
 AND e.first_detected_at <= a.as_of AND e.entity <> 'injury'
),
event_totals AS (
 SELECT count_if(measurement_kind = 'change') AS detected_changes,
 count_if(measurement_kind = 'unknown') AS unknown_events,
 count_if(measurement_kind = 'cold') AS cold_events,
 count_if(measurement_kind = 'baseline') AS baseline_events,
 count_if(measurement_kind = 'change' AND first_detected_at + INTERVAL '24' HOUR <= a.as_of) AS mature_changes,
 count_if(measurement_kind = 'change' AND first_detected_at + INTERVAL '24' HOUR <= a.as_of
   AND status = 'applied' AND bronze_manifest IS NOT NULL AND bronze_manifest NOT LIKE 'raw:%'
   AND committed_at >= first_detected_at AND committed_at <= first_detected_at + INTERVAL '24' HOUR
   AND committed_at <= a.as_of) AS timely_changes,
 count_if(measurement_kind = 'change' AND status = 'failed') AS failed_changes,
 count_if(measurement_kind = 'change' AND first_detected_at + INTERVAL '24' HOUR > a.as_of
   AND status <> 'applied') AS immature_pending,
 count_if(measurement_kind = 'change' AND first_detected_at + INTERVAL '24' HOUR > a.as_of) AS immature_changes
 FROM cohort CROSS JOIN args a
),
coverage_totals AS (
 SELECT count(*) AS denominator, count_if(checked) AS checked_editions FROM competition_coverage
)
SELECT a.*, c.*, e.*,
 100.0 * c.checked_editions / NULLIF(c.denominator, 0) AS coverage_pct,
 100.0 * e.timely_changes / NULLIF(e.mature_changes, 0) AS detection_freshness_pct,
 a.as_of >= a.day_end + INTERVAL '24' HOUR AS cohort_finalized,
 'not_proven' AS site_freshness_status
FROM args a CROSS JOIN coverage_totals c CROSS JOIN event_totals e'''
    return sql, params


# Map of 23.09 for #1383 (task 17/#1402): history editions and requests per day.
HISTORY_EDITIONS_TARGET = 57
HISTORY_REQUESTS_TARGET = 16_500


def build_history_day_sql(day: date | str, *, as_of: datetime | str) -> tuple[str, tuple[Any, ...]]:
    """One read-only SELECT over the history ops journal for one Moscow day.

    Editions: scopes that reached ``complete`` that day.  Requests: distinct raw
    attempt envelopes (responses and transport errors) dated by the earliest
    attempt that references them.  Per-envelope request time lives only in the
    raw store, not in Trino; the attempt's ``source_observed_at`` (its latest
    request) is the closest time there, ``finished_at`` only when it is NULL.
    Streams: distinct history streams of batches claimed that day.
    """
    start, end = day_bounds(day)
    observed = utc(as_of)
    if observed < start:
        raise MeasurementError('as_of predates the report day')
    params = (start.replace(tzinfo=None), end.replace(tzinfo=None), observed.replace(tzinfo=None))
    sql = f'''WITH
args(day_start, day_end, as_of) AS (
 VALUES (CAST(? AS timestamp(6)), CAST(? AS timestamp(6)), CAST(? AS timestamp(6)))
),
done AS (
 SELECT count(*) AS editions FROM {SCOPE_TABLE} s CROSS JOIN args a
 WHERE s.status = 'complete' AND s.updated_at >= a.day_start AND s.updated_at < a.day_end
   AND s.updated_at <= a.as_of
),
envelopes AS (
 SELECT u.envelope_id, min(COALESCE(t.source_observed_at, t.finished_at)) AS first_at FROM {ATTEMPT_TABLE} t
 CROSS JOIN UNNEST(CAST(json_parse(t.raw_evidence_ids_json) AS array(varchar))) AS u(envelope_id)
 GROUP BY u.envelope_id
),
requests AS (
 SELECT count(*) AS requests FROM envelopes e CROSS JOIN args a
 WHERE e.first_at >= a.day_start AND e.first_at < a.day_end AND e.first_at <= a.as_of
),
streams AS (
 SELECT count(DISTINCT u.stream_id) AS streams FROM {BATCH_TABLE} b CROSS JOIN args a
 CROSS JOIN UNNEST(map_values(CAST(json_extract(b.record_json, '$.scope_stream_ids') AS map(varchar, varchar)))) AS u(stream_id)
 WHERE b.claimed_at >= a.day_start AND b.claimed_at < a.day_end AND b.claimed_at <= a.as_of
)
SELECT a.*, d.editions, r.requests, s.streams
FROM args a CROSS JOIN done d CROSS JOIN requests r CROSS JOIN streams s'''
    return sql, params


def format_history_summary(report: Mapping[str, Any]) -> str:
    target = f'{HISTORY_REQUESTS_TARGET / 1000:.1f}'.replace('.', ',')
    return (f'Transfermarkt история {report.get("report_day", "?")} МСК: изданий {report.get("editions", 0)} '
            f'(цель ≥ {HISTORY_EDITIONS_TARGET}), запросов {report.get("requests", 0)} '
            f'(ориентир {target} тыс.), потоков активных {report.get("streams", 0)}')


def format_daily_summary(report: Mapping[str, Any], *, dq_status: str = 'unknown',
                         site_sample: Mapping[str, Any] | None = None) -> str:
    """The morning summary/watchdog use this same pure formatter."""
    def percent(value):
        return 'не доказано' if value is None else f'{float(value):.2f}%'
    sample = site_sample or {}
    site = percent(sample.get('freshness_pct'))
    day = report.get('report_day')
    if day is None and report.get('day_start') is not None:
        stamp = report['day_start']
        stamp = datetime.fromisoformat(stamp) if isinstance(stamp, str) else stamp
        day = stamp.replace(tzinfo=stamp.tzinfo or timezone.utc).astimezone(MOSCOW).date().isoformat()
    day = day or '?'
    return (f'Transfermarkt {day} МСК: охват {report.get("checked_editions", 0)}/{report.get("denominator", 0)} '
            f'({percent(report.get("coverage_pct"))}); изменения ≤24ч от обнаружения '
            f'{report.get("timely_changes", 0)}/{report.get("mature_changes", 0)} '
            f'({percent(report.get("detection_freshness_pct"))}); незрелые {report.get("immature_changes", 0)}, '
            f'ошибки {report.get("failed_changes", 0)}, неизвестные {report.get("unknown_events", 0)}; '
            f'контроль сайта {site}, n={sample.get("mature_samples", 0)}; Bronze DQ {dq_status}')


def import_site_sample(samples: Iterable[Mapping[str, Any]], *, evidence_dir: Path,
                       day: date | str, as_of: datetime | str) -> dict[str, Any]:
    """Validate exported full-/plus/1 raw pairs and physical writer receipts.

    A reference observation proves when the sampled change was first seen in
    that pair, not its publication time. The previous observation is required
    within 24h, so a stale page cannot manufacture a fresh change. Missing commit
    proof remains a pending sample. Exported receipts/rows are evidence, not a
    boolean supplied by the importer. No collector or live connection is used.
    """
    from bs4 import BeautifulSoup
    from scrapers.transfermarkt.scraper import (_parse_squad_page, _parse_mv_history, _parse_transfers,
                                               _parse_coach_history, _source_row_semantic_error)
    from dags.utils.transfermarkt_current_write import _rows
    import pandas as pd

    start, end = day_bounds(day)
    observed = utc(as_of)
    root = Path(evidence_dir).resolve()

    def read(relative, *, raw=False):
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise MeasurementError('sample evidence must remain inside evidence_dir')
        return path.read_bytes() if raw else json.loads(path.read_text())

    def capture(proof, scope, kind):
        body = read(proof['body_path'], raw=True)
        receipt = read(proof['receipt_path'])
        if (receipt.get('status_code') != 200 or receipt.get('scope_id') != scope
            or receipt.get('source') != 'transfermarkt' or receipt.get('hash_algorithm') != 'sha256'
            or hashlib.sha256(body).hexdigest() != receipt.get('content_hash')
            or not receipt.get('capture_id') or receipt.get('decoded_bytes') != len(body)):
            raise MeasurementError('sample raw receipt/body identity differs')
        from urllib.parse import urlsplit
        parsed = urlsplit(receipt['url'])
        if (parsed.scheme != 'https' or parsed.hostname not in {'www.transfermarkt.com', 'www.transfermarkt.de'}
            or parsed.username or parsed.password):
            raise MeasurementError('sample needs Transfermarkt source page')
        page = body.decode('utf-8')
        if kind in {'roster_field', 'roster'}:
            headers = {h.get_text(' ', strip=True).lower() for h in BeautifulSoup(page, 'html.parser').select('table.items thead th')}
            if '/plus/1' not in parsed.path or 'contract' not in headers:
                raise MeasurementError('sample full squad lacks contract header')
        return page, receipt, utc(receipt['fetched_at'])

    results, seen = [], set()
    for sample in samples:
        kind = sample.get('kind', 'roster_field')
        field = sample.get('field', 'membership' if kind == 'roster' else None)
        scope, player, club = sample['scope_id'], str(sample.get('player_id', '')), str(sample.get('club_id', ''))
        selector = sample.get('row_key', {})
        key = (scope, player, club, kind, field, json.dumps(selector, sort_keys=True))
        if key in seen:
            raise MeasurementError('duplicate site sample')
        seen.add(key)
        previous_html, previous_raw, previous_at = capture(sample['previous'], scope, kind)
        current_html, current_raw, first_seen = capture(sample['current'], scope, kind)
        if (previous_raw['url'] != current_raw['url']
            or not timedelta(0) < first_seen - previous_at <= timedelta(hours=24)
            or not start <= first_seen < end or first_seen > observed):
            raise MeasurementError('sample reference identity/time interval differs')
        target, reason = None, ''
        if kind in {'roster_field', 'roster'}:
            if f'/verein/{club}/' not in current_raw['url']:
                raise MeasurementError('sample roster URL club differs')
            before = {str(r['player_id']): r for r in _parse_squad_page(previous_html, club)}
            after = {str(r['player_id']): r for r in _parse_squad_page(current_html, club)}
            def source_player_ids(body):
                return {match.group(1) for anchor in BeautifulSoup(body, 'html.parser').select('table.items a[href]')
                        if (match := re.search(r'/spieler/(\d+)', anchor['href']))}
            if set(before) != source_player_ids(previous_html) or set(after) != source_player_ids(current_html):
                reason = 'reference_roster_parser_completeness_unproven'
            elif kind == 'roster_field':
                if field not in {'market_value_eur', 'contract_until'}:
                    reason = 'unsupported_roster_field'
                elif player not in before or player not in after:
                    raise MeasurementError('sample player absent from reference page')
                else:
                    old, new = before[player].get(field), after[player].get(field)
                    if old == new or old is None or new is None:
                        raise MeasurementError('sample lacks a known reference field change')
                    target = {'player_id': player, 'club_id': club, field: new}
            elif player not in before and player in after:
                target = {'player_id': player, 'club_id': club}
            else:
                reason = 'roster_change_not_proven_as_new_membership'
        elif kind in {'transfer', 'valuation', 'coach'}:
            natural = {'transfer': {'transfer_id'}, 'valuation': {'mv_date'},
                       'coach': {'coach_id', 'appointed_date', 'role'}}[kind]
            if set(selector) != natural:
                reason = 'missing_or_ambiguous_natural_key'
            else:
                if kind == 'coach':
                    if '/mitarbeiterhistorie/verein/' + club not in current_raw['url']:
                        raise MeasurementError('sample coach history URL club differs')
                    def parser(body):
                        parsed = _parse_coach_history(body, club)
                        page = BeautifulSoup(body, 'html.parser')
                        table = page.select_one('table.items')
                        headers = {h.get_text(' ', strip=True).lower() for h in table.select('thead th')} if table else set()
                        source_rows = [row for row in (table.find('tbody') or table).find_all('tr', recursive=False)
                                       if row.select('a[href*="/trainer/"]')] if table else []
                        if not {'appointed', 'end of time in post'} <= headers or len(parsed) != len(source_rows):
                            raise MeasurementError('sample coach history layout/completeness unproven')
                        return parsed
                else:
                    route = '/ceapi/transferHistory/list/' if kind == 'transfer' else '/ceapi/marketValueDevelopment/graph/'
                    if not current_raw['url'].endswith(route + player):
                        raise MeasurementError('sample career URL player differs')
                    payload_key = 'transfers' if kind == 'transfer' else 'list'
                    def parser(body):
                        payload = json.loads(body)
                        if not isinstance(payload, dict) or not isinstance(payload.get(payload_key), list):
                            raise MeasurementError('sample career payload list missing')
                        label = 'transfer_events' if kind == 'transfer' else 'market_value_points'
                        if any(not isinstance(row, dict) or _source_row_semantic_error(label, row)
                               for row in payload[payload_key]):
                            raise MeasurementError('sample career row semantics unproven')
                        parsed = (_parse_transfers if kind == 'transfer' else _parse_mv_history)(payload, player)
                        if len(parsed) != len(payload[payload_key]):
                            raise MeasurementError('sample career parser completeness unproven')
                        return parsed
                def selected(body):
                    try:
                        return [row for row in parser(body) if all(str(row.get(key)) == str(value) for key, value in selector.items())]
                    except (MeasurementError, ValueError, TypeError):
                        return None
                before, after = selected(previous_html), selected(current_html)
                fields = {'transfer': ('transfer_id', 'player_id', 'transfer_date', 'event_season', 'from_club_id', 'to_club_id', 'is_upcoming'),
                          'valuation': ('player_id', 'mv_date', 'value_eur'),
                          'coach': ('coach_id', 'club_id', 'appointed_date', 'left_date', 'role')}[kind]
                if before is None or after is None:
                    reason = 'reference_parser_completeness_unproven'
                elif len(after) != 1 or len(before) > 1:
                    reason = 'ambiguous_or_missing_reference_row'
                elif before and all(before[0].get(key) == after[0].get(key) for key in fields):
                    reason = 'reference_change_not_proven'
                elif kind == 'transfer' and (after[0].get('transfer_date') is None or after[0].get('is_upcoming')
                                             or after[0].get('from_club_id') is None or after[0].get('to_club_id') is None):
                    reason = 'unknown_or_upcoming_transfer'
                elif kind == 'valuation' and after[0].get('value_eur') is None:
                    reason = 'unknown_valuation'
                else:
                    target = {key: after[0].get(key) for key in fields}
        else:
            reason = 'unsupported_change_kind'
        committed_at = None
        if target is not None and sample.get('bronze_receipt_path'):
            receipt = read(sample['bronze_receipt_path'])
            digest = hashlib.sha256(json.dumps({'cycle_id': receipt['cycle_id'], 'outputs': receipt['outputs'],
                                               'manifests': receipt['manifests']}, sort_keys=True).encode()).hexdigest()
            if (receipt.get('verified') is not True or receipt.get('bronze_manifest') != digest
                or not receipt['manifests'] or any(m.get('status') != 'success' or not m.get('rows')
                                                 for m in receipt['manifests'])):
                raise MeasurementError('sample Bronze business receipt differs')
            readback = read(sample['readback_path'])
            output = receipt['outputs'][sample['output_key']]
            rows, columns = readback['rows'], readback['columns']
            frame = pd.DataFrame(rows, columns=columns)
            physical = hashlib.sha256(json.dumps(_rows(frame), sort_keys=True).encode()).hexdigest()
            if (not rows or len(rows) != output.get('rows') or physical != output.get('physical_hash')
                or readback.get('table') != output.get('table') or readback.get('batch_id') != output.get('batch_id')
                or '_batch_id' not in frame or set(frame['_batch_id'].astype(str)) != {str(output['batch_id'])}
                or not str(output.get('table', '')).startswith('iceberg.bronze.transfermarkt_')):
                raise MeasurementError('sample physical read-back differs')
            manifest_rows = [row for manifest in receipt['manifests'] for row in manifest['rows']]
            if not any(row.get('status') == 'success'
                       and row.get('native_table') == output['table'].removeprefix('iceberg.bronze.')
                       and row.get('native_batch_id') == output['batch_id']
                       and row.get('native_rows') == len(rows)
                       and isinstance(row.get('native_hash'), str) and len(row['native_hash']) == 64
                       for row in manifest_rows):
                raise MeasurementError('sample native commit manifest/read-back identity differs')
            expected_table = {'roster_field': 'transfermarkt_player_attribute_observations',
                              'roster': 'transfermarkt_squad_memberships',
                              'valuation': 'transfermarkt_market_value_points',
                              'transfer': 'transfermarkt_transfer_events',
                              'coach': 'transfermarkt_coach_stints'}[kind]
            if output['table'] != 'iceberg.bronze.' + expected_table:
                raise MeasurementError('sample kind/native output table differs')
            candidates = [row for row in rows if row.get('scope_id') == scope
                          and row.get('source_url') == current_raw['url']
                          and row.get('source_body_hash') == current_raw['content_hash']
                          and utc(row.get('fetched_at')) == first_seen]
            if not candidates or not any(all(str(row.get(key)) == str(value) for key, value in target.items()) for row in candidates):
                raise MeasurementError('sample changed field/lineage absent from physical read-back')
            committed_at = utc(receipt['committed_at'])
            if committed_at < first_seen or committed_at > observed:
                raise MeasurementError('sample commit clock differs')
        mature = first_seen + timedelta(hours=24) <= observed
        results.append({'scope_id': scope, 'player_id': player, 'club_id': club, 'field': field,
                        'kind': kind, 'row_key': selector, 'supported': target is not None,
                        'unproven_reason': reason or ('' if committed_at is not None else 'missing_commit_readback'),
                        'previous_raw_capture_id': previous_raw['capture_id'],
                        'raw_capture_id': current_raw['capture_id'], 'site_first_seen_at': first_seen.isoformat(),
                        'previous_observed_at': previous_at.isoformat(),
                        'committed_at': committed_at.isoformat() if committed_at else None,
                        'mature': mature,
                        # Appearance is in (previous_at, first_seen]; this proves
                        # <=24h even for the earliest possible site change.
                        'timely': committed_at is not None and committed_at <= previous_at + timedelta(hours=24),
                        'observed_timely': committed_at is not None and committed_at <= first_seen + timedelta(hours=24),
                        'appearance_interval_unproven': committed_at is not None
                            and previous_at + timedelta(hours=24) < committed_at <= first_seen + timedelta(hours=24)})
    mature_count = sum(item['mature'] for item in results)
    timely = sum(item['mature'] and item['timely'] for item in results)
    return {'measurement_version': VERSION, 'status': 'measured' if mature_count else 'not_proven',
            'sample_count': len(results), 'mature_samples': mature_count,
            'timely_samples': timely, 'immature_samples': len(results) - mature_count,
            'appearance_interval_unproven': sum(item['mature'] and item['appearance_interval_unproven'] for item in results),
            'unsupported_samples': sum(not item['supported'] for item in results),
            'freshness_pct': 100.0 * timely / mature_count if mature_count else None,
            'clock_definition': 'first changed reference raw in a <=24h observation pair; publication time unknown',
            'freshness_definition': 'conservative proven fraction: commit <= previous observation +24h',
            'samples': results}
