"""Strict offline event intake. Valid artifacts never authorize live acceptance.

Inventories and original identities belong to the pinned manifest, not event
summary flags. Coverage rows reconcile independently enumerated events.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

from .history_portion import MAX_HISTORY_ATTEMPTS
from .streams import TransfermarktStreams

from .writer import CAREER_TABLES as TABLES
KINDS = {
    'coverage': 'start end attempts provider_bytes completions rss_process_bytes cgroup_memory_bytes memory_limit_bytes swap_bytes trino_memory_bytes current_waiting writer_observations table_checks',
    'attempt': 'at work_id outcome provider_bytes portion_id',
    'query': 'at query_id peak_memory_bytes',
    'completion': 'at work_id mode',
    'portion': 'start end',
    'task': 'start end portion_id',
    'lease': 'actor grant_id acquired_at expires_at grant_sha256 closed_at reconciliation_actor',
    'current': 'enqueued_at started_at history_wait_seconds',
    'batch': 'at sequence checkpoint_before checkpoint_after resumed_from expected_remainder observed_remainder expected_rows observed_rows duplicates missing raw_times',
    'writer': 'at writer_id participant lock_file_id table before_snapshot after_snapshot expected_keys observed_keys expected_clocks observed_clocks',
    'scenario': 'name payload',
}
ROLES = {'baseline', 'canary1', 'canary2', 'writer', 'history'}


class InvalidEvidence(ValueError):
    pass


def need(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def exact(value, keys, context):
    need(isinstance(value, dict) and set(value) == set(keys.split()), f'{context}: missing/unknown fields')


def number(value, context, *, integer=False, positive=False):
    need(type(value) in ((int,) if integer else (int, float)) and math.isfinite(value)
         and (value > 0 if positive else value >= 0), f'{context}: invalid number')
    return value


def stamp(value):
    need(isinstance(value, str), 'timestamp must be UTC text')
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidEvidence('invalid timestamp') from exc
    need(result.tzinfo is not None and result.utcoffset().total_seconds() == 0,
         'timestamp must include UTC offset')
    return result


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def sha(value):
    need(isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value), 'invalid SHA256')
    return value


def ids(value, context, *, nonempty=False):
    need(isinstance(value, list) and all(isinstance(x, str) and x for x in value), f'{context}: invalid IDs')
    need(len(value) == len(set(value)) and (bool(value) or not nonempty), f'{context}: duplicate/empty IDs')
    return set(value)


def _pairs(items):
    result = {}
    for key, value in items:
        need(key not in result, f'duplicate JSON field: {key}')
        result[key] = value
    return result


def read_jsonl(path):
    rows = []
    with Path(path).open() as source:
        for line_number, line in enumerate(source, 1):
            try:
                need(bool(line.strip()), 'blank JSONL line')
                rows.append(json.loads(line, object_pairs_hook=_pairs,
                                       parse_constant=lambda v: need(False, f'nonfinite JSON value: {v}')))
            except (ValueError, TypeError) as exc:
                raise InvalidEvidence(f'line {line_number}: {exc}') from exc
    need(bool(rows), 'empty JSONL')
    return rows[0], rows[1:]


def _interval(row, window):
    start, end = stamp(row['start']), stamp(row['end'])
    need(window[0] <= start < end <= window[1], 'interval outside window')
    return start, end


def _scenario(row):
    p, name = row['payload'], row['name']
    if name == 'transport':
        exact(p, 'stream_id actor failures paused_until alert_at source_before source_after terminal_scopes recovery_attempts recovery_raw_at recovery_raw_sha256', name)
        need(isinstance(p['failures'], list) and len(p['failures']) == 3, 'transport requires exactly three failure events')
        for f in p['failures']:
            exact(f, 'scope at classification attempt_id', 'transport failure')
            need(f['classification'] == 'transport' and isinstance(f['scope'], str) and f['scope'], 'platform/source failure is not transport')
        times = [stamp(f['at']) for f in p['failures']]
        need(len({f['scope'] for f in p['failures']}) == 3 and (max(times) - min(times)).total_seconds() <= 600,
             'transport requires three distinct scopes within 10 minutes')
        need((stamp(p['paused_until']) - max(times)).total_seconds() == 900 and stamp(p['alert_at']) >= max(times), 'missing exact15min pause/alert')
        need(type(p['source_before']) is int and p['source_before'] >= 0 and p['source_after'] == p['source_before']
             and p['terminal_scopes'] == [], 'transport spent source counter or terminated scopes')
        ids(p['recovery_attempts'], 'recovery', nonempty=True)
        need(len(p['recovery_attempts']) == 1, 'recovery must have exactly one paid attempt')
        need(stamp(p['recovery_raw_at']) >= stamp(p['paused_until']), 'stale raw cannot heal circuit')
        sha(p['recovery_raw_sha256'])
    elif name == 'season_close':
        exact(p, 'registry_before registry_after old_scope expected_players observed_players career_players coach_players completed_at history_claim_at', name)
        for r in ('registry_before', 'registry_after'):
            need(isinstance(p[r], list) and p[r], 'missing registry pair')
            for entry in p[r]:
                exact(entry, 'scope competition season', 'registry entry')
        old = [x for x in p['registry_before'] if x['scope'] == p['old_scope']]
        need(len(old) == 1, 'old scope absent/ambiguous')
        old = old[0]
        successors = [x for x in p['registry_after'] if x['competition'] == old['competition'] and x['season'] != old['season']]
        need(len(successors) == 1, 'registry disappearance is not season transition')
        need(type(old['season']) is int and type(successors[0]['season']) is int
             and successors[0]['season'] == old['season'] + 1, 'invalid season progression')
        expected = ids(p['expected_players'], 'old scope denominator', nonempty=True)
        need(all(ids(p[x], x, nonempty=True) == expected for x in ('observed_players', 'career_players', 'coach_players')), 'old scope incomplete')
        need(stamp(p['completed_at']) < stamp(p['history_claim_at']), 'history claim precedes complete old scope')
    elif name in {'crash_replay', 'cache_replay', 'empty', 'multiunit', 'compaction'}:
        exact(p, 'before_keys after_keys original_clocks observed_clocks files bytes protected_before protected_after'
              + (' wall_seconds' if name == 'compaction' else ''), name)
        before, after = ids(p['before_keys'], name), ids(p['after_keys'], name)
        need(before == after and p['original_clocks'] == p['observed_clocks'], f'{name}: loss/clock drift')
        need(isinstance(p['original_clocks'], dict) and set(p['original_clocks']) == before, f'{name}: missing original clocks')
        for clock in p['original_clocks'].values():
            stamp(clock)
        if name == 'compaction':
            need(isinstance(p['files'], dict) and isinstance(p['bytes'], dict)
                 and set(p['files']) == set(p['bytes']) == TABLES, 'compaction exact4table bounds missing')
            for table in TABLES:
                number(p['files'][table], 'files/table', integer=True)
                number(p['bytes'][table], 'bytes/table', integer=True)
                need(p['files'][table] <= 64 and p['bytes'][table] <= 536870912, 'compaction bounds exceeded')
            number(p['wall_seconds'], 'compaction wall time')
            need(sum(p['bytes'].values()) <= 2147483648 and p['wall_seconds'] <= 600, 'compaction run bounds exceeded')
        else:
            number(p['files'], 'files', integer=True)
            number(p['bytes'], 'bytes', integer=True)
        need(ids(p['protected_before'], 'protected anchors', nonempty=True) == ids(p['protected_after'], 'protected anchors', nonempty=True), 'protected anchors changed')
        if name in {'crash_replay', 'cache_replay', 'multiunit'}:
            need(bool(before), f'{name}: empty denominator')
        if name == 'empty':
            need(not before, 'empty scenario contains keys')
    else:
        raise InvalidEvidence(f'unknown scenario: {name}')


def validate(manifest, events, *, expected_manifest_sha256=None, peak_records=None):
    """Return explicit artifact validity; never return runtime PASS or T0."""
    result = {'schema': 1, 'artifact_status': 'INVALID', 'runtime_status': 'UNKNOWN',
              'actual_acceptance_start': None, 'runtime_eligible': False,
              'blockers': ['Offline intake cannot prove genuine authority, live locks, source clocks or qualification.'],
              'issues': [], 'windows': {}, 'producer_status': 'UNKNOWN',
              'criteria': {k: {'artifact_status': 'UNKNOWN', 'reason': 'No corresponding window producer supplied.'}
                           for k in ('canary', 'writer', 'history', 'crash_replay', 'cache_replay', 'empty', 'multiunit', 'compaction', 'transport', 'season_close')}}
    try:
        exact(manifest, 'schema provenance revision workload workload_sha256 windows writer_ids lock_file_id writer_manifest lease_manifest sampling_seconds producer'
              + (' peak_manifest' if isinstance(manifest, dict) and 'peak_manifest' in manifest else '')
              + (' batch_manifest' if isinstance(manifest, dict) and 'batch_manifest' in manifest else ''), 'manifest')
        need(type(manifest['schema']) is int and manifest['schema'] == 1, 'unsupported schema')
        need(manifest['provenance'] in {'synthetic_fixture', 'retained_runtime'}, 'unknown provenance')
        result['provenance'] = manifest['provenance']
        number(manifest['sampling_seconds'], 'sampling interval', positive=True)
        exact(manifest['producer'], 'id sequence_start sequence_end', 'producer')
        need(isinstance(manifest['producer']['id'], str) and manifest['producer']['id'], 'missing producer ID')
        need(type(manifest['producer']['sequence_start']) is int and manifest['producer']['sequence_start'] == 1
             and type(manifest['producer']['sequence_end']) is int and manifest['producer']['sequence_end'] == len(events) and events, 'incomplete producer sequence')
        if expected_manifest_sha256 is not None:
            need(sha(expected_manifest_sha256) == digest(manifest), 'external pinned manifest digest differs')
            result['producer_status'] = 'PINNED_OFFLINE_ARTIFACT'
        else:
            result['blockers'].append('No external pinned manifest digest: producer completeness is UNKNOWN.')
        need(isinstance(manifest['revision'], str) and len(manifest['revision']) == 40 and all(c in '0123456789abcdef' for c in manifest['revision']), 'missing exact revision')
        need(isinstance(manifest['workload'], list) and manifest['workload'], 'empty workload denominator')
        work = {}
        for item in manifest['workload']:
            exact(item, 'id actor lane policy scope url original_capture_at', 'workload')
            need(all(isinstance(item[k], str) and item[k] for k in ('id', 'actor', 'policy', 'scope', 'url')), 'missing workload identity')
            need(item['lane'] in {'current', 'history'}, 'unknown workload lane')
            stamp(item['original_capture_at'])
            need(item['id'] not in work, 'duplicate workload ID')
            work[item['id']] = item
        need(sha(manifest['workload_sha256']) == digest(manifest['workload']), 'workload digest differs')
        need(isinstance(manifest['windows'], list) and 1 <= len(manifest['windows']) <= 5, 'need at least one known window')
        windows = {}
        for w in manifest['windows']:
            exact(w, 'id role start end total_streams inventory attempt_owners', 'window')
            need(w['role'] in ROLES and w['id'] not in windows and isinstance(w['id'], str), 'unknown/duplicate window')
            need((stamp(w['end']) - stamp(w['start'])).total_seconds() == 86400, 'full24h required')
            need(type(w['total_streams']) is int and w['total_streams'] == (1 if w['role'] == 'baseline' else 2), 'must compare1vs2TOTAL')
            TransfermarktStreams(history_streams=w['total_streams'] - 1)
            exact(w['inventory'], ' '.join(KINDS), 'inventory')
            for kind, values in w['inventory'].items():
                ids(values, kind, nonempty=kind == 'coverage')
            windows[w['id']] = w
        need(len({w['role'] for w in windows.values()}) == len(windows), 'duplicate window roles')
        comparisons = sorted((w for w in windows.values() if w['role'] in {'baseline', 'canary1', 'canary2'}), key=lambda w: w['role'])
        need(all(stamp(a['end']) <= stamp(b['start']) for a, b in zip(comparisons, comparisons[1:])), 'comparison windows overlap/order differs')
        grouped = {key: {kind: [] for kind in KINDS} for key in windows}
        seen = set()
        for sequence, row in enumerate(events, 1):
            need(isinstance(row, dict) and row.get('kind') in KINDS and row.get('window') in windows, 'unknown event/window')
            exact(row, 'id kind window producer_sequence ' + KINDS[row['kind']]
                  + (' stream_id actor' if row['kind'] == 'attempt' else '')
                  + (' capture_units' if row['kind'] == 'batch' and 'capture_units' in row else '')
                  + (' process_identity cgroup_identity' if row['kind'] == 'coverage' and any(k in row for k in ('process_identity', 'cgroup_identity')) else ''), row['kind'])
            need(type(row['producer_sequence']) is int and row['producer_sequence'] == sequence, 'producer sequence gap/reordering')
            identity = (row['window'], row['kind'], row['id'])
            need(isinstance(row['id'], str) and row['id'] and identity not in seen, 'duplicate/invalid event ID')
            seen.add(identity)
            grouped[row['window']][row['kind']].append(row)
        metrics = {}
        for key, w in windows.items():
            criterion = w['role'] if w['role'] in {'writer', 'history'} else 'canary'
            try:
                metrics[w['role']] = _window(manifest, w, grouped[key], work)
                result['windows'][key] = metrics[w['role']]
                if criterion != 'canary':
                    qualified = metrics[w['role']].get('history_identity_qualification', 'VALID')
                    result['criteria'][criterion] = {'artifact_status': qualified, 'denominator': metrics[w['role']],
                                                      'producer_status': result['producer_status']}
            except (InvalidEvidence, KeyError, TypeError, ValueError, OverflowError) as exc:
                result['issues'].append(f'{key}: {exc}')
                result['criteria'][criterion] = {'artifact_status': 'INVALID', 'reason': str(exc)}
        scenario_names = set(result['criteria']) - {'canary', 'writer', 'history'}
        for key, w in windows.items():
            rows = grouped[key]
            counts = Counter(r['name'] for r in rows['scenario'])
            for row in rows['scenario']:
                name = row['name']
                try:
                    need(name in scenario_names, 'unknown scenario criterion')
                    need(counts[name] == 1, 'duplicate scenario criterion')
                    need(w['role'] == ('history' if name in {'transport', 'season_close'} else 'writer'), 'scenario belongs to another criterion window')
                    _scenario_event(row, rows, work, w)
                    result['criteria'][name] = {'artifact_status': 'VALID', 'producer_status': result['producer_status'],
                                                'denominator': {'scenario_events': 1, 'window': key}}
                except (InvalidEvidence, KeyError, TypeError, ValueError, OverflowError) as exc:
                    result['issues'].append(f'{key}/{name}: {exc}')
                    if name in scenario_names:
                        result['criteria'][name] = {'artifact_status': 'INVALID', 'reason': str(exc)}
        if all(k in metrics for k in ('baseline', 'canary1', 'canary2')):
            try:
                baseline = metrics['baseline']
                need(baseline['paid_completions'] > 0, 'zero baseline throughput')
                for name in ('canary1', 'canary2'):
                    ratio = metrics[name]['paid_completions'] / baseline['paid_completions']
                    metrics[name]['throughput_ratio'] = ratio
                    need(ratio >= 1.8, 'throughput below1.8x')
                result['criteria']['canary'] = {'artifact_status': 'VALID', 'producer_status': result['producer_status'],
                                                'denominator': {k: metrics[k] for k in ('baseline', 'canary1', 'canary2')}}
            except InvalidEvidence as exc:
                result['issues'].append(str(exc))
                result['criteria']['canary'] = {'artifact_status': 'INVALID', 'reason': str(exc)}
        if peak_records is not None:
            need(isinstance(peak_records, list), 'peak records must be a list')
            declared = manifest.get('peak_manifest', {})
            need(isinstance(declared, dict) and set(declared).issubset(windows), 'unknown peak window manifest')
            seen_peak_windows = set()
            for record in peak_records:
                need(isinstance(record, dict) and record.get('window') in declared, 'peak record has no pinned identity manifest')
                key = record['window']
                need(key not in seen_peak_windows, 'duplicate whole-window peak record')
                seen_peak_windows.add(key)
                peak = retained_peaks(windows[key], declared[key], record,
                                      manifest_pinned=result['producer_status'] == 'PINNED_OFFLINE_ARTIFACT')
                if peak['qualification'] == 'VALID':
                    samples = grouped[key]['coverage']
                    linked = key in result['windows'] and all(s.get('process_identity') == record['process_identity']
                              and s.get('cgroup_identity') == record['cgroup_identity'] for s in samples)
                    if not linked:
                        peak = {'qualification': 'UNKNOWN', 'reason': 'Whole-window coverage is not linked to the retained process/cgroup owners.'}
                    elif (peak['process_rss_peak_bytes'] < max(s['rss_process_bytes'] for s in samples)
                          or peak['cgroup_memory_peak_bytes'] < max(s['cgroup_memory_bytes'] for s in samples)):
                        peak = {'qualification': 'INVALID', 'reason': 'Retained process/cgroup peak below corresponding point sample.'}
                if key in result['windows']:
                    result['windows'][key]['resource_peak_qualification'] = peak['qualification']
                    result['windows'][key]['retained_peak'] = peak
                result['criteria']['resource_peak_' + key] = {'artifact_status': peak['qualification'], 'denominator': peak}
                if peak['qualification'] == 'INVALID':
                    result['issues'].append(f'{key}/resource_peak: {peak["reason"]}')
        result['artifact_status'] = 'INVALID' if result['issues'] else 'VALID'
    except (InvalidEvidence, KeyError, TypeError, ValueError, OverflowError) as exc:
        result['issues'].append(str(exc))
    return result


def _validated_attempts(w, rows, work):
    """Shared attempt proof required by both interval and transport criteria."""
    observed = [a['id'] for a in rows['attempt']]
    need(len(observed) == len(set(observed)) and set(observed) == ids(w['inventory']['attempt'], 'attempt inventory'), 'attempt inventory differs')
    need(isinstance(w['attempt_owners'], dict) and set(w['attempt_owners']) == set(w['inventory']['attempt']), 'missing pinned attempt owner denominator')
    for a in rows['attempt']:
        owner = w['attempt_owners'][a['id']]
        exact(owner, 'work_id stream_id actor', 'attempt owner')
        need(all(a[k] == owner[k] for k in owner) and a['work_id'] in work
             and a['actor'] == work[a['work_id']]['actor']
             and a['stream_id'] in TransfermarktStreams(history_streams=w['total_streams'] - 1).stream_ids, 'attempt stream/actor identity differs')
        need(a['work_id'] in work and a['outcome'] in {'success', 'timeout', 'error', 'transport'}, 'unknown attempt/work/outcome')
        number(a['provider_bytes'], 'attempt bytes', integer=True)
        need(a['portion_id'] is None or a['portion_id'] in w['inventory']['portion'], 'attempt has unknown portion')
        need(stamp(w['start']) <= stamp(a['at']) < stamp(w['end']), 'attempt outside window')
    return rows['attempt']


def _window(manifest, w, rows, work):
    interval = (stamp(w['start']), stamp(w['end']))
    for kind in KINDS:
        need({r['id'] for r in rows[kind]} == ids(w['inventory'][kind], kind), f'{w["id"]}/{kind}: inventory differs')
        for r in rows[kind]:
            if 'at' in r:
                need(interval[0] <= stamp(r['at']) < interval[1], f'{kind}: outside window')
    _validated_attempts(w, rows, work)
    for c in rows['completion']:
        need(c['work_id'] in work and c['mode'] in {'paid', 'cache'}, 'unknown completion')
        if c['mode'] == 'paid':
            need(any(a['work_id'] == c['work_id'] and a['outcome'] == 'success' and stamp(a['at']) <= stamp(c['at']) for a in rows['attempt']), 'paid completion lacks successful attempt')
    need(len({r['work_id'] for r in rows['completion']}) == len(rows['completion']), 'duplicate completed work')
    for q in rows['query']:
        need(isinstance(q['query_id'], str) and q['query_id'], 'missing attributable query ID')
        number(q['peak_memory_bytes'], 'Trino query memory', integer=True)
    need(len({q['query_id'] for q in rows['query']}) == len(rows['query']), 'duplicate query identity')
    cursor = interval[0]
    for s in sorted(rows['coverage'], key=lambda r: stamp(r['start'])):
        start, end = _interval(s, interval)
        need(start == cursor and (end - start).total_seconds() <= manifest['sampling_seconds'], 'resource coverage gap/overlap or sample exceeds configured interval')
        cursor = end
        for name in KINDS['coverage'].split()[2:-2]:
            number(s[name], name, integer=True, positive=name in {'memory_limit_bytes', 'rss_process_bytes', 'cgroup_memory_bytes'})
        attempts = [a for a in rows['attempt'] if start <= stamp(a['at']) < end]
        complete = [c for c in rows['completion'] if start <= stamp(c['at']) < end]
        queries = [q for q in rows['query'] if start <= stamp(q['at']) < end]
        need(s['attempts'] == len(attempts) and s['provider_bytes'] == sum(a['provider_bytes'] for a in attempts)
             and s['completions'] == len(complete), 'coverage counters differ from complete events')
        need(s['trino_memory_bytes'] == max((q['peak_memory_bytes'] for q in queries), default=0), 'attributable Trino samples missing/differ')
        need(s['rss_process_bytes'] <= s['cgroup_memory_bytes'] <= s['memory_limit_bytes'] and s['swap_bytes'] == 0, 'memory limit/swap violation')
        need(s['current_waiting'] == 0, 'current waiting is nonzero')
        for identity in ('process_identity', 'cgroup_identity'):
            need(s.get(identity) is None or isinstance(s[identity], str) and s[identity], 'invalid coverage owner identity')
        need(isinstance(s['writer_observations'], dict) and isinstance(s['table_checks'], dict), 'invalid sample observations')
        if w['role'] != 'writer':
            need(not s['writer_observations'] and not s['table_checks'], 'writer observations belong to writer window')
    need(cursor == interval[1], 'resource coverage ends early')
    if w['role'] in {'baseline', 'canary1', 'canary2'}:
        need(rows['attempt'] and rows['query'], 'missing attempt/query denominator')
        timeout_ratio = sum(a['outcome'] == 'timeout' for a in rows['attempt']) / len(rows['attempt'])
        need(timeout_ratio < .02, 'timeouts must be strictly<2% of ALL attempts')
    else:
        timeout_ratio = None
    if w['role'] == 'writer':
        _writers(manifest, rows)
    history_identity = _history(manifest, w, rows, interval, work) if w['role'] == 'history' else None
    return {'history_identity_qualification': history_identity or 'VALID', 'all_attempts': len(rows['attempt']), 'provider_bytes': sum(a['provider_bytes'] for a in rows['attempt']),
            'attempts_by_lane': dict(Counter(work[a['work_id']]['lane'] for a in rows['attempt'])),
            'provider_bytes_by_lane': {lane: sum(a['provider_bytes'] for a in rows['attempt'] if work[a['work_id']]['lane'] == lane) for lane in ('current', 'history')},
            'completed_by_lane': dict(Counter(work[c['work_id']]['lane'] for c in rows['completion'] if c['mode'] == 'paid')),
            'timeouts_ratio': timeout_ratio, 'paid_completions': sum(c['mode'] == 'paid' for c in rows['completion']),
            'cache_completions': sum(c['mode'] == 'cache' for c in rows['completion']),
            'workload_denominator': len(work), 'unfinished_work': sorted(set(work) - {c['work_id'] for c in rows['completion']}),
            'resource_peak_qualification': 'UNKNOWN',
            'resource_peak_limitation': 'Point samples do not establish interval peaks; retained VmHWM/memory.peak are absent.',
            'samples': len(rows['coverage']), 'portion_count': len(rows['portion']), 'task_count': len(rows['task']),
            'lease_count': len(rows['lease']), 'unclosed_leases': sum(r['closed_at'] is None for r in rows['lease']),
            'batch_count': len(rows['batch']), 'writer_operations': len(rows['writer']),
            'scenario_names': [r['name'] for r in rows['scenario']], 'rss_process_sampled_peak_bytes': max(s['rss_process_bytes'] for s in rows['coverage']),
            'cgroup_memory_sampled_peak_bytes': max(s['cgroup_memory_bytes'] for s in rows['coverage']),
            'trino_attributable_sampled_peak_bytes': max(s['trino_memory_bytes'] for s in rows['coverage'])}


def _scenario_event(row, rows, work, window):
    _scenario(row)
    if row['name'] == 'transport':
        attempts = _validated_attempts(window, rows, work)
        by_id = {a['id']: a for a in attempts}
        p = row['payload']
        for failure in p['failures']:
            a = by_id.get(failure['attempt_id'])
            need(a is not None and a['outcome'] == 'transport' and a['at'] == failure['at']
                 and work[a['work_id']]['scope'] == failure['scope']
                 and a['stream_id'] == p['stream_id'] and a['actor'] == p['actor'], 'transport failure lacks exact attempt evidence')
        recovery = by_id.get(p['recovery_attempts'][0])
        need(recovery is not None and recovery['outcome'] == 'success'
             and recovery['stream_id'] == p['stream_id'] and recovery['actor'] == p['actor']
             and stamp(p['paused_until']) <= stamp(recovery['at']) <= stamp(p['recovery_raw_at']), 'recovery lacks exact single successful paid attempt')
        trip = max(stamp(f['at']) for f in p['failures'])
        paused_until, healed = stamp(p['paused_until']), stamp(p['recovery_raw_at'])
        failures = {f['attempt_id'] for f in p['failures']}
        affected = [a for a in attempts if a['stream_id'] == p['stream_id']]
        need(not any(trip <= stamp(a['at']) < paused_until and a['id'] not in failures for a in affected), 'paid attempt during affected stream pause')
        probes = [a['id'] for a in affected if paused_until <= stamp(a['at']) <= healed]
        need(probes == p['recovery_attempts'], 'actual affected stream probes differ before verified heal')


def _writers(manifest, rows):
    writers = ids(manifest['writer_ids'], 'writer IDs', nonempty=True)
    need(len(writers) == 2, 'exactly2writers required')
    need(isinstance(manifest['lock_file_id'], str) and len(manifest['lock_file_id'].split(':')) == 2
         and all(x.isdigit() for x in manifest['lock_file_id'].split(':')), 'missing device:inode identity')
    need(isinstance(manifest['writer_manifest'], dict) and set(manifest['writer_manifest']) == TABLES, 'exact4career table denominator required')
    need({r['participant'] for r in rows['writer']} == {'writer', 'maintenance'}, 'missing writer/maintenance lock participant')
    need({r['writer_id'] for r in rows['writer'] if r['participant'] == 'writer'} == writers, 'writer IDs differ')
    for table, expected in manifest['writer_manifest'].items():
        exact(expected, 'keys original_clocks', 'writer table manifest')
        keys = ids(expected['keys'], 'natural keys', nonempty=True)
        need(isinstance(expected['original_clocks'], dict) and set(expected['original_clocks']) == keys, 'missing original clock manifest')
        for clock in expected['original_clocks'].values():
            stamp(clock)
        checks = [r for r in rows['writer'] if r['table'] == table and r['participant'] == 'writer']
        need({r['writer_id'] for r in checks} == writers, 'same-table parallel commit denominator incomplete')
    for r in rows['writer']:
        need(r['table'] in TABLES and r['lock_file_id'] == manifest['lock_file_id'], 'shared lock/table identity differs')
        need(r['participant'] != 'writer' or r['writer_id'] in writers, 'unknown writer')
        expected = manifest['writer_manifest'][r['table']]
        need(ids(r['expected_keys'], 'expected keys') == set(expected['keys']) == ids(r['observed_keys'], 'observed keys'), 'natural-key loss/duplicate')
        need(r['expected_clocks'] == expected['original_clocks'] == r['observed_clocks'], 'original source clock drift')
        sha(r['before_snapshot']); sha(r['after_snapshot'])
        need(r['before_snapshot'] != r['after_snapshot'] or r['participant'] == 'maintenance', 'commit snapshot did not advance')
    for sample in rows['coverage']:
        observations = sample['writer_observations']
        need(isinstance(observations, dict) and set(observations) == writers | {'maintenance'}, 'fullinterval writer/maintenance observation missing')
        need(all(identity == manifest['lock_file_id'] for identity in observations.values()), 'interval lock identity differs')
        checks = sample['table_checks']
        need(isinstance(checks, dict) and set(checks) == TABLES, 'fullinterval4table checks missing')
        for table, check in checks.items():
            exact(check, 'rows duplicates key_sha256 clocks_sha256', 'table DQ sample')
            expected = manifest['writer_manifest'][table]
            number(check['rows'], 'table row count', integer=True)
            need(type(check['duplicates']) is int and check['duplicates'] == 0 and check['rows'] == len(expected['keys'])
                 and check['key_sha256'] == digest(sorted(expected['keys']))
                 and check['clocks_sha256'] == digest(expected['original_clocks']), 'interval table loss/duplicates/clock drift')


def _history(manifest, w, rows, interval, work):
    need(rows['portion'] and rows['task'] and rows['attempt'], 'missing portion/task/shared-attempt denominator')
    for p in rows['portion']:
        start, end = _interval(p, interval)
        need((end - start).total_seconds() <= 2700, 'portion exceeds45min')
        attempts = [a for a in rows['attempt'] if a['portion_id'] == p['id']]
        need(0 < len(attempts) <= MAX_HISTORY_ATTEMPTS, 'shared attempts must be1..500')
        need(all(start <= stamp(a['at']) < end for a in attempts), 'attempt outside portion')
    need(all(a['portion_id'] in w['inventory']['portion'] for a in rows['attempt']), 'unassigned history attempt')
    for t in rows['task']:
        start, end = _interval(t, interval)
        need(t['portion_id'] in w['inventory']['portion'] and (end - start).total_seconds() <= 2700, 'task exceeds45min/unknown portion')
        p = next(p for p in rows['portion'] if p['id'] == t['portion_id'])
        need(stamp(p['start']) <= start < end <= stamp(p['end']), 'task outside portion')
    need({t['portion_id'] for t in rows['task']} == set(w['inventory']['portion']), 'missing task portion')
    need(isinstance(manifest['lease_manifest'], dict) and set(manifest['lease_manifest']) == set(w['inventory']['lease']) and rows['lease'], 'missing full original lease grant denominator')
    unclosed = 0
    for lease in rows['lease']:
        original = manifest['lease_manifest'][lease['id']]
        exact(original, 'actor grant_id acquired_at expires_at grant_sha256', 'original lease grant')
        need(all(lease[k] == v for k, v in original.items()), 'original lease grant identity drift')
        need(all(isinstance(original[k], str) and original[k] for k in ('actor', 'grant_id')), 'missing actor/grant')
        sha(original['grant_sha256'])
        acquired, expires = stamp(original['acquired_at']), stamp(original['expires_at'])
        need(acquired < expires and acquired < interval[1], 'invalid original lease interval')
        if lease['closed_at'] is None:
            unclosed += 1
        else:
            need(acquired <= stamp(lease['closed_at']) <= interval[1] and lease['reconciliation_actor'] == original['actor'], 'lease close/actor mismatch')
    need(unclosed / len(rows['lease']) < .01, 'unclosed leases must be strictly<1%')
    need(rows['current'], 'missing enqueue->start current denominator')
    for c in rows['current']:
        number(c['history_wait_seconds'], 'history waiting')
        need(interval[0] <= stamp(c['enqueued_at']) <= stamp(c['started_at']) < interval[1]
             and c['history_wait_seconds'] == 0, 'history-attributed current waiting')
    need(len(rows['batch']) == 3, 'exactly3consecutive batches required')
    identities = _batch_identities(manifest.get('batch_manifest'), w, work)
    qualified = identities is not None and all('capture_units' in b for b in rows['batch'])
    previous = None
    for i, b in enumerate(sorted(rows['batch'], key=lambda b: stamp(b['at'])), 1):
        for k in ('sequence', 'expected_remainder', 'observed_remainder', 'expected_rows', 'observed_rows', 'duplicates', 'missing'):
            number(b[k], k, integer=True)
        need(b['sequence'] == i and b['expected_remainder'] == b['observed_remainder']
             and b['expected_rows'] == b['observed_rows'] and b['duplicates'] == b['missing'] == 0, 'batch remainder/DQ differs')
        sha(b['checkpoint_before']); sha(b['checkpoint_after'])
        need(b['checkpoint_before'] != b['checkpoint_after'] and b['resumed_from'] == (previous or b['checkpoint_before']), 'missing checkpoint/resume chain')
        if previous:
            need(b['checkpoint_before'] == previous, 'checkpoint chain gap')
        need(isinstance(b['raw_times'], list) and b['raw_times'] and all(stamp(t) <= stamp(b['at']) for t in b['raw_times']), 'missing/future original raw clocks')
        if qualified:
            expected = identities[b['id']]
            need(isinstance(b['capture_units'], list) and len(b['capture_units']) == len(expected)
                 and sorted(b['capture_units'], key=lambda u: u['physical_unit_id']) == sorted(expected, key=lambda u: u['physical_unit_id']), 'batch original capture/physical unit membership or clock drift')
            need(sorted(b['raw_times']) == sorted(u['fetched_at'] for u in expected), 'batch original raw clocks differ from pinned units')
            need(b['expected_rows'] == b['observed_rows'] == sum(u['rows'] for u in expected), 'batch counts differ from complete pinned unit denominator')
            need(all(stamp(u['captured_at']) <= stamp(b['at']) for u in expected), 'capture occurs after batch')
        previous = b['checkpoint_after']
    return 'VALID' if qualified else 'UNKNOWN'


def _batch_identities(declared, window, work):
    if declared is None:
        return None
    need(isinstance(declared, dict) and set(declared) == set(window['inventory']['batch']), 'missing pinned full batch unit denominator')
    physical, capture, raw = {}, {}, {}
    for units in declared.values():
        need(isinstance(units, list) and units, 'empty batch physical-unit denominator')
        seen = set()
        for unit in units:
            exact(unit, 'work_id raw_id raw_sha256 capture_id physical_unit_id native_batch_id table rows kind empty_type fetched_at captured_at', 'capture unit')
            need(unit['work_id'] in work and unit['table'] in TABLES, 'unknown capture work/table')
            for name in ('raw_id', 'capture_id', 'physical_unit_id', 'native_batch_id'):
                need(isinstance(unit[name], str) and unit[name], 'missing original unit identity')
            sha(unit['raw_sha256'])
            number(unit['rows'], 'unit rows', integer=True)
            need(stamp(unit['fetched_at']) <= stamp(unit['captured_at'])
                 and unit['captured_at'] == work[unit['work_id']]['original_capture_at'], 'pinned capture original clock differs from workload')
            empty_type = 'market_value' if 'market_value' in unit['table'] else 'transfer_events'
            need((unit['kind'] == 'rows' and unit['rows'] > 0 and unit['empty_type'] is None)
                 or (unit['kind'] == 'empty' and unit['rows'] == 0 and unit['empty_type'] == empty_type), 'invalid typed empty capture unit')
            need(unit['physical_unit_id'] not in seen, 'duplicate physical unit within batch')
            seen.add(unit['physical_unit_id'])
            for registry, identity, original in ((physical, unit['physical_unit_id'], unit),
                                                  (capture, unit['capture_id'], {k: unit[k] for k in ('raw_id', 'raw_sha256', 'fetched_at', 'captured_at')}),
                                                  (raw, unit['raw_id'], [unit['raw_sha256'], unit['fetched_at']])):
                need(identity not in registry or registry[identity] == original, 'global capture/physical/raw identity or original clock drift')
                registry[identity] = original
    return declared


def resource_event(manifest, events, *, window_id, identity, sequence, start, end, process_status, cgroup_stats,
                   writer_observations=None, table_checks=None, process_identity=None, cgroup_identity=None):
    """Export one span from retained /proc/cgroup text and enumerated events.

    The caller supplies local snapshot contents. This function opens no runtime
    paths and calls no source/Trino. Missing measurements are never zero-filled.
    """
    windows = [w for w in manifest['windows'] if w['id'] == window_id]
    need(len(windows) == 1, 'resource collector requires exact window')
    bounds = (stamp(windows[0]['start']), stamp(windows[0]['end']))
    begin, finish = _interval({'start': start, 'end': end}, bounds)
    need((finish - begin).total_seconds() <= manifest['sampling_seconds'], 'resource span exceeds configured sampling interval')
    rss_lines = [line.split() for line in process_status.splitlines() if line.startswith('VmRSS:')]
    need(len(rss_lines) == 1 and len(rss_lines[0]) == 3 and rss_lines[0][2] == 'kB', 'missing/ambiguous VmRSS measurement')
    try:
        process_rss = int(rss_lines[0][1]) * 1024
    except ValueError as exc:
        raise InvalidEvidence('invalid VmRSS measurement') from exc
    number(process_rss, 'process RSS', integer=True, positive=True)
    exact(cgroup_stats, 'memory.current memory.max memory.swap.current', 'cgroup snapshot')
    for name, value in cgroup_stats.items():
        number(value, name, integer=True, positive=name == 'memory.max')
    relevant = [e for e in events if e.get('window') == window_id]
    attempts = [e for e in relevant if e['kind'] == 'attempt' and begin <= stamp(e['at']) < finish]
    complete = [e for e in relevant if e['kind'] == 'completion' and begin <= stamp(e['at']) < finish]
    queries = [e for e in relevant if e['kind'] == 'query' and begin <= stamp(e['at']) < finish]
    for attempt in attempts:
        number(attempt['provider_bytes'], 'provider bytes', integer=True)
    for query in queries:
        number(query['peak_memory_bytes'], 'query memory', integer=True)
    current = [e for e in relevant if e['kind'] == 'current' and stamp(e['enqueued_at']) < finish and stamp(e['started_at']) >= begin]
    # Exact enqueue/start records supply attribution; continuous sample presence
    # still must be checked against the pinned producer inventory by validate().
    current_waiting = sum(e['history_wait_seconds'] > 0 for e in current)
    return {'id': identity, 'kind': 'coverage', 'window': window_id, 'producer_sequence': sequence,
            'start': start, 'end': end, 'attempts': len(attempts),
            'provider_bytes': sum(e['provider_bytes'] for e in attempts), 'completions': len(complete),
            'rss_process_bytes': process_rss, 'cgroup_memory_bytes': cgroup_stats['memory.current'],
            'memory_limit_bytes': cgroup_stats['memory.max'], 'swap_bytes': cgroup_stats['memory.swap.current'],
            'trino_memory_bytes': max((e['peak_memory_bytes'] for e in queries), default=0),
            'process_identity': process_identity, 'cgroup_identity': cgroup_identity,
            'current_waiting': current_waiting, 'writer_observations': writer_observations or {}, 'table_checks': table_checks or {}}


def retained_peaks(window, expected, record, *, manifest_pinned):
    """Qualify retained high-water marks only for the exact whole-window owner.

    Identity/reset assertions come from the externally pinned manifest. Parsing
    an HWM alone cannot establish its reset interval or ownership.
    """
    result = {'qualification': 'UNKNOWN', 'reason': 'Missing pinned whole-window peak identity/reset proof.'}
    try:
        exact(expected, 'process_identity cgroup_identity process_started_at cgroup_peak_reset_at observed_at record_sha256', 'peak manifest')
        exact(record, 'window process_identity cgroup_identity process_started_at cgroup_peak_reset_at observed_at proc_status cgroup_memory_peak_bytes', 'retained peak')
        need(sha(expected['record_sha256']) == digest(record), 'retained peak record checksum differs')
        for name in ('process_identity', 'cgroup_identity'):
            need(isinstance(expected[name], str) and expected[name] and isinstance(record[name], str) and record[name], 'missing peak owner identity')
        for name in ('process_started_at', 'cgroup_peak_reset_at', 'observed_at'):
            if record[name] is not None: stamp(record[name])
            if expected[name] is not None: stamp(expected[name])
        hwm = [line.split() for line in record['proc_status'].splitlines() if line.startswith('VmHWM:')]
        need(len(hwm) == 1 and len(hwm[0]) == 3 and hwm[0][2] == 'kB', 'missing/ambiguous retained VmHWM')
        process_peak = int(hwm[0][1]) * 1024
        number(process_peak, 'VmHWM', integer=True, positive=True)
        number(record['cgroup_memory_peak_bytes'], 'memory.peak', integer=True, positive=True)
        need(record['cgroup_memory_peak_bytes'] >= process_peak, 'cgroup peak below process high-water mark')
        exact_owner = record['window'] == window['id'] and all(record[name] == expected[name] for name in ('process_identity', 'cgroup_identity', 'process_started_at', 'cgroup_peak_reset_at', 'observed_at'))
        exact_interval = (record['process_started_at'] == record['cgroup_peak_reset_at'] == window['start']
                          and record['observed_at'] == window['end'])
        if manifest_pinned and exact_owner and exact_interval:
            return {'qualification': 'VALID', 'process_rss_peak_bytes': process_peak,
                    'cgroup_memory_peak_bytes': record['cgroup_memory_peak_bytes'],
                    'process_identity': record['process_identity'], 'cgroup_identity': record['cgroup_identity'],
                    'interval': [window['start'], window['end']], 'record_sha256': expected['record_sha256']}
        return result
    except (InvalidEvidence, ValueError, TypeError, KeyError, AttributeError) as exc:
        return {'qualification': 'INVALID', 'reason': str(exc)}
