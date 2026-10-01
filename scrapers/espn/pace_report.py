"""UTC attempt evidence SQL and compact owner-report rows (#1510).

Origin metrics group web.api + core as A and reserve site.api as B; this is
independent of transport's site/core protection clusters. Half-open windows
are explicit, never server/session date arithmetic. Coverage and freshness
must be supplied independently; aggregate rows alone never prove eligibility.
"""
from __future__ import annotations

from .attempts import ATTEMPT_TABLE
from .journal import _literal
from .pace import utc


def _bounds(start, end):
    start, end = utc(start), utc(end)
    if end <= start:
        raise ValueError('pace report window must be nonempty')
    return _literal(start, 'timestamp(6)'), _literal(end, 'timestamp(6)')


def render_attempt_sql(start, end):
    start_sql, end_sql = _bounds(start, end)
    return f"""WITH ranked AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY attempt_id ORDER BY complete DESC) AS rn
 FROM {ATTEMPT_TABLE}
 WHERE requested_at >= {start_sql} AND requested_at < {end_sql}
), attempts AS (
 SELECT *, CASE WHEN origin = 'https://site.api.espn.com' THEN 'B'
 WHEN origin IN ('https://site.web.api.espn.com', 'https://sports.core.api.espn.com') THEN 'A'
 ELSE 'unknown' END AS origin_group
 FROM ranked WHERE rn = 1
)
SELECT origin_group, lane, step, COUNT(*) AS attempts,
 SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) AS count_403,
 SUM(CASE WHEN status = 429 THEN 1 ELSE 0 END) AS count_429,
 SUM(CASE WHEN status = 403 OR status BETWEEN 500 AND 599 OR timeout THEN 1 ELSE 0 END) AS error_attempts,
 approx_percentile(http_ms, 0.95) AS http_p95_ms,
 SUM(direct_bytes) AS direct_bytes,
 SUM(CASE WHEN NOT complete OR http_ms IS NULL OR (status IS NULL AND NOT timeout)
 OR origin_group = 'unknown' THEN 1 ELSE 0 END) AS incomplete_attempts
FROM attempts GROUP BY origin_group, lane, step
ORDER BY origin_group, lane, step"""


def render_load_sql(start, end, *, step, policy):
    """Every FULL five-minute UTC bin, including zero-attempt bins.

    PR B must mark bins with live debt or protection pauses ineligible using
    its durable observer evidence. Unknown observer coverage forbids raising.
    The returned `loaded` is only the quota check, not observer eligibility.
    """
    from datetime import datetime, timezone
    import math
    start, end = utc(start), utc(end)
    interval = policy.pace.load_interval_seconds
    first = math.ceil(start.timestamp() / interval) * interval
    stop = math.floor(end.timestamp() / interval) * interval
    if step not in range(len(policy.steps)) or first >= stop:
        raise ValueError('no complete load interval or invalid step')
    bins = ', '.join('(' + _literal(datetime.fromtimestamp(t, timezone.utc), 'timestamp(6)') + ')'
                     for t in range(first, stop, interval))
    quota = policy.steps[step] * (1 - policy.live_share) * interval / 60
    threshold = quota * policy.pace.history_quota_fraction
    start_sql, end_sql = _bounds(start, end)
    return f"""WITH bins (started_at) AS (VALUES {bins}), attempts AS (
 SELECT DISTINCT attempt_id, requested_at FROM {ATTEMPT_TABLE}
 WHERE requested_at >= {start_sql} AND requested_at < {end_sql}
 AND lane = 'history' AND step = {int(step)}
)
SELECT b.started_at, COUNT(a.attempt_id) AS history_attempts,
 COUNT(a.attempt_id) >= {threshold} AS loaded
FROM bins b LEFT JOIN attempts a ON a.requested_at >= b.started_at
 AND a.requested_at < b.started_at + INTERVAL '{interval}' SECOND
GROUP BY b.started_at ORDER BY b.started_at"""


def format_rows(rows):
    """Rows returned by render_attempt_sql; all attempts, reserve failures included."""
    result = []
    for group, lane, step, attempts, count403, count429, errors, p95, size, incomplete in rows:
        latency = 'unknown' if p95 is None else f'{p95:.0f} ms'
        result.append(f'{group} {lane} S{step}: attempts={attempts}, 403={count403}, 429={count429}, '
                      f'errors={errors}, HTTP p95={latency}, bytes={size}, incomplete={incomplete}')
    return result


def percentile(values, quantile=0.95):
    """Nearest-rank percentile of raw observations, never of group percentiles."""
    import math
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * quantile) - 1)] if values else None


def attempt_metrics(rows):
    rows = list({row['attempt_id']: row for row in rows}.values())
    return {
        'attempts': len(rows),
        'attempts_a': sum(r['origin'] in ('https://site.web.api.espn.com',
                                        'https://sports.core.api.espn.com') for r in rows),
        'attempts_b': sum(r['origin'] == 'https://site.api.espn.com' for r in rows),
        'count_403': sum(r['status'] == 403 for r in rows),
        'count_429': sum(r['status'] == 429 for r in rows),
        'error_attempts': sum(r['status'] == 403 or 500 <= (r['status'] or 0) <= 599
                              or r['timeout'] for r in rows),
        'server_timeout_attempts': sum(500 <= (r['status'] or 0) <= 599 or r['timeout']
                                       for r in rows),
        'http_p95_ms': percentile([r['http_ms'] for r in rows if r['http_ms'] is not None]),
    }


def load_evidence(rows, observations, start, end, *, step, policy, maximum_gap=30):
    """Known pauses are excluded; an observation gap never counts as a pause."""
    import math
    from bisect import bisect_left, bisect_right
    start, end = utc(start).timestamp(), utc(end).timestamp()
    observations = sorted(observations, key=lambda row: row['at'])
    continuous = bool(observations) and observations[0]['at'] <= start and observations[-1]['at'] >= end
    continuous = continuous and all(0 <= b['at'] - a['at'] <= maximum_gap
                                    for a, b in zip(observations, observations[1:]))
    continuous = continuous and all(o.get('known') is True for o in observations)
    interval = policy.pace.load_interval_seconds
    eligible = loaded = paused = unknown = 0
    threshold = policy.steps[step] * (1 - policy.live_share) * interval / 60 * policy.pace.history_quota_fraction
    history = [__import__('datetime').datetime.fromisoformat(r['requested_at']).timestamp()
               for r in rows if r['lane'] == 'history' and r['step'] == step]
    buckets = {}
    for at in history:
        bucket = math.floor(at / interval) * interval
        buckets[bucket] = buckets.get(bucket, 0) + 1
    observation_times = [o['at'] for o in observations]
    bins = []
    for begin in range(math.ceil(start / interval) * interval, math.floor(end / interval) * interval, interval):
        finish = begin + interval
        left = bisect_right(observation_times, begin) - 1
        right = bisect_left(observation_times, finish)
        selected = observations[max(0, left):right + 1]
        known = left >= 0 and right < len(observations) and all(o.get('known') is True for o in selected)
        known = known and all(b['at'] - a['at'] <= maximum_gap for a, b in zip(selected, selected[1:]))
        count = buckets.get(begin, 0)
        if not known:
            unknown += 1
            status = 'unknown'
        elif any(o.get('debt', 0) or o.get('protected') or o.get('step') != step for o in selected):
            paused += 1
            status = 'paused'
        else:
            eligible += 1
            loaded += count >= threshold
            status = 'loaded' if count >= threshold else 'underloaded'
        bins.append(dict(start=begin, attempts=count, status=status))
    return dict(continuous=bool(continuous and unknown == 0), eligible_intervals=eligible,
                loaded_intervals=loaded, paused_intervals=paused, unknown_intervals=unknown,
                load_fraction=loaded / eligible if eligible else None, bins=bins)


def write_metrics(rows):
    useful = [r for r in rows if not r.get('isolated') and r.get('success')]
    isolated = [r for r in rows if r.get('isolated') and r.get('success')]
    def summarize(group):
        total = sum(r['batch_seconds'] for r in group)
        return dict(batches=len(group), matches=sum(r['matches'] for r in group),
                    p95_seconds=percentile([r['write_seconds'] for r in group]),
                    fraction=sum(r['write_seconds'] for r in group) / total if total else None)
    return dict(production=summarize(useful), isolated=summarize(isolated),
                failed=sum(not r.get('success') for r in rows))


def format_measurement(report):
    """Compact owner-readable evidence; missing fields stay explicit warnings."""
    if not report or report.get('stale'):
        return ['• ESPN темп: нет свежих данных контроллера ⚠️']
    completed = report.get('completed')
    if completed:
        lines = []
        for row in completed:
            lines.extend(format_measurement(row))
        last = completed[-1]
        if (last.get('step'), last.get('start'), last.get('end')) != (report.get('step'), report.get('start'), report.get('end')):
            lines.extend(format_measurement({k: v for k, v in report.items() if k != 'completed'}))
        return lines
    def number(value, suffix=''):
        return 'нет данных ⚠️' if value is None else f'{value:.1f}{suffix}'
    m = report.get('metrics', {})
    load = report.get('load', {})
    attempts = m.get('attempts', 0)
    share = 100 * m.get('server_timeout_attempts', 0) / attempts if attempts else None
    fresh = report.get('freshness', {})
    write = report.get('write', {}).get('production', {})
    write_text = ('нет замера записи' if not write.get('batches') else
                  f"p95 {number(write.get('p95_seconds'), ' с')}, доля {number(100 * write['fraction'] if write.get('fraction') is not None else None, '%')}")
    isolated = report.get('isolated_benchmark')
    isolated_text = ('не запускался' if not isolated else number(isolated.get('p95_seconds'), ' с'))
    return [
        f"• ESPN темп S{report.get('step', '?')}: {report.get('start', '?')} — {report.get('end', '?')} UTC",
        f"  попытки A/B {m.get('attempts_a', '?')}/{m.get('attempts_b', '?')}; 403 {m.get('count_403', '?')}; 429 {m.get('count_429', '?')}; 5xx/timeout {number(share, '%')}",
        f"  HTTP p95 {number(m.get('http_p95_ms'), ' мс')}; профиль {number(report.get('profile_p95_ms'), ' мс')}; S0 {number(report.get('baseline_p95_ms'), ' мс')}; сбросов {report.get('resets', '?')}",
        f"  нагрузка {load.get('loaded_intervals', 0)}/{load.get('eligible_intervals', 0)} интервалов; пауз {load.get('paused_intervals', 0)}, неизвестных {load.get('unknown_intervals', 0)}; квота истории {report.get('history_quota_per_minute', '?')}/мин",
        f"  Iceberg: {write_text}; свежесть {fresh.get('ok', '?')}/{fresh.get('due', '?')} за {fresh.get('day', '?')} UTC",
        f"  изолированный стенд: p95 {isolated_text}; замер {isolated.get('measured_at', '?') if isolated else '?'}; массовая запись — проверка #1511",
        f"  повышение: {report.get('reason', 'нет данных ⚠️')}; измерительные чтения не увеличивают охват истории",
    ]
