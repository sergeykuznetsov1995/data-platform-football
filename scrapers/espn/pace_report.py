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
    for group, lane, step, attempts, count429, errors, p95, size, incomplete in rows:
        latency = 'unknown' if p95 is None else f'{p95:.0f} ms'
        result.append(f'{group} {lane} S{step}: attempts={attempts}, 429={count429}, '
                      f'errors={errors}, HTTP p95={latency}, bytes={size}, incomplete={incomplete}')
    return result
