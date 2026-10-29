-- PostgreSQL, read-only. Input evidence tables are described in
-- docs/operations/fbref-proxy-waste-1339.md; no production writes are needed.
WITH attempts AS (
    SELECT r.period, p.page_kind, a.logical_refresh_id, a.status
    FROM fbref_control.fetch_attempt AS a
    JOIN proxy_waste_runs AS r ON r.run_id = a.run_id
    JOIN proxy_waste_windows AS w ON w.period = r.period
    JOIN fbref_control.page_frontier AS p ON p.target_id = a.target_id
    WHERE a.started_at >= w.started_at AND a.started_at < w.ended_at
), pages AS (
    SELECT period, page_kind,
           COUNT(DISTINCT logical_refresh_id) AS pages,
           COUNT(*) AS attempts,
           SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_attempts
    FROM attempts
    GROUP BY period, page_kind
), traffic AS (
    -- Aggregate independently: joining byte events to attempts multiplies bytes.
    -- No time/host/status filter: warmup, errors and post-window tails count.
    SELECT l.period, l.page_kind, SUM(e.bytes) AS total_bytes
    FROM proxy_waste_leases AS l
    JOIN proxy_waste_ledger AS e ON e.lease_id = l.lease_id
    WHERE e.event_version = 'paid-proxy-v2'
      AND e.source = 'fbref' AND e.event_type = 'bytes'
    GROUP BY l.period, l.page_kind
), kinds AS (
    SELECT page_kind FROM pages UNION SELECT page_kind FROM traffic
), comparison AS (
    SELECT k.page_kind,
           b.pages AS before_pages, a.pages AS after_pages,
           b.attempts AS before_attempts, a.attempts AS after_attempts,
           b.failed_attempts AS before_failed, a.failed_attempts AS after_failed,
           bt.total_bytes AS before_bytes, at.total_bytes AS after_bytes,
           1.0 * bt.total_bytes / NULLIF(b.pages, 0) AS before_bytes_per_page,
           1.0 * at.total_bytes / NULLIF(a.pages, 0) AS after_bytes_per_page
    FROM kinds AS k
    LEFT JOIN pages AS b ON b.page_kind = k.page_kind AND b.period = 'before'
    LEFT JOIN pages AS a ON a.page_kind = k.page_kind AND a.period = 'after'
    LEFT JOIN traffic AS bt ON bt.page_kind = k.page_kind AND bt.period = 'before'
    LEFT JOIN traffic AS at ON at.page_kind = k.page_kind AND at.period = 'after'
)
SELECT comparison.*,
       100.0 * (1.0 - after_bytes_per_page / NULLIF(before_bytes_per_page, 0))
           AS reduction_percent,
       CASE WHEN before_bytes_per_page > 0 AND after_bytes_per_page IS NOT NULL
            THEN after_bytes_per_page <= 0.8 * before_bytes_per_page
            ELSE NULL END AS traffic_reduction_ge_20_percent
FROM comparison
ORDER BY page_kind;
