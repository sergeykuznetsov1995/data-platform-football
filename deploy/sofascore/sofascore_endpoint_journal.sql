WITH endpoints AS (
    SELECT 'event' AS endpoint
    UNION ALL
    SELECT 'lineups' AS endpoint
    UNION ALL
    SELECT 'statistics' AS endpoint
    UNION ALL
    SELECT 'shotmap' AS endpoint
    UNION ALL
    SELECT 'incidents' AS endpoint
),
requested AS (
    SELECT CAST(s.game_id AS VARCHAR) AS match_id
    FROM iceberg.bronze.sofascore_schedule AS s
    WHERE CAST(s.game_id AS VARCHAR) IN ({match_ids})
    UNION
    SELECT CAST(m.target_id AS VARCHAR) AS match_id
    FROM iceberg.ops.sofascore_capture_manifest AS m
    WHERE m.target_type = 'event'
      AND CAST(m.target_id AS VARCHAR) IN ({match_ids})
),
grid AS (
    SELECT
        r.match_id AS match_id,
        e.endpoint AS endpoint
    FROM requested AS r
    CROSS JOIN endpoints AS e
),
manifest_core AS (
    SELECT
        CAST(m.target_id AS VARCHAR) AS target_id,
        m.endpoint AS endpoint,
        m.source_tournament_id AS source_tournament_id,
        m.status AS status,
        m.http_status AS http_status,
        m.updated_at AS updated_at,
        m.run_id AS run_id,
        m.freshness_key AS freshness_key
    FROM iceberg.ops.sofascore_capture_manifest AS m
    WHERE m.target_type = 'event'
      AND m.endpoint IN ('event', 'lineups', 'statistics', 'shotmap', 'incidents')
      AND CAST(m.target_id AS VARCHAR) IN ({match_ids})
),
manifest_ranked AS (
    SELECT
        mc.target_id AS target_id,
        mc.endpoint AS endpoint,
        mc.source_tournament_id AS source_tournament_id,
        mc.status AS status,
        mc.http_status AS http_status,
        mc.updated_at AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY mc.target_id, mc.endpoint
            ORDER BY mc.updated_at DESC, mc.run_id DESC, mc.freshness_key DESC
        ) AS rn
    FROM manifest_core AS mc
),
manifest_latest AS (
    SELECT
        mr.target_id AS target_id,
        mr.endpoint AS endpoint,
        mr.status AS status,
        mr.http_status AS http_status,
        mr.updated_at AS updated_at
    FROM manifest_ranked AS mr
    WHERE mr.rn = 1
),
schedule_rows AS (
    SELECT
        CAST(s.game_id AS VARCHAR) AS match_id,
        s.league AS league,
        s.season AS season,
        CAST(s.tournament_unique_tournament_id AS VARCHAR) AS schedule_tournament_id
    FROM iceberg.bronze.sofascore_schedule AS s
    WHERE CAST(s.game_id AS VARCHAR) IN ({match_ids})
),
schedule_ranked AS (
    SELECT
        sr.match_id AS match_id,
        sr.league AS league,
        sr.season AS season,
        sr.schedule_tournament_id AS schedule_tournament_id,
        ROW_NUMBER() OVER (
            PARTITION BY sr.match_id
            ORDER BY
                CASE WHEN sr.league LIKE 'SS-%' THEN 0 ELSE 1 END,
                sr.league,
                sr.season
        ) AS rn
    FROM schedule_rows AS sr
),
schedule_pick AS (
    SELECT
        spr.match_id AS match_id,
        spr.league AS league,
        spr.season AS season,
        spr.schedule_tournament_id AS schedule_tournament_id
    FROM schedule_ranked AS spr
    WHERE spr.rn = 1
),
ss_ids AS (
    SELECT DISTINCT
        sr.match_id AS match_id,
        SUBSTR(sr.league, 4) AS ss_tournament_id
    FROM schedule_rows AS sr
    WHERE sr.league LIKE 'SS-%'
),
ss_rollup AS (
    SELECT
        ss.match_id AS match_id,
        COUNT(*) AS ss_id_count,
        MIN(ss.ss_tournament_id) AS ss_tournament_id
    FROM ss_ids AS ss
    GROUP BY ss.match_id
),
manifest_scope AS (
    SELECT
        mc.target_id AS match_id,
        MIN(mc.source_tournament_id) AS manifest_source_tournament_id
    FROM manifest_core AS mc
    GROUP BY mc.target_id
),
identity AS (
    SELECT
        r.match_id AS match_id,
        p.league AS league,
        p.season AS season,
        CASE
            WHEN x.ss_id_count = 1 THEN x.ss_tournament_id
            WHEN p.schedule_tournament_id IS NOT NULL THEN p.schedule_tournament_id
            ELSE sc.manifest_source_tournament_id
        END AS source_tournament_id,
        CASE WHEN x.ss_id_count > 1 THEN 1 ELSE 0 END AS identity_conflict
    FROM requested AS r
    LEFT JOIN schedule_pick AS p ON p.match_id = r.match_id
    LEFT JOIN ss_rollup AS x ON x.match_id = r.match_id
    LEFT JOIN manifest_scope AS sc ON sc.match_id = r.match_id
)
SELECT
    g.match_id AS match_id,
    i.league AS league,
    i.season AS season,
    i.source_tournament_id AS source_tournament_id,
    g.endpoint AS endpoint,
    CASE
        WHEN lm.target_id IS NULL THEN 'not_collected'
        WHEN lm.status IN ('success', 'legitimate_empty') THEN 'collected'
        WHEN lm.status = 'not_supported' AND lm.http_status = 404 THEN 'source_404'
        WHEN lm.status = 'schema_error' THEN 'quarantine'
        WHEN lm.status IN ('not_supported', 'retryable_failure') THEN 'not_collected'
        ELSE 'invalid'
    END AS status,
    lm.status AS manifest_status,
    lm.http_status AS http_status,
    lm.updated_at AS status_updated_at,
    lm.updated_at AS observed_at,
    i.identity_conflict AS identity_conflict
FROM grid AS g
LEFT JOIN identity AS i ON i.match_id = g.match_id
LEFT JOIN manifest_latest AS lm
    ON lm.target_id = g.match_id
   AND lm.endpoint = g.endpoint
UNION ALL
SELECT
    '__sentinel__' AS match_id,
    NULL AS league,
    NULL AS season,
    NULL AS source_tournament_id,
    NULL AS endpoint,
    NULL AS status,
    NULL AS manifest_status,
    NULL AS http_status,
    NULL AS status_updated_at,
    NULL AS observed_at,
    0 AS identity_conflict
