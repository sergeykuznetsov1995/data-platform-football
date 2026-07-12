-- =============================================================================
-- Shadow Silver v2: one deterministic player attribute record per player_id
-- =============================================================================
-- Immutable bio fields are resolved independently from complete observations
-- (then newest, then lowest club_id as a stable tie-break). Mutable age and
-- contract come strictly from the newest observation; current market value
-- prefers the latest global MV point. In particular, contract_until is NOT
-- carried forward when the newest observation contains NULL.

WITH scored AS (
    SELECT
        o.*,
        (CASE WHEN name        IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN position    IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN dob         IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN height_cm   IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN foot        IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN nationality IS NOT NULL THEN 1 ELSE 0 END) AS completeness_score
    FROM iceberg.silver.transfermarkt_player_attribute_observations_v2 o
),

scoped AS (
    SELECT
        s.*,
        -- ``observed_at`` is fetch time, not source-season recency.  A
        -- historical backfill can be fetched today but its squad page does not
        -- expose the current-season Contract column.  Keep mutable fields in
        -- the newest available membership-season scope, then choose the newest
        -- observation within that scope.
        MAX(season) OVER (PARTITION BY player_id)          AS latest_scope_season
    FROM scored s
),

ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY CASE WHEN season = latest_scope_season THEN 0 ELSE 1 END,
                     observed_at DESC,
                     _bronze_ingested_at DESC,
                     completeness_score DESC,
                     club_id ASC
        ) AS latest_rn
    FROM scoped s
),

latest AS (
    SELECT * FROM ranked WHERE latest_rn = 1
),

-- Resolve every immutable attribute independently.  Choosing one "best row"
-- can still discard a non-NULL foot/dob/etc. present in a different
-- observation; ordered ARRAY_AGG keeps the choice deterministic without that
-- loss.  Completeness wins, then recency, then the stable club_id tie-break.
bio AS (
    SELECT
        player_id,
        ELEMENT_AT(ARRAY_AGG(player_slug ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE player_slug IS NOT NULL), 1)
                                                            AS player_slug,
        ELEMENT_AT(ARRAY_AGG(name ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE name IS NOT NULL), 1)
                                                            AS name,
        ELEMENT_AT(ARRAY_AGG(position ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE position IS NOT NULL), 1)
                                                            AS position,
        ELEMENT_AT(ARRAY_AGG(dob ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE dob IS NOT NULL), 1)
                                                            AS dob,
        ELEMENT_AT(ARRAY_AGG(height_cm ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE height_cm IS NOT NULL), 1)
                                                            AS height_cm,
        ELEMENT_AT(ARRAY_AGG(foot ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE foot IS NOT NULL), 1)
                                                            AS foot,
        ELEMENT_AT(ARRAY_AGG(nationality ORDER BY completeness_score DESC,
                             observed_at DESC, _bronze_ingested_at DESC,
                             club_id ASC) FILTER (WHERE nationality IS NOT NULL), 1)
                                                            AS nationality,
        MAX(completeness_score)                             AS completeness_score
    FROM scored
    GROUP BY player_id
),

latest_mv AS (
    SELECT player_id, value_eur, mv_date
    FROM (
        SELECT
            player_id,
            value_eur,
            mv_date,
            ROW_NUMBER() OVER (
                PARTITION BY player_id
                ORDER BY mv_date DESC, _bronze_ingested_at DESC
            ) AS rn
        FROM iceberg.silver.transfermarkt_market_value_points_v2
    )
    WHERE rn = 1
),

latest_contract AS (
    SELECT player_id, contract_until
    FROM (
        SELECT
            c.player_id,
            c.contract_until,
            ROW_NUMBER() OVER (
                PARTITION BY c.player_id
                ORDER BY e.end_date DESC NULLS LAST,
                         c.observed_at DESC,
                         c._bronze_ingested_at DESC,
                         c.team_id ASC
            ) AS rn
        FROM iceberg.silver.transfermarkt_player_contract_observations_v2 c
        LEFT JOIN iceberg.silver.transfermarkt_competition_editions_v2 e
          ON e.competition_id = c.competition_id
         AND e.edition_id = c.edition_id
        WHERE c.applicability_status <> 'not_applicable'
    )
    WHERE rn = 1
)

SELECT
    b.player_id,
    x.canonical_id,
    b.player_slug,
    b.name,
    b.position,
    b.dob,
    l.age,
    b.height_cm,
    b.foot,
    b.nationality,
    CASE
        WHEN lc.player_id IS NOT NULL THEN lc.contract_until
        ELSE l.contract_until
    END                                                   AS contract_until,
    COALESCE(mv.value_eur, l.market_value_eur)           AS current_market_value_eur,
    mv.mv_date                                           AS current_market_value_date,
    l.club_id                                            AS observed_club_id,
    l.club_name                                          AS observed_club_name,
    l.observed_at,
    l._bronze_ingested_at,
    l.league                                             AS observed_league,
    l.season                                             AS observed_season,
    b.completeness_score
FROM bio b
JOIN latest l
  ON l.player_id = b.player_id
LEFT JOIN iceberg.silver.transfermarkt_player_xref_global_v2 x
  ON x.player_id = b.player_id
 AND x.resolution_status = 'resolved'
LEFT JOIN latest_mv mv
  ON mv.player_id = b.player_id
LEFT JOIN latest_contract lc
  ON lc.player_id = b.player_id
