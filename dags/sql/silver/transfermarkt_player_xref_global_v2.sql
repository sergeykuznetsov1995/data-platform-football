-- =============================================================================
-- Shadow Silver v2: Transfermarkt global player bridge
-- =============================================================================
--
-- Transfermarkt player_id is global, while silver.xref_player is intentionally
-- season-scoped.  A global career fact may use a canonical_id only when the
-- mapping is unambiguous across *all* league/season rows.  We additionally
-- enforce the inverse invariant (one Transfermarkt id per canonical player),
-- because silently merging two source ids would collapse career facts.
--
-- Grain: player_id.  resolution_status is one of:
--   resolved           exactly one candidate in both directions
--   orphan             no non-orphan candidate in xref_player
--   source_conflict    player_id maps to multiple canonical ids
--   canonical_conflict canonical id maps to multiple player_ids
--
-- Conflicts deliberately keep canonical_id NULL.  The native readiness gate
-- requires zero *_conflict rows before cutover.

WITH native_players AS (
    SELECT player_id FROM iceberg.bronze.transfermarkt_squad_memberships
    UNION
    SELECT player_id FROM iceberg.bronze.transfermarkt_player_attribute_observations
    UNION
    SELECT player_id FROM iceberg.bronze.transfermarkt_market_value_points
    UNION
    SELECT player_id FROM iceberg.bronze.transfermarkt_transfer_events
),

source_rollup AS (
    SELECT
        p.player_id,
        COUNT(DISTINCT x.canonical_id) FILTER (
            WHERE x.confidence <> 'orphan' AND x.canonical_id IS NOT NULL
        )                                                   AS canonical_candidate_count,
        MAX(x.canonical_id) FILTER (
            WHERE x.confidence <> 'orphan' AND x.canonical_id IS NOT NULL
        )                                                   AS canonical_candidate,
        COUNT(x.source_id)                                  AS mapping_rows,
        MIN(x.season)                                       AS first_mapped_season,
        MAX(x.season)                                       AS last_mapped_season
    FROM native_players p
    LEFT JOIN iceberg.silver.xref_player x
      ON x.source = 'transfermarkt'
     AND CAST(x.source_id AS varchar) = CAST(p.player_id AS varchar)
    GROUP BY p.player_id
),

canonical_rollup AS (
    SELECT
        canonical_candidate,
        COUNT(DISTINCT player_id)                           AS source_player_count
    FROM source_rollup
    WHERE canonical_candidate_count = 1
    GROUP BY canonical_candidate
)

SELECT
    s.player_id,
    CASE
        WHEN s.canonical_candidate_count = 1
         AND c.source_player_count = 1
        THEN s.canonical_candidate
    END                                                     AS canonical_id,
    CASE
        WHEN s.canonical_candidate_count = 0 THEN 'orphan'
        WHEN s.canonical_candidate_count > 1 THEN 'source_conflict'
        WHEN c.source_player_count > 1 THEN 'canonical_conflict'
        ELSE 'resolved'
    END                                                     AS resolution_status,
    s.canonical_candidate_count,
    COALESCE(c.source_player_count, 0)                      AS source_player_count,
    s.mapping_rows,
    s.first_mapped_season,
    s.last_mapped_season
FROM source_rollup s
LEFT JOIN canonical_rollup c
  ON c.canonical_candidate = s.canonical_candidate
