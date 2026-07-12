-- =============================================================================
-- Silver: fbref_match_events
-- =============================================================================
--
-- Detailed per-event rows (goals, cards, substitutions) for each match.
--
-- Source:
--   iceberg.bronze.fbref_match_events
--
-- Deduplication (#463):
--   ROW_NUMBER() OVER (PARTITION BY match_id, minute,
--                                   COALESCE(player_id, player), event_type
--                       ORDER BY _ingested_at DESC, _batch_id DESC)  =>  rn = 1
--   COALESCE guards rows without player_id (two carded NULL-id players in the
--   same minute must NOT collapse). Key-based dedup is kept deliberately:
--   live bronze (2026-06-12) contains 4 bit-identical duplicate yellow_card
--   rows (parse artifacts) that this key correctly collapses.
--
-- Notes:
--   * event_type values: goal, penalty, penalty_missed, own_goal,
--     yellow_card, second_yellow_card, red_card, substitution.
--   * All columns remain VARCHAR — no TRY_CAST needed.
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH schedule AS (
    SELECT
        REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1) AS match_id,
        score,
        ROW_NUMBER() OVER (
            PARTITION BY REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1)
            ORDER BY _ingested_at DESC, _batch_id DESC
        ) AS rn
    FROM iceberg.bronze.fbref_schedule
    WHERE NULLIF(REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1), '') IS NOT NULL
),

src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, COALESCE(player_id, player), event_type
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
)

SELECT
    src.match_id,
    src.minute,
    src.event_type,
    src.player,
    src.player_id,
    src.team,
    src.team_side,
    src.secondary_player,
    src.secondary_player_id,
    CASE
        WHEN src.event_type IN ('penalty', 'penalty_missed')
         AND NULLIF(TRIM(src.minute), '') IS NULL
         AND REGEXP_LIKE(schedule.score, '\(\d+\).*\(\d+\)') THEN 'shootout'
        WHEN TRY_CAST(SPLIT_PART(src.minute, '+', 1) AS INTEGER) > 90 THEN 'extra_time'
        WHEN NULLIF(TRIM(src.minute), '') IS NOT NULL THEN 'regulation'
        ELSE 'unknown'
    END AS event_phase,
    (src.event_type IN ('penalty', 'penalty_missed')
     AND NULLIF(TRIM(src.minute), '') IS NULL
     AND REGEXP_LIKE(schedule.score, '\(\d+\).*\(\d+\)')) AS is_shootout,

    -- ========= Lineage =========
    src._ingested_at               AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    src.league,
    -- #913 Phase 2
    CASE
         WHEN REGEXP_LIKE(COALESCE(src.source_season_id, ''), '^\d{4}$')
             THEN src.source_season_id
         WHEN REGEXP_LIKE(COALESCE(src.source_season_id, ''), '^\d{4}-\d{4}$')
             THEN SUBSTR(src.source_season_id, 3, 2)
                  || SUBSTR(src.source_season_id, 8, 2)
         WHEN NULLIF(TRIM(src.source_season_id), '') IS NOT NULL
             THEN TRIM(src.source_season_id)
         WHEN src.league = 'INT-World Cup'
             THEN LPAD(CAST(src.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(src.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(src.season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM src
LEFT JOIN schedule
    ON  schedule.match_id = src.match_id
    AND schedule.rn = 1
WHERE src.rn = 1
