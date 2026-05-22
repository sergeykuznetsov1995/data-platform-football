-- =============================================================================
-- Silver: understat_player_season_aggregate
-- =============================================================================
--
-- One row per (canonical_id, league, season) — Understat season-level player
-- stats bridged through `silver.xref_player` (source='understat'). Mirrors the
-- shape of `silver.whoscored_player_season_aggregate` so Gold layer can LEFT
-- JOIN both on (canonical_id, league, season) varchar-slug.
--
-- Bronze `bronze.understat_players` is already season-grain (one row per
-- (understat_player_id, league, season)) — no aggregation needed, just a JOIN
-- through xref_player to expose the canonical_id.
--
-- Sources:
--   bronze.understat_players       (season aggregates per Understat player_id)
--   silver.xref_player              (canonical_id ↔ raw player_id bridge)
--
-- Notes:
--   * (league, season) JOIN predicate against xref_player is mandatory
--     (see CLAUDE.md / feedback_xref_join_season_predicate.md) — without it
--     a player active across multiple seasons fan-outs.
--   * Orphan rows (confidence='orphan' in xref_player) are filtered out:
--     they cannot bridge to FBref-spine in Gold layer anyway.
--   * Season convention: varchar slug ('2526' for 2025/26), matches xref_player.
-- =============================================================================

WITH xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

-- Bronze dedup: append-mode ingest без `replace_partitions` накапливал
-- идентичные копии (см. CLAUDE.md «Full-state Bronze scrapers must pass
-- replace_partitions=True»). Берём самый свежий по `_ingested_at`.
bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.understat_players b
        WHERE player IS NOT NULL
    )
    WHERE rn = 1
)

-- Понятная грань: иногда Understat трекает одного игрока под двумя
-- player_id (e.g. Harrison Reed 910 + 6827). Resolver правильно мапит
-- оба на один canonical_id, но JOIN получает 2 строки на
-- (canonical_id, league, season). Берём «активную» запись — с большим
-- количеством minutes — остальные дропаются.
SELECT
    canonical_id,
    games_played, minutes_played, goals, assists,
    yellow_cards, red_cards,
    expected_goals, expected_assists,
    non_penalty_goals, non_penalty_xg, xg_chain, xg_buildup,
    key_passes, shots,
    league, season
FROM (
    SELECT
        xp.canonical_id,

        -- ========= HARD_FACT (for COALESCE on Gold layer) =========
        b.matches                            AS games_played,
        b.minutes                            AS minutes_played,
        b.goals                              AS goals,
        b.assists                            AS assists,
        b.yellow_cards                       AS yellow_cards,
        b.red_cards                          AS red_cards,

        -- ========= UNIQUE_UNDERSTAT (xG / xA / build-up) =========
        b.xg                                 AS expected_goals,
        b.xa                                 AS expected_assists,
        b.np_goals                           AS non_penalty_goals,
        b.np_xg                              AS non_penalty_xg,
        b.xg_chain                           AS xg_chain,
        b.xg_buildup                         AS xg_buildup,
        b.key_passes                         AS key_passes,
        b.shots                              AS shots,

        -- ========= Partition keys =========
        b.league,
        b.season,

        ROW_NUMBER() OVER (
            PARTITION BY xp.canonical_id, b.league, b.season
            ORDER BY b.minutes DESC NULLS LAST, b.player_id
        ) AS canonical_rn
    FROM bronze_dedup b
    JOIN xp
      ON CAST(b.player_id AS varchar) = xp.source_id
     AND b.league = xp.league
     AND b.season = xp.season
)
WHERE canonical_rn = 1
