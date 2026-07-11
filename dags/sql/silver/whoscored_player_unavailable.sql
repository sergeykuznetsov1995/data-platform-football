-- =============================================================================
-- Silver: whoscored_player_unavailable
-- =============================================================================
-- Per-match player unavailability, sourced from WhoScored "missing players".
-- One row per (match_id, team, ws_player_id).
--
-- Sources:
--   iceberg.bronze.whoscored_missing_players_current  (mp; latest successful
--                                                       preview batch per game)
--   iceberg.bronze.whoscored_schedule         (s)  — joined for match_date
--
-- Architectural filters (D3 + D5):
--   * status = 'Out' (case-insensitive)              — D5: confirmed absence.
--       Bronze "missing players" uses two tiers: 'Out' (confirmed unavailable)
--       and 'Doubtful' (uncertain / rumour-like). We keep only 'Out'. The old
--       literal 'confirmed' never existed in Bronze → matched 0 rows → the table
--       was structurally always empty (#393).
--   * LOWER(reason) <> 'international duty'           — D3: national-team call-ups
--       are not a "form" signal. Bronze reason is lowercase ('international
--       duty'), so the comparison MUST be case-insensitive — the old
--       case-sensitive '<> International duty' never excluded anything (#393).
--   * player_id IS NOT NULL                          — rows without a stable id
--                                                       cannot be deduped or joined
-- =============================================================================

WITH mp_dedup AS (
    SELECT
        mp.league,
        mp.season,
        mp.game,
        mp.team,
        mp.player_id,
        mp.player,
        mp.reason,
        mp.status,
        mp._ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY mp.game, mp.team, mp.player_id
            ORDER BY mp._ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.whoscored_missing_players_current mp
    WHERE LOWER(mp.status) = 'out'
      AND (mp.reason IS NULL OR LOWER(mp.reason) <> 'international duty')
      AND mp.player_id IS NOT NULL
),

sch_dedup AS (
    -- Schedule may carry multiple snapshots; date doesn't change once published,
    -- so keep the latest per (league, season, game).
    SELECT
        league,
        season,
        game,
        date,
        ROW_NUMBER() OVER (
            PARTITION BY league, season, game
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.whoscored_schedule
)

SELECT
    -- ========= Identity =========
    mp.game                                AS match_id,
    TRY_CAST(s.date AS DATE)               AS match_date,

    -- ========= Source attributes =========
    mp.league,
    -- season как varchar-slug ('2526'), per charter S2 (#388). Bronze хранит
    -- season как slug-строку; CAST держит тот же slug без смены значения.
    -- Year-start↔slug мост к bigint-dims делает gold/fct_player_unavailable.sql.
    CAST(mp.season AS varchar)             AS season,
    mp.team                                AS team_name,
    mp.player_id                           AS ws_player_id,
    mp.player                              AS player_name,
    mp.reason,
    mp.status,

    -- ========= Lineage =========
    mp._ingested_at                        AS _bronze_ingested_at

FROM mp_dedup mp
LEFT JOIN sch_dedup s
    ON  s.league = mp.league
    AND s.season = mp.season
    AND s.game   = mp.game
    AND s.rn     = 1
WHERE mp.rn = 1
