-- =============================================================================
-- Gold: fct_player_fifa_rating  (EA Sports FC game ratings, issue #430)
-- =============================================================================
-- One row per (player_id, fifa_edition). Projection of
-- silver.sofifa_player_profile — canonical_id is already resolved in Silver
-- (LEFT JOIN xref_player, source='sofifa'), so there is NO xref hop here
-- (mirrors fct_transfer / dim_player_attributes SoFIFA block).
--
-- IMPORTANT: these are game-side ratings (EA Sports' opinion), NOT real match
-- metrics — a cheap once-a-year proxy of player quality.
--
-- fifa_edition is the natural season key (1:1 with the football season slug —
-- 'FC 26' -> '2526'), so this is a pointwise off-field fact (design §6 rule 5):
-- no separate league/season columns, no partitioning — like fct_team_elo.
--
-- Orphan-ID strategy (design §6 rule 2 — rows are never dropped): canonical_id
-- is NULL for ≈15% of SoFIFA players (loan-out / U21 outside the FBref spine).
-- Each keeps a deterministic 'sf_'-prefixed id instead of a NULL PK component:
--
--   player_id = COALESCE(canonical_id, 'sf_' || player_id)
--
-- 'sf_' mirrors xref_player_resolver._orphan_prefix; the dim_player FK is
-- therefore WARNING rate-mode.
--
-- ROW_NUMBER dedup: Silver is one row per (source player_id, fifa_edition), but
-- two source players could resolve to the same canonical_id within one edition
-- (resolver anomaly) — collapse to the higher-rated row so the PK stays unique.
--
-- PK:           (player_id, fifa_edition)
-- FK:           player_id -> dim_player (soft, WARNING rate-mode)
-- Partitioning: none (small off-field table, edition-keyed)
-- =============================================================================

WITH resolved AS (
    SELECT
        COALESCE(p.canonical_id, 'sf_' || p.player_id)   AS player_id,
        p.player_name,
        p.fifa_edition,
        p.overall,
        p.potential,
        p.pace,
        p.shooting,
        p.passing,
        p.dribbling,
        p.defending,
        p.physical,
        p.gk_diving,
        p.gk_handling,
        p.gk_kicking,
        p.gk_positioning,
        p.gk_reflexes,
        p.value_eur,
        p.wage_eur,
        p._bronze_ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(p.canonical_id, 'sf_' || p.player_id),
                         p.fifa_edition
            ORDER BY p.overall DESC NULLS LAST, p._bronze_ingested_at DESC
        )                                                AS rn
    FROM iceberg.silver.sofifa_player_profile p
)

SELECT
    player_id,
    player_name,
    fifa_edition,
    overall,
    potential,
    pace,
    shooting,
    passing,
    dribbling,
    defending,
    physical,
    gk_diving,
    gk_handling,
    gk_kicking,
    gk_positioning,
    gk_reflexes,
    value_eur,
    wage_eur,
    _bronze_ingested_at
FROM resolved
WHERE rn = 1
