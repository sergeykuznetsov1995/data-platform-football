-- =============================================================================
-- Gold: fct_player_market_value  (issue #430 — two sources + source in PK)
-- =============================================================================
-- One valuation point per (player_id, valuation_date, source).
-- Designed for two sources kept side by side (FotMob + Transfermarkt); the
-- FotMob branch was removed in #1590 with the legacy FotMob Silver layer:
--
--   transfermarkt — silver.transfermarkt_market_value_history (canonical_id IS
--                   resolved in Silver; unresolved rows retain tm_<source_id>).
--
-- Pointwise off-field fact (design §6 rule 5): market value is a career-long
-- timeline, NOT season-bound — so NO league/season columns and NO partitioning
-- (like fct_team_elo). `source` in the PK keeps a FotMob and a Transfermarkt
-- point on the same (player, date) from colliding.
--
-- Cross-season collapse: both sources re-emit the full history in every ingest
-- snapshot, so the same (player, date) point lands in several season partitions
-- of Silver. ROW_NUMBER over the design PK keeps one row (freshest ingest) —
-- without it the dropped (league, season) grain would leave cross-season dups.
--
-- Lossless orphan policy (#871): a missing or ambiguous xref must not delete a
-- valuation.  Source-prefixed ids are stable, collision-safe across sources,
-- and remain visible to the soft dim_player FK DQ until xref resolves them.
--
-- PK:           (player_id, valuation_date, source)
-- FK:           player_id -> dim_player (soft, WARNING rate-mode)
-- Partitioning: none (small off-field table, no season key)
-- =============================================================================

WITH transfermarkt AS (
    SELECT
        COALESCE(
            tm.canonical_id,
            CONCAT('tm_', CAST(tm.player_id AS varchar))
        )                                                 AS player_id,
        tm.mv_date                                        AS valuation_date,
        tm.value_eur                                      AS market_value_eur,
        CAST('EUR' AS varchar)                            AS currency,
        CAST('transfermarkt' AS varchar)                  AS source,
        CAST(tm._bronze_ingested_at AS timestamp(6))      AS _bronze_ingested_at
    FROM iceberg.silver.transfermarkt_market_value_history tm
    WHERE tm.player_id IS NOT NULL
      AND tm.mv_date IS NOT NULL
),

unioned AS (
    -- #1590: the FotMob branch (legacy FotMob Silver market-value history) was
    -- removed; source is now always 'transfermarkt'. The `source` PK column
    -- stays so the schema is unchanged for the new FotMob Silver.
    SELECT * FROM transfermarkt
),

deduped AS (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY u.player_id, u.valuation_date, u.source
            ORDER BY u._bronze_ingested_at DESC
        ) AS rn
    FROM unioned u
)

SELECT
    player_id,
    valuation_date,
    market_value_eur,
    currency,
    source,
    _bronze_ingested_at
FROM deduped
WHERE rn = 1
