-- =============================================================================
-- Gold: fct_player_market_value  (issue #430 — two sources + source in PK)
-- =============================================================================
-- One valuation point per (player_id, valuation_date, source).
-- Two sources, both kept side by side — we do NOT pick a "correct" one here
-- (that is a feature decision, floor 2):
--
--   fotmob        — silver.fotmob_player_market_value_history (canonical_id is
--                   resolved when xref has one unambiguous match; otherwise a
--                   stable fm_<source_id> is retained).
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
-- ⚠️ xref JOIN MUST include (league, season) predicate (CLAUDE.md footgun):
--   silver.xref_player has per-(source, source_id, season) rows; without the
--   season condition the FotMob join fans out 1.5-4×. Season is a varchar slug
--   '2526' on both sides after #404 (slug = slug).
--
-- PK:           (player_id, valuation_date, source)
-- FK:           player_id -> dim_player (soft, WARNING rate-mode)
-- Partitioning: none (small off-field table, no season key)
-- =============================================================================

WITH xref_fotmob AS (
    SELECT
        source_id    AS fotmob_player_id,
        league,
        season       AS season_year,
        CASE
            WHEN COUNT(DISTINCT canonical_id) = 1 THEN MIN(canonical_id)
            ELSE NULL
        END AS canonical_id
    FROM iceberg.silver.xref_player
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
      AND canonical_id IS NOT NULL
    GROUP BY source_id, league, season
),

fotmob AS (
    SELECT
        COALESCE(
            xfm.canonical_id,
            CONCAT('fm_', CAST(mv.player_id AS varchar))
        )                                                 AS player_id,
        mv.value_date                                     AS valuation_date,
        mv.market_value_eur                               AS market_value_eur,
        mv.currency                                       AS currency,
        CAST('fotmob' AS varchar)                         AS source,
        CAST(mv._bronze_ingested_at AS timestamp(6))      AS _bronze_ingested_at
    FROM iceberg.silver.fotmob_player_market_value_history mv
    LEFT JOIN xref_fotmob xfm
        ON  xfm.fotmob_player_id = mv.player_id
        AND xfm.league           = mv.league
        AND xfm.season_year      = mv.season
    WHERE mv.player_id IS NOT NULL
      AND mv.value_date IS NOT NULL
),

transfermarkt AS (
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
    SELECT * FROM fotmob
    UNION ALL
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
