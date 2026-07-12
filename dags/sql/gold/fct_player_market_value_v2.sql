-- Shadow Gold v2: two-source global market-value timeline.
-- PK: (player_id, valuation_date, source).  The Transfermarkt branch reads the
-- native global fact directly; no season reconstruction or scrape-context
-- deduplication remains.  Missing or ambiguous xref never deletes a source
-- point: it receives a stable fm_<id> or tm_<id> (#871).

WITH xref_fotmob AS (
    SELECT
        source_id AS fotmob_player_id,
        league,
        season,
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
            x.canonical_id,
            CONCAT('fm_', CAST(mv.player_id AS varchar))
        )                                                AS player_id,
        mv.value_date                                    AS valuation_date,
        mv.market_value_eur,
        mv.currency,
        CAST('fotmob' AS varchar)                        AS source,
        CAST(mv._bronze_ingested_at AS timestamp(6))     AS _bronze_ingested_at
    FROM iceberg.silver.fotmob_player_market_value_history mv
    LEFT JOIN xref_fotmob x
      ON x.fotmob_player_id = mv.player_id
     AND x.league = mv.league
     AND x.season = mv.season
    WHERE mv.player_id IS NOT NULL
      AND mv.value_date IS NOT NULL
),

transfermarkt AS (
    SELECT
        COALESCE(
            canonical_id,
            CONCAT('tm_', CAST(player_id AS varchar))
        )                                                AS player_id,
        mv_date                                          AS valuation_date,
        value_eur                                        AS market_value_eur,
        CAST('EUR' AS varchar)                           AS currency,
        CAST('transfermarkt' AS varchar)                 AS source,
        _bronze_ingested_at
    FROM iceberg.silver.transfermarkt_market_value_points_v2
    WHERE player_id IS NOT NULL
      AND mv_date IS NOT NULL
),

unioned AS (
    SELECT * FROM fotmob
    UNION ALL
    SELECT * FROM transfermarkt
),

dedup AS (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, valuation_date, source
            ORDER BY _bronze_ingested_at DESC
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
FROM dedup
WHERE rn = 1
