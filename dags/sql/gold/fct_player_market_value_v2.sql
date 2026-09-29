-- Shadow Gold v2: global market-value timeline (Transfermarkt; the FotMob
-- branch was removed in #1590 with the legacy FotMob Silver layer).
-- PK: (player_id, valuation_date, source).  The Transfermarkt branch reads the
-- native global fact directly; no season reconstruction or scrape-context
-- deduplication remains.  Missing or ambiguous xref never deletes a source
-- point: it receives a stable tm_<id> (#871).

WITH transfermarkt AS (
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
    -- #1590: the FotMob branch (legacy FotMob Silver market-value history) was
    -- removed; source is now always 'transfermarkt'. The `source` PK column
    -- stays so the schema is unchanged for the new FotMob Silver.
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
