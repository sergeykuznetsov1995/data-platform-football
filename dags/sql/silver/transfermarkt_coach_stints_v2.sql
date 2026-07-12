-- =============================================================================
-- Shadow Silver v2: Transfermarkt-declared coach employment stints
-- =============================================================================
-- Grain: (club_id, coach_id, appointed_date, left_date).  One bound may be
-- nullable, but rows with both bounds missing are rejected by Bronze DQ.
-- This table is intentionally NOT unioned into gold.fct_manager_stint, whose
-- established contract is FBref match-derived.

WITH coach_xref AS (
    SELECT
        CAST(source_id AS varchar) AS coach_id,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS canonical_id
    FROM iceberg.silver.xref_manager
    WHERE source = 'transfermarkt'
    GROUP BY CAST(source_id AS varchar)
),

team_xref AS (
    SELECT
        source_id AS club_name,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS canonical_id
    FROM iceberg.silver.xref_team
    WHERE source = 'transfermarkt'
    GROUP BY source_id
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY club_id, coach_id, appointed_date, left_date
                ORDER BY _ingested_at DESC, _batch_id DESC,
                         source_body_hash DESC
            ) AS rn
        FROM iceberg.bronze.transfermarkt_coach_stints b
        WHERE club_id IS NOT NULL
          AND coach_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    CAST(b.club_id AS varchar)                            AS club_id,
    tx.canonical_id                                      AS team_id,
    b.club_name,
    CAST(b.coach_id AS varchar)                           AS coach_id,
    cx.canonical_id                                      AS manager_id,
    b.coach_slug,
    b.name,
    b.role,
    TRY_CAST(b.appointed_date AS date)                    AS appointed_date,
    TRY_CAST(b.left_date AS date)                         AS left_date,
    CAST(b._ingested_at AS timestamp(6))                  AS _bronze_ingested_at,
    b._batch_id,
    b.source_competition_id,
    b.source_edition_id,
    b.source_url,
    b.source_body_hash,
    CAST(b.fetched_at AS timestamp(6))                    AS fetched_at,
    b.parser_revision,
    b.schema_revision,
    b.cycle_id,
    b.scope_id
FROM dedup b
LEFT JOIN coach_xref cx ON cx.coach_id = CAST(b.coach_id AS varchar)
LEFT JOIN team_xref tx ON tx.club_name = b.club_name
