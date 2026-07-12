-- Shadow Gold v2: canonical manager dimension enriched from global TM profiles.
-- Transfermarkt stints are deliberately not mixed into fct_manager_stint.

WITH managers AS (
    SELECT
        canonical_id AS manager_id,
        COALESCE(
            MAX(display_name) FILTER (WHERE source = 'fbref'),
            MAX(display_name)
        ) AS manager_name
    FROM iceberg.silver.xref_manager
    WHERE NOT (source = 'transfermarkt' AND confidence = 'orphan')
    GROUP BY canonical_id
),

xref_fotmob AS (
    SELECT canonical_id, source_id AS fotmob_coach_id
    FROM (
        SELECT
            canonical_id,
            source_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_id ORDER BY season DESC
            ) AS rn
        FROM iceberg.silver.xref_manager
        WHERE source = 'fotmob'
          AND confidence <> 'orphan'
    )
    WHERE rn = 1
),

fotmob_manager AS (
    SELECT
        player_id,
        MAX_BY(nationality, season)                       AS nationality,
        MAX_BY(date_of_birth, season)                     AS date_of_birth
    FROM iceberg.silver.fotmob_manager_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
),

xref_tm_source AS (
    SELECT
        CAST(source_id AS varchar)                        AS coach_id,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END                                               AS canonical_id
    FROM iceberg.silver.xref_manager
    WHERE source = 'transfermarkt'
    GROUP BY CAST(source_id AS varchar)
),

xref_tm AS (
    SELECT coach_id, canonical_id
    FROM (
        SELECT
            coach_id,
            canonical_id,
            COUNT(*) OVER (PARTITION BY canonical_id) AS coach_ids_per_canonical
        FROM xref_tm_source
        WHERE canonical_id IS NOT NULL
    )
    WHERE coach_ids_per_canonical = 1
),

tm_manager AS (
    SELECT
        x.canonical_id,
        p.nationality,
        p.dob
    FROM iceberg.silver.transfermarkt_coach_profiles_v2 p
    JOIN xref_tm x ON x.coach_id = p.coach_id
    WHERE x.canonical_id IS NOT NULL
)

SELECT
    m.manager_id,
    m.manager_name,
    COALESCE(fm.nationality, tm.nationality)               AS nationality,
    COALESCE(TRY_CAST(fm.date_of_birth AS date), tm.dob)   AS dob
FROM managers m
LEFT JOIN xref_fotmob xf ON xf.canonical_id = m.manager_id
LEFT JOIN fotmob_manager fm ON fm.player_id = xf.fotmob_coach_id
LEFT JOIN tm_manager tm ON tm.canonical_id = m.manager_id
