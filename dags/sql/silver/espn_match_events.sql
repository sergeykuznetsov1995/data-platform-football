-- =============================================================================
-- Silver: espn_match_events (native v2)
-- =============================================================================
-- Grain/PK: one row per (event_id, seq); seq is ESPN details-array ordinality.
-- Sources (native v2): schedule event rows (event_id bigint, played_final
-- boolean, competition_slug varchar, source_season_year bigint, extra_json
-- varchar, _ingested_at timestamp(6)).
-- Notes: the append-only schedule snapshot is canonicalized before JSON details
-- expansion. Event details has no stable event id, so WITH ORDINALITY is the PK.
-- Footguns: shootout scored penalties are retained but are not match goals;
-- athlete id is on athletesInvolved[0], not athletesInvolved[0].athlete.
-- DAG integration: run_silver_transform wraps this pure SELECT in CTAS.
-- =============================================================================

WITH bronze_src_schedule AS (
    SELECT *
    FROM iceberg.bronze.espn_schedule_generation_v2
),

schedule_dedup AS (
    SELECT *
    FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_schedule b
    )
    WHERE rn = 1
),

event_details AS (
    SELECT
        s.*,
        detail,
        seq
    FROM schedule_dedup s
    CROSS JOIN UNNEST(
        CAST(json_extract(s.extra_json, '$.competition.details') AS array<json>)
    ) WITH ORDINALITY AS u(detail, seq)
    WHERE s.played_final
)

SELECT
    -- ===== Identity / Attributes =====
    s.event_id,
    s.seq,
    json_extract_scalar(s.detail, '$.type.text') AS event_type,
    TRY_CAST(json_extract_scalar(s.detail, '$.type.id') AS bigint) AS event_type_id,
    json_extract_scalar(s.detail, '$.clock.displayValue') AS clock_display,
    TRY_CAST(json_extract_scalar(s.detail, '$.clock.value') AS double) AS clock_seconds,
    TRY_CAST(regexp_extract(json_extract_scalar(s.detail, '$.clock.displayValue'), '^(\\d+)', 1) AS integer) AS minute,
    TRY_CAST(regexp_extract(json_extract_scalar(s.detail, '$.clock.displayValue'), '\\+(\\d+)', 1) AS integer) AS plus_minute,
    TRY_CAST(json_extract_scalar(s.detail, '$.team.id') AS bigint) AS team_id,
    TRY_CAST(json_extract_scalar(s.detail, '$.athletesInvolved[0].id') AS bigint) AS athlete_id,
    json_extract_scalar(s.detail, '$.athletesInvolved[0].displayName') AS player_name,
    TRY_CAST(json_extract_scalar(s.detail, '$.athletesInvolved[0].jersey') AS integer) AS player_jersey,
    json_extract_scalar(s.detail, '$.athletesInvolved[0].position') AS player_position,

    -- ===== HARD_FACT / MODELED =====
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.scoringPlay') AS boolean), false)
        AND NOT COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.shootout') AS boolean), false) AS is_goal,
    TRY_CAST(json_extract_scalar(s.detail, '$.scoreValue') AS integer) AS score_value,
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.ownGoal') AS boolean), false) AS is_own_goal,
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.penaltyKick') AS boolean), false) AS is_penalty,
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.yellowCard') AS boolean), false) AS is_yellow_card,
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.redCard') AS boolean), false) AS is_red_card,
    COALESCE(TRY_CAST(json_extract_scalar(s.detail, '$.shootout') AS boolean), false) AS is_shootout,

    -- ===== Lineage =====
    s._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys =====
    s.competition_slug AS league,
    CAST(s.source_season_year AS varchar) AS season
FROM event_details s
