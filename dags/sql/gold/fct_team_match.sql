-- =============================================================================
-- Gold: fct_team_match (v2 — 4-source: FBref + Understat + WhoScored + SofaScore)
-- =============================================================================
-- Long-form fact: one row per (match_id, team_id).
--
-- Used by: feat_team_form (rolling), feat_team_h2h, feat_team_event_style.
-- v1 column set (19 columns) is preserved exactly per design doc §6.1 —
-- downstream features INNER-JOIN those columns and ANY rename/drop/type-change
-- would break them.
--
-- Design contract: docs/decisions/T6_team_facts_schema.md §6 (issue #95).
--
-- Sources:
--   iceberg.gold.dim_match                 (spine — FBref canonical)
--   iceberg.silver.fbref_match_enriched    (HARD_FACT spine)
--   iceberg.silver.understat_team_match    (#91 — xg/ppda/deep)
--   iceberg.silver.whoscored_team_match    (#92 — SPADL event aggregates)
--   iceberg.silver.sofascore_team_match    (#93 — passes/duels/corners)
--   iceberg.silver.xref_team / xref_match  (canonical bridge per source)
--   iceberg.bronze.whoscored_schedule      (numeric↔name bridge for WS team_id)
--
-- PK: (match_id, team_id) — preserved from v1
--   match_id = FBref raw hex (== canonical for source='fbref')
--   team_id  = canonical (resolved via xref_team in dim_match)
--
-- Partitioning: (league, season)
--
-- Footguns (CLAUDE.md):
--   * xref JOIN MUST include (league, season) predicate — feedback_xref_join_season_predicate.md
--   * xref_team.season per source: FBref = year-start '2025', US/WS/SS = slug '2526'.
--     dim_match.season = bigint 2025. season_slug computed via LPAD(MOD ...) below.
--   * silver.whoscored_team_match.team_id is NUMERIC varchar ('16'), xref source_id
--     is NAME ('Liverpool'). Bridge via bronze.whoscored_schedule (numeric+name pair)
--     is populated only for season='2021' — WS block is intentionally NULL for current
--     seasons until issue #120 lands canonical resolution in Silver.
-- =============================================================================

WITH
-- ===== xref bridges per source =====
-- FBref team xref: season is varchar year-start ('2025') — used only for completeness
-- (dim_match already exposes canonical team_id, so we don't strictly need this CTE
-- for the spine; kept here documented for reference / symmetry with audit table).
--
-- xref_match for FBref is identity (canonical_id == fbref_match_id == dim_match.match_id),
-- so the FBref match bridge CTE is also omitted.

xref_team_us AS (
    SELECT DISTINCT
        canonical_id,
        source_id   AS us_team_name,
        league,
        season      AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

xref_match_us AS (
    SELECT DISTINCT
        canonical_id  AS match_id_canonical,
        source_id     AS us_match_id,
        league,
        season        AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

xref_team_ws AS (
    SELECT DISTINCT
        canonical_id,
        source_id   AS ws_team_name,
        league,
        season      AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_match_ws AS (
    SELECT DISTINCT
        canonical_id  AS match_id_canonical,
        source_id     AS ws_match_id,
        league,
        season        AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_team_ss AS (
    SELECT DISTINCT
        canonical_id,
        source_id   AS ss_team_name,
        league,
        season      AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

xref_match_ss AS (
    SELECT DISTINCT
        canonical_id  AS match_id_canonical,
        source_id     AS ss_match_id,
        league,
        season        AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

-- ===== WS team_id NUMERIC ↔ NAME bridge =====
-- silver.whoscored_team_match.team_id is numeric ('16'); xref_team.source_id is
-- name ('Liverpool'). bronze.whoscored_schedule has both fields per row but is
-- populated only for season='2021'. For other seasons this CTE returns 0 rows
-- and the LEFT JOIN below leaves the WS block as NULL — acceptable per #95
-- decision (see CLAUDE.md / feedback_whoscored_team_id_numeric_vs_xref_name.md).
ws_team_name_bridge AS (
    SELECT DISTINCT
        CAST(CAST(home_team_id AS BIGINT) AS varchar) AS ws_team_id_numeric,
        home_team                                     AS ws_team_name,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE home_team_id IS NOT NULL

    UNION

    SELECT DISTINCT
        CAST(CAST(away_team_id AS BIGINT) AS varchar),
        away_team,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE away_team_id IS NOT NULL
),

-- ===== v1 spine — FBref home + away (preserved as-is) =====
home AS (
    SELECT
        dm.match_id,
        dm.date,
        dm.season,
        dm.league,
        dm.gameweek,
        dm.home_team_id    AS team_id,
        dm.away_team_id    AS opponent_id,
        TRUE               AS is_home,
        m.home_score       AS goals_for,
        m.away_score       AS goals_against,
        m.home_shots       AS shots,
        m.home_sot         AS shots_on_target,
        m.home_possession  AS possession,
        m.home_yellow_cards AS yellow_cards,
        m.home_red_cards   AS red_cards,
        m.home_saves       AS saves,
        dm.is_completed
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched m ON m.match_id = dm.match_id
),
away AS (
    SELECT
        dm.match_id,
        dm.date,
        dm.season,
        dm.league,
        dm.gameweek,
        dm.away_team_id    AS team_id,
        dm.home_team_id    AS opponent_id,
        FALSE              AS is_home,
        m.away_score       AS goals_for,
        m.home_score       AS goals_against,
        m.away_shots       AS shots,
        m.away_sot         AS shots_on_target,
        m.away_possession  AS possession,
        m.away_yellow_cards AS yellow_cards,
        m.away_red_cards   AS red_cards,
        m.away_saves       AS saves,
        dm.is_completed
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched m ON m.match_id = dm.match_id
),
unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
),

-- Compute season slug from dim_match.season (bigint year-start, e.g. 2025 → '2526')
-- for cross-source JOINs against xref_match / silver US/WS/SS team_match tables.
unioned_with_slug AS (
    SELECT
        u.*,
        LPAD(CAST(MOD(u.season,     100) AS varchar), 2, '0')
            || LPAD(CAST(MOD(u.season + 1, 100) AS varchar), 2, '0') AS season_slug
    FROM unioned u
)

SELECT
    -- ===== v1 columns — preserved exactly (backwards-compat invariant) =====
    u.match_id,
    u.team_id,
    u.opponent_id,
    u.date,
    u.gameweek,
    u.is_home,
    u.goals_for,
    u.goals_against,
    u.shots,
    u.shots_on_target,
    u.possession,
    u.yellow_cards,
    u.red_cards,
    u.saves,
    CASE
        WHEN u.goals_for > u.goals_against THEN 3
        WHEN u.goals_for = u.goals_against THEN 1
        ELSE 0
    END AS points,
    CASE
        WHEN u.goals_for > u.goals_against THEN 'W'
        WHEN u.goals_for = u.goals_against THEN 'D'
        ELSE 'L'
    END AS result,
    u.is_completed,

    -- ===== v2 MODELED — xG / xA (Understat primary per RX2; SS fallback) =====
    ROUND(COALESCE(us.xg,          ss.expected_goals),         4) AS expected_goals,
    ROUND(COALESCE(us.xg_against,  ss.expected_goals_against), 4) AS expected_goals_against,
    -- expected_assists: neither silver.understat_team_match nor silver.sofascore_team_match
    -- exposes team-grain xA. Column reserved per design doc §6.2 for Phase 2 FotMob (#97);
    -- emits NULL until source data is available.
    CAST(NULL AS DOUBLE)                                          AS expected_assists,
    ROUND(us.npxg, 4)                                             AS npxg,

    -- ===== v2 UNIQUE_UNDERSTAT (pressing / depth) =====
    us.ppda,
    us.deep_completions,

    -- ===== v2 UNIQUE_WHOSCORED (SPADL event aggregates) =====
    -- NULL for current seasons until issue #120 lands WS canonical resolve.
    ws.pass_total,
    ws.pass_ok,
    ws.pass_pct,
    ws.tackle_att,
    ws.tackle_won,
    ws.takeon_att,
    ws.takeon_won,
    ws.touches_in_box,
    ws.key_passes_ws,

    -- ===== v2 UNIQUE_SOFASCORE (passing / duels / breakdowns) =====
    ss.total_passes,
    ss.accurate_passes,
    ss.accurate_passes_pct,
    ss.corner_kicks,
    ss.fouls           AS fouls_ss,
    ss.offsides        AS offsides_ss,
    ss.ground_duels_won,
    ss.aerial_duels_won,

    -- ===== Lineage =====
    CURRENT_TIMESTAMP                                             AS _gold_created_at,

    -- ===== Partition keys (LAST in SELECT) =====
    u.league,
    u.season

FROM unioned_with_slug u

-- ===== Understat bridge (LEFT) =====
-- silver.understat_team_match already exposes team_id_canonical (resolved inside
-- the Silver SQL), so we only need a match-bridge here.
LEFT JOIN xref_match_us xmu
    ON  xmu.match_id_canonical = u.match_id
    AND xmu.league             = u.league
    AND xmu.season_slug        = u.season_slug
LEFT JOIN iceberg.silver.understat_team_match us
    ON  us.match_id          = xmu.us_match_id
    AND us.team_id_canonical = u.team_id
    AND us.league            = u.league
    AND us.season            = u.season_slug

-- ===== WhoScored bridge (LEFT, fail-soft via ws_team_name_bridge) =====
LEFT JOIN xref_team_ws xtw
    ON  xtw.canonical_id = u.team_id
    AND xtw.league       = u.league
    AND xtw.season_slug  = u.season_slug
LEFT JOIN ws_team_name_bridge wsb
    ON  wsb.ws_team_name = xtw.ws_team_name
    AND wsb.league       = u.league
    AND wsb.season       = u.season_slug
LEFT JOIN xref_match_ws xmw
    ON  xmw.match_id_canonical = u.match_id
    AND xmw.league             = u.league
    AND xmw.season_slug        = u.season_slug
LEFT JOIN iceberg.silver.whoscored_team_match ws
    ON  ws.match_id = xmw.ws_match_id
    AND ws.team_id  = wsb.ws_team_id_numeric
    AND ws.league   = u.league
    AND ws.season   = u.season_slug

-- ===== SofaScore bridge (LEFT) =====
-- silver.sofascore_team_match.team_id is the team NAME, so we JOIN xref_team
-- directly (source_id = name).
LEFT JOIN xref_team_ss xts
    ON  xts.canonical_id = u.team_id
    AND xts.league       = u.league
    AND xts.season_slug  = u.season_slug
LEFT JOIN xref_match_ss xms
    ON  xms.match_id_canonical = u.match_id
    AND xms.league             = u.league
    AND xms.season_slug        = u.season_slug
LEFT JOIN iceberg.silver.sofascore_team_match ss
    ON  ss.match_id = xms.ss_match_id
    AND ss.team_id  = xts.ss_team_name
    AND ss.league   = u.league
    AND ss.season   = u.season_slug

WHERE u.match_id IS NOT NULL
  AND u.team_id  IS NOT NULL
