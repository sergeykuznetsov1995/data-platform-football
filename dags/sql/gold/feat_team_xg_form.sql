-- =============================================================================
-- Gold: feat_team_xg_form
-- =============================================================================
-- Rolling per-team xG / PSxG features computed PRE-match (point-in-time safe).
--
-- xG is one of the strongest single-feature predictors of football match
-- outcomes. FBref shot-level xG (per-shot expected goals from a logistic
-- model trained on shot location, body part, etc.) is aggregated to
-- per-(match, team) totals and then rolled with two windows:
--
--   * L5  — last 5 completed matches (high-recency form)
--   * L10 — last 10 completed matches (medium-term level)
--
-- Window: ROWS BETWEEN N PRECEDING AND 1 PRECEDING — excludes current match
-- to prevent target leakage. Additionally we mask the first match_rn rows
-- per (team_id, season) to NULL, so downstream consumers can't accidentally
-- treat partially-warm rolling means as full ones.
--
-- Mask thresholds:
--   * L5  averages → require match_rn > 5
--   * L10 averages → require match_rn > 5 (NOT > 10)
--     Rationale: APL season is 38 matches. Demanding 10 prior would leave
--     ~26% of rows with NULL L10 features, hurting model coverage. With
--     match_rn > 5 the L10 window is "up to 10 prior, at least 5" — same
--     honesty guarantee as L5 but with broader value range. Trade-off is
--     explicit and shared with feat_team_form.
--
-- Data source:
--   iceberg.silver.fbref_shot_events  (shot-level: match_id, team(name), xg, psxg)
--   iceberg.gold.fct_team_match       (provides team_id, opponent_id, date, season per (match,team))
--   iceberg.gold.dim_match            (provides home/away team names → team_id mapping)
--
-- Why fct_team_match is the spine:
--   shot_events.team holds the team NAME (varchar) but feat tables key by
--   team_id. fct_team_match already provides one row per (match_id, team_id)
--   with date+season — perfect spine. We aggregate shots to (match_id, team_name),
--   then JOIN to fct_team_match via dim_match's home/away name→id mapping.
--
-- PK: (match_id, team_id)
-- Partitioning: (league, season)
-- =============================================================================

WITH shot_team_totals AS (
    -- Sum xG / PSxG per (match, shooting team name).
    -- One row per (match_id, team_name) — per-team match totals.
    SELECT
        match_id,
        team                                     AS team_name,
        SUM(COALESCE(xg,   0.0))                 AS xg_total,
        SUM(COALESCE(psxg, 0.0))                 AS psxg_total,
        COUNT(*)                                 AS shots_total
    FROM iceberg.silver.fbref_shot_events
    WHERE match_id IS NOT NULL
      AND team     IS NOT NULL
    GROUP BY match_id, team
),
match_team_xg AS (
    -- Map team_name -> team_id via dim_match ids + the Silver spine's FBref
    -- team names (#425: dim_match no longer carries denormalised names —
    -- sm.home/sm.away hold the SAME FBref short forms the old columns did,
    -- so the name-keyed JOIN to shot_team_totals is unchanged).
    SELECT
        dm.match_id,
        dm.match_date,
        dm.season,
        dm.league,
        dm.home_team_id  AS team_id,
        dm.away_team_id  AS opponent_id,
        sm.home          AS team_name,
        sm.away          AS opponent_name
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched sm ON sm.match_id = dm.match_id
    UNION ALL
    SELECT
        dm.match_id,
        dm.match_date,
        dm.season,
        dm.league,
        dm.away_team_id,
        dm.home_team_id,
        sm.away,
        sm.home
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched sm ON sm.match_id = dm.match_id
),
team_match_xg AS (
    -- One row per (match_id, team_id) with both xGF (own shots) and xGA
    -- (opponent shots). LEFT JOINs preserve matches with zero shots from
    -- a side (rare, but possible — abandoned matches, data gaps).
    SELECT
        m.match_id,
        m.team_id,
        m.opponent_id,
        m.match_date,
        m.season,
        m.league,
        COALESCE(own.xg_total,   0.0) AS xg_for,
        COALESCE(opp.xg_total,   0.0) AS xg_against,
        COALESCE(own.psxg_total, 0.0) AS psxg_for,
        COALESCE(opp.psxg_total, 0.0) AS psxg_against
    FROM match_team_xg m
    LEFT JOIN shot_team_totals own
        ON own.match_id = m.match_id
       AND own.team_name = m.team_name
    LEFT JOIN shot_team_totals opp
        ON opp.match_id  = m.match_id
       AND opp.team_name = m.opponent_name
),
ranked AS (
    SELECT
        match_id,
        team_id,
        opponent_id,
        match_date,
        season,
        league,
        xg_for,
        xg_against,
        psxg_for,
        psxg_against,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season
            ORDER BY match_date, match_id
        ) AS match_rn
    FROM team_match_xg
),
rolled AS (
    SELECT
        *,
        -- ----- L5 windows -----
        AVG(xg_for)       OVER w5 AS xg_for_l5_avg_raw,
        AVG(xg_against)   OVER w5 AS xg_against_l5_avg_raw,
        AVG(xg_for - xg_against)     OVER w5 AS xg_diff_l5_avg_raw,
        AVG(psxg_for)     OVER w5 AS psxg_for_l5_avg_raw,
        AVG(psxg_against) OVER w5 AS psxg_against_l5_avg_raw,
        AVG(psxg_for - psxg_against) OVER w5 AS psxg_diff_l5_avg_raw,
        -- ----- L10 windows -----
        AVG(xg_for)       OVER w10 AS xg_for_l10_avg_raw,
        AVG(xg_against)   OVER w10 AS xg_against_l10_avg_raw,
        AVG(xg_for - xg_against)     OVER w10 AS xg_diff_l10_avg_raw,
        AVG(psxg_for)     OVER w10 AS psxg_for_l10_avg_raw,
        AVG(psxg_against) OVER w10 AS psxg_against_l10_avg_raw,
        AVG(psxg_for - psxg_against) OVER w10 AS psxg_diff_l10_avg_raw
    FROM ranked
    WINDOW
        w5 AS (
            PARTITION BY team_id, season
            ORDER BY match_date, match_id
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        w10 AS (
            PARTITION BY team_id, season
            ORDER BY match_date, match_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        )
)
SELECT
    match_id,
    team_id,
    opponent_id,
    match_date,
    match_rn,

    -- ----- L5 features (NULL until at least 5 prior matches exist) -----
    CASE WHEN match_rn > 5 THEN xg_for_l5_avg_raw       END AS xg_for_l5_avg,
    CASE WHEN match_rn > 5 THEN xg_against_l5_avg_raw   END AS xg_against_l5_avg,
    CASE WHEN match_rn > 5 THEN xg_diff_l5_avg_raw      END AS xg_diff_l5_avg,
    CASE WHEN match_rn > 5 THEN psxg_for_l5_avg_raw     END AS psxg_for_l5_avg,
    CASE WHEN match_rn > 5 THEN psxg_against_l5_avg_raw END AS psxg_against_l5_avg,
    CASE WHEN match_rn > 5 THEN psxg_diff_l5_avg_raw    END AS psxg_diff_l5_avg,

    -- ----- L10 features (also masked at >5; window itself extends to 10 prior) -----
    CASE WHEN match_rn > 5 THEN xg_for_l10_avg_raw       END AS xg_for_l10_avg,
    CASE WHEN match_rn > 5 THEN xg_against_l10_avg_raw   END AS xg_against_l10_avg,
    CASE WHEN match_rn > 5 THEN xg_diff_l10_avg_raw      END AS xg_diff_l10_avg,
    CASE WHEN match_rn > 5 THEN psxg_for_l10_avg_raw     END AS psxg_for_l10_avg,
    CASE WHEN match_rn > 5 THEN psxg_against_l10_avg_raw END AS psxg_against_l10_avg,
    CASE WHEN match_rn > 5 THEN psxg_diff_l10_avg_raw    END AS psxg_diff_l10_avg,

    league,
    season
FROM rolled
