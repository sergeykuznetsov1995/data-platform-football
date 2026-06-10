-- =============================================================================
-- Gold: fct_player_unavailable
-- =============================================================================
-- One row per (match_id, team_id, player_id_canonical) — players unavailable
-- (confirmed absences) for a given match. Sourced from WhoScored.
--
-- Used by: feat_team_form (l5 rolling unavailability).
--
-- Sources:
--   iceberg.silver.whoscored_player_unavailable  — primary
--   iceberg.gold.dim_match                       — cross-source match bridge
--   iceberg.gold.dim_team                        — team_id validation
--   iceberg.gold.dim_player                      — player_id resolution
--
-- Cross-source resolution (architectural decisions D2 + D4):
--   * match_id: Silver `match_id` is the WhoScored `game` string, NOT the FBref
--     hex slug used by dim_match. We bridge via (match_date, home_slug, away_slug)
--     using the same slug algorithm as `entity_xref` for FBref teams.
--
--   * team_id: canonical slug of the WhoScored team name. Mismatches (e.g.
--     "Wolverhampton" vs "Wolves") surface as NULL team_id and are tracked via
--     the DQ coverage check; future _team_aliases work fixes them.
--
--   * player_id_canonical: LEFT JOIN to dim_player by (player_name, season).
--     On miss — synthetic `'ws_' || ws_player_id` so orphan players are NOT
--     lost (D4). ref_integrity on player_id_canonical is intentionally NOT
--     enforced (E1 xref_player not yet built).
--
-- PK (logical): (match_id, team_id, player_id_canonical)
-- =============================================================================

WITH u AS (
    -- Compute the canonical team slug once — same algorithm as entity_xref
    -- for FBref teams, so the match-bridge join below aligns slug universes.
    SELECT
        match_date,
        league,
        season,
        team_name,
        ws_player_id,
        player_name,
        reason,
        _bronze_ingested_at,
        -- Strip diacritics before slugging (issue #215) so the slug universe
        -- aligns with entity_xref / dim_match regardless of accent spelling.
        LOWER(REGEXP_REPLACE(
            REGEXP_REPLACE(NORMALIZE(team_name, NFD), '\p{Mn}+', ''),
            '[^a-zA-Z0-9]+', '_')) AS team_slug
    FROM iceberg.silver.whoscored_player_unavailable
    WHERE match_id   IS NOT NULL
      AND match_date IS NOT NULL
),

u_resolved AS (
    SELECT
        u.*,
        dm.match_id AS fbref_match_id
    FROM u
    LEFT JOIN iceberg.gold.dim_match dm
        ON  dm.date    = u.match_date
        AND dm.league  = u.league
        -- #404: dim_match.season is slug now → direct season equality.
        AND dm.season = u.season
        AND (dm.home_team_id = u.team_slug OR dm.away_team_id = u.team_slug)
),

-- A name can map to multiple player_ids in a season (rare); MIN() picks a
-- deterministic one. Conflicts surface via DQ.
dp_lookup AS (
    SELECT
        -- #404: dim_player.season is slug now → pass through.
        season,
        player_name,
        MIN(player_id) AS player_id
    FROM iceberg.gold.dim_player
    WHERE player_name IS NOT NULL
      AND player_id   IS NOT NULL
    GROUP BY season, player_name
)

SELECT
    ur.fbref_match_id                                       AS match_id,
    ur.match_date,

    dt.team_id                                              AS team_id,
    ur.team_name                                            AS team_name_raw,

    -- Orphan-safe player resolution (D4) — never NULL.
    COALESCE(dp.player_id, 'ws_' || CAST(ur.ws_player_id AS VARCHAR))
                                                            AS player_id_canonical,
    ur.ws_player_id,
    ur.player_name,

    ur.reason,
    ur._bronze_ingested_at                                  AS _silver_ingested_at,

    ur.league,
    -- season — varchar slug ('2526'); all dim JOINs above are now slug = slug (#404).
    ur.season

FROM u_resolved ur
LEFT JOIN iceberg.gold.dim_team dt
    ON  dt.team_id = ur.team_slug
    AND dt.league  = ur.league
    -- #404: dim_team.season is slug now → direct season equality.
    AND dt.season = ur.season
LEFT JOIN dp_lookup dp
    ON  dp.player_name = ur.player_name
    AND dp.season      = ur.season
-- Drop rows where the cross-source bridge failed; keeps ref_integrity on match_id.
WHERE ur.fbref_match_id IS NOT NULL
