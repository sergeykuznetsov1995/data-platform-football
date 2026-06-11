-- =============================================================================
-- Gold: fct_player_unavailable
-- =============================================================================
-- One row per (match_id, player_id) — players unavailable (confirmed absences)
-- for a given match. Sourced from WhoScored.
--
-- Design contract: docs/design/gold-star-schema.md §4.6 (issue #426).
--   reason — enum injury | suspension | other (raw source label kept in detail).
--   PK verified duplicate-free on (match_id, player_id) pre-cutover (#426 step 0).
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
--   * player_id: LEFT JOIN to dim_player by (player_name, season).
--     On miss — synthetic `'ws_' || ws_player_id` so orphan players are NOT
--     lost (D4). ref_integrity on player_id is intentionally NOT
--     enforced (E1 xref_player not yet built).
--
-- PK (logical): (match_id, player_id)
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
        ON  dm.match_date = u.match_date  -- #425: dim_match renamed date->match_date
        AND dm.league  = u.league
        -- #404: dim_match.season is slug now → direct season equality.
        AND dm.season = u.season
        AND (dm.home_team_id = u.team_slug OR dm.away_team_id = u.team_slug)
),

-- A name can map to multiple player_ids (rare); MIN() picks a deterministic
-- one. Conflicts surface via DQ. #425: dim_player is one row per player —
-- the lookup is cross-season now (same-name collisions across seasons were
-- already collapsed by MIN within a season; the blast radius is unchanged
-- in practice and the resolution is replaced by xref in #426).
dp_lookup AS (
    SELECT
        player_name,
        MIN(player_id) AS player_id
    FROM iceberg.gold.dim_player
    WHERE player_name IS NOT NULL
      AND player_id   IS NOT NULL
    GROUP BY player_name
)

SELECT
    ur.fbref_match_id                                       AS match_id,

    dt.team_id                                              AS team_id,

    -- Orphan-safe player resolution (D4) — never NULL.
    COALESCE(dp.player_id, 'ws_' || CAST(ur.ws_player_id AS VARCHAR))
                                                            AS player_id,

    -- reason — enum per star design §4.6. Raw WhoScored labels observed
    -- (#426 step 0): injured, suspended, other, unfit, ineligible.
    CASE
        WHEN ur.reason IN ('injured', 'unfit') THEN 'injury'
        WHEN ur.reason = 'suspended'           THEN 'suspension'
        WHEN ur.reason IS NULL                 THEN NULL
        ELSE 'other'
    END                                                     AS reason,
    -- detail — raw source label, provenance for the enum above.
    ur.reason                                               AS detail,

    ur._bronze_ingested_at                                  AS _silver_ingested_at,

    ur.league,
    -- season — varchar slug ('2526'); all dim JOINs above are now slug = slug (#404).
    ur.season

FROM u_resolved ur
-- #425: dim_team is one row per club — league/season left the grain.
LEFT JOIN iceberg.gold.dim_team dt
    ON  dt.team_id = ur.team_slug
LEFT JOIN dp_lookup dp
    ON  dp.player_name = ur.player_name
-- Drop rows where the cross-source bridge failed; keeps ref_integrity on match_id.
WHERE ur.fbref_match_id IS NOT NULL
