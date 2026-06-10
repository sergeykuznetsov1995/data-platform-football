-- =============================================================================
-- Gold: fct_lineup
-- =============================================================================
-- Per-player lineup entries unified across FBref and ESPN.
--
-- Sources:
--   iceberg.silver.fbref_match_lineups   — primary (complete coverage)
--   iceberg.silver.espn_lineup           — secondary (E3.2 deliverable)
--   iceberg.silver.xref_match            — match_id resolution (FBref-only in MVP)
--   iceberg.silver.xref_team             — team alias canonicalisation
--   iceberg.silver.xref_player           — player canonical id (FBref/US/WS only)
--   iceberg.silver.fbref_match_enriched  — bridge spine for ESPN → FBref match_id
--   iceberg.bronze.espn_schedule         — provides `date` for bridge JOIN
--
-- PK:           (match_id_canonical, player_id_canonical)
-- Partitioning: (league, season)  — applied externally by Python CTAS.
--
-- =============================================================================
-- ADR — match_id bridging (ESPN ↔ FBref)
-- =============================================================================
--   silver.xref_match in E1 MVP carries `source = 'fbref'` ONLY (Phase B
--   cross-source bridging is the planned follow-up but not yet shipped —
--   see project_medallion_e1.md "Open follow-ups"). This file therefore
--   implements the bridge inline so fct_lineup can ship now.
--
--   Strategy:
--     * FBref rows  -> JOIN silver.xref_match (source='fbref') for canonical_id
--                       (1:1, exact, lossless).
--     * ESPN rows   -> bridge via (date, home_canonical_id, away_canonical_id)
--                       to silver.fbref_match_enriched (which carries the
--                       authoritative FBref match_id). The bridge CTE rebuilds
--                       ESPN's deterministic match_id (`espn_<xxhash64>` from
--                       (league, season, game) — the same xxhash seed used in
--                       silver/espn_lineup.sql) and returns
--                       (espn_match_id, fbref_match_id_canonical).
--                       Where the bridge fails (no date / no team-canonical
--                       match), the row is kept with match_id_canonical = the
--                       ESPN pseudo-id ('espn_<hash>'), preserving lineage and
--                       letting downstream DQ surface the orphan rate.
--
--   Why inline (not via xref_match Phase B):
--     * Phase B requires a resolved xref_team to fuzzy-match teams; xref_team
--       already exists, so the bridge is composable here without touching
--       xref_match. When Phase B ships, this CTE is the migration target —
--       just replace it with a JOIN on silver.xref_match WHERE source='espn'.
--     * Keeps the SQL self-contained and ships unblocked.
--
-- =============================================================================
-- ADR — player_id resolution (ESPN ↔ canonical)
-- =============================================================================
--   xref_player covers FBref / Understat / WhoScored (R2 prototype scope).
--   ESPN is NOT in that resolver — silver.espn_lineup.player_id is NULL,
--   and there is no (name, team) → canonical_id mapping for ESPN players
--   in production today. Behaviour:
--
--     * FBref rows -> LEFT JOIN xref_player (source='fbref') -> 'fb_<id>'.
--     * ESPN rows  -> player_id_canonical = NULL.
--
--   To prevent the dedup PARTITION from collapsing distinct ESPN players
--   sharing a NULL canonical_id, we synthesise a *partition key only*
--   (`_dedup_player_key = COALESCE(player_id_canonical, 'es_' || hash(...))`)
--   that is used solely inside ROW_NUMBER and dropped from the projection.
--   The output column `player_id_canonical` stays NULL for unresolved ESPN
--   rows, so downstream consumers can WHERE-filter or join cautiously.
--
-- =============================================================================
-- ADR — Dedup priority FBref > ESPN
-- =============================================================================
--   When the same (match, player) is covered by both sources we keep the
--   FBref row (more complete schema: real player_id, jersey_number).
--   Implementation: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY
--   source_priority ASC, _bronze_ingested_at DESC), source_priority=1 for
--   FBref, 2 for ESPN. Tie-breaker on _bronze_ingested_at retains the
--   freshest row within a single source.
--
--   Edge case: cross-source dedup only fires when the ESPN→FBref match_id
--   bridge succeeds AND the ESPN player resolves to a canonical_id. Today
--   the second condition is never true (no ESPN player resolver), so
--   FBref + ESPN rows for the same match coexist as separate
--   player_id_canonical groups. That is the correct behaviour until an
--   ESPN player resolver lands in E1.5+ — fct_lineup acts as a UNION ALL
--   and DQ can track the (match × team) starter-count as a sanity probe.
--
-- =============================================================================
-- Output schema (frozen for E3.5)
-- =============================================================================
--   match_id_canonical    varchar  -- via xref_match (FBref) / bridge (ESPN)
--   team_id_canonical     varchar  -- via xref_team — NULL-tolerant
--   player_id_canonical   varchar  -- via xref_player (FBref); NULL for ESPN
--   player_name           varchar  -- passthrough
--   is_starter            boolean
--   position_canonical    varchar  -- raw passthrough — no canonicalisation in MVP
--                                     (FBref Silver also keeps raw position;
--                                      dim_position deferred until used)
--   jersey_number         integer  -- NULL for ESPN (matchsheet has no jersey)
--   lineup_source         varchar  -- 'fbref' | 'espn' (winner after dedup)
--   lineup_version        varchar  -- literal 'v1' (schema versioning hook)
--   league                varchar
--   season                varchar  -- UNIFIED 4-char slug 'YYYY' (e.g. '2425' for 2024-25 season)
--                                     #404: every Silver/xref/gold season is now the varchar
--                                     slug, so both branches read the slug from Silver and pass
--                                     it through — no per-file format()/mod() conversion. Output
--                                     is comparable to ``fct_event.season`` / ``fct_shot.season``
--                                     / ``xref_team.season`` (all varchar slug).
--
--   _silver_created_at    -- NOT projected here; obvyazka adds it.
-- =============================================================================

WITH
-- ============================================================================
-- 1) ESPN → FBref match_id bridge
-- ============================================================================
-- Reconstructs the same xxhash64 seed used by silver/espn_lineup.sql and
-- joins to fbref_match_enriched via (date, home_canonical_id, away_canonical_id).
-- xref_team.canonical_id makes the team-name comparison alias-tolerant.
-- Pre-aggregate xref_team distinct (canonical_id, source_id) pairs WITHIN
-- a league. After #404 xref_team.season is slug for all sources, but
-- canonical_id is league-scoped stable so dropping season from the JOIN here
-- is safe anyway; cross-league ambiguity is impossible because the JOIN to
-- fbref_match_enriched re-applies the league predicate.
xref_team_by_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league
    FROM iceberg.silver.xref_team
    WHERE canonical_id IS NOT NULL
),
-- Collapse silver.xref_player (PK source, source_id, season) to one row per
-- (source, source_id) so the FBref player JOIN below stays 1:1 — see the
-- rationale on that JOIN. Mirrors the fix pattern in fct_shot.sql (#204).
xref_player_dedup AS (
    SELECT source, source_id, ARBITRARY(canonical_id) AS canonical_id
    FROM iceberg.silver.xref_player
    GROUP BY source, source_id
),
espn_match_bridge AS (
    SELECT
        -- Deterministic ESPN match_id (must mirror silver/espn_lineup.sql).
        'espn_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
            COALESCE(es.league, '')
            || '|' || COALESCE(CAST(es.season AS varchar), '')
            || '|' || COALESCE(es.game, '')
        ))))                                                AS espn_match_id,
        fme.match_id                                        AS fbref_match_id,
        es.league                                           AS league,
        es.season                                           AS season  -- #404: ESPN bronze slug ('2526')
    FROM iceberg.bronze.espn_schedule es
    LEFT JOIN xref_team_by_canonical xt_home_es
        ON xt_home_es.source    = 'espn'
       AND xt_home_es.source_id = es.home_team
       AND xt_home_es.league    = es.league
    LEFT JOIN xref_team_by_canonical xt_away_es
        ON xt_away_es.source    = 'espn'
       AND xt_away_es.source_id = es.away_team
       AND xt_away_es.league    = es.league
    LEFT JOIN xref_team_by_canonical xt_home_fb
        ON xt_home_fb.source       = 'fbref'
       AND xt_home_fb.canonical_id = xt_home_es.canonical_id
       AND xt_home_fb.league       = es.league
    LEFT JOIN xref_team_by_canonical xt_away_fb
        ON xt_away_fb.source       = 'fbref'
       AND xt_away_fb.canonical_id = xt_away_es.canonical_id
       AND xt_away_fb.league       = es.league
    LEFT JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.league = es.league
       AND fme.home   = xt_home_fb.source_id
       AND fme.away   = xt_away_fb.source_id
       -- bronze.espn_schedule.match_date is timestamp(6); fme.date is date.
       -- Compare on the date part. Rescheduling within a day is rare for
       -- APL fixtures and surfaces as bridge-miss in DQ.
       AND fme.date   = CAST(es.match_date AS date)
    WHERE es.game IS NOT NULL
),

-- ============================================================================
-- 2) FBref source rows — match_id via silver.xref_match (source='fbref')
-- ============================================================================
fbref_resolved AS (
    SELECT
        -- xref_match canonical_id == FBref match_id for source='fbref'.
        -- LEFT JOIN tolerates rows where Bronze schedule hasn't been ingested
        -- yet (lineup arrives before schedule for fresh fixtures); fall back
        -- to the raw match_id so the row is preserved.
        COALESCE(xm.canonical_id, fl.match_id)         AS match_id_canonical,
        xt.canonical_id                                AS team_id_canonical,
        xp.canonical_id                                AS player_id_canonical,
        fl.player                                      AS player_name,
        fl.is_starter                                  AS is_starter,
        fl.position                                    AS position_canonical,
        fl.jersey_number                               AS jersey_number,
        fl._bronze_ingested_at                         AS _bronze_ingested_at,
        fl.league                                      AS league,
        -- #404: silver.fbref_match_lineups.season is slug ('2425') now → pass through.
        fl.season                                      AS season,
        'fbref'                                        AS lineup_source,
        1                                              AS source_priority,
        fl.player_id                                   AS _raw_player_id_for_dedup
    FROM iceberg.silver.fbref_match_lineups fl
    LEFT JOIN iceberg.silver.xref_match xm
        ON xm.source    = 'fbref'
       AND xm.source_id = fl.match_id
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fl.team
       AND xt.league    = fl.league
       AND xt.season    = CAST(fl.season AS varchar)
    -- xref_player_dedup (NOT raw silver.xref_player): xref_player PK is
    -- (source, source_id, season), so a raw JOIN on (source, source_id) fans
    -- out 1.5-4× over a player's seasons (#205; same footgun fixed in
    -- fct_shot.sql #204). A season predicate would remove the fan-out but
    -- ALSO drop ~36% of canonical assignments — FBref canonical_id is
    -- 'fb_' || player_id (season-independent), so a player resolved only in
    -- one in-scope season legitimately carries that canonical_id on lineup
    -- rows for seasons outside resolver scope. The dedup CTE collapses to one
    -- row per (source, source_id) via ARBITRARY (safe: 0 FBref source_id maps
    -- to >1 canonical_id), eliminating the fan-out while preserving coverage.
    LEFT JOIN xref_player_dedup xp
        ON xp.source    = 'fbref'
       AND xp.source_id = fl.player_id
),

-- ============================================================================
-- 3) ESPN source rows — match_id via espn_match_bridge, falls back to raw
--    'espn_<hash>' pseudo-id when bridge JOIN misses.
-- ============================================================================
espn_resolved AS (
    SELECT
        COALESCE(emb.fbref_match_id, el.match_id)      AS match_id_canonical,
        xt.canonical_id                                AS team_id_canonical,
        -- ESPN player resolver does not exist yet (R2 spike covers FB/US/WS).
        -- Surface NULL; downstream DQ tracks "ESPN orphan rate".
        CAST(NULL AS varchar)                          AS player_id_canonical,
        el.player                                      AS player_name,
        el.is_starter                                  AS is_starter,
        el.position                                    AS position_canonical,
        el.jersey_number                               AS jersey_number,
        el._bronze_ingested_at                         AS _bronze_ingested_at,
        el.league                                      AS league,
        -- season UNIFICATION (E3.5 R4): ESPN Silver stores a varchar slug
        -- (``2425`` for 2024-25, see silver/espn_lineup.sql). CAST AS varchar is
        -- now a no-op kept for clarity; lpad guards against degenerate ``525`` →
        -- '0525' (defensive — production data is always 4-digit, lpad is cheap).
        lpad(CAST(el.season AS varchar), 4, '0')        AS season,
        'espn'                                         AS lineup_source,
        2                                              AS source_priority,
        -- Compose a stable per-row dedup proxy from (player, team) so that
        -- two distinct ESPN players with NULL canonical don't collapse into
        -- one ROW_NUMBER bucket. NOT projected to output.
        el.player                                      AS _raw_player_id_for_dedup
    FROM iceberg.silver.espn_lineup el
    LEFT JOIN espn_match_bridge emb
        ON emb.espn_match_id = el.match_id
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'espn'
       AND xt.source_id = el.team
       AND xt.league    = el.league
       AND xt.season    = CAST(el.season AS varchar)
),

-- ============================================================================
-- 4) UNION + dedup
-- ============================================================================
all_lineups AS (
    SELECT * FROM fbref_resolved
    UNION ALL
    SELECT * FROM espn_resolved
),

dedup AS (
    SELECT
        all_lineups.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                match_id_canonical,
                -- Dedup key precedence:
                --   1. player_id_canonical when present  -> enables
                --      cross-source dedup (FBref wins via source_priority).
                --   2. otherwise (lineup_source || team || raw_native_id) ->
                --      keeps namesake / unresolved players distinct WITHIN
                --      a source.
                --
                -- xref_player has limited season coverage today (only
                -- competitions.yaml seasons are resolved — see
                -- project_medallion_e1.md). For seasons outside the
                -- resolver scope, FBref player_id_canonical is NULL even
                -- though fl.player_id is valid; falling back to
                -- player_id_canonical alone would collapse all such
                -- "unresolved" FBref players in a match into one bucket.
                CASE
                    WHEN player_id_canonical IS NOT NULL
                        THEN player_id_canonical
                    ELSE
                        lineup_source || ':' ||
                        COALESCE(team_id_canonical, '?') || ':' ||
                        COALESCE(_raw_player_id_for_dedup, player_name, '?')
                END
            ORDER BY
                source_priority ASC,
                _bronze_ingested_at DESC
        ) AS rn
    FROM all_lineups
)

SELECT
    match_id_canonical,
    team_id_canonical,
    player_id_canonical,
    player_name,
    is_starter,
    position_canonical,
    jersey_number,
    lineup_source,
    'v1'                            AS lineup_version,
    league,
    season
FROM dedup
WHERE rn = 1
  AND match_id_canonical IS NOT NULL
