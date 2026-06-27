-- =============================================================================
-- Gold: fct_lineup
-- =============================================================================
-- Per-player lineup entries unified across FBref, ESPN, SofaScore, FotMob,
-- WhoScored (#693).
--
-- Sources:
--   iceberg.silver.fbref_match_lineups   — primary (complete coverage)
--   iceberg.silver.sofascore_player_match_aggregate — full lineup source (#693):
--                                          real player_id (resolves via
--                                          xref_player, unlike ESPN), native
--                                          is_starter/is_captain/position
--   iceberg.silver.fotmob_lineup         — full lineup source (#693): starters+
--                                          subs from lineup_json; real player_id,
--                                          is_starter + jersey; is_captain NULL
--   iceberg.silver.whoscored_lineup      — inferred lineup source (#693): real
--                                          player_id + is_starter (appeared & not
--                                          subbed-on); position/captain/jersey NULL
--   iceberg.silver.espn_lineup           — secondary (E3.2 deliverable)
--   iceberg.silver.xref_match            — match_id resolution (FBref-only in MVP)
--   iceberg.silver.xref_team             — team alias canonicalisation
--   iceberg.silver.xref_player           — player canonical id (FBref/US/WS only)
--   iceberg.silver.fbref_match_enriched  — bridge spine for ESPN → FBref match_id
--   iceberg.bronze.espn_schedule         — provides `date` for bridge JOIN
--
-- Design contract: docs/design/gold-star-schema.md §4.5 (issue #426).
-- PK:           (match_id, player_id)
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
--   xref_player covers FBref + 8 other sources incl. ESPN (#692). Behaviour:
--
--     * FBref rows -> LEFT JOIN xref_player_dedup (source='fbref') -> 'fb_<id>'.
--     * ESPN rows  -> LEFT JOIN xref_player (source='espn') keyed on
--                     (display_name, raw_team_name, league, season). ESPN has
--                     NO native player_id, so the resolver matches by name+team
--                     against the FBref spine: 'fb_<id>' when matched,
--                     'es_<player>|<team>' orphan otherwise, NULL when the ESPN
--                     season is outside the resolver's configured scope.
--
--   The dedup PARTITION still synthesises a *partition key only*
--   (`_dedup_player_key = COALESCE(player_id_canonical, 'es_' || hash(...))`)
--   so distinct ESPN players that did NOT resolve (NULL canonical) don't
--   collapse into one ROW_NUMBER bucket. Resolved ESPN rows now carry a real
--   canonical and therefore dedup against their FBref twin (see next ADR).
--
-- =============================================================================
-- ADR — Dedup priority FBref > SofaScore > ESPN (#693)
-- =============================================================================
--   When the same (match, player) is covered by multiple sources we keep the
--   most complete one. Priority by schema richness:
--     1 FBref     — real player_id + jersey_number + position + is_starter
--     2 SofaScore — real player_id + position + is_starter + native is_captain
--                   (no jersey); cross-source dedup against FBref fires via the
--                   shared canonical_id
--     3 FotMob    — real player_id + is_starter + jersey (#693)
--     4 WhoScored — real player_id + is_starter only (#693)
--     5 ESPN      — player_id NOW resolves via xref_player by name+team (#692);
--                   no jersey/captain + lowest schema richness, so ESPN sits at
--                   the tail and never wins cross-source dedup (but resolved
--                   ESPN rows DO dedup away against their FBref twin).
--   Implementation: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY
--   source_priority ASC, _bronze_ingested_at DESC). Tie-breaker on
--   _bronze_ingested_at retains the freshest row within a single source.
--
--   Edge case: cross-source dedup only fires when the ESPN→FBref match_id
--   bridge succeeds AND the ESPN player resolves to a canonical_id (#692
--   enabled the latter). When both hold the FBref row wins and the ESPN twin
--   is dropped — so wiring the ESPN resolver REDUCES the row count for
--   FBref-covered matches (APL is fully FBref-covered). ESPN rows survive
--   only where FBref is missing, the bridge misses, or the player did not
--   resolve. fct_lineup is no longer a blind UNION ALL for overlapping rows.
--
-- =============================================================================
-- Output schema (frozen — star design §4.5)
-- =============================================================================
--   match_id              varchar  -- via xref_match (FBref) / bridge (ESPN)
--   team_id               varchar  -- via xref_team — NULL-tolerant
--   player_id             varchar  -- via xref_player (FBref + ESPN #692);
--                                     NULL when ESPN player unresolved/out-of-scope
--   is_starter            boolean
--   position              varchar  -- raw passthrough — no canonicalisation in MVP
--                                     (FBref Silver also keeps raw position;
--                                      dim_position deferred until used)
--   jersey_number         integer  -- NULL for ESPN (matchsheet has no jersey)
--   is_captain            boolean  -- native on SofaScore rows (#693); FBref rows
--                                     enriched via the SofaScore captain bridge (#439).
--                                     NULL = no coverage (all ESPN rows; FBref players
--                                     absent from SofaScore /lineups)
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
-- #461 (same mechanism as #459 in fct_card/fct_substitution/fct_match_timeline):
-- xref_team is season-grained (PK = source, source_id, league, season; season
-- is the compact slug '2526' for ALL sources since #404). Without the season
-- key one canonical_id expands to every historical FBref name variant; the
-- variant that misses fme.home/away produces a second bridge row with
-- fbref_match_id = NULL and duplicates the lineup under the 'espn_<hash>'
-- pseudo-id (the final dedup partitions by match_id_canonical and cannot
-- collapse the twins).
-- #445: season-scoping alone no longer suffices — xref_team now legally
-- carries TWO same-season fbref spellings per canonical (schedule short name
-- + match-page full name), so espn_match_bridge below aggregates to one row
-- per ESPN game instead of relying on (canonical, league, season) uniqueness.
xref_team_by_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league, season
    FROM iceberg.silver.xref_team
    -- #506: canonical_id IS NOT NULL does NOT exclude orphans — orphan rows
    -- carry a non-NULL source-prefixed canonical ('es_<slug>'/'fb_<slug>').
    -- Add the contract filter so the ESPN→FBref bridge never matches on a
    -- pseudo-canonical. xref_team.sql.j2: orphans excluded from cross-source JOIN.
    WHERE canonical_id IS NOT NULL
      AND confidence <> 'orphan'
),
-- Collapse silver.xref_player (PK source, source_id, season) to one row per
-- (source, source_id) so the FBref player JOIN below stays 1:1 — see the
-- rationale on that JOIN. Mirrors the fix pattern in fct_shot.sql (#204).
xref_player_dedup AS (
    SELECT source, source_id, ARBITRARY(canonical_id) AS canonical_id
    FROM iceberg.silver.xref_player
    GROUP BY source, source_id
),
-- #729: Collapse silver.xref_team (PK source, source_id, league, season) to one
-- row per (source, source_id, league) so the FBref team JOIN below stays
-- season-agnostic — mirrors xref_team_dedup in fct_shot.sql. WHY: xref_team
-- carries a team's SHORT name ('Tottenham') only for the season(s) the bronze
-- schedule emitted it (currently 2526); older seasons hold only the FULL name
-- ('Tottenham Hotspur'). fbref_match_lineups.team uses the short spelling for
-- 1617/2425, so a season-keyed JOIN missed 2967 rows → NULL team_id, breaking
-- the no_nulls(team_id) ERROR gate. canonical_id is season-stable (alias mapping
-- doesn't change across years; 0 fbref source_id maps to >1 canonical), so
-- ARBITRARY is safe and the season key is unnecessary. confidence='orphan'
-- excluded here so the JOIN never leaks a 'fb_<slug>' pseudo-canonical (#506).
xref_team_dedup AS (
    SELECT source, source_id, league, ARBITRARY(canonical_id) AS canonical_id
    FROM iceberg.silver.xref_team
    WHERE confidence <> 'orphan'
    GROUP BY source, source_id, league
),
-- #461: bronze.espn_schedule re-ingests of the same game must collapse to the
-- freshest row BEFORE the bridge — a stale duplicate whose team spelling /
-- date no longer resolves produces a second bridge row with
-- fbref_match_id = NULL, surfacing the lineup twice (hex + pseudo-id).
-- Partition by (league, season, game) — exactly the xxhash64 seed of
-- espn_match_id below — so the bridge emits ≤1 row per espn_match_id.
espn_schedule_dedup AS (
    SELECT
        league, season, game, match_date, home_team, away_team,
        ROW_NUMBER() OVER (
            PARTITION BY league, season, game
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.espn_schedule
    WHERE game IS NOT NULL
),
espn_match_bridge AS (
    SELECT
        -- Deterministic ESPN match_id (must mirror silver/espn_lineup.sql).
        'espn_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
            COALESCE(es.league, '')
            || '|' || COALESCE(CAST(es.season AS varchar), '')
            || '|' || COALESCE(es.game, '')
        ))))                                                AS espn_match_id,
        -- #445: the xt_*_fb reverse lookups fan out across same-season fbref
        -- name variants; only the schedule spelling can match fme.home/away,
        -- so MAX over the NULL twins is exact (not a tiebreak).
        MAX(fme.match_id)                                   AS fbref_match_id,
        es.league                                           AS league,
        es.season                                           AS season  -- #404: ESPN bronze slug ('2526')
    FROM espn_schedule_dedup es
    LEFT JOIN xref_team_by_canonical xt_home_es
        ON xt_home_es.source    = 'espn'
       AND xt_home_es.source_id = es.home_team
       AND xt_home_es.league    = es.league
       AND xt_home_es.season    = es.season         -- #461
    LEFT JOIN xref_team_by_canonical xt_away_es
        ON xt_away_es.source    = 'espn'
       AND xt_away_es.source_id = es.away_team
       AND xt_away_es.league    = es.league
       AND xt_away_es.season    = es.season         -- #461
    LEFT JOIN xref_team_by_canonical xt_home_fb
        ON xt_home_fb.source       = 'fbref'
       AND xt_home_fb.canonical_id = xt_home_es.canonical_id
       AND xt_home_fb.league       = es.league
       AND xt_home_fb.season       = es.season      -- #461
    LEFT JOIN xref_team_by_canonical xt_away_fb
        ON xt_away_fb.source       = 'fbref'
       AND xt_away_fb.canonical_id = xt_away_es.canonical_id
       AND xt_away_fb.league       = es.league
       AND xt_away_fb.season       = es.season      -- #461
    LEFT JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.league = es.league
       AND fme.home   = xt_home_fb.source_id
       AND fme.away   = xt_away_fb.source_id
       -- bronze.espn_schedule.match_date is timestamp(6); fme.date is date.
       -- Compare on the date part. Rescheduling within a day is rare for
       -- APL fixtures and surfaces as bridge-miss in DQ.
       AND fme.date   = CAST(es.match_date AS date)
    WHERE es.rn = 1
    -- #445: ordinals, NOT select aliases — Trino raises COLUMN_NOT_FOUND on
    -- same-level aliases in GROUP BY (DuckDB-based tests mask this).
    -- Ordinal 1 = the espn_match_id hash expression.
    GROUP BY 1, 3, 4
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
        -- FBref lineups carry no captaincy → NULL; enriched downstream via the
        -- SofaScore captain bridge (#439). Column present so the UNION aligns.
        CAST(NULL AS boolean)                          AS is_captain,
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
       AND xm.league    = fl.league
       AND xm.season    = fl.season
    -- #729: season-agnostic dedup. fbref_match_lineups.team carries short names
    -- (Tottenham / West Ham / Nottingham / Newcastle) that xref_team only holds
    -- for season 2526, so the old season-keyed JOIN dropped 2967 older-season
    -- rows to NULL team_id. xref_team_dedup is one row per (source, source_id,
    -- league) with confidence='orphan' already filtered.
    LEFT JOIN xref_team_dedup xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fl.team
       AND xt.league    = fl.league
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
        -- #692: ESPN has no native player_id; xref_player (source='espn')
        -- resolves by (name, team). canonical_id is 'fb_<id>' when matched,
        -- 'es_<player>|<team>' when orphan, NULL when ESPN season is outside
        -- the resolver's scope. Cross-source dedup (FBref priority 1 < ESPN 2)
        -- then collapses resolved ESPN rows into their FBref twin.
        xp.canonical_id                                AS player_id_canonical,
        el.player                                      AS player_name,
        el.is_starter                                  AS is_starter,
        el.position                                    AS position_canonical,
        el.jersey_number                               AS jersey_number,
        -- ESPN matchsheet carries no captaincy → NULL (UNION alignment).
        CAST(NULL AS boolean)                          AS is_captain,
        el._bronze_ingested_at                         AS _bronze_ingested_at,
        el.league                                      AS league,
        -- season UNIFICATION (E3.5 R4): ESPN Silver stores a varchar slug
        -- (``2425`` for 2024-25, see silver/espn_lineup.sql). CAST AS varchar is
        -- now a no-op kept for clarity; lpad guards against degenerate ``525`` →
        -- '0525' (defensive — production data is always 4-digit, lpad is cheap).
        lpad(CAST(el.season AS varchar), 4, '0')        AS season,
        'espn'                                         AS lineup_source,
        -- #693: ESPN moves to the tail of the priority order. Its player_id is
        -- always NULL (no ESPN player resolver), so it never wins cross-source
        -- dedup anyway; FBref(1) > SofaScore(2) > [FotMob(3) > WhoScored(4)] > ESPN(5).
        5                                              AS source_priority,
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
       AND xt.confidence <> 'orphan'   -- #506: don't leak 'es_<slug>' pseudo-canonical
    -- #692: resolve ESPN player_id via xref_player. ESPN has no native
    -- player_id, so the resolver synthesises source_id='<player>|<team>' and
    -- the JOIN keys on (display_name, raw_team_name). The (league, season)
    -- predicate keeps this 1:1 (xref_player PK is (source, source_id, league,
    -- season)) and avoids the cross-season fan-out footgun (#205). A raw
    -- xref_player JOIN (NOT the xref_player_dedup CTE used for FBref) is
    -- correct: ESPN canonical is season-specific (resolved one season may
    -- orphan another), unlike FBref's season-independent 'fb_<player_id>'.
    LEFT JOIN iceberg.silver.xref_player xp
        ON xp.source        = 'espn'
       AND xp.display_name  = el.player
       AND xp.raw_team_name = el.team
       AND xp.league        = el.league
       AND xp.season        = el.season
),

-- ============================================================================
-- 3b) SofaScore source rows (#693) — full lineup source, not just the captain
--     overlay. Unlike ESPN, SofaScore players resolve to a REAL canonical via
--     xref_player (source='sofascore'), so cross-source dedup actually fires
--     (FBref wins via source_priority). Mirrors the ESPN bridge for match_id:
--     resolve via xref_match (confidence<>'orphan'), else fall back to an
--     'ss_<id>' pseudo-id so SofaScore-only matches still add coverage.
--   * (league, season) predicate on BOTH xref JOINs — mandatory, else 1.5-4×
--     fan-out (xref rows are per-(source, source_id, season)).
--   * xref_player_dedup (NOT a season-predicated raw join) keeps this 1:1 and
--     preserves coverage for out-of-scope seasons — same rationale as FBref.
--   * is_starter / is_captain / position are native here (conformed in
--     silver.sofascore_player_match_aggregate from the /lineups overlay).
--   * No jersey_number in SofaScore /lineups → NULL.
--   * No player_name column in the SofaScore aggregate → NULL; only ever used
--     as the dedup fallback key, and spa.player_id (the PK) is always present.
-- ============================================================================
sofascore_resolved AS (
    SELECT
        COALESCE(xm.canonical_id, 'ss_' || spa.match_id) AS match_id_canonical,
        xt.canonical_id                                AS team_id_canonical,
        xp.canonical_id                                AS player_id_canonical,
        CAST(NULL AS varchar)                          AS player_name,
        spa.is_starter                                 AS is_starter,
        spa.position                                   AS position_canonical,
        CAST(NULL AS integer)                          AS jersey_number,
        spa.is_captain                                 AS is_captain,
        spa._bronze_ingested_at                        AS _bronze_ingested_at,
        spa.league                                     AS league,
        spa.season                                     AS season,         -- #404 slug '2526'
        'sofascore'                                    AS lineup_source,
        2                                              AS source_priority,
        spa.player_id                                  AS _raw_player_id_for_dedup
    FROM iceberg.silver.sofascore_player_match_aggregate spa
    LEFT JOIN iceberg.silver.xref_match xm
        ON  xm.source     = 'sofascore'
       AND xm.source_id   = spa.match_id
       AND xm.league      = spa.league
       AND xm.season      = spa.season
       AND xm.confidence <> 'orphan'
    LEFT JOIN iceberg.silver.xref_team xt
        ON  xt.source     = 'sofascore'
       AND xt.source_id   = spa.team_name        -- xref_team source_id = team NAME
       AND xt.league      = spa.league
       AND xt.season      = spa.season
       AND xt.confidence <> 'orphan'
    LEFT JOIN xref_player_dedup xp
        ON  xp.source     = 'sofascore'
       AND xp.source_id   = spa.player_id
),

-- ============================================================================
-- 3c) FotMob source rows (#693) — full lineup parsed from lineup_json into
--     silver.fotmob_lineup (starters + subs, both teams). Same resolution shape
--     as SofaScore: real player_id via xref_player → cross-source dedup fires.
--   * match_id via xref_match (source='fotmob', keyed on the bronze match_id),
--     else 'fm_<id>' pseudo-id so FotMob-only matches still add coverage.
--   * (league, season) predicate on BOTH xref JOINs — mandatory.
--   * jersey_number IS available (FotMob shirtNumber); is_captain is NOT in
--     lineup_json → NULL (enriched via the SofaScore captain bridge below).
--   * position is the FotMob positionId CODE (varchar) for starters, NULL for
--     subs (they hold no formation slot) — raw passthrough, dim_position deferred.
-- ============================================================================
fotmob_resolved AS (
    SELECT
        COALESCE(xm.canonical_id, 'fm_' || fl.match_id) AS match_id_canonical,
        xt.canonical_id                                AS team_id_canonical,
        xp.canonical_id                                AS player_id_canonical,
        fl.player_name                                 AS player_name,
        fl.is_starter                                  AS is_starter,
        fl.position                                    AS position_canonical,
        fl.jersey_number                               AS jersey_number,
        fl.is_captain                                  AS is_captain,   -- always NULL in silver
        fl._bronze_ingested_at                         AS _bronze_ingested_at,
        fl.league                                      AS league,
        fl.season                                      AS season,        -- #404 slug '2526'
        'fotmob'                                       AS lineup_source,
        3                                              AS source_priority,
        fl.player_id                                   AS _raw_player_id_for_dedup
    FROM iceberg.silver.fotmob_lineup fl
    LEFT JOIN iceberg.silver.xref_match xm
        ON  xm.source     = 'fotmob'
       AND xm.source_id   = fl.match_id
       AND xm.league      = fl.league
       AND xm.season      = fl.season
       AND xm.confidence <> 'orphan'
    LEFT JOIN iceberg.silver.xref_team xt
        ON  xt.source     = 'fotmob'
       AND xt.source_id   = fl.team_name        -- xref_team source_id = team NAME
       AND xt.league      = fl.league
       AND xt.season      = fl.season
       AND xt.confidence <> 'orphan'
    LEFT JOIN xref_player_dedup xp
        ON  xp.source     = 'fotmob'
       AND xp.source_id   = fl.player_id
),

-- ============================================================================
-- 3d) WhoScored source rows (#693) — lineup INFERRED from the event stream
--     (silver.whoscored_lineup): a player who appeared and was not subbed on
--     started. Thinnest source: real player_id + is_starter only; position /
--     is_captain / jersey_number are NOT derivable from WhoScored events → NULL.
--   * match via xref_match (source='whoscored', source_id = WhoScored game_id),
--     else 'ws_<id>' pseudo-id fallback.
--   * team via xref_team (source_id = team NAME; silver already resolved the
--     numeric team_id → name through whoscored_schedule).
--   * (league, season) predicate on BOTH xref JOINs.
-- ============================================================================
whoscored_resolved AS (
    SELECT
        COALESCE(xm.canonical_id, 'ws_' || wl.match_id) AS match_id_canonical,
        xt.canonical_id                                AS team_id_canonical,
        xp.canonical_id                                AS player_id_canonical,
        CAST(NULL AS varchar)                          AS player_name,
        wl.is_starter                                  AS is_starter,
        wl.position                                    AS position_canonical,  -- NULL
        wl.jersey_number                               AS jersey_number,       -- NULL
        wl.is_captain                                  AS is_captain,          -- NULL
        wl._bronze_ingested_at                         AS _bronze_ingested_at,
        wl.league                                      AS league,
        wl.season                                      AS season,              -- slug '2526'
        'whoscored'                                    AS lineup_source,
        4                                              AS source_priority,
        wl.player_id                                   AS _raw_player_id_for_dedup
    FROM iceberg.silver.whoscored_lineup wl
    LEFT JOIN iceberg.silver.xref_match xm
        ON  xm.source     = 'whoscored'
       AND xm.source_id   = wl.match_id
       AND xm.league      = wl.league
       AND xm.season      = wl.season
       AND xm.confidence <> 'orphan'
    LEFT JOIN iceberg.silver.xref_team xt
        ON  xt.source     = 'whoscored'
       AND xt.source_id   = wl.team_name        -- xref_team source_id = team NAME
       AND xt.league      = wl.league
       AND xt.season      = wl.season
       AND xt.confidence <> 'orphan'
    LEFT JOIN xref_player_dedup xp
        ON  xp.source     = 'whoscored'
       AND xp.source_id   = wl.player_id
),

-- ============================================================================
-- 4) UNION + dedup
-- ============================================================================
all_lineups AS (
    SELECT * FROM fbref_resolved
    UNION ALL
    SELECT * FROM espn_resolved
    UNION ALL
    SELECT * FROM sofascore_resolved
    UNION ALL
    SELECT * FROM fotmob_resolved
    UNION ALL
    SELECT * FROM whoscored_resolved
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
),

-- ============================================================================
-- 5) SofaScore captain bridge (#439)
-- ============================================================================
-- Neither FBref nor ESPN lineup feeds carry captaincy; SofaScore /lineups do
-- (silver.sofascore_player_match_aggregate.is_captain, conformed to boolean).
-- Resolve the SofaScore (match, player) to the SAME canonical ids fct_lineup
-- already keys on, then LEFT JOIN below. Coverage is FBref-only in practice:
-- ESPN rows have player_id_canonical = NULL (no ESPN resolver), so the
-- player-id equality never matches and they stay is_captain = NULL.
--   * (league, season) predicate on BOTH xref JOINs — mandatory, else 1.5-4×
--     fan-out (xref rows are per-(source, source_id, season)).
--   * confidence <> 'orphan' on xref_match — a SofaScore-orphan match resolves
--     to 'ss_<id>' which never equals a FBref hex anyway; filter is the
--     xref contract (#506) and trims the bridge early.
--   * GROUP BY (match, player) + MAX(is_captain) — one captaincy verdict per
--     resolved pair (true wins over false; both win over an absent overlay).
sofascore_captain AS (
    SELECT
        xm.canonical_id                  AS match_id_canonical,
        xp.canonical_id                  AS player_id_canonical,
        MAX(spa.is_captain)              AS is_captain
    FROM iceberg.silver.sofascore_player_match_aggregate spa
    JOIN iceberg.silver.xref_match xm
        ON xm.source     = 'sofascore'
       AND xm.source_id  = spa.match_id
       AND xm.league     = spa.league
       AND xm.season     = spa.season
       AND xm.confidence <> 'orphan'
    JOIN iceberg.silver.xref_player xp
        ON xp.source     = 'sofascore'
       AND xp.source_id  = spa.player_id
       AND xp.league     = spa.league
       AND xp.season     = spa.season
    WHERE spa.is_captain IS NOT NULL
    GROUP BY 1, 2
),

-- ============================================================================
-- 6) FBref-covered matches — drop redundant non-FBref duplicates (#819)
-- ============================================================================
-- For a match FBref already covers, the FBref lineup is authoritative and
-- complete (see ADR "Dedup priority FBref > SofaScore > …"). A non-FBref row
-- for the SAME match whose player did NOT resolve to a real 'fb_' canonical is
-- a pure duplicate: its player key (NULL / source-prefixed orphan) differs from
-- the FBref twin, so cross-source dedup cannot collapse it and it survives only
-- to inflate the player_id NULL/orphan share. These are the bulk of fct_lineup
-- FK orphans (#819: ~109k fotmob/espn rows sit in FBref-covered matches because
-- xref_player has a per-season coverage gradient and never resolves them).
-- Resolved non-FBref rows ('fb_…') and rows in matches FBref does NOT cover are
-- KEPT — they add real coverage. Note the bridge itself is NOT broken: before
-- dedup ~93-99% of sofascore/whoscored/espn rows DO resolve, they just win
-- under lineup_source='fbref' via source_priority.
fbref_covered_matches AS (
    SELECT DISTINCT match_id_canonical
    FROM fbref_resolved
)

SELECT
    d.match_id_canonical            AS match_id,
    d.team_id_canonical             AS team_id,
    d.player_id_canonical           AS player_id,
    d.is_starter,
    d.position_canonical            AS position,
    d.jersey_number,
    -- is_captain: native captain of the winning source first (SofaScore rows
    -- carry it from /lineups; #693), then the SofaScore captain bridge as a
    -- fallback for FBref rows (#439). NULL where neither covers the player
    -- (all ESPN rows, plus FBref players absent from SofaScore /lineups).
    COALESCE(d.is_captain, sc.is_captain)  AS is_captain,
    d.lineup_source,
    'v1'                            AS lineup_version,
    d.league,
    d.season
FROM dedup d
LEFT JOIN sofascore_captain sc
       ON sc.match_id_canonical  = d.match_id_canonical
      AND sc.player_id_canonical = d.player_id_canonical
LEFT JOIN fbref_covered_matches fcm
       ON fcm.match_id_canonical = d.match_id_canonical
WHERE d.rn = 1
  AND d.match_id_canonical IS NOT NULL
  -- #819: drop redundant non-FBref rows in FBref-covered matches whose player
  -- did not resolve to a real 'fb_' canonical (NULL or source-prefixed orphan).
  -- FBref already carries the authoritative lineup for those matches, so the
  -- unresolved twin only inflates the player_id NULL/orphan share. Resolved
  -- ('fb_…') rows and rows in FBref-uncovered matches are untouched.
  AND NOT (
            d.lineup_source <> 'fbref'
        AND fcm.match_id_canonical IS NOT NULL
        AND (d.player_id_canonical IS NULL
             OR d.player_id_canonical NOT LIKE 'fb\_%' ESCAPE '\')
  )
