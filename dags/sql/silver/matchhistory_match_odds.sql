-- =============================================================================
-- Silver: matchhistory_match_odds
-- =============================================================================
-- DESIGN: bridge MatchHistory (football-data.co.uk) → gold.dim_match через
--   (date, home_canonical, away_canonical), а НЕ через silver.xref_match.
--   1) MatchHistory.hometeam/awayteam canonicalize via inline team_aliases CTE
--      (источник истины — `configs/medallion/team_aliases.yaml`; список
--      сгенерирован через `medallion_config.get_team_alias_pairs(
--      source='matchhistory', competition='ENG-Premier League')` и
--      синхронизирован вручную — при добавлении нового APL-club YAML
--      обновлять оба места).
--   2) INNER JOIN gold.dim_match по (CAST(date AS DATE), league, season,
--      home_team_id=home_canonical, away_team_id=away_canonical).
--   3) НЕ модифицирует silver.xref_match (Phase B расширение deferred —
--      bridging здесь живёт инлайн как в R3 spec для E4 итерации).
--   4) Tall format: UNION ALL по bookmaker × market даёт ~12 row/match
--      (6 bookies × 1x2 open + 4 closing + 4 AH + 4 OU вариаций), что
--      раскладывается без PIVOT, удобно для downstream Brier-backtests.
-- Reference: roadmap E4.3 + `configs/medallion/team_aliases.yaml`.
--
-- =============================================================================
-- Bronze schema reference  (source: iceberg.bronze.matchhistory_results)
-- =============================================================================
--   ВНИМАНИЕ (#307): источник переключён с замороженной legacy
--   `matchhistory_games` (сырые football-data имена) на живую
--   `matchhistory_results`. У results СМЕШАННАЯ схема: COLUMN_MAPPING
--   (scrapers/matchhistory/scraper.py) переименовывает ~50 колонок, остальные
--   остаются сырыми. Все колонки lowercase.
--     renamed : Date→match_date, HomeTeam→home_team, AwayTeam→away_team,
--               B365H→odds_home_b365 (+ BW/PS/WH/VC по 1x2-open).
--     raw     : iw* (IW нет в mapping), все *closing, все ah*, все OU "b365>2.5".
--   `mh` CTE алиасит renamed-колонки обратно к legacy-именам (b365h, hometeam …),
--   поэтому весь downstream ниже не меняется.
--
--   Identity: match_date (timestamp(6)), home_team, away_team, referee, league,
--             season (bigint)
--   Score:    home_goals, away_goals, result, …  (bigint / varchar)
--
--   1x2 open    : odds_home/draw/away_b365, …_bw, iwh/d/a (raw), …_ps, …_wh, …_vc
--   1x2 closing : b365ch/cd/ca, bwch/cd/ca, psch/pscd/psca, whch/whcd/whca,
--                 vcch/vccd/vcca, iwch/iwcd/iwca
--   AH open     : ahh (handicap line), b365ahh/aha, pahh/paha, maxahh/maxaha,
--                 avgahh/avgaha
--   AH closing  : ahch (handicap line), b365cahh/caha, pcahh/pcaha,
--                 maxcahh/maxcaha, avgcahh/avgcaha
--   OU 2.5 open : "b365>2.5", "b365<2.5", "p>2.5", "p<2.5", "max>2.5",
--                 "max<2.5", "avg>2.5", "avg<2.5"
--   OU 2.5 close: "b365c>2.5", "b365c<2.5", "pc>2.5", "pc<2.5",
--                 "maxc>2.5", "maxc<2.5", "avgc>2.5", "avgc<2.5"
--
--   Колонки `>2.5` / `<2.5` содержат ASCII-операторы — обязательно в
--   двойных кавычках (`"b365>2.5"`), иначе Trino парсит как сравнение.
--
-- =============================================================================
-- Output schema
-- =============================================================================
--   match_id_canonical     varchar          -- == gold.dim_match.match_id (FBref hex)
--   bookmaker_code         varchar          -- 'B365'|'PS'|'WH'|'VC'|'IW'|'BW'|'AVG'|'MAX'
--   market                 varchar          -- '1x2' | 'ah' | 'ou_2_5'
--   odds_h                 decimal(6,3)     -- 1x2.home OR ah.home OR ou.over
--   odds_d                 decimal(6,3)     -- 1x2.draw; NULL для market!='1x2'
--   odds_a                 decimal(6,3)     -- 1x2.away OR ah.away OR ou.under
--   ah_handicap            decimal(4,2)     -- handicap line; NULL для market!='ah'
--   ou_line                decimal(4,2)     -- O/U line (=2.5); NULL для market!='ou_2_5'
--   closing_flag           boolean          -- TRUE = closing odds (Cl-prefixed cols)
--   source                 varchar          -- 'matchhistory'
--   source_version         varchar          -- 'v1'
--   league                 varchar          -- partition key
--   season                 varchar          -- partition key (slug '2425', #404)
--   _ingested_at           timestamp(6)     -- bronze lineage passthrough
--
-- Logical PK: (match_id_canonical, bookmaker_code, market, closing_flag)
--
-- =============================================================================
-- Acceptance gates (E4.3 DoD)
-- =============================================================================
--   * Bridge coverage ≥95% (≥1805 distinct match_id_canonical из ~1900 матчей).
--     Misses — ожидаемо: rows с NULL hometeam/awayteam, или canonical_id
--     отсутствующий в dim_match (новые сезоны без FBref schedule).
--   * closing_flag distribution: ≥80% closing rows (PS/B365/WH closing 1x2)
--     populated с non-null odds_h/d/a (старые сезоны до 2019 могут не иметь
--     closing odds в football-data.co.uk).
--   * Партиционирование `(league, season)` применяется снаружи через
--     silver_tasks.run_silver_transform.
--
-- =============================================================================
-- DAG-integration note
-- =============================================================================
--   silver_tasks.run_silver_transform() wraps this SELECT in
--   `CREATE TABLE iceberg.silver.matchhistory_match_odds AS ...` with
--   partitioning by (league, season). This file MUST stay a pure SELECT.
-- =============================================================================

WITH team_aliases AS (
    -- ====== APL-only team alias pool ======
    -- Source of truth: configs/medallion/team_aliases.yaml + verified against
    -- live `gold.dim_match` (smoke 2026-05-08).
    --
    -- KEY INSIGHT: canonical_id RHS = ACTUAL `gold.dim_match.home_team_id`
    -- value as observed in production, NOT slug-of-YAML-canonical_name. Это
    -- потому что `gold.dim_match.home_team_id` строится через
    -- `entity_xref` SQL (`LOWER(REGEXP_REPLACE(fbref.home, ...))`) и FBref
    -- сам неконсистентен между сезонами (e.g. `Newcastle Utd` 2017-18 vs
    -- `Newcastle United` 2018+; `Wolverhampton Wanderers` early seasons vs
    -- `Wolves` later). Прямое сравнение со sliced `home_team_id`
    -- distinct-values из dim_match даёт правильный target slug.
    --
    -- Examples of YAML drift (memory note `feedback_xref_team_canonical_drift`):
    --   YAML canonical_name      → slug-of-yaml             dim_match.home_team_id
    --   `AFC Bournemouth`        → `afc_bournemouth`        `bournemouth`
    --   `Brighton and Hove Albion` → `brighton_and_hove_...`  `brighton`
    --   `Wolverhampton Wanderers` → `wolverhampton_wanderers` `wolves`
    --   `Manchester United`      → `manchester_united`      `manchester_utd`
    --   `West Ham United`        → `west_ham_united`        `west_ham_united` (matches!)
    --
    -- Phase B work для xref_team будет приводить FBref slugs к Wikipedia
    -- canonical_name; до тех пор bridge должен использовать наблюдаемые
    -- dim_match slug-и напрямую.
    --
    -- Generation: distinct-slug query against live dim_match seasons 2021+
    --   SELECT DISTINCT home_team_id FROM iceberg.gold.dim_match WHERE season >= 2021
    -- The 2021+ filter matches Bronze.matchhistory_games season range (5
    -- seasons: 2021-2025). Older clubs (Stoke/Cardiff/Hull/etc.) included
    -- for forward-compat когда Bronze расширится до 2016+.
    --
    -- При расширении: ОБА файла (matchhistory_match_odds,
    -- sofascore_player_ratings) обновлять параллельно. Sunderland +
    -- Huddersfield + Middlesbrough добавлены для исторических CSVs.
    SELECT raw_name, canonical_id
    FROM (VALUES
        ('AFC Bournemouth', 'bournemouth'),
        ('Arsenal', 'arsenal'),
        ('Aston Villa', 'aston_villa'),
        ('Bournemouth', 'bournemouth'),
        ('Brentford', 'brentford'),
        ('Brighton', 'brighton'),
        ('Brighton & Hove Albion', 'brighton'),
        ('Brighton and Hove Albion', 'brighton'),
        ('Burnley', 'burnley'),
        ('Cardiff', 'cardiff_city'),
        ('Cardiff City', 'cardiff_city'),
        ('Chelsea', 'chelsea'),
        ('Crystal Palace', 'crystal_palace'),
        ('Everton', 'everton'),
        ('Fulham', 'fulham'),
        ('Huddersfield', 'huddersfield_town'),
        ('Hull', 'hull_city'),
        ('Hull City', 'hull_city'),
        ('Ipswich', 'ipswich_town'),
        ('Ipswich Town', 'ipswich_town'),
        ('Leeds', 'leeds_united'),
        ('Leeds United', 'leeds_united'),
        ('Leicester', 'leicester_city'),
        ('Leicester City', 'leicester_city'),
        ('Liverpool', 'liverpool'),
        ('Luton', 'luton_town'),
        ('Luton Town', 'luton_town'),
        ('Man City', 'manchester_city'),
        ('Man United', 'manchester_utd'),
        ('Man Utd', 'manchester_utd'),
        ('Manchester City', 'manchester_city'),
        ('Manchester United', 'manchester_utd'),
        ('Manchester Utd', 'manchester_utd'),
        ('Middlesbrough', 'middlesbrough'),
        ('Newcastle', 'newcastle_united'),
        ('Newcastle United', 'newcastle_united'),
        ('Newcastle Utd', 'newcastle_united'),
        ('Norwich', 'norwich_city'),
        ('Norwich City', 'norwich_city'),
        ('Nott''m Forest', 'nottingham_forest'),
        ('Nottingham', 'nottingham_forest'),
        ('Nottingham Forest', 'nottingham_forest'),
        ('Sheff Utd', 'sheffield_united'),
        ('Sheffield United', 'sheffield_united'),
        ('Sheffield Utd', 'sheffield_united'),
        ('Southampton', 'southampton'),
        ('Spurs', 'tottenham_hotspur'),
        ('Stoke', 'stoke_city'),
        ('Stoke City', 'stoke_city'),
        ('Sunderland', 'sunderland'),
        ('Swansea', 'swansea_city'),
        ('Swansea City', 'swansea_city'),
        ('Tottenham', 'tottenham_hotspur'),
        ('Tottenham Hotspur', 'tottenham_hotspur'),
        ('Watford', 'watford'),
        ('West Brom', 'west_brom'),
        ('West Bromwich', 'west_brom'),
        ('West Bromwich Albion', 'west_brom'),
        ('West Ham', 'west_ham_united'),
        ('West Ham United', 'west_ham_united'),
        ('Wolverhampton', 'wolves'),
        ('Wolverhampton Wanderers', 'wolves'),
        ('Wolves', 'wolves')
    ) AS t(raw_name, canonical_id)
),

-- =============================================================================
-- mh: raw bronze, basic typed projection (one row per MatchHistory match)
-- =============================================================================
mh AS (
    SELECT
        -- NOTE: bronze.matchhistory_results has MIXED column naming —
        -- COLUMN_MAPPING (scrapers/matchhistory/scraper.py) renames ~50 cols
        -- (Date→match_date, HomeTeam→home_team, B365H→odds_home_b365, …) but
        -- leaves the rest raw (iw*, *closing, ah*, OU "b365>2.5"). We alias the
        -- renamed physical cols back to the legacy logical names below so the
        -- ~400 lines of downstream UNION ALL stay untouched.
        -- match_date is raw football-data 'DD/MM/YYYY' varchar in results
        -- (legacy games stored it pre-parsed as timestamp) → parse explicitly.
        CAST(date_parse(match_date, '%d/%m/%Y') AS DATE) AS match_date,
        home_team                      AS hometeam,
        away_team                      AS awayteam,
        league,
        -- season → slug ('2425'); matchhistory bronze stores year-start bigint.
        -- Converted here so the gold.dim_match bridge JOIN (slug after #404) and
        -- the final projection are both slug.
        LPAD(CAST(MOD(season,     100) AS varchar), 2, '0')
            || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0') AS season,
        _ingested_at,

        -- ---- 1x2 open (B365/BW/PS/WH/VC renamed via COLUMN_MAPPING; IW raw) ----
        odds_home_b365 AS b365h, odds_draw_b365 AS b365d, odds_away_b365 AS b365a,
        odds_home_bw   AS bwh,   odds_draw_bw   AS bwd,   odds_away_bw   AS bwa,
        iwh,   iwd,   iwa,
        odds_home_ps   AS psh,   odds_draw_ps   AS psd,   odds_away_ps   AS psa,
        odds_home_wh   AS whh,   odds_draw_wh   AS whd,   odds_away_wh   AS wha,
        odds_home_vc   AS vch,   odds_draw_vc   AS vcd,   odds_away_vc   AS vca,

        -- ---- 1x2 closing ----
        b365ch, b365cd, b365ca,
        bwch,   bwcd,   bwca,
        iwch,   iwcd,   iwca,
        psch,   pscd,   psca,
        whch,   whcd,   whca,
        vcch,   vccd,   vcca,

        -- ---- AH open (handicap line: ahh) ----
        ahh,
        b365ahh, b365aha,
        pahh,    paha,
        maxahh,  maxaha,
        avgahh,  avgaha,

        -- ---- AH closing (handicap line: ahch) ----
        ahch,
        b365cahh, b365caha,
        pcahh,    pcaha,
        maxcahh,  maxcaha,
        avgcahh,  avgcaha,

        -- ---- OU 2.5 open (line is constant 2.5) ----
        "b365>2.5" AS b365_o25, "b365<2.5" AS b365_u25,
        "p>2.5"    AS p_o25,    "p<2.5"    AS p_u25,
        "max>2.5"  AS max_o25,  "max<2.5"  AS max_u25,
        "avg>2.5"  AS avg_o25,  "avg<2.5"  AS avg_u25,

        -- ---- OU 2.5 closing ----
        "b365c>2.5" AS b365c_o25, "b365c<2.5" AS b365c_u25,
        "pc>2.5"    AS pc_o25,    "pc<2.5"    AS pc_u25,
        "maxc>2.5"  AS maxc_o25,  "maxc<2.5"  AS maxc_u25,
        "avgc>2.5"  AS avgc_o25,  "avgc<2.5"  AS avgc_u25
    FROM iceberg.bronze.matchhistory_results
    WHERE home_team  IS NOT NULL
      AND away_team  IS NOT NULL
      AND match_date IS NOT NULL
),

-- =============================================================================
-- mh_canonicalized: lookup home/away canonical_id via team_aliases
-- =============================================================================
mh_canonicalized AS (
    SELECT
        mh.*,
        ha.canonical_id AS home_canonical,
        aa.canonical_id AS away_canonical
    FROM mh
    LEFT JOIN team_aliases ha ON ha.raw_name = mh.hometeam
    LEFT JOIN team_aliases aa ON aa.raw_name = mh.awayteam
),

-- =============================================================================
-- mh_bridged: INNER JOIN to gold.dim_match — only matches that bridge survive.
-- season+league predicate enforced (memory note: feedback_xref_join_season_predicate).
-- =============================================================================
mh_bridged AS (
    SELECT
        dm.match_id        AS match_id_canonical,
        c.match_date,
        c.league,
        c.season,
        c._ingested_at,

        c.b365h, c.b365d, c.b365a,
        c.bwh,   c.bwd,   c.bwa,
        c.iwh,   c.iwd,   c.iwa,
        c.psh,   c.psd,   c.psa,
        c.whh,   c.whd,   c.wha,
        c.vch,   c.vcd,   c.vca,
        c.b365ch, c.b365cd, c.b365ca,
        c.bwch,   c.bwcd,   c.bwca,
        c.iwch,   c.iwcd,   c.iwca,
        c.psch,   c.pscd,   c.psca,
        c.whch,   c.whcd,   c.whca,
        c.vcch,   c.vccd,   c.vcca,

        c.ahh,
        c.b365ahh, c.b365aha,
        c.pahh,    c.paha,
        c.maxahh,  c.maxaha,
        c.avgahh,  c.avgaha,

        c.ahch,
        c.b365cahh, c.b365caha,
        c.pcahh,    c.pcaha,
        c.maxcahh,  c.maxcaha,
        c.avgcahh,  c.avgcaha,

        c.b365_o25,  c.b365_u25,
        c.p_o25,     c.p_u25,
        c.max_o25,   c.max_u25,
        c.avg_o25,   c.avg_u25,
        c.b365c_o25, c.b365c_u25,
        c.pc_o25,    c.pc_u25,
        c.maxc_o25,  c.maxc_u25,
        c.avgc_o25,  c.avgc_u25
    FROM mh_canonicalized c
    INNER JOIN iceberg.gold.dim_match dm
        -- #433: dim_match renamed date -> match_date (star dims, #425)
        ON dm.match_date   = c.match_date
       AND dm.league       = c.league
       AND dm.season       = c.season
       AND dm.home_team_id = c.home_canonical
       AND dm.away_team_id = c.away_canonical
),

-- =============================================================================
-- mh_unfolded: tall format. UNION ALL по bookmaker × market.
-- One row per (match_id_canonical, bookmaker_code, market, closing_flag).
-- Empty bookmaker rows (all-NULL odds) filtered downstream by WHERE in `typed`.
-- =============================================================================
mh_unfolded AS (
    -- ===== 1x2 OPEN =====
    SELECT match_id_canonical, 'B365' AS bookmaker_code, '1x2' AS market,
           b365h AS odds_h, b365d AS odds_d, b365a AS odds_a,
           CAST(NULL AS double) AS ah_handicap, CAST(NULL AS double) AS ou_line,
           FALSE AS closing_flag,
           league, season, _ingested_at
      FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'BW', '1x2', bwh, bwd, bwa, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'IW', '1x2', iwh, iwd, iwa, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', '1x2', psh, psd, psa, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'WH', '1x2', whh, whd, wha, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'VC', '1x2', vch, vcd, vca, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged

    -- ===== 1x2 CLOSING =====
    UNION ALL
    SELECT match_id_canonical, 'B365', '1x2', b365ch, b365cd, b365ca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'BW', '1x2', bwch, bwcd, bwca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'IW', '1x2', iwch, iwcd, iwca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', '1x2', psch, pscd, psca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'WH', '1x2', whch, whcd, whca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'VC', '1x2', vcch, vccd, vcca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged

    -- ===== AH OPEN (odds_d=NULL — market=ah имеет only home/away) =====
    UNION ALL
    SELECT match_id_canonical, 'B365', 'ah',
           b365ahh, NULL, b365aha, ahh, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', 'ah',
           pahh, NULL, paha, ahh, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'AVG', 'ah',
           avgahh, NULL, avgaha, ahh, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', 'ah',
           maxahh, NULL, maxaha, ahh, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged

    -- ===== AH CLOSING =====
    UNION ALL
    SELECT match_id_canonical, 'B365', 'ah',
           b365cahh, NULL, b365caha, ahch, NULL, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', 'ah',
           pcahh, NULL, pcaha, ahch, NULL, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'AVG', 'ah',
           avgcahh, NULL, avgcaha, ahch, NULL, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', 'ah',
           maxcahh, NULL, maxcaha, ahch, NULL, TRUE,
           league, season, _ingested_at FROM mh_bridged

    -- ===== OU 2.5 OPEN (odds_h = OVER, odds_a = UNDER, line = 2.5 const) =====
    UNION ALL
    SELECT match_id_canonical, 'B365', 'ou_2_5',
           b365_o25, NULL, b365_u25, NULL, 2.5, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', 'ou_2_5',
           p_o25, NULL, p_u25, NULL, 2.5, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'AVG', 'ou_2_5',
           avg_o25, NULL, avg_u25, NULL, 2.5, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', 'ou_2_5',
           max_o25, NULL, max_u25, NULL, 2.5, FALSE,
           league, season, _ingested_at FROM mh_bridged

    -- ===== OU 2.5 CLOSING =====
    UNION ALL
    SELECT match_id_canonical, 'B365', 'ou_2_5',
           b365c_o25, NULL, b365c_u25, NULL, 2.5, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'PS', 'ou_2_5',
           pc_o25, NULL, pc_u25, NULL, 2.5, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'AVG', 'ou_2_5',
           avgc_o25, NULL, avgc_u25, NULL, 2.5, TRUE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', 'ou_2_5',
           maxc_o25, NULL, maxc_u25, NULL, 2.5, TRUE,
           league, season, _ingested_at FROM mh_bridged
)

-- =============================================================================
-- typed: cast DOUBLE→DECIMAL + drop empty bookmaker rows + final projection.
-- A row is dropped iff ALL three odds are NULL (= bookmaker not present this
-- season). Partial NULLs (e.g. odds_d NULL in market='ah') are preserved.
-- =============================================================================
SELECT
    match_id_canonical,
    bookmaker_code,
    market,
    CAST(odds_h      AS decimal(6,3))     AS odds_h,
    CAST(odds_d      AS decimal(6,3))     AS odds_d,
    CAST(odds_a      AS decimal(6,3))     AS odds_a,
    CAST(ah_handicap AS decimal(4,2))     AS ah_handicap,
    CAST(ou_line     AS decimal(4,2))     AS ou_line,
    closing_flag,
    CAST('matchhistory' AS varchar)       AS source,
    CAST('v1'           AS varchar)       AS source_version,
    league,
    season,
    _ingested_at
FROM mh_unfolded
WHERE NOT (odds_h IS NULL AND odds_d IS NULL AND odds_a IS NULL)
  -- Source-data sanitisation: drop rows where any present odd is non-positive
  -- (e.g. CSV holds 0.000 for missing/voided OU markets). Decimal odds must
  -- be > 1.0 by definition; odds_d only applies to 1x2 (NULL on ah/ou_2_5).
  AND (odds_h IS NULL OR odds_h > 1.0)
  AND (odds_d IS NULL OR odds_d > 1.0)
  AND (odds_a IS NULL OR odds_a > 1.0)
