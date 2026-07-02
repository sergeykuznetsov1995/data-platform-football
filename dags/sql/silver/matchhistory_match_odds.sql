-- =============================================================================
-- Silver: matchhistory_match_odds
-- =============================================================================
-- DESIGN: bridge MatchHistory (football-data.co.uk) → canonical FBref match_id
--   через silver-слой `silver.xref_match` (НЕ через gold.dim_match — #477).
--   football-data has NO native match_id, so we synthesise the SAME
--   'mh_<xxhash64>' source_id that xref_match assigns to MatchHistory rows
--   (keyed on date|home|away|league|season) and join the Silver xref table.
--   1) Канонизация имён команд живёт ВЫШЕ по потоку — в xref_match (через
--      xref_team, единый источник `configs/medallion/team_aliases.yaml`).
--      Здесь больше НЕТ инлайнового team_aliases CTE (устранён дубль #477).
--   2) INNER JOIN silver.xref_match WHERE confidence='date_team_match' —
--      сохраняет прежнюю семантику: только сбриджованные матчи выживают,
--      orphans отбрасываются. PK xref_match = (source, source_id, season),
--      поэтому season-предикат обязателен (от fan-out).
--   3) Зависимость silver→silver (xref_match строится в E1, раньше E4):
--      инверсия слоёв silver→gold устранена, тихая потеря матчей при
--      устаревшем dim_match невозможна.
--   4) Tall format: UNION ALL по bookmaker × market даёт ~12 row/match
--      (6 bookies × 1x2 open + 4 closing + 4 AH + 4 OU вариаций), что
--      раскладывается без PIVOT, удобно для downstream Brier-backtests.
-- Reference: issue #477 + silver/xref_match.sql (mh_resolved).
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
--   1x2 open    : odds_home/draw/away_b365, …_bw, iwh/d/a (raw), …_ps, …_wh,
--                 …_vc, maxh/d/a, avgh/d/a (market consensus, raw)
--   1x2 closing : b365ch/cd/ca, bwch/cd/ca, psch/pscd/psca, whch/whcd/whca,
--                 vcch/vccd/vcca, iwch/iwcd/iwca, maxch/cd/ca, avgch/cd/ca
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

-- =============================================================================
-- mh: raw bronze, basic typed projection (one row per MatchHistory match)
-- =============================================================================
WITH mh AS (
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
        -- Raw year-start bigint (e.g. 2024) — fed verbatim into the
        -- 'mh_<xxhash64>' source_id below so it matches xref_match.mh_resolved
        -- (which hashes the raw bronze season, not the slug).
        season                         AS season_year,
        -- season → slug ('2425'); matchhistory bronze stores year-start bigint.
        -- Converted here so the xref_match bridge JOIN (slug after #404) and
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
        -- Market consensus (raw; present since ~2019 files, NULL before) —
        -- same MAX/AVG aggregates the AH/OU blocks already promote.
        maxh, maxd, maxa,
        avgh, avgd, avga,

        -- ---- 1x2 closing ----
        b365ch, b365cd, b365ca,
        bwch,   bwcd,   bwca,
        iwch,   iwcd,   iwca,
        psch,   pscd,   psca,
        whch,   whcd,   whca,
        vcch,   vccd,   vcca,
        maxch,  maxcd,  maxca,
        avgch,  avgcd,  avgca,

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
    -- Dedup raw bronze by match natural key BEFORE the tall UNFOLD —
    -- football-data has no game_id, so a re-ingest / replace→append regression
    -- would double EVERY odds row. Keep the freshest snapshot per match;
    -- _batch_id breaks _ingested_at ties (#464). _dedup_rn is not re-selected
    -- by `mh` (explicit column list) so the output schema is unchanged.
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY league, match_date, home_team, away_team, season
                   ORDER BY _ingested_at DESC, _batch_id DESC
               ) AS _dedup_rn
        FROM iceberg.bronze.matchhistory_results
        WHERE home_team  IS NOT NULL
          AND away_team  IS NOT NULL
          AND match_date IS NOT NULL
    )
    WHERE _dedup_rn = 1
),

-- =============================================================================
-- mh_keyed: synthesise the same 'mh_<xxhash64>' source_id that
-- silver.xref_match assigns to MatchHistory rows, so we can bridge to the
-- canonical FBref match_id through the Silver xref table (no Gold dependency).
-- !!! KEEP IN SYNC with xref_match.sql `mh_resolved` source_id formula. !!!
-- Inputs are identical to xref_match: parsed ISO date | lower(home) |
-- lower(away) | league | raw year-start season.
-- =============================================================================
mh_keyed AS (
    SELECT
        mh.*,
        'mh_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
            CAST(mh.match_date AS varchar)
            || '|' || COALESCE(LOWER(CAST(mh.hometeam AS varchar)), '')
            || '|' || COALESCE(LOWER(CAST(mh.awayteam AS varchar)), '')
            || '|' || mh.league
            || '|' || CAST(mh.season_year AS varchar)
        )))) AS mh_source_id
    FROM mh
),

-- =============================================================================
-- mh_bridged: bridge to canonical FBref match_id via silver.xref_match.
-- INNER JOIN + confidence='date_team_match' preserves the original behaviour
-- (only matches that bridge to FBref survive; orphans dropped). xref_match PK
-- is (source, source_id, season) → season predicate prevents fan-out.
-- =============================================================================
mh_bridged AS (
    SELECT
        xm.canonical_id    AS match_id_canonical,
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
        c.maxh,  c.maxd,  c.maxa,
        c.avgh,  c.avgd,  c.avga,
        c.b365ch, c.b365cd, c.b365ca,
        c.bwch,   c.bwcd,   c.bwca,
        c.iwch,   c.iwcd,   c.iwca,
        c.psch,   c.pscd,   c.psca,
        c.whch,   c.whcd,   c.whca,
        c.vcch,   c.vccd,   c.vcca,
        c.maxch,  c.maxcd,  c.maxca,
        c.avgch,  c.avgcd,  c.avgca,

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
    FROM mh_keyed c
    INNER JOIN iceberg.silver.xref_match xm
        ON  xm.source     = 'matchhistory'
        AND xm.source_id  = c.mh_source_id
        AND xm.league     = c.league
        AND xm.season     = c.season
        AND xm.confidence = 'date_team_match'
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
    UNION ALL
    SELECT match_id_canonical, 'AVG', '1x2', avgh, avgd, avga, NULL, NULL, FALSE,
           league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', '1x2', maxh, maxd, maxa, NULL, NULL, FALSE,
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
    UNION ALL
    SELECT match_id_canonical, 'AVG', '1x2', avgch, avgcd, avgca,
           NULL, NULL, TRUE, league, season, _ingested_at FROM mh_bridged
    UNION ALL
    SELECT match_id_canonical, 'MAX', '1x2', maxch, maxcd, maxca,
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
