-- =============================================================================
-- Silver: sofifa_team_profile
-- =============================================================================
--
-- Per-edition snapshot команды из SoFIFA (FIFA/EA FC video-game ratings):
-- headline overall/attack/midfield/defence, game-side transfer budget / club
-- worth, размер состава и средний возраст стартового XI. Один row per
-- (team_id, fifa_edition).
--
-- ВАЖНО: это game-side рейтинги (мнение EA Sports), НЕ реальные метрики матчей.
--
-- Источники Bronze:
--   * bronze.sofifa_teams        — identity (team_id, team, league), key
--     (team_id, fifa_edition).
--   * bronze.sofifa_team_ratings — 8 живых рейтингов, тот же key. 15 dead
--     FC-26 колонок (build_up_*/chance_creation_*/defence-tactics/prestige/
--     whole_team_average_age) убраны из парсера и Bronze (#601).
--
-- Bronze team_ratings приходит НЕтипизированным (scrape_team_ratings не зовёт
-- _process_rating_data) — значения это строки, поэтому TRY_CAST везде:
--   * overall/attack/midfield/defence — целые 0-99.
--   * transfer_budget/club_worth — деньги ('€63.3M' / '€2.5B' / '€250K'); парсим
--     в евро (BIGINT). club_worth доходит до миллиардов → ветка B обязательна;
--     число дробное → DOUBLE перед умножением.
--   * players — размер состава (squad_size). starting_xi_average_age — DOUBLE.
--
-- FIFA-edition → football-season mapping: edition назван по году конца сезона
--   (EA FC 26 = сезон 2025/26). 'FC 26' → '2526'. season = LPAD(N-1) || LPAD(N).
--   Это ТО ЖЕ выражение, что в xref_team.sql.j2 для sofifa — season обязан
--   совпасть, иначе canonical_id JOIN не сойдётся.
--
-- canonical_id подтягивается через silver.xref_team (source='sofifa',
-- non-orphan). xref_team ключуется ИМЕНЕМ команды (source_id = team name), НЕ
-- team_id — поэтому JOIN по j.team, а не по team_id. (league, season)
-- JOIN-предикат MANDATORY (CLAUDE.md). canonical_id остаётся NULLable: имя из
-- sofifa может не сматчиться на алиас (orphan).
--
-- Bronze ingest mode = replace_partitions(['fifa_edition']) → ROW_NUMBER dedup
-- defensive против повторных прогонов внутри одной edition.
-- =============================================================================

WITH teams_dedup AS (
    SELECT team_id, fifa_edition, league, team, _ingested_at
    FROM (
        SELECT
            t.team_id, t.fifa_edition, t.league, t.team, t._ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY t.team_id, t.fifa_edition
                ORDER BY t._ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofifa_teams t
        WHERE t.team_id IS NOT NULL
    )
    WHERE rn = 1
),

ratings_dedup AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, fifa_edition
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofifa_team_ratings r
        WHERE team_id IS NOT NULL
    )
    WHERE rn = 1
),

joined AS (
    SELECT
        t.team_id,
        t.league,
        t.team,
        t.fifa_edition,
        -- FIFA/FC edition number → football-season slug ('FC 26' -> '2526').
        LPAD(CAST(
            CAST(regexp_extract(t.fifa_edition, '(\d+)', 1) AS INTEGER) - 1
            AS VARCHAR), 2, '0')
        || LPAD(regexp_extract(t.fifa_edition, '(\d+)', 1), 2, '0') AS season,

        -- Headline ratings (0-99)
        TRY_CAST(r.overall AS INTEGER)              AS overall,
        TRY_CAST(r.attack AS INTEGER)               AS attack,
        TRY_CAST(r.midfield AS INTEGER)             AS midfield,
        TRY_CAST(r.defence AS INTEGER)              AS defence,

        -- Game-side money: '€63.3M' / '€2.5B' / '€915.3M' / '€250K' → euros
        -- (BIGINT). club_worth reaches billions (live: €5.7B) — the B branch is
        -- MANDATORY or top clubs collapse to NULL. Numbers are decimal, so parse
        -- as DOUBLE before the multiply/cast (verified on bronze 2026-06-17).
        CASE
            WHEN r.transfer_budget LIKE '%B'
                THEN CAST(TRY_CAST(regexp_extract(r.transfer_budget, '([\d.]+)', 1)
                          AS DOUBLE) * 1000000000 AS BIGINT)
            WHEN r.transfer_budget LIKE '%M'
                THEN CAST(TRY_CAST(regexp_extract(r.transfer_budget, '([\d.]+)', 1)
                          AS DOUBLE) * 1000000 AS BIGINT)
            WHEN r.transfer_budget LIKE '%K'
                THEN CAST(TRY_CAST(regexp_extract(r.transfer_budget, '([\d.]+)', 1)
                          AS DOUBLE) * 1000 AS BIGINT)
            ELSE CAST(TRY_CAST(regexp_extract(r.transfer_budget, '([\d.]+)', 1)
                      AS DOUBLE) AS BIGINT)
        END                                         AS transfer_budget_eur,
        CASE
            WHEN r.club_worth LIKE '%B'
                THEN CAST(TRY_CAST(regexp_extract(r.club_worth, '([\d.]+)', 1)
                          AS DOUBLE) * 1000000000 AS BIGINT)
            WHEN r.club_worth LIKE '%M'
                THEN CAST(TRY_CAST(regexp_extract(r.club_worth, '([\d.]+)', 1)
                          AS DOUBLE) * 1000000 AS BIGINT)
            WHEN r.club_worth LIKE '%K'
                THEN CAST(TRY_CAST(regexp_extract(r.club_worth, '([\d.]+)', 1)
                          AS DOUBLE) * 1000 AS BIGINT)
            ELSE CAST(TRY_CAST(regexp_extract(r.club_worth, '([\d.]+)', 1)
                      AS DOUBLE) AS BIGINT)
        END                                         AS club_worth_eur,

        -- Squad shape
        TRY_CAST(r.players AS INTEGER)              AS squad_size,
        TRY_CAST(r.starting_xi_average_age AS DOUBLE) AS starting_xi_average_age,

        t._ingested_at                              AS _bronze_ingested_at
    FROM teams_dedup t
    LEFT JOIN ratings_dedup r
        ON r.team_id      = t.team_id
       AND r.fifa_edition = t.fifa_edition
),

xr AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_team
    WHERE source = 'sofifa'
      AND confidence <> 'orphan'
)

SELECT
    j.team_id,
    xr.canonical_id,
    j.team                                          AS team_name,

    j.overall,
    j.attack,
    j.midfield,
    j.defence,

    j.transfer_budget_eur,
    j.club_worth_eur,
    j.squad_size,
    j.starting_xi_average_age,

    j.fifa_edition,

    j._bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    j.league,
    j.season

FROM joined j
LEFT JOIN xr
    ON xr.source_id = j.team
   AND xr.league    = j.league
   AND xr.season    = j.season
