-- =============================================================================
-- Silver: fotmob_match_referee
-- =============================================================================
--
-- One row per (match_id, league, season) — судья матча + его страна из FotMob.
--
-- Issue #290: FotMob — единственный источник, отдающий СТРАНУ судьи. FBref и
-- MatchHistory дают только имя. xref_referee (#270) материализует лишь
-- `$.infoBox.Referee.text`; страна (`country` / `countryCode`) выносилась в
-- эту отдельную match-grain таблицу, т.к. схема xref_referee заморожена
-- (E1 dual-run). Country — будущий дизамбигуатор тёзок-судей между странами
-- при мульти-лиговом расширении (напр. Jarred Gillett / AUS судит в APL).
--
-- Source (#930 cutover: legacy bronze.fotmob_match_details → native):
--   bronze.fotmob_match_payloads_current.match_facts_json (varchar JSON)
--     * $.infoBox.Referee.text        → имя ('Michael Oliver')
--     * $.infoBox.Referee.country     → страна ('England', 'Australia')
--     * $.infoBox.Referee.countryCode → ISO-код ('ENG', 'AUS')
--   Дедуп не нужен: `_current`-view отдаёт одну (последнюю закоммиченную)
--   строку на матч — манифест-гейт + идентичность (target_type, match_id).
--   league — из competition_id (varchar в payloads → CAST bigint → league_map;
--   INNER JOIN одновременно скоупит выдачу прежними 14 лигами).
--   season — год-старта из source_season_key ('2025/2026' | '2025'),
--   далее прежний legacy-CASE слага (#913 Phase 2).
--
-- Pure SELECT: CTAS-обёртку (CREATE OR REPLACE + partitioning league/season +
-- _silver_created_at) навешивает silver_tasks.run_silver_transform().
-- =============================================================================

WITH league_map (competition_id, league) AS (
    VALUES (47, 'ENG-Premier League'), (48, 'ENG-Championship'),
           (87, 'ESP-La Liga'), (54, 'GER-Bundesliga'), (55, 'ITA-Serie A'),
           (53, 'FRA-Ligue 1'), (57, 'NED-Eredivisie'), (61, 'POR-Primeira Liga'),
           (42, 'UEFA-Champions League'), (73, 'UEFA-Europa League'),
           (77, 'INT-World Cup'), (50, 'INT-European Championship'),
           (289, 'INT-Africa Cup of Nations'), (44, 'INT-Copa America')
),

match_facts AS (
    SELECT
        CAST(p.match_id AS varchar) AS match_id,
        lm.league,
        TRY_CAST(substr(p.source_season_key, 1, 4) AS integer) AS season_year,
        p.match_facts_json,
        p._observed_at
    FROM iceberg.bronze.fotmob_match_payloads_current p
    JOIN league_map lm
      ON lm.competition_id = CAST(p.competition_id AS bigint)
    WHERE p.match_facts_json IS NOT NULL
)

SELECT
    match_id,
    league,
    -- season → slug ('2425'); year-start = substr(source_season_key, 1, 4).
    -- #913 Phase 2
    CASE WHEN league = 'INT-World Cup'
         THEN LPAD(CAST(season_year AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(season_year, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(season_year + 1, 100) AS varchar), 2, '0')
    END AS season,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.text')        AS referee_name,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.country')     AS referee_country,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.countryCode') AS referee_country_code,
    _observed_at                                                           AS _bronze_ingested_at
FROM match_facts
WHERE json_extract_scalar(match_facts_json, '$.infoBox.Referee.text') IS NOT NULL
  AND json_extract_scalar(match_facts_json, '$.infoBox.Referee.text') <> ''
