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
-- Source:
--   bronze.fotmob_match_details.match_facts_json (varchar JSON)
--     * $.infoBox.Referee.text        → имя ('Michael Oliver')
--     * $.infoBox.Referee.country     → страна ('England', 'Australia')
--     * $.infoBox.Referee.countryCode → ISO-код ('ENG', 'AUS')
--   Bronze хранит N ingest-снимков на матч → дедуп до последнего по
--   _ingested_at DESC (как в xref_referee.sql.j2 / fotmob_player_market_value).
--
-- Pure SELECT: CTAS-обёртку (CREATE OR REPLACE + partitioning league/season +
-- _silver_created_at) навешивает silver_tasks.run_silver_transform().
-- =============================================================================

WITH details_dedup AS (
    SELECT
        match_id,
        league,
        season,
        match_facts_json,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, league, season
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fotmob_match_details
    WHERE match_facts_json IS NOT NULL
)

SELECT
    match_id,
    league,
    season,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.text')       AS referee_name,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.country')     AS referee_country,
    json_extract_scalar(match_facts_json, '$.infoBox.Referee.countryCode') AS referee_country_code,
    _ingested_at                                                          AS _bronze_ingested_at
FROM details_dedup
WHERE rn = 1
  AND json_extract_scalar(match_facts_json, '$.infoBox.Referee.text') IS NOT NULL
  AND json_extract_scalar(match_facts_json, '$.infoBox.Referee.text') <> ''
