-- FBref non-publishing canary acceptance for DataGrip / Trino.
-- Set :control_run_id to the PostgreSQL control-run UUID for the exact
-- 100-request / 50-MiB / shard-25 dag_ingest_fbref canary.
-- Run fbref_control_dataset_acceptance.sql against PostgreSQL with the same
-- UUID and bindings current/100/50. This script deliberately has no
-- fbref_target_scope or Silver check: the canary branch publishes neither.
-- Every statement below is read-only.
-- The verdict covers the run's cohort. OUTSIDE_COHORT route reviews in
-- PostgreSQL describe other routes without contributing a PASS or FAIL.

-- 1. Lossless generic Bronze parity for this canary batch.
WITH manifest AS (
    SELECT *
    FROM iceberg.bronze.fbref_page_manifest
    WHERE run_id = CAST(:control_run_id AS varchar)
), evidence AS (
    SELECT
        (SELECT count(*) FROM manifest) AS page_rows,
        (SELECT coalesce(sum(table_count), 0) FROM manifest) AS declared_tables,
        (SELECT coalesce(sum(cell_count), 0) FROM manifest) AS declared_cells,
        (SELECT count(*) FROM iceberg.bronze.fbref_table_inventory
          WHERE run_id = CAST(:control_run_id AS varchar)) AS inventory_rows,
        (SELECT count(*) FROM iceberg.bronze.fbref_table_cells
          WHERE run_id = CAST(:control_run_id AS varchar)) AS cell_rows,
        (SELECT count(*) FROM manifest
          WHERE parser_version IS DISTINCT FROM 'fbref-page-document-v6'
             OR parse_status IS DISTINCT FROM 'success'
             OR persist_status IS DISTINCT FROM 'success'
             OR validation_status IS DISTINCT FROM 'success') AS failed_pages
)
SELECT
    'generic_bronze' AS check_name,
    CASE
        WHEN page_rows > 0
         AND declared_tables = inventory_rows
         AND declared_cells = cell_rows
         AND failed_pages = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    *
FROM evidence;

-- 2. Typed table inventory. Optional all-empty datasets are proven by the
-- PostgreSQL manifest matrix. Match availability is scoped to this batch.
WITH expected(
    dataset, table_name, evidence_level, route_policy, physical_requirement
) AS (
    VALUES
        ('schedule', 'fbref_schedule', 'season', 'supported', 'required'),
        ('standings', 'fbref_standings', 'season', 'supported', 'optional'),
        ('squad_roster', 'fbref_squad_roster', 'squad', 'supported', 'optional'),
        ('player_stats', 'fbref_player_stats', 'season', 'supported', 'optional'),
        ('team_stats', 'fbref_team_stats', 'season', 'supported', 'optional'),
        ('player_shooting', 'fbref_player_shooting', 'season', 'supported', 'optional'),
        ('team_shooting', 'fbref_team_shooting', 'season', 'supported', 'optional'),
        ('player_playingtime', 'fbref_player_playingtime', 'season', 'supported', 'optional'),
        ('team_playingtime', 'fbref_team_playingtime', 'season', 'supported', 'optional'),
        ('player_misc', 'fbref_player_misc', 'season', 'supported', 'optional'),
        ('team_misc', 'fbref_team_misc', 'season', 'supported', 'optional'),
        ('keeper_keeper', 'fbref_keeper_keeper', 'season', 'supported', 'optional'),
        ('player_passing', 'fbref_player_passing', 'season', 'policy_exempt', 'optional'),
        ('team_passing', 'fbref_team_passing', 'season', 'policy_exempt', 'optional'),
        ('player_passing_types', 'fbref_player_passing_types', 'season', 'policy_exempt', 'optional'),
        ('team_passing_types', 'fbref_team_passing_types', 'season', 'policy_exempt', 'optional'),
        ('player_gca', 'fbref_player_gca', 'season', 'policy_exempt', 'optional'),
        ('team_gca', 'fbref_team_gca', 'season', 'policy_exempt', 'optional'),
        ('player_defense', 'fbref_player_defense', 'season', 'policy_exempt', 'optional'),
        ('team_defense', 'fbref_team_defense', 'season', 'policy_exempt', 'optional'),
        ('player_possession', 'fbref_player_possession', 'season', 'policy_exempt', 'optional'),
        ('team_possession', 'fbref_team_possession', 'season', 'policy_exempt', 'optional'),
        ('keeper_keeper_adv', 'fbref_keeper_keeper_adv', 'season', 'policy_exempt', 'optional'),
        ('shot_events', 'fbref_shot_events', 'match', 'supported', 'optional'),
        ('match_events', 'fbref_match_events', 'match', 'supported', 'optional'),
        ('lineups', 'fbref_lineups', 'match', 'supported', 'optional'),
        ('match_team_stats', 'fbref_match_team_stats', 'match', 'supported', 'optional'),
        ('match_managers', 'fbref_match_managers', 'match', 'supported', 'optional'),
        ('match_officials', 'fbref_match_officials', 'match', 'supported', 'optional'),
        ('match_keeper_stats', 'fbref_match_keeper_stats', 'match', 'supported', 'optional'),
        ('match_meta', 'fbref_match_meta', 'match', 'supported', 'optional'),
        ('match_player_stats', 'fbref_match_player_stats', 'match', 'supported', 'optional'),
        ('dataset_availability', 'fbref_dataset_availability', 'match', 'evidence', 'required')
), materialized AS (
    SELECT DISTINCT table_name
    FROM iceberg.information_schema.tables
    WHERE table_schema = 'bronze'
), match_signal AS (
    SELECT
        dataset,
        count(*) AS evidence_rows,
        count_if(availability = 'available') AS available_rows
    FROM iceberg.bronze.fbref_dataset_availability
    WHERE _batch_id = CAST(:control_run_id AS varchar)
    GROUP BY dataset
)
SELECT
    'typed_table_inventory' AS check_name,
    CASE
        WHEN route_policy = 'policy_exempt' THEN 'VERIFY_POSTGRES_DATED_ROUTE_REVIEW'
        WHEN expected.dataset IN ('standings', 'squad_roster', 'match_meta') THEN 'VERIFY_PAGE_RECEIPTS'
        WHEN physical_requirement = 'required'
         AND materialized.table_name IS NULL THEN 'FAIL'
        WHEN evidence_level = 'match'
         AND route_policy = 'supported'
         AND materialized.table_name IS NULL
         AND coalesce(match_signal.available_rows, 0) > 0 THEN 'FAIL'
        WHEN evidence_level = 'match'
         AND route_policy = 'supported'
         AND materialized.table_name IS NULL
         AND coalesce(match_signal.evidence_rows, 0) > 0
            THEN 'VERIFY_POSTGRES_EXPLICIT_EMPTY'
        WHEN evidence_level = 'match'
         AND route_policy = 'supported'
         AND materialized.table_name IS NULL THEN 'FAIL'
        WHEN evidence_level = 'season'
         AND route_policy = 'supported'
         AND materialized.table_name IS NULL
            THEN 'VERIFY_POSTGRES_TABLE_REQUIREMENT'
        ELSE 'PASS'
    END AS verdict,
    expected.*,
    materialized.table_name IS NOT NULL AS table_materialized,
    coalesce(match_signal.evidence_rows, 0) AS match_evidence_rows,
    coalesce(match_signal.available_rows, 0) AS match_available_rows
FROM expected
LEFT JOIN materialized
  ON materialized.table_name = expected.table_name
LEFT JOIN match_signal
  ON match_signal.dataset = expected.dataset
ORDER BY evidence_level, dataset;

-- 3. Generate one exact batch-row query for every materialized typed table.
-- Execute every returned query. A zero count is accepted only when the
-- PostgreSQL companion says dataset_requires_materialized_table is false.
WITH expected(dataset, table_name) AS (
    VALUES
        ('schedule', 'fbref_schedule'),
        ('standings', 'fbref_standings'),
        ('player_stats', 'fbref_player_stats'),
        ('team_stats', 'fbref_team_stats'),
        ('player_shooting', 'fbref_player_shooting'),
        ('team_shooting', 'fbref_team_shooting'),
        ('player_playingtime', 'fbref_player_playingtime'),
        ('team_playingtime', 'fbref_team_playingtime'),
        ('player_misc', 'fbref_player_misc'),
        ('team_misc', 'fbref_team_misc'),
        ('keeper_keeper', 'fbref_keeper_keeper'),
        ('player_passing', 'fbref_player_passing'),
        ('team_passing', 'fbref_team_passing'),
        ('player_passing_types', 'fbref_player_passing_types'),
        ('team_passing_types', 'fbref_team_passing_types'),
        ('player_gca', 'fbref_player_gca'),
        ('team_gca', 'fbref_team_gca'),
        ('player_defense', 'fbref_player_defense'),
        ('team_defense', 'fbref_team_defense'),
        ('player_possession', 'fbref_player_possession'),
        ('team_possession', 'fbref_team_possession'),
        ('keeper_keeper_adv', 'fbref_keeper_keeper_adv'),
        ('shot_events', 'fbref_shot_events'),
        ('match_events', 'fbref_match_events'),
        ('lineups', 'fbref_lineups'),
        ('match_team_stats', 'fbref_match_team_stats'),
        ('match_managers', 'fbref_match_managers'),
        ('match_officials', 'fbref_match_officials'),
        ('match_keeper_stats', 'fbref_match_keeper_stats'),
        ('match_player_stats', 'fbref_match_player_stats'),
        ('match_meta', 'fbref_match_meta'),
        ('dataset_availability', 'fbref_dataset_availability')
), materialized AS (
    SELECT DISTINCT table_name
    FROM iceberg.information_schema.tables
    WHERE table_schema = 'bronze'
)
SELECT
    dataset,
    'SELECT ''' || dataset || ''' AS dataset, count(*) AS batch_rows ' ||
    'FROM iceberg.bronze."' || expected.table_name || '" ' ||
    'WHERE _batch_id = ''' || CAST(:control_run_id AS varchar) || ''''
        AS datagrip_verification_sql
FROM expected
JOIN materialized
  ON materialized.table_name = expected.table_name
ORDER BY dataset;

-- Physical page readback for each source target.
-- Run each generated query with the same :control_run_id and the unmodified
-- :control_page_receipts_json returned by the PostgreSQL companion.
-- Both directions are checked, including missing optional tables and extra targets.
WITH candidate(dataset, table_name, target_key) AS (
    VALUES
        ('match_meta', 'fbref_match_meta', '''fbref:match:'' || match_id'),
        ('standings', 'fbref_standings', 'source_target_id'),
        ('squad_roster', 'fbref_squad_roster', 'source_target_id')
), materialized AS (
    SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema = 'bronze'
)
SELECT dataset, format(
'WITH args AS (
    SELECT CAST(:control_run_id AS varchar) AS run_id,
           json_parse(CAST(:control_page_receipts_json AS varchar)) AS envelope,
           ''%s'' AS dataset
), expected AS (
    SELECT json_extract_scalar(item, ''$.target_id'') AS target_id,
           json_extract_scalar(item, ''$.content_hash'') AS content_hash,
           json_extract_scalar(item, ''$.competition_id'') AS competition_id,
           json_extract_scalar(item, ''$.season_id'') AS season_id,
           CAST(json_extract_scalar(item, ''$.expected_rows'') AS bigint) AS expected_rows,
           CAST(json_extract_scalar(item, ''$.ready'') AS boolean) AS ready
    FROM args CROSS JOIN UNNEST(CAST(json_extract(envelope, ''$.receipts'') AS array(json))) AS receipts(item)
    WHERE json_extract_scalar(item, ''$.dataset'') = args.dataset
), physical AS (
    %s
), counts AS (
    SELECT p.target_id, sum(p.n) AS physical_rows,
           count_if(args.dataset <> ''squad_roster'' AND
               (p.competition_id IS DISTINCT FROM e.competition_id OR p.season_id IS DISTINCT FROM e.season_id))
               AS wrong_scope
    FROM physical p CROSS JOIN args LEFT JOIN expected e ON e.target_id = p.target_id
    GROUP BY p.target_id
), pages AS (
    SELECT page.target_id, min(page.content_hash) AS content_hash, count(*) AS versions
    FROM iceberg.bronze.fbref_page_manifest page JOIN args ON page.run_id = args.run_id
    WHERE page.parser_version = ''fbref-page-document-v6''
      AND page.parse_status = ''success'' AND page.persist_status = ''success'' AND page.validation_status = ''success''
    GROUP BY page.target_id
), checked AS (
    SELECT COALESCE(e.target_id, c.target_id) AS target_id,
           e.expected_rows, COALESCE(c.physical_rows, 0) AS physical_rows,
           COALESCE(e.ready AND page.versions = 1 AND page.content_hash = e.content_hash
               AND e.expected_rows = COALESCE(c.physical_rows, 0) AND COALESCE(c.wrong_scope, 0) = 0, false) AS ok
    FROM expected e FULL OUTER JOIN counts c ON c.target_id = e.target_id
    LEFT JOIN pages page ON page.target_id = e.target_id
)
SELECT args.dataset, CASE WHEN json_extract_scalar(envelope, ''$.control_run_id'') = run_id
           AND json_extract_scalar(envelope, ''$.generic_parser_version'') = ''fbref-page-document-v6''
           AND json_extract_scalar(envelope, ''$.typed_parser_version'') = ''fbref-typed-bronze-v8''
           AND cardinality(CAST(json_extract(envelope, ''$.receipts'') AS array(json))) IS NOT NULL
           AND failures = 0 THEN ''PASS'' ELSE ''FAIL'' END AS verdict,
       expected_rows, physical_rows, failures, failed_targets
FROM args CROSS JOIN (
    SELECT COALESCE(sum(expected_rows), 0) AS expected_rows, COALESCE(sum(physical_rows), 0) AS physical_rows,
           COALESCE(count_if(NOT ok), 0) AS failures, array_agg(target_id ORDER BY target_id) FILTER (WHERE NOT ok) AS failed_targets
    FROM checked
) result',
    dataset,
    CASE WHEN materialized.table_name IS NULL THEN
         'SELECT CAST(NULL AS varchar) AS target_id, CAST(NULL AS varchar) AS competition_id, ' ||
         'CAST(NULL AS varchar) AS season_id, CAST(0 AS bigint) AS n WHERE false'
    ELSE 'SELECT ' || target_key || ' AS target_id, source_competition_id AS competition_id, ' ||
         'source_season_id AS season_id, count(*) AS n FROM iceberg.bronze."' || candidate.table_name ||
         '" WHERE _batch_id = (SELECT run_id FROM args) GROUP BY 1, 2, 3' END
) AS datagrip_verification_sql
FROM candidate LEFT JOIN materialized ON materialized.table_name = candidate.table_name
ORDER BY dataset;

-- 4. Explicit match-dataset availability written by this canary batch.
SELECT
    dataset,
    availability,
    count(*) AS evidence_rows,
    count(DISTINCT source_competition_id) AS competitions,
    count(DISTINCT source_season_id) AS seasons
FROM iceberg.bronze.fbref_dataset_availability
WHERE _batch_id = CAST(:control_run_id AS varchar)
GROUP BY dataset, availability
ORDER BY dataset, availability;

-- 5. The canary must leave no FBref staging table behind.
SELECT
    'fbref_staging_tables' AS check_name,
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict,
    count(*) AS staging_table_count,
    array_agg(table_name ORDER BY table_name) FILTER (
        WHERE table_name IS NOT NULL
    ) AS staging_tables
FROM iceberg.information_schema.tables
WHERE table_schema = 'bronze'
  AND regexp_like(table_name, '^fbref_.*__stg_');
