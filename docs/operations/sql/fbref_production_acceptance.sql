-- FBref production acceptance for DataGrip / Trino.
-- Set :control_run_id to the accepted PostgreSQL control run UUID.
-- Set :expected_current_male_competitions from the control PostgreSQL
-- datasource (never hardcode it):
--   SELECT count(*)
--   FROM fbref_control.competition_registry
--   WHERE source = 'fbref' AND gender = 'male' AND crawl_state = 'active'
--     AND lifecycle_state IN ('present', 'missing_once') AND present;
-- Run fbref_control_dataset_acceptance.sql against PostgreSQL for the same
-- control_run_id. It is the authoritative evidence for legitimate empty
-- season datasets, which intentionally have no materialized Iceberg table;
-- production acceptance requires zero FAIL rows from that matrix. For every
-- VERIFY_POSTGRES_TABLE_REQUIREMENT row below, the companion's
-- dataset_requires_materialized_table must be false; true means the absent
-- Iceberg payload table is a production FAIL. The same cross-check is required
-- for VERIFY_POSTGRES_EXPLICIT_EMPTY match rows.
-- Every statement below is read-only.

-- 1. Immutable publication scope. Every active/present male competition in
-- this exported generation must have exactly one eligible current canonical
-- season. The expected count comes from discovery rather than a constant.
WITH scope AS (
    SELECT *
    FROM iceberg.bronze.fbref_target_scope
    WHERE source = 'fbref'
      AND control_run_id = CAST(:control_run_id AS varchar)
), active_male_competitions AS (
    SELECT DISTINCT source_competition_id
    FROM scope
    WHERE gender = 'male'
      AND competition_crawl_state = 'active'
      AND competition_lifecycle_state IN ('present', 'missing_once')
      AND competition_present
), current_canonical AS (
    SELECT
        competition.source_competition_id,
        count_if(
            scope.scope_kind = 'canonical'
            AND scope.eligible_male
            AND scope.season_is_current
        ) AS current_season_count
    FROM active_male_competitions AS competition
    LEFT JOIN scope
      ON scope.source_competition_id = competition.source_competition_id
    GROUP BY competition.source_competition_id
), evidence AS (
    SELECT
        count(*) AS scope_rows,
        count_if(eligible_male) AS eligible_male_rows,
        count(DISTINCT scope_hash) AS distinct_scope_hashes,
        count_if(scope_hash IS NULL) AS null_scope_hashes,
        count(*) - count(DISTINCT (
            source_competition_id, source_season_id, scope_kind
        )) AS duplicate_scope_rows,
        count_if(
            eligible_male IS DISTINCT FROM (
                gender = 'male'
                AND competition_crawl_state = 'active'
                AND competition_lifecycle_state IN ('present', 'missing_once')
                AND competition_present
                AND season_lifecycle_state = 'present'
                AND season_present
            )
        ) AS eligibility_flag_mismatches,
        count_if(
            scope_kind = 'alias'
            AND NOT EXISTS (
                SELECT 1
                FROM scope AS canonical
                WHERE canonical.scope_kind = 'canonical'
                  AND canonical.source_competition_id =
                      scope.source_competition_id
                  AND canonical.canonical_season_id =
                      scope.canonical_season_id
            )
        ) AS orphan_alias_rows,
        count_if(
            eligible_male
            AND (
                nullif(trim(source_competition_id), '') IS NULL
                OR nullif(trim(source_season_id), '') IS NULL
                OR nullif(trim(canonical_season_id), '') IS NULL
            )
        ) AS blank_eligible_id_rows,
        min(scope_hash) AS scope_hash,
        max(exported_at) AS exported_at
    FROM scope
), competition_evidence AS (
    SELECT
        count(*) AS active_male_competitions,
        count_if(current_season_count = 1) AS complete_current_competitions,
        count_if(current_season_count <> 1) AS incomplete_current_competitions
    FROM current_canonical
)
SELECT
    'publication_scope' AS check_name,
    CASE
        WHEN scope_rows > 0
         AND eligible_male_rows > 0
         AND distinct_scope_hashes = 1
         AND null_scope_hashes = 0
         AND duplicate_scope_rows = 0
         AND eligibility_flag_mismatches = 0
         AND orphan_alias_rows = 0
         AND blank_eligible_id_rows = 0
         AND active_male_competitions > 0
         AND active_male_competitions =
             CAST(:expected_current_male_competitions AS bigint)
         AND complete_current_competitions = active_male_competitions
         AND incomplete_current_competitions = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    evidence.*,
    competition_evidence.*,
    CAST(:expected_current_male_competitions AS bigint)
        AS expected_current_male_competitions
FROM evidence
CROSS JOIN competition_evidence;

SELECT DISTINCT
    source_competition_id,
    competition_name,
    source_season_id,
    season_label,
    scope_kind
FROM iceberg.bronze.fbref_target_scope
WHERE source = 'fbref'
  AND control_run_id = CAST(:control_run_id AS varchar)
  AND eligible_male
  AND season_is_current
ORDER BY competition_name, source_season_id;

-- 2. Lossless generic Bronze parity for this run.
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
          WHERE parse_status <> 'success'
             OR persist_status <> 'success'
             OR validation_status <> 'success') AS failed_pages
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

-- 3a. Typed table inventory. Typed writers deliberately do not create a table
-- for an all-empty dataset. Match emptiness is proven by
-- fbref_dataset_availability; season emptiness is proven by the PostgreSQL
-- control-manifest companion query. Policy-skipped routes are explicit rather
-- than being misreported as missing production data.
WITH expected(
    dataset, table_name, evidence_level, route_policy, physical_requirement
) AS (
    VALUES
        ('schedule', 'fbref_schedule', 'season', 'supported', 'required'),
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
    GROUP BY dataset
)
SELECT
    'typed_table_inventory' AS check_name,
    CASE
        WHEN route_policy = 'policy_exempt' THEN 'PASS_POLICY_EXEMPT'
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

-- 3b. DataGrip query generator for row counts and male-scope checks. Only
-- existing tables are referenced in the generated SQL, so intentionally
-- absent all-empty/policy-exempt tables never break planning. Execute the
-- returned statements; every outside_scope_rows value must be zero.
WITH expected(dataset, table_name) AS (
    VALUES
        ('schedule', 'fbref_schedule'),
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
        ('dataset_availability', 'fbref_dataset_availability')
), materialized AS (
    SELECT DISTINCT table_name
    FROM iceberg.information_schema.tables
    WHERE table_schema = 'bronze'
)
SELECT
    dataset,
    'SELECT ''' || dataset || ''' AS dataset, count(*) AS batch_rows, ' ||
    'count_if(scope.source_competition_id IS NULL) AS outside_scope_rows ' ||
    'FROM iceberg.bronze."' || expected.table_name || '" AS typed ' ||
    'LEFT JOIN iceberg.bronze.fbref_target_scope AS scope ON ' ||
    'scope.source = ''fbref'' AND scope.control_run_id = ''' ||
    CAST(:control_run_id AS varchar) || ''' AND scope.eligible_male AND ' ||
    'scope.source_competition_id = typed.source_competition_id AND ' ||
    'scope.source_season_id = typed.source_season_id ' ||
    'WHERE typed._batch_id = ''' || CAST(:control_run_id AS varchar) || ''';'
        AS datagrip_verification_sql
FROM expected
JOIN materialized
  ON materialized.table_name = expected.table_name
ORDER BY dataset;

-- Explicit match-dataset availability for the accepted batch.
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

-- 3c. Eligible competition-season x match-dataset evidence. A direct-match
-- tournament has no schedule page, so its availability rows are the expected
-- evidence. Ordinary seasons require an availability record for every
-- published match-report URL. Upcoming fixtures without a report URL are not
-- match-page targets yet.
WITH scope_tokens AS (
    SELECT DISTINCT
        source_competition_id,
        source_season_id,
        canonical_season_id,
        season_is_current,
        direct_match_only
    FROM iceberg.bronze.fbref_target_scope
    WHERE source = 'fbref'
      AND control_run_id = CAST(:control_run_id AS varchar)
      AND eligible_male
), eligible_seasons AS (
    SELECT DISTINCT
        source_competition_id,
        canonical_season_id,
        season_is_current,
        direct_match_only
    FROM scope_tokens
), required_match_datasets(dataset) AS (
    VALUES
        ('shot_events'),
        ('match_events'),
        ('lineups'),
        ('match_team_stats'),
        ('match_managers'),
        ('match_officials'),
        ('match_keeper_stats'),
        ('match_player_stats')
), schedule_partitions AS (
    SELECT
        token.source_competition_id,
        token.canonical_season_id,
        count(*) AS schedule_rows
    FROM iceberg.bronze.fbref_schedule AS schedule
    JOIN scope_tokens AS token
      ON token.source_competition_id = schedule.source_competition_id
     AND token.source_season_id = schedule.source_season_id
    GROUP BY token.source_competition_id, token.canonical_season_id
), scheduled_match_ids AS (
    SELECT DISTINCT
        token.source_competition_id,
        token.canonical_season_id,
        nullif(
            regexp_extract(schedule.match_url, '/matches/([a-f0-9]+)/', 1),
            ''
        ) AS match_id
    FROM iceberg.bronze.fbref_schedule AS schedule
    JOIN scope_tokens AS token
      ON token.source_competition_id = schedule.source_competition_id
     AND token.source_season_id = schedule.source_season_id
    WHERE nullif(
        regexp_extract(schedule.match_url, '/matches/([a-f0-9]+)/', 1), ''
    ) IS NOT NULL
), availability_ids AS (
    SELECT DISTINCT
        token.source_competition_id,
        token.canonical_season_id,
        availability.dataset,
        availability.match_id
    FROM iceberg.bronze.fbref_dataset_availability AS availability
    JOIN scope_tokens AS token
      ON token.source_competition_id = availability.source_competition_id
     AND token.source_season_id = availability.source_season_id
    WHERE availability.availability IN (
        'available', 'empty', 'restricted', 'not_applicable'
    )
), expected_match_evidence AS (
    SELECT
        scheduled.source_competition_id,
        scheduled.canonical_season_id,
        required.dataset,
        scheduled.match_id
    FROM scheduled_match_ids AS scheduled
    CROSS JOIN required_match_datasets AS required
), completeness AS (
    SELECT
        expected.source_competition_id,
        expected.canonical_season_id,
        expected.dataset,
        count(*) AS expected_matches,
        count_if(availability.match_id IS NULL) AS missing_matches,
        array_agg(expected.match_id ORDER BY expected.match_id) FILTER (
            WHERE availability.match_id IS NULL
        ) AS missing_match_ids
    FROM expected_match_evidence AS expected
    LEFT JOIN availability_ids AS availability
      ON availability.source_competition_id = expected.source_competition_id
     AND availability.canonical_season_id = expected.canonical_season_id
     AND availability.dataset = expected.dataset
     AND availability.match_id = expected.match_id
    GROUP BY expected.source_competition_id, expected.canonical_season_id,
             expected.dataset
), availability_summary AS (
    SELECT
        source_competition_id,
        canonical_season_id,
        dataset,
        count(*) AS availability_matches
    FROM availability_ids
    GROUP BY source_competition_id, canonical_season_id, dataset
), extra_evidence AS (
    SELECT
        availability.source_competition_id,
        availability.canonical_season_id,
        availability.dataset,
        count(*) AS extra_matches,
        array_agg(availability.match_id ORDER BY availability.match_id)
            AS extra_match_ids
    FROM availability_ids AS availability
    LEFT JOIN scheduled_match_ids AS scheduled
      ON scheduled.source_competition_id = availability.source_competition_id
     AND scheduled.canonical_season_id = availability.canonical_season_id
     AND scheduled.match_id = availability.match_id
    WHERE scheduled.match_id IS NULL
    GROUP BY availability.source_competition_id,
             availability.canonical_season_id, availability.dataset
)
SELECT
    'match_dataset_matrix' AS check_name,
    CASE
        WHEN season.direct_match_only
         AND coalesce(availability.availability_matches, 0) > 0
            THEN 'VERIFY_POSTGRES_DIRECT_MATCH_IDENTITIES'
        WHEN season.direct_match_only THEN 'FAIL'
        WHEN coalesce(schedule.schedule_rows, 0) = 0 THEN 'FAIL'
        WHEN coalesce(completeness.expected_matches, 0) = 0
         AND coalesce(availability.availability_matches, 0) = 0
            THEN 'PASS_NO_PUBLISHED_MATCH_REPORTS'
        WHEN coalesce(completeness.missing_matches, 0) = 0
         AND coalesce(extras.extra_matches, 0) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS verdict,
    season.source_competition_id,
    season.canonical_season_id AS source_season_id,
    season.season_is_current,
    season.direct_match_only,
    required.dataset,
    coalesce(schedule.schedule_rows, 0) AS schedule_rows,
    coalesce(completeness.expected_matches, 0) AS expected_matches,
    coalesce(completeness.missing_matches, 0) AS missing_matches,
    completeness.missing_match_ids,
    coalesce(availability.availability_matches, 0) AS availability_matches,
    coalesce(extras.extra_matches, 0) AS extra_matches,
    extras.extra_match_ids
FROM eligible_seasons AS season
CROSS JOIN required_match_datasets AS required
LEFT JOIN schedule_partitions AS schedule
  ON schedule.source_competition_id = season.source_competition_id
 AND schedule.canonical_season_id = season.canonical_season_id
LEFT JOIN completeness
  ON completeness.source_competition_id = season.source_competition_id
 AND completeness.canonical_season_id = season.canonical_season_id
 AND completeness.dataset = required.dataset
LEFT JOIN availability_summary AS availability
  ON availability.source_competition_id = season.source_competition_id
 AND availability.canonical_season_id = season.canonical_season_id
 AND availability.dataset = required.dataset
LEFT JOIN extra_evidence AS extras
  ON extras.source_competition_id = season.source_competition_id
 AND extras.canonical_season_id = season.canonical_season_id
 AND extras.dataset = required.dataset
ORDER BY season_is_current DESC, source_competition_id, source_season_id,
         dataset;

-- 3d. Season-route policy matrix. Policy-exempt routes and direct-match
-- editions are complete without a page. Supported rows are candidates only:
-- the PostgreSQL companion filters them to source-discovered route targets and
-- proves their explicit empty/restricted manifests.
WITH eligible_seasons AS (
    SELECT DISTINCT
        source_competition_id,
        canonical_season_id,
        season_is_current,
        direct_match_only
    FROM iceberg.bronze.fbref_target_scope
    WHERE source = 'fbref'
      AND control_run_id = CAST(:control_run_id AS varchar)
      AND eligible_male
      AND scope_kind = 'canonical'
), season_datasets(dataset, stat_route, route_policy) AS (
    VALUES
        ('player_stats', 'standard', 'supported'),
        ('team_stats', 'standard', 'supported'),
        ('player_shooting', 'shooting', 'supported'),
        ('team_shooting', 'shooting', 'supported'),
        ('player_playingtime', 'playingtime', 'supported'),
        ('team_playingtime', 'playingtime', 'supported'),
        ('player_misc', 'misc', 'supported'),
        ('team_misc', 'misc', 'supported'),
        ('keeper_keeper', 'keepers', 'supported'),
        ('player_passing', 'passing', 'policy_exempt'),
        ('team_passing', 'passing', 'policy_exempt'),
        ('player_passing_types', 'passing_types', 'policy_exempt'),
        ('team_passing_types', 'passing_types', 'policy_exempt'),
        ('player_gca', 'gca', 'policy_exempt'),
        ('team_gca', 'gca', 'policy_exempt'),
        ('player_defense', 'defense', 'policy_exempt'),
        ('team_defense', 'defense', 'policy_exempt'),
        ('player_possession', 'possession', 'policy_exempt'),
        ('team_possession', 'possession', 'policy_exempt'),
        ('keeper_keeper_adv', 'keepersadv', 'policy_exempt')
)
SELECT
    'season_dataset_matrix' AS check_name,
    CASE
        WHEN season.direct_match_only THEN 'PASS_DIRECT_MATCH_ONLY'
        WHEN dataset.route_policy = 'policy_exempt' THEN 'PASS_POLICY_EXEMPT'
        ELSE 'VERIFY_POSTGRES_DISCOVERED_ROUTE_MANIFEST'
    END AS verdict,
    season.*,
    dataset.dataset,
    dataset.stat_route,
    dataset.route_policy
FROM eligible_seasons AS season
CROSS JOIN season_datasets AS dataset
ORDER BY season_is_current DESC, source_competition_id, canonical_season_id,
         route_policy, stat_route, dataset;

-- 4. Silver appeared after this scope generation and natural keys are unique.
-- min(_silver_created_at), plus a NULL count, prevents one fresh row from
-- masking an old or unattributed full-table replacement.
WITH scope AS (
    SELECT max(exported_at) AS exported_at
    FROM iceberg.bronze.fbref_target_scope
    WHERE source = 'fbref'
      AND control_run_id = CAST(:control_run_id AS varchar)
), match_silver AS (
    SELECT count(*) AS rows,
           count_if(match_id IS NULL OR date IS NULL) AS null_keys,
           count(*) - count(DISTINCT match_id) AS duplicate_keys,
           count_if(_silver_created_at IS NULL) AS null_freshness,
           min(_silver_created_at) AS silver_created_at
    FROM iceberg.silver.fbref_match_enriched
), player_silver AS (
    SELECT count(*) AS rows,
           count_if(match_id IS NULL OR player_id IS NULL OR team IS NULL)
               AS null_keys,
           count(*) - count(DISTINCT (match_id, player_id, team))
               AS duplicate_keys,
           count_if(_silver_created_at IS NULL) AS null_freshness,
           min(_silver_created_at) AS silver_created_at
    FROM iceberg.silver.fbref_player_match_stats
)
SELECT
    dataset,
    CASE
        WHEN rows > 0
         AND null_keys = 0
         AND duplicate_keys = 0
         AND null_freshness = 0
         AND CAST(silver_created_at AS timestamp(6)) >= scope.exported_at
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    rows,
    null_keys,
    duplicate_keys,
    null_freshness,
    silver_created_at,
    scope.exported_at AS scope_exported_at
FROM (
    SELECT 'fbref_match_enriched' AS dataset, * FROM match_silver
    UNION ALL
    SELECT 'fbref_player_match_stats', * FROM player_silver
) AS silver
CROSS JOIN scope
ORDER BY dataset;

-- 5. Production acceptance requires no retained FBref staging tables.
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

-- Raw S3 integrity is intentionally not inferred from Trino. Run the
-- PostgreSQL companion query with the same control_run_id: crawl_run.metadata
-- contains the create-once passed artifact digest, path, attempt fingerprint,
-- and run identity that DataGrip can verify without reading Airflow XCom.
