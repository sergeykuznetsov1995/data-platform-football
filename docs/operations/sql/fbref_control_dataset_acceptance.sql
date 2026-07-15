-- FBref typed season-dataset acceptance for DataGrip / PostgreSQL.
-- Set :control_run_id to the same UUID used by the Trino acceptance script.
-- This control-plane evidence is required because a legitimate empty typed
-- dataset intentionally does not create an Iceberg table.

SELECT
    'control_run' AS check_name,
    CASE
        WHEN status = 'succeeded'
         AND metadata ? 'raw_baseline'
         AND metadata ? 'raw_audit'
         AND metadata -> 'raw_audit' ->> 'status' = 'passed'
         AND metadata -> 'raw_audit' ->> 'processing_control_run_id'
             = run_id::text
         AND metadata -> 'raw_audit' ->> 'artifact_sha256'
             ~ '^[0-9a-f]{64}$'
         AND (
             run_type = 'replay'
             OR metadata ? 'raw_fetch_attempt_snapshot'
         )
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    run_id,
    run_type,
    status,
    started_at,
    finished_at,
    metadata -> 'raw_baseline' AS raw_baseline_anchor,
    metadata -> 'raw_fetch_attempt_snapshot' AS raw_attempt_snapshot,
    metadata -> 'raw_audit' AS raw_audit_anchor
FROM fbref_control.crawl_run
WHERE run_id = CAST(:control_run_id AS uuid);

WITH route_targets AS (
    SELECT
        CASE
            WHEN frontier.page_kind = 'season' THEN 'standard'
            ELSE frontier.source_ids ->> 'stat_route'
        END AS stat_route
    FROM fbref_control.page_frontier AS frontier
    JOIN fbref_control.competition_registry AS competition
      ON competition.source = frontier.source
     AND competition.competition_id =
         frontier.source_ids ->> 'competition_id'
    JOIN fbref_control.season_registry AS season
      ON season.source = frontier.source
     AND season.competition_id = competition.competition_id
     AND season.season_id = frontier.source_ids ->> 'season_id'
    WHERE frontier.source = 'fbref'
      AND frontier.page_kind IN ('season', 'season_stats')
      AND competition.gender = 'male'
      AND competition.crawl_state = 'active'
      AND competition.lifecycle_state IN ('present', 'missing_once')
      AND competition.present
      AND season.lifecycle_state = 'present'
      AND season.present
)
SELECT
    'supported_season_route_frontier' AS check_name,
    CASE
        WHEN count(*) FILTER (
            WHERE stat_route IN (
                'standard', 'shooting', 'playingtime', 'misc', 'keepers'
            )
        ) > 0
         AND count(*) FILTER (
            WHERE stat_route IN (
                'passing', 'passing_types', 'gca', 'defense', 'possession',
                'keepersadv'
            )
        ) = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    count(*) FILTER (
        WHERE stat_route IN (
            'standard', 'shooting', 'playingtime', 'misc', 'keepers'
        )
    ) AS supported_route_targets,
    count(*) FILTER (
        WHERE stat_route IN (
            'passing', 'passing_types', 'gca', 'defense', 'possession',
            'keepersadv'
        )
    ) AS policy_exempt_route_targets
FROM route_targets;

WITH eligible_seasons AS (
    SELECT
        season.competition_id,
        season.season_id
    FROM fbref_control.season_registry AS season
    JOIN fbref_control.competition_registry AS competition
      ON competition.source = season.source
     AND competition.competition_id = season.competition_id
    WHERE season.source = 'fbref'
      AND competition.gender = 'male'
      AND competition.crawl_state = 'active'
      AND competition.lifecycle_state IN ('present', 'missing_once')
      AND competition.present
      AND season.lifecycle_state = 'present'
      AND season.present
      AND COALESCE(
          season.metadata ->> 'direct_match_only' = 'true', false
      ) = false
), required(dataset, stat_route) AS (
    VALUES
        ('player_stats', 'standard'),
        ('team_stats', 'standard'),
        ('player_shooting', 'shooting'),
        ('team_shooting', 'shooting'),
        ('player_playingtime', 'playingtime'),
        ('team_playingtime', 'playingtime'),
        ('player_misc', 'misc'),
        ('team_misc', 'misc'),
        ('keeper_keeper', 'keepers')
), discovered_requirements AS (
    SELECT
        season.competition_id,
        season.season_id,
        required.dataset,
        required.stat_route,
        frontier.target_id
    FROM eligible_seasons AS season
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.source = 'fbref'
     AND frontier.page_kind IN ('season', 'season_stats')
     AND frontier.source_ids ->> 'competition_id' = season.competition_id
     AND frontier.source_ids ->> 'season_id' = season.season_id
    JOIN required
      ON required.stat_route = CASE
          WHEN frontier.page_kind = 'season' THEN 'standard'
          ELSE frontier.source_ids ->> 'stat_route'
      END
), latest_observation AS (
    SELECT DISTINCT ON (processing.target_id)
        processing.target_id,
        processing.content_hash,
        processing.typed_parser_version,
        processing.status AS processing_status,
        processing.typed_status,
        processing.validation_status AS processing_validation_status,
        processing.completed_at
    FROM fbref_control.observation_processing AS processing
    ORDER BY processing.target_id,
             COALESCE(processing.completed_at, processing.updated_at) DESC,
             processing.typed_parser_version DESC
), matrix AS (
    SELECT
        requirement.competition_id,
        requirement.season_id,
        requirement.dataset,
        requirement.stat_route,
        requirement.target_id,
        observation.content_hash,
        observation.typed_parser_version,
        observation.processing_status,
        observation.typed_status,
        observation.processing_validation_status,
        completion.dataset IS NOT NULL AS page_complete,
        manifest.availability,
        manifest.parse_status,
        manifest.persistence_status,
        manifest.validation_status,
        manifest.row_count
    FROM discovered_requirements AS requirement
    LEFT JOIN latest_observation AS observation
      ON observation.target_id = requirement.target_id
    LEFT JOIN fbref_control.dataset_manifest AS completion
      ON completion.target_id = observation.target_id
     AND completion.content_hash = observation.content_hash
     AND completion.parser_version = observation.typed_parser_version
     AND completion.dataset = 'typed:__complete__'
     AND completion.parse_status = 'succeeded'
     AND completion.persistence_status = 'succeeded'
     AND completion.validation_status = 'succeeded'
    LEFT JOIN fbref_control.dataset_manifest AS manifest
      ON manifest.target_id = observation.target_id
     AND manifest.content_hash = observation.content_hash
     AND manifest.parser_version = observation.typed_parser_version
     AND manifest.dataset = 'typed:' || requirement.dataset
)
SELECT
    'season_control_manifest_matrix' AS check_name,
    CASE
        WHEN content_hash IS NOT NULL
         AND processing_status = 'succeeded'
         AND typed_status = 'succeeded'
         AND processing_validation_status = 'succeeded'
         AND page_complete
         AND availability IN (
             'available', 'empty', 'restricted', 'not_applicable'
         )
         AND (
             (availability = 'available' AND row_count > 0)
             OR (availability IN ('empty', 'restricted', 'not_applicable')
                 AND row_count = 0)
         )
         AND parse_status = 'succeeded'
         AND persistence_status IN ('succeeded', 'skipped')
         AND validation_status IN ('succeeded', 'skipped')
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    'fbref_' || dataset AS physical_table_name,
    bool_or(availability = 'available' AND row_count > 0)
        OVER (PARTITION BY dataset) AS dataset_requires_materialized_table,
    *
FROM matrix
ORDER BY competition_id, season_id, stat_route, dataset;

-- Prove every eligible match identity from the frontier and require a complete
-- typed manifest for every dataset. Direct-match editions use these identities
-- in place of a schedule page. For any absent Iceberg payload table,
-- dataset_requires_materialized_table must be false.
WITH eligible_matches AS (
    SELECT
        frontier.target_id,
        frontier.source_ids ->> 'competition_id' AS competition_id,
        frontier.source_ids ->> 'season_id' AS season_id,
        frontier.source_ids ->> 'match_id' AS match_id,
        COALESCE(
            season.metadata ->> 'direct_match_only' = 'true', false
        ) AS direct_match_only
    FROM fbref_control.page_frontier AS frontier
    JOIN fbref_control.competition_registry AS competition
      ON competition.source = frontier.source
     AND competition.competition_id =
         frontier.source_ids ->> 'competition_id'
    JOIN fbref_control.season_registry AS season
      ON season.source = frontier.source
     AND season.competition_id = competition.competition_id
     AND season.season_id = frontier.source_ids ->> 'season_id'
    WHERE frontier.source = 'fbref'
      AND frontier.page_kind = 'match'
      AND competition.gender = 'male'
      AND competition.crawl_state = 'active'
      AND competition.lifecycle_state IN ('present', 'missing_once')
      AND competition.present
      AND season.lifecycle_state = 'present'
      AND season.present
), required(dataset) AS (
    VALUES
        ('shot_events'),
        ('match_events'),
        ('lineups'),
        ('match_team_stats'),
        ('match_managers'),
        ('match_officials'),
        ('match_keeper_stats'),
        ('match_player_stats')
), latest_observation AS (
    SELECT DISTINCT ON (processing.target_id)
        processing.target_id,
        processing.content_hash,
        processing.typed_parser_version,
        processing.status AS processing_status,
        processing.typed_status,
        processing.validation_status AS processing_validation_status
    FROM fbref_control.observation_processing AS processing
    ORDER BY processing.target_id,
             COALESCE(processing.completed_at, processing.updated_at) DESC,
             processing.typed_parser_version DESC
)
SELECT
    'match_control_manifest_matrix' AS check_name,
    CASE
        WHEN observation.content_hash IS NOT NULL
         AND observation.processing_status = 'succeeded'
         AND observation.typed_status = 'succeeded'
         AND observation.processing_validation_status = 'succeeded'
         AND completion.dataset IS NOT NULL
         AND manifest.availability IN (
             'available', 'empty', 'restricted', 'not_applicable'
         )
         AND (
             (manifest.availability = 'available' AND manifest.row_count > 0)
             OR (
                 manifest.availability IN (
                     'empty', 'restricted', 'not_applicable'
                 )
                 AND manifest.row_count = 0
             )
         )
         AND manifest.parse_status = 'succeeded'
         AND manifest.persistence_status IN ('succeeded', 'skipped')
         AND manifest.validation_status IN ('succeeded', 'skipped')
        THEN 'PASS' ELSE 'FAIL'
    END AS verdict,
    'fbref_' || required.dataset AS physical_table_name,
    bool_or(
        manifest.availability = 'available' AND manifest.row_count > 0
    ) OVER (PARTITION BY required.dataset)
        AS dataset_requires_materialized_table,
    direct.competition_id,
    direct.season_id,
    direct.match_id,
    direct.direct_match_only,
    required.dataset,
    direct.target_id,
    manifest.availability,
    manifest.row_count
FROM eligible_matches AS direct
CROSS JOIN required
LEFT JOIN latest_observation AS observation
  ON observation.target_id = direct.target_id
LEFT JOIN fbref_control.dataset_manifest AS completion
  ON completion.target_id = observation.target_id
 AND completion.content_hash = observation.content_hash
 AND completion.parser_version = observation.typed_parser_version
 AND completion.dataset = 'typed:__complete__'
 AND completion.parse_status = 'succeeded'
 AND completion.persistence_status = 'succeeded'
 AND completion.validation_status = 'succeeded'
LEFT JOIN fbref_control.dataset_manifest AS manifest
  ON manifest.target_id = observation.target_id
 AND manifest.content_hash = observation.content_hash
 AND manifest.parser_version = observation.typed_parser_version
 AND manifest.dataset = 'typed:' || required.dataset
ORDER BY direct.competition_id, direct.season_id, direct.match_id,
         required.dataset;
