-- FBref male-scope completeness gate for source seasons 2025-2026/2026-2027.
--
-- This file contains two independent read-only statements because the
-- registry/control plane is PostgreSQL while Bronze is Iceberg/Trino. Run the
-- PostgreSQL statement first, then the Trino statement. Every returned verdict must be PASS.
-- Save both result sets beside the accepted run ID.
-- A legitimately empty competition-season is one whose deduplicated schedule
-- has zero played matches; it is not an allowlisted competition. A legitimate
-- dead match target must be in frontier state `dead` with both error class and
-- error message. Every typed empty/restricted/not-applicable decision must
-- carry a nonblank reason and zero rows. There are no silent exemptions.

-- ===========================================================================
-- ENGINE: PostgreSQL
-- Bind :control_run_id to the exact successful current/backfill/replay run.
-- This is direct evidence: it reads observation_processing and
-- dataset_manifest rows, not a caller-supplied summary.
-- ===========================================================================

WITH required_seasons(source_season_id) AS (
    VALUES ('2025-2026'::text), ('2026-2027'::text)
), active_male AS (
    SELECT competition.source,
           competition.competition_id AS source_competition_id,
           competition.name AS competition_name
    FROM fbref_control.competition_registry AS competition
    WHERE competition.source = 'fbref'
      AND competition.gender = 'male'
      AND competition.crawl_state = 'active'
      AND competition.lifecycle_state IN ('present', 'missing_once')
      AND competition.present
), expected_scope AS (
    SELECT competition.*, season.source_season_id
    FROM active_male AS competition
    CROSS JOIN required_seasons AS season
), installed_seasons AS (
    SELECT season.competition_id AS source_competition_id,
           season.season_id AS source_season_id,
           season.lifecycle_state,
           season.present
    FROM fbref_control.season_registry AS season
    WHERE season.source = 'fbref'
      AND season.season_id IN ('2025-2026', '2026-2027')
), run_matches AS (
    SELECT target.logical_refresh_id,
           target.target_id,
           target.status AS target_status,
           frontier.state AS frontier_state,
           frontier.last_error_class,
           frontier.last_error_message,
           frontier.source_ids ->> 'competition_id'
               AS source_competition_id,
           frontier.source_ids ->> 'season_id' AS source_season_id
    FROM fbref_control.run_target AS target
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = target.target_id
    WHERE target.run_id = CAST(:control_run_id AS uuid)
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'match'
      AND frontier.source_ids ->> 'season_id'
          IN ('2025-2026', '2026-2027')
), observations AS (
    SELECT match.logical_refresh_id,
           match.target_id,
           match.target_status,
           match.frontier_state,
           match.last_error_class,
           match.last_error_message,
           match.source_competition_id,
           match.source_season_id,
           observed.content_hash,
           observed.typed_parser_version,
           observed.status AS observation_status,
           observed.generic_status,
           observed.typed_status,
           observed.stateful_status,
           observed.validation_status
    FROM run_matches AS match
    LEFT JOIN LATERAL (
        SELECT candidate.*
        FROM fbref_control.observation_processing AS candidate
        WHERE candidate.logical_refresh_id = match.logical_refresh_id
        ORDER BY candidate.completed_at DESC NULLS LAST,
                 candidate.updated_at DESC
        LIMIT 1
    ) AS observed ON true
), required_datasets(dataset) AS (
    VALUES
        ('typed:shot_events'::text),
        ('typed:match_events'::text),
        ('typed:lineups'::text),
        ('typed:match_team_stats'::text),
        ('typed:match_managers'::text),
        ('typed:match_officials'::text),
        ('typed:match_keeper_stats'::text),
        ('typed:match_player_stats'::text)
), manifest_proof AS (
    SELECT observation.logical_refresh_id,
           count(*) FILTER (
               WHERE manifest.dataset IS NOT NULL
           ) AS availability_decisions,
           count(*) FILTER (
               WHERE manifest.dataset IS NULL
                  OR manifest.parse_status <> 'succeeded'
                  OR manifest.persistence_status NOT IN ('succeeded', 'skipped')
                  OR manifest.validation_status NOT IN ('succeeded', 'skipped')
                  OR manifest.availability NOT IN (
                      'available', 'empty', 'restricted', 'not_applicable'
                  )
                  OR (
                      manifest.availability = 'available'
                      AND manifest.row_count <= 0
                  )
                  OR (
                      manifest.availability
                          IN ('empty', 'restricted', 'not_applicable')
                      AND (
                          manifest.row_count <> 0
                          OR nullif(trim(manifest.error_message), '') IS NULL
                      )
                  )
           ) AS invalid_dataset_decisions
    FROM observations AS observation
    CROSS JOIN required_datasets AS required
    LEFT JOIN fbref_control.dataset_manifest AS manifest
      ON manifest.target_id = observation.target_id
     AND manifest.content_hash = observation.content_hash
     AND manifest.dataset = required.dataset
     AND manifest.parser_version = observation.typed_parser_version
    GROUP BY observation.logical_refresh_id
), target_proof AS (
    SELECT observation.*,
           coalesce(manifest.availability_decisions, 0)
               AS availability_decisions,
           coalesce(manifest.invalid_dataset_decisions, 8)
               AS invalid_dataset_decisions,
           CASE
               WHEN observation.target_status = 'succeeded'
                AND observation.observation_status = 'succeeded'
                AND observation.generic_status = 'succeeded'
                AND observation.typed_status IN ('succeeded', 'skipped')
                AND observation.stateful_status IN ('succeeded', 'skipped')
                AND observation.validation_status = 'succeeded'
                AND manifest.availability_decisions = 8
                AND manifest.invalid_dataset_decisions = 0
               THEN 'durable'
               WHEN observation.frontier_state = 'dead'
                AND nullif(trim(observation.last_error_class), '') IS NOT NULL
                AND nullif(trim(observation.last_error_message), '') IS NOT NULL
               THEN 'dead'
               ELSE 'unproved'
           END AS control_proof
    FROM observations AS observation
    LEFT JOIN manifest_proof AS manifest
      ON manifest.logical_refresh_id = observation.logical_refresh_id
), per_scope AS (
    SELECT expected.source_competition_id,
           expected.competition_name,
           expected.source_season_id,
           count(DISTINCT installed.source_season_id)
               AS installed_season_rows,
           count(target.logical_refresh_id) AS run_match_targets,
           count(target.logical_refresh_id) FILTER (
               WHERE target.control_proof IN ('durable', 'dead')
           ) AS proved_match_targets,
           count(target.logical_refresh_id) FILTER (
               WHERE target.control_proof = 'dead'
           ) AS dead_match_targets,
           count(target.logical_refresh_id) FILTER (
               WHERE target.control_proof = 'unproved'
           ) AS unproved_match_targets
    FROM expected_scope AS expected
    LEFT JOIN installed_seasons AS installed
      ON installed.source_competition_id = expected.source_competition_id
     AND installed.source_season_id = expected.source_season_id
     AND installed.lifecycle_state = 'present'
     AND installed.present
    LEFT JOIN target_proof AS target
      ON target.source_competition_id = expected.source_competition_id
     AND target.source_season_id = expected.source_season_id
    GROUP BY expected.source_competition_id, expected.competition_name,
             expected.source_season_id
)
SELECT CASE
           WHEN min(installed_season_rows) = 1
            AND sum(unproved_match_targets) = 0
            AND sum(proved_match_targets) = sum(run_match_targets)
           THEN 'PASS' ELSE 'FAIL'
       END AS verdict,
       CASE WHEN grouping(source_competition_id) = 1
            THEN 'TOTAL' ELSE source_competition_id END
           AS source_competition_id,
       CASE WHEN grouping(competition_name) = 1
            THEN 'TOTAL' ELSE competition_name END AS competition_name,
       CASE WHEN grouping(source_season_id) = 1
            THEN 'TOTAL' ELSE source_season_id END AS source_season_id,
       sum(installed_season_rows) AS installed_season_rows,
       sum(run_match_targets) AS run_match_targets,
       sum(proved_match_targets) AS proved_match_targets,
       sum(dead_match_targets) AS dead_match_targets,
       sum(unproved_match_targets) AS unproved_match_targets
FROM per_scope
GROUP BY GROUPING SETS (
    (source_competition_id, competition_name, source_season_id),
    ()
)
ORDER BY competition_name, source_season_id;

-- ===========================================================================
-- ENGINE: Trino
-- Bind :control_run_id to the same run. All eight physical typed match tables
-- are referenced intentionally: a missing table is a loud acceptance failure.
-- ===========================================================================

WITH required_seasons(source_season_id) AS (
    VALUES ('2025-2026'), ('2026-2027')
), match_datasets(dataset, table_name) AS (
    VALUES
        ('shot_events', 'fbref_shot_events'),
        ('match_events', 'fbref_match_events'),
        ('lineups', 'fbref_lineups'),
        ('match_team_stats', 'fbref_match_team_stats'),
        ('match_managers', 'fbref_match_managers'),
        ('match_officials', 'fbref_match_officials'),
        ('match_keeper_stats', 'fbref_match_keeper_stats'),
        ('match_player_stats', 'fbref_match_player_stats')
), active_male AS (
    SELECT DISTINCT scope.source_competition_id, scope.competition_name
    FROM iceberg.bronze.fbref_target_scope AS scope
    WHERE scope.source = 'fbref'
      AND scope.control_run_id = CAST(:control_run_id AS varchar)
      AND scope.gender = 'male'
      AND scope.competition_crawl_state = 'active'
      AND scope.competition_lifecycle_state IN ('present', 'missing_once')
      AND scope.competition_present
), expected_scope AS (
    SELECT competition.*, season.source_season_id
    FROM active_male AS competition
    CROSS JOIN required_seasons AS season
), published_scope AS (
    SELECT DISTINCT source_competition_id, source_season_id
    FROM iceberg.bronze.fbref_target_scope
    WHERE source = 'fbref'
      AND control_run_id = CAST(:control_run_id AS varchar)
      AND eligible_male
      AND source_season_id IN ('2025-2026', '2026-2027')
), schedule_base AS (
    SELECT source_competition_id,
           source_season_id,
           regexp_extract(match_url, '/matches/([^/]+)', 1) AS match_id,
           score,
           _ingested_at
    FROM iceberg.bronze.fbref_schedule
    WHERE source_season_id IN ('2025-2026', '2026-2027')
      AND regexp_extract(match_url, '/matches/([^/]+)', 1) IS NOT NULL
), schedule_ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY source_competition_id, source_season_id, match_id
               ORDER BY _ingested_at DESC
           ) AS schedule_rank
    FROM schedule_base
), played AS (
    SELECT source_competition_id, source_season_id, match_id
    FROM schedule_ranked
    WHERE schedule_rank = 1
      AND nullif(trim(score), '') IS NOT NULL
), current_run_matches AS (
    SELECT regexp_extract(
               manifest.target_id, '^fbref:match:([^:]+)$', 1
           ) AS match_id,
           count(*) AS current_run_match_rows,
           count_if(
               manifest.parse_status <> 'succeeded'
               OR manifest.persist_status <> 'succeeded'
               OR manifest.validation_status <> 'succeeded'
           ) AS invalid_current_run_match_rows
    FROM iceberg.bronze.fbref_page_manifest AS manifest
    WHERE manifest.run_id = CAST(:control_run_id AS varchar)
      AND manifest.page_kind = 'match'
      AND regexp_extract(
              manifest.target_id, '^fbref:match:([^:]+)$', 1
          ) IS NOT NULL
    GROUP BY regexp_extract(
                 manifest.target_id, '^fbref:match:([^:]+)$', 1
             )
), availability_ranked AS (
    SELECT match_id, dataset, availability, reason, _ingested_at,
           row_number() OVER (
               PARTITION BY match_id, dataset
               ORDER BY _ingested_at DESC
           ) AS availability_rank
    FROM iceberg.bronze.fbref_dataset_availability
    WHERE dataset IN (SELECT dataset FROM match_datasets)
      AND _batch_id = CAST(:control_run_id AS varchar)
), availability AS (
    SELECT match_id, dataset, availability, reason
    FROM availability_ranked
    WHERE availability_rank = 1
), typed_rows AS (
    SELECT match_id, 'shot_events' AS dataset, count(*) AS typed_rows
      FROM iceberg.bronze.fbref_shot_events
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_events', count(*)
      FROM iceberg.bronze.fbref_match_events
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'lineups', count(*)
      FROM iceberg.bronze.fbref_lineups
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_team_stats', count(*)
      FROM iceberg.bronze.fbref_match_team_stats
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_managers', count(*)
      FROM iceberg.bronze.fbref_match_managers
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_officials', count(*)
      FROM iceberg.bronze.fbref_match_officials
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_keeper_stats', count(*)
      FROM iceberg.bronze.fbref_match_keeper_stats
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
    UNION ALL
    SELECT match_id, 'match_player_stats', count(*)
      FROM iceberg.bronze.fbref_match_player_stats
     WHERE _batch_id = CAST(:control_run_id AS varchar)
     GROUP BY match_id
), match_proof AS (
    SELECT played.source_competition_id, played.source_season_id,
           played.match_id,
           max(coalesce(current.current_run_match_rows, 0))
               AS current_run_match_rows,
           max(coalesce(current.invalid_current_run_match_rows, 1))
               AS invalid_current_run_match_rows,
           count_if(availability.dataset IS NOT NULL)
               AS availability_decisions,
           count_if(
               availability.dataset IS NULL
               OR availability.availability NOT IN (
                   'available', 'empty', 'restricted', 'not_applicable'
               )
               OR (
                   availability.availability = 'available'
                   AND coalesce(typed.typed_rows, 0) <= 0
               )
               OR (
                   availability.availability
                       IN ('empty', 'restricted', 'not_applicable')
                   AND (
                       coalesce(typed.typed_rows, 0) <> 0
                       OR nullif(trim(availability.reason), '') IS NULL
                   )
               )
           ) AS invalid_dataset_decisions,
           count_if(
               availability.availability = 'available'
               AND typed.typed_rows > 0
           ) AS directly_materialized_datasets,
           count_if(
               availability.availability
                   IN ('empty', 'restricted', 'not_applicable')
               AND coalesce(typed.typed_rows, 0) = 0
               AND nullif(trim(availability.reason), '') IS NOT NULL
           ) AS explicitly_empty_datasets
    FROM played
    CROSS JOIN match_datasets AS required
    LEFT JOIN current_run_matches AS current
      ON current.match_id = played.match_id
    LEFT JOIN availability
      ON availability.match_id = played.match_id
     AND availability.dataset = required.dataset
    LEFT JOIN typed_rows AS typed
      ON typed.match_id = played.match_id
     AND typed.dataset = required.dataset
    GROUP BY played.source_competition_id, played.source_season_id,
             played.match_id
), per_scope AS (
    SELECT expected.source_competition_id,
           expected.competition_name,
           expected.source_season_id,
           count(DISTINCT published.source_season_id) AS published_scope_rows,
           count(DISTINCT played.match_id) AS played_matches,
           count(DISTINCT proof.match_id) FILTER (
               WHERE proof.current_run_match_rows = 1
                 AND proof.invalid_current_run_match_rows = 0
                 AND proof.availability_decisions = 8
                 AND proof.invalid_dataset_decisions = 0
           ) AS fully_proved_matches,
           count(DISTINCT proof.match_id) FILTER (
               WHERE proof.current_run_match_rows <> 1
                  OR proof.invalid_current_run_match_rows <> 0
                  OR proof.availability_decisions <> 8
                  OR proof.invalid_dataset_decisions <> 0
           ) AS unproved_matches,
           (
               SELECT count(*) - count(DISTINCT match_id)
               FROM schedule_base AS raw_schedule
               WHERE raw_schedule.source_competition_id =
                         expected.source_competition_id
                 AND raw_schedule.source_season_id = expected.source_season_id
           ) AS duplicate_schedule_rows
    FROM expected_scope AS expected
    LEFT JOIN published_scope AS published
      ON published.source_competition_id = expected.source_competition_id
     AND published.source_season_id = expected.source_season_id
    LEFT JOIN played
      ON played.source_competition_id = expected.source_competition_id
     AND played.source_season_id = expected.source_season_id
    LEFT JOIN match_proof AS proof
      ON proof.source_competition_id = expected.source_competition_id
     AND proof.source_season_id = expected.source_season_id
     AND proof.match_id = played.match_id
    GROUP BY expected.source_competition_id, expected.competition_name,
             expected.source_season_id
)
SELECT CASE
           WHEN min(published_scope_rows) = 1
            AND sum(unproved_matches) = 0
            AND sum(fully_proved_matches) = sum(played_matches)
           THEN 'PASS' ELSE 'FAIL'
       END AS verdict,
       CASE WHEN grouping(source_competition_id) = 1
            THEN 'TOTAL' ELSE source_competition_id END
           AS source_competition_id,
       CASE WHEN grouping(competition_name) = 1
            THEN 'TOTAL' ELSE competition_name END AS competition_name,
       CASE WHEN grouping(source_season_id) = 1
            THEN 'TOTAL' ELSE source_season_id END AS source_season_id,
       sum(published_scope_rows) AS published_scope_rows,
       sum(played_matches) AS played_matches,
       sum(fully_proved_matches) AS fully_proved_matches,
       sum(unproved_matches) AS unproved_matches,
       sum(duplicate_schedule_rows) AS duplicate_schedule_rows
FROM per_scope
GROUP BY GROUPING SETS (
    (source_competition_id, competition_name, source_season_id),
    ()
)
ORDER BY competition_name, source_season_id;
