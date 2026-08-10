-- FBref male-scope completeness gate for source seasons 2025-2026/2026-2027.
--
-- This file contains two independent read-only statements because the
-- registry/control plane is PostgreSQL while Bronze is Iceberg/Trino. Run the
-- PostgreSQL statement first, then the Trino statement. Every returned verdict must be PASS.
-- Save both result sets beside the accepted persistence replay.
--
-- Bind :accepted_control_run_ids_json to a non-empty JSON array containing
-- every successful publishing current/backfill run needed to cover the whole
-- scope. This may be more than one run when a bounded run cannot hold every
-- target. Bind :publication_control_run_id to one member of that set whose
-- immutable fbref_target_scope generation is being accepted.
--
-- Copy these canonical bridge values from the PostgreSQL TOTAL row to Trino:
--
--   accepted_control_run_ids_json, accepted_run_count,
--   accepted_run_keys_md5, expected_scope_ids_json, expected_scope_count,
--   expected_scope_distinct_count, expected_scope_keys_md5,
--   dead_match_ids_json, dead_match_count, dead_match_keys_md5.
--
-- PostgreSQL is authoritative for the complete active male competition-season
-- set. Trino parses that set, recomputes its count/distinct count/digest, and
-- requires exact set equality with both the publication generation and every
-- selected run's separately exported scope generation. A missing whole
-- competition therefore cannot disappear from both sides and pass.
--
-- A legitimately empty competition-season is one whose deduplicated schedule
-- has zero played matches; it is not an allowlisted competition. A legitimate
-- dead match target must be the latest target evidence, be in frontier state
-- `dead`, and retain both error class and error message. Every typed
-- empty/restricted/not-applicable decision must carry a nonblank reason and
-- zero rows. There are no silent exemptions.

-- ===========================================================================
-- ENGINE: PostgreSQL
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
), expected_scope_bridge AS (
    SELECT coalesce(
               jsonb_agg(
                   jsonb_build_object(
                       'source_competition_id', source_competition_id,
                       'source_season_id', source_season_id
                   )
                   ORDER BY source_competition_id, source_season_id
               ),
               '[]'::jsonb
           )::text AS expected_scope_ids_json,
           count(*) AS expected_scope_count,
           count(DISTINCT (
               source_competition_id, source_season_id
           )) AS expected_scope_distinct_count,
           count(*) FILTER (
               WHERE nullif(trim(source_competition_id), '') IS NULL
                  OR nullif(trim(source_season_id), '') IS NULL
                  OR strpos(source_competition_id, chr(31)) > 0
                  OR strpos(source_season_id, chr(31)) > 0
           ) AS invalid_expected_scope_ids,
           md5(coalesce(string_agg(
               source_competition_id || chr(31) || source_season_id,
               chr(30) ORDER BY source_competition_id, source_season_id
           ), '')) AS expected_scope_keys_md5
    FROM expected_scope
), accepted_run_input AS (
    SELECT CAST(value AS uuid) AS control_run_id
    FROM jsonb_array_elements_text(
        CAST(:accepted_control_run_ids_json AS jsonb)
    ) AS selected(value)
), accepted_run_set AS (
    SELECT DISTINCT control_run_id FROM accepted_run_input
), accepted_run_bridge AS (
    SELECT coalesce(
               jsonb_agg(
                   control_run_id::text ORDER BY control_run_id::text
               ),
               '[]'::jsonb
           )::text AS accepted_control_run_ids_json,
           count(*) AS accepted_run_count,
           count(DISTINCT control_run_id) AS accepted_run_distinct_count,
           md5(coalesce(string_agg(
               control_run_id::text, chr(31) ORDER BY control_run_id::text
           ), '')) AS accepted_run_keys_md5
    FROM accepted_run_input
), accepted_run_evidence AS (
    SELECT count(*) FILTER (WHERE run.run_id IS NOT NULL)
               AS installed_accepted_runs,
           count(*) FILTER (
               WHERE run.run_id IS NULL
                  OR run.status <> 'succeeded'
                  OR run.run_type NOT IN ('current', 'backfill')
                  OR coalesce(
                      (run.metadata ->> 'publication_eligible')::boolean,
                      false
                  ) IS NOT true
                  OR coalesce(run.metadata ->> 'execution_mode', '')
                      NOT IN ('publishing', 'backfill')
           ) AS invalid_accepted_runs,
           count(*) FILTER (
               WHERE input.control_run_id =
                     CAST(:publication_control_run_id AS uuid)
           ) AS publication_run_memberships
    FROM accepted_run_input AS input
    LEFT JOIN fbref_control.crawl_run AS run
      ON run.run_id = input.control_run_id
), installed_seasons AS (
    SELECT season.competition_id AS source_competition_id,
           season.season_id AS source_season_id,
           season.lifecycle_state,
           season.present
    FROM fbref_control.season_registry AS season
    WHERE season.source = 'fbref'
      AND season.season_id IN ('2025-2026', '2026-2027')
), latest_global_target AS (
    SELECT DISTINCT ON (target.target_id)
           target.run_id,
           target.logical_refresh_id,
           target.target_id,
           target.status AS target_status,
           target.created_at AS target_created_at,
           target.updated_at AS target_updated_at
    FROM fbref_control.run_target AS target
    JOIN fbref_control.crawl_run AS run
      ON run.run_id = target.run_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = target.target_id
    WHERE frontier.source = 'fbref'
      AND frontier.page_kind = 'match'
      AND frontier.source_ids ->> 'season_id'
          IN ('2025-2026', '2026-2027')
    ORDER BY target.target_id,
             target.updated_at DESC,
             run.finished_at DESC NULLS LAST,
             target.created_at DESC,
             target.logical_refresh_id DESC
), latest_global_attempt AS (
    SELECT DISTINCT ON (attempt.target_id)
           attempt.run_id,
           attempt.target_id,
           attempt.logical_refresh_id,
           attempt.attempt_id,
           attempt.status AS attempt_status,
           attempt.content_hash,
           attempt.raw_manifest_key,
           attempt.finished_at,
           attempt.heartbeat_at,
           attempt.started_at
    FROM fbref_control.fetch_attempt AS attempt
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = attempt.target_id
    WHERE frontier.source = 'fbref'
      AND frontier.page_kind = 'match'
      AND frontier.source_ids ->> 'season_id'
          IN ('2025-2026', '2026-2027')
    ORDER BY attempt.target_id,
             coalesce(
                 attempt.finished_at, attempt.heartbeat_at, attempt.started_at
             ) DESC,
             attempt.attempt_number DESC,
             attempt.attempt_id DESC
), run_matches AS (
    SELECT target.run_id,
           target.logical_refresh_id,
           target.target_id,
           target.target_status,
           attempt.run_id AS attempt_run_id,
           attempt.logical_refresh_id AS attempt_logical_refresh_id,
           attempt.attempt_status,
           attempt.content_hash AS attempt_content_hash,
           attempt.raw_manifest_key,
           frontier.state AS frontier_state,
           frontier.last_content_hash,
           frontier.last_error_class,
           frontier.last_error_message,
           frontier.source_ids ->> 'match_id' AS match_id,
           frontier.source_ids ->> 'competition_id'
               AS source_competition_id,
           frontier.source_ids ->> 'season_id' AS source_season_id
    FROM latest_global_target AS target
    JOIN accepted_run_set AS accepted
      ON target.run_id = accepted.control_run_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = target.target_id
    LEFT JOIN latest_global_attempt AS attempt
      ON attempt.target_id = target.target_id
), observations AS (
    SELECT match.*,
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
          AND candidate.target_id = match.target_id
          AND candidate.content_hash = match.attempt_content_hash
        ORDER BY candidate.completed_at DESC NULLS LAST,
                 candidate.updated_at DESC,
                 candidate.parser_version DESC,
                 candidate.typed_parser_version DESC,
                 candidate.stateful_parser_version DESC
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
                AND observation.attempt_run_id = observation.run_id
                AND observation.attempt_logical_refresh_id =
                    observation.logical_refresh_id
                AND observation.attempt_status = 'succeeded'
                AND observation.raw_manifest_key IS NOT NULL
                AND observation.attempt_content_hash IS NOT NULL
                AND observation.frontier_state = 'fetched'
                AND observation.last_content_hash =
                    observation.attempt_content_hash
                AND observation.content_hash = observation.attempt_content_hash
                AND observation.observation_status = 'succeeded'
                AND observation.generic_status = 'succeeded'
                AND observation.typed_status IN ('succeeded', 'skipped')
                AND observation.stateful_status IN ('succeeded', 'skipped')
                AND observation.validation_status = 'succeeded'
                AND manifest.availability_decisions = 8
                AND manifest.invalid_dataset_decisions = 0
               THEN 'durable'
               WHEN observation.target_status = 'failed'
                AND observation.attempt_run_id = observation.run_id
                AND observation.attempt_logical_refresh_id =
                    observation.logical_refresh_id
                AND observation.frontier_state = 'dead'
                AND nullif(trim(observation.match_id), '') IS NOT NULL
                AND nullif(trim(observation.last_error_class), '') IS NOT NULL
                AND nullif(trim(observation.last_error_message), '') IS NOT NULL
               THEN 'dead'
               ELSE 'unproved'
           END AS control_proof
    FROM observations AS observation
    LEFT JOIN manifest_proof AS manifest
      ON manifest.logical_refresh_id = observation.logical_refresh_id
), dead_bridge AS (
    SELECT coalesce(
               jsonb_agg(match_id ORDER BY match_id), '[]'::jsonb
           )::text AS dead_match_ids_json,
           count(*) AS dead_match_count,
           count(DISTINCT match_id) AS dead_match_distinct_count,
           md5(coalesce(string_agg(match_id, chr(31) ORDER BY match_id), ''))
               AS dead_match_keys_md5
    FROM target_proof
    WHERE control_proof = 'dead'
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
            AND expected.expected_scope_count > 0
            AND expected.expected_scope_count =
                expected.expected_scope_distinct_count
            AND expected.invalid_expected_scope_ids = 0
            AND accepted.accepted_run_count > 0
            AND accepted.accepted_run_count =
                accepted.accepted_run_distinct_count
            AND run_evidence.installed_accepted_runs =
                accepted.accepted_run_count
            AND run_evidence.invalid_accepted_runs = 0
            AND run_evidence.publication_run_memberships = 1
            AND dead.dead_match_count = dead.dead_match_distinct_count
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
       sum(unproved_match_targets) AS unproved_match_targets,
       accepted.accepted_control_run_ids_json,
       accepted.accepted_run_count,
       accepted.accepted_run_distinct_count,
       accepted.accepted_run_keys_md5,
       expected.expected_scope_ids_json,
       expected.expected_scope_count,
       expected.expected_scope_distinct_count,
       expected.expected_scope_keys_md5,
       dead.dead_match_ids_json,
       dead.dead_match_count,
       dead.dead_match_distinct_count,
       dead.dead_match_keys_md5
FROM per_scope
CROSS JOIN accepted_run_bridge AS accepted
CROSS JOIN accepted_run_evidence AS run_evidence
CROSS JOIN expected_scope_bridge AS expected
CROSS JOIN dead_bridge AS dead
GROUP BY GROUPING SETS (
    (source_competition_id, competition_name, source_season_id),
    ()
), accepted.accepted_control_run_ids_json, accepted.accepted_run_count,
   accepted.accepted_run_distinct_count, accepted.accepted_run_keys_md5,
   run_evidence.installed_accepted_runs, run_evidence.invalid_accepted_runs,
   run_evidence.publication_run_memberships, expected.expected_scope_ids_json,
   expected.expected_scope_count, expected.expected_scope_distinct_count,
   expected.invalid_expected_scope_ids, expected.expected_scope_keys_md5,
   dead.dead_match_ids_json, dead.dead_match_count,
   dead.dead_match_distinct_count, dead.dead_match_keys_md5
ORDER BY competition_name, source_season_id;

-- ===========================================================================
-- ENGINE: Trino
-- Bind :publication_control_run_id to the PostgreSQL-validated publication
-- member. Bind every bridge value named in the header from the PostgreSQL
-- TOTAL row. All eight physical typed match tables are referenced
-- intentionally: a missing table is a loud acceptance failure.
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
), accepted_run_bridge AS (
    SELECT trim(value) AS run_id
    FROM UNNEST(
        CAST(
            json_parse(CAST(:accepted_control_run_ids_json AS varchar))
            AS array(varchar)
        )
    ) AS bridge(value)
), accepted_run_set_evidence AS (
    SELECT count(*) AS accepted_run_count,
           count(DISTINCT run_id) AS accepted_run_distinct_count,
           count_if(nullif(trim(run_id), '') IS NULL) AS blank_accepted_run_ids
    FROM accepted_run_bridge
), expected_scope_bridge AS (
    SELECT trim(entry.source_competition_id) AS source_competition_id,
           trim(entry.source_season_id) AS source_season_id
    FROM UNNEST(
        CAST(
            json_parse(CAST(:expected_scope_ids_json AS varchar))
            AS array(row(
                source_competition_id varchar,
                source_season_id varchar
            ))
        )
    ) AS bridge(entry)
), expected_scope_bridge_evidence AS (
    SELECT count(*) AS expected_scope_count,
           count(DISTINCT concat(
               source_competition_id, chr(31), source_season_id
           )) AS expected_scope_distinct_count,
           count_if(
               nullif(trim(source_competition_id), '') IS NULL
               OR nullif(trim(source_season_id), '') IS NULL
               OR strpos(source_competition_id, chr(31)) > 0
               OR strpos(source_season_id, chr(31)) > 0
           ) AS invalid_expected_scope_ids,
           lower(to_hex(md5(to_utf8(coalesce(
               array_join(array_agg(
                   concat(
                       source_competition_id, chr(31), source_season_id
                   ) ORDER BY source_competition_id, source_season_id
               ), chr(30)),
               ''
           ))))) AS expected_scope_keys_md5
    FROM expected_scope_bridge
), accepted_run_bridge_evidence AS (
    SELECT lower(to_hex(md5(to_utf8(coalesce(
               array_join(array_agg(run_id ORDER BY run_id), chr(31)), ''
           ))))) AS accepted_run_keys_md5,
           count_if(
               run_id = CAST(:publication_control_run_id AS varchar)
           ) AS publication_run_memberships
    FROM accepted_run_bridge
), dead_match_bridge AS (
    SELECT trim(value) AS match_id
    FROM UNNEST(
        CAST(
            json_parse(CAST(:dead_match_ids_json AS varchar))
            AS array(varchar)
        )
    ) AS bridge(value)
), published_scope_base AS (
    SELECT scope.source_competition_id,
           scope.source_season_id,
           scope.competition_name,
           scope.scope_hash,
           scope.exported_at
    FROM iceberg.bronze.fbref_target_scope AS scope
    WHERE scope.source = 'fbref'
      AND scope.control_run_id =
          CAST(:publication_control_run_id AS varchar)
      AND scope.eligible_male
      AND scope.source_season_id IN ('2025-2026', '2026-2027')
), published_scope AS (
    SELECT DISTINCT source_competition_id, source_season_id
    FROM published_scope_base
), publication_generation_evidence AS (
    SELECT count(*) AS published_scope_rows,
           count(DISTINCT concat(
               source_competition_id, chr(31), source_season_id
           )) AS published_scope_distinct_rows,
           count(DISTINCT scope_hash) AS publication_scope_hashes,
           count_if(nullif(trim(scope_hash), '') IS NULL)
               AS blank_publication_scope_hashes,
           count(DISTINCT exported_at) AS publication_export_timestamps,
           count_if(exported_at IS NULL) AS missing_publication_export_timestamps
    FROM published_scope_base
), publication_scope_evidence AS (
    SELECT (
               SELECT count(*)
               FROM expected_scope_bridge AS expected
               LEFT JOIN published_scope AS published
                 ON published.source_competition_id =
                    expected.source_competition_id
                AND published.source_season_id = expected.source_season_id
               WHERE published.source_competition_id IS NULL
           ) AS missing_expected_scope_rows,
           (
               SELECT count(*)
               FROM published_scope AS published
               LEFT JOIN expected_scope_bridge AS expected
                 ON expected.source_competition_id =
                    published.source_competition_id
                AND expected.source_season_id = published.source_season_id
               WHERE expected.source_competition_id IS NULL
           ) AS unexpected_published_scope_rows
), selected_run_scope_base AS (
    SELECT scope.control_run_id AS run_id,
           scope.source_competition_id,
           scope.source_season_id,
           scope.scope_hash,
           scope.exported_at
    FROM iceberg.bronze.fbref_target_scope AS scope
    WHERE scope.source = 'fbref'
      AND scope.control_run_id IN (
          SELECT run_id FROM accepted_run_bridge
      )
      AND scope.eligible_male
      AND scope.source_season_id IN ('2025-2026', '2026-2027')
), selected_run_scope AS (
    SELECT DISTINCT run_id, source_competition_id, source_season_id
    FROM selected_run_scope_base
), selected_run_scope_integrity AS (
    SELECT count_if(
               generation.run_id IS NULL
               OR generation.scope_rows <>
                  (SELECT count(*) FROM expected_scope_bridge)
               OR generation.scope_distinct_rows <> generation.scope_rows
               OR generation.scope_hashes <> 1
               OR generation.blank_scope_hashes <> 0
               OR generation.export_timestamps <> 1
               OR generation.missing_export_timestamps <> 0
           ) AS invalid_selected_run_generations
    FROM accepted_run_bridge AS accepted
    LEFT JOIN (
        SELECT run_id,
               count(*) AS scope_rows,
               count(DISTINCT concat(
                   source_competition_id, chr(31), source_season_id
               )) AS scope_distinct_rows,
               count(DISTINCT scope_hash) AS scope_hashes,
               count_if(nullif(trim(scope_hash), '') IS NULL)
                   AS blank_scope_hashes,
               count(DISTINCT exported_at) AS export_timestamps,
               count_if(exported_at IS NULL) AS missing_export_timestamps
        FROM selected_run_scope_base
        GROUP BY run_id
    ) AS generation ON generation.run_id = accepted.run_id
), selected_run_scope_evidence AS (
    SELECT (
               SELECT count(*)
               FROM accepted_run_bridge AS accepted
               CROSS JOIN expected_scope_bridge AS expected
               LEFT JOIN selected_run_scope AS selected
                 ON selected.run_id = accepted.run_id
                AND selected.source_competition_id =
                    expected.source_competition_id
                AND selected.source_season_id = expected.source_season_id
               WHERE selected.run_id IS NULL
           ) AS missing_selected_run_scope_rows,
           (
               SELECT count(*)
               FROM selected_run_scope AS selected
               LEFT JOIN accepted_run_bridge AS accepted
                 ON accepted.run_id = selected.run_id
               LEFT JOIN expected_scope_bridge AS expected
                 ON expected.source_competition_id =
                    selected.source_competition_id
                AND expected.source_season_id = selected.source_season_id
               WHERE accepted.run_id IS NULL
                  OR expected.source_competition_id IS NULL
           ) AS unexpected_selected_run_scope_rows
), schedule_base AS (
    SELECT schedule.source_competition_id,
           schedule.source_season_id,
           regexp_extract(schedule.match_url, '/matches/([^/]+)', 1)
               AS match_id,
           schedule.score,
           schedule._batch_id AS schedule_run_id,
           schedule._ingested_at
    FROM iceberg.bronze.fbref_schedule AS schedule
    JOIN expected_scope_bridge AS expected
      ON expected.source_competition_id = schedule.source_competition_id
     AND expected.source_season_id = schedule.source_season_id
    WHERE regexp_extract(schedule.match_url, '/matches/([^/]+)', 1)
          IS NOT NULL
), schedule_ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY source_competition_id, source_season_id, match_id
               ORDER BY _ingested_at DESC, schedule_run_id DESC
           ) AS schedule_rank
    FROM schedule_base
), played AS (
    SELECT source_competition_id, source_season_id, match_id, schedule_run_id
    FROM schedule_ranked
    WHERE schedule_rank = 1
      AND nullif(trim(score), '') IS NOT NULL
), played_match_keys AS (
    SELECT DISTINCT match_id FROM played
), dead_bridge_evidence AS (
    SELECT count(*) AS dead_match_count,
           count(DISTINCT dead.match_id) AS dead_match_distinct_count,
           count_if(nullif(trim(dead.match_id), '') IS NULL)
               AS blank_dead_match_ids,
           count_if(played.match_id IS NULL) AS orphan_dead_match_ids,
           lower(to_hex(md5(to_utf8(coalesce(
               array_join(
                   array_agg(dead.match_id ORDER BY dead.match_id), chr(31)
               ),
               ''
           ))))) AS dead_match_keys_md5
    FROM dead_match_bridge AS dead
    LEFT JOIN played_match_keys AS played ON played.match_id = dead.match_id
), page_manifest_ranked AS (
    SELECT manifest.*,
           row_number() OVER (
               PARTITION BY manifest.target_id
               ORDER BY manifest.persisted_at DESC,
                        manifest.content_hash DESC,
                        manifest.parser_version DESC,
                        manifest.run_id DESC
           ) AS manifest_rank
    FROM iceberg.bronze.fbref_page_manifest AS manifest
    WHERE manifest.page_kind = 'match'
      AND regexp_extract(
              manifest.target_id, '^fbref:match:([^:]+)$', 1
          ) IS NOT NULL
), current_run_matches AS (
    SELECT regexp_extract(
               manifest.target_id, '^fbref:match:([^:]+)$', 1
           ) AS match_id,
           count(*) AS current_run_match_rows,
           count_if(
               manifest.parse_status <> 'success'
               OR manifest.persist_status <> 'success'
               OR manifest.validation_status <> 'success'
           ) AS invalid_current_run_match_rows,
           count(accepted.run_id) AS accepted_manifest_rows,
           max(manifest.run_id) AS run_id
    FROM page_manifest_ranked AS manifest
    LEFT JOIN accepted_run_bridge AS accepted
      ON accepted.run_id = manifest.run_id
    WHERE manifest.manifest_rank = 1
    GROUP BY regexp_extract(
                 manifest.target_id, '^fbref:match:([^:]+)$', 1
             )
), availability_ranked AS (
    SELECT availability.match_id,
           availability.dataset,
           availability.availability,
           availability.reason,
           availability._ingested_at,
           row_number() OVER (
               PARTITION BY availability.match_id, availability.dataset
               ORDER BY availability._ingested_at DESC
           ) AS availability_rank
    FROM iceberg.bronze.fbref_dataset_availability AS availability
    JOIN current_run_matches AS manifest
      ON manifest.match_id = availability.match_id
     AND availability._batch_id = manifest.run_id
    WHERE availability.dataset IN (SELECT dataset FROM match_datasets)
), availability AS (
    SELECT match_id, dataset, availability, reason
    FROM availability_ranked
    WHERE availability_rank = 1
), typed_rows AS (
    SELECT typed.match_id, 'shot_events' AS dataset, count(*) AS typed_rows
      FROM iceberg.bronze.fbref_shot_events AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_events', count(*)
      FROM iceberg.bronze.fbref_match_events AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'lineups', count(*)
      FROM iceberg.bronze.fbref_lineups AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_team_stats', count(*)
      FROM iceberg.bronze.fbref_match_team_stats AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_managers', count(*)
      FROM iceberg.bronze.fbref_match_managers AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_officials', count(*)
      FROM iceberg.bronze.fbref_match_officials AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_keeper_stats', count(*)
      FROM iceberg.bronze.fbref_match_keeper_stats AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
    UNION ALL
    SELECT typed.match_id, 'match_player_stats', count(*)
      FROM iceberg.bronze.fbref_match_player_stats AS typed
      JOIN current_run_matches AS manifest
        ON manifest.match_id = typed.match_id
       AND typed._batch_id = manifest.run_id
     GROUP BY typed.match_id
), match_proof AS (
    SELECT played.source_competition_id, played.source_season_id,
           played.match_id,
           max(coalesce(current.current_run_match_rows, 0))
               AS current_run_match_rows,
           max(coalesce(current.invalid_current_run_match_rows, 1))
               AS invalid_current_run_match_rows,
           max(coalesce(current.accepted_manifest_rows, 0))
               AS accepted_manifest_rows,
           max(CASE WHEN schedule_accepted.run_id IS NULL THEN 0 ELSE 1 END)
               AS accepted_schedule_rows,
           max(CASE WHEN dead.match_id IS NULL THEN 0 ELSE 1 END)
               AS dead_bridge_rows,
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
    LEFT JOIN accepted_run_bridge AS schedule_accepted
      ON schedule_accepted.run_id = played.schedule_run_id
    LEFT JOIN current_run_matches AS current
      ON current.match_id = played.match_id
    LEFT JOIN dead_match_bridge AS dead
      ON dead.match_id = played.match_id
    LEFT JOIN availability
      ON availability.match_id = played.match_id
     AND availability.dataset = required.dataset
    LEFT JOIN typed_rows AS typed
      ON typed.match_id = played.match_id
     AND typed.dataset = required.dataset
    GROUP BY played.source_competition_id, played.source_season_id,
             played.match_id
), classified_match_proof AS (
    SELECT proof.*,
           CASE
               WHEN dead_bridge_rows = 0
                AND accepted_schedule_rows = 1
                AND current_run_match_rows = 1
                AND invalid_current_run_match_rows = 0
                AND accepted_manifest_rows = 1
                AND availability_decisions = 8
                AND invalid_dataset_decisions = 0
               THEN 'durable'
               WHEN dead_bridge_rows = 1
                AND accepted_schedule_rows = 1
                AND current_run_match_rows = 0
                AND accepted_manifest_rows = 0
                AND availability_decisions = 0
                AND directly_materialized_datasets = 0
                AND explicitly_empty_datasets = 0
               THEN 'dead'
               ELSE 'unproved'
           END AS trino_proof
    FROM match_proof AS proof
), expected_scope AS (
    SELECT expected.source_competition_id,
           coalesce(
               max(published.competition_name),
               expected.source_competition_id
           ) AS competition_name,
           expected.source_season_id
    FROM expected_scope_bridge AS expected
    LEFT JOIN published_scope_base AS published
      ON published.source_competition_id = expected.source_competition_id
     AND published.source_season_id = expected.source_season_id
    GROUP BY expected.source_competition_id, expected.source_season_id
), per_scope AS (
    SELECT expected.source_competition_id,
           expected.competition_name,
           expected.source_season_id,
           count(DISTINCT published.source_season_id) AS published_scope_rows,
           count(DISTINCT played.match_id) AS played_matches,
           count(DISTINCT proof.match_id) FILTER (
               WHERE proof.trino_proof IN ('durable', 'dead')
           ) AS fully_proved_matches,
           count(DISTINCT proof.match_id) FILTER (
               WHERE proof.trino_proof = 'dead'
           ) AS dead_proved_matches,
           count(DISTINCT proof.match_id) FILTER (
               WHERE proof.trino_proof = 'unproved'
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
    LEFT JOIN classified_match_proof AS proof
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
            AND accepted_set.accepted_run_count =
                CAST(:accepted_run_count AS bigint)
            AND accepted_set.accepted_run_count =
                accepted_set.accepted_run_distinct_count
            AND accepted_set.accepted_run_count > 0
            AND accepted_set.blank_accepted_run_ids = 0
            AND accepted_bridge.accepted_run_keys_md5 =
                lower(CAST(:accepted_run_keys_md5 AS varchar))
            AND accepted_bridge.publication_run_memberships = 1
            AND expected_bridge.expected_scope_count =
                CAST(:expected_scope_count AS bigint)
            AND expected_bridge.expected_scope_distinct_count =
                CAST(:expected_scope_distinct_count AS bigint)
            AND expected_bridge.expected_scope_count =
                expected_bridge.expected_scope_distinct_count
            AND expected_bridge.expected_scope_count > 0
            AND expected_bridge.invalid_expected_scope_ids = 0
            AND expected_bridge.expected_scope_keys_md5 =
                lower(CAST(:expected_scope_keys_md5 AS varchar))
            AND publication_generation.published_scope_rows =
                expected_bridge.expected_scope_count
            AND publication_generation.published_scope_distinct_rows =
                publication_generation.published_scope_rows
            AND publication_generation.publication_scope_hashes = 1
            AND publication_generation.blank_publication_scope_hashes = 0
            AND publication_generation.publication_export_timestamps = 1
            AND publication_generation.missing_publication_export_timestamps = 0
            AND publication_set.missing_expected_scope_rows = 0
            AND publication_set.unexpected_published_scope_rows = 0
            AND selected_integrity.invalid_selected_run_generations = 0
            AND selected_set.missing_selected_run_scope_rows = 0
            AND selected_set.unexpected_selected_run_scope_rows = 0
            AND dead.dead_match_count = CAST(:dead_match_count AS bigint)
            AND dead.dead_match_distinct_count = dead.dead_match_count
            AND dead.blank_dead_match_ids = 0
            AND dead.orphan_dead_match_ids = 0
            AND dead.dead_match_keys_md5 =
                lower(CAST(:dead_match_keys_md5 AS varchar))
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
       sum(dead_proved_matches) AS dead_proved_matches,
       sum(unproved_matches) AS unproved_matches,
       sum(duplicate_schedule_rows) AS duplicate_schedule_rows,
       accepted_set.accepted_run_count,
       accepted_set.accepted_run_distinct_count,
       expected_bridge.expected_scope_count,
       expected_bridge.expected_scope_distinct_count,
       publication_set.missing_expected_scope_rows,
       publication_set.unexpected_published_scope_rows,
       selected_set.missing_selected_run_scope_rows,
       selected_set.unexpected_selected_run_scope_rows,
       dead.dead_match_count,
       dead.dead_match_distinct_count,
       dead.blank_dead_match_ids,
       dead.orphan_dead_match_ids,
       dead.dead_match_keys_md5
FROM per_scope
CROSS JOIN accepted_run_set_evidence AS accepted_set
CROSS JOIN accepted_run_bridge_evidence AS accepted_bridge
CROSS JOIN expected_scope_bridge_evidence AS expected_bridge
CROSS JOIN publication_generation_evidence AS publication_generation
CROSS JOIN publication_scope_evidence AS publication_set
CROSS JOIN selected_run_scope_integrity AS selected_integrity
CROSS JOIN selected_run_scope_evidence AS selected_set
CROSS JOIN dead_bridge_evidence AS dead
GROUP BY GROUPING SETS (
    (source_competition_id, competition_name, source_season_id),
    ()
), accepted_set.accepted_run_count,
   accepted_set.accepted_run_distinct_count,
   accepted_set.blank_accepted_run_ids,
   accepted_bridge.accepted_run_keys_md5,
   accepted_bridge.publication_run_memberships,
   expected_bridge.expected_scope_count,
   expected_bridge.expected_scope_distinct_count,
   expected_bridge.invalid_expected_scope_ids,
   expected_bridge.expected_scope_keys_md5,
   publication_generation.published_scope_rows,
   publication_generation.published_scope_distinct_rows,
   publication_generation.publication_scope_hashes,
   publication_generation.blank_publication_scope_hashes,
   publication_generation.publication_export_timestamps,
   publication_generation.missing_publication_export_timestamps,
   publication_set.missing_expected_scope_rows,
   publication_set.unexpected_published_scope_rows,
   selected_integrity.invalid_selected_run_generations,
   selected_set.missing_selected_run_scope_rows,
   selected_set.unexpected_selected_run_scope_rows,
   dead.dead_match_count, dead.dead_match_distinct_count,
   dead.blank_dead_match_ids, dead.orphan_dead_match_ids,
   dead.dead_match_keys_md5
ORDER BY competition_name, source_season_id;
