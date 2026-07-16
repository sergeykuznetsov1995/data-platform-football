-- FBref typed season-dataset acceptance for DataGrip / PostgreSQL.
-- Set :control_run_id to the same UUID used by the Trino acceptance script.
-- Bind :expected_run_type, :expected_request_limit, and
-- :expected_byte_limit_mb to the exact Airflow profile. Supported acceptance
-- bindings are current/100/50 for canary, current/200/100 for production,
-- backfill/200/100 for a live historical batch, and replay/0/0 for offline
-- replay.
-- This control-plane evidence is required because a legitimate empty typed
-- dataset intentionally does not create an Iceberg table.

WITH selected_run AS (
    SELECT
        run.*,
        CASE
            WHEN run.metadata -> 'raw_audit'
                     ->> 'audited_control_run_id'
                 ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
            THEN CAST(
                run.metadata -> 'raw_audit'
                    ->> 'audited_control_run_id' AS uuid
            )
        END AS audited_control_run_id
    FROM fbref_control.crawl_run AS run
    WHERE run.run_id = CAST(:control_run_id AS uuid)
)
SELECT
    'control_run' AS check_name,
    CASE
        WHEN status = 'succeeded'
         AND run_type = CAST(:expected_run_type AS text)
         AND request_limit = CAST(:expected_request_limit AS integer)
         AND byte_limit =
             CAST(:expected_byte_limit_mb AS bigint) * 1048576
         AND metadata ? 'raw_baseline'
         AND metadata ? 'raw_audit'
         AND metadata -> 'raw_audit' ->> 'schema_version'
             = 'fbref-raw-audit-anchor-v1'
         AND metadata -> 'raw_audit' ->> 'status' = 'passed'
         AND metadata -> 'raw_audit' ->> 'run_type' = run_type
         AND metadata -> 'raw_audit' ->> 'zero_delta_required'
             = CASE WHEN run_type = 'replay' THEN 'true' ELSE 'false' END
         AND metadata -> 'raw_audit' ->> 'processing_control_run_id'
             = run_id::text
         AND audited_control_run_id IS NOT NULL
         AND (
             (
                 run_type <> 'replay'
                 AND audited_control_run_id = run_id
             )
             OR (
                 run_type = 'replay'
                 AND audited_control_run_id <> run_id
                 AND EXISTS (
                     SELECT 1
                     FROM fbref_control.crawl_run AS evidence_run
                     WHERE evidence_run.run_id = audited_control_run_id
                       AND evidence_run.status = 'succeeded'
                       AND evidence_run.run_type IN ('current', 'backfill')
                       AND evidence_run.request_limit = 200
                       AND evidence_run.byte_limit = 100 * 1048576
                 )
             )
         )
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
    request_limit,
    byte_limit,
    CAST(:expected_run_type AS text) AS expected_run_type,
    CAST(:expected_request_limit AS integer) AS expected_request_limit,
    CAST(:expected_byte_limit_mb AS bigint) AS expected_byte_limit_mb,
    started_at,
    finished_at,
    metadata -> 'raw_baseline' AS raw_baseline_anchor,
    metadata -> 'raw_fetch_attempt_snapshot' AS raw_attempt_snapshot,
    audited_control_run_id,
    metadata -> 'raw_audit' AS raw_audit_anchor
FROM selected_run;

-- Prove that a live run crossed the Firefox-152 -> warm-HTTP handoff with the
-- reviewed v6 transport. Replay must remain completely network-free.
WITH attempts AS (
    SELECT
        count(*) AS all_fetch_attempts,
        count(*) FILTER (
            WHERE reservation_id IS NOT NULL
        ) AS network_attempts,
        count(*) FILTER (
            WHERE reservation_id IS NOT NULL
              AND status = 'succeeded'
              AND http_status IN (200, 304)
        ) AS successful_warm_http_attempts,
        count(*) FILTER (
            WHERE reservation_id IS NOT NULL
              AND transport_version IS DISTINCT FROM
                  'fbref-camoufox-metered-warm-http-v6'
        ) AS unexpected_transport_versions
    FROM fbref_control.fetch_attempt
    WHERE run_id = CAST(:control_run_id AS uuid)
), sessions AS (
    SELECT
        count(*) AS clearance_sessions,
        count(*) FILTER (
            WHERE status IN ('closed', 'failed', 'expired')
        ) AS terminal_sessions,
        count(*) FILTER (WHERE status = 'active') AS active_sessions,
        count(*) FILTER (
            WHERE session_version IS DISTINCT FROM
                'fbref-camoufox-metered-warm-http-v6'
        ) AS unexpected_session_versions
    FROM fbref_control.clearance_session
    WHERE run_id = CAST(:control_run_id AS uuid)
), handoffs AS (
    SELECT
        count(*) FILTER (
            WHERE attempt.reservation_id IS NOT NULL
              AND attempt.status = 'succeeded'
              AND attempt.http_status IN (200, 304)
              AND attempt.transport_version =
                  'fbref-camoufox-metered-warm-http-v6'
              AND session.session_id IS NOT NULL
              AND session.session_version =
                  'fbref-camoufox-metered-warm-http-v6'
              AND session.status IN ('closed', 'failed', 'expired')
              AND session.browser_bootstrap_attempts > 0
              AND session.browser_bootstrap_requests > 0
              AND session.http_requests > 0
        ) AS linked_successful_warm_http_attempts,
        count(*) FILTER (
            WHERE attempt.reservation_id IS NOT NULL
              AND attempt.status = 'succeeded'
              AND attempt.http_status IN (200, 304)
              AND (
                  session.session_id IS NULL
                  OR attempt.transport_version IS DISTINCT FROM
                      'fbref-camoufox-metered-warm-http-v6'
                  OR session.session_version IS DISTINCT FROM
                      'fbref-camoufox-metered-warm-http-v6'
                  OR session.status NOT IN ('closed', 'failed', 'expired')
                  OR session.browser_bootstrap_attempts <= 0
                  OR session.browser_bootstrap_requests <= 0
                  OR session.http_requests <= 0
              )
        ) AS unlinked_successful_warm_http_attempts
    FROM fbref_control.fetch_attempt AS attempt
    LEFT JOIN fbref_control.clearance_session AS session
      ON session.run_id = attempt.run_id
     AND attempt.session_version = session.session_id::text
    WHERE attempt.run_id = CAST(:control_run_id AS uuid)
)
SELECT
    'reviewed_live_transport' AS check_name,
    CASE
        WHEN CAST(:expected_run_type AS text) = 'replay'
         AND all_fetch_attempts = 0
         AND clearance_sessions = 0
        THEN 'PASS'
        WHEN CAST(:expected_run_type AS text) <> 'replay'
         AND network_attempts > 0
         AND successful_warm_http_attempts > 0
         AND linked_successful_warm_http_attempts > 0
         AND unlinked_successful_warm_http_attempts = 0
         AND unexpected_transport_versions = 0
         AND clearance_sessions > 0
         AND terminal_sessions = clearance_sessions
         AND active_sessions = 0
         AND unexpected_session_versions = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS verdict,
    attempts.*,
    sessions.*,
    handoffs.*
FROM attempts
CROSS JOIN sessions
CROSS JOIN handoffs;

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

WITH selected_run AS (
    SELECT
        run.run_id,
        run.run_type,
        CASE
            WHEN run.run_type = 'replay'
             AND run.metadata -> 'raw_audit'
                     ->> 'audited_control_run_id'
                 ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
            THEN CAST(
                run.metadata -> 'raw_audit'
                    ->> 'audited_control_run_id' AS uuid
            )
            WHEN run.run_type <> 'replay' THEN run.run_id
        END AS evidence_run_id
    FROM fbref_control.crawl_run AS run
    WHERE run.run_id = CAST(:control_run_id AS uuid)
), run_fetches AS (
    SELECT DISTINCT ON (
        attempt.target_id, attempt.logical_refresh_id
    )
        attempt.target_id,
        attempt.logical_refresh_id,
        attempt.content_hash
    FROM fbref_control.fetch_attempt AS attempt
    CROSS JOIN selected_run
    WHERE attempt.run_id = selected_run.evidence_run_id
      AND attempt.status = 'succeeded'
      AND attempt.raw_manifest_key IS NOT NULL
      AND attempt.content_hash IS NOT NULL
    ORDER BY attempt.target_id,
             attempt.logical_refresh_id,
             COALESCE(attempt.finished_at, attempt.heartbeat_at) DESC,
             attempt.attempt_number DESC
), eligible_seasons AS (
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
        frontier.target_id,
        run_fetch.logical_refresh_id
    FROM eligible_seasons AS season
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.source = 'fbref'
     AND frontier.page_kind IN ('season', 'season_stats')
     AND frontier.source_ids ->> 'competition_id' = season.competition_id
     AND frontier.source_ids ->> 'season_id' = season.season_id
    JOIN run_fetches AS run_fetch
      ON run_fetch.target_id = frontier.target_id
    JOIN required
      ON required.stat_route = CASE
          WHEN frontier.page_kind = 'season' THEN 'standard'
          ELSE frontier.source_ids ->> 'stat_route'
      END
), run_observations AS (
    SELECT DISTINCT ON (run_fetch.logical_refresh_id)
        run_fetch.logical_refresh_id,
        processing.target_id,
        processing.content_hash,
        processing.typed_parser_version,
        processing.status AS processing_status,
        processing.typed_status,
        processing.validation_status AS processing_validation_status,
        processing.completed_at
    FROM run_fetches AS run_fetch
    JOIN fbref_control.observation_processing AS processing
      ON processing.logical_refresh_id = run_fetch.logical_refresh_id
     AND processing.target_id = run_fetch.target_id
     AND processing.content_hash = run_fetch.content_hash
     AND processing.parser_version = 'fbref-page-document-v3'
     AND processing.typed_parser_version = 'fbref-typed-bronze-v3'
     AND processing.stateful_parser_version = 'fbref-discovery-parser-v6'
    ORDER BY run_fetch.logical_refresh_id,
             COALESCE(processing.completed_at, processing.updated_at) DESC,
             processing.typed_parser_version DESC
), matrix AS (
    SELECT
        requirement.competition_id,
        requirement.season_id,
        requirement.dataset,
        requirement.stat_route,
        requirement.target_id,
        requirement.logical_refresh_id,
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
    LEFT JOIN run_observations AS observation
      ON observation.logical_refresh_id = requirement.logical_refresh_id
     AND observation.target_id = requirement.target_id
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
WITH selected_run AS (
    SELECT
        run.run_id,
        run.run_type,
        CASE
            WHEN run.run_type = 'replay'
             AND run.metadata -> 'raw_audit'
                     ->> 'audited_control_run_id'
                 ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
            THEN CAST(
                run.metadata -> 'raw_audit'
                    ->> 'audited_control_run_id' AS uuid
            )
            WHEN run.run_type <> 'replay' THEN run.run_id
        END AS evidence_run_id
    FROM fbref_control.crawl_run AS run
    WHERE run.run_id = CAST(:control_run_id AS uuid)
), run_fetches AS (
    SELECT DISTINCT ON (
        attempt.target_id, attempt.logical_refresh_id
    )
        attempt.target_id,
        attempt.logical_refresh_id,
        attempt.content_hash
    FROM fbref_control.fetch_attempt AS attempt
    CROSS JOIN selected_run
    WHERE attempt.run_id = selected_run.evidence_run_id
      AND attempt.status = 'succeeded'
      AND attempt.raw_manifest_key IS NOT NULL
      AND attempt.content_hash IS NOT NULL
    ORDER BY attempt.target_id,
             attempt.logical_refresh_id,
             COALESCE(attempt.finished_at, attempt.heartbeat_at) DESC,
             attempt.attempt_number DESC
), eligible_matches AS (
    SELECT
        frontier.target_id,
        run_fetch.logical_refresh_id,
        frontier.source_ids ->> 'competition_id' AS competition_id,
        frontier.source_ids ->> 'season_id' AS season_id,
        frontier.source_ids ->> 'match_id' AS match_id,
        COALESCE(
            season.metadata ->> 'direct_match_only' = 'true', false
        ) AS direct_match_only
    FROM fbref_control.page_frontier AS frontier
    JOIN run_fetches AS run_fetch
      ON run_fetch.target_id = frontier.target_id
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
), run_observations AS (
    SELECT DISTINCT ON (run_fetch.logical_refresh_id)
        run_fetch.logical_refresh_id,
        processing.target_id,
        processing.content_hash,
        processing.typed_parser_version,
        processing.status AS processing_status,
        processing.typed_status,
        processing.validation_status AS processing_validation_status
    FROM run_fetches AS run_fetch
    JOIN fbref_control.observation_processing AS processing
      ON processing.logical_refresh_id = run_fetch.logical_refresh_id
     AND processing.target_id = run_fetch.target_id
     AND processing.content_hash = run_fetch.content_hash
     AND processing.parser_version = 'fbref-page-document-v3'
     AND processing.typed_parser_version = 'fbref-typed-bronze-v3'
     AND processing.stateful_parser_version = 'fbref-discovery-parser-v6'
    ORDER BY run_fetch.logical_refresh_id,
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
    direct.logical_refresh_id,
    manifest.availability,
    manifest.row_count
FROM eligible_matches AS direct
CROSS JOIN required
LEFT JOIN run_observations AS observation
  ON observation.logical_refresh_id = direct.logical_refresh_id
 AND observation.target_id = direct.target_id
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
