-- One-shot, exact FBref current-season redirect remediation (2026-08-25).
--
-- DO NOT run this as a data-only fix. The matching exact-two redirect mapping
-- in scrapers/fbref/pipeline.py must be deployed first; otherwise the next
-- competition discovery reconciliation writes both old source slugs back.
--
-- EXECUTION ORDER (the FBref DAG stays paused throughout):
--   1. Deploy the code.
--   2. Run this read-only probe in the deployed scheduler; it must print the
--      mapping version and cohort-policy SHA256 shown here:
--      docker exec airflow-scheduler /opt/legacy-scraper-venv/bin/python -B -c "import hashlib,inspect; from scrapers.fbref.control.store import ControlStore; from scrapers.fbref.pipeline import SEASON_INSTALL_REDIRECT_VERSION as v, _canonical_season_install_url as f; h=hashlib.sha256(inspect.getsource(ControlStore.create_due_run_cohort).encode()).hexdigest(); assert v == 'fbref-season-install-redirects-20260825-v1'; assert h == '891f08fbf7ac22ca1d9c4e5ad27f3ec45b76bfe938266f5eea2a319158396c15'; assert f('33','2026-2027','https://fbref.com/en/comps/33/2-Bundesliga-Stats') == 'https://fbref.com/en/comps/33/2'; assert f('59','2026-2027','https://fbref.com/en/comps/59/3-Liga-Stats') == 'https://fbref.com/en/comps/59/3'; print(v,h)"
--   3. Apply this SQL while the active-run guard is clear. Its executable
--      preflight must prove that the production cohort policy would choose
--      comp59 then comp33 as prospective ordinals 0 and 1.
--   4. Trigger the standard bounded 100/50/25 canary. The preserved overdue
--      timestamps must put comp59 then comp33 at run_target ordinals 0 and 1.
--   5. Accept only if those exact two rows both finish with
--      HTTP 200 in exactly one target request each. Any 3xx is acceptance NO-GO.
--      Missing/wrong ordinals are also NO-GO: keep FBref ingestion paused and open a follow-up; do not accept or
--      publish that canary.
--      Run the fail-closed companion gate with the exact Airflow run id:
--      psql "$FBREF_CONTROL_DB_URI" --set=airflow_run_id=manual__fbref_redirect_alias_canary_20260825T163000Z --file=docs/operations/sql/fbref_20260825_redirect_alias_canary_gate.sql
--
-- The source returned the observed Locations with a trailing slash. FBref's
-- canonical URL contract removes trailing slashes, so registry/frontier store
-- /en/comps/33/2 and /en/comps/59/3. The post-update canary must prove the
-- no-slash forms do not redirect again.
--
-- This file preserves raw payloads, fetch attempts, manifests, content hashes,
-- last fetch timestamps, and the last HTTP status. It only repoints the exact
-- two registry/frontier identities, clears stale validators/errors, and keeps
-- their already-overdue scheduling timestamps. Preserving the 2026-08-03/05
-- order keeps these targets at canary ordinals 0/1, as observed in both the
-- 14:55 canary and the 15:40 prod1 run.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

SELECT pg_advisory_xact_lock(
    hashtextextended('fbref:remediation:2026-08-25', 0)
);

-- SHARE conflicts with the ROW EXCLUSIVE lock needed to create or start a
-- crawl run, closing the writer race between this guard and COMMIT.
LOCK TABLE fbref_control.crawl_run IN SHARE MODE;

DO $active_run_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.crawl_run
        WHERE status IN ('pending', 'running')
    ) THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: active crawl_run exists';
    END IF;
END
$active_run_guard$;

-- Freeze every relation that can change current-cohort membership, scope, or
-- ordering. The five-second lock timeout makes concurrent maintenance a loud
-- refusal instead of letting the preflight prove a stale prospective cohort.
LOCK TABLE fbref_control.page_frontier IN SHARE MODE;
LOCK TABLE fbref_control.competition_registry,
           fbref_control.frontier_provenance,
           fbref_control.run_target,
           fbref_control.season_alias,
           fbref_control.season_registry,
           fbref_control.fetch_attempt,
           fbref_control.observation_processing
IN SHARE MODE;

-- initialize_run reaps expired leases before seeding/recovery/live work.
-- With no active run, reject every leased frontier row, not only one already
-- expired: an unexpired orphan could cross its deadline after this transaction
-- and then change the cohort immediately before create_due_run_cohort.
DO $lease_zero_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.page_frontier AS frontier
        WHERE frontier.state = 'leased'
    ) THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: leased frontier row exists';
    END IF;
END
$lease_zero_guard$;

CREATE TEMP TABLE fbref_20260825_redirect_alias_mapping
ON COMMIT DROP
AS
SELECT *
FROM (VALUES
    (
        '33',
        '2026-2027',
        'fbref:season:33:2026-2027',
        'https://fbref.com/en/comps/33/2-Bundesliga-Stats',
        'https://fbref.com/en/comps/33/2/',
        'https://fbref.com/en/comps/33/2'
    ),
    (
        '59',
        '2026-2027',
        'fbref:season:59:2026-2027',
        'https://fbref.com/en/comps/59/3-Liga-Stats',
        'https://fbref.com/en/comps/59/3/',
        'https://fbref.com/en/comps/59/3'
    )
) AS mapping(
    competition_id,
    season_id,
    target_id,
    old_url,
    observed_location,
    canonical_new_url
);

-- No other identity may already own either the exact observed Location or its
-- canonical no-slash form.
DO $collision_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.season_registry AS holder
        JOIN fbref_20260825_redirect_alias_mapping AS mapping
          ON holder.canonical_url IN (
              mapping.observed_location,
              mapping.canonical_new_url
          )
        WHERE holder.source <> 'fbref'
           OR holder.competition_id <> mapping.competition_id
           OR holder.season_id <> mapping.season_id
    ) OR EXISTS (
        SELECT 1
        FROM fbref_control.page_frontier AS holder
        JOIN fbref_20260825_redirect_alias_mapping AS mapping
          ON holder.canonical_url IN (
              mapping.observed_location,
              mapping.canonical_new_url
          )
        WHERE holder.target_id <> mapping.target_id
    ) THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: destination URL collision';
    END IF;
END
$collision_guard$;

-- The DAG upserts this row immediately before recovery/live work. Require
-- that upsert to be scheduling-neutral; otherwise this preflight would rank a
-- state different from the state seen by create_due_run_cohort.
DO $competition_index_seed_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM fbref_control.page_frontier AS frontier
        WHERE frontier.target_id = 'fbref:competition_index:all'
          AND frontier.source = 'fbref'
          AND frontier.page_kind = 'competition_index'
          AND frontier.canonical_url = 'https://fbref.com/en/comps'
          AND frontier.source_ids =
              '{"competition_index":"all"}'::jsonb
          AND frontier.refresh_policy = 'daily'
          AND frontier.priority = 100
          AND frontier.state <> 'leased'
    ) THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: competition-index seed would change cohort ranking';
    END IF;
END
$competition_index_seed_guard$;

-- The current DAG drains reusable raw before opening live transport. A raw
-- observation still missing the exact parser fence could reconcile registry
-- or seed frontier rows between this preflight and live cohort creation. Be
-- conservative: any such current-lane observation makes this ceremony NO-GO.
DO $recovery_zero_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.fetch_attempt AS attempt
        JOIN fbref_control.crawl_run AS source_run
          ON source_run.run_id = attempt.run_id
        JOIN fbref_control.page_frontier AS frontier
          ON frontier.target_id = attempt.target_id
        WHERE frontier.source = 'fbref'
          AND source_run.run_type = 'current'
          AND frontier.state <> 'quarantined'
          AND attempt.status = 'succeeded'
          AND attempt.raw_manifest_key IS NOT NULL
          AND attempt.content_hash IS NOT NULL
          AND frontier.page_kind = ANY (ARRAY[
              'competition_index',
              'competition',
              'season',
              'season_stats',
              'schedule',
              'standings',
              'squad',
              'player',
              'matchlog',
              'match'
          ]::text[])
          AND NOT EXISTS (
              SELECT 1
              FROM fbref_control.observation_processing AS observed
              WHERE observed.logical_refresh_id =
                    attempt.logical_refresh_id
                AND observed.parser_version = 'fbref-page-document-v4'
                AND observed.typed_parser_version =
                    'fbref-typed-bronze-v4'
                AND observed.stateful_parser_version =
                    'fbref-discovery-parser-v6'
                AND observed.status = 'succeeded'
                AND observed.generic_status = 'succeeded'
                AND observed.typed_status IN ('succeeded', 'skipped')
                AND observed.stateful_status IN ('succeeded', 'skipped')
                AND observed.validation_status = 'succeeded'
          )
    ) THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: current raw recovery is not empty';
    END IF;
END
$recovery_zero_guard$;

-- Exact mirror of ControlStore.create_due_run_cohort for the current DAG's
-- fixed page-kind and refresh-policy sets. The deployed-method SHA256 probe
-- above prevents applying this mirror beside a different production policy.
-- No same-run membership exists before a fresh canary; the active-run guard
-- makes the production outstanding-run exclusion empty, but it remains here
-- so the safety property is reviewable in the executable query.
DO $prospective_cohort_guard$
DECLARE
    candidate record;
    prospective_target_ids text[] := ARRAY[]::text[];
BEGIN
    FOR candidate IN
        WITH declared_scope AS (
            SELECT frontier.target_id, frontier.source,
                   frontier.source_ids ->> 'competition_id' AS competition_id,
                   frontier.source_ids ->> 'season_id' AS season_id
            FROM fbref_control.page_frontier AS frontier
            WHERE frontier.source_ids ? 'competition_id'
            UNION
            SELECT edge.child_target_id AS target_id, child.source,
                   edge.carried_competition_id AS competition_id,
                   edge.carried_season_id AS season_id
            FROM fbref_control.frontier_provenance AS edge
            JOIN fbref_control.page_frontier AS child
              ON child.target_id = edge.child_target_id
            WHERE edge.carried_competition_id IS NOT NULL
        ),
        canonical_scope AS (
            SELECT declared.target_id, declared.source,
                   declared.competition_id,
                   COALESCE(alias.season_id, declared.season_id) AS season_id
            FROM declared_scope AS declared
            LEFT JOIN fbref_control.season_alias AS alias
              ON alias.source = declared.source
             AND alias.competition_id = declared.competition_id
             AND alias.alias = declared.season_id
        ),
        scope_rollup AS (
            SELECT scoped.target_id,
                   count(DISTINCT (
                       scoped.competition_id, scoped.season_id
                   )) AS scope_count,
                   bool_or(competition.competition_id IS NULL)
                       AS competition_missing,
                   bool_or(competition.gender = 'female') AS has_female,
                   bool_or(competition.gender = 'unknown') AS has_unknown,
                   bool_or(
                       competition.competition_id IS NOT NULL
                       AND (
                           competition.gender <> 'male'
                           OR competition.crawl_state <> 'active'
                           OR competition.lifecycle_state NOT IN (
                               'present', 'missing_once'
                           )
                           OR NOT competition.present
                       )
                   ) AS inactive_competition,
                   bool_or(
                       scoped.season_id IS NOT NULL
                       AND (
                           season.season_id IS NULL
                           OR season.lifecycle_state <> 'present'
                           OR NOT season.present
                       )
                   ) AS invalid_season,
                   bool_or(
                       scoped.season_id IS NOT NULL
                       AND season.lifecycle_state = 'present'
                       AND season.present
                       AND season.is_current
                   ) AS has_current_season,
                   bool_or(scoped.season_id IS NULL)
                       AS has_competition_scope
            FROM canonical_scope AS scoped
            LEFT JOIN fbref_control.competition_registry AS competition
              ON competition.source = scoped.source
             AND competition.competition_id = scoped.competition_id
            LEFT JOIN fbref_control.season_registry AS season
              ON season.source = scoped.source
             AND season.competition_id = scoped.competition_id
             AND season.season_id = scoped.season_id
            GROUP BY scoped.target_id
        ),
        sla(page_kind, sla_seconds) AS (
            VALUES
                ('competition', 604800),
                ('competition_index', 86400),
                ('match', 86400),
                ('matchlog', 2592000),
                ('player', 2592000),
                ('schedule', 86400),
                ('season', 604800),
                ('season_stats', 604800),
                ('squad', 604800),
                ('standings', 604800)
        ),
        eligible AS MATERIALIZED (
            SELECT frontier.target_id, frontier.page_kind,
                   frontier.last_fetched_at, frontier.created_at,
                   frontier.priority, frontier.next_fetch_at,
                   COALESCE(
                       frontier.retry_after,
                       frontier.next_fetch_at,
                       frontier.last_fetched_at,
                       frontier.created_at
                   ) AS due_at,
                   CASE
                       WHEN frontier.page_kind = 'competition_index'
                         THEN 0
                       WHEN frontier.page_kind = ANY (ARRAY[
                                'competition', 'season', 'schedule'
                            ]::text[])
                        AND frontier.refresh_policy <> 'historical_once'
                        AND COALESCE(
                              frontier.retry_after,
                              frontier.next_fetch_at,
                              frontier.last_fetched_at,
                              frontier.created_at
                            ) < clock_timestamp()
                                - (sla.sla_seconds * interval '1 second')
                         THEN 1
                       WHEN frontier.page_kind = 'match'
                        AND frontier.refresh_policy <> 'historical_once'
                        AND COALESCE(scope.has_current_season, false)
                         THEN 2
                       WHEN frontier.page_kind = ANY (ARRAY[
                                'competition', 'season', 'schedule'
                            ]::text[])
                        AND frontier.refresh_policy <> 'historical_once'
                         THEN 3
                       WHEN frontier.page_kind = ANY (ARRAY[
                                'season_stats', 'standings'
                            ]::text[])
                        AND frontier.refresh_policy <> 'historical_once'
                         THEN 4
                       ELSE 5
                   END AS admission_tier
            FROM fbref_control.page_frontier AS frontier
            LEFT JOIN scope_rollup AS scope
              ON scope.target_id = frontier.target_id
            LEFT JOIN sla
              ON sla.page_kind = frontier.page_kind
            WHERE (
                  frontier.state IN ('queued', 'retry')
                  OR (
                      frontier.state = 'fetched'
                      AND frontier.next_fetch_at IS NOT NULL
                      AND frontier.next_fetch_at <= clock_timestamp()
                  )
            )
              AND (
                  frontier.next_fetch_at IS NULL
                  OR frontier.next_fetch_at <= clock_timestamp()
              )
              AND (
                  frontier.retry_after IS NULL
                  OR frontier.retry_after <= clock_timestamp()
              )
              AND frontier.page_kind = ANY (ARRAY[
                  'competition_index',
                  'competition',
                  'season',
                  'season_stats',
                  'schedule',
                  'standings',
                  'squad',
                  'player',
                  'matchlog',
                  'match'
              ]::text[])
              AND frontier.refresh_policy = ANY (ARRAY[
                  'current_completed_once',
                  'daily',
                  'monthly',
                  'six_hourly',
                  'weekly'
              ]::text[])
              AND (
                  frontier.page_kind = 'competition_index'
                  OR (
                      scope.scope_count > 0
                      AND NOT COALESCE(scope.competition_missing, true)
                      AND NOT COALESCE(scope.has_female, false)
                      AND NOT COALESCE(scope.has_unknown, true)
                      AND NOT COALESCE(scope.inactive_competition, true)
                      AND NOT COALESCE(scope.invalid_season, true)
                      AND (
                          COALESCE(scope.has_competition_scope, false)
                          OR COALESCE(scope.has_current_season, false)
                          OR frontier.refresh_policy = 'historical_once'
                      )
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM fbref_control.run_target AS outstanding
                  JOIN fbref_control.crawl_run AS outstanding_run
                    ON outstanding_run.run_id = outstanding.run_id
                  WHERE outstanding.target_id = frontier.target_id
                    AND outstanding.status IN ('pending', 'leased', 'retry')
                    AND outstanding_run.status IN ('pending', 'running')
              )
        )
        SELECT frontier.target_id,
               eligible.admission_tier,
               eligible.due_at,
               frontier.priority,
               frontier.created_at
        FROM eligible
        JOIN fbref_control.page_frontier AS frontier
          ON frontier.target_id = eligible.target_id
        ORDER BY eligible.admission_tier,
                 eligible.due_at,
                 frontier.priority DESC,
                 frontier.created_at,
                 frontier.target_id
        LIMIT 2
        FOR UPDATE OF frontier SKIP LOCKED
    LOOP
        prospective_target_ids := array_append(
            prospective_target_ids,
            candidate.target_id
        );
    END LOOP;

    IF prospective_target_ids IS DISTINCT FROM ARRAY[
        'fbref:season:59:2026-2027',
        'fbref:season:33:2026-2027'
    ]::text[] THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: prospective cohort prefix is %, expected comp59 then comp33',
            prospective_target_ids;
    END IF;
END
$prospective_cohort_guard$;

CREATE TEMP TABLE fbref_20260825_redirect_registry_selected
ON COMMIT DROP
AS
SELECT registry.competition_id, registry.season_id
FROM fbref_control.season_registry AS registry
JOIN fbref_20260825_redirect_alias_mapping AS mapping
  ON registry.source = 'fbref'
 AND registry.competition_id = mapping.competition_id
 AND registry.season_id = mapping.season_id
 AND registry.canonical_url = mapping.old_url
WHERE registry.is_current
  AND registry.present
  AND registry.lifecycle_state = 'present'
ORDER BY registry.competition_id
FOR UPDATE OF registry;

CREATE TEMP TABLE fbref_20260825_redirect_frontier_selected
ON COMMIT DROP
AS
SELECT frontier.target_id
FROM fbref_control.page_frontier AS frontier
JOIN fbref_20260825_redirect_alias_mapping AS mapping
  ON frontier.target_id = mapping.target_id
 AND frontier.canonical_url = mapping.old_url
WHERE frontier.source = 'fbref'
  AND frontier.page_kind = 'season'
  AND frontier.source_ids ->> 'competition_id' = mapping.competition_id
  AND frontier.source_ids ->> 'season_id' = mapping.season_id
  AND frontier.state = 'queued'
  AND frontier.next_fetch_at < clock_timestamp()
  AND frontier.retry_after IS NULL
  AND frontier.last_http_status = 301
  AND frontier.last_error_class = 'http_status'
  AND ('location=' || mapping.observed_location) = ANY (
      string_to_array(frontier.last_error_message, ',')
  )
ORDER BY frontier.target_id
FOR UPDATE OF frontier;

DO $selection_guard$
DECLARE
    selected_registry_count integer;
    selected_frontier_count integer;
BEGIN
    SELECT count(*)
    INTO selected_registry_count
    FROM fbref_20260825_redirect_registry_selected;

    SELECT count(*)
    INTO selected_frontier_count
    FROM fbref_20260825_redirect_frontier_selected;

    IF selected_registry_count <> 2 THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: expected 2 registry rows, got %',
            selected_registry_count;
    END IF;
    IF selected_frontier_count <> 2 THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: expected 2 frontier rows, got %',
            selected_frontier_count;
    END IF;
END
$selection_guard$;

CREATE TEMP TABLE fbref_20260825_redirect_registry_updated
ON COMMIT DROP
AS
WITH updated_rows AS (
    UPDATE fbref_control.season_registry AS registry
    SET canonical_url = mapping.canonical_new_url
    FROM fbref_20260825_redirect_alias_mapping AS mapping
    JOIN fbref_20260825_redirect_registry_selected AS selected
      ON selected.competition_id = mapping.competition_id
     AND selected.season_id = mapping.season_id
    WHERE registry.source = 'fbref'
      AND registry.competition_id = mapping.competition_id
      AND registry.season_id = mapping.season_id
      AND registry.canonical_url = mapping.old_url
      AND registry.is_current
      AND registry.present
      AND registry.lifecycle_state = 'present'
    RETURNING registry.competition_id, registry.season_id
)
SELECT competition_id, season_id
FROM updated_rows;

CREATE TEMP TABLE fbref_20260825_redirect_frontier_updated
ON COMMIT DROP
AS
WITH updated_rows AS (
    UPDATE fbref_control.page_frontier AS frontier
    SET canonical_url = mapping.canonical_new_url,
        state = 'queued',
        retry_after = NULL,
        last_error_class = NULL,
        last_error_message = NULL,
        last_etag = NULL,
        last_modified = NULL,
        updated_at = clock_timestamp()
    FROM fbref_20260825_redirect_alias_mapping AS mapping
    JOIN fbref_20260825_redirect_frontier_selected AS selected
      ON selected.target_id = mapping.target_id
    WHERE frontier.target_id = mapping.target_id
      AND frontier.canonical_url = mapping.old_url
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season'
      AND frontier.state = 'queued'
      AND frontier.next_fetch_at < clock_timestamp()
      AND frontier.retry_after IS NULL
      AND frontier.last_http_status = 301
      AND frontier.last_error_class = 'http_status'
      AND ('location=' || mapping.observed_location) = ANY (
          string_to_array(frontier.last_error_message, ',')
      )
    RETURNING frontier.target_id
)
SELECT target_id
FROM updated_rows;

DO $update_guard$
DECLARE
    updated_registry_count integer;
    updated_frontier_count integer;
BEGIN
    SELECT count(*)
    INTO updated_registry_count
    FROM fbref_20260825_redirect_registry_updated;

    SELECT count(*)
    INTO updated_frontier_count
    FROM fbref_20260825_redirect_frontier_updated;

    IF updated_registry_count <> 2 THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: updated % registry rows',
            updated_registry_count;
    END IF;
    IF updated_frontier_count <> 2 THEN
        RAISE EXCEPTION
            'FBref redirect remediation refused: updated % frontier rows',
            updated_frontier_count;
    END IF;
END
$update_guard$;

SELECT mapping.target_id,
       mapping.old_url,
       mapping.observed_location,
       mapping.canonical_new_url
FROM fbref_20260825_redirect_alias_mapping AS mapping
JOIN fbref_20260825_redirect_frontier_updated AS updated
  ON updated.target_id = mapping.target_id
ORDER BY mapping.target_id;

COMMIT;
