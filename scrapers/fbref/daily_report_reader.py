"""Bounded SELECT-only snapshot collection for the daily host report."""

from datetime import date, datetime, timedelta
from typing import Callable

from scrapers.fbref.daily_report import adult_competition, instant
from scrapers.fbref.control.store import _frontier_scope_cte_for_candidates

REGISTRY_SQL = """
SELECT jsonb_build_object(
 'competitions', (SELECT coalesce(jsonb_agg(c), '[]') FROM (
   SELECT competition_id,name,gender,classification,crawl_state,present,lifecycle_state,
          jsonb_build_object('source_section',metadata->'source_section',
                             'current_scope_lifecycle',metadata->'current_scope_lifecycle',
                             'current_scope_reason',metadata->'current_scope_reason') AS metadata
   FROM fbref_control.competition_registry WHERE source='fbref') c),
 'seasons', (SELECT coalesce(jsonb_agg(s), '[]') FROM (
   SELECT competition_id,season_id,is_current,present,lifecycle_state
   FROM fbref_control.season_registry WHERE source='fbref' AND is_current) s),
 'schedule_health', (SELECT coalesce(jsonb_agg(h), '[]') FROM (
   SELECT s.competition_id, s.season_id,
          bool_or(f.page_kind = 'schedule' AND f.last_fetched_at IS NOT NULL
                  AND f.last_fetched_at <= '{cutoff}'::timestamptz
                  AND f.last_fetched_at >= '{cutoff}'::timestamptz - interval '24 hours'
                  AND EXISTS (SELECT 1 FROM fbref_control.dataset_manifest m
                      WHERE m.target_id=f.target_id AND m.content_hash=f.last_content_hash
                        AND m.dataset='typed:schedule' AND m.parse_status='succeeded'
                        AND m.persistence_status='succeeded' AND m.validation_status='succeeded'
                        AND m.completed_at <= '{cutoff}'::timestamptz)) AS fetched
   FROM fbref_control.season_registry s
   LEFT JOIN fbref_control.page_frontier f
     ON f.source_ids->>'competition_id' = s.competition_id
    AND f.source_ids->>'season_id' = s.season_id
   WHERE s.is_current AND s.present
   GROUP BY s.competition_id,s.season_id
 ) h)
)::text
"""

OBSERVATIONS_SQL = """
SELECT coalesce(jsonb_agg(o), '[]')::text
FROM fbref_control.match_report_observation o
JOIN fbref_control.season_registry s USING (competition_id,season_id)
WHERE s.is_current AND o.first_seen_at <= '{cutoff}'::timestamptz
"""

READINESS_SQL = """
SELECT coalesce(jsonb_agg(r), '[]')::text FROM (
 SELECT o.competition_id,o.season_id,o.match_id,
        min(a.finished_at) AS first_fetch_at,
        min(CASE WHEN p.completed_at IS NOT NULL AND t.completed_at IS NOT NULL
                 THEN greatest(p.completed_at, t.completed_at) END) AS bronze_ready_at
 FROM fbref_control.match_report_observation o
 JOIN fbref_control.season_registry s USING (competition_id,season_id)
 JOIN fbref_control.page_frontier f ON f.target_id = 'fbref:match:' || o.match_id
 LEFT JOIN fbref_control.fetch_attempt a ON a.target_id = f.target_id
  AND a.status = 'succeeded' AND a.http_status IN (200,304)
  AND a.finished_at <= '{cutoff}'::timestamptz
 LEFT JOIN fbref_control.dataset_manifest p ON p.target_id = f.target_id
  AND p.content_hash = a.content_hash AND p.dataset = '__page__'
  AND p.parse_status = 'succeeded' AND p.persistence_status = 'succeeded'
  AND p.validation_status = 'succeeded'
  AND p.completed_at >= o.first_completed_seen_at
  AND p.completed_at <= '{cutoff}'::timestamptz
 LEFT JOIN fbref_control.dataset_manifest t ON t.target_id = f.target_id
  AND t.content_hash = p.content_hash AND t.dataset = 'typed:__complete__'
  AND t.parse_status = 'succeeded' AND t.persistence_status = 'succeeded'
  AND t.validation_status = 'succeeded'
  AND t.completed_at >= o.first_completed_seen_at
  AND t.completed_at <= '{cutoff}'::timestamptz
 WHERE s.is_current
 GROUP BY o.competition_id,o.season_id,o.match_id
) r
"""


# Canonical scopes include aliases and discovery provenance. Aggregate them
# before joining the frontier so a shared roster counts exactly once.
PAGE_AGES_SQL = _frontier_scope_cte_for_candidates("""
 SELECT target_id FROM fbref_control.page_frontier
 WHERE source='fbref' AND refresh_policy <> 'historical_once'
   AND state NOT IN ('dead','skipped','quarantined')
""") + """
SELECT coalesce(jsonb_agg(p), '[]')::text FROM (
 SELECT f.target_id,f.page_kind,f.canonical_url,f.state,f.refresh_policy,
        f.last_fetched_at, f.last_content_hash,
        EXISTS (SELECT 1 FROM fbref_control.dataset_manifest m
          WHERE m.target_id=f.target_id AND m.content_hash=f.last_content_hash
            AND m.dataset='__page__' AND m.parse_status='succeeded'
            AND m.persistence_status='succeeded' AND m.validation_status='succeeded'
            AND m.completed_at <= '{cutoff}'::timestamptz) AS processed,
        coalesce(scope.has_current_ownership,false)
          AND NOT coalesce(scope.competition_missing,true)
          AND NOT coalesce(scope.has_female,false)
          AND NOT coalesce(scope.has_unknown,true)
          AND NOT coalesce(scope.inactive_competition,true)
          AND NOT coalesce(scope.invalid_season,true) AS current_ownership,
        (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
           'competition_id',c.competition_id,'season_id',c.season_id)), '[]')
         FROM canonical_scope c WHERE c.target_id=f.target_id) AS scopes
 FROM fbref_control.page_frontier f
 LEFT JOIN scope_rollup scope USING (target_id)
 WHERE f.source='fbref' AND f.refresh_policy <> 'historical_once'
   AND f.state NOT IN ('dead','skipped','quarantined') AND f.created_at <= '{cutoff}'::timestamptz
) p
"""


PAGE_AGE_EXCLUSIONS_SQL = """
SELECT coalesce(jsonb_object_agg(reason,n), '{}')::text FROM (
 SELECT CASE WHEN state IN ('dead','skipped','quarantined') THEN state ELSE 'history' END AS reason,
        count(*) AS n
 FROM fbref_control.page_frontier WHERE source='fbref'
   AND created_at <= '{cutoff}'::timestamptz
   AND (state IN ('dead','skipped','quarantined') OR refresh_policy='historical_once')
 GROUP BY 1
) counts
"""


def collect_snapshot(pg: Callable, trino: Callable, *, day: date,
                     as_of: datetime, mapping: dict, lookback: int = 14) -> dict:
    """Call injected SELECT executors; never migrate, seed, fetch or publish."""
    cutoff = instant(as_of).isoformat()
    snapshot = {"competitions": [], "seasons": [], "schedules": [],
                "observations": [], "readiness": [], "schedule_health": [],
                "fotmob_seasons": [], "fotmob_matches": [], "errors": [], "page_age_errors": []}
    def read(label, function, query, default):
        try:
            return function(query)
        except Exception:
            # Driver exceptions may contain credentials: record the failed source only.
            error_key = "page_age_errors" if label in {
                "fbref_page_ages", "fbref_page_age_exclusions", "fbref_standings_inventory"
            } else "errors"
            snapshot[error_key].append({"source": label, "reason": "read_failed"})
            return default
    registry = read("fbref_registry", pg, REGISTRY_SQL.format(cutoff=cutoff), {})
    snapshot.update(registry)
    snapshot["observations"] = read("fbref_observations", pg, OBSERVATIONS_SQL.format(cutoff=cutoff), [])
    snapshot["readiness"] = read("fbref_readiness", pg, READINESS_SQL.format(cutoff=cutoff), [])
    snapshot["page_ages"] = read("fbref_page_ages", pg, PAGE_AGES_SQL.replace("{cutoff}", cutoff), [])
    snapshot["page_age_exclusions"] = read("fbref_page_age_exclusions", pg,
        PAGE_AGE_EXCLUSIONS_SQL.replace("{cutoff}", cutoff), {})
    adults = {str(c["competition_id"]) for c in snapshot["competitions"] if adult_competition(c)}
    if not adults:
        snapshot["errors"].append({"source": "fbref_registry", "reason": "empty_adult_universe"})
        return snapshot
    fb_ids = ",".join("'" + str(int(cid)) + "'" for cid in sorted(adults))
    snapshot["standings_inventory"] = read("fbref_standings_inventory", trino, f"""
        SELECT DISTINCT target_id,content_hash
        FROM iceberg.bronze.fbref_table_inventory
        WHERE page_kind='season' AND regexp_like(table_id, '^results.*_overall$')
          AND availability IN ('available','empty')
          AND persisted_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
    """, [])
    start = (day - timedelta(days=lookback - 1)).isoformat()
    end = day.isoformat()
    snapshot["schedules"] = read("fbref_schedule", trino, f"""
        SELECT source_competition_id,source_season_id,date,time,home,away,score,notes,match_url
        FROM iceberg.bronze.fbref_schedule
        WHERE source_competition_id IN ({fb_ids})
          AND try_cast(date AS date) BETWEEN DATE '{start}' - INTERVAL '1' DAY AND DATE '{end}' + INTERVAL '1' DAY
          AND _ingested_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
    """, [])
    fm_ids = sorted({str(int(fid)) for cid in adults for fid in mapping.get(cid, {}).get("fotmob_ids", [])})
    if not fm_ids:
        return snapshot
    fm_list = ",".join("'" + fid + "'" for fid in fm_ids)
    snapshot["fotmob_seasons"] = read("fotmob_season_manifest", trino, f"""
        WITH evidence AS (
          SELECT *, coalesce(
            try(CAST(from_iso8601_timestamp(json_extract_scalar(
              capabilities_json, '$.source_observed_at')) AT TIME ZONE 'UTC' AS timestamp)),
            fetched_at) AS validated_at
          FROM iceberg.bronze.fotmob_ingest_manifest
        )
        SELECT competition_id,source_season_key AS season_key,
               to_iso8601(with_timezone(max(fetched_at), 'UTC')) AS fetched_at,
               to_iso8601(with_timezone(max(validated_at), 'UTC')) AS validated_at
        FROM evidence
        WHERE target_type = 'league_season' AND competition_id IN ({fm_list})
          AND status IN ('success','not_modified') AND NOT coalesce(stale,false)
          AND fetched_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
          AND validated_at >= fetched_at
          AND validated_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
          AND completed_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
        GROUP BY competition_id,source_season_key
    """, [])
    snapshot["fotmob_phase_catalog"] = read("fotmob_phase_catalog", trino, f"""
        WITH catalogs AS (
          SELECT competition_id,batch_id,fetched_at,stale,
                 row_number() OVER (PARTITION BY competition_id
                                    ORDER BY completed_at DESC,batch_id DESC) AS rn
          FROM iceberg.bronze.fotmob_ingest_manifest
          WHERE target_type='competition_seasons' AND competition_id IN ({fm_list})
            AND status IN ('success','not_modified')
            AND fetched_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
            AND completed_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
        )
        SELECT CAST(s.competition_id AS varchar) AS competition_id,
               s.source_season_key AS season_key,
               to_iso8601(with_timezone(m.fetched_at, 'UTC')) AS fetched_at,
               to_iso8601(with_timezone(m.fetched_at, 'UTC')) AS validated_at
        FROM catalogs m JOIN iceberg.bronze.fotmob_competition_seasons s
          ON s._target_batch_id=m.batch_id
        WHERE m.rn=1 AND NOT coalesce(m.stale,false)
          AND s._ingested_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
    """, [])
    snapshot["fotmob_matches"] = read("fotmob_matches", trino, f"""
        SELECT competition_id,source_season_key AS season_key,match_id,utc_time,timezone,
               finished,cancelled,postponed,awarded
        FROM iceberg.bronze.fotmob_matches_current
        WHERE CAST(competition_id AS varchar) IN ({fm_list})
          AND (try(from_iso8601_timestamp(utc_time)) IS NULL OR
               try(from_iso8601_timestamp(utc_time)) BETWEEN
              from_iso8601_timestamp('{start}T00:00:00Z') - INTERVAL '1' DAY
              AND from_iso8601_timestamp('{end}T23:59:59Z') + INTERVAL '1' DAY)
          AND _observed_at <= CAST(from_iso8601_timestamp('{cutoff}') AT TIME ZONE 'UTC' AS timestamp)
    """, [])
    return snapshot
