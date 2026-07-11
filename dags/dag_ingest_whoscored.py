"""Daily WhoScored ingestion for canonical active competition scopes.

The DAG no longer sends five historical seasons to every competition.  At
parse time it resolves one configured active season per WhoScored competition
and creates one independent task per ``competition=season`` scope.  The shared
``ingest_scraper_pool`` serialises browser work without an ``all_done`` task
chain, so a failed competition remains visible while unrelated scopes can
still finish.

The v2 runner's ``all`` command covers schedule, targeted previews, events and
lineups.  One global profile task then fetches at most 200 unseen players from
the committed lineup roster; it never repeats the old daily full-roster crawl.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import bronze_count, validate_table
from utils.config import DAG_TAGS, SCHEDULES, WHOSCORED_LEAGUES
from utils.default_args import SCRAPER_ARGS
from utils.ingest_helpers import league_slug as _league_slug
from utils.medallion_config import (
    get_active_season,
    get_competition_seasons,
    is_single_year_competition,
)


logger = logging.getLogger(__name__)


def _canonical_season_id(competition_id: str, active_season: int) -> str:
    """Resolve ``get_active_season`` output to a configured canonical ID.

    ``get_active_season`` returns a Gregorian start year for split-year club
    leagues (2025), while the catalog/WhoScored partition uses ``2526``.  A
    single-year tournament already returns its literal canonical year (2026).
    The result must exist in the catalog; a missing new season deliberately
    makes the DAG broken instead of silently writing to a wrong partition.
    """
    configured = {str(value) for value in get_competition_seasons(competition_id)}
    if is_single_year_competition(competition_id):
        candidate = str(active_season)
    else:
        start = int(active_season)
        candidate = f"{start % 100:02d}{(start + 1) % 100:02d}"
    if candidate not in configured:
        raise ValueError(
            f"WhoScored active scope {competition_id}={candidate} is not in "
            "configs/medallion/competitions.yaml"
        )
    return candidate


def _active_scope_specs() -> List[str]:
    """Return deterministic canonical scopes active on this DAG parse."""
    scopes: List[str] = []
    for competition_id in WHOSCORED_LEAGUES:
        active = get_active_season(competition_id)
        if active is None:
            logger.info("WhoScored scope %s is outside its active window", competition_id)
            continue
        scopes.append(
            f"{competition_id}={_canonical_season_id(competition_id, active)}"
        )
    return scopes


ACTIVE_WHOSCORED_SCOPES = _active_scope_specs()


# Transport retries/backoff are explicit and persisted in source manifests.
# An Airflow retry before ``retry_after`` would see zero due candidates and
# incorrectly turn the original retryable exit into a green task, so the next
# scheduled/manual DagRun owns recovery. Captured raw remains network-free.
WHOSCORED_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key not in {"retries", "retry_delay"}
    },
    "execution_timeout": timedelta(hours=2),
    "retries": 0,
}


def validate_schedule(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_schedule (~1900 rows/league).

    #920 Phase 2: per-league floors — every league in WHOSCORED_LEAGUES must
    individually clear its competitions.yaml-derived floor (a missing league
    used to hide behind the whole-table aggregate).
    """
    return validate_table(
        'whoscored_schedule', 'whoscored_schedule', leagues=WHOSCORED_LEAGUES
    )


def validate_events(**_context) -> Dict[str, Any]:
    # Validate only logically committed batches; the physical append table may
    # contain orphan rows from an interrupted write.
    return validate_table("whoscored_events_current", "whoscored_events")


def _validate_bronze_available(table_name: str) -> Dict[str, Any]:
    """Fail closed when a required V2 table/view is unavailable."""
    try:
        rows = bronze_count(table_name)
    except Exception as exc:
        raise AirflowException(
            f"Required WhoScored v2 table iceberg.bronze.{table_name} "
            f"is unavailable: {exc}"
        ) from exc
    return {"table": table_name, "rows": rows}


def _validate_nonempty_bronze(table_name: str) -> Dict[str, Any]:
    """Mandatory non-empty guard for V2 datasets with established history."""
    result = _validate_bronze_available(table_name)
    rows = int(result["rows"])
    if rows < 1:
        raise AirflowException(
            f"Required WhoScored v2 table {table_name} is empty"
        )
    result["threshold"] = 1
    return result


def validate_lineups(**_context) -> Dict[str, Any]:
    """Lineups are committed alongside events and must never be unguarded."""
    return _validate_nonempty_bronze("whoscored_lineups_current")


def _manifest_integrity_summary() -> Dict[str, int]:
    """Check latest-manifest state and physical row-count agreement."""
    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_match_ingest_manifest m
                    ) WHERE rn = 1
                ),
                event_counts AS (
                    SELECT league, season, CAST(game_id AS BIGINT) AS game_id,
                           _game_batch_id AS batch_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_events
                    WHERE _game_batch_id IS NOT NULL
                    GROUP BY league, season, CAST(game_id AS BIGINT), _game_batch_id
                ),
                lineup_counts AS (
                    SELECT league, season, CAST(game_id AS BIGINT) AS game_id,
                           _game_batch_id AS batch_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_lineups
                    WHERE _game_batch_id IS NOT NULL
                    GROUP BY league, season, CAST(game_id AS BIGINT), _game_batch_id
                )
                SELECT
                    COUNT_IF(l.state = 'success') AS successful_games,
                    COUNT_IF(l.state NOT IN (
                        'success', 'retryable', 'terminal', 'parse_failed'
                    )) AS invalid_states,
                    COUNT_IF(
                        l.state = 'success'
                        AND (
                            l.batch_id IS NULL
                            OR l.events_count <= 0
                            OR (
                                l.parser_version <> 'legacy-v1'
                                AND (
                                    l.payload_sha256 IS NULL
                                    OR l.raw_uri IS NULL
                                )
                            )
                        )
                    ) AS invalid_success_rows,
                    COUNT_IF(
                        l.state = 'success'
                        AND (
                            COALESCE(e.rows_count, 0) <> l.events_count
                            OR COALESCE(p.rows_count, 0) <> l.lineups_count
                        )
                    ) AS count_mismatches,
                    (
                        SELECT COUNT(*)
                        FROM iceberg.bronze.whoscored_events
                        WHERE _game_batch_id IS NULL
                    ) + (
                        SELECT COUNT(*)
                        FROM iceberg.bronze.whoscored_lineups
                        WHERE _game_batch_id IS NULL
                    ) AS unbatched_payload_rows
                FROM latest l
                LEFT JOIN event_counts e
                  ON e.league = l.league AND e.season = l.season
                 AND e.game_id = l.game_id AND e.batch_id = l.batch_id
                LEFT JOIN lineup_counts p
                  ON p.league = l.league AND p.season = l.season
                 AND p.game_id = l.game_id AND p.batch_id = l.batch_id
                """
            )
            row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "successful_games": int(row[0] or 0),
        "invalid_states": int(row[1] or 0),
        "invalid_success_rows": int(row[2] or 0),
        "count_mismatches": int(row[3] or 0),
        "unbatched_payload_rows": int(row[4] or 0),
    }


def validate_match_ingest_manifest(**_context) -> Dict[str, Any]:
    """The v2 logical-commit manifest is mandatory before this DAG is enabled."""
    result = _validate_nonempty_bronze("whoscored_match_ingest_manifest")
    try:
        integrity = _manifest_integrity_summary()
    except Exception as exc:
        raise AirflowException(
            f"WhoScored match manifest integrity query failed: {exc}"
        ) from exc
    if integrity["successful_games"] < 1:
        raise AirflowException("WhoScored manifest has no successful game commits")
    violations = sum(
        integrity[key]
        for key in (
            "invalid_states",
            "invalid_success_rows",
            "count_mismatches",
            "unbatched_payload_rows",
        )
    )
    if violations:
        raise AirflowException(
            f"WhoScored manifest integrity violations: {integrity}"
        )
    result["integrity"] = integrity
    return result


def _preview_manifest_integrity_summary() -> Dict[str, int]:
    """Validate logical preview commits, including valid zero-row snapshots."""
    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     COALESCE(batch_id, '') DESC, _batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_preview_ingest_manifest m
                    ) WHERE rn = 1
                ),
                latest_success AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     batch_id DESC, _batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_preview_ingest_manifest m
                        WHERE state = 'success'
                    ) WHERE rn = 1
                ),
                payload_counts AS (
                    SELECT league, season, CAST(game_id AS BIGINT) AS game_id,
                           _preview_batch_id AS batch_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_missing_players
                    WHERE _preview_batch_id IS NOT NULL
                    GROUP BY league, season, CAST(game_id AS BIGINT),
                             _preview_batch_id
                )
                SELECT
                    (SELECT COUNT(*) FROM latest_success) AS successful_games,
                    (SELECT COUNT(*) FROM latest WHERE state NOT IN (
                        'success', 'retryable', 'terminal', 'parse_failed'
                    )) AS invalid_states,
                    (SELECT COUNT(*) FROM latest_success s
                     WHERE s.batch_id IS NULL
                        OR s.missing_players_count < 0
                        OR (
                            s.parser_version <> 'legacy-v1'
                            AND (s.payload_sha256 IS NULL OR s.raw_uri IS NULL)
                        )) AS invalid_success_rows,
                    (SELECT COUNT(*)
                     FROM latest_success s
                     LEFT JOIN payload_counts p
                       ON p.league = s.league AND p.season = s.season
                      AND p.game_id = s.game_id AND p.batch_id = s.batch_id
                     WHERE COALESCE(p.rows_count, 0) <>
                           s.missing_players_count) AS count_mismatches,
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_missing_players
                     WHERE _preview_batch_id IS NULL) AS null_batch_rows
                """
            )
            row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "successful_games": int(row[0] or 0),
        "invalid_states": int(row[1] or 0),
        "invalid_success_rows": int(row[2] or 0),
        "count_mismatches": int(row[3] or 0),
        "null_batch_rows": int(row[4] or 0),
    }


def validate_preview_ingest_manifest(**_context) -> Dict[str, Any]:
    """Preview manifests may be empty, but committed batches must be exact."""
    result = _validate_bronze_available("whoscored_preview_ingest_manifest")
    _validate_bronze_available("whoscored_missing_players_current")
    try:
        integrity = _preview_manifest_integrity_summary()
    except Exception as exc:
        raise AirflowException(
            f"WhoScored preview manifest integrity query failed: {exc}"
        ) from exc
    violations = sum(
        integrity[key]
        for key in (
            "invalid_states",
            "invalid_success_rows",
            "count_mismatches",
            "null_batch_rows",
        )
    )
    if violations:
        raise AirflowException(
            f"WhoScored preview manifest integrity violations: {integrity}"
        )
    result["integrity"] = integrity
    return result


def validate_profile_manifest(**_context) -> Dict[str, Any]:
    """Profiles are global/versioned and must have a committed seed/current set."""
    return _validate_nonempty_bronze("whoscored_profile_ingest_manifest")


with DAG(
    dag_id="dag_ingest_whoscored",
    default_args=WHOSCORED_ARGS,
    description="Ingest active WhoScored scopes with manifest-backed match commits",
    schedule=SCHEDULES.get("dag_ingest_whoscored", "0 10 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get(
        "whoscored", ["scraping", "whoscored", "bronze", "spadl"]
    ),
    max_active_runs=1,
    params={"scopes": ACTIVE_WHOSCORED_SCOPES},
    doc_md=f"""
    ## WhoScored v2 ingestion

    Daily scope is restricted to configured active seasons:
    `{', '.join(ACTIVE_WHOSCORED_SCOPES) or '(none)'}`.

    Each scope is independent and uses the shared one-slot scraper pool. The
    runner calls the direct-first ingestion service with no raw proxy list.
    A task failure is not hidden by a sequential `all_done` chain.

    After every scope has finished, validation checks schedule, events,
    lineups, match commits and preview commits. Both manifest tables and their
    current views are mandatory before the v2 DAG is enabled.
    """,
) as dag:
    task_env = {
        "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
        "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
        "HOME": "/home/airflow",
    }

    scrape_tasks: List[BashOperator] = []
    for scope_spec in ACTIVE_WHOSCORED_SCOPES:
        competition_id, season_id = scope_spec.rsplit("=", 1)
        task_slug = f"{_league_slug(competition_id)}_{season_id}"
        scrape_tasks.append(
            BashOperator(
                task_id=f"scrape_whoscored_{task_slug}",
                bash_command=(
                    "cd /opt/airflow && "
                    "python dags/scripts/run_whoscored_scraper.py all "
                    f"--scope \"{scope_spec}\" "
                    f"--output /tmp/whoscored_result_{task_slug}.json"
                ),
                env=task_env,
                append_env=True,
                # Tasks are independent. SCRAPER_ARGS supplies the shared
                # ingest_scraper_pool, which serialises the browser itself.
                trigger_rule="all_success",
            )
        )

    validation_tasks = [
        PythonOperator(
            task_id="validate_schedule",
            python_callable=validate_schedule,
            trigger_rule="all_done",
        ),
        PythonOperator(
            task_id="validate_events",
            python_callable=validate_events,
            trigger_rule="all_done",
        ),
        PythonOperator(
            task_id="validate_lineups",
            python_callable=validate_lineups,
            trigger_rule="all_done",
        ),
        PythonOperator(
            task_id="validate_match_ingest_manifest",
            python_callable=validate_match_ingest_manifest,
            trigger_rule="all_done",
        ),
        PythonOperator(
            task_id="validate_preview_ingest_manifest",
            python_callable=validate_preview_ingest_manifest,
            trigger_rule="all_done",
        ),
        PythonOperator(
            task_id="validate_profile_manifest",
            python_callable=validate_profile_manifest,
            trigger_rule="all_done",
        ),
    ]

    profile_task = None
    if ACTIVE_WHOSCORED_SCOPES:
        profile_scope_args = " ".join(
            f'--scope "{scope}"' for scope in ACTIVE_WHOSCORED_SCOPES
        )
        profile_task = BashOperator(
            task_id="refresh_whoscored_profiles",
            bash_command=(
                "cd /opt/airflow && "
                "python dags/scripts/run_whoscored_scraper.py profiles "
                f"{profile_scope_args} --limit 200 "
                "--output /tmp/whoscored_result_profiles.json"
            ),
            env=task_env,
            append_env=True,
            trigger_rule="all_success",
            # The 200-player limit is a daily traffic ceiling. Retrying after
            # partial commits could select the next 200 unseen players, so the
            # non-critical profile refresh deliberately waits for tomorrow.
            retries=0,
        )
        for scrape_task in scrape_tasks:
            scrape_task >> profile_task

    # Validators still run after failed scrape/profile tasks and provide data
    # diagnostics. The original upstream failure remains part of DagRun state.
    validation_upstreams = [profile_task] if profile_task is not None else scrape_tasks
    for upstream in validation_upstreams:
        for validation_task in validation_tasks:
            upstream >> validation_task
