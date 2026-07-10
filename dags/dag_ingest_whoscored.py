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
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import bronze_count, validate_table
from utils.config import DAG_TAGS, SCHEDULES, WHOSCORED_LEAGUES
from utils.default_args import SELENIUM_ARGS
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


def _whoscored_raw_store_uri() -> str:
    """Resolve a raw-store URI even in Airflow containers not yet recreated.

    During the rolling v2 deployment an already-running scheduler has the
    long-standing FBref raw-store variable but not necessarily the new
    WhoScored variable.  Both stores are siblings under the same raw prefix,
    so deriving the latter avoids making the first post-deploy task depend on
    a scheduler restart.
    """
    configured = os.environ.get("WHOSCORED_RAW_STORE_URI", "").strip()
    if configured:
        return configured
    # ``football`` is the checked-in Lakekeeper/Trino warehouse name.  The
    # fallback is only for rolling deployment of containers created before
    # either raw-store variable existed; recreated containers receive the
    # interpolated value from compose.
    fbref_uri = os.environ.get(
        "FBREF_RAW_STORE_URI", "s3://football/raw/fbref"
    ).strip().rstrip("/")
    prefix, separator, _ = fbref_uri.rpartition("/")
    if not separator:
        raise ValueError("FBREF_RAW_STORE_URI must contain a path component")
    return f"{prefix}/whoscored"


# Transport retries are explicit and bounded inside the scraper.  One Airflow
# retry remains for infrastructure/storage failures; raw-first persistence
# makes a retry network-free once a payload has been captured.
WHOSCORED_ARGS = {
    **SELENIUM_ARGS,
    "execution_timeout": timedelta(hours=2),
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
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


def _validate_nonempty_bronze(table_name: str) -> Dict[str, Any]:
    """Mandatory presence guard for new v2 tables without global row floors."""
    try:
        rows = bronze_count(table_name)
    except Exception as exc:
        raise AirflowException(
            f"Required WhoScored v2 table iceberg.bronze.{table_name} "
            f"is unavailable: {exc}"
        ) from exc
    if rows < 1:
        raise AirflowException(
            f"Required WhoScored v2 table {table_name} is empty"
        )
    return {"table": table_name, "rows": rows, "threshold": 1}


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
                    ) AS count_mismatches
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
        for key in ("invalid_states", "invalid_success_rows", "count_mismatches")
    )
    if violations:
        raise AirflowException(
            f"WhoScored manifest integrity violations: {integrity}"
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
    runner tries direct HTTP first; no raw proxy file is passed by this DAG.
    A task failure is not hidden by a sequential `all_done` chain.

    After every scope has finished, validation checks schedule, events,
    lineups and the match-ingest manifest. The manifest table is mandatory for
    v2 rollout; the DAG must not be enabled before its additive DDL is applied.
    """,
) as dag:
    task_env = {
        "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
        "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
        "HOME": "/home/airflow",
        # Disable the legacy scraper hook: it routes every FlareSolverr request
        # through PROXY_FILTER_URL. V2 receives the filter endpoint under a
        # distinct name and opens a paid lease only after classified direct CF.
        "PROXY_FILTER_URL": "",
        "WHOSCORED_RAW_STORE_URI": _whoscored_raw_store_uri(),
        "WHOSCORED_PAID_PROXY_URL": os.environ.get(
            "WHOSCORED_PAID_PROXY_URL", "http://proxy_filter:8900"
        ),
        "WHOSCORED_PROXY_CONTROL_URL": os.environ.get(
            "WHOSCORED_PROXY_CONTROL_URL", "http://proxy_filter:8899"
        ),
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
                    "--flaresolverr-url http://flaresolverr:8191 "
                    f"--output /tmp/whoscored_result_{task_slug}.json"
                ),
                env=task_env,
                append_env=True,
                # Tasks are independent.  SELENIUM_ARGS supplies the shared
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
            task_id="validate_profile_manifest",
            python_callable=validate_profile_manifest,
            trigger_rule="all_done",
        ),
    ]

    profile_task = None
    if ACTIVE_WHOSCORED_SCOPES:
        profile_scope = ACTIVE_WHOSCORED_SCOPES[0]
        profile_task = BashOperator(
            task_id="refresh_whoscored_profiles",
            bash_command=(
                "cd /opt/airflow && "
                "python dags/scripts/run_whoscored_scraper.py profiles "
                f"--scope \"{profile_scope}\" --limit 200 "
                "--flaresolverr-url http://flaresolverr:8191 "
                "--output /tmp/whoscored_result_profiles.json"
            ),
            env=task_env,
            append_env=True,
            trigger_rule="all_success",
        )
        for scrape_task in scrape_tasks:
            scrape_task >> profile_task

    # Validators still run after failed scrape/profile tasks and provide data
    # diagnostics. The original upstream failure remains part of DagRun state.
    validation_upstreams = [profile_task] if profile_task is not None else scrape_tasks
    for upstream in validation_upstreams:
        for validation_task in validation_tasks:
            upstream >> validation_task
