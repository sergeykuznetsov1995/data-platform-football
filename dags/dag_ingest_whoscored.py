"""Daily production ingestion for the persisted WhoScored catalog.

Catalog access happens only in runtime tasks.  DAG parsing therefore remains
safe while Trino is restarting or before the additive catalog migration has
run, and there is no static fallback to the historical six competitions.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS


RUN_ROOT = "/opt/airflow/logs/whoscored_runs"
PAID_DAGRUN_LIMIT_BYTES = 8_000_000
PAID_URL_LIMIT_BYTES = 2_000_000
PAID_LEDGER_PATH = os.environ.get(
    "PROXY_FILTER_LEDGER_PATH",
    "/opt/airflow/logs/proxy_filter/paid_requests.jsonl",
)
MAX_LEDGER_EVENT_BYTES = 256 * 1024
EXPECTED_FEEDS_PER_STAGE = 68
SCOPE_PARITY_TABLES = (
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
)

# Two slots bound direct-only scope fan-out without serialising hundreds of
# competitions.  Match/scope commit locks protect idempotency across task
# processes; the paid route still has its independent hard lease limit of one.
DIRECT_POOL = os.environ.get("WHOSCORED_DIRECT_POOL", "whoscored_direct_pool")

WHOSCORED_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key not in {"pool", "retries", "retry_delay", "execution_timeout"}
    },
    "retries": 0,
    "execution_timeout": timedelta(minutes=20),
}

_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_SCHEMA_READY": "1",
    "WHOSCORED_REQUEST_LEDGER_PATH": (
        RUN_ROOT
        + "/{{ dag.dag_id }}/{{ run_id | replace(':', '_') | replace('+', '_') }}"
        + "/requests_{{ task.task_id | replace('.', '_') }}_"
        + "{{ ti.map_index }}_try{{ ti.try_number }}.jsonl"
    ),
}


def _safe_token(value: str) -> str:
    rendered = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return rendered[:120] or "unknown"


def _run_dir_from_context(context: Mapping[str, Any]) -> Path:
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown"
    run_id = context.get("run_id")
    if not run_id and context.get("dag_run") is not None:
        run_id = context["dag_run"].run_id
    return Path(RUN_ROOT) / _safe_token(str(dag_id)) / _safe_token(str(run_id))


def _scope_result_path(run_dir: Path, scope_spec: str) -> Path:
    digest = hashlib.sha256(scope_spec.encode("utf-8")).hexdigest()[:12]
    return run_dir / f"scope_{_safe_token(scope_spec)[:70]}_{digest}.json"


def _active_scope_specs() -> list[str]:
    # Lazy import keeps DAG parsing free of Trino/PyArrow dependencies.
    from dags.scripts.run_whoscored_scraper import resolve_daily_scope_specs

    scopes = resolve_daily_scope_specs()
    if not scopes:
        raise AirflowException("persisted WhoScored catalog has no active scopes")
    if len(scopes) != len(set(scopes)):
        raise AirflowException("persisted WhoScored catalog returned duplicate scopes")
    return sorted(scopes)


def initialize_whoscored_schema() -> Dict[str, Any]:
    """Run additive schema evolution once before any dynamically mapped writer."""
    from scrapers.whoscored.repository import WhoScoredRepository

    WhoScoredRepository().ensure_schema(create_views=True)
    return {"status": "success"}


def build_daily_commands(**context: Any) -> list[str]:
    """Return one safely quoted CLI command per active persisted scope."""
    run_dir = _run_dir_from_context(context)
    direct_only = bool(context.get("params", {}).get("direct_only", False))
    direct_flag = " --direct-only" if direct_only else ""
    commands: list[str] = []
    for scope in _active_scope_specs():
        output = _scope_result_path(run_dir, scope)
        commands.append(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py daily "
            f"--scope {shlex.quote(scope)} --skip-profiles "
            f"--output {shlex.quote(str(output))}{direct_flag}"
        )
    return commands


def build_scope_validation_kwargs(**context: Any) -> list[dict[str, Any]]:
    run_dir = _run_dir_from_context(context)
    require_zero = bool(context.get("params", {}).get("require_zero_paid", True))
    return [
        {
            "scope_spec": scope,
            "result_path": str(_scope_result_path(run_dir, scope)),
            "require_zero_paid": require_zero,
        }
        for scope in _active_scope_specs()
    ]


def _load_result(path: str) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(
            f"WhoScored result {path} is unavailable: {exc}"
        ) from exc
    if not isinstance(value, dict) or value.get("schema_version") != 3:
        raise AirflowException(f"WhoScored result {path} is not report schema v3")
    return value


def _canonical_traffic_url(value: Any) -> str:
    """Keep the full URL while making equivalent query order deterministic."""
    raw = str(value or "").strip()
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        return raw.split("#", 1)[0]
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            query,
            "",
        )
    )


def _iter_jsonl_events(path: Path, *, label: str):
    """Stream bounded JSONL records and fail closed on truncation/corruption."""
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise AirflowException(f"cannot read {label} {path}: {exc}") from exc
    with handle:
        line_number = 0
        while True:
            raw = handle.readline(MAX_LEDGER_EVENT_BYTES + 1)
            if not raw:
                return
            line_number += 1
            if len(raw) > MAX_LEDGER_EVENT_BYTES:
                raise AirflowException(
                    f"oversized {label} {path}:{line_number}: "
                    f"limit is {MAX_LEDGER_EVENT_BYTES} bytes"
                )
            try:
                event = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise AirflowException(
                    f"corrupt {label} {path}:{line_number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise AirflowException(
                    f"corrupt {label} {path}:{line_number}: JSON object required"
                )
            yield event


def _counter_add(target: dict[str, int], key: str, value: int) -> None:
    target[key] = target.get(key, 0) + max(0, int(value))


def _traffic_task_key(event: Mapping[str, Any], *, include_try: bool = False) -> str:
    task_id = str(event.get("task_id") or "unknown")
    try:
        map_index = int(event.get("map_index", -1))
    except (TypeError, ValueError):
        map_index = -1
    key = f"{task_id}[{map_index}]"
    if include_try:
        try:
            try_number = int(event.get("try_number", 0))
        except (TypeError, ValueError):
            try_number = 0
        key += f"/try{try_number}"
    return key


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _expected_feed_state_keys(stage_ids: Sequence[int]) -> frozenset[str]:
    """Build the exact source-feed contract from the current parser catalogs."""
    # Lazy imports preserve the DAG's parse-time isolation from scraper runtime
    # dependencies while keeping DQ aligned with the fetch catalogs.
    from scrapers.whoscored.service import (
        PLAYER_DETAILED_STAT_TABS,
        PLAYER_STAGE_STAT_TABS,
        TEAM_DETAILED_STAT_TABS,
        TEAM_STAGE_STAT_TABS,
    )
    from scrapers.whoscored.stage_feeds import STAGE_TEAM_FEED_CATALOG

    suffixes = {
        *(
            f"team:{category}:{subcategory}"
            for category, subcategory, *_ in TEAM_STAGE_STAT_TABS
        ),
        *(
            f"team-detailed:{spec.category}:{spec.subcategory}"
            for spec in TEAM_DETAILED_STAT_TABS
        ),
        *(
            f"player:{category}:{subcategory}"
            for category, subcategory, _inc_pens in PLAYER_STAGE_STAT_TABS
        ),
        *(
            f"player-detailed:{spec.category}:{spec.subcategory}"
            for spec in PLAYER_DETAILED_STAT_TABS
        ),
        *(f"stagestatfeed:{spec.type_id}" for spec in STAGE_TEAM_FEED_CATALOG),
        "referee:summary",
    }
    if len(suffixes) != EXPECTED_FEEDS_PER_STAGE:
        raise AirflowException(
            "WhoScored source feed catalog drifted without a DQ contract update: "
            f"expected {EXPECTED_FEEDS_PER_STAGE}, got {len(suffixes)}"
        )

    normalized_stages: set[int] = set()
    for raw_stage_id in stage_ids:
        try:
            stage_id = int(raw_stage_id)
        except (TypeError, ValueError) as exc:
            raise AirflowException(
                f"WhoScored catalog contains invalid stage id {raw_stage_id!r}"
            ) from exc
        if stage_id <= 0:
            raise AirflowException(
                f"WhoScored catalog contains invalid stage id {raw_stage_id!r}"
            )
        normalized_stages.add(stage_id)
    return frozenset(
        f"{stage_id}:{suffix}" for stage_id in normalized_stages for suffix in suffixes
    )


def _strict_json_object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key {key!r}")
        value[key] = item
    return value


def _feed_state_integrity_summary(
    dataset_states_json: Any,
    stage_ids: Sequence[int],
) -> Dict[str, int]:
    """Compare the manifest feed map with the exact per-stage source contract."""
    expected = _expected_feed_state_keys(stage_ids)
    stage_count = len(expected) // EXPECTED_FEEDS_PER_STAGE
    feeds: Mapping[str, Any] = {}
    malformed = 0
    if not isinstance(dataset_states_json, str) or not dataset_states_json.strip():
        malformed = 1
    else:
        try:
            states = json.loads(
                dataset_states_json,
                object_pairs_hook=_strict_json_object_pairs,
            )
        except (TypeError, ValueError):
            states = None
            malformed = 1
        if isinstance(states, Mapping):
            candidate = states.get("__feeds__")
            if isinstance(candidate, Mapping):
                feeds = candidate
            else:
                malformed = 1
        elif states is not None:
            malformed = 1

    allowed_statuses = {"available", "empty", "not_available"}
    malformed += sum(
        not isinstance(key, str)
        or not isinstance(status, str)
        or status not in allowed_statuses
        for key, status in feeds.items()
    )
    actual = frozenset(key for key in feeds if isinstance(key, str))
    return {
        "feed_state_stage_count": stage_count,
        "expected_feed_state_count": len(expected),
        "actual_feed_state_count": len(actual),
        "missing_feed_state_count": len(expected - actual),
        "extra_feed_state_count": len(actual - expected),
        "malformed_feed_state_count": int(malformed),
        "unavailable_feed_count": sum(
            key in expected and status == "not_available"
            for key, status in feeds.items()
        ),
    }


def _scope_integrity_summary(scope_spec: str) -> Dict[str, int]:
    competition_id, separator, season_id = scope_spec.rpartition("=")
    if not separator or not competition_id or not season_id:
        raise AirflowException(f"invalid persisted scope {scope_spec!r}")

    from utils.silver_tasks import _get_trino_connection

    league_sql = _sql_string(competition_id)
    season_sql = _sql_string(season_id)
    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH latest_match AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_match_ingest_manifest m
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                    ) WHERE rn = 1 AND state = 'success'
                ),
                schedule AS (
                    SELECT * FROM (
                        SELECT s.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_schedule_current s
                        WHERE league = {league_sql} AND season = {season_sql}
                    ) WHERE rn = 1
                ),
                completed AS (
                    SELECT * FROM schedule
                    WHERE status = 6 OR (
                        status = 1 AND home_score IS NOT NULL
                        AND away_score IS NOT NULL
                        AND date <= CAST(
                            CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                        )
                    )
                ),
                events_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count,
                           MAX(COALESCE(expanded_minute, minute)) AS max_minute
                    FROM iceberg.bronze.whoscored_events_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                lineups_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_lineups_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                incidents_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_match_incidents_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                bets_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_match_bets_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                schedule_counts AS (
                    SELECT COUNT(*) AS physical_rows, COUNT(DISTINCT game_id) AS games
                    FROM schedule
                )
                SELECT
                    (SELECT physical_rows FROM schedule_counts),
                    (SELECT games FROM schedule_counts),
                    (SELECT COUNT(*) FROM latest_match),
                    (SELECT COALESCE(SUM(events_count), 0) FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_events_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(lineups_count), 0) FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_lineups_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COUNT(*) FROM completed),
                    (SELECT COUNT(*) FROM completed c LEFT JOIN latest_match m
                     ON m.game_id = CAST(c.game_id AS BIGINT) WHERE m.game_id IS NULL),
                    (SELECT COUNT(*) FROM latest_match m
                     LEFT JOIN events_by_game e ON e.game_id = m.game_id
                     WHERE COALESCE(e.rows_count, 0) <> m.events_count),
                    (SELECT COUNT(*) FROM latest_match m
                     LEFT JOIN lineups_by_game l ON l.game_id = m.game_id
                     WHERE COALESCE(l.rows_count, 0) <> m.lineups_count),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.matches') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_matches_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.substitutions') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_substitutions_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.formations') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_formations_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.team_match_stats') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_team_match_stats_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.player_match_stats') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_player_match_stats_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COUNT(*)
                     FROM completed c
                     JOIN latest_match m
                       ON m.game_id = CAST(c.game_id AS BIGINT)
                     LEFT JOIN events_by_game e ON e.game_id = m.game_id
                     LEFT JOIN iceberg.bronze.whoscored_matches_current h
                       ON h.league = m.league AND h.season = m.season
                      AND CAST(h.game_id AS BIGINT) = m.game_id
                     WHERE m.is_opta = TRUE AND (
                         COALESCE(json_extract_scalar(
                             json_parse(m.dataset_statuses_json), '$.events'
                         ), '') <> 'available'
                         OR COALESCE(e.rows_count, 0) < GREATEST(
                             20, COALESCE(h.expanded_max_minute, 90)
                         )
                         OR COALESCE(e.max_minute, -1) < GREATEST(
                             1, COALESCE(h.expanded_max_minute, 90) - 15
                         )
                     )),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN incidents_by_game i
                       ON i.game_id = CAST(s.game_id AS BIGINT)
                    WHERE s.has_incidents_summary = TRUE
                       AND COALESCE(i.rows_count, 0) = 0),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN bets_by_game b
                       ON b.game_id = CAST(s.game_id AS BIGINT)
                     WHERE COALESCE(
                         json_size(json_parse(s.bets), '$'), 0
                     ) > 0 AND COALESCE(b.rows_count, 0) = 0),
                    (SELECT COUNT(*) FROM latest_match
                     WHERE dataset_statuses_json IS NULL
                        OR COALESCE(json_size(
                            json_parse(dataset_statuses_json), '$'
                        ), -1) <> 7
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.matches'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.events'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.lineups'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.substitutions'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.formations'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.team_match_stats'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.player_match_stats'
                        ), '') NOT IN ('available', 'empty', 'not_available')),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_events_current e
                     WHERE e.league = {league_sql} AND e.season = {season_sql}
                       AND e._game_batch_id LIKE 'ws2-%'
                       AND (
                           e.source_event_id IS NULL OR e.source_event_id <= 0
                           OR e.team_event_id IS NULL OR e.team_event_id <= 0
                           OR TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.id'
                           )) AS BIGINT) IS DISTINCT FROM e.source_event_id
                           OR TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.eventId'
                           )) AS BIGINT) IS DISTINCT FROM e.team_event_id
                           OR TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.relatedEventId'
                           )) AS BIGINT) IS DISTINCT FROM e.related_team_event_id
                       )),
                    (SELECT COUNT(*) FROM (
                        SELECT game_id, source_event_id
                        FROM iceberg.bronze.whoscored_events_current
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND _game_batch_id LIKE 'ws2-%'
                        GROUP BY 1, 2 HAVING COUNT(*) > 1
                    ) duplicate_source_ids),
                    (SELECT COUNT(*) FROM (
                        SELECT game_id, team_id, team_event_id
                        FROM iceberg.bronze.whoscored_events_current
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND _game_batch_id LIKE 'ws2-%'
                        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
                    ) duplicate_team_event_ids)
                """
            )
            row = cur.fetchall()[0]
            count_sql = ",\n".join(
                f"(SELECT COUNT(*) FROM iceberg.bronze.{table}_current "
                f"WHERE league = {league_sql} AND season = {season_sql})"
                for table in SCOPE_PARITY_TABLES
            )
            cur.execute(
                f"""
                WITH latest_scope AS (
                    SELECT entity_counts_json, dataset_states_json
                    FROM iceberg.bronze.whoscored_scope_ingest_manifest
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND entity_group = 'season' AND state = 'success'
                    ORDER BY completed_at DESC, _ingested_at DESC LIMIT 1
                ),
                scope_stages AS (
                    SELECT stage_id
                    FROM iceberg.bronze.whoscored_stages_current
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND stage_id IS NOT NULL
                    UNION
                    SELECT stage_id
                    FROM iceberg.bronze.whoscored_schedule_current
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND stage_id IS NOT NULL
                )
                SELECT (SELECT entity_counts_json FROM latest_scope),
                       (SELECT dataset_states_json FROM latest_scope),
                       (SELECT ARRAY_AGG(DISTINCT CAST(stage_id AS BIGINT))
                        FROM scope_stages),
                       {count_sql}
                """
            )
            scope_row = cur.fetchall()[0]
            scope_expected = json.loads(scope_row[0]) if scope_row[0] else {}
            scope_manifest_mismatches = sum(
                int(scope_expected.get(table, -1)) != int(actual or 0)
                for table, actual in zip(SCOPE_PARITY_TABLES, scope_row[3:])
            )
            feed_integrity = _feed_state_integrity_summary(
                scope_row[1], scope_row[2] or ()
            )
            cur.execute(
                f"""
                WITH schedule AS (
                    SELECT * FROM (
                        SELECT s.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_schedule_current s
                        WHERE league = {league_sql} AND season = {season_sql}
                    ) WHERE rn = 1
                ),
                latest_preview AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_preview_ingest_manifest m
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND state = 'success' AND batch_id LIKE 'wsp2-%'
                          AND raw_uri IS NOT NULL
                    ) WHERE rn = 1
                )
                SELECT
                    (SELECT COUNT(*) FROM schedule
                     WHERE has_preview = TRUE
                       AND date <= CAST(
                           CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                       )),
                    (SELECT COUNT(*) FROM latest_preview),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN latest_preview p
                       ON p.game_id = CAST(s.game_id AS BIGINT)
                     WHERE s.has_preview = TRUE
                       AND s.date <= CAST(
                           CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                       ) AND p.game_id IS NULL),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.missing_players'
                    ) AS BIGINT)), 0) FROM latest_preview),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_missing_players_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.preview_lineups'
                    ) AS BIGINT)), 0) FROM latest_preview),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_preview_lineups_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.preview_sections'
                    ) AS BIGINT)), 0) FROM latest_preview),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_preview_sections_current
                     WHERE league = {league_sql} AND season = {season_sql}),
                    (SELECT COUNT(*) FROM latest_preview
                     WHERE dataset_statuses_json IS NULL
                        OR COALESCE(json_size(
                            json_parse(dataset_statuses_json), '$'
                        ), -1) <> 3
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.missing_players'
                        ), 'not_available') = 'not_available'
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.preview_lineups'
                        ), 'not_available') = 'not_available'
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.preview_sections'
                        ), 'not_available') = 'not_available')
                """
            )
            preview_row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "schedule_rows": int(row[0] or 0),
        "schedule_games": int(row[1] or 0),
        "successful_matches": int(row[2] or 0),
        "manifest_event_rows": int(row[3] or 0),
        "current_event_rows": int(row[4] or 0),
        "manifest_lineup_rows": int(row[5] or 0),
        "current_lineup_rows": int(row[6] or 0),
        "completed_games": int(row[7] or 0),
        "uncovered_completed_games": int(row[8] or 0),
        "event_game_mismatches": int(row[9] or 0),
        "lineup_game_mismatches": int(row[10] or 0),
        "manifest_match_rows": int(row[11] or 0),
        "current_match_rows": int(row[12] or 0),
        "manifest_substitution_rows": int(row[13] or 0),
        "current_substitution_rows": int(row[14] or 0),
        "manifest_formation_rows": int(row[15] or 0),
        "current_formation_rows": int(row[16] or 0),
        "manifest_team_stat_rows": int(row[17] or 0),
        "current_team_stat_rows": int(row[18] or 0),
        "manifest_player_stat_rows": int(row[19] or 0),
        "current_player_stat_rows": int(row[20] or 0),
        "incomplete_final_opta_games": int(row[21] or 0),
        "uncovered_incident_summaries": int(row[22] or 0),
        "uncovered_bet_matches": int(row[23] or 0),
        "incomplete_match_snapshots": int(row[24] or 0),
        "invalid_event_identity_rows": int(row[25] or 0),
        "duplicate_source_event_ids": int(row[26] or 0),
        "duplicate_team_event_ids": int(row[27] or 0),
        "scope_manifest_mismatches": scope_manifest_mismatches,
        **feed_integrity,
        "required_previews": int(preview_row[0] or 0),
        "successful_previews": int(preview_row[1] or 0),
        "uncovered_previews": int(preview_row[2] or 0),
        "manifest_missing_player_rows": int(preview_row[3] or 0),
        "current_missing_player_rows": int(preview_row[4] or 0),
        "manifest_preview_lineup_rows": int(preview_row[5] or 0),
        "current_preview_lineup_rows": int(preview_row[6] or 0),
        "manifest_preview_section_rows": int(preview_row[7] or 0),
        "current_preview_section_rows": int(preview_row[8] or 0),
        "incomplete_preview_snapshots": int(preview_row[9] or 0),
    }


def _profile_integrity_summary(scope_specs: Sequence[str]) -> Dict[str, int]:
    """Validate manifest/physical parity for the complete active roster."""

    if not scope_specs:
        return {
            "roster_players": 0,
            "current_profile_manifests": 0,
            "current_profile_rows": 0,
            "uncovered_profiles": 0,
            "stale_profiles": 0,
            "manifest_participation_rows": 0,
            "current_participation_rows": 0,
        }
    filters = []
    for spec in scope_specs:
        league, separator, season = spec.rpartition("=")
        if not separator or not league or not season:
            raise AirflowException(f"invalid persisted scope {spec!r}")
        filters.append(
            f"(league = {_sql_string(league)} AND season = {_sql_string(season)})"
        )

    from scrapers.whoscored.parsers import PARSER_VERSION
    from scrapers.whoscored.repository import PROFILE_REFRESH_DAYS
    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH roster AS (
                    SELECT DISTINCT CAST(player_id AS BIGINT) AS player_id
                    FROM iceberg.bronze.whoscored_player_roster
                    WHERE ({" OR ".join(filters)}) AND player_id IS NOT NULL
                ),
                latest_success AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY player_id
                            ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, _profile_batch_id DESC, _batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                        WHERE state = 'success'
                          AND _profile_batch_id LIKE 'wspr2-%'
                          AND raw_uri IS NOT NULL
                          AND payload_sha256 IS NOT NULL
                          AND parser_version = {_sql_string(PARSER_VERSION)}
                    ) WHERE rn = 1
                ),
                physical_profiles AS (
                    SELECT p.player_id
                    FROM iceberg.bronze.whoscored_player_profile_versions p
                    JOIN latest_success m
                      ON m.player_id = p.player_id
                     AND m._profile_batch_id = p._profile_batch_id
                     AND m.payload_sha256 = p.payload_sha256
                     AND m.parser_version = p.parser_version
                ),
                physical_participations AS (
                    SELECT p.player_id
                    FROM iceberg.bronze.whoscored_player_stage_participations_current p
                    JOIN roster r ON r.player_id = p.player_id
                )
                SELECT
                    (SELECT COUNT(*) FROM roster),
                    (SELECT COUNT(*) FROM latest_success m
                     JOIN roster r ON r.player_id = m.player_id),
                    (SELECT COUNT(*) FROM physical_profiles p
                     JOIN roster r ON r.player_id = p.player_id),
                    (SELECT COUNT(*) FROM roster r
                     LEFT JOIN latest_success m ON m.player_id = r.player_id
                     WHERE m.player_id IS NULL),
                    (SELECT COUNT(*) FROM latest_success m
                     JOIN roster r ON r.player_id = m.player_id
                     WHERE COALESCE(
                         m.fetched_at, TIMESTAMP '1970-01-01 00:00:00'
                     ) <= CAST(
                         CURRENT_TIMESTAMP - INTERVAL '{PROFILE_REFRESH_DAYS}' DAY
                         AS TIMESTAMP
                     )),
                    (SELECT COALESCE(SUM(m.participations_count), 0)
                     FROM latest_success m
                     JOIN roster r ON r.player_id = m.player_id),
                    (SELECT COUNT(*) FROM physical_participations)
                """
            )
            row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "roster_players": int(row[0] or 0),
        "current_profile_manifests": int(row[1] or 0),
        "current_profile_rows": int(row[2] or 0),
        "uncovered_profiles": int(row[3] or 0),
        "stale_profiles": int(row[4] or 0),
        "manifest_participation_rows": int(row[5] or 0),
        "current_participation_rows": int(row[6] or 0),
    }


def _catalog_integrity_summary() -> Dict[str, int]:
    """Check that the current catalog is complete and physically atomic."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                    SELECT * FROM iceberg.bronze.whoscored_catalog_manifest
                    WHERE state = 'success'
                    ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC
                    LIMIT 1
                ),
                competitions AS (
                    SELECT c.* FROM iceberg.bronze.whoscored_competitions c
                    JOIN latest m ON m.batch_id = c._catalog_batch_id
                ),
                seasons AS (
                    SELECT s.* FROM iceberg.bronze.whoscored_seasons s
                    JOIN latest m ON m.batch_id = s._catalog_batch_id
                ),
                stages AS (
                    SELECT s.* FROM iceberg.bronze.whoscored_stages s
                    JOIN latest m ON m.batch_id = s._catalog_batch_id
                )
                SELECT
                    (SELECT competitions_count FROM latest),
                    (SELECT COUNT(*) FROM competitions),
                    (SELECT COUNT(DISTINCT record_key) FROM competitions),
                    (SELECT seasons_count FROM latest),
                    (SELECT COUNT(*) FROM seasons),
                    (SELECT COUNT(DISTINCT record_key) FROM seasons),
                    (SELECT stages_count FROM latest),
                    (SELECT COUNT(*) FROM stages),
                    (SELECT COUNT(DISTINCT record_key) FROM stages),
                    (SELECT quarantined_count FROM latest),
                    (SELECT COUNT(*) FROM (
                        SELECT s.competition_id, s.source_season_id
                        FROM seasons s
                        JOIN competitions c
                          ON c.competition_id = s.competition_id
                        LEFT JOIN stages g
                          ON g.competition_id = s.competition_id
                         AND g.source_season_id = s.source_season_id
                        WHERE c.eligibility = 'included'
                          AND s.eligibility = 'included'
                        GROUP BY s.competition_id, s.source_season_id
                        HAVING COUNT(g.stage_id) = 0
                    ))
                """
            )
            row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "manifest_competitions": int(row[0] or 0),
        "physical_competitions": int(row[1] or 0),
        "distinct_competitions": int(row[2] or 0),
        "manifest_seasons": int(row[3] or 0),
        "physical_seasons": int(row[4] or 0),
        "distinct_seasons": int(row[5] or 0),
        "manifest_stages": int(row[6] or 0),
        "physical_stages": int(row[7] or 0),
        "distinct_stages": int(row[8] or 0),
        "quarantined": int(row[9] or 0),
        "eligible_seasons_without_stages": int(row[10] or 0),
    }


def validate_scope_result(
    *,
    scope_spec: str,
    result_path: str,
    require_zero_paid: bool = True,
    **_context: Any,
) -> Dict[str, Any]:
    """Validate this run artifact and logical Bronze parity for one scope."""
    result = _load_result(result_path)
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored producer failed for {scope_spec}: "
            f"status={result.get('status')} errors={result.get('errors', [])}"
        )
    reported = {item.get("scope") for item in result.get("scopes", [])}
    if reported != {scope_spec}:
        raise AirflowException(
            f"WhoScored report scope mismatch: expected {scope_spec}, got {reported}"
        )
    paid = int(result.get("paid_proxy_bytes") or 0)
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(f"paid proxy budget exceeded: {paid} bytes")
    if require_zero_paid and paid:
        raise AirflowException(
            f"normal daily run used paid proxy for {scope_spec}: {paid} bytes"
        )

    integrity = _scope_integrity_summary(scope_spec)
    if integrity["schedule_rows"] < 1:
        raise AirflowException(f"WhoScored schedule is empty for {scope_spec}")
    if integrity["schedule_rows"] != integrity["schedule_games"]:
        raise AirflowException(
            f"WhoScored schedule duplicates for {scope_spec}: {integrity}"
        )
    if integrity["manifest_event_rows"] != integrity["current_event_rows"]:
        raise AirflowException(
            f"WhoScored event manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["manifest_lineup_rows"] != integrity["current_lineup_rows"]:
        raise AirflowException(
            f"WhoScored lineup manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["uncovered_completed_games"]:
        raise AirflowException(
            f"WhoScored completed-match coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["event_game_mismatches"] or integrity["lineup_game_mismatches"]:
        raise AirflowException(
            f"WhoScored per-game manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["incomplete_final_opta_games"]:
        raise AirflowException(
            f"WhoScored final Opta event completeness failed for {scope_spec}: "
            f"{integrity}"
        )
    if integrity["uncovered_incident_summaries"]:
        raise AirflowException(
            f"WhoScored schedule incident coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["uncovered_bet_matches"]:
        raise AirflowException(
            f"WhoScored schedule bet coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["incomplete_match_snapshots"]:
        raise AirflowException(
            f"WhoScored match dataset-state contract failed for {scope_spec}: "
            f"{integrity}"
        )
    if (
        integrity["invalid_event_identity_rows"]
        or integrity["duplicate_source_event_ids"]
        or integrity["duplicate_team_event_ids"]
    ):
        raise AirflowException(
            f"WhoScored event identity contract failed for {scope_spec}: {integrity}"
        )
    for manifest_key, current_key in (
        ("manifest_match_rows", "current_match_rows"),
        ("manifest_substitution_rows", "current_substitution_rows"),
        ("manifest_formation_rows", "current_formation_rows"),
        ("manifest_team_stat_rows", "current_team_stat_rows"),
        ("manifest_player_stat_rows", "current_player_stat_rows"),
    ):
        if integrity[manifest_key] != integrity[current_key]:
            raise AirflowException(
                f"WhoScored dataset manifest parity failed for {scope_spec}: {integrity}"
            )
    if integrity["scope_manifest_mismatches"]:
        raise AirflowException(
            f"WhoScored scope manifest parity failed for {scope_spec}: {integrity}"
        )
    if (
        integrity["feed_state_stage_count"] < 1
        or integrity["missing_feed_state_count"]
        or integrity["extra_feed_state_count"]
        or integrity["malformed_feed_state_count"]
    ):
        raise AirflowException(
            f"WhoScored source feed manifest completeness failed for {scope_spec}: "
            f"{integrity}"
        )
    if integrity["uncovered_previews"] or integrity["incomplete_preview_snapshots"]:
        raise AirflowException(
            f"WhoScored preview coverage failed for {scope_spec}: {integrity}"
        )
    for manifest_key, current_key in (
        ("manifest_missing_player_rows", "current_missing_player_rows"),
        ("manifest_preview_lineup_rows", "current_preview_lineup_rows"),
        ("manifest_preview_section_rows", "current_preview_section_rows"),
    ):
        if integrity[manifest_key] != integrity[current_key]:
            raise AirflowException(
                f"WhoScored preview manifest parity failed for {scope_spec}: "
                f"{integrity}"
            )
    return {"scope": scope_spec, "paid_proxy_bytes": paid, **integrity}


def validate_profile_result(**context: Any) -> Dict[str, Any]:
    path = _run_dir_from_context(context) / "profiles.json"
    result = _load_result(str(path))
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored profile refresh failed: {result.get('errors', [])}"
        )
    paid = int(result.get("paid_proxy_bytes") or 0)
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(f"profile paid budget exceeded: {paid} bytes")
    if context.get("params", {}).get("require_zero_paid", True) and paid:
        raise AirflowException(f"normal profile refresh used paid proxy: {paid} bytes")
    integrity = _profile_integrity_summary(_active_scope_specs())
    if integrity["uncovered_profiles"] or integrity["stale_profiles"]:
        raise AirflowException(
            f"WhoScored active profile coverage is incomplete: {integrity}"
        )
    if (
        integrity["current_profile_manifests"] != integrity["current_profile_rows"]
        or integrity["manifest_participation_rows"]
        != integrity["current_participation_rows"]
    ):
        raise AirflowException(f"WhoScored profile manifest parity failed: {integrity}")
    return {"status": "success", "paid_proxy_bytes": paid, **integrity}


def validate_catalog_result(**context: Any) -> Dict[str, Any]:
    path = _run_dir_from_context(context) / "discovery.json"
    result = _load_result(str(path))
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored discovery failed: {result.get('errors', [])}"
        )
    integrity = _catalog_integrity_summary()
    if integrity["manifest_competitions"] < 100:
        raise AirflowException(f"WhoScored catalog is implausibly small: {integrity}")
    for manifest_key, physical_key, distinct_key in (
        ("manifest_competitions", "physical_competitions", "distinct_competitions"),
        ("manifest_seasons", "physical_seasons", "distinct_seasons"),
        ("manifest_stages", "physical_stages", "distinct_stages"),
    ):
        if not (
            integrity[manifest_key]
            == integrity[physical_key]
            == integrity[distinct_key]
        ):
            raise AirflowException(
                f"WhoScored catalog manifest parity failed: {integrity}"
            )
    if integrity["quarantined"] or integrity["eligible_seasons_without_stages"]:
        raise AirflowException(f"WhoScored catalog is incomplete: {integrity}")
    scopes = _active_scope_specs()
    if not scopes:
        raise AirflowException("WhoScored catalog has no active adult men's scopes")
    return {"status": "success", "active_scopes": len(scopes), **integrity}


def aggregate_traffic_reports(**context: Any) -> Dict[str, Any]:
    """Stream all retries and enforce exact DagRun/per-URL paid ceilings."""
    run_dir = _run_dir_from_context(context)
    reports = []
    for path in sorted(run_dir.glob("*.json")):
        try:
            result = _load_result(str(path))
        except AirflowException:
            continue
        reports.append((path.name, result))
    if not reports:
        raise AirflowException(f"no WhoScored reports found in {run_dir}")

    reported_paid = 0
    reported_paid_by_url: dict[str, int] = {}
    reported_paid_by_task: dict[str, int] = {}
    reported_paid_by_task_try: dict[str, int] = {}
    for name, result in reports:
        try:
            report_paid = max(0, int(result.get("paid_proxy_bytes") or 0))
        except (TypeError, ValueError) as exc:
            raise AirflowException(
                f"invalid paid_proxy_bytes in WhoScored report {name}"
            ) from exc
        reported_paid += report_paid
        airflow_identity = result.get("airflow")
        if not isinstance(airflow_identity, Mapping):
            airflow_identity = {}
        _counter_add(
            reported_paid_by_task,
            _traffic_task_key(airflow_identity),
            report_paid,
        )
        _counter_add(
            reported_paid_by_task_try,
            _traffic_task_key(airflow_identity, include_try=True),
            report_paid,
        )
        traffic = result.get("traffic")
        paid_urls = (
            traffic.get("paid_proxy_bytes_by_url")
            if isinstance(traffic, Mapping)
            else None
        )
        if isinstance(paid_urls, Mapping):
            for raw_url, raw_count in paid_urls.items():
                url = _canonical_traffic_url(raw_url)
                try:
                    count = max(0, int(raw_count or 0))
                except (TypeError, ValueError) as exc:
                    raise AirflowException(
                        f"invalid paid URL bytes in WhoScored report {name}"
                    ) from exc
                if url and count:
                    _counter_add(reported_paid_by_url, url, count)

    route_requests: dict[str, int] = {}
    route_bytes: dict[str, int] = {}
    requests_by_url: dict[str, int] = {}
    bytes_by_url: dict[str, int] = {}
    requests_by_task: dict[str, int] = {}
    bytes_by_task: dict[str, int] = {}
    request_ledger_paid = 0
    request_ledger_paid_by_url: dict[str, int] = {}
    request_ledger_paid_by_task: dict[str, int] = {}
    request_ledger_paid_by_task_try: dict[str, int] = {}
    for path in sorted(run_dir.glob("requests_*.jsonl")):
        for event in _iter_jsonl_events(path, label="request ledger"):
            try:
                request_bytes = max(0, int(event.get("request_bytes") or 0))
                response_bytes = max(0, int(event.get("response_bytes") or 0))
                accounted_paid = max(0, int(event.get("paid_proxy_bytes") or 0))
            except (TypeError, ValueError) as exc:
                raise AirflowException(
                    f"invalid byte counter in request ledger {path}"
                ) from exc
            url = _canonical_traffic_url(event.get("url")) or "unknown"
            task_key = _traffic_task_key(event)
            task_try_key = _traffic_task_key(event, include_try=True)
            # Lease accounting is a byte-delta summary for the same logical
            # fetch, not a second HTTP request. Keep it as an independent
            # retry-safe paid source and exclude it from request counters.
            if str(event.get("status") or "") == "accounted":
                request_ledger_paid += accounted_paid
                _counter_add(request_ledger_paid_by_url, url, accounted_paid)
                _counter_add(request_ledger_paid_by_task, task_key, accounted_paid)
                _counter_add(
                    request_ledger_paid_by_task_try,
                    task_try_key,
                    accounted_paid,
                )
                continue
            route = str(event.get("route") or "unknown")
            transferred = request_bytes + response_bytes
            _counter_add(route_requests, route, 1)
            _counter_add(route_bytes, route, transferred)
            _counter_add(requests_by_url, url, 1)
            _counter_add(bytes_by_url, url, transferred)
            _counter_add(requests_by_task, task_key, 1)
            _counter_add(bytes_by_task, task_key, transferred)

    dag_id = getattr(context.get("dag"), "dag_id", None) or context.get("dag_id")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    durable_paid = 0
    durable_paid_by_url: dict[str, int] = {}
    durable_paid_by_task: dict[str, int] = {}
    durable_paid_by_task_try: dict[str, int] = {}
    paid_ledger = Path(PAID_LEDGER_PATH)
    if paid_ledger.exists():
        for event in _iter_jsonl_events(paid_ledger, label="paid ledger"):
            if (
                event.get("event_type") == "bytes"
                and str(event.get("dag_id") or "") == str(dag_id or "")
                and str(event.get("run_id") or "") == str(run_id or "")
            ):
                try:
                    count = int(event.get("bytes") or 0)
                except (TypeError, ValueError) as exc:
                    raise AirflowException(
                        f"invalid byte counter in paid ledger {paid_ledger}"
                    ) from exc
                if count <= 0:
                    raise AirflowException(
                        f"invalid non-positive byte event in paid ledger {paid_ledger}"
                    )
                url = _canonical_traffic_url(event.get("canonical_url"))
                if not url:
                    raise AirflowException(
                        f"paid ledger event for {dag_id}/{run_id} has no canonical URL"
                    )
                durable_paid += count
                _counter_add(durable_paid_by_url, url, count)
                _counter_add(
                    durable_paid_by_task,
                    _traffic_task_key(event),
                    count,
                )
                _counter_add(
                    durable_paid_by_task_try,
                    _traffic_task_key(event, include_try=True),
                    count,
                )

    paid = max(reported_paid, request_ledger_paid, durable_paid)
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(
            f"WhoScored DagRun paid proxy budget exceeded: {paid} bytes"
        )
    attribution_sources = (
        (
            durable_paid,
            durable_paid_by_url,
            durable_paid_by_task,
            durable_paid_by_task_try,
        ),
        (
            request_ledger_paid,
            request_ledger_paid_by_url,
            request_ledger_paid_by_task,
            request_ledger_paid_by_task_try,
        ),
        (
            reported_paid,
            reported_paid_by_url,
            reported_paid_by_task,
            reported_paid_by_task_try,
        ),
    )
    paid_by_url: dict[str, int] = {}
    paid_by_task: dict[str, int] = {}
    paid_by_task_try: dict[str, int] = {}
    for (
        source_total,
        source_urls,
        source_tasks,
        source_task_tries,
    ) in attribution_sources:
        if source_total == paid and sum(source_urls.values()) == paid:
            paid_by_url = dict(sorted(source_urls.items()))
            paid_by_task = dict(sorted(source_tasks.items()))
            paid_by_task_try = dict(sorted(source_task_tries.items()))
            break
    if paid and not paid_by_url:
        raise AirflowException(
            "WhoScored paid bytes cannot be attributed exactly to full URLs"
        )
    for url, count in paid_by_url.items():
        if count > PAID_URL_LIMIT_BYTES:
            raise AirflowException(
                f"WhoScored paid proxy URL budget exceeded for {url}: {count} bytes"
            )
    require_zero = bool(context.get("params", {}).get("require_zero_paid", True))
    if require_zero and paid:
        raise AirflowException(f"normal WhoScored DagRun used paid proxy: {paid} bytes")
    return {
        "reports": len(reports),
        "paid_proxy_bytes": paid,
        "paid_proxy_mb": round(paid / 1_000_000, 6),
        "limit_bytes": PAID_DAGRUN_LIMIT_BYTES,
        "per_url_limit_bytes": PAID_URL_LIMIT_BYTES,
        "reported_paid_proxy_bytes": reported_paid,
        "request_ledger_paid_proxy_bytes": request_ledger_paid,
        "durable_paid_proxy_bytes": durable_paid,
        "paid_proxy_bytes_by_url": paid_by_url,
        "paid_proxy_bytes_by_task": paid_by_task,
        "paid_proxy_bytes_by_task_try": paid_by_task_try,
        "route_requests": route_requests,
        "route_bytes": route_bytes,
        "requests_by_url": dict(sorted(requests_by_url.items())),
        "bytes_by_url": dict(sorted(bytes_by_url.items())),
        "requests_by_task": dict(sorted(requests_by_task.items())),
        "bytes_by_task": dict(sorted(bytes_by_task.items())),
    }


def enforce_terminal_gate(**context: Any) -> Dict[str, Any]:
    """Prevent ``all_done`` diagnostics from turning a failed run green."""
    dag_run = context.get("dag_run")
    current_ti = context.get("ti")
    if dag_run is None:
        raise AirflowException("terminal gate requires dag_run context")
    current_task_id = getattr(current_ti, "task_id", "final_success_gate")
    failures: list[str] = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id == current_task_id:
            continue
        state = str(task_instance.state or "none").lower().split(".")[-1]
        if state != "success":
            suffix = (
                f"[{task_instance.map_index}]" if task_instance.map_index >= 0 else ""
            )
            failures.append(f"{task_instance.task_id}{suffix}={state}")
    if failures:
        raise AirflowException(
            "WhoScored upstream/DQ tasks were not successful: " + ", ".join(failures)
        )
    return {
        "status": "success",
        "checked_task_instances": len(dag_run.get_task_instances()) - 1,
    }


_RUN_DIR_TEMPLATE = (
    RUN_ROOT + "/{{ dag.dag_id }}/{{ run_id | replace(':', '_') | replace('+', '_') }}"
)

with DAG(
    dag_id="dag_ingest_whoscored",
    default_args=WHOSCORED_ARGS,
    description="Discover and incrementally ingest all active senior-men WhoScored scopes",
    schedule=SCHEDULES.get("dag_ingest_whoscored", "0 10 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"require_zero_paid": True, "direct_only": True},
    tags=DAG_TAGS.get("whoscored", ["scraping", "whoscored", "bronze"]),
    doc_md="""
    Runtime discovery publishes the complete source catalog. Active eligible
    scopes are then dynamically mapped; a missing/quarantined catalog fails at
    runtime without breaking DAG import. Normal runs require zero paid bytes.
    """,
) as dag:
    discover_catalog = BashOperator(
        task_id="discover_whoscored_catalog",
        bash_command=(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py discover "
            f"--output {_RUN_DIR_TEMPLATE}/discovery.json "
            "{% if params.direct_only %}--direct-only{% endif %}"
        ),
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        retries=2,
        retry_delay=timedelta(minutes=5),
        # Initial deployment automatically performs the only full-history
        # catalog snapshot. Raw-backed retries resume it, but a cold source
        # crawl can legitimately exceed the incremental two-hour envelope.
        execution_timeout=timedelta(hours=8),
    )

    catalog_dq = PythonOperator(
        task_id="validate_whoscored_catalog",
        python_callable=validate_catalog_result,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    initialize_schema = PythonOperator(
        task_id="initialize_whoscored_schema",
        python_callable=initialize_whoscored_schema,
        pool=DIRECT_POOL,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )

    dynamic_mapping_available = callable(getattr(BashOperator, "partial", None))
    if dynamic_mapping_available:
        build_commands = PythonOperator(
            task_id="build_active_scope_commands",
            python_callable=build_daily_commands,
        )
        build_dq = PythonOperator(
            task_id="build_scope_dq_inputs",
            python_callable=build_scope_validation_kwargs,
        )
        ingest_scopes = BashOperator.partial(
            task_id="ingest_active_scope",
            env=_TASK_ENV,
            append_env=True,
            pool=DIRECT_POOL,
            # Entity retry_after timestamps are durable in the manifests.
            # An earlier Airflow retry would see no due candidates and could
            # overwrite the failed report with a false-green empty run.
            retries=0,
            execution_timeout=timedelta(minutes=45),
        ).expand(bash_command=build_commands.output)
        scope_dq = PythonOperator.partial(
            task_id="validate_active_scope",
            python_callable=validate_scope_result,
            trigger_rule="all_done",
            execution_timeout=timedelta(minutes=10),
        ).expand(op_kwargs=build_dq.output)
        catalog_dq >> [build_commands, build_dq]
        build_commands >> ingest_scopes
        [ingest_scopes, build_dq] >> scope_dq
    else:
        # Lightweight host test stubs do not implement dynamic mapping.  This
        # task exercises the same runtime catalog path without resolving any
        # scope while the module is imported.
        ingest_scopes = BashOperator(
            task_id="ingest_active_scopes",
            bash_command=(
                "cd /opt/airflow && "
                "python dags/scripts/run_whoscored_scraper.py daily --skip-profiles "
                f"--output {_RUN_DIR_TEMPLATE}/active_scopes.json "
                "{% if params.direct_only %}--direct-only{% endif %}"
            ),
            env=_TASK_ENV,
            append_env=True,
            pool=DIRECT_POOL,
            retries=0,
            execution_timeout=timedelta(hours=2),
        )
        scope_dq = PythonOperator(
            task_id="validate_active_scopes",
            python_callable=aggregate_traffic_reports,
            trigger_rule="all_done",
        )
        catalog_dq >> ingest_scopes >> scope_dq

    profile_task = BashOperator(
        task_id="refresh_whoscored_profiles",
        bash_command=(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py daily --profiles-only "
            "--profiles-limit 500 "
            f"--output {_RUN_DIR_TEMPLATE}/profiles.json "
            "{% if params.direct_only %}--direct-only{% endif %}"
        ),
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        # Profile retry_after is 24 hours; let the next scheduled DagRun own
        # recovery so a premature retry cannot hide a persisted failure.
        retries=0,
        execution_timeout=timedelta(minutes=30),
        trigger_rule="all_success",
    )
    profile_dq = PythonOperator(
        task_id="validate_profile_refresh",
        python_callable=validate_profile_result,
        trigger_rule="all_done",
    )
    traffic_dq = PythonOperator(
        task_id="report_whoscored_traffic",
        python_callable=aggregate_traffic_reports,
        trigger_rule="all_done",
    )
    final_gate = PythonOperator(
        task_id="final_success_gate",
        python_callable=enforce_terminal_gate,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    initialize_schema >> discover_catalog >> catalog_dq
    ingest_scopes >> profile_task >> profile_dq
    [catalog_dq, scope_dq, profile_dq, traffic_dq] >> final_gate
    [discover_catalog, ingest_scopes, profile_task] >> traffic_dq
