"""Data-quality contracts for the native ESPN v2 Silver layer.

The standard checks use :mod:`utils.data_quality`; cross-table semantics are
kept as small read-only SQL checks because their denominators span tables.
There is deliberately no freshness gate while the ESPN v2 ingest is paused.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

from utils.data_quality import CHECK, Check, CheckResult, _get_conn


SILVER_MIN_ROWS = {
    "espn_match": 20_000,
    "espn_team_match": 14_000,
    "espn_player_match_aggregate": 300_000,
    "espn_match_events": 50_000,
    "espn_substitutions": 65_000,
    "espn_venue": 1_000,
}

LINEUP_ZERO_COVERAGE_ALLOWLIST = frozenset({
    "arg.3",
    "caf.championship_qual",
    "fifa.conmebol.olympicsq",
    "ned.3.promotion.relegation",
    "global.gulf_cup",
    "caf.nations_qual",
    "slv.1",
    "sco.tennents_qual",
    "hon.1",
    "rus.1.promotion.relegation",
    "chi.super_cup",
    "bol.ply.rel",
    "arg.trofeo_de_la_campeones",
})

_TABLE_KEYS = {
    "espn_match": ["event_id"],
    "espn_team_match": ["event_id", "team_id"],
    "espn_player_match_aggregate": ["event_id", "team_id", "athlete_id"],
    "espn_match_events": ["event_id", "seq"],
    "espn_substitutions": ["event_id", "team_id", "player_in_id"],
    "espn_venue": ["venue_id"],
}

_CHILD_TABLES = (
    "espn_team_match",
    "espn_player_match_aggregate",
    "espn_match_events",
    "espn_substitutions",
)


def build_espn_silver_checks() -> List[Check]:
    """Return table-local ERROR contracts plus non-blocking range monitors."""
    checks: List[Check] = []
    for table, pk in _TABLE_KEYS.items():
        fq_table = f"silver.{table}"
        required = [*pk, "league", "season"]
        if table == "espn_match":
            required.extend(["kickoff", "status", "home_team_id", "away_team_id"])
        checks.extend([
            CHECK.no_duplicates(fq_table, pk=pk, name=f"pk[{fq_table}]"),
            CHECK.no_nulls(fq_table, cols=required, name=f"required[{fq_table}]"),
            CHECK.row_count(
                fq_table, min_rows=SILVER_MIN_ROWS[table], name=f"row_floor[{fq_table}]"
            ),
        ])

    for child in _CHILD_TABLES:
        checks.append(CHECK.ref_integrity(
            f"silver.{child}", "silver.espn_match", key="event_id",
            name=f"event_ref[{child}]",
        ))

    checks.extend([
        CHECK.no_nulls(
            "silver.espn_match", cols=["home_score", "away_score"],
            where="is_played = true", name="score_present_for_played",
        ),
        CHECK.value_range(
            "silver.espn_team_match", "possession_pct", 0, 100,
            severity="WARNING", name="range[team possession]",
        ),
        CHECK.value_range(
            "silver.espn_match_events", "minute", 0, 130,
            severity="WARNING", name="range[event minute]",
        ),
        CHECK.value_range(
            "silver.espn_match_events", "plus_minute", 0, 20,
            severity="WARNING", name="range[event plus_minute]",
        ),
    ])
    return checks


@dataclass(frozen=True)
class CustomCheck:
    """A read-only multi-table check whose predicate receives one result row."""

    name: str
    sql: str
    severity: str
    passed: Callable[[Tuple[int, ...]], bool]
    kind: str = "cross_table"
    threshold: Optional[float] = None


def _allowlist_sql() -> str:
    return ", ".join("'" + slug.replace("'", "''") + "'" for slug in sorted(LINEUP_ZERO_COVERAGE_ALLOWLIST))


def build_espn_silver_custom_checks() -> List[CustomCheck]:
    """Build source-faithful cross-table checks.

    Every monitoring signal is intentionally static WARNING severity: it must
    never become an Airflow-blocking ERROR merely because its ratio is poor.
    ``children are played`` is the sole custom ERROR invariant.
    """
    allowlist = _allowlist_sql()
    return [
        CustomCheck(
            "children are played",
            """
            SELECT COUNT(*)
            FROM (
                SELECT event_id FROM iceberg.silver.espn_team_match
                UNION ALL SELECT event_id FROM iceberg.silver.espn_player_match_aggregate
                UNION ALL SELECT event_id FROM iceberg.silver.espn_match_events
                UNION ALL SELECT event_id FROM iceberg.silver.espn_substitutions
            ) child
            LEFT JOIN iceberg.silver.espn_match m ON m.event_id = child.event_id
            WHERE COALESCE(m.is_played, false) = false
            """,
            "ERROR", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "coverage[played lineup >=80%]",
            """
            SELECT COUNT(DISTINCT p.event_id), COUNT(DISTINCT m.event_id)
            FROM iceberg.silver.espn_match m
            LEFT JOIN iceberg.silver.espn_player_match_aggregate p ON p.event_id = m.event_id
            WHERE m.is_played = true
            """,
            "WARNING", lambda row: row[1] > 0 and row[0] / row[1] >= 0.80,
            threshold=0.80,
        ),
        CustomCheck(
            "coverage[played team stats >=85%]",
            """
            SELECT COUNT(DISTINCT t.event_id), COUNT(DISTINCT m.event_id)
            FROM iceberg.silver.espn_match m
            LEFT JOIN iceberg.silver.espn_team_match t ON t.event_id = m.event_id
            WHERE m.is_played = true
            """,
            "WARNING", lambda row: row[1] > 0 and row[0] / row[1] >= 0.85,
            threshold=0.85,
        ),
        CustomCheck(
            "coverage[played events >=85%]",
            """
            SELECT COUNT(DISTINCT e.event_id), COUNT(DISTINCT m.event_id)
            FROM iceberg.silver.espn_match m
            LEFT JOIN iceberg.silver.espn_match_events e ON e.event_id = m.event_id
            WHERE m.is_played = true
            """,
            "WARNING", lambda row: row[1] > 0 and row[0] / row[1] >= 0.85,
            threshold=0.85,
        ),
        CustomCheck(
            "lineup zero coverage outside allowlist",
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT m.league
                FROM iceberg.silver.espn_match m
                LEFT JOIN iceberg.silver.espn_player_match_aggregate p ON p.event_id = m.event_id
                WHERE m.is_played = true
                GROUP BY m.league
                HAVING COUNT(p.event_id) = 0 AND m.league NOT IN ({allowlist})
            ) missing_slug
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "0:0 with goal event",
            """
            SELECT COUNT(DISTINCT m.event_id)
            FROM iceberg.silver.espn_match m
            JOIN iceberg.silver.espn_match_events e ON e.event_id = m.event_id AND e.is_goal = true
            WHERE m.is_played = true AND m.home_score = 0 AND m.away_score = 0
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "played 0:0 share <=15%",
            """
            SELECT COUNT_IF(home_score = 0 AND away_score = 0), COUNT(*)
            FROM iceberg.silver.espn_match
            WHERE is_played = true
            """,
            "WARNING", lambda row: row[1] > 0 and row[0] / row[1] <= 0.15,
            threshold=0.15,
        ),
        CustomCheck(
            "goals events <= score + 2",
            """
            SELECT COUNT(*)
            FROM (
                SELECT m.event_id
                FROM iceberg.silver.espn_match m
                LEFT JOIN iceberg.silver.espn_player_match_aggregate p
                    ON p.event_id = m.event_id
                WHERE m.is_played = true AND m.status <> 'STATUS_FINAL_PEN'
                GROUP BY m.event_id, m.home_score, m.away_score
                HAVING SUM(COALESCE(p.goals_events, 0)) > m.home_score + m.away_score + 2
            ) overcounted
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "coverage[played referee >=15%]",
            """
            SELECT COUNT_IF(NULLIF(TRIM(referee), '') IS NOT NULL), COUNT(*)
            FROM iceberg.silver.espn_match
            WHERE is_played = true
            """,
            "WARNING", lambda row: row[1] > 0 and row[0] / row[1] >= 0.15,
            threshold=0.15,
        ),
        CustomCheck(
            "manifest played-lineup disposition",
            """
            WITH bronze_src_manifest AS (
                SELECT scope_id, completed_at, generation_id, run_id, dispositions_json
                FROM iceberg.bronze.espn_ingest_manifest_v2
            ),
            manifest_ranked AS (
                SELECT manifest.*, ROW_NUMBER() OVER (
                    PARTITION BY scope_id
                    ORDER BY completed_at DESC, generation_id DESC, run_id DESC
                ) AS rn
                FROM bronze_src_manifest manifest
            ),
            manifest_dedup AS (
                SELECT * FROM manifest_ranked WHERE rn = 1
            ),
            lineup_dispositions AS (
                SELECT DISTINCT TRY_CAST(json_extract_scalar(item, '$.event_id') AS bigint) AS event_id
                FROM manifest_dedup manifest
                CROSS JOIN UNNEST(CAST(json_extract(manifest.dispositions_json, '$') AS array(json)))
                    AS disposition(item)
                WHERE json_extract_scalar(item, '$.endpoint') = 'lineup'
            )
            SELECT COUNT(*)
            FROM iceberg.silver.espn_match m
            LEFT JOIN lineup_dispositions lineup ON lineup.event_id = m.event_id
            WHERE m.is_played = true
              AND lineup.event_id IS NULL
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "winner parity",
            """
            WITH bronze_src_schedule AS (
                SELECT
                    event_id, home_team_id, away_team_id, home_score, away_score,
                    played_final, extra_json, _ingested_at, generation_id, run_id
                FROM iceberg.bronze.espn_schedule_generation_v2
            ),
            schedule_ranked AS (
                SELECT schedule.*, ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY _ingested_at DESC, generation_id DESC, run_id DESC
                ) AS rn
                FROM bronze_src_schedule schedule
            ),
            schedule_dedup AS (
                SELECT * FROM schedule_ranked WHERE rn = 1
            ),
            winner_flags AS (
                SELECT
                    event_id,
                    CASE
                        WHEN TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.home.competitor.winner') AS boolean) THEN home_team_id
                        WHEN TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.away.competitor.winner') AS boolean) THEN away_team_id
                    END AS raw_winner_team_id,
                    CASE
                        WHEN home_score > away_score THEN home_team_id
                        WHEN away_score > home_score THEN away_team_id
                        WHEN TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.home.competitor.shootoutScore') AS integer)
                             > TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.away.competitor.shootoutScore') AS integer)
                            THEN home_team_id
                        WHEN TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.away.competitor.shootoutScore') AS integer)
                             > TRY_CAST(json_extract_scalar(extra_json,
                            '$.sides.home.competitor.shootoutScore') AS integer)
                            THEN away_team_id
                    END AS modeled_winner_team_id
                FROM schedule_dedup
                WHERE played_final = true
            )
            SELECT COUNT(*)
            FROM winner_flags
            WHERE raw_winner_team_id IS DISTINCT FROM modeled_winner_team_id
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
        CustomCheck(
            "scoreboard totalGoals parity",
            """
            WITH bronze_src_schedule AS (
                SELECT event_id, home_team_id, away_team_id, extra_json,
                       _ingested_at, generation_id, run_id
                FROM iceberg.bronze.espn_schedule_generation_v2
            ),
            schedule_ranked AS (
                SELECT schedule.*, ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY _ingested_at DESC, generation_id DESC, run_id DESC
                ) AS rn
                FROM bronze_src_schedule schedule
            ),
            schedule_dedup AS (
                SELECT * FROM schedule_ranked WHERE rn = 1
            ),
            schedule_sides AS (
                SELECT event_id, home_team_id AS team_id,
                       CAST(json_extract(extra_json,
                           '$.sides.home.competitor.statistics') AS array(json)) AS statistics
                FROM schedule_dedup
                UNION ALL
                SELECT event_id, away_team_id AS team_id,
                       CAST(json_extract(extra_json,
                           '$.sides.away.competitor.statistics') AS array(json)) AS statistics
                FROM schedule_dedup
            ),
            scoreboard_total_goals AS (
                SELECT side.event_id, side.team_id,
                       MAX(TRY_CAST(json_extract_scalar(stat.item, '$.displayValue') AS integer))
                           AS total_goals
                FROM schedule_sides side
                CROSS JOIN UNNEST(side.statistics) AS stat(item)
                WHERE json_extract_scalar(stat.item, '$.name') = 'totalGoals'
                GROUP BY side.event_id, side.team_id
            )
            SELECT COUNT(*)
            FROM iceberg.silver.espn_team_match t
            JOIN iceberg.silver.espn_match m ON m.event_id = t.event_id
            LEFT JOIN scoreboard_total_goals scoreboard
              ON scoreboard.event_id = t.event_id AND scoreboard.team_id = t.team_id
            WHERE t.stats_source = 'scoreboard'
              AND m.is_played = true
              AND scoreboard.total_goals IS DISTINCT FROM t.goals_for
            """,
            "WARNING", lambda row: row[0] == 0,
        ),
    ]


def _safe_row(row: Sequence[object] | None) -> Tuple[int, ...]:
    """Match framework semantics: NULL/missing numeric output becomes zero."""
    return tuple(int(value or 0) for value in (row or ()))


def run_espn_silver_custom_check(check: CustomCheck, conn=None) -> CheckResult:
    """Execute one custom check without mutating ESPN source or Silver tables."""
    owns_connection = conn is None
    conn = conn or _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(check.sql)
            row = _safe_row(cur.fetchone())
        finally:
            cur.close()
        passed = check.passed(row)
        return CheckResult(
            name=check.name, kind=check.kind, severity=check.severity, passed=passed,
            details=f"values={row}", value=row,
        )
    except Exception as exc:
        return CheckResult(
            name=check.name, kind=check.kind, severity=check.severity, passed=False,
            error=str(exc),
        )
    finally:
        if owns_connection:
            conn.close()


def run_espn_silver_custom_checks() -> List[CheckResult]:
    """Run all custom DQ checks on one read-only Trino connection."""
    conn = _get_conn()
    try:
        return [run_espn_silver_custom_check(check, conn=conn)
                for check in build_espn_silver_custom_checks()]
    finally:
        conn.close()
