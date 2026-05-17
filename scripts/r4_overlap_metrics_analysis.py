"""R4 analysis — choose source-of-truth for overlap metrics in fct_player_season_stats.

Issue: https://github.com/sergeykuznetsov1995/data-platform-football/issues/14

Compares FotMob / Understat / FBref values for three duplicated metrics:
    xG     — expected_goals (FotMob) vs expected_goals_understat (Understat)
    xA     — expected_assists (FotMob) vs expected_assists_understat (Understat)
    shots  — shots (FBref) vs shots_understat (Understat)

Scope: APL (ENG-Premier League), seasons 2024 (2024/25 closed) + 2025 (2025/26 current).

Run inside container:
    docker compose exec airflow-webserver \\
        python /opt/airflow/scripts/r4_overlap_metrics_analysis.py

Output goes to stdout as plain-text tables. Numbers should be copied into the ADR
at docs/decisions/R4-overlap-metrics-source-of-truth.md.
"""
from __future__ import annotations

import os
import sys
from typing import Iterable

import trino
from trino.auth import BasicAuthentication

LEAGUE = "ENG-Premier League"
SEASONS = (2024, 2025)

OVERLAPS = [
    ("xG",    "expected_goals",   "expected_goals_understat"),
    ("xA",    "expected_assists", "expected_assists_understat"),
    ("shots", "shots",            "shots_understat"),
]


def connect() -> trino.dbapi.Connection:
    host = os.environ.get("TRINO_HOST", "trino")
    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    if password:
        return trino.dbapi.connect(
            host=host,
            port=int(os.environ.get("TRINO_PORT", 8443)),
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", 8080)),
        user=user,
        catalog="iceberg",
    )


def fetch(conn, sql: str) -> tuple[list[str], list[tuple]]:
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    cur.close()
    return cols, rows


def print_table(title: str, cols: list[str], rows: Iterable[tuple]) -> None:
    print(f"\n=== {title} ===")
    print("\t".join(cols))
    for row in rows:
        print("\t".join("" if v is None else str(v) for v in row))


def section_coverage(conn) -> None:
    print("\n" + "#" * 70)
    print("# 1. COVERAGE — per season, per metric (% non-null in main fct)")
    print("#" * 70)
    for label, col_a, col_b in OVERLAPS:
        sql = f"""
            SELECT
                season,
                COUNT(*) AS total_players,
                COUNT({col_a}) AS n_a,
                COUNT({col_b}) AS n_b,
                ROUND(100.0 * COUNT({col_a}) / NULLIF(COUNT(*), 0), 2) AS cov_a_pct,
                ROUND(100.0 * COUNT({col_b}) / NULLIF(COUNT(*), 0), 2) AS cov_b_pct,
                COUNT(CASE WHEN {col_a} IS NOT NULL AND {col_b} IS NOT NULL THEN 1 END) AS n_both
            FROM iceberg.gold.fct_player_season_stats
            WHERE league = '{LEAGUE}' AND season IN {SEASONS}
            GROUP BY season ORDER BY season
        """
        cols, rows = fetch(conn, sql)
        print_table(f"{label}: {col_a} vs {col_b}", cols, rows)


def section_bias_dispersion(conn) -> None:
    print("\n" + "#" * 70)
    print("# 2. BIAS / DISPERSION — on intersection (both non-null)")
    print("# diff = col_a - col_b  (positive => col_a > col_b)")
    print("#" * 70)
    for label, col_a, col_b in OVERLAPS:
        sql = f"""
            SELECT
                season,
                COUNT(*) AS n_pairs,
                ROUND(AVG({col_a} - {col_b}), 3) AS mean_diff,
                ROUND(STDDEV({col_a} - {col_b}), 3) AS std_diff,
                ROUND(APPROX_PERCENTILE({col_a} - {col_b}, 0.5), 3) AS median_diff,
                ROUND(APPROX_PERCENTILE(ABS({col_a} - {col_b}), 0.95), 3) AS abs_p95,
                ROUND(MIN({col_a} - {col_b}), 3) AS min_diff,
                ROUND(MAX({col_a} - {col_b}), 3) AS max_diff,
                ROUND(AVG({col_a}), 3) AS mean_a,
                ROUND(AVG({col_b}), 3) AS mean_b
            FROM iceberg.gold.fct_player_season_stats
            WHERE league = '{LEAGUE}' AND season IN {SEASONS}
              AND {col_a} IS NOT NULL AND {col_b} IS NOT NULL
            GROUP BY season ORDER BY season
        """
        cols, rows = fetch(conn, sql)
        print_table(f"{label}: {col_a} - {col_b}", cols, rows)


def section_top_n(conn, season: int = 2025, n: int = 20) -> None:
    print("\n" + "#" * 70)
    print(f"# 3. TOP-{n} BY GOALS — season {season} (manual cross-check vs fbref.com / understat.com)")
    print("#" * 70)
    sql = f"""
        SELECT
            COALESCE(dp.player_name, f.player_id_canonical) AS player,
            f.primary_team_name AS team,
            f.goals,
            ROUND(f.expected_goals, 2) AS xg_fm,
            ROUND(f.expected_goals_understat, 2) AS xg_us,
            ROUND(f.expected_assists, 2) AS xa_fm,
            ROUND(f.expected_assists_understat, 2) AS xa_us,
            f.shots AS sh_fbref,
            f.shots_understat AS sh_us
        FROM iceberg.gold.fct_player_season_stats f
        LEFT JOIN iceberg.gold.dim_player dp
          ON dp.player_id = f.player_id_canonical
         AND dp.league = f.league
         AND dp.season = f.season
        WHERE f.league = '{LEAGUE}' AND f.season = {season}
        ORDER BY f.goals DESC NULLS LAST
        LIMIT {n}
    """
    cols, rows = fetch(conn, sql)
    print_table(f"Top-{n} scorers {season}/{(season+1) % 100:02d}", cols, rows)


def section_outliers(conn) -> None:
    print("\n" + "#" * 70)
    print("# 4. OUTLIERS — diffs beyond 2σ (per metric, season 2025)")
    print("#" * 70)
    for label, col_a, col_b in OVERLAPS:
        sql = f"""
            WITH stats AS (
                SELECT AVG({col_a} - {col_b}) AS mu,
                       STDDEV({col_a} - {col_b}) AS sigma
                FROM iceberg.gold.fct_player_season_stats
                WHERE league = '{LEAGUE}' AND season = 2025
                  AND {col_a} IS NOT NULL AND {col_b} IS NOT NULL
            )
            SELECT
                COALESCE(dp.player_name, f.player_id_canonical) AS player,
                f.primary_team_name AS team,
                f.goals,
                ROUND(f.{col_a}, 2) AS val_a,
                ROUND(f.{col_b}, 2) AS val_b,
                ROUND(f.{col_a} - f.{col_b}, 2) AS diff
            FROM iceberg.gold.fct_player_season_stats f
            CROSS JOIN stats s
            LEFT JOIN iceberg.gold.dim_player dp
              ON dp.player_id = f.player_id_canonical
             AND dp.league = f.league AND dp.season = f.season
            WHERE f.league = '{LEAGUE}' AND f.season = 2025
              AND f.{col_a} IS NOT NULL AND f.{col_b} IS NOT NULL
              AND ABS((f.{col_a} - f.{col_b}) - s.mu) > 2 * s.sigma
            ORDER BY ABS(f.{col_a} - f.{col_b}) DESC
            LIMIT 15
        """
        cols, rows = fetch(conn, sql)
        print_table(f"{label} outliers (|diff - mean| > 2σ)", cols, rows)


def section_orphan_breakdown(conn) -> None:
    print("\n" + "#" * 70)
    print("# 5. ORPHAN BREAKDOWN — who is missing in each source (season 2025)")
    print("#" * 70)
    sql = f"""
        SELECT
            CASE
                WHEN expected_goals IS NOT NULL AND expected_goals_understat IS NOT NULL THEN 'both'
                WHEN expected_goals IS NOT NULL THEN 'fotmob_only'
                WHEN expected_goals_understat IS NOT NULL THEN 'understat_only'
                ELSE 'neither'
            END AS xg_coverage_bucket,
            COUNT(*) AS n_players,
            ROUND(AVG(goals), 2) AS avg_goals,
            ROUND(AVG(shots), 1) AS avg_shots_fbref,
            ROUND(AVG(CAST(starts AS DOUBLE)), 1) AS avg_starts
        FROM iceberg.gold.fct_player_season_stats
        WHERE league = '{LEAGUE}' AND season = 2025
        GROUP BY 1 ORDER BY n_players DESC
    """
    cols, rows = fetch(conn, sql)
    print_table("xG source-presence breakdown 2025/26", cols, rows)


def main() -> int:
    try:
        conn = connect()
    except Exception as exc:
        print(f"ERROR: cannot connect to Trino: {exc}", file=sys.stderr)
        return 1

    try:
        section_coverage(conn)
        section_bias_dispersion(conn)
        section_top_n(conn, season=2025, n=20)
        section_top_n(conn, season=2024, n=10)
        section_outliers(conn)
        section_orphan_breakdown(conn)
    except Exception as exc:
        print(f"ERROR during analysis: {exc}", file=sys.stderr)
        return 2

    print("\nDone. Copy these numbers into docs/decisions/R4-overlap-metrics-source-of-truth.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
