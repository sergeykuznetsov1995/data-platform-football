"""Residential-proxy traffic reporting (#789).

The durable FBref pipeline reports its PostgreSQL control-budget total directly.
FlareSolverr and tls_requests scrapers ship a ``traffic`` key in their run-result
JSON. This module normalizes those counters into a human-readable per-run
Airflow log line so residential spend remains visible.

Module-level imports are stdlib only: this runs inside the Airflow scheduler
process and must NOT pull in ``scrapers/`` (CLAUDE.md memory footgun — importing
scrapers adds ~1.5 GB to the task).
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any, Dict, List
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


def _host_of(url: str) -> str:
    """Best-effort host from a ``host/path`` or full ``scheme://host/path`` string."""
    if not url:
        return ""
    if "://" in url:
        netloc = urlsplit(url).netloc
        return netloc or url.split("/", 1)[0]
    return url.split("/", 1)[0]


def _row_mb(row: Dict[str, Any]) -> float:
    """MB of a ``top_traffic_urls`` row, tolerating either ``mb`` or raw ``bytes``."""
    if row.get("mb") is not None:
        return float(row["mb"])
    return float(row.get("bytes") or 0) / 1024 / 1024


def top_domains(domain_mb: Dict[str, float], n: int = 5) -> List[Dict[str, Any]]:
    """Top-``n`` (host, mb) rows by MB, dropping zero-byte hosts."""
    rows = sorted(domain_mb.items(), key=lambda kv: -kv[1])[:n]
    return [{"host": h, "mb": round(mb, 2)} for h, mb in rows if mb > 0]


def summarize_result_traffic(source: str, traffic: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a scraper's ``traffic`` dict into a run-level summary.

    Used by the FlareSolverr (``get_traffic_stats`` → ``fs_response_mb``) and
    tls_requests (``proxy_response_mb``) scrapers, which ship one ``traffic``
    dict in their run-result JSON. Tolerates either MB key; merges
    ``top_traffic_urls`` by host when present.
    """
    traffic = traffic or {}
    total_mb = float(
        traffic.get("proxy_response_mb")
        or traffic.get("fs_response_mb")
        or 0.0
    )
    domain_mb: Dict[str, float] = defaultdict(float)
    for row in traffic.get("top_traffic_urls") or []:
        if not isinstance(row, dict):
            continue
        host = _host_of(str(row.get("url", "")))
        if host:
            domain_mb[host] += _row_mb(row)
    return {
        "source": source,
        "total_mb": round(total_mb, 2),
        "top_domains": top_domains(domain_mb),
        "files_read": 1,
    }


def check_result_traffic_guard(
    entity_paths: Dict[str, str],
    threshold_variable: str = "tm_proxy_mb_threshold",
    default_thresholds: Dict[str, float] = None,
    default_threshold_mb: float = 50.0,
) -> Dict[str, Any]:
    """Per-entity residential-MB kill-switch over run-result JSONs.

    Applies a per-entity guard to scrapers that ship one ``traffic`` dict in
    their run-result JSON (tls_requests style, e.g. Transfermarkt). Threshold
    lookup order per entity: Airflow Variable ``<threshold_variable>_<entity>`` →
    Variable ``<threshold_variable>`` → ``default_thresholds[entity]`` →
    ``default_threshold_mb``.

    A missing/unreadable result file is a warning, not a failure — the
    scrape task itself is red in that case. A breach raises
    AirflowException: the pool bills ~$4/GB, so crossing the budget must
    be loud (an exhausted/burned proxy pool retrying 403 pages is exactly
    the failure mode this catches).
    """
    from airflow.exceptions import AirflowException
    from airflow.models import Variable

    default_thresholds = default_thresholds or {}
    checked: Dict[str, Any] = {}
    breaches: List[str] = []
    for entity, path in entity_paths.items():
        try:
            with open(path) as fh:
                result = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(
                "traffic guard: skipping %s — unreadable %s: %s",
                entity, path, exc,
            )
            continue
        traffic = (result or {}).get("traffic") or {}
        mb = float(
            traffic.get("proxy_response_mb")
            or traffic.get("fs_response_mb")
            or 0.0
        )
        raw = Variable.get(f"{threshold_variable}_{entity}", default_var=None)
        if raw is None:
            raw = Variable.get(threshold_variable, default_var=None)
        if raw is not None:
            threshold = float(raw)
        else:
            threshold = float(
                default_thresholds.get(entity, default_threshold_mb)
            )
        checked[entity] = {"mb": round(mb, 2), "threshold_mb": threshold}
        logger.info(
            "traffic guard: %s spent %.2f MB (threshold %.0f MB)",
            entity, mb, threshold,
        )
        if mb > threshold:
            breaches.append(f"{entity}: {mb:.2f} MB > {threshold:.0f} MB")
    if breaches:
        raise AirflowException(
            "residential proxy budget exceeded — " + "; ".join(breaches)
        )
    return checked


def log_traffic_summary(summary: Dict[str, Any]) -> None:
    """Emit the per-run residential-traffic line to the Airflow task log.

    Grep-friendly prefix ``PROXY_TRAFFIC`` so a run's residential cost is one
    ``grep`` away in the Airflow logs.
    """
    source = summary.get("source", "unknown")
    total_mb = float(summary.get("total_mb") or 0.0)
    top = summary.get("top_domains") or []
    top_str = ", ".join(f"{d['host']} {d['mb']} MB" for d in top) or "—"
    logger.info(
        "PROXY_TRAFFIC source=%s total=%.2f MB (%.3f GB) top: %s",
        source,
        total_mb,
        total_mb / 1024,
        top_str,
    )


# ---------------------------------------------------------------------------
# Phase 2 (#789): persist each run to iceberg.ops + a daily per-source rollup.
#
# The connection helpers live in ``utils.silver_tasks`` (imports ``trino``
# directly, NOT ``scrapers/`` — safe for the scheduler). We import them lazily
# inside each function so this module stays import-light for callers that only
# need the stdlib summarizers above.
# ---------------------------------------------------------------------------

OPS_SCHEMA = "iceberg.ops"
OPS_TABLE = "iceberg.ops.proxy_traffic_runs"


def _sql_str(value: Any) -> str:
    """Quote a value as a Trino string literal, escaping embedded single quotes."""
    return "'" + str(value if value is not None else "").replace("'", "''") + "'"


def ensure_ops_table(conn) -> None:
    """Idempotently create ``iceberg.ops.proxy_traffic_runs`` (#789 Phase 2).

    ``conn`` is a Trino DBAPI connection (see ``utils.silver_tasks``). Uses
    ``_execute`` so the post-DDL ``fetchall()`` runs — a bare ``cursor.close()``
    without it cancels the statement (CLAUDE.md footgun → USER_CANCELED).

    NOTE: a brand-new Iceberg schema needs its HDFS dir group-writable
    (``chmod 777``) for the Airflow user to insert — see
    ``feedback_gold_permissions``. That one-time container step is in the PR
    test plan; the SQL below is the runtime half.
    """
    from utils.silver_tasks import _execute

    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")
    _execute(
        conn,
        f"CREATE TABLE IF NOT EXISTS {OPS_TABLE} ("
        "run_ts timestamp(6), "
        "run_date date, "
        "source varchar, "
        "dag_run_id varchar, "
        "total_mb double, "
        "top_domains varchar"
        ") WITH (partitioning = ARRAY['run_date'])",
    )


def record_traffic_run(
    summary: Dict[str, Any],
    dag_run_id: str = "",
    conn=None,
    replace_existing: bool = False,
) -> bool:
    """Persist one per-run residential-traffic row to the ops table (#789 Phase 2).

    ``summary`` is the normalized source/MB/domain mapping produced by the
    caller or :func:`summarize_result_traffic`. Passive telemetry: **never
    raises** — a failed insert must not break an ingest run (returns ``False``
    and logs a warning).

    When ``conn`` is None a connection is opened and closed here; pass one to
    reuse a connection a caller already holds. ``replace_existing`` makes a
    single run-level reporter retry-idempotent by deleting the exact
    ``(source, dag_run_id)`` row before inserting it. It is opt-in because
    some legacy DAGs intentionally emit several source rows per DAG run.
    """
    from datetime import datetime

    own_conn = False
    try:
        from utils.silver_tasks import _execute, _get_trino_connection

        if conn is None:
            conn = _get_trino_connection()
            own_conn = True

        ensure_ops_table(conn)

        source = str(summary.get("source") or "unknown")
        total_mb = float(summary.get("total_mb") or 0.0)
        top = summary.get("top_domains") or []
        top_str = ", ".join(
            f"{d.get('host')}={d.get('mb')}" for d in top if d.get("host")
        )
        now = datetime.now()
        run_ts = now.strftime("%Y-%m-%d %H:%M:%S")
        run_date = now.strftime("%Y-%m-%d")

        if replace_existing:
            if not str(dag_run_id).strip():
                raise ValueError(
                    "replace_existing requires a non-empty dag_run_id"
                )
            _execute(
                conn,
                f"DELETE FROM {OPS_TABLE} WHERE source = {_sql_str(source)} "
                f"AND dag_run_id = {_sql_str(dag_run_id)}",
            )

        _execute(
            conn,
            f"INSERT INTO {OPS_TABLE} "
            "(run_ts, run_date, source, dag_run_id, total_mb, top_domains) VALUES ("
            f"TIMESTAMP '{run_ts}', DATE '{run_date}', "
            f"{_sql_str(source)}, {_sql_str(dag_run_id)}, "
            f"{total_mb}, {_sql_str(top_str)})",
        )
        logger.info(
            "PROXY_TRAFFIC persisted source=%s total=%.2f MB run=%s",
            source,
            total_mb,
            dag_run_id or "?",
        )
        return True
    except Exception as exc:  # noqa: BLE001 — telemetry must never fail an ingest
        logger.warning(
            "proxy_traffic: record_traffic_run skipped (non-fatal): %s", exc
        )
        return False
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def daily_rollup(conn) -> Dict[str, Any]:
    """Per-source residential-traffic totals for *yesterday* (#789 Phase 2).

    Reuses ``_execute`` (fetch=True). Returns
    ``{total_mb, total_gb, by_source: [{source, mb, gb, runs}], report}`` where
    ``report`` is the human line the daily DAG logs.
    """
    from utils.silver_tasks import _execute

    ensure_ops_table(conn)
    rows = (
        _execute(
            conn,
            "SELECT source, sum(total_mb) AS mb, count(*) AS runs "
            f"FROM {OPS_TABLE} "
            "WHERE run_date = current_date - INTERVAL '1' DAY "
            "GROUP BY source ORDER BY mb DESC",
            fetch=True,
        )
        or []
    )
    by_source = [
        {
            "source": r[0],
            "mb": round(float(r[1] or 0.0), 2),
            "gb": round(float(r[1] or 0.0) / 1024, 3),
            "runs": int(r[2] or 0),
        }
        for r in rows
    ]
    total_mb = round(sum(s["mb"] for s in by_source), 2)
    parts = ", ".join(f"{s['source']} {s['gb']} GB" for s in by_source) or "—"
    report = f"вчера прокси съели {round(total_mb / 1024, 3)} GB ({total_mb} MB): {parts}"
    return {
        "total_mb": total_mb,
        "total_gb": round(total_mb / 1024, 3),
        "by_source": by_source,
        "report": report,
    }
