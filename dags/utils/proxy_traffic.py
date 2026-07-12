"""Residential-proxy traffic reporting (#789).

The scrapers already emit passive byte counters for the residential proxy pool
(`pool.proxys.io`, billed ~$4/GB): FBref writes ``/tmp/fbref_traffic_<label>.json``
per task (real CDP bytes + curl_cffi fast-path, #44/#624); the FlareSolverr and
tls_requests scrapers ship a ``traffic`` key in their run-result JSON. This module
aggregates those counters into one human-readable per-run Airflow log line so the
residential spend is finally visible — "who ate how many MB this run".

Module-level imports are stdlib only: this runs inside the Airflow scheduler
process and must NOT pull in ``scrapers/`` (CLAUDE.md memory footgun — importing
scrapers adds ~1.5 GB to the task).
"""
from __future__ import annotations

import glob
import importlib
import json
import logging
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)
MIB = 1024 * 1024


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


def summarize_fbref_traffic(
    glob_pattern: str = "/tmp/fbref_traffic_*.json",
) -> Dict[str, Any]:
    """Aggregate the per-task FBref traffic JSONs into one run-level summary.

    Each FBref task writes ``/tmp/fbref_traffic_<label>.json`` (#44). Real proxy
    bytes per task = CDP ``real_proxy_mb`` (``loadingFinished``) + curl_cffi
    ``http_mb_downloaded`` fast-path. We sum both across tasks and merge
    ``top_traffic_urls`` by host for the per-domain breakdown.

    Returns a summary dict: ``{source, total_mb, top_domains, files_read}``.
    """
    total_mb = 0.0
    domain_mb: Dict[str, float] = defaultdict(float)
    files = sorted(glob.glob(glob_pattern))
    read = 0
    for path in files:
        try:
            with open(path) as fh:
                summary = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("proxy_traffic: skipping unreadable %s: %s", path, exc)
            continue
        if not isinstance(summary, dict):
            continue
        read += 1
        total_mb += float(summary.get("real_proxy_mb") or 0.0)
        total_mb += float(summary.get("http_mb_downloaded") or 0.0)
        for row in summary.get("top_traffic_urls") or []:
            if not isinstance(row, dict):
                continue
            host = _host_of(str(row.get("url", "")))
            if host:
                domain_mb[host] += _row_mb(row)
    return {
        "source": "fbref",
        "total_mb": round(total_mb, 2),
        "top_domains": top_domains(domain_mb),
        "files_read": read,
    }


def _first_present(mapping: Dict[str, Any], keys: List[str]) -> Any:
    """Return the first explicitly present, non-None value (zero is valid)."""
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def summarize_result_traffic(
    source: str,
    traffic: Dict[str, Any],
    entity: str = None,
    run_key: str = None,
) -> Dict[str, Any]:
    """Normalize a scraper's ``traffic`` dict into a run-level summary.

    Raw decoded-body bytes are authoritative when supplied.  Older scrapers
    that only expose an MB counter remain reportable, but are explicitly
    labelled non-authoritative so paid-traffic guards can fail closed instead
    of comparing rounded values.
    """
    traffic = traffic or {}
    decoded_bytes_raw = _first_present(
        traffic,
        [
            "decoded_response_body_bytes",
            "proxy_response_bytes",  # tls_requests compatibility alias
            "fs_response_bytes",     # FlareSolverr compatibility alias
        ],
    )
    decoded_mb_raw = _first_present(
        traffic,
        [
            "decoded_response_body_mb",
            "proxy_response_mb",
            "fs_response_mb",
        ],
    )
    decoded_bytes: Optional[int] = None
    decoded_bytes_authoritative = False
    if decoded_bytes_raw is not None:
        try:
            decoded_bytes = int(decoded_bytes_raw)
        except (TypeError, ValueError, OverflowError):
            decoded_bytes = None
        else:
            if decoded_bytes < 0:
                decoded_bytes = None
            else:
                decoded_bytes_authoritative = True
    elif decoded_mb_raw is not None:
        try:
            decoded_bytes = int(Decimal(str(decoded_mb_raw)) * MIB)
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            decoded_bytes = None

    telemetry_available = bool(
        traffic.get("telemetry_available", decoded_bytes is not None)
    ) and decoded_bytes is not None
    total_mb = decoded_bytes / MIB if telemetry_available else None
    wire_raw = _first_present(
        traffic,
        [
            "estimated_wire_response_mb", "wire_mb", "proxy_wire_mb",
            "real_proxy_mb",
        ],
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
        "entity": entity or str(traffic.get("entity") or ""),
        "run_key": run_key or str(traffic.get("run_key") or ""),
        "total_mb": round(total_mb, 4) if total_mb is not None else None,
        "decoded_response_body_bytes": (
            decoded_bytes if telemetry_available else None
        ),
        "decoded_response_body_mb": (
            total_mb if total_mb is not None else None
        ),
        "decoded_bytes_authoritative": decoded_bytes_authoritative,
        "wire_mb": round(float(wire_raw), 4) if wire_raw is not None else None,
        "estimated_wire_response_mb": (
            round(float(wire_raw), 4) if wire_raw is not None else None
        ),
        "requests": int(
            _first_present(traffic, ["network_fetches", "requests"]) or 0
        ),
        "retries": int(traffic.get("retries") or 0),
        "failures": int(
            _first_present(traffic, ["failures", "failed_attempts"]) or 0
        ),
        "failed_attempts": int(
            _first_present(traffic, ["failed_attempts", "failures"]) or 0
        ),
        "telemetry_available": telemetry_available,
        "top_domains": top_domains(domain_mb),
        "files_read": 1,
    }


def check_result_traffic_guard(
    entity_paths: Dict[str, str],
    threshold_variable: str = "tm_proxy_mb_threshold",
    default_thresholds: Dict[str, float] = None,
    default_threshold_mb: float = 50.0,
    cycle_budget_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """Exact decoded-byte kill-switch over run-result JSONs.

    Mirrors the FBref guard (``utils.fbref_callbacks.check_traffic_guard``)
    for scrapers that ship one ``traffic`` dict in their run-result JSON
    (tls_requests style, e.g. Transfermarkt). Threshold lookup order per
    entity: Airflow Variable ``<threshold_variable>_<entity>`` →
    Variable ``<threshold_variable>`` → ``default_thresholds[entity]`` →
    ``default_threshold_mb``.

    Missing/corrupt result files, absent counters, and legacy rounded-only
    counters fail closed.  ``cycle_budget_bytes`` optionally adds a shared cap
    across all entity subprocesses without weakening their individual caps.
    """
    from airflow.exceptions import AirflowException
    from airflow.models import Variable

    default_thresholds = default_thresholds or {}
    checked: Dict[str, Any] = {}
    breaches: List[str] = []
    telemetry_errors: List[str] = []
    cycle_decoded_bytes = 0
    cycle_accounted_samples: List[int] = []
    for entity, path in entity_paths.items():
        try:
            with open(path) as fh:
                result = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("traffic guard: %s unreadable %s: %s", entity, path, exc)
            telemetry_errors.append(f"{entity}: result telemetry unreadable")
            continue
        if not isinstance(result, dict) or not isinstance(result.get("traffic"), dict):
            telemetry_errors.append(f"{entity}: traffic block missing")
            continue
        traffic = result["traffic"]
        summary = summarize_result_traffic(
            "transfermarkt", traffic, entity=entity,
            run_key=str(result.get("run_key") or ""),
        )
        if not summary["telemetry_available"]:
            telemetry_errors.append(f"{entity}: decoded-byte telemetry missing")
            continue
        if not summary["decoded_bytes_authoritative"]:
            telemetry_errors.append(f"{entity}: raw decoded-byte telemetry missing")
            continue
        decoded_bytes = int(summary["decoded_response_body_bytes"])
        cycle_decoded_bytes += decoded_bytes
        cycle_evidence = result.get('cycle_budget')
        if isinstance(cycle_evidence, dict):
            raw_accounted = cycle_evidence.get('accounted_after_bytes')
            raw_limit = cycle_evidence.get('limit_bytes')
            try:
                accounted = int(raw_accounted)
                evidence_limit = int(raw_limit)
            except (TypeError, ValueError, OverflowError):
                telemetry_errors.append(
                    f"{entity}: cumulative cycle ledger evidence invalid"
                )
            else:
                if accounted < 0:
                    telemetry_errors.append(
                        f"{entity}: cumulative cycle ledger evidence invalid"
                    )
                elif (
                    cycle_budget_bytes is not None
                    and evidence_limit != int(cycle_budget_bytes)
                ):
                    telemetry_errors.append(
                        f"{entity}: cycle ledger limit mismatch"
                    )
                else:
                    cycle_accounted_samples.append(accounted)
        mb = decoded_bytes / MIB
        raw = Variable.get(f"{threshold_variable}_{entity}", default_var=None)
        if raw is None:
            raw = Variable.get(threshold_variable, default_var=None)
        try:
            if raw is not None:
                threshold_decimal = Decimal(str(raw))
            else:
                threshold_decimal = Decimal(
                    str(default_thresholds.get(entity, default_threshold_mb))
                )
        except (InvalidOperation, TypeError, ValueError):
            telemetry_errors.append(f"{entity}: invalid byte threshold")
            continue
        if not threshold_decimal.is_finite() or threshold_decimal < 0:
            telemetry_errors.append(f"{entity}: invalid byte threshold")
            continue
        threshold_bytes = int(threshold_decimal * MIB)
        threshold = float(threshold_decimal)
        checked[entity] = {"mb": round(mb, 2), "threshold_mb": threshold}
        logger.info(
            "traffic guard: %s spent %d bytes (%.4f MiB; threshold %d bytes)",
            entity, decoded_bytes, mb, threshold_bytes,
        )
        if decoded_bytes > threshold_bytes:
            breaches.append(
                f"{entity}: {mb:.2f} MB ({decoded_bytes} bytes) > "
                f"{threshold:g} MB ({threshold_bytes} bytes)"
            )
    if cycle_budget_bytes is not None:
        try:
            cycle_cap = int(cycle_budget_bytes)
        except (TypeError, ValueError, OverflowError):
            telemetry_errors.append("cycle: invalid decoded-byte threshold")
        else:
            if cycle_cap < 0:
                telemetry_errors.append("cycle: invalid decoded-byte threshold")
            else:
                # Result paths are overwritten on a manual rerun. The
                # reservation ledger is cumulative and survives retries, so
                # its largest settled/accounted observation is authoritative
                # whenever present; raw per-file summing remains a fallback
                # for older producers.
                effective_cycle_bytes = max(
                    [cycle_decoded_bytes, *cycle_accounted_samples]
                )
            if cycle_cap >= 0 and effective_cycle_bytes > cycle_cap:
                breaches.append(
                    "cycle: "
                    f"{effective_cycle_bytes / MIB:.4f} MiB "
                    f"({effective_cycle_bytes} bytes) > "
                    f"{cycle_cap / MIB:.4f} MiB ({cycle_cap} bytes)"
                )
    if telemetry_errors:
        raise AirflowException(
            "residential proxy telemetry unavailable — "
            + "; ".join(telemetry_errors)
        )
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
    total_raw = summary.get("total_mb")
    if total_raw is None:
        logger.error("PROXY_TRAFFIC source=%s total=UNKNOWN", source)
        return
    total_mb = float(total_raw)
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


def _silver_tasks_module():
    """Resolve imports in Airflow and standalone/package execution modes."""
    try:
        return importlib.import_module("utils.silver_tasks")
    except ImportError:
        return importlib.import_module("dags.utils.silver_tasks")


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
    _execute = _silver_tasks_module()._execute

    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")
    _execute(
        conn,
        f"CREATE TABLE IF NOT EXISTS {OPS_TABLE} ("
        "run_ts timestamp(6), "
        "run_date date, "
        "source varchar, "
        "dag_run_id varchar, "
        "total_mb double, "
        "top_domains varchar, "
        "entity varchar, "
        "run_key varchar, "
        "request_count bigint, "
        "retry_count bigint, "
        "failure_count bigint, "
        "failed_attempt_count bigint, "
        "decoded_response_body_bytes bigint, "
        "decoded_response_body_mb double, "
        "wire_mb double, "
        "estimated_wire_response_mb double"
        ") WITH (partitioning = ARRAY['run_date'])",
    )
    # Existing deployments predate the detailed counters. Iceberg/Trino DDL is
    # additive and idempotent, so rolling out the runner does not require a
    # destructive migration or table rewrite.
    for column, sql_type in (
        ("entity", "varchar"),
        ("run_key", "varchar"),
        ("request_count", "bigint"),
        ("retry_count", "bigint"),
        ("failure_count", "bigint"),
        ("failed_attempt_count", "bigint"),
        ("decoded_response_body_bytes", "bigint"),
        ("decoded_response_body_mb", "double"),
        ("wire_mb", "double"),
        ("estimated_wire_response_mb", "double"),
    ):
        _execute(
            conn,
            f"ALTER TABLE {OPS_TABLE} ADD COLUMN IF NOT EXISTS {column} {sql_type}",
        )


def record_traffic_run(
    summary: Dict[str, Any],
    dag_run_id: str = "",
    conn=None,
) -> bool:
    """Persist one per-run residential-traffic row to the ops table (#789 Phase 2).

    ``summary`` is what :func:`summarize_fbref_traffic` / :func:`summarize_result_traffic`
    return. Passive telemetry: **never raises** — a failed insert must not break
    an ingest run (returns ``False`` and logs a warning).

    When ``conn`` is None a connection is opened and closed here; pass one to
    reuse a connection a caller already holds.
    """
    from datetime import datetime

    own_conn = False
    try:
        silver_tasks = _silver_tasks_module()
        _execute = silver_tasks._execute
        _get_trino_connection = silver_tasks._get_trino_connection

        if conn is None:
            conn = _get_trino_connection()
            own_conn = True

        ensure_ops_table(conn)

        source = str(summary.get("source") or "unknown")
        if not summary.get("telemetry_available", summary.get("total_mb") is not None):
            raise ValueError("decoded proxy traffic telemetry is unavailable")
        total_mb = float(summary["total_mb"])
        decoded_bytes = summary.get("decoded_response_body_bytes")
        decoded_mb = summary.get("decoded_response_body_mb")
        wire_mb = summary.get("wire_mb")
        estimated_wire_mb = summary.get("estimated_wire_response_mb")
        entity = str(summary.get("entity") or "")
        run_key = str(summary.get("run_key") or "")
        requests = int(summary.get("requests") or 0)
        retries = int(summary.get("retries") or 0)
        failures = int(summary.get("failures") or 0)
        failed_attempts = int(summary.get("failed_attempts") or failures)
        top = summary.get("top_domains") or []
        top_str = ", ".join(
            f"{d.get('host')}={d.get('mb')}" for d in top if d.get("host")
        )
        now = datetime.now()
        run_ts = now.strftime("%Y-%m-%d %H:%M:%S")
        run_date = now.strftime("%Y-%m-%d")

        _execute(
            conn,
            f"INSERT INTO {OPS_TABLE} "
            "(run_ts, run_date, source, dag_run_id, total_mb, top_domains, "
            "entity, run_key, request_count, retry_count, failure_count, "
            "failed_attempt_count, decoded_response_body_bytes, "
            "decoded_response_body_mb, wire_mb, "
            "estimated_wire_response_mb) VALUES ("
            f"TIMESTAMP '{run_ts}', DATE '{run_date}', "
            f"{_sql_str(source)}, {_sql_str(dag_run_id)}, "
            f"{total_mb}, {_sql_str(top_str)}, {_sql_str(entity)}, "
            f"{_sql_str(run_key)}, {requests}, {retries}, {failures}, "
            f"{failed_attempts}, "
            f"{int(decoded_bytes) if decoded_bytes is not None else 'NULL'}, "
            f"{float(decoded_mb) if decoded_mb is not None else 'NULL'}, "
            f"{float(wire_mb) if wire_mb is not None else 'NULL'}, "
            f"{float(estimated_wire_mb) if estimated_wire_mb is not None else 'NULL'})",
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
    _execute = _silver_tasks_module()._execute

    ensure_ops_table(conn)
    rows = (
        _execute(
            conn,
            "SELECT source, sum(COALESCE("
            f"CAST(decoded_response_body_bytes AS double) / {MIB}.0, "
            "total_mb)) AS mb, count(*) AS runs "
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
            "mb": round(float(r[1] or 0.0), 4),
            "gb": round(float(r[1] or 0.0) / 1024, 3),
            "runs": int(r[2] or 0),
        }
        for r in rows
    ]
    total_mb = round(sum(float(r[1] or 0.0) for r in rows), 4)
    parts = ", ".join(f"{s['source']} {s['gb']} GB" for s in by_source) or "—"
    report = f"вчера прокси съели {round(total_mb / 1024, 3)} GB ({total_mb} MB): {parts}"
    return {
        "total_mb": total_mb,
        "total_gb": round(total_mb / 1024, 3),
        "by_source": by_source,
        "report": report,
    }
