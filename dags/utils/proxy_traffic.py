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
