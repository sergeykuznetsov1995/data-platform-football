"""E0 — Storage baseline snapshot.

Collects per-table Iceberg + HDFS metrics across bronze/silver/gold schemas plus
overall HDFS and host-disk utilization. Output is a JSON snapshot that future
Medallion Redesign stages (E1-E9) compare against to measure deltas.

Per-table metrics (Trino):
  - row_count                       SELECT COUNT(*) FROM iceberg.<schema>.<table>
  - partition_count                 SELECT COUNT(*) FROM iceberg.<schema>."<table>$partitions"
  - iceberg_files_count             SELECT COUNT(*) FROM iceberg.<schema>."<table>$files"
  - iceberg_snapshot_count          SELECT COUNT(*) FROM iceberg.<schema>."<table>$snapshots"
  - latest_snapshot_committed_at    SELECT MAX(committed_at) FROM iceberg.<schema>."<table>$snapshots"

HDFS metrics (via `docker exec namenode hdfs dfs ...`):
  Iceberg appends a UUID suffix to every table directory (e.g.
  `clubelo_ratings-a7808145eb624108a705cb22045fbcd8`) so we cannot guess the
  path from `<schema>.<table>` alone. We resolve the actual location via
  `SHOW CREATE TABLE iceberg.<schema>.<table>` (works for empty tables too)
  and parse the `WITH (location = '...')` clause.
  - location                        Full HDFS path of the Iceberg table root
  - hdfs_total_bytes                <location>           (bulked via -du -s -x)
  - hdfs_metadata_bytes             <location>/metadata  (bulked via -du -s -x)
  - hdfs_overall                    hdfs dfs -df /

Host disk:
  - df -B1 /

Output: data/audit/storage_baseline_<YYYY-MM-DD>.json

Usage:
  TRINO_PASSWORD=... python3 scripts/audit_storage_baseline.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "data" / "audit"

SCHEMAS = ("bronze", "silver", "gold")
HDFS_WAREHOUSE_BASE = "/user/hive/warehouse"
HDFS_NAMENODE_CONTAINER = os.environ.get("HDFS_NAMENODE_CONTAINER", "namenode")
SCHEMA_VERSION = "v1"


# ---------------------------------------------------------------------------
# Trino connection
# ---------------------------------------------------------------------------

def _get_trino_connection():
    """Return a Trino DB-API connection or None if unavailable.

    Mirrors `dags/utils/silver_tasks._get_trino_connection()` but kept local
    so this script can run on the host without Airflow's PYTHONPATH.
    """
    try:
        from trino import dbapi
        from trino.auth import BasicAuthentication
    except ImportError:
        logger.warning("trino lib not installed — Trino metrics will be skipped")
        return None

    host = os.environ.get("TRINO_HOST", "localhost")
    port = int(os.environ.get("TRINO_PORT", 8082))
    user = os.environ.get("TRINO_USER", "airflow")
    pwd = os.environ.get("TRINO_PASSWORD")
    if not pwd:
        logger.warning("TRINO_PASSWORD not set — Trino metrics will be skipped")
        return None

    try:
        return dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=BasicAuthentication(user, pwd),
            verify=False,
        )
    except Exception as e:
        logger.warning(f"Trino connection failed ({e}) — Trino metrics will be skipped")
        return None


def _trino_scalar(conn, sql: str) -> Any:
    """Run a SQL that returns a single row/column. Re-raises on error."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:
            pass
    if not rows:
        return None
    return rows[0][0]


# ---------------------------------------------------------------------------
# Trino: list tables / per-table metrics
# ---------------------------------------------------------------------------

def list_tables(conn, schema: str) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES FROM iceberg.{schema}")
        return sorted(r[0] for r in cur.fetchall())
    except Exception as e:
        logger.warning(f"SHOW TABLES FROM iceberg.{schema} failed: {e}")
        return []
    finally:
        try:
            cur.close()
        except Exception:
            pass


def collect_trino_metrics(conn, schema: str, table: str) -> dict[str, Any]:
    """Collect Trino-side per-table metrics. None for individual fields on error."""
    fqn = f"iceberg.{schema}.{table}"
    metrics: dict[str, Any] = {
        "row_count": None,
        "partition_count": None,
        "iceberg_files_count": None,
        "iceberg_snapshot_count": None,
        "latest_snapshot_committed_at": None,
    }

    # row_count
    try:
        metrics["row_count"] = _trino_scalar(conn, f"SELECT COUNT(*) FROM {fqn}")
    except Exception as e:
        logger.warning(f"  {schema}.{table}: row_count failed: {e}")

    # partition_count — graceful: not all tables are partitioned
    try:
        metrics["partition_count"] = _trino_scalar(
            conn, f'SELECT COUNT(*) FROM iceberg.{schema}."{table}$partitions"'
        )
    except Exception as e:
        msg = str(e)
        if "not partitioned" in msg.lower() or "INVALID_TABLE_PROPERTY" in msg:
            logger.debug(f"  {schema}.{table}: not partitioned")
        else:
            logger.debug(f"  {schema}.{table}: partition_count failed: {e}")
        metrics["partition_count"] = None

    # iceberg_files_count
    try:
        metrics["iceberg_files_count"] = _trino_scalar(
            conn, f'SELECT COUNT(*) FROM iceberg.{schema}."{table}$files"'
        )
    except Exception as e:
        logger.debug(f"  {schema}.{table}: files_count failed: {e}")

    # iceberg_snapshot_count
    try:
        metrics["iceberg_snapshot_count"] = _trino_scalar(
            conn, f'SELECT COUNT(*) FROM iceberg.{schema}."{table}$snapshots"'
        )
    except Exception as e:
        logger.debug(f"  {schema}.{table}: snapshot_count failed: {e}")

    # latest_snapshot_committed_at
    try:
        ts = _trino_scalar(
            conn, f'SELECT MAX(committed_at) FROM iceberg.{schema}."{table}$snapshots"'
        )
        if ts is not None:
            # Trino returns datetime; ensure ISO string with UTC
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                metrics["latest_snapshot_committed_at"] = ts.astimezone(timezone.utc).isoformat()
            else:
                metrics["latest_snapshot_committed_at"] = str(ts)
    except Exception as e:
        logger.debug(f"  {schema}.{table}: latest_snapshot failed: {e}")

    return metrics


# ---------------------------------------------------------------------------
# Table-location resolution
# ---------------------------------------------------------------------------

_LOCATION_RE = re.compile(r"location\s*=\s*'([^']+)'", re.IGNORECASE)


def _location_to_hdfs_path(location: str) -> Optional[str]:
    """Convert a Trino/Iceberg location URI to an absolute HDFS path.

    Examples:
      hdfs://hdfs-namenode:9000/user/hive/warehouse/bronze.db/clubelo_ratings-<uuid>
        -> /user/hive/warehouse/bronze.db/clubelo_ratings-<uuid>
      /user/hive/warehouse/.../tbl-<uuid>  (already a path)
        -> same
    """
    if not location:
        return None
    if location.startswith("/"):
        return location.rstrip("/")
    try:
        parsed = urlparse(location)
    except Exception:
        return None
    if parsed.scheme in ("hdfs", "webhdfs", "swebhdfs", "file") and parsed.path:
        return parsed.path.rstrip("/")
    # Last-ditch: scrape the path component out of the string
    if "/user/hive/warehouse/" in location:
        return "/" + location.split("/user/hive/warehouse/", 1)[1].rstrip("/").lstrip("/")
    return None


def resolve_table_location(conn, schema: str, table: str) -> Optional[str]:
    """Return the absolute HDFS path of an Iceberg table's root.

    Uses SHOW CREATE TABLE and parses the WITH (location = '...') clause —
    works for both empty and non-empty tables, no $path probing needed.
    Returns None if the location cannot be determined.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'SHOW CREATE TABLE iceberg.{schema}."{table}"')
        rows = cur.fetchall()
    except Exception as e:
        logger.warning(f"  {schema}.{table}: SHOW CREATE TABLE failed: {e}")
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass

    if not rows:
        return None
    ddl = rows[0][0]
    if not isinstance(ddl, str):
        return None
    m = _LOCATION_RE.search(ddl)
    if not m:
        logger.warning(f"  {schema}.{table}: no location in SHOW CREATE TABLE output")
        return None
    return _location_to_hdfs_path(m.group(1))


# ---------------------------------------------------------------------------
# HDFS metrics via `docker exec namenode`
# ---------------------------------------------------------------------------

def _docker_hdfs(args: list[str], timeout: int = 30) -> Optional[str]:
    """Run `docker exec <namenode> hdfs dfs <args>`. Return stdout text or None on error."""
    cmd = ["docker", "exec", HDFS_NAMENODE_CONTAINER, "hdfs", "dfs", *args]
    try:
        r = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return r.stdout
    except FileNotFoundError:
        logger.warning("`docker` not found in PATH — HDFS metrics will be null")
        return None
    except subprocess.CalledProcessError as e:
        logger.debug(f"hdfs {' '.join(args)} failed (rc={e.returncode}): {e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        logger.warning(f"hdfs {' '.join(args)} timed out")
        return None
    except Exception as e:
        logger.warning(f"hdfs {' '.join(args)} unexpected error: {e}")
        return None


def hdfs_du_bytes(path: str) -> Optional[int]:
    """`hdfs dfs -du -s -x <path>` → first column (bytes). None if path missing."""
    out = _docker_hdfs(["-du", "-s", "-x", path])
    if not out:
        return None
    line = out.strip().splitlines()[0] if out.strip() else ""
    if not line:
        return None
    try:
        # Format: <size>  <disk_size>  <path>
        return int(line.split()[0])
    except (ValueError, IndexError) as e:
        logger.debug(f"could not parse `hdfs -du` output for {path}: {line!r} ({e})")
        return None


def hdfs_du_bulk(paths: list[str], chunk_size: int = 50) -> dict[str, int]:
    """Run `hdfs dfs -du -x <p1> <p2> ...` in chunks; parse `<size> <disk> <path>` rows.

    Returns mapping {hdfs_path: size_in_bytes} for paths that resolved successfully.
    Missing paths are simply absent from the result — caller treats absence as None.
    Note: this uses `-du` (not `-du -s`) so each input path's own size is reported on
    a single line. We intentionally avoid `-s` because Hadoop's combined-summary mode
    aggregates differently when invoked on a list of siblings.
    """
    result: dict[str, int] = {}
    if not paths:
        return result
    # De-dup while preserving order
    seen: set[str] = set()
    unique = []
    for p in paths:
        if p and p not in seen:
            seen.add(p)
            unique.append(p)

    for i in range(0, len(unique), chunk_size):
        batch = unique[i : i + chunk_size]
        # `-du -s -x <p1> <p2> ...` returns one summary line per input path.
        out = _docker_hdfs(["-du", "-s", "-x", *batch], timeout=120)
        if not out:
            continue
        for ln in out.strip().splitlines():
            parts = ln.split()
            # Expected: <size> <disk_size> <path>
            if len(parts) < 3:
                continue
            try:
                size = int(parts[0])
            except ValueError:
                continue
            # Last token is the path (might contain hdfs:// scheme — normalize)
            raw_path = parts[-1]
            norm = _location_to_hdfs_path(raw_path) or raw_path
            result[norm] = size
    return result


def hdfs_overall() -> dict[str, Any]:
    """`hdfs dfs -df /` → total/used/available/used_pct."""
    out = _docker_hdfs(["-df", "/"])
    result: dict[str, Any] = {
        "total_bytes": None,
        "used_bytes": None,
        "available_bytes": None,
        "used_pct": None,
    }
    if not out:
        return result
    # Output:
    # Filesystem            Size           Used      Available  Use%
    # hdfs://namenode:9000  1234567890     12345     1234555545 1%
    lines = [ln for ln in out.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return result
    parts = lines[1].split()
    # parts: [filesystem, size, used, available, use%]
    if len(parts) < 5:
        return result
    try:
        result["total_bytes"] = int(parts[1])
        result["used_bytes"] = int(parts[2])
        result["available_bytes"] = int(parts[3])
        used_pct_str = parts[4].rstrip("%")
        result["used_pct"] = float(used_pct_str) if used_pct_str else None
    except (ValueError, IndexError) as e:
        logger.debug(f"could not parse `hdfs -df /` output: {lines[1]!r} ({e})")
    return result


# ---------------------------------------------------------------------------
# Host disk metrics
# ---------------------------------------------------------------------------

def host_disk() -> dict[str, Any]:
    """`df -B1 /` → total/used/used_pct."""
    result: dict[str, Any] = {
        "total_bytes": None,
        "used_bytes": None,
        "used_pct": None,
    }
    try:
        r = subprocess.run(
            ["df", "-B1", "/"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = r.stdout
    except Exception as e:
        logger.warning(f"`df -B1 /` failed: {e}")
        return result

    lines = [ln for ln in out.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return result
    parts = lines[1].split()
    # parts: [filesystem, 1B-blocks, used, available, use%, mountpoint]
    if len(parts) < 5:
        return result
    try:
        result["total_bytes"] = int(parts[1])
        result["used_bytes"] = int(parts[2])
        used_pct_str = parts[4].rstrip("%")
        result["used_pct"] = float(used_pct_str) if used_pct_str else None
    except (ValueError, IndexError) as e:
        logger.debug(f"could not parse `df` output: {lines[1]!r} ({e})")
    return result


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _safe_add(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = OUT_DIR / f"storage_baseline_{today}.json"

    conn = _get_trino_connection()

    # 1) discover tables
    schema_tables: dict[str, list[str]] = {}
    if conn is not None:
        for schema in SCHEMAS:
            tables = list_tables(conn, schema)
            if not tables:
                logger.info(f"Schema {schema}: no tables (or unavailable) — skipping")
            else:
                logger.info(f"Schema {schema}: {len(tables)} tables")
            schema_tables[schema] = tables
    else:
        logger.warning("Trino unavailable — Trino metrics for all tables will be null")
        for schema in SCHEMAS:
            schema_tables[schema] = []

    total_count = sum(len(v) for v in schema_tables.values())
    logger.info(f"Total tables to audit: {total_count}")

    # 2a) resolve HDFS root for every table via SHOW CREATE TABLE, batched du
    table_locations: dict[tuple[str, str], Optional[str]] = {}
    if conn is not None:
        logger.info("Resolving table locations via SHOW CREATE TABLE...")
        for schema in SCHEMAS:
            for table in schema_tables[schema]:
                table_locations[(schema, table)] = resolve_table_location(conn, schema, table)
        resolved = sum(1 for v in table_locations.values() if v)
        logger.info(
            f"  resolved {resolved}/{len(table_locations)} table locations"
        )

    # Bulk HDFS du for all roots + their metadata subdirs
    all_paths: list[str] = []
    for loc in table_locations.values():
        if loc:
            all_paths.append(loc)
            all_paths.append(loc + "/metadata")
    logger.info(f"Running batched `hdfs dfs -du -s -x` on {len(all_paths)} paths...")
    du_map = hdfs_du_bulk(all_paths) if all_paths else {}

    # 2b) per-table loop
    table_entries: list[dict[str, Any]] = []
    schema_summary: dict[str, dict[str, Any]] = {
        s: {"table_count": 0, "rows": 0, "hdfs_bytes": 0, "iceberg_metadata_bytes": 0}
        for s in SCHEMAS
    }

    i = 0
    for schema in SCHEMAS:
        for table in schema_tables[schema]:
            i += 1
            fqn = f"iceberg.{schema}.{table}"

            # Trino metrics
            if conn is not None:
                tmetrics = collect_trino_metrics(conn, schema, table)
            else:
                tmetrics = {
                    "row_count": None,
                    "partition_count": None,
                    "iceberg_files_count": None,
                    "iceberg_snapshot_count": None,
                    "latest_snapshot_committed_at": None,
                }

            # HDFS metrics — keyed off the actual table location (UUID-suffixed)
            tbl_path = table_locations.get((schema, table))
            if tbl_path:
                meta_path = tbl_path + "/metadata"
                hdfs_total = du_map.get(tbl_path)
                hdfs_meta = du_map.get(meta_path)
                # Fallback per-path call if bulk lookup missed (rare)
                if hdfs_total is None:
                    hdfs_total = hdfs_du_bytes(tbl_path)
                if hdfs_meta is None:
                    hdfs_meta = hdfs_du_bytes(meta_path)
            else:
                logger.warning(
                    f"  {schema}.{table}: location unresolved — HDFS metrics will be null"
                )
                hdfs_total = None
                hdfs_meta = None

            entry = {
                "schema": schema,
                "table": table,
                "fqn": fqn,
                "location": tbl_path,
                **tmetrics,
                "hdfs_total_bytes": hdfs_total,
                "hdfs_metadata_bytes": hdfs_meta,
            }
            table_entries.append(entry)

            # Roll into schema summary
            schema_summary[schema]["table_count"] += 1
            if entry["row_count"] is not None:
                schema_summary[schema]["rows"] += entry["row_count"]
            if hdfs_total is not None:
                schema_summary[schema]["hdfs_bytes"] += hdfs_total
            if hdfs_meta is not None:
                schema_summary[schema]["iceberg_metadata_bytes"] += hdfs_meta

            logger.info(
                f"  [{i}/{total_count}] {schema}.{table}: "
                f"rows={entry['row_count']}, "
                f"hdfs={hdfs_total}, "
                f"meta={hdfs_meta}, "
                f"snapshots={entry['iceberg_snapshot_count']}"
            )

    # 3) overall metrics
    logger.info("Collecting HDFS overall...")
    hdfs_all = hdfs_overall()
    logger.info("Collecting host disk...")
    host_all = host_disk()

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    # 4) summary
    total_rows: Optional[int] = None
    total_hdfs: Optional[int] = None
    total_meta: Optional[int] = None
    for s in SCHEMAS:
        ss = schema_summary[s]
        if ss["table_count"] > 0:
            total_rows = _safe_add(total_rows, ss["rows"])
            total_hdfs = _safe_add(total_hdfs, ss["hdfs_bytes"])
            total_meta = _safe_add(total_meta, ss["iceberg_metadata_bytes"])

    summary = {
        "total_tables": total_count,
        "total_rows": total_rows,
        "total_hdfs_bytes": total_hdfs,
        "total_iceberg_metadata_bytes": total_meta,
        "schemas": schema_summary,
    }

    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "hdfs_overall": hdfs_all,
        "host_disk": host_all,
        "tables": table_entries,
    }

    out_path.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"Written {out_path}")
    logger.info(f"Summary: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
