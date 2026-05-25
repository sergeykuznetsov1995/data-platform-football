#!/usr/bin/env python3
"""Audit usage of iceberg.gold.* tables across DAG SQL, Superset and OpenMetadata.

Spec: docs/research/R0.5_usage_tracker.md
Output: data/audit/gold_usage_<YYYY-MM-DD>.json (UTC date).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger('audit_gold_usage')


# ---------------------------------------------------------------------------
# Trino: list Gold tables
# ---------------------------------------------------------------------------

def _get_trino_connection(
    host: str | None = None,
    port: int | None = None,
    catalog: str = 'iceberg',
):
    """Create a Trino DBAPI connection (mirrors dags/utils/silver_tasks.py)."""
    import trino as trino_lib  # local import — keep top-level light

    host = host or os.environ.get('TRINO_HOST', 'localhost')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    if password:
        port = port or int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
        )

    port = port or int(os.environ.get('TRINO_PORT', 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
    )


def list_gold_tables(trino_conn) -> tuple[list[str], str]:
    """Return (gold_table_names, method) where method is 'shows_tables' or 'filenames_fallback'."""
    if trino_conn is not None:
        try:
            cur = trino_conn.cursor()
            cur.execute('SHOW TABLES FROM iceberg.gold')
            rows = cur.fetchall()
            tables = sorted({r[0] for r in rows if r and r[0]})
            if tables:
                return tables, 'shows_tables'
            logger.warning('SHOW TABLES FROM iceberg.gold returned 0 rows; falling back to filenames')
        except Exception as exc:
            logger.warning('Trino SHOW TABLES failed (%s) — falling back to dags/sql/gold/*.sql filenames', exc)

    # Fallback: filenames in dags/sql/gold/*.sql
    repo_root = Path(__file__).resolve().parent.parent
    gold_dir = repo_root / 'dags' / 'sql' / 'gold'
    if not gold_dir.is_dir():
        logger.warning('Fallback dir %s does not exist — returning empty list', gold_dir)
        return [], 'filenames_fallback'
    names = sorted({p.stem for p in gold_dir.glob('*.sql') if p.is_file()})
    # Filter helper "_empty" stubs and dotted helpers — keep only base table names.
    # Actual Gold tables in the schema do not have _empty suffix, but stub files
    # like fct_player_unavailable_empty.sql produce real tables when bronze missing.
    # Keep them as-is — output will diff vs Trino if/when available.
    return names, 'filenames_fallback'


# ---------------------------------------------------------------------------
# DAG SQL scan
# ---------------------------------------------------------------------------

_LINE_COMMENT_RE = re.compile(r'--[^\n]*')
_BLOCK_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)
_GOLD_REF_RE = re.compile(r'\b(?:iceberg\.)?gold\.([a-z_][a-z0-9_]*)\b', re.IGNORECASE)


def _strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* ... */ block comments."""
    sql = _BLOCK_COMMENT_RE.sub(' ', sql)
    sql = _LINE_COMMENT_RE.sub('', sql)
    return sql


def scan_dag_sql_files(repo_root: Path) -> dict[str, int]:
    """Walk dags/sql/**/*.sql and count files referencing each gold.* table."""
    counts: dict[str, int] = {}
    sql_root = repo_root / 'dags' / 'sql'
    if not sql_root.is_dir():
        logger.warning('SQL root %s does not exist', sql_root)
        return counts
    for path in sql_root.rglob('*.sql'):
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
        except OSError as exc:
            logger.warning('Cannot read %s: %s', path, exc)
            continue
        cleaned = _strip_sql_comments(text)
        # Per-file dedup: a file referencing the same table 5 times = 1 ref.
        tables_in_file = {m.group(1).lower() for m in _GOLD_REF_RE.finditer(cleaned)}
        for tbl in tables_in_file:
            counts[tbl] = counts.get(tbl, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Superset REST
# ---------------------------------------------------------------------------

def superset_login(base_url: str, username: str, password: str | None) -> str | None:
    """POST /api/v1/security/login → access_token JWT, or None on failure / no creds."""
    if not password:
        return None
    url = f"{base_url.rstrip('/')}/api/v1/security/login"
    try:
        resp = requests.post(
            url,
            json={'username': username, 'password': password, 'provider': 'db', 'refresh': True},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get('access_token')
    except Exception as exc:
        logger.warning('Superset login failed: %s', exc)
        return None


def superset_list_datasets(base_url: str, token: str) -> dict[str, int]:
    """GET /api/v1/dataset/?q=(page_size:100,page:N) → {table_name: dataset_id} for gold.* tables."""
    headers = {'Authorization': f'Bearer {token}'}
    out: dict[str, int] = {}
    page = 0
    page_size = 100
    while True:
        q = f'(page:{page},page_size:{page_size})'
        url = f"{base_url.rstrip('/')}/api/v1/dataset/?q={urllib.parse.quote(q, safe='(),:')}"
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning('Superset dataset list page=%s failed: %s', page, exc)
            break
        body = resp.json() or {}
        result = body.get('result') or []
        if not result:
            break
        for ds in result:
            schema = (ds.get('schema') or '').lower()
            db = ds.get('database') or {}
            db_name = (db.get('database_name') or '').lower()
            table_name = ds.get('table_name')
            if not table_name:
                continue
            if schema == 'gold' or db_name == 'trino_iceberg':
                if schema == 'gold':
                    out[table_name] = ds.get('id')
        if len(result) < page_size:
            break
        page += 1
        if page > 50:  # safety stop
            logger.warning('Superset dataset paging exceeded 50 pages — bailing')
            break
    return out


def superset_chart_count(base_url: str, token: str, dataset_id: int) -> int:
    """GET /api/v1/chart/?q=(filters:!((col:datasource_id,opr:eq,value:N))) → len(result)."""
    headers = {'Authorization': f'Bearer {token}'}
    q = f'(filters:!((col:datasource_id,opr:eq,value:{dataset_id})),page_size:100)'
    url = f"{base_url.rstrip('/')}/api/v1/chart/?q={urllib.parse.quote(q, safe='(),:!')}"
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        body = resp.json() or {}
        # Prefer authoritative `count` if present, else len(result).
        if 'count' in body and isinstance(body['count'], int):
            return body['count']
        return len(body.get('result') or [])
    except Exception as exc:
        logger.warning('Superset chart count for dataset_id=%s failed: %s', dataset_id, exc)
        return 0


# ---------------------------------------------------------------------------
# OpenMetadata REST
# ---------------------------------------------------------------------------

def openmetadata_downstream_count(base_url: str, jwt_token: str | None, fqn: str) -> int:
    """GET /api/v1/lineage/table/name/<fqn>?downstreamDepth=10 → count of downstreamEdges."""
    if not jwt_token:
        return 0
    encoded_fqn = urllib.parse.quote(fqn, safe='')
    url = f"{base_url.rstrip('/')}/api/v1/lineage/table/name/{encoded_fqn}?downstreamDepth=10"
    headers = {'Authorization': f'Bearer {jwt_token}'}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()
        body = resp.json() or {}
        edges = body.get('downstreamEdges') or []
        return len(edges)
    except Exception as exc:
        logger.warning('OpenMetadata lineage for %s failed: %s', fqn, exc)
        return 0


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

def verdict(dag_refs: int, chart_count: int, downstream_count: int) -> str:
    if dag_refs > 0 or chart_count > 0:
        return 'active'
    if downstream_count > 0:
        return 'stale'
    return 'unused'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Audit usage of iceberg.gold.* tables.')
    parser.add_argument('--output-dir', default='data/audit', help='Output directory for JSON snapshot.')
    parser.add_argument('--trino-host', default=os.environ.get('TRINO_HOST', 'localhost'))
    parser.add_argument(
        '--trino-port',
        type=int,
        default=int(os.environ.get('TRINO_PORT', 8082)),
    )
    parser.add_argument(
        '--repo-root',
        default=str(Path(__file__).resolve().parent.parent),
        help='Repository root (auto-detected from script location).',
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = Path(args.repo_root).resolve()

    # 1) Trino connection (best-effort)
    trino_conn = None
    try:
        trino_conn = _get_trino_connection(host=args.trino_host, port=args.trino_port)
    except Exception as exc:
        logger.warning('Trino connect failed (%s) — using filename fallback', exc)

    logger.info('Listing Gold tables...')
    tables, method = list_gold_tables(trino_conn)
    logger.info('  %d Gold tables (method=%s)', len(tables), method)

    # 2) DAG SQL scan
    logger.info('Scanning DAG SQL files...')
    dag_refs = scan_dag_sql_files(repo_root)
    logger.info('  %d unique gold.* references in dags/sql', len(dag_refs))

    # 3) Superset
    superset_base = os.environ.get('SUPERSET_BASE_URL', 'http://localhost:8088')
    superset_user = os.environ.get('SUPERSET_ADMIN_USER', 'admin')
    superset_pass = os.environ.get('SUPERSET_ADMIN_PASSWORD')
    superset_datasets: dict[str, int] = {}
    superset_token: str | None = None
    if superset_pass:
        logger.info('Querying Superset...')
        superset_token = superset_login(superset_base, superset_user, superset_pass)
        if superset_token:
            superset_datasets = superset_list_datasets(superset_base, superset_token)
            logger.info('  %d Superset gold.* datasets', len(superset_datasets))
        else:
            logger.warning('Superset login returned no token — skipping')
    else:
        logger.warning('SUPERSET_ADMIN_PASSWORD not set — skipping Superset')

    # 4) OpenMetadata
    om_base = os.environ.get('OM_BASE_URL', 'http://localhost:8585')
    om_jwt = os.environ.get('OM_JWT_TOKEN')
    om_service = os.environ.get('OM_SERVICE_NAME', 'trino_iceberg')
    if not om_jwt:
        logger.warning('OM_JWT_TOKEN not set — skipping OpenMetadata')

    # 5) Per-table assemble
    summary: dict[str, int] = {'active': 0, 'stale': 0, 'unused': 0}
    rows: list[dict[str, Any]] = []
    for tbl in tables:
        d_refs = int(dag_refs.get(tbl, 0))
        ds_id = superset_datasets.get(tbl)
        c_count = 0
        if ds_id is not None and superset_token:
            c_count = superset_chart_count(superset_base, superset_token, ds_id)
        fqn = f'iceberg.gold.{tbl}'
        om_fqn = f'{om_service}.iceberg.gold.{tbl}'
        ds_count = openmetadata_downstream_count(om_base, om_jwt, om_fqn)
        v = verdict(d_refs, c_count, ds_count)
        summary[v] = summary.get(v, 0) + 1
        rows.append({
            'table': tbl,
            'fqn': fqn,
            'dag_sql_refs': d_refs,
            'superset_dataset_id': ds_id if ds_id is not None else None,
            'superset_chart_count': c_count,
            'openmetadata_downstream_count': ds_count,
            'verdict': v,
        })

    # Trim summary keys with 0 values to match reference (keeps active/unused only when stale absent).
    summary_trimmed = {k: v for k, v in summary.items() if v > 0}

    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'trino_method': method,
        'trino_query_method': 'dag_sql_scan',
        'tables': rows,
        'summary': summary_trimmed,
    }

    # 6) Write output
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = (repo_root / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    out_path = output_dir / f'gold_usage_{today}.json'
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    logger.info('Written %s', out_path)
    logger.info('Summary: %s', summary_trimmed)
    return 0


if __name__ == '__main__':
    sys.exit(main())
