"""
Smoke tests for E2 master-data dims against a live Trino (E2 — 2026-05).

Verifies that, after the Gold DAG has run end-to-end at least once, the
five new ``iceberg.gold.dim_*`` tables exist, are non-empty, and obey the
R0.4 canonical-completeness contract (every row with a non-NULL
``<base>_canonical`` carries ``<base>_source`` AND ``<base>_version``).

These tests **require a running Trino**. They auto-skip when
``TRINO_HOST`` is not set so unit-test CI lanes don't fail on absence.

Per CLAUDE.md, this module imports the ``trino`` Python library directly
(NOT ``scrapers.base.trino_manager``) to avoid pulling in the
~1.5 GB scrapers package as a side effect.
"""

from __future__ import annotations

import os

import pytest


# ---------------------------------------------------------------------------
# Module-level skip when TRINO_HOST is missing
# ---------------------------------------------------------------------------
TRINO_HOST = os.environ.get("TRINO_HOST")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not TRINO_HOST,
        reason="TRINO_HOST not set — E2 smoke requires a live Trino "
        "(skipping in unit-only test runs).",
    ),
]


# ---------------------------------------------------------------------------
# Connection helper — same shape as utils.data_quality._get_conn
# ---------------------------------------------------------------------------

def _get_conn():
    """Open a Trino connection using TRINO_* env vars.

    Mirrors ``utils.data_quality._get_conn`` but is duplicated here so the
    test does not import from ``dags/utils`` (those modules expect Airflow
    on PYTHONPATH; this test is host-only).
    """
    import trino as trino_lib

    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    if password:
        port = int(os.environ.get("TRINO_PORT", 8443))
        return trino_lib.dbapi.connect(
            host=TRINO_HOST,
            port=port,
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
        )
    port = int(os.environ.get("TRINO_PORT", 8080))
    return trino_lib.dbapi.connect(
        host=TRINO_HOST, port=port, user=user, catalog="iceberg"
    )


def _scalar(sql: str) -> int:
    """Execute SQL and return the single scalar from the first row."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else 0
        finally:
            cur.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fixture-driven cases — one row per dim
# ---------------------------------------------------------------------------

# (table, expected_min_rows, expected_exact_rows or None)
_TABLE_CASES = [
    ("iceberg.gold.dim_venue",       1,    None),
    ("iceberg.gold.dim_referee",     1,    None),
    ("iceberg.gold.dim_standings",   1,    None),
    ("iceberg.gold.dim_competition", 5,    5),
    ("iceberg.gold.dim_season",      5,    5),
]


@pytest.mark.parametrize(
    "table,min_rows,exact_rows",
    _TABLE_CASES,
    ids=[c[0].split(".")[-1] for c in _TABLE_CASES],
)
def test_dim_table_row_counts(table, min_rows, exact_rows):
    """Each E2 dim table must exist and respect its row-count contract.

    * dim_competition → exactly 5 (one row per league in leagues.yaml)
    * dim_season      → exactly 5 (5-season window)
    * dim_venue/referee/standings → at least 1 row (depend on Bronze content)
    """
    count = _scalar(f"SELECT COUNT(*) FROM {table}")
    assert count >= min_rows, (
        f"{table} has {count} rows, expected >= {min_rows}"
    )
    if exact_rows is not None:
        assert count == exact_rows, (
            f"{table} has {count} rows, expected exactly {exact_rows}"
        )


# ---------------------------------------------------------------------------
# Canonical-completeness probe — one per dim that carries _canonical/_source/_version
# ---------------------------------------------------------------------------

# (table, base) where the columns are <base>_canonical / <base>_source / <base>_version.
# dim_standings has no _canonical triple — its source-of-truth lives in the
# team_id_source column (see SQL header comment) — so it is intentionally excluded.
_CANONICAL_CASES = [
    ("iceberg.gold.dim_venue",       "venue"),
    ("iceberg.gold.dim_referee",     "referee"),
    ("iceberg.gold.dim_competition", "competition"),
    ("iceberg.gold.dim_season",      "season"),
]


@pytest.mark.parametrize(
    "table,base",
    _CANONICAL_CASES,
    ids=[f"{c[0].split('.')[-1]}/{c[1]}" for c in _CANONICAL_CASES],
)
def test_canonical_completeness_zero_offenders(table, base):
    """R0.4: rows with non-NULL ``<base>_canonical`` MUST also carry non-NULL
    ``<base>_source`` AND ``<base>_version``."""
    sql = (
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE {base}_canonical IS NOT NULL "
        f"AND ({base}_source IS NULL OR {base}_version IS NULL)"
    )
    offenders = _scalar(sql)
    assert offenders == 0, (
        f"{table} has {offenders} row(s) violating canonical completeness "
        f"({base}_canonical present but {base}_source or {base}_version is NULL)"
    )
