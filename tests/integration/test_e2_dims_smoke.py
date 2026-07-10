"""
Smoke tests for the star-schema dims against a live Trino (#425).

Verifies that, after the Gold DAG has run end-to-end at least once, the
``iceberg.gold.dim_*`` tables exist, respect their row-count contracts
(config-driven dims match competitions.yaml exactly) and keep their PKs
unique — the core star-schema invariant.

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
    ("iceberg.gold.fct_standings",   1,    None),
    ("iceberg.gold.dim_manager",     1,    None),
    ("iceberg.gold.dim_player",      1,    None),
    ("iceberg.gold.dim_team",        1,    None),
    ("iceberg.gold.dim_match",       1,    None),
    # #425 + #913: config-driven dims mirror competitions.yaml.
    # dim_season now includes the single_year '2026' (INT-World Cup) → 11 rows.
    ("iceberg.gold.dim_competition", 9,    9),  # 6 in-scope (incl WC) + stubs
    ("iceberg.gold.dim_season",      11,   11),
]


@pytest.mark.parametrize(
    "table,min_rows,exact_rows",
    _TABLE_CASES,
    ids=[c[0].split(".")[-1] for c in _TABLE_CASES],
)
def test_dim_table_row_counts(table, min_rows, exact_rows):
    """Each dim table must exist and respect its row-count contract.

    * dim_competition → one row per competitions.yaml entry (incl. WC)
    * dim_season      → union of in-scope seasons (incl. single_year 2026, #913)
    * остальные dims  → at least 1 row (depend on Bronze content)
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
# PK-uniqueness probe — the core star-schema invariant (#425)
# ---------------------------------------------------------------------------

_PK_CASES = [
    ("iceberg.gold.dim_competition", "league"),
    ("iceberg.gold.dim_season",      "season"),
    ("iceberg.gold.dim_venue",       "venue_id"),
    ("iceberg.gold.dim_player",      "player_id"),
    ("iceberg.gold.dim_team",        "team_id"),
    ("iceberg.gold.dim_referee",     "referee_id"),
    ("iceberg.gold.dim_manager",     "manager_id"),
    ("iceberg.gold.dim_match",       "match_id"),
]


@pytest.mark.parametrize(
    "table,pk",
    _PK_CASES,
    ids=[c[0].split(".")[-1] for c in _PK_CASES],
)
def test_dim_pk_unique(table, pk):
    """#425 DoD: every star dim PK is unique (0 duplicate keys)."""
    dups = _scalar(f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM {table}")
    assert dups == 0, f"{table} has {dups} duplicate {pk} value(s)"
