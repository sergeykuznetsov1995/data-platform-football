"""
Cross-source matching smoke tests against a live Trino (MVP task 3.1).

Verifies that the medallion E1 cross-reference layer actually collapses
the *same* real-world entity (player / match / referee) under different
``source_id`` values onto a single ``canonical_id``:

* C1-C3 — ``silver.xref_player``: Saka / Salah / Haaland appear under
  multiple sources (FBref, Understat, WhoScored, SofaScore) → distinct
  ``canonical_id`` count must be exactly 1.  If a player happens to be
  present in only one source the test soft-skips (resolver coverage is
  data-dependent).
* C4 — ``silver.xref_match``: an Arsenal-vs-Chelsea match in the FBref
  spine is bridged by at least one non-FBref source with
  ``confidence='date_team_match'``.  Soft-skips when no FBref Arsenal-vs-Chelsea row
  exists.
* C5 — ``silver.xref_referee`` + ``gold.dim_referee``: Anthony Taylor's
  ``canonical_id`` is unique across sources (soft-skip when only one
  source carries the name) and ``dim_referee.referee_id`` is a true PK.
* C6 — Arteta manager xref: SKIPPED until R0.2 cross-source manager
  parser lands (xref_manager is FBref-only at E1, so a cross-source
  verification is structurally impossible).
* C7 — generic PK uniqueness invariant on every silver xref table —
  guards against the Mar 2026 fan-out regression.

These tests **require a running Trino**.  They auto-skip when
``TRINO_HOST`` is not set so unit-test CI lanes don't fail on absence.

Per CLAUDE.md the module imports the ``trino`` Python library directly
(NOT ``scrapers.base.trino_manager``) to avoid pulling in the
~1.5 GB scrapers package as an import side effect.
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
        reason="TRINO_HOST not set — cross-source smoke requires a live Trino "
        "(skipping in unit-only test runs).",
    ),
]


# ---------------------------------------------------------------------------
# Connection / query helpers — copied from test_e2_dims_smoke.py so the test
# stays self-contained (no import from dags/utils which expects Airflow on
# PYTHONPATH).
# ---------------------------------------------------------------------------

def _get_conn():
    """Open a Trino connection using TRINO_* env vars."""
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


def _query(sql: str) -> list[tuple]:
    """Execute SQL and return all rows as a list of tuples."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return list(cur.fetchall())
        finally:
            cur.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# C1-C3 — Player canonical_id collapse across sources
# ---------------------------------------------------------------------------

# Each entry is (display_name LIKE pattern, human-readable id).  We use the
# surname as the substring filter — the resolver normalises to ASCII so this
# works for both Cyrillic-faded and diacritic-stripped variants.
_PLAYER_CASES = [
    ("Saka", "Saka"),
    ("Salah", "Salah"),
    ("Haaland", "Haaland"),
]


@pytest.mark.parametrize(
    "name_like,case_id",
    _PLAYER_CASES,
    ids=[c[1] for c in _PLAYER_CASES],
)
def test_player_canonical_id_collapses_across_sources(name_like, case_id):
    """Soft-assert: when a player appears in >=2 sources, all rows must
    share a single ``canonical_id``.  If only one source carries the
    name, skip — that's a data-coverage gap, not a resolver defect."""
    sql = (
        "SELECT source, canonical_id "
        "FROM iceberg.silver.xref_player "
        f"WHERE display_name LIKE '%{name_like}%'"
    )
    rows = _query(sql)
    if not rows:
        pytest.skip(f"{case_id}: no rows found in xref_player (name not yet ingested)")

    present_sources: set[str] = {r[0] for r in rows}
    canonical_ids: set[str] = {r[1] for r in rows}

    if len(present_sources) < 2:
        pytest.skip(
            f"{case_id}: present in only {len(present_sources)} source "
            f"({sorted(present_sources)}) — cross-source verification N/A"
        )

    assert len(canonical_ids) == 1, (
        f"{case_id}: expected 1 canonical_id collapsed across sources, "
        f"got {len(canonical_ids)} across sources={sorted(present_sources)}: "
        f"canonical_ids={sorted(canonical_ids)}"
    )


# ---------------------------------------------------------------------------
# C4 — xref_match Arsenal-vs-Chelsea bridge
# ---------------------------------------------------------------------------

def test_xref_match_arsenal_chelsea_bridged_by_other_source():
    """Pick any Arsenal-vs-Chelsea match in the FBref spine; verify that
    its ``canonical_id`` shows up under at least one non-FBref source
    with ``confidence='date_team_match'`` (cascade bridge worked)."""
    spine_sql = (
        "SELECT canonical_id, display_name, league, season "
        "FROM iceberg.silver.xref_match "
        "WHERE source = 'fbref' "
        "AND ("
        "  (display_name LIKE '%Arsenal%' AND display_name LIKE '%Chelsea%')"
        ") "
        "LIMIT 5"
    )
    spine_rows = _query(spine_sql)
    if not spine_rows:
        pytest.skip(
            "No Arsenal-vs-Chelsea match found in FBref spine of xref_match "
            "(skipping cross-source bridge verification)"
        )

    # Try each candidate FBref-spine match until one shows a non-FBref bridge.
    bridge_diagnostics: list[str] = []
    for canonical_id, display_name, league, season in spine_rows:
        bridge_sql = (
            "SELECT source, confidence "
            "FROM iceberg.silver.xref_match "
            f"WHERE canonical_id = '{canonical_id}' "
            "AND source <> 'fbref' "
            "AND confidence = 'date_team_match'"
        )
        bridge_rows = _query(bridge_sql)
        if bridge_rows:
            bridged_sources = sorted({r[0] for r in bridge_rows})
            assert len(bridged_sources) >= 1, (
                f"canonical_id={canonical_id} ({display_name}, {league}/{season}): "
                f"expected >=1 non-FBref source bridged, got 0"
            )
            return  # success: at least one Arsenal-Chelsea match is bridged
        bridge_diagnostics.append(
            f"  - {canonical_id} ({display_name}, {league}/{season}): no non-FBref bridge"
        )

    pytest.fail(
        "Found {} Arsenal-vs-Chelsea FBref-spine match(es) but none bridged by another "
        "source with confidence='date_team_match':\n{}".format(
            len(spine_rows), "\n".join(bridge_diagnostics)
        )
    )


# ---------------------------------------------------------------------------
# C5 — Anthony Taylor referee canonical + dim_referee PK
# ---------------------------------------------------------------------------

def test_anthony_taylor_referee_canonical_id_unique():
    """Soft-assert: when 'Anthony Taylor' is matched in >=2 sources, his
    ``canonical_id`` collapses to a single value."""
    sql = (
        "SELECT source, canonical_id, display_name "
        "FROM iceberg.silver.xref_referee "
        "WHERE display_name LIKE '%Taylor%' "
        "AND display_name LIKE '%Anthony%'"
    )
    rows = _query(sql)
    if not rows:
        pytest.skip("Anthony Taylor not found in xref_referee (skipping)")

    present_sources: set[str] = {r[0] for r in rows}
    canonical_ids: set[str] = {r[1] for r in rows}

    if len(present_sources) < 2:
        pytest.skip(
            f"Anthony Taylor present in only {sorted(present_sources)} — "
            "cross-source verification N/A"
        )

    assert len(canonical_ids) == 1, (
        f"Anthony Taylor: expected 1 canonical_id, got {len(canonical_ids)} "
        f"across sources={sorted(present_sources)}: canonical_ids={sorted(canonical_ids)}"
    )


def test_dim_referee_referee_id_is_unique_pk():
    """Invariant: ``gold.dim_referee.referee_id`` is the PK and must be unique."""
    dup_count = _scalar(
        "SELECT COUNT(*) - COUNT(DISTINCT referee_id) FROM iceberg.gold.dim_referee"
    )
    assert dup_count == 0, (
        f"gold.dim_referee.referee_id PK violation: {dup_count} duplicate rows "
        "(expected 0). Check dim_referee.sql GROUP BY / DISTINCT semantics."
    )


# ---------------------------------------------------------------------------
# C6 — Arteta manager (DEFERRED until R0.2 cross-source manager parser)
# ---------------------------------------------------------------------------

@pytest.mark.skip(
    reason="manager xref FBref-only до R0.2 (cross-source manager parser); "
    "cross-source verify невозможен. См. docs/MVP_TASKS.md task 3.1"
)
def test_arteta_manager_canonical_id_collapses_across_sources():
    """Placeholder for cross-source Arteta verification.

    At E1, ``silver.xref_manager`` is FBref-only (R0.2 will add
    SofaScore/FotMob managers).  Until then there is structurally only
    one source per manager, so the cross-source assertion is a no-op.
    """
    pass


# ---------------------------------------------------------------------------
# C7 — PK uniqueness invariant on all silver xref tables
# ---------------------------------------------------------------------------

# (table, primary_key_columns_csv).  xref_match has a non-standard PK because
# the same FBref canonical_id appears under multiple sources (intentional —
# see dags/sql/silver/xref_match.sql:35-36).
_XREF_PK_CASES = [
    ("iceberg.silver.xref_player",  "source, source_id, league, season"),
    ("iceberg.silver.xref_team",    "source, source_id, league, season"),
    ("iceberg.silver.xref_referee", "source, source_id, league, season"),
    ("iceberg.silver.xref_manager", "source, source_id, league, season"),
    ("iceberg.silver.xref_match",   "canonical_id, source"),
]


@pytest.mark.parametrize(
    "table,pk_cols",
    _XREF_PK_CASES,
    ids=[c[0].split(".")[-1] for c in _XREF_PK_CASES],
)
def test_xref_table_pk_is_unique(table, pk_cols):
    """Each silver xref table must respect its declared PK — no duplicate
    rows on the PK tuple.  Guards against the Mar 2026 multi-season
    fan-out regression."""
    sql = (
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {pk_cols} FROM {table} "
        f"  GROUP BY {pk_cols} "
        f"  HAVING COUNT(*) > 1"
        f")"
    )
    dup_groups = _scalar(sql)
    assert dup_groups == 0, (
        f"{table} PK ({pk_cols}) violation: {dup_groups} duplicate group(s). "
        "Investigate fan-out from upstream (xref JOIN missing season predicate?)."
    )
