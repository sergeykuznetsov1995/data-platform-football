"""Render-smoke for ``dags/sql/silver/transfermarkt_coaches.sql.j2``.

Coach roster feeding gold.dim_manager nationality/dob enrichment (issue #434),
extended in issue #619 with a `manager_aliases` override CTE. This file freezes
the SQL contract — bronze source, name-normalize canonical_id (must match
xref_manager so TM coaches glue to the spine), dedup, columns, and the alias
branch (placeholder render + anti-fan-out guard + COALESCE override).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for _p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "transfermarkt_coaches.sql.j2"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


def _render(values: str = None) -> str:
    """Render the template with a synthetic manager-alias VALUES body."""
    from utils.medallion_config import render_sql_template

    if values is None:
        values = "('Le Bris, Régis', 'regis_le_bris', 'ENG-Premier League')"
    return render_sql_template(SQL_PATH, manager_aliases_values_sql=values)


pytestmark = pytest.mark.unit


class TestTransfermarktCoachesSql:

    def test_reads_bronze_coaches(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.transfermarkt_coaches" in sql

    def test_canonical_id_name_normalize_idiom(self):
        """Must use the SAME diacritic-stripping idiom as xref_manager.sql so a
        TM coach lands on the spine's canonical_id."""
        sql = _strip_comments(_read_sql())
        assert "NORMALIZE(b.name, NFD)" in sql, "must NORMALIZE(NFD) before slugging"
        assert r"\p{Mn}+" in sql, "must strip combining marks (\\p{Mn})"
        assert "canonical_id" in sql

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+coach_id",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY coach_id, league, season)"

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "coach_id", "canonical_id", "name", "role", "dob", "nationality",
            "current_club_id", "current_club_name", "_bronze_ingested_at",
            "league", "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"transfermarkt_coaches.sql.j2 must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS-wrapping is run_silver_transform's job)"
        )

    # --- issue #619: manager-alias override branch ---------------------------

    def test_has_manager_alias_placeholder(self):
        sql = _read_sql()
        assert "{{ manager_aliases_values_sql }}" in sql, (
            "template must expose the manager_aliases VALUES placeholder"
        )

    def test_alias_cte_present_with_coalesce_override(self):
        sql = _strip_comments(_read_sql())
        assert "manager_aliases AS (" in sql, "must define the manager_aliases CTE"
        # canonical_id = COALESCE(alias override, name-normalize fallback)
        assert re.search(
            r"COALESCE\(\s*a\.canonical_id", sql,
        ), "canonical_id must prefer the alias override via COALESCE(a.canonical_id, ...)"
        assert "LEFT JOIN manager_aliases" in sql, "must LEFT JOIN the alias CTE"

    def test_alias_cte_antifanout_group_by(self):
        """One row per (norm, league) with MAX(canonical_id) guards the LEFT
        JOIN against fan-out (#465)."""
        sql = _strip_comments(_read_sql())
        assert "MAX(canonical_id)" in sql, "alias CTE must collapse to MAX(canonical_id)"
        assert re.search(r"GROUP BY[\s\S]*league", sql), (
            "alias CTE must GROUP BY (norm, league)"
        )

    def test_renders_without_leftover_placeholder(self):
        rendered = _render()
        # The standalone VALUES placeholder line must be substituted. The inline
        # mention inside the comment header is intentionally NOT substituted by
        # render_sql_template (only standalone-line placeholders are), so we
        # check for a leftover standalone line, not any '{{' anywhere.
        standalone = [
            ln for ln in rendered.splitlines()
            if ln.strip() == "{{ manager_aliases_values_sql }}"
        ]
        assert not standalone, "the VALUES placeholder line must be substituted"
        assert "regis_le_bris" in rendered, "alias VALUES must be embedded"
        assert "VALUES" in rendered, "alias CTE must wrap the embedded rows in VALUES"


# ---------------------------------------------------------------------------
# Execution proof (Trino → DuckDB) — issue #619 orphan gluing is deterministic
# ---------------------------------------------------------------------------

_BRONZE_DDL = """
CREATE TABLE bronze.transfermarkt_coaches(
  coach_id VARCHAR, coach_slug VARCHAR, name VARCHAR, role VARCHAR, dob DATE,
  nationality VARCHAR, current_club_id VARCHAR, current_club_name VARCHAR,
  league VARCHAR, season VARCHAR, _ingested_at TIMESTAMP, _source VARCHAR,
  _entity_type VARCHAR, _batch_id VARCHAR)
"""

# Row 1 = orphan (TM surname-first "Le Bris, Régis" → 'le_bris_regis' by plain
# name-normalize); Row 2 = a coach that glues with no alias help.
_BRONZE_ROWS = """
INSERT INTO bronze.transfermarkt_coaches VALUES
 ('1','le-bris','Le Bris, Régis','Manager',DATE '1975-12-08','France','1',
  'Sunderland','ENG-Premier League','2526',NULL,'tm','coaches','b1'),
 ('2','arteta','Mikel Arteta','Manager',DATE '1982-03-26','Spain','11',
  'Arsenal','ENG-Premier League','2526',NULL,'tm','coaches','b1')
"""


def _run_coaches(alias_values: str):
    """Render → transpile → execute the coaches transform against a 2-row
    bronze fixture; return output rows keyed by coach_id."""
    sqlglot = pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")

    rendered = _render(alias_values)
    try:
        out = sqlglot.transpile(rendered, read="trino", write="duckdb")[0]
    except Exception as exc:  # pragma: no cover — sqlglot version drift
        pytest.skip(f"sqlglot could not translate coaches SQL: {exc}")
    out = out.replace("iceberg.bronze.", "bronze.").replace("iceberg.silver.", "silver.")
    # DuckDB lacks NORMALIZE(x, NFD); swap for strip_accents(x) — the \p{Mn}
    # strip then no-ops (RE2 supports \p{Mn}). Same family of dialect shims as
    # the iceberg.* schema rewrites above.
    out = re.sub(r"(?i)normalize\(\s*([^,]+?)\s*,\s*NFD\s*\)", r"strip_accents(\1)", out)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute(_BRONZE_DDL)
    con.execute(_BRONZE_ROWS)
    try:
        cur = con.execute(out)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DuckDB could not execute translated coaches SQL: {exc}")
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}


class TestTransfermarktCoachesAliasExec:

    def test_alias_glues_orphan_to_spine_id(self):
        """The surname-first TM spelling is remapped onto the FBref spine id;
        a normally-spelled coach keeps the plain name-normalize id."""
        rows = _run_coaches(
            "('Le Bris, Régis', 'regis_le_bris', 'ENG-Premier League')"
        )
        assert rows["1"]["canonical_id"] == "regis_le_bris", rows["1"]
        assert rows["2"]["canonical_id"] == "mikel_arteta", rows["2"]

    def test_sentinel_does_not_override(self):
        """The empty-config sentinel row matches nothing — every coach falls
        back to plain name-normalize (pre-#619 behaviour preserved)."""
        rows = _run_coaches(
            "('__no_manager_alias__', '__no_manager_alias__', '__none__')"
        )
        assert rows["1"]["canonical_id"] == "le_bris_regis", rows["1"]
        assert rows["2"]["canonical_id"] == "mikel_arteta", rows["2"]
