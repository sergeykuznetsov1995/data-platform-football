"""
Unit tests for ``dags/sql/silver/xref_referee.sql.j2`` — structural / logical (#143).

Strategy mirrors ``test_xref_team_sql.py``: the pure-SQL ``xref_referee.sql``
became a Jinja template with a single ``{{ referee_aliases_values_sql }}``
placeholder (issue #143 — curated cross-source identity, no fuzzy). We verify:

1. **Structural invariants** of the raw template (2 sources, aliases CTE,
   league predicate, confidence CASE name_alias/orphan, orphan prefixes,
   NFD diacritic fold, pure SELECT, Bronze-only reads).
2. **Render-time correctness**: medallion_config hydrates the template into
   stable Trino SQL (no leftover standalone placeholder, real VALUES embedded,
   both Bronze tables referenced).

No DuckDB / sqlglot — regex over the template is enough.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_referee.sql.j2"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)


def _read_template() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _aliases_cte(sql: str) -> str:
    """Slice the ``aliases`` CTE source (from ``aliases AS`` up to the next
    CTE ``raw_refs AS``) for #465 guard assertions. Since the #930 cutover
    the first CTE is ``league_map`` — ``aliases`` is no longer WITH-adjacent."""
    start = sql.index("\naliases AS")
    end = sql.index("raw_refs AS")
    return sql[start:end]


pytestmark = pytest.mark.unit


class TestXrefRefereeTemplateStructure:
    """Regex/keyword sanity over the raw ``xref_referee.sql.j2``."""

    def test_template_uses_aliases_placeholder(self):
        assert "{{ referee_aliases_values_sql }}" in _read_template(), (
            "xref_referee.sql.j2 must declare the {{ referee_aliases_values_sql }} "
            "placeholder consumed by medallion_config.render_sql_template()"
        )

    def test_three_sources_fbref_matchhistory_fotmob(self):
        sql = _read_template().lower()
        assert "'fbref'" in sql and "'matchhistory'" in sql and "'fotmob'" in sql

    def test_fotmob_source_branch(self):
        """FotMob (issue #270; #930 cutover): 'fotmob' AS source reading
        match_facts_json from the native fotmob_match_payloads_current view."""
        sql = _read_template().lower()
        assert re.search(r"'fotmob'\s+as\s+source", sql)
        assert "iceberg.bronze.fotmob_match_payloads_current" in sql
        assert "iceberg.bronze.fotmob_match_details" not in sql, (
            "legacy bronze.fotmob_match_details must be gone after the #930 "
            "native cutover"
        )
        assert "$.infobox.referee.text" in sql

    def test_fotmob_league_map_and_season_key(self):
        """#930: native payloads carry competition_id/source_season_key, not
        league/season — the branch must reconstruct league via the league_map
        CTE (varchar competition_id → CAST bigint) and the season slug from
        the source_season_key year-start."""
        sql = _read_template().lower()
        assert re.search(r"league_map\s*\(\s*competition_id\s*,\s*league\s*\)\s+as", sql), (
            "league_map CTE (canonical FotMob registry map) must exist"
        )
        assert re.search(
            r"cast\s*\(\s*p\.competition_id\s+as\s+bigint\s*\)", sql
        ), "payloads competition_id is varchar — JOIN to league_map needs CAST"
        assert "source_season_key" in sql
        assert re.search(r"substr\s*\(\s*p\.source_season_key\s*,\s*1\s*,\s*4\s*\)", sql), (
            "season year must come from the first 4 chars of source_season_key "
            "(cutover map §2.2 — never from the key's shape)"
        )

    def test_no_other_sources(self):
        sql = _read_template().lower()
        for forbidden in ["'understat'", "'whoscored'", "'sofascore'",
                          "'clubelo'", "'espn'"]:
            pattern = re.compile(re.escape(forbidden) + r"\s+as\s+source", re.I)
            assert not pattern.search(sql), (
                f"{forbidden} must not be a source in xref_referee — only "
                "fbref + matchhistory + fotmob carry referee data (issue #270)"
            )

    def test_has_aliases_cte(self):
        assert re.search(r"\baliases\s+as\s*\(", _read_template(), re.I)

    def test_league_predicate_in_join(self):
        """Issue #148 league guard must be wired into the alias JOIN."""
        assert re.search(r"a\.league\s*=\s*rt\.league", _read_template(), re.I)

    def test_confidence_is_name_alias_or_orphan(self):
        sql = _read_template().lower()
        assert "'name_alias'" in sql and "'orphan'" in sql
        assert "'name_normalize'" not in sql, (
            "old pure-SQL confidence label must be gone after the #143 refactor"
        )

    def test_orphan_prefixes(self):
        sql = _read_template().lower()
        assert "'fb_ref_'" in sql and "'mh_ref_'" in sql
        assert "'fm_ref_'" in sql  # FotMob orphan fallback (issue #270)

    def test_aliases_cte_guards_join_key(self):
        """#465: the aliases CTE must collapse to ONE row per (norm, league)
        — GROUP BY + MAX(canonical_*) guard, precedent dim_match.venue_aliases
        (#425). No-DuckDB safety net for test_xref_referee_logic.py (whose
        fixture converts transpile errors into pytest.skip)."""
        cte = _aliases_cte(_read_template())
        assert re.search(r"GROUP\s+BY", cte, re.I), (
            "aliases CTE must GROUP BY the (norm, league) join key (#465)"
        )
        assert re.search(r"MAX\s*\(\s*canonical_id\s*\)", cte, re.I), (
            "aliases CTE must aggregate canonical_id with MAX (#465)"
        )
        assert re.search(r"MAX\s*\(\s*canonical_name\s*\)", cte, re.I), (
            "aliases CTE must aggregate canonical_name with MAX (#465)"
        )

    def test_aliases_group_by_repeats_norm_expr_not_alias(self):
        """Trino cannot reference a same-level SELECT alias in GROUP BY
        (feedback_trino_window_alias_duckdb_gap) — the norm expression must be
        repeated verbatim, so its fingerprint appears exactly twice."""
        cte = _aliases_cte(_read_template())
        assert len(re.findall(r"normalize\(lower\(raw_name", cte, re.I)) == 2, (
            "norm expression must appear twice in the aliases CTE "
            "(SELECT list + GROUP BY) — Trino rejects `GROUP BY norm`"
        )
        assert not re.search(r"GROUP\s+BY\s+norm\b", cte, re.I), (
            "GROUP BY must repeat the expression, not the `norm` alias "
            "(COLUMN_NOT_FOUND on live Trino; DuckDB masks this)"
        )

    def test_diacritic_fold_idiom(self):
        r"""NFD + \p{Mn} strip — same fold as xref_team/xref_manager (issue #215)."""
        sql = _read_template()
        assert "NFD" in sql
        assert r"\p{Mn}" in sql

    def test_match_score_null(self):
        assert re.search(r"CAST\(NULL AS double\)", _read_template(), re.I)

    def test_pure_select_no_create_table(self):
        """No DDL in the executable SQL (header comment may mention CTAS)."""
        code = "\n".join(
            line for line in _read_template().splitlines()
            if not line.lstrip().startswith("--")
        ).lower()
        assert "create table" not in code and "create or replace" not in code

    def test_reads_bronze_only(self):
        sql = _read_template().lower()
        assert "iceberg.bronze.fbref_schedule" in sql
        assert "iceberg.bronze.matchhistory_results" in sql
        assert "iceberg.gold." not in sql


class TestXrefRefereeRender:
    """Render the template via the real shipped referee_aliases.yaml."""

    def _render(self) -> str:
        from utils import medallion_config as mc
        mc.reset_cache()
        return mc.render_sql_template(
            SQL_PATH,
            referee_aliases_values_sql=mc.get_referee_alias_sql_values(
                with_canonical_id=True, with_league=True
            ),
        )

    def test_no_leftover_standalone_placeholder(self):
        rendered = self._render()
        assert not re.search(r"^[ \t]*\{\{\s*\w+\s*\}\}[ \t]*$", rendered, re.M)

    def test_embeds_real_alias_values(self):
        rendered = self._render()
        assert "ref_michael_oliver" in rendered
        assert "'M Oliver'" in rendered  # MatchHistory initial form merges to canonical

    def test_references_both_bronze_tables(self):
        rendered = self._render()
        assert "iceberg.bronze.fbref_schedule" in rendered
        assert "iceberg.bronze.matchhistory_results" in rendered
        assert "iceberg.bronze.fotmob_match_payloads_current" in rendered
