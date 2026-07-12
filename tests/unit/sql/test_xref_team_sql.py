"""
Unit tests for ``dags/sql/silver/xref_team.sql.j2`` — structural / logical (T5/E1).

Strategy
--------
T2 produced a pure-SQL CTAS with a single ``{{ team_aliases_values_sql }}``
Jinja-style placeholder. We verify two things WITHOUT a Trino/DuckDB engine:

1. **Structural invariants** of the raw template: regex/keyword checks that
   every documented testable invariant in the SQL header survives editor
   refactors (8 sources unioned, orphan prefixes per source, season cast,
   canonical_id normalize pattern, no Gold-era references).
2. **Render-time correctness**: the medallion_config loader hydrates the
   template into syntactically-stable Trino SQL (no leftover placeholders,
   real alias VALUES embedded, expected Bronze tables referenced).

Approach mirrors ``test_dim_competition_render.py`` (E2 dim renderer pattern).
No DuckDB / sqlglot dependencies — those drag the test runtime up and the
template logic is trivial enough to reason about with regex.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_team.sql.j2"

# ``dags/utils/medallion_config.py`` imports as ``utils.medallion_config``
# inside Airflow because dags/ is on PYTHONPATH. Replicate that here.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

# Point medallion_config at the host-side configs/medallion/ dir before the
# module is imported for the first time.
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)


def _read_template() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _aliases_cte(sql: str) -> str:
    """Slice the ``aliases`` CTE source (from ``WITH aliases AS`` up to the
    next CTE ``raw_teams AS``) for #465 guard assertions."""
    start = sql.index("WITH aliases AS")
    end = sql.index("raw_teams AS")
    return sql[start:end]


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Structural / regex tests — operate on the un-rendered .sql.j2 source.
# ---------------------------------------------------------------------------

class TestXrefTeamTemplateStructure:
    """Regex/keyword sanity over the raw ``xref_team.sql.j2``."""

    def test_template_uses_aliases_placeholder(self):
        """Template must use the documented placeholder name."""
        sql = _read_template()
        assert "{{ team_aliases_values_sql }}" in sql, (
            "xref_team.sql.j2 must declare the {{ team_aliases_values_sql }} "
            "placeholder consumed by medallion_config.render_sql_template()"
        )

    def test_all_8_sources_unioned(self):
        """All 11 documented sources appear as quoted source labels."""
        sql = _read_template().lower()
        expected_sources = {
            "fbref", "understat", "whoscored", "sofascore",
            "fotmob", "matchhistory", "clubelo", "espn",
            "transfermarkt", "capology", "sofifa",
        }
        for src in expected_sources:
            assert f"'{src}'" in sql, (
                f"source {src!r} missing as quoted literal in xref_team.sql.j2"
            )

    def test_canonical_id_uses_explicit_slug(self):
        """Resolved branch returns the explicit YAML slug, NOT a name-derived
        one (issue #141 — identity must not come from the display name)."""
        sql = _read_template()
        # matched branch: `WHEN canonical_id IS NOT NULL THEN canonical_id`
        pattern = re.compile(
            r"WHEN\s+canonical_id\s+IS\s+NOT\s+NULL\s+THEN\s+canonical_id",
            re.IGNORECASE,
        )
        assert pattern.search(sql), (
            "expected `WHEN canonical_id IS NOT NULL THEN canonical_id` — the "
            "resolved canonical_id must be the explicit YAML slug"
        )
        # The old name-derived idiom must be gone for the matched branch.
        assert not re.search(
            r"REGEXP_REPLACE\s*\(\s*canonical_name", sql, re.IGNORECASE
        ), "canonical_id must no longer be derived from canonical_name"

    def test_orphan_id_still_derived_from_source_id(self):
        """Orphan fallback slug is unchanged: LOWER(REGEXP_REPLACE(source_id, ...))."""
        sql = _read_template()
        assert re.search(
            r"LOWER\s*\(\s*REGEXP_REPLACE\s*\(\s*source_id", sql, re.IGNORECASE
        ), "orphan canonical_id must stay source_id-derived (stable for dim_team)"

    def test_alias_join_uses_normalisation(self):
        """Stage 3: alias JOIN matches on a normalised key, not the raw name."""
        sql = _read_template()
        # aliases CTE precomputes a `norm` column; JOIN matches on `a.norm`.
        assert re.search(r"\bAS\s+norm\b", sql, re.IGNORECASE), (
            "aliases CTE must expose a precomputed `norm` join key"
        )
        assert re.search(r"a\.norm\s*=", sql, re.IGNORECASE), (
            "JOIN must match on the normalised key `a.norm = ...`"
        )
        # Normalisation building blocks: lower + diacritic strip + suffix strip.
        assert "normalize(lower(" in sql.lower(), "expected normalize(lower(...))"
        assert r"\p{Mn}" in sql, "expected diacritic (combining-mark) strip"
        assert re.search(r"\(fc\|afc\|cf\)", sql, re.IGNORECASE), (
            "expected trailing legal-form suffix strip (fc|afc|cf)"
        )

    def test_alias_cte_has_four_columns(self):
        """aliases CTE binds the 4-tuple VALUES (issue #148): adds `league`."""
        sql = _read_template()
        assert re.search(
            r"AS\s+t\s*\(\s*raw_name\s*,\s*canonical_name\s*,\s*"
            r"canonical_id\s*,\s*league\s*\)",
            sql,
            re.IGNORECASE,
        ), "aliases CTE must bind t(raw_name, canonical_name, canonical_id, league)"

    def test_alias_join_guards_on_league(self):
        """JOIN disambiguates by league (issue #148): `a.league = rt.league`."""
        sql = _read_template()
        assert re.search(
            r"a\.league\s*=\s*rt\.league", sql, re.IGNORECASE
        ), "alias JOIN must guard on `a.league = rt.league`"

    def test_orphan_id_prefix_per_source(self):
        """Each source has a short orphan prefix (fb_/us_/ws_/.../tm_/cap_)."""
        sql = _read_template()
        for prefix in ["fb_", "us_", "ws_", "ss_", "fm_", "mh_", "ce_", "es_",
                       "tm_", "cap_", "sf_"]:
            assert f"'{prefix}'" in sql, (
                f"orphan prefix {prefix!r} missing — orphans must be uniquely "
                "namespaced per-source so canonical_id collisions are impossible"
            )

    def test_aliases_cte_guards_join_key(self):
        """#465: the aliases CTE must collapse to ONE row per (norm, league)
        — GROUP BY + MAX(canonical_*) guard, precedent dim_match.venue_aliases
        (#425) — so the alias JOIN can never fan out raw_teams rows."""
        cte = _aliases_cte(_read_template())
        assert re.search(r"GROUP\s+BY", cte, re.IGNORECASE), (
            "aliases CTE must GROUP BY the (norm, league) join key (#465)"
        )
        assert re.search(r"MAX\s*\(\s*canonical_id\s*\)", cte, re.IGNORECASE), (
            "aliases CTE must aggregate canonical_id with MAX (#465)"
        )
        assert re.search(r"MAX\s*\(\s*canonical_name\s*\)", cte, re.IGNORECASE), (
            "aliases CTE must aggregate canonical_name with MAX (#465)"
        )

    def test_aliases_group_by_repeats_norm_expr_not_alias(self):
        """Trino cannot reference a same-level SELECT alias in GROUP BY
        (feedback_trino_window_alias_duckdb_gap) — the norm expression must be
        repeated verbatim, so the fc|afc|cf strip appears exactly twice."""
        cte = _aliases_cte(_read_template())
        assert len(re.findall(r"\(fc\|afc\|cf\)", cte)) == 2, (
            "norm expression must appear twice in the aliases CTE "
            "(SELECT list + GROUP BY) — Trino rejects `GROUP BY norm`"
        )
        assert not re.search(r"GROUP\s+BY\s+norm\b", cte, re.IGNORECASE), (
            "GROUP BY must repeat the expression, not the `norm` alias "
            "(COLUMN_NOT_FOUND on live Trino; DuckDB masks this)"
        )

    def test_fbref_match_page_branches_present(self):
        """#445: raw_teams must also enumerate the FBref match-page name
        universe — match pages carry FULL club names ('Wolverhampton
        Wanderers') where the schedule carries SHORT ones ('Wolves'), and
        name-keyed Gold JOINs must resolve either spelling."""
        sql = _read_template()
        for tbl in [
            "iceberg.bronze.fbref_match_player_stats",
            "iceberg.bronze.fbref_match_events",
        ]:
            assert re.search(
                rf"FROM\s+{re.escape(tbl)}\s+WHERE\s+team\s+IS\s+NOT\s+NULL",
                sql,
                re.IGNORECASE,
            ), (
                f"raw_teams must read {tbl!r} with `WHERE team IS NOT NULL` "
                "(#445 — full-name universe from match pages)"
            )
        # Every BIGINT-season branch must keep the year-start→slug expression
        # (#404): 4 fbref (2 schedule + 2 match-page) + 2 fotmob +
        # 2 matchhistory = 8 — a branch without it would silently emit
        # unjoinable season values.
        assert len(re.findall(r"LPAD\(CAST\(MOD\(season, 100\)", sql)) >= 8, (
            "expected the season-slug expression on all 4 fbref branches "
            "(schedule + match-page) plus fotmob/matchhistory (#445/#404)"
        )

    def test_season_cast_to_varchar(self):
        """FBref/MatchHistory/FotMob seasons are BIGINT in Bronze; need CAST."""
        sql = _read_template()
        # Trino tolerates either case; we accept both for editor robustness.
        assert (
            "CAST(season AS varchar)" in sql
            or "CAST(season as varchar)" in sql
        ), (
            "expected `CAST(season AS varchar)` to harmonise BIGINT↔VARCHAR "
            "season columns across the 8-source UNION"
        )

    def test_no_old_pattern_using_match_enriched(self):
        """The refactor goal — read Bronze, not Silver Gold-era tables."""
        sql = _read_template()
        assert "silver.fbref_match_enriched" not in sql, (
            "xref_team must read from Bronze; reading silver.fbref_match_enriched "
            "is a Gold-era pattern that creates a circular dependency"
        )

    def test_pk_columns_present(self):
        """Documented PK = (source, source_id, league, season) — all selected."""
        sql_lower = _read_template().lower()
        for col in ["source", "source_id", "league", "season"]:
            assert col in sql_lower, (
                f"PK column {col!r} missing from xref_team.sql.j2"
            )

    def test_confidence_branches_match_doc(self):
        """confidence ∈ {'name_alias','orphan'} as documented in the header."""
        sql = _read_template()
        assert "'name_alias'" in sql, "missing 'name_alias' confidence literal"
        assert "'orphan'" in sql, "missing 'orphan' confidence literal"

    def test_match_score_is_null(self):
        """match_score is always NULL for team xref (fuzzy lives in T3 player)."""
        sql = _read_template()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "expected CAST(NULL AS double) for match_score (no fuzzy in team xref)"


# ---------------------------------------------------------------------------
# Render-time tests — call into medallion_config to substitute the placeholder.
# ---------------------------------------------------------------------------

class TestXrefTeamRendered:
    """Hydrate the template via medallion_config and assert structure."""

    @pytest.fixture(scope="class")
    def rendered(self) -> str:
        from utils.medallion_config import (  # type: ignore[import-not-found]
            _escape_sql_string,
            get_in_scope_competitions,
            get_team_alias_sql_values,
            render_sql_template,
            reset_cache,
        )

        # Fresh read so a previous test cannot poison the lru_cache state.
        reset_cache()
        # xref_team consumes 4-column tuples (issue #148) — match the DAG call.
        values = get_team_alias_sql_values(with_canonical_id=True, with_league=True)
        # ClubElo league scoping mirrors dag_transform_xref.py (in_scope leagues).
        clubelo_leagues = ', '.join(
            f"'{_escape_sql_string(lg)}'" for lg in get_in_scope_competitions()
        )
        return render_sql_template(
            SQL_PATH,
            team_aliases_values_sql=values,
            clubelo_in_scope_leagues=clubelo_leagues,
        )

    def test_render_leaves_no_active_jinja_placeholders(self, rendered: str):
        """No leftover *active* `{{ ... }}` placeholder after substitution.

        The documented contract of ``render_sql_template`` is that ONLY
        standalone-line placeholders are substituted; ``{{ name }}`` inside
        a ``-- ...`` comment is intentionally preserved (see medallion_config
        docstring). We therefore strip ``-- ...`` comment lines before
        scanning for leftover placeholders.
        """
        non_comment = "\n".join(
            line for line in rendered.splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "{{ " not in non_comment, (
            "render_sql_template left an unresolved Jinja placeholder in "
            "non-comment SQL — substitution failed"
        )
        assert "{{team" not in non_comment, (
            "tight {{team... placeholder leaked into non-comment SQL"
        )

    def test_render_emits_values_clause(self, rendered: str):
        """Rendered SQL must contain the inline VALUES body of alias pairs."""
        assert "VALUES" in rendered.upper(), (
            "rendered SQL is missing a VALUES clause"
        )
        # We expect at least one well-known APL alias pair to be embedded.
        assert "'Wolves'" in rendered, "expected raw alias 'Wolves' in VALUES"
        assert "'Wolverhampton Wanderers'" in rendered, (
            "expected canonical 'Wolverhampton Wanderers' in VALUES"
        )

    def test_render_includes_clubelo_forest_alias(self, rendered: str):
        """#589: ClubElo short name 'Forest' must map to Nottingham Forest.

        ClubElo emits the bare short name 'Forest' for Nottingham Forest;
        before #589 only 'NottinghamForest' was aliased, so 'Forest' fell to an
        orphan 'ce_forest' row. The alias pair must now be embedded in the
        rendered VALUES so the normalised JOIN resolves it to nottingham_forest.
        """
        assert "'Forest', 'Nottingham Forest', 'nottingham_forest'" in rendered, (
            "expected ClubElo alias 'Forest' -> nottingham_forest in VALUES"
        )

    def test_render_scopes_clubelo_to_in_scope_leagues(self, rendered: str):
        """ClubElo league filter renders from competitions.yaml in_scope flags
        instead of a hardcoded 'ENG-Premier League' literal (former TODO(E8b))."""
        from utils.medallion_config import get_in_scope_competitions

        assert "AND league IN (" in rendered, (
            "expected rendered clubelo filter 'AND league IN (...)'"
        )
        for league in get_in_scope_competitions():
            assert f"'{league}'" in rendered, (
                f"in_scope league {league!r} missing from rendered clubelo filter"
            )

    def test_render_references_expected_bronze_tables(self, rendered: str):
        """Rendered SQL reads the documented Bronze sources by table name."""
        rendered_lower = rendered.lower()
        # Sample the most fingerprint-distinctive bronze tables.
        for tbl in [
            "iceberg.bronze.fbref_schedule",
            "iceberg.bronze.fbref_match_player_stats",
            "iceberg.bronze.fbref_match_events",
            "iceberg.bronze.matchhistory_results",
            "iceberg.bronze.clubelo_ratings",
            "iceberg.bronze.clubelo_ratings_historical",  # #589: relegated APL teams
            "iceberg.bronze.transfermarkt_squad_memberships",
            "iceberg.bronze.capology_player_salaries",
        ]:
            assert tbl in rendered_lower, (
                f"rendered xref_team.sql is missing Bronze table {tbl!r}"
            )

    def test_render_no_stray_double_braces_outside_comments(self, rendered: str):
        """Defensive: no `{{` outside comment lines — would indicate a
        missing-context bug. Comment lines may legitimately mention the
        placeholder name in the file header (see medallion_config contract).
        """
        non_comment = "\n".join(
            line for line in rendered.splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "{{" not in non_comment, (
            "rendered SQL still contains `{{` outside comments — "
            "render_sql_template missed a placeholder"
        )

    def test_render_size_sane(self, rendered: str):
        """Rendered SQL is materially larger than the template (VALUES expanded)."""
        template_size = len(_read_template())
        assert len(rendered) > template_size, (
            "rendered SQL is not larger than template — VALUES did not expand"
        )
