"""
Unit tests for ``dags/sql/silver/xref_manager.sql.j2`` — three sources
(issue #144 + the xref-improvements Transfermarkt bridge).

Strategy
--------
Pure regex/keyword sanity over the raw SQL — same approach as
``test_xref_referee_sql.py`` and ``test_xref_team_sql.py`` — plus a render
smoke test through ``medallion_config.render_sql_template`` (the file is a
Jinja template with the ``{{ manager_aliases_values_sql }}`` placeholder).

Documented invariants we exercise:
  * source ∈ {'fbref', 'fotmob', 'transfermarkt'}.
  * canonical_id = LOWER(REGEXP_REPLACE(<name>, '[^a-zA-Z0-9]+', '_')).
  * confidence ∈ {'name_alias', 'name_normalize', 'name_initial', 'orphan'}
    with the cascade precedence alias > exact > initial > orphan.
  * Reads bronze.fbref_match_managers (spine), bronze.fotmob_player_details
    (is_coach rows) and native Transfermarkt coach stints/profiles.
  * name_initial ambiguity guards on both sides (HAVING unique spine key +
    TM-side initial_key_dup window).
  * NULL/empty manager/coach name is filtered out.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_manager.sql.j2"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefManagerStructure:
    """Regex/keyword sanity over ``xref_manager.sql.j2``."""

    def test_emits_three_sources(self):
        """FBref spine + FotMob coachId mirror + Transfermarkt coach_id."""
        sql = _read_sql().lower()
        for src in ("'fbref'", "'fotmob'", "'transfermarkt'"):
            pattern = re.compile(
                re.escape(src) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert pattern.search(sql), f"missing `{src} AS source` literal"

    def test_no_other_sources_emitted(self):
        """Only fbref/fotmob/transfermarkt may be emitted — others have no
        manager metadata in Bronze."""
        sql = _read_sql().lower()
        for forbidden in [
            "'understat'", "'whoscored'", "'sofascore'",
            "'matchhistory'", "'clubelo'", "'espn'",
        ]:
            pattern = re.compile(
                re.escape(forbidden) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert not pattern.search(sql), (
                f"source label {forbidden} must not be emitted in xref_manager — "
                "only FBref + FotMob + TM carry coach identity in Bronze"
            )

    def test_reads_bronze_fbref_match_managers(self):
        """SELECT reads from iceberg.bronze.fbref_match_managers (spine)."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze.fbref_match_managers" in sql_lower, (
            "xref_manager must read from bronze.fbref_match_managers — the "
            "table populated by parsers/finders.py::parse_match_managers"
        )

    def test_reads_bronze_fotmob_player_details(self):
        """FotMob mirror reads from iceberg.bronze.fotmob_player_details."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze.fotmob_player_details" in sql_lower, (
            "xref_manager must read FotMob coaches from "
            "bronze.fotmob_player_details (is_coach rows)"
        )

    def test_reads_native_transfermarkt_coach_contract(self):
        """TM bridge scopes global stints through native club memberships."""
        sql_lower = _read_sql().lower()
        for relation in (
            "iceberg.bronze.transfermarkt_coach_stints",
            "iceberg.bronze.transfermarkt_coach_profiles",
            "iceberg.bronze.transfermarkt_squad_memberships",
        ):
            assert relation in sql_lower
        assert "iceberg.bronze.transfermarkt_coaches" not in sql_lower

    def test_tm_global_stints_are_scoped_to_tenure_overlap(self):
        """A club's historical coaches must not fan out to every season."""
        sql = _read_sql().lower()

        assert "as season_start" in sql
        assert "as season_end" in sql
        assert "s.appointed_date is not null or s.left_date is not null" in sql
        assert "try_cast(s.appointed_date as date) <= m.season_end" in sql
        assert "try_cast(s.left_date as date) >= m.season_start" in sql

    def test_fotmob_filters_is_coach(self):
        """FotMob block keeps only coaches (is_coach = true)."""
        sql_lower = _read_sql().lower()
        assert "is_coach = true" in sql_lower, (
            "FotMob mirror must filter `is_coach = true` — the table also holds "
            "players (filtered out elsewhere via NOT is_coach)"
        )

    def test_fotmob_source_id_is_stable_coach_id(self):
        """FotMob source_id is the stable coachId = CAST(player_id AS varchar)."""
        sql = _read_sql()
        assert re.search(
            r"CAST\s*\(\s*d?\.?player_id\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        ), "expected CAST(player_id AS varchar) AS source_id for FotMob coachId"

    def test_tm_source_id_is_coach_id(self):
        """TM source_id is the stable coach_id (cast to varchar in tm_coach)."""
        sql = _read_sql()
        assert re.search(
            r"CAST\s*\(\s*(?:s\.)?coach_id\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        ), "expected CAST(coach_id AS varchar) for the TM source_id"

    def test_alias_placeholder_present_once(self):
        """Single standalone `{{ manager_aliases_values_sql }}` placeholder —
        the contract of render_sql_template's placeholder regex."""
        sql = _read_sql()
        occurrences = re.findall(
            r"^\s*\{\{\s*manager_aliases_values_sql\s*\}\}\s*$",
            sql, re.MULTILINE,
        )
        assert len(occurrences) == 1, (
            "expected exactly one standalone {{ manager_aliases_values_sql }} "
            f"placeholder line, found {len(occurrences)}"
        )

    def test_confidence_cascade_literals_present(self):
        """All four cascade confidences appear."""
        sql = _read_sql()
        for lit in ("'name_alias'", "'name_normalize'", "'name_initial'",
                    "'orphan'"):
            assert lit in sql, f"expected confidence literal {lit}"

    def test_confidence_cascade_precedence(self):
        """CASE precedence: alias → exact/normalize → initial → orphan
        (in the TM block)."""
        sql = _read_sql()
        m = re.search(
            r"CASE\s+WHEN\s+t\.alias_cid.*?THEN\s+'name_alias'\s*"
            r"WHEN\s+t\.exact_cid.*?THEN\s+'name_normalize'\s*"
            r"WHEN\s+t\.initial_cid.*?THEN\s+'name_initial'\s*"
            r"ELSE\s+'orphan'",
            sql, re.IGNORECASE | re.DOTALL,
        )
        assert m, (
            "TM confidence CASE must rank name_alias > name_normalize > "
            "name_initial > orphan"
        )

    def test_canonical_id_coalesce_precedence(self):
        """TM canonical_id COALESCE mirrors the confidence precedence."""
        sql = _read_sql()
        assert re.search(
            r"COALESCE\s*\(\s*t\.alias_cid\s*,\s*t\.exact_cid\s*,"
            r"\s*t\.initial_cid\s*,\s*t\.norm\s*\)",
            sql, re.IGNORECASE,
        ), "expected COALESCE(alias_cid, exact_cid, initial_cid, norm)"

    def test_initial_tier_guards(self):
        """name_initial ambiguity guards on BOTH sides: unique spine key
        (HAVING) + TM-side initial_key_dup window."""
        sql = _read_sql()
        assert re.search(
            r"HAVING\s+COUNT\s*\(\s*DISTINCT\s+canonical_id\s*\)\s*=\s*1",
            sql, re.IGNORECASE,
        ), "spine-side guard: HAVING COUNT(DISTINCT canonical_id) = 1 missing"
        assert "initial_key_dup" in sql, "TM-side initial_key_dup guard missing"
        assert re.search(
            r"element_at\s*\(\s*split\s*\(", sql, re.IGNORECASE,
        ), "surname extraction via element_at(split(...), -1) missing"

    def test_canonical_id_normalize_pattern(self):
        """canonical_id = LOWER(REGEXP_REPLACE(<name>, '[^a-zA-Z0-9]+', '_'))."""
        sql = _read_sql()
        pattern = re.compile(
            r"LOWER\s*\(\s*REGEXP_REPLACE",
            re.IGNORECASE,
        )
        assert pattern.search(sql), (
            "expected canonical_id derivation via "
            "LOWER(REGEXP_REPLACE(manager_name, '[^a-zA-Z0-9]+', '_'))"
        )

    def test_canonical_id_regex_uses_alphanumeric_class(self):
        """Normalize regex collapses non-alphanumerics to underscore."""
        sql = _read_sql()
        assert "[^a-zA-Z0-9]+" in sql, (
            "expected regex character class `[^a-zA-Z0-9]+` for normalize"
        )

    def test_canonical_id_transliterates_diacritics(self):
        """canonical_id strips diacritics via NORMALIZE(NFD) + `\\p{Mn}` (issue #201).

        FBref emits the same manager both with and without accents
        ("Régis Le Bris" / "Regis Le Bris"); a bare `[^a-zA-Z0-9]+ -> _`
        produces two different canonical_ids and breaks dim_manager's SCD-2 PK.
        """
        sql = _read_sql()
        assert re.search(r"NORMALIZE\s*\(\s*manager_name\s*,\s*NFD\s*\)", sql, re.IGNORECASE), (
            "expected NORMALIZE(manager_name, NFD) to decompose accents before slugging"
        )
        assert r"\p{Mn}" in sql, (
            "expected `\\p{Mn}` (Unicode combining marks) regex to strip diacritics"
        )

    def test_match_score_null(self):
        """match_score must be NULL — no fuzzy matching for managers."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "match_score must be CAST(NULL AS double) for xref_manager"

    def test_season_cast_to_varchar(self):
        """#404: FBref/FotMob bronze season is year-start bigint → slug varchar
        ('2425') via LPAD(MOD(...)); TM bronze already stores the slug."""
        sql = _read_sql()
        assert "LPAD(CAST(MOD(season" in sql or "LPAD(CAST(MOD(d.season" in sql, (
            "xref_manager must build a slug season via LPAD(MOD(...)) (#404)"
        )

    def test_pure_select_no_create_table(self):
        """File stays a pure SELECT — silver_tasks wraps in CTAS."""
        non_comment = "\n".join(
            line for line in _read_sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "xref_manager.sql.j2 must stay pure SELECT in executable SQL"
        )

    def test_filters_null_and_empty_manager(self):
        """`WHERE manager_name IS NOT NULL AND manager_name <> ''`."""
        sql = _read_sql()
        assert "IS NOT NULL" in sql.upper(), (
            "expected NULL-filter on manager_name column"
        )
        assert "<> ''" in sql or "!= ''" in sql, (
            "expected empty-string filter on manager_name column"
        )

    def test_schema_columns_present(self):
        """All 8 documented schema columns appear in SELECT — either as
        ``AS <col>`` alias or as a bare reference (``league``, ``season``).
        """
        sql = _read_sql()
        expected_aliased = [
            "canonical_id", "source", "source_id", "display_name",
            "confidence", "match_score",
        ]
        for col in expected_aliased:
            pattern = re.compile(
                r"AS\s+" + re.escape(col) + r"\b",
                re.IGNORECASE,
            )
            assert pattern.search(sql), (
                f"schema column {col!r} missing as `AS {col}` alias in "
                "xref_manager.sql.j2 — Gold dim_manager will JOIN against this column"
            )
        # ``league`` and ``season`` come from Bronze with the right name,
        # so the SQL forwards them bare (matches xref_referee.sql convention).
        for col in ("league", "season"):
            assert re.search(rf"\b{col}\b", sql, re.IGNORECASE), (
                f"schema column {col!r} missing from xref_manager.sql.j2 SELECT"
            )

    def test_pk_grouping_present(self):
        """GROUP BY / ROW_NUMBER dedup enforce the documented PK =
        (source, source_id, league, season)."""
        sql_lower = _read_sql().lower()
        assert "group by" in sql_lower, (
            "expected GROUP BY clause to act as DISTINCT for the (source, "
            "source_id, league, season) PK contract"
        )
        assert "row_number() over" in sql_lower, (
            "expected ROW_NUMBER dedup for the FotMob / TM per-(id, league, "
            "season) grain"
        )


class TestXrefManagerRender:
    """Render smoke test through the real Jinja machinery."""

    def test_render_with_live_aliases(self):
        """The shipped manager_aliases.yaml renders into valid, placeholder-free
        SQL through get_manager_alias_sql_values(source=None)."""
        import sys
        DAGS_DIR = PROJECT_ROOT / "dags"
        for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
            if p not in sys.path:
                sys.path.insert(0, p)
        from utils.medallion_config import (          # noqa: E402
            get_manager_alias_sql_values,
            render_sql_template,
        )

        rendered = render_sql_template(
            SQL_PATH,
            manager_aliases_values_sql=get_manager_alias_sql_values(source=None),
        )
        assert "{{" not in rendered and "}}" not in rendered, (
            "rendered SQL must not contain unexpanded Jinja placeholders"
        )
        # The alias VALUES body must parse as tuples of 3 (raw, cid, league).
        assert re.search(r"\)\s+AS\s+t\s*\(raw_name,\s*canonical_id,\s*league\)",
                         rendered), "alias VALUES table shape changed"


class TestFbrefSeasonSlugDedup:
    """The FBref branch must dedupe on the season it emits, not on the raw
    columns the slug is derived from."""

    def test_fbref_groups_by_output_columns_not_raw_season(self):
        """A legacy row (season=2025, source_season_id=NULL) and a row from the
        production pipeline (season=2025, source_season_id='2025-2026') both
        render the '2526' slug. Grouping by the raw pair kept them in separate
        groups and emitted the same manager twice, which the xref PK gate
        (source, source_id, league, season) refuses — it did, in production."""
        sql = _read_sql()
        fbref_branch = sql.split("UNION ALL")[0]
        group_by = fbref_branch.rsplit("GROUP BY", 1)[1]
        keys = "\n".join(
            line for line in group_by.splitlines()
            if not line.strip().startswith("--")
        )

        assert "source_season_id" not in keys
        assert "season" not in keys
        assert re.search(r"^\s*1,\s*3,\s*4,\s*5,\s*6\s*$", keys, re.M)
