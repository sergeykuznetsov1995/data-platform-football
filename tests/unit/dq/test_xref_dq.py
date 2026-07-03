"""Unit tests for ``dags.utils.xref_dq`` (T6 deliverable).

Strategy
--------
Two layers:

1. **Pure-Python tests** for builders + helpers — verify the structure of
   the ``Check`` lists and the SQL strings they embed (enum allow-lists,
   table names, severities). No Trino, no DuckDB — read-only attribute
   inspection of the dataclasses returned.

2. **DuckDB-bridged tests** for the orphan-rate evaluator — same approach
   as ``tests/unit/dq/test_e5_checks.py``: monkey-patch
   ``utils.data_quality._get_conn`` to return an in-memory DuckDB
   connection seeded with the schemas the DQ SQL touches. This exercises
   the real SQL generation and result parsing without reaching out to a
   real Trino cluster.

The tests do NOT call ``run_checks`` over the DuckDB bridge for the
``check_enum_compliance`` helper because the universal data_quality
runners use Trino-specific operators in some checks; pure builder
inspection is sufficient to lock the contract.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

# Wire dags/ onto sys.path so ``utils.xref_dq`` resolves.
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
# #437: _fct_player_match_output_columns renders the fct_player_match .sql.j2
# from the shipped config below.
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(REPO_ROOT / "configs" / "medallion")
)

# Import after sys.path wire-up.
from utils import data_quality as dq  # noqa: E402
from utils import xref_dq  # noqa: E402


# ===========================================================================
# Helpers — DuckDB bridge (mirrors test_e5_checks.py)
# ===========================================================================

class _DuckCursor:
    def __init__(self, con):
        self._con = con
        self._result = None

    def execute(self, sql: str):
        self._result = self._con.execute(sql)
        return self

    def fetchone(self):
        return self._result.fetchone() if self._result else None

    def fetchall(self):
        return self._result.fetchall() if self._result else []

    def close(self):
        pass


class _DuckConn:
    def __init__(self, con):
        self._con = con

    def cursor(self):
        return _DuckCursor(self._con)

    def close(self):
        pass


def _build_conn():
    """In-memory DuckDB pre-loaded with the silver xref schemas."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")

    # silver.xref_team
    con.execute(
        """
        CREATE TABLE iceberg.silver.xref_team (
            canonical_id VARCHAR,
            source       VARCHAR,
            source_id    VARCHAR,
            display_name VARCHAR,
            league       VARCHAR,
            season       VARCHAR,
            confidence   VARCHAR,
            match_score  DOUBLE
        )
        """
    )
    # silver.xref_match
    con.execute(
        """
        CREATE TABLE iceberg.silver.xref_match (
            canonical_id VARCHAR,
            source       VARCHAR,
            source_id    VARCHAR,
            display_name VARCHAR,
            league       VARCHAR,
            season       VARCHAR,
            confidence   VARCHAR,
            match_score  DOUBLE
        )
        """
    )
    # silver.xref_player
    con.execute(
        """
        CREATE TABLE iceberg.silver.xref_player (
            canonical_id VARCHAR,
            source       VARCHAR,
            source_id    VARCHAR,
            display_name VARCHAR,
            league       VARCHAR,
            season       VARCHAR,
            confidence   VARCHAR,
            match_score  DOUBLE
        )
        """
    )
    return con


@pytest.fixture
def duck_conn(monkeypatch):
    con = _build_conn()
    bridge = _DuckConn(con)
    # Patch BOTH dq._get_conn AND xref_dq._get_conn (xref_dq imports it
    # directly so the local binding needs its own patch).
    monkeypatch.setattr(dq, "_get_conn", lambda *_a, **_kw: bridge)
    monkeypatch.setattr(xref_dq, "_get_conn", lambda *_a, **_kw: bridge)
    yield con
    con.close()


# ===========================================================================
# Pure-Python builder tests (no Trino/DuckDB)
# ===========================================================================

def _all_sql(checks):
    """Concatenate all check SQL/where fragments for substring assertions."""
    parts = []
    for c in checks:
        parts.append(c.name)
        parts.append(c.kind)
        for v in c.params.values():
            parts.append(repr(v))
    return " ".join(parts)


def test_build_xref_team_checks_count():
    """T6 expects ≥4 checks on xref_team."""
    checks = xref_dq.build_xref_team_checks()
    assert len(checks) >= 4
    haystack = _all_sql(checks)
    assert 'iceberg.silver.xref_team' in haystack


def test_build_xref_team_confidence_enum():
    """xref_team confidence ∈ {name_alias, orphan}."""
    checks = xref_dq.build_xref_team_checks()
    enum_checks = [c for c in checks if 'enum_compliance' in c.name and 'confidence' in c.name]
    assert len(enum_checks) == 1
    where = enum_checks[0].params['where']
    assert "'name_alias'" in where
    assert "'orphan'" in where


def test_build_xref_team_source_enum_8_sources():
    """xref_team source enum covers all 8 documented sources."""
    checks = xref_dq.build_xref_team_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    for src in ['fbref', 'understat', 'whoscored', 'sofascore',
                'fotmob', 'matchhistory', 'clubelo', 'espn']:
        assert f"'{src}'" in where


def test_xref_match_seven_sources():
    """Task 2.1 (Phase B): xref_match cascade allows 7 sources."""
    checks = xref_dq.build_xref_match_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    for src in ['fbref', 'whoscored', 'understat', 'sofascore',
                'fotmob', 'matchhistory', 'espn']:
        assert f"'{src}'" in where
    # clubelo is intentionally NOT in xref_match (no match-grain bronze)
    assert "'clubelo'" not in where


def test_xref_match_confidence_three_tier():
    """Phase B confidence enum = {exact, date_team_match, orphan}."""
    checks = xref_dq.build_xref_match_checks()
    enum_checks = [c for c in checks if 'enum_compliance' in c.name and 'confidence' in c.name]
    assert len(enum_checks) == 1
    where = enum_checks[0].params['where']
    for tier in ['exact', 'date_team_match', 'orphan']:
        assert f"'{tier}'" in where


def test_xref_match_pk_composite():
    """PK is now (canonical_id, source) — same FBref hex appears under
    multiple source values when bridged."""
    checks = xref_dq.build_xref_match_checks()
    dup_checks = [c for c in checks if c.kind == 'no_duplicates']
    assert len(dup_checks) == 1
    assert dup_checks[0].params['pk'] == ['canonical_id', 'source']


def test_xref_match_coverage_checks_per_source():
    """Six bridge_coverage checks (one per non-fbref source) with two-tier thresholds."""
    checks = xref_dq.build_xref_match_checks()
    cov_checks = [c for c in checks if c.kind == 'coverage' and 'bridge_coverage' in c.name]
    assert len(cov_checks) == 6, (
        f"Expected 6 per-source coverage checks (whoscored/understat/sofascore/"
        f"fotmob/matchhistory/espn), got {len(cov_checks)}"
    )
    names = {c.name for c in cov_checks}
    for src in ['whoscored', 'understat', 'sofascore', 'fotmob', 'matchhistory', 'espn']:
        assert f'bridge_coverage[xref_match.{src}]' in names

    # Verify thresholds: warn=0.95, error=0.80
    for c in cov_checks:
        assert c.params['warn_threshold'] == 0.95
        assert c.params['error_threshold'] == 0.80
        assert "confidence != 'orphan'" in c.params['condition']
        assert c.params['where'].startswith("source = '")


def test_xref_referee_source_enum():
    checks = xref_dq.build_xref_referee_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    assert "'fbref'" in where
    assert "'matchhistory'" in where
    assert "'fotmob'" in where  # 3rd source (issue #270)


def test_xref_referee_confidence_enum_is_name_alias_orphan():
    """#143: curated-config confidence labels — name_alias / orphan (not name_normalize)."""
    checks = xref_dq.build_xref_referee_checks()
    conf = [c for c in checks if 'enum_compliance' in c.name and '.confidence' in c.name]
    assert len(conf) == 1
    where = conf[0].params['where']
    assert "'name_alias'" in where and "'orphan'" in where
    assert "'name_normalize'" not in where


def test_xref_referee_has_canonical_id_format_guard():
    """#143: canonical_id prefix guard present (ref_/fb_ref_/mh_ref_)."""
    names = {c.name for c in xref_dq.build_xref_referee_checks()}
    assert 'canonical_id_format[xref_referee]' in names


def test_xref_referee_no_per_canonical_season_guard():
    """#143: the xref_player-style per-canonical dup guard is intentionally
    absent — name-keyed referees legitimately merge multiple raw spellings
    into one canonical per (source, season)."""
    names = {c.name for c in xref_dq.build_xref_referee_checks()}
    assert 'no_duplicates_per_canonical_season[xref_referee]' not in names


def test_xref_referee_known_pairs_guard():
    """#143: known-referee regression guard checks anchors carry 2 sources."""
    checks = xref_dq.build_xref_referee_checks()
    kp = [c for c in checks if c.name == 'known_referee_pairs[xref_referee]']
    assert len(kp) == 1
    where = kp[0].params['where']
    for anchor in xref_dq.KNOWN_REFEREE_CANONICALS:
        assert anchor in where
    assert 'COUNT(DISTINCT source) < 2' in where


def test_xref_manager_three_source_checks():
    """xref_manager — FBref spine + FotMob coachId mirror (#144) + TM bridge.

    Check set: row count bounds, PK uniqueness, NOT NULL on canonical,
    source enum {fbref, fotmob, transfermarkt}, confidence enum
    {name_alias, name_normalize, name_initial, orphan}, per-source
    WARNING-severity name-collision guards, TM source-present floor and the
    known-manager anchor guard.
    """
    checks = xref_dq.build_xref_manager_checks()

    # Row counts: the plain table bound, the TM source-present floor and the
    # anchor guard (all kind='row_count'; enum_compliance shares the kind so
    # filter by name).
    rc = [c for c in checks if c.kind == 'row_count' and 'enum_compliance' not in c.name]
    assert len(rc) == 3
    plain = [c for c in rc if c.name == 'row_count[iceberg.silver.xref_manager]']
    assert len(plain) == 1 and plain[0].params.get('min_rows', 0) > 0
    present = [c for c in rc if c.name == 'source_present[xref_manager.transfermarkt]']
    assert len(present) == 1
    assert present[0].severity == 'WARNING'
    assert present[0].params['where'] == "source = 'transfermarkt'"
    anchors = [c for c in rc if c.name == 'known_manager_anchors[xref_manager]']
    assert len(anchors) == 1
    assert anchors[0].severity == 'WARNING'
    for cid in xref_dq.KNOWN_MANAGER_CANONICALS:
        assert f"'{cid}'" in anchors[0].params['where']

    # PK uniqueness on (source, source_id, league, season).
    pk = [
        c for c in checks
        if c.kind == 'no_duplicates'
        and c.params['pk'] == ['source', 'source_id', 'league', 'season']
    ]
    assert len(pk) == 1

    # Collision guards: WARNING-only no_duplicates on (canonical_id, league,
    # season) per mirror source — two distinct coach ids collapsing to one
    # slug is a suspected false merge.
    collision = [
        c for c in checks
        if c.kind == 'no_duplicates'
        and c.params['pk'] == ['canonical_id', 'league', 'season']
    ]
    assert len(collision) == 2
    assert all(c.severity == 'WARNING' for c in collision)
    assert {c.params.get('where') for c in collision} == {
        "source = 'fotmob'", "source = 'transfermarkt'",
    }

    # NOT NULL on canonical / source / source_id.
    nn = [c for c in checks if c.kind == 'no_nulls']
    assert len(nn) == 1
    assert set(nn[0].params['cols']) >= {'canonical_id', 'source', 'source_id'}

    # Source enum: FBref spine + FotMob mirror + TM bridge.
    src_enum = [
        c for c in checks
        if 'enum_compliance' in c.name and '.source' in c.name
    ]
    assert len(src_enum) == 1
    where_src = src_enum[0].params['where']
    for allowed in ['fbref', 'fotmob', 'transfermarkt']:
        assert f"'{allowed}'" in where_src
    for forbidden in ['understat', 'whoscored', 'sofascore',
                      'matchhistory', 'clubelo', 'espn']:
        assert f"'{forbidden}'" not in where_src, (
            f"xref_manager source enum must NOT include {forbidden!r} — only "
            "FBref + FotMob + TM carry coach identity in Bronze"
        )

    # Confidence enum: the 3-tier cascade + orphan.
    conf_enum = [
        c for c in checks
        if 'enum_compliance' in c.name and 'confidence' in c.name
    ]
    assert len(conf_enum) == 1
    where_conf = conf_enum[0].params['where']
    for allowed in ['name_alias', 'name_normalize', 'name_initial', 'orphan']:
        assert f"'{allowed}'" in where_conf
    for forbidden in ["'exact'", "'name_team'", "'ambiguous'"]:
        assert forbidden not in where_conf, (
            f"xref_manager confidence enum must NOT include {forbidden}"
        )


def test_xref_player_confidence_enum_full():
    """xref_player confidence enum includes all v2-resolver cascade tiers.

    The R2-followup v2 resolver introduced four additional tier labels
    (surname-anchor, token_set subset, nicknames dict, player_aliases YAML)
    plus retained two reserved STUBs (jersey / dob) and the orphan terminal.
    'ambiguous' is INTENTIONALLY excluded — clerical-review rows must land
    in silver.xref_player_review, never in xref_player itself.
    """
    checks = xref_dq.build_xref_player_checks()
    enum_checks = [c for c in checks if 'enum_compliance' in c.name and 'confidence' in c.name]
    assert len(enum_checks) == 1
    where = enum_checks[0].params['where']
    for tier in [
        'exact',
        'name_team',
        'name_team_surname',
        'name_team_subset',
        'name_team_nickname',
        'name_team_alias',
        'name_team_jersey',
        'name_team_dob',
        'orphan',
    ]:
        assert f"'{tier}'" in where, f"missing tier {tier!r} in enum allow-list"
    # 'ambiguous' must NOT be in the xref_player allow-list — see docstring.
    assert "'ambiguous'" not in where, (
        "xref_player allow-list must NOT include 'ambiguous' — "
        "Fellegi-Sunter clerical-review band routes to xref_player_review."
    )


def test_xref_player_canonical_id_format_check():
    """xref_player must enforce canonical_id ^(fb|us|ws|fm|ss|tm|cap)_.+ regex.

    Prefixes match :func:`xref_player_resolver._orphan_prefix`. Issue #104
    added ``tm`` (Transfermarkt) and ``cap`` (Capology) after the resolver
    side of issue #43 / #59 shipped without updating this DQ gate.
    """
    checks = xref_dq.build_xref_player_checks()
    fmt_checks = [c for c in checks if 'canonical_id_format' in c.name]
    assert len(fmt_checks) == 1
    where = fmt_checks[0].params['where']
    assert "regexp_like" in where
    # #601 added 'sf' (SoFIFA); #692 added 'es' (ESPN lineups).
    assert "fb|us|ws|fm|ss|tm|cap|sf|es" in where


def test_xref_player_no_duplicates_per_canonical_season_check():
    """Issue #70 acceptance: a row_count(max=0) ERROR-severity check guards
    against canonical_id fan-out at source level. The dedup itself happens in
    xref_player_resolver._dedup_canonical_per_season; this check makes
    regressions visible if someone bypasses the resolver path."""
    checks = xref_dq.build_xref_player_checks()
    dup_checks = [
        c for c in checks
        if 'no_duplicates_per_canonical_season' in c.name
    ]
    assert len(dup_checks) == 1, (
        "Expected exactly 1 no_duplicates_per_canonical_season check"
    )
    chk = dup_checks[0]
    assert chk.kind == 'row_count'
    assert chk.severity == 'ERROR'
    assert chk.params['min_rows'] == 0
    assert chk.params['max_rows'] == 0
    where = chk.params['where']
    # The WHERE filters rows whose (canonical_id, source, league, season)
    # appears more than once with distinct *player identities* — the exact
    # fan-out pattern that prompted the Gold ROW_NUMBER hack we just removed.
    # #803: split_part(source_id,'|',1) so ESPN's '<name>|<team>' multi-team
    # stints (same player, two clubs) are NOT counted as a collision; only
    # genuinely different players trip the gate.
    assert "COUNT(DISTINCT split_part(source_id, '|', 1)) > 1" in where
    assert "GROUP BY canonical_id, source, league, season" in where
    assert "confidence <> 'orphan'" in where


def test_build_all_xref_checks_aggregates():
    """Aggregated list now also includes xref_player_review (5 checks).

    Original T6 spec was ≥15; v2 adds 5 review checks → ≥20 total.
    """
    checks = xref_dq.build_all_xref_checks()
    assert len(checks) >= 20, f"Expected ≥20 checks, got {len(checks)}"


# ---------------------------------------------------------------------------
# xref_player_review (R2-followup v2 sibling)
# ---------------------------------------------------------------------------
def test_xref_player_review_checks_minimum_set():
    checks = xref_dq.build_xref_player_review_checks()
    # row_count + no_duplicates + no_nulls + 2× enum_compliance
    kinds = [c.kind for c in checks]
    assert 'row_count' in kinds
    assert 'no_duplicates' in kinds
    assert 'no_nulls' in kinds
    enum_checks = [c for c in checks if 'enum_compliance' in c.name]
    assert len(enum_checks) == 2, f"expected 2 enum checks, got {len(enum_checks)}"


def test_xref_player_review_rule_enum():
    checks = xref_dq.build_xref_player_review_checks()
    rule_checks = [c for c in checks if 'enum_compliance' in c.name and '.rule' in c.name]
    assert len(rule_checks) == 1
    where = rule_checks[0].params['where']
    for rule in ['surname_collision', 'token_set_band', 'nickname_collision']:
        assert f"'{rule}'" in where


def test_xref_player_review_source_enum_excludes_fbref():
    """FBref is the spine — never appears in review queue."""
    checks = xref_dq.build_xref_player_review_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    assert "'fbref'" not in where, "fbref must not be in review-source allow-list"
    for src in ['understat', 'whoscored', 'sofascore', 'fotmob',
                'transfermarkt', 'capology']:
        assert f"'{src}'" in where


def test_xref_player_review_row_count_soft_ceiling():
    """Soft ceiling 200 rows across the entire review table."""
    checks = xref_dq.build_xref_player_review_checks()
    rc = [c for c in checks if c.kind == 'row_count' and 'enum' not in c.name]
    assert len(rc) == 1
    assert rc[0].params['min_rows'] == 0
    assert rc[0].params['max_rows'] == 200


def test_check_enum_compliance_helper_basic():
    """Helper generates row_count check with NOT IN predicate."""
    chk = xref_dq.check_enum_compliance(
        'iceberg.silver.xref_team', 'confidence',
        allowed=['name_alias', 'orphan'],
    )
    assert chk.kind == 'row_count'
    assert chk.params['min_rows'] == 0
    assert chk.params['max_rows'] == 0
    where = chk.params['where']
    assert "confidence NOT IN" in where
    assert "'name_alias'" in where
    assert "'orphan'" in where


def test_check_enum_compliance_rejects_unsafe_values():
    """Helper refuses values containing quotes / SQL comments."""
    with pytest.raises(ValueError):
        xref_dq.check_enum_compliance(
            'iceberg.silver.xref_team', 'confidence',
            allowed=["evil'); DROP TABLE x; --"],
        )


def test_check_enum_compliance_rejects_empty_allowed():
    with pytest.raises(ValueError):
        xref_dq.check_enum_compliance(
            'iceberg.silver.xref_team', 'confidence',
            allowed=[],
        )


# ===========================================================================
# DuckDB-bridged tests — orphan-rate evaluator
# ===========================================================================

def test_orphan_rate_per_source_classifies_correctly(duck_conn):
    """Verify the OK/WARNING/ERROR cascade on synthetic orphan distribution."""
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_player VALUES "
        # fbref — 100 rows, 0 orphans → OK
        + ", ".join(
            f"('fb_p{i}', 'fbref', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"'{'orphan' if False else 'exact'}', NULL)"
            for i in range(100)
        )
        + ", "
        # understat — 100 rows, 12 orphans → WARNING (12% > 10% but <25%)
        + ", ".join(
            f"('us_p{i}', 'understat', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"'{'orphan' if i < 12 else 'name_team'}', NULL)"
            for i in range(100)
        )
        + ", "
        # whoscored — 100 rows, 30 orphans → ERROR (>25%)
        + ", ".join(
            f"('ws_p{i}', 'whoscored', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"'{'orphan' if i < 30 else 'name_team'}', NULL)"
            for i in range(100)
        )
    )

    res = xref_dq.evaluate_orphan_rate_per_source(
        table='iceberg.silver.xref_player',
        warning_threshold=10.0,
        error_threshold=25.0,
    )

    assert res['per_source']['fbref']['verdict'] == 'OK'
    assert res['per_source']['fbref']['orphans'] == 0
    assert res['per_source']['understat']['verdict'] == 'WARNING'
    assert res['per_source']['understat']['orphans'] == 12
    assert res['per_source']['whoscored']['verdict'] == 'ERROR'
    assert res['per_source']['whoscored']['orphans'] == 30
    assert res['verdict'] == 'ERROR'  # promoted to strictest
    assert any(b['source'] == 'whoscored' for b in res['breaches'])


def test_orphan_rate_handles_empty_table(duck_conn):
    """No rows → empty per_source dict, verdict OK."""
    res = xref_dq.evaluate_orphan_rate_per_source(
        table='iceberg.silver.xref_player',
    )
    assert res['per_source'] == {}
    assert res['verdict'] == 'OK'
    assert res['breaches'] == []


def test_orphan_rate_current_season_only_excludes_history(duck_conn):
    """#803: a historical season heavy with orphans (thin old FBref spine,
    #788) must NOT red the gate when ``current_season_only=True`` — only the
    latest season is measured."""
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_player VALUES "
        # fotmob 2019 — 100 rows, 90 orphans → 90% (would be ERROR table-wide)
        + ", ".join(
            f"('fm_h{i}', 'fotmob', 'h{i}', 'H{i}', 'ENG', '1920', "
            f"'{'orphan' if i < 90 else 'name_team'}', NULL)"
            for i in range(100)
        )
        + ", "
        # fotmob 2025 — 100 rows, 2 orphans → 2% (current season = healthy)
        + ", ".join(
            f"('fm_c{i}', 'fotmob', 'c{i}', 'C{i}', 'ENG', '2526', "
            f"'{'orphan' if i < 2 else 'name_team'}', NULL)"
            for i in range(100)
        )
    )

    table_wide = xref_dq.evaluate_orphan_rate_per_source(
        table='iceberg.silver.xref_player',
    )
    assert table_wide['per_source']['fotmob']['verdict'] == 'ERROR'

    current = xref_dq.evaluate_orphan_rate_per_source(
        table='iceberg.silver.xref_player',
        current_season_only=True,
    )
    assert current['per_source']['fotmob']['orphans'] == 2
    assert current['per_source']['fotmob']['verdict'] == 'OK'
    assert current['verdict'] == 'OK'
    assert current['breaches'] == []


# ===========================================================================
# Bronze-vs-xref freshness gap (Issue #15 regression guard)
# ===========================================================================

class _MockCursor:
    """Replays a scripted sequence of (sql_substring → rows) tuples.

    Matches by the FIRST substring found in the executed SQL — order matters,
    so the script must list queries in execution order. Tests construct the
    script per scenario.
    """

    def __init__(self, script):
        self._script = list(script)
        self._last_rows = []

    def execute(self, sql):
        for needle, rows in self._script:
            if needle in sql:
                self._last_rows = rows
                self._script.remove((needle, rows))
                return self
        raise AssertionError(f"Unscripted SQL: {sql[:200]}")

    def fetchone(self):
        return self._last_rows[0] if self._last_rows else None

    def fetchall(self):
        return list(self._last_rows)

    def close(self):
        pass


class _MockConn:
    def __init__(self, script):
        self._script = script

    def cursor(self):
        return _MockCursor(self._script)

    def close(self):
        pass


def _patch_freshness_conn(monkeypatch, script):
    """Bind a scripted _MockConn into xref_dq's freshness evaluator."""
    monkeypatch.setattr(
        xref_dq, "_get_conn",
        lambda *_a, **_kw: _MockConn(script),
    )


def test_bronze_xref_freshness_ok_when_xref_fresher_than_bronze(monkeypatch):
    """xref_player committed AFTER all Bronze ingests — no lag → OK verdict."""
    from datetime import datetime, timezone

    xref_ts = datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc)
    us_ts = datetime(2026, 5, 17, 9, 0)
    fm_ts = datetime(2026, 5, 17, 7, 0)
    _patch_freshness_conn(monkeypatch, [
        ('xref_player$snapshots', [(xref_ts,)]),
        ('iceberg.bronze.understat_players', [('2526', us_ts)]),
        ('iceberg.bronze.fotmob_player_stats', [('2526', fm_ts)]),
    ])

    res = xref_dq.evaluate_bronze_xref_freshness_gap()

    assert res['verdict'] == 'OK'
    assert res['breaches'] == []
    assert len(res['per_partition']) == 2
    for p in res['per_partition']:
        assert p['verdict'] == 'OK'
        assert p['lag_hours'] <= 0


def test_bronze_xref_freshness_warning_when_bronze_ahead(monkeypatch):
    """Issue #15 reproduction: Bronze US 2026-05-17 09:00, xref 2026-05-15 14:54.

    Lag ≈ 42h → above warning_lag_hours=24, below error_lag_hours=72 → WARNING.
    """
    from datetime import datetime, timezone

    xref_ts = datetime(2026, 5, 15, 14, 54, tzinfo=timezone.utc)
    us_ts = datetime(2026, 5, 17, 9, 0)
    fm_ts = datetime(2026, 5, 17, 7, 0)
    _patch_freshness_conn(monkeypatch, [
        ('xref_player$snapshots', [(xref_ts,)]),
        ('iceberg.bronze.understat_players', [('2526', us_ts)]),
        ('iceberg.bronze.fotmob_player_stats', [('2526', fm_ts)]),
    ])

    res = xref_dq.evaluate_bronze_xref_freshness_gap()

    assert res['verdict'] == 'WARNING'
    assert len(res['breaches']) == 2
    us = next(p for p in res['per_partition'] if p['source'] == 'understat')
    assert us['lag_hours'] > 24
    assert us['lag_hours'] < 72
    assert us['verdict'] == 'WARNING'


def test_bronze_xref_freshness_error_when_lag_exceeds_3_days(monkeypatch):
    """Worst-case staleness: 5-day-old xref → ERROR escalation."""
    from datetime import datetime, timezone

    xref_ts = datetime(2026, 5, 10, 0, 0, tzinfo=timezone.utc)
    us_ts = datetime(2026, 5, 17, 9, 0)
    _patch_freshness_conn(monkeypatch, [
        ('xref_player$snapshots', [(xref_ts,)]),
        ('iceberg.bronze.understat_players', [('2526', us_ts)]),
        ('iceberg.bronze.fotmob_player_stats', []),
    ])

    res = xref_dq.evaluate_bronze_xref_freshness_gap()

    assert res['verdict'] == 'ERROR'
    us = next(p for p in res['per_partition'] if p['source'] == 'understat')
    assert us['lag_hours'] > 72
    assert us['verdict'] == 'ERROR'


def test_bronze_xref_freshness_handles_empty_xref_snapshots(monkeypatch):
    """Cold-start: xref never materialised → MAX(committed_at) IS NULL.

    Evaluator must not crash — partitions emit lag_hours=None / verdict=OK
    (no signal to alert on without a baseline).
    """
    from datetime import datetime

    us_ts = datetime(2026, 5, 17, 9, 0)
    _patch_freshness_conn(monkeypatch, [
        ('xref_player$snapshots', [(None,)]),
        ('iceberg.bronze.understat_players', [('2526', us_ts)]),
        ('iceberg.bronze.fotmob_player_stats', []),
    ])

    res = xref_dq.evaluate_bronze_xref_freshness_gap()

    assert res['verdict'] == 'OK'
    assert res['xref_max_committed_at'] is None
    us = next(p for p in res['per_partition'] if p['source'] == 'understat')
    assert us['lag_hours'] is None


# ===========================================================================
# E1.5 post-cutover checks (issue #451)
# ===========================================================================

def _fct_player_match_output_columns() -> set:
    """Derive the output schema of gold.fct_player_match from its SQL file.

    Parses the final SELECT (between the top-level ``SELECT`` and ``FROM``
    markers, both at column 0) and collects the ``AS <alias>`` projections —
    derive-from-source so the test follows schema renames instead of
    hardcoding a snapshot.
    """
    from utils.medallion_config import render_fact_sql

    # #437: render the .sql.j2 so the COALESCE metric columns (now templated
    # from source_priority.yaml) reappear as `AS <alias>` projections.
    sql = render_fact_sql(
        REPO_ROOT / "dags" / "sql" / "gold" / "fct_player_match.sql.j2",
        "fct_player_match",
    )
    final_select = re.split(r"^SELECT\s*$", sql, flags=re.MULTILINE)[-1]
    final_select = re.split(r"^FROM\s", final_select, flags=re.MULTILINE)[0]
    return set(re.findall(r"\bAS\s+(\w+)\s*,?\s*$", final_select, flags=re.MULTILINE))


def test_post_cutover_checks_structure():
    """build_e1_5_post_cutover_checks: 4 WARNING-only row_count guards."""
    checks = xref_dq.build_e1_5_post_cutover_checks()
    assert len(checks) == 4
    assert all(c.severity == 'WARNING' for c in checks)
    assert all(c.kind == 'row_count' for c in checks)


def test_post_cutover_fct_player_match_predicates_match_sql_schema():
    """#451 regression: WHERE predicates must reference live fct_player_match
    columns. #438 renamed *_canonical → plain ids; the stale predicates made
    both checks die with COLUMN_NOT_FOUND instead of validating.
    """
    schema = _fct_player_match_output_columns()
    # Parser sanity — the live schema must expose the plain ids.
    assert {'team_id', 'player_id'} <= schema

    checks = [c for c in xref_dq.build_e1_5_post_cutover_checks()
              if c.params.get('table') == 'iceberg.gold.fct_player_match']
    assert len(checks) == 2
    for check in checks:
        where = check.params['where']
        for col in re.findall(r'\b(?:team_id|player_id)\w*', where):
            assert col in schema, (
                f"{check.name}: predicate references '{col}' which is not an "
                f"output column of fct_player_match.sql"
            )


# ===========================================================================
# DOB corroboration DQ (companion to the resolver name_team_dob tier)
# ===========================================================================

def test_xref_player_review_rule_enum_includes_dob_veto():
    """The resolver's DOB-veto pass emits rule='dob_veto' — the enum gate
    must allow it or the first vetoed row turns the DAG red."""
    checks = xref_dq.build_xref_player_review_checks()
    rule_checks = [c for c in checks
                   if 'enum_compliance' in c.name and '.rule' in c.name]
    assert len(rule_checks) == 1
    assert "'dob_veto'" in rule_checks[0].params['where']


def test_default_player_dob_projections_are_bronze_only():
    """Circularity guard: DOB must come from Bronze — silver profile tables
    depend on xref_player themselves."""
    for src, proj in xref_dq.DEFAULT_PLAYER_DOB_PROJECTIONS:
        assert 'iceberg.bronze.' in proj, f"{src}: non-Bronze DOB projection"
        assert 'silver' not in proj, f"{src}: circular silver read"
    assert {s for s, _ in xref_dq.DEFAULT_PLAYER_DOB_PROJECTIONS} == {
        'fotmob', 'sofascore', 'transfermarkt', 'sofifa', 'whoscored',
    }


def _seed_dob_conflict_fixture(duck_conn, tm_dob: str):
    """xref_player + two injectable DOB projections (fotmob vs TM)."""
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_player VALUES "
        "('fb_a', 'fotmob', '10', 'Player A', 'ENG', '2425', 'name_team', 95.0), "
        "('fb_a', 'transfermarkt', '77', 'Player A', 'ENG', '2425', "
        " 'name_team_surname', 99.0), "
        # Orphans are excluded from the conflict scan by design.
        "('tm_99', 'transfermarkt', '99', 'Player X', 'ENG', '2425', 'orphan', NULL)"
    )
    duck_conn.execute(
        "CREATE TABLE dob_fm AS SELECT '10' AS source_id, DATE '1998-03-01' AS dob"
    )
    duck_conn.execute(
        f"CREATE TABLE dob_tm AS SELECT '77' AS source_id, DATE '{tm_dob}' AS dob "
        "UNION ALL SELECT '99', DATE '1990-01-01'"
    )
    return (
        ('fotmob', 'SELECT source_id, dob FROM dob_fm'),
        ('transfermarkt', 'SELECT source_id, dob FROM dob_tm'),
    )


def test_evaluate_dob_conflicts_flags_disagreement(duck_conn):
    projections = _seed_dob_conflict_fixture(duck_conn, tm_dob='1995-07-20')
    res = xref_dq.evaluate_dob_conflicts(dob_projections=projections)
    assert res['verdict'] == 'WARNING'
    assert res['conflicts'] == 1
    assert res['rows'][0]['canonical_id'] == 'fb_a'
    assert res['rows'][0]['n_sources'] == 2
    assert res['rows'][0]['spread_days'] > 1
    assert res['truncated'] is False


def test_evaluate_dob_conflicts_tolerates_one_day(duck_conn):
    projections = _seed_dob_conflict_fixture(duck_conn, tm_dob='1998-03-02')
    res = xref_dq.evaluate_dob_conflicts(dob_projections=projections)
    assert res['verdict'] == 'OK'
    assert res['conflicts'] == 0
    assert res['rows'] == []


def test_evaluate_dob_conflicts_empty_table_is_ok(duck_conn):
    duck_conn.execute("CREATE TABLE dob_empty (source_id VARCHAR, dob DATE)")
    res = xref_dq.evaluate_dob_conflicts(
        dob_projections=(('fotmob', 'SELECT source_id, dob FROM dob_empty'),)
    )
    assert res == {'conflicts': 0, 'rows': [], 'truncated': False,
                   'verdict': 'OK'}


def _seed_manager_dob_fixture(duck_conn, tm_dob: str):
    """xref_manager + fotmob_manager_profile + transfermarkt_coaches."""
    duck_conn.execute(
        """
        CREATE TABLE iceberg.silver.xref_manager (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR,
            confidence VARCHAR, match_score DOUBLE
        )
        """
    )
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_manager VALUES "
        "('coach_a', 'fbref', 'Coach A', 'Coach A', 'ENG', '2425', "
        " 'name_normalize', NULL), "
        "('coach_a', 'fotmob', '500', 'Coach A', 'ENG', '2425', "
        " 'name_normalize', NULL), "
        "('coach_a', 'transfermarkt', '900', 'C. A', 'ENG', '2425', "
        " 'name_initial', NULL)"
    )
    duck_conn.execute(
        "CREATE TABLE iceberg.silver.fotmob_manager_profile AS "
        "SELECT '500' AS player_id, 'Coach A' AS name, "
        "'1970-01-01' AS date_of_birth, 'ENG' AS league, '2425' AS season"
    )
    duck_conn.execute(
        f"CREATE TABLE iceberg.silver.transfermarkt_coaches AS "
        f"SELECT '900' AS coach_id, 'C. A' AS name, DATE '{tm_dob}' AS dob, "
        f"'ENG' AS league, '2425' AS season"
    )


def test_evaluate_manager_dob_collisions_flags_mismatch(duck_conn):
    """FotMob-vs-TM dob disagreement on one canonical → WARNING with the TM
    row's confidence exposed (name_initial = suspected false merge)."""
    _seed_manager_dob_fixture(duck_conn, tm_dob='1965-05-05')
    res = xref_dq.evaluate_manager_dob_collisions()
    assert res['verdict'] == 'WARNING'
    assert res['collisions'] == 1
    row = res['rows'][0]
    assert row['canonical_id'] == 'coach_a'
    assert row['tm_confidence'] == 'name_initial'
    assert row['fotmob_dob'] == '1970-01-01'
    assert row['tm_dob'] == '1965-05-05'


def test_evaluate_manager_dob_collisions_ok_when_agreeing(duck_conn):
    _seed_manager_dob_fixture(duck_conn, tm_dob='1970-01-01')
    res = xref_dq.evaluate_manager_dob_collisions()
    assert res['verdict'] == 'OK'
    assert res['collisions'] == 0
    assert res['rows'] == []
