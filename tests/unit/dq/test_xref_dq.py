"""Unit tests for ``dags.utils.xref_dq`` (T6 deliverable).

Strategy
--------
Two layers:

1. **Pure-Python tests** for builders + helpers — verify the structure of
   the ``Check`` lists and the SQL strings they embed (enum allow-lists,
   table names, severities). No Trino, no DuckDB — read-only attribute
   inspection of the dataclasses returned.

2. **DuckDB-bridged tests** for the parity validators and orphan-rate
   evaluator — same approach as ``tests/unit/dq/test_e5_checks.py``:
   monkey-patch ``utils.data_quality._get_conn`` to return an in-memory
   DuckDB connection seeded with the schemas the parity SQL touches.
   This exercises the real SQL generation and result parsing without
   reaching out to a real Trino cluster.

The tests do NOT call ``run_checks`` over the DuckDB bridge for the
``check_enum_compliance`` helper because the universal data_quality
runners use Trino-specific operators in some checks; pure builder
inspection is sufficient to lock the contract.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Wire dags/ onto sys.path so ``utils.xref_dq`` resolves.
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

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
    """In-memory DuckDB pre-loaded with the xref + entity_xref schemas."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
    # NB: DuckDB blocks user-CREATE in information_schema, so we don't try
    # to seed it here. Tests instead monkey-patch ``xref_dq._legacy_table_exists``
    # directly when needed (default = True for parity tests).

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
    # gold.entity_xref (legacy)
    con.execute(
        """
        CREATE TABLE iceberg.gold.entity_xref (
            entity_type  VARCHAR,
            source       VARCHAR,
            source_id    VARCHAR,
            canonical_id VARCHAR,
            display_name VARCHAR,
            league       VARCHAR,
            season       VARCHAR
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
    # Default: pretend legacy entity_xref exists so parity SQL runs against
    # the seeded DuckDB schema. Individual tests can override with monkeypatch.
    monkeypatch.setattr(xref_dq, "_legacy_table_exists", lambda: True)
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


def test_xref_match_only_fbref():
    """E1 MVP: xref_match source enum allow-list = ['fbref']."""
    checks = xref_dq.build_xref_match_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    assert "'fbref'" in where
    # No other sources allowed
    for forbidden in ['understat', 'whoscored', 'sofascore']:
        assert f"'{forbidden}'" not in where


def test_xref_referee_source_enum():
    checks = xref_dq.build_xref_referee_checks()
    src_checks = [c for c in checks if 'enum_compliance' in c.name and '.source' in c.name]
    assert len(src_checks) == 1
    where = src_checks[0].params['where']
    assert "'fbref'" in where
    assert "'matchhistory'" in where


def test_xref_manager_zero_rows():
    """STUB phase — xref_manager must have a row_count(min=0, max=0) ERROR."""
    checks = xref_dq.build_xref_manager_checks()
    rc = [c for c in checks if c.kind == 'row_count']
    assert any(
        c.params.get('min_rows') == 0 and c.params.get('max_rows') == 0
        and c.severity == 'ERROR'
        for c in rc
    )


def test_xref_player_confidence_enum_full():
    """xref_player confidence enum includes all 5 cascade tiers."""
    checks = xref_dq.build_xref_player_checks()
    enum_checks = [c for c in checks if 'enum_compliance' in c.name and 'confidence' in c.name]
    assert len(enum_checks) == 1
    where = enum_checks[0].params['where']
    for tier in ['exact', 'name_team', 'name_team_jersey', 'name_team_dob', 'orphan']:
        assert f"'{tier}'" in where


def test_xref_player_canonical_id_format_check():
    """xref_player must enforce canonical_id ^(fb|us|ws|ss)_.+ regex."""
    checks = xref_dq.build_xref_player_checks()
    fmt_checks = [c for c in checks if 'canonical_id_format' in c.name]
    assert len(fmt_checks) == 1
    where = fmt_checks[0].params['where']
    assert "regexp_like" in where
    assert "fb|us|ws|ss" in where


def test_build_all_xref_checks_aggregates():
    """Aggregated list ≥ 16 checks (4 team + 5 match + 5 referee + 2 manager + 6 player)."""
    checks = xref_dq.build_all_xref_checks()
    # The exact target is per the T6 spec: ≥15
    assert len(checks) >= 15, f"Expected ≥15 checks, got {len(checks)}"


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
# DuckDB-bridged tests — orphan-rate + parity validators
# ===========================================================================

def test_orphan_rate_per_source_classifies_correctly(duck_conn):
    """Verify the OK/WARNING/ERROR cascade on synthetic orphan distribution."""
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_player VALUES "
        # fbref — 100 rows, 0 orphans → OK
        + ", ".join(
            f"('fb_p{i}', 'fbref', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"{'\'orphan\'' if False else '\'exact\''}, NULL)"
            for i in range(100)
        )
        + ", "
        # understat — 100 rows, 12 orphans → WARNING (12% > 10% but <25%)
        + ", ".join(
            f"('us_p{i}', 'understat', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"{'\'orphan\'' if i < 12 else '\'name_team\''}, NULL)"
            for i in range(100)
        )
        + ", "
        # whoscored — 100 rows, 30 orphans → ERROR (>25%)
        + ", ".join(
            f"('ws_p{i}', 'whoscored', 'p{i}', 'P{i}', 'ENG', '2024', "
            f"{'\'orphan\'' if i < 30 else '\'name_team\''}, NULL)"
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


def test_parity_check_team_returns_metrics_shape(duck_conn):
    """Empty Silver + empty Gold → matched=0 silver_only=0 gold_only=0."""
    res = xref_dq.parity_check_xref_team_vs_gold()
    expected_keys = {
        'silver_rows', 'gold_legacy_rows', 'matched_pairs',
        'silver_only', 'gold_only', 'cid_diff',
        'canonical_id_match_pct', 'sample_diffs', 'verdict',
    }
    assert expected_keys.issubset(res.keys())
    assert res['silver_rows'] == 0
    assert res['gold_legacy_rows'] == 0
    assert res['verdict'] == 'PARITY_OK'
    assert res['canonical_id_match_pct'] == 1.0


def test_parity_check_team_detects_full_match(duck_conn):
    """Identical (source, source_id, league, season, canonical_id) in both → PARITY_OK."""
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_team VALUES "
        "('arsenal', 'fbref', 'Arsenal', 'Arsenal', 'ENG', '2024', 'name_alias', NULL),"
        "('liverpool', 'fbref', 'Liverpool', 'Liverpool', 'ENG', '2024', 'name_alias', NULL)"
    )
    duck_conn.execute(
        "INSERT INTO iceberg.gold.entity_xref VALUES "
        "('team', 'fbref', 'Arsenal', 'arsenal', 'Arsenal', 'ENG', '2024'),"
        "('team', 'fbref', 'Liverpool', 'liverpool', 'Liverpool', 'ENG', '2024')"
    )
    res = xref_dq.parity_check_xref_team_vs_gold()
    assert res['verdict'] == 'PARITY_OK'
    assert res['matched_pairs'] == 2
    assert res['cid_diff'] == 0
    assert res['silver_only'] == 0
    assert res['gold_only'] == 0
    assert res['canonical_id_match_pct'] == 1.0


def test_parity_check_team_detects_diffs(duck_conn):
    """Mix of matched / diff / silver_only / gold_only → DIFF_DETECTED."""
    # Silver: 3 fbref teams + 1 understat (silver_only since legacy is FBref-only)
    duck_conn.execute(
        "INSERT INTO iceberg.silver.xref_team VALUES "
        "('arsenal', 'fbref', 'Arsenal', 'Arsenal', 'ENG', '2024', 'name_alias', NULL),"
        "('liverpool', 'fbref', 'Liverpool', 'Liverpool', 'ENG', '2024', 'name_alias', NULL),"
        "('manchester_united', 'fbref', 'Man United', 'Man United', 'ENG', '2024', 'name_alias', NULL),"
        "('us_some_team', 'understat', 'Some Team', 'Some Team', 'ENG', '2024', 'orphan', NULL)"
    )
    # Gold legacy: 2 matched + 1 cid_diff + 1 gold_only
    duck_conn.execute(
        "INSERT INTO iceberg.gold.entity_xref VALUES "
        "('team', 'fbref', 'Arsenal', 'arsenal', 'Arsenal', 'ENG', '2024'),"
        "('team', 'fbref', 'Liverpool', 'liverpool', 'Liverpool', 'ENG', '2024'),"
        "('team', 'fbref', 'Man United', 'man_united', 'Man United', 'ENG', '2024'),"
        "('team', 'fbref', 'Tottenham', 'tottenham', 'Tottenham', 'ENG', '2024')"
    )
    res = xref_dq.parity_check_xref_team_vs_gold()
    assert res['verdict'] == 'DIFF_DETECTED'
    # Arsenal/Liverpool are perfect matches; Man United is cid_diff;
    # Tottenham is gold_only; understat row is silver_only.
    assert res['cid_diff'] == 1
    assert res['silver_only'] == 1
    assert res['gold_only'] == 1
    assert res['matched_pairs'] == 3  # matched (2) + cid_diff (1)
    # 2 of 3 matched_pairs have identical canonical_id → 2/3 ≈ 0.6667
    assert abs(res['canonical_id_match_pct'] - (2 / 3)) < 0.01
    assert len(res['sample_diffs']) >= 1


def test_parity_legacy_absent_returns_marker(duck_conn, monkeypatch):
    """If _legacy_table_exists() returns False → LEGACY_ABSENT verdict."""
    monkeypatch.setattr(xref_dq, "_legacy_table_exists", lambda: False)
    res = xref_dq.parity_check_xref_team_vs_gold()
    assert res['verdict'] == 'LEGACY_ABSENT'
    # No SQL ran against entity_xref, so all counts stay zero.
    assert res['silver_rows'] == 0
    assert res['gold_legacy_rows'] == 0


def test_maybe_alert_parity_below_threshold(monkeypatch):
    """All branches below threshold → no Telegram call.

    With the T4 alert split (REGRESSION on gold_only > 0, INFO on cid_diff
    growth, THRESHOLD on total >= diff_threshold) the ONLY way to get a
    no-op outcome is: gold_only=0 everywhere AND team.cid_diff <= baseline+
    tolerance AND total < diff_threshold for every entity.
    """
    sent: list = []

    def fake_send_telegram(*_a, **_kw):
        sent.append(_a)
        return True

    monkeypatch.setattr(
        'utils.alerts._send_telegram', fake_send_telegram, raising=False
    )

    parity = {
        # silver_only is allowed at any size (Silver expands beyond legacy).
        # gold_only MUST be 0 — otherwise REGRESSION fires.
        'team':   {'silver_only': 5, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.99},
        'match':  {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'player': {'silver_only': 10, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.99},
    }
    alerted = xref_dq.maybe_alert_parity(parity, diff_threshold=100)
    assert alerted is False
    assert sent == []


def test_maybe_alert_parity_above_threshold(monkeypatch):
    """Diff totals at/above threshold → Telegram called once."""
    sent: list = []

    def fake_send_telegram(message, **_kw):
        sent.append(message)
        return True

    # Patch both the module-level reference and the alerts source.
    monkeypatch.setattr(
        'utils.alerts._send_telegram', fake_send_telegram, raising=False
    )

    parity = {
        'team':   {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'match':  {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'player': {'silver_only': 100, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.95},
    }
    alerted = xref_dq.maybe_alert_parity(parity, diff_threshold=100)
    assert alerted is True
    assert len(sent) == 1
    assert 'parity' in sent[0].lower()


# ===========================================================================
# E1.5 post-cutover checks (T4 deliverable)
# ===========================================================================

def test_post_cutover_check_dim_team_ref_silver():
    """build_e1_5_post_cutover_checks emits a WHERE-based ref_integrity
    check on dim_team.team_id ⊆ silver.xref_team(source='fbref').

    The runner uses ``row_count(max=0, where=...)`` because the universal
    CHECK.ref_integrity primitive has no WHERE-filter mode (yet).
    """
    checks = xref_dq.build_e1_5_post_cutover_checks()
    target = [
        c for c in checks
        if c.name == 'ref_integrity[dim_team.team_id->silver.xref_team(fbref)]'
    ]
    assert len(target) == 1, "Expected exactly one dim_team→silver.xref_team check"
    chk = target[0]
    assert chk.kind == 'row_count'
    assert chk.severity == 'WARNING', "Prep PR — WARNING during gate-watch"
    assert chk.params['table'] == 'iceberg.gold.dim_team'
    assert chk.params['min_rows'] == 0
    assert chk.params['max_rows'] == 0
    where = chk.params['where']
    # SQL must reference the silver source-of-truth and filter by FBref.
    assert 'iceberg.silver.xref_team' in where
    assert "source = 'fbref'" in where
    assert 'team_id NOT IN' in where


def test_post_cutover_canonical_format_checks_present():
    """dim_player.player_id and fct_player_match.player_id must both be
    guarded against canonical-prefix drift (regex '^fb_').

    These live in build_e1_5_post_cutover_checks at WARNING severity until
    the cutover-merge follow-up PR.
    """
    checks = xref_dq.build_e1_5_post_cutover_checks()
    fmt = [c for c in checks if 'canonical_format' in c.name]
    assert len(fmt) == 2, f"Expected 2 canonical_format checks, got {len(fmt)}"
    names = {c.name for c in fmt}
    assert 'canonical_format[dim_player.player_id]' in names
    assert 'canonical_format[fct_player_match.player_id]' in names
    for c in fmt:
        assert c.kind == 'row_count'
        assert c.severity == 'WARNING'
        assert "regexp_like(player_id, '^fb_.+')" in c.params['where']


def test_post_cutover_six_checks_total():
    """Sanity: 6 checks total — 4 ref_integrity + 2 canonical_format."""
    checks = xref_dq.build_e1_5_post_cutover_checks()
    assert len(checks) == 6
    # All checks ship at WARNING in the prep PR.
    assert all(c.severity == 'WARNING' for c in checks)


def test_telegram_alert_on_gold_only_regression(monkeypatch):
    """gold_only > 0 anywhere → REGRESSION-tagged Telegram message.

    This is a NEW alert branch (prior to T4 there was only the threshold
    branch). gold_only > 0 means a row exists in legacy but disappeared in
    Silver xref → the new SQL refactor lost coverage during gate-watch.
    """
    sent: list = []

    def fake_send_telegram(message, **_kw):
        sent.append(message)
        return True

    monkeypatch.setattr(
        'utils.alerts._send_telegram', fake_send_telegram, raising=False
    )

    parity = {
        # team has a single legacy-only row — regression!
        'team':   {'silver_only': 0, 'gold_only': 1, 'cid_diff': 0,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.99},
        'match':  {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'player': {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
    }
    alerted = xref_dq.maybe_alert_parity(parity, diff_threshold=100)
    assert alerted is True
    # At least one of the messages must carry the REGRESSION tag.
    assert any('REGRESSION' in m for m in sent), \
        f"Expected REGRESSION-tagged alert, got messages: {sent}"


def test_telegram_alert_on_cid_diff_growth(monkeypatch):
    """team.cid_diff > baseline + tolerance → INFO cid_diff_growth message.

    Baseline (2026-05-09) is 43; default tolerance is 5 → trigger at >48.
    """
    sent: list = []

    def fake_send_telegram(message, **_kw):
        sent.append(message)
        return True

    monkeypatch.setattr(
        'utils.alerts._send_telegram', fake_send_telegram, raising=False
    )

    # team.cid_diff = 60 — clearly above 43 + 5
    # Pre-cutover the legacy player row uses raw player_id while silver uses
    # 'fb_<id>', so EVERY FBref player row appears as cid_diff. The default
    # diff_threshold=100 is sized for this state — to isolate the cid_diff
    # growth branch we keep player diffs below 100 in the fixture.
    parity = {
        'team':   {'silver_only': 0, 'gold_only': 0, 'cid_diff': 60,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.85},
        'match':  {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'player': {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
    }
    alerted = xref_dq.maybe_alert_parity(
        parity, diff_threshold=100, cid_diff_growth_threshold=5,
    )
    assert alerted is True
    # At least one of the messages must carry the cid_diff_growth INFO tag.
    assert any('cid_diff_growth' in m for m in sent), \
        f"Expected cid_diff_growth INFO alert, got messages: {sent}"
    # AND it should NOT include the REGRESSION tag (no gold_only).
    assert not any('REGRESSION' in m for m in sent), \
        f"Did not expect REGRESSION tag, got messages: {sent}"


def test_telegram_alert_baseline_no_growth_no_alert(monkeypatch):
    """team.cid_diff at baseline (43) → no growth alert, no threshold alert.

    Establishes that the baseline state itself does NOT spam during the
    gate-watch window — alerts only fire on drift FROM baseline.
    """
    sent: list = []

    def fake_send_telegram(message, **_kw):
        sent.append(message)
        return True

    monkeypatch.setattr(
        'utils.alerts._send_telegram', fake_send_telegram, raising=False
    )

    parity = {
        'team':   {'silver_only': 0, 'gold_only': 0, 'cid_diff': 43,
                   'verdict': 'DIFF_DETECTED', 'canonical_id_match_pct': 0.785},
        'match':  {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
        'player': {'silver_only': 0, 'gold_only': 0, 'cid_diff': 0,
                   'verdict': 'PARITY_OK', 'canonical_id_match_pct': 1.0},
    }
    alerted = xref_dq.maybe_alert_parity(parity, diff_threshold=100)
    assert alerted is False
    assert sent == []
