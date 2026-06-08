"""
Unit tests for scripts/audit_silver_charter.py — Layer B rule S2 (season type).

Strategy
--------
``audit_silver_charter`` is a top-level script (not a package), so we load it
via ``importlib.util`` from its absolute path. ``audit_schema(cur, table)`` is
pure given a cursor whose ``DESCRIBE`` result we fake — no Trino, no network.

What we cover
-------------
S2 sanctioned year-start allowlist (#373):
- a year-start table (e.g. fbref_player_match_stats) with bigint ``season``
  yields a **WARN**, not an ERROR.
- a non-allowlisted table with bigint ``season`` still yields an **ERROR**.
- a table with proper varchar ``season`` yields no S2 finding.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'audit_silver_charter.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('audit_silver_charter', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['audit_silver_charter'] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


class _FakeCursor:
    """Returns a canned DESCRIBE result: list of (column, type) rows."""

    def __init__(self, cols):
        self._cols = cols

    def execute(self, sql):
        pass

    def fetchall(self):
        # audit_schema reads r[0] (name) and r[1] (type) only.
        return [(name, dtype) for name, dtype in self._cols]


def _s2(findings):
    return [f for f in findings if f['rule'] == 'S2']


class TestS2SeasonYearStartAllowlist:
    def test_year_start_table_with_bigint_season_is_warn(self):
        cols = [('_silver_created_at', 'timestamp(6) with time zone'),
                ('league', 'varchar'), ('season', 'bigint')]
        findings = mod.audit_schema(_FakeCursor(cols), 'fbref_player_match_stats')
        s2 = _s2(findings)
        assert len(s2) == 1
        assert s2[0]['sev'] == 'WARN'
        assert 'sanctioned year-start' in s2[0]['detail']

    def test_non_allowlisted_table_with_bigint_season_is_error(self):
        cols = [('_silver_created_at', 'timestamp(6) with time zone'),
                ('league', 'varchar'), ('season', 'bigint')]
        findings = mod.audit_schema(_FakeCursor(cols), 'some_other_table')
        s2 = _s2(findings)
        assert len(s2) == 1
        assert s2[0]['sev'] == 'ERROR'

    def test_varchar_season_yields_no_s2(self):
        cols = [('_silver_created_at', 'timestamp(6) with time zone'),
                ('league', 'varchar'), ('season', 'varchar')]
        findings = mod.audit_schema(_FakeCursor(cols), 'fbref_player_match_stats')
        assert _s2(findings) == []


class TestUnsanctionedErrorGate:
    """has_unsanctioned_error — the --check gate condition (#372)."""

    def test_error_on_unsanctioned_table_trips_gate(self):
        findings = [{'rule': 'R6', 'sev': 'ERROR', 'detail': 'file contains DDL'}]
        assert mod.has_unsanctioned_error('some_new_table', findings) is True

    def test_error_on_sanctioned_table_does_not_trip_gate(self):
        # fotmob_team_season is a registry EXCEPTION — sanctioned, must not block.
        findings = [{'rule': 'R1', 'sev': 'ERROR', 'detail': 'season-grain rollup'}]
        assert mod.has_unsanctioned_error('fotmob_team_season', findings) is False

    def test_no_findings_does_not_trip_gate(self):
        assert mod.has_unsanctioned_error('some_new_table', []) is False

    def test_warn_only_does_not_trip_gate(self):
        findings = [{'rule': 'R2', 'sev': 'WARN', 'detail': 'reads silver.x'}]
        assert mod.has_unsanctioned_error('some_new_table', findings) is False

    def test_resolved_table_no_longer_sanctioned(self):
        # sofascore_team_match resolved (#367): removed from registry, now COMPLIANT.
        findings = [{'rule': 'R2', 'sev': 'WARN', 'detail': 'reads silver.x'}]
        # WARN-only → COMPLIANT-ish, gate not tripped:
        assert mod.has_unsanctioned_error('sofascore_team_match', findings) is False
        # but a hypothetical ERROR on it WOULD now trip (no longer sanctioned):
        err = [{'rule': 'R6', 'sev': 'ERROR', 'detail': 'ddl'}]
        assert mod.has_unsanctioned_error('sofascore_team_match', err) is True
        # verdict: clean findings on a non-registry table → COMPLIANT.
        assert mod.verdict_of('sofascore_team_match', []) == ('COMPLIANT', '')
