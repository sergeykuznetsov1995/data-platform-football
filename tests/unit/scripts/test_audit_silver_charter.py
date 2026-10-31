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
        # #404: the SEASON_YEAR_START_OK allowlist is retired — a bigint season
        # is now a hard ERROR for EVERY Silver table (formerly-allowlisted ones too).
        cols = [('_silver_created_at', 'timestamp(6) with time zone'),
                ('league', 'varchar'), ('season', 'bigint')]
        findings = mod.audit_schema(_FakeCursor(cols), 'fbref_player_match_stats')
        s2 = _s2(findings)
        assert len(s2) == 1
        assert s2[0]['sev'] == 'ERROR'
        assert 'expected varchar slug' in s2[0]['detail']

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
        # whoscored_player_season_aggregate is a registry EXCEPTION — sanctioned,
        # must not block (the 4 *_team_season rollups were migrated out in #370).
        findings = [{'rule': 'R1', 'sev': 'ERROR', 'detail': 'season-grain rollup'}]
        assert mod.has_unsanctioned_error('whoscored_player_season_aggregate', findings) is False

    def test_migrated_team_season_no_longer_sanctioned(self):
        # #370 team-wave: the 4 *_team_season rollups moved to Gold; their silver
        # SQL was deleted, so they are no longer in the registry. fbref_team_season_profile
        # stays in Silver but is now COMPLIANT (season-from-season conform, not a rollup).
        for t in ('fotmob_team_season', 'understat_team_season',
                  'whoscored_team_season', 'sofascore_team_season',
                  'fbref_team_season_profile'):
            assert t not in mod.SANCTIONED

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


class TestFallbackFilesExcluded:
    """`*_empty.sql` fallbacks are not standalone tables — excluded from scan (#369)."""

    def test_sofifa_empty_fallback_not_in_registry(self):
        # Resolved #369: removed from SANCTIONED (was INVESTIGATE 'possible dead stub').
        assert 'sofifa_player_profile_empty' not in mod.SANCTIONED

    def test_silver_sql_files_excludes_empty_fallbacks(self):
        stems = {f.name.replace('.sql.j2', '').replace('.sql', '')
                 for f in mod.silver_sql_files()}
        # The empty fallback must not surface as a phantom table...
        assert 'sofifa_player_profile_empty' not in stems
        # ...while the real table it materializes is still audited.
        assert 'sofifa_player_profile' in stems
        # No `*_empty` stem leaks through the filter.
        assert not any(s.endswith(mod.FALLBACK_SUFFIX) for s in stems)


# ---- Charter §8: FotMob core schema (#2016) --------------------------------

_FOTMOB_CORE = {
    'context_columns': ['competition_id', 'source_season_key'],
    'lineage_columns': [{'name': '_bronze_ingested_at', 'type': 'TIMESTAMP(6)'}],
    'tables': {
        'fotmob_matches': {
            'scope': 'context',
            'columns': [
                {'name': 'competition_id', 'type': 'BIGINT'},
                {'name': 'source_season_key', 'type': 'VARCHAR'},
                {'name': 'match_id', 'type': 'BIGINT'},
                {'name': 'kickoff_utc', 'type': 'TIMESTAMP(6) WITH TIME ZONE'},
            ],
            'raw_json': [{'name': 'h2h_json'}],
        },
        'fotmob_players': {
            'scope': 'global',
            'columns': [{'name': 'player_id', 'type': 'BIGINT'}],
        },
    },
}

_MATCHES_OK = [('competition_id', 'bigint'), ('source_season_key', 'varchar'),
               ('match_id', 'bigint'), ('kickoff_utc', 'timestamp(6) with time zone'),
               ('h2h_json', 'varchar'), ('_bronze_ingested_at', 'timestamp(6)'),
               ('_silver_created_at', 'timestamp(6)')]


_CORES = {'fotmob': _FOTMOB_CORE}


def _schema(table, cols, cores=_CORES):
    return mod.audit_schema(_FakeCursor(cols), table, cores)


class TestFotmobCoreSchema:
    def test_declared_context_table_without_league_season_is_clean(self):
        assert _schema('fotmob_matches', _MATCHES_OK) == []

    def test_global_profile_needs_no_context_columns(self):
        cols = [('player_id', 'bigint'), ('_bronze_ingested_at', 'timestamp(6)'),
                ('_silver_created_at', 'timestamp(6)')]
        assert _schema('fotmob_players', cols) == []

    def test_missing_context_column_is_error(self):
        cols = [c for c in _MATCHES_OK if c[0] != 'source_season_key']
        rules = {(f['rule'], f['sev']) for f in _schema('fotmob_matches', cols)}
        assert ('S1', 'ERROR') in rules and ('S4', 'ERROR') in rules

    def test_type_drift_is_error(self):
        cols = [(n, 'varchar' if n == 'competition_id' else t) for n, t in _MATCHES_OK]
        found = _schema('fotmob_matches', cols)
        assert [f['sev'] for f in found if f['rule'] == 'S4'] == ['ERROR']

    def test_undeclared_column_is_warn(self):
        found = _schema('fotmob_matches', _MATCHES_OK + [('extra_col', 'varchar')])
        assert [(f['rule'], f['sev']) for f in found] == [('S4', 'WARN')]

    def test_other_sources_keep_league_season_and_bare_id_rules(self):
        # The FotMob rules must not weaken the charter for any other source.
        found = _schema('sofascore_shots', [('shot_id', 'bigint'),
                                            ('_silver_created_at', 'timestamp(6)')])
        rules = {(f['rule'], f['sev']) for f in found}
        assert ('S1', 'ERROR') in rules and ('S3', 'WARN') in rules

    def test_undeclared_fotmob_table_falls_back_to_standard_rules(self):
        found = _schema('fotmob_shots', [('shot_id', 'bigint'),
                                         ('_silver_created_at', 'timestamp(6)')])
        assert {f['rule'] for f in found} >= {'S1', 'S3'}


class TestFotmobCoreLayerA:
    def _audit(self, tmp_path, stem):
        f = tmp_path / f'{stem}.sql.j2'
        f.write_text('SELECT match_id FROM iceberg.bronze.fotmob_matches_current\n')
        return mod.audit_file(f, _CORES)

    def test_undeclared_fotmob_file_is_unsanctioned_error(self, tmp_path):
        found = self._audit(tmp_path, 'fotmob_match_extras')
        assert [f['rule'] for f in found if f['sev'] == 'ERROR'] == ['R7']
        assert mod.has_unsanctioned_error('fotmob_match_extras', found)

    def test_declared_fotmob_file_skips_naming_heuristic(self, tmp_path):
        # 'fotmob_players' has a grain token anyway; use a declared name without one.
        cores = {'fotmob': {'tables': {'fotmob_standings': {'scope': 'context'}}}}
        f = tmp_path / 'fotmob_standings.sql'
        f.write_text('SELECT 1 AS x FROM iceberg.bronze.fotmob_standings_current\n')
        assert mod.audit_file(f, cores) == []

    def test_non_fotmob_naming_rule_unchanged(self, tmp_path):
        f = tmp_path / 'sofascore_standings.sql'
        f.write_text('SELECT 1 AS x FROM iceberg.bronze.sofascore_standings\n')
        assert [f_['rule'] for f_ in mod.audit_file(f, _CORES)] == ['R5']

    def test_only_fotmob_is_a_core_schema_source(self):
        # #2016 registers FotMob only; other sources opt in with their own schema file.
        assert set(mod.CORE_SCHEMAS) == {'fotmob'}

    def test_missing_schema_file_loads_empty(self, tmp_path):
        assert mod._load_yaml(tmp_path / 'absent.yaml') == {}
