
import pytest

from scripts import reconcile_fotmob_silver as reconcile

pytestmark = pytest.mark.unit


def scopes():
    return {table: [[230, '2026/2027 - Apertura' if table in reconcile.SEASONED else '']]
            for table in reconcile.SCOPED}


def test_discovery_covers_removed_keys_and_has_no_pilot_filter():
    queries = reconcile.discovery('iceberg.silver', 'b')
    assert set(queries) == set(reconcile.SCOPED)
    for table, sql in queries.items():
        assert 'UNION' in sql and f'iceberg.silver.{table}_b' in sql
        assert '47' not in sql and '112' not in sql


def test_materialize_all22_and_preserve_parent_match_scope():
    units = reconcile.materialize('iceberg.silver', 'a', scopes())
    assert {u['table'] for u in units} == set(reconcile.build.ALL_TABLES)
    assert len({u['id'] for u in units}) == len(units)
    match = next(u['sql'] for u in units if u['table'] == 'fotmob_matches')
    assert "competition_id = '230'" in match
    assert "expected WHERE competition_id = 230 AND source_season_key = '2026/2027 - Apertura'" in match
    assert '{{' not in match
    assert sum(u['table'] == 'fotmob_players' for u in units) == 8


@pytest.mark.parametrize('schema,slot', [('silver;DROP TABLE x', 'a'), ('iceberg.silver', 'c')])
def test_reject_identifiers(schema, slot):
    with pytest.raises(ValueError):
        reconcile.discovery(schema, slot)


def test_incomplete_discovery_and_duplicate_keys_rejected():
    with pytest.raises(ValueError):
        reconcile.materialize('iceberg.silver', 'a', {})
    values = scopes()
    values['fotmob_matches'] *= 2
    with pytest.raises(ValueError):
        reconcile.materialize('iceberg.silver', 'a', values)


def test_literal_quotes_are_escaped():
    values = scopes()
    values['fotmob_matches'] = [[230, "season's end"]]
    sql = reconcile.materialize('iceberg.silver', 'a', values)[0]['sql']
    assert "season''s end" in sql


def test_report_never_passes_drift_or_partial_results():
    guards = {'sources': [1], 'reader_state': [2], 'build_state': [3]}
    assert reconcile.classify(['a'], {'a': [3, 3]}, guards, guards) == 'equal_counts'
    assert reconcile.classify(['a'], {'a': [3, 4]}, guards, guards) == 'differences_require_explanation'
    assert reconcile.classify(['a', 'b'], {'a': [3, 3]}, guards, guards) == 'incomplete'
    assert reconcile.classify(['a'], {'a': [3, 3]}, guards, {**guards, 'sources': [4]}).startswith('inconclusive')
    assert reconcile.classify(['a'], {'a': [3, 3]}, {}, {}).startswith('inconclusive')
    assert reconcile.classify(['a'], {'a': [-1, -1]}, guards, guards) == 'incomplete'


def test_cli_never_executes_and_defaults_to_discovery(capsys):
    assert reconcile.main(['--slot', 'a']) == 0
    output = capsys.readouterr().out
    assert 'disabled; SQL preparation only' in output and 'discovery' in output


def test_standalone_entrypoint_does_not_need_pytest_pythonpath(tmp_path):
    import os
    import subprocess
    import sys
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    result = subprocess.run([sys.executable, str(reconcile.ROOT / 'scripts/reconcile_fotmob_silver.py'),
                             '--slot', 'b'], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert '"discovery"' in result.stdout
