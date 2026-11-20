from __future__ import annotations

import json

from dags.scripts import transfermarkt_top5_history as cli
from dags.utils import transfermarkt_top5_history as top5


def _pins(offset=0):
    return {
        table: 3000 + offset + index
        for index, table in enumerate(top5.ENTITY_TABLES.values())
    }


def _coverage_rows(scopes):
    return [
        {
            'entity': entity,
            'competition_id': scope.competition_id,
            'edition_id': scope.edition_id,
            'native_rows': 1,
            'full_lineage_rows': 1,
            'null_scope_rows': 0,
        }
        for scope in scopes
        for entity in top5.ENTITY_TABLES
    ]


def _reconciliation_rows():
    return [
        {
            'entity': entity,
            'key_groups': 3,
            'native_rows': 3,
            'legacy_rows': 3,
            'legacy_only_groups': 0,
            'native_only_groups': 0,
            'count_mismatch_groups': 0,
        }
        for entity in top5.ROSTER_ENTITIES
    ]


def _write(path, value):
    path.write_text(json.dumps(value), encoding='utf-8')
    return str(path)


def test_matrix_command(capsys):
    assert cli.main(['matrix']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['issue'] == 1403
    assert len(result['scopes']) == 55


def test_sql_commands_use_supplied_pins(tmp_path, capsys):
    coverage_path = _write(tmp_path / 'coverage-pins.json', _pins())
    assert cli.main(['coverage-sql', '--snapshots-json', coverage_path]) == 0
    coverage_sql = capsys.readouterr().out
    assert coverage_sql.startswith('WITH')
    assert 'FOR VERSION AS OF 3000' in coverage_sql

    roster_pins = {
        top5.ENTITY_TABLES[entity]: 4000 + index
        for index, entity in enumerate(top5.ROSTER_ENTITIES)
    }
    roster_pins[top5.LEGACY_PLAYERS_TABLE] = 4003
    roster_path = _write(tmp_path / 'roster-pins.json', roster_pins)
    assert cli.main([
        'reconciliation-sql',
        '--competition-id', 'GB1',
        '--edition-id', '2025',
        '--snapshots-json', roster_path,
    ]) == 0
    roster_sql = capsys.readouterr().out
    assert 'FULL OUTER JOIN' in roster_sql
    assert "'ENG-Premier League'" in roster_sql
    assert 'FOR VERSION AS OF 4003' in roster_sql


def test_coverage_gate_accepts_array_and_wrapped_results(tmp_path, capsys):
    scope = top5.scope_for('GB1', '2025')
    rows = _coverage_rows((scope,))
    for index, payload in enumerate((rows, {'results': rows})):
        path = _write(tmp_path / f'rows-{index}.json', payload)
        assert cli.main([
            'check-coverage', '--rows-json', path,
            '--competition-id', 'GB1', '--edition-id', '2025',
        ]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result == {
            'status': 'PASS', 'scopes': 1,
            'violations': [], 'quarantine_holds': [],
        }


def test_coverage_gate_fails_with_structured_violations(tmp_path, capsys):
    scope = top5.scope_for('GB1', '2025')
    rows = _coverage_rows((scope,))
    rows[0]['null_scope_rows'] = 2
    path = _write(tmp_path / 'rows.json', rows[1:] + [rows[0]])
    assert cli.main([
        'check-coverage', '--rows-json', path,
        '--competition-id', 'GB1', '--edition-id', '2025',
    ]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'FAIL'
    assert any(item['kind'] == 'null_scope' for item in result['violations'])


def test_native_gate_preserves_quarantine_hold_without_final_pass(tmp_path, capsys):
    scope = top5.scope_for('GB1', '2025')
    rows = _coverage_rows((scope,))
    rows[0]['null_scope_rows'] = 2
    path = _write(tmp_path / 'native.json', rows)
    assert cli.main([
        'check-coverage', '--rows-json', path,
        '--competition-id', 'GB1', '--edition-id', '2025',
        '--gate', 'native',
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'NATIVE_PASS_QUARANTINE_HOLD'
    assert result['violations'] == []
    assert result['quarantine_holds'][0]['kind'] == 'null_scope'

    rows[0]['native_rows'] = 0
    rows[0]['full_lineage_rows'] = 0
    path = _write(tmp_path / 'native-fail.json', rows)
    assert cli.main([
        'check-coverage', '--rows-json', path,
        '--competition-id', 'GB1', '--edition-id', '2025',
        '--gate', 'native',
    ]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'FAIL'
    assert any(item['kind'] == 'missing_native' for item in result['violations'])


def test_reconciliation_gate_success_and_failure(tmp_path, capsys):
    path = _write(tmp_path / 'valid.json', _reconciliation_rows())
    assert cli.main(['check-reconciliation', '--rows-json', path]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'PASS'

    rows = _reconciliation_rows()
    rows[0]['legacy_only_groups'] = 1
    path = _write(tmp_path / 'invalid.json', rows)
    assert cli.main(['check-reconciliation', '--rows-json', path]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['violations'][0]['kind'] == 'legacy_only'


def test_bad_inputs_return_two_without_traceback(tmp_path, capsys):
    path = _write(tmp_path / 'bad-pins.json', {'wrong.table': 1})
    assert cli.main(['coverage-sql', '--snapshots-json', path]) == 2
    captured = capsys.readouterr()
    assert 'snapshot' in captured.err
    assert 'Traceback' not in captured.err

    path = _write(tmp_path / 'pins.json', _pins())
    assert cli.main([
        'reconciliation-sql', '--competition-id', 'GB1', '--edition-id', '2026',
        '--snapshots-json', path,
    ]) == 2
    captured = capsys.readouterr()
    assert 'outside' in captured.err
    assert 'Traceback' not in captured.err
