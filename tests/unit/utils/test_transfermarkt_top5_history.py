from __future__ import annotations

import json
import re

import pytest

from dags.utils import transfermarkt_top5_history as top5


PIN_TABLES = {
    'squad_memberships': 'iceberg.bronze.transfermarkt_squad_memberships',
    'player_attribute_observations': 'iceberg.bronze.transfermarkt_player_attribute_observations',
    'player_contract_observations': 'iceberg.bronze.transfermarkt_player_contract_observations',
    'market_value_points': 'iceberg.bronze.transfermarkt_market_value_points',
    'transfer_events': 'iceberg.bronze.transfermarkt_transfer_events',
}


def _pins(offset=0):
    return {
        table: 1000 + index + offset
        for index, table in enumerate(PIN_TABLES.values())
    }


def _coverage_rows(scopes):
    return [
        {
            'entity': entity,
            'competition_id': scope.competition_id,
            'edition_id': scope.edition_id,
            'native_rows': 2,
            'full_lineage_rows': 2,
            'null_scope_rows': 0,
        }
        for scope in scopes
        for entity in PIN_TABLES
    ]


def _reconciliation_rows():
    return [
        {
            'entity': entity,
            'key_groups': 10,
            'native_rows': 10,
            'legacy_rows': 10,
            'legacy_only_groups': 0,
            'native_only_groups': 0,
            'count_mismatch_groups': 0,
        }
        for entity in (
            'squad_memberships',
            'player_attribute_observations',
            'player_contract_observations',
        )
    ]


def test_acceptance_matrix_is_exact_and_persistent():
    scopes = top5.acceptance_scopes()
    assert len(scopes) == 55
    assert len({(item.competition_id, item.edition_id) for item in scopes}) == 55
    assert {item.competition_id for item in scopes} == {
        'GB1', 'ES1', 'IT1', 'L1', 'FR1',
    }
    assert {item.edition_id for item in scopes} == {
        str(year) for year in range(2015, 2026)
    }

    matrix = top5.load_acceptance_matrix()
    assert matrix['version'] == 1
    assert matrix['issue'] == 1403
    assert matrix['scopes'] == [item.as_dict() for item in scopes]
    assert set(matrix['entities']) == set(PIN_TABLES)


def test_scope_identity_and_legacy_mapping():
    from scrapers.transfermarkt.registry import deterministic_scope_id

    scope = top5.scope_for('GB1', '2025')
    assert scope.as_dict() == {
        'competition_id': 'GB1',
        'edition_id': '2025',
        'scope_id': deterministic_scope_id('GB1', '2025'),
        'legacy_league': 'ENG-Premier League',
        'legacy_season': '2526',
    }
    assert top5.scope_for('ES1', '2015').legacy_league == 'ESP-La Liga'
    assert top5.scope_for('IT1', '2015').legacy_league == 'ITA-Serie A'
    assert top5.scope_for('L1', '2015').legacy_league == 'GER-Bundesliga'
    assert top5.scope_for('FR1', '2015').legacy_league == 'FRA-Ligue 1'
    assert top5.scope_for('ES1', '2015').legacy_season == '1516'
    with pytest.raises(top5.Top5HistoryError, match='outside'):
        top5.scope_for('GB1', '2026')


def test_matrix_tampering_fails_closed(tmp_path):
    matrix = top5.load_acceptance_matrix()
    matrix['scopes'].pop()
    path = tmp_path / 'matrix.json'
    path.write_text(json.dumps(matrix))
    with pytest.raises(top5.Top5HistoryError, match='do not match'):
        top5.load_acceptance_matrix(path)


def test_complete_coverage_and_single_scope_pass():
    assert top5.coverage_violations(_coverage_rows(top5.acceptance_scopes())) == []
    scope = top5.scope_for('GB1', '2025')
    assert top5.coverage_violations(
        _coverage_rows((scope,)), scopes=(scope,),
    ) == []


@pytest.mark.parametrize(
    ('field', 'value', 'kind'),
    (
        ('native_rows', 0, 'missing_native'),
        ('full_lineage_rows', 0, 'incomplete_lineage'),
        ('null_scope_rows', 1, 'null_scope'),
    ),
)
def test_coverage_defects_are_explicit(field, value, kind):
    rows = _coverage_rows(top5.acceptance_scopes())
    rows[0][field] = value
    violations = top5.coverage_violations(rows)
    assert any(item['kind'] == kind for item in violations)


def test_missing_and_duplicate_coverage_rows_do_not_pass():
    rows = _coverage_rows(top5.acceptance_scopes())
    missing = top5.coverage_violations(rows[1:])
    assert any(item['kind'] == 'missing_result' for item in missing)
    duplicated = top5.coverage_violations([*rows, rows[0]])
    assert any(item['kind'] == 'duplicate_result' for item in duplicated)


def test_coverage_sql_is_pinned_select_for_all_five_entities():
    sql = top5.build_coverage_sql(_pins())
    assert sql.lstrip().upper().startswith('WITH')
    assert not re.search(r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER)\b', sql)
    for table, snapshot in _pins().items():
        assert f'{table} FOR VERSION AS OF {snapshot}' in sql
    assert 'GB1__2025' not in sql
    assert "'GB1'" in sql and "'2025'" in sql
    assert 'full_lineage_rows' in sql
    assert 'null_scope_rows' in sql


def test_coverage_sql_requires_complete_positive_integer_pins():
    pins = _pins()
    pins.pop(next(iter(pins)))
    with pytest.raises(top5.Top5HistoryError, match='snapshot'):
        top5.build_coverage_sql(pins)
    pins = _pins()
    pins[next(iter(pins))] = True
    with pytest.raises(top5.Top5HistoryError, match='positive integer'):
        top5.build_coverage_sql(pins)


def test_roster_reconciliation_sql_is_bidirectional_and_exact():
    pins = {
        PIN_TABLES['squad_memberships']: 2001,
        PIN_TABLES['player_attribute_observations']: 2002,
        PIN_TABLES['player_contract_observations']: 2003,
        'iceberg.bronze.transfermarkt_players': 2004,
    }
    sql = top5.build_roster_reconciliation_sql(top5.scope_for('GB1', '2025'), pins)
    assert sql.count('FULL OUTER JOIN') == 3
    assert "'ENG-Premier League'" in sql
    assert "'2526'" in sql
    assert "'GB1'" in sql and "'2025'" in sql
    for table, snapshot in pins.items():
        assert f'{table} FOR VERSION AS OF {snapshot}' in sql
    assert 'native_only_groups' in sql
    assert 'legacy_only_groups' in sql
    assert 'count_mismatch_groups' in sql


def test_reconciliation_gate_requires_three_exact_entities():
    assert top5.reconciliation_violations(_reconciliation_rows()) == []
    rows = _reconciliation_rows()
    rows[0]['native_only_groups'] = 1
    assert top5.reconciliation_violations(rows)[0]['kind'] == 'native_only'
    rows = _reconciliation_rows()
    rows[1]['count_mismatch_groups'] = 1
    assert top5.reconciliation_violations(rows)[0]['kind'] == 'count_mismatch'
    assert any(
        item['kind'] == 'missing_result'
        for item in top5.reconciliation_violations(_reconciliation_rows()[1:])
    )
