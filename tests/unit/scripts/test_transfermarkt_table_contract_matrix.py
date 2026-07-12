from __future__ import annotations

import ast
from pathlib import Path

from dags.utils import transfermarkt_native_v2 as tm_v2


ROOT = Path(__file__).resolve().parents[3]
TRANSFORM_DAG = ROOT / 'dags/dag_transform_transfermarkt_silver.py'
DISCOVERY_DAG = ROOT / 'dags/dag_discover_transfermarkt_registry.py'
INGEST_DAG = ROOT / 'dags/dag_ingest_transfermarkt.py'
SCOPE_RUNNER = ROOT / 'dags/scripts/run_transfermarkt_scope_cycle.py'
CHECKLIST = ROOT / 'docs/research/transfermarkt-native-v2-regression-checklist.md'


def _literal_assignment(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(item, ast.Name) and item.id == name for item in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f'{name} is not a literal assignment in {path}')


def test_native_matrix_has_exact_declared_layer_counts():
    report = tm_v2.validate_table_contracts()
    assert report == {
        'passed': True,
        'counts': {'bronze': 9, 'silver': 12, 'gold': 4},
        'relations': 25,
    }


def test_every_declared_relation_has_executable_contract():
    for contract in tm_v2.TABLE_CONTRACTS:
        assert contract.grain
        assert contract.key_columns
        assert contract.dedup_order
        assert contract.lineage_columns
        assert {'completeness', 'freshness'} & set(contract.dq_checks)
        assert contract.airflow_task_id
        assert contract.consumers
        assert contract.empty_policy == 'authoritative_empty_or_not_applicable'


def test_contracts_cover_required_native_entities():
    names = {contract.name for contract in tm_v2.TABLE_CONTRACTS}
    assert {
        'competitions',
        'competition_editions',
        'squad_memberships',
        'player_attribute_observations',
        'player_contract_observations',
        'market_value_points',
        'transfer_events',
        'coach_profiles',
        'coach_stints',
    } <= names


def test_contract_paths_use_one_catalog_convention():
    for contract in tm_v2.TABLE_CONTRACTS:
        assert contract.openmetadata_path.startswith(
            'configs/openmetadata/descriptions/'
        )
        assert contract.openmetadata_path.endswith('.yaml')


def test_every_contract_has_real_catalog_file_and_closed_lineage():
    relations = {contract.output_table for contract in tm_v2.TABLE_CONTRACTS}
    allowed_external_inputs = {'iceberg.silver.xref_player'}
    for contract in tm_v2.TABLE_CONTRACTS:
        catalog = ROOT / contract.openmetadata_path
        assert catalog.is_file(), contract.output_table
        catalog_text = catalog.read_text(encoding='utf-8')
        assert contract.output_table in catalog_text
        assert 'description:' in catalog_text
        assert 'columns:' in catalog_text
        for source in contract.source_tables:
            assert source in relations | allowed_external_inputs, (
                contract.output_table,
                source,
            )


def test_silver_and_gold_contracts_match_real_sql_and_airflow_tasks():
    silver_specs = _literal_assignment(TRANSFORM_DAG, 'NATIVE_V2_TRANSFORMS')
    gold_specs = _literal_assignment(TRANSFORM_DAG, 'NATIVE_V2_GOLD_TRANSFORMS')
    contracts = {contract.output_table: contract for contract in tm_v2.TABLE_CONTRACTS}

    for task_id, sql_path, table_name, _partitions in silver_specs:
        relation = f'iceberg.silver.{table_name}'
        assert (ROOT / sql_path).is_file(), relation
        assert contracts[relation].airflow_task_id == (
            f'native_v2_transforms.{task_id}'
        )

    for task_id, sql_path, table_name, _partitions in gold_specs:
        relation = f'iceberg.gold.{table_name}'
        assert (ROOT / sql_path).is_file(), relation
        assert contracts[relation].airflow_task_id == f'native_v2_gold.{task_id}'

    assert len(silver_specs) == 12
    assert len(gold_specs) == 4


def test_bronze_contracts_point_to_real_bounded_airflow_tasks():
    discovery_text = DISCOVERY_DAG.read_text(encoding='utf-8')
    ingest_text = INGEST_DAG.read_text(encoding='utf-8')
    scope_text = SCOPE_RUNNER.read_text(encoding='utf-8')
    bronze = {
        contract.name: contract
        for contract in tm_v2.TABLE_CONTRACTS
        if contract.layer == 'bronze'
    }
    assert set(bronze) == set(tm_v2.ALL_NATIVE_ENTITIES)
    for name in tm_v2.REGISTRY_ENTITIES:
        assert bronze[name].airflow_task_id == 'discover_registry'
        assert 'DISCOVERY_TASK_ID = "discover_registry"' in discovery_text
    for name in tm_v2.NATIVE_ENTITIES:
        assert bronze[name].airflow_task_id == 'run_exact_child_cycle'
        assert name in scope_text
    assert "task_id='run_exact_child_cycle'" in ingest_text


def test_unsupported_entities_are_not_advertised_as_tables():
    unsupported = {'fixtures', 'stages', 'results', 'awards', 'achievements'}
    advertised = {contract.name for contract in tm_v2.TABLE_CONTRACTS}
    assert unsupported.isdisjoint(advertised)
    assert all(
        not any(f'transfermarkt_{name}' in contract.output_table for name in unsupported)
        for contract in tm_v2.TABLE_CONTRACTS
    )
    checklist = ' '.join(CHECKLIST.read_text(encoding='utf-8').lower().split())
    assert 'awards and achievements remain roadmap-only' in checklist
    assert 'fixtures, stages, results' in checklist
