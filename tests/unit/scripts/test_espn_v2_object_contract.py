from __future__ import annotations

import pytest

from scripts import espn_v2_object_contract as contract


def _complete_inventory(layout_mode: str):
    return tuple(
        contract.RelationInventory(
            schema=relation.schema,
            name=relation.name,
            kind=relation.kind,
            columns=relation.required_columns,
        )
        for relation in contract.required_layout_relations(layout_mode)
    )


def test_native_object_contract_covers_generations_current_views_and_manifests():
    assert set(contract.GENERATION_TABLES) == {
        "espn_schedule_generation_v2",
        "espn_lineup_generation_v2",
        "espn_matchsheet_generation_v2",
    }
    assert set(contract.CURRENT_VIEWS) == {
        "espn_schedule_current",
        "espn_lineup_current",
        "espn_matchsheet_current",
    }
    assert {
        "espn_ingest_manifest_v2",
        "espn_request_ledger_generation_v2",
        "espn_scope_cutover_v2",
        "espn_catalog_snapshot_v2",
        "espn_legacy_baseline_v2",
    } <= set(contract.REQUIRED_COLUMNS)


def test_native_entities_require_source_ids_and_immutable_lineage():
    for table in (*contract.GENERATION_TABLES, *contract.CURRENT_VIEWS):
        required = contract.REQUIRED_COLUMNS[table]
        assert {
            "scope_id",
            "competition_id",
            "source_season_year",
            "event_id",
        } <= required
        assert {
            "generation_id",
            "generation_signature",
            "registry_signature",
            "plan_signature",
            "raw_uri",
            "raw_sha256",
            "_batch_id",
        } <= required
    for table in (
        "espn_lineup_generation_v2",
        "espn_lineup_current",
    ):
        assert {"team_id", "athlete_id"} <= contract.REQUIRED_COLUMNS[table]
    for table in (
        "espn_matchsheet_generation_v2",
        "espn_matchsheet_current",
    ):
        assert "team_id" in contract.REQUIRED_COLUMNS[table]


def test_lineup_and_matchsheet_empty_state_is_capability_gated():
    assert contract.CAPABILITY_GATED_TABLES == frozenset(
        {
            "espn_lineup_generation_v2",
            "espn_matchsheet_generation_v2",
            "espn_lineup_current",
            "espn_matchsheet_current",
        }
    )


def test_layout_contract_exposes_exact_legacy14_and_compact6_public_topologies():
    legacy_public = contract.PUBLIC_RELATIONS_BY_LAYOUT["legacy14"]
    compact_public = contract.PUBLIC_RELATIONS_BY_LAYOUT["compact6"]

    assert len(legacy_public) == 14
    assert {
        (relation.schema, relation.name, relation.kind) for relation in legacy_public
    } == {
        *(('bronze', table, 'BASE TABLE') for table in contract.LEGACY_TABLES),
        *(('bronze', table, 'BASE TABLE') for table in contract.GENERATION_TABLES),
        *(('bronze', table, 'VIEW') for table in contract.CURRENT_VIEWS),
        *(('bronze', table, 'BASE TABLE') for table in contract.CONTROL_TABLES),
    }
    assert {
        (relation.schema, relation.name, relation.kind) for relation in compact_public
    } == {
        ('bronze', 'espn_schedule', 'VIEW'),
        ('bronze', 'espn_lineup', 'VIEW'),
        ('bronze', 'espn_matchsheet', 'VIEW'),
        ('bronze', 'espn_ingest_manifest_v2', 'BASE TABLE'),
        ('bronze', 'espn_request_ledger_generation_v2', 'BASE TABLE'),
        ('bronze', 'espn_catalog_snapshot_v2', 'BASE TABLE'),
    }


def test_compact6_internal_contract_covers_all_operational_relations():
    relations = contract.COMPACT6_INTERNAL_RELATIONS
    actual = {(relation.schema, relation.name, relation.kind) for relation in relations}

    assert all(relation.schema == 'espn_internal' for relation in relations)
    assert {
        ('espn_internal', table, 'BASE TABLE')
        for table in (
            *contract.GENERATION_TABLES,
            'espn_scope_cutover_v2',
            'espn_legacy_baseline_v2',
            'espn_legacy_archive_manifest_v1',
            'espn_legacy_disposition_v1',
            'espn_layout_state_v2',
            'espn_compact6_journal_v2',
        )
    } <= actual
    for entity in ('schedule', 'lineup', 'matchsheet'):
        assert {
            ('espn_internal', f'espn_{entity}_legacy_archive_v1', 'BASE TABLE'),
            ('espn_internal', f'espn_{entity}_legacy_main_retained_v1', 'BASE TABLE'),
            ('espn_internal', f'espn_{entity}_compact6_shadow_v1', 'BASE TABLE'),
            ('espn_internal', f'espn_{entity}_current', 'VIEW'),
            ('espn_internal', f'espn_{entity}_emergency_legacy_v1', 'VIEW'),
        } <= actual

    required = {relation.name: relation.required_columns for relation in relations}
    assert {
        'manifest_version',
        'legacy_snapshot_ids_json',
        'whole_rowset_metrics_json',
        'legacy_disposition_snapshot_id',
        'legacy_disposition_metrics_json',
        'legacy_dispositions_json',
        'native_replacements_json',
        'manifest_sha256',
    } <= required['espn_legacy_archive_manifest_v1']
    assert {
        'archive_id', 'league', 'season', 'disposition', 'replacement_scope_id'
    } <= required['espn_legacy_disposition_v1']
    assert {
        'layout_version',
        'layout_mode',
        'archive_id',
        'transition_id',
        'effective_at',
        'plan_sha256',
        'archive_manifest_sha256',
        'state_sha256',
    } == required['espn_layout_state_v2']
    assert {
        'transition_id', 'command', 'step_index', 'status', 'checkpoint_sha256'
    } <= required['espn_compact6_journal_v2']
    assert contract.LEGACY_DISPOSITION_VALUES == frozenset(
        {'compatibility_only', 'native_current_replaced', 'quarantined'}
    )


def test_dependency_free_contract_relation_names_match_runtime_layout_constants():
    from scrapers.espn import layout

    def triples(schema, objects):
        return {(schema, name, kind) for name, kind in objects.items()}

    assert {
        (relation.schema, relation.name, relation.kind)
        for relation in contract.LEGACY14_PUBLIC_RELATIONS
    } == triples(layout.BRONZE_SCHEMA, layout.LEGACY14_PUBLIC_OBJECTS)
    assert {
        (relation.schema, relation.name, relation.kind)
        for relation in contract.COMPACT6_PUBLIC_RELATIONS
    } == triples(layout.BRONZE_SCHEMA, layout.COMPACT6_PUBLIC_OBJECTS)
    assert {
        (relation.schema, relation.name, relation.kind)
        for relation in contract.COMPACT6_INTERNAL_RELATIONS
    } == triples(layout.INTERNAL_SCHEMA, layout.COMPACT6_INTERNAL_REQUIRED_OBJECTS)
    required_columns = {
        relation.name: relation.required_columns
        for relation in contract.COMPACT6_INTERNAL_RELATIONS
    }
    assert required_columns['espn_legacy_archive_manifest_v1'] == frozenset(
        layout.ARCHIVE_MANIFEST_COLUMNS
    )
    assert required_columns['espn_legacy_disposition_v1'] == frozenset(
        layout.LEGACY_DISPOSITION_COLUMNS
    )
    assert required_columns['espn_layout_state_v2'] == frozenset(
        layout.LAYOUT_STATE_COLUMNS
    )


def test_layout_inventory_audit_rejects_extra_missing_and_wrong_kind_relations():
    compact = _complete_inventory('compact6')
    assert contract.audit_layout_inventory('compact6', compact) is None

    with pytest.raises(contract.ObjectInventoryError, match='missing'):
        contract.audit_layout_inventory('compact6', compact[1:])

    extra = (*compact, contract.RelationInventory(
        schema='bronze',
        name='espn_unreviewed_relation',
        kind='BASE TABLE',
        columns=frozenset({'unreviewed_id'}),
    ))
    with pytest.raises(contract.ObjectInventoryError, match='unexpected'):
        contract.audit_layout_inventory('compact6', extra)

    wrong_kind = list(compact)
    index = next(
        index
        for index, relation in enumerate(wrong_kind)
        if relation.schema == 'bronze' and relation.name == 'espn_schedule'
    )
    wrong_kind[index] = contract.RelationInventory(
        schema='bronze',
        name='espn_schedule',
        kind='BASE TABLE',
        columns=wrong_kind[index].columns,
    )
    with pytest.raises(contract.ObjectInventoryError, match='kind'):
        contract.audit_layout_inventory('compact6', wrong_kind)

    internal_kind = list(compact)
    index = next(
        index
        for index, relation in enumerate(internal_kind)
        if relation.schema == 'espn_internal' and relation.name == 'espn_layout_state_v2'
    )
    internal_kind[index] = contract.RelationInventory(
        schema='espn_internal',
        name='espn_layout_state_v2',
        kind='VIEW',
        columns=internal_kind[index].columns,
    )
    with pytest.raises(contract.ObjectInventoryError, match='kind'):
        contract.audit_layout_inventory('compact6', internal_kind)


def test_layout_inventory_audit_rejects_missing_required_internal_columns_and_modes():
    compact = list(_complete_inventory('compact6'))
    index = next(
        index
        for index, relation in enumerate(compact)
        if relation.name == 'espn_legacy_archive_manifest_v1'
    )
    compact[index] = contract.RelationInventory(
        schema='espn_internal',
        name='espn_legacy_archive_manifest_v1',
        kind='BASE TABLE',
        columns=compact[index].columns - {'manifest_sha256'},
    )
    with pytest.raises(contract.ObjectInventoryError, match='columns'):
        contract.audit_layout_inventory('compact6', compact)
    with pytest.raises(contract.ObjectInventoryError, match='layout mode'):
        contract.audit_layout_inventory('typo', ())
