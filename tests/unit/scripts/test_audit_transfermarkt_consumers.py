"""Static guard for Transfermarkt legacy physical consumers."""

from __future__ import annotations

import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / 'scripts' / 'audit_transfermarkt_consumers.py'
SPEC = importlib.util.spec_from_file_location('audit_transfermarkt_consumers', SCRIPT)
assert SPEC and SPEC.loader
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


def test_runtime_consumer_inventory_is_fully_classified():
    report = audit.audit_consumers(PROJECT_ROOT)

    assert report['passed'], report['violations']
    assert report['violations'] == []
    assert set(report['legacy_consumers']) <= (
        audit.LEGACY_WRITER_ALLOWLIST | audit.LEGACY_ROLLBACK_ALLOWLIST
    )


def test_allowlist_has_only_writer_or_rollback_categories():
    assert audit.LEGACY_WRITER_ALLOWLIST
    assert audit.LEGACY_ROLLBACK_ALLOWLIST
    assert audit.LEGACY_WRITER_ALLOWLIST.isdisjoint(
        audit.LEGACY_ROLLBACK_ALLOWLIST
    )
    assert audit.STABLE_RUNTIME_CONSUMERS.isdisjoint(
        audit.LEGACY_WRITER_ALLOWLIST | audit.LEGACY_ROLLBACK_ALLOWLIST
    )
    assert audit.SHADOW_V2_CONSUMERS.isdisjoint(audit.STABLE_RUNTIME_CONSUMERS)
    assert audit.PHYSICAL_V2_CONTROL_ALLOWLIST.isdisjoint(
        audit.STABLE_RUNTIME_CONSUMERS
    )


def test_canonical_v2_inventory_has_no_legacy_relation():
    findings = audit.scan_legacy_consumers(PROJECT_ROOT)

    leaking = {
        path: findings[path]
        for path in audit.CANONICAL_V2_CONSUMERS
        if path in findings
    }
    assert leaking == {}


def test_stable_runtime_inventory_has_no_physical_v2_relation():
    report = audit.audit_consumers(PROJECT_ROOT)
    assert not [
        item for item in report['violations']
        if item['reason'] == 'stable_runtime_consumer_reads_physical_v2'
    ]


def test_every_physical_v2_reader_is_shadow_or_control():
    report = audit.audit_consumers(PROJECT_ROOT)
    allowed = audit.SHADOW_V2_CONSUMERS | audit.PHYSICAL_V2_CONTROL_ALLOWLIST

    assert set(report['physical_v2_consumers']) <= allowed


def test_unclassified_physical_v2_reader_fails_closed(tmp_path):
    reader = tmp_path / 'dags' / 'utils' / 'new_v2_reader.py'
    reader.parent.mkdir(parents=True)
    reader.write_text(
        'SQL = "SELECT * FROM iceberg.gold.fct_transfer_v2"\n',
        encoding='utf-8',
    )

    report = audit.audit_consumers(tmp_path)

    assert report['passed'] is False
    assert report['violations'] == [{
        'path': 'dags/utils/new_v2_reader.py',
        'relations': ['gold.fct_transfer_v2'],
        'reason': 'unclassified_physical_v2_consumer',
    }]


def test_unclassified_slotted_physical_v2_reader_fails_closed(tmp_path):
    reader = tmp_path / 'dags' / 'utils' / 'new_slotted_v2_reader.py'
    reader.parent.mkdir(parents=True)
    reader.write_text(
        'SQL = "SELECT * FROM iceberg.gold.fct_transfer_v2_a '
        'UNION ALL SELECT * FROM silver.dim_manager_v2_b"\n',
        encoding='utf-8',
    )

    report = audit.audit_consumers(tmp_path)

    assert report['passed'] is False
    assert report['violations'] == [{
        'path': 'dags/utils/new_slotted_v2_reader.py',
        'relations': [
            'gold.fct_transfer_v2_a',
            'silver.dim_manager_v2_b',
        ],
        'reason': 'unclassified_physical_v2_consumer',
    }]


def test_unversioned_canonical_inventory_includes_serving_dashboard():
    findings = audit.scan_canonical_consumers(PROJECT_ROOT)
    relations = findings['configs/superset/dashboards/league_overview.py']

    assert 'silver.transfermarkt_players' in relations
    assert 'gold.fct_transfer' in relations
    assert 'gold.fct_player_market_value' in relations


def test_new_unclassified_reader_fails_closed(tmp_path):
    reader = tmp_path / 'dags' / 'utils' / 'new_reader.py'
    reader.parent.mkdir(parents=True)
    reader.write_text(
        'SQL = "SELECT * FROM iceberg.silver.transfermarkt_players_legacy"\n',
        encoding='utf-8',
    )

    report = audit.audit_consumers(tmp_path)

    assert report['passed'] is False
    assert report['violations'] == [{
        'path': 'dags/utils/new_reader.py',
        'relations': ['silver.transfermarkt_players_legacy'],
        'reason': 'unclassified_legacy_consumer',
    }]


def test_new_unclassified_legacy_source_reader_fails_closed(tmp_path):
    reader = tmp_path / 'dags' / 'utils' / 'new_source_reader.py'
    reader.parent.mkdir(parents=True)
    reader.write_text(
        'SQL = "SELECT * FROM iceberg.gold.fct_transfer_legacy_source"\n',
        encoding='utf-8',
    )

    report = audit.audit_consumers(tmp_path)

    assert report['passed'] is False
    assert report['violations'] == [{
        'path': 'dags/utils/new_source_reader.py',
        'relations': ['gold.fct_transfer_legacy_source'],
        'reason': 'unclassified_legacy_consumer',
    }]


def test_comments_and_docstrings_are_not_consumers(tmp_path):
    path = tmp_path / 'dags' / 'only_docs.py'
    path.parent.mkdir(parents=True)
    path.write_text(
        '"""Mentions iceberg.silver.transfermarkt_players only in docs."""\n'
        '# iceberg.bronze.transfermarkt_coaches\n'
        'VALUE = 1\n',
        encoding='utf-8',
    )

    assert audit.scan_legacy_consumers(tmp_path) == {}
