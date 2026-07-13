"""Safety and state-machine tests for Transfermarkt native-v2 promotion."""
from __future__ import annotations

import importlib.util
import io
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'scripts' / 'transfermarkt_native_v2.py'


def _load():
    name = 'transfermarkt_native_v2_test_module'
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_bootstrap_is_non_destructive_and_seeds_audited_legacy_state():
    mod = _load()
    sql = '\n'.join(mod.bootstrap_sql())
    assert 'DROP TABLE' not in sql.upper()
    assert 'WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE' in sql
    assert 'schema_version varchar' in sql
    assert 'first_attempt_at timestamp(6)' in sql
    assert 'transfermarkt_reader_state_v2' in sql
    assert 'transfermarkt_reader_state_history_v2' in sql
    assert 'transfermarkt_model_build_manifest_v2' in sql
    assert 'approved_league varchar' in sql
    assert 'approved_season integer' in sql
    assert 'approved_scope_set_id varchar' in sql
    assert 'previous_scope_set_id varchar' in sql
    assert 'pinned_input_snapshot_ids varchar' in sql
    assert 'ADD COLUMN IF NOT EXISTS league varchar' in sql
    assert 'ADD COLUMN IF NOT EXISTS season integer' in sql
    assert "'canonical', 'legacy'" in sql


def test_bootstrap_does_not_invent_unbounded_coach_stints():
    mod = _load()
    statements = mod.bootstrap_sql()
    stint = [
        s for s in statements
        if s.lstrip().startswith(
            'CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_coach_stints'
        )
    ]
    assert len(stint) == 1
    assert 'WHERE false' in stint[0]
    assert 'MERGE INTO iceberg.bronze.transfermarkt_coach_stints' not in stint[0]


def test_transfer_fallback_id_matches_runtime_identity_surface():
    mod = _load()
    sql = mod._event_id_sql('b')
    assert 'SHA256' in sql and 'player_id' in sql
    assert '"occurrence":0' in sql
    assert '"source_id":null' in sql
    assert 'fee_text' not in sql
    assert 'is_upcoming' not in sql
    assert 'ELSE NULL END' in mod._event_season_sql('b')


def test_parity_is_exactly_six_batch_scoped_bidirectional_pairs():
    mod = _load()
    pairs = mod.parity_pairs()
    assert {pair.name for pair in pairs} == set(mod.NATIVE_ENTITIES)
    for pair in pairs:
        left, right = pair.queries(
            legacy_batch_id='legacy-batch', native_batch_id='native-batch',
        )
        assert ' EXCEPT ' in left and ' EXCEPT ' in right
        assert "_batch_id = 'legacy-batch'" in left + right
        assert "_batch_id = 'native-batch'" in left + right


def test_coach_parity_ignores_out_of_scope_native_history():
    mod = _load()
    profiles = next(p for p in mod.parity_pairs() if p.name == 'coach_profiles')
    stints = next(p for p in mod.parity_pairs() if p.name == 'coach_stints')
    profile_sql = ' '.join(profiles.queries(
        legacy_batch_id='l', native_batch_id='n',
    ))
    stint_sql = ' '.join(stints.queries(
        legacy_batch_id='l', native_batch_id='n',
    ))
    assert 'FROM iceberg.bronze.transfermarkt_coaches' in profile_sql
    assert '(coach_id) IN (SELECT CAST(coach_id AS varchar)' in profile_sql
    assert '(club_id,coach_id) IN (SELECT CAST(current_club_id AS varchar)' in stint_sql


def _manifest_rows(mod, *, league='ENG-Premier League', season=2025):
    rows = []
    for pair in mod.parity_pairs():
        rows.append((
            league, season, pair.name, pair.legacy_table, pair.native_table,
            'batch-uuid', 'batch-uuid', 10, 10, 'same-hash', 'same-hash',
            'success', '2026-07-11', True,
        ))
    return rows


def test_manifest_requires_exact_entity_and_requested_scope():
    mod = _load()
    report, evidence = mod.control._validate_manifest_rows(
        _manifest_rows(mod), league='ENG-Premier League', season=2025,
    )
    assert report['passed'] is True
    assert set(evidence) == set(mod.NATIVE_ENTITIES)

    wrong = _manifest_rows(mod)
    wrong[0] = ('ESP-La Liga',) + wrong[0][1:]
    report, _ = mod.control._validate_manifest_rows(
        wrong, league='ENG-Premier League', season=2025,
    )
    assert report['passed'] is False


def test_vanished_live_batches_cannot_pass_empty_except(monkeypatch):
    mod = _load()
    control = mod.control
    pair = control.ParityPair(
        'sample', 'legacy_sample', 'native_sample',
        ('id', 'value'), ('id', 'value'),
    )
    monkeypatch.setattr(control, 'PARITY_PAIRS', (pair,))
    expected_hash = control._fingerprint_rows([('1', 'ok')])[1]
    evidence = {
        'sample': {
            'legacy_batch_id': 'batch', 'native_batch_id': 'batch',
            'legacy_rows': 1, 'native_rows': 1,
            'legacy_hash': expected_hash, 'native_hash': expected_hash,
        },
    }

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [(0,)] if 'SELECT COUNT(*) FROM (' in self.sql else []

    report = control.run_parity(
        Cursor(), 'cycle', league='ENG-Premier League', season=2025,
        manifest_evidence=evidence,
    )
    assert report['sample']['passed'] is False
    assert report['sample']['live_manifest_match'] is False


def test_business_value_corruption_fails_live_manifest_hash(monkeypatch):
    mod = _load()
    control = mod.control
    pair = control.ParityPair(
        'sample', 'legacy_sample', 'native_sample',
        ('id', 'value'), ('id', 'value'),
    )
    monkeypatch.setattr(control, 'PARITY_PAIRS', (pair,))
    expected_hash = control._fingerprint_rows([('1', 'ok')])[1]
    evidence = {
        'sample': {
            'legacy_batch_id': 'batch', 'native_batch_id': 'batch',
            'legacy_rows': 1, 'native_rows': 1,
            'legacy_hash': expected_hash, 'native_hash': expected_hash,
        },
    }

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            if 'SELECT COUNT(*) FROM (' in self.sql:
                return [(0,)]
            if 'legacy_sample' in self.sql:
                return [('1', 'ok')]
            if 'native_sample' in self.sql:
                return [('1', 'corrupt')]
            raise AssertionError(self.sql)

    report = control.run_parity(
        Cursor(), 'cycle', league='ENG-Premier League', season=2025,
        manifest_evidence=evidence,
    )
    assert report['sample']['passed'] is False
    assert report['sample']['live_native_rows'] == 1


def test_reader_selector_only_authorizes_one_v2_row():
    mod = _load()
    sql = mod.control.reader_selector_sql()
    assert 'COUNT(*) = 1' in sql
    assert "active_version = 'v2'" in sql
    assert "active_slot IN ('a', 'b')" in sql
    assert "NULLIF(TRIM(approved_cycle_id), '') IS NOT NULL" in sql
    assert "NULLIF(TRIM(approved_league), '') IS NOT NULL" in sql
    assert 'approved_season IS NOT NULL' in sql
    assert 'approved_model_revision >= 0' in sql
    assert "REGEXP_LIKE(approved_scope_set_id, '^[0-9a-f]{64}$')" in sql
    assert 'revision >= 0' in sql
    assert "ELSE 'legacy'" in sql


def test_incomplete_v2_selector_falls_back_before_cleanup_and_zeroes_after():
    mod = _load()
    relation = mod.control.CANONICAL_READER_RELATIONS[0]
    selector = mod.control.reader_selector_sql()
    normal = mod.control.canonical_reader_view_sql(relation)
    cleaned = mod.control.post_cleanup_reader_view_sql(relation)

    # Missing any approved-scope field makes the selector emit legacy/NULL.
    for predicate in (
        "NULLIF(TRIM(approved_cycle_id), '') IS NOT NULL",
        "NULLIF(TRIM(approved_league), '') IS NOT NULL",
        'approved_season IS NOT NULL',
        'approved_model_revision >= 0',
        "REGEXP_LIKE(approved_scope_set_id, '^[0-9a-f]{64}$')",
        'revision >= 0',
    ):
        assert selector.count(predicate) >= 2
    assert f'FROM {relation.legacy} legacy' in normal
    assert "WHERE state.active_version = 'legacy'" in normal
    assert relation.legacy not in cleaned
    assert "state.active_version = 'v2' AND state.active_slot = 'a'" in cleaned
    assert "state.active_version = 'v2' AND state.active_slot = 'b'" in cleaned


class _StateCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _sql):
        pass

    def fetchall(self):
        return self.rows


def _state_row(*, version='legacy', cycle=None, league=None, season=None, revision=0,
               retention=None, rollback=None):
    return (
        'canonical', version, cycle, league, season, revision,
        None, retention, rollback, None, 'tester',
    )


def test_reader_state_missing_is_legacy_but_duplicate_is_rejected():
    mod = _load()
    missing = mod.control.read_reader_state(_StateCursor([]), allow_missing=True)
    assert missing.exists is False and missing.active_version == 'legacy'
    with pytest.raises(mod.control.StateInvariantError, match='singleton'):
        mod.control.read_reader_state(
            _StateCursor([_state_row(), _state_row()]), allow_missing=False,
        )


def test_reader_state_reads_scope_set_bound_shape():
    mod = _load()
    scope_set = 'b' * 64
    row = (
        'canonical', 'v2', 'a', 'cycle', 'ENG-Premier League', 2025, 9,
        scope_set, None, None, None, None, None, None, 10,
        None, None, None, None, 'operator', None, None, None,
    )
    state = mod.control.read_reader_state(_StateCursor([row]), allow_missing=False)
    assert state.approved_scope_set_id == scope_set
    assert state.revision == 10


def test_cutover_and_rollback_dry_runs_are_explicit_cas(monkeypatch):
    mod = _load()
    control = mod.control
    legacy = control.ReaderState(exists=True, revision=7, updated_by='tester')
    monkeypatch.setattr(
        control, 'readiness',
        lambda *a, **k: {'ready': True, 'scope_set_id': 'a' * 64},
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: legacy)
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    cut = control.cutover(
        object(), cycle_id='cycle-1', league='ENG-Premier League', season=2025,
        expected_revision=7, actor='operator', apply=False,
    )
    assert cut['status'] == 'dry_run'
    assert "active_version = 'v2'" in cut['sql']
    assert "approved_league = 'ENG-Premier League'" in cut['sql']
    assert 'approved_season = 2025' in cut['sql']
    assert 'revision = 7' in cut['sql']
    assert "INTERVAL '30' DAY" in cut['sql']

    active = control.ReaderState(
        exists=True, active_version='v2', approved_cycle_id='cycle-1',
        approved_league='ENG-Premier League', approved_season=2025,
        approved_model_revision=7, active_slot='a', revision=8,
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: active)
    rollback = control.rollback(
        object(), cycle_id='cycle-1', league='ENG-Premier League', season=2025,
        expected_revision=8, actor='operator', apply=False,
    )
    assert "active_version = 'legacy'" in rollback['sql']
    assert 'rollback_verified_at = CURRENT_TIMESTAMP' not in rollback['sql']
    assert 'approved_cycle_id = CAST(NULL AS varchar)' not in rollback['sql']
    assert 'rollback_verified_at = CAST(NULL AS timestamp(6))' in rollback['sql']


def test_cas_without_exact_update_count_fails_closed(monkeypatch):
    mod = _load()
    control = mod.control
    before = control.ReaderState(exists=True, revision=1)
    after = control.ReaderState(
        exists=True, active_version='v2', approved_cycle_id='cycle',
        approved_league='ENG-Premier League', approved_season=2025,
        approved_model_revision=1, approved_scope_set_id='a' * 64,
        active_slot='a', revision=2,
    )
    states = iter((before, after))
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: next(states))
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    monkeypatch.setattr(
        control, 'readiness',
        lambda *a, **k: {'ready': True, 'scope_set_id': 'a' * 64},
    )
    monkeypatch.setattr(control, '_drain', lambda *a, **k: [])
    with pytest.raises(control.RevisionConflict, match='did not apply exactly once'):
        control._apply_transition(
            object(), action='cutover', cycle_id='cycle',
            league='ENG-Premier League', season=2025, expected_revision=1,
            actor='operator', reason='test',
            report={'ready': True, 'scope_set_id': 'a' * 64}, apply=True,
            quiesced=True,
        )


def test_production_cutover_requires_immutable_scope_set(monkeypatch):
    mod = _load()
    control = mod.control
    monkeypatch.setattr(
        control, 'read_reader_state',
        lambda *a, **k: control.ReaderState(exists=True, revision=3),
    )
    with pytest.raises(control.ReadinessError, match='scope_set_id'):
        control._apply_transition(
            object(), action='cutover', cycle_id='cycle',
            league='ENG-Premier League', season=2025, expected_revision=3,
            actor='operator', reason='test', report={'ready': True},
            apply=True, quiesced=True,
        )


def test_model_manifest_requires_one_build_and_current_snapshots(monkeypatch):
    mod = _load()
    control = mod.control
    rows = []
    pinned = {
        table: 11 for table in control.required_pinned_source_tables()
    }
    for contract in control.MODEL_CONTRACTS:
        sources = control.contract_source_tables(contract, 'a')
        inputs = {table: 11 for table in sources}
        rows.append((
            'ENG-Premier League', 2025, 'a', contract.name,
            json.dumps(sources),
            control.contract_output_table(contract, 'a'),
            json.dumps(inputs), 11, 10, 'key-hash', 'success', 4,
            'build-1', '2026-07-11', 'c' * 64,
            json.dumps(pinned, sort_keys=True), True, True,
        ))

    class Cursor:
        def execute(self, _sql):
            pass

        def fetchall(self):
            return rows

    monkeypatch.setattr(control, '_table_snapshot_id', lambda *a, **k: 11)
    report = control._model_manifest_report(
        Cursor(), 'cycle', league='ENG-Premier League', season=2025,
        expected_revision=4, expected_slot='a',
    )
    assert report['passed'] is True
    rows[0] = rows[0][:12] + ('other-build',) + rows[0][13:]
    report = control._model_manifest_report(
        Cursor(), 'cycle', league='ENG-Premier League', season=2025,
        expected_revision=4, expected_slot='a',
    )
    assert report['passed'] is False
    assert report['single_build_id'] is False


def _scope_manifest_fixture(
    control,
    *,
    parent_cycle_id='parent-1',
    scope_id='GB1:2025',
    child_cycle_id='child-1',
    competition_id='GB1',
    edition_id='2025',
    requests=7,
    retries=1,
    provider_bytes=120,
    reader_revision=4,
):
    from dags.utils import transfermarkt_dq_contracts as dq
    from dags.utils import transfermarkt_scope_state as scope_state

    entities = []
    for index, entity in enumerate(control.NATIVE_ENTITIES):
        entities.append(scope_state.EntityEvidence(
            entity=entity,
            applicability_status='ok',
            expected_rows=1,
            raw_rows=1,
            dedup_rows=1,
            key_hash='a' * 64,
            content_hash='a' * 64,
            dq_status='passed',
            decoded_bytes=100 if index == 0 else 0,
            wire_bytes=80 if index == 0 else 0,
            provider_metered_bytes=provider_bytes if index == 0 else 0,
            requests=requests if index == 0 else 0,
            retries=retries if index == 0 else 0,
            cache_hits=2 if index == 0 else 0,
            duration_ms=25 if index == 0 else 0,
        ))
    competition_type = (
        'continental_club' if competition_id == 'CL' else 'domestic_league'
    )
    strict = competition_type == 'continental_club'
    capture = {
        'schema_version': 1,
        'scope_id': scope_id,
        'competition_id': competition_id,
        'edition_id': edition_id,
        'competition_type': competition_type,
        'gender': 'men',
        'team_type': 'club',
        'age_category': 'senior',
        'listing_status': 'ok',
        'listing_source_url': 'https://example.test/listing',
        'listing_source_body_hash': 'listing-hash',
        'expected_team_ids': ['1', '2'],
        'observed_team_ids': ['1', '2'],
        'endpoint_status_by_team': {'1': 'ok', '2': 'ok'},
        'fetched_at': '2026-07-11T00:00:00+00:00',
    }
    dq_evidence = {
        'status': 'passed',
        'registry_participant_count': 2,
        'edition_current': True,
        'scope_capture': capture,
        'entity_statuses': {
            item.entity: item.applicability_status for item in entities
        },
        'entity_contracts': dq.entity_applicability_contracts(
            entities=(item.entity for item in entities),
            competition_type=competition_type,
            team_type='club',
        ),
        'authoritative_empty_evidence': {},
        'roster_coverage': {},
        'career_fetches_pending': 0,
        'participant_contract': {
            'passed': True,
            'competition_type': competition_type,
            'strict': strict,
            'participant_count': 2,
            'observed_participant_count': 2,
            'participant_coverage': 1.0,
            'endpoint_coverage': 1.0,
            'minimum_participant_coverage': 1.0 if strict else 0.9,
            'fresh': True,
        },
    }
    return scope_state.ScopeManifest(
        parent_cycle_id=parent_cycle_id,
        child_cycle_id=child_cycle_id,
        scope_id=scope_id,
        competition_id=competition_id,
        edition_id=edition_id,
        canonical_competition_id='ENG-Premier League',
        canonical_season='2526',
        registry_snapshot_id='registry-1',
        capture_revision='capture-1',
        parser_revision='parser-1',
        schema_revision='schema-1',
        reader_revision=reader_revision,
        entities=tuple(entities),
        dq_evidence=dq_evidence,
    )


class _ScopeEvidenceCursor:
    def __init__(
        self,
        control,
        manifests,
        *,
        traffic_extra=None,
        reader_revision=None,
    ):
        from dags.utils import transfermarkt_scope_state as scope_state

        self.control = control
        self.manifests = tuple(manifests)
        self.scope_set = scope_state.ScopeSetManifest.build(
            self.manifests,
            expected_entities=control.NATIVE_ENTITIES,
            reader_revision=reader_revision,
        )
        traffic = scope_state.aggregate_traffic(self.manifests)
        if traffic_extra:
            traffic.update(traffic_extra)
        self.set_rows = [(
            self.scope_set.registry_snapshot_id,
            self.scope_set.capture_revision,
            self.scope_set.parser_revision,
            self.scope_set.schema_revision,
            self.scope_set.reader_revision,
            json.dumps([list(item) for item in self.scope_set.scope_digests]),
            json.dumps(traffic, sort_keys=True),
            'success', '2026-07-11', True,
        )]
        self.scope_rows = [(
            item.parent_cycle_id,
            item.child_cycle_id,
            item.scope_id,
            item.competition_id,
            item.edition_id,
            item.canonical_competition_id,
            item.canonical_season,
            item.registry_snapshot_id,
            item.capture_revision,
            item.parser_revision,
            item.schema_revision,
            item.reader_revision,
            json.dumps({
                'entities': [entity.as_dict() for entity in item.entities],
                'dq_evidence': item.dq_evidence,
            }),
            item.digest,
            scope_state.SCOPE_COMPLETION_STATUS, '2026-07-11', True,
        ) for item in self.manifests]
        self.ledger_rows = []
        for parent in sorted({item.parent_cycle_id for item in self.manifests}):
            parent_manifests = tuple(
                item for item in self.manifests
                if item.parent_cycle_id == parent
            )
            for entity in control.NATIVE_ENTITIES:
                evidence = [
                    item
                    for manifest in parent_manifests
                    for item in manifest.entities
                    if item.entity == entity
                ]
                self.ledger_rows.append((
                    parent,
                    entity,
                    *(sum(getattr(item, field) for item in evidence) for field in (
                        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
                        'requests', 'retries', 'cache_hits', 'duration_ms',
                    )),
                    control.SCOPE_SET_HARD_BYTE_CAP,
                    control.SCOPE_SET_SOFT_BYTE_STOP,
                ))
        self.rows = []
        self.sql = []

    def execute(self, sql):
        from dags.utils import transfermarkt_scope_state as scope_state

        self.sql.append(sql)
        if f'FROM {scope_state.SCOPE_SET_MANIFEST_TABLE}' in sql:
            self.rows = self.set_rows
        elif f'FROM {scope_state.SCOPE_MANIFEST_TABLE}' in sql:
            self.rows = self.scope_rows
        elif f'FROM {scope_state.PROXY_LEDGER_TABLE}' in sql:
            self.rows = self.ledger_rows
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return self.rows


def test_scope_set_readiness_dispatch_is_explicit_and_lowercase(monkeypatch):
    mod = _load()
    control = mod.control
    calls = []
    monkeypatch.setattr(
        control,
        '_scope_set_readiness',
        lambda *args, **kwargs: calls.append((args, kwargs)) or {'ready': False},
    )
    report = control.readiness(
        object(), 'parent-1', expected_revision=4,
        scope_set_id='b' * 64, parent_cycle_id='parent-1',
    )
    assert report == {'ready': False}
    assert calls[0][1]['scope_set_id'] == 'b' * 64
    with pytest.raises(ValueError, match='scope_set_id is required'):
        control.readiness(
            object(), 'parent-1', expected_revision=4,
            parent_cycle_id='parent-1',
        )
    with pytest.raises(ValueError, match='lowercase sha256'):
        control._normalise_scope_set_id('B' * 64)


def test_scope_set_evidence_rebuilds_exact_ops_rows_and_ledger():
    mod = _load()
    control = mod.control
    manifest = _scope_manifest_fixture(control)
    cursor = _ScopeEvidenceCursor(control, [manifest])
    report, manifests = control._scope_set_evidence(
        cursor,
        scope_set_id=cursor.scope_set.scope_set_id,
        expected_revision=4,
        require_fresh=True,
    )
    assert report['passed'] is True
    assert report['proxy_ledger_exact'] is True
    assert report['parent_cycle_ids'] == ['parent-1']
    assert report['aggregate_traffic_is_metric_only'] is True
    assert manifests == (manifest,)
    assert any('WHERE manifest_digest IN' in sql for sql in cursor.sql)
    assert all('WHERE parent_cycle_id =' not in sql for sql in cursor.sql)


def test_scope_set_evidence_rejects_reused_child_and_budget_overrun():
    mod = _load()
    control = mod.control
    first = _scope_manifest_fixture(control)
    second = _scope_manifest_fixture(
        control,
        parent_cycle_id='parent-2',
        scope_id='CL:2025',
        child_cycle_id=first.child_cycle_id,
        competition_id='CL',
        edition_id='2025',
    )
    duplicate = _ScopeEvidenceCursor(control, [first, second])
    with pytest.raises(control.ReadinessError, match='reuses a child cycle'):
        control._scope_set_evidence(
            duplicate,
            scope_set_id=duplicate.scope_set.scope_set_id,
            expected_revision=4,
            require_fresh=True,
        )

    excessive = _scope_manifest_fixture(
        control, requests=control.SCOPE_SET_REQUEST_LIMIT + 1,
    )
    over_budget = _ScopeEvidenceCursor(control, [excessive])
    with pytest.raises(control.ReadinessError, match='request limit'):
        control._scope_set_evidence(
            over_budget,
            scope_set_id=over_budget.scope_set.scope_set_id,
            expected_revision=4,
            require_fresh=True,
        )


def test_scope_set_evidence_enforces_caps_per_parent_not_on_aggregate():
    mod = _load()
    control = mod.control
    first = _scope_manifest_fixture(
        control,
        parent_cycle_id='parent-1',
        child_cycle_id='child-1',
        requests=200,
        retries=1,
        provider_bytes=8_000_000,
        reader_revision=3,
    )
    second = _scope_manifest_fixture(
        control,
        parent_cycle_id='parent-2',
        scope_id='CL:2025',
        child_cycle_id='child-2',
        competition_id='CL',
        requests=200,
        retries=1,
        provider_bytes=8_000_000,
        reader_revision=4,
    )
    cursor = _ScopeEvidenceCursor(
        control, [first, second], reader_revision=4,
    )

    report, manifests = control._scope_set_evidence(
        cursor,
        scope_set_id=cursor.scope_set.scope_set_id,
        expected_revision=4,
        require_fresh=True,
    )

    assert manifests == (first, second)
    assert report['traffic']['provider_metered_bytes'] == 16_000_000
    assert report['traffic']['requests'] == 400
    assert report['parent_cycle_ids'] == ['parent-1', 'parent-2']
    assert all(
        item['proxy_ledger_exact']
        for item in report['parent_cycles'].values()
    )


def test_scope_set_evidence_rejects_one_parent_over_hard_byte_cap():
    mod = _load()
    control = mod.control
    excessive = _scope_manifest_fixture(
        control,
        provider_bytes=control.SCOPE_SET_HARD_BYTE_CAP + 1,
    )
    cursor = _ScopeEvidenceCursor(control, [excessive])

    with pytest.raises(control.ReadinessError, match='parent-1.*hard byte cap'):
        control._scope_set_evidence(
            cursor,
            scope_set_id=cursor.scope_set.scope_set_id,
            expected_revision=4,
            require_fresh=True,
        )


def test_readiness_exposes_separate_strict_competition_thresholds():
    mod = _load()
    control = mod.control
    league = _scope_manifest_fixture(control)
    continental = _scope_manifest_fixture(
        control,
        scope_id='CL:2025',
        child_cycle_id='child-2',
        competition_id='CL',
        edition_id='2025',
    )
    report = control._scope_competition_type_dq_report(
        (league, continental),
    )
    assert report['passed'] is True
    assert report['scopes']['GB1:2025']['minimum_participant_coverage'] == 0.9
    assert report['scopes']['CL:2025']['minimum_participant_coverage'] == 1.0
    assert report['scopes']['CL:2025']['minimum_required_entity_coverage'] == 1.0


class _CoverageCursor:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)

    def fetchall(self):
        return [next(self._rows)]


def test_scope_set_coverage_is_exact_for_strict_competitions():
    mod = _load()
    control = mod.control
    manifest = _scope_manifest_fixture(
        control,
        scope_id='CL:2025',
        competition_id='CL',
        edition_id='2025',
    )
    cursor = _CoverageCursor(((10, 9), (10, 10), (5, 4)))

    report = control._scope_set_coverage_report(
        cursor, manifests=(manifest,), candidate_slot='a',
    )

    scope = report['scopes']['CL:2025']
    assert report['passed'] is False
    assert scope['competition_type'] == 'continental_club'
    assert scope['strict'] is True
    assert scope['entities']['squad_memberships']['error_threshold'] == 1.0
    assert scope['entities']['squad_memberships']['passed'] is False
    assert scope['entities']['player_attribute_observations']['passed'] is True
    assert scope['entities']['transfer_events']['error_threshold'] == 1.0
    assert scope['entities']['transfer_events']['passed'] is False


def test_scope_set_coverage_keeps_domestic_league_thresholds():
    mod = _load()
    control = mod.control
    manifest = _scope_manifest_fixture(control)
    cursor = _CoverageCursor(((10, 6), (10, 6), (5, 3)))

    report = control._scope_set_coverage_report(
        cursor, manifests=(manifest,), candidate_slot='a',
    )

    scope = report['scopes']['GB1:2025']
    assert report['passed'] is True
    assert scope['strict'] is False
    for item in scope['entities'].values():
        assert item['warn_threshold'] == 0.80
        assert item['error_threshold'] == 0.60
        assert item['severity'] == 'WARNING'
        assert item['passed'] is True


def test_strict_authoritative_empty_transfer_requires_proof_and_zero_rows():
    mod = _load()
    control = mod.control
    manifest = _scope_manifest_fixture(
        control,
        scope_id='CL:2025',
        competition_id='CL',
        edition_id='2025',
    )
    entities = tuple(
        replace(
            item,
            applicability_status='authoritative_empty',
            expected_rows=0,
            raw_rows=0,
            dedup_rows=0,
        )
        if item.entity == 'transfer_events'
        else item
        for item in manifest.entities
    )
    evidence = dict(manifest.dq_evidence)
    evidence['entity_statuses'] = dict(evidence['entity_statuses'])
    evidence['entity_statuses']['transfer_events'] = 'authoritative_empty'
    evidence['authoritative_empty_evidence'] = {
        'transfer_events': {
            'kind': 'typed_fetch_state',
            'result_sha256': 'b' * 64,
        },
    }
    manifest = replace(manifest, entities=entities, dq_evidence=evidence)
    manifest.validate(control.NATIVE_ENTITIES)

    report = control._scope_set_coverage_report(
        _CoverageCursor(((10, 10), (10, 10), (0, 0))),
        manifests=(manifest,),
        candidate_slot='a',
    )

    transfer = report['scopes']['CL:2025']['entities']['transfer_events']
    assert report['passed'] is True
    assert transfer['terminal_status'] == 'authoritative_empty'
    assert transfer['authoritative_empty_proof_valid'] is True
    assert transfer['severity'] == 'PASS'
    assert transfer['error_threshold'] == 0.60

    leaked = control._scope_set_coverage_report(
        _CoverageCursor(((10, 10), (10, 10), (1, 0))),
        manifests=(manifest,),
        candidate_slot='a',
    )
    leaked_transfer = leaked['scopes']['CL:2025']['entities'][
        'transfer_events'
    ]
    assert leaked['passed'] is False
    assert leaked_transfer['passed'] is False
    assert leaked_transfer['severity'] == 'ERROR'


def test_scope_set_model_manifest_accepts_null_scope_and_requires_bound_lineage(
    monkeypatch,
):
    mod = _load()
    control = mod.control
    scope_set_id = 'c' * 64
    pins = {table: 11 for table in control.required_pinned_source_tables()}
    rows = []
    for contract in control.MODEL_CONTRACTS:
        sources = control.contract_source_tables(contract, 'a')
        rows.append((
            None, None, 'a', contract.name, json.dumps(sources),
            control.contract_output_table(contract, 'a'),
            json.dumps({table: 11 for table in sources}),
            11, 10, 'key-hash', 'success', 4, 'build-1', '2026-07-11',
            scope_set_id, json.dumps(pins, sort_keys=True), True, True,
        ))

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return rows

    cursor = Cursor()
    monkeypatch.setattr(control, '_table_snapshot_id', lambda *a, **k: 11)
    report = control._model_manifest_report(
        cursor, 'parent-1', expected_revision=4, expected_slot='a',
        scope_set_id=scope_set_id,
    )
    assert report['passed'] is True
    assert all(item['season'] is None for item in report['models'].values())
    assert 'transfermarkt_scope_set_manifest_v2' in cursor.sql
    assert 'WHERE sm.parent_cycle_id' not in cursor.sql

    rows[0] = rows[0][:4] + (json.dumps(['iceberg.bronze.wrong']),) + rows[0][5:]
    report = control._model_manifest_report(
        cursor, 'parent-1', expected_revision=4, expected_slot='a',
        scope_set_id=scope_set_id,
    )
    assert report['passed'] is False
    assert report['models'][control.MODEL_CONTRACTS[0].name]['passed'] is False


def test_scope_write_manifest_binds_physical_table_names():
    mod = _load()
    control = mod.control
    pair = control.PARITY_PAIRS[0]
    evidence = SimpleNamespace(
        entity=pair.name, dedup_rows=1, key_hash='a' * 64,
    )
    manifest = SimpleNamespace(
        child_cycle_id='child-1', reader_revision=4,
        entities=(evidence,),
    )

    class Cursor:
        def execute(self, _sql):
            pass

        def fetchall(self):
            return [(
                'child-1', pair.name, pair.legacy_table,
                'wrong_native_table', 'batch-1', 'batch-1', 1, 1,
                'a' * 64, 'a' * 64, 'success', '2026-07-11', True,
            )]

    with pytest.raises(control.ReadinessError, match='native write table drifted'):
        control._scope_write_manifest_report(
            Cursor(), manifests=(manifest,), expected_revision=4,
            write_mode='dual', require_fresh=True,
        )


def test_record_scope_set_model_manifest_writes_null_legacy_scope(monkeypatch):
    mod = _load()
    control = mod.control
    contract = control.ModelContract(
        'sample_model', 'iceberg.silver.sample_model', ('id',),
        ('iceberg.bronze.sample_source',),
    )
    monkeypatch.setattr(control, 'MODEL_CONTRACTS', (contract,))
    state = control.ReaderState(exists=True, active_slot='b', revision=4)
    monkeypatch.setattr(control, 'assert_reader_revision', lambda *_a, **_k: state)
    monkeypatch.setattr(control, '_table_snapshot_id', lambda *_a, **_k: 11)
    monkeypatch.setattr(
        control, '_key_fingerprint_table', lambda *_a, **_k: (1, 'key-hash'),
    )

    class Cursor:
        sql = []

        def execute(self, sql):
            self.sql.append(sql)

        def fetchall(self):
            return []

    cursor = Cursor()
    result = control.record_model_build_manifest(
        cursor,
        cycle_id='parent-1',
        build_id='build-1',
        expected_revision=4,
        candidate_slot='a',
        scope_set_id='d' * 64,
        pinned_input_snapshot_ids={'iceberg.bronze.sample_source': 11},
    )
    assert result['league'] is None and result['season'] is None
    assert len(cursor.sql) == 1
    assert "VALUES (\n    'parent-1', NULL,\n    NULL, 'build-1'" in cursor.sql[0]


def test_v2_reader_state_without_scope_set_fails_closed():
    mod = _load()
    control = mod.control
    old_v2_row = (
        'canonical', 'v2', 'a', 'cycle', 'ENG-Premier League', 2025, 4,
        None, None, None, None, None, 5,
        None, None, None, None, 'operator', None, None, None,
    )
    with pytest.raises(control.StateInvariantError, match='complete approved'):
        control.read_reader_state(_StateCursor([old_v2_row]), allow_missing=False)


def test_reader_registry_and_bootstrap_adapters_are_exhaustive():
    mod = _load()
    control = mod.control
    assert {item.canonical for item in control.CANONICAL_READER_RELATIONS} == {
        'iceberg.silver.transfermarkt_players',
        'iceberg.silver.transfermarkt_coaches',
        'iceberg.gold.dim_manager',
        'iceberg.gold.fct_transfer',
        'iceberg.gold.fct_player_market_value',
        'iceberg.gold.transfermarkt_team_season_market_value',
    }
    players = control.transfermarkt_players_adapter_sql()
    coaches = control.transfermarkt_coaches_adapter_sql()
    transfer = control.legacy_transfer_adapter_sql()
    team_value = control.legacy_team_season_market_value_adapter_sql()
    assert 'transfermarkt_player_attributes_v2' in players
    assert 'transfermarkt_squad_memberships_v2' in players
    assert 'transfermarkt_coach_stints_v2' in coaches
    assert 'transfermarkt_coach_profiles_v2' in coaches
    assert 'transfermarkt_competition_editions_v2_a' in coaches
    assert 'b.competition_id = m.competition_id' in coaches
    assert 'b.edition_id = m.edition_id' in coaches
    assert "season_format = 'single_year'" in coaches
    assert "CONCAT(canonical_season, '-01-01')" in coaches
    assert "CONCAT(canonical_season, '-12-31')" in coaches
    assert "SUBSTR(c.season" not in coaches
    team_value_contract = control.MODEL_BY_NAME[
        'transfermarkt_team_season_market_value_v2'
    ]
    assert (
        'iceberg.silver.transfermarkt_competition_editions_v2'
        in team_value_contract.source_tables
    )
    assert 'fct_transfer_legacy_source' in transfer
    assert 'transfer_id' in transfer and 'event_season' in transfer
    assert ' league' not in transfer.split('FROM ')[0].lower()
    assert 'transfermarkt_players_legacy' in team_value
    assert 'ambiguous_players_excluded' in team_value


def test_coach_adapter_uses_epl_and_world_cup_edition_windows():
    duckdb = pytest.importorskip('duckdb')
    sqlglot = pytest.importorskip('sqlglot')
    mod = _load()
    sql = sqlglot.transpile(
        mod.control.transfermarkt_coaches_adapter_sql(),
        read='trino',
        write='duckdb',
    )[0].replace('iceberg.silver.', 'silver.')

    con = duckdb.connect(':memory:')
    con.execute('CREATE SCHEMA silver')
    con.execute('''CREATE TABLE silver.transfermarkt_squad_memberships_v2_a (
        club_id varchar, competition_id varchar, edition_id varchar,
        league varchar, season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_competition_editions_v2_a (
        competition_id varchar, edition_id varchar, start_date date,
        end_date date, season_format varchar, canonical_season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_coach_stints_v2_a (
        coach_id varchar, name varchar, role varchar, club_id varchar,
        club_name varchar, appointed_date date, left_date date,
        _bronze_ingested_at timestamp
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_coach_profiles_v2_a (
        coach_id varchar, name varchar, dob date, nationality varchar,
        _bronze_ingested_at timestamp
    )''')
    con.execute('''CREATE TABLE silver.xref_manager (
        source_id varchar, canonical_id varchar, confidence varchar,
        source varchar
    )''')
    con.executemany(
        'INSERT INTO silver.transfermarkt_squad_memberships_v2_a '
        'VALUES (?,?,?,?,?)',
        [
            ('epl', 'GB1', '2025', 'ENG', '2526'),
            ('world', 'FIWC', '2026', 'WORLD', '2026'),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_competition_editions_v2_a '
        'VALUES (?,?,?,?,?,?)',
        [
            (
                'GB1', '2025', '2025-08-15', '2026-05-24',
                'split_year', '2526',
            ),
            (
                'FIWC', '2026', '2026-06-11', '2026-07-19',
                'single_year', '2026',
            ),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_coach_stints_v2_a '
        'VALUES (?,?,?,?,?,?,?,?)',
        [
            (
                'epl_coach', 'EPL Coach', 'Manager', 'epl', 'EPL Club',
                '2025-08-01', '2026-06-01', '2026-07-01',
            ),
            (
                'world_coach', 'World Coach', 'Manager', 'world',
                'World Team', '2026-06-01', '2026-07-20', '2026-07-20',
            ),
            (
                'stale_world_coach', 'Stale Coach', 'Manager', 'world',
                'World Team', '2021-01-01', '2022-01-01', '2022-01-01',
            ),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_coach_profiles_v2_a '
        'VALUES (?,?,?,?,?)',
        [
            ('epl_coach', 'EPL Coach', None, 'England', '2026-07-01'),
            ('world_coach', 'World Coach', None, 'Spain', '2026-07-20'),
            ('stale_world_coach', 'Stale Coach', None, 'Italy', '2022-01-01'),
        ],
    )

    con.execute(sql)
    coach_ids = {
        row[0]
        for row in con.execute(
            'SELECT coach_id FROM silver.transfermarkt_coaches_v2_a'
        ).fetchall()
    }
    assert coach_ids == {'epl_coach', 'world_coach'}


def test_reader_bootstrap_plans_adapters_before_canonical_views(monkeypatch):
    mod = _load()
    control = mod.control
    inventory = {name: 'BASE TABLE' for name in control.READER_PREFLIGHT_RELATIONS}
    inventory.update({
        contract.output_table: 'BASE TABLE'
        for contract in control.MODEL_CONTRACTS
    })
    for relation in control.CANONICAL_READER_RELATIONS:
        if relation.canonical != 'iceberg.gold.transfermarkt_team_season_market_value':
            inventory[relation.canonical] = 'BASE TABLE'
        inventory[relation.v2] = 'BASE TABLE'
    monkeypatch.setattr(control, '_relation_inventory', lambda _cur: dict(inventory))
    monkeypatch.setattr(control, '_probe_relation', lambda *_a, **_k: None)
    plan = control.reader_view_bootstrap_plan(object())
    adapters_at = next(
        index for index, sql in enumerate(plan)
        if 'transfermarkt_players_v2_a AS' in sql
    )
    canonical_at = next(
        index for index, sql in enumerate(plan)
        if sql.startswith(
            'CREATE OR REPLACE VIEW iceberg.silver.transfermarkt_players AS'
        )
    )
    assert adapters_at < canonical_at
    assert any(
        sql.startswith('ALTER TABLE iceberg.silver.transfermarkt_players ')
        for sql in plan
    )


def test_reader_bootstrap_surfaces_partial_reverse_compensation(monkeypatch):
    mod = _load()
    control = mod.control
    monkeypatch.setattr(control, '_relation_inventory', lambda _cur: {
        'iceberg.silver.transfermarkt_players': 'BASE TABLE',
    })
    monkeypatch.setattr(control, 'reader_view_bootstrap_plan', lambda _cur: [
        'ALTER TABLE iceberg.silver.transfermarkt_players '
        'RENAME TO transfermarkt_players_legacy',
        'BROKEN DDL',
    ])

    def drain(_cur, sql):
        if sql.startswith('ALTER TABLE iceberg.silver.transfermarkt_players '):
            return []
        if sql == 'BROKEN DDL' or 'transfermarkt_players_legacy' in sql:
            raise RuntimeError('boom')
        return []

    monkeypatch.setattr(control, '_drain', drain)
    with pytest.raises(control.StateInvariantError, match='partial'):
        control.apply_reader_view_bootstrap(object())


def test_rollback_verification_is_separate_and_records_downstream_run(monkeypatch):
    mod = _load()
    control = mod.control
    state = control.ReaderState(
        exists=True, revision=2, active_slot='a', approved_cycle_id='cycle',
        approved_league='ENG-Premier League', approved_season=2025,
        approved_model_revision=0,
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(control, '_scalar', lambda *a, **k: 1)
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    monkeypatch.setattr(control, '_rollback_dq_report', lambda *a, **k: {'passed': True})
    report = control.verify_rollback(
        object(), cycle_id='cycle', league='ENG-Premier League', season=2025,
        expected_revision=2, actor='operator',
        downstream_dq_run_id='gold-dq-run-42', apply=False,
    )
    assert report['action'] == 'rollback_verify'
    assert report['downstream_dq_run_id'] == 'gold-dq-run-42'
    assert 'rollback_verified_at = CURRENT_TIMESTAMP' in report['sql']


def test_cleanup_boundary_requires_rollback_and_uses_nonfresh_live_dq(monkeypatch):
    mod = _load()
    control = mod.control
    retention = datetime(2026, 8, 10, tzinfo=timezone.utc)
    state = control.ReaderState(
        exists=True, active_version='v2', approved_cycle_id='cycle',
        approved_league='ENG-Premier League', approved_season=2025,
        approved_model_revision=2, active_slot='a', revision=3,
        retention_until=retention,
        rollback_verified_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
        legacy_writers_disabled_at=datetime(2026, 8, 10, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(control, '_scalar', lambda *a, **k: 1)
    seen = {}

    def ready(*_a, **kwargs):
        seen.update(kwargs)
        return {'ready': True}

    monkeypatch.setattr(control, 'readiness', ready)
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    monkeypatch.setattr(control, '_rollback_dq_report', lambda *a, **k: {'passed': True})
    before = control.cleanup_check(
        object(), cycle_id='cycle', league='ENG-Premier League', season=2025,
        now=retention - timedelta(microseconds=1),
    )
    at_boundary = control.cleanup_check(
        object(), cycle_id='cycle', league='ENG-Premier League', season=2025,
        now=retention,
    )
    assert before['eligible'] is False
    assert at_boundary['eligible'] is False
    assert at_boundary['guards']['retention_expired'] is True
    assert at_boundary['guards']['post_cleanup_slot_rollback_verified'] is False
    assert seen['require_fresh'] is False
    assert seen['require_current_snapshots'] is False


def test_cleanup_sql_replaces_dynamic_views_before_drops():
    mod = _load()
    sql = mod.cleanup_sql()
    first_drop = sql.index('DROP ')
    assert 'CREATE OR REPLACE VIEW' in sql[:first_drop]
    assert 'iceberg.gold.fct_transfer_v2_a' in sql[:first_drop]
    assert 'iceberg.gold.fct_transfer_v2_b' in sql[:first_drop]
    assert 'iceberg.ops.transfermarkt_reader_state_v2' in sql[:first_drop]
    assert 'DROP TABLE IF EXISTS iceberg.gold.fct_transfer_legacy_source' in sql


def test_bootstrap_defaults_to_dry_run_without_connecting(monkeypatch):
    mod = _load()
    monkeypatch.setattr(
        mod, '_connect', lambda: (_ for _ in ()).throw(AssertionError('must not connect')),
    )
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rc = mod.main(['bootstrap'])
    assert rc == 0
    assert 'CREATE TABLE IF NOT EXISTS' in out.getvalue()
    assert 'DRY RUN' in err.getvalue()


def test_bootstrap_apply_drains_every_trino_result(monkeypatch):
    mod = _load()

    class Cursor:
        def __init__(self):
            self.executed = self.drained = 0

        def execute(self, _sql):
            assert self.executed == self.drained
            self.executed += 1

        def fetchall(self):
            self.drained += 1
            return []

        def close(self):
            pass

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def close(self):
            pass

    conn = Conn()
    monkeypatch.setattr(mod, '_connect', lambda: conn)
    with redirect_stdout(io.StringIO()):
        assert mod.main(['bootstrap', '--apply']) == 0
    assert conn.cur.executed == conn.cur.drained == len(mod.bootstrap_sql())


def test_legacy_backup_status_is_read_only_and_exact():
    mod = _load()

    class Cursor:
        def __init__(self):
            self.sql = ''
            self.executed = []

        def execute(self, sql):
            self.sql = sql
            self.executed.append(sql)

        def fetchall(self):
            if '$snapshots' in self.sql:
                return [(1000 + len(self.executed),)]
            return [(200 + len(self.executed),)]

    cursor = Cursor()
    report = mod.legacy_backup_status(cursor)
    assert set(report) == set(mod.LEGACY_BACKUP_RELATIONS)
    assert all(value['row_count'] > 0 for value in report.values())
    assert all(value['snapshot_id'] > 0 for value in report.values())
    assert len(cursor.executed) == 2 * len(mod.LEGACY_BACKUP_RELATIONS)
    assert all(sql.lstrip().upper().startswith('SELECT ') for sql in cursor.executed)


def test_registry_backup_status_is_read_only_and_exact():
    mod = _load()

    class Cursor:
        def __init__(self):
            self.sql = ''
            self.executed = []

        def execute(self, sql):
            self.sql = sql
            self.executed.append(sql)

        def fetchall(self):
            if '$snapshots' in self.sql:
                return [(9000 + len(self.executed),)]
            return [(300 + len(self.executed),)]

    cursor = Cursor()
    report = mod.registry_backup_status(cursor)
    assert set(report) == set(mod.REGISTRY_DISCOVERY_RELATIONS)
    assert all(value['row_count'] > 0 for value in report.values())
    assert all(value['snapshot_id'] > 0 for value in report.values())
    assert len(cursor.executed) == 2 * len(mod.REGISTRY_DISCOVERY_RELATIONS)
    assert all(sql.lstrip().upper().startswith('SELECT ') for sql in cursor.executed)


def test_registry_backup_status_command_includes_reader(monkeypatch):
    mod = _load()

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [(101,)] if '$snapshots' in self.sql else [(2,)]

        def close(self):
            pass

    class Conn:
        cur = Cursor()

        def cursor(self):
            return self.cur

        def close(self):
            pass

    monkeypatch.setattr(mod, '_connect', lambda: Conn())
    monkeypatch.setattr(
        mod.control, 'status', lambda _cur: {'active_version': 'legacy'},
    )
    out = io.StringIO()
    with redirect_stdout(out):
        assert mod.main(['registry-backup-status']) == 0
    payload = json.loads(out.getvalue())
    assert payload['reader'] == {'active_version': 'legacy'}
    assert set(payload['registry_tables']) == set(
        mod.REGISTRY_DISCOVERY_RELATIONS
    )


def test_registry_discovery_rollback_defaults_to_exact_offline_dry_run(
    monkeypatch,
):
    mod = _load()
    cycle_id = 'tm-registry-' + 'a' * 24
    monkeypatch.setattr(
        mod, '_connect',
        lambda: (_ for _ in ()).throw(AssertionError('must not connect')),
    )
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        assert mod.main([
            'rollback-registry-discovery', '--cycle-id', cycle_id,
        ]) == 0
    assert out.getvalue().splitlines() == [
        f"DELETE FROM {relation} WHERE cycle_id = '{cycle_id}';"
        for relation in mod.REGISTRY_DISCOVERY_RELATIONS
    ]
    assert err.getvalue() == ''
    assert 'manifest' not in out.getvalue().lower()
    assert 'cache' not in out.getvalue().lower()


def test_registry_discovery_rollback_apply_drains_and_proves_zero(monkeypatch):
    mod = _load()
    cycle_id = 'tm-registry-' + 'b' * 24

    class Cursor:
        def __init__(self):
            self.executed = []
            self.pending = False
            self.sql = ''

        def execute(self, sql):
            assert self.pending is False
            self.sql = sql
            self.executed.append(sql)
            self.pending = True

        def fetchall(self):
            assert self.pending is True
            self.pending = False
            return [(0,)] if self.sql.startswith('SELECT COUNT(*)') else []

        def close(self):
            pass

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def close(self):
            pass

    conn = Conn()
    monkeypatch.setattr(mod, '_connect', lambda: conn)
    out = io.StringIO()
    with redirect_stdout(out):
        assert mod.main([
            'rollback-registry-discovery', '--cycle-id', cycle_id, '--apply',
        ]) == 0
    assert conn.cur.pending is False
    assert conn.cur.executed[:2] == list(
        mod.registry_discovery_rollback_sql(cycle_id)
    )
    assert all(
        sql.startswith('SELECT COUNT(*)') for sql in conn.cur.executed[2:]
    )
    payload = json.loads(out.getvalue())
    assert payload['remaining_rows'] == {
        relation: 0 for relation in mod.REGISTRY_DISCOVERY_RELATIONS
    }
    assert payload['retained_evidence'] == ['manifests', 'cache']


def test_registry_discovery_rollback_rejects_unsafe_cycle_and_residual_rows():
    mod = _load()
    with pytest.raises(ValueError, match='tm-registry'):
        mod.registry_discovery_rollback_sql("tm-registry-a' OR true")

    class Cursor:
        sql = ''

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [(1,)] if self.sql.startswith('SELECT COUNT(*)') else []

    with pytest.raises(RuntimeError, match='left rows behind'):
        mod.rollback_registry_discovery(
            Cursor(), 'tm-registry-' + 'c' * 24,
        )


def test_readiness_refuses_to_infer_any_scope_component():
    mod = _load()
    with pytest.raises(ValueError, match='cycle_id is required'):
        mod.readiness(
            None, '', 'ENG-Premier League', 2025, 0,
        )
    with pytest.raises(ValueError, match='league is required'):
        mod.readiness(None, 'cycle', '', 2025, 0)


def test_native_sql_rewrite_pins_every_model_reference_to_one_slot():
    mod = _load()
    control = mod.control
    sql = ' UNION ALL '.join(
        f'SELECT * FROM {contract.output_table}'
        for contract in control.MODEL_CONTRACTS
    )
    rewritten = control.rewrite_native_relations(sql, 'b')
    for contract in control.MODEL_CONTRACTS:
        assert f'{contract.output_table}_b' in rewritten
        assert f'{contract.output_table} ' not in rewritten


def test_canonical_views_route_legacy_or_exactly_one_slot():
    mod = _load()
    control = mod.control
    for relation in control.CANONICAL_READER_RELATIONS:
        sql = control.canonical_reader_view_sql(relation)
        assert relation.legacy in sql
        assert relation.v2_a in sql and relation.v2_b in sql
        assert "state.active_slot = 'a'" in sql
        assert "state.active_slot = 'b'" in sql


def test_advance_cycle_cas_preserves_original_retention(monkeypatch):
    mod = _load()
    control = mod.control
    retention = datetime(2026, 8, 10, tzinfo=timezone.utc)
    state = control.ReaderState(
        exists=True, active_version='v2', active_slot='a',
        approved_cycle_id='old', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=4, revision=5,
        activated_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        retention_until=retention,
    )
    report = {'ready': True, 'candidate_slot': 'b', 'expected_state_revision': 5}
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    readiness_calls = []

    def ready(*args, **kwargs):
        readiness_calls.append(kwargs)
        return dict(report)

    monkeypatch.setattr(control, 'readiness', ready)
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    planned = control.advance_cycle(
        object(), cycle_id='new', league='ENG-Premier League', season=2025,
        expected_revision=5, actor='operator', apply=False,
    )
    assert planned['to_slot'] == 'b'
    assert "active_slot = 'b'" in planned['sql']
    assert 'retention_until = COALESCE' in planned['sql']
    assert "CURRENT_TIMESTAMP + INTERVAL '30' DAY" in planned['sql']


def test_restore_verified_retained_slot_requires_no_rebuild(monkeypatch):
    mod = _load()
    control = mod.control
    state = control.ReaderState(
        exists=True, active_version='legacy', active_slot='a',
        approved_cycle_id='cycle', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=4, revision=7,
        rollback_verified_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
        retention_until=datetime(2026, 8, 10, tzinfo=timezone.utc),
    )
    report = {'ready': True, 'candidate_slot': 'a', 'expected_state_revision': 4}
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    readiness_calls = []

    def ready(*args, **kwargs):
        readiness_calls.append(kwargs)
        return dict(report)

    monkeypatch.setattr(control, 'readiness', ready)
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    planned = control.restore_v2(
        object(), cycle_id='cycle', league='ENG-Premier League', season=2025,
        expected_revision=7, actor='operator', apply=False,
    )
    assert planned['to_slot'] == 'a'
    assert "active_version = 'v2'" in planned['sql']
    assert 'retention_until' not in planned['sql']
    assert len(readiness_calls) == 2
    assert all(call['require_fresh'] is False for call in readiness_calls)
    assert all(
        call['require_current_snapshots'] is False
        for call in readiness_calls
    )


def test_cleanup_stays_blocked_without_verified_previous_slot_rollback(monkeypatch):
    mod = _load()
    control = mod.control
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    state = control.ReaderState(
        exists=True, active_version='v2', active_slot='a',
        approved_cycle_id='cycle', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=1, revision=4,
        retention_until=now - timedelta(days=1), rollback_verified_at=now,
        legacy_writers_disabled_at=now,
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(control, '_scalar', lambda *a, **k: 1)
    monkeypatch.setattr(control, 'readiness', lambda *a, **k: {'ready': True})
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    monkeypatch.setattr(control, '_rollback_dq_report', lambda *a, **k: {'passed': True})
    result = control.cleanup_check(
        object(), cycle_id='cycle', league='ENG-Premier League', season=2025,
        now=now,
    )
    assert result['eligible'] is False
    assert result['guards']['post_cleanup_slot_rollback_verified'] is False


def test_no_proxy_previous_slot_rollback_swaps_durable_evidence(monkeypatch):
    mod = _load()
    control = mod.control
    state = control.ReaderState(
        exists=True, active_version='v2', active_slot='b',
        approved_cycle_id='new', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=8,
        previous_slot='a', previous_cycle_id='old',
        previous_league='ENG-Premier League', previous_season=2025,
        previous_model_revision=5, revision=9,
        activated_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        retention_until=datetime(2026, 8, 10, tzinfo=timezone.utc),
    )
    report = {'ready': True, 'candidate_slot': 'a', 'expected_state_revision': 5}
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(control, 'readiness', lambda *a, **k: dict(report))
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    planned = control.rollback_slot(
        object(), cycle_id='old', league='ENG-Premier League', season=2025,
        expected_revision=9, actor='operator', apply=False,
    )
    assert planned['to_slot'] == 'a'
    assert "active_slot = 'a'" in planned['sql']
    assert "previous_slot = 'b'" in planned['sql']
    assert 'slot_rollback_verified_at = CURRENT_TIMESTAMP' in planned['sql']
    assert 'retention_until' not in planned['sql']


def test_cleanup_eligible_only_with_two_native_slots_and_slot_drill(monkeypatch):
    mod = _load()
    control = mod.control
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    state = control.ReaderState(
        exists=True, active_version='v2', active_slot='b',
        approved_cycle_id='native-b', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=8,
        previous_slot='a', previous_cycle_id='native-a',
        previous_league='ENG-Premier League', previous_season=2025,
        previous_model_revision=6, revision=10,
        retention_until=now - timedelta(days=1), rollback_verified_at=now,
        legacy_writers_disabled_at=now, slot_rollback_verified_at=now,
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(control, '_scalar', lambda *a, **k: 1)
    monkeypatch.setattr(
        control, 'readiness',
        lambda *a, **k: {'ready': True, 'write_mode': 'native-only'},
    )
    monkeypatch.setattr(control, 'verify_reader_views', lambda *a, **k: {'passed': True})
    monkeypatch.setattr(control, '_rollback_dq_report', lambda *a, **k: {'passed': True})
    result = control.cleanup_check(
        object(), cycle_id='native-b', league='ENG-Premier League', season=2025,
        now=now,
    )
    assert result['eligible'] is True
    assert result['guards']['active_slot_native_only_evidence'] is True
    assert result['guards']['previous_slot_native_only_evidence'] is True


def test_complete_cleanup_is_separate_verified_cas(monkeypatch):
    mod = _load()
    control = mod.control
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    state = control.ReaderState(
        exists=True, active_version='v2', active_slot='b',
        approved_cycle_id='native-b', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=8,
        previous_slot='a', previous_cycle_id='native-a',
        previous_league='ENG-Premier League', previous_season=2025,
        previous_model_revision=6, revision=10,
        legacy_writers_disabled_at=now, slot_rollback_verified_at=now,
    )
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: state)
    monkeypatch.setattr(
        control, 'readiness',
        lambda *a, **k: {'ready': True, 'write_mode': 'native-only'},
    )
    monkeypatch.setattr(
        control, 'post_cleanup_verify', lambda *a, **k: {'passed': True},
    )
    planned = control.complete_cleanup(
        object(), expected_revision=10, actor='operator', apply=False,
    )
    assert planned['status'] == 'dry_run'
    assert 'cleanup_completed_at = CURRENT_TIMESTAMP' in planned['sql']
    assert "active_slot = 'b'" in planned['sql']
    assert "previous_slot = 'a'" in planned['sql']


def test_complete_cleanup_compensates_failed_post_cas_verifier(monkeypatch):
    mod = _load()
    control = mod.control
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    base = dict(
        exists=True, active_version='v2', active_slot='b',
        approved_cycle_id='native-b', approved_league='ENG-Premier League',
        approved_season=2025, approved_model_revision=8,
        previous_slot='a', previous_cycle_id='native-a',
        previous_league='ENG-Premier League', previous_season=2025,
        previous_model_revision=6, legacy_writers_disabled_at=now,
        slot_rollback_verified_at=now,
    )
    before = control.ReaderState(revision=10, **base)
    changed = control.ReaderState(
        revision=11, cleanup_completed_at=now, **base,
    )
    restored = control.ReaderState(revision=12, **base)
    states = iter((before, changed, restored))
    monkeypatch.setattr(control, 'read_reader_state', lambda *a, **k: next(states))
    monkeypatch.setattr(
        control, 'readiness',
        lambda *a, **k: {'ready': True, 'write_mode': 'native-only'},
    )
    verifier_calls = iter(({'passed': True}, RuntimeError('post failed')))

    def verify(*_a, **_k):
        value = next(verifier_calls)
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(control, 'post_cleanup_verify', verify)
    seen = []

    def drain(_cur, sql):
        seen.append(sql)
        if sql.startswith(f'UPDATE {control.STATE_TABLE}'):
            return [(1,)]
        return []

    monkeypatch.setattr(control, '_drain', drain)
    with pytest.raises(control.RevisionConflict, match='post failed'):
        control.complete_cleanup(
            object(), expected_revision=10, actor='operator', apply=True,
        )
    assert any('cleanup_completed_at = CAST(NULL' in sql for sql in seen)


def test_candidate_transfer_gate_rejects_semantic_duplicates():
    source = (ROOT / 'dags' / 'utils' / 'transfermarkt_native_v2.py').read_text()
    assert 'semantic_duplicate_violations' in source
    assert 'player_id, transfer_date, event_season, from_team_id, to_team_id' in source


def test_promoted_native_manifest_does_not_require_expired_bronze_batch():
    mod = _load()
    control = mod.control
    rows = [
        (
            'ENG-Premier League', 2025, pair.name, pair.native_table,
            'expired-batch', 10, 'persisted-hash', 4, 'native-only',
            'success', '2026-07-11', True,
        )
        for pair in control.PARITY_PAIRS
    ]

    class NoLiveBronzeCursor:
        def execute(self, _sql):
            raise AssertionError('historical promoted evidence must not read Bronze')

    report, evidence = control._validate_native_manifest(
        NoLiveBronzeCursor(), rows, league='ENG-Premier League', season=2025,
        expected_revision=4, require_fresh=False, require_live_batch=False,
    )
    assert report['passed'] is True
    assert report['live_batch_required'] is False
    assert set(evidence) == set(control.NATIVE_ENTITIES)
