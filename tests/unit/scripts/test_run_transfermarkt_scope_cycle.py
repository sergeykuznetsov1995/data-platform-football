from __future__ import annotations

import hashlib
import itertools
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from dags.scripts import run_transfermarkt_scope_cycle as cycle
from dags.utils.transfermarkt_approval import (
    ApprovalJournal,
    ApprovalPacket,
    load_standing_policy,
)
from scrapers.transfermarkt.registry import (
    EditionRecord,
    SeasonFormat,
    deterministic_scope_id,
    resolve_competition,
)


def _payload(tmp_path: Path) -> dict:
    snapshot = 'registry-snapshot-20260711'
    competition = resolve_competition('GB1')
    competition_row = competition.as_dict()
    competition_row['registry_snapshot_id'] = snapshot
    edition = EditionRecord(
        competition_id='GB1',
        edition_id='2025',
        edition_label='2025',
        canonical_season='2526',
        season_format=SeasonFormat.SPLIT_YEAR,
        start_date=None,
        end_date=None,
        active=True,
        current=True,
        participant_count=20,
        participant_hash='participants-2025',
        source_url=(
            'https://www.transfermarkt.com/premier-league/startseite/'
            'wettbewerb/GB1/saison_id/2025'
        ),
        discovered_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        registry_snapshot_id=snapshot,
        source_body_hash='edition-body-hash',
    )
    base = tmp_path / 'cycles' / ('a' * 64) / 'scopes' / ('b' * 64)
    return {
        'parent_cycle_id': 'scheduled__2026-07-11',
        'child_cycle_id': 'tm-child-0123456789abcdef01234567',
        'competition_id': 'GB1',
        'edition_id': '2025',
        'canonical_competition_id': 'ENG-Premier League',
        'canonical_season': '2526',
        'registry_snapshot_id': snapshot,
        'scope_id': deterministic_scope_id('GB1', '2025'),
        'competition_record': competition_row,
        'edition_record': edition.as_dict(),
        'result_paths': {
            'base_dir': str(base),
            'entity_staging_dir': str(base / 'entities'),
            'scope_manifest': str(base / 'scope-manifest.json'),
        },
        'parent_ledger': {
            'parent_cycle_id': 'scheduled__2026-07-11',
            'path': str(tmp_path / 'cycles' / ('a' * 64) / 'proxy-ledger.json'),
        },
    }


def _continental_payload(tmp_path: Path) -> dict:
    payload = _payload(tmp_path)
    snapshot = payload['registry_snapshot_id']
    competition = resolve_competition('CL')
    competition_row = competition.as_dict()
    competition_row['registry_snapshot_id'] = snapshot
    edition = EditionRecord(
        competition_id='CL',
        edition_id='2025',
        edition_label='2025/26',
        canonical_season='2526',
        season_format=SeasonFormat.SPLIT_YEAR,
        start_date=None,
        end_date=None,
        active=True,
        current=True,
        participant_count=36,
        participant_hash='cl-participants-2025',
        source_url=(
            'https://www.transfermarkt.com/uefa-champions-league/startseite/'
            'pokalwettbewerb/CL/saison_id/2025'
        ),
        discovered_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        registry_snapshot_id=snapshot,
        source_body_hash='cl-edition-body-hash',
    )
    payload.update({
        'competition_id': 'CL',
        'canonical_competition_id': (
            competition.canonical_competition_id or 'TM-CL'
        ),
        'scope_id': deterministic_scope_id('CL', '2025'),
        'competition_record': competition_row,
        'edition_record': edition.as_dict(),
    })
    return payload


def _approved_args(tmp_path: Path, payload: dict) -> tuple[list[str], Path]:
    journal_path = tmp_path / 'approvals.json'
    base = [
        '--payload-json', json.dumps(payload, sort_keys=True),
        '--reader-revision', '7',
        '--candidate-slot', 'b',
        '--write-mode', 'dual',
        '--approval-journal', str(journal_path),
    ]
    operation = cycle.approved_operation_argv(base)
    tables = tuple(sorted(cycle.required_write_tables('dual')))
    values = {
        'argv': operation,
        'concurrency': 1,
        'expected_duration_seconds': 1800,
        'affected_tables': tables,
        'affected_files': (payload['result_paths']['base_dir'],),
        'stop_conditions': (
            'hard byte cap',
            'request or retry cap',
            'red entity DQ',
        ),
        'backup_commands': (
            ('sha256sum', payload['result_paths']['base_dir']),
        ),
        'rollback_commands': (
            ('true',),
        ),
    }
    paid = ApprovalPacket(
        packet_id='tm-paid-scope-001',
        action='paid_proxy',
        byte_cap_bytes=cycle.HARD_BYTE_CAP,
        byte_cap_mib=Decimal('24'),
        request_limit=sum(
            item['requests'] for item in cycle.DEFAULT_ENTITY_LIMITS.values()
        ),
        retry_limit=cycle.SCOPE_RETRY_LIMIT,
        **values,
    )
    write = ApprovalPacket(
        packet_id='tm-write-scope-001',
        action='production_write',
        byte_cap_bytes=0,
        byte_cap_mib=Decimal('0'),
        request_limit=0,
        retry_limit=0,
        **values,
    )
    journal = ApprovalJournal(journal_path)
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    for packet in (paid, write):
        journal.issue(packet, expires_at=expires)
        journal.approve(packet, presented_hash=packet.packet_hash)
    full = base + [
        '--paid-proxy-approval-packet-id', paid.packet_id,
        '--paid-proxy-approval-packet-hash', paid.packet_hash,
        '--production-write-approval-packet-id', write.packet_id,
        '--production-write-approval-packet-hash', write.packet_hash,
    ]
    assert cycle.approved_operation_argv(full) == operation
    return full, journal_path


def _parse_args(argv: list[str]):
    args = cycle._parser().parse_args(argv)
    cycle._validate_args(args)
    return args


def _fake_result(command: tuple[str, ...], *, retries: int = 0) -> dict:
    def option(name: str) -> str:
        return command[command.index(name) + 1]

    parser_entity = option('--entity')
    competition_id = option('--competition-id')
    edition_id = option('--edition-id')
    outputs = {}
    rows = []
    for output_key, entity in cycle.ENTITY_OUTPUTS[parser_entity]:
        count = 2
        digest = hashlib.sha256(entity.encode()).hexdigest()
        outputs[output_key] = {
            'rows': count,
            'table': cycle.ENTITY_TABLES[entity],
            'applicability_status': 'ok',
        }
        rows.append({
            'entity': entity,
            'native_rows': count,
            'native_hash': digest,
            'status': 'success',
        })
    result = {
        'entity': parser_entity,
        'run_key': option('--run-key'),
        'cycle_ledger_key': option('--cycle-ledger-key'),
        'competition_id': competition_id,
        'edition_id': edition_id,
        'canonical_season': '2526',
        'registry_snapshot_id': 'registry-snapshot-20260711',
        'scope_id': deterministic_scope_id(competition_id, edition_id),
        'errors': [],
        'fallback': False,
        'native_write_complete': True,
        'dual_write_complete': True,
        'batch_manifest': {'status': 'success', 'rows': rows},
        'outputs': outputs,
        'decoded_response_body_bytes': 100,
        'wire_response_bytes': 120,
        'provider_metered_bytes': 125,
        'provider_metering_available': True,
        'provider_byte_grant': cycle.HARD_BYTE_CAP,
        'network_fetches': 2,
        'retries': retries,
        'cache_hits': 1,
        'traffic': {
            'telemetry_available': True,
            'hard_provider_byte_budget': cycle.HARD_BYTE_CAP,
            'soft_provider_byte_stop': cycle.SOFT_BYTE_STOP,
        },
    }
    if parser_entity == 'players':
        competition = resolve_competition(competition_id)
        participant_count = {'GB1': 20, 'CL': 36}.get(competition_id, 2)
        participant_ids = [str(value) for value in range(1, participant_count + 1)]
        result['scope_capture'] = {
            'schema_version': 1,
            'scope_id': deterministic_scope_id(competition_id, edition_id),
            'competition_id': competition_id,
            'edition_id': edition_id,
            'competition_type': competition.competition_type.value,
            'gender': 'men',
            'team_type': competition.team_type.value,
            'age_category': 'senior',
            'listing_status': 'ok',
            'listing_source_url': 'https://example.test/GB1/2025',
            'listing_source_body_hash': 'listing-hash',
            'expected_team_ids': participant_ids,
            'observed_team_ids': participant_ids,
            'endpoint_status_by_team': {
                team_id: 'ok' for team_id in participant_ids
            },
            'fetched_at': datetime.now(timezone.utc).isoformat(),
        }
    return result


def _fake_subprocess(calls: list, *, first_retries: int = 0, mutate=None):
    def run(command, **kwargs):
        command = tuple(command)
        calls.append((command, kwargs))
        result = _fake_result(
            command,
            retries=(first_retries if len(calls) == 1 else 0),
        )
        if mutate is not None:
            result = mutate(command, result)
        Path(command[command.index('--output') + 1]).write_text(
            json.dumps(result), encoding='utf-8',
        )
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    return run


def _authoritative_empty_result(
    command, result, *, parser_entity: str, output_key: str, proven: bool,
):
    if result['entity'] != parser_entity:
        return result
    entity = dict(cycle.ENTITY_OUTPUTS[parser_entity])[output_key]
    result['outputs'][output_key] = {
        'rows': 0,
        'table': cycle.ENTITY_TABLES[entity],
        'applicability_status': 'authoritative_empty',
    }
    for row in result['batch_manifest']['rows']:
        if row['entity'] == entity:
            row['native_rows'] = 0
            row['native_hash'] = cycle.stable_hash([])
    if proven:
        result['authoritative_empty'] = True
        result['valid_empty'] = True
    return result


def test_exact_cycle_runs_sequentially_without_shell_and_commits_manifest(tmp_path):
    payload = _payload(tmp_path)
    argv, journal_path = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    persisted = []
    persisted_ledgers = []
    ticks = itertools.count(start=0, step=1_000_000)

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess(calls),
        manifest_writer=persisted.append,
        parent_ledger_writer=persisted_ledgers.append,
        monotonic_ns=ticks.__next__,
    )

    assert [
        command[command.index('--entity') + 1] for command, _ in calls
    ] == list(cycle.ENTITY_ORDER)
    assert all(options['shell'] is False for _, options in calls)
    assert all(options['check'] is False for _, options in calls)
    assert all(
        command[command.index('--run-key') + 1] == payload['child_cycle_id']
        for command, _ in calls
    )
    assert all(
        command[command.index('--cycle-ledger-key') + 1]
        == payload['child_cycle_id']
        for command, _ in calls
    )
    assert all(
        options['env']['TM_REQUIRE_METERED_PROXY'] == 'true'
        and options['env']['TM_SCOPE_ID'] == payload['scope_id']
        for _, options in calls
    )
    budget = str(cycle.SCOPE_RETRY_LIMIT)
    assert all(
        command[command.index('--retry-budget') + 1] == budget
        and options['env']['TM_RETRY_BUDGET'] == budget
        for command, options in calls
    )
    assert manifest['status'] == 'complete'
    assert manifest['dq']['silver_trigger_allowed'] is True
    assert manifest['dq']['participant_contract']['endpoint_coverage'] == 1.0
    assert manifest['dq_evidence']['status'] == 'passed'
    assert manifest['dq_evidence']['registry_participant_count'] == 20
    player_result = json.loads(
        (
            Path(payload['result_paths']['entity_staging_dir']) / 'players.json'
        ).read_text()
    )
    assert manifest['dq_evidence']['scope_capture'] == player_result['scope_capture']
    assert set(manifest['dq_evidence']['entity_statuses']) == set(
        cycle.EXPECTED_ENTITIES
    )
    assert manifest['manifest_digest'] == cycle.stable_hash({
        key: manifest[key] for key in cycle.ScopeManifest.__dataclass_fields__
    })
    assert {item['entity'] for item in manifest['entities']} == set(
        cycle.EXPECTED_ENTITIES
    )
    assert manifest['traffic']['totals']['provider_metered_bytes'] == 500
    assert persisted == [manifest]
    assert len(persisted_ledgers) == 1
    assert set(persisted_ledgers[0]['by_entity']) == set(cycle.EXPECTED_ENTITIES)
    assert Path(payload['result_paths']['scope_manifest']).is_file()
    assert all(
        (Path(payload['result_paths']['entity_staging_dir']) / f'{name}.json').is_file()
        for name in cycle.ENTITY_ORDER
    )

    sql = cycle.scope_manifest_merge_sql(manifest)
    assert 'dq_evidence' in sql
    assert 'registry_participant_count' in sql

    ledger = json.loads(Path(payload['parent_ledger']['path']).read_text())
    assert ledger['provider_metered_bytes'] == 500
    assert ledger['requests'] == 8
    assert ledger['retries'] == 0
    assert ledger['manifest_count'] == 1
    assert ledger['hard_provider_byte_budget'] == cycle.PARENT_BYTE_BUDGET
    assert ledger['request_limit'] == cycle.PARENT_REQUEST_LIMIT
    assert ledger['retry_limit'] == cycle.PARENT_RETRY_LIMIT
    for field in (
        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
        'requests', 'retries', 'cache_hits', 'duration_ms',
    ):
        assert sum(
            item[field] for item in ledger['by_entity'].values()
        ) == ledger[field]
    journal = ApprovalJournal(journal_path)
    assert all(
        journal.get(record['packet_hash']).status == 'consumed'
        for record in (
            json.loads(Path(journal_path).read_text()).values()
        )
    )


def test_nonempty_scope_cannot_mark_memberships_authoritative_empty(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)

    def mutate(command, result):
        return _authoritative_empty_result(
            command, result,
            parser_entity='players', output_key='memberships', proven=True,
        )

    with pytest.raises(cycle.ScopeCycleError, match='applicability contract'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=_fake_subprocess([], mutate=mutate),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_optional_authoritative_empty_is_hash_bound_when_proven(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)

    def mutate(command, result):
        return _authoritative_empty_result(
            command, result,
            parser_entity='market_value_history',
            output_key='market_value_points',
            proven=True,
        )

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([], mutate=mutate),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
    )
    proof = manifest['dq_evidence']['authoritative_empty_evidence'][
        'market_value_points'
    ]
    assert proof['kind'] == 'typed_fetch_state'
    assert len(proof['result_sha256']) == 64
    contract = manifest['dq_evidence']['entity_contracts'][
        'market_value_points'
    ]
    assert contract['allowed_statuses'] == ['ok', 'authoritative_empty']


def test_optional_authoritative_empty_without_typed_proof_is_blocked(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)

    def mutate(command, result):
        return _authoritative_empty_result(
            command, result,
            parser_entity='transfers', output_key='transfer_events',
            proven=False,
        )

    with pytest.raises(cycle.ScopeCycleError, match='lacks typed proof'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=_fake_subprocess([], mutate=mutate),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_complete_hash_verified_resume_uses_no_approval_or_subprocess(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    ticks = itertools.count(start=0, step=1_000_000)
    first = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess(calls),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )
    calls.clear()
    persisted = []

    second = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=lambda *args, **kwargs: pytest.fail('subprocess ran'),
        manifest_writer=persisted.append,
        parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
    )

    assert second == first
    assert calls == []
    assert persisted == []


def test_checkpoint_hash_tamper_fails_without_rerun(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)
    cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )
    player_result = (
        Path(payload['result_paths']['entity_staging_dir']) / 'players.json'
    )
    player_result.write_text('{}', encoding='utf-8')

    with pytest.raises(cycle.ScopeCycleError, match='checkpoint hash mismatch'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *args, **kwargs: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_retry_limit_stops_next_entity_before_paid_io(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    ticks = itertools.count(start=0, step=1_000_000)

    with pytest.raises(cycle.ScopeCycleError, match='retry limit exhausted'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=_fake_subprocess(
                calls, first_retries=cycle.SCOPE_RETRY_LIMIT,
            ),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
            monotonic_ns=ticks.__next__,
        )

    assert len(calls) == 1
    assert not Path(payload['result_paths']['scope_manifest']).exists()
    attempts = json.loads(
        Path(payload['parent_ledger']['path'] + '.attempts').read_text()
    )
    assert attempts['retries'] == cycle.SCOPE_RETRY_LIMIT


def test_remaining_parent_retry_budget_is_pinned_in_argv_and_env(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    ticks = itertools.count(start=0, step=1_000_000)

    cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess(calls, first_retries=1),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )

    full = str(cycle.SCOPE_RETRY_LIMIT)
    after_one_retry = str(cycle.SCOPE_RETRY_LIMIT - 1)
    assert calls[0][0][calls[0][0].index('--retry-budget') + 1] == full
    assert calls[0][1]['env']['TM_RETRY_BUDGET'] == full
    for command, options in calls[1:]:
        assert command[command.index('--retry-budget') + 1] == after_one_retry
        assert options['env']['TM_RETRY_BUDGET'] == after_one_retry


def test_missing_output_is_red_and_never_checkpointed_or_promoted(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    ticks = itertools.count(start=0, step=1_000_000)

    def malformed(command, **kwargs):
        completed = _fake_subprocess(calls)(command, **kwargs)
        if command[command.index('--entity') + 1] == 'coaches':
            output = Path(command[command.index('--output') + 1])
            value = json.loads(output.read_text())
            value['outputs'].pop('stints')
            output.write_text(json.dumps(value), encoding='utf-8')
        return completed

    with pytest.raises(cycle.ScopeCycleError, match='coach_stints output is missing'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=malformed,
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
            monotonic_ns=ticks.__next__,
        )

    checkpoint = json.loads(
        (Path(payload['result_paths']['base_dir']) / 'scope-cycle-checkpoint.json')
        .read_text()
    )
    assert checkpoint['status'] == 'in_progress'
    assert 'coaches' not in checkpoint['entities']
    assert not Path(payload['result_paths']['scope_manifest']).exists()


def test_continental_scope_requires_every_listing_participant_endpoint(tmp_path):
    payload = _continental_payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []

    def incomplete_players(command, **kwargs):
        completed = _fake_subprocess(calls)(command, **kwargs)
        output = Path(command[command.index('--output') + 1])
        value = json.loads(output.read_text())
        capture = value['scope_capture']
        capture['observed_team_ids'] = ['1']
        capture['endpoint_status_by_team'] = {
            team_id: ('ok' if team_id == '1' else 'retry_exhausted')
            for team_id in capture['expected_team_ids']
        }
        output.write_text(json.dumps(value), encoding='utf-8')
        return completed

    with pytest.raises(cycle.ScopeCycleError, match='participant mismatch'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=incomplete_players,
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )

    assert len(calls) == 1
    assert not Path(payload['result_paths']['scope_manifest']).exists()


def test_approval_argv_drift_fails_before_subprocess(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    drifted = cycle.approved_operation_argv(argv) + ('--force-replace',)

    with pytest.raises(Exception, match='approval argv differs'):
        cycle.run_scope_cycle(
            args,
            operation_argv=drifted,
            subprocess_runner=lambda *args, **kwargs: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_scope_manifest_sql_is_exact_idempotent_complete_merge(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)
    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )

    sql = cycle.scope_manifest_merge_sql(manifest)
    assert cycle.SCOPE_MANIFEST_TABLE in sql
    assert 't.parent_cycle_id = s.parent_cycle_id' in sql
    assert 't.child_cycle_id = s.child_cycle_id' in sql
    assert 't.scope_id = s.scope_id' in sql
    assert 't.manifest_digest = s.manifest_digest' in sql
    assert f"'{cycle.SCOPE_COMPLETION_STATUS}'" in sql
    assert "'success'" not in sql
    assert "'failed'" not in sql
    assert '"status":"complete"' not in sql

    ledger = json.loads(Path(payload['parent_ledger']['path']).read_text())
    proxy_sql = cycle.proxy_ledger_merge_sql(ledger)
    assert cycle.PROXY_LEDGER_TABLE in proxy_sql
    assert 't.parent_cycle_id = s.parent_cycle_id' in proxy_sql
    assert 't.entity = s.entity' in proxy_sql
    assert proxy_sql.count("'scheduled__2026-07-11'") == 7
    assert str(cycle.PARENT_BYTE_BUDGET) in proxy_sql
    assert str(cycle.PARENT_SOFT_BYTE_STOP) in proxy_sql


def test_a_calendar_league_edition_is_read_as_the_season_it_is_labelled(tmp_path):
    # The source offsets some calendar leagues' saison_id from the season it
    # names: saison_id 2023 is the 2024 season. The registry records the label's
    # season, so deriving the season from the saison_id would reject the scope.
    snapshot = 'registry-snapshot-20260711'
    competition = resolve_competition('FIWC').as_dict()
    competition['registry_snapshot_id'] = snapshot
    edition = EditionRecord(
        competition_id='FIWC',
        edition_id='2023',
        edition_label='2024',
        canonical_season='2024',
        season_format=SeasonFormat.SINGLE_YEAR,
        start_date=None,
        end_date=None,
        active=True,
        current=False,
        participant_count=12,
        participant_hash='participants-2023',
        source_url='https://www.transfermarkt.com/x/startseite/wettbewerb/FIWC/saison_id/2023',
        discovered_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        registry_snapshot_id=snapshot,
        source_body_hash='edition-body-hash',
    )
    base = tmp_path / 'cycles' / ('a' * 64) / 'scopes' / ('b' * 64)
    payload = {
        'parent_cycle_id': 'scheduled__2026-07-11',
        'child_cycle_id': 'tm-child-0123456789abcdef01234567',
        'competition_id': 'FIWC',
        'edition_id': '2023',
        'canonical_season': '2024',
        'registry_snapshot_id': snapshot,
        'scope_id': deterministic_scope_id('FIWC', '2023'),
        'competition_record': competition,
        'edition_record': edition.as_dict(),
        'result_paths': {
            'base_dir': str(base),
            'entity_staging_dir': str(base / 'entities'),
            'scope_manifest': str(base / 'scope-manifest.json'),
        },
        'parent_ledger': {
            'parent_cycle_id': 'scheduled__2026-07-11',
            'path': str(tmp_path / 'cycles' / ('a' * 64) / 'proxy-ledger.json'),
        },
    }
    args = SimpleNamespace(
        payload_json=json.dumps(payload),
        parent_cycle_id=None,
        child_cycle_id=None,
        competition_id=None,
        edition_id=None,
        registry_snapshot_id=None,
        scope_id=None,
        canonical_competition_id=None,
        canonical_season=None,
        capture_revision=None,
        refresh_mode='historical',
        result_base_dir=None,
        entity_staging_dir=None,
        scope_manifest=None,
        parent_ledger_path=None,
    )

    identity = cycle._scope_identity(args)

    assert (identity.edition_id, identity.canonical_season) == ('2023', '2024')


def test_the_approved_interpreter_is_the_one_behind_the_alias(tmp_path):
    # The DAG's PATH finds /usr/local/bin/python while a shell finds another
    # alias of the same binary; an argv keyed on the alias would drift.
    real = Path(sys.executable).resolve()
    alias = tmp_path / 'python'
    alias.symlink_to(real)

    argv = cycle.approved_operation_argv(
        ('--refresh-mode', 'historical'), executable=str(alias),
    )

    assert argv[0] == str(real)


def _standing_policy_file(tmp_path: Path, **overrides) -> Path:
    now = datetime.now(timezone.utc)
    value = {
        'policy_version': 1,
        'dag_id': 'dag_ingest_transfermarkt',
        'approved_by': 'sergeykuznetsov1995',
        'approved_at': (now - timedelta(days=1)).isoformat(),
        'expires_at': (now + timedelta(days=30)).isoformat(),
        'paid_proxy': {
            'byte_cap_bytes': cycle.HARD_BYTE_CAP,
            'request_limit': sum(
                item['requests'] for item in cycle.DEFAULT_ENTITY_LIMITS.values()
            ),
            'retry_limit': cycle.SCOPE_RETRY_LIMIT,
            'concurrency': 1,
        },
        'production_write': {
            'byte_cap_bytes': 0,
            'request_limit': 0,
            'retry_limit': 0,
            'concurrency': 1,
        },
        'allowed_write_tables': sorted(
            cycle.required_write_tables('dual')
            | cycle.required_write_tables('native-only')
        ),
    }
    value.update(overrides)
    path = tmp_path / 'standing-approval-policy.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    return path


def _standing_args(
    tmp_path: Path, payload: dict, **policy_overrides,
) -> list[str]:
    policy_path = _standing_policy_file(tmp_path, **policy_overrides)
    policy = load_standing_policy(policy_path)
    return [
        '--payload-json', json.dumps(payload, sort_keys=True),
        '--reader-revision', '7',
        '--candidate-slot', 'b',
        '--write-mode', 'dual',
        '--standing-policy', str(policy_path),
        '--standing-policy-sha256', policy.policy_hash,
    ]


def test_standing_policy_cycle_commits_manifest_and_parent_ledger(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    persisted = []
    persisted_ledgers = []
    ticks = itertools.count(start=0, step=1_000_000)

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess(calls),
        manifest_writer=persisted.append,
        parent_ledger_writer=persisted_ledgers.append,
        monotonic_ns=ticks.__next__,
    )

    assert manifest['status'] == 'complete'
    assert persisted == [manifest]
    assert len(persisted_ledgers) == 1
    assert [
        command[command.index('--entity') + 1] for command, _ in calls
    ] == list(cycle.ENTITY_ORDER)
    policy = load_standing_policy(argv[argv.index('--standing-policy') + 1])
    assert all(
        options['env']['TM_PAID_PROXY_APPROVAL_PACKET_ID']
        == 'standing-policy-v1'
        and options['env']['TM_PAID_PROXY_APPROVAL_PACKET_HASH']
        == policy.policy_hash
        and options['env']['TM_PRODUCTION_WRITE_APPROVAL_PACKET_ID']
        == 'standing-policy-v1'
        for _, options in calls
    )
    assert Path(payload['parent_ledger']['path'] + '.attempts').is_file()
    assert Path(payload['parent_ledger']['path']).is_file()
    assert not (tmp_path / 'approvals.json').exists()
    assert not list(tmp_path.glob('**/journal*.json'))


def test_standing_policy_records_authorization_in_own_file(
    tmp_path, monkeypatch,
):
    # The manifest's dq_evidence key set is validator-bound, so the durable
    # record of which policy authorized the cycle lives in its own file —
    # not in scope-status.json, which a later failed CLI rerun overwrites.
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )

    policy = load_standing_policy(argv[argv.index('--standing-policy') + 1])
    base = Path(payload['result_paths']['base_dir'])
    record = json.loads((base / 'standing-authorization.json').read_text())
    assert record == {
        'status': 'complete',
        'parent_cycle_id': payload['parent_cycle_id'],
        'child_cycle_id': payload['child_cycle_id'],
        'scope_id': payload['scope_id'],
        'manifest_digest': manifest['manifest_digest'],
        'approval_mode': 'standing_policy',
        'standing_policy': {
            'policy_hash': policy.policy_hash,
            'policy_version': 1,
        },
    }
    assert not (base / 'scope-status.json').exists()


def test_one_shot_cycle_writes_no_standing_authorization_file(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)

    cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )

    base = Path(payload['result_paths']['base_dir'])
    assert not (base / 'standing-authorization.json').exists()
    assert not (base / 'scope-status.json').exists()


def test_standing_resume_rewrites_missing_authorization_record(
    tmp_path, monkeypatch,
):
    # A cycle that died between the complete checkpoint and the authorization
    # record must still leave the record behind after the no-op resume.
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)
    first = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )
    base = Path(payload['result_paths']['base_dir'])
    record_path = base / 'standing-authorization.json'
    original = record_path.read_text()
    record_path.unlink()

    second = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
        manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
        parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
    )

    assert second == first
    assert record_path.read_text() == original


def test_failed_cli_rerun_does_not_clobber_standing_authorization(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)
    cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )
    base = Path(payload['result_paths']['base_dir'])
    record_path = base / 'standing-authorization.json'
    original = record_path.read_text()

    drifted = list(argv)
    drifted[drifted.index('--standing-policy-sha256') + 1] = '0' * 64

    assert cycle.main(drifted) == 1

    failure = json.loads((base / 'scope-status.json').read_text())
    assert failure['status'] == 'failed'
    assert failure['error_type'] == 'ApprovalDriftError'
    assert record_path.read_text() == original


def test_standing_policy_flag_without_sha_fails_before_subprocess(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    index = argv.index('--standing-policy-sha256')
    del argv[index:index + 2]
    args = _parse_args(argv)

    with pytest.raises(
        cycle.ScopeCycleError, match='standing_policy_sha256 is required',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_sha_without_policy_path_fails_before_subprocess(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    index = argv.index('--standing-policy')
    del argv[index:index + 2]
    args = _parse_args(argv)

    with pytest.raises(
        cycle.ScopeCycleError, match='standing_policy is required',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_one_shot_without_journal_fails_before_subprocess(tmp_path):
    payload = _payload(tmp_path)
    argv = [
        '--payload-json', json.dumps(payload, sort_keys=True),
        '--reader-revision', '7',
        '--candidate-slot', 'b',
        '--write-mode', 'dual',
        '--paid-proxy-approval-packet-id', 'paid-1',
        '--paid-proxy-approval-packet-hash', 'a' * 64,
        '--production-write-approval-packet-id', 'write-1',
        '--production-write-approval-packet-hash', 'b' * 64,
    ]
    args = _parse_args(argv)

    with pytest.raises(
        cycle.ScopeCycleError, match='approval_journal is required',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_policy_requires_env_gate(tmp_path, monkeypatch):
    monkeypatch.delenv('TM_STANDING_POLICY_ENABLED', raising=False)
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)

    with pytest.raises(
        cycle.ScopeCycleError, match='TM_STANDING_POLICY_ENABLED must be true',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_policy_content_drift_fails_before_subprocess(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    argv[argv.index('--standing-policy-sha256') + 1] = '0' * 64
    args = _parse_args(argv)

    with pytest.raises(Exception, match='differs from the pinned sha256'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_and_one_shot_flags_are_mutually_exclusive(tmp_path):
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload) + [
        '--approval-journal', str(tmp_path / 'approvals.json'),
    ]

    with pytest.raises(cycle.ScopeCycleError, match='mutually exclusive'):
        _parse_args(argv)

    bare = [
        '--payload-json', json.dumps(payload, sort_keys=True),
        '--reader-revision', '7',
        '--candidate-slot', 'b',
        '--write-mode', 'dual',
    ]
    with pytest.raises(cycle.ScopeCycleError, match='approval mode is required'):
        _parse_args(bare)


@pytest.mark.parametrize(
    'paid_override',
    [
        {'byte_cap_bytes': 16 * 1024 * 1024},
        {'request_limit': 1609},
        {'retry_limit': 799},
        {'concurrency': 2},
    ],
)
def test_standing_policy_limits_must_match_wrapper_argv(
    tmp_path, monkeypatch, paid_override,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    paid = {
        'byte_cap_bytes': cycle.HARD_BYTE_CAP,
        'request_limit': sum(
            item['requests'] for item in cycle.DEFAULT_ENTITY_LIMITS.values()
        ),
        'retry_limit': cycle.SCOPE_RETRY_LIMIT,
        'concurrency': 1,
    }
    paid.update(paid_override)
    argv = _standing_args(tmp_path, payload, paid_proxy=paid)
    args = _parse_args(argv)

    with pytest.raises(
        cycle.ScopeCycleError, match='caps differ from wrapper limits',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_policy_expired_fails_before_subprocess(tmp_path, monkeypatch):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    now = datetime.now(timezone.utc)
    argv = _standing_args(
        tmp_path,
        payload,
        approved_at=(now - timedelta(days=2)).isoformat(),
        expires_at=(now - timedelta(days=1)).isoformat(),
    )
    args = _parse_args(argv)

    with pytest.raises(Exception, match='expired'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_standing_policy_resume_uses_no_subprocess(tmp_path, monkeypatch):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    ticks = itertools.count(start=0, step=1_000_000)
    first = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=ticks.__next__,
    )

    second = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
        manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
        parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
    )

    assert second == first


def test_standing_cycle_retry_limit_still_stops_before_paid_io(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    payload = _payload(tmp_path)
    argv = _standing_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []
    ticks = itertools.count(start=0, step=1_000_000)

    with pytest.raises(cycle.ScopeCycleError, match='retry limit exhausted'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=_fake_subprocess(
                calls, first_retries=cycle.SCOPE_RETRY_LIMIT,
            ),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
            monotonic_ns=ticks.__next__,
        )

    assert len(calls) == 1
    assert not Path(payload['result_paths']['scope_manifest']).exists()
    attempts = json.loads(
        Path(payload['parent_ledger']['path'] + '.attempts').read_text()
    )
    assert attempts['retries'] == cycle.SCOPE_RETRY_LIMIT


# ---------------------------------------------------------------------------
# Budget canon pinning: scope caps vs the parent (daily) aggregate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('flag', 'value', 'message'),
    [
        ('--cycle-budget-bytes', cycle.HARD_BYTE_CAP + 1,
         'scope byte cap must equal'),
        ('--soft-byte-stop-bytes', cycle.SOFT_BYTE_STOP - 1,
         'scope soft byte stop must equal'),
        ('--request-limit', cycle.SCOPE_REQUEST_LIMIT - 1,
         'scope request/retry limits must equal'),
        ('--retry-limit', cycle.SCOPE_RETRY_LIMIT + 1,
         'scope request/retry limits must equal'),
        ('--parent-byte-budget', cycle.PARENT_BYTE_BUDGET - 1,
         'parent byte budget must equal'),
        ('--parent-soft-byte-stop', cycle.PARENT_SOFT_BYTE_STOP + 1,
         'parent soft byte stop must equal'),
        ('--parent-request-limit', cycle.PARENT_REQUEST_LIMIT + 1,
         'parent request/retry limits must equal'),
        ('--parent-retry-limit', cycle.PARENT_RETRY_LIMIT - 1,
         'parent request/retry limits must equal'),
    ],
)
def test_any_drifted_paid_cap_is_refused_at_argv_validation(
    tmp_path, flag, value, message,
):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)

    with pytest.raises(cycle.ScopeCycleError, match=message):
        _parse_args(argv + [flag, str(value)])


def _daily_scope_fixture(tmp_path: Path, index: int, provider_bytes: int):
    """One committed scope manifest worth ``provider_bytes`` for the parent."""

    identity = SimpleNamespace(
        parent_cycle_id='scheduled__daily',
        child_cycle_id=f'tm-child-{index}',
        scope_id=f'SC{index}__2025',
        parent_ledger_path=str(tmp_path / 'proxy-ledger.json'),
    )
    fields = (
        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
        'requests', 'retries', 'cache_hits', 'duration_ms',
    )
    entities = []
    for position, entity in enumerate(cycle.EXPECTED_ENTITIES):
        share = provider_bytes if position == 0 else 0
        entities.append({
            'entity': entity,
            'decoded_bytes': share,
            'wire_bytes': share,
            'provider_metered_bytes': share,
            'requests': 1 if position == 0 else 0,
            'retries': 0,
            'cache_hits': 0,
            'duration_ms': 1 if position == 0 else 0,
        })
    totals = {
        field: sum(int(item[field]) for item in entities) for field in fields
    }
    manifest = {
        'manifest_digest': f'{index:064x}',
        'entities': entities,
        'traffic': {'totals': totals},
    }
    return identity, manifest


def _update_daily_ledger(identity, manifest):
    return cycle._update_parent_ledger(
        identity,
        manifest,
        hard_cap=cycle.PARENT_BYTE_BUDGET,
        soft_stop=cycle.PARENT_SOFT_BYTE_STOP,
        request_limit=cycle.PARENT_REQUEST_LIMIT,
        retry_limit=cycle.PARENT_RETRY_LIMIT,
    )


def test_parent_daily_ledger_admits_four_full_scopes_and_stops_the_next_byte(
    tmp_path,
):
    # A cold big-league scope costs ~18-21 MiB; four of them at 21 MiB land
    # exactly on the 84 MiB daily budget and every one must be admitted.
    per_scope = 21 * cycle.MIB
    assert 4 * per_scope == cycle.PARENT_BYTE_BUDGET
    payload = None
    for index in range(4):
        identity, manifest = _daily_scope_fixture(tmp_path, index, per_scope)
        payload = _update_daily_ledger(identity, manifest)

    assert payload['manifest_count'] == 4
    assert payload['provider_metered_bytes'] == cycle.PARENT_BYTE_BUDGET
    assert payload['hard_provider_byte_budget'] == cycle.PARENT_BYTE_BUDGET
    assert payload['soft_provider_byte_stop'] == cycle.PARENT_SOFT_BYTE_STOP

    # One more byte anywhere in the day pierces the parent budget.
    identity, manifest = _daily_scope_fixture(tmp_path, 4, 1)
    with pytest.raises(
        cycle.ScopeCycleError, match='parent provider byte budget exceeded',
    ):
        _update_daily_ledger(identity, manifest)


def test_scope_above_its_own_cap_fails_even_with_an_empty_parent_ledger(
    tmp_path,
):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)

    def mutate(command, result):
        # 4 x 7 MiB = 28 MiB provider-metered: over the 24 MiB scope cap while
        # the parent (daily) ledger is still completely empty.
        result['provider_metered_bytes'] = 7 * cycle.MIB
        return result

    with pytest.raises(
        cycle.ScopeCycleError, match='scope provider traffic exceeds',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=_fake_subprocess([], mutate=mutate),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


# ---------------------------------------------------------------------------
# Daily byte admission before any paid I/O
# ---------------------------------------------------------------------------

def _write_sibling_scope_ledger(
    payload: dict, name: str, *, run_key: str,
    consumed: int = 0, reserved: int = 0,
):
    """One runner-side scope file ledger left behind by another scope."""

    ledger_dir = Path(payload['parent_ledger']['path']).parent
    ledger_dir.mkdir(parents=True, exist_ok=True)
    events = [{'entity': 'players', 'decoded_response_body_bytes': consumed}]
    reservations = (
        [{
            'reservation_id': 'r-1', 'entity': 'players',
            'reserved_bytes': reserved, 'status': 'active',
        }]
        if reserved else []
    )
    (ledger_dir / f'transfermarkt_cycle_{name}.json').write_text(
        json.dumps({
            'run_key': run_key,
            'limit_bytes': cycle.HARD_BYTE_CAP,
            'events': events,
            'reservations': reservations,
        }),
        encoding='utf-8',
    )


def test_daily_admission_accepts_the_fourth_scope_at_sixty_mib(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    for index in range(3):
        _write_sibling_scope_ledger(
            payload, f'{index:024x}', run_key=f'other-child-{index}',
            consumed=20 * cycle.MIB,
        )

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=itertools.count(start=0, step=1_000_000).__next__,
    )

    # 60 MiB committed + one full 24 MiB scope cap == exactly the 84 MiB day.
    assert manifest['status'] == 'complete'


def test_daily_admission_refuses_one_byte_past_sixty_mib(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    _write_sibling_scope_ledger(
        payload, 'a' * 24, run_key='other-child-a',
        consumed=60 * cycle.MIB + 1,
    )

    with pytest.raises(
        cycle.ScopeCycleError, match='cannot admit another scope',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_daily_admission_counts_a_failed_scope_without_a_manifest(tmp_path):
    # A crashed sibling left only an active reservation (no manifest, no
    # parent-ledger entry) — its bytes still count against the day.
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    _write_sibling_scope_ledger(
        payload, 'b' * 24, run_key='crashed-child',
        consumed=40 * cycle.MIB, reserved=20 * cycle.MIB + 1,
    )

    with pytest.raises(
        cycle.ScopeCycleError, match='cannot admit another scope',
    ):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_daily_admission_ignores_this_scopes_own_resumed_ledger(tmp_path):
    # The current scope's own earlier attempt is bounded by its own file
    # ledger; the gate adds a full scope cap for it instead of double-counting.
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    _write_sibling_scope_ledger(
        payload, 'c' * 24, run_key=payload['child_cycle_id'],
        consumed=61 * cycle.MIB,
    )
    _write_sibling_scope_ledger(
        payload, 'd' * 24, run_key='other-child-d',
        consumed=60 * cycle.MIB,
    )

    manifest = cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess([]),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=itertools.count(start=0, step=1_000_000).__next__,
    )

    assert manifest['status'] == 'complete'


def test_career_entities_get_the_canon_timeout(tmp_path):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    calls = []

    cycle.run_scope_cycle(
        args,
        operation_argv=cycle.approved_operation_argv(argv),
        subprocess_runner=_fake_subprocess(calls),
        manifest_writer=lambda manifest: None,
        parent_ledger_writer=lambda ledger: None,
        monotonic_ns=itertools.count(start=0, step=1_000_000).__next__,
    )

    timeouts = {
        command[command.index('--entity') + 1]: options['timeout']
        for command, options in calls
    }
    assert timeouts == {
        'players': 3600,
        'market_value_history': cycle.CAREER_ENTITY_TIMEOUT_SECONDS,
        'transfers': cycle.CAREER_ENTITY_TIMEOUT_SECONDS,
        'coaches': 3600,
    }
    assert cycle.CAREER_ENTITY_TIMEOUT_SECONDS == 5_400


# ---------------------------------------------------------------------------
# Grant cross-checks in _traffic_metrics
# ---------------------------------------------------------------------------

def _grant_metrics_run(
    *, grant, hard, soft, provider_bytes=1,
) -> cycle.EntityRun:
    result = {
        'traffic': {
            'telemetry_available': True,
            'hard_provider_byte_budget': hard,
            'soft_provider_byte_stop': soft,
        },
        'provider_metering_available': True,
        'decoded_response_body_bytes': 1,
        'wire_response_bytes': 1,
        'provider_metered_bytes': provider_bytes,
        'network_fetches': 1,
        'retries': 0,
        'cache_hits': 0,
    }
    if grant is not None:
        result['provider_byte_grant'] = grant
    return cycle.EntityRun(
        parser_entity='players',
        result_path='ignored',
        result_sha256='0' * 64,
        result=result,
        wall_clock_duration_ms=1,
        resumed=False,
    )


@pytest.mark.parametrize(
    ('run_kwargs', 'message'),
    [
        # Client ledger above the pinned scope cap.
        (dict(grant=cycle.HARD_BYTE_CAP + 1, hard=cycle.HARD_BYTE_CAP + 1,
              soft=cycle.SOFT_BYTE_STOP), 'hard provider cap drift'),
        # Ledger does not equal min(grant, scope cap).
        (dict(grant=10 * cycle.MIB, hard=cycle.HARD_BYTE_CAP,
              soft=cycle.SOFT_BYTE_STOP), 'hard provider cap drift'),
        # Degenerate soft stop.
        (dict(grant=10 * cycle.MIB, hard=10 * cycle.MIB,
              soft=10 * cycle.MIB), 'soft provider stop drift'),
        # No grant evidence at all.
        (dict(grant=None, hard=cycle.HARD_BYTE_CAP,
              soft=cycle.SOFT_BYTE_STOP), 'grant evidence is absent'),
        # The ledger was pierced.
        (dict(grant=10 * cycle.MIB, hard=10 * cycle.MIB,
              soft=9 * cycle.MIB, provider_bytes=10 * cycle.MIB + 1),
         'exceeds its grant ledger'),
    ],
)
def test_traffic_metrics_rejects_grant_boundary_violations(
    run_kwargs, message,
):
    run = _grant_metrics_run(**run_kwargs)

    with pytest.raises(cycle.ScopeCycleError, match=message):
        cycle._traffic_metrics(run, hard_cap=cycle.HARD_BYTE_CAP)


def test_traffic_metrics_accepts_a_partial_grant_ledger():
    run = _grant_metrics_run(
        grant=10 * cycle.MIB, hard=10 * cycle.MIB, soft=9 * cycle.MIB,
        provider_bytes=10 * cycle.MIB,
    )

    metrics = cycle._traffic_metrics(run, hard_cap=cycle.HARD_BYTE_CAP)

    assert metrics['provider_metered_bytes'] == 10 * cycle.MIB


# ---------------------------------------------------------------------------
# Sibling scope-ledger contract (cross-process, so pinned explicitly)
# ---------------------------------------------------------------------------

def test_sibling_scope_ledger_contract_is_pinned(tmp_path):
    payload = _payload(tmp_path)
    identity = cycle._scope_identity(_parse_args(_approved_args(tmp_path, payload)[0]))
    ledger_dir = Path(payload['parent_ledger']['path']).parent
    ledger_dir.mkdir(parents=True, exist_ok=True)

    # The runner writes exactly this shape, under exactly this name, into
    # exactly this directory (its TM_CYCLE_BUDGET_DIR is the parent ledger's
    # own directory — see _runner_environment).
    from dags.scripts import run_transfermarkt_scraper as runner

    assert runner._cycle_budget_path(
        'other-child', root_override=str(ledger_dir),
    ) == str(
        ledger_dir
        / f'transfermarkt_cycle_'
          f'{hashlib.sha256(b"other-child").hexdigest()[:24]}.json'
    )

    (ledger_dir / 'transfermarkt_cycle_deadbeef.json').write_text(
        json.dumps({
            'run_key': 'other-child',
            'limit_bytes': cycle.HARD_BYTE_CAP,
            'events': [
                {'entity': 'players', 'decoded_response_body_bytes': 7},
                {'entity': 'transfers', 'decoded_response_body_bytes': 3},
            ],
            'reservations': [
                {'reservation_id': 'a', 'reserved_bytes': 11, 'status': 'active'},
                {'reservation_id': 'b', 'reserved_bytes': 99, 'status': 'settled'},
            ],
        }),
        encoding='utf-8',
    )
    # Settled events + still-active reservations; settled reservations are
    # already represented by their event.
    assert cycle._sibling_scope_ledger_bytes(identity) == 7 + 3 + 11

    # A file that does not match the runner's name is not a scope ledger.
    (ledger_dir / 'unrelated.json').write_text('{"events": [{"a": 1}]}')
    assert cycle._sibling_scope_ledger_bytes(identity) == 21


@pytest.mark.parametrize('body', ['{not json', '[]'])
def test_unreadable_sibling_scope_ledger_refuses_paid_io(tmp_path, body):
    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    ledger_dir = Path(payload['parent_ledger']['path']).parent
    ledger_dir.mkdir(parents=True, exist_ok=True)
    (ledger_dir / 'transfermarkt_cycle_broken.json').write_text(
        body, encoding='utf-8',
    )

    with pytest.raises(cycle.ScopeCycleError, match='sibling scope ledger'):
        cycle.run_scope_cycle(
            args,
            operation_argv=cycle.approved_operation_argv(argv),
            subprocess_runner=lambda *a, **kw: pytest.fail('subprocess ran'),
            manifest_writer=lambda manifest: pytest.fail('manifest persisted'),
            parent_ledger_writer=lambda ledger: pytest.fail('ledger persisted'),
        )


def test_the_scope_task_timeout_covers_every_entity_timeout(tmp_path):
    from scrapers.transfermarkt import models

    payload = _payload(tmp_path)
    argv, _ = _approved_args(tmp_path, payload)
    args = _parse_args(argv)
    worst_case = sum(
        cycle._entity_timeout_seconds(args, entity)
        for entity in cycle.ENTITY_ORDER
    )

    assert worst_case == 18_000
    assert worst_case <= models.SCOPE_WALL_CLOCK_TIMEOUT_SECONDS == 18_900
    # A caller may only shorten a small entity's timeout, never lengthen it.
    with pytest.raises(cycle.ScopeCycleError, match='entity timeout cannot exceed'):
        _parse_args(argv + ['--entity-timeout-seconds', '3601'])
