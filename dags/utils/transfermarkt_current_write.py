"""Bounded current Bronze writes with physical read-back before acknowledgement.

This module writes only the requested business bundle. It does not claim that
an edition has completed every source entity, and it never supplies scope DQ.
"""
from __future__ import annotations

from dataclasses import replace
from contextlib import contextmanager
from functools import wraps
from datetime import date, datetime, timezone
import hashlib
import json
import os
import time
from typing import Any, Mapping

import pandas as pd

from dags.scripts import run_transfermarkt_scraper as run
from scrapers.transfermarkt.writer import writer_lock, writer_deadline, guard_frames
from scrapers.transfermarkt.client import CurrentPortionDeadlineExceeded
from scrapers.transfermarkt.current_capture import FullRosterSnapshot
from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.season import season_to_saison_id


class CurrentWriteError(RuntimeError):
    """No acknowledgement may escape a failed physical current write."""


def bind_current_connection_deadline(connection, deadline_monotonic, clock=time.monotonic):
    """Use the lane's per-poll bound on this connection's original HTTP session."""
    from dags.scripts.run_transfermarkt_current import _bound_connection

    return _bound_connection(connection, float(deadline_monotonic), clock)


@contextmanager
def _current_writer_deadline(scraper):
    deadline = getattr(scraper, '_current_deadline_monotonic', None)
    if deadline is None:
        yield
        return
    clock = getattr(scraper, '_current_clock', getattr(scraper._http_client, '_monotonic', time.monotonic))
    manager = scraper._iceberg_writer._get_trino_manager()
    original_factory = manager._create_connection
    missing = object()
    retries = {key: manager.__dict__.get(key, missing) for key in ('_CONNECT_RETRIES', '_COMMIT_RETRIES')}
    def create():
        if deadline - clock() <= 30:
            raise CurrentWriteError('current deadline prohibits another writer connection')
        return bind_current_connection_deadline(original_factory(), deadline, clock)
    manager._create_connection = create
    manager._CONNECT_RETRIES = manager._COMMIT_RETRIES = 1
    try:
        if manager._conn is not None:
            bind_current_connection_deadline(manager._conn, deadline, clock)
        yield
    finally:
        manager._create_connection = original_factory
        for key, value in retries.items():
            if value is missing:
                manager.__dict__.pop(key, None)
            else:
                setattr(manager, key, value)


def _bounded_current_writer(function):
    @wraps(function)
    def call(scraper, *args, **kwargs):
        with _current_writer_deadline(scraper):
            return function(scraper, *args, **kwargs)
    return call


def _authorize(preflight):
    if not isinstance(preflight, Mapping) or preflight.get('write_mode') not in {'dual', 'native-only'}:
        raise CurrentWriteError('current writer requires qualified dual/native-only preflight')
    revision = preflight.get('revision')
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise CurrentWriteError('current writer requires exact preflight revision')
    return preflight['write_mode'], revision, run._authorize_write_mode(preflight['write_mode'], revision)


def _scope(scraper, scope):
    competition = scope.competition_id if isinstance(scope, FullRosterSnapshot) else scope['competition_id']
    edition = scope.edition_id if isinstance(scope, FullRosterSnapshot) else scope['edition_id']
    resolved = scraper._resolve_scope(str(competition), str(edition))
    if str(resolved['competition_id']) != str(competition) or str(resolved['edition_id']) != str(edition):
        raise CurrentWriteError('resolved writer scope differs from requested registry scope')
    if isinstance(scope, FullRosterSnapshot):
        saison = int(resolved.get('saison_id', season_to_saison_id(resolved['canonical_season'], resolved['season_format'])))
        if saison != scope.squad_saison_id or resolved['scope_id'] != scope.scope_id:
            raise CurrentWriteError('full roster saison/registry identity differs')
    return resolved


def _stamp(value):
    return pd.Timestamp(value).tz_convert('UTC').tz_localize(None).to_pydatetime()


def _cell(column, value):
    if value is None or pd.isna(value):
        return None
    if column in {'fetched_at', 'observed_at', '_ingested_at'}:
        parsed = pd.Timestamp(value)
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert('UTC').tz_localize(None)
        return parsed.isoformat(timespec='microseconds')
    return run._canonical_cell(value)


def _rows(frame):
    rows = [tuple(_cell(column, value) for column, value in zip(frame.columns, row))
            for row in frame.itertuples(index=False, name=None)]
    return sorted(rows, key=lambda row: json.dumps(row))


def _read_back(scraper, output, frame, cycle_id, *, empty_ids=(), empty_key='player_id', scope=None, snapshot_id=None):
    table = 'iceberg.bronze.' + output.table_name
    relation = table + (f' FOR VERSION AS OF {snapshot_id}' if snapshot_id else '')
    if empty_ids and not (empty_key == 'club_id' and output.key == 'profiles'):
        placeholders = ', '.join('?' for _ in empty_ids)
        predicate = f'{empty_key} IN ({placeholders})'
        params = list(empty_ids)
        if output.is_legacy and empty_key == 'current_club_id':
            predicate += ' AND league = ? AND season = ?'
            params += [scope['compatibility_league'], scope['canonical_season']]
        try:
            rows = run._execute_cursor(scraper._bronze_connection(),
                f'SELECT {empty_key} FROM {relation} WHERE {predicate}', params, fetch=True)
        except Exception as exc:
            if not frame.empty or not _missing_table(exc):
                raise CurrentWriteError('authoritative empty physical read-back failed') from exc
            rows = []
        if rows:
            raise CurrentWriteError(f'authoritative empty rows remain in {table}')
    if frame.empty:
        predicate = '_batch_id = ?'
        params = [str(scraper._batch_id)]
        if {'competition_id', 'edition_id'} <= set(frame.columns):
            predicate += ' AND competition_id = ? AND edition_id = ?'
            params += [scope['competition_id'], scope['edition_id']]
        try:
            persisted = run._query_dataframe(scraper._bronze_connection(),
                f'SELECT {", ".join(frame.columns)} FROM {relation} WHERE {predicate}', params)
        except Exception as exc:
            if not any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist')):
                raise CurrentWriteError('empty output physical read-back failed') from exc
            persisted = frame
        if not persisted.empty:
            raise CurrentWriteError('empty output contains current physical batch rows')
        return {'table': table, 'batch_id': str(scraper._batch_id), 'rows': 0,
                'physical_hash': hashlib.sha256(b'[]').hexdigest()}
    if '_batch_id' not in frame or frame['_batch_id'].isna().any() or frame['_batch_id'].nunique() != 1:
        raise CurrentWriteError(f'exact batch identity unavailable for {output.key}')
    batch_id = run._frame_batch_id(frame, cycle_id)
    predicate = '_batch_id = ?'
    params = [batch_id]
    if output.replace_keys:
        predicate += ' AND (' + scraper._build_partition_delete_filter(frame, list(output.replace_keys)) + ')'
    elif {'competition_id', 'edition_id'} <= set(frame.columns):
        predicate += ' AND competition_id = ? AND edition_id = ?'
        params += [str(frame.iloc[0]['competition_id']), str(frame.iloc[0]['edition_id'])]
    try:
        persisted = run._query_dataframe(scraper._bronze_connection(),
            f'SELECT {", ".join(frame.columns)} FROM {relation} WHERE {predicate}', params)
    except Exception as exc:
        raise CurrentWriteError(f'physical read-back failed for {table}') from exc
    if list(persisted.columns) != list(frame.columns) or _rows(persisted) != _rows(frame):
        raise CurrentWriteError(f'physical business/lineage mismatch in {table}')
    digest = hashlib.sha256(json.dumps(_rows(persisted), sort_keys=True).encode()).hexdigest()
    club_column = next((column for column in ('club_id', 'team_id', 'current_club_id') if column in persisted), None)
    club_hashes = ({str(club): {'rows': len(part), 'physical_hash': hashlib.sha256(json.dumps(_rows(part), sort_keys=True).encode()).hexdigest()}
                   for club, part in persisted.groupby(club_column)} if club_column else {})
    return {'table': table, 'batch_id': batch_id, 'rows': len(persisted), 'physical_hash': digest,
            **({'club_hashes': club_hashes} if club_hashes else {})}


def _precheck(scraper, spec, frames):
    # Every applicable 90% guard runs before any output can delete its partition.
    for output in spec.outputs:
        frame = frames[output.key]
        if not frame.empty and output.guard_key:
            deletion = scraper._build_partition_delete_filter(frame, list(output.replace_keys))
            scraper._enforce_replace_guard(frame, 'bronze', output.table_name,
                                           deletion, run._MIN_REPLACE_RATIO, output.guard_key)


class _CurrentMetadataWriter:
    """Preserve the parser's shared unit identity through BaseScraper.save."""

    def __init__(self, writer):
        self.writer = writer

    def __getattr__(self, name):
        return getattr(self.writer, name)

    def write_dataframe(self, **kwargs):
        return self.writer.write_dataframe(**{**kwargs, 'add_metadata': False})


def _save_current_frame(scraper, **options):
    original = scraper._iceberg_writer
    scraper._iceberg_writer = _CurrentMetadataWriter(original)
    try:
        return scraper.save_to_iceberg(**options)
    finally:
        scraper._iceberg_writer = original


def _commit(scraper, spec, frames, scope, mode, revision, cycle_id, *, empty_ids=(), empty_key='player_id'):
    with writer_deadline(getattr(scraper, '_current_deadline_monotonic', None)), writer_lock():
        run._authorize_write_mode(mode, revision)
        results = {'outputs': {key: run._frame_output_summary(frame) for key, frame in frames.items()}, 'tables': []}
        frames = run._carry_forward_observed_at(scraper, spec, frames, results)
        # A portion may write this scope twice. Distinct business bundles must not
        # mix their batches or overwrite each other's compatibility manifest rows.
        identity = {'cycle_id': cycle_id, 'scope_id': scope['scope_id'], 'entity': spec.name,
                    'frames': {key: _rows(frame.drop(columns=['_batch_id', '_ingested_at'], errors='ignore'))
                               for key, frame in frames.items()}}
        unit_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:32]
        scraper._batch_id = unit_id
        frames = {key: frame.assign(_batch_id=unit_id) for key, frame in frames.items()}
        manifest_cycle = f'{cycle_id}:current-unit:{unit_id}'
        from scrapers.transfermarkt.career_refs import retained_bundle_snapshots, persist_capture_refs
        connection = scraper._bronze_connection()
        try:
            snapshots = retained_bundle_snapshots(connection, manifest_cycle, spec.outputs, frames)
        finally:
            connection.close()
        if not snapshots:
            guard_frames(scraper, spec.outputs, frames)
            _precheck(scraper, spec, frames)
            for output in spec.outputs:
                frame = frames[output.key]
                table = 'iceberg.bronze.' + output.table_name
                if not frame.empty:
                    options = dict(df=frame, table_name=output.table_name, partition_cols=list(output.partition_cols))
                    if output.key == 'attribute_observations':
                        options['natural_keys'] = ['competition_id', 'edition_id', 'club_id', 'player_id', 'observed_at']
                    else:
                        options.update(replace_partitions=list(output.replace_keys) or None,
                            min_replace_ratio=run._MIN_REPLACE_RATIO if output.guard_key else None,
                            replace_guard_key=output.guard_key)
                    table = _save_current_frame(scraper, **options)
                results['outputs'][output.key]['table'] = table
            if empty_ids:
                run._delete_valid_empty_rows(scraper, spec, empty_ids, scope['compatibility_league'],
                                             int(scope['edition_id']), source_key=empty_key)
            if spec.name == run.ENTITY_COACHES and mode == 'dual':
                clubs = sorted(set(frames['stints']['club_id'].astype(str))) or list(empty_ids)
                run._delete_empty_season_coach_rows(scraper, clubs, frames['legacy_coaches'],
                    scope['compatibility_league'], int(scope['edition_id']))
        if snapshots:
            for output in spec.outputs:
                results['outputs'][output.key]['table'] = 'iceberg.bronze.' + output.table_name
        else:
            connection = scraper._bronze_connection()
            try:
                for output in spec.outputs:
                    if not output.is_legacy:
                        persist_capture_refs(connection, manifest_cycle, output.key, output.table_name,
                            frames[output.key], legacy=mode == 'dual')
                snapshots = retained_bundle_snapshots(connection, manifest_cycle, spec.outputs, frames)
            finally:
                connection.close()
        proof = {output.key: _read_back(scraper, output, frames[output.key], cycle_id,
                 empty_ids=empty_ids, empty_key='current_club_id' if empty_key == 'club_id' and output.is_legacy else empty_key, scope=scope, snapshot_id=snapshots.get(output.key))
                 for output in spec.outputs if not (empty_key == 'club_id' and output.key == 'profiles' and frames[output.key].empty)}
        manifests = []
        if mode == 'native-only':
            manifests.append(run._persist_native_write_manifest(scraper, spec, frames, results,
                manifest_cycle, scope['competition_id'], int(scope['edition_id']), revision))
        else:
            legacy = next(output for output in spec.outputs if output.is_legacy)
            for native in (output for output in spec.outputs if not output.is_legacy):
                paired = replace(spec, outputs=(native, legacy))
                pair_frames = {native.key: frames[native.key], legacy.key: frames[legacy.key]}
                if native.key == 'attribute_observations':
                    clubs = set(frames[native.key]['club_id'].astype(str))
                    pair_frames[legacy.key] = frames[legacy.key][frames[legacy.key]['current_club_id'].astype(str).isin(clubs)]
                manifests.append(run._persist_dual_write_manifest(scraper, paired, pair_frames, results,
                    manifest_cycle, scope['competition_id'], int(scope['edition_id'])))
        if any(item.get('status') != 'success' for item in manifests):
            raise CurrentWriteError('current business compatibility manifest did not pass')
        digest = hashlib.sha256(json.dumps({'cycle_id': cycle_id, 'outputs': proof, 'manifests': manifests}, sort_keys=True).encode()).hexdigest()
        return {'verified': True, 'bronze_manifest': digest,
                'committed_at': datetime.now(timezone.utc).isoformat(), 'outputs': proof,
                'cycle_id': cycle_id, 'manifest_cycle_id': manifest_cycle,
                'manifests': manifests, 'writer_revision': revision, 'write_mode': mode}, frames

def _roster_frames(scraper, snapshot, scope, cycle_id):
    now = _stamp(getattr(scraper, '_current_roster_write_at', None) or datetime.now(timezone.utc))
    retained_stamps = _retained_roster_stamps(scraper, snapshot, scope)
    memberships, observations, contracts = [], [], []
    for row in snapshot.rows:
        row = dict(row)
        for column in ('dob', 'contract_until'):
            value = row.get(column)
            if value is not None:
                try:
                    row[column] = value.date() if isinstance(value, datetime) else value if isinstance(value, date) else date.fromisoformat(value)
                except (TypeError, ValueError) as exc:
                    raise CurrentWriteError('warm roster lost its typed DATE field') from exc
        fetched_at = _stamp(row['_source_fetched_at'])
        changed = str(row['club_id']) in snapshot.changed_club_ids
        key = (str(row['club_id']), str(row['player_id']), row['_source_body_hash'], fetched_at.isoformat(timespec='microseconds'))
        observed_at = now if changed else retained_stamps.get(('memberships', key), fetched_at)
        lineage = {'source_competition_id': scope['competition_id'], 'source_edition_id': scope['edition_id'],
            'source_url': row['_source_url'], 'source_body_hash': row['_source_body_hash'], 'fetched_at': fetched_at,
            'parser_revision': tm.PARSER_REVISION, 'schema_revision': tm.SCHEMA_REVISION,
            'cycle_id': cycle_id, 'scope_id': scope['scope_id']}
        common = {'competition_id': scope['competition_id'], 'edition_id': scope['edition_id'],
            'league': scope['compatibility_league'], 'season': scope['canonical_season'],
            'club_id': str(row['club_id']), 'player_id': str(row['player_id']), 'observed_at': observed_at, **lineage}
        memberships.append({**common, 'club_slug': row.get('club_slug'), 'club_name': row.get('current_club_name'),
                            'player_slug': row['player_slug'], 'player_name': row['name']})
        observations.append({**row, **common, 'club_name': row.get('current_club_name')})
        if scope['record'].team_type.value == 'club':
            contracts.append({**lineage, 'competition_id': scope['competition_id'], 'edition_id': scope['edition_id'],
                'team_id': str(row['club_id']), 'team_name': row.get('current_club_name'), 'player_id': str(row['player_id']),
                'contract_until': row.get('contract_until'),
                'observed_at': now if changed else retained_stamps.get(('contract_observations', key), fetched_at),
                'applicability_status': 'ok'})
    bundle = {}
    for key, rows, columns, entity in (
        ('memberships', memberships, tm.SQUAD_MEMBERSHIP_COLUMNS, 'squad_memberships'),
        ('attribute_observations', observations, tm.PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS, 'player_attribute_observations'),
        ('contract_observations', contracts, tm.PLAYER_CONTRACT_OBSERVATION_COLUMNS, 'player_contract_observations')):
        bundle[key] = tm._with_metadata(rows, columns, entity_type=entity, batch_id=scraper._batch_id, ingested_at=now)
    bundle['contract_observations'].attrs['fetch_status'] = 'ok' if contracts else 'not_applicable'
    for frame in bundle.values():
        frame.attrs['tm_bundle_fetched_at'] = max(club.raw_fetched_at for club in snapshot.clubs.values()).isoformat()
    bundle['legacy_players'] = scraper.materialize_legacy_players(bundle['memberships'], bundle['attribute_observations'])
    bundle['attribute_observations'] = bundle['attribute_observations'][bundle['attribute_observations']['club_id'].astype(str).isin(snapshot.changed_club_ids)].copy()
    return bundle


def _retained_roster_stamps(scraper, snapshot, scope):
    retained = sorted(set(snapshot.expected_team_ids) - set(snapshot.changed_club_ids))
    if not retained:
        return {}
    placeholders = ', '.join('?' for _ in retained)
    stamps = {}
    for key, table, club_column in (
        ('memberships', 'transfermarkt_squad_memberships', 'club_id'),
        ('contract_observations', 'transfermarkt_player_contract_observations', 'team_id')):
        try:
            frame = run._query_dataframe(scraper._bronze_connection(),
                f'SELECT {club_column}, player_id, source_body_hash, fetched_at, observed_at '
                f'FROM iceberg.bronze.{table} WHERE competition_id = ? AND edition_id = ? '
                f'AND {club_column} IN ({placeholders})', [scope['competition_id'], scope['edition_id'], *retained])
        except Exception as exc:
            if any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist')):
                continue
            raise CurrentWriteError('retained observation stamp lookup failed') from exc
        for club, player, digest, fetched, observed in frame.itertuples(index=False, name=None):
            if fetched is None or observed is None:
                raise CurrentWriteError('retained observation stamp is unavailable')
            timestamp = pd.Timestamp(observed)
            if timestamp.tzinfo is not None:
                timestamp = timestamp.tz_convert('UTC').tz_localize(None)
            stamps[(key, (str(club), str(player), str(digest), _cell('fetched_at', fetched)))] = timestamp.to_pydatetime()
    return stamps


@_bounded_current_writer
def write_current_roster(scraper, snapshot: FullRosterSnapshot, preflight, cycle_id):
    mode, revision, _ = _authorize(preflight)
    if not isinstance(snapshot, FullRosterSnapshot) or (
        not snapshot.changed_club_ids and getattr(scraper, '_current_membership_changed', False) is not True
    ):
        raise CurrentWriteError('roster write requires complete snapshot with updated clubs')
    for club_id in set(snapshot.clubs) - set(snapshot.changed_club_ids):
        if not snapshot.clubs[club_id].bronze_manifest:
            raise CurrentWriteError('retained club has no committed Bronze baseline')
    scope = _scope(scraper, snapshot)
    frames = _roster_frames(scraper, snapshot, scope, cycle_id)
    empty_clubs = {club for club, value in snapshot.clubs.items() if value.applicability_status == 'authoritative_empty'}
    if set(frames['memberships']['club_id'].astype(str)) | empty_clubs != set(snapshot.expected_team_ids):
        raise CurrentWriteError('materialized memberships do not cover 100% participants')
    if frames['memberships'].empty:
        raise CurrentWriteError('all-empty current roster cannot replace a populated baseline')
    spec = run._spec_for_write_mode(run.ENTITY_SPECS[run.ENTITY_PLAYERS], mode)
    if mode == 'native-only':
        frames.pop('legacy_players')
    if frames['attribute_observations'].empty:
        spec = replace(spec, outputs=tuple(output for output in spec.outputs if output.key != 'attribute_observations'))
        frames.pop('attribute_observations')
    receipt, _ = _commit(scraper, spec, frames, scope, mode, revision, cycle_id)
    receipt.update(business_entity='roster', scope_id=snapshot.scope_id,
        roster_player_ids=sorted({str(row['player_id']) for row in snapshot.rows}),
        full_roster_club_ids=list(snapshot.expected_team_ids), changed_club_ids=list(snapshot.changed_club_ids),
        authoritative_empty_club_ids=sorted(empty_clubs),
        raw_inputs={club: {'raw_capture_id': value.raw_capture_id,
                          'raw_fetched_at': value.raw_fetched_at.isoformat(),
                          'source_url': value.source_url, 'source_body_hash': value.source_body_hash,
                          'applicability_status': value.applicability_status}
                    for club, value in snapshot.clubs.items()},
        retained_baseline_inputs={club: snapshot.clubs[club].bronze_manifest for club in snapshot.expected_team_ids if club not in snapshot.changed_club_ids})
    return receipt


@_bounded_current_writer
def fetch_current_career(scraper, endpoint, ids, scope, preflight, cycle_id, *, decoded_body_soft_stop_bytes):
    mode, revision, _ = _authorize(preflight)
    run._reconcile_pending_fetch_state(scraper)
    resolved = _scope(scraper, scope)
    name = {'market_value_points': run.ENTITY_MV_HISTORY, 'market_value_history': run.ENTITY_MV_HISTORY,
            'transfer_events': run.ENTITY_TRANSFERS, 'transfers': run.ENTITY_TRANSFERS}.get(endpoint)
    if name is None or decoded_body_soft_stop_bytes <= 0:
        raise CurrentWriteError('invalid current career endpoint or soft stop')
    selected = list(dict.fromkeys(str(value) for value in ids))
    if not selected:
        raise CurrentWriteError('current career selection is empty')
    spec = run._spec_for_write_mode(run.ENTITY_SPECS[name], mode)
    from scrapers.transfermarkt.write_intents import pending_intents, save_intent, unpack_frames, finish_intent
    intent_identity = {'kind': 'current', 'scope_id': resolved['scope_id'], 'entity': name}
    pending = pending_intents(intent_identity)
    if pending:
        if len(pending) != 1:
            raise CurrentWriteError('ambiguous durable current career writes; refusing paid I/O')
        path, payload = pending[0]
        evidence = payload['evidence']
        if evidence['mode'] != mode or evidence['revision'] != revision:
            raise CurrentWriteError('career reconciliation requires original writer authority')
        scraper._batch_id = evidence['batch_id']
        frames = unpack_frames(payload['frames'])
        scraper._tm_empty_capture_times = evidence.get('captured_at_by_id', {})
        receipt, _ = _commit(scraper, spec, frames, resolved, mode, revision,
            evidence['cycle_id'], empty_ids=evidence['empty'])
        receipt['checkpoint_status'] = run._commit_checkpoint_or_pending(scraper, spec,
            evidence['processed'], evidence['state_rows'], evidence['cycle_id'],
            resolved['competition_id'], int(resolved['edition_id']), captured_at_by_id=evidence.get('captured_at_by_id'))
        receipt.update(business_entity=spec.state_endpoint, career_window=evidence['window'],
            reconciled_without_http=True, original_capture_attempts=evidence.get('raw_attempts', []),
            original_cache_sources=evidence.get('cache_sources', []), **evidence['window'])
        finish_intent(path)
        return receipt
    pieces = []
    processed = []
    state_rows = []
    empty = []
    admitted_windows = []
    stop_reason = 'window_complete'
    for player in selected:
        decoded = scraper._http_client.get_traffic_stats()['decoded_response_body_bytes']
        if decoded >= decoded_body_soft_stop_bytes:
            stop_reason = 'decoded_body_soft_stop'
            break
        try:
            bundle, authoritative, _ = run._read_frames(scraper, spec, resolved['competition_id'], int(resolved['edition_id']),
                None, 0, [player], mode == 'dual', write_mode=mode,
                decoded_body_soft_stop_bytes=decoded_body_soft_stop_bytes)
        except CurrentPortionDeadlineExceeded:
            stop_reason = 'request_deadline'
            break
        evidence = {}
        if run._apply_career_window(scraper, spec, [player], evidence) != [player]:
            raise CurrentWriteError('full career endpoint admission differs from requested player')
        rows = run._state_rows(scraper, spec, [player], bundle[authoritative])
        if rows[0][0] not in {'success', 'authoritative_empty'}:
            raise CurrentWriteError(f'career {player} lacks successful typed full endpoint')
        processed.append(player)
        admitted_windows.append({'player_id': player, **evidence['career_window']})
        state_rows.extend(rows)
        if rows[0][0] == 'authoritative_empty':
            empty.append(player)
        pieces.append(bundle)
    deferred = selected[len(processed):]
    window = {'requested_player_ids': selected, 'processed_player_ids': processed, 'deferred_player_ids': deferred,
        'decoded_body_soft_stop_bytes': decoded_body_soft_stop_bytes,
        'stop_reason': stop_reason, 'admitted_endpoint_windows': admitted_windows}
    if not processed:
        return {'verified': False, 'bronze_manifest': None, 'committed_at': None,
                'business_entity': spec.state_endpoint, 'career_window': window, **window}
    frames = {output.key: pd.concat([part[output.key] for part in pieces], ignore_index=True) for output in spec.outputs}
    for frame in frames.values():
        if frame.empty and empty:
            frame.attrs['fetch_status'] = 'authoritative_empty'
    scraper._tm_empty_capture_times = run._career_capture_times(scraper, spec, frames, processed)
    intent_path = save_intent(intent_identity, frames, mode=mode, revision=revision,
        batch_id=str(scraper._batch_id), cycle_id=cycle_id, empty=empty,
        processed=processed, state_rows=state_rows, window=window,
        captured_at_by_id=run._career_capture_times(scraper, spec, frames, processed),
        raw_attempts=list(scraper.get_raw_attempt_records()), cache_sources=list(scraper.get_cache_source_records()))
    receipt, _ = _commit(scraper, spec, frames, resolved, mode, revision, cycle_id, empty_ids=empty)
    checkpoint_status = run._commit_checkpoint_or_pending(scraper, spec, processed, state_rows, cycle_id,
        resolved['competition_id'], int(resolved['edition_id']),
        captured_at_by_id=run._career_capture_times(scraper, spec, frames, processed))
    receipt['checkpoint_status'] = checkpoint_status
    receipt.update(business_entity=spec.state_endpoint, career_window=window, **window)
    finish_intent(intent_path)
    return receipt


@_bounded_current_writer
def fetch_current_coaches(scraper, scope, preflight, cycle_id, *, clubs=None):
    mode, revision, _ = _authorize(preflight)
    resolved = _scope(scraper, scope)
    memberships = run._load_coach_memberships(scraper, resolved['compatibility_league'], int(resolved['edition_id']))
    if clubs is not None:
        club_ids = [str(club['club_id']) if isinstance(club, Mapping) else str(club) for club in clubs]
        memberships = memberships[memberships['club_id'].astype(str).isin(club_ids)]
    ttl = int(os.environ.get('TM_COACH_HISTORY_TTL_DAYS', '28'))
    if ttl <= 0:
        raise CurrentWriteError('coach history TTL must remain positive')
    selected_memberships, selected, cached, _, _ = run._select_coach_memberships(
        scraper, memberships, resolved['competition_id'], int(resolved['edition_id']), 'current', cycle_id, ttl, False)
    spec = run._spec_for_write_mode(run.ENTITY_SPECS[run.ENTITY_COACHES], mode)
    receipts, processed = [], []
    cached_receipts = _cached_coach_receipts(scraper, cached, resolved, mode, ttl)
    for club in selected:
        try:
            bundle = scraper.read_coach_data(resolved['competition_id'], int(resolved['edition_id']),
                memberships=selected_memberships[selected_memberships['club_id'].astype(str) == club])
        except CurrentPortionDeadlineExceeded:
            break
        if mode == 'native-only':
            bundle.pop('legacy_coaches', None)
        checkpoint = run.COACH_HISTORY_CHECKPOINT_SPEC
        rows = run._state_rows(scraper, checkpoint, [club], bundle['stints'])
        if rows[0][0] not in {'success', 'authoritative_empty'}:
            raise CurrentWriteError(f'coach history {club} is not a complete typed endpoint')
        empty = [club] if rows[0][0] == 'authoritative_empty' else []
        if empty:
            for frame in bundle.values():
                if frame.empty:
                    frame.attrs['fetch_status'] = 'authoritative_empty'
        receipt, _ = _commit(scraper, spec, bundle, resolved, mode, revision, cycle_id,
                              empty_ids=empty, empty_key='club_id')
        if not run._persist_fetch_state(scraper, checkpoint, [club], rows, cycle_id):
            raise CurrentWriteError('coach history fetch-state commit failed')
        receipts.append(receipt)
        processed.append(club)
    all_receipts = cached_receipts + receipts
    return {'verified': bool(all_receipts), 'business_entity': 'coaches', 'receipts': receipts,
            'cached_receipts': cached_receipts,
            'processed_club_ids': processed, 'deferred_club_ids': selected[len(processed):],
            'cached_club_ids': cached, 'coach_history_ttl_days': ttl,
            'bronze_manifest': hashlib.sha256(json.dumps(all_receipts, sort_keys=True).encode()).hexdigest() if all_receipts else None,
            'committed_at': (datetime.now(timezone.utc).isoformat() if receipts
                             else max((item['committed_at'] for item in cached_receipts), default=None))}


def _cached_coach_receipts(scraper, clubs, scope, mode, ttl):
    """Qualify retained coach business rows without assigning them fresh stamps."""
    if not clubs:
        return []
    state = run._load_fetch_state(scraper, 'coach_history', strict=True)
    derived = run._load_data_derived_state(scraper, 'coach_history', scope['competition_id'], int(scope['edition_id']),
                                          [club for club in clubs if club not in state])
    state.update(derived)
    receipts = []
    cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=ttl)
    conn = scraper._bronze_connection()
    for club in clubs:
        prior = state.get(club, {})
        stamp = run._as_utc(prior.get('last_success_at'))
        if prior.get('status') not in {'success', 'authoritative_empty', 'valid_empty'} or stamp is None or stamp <= cutoff:
            raise CurrentWriteError('cached coach history lacks an eligible committed TTL receipt')
        try:
            stints = run._query_dataframe(conn, 'SELECT ' + ', '.join(tm.COACH_STINT_COLUMNS + tm._METADATA_COLUMNS)
                + ' FROM iceberg.bronze.transfermarkt_coach_stints WHERE club_id = ?', [club])
        except Exception as exc:
            if not _missing_table(exc) or prior.get('status') not in {'authoritative_empty', 'valid_empty'}:
                raise CurrentWriteError('cached coach history is unavailable') from exc
            stints = pd.DataFrame(columns=tm.COACH_STINT_COLUMNS + tm._METADATA_COLUMNS)
        if stints.empty:
            if prior.get('status') not in {'authoritative_empty', 'valid_empty'}:
                raise CurrentWriteError('cached nonempty coach history lost its physical rows')
            profiles = pd.DataFrame(columns=tm.COACH_PROFILE_COLUMNS + tm._METADATA_COLUMNS)
        else:
            ids = sorted(set(stints['coach_id'].astype(str)))
            window = tm._season_window(scope['season_year'], scope['season_format'])
            required = {str(row['coach_id']) for row in stints.to_dict('records') if tm._stint_overlaps_season(row, *window)}
            try:
                profiles = run._query_dataframe(conn, 'SELECT ' + ', '.join(tm.COACH_PROFILE_COLUMNS + tm._METADATA_COLUMNS)
                    + ' FROM iceberg.bronze.transfermarkt_coach_profiles WHERE coach_id IN (' + ', '.join('?' for _ in ids) + ')', ids)
            except Exception as exc:
                if not _missing_table(exc) or required:
                    raise CurrentWriteError('cached season coach profiles are unavailable') from exc
                profiles = pd.DataFrame(columns=tm.COACH_PROFILE_COLUMNS + tm._METADATA_COLUMNS)
            if not required <= set(profiles['coach_id'].astype(str)):
                raise CurrentWriteError('cached season coach profile identities are incomplete')
        legacy = scraper.materialize_legacy_coaches(profiles, stints, scope['compatibility_league'],
                                                   scope['season_year'], scope['season_format'])
        if mode == 'dual':
            try:
                actual = run._query_dataframe(conn, 'SELECT ' + ', '.join(tm.LEGACY_COACH_COLUMNS + tm._METADATA_COLUMNS)
                    + ' FROM iceberg.bronze.transfermarkt_coaches WHERE league = ? AND season = ? AND current_club_id = ?',
                    [scope['compatibility_league'], scope['canonical_season'], club])
            except Exception as exc:
                if not _missing_table(exc) or not legacy.empty:
                    raise CurrentWriteError('cached legacy coach business is unavailable') from exc
                actual = legacy
            contract = run._MANIFEST_COMPATIBILITY['stints']['legacy']
            if run._compatibility_fingerprint(actual, contract) != run._compatibility_fingerprint(legacy, contract):
                raise CurrentWriteError('cached coach legacy business parity changed')
        proof = {key: {'rows': len(frame), 'physical_hash': hashlib.sha256(json.dumps(_rows(frame)).encode()).hexdigest()}
                 for key, frame in {'profiles': profiles, 'stints': stints}.items()}
        receipts.append({'verified': True, 'cached': True, 'club_id': club, 'committed_at': stamp.isoformat(),
                         'retained_baseline_inputs': {'run_key': prior.get('run_key'), 'status': prior['status']},
                         'outputs': proof, 'bronze_manifest': hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()})
    return receipts


def _missing_table(exc):
    return any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist'))


def _verify_manifest_receipt(scraper, receipt):
    if receipt.get('verified') is not True or not receipt.get('cycle_id') or not receipt.get('manifests'):
        raise CurrentWriteError('retained business receipt is incomplete')
    computed = hashlib.sha256(json.dumps({'cycle_id': receipt['cycle_id'], 'outputs': receipt['outputs'],
        'manifests': receipt['manifests']}, sort_keys=True).encode()).hexdigest()
    if computed != receipt.get('bronze_manifest'):
        raise CurrentWriteError('retained business receipt checksum differs')
    for manifest in receipt['manifests']:
        if manifest.get('status') != 'success' or not manifest.get('rows'):
            raise CurrentWriteError('retained compatibility manifest was not successful')
        table = run.NATIVE_WRITE_MANIFEST_TABLE if receipt['write_mode'] == 'native-only' else run.DUAL_WRITE_MANIFEST_TABLE
        for row in manifest.get('rows', []):
            if row.get('status') != 'success':
                raise CurrentWriteError('retained compatibility output was not successful')
            columns = ['native_table', 'native_batch_id', 'native_rows', 'native_hash', 'status']
            columns += (['writer_revision', 'write_mode'] if receipt['write_mode'] == 'native-only'
                        else ['legacy_table', 'legacy_batch_id', 'legacy_rows', 'legacy_hash'])
            expected = [row.get(column, receipt.get(column)) for column in columns]
            actual = run._execute_cursor(scraper._bronze_connection(),
                f'SELECT {", ".join(columns)} FROM {table} WHERE cycle_id = ? AND league = ? AND season = ? AND entity = ?',
                [manifest['cycle_id'], manifest['league'], manifest['season'], row['entity']], fetch=True)
            if len(actual or []) != 1 or tuple(_cell(column, value) for column, value in zip(columns, actual[0])) != tuple(
                    _cell(column, value) for column, value in zip(columns, expected)):
                raise CurrentWriteError('retained native/dual manifest identity differs from ops')


def verify_current_roster_baseline(scraper, snapshot: FullRosterSnapshot, receipts, preflight):
    """Read-only qualification of a recovery JSON roster against current Bronze.

    Full replacement outputs bind to the latest full-roster receipt; attributes
    bind to each club's own latest successful capture receipt. Raw bytes must
    additionally be replayed and checked by the recovery caller before use.
    """
    mode, revision, _ = _authorize(preflight)
    scope = _scope(scraper, snapshot)
    if not isinstance(receipts, (list, tuple)) or not receipts:
        raise CurrentWriteError('retained roster has no structured business receipts')
    eligible = [receipt for receipt in receipts if receipt.get('business_entity') == 'roster'
                and receipt.get('scope_id') == snapshot.scope_id]
    if not eligible:
        raise CurrentWriteError('retained roster receipt belongs to another scope')
    latest = max(eligible, key=lambda receipt: pd.Timestamp(receipt['committed_at']))
    if latest.get('write_mode') != mode or latest.get('writer_revision') != revision:
        raise CurrentWriteError('retained roster writer qualification differs from preflight')
    _verify_manifest_receipt(scraper, latest)
    expected = _roster_frames(scraper, replace(snapshot, changed_club_ids=snapshot.expected_team_ids), scope, latest['cycle_id'])
    spec = run._spec_for_write_mode(run.ENTITY_SPECS[run.ENTITY_PLAYERS], mode)
    conn = scraper._bronze_connection()
    verified = {}
    for output in spec.outputs:
        if output.key == 'attribute_observations':
            continue
        frame = expected[output.key]
        proof = latest['outputs'].get(output.key)
        if not isinstance(proof, Mapping) or not proof.get('physical_hash'):
            raise CurrentWriteError('retained full-roster output lacks physical receipt')
        if proof.get('table') != 'iceberg.bronze.' + output.table_name:
            raise CurrentWriteError('retained physical receipt table is outside the roster contract')
        if output.is_legacy:
            predicate, params = 'league = ? AND season = ?', [scope['compatibility_league'], scope['canonical_season']]
        else:
            predicate, params = 'competition_id = ? AND edition_id = ?', [scope['competition_id'], scope['edition_id']]
        try:
            physical = run._query_dataframe(conn, f'SELECT {", ".join(frame.columns)} FROM {proof["table"]} WHERE {predicate}', params)
        except Exception as exc:
            if frame.empty and any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist')):
                physical = frame
            else:
                raise CurrentWriteError('retained full-roster output is unavailable') from exc
        digest = hashlib.sha256(json.dumps(_rows(physical), sort_keys=True).encode()).hexdigest()
        if len(physical) != proof['rows'] or digest != proof['physical_hash']:
            raise CurrentWriteError('retained full-roster physical checksum differs')
        columns = [column for column in frame.columns if column not in {'observed_at', '_ingested_at', '_batch_id'}]
        if _rows(physical[columns]) != _rows(frame[columns]):
            raise CurrentWriteError('recovery roster business/lineage differs from committed physical rows')
        verified[output.key] = dict(proof)
    attribute_columns = tm.PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS + tm._METADATA_COLUMNS
    for club_id, club in snapshot.clubs.items():
        matches = [receipt for receipt in eligible if receipt.get('bronze_manifest') == club.bronze_manifest]
        if len(matches) != 1:
            raise CurrentWriteError('retained club does not identify exactly one committed receipt')
        receipt = matches[0]
        _verify_manifest_receipt(scraper, receipt)
        if not club.rows:
            if club.applicability_status != 'authoritative_empty':
                raise CurrentWriteError('empty retained club lacks typed authority')
            continue
        proof = receipt['outputs'].get('attribute_observations')
        club_proof = proof.get('club_hashes', {}).get(club_id) if isinstance(proof, Mapping) else None
        if not isinstance(club_proof, Mapping):
            raise CurrentWriteError('retained club lacks physical attribute slice receipt')
        if proof.get('table') != 'iceberg.bronze.transfermarkt_player_attribute_observations':
            raise CurrentWriteError('retained attribute receipt table is outside the roster contract')
        players = [str(row['player_id']) for row in club.rows]
        placeholders = ', '.join('?' for _ in players)
        physical = run._query_dataframe(conn,
            f'SELECT {", ".join(attribute_columns)} FROM (SELECT {", ".join(attribute_columns)}, '
            'ROW_NUMBER() OVER (PARTITION BY competition_id, edition_id, club_id, player_id '
            'ORDER BY observed_at DESC, _ingested_at DESC, _batch_id DESC) AS rn '
            f'FROM {proof["table"]} WHERE competition_id = ? AND edition_id = ? AND club_id = ? '
            f'AND player_id IN ({placeholders})) WHERE rn = 1',
            [scope['competition_id'], scope['edition_id'], club_id, *players])
        digest = hashlib.sha256(json.dumps(_rows(physical), sort_keys=True).encode()).hexdigest()
        if len(physical) != club_proof['rows'] or digest != club_proof['physical_hash']:
            raise CurrentWriteError('retained club attributes physical checksum differs')
        club_expected = expected['attribute_observations'][expected['attribute_observations']['club_id'].astype(str) == club_id].copy()
        club_expected['cycle_id'] = receipt['cycle_id']
        columns = [column for column in attribute_columns if column not in {'observed_at', '_ingested_at', '_batch_id'}]
        if _rows(physical[columns]) != _rows(club_expected[columns]):
            raise CurrentWriteError('retained club attributes business/lineage differs')
    return {'verified': True, 'business_entity': 'retained_roster', 'scope_id': snapshot.scope_id,
            'bronze_manifest': latest['bronze_manifest'], 'committed_at': latest['committed_at'],
            'outputs': verified, 'retained_club_ids': list(snapshot.expected_team_ids)}


def verify_current_complete_scope(scraper, scope_manifest, entry, preflight):
    """Qualify an older complete seven-entity scope without collecting it again.

    ``entry.scope_sources`` links the four original parser result files by
    absolute path and SHA-256. Immutable ops scope/write manifests and the
    existing control-plane live Bronze fingerprints remain the authority.
    """
    from types import SimpleNamespace
    from dags.scripts import run_transfermarkt_scope_cycle as cycle
    from dags.utils import transfermarkt_native_v2 as control
    from dags.utils.transfermarkt_scope_state import ScopeManifest, SCOPE_MANIFEST_TABLE

    mode, revision, _ = _authorize(preflight)
    manifest = scope_manifest if isinstance(scope_manifest, ScopeManifest) else ScopeManifest.from_mapping(scope_manifest)
    manifest.validate(cycle.EXPECTED_ENTITIES)
    if manifest.dq_evidence.get('edition_current') is not True or manifest.dq_evidence.get('career_fetches_pending') != 0:
        raise CurrentWriteError('baseline_required: scope is historical or has unfinished careers')
    if manifest.reader_revision > revision:
        raise CurrentWriteError('baseline_required: complete scope reader revision is newer than qualified state')
    declared = ScopeManifest.from_mapping(entry['scope_manifest'])
    if declared.digest != manifest.digest:
        raise CurrentWriteError('baseline_required: complete scope bundle digest differs')
    snapshot = FullRosterSnapshot.from_mapping(entry['snapshot'])
    if snapshot.scope_id != manifest.scope_id:
        raise CurrentWriteError('baseline_required: full roster belongs to another complete scope')
    resolved = _scope(scraper, snapshot)
    if resolved['canonical_season'] != manifest.canonical_season or resolved['compatibility_league'] != manifest.canonical_competition_id:
        raise CurrentWriteError('baseline_required: scope season/competition differs from source registry')
    sources = entry.get('scope_sources')
    if not isinstance(sources, Mapping) or set(sources) != set(cycle.ENTITY_ORDER):
        raise CurrentWriteError('baseline_required: four exact original parser result receipts are required')
    evidence = {item.entity: item for item in manifest.entities}
    linked = {}
    from pathlib import Path
    import re
    for entity in cycle.ENTITY_ORDER:
        source = sources[entity]
        if not isinstance(source, Mapping) or set(source) != {'result_path', 'result_sha256'}:
            raise CurrentWriteError('baseline_required: parser source receipt fields differ')
        path = Path(str(source['result_path']))
        if not path.is_absolute() or not path.is_file() or not re.fullmatch('[a-f0-9]{64}', str(source['result_sha256'])):
            raise CurrentWriteError('baseline_required: original parser result file/hash is unavailable')
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != source['result_sha256']:
            raise CurrentWriteError('baseline_required: original parser result file changed')
        result = json.loads(body)
        cycle._validate_result_identity(result, SimpleNamespace(**manifest.as_dict()), entity)
        if result.get('write_mode') != mode:
            raise CurrentWriteError('baseline_required: parser writer mode differs from qualified state')
        source_manifest = result.get('batch_manifest' if mode == 'dual' else 'native_write_manifest', {})
        if source_manifest.get('cycle_id') != manifest.child_cycle_id:
            raise CurrentWriteError('baseline_required: parser manifest belongs to another child cycle')
        rows = cycle._manifest_rows(result, mode)
        if set(rows) != {name for _, name in cycle.ENTITY_OUTPUTS[entity]}:
            raise CurrentWriteError('baseline_required: original parser write output set differs')
        for key, name in cycle.ENTITY_OUTPUTS[entity]:
            committed = rows[name]
            expected = evidence[name]
            output = result.get('outputs', {}).get(key, {})
            if (committed.get('status') != 'success' or committed.get('native_hash') != expected.key_hash
                or expected.content_hash != expected.key_hash or committed.get('native_rows') != expected.dedup_rows
                or output.get('rows') != expected.raw_rows or output.get('table') != cycle.ENTITY_TABLES[name]):
                raise CurrentWriteError('baseline_required: parser receipt differs from complete entity evidence')
        linked[entity] = dict(source)
    columns = ['parent_cycle_id', 'child_cycle_id', 'scope_id', 'competition_id', 'edition_id',
        'canonical_competition_id', 'canonical_season', 'registry_snapshot_id', 'capture_revision',
        'parser_revision', 'schema_revision', 'reader_revision', 'entity_manifest_json',
        'manifest_digest', 'status', 'committed_at']
    connection = scraper._bronze_connection()
    rows = run._execute_cursor(connection, f'SELECT {", ".join(columns)} FROM {SCOPE_MANIFEST_TABLE} '
        'WHERE parent_cycle_id = ? AND child_cycle_id = ? AND scope_id = ? AND manifest_digest = ?',
        [manifest.parent_cycle_id, manifest.child_cycle_id, manifest.scope_id, manifest.digest], fetch=True)
    if len(rows or []) != 1:
        raise CurrentWriteError('baseline_required: exact complete scope ops row is absent or ambiguous')
    stored = dict(zip(columns, rows[0]))
    payload = json.loads(stored.pop('entity_manifest_json'))
    committed_at = run._as_utc(stored.pop('committed_at'))
    if stored.pop('status') != 'complete' or stored.pop('manifest_digest') != manifest.digest or committed_at is None:
        raise CurrentWriteError('baseline_required: complete scope ops receipt is invalid')
    actual = ScopeManifest.from_mapping({**stored, **payload})
    actual.validate(cycle.EXPECTED_ENTITIES)
    if actual.digest != manifest.digest:
        raise CurrentWriteError('baseline_required: complete scope ops payload differs')
    cursor = connection.cursor()
    try:
        report = control._scope_write_manifest_report(cursor, manifests=(manifest,), expected_revision=revision,
            write_mode=mode, require_fresh=False)
    finally:
        cursor.close()
    if report.get('passed') is not True or report.get('expected_rows') != len(cycle.EXPECTED_ENTITIES):
        raise CurrentWriteError('baseline_required: complete scope physical Bronze qualification failed')
    outputs = {name: report['rows'][manifest.child_cycle_id + ':' + name] for name in cycle.EXPECTED_ENTITIES}
    return {'verified': True, 'bronze_manifest': manifest.digest, 'committed_at': committed_at.isoformat(),
            'scope_id': manifest.scope_id, 'business_entity': 'complete_scope_baseline',
            'outputs': outputs, 'scope_sources': linked, 'original_reader_revision': manifest.reader_revision,
            'writer_revision': revision, 'write_mode': mode}
