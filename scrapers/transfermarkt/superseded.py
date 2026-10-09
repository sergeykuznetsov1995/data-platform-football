"""Terminal failure of a partial career job, with verified newer capture reuse."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import pandas as pd

from scrapers.transfermarkt import career_refs, write_intents
from scrapers.transfermarkt.writer import execute_statement, writer_lock, StaleTransfermarktWrite


def _hash(frame):
    from dags.utils.transfermarkt_current_write import _rows
    return hashlib.sha256(json.dumps(_rows(frame), sort_keys=True).encode()).hexdigest()


def _raw_player(scraper, journal, frame, player, entity):
    """Validate original successful raw envelope/body and full parser output."""
    from scrapers.transfermarkt.scraper import _parse_mv_history, _parse_transfers, PARSER_REVISION, SCHEMA_REVISION, MARKET_VALUE_POINT_COLUMNS, TRANSFER_EVENT_COLUMNS, _SCOPE_LINEAGE_COLUMNS, _apply_nullable_dtypes
    from scrapers.transfermarkt.models import stable_payload_hash
    from dags.utils.transfermarkt_current_write import _rows
    evidence = journal['evidence']
    selected = evidence.get('processed', evidence.get('checkpoint_ids', []))
    states = dict(zip(selected, evidence['state_rows'], strict=True))
    state = states.get(player)
    if state is None or state[0] not in {'success', 'authoritative_empty', 'valid_empty'}:
        raise RuntimeError('supersession source has no successful full endpoint')
    path = '/ceapi/marketValueDevelopment/graph/' if entity == 'market_value_points' else '/ceapi/transferHistory/list/'
    source = scraper._http_client._raw_store
    stamps = evidence['captured_at_by_id']
    records = evidence.get('raw_attempts', []) + evidence.get('cache_sources', [])
    matches = [item for item in records if str(item.get('url', '')).split('?', 1)[0].endswith(path + player)
               and item.get('outcome_kind') == 'response' and item.get('status_code') == 200]
    for item in matches:
        envelope = source.verify_attempt_envelope(item['envelope_id'])
        if asdict(envelope) != item:
            raise RuntimeError('supersession raw envelope differs from immutable journal')
        body, capture = source.load_capture(envelope.capture_id)
        if (capture.url != item['url'] or capture.content_hash != item['raw_body_hash']
            or pd.to_datetime(capture.fetched_at, utc=True) != pd.to_datetime(stamps[player], utc=True)):
            continue
        payload = json.loads(body)
        key = 'list' if entity == 'market_value_points' else 'transfers'
        if not isinstance(payload, dict) or not isinstance(payload.get(key), list):
            raise RuntimeError('supersession raw body is not the full typed endpoint')
        parsed = (_parse_mv_history if entity == 'market_value_points' else _parse_transfers)(payload, player)
        if state[0] in {'authoritative_empty', 'valid_empty'}:
            if parsed or payload[key] or state[1] != 0 or not frame.empty:
                raise RuntimeError('supersession typed empty disagrees with raw or physical rows')
        else:
            if len(parsed) != int(state[1]) or len(parsed) != len(frame) or not parsed:
                raise RuntimeError('supersession full raw row count differs')
            columns = [column for column in (MARKET_VALUE_POINT_COLUMNS if entity == 'market_value_points' else TRANSFER_EVENT_COLUMNS) if column not in _SCOPE_LINEAGE_COLUMNS]
            projected = _apply_nullable_dtypes(pd.DataFrame(parsed).reindex(columns=columns))
            if not set(projected.columns) <= set(frame.columns) or _rows(projected) != _rows(frame[list(projected.columns)]):
                raise RuntimeError('supersession full raw business fields differ')
            if (set(frame.source_url.astype(str)) != {capture.url}
                or set(frame.source_body_hash.astype(str)) != {stable_payload_hash(payload)}
                or any(pd.to_datetime(frame.fetched_at, utc=True) < pd.to_datetime(capture.fetched_at, utc=True))
                or set(frame.parser_revision.astype(str)) != {PARSER_REVISION}
                or set(frame.schema_revision.astype(str)) != {SCHEMA_REVISION}):
                raise RuntimeError('supersession raw lineage differs from physical capture')
        return {'capture_id': capture.capture_id, 'body_hash': capture.content_hash,
                'fetched_at': capture.fetched_at, 'url': capture.url, 'envelope': item}
    raise RuntimeError('supersession has no exact original successful raw capture')


def attest_career_manifest(scraper, receipt):
    """Bind the real original writer namespace readback to its capture."""
    from dags.scripts import run_transfermarkt_scraper as run
    manifest, = receipt['manifests']
    row, = manifest['rows']
    fields = ('native_table', 'native_batch_id', 'native_rows', 'native_hash', 'writer_revision', 'write_mode', 'status')
    mode = receipt['write_mode']
    table = run.NATIVE_WRITE_MANIFEST_TABLE
    if mode == 'dual':
        fields = ('native_table', 'legacy_table', 'native_batch_id', 'legacy_batch_id',
            'native_rows', 'legacy_rows', 'native_hash', 'legacy_hash', 'status')
        table = run.DUAL_WRITE_MANIFEST_TABLE
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        execute_statement(cursor, 'SELECT ' + ', '.join(fields) + f' FROM {table} '
            'WHERE cycle_id=? AND entity=? AND league=? AND season=?',
            (receipt['manifest_cycle_id'], row['entity'], manifest['league'], manifest['season']))
        actual = cursor.fetchall()
        if actual != [tuple(row[field] for field in fields)] or row['status'] != 'success':
            raise RuntimeError('career archive has no genuine original manifest readback')
        proof = {'cycle_id': receipt['manifest_cycle_id'], 'entity': row['entity'], 'write_mode': mode,
            'writer_revision': receipt['writer_revision'],
            'league': manifest['league'], 'season': manifest['season'], 'fields': list(fields), 'row': list(actual[0])}
        body = json.dumps(proof, sort_keys=True, separators=(',', ':'))
        receipt['native_manifest_attestation' if mode == 'native-only' else 'dual_manifest_attestation'] = {'proof': proof, 'sha256': hashlib.sha256(body.encode()).hexdigest()}
    finally:
        cursor.close()
        connection.close()


def verify_complete(scraper, spec, archived, *, players, live=True, allow_later_manifest=True):
    """Reprove real successful dual manifest, both physical sides and raw."""
    from dags.scripts import run_transfermarkt_scraper as run
    from dags.utils.transfermarkt_current_write import _read_back
    receipt, journal = archived['receipt'], archived['journal']
    mode = receipt.get('write_mode')
    if receipt.get('verified') is not True or mode not in {'dual', 'native-only'} or journal['evidence']['mode'] != mode:
        raise RuntimeError('supersession requires a complete successful captured writer mode')
    spec = run._spec_for_write_mode(run.ENTITY_SPECS[spec.name], mode)
    frames = write_intents.unpack_frames(archived['frames'])
    native = next(output for output in spec.outputs if not output.is_legacy)
    manifests = [row for manifest in receipt['manifests'] for row in manifest['rows'] if row['entity'] == native.key]
    if len(manifests) != 1:
        raise RuntimeError('supersession dual receipt identity differs')
    row = manifests[0]
    fields = ('native_table', 'legacy_table', 'native_batch_id', 'legacy_batch_id',
              'native_rows', 'legacy_rows', 'native_hash', 'legacy_hash', 'status')
    manifest_table = run.DUAL_WRITE_MANIFEST_TABLE
    if mode == 'native-only':
        fields = ('native_table', 'native_batch_id', 'native_rows', 'native_hash', 'writer_revision', 'write_mode', 'status')
        manifest_table = run.NATIVE_WRITE_MANIFEST_TABLE
    expected_attestation = {'cycle_id': receipt['manifest_cycle_id'], 'entity': row['entity'], 'write_mode': mode,
        'writer_revision': receipt['writer_revision'],
        'league': receipt['manifests'][0]['league'], 'season': receipt['manifests'][0]['season'],
        'fields': list(fields), 'row': [row[field] for field in fields]}
    attested = receipt.get('native_manifest_attestation' if mode == 'native-only' else 'dual_manifest_attestation', {})
    body = json.dumps(expected_attestation, sort_keys=True, separators=(',', ':'))
    if attested.get('proof') != expected_attestation or attested.get('sha256') != hashlib.sha256(body.encode()).hexdigest():
        raise RuntimeError('original career manifest attestation differs or is missing')
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        execute_statement(cursor, 'SELECT ' + ', '.join(fields) + f' FROM {manifest_table} '
            'WHERE cycle_id=? AND entity=? AND league=? AND season=?',
            (receipt['manifest_cycle_id'], native.key, receipt['manifests'][0]['league'], receipt['manifests'][0]['season']))
        actual_manifest = cursor.fetchall()
        if actual_manifest != [tuple(row[field] for field in fields)]:
            later = None
            if allow_later_manifest and len(actual_manifest) == 1:
                actual = dict(zip(fields, actual_manifest[0], strict=True))
                if (actual['status'] == 'success'
                    and (mode == 'dual' or actual['write_mode'] == mode and actual['writer_revision'] == receipt['writer_revision'])
                    and actual['native_table'] == row['native_table']
                    and (mode != 'dual' or actual['legacy_table'] == row['legacy_table'])
                    and actual['native_batch_id'] != row['native_batch_id']):
                    later = write_intents.completed_capture(receipt['manifest_cycle_id'], native.key, actual['native_batch_id'])
            if later is None:
                raise RuntimeError('supersession lacks its genuine successful ' + ('dual' if mode == 'dual' else 'native-only') + ' manifest')
            later_receipt = later['receipt']
            later_row, = later_receipt['manifests'][0]['rows']
            if (later_receipt['write_mode'] != mode or later_receipt['writer_revision'] != receipt['writer_revision']
                or later_receipt['manifest_cycle_id'] != receipt['manifest_cycle_id']
                or later_receipt['manifests'][0]['league'] != receipt['manifests'][0]['league']
                or later_receipt['manifests'][0]['season'] != receipt['manifests'][0]['season']
                or tuple(later_row[field] for field in fields) != actual_manifest[0]):
                raise RuntimeError('later native-only complete archive differs from actual manifest')
            later_evidence = later['journal']['evidence']
            verify_complete(scraper, spec, later,
                players=later_evidence.get('processed', later_evidence.get('checkpoint_ids', [])),
                live=False, allow_later_manifest=False)
        if row['status'] != 'success':
            raise RuntimeError('supersession original capture manifest is unsuccessful')
        capture = career_refs.physical_predicate(cursor, receipt['manifest_cycle_id'], native.key,
            native.table_name, batch_id=row['native_batch_id'])
        if capture is None or capture.refs != row['physical_refs']:
            raise RuntimeError('supersession physical refs differ')
        if not set(players) <= {player for player, _, _ in capture.refs}:
            raise RuntimeError('supersession does not cover every retired player')
        empty = journal['evidence'].get('empty', [])
        for output in spec.outputs:
            frame = frames[output.key]
            snapshot = capture.snapshot_id if output == native else capture.legacy_snapshot_id
            def native_readback(selected_players, *, snapshot_id=None):
                expected = frame[frame.player_id.astype(str).isin(selected_players)]
                refs = [item for item in capture.refs if item[0] in selected_players]
                expected_pairs = sorted([str(player), str(batch), len(part)] for (player, batch), part
                    in expected.groupby(['player_id', '_batch_id'], dropna=False))
                if expected_pairs != sorted(item for item in refs if item[2] > 0):
                    raise RuntimeError('supersession native frames differ from exact original player/batch refs')
                terms = [f'(player_id={career_refs._q(player)} AND _batch_id={career_refs._q(batch)})'
                    for player, batch, count in refs if count > 0]
                terms += [f'player_id={career_refs._q(player)}' for player, _, count in refs if count == 0]
                if snapshot_id is None:
                    # A live full career includes every current row for these
                    # players, so an extra/different batch cannot be hidden.
                    terms = [f'player_id={career_refs._q(player)}' for player in selected_players]
                relation = 'iceberg.bronze.' + output.table_name
                if snapshot_id is not None:
                    relation += f' FOR VERSION AS OF {snapshot_id}'
                try:
                    execute_statement(cursor, 'SELECT ' + ', '.join(frame.columns) + ' FROM ' + relation
                        + ' WHERE (' + ' OR '.join(terms) + ')')
                    actual = pd.DataFrame(cursor.fetchall(), columns=frame.columns)
                except Exception as exc:
                    if not expected.empty or not any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                        raise
                    actual = expected
                if _hash(actual) != _hash(expected) or len(actual) != len(expected):
                    from dags.utils.transfermarkt_current_write import CurrentWriteError
                    raise CurrentWriteError('supersession native exact player/batch business and lineage differ')
                return {'rows': len(actual), 'physical_hash': _hash(actual)}
            if snapshot is None:
                if not frame.empty:
                    raise RuntimeError('supersession nonempty capture lacks its snapshot')
                anchors = write_intents.read_snapshot_anchors(Path(write_intents._root()) / (archived['intent_sha256'] + '.json'))
                if output.table_name not in anchors or anchors[output.table_name] is not None:
                    raise RuntimeError('supersession absent table lacks original boundary')
                pinned = {'rows': 0, 'physical_hash': hashlib.sha256(b'[]').hexdigest()}
            elif output == native:
                pinned = native_readback([item[0] for item in capture.refs], snapshot_id=snapshot)
            else:
                pinned = _read_back(scraper, output, frame, receipt['cycle_id'], empty_ids=empty, snapshot_id=snapshot)
            if pinned['rows'] != receipt['outputs'][output.key]['rows'] or pinned['physical_hash'] != receipt['outputs'][output.key]['physical_hash']:
                raise RuntimeError('supersession pinned physical count/hash differs')
            if live:
                # The original archived bundle remains fully pinned and proved.
                # Other players in it may have genuine later captures; their
                # replacement must not invalidate the selected players' proof.
                selected = frame[frame.player_id.astype(str).isin(players)]
                selected_empty = [player for player in empty if player in players]
                try:
                    actual = (native_readback(players) if output == native else
                        _read_back(scraper, output, selected, receipt['cycle_id'],
                            empty_ids=selected_empty, player_ids=players))
                except Exception as exc:
                    from dags.utils.transfermarkt_current_write import CurrentWriteError
                    if isinstance(exc, CurrentWriteError):
                        raise StaleTransfermarktWrite('superseding physical capture is no longer current') from exc
                    raise
                if actual['rows'] != len(selected) or actual['physical_hash'] != _hash(selected):
                    raise StaleTransfermarktWrite('supersession is no longer the complete current physical capture')
        raw = {player: _raw_player(scraper, journal, frames[native.key][frames[native.key].player_id.astype(str) == player], player, native.key)
               for player in players}
        return {'archive': archived, 'frames': frames, 'raw': raw, 'capture': capture,
                'unit': {'cycle_id': receipt['manifest_cycle_id'], 'entity': native.key,
                         'native_batch_id': row['native_batch_id'], 'intent_sha256': archived['intent_sha256']}}
    finally:
        cursor.close()
        connection.close()


def retire_partial(scraper, spec, path, payload, frames, *, delivery_cycle):
    """Failure receipt only; cannot acknowledge the original incomplete dual job."""
    native = next(output for output in spec.outputs if not output.is_legacy)
    legacy = next(output for output in spec.outputs if output.is_legacy)
    evidence = payload['evidence']
    if evidence['mode'] != 'dual':
        return None
    players = list(evidence['processed'])
    original = {key: frame.assign(_batch_id=str(scraper._batch_id)) for key, frame in frames.items()}
    refs = original[native.key].attrs.get('tm_original_capture_refs', [])
    if refs:
        original[native.key]['_batch_id'] = original[native.key].player_id.astype(str).map({item['player_id']: item['batch_id'] for item in refs})
    with writer_lock():
        connection = scraper._bronze_connection()
        try:
            committed = None
            if frames[native.key].empty:
                committed = write_intents.read_empty_commit(path, native.table_name, evidence['empty'])
                if committed is None:
                    return None
            partial = career_refs.recover_bundle_snapshots(connection, evidence['cycle_id'], spec.outputs,
                original, path, batch_id=str(scraper._batch_id), partial=True)
            if committed is not None:
                if partial.get(native.key) != (committed['snapshot_id'] or 0):
                    return None
            if native.key not in partial or legacy.key in partial:
                return None
            original_raw = {player: _raw_player(scraper, payload, original[native.key][original[native.key].player_id.astype(str) == player], player, native.key)
                            for player in players}
            cursor = connection.cursor()
            try:
                execute_statement(cursor, f'SELECT cycle_id, entity, native_batch_id, capture_times_json FROM {career_refs.TABLE} '
                    'WHERE native_table=? ORDER BY committed_at DESC LIMIT 64', (native.table_name,))
                candidates = cursor.fetchall()
            except Exception as exc:
                if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                    return None
                raise
            finally:
                cursor.close()
            covered, units = {}, []
            for cycle, entity, batch, clocks_json in candidates:
                clocks = json.loads(clocks_json)
                selected = [player for player in players if player not in covered and player in clocks
                    and pd.to_datetime(clocks[player], utc=True) > pd.to_datetime(evidence['captured_at_by_id'][player], utc=True)]
                if not selected:
                    continue
                archived = write_intents.completed_capture(cycle, entity, batch)
                if archived is None:
                    continue  # Pre-archive successful captures cannot invent raw proof.
                if archived['receipt'].get('writer_revision') != evidence['revision'] or archived['journal']['evidence'].get('revision') != evidence['revision']:
                    raise RuntimeError('supersession writer authority changed')
                try:
                    proof = verify_complete(scraper, spec, archived, players=selected)
                except StaleTransfermarktWrite:
                    continue  # An older genuine capture cannot prove current supersession.
                units.append(proof['unit'])
                covered.update({player: {'unit': proof['unit'], 'raw': proof['raw'][player]} for player in selected
                    if pd.to_datetime(proof['raw'][player]['fetched_at'], utc=True) > pd.to_datetime(evidence['captured_at_by_id'][player], utc=True)})
            if set(covered) != set(players):
                return None
            resolution = {'status': 'superseded_partial_write', 'verified': False, 'intent_sha256': Path(path).stem,
                'delivery_cycle_id': delivery_cycle, 'original_cycle_id': evidence['cycle_id'], 'original_batch_id': str(scraper._batch_id),
                'original_native_snapshot_id': partial[native.key] or None, 'original_native_hash': _hash(original[native.key]),
                'original_native_table_absent': partial[native.key] == 0,
                'original_legacy_snapshot_id': None, 'original_raw': original_raw,
                'original_window': evidence['window'], 'signal_generations': evidence.get('signal_generations', {}),
                'retired_player_ids': players, 'superseding_players': covered, 'superseding_units': units}
            write_intents.retire_intent(path, resolution)
            return resolution
        finally:
            connection.close()


def cached_supersession(scraper, spec, retired, selected, *, reconcile=False):
    """Reuse only the exact currently complete successful full source capture."""
    players, sources, blocked = {}, {}, {}
    current_generations = dict(getattr(scraper, '_current_career_signal_generations', {}) or {})
    resolution_raw, old_generations = {}, {}
    for _, _, resolution in retired:
        for player in resolution['retired_player_ids']:
            raw = resolution['superseding_players'][player]['raw']
            if player not in resolution_raw or pd.to_datetime(raw['fetched_at'], utc=True) > pd.to_datetime(resolution_raw[player]['fetched_at'], utc=True):
                resolution_raw[player] = raw
            old_generations.setdefault(player, set()).add(resolution.get('signal_generations', {}).get(player))
    for _, _, resolution in retired:
        for player in set(selected) & set(resolution['retired_player_ids']):
            unit = resolution['superseding_players'][player]['unit']
            ids = sources.setdefault(json.dumps(unit, sort_keys=True), (unit, []))[1]
            if player not in ids:
                ids.append(player)
    native = next(output for output in spec.outputs if not output.is_legacy)
    for unit, ids in sources.values():
        archived = write_intents.completed_capture(unit['cycle_id'], unit['entity'], unit['native_batch_id'])
        if archived is None:
            raise RuntimeError('retired career superseding raw archive is unavailable')
        try:
            proof = verify_complete(scraper, spec, archived, players=ids)
            current_proofs = {player: proof for player in ids}
        except StaleTransfermarktWrite:
            # The immutable retirement points to the capture that superseded
            # the old job. Subsequent genuine captures can supersede that one.
            connection = scraper._bronze_connection()
            cursor = connection.cursor()
            try:
                execute_statement(cursor, f'SELECT cycle_id, entity, native_batch_id FROM {career_refs.TABLE} '
                    'WHERE native_table=? ORDER BY committed_at DESC LIMIT 64', (native.table_name,))
                candidates = cursor.fetchall()
            finally:
                cursor.close()
                connection.close()
            current_proofs = {}
            # Captures can share the same Bronze commit clock. Revisit a
            # bundle after another candidate proved its replaced players,
            # now checking only the remaining players. Two bounded passes.
            for cycle, entity, batch in candidates + candidates:
                candidate = write_intents.completed_capture(cycle, entity, batch)
                if candidate is None:
                    continue
                candidate_ids = [player for player in ids if player not in current_proofs
                    and player in candidate['journal']['evidence'].get('processed', candidate['journal']['evidence'].get('checkpoint_ids', []))]
                if not candidate_ids:
                    continue
                try:
                    newer = verify_complete(scraper, spec, candidate, players=candidate_ids)
                except StaleTransfermarktWrite:
                    continue
                current_proofs.update({player: newer for player in candidate_ids
                    if pd.to_datetime(newer['raw'][player]['fetched_at'], utc=True) >=
                    pd.to_datetime(resolution_raw[player]['fetched_at'], utc=True)})
                if set(current_proofs) == set(ids):
                    break
            if set(current_proofs) != set(ids):
                raise StaleTransfermarktWrite('retired job has no proven complete current successor')
        for player in ids:
            proof = current_proofs[player]
            archived = proof['archive']
            current_generation = current_generations.get(player)
            captured_generation = archived['journal']['evidence'].get('signal_generations', {}).get(player)
            age = scraper._http_client._time() - pd.to_datetime(proof['raw'][player]['fetched_at'], utc=True).timestamp()
            configured_ttl = getattr(scraper, '_cache_ttl_seconds', None) or 86400
            fresh = 0 <= age < min(86400, float(configured_ttl))
            same_generation = current_generation is None or current_generation == captured_generation
            if not reconcile and (not fresh or not same_generation):
                # A genuinely new observed generation may take the ordinary
                # paid path. Never buy the failed original generation again.
                original_generations = old_generations.get(player, {None})
                if current_generation is None or None in original_generations or current_generation in original_generations:
                    blocked[player] = 'career_raw_cache_expired' if not fresh else 'career_cache_generation_differs'
                continue
            players[player] = {'frame': proof['frames'][native.key][proof['frames'][native.key].player_id.astype(str) == player].copy(),
                               'raw': proof['raw'][player], 'state': next(row for key, row in zip(
                                   archived['journal']['evidence'].get('processed', archived['journal']['evidence'].get('checkpoint_ids', [])), archived['journal']['evidence']['state_rows'], strict=True) if key == player)}
    return players, blocked
