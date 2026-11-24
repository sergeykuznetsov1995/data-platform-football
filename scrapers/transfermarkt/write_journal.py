"""Durable core write intents, anchors and immutable receipts (#2404).

An intent is saved before the first mutation of a core unit, so a crash
between commit and acknowledgement replays from disk instead of another paid
request. Anchors record the pre-write warehouse boundary per table; receipts
are immutable proof of a physically verified commit. An append-only index
replaces directory scanning: readers consult the index and open only the
intent files it names.
"""

import fcntl
import hashlib
import json
import os
import re
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import uuid4


def _root() -> Path:
    return Path(os.environ.get(
        'TM_CORE_WRITE_JOURNAL_DIR', '/opt/airflow/logs/transfermarkt-core/write-journal'
    ))


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _invalid_json_number(value):
    raise ValueError(f'invalid JSON number: {value}')


@contextmanager
def _directory_fd(directory: Path):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        yield fd
    finally:
        os.close(fd)


def _fsync_directory(directory: Path) -> None:
    with _directory_fd(directory) as fd:
        os.fsync(fd)


def _ensure_directory(directory: Path) -> None:
    if not directory.is_dir():
        _ensure_directory(directory.parent)
        directory.mkdir(exist_ok=True)
        _fsync_directory(directory.parent)


def _write_once(path: Path, data: bytes, mismatch: str, *, replace=False) -> None:
    _ensure_directory(path.parent)
    with _directory_fd(path.parent) as directory:
        # Serialize the existence check and rename, including standalone journal users.
        fcntl.flock(directory, fcntl.LOCK_EX)
        if path.exists() and not replace:
            if path.read_bytes() != data:
                raise RuntimeError(mismatch)
            return
        with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
            tmp = Path(temp.name)
            try:
                temp.write(data)
                temp.flush()
                os.fsync(temp.fileno())
                os.replace(tmp, path)
                os.fsync(directory)
            finally:
                tmp.unlink(missing_ok=True)


def _store_record(path: Path, payload: Mapping, *, replace=False) -> None:
    payload = dict(payload)
    checksum = hashlib.sha256(_canonical(payload)).hexdigest()
    data = _canonical({'payload': payload, 'sha256': checksum})
    _write_once(path, data, 'core journal record differs', replace=replace)


def _immutable_record(path: Path, payload: Mapping) -> None:
    _store_record(path, payload)


def _read_record(path: Path) -> dict:
    try:
        record = json.loads(path.read_bytes(), parse_constant=_invalid_json_number)
        payload = record['payload']
        checksum = hashlib.sha256(_canonical(payload)).hexdigest()
        if isinstance(payload, dict) and record['sha256'] == checksum:
            return payload
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError('core journal record checksum differs') from exc
    raise RuntimeError('core journal record checksum differs')


def _index_path() -> Path:
    return _root() / 'index.jsonl'


def _discard_torn_tail(stream) -> None:
    # A retry must not join its new line to an uncommitted fragment.
    end = stream.seek(0, os.SEEK_END)
    if not end:
        return
    stream.seek(end - 1)
    if stream.read(1) == b'\n':
        return
    while end:
        start = max(0, end - 8192)
        stream.seek(start)
        newline = stream.read(end - start).rfind(b'\n')
        if newline >= 0:
            stream.truncate(start + newline + 1)
            return
        end = start
    stream.truncate(0)


def _append_index(record: Mapping) -> None:
    path = _index_path()
    _ensure_directory(path.parent)
    data = _canonical(dict(record)) + b'\n'
    with path.open('a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        _discard_torn_tail(stream)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    _fsync_directory(path.parent)


def save_intent(identity: Mapping, payload: Mapping, **evidence) -> Path:
    """Persist a replayable unit before publishing its pending index entry."""
    identity = dict(identity)
    body = _canonical({'identity': identity, 'payload': dict(payload), 'evidence': evidence})
    digest = hashlib.sha256(body).hexdigest()
    path = _root() / (digest + '.json')
    _write_once(path, body, 'core write intent checksum collision')
    _append_index({'digest': digest, 'identity': identity, 'status': 'pending'})
    return path


def _intent_body(path: Path, mismatch: str) -> dict:
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != path.stem:
        raise RuntimeError(mismatch)
    try:
        body = json.loads(data, parse_constant=_invalid_json_number)
    except ValueError as exc:
        raise RuntimeError('core write intent is corrupt') from exc
    if not isinstance(body, dict) or not all(
        isinstance(body.get(key), dict) for key in ('identity', 'payload', 'evidence')
    ):
        raise RuntimeError('core write intent is corrupt')
    return body


def _index_entry(line: bytes) -> dict:
    try:
        entry = json.loads(line, parse_constant=_invalid_json_number)
        if not isinstance(entry, dict) or not re.fullmatch(r'[0-9a-f]{64}', entry['digest']):
            raise ValueError('invalid digest')
        if entry['status'] == 'finished' and isinstance(entry.get('identity', {}), dict):
            return entry
        if entry['status'] == 'pending' and isinstance(entry['identity'], dict):
            return entry
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError('core write journal index is corrupt') from exc
    raise RuntimeError('core write journal index is corrupt')


def _load_index_entries() -> list[dict]:
    entries = {}
    try:
        with _index_path().open('rb') as stream:
            for line in stream:
                if line.endswith(b'\n'):
                    entry = _index_entry(line)
                    if entry['status'] == 'finished' and entry['digest'] in entries:
                        # Older finish records contain only the file hash. Keep
                        # the preceding pending identity for upgrade discovery.
                        previous = entries[entry['digest']]
                        if ('identity' in previous and 'identity' in entry
                                and previous['identity'] != entry['identity']):
                            raise RuntimeError('core write intent identity differs')
                        entry = {**previous, **entry}
                    entries[entry['digest']] = entry
    except FileNotFoundError:
        pass
    return list(entries.values())


def _load_index() -> list[dict]:
    root = _root()
    entries = {entry['digest']: entry for entry in _load_index_entries()}

    files = {path.stem: path for path in root.glob('*.json')}
    for digest, entry in entries.items():
        if entry['status'] == 'pending' and digest not in files:
            # A writer may have finished this unit between our index read and
            # this listing; re-reading the index settles it without guessing.
            if any(item['digest'] == digest and item['status'] == 'finished'
                   for item in _load_index_entries()):
                continue
            raise RuntimeError('core write journal pending intent file is missing')
    for digest in files.keys() - entries.keys():
        body = _intent_body(files[digest], 'core write journal orphan checksum differs')
        entries[digest] = {'digest': digest, 'status': 'pending', 'identity': body['identity']}
    return list(entries.values())


def pending_intents(identity: Mapping, *, untracked_only=False) -> list[tuple[Path, dict]]:
    """Read indexed and orphaned pending units that match the requested identity."""
    pending = []
    for entry in _load_index():
        if entry['status'] != 'pending' or not identity.items() <= entry['identity'].items():
            continue
        if untracked_only:
            if entry['identity'].get('statement_protocol') == 1:
                continue
            marker = _record_path('adopted-intents', entry['digest'])
            if marker.exists():
                if _read_record(marker) != {'intent': entry['digest']}:
                    raise RuntimeError('core adopted intent differs')
                continue
        path = _root() / (entry['digest'] + '.json')
        body = _intent_body(path, 'core write intent checksum differs')
        if body['identity'] != entry['identity']:
            raise RuntimeError('core write intent identity differs')
        pending.append((path, body))
    return sorted(pending, key=lambda pair: pair[0].name)


def adopt_intent(path):
    """Record a handover proven before first dispatch or after complete readback."""
    _immutable_record(_record_path('adopted-intents', path.stem), {'intent': path.stem})


def finish_intent(path: Path) -> None:
    """Publish completion before removing the verified intent file."""
    body = _intent_body(path, 'core write intent checksum differs')
    entry = {'digest': path.stem, 'status': 'finished', 'identity': body['identity']}
    _immutable_record(_record_path('finished-identities', path.stem), entry)
    _append_index(entry)
    path.unlink()


def guard_history():
    """An index loss cannot discard the identity of an archived scope owner."""
    entries = {entry['digest']: entry for entry in _load_index()}
    for path in (_root() / 'finished-identities').glob('*.json'):
        archived = _read_record(path)
        if archived['digest'] != path.stem or archived['status'] != 'finished':
            raise RuntimeError('core finished identity differs')
        indexed = entries.get(path.stem)
        if indexed and indexed.get('identity', archived['identity']) != archived['identity']:
            raise RuntimeError('core finished identity differs')
        entries[path.stem] = archived
    for directory in ('migrated-scopes', 'guard-inventories'):
        for path in (_root() / directory).glob('*.json'):
            record = _read_record(path)
            if record['intent'] != path.stem:
                raise RuntimeError('core migration identity differs')
            entry = entries.setdefault(path.stem, {'digest': path.stem, 'status': 'finished',
                                                   'identity': {'digest': record['digest']}})
            identity = entry.setdefault('identity', {'digest': record['digest']})
            if identity.get('digest') != record['digest']:
                raise RuntimeError('core migration identity differs')
    known = {entry.get('identity', {}).get('digest') for entry in entries.values()}
    accounted = known.copy()
    for path in (_root() / 'receipts').glob('*.json'):
        digest = path.stem
        if digest in known:
            continue
        receipt = read_receipt(digest)
        if not acknowledgement_bound(digest, receipt):
            accounted.add(digest)
            yield {'digest': digest, 'status': 'finished', 'identity': {'digest': digest}}
            continue
        warehouse = (receipt.get('identity') or {}).get('digest', digest)
        accounted.update((digest, warehouse))
        if warehouse not in known:
            # No dispatch protocol survives. The acknowledgement binds the
            # affected targets, but does not certify terminal DML.
            yield {'digest': digest, 'status': 'finished',
                   'identity': {'run_id': receipt['run_id'], 'digest': digest}}
    for path in (_root() / 'anchors').glob('*.json'):
        if path.stem in accounted:
            continue
        alias = _record_path('anchor-copies', path.stem)
        if alias.exists():
            copy = _read_record(alias)
            if (copy['target'] != path.stem or copy['source'] == path.stem or
                    read_snapshot_anchors(copy['source']) != read_snapshot_anchors(path.stem)):
                raise RuntimeError('core alias snapshot anchors differ')
            if acknowledgement_bound(copy['source'], read_receipt(copy['source'])):
                continue  # interrupted between copying the anchors and publishing their alias receipt
        yield {'digest': path.stem, 'status': 'finished', 'identity': {'digest': path.stem}}
    for entry in entries.values():
        if entry['status'] == 'pending':
            # Completion precedes owner unlink and index retirement. Its
            # unresolved DML guards already apply throughout that interval.
            completion = completed_intent(entry['digest'])
            if completion and not completion['terminal']:
                yield {**entry, 'status': 'finished'}
            continue
        if entry['status'] == 'finished':
            archive = _record_path('finished-identities', entry['digest'])
            if entry.get('identity', {}).get('run_id') and not archive.exists():
                _immutable_record(archive, entry)
            yield entry


def _record_path(directory: str, digest: str) -> Path:
    if not digest or Path(digest).name != digest:
        raise ValueError('invalid core journal digest')
    return _root() / directory / (digest + '.json')


def _statement_path(key):
    return _record_path('pending-statements', hashlib.sha256(_canonical(key)).hexdigest())


def _attempt_path(key):
    return _record_path('dispatch-attempts', _statement_path(key).stem)


def _operation_directory(scope):
    # A restore of the replay journal must not rewind the dispatch boundary.
    root = _root()
    return root.with_name(root.name + '.attempts') / hashlib.sha256(_canonical(scope)).hexdigest()


def _operation_path(scope, key, member):
    name = hashlib.sha256(_canonical([key, member])).hexdigest()
    return _operation_directory(scope) / (name + '.json')


def _submission_path(owner):
    # Kept outside the replay journal and its operation backup. One current
    # submission per packet is enough to detect a rewind of the full pair:
    # all its submits are serialized by the shared writer lock.
    if not re.fullmatch('tm-core-receipt-[0-9a-f]{32}', owner):
        raise ValueError('invalid core submission owner')
    root = _root()
    return root.with_name(root.name + '.submissions') / (owner + '.json')


def _review_required_path(owner, intent):
    if not re.fullmatch('[0-9a-f]{64}', intent):
        raise ValueError('invalid core review intent')
    return _submission_path(owner).parent / 'review-required' / owner / (intent + '.json')


def _packet_watermark(owner):
    root = _root()
    return root.with_name(root.name + '.attempts') / 'watermarks' / 'packets' / (owner + '.json')


def _watermark_binding(owner):
    # An ordinary file restore of the watermark itself must not authenticate
    # an older pair, even when all the restored JSON content agrees.
    stat = _submission_path(owner).stat()
    return {'owner': owner, 'inode': stat.st_ino, 'ctime_ns': stat.st_ctime_ns}


def _save_submission(head):
    owner = head['owner']
    _store_record(_submission_path(owner), head, replace=True)
    _store_record(_packet_watermark(owner), _watermark_binding(owner), replace=True)


def submission_head(owner):
    path = _submission_path(owner)
    if path.exists():
        head = _read_record(path)
        if head['owner'] != owner:
            raise RuntimeError('core submission owner differs')
        return head


def start_submissions(owner):
    """Establish a positive pre-dispatch boundary before saving the intent."""
    existing = submission_head(owner)
    if existing is None:
        _save_submission({'owner': owner})
    elif 'statement' in existing or not submission_is_current(existing):
        raise RuntimeError('core prior packet submission is still pending')


def submission_is_current(head):
    owner = head['owner']
    path = _packet_watermark(owner)
    if not path.exists():
        return False
    watermark = _read_record(path)
    binding = _watermark_binding(owner)
    if not binding.items() <= watermark.items():
        return False
    if 'statement' not in head:
        return 'retired' not in watermark
    statement = head['statement']
    op = statement['operation']
    if op['owner'] != head['owner']:
        raise RuntimeError('core submission owner differs')
    record = latest_operation(op['scope'], statement['key'], op['member'])
    if record is not None:
        return record.get('statement') == statement
    return watermark.get('retired') == statement['attempt']


def confirm_reviewed_submission(owner):
    # The caller verified the explicit decisions and the entire live packet.
    if submission_head(owner) is not None:
        _store_record(_packet_watermark(owner), _watermark_binding(owner), replace=True)


def _read_operation(path, scope, record=None):
    if record is None:
        record = _read_record(path)
    operation = record['operation']
    statement = record.get('statement')
    publications = operation.get('after_publication', [])
    if (operation['scope'] != list(scope)
            or path != _operation_path(scope, record['key'], operation['member'])
            or not isinstance(publications, list)
            or any(not isinstance(intent, str) or not re.fullmatch('[0-9a-f]{64}', intent)
                   for intent in publications)
            or record['state'] not in ('dispatched', 'returned', 'observed', 'unchanged', 'rejected', 'FINISHED')
            or (record['state'] == 'unchanged') != (statement is None)
            or statement is not None and (statement['key'] != record['key']
                                          or statement.get('operation', operation) != operation)
            or record['state'] == 'FINISHED' and not record.get('query_id')):
        raise RuntimeError('core operation binding differs')
    return record


def scope_operations(scope):
    for path in _operation_directory(scope).glob('*.json'):
        yield _read_operation(path, scope)


def latest_operation(scope, key, member):
    path = _operation_path(scope, key, member)
    return _read_operation(path, scope) if path.exists() else None


def published_intents(digest, receipt, descriptor):
    """Original intent hashes whose terminal completions authenticate this publication."""
    result = []
    for entry in _load_index():
        if entry.get('identity', {}).get('digest') == digest:
            completion = completed_intent(entry['digest'])
            if (completion and completion['terminal'] is True and completion.get('acknowledgement')
                    and completion.get('descriptor_digest') == descriptor
                    and acknowledgement_bound(digest, receipt, receipt['run_id'], completion)):
                result.append(entry['digest'])
    return result


def later_repair_survives(intent, operation):
    """A repair of this published intent cannot belong to its pre-first-publication backup."""
    for record in scope_operations(operation['scope']):
        repair = record['operation']
        if (intent in repair.get('after_publication', ()) and repair['intent'] != intent
                and record['state'] in ('dispatched', 'FINISHED')
                and all(repair[k] == operation[k] for k in ('owner', 'member', 'plan_sha256'))):
            return True
    return False


def record_reconciliation(intent, statement, query_id, *, reviewer):
    """Record an operator's independent review of the member and all its submissions.

    Called explicitly under the writer lock. The operator must recover the actual
    latest statement and rule out outstanding writes, including hidden attempts.
    Never called by dispatch, statement settlement, or replay.
    """
    if not re.fullmatch(r'[0-9a-f]{64}', intent):
        raise ValueError('invalid core reconciliation intent')
    if not all(isinstance(value, str) and value.strip() for value in (query_id, reviewer)):
        raise ValueError('core reconciliation requires a reviewer and a query id')
    operation = statement['operation']
    decision = {'intent': intent, 'statement': statement, 'query_id': query_id,
                'operations_sha256': _member_operations_digest(operation),
                'reviewer': reviewer, 'decision': 'publish_exact_postimage'}
    _check_reviewed_attempt(operation, decision)
    path = _record_path('operator-reconciliations/' + intent, operation['member'])
    _immutable_record(path, decision)
    return decision


def check_reconciliation(intent, operation, decision):
    """Bind the explicitly presented decision to the surviving reviewed attempt."""
    path = _record_path('operator-reconciliations/' + intent, operation['member'])
    if (not path.exists() or _read_record(path) != decision or decision.get('intent') != intent
            or decision.get('decision') != 'publish_exact_postimage' or not decision.get('reviewer')
            or not decision.get('query_id')):
        raise RuntimeError('core operator reconciliation binding differs')
    return _check_reviewed_attempt(operation, decision)


def _member_operations_digest(operation):
    attempts = [(r['key'], r.get('statement')) for r in scope_operations(operation['scope'])
                if r['operation']['member'] == operation['member']]
    return hashlib.sha256(_canonical(sorted(attempts, key=lambda pair: pair[0]))).hexdigest()


def _check_reviewed_attempt(operation, decision):
    statement = decision['statement']
    key = statement['key']
    current = latest_operation(operation['scope'], key, operation['member'])
    reviewed = statement['operation']
    head = submission_head(operation['owner'])
    latest = head and head.get('statement')
    if latest and latest['operation']['member'] == operation['member'] and latest != statement:
        raise RuntimeError('core operator reconciliation attempt differs')
    if (decision['operations_sha256'] != _member_operations_digest(operation)
            or not current or current.get('statement') != statement or current['operation'] != reviewed
            or any(reviewed[field] != operation[field] for field in ('scope', 'owner', 'member', 'plan_sha256'))
            or not re.fullmatch('[0-9a-f]{32}', statement['attempt'])
            or current['state'] == 'rejected' or current.get('query_id') not in (None, decision['query_id'])
            or (statement_query(statement) or current.get('query_id')) != decision['query_id']):
        raise RuntimeError('core operator reconciliation attempt differs')
    return statement


def _matching_operations(field, values):
    """Find a surviving statement when its primary journal index is missing."""
    root = _root()
    for path in root.with_name(root.name + '.attempts').glob('*/*.json'):
        record = _read_record(path)
        statement = record.get('statement')
        if statement and statement.get(field) in values:
            yield _read_operation(path, record['operation']['scope'], record)


def statement_for_readback(scope, key, member):
    statement = pending_statement(key)
    if statement:
        return statement
    current = latest_operation(scope, key, member)
    if current and current['state'] == 'FINISHED':
        return current['statement']


def remember_unchanged_target(operation):
    """Bind a verified target that required no DML, before publishing its walk."""
    scope = operation['scope']
    path = _operation_path(scope, scope, operation['member'])
    if not path.exists():
        _immutable_record(path, {'key': scope, 'operation': operation, 'state': 'unchanged'})


def _statement_operation(statement):
    operation = statement.get('operation')
    if operation is None:
        path = _record_path('operation-bindings', statement['attempt'])
        if not path.exists():
            return
        binding = _read_record(path)
        if binding['statement'] != statement:
            raise RuntimeError('core operation attempt differs')
        operation = binding['operation']
    return operation


def _operation_transition(statement, state, query_id=None):
    operation = _statement_operation(statement)
    if operation is None:
        return
    scope, member = operation['scope'], operation['member']
    path = _operation_path(scope, statement['key'], member)
    previous = latest_operation(scope, statement['key'], member)
    if state == 'dispatched':
        if previous and previous['state'] in ('dispatched', 'FINISHED'):
            raise RuntimeError('core operation outcome is still pending')
    elif not previous or previous.get('statement') != statement:
        raise RuntimeError('core operation attempt differs')
    elif previous.get('query_id') and statement_query(statement) not in (None, previous['query_id']):
        raise RuntimeError('core operation query binding differs')
    elif previous['state'] != 'dispatched' and not (
            state == 'FINISHED' and previous['state'] in ('observed', 'returned')):
        if state == 'FINISHED' and (previous['state'], previous.get('query_id')) != (state, query_id):
            raise RuntimeError('core operation outcome differs')
        return previous.get('query_id')  # resume interrupted statement cleanup
    record = {'key': statement['key'], 'statement': statement, 'operation': operation, 'state': state}
    if state == 'FINISHED':
        if not query_id or statement_query(statement) not in (None, query_id):
            raise RuntimeError('core operation query binding differs')
        record['query_id'] = query_id
    elif state != 'dispatched' and (mapped := statement_query(statement)) is not None:
        record['query_id'] = mapped
    _store_record(path, record, replace=True)
    return record.get('query_id')


def verified_operation(statement, query_id):
    _operation_transition(statement, 'FINISHED', query_id)


def note_observed(statement):
    _operation_transition(statement, 'observed')


def bind_pending_operation(statement, operation):
    """Adopt a surviving pre-upgrade statement before reading its coordinator outcome."""
    current = latest_operation(operation['scope'], statement['key'], operation['member'])
    if current:
        # The recovered dispatch can belong to a later repair intent than the
        # restored replay descriptor. Authenticate the actual latest statement.
        if current.get('statement') != statement or current['operation']['owner'] != operation['owner']:
            raise RuntimeError('core latest operation outcome is still pending')
        if 'operation' not in statement:
            _immutable_record(_record_path('operation-bindings', statement['attempt']),
                              {'statement': statement, 'operation': current['operation']})
        return current
    if 'operation' in statement:
        raise RuntimeError('core latest operation evidence is still pending')
    path = _record_path('operation-bindings', statement['attempt'])
    _immutable_record(path, {'statement': statement, 'operation': operation})
    _operation_transition(statement, 'dispatched')
    return latest_operation(operation['scope'], statement['key'], operation['member'])


def retire_operations(scopes, owner):
    head = submission_head(owner)
    for scope in scopes:
        for record in scope_operations(scope):
            operation = record['operation']
            if operation['owner'] != owner:
                continue
            statement = record.get('statement')
            if record['state'] == 'dispatched' or statement and pending_statement(record['key']) == statement:
                raise RuntimeError('core operation outcome is still pending')
            if statement and (query_id := statement_query(statement)) is not None:
                if record['state'] != 'FINISHED' or record['query_id'] != query_id:
                    raise RuntimeError('core operation query binding differs')
                _unlink_record(_record_path('statement-queries', statement['attempt']))
            if statement and 'operation' not in statement:
                _unlink_record(_record_path('operation-bindings', statement['attempt']))
            if statement and head and head.get('statement') == statement:
                # Keep the last submit identity in the sibling after retirement.
                _store_record(_packet_watermark(owner), {**_watermark_binding(owner), 'retired': statement['attempt']},
                              replace=True)
            _unlink_record(_operation_path(scope, record['key'], operation['member']))


def _unlink_record(path):
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


def _check_settled(marker, statement):
    # Older markers held just the statement. A marker is cleanup evidence
    # only after its own operation crossed the durable settlement boundary.
    if marker.get('statement', marker) != statement:
        raise RuntimeError('core settled statement binding differs')
    query_id = statement_query(statement)
    operation = _statement_operation(statement)
    if operation is not None:
        current = latest_operation(operation['scope'], statement['key'], operation['member'])
        if (not current or current.get('statement') != statement or current['operation'] != operation
                or current['state'] == 'dispatched'
                or current.get('query_id') and query_id not in (None, current['query_id'])):
            raise RuntimeError('core settled operation binding differs')
        query_id = current.get('query_id', query_id)
    if 'statement' in marker and (query_id is not None or operation) and marker['query_id'] != query_id:
        raise RuntimeError('core settled query binding differs')


def pending_statement(key):
    path = _statement_path(key)
    binding = _attempt_path(key)
    settled = _record_path('settled-statements', path.stem)
    marker = _read_record(settled) if settled.exists() else None
    if not path.exists() and not binding.exists():
        if marker is not None:
            statement = marker.get('statement', marker)
            if statement['key'] != list(key):
                raise RuntimeError('core settled statement key differs')
            _check_settled(marker, statement)
            _unlink_record(settled)
        return None
    statement = _read_record(path if path.exists() else binding)
    if statement['key'] != list(key):
        raise RuntimeError('core pending statement key differs')
    if marker is not None:
        _check_settled(marker, statement)
    if binding.exists():
        if _read_record(binding) != statement:
            raise RuntimeError('core pending statement binding differs')
    else:
        _immutable_record(binding, statement)
    if not path.exists():
        _immutable_record(path, statement)
    if marker is not None:
        finish_statement(statement)
        return None
    return statement


def _dispatch_records():
    for directory in ('pending-statements', 'dispatch-attempts'):
        for path in (_root() / directory).glob('*.json'):
            statement = _read_record(path)
            if path.name != _statement_path(statement['key']).name:
                raise RuntimeError('core pending statement key differs')
            yield statement


def check_dispatch_bindings(scopes=None):
    """Route restored query mappings without settling another scope's statements."""
    root = _root()
    queries = {path.stem: path for path in (root / 'statement-queries').glob('*.json')}
    if not queries:
        return
    for statement in _dispatch_records():
        queries.pop(statement['attempt'], None)
    # Legacy query mappings contain only a query ID. Their sibling operation
    # can still identify the attempt after both local statement files were unlinked.
    if not queries:
        return
    bindings = {}
    for current in _matching_operations('attempt', queries):
        attempt = current['statement']['attempt']
        if attempt in bindings:
            raise RuntimeError('core operation attempt differs')
        bindings[attempt] = current
    for attempt, path in queries.items():
        mapping = _read_record(path)
        current = bindings.get(attempt)
        statement = mapping.get('statement', {})
        affected = {tuple(op['scope']) for op in (statement.get('operation'),
                    current and current['operation']) if op}
        if affected and scopes is not None and affected.isdisjoint(scopes):
            continue
        if (not current or current['state'] != 'FINISHED' or current['query_id'] != mapping['query_id']
                or 'statement' in mapping and current['statement'] != statement):
            raise RuntimeError('unbound core statement outcome is still pending')


def stage_in_use(record):
    for current in _matching_operations('stage', {record['stage']}):
        if (current['operation']['member'] != record['receipt_id']
                or current['key'][0] != record['table'] or current['state'] == 'dispatched'):
            return True
    for target in _dispatch_records():
        if target['key'][0] == record['table'] and (
                target.get('stage') == record['stage'] or not target.get('stage') and
                target['expected'] in (record['receipt_id'], {'single_submit': record['receipt_id']})):
            return True
    return False


def begin_statement(key, expected, *, observable=True, target_preimage=False, stage=None, operation=None):
    """Fence one dispatch under the shared writer lock, before it can reach Trino."""
    statement = {'key': list(key), 'expected': expected, 'observable': observable,
                 'attempt': uuid4().hex}
    if target_preimage:
        statement['target_preimage'] = True
    if stage is not None:
        statement['stage'] = stage
    if operation is not None:
        statement['operation'] = operation
    if pending_statement(key) is not None:
        raise RuntimeError('core statement outcome is still pending: ' + key[0])
    if operation is not None:
        owner = operation['owner']
        previous = submission_head(owner)
        if previous and not submission_is_current(previous):
            raise RuntimeError('core pre-publication operator reconciliation is still pending')
    _operation_transition(statement, 'dispatched')
    if operation is not None:
        _save_submission({'owner': owner, 'statement': statement})
    _immutable_record(_attempt_path(key), statement)
    _immutable_record(_statement_path(key), statement)
    return statement


def remember_query(statement, query_id):
    if isinstance(query_id, str) and query_id:
        path = _record_path('statement-queries', statement['attempt'])
        if path.exists() and statement_query(statement) == query_id:
            return
        record = {'query_id': query_id}
        if 'operation' in statement:
            record['statement'] = statement
        _immutable_record(path, record)


def statement_query(statement):
    path = _record_path('statement-queries', statement['attempt'])
    if path.exists():
        record = _read_record(path)
        if record.get('statement', statement) != statement:
            raise RuntimeError('core operation query binding differs')
        return record['query_id']


def finish_statement(statement, *, outcome='returned'):
    """Release the fence only after the attempt is settled, including across a crash."""
    path = _statement_path(statement['key'])
    binding = _attempt_path(statement['key'])
    for record in (path, binding):
        if record.exists() and _read_record(record) != statement:
            raise RuntimeError('core pending statement changed')
    if not (path.exists() or binding.exists()):
        raise RuntimeError('core pending statement is missing')
    settled = _record_path('settled-statements', path.stem)
    if settled.exists():
        _check_settled(_read_record(settled), statement)
    else:
        query_id = statement_query(statement)
        bound_query = _operation_transition(statement, outcome)
        _immutable_record(settled, {'statement': statement, 'query_id': bound_query or query_id})
    if statement.get('stage'):
        mark_stage_idle(statement['expected']['single_submit'], statement['stage'])
    _unlink_record(_record_path('statement-queries', statement['attempt']))
    _unlink_record(path)
    _unlink_record(binding)
    _unlink_record(settled)


def _scope_owner_path(scope):
    return _record_path('scope-owners', hashlib.sha256(_canonical(scope)).hexdigest())


def scope_owner(scope):
    path = _scope_owner_path(scope)
    if path.exists():
        record = _read_record(path)
        if record['scope'] != list(scope):
            raise RuntimeError('core scope guard differs')
        return record['owner']
    return None


def hold_scope(scope, owner):
    _immutable_record(_scope_owner_path(scope), {'scope': list(scope), 'owner': owner})


def release_scope(scope, owner):
    if scope_owner(scope) == owner:
        path = _scope_owner_path(scope)
        path.unlink()
        _fsync_directory(path.parent)


def bundle_reservations():
    for path in (_root() / 'reserving-bundles').glob('*.json'):
        record = _read_record(path)
        if record['owner'] != path.stem:
            raise RuntimeError('core bundle reservation differs')
        yield record


def reserve_scopes(scopes, owner):
    """Publish the entire reservation before materializing individual scope guards."""
    scopes = sorted(set(scopes))
    path = _record_path('reserving-bundles', owner)
    if len(scopes) > 1:
        _immutable_record(path, {'owner': owner, 'scopes': scopes})
    for scope in scopes:
        hold_scope(scope, owner)


def finish_reservation(owner, scopes):
    path = _record_path('reserving-bundles', owner)
    if path.exists():
        expected = {'owner': owner, 'scopes': [list(s) for s in sorted(set(scopes))]}
        if _read_record(path) != expected:
            raise RuntimeError('core bundle reservation differs')
        path.unlink()
        _fsync_directory(path.parent)


def completed_intent(intent):
    path = _record_path('completed-intents', intent)
    if path.exists():
        record = _read_record(path)
        if record['intent'] != intent:
            raise RuntimeError('core completed intent differs')
        return record


def complete_intent(path, digest, owner, terminal):
    from .native_writer import _bundle_identity, _intent_unit

    body = _intent_body(path, 'core write intent checksum differs')
    run = body['identity']['run_id']
    plans, _, _ = _intent_unit(body)
    descriptor_digest = _bundle_identity(plans, run)[1]
    receipt = read_receipt(digest)
    binding = {'intent': path.stem, 'digest': digest, 'owner': owner, 'terminal': terminal}
    existing = completed_intent(path.stem)
    completion = existing or {
        **binding, 'descriptor_digest': descriptor_digest,
        'acknowledgement': hashlib.sha256(_canonical(receipt)).hexdigest()}
    if (not binding.items() <= completion.items()
            or completion.get('descriptor_digest', descriptor_digest) != descriptor_digest
            or not acknowledgement_bound(digest, receipt, run, completion)):
        raise RuntimeError('core completed intent acknowledgement differs')
    _immutable_record(_record_path('completed-intents', path.stem), completion)


def terminal_target(intent, receipt_id, statement=None, *, scope=None):
    current = None
    if scope is not None:
        current = latest_operation(scope, scope, receipt_id)
        if current:
            if current['state'] != 'FINISHED':
                return False
            statement = current['statement']
    witness = terminal_witness(intent, receipt_id, statement)
    if current and witness and current['query_id'] != witness['query_id']:
        raise RuntimeError('core terminal target binding differs')
    return witness is not None and witness.get('outcome') == 'FINISHED'


def terminal_witness(intent, receipt_id, statement=None):
    """Read the bound coordinator witness, including older ID-only records."""
    directory = 'terminal-targets/' + intent + '/' + receipt_id
    paths = ((_root() / directory).glob('*.json') if statement is None
             else (_record_path(directory, statement['attempt']),))
    for path in paths:
        if path.exists():
            record = _read_record(path)
            if (record['intent'] != intent or record['receipt_id'] != receipt_id
                    or not isinstance(record['query_id'], str) or not record['query_id']
                    or record['attempt'] != path.stem or statement is not None and (
                        statement['expected'] != {'single_submit': receipt_id}
                        or statement_query(statement) not in (None, record['query_id']))):
                raise RuntimeError('core terminal target binding differs')
            if 'outcome' in record or 'statement_sha256' in record:
                if (record.get('outcome') != 'FINISHED' or not re.fullmatch('[0-9a-f]{64}', record.get('statement_sha256') or '')
                        or statement is not None and record['statement_sha256'] != hashlib.sha256(
                            _canonical(statement)).hexdigest()):
                    raise RuntimeError('core terminal target binding differs')
            if statement is not None or 'outcome' in record:
                return record
    return None


def remember_terminal_target(intent, receipt_id, statement):
    """Preserve the bound coordinator outcome before unlinking its statement."""
    query_id = statement_query(statement)
    if not query_id or statement['expected'] != {'single_submit': receipt_id}:
        raise RuntimeError('core terminal target binding differs')
    directory = 'terminal-targets/' + intent + '/' + receipt_id
    path = _record_path(directory, statement['attempt'])
    previous = terminal_witness(intent, receipt_id, statement)
    _store_record(path, {
        'intent': intent, 'receipt_id': receipt_id, 'attempt': statement['attempt'], 'query_id': query_id,
        'outcome': 'FINISHED', 'statement_sha256': hashlib.sha256(_canonical(statement)).hexdigest()},
        replace=previous is not None and 'outcome' not in previous)


def stage_record(receipt_id, stage):
    return _record_path('stages/' + receipt_id, stage)


def _idle_stage_path(receipt_id, stage):
    return _record_path('idle-stages/' + receipt_id, stage)


def mark_stage_idle(receipt_id, stage):
    _immutable_record(_idle_stage_path(receipt_id, stage), {'receipt_id': receipt_id, 'stage': stage})


def stage_idle(receipt_id, stage):
    path = _idle_stage_path(receipt_id, stage)
    if not path.exists():
        return False
    if _read_record(path) != {'receipt_id': receipt_id, 'stage': stage}:
        raise RuntimeError('core retained stage identity differs')
    return True


def clear_stage_idle(receipt_id, stage):
    _unlink_record(_idle_stage_path(receipt_id, stage))


def retained_stages(receipt_id):
    for path in sorted((_root() / 'stages' / receipt_id).glob('*.json')):
        yield path, _read_record(path)


def forget_stage(path):
    path.unlink()
    _fsync_directory(path.parent)
    clear_stage_idle(path.parent.name, path.stem)


def read_snapshot_anchors(digest: str) -> dict[str, int | None]:
    """Read the original snapshot boundaries recorded for a unit."""
    path = _record_path('anchors', digest)
    if not path.exists():
        return {}
    payload = _read_record(path)
    tables = payload.get('tables')
    if payload.get('intent_sha256') != digest or not isinstance(tables, dict):
        raise RuntimeError('core journal snapshot anchor differs')
    if any(value is not None and type(value) is not int for value in tables.values()):
        raise RuntimeError('core journal snapshot anchor differs')
    return tables


def _table_not_found(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in ('table_not_found', 'table not found', 'does not exist'))


def snapshot_anchors(digest: str, connection, tables: Iterable[str]) -> dict[str, int | None]:
    """Save the pre-write snapshot for each target exactly once.

    Names may carry a schema prefix (``ops.<table>``); bare names address
    the Bronze schema like the rest of the core writer.
    """
    from .native_writer import _query_rows

    names = set(tables)
    if any(not re.fullmatch(r'(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*', name) for name in names):
        raise ValueError('invalid core snapshot table name')
    path = _record_path('anchors', digest)
    if path.exists():
        anchors = read_snapshot_anchors(digest)
        if anchors.keys() != names:
            raise RuntimeError('core journal snapshot anchor tables differ')
        return anchors

    anchors = {}
    for name in sorted(names):
        schema = name.rsplit('.', 1)[0] if '.' in name else 'bronze'
        table = name.rsplit('.', 1)[-1]
        cursor = connection.cursor()
        try:
            rows = _query_rows(
                cursor,
                f'SELECT snapshot_id FROM iceberg.{schema}."{table}$snapshots" '
                'ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1',
            )
            row = rows[0] if rows else None
        except Exception as exc:
            if not _table_not_found(exc):
                raise
            row = None
        anchors[name] = int(row[0]) if row and row[0] is not None else None
    _immutable_record(path, {'intent_sha256': digest, 'tables': anchors})
    return anchors


def copy_snapshot_anchors(source: str, target: str) -> bool:
    """Publish a unit's pre-write anchors under another identity of that unit.

    An acknowledgement written under one identity and journalled under
    another keeps the anchors recorded before its write; the new identity
    never anchors a later state. Nothing changes when the source has no
    anchors or the target already has its own.
    """
    source_path, target_path = _record_path('anchors', source), _record_path('anchors', target)
    if not source_path.exists() or target_path.exists():
        return False
    _immutable_record(_record_path('anchor-copies', target), {'source': source, 'target': target})
    _immutable_record(target_path, {**_read_record(source_path), 'intent_sha256': target})
    return True


def record_receipt(digest: str, receipt: Mapping) -> None:
    """Persist immutable evidence of a physically verified commit."""
    if receipt.get('verified') is not True:
        raise RuntimeError('core journal receipt is unverified')
    path = _record_path('receipts', digest)
    _immutable_record(path, dict(receipt))
    if _canonical(_read_record(path)) != _canonical(dict(receipt)):
        raise RuntimeError('core journal receipt readback differs')


def guard_inventories():
    for path in (_root() / 'guard-inventories').glob('*.json'):
        inventory = _read_record(path)
        if inventory['intent'] != path.stem:
            raise RuntimeError('core guard inventory identity differs')
        yield inventory


def acknowledgement_matches_inventory(digest, receipt):
    originals = {i['acknowledgement'] for i in guard_inventories() if i['digest'] == digest}
    return not originals or originals == {hashlib.sha256(_canonical(receipt)).hexdigest()}


def acknowledgement_bound(digest, receipt, run=None, completion=None):
    """Authenticate the acknowledgement before using its targets, clocks or release."""
    from .native_writer import _batch_id_for_run, _digest, _receipt_id

    if not receipt or receipt.get('plan_sha256') != digest or run is not None and receipt.get('run_id') != run:
        return False
    warehouse = receipt.get('identity')
    run = receipt.get('run_id')
    if (not isinstance(run, str) or not run or receipt.get('committed_batch') != _batch_id_for_run(run)
            or receipt.get('receipt_readback') is not True):
        return False
    if warehouse is not None:
        if not isinstance(warehouse, dict):
            return False
        hashes = warehouse.get('plan_hashes')
        if (not isinstance(hashes, list) or not hashes
                or warehouse.get('digest') != _digest({'plans': hashes, 'run_id': run})
                or digest != warehouse['digest'] and receipt.get('descriptor_digest', digest) != digest):
            return False
    if completion is None:
        return True
    if completion['digest'] != digest:
        return False
    if 'acknowledgement' in completion:
        if completion['acknowledgement'] != _digest(receipt):
            return False
        descriptor = completion['descriptor_digest']
    else:
        descriptor = digest
    if receipt.get('descriptor_digest', descriptor) != descriptor:
        return False
    if warehouse is None:
        # A Stage-B acknowledgement predates embedded member hashes.
        return 'acknowledgement' in completion
    return completion['owner'] == _receipt_id(hashes[0], run, warehouse['digest'])


def acknowledgements():
    """Read the verified, immutable acknowledgements, including pre-upgrade records."""
    fingerprints = {}
    for inventory in guard_inventories():
        fingerprints.setdefault(inventory['digest'], set()).add(inventory['acknowledgement'])
    completions = {}
    for path in (_root() / 'completed-intents').glob('*.json'):
        completion = completed_intent(path.stem)
        completions.setdefault(completion['digest'], []).append(completion)
    for path in sorted((_root() / 'receipts').glob('*.json')):
        receipt = _read_record(path)
        if receipt.get('verified') is not True:
            raise RuntimeError('core journal receipt is unverified')
        if (not acknowledgement_bound(path.stem, receipt) or any(
                not acknowledgement_bound(path.stem, receipt, completion=c)
                for c in completions.get(path.stem, ()))):
            continue
        if path.stem in fingerprints and fingerprints[path.stem] != {hashlib.sha256(_canonical(receipt)).hexdigest()}:
            continue  # the inventory fences this acknowledgement's original scopes
        yield receipt


def acknowledged_walk(receipt_id: str) -> tuple[Mapping, object] | None:
    """The walk an acknowledgement logged in the warehouse under one receipt.

    The original cells and acknowledgement clock also identify the digest
    format to reproduce when verifying a walk written before an upgrade.
    """
    result = None
    for receipt in acknowledgements():
        for walk in receipt.get('walks') or ():
            if isinstance(walk, Mapping) and walk.get('receipt_id') == receipt_id:
                candidate = (walk, receipt.get('committed_at'))
                if result is not None and result != candidate:
                    raise RuntimeError('core journal walk differs')
                result = candidate
    return result


def scope_walks(table, scope):
    """Find original walks and older plan proofs that still need a warehouse witness."""
    from .native_writer import _receipt_id

    walks, missing = {}, {}
    for receipt in acknowledgements():
        recorded = {}
        for walk in receipt.get('walks') or ():
            if walk['table'] != table:
                continue
            recorded[walk['receipt_id']] = walk['scope_values_json']
            if walk['scope_values_json'] == scope:
                candidate = (walk, receipt['committed_at'])
                if walks.setdefault(walk['receipt_id'], candidate) != candidate:
                    raise RuntimeError('core journal walk differs')
        proofs = [(i, proof) for i, proof in enumerate(receipt.get('plans', ()))
                  if proof['table'] == table.rsplit('.', 1)[-1]]
        identity = receipt.get('identity')
        if not identity:
            if len(proofs) > len(recorded):
                raise RuntimeError('core journal walk lacks warehouse identity')
            continue
        for index, proof in proofs:
            key = _receipt_id(identity['plan_hashes'][index], receipt['run_id'], identity['digest'])
            if key in recorded:
                if proof.get('scope_values_json', recorded[key]) != recorded[key]:
                    raise RuntimeError('core journal walk scope differs')
            elif proof.get('scope_values_json', scope) == scope:
                evidence = (proof.get('scope_values_json'), receipt['committed_at'])
                if missing.setdefault(key, evidence) != evidence:
                    raise RuntimeError('core journal walk differs')
    return walks, missing


def read_receipt(digest: str) -> dict | None:
    """Read a verified acknowledgement when the unit has already committed."""
    path = _record_path('receipts', digest)
    if not path.exists():
        return None
    receipt = _read_record(path)
    if receipt.get('verified') is not True:
        raise RuntimeError('core journal receipt is unverified')
    return receipt
