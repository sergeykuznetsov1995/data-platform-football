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


def _write_once(path: Path, data: bytes, mismatch: str) -> None:
    _ensure_directory(path.parent)
    with _directory_fd(path.parent) as directory:
        # Serialize the existence check and rename, including standalone journal users.
        fcntl.flock(directory, fcntl.LOCK_EX)
        if path.exists():
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


def _immutable_record(path: Path, payload: Mapping) -> None:
    payload = dict(payload)
    checksum = hashlib.sha256(_canonical(payload)).hexdigest()
    data = _canonical({'payload': payload, 'sha256': checksum})
    _write_once(path, data, 'core journal record differs')


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
        if entry['status'] == 'finished':
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


def pending_intents(identity: Mapping) -> list[tuple[Path, dict]]:
    """Read indexed and orphaned pending units that match the requested identity."""
    pending = []
    for entry in _load_index():
        if entry['status'] != 'pending' or not identity.items() <= entry['identity'].items():
            continue
        path = _root() / (entry['digest'] + '.json')
        body = _intent_body(path, 'core write intent checksum differs')
        if body['identity'] != entry['identity']:
            raise RuntimeError('core write intent identity differs')
        pending.append((path, body))
    return sorted(pending, key=lambda pair: pair[0].name)


def finish_intent(path: Path) -> None:
    """Publish completion before removing the verified intent file."""
    _intent_body(path, 'core write intent checksum differs')
    _append_index({'digest': path.stem, 'status': 'finished'})
    path.unlink()


def _record_path(directory: str, digest: str) -> Path:
    if not digest or Path(digest).name != digest:
        raise ValueError('invalid core journal digest')
    return _root() / directory / (digest + '.json')


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


def record_receipt(digest: str, receipt: Mapping) -> None:
    """Persist immutable evidence of a physically verified commit."""
    if receipt.get('verified') is not True:
        raise RuntimeError('core journal receipt is unverified')
    _immutable_record(_record_path('receipts', digest), dict(receipt))


def read_receipt(digest: str) -> dict | None:
    """Read a verified acknowledgement when the unit has already committed."""
    path = _record_path('receipts', digest)
    if not path.exists():
        return None
    receipt = _read_record(path)
    if receipt.get('verified') is not True:
        raise RuntimeError('core journal receipt is unverified')
    return receipt
