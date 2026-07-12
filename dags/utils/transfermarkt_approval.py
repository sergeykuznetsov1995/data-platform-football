"""One-shot approval packets for paid Transfermarkt and production actions.

This module deliberately does not execute commands.  It freezes the complete
operator-visible intent into canonical JSON, binds approval to its SHA-256
digest and permits the exact command vector to be consumed once before a
separate caller executes it.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
import threading
from collections.abc import Callable, MutableMapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal


MIB = 1024 * 1024
ApprovalAction = Literal['paid_proxy', 'production_write']
ApprovalStatus = Literal['issued', 'approved', 'consumed', 'failed']


class ApprovalError(RuntimeError):
    """Base class for approval validation and journal failures."""


class ApprovalValidationError(ApprovalError, ValueError):
    """The proposed action is incomplete or cannot be represented safely."""


class ApprovalStateError(ApprovalError):
    """The requested journal transition is not allowed."""


class ApprovalExpiredError(ApprovalStateError):
    """The one-shot approval window has expired."""


class ApprovalDriftError(ApprovalStateError):
    """The presented packet or execution command differs from the approval."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ApprovalValidationError('approval timestamps must be timezone-aware')
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(
        timezone.utc,
    )


def _validate_text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ApprovalValidationError(f'{field} must contain non-blank text')
    if value != value.strip():
        raise ApprovalValidationError(f'{field} must not have outer whitespace')
    if any(character in value for character in ('\x00', '\n', '\r')):
        raise ApprovalValidationError(f'{field} contains a control character')
    return value


def _validate_argv(argv: Sequence[str], *, field: str) -> tuple[str, ...]:
    if isinstance(argv, (str, bytes)) or not argv:
        raise ApprovalValidationError(f'{field} must be a non-empty argv vector')
    result = tuple(
        _validate_text(value, field=f'{field}[{index}]')
        for index, value in enumerate(argv)
    )
    executable = Path(result[0]).name
    if executable in {'sh', 'bash', 'dash', 'zsh', 'fish', 'ksh'}:
        raise ApprovalValidationError(
            f'{field} must disclose argv directly, not invoke a shell',
        )
    for value in result:
        if '$' in value or '`' in value or '<(' in value or '>(' in value:
            raise ApprovalValidationError(
                f'{field} contains an unexpanded shell substitution',
            )
        if value == '~' or value.startswith('~/'):
            raise ApprovalValidationError(
                f'{field} contains an unexpanded home-directory shortcut',
            )
    return result


def _validate_commands(
    commands: Sequence[Sequence[str]],
    *,
    field: str,
) -> tuple[tuple[str, ...], ...]:
    if isinstance(commands, (str, bytes)) or not commands:
        raise ApprovalValidationError(f'{field} must contain at least one command')
    return tuple(
        _validate_argv(command, field=f'{field}[{index}]')
        for index, command in enumerate(commands)
    )


def _unique_texts(values: Sequence[str], *, field: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ApprovalValidationError(f'{field} must be a sequence of values')
    result = tuple(
        _validate_text(value, field=f'{field}[{index}]')
        for index, value in enumerate(values)
    )
    if len(result) != len(set(result)):
        raise ApprovalValidationError(f'{field} contains duplicate values')
    return result


@dataclass(frozen=True)
class ApprovalPacket:
    """Complete immutable intent shown before a paid or production action."""

    packet_id: str
    action: ApprovalAction
    argv: tuple[str, ...]
    byte_cap_bytes: int
    byte_cap_mib: Decimal
    request_limit: int
    retry_limit: int
    concurrency: int
    expected_duration_seconds: int
    affected_tables: tuple[str, ...]
    affected_files: tuple[str, ...]
    stop_conditions: tuple[str, ...]
    backup_commands: tuple[tuple[str, ...], ...]
    rollback_commands: tuple[tuple[str, ...], ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            'packet_id',
            _validate_text(self.packet_id, field='packet_id'),
        )
        if self.action not in ('paid_proxy', 'production_write'):
            raise ApprovalValidationError(f'unsupported action: {self.action!r}')
        object.__setattr__(self, 'argv', _validate_argv(self.argv, field='argv'))

        if isinstance(self.byte_cap_bytes, bool):
            raise ApprovalValidationError('byte_cap_bytes must be an integer')
        try:
            cap_bytes = int(self.byte_cap_bytes)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ApprovalValidationError(
                'byte_cap_bytes must be an integer',
            ) from exc
        if cap_bytes != self.byte_cap_bytes or cap_bytes < 0:
            raise ApprovalValidationError(
                'byte_cap_bytes must be a non-negative integer',
            )
        try:
            cap_mib = Decimal(str(self.byte_cap_mib))
        except (InvalidOperation, ValueError) as exc:
            raise ApprovalValidationError('byte_cap_mib is invalid') from exc
        if not cap_mib.is_finite() or cap_mib < 0:
            raise ApprovalValidationError('byte_cap_mib must be finite and non-negative')
        if cap_mib * MIB != cap_bytes:
            raise ApprovalValidationError(
                'byte_cap_bytes and byte_cap_mib describe different limits',
            )
        if self.action == 'paid_proxy' and cap_bytes == 0:
            raise ApprovalValidationError('paid proxy actions require a positive byte cap')
        object.__setattr__(self, 'byte_cap_bytes', cap_bytes)
        object.__setattr__(self, 'byte_cap_mib', cap_mib.normalize())

        integer_fields = {
            'request_limit': self.request_limit,
            'retry_limit': self.retry_limit,
            'concurrency': self.concurrency,
            'expected_duration_seconds': self.expected_duration_seconds,
        }
        for field, value in integer_fields.items():
            if isinstance(value, bool) or not isinstance(value, int):
                raise ApprovalValidationError(f'{field} must be an integer')
        if self.request_limit < 0:
            raise ApprovalValidationError('request_limit must be non-negative')
        if self.action == 'paid_proxy' and self.request_limit == 0:
            raise ApprovalValidationError(
                'paid proxy actions require a positive request limit',
            )
        if self.retry_limit < 0:
            raise ApprovalValidationError('retry_limit must be non-negative')
        if self.concurrency < 1:
            raise ApprovalValidationError('concurrency must be positive')
        if self.expected_duration_seconds < 1:
            raise ApprovalValidationError(
                'expected_duration_seconds must be positive',
            )

        tables = _unique_texts(self.affected_tables, field='affected_tables')
        files = _unique_texts(self.affected_files, field='affected_files')
        if not tables and not files:
            raise ApprovalValidationError(
                'at least one affected table or file must be declared',
            )
        object.__setattr__(self, 'affected_tables', tables)
        object.__setattr__(self, 'affected_files', files)
        stop_conditions = _unique_texts(
            self.stop_conditions,
            field='stop_conditions',
        )
        if not stop_conditions:
            raise ApprovalValidationError('stop_conditions cannot be empty')
        object.__setattr__(self, 'stop_conditions', stop_conditions)
        object.__setattr__(
            self,
            'backup_commands',
            _validate_commands(self.backup_commands, field='backup_commands'),
        )
        object.__setattr__(
            self,
            'rollback_commands',
            _validate_commands(self.rollback_commands, field='rollback_commands'),
        )

    def payload(self) -> dict[str, Any]:
        """Return the exact JSON-safe structure bound to operator approval."""

        return {
            'action': self.action,
            'affected_files': list(self.affected_files),
            'affected_tables': list(self.affected_tables),
            'argv': list(self.argv),
            'backup_commands': [list(command) for command in self.backup_commands],
            'byte_cap_bytes': self.byte_cap_bytes,
            'byte_cap_mib': format(self.byte_cap_mib, 'f'),
            'concurrency': self.concurrency,
            'expected_duration_seconds': self.expected_duration_seconds,
            'packet_id': self.packet_id,
            'request_limit': self.request_limit,
            'retry_limit': self.retry_limit,
            'rollback_commands': [
                list(command) for command in self.rollback_commands
            ],
            'stop_conditions': list(self.stop_conditions),
        }

    @property
    def canonical_json(self) -> str:
        return json.dumps(
            self.payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        )

    @property
    def packet_hash(self) -> str:
        return hashlib.sha256(self.canonical_json.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class JournalRecord:
    """Persistent evidence for one packet's monotonic state transitions."""

    packet_hash: str
    packet_id: str
    action: ApprovalAction
    status: ApprovalStatus
    canonical_json: str
    issued_at: str
    expires_at: str
    approved_at: str | None = None
    consumed_at: str | None = None
    failed_at: str | None = None
    failure_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> 'JournalRecord':
        return cls(**value)


Storage = MutableMapping[str, dict[str, Any]] | str | os.PathLike[str]


class ApprovalJournal:
    """One-shot journal backed by an injected mapping or an atomic JSON file."""

    def __init__(
        self,
        storage: Storage,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(storage, (MutableMapping, str, os.PathLike)):
            raise TypeError('storage must be a mutable mapping or file path')
        self._mapping = storage if isinstance(storage, MutableMapping) else None
        self._path = None if self._mapping is not None else Path(storage)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._thread_lock = threading.RLock()

    def _now(self) -> datetime:
        return _utc(self._clock())

    @staticmethod
    def _decode_records(value: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(value, dict):
            raise ApprovalStateError('approval journal must contain a JSON object')
        records: dict[str, dict[str, Any]] = {}
        for key, record in value.items():
            if not isinstance(key, str) or not isinstance(record, dict):
                raise ApprovalStateError('approval journal contains an invalid record')
            records[key] = dict(record)
        return records

    def _read_file_unlocked(self) -> dict[str, dict[str, Any]]:
        assert self._path is not None
        if not self._path.exists():
            return {}
        try:
            return self._decode_records(json.loads(self._path.read_text('utf-8')))
        except (OSError, json.JSONDecodeError) as exc:
            raise ApprovalStateError('approval journal file is unreadable') from exc

    def _write_file_unlocked(self, records: dict[str, dict[str, Any]]) -> None:
        assert self._path is not None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f'.{self._path.name}.',
            suffix='.tmp',
            dir=self._path.parent,
        )
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
                json.dump(
                    records,
                    handle,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(',', ':'),
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def _mutate(
        self,
        operation: Callable[[dict[str, dict[str, Any]]], JournalRecord],
    ) -> JournalRecord:
        with self._thread_lock:
            if self._mapping is not None:
                records = self._decode_records(dict(self._mapping))
                result = operation(records)
                self._mapping.clear()
                self._mapping.update(records)
                return result

            assert self._path is not None
            self._path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = self._path.with_name(f'.{self._path.name}.lock')
            with lock_path.open('a+', encoding='utf-8') as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                records = self._read_file_unlocked()
                result = operation(records)
                self._write_file_unlocked(records)
                return result

    def _read(self) -> dict[str, dict[str, Any]]:
        with self._thread_lock:
            if self._mapping is not None:
                return self._decode_records(dict(self._mapping))
            assert self._path is not None
            self._path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = self._path.with_name(f'.{self._path.name}.lock')
            with lock_path.open('a+', encoding='utf-8') as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_SH)
                return self._read_file_unlocked()

    def get(self, packet_hash: str) -> JournalRecord:
        try:
            return JournalRecord.from_dict(self._read()[packet_hash])
        except KeyError as exc:
            raise ApprovalStateError('approval packet was not issued') from exc

    @staticmethod
    def _assert_presented_hash(
        packet: ApprovalPacket,
        presented_hash: str,
    ) -> None:
        if presented_hash != packet.packet_hash:
            raise ApprovalDriftError('presented hash does not match the packet')

    @staticmethod
    def _assert_exact(
        record: JournalRecord,
        packet: ApprovalPacket,
        presented_hash: str,
    ) -> None:
        ApprovalJournal._assert_presented_hash(packet, presented_hash)
        if record.packet_hash != presented_hash:
            raise ApprovalDriftError('journal hash does not match the packet')
        if record.packet_id != packet.packet_id:
            raise ApprovalDriftError('journal packet id does not match the packet')
        if record.canonical_json != packet.canonical_json:
            raise ApprovalDriftError('packet content changed after it was issued')

    @staticmethod
    def _assert_not_expired(record: JournalRecord, now: datetime) -> None:
        if now >= _parse_timestamp(record.expires_at):
            raise ApprovalExpiredError('approval packet has expired')

    def issue(
        self,
        packet: ApprovalPacket,
        *,
        expires_at: datetime,
    ) -> JournalRecord:
        now = self._now()
        expiry = _utc(expires_at)
        if expiry <= now:
            raise ApprovalExpiredError('approval expiry must be in the future')

        def operation(records: dict[str, dict[str, Any]]) -> JournalRecord:
            if packet.packet_hash in records:
                raise ApprovalStateError('approval packet hash cannot be reissued')
            if any(
                record.get('packet_id') == packet.packet_id
                for record in records.values()
            ):
                raise ApprovalStateError('approval packet id cannot be reused')
            result = JournalRecord(
                packet_hash=packet.packet_hash,
                packet_id=packet.packet_id,
                action=packet.action,
                status='issued',
                canonical_json=packet.canonical_json,
                issued_at=_timestamp(now),
                expires_at=_timestamp(expiry),
            )
            records[packet.packet_hash] = result.as_dict()
            return result

        return self._mutate(operation)

    def approve(
        self,
        packet: ApprovalPacket,
        *,
        presented_hash: str,
    ) -> JournalRecord:
        now = self._now()
        self._assert_presented_hash(packet, presented_hash)

        def operation(records: dict[str, dict[str, Any]]) -> JournalRecord:
            try:
                current = JournalRecord.from_dict(records[presented_hash])
            except KeyError as exc:
                raise ApprovalStateError('approval packet was not issued') from exc
            self._assert_exact(current, packet, presented_hash)
            self._assert_not_expired(current, now)
            if current.status != 'issued':
                raise ApprovalStateError(
                    f'cannot approve packet in {current.status!r} state',
                )
            result = replace(
                current,
                status='approved',
                approved_at=_timestamp(now),
            )
            records[presented_hash] = result.as_dict()
            return result

        return self._mutate(operation)

    def consume(
        self,
        packet: ApprovalPacket,
        *,
        presented_hash: str,
        execution_argv: Sequence[str],
    ) -> JournalRecord:
        """Consume approval once after checking the exact command to be run."""

        now = self._now()
        command = _validate_argv(execution_argv, field='execution_argv')
        self._assert_presented_hash(packet, presented_hash)

        def operation(records: dict[str, dict[str, Any]]) -> JournalRecord:
            try:
                current = JournalRecord.from_dict(records[presented_hash])
            except KeyError as exc:
                raise ApprovalStateError('approval packet was not issued') from exc
            self._assert_exact(current, packet, presented_hash)
            self._assert_not_expired(current, now)
            if current.status != 'approved':
                raise ApprovalStateError(
                    f'cannot consume packet in {current.status!r} state',
                )
            if command != packet.argv:
                raise ApprovalDriftError(
                    'execution argv differs from the approved command',
                )
            result = replace(
                current,
                status='consumed',
                consumed_at=_timestamp(now),
            )
            records[presented_hash] = result.as_dict()
            return result

        return self._mutate(operation)

    def fail(
        self,
        packet: ApprovalPacket,
        *,
        presented_hash: str,
        reason: str,
    ) -> JournalRecord:
        """Close a packet permanently, including after a failed command attempt."""

        now = self._now()
        failure_reason = _validate_text(reason, field='reason')
        self._assert_presented_hash(packet, presented_hash)

        def operation(records: dict[str, dict[str, Any]]) -> JournalRecord:
            try:
                current = JournalRecord.from_dict(records[presented_hash])
            except KeyError as exc:
                raise ApprovalStateError('approval packet was not issued') from exc
            self._assert_exact(current, packet, presented_hash)
            if current.status == 'failed':
                raise ApprovalStateError('failed approval packet cannot be reused')
            result = replace(
                current,
                status='failed',
                failed_at=_timestamp(now),
                failure_reason=failure_reason,
            )
            records[presented_hash] = result.as_dict()
            return result

        return self._mutate(operation)
