"""Process-safe, persistent cooldown for the WhoScored source.

The circuit is intentionally independent from HTTP and parser code.  A caller
asks for a permit immediately before source traffic, then reports one of three
outcomes:

* ``succeed`` closes a matching half-open probe;
* ``trip`` records authoritative direct-browser Cloudflare evidence;
* ``inconclusive``/``abandon`` preserve the cooldown after an uncertain probe.

Missing state means a new closed circuit.  Every persisted transition is made
under a stable ``flock`` and committed with ``fsync`` + ``os.replace``.  The
JSON schema is deliberately tiny and cannot retain URLs, cookies, sessions, or
Airflow request context.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import random
import re
import secrets
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional

from scrapers.whoscored.runtime_contract import require_production_runtime_class


STATE_SCHEMA_VERSION = 1
DEFAULT_COOLDOWN_SECONDS = 15 * 60.0
DEFAULT_COOLDOWN_FACTOR = 2.0
MAX_COOLDOWN_SECONDS = 60 * 60.0
DEFAULT_JITTER_SECONDS = 60.0
DEFAULT_PROBE_LEASE_SECONDS = 180.0
DEFAULT_WAIT_POLL_SECONDS = 1.0

_STATE_FIELDS = frozenset(
    {
        "schema_version",
        "state",
        "generation",
        "level",
        "updated_at",
        "not_before",
        "probe_lease_until",
        "probe_nonce",
    }
)
_STATES = frozenset({"closed", "open", "half_open"})
_NONCE_RE = re.compile(r"\A[0-9a-f]{32}\Z", re.ASCII)
_MAX_STATE_BYTES = 4 * 1024
_MAX_GENERATION = 2**63 - 1
_MAX_LEVEL = 63
_TIMESTAMP_EPSILON = 1e-6


class SourceCircuitError(RuntimeError):
    """Base class with transport-friendly, secret-free classification."""

    kind = "config"
    retryable = False


class SourceCircuitConfigurationError(SourceCircuitError):
    """The circuit cannot safely use its configured path or policy."""


class SourceCircuitStateError(SourceCircuitError):
    """Persisted circuit state is corrupt or cannot be committed safely."""


class SourceCircuitOpen(SourceCircuitError):
    """No source request is admitted until the shared cooldown permits it."""

    kind = "cooldown"
    retryable = True

    def __init__(self, *, state: str, retry_at: float) -> None:
        self.state = state
        self.retry_at = float(retry_at)
        super().__init__(
            "WhoScored source circuit is open; retry after "
            f"{max(0, int(math.ceil(self.retry_at)))} UTC epoch seconds"
        )


@dataclass(frozen=True)
class CircuitPermit:
    """Opaque generation-bound authority for one logical source attempt."""

    generation: int
    probe_nonce: Optional[str] = field(default=None, repr=False)

    @property
    def is_probe(self) -> bool:
        return self.probe_nonce is not None


@dataclass(frozen=True)
class CircuitSnapshot:
    """Safe operational state; the probe nonce is intentionally omitted."""

    state: str
    generation: int
    level: int
    updated_at: float
    not_before: Optional[float]
    probe_lease_until: Optional[float]


@dataclass(frozen=True)
class _CircuitState:
    state: str
    generation: int
    level: int
    updated_at: float
    not_before: Optional[float]
    probe_lease_until: Optional[float]
    probe_nonce: Optional[str] = field(repr=False)

    @classmethod
    def closed(cls, *, generation: int, updated_at: float) -> "_CircuitState":
        return cls(
            state="closed",
            generation=generation,
            level=0,
            updated_at=updated_at,
            not_before=None,
            probe_lease_until=None,
            probe_nonce=None,
        )

    def document(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "state": self.state,
            "generation": self.generation,
            "level": self.level,
            "updated_at": self.updated_at,
            "not_before": self.not_before,
            "probe_lease_until": self.probe_lease_until,
            "probe_nonce": self.probe_nonce,
        }

    def snapshot(self) -> CircuitSnapshot:
        return CircuitSnapshot(
            state=self.state,
            generation=self.generation,
            level=self.level,
            updated_at=self.updated_at,
            not_before=self.not_before,
            probe_lease_until=self.probe_lease_until,
        )


def _finite_number(name: str, value: object, *, minimum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < minimum
    ):
        raise SourceCircuitConfigurationError(
            f"WhoScored source circuit {name} is invalid"
        )
    return float(value)


def _json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


class SharedSourceCircuit:
    """One host-local circuit shared by independent worker processes."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        cooldown_factor: float = DEFAULT_COOLDOWN_FACTOR,
        max_cooldown_seconds: float = MAX_COOLDOWN_SECONDS,
        jitter_seconds: float = DEFAULT_JITTER_SECONDS,
        probe_lease_seconds: float = DEFAULT_PROBE_LEASE_SECONDS,
        wait_poll_seconds: float = DEFAULT_WAIT_POLL_SECONDS,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
        nonce_factory: Callable[[], str] = lambda: secrets.token_hex(16),
    ) -> None:
        require_production_runtime_class(operation="WhoScored source circuit")
        try:
            raw_path = os.fspath(path)
        except TypeError as exc:
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit path is invalid"
            ) from exc
        configured_path = Path(raw_path) if isinstance(raw_path, str) else None
        if (
            not isinstance(raw_path, str)
            or not raw_path
            or "\x00" in raw_path
            or configured_path is None
            or not configured_path.is_absolute()
            or configured_path.name in {"", ".", ".."}
            or ".." in configured_path.parts
        ):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit path must be an absolute file path"
            )
        if not callable(clock) or not callable(sleep) or not callable(random_uniform):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit runtime hooks are invalid"
            )
        if not callable(nonce_factory):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit nonce factory is invalid"
            )
        if not getattr(os, "O_NOFOLLOW", 0) or not getattr(os, "O_DIRECTORY", 0):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit requires O_NOFOLLOW and O_DIRECTORY"
            )

        self.path = configured_path
        self.lock_path = self.path.with_name(self.path.name + ".lock")
        self.cooldown_seconds = _finite_number(
            "cooldown", cooldown_seconds, minimum=0.001
        )
        self.cooldown_factor = _finite_number(
            "cooldown factor", cooldown_factor, minimum=1.0
        )
        self.max_cooldown_seconds = _finite_number(
            "maximum cooldown", max_cooldown_seconds, minimum=0.001
        )
        if self.max_cooldown_seconds < self.cooldown_seconds:
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit maximum cooldown is too small"
            )
        self.jitter_seconds = _finite_number(
            "jitter", jitter_seconds, minimum=0.0
        )
        self.probe_lease_seconds = _finite_number(
            "probe lease", probe_lease_seconds, minimum=0.001
        )
        self.wait_poll_seconds = _finite_number(
            "wait poll", wait_poll_seconds, minimum=0.001
        )
        if not math.isfinite(self.max_cooldown_seconds + self.jitter_seconds):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit maximum cooldown plus jitter is invalid"
            )
        self._clock = clock
        self._sleep = sleep
        self._random_uniform = random_uniform
        self._nonce_factory = nonce_factory
        self._local_lock = threading.Lock()
        self._active_probe: Optional[CircuitPermit] = None

    def admit(self, *, wait: bool = False) -> CircuitPermit:
        """Return a permit, or fail/wait without making a source request."""

        if type(wait) is not bool:
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit wait mode must be boolean"
            )
        while True:
            with self._local_lock:
                permit, retry_at, state_name = self._admit_once()
            if permit is not None:
                return permit
            assert retry_at is not None and state_name is not None
            if not wait:
                raise SourceCircuitOpen(state=state_name, retry_at=retry_at)
            now = self._now()
            remaining = max(0.0, retry_at - now)
            if remaining <= _TIMESTAMP_EPSILON:
                continue
            self._sleep(min(self.wait_poll_seconds, remaining))

    def succeed(self, permit: CircuitPermit) -> CircuitSnapshot:
        """Close one matching half-open probe; stale outcomes are ignored."""

        self._validate_permit(permit)
        with self._local_lock, self._locked_state() as parent_descriptor:
            now = self._now()
            state, _exists = self._read_state_locked(now, parent_descriptor)
            if self._same_probe_identity(state, permit):
                state = _CircuitState.closed(
                    generation=self._next_generation(state.generation),
                    updated_at=now,
                )
                self._write_state_locked(state, parent_descriptor)
            self._forget_probe(permit)
            return state.snapshot()

    def trip(self, permit: CircuitPermit) -> CircuitSnapshot:
        """Open/reopen after authoritative direct-browser Cloudflare evidence."""

        self._validate_permit(permit)
        with self._local_lock, self._locked_state() as parent_descriptor:
            now = self._now()
            state, _exists = self._read_state_locked(now, parent_descriptor)
            if self._same_probe_identity(state, permit):
                state = self._open_state(
                    state,
                    level=min(_MAX_LEVEL, state.level + 1),
                    now=now,
                )
                self._write_state_locked(state, parent_descriptor)
            elif (
                not permit.is_probe
                and state.state == "closed"
                and state.generation == permit.generation
            ):
                state = self._open_state(state, level=0, now=now)
                self._write_state_locked(state, parent_descriptor)
            self._forget_probe(permit)
            return state.snapshot()

    def inconclusive(self, permit: CircuitPermit) -> CircuitSnapshot:
        """Reopen a matching probe at the same level after an uncertain error."""

        return self._reopen_probe(permit, escalate=False)

    def abandon(self, permit: Optional[CircuitPermit] = None) -> CircuitSnapshot:
        """Release a graceful in-flight probe without waiting for lease expiry."""

        selected = permit
        with self._local_lock:
            if selected is None:
                selected = self._active_probe
        if selected is None:
            return self.snapshot()
        return self._reopen_probe(selected, escalate=False)

    def snapshot(self) -> CircuitSnapshot:
        """Read secret-free state without changing the circuit generation."""

        with self._local_lock, self._locked_state() as parent_descriptor:
            state, _exists = self._read_state_locked(
                self._now(), parent_descriptor
            )
            return state.snapshot()

    def _reopen_probe(
        self, permit: CircuitPermit, *, escalate: bool
    ) -> CircuitSnapshot:
        self._validate_permit(permit)
        with self._local_lock, self._locked_state() as parent_descriptor:
            now = self._now()
            state, _exists = self._read_state_locked(now, parent_descriptor)
            if self._same_probe_identity(state, permit):
                level = min(_MAX_LEVEL, state.level + int(escalate))
                state = self._open_state(state, level=level, now=now)
                self._write_state_locked(state, parent_descriptor)
            self._forget_probe(permit)
            return state.snapshot()

    def _admit_once(
        self,
    ) -> tuple[Optional[CircuitPermit], Optional[float], Optional[str]]:
        with self._locked_state() as parent_descriptor:
            now = self._now()
            state, exists = self._read_state_locked(now, parent_descriptor)
            if not exists:
                self._write_state_locked(state, parent_descriptor)

            active = self._active_probe
            if active is not None:
                if self._active_probe_identity(state, active, now=now):
                    assert state.probe_lease_until is not None
                    return None, state.probe_lease_until, state.state
                self._active_probe = None

            if state.state == "closed":
                return CircuitPermit(generation=state.generation), None, None
            if state.state == "open":
                assert state.not_before is not None
                if now + _TIMESTAMP_EPSILON < state.not_before:
                    return None, state.not_before, state.state
                state, permit = self._claim_probe(state, now=now)
                self._write_state_locked(state, parent_descriptor)
                self._active_probe = permit
                return permit, None, None

            assert state.state == "half_open"
            assert state.probe_lease_until is not None
            if now + _TIMESTAMP_EPSILON < state.probe_lease_until:
                return None, state.probe_lease_until, state.state
            # The previous process may have died after claiming the probe.  Its
            # generation/nonce can no longer change this replacement lease.
            state, permit = self._claim_probe(state, now=now)
            self._write_state_locked(state, parent_descriptor)
            self._active_probe = permit
            return permit, None, None

    def _open_state(
        self, state: _CircuitState, *, level: int, now: float
    ) -> _CircuitState:
        try:
            raw_jitter = self._random_uniform(0.0, self.jitter_seconds)
        except Exception as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit jitter generation failed"
            ) from exc
        if (
            isinstance(raw_jitter, bool)
            or not isinstance(raw_jitter, (int, float))
            or not math.isfinite(float(raw_jitter))
            or not 0.0 <= float(raw_jitter) <= self.jitter_seconds
        ):
            raise SourceCircuitStateError(
                "WhoScored source circuit jitter generation is invalid"
            )
        base_delay = self._cooldown_delay(level)
        return _CircuitState(
            state="open",
            generation=self._next_generation(state.generation),
            level=level,
            updated_at=now,
            not_before=self._deadline(
                now,
                base_delay + float(raw_jitter),
                label="cooldown deadline",
            ),
            probe_lease_until=None,
            probe_nonce=None,
        )

    def _cooldown_delay(self, level: int) -> float:
        base_delay = self.cooldown_seconds
        for _ in range(level):
            if base_delay >= self.max_cooldown_seconds / self.cooldown_factor:
                return self.max_cooldown_seconds
            base_delay *= self.cooldown_factor
        return min(base_delay, self.max_cooldown_seconds)

    def _claim_probe(
        self, state: _CircuitState, *, now: float
    ) -> tuple[_CircuitState, CircuitPermit]:
        try:
            nonce = self._nonce_factory()
        except Exception as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit probe nonce generation failed"
            ) from exc
        if type(nonce) is not str or _NONCE_RE.fullmatch(nonce) is None:
            raise SourceCircuitStateError(
                "WhoScored source circuit probe nonce is invalid"
            )
        generation = self._next_generation(state.generation)
        claimed = _CircuitState(
            state="half_open",
            generation=generation,
            level=state.level,
            updated_at=now,
            not_before=state.not_before,
            probe_lease_until=self._deadline(
                now, self.probe_lease_seconds, label="probe lease deadline"
            ),
            probe_nonce=nonce,
        )
        return claimed, CircuitPermit(generation=generation, probe_nonce=nonce)

    @staticmethod
    def _validate_permit(permit: CircuitPermit) -> None:
        if (
            type(permit) is not CircuitPermit
            or isinstance(permit.generation, bool)
            or not isinstance(permit.generation, int)
            or not 0 <= permit.generation <= _MAX_GENERATION
            or (
                permit.probe_nonce is not None
                and (
                    type(permit.probe_nonce) is not str
                    or _NONCE_RE.fullmatch(permit.probe_nonce) is None
                )
            )
        ):
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit permit is invalid"
            )

    @staticmethod
    def _same_probe_identity(
        state: _CircuitState,
        permit: CircuitPermit,
    ) -> bool:
        """Match an outcome until another caller replaces its generation.

        The lease bounds *admission*, not an already-running request.  A slow
        but genuine result must still close/reopen the exact half-open probe
        unless a replacement probe has already advanced the generation.
        """

        return bool(
            permit.is_probe
            and state.state == "half_open"
            and state.generation == permit.generation
            and state.probe_nonce == permit.probe_nonce
        )

    @classmethod
    def _active_probe_identity(
        cls,
        state: _CircuitState,
        permit: CircuitPermit,
        *,
        now: float,
    ) -> bool:
        return bool(
            cls._same_probe_identity(state, permit)
            and state.probe_lease_until is not None
            and now <= state.probe_lease_until + _TIMESTAMP_EPSILON
        )

    def _forget_probe(self, permit: CircuitPermit) -> None:
        if self._active_probe == permit:
            self._active_probe = None

    @staticmethod
    def _next_generation(generation: int) -> int:
        if generation >= _MAX_GENERATION:
            raise SourceCircuitStateError(
                "WhoScored source circuit generation is exhausted"
            )
        return generation + 1

    def _now(self) -> float:
        try:
            now = self._clock()
        except Exception as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit clock failed"
            ) from exc
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
            or float(now) < 0
        ):
            raise SourceCircuitStateError(
                "WhoScored source circuit clock is invalid"
            )
        return float(now)

    @staticmethod
    def _deadline(now: float, delay: float, *, label: str) -> float:
        deadline = now + delay
        if not math.isfinite(deadline) or deadline < now:
            raise SourceCircuitStateError(
                f"WhoScored source circuit {label} is invalid"
            )
        return deadline

    def _read_state_locked(
        self, now: float, parent_descriptor: int
    ) -> tuple[_CircuitState, bool]:
        try:
            metadata = os.stat(
                self.path.name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return _CircuitState.closed(generation=0, updated_at=now), False
        except OSError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit state metadata is unavailable"
            ) from exc
        self._validate_named_private_file(self.path, metadata)

        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            descriptor = os.open(
                self.path.name,
                flags,
                dir_fd=parent_descriptor,
            )
        except OSError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit state cannot be opened safely"
            ) from exc
        try:
            current = os.fstat(descriptor)
            self._validate_open_private_file(current)
            if (metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino):
                raise SourceCircuitStateError(
                    "WhoScored source circuit state changed during open"
                )
            payload = bytearray()
            while len(payload) <= _MAX_STATE_BYTES:
                chunk = os.read(descriptor, _MAX_STATE_BYTES + 1 - len(payload))
                if not chunk:
                    break
                payload.extend(chunk)
        except OSError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit state cannot be read"
            ) from exc
        finally:
            os.close(descriptor)
        if not payload or len(payload) > _MAX_STATE_BYTES:
            raise SourceCircuitStateError(
                "WhoScored source circuit state size is invalid"
            )
        try:
            document = json.loads(
                bytes(payload).decode("utf-8"),
                object_pairs_hook=_json_object_without_duplicates,
            )
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit state JSON is invalid"
            ) from exc
        return self._state_from_document(document), True

    def _state_from_document(self, document: object) -> _CircuitState:
        if not isinstance(document, Mapping) or set(document) != _STATE_FIELDS:
            raise SourceCircuitStateError(
                "WhoScored source circuit state schema is invalid"
            )
        if document.get("schema_version") != STATE_SCHEMA_VERSION:
            raise SourceCircuitStateError(
                "WhoScored source circuit state version is invalid"
            )
        state_name = document.get("state")
        generation = document.get("generation")
        level = document.get("level")
        if (
            type(state_name) is not str
            or state_name not in _STATES
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or not 0 <= generation <= _MAX_GENERATION
            or isinstance(level, bool)
            or not isinstance(level, int)
            or not 0 <= level <= _MAX_LEVEL
        ):
            raise SourceCircuitStateError(
                "WhoScored source circuit state identity is invalid"
            )
        updated_at = self._state_timestamp(document.get("updated_at"), nullable=False)
        not_before = self._state_timestamp(document.get("not_before"), nullable=True)
        probe_lease_until = self._state_timestamp(
            document.get("probe_lease_until"), nullable=True
        )
        probe_nonce = document.get("probe_nonce")

        if state_name == "closed":
            valid = (
                level == 0
                and not_before is None
                and probe_lease_until is None
                and probe_nonce is None
            )
        elif state_name == "open":
            base_delay = self._cooldown_delay(level)
            valid = (
                not_before is not None
                and not_before + _TIMESTAMP_EPSILON
                >= updated_at + base_delay
                and not_before
                <= updated_at + base_delay + self.jitter_seconds + _TIMESTAMP_EPSILON
                and probe_lease_until is None
                and probe_nonce is None
            )
        else:
            valid = (
                not_before is not None
                and not_before <= updated_at + _TIMESTAMP_EPSILON
                and probe_lease_until is not None
                and probe_lease_until + _TIMESTAMP_EPSILON
                >= updated_at + self.probe_lease_seconds
                and probe_lease_until
                <= updated_at + self.probe_lease_seconds + _TIMESTAMP_EPSILON
                and type(probe_nonce) is str
                and _NONCE_RE.fullmatch(probe_nonce) is not None
            )
        if not valid:
            raise SourceCircuitStateError(
                "WhoScored source circuit state transition is invalid"
            )
        return _CircuitState(
            state=state_name,
            generation=generation,
            level=level,
            updated_at=updated_at,
            not_before=not_before,
            probe_lease_until=probe_lease_until,
            probe_nonce=probe_nonce,
        )

    @staticmethod
    def _state_timestamp(value: object, *, nullable: bool) -> Optional[float]:
        if nullable and value is None:
            return None
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            raise SourceCircuitStateError(
                "WhoScored source circuit state timestamp is invalid"
            )
        return float(value)

    def _write_state_locked(
        self, state: _CircuitState, parent_descriptor: int
    ) -> None:
        payload = (
            json.dumps(
                state.document(),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        if len(payload) > _MAX_STATE_BYTES:
            raise SourceCircuitStateError(
                "WhoScored source circuit state is too large"
            )
        temporary_name = f".{self.path.name}.{secrets.token_hex(16)}.tmp"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | os.O_NOFOLLOW
        )
        descriptor: Optional[int] = None
        try:
            descriptor = os.open(
                temporary_name,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
            os.fchmod(descriptor, 0o600)
            pending = memoryview(payload)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("state write made no progress")
                pending = pending[written:]
            os.fsync(descriptor)
            temporary_metadata = os.fstat(descriptor)
            self._validate_open_private_file(temporary_metadata)
            self._validate_parent_identity(parent_descriptor)
            os.replace(
                temporary_name,
                self.path.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            os.fsync(parent_descriptor)
            committed_metadata = os.stat(
                self.path.name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            self._validate_named_private_file(self.path, committed_metadata)
            if (temporary_metadata.st_dev, temporary_metadata.st_ino) != (
                committed_metadata.st_dev,
                committed_metadata.st_ino,
            ):
                raise SourceCircuitStateError(
                    "WhoScored source circuit state changed during commit"
                )
            self._validate_parent_identity(parent_descriptor)
        except (OSError, SourceCircuitStateError) as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit state commit failed"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=parent_descriptor)
            except FileNotFoundError:
                pass
            except OSError:
                # The state commit already succeeded or raised its primary
                # fail-closed error.  A private orphan temp is harmless.
                pass

    @contextmanager
    def _locked_state(self) -> Iterator[int]:
        parent_descriptor = self._open_parent(create=True)
        flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
        lock_descriptor: Optional[int] = None
        try:
            self._validate_parent_identity(parent_descriptor)
            try:
                lock_descriptor = os.open(
                    self.lock_path.name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_descriptor,
                )
                os.fchmod(lock_descriptor, 0o600)
            except FileExistsError:
                lock_descriptor = os.open(
                    self.lock_path.name,
                    flags,
                    dir_fd=parent_descriptor,
                )
            self._validate_open_private_file(os.fstat(lock_descriptor))
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
            metadata = os.stat(
                self.lock_path.name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            current = os.fstat(lock_descriptor)
            self._validate_named_private_file(self.lock_path, metadata)
            if (metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino):
                raise SourceCircuitStateError(
                    "WhoScored source circuit lock changed during open"
                )
            self._validate_parent_identity(parent_descriptor)
            yield parent_descriptor
            self._validate_parent_identity(parent_descriptor)
        except SourceCircuitError:
            raise
        except OSError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit lock is unavailable"
            ) from exc
        finally:
            try:
                if lock_descriptor is not None:
                    try:
                        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(lock_descriptor)
            finally:
                os.close(parent_descriptor)

    def _open_parent(self, *, create: bool) -> int:
        """Open the configured parent without ever following an ancestor link."""

        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
        current_descriptor: Optional[int] = None
        try:
            current_descriptor = os.open(os.sep, flags)
            for component in self.path.parent.parts[1:]:
                created = False
                next_descriptor: Optional[int] = None
                try:
                    try:
                        next_descriptor = os.open(
                            component,
                            flags,
                            dir_fd=current_descriptor,
                        )
                    except FileNotFoundError:
                        if not create:
                            raise
                        try:
                            os.mkdir(component, 0o700, dir_fd=current_descriptor)
                            created = True
                        except FileExistsError:
                            pass
                        next_descriptor = os.open(
                            component,
                            flags,
                            dir_fd=current_descriptor,
                        )
                    if created:
                        os.fchmod(next_descriptor, 0o700)
                except Exception:
                    if next_descriptor is not None:
                        os.close(next_descriptor)
                    raise
                os.close(current_descriptor)
                current_descriptor = next_descriptor
            assert current_descriptor is not None
            return current_descriptor
        except OSError as exc:
            if current_descriptor is not None:
                os.close(current_descriptor)
            raise SourceCircuitConfigurationError(
                "WhoScored source circuit parent is unavailable"
            ) from exc

    def _validate_parent_identity(self, parent_descriptor: int) -> None:
        """Verify that the pinned directory is still at the configured path."""

        try:
            configured_descriptor = self._open_parent(create=False)
        except SourceCircuitConfigurationError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit parent changed during transaction"
            ) from exc
        try:
            pinned = os.fstat(parent_descriptor)
            configured = os.fstat(configured_descriptor)
            if (pinned.st_dev, pinned.st_ino) != (
                configured.st_dev,
                configured.st_ino,
            ):
                raise SourceCircuitStateError(
                    "WhoScored source circuit parent changed during transaction"
                )
        except OSError as exc:
            raise SourceCircuitStateError(
                "WhoScored source circuit parent identity is unavailable"
            ) from exc
        finally:
            os.close(configured_descriptor)

    @staticmethod
    def _validate_open_private_file(metadata: os.stat_result) -> None:
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise SourceCircuitStateError(
                "WhoScored source circuit file is not private and regular"
            )

    @classmethod
    def _validate_named_private_file(
        cls, path: Path, metadata: os.stat_result
    ) -> None:
        if stat.S_ISLNK(metadata.st_mode):
            raise SourceCircuitStateError(
                "WhoScored source circuit symlink is forbidden"
            )
        cls._validate_open_private_file(metadata)


__all__ = [
    "CircuitPermit",
    "CircuitSnapshot",
    "DEFAULT_COOLDOWN_FACTOR",
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_JITTER_SECONDS",
    "DEFAULT_PROBE_LEASE_SECONDS",
    "MAX_COOLDOWN_SECONDS",
    "STATE_SCHEMA_VERSION",
    "SharedSourceCircuit",
    "SourceCircuitConfigurationError",
    "SourceCircuitError",
    "SourceCircuitOpen",
    "SourceCircuitStateError",
]
