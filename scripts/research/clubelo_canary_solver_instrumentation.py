"""Local, fail-closed observation of isolated FlareSolverr replicas.

Use a private, pre-existing directory per run. Publish ``write_active_run``
before submitting the request, then install on the upstream/root logger before
its non-forced basicConfig. No log content is written to disk. An error marker
invalidates the directory permanently; use a fresh directory for another run.

Attempt correlation requires numeric ``started_epoch`` and ``finished_epoch``
boundaries, including the final timeout. Missing observations are unknown.
Solver success is independent of response/clearance validation.
"""

from contextlib import contextmanager
import fcntl
import json
import logging
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
import uuid


ACTIVE_FILE = "active_run.json"
EVENT_FILE = "solver_events.jsonl"
ERROR_FILE = "observer_error.json"
ACTIVE_SCHEMA = "clubelo.canary.active_run.v1"
EVENT_SCHEMA = "clubelo.canary.solver_event.v1"
ERROR_SCHEMA = "clubelo.canary.observer_error.v1"
INVALID = "invalid solver observation"
_ACTIVE_KEYS = {"schema", "run_id", "arm", "started", "expires", "active"}
_EVENT_KEYS = {"schema", "sequence", "run_id", "arm", "kind", "observed_epoch"}
_INSTALL_LOCK = threading.RLock()


def _invalid():
    return ValueError(INVALID)


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _run_id(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is not None


def _arm(value):
    return value in ("control", "candidate")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _invalid()
        result[key] = value
    return result


def _decode(data):
    return json.loads(data.decode("utf-8"), object_pairs_hook=_pairs)


def _encode(value):
    return (json.dumps(value, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")


@contextmanager
def _directory(state_dir):
    # Walk with directory descriptors, refusing symlinks in every component.
    path = Path(os.path.abspath(os.fspath(state_dir)))
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        info = os.fstat(fd)
        euid = os.geteuid()
        if (info.st_uid != euid and not (euid == 0 and info.st_uid == 1000)) or info.st_mode & 0o022:
            raise _invalid()
        identities = set()
        for name in (ACTIVE_FILE, EVENT_FILE, ERROR_FILE):
            try:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            identity = (info.st_dev, info.st_ino)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or identity in identities:
                raise _invalid()
            identities.add(identity)
        yield fd
    finally:
        os.close(fd)


def _open(fd, name, flags):
    result = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=fd)
    info = os.fstat(result)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        os.close(result)
        raise _invalid()
    return result


def _read(fd, name):
    with os.fdopen(_open(fd, name, os.O_RDONLY), "rb") as stream:
        return stream.read()


def _no_error(fd):
    try:
        os.stat(ERROR_FILE, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise _invalid()


def _atomic(fd, name, value):
    temporary = ".observer-" + uuid.uuid4().hex
    try:
        with os.fdopen(_open(fd, temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
            directory_info = os.fstat(fd)
            file_info = os.fstat(stream.fileno())
            if (file_info.st_uid, file_info.st_gid) != (directory_info.st_uid, directory_info.st_gid):
                os.fchown(stream.fileno(), directory_info.st_uid, directory_info.st_gid)
            stream.write(_encode(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=fd)
        except FileNotFoundError:
            pass


def _active(value, arm, observed):
    if (type(value) is not dict or set(value) != _ACTIVE_KEYS
            or value["schema"] != ACTIVE_SCHEMA or not _run_id(value["run_id"])
            or not _arm(value["arm"]) or value["arm"] != arm
            or value["active"] is not True
            or not _number(value["started"]) or not _number(value["expires"])
            or not _number(observed)
            or not value["started"] <= observed <= value["expires"]):
        raise _invalid()
    return value


def write_active_run(state_dir, run_id, arm, started, expires):
    """Atomically publish the controller's run lease; never clear an error."""
    try:
        value = dict(schema=ACTIVE_SCHEMA, run_id=run_id, arm=arm,
                     started=started, expires=expires, active=True)
        _active(value, arm, started)
        with _directory(state_dir) as fd:
            _no_error(fd)
            try:
                existing = _decode(_read(fd, ACTIVE_FILE))
            except FileNotFoundError:
                pass
            else:
                _active(existing, arm, started)
                if existing["run_id"] != run_id:
                    raise _invalid()
            try:
                for event in _parse_events(_read(fd, EVENT_FILE), run_id, arm):
                    _active(value, arm, event["observed_epoch"])
            except FileNotFoundError:
                pass
            _atomic(fd, ACTIVE_FILE, value)
    except Exception:
        raise _invalid() from None


def _validate_events(events, run_id, arm=None):
    if not _run_id(run_id):
        raise _invalid()
    previous_time = None
    pending = False
    for sequence, event in enumerate(events, 1):
        if (type(event) is not dict or set(event) != _EVENT_KEYS
                or event["schema"] != EVENT_SCHEMA or event["run_id"] != run_id
                or type(event["sequence"]) is not int or event["sequence"] != sequence
                or not _arm(event["arm"]) or not _number(event["observed_epoch"])
                or event["kind"] not in ("detected", "solved", "not_detected")):
            raise _invalid()
        if arm is None:
            arm = event["arm"]
        observed = event["observed_epoch"]
        if event["arm"] != arm or (previous_time is not None and observed < previous_time):
            raise _invalid()
        previous_time = observed
        if event["kind"] == "solved":
            if not pending:
                raise _invalid()
            pending = False
        else:
            pending = event["kind"] == "detected"
    return events


def _parse_events(data, run_id, arm=None):
    if data and not data.endswith(b"\n"):
        raise _invalid()
    return _validate_events([_decode(line) for line in data.splitlines()], run_id, arm)


def read_events(state_dir, run_id):
    """Read the entire dedicated journal strictly; absent events mean unknown."""
    try:
        if not _run_id(run_id):
            raise _invalid()
        with _directory(state_dir) as fd:
            _no_error(fd)
            active = _decode(_read(fd, ACTIVE_FILE))
            _active(active, active.get("arm"), active.get("started"))
            if active["run_id"] != run_id:
                raise _invalid()
            try:
                event_fd = _open(fd, EVENT_FILE, os.O_RDONLY)
            except FileNotFoundError:
                return []
            with os.fdopen(event_fd, "rb") as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                events = _parse_events(stream.read(), run_id, active["arm"])
                for event in events:
                    _active(active, active["arm"], event["observed_epoch"])
            _no_error(fd)
            return events
    except Exception:
        raise _invalid() from None


def _kind(message):
    if message.startswith(("Challenge detected. Title found:", "Challenge detected. Selector found:")):
        return "detected"
    return {"Challenge solved!": "solved", "Challenge not detected!": "not_detected"}.get(message)


class SolverEventHandler(logging.Handler):
    """Only approved message shapes become redacted events on durable storage."""

    def __init__(self, state_dir, arm, *, clock=time.time):
        super().__init__()
        if not _arm(arm):
            raise _invalid()
        self.state_dir = Path(os.path.abspath(os.fspath(state_dir)))
        try:
            with _directory(self.state_dir) as fd:
                _no_error(fd)
        except Exception:
            raise _invalid() from None
        self.arm = arm
        self.clock = clock
        self.invalid = False
        self._stopped = False

    def _fail(self, reason):
        self.invalid = True
        try:
            with _directory(self.state_dir) as fd:
                try:
                    os.stat(ERROR_FILE, dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    _atomic(fd, ERROR_FILE, dict(schema=ERROR_SCHEMA, version=1, reason=reason))
        except Exception:
            # A failed marker cannot be silently mistaken for a clean readback.
            raise _invalid() from None

    def emit(self, record):
        if self._stopped:
            return
        reason = "invalid_record"
        try:
            kind = _kind(record.getMessage())
            if kind is None:
                return
            reason = "invalid_state"
            with _directory(self.state_dir) as fd:
                observed = self.clock()
                active = _active(_decode(_read(fd, ACTIVE_FILE)), self.arm, observed)
                _no_error(fd)
                if self.invalid:
                    raise _invalid()
                reason = "invalid_events"
                with os.fdopen(_open(fd, EVENT_FILE, os.O_RDWR | os.O_CREAT | os.O_APPEND), "a+b") as stream:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    stream.seek(0)
                    events = _parse_events(stream.read(), active["run_id"], self.arm)
                    for previous in events:
                        _active(active, self.arm, previous["observed_epoch"])
                    event = dict(schema=EVENT_SCHEMA, sequence=len(events) + 1,
                                 run_id=active["run_id"], arm=self.arm, kind=kind,
                                 observed_epoch=observed)
                    _validate_events(events + [event], active["run_id"], self.arm)
                    reason = "io_error"
                    stream.seek(0, os.SEEK_END)
                    stream.write(_encode(event))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(fd)
        except Exception:
            self._fail(reason)

    def close(self):
        self.acquire()
        try:
            self._stopped = True
            super().close()
        finally:
            self.release()


def install(state_dir, arm, *, logger=None, clock=time.time):
    """Idempotently attach before upstream non-forced logging.basicConfig."""
    logger = logging.getLogger() if logger is None else logger
    path = Path(os.path.abspath(os.fspath(state_dir)))
    with _INSTALL_LOCK:
        for handler in logger.handlers:
            if isinstance(handler, SolverEventHandler) and handler.state_dir == path and not handler._stopped:
                if handler.arm != arm:
                    raise _invalid()
                return handler
        handler = SolverEventHandler(path, arm, clock=clock)
        handler._installed_logger = logger
        handler._previous_level = logger.level
        handler._changed_level = logger.getEffectiveLevel() > logging.DEBUG
        if handler._changed_level:
            logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return handler


def uninstall(handler, *, logger=None):
    """Detach and close without threads, queues, or open journal descriptors."""
    logger = getattr(handler, "_installed_logger", logging.getLogger()) if logger is None else logger
    with _INSTALL_LOCK:
        logger.removeHandler(handler)
        if (getattr(handler, "_changed_level", False) and logger.level == logging.DEBUG
                and not any(isinstance(item, SolverEventHandler) for item in logger.handlers)):
            logger.setLevel(handler._previous_level)
        handler.close()


def join_attempt_events(attempts, events, run_id):
    """Join closed epoch intervals; unknown is distinct from not_detected.

    Results retain each input attempt (including timeout/validation outcomes).
    ``solved`` is True/False for observed attempts and None for unknown ones.
    Boundary events belonging to two attempts are ambiguous and rejected.
    """
    try:
        events = _validate_events(list(events), run_id)
        ordered = []
        for attempt in attempts:
            if not isinstance(attempt, dict) or attempt.get("run_id", run_id) != run_id:
                raise _invalid()
            start, finish = attempt["started_epoch"], attempt["finished_epoch"]
            if not _number(start) or not _number(finish) or finish < start:
                raise _invalid()
            ordered.append((start, finish, dict(attempt)))
        ordered.sort(key=lambda item: (item[0], item[1]))
        for previous, current in zip(ordered, ordered[1:]):
            if current[0] < previous[1] or current[:2] == previous[:2]:
                raise _invalid()
        results = [dict(attempt=item[2], detection="unknown", solved=None) for item in ordered]
        for event in events:
            matches = [index for index, (start, finish, _) in enumerate(ordered)
                       if start <= event["observed_epoch"] <= finish]
            if len(matches) != 1:
                raise _invalid()
            result = results[matches[0]]
            if event["kind"] == "solved":
                if result["detection"] != "detected" or result["solved"] is True:
                    raise _invalid()
                result["solved"] = True
            else:
                if result["detection"] != "unknown":
                    raise _invalid()
                result.update(detection=event["kind"], solved=False)
        return dict(run_id=run_id, attempts=results, totals=dict(
            detected=sum(item["detection"] == "detected" for item in results),
            solved=sum(item["solved"] is True for item in results),
            unknown=sum(item["detection"] == "unknown" for item in results)))
    except Exception:
        raise _invalid() from None
