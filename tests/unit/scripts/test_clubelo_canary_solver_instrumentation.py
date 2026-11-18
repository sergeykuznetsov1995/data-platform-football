"""Synthetic, local-only observer coverage; no upstream service is required."""

import json
import logging
import os
import stat

import pytest

from scripts.research import clubelo_canary_solver_instrumentation as observer


@pytest.fixture
def state(tmp_path):
    directory = tmp_path / "state"
    directory.mkdir(mode=0o700)
    observer.write_active_run(directory, "run-1", "control", 100, 200)
    return directory


@pytest.fixture
def capture(state):
    logger = logging.Logger("isolated-solver", level=logging.WARNING)
    logger.propagate = False
    now = [110]
    handler = observer.install(state, "control", logger=logger, clock=lambda: now[0])
    yield logger, handler, now
    observer.uninstall(handler, logger=logger)


def event(sequence=1, kind="detected", observed=110, **extra):
    value = dict(schema=observer.EVENT_SCHEMA, sequence=sequence, run_id="run-1",
                 arm="control", kind=kind, observed_epoch=observed)
    value.update(extra)
    return value


def journal(state, values):
    (state / observer.EVENT_FILE).write_text(
        "".join(json.dumps(value) + "\n" for value in values), encoding="utf-8")


def attempt(start=100, finish=120, **extra):
    return dict(started_epoch=start, finished_epoch=finish, **extra)


def assert_invalid(call):
    with pytest.raises(ValueError, match="^invalid solver observation$"):
        call()


@pytest.mark.parametrize("message,kind", [
    ("Challenge detected. Title found: synthetic-title", "detected"),
    ("Challenge detected. Selector found: #synthetic-selector", "detected"),
    ("Challenge detected. Title found:", "detected"),
    ("Challenge not detected!", "not_detected"),
])
def test_exact_mapping(state, capture, message, kind):
    logger, _, _ = capture
    logger.info(message)
    assert observer.read_events(state, "run-1") == [event(kind=kind)]


@pytest.mark.parametrize("message", [
    "Challenge solved! ", " Challenge solved!", "Challenge solved!\n",
    "Challenge not detected! extra", "Challenge detected.",
    "Challenge detected. Title found", "challenge solved!",
    "prefix Challenge detected. Selector found: #example", "unrelated error",
])
def test_unaccepted_messages_are_ignored(state, capture, message):
    logger, handler, _ = capture
    (state / observer.ACTIVE_FILE).unlink()
    logger.warning(message)
    assert not handler.invalid
    assert not (state / observer.EVENT_FILE).exists()
    assert not (state / observer.ERROR_FILE).exists()


def test_redaction_and_solved_mapping(state, capture):
    logger, _, now = capture
    sensitive = "https://synthetic.invalid/?cookie=sample <html> synthetic-UA #selector"
    logger.info("Challenge detected. Title found: %s", sensitive)
    now[0] = 111
    logger.error("Challenge solved!", exc_info=(ValueError, ValueError(sensitive), None))
    events = observer.read_events(state, "run-1")
    assert events == [event(), event(2, "solved", 111)]
    persisted = (state / observer.EVENT_FILE).read_text()
    assert sensitive not in persisted
    assert "Challenge" not in persisted
    assert all(set(item) == observer._EVENT_KEYS for item in events)


def test_install_is_idempotent_and_uninstall_is_bounded(state, capture):
    logger, handler, _ = capture
    assert observer.install(state, "control", logger=logger) is handler
    assert logger.handlers == [handler]
    assert_invalid(lambda: observer.install(state, "candidate", logger=logger))
    observer.uninstall(handler)
    observer.uninstall(handler)
    assert logger.handlers == []
    assert logger.level == logging.WARNING
    handler.emit(logging.makeLogRecord({"msg": "Challenge solved!"}))
    assert not (state / observer.EVENT_FILE).exists()


def test_survives_nonforced_basic_config_at_info_level(state, monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    original_level = root.level
    root.setLevel(logging.WARNING)
    handler = observer.install(state, "control", clock=lambda: 110)
    try:
        logging.basicConfig(level=logging.INFO)
        assert root.handlers == [handler]
        root.info("Challenge not detected!")
        assert observer.read_events(state, "run-1") == [event(kind="not_detected")]
    finally:
        observer.uninstall(handler)
        root.setLevel(original_level)


@pytest.mark.parametrize("mutation", [
    "missing", "malformed", "expired", "future", "wrong_arm", "inactive",
    "extra", "bad_schema", "unsafe_id", "boolean_time", "nonfinite", "duplicate_key",
])
def test_invalid_active_state_latches_safe_marker(state, capture, mutation):
    path = state / observer.ACTIVE_FILE
    value = json.loads(path.read_text())
    changes = {
        "expired": {"expires": 109}, "future": {"started": 111},
        "wrong_arm": {"arm": "candidate"}, "inactive": {"active": False},
        "extra": {"title": "synthetic-private"}, "bad_schema": {"schema": "v0"},
        "unsafe_id": {"run_id": "../synthetic"}, "boolean_time": {"started": True},
        "nonfinite": {"expires": float("inf")},
    }
    if mutation == "missing":
        path.unlink()
    elif mutation == "malformed":
        path.write_text("{synthetic-private")
    elif mutation == "duplicate_key":
        path.write_text(json.dumps(value)[:-1] + ',"active":true}')
    else:
        value.update(changes[mutation])
        path.write_text(json.dumps(value))
    logger, handler, _ = capture
    logger.info("Challenge detected. Title found: synthetic-private")
    assert handler.invalid
    marker = state / observer.ERROR_FILE
    original = marker.read_bytes()
    assert len(original) < 200
    assert json.loads(original) == dict(schema=observer.ERROR_SCHEMA, version=1, reason="invalid_state")
    assert b"synthetic-private" not in original
    path.write_text(json.dumps(dict(schema=observer.ACTIVE_SCHEMA, run_id="run-1",
                                    arm="control", started=100, expires=200, active=True)))
    logger.info("Challenge not detected!")
    assert marker.read_bytes() == original
    assert not (state / observer.EVENT_FILE).exists()
    assert_invalid(lambda: observer.read_events(state, "run-1"))
    assert_invalid(lambda: observer.write_active_run(state, "run-1", "control", 100, 200))


def test_active_state_is_rechecked_on_every_message(state, capture):
    logger, handler, now = capture
    logger.info("Challenge detected. Selector found: #synthetic")
    now[0] = 201
    logger.info("Challenge solved!")
    assert handler.invalid
    assert len((state / observer.EVENT_FILE).read_text().splitlines()) == 1
    assert_invalid(lambda: observer.read_events(state, "run-1"))


def test_changed_active_file_is_reloaded_without_clock_change(state, capture):
    logger, handler, _ = capture
    logger.info("Challenge detected. Title found: synthetic")
    path = state / observer.ACTIVE_FILE
    value = json.loads(path.read_text())
    value["active"] = False
    path.write_text(json.dumps(value))
    logger.info("Challenge solved!")
    assert handler.invalid
    assert len((state / observer.EVENT_FILE).read_text().splitlines()) == 1
    assert_invalid(lambda: observer.read_events(state, "run-1"))


def test_candidate_arm_is_supported(tmp_path):
    observer.write_active_run(tmp_path, "run-1", "candidate", 100, 200)
    logger = logging.Logger("candidate")
    handler = observer.install(tmp_path, "candidate", logger=logger, clock=lambda: 110)
    try:
        logger.info("Challenge not detected!")
        assert observer.read_events(tmp_path, "run-1") == [event(kind="not_detected", arm="candidate")]
    finally:
        observer.uninstall(handler)


def test_sequence_resumes_and_endpoints_are_inclusive(state):
    logger = logging.Logger("resume")
    first = observer.install(state, "control", logger=logger, clock=lambda: 100)
    logger.info("Challenge detected. Title found: synthetic")
    observer.uninstall(first)
    second = observer.install(state, "control", logger=logger, clock=lambda: 200)
    try:
        logger.info("Challenge solved!")
        assert observer.read_events(state, "run-1") == [event(observed=100), event(2, "solved", 200)]
    finally:
        observer.uninstall(second)


def test_controller_publishes_atomically(state, monkeypatch):
    original = observer.os.replace
    seen = []

    def replace(source, destination, **kwargs):
        # The complete new lease exists before the visible name is replaced.
        seen.append(json.loads((state / source).read_text()))
        assert json.loads((state / destination).read_text())["expires"] == 200
        return original(source, destination, **kwargs)

    monkeypatch.setattr(observer.os, "replace", replace)
    observer.write_active_run(state, "run-1", "control", 100, 210)
    assert seen[0]["expires"] == 210
    assert sorted(path.name for path in state.iterdir()) == [observer.ACTIVE_FILE]
    assert_invalid(lambda: observer.write_active_run(state, "run-2", "control", 100, 210))


@pytest.mark.parametrize("directory_gid", [1000, 1234])
def test_root_controller_hands_off_private_file_to_solver(tmp_path, monkeypatch, directory_gid):
    directory = tmp_path / "solver"
    directory.mkdir(mode=0o700)
    directory_info = directory.stat()
    directory_identity = (directory_info.st_dev, directory_info.st_ino)
    original_fstat = os.fstat
    original_replace = os.replace
    owners = {}
    published = []

    def fstat(fd):
        info = original_fstat(fd)
        identity = (info.st_dev, info.st_ino)
        values = list(info)
        if identity == directory_identity:
            values[4:6] = (1000, directory_gid)
        elif stat.S_ISREG(info.st_mode):
            values[4:6] = owners.get(identity, (0, 0))
        return os.stat_result(values)

    def fchown(fd, uid, gid):
        info = original_fstat(fd)
        assert stat.S_ISREG(info.st_mode)
        assert info.st_nlink == 1
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert (uid, gid) == (1000, directory_gid)
        owners[(info.st_dev, info.st_ino)] = (uid, gid)

    def replace(source, destination, **kwargs):
        info = os.stat(source, dir_fd=kwargs["src_dir_fd"], follow_symlinks=False)
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert owners[(info.st_dev, info.st_ino)] == (1000, directory_gid)
        published.append(destination)
        return original_replace(source, destination, **kwargs)

    # Emulate only ownership; real descriptors, modes and publication work on
    # non-root CI without any privileged chown or skipped coverage.
    monkeypatch.setattr(observer.os, "geteuid", lambda: 0)
    monkeypatch.setattr(observer.os, "fstat", fstat)
    monkeypatch.setattr(observer.os, "fchown", fchown)
    monkeypatch.setattr(observer.os, "replace", replace)
    observer.write_active_run(directory, "run-1", "control", 100, 200)
    assert observer.read_events(directory, "run-1") == []
    journal(directory, [event()])
    assert observer.read_events(directory, "run-1") == [event()]
    with observer._directory(directory) as fd:
        value = observer._decode(observer._read(fd, observer.ACTIVE_FILE))
        value["active"] = False
        observer._atomic(fd, observer.ACTIVE_FILE, value)
        assert observer._decode(observer._read(fd, observer.ACTIVE_FILE))["active"] is False
    assert published == [observer.ACTIVE_FILE, observer.ACTIVE_FILE]
    assert_invalid(lambda: observer.read_events(directory, "run-1"))


@pytest.mark.parametrize("euid,owner,allowed", [
    (0, 0, True), (0, 1000, True), (0, 1001, False),
    (1000, 1000, True), (1000, 0, False), (1000, 1001, False),
    (1001, 1001, True), (1001, 1000, False),
])
@pytest.mark.parametrize("mode", [0o700, 0o720, 0o702])
def test_directory_owner_override_is_narrow(tmp_path, monkeypatch, euid, owner, allowed, mode):
    original_fstat = os.fstat
    directory_info = tmp_path.stat()

    def fstat(fd):
        info = original_fstat(fd)
        if (info.st_dev, info.st_ino) == (directory_info.st_dev, directory_info.st_ino):
            values = list(info)
            values[0] = stat.S_IFDIR | mode
            values[4] = owner
            return os.stat_result(values)
        return info

    monkeypatch.setattr(observer.os, "geteuid", lambda: euid)
    monkeypatch.setattr(observer.os, "fstat", fstat)

    def open_directory():
        with observer._directory(tmp_path):
            pass

    if allowed and mode == 0o700:
        open_directory()
    else:
        assert_invalid(open_directory)


def test_matching_file_ownership_needs_no_chown(state, monkeypatch):
    def unexpected_chown(*args):
        pytest.fail("matching ownership must not require chown")

    monkeypatch.setattr(observer.os, "fchown", unexpected_chown)
    observer.write_active_run(state, "run-1", "control", 100, 210)
    info = (state / observer.ACTIVE_FILE).stat()
    directory_info = state.stat()
    assert (info.st_uid, info.st_gid) == (directory_info.st_uid, directory_info.st_gid)
    assert stat.S_IMODE(info.st_mode) == 0o600


def test_append_is_flushed_and_fsynced(state, capture, monkeypatch):
    original = observer.os.fsync
    durable = []

    def fsync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            durable.append((state / observer.EVENT_FILE).read_bytes())
        return original(fd)

    monkeypatch.setattr(observer.os, "fsync", fsync)
    capture[0].info("Challenge not detected!")
    assert len(durable) == 1
    assert durable[0].endswith(b"\n")
    assert json.loads(durable[0])["kind"] == "not_detected"


@pytest.mark.parametrize("failure", ["open", "fsync"])
def test_io_failure_invalidates_even_a_written_record(state, capture, monkeypatch, failure):
    if failure == "open":
        original = observer._open

        def failing_open(fd, name, flags):
            if name == observer.EVENT_FILE:
                raise OSError("synthetic-private")
            return original(fd, name, flags)

        monkeypatch.setattr(observer, "_open", failing_open)
    else:
        original = observer.os.fsync
        calls = []

        def failing_fsync(fd):
            calls.append(fd)
            if len(calls) == 1:
                raise OSError("synthetic-private")
            return original(fd)

        monkeypatch.setattr(observer.os, "fsync", failing_fsync)
    capture[0].info("Challenge not detected!")
    assert capture[1].invalid
    assert "synthetic-private" not in (state / observer.ERROR_FILE).read_text()
    assert_invalid(lambda: observer.read_events(state, "run-1"))


def test_unwritable_error_marker_raises_safe_failure(state, capture, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("synthetic-private")

    (state / observer.ACTIVE_FILE).unlink()
    monkeypatch.setattr(observer, "_atomic", fail)
    assert_invalid(lambda: capture[0].info("Challenge solved!"))
    assert capture[1].invalid


@pytest.mark.parametrize("name", [observer.ACTIVE_FILE, observer.EVENT_FILE, observer.ERROR_FILE])
def test_symlink_files_fail_closed_without_touching_target(state, capture, tmp_path, name):
    target = tmp_path / "target"
    target.write_text("synthetic-private")
    path = state / name
    if path.exists():
        path.unlink()
    path.symlink_to(target)
    assert_invalid(lambda: capture[0].info("Challenge not detected!"))
    assert capture[1].invalid
    assert target.read_text() == "synthetic-private"
    assert_invalid(lambda: observer.read_events(state, "run-1"))


def test_state_directory_must_preexist_and_not_be_symlink(tmp_path, state):
    missing = tmp_path / "missing"
    assert_invalid(lambda: observer.write_active_run(missing, "run-1", "control", 100, 200))
    assert not missing.exists()
    linked = tmp_path / "linked"
    linked.symlink_to(state, target_is_directory=True)
    assert_invalid(lambda: observer.read_events(linked, "run-1"))
    assert_invalid(lambda: observer.install(missing, "control"))
    assert_invalid(lambda: observer.install(linked, "control"))


def test_distinct_regular_files_required(state):
    os.link(state / observer.ACTIVE_FILE, state / observer.EVENT_FILE)
    assert_invalid(lambda: observer.read_events(state, "run-1"))
    (state / observer.EVENT_FILE).unlink()
    (state / observer.EVENT_FILE).mkdir()
    assert_invalid(lambda: observer.read_events(state, "run-1"))


@pytest.mark.parametrize("values", [
    [event(), event()],
    [event(sequence=2)],
    [event(kind="solved")],
    [event(kind="not_detected"), event(2, "solved", 111)],
    [event(), event(2, "solved", 111), event(3, "solved", 112)],
    [event(), event(2, "solved", 109)],
    [event(run_id="other-run")],
    [event(), event(2, "solved", 111, run_id="other-run")],
    [event(), event(2, "solved", 111, arm="candidate")],
    [event(title="synthetic-private")],
    [event(sequence=True)],
    [event(observed=float("nan"))],
    [event(observed=True)],
    [event(observed=201)],
    [event(schema="wrong")],
])
def test_strict_readback_rejects_invalid_events(state, values):
    journal(state, values)
    assert_invalid(lambda: observer.read_events(state, "run-1"))


@pytest.mark.parametrize("tail", [b'{"raw":"synthetic-private"}', b"\n", b"{broken}\n", b"\xff\n"])
def test_corrupt_or_partial_tail_invalidates_readback_and_append(state, capture, tail):
    journal(state, [event()])
    with (state / observer.EVENT_FILE).open("ab") as stream:
        stream.write(tail)
    before = (state / observer.EVENT_FILE).read_bytes()
    assert_invalid(lambda: observer.read_events(state, "run-1"))
    capture[0].info("Challenge solved!")
    assert capture[1].invalid
    assert (state / observer.EVENT_FILE).read_bytes() == before
    assert json.loads((state / observer.ERROR_FILE).read_text())["reason"] == "invalid_events"


def test_duplicate_json_keys_are_rejected(state):
    line = json.dumps(event())[:-1] + ',"kind":"detected"}\n'
    (state / observer.EVENT_FILE).write_text(line)
    assert_invalid(lambda: observer.read_events(state, "run-1"))


def test_readback_after_expiry_uses_observation_time(state, capture):
    capture[0].info("Challenge not detected!")
    capture[2][0] = 300
    assert observer.read_events(state, "run-1") == [event(kind="not_detected")]


def test_valid_correlation_preserves_timeout_and_independent_clearance():
    attempts = [attempt(160, 170), attempt(100, 120, outcome="timeout"),
                attempt(140, 150), attempt(125, 135, clearance_valid=False)]
    events = [event(), event(2, "detected", 126), event(3, "solved", 127),
              event(4, "not_detected", 141)]
    result = observer.join_attempt_events(attempts, events, "run-1")
    rows = result["attempts"]
    assert [(row["detection"], row["solved"]) for row in rows] == [
        ("detected", False), ("detected", True), ("not_detected", False), ("unknown", None)]
    assert rows[0]["attempt"]["outcome"] == "timeout"
    assert rows[1]["attempt"]["clearance_valid"] is False
    assert result["totals"] == dict(detected=2, solved=1, unknown=1)
    assert attempts[0] == attempt(160, 170)


def test_no_detection_is_unknown_even_if_clearance_passes(state):
    assert observer.read_events(state, "run-1") == []
    result = observer.join_attempt_events([attempt(clearance_valid=True)], [], "run-1")
    assert result["attempts"][0]["detection"] == "unknown"
    assert result["attempts"][0]["solved"] is None
    assert result["totals"] == dict(detected=0, solved=0, unknown=1)


@pytest.mark.parametrize("attempts,events", [
    ([], [event()]),
    ([attempt(120, 130)], [event()]),
    ([attempt(), attempt(120, 130)], [event(observed=120)]),
    ([attempt(), attempt(110, 130)], []),
    ([attempt(), attempt()], []),
    ([attempt()], [event(), event(2, "detected", 111)]),
    ([attempt()], [event(), event(2, "not_detected", 111)]),
    ([attempt()], [event(kind="solved")]),
    ([attempt()], [event(), event(2, "solved", 111), event(3, "solved", 112)]),
    ([attempt(), attempt(125, 140)], [event(), event(2, "solved", 126)]),
    ([attempt(run_id="other-run")], []),
    ([attempt(120, 100)], []),
    ([attempt(True, 120)], []),
    ([{}], []),
])
def test_invalid_or_ambiguous_correlation(attempts, events):
    assert_invalid(lambda: observer.join_attempt_events(attempts, events, "run-1"))
