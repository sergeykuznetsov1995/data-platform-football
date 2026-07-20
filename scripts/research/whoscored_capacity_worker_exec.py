#!/usr/bin/env python3
"""Exec one capacity worker with a Linux parent-death SIGKILL contract."""

from __future__ import annotations

import argparse
import ctypes
import os
import signal
import sys


_PR_SET_PDEATHSIG = 1
_MIN_PROTOCOL_FD = 3
_READY_PAYLOAD = b"READY\n"
_RELEASE_BYTE = b"G"


def _install_parent_death_signal(expected_parent_pid: int) -> None:
    if sys.platform != "linux":
        raise RuntimeError("capacity parent-death protection requires Linux")
    if expected_parent_pid <= 1:
        raise ValueError("expected parent pid must be greater than one")

    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    if prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, "PR_SET_PDEATHSIG failed")

    # The parent can die between fork/exec and prctl.  Set the contract first,
    # then compare against the supervisor PID supplied before Popen.  Killing
    # this wrapper closes that race without ever starting the real worker.
    if os.getppid() != expected_parent_pid:
        os.kill(os.getpid(), signal.SIGKILL)
        raise RuntimeError("capacity supervisor disappeared before worker exec")


def _validate_protocol_fds(
    ready_fd: int, release_fd: int, close_fds: list[int]
) -> None:
    for name, fd in (("ready", ready_fd), ("release", release_fd)):
        if type(fd) is not int or fd < _MIN_PROTOCOL_FD:
            raise ValueError(
                f"capacity {name} fd must be an integer greater than or equal to "
                f"{_MIN_PROTOCOL_FD}"
            )
    if ready_fd == release_fd:
        raise ValueError("capacity ready and release fds must be distinct")
    if (
        any(type(fd) is not int or fd < _MIN_PROTOCOL_FD for fd in close_fds)
        or len(close_fds) != len(set(close_fds))
        or any(fd in {ready_fd, release_fd} for fd in close_fds)
    ):
        raise ValueError("capacity close fds must be distinct protocol-safe integers")


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise RuntimeError("capacity readiness pipe accepted no bytes")
        view = view[written:]


def _signal_ready(ready_fd: int) -> None:
    try:
        _write_all(ready_fd, _READY_PAYLOAD)
    finally:
        os.close(ready_fd)


def _await_release(release_fd: int) -> None:
    try:
        # Every member of one cohort shares this pipe and consumes exactly one
        # byte from the supervisor's single atomic write.  Reading again could
        # steal another worker's permit, so EOF is deliberately not required.
        if os.read(release_fd, 1) != _RELEASE_BYTE:
            raise RuntimeError("capacity release byte does not match")
    finally:
        os.close(release_fd)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--expected-parent-pid", required=True, type=int)
    parser.add_argument("--ready-fd", required=True, type=int)
    parser.add_argument("--release-fd", required=True, type=int)
    parser.add_argument("--close-fd", action="append", type=int, default=[])
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    args = _parser().parse_args()
    argv = list(args.argv)
    if argv[:1] == ["--"]:
        argv = argv[1:]
    if not argv:
        raise ValueError("capacity worker command is required")

    _validate_protocol_fds(args.ready_fd, args.release_fd, args.close_fd)
    _install_parent_death_signal(args.expected_parent_pid)
    _signal_ready(args.ready_fd)
    _await_release(args.release_fd)
    for descriptor in args.close_fd:
        os.close(descriptor)
    os.execvpe(argv[0], argv, os.environ)
    raise AssertionError("execvpe returned")


if __name__ == "__main__":
    raise SystemExit(main())
