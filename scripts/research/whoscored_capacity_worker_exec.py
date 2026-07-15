#!/usr/bin/env python3
"""Exec one capacity worker with a Linux parent-death SIGKILL contract."""

from __future__ import annotations

import argparse
import ctypes
import os
import signal
import sys


_PR_SET_PDEATHSIG = 1


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--expected-parent-pid", required=True, type=int)
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    args = _parser().parse_args()
    argv = list(args.argv)
    if argv[:1] == ["--"]:
        argv = argv[1:]
    if not argv:
        raise ValueError("capacity worker command is required")
    _install_parent_death_signal(args.expected_parent_pid)
    os.execvpe(argv[0], argv, os.environ)
    raise AssertionError("execvpe returned")


if __name__ == "__main__":
    raise SystemExit(main())
