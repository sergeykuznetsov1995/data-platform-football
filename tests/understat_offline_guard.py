"""Opt-in network prohibition: pytest -p tests.understat_offline_guard ..."""

import socket

import pytest


_patch = pytest.MonkeyPatch()


def _forbidden(*args, **kwargs):
    # pytest.fail is not caught by transport retries or network-skip fixtures.
    pytest.fail("Understat offline profile forbids DNS and network connections")


def pytest_configure(config):
    for name in (
        "getaddrinfo", "getnameinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr",
        "create_connection",
    ):
        _patch.setattr(socket, name, _forbidden)
    for name in ("connect", "connect_ex", "sendto", "sendmsg"):
        if hasattr(socket.socket, name):
            _patch.setattr(socket.socket, name, _forbidden)


def pytest_unconfigure(config):
    _patch.undo()
