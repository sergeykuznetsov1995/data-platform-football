from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import pytest
from pyarrow import fs

from scrapers.fbref.readiness import (
    ReadinessError,
    check_raw_store_roundtrip,
    check_trino_roundtrip,
    validate_proxy_pool,
    validate_raw_store_uri,
)
from scrapers.fbref.raw_store import RawPageStore


def test_raw_store_uri_is_exact_and_fail_closed():
    assert validate_raw_store_uri("s3://football/raw/fbref/") == (
        "s3://football/raw/fbref"
    )
    with pytest.raises(ReadinessError, match="FBREF_RAW_STORE_URI"):
        validate_raw_store_uri("/tmp/fbref")


def test_proxy_preflight_is_secret_safe_and_tcp_only(tmp_path):
    path = tmp_path / "proxys.txt"
    path.write_text("one:80:user:password\ntwo:81:user:other\n")
    os.chmod(path, 0o600)

    class Manager:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def load_from_file_custom_format(self, value):
            assert value == str(path)
            return 2

        def validate_proxies(self, **kwargs):
            assert kwargs == {
                "timeout": 3.0,
                "max_workers": 100,
                "ban_failed": True,
            }
            return {"alive": 2, "dead": 0, "total": 2}

    result = validate_proxy_pool(
        path, minimum_healthy=2, manager_factory=Manager
    )

    assert result["healthy"] == 2
    assert result["probe"] == "tcp_connect_only"
    assert "password" not in repr(result)
    assert "one" not in repr(result)


def test_proxy_preflight_rejects_malformed_or_writable_pool(tmp_path):
    path = tmp_path / "proxys.txt"
    path.write_text("one:80:user:password\nbad\n")
    os.chmod(path, 0o622)
    with pytest.raises(ReadinessError, match="group/world writable"):
        validate_proxy_pool(path, minimum_healthy=1)

    os.chmod(path, 0o600)
    manager = SimpleNamespace(
        load_from_file_custom_format=lambda _: 1,
        validate_proxies=lambda **_: {"alive": 1, "dead": 0, "total": 1},
    )
    with pytest.raises(ReadinessError, match="malformed entries"):
        validate_proxy_pool(
            path, minimum_healthy=1, manager_factory=lambda **_: manager
        )


def test_raw_health_roundtrip_cleans_up(tmp_path):
    store = RawPageStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    result = check_raw_store_roundtrip(store, token=uuid.UUID(int=1))

    assert result["status"] == "passed"
    assert result["cleanup_verified"] is True
    health = tmp_path / "raw" / "_health"
    assert not list(health.glob("*.bin"))


def test_raw_health_roundtrip_deletes_after_readback_failure(tmp_path):
    store = RawPageStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    delegate = store.filesystem

    class CorruptingStream:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b"corrupt"

    class CorruptingFilesystem:
        def __getattr__(self, name):
            return getattr(delegate, name)

        def open_input_file(self, _):
            return CorruptingStream()

    store.filesystem = CorruptingFilesystem()
    with pytest.raises(ReadinessError, match="SHA256"):
        check_raw_store_roundtrip(store, token=uuid.UUID(int=2))
    assert not list((tmp_path / "raw" / "_health").glob("*.bin"))


def test_trino_health_roundtrip_always_drops_table():
    class Manager:
        def __init__(self):
            self.exists = False
            self.dropped = []

        def table_exists(self, schema, table):
            return self.exists

        def _execute(self, sql, fetch=False):
            if sql.startswith("CREATE TABLE"):
                self.exists = True
                return None
            assert fetch is True
            token = sql.split('"')[-2]
            return [(token,)]

        def drop_table(self, schema, table, if_exists=True):
            self.dropped.append((schema, table, if_exists))
            self.exists = False

    manager = Manager()
    result = check_trino_roundtrip(manager, token=uuid.UUID(int=3))

    assert result == {"status": "passed", "cleanup_verified": True}
    assert manager.dropped


def test_trino_health_accepts_driver_list_rows():
    class Manager:
        exists = False

        def table_exists(self, _schema, _table):
            return self.exists

        def _execute(self, sql, fetch=False):
            if sql.startswith("CREATE TABLE"):
                self.exists = True
                return None
            assert fetch is True
            token = sql.split('"')[-2]
            return [[token]]

        def drop_table(self, _schema, _table, if_exists=True):
            assert if_exists is True
            self.exists = False

    assert check_trino_roundtrip(
        Manager(), token=uuid.UUID(int=4)
    )["status"] == "passed"
