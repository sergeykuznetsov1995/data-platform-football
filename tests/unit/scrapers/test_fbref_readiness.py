from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from pyarrow import fs

from scrapers.fbref.readiness import (
    ReadinessError,
    check_raw_store_read_access,
    check_raw_store_roundtrip,
    check_trino_read_access,
    check_trino_roundtrip,
    validate_camoufox_runtime,
    validate_fbref_proxy_meter,
    validate_raw_store_uri,
)
from scrapers.fbref.raw_store import RawPageStore


def test_raw_store_uri_is_exact_and_fail_closed():
    assert validate_raw_store_uri("s3://football/raw/fbref/") == (
        "s3://football/raw/fbref"
    )
    with pytest.raises(ReadinessError, match="FBREF_RAW_STORE_URI"):
        validate_raw_store_uri("/tmp/fbref")


def _camoufox_install(tmp_path, *, version="152.0.4", release="beta.26"):
    root = tmp_path / "camoufox"
    root.mkdir()
    (root / "version.json").write_text(
        '{"version":"%s","release":"%s"}' % (version, release),
        encoding="utf-8",
    )
    executable = root / "camoufox-bin"
    executable.write_text(
        f"#!/bin/sh\nprintf '%s\\n' "
        f"'Camoufox Camoufox {version}-{release}'\n",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    fontconfig = root / "fontconfig" / "windows" / "fonts.conf"
    fontconfig.parent.mkdir(parents=True)
    fontconfig.write_text("<fontconfig/>\n", encoding="utf-8")
    return root


def test_camoufox_runtime_requires_the_reviewed_exact_stack(tmp_path):
    result = validate_camoufox_runtime(
        package_version="0.4.11",
        playwright_version="1.59.0",
        curl_cffi_version="0.15.0",
        install_dir=_camoufox_install(tmp_path),
    )

    assert result == {
        "status": "passed",
        "camoufox_package": "0.4.11",
        "camoufox_browser": "152.0.4-beta.26",
        "playwright": "1.59.0",
        "curl_cffi": "0.15.0",
        "executable_verified": True,
        "executable_probe_verified": True,
        "fontconfig_verified": True,
    }


def test_camoufox_runtime_falls_back_to_scraper_runner_venv(tmp_path, monkeypatch):
    import importlib.metadata

    from scrapers.fbref import readiness

    def _missing(_name):
        raise importlib.metadata.PackageNotFoundError("camoufox")

    monkeypatch.setattr(importlib.metadata, "version", _missing)
    venv_python = tmp_path / "venv-python"
    venv_python.write_text(
        "#!/bin/sh\nprintf '0.4.11\\n1.59.0\\n0.15.0\\n'\n",
        encoding="utf-8",
    )
    venv_python.chmod(0o755)
    monkeypatch.setattr(readiness, "_SCRAPER_RUNNER_PYTHON", venv_python)

    result = validate_camoufox_runtime(install_dir=_camoufox_install(tmp_path))

    assert result["status"] == "passed"
    assert result["camoufox_package"] == "0.4.11"
    assert result["playwright"] == "1.59.0"
    assert result["curl_cffi"] == "0.15.0"


@pytest.mark.parametrize(
    ("package", "playwright", "curl_cffi", "browser", "release"),
    [
        ("0.4.10", "1.59.0", "0.15.0", "152.0.4", "beta.26"),
        ("0.4.11", "1.60.0", "0.15.0", "152.0.4", "beta.26"),
        ("0.4.11", "1.59.0", "0.14.0", "152.0.4", "beta.26"),
        ("0.4.11", "1.59.0", "0.15.0", "135.0.1", "beta.24"),
        ("0.4.11", "1.59.0", "0.15.0", "152.0.4", "beta.25"),
    ],
)
def test_camoufox_runtime_rejects_unreviewed_drift(
    tmp_path, package, playwright, curl_cffi, browser, release
):
    with pytest.raises(ReadinessError, match="reviewed production pin"):
        validate_camoufox_runtime(
            package_version=package,
            playwright_version=playwright,
            curl_cffi_version=curl_cffi,
            install_dir=_camoufox_install(
                tmp_path, version=browser, release=release
            ),
        )


def test_camoufox_runtime_rejects_a_broken_executable(tmp_path):
    root = _camoufox_install(tmp_path)
    (root / "camoufox-bin").write_text(
        "#!/bin/sh\nprintf '%s\\n' 'wrong browser'\n",
        encoding="utf-8",
    )

    with pytest.raises(ReadinessError, match="version is invalid"):
        validate_camoufox_runtime(
            package_version="0.4.11",
            playwright_version="1.59.0",
            curl_cffi_version="0.15.0",
            install_dir=root,
        )


def test_camoufox_runtime_rejects_missing_fontconfig(tmp_path):
    root = _camoufox_install(tmp_path)
    (root / "fontconfig" / "windows" / "fonts.conf").unlink()

    with pytest.raises(ReadinessError, match="reviewed production pin"):
        validate_camoufox_runtime(
            package_version="0.4.11",
            playwright_version="1.59.0",
            curl_cffi_version="0.15.0",
            install_dir=root,
        )


def _meter_payload(**changes):
    payload = {
        "status": "ok",
        "meter": "proxy_filter_provider_path_v2",
        "daily_total_bytes": 10,
        "daily_budget_bytes": 300_000_000,
        "daily_remaining_bytes": 299_999_990,
        "max_lease_bytes": 104_857_600,
        "max_lease_ttl_seconds": 7200,
        "max_active_leases": 1,
        "dagrun_budget_bytes": 104_857_600,
        "url_budget_bytes": 104_857_600,
        "lease_proxy_url": "http://fbref_proxy_filter:8900",
        "configured_pool_count": 4,
        "fbref_source_ready": True,
        "fbref_dag_ids": [
            "dag_accept_fbref_bronze",
            "dag_backfill_fbref",
            "dag_bootstrap_fbref",
            "dag_ingest_fbref",
        ],
    }
    payload.update(changes)
    return payload


class _MeterSession:
    def __init__(self, payload, *, status=200):
        self.payload = payload
        self.status = status
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return SimpleNamespace(
            status_code=self.status,
            json=lambda: self.payload,
        )


def test_proxy_meter_preflight_is_authenticated_secret_safe_and_zero_paid():
    session = _MeterSession(_meter_payload())

    result = validate_fbref_proxy_meter(
        "http://fbref_proxy_filter:8899",
        control_token="s" * 32,
        required_bytes=100 * 1024 * 1024,
        minimum_configured_exits=4,
        session=session,
    )

    assert result["configured"] == 4
    assert result["probe"] == "authenticated_control_only_zero_paid_bytes"
    assert "s" * 32 not in repr(result)
    assert session.calls == [
        (
            "http://fbref_proxy_filter:8899/v1/auth-check",
            {
                "headers": {"X-Proxy-Control-Token": "s" * 32},
                "timeout": 5.0,
            },
        )
    ]


@pytest.mark.parametrize(
    ("payload", "status"),
    [
        (_meter_payload(), 401),
        (_meter_payload(dagrun_budget_bytes=99), 200),
        (_meter_payload(max_active_leases=2), 200),
        (_meter_payload(lease_proxy_url="http://shared-proxy:8900"), 200),
        (_meter_payload(configured_pool_count=3), 200),
    ],
)
def test_proxy_meter_preflight_fails_closed(payload, status):
    with pytest.raises(ReadinessError):
        validate_fbref_proxy_meter(
            "http://fbref_proxy_filter:8899",
            control_token="s" * 32,
            required_bytes=100 * 1024 * 1024,
            minimum_configured_exits=4,
            session=_MeterSession(payload, status=status),
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


def test_raw_read_probe_lists_without_mutating(tmp_path):
    root = tmp_path / "raw"
    root.mkdir()
    existing = root / "immutable.json"
    existing.write_bytes(b"unchanged")
    store = RawPageStore(fs.LocalFileSystem(), str(root))

    before = {path.name: path.read_bytes() for path in root.iterdir()}
    result = check_raw_store_read_access(store)
    after = {path.name: path.read_bytes() for path in root.iterdir()}

    assert result == {
        "status": "passed",
        "probe": "read_only_root_listing",
        "visible_entries": 1,
    }
    assert after == before


def test_raw_read_probe_fails_when_root_is_unreadable(tmp_path):
    store = RawPageStore(fs.LocalFileSystem(), str(tmp_path / "missing"))

    with pytest.raises(ReadinessError, match="raw root is not readable"):
        check_raw_store_read_access(store)


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


def test_trino_read_probe_uses_select_only_and_requires_bronze():
    class Manager:
        def __init__(self, rows):
            self.rows = rows
            self.calls = []

        def _execute(self, sql, fetch=False):
            self.calls.append((sql, fetch))
            return self.rows

    manager = Manager([("bronze",)])
    assert check_trino_read_access(manager) == {
        "status": "passed",
        "probe": "read_only_bronze_schema_select",
    }
    sql, fetch = manager.calls[0]
    assert fetch is True
    assert sql.lstrip().upper().startswith("SELECT ")
    assert not {"CREATE", "INSERT", "UPDATE", "DELETE", "DROP"} & set(
        sql.upper().split()
    )

    with pytest.raises(ReadinessError, match="Bronze schema"):
        check_trino_read_access(Manager([]))


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
