"""Offline contracts for the native tls_requests library in Airflow."""

from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = REPO_ROOT / "docker/images/airflow/Dockerfile.scheduler-runtime"
LEGACY_DOCKERFILE = REPO_ROOT / "docker/images/airflow/Dockerfile"
SCRAPING_REQUIREMENTS = (
    REPO_ROOT / "docker/images/airflow/requirements-scraping.txt"
)
COMPOSE_FILE = REPO_ROOT / "compose.yaml"

EXPECTED_VERSION = "1.13.1"
EXPECTED_SHA256 = (
    "e4a4a5d771d1bd1558186a5ee46af1dfa1318bd31950d68ccd81ed30bad078fc"
)
EXPECTED_PATH = "/opt/tls-client/tls-client.so"


@pytest.mark.unit
def test_tls_wrapper_and_native_library_are_pinned_in_the_image():
    requirements = SCRAPING_REQUIREMENTS.read_text(encoding="utf-8")
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    legacy_dockerfile = LEGACY_DOCKERFILE.read_text(encoding="utf-8")

    # Do not invalidate and re-resolve the large shared scraping layer just to
    # pin one transitive dependency; unrelated packages must not drift.
    assert requirements.splitlines().count("wrapper-tls-requests==1.2.5") == 0
    assert "wrapper-tls-requests==1.2.5" not in legacy_dockerfile
    assert "TLS_LIBRARY_PATH" not in legacy_dockerfile
    assert "ARG AIRFLOW_RUNTIME_BASE=" in dockerfile
    assert "FROM ${AIRFLOW_RUNTIME_BASE}" in dockerfile
    wrapper_pin = (
        "RUN pip install --no-cache-dir --no-deps "
        "wrapper-tls-requests==1.2.5"
    )
    assert dockerfile.splitlines().count(wrapper_pin) == 1
    assert (
        f"releases/download/v{EXPECTED_VERSION}/"
        f"tls-client-linux-ubuntu-amd64-{EXPECTED_VERSION}.so"
        in dockerfile
    )
    assert f'echo "{EXPECTED_SHA256}  {EXPECTED_PATH}"' in dockerfile
    assert "--proto-redir '=https'" in dockerfile
    assert f"ENV TLS_LIBRARY_PATH={EXPECTED_PATH}" in dockerfile

    pin = dockerfile.index(wrapper_pin)
    download = dockerfile.index("curl --proto '=https' --tlsv1.2", pin)
    checksum = dockerfile.index("| sha256sum -c -", download)
    make_read_only = dockerfile.index(f"chmod 0444 {EXPECTED_PATH}", checksum)
    runtime_path = dockerfile.index(f"ENV TLS_LIBRARY_PATH={EXPECTED_PATH}")
    abi_smoke = dockerfile.index("client = tls_requests.Client", runtime_path)
    assert pin < download < checksum < make_read_only
    assert make_read_only < runtime_path < abi_smoke
    assert dockerfile.rfind("USER root", 0, download) >= 0
    assert dockerfile.index("USER airflow", make_read_only) < abi_smoke


@pytest.mark.unit
def test_compose_cannot_clear_the_image_level_tls_library_path():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert "TLS_LIBRARY_PATH:" not in compose
    assert "TLS_LIBRARY_PATH=" not in compose
    assert compose.count("dockerfile: Dockerfile.scheduler-runtime") == 1
    assert "AIRFLOW_RUNTIME_BASE:" in compose


@pytest.mark.unit
def test_fbref_browser_is_checksum_pinned_and_isolated_from_sofascore():
    from scrapers.fbref import browser_runtime

    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    version = browser_runtime.CAMOUFOX_BROWSER_VERSION
    release = browser_runtime.CAMOUFOX_BROWSER_RELEASE
    install_dir = str(browser_runtime.INSTALL_DIR)
    sha256 = (
        "a03872a221ab766f58d04fdaf0d7f3431c2662d5086844c67d2fc01154ebc1f8"
    )

    assert f"FBREF_CAMOUFOX_VERSION={version}" in dockerfile
    assert f"FBREF_CAMOUFOX_RELEASE={release}" in dockerfile
    assert f"FBREF_CAMOUFOX_SHA256={sha256}" in dockerfile
    assert "releases/download/v${FBREF_CAMOUFOX_VERSION}-" in dockerfile
    assert 'echo "${FBREF_CAMOUFOX_SHA256}  /tmp/' in dockerfile
    assert f"test -x {install_dir}/camoufox-bin" in dockerfile
    assert "/home/airflow/.cache/camoufox" not in dockerfile
    assert "download_mmdb" not in dockerfile
    assert "maybe_download_addons" not in dockerfile
    assert "python -m camoufox fetch" not in dockerfile
    assert "exclude_addons=list(DefaultAddons)" in dockerfile
    assert "browser.new_page().evaluate('navigator.userAgent')" in dockerfile


@pytest.mark.unit
def test_explicit_library_path_never_calls_upstream_download(monkeypatch):
    from tls_requests.models import libraries

    loaded = object()
    download_calls: list[str | None] = []
    monkeypatch.setattr(libraries, "TLS_LIBRARY_PATH", EXPECTED_PATH)
    monkeypatch.setattr(libraries.TLSLibrary, "_PATH", None)
    monkeypatch.setattr(libraries.TLSLibrary, "_LIBRARY", None)
    monkeypatch.setattr(
        libraries.ctypes.cdll,
        "LoadLibrary",
        lambda path: loaded if path == EXPECTED_PATH else None,
    )

    def forbidden_download(cls, version=None):
        download_calls.append(version)
        raise AssertionError("runtime download attempted")

    monkeypatch.setattr(
        libraries.TLSLibrary,
        "download",
        classmethod(forbidden_download),
    )

    assert libraries.TLSLibrary.load() is loaded
    assert libraries.TLSLibrary._PATH == EXPECTED_PATH
    assert download_calls == []


@pytest.mark.unit
def test_missing_explicit_library_fails_closed_without_download(monkeypatch):
    from tls_requests.models import libraries

    download_calls: list[str | None] = []
    monkeypatch.setattr(libraries, "TLS_LIBRARY_PATH", EXPECTED_PATH)
    monkeypatch.setattr(libraries.TLSLibrary, "_PATH", None)
    monkeypatch.setattr(libraries.TLSLibrary, "_LIBRARY", None)
    monkeypatch.setattr(
        libraries.ctypes.cdll,
        "LoadLibrary",
        lambda _path: (_ for _ in ()).throw(OSError("missing")),
    )
    monkeypatch.setattr(libraries.os, "remove", lambda _path: None)

    def forbidden_download(cls, version=None):
        download_calls.append(version)
        raise AssertionError("runtime download attempted")

    monkeypatch.setattr(
        libraries.TLSLibrary,
        "download",
        classmethod(forbidden_download),
    )

    assert libraries.TLSLibrary.load() is None
    assert download_calls == []
