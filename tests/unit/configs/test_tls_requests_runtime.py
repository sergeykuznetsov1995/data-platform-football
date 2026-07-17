"""Offline contracts for the native tls_requests library in Airflow."""

from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = REPO_ROOT / "docker/images/airflow/Dockerfile"
SCRAPING_REQUIREMENTS = (
    REPO_ROOT / "docker/images/airflow/requirements-scraping.txt"
)
RUNNER_REQUIREMENTS = (
    REPO_ROOT / "docker/images/airflow/requirements-scraper-runner.txt"
)
CORE_REQUIREMENTS = REPO_ROOT / "docker/images/airflow/requirements.txt"
COMPOSE_FILE = REPO_ROOT / "compose.yaml"

EXPECTED_VERSION = "1.13.1"
EXPECTED_SHA256 = (
    "e4a4a5d771d1bd1558186a5ee46af1dfa1318bd31950d68ccd81ed30bad078fc"
)
EXPECTED_PATH = "/opt/tls-client/tls-client.so"


@pytest.mark.unit
def test_tls_wrapper_and_native_library_are_pinned_in_the_image():
    requirements = SCRAPING_REQUIREMENTS.read_text(encoding="utf-8")
    runner_requirements = RUNNER_REQUIREMENTS.read_text(encoding="utf-8")
    core_requirements = CORE_REQUIREMENTS.read_text(encoding="utf-8")
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    # Do not invalidate and re-resolve the large shared scraping layer just to
    # pin one transitive dependency; unrelated packages must not drift.
    assert requirements.splitlines().count("wrapper-tls-requests==1.2.5") == 0
    wrapper_locks = [
        line
        for line in runner_requirements.splitlines()
        if line.startswith("wrapper-tls-requests==1.2.5 ")
    ]
    assert len(wrapper_locks) == 1
    assert "--hash=sha256:" in wrapper_locks[0]
    chardet_locks = [
        line
        for line in core_requirements.splitlines()
        if line.startswith("chardet==5.2.0 ")
    ]
    assert len(chardet_locks) == 1
    assert "--hash=sha256:" in chardet_locks[0]
    boto3_locks = [
        line
        for line in core_requirements.splitlines()
        if line.startswith("boto3==1.42.61 ")
    ]
    assert boto3_locks == [
        "boto3==1.42.61 "
        "--hash=sha256:156efcc298a33206be6dfd220815c64aa8b09424017534cabe717636961fc306"
    ]
    assert "AS airflow-base" in dockerfile
    assert "FROM airflow-base AS airflow-scheduler" in dockerfile
    assert "apache/airflow:2.11.2-python3.11@sha256:" in dockerfile
    wrapper_pin = "-r /tmp/requirements-scraper-runner.txt"
    assert dockerfile.count(wrapper_pin) == 1
    assert "--no-deps --require-hashes --only-binary=:all:" in dockerfile
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
    assert "PYTHONWARNINGS=error /usr/local/bin/python" in dockerfile
    assert "m.version('chardet') == '5.2.0'" in dockerfile
    assert "m.version('wrapper-tls-requests') == '1.2.5'" in dockerfile


@pytest.mark.unit
def test_compose_cannot_clear_the_image_level_tls_library_path():
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert "TLS_LIBRARY_PATH:" not in compose
    assert "TLS_LIBRARY_PATH=" not in compose
    assert "Dockerfile.transfermarkt-runtime" not in compose
    assert compose.count("target: airflow-base") == 3
    assert compose.count("target: airflow-scheduler") == 1
    assert "WHOSCORED_SCRAPER_PYTHON: /usr/local/bin/python" in compose
    workflow = (REPO_ROOT / ".github/workflows/whoscored-ci.yml").read_text(
        encoding="utf-8"
    )
    assert "import curl_cffi,tls_client,tls_requests" in workflow
    assert 'm.version(\\"tls-client-python\\") == \\"1.15.1\\"' in workflow


@pytest.mark.unit
def test_legacy_browser_jobs_use_only_the_isolated_runner():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "python -S -m venv /opt/legacy-scraper-venv" in dockerfile
    assert "--system-site-packages" not in dockerfile
    assert "/opt/legacy-scraper-venv/bin/python -I -m pip check" in dockerfile
    assert "s/^include-system-site-packages = //p" in dockerfile
    assert "assert sys.prefix == '/opt/legacy-scraper-venv'" in dockerfile
    assert "PIP_REQUIRE_VIRTUALENV=1" in dockerfile
    assert dockerfile.count("PIP_REQUIRE_VIRTUALENV=1") == 2
    assert "/home/airflow/soccerdata" in dockerfile
    assert "/opt/legacy-scraper-venv/bin/python -I -m pip install" in dockerfile
    assert "--no-cache-dir --no-deps --require-hashes --only-binary=:all:" in dockerfile
    assert (
        "/opt/legacy-scraper-venv/bin/python -I -m pip install "
        "--no-cache-dir --user"
    ) not in dockerfile
    commands = {
        "dags/dag_ingest_sofifa.py": "run_sofifa_scraper.py",
        "dags/dag_ingest_understat.py": "run_understat_scraper.py",
        "dags/dag_ingest_espn.py": "run_espn_scraper.py",
        "dags/dag_ingest_clubelo.py": "run_clubelo_scraper.py",
        "dags/dag_ingest_sofascore.py": "run_sofascore_scraper.py",
    }
    for relative, runner in commands.items():
        source = (REPO_ROOT / relative).read_text(encoding="utf-8")
        assert f"/opt/legacy-scraper-venv/bin/python dags/scripts/{runner}" in source


@pytest.mark.unit
def test_fresh_and_existing_soccerdata_volumes_are_owned_before_airflow_starts():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    compose = COMPOSE_FILE.read_text(encoding="utf-8")

    base_stage = dockerfile.split("FROM airflow-base AS airflow-scheduler-payload", 1)[0]
    assert (
        "install -d -o 50000 -g 0 -m 0755 /home/airflow/soccerdata"
        in base_stage
    )
    log_init = compose.split("  airflow-log-init:", 1)[1].split(
        "\n  airflow-init:", 1
    )[0]
    assert "soccerdata_cache:/home/airflow/soccerdata" in log_init
    assert "chown -R --no-dereference 50000:0 /home/airflow/soccerdata" in log_init
    assert "chmod -R u+rwX,g+rwX,o-rwx /home/airflow/soccerdata" in log_init


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
    assert 'stat -c %s /tmp/fbref-camoufox.zip)" -eq 663773735' in dockerfile
    assert f"test -x {install_dir}/camoufox-bin" in dockerfile
    assert "/home/airflow/.cache/camoufox" in dockerfile
    assert "/opt/legacy-scraper-venv/bin/python -I -c" in dockerfile
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
