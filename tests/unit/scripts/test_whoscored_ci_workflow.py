"""Static contracts for the dedicated WhoScored production CI workflow."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "whoscored-ci.yml"


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_every_pull_request_runs_the_cross_boundary_contract():
    text = _workflow_text()
    trigger = text.split("permissions:", 1)[0]

    assert "pull_request:" in trigger
    assert "paths:" not in trigger


def test_real_airflow_211_import_gate_is_not_a_stub_only_test():
    text = _workflow_text()
    job = text.split("  real-airflow-dag-import:\n", 1)[1]
    checker = (
        ROOT / "scripts/ci/check_whoscored_dag_imports.py"
    ).read_text(encoding="utf-8")

    assert 'airflow-version: ["2.7.3", "2.11.2"]' in text
    assert "Dockerfile.ci-dag-import-${{ matrix.airflow-version }}" in text
    assert "docker build" in text
    assert "--read-only" in job
    assert "--network none" in job
    assert "--cap-drop ALL" in job
    assert "--security-opt no-new-privileges:true" in job
    assert "--tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777" in job
    assert '"${GITHUB_WORKSPACE}:/workspace:ro"' in job
    assert "airflow dags list-import-errors" in job
    assert "scripts/ci/check_whoscored_dag_imports.py" in job
    assert "AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags/.whoscored-ci-staged" in job
    assert "AIRFLOW__CORE__DAGS_FOLDER=/opt/whoscored-dags" not in job
    assert "${GITHUB_WORKSPACE}/dags/.whoscored-ci-staged" in job
    assert "actual != expected" in checker
    assert "from airflow.models import DagBag" in checker
    for dag_id in (
        "dag_ingest_whoscored",
        "dag_backfill_whoscored",
        "dag_canary_whoscored_proxy",
        "dag_backup_whoscored_storage",
    ):
        assert dag_id in text
        assert dag_id in checker


def test_ci_builds_auxiliary_python_images_from_their_locked_dockerfiles():
    text = _workflow_text()

    assert "Build immutable Superset and JupyterHub runtimes" in text
    assert "docker/images/superset" in text
    assert "docker/images/jupyterhub" in text
    assert "data-platform-superset:ci" in text
    assert "data-platform-jupyterhub:ci" in text


def test_ci_runs_public_writer_and_capacity_contracts():
    text = _workflow_text()

    assert "tests/unit/scrapers/test_iceberg_writer.py" in text
    assert "bench_whoscored_capacity.py" in text
    assert "whoscored_capacity_container_runtime.py" in text
    assert "docker/images/airflow/whoscored_capacity_worker_bootstrap.py" in text
    assert "find tests -type f -name '*.py'" in text


def test_production_admission_tests_run_from_a_root_owned_protected_release():
    text = _workflow_text()
    protected = text.split(
        "- name: Test production admission from a protected release", 1
    )[1].split("- name: WhoScored unit, DAG and storage contract", 1)[0]
    broad_contract = text.split(
        "- name: WhoScored unit, DAG and storage contract", 1
    )[1]

    assert "protected_root=/root/whoscored-ci-admission" in protected
    assert "sudo install -d -o root -g root -m 0755" in protected
    assert "sudo install -o root -g root -m 0444" in protected
    assert "sudo /usr/bin/env -i" in protected
    assert '"$test_python" -' in protected
    assert "scripts/whoscored_production_admission.py" in protected
    assert "scripts/validate_whoscored_build_provenance.py" in protected
    assert 'PYTHONPATH="$protected_root:${GITHUB_WORKSPACE}"' in protected
    assert "from scripts import whoscored_production_admission as admission" in protected
    assert "Path(admission.__file__).absolute().parents[1] == expected_root" in protected
    assert "grep -v '/test_whoscored_production_admission.py$'" in broad_contract


def test_ci_uses_test_runtime_and_smokes_immutable_flaresolverr():
    text = _workflow_text()
    scheduler_smoke = text.split(
        "- name: Build and smoke the hardened scheduler test image", 1
    )[1].split("- name: Build production targets", 1)[0]

    assert "--target airflow-scheduler-test" in text
    assert "docker/images/flaresolverr-whoscored/Dockerfile" in text
    assert "--read-only" in text
    assert text.count("--security-opt apparmor=docker-default") >= 2
    assert text.count("--security-opt seccomp=builtin") >= 2
    assert "--cap-add SYS_PTRACE" not in text
    assert 'test "$(id -u):$(id -g)" = 50000:0' in text
    assert "0000000000000000" in text
    assert "whoscored-zero-cap-smoke" in text
    assert "--headless --no-sandbox --disable-setuid-sandbox" in text
    assert "-e PYTHONPATH=/opt/airflow:/opt/airflow/dags" in text
    assert "Camoufox(headless='virtual'" in scheduler_smoke
    assert "executable_path='/opt/fbref-camoufox/camoufox-bin'" in scheduler_smoke
    assert "assert 'Firefox/152.0' in ua" in scheduler_smoke
    assert "--network none" in scheduler_smoke
    assert "--user 50000:0" in scheduler_smoke
    assert "--read-only" in scheduler_smoke
    assert "--shm-size 1g" in scheduler_smoke
    assert (
        "--tmpfs /tmp:rw,noexec,nosuid,nodev,size=256m,uid=50000,gid=0,mode=0700"
    ) in scheduler_smoke
    assert "timeout --kill-after=5s 45s docker run --rm" in scheduler_smoke
    assert "serve_logs(port=18793)" in scheduler_smoke
    assert "airflow_root_ctime" in scheduler_smoke
    assert "test ! -e /opt/airflow/airflow.cfg" in scheduler_smoke
    assert "test ! -e /opt/airflow/gunicorn.ctl" in scheduler_smoke
    assert "Control socket listening" in scheduler_smoke
    assert '--cidfile "$scheduler_smoke_cidfile"' in scheduler_smoke
    assert 'scheduler_smoke_tmpdir="$(mktemp -d)"' in scheduler_smoke
    assert 'scheduler_smoke_cidfile="$scheduler_smoke_tmpdir/cid"' in scheduler_smoke
    assert "trap cleanup_scheduler_smoke EXIT" in scheduler_smoke
    assert "'FONTCONFIG_PATH': '/opt/fbref-camoufox/fontconfig/windows'" in scheduler_smoke
    assert "'HOME': home.name" in scheduler_smoke
    assert "dict(os.environ)" not in scheduler_smoke
    assert "os='windows'" in scheduler_smoke
    assert "platform == 'Win32'" in scheduler_smoke
    assert "/v1/whoscored/runtime-identity" in text
    assert 'assert response["extension_sha256"] == identity["extension_sha256"]' in text


def test_ci_proves_both_production_targets_match_declared_evidence_state():
    text = _workflow_text()

    assert "validate_whoscored_build_provenance.py" in text
    assert "python -I -S scripts/validate_whoscored_build_provenance.py" in text
    assert "--expect-blocked" in text
    assert "--expect-ready-build" in text
    assert "--release-revision" in text
    assert "github.event.pull_request.head.sha" in text
    assert "fetch-depth: 0" in text
    assert 'case "$provenance_status" in' in text
    assert "blocked-v1)" in text
    assert "ready-v1)" in text
    assert "--target airflow-scheduler" in text
    assert "--target airflow-whoscored-proxy" in text
    assert "assert_exit_78_before_sentinel" in text
    assert "assert_success_after_gate" in text
    assert "scheduler-ready" in text
    assert "proxy-ready" in text
    assert "for alias in python python3 python3.11" in text
    assert "scheduler-bash-python-s" in text
    assert "/usr/local/libexec/whoscored-python-real" in text
    assert "/opt/airflow/dags/scripts/run_whoscored_scraper.py" in text
    assert "--entrypoint /opt/legacy-scraper-venv/bin/python" in text


def test_production_provenance_is_pushed_and_smoked_by_digest():
    text = _workflow_text()
    production = text.split(
        "- name: Build production targets and prove the declared gate state", 1
    )[1].split(
        "- name: Build and smoke the immutable WhoScored FlareSolverr image", 1
    )[0]

    assert (
        "registry:2.8.3@sha256:"
        "a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
    ) in text
    assert 'network=host' in text
    assert text.count("provenance-add-gha=false") == 1
    assert '[registry."127.0.0.1:5000"]' in text
    assert production.count("--provenance=mode=max,version=v1") == 2
    assert production.count("--push") == 2
    attested_commands = [
        block.split("docker/images/airflow", 1)[0]
        for block in production.split("docker buildx build \\\n")[1:]
        if "--provenance=" in block
    ]
    assert len(attested_commands) == 2
    assert all("--load" not in command for command in attested_commands)
    assert 'docker pull "$scheduler"' in production
    assert 'docker pull "$proxy"' in production
    assert 'docker tag "$scheduler" "$scheduler_tag"' in production
    assert 'docker tag "$proxy" "$proxy_tag"' in production
    assert 'scheduler="$scheduler_tag@$scheduler_digest"' in production
    assert 'proxy="$proxy_tag@$proxy_digest"' in production
    assert (
        "WHOSCORED_SCHEDULER_IMAGE: "
        "127.0.0.1:5000/data-platform-airflow-scheduler:provenance-ci"
    ) in text
    assert "WHOSCORED_SCHEDULER_IMAGE: data-platform-airflow-scheduler" not in text


def test_ci_exercises_every_runner_moved_to_the_legacy_venv():
    text = _workflow_text()

    for runner in (
        "run_clubelo_scraper.py",
        "run_espn_scraper.py",
        "run_understat_scraper.py",
        "run_sofifa_scraper.py",
        "prepare_sofascore_workload.py",
        "run_sofascore_scraper.py",
    ):
        assert runner in text
    assert (
        '/opt/legacy-scraper-venv/bin/python "/opt/airflow/dags/scripts/${runner}"'
        in text
    )
    for test_path in (
        "tests/unit/configs/test_tls_requests_runtime.py",
        "tests/unit/dags/test_dag_ingest_clubelo.py",
        "tests/unit/dags/test_dag_ingest_sofascore.py",
        "tests/unit/dags/test_dag_ingest_sofifa.py",
        "tests/unit/dags/test_dag_ingest_understat.py",
        "tests/unit/scrapers/test_run_clubelo_scraper.py",
        "tests/unit/scrapers/test_run_espn_scraper.py",
        "tests/unit/scrapers/test_run_sofascore_scraper.py",
        "tests/unit/scrapers/test_run_sofifa_scraper.py",
        "tests/unit/scrapers/test_run_understat_scraper.py",
    ):
        assert test_path in text
