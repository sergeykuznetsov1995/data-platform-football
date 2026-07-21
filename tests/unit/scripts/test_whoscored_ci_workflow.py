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
    assert "mapfile -t whoscored_tests" in text
    assert "find tests -type f -name 'test_*whoscored*.py'" in text
    assert '"${whoscored_tests[@]}"' in text
    assert "mapfile -t whoscored_static_paths" in text
    assert '"${whoscored_static_paths[@]}"' in text
    assert "rg --files tests" not in text


def test_ci_isolates_root_only_contracts_from_the_host_runner():
    text = _workflow_text()
    unit_step = text.split(
        "- name: WhoScored unit, DAG and storage contract", 1
    )[1].split("- name: Static and import checks", 1)[0]
    real_docker_step = text.split(
        "- name: Prove real Buildx metadata and Docker digest last", 1
    )[1].split("- name: Cleanup test runtime", 1)[0]
    lock = (ROOT / ".github" / "workflows" / "whoscored-test.lock").read_text(
        encoding="utf-8"
    )

    assert "sudo " not in unit_step
    assert "ubuntu:24.04@sha256:" in unit_step
    assert "4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90" in (
        unit_step
    )
    assert "--network none" in unit_step
    assert "--cap-drop ALL" in unit_step
    assert "--cap-add CHOWN" in unit_step
    assert "--cap-add DAC_OVERRIDE" in unit_step
    assert "--cap-add FOWNER" in unit_step
    assert "--cap-add SYS_ADMIN" in unit_step
    assert "--security-opt no-new-privileges:true" in unit_step
    assert "--security-opt apparmor=docker-default" in unit_step
    assert "--security-opt seccomp=builtin" in unit_step
    assert "seccomp=unconfined" not in unit_step
    assert "'{{json .HostConfig.Binds}}'" in unit_step
    assert "'{{json .HostConfig.Mounts}}'" in unit_step
    assert "'{{json .Mounts}}'" in unit_step
    assert 'tar -C "$GITHUB_WORKSPACE" -cf - .' in unit_step
    assert "tar --no-same-owner -C /root/data-platform-football -xf -" in unit_step
    assert "ldd " not in unit_step
    assert "tar -C /usr/lib/x86_64-linux-gnu -cf - ." in unit_step
    assert "tar -C /usr/share/zoneinfo -cf - ." in unit_step
    assert 'docker cp /usr/bin/node "$test_container:/usr/bin/node"' in unit_step
    assert "unexpected_runtime_path" in unit_step
    assert "! -user root -o -writable" in unit_step
    assert "! -user root -o ! -group root" in unit_step
    assert 'if ! unexpected_ownership="$(docker exec' in unit_step
    assert 'test -z "$unexpected_ownership"' in unit_step
    assert "/usr/libexec/docker/cli-plugins/docker-compose" in unit_step
    assert "test -S /run/docker.sock" in unit_step
    assert 'endpoint.connect("/run/docker.sock")' in unit_step
    assert "GITHUB_WORKSPACE=/root/data-platform-football" in unit_step
    assert "--workdir /root/data-platform-football" in unit_step
    assert "/root/.secrets/whoscored-runtime-v2.env" in unit_step
    assert "/root/.secrets/whoscored-proxy-v2.env" in unit_step
    assert 'Path("/usr/local/bin/docker")' in unit_step
    assert 'exec /usr/bin/docker \\"$@\\"' in unit_step
    assert "unshare from util-linux 2.39.3" in unit_step
    assert "51bcc77ba5db162c80028f861f0a2770d728c1de80773816d863f28d7a817adb" in (
        unit_step
    )
    assert "--env WHOSCORED_REAL_DOCKER_TEST" not in unit_step
    assert "WHOSCORED_REAL_DOCKER_TEST=1" not in unit_step
    assert 'test "$isolated_status" = 0' in unit_step
    assert "WHOSCORED_REAL_DOCKER_TEST=1" in real_docker_step
    assert (
        "test_generate_whoscored_deployment_attestation.py::"
        "test_real_buildx_metadata_and_docker_digest_path_when_ci_provides_it"
    ) in real_docker_step
    assert (
        lock.splitlines().count(
            "curl-cffi==0.15.0 "
            "--hash=sha256:2b6c847d86283b07ae69bb72c82eb8a59242277142aa35b89850f89e792a02fc"
        )
        == 1
    )
    install_step = text.split("- name: Install test runtime", 1)[1].split(
        "- name: WhoScored unit, DAG and storage contract", 1
    )[0]
    assert "sudo " not in install_step
    assert 'test_venv="$(mktemp -d /tmp/whoscored-ci-test-venv.XXXXXXXX)"' in (
        install_step
    )
    assert '"$WHOSCORED_CI_SETUP_PYTHON" -I -S -m venv --copies' in install_step
    assert '"$test_venv/bin/python" -I -m pip install' in install_step
    assert "WHOSCORED_CI_PYTHON_PREFIX=$setup_python_prefix" in install_step
    assert "--require-hashes --only-binary=:all:" in install_step
    assert '-r .github/workflows/whoscored-test.lock' in install_step
    assert text.count("-m pip install \\") == 1


def test_ci_uses_test_runtime_and_smokes_immutable_flaresolverr():
    text = _workflow_text()
    contract_job = text.split("  contract:\n", 1)[1].split(
        "  real-airflow-dag-import:\n", 1
    )[0]
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
    assert "docker/setup-buildx-action@" not in text
    assert "/usr/libexec/docker/cli-plugins/docker-buildx" in text
    assert "d41ece72044243b4f58b343441ae37446d9c29a7d6b5e11c61847bbcf8f7dfda" in (
        text
    )
    assert "a319e5b15052cf6557ceb666eb8ff6e32380b782" in text
    assert 'test ! -e "$HOME/.docker/cli-plugins/docker-buildx"' in text
    assert "docker buildx build" not in text
    assert text.count('"$buildx_exec" build "$@"') == 4
    assert text.count("buildx_build \\\n") == 7
    assert text.count("for attempt in 1 2 3") == 4
    assert "type=gha" not in text
    assert text.count(
        "exec 9< /usr/libexec/docker/cli-plugins/docker-buildx"
    ) == 4
    assert text.count('--builder "$WHOSCORED_CI_BUILDER"') == 7
    early_verification = text.index(
        "- name: Create an isolated immutable builder before repository code runs"
    )
    assert early_verification < text.index(
        "- name: Build immutable Superset and JupyterHub runtimes"
    )
    assert text.index(
        "- name: Build and smoke the immutable WhoScored FlareSolverr image"
    ) < text.index("- name: Install test runtime")
    assert text.index("- name: Install test runtime") < text.index(
        "- name: Static and import checks"
    )
    assert "BASH_ENV: /dev/null" in text
    assert "uses:" not in contract_job
    assert "refs/pull/${WHOSCORED_CI_PR_NUMBER}/merge" in contract_job
    assert "fetch --no-tags --depth=3 origin" in contract_job
    assert "GIT_CONFIG_NOSYSTEM: \"1\"" in contract_job
    assert "filter.lfs.process" in contract_job
    assert "WHOSCORED_CI_SETUP_PYTHON=$python_bin" in contract_job
    assert "/opt/hostedtoolcache/Python" in contract_job
    assert "x64/bin/python3.11" in contract_job
    assert "/usr/bin/sudo /bin/chown -R root:root /opt/hostedtoolcache/Python" in (
        contract_job
    )
    assert "source_prefix\" =~ ^/opt/hostedtoolcache/Python/3\\.11" in contract_job
    assert "unexpected_python_path" in contract_job
    assert "unexpected_python_link" in contract_job
    assert text.count("persist-credentials: false") == 1
    assert text.count("if: ${{ !cancelled() }}") == 5
    assert text.index(
        "- name: Prove checked-in evidence and closure report agree"
    ) < text.index("- name: Prove real Buildx metadata and Docker digest last")
    assert text.index(
        "- name: Prove real Buildx metadata and Docker digest last"
    ) < text.index("- name: Cleanup test runtime")
    assert "- name: Cleanup test runtime" in text
    assert "if: ${{ always() }}" in text
    assert "buildx_buildkit_${builder_name}0" in text
    assert "DOCKER_CONFIG=$docker_config" in text
    assert "DOCKER_HOST=unix:///var/run/docker.sock" in text
    assert "BUILDX_CONFIG=$docker_config/buildx" in text
    assert "BUILDX_HOST BUILDX_CONFIG" in text
    assert "--buildkitd-config \"$buildkit_config\"" in text
    assert "{{.HostConfig.NetworkMode}}" in text
    assert (
        "image=moby/buildkit:v0.31.2@sha256:"
        "2f5adac4ecd194d9f8c10b7b5d7bceb5186853db1b26e5abd3a657af0b7e26ec"
    ) in text


def test_ci_proves_both_production_targets_match_declared_evidence_state():
    text = _workflow_text()

    assert "validate_whoscored_build_provenance.py" in text
    assert "python -I -S scripts/validate_whoscored_build_provenance.py" in text
    assert "--expect-blocked" in text
    assert "--expect-ready-build" in text
    assert "--release-revision" in text
    assert "github.event.pull_request.head.sha" in text
    assert '"refs/remotes/pull/${WHOSCORED_CI_PR_NUMBER}/merge"' in text
    assert "/usr/bin/git rev-parse HEAD^2" in text
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


def test_production_provenance_is_pushed_and_smoked_by_digest() -> None:
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
    assert '[registry."127.0.0.1:5000"]' in text
    assert "--driver-opt network=host" in text
    assert "network=host" in text
    assert production.count("--provenance=mode=max,version=v1") == 2
    assert production.count("--push") == 2
    build_commands = [
        block.split("docker/images/airflow", 1)[0]
        for block in production.split(
            "buildx_build \\\n"
        )[1:]
    ]
    assert sum("--provenance=" in command for command in build_commands) == 2
    assert all(
        "--load" not in command
        for command in build_commands
        if "--provenance=" in command
    )
    assert 'docker pull "$scheduler"' in production
    assert 'docker pull "$proxy"' in production
    assert 'scheduler="$scheduler_tag@$scheduler_digest"' in production
    assert 'proxy="$proxy_tag@$proxy_digest"' in production


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
