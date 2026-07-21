from __future__ import annotations

import ast
import hashlib
import inspect
import json
import os
from pathlib import Path
import py_compile
import re
import subprocess
import sys

import pytest

from scrapers.whoscored.runtime_contract import (
    EXPECTED_RUNTIME_FILES,
    RUNTIME_CONTRACT_PATH,
    RuntimeContractError,
    validate_runtime_contract,
)


ROOT = Path(__file__).resolve().parents[3]
IMAGE_TRUST_ROOT_PATH = ROOT / (
    "docker/images/airflow/whoscored-runtime-trust-root-production"
)
GENERIC_IMAGE_TRUST_ROOT_PATH = ROOT / (
    "docker/images/airflow/whoscored-runtime-trust-root-generic"
)
TEST_IMAGE_TRUST_ROOT_PATH = ROOT / (
    "docker/images/airflow/whoscored-runtime-trust-root-test"
)
RUNTIME_ENTRYPOINTS = frozenset(
    {
        "dags/dag_backfill_whoscored.py",
        "dags/dag_backup_whoscored_storage.py",
        "dags/dag_canary_whoscored_proxy.py",
        "dags/dag_ingest_whoscored.py",
        "dags/scripts/run_whoscored_backfill_item.py",
        "dags/scripts/run_whoscored_scraper.py",
        "dags/scripts/whoscored_proxy_runtime.py",
        "scripts/proxy_filter/filter_proxy.py",
        "scripts/whoscored_proxy_campaign.py",
        "scripts/whoscored_paid_gateway.py",
        "scripts/whoscored_raw_backup.py",
        "scripts/whoscored_v2_object_contract.py",
    }
)
# Airflow exposes ``dags`` on sys.path, so every bare ``utils.*`` import must be
# mapped back to its checked-in module instead of escaping the runtime closure.
RUNTIME_MODULE_ALIASES = {
    "utils.alerts": "dags.utils.alerts",
    "utils.config": "dags.utils.config",
    "utils.default_args": "dags.utils.default_args",
    "utils.silver_tasks": "dags.utils.silver_tasks",
}
# ``utils.config`` imports this only inside ``scale_floor_for_league``;
# WhoScored entrypoints consume DAG_TAGS/SCHEDULES and never call that function.
NON_RUNTIME_LAZY_ALIASES = frozenset({"utils.medallion_config"})
RUNTIME_STATIC_FILES = frozenset(
    {
        ".dockerignore",
        "configs/medallion/competitions.yaml",
        "docker/images/airflow/Dockerfile",
        "docker/images/airflow/requirements-airflow.txt",
        "docker/images/airflow/requirements-build-tools.txt",
        "docker/images/airflow/requirements-scheduler.txt",
        "docker/images/airflow/requirements-scraper-runner.txt",
        "docker/images/airflow/requirements-scraping.txt",
        "docker/images/airflow/requirements.txt",
        "docker/images/airflow/whoscored-production-entrypoint",
        "docker/images/airflow/whoscored-production-gate",
        "docker/images/airflow/whoscored-production-python",
        "docker/images/airflow/whoscored_capacity_worker_bootstrap.py",
        "docker/images/airflow/whoscored_production_gate.py",
        "docker/images/airflow/whoscored_runtime_pth.py",
        "docker/images/airflow/whoscored_runtime_startup.py",
        "docker/images/flaresolverr-whoscored/Dockerfile",
        "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
        "docker/images/flaresolverr-whoscored/entrypoint.sh",
        "dags/utils/maintenance_tasks.py",
        "scrapers/whoscored/runtime_contract.py",
        "scripts/flaresolverr_extended.py",
        # The host-side capacity supervisor seals these path-loaded helpers
        # into the worker bundle; ordinary Python imports cannot expose those
        # edges to the fixed-point walker below.
        "scripts/research/bench_whoscored_capacity.py",
        "scripts/research/bench_whoscored_workflow.py",
        "scripts/research/whoscored_capacity_container_runtime.py",
        "scripts/research/whoscored_capacity_worker_exec.py",
        # These two host-side admission sources are loaded through an exact
        # fd-relative protected path, so no ordinary Python import edge can
        # make them visible to the AST fixed-point walker.
        "scripts/validate_whoscored_build_provenance.py",
        "scripts/whoscored_production_admission.py",
    }
)


def _module_name(relative: str) -> str:
    parts = list(Path(relative).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _local_runtime_module_index() -> dict[str, str]:
    result: dict[str, str] = {}
    for prefix in ("dags", "scrapers", "scripts"):
        for path in (ROOT / prefix).rglob("*.py"):
            relative = path.relative_to(ROOT).as_posix()
            result[_module_name(relative)] = relative
    return result


def _module_paths_with_ancestors(
    module: str,
    *,
    module_index: dict[str, str],
) -> set[str]:
    """Return a module plus every package ``__init__`` Python executes first."""
    parts = module.split(".")
    return {
        relative
        for depth in range(1, len(parts) + 1)
        if (relative := module_index.get(".".join(parts[:depth]))) is not None
    }


def _runtime_imports(
    relative: str,
    *,
    module_index: dict[str, str],
) -> tuple[set[str], set[str]]:
    path = ROOT / relative
    current_module = _module_name(relative)
    package = (
        current_module
        if path.name == "__init__.py"
        else current_module.rpartition(".")[0]
    )
    dependencies: set[str] = set()
    unknown_aliases: set[str] = set()

    def add(module: str) -> None:
        if module.startswith("utils."):
            if module in NON_RUNTIME_LAZY_ALIASES:
                return
            mapped = RUNTIME_MODULE_ALIASES.get(module)
            if mapped is None:
                unknown_aliases.add(module)
                return
            module = mapped
        if module.startswith(("dags.", "scrapers.", "scripts.")):
            dependencies.update(
                _module_paths_with_ancestors(
                    module,
                    module_index=module_index,
                )
            )

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                add(alias.name)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            package_parts = package.split(".") if package else []
            base_parts = package_parts[: len(package_parts) - (node.level - 1)]
            if node.module:
                base_parts.extend(node.module.split("."))
            imported_from = ".".join(base_parts)
        else:
            imported_from = node.module or ""
        add(imported_from)
        # ``from package import module`` needs both package/__init__.py and the
        # imported module when that name resolves to a real local file.
        for alias in node.names:
            candidate = f"{imported_from}.{alias.name}" if imported_from else alias.name
            if candidate in module_index:
                add(candidate)
    return dependencies, unknown_aliases


def _copy_runtime_tree(target_root: Path) -> dict:
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    contract["files"] = {}
    for relative in EXPECTED_RUNTIME_FILES:
        source = ROOT / relative
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = source.read_bytes()
        target.write_bytes(payload)
        contract["files"][relative] = hashlib.sha256(payload).hexdigest()
    return contract


def _write_startup_trust_root(
    path: Path,
    runtime_root: Path,
    *,
    runtime_class: str = "production-v1",
) -> None:
    source = runtime_root / "scrapers/whoscored/runtime_contract.py"
    lock = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    path.write_text(
        "\n".join(
            (
                "schema_version=1",
                "runtime_class=" + runtime_class,
                "runtime_contract_source_sha256="
                + hashlib.sha256(source.read_bytes()).hexdigest(),
                "runtime_contract_lock_sha256="
                + hashlib.sha256(lock.read_bytes()).hexdigest(),
                "",
            )
        ),
        encoding="ascii",
    )


def _fatal_pth_line(call: str, *, wrapper_path: Path) -> str:
    program = (
        "try:\n"
        f" _path={str(wrapper_path)!r}\n"
        " _namespace={'__builtins__':__builtins__}\n"
        " exec(compile(open(_path,'rb').read(),_path,'exec'),_namespace)\n"
        f" _namespace['run']{call}\n"
        "except BaseException:\n"
        " sys.modules['posix']._exit(78)\n"
    )
    return f"import sys;exec({program!r},{{'sys':sys}})\n"


def _write_successful_production_gate(path: Path) -> None:
    path.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    path.chmod(0o755)


@pytest.mark.unit
def test_image_trust_root_matches_mounted_attestor_and_lock():
    for path, runtime_class in (
        (GENERIC_IMAGE_TRUST_ROOT_PATH, "generic-v1"),
        (TEST_IMAGE_TRUST_ROOT_PATH, "test-v1"),
        (IMAGE_TRUST_ROOT_PATH, "production-v1"),
    ):
        values = dict(
            line.split("=", 1) for line in path.read_text(encoding="ascii").splitlines()
        )

        assert values == {
            "schema_version": "1",
            "runtime_class": runtime_class,
            "runtime_contract_source_sha256": hashlib.sha256(
                (ROOT / "scrapers/whoscored/runtime_contract.py").read_bytes()
            ).hexdigest(),
            "runtime_contract_lock_sha256": hashlib.sha256(
                RUNTIME_CONTRACT_PATH.read_bytes()
            ).hexdigest(),
        }


@pytest.mark.unit
def test_docker_installs_last_ordered_root_owned_runtime_trust_anchor():
    dockerfile = (ROOT / "docker/images/airflow/Dockerfile").read_text(encoding="utf-8")

    assert (
        'ENV GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm --no-control-socket"'
        in dockerfile
    )
    assert "AIRFLOW_CONFIG=/usr/local/share/whoscored/airflow.cfg" in dockerfile
    assert "= \"0:0:444:0\"" in dockerfile
    assert 'GunicornOption("control_socket_disable", True)' in dockerfile
    assert (
        "370759b8078edd002470a0ddfb0e3030365a08459f2844b3e102367ffa166e2b"
        in dockerfile
    )
    assert "NwdZuAeO3QAkcKDd-w4wMDZaCEWfKESz4QI2f_oWbis,7233" in dockerfile
    assert (
        "cbeb35f3979408ad5997cb5bc10f9733a7d4b42d2d28174126536271f2fbe083"
        in dockerfile
    )
    assert "whoscored-runtime-trust-root-generic" in dockerfile
    assert "whoscored-runtime-trust-root-production" in dockerfile
    assert "whoscored-runtime-trust-root-test" in dockerfile
    assert "FROM airflow-base AS airflow-scheduler-payload" in dockerfile
    assert "FROM airflow-scheduler-payload AS airflow-scheduler-test" in dockerfile
    assert "FROM airflow-scheduler-payload AS airflow-scheduler" in dockerfile
    assert (
        "FROM airflow-whoscored-proxy-payload AS airflow-whoscored-proxy" in dockerfile
    )
    assert "00000000-whoscored-runtime-bootstrap.pth" in dockerfile
    assert "zzzzzzzz-whoscored-runtime-finalize.pth" in dockerfile
    dependency_install = dockerfile.index("-r /tmp/requirements.txt")
    build_tool_install = dockerfile.index("-r /tmp/requirements-build-tools.txt")
    assert build_tool_install < dependency_install
    assert dockerfile.rfind("USER root", 0, build_tool_install) > 0
    assert dockerfile.index("USER airflow", build_tool_install) < dependency_install
    assert dockerfile.count("--user --no-deps --require-hashes") == 2
    assert dockerfile.count("PYTHONUSERBASE=/home/airflow/.local") == 2
    assert dockerfile.count("PIP_REQUIRE_VIRTUALENV=1") == 2
    assert "/opt/legacy-scraper-venv/bin/python -I -m pip install" in dockerfile
    assert "--no-cache-dir --no-deps --require-hashes --only-binary=:all:" in dockerfile
    assert (
        "/opt/legacy-scraper-venv/bin/python -I -m pip install --no-cache-dir --user"
    ) not in dockerfile
    bootstrap_activation = dockerfile.index(
        "> /usr/local/lib/python3.11/site-packages/"
        "00000000-whoscored-runtime-bootstrap.pth"
    )
    assert dependency_install < bootstrap_activation
    assert dockerfile.rfind("USER root", dependency_install, bootstrap_activation) > 0
    scheduler_payload = dockerfile.split(
        "FROM airflow-base AS airflow-scheduler-payload", 1
    )[1].split("FROM airflow-base AS airflow-whoscored-proxy-payload", 1)[0]
    disable_hooks = scheduler_payload.index(
        "/usr/local/share/whoscored/build-hooks/bootstrap"
    )
    scheduler_install = scheduler_payload.index(
        "/usr/local/bin/python -S -m pip install"
    )
    restore_hooks = scheduler_payload.rindex(
        "/usr/local/share/whoscored/build-hooks/bootstrap"
    )
    final_order_check = scheduler_payload.rindex("sort | head -n 1")
    assert disable_hooks < scheduler_install < restore_hooks < final_order_check
    restored_tail = scheduler_payload[
        scheduler_payload.rindex("rmdir /usr/local/share/whoscored/build-hooks") :
    ]
    assert "\nRUN " not in restored_tail
    assert "sort | head -n 1" in dockerfile
    assert "sort | tail -n 1" in dockerfile
    assert "chmod 0444" in dockerfile
    assert "rm -rf /__whoscored_runtime_bytecode_disabled__" in scheduler_payload
    assert "test ! -e /__whoscored_runtime_bytecode_disabled__" in scheduler_payload
    assert "ENV CHROME_CONFIG_HOME=/tmp/airflow-chromium-config" in scheduler_payload
    assert 'ENV PYTHONPATH=""' in dockerfile
    assert "PYTHONNOUSERSITE=1" in dockerfile
    assert '_n[\\"run\\"](\\"bootstrap\\")' in dockerfile
    assert '_n[\\"run\\"](\\"finalize\\")' in dockerfile
    assert 'sys.modules[\\"posix\\"]._exit(78)' in dockerfile
    assert (
        dockerfile.count(
            'ENTRYPOINT ["/usr/bin/dumb-init", "--", '
            '"/usr/local/bin/whoscored-production-entrypoint", "/entrypoint"]'
        )
        == 2
    )
    for relative in (
        "whoscored-production-entrypoint",
        "whoscored-production-gate",
        "whoscored-production-python",
        "whoscored_production_gate.py",
    ):
        assert (
            "/opt/airflow/runtime-contract/docker/images/airflow/" + relative
        ) in dockerfile


@pytest.mark.unit
def test_airflow_context_mirrors_exact_flaresolverr_supply_inputs():
    airflow_context = ROOT / "docker/images/airflow"
    flaresolverr_context = ROOT / "docker/images/flaresolverr-whoscored"

    assert (
        airflow_context / "whoscored-flaresolverr-runtime.root.dockerignore"
    ).read_bytes() == (ROOT / ".dockerignore").read_bytes()
    assert (
        airflow_context / "whoscored-flaresolverr-runtime.Dockerfile"
    ).read_bytes() == (flaresolverr_context / "Dockerfile").read_bytes()
    assert (
        airflow_context / "whoscored-flaresolverr-runtime.Dockerfile.dockerignore"
    ).read_bytes() == (flaresolverr_context / "Dockerfile.dockerignore").read_bytes()
    assert (
        airflow_context / "whoscored-flaresolverr-runtime.entrypoint.sh"
    ).read_bytes() == (flaresolverr_context / "entrypoint.sh").read_bytes()
    dockerfile = (airflow_context / "Dockerfile").read_text(encoding="utf-8")
    assert "/opt/airflow/runtime-contract/.dockerignore" in dockerfile
    assert (
        "/opt/airflow/runtime-contract/docker/images/flaresolverr-whoscored/Dockerfile"
    ) in dockerfile
    assert (
        "/opt/airflow/runtime-contract/docker/images/"
        "flaresolverr-whoscored/Dockerfile.dockerignore"
    ) in dockerfile
    assert (
        "/opt/airflow/runtime-contract/docker/images/"
        "flaresolverr-whoscored/entrypoint.sh"
    ) in dockerfile


@pytest.mark.unit
def test_real_pth_execution_namespace_can_invoke_runtime_loader(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    site_directory = tmp_path / "site-packages"
    site_directory.mkdir()
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    pth_wrapper = runtime_root / "docker/images/airflow/whoscored_runtime_pth.py"
    (site_directory / "whoscored_runtime_pth.py").write_bytes(pth_wrapper.read_bytes())
    marker = tmp_path / "mutable-runtime-sentinel-executed"
    (runtime_root / "sentinel_before_attestation.py").write_text(
        f"open({str(marker)!r}, 'w').write('executed')\n",
        encoding="utf-8",
    )
    malicious_helper_marker = tmp_path / "malicious-pth-helper-executed"
    (runtime_root / "whoscored_runtime_pth.py").write_text(
        f"open({str(malicious_helper_marker)!r}, 'w').write('executed')\n"
        "def run(*_args, **_kwargs): pass\n",
        encoding="utf-8",
    )
    invocation = (
        "(%r,"
        f"startup_path={str(startup)!r},runtime_root={str(runtime_root)!r},"
        "require_full=False,enforce_trust_ownership=False)"
    )
    (site_directory / "00000000-whoscored-runtime-bootstrap.pth").write_text(
        _fatal_pth_line(
            invocation % "bootstrap",
            wrapper_path=site_directory / "whoscored_runtime_pth.py",
        ),
        encoding="utf-8",
    )
    (site_directory / "middle-import-sentinel.pth").write_text(
        "import sentinel_before_attestation\n",
        encoding="utf-8",
    )
    (site_directory / "middle-z-readd-runtime-root.pth").write_text(
        f"import sys;sys.path.insert(0,{str(runtime_root)!r})\n",
        encoding="utf-8",
    )
    (site_directory / "zzzzzzzz-whoscored-runtime-finalize.pth").write_text(
        _fatal_pth_line(
            invocation % "finalize",
            wrapper_path=site_directory / "whoscored_runtime_pth.py",
        ),
        encoding="utf-8",
    )
    script = f"""
import site
import sys
site.addsitedir({str(site_directory)!r})
assert sys._whoscored_runtime_startup_schema == 2
contract = sys._load_whoscored_runtime_contract({str(runtime_root)!r})
assert contract is sys._whoscored_runtime_contract
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not marker.exists()
    assert not malicious_helper_marker.exists()


@pytest.mark.unit
@pytest.mark.parametrize("wrapper_state", ["valid", "missing", "syntax-error"])
def test_real_site_failure_exits_before_trailing_pth_or_process_code(
    tmp_path,
    wrapper_state,
):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    site_directory = tmp_path / "site-packages"
    site_directory.mkdir()
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    wrapper = runtime_root / "docker/images/airflow/whoscored_runtime_pth.py"
    installed_wrapper = site_directory / "whoscored_runtime_pth.py"
    if wrapper_state == "valid":
        installed_wrapper.write_bytes(wrapper.read_bytes())
    elif wrapper_state == "syntax-error":
        installed_wrapper.write_text("this is not valid Python !!!\n", encoding="utf-8")
    missing_trust_root = tmp_path / "missing-trust-root"
    first_call = (
        "('bootstrap',"
        f"startup_path={str(startup)!r},runtime_root={str(runtime_root)!r},"
        f"trust_root_path={str(missing_trust_root)!r},require_full=True,"
        "enforce_trust_ownership=False)"
    )
    (site_directory / "00000000-whoscored-runtime-bootstrap.pth").write_text(
        _fatal_pth_line(first_call, wrapper_path=installed_wrapper),
        encoding="utf-8",
    )
    trailing_marker = tmp_path / "trailing-pth-executed"
    (site_directory / "zzzzzzzz-trailing-sentinel.pth").write_text(
        f"import builtins;builtins.open({str(trailing_marker)!r},'w').write('x')\n",
        encoding="utf-8",
    )
    process_marker = tmp_path / "process-continued"
    script = f"""
import site
import sys
sys.argv[0] = 'airflow'
site.addsitedir({str(site_directory)!r})
open({str(process_marker)!r}, 'w').write('continued')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 78, completed.stderr
    assert not trailing_marker.exists()
    assert not process_marker.exists()


@pytest.mark.unit
def test_production_gate_blocks_ordinary_python_before_user_code(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    contract_path = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    trust_root = tmp_path / "runtime-trust-root"
    _write_startup_trust_root(trust_root, runtime_root)
    gate = tmp_path / "whoscored-production-gate"
    gate.write_text("#!/bin/sh\nexit 9\n", encoding="ascii")
    gate.chmod(0o755)
    site_directory = tmp_path / "site-packages"
    site_directory.mkdir()
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    wrapper = runtime_root / "docker/images/airflow/whoscored_runtime_pth.py"
    installed_wrapper = site_directory / "whoscored_runtime_pth.py"
    installed_wrapper.write_bytes(wrapper.read_bytes())
    call = (
        "('bootstrap',"
        f"startup_path={str(startup)!r},runtime_root={str(runtime_root)!r},"
        f"trust_root_path={str(trust_root)!r},production_gate_path={str(gate)!r},"
        "require_full=True,enforce_trust_ownership=False)"
    )
    (site_directory / "00000000-whoscored-runtime-bootstrap.pth").write_text(
        _fatal_pth_line(call, wrapper_path=installed_wrapper),
        encoding="utf-8",
    )
    process_marker = tmp_path / "ordinary-python-user-code"
    script = f"""
import site
site.addsitedir({str(site_directory)!r})
open({str(process_marker)!r}, 'w').write('continued')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 78, completed.stderr
    assert not process_marker.exists()


@pytest.mark.unit
@pytest.mark.parametrize("runtime_class", ("generic-v1", "test-v1"))
def test_nonproduction_image_class_cannot_claim_source_authority(
    tmp_path,
    runtime_class,
):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    contract_path = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    trust_root_path = tmp_path / "runtime-trust-root"
    _write_startup_trust_root(
        trust_root_path,
        runtime_root,
        runtime_class=runtime_class,
    )
    import time as time_module

    time_module.sleep(0.2)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': True,
    '_WHOSCORED_TRUST_ROOT_PATH': {str(trust_root_path)!r},
    '_WHOSCORED_ENFORCE_TRUST_OWNERSHIP': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
sys._load_whoscored_runtime_contract(root)
assert sys._whoscored_runtime_class == {runtime_class!r}
assert sys._whoscored_runtime_class != 'production-v1'
contract = sys._whoscored_runtime_contract
try:
    contract.require_production_runtime_class(operation='WhoScored source bypass test')
except contract.RuntimeContractError as exc:
    assert 'requires WhoScored runtime class production-v1' in str(exc), exc
    assert 'actual={runtime_class}' in str(exc), exc
else:
    raise AssertionError('non-production runtime acquired source authority')
from dags.scripts import run_whoscored_scraper
runner_output = {str(tmp_path / "generic-runner-output.json")!r}
try:
    run_whoscored_scraper.main(['discover', '--output', runner_output])
except contract.RuntimeContractError as exc:
    assert 'production-v1' in str(exc), exc
else:
    raise AssertionError('non-production runner reached source execution')
assert not __import__('os').path.exists(runner_output)
import scrapers.whoscored.raw_store as raw_store_module
from scrapers.whoscored.raw_store import WhoScoredRawStore
from scripts import whoscored_raw_backup
from scrapers.whoscored.repository import WhoScoredRepository
from scrapers.whoscored.source_circuit import SharedSourceCircuit
from scrapers.whoscored.transport import WhoScoredTransport
from dags.scripts.whoscored_ops_store import WhoScoredOpsStore
from dags.scripts import whoscored_frozen_dq
from dags.utils import maintenance_tasks
raw_placeholder = object.__new__(WhoScoredRawStore)
filesystem_marker = {str(tmp_path / "forbidden-filesystem-constructor")!r}
def forbidden_s3_filesystem(**_kwargs):
    open(filesystem_marker, 'w').write('called')
    raise AssertionError('S3 filesystem constructor reached')
raw_store_module.fs.S3FileSystem = forbidden_s3_filesystem
try:
    WhoScoredRawStore.from_uri('s3://forbidden/raw')
except contract.RuntimeContractError as exc:
    assert 'production-v1' in str(exc), exc
else:
    raise AssertionError('raw URI opened a filesystem outside production')
assert not __import__('os').path.exists(filesystem_marker)
try:
    whoscored_raw_backup.open_store(
        's3://forbidden/backup', role='destination'
    )
except contract.RuntimeContractError as exc:
    assert 'production-v1' in str(exc), exc
else:
    raise AssertionError('raw backup opened a filesystem outside production')
assert not __import__('os').path.exists(filesystem_marker)
maintenance_marker = {str(tmp_path / "forbidden-whoscored-maintenance")!r}
def forbidden_maintenance_connection():
    open(maintenance_marker, 'w').write('called')
    raise AssertionError('maintenance connection reached')
maintenance_tasks._connect = forbidden_maintenance_connection
class ForbiddenCursor:
    def execute(self, _sql):
        open(maintenance_marker, 'w').write('called')
        raise AssertionError('frozen DQ cursor reached')
for label, operation in (
    (
        'frozen DQ stage',
        lambda: whoscored_frozen_dq.stage_frozen_population(
            {{}}, writer=object()
        ),
    ),
    (
        'frozen DQ cleanup',
        lambda: whoscored_frozen_dq.cleanup_staged_frozen_populations(
            ForbiddenCursor()
        ),
    ),
    (
        'WhoScored maintenance',
        lambda: maintenance_tasks.maintain_iceberg_tables(
            table_filter=maintenance_tasks.WHOSCORED_HIGH_CHURN
        ),
    ),
    (
        'WhoScored scheduled cleanup',
        maintenance_tasks.cleanup_whoscored_dq_stage_partitions,
    ),
):
    try:
        operation()
    except contract.RuntimeContractError as exc:
        assert 'production-v1' in str(exc), (label, exc)
    else:
        raise AssertionError(label + ' accepted non-production runtime')
assert not __import__('os').path.exists(maintenance_marker)
for label, constructor in (
    ('transport', lambda: WhoScoredTransport()),
    ('raw store', lambda: WhoScoredRawStore(None, 'raw')),
    (
        'repository',
        lambda: WhoScoredRepository(writer=object(), trino=object()),
    ),
    ('ops store', lambda: WhoScoredOpsStore(raw_placeholder)),
    (
        'source circuit',
        lambda: SharedSourceCircuit({str(tmp_path / "forbidden-circuit.json")!r}),
    ),
):
    try:
        constructor()
    except contract.RuntimeContractError as exc:
        assert 'production-v1' in str(exc), (label, exc)
    else:
        raise AssertionError(label + ' accepted non-production runtime')
try:
    __import__('scripts.whoscored_v2_object_contract', fromlist=('contract',))
except contract.RuntimeContractError as exc:
    assert 'production-v1' in str(exc), exc
else:
    raise AssertionError('object contract loaded outside production')
real_verifier = sys._require_whoscored_runtime_class
sys._require_whoscored_runtime_class = lambda *_args: 'production-v1'
try:
    contract.require_production_runtime_class(operation='forged verifier bypass test')
except contract.RuntimeContractError as exc:
    assert 'runtime-class verifier was replaced' in str(exc), exc
else:
    raise AssertionError('replacement verifier acquired source authority')
sys._require_whoscored_runtime_class = real_verifier
sys._whoscored_runtime_class = 'production-v1'
try:
    contract.require_production_runtime_class(operation='forged marker bypass test')
except contract.RuntimeContractError as exc:
    assert 'runtime-class marker was replaced' in str(exc), exc
else:
    raise AssertionError('replacement marker acquired source authority')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=runtime_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_checked_in_whoscored_runtime_contract_matches_release_tree():
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    result = validate_runtime_contract()

    assert result["status"] == "success"
    assert result["parser_version"] == "whoscored-parser-v8"
    assert result["report_schema_version"] == 3
    assert result["business_dataset_count"] == 25
    assert EXPECTED_RUNTIME_FILES == tuple(sorted(EXPECTED_RUNTIME_FILES))
    assert len(EXPECTED_RUNTIME_FILES) == len(set(EXPECTED_RUNTIME_FILES))
    assert tuple(contract["files"]) == EXPECTED_RUNTIME_FILES
    assert result["file_count"] == len(EXPECTED_RUNTIME_FILES) == 88
    assert len(result["code_tree_sha256"]) == 64
    assert len(result["manifest_sha256"]) == 64


@pytest.mark.unit
def test_runtime_hashes_the_complete_closure_before_application_imports():
    source = inspect.getsource(validate_runtime_contract)
    import_barrier = source.index("from scrapers.base.iceberg_writer import")

    assert source.index("_validate_declared_file_set(files)") < import_barrier
    assert source.index("actual_hashes[relative] = actual_hash") < import_barrier


@pytest.mark.unit
def test_every_runtime_entrypoint_requires_current_startup_schema():
    for relative in RUNTIME_ENTRYPOINTS:
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "_whoscored_runtime_startup_schema" in source, relative
        assert re.search(r"\)\s*!=\s*2\s*\)?\s*:", source), relative


@pytest.mark.unit
def test_importing_runtime_contract_does_not_preimport_mutable_application_modules():
    script = f"""
import sys
sys.path.insert(0, {str(ROOT)!r})
import scrapers.whoscored.runtime_contract
for name in (
    'scrapers.whoscored.catalog',
    'scrapers.whoscored.domain',
    'scrapers.whoscored.parsers',
    'scrapers.whoscored.raw_store',
    'scrapers.whoscored.repository',
    'scrapers.whoscored.service',
    'scrapers.whoscored.transport',
):
    assert name not in sys.modules, name
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_runtime_file_allowlist_is_the_fixed_point_import_closure():
    module_index = _local_runtime_module_index()
    closure: set[str] = set(RUNTIME_STATIC_FILES)
    for entrypoint in RUNTIME_ENTRYPOINTS:
        closure.add(entrypoint)
        closure.update(
            _module_paths_with_ancestors(
                _module_name(entrypoint),
                module_index=module_index,
            )
        )
    pending = [relative for relative in closure if relative.endswith(".py")]
    unknown_aliases: set[str] = set()

    while pending:
        relative = pending.pop()
        dependencies, aliases = _runtime_imports(
            relative,
            module_index=module_index,
        )
        unknown_aliases.update(aliases)
        for dependency in dependencies:
            if dependency not in closure:
                closure.add(dependency)
                pending.append(dependency)

    assert unknown_aliases == set()
    assert tuple(sorted(closure)) == EXPECTED_RUNTIME_FILES


@pytest.mark.unit
def test_runtime_contract_fails_closed_on_one_mixed_file(tmp_path):
    contract = _copy_runtime_tree(tmp_path)
    mixed = tmp_path / "scrapers" / "base" / "iceberg_writer.py"
    mixed.write_bytes(mixed.read_bytes() + b"\n# stale deployment\n")
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(
        RuntimeContractError, match="file hash mismatch.*iceberg_writer"
    ):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=tmp_path,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_missing_runtime_file(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    missing = runtime_root / "dags" / "dag_ingest_whoscored.py"
    missing.unlink()
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="cannot open required runtime file"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=runtime_root,
        )


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["missing", "unexpected"])
def test_runtime_contract_rejects_manifest_file_set_drift(tmp_path, mutation):
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    if mutation == "missing":
        contract["files"].pop("dags/dag_ingest_whoscored.py")
    else:
        contract["files"]["dags/dag_stale_whoscored.py"] = "0" * 64
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match=rf"file set mismatch.*{mutation}"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=ROOT,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_duplicate_json_file_key(tmp_path):
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    metadata = {key: value for key, value in contract.items() if key != "files"}
    file_items = list(contract["files"].items())
    duplicate_key, duplicate_hash = file_items[0]
    serialized_files = [
        f"{json.dumps(key)}:{json.dumps(value)}" for key, value in file_items
    ]
    serialized_files.append(f"{json.dumps(duplicate_key)}:{json.dumps(duplicate_hash)}")
    payload = (
        json.dumps(metadata)[:-1] + ',"files":{' + ",".join(serialized_files) + "}}"
    )
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(payload, encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="duplicate key"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=ROOT,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_manifest_path_escape(tmp_path):
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    contract["files"]["../escaped.py"] = "0" * 64
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="invalid.*escaped"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=ROOT,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_symlink_path_escape(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    escaped = tmp_path / "escaped.py"
    escaped.write_text("stale deployment", encoding="utf-8")
    linked = runtime_root / "scrapers" / "base" / "iceberg_writer.py"
    linked.unlink()
    linked.symlink_to(escaped)
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="cannot open required runtime file"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=runtime_root,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_matching_in_root_file_symlink(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    linked = runtime_root / "scrapers" / "base" / "iceberg_writer.py"
    staged = runtime_root / "staged_iceberg_writer.py"
    staged.write_bytes(linked.read_bytes())
    linked.unlink()
    linked.symlink_to(staged)
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="cannot open required runtime file"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=runtime_root,
        )


@pytest.mark.unit
def test_runtime_contract_rejects_unexpected_core_module(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    stale = runtime_root / "scrapers" / "whoscored" / "stale.py"
    stale.write_text("# stale deployment\n", encoding="utf-8")
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(
        RuntimeContractError, match="core runtime file set mismatch.*stale"
    ):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=runtime_root,
        )


@pytest.mark.unit
def test_runtime_contract_fingerprints_are_deterministic(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    reversed_contract = {key: contract[key] for key in reversed(tuple(contract))}
    reversed_contract["files"] = dict(reversed(tuple(contract["files"].items())))
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text(json.dumps(contract), encoding="utf-8")
    second_path.write_text(json.dumps(reversed_contract), encoding="utf-8")

    first = validate_runtime_contract(
        contract_path=first_path,
        runtime_root=runtime_root,
    )
    second = validate_runtime_contract(
        contract_path=second_path,
        runtime_root=runtime_root,
    )

    assert first["code_tree_sha256"] == second["code_tree_sha256"]
    assert first["manifest_sha256"] == second["manifest_sha256"]


@pytest.mark.unit
def test_default_runtime_rejects_coherent_tree_replaced_after_module_import(tmp_path):
    """An old loaded module cannot attest a newer bind-mounted release tree."""

    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    contract_path = runtime_root / "scrapers" / "whoscored" / "runtime_contract.lock"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    script = f"""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
sys.path.insert(0, {str(runtime_root)!r})
from scrapers.whoscored import parsers
from scrapers.whoscored.runtime_contract import (
    RuntimeContractError,
    _process_start_time_ns,
    validate_runtime_contract,
)
loaded_parser_version = parsers.PARSER_VERSION
time.sleep(0.1)
parser_path = {str(runtime_root / "scrapers/whoscored/parsers.py")!r}
contract_path = {str(contract_path)!r}
with open(parser_path, 'ab') as handle:
    handle.write(b'\\n# coherently deployed replacement\\n')
with open(contract_path, encoding='utf-8') as handle:
    contract = json.load(handle)
with open(parser_path, 'rb') as handle:
    contract['files']['scrapers/whoscored/parsers.py'] = hashlib.sha256(handle.read()).hexdigest()
with open(contract_path, 'w', encoding='utf-8') as handle:
    json.dump(contract, handle)
assert os.stat(contract_path).st_ctime_ns > _process_start_time_ns(), (
    os.stat(contract_path).st_ctime_ns,
    _process_start_time_ns(),
)
try:
    validate_runtime_contract()
except RuntimeContractError as exc:
    assert 'changed after process start' in str(exc), exc
else:
    raise AssertionError('replacement tree was accepted by an old interpreter')
assert parsers.PARSER_VERSION == loaded_parser_version
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=runtime_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_fd_relative_runtime_rejects_pre_staged_directory_rename(tmp_path):
    root = tmp_path / "deployment"
    live = root / "live"
    staged = root / "staged"
    live.mkdir(parents=True)
    staged.mkdir()
    (live / "module.py").write_text("VALUE = 'old'\n", encoding="utf-8")
    (staged / "module.py").write_text("VALUE = 'new'\n", encoding="utf-8")
    script = f"""
import os
from pathlib import Path
import sys
import time
sys.path.insert(0, {str(ROOT)!r})
from scrapers.whoscored.runtime_contract import (
    RuntimeContractError,
    _open_runtime_root,
    _process_start_time_ns,
    _sha256_relative,
)
started_ns = _process_start_time_ns()
time.sleep(0.1)
os.rename({str(live)!r}, {str(root / "old")!r})
os.rename({str(staged)!r}, {str(live)!r})
descriptor = _open_runtime_root({str(root)!r})
try:
    try:
        _sha256_relative(
            descriptor,
            Path({str(root)!r}),
            'live/module.py',
            process_started_ns=started_ns,
            enforce_directory_immutability=True,
        )
    except RuntimeContractError as exc:
        assert 'directory changed after process start' in str(exc), exc
    else:
        raise AssertionError('pre-staged directory replacement was accepted')
finally:
    os.close(descriptor)
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_production_boundary_rejects_timestamp_valid_malicious_pyc(
    monkeypatch,
    tmp_path,
):
    from scrapers.whoscored import runtime_contract

    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    parser_path = runtime_root / "scrapers" / "whoscored" / "parsers.py"
    clean = parser_path.read_bytes()
    malicious = clean.replace(
        b"whoscored-parser-v8",
        b"whoscored-parser-x8",
        1,
    )
    assert malicious != clean and len(malicious) == len(clean)
    timestamp_ns = 1_700_000_000_000_000_000
    parser_path.write_bytes(malicious)
    os.utime(parser_path, ns=(timestamp_ns, timestamp_ns))
    cache_path = (
        parser_path.parent
        / "__pycache__"
        / f"parsers.cpython-{sys.version_info.major}{sys.version_info.minor}.pyc"
    )
    cache_path.parent.mkdir()
    py_compile.compile(
        str(parser_path),
        cfile=str(cache_path),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP,
    )
    parser_path.write_bytes(clean)
    os.utime(parser_path, ns=(timestamp_ns, timestamp_ns))

    proof = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                f"import sys; sys.path.insert(0, {str(runtime_root)!r}); "
                "from scrapers.whoscored.parsers import PARSER_VERSION; "
                "assert PARSER_VERSION == 'whoscored-parser-x8', PARSER_VERSION"
            ),
        ],
        cwd=runtime_root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert proof.returncode == 0, proof.stderr

    monkeypatch.setattr(
        runtime_contract.sys,
        "pycache_prefix",
        str(runtime_contract._DISABLED_PYCACHE_PREFIX),
    )
    monkeypatch.setattr(runtime_contract.sys, "dont_write_bytecode", True)
    descriptor = runtime_contract._open_runtime_root(runtime_root)
    try:
        with pytest.raises(RuntimeContractError, match="executable bytecode"):
            runtime_contract._validate_python_bytecode_boundary(
                runtime_root,
                root_descriptor=descriptor,
                process_started_ns=None,
                enforce=True,
            )
    finally:
        os.close(descriptor)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("relative", "kind"),
    [
        ("scrapers/whoscored/runtime_contract", "package"),
        ("scrapers/whoscored/parsers.abi3.so", "file"),
        ("scrapers/whoscored/parsers.pyc", "file"),
        ("scrapers/whoscored/parsers", "namespace"),
    ],
)
def test_fd_bootstrap_rejects_importable_sibling_artifacts(
    tmp_path,
    relative,
    kind,
):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    artifact = runtime_root / relative
    if kind == "package":
        artifact.mkdir()
        (artifact / "__init__.py").write_text("RAISED = True\n", encoding="utf-8")
    elif kind == "namespace":
        artifact.mkdir()
    else:
        artifact.write_bytes(b"not executable")
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
try:
    sys._load_whoscored_runtime_contract(root)
except RuntimeError as exc:
    assert 'shadow' in str(exc) or 'candidate' in str(exc), exc
else:
    raise AssertionError('importable sibling artifact was accepted')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_fd_bootstrap_rejects_pythonpath_shadow_before_package_execution(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    shadow_root = tmp_path / "shadow"
    marker = tmp_path / "shadow-executed"
    package = shadow_root / "scrapers"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        f"open({str(marker)!r}, 'w').write('executed')\n",
        encoding="utf-8",
    )
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
try:
    sys._load_whoscored_runtime_contract(root)
except RuntimeError as exc:
    assert 'PYTHONPATH differs' in str(exc), exc
else:
    raise AssertionError('higher-priority PYTHONPATH shadow was accepted')
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join((str(shadow_root), str(runtime_root)))
    completed = subprocess.run(
        [sys.executable, "-S", "-c", script],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not marker.exists()


@pytest.mark.unit
def test_fd_bootstrap_rejects_poisoned_loaded_runtime_module(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
contract = sys._load_whoscored_runtime_contract(root)
bootstrap = sys.modules['_frozen_importlib']
external = sys.modules['_frozen_importlib_external']
fake = type(sys)('scrapers.whoscored.parsers')
loader = external.SourceFileLoader(fake.__name__, '/tmp/poisoned-parsers.py')
fake.__spec__ = bootstrap.ModuleSpec(
    fake.__name__, loader, origin='/tmp/poisoned-parsers.py'
)
fake.__file__ = '/tmp/poisoned-parsers.py'
fake.__loader__ = loader
sys.modules[fake.__name__] = fake
try:
    contract.validate_runtime_import_boundary(runtime_root=root)
except RuntimeError as exc:
    assert 'not the fd-attested source' in str(exc), exc
else:
    raise AssertionError('poisoned loaded module was accepted')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_image_anchor_never_executes_forged_attestor_and_matching_lock(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    contract_path = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    trust_root_path = tmp_path / "runtime-trust-root"
    _write_startup_trust_root(trust_root_path, runtime_root)
    production_gate_path = tmp_path / "whoscored-production-gate"
    _write_successful_production_gate(production_gate_path)

    marker = tmp_path / "forged-attestor-executed"
    attestor_path = runtime_root / "scrapers/whoscored/runtime_contract.py"
    forged_source = (
        f"open({str(marker)!r}, 'w').write('executed')\n"
        "class RuntimeContractError(RuntimeError): pass\n"
        "def validate_runtime_contract(**_kwargs): return {'status': 'success'}\n"
        "def validate_runtime_import_boundary(**_kwargs): return None\n"
    ).encode("utf-8")
    attestor_path.write_bytes(forged_source)
    contract["files"]["scrapers/whoscored/runtime_contract.py"] = hashlib.sha256(
        forged_source
    ).hexdigest()
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': True,
    '_WHOSCORED_TRUST_ROOT_PATH': {str(trust_root_path)!r},
    '_WHOSCORED_ENFORCE_TRUST_OWNERSHIP': False,
    '_WHOSCORED_PRODUCTION_GATE_PATH': {str(production_gate_path)!r},
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
try:
    sys._load_whoscored_runtime_contract(root)
except RuntimeError as exc:
    assert 'differs from the image trust root' in str(exc), exc
else:
    raise AssertionError('forged attestor and matching forged lock were accepted')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not marker.exists()


@pytest.mark.unit
def test_image_anchor_defers_path_rewrite_for_non_whoscored_scripts(tmp_path):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    startup = ROOT / "docker/images/airflow/whoscored_runtime_startup.py"
    sibling = tmp_path / "ordinary-script-dir"
    sibling.mkdir()
    (sibling / "ordinary_peer.py").write_text("VALUE = 17\n", encoding="utf-8")
    script = f"""
import sys
sys.path.insert(0, {str(sibling)!r})
before = tuple(sys.path)
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': {str(runtime_root)!r},
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
assert tuple(sys.path) == before
peer = __import__('ordinary_peer')
assert peer.VALUE == 17
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_image_anchor_keeps_immutable_dependencies_before_application_root(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
stdlib = sys.base_prefix + '/lib/python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
site = sys.prefix + '/lib/python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor) + '/site-packages'
sys.path[:] = [sys.base_prefix + '/bin', root + '/dags', stdlib, site, root]
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
sys._load_whoscored_runtime_contract(root)
assert sys.base_prefix + '/bin' not in sys.path
assert sys.path.index(stdlib) < sys.path.index(site)
assert sys.path.index(site) < sys.path.index(root)
assert sys.path.index(root) < sys.path.index(root + '/dags')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_attested_app_root_cannot_shadow_image_third_party_package(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_runtime_tree(runtime_root)
    marker = tmp_path / "third-party-shadow-executed"
    (runtime_root / "yaml.py").write_text(
        f"open({str(marker)!r}, 'w').write('executed')\n",
        encoding="utf-8",
    )
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
sys._load_whoscored_runtime_contract(root)
import yaml
assert not yaml.__file__.startswith(root + '/'), yaml.__file__
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not marker.exists()


@pytest.mark.unit
def test_cached_production_anchor_rechecks_full_contract_before_second_entrypoint(
    tmp_path,
):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    contract_path = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    trust_root_path = tmp_path / "runtime-trust-root"
    _write_startup_trust_root(trust_root_path, runtime_root)
    production_gate_path = tmp_path / "whoscored-production-gate"
    _write_successful_production_gate(production_gate_path)
    # /proc process starts are clock-tick granular; keep staged files clearly
    # older than the child process for the first successful attestation.
    import time as time_module

    time_module.sleep(0.2)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    changed = runtime_root / "dags/dag_backup_whoscored_storage.py"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': True,
    '_WHOSCORED_TRUST_ROOT_PATH': {str(trust_root_path)!r},
    '_WHOSCORED_ENFORCE_TRUST_OWNERSHIP': False,
    '_WHOSCORED_PRODUCTION_GATE_PATH': {str(production_gate_path)!r},
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
first = sys._load_whoscored_runtime_contract(root)
assert sys._whoscored_runtime_class == 'production-v1'
assert first.require_production_runtime_class(
    operation='production source test'
) == 'production-v1'
first.validate_runtime_contract = lambda **_kwargs: {{'status': 'forged-success'}}
import hashlib
import json
import time
time.sleep(0.1)
changed = {str(changed)!r}
contract_path = {str(contract_path)!r}
with open(changed, 'ab') as handle:
    handle.write(b'\\n# coherent replacement after scheduler start\\n')
with open(contract_path, encoding='utf-8') as handle:
    contract = json.load(handle)
with open(changed, 'rb') as handle:
    contract['files']['dags/dag_backup_whoscored_storage.py'] = hashlib.sha256(
        handle.read()
    ).hexdigest()
with open(contract_path, 'w', encoding='utf-8') as handle:
    json.dump(contract, handle)
try:
    sys._load_whoscored_runtime_contract(root)
except RuntimeError as exc:
    assert 'changed after process start' in str(exc), exc
else:
    raise AssertionError('cached anchor accepted a later coherent deployment')
assert first is sys._whoscored_runtime_contract
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=runtime_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_fd_import_guard_rejects_post_hash_mutation_before_sentinel_exec(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    parser_path = runtime_root / "scrapers/whoscored/parsers.py"
    marker = tmp_path / "mutated-module-executed"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
contract_module = sys._load_whoscored_runtime_contract(root)
from pathlib import Path
descriptor = contract_module._open_runtime_root(Path(root))
try:
    contract_module._install_runtime_import_guard(
        Path(root),
        root_descriptor=descriptor,
        files={contract["files"]!r},
        process_started_ns=10**30,
        enforce=True,
    )
finally:
    contract_module.os.close(descriptor)
parser_path = {str(parser_path)!r}
marker = {str(marker)!r}
with open(parser_path, 'ab') as handle:
    handle.write(("\\nopen(" + repr(marker) + ", 'w').write('executed')\\n").encode())
try:
    __import__('scrapers.whoscored.parsers', fromlist=('PARSER_VERSION',))
except (ImportError, RuntimeError) as exc:
    assert 'changed' in str(exc) or 'source' in str(exc), exc
else:
    raise AssertionError('mutated source imported after hash validation')
assert not Path(marker).exists()
try:
    compile(open(parser_path, 'rb').read(), parser_path, 'exec')
except RuntimeError as exc:
    assert 'changed before compilation' in str(exc), exc
else:
    raise AssertionError('direct loader compilation bypassed the audit guard')
assert not Path(marker).exists()
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_default_catalog_uses_static_bytes_captured_at_hash_barrier(tmp_path):
    runtime_root = tmp_path / "runtime"
    contract = _copy_runtime_tree(runtime_root)
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    catalog_path = runtime_root / "configs/medallion/competitions.yaml"
    script = f"""
import sys
root = {str(runtime_root)!r}
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': root,
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
contract_module = sys._load_whoscored_runtime_contract(root)
from pathlib import Path
descriptor = contract_module._open_runtime_root(Path(root))
try:
    contract_module._install_runtime_import_guard(
        Path(root),
        root_descriptor=descriptor,
        files={contract["files"]!r},
        process_started_ns=10**30,
        enforce=True,
    )
finally:
    contract_module.os.close(descriptor)
catalog_path = Path({str(catalog_path)!r})
original = catalog_path.read_bytes()
mutated = original.replace(
    b'"ENG-Premier League"', b'"PWN-Premier League"', 1
)
assert mutated != original and len(mutated) == len(original)
catalog_path.write_bytes(mutated)
captured = contract_module.read_attested_static_runtime_file(
    'configs/medallion/competitions.yaml'
)
assert captured == original
assert contract_module.attested_runtime_file_sha256(
    'scripts/flaresolverr_extended.py'
) == {contract["files"]["scripts/flaresolverr_extended.py"]!r}
from scrapers.whoscored.catalog import WhoScoredCatalog
catalog = WhoScoredCatalog.from_file()
competition_ids = {{item.competition_id for item in catalog.competitions}}
assert 'ENG-Premier League' in competition_ids
assert 'PWN-Premier League' not in competition_ids
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=runtime_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
@pytest.mark.parametrize("replacement", ["root", "subtree"])
def test_static_guard_rejects_atomic_runtime_directory_replacement(
    tmp_path,
    replacement,
):
    live = tmp_path / "live"
    staged = tmp_path / "staged"
    contract = _copy_runtime_tree(live)
    _copy_runtime_tree(staged)
    startup = live / "docker/images/airflow/whoscored_runtime_startup.py"
    script = f"""
import os
from pathlib import Path
import sys
root = Path({str(live)!r})
path = {str(startup)!r}
namespace = {{
    '__builtins__': __builtins__,
    'sys': sys,
    '_WHOSCORED_RUNTIME_ROOT': str(root),
    '_WHOSCORED_REQUIRE_FULL_ATTESTATION': False,
}}
exec(compile(open(path, 'rb').read(), path, 'exec'), namespace)
contract_module = sys._load_whoscored_runtime_contract(str(root))
descriptor = contract_module._open_runtime_root(root)
try:
    contract_module._install_runtime_import_guard(
        root,
        root_descriptor=descriptor,
        files={contract["files"]!r},
        process_started_ns=10**30,
        enforce=True,
    )
finally:
    contract_module.os.close(descriptor)
replacement = {replacement!r}
staged = Path({str(staged)!r})
if replacement == 'root':
    os.rename(root, {str(tmp_path / "old-root")!r})
    os.rename(staged, root)
else:
    os.rename(root / 'configs/medallion', {str(tmp_path / "old-medallion")!r})
    os.rename(staged / 'configs/medallion', root / 'configs/medallion')
try:
    contract_module.read_attested_static_runtime_file(
        'configs/medallion/competitions.yaml'
    )
except RuntimeError as exc:
    expected = 'root path changed' if replacement == 'root' else 'subtree changed'
    assert expected in str(exc), exc
else:
    raise AssertionError('atomic runtime directory replacement was accepted')
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.unit
def test_airflow_source_pool_contract_rejects_name_and_size_drift(monkeypatch):
    from scrapers.whoscored import runtime_contract

    monkeypatch.setenv("WHOSCORED_SOURCE_POOL_SLOTS", "4")
    monkeypatch.setattr(runtime_contract, "_airflow_pool_slots", lambda _pool: 4)
    assert (
        runtime_contract.validate_airflow_source_pool(
            direct_pool="whoscored_direct_pool",
            backfill_pool="whoscored_direct_pool",
        )["actual_slots"]
        == 4
    )

    with pytest.raises(RuntimeContractError, match="must share one Airflow source"):
        runtime_contract.validate_airflow_source_pool(
            direct_pool="whoscored_direct_pool",
            backfill_pool="separate_pool",
        )

    monkeypatch.setattr(runtime_contract, "_airflow_pool_slots", lambda _pool: 3)
    with pytest.raises(RuntimeContractError, match="pool size mismatch"):
        runtime_contract.validate_airflow_source_pool(
            direct_pool="whoscored_direct_pool",
            backfill_pool="whoscored_direct_pool",
        )
