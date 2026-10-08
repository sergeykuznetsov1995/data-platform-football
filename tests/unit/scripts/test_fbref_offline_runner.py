"""Portable offline invocation and the production Compose boundary."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
EXTRAS = (
    'tests/unit/dags/test_dag_iceberg_maintenance_daily.py',
    'tests/unit/dags/test_maintenance_tasks.py',
    'tests/unit/scripts/test_filter_proxy.py',
    'tests/unit/scrapers/test_proxy_manager.py',
    'tests/unit/scrapers/test_scrapers_lazy_import.py',
)


def _repo(tmp_path, *, populated=True, fake_pytest=True):
    repo = tmp_path / 'checkout with spaces'
    script = repo / 'scripts/ci/run_fbref_offline.py'
    script.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / 'scripts/ci/run_fbref_offline.py', script)
    shutil.copyfile(ROOT / 'Makefile', repo / 'Makefile')
    if populated:
        for name in ('tests/unit/nested dir/test_fbref_sample.py',
                     'tests/unit/test_fbref_root.py', *EXTRAS):
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('def test_offline():\n    assert True\n')
        # The old find -type f selection did not follow matching symlink files.
        (repo / 'tests/unit/test_fbref_link.py').symlink_to(
            repo / 'tests/unit/test_fbref_root.py'
        )
        # Neither non-FBref unit tests nor matching integration files belong
        # to the established offline selection.
        for name in ('tests/unit/test_other.py', 'tests/integration/test_fbref_live.py'):
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('def test_excluded():\n    assert False\n')
    if fake_pytest:
        (repo / 'pytest.py').write_text(
            'import json, os, pathlib, sys\n'
            'pathlib.Path(os.environ["CAPTURE"]).write_text(json.dumps({\n'
            '"argv": sys.argv[1:], "cwd": os.getcwd(),\n'
            '"env": {key: os.environ.get(key) for key in '
            '["PYTEST_ADDOPTS", "PYTEST_PLUGINS", "PYTEST_DISABLE_PLUGIN_AUTOLOAD"]}}))\n'
            'sys.exit(int(os.environ.get("TEST_EXIT", "0")))\n'
        )
    return repo


def _env(repo):
    return dict(os.environ, PYTHONPATH=str(repo), CAPTURE=str(repo / 'capture.json'),
                PYTEST_ADDOPTS='--collect-only -k never_matches',
                PYTEST_PLUGINS='plugin_that_does_not_exist',
                PYTEST_DISABLE_PLUGIN_AUTOLOAD='0')


def test_runner_uses_its_checkout_and_exact_original_selection(tmp_path):
    repo = _repo(tmp_path)
    result = subprocess.run([sys.executable, str(repo / 'scripts/ci/run_fbref_offline.py')],
                            cwd=tmp_path, env=_env(repo), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    capture = json.loads((repo / 'capture.json').read_text())
    assert capture['cwd'] == str(repo)
    assert capture['argv'] == ['-q', 'tests/unit/nested dir/test_fbref_sample.py',
                               'tests/unit/test_fbref_root.py', *EXTRAS]
    assert capture['env'] == {'PYTEST_ADDOPTS': None, 'PYTEST_PLUGINS': None,
                              'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1'}


@pytest.mark.parametrize('code', [1, 2, 5])
def test_runner_preserves_pytest_failure_exit(tmp_path, code):
    repo = _repo(tmp_path)
    result = subprocess.run([sys.executable, str(repo / 'scripts/ci/run_fbref_offline.py')],
                            env=dict(_env(repo), TEST_EXIT=str(code)), capture_output=True)
    assert result.returncode == code


def test_empty_selection_fails_without_invoking_pytest(tmp_path):
    repo = _repo(tmp_path, populated=False)
    result = subprocess.run([sys.executable, str(repo / 'scripts/ci/run_fbref_offline.py')],
                            env=_env(repo), capture_output=True, text=True)
    assert result.returncode == 1
    assert 'No offline FBref tests' in result.stderr
    assert not (repo / 'capture.json').exists()


def test_missing_pytest_fails_with_preparation_instruction(tmp_path):
    repo = _repo(tmp_path, fake_pytest=False)
    result = subprocess.run([sys.executable, '-S', str(repo / 'scripts/ci/run_fbref_offline.py')],
                            env=_env(repo), capture_output=True, text=True)
    assert result.returncode == 1
    assert 'pytest is missing' in result.stderr
    assert 'requirements/test/fbref-unit-py311.lock' in result.stderr


def test_real_pytest_runs_matching_unit_and_all_extras(tmp_path):
    repo = _repo(tmp_path, fake_pytest=False)
    result = subprocess.run([sys.executable, str(repo / 'scripts/ci/run_fbref_offline.py')],
                            env=_env(repo), capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert '7 passed' in result.stdout
    # A missing required extra must fail; it cannot quietly reduce coverage.
    (repo / EXTRAS[-1]).unlink()
    result = subprocess.run([sys.executable, str(repo / 'scripts/ci/run_fbref_offline.py')],
                            env=_env(repo), capture_output=True, text=True)
    assert result.returncode != 0
    assert 'file or directory not found' in result.stderr


@pytest.mark.parametrize('override', [False, True])
def test_make_runs_local_python_without_docker(tmp_path, override):
    repo = _repo(tmp_path)
    tools = tmp_path / 'tools with spaces'
    tools.mkdir()
    marker = tmp_path / 'docker-called'
    docker = tools / 'docker'
    docker.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\nexit 99\n')
    docker.chmod(0o755)
    python = tools / 'python3'
    python.write_text('#!/bin/sh\nexec "' + sys.executable + '" "$@"\n')
    python.chmod(0o755)
    command = ['make', 'test-fbref-offline']
    if override:
        command.append('PYTHON=' + str(python))
    result = subprocess.run(command, cwd=repo,
                            env=dict(_env(repo), PATH=str(tools) + os.pathsep + os.environ['PATH']),
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (repo / 'capture.json').exists()
    assert not marker.exists()


def test_compose_still_rejects_scheduler_exec_before_docker(tmp_path):
    marker = tmp_path / 'docker-called'
    docker = tmp_path / 'docker'
    docker.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\nexit 99\n')
    docker.chmod(0o755)
    result = subprocess.run(['bash', str(ROOT / 'scripts/compose.sh'),
                             'exec', 'airflow-scheduler', 'python', '-m', 'pytest'],
                            cwd=ROOT, env=dict(os.environ, PATH=str(tmp_path) + os.pathsep + os.environ['PATH']),
                            capture_output=True, text=True)
    assert result.returncode == 78
    assert 'production-gated WhoScored service' in result.stderr
    assert not marker.exists()
