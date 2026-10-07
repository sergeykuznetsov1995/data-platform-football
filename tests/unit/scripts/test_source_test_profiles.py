"""Source CI locks retain their dependencies, triggers and isolation contracts."""

import fnmatch
from pathlib import Path
import re

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
PROFILES = (
    ('fbref-ci.yml', 'unit', 'fbref-unit-py311', '3.11',
     'pytest ruff pandas numpy pyarrow boto3 trino requests beautifulsoup4 lxml psycopg2-binary python-dotenv tenacity pybreaker rapidfuzz unidecode nicknames pyyaml jsonschema psutil jinja2'),
    ('fbref-ci.yml', 'postgres-semantics', 'fbref-postgres-py311', '3.11',
     'pytest psycopg2-binary'),
    ('fbref-ci.yml', 'seaweedfs-raw-smoke', 'fbref-s3-py311', '3.11',
     'pytest pyarrow boto3 psycopg2-binary'),
    ('sofascore-ci.yml', 'unit', 'sofascore-unit-py311', '3.11',
     'pytest ruff pandas numpy pyarrow trino requests pyyaml jinja2 jsonschema tls-client-python python-dotenv tenacity pybreaker rapidfuzz unidecode nicknames sqlglot duckdb'),
    ('sofascore-discovery.yml', 'validate', 'sofascore-discovery-py312', '3.12',
     'pytest pyyaml requests pandas typing-extensions tls-client-python'),
    ('fotmob-ci.yml', 'recipe', 'fotmob-recipe-py312', '3.12', 'pytest pyyaml'),
)


def _requirements(path, *, hashed=False):
    dependencies = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        match = re.fullmatch(r'([\w.-]+)==([\w.+-]+)' +
                             (r' --hash=sha256:[0-9a-f]{64}' if hashed else ''), line)
        assert match, (path, line)
        name = match[1].lower().replace('_', '-')
        assert name not in dependencies, (path, name)
        dependencies[name] = match[2]
    assert dependencies
    return dependencies


@pytest.mark.parametrize('workflow,job,profile,python,direct', PROFILES)
def test_source_gate_uses_complete_hash_lock_and_keeps_original_direct_dependencies(
    workflow, job, profile, python, direct,
):
    config = yaml.safe_load((ROOT / '.github/workflows' / workflow).read_text())
    lock_path = f'requirements/test/{profile}.lock'
    inputs_path = f'requirements/test/{profile}.in'
    lock = _requirements(ROOT / lock_path, hashed=True)
    inputs = _requirements(ROOT / inputs_path)
    assert set(inputs) == set(direct.split())
    assert all(lock[name] == version for name, version in inputs.items())
    assert not any(name.startswith('apache-airflow') for name in lock)
    steps = config['jobs'][job]['steps']
    setup = next(step['with'] for step in steps if step.get('uses', '').startswith('actions/setup-python@'))
    assert setup['python-version'] == python
    assert setup['cache'] == 'pip'
    assert setup['cache-dependency-path'] == lock_path
    install = next(step['run'] for step in steps if 'pip install' in step.get('run', ''))
    assert '--require-hashes' in install
    assert f'-r {lock_path}' in install
    assert 'python -m pip check' in install
    triggers = config.get('on', config.get(True))
    for event in ('pull_request', 'push'):
        if event not in triggers:
            continue
        paths = triggers[event]['paths']
        assert any(fnmatch.fnmatchcase(lock_path, pattern) for pattern in paths)
        assert any(fnmatch.fnmatchcase(inputs_path, pattern) for pattern in paths)


def test_existing_version_pins_remain_unchanged():
    expected = {
        'fbref-unit-py311': {'boto3': '1.42.61', 'ruff': '0.15.20', 'jinja2': '3.1.6'},
        'fbref-s3-py311': {'boto3': '1.42.61'},
        'sofascore-unit-py311': {'ruff': '0.15.20', 'tls-client-python': '1.15.1',
                                  'sqlglot': '30.12.0', 'duckdb': '1.5.4'},
        'sofascore-discovery-py312': {'typing-extensions': '4.15.0', 'tls-client-python': '1.15.1'},
    }
    for profile, versions in expected.items():
        lock = _requirements(ROOT / f'requirements/test/{profile}.lock', hashed=True)
        assert all(lock[name] == version for name, version in versions.items())


def test_fbref_ci_and_make_share_the_same_offline_runner():
    config = yaml.safe_load((ROOT / '.github/workflows/fbref-ci.yml').read_text())
    step = next(step for step in config['jobs']['unit']['steps']
                if step.get('name') == 'Complete offline FBref suite')
    assert step['run'] == 'python scripts/ci/run_fbref_offline.py'
    make_target = (ROOT / 'Makefile').read_text().split('test-fbref-offline:\n', 1)[1].split('\n\n', 1)[0]
    assert '"$(PYTHON)" scripts/ci/run_fbref_offline.py' in make_target


def test_discovery_schedule_keeps_its_existing_direct_runtime():
    config = yaml.safe_load((ROOT / '.github/workflows/sofascore-discovery.yml').read_text())
    discover = config['jobs']['discover']
    install = next(step['run'] for step in discover['steps'] if 'pip install' in step.get('run', ''))
    assert install == ('python -m pip install --disable-pip-version-check \\\n'
                       '  "typing-extensions==4.15.0" "tls-client-python==1.15.1"\n'
                       'python -c "from tls_client import Session; print(Session.__module__)"\n')
    assert 'self-hosted' in discover['runs-on']
    assert {row['cron'] for row in config.get('on', config.get(True))['schedule']} == {
        '17 4 * * 1-6', '17 4 * * 0',
    }
