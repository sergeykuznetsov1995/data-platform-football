# Source test profiles

These hash locks cover the complete direct and transitive test dependencies for
Linux x86_64 and the CPython minor shown in each filename. They contain the wheels
selected for that platform; installation on another architecture or Python minor
can fail by design. They are test profiles, not production or Airflow constraints.
The `.in` files preserve every previous direct dependency and pin it to the same
successful CI baseline as the lock. None of these profiles installs Airflow.

| Profile | Python | Successful CI baseline / job |
| --- | --- | --- |
| `fbref-unit-py311` | 3.11 | [37644381187](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/37644381187), `unit`, 2026-10-07 |
| `fbref-postgres-py311` | 3.11 | same run, `postgres-semantics` |
| `fbref-s3-py311` | 3.11 | same run, `seaweedfs-raw-smoke` |
| `sofascore-unit-py311` | 3.11 | [37629880830](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/37629880830), `unit`, 2026-10-07 |
| `sofascore-discovery-py312` | 3.12 | [37629880837](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/37629880837), `validate`, 2026-10-07 |
| `fotmob-recipe-py312` | 3.12 | [37445270245](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/37445270245), `recipe`, 2026-10-06 |

All versions come from each job's `Successfully installed` log. Wheel SHA-256
hashes were computed from those exact downloaded versions; dependency closure was
checked by installation into clean venvs with `--require-hashes` and `pip check`.
Existing explicit pins remain unchanged, including Ruff 0.15.20, boto3 1.42.61,
Jinja2 3.1.6, tls-client-python 1.15.1, sqlglot 30.12.0 and duckdb 1.5.4.
Discovery's typing-extensions stays at 4.15.0. The fresh SofaScore baseline includes
the current SQL dependencies; the older run 31415840227 predates that profile.
Ruff's pin prevents upstream rule changes from silently changing the merge gate;
SofaScore's SQL type and execution tests need sqlglot and duckdb.

## Local preparation

Run these commands from an isolated development checkout on Linux x86_64 with
Python 3.11 available. Create a venv outside any production or mounted tree:

```bash
python3.11 -m venv /tmp/dpf-fbref-test
. /tmp/dpf-fbref-test/bin/activate
python -m pip install --disable-pip-version-check --require-hashes \
  -r requirements/test/fbref-unit-py311.lock
python -m pip check
make test-fbref-offline
```

Make uses the activated venv's `python3` by default. An explicit interpreter also
works: `make test-fbref-offline PYTHON=/path/to/test-venv/bin/python` (quote the whole
`PYTHON=...` argument when the path has spaces). CI runs the same
`scripts/ci/run_fbref_offline.py` through its Python 3.11 interpreter. The runner
selects every `tests/unit/**/*fbref*.py` plus the five existing maintenance/proxy
extras. It clears inherited `PYTEST_ADDOPTS` and `PYTEST_PLUGINS`, disables automatic
plugin loading, fails if pytest or the FBref selection is missing, and returns the
pytest exit status. The runner prepares no environment and uses no containers.

Offline here means no requests to sources or production services. Some existing
control/proxy tests start disposable TCP servers on `127.0.0.1`. An environment
that forbids all socket creation cannot run those fixtures; keep plugin autoload
disabled and use the normal approval path for the isolated loopback tests rather
than changing the Compose wrapper or testing in production.

For SofaScore unit tests use the analogous `sofascore-unit-py311.lock` environment
and the unchanged selection in `sofascore-ci.yml`. Discovery validate and FotMob
recipe use Python 3.12 with their corresponding locks and existing workflow
selections. The PostgreSQL and S3 integration checks still use CI's ephemeral
services. FotMob's Compose render remains a separate CI check; the runner does
not render Compose or enter the shared scheduler. The scheduled SofaScore
`discover` job retains its existing direct installation and schedule.

A prepared wheelhouse makes subsequent installation independent of the index:

```bash
python -m pip download --only-binary=:all: --require-hashes \
  -r requirements/test/fbref-unit-py311.lock -d /tmp/dpf-fbref-wheels
# In a second clean Python 3.11 venv:
python -m pip install --no-index --find-links /tmp/dpf-fbref-wheels \
  --require-hashes -r requirements/test/fbref-unit-py311.lock
python -m pip check
```

## Updating a profile

Dependency updates are deliberate changes. Start from a clean venv on the same
Linux architecture and Python minor. Keep the `.in` direct dependencies and their
pins unless the dependency change itself is intended. Prefer a matching successful
CI job's complete installed-version log as the baseline; record its run/job/date.
If no suitable complete log exists, install the original profile command in a
clean venv, run the unchanged source tests, and record that tested baseline first.
Do not install the broad `requirements-ci.txt` into Python 3.11: some pins target
Python 3.12.

Install the intended `.in`, run `pip check` and the affected source tests, then
capture `python -m pip freeze` in a temporary file. Download the complete frozen
closure with `pip download --only-binary=:all: -r <freeze-file> -d <wheelhouse>`.
For each wheel record its metadata name/version and SHA-256 in the lock using the
existing `name==version --hash=sha256:...` format. Exclude pip/setuptools tooling
when it is not a profile dependency. Verify the resulting lock in a second clean
venv with `--require-hashes`, run `pip check`, rerun the existing source selection
and workflow static checks, and confirm a changed hash is rejected. Review the
version diff and update this provenance before delivery. Each workflow's trigger
paths include its `.in`/`.lock`; its pip cache key uses the exact lock. Airflow's
release constraints and matrices are maintained independently.
