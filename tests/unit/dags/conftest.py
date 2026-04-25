"""
Pytest fixtures for unit tests of Airflow DAGs.

The host where these unit tests run does NOT have Airflow installed
(scrapers/DAG runtime lives inside the airflow container). To exercise
DAG modules as plain Python on the host we install lightweight stubs
into ``sys.modules`` for every Airflow API the DAGs touch.

The stubs intentionally do as little as possible: ``@dag`` and ``@task``
decorators just return the wrapped function so that ``check_alerts``
remains a callable we can test directly. ``Variable.get`` returns the
``default_var`` so we never try to talk to a metadata DB.

Tests that need different behaviour (e.g. ``Variable.get`` returning a
custom JSON) can still ``monkeypatch`` the stub.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# 1. Make sure the project root + dags/ are on sys.path so test files can
#    `import dags.dag_superset_alerts` and `from utils.alerts import ...`.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"

for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ---------------------------------------------------------------------------
# 2. Install minimal Airflow stubs BEFORE any test imports a DAG module.
# ---------------------------------------------------------------------------
def _install_airflow_stubs() -> None:
    """Idempotent — only inject stubs if real Airflow is missing."""
    try:
        import airflow  # noqa: F401
        # Even if a top-level airflow package exists, the submodules we
        # need may not — the import below will raise and we fall through.
        from airflow.decorators import dag as _real_dag  # noqa: F401
        from airflow.models import Variable as _real_var  # noqa: F401
        return
    except Exception:
        pass

    # ---- airflow + airflow.decorators -----------------------------------
    airflow_mod = types.ModuleType("airflow")
    decorators_mod = types.ModuleType("airflow.decorators")
    models_mod = types.ModuleType("airflow.models")
    exceptions_mod = types.ModuleType("airflow.exceptions")
    operators_mod = types.ModuleType("airflow.operators")
    operators_python_mod = types.ModuleType("airflow.operators.python")
    operators_trigger_mod = types.ModuleType("airflow.operators.trigger_dagrun")

    def _dag_decorator(*dargs, **dkwargs):
        """Pass-through @dag decorator.

        Behaves like the real Airflow ``@dag`` from a unit-test POV:
        decorating a function returns a callable that, when invoked,
        runs the function (so the inner @task definitions execute).
        """

        def _wrap(fn):
            def _factory(*a, **kw):
                # Real Airflow returns a DAG instance; for unit tests we
                # just need *something* truthy so module load succeeds.
                fn(*a, **kw)
                stub_dag = MagicMock(name="StubDAG")
                stub_dag.dag_id = dkwargs.get("dag_id", fn.__name__)
                stub_dag.schedule = dkwargs.get("schedule")
                stub_dag.tags = dkwargs.get("tags", [])
                stub_dag.tasks = []
                # Surface the metadata so tests can assert on it
                stub_dag._dag_kwargs = dkwargs
                return stub_dag

            _factory._dag_kwargs = dkwargs
            _factory._wrapped = fn
            return _factory

        return _wrap

    def _task_decorator(*targs, **tkwargs):
        """Pass-through @task decorator that yields the original callable."""
        # Two call patterns: @task or @task(...). Disambiguate by argc.
        if len(targs) == 1 and callable(targs[0]) and not tkwargs:
            return targs[0]

        def _wrap(fn):
            return fn

        return _wrap

    decorators_mod.dag = _dag_decorator
    decorators_mod.task = _task_decorator

    # ---- airflow.models.Variable ----------------------------------------
    class _StubVariable:
        """Minimal stand-in for airflow.models.Variable.

        ``get`` honours ``default_var`` so DAG load never blows up looking
        for a metadata DB. Tests can monkeypatch
        ``airflow.models.Variable.get`` to inject custom values.
        """

        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            if default_var is not None:
                return default_var
            raise KeyError(key)

    models_mod.Variable = _StubVariable

    # ---- airflow.exceptions.AirflowException ----------------------------
    class _AirflowException(Exception):
        pass

    exceptions_mod.AirflowException = _AirflowException

    # ---- airflow.operators.python.PythonOperator (for completeness) -----
    class _PythonOperator:
        def __init__(self, *a, **kw):
            self.task_id = kw.get("task_id", "stub")
            self.python_callable = kw.get("python_callable")
            self.op_kwargs = kw.get("op_kwargs", {})

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return other

    operators_python_mod.PythonOperator = _PythonOperator

    class _TriggerDagRunOperator(_PythonOperator):
        pass

    operators_trigger_mod.TriggerDagRunOperator = _TriggerDagRunOperator

    # ---- register --------------------------------------------------------
    sys.modules.setdefault("airflow", airflow_mod)
    sys.modules.setdefault("airflow.decorators", decorators_mod)
    sys.modules.setdefault("airflow.models", models_mod)
    sys.modules.setdefault("airflow.exceptions", exceptions_mod)
    sys.modules.setdefault("airflow.operators", operators_mod)
    sys.modules.setdefault("airflow.operators.python", operators_python_mod)
    sys.modules.setdefault(
        "airflow.operators.trigger_dagrun", operators_trigger_mod
    )

    # Cross-link so `from airflow.decorators import dag` works through
    # both the ``airflow`` package and the explicit submodule path.
    airflow_mod.decorators = decorators_mod
    airflow_mod.models = models_mod
    airflow_mod.exceptions = exceptions_mod
    airflow_mod.operators = operators_mod


_install_airflow_stubs()


# ---------------------------------------------------------------------------
# 3. Shared fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def telegram_env(monkeypatch):
    """Set Telegram env vars so send_telegram_message can dispatch."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-123")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("ALERT_ENV", "test")
    yield


@pytest.fixture
def no_telegram_env(monkeypatch):
    """Ensure Telegram env vars are NOT set."""
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    yield


@pytest.fixture
def superset_env(monkeypatch):
    """Provide Superset connection env vars used by the alerts DAG."""
    monkeypatch.setenv("SUPERSET_URL", "http://superset.test:8088")
    monkeypatch.setenv("SUPERSET_ADMIN_USERNAME", "admin")
    monkeypatch.setenv("SUPERSET_ADMIN_PASSWORD", "secret-pwd")
    yield
