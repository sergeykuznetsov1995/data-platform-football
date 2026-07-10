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
from unittest.mock import MagicMock

import pytest


# sys.path setup (project root + dags/ so test files can
# `import dags.dag_superset_alerts` and `from utils.alerts import ...`) is
# centralised in the root conftest.py.


# ---------------------------------------------------------------------------
# Install minimal Airflow stubs BEFORE any test imports a DAG module.
# ---------------------------------------------------------------------------
def _install_airflow_stubs() -> None:
    """Idempotent — only inject stubs if real Airflow is missing.

    NOTE: ``/root/data_platform/airflow`` exists as a namespace package
    (config/plugins dirs only) and Python eagerly loads it into
    ``sys.modules['airflow']``. We must therefore *evict* any existing
    ``airflow*`` entries before installing our stubs, otherwise
    ``sys.modules.setdefault`` is a no-op and the namespace package wins.
    """
    try:
        # If a real Airflow with the APIs we need is installed, leave it.
        from airflow.decorators import dag as _real_dag  # noqa: F401
        from airflow.models import Variable as _real_var  # noqa: F401
        from airflow.operators.bash import BashOperator as _real_bash  # noqa: F401
        return
    except Exception:
        pass

    # Evict any partial / namespace-package ``airflow`` entries so our
    # ``sys.modules[...] = stub_mod`` writes below are authoritative.
    for _name in list(sys.modules):
        if _name == "airflow" or _name.startswith("airflow."):
            del sys.modules[_name]

    # ---- airflow + airflow.decorators -----------------------------------
    airflow_mod = types.ModuleType("airflow")
    decorators_mod = types.ModuleType("airflow.decorators")
    models_mod = types.ModuleType("airflow.models")
    exceptions_mod = types.ModuleType("airflow.exceptions")
    operators_mod = types.ModuleType("airflow.operators")
    operators_python_mod = types.ModuleType("airflow.operators.python")
    operators_trigger_mod = types.ModuleType("airflow.operators.trigger_dagrun")
    operators_bash_mod = types.ModuleType("airflow.operators.bash")
    sensors_mod = types.ModuleType("airflow.sensors")
    sensors_ext_mod = types.ModuleType("airflow.sensors.external_task")
    utils_mod = types.ModuleType("airflow.utils")
    utils_tg_mod = types.ModuleType("airflow.utils.task_group")

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

    # ---- airflow.models.param.Param -------------------------------------
    # UI-configurable DAG params (e.g. season backfill in dag_ingest_*). The
    # stub just stores ``default`` so ``params={'season': Param(default=...)}``
    # captures cleanly and tests can read the default back.
    models_param_mod = types.ModuleType("airflow.models.param")

    class _Param:
        def __init__(self, default=None, **kw):
            self.value = default
            self.default = default
            self._kw = kw

    models_param_mod.Param = _Param
    models_mod.param = models_param_mod

    # ---- airflow.exceptions.AirflowException ----------------------------
    class _AirflowException(Exception):
        pass

    exceptions_mod.AirflowException = _AirflowException

    # ---- airflow.operators.python.PythonOperator (for completeness) -----
    class _PythonOperator:
        # Class-level registry — every instance appends itself so DAG-load
        # tests can inspect the operators that were created. Tests that
        # rely on a clean slate should call ``_PythonOperator._instances.clear()``
        # (or ``BashOperator._instances.clear()``) before re-importing the DAG.
        _instances: list = []

        def __init__(self, *a, **kw):
            self.task_id = kw.get("task_id", "stub")
            self.python_callable = kw.get("python_callable")
            self.op_kwargs = kw.get("op_kwargs", {})
            # Per-instance bookkeeping for upstream/downstream tracking via
            # >>/<<. These start empty and get populated by __rshift__ /
            # __lshift__ so DAG-topology tests can verify ``X >> Y >> Z``
            # produced the expected dependency edges.
            self.upstream_task_ids: set = set()
            self.downstream_task_ids: set = set()
            # If we're inside a TaskGroup ``with`` block, prepend the group_id
            # to mirror Airflow's task_id formatting (``group.task``). The
            # active-group stack is a class attribute on _TaskGroup below.
            try:
                if _TaskGroup._stack:
                    group = _TaskGroup._stack[-1]
                    self.task_id = f"{group.group_id}.{self.task_id}"
                    group.children.append(self)
            except NameError:
                # _TaskGroup not yet defined at module-import time
                pass
            _PythonOperator._instances.append(self)

        def _add_downstream(self, other):
            """Record ``self >> other`` for both PythonOperators and TaskGroups."""
            if isinstance(other, _PythonOperator):
                self.downstream_task_ids.add(other.task_id)
                other.upstream_task_ids.add(self.task_id)
            elif isinstance(other, _TaskGroup):
                # X >> group  —> X is upstream of every leaf in the group.
                for child in other.children:
                    if isinstance(child, _PythonOperator):
                        self.downstream_task_ids.add(child.task_id)
                        child.upstream_task_ids.add(self.task_id)

        def __rshift__(self, other):
            self._add_downstream(other)
            return other

        def __lshift__(self, other):
            if isinstance(other, _PythonOperator):
                other._add_downstream(self)
            elif isinstance(other, _TaskGroup):
                for child in other.children:
                    if isinstance(child, _PythonOperator):
                        child._add_downstream(self)
            return other

        def __rrshift__(self, other):
            # Airflow fan-in: ``[a, b, ...] >> self``. Record each upstream
            # item that tracks dependencies (PythonOperators); tolerate ones
            # that don't (the lightweight _BashOperator stub). Returns self so
            # ``[list] >> a >> b`` keeps chaining from ``a``.
            for item in other:
                if isinstance(item, _PythonOperator):
                    item._add_downstream(self)
            return self

    operators_python_mod.PythonOperator = _PythonOperator

    class _TriggerDagRunOperator(_PythonOperator):
        pass

    operators_trigger_mod.TriggerDagRunOperator = _TriggerDagRunOperator

    # ShortCircuitOperator behaves like PythonOperator for DAG-load tests
    # (instance-tracked via the shared registry); its python_callable is the
    # gate hook. Subclass so the gate task is discoverable by _python_task().
    class _ShortCircuitOperator(_PythonOperator):
        pass

    operators_python_mod.ShortCircuitOperator = _ShortCircuitOperator

    # ---- airflow.operators.bash.BashOperator ----------------------------
    # Records every ctor kwarg so tests can assert on `append_env`,
    # `bash_command`, `env`, etc. exactly the same way a real BashOperator
    # would expose them.
    class _BashOperator:
        # Class-level registry — every instance appends itself so DAG-load
        # tests can inspect the operators that were created.
        _instances: list = []

        def __init__(self, *a, **kw):
            self.task_id = kw.get("task_id", "stub")
            self.bash_command = kw.get("bash_command")
            self.env = kw.get("env")
            self.append_env = kw.get("append_env", False)
            self._init_kwargs = dict(kw)
            _BashOperator._instances.append(self)

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return other

    operators_bash_mod.BashOperator = _BashOperator

    # ---- airflow.operators.empty.EmptyOperator --------------------------
    # No-op marker operator (start/end markers in dag_transform_e3 etc.).
    # Behaves like PythonOperator for DAG-load tests (instance-tracked,
    # supports >>/<< topology wiring).
    operators_empty_mod = types.ModuleType("airflow.operators.empty")

    class _EmptyOperator(_PythonOperator):
        pass

    operators_empty_mod.EmptyOperator = _EmptyOperator

    # ---- airflow.sensors.external_task.ExternalTaskSensor ---------------
    class _ExternalTaskSensor(_PythonOperator):
        pass

    sensors_ext_mod.ExternalTaskSensor = _ExternalTaskSensor

    # ---- airflow.utils.task_group.TaskGroup -----------------------------
    class _TaskGroup:
        # Stack of active groups so PythonOperators created inside ``with``
        # blocks can prepend the group_id and register themselves as children.
        _stack: list = []

        def __init__(self, *a, **kw):
            self.group_id = kw.get("group_id", a[0] if a else "stub")
            self.children = []

        def __enter__(self):
            _TaskGroup._stack.append(self)
            return self

        def __exit__(self, *a):
            _TaskGroup._stack.pop()
            return False

        # Real Airflow TaskGroup supports >> / << for cross-group deps;
        # the smoke-tested DAGs do `tg1 >> tg2 >> task3`. Translate this
        # into an all-leaves-of-A → all-roots-of-B fanout so per-task
        # ``upstream_task_ids`` accurately reflects the topology a real
        # Airflow scheduler would build.
        def _add_downstream(self, other):
            if isinstance(other, _PythonOperator):
                for child in self.children:
                    if isinstance(child, _PythonOperator):
                        child.downstream_task_ids.add(other.task_id)
                        other.upstream_task_ids.add(child.task_id)
            elif isinstance(other, _TaskGroup):
                for left in self.children:
                    for right in other.children:
                        if isinstance(left, _PythonOperator) and isinstance(right, _PythonOperator):
                            left.downstream_task_ids.add(right.task_id)
                            right.upstream_task_ids.add(left.task_id)

        def __rshift__(self, other):
            self._add_downstream(other)
            return other

        def __lshift__(self, other):
            if isinstance(other, _PythonOperator):
                for child in self.children:
                    if isinstance(child, _PythonOperator):
                        other.downstream_task_ids.add(child.task_id)
                        child.upstream_task_ids.add(other.task_id)
            elif isinstance(other, _TaskGroup):
                for left in other.children:
                    for right in self.children:
                        if isinstance(left, _PythonOperator) and isinstance(right, _PythonOperator):
                            left.downstream_task_ids.add(right.task_id)
                            right.upstream_task_ids.add(left.task_id)
            return other

    utils_tg_mod.TaskGroup = _TaskGroup

    # ---- airflow.DAG context manager ------------------------------------
    # Real Airflow exposes DAG at the top level (`from airflow import DAG`)
    # AND as `airflow.models.DAG`. Provide a no-op context manager so
    # `with DAG(...) as dag:` blocks execute their body for kwarg capture.
    class _StubDAG:
        def __init__(self, *a, **kw):
            self._dag_kwargs = dict(kw)
            self.dag_id = kw.get("dag_id")
            self.schedule = kw.get("schedule")
            self.tags = kw.get("tags", [])
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    airflow_mod.DAG = _StubDAG
    models_mod.DAG = _StubDAG

    # ---- register --------------------------------------------------------
    # Use direct assignment (not setdefault) — there may be a stale namespace
    # package already in sys.modules; we want our stubs to win.
    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.decorators"] = decorators_mod
    sys.modules["airflow.models"] = models_mod
    sys.modules["airflow.models.param"] = models_param_mod
    sys.modules["airflow.exceptions"] = exceptions_mod
    sys.modules["airflow.operators"] = operators_mod
    sys.modules["airflow.operators.python"] = operators_python_mod
    sys.modules["airflow.operators.trigger_dagrun"] = operators_trigger_mod
    sys.modules["airflow.operators.bash"] = operators_bash_mod
    sys.modules["airflow.operators.empty"] = operators_empty_mod
    sys.modules["airflow.sensors"] = sensors_mod
    sys.modules["airflow.sensors.external_task"] = sensors_ext_mod
    sys.modules["airflow.utils"] = utils_mod
    sys.modules["airflow.utils.task_group"] = utils_tg_mod

    # Cross-link so `from airflow.decorators import dag` works through
    # both the ``airflow`` package and the explicit submodule path.
    airflow_mod.decorators = decorators_mod
    airflow_mod.models = models_mod
    airflow_mod.exceptions = exceptions_mod
    airflow_mod.operators = operators_mod
    operators_mod.python = operators_python_mod
    operators_mod.bash = operators_bash_mod
    operators_mod.empty = operators_empty_mod
    operators_mod.trigger_dagrun = operators_trigger_mod
    airflow_mod.sensors = sensors_mod
    airflow_mod.utils = utils_mod
    sensors_mod.external_task = sensors_ext_mod
    utils_mod.task_group = utils_tg_mod


_install_airflow_stubs()


# ---------------------------------------------------------------------------
# 3. Shared fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def real_medallion_config_dir(monkeypatch):
    """Point ``utils.medallion_config`` at the real shipped
    ``configs/medallion`` directory.

    Needed by any DAG module that calls a ``medallion_config`` function (e.g.
    ``is_single_year_competition``, #920 Phase 1) at import/parse time to
    build its task graph — on the host, ``CONFIG_DIR`` otherwise defaults to
    ``/opt/airflow/configs/medallion``, which doesn't exist outside the
    container. ``CONFIG_DIR`` is resolved once at module-import time, so we
    patch the module attribute directly (not the env var) — mirrors
    ``tests/unit/sql/test_dim_competition_render.py``.
    """
    from pathlib import Path

    from utils import medallion_config

    repo_root = Path(__file__).resolve().parents[3]
    monkeypatch.setattr(
        medallion_config, "CONFIG_DIR", repo_root / "configs" / "medallion"
    )
    medallion_config.reset_cache()
    yield
    medallion_config.reset_cache()


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
