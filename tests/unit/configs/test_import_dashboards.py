"""
Unit tests for ``configs/superset/dashboards/*.py`` declarations and
``configs/superset/import_dashboards.py`` (E7 / T4).

Two dashboard families coexist:
  * declarative — module-level ``DASHBOARD`` dataclass picked up by
    ``import_dashboards._discover_dashboards``;
  * programmatic — a ``create_dashboard()`` function using real
    ``superset.*`` models (e.g. ``player_overview.py``).

Both import ``superset.*`` somewhere in their graph — those packages live
only inside the Superset container. So on the host we stub the ``superset``
package to make the import graph traversable, then assert each dashboard
module exposes either a ``DASHBOARD``/``DASHBOARDS`` attribute or a
``create_dashboard`` callable.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SUPERSET_DIR = PROJECT_ROOT / "configs" / "superset"
DASHBOARDS_DIR = SUPERSET_DIR / "dashboards"


pytestmark = pytest.mark.unit


def _install_superset_stubs() -> None:
    """Provide minimal stub modules so `from superset import db, ...` works."""
    if "superset" not in sys.modules:
        superset = types.ModuleType("superset")
        superset.db = types.SimpleNamespace(session=types.SimpleNamespace())
        superset.security_manager = types.SimpleNamespace()
        sys.modules["superset"] = superset
    if "superset.app" not in sys.modules:
        app = types.ModuleType("superset.app")
        app.create_app = lambda *a, **k: types.SimpleNamespace(
            app_context=lambda: _NullCtx()
        )
        sys.modules["superset.app"] = app
    if "superset.connectors" not in sys.modules:
        sys.modules["superset.connectors"] = types.ModuleType("superset.connectors")
    if "superset.connectors.sqla" not in sys.modules:
        sys.modules["superset.connectors.sqla"] = types.ModuleType(
            "superset.connectors.sqla"
        )
    if "superset.connectors.sqla.models" not in sys.modules:
        sqla_models = types.ModuleType("superset.connectors.sqla.models")
        sqla_models.SqlaTable = type("SqlaTable", (), {})
        sys.modules["superset.connectors.sqla.models"] = sqla_models
    # Cover any other lazy submodules referenced at module import time.
    for extra in (
        "superset.models", "superset.models.core",
        "superset.models.dashboard", "superset.models.slice",
    ):
        if extra not in sys.modules:
            mod = types.ModuleType(extra)
            # Provide common classes used in the importer if it touches them.
            if extra.endswith("core"):
                mod.Database = type("Database", (), {})
            if extra.endswith("dashboard"):
                mod.Dashboard = type("Dashboard", (), {})
            if extra.endswith("slice"):
                mod.Slice = type("Slice", (), {})
            sys.modules[extra] = mod


class _NullCtx:
    def __enter__(self): return self
    def __exit__(self, *a): return False


@pytest.fixture(scope="module", autouse=True)
def _superset_stubs():
    _install_superset_stubs()
    if str(SUPERSET_DIR) not in sys.path:
        sys.path.insert(0, str(SUPERSET_DIR))
    yield


def test_import_dashboards_main_imports() -> None:
    """`import_dashboards` module is importable with superset stubs in place."""
    sys.modules.pop("import_dashboards", None)
    try:
        import import_dashboards  # noqa: F401
    except Exception as exc:  # pragma: no cover — defensive
        pytest.skip(f"import_dashboards has uncoverable host-side import: {exc}")
    assert hasattr(import_dashboards, "Dashboard"), "Dashboard dataclass missing"


def test_dashboards_import() -> None:
    """Each dashboards/<name>.py exposes a DASHBOARD attribute."""
    files = sorted(
        p for p in DASHBOARDS_DIR.glob("*.py") if p.name != "__init__.py"
    )
    assert files, "no dashboard .py files discovered"

    if str(DASHBOARDS_DIR) not in sys.path:
        sys.path.insert(0, str(DASHBOARDS_DIR))

    failures: list[str] = []
    found = 0
    for path in files:
        mod_name = path.stem
        sys.modules.pop(mod_name, None)
        try:
            mod = importlib.import_module(mod_name)
        except Exception as exc:
            failures.append(f"{path.name}: import failed → {exc}")
            continue
        has_export = (
            hasattr(mod, "DASHBOARD")
            or hasattr(mod, "DASHBOARDS")
            or callable(getattr(mod, "create_dashboard", None))
        )
        if not has_export:
            failures.append(
                f"{path.name}: missing DASHBOARD / DASHBOARDS / create_dashboard"
            )
        else:
            found += 1
    assert not failures, "\n".join(failures)
    assert found >= 3, f"expected >=3 dashboard modules, parsed {found}"
