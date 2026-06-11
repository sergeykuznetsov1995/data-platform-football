"""
Smoke tests for BI / Catalog Python scripts.

These scripts run *inside* the Superset / OpenMetadata containers and depend
on SDKs (``superset``, ``metadata`` from openmetadata-ingestion) that are
NOT installed on the host or in the test environment. Yet we still want
guard-rails on the host CI:

  1. ``py_compile`` — syntax errors caught fast (no runtime imports).
  2. AST-driven structural checks — every dashboard module declares
     ``create_dashboard()``; the orchestrator module references it; the
     description applier / datasource importer have a ``main()``.
  3. Runtime import probe — with the missing SDKs stubbed via ``sys.modules``
     the modules must import cleanly. This catches typos in module-level
     code (e.g. wrong env var names, top-level statements that crash).

Marker: ``unit`` — no docker, no network, no real dependencies.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import py_compile
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUPERSET_DIR = PROJECT_ROOT / "configs" / "superset"
DASHBOARDS_DIR = SUPERSET_DIR / "dashboards"
OPENMETADATA_DIR = PROJECT_ROOT / "configs" / "openmetadata"


# ---------------------------------------------------------------------------
# Discover scripts dynamically. Skipping if a directory is missing keeps
# the suite resilient against future restructures.
# ---------------------------------------------------------------------------
def _scripts() -> list[Path]:
    found: list[Path] = []
    for root in (SUPERSET_DIR, DASHBOARDS_DIR, OPENMETADATA_DIR):
        if not root.is_dir():
            continue
        for path in root.glob("*.py"):
            if path.name == "__init__.py":
                continue
            # superset_config.py is a Superset config (loaded by their CLI),
            # not a script we need to ast-check for main()/create_dashboard()
            found.append(path)
    return sorted(found)


SCRIPT_PATHS = _scripts()


# ---------------------------------------------------------------------------
# 1. py_compile — every script must be syntactically valid Python 3.
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("script", SCRIPT_PATHS, ids=lambda p: p.name)
def test_script_compiles(script: Path) -> None:
    try:
        py_compile.compile(str(script), doraise=True)
    except py_compile.PyCompileError as exc:
        pytest.fail(f"{script.relative_to(PROJECT_ROOT)} failed to compile: {exc}")


# ---------------------------------------------------------------------------
# 2. AST checks — module-level contract per file role.
# ---------------------------------------------------------------------------


def _module_top_level_names(path: Path) -> set[str]:
    """Return the set of top-level def/class names defined in a module."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
    return names


@pytest.mark.unit
class TestDashboardModulesContract:
    """Each dashboard module MUST define ``create_dashboard``."""

    @pytest.mark.parametrize(
        "module_name", ["player_overview"]
    )
    def test_dashboard_defines_create_dashboard(self, module_name):
        path = DASHBOARDS_DIR / f"{module_name}.py"
        assert path.exists(), f"dashboard module missing: {path}"
        names = _module_top_level_names(path)
        assert "create_dashboard" in names, (
            f"{module_name}.py must define create_dashboard()"
        )

    def test_orchestrator_references_all_dashboards(self):
        """``import_dashboards.DASHBOARDS`` lists each module by name."""
        path = DASHBOARDS_DIR / "import_dashboards.py"
        assert path.exists()
        source = path.read_text(encoding="utf-8")
        for required in ("player_overview",):
            assert required in source, (
                f"import_dashboards.py must reference '{required}'"
            )


@pytest.mark.unit
class TestImporterModulesContract:
    @pytest.mark.parametrize(
        "rel_path",
        [
            "configs/superset/import_datasources.py",
            "configs/openmetadata/apply_descriptions.py",
            "configs/superset/dashboards/import_dashboards.py",
        ],
    )
    def test_module_defines_main(self, rel_path):
        path = PROJECT_ROOT / rel_path
        assert path.exists()
        names = _module_top_level_names(path)
        assert "main" in names, f"{rel_path} must define a main() entrypoint"


# ---------------------------------------------------------------------------
# 3. Runtime import probe — install minimal stubs for SDKs that are only
# present inside the container, then import each module.
# ---------------------------------------------------------------------------


@pytest.fixture
def superset_sdk_stubs(monkeypatch):
    """Stub ``superset`` SDK so dashboard modules can import on the host."""
    superset = types.ModuleType("superset")
    superset.db = MagicMock(name="superset.db")
    superset.security_manager = MagicMock(name="superset.security_manager")

    superset_app = types.ModuleType("superset.app")
    superset_app.create_app = MagicMock(name="superset.app.create_app")

    sqla_models = types.ModuleType("superset.connectors.sqla.models")
    sqla_models.SqlaTable = MagicMock(name="SqlaTable")

    core_models = types.ModuleType("superset.models.core")
    core_models.Database = MagicMock(name="Database")

    dashboard_models = types.ModuleType("superset.models.dashboard")
    dashboard_models.Dashboard = MagicMock(name="Dashboard")

    slice_models = types.ModuleType("superset.models.slice")
    slice_models.Slice = MagicMock(name="Slice")

    monkeypatch.setitem(sys.modules, "superset", superset)
    monkeypatch.setitem(sys.modules, "superset.app", superset_app)
    monkeypatch.setitem(sys.modules, "superset.connectors", types.ModuleType("superset.connectors"))
    monkeypatch.setitem(sys.modules, "superset.connectors.sqla", types.ModuleType("superset.connectors.sqla"))
    monkeypatch.setitem(sys.modules, "superset.connectors.sqla.models", sqla_models)
    monkeypatch.setitem(sys.modules, "superset.models", types.ModuleType("superset.models"))
    monkeypatch.setitem(sys.modules, "superset.models.core", core_models)
    monkeypatch.setitem(sys.modules, "superset.models.dashboard", dashboard_models)
    monkeypatch.setitem(sys.modules, "superset.models.slice", slice_models)
    yield


@pytest.fixture
def openmetadata_sdk_stubs(monkeypatch):
    """Stub the slice of ``metadata`` SDK that apply_descriptions touches."""
    # Each stubbed module gets a sentinel object for every imported name.
    def _ensure_module(name: str) -> types.ModuleType:
        if name in sys.modules:
            return sys.modules[name]
        mod = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, mod)
        return mod

    _ensure_module("metadata")
    _ensure_module("metadata.generated")
    _ensure_module("metadata.generated.schema")
    _ensure_module("metadata.generated.schema.entity")
    _ensure_module("metadata.generated.schema.entity.data")
    table_mod = _ensure_module("metadata.generated.schema.entity.data.table")
    table_mod.Column = MagicMock(name="Column")
    table_mod.Table = MagicMock(name="Table")

    _ensure_module("metadata.generated.schema.entity.services")
    _ensure_module("metadata.generated.schema.entity.services.connections")
    _ensure_module("metadata.generated.schema.entity.services.connections.metadata")
    om_conn_mod = _ensure_module(
        "metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection"
    )
    om_conn_mod.AuthProvider = MagicMock(name="AuthProvider")
    om_conn_mod.OpenMetadataConnection = MagicMock(name="OpenMetadataConnection")

    _ensure_module("metadata.generated.schema.security")
    _ensure_module("metadata.generated.schema.security.client")
    om_jwt_mod = _ensure_module(
        "metadata.generated.schema.security.client.openMetadataJWTClientConfig"
    )
    om_jwt_mod.OpenMetadataJWTClientConfig = MagicMock(
        name="OpenMetadataJWTClientConfig"
    )

    _ensure_module("metadata.generated.schema.type")
    tag_mod = _ensure_module("metadata.generated.schema.type.tagLabel")
    tag_mod.LabelType = MagicMock(name="LabelType")
    tag_mod.State = MagicMock(name="State")
    tag_mod.TagLabel = MagicMock(name="TagLabel")
    tag_mod.TagSource = MagicMock(name="TagSource")

    _ensure_module("metadata.ingestion")
    _ensure_module("metadata.ingestion.ometa")
    om_api_mod = _ensure_module("metadata.ingestion.ometa.ometa_api")
    om_api_mod.OpenMetadata = MagicMock(name="OpenMetadata")
    yield


def _import_by_path(path: Path, mod_name: str):
    """Importlib helper that loads a module from an arbitrary file path."""
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
class TestImportProbe:
    def test_import_datasources_loads(self, superset_sdk_stubs):
        path = SUPERSET_DIR / "import_datasources.py"
        mod = _import_by_path(path, "_smoke_import_datasources")
        # Public surface check
        assert callable(getattr(mod, "main", None))

    def test_player_overview_loads(self, superset_sdk_stubs):
        path = DASHBOARDS_DIR / "player_overview.py"
        mod = _import_by_path(path, "_smoke_player_overview")
        assert callable(getattr(mod, "create_dashboard", None))

    def test_import_dashboards_loads(self, superset_sdk_stubs):
        path = DASHBOARDS_DIR / "import_dashboards.py"
        mod = _import_by_path(path, "_smoke_import_dashboards")
        assert isinstance(getattr(mod, "DASHBOARDS", None), list)
        # Sanity: registry references match the per-file modules above
        assert "player_overview" in mod.DASHBOARDS

    def test_apply_descriptions_loads(self, openmetadata_sdk_stubs):
        path = OPENMETADATA_DIR / "apply_descriptions.py"
        mod = _import_by_path(path, "_smoke_apply_descriptions")
        assert callable(getattr(mod, "main", None))


# ---------------------------------------------------------------------------
# 4. ALL .py files are listed by the discovery helper — guards against
# silently dropping a script.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_at_least_five_scripts_discovered():
    """Sanity bound (#478 — остался один дашборд): import_datasources,
    superset_config, apply_descriptions, player_overview, import_dashboards —
    уже 5. Anything fewer means a directory is missing or restructured."""
    assert len(SCRIPT_PATHS) >= 5, (
        f"Only discovered {len(SCRIPT_PATHS)} scripts — restructure?"
    )
