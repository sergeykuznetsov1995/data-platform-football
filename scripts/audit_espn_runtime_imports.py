#!/usr/bin/env python3
"""Fail closed when ESPN Native reaches soccerdata or its legacy facade."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib
from importlib.abc import MetaPathFinder
import json
from pathlib import Path
import sys
import types
from typing import Iterable


SCHEMA_VERSION = "espn-runtime-import-audit-v1"
FORBIDDEN_MODULE = "soccerdata"
FORBIDDEN_MODULES = ("soccerdata", "scrapers.espn.scraper")
FORBIDDEN_FACADE = "scrapers.espn.ESPNScraper"
RUNTIME_MODULES = (
    "scrapers.espn.daily_owner",
    "scrapers.espn.registry",
    "scrapers.espn.discovery",
    "scrapers.espn.raw_store",
    "scrapers.espn.transport",
    "scrapers.espn.parsers",
    "scrapers.espn.repository",
    "scrapers.espn.runner",
    "scrapers.espn.operations",
    "scrapers.espn.migration",
    "scrapers.espn.repair",
    "utils.espn_native_tasks",
    "utils.espn_dag_factory",
    "dag_ingest_espn",
    "dag_repair_espn",
    "dag_backfill_espn",
    "dag_replay_espn",
    "dag_monitor_espn",
    "dag_discover_espn_registry",
    "dag_trigger_espn_daily",
    "dags.scripts.run_espn_scraper",
    "migrate_espn_native_v2",
    "audit_espn_repair",
    "extract_espn_repair_audit",
    "espn_v2_object_contract",
)


class ForbiddenRuntimeImport(ImportError):
    """The native runtime attempted to resolve the retired dependency."""


def _is_forbidden(module: str | None) -> bool:
    return isinstance(module, str) and (
        module == FORBIDDEN_FACADE
        or any(
            module == forbidden or module.startswith(forbidden + ".")
            for forbidden in FORBIDDEN_MODULES
        )
    )


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_string(node.left)
        right = _literal_string(node.right)
        return left + right if left is not None and right is not None else None
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                return None
            parts.append(value.value)
        return "".join(parts)
    return None


def _attribute_path(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _attribute_path(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None


def scan_file(path: Path) -> list[dict[str, object]]:
    """Return direct and common dynamic forbidden-import findings."""

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        return [
            {
                "path": str(path),
                "line": getattr(exc, "lineno", None),
                "kind": "scan_error",
                "module": None,
                "error": str(exc),
            }
        ]

    importlib_aliases = {"importlib"}
    import_module_aliases: set[str] = set()
    espn_facade_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "importlib":
                    importlib_aliases.add(alias.asname or alias.name)
                if alias.name == "scrapers.espn":
                    espn_facade_aliases.add(alias.asname or "scrapers.espn")
        elif isinstance(node, ast.ImportFrom):
            if node.module == "importlib":
                for alias in node.names:
                    if alias.name == "import_module":
                        import_module_aliases.add(alias.asname or alias.name)
            if node.module == "scrapers":
                for alias in node.names:
                    if alias.name == "espn":
                        espn_facade_aliases.add(alias.asname or alias.name)

    findings: list[dict[str, object]] = []
    in_espn_package = (
        path.parent.name == "espn" and path.parent.parent.name == "scrapers"
    )
    for node in ast.walk(tree):
        modules: Iterable[str] = ()
        if isinstance(node, ast.Import):
            modules = (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            resolved_module = node.module
            if node.level and in_espn_package:
                resolved_module = "scrapers.espn" + (
                    f".{node.module}" if node.module else ""
                )
            modules = (
                (
                    resolved_module,
                    *(f"{resolved_module}.{alias.name}" for alias in node.names),
                )
                if resolved_module
                else ()
            )
        for module in modules:
            if _is_forbidden(module):
                findings.append(
                    {
                        "path": str(path),
                        "line": node.lineno,
                        "kind": "import",
                        "module": module,
                    }
                )

        if (
            isinstance(node, ast.Attribute)
            and node.attr == "ESPNScraper"
            and _attribute_path(node.value) in espn_facade_aliases
        ):
            findings.append(
                {
                    "path": str(path),
                    "line": node.lineno,
                    "kind": "legacy_facade_access",
                    "module": FORBIDDEN_FACADE,
                }
            )

        if not isinstance(node, ast.Call) or not node.args:
            continue
        dynamic_loader = False
        if isinstance(node.func, ast.Name):
            dynamic_loader = (
                node.func.id == "__import__" or node.func.id in import_module_aliases
            )
        elif isinstance(node.func, ast.Attribute):
            dynamic_loader = (
                node.func.attr == "import_module"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in importlib_aliases
            )
        module = _literal_string(node.args[0]) if dynamic_loader else None
        if dynamic_loader and module and module.startswith("."):
            package = None
            if len(node.args) >= 2:
                package = _literal_string(node.args[1])
            for keyword in node.keywords:
                if keyword.arg == "package":
                    package = _literal_string(keyword.value)
            if package:
                module = package + module
        if _is_forbidden(module):
            findings.append(
                {
                    "path": str(path),
                    "line": node.lineno,
                    "kind": "dynamic_import",
                    "module": module,
                }
            )
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _attribute_path(node.args[0]) in espn_facade_aliases
            and _literal_string(node.args[1]) == "ESPNScraper"
        ):
            findings.append(
                {
                    "path": str(path),
                    "line": node.lineno,
                    "kind": "legacy_facade_access",
                    "module": FORBIDDEN_FACADE,
                }
            )
    return sorted(
        findings,
        key=lambda item: (
            str(item["path"]),
            int(item.get("line") or 0),
            str(item["kind"]),
        ),
    )


def production_files(root: Path) -> tuple[Path, ...]:
    """Return the explicit native runtime surface; retained legacy is excluded."""

    root = root.resolve()
    native = tuple(
        path
        for path in (root / "scrapers" / "espn").glob("*.py")
        if path.name not in {"scraper.py", "__init__.py"}
    )
    dags = tuple((root / "dags").glob("dag_*espn*.py"))
    exact = (
        root / "dags" / "utils" / "espn_native_tasks.py",
        root / "dags" / "utils" / "espn_dag_factory.py",
        root / "dags" / "scripts" / "run_espn_scraper.py",
        root / "scripts" / "migrate_espn_native_v2.py",
        root / "scripts" / "audit_espn_repair.py",
        root / "scripts" / "extract_espn_repair_audit.py",
    )
    paths = tuple(sorted({*native, *dags, *exact}))
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"production runtime file missing: {missing[0]}")
    return paths


class _Blocker(MetaPathFinder):
    def __init__(self) -> None:
        self.attempts: list[str] = []

    def find_spec(self, fullname, path=None, target=None):
        if _is_forbidden(fullname):
            self.attempts.append(fullname)
            raise ForbiddenRuntimeImport(
                f"ESPN Native runtime attempted forbidden import {fullname!r}"
            )
        return None


def _install_airflow_probe_stubs() -> dict[str, object] | None:
    """Install the minimum DAG-parse API only when real Airflow is absent."""

    try:
        from airflow import DAG as _dag  # noqa: F401
        from airflow.models.param import Param as _param  # noqa: F401
        from airflow.operators.python import PythonOperator as _operator  # noqa: F401

        return None
    except Exception:
        pass
    saved = {
        name: module
        for name, module in tuple(sys.modules.items())
        if name == "airflow" or name.startswith("airflow.")
    }
    for name in saved:
        sys.modules.pop(name, None)

    airflow = types.ModuleType("airflow")
    models = types.ModuleType("airflow.models")
    params = types.ModuleType("airflow.models.param")
    operators = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    trigger_dagrun = types.ModuleType("airflow.operators.trigger_dagrun")

    class _XCom:
        def __init__(self, operator, key=None):
            self.operator = operator
            self.key = key

        def __getitem__(self, key):
            return _XCom(self.operator, key)

    class _Partial:
        def __init__(self, values):
            self.values = values

        def expand(self, **_mapped):
            return _Operator(**self.values)

    class _Operator:
        @classmethod
        def partial(cls, **values):
            return _Partial(values)

        def __init__(self, **values):
            self.task_id = values.get("task_id", "probe")
            self.output = _XCom(self)

        def __rshift__(self, other):
            return other

        def __rrshift__(self, _other):
            return self

        def __lshift__(self, other):
            return other

    class _DAG:
        def __init__(self, *args, **values):
            self.dag_id = values.get("dag_id", args[0] if args else None)
            self.schedule = values.get("schedule")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Param:
        def __init__(self, default=None, **_values):
            self.default = default

    airflow.DAG = _DAG
    models.DAG = _DAG
    params.Param = _Param
    python.PythonOperator = _Operator
    trigger_dagrun.TriggerDagRunOperator = _Operator
    airflow.models = models
    airflow.operators = operators
    models.param = params
    operators.python = python
    operators.trigger_dagrun = trigger_dagrun
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.models": models,
            "airflow.models.param": params,
            "airflow.operators": operators,
            "airflow.operators.python": python,
            "airflow.operators.trigger_dagrun": trigger_dagrun,
        }
    )
    return saved


def runtime_probe(root: Path) -> list[dict[str, str]]:
    """Import the runtime with a meta-path tripwire in a fresh CLI process."""

    root = root.resolve()
    search_paths = (str(root / "scripts"), str(root / "dags"), str(root))
    previous_path = list(sys.path)
    retained_forbidden = {
        name: module
        for name, module in tuple(sys.modules.items())
        if _is_forbidden(name)
    }
    for name in retained_forbidden:
        sys.modules.pop(name, None)
    blocker = _Blocker()
    sys.meta_path.insert(0, blocker)
    results: list[dict[str, str]] = []
    saved_airflow = None
    try:
        for value in reversed(search_paths):
            if value not in sys.path:
                sys.path.insert(0, value)
        saved_airflow = _install_airflow_probe_stubs()
        for module_name in RUNTIME_MODULES:
            try:
                importlib.import_module(module_name)
            except Exception as exc:
                results.append(
                    {
                        "module": module_name,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
            else:
                results.append({"module": module_name, "status": "imported"})
    finally:
        if blocker in sys.meta_path:
            sys.meta_path.remove(blocker)
        sys.path[:] = previous_path
        sys.modules.update(retained_forbidden)
        if saved_airflow is not None:
            for name in tuple(sys.modules):
                if name == "airflow" or name.startswith("airflow."):
                    sys.modules.pop(name, None)
            sys.modules.update(saved_airflow)
    if blocker.attempts:
        results.append(
            {
                "module": ",".join(sorted(set(blocker.attempts))),
                "status": "failed",
                "error_type": "ForbiddenRuntimeImport",
                "error": "forbidden dependency resolution attempted",
            }
        )
    return results


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def build_report(root: Path) -> dict[str, object]:
    root = root.resolve()
    files = production_files(root)
    static_findings = []
    for path in files:
        for finding in scan_file(path):
            static_findings.append(
                {
                    **finding,
                    "path": path.relative_to(root).as_posix(),
                }
            )
    probe = runtime_probe(root)
    passed = not static_findings and all(item["status"] == "imported" for item in probe)
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass" if passed else "fail",
        "forbidden_module": FORBIDDEN_MODULE,
        "forbidden_modules": list(FORBIDDEN_MODULES) + [FORBIDDEN_FACADE],
        "scanned_files": [path.relative_to(root).as_posix() for path in files],
        "static_findings": static_findings,
        "runtime_probe": probe,
    }
    report["result_sha256"] = hashlib.sha256(_canonical_bytes(report)).hexdigest()
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args(argv)
    report = build_report(args.root)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
