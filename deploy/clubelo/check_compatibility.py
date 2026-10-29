"""Offline ClubElo gate against a complete snapshot of the live shared modules."""
from __future__ import annotations

import ast
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def shared_files(tree, paths):
    result = {}
    for name in paths:
        path = tree / name
        if path.is_symlink():
            raise RuntimeError(f"shared symlink: {name}")
        if not path.exists():
            continue
        def walk_error(error):
            raise error
        items = [path]
        if path.is_dir():
            items = []
            for parent, dirs, files in os.walk(path, onerror=walk_error):
                dirs[:] = [name for name in dirs if name != "__pycache__"]
                items.extend(Path(parent) / name for name in dirs + files)
        for item in sorted(items):
            if "__pycache__" in item.parts or item.suffix == ".pyc":
                continue
            if item.is_symlink():
                raise RuntimeError(f"shared symlink: {item}")
            if item.is_file():
                result[str(item.relative_to(tree))] = hashlib.sha256(item.read_bytes()).hexdigest()
    return result


def no_network(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "socket.bind", "socket.sendto"}:
        raise RuntimeError("network disabled in ClubElo compatibility check")


def check_calls(root, writer, send, failure, solver, trino):
    # Bind the candidate's actual call shapes to real shared signatures. Unit
    # tests may mock IO methods without autospec, hiding incompatible APIs.
    for path in (root / "scrapers/clubelo").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            function, prefix = None, []
            if isinstance(node.func, ast.Name):
                function = {"IcebergWriter": writer, "send_telegram_message": send,
                            "telegram_on_failure": failure, "FlareSolverrClient": solver}.get(node.func.id)
            elif (isinstance(node.func, ast.Attribute)
                  and isinstance(node.func.value, ast.Attribute)
                  and isinstance(node.func.value.value, ast.Name)
                  and node.func.value.value.id == "self" and node.func.value.attr in {"writer", "_solver"}):
                owner = writer if node.func.value.attr == "writer" else solver
                function = getattr(owner, node.func.attr)
                prefix = [None]  # unbound instance method
            elif (isinstance(node.func, ast.Attribute)
                  and isinstance(node.func.value, ast.Call)
                  and isinstance(node.func.value.func, ast.Attribute)
                  and node.func.value.func.attr == "_get_trino_manager"
                  and isinstance(node.func.value.func.value, ast.Attribute)
                  and node.func.value.func.value.attr == "writer"
                  and isinstance(node.func.value.func.value.value, ast.Name)
                  and node.func.value.func.value.value.id == "self"):
                function = getattr(trino, node.func.attr)
                prefix = [None]  # unbound instance method
            if function is not None:
                if any(isinstance(arg, ast.Starred) for arg in node.args) or any(k.arg is None for k in node.keywords):
                    raise RuntimeError(f"unverified dynamic shared call: {path}:{node.lineno}")
                inspect.signature(function).bind(*prefix, *([None] * len(node.args)),
                                                 **{k.arg: None for k in node.keywords})
    # Transport registers this bound method as a zero-argument cleanup callback.
    inspect.signature(solver.close).bind(None)


def run_tests(root):
    os.chdir(root)
    sys.path[:0] = [str(root), str(root / "dags")]
    sys.addaudithook(no_network)
    # Check real lazy dependencies before unit tests can install mocks.
    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.base.flaresolverr_client import FlareSolverrClient
    from utils.alerts import send_telegram_message, telegram_on_failure
    from scrapers.clubelo import daily, history
    assert callable(IcebergWriter) and callable(send_telegram_message) and callable(telegram_on_failure)
    check_calls(root, IcebergWriter, send_telegram_message, telegram_on_failure, FlareSolverrClient,
                TrinoTableManager)
    import pytest
    tests = sorted(str(p.relative_to(root)) for p in (root / "tests/unit").rglob("test_*clubelo*.py")
                   if "deploy" not in p.relative_to(root / "tests/unit").parts)
    if not tests:
        raise RuntimeError("no ClubElo tests")
    return pytest.main(["-q", "-p", "no:cacheprovider", *tests])


def main():
    mode, work_arg, tree_arg, *paths = sys.argv[1:]
    work, tree = Path(work_arg), Path(tree_arg)
    manifest = work / "shared.json"
    if mode == "test":
        return run_tests(work)
    if mode == "verify":
        if shared_files(tree, paths) != json.loads(manifest.read_text()):
            raise RuntimeError("shared snapshot changed")
        return 0
    before = shared_files(tree, paths)
    for name in paths:
        target = work / name
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists() or target.is_symlink():
            target.unlink()
    for name in before:
        target = work / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tree / name, target)
    if shared_files(work, paths) != before or shared_files(tree, paths) != before:
        raise RuntimeError("shared snapshot changed while copying")
    manifest.write_text(json.dumps(before))
    env = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", PYTHONDONTWRITEBYTECODE="1")
    for name in ("PYTHONPATH", "PYTHONHOME", "PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        env.pop(name, None)
    return subprocess.run([sys.executable, "-I", __file__, "test", str(work), str(tree)],
                          cwd=work, env=env, timeout=240).returncode


if __name__ == "__main__":
    raise SystemExit(main())
