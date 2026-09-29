"""Разрешённое отставание dags/utils/alerts.py в бою для автомата ClubElo (#1465).

Автомат `deploy/clubelo/auto_deliver.sh` пускает доставку, когда `dags/utils/alerts.py` в бою
равен версии до #1477 (ALLOWED_LAG). Это безопасно, пока ВСЁ, что ClubElo берёт из alerts.py,
одинаково в master и в разрешённой версии. ClubElo берёт: `send_telegram_message`
(scrapers/clubelo/history.py) и `telegram_on_failure` (через utils.default_args, его импортирует
DAG). Тест строит замыкание этих функций по модулю (вызываемые функции, константы, импорты
верхнего уровня) и сравнивает хеш его исходника с зафиксированным хешем разрешённой версии.
Падает — master разошёлся в том, чем пользуется ClubElo: убрать/пересмотреть ALLOWED_LAG.
"""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
ALERTS = ROOT / "dags" / "utils" / "alerts.py"
AUTO = ROOT / "deploy" / "clubelo" / "auto_deliver.sh"
ENTRIES = ("send_telegram_message", "telegram_on_failure")
# Хеш замыкания ENTRIES в alerts.py blob 30c4988a (0caf6bca^, до #1477).
PINNED = "0a536f7476a14236925e645a61d1c44c050f9991bf94cf3ef2cfde45a09ee958"


def closure(src: str, entries=ENTRIES):
    tree = ast.parse(src)
    defs = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for n in ast.walk(target):
                    if isinstance(n, ast.Name):
                        defs[n.id] = node
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                defs[(alias.asname or alias.name).split(".")[0]] = node
    seen, todo = set(), list(entries)
    while todo:
        name = todo.pop()
        node = defs.get(name)
        if node is None or name in seen:
            continue
        seen.add(name)
        todo.extend(n.id for n in ast.walk(node) if isinstance(n, ast.Name) and n.id in defs)
    nodes = sorted({id(defs[n]): defs[n] for n in seen}.values(), key=lambda n: n.lineno)
    text = "\n".join(ast.get_source_segment(src, n) for n in nodes)
    return seen, hashlib.sha256(text.encode()).hexdigest()


def test_master_alerts_matches_the_allowed_version_in_what_clubelo_uses():
    names, digest = closure(ALERTS.read_text(encoding="utf-8"))
    assert set(ENTRIES) <= names and "_send_telegram" in names
    assert digest == PINNED, (
        "alerts.py в master разошёлся с разрешённой версией в том, чем пользуется ClubElo — "
        "убрать или пересмотреть ALLOWED_LAG в deploy/clubelo/auto_deliver.sh")


def test_closure_sees_a_change_in_used_code_and_ignores_unused():
    src = ALERTS.read_text(encoding="utf-8")
    base = closure(src)[1]
    used = src.replace("def _send_telegram(", "def _send_telegram(_extra=None, *, ", 1)
    assert used != src and closure(used)[1] != base
    unused = src.replace("def telegram_dq_summary(", "def telegram_dq_summary_x(", 1)
    assert unused != src and closure(unused)[1] == base


def test_allowed_blob_of_the_script_has_the_pinned_closure():
    lag = re.findall(r'^ALLOWED_LAG="([^"]*)"$', AUTO.read_text(encoding="utf-8"), re.M)
    assert lag == ["dags/utils/alerts.py=30c4988a7cf742d67974a9be126e0bfc829b9008"]
    path, blob = lag[0].split("=")
    got = subprocess.run(["git", "-C", str(ROOT), "cat-file", "blob", blob], capture_output=True, text=True)
    if got.returncode != 0:
        pytest.skip(f"blob {blob[:8]} нет в клоне (мелкий клон) — сверка по PINNED выше")
    assert closure(got.stdout)[1] == PINNED


def test_clubelo_takes_from_alerts_only_the_checked_names():
    files = [ROOT / "dags/dag_ingest_clubelo.py", ROOT / "dags/utils/clubelo_tasks.py",
             ROOT / "dags/scripts/run_clubelo_scraper.py", *sorted((ROOT / "scrapers/clubelo").glob("*.py"))]
    shared = [ROOT / "dags/utils/default_args.py", ROOT / "dags/utils/config.py"]
    taken = set()
    for f in files + shared:
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("alerts"):
                taken |= {a.name for a in node.names}
            if isinstance(node, ast.Import):
                assert not any(a.name.endswith("alerts") for a in node.names), f
    assert taken == set(ENTRIES)
    # config.py alerts не импортирует; ClubElo берёт из default_args только LIGHT_ARGS
    assert "alerts" not in (ROOT / "dags/utils/config.py").read_text(encoding="utf-8")
