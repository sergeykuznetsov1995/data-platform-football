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
from importlib.util import resolve_name
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
ALLOWED_BLOBS = {
    "dags/utils/alerts.py": "30c4988a7cf742d67974a9be126e0bfc829b9008",
    "dags/utils/config.py": "221a12711ca7651274d22e3539a4b3b20b7284bf",
    "dags/utils/medallion_config.py": "697fe43d6eb46e49c4246780f2cba59fabf87e4d",
}
# Reviewed pre/post #1590 source, pinned in full on purpose. A future change
# requires a fresh compatibility review even in a shallow clone without old blobs.
# #1363: only get_season_team_count changed; it is outside the ClubElo closure.
# Pin that reachable closure below before retaining the existing ALLOWED_LAG.
CONFIG_SHA256 = {
    "dags/utils/config.py": (
        "df91f3f6dcd1d9c0f849721f4e33e6df3d1240c7176d55e4361a8fc340d2bb78",
        "23853181485b752be1cf120cf00e5284c319320168dd4b8dcd103ed4c0cbeb8d",
    ),
    "dags/utils/medallion_config.py": (
        "94384b372e4776bd1178c25397ce82b6ddb5ac5d4d7b4836dc287c886fca5e15",
        "b3cde46db5fa568152b1a8e3c8a7775dc8b327b51ae7a323efeb21878692eb1e",
    ),
}


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
    assert len(lag) == 1
    entries = lag[0].split()
    assert len(entries) == len(ALLOWED_BLOBS)
    assert dict(entry.split("=") for entry in entries) == ALLOWED_BLOBS
    blob = ALLOWED_BLOBS["dags/utils/alerts.py"]
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


@pytest.mark.parametrize("path", CONFIG_SHA256)
def test_reviewed_master_config_is_pinned_even_without_git_history(path):
    assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == CONFIG_SHA256[path][1], (
        f"{path} changed since the #1590 compatibility review; reassess ALLOWED_LAG")


@pytest.mark.parametrize("path", CONFIG_SHA256)
def test_allowed_config_blob_is_the_reviewed_production_version(path):
    got = subprocess.run(["git", "-C", str(ROOT), "cat-file", "blob", ALLOWED_BLOBS[path]],
                         capture_output=True)
    if got.returncode:
        pytest.skip("old blob absent in shallow clone; current source is separately pinned")
    assert hashlib.sha256(got.stdout).hexdigest() == CONFIG_SHA256[path][0]


def test_clubelo_medallion_helpers_match_the_allowed_version():
    entries = ("get_competition_floor_basis", "get_competition_season_format",
               "is_single_year_competition", "get_competition_format")
    pinned = "05e9006388c878985863182970890e8410c35ad0e220f29a776f60ba6b360bf9"
    path = "dags/utils/medallion_config.py"
    names, digest = closure((ROOT / path).read_text(), entries)
    assert "get_season_team_count" not in names
    assert digest == pinned
    old = subprocess.run(["git", "-C", str(ROOT), "cat-file", "blob", ALLOWED_BLOBS[path]],
                         capture_output=True, text=True)
    if old.returncode == 0:
        assert closure(old.stdout, entries)[1] == pinned


def imported_module(node: ast.ImportFrom, path: Path) -> str:
    if not node.level:
        return node.module or ""
    package = ".".join(path.relative_to(ROOT).parent.parts)
    return resolve_name("." * node.level + (node.module or ""), package)


@pytest.mark.parametrize("statement, expected", [
    ("from .medallion_config import get_source_priority_exprs", "dags.utils.medallion_config"),
    ("from . import config", "dags.utils"),
    ("from utils.config import DAG_TAGS", "utils.config"),
])
def test_relative_config_imports_are_resolved(statement, expected):
    node = ast.parse(statement).body[0]
    assert imported_module(node, ROOT / "dags/utils/clubelo_tasks.py") == expected


def test_clubelo_config_imports_stay_within_reviewed_surface():
    # Package initialization re-exports SCHEDULES but does not read the removed
    # FotMob key. Pin that initializer so new import-time use requires review.
    initializer = ROOT / "dags/utils/__init__.py"
    assert hashlib.sha256(initializer.read_bytes()).hexdigest() == (
        "58ffcd31e139fd39bed5ac90339ac4e3080afa404575a2126fe92010c219eb00")
    owned = [ROOT / "dags/dag_ingest_clubelo.py", ROOT / "dags/utils/clubelo_tasks.py",
             ROOT / "dags/scripts/run_clubelo_scraper.py",
             *sorted((ROOT / "scrapers/clubelo").glob("*.py"))]
    shared = [ROOT / "dags/utils/default_args.py", ROOT / "dags/utils/alerts.py",
              ROOT / "scrapers/__init__.py",
              *sorted((ROOT / "scrapers/base").rglob("*.py")),
              *sorted((ROOT / "scrapers/utils").rglob("*.py"))]
    config_modules = {"utils.config", "dags.utils.config"}
    medallion_modules = {"utils.medallion_config", "dags.utils.medallion_config"}
    allowed_medallion = {"get_competition_floor_basis", "get_competition_season_format",
                         "is_single_year_competition", "get_competition_format"}
    taken = set()
    for path in owned + shared + [ROOT / "dags/utils/config.py"]:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                names = {a.name for a in node.names}
                module = imported_module(node, path)
                if module in config_modules:
                    assert names <= {"DAG_TAGS"}, path
                    taken |= names
                if module in medallion_modules:
                    assert path not in owned and names <= allowed_medallion, path
                # Reject module aliases (`from utils import config as c`) as
                # well as star imports: they evade a named-symbol guard.
                if module in {"utils", "dags.utils"}:
                    assert not names & {"config", "medallion_config", "SCHEDULES", "*"}, path
            if isinstance(node, ast.Import):
                assert not {a.name for a in node.names} & (
                    config_modules | medallion_modules | {"utils", "dags.utils"}), path
    assert taken == {"DAG_TAGS"}
