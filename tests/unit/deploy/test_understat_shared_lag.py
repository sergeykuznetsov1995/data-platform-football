"""Точные совместимые версии общих модулей для автомата Understat.

ALLOWED_LAG содержит конечные git-blob pins, произвольные версии запрещены.
Understat берёт telegram_on_failure из alerts и проверенные константы config.
Из medallion_config код Understat ничего не импортирует (транзитивная проверка).
ProxyManager импортируется базовым классом, но штатный native transport не
создаёт proxy pool; поведенческий tripwire ловит изменение этого условия.

Замена medallion pin и добавление proxy pin проверены на копиях боевых модулей.
Новый потребитель medallion или common proxy API требует пересмотра исключения.
"""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
UTILS = ROOT / "dags" / "utils"
AUTO = ROOT / "deploy" / "understat" / "auto_deliver.sh"
LAG = {
    "dags/utils/alerts.py": "30c4988a7cf742d67974a9be126e0bfc829b9008",
    "dags/utils/config.py": "221a12711ca7651274d22e3539a4b3b20b7284bf",
    "dags/utils/medallion_config.py": "8b52267bf748ffa3bb0156a89c97774e14c19c91",
    "scrapers/utils/proxy_manager.py": "01c904cbbef5f5d694648d97d13baa5431233c5a",
}
# Хеш замыкания telegram_on_failure в alerts.py blob 30c4988a (0caf6bca^, до #1477).
ALERTS_PINNED = "fe0c8ab93ff3897e5dd10a7e9860bff2cb9e93094eb3cd4759b7931cc6b1d7b1"
# То, что Understat берёт из config.py blob 221a1271 (до #1590).
CONFIG_PINNED = {
    "DAG_TAGS": {"understat": ["scraping", "understat", "bronze", "football", "xg"]},
    "SCHEDULES": {"dag_ingest_understat": "0 9 * * *"},
    "UNDERSTAT_LEAGUES": ["ENG-Premier League", "ESP-La Liga", "GER-Bundesliga", "ITA-Serie A",
                          "FRA-Ligue 1", "RUS-Premier League"],
}
# Прочее из config.py, что исполняется у Understat (dags/utils/__init__.py): хеш замыкания в blob 221a1271.
CONFIG_OTHER = ("LEAGUES", "CURRENT_SEASON")
CONFIG_OTHER_PINNED = "781325035019e4cc9c61a56cf3ba4ee187a46fffbf2d50f33a86151ce12dcce8"
UNDERSTAT_FILES = [ROOT / "dags/dag_ingest_understat.py", ROOT / "dags/dag_backfill_understat.py",
                   ROOT / "dags/utils/understat_tasks.py", ROOT / "dags/scripts/run_understat_scraper.py",
                   *sorted((ROOT / "scrapers/understat").rglob("*.py"))]


def closure(src: str, entries=("telegram_on_failure",)):
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
    return seen, hashlib.sha256(text.encode()).hexdigest(), text


def config_used(src: str) -> dict:
    values = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            values[node.target.id] = node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            values[node.targets[0].id] = node.value
    tags = ast.literal_eval(values["DAG_TAGS"])
    schedules = ast.literal_eval(values["SCHEDULES"])
    return {
        "DAG_TAGS": {"understat": tags["understat"]},
        "SCHEDULES": {k: v for k, v in schedules.items() if "understat" in k},
        "UNDERSTAT_LEAGUES": ast.literal_eval(values["UNDERSTAT_LEAGUES"]),
    }


def blob_text(blob: str) -> str:
    got = subprocess.run(["git", "-C", str(ROOT), "cat-file", "blob", blob], capture_output=True, text=True)
    if got.returncode != 0:
        pytest.skip(f"blob {blob[:8]} нет в клоне (мелкий клон) — сверка по зафиксированным значениям")
    return got.stdout


def module_file(dotted: str) -> Path | None:
    """Файл первой стороны для модуля: utils.* — это dags/utils/* (dags/ в sys.path Airflow)."""
    parts = dotted.split(".")
    base = ROOT / "dags" if parts[0] == "utils" else ROOT
    for cand in (base.joinpath(*parts).with_suffix(".py"), base.joinpath(*parts, "__init__.py")):
        if cand.is_file():
            return cand
    return None


def package_of(f: Path) -> list[str]:
    rel = f.relative_to(ROOT / "dags" if f.is_relative_to(ROOT / "dags") else ROOT).with_suffix("").parts
    return list(rel[:-1])


def taken_by(start: list[Path]) -> dict[str, set[str]]:
    """Имена, которые код, достижимый импортами из start, берёт из трёх dags-модулей.

    Обход транзитивный по файлам первой стороны, включая ленивые импорты и __init__.py пакетов.
    Импорт отстающего модуля объектом (`from utils import alerts`, `import utils.alerts`) — имя «*»:
    тогда из него можно взять что угодно, и тест обязан упасть.
    """
    lagged = {module_file(f"utils.{m}"): m for m in ("alerts", "config", "medallion_config")}
    taken: dict[str, set[str]] = {m: set() for m in lagged.values()}
    todo, seen = list(start), set()

    def visit(dotted: str, names: list[str] | None) -> None:
        parts = dotted.split(".")
        for k in range(1, len(parts)):            # __init__.py пакетов по пути
            pkg = module_file(".".join(parts[:k]))
            if pkg is not None and pkg not in lagged:
                todo.append(pkg)
        target = module_file(dotted)
        if target in lagged:
            taken[lagged[target]] |= set(names) if names is not None else {"*"}
            return
        if target is not None:
            todo.append(target)
        for n in names or []:                     # from pkg import module
            sub = module_file(f"{dotted}.{n}") if dotted else None
            if sub in lagged:
                taken[lagged[sub]].add("*")
            elif sub is not None:
                todo.append(sub)

    while todo:
        f = todo.pop()
        if f in seen:
            continue
        seen.add(f)
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                for a in node.names:
                    visit(a.name, None)
            elif isinstance(node, ast.ImportFrom):
                pkg = package_of(f)
                prefix = pkg[: len(pkg) - node.level + 1] if node.level else []
                dotted = ".".join([*prefix, *(node.module.split(".") if node.module else [])])
                visit(dotted, [a.name for a in node.names])
    return taken


def test_master_alerts_matches_the_allowed_version_in_what_understat_uses():
    names, digest, _ = closure((UTILS / "alerts.py").read_text(encoding="utf-8"))
    assert "telegram_on_failure" in names and "_send_telegram" in names
    assert digest == ALERTS_PINNED, (
        "alerts.py в master разошёлся с разрешённой версией в том, чем пользуется Understat — "
        "убрать или пересмотреть ALLOWED_LAG в deploy/understat/auto_deliver.sh")


def test_master_config_matches_the_allowed_version_in_what_understat_uses():
    assert config_used((UTILS / "config.py").read_text(encoding="utf-8")) == CONFIG_PINNED, (
        "config.py в master разошёлся с разрешённой версией в том, чем пользуется Understat — "
        "убрать или пересмотреть ALLOWED_LAG в deploy/understat/auto_deliver.sh")


def test_closure_sees_a_change_in_used_code_and_ignores_unused():
    src = (UTILS / "alerts.py").read_text(encoding="utf-8")
    base = closure(src)[1]
    used = src.replace("def _send_telegram(", "def _send_telegram(_extra=None, *, ", 1)
    assert used != src and closure(used)[1] != base
    unused = src.replace("def telegram_dq_summary(", "def telegram_dq_summary_x(", 1)
    assert unused != src and closure(unused)[1] == base


def test_config_used_sees_understat_entries_and_ignores_others():
    src = (UTILS / "config.py").read_text(encoding="utf-8")
    base = config_used(src)
    assert config_used(src.replace("'dag_ingest_understat': '", "'dag_ingest_understat': '1", 1)) != base
    assert config_used(src.replace("'dag_ingest_capology':", "'dag_ingest_capology_x':", 1)) == base


def test_script_allows_exactly_the_checked_blobs():
    lag = re.findall(r'^ALLOWED_LAG="([^"]*)"$', AUTO.read_text(encoding="utf-8"), re.M)
    assert len(lag) == 1 and dict(p.split("=") for p in lag[0].split()) == LAG


def test_allowed_blobs_have_the_pinned_contents():
    assert closure(blob_text(LAG["dags/utils/alerts.py"]))[1] == ALERTS_PINNED
    assert config_used(blob_text(LAG["dags/utils/config.py"])) == CONFIG_PINNED
    assert closure(blob_text(LAG["dags/utils/config.py"]), CONFIG_OTHER)[1] == CONFIG_OTHER_PINNED


def test_understat_takes_only_the_checked_names():
    taken = taken_by(UNDERSTAT_FILES)
    assert taken == {
        "alerts": {"telegram_on_failure"},
        # LEAGUES, CURRENT_SEASON — из dags/utils/__init__.py, он исполняется при любом import utils.*
        "config": {"DAG_TAGS", "SCHEDULES", "UNDERSTAT_LEAGUES", "LEAGUES", "CURRENT_SEASON"},
        "medallion_config": set(),
    }, "Understat берёт из отстающих модулей новое — сверить с разрешёнными версиями"


def test_master_config_other_names_match_the_allowed_version():
    names, digest, text = closure((UTILS / "config.py").read_text(encoding="utf-8"), CONFIG_OTHER)
    assert set(CONFIG_OTHER) <= names and "medallion_config" not in text
    assert digest == CONFIG_OTHER_PINNED


def test_taken_by_follows_transitive_lazy_and_module_object_imports(tmp_path, monkeypatch):
    # образец лежит вне репозитория, а импортирует реальные модули; пакет образца — scrapers.understat
    monkeypatch.setattr(sys.modules[__name__], "package_of", lambda _f: ["scrapers", "understat"])
    cases = {
        "from utils import alerts\n": {"alerts": {"*"}},
        "import utils.alerts as a\n": {"alerts": {"*"}},
        "def f():\n    from utils.alerts import telegram_dq_summary\n": {"alerts": {"telegram_dq_summary"}},
        "from utils.default_args import DEFAULT_ARGS\n": {"alerts": {"telegram_on_failure"}},
        "from scrapers.utils.competition_format import x\n": {"medallion_config": {"get_competition_format"}},
    }
    for src, want in cases.items():
        f = tmp_path / "sample.py"
        f.write_text(src, encoding="utf-8")
        got = taken_by([f])
        for mod, names in want.items():
            assert names <= got[mod], (src, got)

def test_native_default_transport_does_not_create_common_proxy_pool(monkeypatch, tmp_path):
    """The production facade uses its native session; legacy proxy APIs are unused."""
    import requests
    from scrapers.understat.scraper import UnderstatScraper
    from scrapers.utils.proxy_manager import ProxyManager

    class NoNetworkSession:
        def __init__(self):
            self.headers = {}

        def get(self, *_args, **_kwargs):
            raise AssertionError("compatibility check must not request the source")

    def forbidden_pool(*_args, **_kwargs):
        raise AssertionError("Understat default transport started using common proxy APIs")

    monkeypatch.setattr(ProxyManager, "__init__", forbidden_pool)
    monkeypatch.setattr(requests, "Session", NoNetworkSession)
    monkeypatch.setattr("scrapers.base.base_scraper.IcebergWriter", lambda: object())
    scraper = UnderstatScraper(
        leagues=["ENG-Premier League"], seasons=["2627"], cache_dir=tmp_path
    )
    assert scraper._proxy_manager is None
    assert isinstance(scraper.client.session, NoNetworkSession)
