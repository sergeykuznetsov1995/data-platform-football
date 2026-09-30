"""Разрешённое отставание общих модулей в бою для автомата Understat (#1594).

Автомат `deploy/understat/auto_deliver.sh` пускает доставку, когда `dags/utils/alerts.py`,
`dags/utils/config.py`, `dags/utils/medallion_config.py` в бою равны разрешённым версиям
(ALLOWED_LAG; все три — из принятой базы 8b61969). Это безопасно, пока ВСЁ, что Understat берёт
из этих модулей, одинаково в master и в разрешённой версии. Understat берёт:
`telegram_on_failure` из alerts.py (через utils.default_args); `DAG_TAGS["understat"]`,
`SCHEDULES[*understat*]`, `UNDERSTAT_LEAGUES` из config.py; из medallion_config.py — ничего.
Падает — master разошёлся в том, чем пользуется Understat: убрать/пересмотреть ALLOWED_LAG.
"""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
UTILS = ROOT / "dags" / "utils"
AUTO = ROOT / "deploy" / "understat" / "auto_deliver.sh"
LAG = {
    "dags/utils/alerts.py": "30c4988a7cf742d67974a9be126e0bfc829b9008",
    "dags/utils/config.py": "221a12711ca7651274d22e3539a4b3b20b7284bf",
    "dags/utils/medallion_config.py": "697fe43d6eb46e49c4246780f2cba59fabf87e4d",
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
UNDERSTAT_FILES = [ROOT / "dags/dag_ingest_understat.py", ROOT / "dags/dag_backfill_understat.py",
                   ROOT / "dags/utils/understat_tasks.py", ROOT / "dags/scripts/run_understat_scraper.py",
                   *sorted((ROOT / "scrapers/understat").glob("*.py"))]


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
    return seen, hashlib.sha256(text.encode()).hexdigest()


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


def taken_from(module: str) -> set[str]:
    taken = set()
    for f in UNDERSTAT_FILES + [UTILS / "default_args.py"]:
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[-1] == module:
                taken |= {a.name for a in node.names}
            if isinstance(node, ast.Import):
                assert not any(a.name.split(".")[-1] == module for a in node.names), f
    return taken


def test_master_alerts_matches_the_allowed_version_in_what_understat_uses():
    names, digest = closure((UTILS / "alerts.py").read_text(encoding="utf-8"))
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


def test_understat_takes_only_the_checked_names():
    assert taken_from("alerts") == {"telegram_on_failure"}
    assert taken_from("config") == {"DAG_TAGS", "SCHEDULES", "UNDERSTAT_LEAGUES"}
    assert taken_from("medallion_config") == set()
    # config.py тянет medallion_config только лениво внутри функции, которой Understat не берёт;
    # default_args берёт из config только то, что не зависит от medallion_config
    assert "medallion_config" not in (UTILS / "default_args.py").read_text(encoding="utf-8")
