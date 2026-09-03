"""Рецепт контура FotMob в deploy/fotmob/ — один источник (#1155, этап 3).

Проверяет статически (без docker), что compose-файл живого контура самодостаточен:
ни одного хостового пути литералом, ни одного override-тега, каждый bind — из
переменной без дефолта; что блок-лист DagBag прячет КАЖДЫЙ чужой DAG репозитория и
ни одного своего (RE2: allow-list через lookahead невозможен, поэтому блок-лист
обязан быть полным — иначе новый чужой DAG на master молча въедет в изолят); что
скрипты автомата доставки не зашивают пути хоста и пины выката. Рендер через
`docker compose config` живёт в tests/integration/test_compose_validity.py, прогон
скриптов против заглушек — в test_fotmob_delivery_scripts.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "fotmob"
COMPOSE = DEPLOY / "isolated.compose.yaml"
BLOCKLIST = DEPLOY / "isolated.airflowignore"
ENV_EXAMPLE = DEPLOY / "fotmob.env.example"
ENV_SH = DEPLOY / "env.sh"
RUNBOOK = ROOT / "docs" / "operations" / "fotmob-isolated-ceremony-free.md"
# Имена, которые скрипты законно берут из окружения ДО загрузчика (он сбрасывает все
# FOTMOB_*): путь к самому env-файлу и эстафета автомата b6.
PROCESS_PROTOCOL = {"FOTMOB_ENV_FILE", "FOTMOB_DELIVER_NONCE", "FOTMOB_DELIVER_NONCE_FILE"}
SCRIPTS = (
    DEPLOY / "auto_deliver.sh",
    DEPLOY / "b6_deliver.sh",
    DEPLOY / "window_alert.sh",
)
# Ровно семь корневых DAG изолята (docs/operations/fotmob-isolated-ceremony-free.md).
CONTOUR_DAGS = {
    "dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver.py",
    "dag_trigger_fotmob_daily.py",
    "dag_refresh_fotmob.py",
    "dag_backfill_fotmob.py",
    "dag_collect_fotmob_players.py",
    "dag_orchestrate_fotmob.py",
}

# Host-side paths that must never be baked into the recipe; /home/airflow is the
# container home and is allowed.
HOST_PATH_LITERAL = re.compile(r"(?<![\w$])/(root|tmp)(?![\w-])|(?<![\w$])/home/(?!airflow/)")
FAIL_CLOSED_VAR = re.compile(r"^\$\{(?P<name>[A-Z0-9_]+):\?[^}]+\}")
SECRET_LITERAL = re.compile(r"://[^:/\s]+:(?!\$\{)[^@\s]+@")
HEX40 = re.compile(r"\b[0-9a-f]{40}\b")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _code_lines(path: Path) -> str:
    """File text without comment-only lines (comments may name the old layout)."""
    return "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )


def _split_short_volume(spec: str) -> list[str]:
    """Split ``source:target[:mode]`` without breaking on ``${VAR:?message}``."""
    parts, depth, current = [], 0, []
    for char in spec:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        if char == ":" and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    parts.append("".join(current))
    return parts


def _bind_sources(service: dict) -> list[str]:
    sources = []
    for volume in service.get("volumes", []):
        if isinstance(volume, dict):
            if volume.get("type") == "bind":
                sources.append(volume["source"])
        else:
            source = _split_short_volume(volume)[0]
            if source.startswith(("/", "$", ".")):
                sources.append(source)
    return sources


def _targets(service: dict) -> set[str]:
    return {
        (v["target"] if isinstance(v, dict) else _split_short_volume(v)[1])
        for v in service.get("volumes", [])
    }


def _patterns(path: Path) -> list[str]:
    return [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]


def _env_keys(path: Path) -> set[str]:
    return {
        line.split("=", 1)[0]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }


@pytest.mark.unit
def test_compose_is_self_contained_and_host_agnostic() -> None:
    text = _code_lines(COMPOSE)
    assert "!override" not in text and "!reset" not in text, "chain tags mean the recipe is split again"
    assert not HOST_PATH_LITERAL.search(text), "host paths must come from the contour env file"
    assert not SECRET_LITERAL.search(text), "credentials embedded in a URL literal"
    assert not HEX40.search(text), "commit pins are deployment values, not file contents"
    cfg = yaml.safe_load(text)
    assert cfg["name"] == "fotmob-airflow"
    for name, service in cfg["services"].items():
        assert "build" not in service, f"{name}: the contour never builds images in place"
        assert FAIL_CLOSED_VAR.match(str(service.get("image", ""))), (
            f"{name}: image must be a pinned fail-closed variable"
        )
        for source in _bind_sources(service):
            assert FAIL_CLOSED_VAR.match(source), f"{name}: bind source {source!r} is not a fail-closed variable"
        for volume in service.get("volumes", []):
            if isinstance(volume, str) and not volume.startswith("fm_"):
                pytest.fail(f"{name}: short-syntax bind {volume!r} lets Docker create a missing source")
            if isinstance(volume, dict) and volume.get("type") == "bind":
                assert volume["bind"]["create_host_path"] is False, f"{name}: {volume['target']}"
    for name, network in cfg["networks"].items():
        assert name == "default" or network.get("external") is True, name


@pytest.mark.unit
def test_compose_pins_the_live_contour_shape() -> None:
    cfg = _load(COMPOSE)
    services = cfg["services"]
    assert set(services) == {"airflow-metadb", "airflow-init", "airflow-scheduler", "airflow-webserver"}
    # Автомат, сторож и рунбук ходят к контейнерам по этим именам.
    assert {s["container_name"] for s in services.values()} == {
        "fotmob-airflow-metadb", "fotmob-airflow-init", "fotmob-airflow-scheduler", "fotmob-airflow-webserver",
    }
    scheduler = services["airflow-scheduler"]
    assert _targets(scheduler) == {
        "/opt/airflow/dags",
        "/opt/airflow/dags/.airflowignore",
        "/opt/airflow/logs",
        "/opt/airflow/scrapers",
        "/opt/airflow/scripts",
        "/opt/airflow/configs/medallion",
        "/opt/airflow/configs/fotmob",
    }
    # init поднимается из того же якоря: v1-рецепт с протухшим блок-листом и file-bind
    # мини-дага больше не может воскреснуть при пересоздании init.
    assert _targets(services["airflow-init"]) == _targets(scheduler)
    binds = {v["target"]: v for v in scheduler["volumes"]}
    ignore = binds["/opt/airflow/dags/.airflowignore"]
    assert ignore["source"].startswith("${FOTMOB_DAGBAG_IGNORE_FILE:?"), (
        "блок-лист монтируется из хостового файла: file-bind из дерева, которое переставляет "
        "git checkout, протухает по иноду"
    )
    assert ignore["read_only"] is True
    for target, bind in binds.items():
        if target != "/opt/airflow/dags/.airflowignore":
            assert bind["source"].startswith("${FOTMOB_RELEASE_ROOT:?"), target
        assert bind.get("read_only", False) is (target != "/opt/airflow/logs"), target
    env = scheduler["environment"]
    assert env["FOTMOB_ISOLATED_STACK"] == "1"
    assert env["ALERT_ENV"] == "fotmob-isolated"
    conn = env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert conn.startswith("postgresql+psycopg2://airflow:${FOTMOB_AIRFLOW_DB_PASSWORD:?")
    assert conn.endswith("@fotmob-airflow-metadb:5432/airflow")
    assert services["airflow-metadb"]["environment"]["POSTGRES_PASSWORD"].startswith(
        "${FOTMOB_AIRFLOW_DB_PASSWORD:?"
    )
    assert "SYS_PTRACE" in scheduler["cap_add"]
    assert set(scheduler["networks"]) == {"default", "dp-storage"}
    assert "dp-backend" not in cfg["networks"], "alias postgres общей Airflow-БД — риск двойного scheduler'а"
    assert scheduler["deploy"]["resources"]["limits"]["memory"] == "10G"
    assert services["airflow-metadb"]["deploy"]["resources"]["limits"]["memory"] == "512M"
    assert services["airflow-webserver"]["profiles"] == ["ui"]
    assert cfg["volumes"]["fm_airflow_pgdata"]["name"] == "fotmob_airflow_pgdata"


@pytest.mark.unit
def test_blocklist_hides_every_foreign_dag_and_none_of_the_contour() -> None:
    patterns = _patterns(BLOCKLIST)
    assert "(?!" not in "".join(patterns), "RE2: lookahead is silently ignored"
    for pattern in patterns:
        re.compile(pattern)
    dag_files = sorted(p.name for p in (ROOT / "dags").glob("dag_*.py"))
    assert CONTOUR_DAGS <= set(dag_files)
    leaked = [
        name for name in dag_files
        if name not in CONTOUR_DAGS and not any(re.match(pattern, name) for pattern in patterns)
    ]
    assert not leaked, (
        f"чужие DAG без строки в deploy/fotmob/isolated.airflowignore: {leaked} — "
        "добавь их в блок-лист (иначе они попадут в DagBag изолята)"
    )
    hidden = [name for name in CONTOUR_DAGS if any(re.match(pattern, name) for pattern in patterns)]
    assert not hidden, f"блок-лист прячет собственные DAG контура: {hidden}"
    for directory in ("configs", "scripts", "sql", "utils"):
        assert any(re.match(pattern, f"{directory}/x.py") for pattern in patterns), directory


@pytest.mark.unit
def test_delivery_scripts_take_every_host_path_and_pin_from_the_env_file() -> None:
    example_keys = _env_keys(ENV_EXAMPLE)
    for script in SCRIPTS:
        text = _code_lines(script)
        assert not HOST_PATH_LITERAL.search(text), f"{script.name}: host path literal"
        assert not HEX40.search(text), f"{script.name}: commit pin baked into the script"
        assert "FOTMOB_ENV_FILE" in text, f"{script.name}: must read the contour env file"
        assert 'fotmob_load_env "$ENV_FILE"' in text, f"{script.name}: must use the shared loader"
        assert "set -a" not in text, f"{script.name}: exporting the env file lets stale values leak"
        # Загрузчик сбрасывает все FOTMOB_* окружения, поэтому всё, что скрипт берёт из
        # окружения под этим именем, снимается ДО него — и только имена протокола процесса.
        before, after = text.split('fotmob_load_env "$ENV_FILE"', 1)
        early = set(re.findall(r"\$\{(FOTMOB_[A-Z0-9_]+)", before))
        assert early <= PROCESS_PROTOCOL, f"{script.name}: {sorted(early - PROCESS_PROTOCOL)} read before the loader"
        assert "${FOTMOB_DELIVER_NONCE" not in after, f"{script.name}: handshake read after the loader wiped it"
        subprocess.run(["bash", "-n", str(script)], check=True)
        for key in re.findall(r"\$\{(FOTMOB_[A-Z0-9_]+):\?", text):
            assert key in example_keys, f"{script.name}: {key} missing from fotmob.env.example"
    auto = _code_lines(DEPLOY / "auto_deliver.sh")
    b6 = _code_lines(DEPLOY / "b6_deliver.sh")
    # Оба скрипта берут ОДИН замок — иначе b6 отбивает законный вызов автомата.
    assert "fotmob-auto-deliver.lock" in auto and "fotmob-auto-deliver.lock" in b6
    assert "${FOTMOB_STATE_DIR:?" in b6
    # Автомат зовёт b6 по соседству, а не по зашитому пути.
    assert 'b6_deliver.sh' in auto
    assert not re.search(r"^\s*TG=", auto, re.M), "unused TG hook path was the last host literal"
    assert "fotmob_load_env()" in ENV_SH.read_text(encoding="utf-8")


@pytest.mark.unit
def test_env_example_covers_every_fail_closed_compose_variable() -> None:
    example_keys = _env_keys(ENV_EXAMPLE)
    platform_keys = _env_keys(ROOT / ".env.example")
    required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", COMPOSE.read_text(encoding="utf-8")))
    missing = required - example_keys - platform_keys
    assert not missing, f"{COMPOSE.name}: {sorted(missing)} documented nowhere"
    assert all(key.startswith("FOTMOB_") for key in example_keys), "env.sh accepts only FOTMOB_* keys"
    assert not HOST_PATH_LITERAL.search(ENV_EXAMPLE.read_text(encoding="utf-8"))
    assert "FOTMOB_DAGBAG_IGNORE_FILE" in example_keys
    # Ключ без потребителя — второй источник хостового пути: его правят, а читает никто.
    referenced: set[str] = set()
    for path in (COMPOSE, *SCRIPTS, RUNBOOK):
        referenced |= set(re.findall(r"FOTMOB_[A-Z0-9_]+", path.read_text(encoding="utf-8")))
    unused = example_keys - referenced
    assert not unused, f"{ENV_EXAMPLE.name}: {sorted(unused)} read by nothing (compose, scripts, runbook)"


@pytest.mark.unit
def test_runbook_never_runs_compose_up_without_no_deps() -> None:
    """`up` без `--no-deps` тянет depends_on и пересоздаёт airflow-init (общий стек —
    SHARED-STACK-PROTOCOL.md); первый запуск тоже идёт по одному сервису."""
    for path in (RUNBOOK, COMPOSE):
        for line in path.read_text(encoding="utf-8").splitlines():
            if re.search(r"\bup( -d)?\b", line) and ("compose" in line or line.lstrip().startswith("fmc ")):
                assert "--no-deps" in line, f"{path.name}: {line.strip()}"
