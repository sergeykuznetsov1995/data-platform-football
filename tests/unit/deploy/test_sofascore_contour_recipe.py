"""Рецепт контура SofaScore в deploy/sofascore/ — один источник (#1155, этап 3).

Проверяет статически (без docker), что оба compose-файла самодостаточны: ни одного
хостового пути литералом, ни одного override-тега, каждый bind — из переменной без
дефолта; что мини-DAG контура спрятаны от общего scheduler'а и не спрятаны от своего;
что скрипты ротации не зашивают пути хоста. Рендер через `docker compose config`
живёт в tests/integration/test_compose_validity.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "sofascore"
AIRFLOW_COMPOSE = DEPLOY / "airflow.compose.yaml"
GATEWAY_COMPOSE = DEPLOY / "gateway.compose.yaml"
CONTOUR_AIRFLOWIGNORE = DEPLOY / ".airflowignore"
SHARED_AIRFLOWIGNORE = ROOT / "dags" / ".airflowignore"
ENV_EXAMPLE = DEPLOY / "sofascore.env.example"
UNIT = DEPLOY / "systemd" / "sofascore-gw-lease-watchdog.service"
SCRIPTS = (
    DEPLOY / "freeze_release.sh",
    DEPLOY / "run_canary.sh",
    DEPLOY / "deploy.sh",
    DEPLOY / "postdeploy_checks.sh",
)
MINI_DAGS = ("dag_trigger_sofascore_daily.py", "dag_sofascore_manifest_maintenance.py")

# Три полосы источника (#1244): свой шлюз, свой дневной потолок, свой слот аренды,
# свой каталог состояния. До развода все три ходили через sofascore_gw_951 и давали
# `HTTP 429: paid-proxy concurrency limit reached` на ≥85 % запусков истории.
GATEWAY_LANES = {
    "sofascore_proxy_filter": {
        "container": "sofascore_gw_951",
        "budget": "${SOFASCORE_PROXY_DAILY_BUDGET_MB:-600}",
        "leases": "${SOFASCORE_PROXY_MAX_ACTIVE_LEASES:-1}",
        "state": "${SOFASCORE_GATEWAY_STATE_HOST_DIR:?",
    },
    "sofascore_gw_history": {
        "container": "sofascore_gw_history",
        "budget": "${SOFASCORE_HISTORY_GW_DAILY_BUDGET_MB:-2000}",
        "leases": "${SOFASCORE_HISTORY_GW_MAX_ACTIVE_LEASES:-1}",
        "state": "${SOFASCORE_HISTORY_GW_STATE_HOST_DIR:?",
    },
    "sofascore_gw_players": {
        "container": "sofascore_gw_players",
        "budget": "${SOFASCORE_PLAYERS_GW_DAILY_BUDGET_MB:-400}",
        "leases": "${SOFASCORE_PLAYERS_GW_MAX_ACTIVE_LEASES:-1}",
        "state": "${SOFASCORE_PLAYERS_GW_STATE_HOST_DIR:?",
    },
}

# Host-side paths that must never be baked into the recipe; /home/airflow is the
# container home and is allowed.
HOST_PATH_LITERAL = re.compile(r"(?<![\w$])/(root/|tmp/|home/(?!airflow/))")
FAIL_CLOSED_VAR = re.compile(r"^\$\{(?P<name>[A-Z0-9_]+):\?[^}]+\}")
SECRET_LITERAL = re.compile(r"://[^:/\s]+:(?!\$\{)[^@\s]+@")
HEX64 = re.compile(r"\b[0-9a-f]{64}\b")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _code_lines(path: Path) -> str:
    """File text without comment-only lines (comments may name the old chain)."""
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


@pytest.mark.unit
@pytest.mark.parametrize("compose", [AIRFLOW_COMPOSE, GATEWAY_COMPOSE], ids=["airflow", "gateway"])
def test_compose_is_self_contained_and_host_agnostic(compose: Path) -> None:
    text = _code_lines(compose)
    assert "!override" not in text and "!reset" not in text, "chain tags mean the recipe is split again"
    assert not HOST_PATH_LITERAL.search(text), "host paths must come from the contour env file"
    assert not SECRET_LITERAL.search(text), "credentials embedded in a URL literal"
    assert not HEX64.search(text), "artifact ids are deployment values, not file contents"
    cfg = yaml.safe_load(text)
    assert cfg["name"] in {"sofascore-airflow", "sofascore-gw"}
    for name, service in cfg["services"].items():
        assert "build" not in service, f"{name}: the contour never builds images in place"
        assert FAIL_CLOSED_VAR.match(str(service.get("image", ""))), (
            f"{name}: image must be a pinned fail-closed variable"
        )
        for source in _bind_sources(service):
            assert FAIL_CLOSED_VAR.match(source), f"{name}: bind source {source!r} is not a fail-closed variable"
        for volume in service.get("volumes", []):
            if isinstance(volume, str) and not volume.startswith("ss_"):
                pytest.fail(f"{name}: short-syntax bind {volume!r} lets Docker create a missing source")
            if isinstance(volume, dict) and volume.get("type") == "bind":
                assert volume["bind"]["create_host_path"] is False, f"{name}: {volume['target']}"
    for network in cfg["networks"].values():
        assert network.get("external") is True


@pytest.mark.unit
def test_airflow_compose_pins_the_live_scheduler_shape() -> None:
    cfg = _load(AIRFLOW_COMPOSE)
    assert set(cfg["services"]) == {"airflow-metadb", "airflow-init", "airflow-scheduler", "airflow-webserver"}
    scheduler = cfg["services"]["airflow-scheduler"]
    targets = _targets(scheduler)
    assert targets == {
        "/opt/airflow/dags",
        "/opt/airflow/dags/.airflowignore",
        "/opt/airflow/logs",
        "/opt/airflow/scrapers",
        "/opt/airflow/scripts",
        "/opt/airflow/configs/medallion",
        "/opt/airflow/configs/soccerdata",
        "/opt/airflow/configs/sofascore",
        "/opt/airflow/configs/proxy_filter",
        "/opt/airflow/docker",
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
        "/opt/airflow/runtime/sofascore/all-men",
        "/opt/airflow/proxys.txt",
        "/opt/legacy-scraper-venv",
        "/home/airflow/soccerdata",
    }
    # Mini-DAGs are ordinary repository files now; no file bind may shadow them.
    assert not any(t.endswith(tuple(MINI_DAGS)) for t in targets)
    env = scheduler["environment"]
    assert env["SOFASCORE_PROXY_CONTROL_URL"] == "http://sofascore_proxy_filter:8899"
    assert env["SOFASCORE_ALL_MENS_STATE"] == "/opt/airflow/runtime/sofascore/all-men/state.json"
    assert env["SOFASCORE_REFRESH_BATCH_SIZE"] == "${SOFASCORE_REFRESH_BATCH_SIZE:-3}"
    assert "dp-backend" not in cfg["networks"]
    assert set(scheduler["networks"]) == {"sofascore-net", "dp-storage"}
    assert scheduler["deploy"]["resources"]["limits"]["memory"] == "10G"
    assert cfg["volumes"]["ss_airflow_pgdata"]["name"] == "sofascore_airflow_pgdata"
    # The contour metadata-DB password is its own secret on both sides of the connection.
    conn = env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert conn.startswith("postgresql+psycopg2://airflow:${SOFASCORE_AIRFLOW_DB_PASSWORD:?")
    assert conn.endswith("@airflow-metadb:5432/airflow")
    assert cfg["services"]["airflow-metadb"]["environment"]["POSTGRES_PASSWORD"].startswith(
        "${SOFASCORE_AIRFLOW_DB_PASSWORD:?"
    )


@pytest.mark.unit
def test_gateway_compose_pins_the_live_gateway_shape() -> None:
    cfg = _load(GATEWAY_COMPOSE)
    assert set(cfg["services"]) == set(GATEWAY_LANES)
    state_sources, artifact_sources, tokens = [], set(), set()
    for service, lane in GATEWAY_LANES.items():
        gateway = cfg["services"][service]
        assert gateway["container_name"] == lane["container"], service
        assert gateway["environment"]["PYTHONPATH"] == "/opt/sofascore-repo", service
        assert gateway["environment"]["PROXY_POOL_JSON"].startswith("${SOFASCORE_PROXY_POOL_JSON:?"), service
        binds = {v["target"]: v for v in gateway["volumes"]}
        assert set(binds) == {
            "/opt/sofascore-repo",
            "/opt/airflow/proxys.txt",
            "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
            "/opt/airflow/logs/sofascore_proxy_filter",
        }, service
        for target, bind in binds.items():
            assert bind["bind"]["create_host_path"] is False, (service, target)
            assert bind.get("read_only", False) is (target != "/opt/airflow/logs/sofascore_proxy_filter"), (
                service, target
            )
        command = gateway["command"]
        assert command[:2] == ["python", "/opt/sofascore-repo/scripts/proxy_filter/filter_proxy.py"], service
        # Каждый шлюз арендует прокси у СЕБЯ: общий lease-url свёл бы три полосы
        # обратно в один слот аренды — ровно тот дефект, который чинит #1244.
        assert command[command.index("--lease-proxy-url") + 1] == f"http://{service}:8900", service
        assert command[command.index("--daily-budget-mb") + 1] == lane["budget"], service
        assert command[command.index("--max-active-leases") + 1] == lane["leases"], service
        assert gateway["healthcheck"]["test"][:4] == [
            "CMD", "python", "/opt/sofascore-repo/scripts/sofascore_runtime_preflight.py", "gateway-health",
        ], service
        assert gateway["networks"] == ["sofascore-net"], service
        assert gateway["deploy"]["resources"]["limits"]["memory"] == "1G", service
        state = binds["/opt/airflow/logs/sofascore_proxy_filter"]["source"]
        assert state.startswith(lane["state"]), service
        state_sources.append(state)
        artifact_sources.add(binds["/opt/airflow/runtime/sofascore/proxy_budget_canary.json"]["source"])
        tokens.add(gateway["environment"]["PROXY_FILTER_CONTROL_TOKEN"])
    # WAL/ledger рассчитаны на единственного писателя: общий каталог на три процесса
    # портит учёт байтов и восстановление аренд.
    assert len(set(state_sources)) == 3, state_sources
    # Артефакт и токен — общие: тот же digest, та же контрольная плоскость.
    assert len(artifact_sources) == 1 and len(tokens) == 1
    assert set(cfg["networks"]) == {"sofascore-net"}


@pytest.mark.unit
def test_mini_dags_are_repository_files_hidden_only_from_the_shared_scheduler() -> None:
    for name in MINI_DAGS:
        source = (ROOT / "dags" / name).read_text(encoding="utf-8")
        assert source.strip(), name
        compile(source, name, "exec")
        assert f'dag_id="{name[:-3]}"' in source
    shared = [
        line for line in SHARED_AIRFLOWIGNORE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    contour = [
        line for line in CONTOUR_AIRFLOWIGNORE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    for name in MINI_DAGS:
        assert any(re.match(pattern, name) for pattern in shared), f"shared scheduler must ignore {name}"
        assert not any(re.match(pattern, name) for pattern in contour), f"contour must load {name}"
    for name in ("dag_ingest_sofascore.py", "dag_backfill_sofascore_all_mens.py", "dag_refresh_sofascore_all_mens.py"):
        assert not any(re.match(pattern, name) for pattern in contour), name
    for foreign in ("dag_backup_whoscored_storage.py", "dag_ingest_fbref.py", "dag_sofascore_pipeline.py", "dag_iceberg_maintenance.py"):
        assert any(re.match(pattern, foreign) for pattern in contour), foreign
    # RE2: no lookahead in either file.
    assert "(?!" not in "".join(shared + contour)


@pytest.mark.unit
def test_rotation_scripts_take_every_host_path_from_the_env_file() -> None:
    example_keys = {
        line.split("=", 1)[0]
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    for script in SCRIPTS:
        text = _code_lines(script)
        assert not HOST_PATH_LITERAL.search(text), f"{script.name}: host path literal"
        assert "SOFASCORE_ENV_FILE" in text, f"{script.name}: must read the contour env file"
        assert 'sofascore_load_env "$ENV_FILE"' in text, f"{script.name}: must use the shared loader"
        assert "set -a" not in text, f"{script.name}: exporting the env file lets stale values outrank --env-file"
        subprocess.run(["bash", "-n", str(script)], check=True)
        for key in re.findall(r"\$\{(SOFASCORE_[A-Z0-9_]+):\?", text):
            assert key in example_keys, f"{script.name}: {key} missing from sofascore.env.example"
    deploy = (DEPLOY / "deploy.sh").read_text(encoding="utf-8")
    for key in ("SOFASCORE_RELEASE_ROOT", "SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST", "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID"):
        assert f"set_env_var {key} " in deploy, f"deploy.sh must repin {key} in the env file"
    assert "sed -i" not in deploy.replace('sed -i "s#^${key}=', ""), "no ad-hoc edits of system files"
    assert "--no-deps --force-recreate airflow-scheduler" in deploy
    assert "--no-deps --force-recreate sofascore_proxy_filter" in deploy
    assert "--project-directory \"$RELEASE\"" in deploy


@pytest.mark.unit
def test_env_example_covers_every_fail_closed_compose_variable() -> None:
    example_keys = {
        line.split("=", 1)[0]
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    platform_keys = {
        line.split("=", 1)[0]
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    for compose in (AIRFLOW_COMPOSE, GATEWAY_COMPOSE):
        required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", compose.read_text(encoding="utf-8")))
        missing = required - example_keys - platform_keys
        assert not missing, f"{compose.name}: {sorted(missing)} documented nowhere"
    assert not HOST_PATH_LITERAL.search(ENV_EXAMPLE.read_text(encoding="utf-8"))


@pytest.mark.unit
def test_watchdog_unit_reads_the_contour_env_file_instead_of_pinning_a_tree() -> None:
    unit = _code_lines(UNIT)
    assert "EnvironmentFile=/etc/data-platform/sofascore.env" in unit
    assert "--expected-mount ${SOFASCORE_RELEASE_ROOT}" in unit
    assert "--state-dir ${SOFASCORE_GATEWAY_STATE_HOST_DIR}" in unit
    assert not HOST_PATH_LITERAL.search(unit)
    assert "dpf-release" not in unit
