"""Рецепт своего контура Transfermarkt в deploy/transfermarkt/ (#1387).

Статически (без docker): блок-лист .airflowignore оставляет планировщику ровно четыре
TM-DAG; оба compose-файла самодостаточны (ни одного хостового пути литералом, каждый
bind — из переменной без дефолта, образ пинован); шлюз — в режиме transfermarkt-only без
бюджетных флагов; скрипты читают env-файл контура общим загрузчиком. Рендер через
`docker compose config` — последний тест, пропускается без docker.
"""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "transfermarkt"
AIRFLOW_COMPOSE = DEPLOY / "airflow.compose.yaml"
GATEWAY_COMPOSE = DEPLOY / "gateway.compose.yaml"
AIRFLOWIGNORE = DEPLOY / ".airflowignore"
ENV_EXAMPLE = DEPLOY / "transfermarkt.env.example"
SCRIPTS = tuple(sorted(p for p in DEPLOY.glob("*.sh") if p.name != "env.sh"))
TM_DAGS = {
    "dag_ingest_transfermarkt.py",
    "dag_discover_transfermarkt_registry.py",
    "dag_backfill_transfermarkt.py",
    "dag_transform_transfermarkt_silver.py",
}
HOST_PATH_LITERAL = re.compile(r"(?<![\w$])/(root/|tmp/|home/(?!airflow/))")
FAIL_CLOSED_VAR = re.compile(r"^\$\{(?P<name>[A-Z0-9_]+):\?[^}]+\}")
BUDGET_FLAGS = (
    "--daily-budget-mb",
    "--daily-budget-bytes",
    "--dagrun-budget-bytes",
    "--transfermarkt-dagrun-budget-bytes",
    "--transfermarkt-backfill-dagrun-budget-bytes",
    "--url-budget-bytes",
)


def _patterns() -> list[str]:
    return [
        line for line in AIRFLOWIGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def _code_lines(path: Path) -> str:
    return "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )


def _tracked_dag_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "dags"],
        check=True, capture_output=True, text=True,
    ).stdout.split()
    return [p[len("dags/"):] for p in out if p.endswith(".py")]


@pytest.mark.unit
def test_airflowignore_leaves_exactly_the_four_transfermarkt_dags() -> None:
    patterns = _patterns()
    # RE2 в Airflow: lookahead молча не работает — только явный блок-лист.
    assert "(?!" not in "".join(patterns)
    visible = [
        rel for rel in _tracked_dag_files()
        if not any(re.search(p, rel) for p in patterns)
    ]
    # Кроме четырёх DAG виден только пакетный __init__.py (DAG не объявляет).
    assert set(visible) == TM_DAGS | {"__init__.py"}, sorted(visible)
    init = (ROOT / "dags" / "__init__.py").read_text(encoding="utf-8")
    assert "DAG(" not in init and "@dag" not in init
    for name in TM_DAGS:
        assert "DAG(" in (ROOT / "dags" / name).read_text(encoding="utf-8"), name


@pytest.mark.unit
@pytest.mark.parametrize("compose", [AIRFLOW_COMPOSE, GATEWAY_COMPOSE], ids=["airflow", "gateway"])
def test_compose_is_self_contained_and_host_agnostic(compose: Path) -> None:
    text = _code_lines(compose)
    assert not HOST_PATH_LITERAL.search(text), "host paths must come from the contour env file"
    cfg = yaml.safe_load(text)
    assert cfg["name"] in {"transfermarkt-airflow", "transfermarkt-gw"}
    for name, service in cfg["services"].items():
        assert "build" not in service, f"{name}: the contour never builds images in place"
        assert FAIL_CLOSED_VAR.match(str(service.get("image", ""))), name
        for volume in service.get("volumes", []):
            if isinstance(volume, dict) and volume.get("type") == "bind":
                assert FAIL_CLOSED_VAR.match(volume["source"]), (name, volume["source"])
                assert volume["bind"]["create_host_path"] is False, (name, volume["target"])
        logging = service.get("logging") or cfg.get("x-tm-logging")
        assert logging["options"] == {"max-size": "50m", "max-file": "5"}, name
    for network in cfg["networks"].values():
        assert network["external"] is True


@pytest.mark.unit
def test_airflow_compose_pins_the_contour_shape() -> None:
    cfg = yaml.safe_load(AIRFLOW_COMPOSE.read_text(encoding="utf-8"))
    scheduler = cfg["services"]["airflow-scheduler"]
    env = scheduler["environment"]
    assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor"
    assert env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "true"
    assert "@airflow-metadb:5432/" in env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert env["TM_PROXY_CONTROL_URL"] == "http://transfermarkt_gw:8899"
    assert env["TM_BACKFILL_PROXY_CONTROL_URL"] == "http://transfermarkt_gw:8899"
    assert env["TRANSFERMARKT_REQUIRE_RAW_STORE"] == "true"
    assert env["TRANSFERMARKT_RAW_STORE_URI"] == "s3://warehouse/raw/transfermarkt"
    assert env["ALERT_ENV"] == "transfermarkt-isolated"
    # Фолбэка на общий токен нет: шлюз отказывает классу transfermarkt при равенстве.
    assert FAIL_CLOSED_VAR.match(env["TM_PROXY_CONTROL_TOKEN"])
    targets = {v["target"]: v["source"] for v in scheduler["volumes"]}
    assert targets["/opt/airflow/dags/.airflowignore"].endswith("/deploy/transfermarkt/.airflowignore")
    assert targets["/opt/airflow/logs"] == (
        "${TRANSFERMARKT_RUNTIME_DIR:?set the durable Transfermarkt runtime directory}/logs"
    )
    for target in ("/opt/airflow/dags", "/opt/airflow/scrapers", "/opt/airflow/scripts", "/opt/airflow/configs"):
        assert targets[target].startswith("${TRANSFERMARKT_RELEASE_ROOT:?"), target
    assert scheduler["networks"] == ["transfermarkt-net", "dp-storage"]
    init = cfg["services"]["airflow-init"]["command"][-1]
    for pool in ("transfermarkt_proxy", "transfermarkt_backfill_proxy", "transfermarkt_backfill_control"):
        assert f"airflow pools set '{pool}' 1 " in init, pool
    web = cfg["services"]["airflow-webserver"]
    assert web["profiles"] == ["ui"] and web["ports"] == ["127.0.0.1:8084:8080"]
    assert cfg["volumes"]["tm_airflow_pgdata"]["name"] == "transfermarkt_airflow_pgdata"


@pytest.mark.unit
def test_gateway_compose_is_transfermarkt_only_without_byte_budgets() -> None:
    cfg = yaml.safe_load(GATEWAY_COMPOSE.read_text(encoding="utf-8"))
    (name, gw), = cfg["services"].items()
    assert name == "transfermarkt_gw" and gw["container_name"] == "transfermarkt_gw"
    cmd = gw["command"]
    assert cmd[cmd.index("--source-mode") + 1] == "transfermarkt-only"
    assert not set(BUDGET_FLAGS) & set(cmd)
    assert cmd[cmd.index("--max-lease-mb") + 1] == "24"
    assert cmd[cmd.index("--max-active-leases") + 1] == "4"
    assert cmd[cmd.index("--lease-proxy-url") + 1] == "http://transfermarkt_gw:8900"
    assert cmd[cmd.index("--blocklist") + 1].startswith("/opt/transfermarkt-repo/")
    env = gw["environment"]
    assert env["PYTHONPATH"] == "/opt/transfermarkt-repo"
    assert env["PROXY_FILTER_ALLOW_FILE_FALLBACK"] == "false"
    # Своё имя переменной пула: общий .env платформы несёт PROXY_POOL_JSON общего шлюза.
    assert env["PROXY_POOL_JSON"].startswith("${TRANSFERMARKT_PROXY_POOL_JSON:?")
    targets = {v["target"]: v for v in gw["volumes"]}
    assert targets["/opt/transfermarkt-repo"]["read_only"] is True
    assert targets["/opt/airflow/logs/proxy_filter"]["source"].endswith("}/gateway-state")
    assert gw["deploy"]["resources"]["limits"]["memory"] == "1G"
    assert gw["healthcheck"]["test"] == ["CMD", "curl", "--fail", "--silent", "http://127.0.0.1:8899/health"]
    assert gw["networks"] == ["transfermarkt-net"]


@pytest.mark.unit
@pytest.mark.parametrize("script", SCRIPTS, ids=[p.name for p in SCRIPTS])
def test_scripts_read_the_contour_env_file_and_parse(script: Path) -> None:
    text = _code_lines(script)
    assert not HOST_PATH_LITERAL.search(text), f"{script.name}: host path literal"
    assert "TRANSFERMARKT_ENV_FILE" in text
    assert 'transfermarkt_load_env "$ENV_FILE"' in text
    assert "set -a" not in text
    subprocess.run(["bash", "-n", str(script)], check=True)


@pytest.mark.unit
def test_env_sh_accepts_only_contour_keys(tmp_path: Path) -> None:
    good = tmp_path / "good.env"
    good.write_text("TRANSFERMARKT_RELEASE_ROOT=/opt/x\nTM_PROXY_CONTROL_TOKEN='a b'\n", encoding="utf-8")
    bad = tmp_path / "bad.env"
    bad.write_text("PATH=/tmp/evil\n", encoding="utf-8")
    script = (
        f'. "{DEPLOY / "env.sh"}"; transfermarkt_load_env "$1" || exit $?; '
        'printf "%s|%s" "$TRANSFERMARKT_RELEASE_ROOT" "$TM_PROXY_CONTROL_TOKEN"'
    )
    ok = subprocess.run(["bash", "-c", script, "x", str(good)], capture_output=True, text=True)
    assert ok.returncode == 0 and ok.stdout == "/opt/x|a b"
    rejected = subprocess.run(["bash", "-c", script, "x", str(bad)], capture_output=True, text=True)
    assert rejected.returncode == 2


@pytest.mark.unit
def test_deploy_script_order_and_pool_handling() -> None:
    text = _code_lines(DEPLOY / "deploy.sh")
    order = [
        "state='running'",
        'airflow dags pause "$d"',
        "state IN ('queued','running','restarting')",
        'transfermarkt_set_env_var "$ENV_FILE" TRANSFERMARKT_RELEASE_ROOT "$RELEASE"',
        'TRANSFERMARKT_PROXY_POOL_JSON="$(cat "$TRANSFERMARKT_PROXY_POOL_FILE")"',
        'up -d --no-deps --force-recreate "$GW"',
        "up -d --no-deps --force-recreate airflow-scheduler",
        "set_pool transfermarkt_proxy 1",
        "last_parsed_time > TIMESTAMPTZ '$STARTED'",
        '["$INGEST"]=f ["$DISCOVER"]=f ["$BACKFILL"]=t ["$SILVER"]=t',
        'set_pause "$d" "$want"',
    ]
    positions = [text.index(marker) for marker in order]
    assert positions == sorted(positions)
    # Пул никогда не попадает в журнал и в env-файл.
    assert "echo \"$TRANSFERMARKT_PROXY_POOL_JSON" not in text
    assert "set_env_var \"$ENV_FILE\" TRANSFERMARKT_PROXY_POOL_JSON" not in text


@pytest.mark.unit
def test_auto_deliver_window_and_contract() -> None:
    text = (DEPLOY / "auto_deliver.sh").read_text(encoding="utf-8")
    assert "WINDOW_FROM=${WINDOW_FROM:-0100}" in text
    assert "WINDOW_TO=${WINDOW_TO:-0300}" in text
    assert "FAIL_NIGHTS_MAX=${FAIL_NIGHTS_MAX:-3}" in text
    assert "--drill-rollback" in text
    assert '"$old/deploy/transfermarkt/deploy.sh" "$old"' in text
    assert 'healthy 1073741824 transfermarkt-gw' in text
    # Слоты пулов возвращаются к снимку ДО приёмки, которая их сверяет.
    tail = text[text.index('log "deploy.sh вернул $rc"'):]
    assert tail.index("restore_state || restored=0") < tail.index('seen=$(acceptance_seen "$NEW"')
    # Запас окна (доставка + откат) пересчитывается после заморозки, перед снимком.
    assert "NEED_BUDGET=$(( 2 * (DEPLOY_CEILING + 30 + ACCEPT_WAIT + ACCEPT_POLL) + BUDGET_RESERVE ))" in text
    # Бюджет по умолчанию (доставка + откат + резерв) влезает в окно 01:00–03:00.
    need = 2 * (1800 + 30 + 480 + 20) + 600
    assert need < 7200 - 300, need
    assert text.index('freeze_release.sh" "$WANT"') < text.rindex('-lt "$NEED_BUDGET"') < text.index("Шаг 8")
    # Приёмка требует живого SchedulerJob.
    assert "[ \"$got\" = healthy ] || { echo 0; return; }" in text
    # Обрыв после остановки шлюза: монты на OLD не доказывают живой бой.
    assert "[ \"$gw_health\" != healthy ]" in text


@pytest.mark.unit
def test_env_example_covers_every_fail_closed_compose_variable() -> None:
    def keys(path: Path) -> set[str]:
        return {
            line.split("=", 1)[0].lstrip("# ").strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if "=" in line and re.match(r"^#? ?[A-Z_][A-Z0-9_]*=", line)
        }

    documented = keys(ENV_EXAMPLE) | keys(ROOT / ".env.example")
    for compose in (AIRFLOW_COMPOSE, GATEWAY_COMPOSE):
        required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", compose.read_text(encoding="utf-8")))
        # Пул подаёт deploy.sh из файла — в env-файле его нет намеренно.
        missing = required - documented - {"TRANSFERMARKT_PROXY_POOL_JSON"}
        assert not missing, f"{compose.name}: {sorted(missing)} documented nowhere"


@pytest.mark.unit
@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI is not available")
def test_compose_config_renders_with_a_fake_env(tmp_path: Path) -> None:
    platform = tmp_path / "platform.env"
    platform.write_text(
        "AIRFLOW__CORE__FERNET_KEY=f\nAIRFLOW__WEBSERVER__SECRET_KEY=s\nTRINO_PORT=8443\n"
        "TRINO_PASSWORD=p\nS3_ACCESS_KEY=a\nS3_SECRET_KEY=b\n_AIRFLOW_WWW_USER_USERNAME=u\n"
        "_AIRFLOW_WWW_USER_PASSWORD=p\nPROXY_FILTER_CONTROL_TOKEN=shared\n"
        "PROXY_POOL_JSON=shared-pool\nTM_PROXY_CONTROL_URL=http://proxy_filter:8899\n",
        encoding="utf-8",
    )
    contour = tmp_path / "tm.env"
    contour.write_text(
        "TRANSFERMARKT_RELEASE_ROOT=/opt/transfermarkt/releases/release-00000000\n"
        "TRANSFERMARKT_RUNTIME_DIR=/srv/tm-runtime\nTRANSFERMARKT_AIRFLOW_IMAGE=img:pinned\n"
        "TRANSFERMARKT_POSTGRES_IMAGE=postgres:16-alpine\nTRANSFERMARKT_AIRFLOW_DB_PASSWORD=p\n"
        "TM_PROXY_CONTROL_TOKEN=tm\n",
        encoding="utf-8",
    )
    for compose in (AIRFLOW_COMPOSE, GATEWAY_COMPOSE):
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose), "--env-file", str(platform),
             "--env-file", str(contour), "config", "--format", "json"],
            cwd=tmp_path, capture_output=True, text=True,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "TRANSFERMARKT_PROXY_POOL_JSON": "tm-pool"},
        )
        if result.returncode != 0 and "is not a docker command" in result.stderr:
            pytest.skip("docker compose plugin is not available")
        assert result.returncode == 0, result.stderr
        rendered = result.stdout
        assert "shared-pool" not in rendered
        assert "http://proxy_filter:8899" not in rendered
