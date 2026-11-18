"""Static isolation checks; never start Docker, Airflow tasks or a browser."""

import ast
from pathlib import Path
import re

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "deploy/clubelo/solver_canary.compose.yaml"
DAG = ROOT / "deploy/clubelo/solver_canary_dag.py"
SCRIPT = ROOT / "scripts/research/clubelo_solver_canary.py"
OBSERVER_DIR = "/var/lib/clubelo-canary-run"
PLAN = ROOT / "docs/operations/clubelo-solver-stack-eval-2251.md"


@pytest.fixture
def compose():
    return yaml.safe_load(COMPOSE.read_text())


def test_network_membership_and_no_host_publication(compose):
    assert set(compose["services"]) == {"clubelo-canary-gateway", "solver-control", "solver-candidate", "runner"}
    assert set(compose["networks"]) == {"internal", "egress"}
    assert compose["networks"]["internal"] == {"internal": True, "driver": "bridge"}
    assert compose["networks"]["egress"] == {"driver": "bridge"}
    for name, service in compose["services"].items():
        assert set(service["networks"]) == ({"internal"} if name.startswith("solver-") else {"internal", "egress"})
        assert not any(key in service for key in (
            "ports", "network_mode", "privileged", "cap_add", "devices", "sysctls",
            "pid", "ipc", "extra_hosts", "volumes_from", "extends"))
        assert service["cap_drop"] == ["ALL"]
        assert service["read_only"] is True
        assert service["restart"] == "no"
        assert service["security_opt"] == ["no-new-privileges:true"]
        assert service["logging"]["driver"] == "none"
        assert service["mem_limit"] and service["pids_limit"] and service["cpus"]
        assert "env_file" not in service
        for mount in service["volumes"]:
            assert mount["type"] == "bind"
            assert mount["bind"] == {"create_host_path": False}
    assert "dp-backend" not in COMPOSE.read_text()


def test_per_service_non_root_users(compose):
    expected_users = {
        "clubelo-canary-gateway": "${CLUBELO_CANARY_GATEWAY_UID:-50000}:${CLUBELO_CANARY_GATEWAY_GID:-0}",
        "solver-control": "${CLUBELO_CANARY_SOLVER_UID:-1000}:${CLUBELO_CANARY_SOLVER_GID:-1000}",
        "solver-candidate": "${CLUBELO_CANARY_SOLVER_UID:-1000}:${CLUBELO_CANARY_SOLVER_GID:-1000}",
        "runner": "${CLUBELO_CANARY_RUNNER_UID:-50000}:${CLUBELO_CANARY_RUNNER_GID:-0}",
    }
    assert {name: service["user"] for name, service in compose["services"].items()} == expected_users
    for service in compose["services"].values():
        match = re.fullmatch(
            r"\$\{CLUBELO_CANARY_[A-Z]+_UID:-(\d+)\}:"
            r"\$\{CLUBELO_CANARY_[A-Z]+_GID:-(\d+)\}",
            service["user"],
        )
        assert match is not None
        assert int(match.group(1)) > 0
    source = COMPOSE.read_text()
    assert "CLUBELO_CANARY_UID" not in source
    assert "CLUBELO_CANARY_GID" not in source


def test_immutable_images_and_separate_arm_state(compose):
    services = compose["services"]
    for name, service in services.items():
        if name == "clubelo-canary-gateway":
            # Existing gateway contract takes the entire digest-pinned image.
            # Resolved value/provenance verification belongs to the controller.
            assert service["image"] == "${CLUBELO_GATEWAY_IMAGE:?Required existing gateway image pinned by sha256 digest}"
        else:
            assert "@sha256:" in service["image"]
        assert service["pull_policy"] == "never"
        assert "build" not in service
    assert services["runner"]["image"] == (
        "apache/airflow:2.11.2-python3.11@sha256:"
        "aac8468234dfa802c5ac4d6318bbf3b620793b55569ac230f584dc3c1f0ca49d")
    arm_sources = []
    for arm in ("control", "candidate"):
        solver = services["solver-" + arm]
        assert solver["profiles"] == [arm]
        mounts = {mount["target"]: mount for mount in solver["volumes"]}
        assert len(solver["volumes"]) == 1
        assert set(mounts) == {OBSERVER_DIR}
        assert mounts[OBSERVER_DIR]["source"] == (
            "${CLUBELO_CANARY_" + arm.upper()
            + "_OBSERVER_DIR:?Required fresh " + arm + " run observer dir}"
        )
        assert not mounts[OBSERVER_DIR].get("read_only", False)
        env = solver["environment"]
        assert env == {
            "PYTHONDONTWRITEBYTECODE": "1",
            "CAPTCHA_SOLVER": "none",
            "CLUBELO_CANARY_UPSTREAM": arm,
        }
        # Preserve the common image's reviewed entrypoint and instrumentation.
        assert "entrypoint" not in solver and "command" not in solver
        assert env["CAPTCHA_SOLVER"] == "none"
        arm_sources.extend(mount["source"] for mount in mounts.values())
    assert len(set(arm_sources)) == 2


def test_runner_only_selected_state_code_dag_token_and_output(compose):
    runner = compose["services"]["runner"]
    assert "env_file" not in runner
    mounts = {mount["target"]: mount for mount in runner["volumes"]}
    assert len(runner["volumes"]) == 5
    assert set(mounts) == {"/canary/code", "/canary/dags/solver_canary_dag.py",
                           "/canary/secrets/control_token", OBSERVER_DIR,
                           "/canary/output"}
    for target, mount in mounts.items():
        assert mount["type"] == "bind"
        assert mount.get("read_only", False) == (target != "/canary/output")
        assert "docker.sock" not in mount["source"]
        assert "/opt/airflow" not in mount["source"]
        if target != "/canary/dags/solver_canary_dag.py":
            assert mount["source"].startswith("${CLUBELO_CANARY_") and ":?" in mount["source"]
    assert "SELECTED_OBSERVER_DIR" in mounts[OBSERVER_DIR]["source"]
    env = runner["environment"]
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert env["AIRFLOW_HOME"] == "/canary/airflow"
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == "sqlite:////canary/airflow/airflow.db"
    assert env["AIRFLOW__CORE__EXECUTOR"] == "SequentialExecutor"
    assert env["AIRFLOW__CORE__DAGS_FOLDER"] == "/canary/dags"
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"
    assert env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "true"
    assert env["AIRFLOW__LOGGING__BASE_LOG_FOLDER"] == "/canary/airflow/logs"
    assert env["AIRFLOW__LOGGING__REMOTE_LOGGING"] == "false"
    assert env["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] == "false"
    assert env["PYTHONPATH"] == "/canary/code"
    assert {key: value for key, value in env.items() if key.startswith("CLUBELO_CANARY_")} == {
        "CLUBELO_CANARY_ARM": "${CLUBELO_CANARY_ARM:?Required selected arm}",
        "CLUBELO_CANARY_RUN_ID": "${CLUBELO_CANARY_RUN_ID:?Required unique real DagRun id}",
    }
    assert any(value.startswith("/canary/airflow:") for value in runner["tmpfs"])
    assert env["CLUBELO_PROXY_CONTROL_URL"] == "http://clubelo-canary-gateway:8899"
    assert env["CLUBELO_PROXY_MAX_BYTES"] == "33554432"
    assert env["CLUBELO_PROXY_TTL_SECONDS"] == "3600"
    assert env["CLUBELO_PROXY_CONTROL_TOKEN_FILE"] == "/canary/secrets/control_token"
    assert "CLUBELO_PROXY_CONTROL_TOKEN" not in env
    assert "CLUBELO_FLARESOLVERR_URL" not in env
    assert "CLUBELO_CANARY_COUNTER_CONTRACT" not in env
    assert runner["entrypoint"] == ["python", "-m", "scripts.research.clubelo_solver_canary", "airflow-task"]


def test_gateway_has_exact_release_contract_and_private_state(compose):
    gateway = compose["services"]["clubelo-canary-gateway"]
    assert "env_file" not in gateway
    assert gateway["entrypoint"] == ["/usr/bin/dumb-init", "--", "/entrypoint"]
    assert gateway["working_dir"] == "/canary/gateway"
    env = gateway["environment"]
    assert env == {
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": "/canary/gateway",
        "CONNECTION_CHECK_MAX_COUNT": "0",
        "PROXY_POOL_JSON": "${PROXY_POOL_JSON:?Required dedicated canary residential proxy pool}",
        "PROXY_FILTER_CONTROL_TOKEN": "${PROXY_FILTER_CONTROL_TOKEN:?Required distinct control token}",
        "CLUBELO_PROXY_CONTROL_TOKEN": "${CLUBELO_PROXY_CONTROL_TOKEN:?Required ClubElo control token}",
        "PROXY_FILTER_CLUBELO_DAGRUN_BUDGET_BYTES": "33554432",
        "PROXY_FILTER_CLUBELO_HISTORY_DAGRUN_BUDGET_BYTES": "33554432",
        "PROXY_FILTER_ALLOW_FILE_FALLBACK": "false",
    }
    assert gateway["command"] == [
        "python", "/canary/gateway/scripts/fbref_proxy/filter_proxy.py",
        "--source-mode", "shared-no-whoscored",
        "--listen", "0.0.0.0:8899", "--lease-listen", "0.0.0.0:8900",
        "--lease-proxy-url", "http://clubelo-canary-gateway:8900",
        "--daily-budget-mb", "2048", "--max-lease-mb", "32",
        "--url-budget-bytes", "33554432", "--max-lease-ttl-seconds", "3600",
        "--max-active-leases", "1", "--out", "/canary/state/bytes.json",
        "--ledger", "/canary/state/paid_requests.jsonl",
    ]
    mounts = {mount["target"]: mount for mount in gateway["volumes"]}
    assert set(mounts) == {"/canary/gateway", "/canary/state"}
    assert mounts["/canary/gateway"]["read_only"]
    assert mounts["/canary/gateway"]["source"].startswith("${CLUBELO_RELEASE_ROOT:?")
    assert mounts["/canary/state"]["source"].startswith("${CLUBELO_GATEWAY_STATE_HOST_DIR:?")
    assert not mounts["/canary/state"].get("read_only", False)
    # Static checks cannot establish provider quota or actual image provenance.


def test_no_production_paths_or_context_assignments():
    source = COMPOSE.read_text()
    for forbidden in ("/var/run/docker", "dp-backend", "seaweed", "lakekeeper",
                      "auto-deliver", "clubelo-runtime", "clubelo-deliveries", "AIRFLOW_CTX_"):
        assert forbidden not in source
    for service in yaml.safe_load(source)["services"].values():
        assert not any(key.startswith("AIRFLOW_CTX_") for key in service.get("environment", {}))
    for path in (SCRIPT, DAG):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    text = ast.unparse(target)
                    assert "AIRFLOW_CTX_" not in text
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"setenv", "putenv"}:
                assert not any(isinstance(arg, ast.Constant) and str(arg.value).startswith("AIRFLOW_CTX_") for arg in node.args)
    assert not re.search(r"Bearer\s+[A-Za-z0-9_-]{16,}", source)


def test_single_task_dag_static_contract():
    tree = ast.parse(DAG.read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    dags = [node for node in calls if isinstance(node.func, ast.Name) and node.func.id == "DAG"]
    operators = [node for node in calls if isinstance(node.func, ast.Name) and node.func.id.endswith("Operator")]
    assert len(dags) == 1 and len(operators) == 1
    dag = {item.arg: item.value for item in dags[0].keywords}
    task = {item.arg: item.value for item in operators[0].keywords}
    assert ast.literal_eval(dag["dag_id"]) == "dag_ingest_clubelo"
    assert ast.literal_eval(dag["schedule"]) is None
    assert ast.literal_eval(dag["catchup"]) is False
    assert ast.literal_eval(task["task_id"]) == "check_watch"
    assert ast.literal_eval(task["retries"]) == 0
    assert ast.literal_eval(task["do_xcom_push"]) is False
    assert ast.literal_eval(task["append_env"]) is True
    assert ast.literal_eval(task["bash_command"]) == "exec python -m scripts.research.clubelo_solver_canary run-once"
    assert "env" not in task
    assert "AIRFLOW_CTX_" not in DAG.read_text()
    assert DAG.parent == ROOT / "deploy/clubelo"


def test_airflow_launcher_uses_real_dagrun_and_supported_task_test():
    tree = ast.parse(SCRIPT.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "airflow_task")
    calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)]
    assert any(isinstance(node.func, ast.Attribute) and node.func.attr == "create_dagrun" for node in calls)
    commands = [node for node in calls if isinstance(node.func, ast.Attribute)
                and ast.unparse(node.func) == "subprocess.run"]
    assert len(commands) == 1
    command = commands[0].args[0]
    literals = [item.value for item in command.elts if isinstance(item, ast.Constant)]
    assert literals[:5] == ["-m", "airflow", "tasks", "test", "dag_ingest_clubelo"]
    assert "check_watch" in literals
    assert "--subdir" in literals
    assert not any(keyword.arg == "shell" for keyword in commands[0].keywords)



def test_solver_entrypoint_environment_and_production_tmpfs(compose):
    required_tmpfs = {
        "/tmp:rw,exec,nosuid,nodev,size=2g,uid=1000,gid=1000,mode=1770",
        "/config:rw,noexec,nosuid,nodev,size=16m,uid=1000,gid=1000,mode=0700",
        "/app/.config:rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700",
        "/app/.local:rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700",
        "/home/seluser:rw,nosuid,nodev,size=128m",
    }
    for arm in ("control", "candidate"):
        solver = compose["services"]["solver-" + arm]
        env = solver["environment"]
        assert env["CLUBELO_CANARY_UPSTREAM"] == arm
        assert required_tmpfs == set(solver["tmpfs"])
        assert solver["shm_size"] == "512m"


def test_airflow_launcher_uses_execution_date_not_run_id():
    tree = ast.parse(SCRIPT.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "airflow_task")
    commands = [node for node in ast.walk(function)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == "subprocess.run"]
    assert len(commands) == 1
    argv = commands[0].args[0].elts
    assert any(isinstance(item, ast.Call) and ast.unparse(item) == "now.isoformat()" for item in argv)
    assert not any(isinstance(item, ast.Name) and item.id == "run_id" for item in argv[3:4])


def test_compose_has_no_browser_budget_cdp_or_revision_contract():
    text = COMPOSE.read_text().lower()
    for forbidden in (
        "browserbudgetgate", "browser_budget", "clubelo-canary-budget",
        "budget.json", "counter.json", "budget_path", "counter_path",
        "budget_state_dir", "_budget_dir", "armed_revision",
        "clubelo_canary_revision", "counter_contract", "allowed_admitted",
        "admitted_requests", "blocked_requests", "cdp", "devtools",
        "10000", "asset-cap",
    ):
        assert forbidden not in text


def test_compose_preserves_observer_lifecycle_and_remaining_limits():
    text = COMPOSE.read_text()
    for required in (
        "active_run.json", "sys.modules", "owner-only",
        "BEFORE", "AFTER a complete atomic report", "requests/cleanup",
        "30 days", "60 runs/arm", "240 attempts total",
        "2 bootstrap starts/minute", "2 GiB provider bytes total",
        "30 detected challenges, 40 attempts and 20 runs",
        "3 attempts/run", "10s retry pause", "90s challenge", "120s client",
        "independent provider-side 2048 MiB daily HARD cap",
        "otherwise fail closed",
    ):
        assert required in text


def test_plan_removes_additional_asset_cap_by_owner_decision():
    text = PLAN.read_text()
    assert "Владелец явно выбрал «Убрать asset-cap»" in text
    assert "при потере CDP Chrome возобновляет" in text
    assert "приостановленные запросы" in text
    assert "без опционального режима или обещания совместимости" in text
    assert "Новая метрика запросов\nвзамен не вводится" in text
    for forbidden in (
        "BrowserBudgetGate", "browser_budget.py", "clubelo-canary-budget",
        "CLUBELO_CANARY_REVISION", "budget.json", "counter.json",
        "allowed_admitted", "admitted_requests", "blocked_requests",
    ):
        assert forbidden not in text
    # The statistical bootstrap count remains; it is not a traffic ceiling.
    assert text.count("10000") == 1
    assert "10000 реплик, seed 2251" in text


def test_plan_preserves_minima_byte_attempt_run_rate_and_time_limits():
    text = " ".join(PLAN.read_text().split())
    for required in (
        "Минимумы НА ПЛЕЧО: 30 detected, 40 попыток, 20 рунов",
        "30 календарных дней; 60 рунов на плечо (120 всего)",
        "240 начатых bootstrap-попыток всего; 2 старта bootstrap в минуту",
        "2 GiB трафика провайдера всего",
        "независимый жёсткий суточный лимит на стороне выделенного провайдера — 2048 MiB",
        "Суточный сброс не сбрасывает общий предел эксперимента",
        "3 попытки, паузу 10 с, таймауты 90 с (challenge) / 120 с (клиент)",
        "Более строгие существующие лимиты lease/gateway имеют приоритет",
        "Если хотя бы один предел нельзя измерить и контролировать — план НЕ ГОТОВ",
        "автоматического продления и дополнительного трафика нет",
    ):
        assert required in text


def test_plan_preserves_success_other_source_gates_and_rollback():
    text = " ".join(PLAN.read_text().split())
    for required in (
        "S/D кандидата ≥ 90% и разница кандидат − контроль ≥ 20 п.п.",
        "нижняя односторонняя 95%-я граница для разницы > 0",
        "кластерный по рунам бутстреп", "10000 реплик, seed 2251",
        "Если контроль восстановился и преимущества кандидата нет — НЕ переключать",
        "(validated ∩ solved)/S ≥ 95%",
        "V/A и доля validated-clearance на рун — каждое не более чем на 5 п.п. ниже контроля",
        "validated-clearance на рун у кандидата ≥ 85%",
        "0 невалидных решений, переданных в запросы",
        "p95 (nearest-rank) по всем попыткам ≤ 120 с и ≤ 1.20× контроля (неудачи включены)",
        "p95 (nearest-rank) успешной валидации ≤ 100 с",
        "WhoScored и SoFIFA",
        "0 запросов/сессий других источников к кандидату",
        "0 изменений digest/конфигурации общего стека",
        "0 новых OOM/рестартов общего стека",
        "≥ 30 запросов в каждом окне; рост доли ошибок ≤ 2 п.п.",
        "p95 задержки ≤ 1.10× базовой линии",
        "недостаточность выборки — inconclusive",
        "5 последовательных рунов кандидата без validated clearance",
        "прекратить новые назначения кандидата",
        "восстановить заранее сохранённую ClubElo-маршрутизацию на контроль и сверить контрольную сумму",
        "закрыть сессии/lease кандидата",
        "сохранить санитизированные свидетельства",
        "остановить/удалить только изолированного кандидата",
        "проверить неизменность идентичностей общего контейнера/образа/конфигурации",
        "Сам откат дополнительно требует отдельного одобрения и окна владельца",
    ):
        assert required in text
