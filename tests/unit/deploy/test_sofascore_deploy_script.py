"""deploy/sofascore/deploy.sh против заглушек docker/systemctl (#1155, этап 3).

Скрипт выката гоняется целиком: заглушка `docker` пишет каждый вызов и то, какие
SOFASCORE_* переменные видит compose в окружении; заглушка метабазы держит состояние
паузы DAG-ов. Ловит два дефекта первого ревью: устаревшее значение из окружения
процесса перекрывало перепинованный env-файл, и актуалка не ставилась на паузу.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "sofascore"
DIGEST = "0123456789abcdef" * 4
TAG = DIGEST[:8]
HIST = "dag_backfill_sofascore_all_mens"
REFRESH = "dag_refresh_sofascore_all_mens"


def _write(path: Path, text: str, mode: int | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    if mode is not None:
        path.chmod(mode)
    return path


def _stubs(bin_dir: Path, state_dir: Path) -> None:
    _write(
        bin_dir / "docker",
        f'''\
        #!/usr/bin/env bash
        STATE="{state_dir}"
        printf '%s\\t%s\\t%s\\n' "$*" "${{SOFASCORE_RELEASE_ROOT-<unset>}}" \\
          "${{SOFASCORE_PROXY_BUDGET_ARTIFACT_ID-<unset>}}" >> "$STATE/calls.log"
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-metadb ]; then
          sql="${{@: -1}}"
          case "$sql" in
            *"UPDATE dag SET is_paused=false"*)
              dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}
              echo f > "$STATE/paused_$dag"; echo "UPDATE 1" ;;
            *"SELECT is_paused"*)
              dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}
              cat "$STATE/paused_$dag" ;;
            *"is_active=true"*) cat "$STATE/active_count" 2>/dev/null || echo 3 ;;
            # run_id прогона истории, который держит слот пула ('-' — такого нет)
            *"coalesce((SELECT run_id"*) cat "$STATE/hist_run" 2>/dev/null || echo - ;;
            # Четыре числа шага drain. Третье считается ЧЕСТНО: отслеживаемым признаётся
            # только тот run_id, который скрипт спросил, — иначе тест не отличил бы возврат
            # к старому «считаем все прогоны истории».
            *"run_id="*)
              want=${{sql#*run_id=\\'}}; want=${{want%%\\'*}}
              hist=0
              if [ -n "$want" ] && [ "$want" = "$(cat "$STATE/hist_run" 2>/dev/null)" ] \\
                 && [ -e "$STATE/hist_run_active" ]; then hist=1; fi
              printf '%s|%s|%s|%s\\n' "$(cat "$STATE/active_dr" 2>/dev/null || echo 0)" \\
                "$(cat "$STATE/busy" 2>/dev/null || echo 0)" "$hist" \\
                "$(cat "$STATE/hist_busy" 2>/dev/null || echo 0)" ;;
            # Прогон актуалки, замёрзший под паузой: планировщик его не двигает, поэтому
            # ожидание, которое считает ЕГО, не кончится никогда.
            *dag_run*"'dag_refresh_sofascore_all_mens'"*)
              cat "$STATE/active_refresh" 2>/dev/null || cat "$STATE/active" 2>/dev/null || echo 0 ;;
            *task_instance*"state IN ("*) cat "$STATE/busy" 2>/dev/null || echo 0 ;;
            *dag_run*"state IN ("*) cat "$STATE/active" 2>/dev/null || echo 0 ;;
            *"count(*)"*) echo 0 ;;
            *) echo "unexpected sql: $sql" >&2; exit 9 ;;
          esac
          exit 0
        fi
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-scheduler ] && [ "$3" = python ]; then
          # close_stale_runs: висящий dag_run закрыт ORM-ом внутри планировщика
          rm -f "$STATE/active"
          exit 0
        fi
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-scheduler ] && [ "$3" = airflow ]; then
          # @continuous успевает создать новый dag_run между «контур свободен» и паузой;
          # под паузой он замерзает и сам никогда не закроется.
          [ "$5" = pause ] && [ -e "$STATE/stale_on_pause_$6" ] && echo 1 > "$STATE/active"
          # scheduler-down simulation: `airflow dags unpause` fails once the flag exists
          [ "$5" = unpause ] && [ -e "$STATE/scheduler_down" ] && exit 1
          case "$5" in pause) echo t > "$STATE/paused_$6" ;; unpause) echo f > "$STATE/paused_$6" ;; esac
          exit 0
        fi
        if [ "$1" = inspect ]; then
          case "$*" in
            *Health.Status*) cat "$STATE/health" 2>/dev/null || echo healthy ;;
            *HostConfig.Memory*) echo 1073741824 ;;
          esac
          exit 0
        fi
        exit 0
        ''',
        0o755,
    )
    _write(
        bin_dir / "systemctl",
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "{state_dir}/systemctl.log"\necho active\n',
        0o755,
    )
    _write(bin_dir / "chown", "#!/usr/bin/env bash\nexit 0\n", 0o755)
    # The script polls with sleep 10/60; the stub makes failure paths finish instantly.
    _write(bin_dir / "sleep", "#!/usr/bin/env bash\nexit 0\n", 0o755)
    # Часы: `+%s` двигаются на 60 с за вызов, но только когда тест положил файл `clock` —
    # так проверяется, что потолок ожидания считается по ЧАСАМ, а не по сумме sleep.
    # Без этого файла время стоит, и остальные тесты остаются детерминированными.
    _write(
        bin_dir / "date",
        f"""\
        #!/usr/bin/env bash
        STATE="{state_dir}"
        case "$*" in
          *%H%M*) echo 0000 ;;
          *%s*)
            if [ -e "$STATE/clock" ]; then
              n=$(cat "$STATE/clock"); n=$(( n + 60 )); echo "$n" > "$STATE/clock"; echo "$n"
            else
              echo 1767225600
            fi ;;
          *) echo 2026-01-01T00:00:00Z ;;
        esac
        """,
        0o755,
    )
    _write(
        bin_dir / "host-python",
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "{state_dir}/host-python.log"\n'
        f'case "$*" in *runtime_fingerprint*) echo {DIGEST} ;; esac\nexit 0\n',
        0o755,
    )


def _layout(tmp_path: Path, *, refresh_paused: str) -> tuple[Path, Path, Path, Path]:
    runtime = tmp_path / "runtime"
    release = tmp_path / "releases" / f"release-{TAG}-abcdef12"
    state_dir = tmp_path / "stub-state"
    bin_dir = tmp_path / "bin"
    state_dir.mkdir()
    for lane in ("gateway-state", "gateway-state-history", "gateway-state-players"):
        (runtime / lane).mkdir(parents=True)
    _stubs(bin_dir, state_dir)
    (state_dir / f"paused_{HIST}").write_text("f\n")
    (state_dir / f"paused_{REFRESH}").write_text(f"{refresh_paused}\n")
    for name in ("airflow.compose.yaml", "gateway.compose.yaml"):
        _write(release / "deploy" / "sofascore" / name, "services: {}\n")
    _write(runtime / "all-men" / "snapshot.json", "{}\n")
    # #1245: the budget artifact is the static policy shipped in the release
    # tree; there is no canary workspace and no VERIFIED gate any more.
    _write(
        release / "configs" / "sofascore" / "workload_policy.json",
        json.dumps({"schema_version": 4, "source": "sofascore"}),
    )
    platform_env = _write(tmp_path / "platform.env", "TRINO_PORT=8443\n")
    env_file = _write(
        tmp_path / "sofascore.env",
        f"""\
        # контур
        SOFASCORE_RUNTIME_DIR={runtime}
        SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR={runtime}/all-men
        SOFASCORE_GATEWAY_STATE_HOST_DIR={runtime}/gateway-state
        SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state-history
        SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/gateway-state-players
        SOFASCORE_PLATFORM_ENV_FILE={platform_env}
        SOFASCORE_HOST_PYTHON={bin_dir}/host-python
        SOFASCORE_RELEASE_ROOT=/old/release-deadbeef
        SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST=/old/artifact.json
        SOFASCORE_PROXY_BUDGET_ARTIFACT_ID={"f" * 64}
        SOFASCORE_PROXY_POOL_JSON='[{{"host":"pool","port":1,"username":"u","password":"p"}}]'
        """,
    )
    return runtime, release, env_file, state_dir


def _run_deploy(tmp_path: Path, *, refresh_paused: str) -> tuple[subprocess.CompletedProcess, Path, Path, Path]:
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused=refresh_paused)
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        # The operator's shell may still carry the previous rotation's values:
        # the process environment outranks --env-file for docker compose.
        "SOFASCORE_RELEASE_ROOT": "/stale/from-operator-shell",
        "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID": "e" * 64,
    }
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return proc, release, env_file, state_dir


def _calls(state_dir: Path) -> list[tuple[str, str, str]]:
    return [
        tuple(line.split("\t"))
        for line in (state_dir / "calls.log").read_text(encoding="utf-8").splitlines()
    ]


@pytest.mark.unit
def test_deploy_passes_the_new_release_to_compose_even_with_a_stale_shell_environment(tmp_path: Path) -> None:
    proc, release, env_file, state_dir = _run_deploy(tmp_path, refresh_paused="f")
    assert proc.returncode == 0, proc.stdout + proc.stderr

    artifact_host = tmp_path / "runtime" / "artifacts" / TAG / "workload_policy.json"
    assert artifact_host.is_file()
    artifact_id = subprocess.run(
        ["sha256sum", str(artifact_host)], capture_output=True, text=True, check=True
    ).stdout.split()[0]
    env_text = env_file.read_text(encoding="utf-8")
    assert f"SOFASCORE_RELEASE_ROOT={release}\n" in env_text
    assert f"SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST={artifact_host}\n" in env_text
    assert f"SOFASCORE_PROXY_BUDGET_ARTIFACT_ID={artifact_id}\n" in env_text
    assert "SOFASCORE_PROXY_POOL_JSON='[{" in env_text, "untouched lines survive the repin"

    compose_calls = [call for call in _calls(state_dir) if call[0].startswith("compose ")]
    assert len(compose_calls) == 2
    for args, seen_root, seen_id in compose_calls:
        assert seen_root == str(release), args
        assert seen_id == artifact_id, args
        assert f"--env-file {env_file}" in args
        assert "--no-deps --force-recreate" in args
    assert f"-f {release}/deploy/sofascore/airflow.compose.yaml" in compose_calls[0][0]
    assert f"--project-directory {release}" in compose_calls[1][0]
    # Три полосы (#1244): три шлюза одним вызовом, три пула, три сторожа.
    assert compose_calls[1][0].endswith(
        "sofascore_proxy_filter sofascore_gw_history sofascore_gw_players"
    ), compose_calls[1][0]
    args = [call[0] for call in _calls(state_dir)]
    pools = [a for a in args if a.startswith("exec sofascore-airflow-scheduler airflow pools set ")]
    first_compose = min(i for i, a in enumerate(args) if a.startswith("compose "))
    last_compose = max(i for i, a in enumerate(args) if a.startswith("compose "))
    # Шаг drain закрывает полосу истории ДО пересоздания: пул, а не пауза, не даёт стартовать
    # новому скоупу, пока хвост уже начатого досчитывается.
    assert pools[0].split()[5:7] == ["sofascore_history_pool", "0"], pools
    assert args.index(pools[0]) < first_compose, pools
    restored = pools[1:]
    # deploy.sh не пересоздаёт airflow-init, где пулы заводятся впервые: без этого
    # шага задачи полос повисли бы в несуществующем пуле.
    assert [a.split()[5] for a in restored] == [
        "ingest_scraper_pool", "sofascore_history_pool", "sofascore_players_pool"
    ], pools
    assert all(a.split()[6] == "1" for a in restored), pools
    assert min(args.index(a) for a in restored) > last_compose
    preflights = [
        line for line in (state_dir / "host-python.log").read_text(encoding="utf-8").splitlines()
        if " preflight " in line
    ]
    # Полный валидатор (каноничность, владелец, доступ UID 50000, вне дерева релиза)
    # обязан пройти по КАЖДОЙ полосе, а не только по каталогу актуалки.
    runtime = tmp_path / "runtime"
    assert sorted(line.split("--state-dir ")[1].split(" ")[0] for line in preflights) == sorted(
        [
            f"{runtime}/gateway-state",
            f"{runtime}/gateway-state-history",
            f"{runtime}/gateway-state-players",
        ]
    ), preflights
    restarts = [
        line.split(" ", 1)[1]
        for line in (state_dir / "systemctl.log").read_text(encoding="utf-8").splitlines()
        if line.startswith("restart ")
    ]
    assert restarts == [
        "sofascore-gw-lease-watchdog.service",
        "sofascore-gw-lease-watchdog-history.service",
        "sofascore-gw-lease-watchdog-players.service",
    ], restarts


@pytest.mark.unit
@pytest.mark.parametrize(
    "missing",
    ["SOFASCORE_HISTORY_GW_STATE_HOST_DIR", "SOFASCORE_PLAYERS_GW_STATE_HOST_DIR"],
)
def test_deploy_refuses_before_touching_anything_when_a_lane_state_dir_is_unset(
    tmp_path: Path, missing: str
) -> None:
    # Каталог состояния полосы — fail-closed вход compose. Без проверки в начале
    # скрипта пропущенная переменная валила бы выкат только на gateway-up: уже
    # после паузы кампаний, перепиновки env-файла и пересоздания scheduler'а.
    _runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    env_file.write_text(
        "\n".join(
            line for line in env_file.read_text(encoding="utf-8").splitlines()
            if not line.startswith(f"{missing}=")
        )
        + "\n",
        encoding="utf-8",
    )
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}", "SOFASCORE_ENV_FILE": str(env_file)}
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode != 0, proc.stdout
    assert missing in proc.stderr, proc.stderr
    assert not (state_dir / "calls.log").exists(), "ни одного вызова docker до отказа"


def _symlink_players_state_onto_history(text: str, runtime: Path) -> str:
    """Разные строки — один каталог: сравнение строк такое не ловит."""
    alias = runtime / "gateway-state-players-alias"
    (runtime / "gateway-state-players").rmdir()
    alias.symlink_to(runtime / "gateway-state-history")
    return text.replace(
        f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/gateway-state-players",
        f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={alias}",
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("break_env", "reason"),
    [
        (
            lambda text, runtime: text.replace(
                f"SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state-history",
                f"SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state",
            ),
            "указывают на один каталог",
        ),
        (
            lambda text, runtime: text.replace(
                f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/gateway-state-players",
                f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/nowhere",
            ),
            "не существует",
        ),
        (
            _symlink_players_state_onto_history,
            "указывают на один каталог",
        ),
    ],
    ids=[
        "two-lanes-share-one-state-dir",
        "lane-state-dir-does-not-exist",
        "lane-state-dir-is-a-symlink-to-another-lane",
    ],
)
def test_deploy_refuses_a_state_layout_that_would_give_a_wal_two_writers(
    tmp_path: Path, break_env, reason: str
) -> None:
    # Каталог состояния — не просто строка в env: два одинаковых пути дали бы двух
    # писателей на один WAL, а приёмка бы это пропустила (ожидания она берёт из того
    # же файла). Отказ обязан случиться до паузы кампаний и любого вызова docker.
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    env_file.write_text(break_env(env_file.read_text(encoding="utf-8"), runtime), encoding="utf-8")
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}", "SOFASCORE_ENV_FILE": str(env_file)}
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert reason in proc.stderr, proc.stderr
    assert not (state_dir / "calls.log").exists(), "ни одного вызова docker до отказа"


@pytest.mark.unit
def test_deploy_no_longer_gates_on_a_paid_canary_or_a_tree_digest(tmp_path: Path) -> None:
    """#1245: no VERIFIED file, no candidate.json, no runtime-fingerprint compare."""

    proc, _release, _env_file, state_dir = _run_deploy(tmp_path, refresh_paused="f")
    assert proc.returncode == 0, proc.stdout + proc.stderr

    host_python_log = state_dir.parent / "stub-state" / "host-python.log"
    calls = (
        host_python_log.read_text(encoding="utf-8")
        if host_python_log.exists()
        else ""
    )
    assert "runtime_fingerprint" not in calls
    assert not (tmp_path / "runtime" / f"canary-{TAG}").exists()
    script = (DEPLOY / "deploy.sh").read_text(encoding="utf-8")
    assert "VERIFIED" not in script
    assert "candidate.json" not in script


@pytest.mark.unit
def test_deploy_refuses_a_release_tree_without_the_static_workload_policy(
    tmp_path: Path,
) -> None:
    runtime, release, env_file, _state_dir = _layout(tmp_path, refresh_paused="f")
    (release / "configs" / "sofascore" / "workload_policy.json").unlink()

    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)],
        env={
            **os.environ,
            "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
            "SOFASCORE_ENV_FILE": str(env_file),
        },
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "workload_policy.json" in proc.stderr
    assert f"SOFASCORE_RELEASE_ROOT={release}" not in env_file.read_text(
        encoding="utf-8"
    )
    assert not (runtime / "artifacts").exists()


@pytest.mark.unit
def test_deploy_pauses_both_campaigns_before_recreating_and_restores_refresh(tmp_path: Path) -> None:
    proc, _release, _env_file, state_dir = _run_deploy(tmp_path, refresh_paused="f")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    first_compose = next(i for i, a in enumerate(args) if a.startswith("compose "))
    pause_hist = args.index(f"exec sofascore-airflow-scheduler airflow dags pause {HIST}")
    pause_refresh = args.index(f"exec sofascore-airflow-scheduler airflow dags pause {REFRESH}")
    assert pause_hist < first_compose and pause_refresh < first_compose
    assert any(f"'{REFRESH}'" in a and "task_instance" in a for a in args), "idle wait must cover refresh"
    unpause = args.index(f"exec sofascore-airflow-scheduler airflow dags unpause {REFRESH}")
    assert unpause > first_compose
    assert (state_dir / f"paused_{REFRESH}").read_text().strip() == "f"
    assert (state_dir / f"paused_{HIST}").read_text().strip() == "t"
    assert f"unpause {HIST}" not in "\n".join(args)


@pytest.mark.unit
def test_deploy_keeps_refresh_paused_when_it_was_paused_before(tmp_path: Path) -> None:
    proc, _release, _env_file, state_dir = _run_deploy(tmp_path, refresh_paused="t")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = "\n".join(call[0] for call in _calls(state_dir))
    assert f"unpause {REFRESH}" not in args
    assert (state_dir / f"paused_{REFRESH}").read_text().strip() == "t"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("break_state", "expected_rc", "expected_step"),
    [
        ({"health": "unhealthy"}, 5, "gateway-health"),
        ({"active_count": "2"}, 6, "scheduler-health"),
        ({"health": "unhealthy", "scheduler_down": ""}, 5, "gateway-health"),
    ],
    ids=["gateway-unhealthy", "core-dags-missing", "scheduler-down-unpause-via-metadb"],
)
def test_deploy_restores_refresh_and_names_the_step_when_a_late_step_fails(
    tmp_path: Path, break_state: dict, expected_rc: int, expected_step: str
) -> None:
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    for name, value in break_state.items():
        (state_dir / name).write_text(value + "\n")
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}", "SOFASCORE_ENV_FILE": str(env_file)}
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == expected_rc, proc.stdout + proc.stderr
    log = (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")
    assert f"FAILED at step '{expected_step}'" in log
    # The refresh campaign was unpaused before the rotation; a failed rotation
    # must not leave it paused.
    assert (state_dir / f"paused_{REFRESH}").read_text().strip() == "f"
    args = [call[0] for call in _calls(state_dir)]
    assert args.index(f"exec sofascore-airflow-scheduler airflow dags unpause {REFRESH}") > max(
        i for i, a in enumerate(args) if a.startswith("compose ")
    )
    via_metadb = any("UPDATE dag SET is_paused=false" in a for a in args)
    assert via_metadb == ("scheduler_down" in break_state)
    assert "MANUAL ACTION REQUIRED" not in log
    # Шаг pools стоит ПОСЛЕ gateway-health и scheduler-health: до него выкат не дошёл,
    # и полоса истории осталась бы с нулём слотов, если бы её не вернул on_exit.
    pools = [
        a.split()[5:7] for a in args
        if a.startswith("exec sofascore-airflow-scheduler airflow pools set ")
    ]
    assert pools[0] == ["sofascore_history_pool", "0"], pools
    assert pools[-1] == ["sofascore_history_pool", "1"], pools
    assert f"sofascore_history_pool restored to 1 slots" in log


@pytest.mark.unit
def test_set_env_var_rewrites_only_its_own_line_and_fails_on_a_missing_key(tmp_path: Path) -> None:
    """Перепин живёт в общем загрузчике: тот же `sed` делает откат автомата (#1245).

    Две копии одной правки рано или поздно разъехались бы, и откат перепинывал бы не
    то, что перепинул выкат."""
    env_file = _write(
        tmp_path / "sofascore.env",
        """\
        # шапка
        SOFASCORE_RELEASE_ROOT=/old/release-deadbeef
        SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=old
        SOFASCORE_PROXY_POOL_JSON='[{"host":"pool","port":1}]'
        """,
    )
    script = f"""\
        . {DEPLOY}/env.sh
        sofascore_set_env_var {env_file} SOFASCORE_RELEASE_ROOT /new/release-cafebabe || exit 8
        sofascore_set_env_var {env_file} SOFASCORE_MISSING x; echo "rc=$?"
        """
    proc = subprocess.run(
        ["bash", "-c", textwrap.dedent(script)], capture_output=True, text=True, check=True
    )
    assert "rc=2" in proc.stdout, proc.stdout + proc.stderr
    assert "нет строки SOFASCORE_MISSING=" in proc.stderr
    assert env_file.read_text(encoding="utf-8").splitlines() == [
        "# шапка",
        "SOFASCORE_RELEASE_ROOT=/new/release-cafebabe",
        "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=old",
        """SOFASCORE_PROXY_POOL_JSON='[{"host":"pool","port":1}]'""",
    ]


@pytest.mark.unit
def test_deploy_gives_up_honestly_when_the_contour_never_goes_idle(tmp_path: Path) -> None:
    """Потолок ожидания. Раньше `while true` без таймаута висел ВЕЧНО: история идёт
    @continuous, а под паузой её хвост не выполняется вовсе и dag_run не двигается.
    Теперь исчерпанный потолок — код 4 «контур занят, выкат не начат», и он обязан быть
    честным: ни одного compose, env не перепинован, артефакт не создан, а осушённый пул
    и пауза истории возвращены как были."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (state_dir / "busy").write_text("1\n")
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "SOFASCORE_DEPLOY_IDLE_WAIT": "0",
    }
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 4, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    assert not [a for a in args if a.startswith("compose ")], args
    assert f"SOFASCORE_RELEASE_ROOT={release}" not in env_file.read_text(encoding="utf-8")
    assert not (runtime / "artifacts").exists()
    pools = [
        a.split()[5:7] for a in args
        if a.startswith("exec sofascore-airflow-scheduler airflow pools set ")
    ]
    assert pools == [["sofascore_history_pool", "0"], ["sofascore_history_pool", "1"]], pools
    # Контур занят — значит НИЧЕГО не изменилось: обе кампании работают дальше.
    assert (state_dir / f"paused_{REFRESH}").read_text().strip() == "f"
    assert (state_dir / f"paused_{HIST}").read_text().strip() == "f"
    log = (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")
    assert "FAILED at step 'drain'" in log
    assert "nothing deployed" in log


@pytest.mark.unit
def test_the_idle_ceiling_counts_wall_clock_not_the_sum_of_sleeps(tmp_path: Path) -> None:
    """Ревью Sol, раунд 1. Каждый виток ожидания делает ДВА запроса к метабазе с таймаутом
    до SOFASCORE_DEPLOY_METADB_TIMEOUT секунд каждый. Пока потолок уменьшался «на 30 за
    виток», на недоступной метабазе 5400 с превращались почти в 4,5 часа — то есть выкат
    всё равно съедал бы всё окно ночной доставки. Часы идут по 60 с за обращение: при
    потолке 100 с честный счёт даёт не больше двух витков, счёт по sleep дал бы четыре."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (state_dir / "busy").write_text("1\n")
    (state_dir / "clock").write_text("1767225600\n")
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "SOFASCORE_DEPLOY_IDLE_WAIT": "100",
    }
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 4, proc.stdout + proc.stderr
    waits = [a for a, *_ in _calls(state_dir) if "task_instance" in a]
    assert 1 <= len(waits) <= 2, waits
    log = (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")
    assert "contour still busy after 100s" in log


@pytest.mark.unit
def test_deploy_closes_the_history_run_that_froze_under_the_pause(tmp_path: Path) -> None:
    """@continuous успевает создать новый dag_run между «контур свободен» и паузой.
    Под паузой планировщик его не рассматривает вовсе (next_dagruns_to_examine требует
    is_paused == false, а dagrun_timeout проверяется только в _schedule_dag_run), поэтому
    он висит НАВСЕГДА. Закрывает его ORM внутри планировщика — и только у истории:
    дейли выкатом не паузится, его прогон живой, закрывать чужой прогон — порча боя."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (state_dir / f"stale_on_pause_{HIST}").write_text("1\n")
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}", "SOFASCORE_ENV_FILE": str(env_file)}
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    closers = [a for a in args if a.startswith("exec sofascore-airflow-scheduler python -c ")]
    assert len(closers) == 1, closers
    assert "DagRunState.FAILED" in closers[0], closers
    assert closers[0].endswith(f" {HIST}"), closers
    assert REFRESH not in closers[0] and "dag_ingest_sofascore" not in closers[0], closers
    # Закрытие идёт ПОСЛЕ паузы истории и ДО пересоздания контейнеров.
    assert args.index(f"exec sofascore-airflow-scheduler airflow dags pause {HIST}") < args.index(closers[0])
    assert args.index(closers[0]) < min(i for i, a in enumerate(args) if a.startswith("compose "))


@pytest.mark.unit
def test_deploy_drains_the_pool_before_pausing_history_and_never_unpauses_it(tmp_path: Path) -> None:
    """Порядок — единственное, что бережёт оплаченный скоуп. Пауза истории ДО ожидания
    не даёт выполниться validate_historical_scope, а он единственный засчитывает скоуп в
    state.json: следующий прогон получил бы новый run_id и купил те же 8–81 минуты
    платного трафика заново. Поэтому дверь новым скоупам закрывает ПУЛ, а пауза приходит
    после ожидания; распаузивать историю штатный выкат по-прежнему не вправе."""
    proc, _release, _env_file, state_dir = _run_deploy(tmp_path, refresh_paused="f")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    drain = next(
        i for i, a in enumerate(args)
        if a.startswith("exec sofascore-airflow-scheduler airflow pools set sofascore_history_pool 0")
    )
    first_wait = next(i for i, a in enumerate(args) if "task_instance" in a)
    pause_hist = args.index(f"exec sofascore-airflow-scheduler airflow dags pause {HIST}")
    pause_refresh = args.index(f"exec sofascore-airflow-scheduler airflow dags pause {REFRESH}")
    first_compose = min(i for i, a in enumerate(args) if a.startswith("compose "))
    assert drain < first_wait < pause_hist < first_compose, args
    assert pause_refresh < first_wait, args
    assert f"unpause {HIST}" not in "\n".join(args), args


@pytest.mark.unit
def test_drain_waits_for_the_tracked_history_run_not_for_an_empty_contour(tmp_path: Path) -> None:
    """Ревью Sol, раунд 3, находка №1. История идёт @continuous: как только отслеживаемый
    прогон кончается, планировщик почти мгновенно (замер 04.09: медиана 26 с) создаёт
    следующий, и с осушённым пулом тот остаётся running навсегда — run_historical_scope
    вечно scheduled. Условие «прогонов истории нет» достижимо ровно один раз, в промежутке
    ~26 с при опросе раз в 30 с: монетка, промах — rc=4 и ночь без доставки. Ждать надо
    завершения ИМЕННО того прогона, который работал на входе; новый пустой закрывает
    close_stale_runs после паузы."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    tracked = "scheduled__2026-09-04T03:31:00+00:00"
    (state_dir / "hist_run").write_text(f"{tracked}\n")   # прогон, который шёл на входе...
    # ...он уже закончился (флага hist_run_active нет), а сменщик создан и висит running
    (state_dir / "active").write_text("1\n")
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "SOFASCORE_DEPLOY_IDLE_WAIT": "60",
    }
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    waits = [a for a in args if "run_id=" in a]
    assert waits, args
    assert f"run_id='{tracked}'" in waits[0], waits[0]
    assert [a for a in args if a.startswith("compose ")], "выкат обязан состояться, а не уйти в rc=4"
    log = (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")
    assert f"ждём прогон истории '{tracked}'" in log, log


@pytest.mark.unit
def test_drain_refuses_to_start_when_the_metadb_cannot_name_the_history_run(tmp_path: Path) -> None:
    """Пустой ответ на «какой прогон истории сейчас работает» — это «не знаю», а не «его
    нет»: выкатывать вслепую значит оборвать оплаченный скоуп. Код 4 — «контур занят, выкат
    не начат», бой не тронут. `|| true` на этом чтении обязателен и по второй причине: без
    него отказ метабазы под `set -e` вышел бы кодом timeout (124), а для автомата ночной
    доставки 124 — это «таймаут доставки», то есть полный откат боя, которого не было."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (state_dir / "hist_run").write_text("")   # метабаза не ответила
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}", "SOFASCORE_ENV_FILE": str(env_file)}
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 4, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    assert not [a for a in args if a.startswith("compose ")], args
    assert f"SOFASCORE_RELEASE_ROOT={release}" not in env_file.read_text(encoding="utf-8")
    assert (state_dir / f"paused_{HIST}").read_text().strip() == "f"
    log = (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")
    assert "метабаза не ответила про идущий прогон истории" in log, log


@pytest.mark.unit
def test_a_refresh_run_frozen_by_the_pause_does_not_block_the_deploy(tmp_path: Path) -> None:
    """Тот же класс, что находка №1, только на соседнем DAG. Пауза не даёт планировщику
    двигать прогон (03.09 так замёрз прогон истории, и его закрывали руками), поэтому ждать
    ЗАКРЫТИЯ прогона актуалки, которую этот же шаг только что запаузил, значит ждать до
    потолка и уйти в rc=4. Ждём её ЗАДАЧ — именно их обрывает пересоздание, — а сам прогон
    доработает, когда шаг restore-pause вернёт актуалку в работу."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (state_dir / "active_refresh").write_text("1\n")   # прогон актуалки идёт с 00:30 UTC
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "SOFASCORE_DEPLOY_IDLE_WAIT": "60",
    }
    proc = subprocess.run(
        ["bash", str(DEPLOY / "deploy.sh"), str(release)], env=env, capture_output=True, text=True, timeout=120
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    assert [a for a in args if a.startswith("compose ")], "замёрзший прогон актуалки не повод не выкатывать"
    assert (state_dir / f"paused_{REFRESH}").read_text().strip() == "f", "актуалка возвращена в работу"


@pytest.mark.unit
def test_env_loader_strips_quotes_and_never_expands_or_exports(tmp_path: Path) -> None:
    env_file = tmp_path / "x.env"
    env_file.write_bytes(
        b"# comment\r\n"
        b"SOFASCORE_PLAIN=a b\r\n"
        b"SOFASCORE_SINGLE='[{\"host\":\"h\",\"port\":1},{\"x\":\"$HOME\"}]'\r\n"
        b'SOFASCORE_DOUBLE="q,{r} \\"quoted\\" back\\\\slash"\n'
        b"SOFASCORE_EMPTY=\r\n"
        b"SOFASCORE_STALE=fresh\n"
    )
    script = f"""\
        export SOFASCORE_STALE=from-operator-shell
        . {DEPLOY}/env.sh
        sofascore_load_env {env_file} || exit 9
        printf '%s|%s|%s|%s|%s\\n' "$SOFASCORE_PLAIN" "$SOFASCORE_SINGLE" "$SOFASCORE_DOUBLE" \\
          "${{SOFASCORE_EMPTY-unset}}" "$SOFASCORE_STALE"
        env | grep -c '^SOFASCORE_' || true
        """
    proc = subprocess.run(["bash", "-c", textwrap.dedent(script)], capture_output=True, text=True, check=True)
    values, exported = proc.stdout.splitlines()
    assert values == 'a b|[{"host":"h","port":1},{"x":"$HOME"}]|q,{r} "quoted" back\\slash||fresh'
    # A value inherited as exported from the operator shell is replaced AND un-exported.
    assert exported == "0"
    for bad_text, reason in (
        ("NOT A LINE\n", "no equals sign"),
        ("PATH=/evil\n", "foreign key must not clobber the script environment"),
        ("SOFASCORE_ok-ish=1\n", "invalid identifier"),
    ):
        bad = _write(tmp_path / "bad.env", bad_text)
        proc = subprocess.run(
            ["bash", "-c", f"export PATH; . {DEPLOY}/env.sh; sofascore_load_env {bad}; echo rc=$?; command -v bash"],
            capture_output=True, text=True,
        )
        assert "rc=2" in proc.stdout, reason
        assert proc.stdout.strip().endswith("bash"), "PATH survived the rejected file"


def _postdeploy_stub(bin_dir: Path, mounts_file: Path) -> None:
    _write(
        bin_dir / "docker",
        f"""\
        #!/usr/bin/env bash
        if [ "$1" = inspect ]; then
          case "$*" in
            *".Type}}}}:{{{{.Destination}}}}={{{{.Source}}}}"*) grep "^$(printf '%s' "${{@: -1}}")|" "{mounts_file}" | cut -d'|' -f2- ;;
            *Health.Status*) echo healthy ;;
            *HostConfig.Memory*) echo 1073741824 ;;
            *Config.Cmd*) echo -- --sofascore-discovery-dagrun-budget-bytes; echo 67108864 ;;
            *Config.Env*) echo SOFASCORE_ALL_MENS_STATE=/x ;;
            *) echo "Memory=1073741824 Started=now Health=healthy" ;;
          esac
          exit 0
        fi
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-metadb ]; then
          case "${{@: -1}}" in
            *import_error*) echo 0 ;;
            *is_active=true*) echo 5 ;;
            *slot_pool*) echo 1 ;;
            *) echo "dag|f|t" ;;
          esac
          exit 0
        fi
        if [ "$1" = exec ]; then echo '{{"status":"ok"}}'; exit 0; fi
        exit 0
        """,
        0o755,
    )
    # У каждого unit'а свой ExecStart: приёмка обязана ловить сторожа, который
    # active, но сторожит чужой шлюз по чужому каталогу состояния.
    _write(
        bin_dir / "systemctl",
        """\
        #!/usr/bin/env bash
        case "$*" in
          *show*)
            case "$*" in
              *-history.service*) c=$WD_HISTORY_CONTAINER; s=$WD_HISTORY_STATE ;;
              *-players.service*) c=$WD_PLAYERS_CONTAINER; s=$WD_PLAYERS_STATE ;;
              *) c=$WD_MAIN_CONTAINER; s=$WD_MAIN_STATE ;;
            esac
            echo "ExecStart={ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 /usr/local/libexec/sofascore-gw-lease-watchdog --container $c --state-dir $s --expected-mount $EXPECTED_MOUNT --alert-command /a ; ignore_errors=no }" ;;
          *) echo active ;;
        esac
        """,
        0o755,
    )


GATEWAY_CONTAINERS = ("sofascore_gw_951", "sofascore_gw_history", "sofascore_gw_players")


def _run_postdeploy(
    tmp_path: Path,
    scheduler_mounts: dict[str, str],
    gateway_mounts: dict[str, dict[str, str]],
    watchdogs: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    runtime = tmp_path / "runtime"
    release = tmp_path / "releases" / f"release-{TAG}-abcdef12"
    _write(runtime / "all-men" / "state.json", json.dumps({"completed": [1, 2]}))
    (runtime / "all-men" / "results").mkdir(parents=True, exist_ok=True)
    env_file = _write(
        tmp_path / "sofascore.env",
        f"""\
        SOFASCORE_RELEASE_ROOT={release}
        SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR={runtime}/all-men
        SOFASCORE_GATEWAY_STATE_HOST_DIR={runtime}/gateway-state
        SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state-history
        SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/gateway-state-players
        SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST={runtime}/artifacts/{DIGEST}/proxy_budget_canary.json
        SOFASCORE_PROXY_POOL_FILE={runtime}/proxys.txt
        SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE={runtime}/fallback.txt
        SOFASCORE_LEGACY_SCRAPER_VENV_HOST_DIR={runtime}/legacy-scraper-venv
        """,
    )
    mounts_file = tmp_path / "mounts.txt"
    def _typed(dest: str, source: str) -> str:
        kind = "volume" if source.startswith("/var/lib/docker/volumes/") else "bind"
        return f"{kind}:{dest}={source}"

    mounts_file.write_text(
        "".join(f"sofascore-airflow-scheduler|{_typed(d, s)}\n" for d, s in scheduler_mounts.items())
        + "".join(
            f"{container}|{_typed(d, s)}\n"
            for container, mounts in gateway_mounts.items()
            for d, s in mounts.items()
        ),
        encoding="utf-8",
    )
    _postdeploy_stub(tmp_path / "bin", mounts_file)
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "EXPECTED_MOUNT": str(release),
        "WD_MAIN_CONTAINER": "sofascore_gw_951",
        "WD_MAIN_STATE": f"{runtime}/gateway-state",
        "WD_HISTORY_CONTAINER": "sofascore_gw_history",
        "WD_HISTORY_STATE": f"{runtime}/gateway-state-history",
        "WD_PLAYERS_CONTAINER": "sofascore_gw_players",
        "WD_PLAYERS_STATE": f"{runtime}/gateway-state-players",
        **(watchdogs or {}),
    }
    return subprocess.run(
        ["bash", str(DEPLOY / "postdeploy_checks.sh")], env=env, capture_output=True, text=True, timeout=60
    )


def _expected_mounts(tmp_path: Path) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    runtime = tmp_path / "runtime"
    release = tmp_path / "releases" / f"release-{TAG}-abcdef12"
    artifact = f"{runtime}/artifacts/{DIGEST}/proxy_budget_canary.json"
    scheduler = {
        "/opt/airflow/dags": f"{release}/dags",
        "/opt/airflow/dags/.airflowignore": f"{release}/deploy/sofascore/.airflowignore",
        "/opt/airflow/logs": f"{release}/logs",
        "/opt/airflow/scrapers": f"{release}/scrapers",
        "/opt/airflow/scripts": f"{release}/scripts",
        "/opt/airflow/configs/medallion": f"{release}/configs/medallion",
        "/opt/airflow/configs/soccerdata": f"{release}/configs/soccerdata",
        "/opt/airflow/configs/sofascore": f"{release}/configs/sofascore",
        "/opt/airflow/configs/proxy_filter": f"{release}/configs/proxy_filter",
        "/opt/airflow/docker": f"{release}/docker",
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json": artifact,
        "/opt/airflow/runtime/sofascore/all-men": f"{runtime}/all-men",
        "/opt/airflow/proxys.txt": f"{runtime}/proxys.txt",
        "/opt/legacy-scraper-venv": f"{runtime}/legacy-scraper-venv",
        "/home/airflow/soccerdata": "/var/lib/docker/volumes/sofascore_soccerdata_cache/_data",
    }
    # Дерево, fallback-файл и артефакт общие у трёх полос; каталог состояния — свой:
    # WAL/ledger шлюза рассчитаны на единственного писателя.
    state_dirs = {
        "sofascore_gw_951": f"{runtime}/gateway-state",
        "sofascore_gw_history": f"{runtime}/gateway-state-history",
        "sofascore_gw_players": f"{runtime}/gateway-state-players",
    }
    gateways = {
        container: {
            "/opt/sofascore-repo": str(release),
            "/opt/airflow/proxys.txt": f"{runtime}/fallback.txt",
            "/opt/airflow/runtime/sofascore/proxy_budget_canary.json": artifact,
            "/opt/airflow/logs/sofascore_proxy_filter": state_dir,
        }
        for container, state_dir in state_dirs.items()
    }
    return scheduler, gateways


@pytest.mark.unit
def test_postdeploy_passes_only_when_every_mount_pair_matches(tmp_path: Path) -> None:
    scheduler, gateways = _expected_mounts(tmp_path)
    assert tuple(gateways) == GATEWAY_CONTAINERS
    proc = _run_postdeploy(tmp_path, scheduler, gateways)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ПРИЁМКА: ок" in proc.stdout
    # Три полосы проверяются целиком: health, пулы, сторожа.
    for container in GATEWAY_CONTAINERS:
        assert f"✓ {container} healthy" in proc.stdout, proc.stdout
        assert f"✓ {container} лимит памяти 1 GiB" in proc.stdout, proc.stdout
    for service in ("sofascore_proxy_filter", "sofascore_gw_history", "sofascore_gw_players"):
        assert f"✓ {service} /health отвечает" in proc.stdout, proc.stdout
    for unit, container in (
        ("sofascore-gw-lease-watchdog.service", "sofascore_gw_951"),
        ("sofascore-gw-lease-watchdog-history.service", "sofascore_gw_history"),
        ("sofascore-gw-lease-watchdog-players.service", "sofascore_gw_players"),
    ):
        assert f"✓ {unit} active" in proc.stdout, proc.stdout
        assert f"✓ {unit} --container {container}" in proc.stdout, proc.stdout
    for pool in ("ingest_scraper_pool", "sofascore_history_pool", "sofascore_players_pool"):
        assert f"✓ пул {pool} slots=1" in proc.stdout, proc.stdout


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda s, g: s.__setitem__("/opt/airflow/scripts", "/old/release-deadbeef/scripts"),
        lambda s, g: s.pop("/opt/airflow/docker"),
        lambda s, g: s.__setitem__("/opt/airflow/dags/dag_trigger_sofascore_daily.py", "/old/runtime/dag.py"),
        lambda s, g: g["sofascore_gw_951"].__setitem__("/opt/sofascore-repo", "/old/release-deadbeef"),
        lambda s, g: s.__setitem__("/opt/airflow/extra", "/old/release-deadbeef/scripts"),
        lambda s, g: g["sofascore_gw_951"].__setitem__("/opt/airflow/proxys-extra.txt", "/old/runtime/proxys.txt"),
        lambda s, g: g["sofascore_gw_history"].__setitem__("/opt/sofascore-repo", "/old/release-deadbeef"),
        lambda s, g: g["sofascore_gw_players"].__setitem__(
            "/opt/airflow/logs/sofascore_proxy_filter",
            g["sofascore_gw_951"]["/opt/airflow/logs/sofascore_proxy_filter"],
        ),
        lambda s, g: g.pop("sofascore_gw_players"),
    ],
    ids=[
        "old-tree-mount", "missing-mount", "stale-mini-dag-file-bind", "gateway-old-tree",
        "scheduler-extra-bind-elsewhere", "gateway-extra-bind",
        "history-gateway-old-tree", "players-gateway-shares-state-dir", "players-gateway-missing",
    ],
)
def test_postdeploy_fails_on_a_wrong_missing_or_extra_mount(tmp_path: Path, mutate) -> None:
    scheduler, gateways = _expected_mounts(tmp_path)
    mutate(scheduler, gateways)
    proc = _run_postdeploy(tmp_path, scheduler, gateways)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ПРИЁМКА: " in proc.stdout and "ПРИЁМКА: ок" not in proc.stdout, proc.stdout


@pytest.mark.unit
@pytest.mark.parametrize(
    "watchdogs",
    [
        {"WD_HISTORY_CONTAINER": "sofascore_gw_951"},
        {"WD_PLAYERS_STATE": "/runtime/gateway-state"},
        {"WD_MAIN_STATE": "/runtime/gateway-state-history"},
    ],
    ids=["history-watchdog-guards-the-refresh-gateway", "players-watchdog-on-shared-state", "main-watchdog-on-history-state"],
)
def test_postdeploy_fails_when_a_watchdog_guards_the_wrong_lane(
    tmp_path: Path, watchdogs: dict[str, str]
) -> None:
    # Сторож может быть active и стоять на верном дереве, но освобождать аренду
    # чужого шлюза по чужому WAL — приёмка обязана это ловить.
    scheduler, gateways = _expected_mounts(tmp_path)
    proc = _run_postdeploy(tmp_path, scheduler, gateways, watchdogs=watchdogs)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ПРИЁМКА: ок" not in proc.stdout, proc.stdout
