"""deploy/sofascore/deploy.sh против заглушек docker/systemctl (#1155, этап 3).

Скрипт выката гоняется целиком: заглушка `docker` пишет каждый вызов и то, какие
SOFASCORE_* переменные видит compose в окружении; заглушка метабазы держит состояние
паузы DAG-ов. Ловит два дефекта первого ревью: устаревшее значение из окружения
процесса перекрывало перепинованный env-файл, и актуалка не ставилась на паузу.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
import subprocess
import textwrap

import pytest

from tests.unit.deploy.conftest import rows, seed, write_metadb_stub
from tests.unit.deploy.conftest import world as world_helper


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "sofascore"
DIGEST = "0123456789abcdef" * 4
TAG = DIGEST[:8]
HIST = "dag_backfill_sofascore_all_mens"
REFRESH = "dag_refresh_sofascore_all_mens"
MAINT = "dag_sofascore_manifest_maintenance"


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
        PY="{sys.executable}"
        printf '%s\\t%s\\t%s\\n' "$*" "${{SOFASCORE_RELEASE_ROOT-<unset>}}" \\
          "${{SOFASCORE_PROXY_BUDGET_ARTIFACT_ID-<unset>}}" >> "$STATE/calls.log"
        if [ "$1" = exec ]; then
          sql="${{@: -1}}"
          shift
          [ "$1" = "-i" ] && shift
          container="$1"; shift
          if [ "$container" = sofascore-airflow-metadb ]; then
            case "$sql" in
              *"UPDATE dag SET is_paused=false"*)
                dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}
                echo f > "$STATE/paused_$dag"; echo "UPDATE 1"; exit 0 ;;
              *"SELECT is_paused"*)
                dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}
                cat "$STATE/paused_$dag"; exit 0 ;;
              *"is_active=true"*) cat "$STATE/active_count" 2>/dev/null || echo 3; exit 0 ;;
              # Всё остальное исполняется по-настоящему против sqlite-метабазы.
              *)
                # «Метабаза не отвечает»: пустой вывод и ненулевой код — то же, что таймаут.
                [ -e "$STATE/metadb_down" ] && exit 1
                case "$sql" in
                  *"state IN ('scheduled','up_for_retry')"*)
                    # Строка опроса: перед N-м витком применяем мир turn_N.sql, если он есть.
                    n=$(( $(cat "$STATE/turn" 2>/dev/null || echo 0) + 1 ))
                    echo "$n" > "$STATE/turn"
                    if [ -f "$STATE/turn_$n.sql" ]; then
                      "$PY" "$STATE/sqlrun.py" "$STATE/metadb.sqlite" "$(cat "$STATE/turn_$n.sql")" script
                    fi ;;
                esac
                exec "$PY" "$STATE/sqlrun.py" "$STATE/metadb.sqlite" "$sql" ;;
            esac
          fi
          if [ "$container" = sofascore-airflow-scheduler ] && [ "$1" = python ]; then
            if [ "$2" = - ]; then
              # Ломатель тупика: текст идёт по stdin и исполняется НАСТОЯЩИЙ, поверх той же
              # sqlite-базы. Последствия для планировщика — отдельным файлом.
              if [ -e "$STATE/breaker_fails" ]; then cat > /dev/null; echo "breaker stub failure" >&2; exit 1; fi
              shift 2
              "$PY" "$STATE/breaker_host.py" "$STATE/metadb.sqlite" "$@" || exit $?
              if [ -f "$STATE/after_breaker.sql" ]; then
                "$PY" "$STATE/sqlrun.py" "$STATE/metadb.sqlite" "$(cat "$STATE/after_breaker.sql")" script
              else
                echo "NO-EFFECT" >> "$STATE/breaker.log"
              fi
              exit 0
            fi
            if [ "$2" = -c ]; then
              # close_stale_runs: ORM внутри планировщика закрывает висящие прогоны.
              exec "$PY" "$STATE/sqlrun.py" "$STATE/metadb.sqlite" \\
                "UPDATE dag_run SET state='failed' WHERE dag_id='$4' AND state IN ('queued','running')"
            fi
            exit 0
          fi
          if [ "$container" = sofascore-airflow-scheduler ] && [ "$1" = airflow ]; then
            # @continuous успевает создать новый dag_run между «контур свободен» и паузой;
            # под паузой он замерзает и сам никогда не закроется.
            if [ "$3" = pause ] && [ -e "$STATE/stale_on_pause_$4" ]; then
              "$PY" "$STATE/sqlrun.py" "$STATE/metadb.sqlite" \\
                "INSERT INTO dag_run (dag_id, run_id, state, start_date) VALUES ('$4','stale-on-pause','running','2026-09-05T04:00:00')"
            fi
            # scheduler-down simulation: `airflow dags unpause` fails once the flag exists
            [ "$3" = unpause ] && [ -e "$STATE/scheduler_down" ] && exit 1
            # обслуживание манифеста не распаузилось: команда прошла, состояние не сошлось
            [ "$3" = unpause ] && [ -e "$STATE/maint_unpause_fails" ] && [ "$4" = "dag_sofascore_manifest_maintenance" ] && exit 0
            case "$3" in pause) echo t > "$STATE/paused_$4" ;; unpause) echo f > "$STATE/paused_$4" ;; esac
            exit 0
          fi
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


def _layout(
    tmp_path: Path, *, refresh_paused: str, maint_paused: str = "t"
) -> tuple[Path, Path, Path, Path]:
    runtime = tmp_path / "runtime"
    release = tmp_path / "releases" / f"release-{TAG}-abcdef12"
    state_dir = tmp_path / "stub-state"
    bin_dir = tmp_path / "bin"
    state_dir.mkdir()
    for lane in ("gateway-state", "gateway-state-history", "gateway-state-players", "auto-deliver"):
        (runtime / lane).mkdir(parents=True)
    _stubs(bin_dir, state_dir)
    write_metadb_stub(state_dir)
    (state_dir / f"paused_{HIST}").write_text("f\n")
    (state_dir / f"paused_{REFRESH}").write_text(f"{refresh_paused}\n")
    (state_dir / f"paused_{MAINT}").write_text(f"{maint_paused}\n")
    for name in ("airflow.compose.yaml", "gateway.compose.yaml"):
        _write(release / "deploy" / "sofascore" / name, "services: {}\n")
    # Ломатель тупика едет в дереве релиза: deploy.sh кормит его текстом stdin `docker exec`.
    _write(
        release / "deploy" / "sofascore" / "drain_breaker.py",
        (DEPLOY / "drain_breaker.py").read_text(encoding="utf-8"),
    )
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
    # Файла может не быть вовсе: занятый замок выката останавливает скрипт до первого
    # обращения к docker, и это само по себе доказательство «бой не тронут».
    log = state_dir / "calls.log"
    if not log.exists():
        return []
    return [tuple(line.split("\t")) for line in log.read_text(encoding="utf-8").splitlines()]


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


_RUN_ID = "scheduled__2026-09-05T02:15:41.446584+00:00"
_DEFAULT = object()
# Занятый контур: у дейли идёт задача — второе число строки опроса никогда не станет нулём.
_DAILY_BUSY = (
    "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number)"
    " VALUES ('dag_ingest_sofascore','daily-1','ingest',-1,'running','default_pool',1);"
)
# Что делает ПЛАНИРОВЩИК после того, как ломатель поставил скоупу failed: validate-плейсхолдер
# получает upstream_failed, finalize пишет отказ в failures.json, propagate закрывает прогон.
_CLOSE_RUN = "UPDATE dag_run SET state='{state}' WHERE run_id='" + _RUN_ID + "';"
_TAIL_AFTER_FAILURE = (
    "UPDATE task_instance SET state='upstream_failed' WHERE task_id='validate_historical_scope';"
    "UPDATE task_instance SET state='success'"
    " WHERE task_id IN ('finalize_historical_run','propagate_historical_status');"
    f"UPDATE dag_run SET state='failed' WHERE run_id='{_RUN_ID}';"
)


def _env(tmp_path: Path, env_file: Path, *, idle_wait: str, extra: dict | None = None) -> dict:
    return {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "SOFASCORE_ENV_FILE": str(env_file),
        "SOFASCORE_DEPLOY_IDLE_WAIT": idle_wait,
        **(extra or {}),
    }


class _Run:
    """Результат прогона deploy.sh против sqlite-метабазы стенда."""

    def __init__(self, proc, runtime: Path, release: Path, env_file: Path, state_dir: Path) -> None:
        self.proc, self.runtime, self.release = proc, runtime, release
        self.env_file, self.state_dir = env_file, state_dir

    @property
    def out(self) -> str:
        return self.proc.stdout + self.proc.stderr

    @property
    def args(self) -> list[str]:
        return [call[0] for call in _calls(self.state_dir)]

    @property
    def compose_calls(self) -> list[str]:
        return [a for a in self.args if a.startswith("compose ")]

    @property
    def breaker_calls(self) -> list[str]:
        return [a for a in self.args if " python - " in a]

    @property
    def log(self) -> str:
        return (self.runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")

    def scope_state(self) -> str | None:
        return rows(
            self.state_dir,
            "SELECT state FROM task_instance WHERE task_id='run_historical_scope' AND map_index=0",
        )[0][0]

    def dag_run_state(self) -> str:
        return rows(self.state_dir, "SELECT state FROM dag_run LIMIT 1")[0][0]

    def drain_env(self) -> dict[str, str]:
        text = (self.runtime / "auto-deliver" / "last-drain.env").read_text(encoding="utf-8")
        return dict(line.split("=", 1) for line in text.splitlines() if line)


def _deploy(
    tmp_path: Path,
    *,
    idle_wait: str = "600",
    world: dict | None | object = _DEFAULT,
    seed_sql: str = "",
    turns: dict[int, str] | None = None,
    after_breaker: str | None = None,
    breaker_fails: bool = False,
    metadb_down: bool = False,
    clock: bool = False,
    health: str | None = None,
    refresh_paused: str = "f",
    maint_paused: str = "t",
    under_lock_fd: bool = False,
    env_extra: dict | None = None,
    pre=None,
) -> _Run:
    runtime, release, env_file, state_dir = _layout(
        tmp_path, refresh_paused=refresh_paused, maint_paused=maint_paused
    )
    if world is not None:
        world_helper(state_dir, run_id=_RUN_ID, **({} if world is _DEFAULT else world))
    if seed_sql:
        seed(state_dir, seed_sql)
    for turn, sql in (turns or {}).items():
        (state_dir / f"turn_{turn}.sql").write_text(sql, encoding="utf-8")
    if after_breaker is not None:
        (state_dir / "after_breaker.sql").write_text(after_breaker, encoding="utf-8")
    if breaker_fails:
        (state_dir / "breaker_fails").touch()
    if metadb_down:
        (state_dir / "metadb_down").touch()
    if clock:
        (state_dir / "clock").write_text("1767225600\n")
    if health:
        (state_dir / "health").write_text(f"{health}\n")
    if pre is not None:
        pre(runtime)
    env = _env(tmp_path, env_file, idle_wait=idle_wait, extra=env_extra)
    if under_lock_fd:
        env["SOFASCORE_DEPLOY_LOCK_FD"] = "8"
        cmd = [
            "bash", "-c",
            f'exec 8>"{runtime}/deploy.lock"; flock -n 8 || exit 9;'
            f' exec bash "{DEPLOY}/deploy.sh" "{release}"',
        ]
    else:
        cmd = ["bash", str(DEPLOY / "deploy.sh"), str(release)]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=180)
    return _Run(proc, runtime, release, env_file, state_dir)


@pytest.mark.unit
def test_deploy_gives_up_honestly_when_the_contour_never_goes_idle(tmp_path: Path) -> None:
    """Потолок ожидания. Раньше `while true` без таймаута висел ВЕЧНО: история идёт
    @continuous, а под паузой её хвост не выполняется вовсе и dag_run не двигается.
    Теперь исчерпанный потолок — код 4 «контур занят, выкат не начат», и он обязан быть
    честным: ни одного compose, env не перепинован, артефакт не создан, а осушённый пул
    и пауза истории возвращены как были."""
    r = _deploy(tmp_path, idle_wait="0", seed_sql=_DAILY_BUSY, world=None)

    assert r.proc.returncode == 4, r.out
    assert not r.compose_calls, r.args
    assert f"SOFASCORE_RELEASE_ROOT={r.release}" not in r.env_file.read_text(encoding="utf-8")
    assert not (r.runtime / "artifacts").exists()
    pools = [
        a.split()[5:7] for a in r.args
        if a.startswith("exec sofascore-airflow-scheduler airflow pools set ")
    ]
    assert pools == [["sofascore_history_pool", "0"], ["sofascore_history_pool", "1"]], pools
    # Контур занят — значит НИЧЕГО не изменилось: обе кампании работают дальше.
    assert (r.state_dir / f"paused_{REFRESH}").read_text().strip() == "f"
    assert (r.state_dir / f"paused_{HIST}").read_text().strip() == "f"
    assert "FAILED at step 'drain'" in r.log
    assert "nothing deployed" in r.log


@pytest.mark.unit
def test_the_idle_ceiling_counts_wall_clock_not_the_sum_of_sleeps(tmp_path: Path) -> None:
    """Ревью Sol, раунд 1. Каждый виток ожидания делает запросы к метабазе с таймаутом до
    SOFASCORE_DEPLOY_METADB_TIMEOUT секунд каждый. Пока потолок уменьшался «на 30 за
    виток», на недоступной метабазе 5400 с превращались почти в 4,5 часа — то есть выкат
    всё равно съедал бы всё окно ночной доставки. Часы идут по 60 с за обращение: при
    потолке 100 с честный счёт даёт не больше двух витков, счёт по sleep дал бы четыре."""
    r = _deploy(tmp_path, idle_wait="100", seed_sql=_DAILY_BUSY, clock=True, world=None)

    assert r.proc.returncode == 4, r.out
    polls = [a for a in r.args if "state IN ('scheduled','up_for_retry')" in a]
    assert 1 <= len(polls) <= 2, polls
    assert "contour still busy after 100s" in r.log


@pytest.mark.unit
def test_drain_refuses_to_start_when_the_metadb_cannot_name_the_history_run(tmp_path: Path) -> None:
    """Пустой ответ на «какой прогон истории начал платную работу» — это «не знаю», а не
    «его нет»: выкатывать вслепую значит оборвать оплаченный скоуп. Код 4 — «контур занят,
    выкат не начат», бой не тронут. `|| true` на этом чтении обязателен и по второй причине:
    без него отказ метабазы под `set -e` вышел бы кодом timeout (124), а для автомата ночной
    доставки 124 — это «таймаут доставки», то есть полный откат боя, которого не было."""
    r = _deploy(tmp_path, metadb_down=True)

    assert r.proc.returncode == 4, r.out
    assert not r.compose_calls, r.args
    assert f"SOFASCORE_RELEASE_ROOT={r.release}" not in r.env_file.read_text(encoding="utf-8")
    assert (r.state_dir / f"paused_{HIST}").read_text().strip() == "f"
    assert "метабаза не ответила про идущий прогон истории" in r.log


@pytest.mark.unit
def test_a_refresh_run_frozen_by_the_pause_does_not_block_the_deploy(tmp_path: Path) -> None:
    """Пауза не даёт планировщику двигать прогон (03.09 так замёрз прогон истории, и его
    закрывали руками), поэтому ждать ЗАКРЫТИЯ прогона актуалки, которую этот же шаг только
    что запаузил, значит ждать до потолка и уйти в rc=4. Ждём её ЗАДАЧ — именно их обрывает
    пересоздание, — а сам прогон доработает, когда шаг restore-pause вернёт актуалку."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=None,
        seed_sql=(
            "INSERT INTO dag_run (dag_id, run_id, state, start_date)"
            f" VALUES ('{REFRESH}','refresh-00:30','running','2026-09-05T00:30:00');"
        ),
    )

    assert r.proc.returncode == 0, r.out
    assert r.compose_calls, "замёрзший прогон актуалки не повод не выкатывать"
    assert (r.state_dir / f"paused_{REFRESH}").read_text().strip() == "f"


# --- §2, группа (а): выбор отслеживаемого прогона на входе в drain ------------------------
# Отслеживается прогон, который НАЧАЛ платную работу. Всё остальное — не ждём и не трогаем.


@pytest.mark.unit
@pytest.mark.parametrize("state", ["running", "queued", "restarting"])
def test_a_working_scope_is_tracked_and_waited_for_without_the_breaker(
    tmp_path: Path, state: str
) -> None:
    """Ночь 06.09: скоуп честно работал 62 минуты. Такой прогон ждут, ломателя не зовут —
    задача не припаркована, она делает оплаченную работу. `restarting` ставит `clear`
    работающей задачи: это работа, а не её отсутствие."""
    r = _deploy(tmp_path, idle_wait="60", world=dict(scope_state=state, scope_try=1))

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert not r.compose_calls, r.args
    assert "отслеживаю прогон истории" in r.log


@pytest.mark.unit
@pytest.mark.parametrize(
    "scope_state,scope_try",
    [("up_for_retry", 1), ("scheduled", 2)],
)
def test_a_parked_retry_is_broken_and_the_deploy_goes_on(
    tmp_path: Path, scope_state: str, scope_try: int
) -> None:
    """Ночь 05.09 обеими сторонами щели: скоуп упал (`up_for_retry`), через retry_delay
    повтор встал `scheduled` (try 2) в осушённый пул. Слота до конца выката не будет, а
    прогон висит running — старый выбор видел `0|0|1|0` с первого же опроса и ждал 92
    минуты до rc=4. Ломатель переводит скоуп в failed, планировщик доигрывает хвост
    (это делает after_breaker.sql), и выкат состоится."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_state=scope_state, scope_try=scope_try),
        after_breaker=_TAIL_AFTER_FAILURE,
    )

    assert r.proc.returncode == 0, r.out
    assert len(r.breaker_calls) == 1, r.breaker_calls
    assert r.compose_calls, r.args
    # Ломатель поставил ровно failed — и ровно скоупу, а не прогону.
    assert r.scope_state() == "failed"
    assert "-> failed" in r.log


@pytest.mark.unit
def test_the_breaker_is_called_with_the_tracked_run_and_the_drained_pool(tmp_path: Path) -> None:
    """Аргументы ломателя — не украшение: чужой прогон он ронять не вправе, а пул выката и
    пул задачи обязаны быть одним значением (HIST_POOL)."""
    r = _deploy(tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE)

    assert len(r.breaker_calls) == 1, r.breaker_calls
    call = r.breaker_calls[0]
    assert call.endswith(f"python - {HIST} {_RUN_ID} sofascore_history_pool"), call


@pytest.mark.unit
def test_a_scope_scheduled_on_its_first_try_is_not_tracked_at_all(tmp_path: Path) -> None:
    """Сменщик, созданный @continuous после конца отслеживаемого прогона: его скоуп
    `scheduled` try 1 в осушённом пуле, платного трафика он не купил. Такой прогон не
    ждут и не трогают — после паузы его закрывает close_stale_runs (так было 07.09)."""
    r = _deploy(tmp_path, idle_wait="600", world=dict(scope_state="scheduled", scope_try=1))

    assert r.proc.returncode == 0, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert "прогона истории с начатой платной работой нет" in r.log
    # Закрыл его close_stale_runs (ORM внутри планировщика), а не ломатель.
    assert r.dag_run_state() == "failed"
    closers = [a for a in r.args if a.startswith("exec sofascore-airflow-scheduler python -c ")]
    assert len(closers) == 1, closers


@pytest.mark.unit
def test_a_scope_without_a_state_is_not_tracked(tmp_path: Path) -> None:
    """NULL у скоупа — ручной `clear` или сброс orphan TI планировщиком. Платной работы за
    ним не видно, стартовать в осушённом пуле он не может: не ждём и не трогаем."""
    r = _deploy(tmp_path, idle_wait="600", world=dict(scope_state=None, scope_try=0))

    assert r.proc.returncode == 0, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert "прогона истории с начатой платной работой нет" in r.log


@pytest.mark.unit
def test_a_successful_scope_with_a_pending_validate_is_still_tracked(tmp_path: Path) -> None:
    """Дыра старого предиката «queued/running»: скоуп уже success, а validate (тот, что
    засчитывает оплаченный скоуп в state.json) ещё не начался. Прогон обязан ждаться до
    закрытия — иначе пересоздание scheduler'а обрывает учёт оплаченной работы."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="success", scope_try=1, validate_state=None),
    )

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert not r.compose_calls, r.args
    assert f"отслеживаю прогон истории '{_RUN_ID}'" in r.log


@pytest.mark.unit
def test_a_failed_scope_in_an_open_run_is_waited_for_not_broken(tmp_path: Path) -> None:
    """Скоуп терминален, но прогон ещё открыт: учёт держат validate и finalize. Ломателю
    здесь делать нечего — ждём закрытия прогона."""
    r = _deploy(tmp_path, idle_wait="60", world=dict(scope_state="failed", scope_try=2))

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls


@pytest.mark.unit
def test_a_parked_scope_in_another_pool_is_waited_for_with_a_diagnosis(tmp_path: Path) -> None:
    """Рассинхрон SOFASCORE_HISTORY_POOL: скоуп припаркован в ЧУЖОМ пуле. Пятое число его
    не считает (drain осушил не тот пул), ломателя не зовём — менять чужое состояние вслепую
    нельзя. Ждём до потолка и говорим, почему."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="up_for_retry", scope_pool="default_pool", scope_try=1),
    )

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert "скоуп в пуле 'default_pool', drain осушил 'sofascore_history_pool'" in r.log


@pytest.mark.unit
def test_the_breaker_refuses_a_batch_bigger_than_one(tmp_path: Path) -> None:
    """Боевой SOFASCORE_HISTORY_BATCH_SIZE не задан (batch=1), и версия поддерживает только
    его: при двух mapped-скоупах «сломать тупик» значило бы погасить учёт соседнего,
    возможно успешного, скоупа. Ломатель зовётся, но не меняет НИЧЕГО."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="running", scope_try=1, extra_scope=(1, "scheduled", 1)),
    )

    assert r.proc.returncode == 4, r.out
    assert r.breaker_calls, "пятое число > 0 — ломателя обязаны позвать"
    assert "batch>1 не поддержан" in r.log
    assert sorted(s for (s,) in rows(
        r.state_dir, "SELECT state FROM task_instance WHERE task_id='run_historical_scope'"
    )) == ["running", "scheduled"]


@pytest.mark.unit
def test_a_parked_scope_of_another_run_is_not_touched(tmp_path: Path) -> None:
    """Пятое число считает задачи ТОЛЬКО отслеживаемого прогона: припаркованный скоуп
    закрытого соседа (его закроет close_stale_runs) ломателя не будит."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="success", scope_try=1, validate_state=None),
        seed_sql=(
            "INSERT INTO dag_run (dag_id, run_id, state, start_date)"
            f" VALUES ('{HIST}','other-run','failed','2026-09-05T02:00:00');"
            "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number)"
            f" VALUES ('{HIST}','other-run','run_historical_scope',0,'up_for_retry','sofascore_history_pool',1);"
        ),
    )

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls


# --- §2, группа (б): переходы уже выбранного прогона --------------------------------------
# Мир меняется между витками опроса (файлы turn_<N>.sql применяются перед N-м опросом).


@pytest.mark.unit
def test_a_running_scope_that_falls_into_a_parked_retry_is_broken_later(tmp_path: Path) -> None:
    """Ровно ночь 05.09: на входе в drain скоуп ещё работал, упал через 2,5 минуты, ушёл в
    up_for_retry — и повтор встал в очередь за слотом, которого нет. Прогон выбран на первом
    витке и удерживается: ломатель приходит к нему, а не ждёт нового выбора."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_state="running", scope_try=1),
        turns={
            2: "UPDATE task_instance SET state='failed' WHERE task_id='run_historical_scope';",
            3: "UPDATE task_instance SET state='up_for_retry' WHERE task_id='run_historical_scope';",
        },
        after_breaker=_TAIL_AFTER_FAILURE,
    )

    assert r.proc.returncode == 0, r.out
    assert len(r.breaker_calls) == 1, r.breaker_calls
    assert r.scope_state() == "failed"


@pytest.mark.unit
def test_a_scope_that_finishes_on_its_own_closes_the_run_without_the_breaker(tmp_path: Path) -> None:
    """Ночь 06.09 и 07.09: скоуп доработал сам, дальше validate → finalize → cooldown →
    propagate → прогон закрыт. Ломателя не зовут ни разу."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_state="running", scope_try=1),
        turns={
            2: (
                "UPDATE task_instance SET state='success' WHERE task_id='run_historical_scope';"
                "UPDATE task_instance SET state='success', map_index=0"
                " WHERE task_id='validate_historical_scope';"
            ),
            3: (
                "UPDATE task_instance SET state='success'"
                " WHERE task_id IN ('finalize_historical_run','propagate_historical_status');"
                f"UPDATE dag_run SET state='success' WHERE run_id='{_RUN_ID}';"
            ),
        },
    )

    assert r.proc.returncode == 0, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert r.drain_env()["DRAIN_ACCOUNTED"] == "t", r.drain_env()


@pytest.mark.unit
def test_a_cleared_scope_of_the_tracked_run_is_waited_for_with_a_diagnosis(tmp_path: Path) -> None:
    """Ручной `clear` в окне выката — вне протокола (рунбук это запрещает). Состояние стало
    NULL: стартовать в осушённом пуле задача не может, а менять его — гадать. Ждём до
    потолка и называем причину."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="running", scope_try=1),
        turns={2: "UPDATE task_instance SET state=NULL WHERE task_id='run_historical_scope';"},
    )

    assert r.proc.returncode == 4, r.out
    assert not r.breaker_calls, r.breaker_calls
    assert "состояние вне протокола" in r.log


@pytest.mark.unit
def test_the_deploy_waits_for_the_run_to_close_after_the_breaker(tmp_path: Path) -> None:
    """После ломателя ждём не «скоуп терминален», а закрытия прогона: finalize уже success,
    но dag_run ещё running — учёт дописывается, пересоздавать scheduler рано."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        after_breaker=(
            "UPDATE task_instance SET state='upstream_failed'"
            " WHERE task_id='validate_historical_scope';"
            "UPDATE task_instance SET state='success' WHERE task_id='finalize_historical_run';"
        ),
    )

    assert r.proc.returncode == 4, r.out
    assert len(r.breaker_calls) == 1, r.breaker_calls
    assert not r.compose_calls, r.args


@pytest.mark.unit
def test_a_successful_breaker_is_called_exactly_once(tmp_path: Path) -> None:
    """Ломатель ставит failed один раз: после него пятое число — ноль, и звать его снова
    незачем. Без последствий планировщика (файла after_breaker.sql нет) выкат честно уходит
    в rc=4, но вызов остаётся РОВНО один."""
    r = _deploy(tmp_path, idle_wait="60")

    assert r.proc.returncode == 4, r.out
    assert len(r.breaker_calls) == 1, r.breaker_calls
    assert (r.state_dir / "breaker.log").read_text().count("NO-EFFECT") == 1


@pytest.mark.unit
def test_a_technically_failed_breaker_is_retried_up_to_the_cap(tmp_path: Path) -> None:
    """Сбой `docker exec` до commit — не «состояние вне протокола», а «не дозвонились»:
    повторяем, но не бесконечно. Потолок — пять вызовов за drain, дальше честное ожидание
    до потолка и rc=4; на код возврата выката сбой ломателя не влияет (иначе автомат
    ночной доставки откатил бы НЕТРОНУТЫЙ бой)."""
    r = _deploy(tmp_path, idle_wait="600", breaker_fails=True)

    assert r.proc.returncode == 4, r.out
    assert len(r.breaker_calls) == 5, r.breaker_calls
    assert "тупик не ломается" in r.log


# --- Доказательство учёта -----------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "world_kw,expected",
    [
        # 05.09: скоуп упал, validate не раскрылся (учёт несёт NULL-плейсхолдер map -1),
        # finalize записал отказ в failures.json.
        (dict(scope_state="failed", validate_state="upstream_failed", finalize_state="success"), "t"),
        # штатный зачёт через mark_completed
        (dict(scope_state="success", validate_state="success", validate_map=0,
              finalize_state="success"), "t"),
        # успешный скоуп с провалившейся валидацией: отказ пишет finalize
        (dict(scope_state="success", validate_state="failed", validate_map=0,
              finalize_state="success"), "t"),
        # finalize не отработал — учёт НЕ подтверждён, хотя прогон терминален
        (dict(scope_state="failed", validate_state="upstream_failed", finalize_state="failed"), "f"),
        (dict(scope_state="failed", validate_state="upstream_failed", finalize_state=None), "f"),
        # dagrun_timeout закрыл прогон вовсе без хвоста
        (dict(scope_state="failed", validate_state=None, finalize_state=None), "f"),
    ],
)
def test_the_capture_scope_accounting_is_proven_from_the_metadb(
    tmp_path: Path, world_kw: dict, expected: str
) -> None:
    """Терминальный прогон — ещё не учтённый скоуп. Доказательство пишется в момент drain:
    failures.json к утру перезаписывается следующим отказом, а в state.json у `completed`
    идентификатора прогона нет вовсе."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_try=1, **world_kw),
        turns={2: _CLOSE_RUN.format(state="failed")},
    )

    assert r.proc.returncode == 0, r.out
    env = r.drain_env()
    assert env["DRAIN_ACCOUNTED"] == expected, env
    assert env["DRAIN_RUN_ID"] == _RUN_ID
    assert env["DRAIN_SCOPE_KIND"] == "capture"
    assert env["DRAIN_SCOPE_KEY"] == "camp1:937:78750"
    assert ("учёт: подтверждён" if expected == "t" else "учёт: НЕ подтверждён") in r.log


@pytest.mark.unit
@pytest.mark.parametrize(
    "scope_state,expected",
    # Упавшая волна метаданных — `unknown`: чекпойнт пишется в середине задачи
    # (scripts/enrich_sofascore_all_mens_snapshot.py), поэтому `failed` бывает и до записи
    # (волну купят заново), и после неё (волна учтена, упало закрытие клиента) — по цвету
    # задачи эти случаи неразличимы. Третий случай: прогон закрыт (dagrun_timeout), а
    # задача метаданных так и не стала терминальной — результата нет, учёт не подтверждён.
    [("success", "t"), ("failed", "unknown"), ("restarting", "f")],
)
def test_the_metadata_scope_accounting_uses_its_own_task_state(
    tmp_path: Path, scope_state: str, expected: str
) -> None:
    """У метаданных нет SOFASCORE_SCOPE_KEY: finalize их пропускает, и результат несёт
    состояние самой задачи. Ключ собирается из плана — <campaign_id>:metadata:<wave>."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(
            scope_state=scope_state, scope_try=1,
            plan_kind="metadata", finalize_state="success",
        ),
        turns={2: _CLOSE_RUN.format(state="success")},
    )

    assert r.proc.returncode == 0, r.out
    env = r.drain_env()
    assert env["DRAIN_SCOPE_KIND"] == "metadata"
    assert env["DRAIN_SCOPE_KEY"] == "camp1:metadata:2024"
    assert env["DRAIN_ACCOUNTED"] == expected, env


@pytest.mark.unit
def test_a_night_without_a_paid_run_is_accounted_as_not_applicable(tmp_path: Path) -> None:
    """Прогона с начатой платной работой не было — учитывать нечего. Это не провал: `n/a`
    идёт в зачёт приёмки наравне с `t`."""
    r = _deploy(tmp_path, idle_wait="600", world=dict(scope_state="scheduled", scope_try=1))

    assert r.proc.returncode == 0, r.out
    env = r.drain_env()
    assert env["DRAIN_ACCOUNTED"] == "n/a"
    assert env["DRAIN_RUN_ID"] == "-"


@pytest.mark.unit
def test_the_drain_proof_carries_the_window_and_replaces_the_previous_one(tmp_path: Path) -> None:
    """Свидетельство привязано к окну автомата: файл прошлой ночи не должен подтверждать
    сегодняшнюю. Старый удаляется в начале drain, новый пишется атомарно (tmp + mv), и
    временного файла после выката не остаётся."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_state="success", validate_state="success",
                   validate_map=0, finalize_state="success", scope_try=1),
        turns={2: _CLOSE_RUN.format(state="success")},
        env_extra={"SOFASCORE_DEPLOY_WINDOW_ID": "2026-09-08"},
        pre=lambda runtime: _write(runtime / "auto-deliver" / "last-drain.env", "DRAIN_WINDOW_ID=2026-01-01\n"),
    )

    assert r.proc.returncode == 0, r.out
    env = r.drain_env()
    assert env["DRAIN_WINDOW_ID"] == "2026-09-08"
    assert env["DRAIN_BREAKER_CALLS"] == "0"
    assert not list((r.runtime / "auto-deliver").glob("*.tmp"))


@pytest.mark.unit
def test_a_manual_deploy_stamps_its_own_window_id(tmp_path: Path) -> None:
    """Ручной выкат окна автомата не знает: свидетельство помечается `manual-<время>`,
    и автомат его не примет за свою ночь."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        world=dict(scope_state="success", validate_state="success",
                   validate_map=0, finalize_state="success", scope_try=1),
        turns={2: _CLOSE_RUN.format(state="success")},
    )

    assert r.drain_env()["DRAIN_WINDOW_ID"].startswith("manual-"), r.drain_env()


# --- Обслуживание манифеста ---------------------------------------------------------------


@pytest.mark.unit
def test_the_manifest_maintenance_is_paused_before_the_first_wait(tmp_path: Path) -> None:
    """Воскресный дедлайн 04:45 срезал окно доставки, потому что в 05:00 стартует
    обслуживание манифеста. Решение владельца 07.09: окно до 06:00 каждый день, а
    обслуживание — под паузой на весь выкат. Пауза обязана лечь ДО первого ожидания:
    иначе прогон обслуживания успел бы стартовать в осушаемом контуре."""
    r = _deploy(tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE)

    assert r.proc.returncode == 0, r.out
    pause_maint = r.args.index(f"exec sofascore-airflow-scheduler airflow dags pause {MAINT}")
    first_poll = next(i for i, a in enumerate(r.args) if "state IN ('scheduled','up_for_retry')" in a)
    assert pause_maint < first_poll, r.args


@pytest.mark.unit
def test_a_running_maintenance_task_is_waited_for(tmp_path: Path) -> None:
    """Задачи обслуживания попали во второе число строки опроса: пересоздание scheduler'а
    оборвало бы их так же, как задачи дейли."""
    r = _deploy(
        tmp_path,
        idle_wait="60",
        world=dict(scope_state="scheduled", scope_try=1),
        seed_sql=(
            "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number)"
            f" VALUES ('{MAINT}','maint-1','compact_manifest',-1,'running','default_pool',1);"
        ),
    )

    assert r.proc.returncode == 4, r.out
    assert not r.compose_calls, r.args


@pytest.mark.unit
@pytest.mark.parametrize("health,rc", [("healthy", 0), ("unhealthy", 5)])
def test_a_manual_deploy_restores_the_maintenance_pause_on_every_outcome(
    tmp_path: Path, health: str, rc: int
) -> None:
    """Ручной запуск — владелец снимка пауз: он же обязан вернуть обслуживание, и на
    успехе, и на аварии."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        maint_paused="f",
        after_breaker=_TAIL_AFTER_FAILURE,
        health=health,
    )

    assert r.proc.returncode == rc, r.out
    assert (r.state_dir / f"paused_{MAINT}").read_text().strip() == "f"


@pytest.mark.unit
@pytest.mark.parametrize("health,rc", [("healthy", 0), ("unhealthy", 5)])
def test_a_deploy_from_the_automat_leaves_the_maintenance_paused(
    tmp_path: Path, health: str, rc: int
) -> None:
    """Запуск из автомата ночной доставки (задан SOFASCORE_DEPLOY_LOCK_FD): паузу держит
    автомат до конца приёмки или отката. Снять её здесь значило бы отдать обслуживанию
    контур посреди разрушительного пути — прогон встретил бы пересоздание контейнеров."""
    r = _deploy(
        tmp_path,
        idle_wait="600",
        maint_paused="f",
        after_breaker=_TAIL_AFTER_FAILURE,
        health=health,
        under_lock_fd=True,
    )

    assert r.proc.returncode == rc, r.out
    assert (r.state_dir / f"paused_{MAINT}").read_text().strip() == "t"
    assert "её вернёт автомат ночной доставки" in r.log


# --- Замок выката --------------------------------------------------------------------------


@pytest.mark.unit
def test_a_busy_deploy_lock_stops_the_deploy_before_it_touches_anything(tmp_path: Path) -> None:
    """Раньше автомат отличал ручной выкат по `pgrep deploy.sh`, а сам deploy.sh не
    проверял ничего: между «процесса нет» и первым изменением контура помещался целый чужой
    выкат. Теперь обе стороны берут один замок ДО первого изменения. Занят — rc=4 «выкат не
    начат»: ни осушения пула, ни пауз."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    lock = runtime / "deploy.lock"
    lock.touch()
    holder = subprocess.Popen(["flock", "-n", str(lock), "sleep", "60"])
    try:
        proc = subprocess.run(
            ["bash", str(DEPLOY / "deploy.sh"), str(release)],
            env=_env(tmp_path, env_file, idle_wait="600"),
            capture_output=True, text=True, timeout=120,
        )
    finally:
        holder.kill()
        holder.wait()

    assert proc.returncode == 4, proc.stdout + proc.stderr
    args = [call[0] for call in _calls(state_dir)]
    assert not [a for a in args if "pools set" in a], args
    assert not [a for a in args if "dags pause" in a], args
    assert "замок выката занят" in (runtime / "all-men" / "deploy.log").read_text(encoding="utf-8")


@pytest.mark.unit
def test_an_inherited_lock_descriptor_lets_the_deploy_run(tmp_path: Path) -> None:
    """Автомат держит замок весь тик и передаёт дескриптор: на том же open file description
    flock отдаёт замок сразу, иначе доставка спотыкалась бы о собственный замок."""
    r = _deploy(tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE, under_lock_fd=True)

    assert r.proc.returncode == 0, r.out
    assert r.compose_calls, r.args


@pytest.mark.unit
def test_a_descriptor_that_is_not_the_lock_is_an_error_not_a_busy_contour(tmp_path: Path) -> None:
    """«Выкат под замком» обязан быть фактом, а не словом: чужой дескриптор — поломка (rc=2),
    а не «контур занят» (rc=4), иначе автомат счёл бы ночь мирно пропущенной."""
    runtime, release, env_file, state_dir = _layout(tmp_path, refresh_paused="f")
    (runtime / "not-the-lock").touch()
    proc = subprocess.run(
        ["bash", "-c",
         f'exec 8>"{runtime}/not-the-lock"; exec bash "{DEPLOY}/deploy.sh" "{release}"'],
        env={**_env(tmp_path, env_file, idle_wait="600"), "SOFASCORE_DEPLOY_LOCK_FD": "8"},
        capture_output=True, text=True, timeout=120,
    )

    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "ведёт не на замок выката" in proc.stderr


@pytest.mark.unit
def test_a_maintenance_that_did_not_come_back_is_not_a_finished_deploy(tmp_path: Path) -> None:
    """Ревью Sol, круг 2. Несошедшееся постусловие возврата паузы обслуживания только
    писалось в журнал, а функция возвращала ноль: выкат заканчивался DONE, обслуживание
    оставалось под паузой навсегда, и воскресный прогон просто не запускался бы."""
    r = _deploy(
        tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE, maint_paused="f",
        pre=lambda rt: (rt.parent / "stub-state" / "maint_unpause_fails").touch(),
    )

    assert r.proc.returncode == 7, r.out
    assert "MANUAL ACTION REQUIRED" in r.log
    assert "DONE" not in r.log


@pytest.mark.unit
def test_the_proof_lands_in_the_state_directory_the_automat_reads(tmp_path: Path) -> None:
    """Ревью Sol, круг 2. Писатель брал каталог из SOFASCORE_RUNTIME_DIR, а читатель —
    из отдельно настраиваемого SOFASCORE_AUTO_STATE_DIR: разъехавшись, автомат читал бы
    вечно чужой файл и молчал бы об этом."""
    alien = tmp_path / "elsewhere"
    alien.mkdir()
    r = _deploy(
        tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE,
        env_extra={"SOFASCORE_AUTO_STATE_DIR": str(alien)},
    )

    assert r.proc.returncode == 0, r.out
    assert (alien / "last-drain.env").exists(), list(alien.iterdir())
    assert not (r.runtime / "auto-deliver" / "last-drain.env").exists()


@pytest.mark.unit
def test_the_proof_is_not_written_into_a_state_directory_that_does_not_exist(tmp_path: Path) -> None:
    """Ревью Sol, круг 2. `mkdir -p` под доказательство учёта воссоздавал бы каталог
    состояния автомата — тот самый, где живут выключатель, маркер незакрытой доставки и
    снимок отката. Потеряв каталог, автомат обязан остановиться fail-closed, а не найти
    свежесозданный пустой и пойти доставлять."""
    r = _deploy(
        tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE,
        pre=lambda rt: (rt / "auto-deliver").rmdir(),
    )

    assert r.proc.returncode == 0, r.out
    assert not (r.runtime / "auto-deliver").exists(), "каталог состояния автомата не создаём"
    assert "каталога состояния автомата нет" in r.log


@pytest.mark.unit
def test_taking_the_lock_never_truncates_the_file_it_points_at(tmp_path: Path) -> None:
    """Путь замка настраиваемый (SOFASCORE_DEPLOY_LOCK), а открытие через `>` обнулило бы
    файл, на который указала опечатка, ещё до flock — в том числе файл боевого дерева."""
    r = _deploy(
        tmp_path, idle_wait="600", after_breaker=_TAIL_AFTER_FAILURE,
        pre=lambda rt: (rt / "deploy.lock").write_text("важные данные\n", encoding="utf-8"),
    )

    assert r.proc.returncode == 0, r.out
    assert (r.runtime / "deploy.lock").read_text(encoding="utf-8") == "важные данные\n"
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
