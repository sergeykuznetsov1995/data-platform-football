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
            *"count(*)"*) echo 0 ;;
            *) echo "unexpected sql: $sql" >&2; exit 9 ;;
          esac
          exit 0
        fi
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-scheduler ] && [ "$3" = airflow ]; then
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
    _write(
        bin_dir / "date",
        '#!/usr/bin/env bash\ncase "$*" in *%H%M*) echo 0000 ;; *) echo 2026-01-01T00:00:00Z ;; esac\n',
        0o755,
    )
    _write(
        bin_dir / "host-python",
        f'#!/usr/bin/env bash\ncase "$*" in *runtime_fingerprint*) echo {DIGEST} ;; esac\nexit 0\n',
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
    workspace = runtime / f"canary-{TAG}"
    _write(workspace / "VERIFIED", "")
    _write(workspace / "candidate.json", json.dumps({"runtime_fingerprint": {"digest": DIGEST}}))
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

    artifact_host = tmp_path / "runtime" / "artifacts" / DIGEST / "proxy_budget_canary.json"
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
    assert [a.split()[5] for a in pools] == [
        "ingest_scraper_pool", "sofascore_history_pool", "sofascore_players_pool"
    ], pools
    assert all(a.split()[6] == "1" for a in pools), pools
    # deploy.sh не пересоздаёт airflow-init, где пулы заводятся впервые: без этого
    # шага задачи полос повисли бы в несуществующем пуле.
    assert min(args.index(a) for a in pools) > max(i for i, a in enumerate(args) if a.startswith("compose "))
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


@pytest.mark.unit
@pytest.mark.parametrize(
    ("break_env", "reason"),
    [
        (
            lambda text, runtime: text.replace(
                f"SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state-history",
                f"SOFASCORE_HISTORY_GW_STATE_HOST_DIR={runtime}/gateway-state",
            ),
            "должны быть разными",
        ),
        (
            lambda text, runtime: text.replace(
                f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/gateway-state-players",
                f"SOFASCORE_PLAYERS_GW_STATE_HOST_DIR={runtime}/nowhere",
            ),
            "недоступен на запись",
        ),
    ],
    ids=["two-lanes-share-one-state-dir", "lane-state-dir-does-not-exist"],
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
