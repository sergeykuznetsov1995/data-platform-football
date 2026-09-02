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
            *"SELECT is_paused"*)
              dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}
              cat "$STATE/paused_$dag" ;;
            *"is_active=true"*) echo 3 ;;
            *"count(*)"*) echo 0 ;;
            *) echo "unexpected sql: $sql" >&2; exit 9 ;;
          esac
          exit 0
        fi
        if [ "$1" = exec ] && [ "$2" = sofascore-airflow-scheduler ] && [ "$3" = airflow ]; then
          case "$5" in pause) echo t > "$STATE/paused_$6" ;; unpause) echo f > "$STATE/paused_$6" ;; esac
          exit 0
        fi
        if [ "$1" = inspect ]; then
          case "$*" in *Health.Status*) echo healthy ;; *HostConfig.Memory*) echo 1073741824 ;; esac
          exit 0
        fi
        exit 0
        ''',
        0o755,
    )
    _write(bin_dir / "systemctl", "#!/usr/bin/env bash\necho active\n", 0o755)
    _write(bin_dir / "chown", "#!/usr/bin/env bash\nexit 0\n", 0o755)
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
    release = tmp_path / "releases" / f"release-{TAG}"
    state_dir = tmp_path / "stub-state"
    bin_dir = tmp_path / "bin"
    state_dir.mkdir()
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
def test_env_loader_strips_quotes_and_never_expands_or_exports(tmp_path: Path) -> None:
    env_file = _write(
        tmp_path / "x.env",
        """\
        # comment
        PLAIN=a b
        SINGLE='[{"host":"h","port":1},{"x":"$HOME"}]'
        DOUBLE="q,{r}"
        EMPTY=
        """,
    )
    script = f"""\
        . {DEPLOY}/env.sh
        sofascore_load_env {env_file}
        printf '%s|%s|%s|%s\\n' "$PLAIN" "$SINGLE" "$DOUBLE" "${{EMPTY-unset}}"
        env | grep -c '^SINGLE=' || true
        """
    proc = subprocess.run(["bash", "-c", textwrap.dedent(script)], capture_output=True, text=True, check=True)
    values, exported = proc.stdout.splitlines()
    assert values == 'a b|[{"host":"h","port":1},{"x":"$HOME"}]|q,{r}|'
    assert exported == "0"
    bad = _write(tmp_path / "bad.env", "NOT A LINE\n")
    proc = subprocess.run(
        ["bash", "-c", f". {DEPLOY}/env.sh; sofascore_load_env {bad}"], capture_output=True, text=True
    )
    assert proc.returncode == 2
