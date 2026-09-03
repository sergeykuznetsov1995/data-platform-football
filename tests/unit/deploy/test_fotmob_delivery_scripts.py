"""Скрипты автомата доставки FotMob против заглушек (#1155, этап 3).

Пути машины и пины выката приходят из env-файла контура через deploy/fotmob/env.sh.
Здесь проверяется, что скрипты действительно берут их оттуда: загрузчик читает файл
как compose (кавычки, CRLF, чужие ключи, отсутствие export), автомат без env-файла не
делает ничего, с выключателем — берёт замок ровно в каталоге состояния из env-файла,
без каталога состояния — пишет в лог из env-файла; b6 в сухом режиме проверяет
дерево по пинам из env-файла; сторож окна ходит к контейнеру и хуку из env-файла.
Docker, pgrep и date подменяются заглушками в PATH.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "fotmob"
ENV_SH = DEPLOY / "env.sh"
AUTO = DEPLOY / "auto_deliver.sh"
B6 = DEPLOY / "b6_deliver.sh"
ALERT = DEPLOY / "window_alert.sh"

REQUIRED = {
    "FOTMOB_RELEASE_ROOT",
    "FOTMOB_CAMPAIGN_DIR",
    "FOTMOB_STATE_DIR",
    "FOTMOB_LOG",
    "FOTMOB_METADB_CONTAINER",
    "FOTMOB_SCHEDULER_CONTAINER",
    "FOTMOB_TG_ENV",
    "FOTMOB_TG_HOOK",
    "FOTMOB_HOST_PYTEST",
    "FOTMOB_TARGET",
    "FOTMOB_ROLLBACK_REF",
    "FOTMOB_ROLLBACK_SHA",
    "FOTMOB_NEW_CODE_FILE",
    "FOTMOB_NEW_CODE_MD5",
}


def _write_env(path: Path, **values: str) -> Path:
    path.write_text("".join(f"{k}={v}\n" for k, v in values.items()), encoding="utf-8")
    return path


def _stub(directory: Path, name: str, body: str) -> None:
    script = directory / name
    script.write_text("#!/bin/bash\n" + body, encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _layout(tmp_path: Path) -> dict[str, str]:
    """Каталоги машины, на которые ссылается env-файл."""
    state = tmp_path / "watchdog" / "state"
    campaign = tmp_path / "campaign"
    state.mkdir(parents=True)
    (campaign / "state").mkdir(parents=True)
    (campaign / "logs").mkdir()
    (tmp_path / "tree").mkdir()
    return {
        "FOTMOB_RELEASE_ROOT": str(tmp_path / "tree"),
        "FOTMOB_CAMPAIGN_DIR": str(campaign),
        "FOTMOB_STATE_DIR": str(state),
        "FOTMOB_LOG": str(tmp_path / "watchdog" / "auto.log"),
        "FOTMOB_METADB_CONTAINER": "stub-metadb",
        "FOTMOB_SCHEDULER_CONTAINER": "stub-scheduler",
        "FOTMOB_TG_ENV": str(tmp_path / "telegram.env"),
        "FOTMOB_TG_HOOK": str(tmp_path / "bin" / "tg-send"),
        "FOTMOB_HOST_PYTEST": "/bin/true",
        "FOTMOB_TARGET": "1" * 40,
        "FOTMOB_ROLLBACK_REF": "2" * 40,
        "FOTMOB_ROLLBACK_SHA": "2" * 40,
        "FOTMOB_NEW_CODE_FILE": "/opt/airflow/scrapers/fotmob/service.py",
        "FOTMOB_NEW_CODE_MD5": "0" * 32,
    }


def _run(script: Path, *args: str, env_file: Path, stubs: Path | None = None, extra_env: dict | None = None,
         cwd: Path | None = None) -> subprocess.CompletedProcess:
    env = {
        "PATH": os.environ["PATH"],
        "HOME": os.environ.get("HOME", "/nonexistent"),
        "FOTMOB_ENV_FILE": str(env_file),
        "LANG": "C.UTF-8",
    }
    if stubs is not None:
        env["PATH"] = f"{stubs}:{env['PATH']}"
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", str(script), *args], env=env, capture_output=True, text=True, timeout=60,
        cwd=str(cwd) if cwd else None,
    )


@pytest.mark.unit
def test_env_loader_reads_the_file_like_compose_and_never_exports(tmp_path: Path) -> None:
    env_file = tmp_path / "fotmob.env"
    env_file.write_bytes(
        b"# comment\r\n"
        b"FOTMOB_TARGET=abc123\r\n"
        b"FOTMOB_LOG='/x/with space/log'\n"
        b"FOTMOB_TG_HOOK=\"/x/q\\\"uote\\\\y\"\n"
    )
    probe = (
        f'. "{ENV_SH}"; fotmob_load_env "{env_file}" || exit 9; '
        'printf "%s|%s|%s|" "$FOTMOB_TARGET" "$FOTMOB_LOG" "$FOTMOB_TG_HOOK"; '
        'env | grep -c "^FOTMOB_TARGET=" || true'
    )
    proc = subprocess.run(
        ["bash", "-c", probe], env={**os.environ, "FOTMOB_TARGET": "stale-exported"},
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == 'abc123|/x/with space/log|/x/q"uote\\y|0\n'

    foreign = _write_env(tmp_path / "foreign.env", FOTMOB_TARGET="a", PATH="/evil")
    proc = subprocess.run(
        ["bash", "-c", f'. "{ENV_SH}"; fotmob_load_env "{foreign}"'], capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 2 and "недопустимый ключ" in proc.stderr


@pytest.mark.unit
def test_auto_deliver_without_env_file_does_nothing(tmp_path: Path) -> None:
    proc = _run(AUTO, env_file=tmp_path / "missing.env")
    assert proc.returncode == 2
    assert "нет env-файла" in proc.stderr
    assert not list(tmp_path.iterdir()), "ни замка, ни лога, ни маркеров без env-файла"


@pytest.mark.unit
def test_auto_deliver_takes_its_lock_in_the_state_dir_from_the_env_file(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    state = Path(values["FOTMOB_STATE_DIR"])
    (state / "fotmob-auto-deliver.off").touch()  # выключатель: автомат стоит и молчит
    proc = _run(AUTO, env_file=env_file)
    assert proc.returncode == 0, proc.stderr
    assert (state / "fotmob-auto-deliver.lock").is_file(), "замок — в каталоге состояния из env-файла"
    assert not Path(values["FOTMOB_LOG"]).exists(), "заглушенный автомат без незакрытой доставки молчит"
    assert not (Path(values["FOTMOB_RELEASE_ROOT"])).joinpath(".git").exists(), "дерево не тронуто"


@pytest.mark.unit
def test_auto_deliver_reports_a_missing_state_dir_into_the_log_from_the_env_file(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    values["FOTMOB_STATE_DIR"] = str(tmp_path / "watchdog" / "gone")
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    proc = _run(AUTO, env_file=env_file)
    assert proc.returncode == 1, proc.stderr
    log = Path(values["FOTMOB_LOG"]).read_text(encoding="utf-8")
    assert "КАТАЛОГ СОСТОЯНИЯ" in log and values["FOTMOB_STATE_DIR"] in log
    assert "АЛЕРТ НЕ ДОСТАВЛЕН (нет " + values["FOTMOB_TG_ENV"] in log, "TG-env берётся из env-файла"


@pytest.mark.unit
def test_auto_deliver_fails_closed_on_a_missing_pin(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    del values["FOTMOB_TARGET"]
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    proc = _run(AUTO, env_file=env_file)
    assert proc.returncode != 0
    assert "FOTMOB_TARGET" in proc.stderr
    assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-auto-deliver.lock").exists()


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


@pytest.mark.unit
def test_b6_check_dry_run_validates_the_tree_against_pins_from_the_env_file(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    repo = tmp_path / "copy"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "a.txt").write_text("a", encoding="utf-8")
    _git(repo, "add", "a.txt")
    _git(repo, "commit", "-q", "-m", "one")
    one = _git(repo, "rev-parse", "HEAD")
    (repo / "a.txt").write_text("b", encoding="utf-8")
    _git(repo, "commit", "-q", "-am", "two")
    two = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "--detach", one)

    values.update(FOTMOB_TARGET=two, FOTMOB_ROLLBACK_REF=one, FOTMOB_ROLLBACK_SHA=one[:8])
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    # TREE ≠ FOTMOB_RELEASE_ROOT → сухой прогон: без окна, docker и замка.
    proc = _run(B6, "check", env_file=env_file, extra_env={"TREE": str(repo)})
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "сухой прогон на копии" in proc.stdout
    assert f"дерево чистое на {one[:8]}, цель {two} доступна" in proc.stdout
    assert "check ПРОЙДЕН" in proc.stdout
    assert _git(repo, "rev-parse", "HEAD") == one, "check ничего не переключает"

    # Пин отката не совпадает с HEAD копии — отказ до любого checkout.
    values.update(FOTMOB_ROLLBACK_SHA=two)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    proc = _run(B6, "check", env_file=env_file, extra_env={"TREE": str(repo)})
    assert proc.returncode == 1
    assert "ОТКАЗ: HEAD не" in proc.stderr


@pytest.mark.unit
def test_b6_refuses_a_production_apply_outside_the_automaton(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    proc = _run(B6, "apply", env_file=env_file)
    assert proc.returncode == 1
    assert "ручная доставка на бой запрещена" in proc.stderr


def _date_stub(stubs: Path, hhmm: str) -> None:
    _stub(
        stubs, "date",
        'case "$*" in\n'
        f'  *"+%H%M") echo {hhmm} ;;\n'
        '  *"+%F") echo 2026-01-01 ;;\n'
        '  *"+%H:%M") echo 20:00 ;;\n'
        '  *) exec /bin/date "$@" ;;\n'
        'esac\n',
    )


@pytest.mark.unit
def test_window_alert_outside_the_window_touches_nothing(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "1200")
    _stub(stubs, "docker", 'echo "docker must not be called" >&2; exit 99\n')
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0, proc.stderr
    assert proc.stderr == ""
    assert not list(Path(values["FOTMOB_STATE_DIR"]).iterdir())


@pytest.mark.unit
def test_window_alert_inside_an_open_window_uses_container_and_hook_from_the_env_file(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "pgrep", "exit 1\n")
    calls = tmp_path / "docker.calls"
    _stub(stubs, "docker", f'echo "$*" >> "{calls}"; echo 0\n')
    sent = tmp_path / "tg.sent"
    _stub(stubs, "tg-send", f'printf "%s\\n" "$1" >> "{sent}"\n')
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0, proc.stderr
    assert calls.read_text(encoding="utf-8").startswith("exec stub-metadb psql")
    message = sent.read_text(encoding="utf-8")
    assert "окно доставки ОТКРЫТО" in message and values["FOTMOB_LOG"] in message
    assert (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-window-alert-2026-01-01").exists()
    # Второй тик того же дня молчит.
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0
    assert sent.read_text(encoding="utf-8") == message
