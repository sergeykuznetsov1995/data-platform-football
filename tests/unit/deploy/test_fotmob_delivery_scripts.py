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
import shutil
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "fotmob"
ENV_SH = DEPLOY / "env.sh"
AUTO = DEPLOY / "auto_deliver.sh"
B6 = DEPLOY / "b6_deliver.sh"
ALERT = DEPLOY / "window_alert.sh"
# Фиксированный PATH автомата (cron-гигиена): заглушки из PATH окружения он не видит,
# поэтому прогон идёт на установленной копии с этой единственной правкой.
PATH_LINE = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"

REQUIRED = {
    "FOTMOB_RELEASE_ROOT",
    "FOTMOB_CAMPAIGN_DIR",
    "FOTMOB_STATE_DIR",
    "FOTMOB_LOG",
    "FOTMOB_METADB_CONTAINER",
    "FOTMOB_SCHEDULER_CONTAINER",
    "FOTMOB_TG_ENV",
    "FOTMOB_HOST_PYTEST",
    "FOTMOB_TARGET",
    "FOTMOB_ROLLBACK_REF",
    "FOTMOB_ROLLBACK_SHA",
    "FOTMOB_NEW_CODE_FILE",
    "FOTMOB_NEW_CODE_MD5",
}


RUNTIME_MODULES = (DEPLOY / "release-1292.env").read_text().split("FOTMOB_RUNTIME_HASHES=")[1].splitlines()[0].split("|")


PACKAGE_KEYS = {
    "FOTMOB_TARGET", "FOTMOB_ROLLBACK_REF", "FOTMOB_ROLLBACK_SHA",
    "FOTMOB_NEW_CODE_FILE", "FOTMOB_NEW_CODE_MD5", "FOTMOB_REPOSITORY_MD5",
    "FOTMOB_RUNTIME_HASHES", "FOTMOB_REQUIRED_MODULES",
}


def _write_env(path: Path, **values: str) -> Path:
    if "FOTMOB_RELEASE_ROOT" in values:
        pins = {key: values.pop(key) for key in PACKAGE_KEYS if key in values}
        package = path.with_suffix(".release")
        _write_env(package, **pins)
        values["FOTMOB_DELIVERY_PACKAGE"] = str(package)
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
        "FOTMOB_HOST_PYTEST": "/bin/true",
        "FOTMOB_VALIDATION_ROOT": str(tmp_path / "validation"),
        "FOTMOB_REPOSITORY_MD5": "0" * 32,
        "FOTMOB_RUNTIME_HASHES": "|".join("a" * 64 + " " + item.split(" ", 1)[1] for item in RUNTIME_MODULES),
        "FOTMOB_REQUIRED_MODULES": "|".join(item.split(" ", 1)[1] for item in RUNTIME_MODULES),
        # Короткие SHA, как `git rev-parse --short`: автомат сравнивает пины с ним по префиксу.
        "FOTMOB_TARGET": "11111111",
        "FOTMOB_ROLLBACK_REF": "22222222",
        "FOTMOB_ROLLBACK_SHA": "22222222",
        "FOTMOB_NEW_CODE_FILE": "/opt/airflow/scrapers/fotmob/service.py",
        "FOTMOB_NEW_CODE_MD5": "0" * 32,
    }


BARRIER_PRODUCERS = (
    "dag_backfill_fotmob", "dag_collect_fotmob_players", "dag_orchestrate_fotmob",
    "dag_refresh_fotmob", "dag_refresh_fotmob_c1", "dag_trigger_fotmob_daily",
)


def _barrier_stub_cases(tmp_path: Path) -> str:
    external = tmp_path / "external.airflowignore"
    external.write_text("external block-list\n", encoding="utf-8")
    snapshot = "\n".join(f"{dag}|{'t' if dag == 'dag_backfill_fotmob' else 'f'}" for dag in BARRIER_PRODUCERS)
    return (
        f'  *"SELECT dag_id, is_paused"*) printf "%s\\n" "{snapshot}" ;;\n'
        '  *"UPDATE dag SET is_paused=true"*) echo 0 ;;\n'
        '  *"BEGIN;"*) echo 5 ;;\n'
        f'  *inspect*) echo "{external}" ;;\n'
        f'  *"md5sum /opt/airflow/dags/.airflowignore"*) md5sum "{external}" ;;\n'
    )


def _install(tmp_path: Path, stubs: Path) -> Path:
    """Копия deploy/fotmob как после `install` рунбука (скрипты рядом, автомат зовёт
    b6_deliver.sh по соседству) с ровно одной правкой: фиксированный PATH автомата
    получает впереди каталог заглушек. Возвращает путь установленного автомата."""
    libexec = tmp_path / "libexec"
    libexec.mkdir()
    for src in (AUTO, B6, ALERT, ENV_SH, DEPLOY / "delivery_barrier.sh"):
        text = src.read_text(encoding="utf-8")
        if src == AUTO:
            assert text.count(PATH_LINE) == 1, "автомат перестал фиксировать PATH — правь тест"
            text = text.replace(PATH_LINE, PATH_LINE.replace("export PATH=", f"export PATH={stubs}:"))
        (libexec / src.name).write_text(text, encoding="utf-8")
        (libexec / src.name).chmod(0o755)
    return libexec / AUTO.name


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
        b"FOTMOB_HOST_PYTEST=\"/x/q\\\"uote\\\\y\"\n"
    )
    probe = (
        f'. "{ENV_SH}"; fotmob_load_env "{env_file}" || exit 9; '
        'printf "%s|%s|%s|%s|" "$FOTMOB_TARGET" "$FOTMOB_LOG" "$FOTMOB_HOST_PYTEST" "${FOTMOB_ROLLBACK_SHA-unset}"; '
        'env | grep -c "^FOTMOB_TARGET=" || true'
    )
    # Экспортированный FOTMOB_ROLLBACK_SHA ключа в файле не имеет — после загрузки его
    # не должно быть вовсе: файл — единственный источник, не «файл поверх окружения».
    proc = subprocess.run(
        ["bash", "-c", probe],
        env={**os.environ, "FOTMOB_TARGET": "stale-exported", "FOTMOB_ROLLBACK_SHA": "deadbeef"},
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == 'abc123|/x/with space/log|/x/q"uote\\y|unset|0\n'

    for name, values, message in (
        ("foreign", {"FOTMOB_TARGET": "a", "PATH": "/evil"}, "недопустимый ключ"),
        # Compose подставил бы ${X} и отрезал бы ` # …` — загрузчик такое отвергает, а не
        # читает по-своему.
        ("interp", {"FOTMOB_LOG": "/x/${HOME}/log"}, "подстановки не поддерживаются"),
        ("inline", {"FOTMOB_LOG": "/x/log # comment"}, "inline-комментарий"),
    ):
        bad = _write_env(tmp_path / f"{name}.env", **values)
        proc = subprocess.run(
            ["bash", "-c", f'. "{ENV_SH}"; fotmob_load_env "{bad}"'], capture_output=True, text=True, check=False,
        )
        assert proc.returncode == 2 and message in proc.stderr, (name, proc.stderr)


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
    assert "пакет доставки" in (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-pending-alert").read_text()
    # Значение из окружения (cron-строка, забытый export) файл без ключа не заменяет.
    proc = _run(AUTO, env_file=env_file, extra_env={"FOTMOB_TARGET": "deadbee"})
    assert proc.returncode != 0
    assert "FOTMOB_TARGET" in proc.stderr
    assert "ПАКЕТ ДОСТАВКИ" in Path(values["FOTMOB_LOG"]).read_text()


@pytest.mark.unit
@pytest.mark.parametrize("key", ["FOTMOB_TARGET", "FOTMOB_ROLLBACK_SHA"])
@pytest.mark.parametrize(
    "bad", ["1" * 40, "abc12", "deadbeef1", "DEADBEE", "deploy/fotmob-b6-master"],
    ids=["full-sha", "too-short", "nine-hex", "upper", "ref-name"],
)
def test_scripts_refuse_a_pin_that_is_not_a_short_sha(tmp_path: Path, key: str, bad: str) -> None:
    """Пины сравниваются с `git rev-parse --short HEAD` по префиксу: полный SHA (или имя
    ветки) никогда не совпал бы, и исправное дерево считалось бы незаконным — лучше не
    стартовать. Оба скрипта, оба сверяемых пина."""
    values = _layout(tmp_path)
    values[key] = bad
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    for script in (AUTO, B6):
        proc = _run(script, "check", env_file=env_file)
        assert proc.returncode == 2, (script.name, proc.stderr)
        assert key in proc.stderr and "короткий SHA" in proc.stderr, (script.name, proc.stderr)
        assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-b6-inflight").exists()


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


def _two_commits(repo: Path) -> tuple[str, str]:
    """Репозиторий с коммитами one → two, HEAD отцеплен на one (откат), two — цель."""
    repo.mkdir(exist_ok=True)
    _git(repo, "init", "-q")
    (repo / "a.txt").write_text("a", encoding="utf-8")
    (repo / "dags").mkdir()
    (repo / "dags/.airflowignore").write_text("one\n")
    _git(repo, "add", "a.txt", "dags/.airflowignore")
    _git(repo, "commit", "-q", "-m", "one")
    one = _git(repo, "rev-parse", "HEAD")
    (repo / "a.txt").write_text("b", encoding="utf-8")
    (repo / "dags/.airflowignore").write_text("two\n")
    _git(repo, "commit", "-q", "-am", "two")
    two = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "--detach", one)
    return one, two


@pytest.mark.unit
def test_b6_check_dry_run_validates_the_tree_against_pins_from_the_env_file(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    repo = tmp_path / "copy"
    one, two = _two_commits(repo)

    values.update(FOTMOB_TARGET=two[:8], FOTMOB_ROLLBACK_REF=one[:8], FOTMOB_ROLLBACK_SHA=one[:8])
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    # TREE ≠ FOTMOB_RELEASE_ROOT → сухой прогон: без окна, docker и замка.
    proc = _run(B6, "check", env_file=env_file, extra_env={"TREE": str(repo)})
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "сухой прогон на копии" in proc.stdout
    assert f"дерево чистое на {one[:8]}, цель {two[:8]} доступна" in proc.stdout
    assert "check ПРОЙДЕН" in proc.stdout
    assert _git(repo, "rev-parse", "HEAD") == one, "check ничего не переключает"

    # Пин отката не совпадает с HEAD копии — отказ до любого checkout.
    values.update(FOTMOB_ROLLBACK_SHA=two[:8])
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


@pytest.mark.unit
@pytest.mark.parametrize("mutate_validation", [False, True])
def test_auto_deliver_hands_lock_and_nonce_to_b6_and_accepts_against_stubs(tmp_path: Path, mutate_validation: bool) -> None:
    """Настоящий автомат против заглушек: окно — из date, живые раннеры — из pgrep, метабаза
    и контейнер — из docker. Автомат берёт замок в каталоге состояния из env-файла (по
    СЫРОМУ пути, с хвостовым `/`), выписывает одноразовый пропуск и зовёт СОСЕДНИЙ
    b6_deliver.sh; тот принимает эстафету по унаследованному fd 9 и пропуску, переключает
    дерево на цель; автомат видит приёмку (md5 цели в контейнере, перечитанные даги, ноль
    ошибок импорта), записывает принятый SHA и поднимает драйвер кампании из каталога,
    указанного в env-файле. Ни один путь и ни один пин не приходят иначе как из файла."""
    values = _layout(tmp_path)
    repo = Path(values["FOTMOB_RELEASE_ROOT"])
    one, two = _two_commits(repo)
    # Пины — ровно то, что печатает `git rev-parse --short` в ЭТОМ дереве: автомат
    # сравнивает их с коротким HEAD по префиксу, пин длиннее не совпал бы никогда.
    one_short = _git(repo, "rev-parse", "--short", one)
    two_short = _git(repo, "rev-parse", "--short", two)
    state = Path(values["FOTMOB_STATE_DIR"])
    campaign = Path(values["FOTMOB_CAMPAIGN_DIR"])
    md5 = "d41d8cd98f00b204e9800998ecf8427e"
    values.update(
        FOTMOB_STATE_DIR=str(state) + "/",
        FOTMOB_TARGET=two_short, FOTMOB_ROLLBACK_REF=one_short, FOTMOB_ROLLBACK_SHA=one_short,
        FOTMOB_NEW_CODE_MD5=md5,
    )
    validation = Path(values["FOTMOB_VALIDATION_ROOT"])
    _git(tmp_path, "clone", "-q", str(repo), str(validation))
    _git(validation, "checkout", "-q", "--detach", two)
    values["FOTMOB_REPOSITORY_MD5"] = md5
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    # `sleep 5` — ожидание старта драйвера кампании: заглушке драйвера нужно мгновение,
    # чтобы оставить след; остальные ожидания (приёмка, 90 с) — мгновенны.
    _stub(stubs, "sleep", '[ "$1" = 5 ] && exec /bin/sleep 1\nexit 0\n')
    if mutate_validation:
        _stub(stubs, "sleep", f'[ "$1" = 90 ] && touch "{validation}/changed"\n'
              '[ "$1" = 5 ] && exec /bin/sleep 1\nexit 0\n')
    driver_started = campaign / "state" / "driver-started"
    _stub(campaign, "driver.sh", f'touch "{driver_started}"\nexit 0\n')
    _stub(
        stubs, "pgrep",
        f'case "$*" in *driver.sh*) [ -e "{driver_started}" ] && exit 0; exit 1 ;; *) exit 1 ;; esac\n',
    )
    calls = tmp_path / "docker.calls"
    _stub(
        stubs, "docker",
        f'echo "$*" >> "{calls}"\n'
        'case "$*" in\n'
        + _barrier_stub_cases(tmp_path) +
        '  *pgrep*) exit 1 ;;\n'                    # юнит кампании в контейнере не жив
        f'  *md5sum*) echo "{md5}  file" ;;\n'      # целевой модуль виден из контейнера
        '  *sha256sum*) echo "' + 'a' * 64 + '  file" ;;\n'
        '  *"last_parsed_time >"*) echo 7 ;;\n'
        '  *"FROM dag WHERE"*) echo 1 ;;\n'         # fotmob-даг перечитан планировщиком
        '  *"count(*)"*) echo 0 ;;\n'               # ни активных ранов, ни ошибок импорта
        '  *) echo "dag_x | f" ;;\n'
        'esac\n',
    )
    auto = _install(tmp_path, stubs)
    proc = _run(auto, env_file=env_file, stubs=stubs)
    if mutate_validation:
        assert proc.returncode == 1, proc.stderr + proc.stdout
        assert _git(repo, "rev-parse", "HEAD") == one
        assert "ОТКАЗ: validation tree грязное" in Path(values["FOTMOB_LOG"]).read_text()
        assert "B6 ДОСТАВЛЕН" not in Path(values["FOTMOB_LOG"]).read_text()
        return
    assert proc.returncode == 0, proc.stderr + proc.stdout
    log = Path(values["FOTMOB_LOG"]).read_text(encoding="utf-8")
    assert "ОКНО ОТКРЫТО" in log and f"{auto.parent}/b6_deliver.sh apply" in log, "b6 — сосед автомата"
    assert "окно открыто" in log and "B6 ДОСТАВЛЕН" in log, "вывод b6 уходит в лог автомата"
    assert (state / "fotmob-recovery.env").read_bytes() == env_file.with_suffix(".release").read_bytes()
    assert "приёмка подтверждена" in log and f"ДОСТАВЛЕНО: HEAD={two_short}" in log
    assert "кампания истории запущена" in log
    assert _git(repo, "rev-parse", "HEAD") == two
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "deploy/fotmob-b6-master"
    assert (state / "fotmob-b6-accepted").read_text(encoding="utf-8").strip() == two_short
    # Момент доставки закреплён отметкой — её читает вторая фаза приёмки (#1256).
    delivered_at = (state / f"fotmob-delivered-at-{two_short}").read_text(encoding="utf-8").strip()
    assert delivered_at.endswith("Z") and delivered_at[:4].isdigit() and "T" in delivered_at, delivered_at
    assert not (state / "fotmob-deliver-nonce").exists(), "пропуск одноразовый"
    assert not (state / "fotmob-b6-inflight").exists()
    assert (state / "fotmob-auto-deliver-attempted-2026-01-01").exists()
    assert (state / "fotmob-campaign-started").exists()
    assert (campaign / "state" / "campaign_enabled").exists()
    docker_log = calls.read_text(encoding="utf-8")
    assert "exec stub-metadb psql" in docker_log
    assert "exec stub-scheduler md5sum /opt/airflow/scrapers/fotmob/service.py" in docker_log
    # Telegram недоступен (нет TG-env) — исход не потерян, а отложен в очередь каталога
    # состояния из env-файла.
    assert "FotMob B6 доставлен" in (state / "fotmob-pending-alert").read_text(encoding="utf-8")
    assert not (state / "fotmob-auto-deliver.off").exists()

def _date_stub(stubs: Path, hhmm: str) -> None:
    _stub(
        stubs, "date",
        'case "$*" in\n'
        f'  *"+%H%M") echo {hhmm} ;;\n'
        f'  *"+%H%M%S") echo {hhmm}00 ;;\n'
        '  *"+%F") echo 2026-01-01 ;;\n'
        '  *"+%H:%M") echo 20:00 ;;\n'
        '  *) exec /bin/date "$@" ;;\n'
        'esac\n',
    )


def _tg_env(values: dict[str, str]) -> None:
    Path(values["FOTMOB_TG_ENV"]).write_text("TELEGRAM_BOT_TOKEN=t0k\nTELEGRAM_CHAT_ID=42\n", encoding="utf-8")


def _curl_stub(stubs: Path, sent: Path, reply: str) -> None:
    _stub(stubs, "curl", f'printf "%s\\n" "$*" >> "{sent}"; printf "%s" \'{reply}\'\n')


@pytest.mark.unit
def test_window_alert_outside_the_window_touches_nothing(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    _tg_env(values)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "1200")
    _stub(stubs, "docker", 'echo "docker must not be called" >&2; exit 99\n')
    _stub(stubs, "curl", 'echo "curl must not be called" >&2; exit 99\n')
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0, proc.stderr
    assert proc.stderr == ""
    assert not list(Path(values["FOTMOB_STATE_DIR"]).iterdir())


@pytest.mark.unit
def test_window_alert_without_the_telegram_env_is_loud_and_sets_no_latch(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 2
    assert values["FOTMOB_TG_ENV"] in proc.stderr
    assert not list(Path(values["FOTMOB_STATE_DIR"]).iterdir()), "без токена защёлка не ставится"


@pytest.mark.unit
def test_window_alert_never_creates_the_state_dir(tmp_path: Path) -> None:
    """Пустой каталог состояния, созданный сторожем перед тиком автомата, снял бы его
    fail-closed проверку (нет выключателя, маркеров и суточной защёлки)."""
    values = _layout(tmp_path)
    _tg_env(values)
    values["FOTMOB_STATE_DIR"] = str(tmp_path / "watchdog" / "gone")
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "curl", 'echo "curl must not be called" >&2; exit 99\n')
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 2
    assert values["FOTMOB_STATE_DIR"] in proc.stderr
    assert not Path(values["FOTMOB_STATE_DIR"]).exists()


@pytest.mark.unit
def test_window_alert_inside_an_open_window_sends_via_the_env_token_and_latches_once(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    _tg_env(values)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "pgrep", "exit 1\n")
    calls = tmp_path / "docker.calls"
    _stub(stubs, "docker", f'echo "$*" >> "{calls}"; echo 0\n')
    sent = tmp_path / "tg.sent"
    _curl_stub(stubs, sent, '{"ok":true,"result":{}}')
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0, proc.stderr
    assert calls.read_text(encoding="utf-8").startswith("exec stub-metadb psql")
    request = sent.read_text(encoding="utf-8")
    assert "api.telegram.org/bott0k/sendMessage" in request and "chat_id=42" in request
    assert "окно доставки ОТКРЫТО" in request and values["FOTMOB_LOG"] in request
    assert (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-window-alert-2026-01-01").exists()
    # Второй тик того же дня молчит.
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0
    assert sent.read_text(encoding="utf-8") == request


@pytest.mark.unit
@pytest.mark.parametrize("reply", ['{"ok":false,"error_code":429}', ""], ids=["api-refused", "curl-failed"])
def test_window_alert_sets_no_latch_when_telegram_did_not_accept(tmp_path: Path, reply: str) -> None:
    """Живой внешний хук возвращал 0 и без конфигурации, и при упавшем curl — поэтому
    сторож судит по ответу API сам, а защёлку ставит только на "ok":true."""
    values = _layout(tmp_path)
    _tg_env(values)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "pgrep", "exit 1\n")
    _stub(stubs, "docker", "echo 0\n")
    sent = tmp_path / "tg.sent"
    _curl_stub(stubs, sent, reply)
    proc = _run(ALERT, env_file=env_file, stubs=stubs)
    assert proc.returncode == 3
    assert "не доставлен" in proc.stderr
    assert not list(Path(values["FOTMOB_STATE_DIR"]).iterdir()), "недоставленный алерт защёлку не ставит"
    # Следующий тик пробует снова, а не молчит до завтра.
    _run(ALERT, env_file=env_file, stubs=stubs)
    assert sent.read_text(encoding="utf-8").count("\n") == 2


@pytest.mark.unit
def test_auto_deliver_shuts_itself_off_without_the_campaign_runner_dir(tmp_path: Path) -> None:
    """Раннер кампании истории живёт вне репозитория; без его каталога (из env-файла)
    автомат не доставляет вслепую, а глушит себя и говорит об этом в лог."""
    values = _layout(tmp_path)
    shutil.rmtree(values["FOTMOB_CAMPAIGN_DIR"])
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    proc = _run(AUTO, env_file=env_file)
    assert proc.returncode == 1, proc.stderr
    log = Path(values["FOTMOB_LOG"]).read_text(encoding="utf-8")
    assert "КАТАЛОГ КАМПАНИИ" in log and values["FOTMOB_CAMPAIGN_DIR"] in log
    state = Path(values["FOTMOB_STATE_DIR"])
    assert (state / "fotmob-auto-deliver.off").exists(), "автомат глушит себя"
    assert "каталог кампании" in (state / "fotmob-pending-alert").read_text(encoding="utf-8")


# --- Вторая фаза приёмки: исход первой волны на новом коде (#1256) ------------
# Дерево уже стоит на цели и приёмка первой фазы подтверждена заглушками (целевой
# md5 в контейнере, даги перечитаны, ошибок импорта нет) — автомат доходит до фазы 2
# и судит по волнам, которые отдаёт заглушка метабазы.

WAVE_T = "2026-09-08T00:00:00Z"
WAVE_TC = "20260908T000000Z"


def _phase2_world(tmp_path: Path, rows: str, runner_err: str = "", writer_alive: bool = False,
                  log_mode: str = "ok", deliver_marker: bool = False) -> dict:
    values = _layout(tmp_path)
    repo = Path(values["FOTMOB_RELEASE_ROOT"])
    one, two = _two_commits(repo)
    one_short = _git(repo, "rev-parse", "--short", one)
    two_short = _git(repo, "rev-parse", "--short", two)
    # Дерево на ЦЕЛИ: доставка уже состоялась, автомат входит в фазу 2 с delivered=1.
    _git(repo, "checkout", "-q", "--detach", two)
    md5 = "d41d8cd98f00b204e9800998ecf8427e"
    values.update(
        FOTMOB_TARGET=two_short, FOTMOB_ROLLBACK_REF=one_short, FOTMOB_ROLLBACK_SHA=one_short,
        FOTMOB_NEW_CODE_MD5=md5,
    )
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    state = Path(values["FOTMOB_STATE_DIR"])
    # Момент доставки — из строки лога прошлой доставки (В1 доставлена автоматом,
    # у которого отметки $STATE/fotmob-delivered-at-<sha> ещё не было).
    log = Path(values["FOTMOB_LOG"])
    log.parent.mkdir(parents=True, exist_ok=True)
    if deliver_marker:
        # Момент доставки закреплён отметкой — строки в логе нет вовсе (ротация).
        log.write_text("", encoding="utf-8")
        (state / f"fotmob-delivered-at-{two_short}").write_text(WAVE_T + "\n", encoding="utf-8")
    else:
        log.write_text(f"{WAVE_T} ДОСТАВЛЕНО: HEAD={two_short}\n", encoding="utf-8")
    (state / "fotmob-b6-accepted").write_text(two_short + "\n", encoding="utf-8")

    stubs = tmp_path / "bin"
    stubs.mkdir()
    _stub(stubs, "sleep", '[ "$1" = 5 ] && exec /bin/sleep 1\nexit 0\n')
    driver_started = Path(values["FOTMOB_CAMPAIGN_DIR"]) / "state" / "driver-started"
    _stub(Path(values["FOTMOB_CAMPAIGN_DIR"]), "driver.sh", f'touch "{driver_started}"\nexit 0\n')
    _pgrep_stub(stubs, driver_started, writer_alive)
    rows_file = tmp_path / "waves.txt"
    rows_file.write_text(rows + ("\n" if rows else ""), encoding="utf-8")
    # Task-логи волн лежат на диске так же, как в контейнере, а заглушка docker
    # ИСПОЛНЯЕТ ту самую команду, которую автомат посылает внутрь, лишь подменив
    # корень путей. Иначе тест проверял бы заглушку, а не разбор кодов возврата:
    # различить «совпадений нет» и «лог не прочитан» можно только на настоящем
    # конвейере `ls || exit 3; grep | cut`.
    logs_root = tmp_path / "airflow-logs"
    for line in [r for r in rows.split("\n") if r.strip()]:
        run_id = line.split("|", 1)[0]
        attempt = logs_root / f"dag_id=dag_ingest_fotmob/run_id={run_id}/task_id=scrape_fotmob_data"
        if log_mode == "missing":
            continue
        attempt.mkdir(parents=True)
        if log_mode == "unreadable":
            # Каталог на месте файла: `ls` доволен, `grep` возвращает 2 — ровно тот
            # сбой чтения, который без pipefail выглядел бы «ошибки раннера нет».
            (attempt / "attempt=1.log").mkdir()
        else:
            (attempt / "attempt=1.log").write_text(
                "2026-09-08 00:00:11 INFO wave started\n" + runner_err, encoding="utf-8"
            )
    calls = tmp_path / "docker.calls"
    _stub(
        stubs, "docker",
        f'echo "$*" >> "{calls}"\n'
        'case "$*" in\n'
        + _barrier_stub_cases(tmp_path) +
        '  *pgrep*) exit 1 ;;\n'
        f'  *md5sum*) echo "{md5}  file" ;;\n'
        # Команду чтения task-лога исполняем по-настоящему, подменив только корень путей.
        '  *__main__*)\n'
        '    payload=$(printf "%s" "${@: -1}" | '
        f'sed "s|/opt/airflow/logs|{logs_root}|g")\n'
        '    exec bash -c "$payload" ;;\n'
        '  *"FROM dag WHERE"*) echo 1 ;;\n'             # даг перечитан планировщиком
        '  *"count(*)"*) echo 0 ;;\n'                   # import_error и активные раны
        # Список волн фазы 2 отдаём ТОЛЬКО на запрос нужной формы: иначе тест не
        # заметил бы ни потерянного отсева ручных прогонов, ни отвязки от момента
        # доставки, ни утраты разбора conf, ни сбитого порядка.
        '  *"FROM dag_run WHERE"*)\n'
        '    if [[ "$*" == *"run_id LIKE \'fotmob_orchestrated"* '
        '&& "$*" == *"regexp_match"* '
        f'&& "$*" == *"start_date > \'{WAVE_T}\'"* '
        '&& "$*" == *"ORDER BY start_date;"* ]]; then\n'
        # Строки задают режим conf. SQL-заглушка сохраняет только те режимы,
        # которые реальный regexp_match распознал бы в conf: неизвестный → пусто.
        '      modes=$(printf "%s" "${@: -1}" | '
        'sed -n "s/.*mode.{0,12}(\\([^)]*\\)).*/\\1/p")\n'
        '      while IFS="|" read -r run state started mode; do\n'
        '        [[ "$mode" =~ ^($modes)$ ]] || mode=""\n'
        '        printf "%s|%s|%s|%s\\n" "$run" "$state" "$started" "$mode"\n'
        f'      done < "{rows_file}"\n'
        '    else\n'
        '      echo "ЗАПРОС-ВОЛН-НЕ-ТОЙ-ФОРМЫ"\n'
        '    fi ;;\n'
        '  *"SELECT 1;"*) echo 1 ;;\n'                  # метабаза жива
        '  *) echo "dag_x | f" ;;\n'
        'esac\n',
    )
    sent = tmp_path / "tg.sent"
    _tg_env(values)
    _curl_stub(stubs, sent, '{"ok":true}')
    return {
        "auto": _install(tmp_path, stubs), "env_file": env_file, "values": values, "repo": repo,
        "state": state, "stubs": stubs, "sent": sent, "one": one, "two": two,
        "one_short": one_short, "two_short": two_short, "log": log,
        "driver_started": driver_started,
    }


def _pgrep_stub(stubs: Path, driver_started: Path, writer_alive: bool) -> None:
    _stub(
        stubs, "pgrep",
        f'case "$*" in\n'
        f'  *driver.sh*) [ -e "{driver_started}" ] && exit 0; exit 1 ;;\n'
        f'  *run_fotmob_scrape*) exit {0 if writer_alive else 1} ;;\n'
        '  *) exit 1 ;;\n'
        'esac\n',
    )


@pytest.mark.unit
def test_phase2_accepts_the_first_green_refresh_wave(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__aaa|success|2026-09-08T00:00:10|refresh")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    verdict = w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}"
    assert verdict.is_file(), sorted(p.name for p in w["state"].iterdir())
    assert "accepted" in verdict.read_text(encoding="utf-8")
    sent = w["sent"].read_text(encoding="utf-8")
    assert "первая волна актуалки" in sent and "mode=refresh" in sent and "не легла" in sent
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"], "зелёная волна ничего не откатывает"
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["refresh", "daily"])
@pytest.mark.parametrize("state", ["success", "failed"])
def test_phase2_skips_green_transfers_and_judges_the_next_current_wave(
    tmp_path: Path, mode: str, state: str,
) -> None:
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__transfer|success|2026-09-08T00:00:10|transfers\n"
        f"fotmob_orchestrated__current|{state}|2026-09-08T04:00:00|{mode}",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == (0 if state == "success" else 1), proc.stderr + proc.stdout
    verdict = w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}"
    assert verdict.is_file(), w["log"].read_text(encoding="utf-8")
    assert "fotmob_orchestrated__current" in verdict.read_text(encoding="utf-8")
    assert "fotmob-wave-mode-unknown-fotmob_orchestrated__transfer" not in {
        path.name for path in w["state"].iterdir()
    }
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two" if state == "success" else "one"]


@pytest.mark.unit
def test_phase2_green_transfers_alone_do_not_accept_current(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__transfer|success|2026-09-08T00:00:10|transfers")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists()
    assert not (w["state"] / "fotmob-wave-mode-unknown-fotmob_orchestrated__transfer").exists()
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]


@pytest.mark.unit
def test_phase2_failed_transfers_do_not_accept_a_later_green_current(tmp_path: Path) -> None:
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__transfer|failed|2026-09-08T00:00:10|transfers\n"
        "fotmob_orchestrated__current|success|2026-09-08T04:00:00|refresh",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    verdict = w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}"
    assert "fotmob_orchestrated__transfer rollback" in verdict.read_text(encoding="utf-8")
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert "красная волна трансферов" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
def test_phase2_unknown_mode_still_blocks_a_later_current_verdict(tmp_path: Path) -> None:
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__unknown|success|2026-09-08T00:00:10|future_mode\n"
        "fotmob_orchestrated__current|failed|2026-09-08T04:00:00|refresh",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists()
    assert (w["state"] / "fotmob-wave-mode-unknown-fotmob_orchestrated__unknown").is_file()
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["refresh", "daily", "backfill", "transfers", "future_mode"])
def test_phase2_rolls_back_on_a_runner_error_when_no_writer_is_alive(tmp_path: Path, mode: str) -> None:
    w = _phase2_world(
        tmp_path,
        f"fotmob_orchestrated__bbb|failed|2026-09-08T00:00:10|{mode}",
        runner_err="[ERROR] __main__: semantic batch has 1 stored rows; expected either 0 or 2\n",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"], "бой вернулся на откатный пин"
    assert (w["state"] / "fotmob-auto-deliver.off").is_file(), "защёлка живёт сутки — без .off тот же пин поехал бы снова"
    assert not (w["state"] / f"fotmob-wave-rollback-pending-{w['two_short']}-{WAVE_TC}").exists()
    verdict = w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}"
    assert "rollback" in verdict.read_text(encoding="utf-8")
    sent = w["sent"].read_text(encoding="utf-8")
    assert "красная первая волна" in sent and "semantic batch" in sent
    assert "Откат подтверждён" in sent
    # Выключатель ставится ДО дерева: смерть между checkout и set_off оставила бы
    # автомат взведённым, а pending — недостижимым (delivered=0 в фазу не заходит).
    log = w["log"].read_text(encoding="utf-8")
    assert log.index("выключатель поставлен") < log.index("дерево переключено на")
    assert not (w["state"] / "fotmob-b6-accepted").exists(), "принятым остаётся только подтверждённый SHA"


@pytest.mark.unit
def test_phase2_reports_a_red_history_wave_without_rolling_back(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__ccc|failed|2026-09-08T00:00:10|backfill")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"], "краснота истории откат не запускает"
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists(), "вердикт выносит только актуалка"
    assert (w["state"] / "fotmob-wave-history-red-fotmob_orchestrated__ccc").is_file()
    sent = w["sent"].read_text(encoding="utf-8")
    assert "красная волна истории" in sent and "не откат" in sent


@pytest.mark.unit
def test_phase2_rolls_back_red_current_without_guessing_its_cause(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__ddd|failed|2026-09-08T00:00:10|daily")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    sent = w["sent"].read_text(encoding="utf-8")
    assert "ошибки раннера нет" in sent and "причина не установлена" in sent
    assert "no_progress_failure" not in sent and "вероятен отказ источника" not in sent
    assert "отчёт волны" in sent


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["refresh", "daily", "backfill", "transfers", "future_mode"])
def test_phase2_waits_for_the_gap_between_waves_and_rolls_back_on_the_next_tick(
    tmp_path: Path, mode: str,
) -> None:
    w = _phase2_world(
        tmp_path,
        f"fotmob_orchestrated__eee|failed|2026-09-08T00:00:10|{mode}",
        runner_err="[ERROR] __main__: boom\n",
        writer_alive=True,
    )
    pending = w["state"] / f"fotmob-wave-rollback-pending-{w['two_short']}-{WAVE_TC}"
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"], extra_env={"WRITER_WAIT": "0"})
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"], "под живым писателем дерево не трогаем"
    assert not (w["state"] / "fotmob-auto-deliver.off").exists(), "занятое дерево — не авария, автомат не глушим"
    assert pending.is_file() and "fotmob_orchestrated__eee" in pending.read_text(encoding="utf-8")
    assert "жду паузы между волнами" in w["sent"].read_text(encoding="utf-8")

    # Писатель ушёл — тот же маркер доводит откат до конца, метабазу больше не спрашиваем.
    _pgrep_stub(w["stubs"], w["driver_started"], writer_alive=False)
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"], extra_env={"WRITER_WAIT": "0"})
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    assert not pending.exists()
    assert (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").is_file()


@pytest.mark.unit
def test_phase2_rolls_back_a_history_wave_that_carries_a_runner_error(tmp_path: Path) -> None:
    """Ошибка раннера — наш код в любой полосе: краснота истории освобождена от отката
    только тогда, когда `[ERROR] __main__` в task-логе НЕТ."""
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__fff|failed|2026-09-08T00:00:10|backfill",
        runner_err="[ERROR] __main__: RuntimeError: writer lock lost\n",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    assert not (w["state"] / "fotmob-wave-history-red-fotmob_orchestrated__fff").exists()
    assert "writer lock lost" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["refresh", "daily", "backfill", "transfers", "future_mode"])
def test_phase2_defers_the_verdict_when_the_task_log_cannot_be_read(tmp_path: Path, mode: str) -> None:
    """«Лог не прочитан» ≠ «ошибки раннера нет»: иначе красная волна с нашей ошибкой
    ушла бы в «краснота истории», а следующая зелёная актуалка приняла бы этот код."""
    w = _phase2_world(
        tmp_path, f"fotmob_orchestrated__ggg|failed|2026-09-08T00:00:10|{mode}", log_mode="missing",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"], "вслепую не откатываем"
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists(), "и не принимаем"
    assert not (w["state"] / "fotmob-wave-history-red-fotmob_orchestrated__ggg").exists()
    assert "task-лог не прочитан" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
def test_phase2_skips_history_and_judges_by_the_first_current_wave(tmp_path: Path) -> None:
    """Порядок волн не фиксирован: между доставкой и первой актуалкой может встать
    красная историческая — она сообщает о себе и пропускается, вердикт даёт актуалка."""
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__h1|failed|2026-09-08T00:10:00|backfill\n"
        "fotmob_orchestrated__h2|success|2026-09-08T04:00:00|refresh",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert (w["state"] / "fotmob-wave-history-red-fotmob_orchestrated__h1").is_file()
    verdict = w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}"
    assert "fotmob_orchestrated__h2" in verdict.read_text(encoding="utf-8")
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]
    sent = w["sent"].read_text(encoding="utf-8")
    assert "красная волна истории" in sent and "не легла" in sent


@pytest.mark.unit
def test_phase2_takes_the_delivery_moment_from_the_marker_when_the_log_rotated(tmp_path: Path) -> None:
    """Момент доставки живёт в отметке $STATE/fotmob-delivered-at-<sha>: имена маркеров
    фазы содержат его, и ротация лога иначе осиротила бы начатый откат."""
    w = _phase2_world(
        tmp_path, "fotmob_orchestrated__iii|success|2026-09-08T00:00:10|daily", deliver_marker=True,
    )
    assert w["log"].read_text(encoding="utf-8") == "", "строки ДОСТАВЛЕНО в логе нет"
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").is_file()
    assert "mode=daily" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
def test_phase2_waits_while_the_metadb_still_has_an_active_wave(tmp_path: Path) -> None:
    """Живого процесса мало: оркестратор допускает волну каждые пять минут, и новый
    DagRun существует раньше, чем его раннер виден pgrep."""
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__jjj|failed|2026-09-08T00:00:10|refresh",
        runner_err="[ERROR] __main__: boom\n",
    )
    # Писателей нет, но метабаза говорит «одна волна queued» — дерево не трогаем.
    stub = (w["stubs"] / "docker").read_text(encoding="utf-8")
    stub = stub.replace('  *"count(*)"*) echo 0 ;;', '  *"running\',\'queued"*) echo 1 ;;\n  *"count(*)"*) echo 0 ;;')
    (w["stubs"] / "docker").write_text(stub, encoding="utf-8")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"], extra_env={"WRITER_WAIT": "0"})
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert (w["state"] / f"fotmob-wave-rollback-pending-{w['two_short']}-{WAVE_TC}").is_file()
    assert "жду паузы между волнами" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
def test_phase2_defers_the_verdict_when_the_task_log_cannot_be_grepped(tmp_path: Path) -> None:
    """Лог на месте, но не читается (каталог вместо файла, нет прав, битый том): `grep`
    возвращает 2. Без `pipefail` внутри контейнера этот код терялся бы за `cut`, и сбой
    чтения выглядел бы как «ошибки раннера нет» — красная волна прошла бы мимо отката."""
    w = _phase2_world(
        tmp_path, "fotmob_orchestrated__kkk|failed|2026-09-08T00:00:10|backfill", log_mode="unreadable",
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()
    assert not (w["state"] / "fotmob-wave-history-red-fotmob_orchestrated__kkk").exists(), \
        "сбой чтения не смеет выдавать себя за «ошибки раннера нет»"
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists()
    assert "task-лог не прочитан" in w["sent"].read_text(encoding="utf-8")


@pytest.mark.unit
def test_phase2_does_not_close_an_unconfirmed_rollback(tmp_path: Path) -> None:
    """Неподтверждённый откат (метабаза так и держит ошибки импорта) не записывает
    вердикт и не снимает маркер незавершённого отката: иначе после снятия `.off`
    руками фаза увидела бы готовое «rollback» и пропустила забракованный пин."""
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__lll|failed|2026-09-08T00:00:10|refresh",
        runner_err="[ERROR] __main__: boom\n",
    )
    # Ссылка отката ведёт не туда, куда смотрит пин отката: checkout проходит, но
    # confirm_rollback видит чужой HEAD и подтверждения не даёт.
    env_file = _write_env(
        tmp_path / "fotmob.env", **{**w["values"], "FOTMOB_ROLLBACK_REF": w["two_short"]}
    )
    proc = _run(w["auto"], env_file=env_file, stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert "дерево переключено на" in w["log"].read_text(encoding="utf-8"), "откат был начат"
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    assert (w["state"] / f"fotmob-wave-rollback-pending-{w['two_short']}-{WAVE_TC}").is_file(), \
        "незавершённый откат обязан пережить тик"
    assert not (w["state"] / f"fotmob-wave-verdict-{w['two_short']}-{WAVE_TC}").exists()
    assert "откат НЕ подтверждён" in w["sent"].read_text(encoding="utf-8")


def _pin_probe(script: Path, tmp_path: Path) -> Path:
    """Только настоящий startup скрипта: ни git, ни docker, ни писателей."""
    text = script.read_text(encoding="utf-8")
    boundary = "\nLOCK=" if script == AUTO else "\n# Ручной запуск"
    probe = tmp_path / script.name
    probe.write_text(
        text.split(boundary, 1)[0]
        + '\nprintf "%s\\n" "$TARGET" "$ROLLBACK_REF" "$ROLLBACK_SHA" '
        '"$NEW_CODE_FILE" "$NEW_CODE_MD5" "$FOTMOB_REPOSITORY_MD5" '
        '"$FOTMOB_RUNTIME_HASHES" "$PACKAGE_FILE" "$PACKAGE_DIGEST"\n',
        encoding="utf-8",
    )
    shutil.copy(ENV_SH, tmp_path / "env.sh")
    shutil.copy(DEPLOY / "delivery_barrier.sh", tmp_path / "delivery_barrier.sh")
    return probe


@pytest.mark.unit
def test_one_package_pointer_switches_both_scripts_and_pins_the_handoff(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    auto = _pin_probe(AUTO, tmp_path)
    b6 = _pin_probe(B6, tmp_path)
    first = _run(auto, env_file=env_file)
    assert first.returncode == 0, first.stderr
    old = first.stdout.splitlines()
    assert _run(b6, env_file=env_file).stdout == first.stdout

    # Новый неизменяемый пакет, одна строка указателя; изменяется весь набор пинов.
    second_values = {
        "FOTMOB_TARGET": "33333333", "FOTMOB_ROLLBACK_REF": "11111111",
        "FOTMOB_ROLLBACK_SHA": "11111111",
        "FOTMOB_NEW_CODE_FILE": "/opt/airflow/scrapers/fotmob/repository.py",
        "FOTMOB_NEW_CODE_MD5": "b" * 32, "FOTMOB_REPOSITORY_MD5": "c" * 32,
        "FOTMOB_RUNTIME_HASHES": "|".join("d" * 64 + " " + item.split(" ", 1)[1] for item in RUNTIME_MODULES),
    }
    package = _write_env(tmp_path / "next.release", **second_values,
                         FOTMOB_REQUIRED_MODULES=values["FOTMOB_REQUIRED_MODULES"])
    text = env_file.read_text(encoding="utf-8")
    env_file.write_text(text.replace(old[-2], str(package)), encoding="utf-8")
    for probe in (auto, b6):
        result = _run(probe, env_file=env_file)
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines()[:7] == list(second_values.values())

    # Автомат уже прочёл старый пакет: B6 игнорирует новый указатель в env.
    handoff = {"FOTMOB_PINNED_PACKAGE": old[-2], "FOTMOB_PINNED_DIGEST": old[-1]}
    result = _run(b6, env_file=env_file, extra_env=handoff)
    assert result.returncode == 0, result.stderr
    assert result.stdout == first.stdout
    Path(old[-2]).write_text(package.read_text(encoding="utf-8"), encoding="utf-8")
    result = _run(b6, env_file=env_file, extra_env=handoff)
    assert result.returncode == 2
    assert "пакет доставки изменён" in result.stderr


@pytest.mark.unit
@pytest.mark.parametrize("key", sorted(PACKAGE_KEYS))
def test_incomplete_package_refuses_both_scripts_before_any_action(tmp_path: Path, key: str) -> None:
    values = _layout(tmp_path)
    del values[key]
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    for script in (AUTO, B6):
        proc = _run(script, env_file=env_file, extra_env={key: "stale"})
        assert proc.returncode == 2
        assert key in proc.stderr
    assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-b6-inflight").exists()


@pytest.mark.unit
@pytest.mark.parametrize("bad", ["missing-module", "duplicate", "bad-sha", "host-key"])
def test_package_cannot_weaken_fingerprints_or_change_host_paths(tmp_path: Path, bad: str) -> None:
    values = _layout(tmp_path)
    entries = values["FOTMOB_RUNTIME_HASHES"].split("|")
    if bad == "missing-module":
        values["FOTMOB_RUNTIME_HASHES"] = "|".join(entries[1:])
    elif bad == "duplicate":
        values["FOTMOB_RUNTIME_HASHES"] += "|" + entries[0]
    elif bad == "bad-sha":
        values["FOTMOB_RUNTIME_HASHES"] = values["FOTMOB_RUNTIME_HASHES"].replace("a" * 64, "short")
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    if bad == "host-key":
        with env_file.with_suffix(".release").open("a", encoding="utf-8") as package:
            package.write("FOTMOB_RELEASE_ROOT=/other/tree\n")
    for script in (AUTO, B6):
        result = _run(script, env_file=env_file)
        assert result.returncode == 2, result.stderr
    assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-b6-inflight").exists()


@pytest.mark.unit
@pytest.mark.parametrize("bad", ["missing", "production", "wrong-head", "dirty"])
def test_review_validation_preflight_refuses_before_checkout(tmp_path: Path, bad: str) -> None:
    values = _layout(tmp_path)
    repo = Path(values["FOTMOB_RELEASE_ROOT"])
    one, two = _two_commits(repo)
    validation = Path(values["FOTMOB_VALIDATION_ROOT"])
    if bad != "missing":
        _git(tmp_path, "clone", "-q", str(repo), str(validation))
        _git(validation, "checkout", "-q", "--detach", one if bad == "wrong-head" else two)
        if bad == "dirty":
            (validation / "a.txt").write_text("dirty")
    if bad == "production":
        values["FOTMOB_VALIDATION_ROOT"] = str(repo)
    values.update(FOTMOB_TARGET=two[:8], FOTMOB_ROLLBACK_SHA=one[:8], FOTMOB_ROLLBACK_REF=one[:8])
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "pgrep", "exit 1\n")
    _stub(stubs, "docker", "echo 0\n")
    proc = _run(B6, "check", env_file=env_file, stubs=stubs)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ОТКАЗ:" in proc.stderr
    assert _git(repo, "rev-parse", "HEAD") == one


@pytest.mark.unit
def test_validation_tree_of_another_owner_passes_preflight(tmp_path: Path) -> None:
    # 28.10: root из cron (без SUDO_UID) читал validation tree claude-am — git отверг
    # «dubious ownership», rc=128, доставка откатилась. Чужого владельца имитирует
    # GIT_TEST_ASSUME_DIFFERENT_OWNER, но только для validation tree (бой принадлежит root).
    values = _layout(tmp_path)
    repo = Path(values["FOTMOB_RELEASE_ROOT"])
    one, two = _two_commits(repo)
    validation = Path(values["FOTMOB_VALIDATION_ROOT"])
    _git(tmp_path, "clone", "-q", str(repo), str(validation))
    _git(validation, "checkout", "-q", "--detach", two)
    values.update(FOTMOB_TARGET=two[:8], FOTMOB_ROLLBACK_SHA=one[:8], FOTMOB_ROLLBACK_REF=one[:8])
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    _stub(stubs, "pgrep", "exit 1\n")
    _stub(stubs, "docker", "echo 0\n")
    real_git = shutil.which("git")
    _stub(stubs, "git", f'case " $* " in *" {validation} "*) export GIT_TEST_ASSUME_DIFFERENT_OWNER=1;; esac\n'
                        f'exec {real_git} "$@"\n')
    proc = _run(B6, "check", env_file=env_file, stubs=stubs)
    assert "dubious ownership" not in proc.stderr, proc.stderr
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "check ПРОЙДЕН" in proc.stdout


@pytest.mark.unit
def test_review_repository_md5_exec_failure_has_a_refusal(tmp_path: Path) -> None:
    # Реальная секция приёмки; lock/checkout не нужны для проверки set -e.
    section = B6.read_text().split('say "4.', 1)[1].split('parse_left=', 1)[0]
    probe = tmp_path / "probe.sh"
    probe.write_text('set -euo pipefail\nsay(){ :; }; die(){ echo "ОТКАЗ: $*" >&2; exit 1; }\n'
                     'SCHED=s; METADB=m; TREE=t; ROLLBACK_REF=r; NEW_CODE_FILE=/new; '
                     'NEW_CODE_MD5=' + '0' * 32 + '; FOTMOB_REPOSITORY_MD5=' + '0' * 32 + '\n'
                     'say "4.' + section)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _stub(stubs, "sleep", "exit 0\n")
    _stub(stubs, "docker", 'case "$*" in *repository.py*) exit 42;; *md5sum*) echo ' + '0' * 32 + ';; *) echo 0;; esac\n')
    proc = _run(probe, env_file=tmp_path / "unused", stubs=stubs)
    assert proc.returncode == 1
    assert "ОТКАЗ:" in proc.stderr and "repository.py" in proc.stderr


@pytest.mark.unit
def test_review_timeout_covers_b6_budget() -> None:
    import re
    seconds = int(re.search(r'DELIVER_TIMEOUT=\$\{DELIVER_TIMEOUT:-(\d+)\}', AUTO.read_text())[1])
    # 31 sha + 2 md5 + 16 parse polls + 3 other metadb calls, 19-file suite.
    assert seconds >= 90 + 300 + 52 * 10 + 900 + 300


@pytest.mark.unit
@pytest.mark.parametrize(
    ("hhmm", "configured", "expected"),
    [("2000", "2400", 2400), ("2320", "2400", 600), ("2320", "300", 300)],
)
def test_late_delivery_timeout_leaves_time_for_rollback(
    tmp_path: Path, hhmm: str, configured: str, expected: int,
) -> None:
    w = _phase2_world(tmp_path, "")
    _git(w["repo"], "checkout", "-q", "--detach", w["one"])
    (w["state"] / "fotmob-b6-accepted").unlink()
    w["log"].write_text("")
    _date_stub(w["stubs"], hhmm)
    elapsed = tmp_path / "clock"
    elapsed.write_text("0")
    budget = tmp_path / "budget"
    deliver = w["auto"].parent / "b6_deliver.sh"
    start = int(hhmm[:2]) * 3600 + int(hhmm[2:]) * 60
    # Зависание B6 после checkout: timeout съедает весь бюджет и kill-after.
    # После полуночи появляется раннер новой волны, под ним откат запрещён.
    _stub(
        w["stubs"], "timeout",
        f'if [ "$4" = "{deliver}" ]; then\n'
        f'  echo "$3" > "{budget}"\n'
        f'  git -C "{w["repo"]}" checkout -q --detach "{w["two"]}"\n'
        f'  echo "$(({start} + $3 + 30))" > "{elapsed}"\n'
        '  exit 124\nfi\nexec /usr/bin/timeout "$@"\n',
    )
    _stub(
        w["stubs"], "pgrep",
        f'case "$*" in *run_fotmob_scrape*) [ "$(cat "{elapsed}")" -ge 86400 ]; exit $? ;; esac\n'
        'exit 1\n',
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"],
                extra_env={"DELIVER_TIMEOUT": configured, "WRITER_WAIT": "0"})
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert int(budget.read_text()) == expected
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert "ОТКАТ ПОДТВЕРЖДЁН" in w["log"].read_text()
    assert not (w["state"] / "fotmob-b6-inflight").exists()


@pytest.mark.unit
def test_late_preflight_without_time_left_clears_unstarted_delivery(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "")
    _git(w["repo"], "checkout", "-q", "--detach", w["one"])
    (w["state"] / "fotmob-b6-accepted").unlink()
    w["log"].write_text("")
    # Окно проверено в 23:20, а к запуску B6 предполётные проверки дотянули до 23:30.
    _stub(
        w["stubs"], "date",
        'case "$*" in\n'
        '  *"+%H%M") echo 2320 ;;\n'
        '  *"+%H%M%S") echo 233000 ;;\n'
        '  *"+%F") echo 2026-01-01 ;;\n'
        '  *"+%H:%M") echo 23:30 ;;\n'
        '  *) exec /bin/date "$@" ;;\n'
        'esac\n',
    )
    called = tmp_path / "b6-called"
    deliver = w["auto"].parent / "b6_deliver.sh"
    _stub(
        w["stubs"], "timeout",
        f'if [ "$4" = "{deliver}" ]; then touch "{called}"; exit 0; fi\n'
        'exec /usr/bin/timeout "$@"\n',
    )
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"],
                extra_env={"WRITER_WAIT": "0"})
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert not called.exists()
    assert "НЕТ ВРЕМЕНИ" in w["log"].read_text()
    assert not (w["state"] / "fotmob-b6-inflight").exists()
    assert not (w["state"] / "fotmob-deliver-nonce").exists()
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]


@pytest.mark.unit
def test_acceptance_budget_includes_slow_queries(tmp_path: Path) -> None:
    section = AUTO.read_text().split('      seen=0\n', 1)[1].split('      if [ "$seen" != 1 ]; then', 1)[0]
    clock = tmp_path / "clock"
    clock.write_text("0")
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _stub(stubs, "date", f'cat "{clock}"\n')
    probe = tmp_path / "acceptance.sh"
    probe.write_text(
        f'clock="{clock}"\n'
        'advance(){ echo "$(($(cat "$clock") + $1))" > "$clock"; }\n'
        'acceptance_seen(){ advance 105; echo 0; }\n'
        'sleep(){ advance "$1"; }\nlog(){ :; }\nseen=0\n' + section,
    )
    proc = _run(probe, env_file=tmp_path / "unused", stubs=stubs)
    assert proc.returncode == 0, proc.stderr
    # 480 с ожидания + последняя пауза (20 с) и проверка (3 × 35 с).
    assert 480 <= int(clock.read_text()) <= 605


@pytest.mark.unit
def test_review_runtime_composition_is_owned_by_package(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    values["FOTMOB_REQUIRED_MODULES"] = "/opt/airflow/new.py"
    values["FOTMOB_RUNTIME_HASHES"] = "a" * 64 + " /opt/airflow/new.py"
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    probe = _pin_probe(B6, tmp_path)
    result = _run(probe, env_file=env_file)
    assert result.returncode == 0, result.stderr
    # Новый состав — из пакета, но неполный manifest по-прежнему запрещён.
    with env_file.with_suffix(".release").open("a") as file:
        file.write("FOTMOB_REQUIRED_MODULES=/opt/airflow/new.py|/opt/airflow/other.py\n")
    result = _run(probe, env_file=env_file)
    assert result.returncode == 2 and "нет runtime fingerprint" in result.stderr


@pytest.mark.unit
@pytest.mark.parametrize("bad", ["md5", "sha", "path", "duplicate"])
def test_review_bad_package_explains_refusal(tmp_path: Path, bad: str) -> None:
    values = _layout(tmp_path)
    if bad == "md5":
        values["FOTMOB_NEW_CODE_MD5"] = "bad"
    elif bad == "sha":
        values["FOTMOB_RUNTIME_HASHES"] = "bad /opt/airflow/x.py"
    elif bad == "path":
        values["FOTMOB_NEW_CODE_FILE"] = "/outside"
    else:
        values["FOTMOB_RUNTIME_HASHES"] += "|" + values["FOTMOB_RUNTIME_HASHES"].split("|")[0]
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    result = _run(B6, "check", env_file=env_file)
    assert result.returncode == 2
    assert result.stderr.strip(), "отказ пакета обязан объяснять причину"


@pytest.mark.unit
@pytest.mark.parametrize("broken", ["missing", "corrupt"])
def test_review_broken_package_does_not_prevent_inflight_rollback(tmp_path: Path, broken: str) -> None:
    w = _phase2_world(tmp_path, "")
    recovery = w["state"] / "fotmob-recovery.env"
    shutil.copy(w["env_file"].with_suffix(".release"), recovery)
    (w["state"] / "fotmob-b6-inflight").touch()
    package = w["env_file"].with_suffix(".release")
    if broken == "missing":
        package.unlink()
    else:
        package.write_text("bad\n")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"], proc.stderr
    assert not (w["state"] / "fotmob-b6-inflight").exists()
    assert "восстановление подтверждено" in w["log"].read_text()


@pytest.mark.unit
def test_review_bad_package_without_recovery_alerts(tmp_path: Path) -> None:
    values = _layout(tmp_path)
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    env_file.with_suffix(".release").unlink()
    (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-b6-inflight").touch()
    proc = _run(AUTO, env_file=env_file)
    assert proc.returncode == 2
    assert "пакет доставки" in (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-pending-alert").read_text()


@pytest.mark.unit
def test_review_bad_package_preserves_first_wave_rollback(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__bad|failed|2026-09-08T00:00:10|refresh",
                      runner_err="[ERROR] __main__: boom\n")
    shutil.copy(w["env_file"].with_suffix(".release"), w["state"] / "fotmob-recovery.env")
    w["env_file"].with_suffix(".release").write_text("bad\n")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert "ОТКАТ ПО КРАСНОЙ ВОЛНЕ ПОДТВЕРЖДЁН" in w["log"].read_text()


@pytest.mark.unit
def test_review_recovery_package_cannot_start_new_delivery(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "")
    _git(w["repo"], "checkout", "-q", "--detach", w["one"])
    shutil.copy(w["env_file"].with_suffix(".release"), w["state"] / "fotmob-recovery.env")
    w["env_file"].with_suffix(".release").write_text("bad\n")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 2, proc.stderr
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert not list(w["state"].glob("fotmob-auto-deliver-attempted-*"))
    assert not w["driver_started"].exists()


def _trace_barrier(w: dict, tmp_path: Path, failure: str = "") -> Path:
    """SQL/process stubs only; checkout and shell control flow remain real."""
    events = tmp_path / "barrier.events"
    live = tmp_path / "barrier.live"
    docker = w["stubs"] / "docker"
    text = docker.read_text()
    cases = (
        '  *"UPDATE dag SET is_paused=true"*)\n'
        f'    if [ -e "{live}" ]; then echo pause >> "{events}"; else echo drain-open >> "{events}"; fi\n'
        + ('    exit 42 ;;\n' if failure == "pause" else
           '    echo 1 ;;\n' if failure == "unconfirmed" else '    echo 0 ;;\n')
        + '  *"FROM task_instance"*)\n'
        f'    if [ ! -e "{live}" ]; then touch "{live}"; echo live >> "{events}"; echo 1;\n'
        f'    else echo drained >> "{events}"; echo 0; fi ;;\n'
        '  *"BEGIN;"*)\n'
        # Pauses originally set by the owner must never be removed.
        '    [[ "$*" != *"is_paused=false WHERE dag_id=\'dag_backfill_fotmob\'"* ]] || exit 43\n'
        f'    echo release >> "{events}"; echo 5 ;;\n'
    )
    if failure in ("query", "timeout"):
        start = cases.index('  *"FROM task_instance"*)')
        end = cases.index('  *"BEGIN;"*)', start)
        cases = cases[:start] + '  *"FROM task_instance"*) ' + (
            'exit 42 ;;\n' if failure == "query" else 'echo 1 ;;\n'
        ) + cases[end:]
    docker.write_text(text.replace('case "$*" in\n', 'case "$*" in\n' + cases, 1))
    _stub(w["stubs"], "git",
          'for arg in "$@"; do [ "$arg" = checkout ] || continue\n'
          f'  echo checkout >> "{events}"\ndone\nexec /usr/bin/git "$@"\n')
    return events


@pytest.mark.unit
@pytest.mark.parametrize("route", ["red-wave", "recovery", "apply", "failed-apply"])
def test_checkout_barrier_drains_a_wave_born_after_the_zero_snapshot(tmp_path: Path, route: str) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__race|failed|2026-09-08T00:00:10|refresh")
    if route == "recovery":
        (w["state"] / "fotmob-b6-inflight").touch()
    if route in ("apply", "failed-apply"):
        _git(w["repo"], "checkout", "-q", "--detach", w["one"])
        (w["state"] / "fotmob-b6-accepted").unlink()
        w["log"].write_text("")
        validation = Path(w["values"]["FOTMOB_VALIDATION_ROOT"])
        _git(tmp_path, "clone", "-q", str(w["repo"]), str(validation))
        _git(validation, "checkout", "-q", "--detach", w["two"])
        _date_stub(w["stubs"], "2000")
        docker = w["stubs"] / "docker"
        text = docker.read_text().replace('case "$*" in\n', 'case "$*" in\n'
            '  *sha256sum*) echo "' + 'a' * 64 + '  file" ;;\n'
            '  *"last_parsed_time >"*) echo 7 ;;\n', 1)
        if route == "failed-apply":
            # B6 fails after checkout at import_error, then the real auto rollback runs.
            text = text.replace('  *"count(*)"*) echo 0 ;;', '  *"FROM import_error"*) echo 1 ;;\n  *"count(*)"*) echo 0 ;;')
        docker.write_text(text)
        w["values"]["FOTMOB_REPOSITORY_MD5"] = w["values"]["FOTMOB_NEW_CODE_MD5"]
        _write_env(w["env_file"], **w["values"])
        (w["state"] / "fotmob-campaign.hold").touch()
    events = _trace_barrier(w, tmp_path)
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"], extra_env={"BARRIER_WAIT": "10"})
    rows = events.read_text().splitlines()
    assert rows[:5] == ["drain-open", "live", "pause", "drained", "checkout"], proc.stderr + w["log"].read_text()
    if route == "failed-apply":
        # Import error deliberately also blocks confirmation: keep pauses for the owner.
        assert rows.count("checkout") == 2
        assert "release" not in rows
        assert (w["state"] / "fotmob-delivery-pauses").is_file()
    else:
        assert rows[-1] == "release"
        assert not (w["state"] / "fotmob-delivery-pauses").exists()
    assert "AIRFLOWIGNORE md5 до/после checkout" in w["log"].read_text()
    assert (tmp_path / "external.airflowignore").read_text() == "external block-list\n"
    assert (w["repo"] / "dags/.airflowignore").read_text() == ("two\n" if route == "apply" else "one\n")
    assert _git(w["repo"], "rev-parse", "HEAD") == (w["two"] if route == "apply" else w["one"])
    assert proc.returncode == (0 if route == "apply" else 1)


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["pause", "unconfirmed", "query", "timeout"])
def test_checkout_barrier_fails_closed(tmp_path: Path, failure: str) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__race|failed|2026-09-08T00:00:10|refresh")
    events = _trace_barrier(w, tmp_path, failure)
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"], extra_env={"BARRIER_WAIT": "0"})
    assert proc.returncode == 1
    assert "checkout" not in events.read_text()
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]
    assert (w["state"] / "fotmob-delivery-pauses").is_file()
    assert "checkout запрещён" in w["log"].read_text()


@pytest.mark.unit
@pytest.mark.parametrize("resume", ["helper", "auto-inflight", "auto-off"])
def test_checkout_barrier_recovers_original_pauses_after_sigkill(tmp_path: Path, resume: str) -> None:
    w = _phase2_world(tmp_path, "")
    events = _trace_barrier(w, tmp_path)
    journal = w["state"] / "fotmob-delivery-pauses"
    # Kill after UPDATE, before drain. Journal must predate the UPDATE.
    docker = w["stubs"] / "docker"
    text = docker.read_text()
    docker.write_text(text.replace('    echo 0 ;;', f'    kill -KILL "$(cat {tmp_path / "barrier.pid"})"; exit 42 ;;', 1))
    script = tmp_path / "barrier.sh"
    script.write_text(f'echo $$ > "{tmp_path / "barrier.pid"}"\n. "{DEPLOY / "delivery_barrier.sh"}"\nbarrier_acquire\n')
    proc = _run(script, env_file=w["env_file"], stubs=w["stubs"], extra_env=w["values"])
    assert proc.returncode != 0
    original = journal.read_text()
    assert "dag_orchestrate_fotmob|f" in original and "dag_backfill_fotmob|t" in original
    if resume != "helper":
        docker.write_text(text)
        _git(w["repo"], "checkout", "-q", "--detach", w["one"])
        marker = w["state"] / ("fotmob-b6-inflight" if resume == "auto-inflight" else "fotmob-auto-deliver.off")
        marker.touch()
        events.write_text("")
        proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
        assert proc.returncode == (1 if resume == "auto-inflight" else 0), proc.stderr
        assert not journal.exists()
        assert marker.exists(), "recovery must not clear INFLIGHT/OFF"
        assert events.read_text().splitlines() == ["release"], "no checkout during pause restoration"
        return
    # Snapshot now says all paused, but recovery must reuse the saved original.
    docker.write_text(text.replace("|f", "|t"))
    script.write_text(f'. "{DEPLOY / "delivery_barrier.sh"}"\nbarrier_acquire && barrier_release\n')
    proc = _run(script, env_file=w["env_file"], stubs=w["stubs"], extra_env=w["values"])
    assert proc.returncode == 0, proc.stderr
    assert not journal.exists()
    assert events.read_text().splitlines()[-1] == "release"


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["late-apply", "internal-ignore", "changed-ignore"])
def test_checkout_barrier_rechecks_window_and_external_ignore(tmp_path: Path, failure: str) -> None:
    w = _phase2_world(tmp_path, "")
    events = _trace_barrier(w, tmp_path)
    _date_stub(w["stubs"], "2321" if failure == "late-apply" else "2000")
    if failure == "internal-ignore":
        docker = w["stubs"] / "docker"
        docker.write_text(docker.read_text().replace(
            str(tmp_path / "external.airflowignore"), str(w["repo"] / "dags/.airflowignore")))
    if failure == "changed-ignore":
        git = w["stubs"] / "git"
        git.write_text(git.read_text().replace('  echo checkout',
            f'  echo changed > "{tmp_path / "external.airflowignore"}"\n  echo checkout'))
    script = tmp_path / "barrier.sh"
    script.write_text(f'. "{DEPLOY / "delivery_barrier.sh"}"\n'
                      f'barrier_checkout -B deploy/test "{w["one"]}"\n')
    proc = _run(script, env_file=w["env_file"], stubs=w["stubs"],
                extra_env={**w["values"], "TREE": str(w["repo"])})
    assert proc.returncode == 1, proc.stderr
    rows = events.read_text().splitlines()
    assert ("checkout" in rows) == (failure == "changed-ignore")
    assert "release" not in rows
    assert (w["state"] / "fotmob-delivery-pauses").is_file()


@pytest.mark.unit
@pytest.mark.parametrize("live_parent", [False, True])
def test_checkout_barrier_rolls_back_when_c1_appears_after_apply(tmp_path: Path, live_parent: bool) -> None:
    """Execute barrier SQL: the target adds c1 after a five-producer snapshot."""
    import sqlite3
    import sys

    w = _phase2_world(tmp_path, "")
    _date_stub(w["stubs"], "2000")
    db_path = tmp_path / "metadata.sqlite"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE dag (dag_id TEXT PRIMARY KEY, is_paused BOOLEAN)")
        db.executemany("INSERT INTO dag VALUES (?, ?)", [
            (dag, dag == "dag_backfill_fotmob") for dag in BARRIER_PRODUCERS if dag != "dag_refresh_fotmob_c1"
        ])
        db.execute("CREATE TABLE dag_run (dag_id TEXT, state TEXT)")
        db.execute("CREATE TABLE task_instance (dag_id TEXT, state TEXT)")
        if live_parent:
            db.execute("INSERT INTO dag_run VALUES ('dag_orchestrate_fotmob', 'running')")
            db.executemany("INSERT INTO task_instance VALUES ('dag_orchestrate_fotmob', ?)",
                           [("scheduled",), (None,)])
    sql_runner = tmp_path / "metadata.py"
    sql_runner.write_text('''import sqlite3
import sys

class BoolAnd:
    def __init__(self):
        self.value = True
    def step(self, value):
        self.value = self.value and bool(value)
    def finalize(self):
        return self.value

db = sqlite3.connect(sys.argv[1], isolation_level=None)
db.create_aggregate("bool_and", 1, BoolAnd)
if sys.argv[2] == "add-c1":
    db.execute("INSERT INTO dag VALUES ('dag_refresh_fotmob_c1', true)")
else:
    # Model Airflow: a paused parent cannot schedule finalize/advance after ingest.
    # Each drain poll schedules one remaining task only while its DAG is unpaused.
    if "FROM task_instance" in sys.argv[-1]:
        paused = db.execute("SELECT is_paused FROM dag WHERE dag_id='dag_orchestrate_fotmob'").fetchone()[0]
        if not paused:
            db.execute("UPDATE task_instance SET state='success' WHERE rowid=(SELECT min(rowid) FROM task_instance WHERE state IS NULL OR state='scheduled')")
            if not db.execute("SELECT count(*) FROM task_instance WHERE state IS NULL OR state='scheduled'").fetchone()[0]:
                db.execute("UPDATE dag_run SET state='success'")
    for statement in sys.argv[-1].split(";"):
        if not statement.strip():
            continue
        cursor = db.execute(statement.replace(" FOR UPDATE", ""))
        if cursor.description:
            for row in cursor:
                print("|".join("t" if value == 1 else "f" if value == 0 else str(value)
                               for value in row) if len(row) == 2 else row[0])
''')
    docker = w["stubs"] / "docker"
    fallback = w["stubs"] / "docker-fallback"
    shutil.copy(docker, fallback)
    _stub(w["stubs"], "docker", 'case "$*" in\n'
          '  *psql*) exec "' + sys.executable + '" "' + str(sql_runner) + '" "' + str(db_path) + '" query "${@: -1}" ;;\n'
          'esac\nexec "' + str(fallback) + '" "$@"\n')
    script = tmp_path / "barrier.sh"
    script.write_text(f'. "{DEPLOY / "delivery_barrier.sh"}"\n'
                      f'barrier_checkout -B deploy/test "{w["two"]}" || exit 11\n'
                      f'"{sys.executable}" "{sql_runner}" "{db_path}" add-c1 || exit 12\n'
                      f'barrier_checkout -f "{w["one"]}" || exit 13\n'
                      'barrier_release || exit 14\n')
    proc = _run(script, env_file=w["env_file"], stubs=w["stubs"],
                extra_env={**w["values"], "TREE": str(w["repo"]), "BARRIER_WAIT": "2"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    with sqlite3.connect(db_path) as db:
        paused = dict(db.execute("SELECT dag_id, is_paused FROM dag"))
    assert {dag for dag, value in paused.items() if value} == {"dag_backfill_fotmob", "dag_refresh_fotmob_c1"}
    assert not (w["state"] / "fotmob-delivery-pauses").exists()


@pytest.mark.unit
@pytest.mark.parametrize("broken", ["empty", "duplicate", "unknown", "symlink", "unterminated"])
def test_checkout_barrier_rejects_a_damaged_pause_journal(tmp_path: Path, broken: str) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__race|failed|2026-09-08T00:00:10|refresh")
    events = _trace_barrier(w, tmp_path)
    journal = w["state"] / "fotmob-delivery-pauses"
    rows = [f"{dag}|f" for dag in BARRIER_PRODUCERS]
    if broken == "empty":
        rows = []
    elif broken == "duplicate":
        rows[-1] = rows[0]
    elif broken == "unknown":
        rows[-1] = "foreign_dag|f"
    journal.write_text("".join(row + "\n" for row in rows))
    if broken == "unterminated":
        with journal.open("a") as output:
            output.write("dag_refresh_fotmob_c1|f")
    if broken == "symlink":
        journal.rename(tmp_path / "journal-target")
        journal.symlink_to(tmp_path / "journal-target")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1
    assert not events.exists(), "a damaged journal must block before UPDATE/checkout"
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"]
