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
        "FOTMOB_HOST_PYTEST": "/bin/true",
        # Короткие SHA, как `git rev-parse --short`: автомат сравнивает пины с ним по префиксу.
        "FOTMOB_TARGET": "11111111",
        "FOTMOB_ROLLBACK_REF": "22222222",
        "FOTMOB_ROLLBACK_SHA": "22222222",
        "FOTMOB_NEW_CODE_FILE": "/opt/airflow/scrapers/fotmob/service.py",
        "FOTMOB_NEW_CODE_MD5": "0" * 32,
    }


def _install(tmp_path: Path, stubs: Path) -> Path:
    """Копия deploy/fotmob как после `install` рунбука (скрипты рядом, автомат зовёт
    b6_deliver.sh по соседству) с ровно одной правкой: фиксированный PATH автомата
    получает впереди каталог заглушек. Возвращает путь установленного автомата."""
    libexec = tmp_path / "libexec"
    libexec.mkdir()
    for src in (AUTO, B6, ALERT, ENV_SH):
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
    assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-auto-deliver.lock").exists()
    # Значение из окружения (cron-строка, забытый export) файл без ключа не заменяет.
    proc = _run(AUTO, env_file=env_file, extra_env={"FOTMOB_TARGET": "deadbee"})
    assert proc.returncode != 0
    assert "FOTMOB_TARGET" in proc.stderr
    assert not Path(values["FOTMOB_LOG"]).exists(), "автомат не сделал ни шага"


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
        assert not (Path(values["FOTMOB_STATE_DIR"]) / "fotmob-auto-deliver.lock").exists()


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
    _git(repo, "add", "a.txt")
    _git(repo, "commit", "-q", "-m", "one")
    one = _git(repo, "rev-parse", "HEAD")
    (repo / "a.txt").write_text("b", encoding="utf-8")
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
def test_auto_deliver_hands_lock_and_nonce_to_b6_and_accepts_against_stubs(tmp_path: Path) -> None:
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
    env_file = _write_env(tmp_path / "fotmob.env", **values)
    stubs = tmp_path / "bin"
    stubs.mkdir()
    _date_stub(stubs, "2000")
    # `sleep 5` — ожидание старта драйвера кампании: заглушке драйвера нужно мгновение,
    # чтобы оставить след; остальные ожидания (приёмка, 90 с) — мгновенны.
    _stub(stubs, "sleep", '[ "$1" = 5 ] && exec /bin/sleep 1\nexit 0\n')
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
        '  *pgrep*) exit 1 ;;\n'                    # юнит кампании в контейнере не жив
        f'  *md5sum*) echo "{md5}  file" ;;\n'      # целевой модуль виден из контейнера
        '  *"FROM dag WHERE"*) echo 1 ;;\n'         # fotmob-даг перечитан планировщиком
        '  *"count(*)"*) echo 0 ;;\n'               # ни активных ранов, ни ошибок импорта
        '  *) echo "dag_x | f" ;;\n'
        'esac\n',
    )
    auto = _install(tmp_path, stubs)
    proc = _run(auto, env_file=env_file, stubs=stubs)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    log = Path(values["FOTMOB_LOG"]).read_text(encoding="utf-8")
    assert "ОКНО ОТКРЫТО" in log and f"{auto.parent}/b6_deliver.sh apply" in log, "b6 — сосед автомата"
    assert "окно открыто" in log and "B6 ДОСТАВЛЕН" in log, "вывод b6 уходит в лог автомата"
    assert "приёмка подтверждена" in log and f"ДОСТАВЛЕНО: HEAD={two_short}" in log
    assert "кампания истории запущена" in log
    assert _git(repo, "rev-parse", "HEAD") == two
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "deploy/fotmob-b6-master"
    assert (state / "fotmob-b6-accepted").read_text(encoding="utf-8").strip() == two_short
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


def _phase2_world(tmp_path: Path, rows: str, runner_err: str = "", writer_alive: bool = False) -> dict:
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
    err_file = tmp_path / "runner_err.txt"
    err_file.write_text(runner_err, encoding="utf-8")
    calls = tmp_path / "docker.calls"
    _stub(
        stubs, "docker",
        f'echo "$*" >> "{calls}"\n'
        'case "$*" in\n'
        '  *pgrep*) exit 1 ;;\n'
        f'  *md5sum*) echo "{md5}  file" ;;\n'
        f'  *__main__*) cat "{err_file}" ;;\n'          # task-лог волны: строка ошибки раннера
        f'  *"FROM dag_run WHERE"*) cat "{rows_file}" ;;\n'   # волны после доставки (фаза 2)
        '  *"FROM dag WHERE"*) echo 1 ;;\n'             # даг перечитан планировщиком
        '  *"SELECT 1;"*) echo 1 ;;\n'                  # метабаза жива
        '  *"count(*)"*) echo 0 ;;\n'                   # import_error
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
    assert "первая волна актуалки" in sent and "mode=refresh" in sent and "принято" in sent
    assert _git(w["repo"], "rev-parse", "HEAD") == w["two"], "зелёная волна ничего не откатывает"
    assert not (w["state"] / "fotmob-auto-deliver.off").exists()


@pytest.mark.unit
def test_phase2_rolls_back_on_a_runner_error_when_no_writer_is_alive(tmp_path: Path) -> None:
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__bbb|failed|2026-09-08T00:00:10|refresh",
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
def test_phase2_rolls_back_a_red_refresh_wave_without_a_runner_error_and_names_the_source(tmp_path: Path) -> None:
    w = _phase2_world(tmp_path, "fotmob_orchestrated__ddd|failed|2026-09-08T00:00:10|daily")
    proc = _run(w["auto"], env_file=w["env_file"], stubs=w["stubs"])
    assert proc.returncode == 1, proc.stderr + proc.stdout
    assert _git(w["repo"], "rev-parse", "HEAD") == w["one"]
    assert (w["state"] / "fotmob-auto-deliver.off").is_file()
    sent = w["sent"].read_text(encoding="utf-8")
    assert "ошибки раннера нет" in sent and "no_progress_failure" in sent
    assert "повторить доставку в ближайшее окно" in sent


@pytest.mark.unit
def test_phase2_waits_for_the_gap_between_waves_and_rolls_back_on_the_next_tick(tmp_path: Path) -> None:
    w = _phase2_world(
        tmp_path,
        "fotmob_orchestrated__eee|failed|2026-09-08T00:00:10|refresh",
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
