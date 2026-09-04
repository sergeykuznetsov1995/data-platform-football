"""Автомат ночной доставки SofaScore против заглушек (#1245, PR-2).

Настоящий `deploy/sofascore/auto_deliver.sh` гоняется целиком на установленной копии
(как после `install` рунбука) с ЕДИНСТВЕННОЙ правкой — фиксированный PATH автомата
получает впереди каталог заглушек. Без этой правки стенд ходил бы в настоящую метабазу
и в настоящий docker: мина, всплывавшая у FotMob дважды.

Git — настоящий: боевое дерево это клон тестового репозитория на первом коммите, master
репозитория — второй. Репозиторий создаётся `git init -b master`: при
`init.defaultBranch=main` ветка `refs/heads/master` была бы пуста, автомат воспринял бы
это как «сеть недоступна» и молча вышел нулём, а все сценарии стали бы зелёными впустую.

Часы контура тоже заглушены и КОГЕРЕНТНЫ: заглушка `date` отвечает на любой формат
одним и тем же поддельным моментом, поэтому окно (`+%H%M`), день недели (`+%u`) и
арифметика запаса (`+%s`) не разъезжаются.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "sofascore"
ENV_SH = DEPLOY / "env.sh"
AUTO = DEPLOY / "auto_deliver.sh"
# Фиксированный PATH автомата (cron-гигиена): заглушки из PATH окружения он не видит.
PATH_LINE = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"

HIST = "dag_backfill_sofascore_all_mens"
REFRESH = "dag_refresh_sofascore_all_mens"
POOLS = ("ingest_scraper_pool", "sofascore_history_pool", "sofascore_players_pool")
GATEWAYS = ("sofascore_gw_951", "sofascore_gw_history", "sofascore_gw_players")
SCHEDULER = "sofascore-airflow-scheduler"
METADB = "sofascore-airflow-metadb"
# Четверг 03:30 UTC — середина окна доставки; воскресенье того же формата — для сдвига.
THU_0330 = "2026-09-03 03:30:00"
SUN_0330 = "2026-09-06 03:30:00"


def _script(directory: Path, name: str, body: str) -> Path:
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


def _epoch(when: str) -> int:
    return int(
        subprocess.run(["date", "-u", "-d", when, "+%s"], capture_output=True, text=True, check=True).stdout
    )


class Stand:
    """Стенд: репозиторий-источник, боевое дерево, каталоги состояния и заглушки."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.stubs = tmp_path / "bin"
        self.state = tmp_path / "auto-state"
        self.stub_state = tmp_path / "stub-state"
        self.releases = tmp_path / "releases"
        self.runtime = tmp_path / "runtime"
        self.log = tmp_path / "auto.log"
        for d in (self.stubs, self.state, self.stub_state, self.releases, self.runtime):
            d.mkdir(parents=True)
        self.source = tmp_path / "source.git"
        self.old_sha, self.new_sha = self._make_repo()
        self.old_tree = self.releases / f"release-{self.old_sha[:8]}"
        self.new_tree = self.releases / f"release-{self.new_sha[:8]}"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.old_tree)], check=True)
        _git(self.old_tree, "checkout", "-q", "--detach", self.old_sha)
        self.old_tree.chmod(0o755)
        (self.old_tree / "logs").mkdir()
        self.platform_env = tmp_path / "platform.env"
        self.platform_env.write_text("TRINO_PORT=8443\n", encoding="utf-8")
        self.tg_env = tmp_path / "telegram.env"   # намеренно не создаётся: исход уходит в очередь
        self.env_file = tmp_path / "sofascore.env"
        self._write_env(self.old_tree)
        self._defaults()
        self._make_stubs()
        self.watchdogs: list[subprocess.Popen] = []

    # -- репозиторий-источник --------------------------------------------------
    def _make_repo(self) -> tuple[str, str]:
        self.source.mkdir()
        _git(self.source, "init", "-q", "-b", "master")
        _script(
            self.source / "deploy" / "sofascore", "freeze_release.sh",
            '''set -e
sha="$1"
S="${STUB_STATE:?}"
env_file="${SOFASCORE_ENV_FILE:?}"
rel=$(sed -n 's/^SOFASCORE_RELEASES_DIR=//p' "$env_file" | head -1)
src=$(sed -n 's/^SOFASCORE_SOURCE_REPO=//p' "$env_file" | head -1)
[ -e "$S/freeze_fails" ] && { echo "заморозка не удалась" >&2; exit 1; }
# Заморозка идёт до 900 с (на стенде — столько, сколько попросил тест).
[ -e "$S/freeze_slow" ] && echo $(( $(cat "$S/now_epoch") + $(cat "$S/freeze_slow") )) > "$S/now_epoch"
tree="$rel/release-${sha:0:8}"
git clone -q "$src" "$tree"
git -C "$tree" checkout -q --detach "$sha"
chmod 755 "$tree"
mkdir -p "$tree/logs"
echo "дерево заморожено: $tree (sha ${sha:0:8})"
echo "дальше: bash deploy/sofascore/deploy.sh $tree"
''',
        )
        _script(
            self.source / "deploy" / "sofascore", "deploy.sh",
            '''S="${STUB_STATE:?}"
printf '%s|%s\\n' "$*" "${SOFASCORE_DEPLOY_IDLE_WAIT-<unset>}" >> "$S/deploy.calls"
rc=$(cat "$S/deploy_rc" 2>/dev/null || echo 0)
new="$1"
# rc=4 — «контур занят, выкат не начат»: пул и паузы возвращает on_exit самого deploy.sh,
# но best effort — здесь эмулируется случай, когда вернуть их он не смог.
[ "$rc" = 4 ] && [ -e "$S/deploy_leaves_history_paused" ] \
  && echo t > "$S/paused_dag_backfill_sofascore_all_mens"
if [ "$rc" = 0 ] || [ -e "$S/deploy_half" ]; then
  sed -i "s#^SOFASCORE_RELEASE_ROOT=.*#SOFASCORE_RELEASE_ROOT=$new#" "$SOFASCORE_ENV_FILE"
  sed -i "s#^SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST=.*#SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST=$new/artifact.json#" "$SOFASCORE_ENV_FILE"
  sed -i "s#^SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=.*#SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=deadbeef#" "$SOFASCORE_ENV_FILE"
  printf '%s\\n' "$new" > "$S/mounts_sched_root"
  echo "created-new" > "$S/created"
  echo "2026-09-03T03:40:00.000000000Z" > "$S/started"
  cat "$S/wd_pid_new" > "$S/wd_pid"
  [ -e "$S/deploy_eats_artifact_id" ] && sed -i '/^SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=/d' "$SOFASCORE_ENV_FILE"
  echo t > "$S/paused_dag_backfill_sofascore_all_mens"
  # Половинчатый выкат: scheduler уехал на новое дерево, шлюзы остались на старом.
  [ -e "$S/deploy_half" ] || printf '%s\\n' "$new" > "$S/mounts_root"
fi
exit "$rc"
''',
        )
        for name in ("airflow.compose.yaml", "gateway.compose.yaml"):
            (self.source / "deploy" / "sofascore" / name).write_text("services: {}\n", encoding="utf-8")
        (self.source / "marker.txt").write_text("one\n", encoding="utf-8")
        _git(self.source, "add", "-A")
        _git(self.source, "commit", "-q", "-m", "one")
        old = _git(self.source, "rev-parse", "HEAD")
        (self.source / "marker.txt").write_text("two\n", encoding="utf-8")
        _git(self.source, "commit", "-q", "-am", "two")
        new = _git(self.source, "rev-parse", "HEAD")
        return old, new

    # -- env-файл контура -------------------------------------------------------
    def _write_env(self, release_root: Path) -> None:
        self.env_file.write_text(
            "\n".join(
                [
                    f"SOFASCORE_AUTO_STATE_DIR={self.state}",
                    f"SOFASCORE_AUTO_LOG={self.log}",
                    f"SOFASCORE_TG_ENV={self.tg_env}",
                    f"SOFASCORE_METADB_CONTAINER={METADB}",
                    f"SOFASCORE_SCHEDULER_CONTAINER={SCHEDULER}",
                    f"SOFASCORE_RELEASE_ROOT={release_root}",
                    f"SOFASCORE_SOURCE_REPO={self.source}",
                    f"SOFASCORE_RELEASES_DIR={self.releases}",
                    f"SOFASCORE_PLATFORM_ENV_FILE={self.platform_env}",
                    f"SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST={self.runtime}/artifacts/old/workload_policy.json",
                    "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=cafebabe",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def env_value(self, key: str) -> str:
        for line in self.env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1]
        return ""

    # -- состояние заглушек -----------------------------------------------------
    def put(self, name: str, value: str = "1") -> None:
        (self.stub_state / name).write_text(f"{value}\n", encoding="utf-8")

    def _defaults(self) -> None:
        self.put("created", "created-old")
        self.put("started", "2026-09-03T00:00:00.000000000Z")
        self.put("mounts_root", str(self.old_tree))
        self.put("mounts_sched_root", str(self.old_tree))
        self.put("dags", "5")
        self.put("import_error", "0")
        self.put("busy", "0")
        self.put("gw_health", "healthy")
        self.put("gw_memory", "1073741824")
        self.put("gw_project", "sofascore-gw")
        self.put("now_epoch", str(_epoch(THU_0330)))
        for pool in POOLS:
            self.put(f"pool_{pool}", "1")
        for dag in (HIST, REFRESH):
            self.put(f"paused_{dag}", "f")

    # -- сторожа: настоящие процессы с настоящим /proc/<pid>/cmdline ------------
    def watchdog_pids(self) -> None:
        for name, tree in (("wd_pid_old", self.old_tree), ("wd_pid_new", self.new_tree)):
            proc = subprocess.Popen(
                ["/bin/sh", "-c", "sleep 120", "--expected-mount", str(tree)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.watchdogs.append(proc)
            self.put(name, str(proc.pid))
        self.put("wd_pid", (self.stub_state / "wd_pid_old").read_text().strip())

    def close(self) -> None:
        for proc in self.watchdogs:
            proc.kill()
            proc.wait()

    # -- заглушки ---------------------------------------------------------------
    def _make_stubs(self) -> None:
        s = self.stub_state
        _script(
            self.stubs, "date",
            f'''S="{s}"
FAKE=$(cat "$S/now_epoch" 2>/dev/null || true)
[ -n "$FAKE" ] || exec /bin/date "$@"
for a in "$@"; do [ "$a" = "-d" ] && exec /bin/date "$@"; done
fmt=""
for a in "$@"; do case "$a" in +*) fmt=$a ;; esac; done
[ -n "$fmt" ] || exec /bin/date "$@"
exec /bin/date -u -d "@$FAKE" "$fmt"
''',
        )
        _script(self.stubs, "sleep", "exit 0\n")
        _script(
            self.stubs, "pgrep",
            f'''case "$*" in
  *deploy*) [ -e "{s}/manual_deploy" ] && exit 0; exit 1 ;;
esac
exit 1
''',
        )
        _script(
            self.stubs, "systemctl",
            f'''S="{s}"
printf '%s\\n' "$*" >> "$S/systemctl.calls"
case "$1" in
  restart)
    # Сторож перечитывает EnvironmentFile при старте: после отката он смотрит на старое дерево.
    cat "$S/wd_pid_old" > "$S/wd_pid"
    exit 0 ;;
  show)
    case "$*" in
      *ActiveState*) cat "$S/wd_active" 2>/dev/null || echo active ;;
      *MainPID*) cat "$S/wd_pid" ;;
    esac
    exit 0 ;;
esac
exit 0
''',
        )
        _script(
            self.stubs, "docker",
            f'''S="{s}"
printf '%s\\n' "$*" >> "$S/docker.calls"
[ -e "$S/docker_down" ] && exit 1
sql="${{@: -1}}"
case "$1" in
  compose)
    printf '%s\\n' "$*" >> "$S/compose.calls"
    if [ -e "$S/rollback_works" ]; then
      cp "$S/rollback_root" "$S/mounts_root"
      cp "$S/rollback_root" "$S/mounts_sched_root"
      echo "2026-09-03T04:10:00.000000000Z" > "$S/started"
      echo "created-rollback" > "$S/created"
    fi
    exit 0 ;;
  logs) exit 0 ;;
  exec)
    if [ "$2" = "{METADB}" ]; then
      case "$sql" in
        *slot_pool*) pool=${{sql#*pool=\\'}}; pool=${{pool%%\\'*}}; cat "$S/pool_$pool" 2>/dev/null || echo "" ;;
        *is_paused*) dag=${{sql#*dag_id=\\'}}; dag=${{dag%%\\'*}}; cat "$S/paused_$dag" ;;
        *"FROM import_error"*) cat "$S/import_error" ;;
        *dag_run*) cat "$S/busy" ;;
        *last_parsed_time*) cat "$S/dags" ;;
        *) echo 0 ;;
      esac
      exit 0
    fi
    case "$4" in
      dags)
        case "$5" in
          pause) echo t > "$S/paused_$6" ;;
          unpause) [ -e "$S/unpause_fails" ] || echo f > "$S/paused_$6" ;;
        esac ;;
    esac
    exit 0 ;;
  inspect)
    fmt="$3"
    shift 3
    for c in "$@"; do
      case "$fmt" in
        *Created*) cat "$S/created" ;;
        *StartedAt*) cat "$S/started" ;;
        *Health.Status*)
          printf '%s %s %s\\n' "$(cat "$S/gw_health")" "$(cat "$S/gw_memory")" \\
            "$(cat "$S/gw_project_$c" 2>/dev/null || cat "$S/gw_project")" ;;
        *Mounts*)
          if [ "$c" = "{SCHEDULER}" ]; then
            root=$(cat "$S/mounts_sched_root"); n=10
          else
            root=$(cat "$S/mounts_root"); n=1
          fi
          i=1
          while [ "$i" -le "$n" ]; do echo "$root/part$i"; i=$(( i + 1 )); done
          echo "{self.runtime}/all-men" ;;
      esac
    done
    exit 0 ;;
esac
exit 0
''',
        )

    # -- запуск -----------------------------------------------------------------
    def install(self) -> Path:
        libexec = self.tmp / "libexec"
        libexec.mkdir(exist_ok=True)
        for src in (AUTO, ENV_SH):
            text = src.read_text(encoding="utf-8")
            if src == AUTO:
                assert text.count(PATH_LINE) == 1, "автомат перестал фиксировать PATH — правь тест"
                text = text.replace(PATH_LINE, PATH_LINE.replace("export PATH=", f"export PATH={self.stubs}:"))
            (libexec / src.name).write_text(text, encoding="utf-8")
            (libexec / src.name).chmod(0o755)
        return libexec / AUTO.name

    def run(self, **extra: str) -> subprocess.CompletedProcess:
        env = {
            "PATH": os.environ["PATH"],
            "HOME": os.environ.get("HOME", "/nonexistent"),
            "LANG": "C.UTF-8",
            "SOFASCORE_ENV_FILE": str(self.env_file),
            "STUB_STATE": str(self.stub_state),
            "ACCEPT_WAIT": "40",
            "ACCEPT_POLL": "20",
            "ROLLBACK_IDLE_WAIT": "0",
            **extra,
        }
        return subprocess.run(
            ["bash", str(self.install())], env=env, capture_output=True, text=True, timeout=120
        )

    # -- чтение результатов ------------------------------------------------------
    def calls(self, name: str) -> list[str]:
        path = self.stub_state / f"{name}.calls"
        return path.read_text(encoding="utf-8").splitlines() if path.exists() else []

    def write_snapshot(self, **override: str) -> None:
        """Снимок отката, каким его пишет сам автомат: пути обоих деревьев, пины артефакта,
        паузы обеих кампаний, слоты трёх пулов. Неполный снимок автомат обязан отвергнуть."""
        fields = {
            "OLD_RELEASE_ROOT": str(self.old_tree),
            "NEW_RELEASE_ROOT": str(self.new_tree),
            "OLD_ARTIFACT_HOST": f"{self.runtime}/artifacts/old/workload_policy.json",
            "OLD_ARTIFACT_ID": "cafebabe",
            "HIST_PAUSED": "f",
            "REFRESH_PAUSED": "f",
            **{f"POOL_{p}": "1" for p in POOLS},
            "SCHED_CREATED": "created-old",
        }
        fields.update(override)
        (self.state / "sofascore-rollback.env").write_text(
            "".join(f"{k}={v}\n" for k, v in fields.items() if v is not None), encoding="utf-8"
        )

    def log_text(self) -> str:
        return self.log.read_text(encoding="utf-8") if self.log.exists() else ""

    def pending(self) -> str:
        path = self.state / "sofascore-pending-alert"
        return path.read_text(encoding="utf-8") if path.exists() else ""


@pytest.fixture()
def stand(tmp_path: Path):
    st = Stand(tmp_path)
    yield st
    st.close()


@pytest.mark.unit
def test_the_test_repo_really_publishes_a_master_branch(stand: Stand) -> None:
    """`git init` без `-b master` при init.defaultBranch=main оставил бы refs/heads/master
    пустым: автомат прочитал бы это как «сеть недоступна» и молча вышел нулём, а весь
    сьют стал бы зелёным впустую."""
    out = subprocess.run(
        ["git", "ls-remote", str(stand.source), "refs/heads/master"],
        capture_output=True, text=True, check=True,
    ).stdout
    assert out.split("\t")[0] == stand.new_sha, out


@pytest.mark.unit
def test_without_the_env_file_the_automaton_does_nothing(tmp_path: Path) -> None:
    proc = subprocess.run(
        ["bash", str(AUTO)],
        env={"PATH": os.environ["PATH"], "SOFASCORE_ENV_FILE": str(tmp_path / "missing.env")},
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 2
    assert "нет env-файла" in proc.stderr
    assert not list(tmp_path.iterdir()), "ни замка, ни лога, ни маркеров без env-файла"


@pytest.mark.unit
def test_the_switch_stops_the_automaton_and_the_lock_lands_in_the_state_dir(stand: Stand) -> None:
    (stand.state / "sofascore-auto-deliver.off").touch()
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert (stand.state / "sofascore-auto-deliver.lock").is_file(), "замок — в каталоге состояния из env-файла"
    assert not stand.calls("compose"), stand.calls("compose")
    assert not stand.calls("deploy"), "бой не тронут"


@pytest.mark.unit
def test_a_missing_state_dir_is_reported_and_never_created(stand: Stand) -> None:
    """Пустой каталог состояния, созданный молча, снял бы fail-closed: вместе с ним
    исчезают маркер незакрытой доставки, снимок отката и выключатель."""
    gone = stand.tmp / "auto-state-gone"
    stand.env_file.write_text(
        stand.env_file.read_text(encoding="utf-8").replace(
            f"SOFASCORE_AUTO_STATE_DIR={stand.state}", f"SOFASCORE_AUTO_STATE_DIR={gone}"
        ),
        encoding="utf-8",
    )
    proc = stand.run()
    assert proc.returncode != 0
    assert "КАТАЛОГ СОСТОЯНИЯ" in stand.log_text() and str(gone) in stand.log_text()
    assert not gone.exists()


@pytest.mark.unit
@pytest.mark.parametrize("key", ["SOFASCORE_AUTO_STATE_DIR", "SOFASCORE_TG_ENV", "SOFASCORE_SCHEDULER_CONTAINER"])
def test_a_missing_key_fails_closed_before_the_lock(stand: Stand, key: str) -> None:
    stand.env_file.write_text(
        "\n".join(
            line for line in stand.env_file.read_text(encoding="utf-8").splitlines()
            if not line.startswith(f"{key}=")
        ) + "\n",
        encoding="utf-8",
    )
    proc = stand.run()
    assert proc.returncode != 0
    assert key in proc.stderr, proc.stderr
    assert not (stand.state / "sofascore-auto-deliver.lock").exists()
    assert not stand.calls("deploy")


def _production_on_master(stand: Stand) -> None:
    """Бой после нормальной ротации: своё замороженное дерево на master, контейнеры на нём.
    Именно так это выглядит в жизни — правка внутри замороженного дерева в бою запрещена.
    Сторожа тоже на этом дереве: маркер приёмки выдаётся только по полному контракту из шести
    признаков, и сторож с `--expected-mount` на прежнее дерево — законный повод его не выдать."""
    subprocess.run(["git", "clone", "-q", str(stand.source), str(stand.new_tree)], check=True)
    _git(stand.new_tree, "checkout", "-q", "--detach", stand.new_sha)
    stand.new_tree.chmod(0o755)
    (stand.new_tree / "logs").mkdir()
    stand._write_env(stand.new_tree)
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.watchdog_pids()
    stand.put("wd_pid", (stand.stub_state / "wd_pid_new").read_text().strip())


@pytest.mark.unit
def test_nothing_happens_when_production_already_runs_master(stand: Stand) -> None:
    """Совпали и контур целиком на одном дереве — к бою больше не обращаемся: ни окна, ни
    выката, ни защёлки."""
    _production_on_master(stand)
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert not stand.calls("compose") and not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))
    assert (stand.state / "sofascore-accepted").read_text(encoding="utf-8").strip() == stand.new_sha


@pytest.mark.unit
def test_a_mixed_contour_is_never_silently_declared_accepted(stand: Stand) -> None:
    """Ревью Sol, раунд 2. `deploy.sh` — в том числе ручной, по слову «выкатывай» —
    перепинивает env-файл ДО пересоздания контейнеров. Обрыв ровно в этой щели даёт env на
    новом дереве при контейнерах на старом: HEAD сходится с master, а контур смешанный.
    Раньше автомат в этом состоянии молча писал маркер приёмки и выходил нулём каждые пять
    минут — навсегда, без единого сообщения."""
    _production_on_master(stand)
    stand.put("mounts_root", str(stand.old_tree))       # шлюзы остались на прежнем дереве
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "КОНТУР СМЕШАННЫЙ" in stand.log_text()
    assert not (stand.state / "sofascore-accepted").exists(), "маркер приёмки не выдаётся авансом"
    assert "контур смешанный" in stand.pending() and "НУЖНЫ РУКИ" in stand.pending()
    assert not stand.calls("deploy")
    # Второй тик тех же суток повторяет отказ, но не повторяет сообщение.
    first = stand.pending()
    stand.run()
    assert stand.pending() == first


@pytest.mark.unit
def test_a_matching_head_with_docker_down_leaves_the_acceptance_marker_alone(stand: Stand) -> None:
    _production_on_master(stand)
    stand.put("docker_down")
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (stand.state / "sofascore-accepted").exists()
    assert "проверить монты нечем" in stand.log_text()


@pytest.mark.unit
def test_an_unreachable_source_repo_stops_the_tick_without_touching_anything(stand: Stand) -> None:
    stand.env_file.write_text(
        stand.env_file.read_text(encoding="utf-8").replace(
            f"SOFASCORE_SOURCE_REPO={stand.source}", f"SOFASCORE_SOURCE_REPO={stand.tmp}/no-such-repo"
        ),
        encoding="utf-8",
    )
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert "master недоступен" in stand.log_text()
    assert not stand.calls("compose") and not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))


@pytest.mark.unit
def test_a_manual_deploy_in_progress_skips_the_tick(stand: Stand) -> None:
    """deploy.sh — инструмент владельца по слову «выкатывай»: запрещать его автомат не
    вправе, но и лезть под него не должен."""
    stand.put("manual_deploy")
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert "идёт ручной выкат" in stand.log_text()
    assert not stand.calls("deploy")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("when", "hhmm_note"),
    [(f"2026-09-03 02:00:00", "до окна"), (f"2026-09-03 07:00:00", "после окна")],
    ids=["before-window", "after-window"],
)
def test_outside_the_window_the_automaton_is_silent(stand: Stand, when: str, hhmm_note: str) -> None:
    stand.put("now_epoch", str(_epoch(when)))
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr + hhmm_note
    assert not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))


@pytest.mark.unit
def test_sunday_closes_the_window_before_the_manifest_maintenance(stand: Stand) -> None:
    """Воскресный dag_sofascore_manifest_maintenance стартует в 05:00 UTC, а гейт «контур
    свободен» проверяется ОДИН раз — до осушения, которое длится десятки минут."""
    stand.put("now_epoch", str(_epoch("2026-09-06 04:50:00")))   # воскресенье, после 04:45
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert not stand.calls("deploy")
    # В четверг тот же час — рабочий: запаса ещё хватает.
    stand.put("now_epoch", str(_epoch("2026-09-03 04:50:00")))
    stand.run()
    assert stand.calls("deploy"), "в будни 04:50 — ещё окно"


@pytest.mark.unit
def test_no_headroom_left_means_no_delivery_and_no_latch(stand: Stand) -> None:
    """Запас = дедлайн − потолок выката − ожидание приёмки. Меньше MIN_DRAIN — не начинаем
    вовсе: доставка упёрлась бы в дедлайн уже после пересоздания контейнеров."""
    stand.put("now_epoch", str(_epoch("2026-09-03 05:50:00")))
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert "запаса нет" in stand.log_text()
    assert not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))


@pytest.mark.unit
def test_five_minutes_before_the_deadline_the_missed_window_is_announced_once(stand: Stand) -> None:
    stand.put("now_epoch", str(_epoch("2026-09-03 05:57:00")))
    stand.run()
    assert "окно доставки закрывается" in stand.pending()
    first = stand.pending()
    stand.run()
    assert stand.pending() == first, "второй тик тех же суток молчит"


@pytest.mark.unit
def test_a_busy_contour_skips_the_tick(stand: Stand) -> None:
    stand.put("busy", "1")
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert "контур занят" in stand.log_text()
    assert not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))


@pytest.mark.unit
def test_a_failed_freeze_costs_neither_the_latch_nor_the_inflight_marker(stand: Stand) -> None:
    stand.put("freeze_fails")
    proc = stand.run()
    assert proc.returncode == 1, proc.stderr
    assert "заморозка дерева" in stand.pending()
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))
    assert not (stand.state / "sofascore-inflight").exists()
    assert not stand.calls("deploy")


@pytest.mark.unit
@pytest.mark.parametrize("break_it", ["no-logs", "mode-0700"], ids=["missing-logs-dir", "wrong-mode"])
def test_a_broken_leftover_release_dir_is_never_reused(stand: Stand, break_it: str) -> None:
    """freeze_release.sh:44 делает `mv` ДО `chmod 755` и до `mkdir logs`, а
    airflow.compose.yaml монтирует ${ROOT}/logs с create_host_path: false. Пересоздать
    такой каталог заморозка откажется (:43) — переиспользовать можно только целый."""
    subprocess.run(["git", "clone", "-q", str(stand.source), str(stand.new_tree)], check=True)
    _git(stand.new_tree, "checkout", "-q", "--detach", stand.new_sha)
    if break_it == "no-logs":
        stand.new_tree.chmod(0o755)
    else:
        (stand.new_tree / "logs").mkdir()
        stand.new_tree.chmod(0o700)
    proc = stand.run()
    assert proc.returncode == 1, proc.stderr
    assert "БИТЫЙ" in stand.log_text()
    assert "не годится" in stand.pending()
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_the_happy_path_delivers_accepts_and_restores_the_snapshot(stand: Stand) -> None:
    """Сквозной успех: заморозка → deploy.sh нового дерева → приёмка по шести признакам →
    маркер принятого sha, снятый INFLIGHT, паузы и пулы как до доставки, ✅ в очереди."""
    stand.watchdog_pids()
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()

    deploy = stand.calls("deploy")
    assert len(deploy) == 1, deploy
    args, idle = deploy[0].split("|")
    assert args.split() == [str(stand.new_tree), str(stand.old_tree)], args
    # Запас на осушение доезжает окружением процесса, а не через env-файл контура:
    # любой ключ вне SOFASCORE_*/PROXY_FILTER_SOFASCORE_* уронил бы все три скрипта ротации.
    assert idle.isdigit() and int(idle) >= 900, idle
    assert "SOFASCORE_DEPLOY_IDLE_WAIT" not in stand.env_file.read_text(encoding="utf-8")

    assert (stand.state / "sofascore-accepted").read_text(encoding="utf-8").strip() == stand.new_sha
    assert not (stand.state / "sofascore-inflight").exists()
    assert (stand.state / f"sofascore-auto-deliver-attempted-2026-09-03").exists()
    assert not (stand.state / "sofascore-auto-deliver.off").exists()
    assert "доставлено" in stand.pending() and stand.new_sha[:8] in stand.pending()
    assert not stand.calls("compose"), "отката не было"
    # deploy.sh оставляет историю на паузе; автомат возвращает её к снимку — иначе кампания
    # стояла бы до утра, а ради этого автомат и заведён.
    assert (stand.stub_state / f"paused_{HIST}").read_text().strip() == "f"
    assert (stand.stub_state / f"paused_{REFRESH}").read_text().strip() == "f"


@pytest.mark.unit
def test_rc4_means_the_contour_was_busy_and_nothing_was_touched(stand: Stand) -> None:
    """Код 4 deploy.sh — «контур занят, выкат не начат»: откатывать нечего, защёлку сутки
    держать не за что. Второй попытки за ночь всё равно не будет: IDLE_WAIT — это запас до
    дедлайна, и следующий тик не пройдёт порог MIN_DRAIN."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "4")
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (stand.state / "sofascore-auto-deliver-attempted-2026-09-03").exists()
    assert not (stand.state / "sofascore-inflight").exists()
    assert not stand.calls("compose"), "отката не было"
    assert "контур не освободился" in stand.log_text()
    assert (stand.stub_state / f"paused_{HIST}").read_text().strip() == "f"
    for pool in POOLS:
        assert (stand.stub_state / f"pool_{pool}").read_text().strip() == "1"


@pytest.mark.unit
def test_a_delivery_timeout_is_named_and_rolled_back(stand: Stand) -> None:
    stand.watchdog_pids()
    stand.put("deploy_rc", "124")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ТАЙМАУТ доставки" in stand.log_text()
    assert stand.calls("compose"), "откат комплектом"
    assert "⛔" in stand.pending()


@pytest.mark.unit
def test_a_failed_deploy_rolls_the_whole_kit_back_to_the_old_tree(stand: Stand) -> None:
    """Откат комплектом: env-файл возвращается ПЕРВЫМ и перечитывается (без этой сверки
    compose пересоздал бы контейнеры обратно на НОВОЕ дерево, то есть «откат» доставил бы),
    затем оба compose из СТАРОГО дерева, затем сторожа."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")           # выкат успел перепиновать env и увести scheduler
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr

    assert stand.env_value("SOFASCORE_RELEASE_ROOT") == str(stand.old_tree)
    assert stand.env_value("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID") == "cafebabe"
    compose = stand.calls("compose")
    assert len(compose) == 2, compose
    assert f"-f {stand.old_tree}/deploy/sofascore/airflow.compose.yaml" in compose[0], compose[0]
    assert f"-f {stand.old_tree}/deploy/sofascore/gateway.compose.yaml" in compose[1], compose[1]
    assert f"--project-directory {stand.old_tree}" in compose[1], compose[1]
    assert "--no-deps --force-recreate airflow-scheduler" in compose[0], compose[0]
    assert compose[1].endswith("sofascore_proxy_filter sofascore_gw_history sofascore_gw_players"), compose[1]
    restarts = [c for c in stand.calls("systemctl") if c.startswith("restart ")]
    assert len(restarts) == 3, restarts
    assert "⛔" in stand.pending() and "Откат на" in stand.pending()
    assert not (stand.state / "sofascore-inflight").exists()
    assert not (stand.state / "sofascore-auto-deliver.off").exists(), "одна ночь — ещё не повод глушиться"


@pytest.mark.unit
def test_a_half_delivered_contour_is_not_accepted(stand: Stand) -> None:
    """Scheduler на новом дереве, шлюзы на старом: без проверки монтов ВСЕХ четырёх
    контейнеров такой выкат прошёл бы приёмку."""
    stand.watchdog_pids()
    stand.put("deploy_half")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ПРОВАЛ доставки" in stand.log_text()
    assert stand.calls("compose"), "половинчатый выкат обязан быть откачен"


@pytest.mark.unit
def test_a_foreign_container_with_our_name_is_not_accepted(stand: Stand) -> None:
    """Рядом живёт ЧУЖОЙ контейнер, буквально названный sofascore_proxy_filter (проект
    dpf-whoscored-merge), и он тоже healthy. Отличаем по метке проекта, не по здоровью."""
    stand.watchdog_pids()
    stand.put("gw_project_sofascore_gw_951", "dpf-whoscored-merge")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert stand.calls("compose"), "чужой шлюз приёмкой не считается"


@pytest.mark.unit
def test_an_unknown_answer_waits_and_then_rolls_back_instead_of_declaring_success(stand: Stand) -> None:
    """«Не знаю» (метабаза или docker недоступны) — не приёмка и не провал: ждём в цикле,
    по исчерпании ожидания идём в откат, а не объявляем успех."""
    stand.watchdog_pids()
    stand.put("wd_active", "activating")   # сторож ещё не поднялся — приёмка не сходится
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "приёмки пока нет" in stand.log_text()
    assert (stand.state / "sofascore-auto-deliver.off").exists(), "неподтверждённый откат — руки"
    assert (stand.state / "sofascore-inflight").exists(), "маркер незакрытой доставки остаётся"
    assert "НУЖНЫ РУКИ" in stand.pending()


@pytest.mark.unit
def test_an_unconfirmed_rollback_shuts_the_automaton_off_and_keeps_the_marker(stand: Stand) -> None:
    stand.watchdog_pids()
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    proc = stand.run()                     # rollback_works не выставлен: бой остался на новом
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert (stand.state / "sofascore-inflight").exists()
    assert "🆘" in stand.pending() and "sofascore_allocations.json" in stand.pending()


@pytest.mark.unit
def test_an_interrupted_delivery_that_never_moved_production_only_clears_the_marker(stand: Stand) -> None:
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "бой целиком на" in stand.log_text()
    assert not stand.calls("compose"), "откатывать нечего"
    assert not (stand.state / "sofascore-inflight").exists()
    assert "⚠️" in stand.pending()


@pytest.mark.unit
def test_an_interrupted_delivery_that_moved_production_is_rolled_back(stand: Stand) -> None:
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "откатываю комплектом" in stand.log_text()
    assert len(stand.calls("compose")) == 2, stand.calls("compose")
    assert not (stand.state / "sofascore-inflight").exists()
    assert "⛔" in stand.pending()


@pytest.mark.unit
def test_an_interrupted_delivery_before_the_containers_still_puts_the_env_file_back(stand: Stand) -> None:
    """Ревью Sol, раунд 1. deploy.sh перепинивает env-файл ДО пересоздания контейнеров.
    Обрыв ровно в этой щели оставляет env на новом дереве при контейнерах на старом, и
    раньше автомат просто снимал маркер: следующий тик читал HEAD уже НОВОГО дерева, видел
    совпадение с master, писал маркер приёмки без единой проверки и молча выходил нулём
    каждые пять минут — контур навсегда оставался смешанным."""
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    # Так выглядит контур после обрыва: env уже на новом дереве, контейнеры — ещё на старом.
    stand.env_file.write_text(
        stand.env_file.read_text(encoding="utf-8")
        .replace(f"SOFASCORE_RELEASE_ROOT={stand.old_tree}", f"SOFASCORE_RELEASE_ROOT={stand.new_tree}")
        .replace("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=cafebabe", "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=deadbeef"),
        encoding="utf-8",
    )
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert stand.env_value("SOFASCORE_RELEASE_ROOT") == str(stand.old_tree)
    assert stand.env_value("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID") == "cafebabe"
    assert not stand.calls("compose"), "контейнеры не трогали — их и не надо пересоздавать"
    assert not (stand.state / "sofascore-inflight").exists()
    assert "env-файл, паузы и пулы вернул к снимку" in stand.pending()


@pytest.mark.unit
def test_a_delivery_that_leaves_the_campaign_paused_is_not_reported_as_success(stand: Stand) -> None:
    """Ревью Sol, раунд 1. deploy.sh штатно оставляет историю на паузе; вернуть её —
    единственное, ради чего автомат и заведён. Раньше неудача возврата писала
    MANUAL ACTION REQUIRED в лог, а наружу всё равно уходило ✅ — прямое «зелёное, но
    пустое»: код в бою, приёмка сошлась, кампания стоит до утра."""
    stand.watchdog_pids()
    stand.put("unpause_fails")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "🆘" in stand.pending() and "✅" not in stand.pending()
    assert "контур не вернулся в рабочее состояние" in stand.pending()
    assert HIST in stand.pending()
    # Доставка всё-таки состоялась и принята — маркер приёмки честный, INFLIGHT закрыт.
    assert (stand.state / "sofascore-accepted").read_text(encoding="utf-8").strip() == stand.new_sha
    assert not (stand.state / "sofascore-inflight").exists()
    # Выключатель обязателен: контур стоит, и следующий коммит начал бы новый выкат прямо
    # на остановленной кампании.
    assert (stand.state / "sofascore-auto-deliver.off").exists()


@pytest.mark.unit
def test_a_rollback_checks_all_three_env_lines_against_the_file(stand: Stand) -> None:
    """Ревью Sol, раунд 1. Сверялась одна строка из трёх, и сверялась по переменной
    оболочки: sofascore_load_env снимает только ключи, которые есть в файле, поэтому
    исчезнувшая строка оставляла в памяти прежнее значение и проверка её не видела. Старые
    контейнеры поднялись бы с чужим артефактом бюджета, а приёмка env-файл не читает."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    stand.put("deploy_eats_artifact_id")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "ENV-ФАЙЛ НЕ ВЕРНУЛСЯ К СНИМКУ" in stand.log_text()
    assert not stand.calls("compose"), "без вернувшегося env пересоздавать контейнеры нельзя"
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert (stand.state / "sofascore-inflight").exists()


@pytest.mark.unit
def test_an_interrupted_delivery_that_cannot_restart_the_campaign_is_not_a_warning(stand: Stand) -> None:
    """Ревью Sol, раунд 2. Раньше ⚠️ уходило и маркер снимался ДО восстановления пауз и
    пулов — оно шло EXIT-trap'ом. Осушённый пул или запаузенная история не попадали ни в
    одно сообщение: автомат отчитывался «ничего страшного» о ночи, в которой кампания не
    работает."""
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    stand.put("unpause_fails")
    stand.put(f"paused_{HIST}", "t")     # deploy.sh успел запаузить историю до обрыва
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "🆘" in stand.pending() and "⚠️" not in stand.pending()
    assert "контур не вернулся в рабочее состояние" in stand.pending()
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert not (stand.state / "sofascore-inflight").exists()


@pytest.mark.unit
@pytest.mark.parametrize(
    "drop", ["OLD_ARTIFACT_HOST", "OLD_ARTIFACT_ID", "POOL_sofascore_history_pool", "HIST_PAUSED"]
)
def test_an_incomplete_snapshot_is_refused_like_a_missing_one(stand: Stand, drop: str) -> None:
    """Ревью Sol, раунд 2. Неполный снимок опаснее отсутствующего: пустые пины артефакта
    проходили сверку как «успешно восстановленные», и старые контейнеры поднялись бы с
    пустым (fail-closed для compose) бюджетом, а маркер доставки при этом снимался."""
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(**{drop: None})
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "снимок отката неполон" in stand.log_text()
    assert not stand.calls("compose")
    assert (stand.state / "sofascore-inflight").exists()
    assert (stand.state / "sofascore-auto-deliver.off").exists()


@pytest.mark.unit
def test_an_interrupted_delivery_without_a_snapshot_touches_nothing(stand: Stand) -> None:
    (stand.state / "sofascore-inflight").touch()
    proc = stand.run()
    assert proc.returncode == 1, proc.stderr
    assert not stand.calls("compose")
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert (stand.state / "sofascore-inflight").exists()


@pytest.mark.unit
@pytest.mark.parametrize(
    "marker",
    ["sofascore-inflight", "sofascore-accepted", "sofascore-rollback.env",
     "sofascore-pending-alert", "sofascore-auto-deliver.off", "sofascore-fail-nights"],
)
def test_a_substituted_marker_stops_everything(stand: Stand, marker: str) -> None:
    """Symlink принимает запись с нулевым кодом и читается пустым, каталог рвёт
    перенаправление, FIFO вешает чтение навсегда вместе с замком. Ни один из них — не
    «маркера нет»."""
    (stand.state / marker).symlink_to(stand.tmp / "elsewhere")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "НЕ ОБЫЧНЫЙ ФАЙЛ" in stand.log_text(), stand.log_text()
    assert (stand.state / "sofascore-auto-deliver.off").is_file() or marker.endswith(".off")
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_a_busy_lock_exits_quietly(stand: Stand) -> None:
    lock = stand.state / "sofascore-auto-deliver.lock"
    lock.touch()
    holder = subprocess.Popen(["flock", str(lock), "sleep", "20"])
    try:
        proc = stand.run()
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert not stand.calls("deploy")
        assert stand.log_text() == "", "занятый замок — не событие"
    finally:
        holder.kill()
        holder.wait()


@pytest.mark.unit
def test_three_failed_nights_in_a_row_shut_the_automaton_off(stand: Stand) -> None:
    stand.watchdog_pids()
    (stand.state / "sofascore-fail-nights").write_text("2\n", encoding="utf-8")
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert (stand.state / "sofascore-fail-nights").read_text(encoding="utf-8").strip() == "3"
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert "Больше не пробую" in stand.pending()


@pytest.mark.unit
def test_an_unreadable_platform_env_stops_the_delivery_before_it_breaks_the_contour(stand: Stand) -> None:
    """Без общего .env платформы compose упал бы уже ПОСЛЕ перепина env-файла контура —
    то есть контур остался бы наполовину переставленным."""
    stand.platform_env.unlink()
    proc = stand.run()
    assert proc.returncode == 1, proc.stderr
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_a_dirty_production_tree_is_never_used_as_a_rollback_target(stand: Stand) -> None:
    """Цель отката — путь к дереву, которое стоит в бою СЕЙЧАС. Грязное дерево такой целью
    быть не может: откат вернул бы не то, что было."""
    (stand.old_tree / "marker.txt").write_text("правка на месте\n", encoding="utf-8")
    proc = stand.run()
    assert proc.returncode == 1, proc.stderr
    assert "НЕ В ЗАКОННОМ СОСТОЯНИИ" in stand.log_text()
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_rc4_that_leaves_the_campaign_stopped_is_an_alarm_not_a_warning(stand: Stand) -> None:
    """Ревью Sol, раунд 3, находка 2. Код 4 безопасен для БОЯ, но не для кампании: к этому
    моменту deploy.sh уже осушил пул и запаузил актуалку, а его собственный возврат —
    best effort. Раньше автомат выходил нулём, и неудача restore_state уходила в EXIT-trap:
    наружу шло ⚠️ «ничего страшного» о ночи, в которую кампания стоит."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "4")
    stand.put("deploy_leaves_history_paused")
    stand.put("unpause_fails")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "🆘" in stand.pending() and "⚠️" not in stand.pending()
    assert "контур не вернулся в рабочее состояние" in stand.pending()
    assert HIST in stand.pending()
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert not stand.calls("compose"), "выкат не начинался — откатывать нечего"
    assert not (stand.state / "sofascore-inflight").exists()


@pytest.mark.unit
def test_a_confirmed_rollback_with_a_stopped_campaign_is_an_alarm(stand: Stand) -> None:
    """Ревью Sol, раунд 3, находка 3. Исход отката решался одной приёмкой, а код возврата
    restore_state игнорировался: бой возвращался на старое дерево, кампания оставалась
    стоять, и уходило ⛔ «попробуем завтра». Следующий коммит начал бы новую доставку прямо
    на остановленном контуре."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    stand.put("unpause_fails")     # история осталась на паузе после выката
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert stand.calls("compose"), "откат комплектом всё равно состоялся"
    assert "🆘" in stand.pending() and "⛔" not in stand.pending()
    assert "контур не вернулся в рабочее состояние" in stand.pending()
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert not (stand.state / "sofascore-inflight").exists(), "откат подтверждён — маркер снят"


@pytest.mark.unit
def test_the_headroom_is_recounted_after_the_freeze_ate_the_window(stand: Stand) -> None:
    """Ревью Sol, раунд 3, находка 4. Запас считался ДО заморозки, а она идёт до 900 с:
    воскресный старт в 03:30 уезжал к 05:00 против объявленного дедлайна 04:45, прямо на
    обслуживание манифеста. Пересчёт стоит до снимка отката: после него любой выход шёл бы
    через EXIT-trap, и неудача возврата контура осталась бы немой."""
    stand.put("freeze_slow", "7000")     # заморозка длилась почти два часа
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "после заморозки запаса нет" in stand.log_text()
    assert not stand.calls("deploy"), "выкат не начинаем — он упёрся бы в дедлайн"
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))
    assert not (stand.state / "sofascore-inflight").exists()


@pytest.mark.unit
def test_a_matching_head_without_the_full_contract_gets_no_acceptance_marker(stand: Stand) -> None:
    """Ревью Sol, раунд 3, находка 5. Совпадение HEAD проверялось одними монтами, а маркер
    приёмки выдавался и fail-ночи сбрасывались — то есть «принято» объявлялось по одному
    признаку из шести. Сторож, оставшийся на прежнем дереве, — ровно тот случай: код в бою,
    а следит за ним чужой процесс."""
    _production_on_master(stand)
    stand.put("wd_pid", (stand.stub_state / "wd_pid_old").read_text().strip())
    (stand.state / "sofascore-fail-nights").write_text("2\n", encoding="utf-8")
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (stand.state / "sofascore-accepted").exists(), "маркер приёмки не выдаётся авансом"
    assert "КОНТРАКТ ПРИЁМКИ НЕ СОШЁЛСЯ" in stand.log_text()
    assert "⚠️" in stand.pending() and "контракт приёмки" in stand.pending()
    assert (stand.state / "sofascore-fail-nights").exists(), "счётчик провальных ночей не сбрасываем"
    # Второй тик тех же суток повторяет отказ, но не повторяет сообщение.
    first = stand.pending()
    stand.run()
    assert stand.pending() == first


@pytest.mark.unit
def test_an_unreachable_master_still_announces_the_closing_window(stand: Stand) -> None:
    """Ревью Sol, раунд 3, находка 6. Недоступный master завершал тик ДО разговора об окне,
    поэтому обещанный ежесуточный сигнал не звучал ни разу за такую ночь — а бой остаётся на
    старом коде ровно так же, как при неудачной доставке."""
    stand.env_file.write_text(
        stand.env_file.read_text(encoding="utf-8").replace(
            f"SOFASCORE_SOURCE_REPO={stand.source}", f"SOFASCORE_SOURCE_REPO={stand.tmp}/no-such-repo"
        ),
        encoding="utf-8",
    )
    stand.put("now_epoch", str(_epoch("2026-09-03 05:57:00")))
    proc = stand.run()
    assert proc.returncode == 0, proc.stderr
    assert "master недоступен" in stand.log_text()
    assert "окно доставки закрывается" in stand.pending() and "master недоступен" in stand.pending()
    assert not stand.calls("deploy")
    # Один маркер на все причины: второй тик тех же суток молчит.
    first = stand.pending()
    stand.run()
    assert stand.pending() == first
