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
# Слоты полос: история идёт батчем из трёх скоупов (#1248 ступень 1), остальные серийны.
POOL_SLOTS = {
    "ingest_scraper_pool": "1",
    "sofascore_history_pool": "3",
    "sofascore_players_pool": "1",
}
GATEWAYS = ("sofascore_gw_951", "sofascore_gw_history", "sofascore_gw_players")
SCHEDULER = "sofascore-airflow-scheduler"
METADB = "sofascore-airflow-metadb"
MAINT = "dag_sofascore_manifest_maintenance"
# Четверг 03:30 UTC — середина окна доставки; воскресенье того же формата — для сдвига.
THU_0330 = "2026-09-03 03:30:00"
SUN_0330 = "2026-09-06 03:30:00"


def _installed_text(src: Path, stubs: Path) -> str:
    """Текст копии, какой её кладёт стенд: автомат — с каталогом заглушек впереди PATH.
    Та же копия лежит в тестовом репозитории (#1362): автомат сверяет себя с релизом по md5,
    и релиз стенда обязан нести ровно установленный текст — иначе самоустановка поставила бы
    копию с настоящим PATH, и следующий тик стенда пошёл бы в живой docker."""
    text = src.read_text(encoding="utf-8")
    if src == AUTO:
        assert text.count(PATH_LINE) == 1, "автомат перестал фиксировать PATH — правь тест"
        text = text.replace(PATH_LINE, PATH_LINE.replace("export PATH=", f"export PATH={stubs}:"))
    return text


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


def _window(stand: "Stand", day: str) -> dict[str, str]:
    """Запись окна `sofascore-window-<day>.env` как словарь (пустой — записи нет)."""
    path = stand.state / f"sofascore-window-{day}.env"
    if not path.exists():
        return {}
    return dict(line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line)


def _put_window(stand: "Stand", day: str, **fields: str) -> None:
    """Запись окна, какой её оставил прошлый тик (открытая — без OUTCOME)."""
    base = {"WINDOW_ID": day, "LIVE": stand.old_sha, "TARGET": stand.new_sha, "LAST_REASON": ""}
    base.update(fields)
    (stand.state / f"sofascore-window-{day}.env").write_text(
        "".join(f"{k}={v}\n" for k, v in base.items()), encoding="utf-8"
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
printf '%s|%s|%s|%s\\n' "$*" "${SOFASCORE_DEPLOY_IDLE_WAIT-<unset>}" \\
  "${SOFASCORE_DEPLOY_WINDOW_ID-<unset>}" "${SOFASCORE_DEPLOY_LOCK_FD-<unset>}" >> "$S/deploy.calls"
# Замок выката держит автомат весь тик: снаружи он не берётся, пока идёт доставка.
flock -n "${SOFASCORE_DEPLOY_LOCK:?}" true 2>/dev/null \\
  && echo free >> "$S/lock.probe" || echo held >> "$S/lock.probe"
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
        for src in (AUTO, ENV_SH):
            copy = self.source / "deploy" / "sofascore" / src.name
            copy.write_text(_installed_text(src, self.stubs), encoding="utf-8")
            copy.chmod(0o755)
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
                    # Замок выката (#1245): один протокол на автомат и ручной deploy.sh.
                    f"SOFASCORE_DEPLOY_LOCK={self.runtime}/deploy.lock",
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

    def _flock_stub(self) -> None:
        _script(
            self.stubs, "flock",
            '''S="${STUB_STATE:?}"
# Чужой ручной выкат «заканчивается» ровно между первым чтением env-файла и замком:
# автомат обязан перечитать env под замком, иначе снимок отката укажет на дерево,
# которого в бою уже нет.
if [ -e "$S/env_swap_root" ] && [ ! -e "$S/env_swapped" ]; then
  : > "$S/env_swapped"
  sed -i "s#^SOFASCORE_RELEASE_ROOT=.*#SOFASCORE_RELEASE_ROOT=$(cat "$S/env_swap_root")#" \
    "${SOFASCORE_ENV_FILE:?}"
fi
exec /usr/bin/flock "$@"
''',
        )

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
        self._flock_stub()
        self.put("gw_health", "healthy")
        self.put("gw_memory", "1073741824")
        self.put("gw_project", "sofascore-gw")
        self.put("now_epoch", str(_epoch(THU_0330)))
        self.today = THU_0330.split(" ")[0]
        for pool in POOLS:
            self.put(f"pool_{pool}", POOL_SLOTS[pool])
        for dag in (HIST, REFRESH):
            self.put(f"paused_{dag}", "f")
        # Обслуживание манифеста живёт на паузе и распаущивается только своим прогоном:
        # deploy.sh паузит его на выкат, автомат возвращает после приёмки или отката.
        self.put(f"paused_{MAINT}", "t")

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
          pause)
            [ -e "$S/pause_maint_fails" ] && [ "$6" = "dag_sofascore_manifest_maintenance" ] || echo t > "$S/paused_$6" ;;
          unpause)
            skip=0
            [ -e "$S/unpause_fails" ] && skip=1
            [ -e "$S/unpause_maint_fails" ] && [ "$6" = "dag_sofascore_manifest_maintenance" ] && skip=1
            [ "$skip" = 1 ] || echo f > "$S/paused_$6" ;;
        esac ;;
    esac
    exit 0 ;;
  inspect)
    # «docker не отвечает»: код 1 и пустой вывод — так автомат отличает «не знаю» от «нет».
    [ -e "$S/inspect_fails" ] && exit 1
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
            (libexec / src.name).write_text(_installed_text(src, self.stubs), encoding="utf-8")
            (libexec / src.name).chmod(0o755)
        return libexec / AUTO.name

    def run(self, *, keep_copy: bool = False, **extra: str) -> subprocess.CompletedProcess:
        """Один тик cron. keep_copy — не переустанавливать копию: тик идёт тем, что в
        libexec сейчас (в том числе копией, которую автомат поставил себе сам)."""
        script = self.tmp / "libexec" / AUTO.name if keep_copy else self.install()
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
            ["bash", str(script)], env=env, capture_output=True, text=True, timeout=120
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
            "SNAPSHOT_VERSION": "2",
            "WINDOW_ID": "2026-09-03",
            "HIST_PAUSED": "f",
            "REFRESH_PAUSED": "f",
            "MAINT_PAUSED": "t",
            **{f"POOL_{p}": POOL_SLOTS[p] for p in POOLS},
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
    # #1362: «бой = master» внутри окна больше не молчит — запись окна открыта с TARGET=-.
    rec = _window(stand, stand.today)
    assert rec["TARGET"] == "-" and "OUTCOME" not in rec, rec


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
def test_a_busy_deploy_lock_skips_the_tick(stand: Stand) -> None:
    """deploy.sh — инструмент владельца по слову «выкатывай»: запрещать его автомат не
    вправе, но и лезть под него не должен. Раньше это решал `pgrep deploy.sh` — между
    «процесса нет» и первым изменением контура помещался целый чужой выкат. Теперь обе
    стороны берут один замок."""
    lock = stand.runtime / "deploy.lock"
    lock.touch()
    holder = subprocess.Popen(["flock", "-n", str(lock), "sleep", "60"])
    try:
        proc = stand.run()
    finally:
        holder.kill()
        holder.wait()

    assert proc.returncode == 0, proc.stderr
    assert "идёт другой выкат" in stand.log_text()
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_the_automaton_holds_the_deploy_lock_for_the_whole_tick(stand: Stand) -> None:
    """Замок держится весь тик — снимок, выкат, приёмка, восстановление. Дескриптор и
    окно уезжают в deploy.sh: свидетельство учёта привязано к этой ночи, а сам выкат идёт
    под уже взятым замком, а не спотыкается о него."""
    stand.watchdog_pids()
    proc = stand.run()

    assert proc.returncode == 0, proc.stderr
    calls = stand.calls("deploy")
    assert calls, stand.log_text()
    window, fd = calls[0].split("|")[2], calls[0].split("|")[3]
    assert window == stand.today, calls[0]
    assert fd == "8", calls[0]
    assert (stand.stub_state / "lock.probe").read_text().strip() == "held"


@pytest.mark.unit
def test_the_env_file_is_reread_under_the_lock(stand: Stand) -> None:
    """Ручной выкат мог закончиться между первым чтением env-файла и захватом замка: без
    перечитывания снимок отката указывал бы на дерево, которого в бою уже нет."""
    other = stand.releases / "release-99999999"
    (other / "logs").mkdir(parents=True)
    stand.put("env_swap_root", str(other))
    proc = stand.run()

    # Перечитанный env указывает на дерево, которое законным клоном не является: автомат
    # об этом и говорит. Без перечитывания он спокойно доставил бы на прежнее дерево.
    assert proc.returncode == 1, proc.stderr + stand.log_text()
    assert "НЕ В ЗАКОННОМ СОСТОЯНИИ" in stand.log_text()
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
def test_sunday_is_an_ordinary_night_now(stand: Stand) -> None:
    """Решение владельца 07.09: воскресного дедлайна 04:45 больше нет. Он оставлял на
    осушение 1020 с и съел ночь 06.09 при честно работавшем 62-минутном скоупе. Прогон
    обслуживания манифеста в 05:00 не мешает: deploy.sh паузит его на весь выкат, автомат
    возвращает паузу после приёмки или отката."""
    stand.put("now_epoch", str(_epoch("2026-09-06 04:50:00")))   # воскресенье
    stand.watchdog_pids()
    proc = stand.run()

    assert proc.returncode == 0, proc.stderr
    assert stand.calls("deploy"), "в воскресенье 04:50 окно открыто, как в четверг"

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
    # #1362: попыток в этом окне больше не будет — запись закрыта failed.
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and "запаса нет" in rec["REASON"], rec


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
@pytest.mark.parametrize(
    "content,expect",
    [
        ("DRAIN_WINDOW_ID={today}\nDRAIN_ACCOUNTED=t\n", "Учёт оплаченного скоупа истории: подтверждён"),
        ("DRAIN_WINDOW_ID={today}\nDRAIN_ACCOUNTED=f\n", "НЕ подтверждён"),
        ("DRAIN_WINDOW_ID={today}\nDRAIN_ACCOUNTED=n/a\n", "платной работы в это окно не было"),
        # След прошлой ночи или ручного выката: выдать его за сегодняшний учёт нельзя.
        ("DRAIN_WINDOW_ID=2026-08-01\nDRAIN_ACCOUNTED=t\n", "неизвестен (файл от окна 2026-08-01)"),
        (None, "неизвестен (файла"),
    ],
)
def test_the_report_of_the_night_carries_the_drain_accounting(
    stand: Stand, content: str | None, expect: str
) -> None:
    """Ревью Sol, круг 1: доказательство учёта из шага drain было мёртвым контрактом —
    deploy.sh его писал, а не читал никто. Теперь оно звучит в отчёте ночи; на исход
    доставки (его решает приёмка) не влияет."""
    stand.watchdog_pids()
    if content is not None:
        (stand.state / "last-drain.env").write_text(content.format(today=stand.today), encoding="utf-8")
    proc = stand.run()

    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    assert "✅" in stand.pending()
    assert expect in stand.pending(), stand.pending()
    # #1362: ACCOUNTED переезжает в запись окна только из своего окна; f — всё равно delivered.
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "delivered", rec
    own = content is not None and content.startswith("DRAIN_WINDOW_ID={today}")
    want = content.split("DRAIN_ACCOUNTED=")[1].strip() if own else "unknown"
    assert rec["ACCOUNTED"] == want, rec


@pytest.mark.unit
def test_the_happy_path_delivers_accepts_and_restores_the_snapshot(stand: Stand) -> None:
    """Сквозной успех: заморозка → deploy.sh нового дерева → приёмка по шести признакам →
    маркер принятого sha, снятый INFLIGHT, паузы и пулы как до доставки, ✅ в очереди."""
    stand.watchdog_pids()
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()

    deploy = stand.calls("deploy")
    assert len(deploy) == 1, deploy
    args, idle = deploy[0].split("|")[:2]
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
    """Код 4 deploy.sh — «контур занят, выкат не начат»: откатывать нечего. #1362: это
    провальная ночь — запись окна failed, защёлка остаётся (второй попытки за ночь нет:
    IDLE_WAIT — весь запас до дедлайна)."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "4")
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (stand.state / "sofascore-auto-deliver-attempted-2026-09-03").exists()
    assert not (stand.state / "sofascore-inflight").exists()
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and "rc=4" in rec["REASON"] and rec["RESTORED"] == "t", rec
    assert "Ночь 1 из 3 подряд" in stand.pending(), stand.pending()
    assert not stand.calls("compose"), "отката не было"
    assert "контур не освободился" in stand.log_text()
    assert (stand.stub_state / f"paused_{HIST}").read_text().strip() == "f"
    for pool in POOLS:
        assert (stand.stub_state / f"pool_{pool}").read_text().strip() == POOL_SLOTS[pool]


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
    assert [p.name for p in stand.state.glob("sofascore-window-*")] == ["sofascore-window-2026-09-03.env"]
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and "124" in rec["REASON"], rec
    assert stand.log_text().count("ИТОГ ОКНА") == 1


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
def test_the_snapshot_remembers_the_maintenance_pause_and_the_window(stand: Stand) -> None:
    """Снимок версии 2: паузу обслуживания манифеста снимает deploy.sh, а возвращает
    автомат — значит, помнить её обязан снимок. WINDOW_ID привязывает разбор незакрытой
    доставки к ТОЙ ночи, в которую она началась, даже если разбор пришёлся на следующие сутки."""
    stand.watchdog_pids()
    proc = stand.run()

    assert proc.returncode == 0, proc.stderr + stand.log_text()
    snapshot = (stand.state / "sofascore-rollback.env").read_text(encoding="utf-8")
    assert "SNAPSHOT_VERSION=2\n" in snapshot, snapshot
    assert "MAINT_PAUSED=t\n" in snapshot, snapshot
    assert f"WINDOW_ID={stand.today}\n" in snapshot, snapshot
    # Пауза обслуживания вернулась туда же, где была до доставки.
    assert (stand.stub_state / f"paused_{MAINT}").read_text().strip() == "t"


@pytest.mark.unit
def test_an_unreadable_maintenance_pause_makes_the_snapshot_unusable(stand: Stand) -> None:
    """Ответ X (метабаза молчала) о паузе обслуживания — такой же негодный снимок, как
    непрочитанные паузы кампаний: вернуть её после отката было бы не из чего."""
    stand.watchdog_pids()
    stand.put(f"paused_{MAINT}", "X")
    proc = stand.run()

    assert proc.returncode == 1, proc.stderr
    assert not stand.calls("deploy"), "доставки быть не должно"
    assert (stand.state / "sofascore-auto-deliver.off").exists()


@pytest.mark.unit
def test_an_unfinished_teardown_leaves_the_maintenance_paused(stand: Stand) -> None:
    """Инвариант: на разрушительном пути обслуживание манифеста под паузой. Разбор
    незакрытой доставки не состоялся (docker молчит про монты) — EXIT-trap возвращает пулы
    и паузы кампаний, но паузу обслуживания НЕ трогает: следующий тик будет откатывать бой
    пересозданием контейнеров, и живой прогон обслуживания он бы оборвал."""
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(MAINT_PAUSED="f")
    stand.put(f"paused_{MAINT}", "t")     # deploy.sh запаузил обслуживание до обрыва
    stand.put("inspect_fails")            # docker не отвечает про монты
    proc = stand.run()

    assert proc.returncode == 1, proc.stderr
    assert "docker не отвечает" in stand.log_text()
    assert (stand.stub_state / f"paused_{MAINT}").read_text().strip() == "t", stand.log_text()
    assert (stand.state / "sofascore-inflight").exists(), "разбор не завершён — маркер остаётся"


@pytest.mark.unit
def test_a_confirmed_rollback_puts_the_maintenance_pause_back(stand: Stand) -> None:
    """Откат завершён — обслуживание возвращается в то состояние, в каком было до доставки.
    Сам откат пересоздаёт контейнеры, поэтому паузу он ставит себе сам, независимо от того,
    кто её снял."""
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(MAINT_PAUSED="f")
    stand.put(f"paused_{MAINT}", "t")
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()

    assert proc.returncode == 1, proc.stderr
    assert "откатываю комплектом" in stand.log_text()
    assert (stand.stub_state / f"paused_{MAINT}").read_text().strip() == "f"


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
def test_a_teardown_that_cannot_unpause_the_maintenance_is_not_a_success(stand: Stand) -> None:
    """Ревью Sol, круг 1. Разбор незакрытой доставки возвращал обслуживание, но код возврата
    не смотрел: обслуживание оставалось под паузой навсегда, а автомат снимал маркер,
    отчитывался ⚠️ «ничего страшного» и не глушил себя — воскресный прогон обслуживания
    просто больше не запускался бы."""
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(MAINT_PAUSED="f")
    stand.put(f"paused_{MAINT}", "t")
    stand.put("unpause_maint_fails")
    proc = stand.run()

    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "🆘" in stand.pending() and "⚠️" not in stand.pending()
    assert "контур не вернулся в рабочее состояние" in stand.pending()
    assert (stand.state / "sofascore-auto-deliver.off").exists()


@pytest.mark.unit
def test_an_unconfirmed_rollback_still_says_the_maintenance_was_not_paused(stand: Stand) -> None:
    """Ревью Sol, круг 3. В ветке «откат не подтверждён» аварийный алерт терял ROLLBACK_NOTE
    вместе с предупреждением о незапаузившемся обслуживании — самое время о нём молчать
    было бы худшим: контейнеры пересоздавали при живом прогоне обслуживания."""
    stand.watchdog_pids()
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(MAINT_PAUSED="f")
    stand.put(f"paused_{MAINT}", "f")
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.put("pause_maint_fails")
    proc = stand.run()                     # rollback_works не выставлен: откат не подтверждён

    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "🆘" in stand.pending()
    assert "не встал на паузу перед откатом" in stand.pending(), stand.pending()


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
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and rec["RESTORED"] == "f", rec


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
     "sofascore-pending-alert", "sofascore-auto-deliver.off", "sofascore-window-2026-09-03.env",
     # Ревью Sol, круг 3: доказательство учёта тоже своё состояние. Каталог на его месте
     # уронил бы `rm -f` в deploy.sh кодом 1 — и автомат откатил бы НЕТРОНУТЫЙ бой.
     "last-drain.env"],
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
@pytest.mark.parametrize(
    ("history", "off"),
    [
        # Две провальные ночи до сегодняшней — третья глушит автомат.
        ({"2026-09-01": "failed", "2026-09-02": "failed"}, True),
        # unknown между failed серию не рвёт (не считает и не сбрасывает).
        ({"2026-08-31": "failed", "2026-09-01": "unknown", "2026-09-02": "failed"}, True),
        # delivered обрывает счёт.
        ({"2026-08-31": "failed", "2026-09-01": "delivered", "2026-09-02": "failed"}, False),
        # no-target и needs-hands пропускаются.
        ({"2026-08-30": "failed", "2026-08-31": "no-target", "2026-09-01": "needs-hands",
          "2026-09-02": "failed"}, True),
    ],
)
def test_three_failed_nights_in_a_row_shut_the_automaton_off(
    stand: Stand, history: dict[str, str], off: bool
) -> None:
    """Серия провалов выводится из записей окон (#1362); файла-счётчика больше нет."""
    stand.watchdog_pids()
    for day, outcome in history.items():
        _put_window(stand, day, OUTCOME=outcome, REASON="прошлая ночь", RESTORED="t")
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert not (stand.state / "sofascore-fail-nights").exists()
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and rec["RESTORED"] == "t", rec
    assert (stand.state / "sofascore-auto-deliver.off").exists() is off
    assert ("Больше не пробую" in stand.pending()) is off
    assert "ИТОГ ОКНА 2026-09-03" in stand.log_text()


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
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and rec["RESTORED"] == "f", rec


@pytest.mark.unit
def test_a_rollback_that_could_not_pause_the_maintenance_says_so(stand: Stand) -> None:
    """Ревью Sol, круг 2. Пауза обслуживания перед откатом ставилась вслепую: команда могла
    отказать, прогон обслуживания стартовал бы между проверкой «контур свободен» и
    пересозданием контейнеров и был бы оборван молча, а откат доложили бы как штатный.
    Откат из-за этого не отменяем — бой на непринятом дереве хуже."""
    stand.watchdog_pids()
    stand.put("deploy_rc", "5")
    stand.put("deploy_half")
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    stand.put("pause_maint_fails")
    stand.put(f"paused_{MAINT}", "f")   # до отката обслуживание не под паузой
    proc = stand.run()

    assert stand.calls("compose"), "откат всё равно состоялся"
    assert "не встал на паузу перед откатом" in stand.pending(), stand.pending()
    assert proc.returncode != 0


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
    for day in ("2026-09-01", "2026-09-02"):
        _put_window(stand, day, OUTCOME="failed", REASON="прошлая ночь", RESTORED="t")
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (stand.state / "sofascore-accepted").exists(), "маркер приёмки не выдаётся авансом"
    assert "КОНТРАКТ ПРИЁМКИ НЕ СОШЁЛСЯ" in stand.log_text()
    assert "⚠️" in stand.pending() and "контракт приёмки" in stand.pending()
    # Серию провальных ночей не сбрасываем: бой на master не проверен — цель «?», не «-».
    assert _window(stand, "2026-09-02")["OUTCOME"] == "failed"
    rec = _window(stand, stand.today)
    assert rec["TARGET"] == "?" and "контракт приёмки" in rec["LAST_REASON"], rec
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


# ---- #1362: автомат = артефакт релиза -------------------------------------------------------

def _advance_master(stand: Stand, automat_tail: str | None = None) -> None:
    """Новый коммит в master стенда (к автомату в нём, если попросили, добавлен хвост);
    цель доставки — он. Вызывать ДО watchdog_pids: сторож нового дерева смотрит на new_tree."""
    if automat_tail is not None:
        path = stand.source / "deploy" / "sofascore" / AUTO.name
        path.write_text(path.read_text(encoding="utf-8") + automat_tail, encoding="utf-8")
    (stand.source / "marker.txt").write_text("three\n", encoding="utf-8")
    _git(stand.source, "commit", "-q", "-am", "three")
    stand.new_sha = _git(stand.source, "rev-parse", "HEAD")
    stand.new_tree = stand.releases / f"release-{stand.new_sha[:8]}"


def _md5(path: Path) -> str:
    import hashlib
    return hashlib.md5(path.read_bytes()).hexdigest()


@pytest.mark.unit
def test_a_matching_automat_is_verified_before_the_delivery(stand: Stand) -> None:
    """Копия = релиз → строка «сверен» с md5 обеих половин, доставка идёт, AUTOMAT=ok."""
    stand.watchdog_pids()
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    copy = stand.tmp / "libexec" / AUTO.name
    assert f"автомат сверен с релизом {stand.new_sha[:8]}: auto_deliver.sh {_md5(copy)}" in stand.log_text()
    assert len(stand.calls("deploy")) == 1
    rec = _window(stand, stand.today)
    assert rec["AUTOMAT"] == "ok" and rec["OUTCOME"] == "delivered", rec


@pytest.mark.unit
def test_a_stale_automat_installs_itself_from_the_release_and_delivers_next_tick(stand: Stand) -> None:
    """Копия ≠ релиз впервые → установлена из релиза атомарно, md5 совпал, тик вышел без
    выката и без защёлки; следующий тик (уже новой копией) доставил."""
    _advance_master(stand, "# новая версия автомата\n")
    stand.watchdog_pids()
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    copy = stand.tmp / "libexec" / AUTO.name
    assert _md5(copy) == _md5(stand.new_tree / "deploy" / "sofascore" / AUTO.name)
    assert copy.stat().st_mode & 0o777 == 0o755
    assert "автомат обновлён из релиза" in stand.log_text()
    assert (stand.state / f"sofascore-automat-installed-{stand.new_sha[:8]}").exists()
    assert not stand.calls("deploy")
    assert not list(stand.state.glob("sofascore-auto-deliver-attempted-*"))
    assert not list((stand.tmp / "libexec").glob(".*install-tmp"))
    rec = _window(stand, stand.today)
    assert rec["AUTOMAT"] == "installed" and "OUTCOME" not in rec, rec

    stand.put("now_epoch", str(_epoch("2026-09-03 03:35:00")))
    proc = stand.run(keep_copy=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    assert "автомат сверен с релизом" in stand.log_text()
    assert len(stand.calls("deploy")) == 1
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "delivered" and rec["AUTOMAT"] == "ok", rec


def _assert_mismatch(stand: Stand, proc: subprocess.CompletedProcess, copy_md5: str) -> None:
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    assert (stand.state / "sofascore-automat-mismatch-2026-09-03").exists()
    assert (stand.state / "sofascore-auto-deliver-attempted-2026-09-03").exists()
    assert not stand.calls("deploy") and not stand.calls("compose")
    assert _md5(stand.tmp / "libexec" / AUTO.name) == copy_md5, "копия не подменена наполовину"
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and rec["AUTOMAT"] == "mismatch", rec
    assert "автомат ≠ релиз" in rec["REASON"], rec
    assert "⛔" in stand.pending() and "не совпадает с релизом" in stand.pending()


@pytest.mark.unit
def test_a_stale_automat_after_an_install_is_a_red_night(stand: Stand) -> None:
    """Копия ≠ релиз, а отметка об установке из этого релиза уже стоит → красный маркер,
    запись failed, доставки нет, тревога."""
    _advance_master(stand, "# новая версия автомата\n")
    stand.watchdog_pids()
    (stand.state / f"sofascore-automat-installed-{stand.new_sha[:8]}").touch()
    copy_md5 = _md5(stand.install())
    _assert_mismatch(stand, stand.run(keep_copy=True), copy_md5)


@pytest.mark.unit
def test_an_automat_that_cannot_install_itself_is_a_red_night(stand: Stand) -> None:
    """Каталог копии недоступен на запись (стенд идёт от root, поэтому невозможность записи
    изображается каталогом на месте временного файла установки) → то же, что выше."""
    _advance_master(stand, "# новая версия автомата\n")
    stand.watchdog_pids()
    copy_md5 = _md5(stand.install())
    blocker = stand.tmp / "libexec" / f".{AUTO.name}.install-tmp"
    blocker.mkdir()
    (blocker / "keep").touch()
    _assert_mismatch(stand, stand.run(keep_copy=True), copy_md5)
    assert "установка копии из релиза не удалась" in _window(stand, stand.today)["REASON"]


# ---- #1265 / #1362: запись окна ------------------------------------------------------------

@pytest.mark.unit
def test_a_quiet_night_is_one_record_closed_as_no_target_at_the_deadline(stand: Stand) -> None:
    _production_on_master(stand)
    for when in ("2026-09-03 03:30:00", "2026-09-03 04:10:00"):
        stand.put("now_epoch", str(_epoch(when)))
        assert stand.run().returncode == 0
    assert [p.name for p in stand.state.glob("sofascore-window-*")] == ["sofascore-window-2026-09-03.env"]
    rec = _window(stand, stand.today)
    assert rec["TARGET"] == "-" and "OUTCOME" not in rec and len(rec["SCRIPT_SHA"]) == 64, rec
    stand.put("now_epoch", str(_epoch("2026-09-03 06:05:00")))
    assert stand.run().returncode == 0
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "no-target", rec
    assert "ИТОГ ОКНА 2026-09-03: цель -, исход no-target" in stand.log_text()


@pytest.mark.unit
def test_a_commit_that_appears_inside_the_window_is_delivered_not_no_target(stand: Stand) -> None:
    stand.watchdog_pids()
    _git(stand.source, "update-ref", "refs/heads/master", stand.old_sha)   # master = бой
    assert stand.run().returncode == 0
    assert _window(stand, stand.today)["TARGET"] == "-"
    _git(stand.source, "update-ref", "refs/heads/master", stand.new_sha)   # коммит появился
    stand.put("now_epoch", str(_epoch("2026-09-03 03:35:00")))
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    rec = _window(stand, stand.today)
    assert rec["TARGET"] == stand.new_sha and rec["OUTCOME"] == "delivered", rec


@pytest.mark.unit
def test_a_failed_freeze_keeps_the_record_open_and_the_next_tick_delivers(stand: Stand) -> None:
    stand.watchdog_pids()
    stand.put("freeze_fails")
    assert stand.run().returncode == 1
    rec = _window(stand, stand.today)
    assert "OUTCOME" not in rec and "заморозка" in rec["LAST_REASON"], rec
    (stand.stub_state / "freeze_fails").unlink()
    stand.put("now_epoch", str(_epoch("2026-09-03 03:35:00")))
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "delivered" and "заморозка" in rec["LAST_REASON"], rec


@pytest.mark.unit
def test_a_contour_busy_until_the_deadline_is_a_failed_night(stand: Stand) -> None:
    stand.put("busy", "1")
    assert stand.run().returncode == 0
    assert "контур занят" in _window(stand, stand.today)["LAST_REASON"]
    stand.put("now_epoch", str(_epoch("2026-09-03 06:05:00")))
    assert stand.run().returncode == 0
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "failed" and rec["REASON"].startswith("окно истекло: контур занят"), rec
    assert not stand.calls("deploy")


@pytest.mark.unit
def test_an_unreachable_master_all_window_long_is_unknown(stand: Stand) -> None:
    stand.env_file.write_text(
        stand.env_file.read_text(encoding="utf-8").replace(
            f"SOFASCORE_SOURCE_REPO={stand.source}", f"SOFASCORE_SOURCE_REPO={stand.tmp}/no-such-repo"
        ),
        encoding="utf-8",
    )
    assert stand.run().returncode == 0
    assert _window(stand, stand.today)["TARGET"] == "?"
    stand.put("now_epoch", str(_epoch("2026-09-03 06:05:00")))
    assert stand.run().returncode == 0
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "unknown" and "master недоступен" in rec["REASON"], rec


@pytest.mark.unit
def test_a_missing_record_for_yesterday_is_closed_as_unknown_on_the_first_tick(stand: Stand) -> None:
    _put_window(stand, "2026-09-01", OUTCOME="delivered", REASON="приёмка подтверждена", RESTORED="t")
    stand.put("now_epoch", str(_epoch("2026-09-03 01:00:00")))
    assert stand.run().returncode == 0
    rec = _window(stand, "2026-09-02")
    assert rec["OUTCOME"] == "unknown" and "не работал" in rec["REASON"], rec
    assert _window(stand, stand.today) == {}, "сегодняшнее окно ещё не началось"


@pytest.mark.unit
def test_the_very_first_recording_tick_does_not_invent_past_windows(stand: Stand) -> None:
    """Копия, впервые пишущая записи, окон до себя не видела — «unknown» за вчера был бы
    ложной тревогой в первое же утро после установки."""
    stand.put("now_epoch", str(_epoch("2026-09-03 01:00:00")))
    assert stand.run().returncode == 0
    assert not list(stand.state.glob("sofascore-window-*"))


@pytest.mark.unit
def test_the_switch_still_closes_overdue_windows_but_touches_nothing(stand: Stand) -> None:
    """.off + INFLIGHT + смешанные монты: записи исходов — да, изменения контура — нет."""
    stand.watchdog_pids()
    (stand.state / "sofascore-auto-deliver.off").touch()
    _put_window(stand, "2026-09-02", LAST_REASON="контур занят")
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(WINDOW_ID="2026-09-03")
    stand.put("mounts_root", str(stand.new_tree))
    env_before = stand.env_file.read_text(encoding="utf-8")
    stand.put("now_epoch", str(_epoch("2026-09-03 07:00:00")))
    assert stand.run().returncode == 0
    assert _window(stand, "2026-09-02")["OUTCOME"] == "failed"
    assert _window(stand, stand.today) == {}, "окно разбираемой доставки оставлено разбору"
    assert not stand.calls("deploy") and not stand.calls("compose")
    assert stand.env_file.read_text(encoding="utf-8") == env_before
    assert not [c for c in stand.calls("docker") if " dags " in c or " pools " in c]


@pytest.mark.unit
def test_an_interrupted_delivery_is_accounted_to_its_own_window_next_day(stand: Stand) -> None:
    stand.watchdog_pids()
    _put_window(stand, "2026-09-03", ATTEMPT_AT="2026-09-03T03:31:00Z")
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot(WINDOW_ID="2026-09-03")
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    stand.put("now_epoch", str(_epoch("2026-09-04 02:00:00")))
    proc = stand.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    rec = _window(stand, "2026-09-03")
    assert rec["OUTCOME"] == "failed" and "откат после обрыва" in rec["REASON"], rec
    assert _window(stand, "2026-09-04") == {}


@pytest.mark.unit
def test_a_break_between_acceptance_and_clearing_the_marker_is_rolled_back_as_failed(stand: Stand) -> None:
    stand.watchdog_pids()
    _put_window(stand, "2026-09-03", DELIVERY_PHASE="finishing")
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    stand.put("mounts_root", str(stand.new_tree))
    stand.put("mounts_sched_root", str(stand.new_tree))
    stand.put("rollback_root", str(stand.old_tree))
    stand.put("rollback_works")
    stand.put("now_epoch", str(_epoch("2026-09-03 04:00:00")))
    assert stand.run().returncode == 1
    assert _window(stand, stand.today)["OUTCOME"] == "failed"
    assert len(stand.calls("compose")) == 2


@pytest.mark.unit
def test_a_break_between_clearing_the_marker_and_the_record_is_unknown(stand: Stand) -> None:
    _put_window(stand, "2026-09-03", DELIVERY_PHASE="finishing")
    stand.put("now_epoch", str(_epoch("2026-09-03 06:05:00")))
    stand.run()
    rec = _window(stand, stand.today)
    assert rec["OUTCOME"] == "unknown" and "обрыв между снятием" in rec["REASON"], rec


@pytest.mark.unit
def test_an_inflight_marker_next_to_a_delivered_record_breaks_the_invariant(stand: Stand) -> None:
    _put_window(stand, "2026-09-03", OUTCOME="delivered", REASON="приёмка подтверждена", RESTORED="t")
    (stand.state / "sofascore-inflight").touch()
    stand.write_snapshot()
    assert stand.run().returncode == 1
    assert _window(stand, stand.today)["OUTCOME"] == "unknown"
    assert (stand.state / "sofascore-auto-deliver.off").exists()
    assert "🆘" in stand.pending()
    assert not stand.calls("compose")


@pytest.mark.unit
def test_a_success_is_written_last_with_the_night_summary(stand: Stand) -> None:
    stand.watchdog_pids()
    proc = stand.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr + stand.log_text()
    rec = _window(stand, stand.today)
    for key in ("WINDOW_ID", "LIVE", "TARGET", "SCRIPT_SHA", "OPENED_AT", "OUTCOME", "REASON",
                "ACCOUNTED", "RESTORED", "CLOSED_AT", "AUTOMAT"):
        assert key in rec, (key, rec)
    assert rec["OUTCOME"] == "delivered" and rec["DELIVERY_PHASE"] == "finishing", rec
    assert rec["TARGET"] == stand.new_sha and rec["LIVE"] == stand.old_sha, rec
    log = stand.log_text()
    assert log.index("ДОСТАВЛЕНО") < log.index("ИТОГ ОКНА 2026-09-03")
    assert f"ИТОГ ОКНА 2026-09-03: цель {stand.new_sha[:8]}, исход delivered" in log
    assert not list(stand.state.glob("*.tmp"))
