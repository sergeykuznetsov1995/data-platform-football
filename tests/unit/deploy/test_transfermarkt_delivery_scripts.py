"""Сценарии автомата доставки Transfermarkt (#1387): ветка незакрытой доставки.

Стенд: копия auto_deliver.sh + env.sh во временном каталоге установки, заглушка docker
впереди фиксированного PATH автомата; метабаза, монты и здоровье контейнеров — файлы
стенда. Старое дерево несёт заглушку deploy.sh (откат = deploy.sh старого дерева).
Ни один сценарий не ходит в настоящий docker и в сеть.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "transfermarkt"
PATH_LINE = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
DAGS = (
    "dag_ingest_transfermarkt",
    "dag_discover_transfermarkt_registry",
    "dag_backfill_transfermarkt",
    "dag_transform_transfermarkt_silver",
)
POOLS = ("ingest_scraper_pool", "transfermarkt_proxy", "transfermarkt_backfill_proxy", "transfermarkt_backfill_control")
SCHED = "transfermarkt-airflow-scheduler"
GW = "transfermarkt_gw"

DOCKER_STUB = r"""#!/bin/bash
S=__STATE__
cmd=$1; shift
case "$cmd" in
  inspect)
    fmt=""; [ "$1" = -f ] && { fmt=$2; shift 2; }
    for c in "$@"; do
      case "$fmt" in
        *'.Mounts'*) cat "$S/mounts_$c" ;;
        '{{.State.Health.Status}}') cat "$S/health_$c" ;;
        '{{.State.StartedAt}}') cat "$S/started_$c" ;;
        '{{.Created}}') cat "$S/created_$c" ;;
        *HostConfig.Memory*) echo "$(cat "$S/health_$c") 1073741824 transfermarkt-gw" ;;
        *) exit 1 ;;
      esac
    done ;;
  logs) exit 0 ;;
  exec)
    c=$1; shift
    if [ "$c" = transfermarkt-airflow-metadb ]; then
      sql=${@: -1}
      case "$sql" in
        *is_paused*) dag=${sql#*dag_id=\'}; dag=${dag%%\'*}; cat "$S/paused_$dag" ;;
        *last_parsed_time*) cat "$S/dags" ;;
        *import_error*) echo 0 ;;
        *slot_pool*) pool=${sql#*pool=\'}; pool=${pool%%\'*}; cat "$S/pool_$pool" ;;
        *dag_run*) echo 0 ;;
        *) echo "unexpected sql: $sql" >&2; exit 1 ;;
      esac
      exit 0
    fi
    case "$1 $2 $3" in
      "airflow dags pause") [ -e "$S/pause_fails_$4" ] || echo t > "$S/paused_$4" ;;
      "airflow dags unpause") [ -e "$S/pause_fails_$4" ] || echo f > "$S/paused_$4" ;;
      "airflow pools set") echo "$5" > "$S/pool_$4" ;;
      python*) echo ok ;;
      *) echo "unexpected exec: $*" >&2; exit 1 ;;
    esac ;;
  *) echo "unexpected docker $cmd" >&2; exit 1 ;;
esac
"""

OLD_DEPLOY_STUB = r"""#!/bin/bash
# Заглушка deploy.sh старого дерева: перепин env, пересоздание (монты и старт), пулы 1.
S=__STATE__
new=$1
sed -i "s#^TRANSFERMARKT_RELEASE_ROOT=.*#TRANSFERMARKT_RELEASE_ROOT=$new#" "$TRANSFERMARKT_ENV_FILE"
printf '%s/dags\n%s/deploy/transfermarkt/.airflowignore\n%s/scrapers\n%s/scripts\n%s/configs\n' \
  "$new" "$new" "$new" "$new" "$new" > "$S/mounts___SCHED__"
echo "$new" > "$S/mounts___GW__"
echo healthy > "$S/health___GW__"
echo 2026-09-24T02:00:00Z > "$S/started___SCHED__"
for p in __POOLS__; do echo 1 > "$S/pool_$p"; done
echo called > "$S/old_deploy_called"
exit 0
"""


class Stand:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.state = tmp / "stand"
        self.state.mkdir()
        self.runtime = tmp / "runtime"
        self.runtime.mkdir(mode=0o755)
        self.auto = self.runtime / "auto-deliver"
        self.auto.mkdir(mode=0o700)
        self.releases = tmp / "releases"
        self.old = self.releases / "release-0ld00000"
        self.new = self.releases / "release-ne000000"
        for tree in (self.old, self.new):
            (tree / "deploy" / "transfermarkt").mkdir(parents=True)
        stub = OLD_DEPLOY_STUB.replace("__STATE__", str(self.state)).replace(
            "__SCHED__", SCHED).replace("__GW__", GW).replace("__POOLS__", " ".join(POOLS))
        old_deploy = self.old / "deploy" / "transfermarkt" / "deploy.sh"
        old_deploy.write_text(stub, encoding="utf-8")
        old_deploy.chmod(0o755)
        stubs = tmp / "stubs"
        stubs.mkdir()
        docker = stubs / "docker"
        docker.write_text(DOCKER_STUB.replace("__STATE__", str(self.state)), encoding="utf-8")
        docker.chmod(0o755)
        install = tmp / "libexec"
        install.mkdir()
        text = (DEPLOY / "auto_deliver.sh").read_text(encoding="utf-8")
        assert text.count(PATH_LINE) == 1
        self.automat = install / "auto_deliver.sh"
        self.automat.write_text(
            text.replace(PATH_LINE, PATH_LINE.replace("export PATH=", f"export PATH={stubs}:")),
            encoding="utf-8",
        )
        self.automat.chmod(0o755)
        (install / "env.sh").write_text((DEPLOY / "env.sh").read_text(encoding="utf-8"), encoding="utf-8")
        platform = tmp / "platform.env"
        platform.write_text("X=1\n", encoding="utf-8")
        self.env_file = tmp / "transfermarkt.env"
        self.env_file.write_text(
            f"TRANSFERMARKT_RUNTIME_DIR={self.runtime}\n"
            f"TRANSFERMARKT_AUTO_STATE_DIR={self.auto}\n"
            f"TRANSFERMARKT_AUTO_LOG={self.auto}/auto_deliver.log\n"
            f"TRANSFERMARKT_TG_ENV={tmp}/no-telegram.env\n"
            f"TRANSFERMARKT_RELEASE_ROOT={self.new}\n"
            f"TRANSFERMARKT_SOURCE_REPO={tmp}/no-repo\n"
            f"TRANSFERMARKT_RELEASES_DIR={self.releases}\n"
            f"TRANSFERMARKT_PLATFORM_ENV_FILE={platform}\n",
            encoding="utf-8",
        )
        # Бой до доставки: паузы и пулы как их оставляет deploy.sh.
        snapshot = [f"OLD_RELEASE_ROOT={self.old}", f"NEW_RELEASE_ROOT={self.new}",
                    "SNAPSHOT_VERSION=1", "WINDOW_ID=2026-09-24"]
        snapshot += [f"PAUSED_{d}={'f' if 'ingest' in d or 'discover' in d else 't'}" for d in DAGS]
        snapshot += [f"POOL_{p}=1" for p in POOLS]
        (self.auto / "transfermarkt-rollback.env").write_text("\n".join(snapshot) + "\n", encoding="utf-8")
        (self.auto / "transfermarkt-inflight").write_text("", encoding="utf-8")
        for d in DAGS:
            self.put(f"paused_{d}", "t")  # deploy.sh оборвался после паузы всех четырёх
        for p in POOLS:
            self.put(f"pool_{p}", "1")
        self.put("dags", "4")
        self.put(f"started_{SCHED}", "2026-09-24T01:00:00Z")
        self.put(f"created_{SCHED}", "2026-09-24T01:00:00Z")
        self.mounts_on(self.new)
        self.put(f"health_{GW}", "healthy")
        self.put(f"health_{SCHED}", "healthy")

    def put(self, name: str, value: str) -> None:
        (self.state / name).write_text(value + "\n", encoding="utf-8")

    def mounts_on(self, tree: Path) -> None:
        self.put(f"mounts_{SCHED}", "\n".join(
            f"{tree}/{p}" for p in ("dags", "deploy/transfermarkt/.airflowignore", "scrapers", "scripts", "configs")))
        self.put(f"mounts_{GW}", str(tree))

    def run(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(self.automat)],
            env={"PATH": os.environ["PATH"], "TRANSFERMARKT_ENV_FILE": str(self.env_file),
                 "ACCEPT_WAIT": "2", "ACCEPT_POLL": "1"},
            capture_output=True, text=True, timeout=120,
        )

    def window(self) -> str:
        path = self.auto / "transfermarkt-window-2026-09-24.env"
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def env_root(self) -> str:
        for line in self.env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("TRANSFERMARKT_RELEASE_ROOT="):
                return line.split("=", 1)[1]
        return ""


@pytest.fixture
def stand(tmp_path: Path) -> Stand:
    return Stand(tmp_path)


@pytest.mark.unit
def test_interrupted_delivery_rolls_back_and_restores_the_snapshot(stand: Stand) -> None:
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert (stand.state / "old_deploy_called").exists()
    assert stand.env_root() == str(stand.old)
    assert not (stand.auto / "transfermarkt-inflight").exists()
    assert not (stand.auto / "transfermarkt-auto-deliver.off").exists()
    window = stand.window()
    assert "OUTCOME=failed" in window and "RESTORED=t" in window
    assert (stand.state / "paused_dag_ingest_transfermarkt").read_text().strip() == "f"
    assert (stand.state / "paused_dag_backfill_transfermarkt").read_text().strip() == "t"


@pytest.mark.unit
def test_a_confirmed_rollback_with_a_failed_pause_restore_is_not_a_success(stand: Stand) -> None:
    stand.put("pause_fails_dag_ingest_transfermarkt", "1")
    result = stand.run()
    assert result.returncode == 1, result.stderr
    window = stand.window()
    assert "OUTCOME=needs-hands" in window, window
    assert "RESTORED=t" not in window
    assert (stand.auto / "transfermarkt-auto-deliver.off").exists()


@pytest.mark.unit
def test_mounts_on_old_with_a_stopped_gateway_are_rolled_back_not_declared_fine(stand: Stand) -> None:
    # Обрыв между остановкой шлюза и его пересозданием.
    stand.mounts_on(stand.old)
    stand.put(f"health_{GW}", "unhealthy")
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert (stand.state / "old_deploy_called").exists()
    assert (stand.state / f"health_{GW}").read_text().strip() == "healthy"
    assert "RESTORED=t" in stand.window()


@pytest.mark.unit
def test_mounts_on_old_with_a_live_gateway_only_repins_env_and_restores(stand: Stand) -> None:
    stand.mounts_on(stand.old)
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert not (stand.state / "old_deploy_called").exists()
    assert stand.env_root() == str(stand.old)
    assert not (stand.auto / "transfermarkt-inflight").exists()
    assert "OUTCOME=failed" in stand.window() and "RESTORED=t" in stand.window()
    assert (stand.state / "paused_dag_discover_transfermarkt_registry").read_text().strip() == "f"


@pytest.mark.unit
def test_mounts_on_old_with_a_failed_pause_restore_needs_hands(stand: Stand) -> None:
    stand.mounts_on(stand.old)
    stand.put("pause_fails_dag_discover_transfermarkt_registry", "1")
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert "OUTCOME=needs-hands" in stand.window()
    assert (stand.auto / "transfermarkt-auto-deliver.off").exists()


DEPLOY_DOCKER_STUB = r"""#!/bin/bash
S=__STATE__
cmd=$1; shift
case "$cmd" in
  compose)
    printf '%s\n' "$*" >> "$S/compose_calls"
    case " $* " in
      *" up "*transfermarkt_gw*)
        [ -n "${TRANSFERMARKT_PROXY_POOL_JSON:-}" ] && echo fed > "$S/pool_fed"
        echo healthy > "$S/health_transfermarkt_gw"
        echo "$TRANSFERMARKT_RELEASE_ROOT" > "$S/mounts_transfermarkt_gw" ;;
      *" up "*airflow-scheduler*)
        r=$TRANSFERMARKT_RELEASE_ROOT
        printf '%s/dags\n%s/deploy/transfermarkt/.airflowignore\n%s/scrapers\n%s/scripts\n%s/configs\n' \
          "$r" "$r" "$r" "$r" "$r" > "$S/mounts_transfermarkt-airflow-scheduler"
        echo healthy > "$S/health_transfermarkt-airflow-scheduler"
        echo 4 > "$S/registered" ;;
    esac ;;
  inspect)
    fmt=""; [ "$1" = -f ] && { fmt=$2; shift 2; }
    c=$1
    case "$fmt" in
      *'.Mounts'*) cat "$S/mounts_$c" ;;
      '{{.State.Health.Status}}') cat "$S/health_$c" 2>/dev/null ;;
      '{{.HostConfig.Memory}}') echo 1073741824 ;;
      '{{.State.StartedAt}}') echo 2026-09-24T01:10:00Z ;;
      *) exit 1 ;;
    esac ;;
  exec)
    c=$1; shift
    if [ "$c" = transfermarkt-airflow-metadb ]; then
      sql=${@: -1}
      case "$sql" in
        *is_paused*) dag=${sql#*dag_id=\'}; dag=${dag%%\'*}; cat "$S/paused_$dag" 2>/dev/null || echo - ;;
        *last_parsed_time*) cat "$S/registered" 2>/dev/null || echo 0 ;;
        *import_error*|*dag_run*) echo 0 ;;
        *) echo "unexpected sql: $sql" >&2; exit 1 ;;
      esac
      exit 0
    fi
    [ -e "$S/registered" ] || { echo "no such container: $c" >&2; exit 1; }
    case "$1 $2 $3" in
      "airflow dags pause") echo t > "$S/paused_$4" ;;
      "airflow dags unpause") echo f > "$S/paused_$4" ;;
      "airflow pools set") echo "$5" > "$S/pool_$4" ;;
      "airflow pools list") ;;
      python*) echo "ok source_mode=transfermarkt-only paid_enabled=True" ;;
      *) echo "unexpected exec: $*" >&2; exit 1 ;;
    esac ;;
  *) echo "unexpected docker $cmd" >&2; exit 1 ;;
esac
"""


def _deploy_stand(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    """Каталоги и заглушки для прямого запуска deploy.sh: state, runtime, release, stubs, env."""
    state = tmp_path / "stand"
    state.mkdir()
    runtime = tmp_path / "runtime"
    (runtime / "logs").mkdir(parents=True)
    (runtime / "gateway-state").mkdir()
    runtime.chmod(0o755)
    releases = tmp_path / "releases"
    release = releases / "release-f1r5t000"
    (release / "deploy" / "transfermarkt").mkdir(parents=True)
    for name in ("airflow.compose.yaml", "gateway.compose.yaml", ".airflowignore", "deploy.sh", "env.sh"):
        (release / "deploy" / "transfermarkt" / name).write_text(
            (DEPLOY / name).read_text(encoding="utf-8"), encoding="utf-8")
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    docker = stubs / "docker"
    docker.write_text(DEPLOY_DOCKER_STUB.replace("__STATE__", str(state)), encoding="utf-8")
    docker.chmod(0o755)
    sleep = stubs / "sleep"
    sleep.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    sleep.chmod(0o755)
    pool = runtime / "proxy-pool.json"
    pool.write_text('[{"host":"example.invalid"}]', encoding="utf-8")
    platform = tmp_path / "platform.env"
    platform.write_text("X=1\n", encoding="utf-8")
    env_file = tmp_path / "transfermarkt.env"
    env_file.write_text(
        f"TRANSFERMARKT_RUNTIME_DIR={runtime}\nTRANSFERMARKT_RELEASES_DIR={releases}\n"
        f"TRANSFERMARKT_RELEASE_ROOT={releases}/release-00000000\n"
        f"TRANSFERMARKT_PLATFORM_ENV_FILE={platform}\nTRANSFERMARKT_PROXY_POOL_FILE={pool}\n",
        encoding="utf-8",
    )
    return state, runtime, release, stubs, env_file


def _run_deploy(release: Path, stubs: Path, env_file: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(release / "deploy" / "transfermarkt" / "deploy.sh"), str(release)],
        env={"PATH": f"{stubs}:{os.environ['PATH']}", "TRANSFERMARKT_ENV_FILE": str(env_file)},
        capture_output=True, text=True, timeout=120,
    )


@pytest.mark.unit
def test_first_deploy_on_a_fresh_metabase_registers_and_sets_pauses(tmp_path: Path) -> None:
    """README: metadb + init, затем deploy.sh — в метабазе ещё нет ни одного DAG."""
    state, runtime, release, stubs, env_file = _deploy_stand(tmp_path)
    result = _run_deploy(release, stubs, env_file)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"TRANSFERMARKT_RELEASE_ROOT={release}\n" in env_file.read_text(encoding="utf-8")
    assert (state / "pool_fed").exists()
    deploy_log = (runtime / "deploy.log").read_text(encoding="utf-8")
    assert "example.invalid" not in deploy_log + (state / "compose_calls").read_text(encoding="utf-8")
    want = {"dag_ingest_transfermarkt": "f", "dag_discover_transfermarkt_registry": "f",
            "dag_backfill_transfermarkt": "t", "dag_transform_transfermarkt_silver": "t"}
    for dag, paused in want.items():
        assert (state / f"paused_{dag}").read_text().strip() == paused, dag
    for p in POOLS:
        assert (state / f"pool_{p}").read_text().strip() == "1", p


@pytest.mark.unit
def test_a_manual_pause_survives_deploy(tmp_path: Path) -> None:
    """Выкат возвращает паузы, снятые до него, а не дефолт: ручная пауза ingest остаётся."""
    state, _runtime, release, stubs, env_file = _deploy_stand(tmp_path)
    (state / "registered").write_text("4\n", encoding="utf-8")
    before = {"dag_ingest_transfermarkt": "t", "dag_discover_transfermarkt_registry": "f",
              "dag_backfill_transfermarkt": "t", "dag_transform_transfermarkt_silver": "t"}
    for dag, paused in before.items():
        (state / f"paused_{dag}").write_text(paused + "\n", encoding="utf-8")
    result = _run_deploy(release, stubs, env_file)
    assert result.returncode == 0, result.stdout + result.stderr
    for dag, paused in before.items():
        assert (state / f"paused_{dag}").read_text().strip() == paused, dag


@pytest.mark.unit
def test_a_failed_ledger_archive_stops_deploy_and_keeps_the_ledger(tmp_path: Path) -> None:
    state, runtime, release, stubs, env_file = _deploy_stand(tmp_path)
    ledger = runtime / "gateway-state" / "paid_requests.jsonl"
    body = '{"occurred_at":"2020-01-01T00:00:00Z","bytes":1}\n'
    ledger.write_text(body, encoding="utf-8")
    gzip = stubs / "gzip"
    gzip.write_text("#!/bin/bash\necho 'gzip: no space left' >&2\nexit 1\n", encoding="utf-8")
    gzip.chmod(0o755)
    result = _run_deploy(release, stubs, env_file)
    assert result.returncode == 5, result.stdout + result.stderr
    assert ledger.read_text(encoding="utf-8") == body
    assert list((runtime / "gateway-state").glob("paid_requests.*.gz*")) == []
    assert "ledger archive failed" in (runtime / "deploy.log").read_text(encoding="utf-8")
    assert not (state / "pool_fed").exists()


@pytest.mark.unit
def test_a_hung_scheduler_does_not_confirm_the_rollback(stand: Stand) -> None:
    stand.put(f"health_{SCHED}", "unhealthy")
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert (stand.state / "old_deploy_called").exists()
    window = stand.window()
    assert "OUTCOME=needs-hands" in window and "RESTORED=t" not in window
    assert (stand.auto / "transfermarkt-inflight").exists()
    assert (stand.auto / "transfermarkt-auto-deliver.off").exists()
