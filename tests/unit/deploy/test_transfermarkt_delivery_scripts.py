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
import time

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
POOLS = ("transfermarkt_control", "transfermarkt_proxy", "transfermarkt_backfill_proxy", "transfermarkt_backfill_control")
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
        *last_parsed_time*) cat "$S/dags" ;;
        *import_error*) cat "$S/import_errors" 2>/dev/null || echo 0 ;;
        *next_dagrun_create_after*) cat "$S/next_run" 2>/dev/null || echo 0 ;;
        *task_instance*) cat "$S/busy" 2>/dev/null || echo 0 ;;
        *"state IN ('queued','running')"*) cat "$S/registry_busy" 2>/dev/null || echo 0 ;;
        *is_paused*) dag=${sql#*dag_id=\'}; dag=${dag%%\'*}; cat "$S/paused_$dag" ;;
        *slot_pool*) pool=${sql#*pool=\'}; pool=${pool%%\'*}
          [ ! -e "$S/pool_query_fails_$pool" ] || exit 1
          cat "$S/pool_$pool" ;;
        *dag_run*) echo 0 ;;
        *) echo "unexpected sql: $sql" >&2; exit 1 ;;
      esac
      exit 0
    fi
    case "$1 $2 $3" in
      "airflow dags pause") [ -e "$S/pause_fails_$4" ] || echo t > "$S/paused_$4" ;;
      "airflow dags unpause") [ -e "$S/pause_fails_$4" ] || echo f > "$S/paused_$4" ;;
      "airflow pools set") echo "$5" > "$S/pool_$4" ;;
      "python -m scrapers.transfermarkt.airflow_pools")
        if [ -e "$S/old_without_helper" ] && [[ "$(head -1 "$S/mounts_$c")" == "$(cat "$S/old_without_helper")/"* ]]; then
          echo called > "$S/helper_on_old_called"
          exit 99
        fi
        if [ "$4" = --apply ]; then
          for p in transfermarkt_control transfermarkt_proxy; do echo 1 > "$S/pool_$p"; done
          for p in transfermarkt_backfill_control transfermarkt_backfill_proxy; do echo 0 > "$S/pool_$p"; done
        else
          if [ -e "$S/want_$5" ]; then cat "$S/want_$5"
          else case "$5" in *backfill*) echo 0 ;; *) echo 1 ;; esac; fi
        fi ;;
      python*) cat "$S/gateway_health" 2>/dev/null || echo ok ;;
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

    def probe(self, commands: str) -> subprocess.CompletedProcess[str]:
        # Load the real functions and env, but stop before any delivery/window logic.
        # All docker commands still go to the filesystem-backed stub above.
        prefix = self.automat.read_text().split("# ============================ Шаг 0:", 1)[0]
        probe = self.automat.parent / "probe.sh"
        probe.write_text(prefix + "\n" + commands + "\n", encoding="utf-8")
        return subprocess.run(
            ["bash", str(probe)],
            env={"PATH": os.environ["PATH"], "TRANSFERMARKT_ENV_FILE": str(self.env_file)},
            capture_output=True, text=True, timeout=30,
        )

    def acceptance(self, tree: Path | None = None) -> subprocess.CompletedProcess[str]:
        tree = tree or self.new
        return self.probe(f'acceptance_seen "{tree}" "2026-09-24T01:00:00Z"')

    def log(self) -> str:
        path = self.auto / "auto_deliver.log"
        return path.read_text(encoding="utf-8") if path.exists() else ""

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
@pytest.mark.parametrize("history", [0, 1])
def test_new_release_restores_pauses_and_shared_pool_but_keeps_canonical_tm_slots(
    stand: Stand, history: int,
) -> None:
    # The old snapshot admits one historical task even when the new H0 contour
    # admits none. Restoring that old value before checking the new contract
    # caused deploy.sh DONE followed by final acceptance 0 on every poll.
    snapshot = stand.auto / "transfermarkt-rollback.env"
    snapshot.write_text(snapshot.read_text() + "POOL_ingest_scraper_pool=3\n")
    stand.put("pool_ingest_scraper_pool", "1")
    for pool in POOLS:
        slots = str(history) if "backfill" in pool else "1"
        stand.put(f"pool_{pool}", slots)
        stand.put(f"want_{pool}", slots)
    result = stand.probe(
        f'RESTORE_PENDING=1\nrestore_state new || exit 91\n'
        f'acceptance_seen "{stand.new}" "2026-09-24T01:00:00Z"'
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1", stand.log()
    for pool in POOLS:
        assert (stand.state / f"pool_{pool}").read_text().strip() == (
            str(history) if "backfill" in pool else "1"
        )
    assert (stand.state / "pool_ingest_scraper_pool").read_text().strip() == "3"
    for dag in DAGS:
        assert (stand.state / f"paused_{dag}").read_text().strip() == (
            "f" if "ingest" in dag or "discover" in dag else "t"
        )


@pytest.mark.unit
def test_final_pool_failure_still_rolls_back_exact_old_slots(stand: Stand) -> None:
    stand.put("pool_transfermarkt_backfill_proxy", "0")
    stand.put("pool_transfermarkt_backfill_control", "0")
    stand.put("pool_transfermarkt_control", "2")
    result = stand.acceptance()
    assert result.stdout.strip() == "0"
    assert "predicate=pool_slots" in stand.log()
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert "RESTORED=t" in stand.window()
    assert stand.env_root() == str(stand.old)
    for pool in POOLS:
        assert (stand.state / f"pool_{pool}").read_text().strip() == "1"


@pytest.mark.unit
def test_new_release_pause_restore_failure_is_reported_without_rewriting_h0(stand: Stand) -> None:
    stand.put("pause_fails_dag_ingest_transfermarkt", "1")
    for pool in POOLS:
        stand.put(f"pool_{pool}", "0" if "backfill" in pool else "1")
    result = stand.probe('RESTORE_PENDING=1\nrestore_state new\nrc=$?\necho "$rc:$RESTORE_NOTE"')
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("1:"), result.stdout
    assert "dag_ingest_transfermarkt" in result.stdout
    assert (stand.state / "paused_dag_ingest_transfermarkt").read_text().strip() == "t"
    assert (stand.state / "pool_transfermarkt_backfill_proxy").read_text().strip() == "0"


@pytest.mark.unit
def test_exit_trap_uses_old_snapshot_when_new_deploy_did_not_finish(stand: Stand) -> None:
    for pool in POOLS:
        stand.put(f"pool_{pool}", "0" if "backfill" in pool else "1")
    result = stand.probe('RESTORE_PENDING=1\nexit 7')
    assert result.returncode == 7, result.stderr
    for pool in POOLS:
        assert (stand.state / f"pool_{pool}").read_text().strip() == "1"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("state", "value", "predicate"),
    [
        ("dags", "3", "dag_parse"),
        ("import_errors", "1", "dag_parse"),
        (f"health_{SCHED}", "unhealthy", "scheduler_health"),
        (f"health_{GW}", "unhealthy", "gateway_container"),
        (f"mounts_{GW}", "/outside/releases", "mounts"),
        ("gateway_health", "bad source_mode=shared", "gateway_health"),
        ("pool_transfermarkt_control", "2", "pool_slots"),
    ],
)
def test_acceptance_reports_failed_predicate_without_accepting(
    stand: Stand, state: str, value: str, predicate: str,
) -> None:
    stand.put(state, value)
    result = stand.acceptance()
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0"
    assert f"predicate={predicate}" in stand.log()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("state", "value"),
    [("pool_query_fails_transfermarkt_control", "1"),
     ("want_transfermarkt_control", "log noise\n1")],
)
def test_unknown_pool_read_or_invalid_helper_output_fails_closed(
    stand: Stand, state: str, value: str,
) -> None:
    stand.put(state, value)
    result = stand.acceptance()
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "X"
    assert "predicate=pool_slots" in stand.log()


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
def test_first_upgrade_rolls_back_without_new_helper_or_control_pool(stand: Stand) -> None:
    legacy_pools = ('ingest_scraper_pool', 'transfermarkt_proxy',
                    'transfermarkt_backfill_proxy', 'transfermarkt_backfill_control')
    snapshot = stand.auto / 'transfermarkt-rollback.env'
    text = snapshot.read_text().replace('POOL_transfermarkt_control=1\n', '')
    text += 'OLD_POOLS=' + ' '.join(legacy_pools) + '\nPOOL_ingest_scraper_pool=3\n'
    snapshot.write_text(text)
    old_deploy = stand.old / 'deploy/transfermarkt/deploy.sh'
    old_deploy.write_text(old_deploy.read_text().replace('transfermarkt_control ', 'ingest_scraper_pool '))
    stand.put('old_without_helper', str(stand.old))
    result = stand.run()
    assert result.returncode == 1, result.stderr
    assert stand.env_root() == str(stand.old)
    assert not (stand.state / 'helper_on_old_called').exists()
    assert not (stand.auto / 'transfermarkt-auto-deliver.off').exists()
    assert 'RESTORED=t' in stand.window()
    assert (stand.state / 'pool_ingest_scraper_pool').read_text().strip() == '3'


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
      "python -m scrapers.transfermarkt.airflow_pools")
        if [ "$4" = --apply ]; then
          for p in transfermarkt_control transfermarkt_proxy; do echo 1 > "$S/pool_$p"; done
          for p in transfermarkt_backfill_control transfermarkt_backfill_proxy; do echo 0 > "$S/pool_$p"; done
        else
          case "$5" in *backfill*) echo 0 ;; *) echo 1 ;; esac
        fi ;;
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
        assert (state / f"pool_{p}").read_text().strip() == ("0" if "backfill" in p else "1"), p


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
def test_deploy_makes_existing_ledger_archives_group_readable(tmp_path: Path) -> None:
    """#1404: архивы прежних выкатов (0600) становятся читаемыми для отчёта трафика."""
    _state, runtime, release, stubs, env_file = _deploy_stand(tmp_path)
    archive = runtime / "gateway-state" / "paid_requests.20261001T000000Z.jsonl.gz"
    archive.write_bytes(b"x")
    archive.chmod(0o600)
    result = _run_deploy(release, stubs, env_file)
    assert result.returncode == 0, result.stdout + result.stderr
    assert archive.stat().st_mode & 0o777 == 0o640


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


@pytest.mark.unit
def test_history_preflight_runs_without_the_scrapers_package_imports(tmp_path: Path) -> None:
    """#2030: системный python3 хоста без pandas — валидатор пула истории запускается файлом,
    а не `-m` (импорт пакета scrapers.transfermarkt тянул pandas и ронял preflight)."""
    _state, runtime, release, stubs, env_file = _deploy_stand(tmp_path)
    pkg = release / "scrapers" / "transfermarkt"
    pkg.mkdir(parents=True)
    for init in (release / "scrapers" / "__init__.py", pkg / "__init__.py"):
        init.write_text("raise ModuleNotFoundError(\"No module named 'pandas'\")\n", encoding="utf-8")
    (pkg / "history_pool.py").write_text(
        (ROOT / "scrapers" / "transfermarkt" / "history_pool.py").read_text(encoding="utf-8"),
        encoding="utf-8")
    row = '[{{"host":"{}","port":10001,"username":"u","password":"p"}}]'
    (runtime / "proxy-pool.json").write_text(row.format("current.example"), encoding="utf-8")
    history = runtime / "proxy-pool.history.json"
    history.write_text(row.format("history.example"), encoding="utf-8")
    with env_file.open("a", encoding="utf-8") as fh:
        fh.write(f"TM_HISTORY_STREAMS=2\nTM_PROXY_CONTROL_TOKEN={'c' * 32}\n"
                 f"TM_BACKFILL_PROXY_CONTROL_TOKEN={'h' * 32}\n"
                 f"TRANSFERMARKT_BACKFILL_PROXY_POOL_FILE={history}\n")
    result = _run_deploy(release, stubs, env_file)
    assert "pandas" not in result.stdout + result.stderr
    assert result.returncode == 0, result.stdout + result.stderr


# --------------------------- #2039: доставка в любой час с дренажом полосы ---------------------------

FREEZE_STUB = r"""#!/bin/bash
set -e
S=__STATE__
[ -e "$S/freeze_fails" ] && { echo "заморозка не удалась" >&2; exit 1; }
rel=$(sed -n 's/^TRANSFERMARKT_RELEASES_DIR=//p' "$TRANSFERMARKT_ENV_FILE" | head -1)
src=$(sed -n 's/^TRANSFERMARKT_SOURCE_REPO=//p' "$TRANSFERMARKT_ENV_FILE" | head -1)
tree="$rel/release-${1:0:8}"
git clone -q "$src" "$tree"
git -C "$tree" checkout -q --detach "$1"
chmod 755 "$tree"
echo "дерево заморожено: $tree (sha ${1:0:8})"
"""

# deploy.sh релиза: перепин env, пересоздание (монты, .Created), TM-пулы по streams. Паузы, как
# настоящий deploy.sh, оставляет такими, какими их видел (полоса после дренажа — на паузе).
RELEASE_DEPLOY_STUB = r"""#!/bin/bash
S=__STATE__
new=$1
echo "$*" >> "$S/deploy_calls"
rc=$(cat "$S/deploy_rc" 2>/dev/null || echo 0)
[ "$rc" = 0 ] || exit "$rc"
sed -i "s#^TRANSFERMARKT_RELEASE_ROOT=.*#TRANSFERMARKT_RELEASE_ROOT=$new#" "$TRANSFERMARKT_ENV_FILE"
printf '%s/dags\n%s/deploy/transfermarkt/.airflowignore\n%s/scrapers\n%s/scripts\n%s/configs\n' \
  "$new" "$new" "$new" "$new" "$new" > "$S/mounts___SCHED__"
echo "$new" > "$S/mounts___GW__"
echo "created-$new" > "$S/created___SCHED__"
echo 2026-09-24T03:00:00Z > "$S/started___SCHED__"
for p in transfermarkt_control transfermarkt_proxy; do echo 1 > "$S/pool_$p"; done
for p in transfermarkt_backfill_control transfermarkt_backfill_proxy; do echo 0 > "$S/pool_$p"; done
exit 0
"""


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


class Delivery(Stand):
    """Стенд полной доставки: бой — клон тестового репозитория на первом коммите, master —
    второй. Незакрытой доставки нет; паузы — как в бою (ingest/discover f, backfill/silver t)."""

    def __init__(self, tmp: Path) -> None:
        super().__init__(tmp)
        (self.auto / "transfermarkt-rollback.env").unlink()
        (self.auto / "transfermarkt-inflight").unlink()
        self.source = tmp / "source"
        self.source.mkdir()
        _git(self.source, "init", "-q", "-b", "master")
        d = self.source / "deploy" / "transfermarkt"
        d.mkdir(parents=True)
        for name, body in (("freeze_release.sh", FREEZE_STUB), ("deploy.sh", RELEASE_DEPLOY_STUB)):
            (d / name).write_text(body.replace("__STATE__", str(self.state)).replace(
                "__SCHED__", SCHED).replace("__GW__", GW), encoding="utf-8")
            (d / name).chmod(0o755)
        # Релиз несёт ровно установленную копию автомата: сверка по md5 (#1362).
        (d / "auto_deliver.sh").write_text(self.automat.read_text(encoding="utf-8"), encoding="utf-8")
        (d / "env.sh").write_text((DEPLOY / "env.sh").read_text(encoding="utf-8"), encoding="utf-8")
        (self.source / "marker.txt").write_text("one\n", encoding="utf-8")
        _git(self.source, "add", "-A")
        _git(self.source, "commit", "-q", "-m", "one")
        self.old_sha = _git(self.source, "rev-parse", "HEAD")
        (self.source / "marker.txt").write_text("two\n", encoding="utf-8")
        _git(self.source, "commit", "-q", "-am", "two")
        self.new_sha = _git(self.source, "rev-parse", "HEAD")
        self.live = self.releases / f"release-{self.old_sha[:8]}"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.live)], check=True)
        _git(self.live, "checkout", "-q", "--detach", self.old_sha)
        self.live.chmod(0o755)
        self.target = self.releases / f"release-{self.new_sha[:8]}"
        text = self.env_file.read_text(encoding="utf-8")
        text = text.replace(f"TRANSFERMARKT_RELEASE_ROOT={self.new}", f"TRANSFERMARKT_RELEASE_ROOT={self.live}")
        text = text.replace(f"TRANSFERMARKT_SOURCE_REPO={tmp}/no-repo", f"TRANSFERMARKT_SOURCE_REPO={self.source}")
        self.env_file.write_text(text, encoding="utf-8")
        self.mounts_on(self.live)
        self.put(f"created_{SCHED}", "created-old")
        for dag in DAGS:
            self.put(f"paused_{dag}", "f" if "ingest" in dag or "discover" in dag else "t")
        self.next_run(6 * 3600)

    def next_run(self, seconds: int) -> None:
        self.put("next_run", str(int(time.time()) + seconds))

    def paused(self, dag: str) -> str:
        return (self.state / f"paused_{dag}").read_text().strip()

    def drain(self) -> Path:
        return self.auto / "transfermarkt-drain.env"

    def latch(self, sha: str | None = None) -> Path:
        return self.auto / f"transfermarkt-auto-deliver-attempted-{(sha or self.new_sha)[:8]}"

    def deployed(self) -> bool:
        return (self.state / "deploy_calls").exists()

    def records(self) -> dict[str, dict[str, str]]:
        out = {}
        for path in sorted(self.auto.glob("transfermarkt-window-*.env")):
            out[path.name[len("transfermarkt-window-"):-len(".env")]] = dict(
                line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line)
        return out

    def put_drain(self, started_ago: int) -> None:
        self.drain().write_text(
            f"TARGET={self.new_sha}\nSTARTED_AT={int(time.time()) - started_ago}\n"
            "PAUSED_dag_ingest_transfermarkt=f\nPAUSED_dag_backfill_transfermarkt=t\n", encoding="utf-8")
        self.put("paused_dag_ingest_transfermarkt", "t")


@pytest.fixture
def delivery(tmp_path: Path) -> Delivery:
    return Delivery(tmp_path)


def _assert_pauses_as_before(d: Delivery) -> None:
    for dag in DAGS:
        assert d.paused(dag) == ("f" if "ingest" in dag or "discover" in dag else "t"), dag


@pytest.mark.unit
def test_any_hour_with_a_free_lane_delivers(delivery: Delivery) -> None:
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert delivery.deployed(), delivery.log()
    assert delivery.env_root() == str(delivery.target)
    [(win, rec)] = delivery.records().items()
    assert "T" in win and rec["OUTCOME"] == "delivered" and rec["DEADLINE"].isdigit(), rec
    assert not delivery.drain().exists()
    assert delivery.latch().exists()
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_a_running_portion_is_not_paused_the_lane_is_drained_at_its_end(delivery: Delivery) -> None:
    # Пауза посреди порции заморозила бы её прогон (Airflow 2): ждём границы без паузы.
    delivery.put("busy", "1")
    result = delivery.run()
    assert result.returncode == 0, result.stderr
    assert not delivery.deployed() and not delivery.drain().exists()
    assert delivery.paused("dag_ingest_transfermarkt") == "f"
    assert "жду конца порции" in delivery.log()
    delivery.put("busy", "0")
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert delivery.deployed()
    assert "ДРЕНАЖ: ставлю на паузу" in delivery.log()
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_the_drain_survives_a_failed_freeze_and_the_snapshot_keeps_pre_drain_pauses(delivery: Delivery) -> None:
    delivery.put("freeze_fails", "1")
    result = delivery.run()
    assert result.returncode == 1, result.stderr
    assert delivery.drain().exists()
    assert delivery.paused("dag_ingest_transfermarkt") == "t"
    assert "PAUSED_dag_ingest_transfermarkt=f" in delivery.drain().read_text()
    (delivery.state / "freeze_fails").unlink()
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert delivery.deployed() and not delivery.drain().exists()
    [rec] = delivery.records().values()
    assert rec["OUTCOME"] == "delivered", rec
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_a_drain_timeout_restores_pauses_and_defers_the_sha(delivery: Delivery) -> None:
    delivery.put_drain(started_ago=4000)
    delivery.put("busy", "1")
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert not delivery.deployed() and not delivery.drain().exists()
    _assert_pauses_as_before(delivery)
    assert delivery.latch().read_text().startswith("busy ")
    [rec] = delivery.records().values()
    assert rec["OUTCOME"] == "failed" and "не освободилась" in rec["REASON"], rec
    delivery.put("busy", "0")
    assert delivery.run().returncode == 0
    assert not delivery.deployed() and not delivery.drain().exists()


@pytest.mark.unit
def test_the_off_switch_during_a_drain_returns_the_lane(delivery: Delivery) -> None:
    delivery.put_drain(started_ago=60)
    (delivery.auto / "transfermarkt-auto-deliver.off").write_text("")
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert not delivery.drain().exists() and not delivery.deployed()
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_a_registry_run_blocks_the_drain_and_aborts_a_started_one(delivery: Delivery) -> None:
    delivery.put("busy", "1")            # прогон registry — тоже прогон TM
    delivery.put("registry_busy", "1")
    assert delivery.run().returncode == 0
    assert not delivery.drain().exists() and delivery.paused("dag_ingest_transfermarkt") == "f"
    delivery.put_drain(started_ago=60)
    assert delivery.run().returncode == 0
    assert not delivery.drain().exists() and not delivery.deployed()
    assert not delivery.latch().exists()
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_short_headroom_before_the_registry_run_starts_nothing(delivery: Delivery) -> None:
    delivery.next_run(2 * 3600)          # меньше DRAIN_MAX + NEED_BUDGET + резерв
    result = delivery.run()
    assert result.returncode == 0, result.stderr
    assert not delivery.deployed() and not delivery.drain().exists()
    assert "мало запаса" in delivery.log()
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_an_attempted_sha_is_not_retried_and_a_new_sha_is(delivery: Delivery) -> None:
    delivery.latch().write_text("")
    assert delivery.run().returncode == 0
    assert not delivery.deployed()
    delivery.latch().unlink()
    delivery.latch("0" * 40).write_text("")
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    assert delivery.deployed()


@pytest.mark.unit
def test_rc4_is_busy_not_a_failure_and_defers_the_sha(delivery: Delivery) -> None:
    delivery.put("deploy_rc", "4")
    result = delivery.run()
    assert result.returncode == 0, result.stderr + delivery.log()
    [rec] = delivery.records().values()
    assert rec["OUTCOME"] == "busy", rec
    assert delivery.latch().read_text().startswith("busy ")
    _assert_pauses_as_before(delivery)


@pytest.mark.unit
def test_overdue_records_close_by_deadline_and_legacy_ones_at_0300(delivery: Delivery) -> None:
    delivery.put("busy", "1")
    legacy = delivery.auto / "transfermarkt-window-2026-09-24.env"
    legacy.write_text(f"WINDOW_ID=2026-09-24\nTARGET={delivery.new_sha}\nLAST_REASON=контур занят\n")
    overdue = delivery.auto / "transfermarkt-window-2026-10-01T1200.env"
    overdue.write_text(f"WINDOW_ID=2026-10-01T1200\nTARGET={delivery.new_sha}\nDEADLINE={int(time.time()) - 60}\n"
                       "LAST_REASON=контур занят\n")
    assert delivery.run().returncode == 0
    recs = delivery.records()
    assert recs["2026-09-24"]["OUTCOME"] == "failed", recs
    assert recs["2026-10-01T1200"]["OUTCOME"] == "failed", recs
