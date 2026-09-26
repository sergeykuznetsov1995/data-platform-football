"""Автодоставка нового контура ESPN (#1507): решения автомата deploy/espn/auto_deliver.py.

git настоящий (временный репозиторий с origin), docker/metadb — подделка, которая ведёт себя
как контур: `up` пересоздаёт scheduler на корне из ESPN_RELEASE_ROOT, DAG с маркером BROKEN
даёт import_error, `airflow-init` импортирует pools.json корня, `docker exec … sha256sum`
считает байты смонтированного корня. Часы и Telegram — подделки.
"""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "deploy/espn/auto_deliver.py"
COMPOSE_PATH = ROOT / "deploy/espn/airflow.compose.yaml"


def _load():
    spec = importlib.util.spec_from_file_location("espn_auto_deliver", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["espn_auto_deliver"] = module
    spec.loader.exec_module(module)
    return module


ad = _load()
START = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)   # до волны 06:00 — 3 ч

DAG_OK = '''from datetime import datetime
from airflow import DAG


def run(**_):
    from scrapers.espn import wave
    return wave.go()
'''
FILES = {
    "deploy/espn/dags/dag_espn_current.py": DAG_OK,
    "deploy/espn/pools.json": json.dumps({"espn_live": {"slots": 4, "description": "d"}}),
    "deploy/espn/airflow.compose.yaml": COMPOSE_PATH.read_text(),
    "deploy/espn/auto_deliver.py": MODULE_PATH.read_text(),
    "scrapers/__init__.py": "",
    "scrapers/espn/__init__.py": "",
    "scrapers/espn/wave.py": "from .helper import go\nfrom scrapers.base.trino_manager import T\n",
    "scrapers/espn/helper.py": "def go():\n    return 1\n",
    "scrapers/base/__init__.py": "",
    "scrapers/base/trino_manager.py": "T = 1\n",
    "scrapers/other/x.py": "X = 1\n",
    "configs/espn/denominator.tsv": "slug\n",
}


def sh(*cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class Repo:
    """Рабочий клон с origin: коммит → push в origin/master (или в ветку)."""

    def __init__(self, base: Path):
        self.origin = base / "origin.git"
        self.work = base / "author"
        self.clone = base / "repo"
        sh("git", "init", "-q", "--bare", "-b", "master", str(self.origin))
        sh("git", "clone", "-q", str(self.origin), str(self.work))
        sh("git", "checkout", "-q", "-b", "master", cwd=self.work)
        self.commit(FILES, "init")
        sh("git", "clone", "-q", str(self.origin), str(self.clone))

    def commit(self, files: dict[str, str], msg: str, branch: str = "master") -> str:
        for rel, text in files.items():
            path = self.work / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        sh("git", "add", "-A", cwd=self.work)
        sh("git", "commit", "-q", "--allow-empty", "-m", msg, cwd=self.work)
        sh("git", "push", "-q", "origin", f"HEAD:{branch}", cwd=self.work)
        return sh("git", "rev-parse", "HEAD", cwd=self.work)

    def branch(self, name: str) -> None:
        sh("git", "checkout", "-q", "-b", name, cwd=self.work)

    def checkout(self, name: str) -> None:
        sh("git", "checkout", "-q", name, cwd=self.work)


class World:
    """Контур espn-live глазами автомата."""

    def __init__(self):
        self.root: Path | None = None          # корень, на котором стоит scheduler
        self.parsed: datetime | None = None
        self.dag_error = False
        self.import_errors: list[tuple[str, datetime]] = []
        # slot_pool: имя → (слоты, include_deferred)
        self.pools: dict[str, tuple[int, bool]] = {"default_pool": (128, False), "espn_live": (4, False)}
        self.running = 0
        self.metadb_up = True
        self.env = ["AIRFLOW_HOME=/opt/airflow", "TRINO_PASSWORD=secret"]
        self.tamper: set[str] = set()          # относительные пути, чьи байты в контейнере «чужие»
        self.labelled: set[str] = set()        # корни в метках контейнеров (в т.ч. остановленных)
        self.compose_calls: list[list[str]] = []
        self.compose_envs: list[str] = []
        self.ignore_init = False


class FakeHost(ad.Host):
    def __init__(self, paths, world):
        super().__init__(paths)
        self.world = world
        self.clock = START
        self.sent: list[str] = []
        self.lines: list[str] = []

    def now(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += timedelta(seconds=seconds)

    def log(self, msg):
        self.lines.append(msg)

    def telegram(self, text):
        self.sent.append(text)
        return True

    def run(self, cmd, *, env=None, timeout=900):
        if cmd[0] != "docker":
            return super().run(cmd, env=env, timeout=timeout)
        w = self.world
        if cmd[1] == "compose":
            return self._compose(cmd, env)
        if cmd[1] == "exec" and cmd[2] == ad.METADB:
            return self._psql(cmd[-1])
        if cmd[1] == "exec" and cmd[2] == ad.SCHEDULER:
            target = cmd[-1]
            rel = {v: k for k, v in ad.BYTE_CHECK.items()}[target]
            base = w.root / rel
            lines = []
            for p in sorted(base.rglob("*")):
                if p.is_file():
                    name = str(p.relative_to(base))
                    digest = hashlib.sha256(p.read_bytes() + (b"x" if f"{rel}/{name}" in w.tamper else b"")).hexdigest()
                    lines.append(f"{digest}  ./{name}")
            return 0, "\n".join(lines) + "\n", ""
        if cmd[1] == "inspect":
            mounts = [{"Destination": "/opt/airflow/dags", "Source": str(w.root / ad.DAG_DIR_REL)}]
            return 0, json.dumps(mounts) + "|" + json.dumps(w.env) + "\n", ""
        if cmd[1] == "ps":
            root = cmd[-1].split("=", 2)[2]
            return 0, ("abc123\n" if root in w.labelled else ""), ""
        raise AssertionError(cmd)

    def _compose(self, cmd, env):
        w = self.world
        assert cmd[2:4] == ["-p", ad.PROJECT]
        root = Path(env["ESPN_RELEASE_ROOT"])
        assert cmd[cmd.index("-f") + 1] == str(root / ad.COMPOSE_REL)
        assert not any(ad.is_proxy_name(k) for k in env)
        args = cmd[cmd.index("--env-file") + 2:]
        w.compose_calls.append(args)
        w.compose_envs.append(str(root))
        if args[:1] == ["run"]:
            if not w.ignore_init:   # как airflow-init: import + удаление пулов не из кода
                code = json.loads((root / ad.POOLS_REL).read_text())
                w.pools = {"default_pool": w.pools["default_pool"], **{
                    n: (c["slots"], c.get("include_deferred", False)) for n, c in code.items()}}
            return 0, "", ""
        if "airflow-metadb" in args:
            return 0, "", ""
        # up scheduler/webserver: пересоздание и первый разбор DAG через 30 с
        if w.root is not None:
            w.labelled.discard(str(w.root))
        w.root = root
        w.labelled.add(str(root))
        broken = "BROKEN" in (root / "deploy/espn/dags/dag_espn_current.py").read_text()
        at = self.clock + timedelta(seconds=30)
        if broken:
            w.import_errors = [("/opt/airflow/dags/dag_espn_current.py", at)]
            w.dag_error = True
        else:
            w.import_errors = []
            w.dag_error = False
            w.parsed = at
        return 0, "", ""

    def _psql(self, sql):
        w = self.world
        if not w.metadb_up:
            return 2, "", "down"
        cut_m = re.search(r"timestamptz '([^']+)'", sql)
        cut = datetime.fromisoformat(cut_m.group(1)) if cut_m else None
        visible = lambda t: t <= self.clock   # событие разбора видно, когда наступило
        if sql == "SELECT now()":
            return 0, self.clock.isoformat() + "\n", ""
        if sql.startswith("SELECT count(*) FROM dag_run"):
            return 0, f"{w.running}\n", ""
        if sql.startswith("SELECT filename FROM import_error"):
            return 0, "".join(f"{f}\n" for f, t in w.import_errors if visible(t) and t > cut), ""
        if sql == "SELECT count(*) FROM import_error":
            return 0, f"{sum(visible(t) for _, t in w.import_errors)}\n", ""
        if sql.startswith("SELECT has_import_errors"):
            if w.parsed is None and not w.dag_error:
                return 0, "", ""
            fresh = w.parsed is not None and visible(w.parsed) and w.parsed > cut
            return 0, f"{'t' if w.dag_error else 'f'}|{'t' if fresh else 'f'}\n", ""
        if sql.startswith("SELECT pool || '|' || slots || '|' || CASE WHEN include_deferred"):
            assert "WHERE pool <> 'default_pool'" in sql
            return 0, "".join(f"{p}|{s}|{'t' if d else 'f'}\n" for p, (s, d) in sorted(w.pools.items())
                              if p != "default_pool"), ""
        raise AssertionError(sql)


@pytest.fixture(autouse=True)
def _git_identity(monkeypatch):
    for key in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        monkeypatch.setenv(key, "t")
    for key in ("GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"):
        monkeypatch.setenv(key, "t@t")


@pytest.fixture
def env(tmp_path):
    repo = Repo(tmp_path)
    paths = ad.Paths(repo=repo.clone, deploy_dir=tmp_path / "deploy", release_parent=tmp_path / "rel",
                     env_file=tmp_path / "espn.env", tg_env=tmp_path / "tg.env", self_file=MODULE_PATH)
    paths.release_parent.mkdir()
    paths.env_file.write_text("TRINO_PASSWORD=x\nS3_ACCESS_KEY=y\n")
    paths.env_file.chmod(0o600)
    world = World()
    host = FakeHost(paths, world)
    base = sh("git", "rev-parse", "HEAD", cwd=repo.work)
    return repo, paths, world, host, base


def seed(env_):
    repo, paths, world, host, base = env_
    assert ad.main(["--seed", base], host=host) == 0
    host.sent.clear()
    world.compose_calls.clear()
    world.compose_envs.clear()
    return base


def state(paths, name):
    p = paths.state / name
    return p.read_text().strip() if p.exists() else None


def up_calls(world):
    return [c for c in world.compose_calls if c[:1] == ["up"] and "airflow-scheduler" in c]


# ---------- посев и успешная доставка ----------

def test_seed_brings_up_metadb_init_services_and_pins_accepted(env):
    repo, paths, world, host, base = env
    assert ad.main(["--seed", base], host=host) == 0
    assert world.compose_calls == [
        ["up", "-d", "--no-deps", "--wait", "airflow-metadb"],
        ["run", "--rm", "--no-deps", "-T", "airflow-init"],
        ["up", "-d", "--no-deps", "--force-recreate", "airflow-scheduler", "airflow-webserver"],
    ]
    assert state(paths, "accepted") == base
    assert world.root == paths.root(base)
    assert host.sent == [f"✅ ESPN: контур espn-live посеян, {base[:12]}"]


def test_contour_commit_is_delivered_and_pinned(env):
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    assert ad.main([], host=host) == 0
    assert state(paths, "accepted") == sha
    assert state(paths, "accepted-prev") == base
    assert not (paths.state / "inflight").exists()
    assert world.root == paths.root(sha)
    # pools.json не менялся — airflow-init не запускался
    assert world.compose_calls == [["up", "-d", "--no-deps", "--force-recreate",
                                    "airflow-scheduler", "airflow-webserver"]]
    assert host.sent == [f"✅ ESPN выкачен {sha[:12]} (master)"]


def test_pin_lives_only_in_accepted(env):
    """Корень релиза в вызове compose выводится из state/accepted; env-файл его задавать не может."""
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({"configs/espn/denominator.tsv": "slug\nx\n"}, "cfg")
    assert ad.main([], host=host) == 0
    assert world.compose_envs == [str(paths.root(state(paths, "accepted")))]
    assert sorted(p.name for p in paths.state.iterdir() if re.search(r"[0-9a-f]{40}", p.read_text() if p.is_file() else "")) \
        == ["accepted", "accepted-prev"]
    paths.env_file.write_text("TRINO_PASSWORD=x\nESPN_RELEASE_ROOT=/root/espn-release-x\n")
    repo.commit({"scrapers/espn/helper.py": "def go():\n    return 3\n"}, "next")
    assert ad.main([], host=host) == 1
    assert "ESPN_RELEASE_ROOT" in host.lines[-1]
    assert state(paths, "accepted") == sha


def test_env_file_with_proxy_or_wide_mode_is_refused(env):
    repo, paths, world, host, base = env
    seed(env)
    repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    paths.env_file.write_text("HTTPS_PROXY=http://p\n")
    assert ad.main([], host=host) == 1 and "HTTPS_PROXY" in host.lines[-1]
    paths.env_file.write_text("TRINO_PASSWORD=x\n")
    paths.env_file.chmod(0o644)
    assert ad.main([], host=host) == 1 and "0600" in host.lines[-1]
    assert up_calls(world) == []


def test_pools_change_runs_init_before_up_and_is_accepted(env):
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({"deploy/espn/pools.json": json.dumps({"espn_live": {"slots": 6, "description": "d"}})}, "pools")
    assert ad.main([], host=host) == 0
    assert [c[0] for c in world.compose_calls] == ["run", "up"]
    assert world.pools["espn_live"] == (6, False)
    assert state(paths, "accepted") == sha


# ---------- что выкатывать ----------

def test_monotonic_master_must_descend_from_accepted(env):
    repo, paths, world, host, base = env
    seed(env)
    repo.branch("side")
    side = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 9\n"}, "side", branch="side")
    (paths.state / "accepted").write_text(side + "\n")   # живой SHA не в истории master
    repo.checkout("master")
    repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "m")
    assert ad.main([], host=host) == 1
    assert "не потомок" in host.lines[-1]
    assert up_calls(world) == []
    assert ad.main([], host=host) == 1
    assert len(host.sent) == 1   # та же остановка — в Telegram раз в сутки


@pytest.mark.parametrize("rel, delivered", [
    ("scrapers/other/x.py", False),               # вне замыкания импортов DAG
    ("README.md", False),
    ("scrapers/espn/helper.py", True),            # относительный импорт из wave
    ("scrapers/base/trino_manager.py", True),     # общий модуль в замыкании
    ("configs/espn/denominator.tsv", True),
    ("deploy/espn/README.md", True),
])
def test_path_filter(env, rel, delivered):
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({rel: "# changed\nX = 2\n"}, "touch")
    assert ad.main([], host=host) == 0
    assert (state(paths, "accepted") == sha) is delivered
    assert bool(up_calls(world)) is delivered


def test_import_closure_follows_relative_and_lazy_imports():
    files = {"d.py": "def f():\n    from pkg import a\n",
             "pkg/__init__.py": "def __getattr__(name):\n    from .c import C\n    return C\n",
             "pkg/a.py": "from .b import x\nimport os\n", "pkg/b.py": "x = 1\n", "pkg/c.py": ""}
    assert ad.import_closure(files.get, ["d.py"]) == {"d.py", "pkg/__init__.py", "pkg/a.py", "pkg/b.py"}


def test_rejected_sha_and_same_contour_are_not_retried(env):
    repo, paths, world, host, base = env
    seed(env)
    bad = repo.commit({"deploy/espn/dags/dag_espn_current.py": DAG_OK + "# BROKEN\n"}, "broken")
    assert ad.main([], host=host) == 1
    assert state(paths, "rejected") == bad
    calls = len(world.compose_calls)
    assert ad.main([], host=host) == 0                      # тот же master — не повторяем
    repo.commit({"scrapers/other/x.py": "X = 5\n"}, "unrelated")
    assert ad.main([], host=host) == 0                      # контур = отклонённому — не повторяем
    assert len(world.compose_calls) == calls
    fix = repo.commit({"deploy/espn/dags/dag_espn_current.py": DAG_OK + "# fixed\n"}, "fix")
    assert ad.main([], host=host) == 0
    assert state(paths, "accepted") == fix


# ---------- окно ----------

@pytest.mark.parametrize("now, wave", [
    ("2026-10-01T03:00:00", "2026-10-01T06:00:00"),
    ("2026-10-01T06:00:00", "2026-10-01T12:00:00"),
    ("2026-10-01T23:55:00", "2026-10-02T00:00:00"),
])
def test_next_wave(now, wave):
    tz = timezone.utc
    assert ad.next_wave(datetime.fromisoformat(now).replace(tzinfo=tz)) == datetime.fromisoformat(wave).replace(tzinfo=tz)


@pytest.mark.parametrize("clock, running, busy", [
    (datetime(2026, 10, 1, 5, 51, tzinfo=timezone.utc), 0, True),    # до волны 06:00 9 мин
    (datetime(2026, 10, 1, 5, 50, tzinfo=timezone.utc), 0, False),   # ровно 10 мин
    (datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc), 1, True),     # идёт волна
])
def test_window_between_waves(env, clock, running, busy):
    repo, paths, world, host, base = env
    seed(env)
    host.clock, world.running = clock, running
    sha = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    assert ad.main([], host=host) == 0
    assert (state(paths, "accepted") == sha) is not busy
    assert bool(up_calls(world)) is not busy
    assert host.sent == ([] if busy else [f"✅ ESPN выкачен {sha[:12]} (master)"])


# ---------- приёмка и откат ----------

def test_broken_dag_target_is_rolled_back(env):
    repo, paths, world, host, base = env
    seed(env)
    repo.branch("espn/broken")
    bad = repo.commit({"deploy/espn/dags/dag_espn_current.py": "import nope\n# BROKEN\n"}, "b", branch="espn/broken")
    assert ad.main(["--target", "origin/espn/broken"], host=host) == 1
    assert state(paths, "accepted") == base
    assert state(paths, "rejected") == bad
    assert world.root == paths.root(base)
    assert world.compose_envs == [str(paths.root(bad)), str(paths.root(base))]
    assert not (paths.state / "inflight").exists()
    assert len(host.sent) == 1 and host.sent[0].startswith(f"❌ ESPN: {bad[:12]} (--target) не принят — import_error")
    assert f"откат на {base[:12]} принят" in host.sent[0]


@pytest.mark.parametrize("spoil, reason", [
    (lambda w: w.env.append("HTTPS_PROXY=http://p"), "прокси: HTTPS_PROXY"),
    (lambda w: w.tamper.add("scrapers/espn/helper.py"), "байты /opt/airflow/scrapers/espn"),
])
def test_acceptance_checks_container(env, spoil, reason):
    repo, paths, world, host, base = env
    seed(env)
    spoil(world)
    sha = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    assert ad.main([], host=host) == 2   # откат упирается в ту же порчу — нужны руки
    assert reason in host.sent[0]
    assert state(paths, "accepted") == base and state(paths, "rejected") == sha
    assert (paths.state / "off").exists()
    assert ad.main([], host=host) == 0 and "выключатель" in host.lines[-1]


def test_pools_differ_from_code_fail_acceptance(env):
    repo, paths, world, host, base = env
    seed(env)
    world.ignore_init = True
    sha = repo.commit({"deploy/espn/pools.json": json.dumps({"espn_live": {"slots": 6, "description": "d"}})}, "pools")
    assert ad.main([], host=host) == 1   # откат на старые пулы (4) проходит
    assert "пулы ≠ pools.json (слоты|include_deferred): espn_live=4|f (надо 6|f)" in host.sent[0]
    assert state(paths, "rejected") == sha


def test_include_deferred_change_runs_init_and_extra_pool_fails_acceptance(env):
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({"deploy/espn/pools.json": json.dumps(
        {"espn_live": {"slots": 4, "description": "d", "include_deferred": True}})}, "deferred")
    assert ad.main([], host=host) == 0
    assert [c[0] for c in world.compose_calls] == ["run", "up"]
    assert world.pools["espn_live"] == (4, True) and state(paths, "accepted") == sha
    # лишний пул в metadb (завели руками) — пулы ≠ коду, приёмка не проходит
    world.compose_calls.clear()
    world.pools["espn_manual"] = (1, False)
    bad = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 7\n"}, "next")
    assert ad.main([], host=host) == 2
    assert "espn_manual=1|f (надо нет)" in host.sent[-1]
    assert state(paths, "rejected") == bad


def test_syntax_broken_rejected_release_does_not_block_the_fix(env):
    """Astra 1507 р1 п.1: отклонённый релиз с синтаксической ошибкой в DAG не валит следующий cron."""
    repo, paths, world, host, base = env
    seed(env)
    repo.branch("espn/syntax")
    bad = repo.commit({"deploy/espn/dags/dag_espn_current.py": "def (:\n# BROKEN\n"}, "s", branch="espn/syntax")
    assert ad.main(["--target", "origin/espn/syntax"], host=host) == 1
    assert state(paths, "rejected") == bad
    repo.checkout("master")
    fix = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    assert ad.main([], host=host) == 0
    assert state(paths, "accepted") == fix


def test_dag_not_reparsed_times_out_after_7_min(env):
    repo, paths, world, host, base = env
    seed(env)
    world.metadb_up = True
    sha = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    orig = host._compose

    def no_parse(cmd, env_):
        rc = orig(cmd, env_)
        if "airflow-scheduler" in cmd and Path(env_["ESPN_RELEASE_ROOT"]) == paths.root(sha):
            world.parsed = None
        return rc
    host._compose = no_parse
    t0 = host.clock
    assert ad.main([], host=host) == 1
    assert host.clock - t0 >= timedelta(seconds=ad.ACCEPT_TIMEOUT_S)
    assert "has_import_errors|перечитан" in host.sent[0]


def test_inflight_blocks_and_alerts_once(env):
    repo, paths, world, host, base = env
    seed(env)
    (paths.state / "inflight").write_text("abc master\n")
    assert ad.main([], host=host) == 0
    assert ad.main([], host=host) == 0
    assert len(host.sent) == 1 and "незавершённая доставка" in host.sent[0]


def test_stale_host_copy_stops_before_anything(env, tmp_path):
    repo, paths, world, host, base = env
    seed(env)
    repo.commit({"deploy/espn/auto_deliver.py": MODULE_PATH.read_text() + "\n# v2\n"}, "self")
    assert ad.main([], host=host) == 1
    assert "обнови автомат" in host.sent[0]
    assert up_calls(world) == []


def test_manual_rollback_returns_previous_and_rejects_current(env):
    repo, paths, world, host, base = env
    seed(env)
    sha = repo.commit({"scrapers/espn/helper.py": "def go():\n    return 2\n"}, "fix")
    assert ad.main([], host=host) == 0
    assert ad.main(["--rollback"], host=host) == 0
    assert state(paths, "accepted") == base
    assert state(paths, "rejected") == sha
    assert state(paths, "accepted-prev") is None
    assert world.root == paths.root(base)
    assert ad.main([], host=host) == 0          # cron не возвращает откаченный master
    assert state(paths, "accepted") == base


# ---------- корни релиза ----------

def test_release_root_is_read_only_archive_of_mounted_paths(env):
    repo, paths, world, host, base = env
    seed(env)
    root = paths.root(base)
    assert sorted(p.name for p in root.iterdir()) == ["configs", "deploy", "scrapers"]
    files = [p for p in root.rglob("*")]
    assert files and all(not (p.stat().st_mode & 0o222) for p in [root, *files])


def test_cleanup_keeps_three_newest_accepted_prev_and_labelled(env):
    repo, paths, world, host, base = env
    seed(env)
    old = []
    for i in range(4):
        sha = repo.commit({"scrapers/espn/helper.py": f"def go():\n    return {i}\n"}, f"c{i}")
        sh("git", "-C", str(repo.clone), "fetch", "-q", "origin")
        root = ad.build_root(host, sha)
        os.utime(root, (1_000_000 + i, 1_000_000 + i))
        old.append(root)
    os.utime(paths.root(base), (900_000, 900_000))   # живой корень — самый старый
    world.labelled.add(str(old[0]))                  # на старый корень ссылается остановленный контейнер
    ad.cleanup_roots(host)
    left = {p for p in paths.release_parent.iterdir() if p.name.startswith(ad.RELEASE_PREFIX)}
    # три новейших + живой (accepted) + помеченный меткой контейнера — удалять нечего
    assert left == {paths.root(base), *old}
    world.labelled.discard(str(old[0]))
    ad.cleanup_roots(host)
    left = {p for p in paths.release_parent.iterdir() if p.name.startswith(ad.RELEASE_PREFIX)}
    assert left == {paths.root(base), old[1], old[2], old[3]}


# ---------- compose и замыкание импортов реального DAG ----------

def _compose():
    return yaml.safe_load(COMPOSE_PATH.read_text())


def _bind_sources(service):
    return [v["source"] for v in service.get("volumes", []) if isinstance(v, dict) and v.get("type") == "bind"]


def test_compose_project_images_and_no_proxy():
    doc = _compose()
    assert doc["name"] == ad.PROJECT
    text = "\n".join(l.split(" #")[0] for l in COMPOSE_PATH.read_text().splitlines()
                     if not l.lstrip().startswith("#"))
    for name, svc in doc["services"].items():
        assert re.search(r"@sha256:[0-9a-f]{64}$", svc["image"]), name
        assert svc.get("pull_policy") == "never", name
        env_names = list(svc.get("environment", {}))
        assert not [n for n in env_names if ad.is_proxy_name(n)], name
    assert not re.search(r"(?i)_proxy\b", text)
    for forbidden in ("CANARY", "CONTROL_DATABASE", "RELEASE_COMMIT", "TREE_SHA256"):
        assert forbidden not in text
    # корень релиза без умолчания: без автомата compose не рендерится
    assert "${ESPN_RELEASE_ROOT:?" in text and not re.search(r"\$\{ESPN_RELEASE_ROOT:?-", text)
    assert doc["services"]["airflow-webserver"]["ports"] == ["127.0.0.1:8089:8080"]
    assert {v["name"] for v in doc["volumes"].values()} == {"espn_live_pgdata", "espn_live_logs", "espn_live_state"}
    assert "espn.release_root" not in json.dumps(doc["services"]["airflow-metadb"])


def test_compose_binds_are_directories_of_the_release_root():
    doc = _compose()
    prefix = "${ESPN_RELEASE_ROOT}/"
    for name, svc in doc["services"].items():
        for src in _bind_sources(svc):
            assert src.startswith(prefix), (name, src)
            rel = src[len(prefix):]
            assert (ROOT / rel).is_dir(), (name, src)   # файловых монтирований нет
            assert any(rel == p or rel.startswith(p + "/") for p in ad.ARCHIVE_PATHS), (name, src)


def test_dag_import_closure_is_inside_mounted_dirs():
    """Всё, что импортирует DAG нового контура, лежит в каталогах, смонтированных в scheduler."""
    sched = _compose()["services"]["airflow-scheduler"]
    mounted = [s[len("${ESPN_RELEASE_ROOT}/"):] + "/" for s in _bind_sources(sched)]

    def read(rel):
        p = ROOT / rel
        return p.read_text() if p.is_file() else None

    dags = sorted(str(p.relative_to(ROOT)) for p in (ROOT / ad.DAG_DIR_REL).glob("*.py"))
    closure = ad.import_closure(read, dags)
    assert "scrapers/espn/wave.py" in closure and "scrapers/base/iceberg_writer.py" in closure
    outside = sorted(f for f in closure if not any(f.startswith(m) for m in mounted))
    assert outside == []
    assert all(any(f.startswith(p) for p in ad.ARCHIVE_PATHS) for f in closure)


def test_compose_metadb_healthcheck_is_tcp():
    """Astra 1507 р1 п.5: временный сервер инициализации слушает только сокет."""
    test = _compose()["services"]["airflow-metadb"]["healthcheck"]["test"]
    assert "pg_isready -h 127.0.0.1" in test[-1]


def test_pools_json_matches_dag_pool_and_concurrency():
    pools = ad.pools_of(ROOT)
    src = (ROOT / "deploy/espn/dags/dag_espn_current.py").read_text()
    tree = ast.parse(src)
    live_pool = next(n.value.value for n in tree.body if isinstance(n, ast.Assign)
                     and getattr(n.targets[0], "id", None) == "LIVE_POOL")
    tis = [kw.value.value for n in ast.walk(tree) if isinstance(n, ast.Call)
           for kw in n.keywords if kw.arg == "max_active_tis_per_dag"]
    assert pools == {live_pool: f"{tis[0]}|f"} == {"espn_live": "4|f"}
