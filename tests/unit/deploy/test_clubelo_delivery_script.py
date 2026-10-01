"""Автомат ночной доставки ClubElo против заглушек (#1465).

Копия стенда `test_understat_delivery_script.py`: настоящий `deploy/clubelo/auto_deliver.sh`
гоняется целиком на «установленной» копии с ЕДИНСТВЕННОЙ правкой — фиксированный PATH автомата
получает впереди каталог заглушек (`docker`, `date`, `curl`, `pgrep`, `sleep`). Та же копия
лежит в тестовом репозитории: автомат сверяет себя с master по md5.

Git — настоящий: коммит 1 — «бой», коммит 2 — master с правками сценария; боевое дерево — клон
на коммите 1, канонический клон — клон с origin. Заглушка метабазы отвечает по подстрокам SQL,
но ответы выводит из содержимого боевого дерева: маркер BROKEN — DAG «не импортируется»
(`t|f`), маркер IMPORTERR — своя строка `import_error` ClubElo; запрос `import_error` без
фильтра ClubElo получает 6 (чужие строки), файл `stale` — планировщик не перечитывает DAG
(`f|f`). На каждый `now()` заглушка снимает содержимое daily.py — так видно, что метка взята
после записи. Отличие от Understat: удаление внутри scrapers/clubelo/ и тестов ClubElo
доставляется и откатывается; удаление в dags/ — руки.
"""

from __future__ import annotations

from pathlib import Path
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
AUTO = ROOT / "deploy" / "clubelo" / "auto_deliver.sh"
PATH_LINE = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
NIGHT = "2026-09-24 01:40:00"
DAY = "20260924"
MAIN = "scrapers/clubelo/daily.py"
# Разрешённое отставание (#1465): в стенде alerts.py «до #1477» — этот текст; его blob
# подставляется в ALLOWED_LAG установленной копии (и в копию в тестовом master).
LAG_BODY = "def telegram_on_failure(ctx): pass  # до #1477\n"
LAG_BLOB = subprocess.run(["git", "hash-object", "--stdin"], input=LAG_BODY, capture_output=True,
                          text=True, check=True).stdout.strip()
CONFIG_LAG_BODIES = {
    "dags/utils/config.py": "SCHEDULES = {'dag_transform_fotmob_silver': None}\n",
    "dags/utils/medallion_config.py": "def get_source_priority_exprs(): return 'COALESCE(x)'\n",
}
LAG_BLOBS = {
    path: subprocess.run(["git", "hash-object", "--stdin"], input=body,
                         capture_output=True, text=True, check=True).stdout.strip()
    for path, body in {"dags/utils/alerts.py": LAG_BODY, **CONFIG_LAG_BODIES}.items()
}

BASE_FILES = {
    "scrapers/__init__.py": "",
    "scrapers/base/base_scraper.py": "BASE = 1\n",
    "scrapers/utils/__init__.py": "",
    "dags/utils/__init__.py": "",
    "dags/utils/config.py": "SCHEDULES = {}\n",
    "dags/utils/default_args.py": "DEFAULT_ARGS = {}\n",
    "dags/utils/alerts.py": "def telegram_on_failure(ctx): pass\n",
    "dags/utils/medallion_config.py": "FLOOR = 1\n",
    "scrapers/utils/retry_policy.py": "RETRY = 1\n",
    "scrapers/clubelo/__init__.py": "",
    MAIN: "VERSION = 1\n",
    "scrapers/clubelo/scraper.py": "OLD = 1\n",
    "dags/utils/clubelo_tasks.py": "TASKS = 1\n",
    "dags/scripts/run_clubelo_scraper.py": "RUN = 1\n",
    "dags/dag_ingest_clubelo.py": "INGEST = 1\n",
    "dags/dag_other.py": "OTHER = 1\n",
    "tests/unit/scrapers/test_clubelo_scraper.py": "def test_old(): pass\n",
    "tests/unit/scrapers/test_other.py": "def test_other(): pass\n",
}


def _script(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


class Stand:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.stubs, self.ss = tmp / "bin", tmp / "stub-state"
        self.state, self.out = tmp / "state", tmp / "out"
        for d in (self.stubs, self.ss):
            d.mkdir()
        text = AUTO.read_text(encoding="utf-8")
        assert text.count(PATH_LINE) == 1, "автомат перестал фиксировать PATH — правь тест"
        self.installed_text = text.replace(PATH_LINE, PATH_LINE.replace("export PATH=", f"export PATH={self.stubs}:"))
        lag = [ln for ln in text.splitlines() if ln.startswith('ALLOWED_LAG="')]
        assert len(lag) == 1, lag
        # Substitute only entries present in the real script: a missing production
        # allowance must make the new regression fail, not get added by the fixture.
        entries = dict(item.split("=") for item in lag[0].split('"')[1].split())
        assert entries.keys() <= LAG_BLOBS.keys(), entries
        fixture_lag = " ".join(f"{path}={LAG_BLOBS[path]}" for path in entries)
        self.installed_text = self.installed_text.replace(lag[0], f'ALLOWED_LAG="{fixture_lag}"')
        self.installed = tmp / "clubelo-auto-deliver.sh"
        self.installed.write_text(self.installed_text, encoding="utf-8")
        self.installed.chmod(0o755)
        self.source = tmp / "source"
        self.source.mkdir()
        _git(self.source, "init", "-q", "-b", "master")
        for rel, body in BASE_FILES.items():
            self.write(rel, body)
        self.write("deploy/clubelo/auto_deliver.sh", self.installed_text)
        _git(self.source, "add", "-A")
        _git(self.source, "commit", "-q", "-m", "бой")
        self.base = _git(self.source, "rev-parse", "HEAD")
        self.tree = tmp / "tree"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.tree)], check=True)
        _git(self.tree, "checkout", "-q", "--detach", self.base)
        self.repo = tmp / "canon"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.repo)], check=True)
        self.state.mkdir()
        (self.state / "clubelo-accepted").write_text(self.base + "\n", encoding="utf-8")
        self.tg_env = tmp / "telegram.env"
        self.tg_env.write_text("TELEGRAM_BOT_TOKEN=fake\nTELEGRAM_CHAT_ID=1\n", encoding="utf-8")
        (self.ss / "now").write_text(NIGHT, encoding="utf-8")
        self._stubs()

    def write(self, rel: str, body: str) -> None:
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")

    def master(self, changes: dict[str, str | None], renames: dict[str, str] | None = None) -> str:
        for rel, body in changes.items():
            if body is None:
                _git(self.source, "rm", "-q", rel)
            else:
                self.write(rel, body)
        for old, new in (renames or {}).items():
            (self.source / new).parent.mkdir(parents=True, exist_ok=True)
            _git(self.source, "mv", old, new)
        _git(self.source, "add", "-A")
        _git(self.source, "commit", "-q", "-m", "master")
        return _git(self.source, "rev-parse", "HEAD")

    def _stubs(self) -> None:
        ss, tree = self.ss, self.tree
        _script(self.stubs / "docker", f'''sql="${{@: -1}}"
echo "$sql" >> {ss}/docker.log
case "$sql" in
  *has_import_errors*)
    echo "$sql" | grep -o "dag_id='[a-z_]*'" >> {ss}/dags_queried
    if [ -f {ss}/stale ]; then echo "f|f"
    elif grep -rqs BROKEN {tree}/scrapers/clubelo {tree}/dags; then echo "t|f"; else echo "f|t"; fi ;;
  *import_error*"like '%clubelo%'"*) grep -rqs IMPORTERR {tree}/scrapers/clubelo {tree}/dags && echo 1 || echo 0 ;;
  *import_error*) echo 6 ;;
  *task_instance*) cat {ss}/busy 2>/dev/null || echo 0 ;;
  *"now()"*) cat {tree}/{MAIN} >> {ss}/now_snapshots; echo "2026-09-24 01:40:05+00" ;;
  *) echo "неизвестный SQL: $sql" >&2; exit 1 ;;
esac
''')
        _script(self.stubs / "date", f'''args=(); for a in "$@"; do [ "$a" = -u ] || args+=("$a"); done
exec /bin/date -u -d "$(cat {ss}/now)" "${{args[@]}}"
''')
        _script(self.stubs / "curl", f'printf "%s\\n" "$*" >> {ss}/tg.log; echo \'{{"ok":true}}\'\n')
        _script(self.stubs / "pgrep", "exit 1\n")
        _script(self.stubs / "sleep", "exit 0\n")

    def run(self, *args: str) -> subprocess.CompletedProcess:
        env = {
            "HOME": str(self.tmp), "TREE": str(self.tree), "REPO": str(self.repo),
            "STATE": str(self.state), "OUT": str(self.out), "TG_ENV": str(self.tg_env),
        }
        return subprocess.run(
            ["bash", str(self.installed), *args], env=env, capture_output=True, text=True, timeout=120,
        )

    def accepted(self) -> str:
        return (self.state / "clubelo-accepted").read_text(encoding="utf-8").strip()

    def tg(self) -> str:
        path = self.ss / "tg.log"
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def log(self) -> str:
        path = self.out / "auto_deliver.log"
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def tree_text(self, rel: str) -> str:
        return (self.tree / rel).read_text(encoding="utf-8")


@pytest.fixture
def stand(tmp_path: Path) -> Stand:
    return Stand(tmp_path)


def test_nothing_to_deliver_moves_base_without_writes(stand: Stand) -> None:
    sha = stand.master({"README.md": "не ClubElo\n", "tests/unit/scrapers/test_other.py": "X = 2\n"})
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.accepted() == sha
    assert "нечего доставлять" in stand.log()
    assert not (stand.out / DAY).exists()
    assert stand.tg() == ""
    assert (stand.state / f"clubelo-auto-deliver-attempted-{DAY}").exists()


@pytest.mark.parametrize("shared", ["dags/utils/config.py", "dags/utils/alerts.py", "scrapers/base/base_scraper.py"])
def test_shared_module_drift_cancels(stand: Stand, shared: str) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    (stand.tree / shared).write_text("# живая правка общего модуля\n", encoding="utf-8")
    res = stand.run("--check")
    assert res.returncode == 1
    assert f"ОТМЕНА: общий модуль {shared} в бою ≠ master" in res.stdout
    assert stand.tg() == ""
    res = stand.run()
    assert res.returncode == 1
    assert f"ОТМЕНА: общий модуль {shared} в бою ≠ master" in stand.log()
    assert f"общий модуль {shared}" in stand.tg()
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("kind", ["extra_in_tree", "deleted_in_master"])
def test_shared_module_composition_drift_cancels(stand: Stand, kind: str) -> None:
    if kind == "extra_in_tree":
        stand.master({MAIN: "VERSION = 2\n"})
        (stand.tree / "scrapers/utils/stray.py").write_text("X = 1\n", encoding="utf-8")
        name = "scrapers/utils/stray.py"
    else:
        stand.master({MAIN: "VERSION = 2\n", "scrapers/utils/retry_policy.py": None})
        name = "scrapers/utils/retry_policy.py"
    (stand.tree / "scrapers/utils/__pycache__").mkdir()
    (stand.tree / "scrapers/utils/__pycache__/x.cpython-311.pyc").write_bytes(b"\0")
    res = stand.run("--check")
    assert res.returncode == 1
    assert f"ОТМЕНА: общий модуль {name} в бою ≠ master" in res.stdout
    res = stand.run()
    assert res.returncode == 1
    assert f"общий модуль {name}" in stand.tg()
    assert stand.tree_text(MAIN) == "VERSION = 1\n"


def test_delivers_in_place_creates_new_module_and_rolls_back_by_hand(stand: Stand) -> None:
    sha = stand.master({
        MAIN: "VERSION = 2\n",
        "scrapers/clubelo/extra.py": "EXTRA = 1\n",
        "dags/dag_ingest_clubelo.py": "INGEST = 2\n",
    })
    inode = (stand.tree / MAIN).stat().st_ino
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 2\n"
    assert (stand.tree / MAIN).stat().st_ino == inode
    assert stand.tree_text("scrapers/clubelo/extra.py") == "EXTRA = 1\n"
    assert stand.tree_text("dags/dag_ingest_clubelo.py") == "INGEST = 2\n"
    bk = stand.out / DAY
    assert (bk / f"{MAIN}.prev-{DAY}").read_text(encoding="utf-8") == "VERSION = 1\n"
    assert (bk / f"scrapers/clubelo/extra.py.absent-{DAY}").exists()
    # Порядок записи: модули ClubElo раньше DAG.
    assert (bk / "files").read_text(encoding="utf-8").split("\n")[:3] == [
        f"M {MAIN}", "A scrapers/clubelo/extra.py", "M dags/dag_ingest_clubelo.py"]
    assert stand.accepted() == sha
    assert not (stand.state / "clubelo-inflight").exists()
    assert "доставлено" in (stand.out / "journal.log").read_text(encoding="utf-8")
    assert f"✅ ClubElo: доставлено 3 файлов из {sha[:7]}" in stand.tg()
    # Метка приёмки взята после записи; приёмка смотрит DAG с запасом на идущий разбор.
    assert (stand.ss / "now_snapshots").read_text(encoding="utf-8").splitlines()[0] == "VERSION = 2"
    assert set((stand.ss / "dags_queried").read_text(encoding="utf-8").split()) == {"dag_id='dag_ingest_clubelo'"}
    assert "interval '60 seconds'" in (stand.ss / "docker.log").read_text(encoding="utf-8")

    (stand.ss / "now").write_text("2026-09-24 09:30:00", encoding="utf-8")
    res = stand.run("--rollback", DAY)
    assert res.returncode == 1 and "вне окна" in res.stdout
    assert stand.tree_text(MAIN) == "VERSION = 2\n"

    (stand.ss / "now").write_text(NIGHT, encoding="utf-8")
    res = stand.run("--rollback", DAY)
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.tree_text("dags/dag_ingest_clubelo.py") == "INGEST = 1\n"
    assert not (stand.tree / "scrapers/clubelo/extra.py").exists()
    assert stand.accepted() == stand.base


def test_deletion_in_own_dirs_is_delivered_and_rolled_back(stand: Stand) -> None:
    # Как первая доставка #1465: scraper.py и его тест уходят, новые модули и тесты приходят.
    sha = stand.master({
        MAIN: "VERSION = 2\n",
        "scrapers/clubelo/__init__.py": "# без scraper\n",
        "scrapers/clubelo/scraper.py": None,
        "tests/unit/scrapers/test_clubelo_scraper.py": None,
        "tests/unit/scrapers/test_clubelo_daily.py": "def test_daily(): pass\n",
        "tests/fixtures/clubelo/20260924/Ranking.html.gz": "gz\n",
    })
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (stand.tree / "scrapers/clubelo/scraper.py").exists()
    assert not (stand.tree / "tests/unit/scrapers/test_clubelo_scraper.py").exists()
    assert stand.tree_text("tests/fixtures/clubelo/20260924/Ranking.html.gz") == "gz\n"
    files = (stand.out / DAY / "files").read_text(encoding="utf-8").splitlines()
    # Удаления — последними, после __init__ и тестов.
    assert files == [
        f"M {MAIN}", "M scrapers/clubelo/__init__.py",
        "A tests/fixtures/clubelo/20260924/Ranking.html.gz", "A tests/unit/scrapers/test_clubelo_daily.py",
        "D scrapers/clubelo/scraper.py", "D tests/unit/scrapers/test_clubelo_scraper.py"]
    assert stand.accepted() == sha
    assert "доставлено 6 файлов" in stand.tg()

    res = stand.run("--rollback", DAY)
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text("scrapers/clubelo/scraper.py") == "OLD = 1\n"
    assert stand.tree_text("tests/unit/scrapers/test_clubelo_scraper.py") == "def test_old(): pass\n"
    assert stand.tree_text("scrapers/clubelo/__init__.py") == ""
    assert not (stand.tree / "tests/unit/scrapers/test_clubelo_daily.py").exists()
    assert not (stand.tree / "tests/fixtures/clubelo/20260924/Ranking.html.gz").exists()
    assert stand.accepted() == stand.base


def test_failed_acceptance_restores_deleted_file(stand: Stand) -> None:
    stand.master({MAIN: "VERSION = 2  # BROKEN\n", "scrapers/clubelo/scraper.py": None})
    res = stand.run()
    assert res.returncode == 1, res.stdout + res.stderr
    assert stand.tree_text("scrapers/clubelo/scraper.py") == "OLD = 1\n"
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert "🔴 ClubElo: доставка" in stand.tg() and "откачено" in stand.tg()
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("kind", ["add_in_dags", "delete_in_dags", "delete_top_scrapers"])
def test_structural_change_needs_hands(stand: Stand, kind: str) -> None:
    if kind == "add_in_dags":
        stand.master({"dags/utils/clubelo_new.py": "X = 1\n", "dags/dag_ingest_clubelo.py": "INGEST = 2\n"})
    elif kind == "delete_in_dags":
        stand.master({"dags/scripts/run_clubelo_scraper.py": None, "dags/dag_ingest_clubelo.py": "INGEST = 2\n"})
    else:
        stand.base_extra = stand.master({"scrapers/clubelo_legacy.py": "L = 1\n"})
        (stand.state / "clubelo-accepted").write_text(stand.base_extra + "\n", encoding="utf-8")
        _git(stand.tree, "fetch", "-q", "origin")
        _git(stand.tree, "checkout", "-q", "--detach", stand.base_extra)
        stand.base = stand.base_extra
        stand.master({"scrapers/clubelo_legacy.py": None, "dags/dag_ingest_clubelo.py": "INGEST = 2\n"})
    res = stand.run()
    assert res.returncode == 1
    assert "нужны руки: сторож каталогов" in stand.log()
    assert "сторож каталогов" in stand.tg()
    assert stand.tree_text("dags/dag_ingest_clubelo.py") == "INGEST = 1\n"
    assert (stand.tree / "dags/scripts/run_clubelo_scraper.py").exists()
    assert stand.accepted() == stand.base


def test_rename_in_own_dir_is_delete_plus_add(stand: Stand) -> None:
    sha = stand.master({}, renames={MAIN: "scrapers/clubelo/daily2.py"})
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (stand.tree / MAIN).exists()
    assert stand.tree_text("scrapers/clubelo/daily2.py") == "VERSION = 1\n"
    assert stand.accepted() == sha


@pytest.mark.parametrize("stray", [None, "scrapers/clubelo/stray.py", "dags/utils/clubelo_stray.py",
                                   "tests/unit/scrapers/test_clubelo_stray.py"])
def test_foreign_live_edit_stops(stand: Stand, stray: str | None) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    if stray is None:
        (stand.tree / MAIN).write_text("VERSION = 'живая правка'\n", encoding="utf-8")
        expected = f"чужая живая правка: {MAIN}"
    else:
        (stand.tree / stray).write_text("X = 1\n", encoding="utf-8")
        expected = f"лишние файлы ClubElo в бою: {stray}"
    (stand.tree / "scrapers/clubelo/__pycache__").mkdir()
    (stand.tree / "scrapers/clubelo/__pycache__/daily.cpython-311.pyc").write_bytes(b"\0")
    (stand.tree / "tests/unit/scrapers/__pycache__").mkdir()
    (stand.tree / "tests/unit/scrapers/__pycache__/test_clubelo_scraper.cpython-312.pyc").write_bytes(b"\0")
    res = stand.run()
    assert res.returncode == 1
    assert expected in stand.log()
    assert stand.tree_text(MAIN) != "VERSION = 2\n"
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("cause", ["BROKEN", "IMPORTERR"])
def test_failed_acceptance_rolls_back(stand: Stand, cause: str) -> None:
    stand.master({
        MAIN: f"VERSION = 2  # {cause}\n",
        "scrapers/clubelo/extra.py": "EXTRA = 1\n",
    })
    res = stand.run()
    assert res.returncode == 1, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert not (stand.tree / "scrapers/clubelo/extra.py").exists()
    assert "🔴 ClubElo: доставка" in stand.tg() and "откачено" in stand.tg()
    assert stand.accepted() == stand.base
    assert not (stand.state / "clubelo-inflight").exists()
    assert not (stand.state / "clubelo-auto-deliver.off").exists()
    assert (stand.state / f"clubelo-auto-deliver-attempted-{DAY}").exists()


def test_unconfirmed_rollback_switches_off(stand: Stand) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    (stand.ss / "stale").write_text("", encoding="utf-8")
    res = stand.run()
    assert res.returncode == 2, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert "🆘" in stand.tg() and "НУЖНЫ РУКИ" in stand.tg()
    assert (stand.state / "clubelo-auto-deliver.off").exists()
    assert (stand.state / "clubelo-inflight").exists()
    assert stand.accepted() == stand.base
    (stand.state / f"clubelo-auto-deliver-attempted-{DAY}").unlink()
    res = stand.run()
    assert res.returncode == 0 and "выключатель" in res.stdout


def test_unconfirmed_manual_rollback_switches_off(stand: Stand) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    assert stand.run().returncode == 0
    (stand.ss / "stale").write_text("", encoding="utf-8")
    res = stand.run("--rollback", DAY)
    assert res.returncode == 2, res.stdout + res.stderr
    assert "🆘 ClubElo: ручной откат" in stand.tg()
    assert (stand.state / "clubelo-auto-deliver.off").exists()
    assert (stand.state / "clubelo-inflight").read_text(encoding="utf-8").strip() == f"rollback {DAY}"


@pytest.mark.parametrize("why, now", [("window", "2026-09-24 09:30:00"), ("window", "2026-09-24 01:29:00"),
                                      ("window", "2026-09-24 02:25:00"), ("busy", NIGHT)])
def test_outside_window_or_busy_does_nothing(stand: Stand, why: str, now: str) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    (stand.ss / "now").write_text(now, encoding="utf-8")
    if why == "busy":
        (stand.ss / "busy").write_text("1\n", encoding="utf-8")
    res = stand.run()
    assert res.returncode == 0
    assert ("вне окна" if why == "window" else "ClubElo занят") in stand.log()
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert not (stand.state / f"clubelo-auto-deliver-attempted-{DAY}").exists()
    assert stand.tg() == ""


def test_check_writes_nothing(stand: Stand) -> None:
    stand.master({MAIN: "VERSION = 2\n", "scrapers/clubelo/extra.py": "E = 1\n",
                  "scrapers/clubelo/scraper.py": None})
    res = stand.run("--check")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "к доставке (3)" in res.stdout and "проверки пройдены" in res.stdout
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert not (stand.tree / "scrapers/clubelo/extra.py").exists()
    assert (stand.tree / "scrapers/clubelo/scraper.py").exists()
    assert not (stand.out / DAY).exists()
    assert stand.accepted() == stand.base
    assert not (stand.state / f"clubelo-auto-deliver-attempted-{DAY}").exists()
    assert not (stand.state / "clubelo-inflight").exists()
    assert stand.tg() == ""


@pytest.mark.parametrize("tree_alerts, ok", [(LAG_BODY, True), ("def telegram_on_failure(ctx): 3\n", False)])
def test_allowed_lag_of_a_shared_module(stand: Stand, tree_alerts: str, ok: bool) -> None:
    # master ушёл вперёд по alerts.py; бой = разрешённая версия → доставка с записью в журнал,
    # бой = третья версия → отмена, как раньше
    (stand.tree / "dags/utils/alerts.py").write_text(tree_alerts, encoding="utf-8")
    sha = stand.master({MAIN: "VERSION = 2\n",
                        "dags/utils/alerts.py": "def telegram_on_failure(ctx): pass  # после #1477\n"})
    res = stand.run()
    if ok:
        assert res.returncode == 0, res.stdout + res.stderr
        assert stand.tree_text(MAIN) == "VERSION = 2\n" and stand.accepted() == sha
        assert f"общий модуль отстаёт (разрешено): dags/utils/alerts.py={LAG_BLOB[:8]}" in stand.log()
        assert f"общий модуль отстаёт (разрешено): dags/utils/alerts.py={LAG_BLOB[:8]}" in (
            stand.out / "journal.log").read_text(encoding="utf-8")
        assert stand.tree_text("dags/utils/alerts.py") == LAG_BODY   # общий модуль не тронут
    else:
        assert res.returncode == 1
        assert "ОТМЕНА: общий модуль dags/utils/alerts.py в бою ≠ master" in stand.log()
        assert stand.tree_text(MAIN) == "VERSION = 1\n" and stand.accepted() == stand.base


def test_tree_equal_to_master_logs_no_lag(stand: Stand) -> None:
    stand.master({MAIN: "VERSION = 2\n"})
    assert stand.run().returncode == 0
    assert "отстаёт" not in stand.log() and stand.tree_text(MAIN) == "VERSION = 2\n"


def test_allowed_lag_does_not_cover_other_shared_files(stand: Stand) -> None:
    # blob разрешён только для alerts.py: тот же текст в другом общем модуле — отмена
    stand.master({MAIN: "VERSION = 2\n", "dags/utils/config.py": "SCHEDULES = {1: 1}\n"})
    (stand.tree / "dags/utils/config.py").write_text(LAG_BODY, encoding="utf-8")
    res = stand.run()
    assert res.returncode == 1 and "ОТМЕНА: общий модуль dags/utils/config.py в бою ≠ master" in stand.log()


@pytest.mark.parametrize("lagged", [
    tuple(CONFIG_LAG_BODIES), ("dags/utils/config.py",),
    ("dags/utils/medallion_config.py",), (),
])
def test_fotmob_config_lag_delivers_without_changing_shared_files(stand: Stand, lagged) -> None:
    for path in lagged:
        (stand.tree / path).write_text(CONFIG_LAG_BODIES[path], encoding="utf-8")
    (stand.tree / "dags/utils/alerts.py").write_text(LAG_BODY, encoding="utf-8")
    before = {p: stand.tree_text(p) for p in LAG_BLOBS}
    sha = stand.master({MAIN: "VERSION = 2\n"})
    checked = stand.run("--check")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.accepted() == stand.base
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 2\n"
    assert stand.accepted() == sha
    assert {p: stand.tree_text(p) for p in LAG_BLOBS} == before
    for path in lagged:
        assert f"{path}={LAG_BLOBS[path][:8]}" in stand.log()


@pytest.mark.parametrize("changed", CONFIG_LAG_BODIES)
def test_fotmob_config_third_version_still_blocks(stand: Stand, changed: str) -> None:
    for path, body in CONFIG_LAG_BODIES.items():
        (stand.tree / path).write_text(body, encoding="utf-8")
    with (stand.tree / changed).open("a", encoding="utf-8") as handle:
        handle.write("# unreviewed production edit\n")
    stand.master({MAIN: "VERSION = 2\n"})
    res = stand.run()
    assert res.returncode == 1, res.stdout + res.stderr
    assert f"ОТМЕНА: общий модуль {changed}" in stand.log()
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.accepted() == stand.base
