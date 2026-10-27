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
BASE_FILES = {
    "scrapers/__init__.py": "",
    "scrapers/base/base_scraper.py": "BASE = 1\n",
    "scrapers/base/iceberg_writer.py": "class IcebergWriter: pass\n",
    "scrapers/base/flaresolverr_client.py": "class FlareSolverrClient:\n"
        "    def __init__(self, url, default_timeout, default_max_timeout_ms): pass\n"
        "    def create_session(self, session_id, proxy_url=None): pass\n"
        "    def get(self, url, session_id): pass\n"
        "    def destroy_session(self, session_id): pass\n"
        "    def close(self): pass\n",
    "scrapers/clubelo/history.py": "HISTORY = 1\n",
    "scrapers/utils/__init__.py": "",
    "dags/utils/__init__.py": "",
    "dags/utils/config.py": "SCHEDULES = {}\n",
    "dags/utils/default_args.py": "DEFAULT_ARGS = {}\n",
    "dags/utils/alerts.py": "def telegram_on_failure(ctx): pass\ndef send_telegram_message(text, level='info'): pass\n",
    "dags/utils/medallion_config.py": "FLOOR = 1\n",
    "scrapers/utils/retry_policy.py": "RETRY = 1\n",
    "scrapers/clubelo/__init__.py": "",
    MAIN: "VERSION = 1\n",
    "scrapers/clubelo/scraper.py": "OLD = 1\n",
    "dags/utils/clubelo_tasks.py": "TASKS = 1\n",
    "dags/scripts/run_clubelo_scraper.py": "RUN = 1\n",
    "dags/dag_ingest_clubelo.py": "INGEST = 1\n",
    "dags/dag_other.py": "OTHER = 1\n",
    "tests/unit/scrapers/test_clubelo_scraper.py": "from scrapers.base.base_scraper import BASE\nfrom utils.config import SCHEDULES\n"
        "from utils.alerts import send_telegram_message\n"
        "def test_old():\n    assert BASE == 1 and isinstance(SCHEDULES, dict)\n    send_telegram_message('test')\n",
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
        self.installed = tmp / "clubelo-auto-deliver.sh"
        self.installed.write_text(self.installed_text, encoding="utf-8")
        self.installed.chmod(0o755)
        self.source = tmp / "source"
        self.source.mkdir()
        _git(self.source, "init", "-q", "-b", "master")
        for rel, body in BASE_FILES.items():
            self.write(rel, body)
        self.write("deploy/clubelo/auto_deliver.sh", self.installed_text)
        self.write("deploy/clubelo/check_compatibility.py",
                   (AUTO.parent / "check_compatibility.py").read_text())
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


@pytest.mark.parametrize("kind", ["symbol", "call", "missing", "environment", "no_tests", "master_fallback", "writer_api", "unreadable"])
def test_incompatible_shared_cancels_before_writes(stand: Stand, kind: str) -> None:
    changes = {MAIN: "VERSION = 2\n"}
    if kind == "symbol":
        (stand.tree / "scrapers/base/base_scraper.py").write_text("OTHER = 1\n")
    elif kind == "call":
        (stand.tree / "dags/utils/alerts.py").write_text("def send_telegram_message(text, required): pass\ndef telegram_on_failure(ctx): pass\n")
        changes[MAIN] = "from utils.alerts import send_telegram_message\ndef notify(text): send_telegram_message(text, level='error')\n"
    elif kind == "missing":
        (stand.tree / "scrapers/base/iceberg_writer.py").unlink()
    elif kind == "environment":
        (stand.tree / "scrapers/base/iceberg_writer.py").write_text("import missing_dependency_xyz\n")
    elif kind == "writer_api":
        changes[MAIN] = "class Store:\n    def read(self): return self.writer.read_table('db', 'table', columns=['slug'])\n"
    elif kind == "unreadable":
        (stand.tree / "scrapers/base/linked.py").symlink_to("base_scraper.py")
    elif kind == "no_tests":
        changes["tests/unit/scrapers/test_clubelo_scraper.py"] = None
    else:
        changes["scrapers/base/only_master.py"] = "VALUE = 1\n"
        changes[MAIN] = "from scrapers.base.only_master import VALUE\n"
    stand.master(changes)
    for args in [("--check",), ()]:
        res = stand.run(*args)
        assert res.returncode == 1, res.stdout + res.stderr
        assert "ОТМЕНА: совместимость" in res.stdout
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.accepted() == stand.base
    assert not (stand.state / "clubelo-inflight").exists()


@pytest.mark.parametrize("method", [None, "__init__", "create_session", "get", "destroy_session", "close"])
def test_solver_api_checked_before_writes(stand: Stand, method: str | None) -> None:
    path = "scrapers/base/flaresolverr_client.py"
    body = BASE_FILES[path]
    if method is not None:
        import re
        # create_session keeps its positional API but loses proxy_url.
        signature = "self, session_id" if method == "create_session" else "self, *, incompatible"
        body = re.sub(rf"def {method}\([^)]*\)", f"def {method}({signature})", body)
    (stand.tree / path).write_text(body)
    sha = stand.master({MAIN: "VERSION = 2\n", "scrapers/clubelo/transport.py":
                        (ROOT / "scrapers/clubelo/transport.py").read_text()})
    for args in [("--check",), ()]:
        res = stand.run(*args)
        assert res.returncode == (1 if method else 0), res.stdout + res.stderr
        if method:
            assert "ОТМЕНА: совместимость" in res.stdout
    assert stand.tree_text(MAIN) == ("VERSION = 1\n" if method else "VERSION = 2\n")
    assert stand.accepted() == (stand.base if method else sha)
    assert stand.tree_text(path) == body
    assert not (stand.state / "clubelo-inflight").exists()


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
    assert stand.tree_text("tests/unit/scrapers/test_clubelo_scraper.py") == BASE_FILES["tests/unit/scrapers/test_clubelo_scraper.py"]
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
    sha = stand.master({}, renames={"scrapers/clubelo/scraper.py": "scrapers/clubelo/legacy.py"})
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (stand.tree / "scrapers/clubelo/scraper.py").exists()
    assert stand.tree_text("scrapers/clubelo/legacy.py") == "OLD = 1\n"
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


def test_compatible_shared_drift_delivers_without_shared_writes(stand: Stand) -> None:
    before = {}
    for name in ["dags/utils/config.py", "dags/utils/alerts.py", "scrapers/base/base_scraper.py"]:
        path = stand.tree / name
        path.write_text(path.read_text() + "# arbitrary compatible version\n")
        before[name] = path.read_bytes()
    (stand.tree / "scrapers/base/unused.py").write_text("UNUSED = True\n")
    sha = stand.master({MAIN: "VERSION = 2\n", "scrapers/utils/retry_policy.py": None})
    checked = stand.run("--check")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.accepted() == sha
    assert all((stand.tree / name).read_bytes() == body for name, body in before.items())
    assert (stand.tree / "scrapers/base/unused.py").exists()
    assert (stand.tree / "scrapers/utils/retry_policy.py").exists()


@pytest.mark.parametrize("rollback", [False, True])
def test_mixed_base_target_delivers_and_restores_snapshot(stand: Stand, rollback: bool) -> None:
    extra = "scrapers/clubelo/new.py"
    sha = stand.master({MAIN: "VERSION = 2\n", extra: "NEW = 1\n",
                        "dags/dag_ingest_clubelo.py": "INGEST = 2" + (" # BROKEN\n" if rollback else "\n")})
    (stand.tree / MAIN).write_text("VERSION = 2\n")
    (stand.tree / extra).write_text("NEW = 1\n")
    res = stand.run()
    assert res.returncode == (1 if rollback else 0), res.stdout + res.stderr
    if rollback:
        assert "откачено" in stand.tg()
        assert not (stand.state / "clubelo-auto-deliver.off").exists()
        assert stand.accepted() == stand.base
    else:
        assert stand.accepted() == sha
        res = stand.run("--rollback", DAY)
        assert res.returncode == 0, res.stdout + res.stderr
        assert stand.accepted() == stand.base
    assert stand.tree_text(MAIN) == "VERSION = 2\n"
    assert stand.tree_text(extra) == "NEW = 1\n"
    assert stand.tree_text("dags/dag_ingest_clubelo.py") == "INGEST = 1\n"


@pytest.mark.parametrize("name", [MAIN, "scrapers/clubelo/new.py"])
def test_mixed_unrecognized_bytes_still_stop(stand: Stand, name: str) -> None:
    stand.master({MAIN: "VERSION = 2\n", "scrapers/clubelo/new.py": "NEW = 1\n"})
    (stand.tree / name).write_text("FOREIGN = 1\n")
    res = stand.run()
    assert res.returncode == 1 and "чужая живая правка" in res.stdout
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("change", ["shared", "window", "busy"])
def test_rechecks_after_compatibility(stand: Stand, change: str) -> None:
    target = {"shared": stand.tree / "dags/utils/config.py", "window": stand.ss / "now",
              "busy": stand.ss / "busy"}[change]
    value = {"shared": "SCHEDULES = {} # changed", "window": "2026-09-24 02:25:00", "busy": "1"}[change]
    stand.master({MAIN: "VERSION = 2\n", "tests/unit/scrapers/test_clubelo_scraper.py":
                  f"from pathlib import Path\ndef test_mutate(): Path({str(target)!r}).write_text({value!r})\n"})
    res = stand.run()
    assert res.returncode == 1 and "ОТМЕНА:" in res.stdout
    assert stand.tree_text(MAIN) == "VERSION = 1\n"
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("day", ["2026-07-15", "2026-12-15", "2026-03-29", "2026-10-25"])
def test_documented_cron_has_one_attempt_across_dst(stand: Stand, day: str) -> None:
    from datetime import datetime, timedelta, timezone
    import re
    from zoneinfo import ZoneInfo
    cron = re.findall(r"^40 (\*|[23]) \* \* \* /root/clubelo-auto-deliver.sh", (AUTO.parent / "README.md").read_text(), re.M)
    assert cron
    stand.master({MAIN: "VERSION = 2\n"})
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    # Debian cron suppresses fixed-hour jobs in the repeated autumn hour;
    # wildcard-hour jobs follow the new wall clock, including that hour.
    for minute in range(240):
        utc = start + timedelta(minutes=minute)
        local = utc.astimezone(ZoneInfo("Europe/Berlin"))
        scheduled = "*" in cron or (str(local.hour) in cron and not local.fold)
        if local.minute == 40 and scheduled:
            (stand.ss / "now").write_text(utc.strftime("%Y-%m-%d %H:%M:%S"))
            res = stand.run()
            assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text(MAIN) == "VERSION = 2\n"
    assert len(list(stand.state.glob("clubelo-auto-deliver-attempted-*"))) == 1
    assert stand.tg().count("sendMessage") == 1
