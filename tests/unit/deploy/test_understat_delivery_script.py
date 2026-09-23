"""Автомат ночной доставки Understat против заглушек (#1432).

Настоящий `deploy/understat/auto_deliver.sh` гоняется целиком на «установленной» копии с
ЕДИНСТВЕННОЙ правкой — фиксированный PATH автомата получает впереди каталог заглушек
(`docker`, `date`, `curl`, `pgrep`, `sleep`). Та же копия лежит в тестовом репозитории:
автомат сверяет себя с master по md5.

Git — настоящий: репозиторий создаётся `git init -b master`, коммит 1 — «бой», коммит 2 —
master с правками сценария; боевое дерево — клон на коммите 1, канонический клон — клон с
origin. Заглушка метабазы отвечает по подстрокам SQL; флаги разбора DAG она выводит из
содержимого боевого дерева: файл с маркером BROKEN «не импортируется» (`t|f`), как в бою.
Ни боевое дерево, ни docker, ни сеть тесты не трогают.
"""

from __future__ import annotations

from pathlib import Path
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
AUTO = ROOT / "deploy" / "understat" / "auto_deliver.sh"
PATH_LINE = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
NIGHT = "2026-09-24 01:10:00"
DAY = "20260924"

BASE_FILES = {
    "scrapers/__init__.py": "",
    "scrapers/base/base_scraper.py": "BASE = 1\n",
    "scrapers/utils/__init__.py": "",
    "dags/utils/__init__.py": "",
    "dags/utils/config.py": "SCHEDULES = {}\n",
    "dags/utils/default_args.py": "DEFAULT_ARGS = {}\n",
    "scrapers/understat/__init__.py": "",
    "scrapers/understat/client.py": "VERSION = 1\n",
    "dags/utils/understat_tasks.py": "TASKS = 1\n",
    "dags/scripts/run_understat_scraper.py": "RUN = 1\n",
    "dags/dag_backfill_understat.py": "BACKFILL = 1\n",
    "dags/dag_ingest_understat.py": "INGEST = 1\n",
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
        self.installed = tmp / "understat-auto-deliver.sh"
        self.installed.write_text(self.installed_text, encoding="utf-8")
        self.installed.chmod(0o755)
        self.source = tmp / "source"
        self.source.mkdir()
        _git(self.source, "init", "-q", "-b", "master")
        for rel, body in BASE_FILES.items():
            self.write(rel, body)
        self.write("deploy/understat/auto_deliver.sh", self.installed_text)
        _git(self.source, "add", "-A")
        _git(self.source, "commit", "-q", "-m", "бой")
        self.base = _git(self.source, "rev-parse", "HEAD")
        self.tree = tmp / "tree"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.tree)], check=True)
        _git(self.tree, "checkout", "-q", "--detach", self.base)
        self.repo = tmp / "canon"
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.repo)], check=True)
        self.state.mkdir()
        (self.state / "understat-accepted").write_text(self.base + "\n", encoding="utf-8")
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
  *has_import_errors*) if grep -rqs BROKEN {tree}/scrapers/understat {tree}/dags; then echo "t|f"; else echo "f|t"; fi ;;
  *import_error*) echo 0 ;;
  *task_instance*) cat {ss}/busy 2>/dev/null || echo 0 ;;
  *"now()"*) echo "2026-09-24 01:10:05+00" ;;
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
        return (self.state / "understat-accepted").read_text(encoding="utf-8").strip()

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
    sha = stand.master({"README.md": "не Understat\n"})
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.accepted() == sha
    assert "нечего доставлять" in stand.log()
    assert not (stand.out / DAY).exists()
    assert stand.tg() == ""
    assert (stand.state / f"understat-auto-deliver-attempted-{DAY}").exists()


def test_shared_module_drift_cancels(stand: Stand) -> None:
    stand.master({"scrapers/understat/client.py": "VERSION = 2\n"})
    (stand.tree / "dags/utils/config.py").write_text("SCHEDULES = {'x': 1}\n", encoding="utf-8")
    res = stand.run()
    assert res.returncode == 1
    assert "ОТМЕНА: общий модуль dags/utils/config.py в бою ≠ master" in stand.log()
    assert "общий модуль dags/utils/config.py" in stand.tg()
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 1\n"
    assert stand.accepted() == stand.base


def test_delivers_in_place_creates_new_module_and_rolls_back_by_hand(stand: Stand) -> None:
    sha = stand.master({
        "scrapers/understat/client.py": "VERSION = 2\n",
        "scrapers/understat/extra.py": "EXTRA = 1\n",
        "dags/dag_ingest_understat.py": "INGEST = 2\n",
    })
    inode = (stand.tree / "scrapers/understat/client.py").stat().st_ino
    res = stand.run()
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 2\n"
    assert (stand.tree / "scrapers/understat/client.py").stat().st_ino == inode
    assert stand.tree_text("scrapers/understat/extra.py") == "EXTRA = 1\n"
    assert stand.tree_text("dags/dag_ingest_understat.py") == "INGEST = 2\n"
    bk = stand.out / DAY
    assert (bk / f"scrapers/understat/client.py.prev-{DAY}").read_text(encoding="utf-8") == "VERSION = 1\n"
    assert (bk / f"scrapers/understat/extra.py.absent-{DAY}").exists()
    # Порядок записи: модули Understat раньше DAG.
    assert (bk / "files").read_text(encoding="utf-8").split("\n")[:3] == [
        "M scrapers/understat/client.py", "A scrapers/understat/extra.py", "M dags/dag_ingest_understat.py"]
    assert stand.accepted() == sha
    assert not (stand.state / "understat-inflight").exists()
    assert "доставлено" in (stand.out / "journal.log").read_text(encoding="utf-8")
    assert f"доставлено 3 файлов из {sha[:7]}" in stand.tg()

    res = stand.run("--rollback", DAY)
    assert res.returncode == 0, res.stdout + res.stderr
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 1\n"
    assert stand.tree_text("dags/dag_ingest_understat.py") == "INGEST = 1\n"
    assert not (stand.tree / "scrapers/understat/extra.py").exists()
    assert stand.accepted() == stand.base


@pytest.mark.parametrize("kind", ["add_in_dags", "delete", "rename"])
def test_structural_change_needs_hands(stand: Stand, kind: str) -> None:
    if kind == "add_in_dags":
        stand.master({"dags/utils/understat_new.py": "X = 1\n", "dags/dag_ingest_understat.py": "INGEST = 2\n"})
    elif kind == "delete":
        stand.master({"scrapers/understat/client.py": None})
    else:
        stand.master({}, renames={"scrapers/understat/client.py": "scrapers/understat/client2.py"})
    res = stand.run()
    assert res.returncode == 1
    assert "нужны руки: сторож каталогов" in stand.log()
    assert "сторож каталогов" in stand.tg()
    assert stand.tree_text("dags/dag_ingest_understat.py") == "INGEST = 1\n"
    assert (stand.tree / "scrapers/understat/client.py").exists()
    assert stand.accepted() == stand.base


def test_foreign_live_edit_stops(stand: Stand) -> None:
    stand.master({"scrapers/understat/client.py": "VERSION = 2\n"})
    (stand.tree / "scrapers/understat/client.py").write_text("VERSION = 'живая правка'\n", encoding="utf-8")
    res = stand.run()
    assert res.returncode == 1
    assert "чужая живая правка: scrapers/understat/client.py" in stand.log()
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 'живая правка'\n"
    assert stand.accepted() == stand.base


def test_failed_acceptance_rolls_back(stand: Stand) -> None:
    stand.master({
        "scrapers/understat/client.py": "VERSION = 2  # BROKEN\n",
        "scrapers/understat/extra.py": "EXTRA = 1\n",
    })
    res = stand.run()
    assert res.returncode == 1, res.stdout + res.stderr
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 1\n"
    assert not (stand.tree / "scrapers/understat/extra.py").exists()
    assert "🔴 Understat: доставка" in stand.tg() and "откачено" in stand.tg()
    assert stand.accepted() == stand.base
    assert not (stand.state / "understat-inflight").exists()
    assert not (stand.state / "understat-auto-deliver.off").exists()
    assert (stand.state / f"understat-auto-deliver-attempted-{DAY}").exists()


def test_outside_window_does_nothing(stand: Stand) -> None:
    stand.master({"scrapers/understat/client.py": "VERSION = 2\n"})
    (stand.ss / "now").write_text("2026-09-24 09:30:00", encoding="utf-8")
    res = stand.run()
    assert res.returncode == 0
    assert "вне окна" in stand.log()
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 1\n"
    assert not (stand.state / f"understat-auto-deliver-attempted-{DAY}").exists()
    assert stand.tg() == ""


def test_check_writes_nothing(stand: Stand) -> None:
    stand.master({"scrapers/understat/client.py": "VERSION = 2\n", "scrapers/understat/extra.py": "E = 1\n"})
    res = stand.run("--check")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "к доставке (2)" in res.stdout and "проверки пройдены" in res.stdout
    assert stand.tree_text("scrapers/understat/client.py") == "VERSION = 1\n"
    assert not (stand.tree / "scrapers/understat/extra.py").exists()
    assert not (stand.out / DAY).exists()
    assert stand.accepted() == stand.base
    assert not (stand.state / f"understat-auto-deliver-attempted-{DAY}").exists()
    assert not (stand.state / "understat-inflight").exists()
    assert stand.tg() == ""
