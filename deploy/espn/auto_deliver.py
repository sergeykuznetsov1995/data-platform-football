#!/usr/bin/env python3
"""Автодоставка нового контура ESPN (#1507): master → проект espn-live.

Хост-копия /root/espn-deploy/auto_deliver.py, cron */5. Шаг:
  самопроверка (хост-копия = master) → fetch → цель: master, потомок принятого SHA, не
  отклонённый, diff задевает пути контура → окно (нет running/queued dag_espn_current, до волны
  00/06/12/18 UTC ≥ 10 мин) → git archive → /root/espn-release-<sha> (ro) → [airflow-init при
  смене pools.json] → up -d --no-deps --force-recreate scheduler/webserver → приёмка по metadb,
  байтам в контейнере, пулам и env → успех: пин; провал: тот же up на принятом, SHA → rejected.

Единственный пин — state/accepted (живой SHA); корень релиза выводится из него, в env-файле
его нет. Режимы: без аргументов (cron) | --target <sha> | --rollback | --seed <sha>.
Выключатель — файл state/off. README «Доставка» — посев, откат, выключатель.
"""
from __future__ import annotations

import argparse
import ast
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT = "espn-live"
SELF_REL = "deploy/espn/auto_deliver.py"
COMPOSE_REL = "deploy/espn/airflow.compose.yaml"
POOLS_REL = "deploy/espn/pools.json"
DAG_DIR_REL = "deploy/espn/dags"
DAG_ID = "dag_espn_current"
METADB = f"{PROJECT}-airflow-metadb-1"
SCHEDULER = f"{PROJECT}-airflow-scheduler-1"
SERVICES = ("airflow-scheduler", "airflow-webserver")
LABEL = "espn.release_root"
# Что попадает в корень релиза (всё, что монтирует compose).
ARCHIVE_PATHS = ("deploy/espn", "scrapers", "configs/espn")
# Пути контура сверх замыкания импортов DAG: код доставки и данные, которые читает код.
STATIC_PATHS = ("deploy/espn", "configs/espn")
# Байты в контейнере = корню релиза: путь в корне → путь в контейнере.
BYTE_CHECK = {"deploy/espn/dags": "/opt/airflow/dags", "scrapers/espn": "/opt/airflow/scrapers/espn"}
DAGS_TARGET = "/opt/airflow/dags"
WAVE_HOURS = (0, 6, 12, 18)
WINDOW = timedelta(minutes=10)
ACCEPT_TIMEOUT_S = 420
ACCEPT_POLL_S = 20
KEEP_ROOTS = 3
RELEASE_PREFIX = "espn-release-"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass
class Paths:
    repo: Path = Path("/root/data-platform-football")
    deploy_dir: Path = Path("/root/espn-deploy")
    release_parent: Path = Path("/root")
    env_file: Path = Path("/root/.secrets/espn.env")
    tg_env: Path = Path("/root/.claude/telegram.env")
    self_file: Path = field(default_factory=lambda: Path(__file__).resolve())

    @property
    def state(self) -> Path:
        return self.deploy_dir / "state"

    def root(self, sha: str) -> Path:
        return self.release_parent / f"{RELEASE_PREFIX}{sha}"


class Stop(Exception):
    """Доставку не начинаем (или уже откатили): причина уходит в лог и Telegram."""


class Host:
    """Все побочные эффекты: процессы, часы, Telegram. Тесты подменяют."""

    def __init__(self, paths: Paths):
        self.paths = paths

    def run(self, cmd, *, env=None, timeout=900):
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "", f"timeout {timeout}s"
        return p.returncode, p.stdout, p.stderr

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def log(self, msg: str) -> None:
        print(f"{self.now():%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)

    def telegram(self, text: str) -> bool:
        try:
            env = read_env_file(self.paths.tg_env)
            data = urllib.parse.urlencode({"chat_id": env["TELEGRAM_CHAT_ID"], "text": text}).encode()
            url = f"https://api.telegram.org/bot{env['TELEGRAM_BOT_TOKEN']}/sendMessage"
            with urllib.request.urlopen(url, data=data, timeout=15) as resp:
                return json.load(resp).get("ok") is True
        except Exception as exc:  # алерт не должен ронять доставку
            self.log(f"АЛЕРТ НЕ ДОСТАВЛЕН ({type(exc).__name__}): {text}")
            return False


def read_env_file(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            out[key.strip().removeprefix("export ").strip()] = value.strip().strip("'\"")
    return out


def is_proxy_name(name: str) -> bool:
    return name.upper().endswith("_PROXY")


# ---------- состояние ----------

def read_sha(path: Path) -> str | None:
    try:
        value = path.read_text().strip()
    except FileNotFoundError:
        return None
    return value if SHA_RE.match(value) else None


def write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def read_rejected(state: Path) -> list[str]:
    try:
        return [s for s in (state / "rejected").read_text().split() if SHA_RE.match(s)]
    except FileNotFoundError:
        return []


def add_rejected(state: Path, sha: str) -> None:
    if sha not in read_rejected(state):
        with open(state / "rejected", "a") as f:
            f.write(sha + "\n")


def notify_once(h: Host, key: str, text: str) -> None:
    """Затяжная остановка (каждые 5 мин одна и та же) — в Telegram раз в сутки."""
    marker = h.paths.state / "notified"
    stamp = f"{h.now():%Y%m%d} {key}"
    try:
        seen = marker.read_text().splitlines()
    except FileNotFoundError:
        seen = []
    if stamp in seen:
        return
    if h.telegram(text):
        write_atomic(marker, "\n".join([*seen[-50:], stamp]) + "\n")


# ---------- git ----------

def git(h: Host, *args: str) -> str:
    rc, out, err = h.run(["git", "-C", str(h.paths.repo), *args], timeout=300)
    if rc != 0:
        raise Stop(f"git {args[0]} не удался: {err.strip()[:200]}")
    return out


def git_ok(h: Host, *args: str) -> bool:
    return h.run(["git", "-C", str(h.paths.repo), *args], timeout=300)[0] == 0


def git_file(h: Host, sha: str, rel: str) -> str | None:
    rc, out, _ = h.run(["git", "-C", str(h.paths.repo), "show", f"{sha}:{rel}"], timeout=60)
    return out if rc == 0 else None


def import_closure(read, entries) -> set[str]:
    """Файлы репозитория, которые транзитивно импортируют entries (в т.ч. относительные и
    ленивые импорты внутри функций). Не идём внутрь модульного `__getattr__` (PEP 562): он
    отдаёт ленивые экспорты по имени (scrapers/espn/__init__.py → старый ESPNScraper), а
    `from scrapers.espn import wave` его экспорт не трогает. read(rel) → текст или None."""
    def module_file(name: str) -> str | None:
        base = name.replace(".", "/")
        for rel in (f"{base}.py", f"{base}/__init__.py"):
            if read(rel) is not None:
                return rel
        return None

    seen: set[str] = set()
    stack = list(entries)
    while stack:
        rel = stack.pop()
        if rel in seen:
            continue
        seen.add(rel)
        package = rel.rsplit("/", 1)[0].replace("/", ".") if "/" in rel else ""
        tree = ast.parse(read(rel), rel)
        lazy = {id(n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "__getattr__"}
        nodes = [tree]
        while nodes:
            node = nodes.pop()
            nodes.extend(n for n in ast.iter_child_nodes(node) if id(n) not in lazy)
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    parts = package.split(".")
                    parts = parts[: len(parts) - node.level + 1]
                    mod = ".".join([*parts, node.module] if node.module else parts)
                else:
                    mod = node.module or ""
                names = [mod, *(f"{mod}.{a.name}" for a in node.names)]
            for name in names:
                parts = name.split(".")
                for i in range(1, len(parts) + 1):
                    found = module_file(".".join(parts[:i]))
                    if found and found not in seen:
                        stack.append(found)
    return seen


def contour_paths(h: Host, sha: str) -> set[str]:
    """Пути контура в коммите: STATIC_PATHS + замыкание импортов DAG из deploy/espn/dags."""
    cache: dict[str, str | None] = {}

    def read(rel):
        if rel not in cache:
            cache[rel] = git_file(h, sha, rel)
        return cache[rel]

    listing = git(h, "ls-tree", "-r", "--name-only", sha, "--", DAG_DIR_REL).split()
    dags = [p for p in listing if p.endswith(".py")]
    return set(STATIC_PATHS) | import_closure(read, dags)


def contour_differs(h: Host, a: str, b: str) -> bool:
    paths = sorted(contour_paths(h, a) | contour_paths(h, b))
    return bool(git(h, "diff", "--name-only", a, b, "--", *paths).strip())


# ---------- окно ----------

def next_wave(now: datetime) -> datetime:
    day = now.replace(minute=0, second=0, microsecond=0)
    for days in (0, 1):
        for hour in WAVE_HOURS:
            wave = (day + timedelta(days=days)).replace(hour=hour)
            if wave > now:
                return wave
    raise AssertionError("unreachable")


def psql(h: Host, sql: str) -> str | None:
    rc, out, _ = h.run(["docker", "exec", METADB, "psql", "-U", "airflow", "-d", "airflow",
                        "-At", "-c", sql], timeout=60)
    return out.strip() if rc == 0 else None


def busy_reason(h: Host) -> str | None:
    now = h.now()
    wave = next_wave(now)
    if wave - now < WINDOW:
        return f"до волны {wave:%H:%M} UTC меньше {int(WINDOW.total_seconds() // 60)} мин"
    n = psql(h, f"SELECT count(*) FROM dag_run WHERE dag_id = '{DAG_ID}' "
                "AND state IN ('running', 'queued')")
    if n is None:
        return f"metadb {METADB} не отвечает"
    if n != "0":
        return f"идёт волна: dag_run running/queued = {n}"
    return None


# ---------- корни релиза ----------

def build_root(h: Host, sha: str) -> Path:
    root = h.paths.root(sha)
    if root.is_dir():
        return root
    tmp = h.paths.release_parent / f".{RELEASE_PREFIX}{sha}.tmp"
    if tmp.exists():
        make_writable(tmp)
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    rc, _, err = h.run(["bash", "-c", 'set -o pipefail; git -C "$1" archive "$2" -- "${@:4}" | tar -x -C "$3"',
                        "archive", str(h.paths.repo), sha, str(tmp), *ARCHIVE_PATHS], timeout=300)
    if rc != 0:
        make_writable(tmp)
        shutil.rmtree(tmp)
        raise Stop(f"git archive {sha[:12]} не удался: {err.strip()[:200]}")
    os.replace(tmp, root)
    for path in [root, *root.rglob("*")]:
        if not path.is_symlink():
            path.chmod(path.stat().st_mode & ~0o222)
    return root


def make_writable(root: Path) -> None:
    for path in [root, *root.rglob("*")]:
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | 0o200)


def cleanup_roots(h: Host) -> None:
    """Храним KEEP_ROOTS последних корней и корни accepted/accepted-prev; старший удаляем,
    только если на него не ссылается метка ни одного контейнера (в т.ч. остановленного)."""
    state = h.paths.state
    keep = {read_sha(state / "accepted"), read_sha(state / "accepted-prev")}
    roots = sorted((p for p in h.paths.release_parent.glob(f"{RELEASE_PREFIX}*")
                    if p.is_dir() and SHA_RE.match(p.name[len(RELEASE_PREFIX):])),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    for root in roots[KEEP_ROOTS:]:
        if root.name[len(RELEASE_PREFIX):] in keep:
            continue
        rc, out, _ = h.run(["docker", "ps", "-a", "-q", "--filter", f"label={LABEL}={root}"], timeout=60)
        if rc != 0 or out.strip():
            h.log(f"корень {root} не удалён: {'на него ссылается контейнер' if rc == 0 else 'docker ps упал'}")
            continue
        make_writable(root)
        shutil.rmtree(root)
        h.log(f"удалён старый корень {root}")


# ---------- выкат и приёмка ----------

def compose(h: Host, root: Path, *args: str) -> tuple[int, str]:
    cmd = ["docker", "compose", "-p", PROJECT, "--project-directory", str(h.paths.deploy_dir),
           "-f", str(root / COMPOSE_REL), "--env-file", str(h.paths.env_file), *args]
    env = {k: v for k, v in os.environ.items() if not is_proxy_name(k)}
    env["ESPN_RELEASE_ROOT"] = str(root)
    rc, out, err = h.run(cmd, env=env, timeout=900)
    return rc, (err or out).strip()[-300:]


def check_env_file(h: Host) -> None:
    path = h.paths.env_file
    if not path.is_file():
        raise Stop(f"нет env-файла {path}")
    if path.stat().st_mode & 0o077:
        raise Stop(f"{path}: права шире 0600")
    names = read_env_file(path)
    bad = sorted(n for n in names if n == "ESPN_RELEASE_ROOT" or is_proxy_name(n))
    if bad:
        raise Stop(f"{path} задаёт {', '.join(bad)} — корень релиза пинует только state/accepted, прокси запрещены")


def pools_of(root: Path) -> dict[str, int]:
    return {name: int(cfg["slots"]) for name, cfg in
            json.loads((root / POOLS_REL).read_text()).items()}


def roll_out(h: Host, root: Path, init: bool) -> str | None:
    """Выкат корня; None — успех, иначе причина."""
    if init:
        rc, err = compose(h, root, "run", "--rm", "--no-deps", "-T", "airflow-init")
        if rc != 0:
            return f"airflow-init rc={rc}: {err}"
    rc, err = compose(h, root, "up", "-d", "--no-deps", "--force-recreate", *SERVICES)
    if rc != 0:
        return f"compose up rc={rc}: {err}"
    return None


def host_hashes(base: Path) -> dict[str, str]:
    return {str(p.relative_to(base)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(base.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}


def container_hashes(h: Host, target: str) -> dict[str, str] | None:
    rc, out, _ = h.run(["docker", "exec", SCHEDULER, "sh", "-c",
                        'cd "$1" && find . -type f ! -path "*/__pycache__/*" -exec sha256sum {} +',
                        "sh", target], timeout=120)
    if rc != 0:
        return None
    hashes = {}
    for line in out.splitlines():
        digest, _, name = line.partition("  ")
        hashes[name.removeprefix("./")] = digest
    return hashes


def static_checks(h: Host, root: Path) -> str | None:
    rc, out, _ = h.run(["docker", "inspect", "--format", "{{json .Mounts}}|{{json .Config.Env}}",
                        SCHEDULER], timeout=60)
    if rc != 0:
        return f"docker inspect {SCHEDULER} не удался"
    mounts_json, _, env_json = out.strip().partition("|")
    sources = {m.get("Destination"): m.get("Source") for m in json.loads(mounts_json)}
    if sources.get(DAGS_TARGET) != str(root / DAG_DIR_REL):
        return f"{DAGS_TARGET} смонтирован из {sources.get(DAGS_TARGET)}, а не из {root}"
    proxies = sorted({e.split("=", 1)[0] for e in json.loads(env_json)
                      if is_proxy_name(e.split("=", 1)[0])})
    if proxies:
        return f"в env контейнера прокси: {', '.join(proxies)}"
    for rel, target in BYTE_CHECK.items():
        got = container_hashes(h, target)
        if got is None:
            return f"байты {target} в контейнере не прочитаны"
        if got != host_hashes(root / rel):
            return f"байты {target} в контейнере ≠ {rel} корня релиза"
    want = pools_of(root)
    out = psql(h, "SELECT pool || '|' || slots FROM slot_pool ORDER BY 1")
    if out is None:
        return "пулы не прочитаны: metadb не отвечает"
    have = dict(line.split("|", 1) for line in out.splitlines() if "|" in line)
    wrong = sorted(n for n, s in want.items() if have.get(n) != str(s))
    if wrong:
        return "пулы ≠ pools.json: " + ", ".join(f"{n}={have.get(n, 'нет')} (надо {want[n]})" for n in wrong)
    return None


def accept(h: Host, root: Path, cut: str) -> str | None:
    """Приёмка выката (≤ 7 мин); None — принято, иначе причина."""
    deadline = h.now() + timedelta(seconds=ACCEPT_TIMEOUT_S)
    reason = f"{DAG_ID} не перечитан за {ACCEPT_TIMEOUT_S // 60} мин"
    while True:
        fresh = psql(h, f"SELECT filename FROM import_error WHERE \"timestamp\" > timestamptz '{cut}' ORDER BY 1")
        if fresh:
            return f"import_error после выката: {fresh.splitlines()[0][:200]}"
        errors = psql(h, "SELECT count(*) FROM import_error")
        dag = psql(h, f"SELECT has_import_errors, last_parsed_time > timestamptz '{cut}' "
                      f"FROM dag WHERE dag_id = '{DAG_ID}'")
        if errors == "0" and dag == "f|t":
            break
        if errors is None or dag is None:
            reason = f"metadb {METADB} не отвечает"
        elif errors != "0":
            reason = f"import_error = {errors}"
        else:
            reason = f"{DAG_ID}: has_import_errors|перечитан = '{dag or 'нет в metadb'}'"
        if h.now() >= deadline:
            return reason
        h.sleep(ACCEPT_POLL_S)
    return static_checks(h, root)


def deliver(h: Host, root: Path, init: bool) -> str | None:
    """Выкат + приёмка; None — принято."""
    reason = roll_out(h, root, init)
    if reason:
        return reason
    cut = psql(h, "SELECT now()")
    if cut is None:
        return f"metadb {METADB} не отвечает (now())"
    return accept(h, root, cut)


def switch(h: Host, sha: str, accepted: str, *, label: str, on_success) -> int:
    """Выкатить sha поверх принятого accepted; при провале — вернуть accepted."""
    state = h.paths.state
    root = build_root(h, sha)
    live = h.paths.root(accepted)
    if not live.is_dir():
        live = build_root(h, accepted)
    write_atomic(state / "inflight", f"{sha} {label} {h.now():%Y-%m-%dT%H:%M:%SZ}\n")
    h.log(f"{label}: выкат {sha[:12]} (живой {accepted[:12]})")
    reason = deliver(h, root, init=pools_of(root) != pools_of(live))
    if reason is None:
        on_success()
        (state / "inflight").unlink()
        h.log(f"{label}: принято {sha[:12]}")
        h.telegram(f"✅ ESPN выкачен {sha[:12]} ({label})")
        cleanup_roots(h)
        return 0
    h.log(f"{label}: приёмка {sha[:12]} провалена: {reason} — откат на {accepted[:12]}")
    add_rejected(state, sha)
    back = deliver(h, live, init=pools_of(root) != pools_of(live))
    if back is None:
        (state / "inflight").unlink()
        h.telegram(f"❌ ESPN: {sha[:12]} ({label}) не принят — {reason}; откат на {accepted[:12]} принят")
        return 1
    (state / "off").touch()
    h.telegram(f"🆘 ESPN: {sha[:12]} ({label}) не принят — {reason}; откат на {accepted[:12]} "
               f"НЕ подтверждён — {back}. НУЖНЫ РУКИ; автомат выключен ({state / 'off'})")
    return 2


def seed(h: Host, sha: str) -> int:
    """Первый выкат: metadb → airflow-init → scheduler/webserver, та же приёмка."""
    state = h.paths.state
    if read_sha(state / "accepted"):
        raise Stop("посев: state/accepted уже есть — контур посеян")
    root = build_root(h, sha)
    rc, err = compose(h, root, "up", "-d", "--no-deps", "--wait", "airflow-metadb")
    if rc != 0:
        raise Stop(f"посев: metadb не поднялась rc={rc}: {err}")
    reason = deliver(h, root, init=True)
    if reason:
        raise Stop(f"посев {sha[:12]} не принят: {reason}")
    write_atomic(state / "accepted", sha + "\n")
    h.log(f"посев: принято {sha[:12]}")
    h.telegram(f"✅ ESPN: контур {PROJECT} посеян, {sha[:12]}")
    return 0


# ---------- режимы ----------

def self_check(h: Host) -> str:
    master = git(h, "rev-parse", "origin/master").strip()
    want = git_file(h, master, SELF_REL)
    if want is None or want.encode() != h.paths.self_file.read_bytes():
        raise Stop(f"копия автомата {h.paths.self_file} ≠ master {master[:12]} — обнови автомат (README «Доставка»)",
                   "self")
    return master


def pick_cron_target(h: Host, master: str, accepted: str) -> str | None:
    if master == accepted:
        return None
    if not git_ok(h, "merge-base", "--is-ancestor", accepted, master):
        raise Stop(f"master {master[:12]} не потомок живого {accepted[:12]} — нужны руки", "ancestry")
    for bad in read_rejected(h.paths.state):
        if bad != master and not git_ok(h, "cat-file", "-e", f"{bad}^{{commit}}"):
            continue   # ветка --target удалена и коммит собран сборщиком мусора
        if master == bad or not contour_differs(h, bad, master):
            h.log(f"master {master[:12]}: контур = отклонённому {bad[:12]} — ждём исправления")
            return None
    if not contour_differs(h, accepted, master):
        h.log(f"master {master[:12]}: пути контура не менялись с {accepted[:12]}")
        return None
    return master


def run(h: Host, args) -> int:
    state = h.paths.state
    if args.seed:
        git(h, "fetch", "-q", "origin")
        self_check(h)
        check_env_file(h)
        return seed(h, git(h, "rev-parse", "--verify", f"{args.seed}^{{commit}}").strip())
    if (state / "off").exists():
        h.log(f"выключатель {state / 'off'} — выход")
        return 0
    if (state / "inflight").exists():
        text = (state / "inflight").read_text().strip()
        h.log(f"висит inflight ({text}) — выход")
        notify_once(h, "inflight", f"🆘 ESPN: незавершённая доставка ({text}) — НУЖНЫ РУКИ")
        return 0
    accepted = read_sha(state / "accepted")
    if accepted is None:
        raise Stop("нет state/accepted — контур не посеян (README «Доставка»)", "accepted")
    git(h, "fetch", "-q", "origin")
    master = self_check(h)
    if args.rollback:
        prev = read_sha(state / "accepted-prev")
        if prev is None:
            raise Stop("--rollback: нет state/accepted-prev")
        target, label = prev, "откат --rollback"
    elif args.target:
        target = git(h, "rev-parse", "--verify", f"{args.target}^{{commit}}").strip()
        label = "--target"
        if target in read_rejected(state):
            raise Stop(f"--target {target[:12]} в rejected")
        if not git_ok(h, "merge-base", "--is-ancestor", accepted, target):
            raise Stop(f"--target {target[:12]} не потомок живого {accepted[:12]}")
        if target == accepted:
            h.log("--target = живой SHA — нечего делать")
            return 0
    else:
        target, label = pick_cron_target(h, master, accepted), "master"
        if target is None:
            return 0
    busy = busy_reason(h)
    if busy:
        h.log(f"{label} {target[:12]}: не в окне — {busy}")
        if args.target or args.rollback:
            raise Stop(f"{label} {target[:12]} не выполнен: {busy}")
        return 0
    check_env_file(h)

    def promote():
        if args.rollback:
            write_atomic(state / "accepted", target + "\n")
            add_rejected(state, accepted)
            (state / "accepted-prev").unlink()
        else:
            write_atomic(state / "accepted-prev", accepted + "\n")
            write_atomic(state / "accepted", target + "\n")

    return switch(h, target, accepted, label=label, on_success=promote)


def main(argv=None, host: Host | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--target", help="выкатить этот SHA (потомок живого) вне фильтра путей")
    mode.add_argument("--rollback", action="store_true", help="вернуть state/accepted-prev")
    mode.add_argument("--seed", metavar="SHA", help="первый выкат контура (state/accepted пуст)")
    args = parser.parse_args(argv)
    h = host or Host(Paths())
    h.paths.state.mkdir(parents=True, exist_ok=True)
    with open(h.paths.state / "lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            h.log("другой запуск держит lock — выход")
            return 0
        try:
            return run(h, args)
        except Stop as stop:
            reason = stop.args[0]
            h.log(f"СТОП: {reason}")
            # cron повторяет ту же остановку каждые 5 мин — в Telegram раз в сутки на причину.
            notify_once(h, stop.args[1] if len(stop.args) > 1 else reason,
                        f"⛔ ESPN автодоставка: {reason}")
            return 1


if __name__ == "__main__":
    sys.exit(main())
