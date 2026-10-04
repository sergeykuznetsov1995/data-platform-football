"""One-file shared writer release. No service restart, pause or data operations."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time

WRITER = "scrapers/base/iceberg_writer.py"
BEFORE = "5ab88ead5d77317f17dde11db8033d79501c6ebd0a48fd5b6eb60efc70ad24d3"
AFTER = "a1d2d2dfa33b496ca891e0752560099ac0b99fef71146c67e4a1d7f09970e9c4"
ROOT = Path("/root/dpf-whoscored-merge")
STATE = Path("/root/watchdog/state")
LOCKS = ("shared-writer-release.lock", "clubelo-auto-deliver.lock",
         "understat-auto-deliver.lock", "whoscored-deliver.lock", "fotmob-auto-deliver.lock",
         "sofascore-auto-deliver.lock", "transfermarkt-auto-deliver.lock")
MARKERS = ("clubelo-inflight", "understat-inflight", "whoscored-inflight", "fotmob-b6-inflight",
           "sofascore-inflight", "transfermarkt-inflight")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def safe_path(path):
    path = Path(path).absolute()
    require(path.resolve() == path, f"symlink/noncanonical path: {path}")
    return path


def regular(path):
    path = safe_path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, f"not a single regular file: {path}")
    return info


def read(path):
    regular(path)
    return Path(path).read_bytes()


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, data):
    """Durable private state; temporary files stay outside the runtime tree."""
    path = safe_path(path)
    if path.exists():
        regular(path)
    fd, tmp = tempfile.mkstemp(prefix=".journal-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        sync_dir(path.parent)
    finally:
        Path(tmp).unlink(missing_ok=True)


def closure(root):
    """Only source code and the original startup lock; never read .env/secrets."""
    root = safe_path(root)
    files = {}
    for tree in ("scrapers", "dags", "scripts"):
        require((root / tree).is_dir(), f"missing code tree: {tree}")
        for parent, dirs, names in os.walk(root / tree):
            dirs[:] = sorted(d for d in dirs if d not in ("__pycache__", ".git"))
            for directory in dirs:
                safe_path(Path(parent) / directory)
            for name in sorted(names):
                if name.endswith(".py") or name in ("runtime_contract.lock", ".airflowignore"):
                    file = Path(parent) / name
                    info = regular(file)
                    files[str(file.relative_to(root))] = {
                        "sha256": digest(read(file)), "mode": stat.S_IMODE(info.st_mode),
                        "uid": info.st_uid, "gid": info.st_gid,
                    }
    require(WRITER in files and any(p.endswith("runtime_contract.lock") for p in files), "incomplete runtime")
    return files


def command(args, timeout=60):
    return subprocess.run(args, check=True, capture_output=True, timeout=timeout).stdout


def payload(repo, commit):
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "full commit SHA required")
    entry = command(["git", "-C", str(repo), "ls-tree", commit, "--", WRITER]).decode()
    require(entry.startswith("100644 blob "), "payload is not a regular Git blob")
    result = command(["git", "-C", str(repo), "show", f"{commit}:{WRITER}"])
    require(digest(result) == AFTER, "payload outside the approved one-line fix")
    return result


def merged(repo, commit):
    command(["git", "-C", str(repo), "fetch", "origin", "master"], timeout=120)
    command(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, "origin/master"])
    require(digest(command(["git", "-C", str(repo), "show", f"origin/master:{WRITER}"])) == AFTER,
            "master writer has advanced; prepare a new reviewed release")


def tool_hashes():
    directory = Path(__file__).resolve().parent
    return {name: digest(read(directory / name)) for name in ("release.py", "host.py", "probe.py", "legacy_errors.py", "processes.py", "work.py")}


def prepare(bundle, root, repo, commit, start, end, host):
    bundle, root = safe_path(bundle), safe_path(root)
    require(not bundle.is_relative_to(root) and not root.is_relative_to(bundle), "bundle must be outside runtime")
    require(0 < end - start <= 3600 and end > time.time(), "use a future window of at most one hour")
    require(not bundle.exists(), "bundle already exists")
    new = payload(repo, commit)
    files = closure(root)
    require(files[WRITER]["sha256"] == BEFORE, "unexpected live writer")
    old = read(root / WRITER)
    before = host.snapshot()
    require(closure(root) == files and digest(old) == BEFORE, "runtime changed during capture")
    manifest = {"schema": 1, "root": str(root), "repo": str(safe_path(repo)), "commit": commit,
                "start": start, "end": end, "files": files, "host": before, "tools": tool_hashes()}
    bundle.mkdir(mode=0o700, parents=False)
    save(bundle / "before.py", old)
    save(bundle / "after.py", new)
    save(bundle / "manifest.json", canonical(manifest))
    journal(bundle, digest(canonical(manifest)), "prepared")
    return digest(canonical(manifest))


def load(bundle, approval):
    bundle = safe_path(bundle)
    info = bundle.stat()
    require(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.geteuid(), "bundle must be owned private directory")
    raw = read(bundle / "manifest.json")
    require(digest(raw) == approval, "approval does not match manifest digest")
    m = json.loads(raw)
    require(m["schema"] == 1 and m["tools"] == tool_hashes(), "unsupported or changed release tools")
    require(digest(read(bundle / "before.py")) == BEFORE and digest(read(bundle / "after.py")) == AFTER, "corrupt bundle")
    root = safe_path(m["root"])
    require(not bundle.is_relative_to(root) and not root.is_relative_to(bundle), "bundle overlaps runtime")
    require(bundle.stat().st_dev == (root / WRITER).parent.stat().st_dev, "bundle and target must share filesystem")
    s = json.loads(read(bundle / "state.json"))
    require(s["manifest"] == approval, "journal belongs to another manifest")
    return m, s


def expected(m, new):
    result = {key: dict(value) for key, value in m["files"].items()}
    result[WRITER]["sha256"] = AFTER if new else BEFORE
    return result


def check_files(m, new):
    require(closure(Path(m["root"])) == expected(m, new), "runtime code/permissions drift")


@contextmanager
def locks(state=STATE):
    state = safe_path(state)
    handles = []
    try:
        for name in sorted(LOCKS):
            file = state / name
            flags = os.O_RDWR | os.O_NOFOLLOW
            if name == "shared-writer-release.lock":
                flags |= os.O_CREAT
            fd = os.open(file, flags, 0o600)
            handles.append(fd)
            regular(file)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require(not any((state / p).exists() or (state / p).is_symlink() for p in MARKERS), "another delivery is inflight")
        yield
    finally:
        for fd in reversed(handles):
            os.close(fd)


def journal(bundle, approval, phase, **extra):
    record = canonical({"manifest": approval, "phase": phase, "time": time.time(), **extra})
    history = safe_path(bundle / "journal.jsonl")
    if history.exists():
        regular(history)
    fd = os.open(history, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "ab") as stream:
        stream.write(record)
        stream.flush()
        os.fsync(stream.fileno())
    save(bundle / "state.json", record)


def replace(bundle, m, new):
    """Single atomic rename, preserving owner and mode; source temp outside seals."""
    target = Path(m["root"]) / WRITER
    meta = m["files"][WRITER]
    regular(target)
    fd, tmp = tempfile.mkstemp(prefix=".writer-", dir=bundle)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(read(bundle / ("after.py" if new else "before.py")))
            os.fchown(stream.fileno(), meta["uid"], meta["gid"])
            os.fchmod(stream.fileno(), meta["mode"])
            stream.flush()
            os.fsync(stream.fileno())
        # A second check immediately before publication, in addition to held locks.
        check_files(m, not new)
        os.replace(tmp, target)
        sync_dir(target.parent)
        sync_dir(bundle)
    finally:
        Path(tmp).unlink(missing_ok=True)


def rollback(bundle, m, approval, host):
    current = digest(read(Path(m["root"]) / WRITER))
    require(current in (BEFORE, AFTER), "foreign writer; manual investigation required")
    check_files(m, current == AFTER)
    # Do not roll old code into changed images/locks or an active maintenance window.
    save(bundle / "rollback-preflight.json", canonical(host.rollback_preflight(m["host"])))
    cut = time.time()
    journal(bundle, approval, "rolling_back", cut=cut)
    if current == AFTER:
        replace(bundle, m, False)
    check_files(m, False)
    save(bundle / "rollback-postflight.json", canonical(host.postflight(m["host"], cut)))
    check_files(m, False)
    save(bundle / "rollback-writer-hashes.json", canonical(host.writer_hashes(BEFORE)))
    journal(bundle, approval, "rolled_back")


def execute(bundle, approval, host, *, recover=False, state=STATE):
    bundle = safe_path(bundle)
    with locks(state):
        m, s = load(bundle, approval)
        marker = state / "shared-writer-inflight.json"
        owned = marker.exists() or marker.is_symlink()
        if owned:
            require(json.loads(read(marker)) == {"manifest": approval}, "another shared release needs recovery")
        if recover:
            require(s["phase"] in ("inflight", "swapped", "accepted", "rolling_back", "blocked")
                    or (owned and s["phase"] in ("prepared", "rolled_back")), "nothing to recover")
            save(marker, canonical({"manifest": approval}))
            try:
                rollback(bundle, m, approval, host)
            except Exception as exc:
                journal(bundle, approval, "blocked", error=type(exc).__name__)
                raise
            marker.unlink()
            sync_dir(state)
            return
        require(not owned, "this shared release needs recovery")
        require(s["phase"] == "prepared", "bundle already used; recover or prepare anew")
        require(m["start"] <= time.time() < m["end"], "outside approved window")
        merged(Path(m["repo"]), m["commit"])
        receipt = json.loads(read(bundle / "rehearsal.json"))
        require(receipt == {"manifest": approval, "passed": True}, "missing exact-bundle rehearsal")
        check_files(m, False)
        save(bundle / "preflight.json", canonical(host.preflight(m["host"])))
        save(bundle / "before-writer-hashes.json", canonical(host.writer_hashes(BEFORE)))
        require(m["start"] <= time.time() < m["end"], "window expired during preflight")
        cut = time.time()
        # A durable common marker prevents a second bundle overtaking a crashed release.
        save(marker, canonical({"manifest": approval}))
        journal(bundle, approval, "inflight", cut=cut)
        try:
            replace(bundle, m, True)
            journal(bundle, approval, "swapped", cut=cut)
            check_files(m, True)
            save(bundle / "postflight.json", canonical(host.postflight(m["host"], cut)))
            check_files(m, True)
            save(bundle / "after-writer-hashes.json", canonical(host.writer_hashes(AFTER)))
            journal(bundle, approval, "accepted")
            marker.unlink()
            sync_dir(state)
        except Exception:
            try:
                rollback(bundle, m, approval, host)
                marker.unlink()
                sync_dir(state)
            except Exception as exc:
                journal(bundle, approval, "blocked", error=type(exc).__name__)
            raise


def rehearse(bundle, approval):
    """Twelve fresh, networkless processes against copied code, never live mounts."""
    bundle = safe_path(bundle)
    m, s = load(bundle, approval)
    require(s["phase"] == "prepared", "rehearse before apply")
    check_files(m, False)
    root = Path(m["root"])
    stage = bundle / "rehearsal-runtime"
    require(not stage.exists(), "existing rehearsal copy; prepare a fresh bundle")
    stage.mkdir(mode=0o755)
    for directory in ("scrapers", "dags", "scripts"):
        (stage / directory).mkdir()
    for name, info in m["files"].items():
        target = stage / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(read(root / name))
        os.chown(target, info["uid"], info["gid"])
        os.chmod(target, info["mode"])
    check_files(m, False)
    rehearsal_m = dict(m, root=str(stage))
    probe = read(Path(__file__).resolve().parent / "probe.py")
    for phase in ("before", "after", "rollback"):
        if phase != "before":
            replace(bundle, rehearsal_m, phase == "after")
        check_files(rehearsal_m, phase == "after")
        for name, consumer in m["host"]["identity"]["consumers"].items():
            container_name = f"writer-rehearsal-{os.getpid()}-{phase}-{name}"
            args = ["docker", "run", "--rm", "--name", container_name, "-i", "--network", "none", "--read-only",
                    "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m", "--cpus", "1", "--memory", "1g",
                    "--pids-limit", "128", "--mount", f"type=bind,src={stage},dst=/opt/airflow,readonly",
                    "--workdir", "/opt/airflow", "--entrypoint", "/usr/local/bin/python",
                    consumer["image"], "-B", "-"]
            try:
                result = subprocess.run(args, input=probe, capture_output=True, timeout=120)
            finally:
                # Kill only this isolated test container if the client times out.
                subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=30)
            save(bundle / f"rehearsal-{phase}-{name}.log", result.stdout + result.stderr)
            require(result.returncode == 0, f"isolated startup failed: {phase}/{name}")
    check_files(m, False)
    save(bundle / "rehearsal.json", canonical({"manifest": approval, "passed": True}))


def main():
    from deploy.shared_writer.host import Host
    from deploy.shared_writer.legacy_errors import POLICIES
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "rehearse", "check", "apply", "recover", "rollback"))
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--approval", help="exact manifest SHA256; does not replace owner permission")
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--start", type=float, help="approved window UTC epoch seconds")
    parser.add_argument("--end", type=float)
    parser.add_argument("--import-error-policy", choices=POLICIES, default=None,
                        help="prepare only: explicit reviewed legacy baseline; default none")
    args = parser.parse_args()
    require(args.import_error_policy is None or args.action == "prepare",
            "import-error policy is pinned at prepare; cannot change an existing bundle")
    host = Host(ROOT, import_error_policy=args.import_error_policy or "none")
    if args.action == "prepare":
        require(all(v is not None for v in (args.repo, args.commit, args.start, args.end)), "prepare needs repo/commit/start/end")
        print(prepare(args.bundle, ROOT, args.repo, args.commit, args.start, args.end, host))
    elif args.action == "rehearse":
        rehearse(args.bundle, args.approval)
    elif args.action == "check":
        m, _ = load(args.bundle, args.approval)
        require(Path(m["root"]) == ROOT, "wrong live target")
        check_files(m, False)
        host.preflight(m["host"])
        print("preflight passed; delivery not performed")
    else:
        m, _ = load(args.bundle, args.approval)
        require(Path(m["root"]) == ROOT, "wrong live target")
        execute(args.bundle, args.approval, host, recover=args.action != "apply")
        print(json.loads(read(args.bundle / "state.json"))["phase"])


if __name__ == "__main__":
    main()
