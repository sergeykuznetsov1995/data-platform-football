"""Bounded medallion/proxy release for ClubElo #1465; no pause or data operations."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time

from deploy.shared_writer import release as shared
from deploy.shared_writer.host import Host as SharedHost, CONSUMERS
from deploy.shared_writer.legacy_errors import POLICIES

ROOT, STATE = shared.ROOT, shared.STATE
RELEASE = "clubelo-1465-shared-dependencies-20261009"
PINS = {
    "dags/utils/medallion_config.py": (
        "07b782cfd39d1edca87f66df79040f9e8924377f6eacf91bef3d02dff9493e91",
        "b3cde46db5fa568152b1a8e3c8a7775dc8b327b51ae7a323efeb21878692eb1e"),
    "scrapers/utils/proxy_manager.py": (
        "3ee21c9045090046bd2f9509a038fbc2bc815e3cd1b67006841404bdd57b564b",
        "d10e6da14d1ee79c9a8233e1b20669ea735463c652a59bb7525faf88df5d11e0"),
}
# Public catalog inputs needed for real DAG imports; no environment or proxy pools.
CONFIGS = (
    "configs/medallion/competitions.yaml", "configs/medallion/country_codes.yaml",
    "configs/medallion/team_aliases.yaml", "configs/medallion/player_aliases.yaml",
    "configs/medallion/manager_aliases.yaml", "configs/medallion/referee_aliases.yaml",
    "configs/medallion/venue_aliases.yaml", "configs/medallion/source_priority.yaml",
    "configs/medallion/match_result_overrides.yaml", "configs/medallion/fbref_event_source_gaps.yaml",
    "configs/sofascore/tournaments.json", "configs/fotmob/competitions.json",
)
require, digest, canonical = shared.require, shared.digest, shared.canonical
read, save, safe_path = shared.read, shared.save, shared.safe_path
journal = shared.journal


def closure(root):
    files = shared.closure(root)
    for name in CONFIGS:
        info = shared.regular(root / name)
        files[name] = {"sha256": digest(read(root / name)), "mode": stat.S_IMODE(info.st_mode),
                       "uid": info.st_uid, "gid": info.st_gid}
    require(set(PINS) <= files.keys(), "missing shared dependency")
    return files


def tool_hashes():
    own = Path(__file__).resolve().parent
    return {**{f"shared_writer/{k}": v for k, v in shared.tool_hashes().items()},
            **{f"shared_dependencies/{name}": digest(read(own / name))
               for name in ("release.py", "probe.py")}}


def payload(repo, commit):
    require(isinstance(commit, str) and re.fullmatch(r"[0-9a-f]{40}", commit), "full commit SHA required")
    result = {}
    for name, (_, after) in PINS.items():
        entry = shared.command(["git", "-C", str(repo), "ls-tree", commit, "--", name]).decode()
        require(entry.startswith("100644 blob "), "payload must be regular Git source")
        result[name] = shared.command(["git", "-C", str(repo), "show", f"{commit}:{name}"])
        require(digest(result[name]) == after, f"unreviewed payload: {name}")
    return result


def merged(repo, commit):
    shared.command(["git", "-C", str(repo), "fetch", "origin", "master"], timeout=120)
    shared.command(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, "origin/master"])
    for name, (_, after) in PINS.items():
        require(digest(shared.command(["git", "-C", str(repo), "show", f"origin/master:{name}"])) == after,
                f"master dependency advanced: {name}")


def prepare(bundle, root, repo, commit, start, end, host):
    bundle, root, repo = safe_path(bundle), safe_path(root), safe_path(repo)
    require(not bundle.is_relative_to(root) and not root.is_relative_to(bundle), "bundle overlaps runtime")
    require(not repo.is_relative_to(root) and not root.is_relative_to(repo), "repository overlaps runtime")
    require(all(type(v) in (int, float) and math.isfinite(v) for v in (start, end))
            and 0 < end - start <= 3600 and end > time.time(), "future window of at most one hour required")
    require(not bundle.exists(), "bundle already exists")
    require(bundle.parent.stat().st_dev == root.stat().st_dev, "bundle must share target filesystem")
    new = payload(repo, commit)
    files = closure(root)
    for name, (before, _) in PINS.items():
        require(files[name]["sha256"] == before, f"unexpected live dependency: {name}")
    observation = host.snapshot()
    require(files == closure(root), "runtime changed during capture")
    manifest = {"schema": 1, "release": RELEASE, "root": str(root), "repo": str(repo),
                "commit": commit, "start": start, "end": end, "files": files,
                "host": observation, "tools": tool_hashes()}
    bundle.mkdir(mode=0o700)
    for index, name in enumerate(PINS):
        save(bundle / f"before-{index}.py", read(root / name))
        save(bundle / f"after-{index}.py", new[name])
    require(files == closure(root), "runtime changed while saving originals")
    save(bundle / "manifest.json", canonical(manifest))
    approval = digest(canonical(manifest))
    journal(bundle, approval, "prepared")
    return approval


def load(bundle, approval):
    bundle = safe_path(bundle)
    info = bundle.stat()
    require(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.geteuid(), "private owned bundle required")
    raw = read(bundle / "manifest.json")
    require(digest(raw) == approval, "manifest approval mismatch")
    m = json.loads(raw)
    require(m["schema"] == 1 and m["release"] == RELEASE and m["tools"] == tool_hashes(), "changed release tools/schema")
    root = safe_path(m["root"])
    require(not bundle.is_relative_to(root) and not root.is_relative_to(bundle), "bundle overlaps runtime")
    repo = safe_path(m["repo"])
    require(not repo.is_relative_to(root) and not root.is_relative_to(repo), "repository overlaps runtime")
    require(bundle.stat().st_dev == root.stat().st_dev, "bundle filesystem changed")
    for index, (name, (before, after)) in enumerate(PINS.items()):
        require(m["files"][name]["sha256"] == before, "manifest before fingerprint mismatch")
        require(digest(read(bundle / f"before-{index}.py")) == before
                and digest(read(bundle / f"after-{index}.py")) == after, "corrupt payload/backup")
    s = json.loads(read(bundle / "state.json"))
    require(s["manifest"] == approval, "foreign journal")
    return m, s


def states(m):
    """Infer each file's exact old/new state; a foreign or metadata change blocks."""
    actual = closure(Path(m["root"]))
    expected = {k: dict(v) for k, v in m["files"].items()}
    result = {}
    for name, pins in PINS.items():
        fingerprint = actual[name]["sha256"]
        require(fingerprint in pins, f"foreign dependency: {name}")
        result[name] = pins.index(fingerprint)
        expected[name]["sha256"] = fingerprint
    require(actual == expected, "runtime code/catalog/metadata drift")
    return result


def check_files(m, target):
    require(all(value == target for value in states(m).values()), "dependency set does not match expected phase")


def replace_one(bundle, m, name, target):
    current = states(m)
    if current[name] == target:
        return
    metadata = m["files"][name]
    index = list(PINS).index(name)
    fd, temporary = tempfile.mkstemp(prefix=".dependency-", dir=bundle)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(read(bundle / f"{'after' if target else 'before'}-{index}.py"))
            os.fchown(stream.fileno(), metadata["uid"], metadata["gid"])
            os.fchmod(stream.fileno(), metadata["mode"])
            stream.flush()
            os.fsync(stream.fileno())
        require(digest(read(temporary)) == PINS[name][target], "staged bytes changed")
        require(states(m) == current, "runtime changed before publication")
        os.replace(temporary, Path(m["root"]) / name)
        shared.sync_dir((Path(m["root"]) / name).parent)
        shared.sync_dir(bundle)
    finally:
        Path(temporary).unlink(missing_ok=True)


class Host(SharedHost):
    """Preserve existing readiness/legacy-error gates; observe both mounted files."""
    def dependency_hashes(self, target):
        result = {}
        for consumer in CONSUMERS:
            hashes = {}
            for name, pins in PINS.items():
                path = "/opt/airflow/" + name
                fields = self._run(["docker", "exec", consumer, "sha256sum", path]).strip().split()
                require(fields == [pins[target], path], f"mounted dependency mismatch: {consumer}/{name}")
                hashes[name] = fields[0]
            result[consumer] = hashes
        return result


def rollback(bundle, m, approval, host):
    states(m)
    save(bundle / "rollback-preflight.json", canonical(host.rollback_preflight(m["host"])))
    journal(bundle, approval, "rolling_back")
    for name in reversed(PINS):
        replace_one(bundle, m, name, 0)
    cut = time.time()  # all bytes have been restored before the parse boundary
    check_files(m, 0)
    save(bundle / "rollback-postflight.json", canonical(host.postflight(m["host"], cut)))
    save(bundle / "rollback-consumers.json", canonical(host.dependency_hashes(0)))
    check_files(m, 0)
    journal(bundle, approval, "rolled_back")


def clear_marker(marker, approval, state):
    """Cleanup of a terminal release never starts another runtime rollback."""
    try:
        marker.unlink()
        shared.sync_dir(state)
    except Exception:
        # Held delivery locks prevent another release while we restore fencing.
        # If durable I/O remains unavailable, the terminal code is still verified;
        # this cleanup error must not initiate an unfenced partial rollback.
        save(marker, canonical({"manifest": approval}))
        raise


def execute(bundle, approval, host, *, recover=False, state=STATE):
    bundle, state = safe_path(bundle), safe_path(state)
    with shared.locks(state):
        m, s = load(bundle, approval)
        marker = state / "shared-writer-inflight.json"  # shared with the writer runner
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
            clear_marker(marker, approval, state)
            return
        require(not owned and s["phase"] == "prepared", "bundle already used or needs recovery")
        require(m["start"] <= time.time() < m["end"], "outside approved window")
        merged(Path(m["repo"]), m["commit"])
        require(json.loads(read(bundle / "rehearsal.json")) == {"manifest": approval, "passed": True}, "missing exact rehearsal")
        check_files(m, 0)
        save(bundle / "preflight.json", canonical(host.preflight(m["host"])))
        save(bundle / "before-consumers.json", canonical(host.dependency_hashes(0)))
        require(m["start"] <= time.time() < m["end"], "window expired during preflight")
        save(marker, canonical({"manifest": approval}))
        journal(bundle, approval, "inflight")
        try:
            for name in PINS:
                require(time.time() < m["end"], "window expired during publication")
                replace_one(bundle, m, name, 1)
            cut = time.time()
            journal(bundle, approval, "swapped", cut=cut)
            check_files(m, 1)
            save(bundle / "postflight.json", canonical(host.postflight(m["host"], cut)))
            save(bundle / "after-consumers.json", canonical(host.dependency_hashes(1)))
            check_files(m, 1)
            journal(bundle, approval, "accepted")
        except Exception:
            try:
                rollback(bundle, m, approval, host)
            except Exception as exc:
                journal(bundle, approval, "blocked", error=type(exc).__name__)
            else:
                clear_marker(marker, approval, state)
            raise
        # Everything above was durably accepted. A cleanup failure may retain
        # fencing and return nonzero, but cannot turn this into mixed runtime.
        clear_marker(marker, approval, state)


def rehearse(bundle, approval):
    m, s = load(bundle, approval)
    require(s["phase"] == "prepared", "rehearse before apply")
    check_files(m, 0)
    stage = bundle / "rehearsal-runtime"
    require(not stage.exists(), "existing rehearsal; prepare a fresh bundle")
    stage.mkdir(mode=0o755)
    for directory in ("scrapers", "dags", "scripts"):
        (stage / directory).mkdir()
    for name, info in m["files"].items():
        path = stage / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(read(Path(m["root"]) / name))
        os.chown(path, info["uid"], info["gid"])
        os.chmod(path, info["mode"])
    copy = dict(m, root=str(stage))
    check_files(m, 0)
    # Forward and reverse prefixes are compatible before any production apply.
    phases = (("before", None, 0), ("medallion-only", list(PINS)[0], 1),
              ("after", list(PINS)[1], 1), ("rollback-prefix", list(PINS)[1], 0),
              ("rollback", list(PINS)[0], 0))
    for phase, changed, target in phases:
        if changed:
            replace_one(bundle, copy, changed, target)
        current = states(copy)
        dag_files = {}
        for dag_id, dag in m["host"]["dags"].items():
            path = str(Path(dag["fileloc"]).relative_to("/opt/airflow"))
            require(path in m["files"], "DAG outside pinned closure")
            dag_files.setdefault(path, []).append(dag_id)
        plan = {"dag_files": dag_files, "hashes": {name: PINS[name][current[name]] for name in PINS},
                "medallion_new": bool(current[list(PINS)[0]]), "proxy_new": bool(current[list(PINS)[1]])}
        save(stage / "dependency-probe.json", canonical(plan))
        (stage / "dependency-probe.json").chmod(0o644)
        for name, consumer in m["host"]["identity"]["consumers"].items():
            container = f"dependencies-rehearsal-{os.getpid()}-{phase}-{name}"
            args = ["docker", "run", "--rm", "--name", container, "-i", "--network", "none", "--read-only",
                    "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m", "--cpus", "1", "--memory", "1g", "--pids-limit", "128",
                    "--mount", f"type=bind,src={stage},dst=/opt/airflow,readonly", "--workdir", "/opt/airflow",
                    "-e", "AIRFLOW_HOME=/tmp/airflow", "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db",
                    "-e", "AIRFLOW__CORE__LOAD_EXAMPLES=False", "--entrypoint", "/usr/local/bin/python", consumer["image"], "-B", "-"]
            try:
                result = subprocess.run(args, input=read(Path(__file__).with_name("probe.py")), capture_output=True, timeout=120)
            finally:
                subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
            save(bundle / f"rehearsal-{phase}-{name}.log", result.stdout + result.stderr)
            require(result.returncode == 0, f"isolated startup/import/behavior failed: {phase}/{name}")
    check_files(copy, 0)
    check_files(m, 0)
    save(bundle / "rehearsal.json", canonical({"manifest": approval, "passed": True}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "rehearse", "check", "apply", "recover"))
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--approval", help="manifest SHA256; does not replace owner permission")
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--start", type=float)
    parser.add_argument("--end", type=float)
    parser.add_argument("--import-error-policy", choices=POLICIES)
    args = parser.parse_args()
    require(args.import_error_policy is None or args.action == "prepare", "policy pinned at prepare")
    host = Host(ROOT, import_error_policy=args.import_error_policy or "none")
    if args.action == "prepare":
        require(all(v is not None for v in (args.repo, args.commit, args.start, args.end)), "prepare needs repo/commit/start/end")
        print(prepare(args.bundle, ROOT, args.repo, args.commit, args.start, args.end, host))
        return
    m, _ = load(args.bundle, args.approval)
    require(Path(m["root"]) == ROOT, "wrong live target")
    if args.action == "rehearse":
        rehearse(args.bundle, args.approval)
    elif args.action == "check":
        check_files(m, 0)
        host.preflight(m["host"])
        print("preflight passed; no delivery")
    else:
        execute(args.bundle, args.approval, host, recover=args.action == "recover")
        print(json.loads(read(args.bundle / "state.json"))["phase"])


if __name__ == "__main__":
    main()
