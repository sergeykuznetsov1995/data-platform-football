"""Read-only host evidence for the separately authorized shared writer window.

This adapter never pauses DAGs, parses production Python, or executes a delivery.
The caller must hold the agreed delivery locks and coordinate other operators;
observing an idle process list is not a lock against future external launches.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
import stat
import subprocess
import time
from typing import Any

from deploy.shared_writer import legacy_errors
from deploy.shared_writer.processes import classify_processes
from deploy.shared_writer.work import WORK_SQL, classify_work


CONSUMERS = (
    "airflow-scheduler",
    "airflow-webserver",
    "proxy_filter",
    "fbref_proxy_filter",
)
METADB = "postgres"
INSPECT_FORMAT = (
    '{"init_pid":{{json .State.Pid}},"privileged":{{json .HostConfig.Privileged}},'
    '"pid_mode":{{json .HostConfig.PidMode}},"cap_add":{{json .HostConfig.CapAdd}},'
    '"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},'
    '"mounts":{{json .Mounts}},"status":{{json .State.Status}},'
    '"health":{{with (index .State "Health")}}{{json .Status}}{{else}}null{{end}},'
    '"project":{{with (index .Config "Labels")}}{{json (index . "com.docker.compose.project")}}{{else}}null{{end}},'
    '"working_dir":{{with (index .Config "Labels")}}{{json (index . "com.docker.compose.project.working_dir")}}{{else}}null{{end}},'
    '"config_files":{{with (index .Config "Labels")}}{{json (index . "com.docker.compose.project.config_files")}}{{else}}null{{end}}}'
)
DRIVER_PATTERN = (
    r"run_(clubelo|understat|fbref|fotmob|sofascore|transfermarkt|whoscored)_scrape[r]"
    r"|auto_delive[r]\.sh|auto-delive[r]\.sh"
    r"|(clubelo|understat|fbref|fotmob|sofascore|transfermarkt|whoscored)"
    r"_(history_backfill|backfill|increment)[/]"
    r"|run_live_wave[s]|run_fbre[f]|run_(clubelo|understat|fotmob|sofascore|transfermarkt|whoscored)_backfil[l]"
)
SNAPSHOT_SQL = """
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '10s';
SELECT json_build_object(
  'dags', COALESCE((SELECT json_object_agg(dag_id, json_build_object(
    'paused', is_paused, 'errors', has_import_errors, 'fileloc', fileloc,
    'parsed', extract(epoch FROM last_parsed_time)))
    FROM dag WHERE is_active IS TRUE), '{}'::json),
  'work', __WORK_SQL__,
  'import_errors', COALESCE((SELECT json_agg(json_build_object(
    'filename', filename, 'stacktrace', stacktrace)) FROM import_error), '[]'::json)
);
ROLLBACK;
""".replace("__WORK_SQL__", WORK_SQL)


class Host:
    def __init__(self, root: Path = Path("/root/dpf-whoscored-merge"), *, import_error_policy: str = "none") -> None:
        self.root = Path(root).absolute()
        if import_error_policy not in legacy_errors.POLICIES:
            raise RuntimeError("unknown import-error policy")
        self.import_error_policy = import_error_policy

    def _run(self, args: list[str], *, deadline: float | None = None, empty: bool = False) -> str:
        timeout = 20.0 if deadline is None else min(20.0, deadline - time.monotonic())
        if timeout <= 0:
            raise RuntimeError("host observation deadline exceeded")
        try:
            result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"host command failed or timed out: {args[0]}") from exc
        if result.returncode == 1 and empty and not result.stdout.strip():
            return ""
        if result.returncode != 0:
            # Neither stderr nor command arguments are copied into the journal.
            raise RuntimeError(f"host command failed: {args[0]} (exit {result.returncode})")
        return result.stdout

    def _inventory(self, deadline: float | None, *, recovery: bool = False) -> dict[str, Any]:
        ids = self._run(["docker", "ps", "-aq", "--no-trunc"], deadline=deadline).split()
        if not ids or any(not re.fullmatch(r"[0-9a-f]{12,64}", value) for value in ids):
            raise RuntimeError("invalid or empty Docker inventory")
        output = self._run(["docker", "inspect", "--format", INSPECT_FORMAT, *ids], deadline=deadline)
        try:
            rows = [json.loads(line) for line in output.splitlines() if line.strip()]
            containers = {row["name"].removeprefix("/"): row for row in rows}
            if len(rows) != len(ids) or len(containers) != len(rows):
                raise ValueError("incomplete inventory")
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise RuntimeError("invalid Docker inventory response") from exc
        self._containers = rows
        identities = {}
        other_shared_mounts = {}
        dormant_consumers = {}
        code_roots = [self.root / part for part in ("scrapers", "dags", "scripts")]
        writer = self.root / "scrapers/base/iceberg_writer.py"
        for name, row in containers.items():
            mounts = row.get("mounts")
            if not isinstance(mounts, list):
                raise RuntimeError("missing mount inventory")
            for mount in mounts:
                source = Path(mount.get("Source", "/__missing__"))
                if name not in CONSUMERS:
                    if source == writer or source in writer.parents or any(source == code or source in code.parents for code in code_roots):
                        if row.get("status") != "exited":
                            raise RuntimeError(f"unexpected consumer of shared tree: {name}")
                        dormant_consumers[name] = {
                            **row, "mounts": sorted(mounts, key=lambda m: m.get("Destination", "")),
                        }
                    if source == self.root or self.root in source.parents:
                        other_shared_mounts.setdefault(name, []).append({
                            "source": str(source), "destination": mount.get("Destination"),
                        })
            if name not in (*CONSUMERS, METADB):
                continue
            if row.get("status") != "running" or (row.get("health") != "healthy" and not (recovery and name in CONSUMERS)):
                raise RuntimeError(f"container is not running and healthy: {name}")
            if not isinstance(row.get("id"), str) or not re.fullmatch(r"[0-9a-f]{64}", row["id"]):
                raise RuntimeError("missing container identity")
            if not isinstance(row.get("image"), str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", row["image"]):
                raise RuntimeError("missing container identity")
            config_hashes = {}
            if name in CONSUMERS:
                if row.get("working_dir") != str(self.root) or row.get("project") != "data-platform":
                    raise RuntimeError(f"unexpected Compose project/root: {name}")
                by_destination = {m.get("Destination"): m for m in mounts}
                if len(by_destination) != len(mounts):
                    raise RuntimeError("duplicate Docker mount destination")
                for directory in ("scrapers", "dags", "scripts"):
                    mount = by_destination.get(f"/opt/airflow/{directory}", {})
                    if mount.get("Source") != str(self.root / directory) or mount.get("Type") != "bind" or mount.get("RW") is not False:
                        raise RuntimeError(f"unexpected shared code mount: {name}/{directory}")
                config_files = row.get("config_files")
                if not isinstance(config_files, str) or not config_files:
                    raise RuntimeError("missing Compose configuration list")
                for filename in config_files.split(","):
                    path = Path(filename)
                    try:
                        if not path.is_absolute() or not stat.S_ISREG(path.lstat().st_mode):
                            raise RuntimeError("Compose configuration is not a regular absolute file")
                        config_hashes[filename] = hashlib.sha256(path.read_bytes()).hexdigest()
                    except OSError as exc:
                        raise RuntimeError("cannot read Compose configuration fingerprint") from exc
            identities[name] = {
                "id": row["id"], "image": row["image"],
                "mounts": sorted(mounts, key=lambda m: m.get("Destination", "")),
                "project": row.get("project"), "working_dir": row.get("working_dir"),
                "config_files": row.get("config_files"), "config_hashes": config_hashes,
            }
        if set(identities) != {*CONSUMERS, METADB}:
            raise RuntimeError("missing required shared consumer or metabase")
        return {
            "consumers": {name: identities[name] for name in CONSUMERS},
            "metadb": identities[METADB],
            "dormant_consumers": dormant_consumers,
            "other_shared_mounts": {name: sorted(mounts, key=lambda m: (m["source"], m["destination"]))
                                    for name, mounts in other_shared_mounts.items()},
        }

    def _snapshot(self, deadline: float | None = None, *, recovery: bool = False, policy: str | None = None) -> dict[str, Any]:
        policy = self.import_error_policy if policy is None else policy
        protected = legacy_errors.protected_hashes(self.root, policy)
        identity = self._inventory(deadline, recovery=recovery)
        output = self._run(
            ["docker", "exec", METADB, "psql", "-X", "-qAt", "-U", "airflow", "-d", "airflow",
             "-v", "ON_ERROR_STOP=1", "-c", SNAPSHOT_SQL], deadline=deadline,
        )
        try:
            data = json.loads(output)
            if not isinstance(data, dict) or set(data) != {"dags", "work", "import_errors"}:
                raise ValueError("invalid snapshot keys")
            work = classify_work(data["work"])
            data["work"] = work
            data["active"] = work["active"]
            errors = data["import_errors"]
            if not isinstance(errors, list):
                raise ValueError("invalid import errors")
            details = []
            for error in errors:
                if (not isinstance(error, dict) or set(error) != {"filename", "stacktrace"}
                        or not isinstance(error["filename"], str) or not error["filename"]
                        or not isinstance(error["stacktrace"], str) or not error["stacktrace"]):
                    raise ValueError("invalid import error record")
                details.append({"filename": error["filename"],
                                "sha256": hashlib.sha256(error["stacktrace"].encode()).hexdigest()})
            if len({e["filename"] for e in details}) != len(details):
                raise ValueError("duplicate import error filename")
            data["import_errors"] = len(details)
            data["import_error_details"] = sorted(details, key=lambda e: e["filename"])
            if not isinstance(data["dags"], dict) or not data["dags"]:
                raise ValueError("empty active DagBag")
            for dag_id, dag in data["dags"].items():
                if not isinstance(dag_id, str) or not dag_id or not isinstance(dag, dict) or set(dag) != {"paused", "errors", "parsed", "fileloc"}:
                    raise ValueError("invalid DAG record")
                if type(dag["paused"]) is not bool or type(dag["errors"]) is not bool:
                    raise ValueError("invalid DAG boolean")
                if not isinstance(dag["fileloc"], str) or not dag["fileloc"].startswith("/"):
                    raise ValueError("invalid DAG file location")
                parsed = dag["parsed"]
                if parsed is not None and (type(parsed) not in (int, float) or not math.isfinite(parsed)):
                    raise ValueError("invalid parse timestamp")
                dag["parsed"] = float(parsed) if parsed is not None else 0.0
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimeError("invalid read-only metabase snapshot") from exc
        output = self._run(["pgrep", "-f", DRIVER_PATTERN], deadline=deadline, empty=True)
        pids = output.split()
        if any(not re.fullmatch(r"[1-9][0-9]*", pid) for pid in pids):
            raise RuntimeError("invalid external driver PID response")
        processes = classify_processes(sorted({int(pid) for pid in pids}), self._containers, self.root)
        return {"identity": identity, **data,
                "import_error_policy": policy, "import_error_protected": protected,
                "drivers": [p["pid"] for p in processes if p["blocks"]], "processes": processes}

    def snapshot(self) -> dict[str, Any]:
        result = self._snapshot()
        legacy_errors.validate(result)
        return result

    def writer_hashes(self, expected: str) -> dict[str, str]:
        """Read the file each mounted consumer actually sees; never run Python there."""
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise RuntimeError("invalid expected writer fingerprint")
        result = {}
        for name in CONSUMERS:
            output = self._run(["docker", "exec", name, "sha256sum", "/opt/airflow/scrapers/base/iceberg_writer.py"])
            fields = output.strip().split()
            if fields != [expected, "/opt/airflow/scrapers/base/iceberg_writer.py"]:
                raise RuntimeError(f"mounted writer fingerprint mismatch: {name}")
            result[name] = fields[0]
        return result

    def _preflight(self, baseline: dict[str, Any], deadline: float | None = None, *, allow_import_errors: bool = False) -> dict[str, Any]:
        legacy_errors.validate(baseline)
        fresh = self._snapshot(deadline, recovery=allow_import_errors,
                               policy=baseline.get("import_error_policy", "none"))
        if fresh["identity"] != baseline.get("identity"):
            raise RuntimeError("shared container/mount/config identity drift")
        expected = baseline.get("dags", {})
        if set(fresh["dags"]) != set(expected):
            raise RuntimeError("active DagBag membership changed")
        for dag_id, dag in fresh["dags"].items():
            if dag["fileloc"] != expected[dag_id].get("fileloc"):
                raise RuntimeError("active DAG file location changed")
            if dag["paused"] != expected[dag_id].get("paused"):
                raise RuntimeError("DAG pause state changed")
            if dag["paused"] is not True:
                raise RuntimeError("maintenance window requires every active shared DAG paused")
            if dag["errors"] and not allow_import_errors:
                raise RuntimeError("DAG reports import errors")
        if not allow_import_errors:
            legacy_errors.validate(fresh)
        if fresh["active"] or fresh["drivers"]:
            raise RuntimeError("shared runtime has unfinished work, import errors, or external drivers")
        return fresh

    def preflight(self, baseline: dict[str, Any]) -> dict[str, Any]:
        return self._preflight(baseline)

    def rollback_preflight(self, baseline: dict[str, Any]) -> dict[str, Any]:
        """Permit writer-caused import/health errors while preserving the idle window."""
        return self._preflight(baseline, allow_import_errors=True)

    def postflight(self, baseline: dict[str, Any], since: float, timeout: float = 420) -> dict[str, Any]:
        if type(since) not in (int, float) or type(timeout) not in (int, float) or not math.isfinite(since) or not math.isfinite(timeout) or timeout <= 0:
            raise RuntimeError("invalid postflight time bounds")
        deadline = time.monotonic() + timeout
        while True:
            fresh = self._preflight(baseline, deadline)
            if time.monotonic() >= deadline:
                raise RuntimeError("fresh scheduler parse deadline exceeded")
            if all(dag["parsed"] > since + 60 for dag in fresh["dags"].values()):
                return fresh
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("fresh scheduler parse deadline exceeded")
            time.sleep(min(20.0, remaining))
