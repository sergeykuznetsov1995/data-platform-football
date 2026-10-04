"""Conservative, argument-free classification of pgrep driver candidates.

A process name is never evidence of isolation. Exceptions require an observed
executable or a running Docker container with a separate mounted writer. This
is an observation, not a lock against launches or mount changes after the read.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import stat
from typing import Any

PROC_ROOT = Path('/proc')
TAIL = Path('/usr/bin/tail')
WRITER = Path('/opt/airflow/scrapers/base/iceberg_writer.py')
MAX_BINARY_BYTES = 8 * 1024 * 1024
_DOCKER_ID = re.compile(r'(?:^|/)(?:docker-([0-9a-f]{64})\.scope|docker/([0-9a-f]{64}))(?:/|$)')
_PYTHON = re.compile(r'python(?:[23](?:\.\d+)*)?')
_ISOLATED_CAPS = {'CHOWN', 'SETGID', 'SETUID', 'SYS_PTRACE'}
# Old processes can keep the genuine tail inode after a package upgrade.
# Verified from Ubuntu's signed snapshot, not learned from a running process:
# https://snapshot.ubuntu.com/ubuntu/20260801T000000Z/dists/noble-updates/InRelease
# Signing fingerprint: F6ECB3762474EDA9D21B7022871920D1991BC93C
# main/binary-amd64/Packages.xz SHA256:
# acfd8cc6ad032438e807b5c95d64535de2cb16b6148c343467948aa38e7bfc86
# pool/main/c/coreutils/coreutils_9.4-3ubuntu6.2_amd64.deb SHA256:
# d623abbb182acfead9d6352252e687674b6e5b3b79d42aa1324b08a3a6e1131b
# Only its extracted usr/bin/tail binary is eligible (SHA256 below).
_PACKAGED_TAIL_SHA256 = frozenset({
    '871571a28ad9483140d92027915a263311462f4ca96e0451e38d6ebcce370c0b',
})


def _identity(path: Path) -> tuple[int, int]:
    value = path.stat()
    return value.st_dev, value.st_ino


def _start(pid: int) -> int:
    # comm can contain spaces and closing parentheses; fields after its last ')'
    # start at field 3, and starttime is field 22.
    value = (PROC_ROOT / str(pid) / 'stat').read_text()
    fields = value[value.rindex(')') + 2:].split()
    started = int(fields[19])
    if started <= 0:
        raise ValueError('invalid starttime')
    return started


def _process(pid: int) -> dict[str, Any]:
    directory = PROC_ROOT / str(pid)
    started = _start(pid)
    result = {
        'start': started,
        'exe': os.readlink(directory / 'exe'),
        'exe_identity': _identity(directory / 'exe'),
        'root': _identity(directory / 'root'),
        'mnt': _identity(directory / 'ns/mnt'),
        'pid_ns': _identity(directory / 'ns/pid'),
        'cgroup': (directory / 'cgroup').read_text(),
    }
    if _start(pid) != started:
        raise ValueError('process changed')
    return result


def _digest(path: Path) -> bytes:
    with path.open('rb') as stream:
        value = stream.read(MAX_BINARY_BYTES + 1)
    if len(value) > MAX_BINARY_BYTES:
        raise ValueError('oversized executable')
    return hashlib.sha256(value).digest()


def _trusted_tail(pid: int, process: dict[str, Any]) -> bool:
    # An argv[0], comm, or the spelling of a deleted exe link is insufficient.
    trusted = TAIL.stat()
    if not stat.S_ISREG(trusted.st_mode) or trusted.st_uid != 0 or trusted.st_mode & 0o022:
        return False
    exe = PROC_ROOT / str(pid) / 'exe'
    if process['exe_identity'] == (trusted.st_dev, trusted.st_ino):
        return True
    executable_hash = _digest(exe)
    return executable_hash.hex() in _PACKAGED_TAIL_SHA256 or executable_hash == _digest(TAIL)


def _container_id(cgroup: str) -> str | None:
    identities = set()
    for line in cgroup.splitlines():
        fields = line.split(':', 2)
        if len(fields) != 3:
            return None
        match = _DOCKER_ID.search(fields[2])
        if match:
            identities.add(match.group(1) or match.group(2))
    return next(iter(identities)) if len(identities) == 1 else None


def _overlaps(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def _separate_writer(pid: int, process: dict[str, Any], row: dict[str, Any], root: Path) -> bool:
    if row.get('status') != 'running' or row.get('privileged') is not False:
        return False
    if row.get('pid_mode') != '' or 'cap_add' not in row:
        return False
    capabilities = row['cap_add']
    if capabilities is not None and (
        not isinstance(capabilities, list)
        or any(not isinstance(cap, str) or cap.removeprefix('CAP_') not in _ISOLATED_CAPS for cap in capabilities)
    ):
        return False
    init_pid = row.get('init_pid')
    if type(init_pid) is not int or init_pid <= 0:
        return False
    initial = _process(init_pid)
    if _container_id(initial['cgroup']) != row['id']:
        return False
    if any(process[key] != initial[key] for key in ('root', 'mnt', 'pid_ns')):
        return False
    if (process['root'] == _identity(Path('/'))
        or process['mnt'] == _identity(PROC_ROOT / 'self/ns/mnt')
        or process['pid_ns'] == _identity(PROC_ROOT / 'self/ns/pid')):
        return False

    shared = root.resolve(strict=True)
    writer_path = shared / 'scrapers/base/iceberg_writer.py'
    shared_writer = writer_path.resolve(strict=True)
    protected = [*(shared / part for part in ('scrapers', 'dags', 'scripts')), shared_writer]
    # Bind aliases are not resolved by realpath. Every ancestor can expose the
    # writer, including scrapers/base and broad mounts such as /root. Include
    # both the configured and canonical paths if the writer is a symlink.
    protected_ids = {
        _identity(path) for path in (*protected, *writer_path.parents, *shared_writer.parents)
    }
    mounts = row.get('mounts')
    if not isinstance(mounts, list) or not mounts:
        return False
    writer_mounts = []
    destinations = set()
    for mount in mounts:
        if not isinstance(mount, dict) or mount.get('Type') not in ('bind', 'volume'):
            return False
        source_text, destination_text = mount.get('Source'), mount.get('Destination')
        if not isinstance(source_text, str) or not isinstance(destination_text, str):
            return False
        source, destination = Path(source_text), Path(destination_text)
        if not source.is_absolute() or not destination.is_absolute() or '..' in destination.parts:
            return False
        if destination in destinations:
            return False
        destinations.add(destination)
        source = source.resolve(strict=True)
        source_stat = source.stat()
        if any(_overlaps(source, path) for path in protected) or _identity(source) in protected_ids:
            return False
        if stat.S_ISSOCK(source_stat.st_mode) or source.name.endswith('.sock'):
            return False
        if any(_overlaps(source, Path(path)) for path in ('/run', '/var/run', '/proc', '/sys', '/dev')):
            return False
        if destination == WRITER or destination in WRITER.parents:
            writer_mounts.append((destination, source, mount.get('Type')))
    if not writer_mounts:
        return False
    destination, source, kind = max(writer_mounts, key=lambda value: len(value[0].parts))
    if kind != 'bind':
        return False
    mapped_writer = (source / WRITER.relative_to(destination)).resolve(strict=True)
    observed_writer = PROC_ROOT / str(pid) / 'root' / WRITER.relative_to('/')
    writer_stat = observed_writer.stat()
    if not stat.S_ISREG(writer_stat.st_mode):
        return False
    shared_identity = _identity(shared_writer)
    mapped_identity = _identity(mapped_writer)
    if mapped_writer == shared_writer or mapped_identity == shared_identity:
        return False
    if _identity(observed_writer) != mapped_identity:
        return False
    # Validate both process namespace and its container anchor after mount reads.
    return (
        _process(init_pid) == initial
        and _identity(observed_writer) == mapped_identity
        and _identity(mapped_writer) == mapped_identity
        and _identity(shared_writer) == shared_identity
    )


def classify_processes(pids: list[int], containers: list[dict], root: Path) -> list[dict]:
    """Return sanitized evidence; callers must block on every ``blocks=True``.

    Required inspect fields are id, status, mounts, init_pid, privileged,
    pid_mode and cap_add. Missing, unreadable, ambiguous or changing evidence
    blocks the window. No command line, environment, or arbitrary /proc text is
    returned (including in error messages).
    """
    if any(type(pid) is not int or pid <= 0 for pid in pids):
        raise ValueError('invalid driver PID')
    evidence = []
    for pid in sorted(set(pids)):
        result = {'pid': pid, 'blocks': True, 'reason': 'unverified_process'}
        try:
            process = _process(pid)
            result['start_time'] = process['start']
            if _trusted_tail(pid, process):
                result.update(blocks=False, reason='verified_tail')
            else:
                container_id = _container_id(process['cgroup'])
                rows = [row for row in containers if row.get('id') == container_id] if container_id else []
                # Shells and wrappers can deliver across containers. Only source
                # Python workers are eligible for the isolated-writer exception.
                executable = process['exe'].removesuffix(' (deleted)')
                if len(rows) == 1 and _PYTHON.fullmatch(Path(executable).name):
                    if _separate_writer(pid, process, rows[0], root):
                        result.update(blocks=False, reason='separate_container_writer', container_id=container_id)
            if _process(pid) != process:
                result.update(blocks=True, reason='process_changed')
        except (OSError, ValueError, IndexError, KeyError, TypeError):
            # A PID absent before any successful metadata read has exited. A
            # failure after observing it cannot prove the same process exited.
            if 'start_time' not in result and not os.path.lexists(PROC_ROOT / str(pid)):
                result.update(blocks=False, reason='exited')
            else:
                result.update(blocks=True, reason='unreadable_or_changed_process')
        evidence.append(result)
    return evidence
