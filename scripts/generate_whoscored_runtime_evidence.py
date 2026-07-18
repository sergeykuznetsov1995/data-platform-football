#!/usr/bin/env python3
"""Deterministically check or refresh WhoScored runtime evidence.

The runtime lock is derived from the explicit ``EXPECTED_RUNTIME_FILES`` tuple
without importing repository code.  The three image trust roots are then
derived from the exact source and newly rendered lock bytes.  Publishing the
lock first keeps an interrupted refresh fail closed: old trust roots cannot
authenticate new lock bytes.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
import sys
from typing import Mapping, Sequence


EXIT_CONFIG = 78
SOURCE_RELATIVE = PurePosixPath("scrapers/whoscored/runtime_contract.py")
LOCK_RELATIVE = PurePosixPath("scrapers/whoscored/runtime_contract.lock")
PARSER_RELATIVE = PurePosixPath("scrapers/whoscored/parsers.py")
REPOSITORY_RELATIVE = PurePosixPath("scrapers/whoscored/repository.py")
REPORT_RELATIVE = PurePosixPath("dags/scripts/run_whoscored_scraper.py")
TRUST_ROOTS = (
    (
        PurePosixPath(
            "docker/images/airflow/whoscored-runtime-trust-root-generic"
        ),
        "generic-v1",
    ),
    (
        PurePosixPath("docker/images/airflow/whoscored-runtime-trust-root-test"),
        "test-v1",
    ),
    (
        PurePosixPath(
            "docker/images/airflow/whoscored-runtime-trust-root-production"
        ),
        "production-v1",
    ),
)
LOCK_SCHEMA_VERSION = 1
_DIGEST = re.compile(r"[0-9a-f]{64}")
_IDENTITY_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_gid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


class EvidenceError(RuntimeError):
    """Raised when deterministic runtime evidence cannot be established."""


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(metadata, field) for field in _IDENTITY_FIELDS)


def _validated_relative(value: str, *, label: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or value != path.as_posix()
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise EvidenceError(f"{label} is not a canonical relative path: {value!r}")
    return path


def _open_root(root: Path) -> int:
    absolute = Path(os.path.abspath(root))
    try:
        descriptor = os.open(
            absolute,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
    except OSError as exc:
        raise EvidenceError(f"runtime root is missing or unsafe: {absolute}") from exc
    metadata = os.fstat(descriptor)
    if metadata.st_mode & 0o022:
        os.close(descriptor)
        raise EvidenceError(f"runtime root is group/world writable: {absolute}")
    return descriptor


def _read_relative(root_fd: int, relative: PurePosixPath, *, label: str) -> bytes:
    directory_fd = os.dup(root_fd)
    file_fd = -1
    try:
        for component in relative.parts[:-1]:
            child_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = child_fd
            directory = os.fstat(directory_fd)
            if directory.st_mode & 0o022:
                raise EvidenceError(f"{label} has a writable parent: {relative}")
        file_fd = os.open(
            relative.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=directory_fd,
        )
        before = os.fstat(file_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
        ):
            raise EvidenceError(f"{label} is not a protected regular file: {relative}")
        chunks: list[bytes] = []
        while chunk := os.read(file_fd, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(file_fd)
        entry = os.stat(relative.name, dir_fd=directory_fd, follow_symlinks=False)
    except EvidenceError:
        raise
    except OSError as exc:
        raise EvidenceError(f"{label} is missing or unreadable: {relative}") from exc
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        os.close(directory_fd)
    if _identity(before) != _identity(after) or _identity(after) != _identity(entry):
        raise EvidenceError(f"{label} changed while it was read: {relative}")
    return b"".join(chunks)


def _expected_runtime_files(source: bytes) -> tuple[PurePosixPath, ...]:
    try:
        tree = ast.parse(source, filename=SOURCE_RELATIVE.as_posix())
    except (SyntaxError, ValueError) as exc:
        raise EvidenceError("runtime contract source is not valid Python") from exc
    declarations: list[ast.AST] = []
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(
            isinstance(target, ast.Name) and target.id == "EXPECTED_RUNTIME_FILES"
            for target in targets
        ):
            value = node.value
            if value is not None:
                declarations.append(value)
    if len(declarations) != 1:
        raise EvidenceError("runtime file allowlist must have one static declaration")
    try:
        values = ast.literal_eval(declarations[0])
    except (ValueError, TypeError, SyntaxError) as exc:
        raise EvidenceError("runtime file allowlist is not a literal tuple") from exc
    if not isinstance(values, tuple) or any(not isinstance(item, str) for item in values):
        raise EvidenceError("runtime file allowlist is not a literal string tuple")
    paths = tuple(
        _validated_relative(item, label="runtime file") for item in values
    )
    canonical = tuple(sorted(paths, key=PurePosixPath.as_posix))
    if paths != canonical or len(paths) != len(set(paths)):
        raise EvidenceError("runtime file allowlist is not sorted and unique")
    if SOURCE_RELATIVE not in paths:
        raise EvidenceError("runtime file allowlist does not bind its own source")
    return paths


def _static_assignment(source: bytes, name: str, *, label: str) -> ast.AST:
    try:
        tree = ast.parse(source, filename=label)
    except (SyntaxError, ValueError) as exc:
        raise EvidenceError(f"{label} is not valid Python") from exc
    declarations: list[ast.AST] = []
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            if node.value is not None:
                declarations.append(node.value)
    if len(declarations) != 1:
        raise EvidenceError(f"{label} must declare {name} exactly once")
    return declarations[0]


def _literal_assignment(source: bytes, name: str, *, label: str) -> object:
    node = _static_assignment(source, name, label=label)
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError) as exc:
        raise EvidenceError(f"{label} {name} is not a static literal") from exc


def _lock_metadata(captured: Mapping[PurePosixPath, bytes]) -> dict[str, object]:
    parser_version = _literal_assignment(
        captured[PARSER_RELATIVE],
        "PARSER_VERSION",
        label=PARSER_RELATIVE.as_posix(),
    )
    report_schema_version = _literal_assignment(
        captured[REPORT_RELATIVE],
        "REPORT_SCHEMA_VERSION",
        label=REPORT_RELATIVE.as_posix(),
    )
    business_tables = _static_assignment(
        captured[REPOSITORY_RELATIVE],
        "WHOSCORED_BUSINESS_TABLES",
        label=REPOSITORY_RELATIVE.as_posix(),
    )
    if not isinstance(parser_version, str) or not parser_version:
        raise EvidenceError("PARSER_VERSION must be a non-empty string literal")
    if type(report_schema_version) is not int or report_schema_version < 1:
        raise EvidenceError("REPORT_SCHEMA_VERSION must be a positive integer literal")
    if not isinstance(business_tables, ast.Tuple) or not business_tables.elts:
        raise EvidenceError("WHOSCORED_BUSINESS_TABLES must be a non-empty tuple")
    return {
        "schema_version": LOCK_SCHEMA_VERSION,
        "parser_version": parser_version,
        "report_schema_version": report_schema_version,
        "business_dataset_count": len(business_tables.elts),
    }


def _lock_bytes(files: Mapping[str, str], metadata: Mapping[str, object]) -> bytes:
    if any(_DIGEST.fullmatch(digest) is None for digest in files.values()):
        raise EvidenceError("runtime lock contains an invalid digest")
    document = {**metadata, "files": dict(files)}
    return (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _trust_root_bytes(
    *, runtime_class: str, source_sha256: str, lock_sha256: str
) -> bytes:
    if (
        runtime_class not in {"generic-v1", "production-v1", "test-v1"}
        or _DIGEST.fullmatch(source_sha256) is None
        or _DIGEST.fullmatch(lock_sha256) is None
    ):
        raise EvidenceError("runtime trust-root input is invalid")
    return (
        "schema_version=1\n"
        f"runtime_class={runtime_class}\n"
        f"runtime_contract_source_sha256={source_sha256}\n"
        f"runtime_contract_lock_sha256={lock_sha256}\n"
    ).encode("ascii")


def render_evidence(root: Path) -> dict[PurePosixPath, bytes]:
    """Render every generated file from one stable snapshot of the closure."""

    root_fd = _open_root(root)
    try:
        source = _read_relative(
            root_fd, SOURCE_RELATIVE, label="runtime contract source"
        )
        paths = _expected_runtime_files(source)
        captured = {
            relative: source
            if relative == SOURCE_RELATIVE
            else _read_relative(root_fd, relative, label="runtime file")
            for relative in paths
        }
    finally:
        os.close(root_fd)
    for required in (PARSER_RELATIVE, REPOSITORY_RELATIVE, REPORT_RELATIVE):
        if required not in captured:
            raise EvidenceError(
                f"runtime file allowlist does not bind metadata source: {required}"
            )
    files = {
        relative.as_posix(): hashlib.sha256(payload).hexdigest()
        for relative, payload in captured.items()
    }
    lock = _lock_bytes(files, _lock_metadata(captured))
    source_sha256 = hashlib.sha256(source).hexdigest()
    lock_sha256 = hashlib.sha256(lock).hexdigest()
    rendered = {LOCK_RELATIVE: lock}
    for relative, runtime_class in TRUST_ROOTS:
        rendered[relative] = _trust_root_bytes(
            runtime_class=runtime_class,
            source_sha256=source_sha256,
            lock_sha256=lock_sha256,
        )
    return rendered


def check_evidence(root: Path, rendered: Mapping[PurePosixPath, bytes]) -> bool:
    root_fd = _open_root(root)
    try:
        return all(
            _read_relative(root_fd, relative, label="runtime evidence") == expected
            for relative, expected in rendered.items()
        )
    finally:
        os.close(root_fd)


def _replace_relative(
    root_fd: int, relative: PurePosixPath, payload: bytes
) -> None:
    parent_fd = os.dup(root_fd)
    temporary_fd = -1
    temporary_name = f".{relative.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    try:
        for component in relative.parts[:-1]:
            child_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            os.close(parent_fd)
            parent_fd = child_fd
            if os.fstat(parent_fd).st_mode & 0o022:
                raise EvidenceError(
                    f"runtime evidence has a writable parent: {relative}"
                )
        current = os.stat(relative.name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or current.st_mode & 0o022
        ):
            raise EvidenceError(f"runtime evidence output is unsafe: {relative}")
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent_fd,
        )
        offset = 0
        while offset < len(payload):
            written = os.write(temporary_fd, payload[offset:])
            if written <= 0:
                raise EvidenceError(f"cannot write runtime evidence: {relative}")
            offset += written
        os.fchmod(temporary_fd, 0o644)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = -1
        os.replace(
            temporary_name,
            relative.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        os.fsync(parent_fd)
    except EvidenceError:
        raise
    except OSError as exc:
        raise EvidenceError(f"cannot publish runtime evidence: {relative}") from exc
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if parent_fd >= 0:
            try:
                os.unlink(temporary_name, dir_fd=parent_fd)
            except OSError:
                pass
            os.close(parent_fd)


def write_evidence(root: Path, rendered: Mapping[PurePosixPath, bytes]) -> None:
    """Publish lock first and trust roots last, leaving interruptions blocked."""

    expected_order = (LOCK_RELATIVE, *(relative for relative, _ in TRUST_ROOTS))
    if tuple(rendered) != expected_order:
        raise EvidenceError("runtime evidence output set or order is invalid")
    root_fd = _open_root(root)
    try:
        for relative in expected_order:
            _replace_relative(root_fd, relative, rendered[relative])
    finally:
        os.close(root_fd)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if (
            not sys.flags.isolated
            or not sys.flags.no_site
            or not sys.flags.ignore_environment
        ):
            raise EvidenceError("runtime evidence requires Python -I -S")
        rendered = render_evidence(args.root)
        current = check_evidence(args.root, rendered)
        if args.write and not current:
            write_evidence(args.root, rendered)
        verified = render_evidence(args.root)
        if verified != rendered:
            raise EvidenceError("runtime source closure changed during evidence check")
        current = check_evidence(args.root, verified)
        if not current:
            raise EvidenceError("checked-in runtime evidence is stale")
    except EvidenceError as exc:
        print(f"WhoScored runtime evidence blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    report = {
        "file_count": len(json.loads(rendered[LOCK_RELATIVE])["files"]),
        "lock_sha256": hashlib.sha256(rendered[LOCK_RELATIVE]).hexdigest(),
        "status": "current-v1",
    }
    sys.stdout.write(
        json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
