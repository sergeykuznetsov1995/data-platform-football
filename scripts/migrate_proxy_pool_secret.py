#!/usr/bin/env python3
"""Convert the legacy proxy pool into a no-clobber Compose secret env file.

The command never prints proxy endpoints or credentials.  Planning reads and
validates the source but performs no writes.  Applying requires the exact
canonical JSON SHA-256 shown by the plan.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from typing import Any


MAX_SOURCE_BYTES = 1024 * 1024
MAX_ENTRIES = 1000
DEFAULT_SOURCE = Path("/root/data-platform-football/proxys.txt")
DEFAULT_OUTPUT = Path("/root/data-platform-football/.env.proxy-pool")
_DNS_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class SecretMigrationError(ValueError):
    """A redaction-safe migration failure."""


def _read_regular_file(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SecretMigrationError(f"source is unavailable: {path}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SecretMigrationError("source must be a regular file")
        if metadata.st_size <= 0 or metadata.st_size > MAX_SOURCE_BYTES:
            raise SecretMigrationError("source size is outside the allowed range")
        chunks: list[bytes] = []
        remaining = MAX_SOURCE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > MAX_SOURCE_BYTES:
            raise SecretMigrationError("source exceeds the size limit")
        return payload
    finally:
        os.close(descriptor)


def _control(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _host(value: str, *, line_number: int) -> str:
    if not value or value != value.strip() or _control(value) or len(value) > 253:
        raise SecretMigrationError(f"line {line_number}: invalid host")
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError:
        pass
    try:
        ascii_host = value.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise SecretMigrationError(f"line {line_number}: invalid host") from exc
    labels = ascii_host.split(".")
    if (
        ascii_host.endswith(".")
        or not labels
        or len(ascii_host) > 253
        or any(not _DNS_LABEL.fullmatch(label) for label in labels)
    ):
        raise SecretMigrationError(f"line {line_number}: invalid host")
    return ascii_host


def _credential(
    value: str,
    *,
    line_number: int,
    field: str,
    max_length: int,
) -> str:
    if not value or len(value) > max_length or _control(value):
        raise SecretMigrationError(f"line {line_number}: invalid {field}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SecretMigrationError(f"line {line_number}: invalid {field}") from exc
    if field == "username" and ":" in value:
        raise SecretMigrationError(f"line {line_number}: invalid username")
    return value


def parse_legacy_pool(payload: bytes) -> tuple[dict[str, Any], ...]:
    """Strictly parse host:port:username:password without exposing values."""

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SecretMigrationError("source is not valid UTF-8") from exc
    records: list[dict[str, Any]] = []
    identities: set[tuple[str, int, str]] = set()
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":", 3)
        if len(parts) != 4:
            raise SecretMigrationError(
                f"line {line_number}: expected host:port:username:password"
            )
        host = _host(parts[0], line_number=line_number)
        try:
            port = int(parts[1])
        except ValueError as exc:
            raise SecretMigrationError(f"line {line_number}: invalid port") from exc
        if not 1 <= port <= 65535:
            raise SecretMigrationError(f"line {line_number}: invalid port")
        username = _credential(
            parts[2],
            line_number=line_number,
            field="username",
            max_length=1024,
        )
        password = _credential(
            parts[3],
            line_number=line_number,
            field="password",
            max_length=4096,
        )
        identity = (host, port, username)
        if identity in identities:
            raise SecretMigrationError(
                f"line {line_number}: duplicate endpoint identity"
            )
        identities.add(identity)
        records.append(
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
            }
        )
        if len(records) > MAX_ENTRIES:
            raise SecretMigrationError("source contains too many entries")
    if not records:
        raise SecretMigrationError("source contains no proxy entries")
    return tuple(records)


def canonical_pool_json(records: tuple[dict[str, Any], ...]) -> str:
    raw = json.dumps(
        list(records),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    # Compose single-quoted values are literal. JSON escaping the apostrophe
    # keeps the wrapper unambiguous while json.loads restores the credential.
    safe = raw.replace("'", "\\u0027").replace("$", "\\u0024")
    if len(safe.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise SecretMigrationError("canonical proxy JSON exceeds the size limit")
    decoded = json.loads(safe)
    if decoded != list(records):
        raise SecretMigrationError("canonical proxy JSON round-trip failed")
    return safe


def validate_runtime_parser(
    canonical: str,
    records: tuple[dict[str, Any], ...],
) -> bool:
    """Require the exact deployed proxy parser to accept the migration."""

    parser_path = Path(__file__).resolve().parent / "proxy_filter" / "filter_proxy.py"
    spec = importlib.util.spec_from_file_location(
        "_proxy_filter_migration_contract",
        parser_path,
    )
    if spec is None or spec.loader is None:
        raise SecretMigrationError("runtime proxy parser is unavailable")
    filter_proxy = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(filter_proxy)
    except Exception:  # noqa: BLE001 - redact import/runtime internals
        raise SecretMigrationError("runtime proxy parser is unavailable") from None
    parser = getattr(filter_proxy, "_parse_proxy_pool_json", None)
    if parser is None:
        raise SecretMigrationError("runtime proxy parser is unavailable")
    try:
        parsed = tuple(parser(canonical))
    except Exception:  # noqa: BLE001 - redact runtime parser internals
        raise SecretMigrationError(
            "runtime proxy parser rejected canonical JSON"
        ) from None
    if parsed != records:
        raise SecretMigrationError("runtime proxy parser normalisation drift")
    return True


def build_plan(source: Path, output: Path) -> dict[str, Any]:
    payload = _read_regular_file(source)
    records = parse_legacy_pool(payload)
    canonical = canonical_pool_json(records)
    canonical_sha256 = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    source_path = str(source.resolve())
    output_path = str(output.resolve(strict=False))
    bound = {
        "canonical_sha256": canonical_sha256,
        "entry_count": len(records),
        "output": output_path,
        "source": source_path,
        "source_size_bytes": len(payload),
    }
    plan_sha256 = hashlib.sha256(
        json.dumps(bound, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "status": "planned",
        **bound,
        "plan_sha256": plan_sha256,
        "runtime_parser_validated": validate_runtime_parser(canonical, records),
        "canonical_json": canonical,
    }


def _atomic_no_clobber(path: Path, content: bytes) -> None:
    if path.is_symlink() or path.exists():
        raise SecretMigrationError(f"refusing to overwrite output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        os.unlink(temporary)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def apply_plan(
    plan: dict[str, Any],
    *,
    expected_sha256: str,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    if not _DIGEST.fullmatch(expected_sha256):
        raise SecretMigrationError("expected SHA-256 must be a lowercase digest")
    if expected_sha256 != plan["canonical_sha256"]:
        raise SecretMigrationError("canonical proxy JSON SHA-256 drift")
    if not _DIGEST.fullmatch(expected_plan_sha256):
        raise SecretMigrationError("expected plan SHA-256 must be a lowercase digest")
    if expected_plan_sha256 != plan["plan_sha256"]:
        raise SecretMigrationError("proxy secret migration plan SHA-256 drift")
    content = f"PROXY_POOL_JSON='{plan['canonical_json']}'\n".encode("utf-8")
    _atomic_no_clobber(Path(plan["output"]), content)
    return {key: value for key, value in plan.items() if key != "canonical_json"} | {
        "status": "applied",
        "mode": "0600",
    }


def _public(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if key != "canonical_json"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--expected-sha256", default="")
    parser.add_argument("--expected-plan-sha256", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        plan = build_plan(args.source, args.output)
        result = (
            apply_plan(
                plan,
                expected_sha256=args.expected_sha256,
                expected_plan_sha256=args.expected_plan_sha256,
            )
            if args.apply
            else _public(plan)
        )
    except (OSError, SecretMigrationError) as exc:
        print(f"secret migration error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
