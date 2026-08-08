#!/usr/bin/env python3
"""Probe one Decodo sticky exit without contacting FBref or exposing secrets.

The command performs six requests to Decodo's IP-check endpoint over two
hours.  Only a SHA-256 digest of the observed exit is retained.  Proxy
credentials stay in the protected input file and are never rendered.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import stat
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote, urlsplit


DECODO_IP_CHECK_URL = "https://ip.decodo.com/json"
PROBE_OFFSETS_SECONDS = (0, 600, 1800, 3600, 5400, 7200)
STICKY_CANDIDATE_MINUTES = (60, 90, 120)
PROVIDER_STICKY_MINUTES = 120
LOCAL_LEASE_MINUTES = 115
SCHEDULE_TOLERANCE_SECONDS = 60


class CanaryConfigurationError(ValueError):
    """The canary input is unsafe or incomplete."""


class CanaryProbeError(RuntimeError):
    """A probe failed; its underlying error is deliberately redacted."""


@dataclass(frozen=True)
class ProbeObservation:
    offset_seconds: int
    elapsed_seconds: float
    observed_at: str
    exit_hash: str


def hash_exit_identity(ip: str) -> str:
    """Return a stable hash of a normalized IP address."""

    try:
        normalized = ipaddress.ip_address(str(ip).strip()).compressed
    except ValueError:
        raise CanaryProbeError("IP-check response did not contain an IP") from None
    return hashlib.sha256(normalized.encode("ascii")).hexdigest()


def _proxy_url(entry: str) -> str:
    candidate = entry.strip()
    if "://" in candidate:
        parsed = urlsplit(candidate)
        try:
            port = parsed.port
        except ValueError:
            raise CanaryConfigurationError("proxy entry is invalid") from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or port is None
            or not parsed.username
            or parsed.password is None
        ):
            raise CanaryConfigurationError("proxy entry is invalid")
        return candidate

    parts = candidate.split(":", 3)
    if len(parts) != 4:
        raise CanaryConfigurationError("proxy entry is invalid")
    host, port_text, username, password = parts
    try:
        port = int(port_text)
    except ValueError:
        raise CanaryConfigurationError("proxy entry is invalid") from None
    if not host or not username or not password or not 1 <= port <= 65535:
        raise CanaryConfigurationError("proxy entry is invalid")
    return (
        f"http://{quote(username, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}"
    )


def read_single_proxy_url(path: Path) -> str:
    """Read exactly one protected proxy entry without echoing its contents."""

    proxy_file = Path(path)
    try:
        metadata = proxy_file.lstat()
    except OSError:
        raise CanaryConfigurationError("proxy file is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise CanaryConfigurationError("proxy file must be a regular file")
    # 0640 and 0600 are accepted.  Group write/execute and every world bit are
    # rejected; credentials must not be editable by another runtime user.
    if stat.S_IMODE(metadata.st_mode) & 0o027:
        raise CanaryConfigurationError("proxy file permissions are unsafe")
    try:
        entries = [
            line.strip()
            for line in proxy_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except (OSError, UnicodeError):
        raise CanaryConfigurationError("proxy file is unreadable") from None
    if len(entries) != 1:
        raise CanaryConfigurationError(
            "proxy file must contain exactly one entry"
        )
    return _proxy_url(entries[0])


def _default_probe(*, proxy_url: str, check_url: str) -> str:
    """Perform one bounded IP-check request through the supplied proxy."""

    session = None
    try:
        import requests

        session = requests.Session()
        session.trust_env = False
        response = session.get(
            check_url,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=(10, 30),
            allow_redirects=False,
            headers={"Accept": "application/json", "User-Agent": "fbref-canary/1"},
        )
        response.raise_for_status()
        if len(response.content) > 16 * 1024:
            raise ValueError("oversized response")
        try:
            payload: Any = response.json()
        except ValueError:
            payload = response.text.strip()
        if isinstance(payload, Mapping):
            for key in ("ip", "proxy", "address"):
                if payload.get(key):
                    return str(payload[key]).strip()
            raise ValueError("missing IP")
        return str(payload).strip()
    except Exception:
        raise CanaryProbeError("Decodo IP-check probe failed") from None
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


def _provider_counter_evidence(
    before: int | None, after: int | None
) -> dict[str, Any]:
    if before is None and after is None:
        return {"status": "not_supplied", "delta_bytes": None}
    if before is None or after is None:
        return {"status": "invalid", "delta_bytes": None}
    try:
        normalized_before = int(before)
        normalized_after = int(after)
    except (TypeError, ValueError):
        return {"status": "invalid", "delta_bytes": None}
    if normalized_before < 0 or normalized_after < normalized_before:
        return {"status": "invalid", "delta_bytes": None}
    if normalized_after == normalized_before:
        # Provider dashboards can lag.  Zero is not evidence of zero traffic.
        return {"status": "pending", "delta_bytes": None}
    return {
        "status": "reconciled",
        "delta_bytes": normalized_after - normalized_before,
    }


def build_probe_report(
    observations: Sequence[ProbeObservation],
    *,
    provider_bytes_before: int | None = None,
    provider_bytes_after: int | None = None,
) -> dict[str, Any]:
    """Build a redacted report and fail closed on missing/late probes."""

    normalized = tuple(observations)
    offsets = tuple(item.offset_seconds for item in normalized)
    if offsets != PROBE_OFFSETS_SECONDS:
        raise CanaryConfigurationError("probe observations are incomplete")
    hashes = {item.exit_hash for item in normalized}
    if any(
        len(item.exit_hash) != 64
        or any(character not in "0123456789abcdef" for character in item.exit_hash)
        for item in normalized
    ):
        raise CanaryConfigurationError("probe observation hash is invalid")
    schedule_ok = all(
        abs(float(item.elapsed_seconds) - item.offset_seconds)
        <= SCHEDULE_TOLERANCE_SECONDS
        for item in normalized
    )
    stable = len(hashes) == 1
    failures = []
    if not schedule_ok:
        failures.append("probe_schedule_late")
    if not stable:
        failures.append("exit_changed")
    sticky_passed = schedule_ok and stable
    candidate_stability = {
        str(minutes): (
            schedule_ok
            and len(
                {
                    item.exit_hash
                    for item in normalized
                    if item.offset_seconds <= minutes * 60
                }
            )
            == 1
        )
        for minutes in STICKY_CANDIDATE_MINUTES
    }
    return {
        "schema_version": "fbref-decodo-sticky-canary-v1",
        "endpoint_class": "decodo_ip_check_only",
        "probe_offsets_seconds": list(PROBE_OFFSETS_SECONDS),
        "schedule_tolerance_seconds": SCHEDULE_TOLERANCE_SECONDS,
        "schedule_within_tolerance": schedule_ok,
        "candidate_stability": candidate_stability,
        "sticky_passed": sticky_passed,
        "failures": failures,
        "recommended_provider_sticky_minutes": (
            PROVIDER_STICKY_MINUTES if sticky_passed else None
        ),
        "recommended_local_lease_minutes": (
            LOCAL_LEASE_MINUTES if sticky_passed else None
        ),
        "provider_counter": _provider_counter_evidence(
            provider_bytes_before, provider_bytes_after
        ),
        "observations": [asdict(item) for item in normalized],
    }


def run_canary(
    *,
    proxy_file: Path,
    provider_bytes_before: int | None = None,
    provider_bytes_after: int | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    probe: Callable[..., str] = _default_probe,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    """Execute exactly the six scheduled Decodo IP-check probes."""

    proxy_url = read_single_proxy_url(proxy_file)
    started = clock()
    observations = []
    for offset in PROBE_OFFSETS_SECONDS:
        remaining = started + offset - clock()
        if remaining > 0:
            sleep(remaining)
        try:
            observed_ip = probe(
                proxy_url=proxy_url,
                check_url=DECODO_IP_CHECK_URL,
            )
            exit_hash = hash_exit_identity(observed_ip)
        except CanaryProbeError:
            raise
        except Exception:
            raise CanaryProbeError("Decodo IP-check probe failed") from None
        observations.append(
            ProbeObservation(
                offset_seconds=offset,
                elapsed_seconds=clock() - started,
                observed_at=now().astimezone(timezone.utc).isoformat(),
                exit_hash=exit_hash,
            )
        )
    return build_probe_report(
        observations,
        provider_bytes_before=provider_bytes_before,
        provider_bytes_after=provider_bytes_after,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy-file", type=Path, required=True)
    parser.add_argument("--provider-bytes-before", type=int)
    parser.add_argument("--provider-bytes-after", type=int)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        report = run_canary(
            proxy_file=args.proxy_file,
            provider_bytes_before=args.provider_bytes_before,
            provider_bytes_after=args.provider_bytes_after,
        )
    except (CanaryConfigurationError, CanaryProbeError):
        print("Decodo sticky-session canary failed", file=sys.stderr)
        return 2
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        try:
            args.output.write_text(rendered + "\n", encoding="utf-8")
            os.chmod(args.output, 0o600)
        except OSError:
            print("Could not write canary report", file=sys.stderr)
            return 2
    print(rendered)
    return 0 if report["sticky_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
