#!/usr/bin/env python3
"""Bind a validated WhoScored release to immutable production containers.

The helper never creates, starts, restarts, or removes a protected workload
container.  Post-create admission runs one ephemeral, no-network, read-only,
capability-free probe from the attested scheduler digest solely to prove the
kernel applies ``docker-default`` in enforce mode.
"""

from __future__ import annotations

import sys as _bootstrap_sys


if __name__ == "__main__" and (
    not _bootstrap_sys.flags.isolated
    or not _bootstrap_sys.flags.no_site
    or not _bootstrap_sys.flags.ignore_environment
):
    _bootstrap_sys.modules["posix"]._exit(78)

import argparse
import errno
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import stat
import subprocess
import types
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


sys = _bootstrap_sys


_SCRIPT_PATH = Path(os.path.abspath(__file__))
_REPOSITORY_ROOT = _SCRIPT_PATH.parents[1]
_WHOSCORED_APPROVAL_PATH_RE = re.compile(
    r"/opt/airflow/secure/whoscored-approvals/"
    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\.json"
)
def _trusted_source_uids(*, require_protected: bool) -> frozenset[int]:
    """Return owners accepted for the exact sibling source load."""

    if require_protected:
        return frozenset({0})
    return frozenset({0, os.geteuid()})


def _load_exact_provenance_validator(
    *, require_protected: bool
) -> types.ModuleType:
    """Execute the exact sibling validator without consulting import paths."""

    module_name = "_whoscored_exact_build_provenance_validator"
    if module_name in sys.modules:
        raise RuntimeError("WhoScored provenance validator module was preloaded")
    path = _REPOSITORY_ROOT / "scripts/validate_whoscored_build_provenance.py"
    components = path.absolute().parts[1:]
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    trusted_source_uids = _trusted_source_uids(
        require_protected=require_protected
    )
    parent_descriptor = os.open("/", directory_flags)
    descriptor = -1
    try:
        for component in components[:-1]:
            child = os.open(component, directory_flags, dir_fd=parent_descriptor)
            os.close(parent_descriptor)
            parent_descriptor = child
            parent = os.fstat(parent_descriptor)
            writable = parent.st_mode & 0o022
            sticky_root = (
                parent.st_uid == 0
                and parent.st_mode & stat.S_ISVTX
                and parent.st_mode & 0o002
            )
            if parent.st_uid not in trusted_source_uids or (
                writable and not sticky_root
            ):
                raise RuntimeError("WhoScored validator has an unsafe parent directory")
        descriptor = os.open(components[-1], file_flags, dir_fd=parent_descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in trusted_source_uids
            or before.st_nlink != 1
            or before.st_mode & 0o022
        ):
            raise RuntimeError("WhoScored provenance validator source is not protected")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > 4 * 1024 * 1024:
                raise RuntimeError("WhoScored provenance validator source is too large")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        entry = os.stat(components[-1], dir_fd=parent_descriptor, follow_symlinks=False)
        identity = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(
            getattr(before, field) != getattr(candidate, field)
            for candidate in (after, entry)
            for field in identity
        ):
            raise RuntimeError("WhoScored provenance validator changed while loading")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        exec(compile(b"".join(chunks), str(path), "exec"), module.__dict__)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    if module.__file__ != str(path):
        raise RuntimeError("WhoScored provenance validator identity changed")
    module._whoscored_loaded_source_sha256 = hashlib.sha256(
        b"".join(chunks)
    ).hexdigest()
    return module


# Every privileged load stays root-only.  A non-root process may use its
# owner-protected checkout for offline validation, tests, and ``--help``;
# ``main`` rejects real admission unless it proves the privileged release.
try:
    provenance = _load_exact_provenance_validator(
        require_protected=os.geteuid() == 0
    )
except BaseException:
    if __name__ == "__main__":
        _bootstrap_sys.modules["posix"]._exit(78)
    raise


EXIT_CONFIG = 78
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_PROVIDER_QUOTA_RECEIPT_BYTES = 32 * 1024
MAX_PROVIDER_QUOTA_RECEIPT_AGE = timedelta(hours=24)
FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH = (
    "/opt/airflow/secure/fbref-geoip/GeoLite2-City.mmdb"
)
FBREF_CAMOUFOX_GEOIP_DATABASE_SHA256 = (
    "0772278c513e6ab3c65e9ae53d6861f137ab696f91eec763a2e6fe76befd83b2"
)
FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE = 66_164_133
_PROVIDER_QUOTA_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "provider",
        "order_id",
        "plan",
        "quota_decimal_gb",
        "remaining_decimal_gb",
        "observed_at",
        "screenshot_path",
        "screenshot_sha256",
    }
)
_PROVIDER_POLICY_UNSIGNED_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "provider_id",
        "order_id",
        "plan_id",
        "valid_from",
        "valid_until",
        "receipt_sha256",
        "provider_quota_bytes",
        "safety_cap_bytes",
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "signature_algorithm",
    }
)
_PROVIDER_POLICY_FIELDS = _PROVIDER_POLICY_UNSIGNED_FIELDS | {
    "document_sha256",
    "signature",
}
PROTECTED_SERVICES = (
    "airflow-scheduler",
    "flaresolverr",
    "flaresolverr_whoscored_paid",
    "whoscored_paid_gateway",
    "whoscored_proxy_filter",
)
_PROTECTED_SERVICE_SET = frozenset(PROTECTED_SERVICES)
COMMON_PROTECTED_SERVICES = ("airflow-scheduler", "flaresolverr")
GATEWAY_PROTECTED_SERVICES = (
    "flaresolverr_whoscored_paid",
    "whoscored_paid_gateway",
    "whoscored_proxy_filter",
)
COMMON_PROJECT = "data-platform"
GATEWAY_PROJECT = "whoscored-gw"
_SERVICE_PROJECT = {
    **{service: COMMON_PROJECT for service in COMMON_PROTECTED_SERVICES},
    **{service: GATEWAY_PROJECT for service in GATEWAY_PROTECTED_SERVICES},
}
_COMMON_EXTERNAL_NETWORKS = {
    "whoscored-paid-api": {
        "external": True,
        "ipam": {},
        "name": "dp-whoscored-paid-api",
    }
}
_NETWORK_PROJECT = {
    "backend": COMMON_PROJECT,
    "frontend": COMMON_PROJECT,
    "storage": COMMON_PROJECT,
    "whoscored-paid-api": GATEWAY_PROJECT,
    "whoscored-paid-browser": GATEWAY_PROJECT,
    "whoscored-paid-direct-egress": GATEWAY_PROJECT,
    "whoscored-paid-provider-egress": GATEWAY_PROJECT,
}
_DIGEST = re.compile(r"\A[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"\A[0-9a-f]{40}\Z")
_PINNED_IMAGE = re.compile(r"\A[^\s@]+@sha256:[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_CONTAINER_ID = re.compile(r"\A[0-9a-f]{64}\Z")
_MAC_ADDRESS = re.compile(r"\A(?:[0-9a-f]{2}:){5}[0-9a-f]{2}\Z")
_PROJECT_NAME = re.compile(r"\A[a-z0-9][a-z0-9_-]*\Z")
_CONFIG_HASH = re.compile(r"\A[0-9a-f]{64}\Z")
_COMPOSE_VERSION = re.compile(
    r"\A([0-9]+)\.([0-9]+)\.([0-9]+)(?:[-+][0-9A-Za-z.-]+)?\Z"
)
_REQUIRED_DOCKER_SECURITY_OPTIONS = frozenset(
    {"name=apparmor", "name=seccomp,profile=builtin"}
)
_EXPECTED_SECURITY_OPT = (
    "no-new-privileges:true",
    "apparmor=docker-default",
    "seccomp=builtin",
)
_DOCKER_CLI = Path("/usr/bin/docker")
_DOCKER_SOCKET = Path("/run/docker.sock")
_SYSTEM_PYTHON = Path("/usr/bin/python3")
_FORBIDDEN_CONTROL_ENV = frozenset(
    {
        "COMPOSE_DISABLE_ENV_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_FILE",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "DOCKER_API_VERSION",
        "DOCKER_CERT_PATH",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "LD_AUDIT",
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "LOCPATH",
        "MALLOC_TRACE",
        "PYTHONPATH",
    }
)
_AIRFLOW_ENTRYPOINT = (
    "/usr/bin/dumb-init",
    "--",
    "/usr/local/bin/whoscored-production-entrypoint",
    "/entrypoint",
)
_EXPECTED_ENTRYPOINTS = {
    "airflow-scheduler": _AIRFLOW_ENTRYPOINT,
    "flaresolverr": ("/usr/bin/dumb-init", "--"),
    "flaresolverr_whoscored_paid": ("/usr/bin/dumb-init", "--"),
    "whoscored_paid_gateway": _AIRFLOW_ENTRYPOINT,
    "whoscored_proxy_filter": _AIRFLOW_ENTRYPOINT,
}
_EXPECTED_IMAGE_USER = {
    "airflow-scheduler": "50000:0",
    "flaresolverr": "1000:1000",
    "flaresolverr_whoscored_paid": "1000:1000",
    "whoscored_paid_gateway": "50000:0",
    "whoscored_proxy_filter": "50000:0",
}
_EXPECTED_WORKING_DIR = {
    "airflow-scheduler": "/opt/airflow",
    "flaresolverr": "/app",
    "flaresolverr_whoscored_paid": "/app",
    "whoscored_paid_gateway": "/opt/airflow",
    "whoscored_proxy_filter": "/opt/airflow",
}
_EXPECTED_COMMANDS = {
    "airflow-scheduler": ("scheduler",),
    "flaresolverr": ("/usr/local/bin/whoscored-flaresolverr-entrypoint",),
    "flaresolverr_whoscored_paid": (
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
    ),
    "whoscored_paid_gateway": (
        "python",
        "/opt/airflow/scripts/whoscored_paid_gateway.py",
        "--host",
        "0.0.0.0",
        "--port",
        "8898",
        "--proxy-url",
        "http://whoscored_proxy_filter:8900",
        "--proxy-control-url",
        "http://whoscored_proxy_filter:8899",
        "--flaresolverr-url",
        "http://flaresolverr_whoscored_paid:8191",
    ),
}
_SECURITY_POLICY = {
    "airflow-scheduler": {
        "cap_add": frozenset(),
        "read_only": False,
    },
    "flaresolverr": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "flaresolverr_whoscored_paid": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "whoscored_paid_gateway": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "whoscored_proxy_filter": {"cap_add": frozenset(), "read_only": True},
}
_CRITICAL_IMAGE_PATHS = {
    "airflow-scheduler": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/opt/legacy-scraper-venv",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
    "flaresolverr": (
        "/app/chromedriver",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
        "/usr/local/libexec/whoscored",
        "/usr/local/share/whoscored",
    ),
    "flaresolverr_whoscored_paid": (
        "/app/chromedriver",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
        "/usr/local/libexec/whoscored",
        "/usr/local/share/whoscored",
    ),
    "whoscored_paid_gateway": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
    "whoscored_proxy_filter": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
}
_ALLOWED_VOLUME_TARGETS = {
    "airflow-scheduler": {
        "/home/airflow/soccerdata": ("volume", False),
        "/opt/airflow/configs/fotmob": ("bind", True),
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/configs/soccerdata": ("bind", True),
        "/opt/airflow/configs/sofascore": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/fotmob-admission": ("bind", True),
        "/opt/airflow/logs": ("bind", False),
        "/opt/airflow/proxys.txt": ("bind", True),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH: ("bind", True),
        "/opt/airflow/secure/whoscored-approvals": ("bind", True),
        "/opt/airflow/secure/whoscored-scheduled-pointers": ("bind", True),
        "/opt/airflow/state/whoscored-proxy-filter": ("bind", True),
        "/opt/airflow/transform": ("bind", True),
        "/opt/airflow/webserver_config.py": ("bind", True),
    },
    "flaresolverr": {},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        "/opt/airflow/secure/whoscored-alert-authority": ("bind", True),
        "/opt/airflow/state/whoscored-paid-gateway": ("bind", False),
    },
    "whoscored_proxy_filter": {
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/configs/proxy_filter": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        "/opt/airflow/state/whoscored-proxy-filter": ("bind", False),
    },
}
_RELEASE_BIND_TARGETS = {
    "airflow-scheduler": {
        "/opt/airflow/configs/fotmob": "configs/fotmob",
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/configs/soccerdata": "configs/soccerdata",
        "/opt/airflow/configs/sofascore": "configs/sofascore",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
        "/opt/airflow/transform": "transform",
        "/opt/airflow/webserver_config.py": "configs/airflow/webserver_config.py",
    },
    "flaresolverr": {},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
    },
    "whoscored_proxy_filter": {
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/configs/proxy_filter": "configs/proxy_filter",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
    },
}
_RUNTIME_HOST_BIND_TARGETS = {
    (
        "airflow-scheduler",
        "/opt/airflow/fotmob-admission",
    ): "protected-directory",
    (
        "airflow-scheduler",
        FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH,
    ): "fbref-geoip-database",
    ("airflow-scheduler", "/opt/airflow/logs"): "writable-directory",
    ("airflow-scheduler", "/opt/airflow/proxys.txt"): "protected-file",
    (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-approvals",
    ): "airflow-authority-directory",
    (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-scheduled-pointers",
    ): "airflow-authority-directory",
    (
        "airflow-scheduler",
        "/opt/airflow/state/whoscored-proxy-filter",
    ): "writable-directory",
    (
        "whoscored_paid_gateway",
        "/opt/airflow/state/whoscored-paid-gateway",
    ): "writable-directory",
    (
        "whoscored_paid_gateway",
        "/opt/airflow/secure/whoscored-alert-authority",
    ): "protected-directory",
    (
        "whoscored_proxy_filter",
        "/opt/airflow/state/whoscored-proxy-filter",
    ): "writable-directory",
}
_AIRFLOW_RUNTIME_UID = 50_000
_ALLOWED_TMPFS = {
    "airflow-scheduler": {},
    "flaresolverr": {
        "/app/.config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/app/.local": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=16m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/tmp": frozenset(
            {
                "rw",
                "exec",
                "nosuid",
                "nodev",
                "size=2g",
                "uid=1000",
                "gid=1000",
                "mode=1770",
            }
        ),
    },
    "flaresolverr_whoscored_paid": {
        "/app/.config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/app/.local": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=16m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/tmp": frozenset(
            {
                "rw",
                "exec",
                "nosuid",
                "nodev",
                "size=2g",
                "uid=1000",
                "gid=1000",
                "mode=1770",
            }
        ),
    },
    "whoscored_paid_gateway": {
        "/tmp": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=32m",
                "uid=50000",
                "gid=0",
                "mode=0700",
            }
        ),
    },
    "whoscored_proxy_filter": {
        "/tmp": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=32m",
                "uid=50000",
                "gid=0",
                "mode=0700",
            }
        ),
    },
}
_EXPECTED_HEALTHCHECKS = {
    "airflow-scheduler": {
        "Interval": 30_000_000_000,
        "Retries": 5,
        "StartPeriod": 60_000_000_000,
        "Test": (
            "CMD-SHELL",
            'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"',
        ),
        "Timeout": 30_000_000_000,
    },
    "flaresolverr": {
        "Interval": 30_000_000_000,
        "Retries": 3,
        "StartPeriod": 30_000_000_000,
        "Test": ("CMD-SHELL", "curl -fsS http://localhost:8191/health || exit 1"),
        "Timeout": 10_000_000_000,
    },
    "flaresolverr_whoscored_paid": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 30_000_000_000,
        "Test": ("CMD-SHELL", "curl -fsS http://localhost:8191/health || exit 1"),
        "Timeout": 5_000_000_000,
    },
    "whoscored_paid_gateway": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 5_000_000_000,
        "Test": (
            "CMD",
            "curl",
            "--fail",
            "--silent",
            "http://localhost:8898/health",
        ),
        "Timeout": 3_000_000_000,
    },
    "whoscored_proxy_filter": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 5_000_000_000,
        "Test": (
            "CMD",
            "curl",
            "--fail",
            "--silent",
            "http://localhost:8899/health",
        ),
        "Timeout": 3_000_000_000,
    },
}
_ALLOWED_RENDERED_KEYS = {
    "airflow-scheduler": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "depends_on",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "memswap_limit",
            "networks",
            "restart",
            "security_opt",
            "shm_size",
            "volumes",
        }
    ),
    "flaresolverr": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "ports",
            "read_only",
            "restart",
            "security_opt",
            "shm_size",
            "tmpfs",
        }
    ),
    "flaresolverr_whoscored_paid": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "shm_size",
            "tmpfs",
        }
    ),
    "whoscored_paid_gateway": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "depends_on",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "tmpfs",
            "volumes",
        }
    ),
    "whoscored_proxy_filter": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "tmpfs",
            "volumes",
        }
    ),
}
_EXPECTED_NETWORKS = {
    "airflow-scheduler": {
        "backend": None,
        "frontend": None,
        "storage": None,
        "whoscored-paid-api": None,
    },
    "flaresolverr": {"backend": None},
    "flaresolverr_whoscored_paid": {"whoscored-paid-browser": None},
    "whoscored_paid_gateway": {
        "whoscored-paid-api": None,
        "whoscored-paid-browser": None,
        "whoscored-paid-direct-egress": None,
    },
    "whoscored_proxy_filter": {
        "whoscored-paid-browser": None,
        "whoscored-paid-provider-egress": None,
    },
}
_EXPECTED_NETWORK_MODE = {
    "airflow-scheduler": "dp-backend",
    "flaresolverr": "dp-backend",
    "flaresolverr_whoscored_paid": "dp-whoscored-paid-browser",
    "whoscored_paid_gateway": "dp-whoscored-paid-api",
    "whoscored_proxy_filter": "dp-whoscored-paid-browser",
}
_EXPECTED_NETWORK_DEFINITIONS = {
    "backend": {"driver": "bridge", "ipam": {}, "name": "dp-backend"},
    "frontend": {"driver": "bridge", "ipam": {}, "name": "dp-frontend"},
    "storage": {"driver": "bridge", "ipam": {}, "name": "dp-storage"},
    "whoscored-paid-api": {
        "driver": "bridge",
        "internal": True,
        "ipam": {},
        "name": "dp-whoscored-paid-api",
    },
    "whoscored-paid-browser": {
        "driver": "bridge",
        "internal": True,
        "ipam": {},
        "name": "dp-whoscored-paid-browser",
    },
    "whoscored-paid-direct-egress": {
        "driver": "bridge",
        "ipam": {},
        "name": "dp-whoscored-paid-direct-egress",
    },
    "whoscored-paid-provider-egress": {
        "driver": "bridge",
        "ipam": {},
        "name": "dp-whoscored-paid-provider-egress",
    },
}
_EXPECTED_DEPENDS_ON = {
    "airflow-scheduler": {
        "airflow-init": {
            "condition": "service_completed_successfully",
            "required": True,
        },
        "airflow-webserver": {"condition": "service_healthy", "required": True},
    },
    "flaresolverr": None,
    "flaresolverr_whoscored_paid": None,
    "whoscored_paid_gateway": {
        "flaresolverr_whoscored_paid": {
            "condition": "service_healthy",
            "required": True,
        },
        "whoscored_proxy_filter": {
            "condition": "service_healthy",
            "required": True,
        },
    },
    "whoscored_proxy_filter": None,
}
_EXPECTED_DEPLOY = {
    "airflow-scheduler": {
        "placement": {},
        "resources": {
            "limits": {"memory": "17179869184"},
            "reservations": {"memory": "1073741824"},
        },
    },
    "flaresolverr": {
        "placement": {},
        "resources": {
            "limits": {"memory": "4294967296"},
            "reservations": {"memory": "536870912"},
        },
    },
    "flaresolverr_whoscored_paid": {
        "placement": {},
        "resources": {
            "limits": {"memory": "2147483648"},
            "reservations": {"memory": "536870912"},
        },
    },
    "whoscored_paid_gateway": {
        "placement": {},
        "resources": {
            "limits": {"memory": "268435456"},
            "reservations": {"memory": "67108864"},
        },
    },
    "whoscored_proxy_filter": {
        "placement": {},
        "resources": {
            "limits": {"memory": "268435456"},
            "reservations": {"memory": "67108864"},
        },
    },
}
_EXPECTED_CONTAINER_RESOURCES = {
    "airflow-scheduler": {
        "Memory": 17_179_869_184,
        "MemoryReservation": 1_073_741_824,
        "MemorySwap": 17_179_869_184,
    },
    "flaresolverr": {
        "Memory": 4_294_967_296,
        "MemoryReservation": 536_870_912,
        # Compose leaves memswap_limit unset for this service. Engine 29
        # normalizes that request to memory + an equal swap allowance in the
        # inspected HostConfig, even when the host currently has no swap.
        "MemorySwap": 8_589_934_592,
    },
    "flaresolverr_whoscored_paid": {
        "Memory": 2_147_483_648,
        "MemoryReservation": 536_870_912,
        "MemorySwap": 4_294_967_296,
    },
    "whoscored_paid_gateway": {
        "Memory": 268_435_456,
        "MemoryReservation": 67_108_864,
        "MemorySwap": 536_870_912,
    },
    "whoscored_proxy_filter": {
        "Memory": 268_435_456,
        "MemoryReservation": 67_108_864,
        "MemorySwap": 536_870_912,
    },
}
_SCHEDULER_ENVIRONMENT_NAMES = frozenset(
    """
    AIRFLOW__CELERY__BROKER_URL AIRFLOW__CELERY__RESULT_BACKEND
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION AIRFLOW__CORE__EXECUTOR
    AIRFLOW__CORE__FERNET_KEY AIRFLOW__CORE__LOAD_EXAMPLES
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN AIRFLOW__WEBSERVER__EXPOSE_CONFIG
    AIRFLOW__WEBSERVER__SECRET_KEY ALERT_ENV FBREF_PROXY_CONTROL_TOKEN
    FBREF_CAMOUFOX_GEOIP_DATABASE_PATH FBREF_CONTROL_DB_URI FOTMOB_DEPLOY_GIT_SHA
    FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH
    FBREF_PROXY_CONTROL_URL FBREF_PROXY_LEASE_TTL_SECONDS FBREF_RAW_S3_ENDPOINT
    FBREF_RAW_S3_SCHEME FBREF_RAW_STORE_URI FBREF_STAGE_JANITOR_MODE
    FOTMOB_RAW_S3_ENDPOINT
    FOTMOB_RAW_S3_SCHEME FOTMOB_RAW_STORE_URI ICEBERG_REST_WAREHOUSE
    ICEBERG_WAREHOUSE JAVA_HOME LEGACY_SCRAPER_PYTHON
    PROXY_FILTER_CONTROL_TOKEN PROXY_FILTER_LEDGER_PATH
    PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES PROXY_FILTER_URL
    S3_ACCESS_KEY S3_ENDPOINT S3_SCHEME S3_SECRET_KEY
    SEAWEEDFS_CUTOVER_BACKUP_MIBPS SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS
    SEAWEEDFS_CUTOVER_INVENTORY_MIBPS SEAWEEDFS_CUTOVER_MAX_DOWNTIME_SECONDS
    SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY
    SEAWEEDFS_CUTOVER_REHEARSAL_MAX_AGE_HOURS
    SEAWEEDFS_CUTOVER_VERIFY_MIBPS SOFASCORE_MANIFEST_BACKEND
    SOFASCORE_PROXY_BUDGET_ARTIFACT SOFASCORE_PROXY_BUDGET_LEDGER
    SOFASCORE_PROXY_CONTROL_TOKEN SOFASCORE_PROXY_CONTROL_URL
    SOFASCORE_PROXY_LEASE_TTL_SECONDS SOFASCORE_RAW_STORE_URI
    SOFASCORE_REGISTRY_PATH SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES
    SOFASCORE_PLAYER_ROTATION_MODULUS SOFASCORE_WORKLOAD_PLAN_DIR TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID TM_NATIVE_V2_ENABLED TM_STANDING_POLICY_ENABLED
    TM_PROXY_CONTROL_TOKEN TM_PROXY_CONTROL_URL TM_PROXY_LEASE_TTL_SECONDS
    TM_REQUIRE_METERED_PROXY TRINO_HOST
    TRINO_PASSWORD TRINO_PORT WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY
    WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS WHOSCORED_BACKFILL_POOL
    WHOSCORED_BACKFILL_REQUEST_UNITS_PER_RUN
    WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE
    WHOSCORED_BACKUP_DESTINATION_S3_ACCESS_KEY
    WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT
    WHOSCORED_BACKUP_DESTINATION_S3_REGION
    WHOSCORED_BACKUP_DESTINATION_S3_SCHEME
    WHOSCORED_BACKUP_DESTINATION_S3_SECRET_KEY
    WHOSCORED_BACKUP_DESTINATION_SITE_ID WHOSCORED_BACKUP_DESTINATION_URI
    WHOSCORED_BACKUP_LOCAL_RETENTION_DAYS
    WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY
    WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT WHOSCORED_BACKUP_RESTORE_S3_REGION
    WHOSCORED_BACKUP_RESTORE_S3_SCHEME WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY
    WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT
    WHOSCORED_BACKUP_SOURCE_S3_REGION WHOSCORED_BACKUP_SOURCE_S3_SCHEME
    WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY WHOSCORED_BACKUP_SOURCE_SITE_ID
    WHOSCORED_BACKUP_WORKERS WHOSCORED_CATALOG_REQUESTS_PER_MINUTE
    WHOSCORED_DAILY_P95_LIMIT_HOURS WHOSCORED_DAILY_PROFILE_MAX_LIMIT
    WHOSCORED_DAILY_SLO_MIN_SAMPLES WHOSCORED_DAILY_SLO_WINDOW
    WHOSCORED_DIRECT_POOL WHOSCORED_DQ_POOL WHOSCORED_LOCK_DIR
    WHOSCORED_OPS_IO_ATTEMPTS WHOSCORED_OPS_RETRY_BASE_SECONDS
    WHOSCORED_OPS_STORE_URI
    WHOSCORED_PAID_BATCH_ENABLED WHOSCORED_PAID_GATEWAY_TOKEN
    WHOSCORED_PAID_GATEWAY_URL
    WHOSCORED_PROXY_APPROVAL_ROOT WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH
    WHOSCORED_SCHEDULED_PAID_MODE WHOSCORED_SCHEDULED_PAID_POINTER_ROOT
    WHOSCORED_RAW_IO_ATTEMPTS WHOSCORED_RAW_LOCK_DIR
    WHOSCORED_RAW_LOCK_TIMEOUT_SECONDS WHOSCORED_RAW_RETRY_BASE_SECONDS
    WHOSCORED_RAW_S3_ACCESS_KEY WHOSCORED_RAW_S3_ENDPOINT
    WHOSCORED_RAW_S3_SCHEME WHOSCORED_RAW_S3_SECRET_KEY
    WHOSCORED_RAW_SNAPSHOT_LOCK_TIMEOUT_SECONDS WHOSCORED_RAW_STORE_URI
    WHOSCORED_REQUEST_LEDGER_PATH WHOSCORED_RUN_RETENTION_DAYS
    WHOSCORED_SCOPE_WRITE_CHUNK_ROWS WHOSCORED_SCRAPER_PYTHON
    WHOSCORED_SOURCE_CIRCUIT_PATH WHOSCORED_SOURCE_CIRCUIT_WAIT
    WHOSCORED_SOURCE_POOL_SLOTS WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE
    """.split()
)
_EXPECTED_ENVIRONMENT_NAMES = {
    "airflow-scheduler": _SCHEDULER_ENVIRONMENT_NAMES,
    "flaresolverr": frozenset({"CAPTCHA_SOLVER", "LOG_HTML", "LOG_LEVEL", "TZ"}),
    "flaresolverr_whoscored_paid": frozenset(
        {
            "CAPTCHA_SOLVER",
            "LOG_HTML",
            "LOG_LEVEL",
            "TZ",
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_FLARESOLVERR_PAID_EXCLUSIVE",
        }
    ),
    "whoscored_paid_gateway": frozenset(
        {
            "ALERT_ENV",
            "CONNECTION_CHECK_MAX_COUNT",
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_PAID_ALERT_BINDING_PATH",
            "WHOSCORED_PAID_ALERT_HMAC_SECRET",
            "WHOSCORED_PAID_ALERT_RECEIPT_ROOT",
            "WHOSCORED_PAID_ALERT_AUTHORITY_ROOT",
            "WHOSCORED_PAID_ALERT_SECRET_PATH",
            "WHOSCORED_PAID_BATCH_ENABLED",
            "WHOSCORED_PAID_GATEWAY_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CONTROL_TOKEN",
        }
    ),
    "whoscored_proxy_filter": frozenset(
        {
            "CONNECTION_CHECK_MAX_COUNT",
            "PROXY_FILTER_ALLOW_FILE_FALLBACK",
            "PROXY_FILTER_CONTROL_TOKEN",
            "PROXY_POOL_JSON",
            "TM_PROXY_CONTROL_TOKEN",
            "WHOSCORED_PROVIDER_ORDER_ID",
            "WHOSCORED_PROVIDER_POLICY_SHA256",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH",
            "WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES",
            "WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES",
            "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        }
    ),
}
_FIXED_ENVIRONMENT = {
    "airflow-scheduler": {
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "true",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__WEBSERVER__EXPOSE_CONFIG": "false",
        "FBREF_PROXY_CONTROL_URL": "http://fbref_proxy_filter:8899",
        "FBREF_PROXY_LEASE_TTL_SECONDS": "7200",
        "FBREF_STAGE_JANITOR_MODE": "apply",
        "FBREF_CAMOUFOX_GEOIP_DATABASE_PATH": (
            FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH
        ),
        "LEGACY_SCRAPER_PYTHON": "/opt/legacy-scraper-venv/bin/python",
        "PROXY_FILTER_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl"
        ),
        "PROXY_FILTER_URL": "",
        "WHOSCORED_BACKFILL_POOL": "whoscored_direct_pool",
        "WHOSCORED_DIRECT_POOL": "whoscored_direct_pool",
        "WHOSCORED_DQ_POOL": "whoscored_dq_pool",
        "WHOSCORED_LOCK_DIR": "/opt/airflow/logs/whoscored/commit_locks",
        "WHOSCORED_OPS_STORE_URI": "s3://warehouse/ops/whoscored",
        "WHOSCORED_PAID_GATEWAY_URL": "http://whoscored_paid_gateway:8898",
        "WHOSCORED_PROXY_APPROVAL_ROOT": ("/opt/airflow/secure/whoscored-approvals"),
        "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT": (
            "/opt/airflow/secure/whoscored-scheduled-pointers"
        ),
        "WHOSCORED_SCHEDULED_PAID_MODE": "required",
        "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json"
        ),
        "WHOSCORED_RAW_LOCK_DIR": "/opt/airflow/logs/whoscored/raw_locks",
        "WHOSCORED_RAW_S3_ENDPOINT": "seaweedfs:8333",
        "WHOSCORED_RAW_S3_SCHEME": "http",
        "WHOSCORED_RAW_STORE_URI": "s3://warehouse/raw/whoscored",
        "WHOSCORED_REQUEST_LEDGER_PATH": (
            "/opt/airflow/logs/whoscored/request_ledger.jsonl"
        ),
        "WHOSCORED_SCRAPER_PYTHON": "/usr/local/bin/python",
        "WHOSCORED_SOURCE_CIRCUIT_PATH": (
            "/opt/airflow/logs/whoscored/source-circuit-v1.json"
        ),
        "WHOSCORED_SOURCE_CIRCUIT_WAIT": "0",
    },
    "flaresolverr": {
        "CAPTCHA_SOLVER": "none",
        "LOG_HTML": "false",
        "LOG_LEVEL": "info",
        "TZ": "UTC",
    },
    "flaresolverr_whoscored_paid": {
        "CAPTCHA_SOLVER": "none",
        "LOG_HTML": "false",
        "LOG_LEVEL": "info",
        "TZ": "UTC",
        "WHOSCORED_FLARESOLVERR_PAID_EXCLUSIVE": "1",
    },
    "whoscored_paid_gateway": {
        "ALERT_ENV": "prod",
        "CONNECTION_CHECK_MAX_COUNT": "0",
        "WHOSCORED_PAID_ALERT_AUTHORITY_ROOT": (
            "/opt/airflow/secure/whoscored-alert-authority"
        ),
        "WHOSCORED_PAID_ALERT_RECEIPT_ROOT": (
            "/opt/airflow/state/whoscored-paid-gateway/alert-receipts"
        ),
    },
    "whoscored_proxy_filter": {
        "CONNECTION_CHECK_MAX_COUNT": "0",
        "PROXY_FILTER_ALLOW_FILE_FALLBACK": "false",
        "TM_PROXY_CONTROL_TOKEN": "",
        "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json"
        ),
    },
}
_AIRFLOW_IMAGE_ENVIRONMENT = {
    "AIRFLOW_CONFIG": "/usr/local/share/whoscored/airflow.cfg",
    "GUNICORN_CMD_ARGS": "--worker-tmp-dir /dev/shm --no-control-socket",
    "LD_LIBRARY_PATH": "",
    "PATH": (
        "/opt/spark/bin:/usr/lib/jvm/java-17-openjdk-amd64/bin:/root/bin:"
        "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:"
        "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    ),
    "PYTHONNOUSERSITE": "1",
    "PYTHONPATH": "",
    "PYTHONPYCACHEPREFIX": "/__whoscored_runtime_bytecode_disabled__",
    "PYTHONSAFEPATH": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_EXPECTED_IMAGE_ENVIRONMENT = {
    "airflow-scheduler": _AIRFLOW_IMAGE_ENVIRONMENT,
    "flaresolverr": {
        "PATH": "/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    },
    "flaresolverr_whoscored_paid": {
        "PATH": "/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    },
    "whoscored_paid_gateway": _AIRFLOW_IMAGE_ENVIRONMENT,
    "whoscored_proxy_filter": _AIRFLOW_IMAGE_ENVIRONMENT,
}
_EXPECTED_MASKED_PATHS = (
    "/proc/acpi",
    "/proc/asound",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/sys/devices/virtual/powercap",
    "/sys/firmware",
)
_EXPECTED_READONLY_PATHS = (
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
)
_SAFE_IMAGE_LABEL_PREFIXES = (
    "org.apache.airflow.",
    "org.opencontainers.image.",
)
_EXPECTED_SHM_SIZE = {
    "airflow-scheduler": 536_870_912,
    "flaresolverr": 1_073_741_824,
    "flaresolverr_whoscored_paid": 1_073_741_824,
    "whoscored_paid_gateway": 67_108_864,
    "whoscored_proxy_filter": 67_108_864,
}
_EXPECTED_PORT_BINDINGS = {
    "airflow-scheduler": {},
    "flaresolverr": {"8191/tcp": ({"HostIp": "127.0.0.1", "HostPort": "8191"},)},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {},
    "whoscored_proxy_filter": {},
}


class AdmissionError(RuntimeError):
    """Raised when immutable deployment admission cannot be proven."""


@dataclass(frozen=True)
class ValidatedBindingsEvidence:
    """Exact snapshots and identities used to admit one release binding."""

    bindings: Mapping[str, str]
    build_attestation_raw: bytes
    build_attestation_identity: tuple[int, ...]
    build_manifest_raw: bytes
    build_manifest_identity: tuple[int, ...]
    deployment_attestation_raw: bytes
    deployment_attestation_identity: tuple[int, ...]
    validated_release_revision: str
    validated_payload_revision: str
    validated_manifest_sha256: str
    validated_source_tree_sha256: str
    validated_payload_image_ids: Mapping[str, str]


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_regular_file(path: Path, *, label: str) -> tuple[bytes, tuple[int, ...]]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AdmissionError(
            f"{label} is missing, symlinked, or unreadable: {path}"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AdmissionError(f"{label} is not a regular file: {path}")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_JSON_BYTES:
                raise AdmissionError(f"{label} exceeds {MAX_JSON_BYTES} bytes")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            raise AdmissionError(f"{label} changed while it was read: {path}")
        return b"".join(chunks), _stat_identity(after)
    except OSError as exc:
        raise AdmissionError(f"cannot read {label}: {path}") from exc
    finally:
        os.close(descriptor)


def _load_json_object(
    path: Path,
    *,
    label: str,
    canonical: bool,
) -> tuple[dict[str, Any], bytes, tuple[int, ...]]:
    raw, identity = _read_regular_file(path, label=label)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(f"{label} is not unambiguous JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AdmissionError(f"{label} must contain one JSON object: {path}")
    if canonical and raw != _canonical_bytes(value):
        raise AdmissionError(f"{label} is not canonical JSON: {path}")
    return value, raw, identity


def _final_image_bindings(
    deployment: Mapping[str, Any],
    *,
    expected_payloads: Mapping[str, str],
) -> dict[str, str]:
    if set(deployment) != {
        "images",
        "provenance_manifest_sha256",
        "schema_version",
        "status",
    }:
        raise AdmissionError("deployment attestation schema is invalid")
    manifest_digest = deployment.get("provenance_manifest_sha256")
    if (
        deployment.get("schema_version") != 1
        or deployment.get("status") != "ready-v1"
        or not isinstance(manifest_digest, str)
        or _DIGEST.fullmatch(manifest_digest) is None
    ):
        raise AdmissionError("deployment attestation identity is invalid")
    images = deployment.get("images")
    if not isinstance(images, (list, tuple)):
        raise AdmissionError("deployment attestation images must be a sequence")
    observed_payloads: dict[str, str] = {}
    final_images: dict[str, str] = {}
    prior = ""
    for record in images:
        if not isinstance(record, Mapping) or set(record) != {
            "final_image",
            "payload_image_id",
            "service",
        }:
            raise AdmissionError("deployment image record schema is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        final_image = record.get("final_image")
        if (
            not isinstance(service, str)
            or not service
            or service <= prior
            or service in observed_payloads
            or not isinstance(payload, str)
            or _IMAGE_ID.fullmatch(payload) is None
            or not isinstance(final_image, str)
            or _PINNED_IMAGE.fullmatch(final_image) is None
        ):
            raise AdmissionError(
                "deployment image records are duplicated, unsorted, or mutable"
            )
        prior = service
        observed_payloads[service] = payload
        final_images[service] = final_image
    if observed_payloads != dict(expected_payloads):
        raise AdmissionError(
            "deployment attestation has extra, missing, or changed services"
        )
    protected = {
        service: final_images[service]
        for service in PROTECTED_SERVICES
        if service in final_images
    }
    if set(protected) != _PROTECTED_SERVICE_SET:
        raise AdmissionError(
            "deployment attestation does not bind every protected service"
        )
    return protected


def _validate_bindings_and_discovery(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> tuple[dict[str, str], Any]:

    if frozenset(provenance.PROTECTED_PRODUCTION_SERVICES) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("validator and admission protected-service sets differ")
    try:
        discovery = provenance.validate(
            root,
            attestation_path=attestation_path,
            manifest_path=manifest_path,
            deployment_attestation_path=deployment_attestation_path,
            expect_blocked=False,
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    deployment = discovery.deployment_attestation
    deployment_raw = discovery.deployment_attestation_raw
    validator_final_images = discovery.deployment_final_images
    if (
        not isinstance(deployment, Mapping)
        or not isinstance(deployment_raw, bytes)
        or not deployment_raw
        or not isinstance(validator_final_images, Mapping)
    ):
        raise AdmissionError(
            "validator did not return its fd-pinned deployment attestation"
        )
    local_images = discovery.records.get("local_images")
    if not isinstance(local_images, list) or not local_images:
        raise AdmissionError("validated provenance has no local image records")
    expected_payloads: dict[str, str] = {}
    for record in local_images:
        if not isinstance(record, dict):
            raise AdmissionError("validated local image record is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        if (
            not isinstance(service, str)
            or not service
            or service in expected_payloads
            or not isinstance(payload, str)
            or _IMAGE_ID.fullmatch(payload) is None
        ):
            raise AdmissionError("validated local image bindings are invalid")
        expected_payloads[service] = payload
    if list(expected_payloads) != sorted(expected_payloads):
        raise AdmissionError("validated local image bindings are not sorted")
    bindings = _final_image_bindings(deployment, expected_payloads=expected_payloads)
    observed_final_images = {
        str(service): str(image) for service, image in validator_final_images.items()
    }
    images = deployment.get("images")
    assert isinstance(images, (list, tuple))
    parsed_final_images = {
        str(record["service"]): str(record["final_image"])
        for record in images
        if isinstance(record, Mapping)
    }
    if observed_final_images != parsed_final_images:
        raise AdmissionError("validator final-image bindings differ from attestation")
    return bindings, discovery


def validate_bindings(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> dict[str, str]:
    """Validate provenance and return every immutable protected image ref."""

    bindings, _ = _validate_bindings_and_discovery(
        root=root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_attestation_path,
    )
    return bindings


def _evidence_identity(value: object, *, label: str) -> tuple[int, ...]:
    if (
        not isinstance(value, tuple)
        or len(value) != 9
        or any(type(item) is not int for item in value)
    ):
        raise AdmissionError(f"validator returned an invalid {label} identity")
    return value


def _evidence_raw(value: object, *, label: str) -> bytes:
    if not isinstance(value, bytes) or not value:
        raise AdmissionError(f"validator did not return its fd-pinned {label}")
    return value


def validate_bindings_with_evidence(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> ValidatedBindingsEvidence:
    """Return bindings and the exact fd snapshots used to validate them."""

    bindings, discovery = _validate_bindings_and_discovery(
        root=root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_attestation_path,
    )
    release_revision = getattr(discovery, "validated_release_revision", None)
    payload_revision = getattr(discovery, "validated_payload_revision", None)
    manifest_digest = getattr(discovery, "validated_manifest_sha256", None)
    source_tree_digest = getattr(discovery, "validated_source_tree_sha256", None)
    payload_image_ids = getattr(discovery, "validated_payload_image_ids", None)
    if (
        not isinstance(release_revision, str)
        or _COMMIT.fullmatch(release_revision) is None
    ):
        raise AdmissionError("validator did not preserve the release revision")
    if (
        not isinstance(payload_revision, str)
        or _COMMIT.fullmatch(payload_revision) is None
    ):
        raise AdmissionError("validator did not preserve the payload revision")
    if (
        not isinstance(manifest_digest, str)
        or _DIGEST.fullmatch(manifest_digest) is None
    ):
        raise AdmissionError("validator did not preserve the manifest digest")
    if (
        not isinstance(source_tree_digest, str)
        or _DIGEST.fullmatch(source_tree_digest) is None
    ):
        raise AdmissionError("validator did not preserve the source-tree digest")
    if not isinstance(payload_image_ids, Mapping):
        raise AdmissionError("validator did not preserve payload image bindings")
    normalized_payloads: dict[str, str] = {}
    for service, image_id in payload_image_ids.items():
        if (
            not isinstance(service, str)
            or not service
            or service in normalized_payloads
            or not isinstance(image_id, str)
            or _IMAGE_ID.fullmatch(image_id) is None
        ):
            raise AdmissionError("validator returned invalid payload image bindings")
        normalized_payloads[service] = image_id
    local_images = discovery.records.get("local_images")
    expected_payloads = {
        str(record["service"]): str(record["payload_image_id"])
        for record in local_images
        if isinstance(record, Mapping)
    }
    if normalized_payloads != expected_payloads:
        raise AdmissionError("validator payload image bindings differ from manifest")
    return ValidatedBindingsEvidence(
        bindings=types.MappingProxyType(dict(bindings)),
        build_attestation_raw=_evidence_raw(
            getattr(discovery, "build_attestation_raw", None),
            label="build attestation",
        ),
        build_attestation_identity=_evidence_identity(
            getattr(discovery, "build_attestation_identity", None),
            label="build attestation",
        ),
        build_manifest_raw=_evidence_raw(
            getattr(discovery, "build_manifest_raw", None),
            label="build manifest",
        ),
        build_manifest_identity=_evidence_identity(
            getattr(discovery, "build_manifest_identity", None),
            label="build manifest",
        ),
        deployment_attestation_raw=_evidence_raw(
            getattr(discovery, "deployment_attestation_raw", None),
            label="deployment attestation",
        ),
        deployment_attestation_identity=_evidence_identity(
            getattr(discovery, "deployment_attestation_identity", None),
            label="deployment attestation",
        ),
        validated_release_revision=release_revision,
        validated_payload_revision=payload_revision,
        validated_manifest_sha256=manifest_digest,
        validated_source_tree_sha256=source_tree_digest,
        validated_payload_image_ids=types.MappingProxyType(normalized_payloads),
    )


def compose_override_bytes(
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> bytes:
    if set(bindings) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("override bindings must name every protected service")
    selected = tuple(services)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(service not in _PROTECTED_SERVICE_SET for service in selected)
    ):
        raise AdmissionError("override services must be a unique protected subset")
    lines = ["services:"]
    for service in selected:
        image = bindings[service]
        if not isinstance(image, str) or _PINNED_IMAGE.fullmatch(image) is None:
            raise AdmissionError(f"protected service has a mutable image: {service}")
        lines.extend(
            (
                f"  {service}:",
                "    build: !reset null",
                f"    image: {json.dumps(image, ensure_ascii=True)}",
            )
        )
    return ("\n".join(lines) + "\n").encode("ascii")


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise AdmissionError("short write while creating admission output")
        offset += written


def _open_protected_output_parent(path: Path) -> tuple[int, str]:
    """Walk an absolute output path without symlinks and return its parent fd."""

    try:
        return provenance.open_protected_parent(path, label="admission output")
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    except OSError as exc:
        raise AdmissionError(
            f"admission output has an unsafe or symlinked parent: {path}"
        ) from exc


def write_new_regular_file(path: Path, payload: bytes) -> None:
    """Publish a complete mode-0600 file atomically without overwriting."""

    if not path.is_absolute() or not path.name:
        raise AdmissionError("admission output path must be an absolute file path")
    directory, output_name = _open_protected_output_parent(path)
    temporary = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    descriptor: int | None = None
    linked = False
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        try:
            os.link(
                temporary,
                output_name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                raise AdmissionError(
                    f"admission output already exists and will not be overwritten: {path}"
                ) from exc
            raise AdmissionError(f"cannot publish admission output: {path}") from exc
        linked = True
        os.fsync(directory)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass
        if linked:
            os.fsync(directory)
        os.close(directory)
    try:
        captured = provenance.read_protected_regular_file(
            path, label="admission output"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not hmac.compare_digest(captured, payload):
        raise AdmissionError("published admission output differs from requested bytes")


def verify_override_snapshot(
    path: Path,
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> tuple[bytes, tuple[int, ...]]:
    """Verify one protected override read and return that exact snapshot."""

    try:
        actual, identity = provenance.read_protected_regular_file_snapshot(
            path, label="production Compose override"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    expected = compose_override_bytes(bindings, services)
    if not hmac.compare_digest(actual, expected):
        raise AdmissionError(
            "production Compose override differs from the attested digest-only model"
        )
    return actual, identity


def verify_override(
    path: Path,
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> None:
    verify_override_snapshot(path, bindings, services)


def _string_sequence(value: object, *, label: str) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise AdmissionError(f"{label} must be a string sequence")
    return tuple(value)


def _normal_capabilities(value: object, *, label: str) -> frozenset[str]:
    sequence = _string_sequence(value, label=label) or ()
    normalized: set[str] = set()
    for item in sequence:
        capability = item.upper().removeprefix("CAP_")
        if not capability or capability in normalized:
            raise AdmissionError(f"{label} contains duplicate or invalid capabilities")
        normalized.add(capability)
    return frozenset(normalized)


def _forbidden_environment_names(
    environment: Mapping[str, str], *, include_empty: bool
) -> set[str]:
    exact = {
        "BASH_ENV",
        "BASHOPTS",
        "CDPATH",
        "ENV",
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "IFS",
        "LOCPATH",
        "MALLOC_TRACE",
        "NODE_OPTIONS",
        "PERL5OPT",
        "PYTHONHOME",
        "PYTHONBREAKPOINT",
        "PYTHONCASEOK",
        "PYTHONDEBUG",
        "PYTHONINSPECT",
        "PYTHONPATH",
        "PYTHONPLATLIBDIR",
        "PYTHONSTARTUP",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
        "ALL_PROXY",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "JDK_JAVA_OPTIONS",
        "JAVA_TOOL_OPTIONS",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
        "_JAVA_OPTIONS",
        "all_proxy",
        "http_proxy",
        "https_proxy",
        "RUBYOPT",
        "SHELLOPTS",
    }
    return {
        name
        for name, value in environment.items()
        if (include_empty or value)
        and (name in exact or name.startswith(("LD_", "DYLD_")))
    }


def _validate_rendered_environment(
    environment: Mapping[str, str], *, service: str
) -> None:
    expected_names = _EXPECTED_ENVIRONMENT_NAMES[service]
    legacy_scheduler_names = (
        expected_names
        - {
            "WHOSCORED_SCHEDULED_PAID_MODE",
            "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT",
        }
    ) | {"WHOSCORED_PROXY_APPROVAL_PATH"}
    legacy_scheduler = (
        service == "airflow-scheduler"
        and set(environment) == legacy_scheduler_names
    )
    if set(environment) != expected_names and not legacy_scheduler:
        raise AdmissionError(f"rendered environment names differ: {service}")
    if any(
        environment.get(name) != value
        for name, value in _FIXED_ENVIRONMENT[service].items()
        if not legacy_scheduler
        or name
        not in {
            "WHOSCORED_SCHEDULED_PAID_MODE",
            "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT",
        }
    ):
        raise AdmissionError(f"rendered security environment differs: {service}")
    if service == "airflow-scheduler" and environment.get(
        "WHOSCORED_SOURCE_POOL_SLOTS"
    ) not in {"2", "3", "4"}:
        raise AdmissionError("rendered WhoScored source-pool size differs")
    if service in {"airflow-scheduler", "whoscored_paid_gateway"} and environment.get(
        "WHOSCORED_PAID_BATCH_ENABLED"
    ) not in {"0", "1"}:
        raise AdmissionError("rendered WhoScored paid-batch control differs")
    if service == "airflow-scheduler":
        approval_path = environment.get("WHOSCORED_PROXY_APPROVAL_PATH", "")
        if (
            legacy_scheduler
            and approval_path
            and _WHOSCORED_APPROVAL_PATH_RE.fullmatch(approval_path) is None
        ):
            raise AdmissionError("rendered WhoScored approval path differs")
        if len(environment.get("FBREF_PROXY_CONTROL_TOKEN", "").strip()) < 32:
            raise AdmissionError("rendered FBref proxy-control token is invalid")
        tm_boolean_names = (
            "TM_NATIVE_V2_ENABLED",
            "TM_STANDING_POLICY_ENABLED",
            "TM_REQUIRE_METERED_PROXY",
        )
        if any(
            environment.get(name) not in {"true", "false"} for name in tm_boolean_names
        ):
            raise AdmissionError("rendered Transfermarkt boolean controls differ")
        if environment.get("TM_NATIVE_V2_ENABLED") == "true" and (
            environment.get("TM_STANDING_POLICY_ENABLED") != "true"
            or environment.get("TM_REQUIRE_METERED_PROXY") != "true"
            or environment.get("TM_PROXY_CONTROL_URL") != "http://proxy_filter:8899"
            or len(environment.get("TM_PROXY_CONTROL_TOKEN", "").strip()) < 32
        ):
            raise AdmissionError(
                "rendered Transfermarkt paid controls are not fail-closed"
            )
    if service == "whoscored_proxy_filter":
        if (
            re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
                environment.get("WHOSCORED_PROVIDER_ORDER_ID", ""),
            )
            is None
            or _DIGEST.fullmatch(
                environment.get("WHOSCORED_PROVIDER_POLICY_SHA256", "")
            )
            is None
        ):
            raise AdmissionError("rendered WhoScored provider-policy identity differs")
        _positive_capped_decimal(
            environment.get("WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES", ""),
            flag="daily-budget-bytes",
            maximum=300_000_000,
        )
        if environment.get("WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES") != "2000000":
            raise AdmissionError("rendered max-lease-bytes differs from policy")
    if (
        service == "airflow-scheduler"
        and len(environment.get("WHOSCORED_PAID_GATEWAY_TOKEN", "").strip()) < 32
    ):
        raise AdmissionError("rendered WhoScored paid-gateway token is invalid")
    if service == "whoscored_paid_gateway":
        authority_root = Path("/opt/airflow/secure/whoscored-alert-authority")
        authority_paths = tuple(
            environment.get(name, "")
            for name in (
                "WHOSCORED_PAID_ALERT_SECRET_PATH",
                "WHOSCORED_PAID_ALERT_BINDING_PATH",
            )
        )
        if len(set(authority_paths)) != 2 or any(
            not value
            or value != Path(value).as_posix()
            or ".." in Path(value).parts
            or authority_root not in Path(value).parents
            for value in authority_paths
        ):
            raise AdmissionError(
                f"rendered paid-alert authority paths are invalid: {service}"
            )
    secret_names = {
        "flaresolverr_whoscored_paid": ("WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",),
        "whoscored_paid_gateway": (
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_PAID_ALERT_HMAC_SECRET",
            "WHOSCORED_PAID_GATEWAY_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CONTROL_TOKEN",
        ),
        "whoscored_proxy_filter": (
            "PROXY_FILTER_CONTROL_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        ),
    }
    if any(
        len(environment.get(name, "").strip()) < 32
        for name in secret_names.get(service, ())
    ):
        raise AdmissionError(f"rendered boundary secret is invalid: {service}")


def _rendered_volumes(
    model: Mapping[str, Any], *, service: str
) -> tuple[tuple[Any, ...], ...]:
    volumes = model.get("volumes")
    if volumes is None:
        return ()
    if not isinstance(volumes, list):
        raise AdmissionError(f"rendered volumes are invalid: {service}")
    result: list[tuple[Any, ...]] = []
    targets: set[str] = set()
    for volume in volumes:
        if not isinstance(volume, dict):
            raise AdmissionError(f"rendered volume is invalid: {service}")
        volume_type = volume.get("type")
        source = volume.get("source", "")
        target = volume.get("target")
        read_only = volume.get("read_only", False)
        if volume_type not in ("bind", "volume"):
            raise AdmissionError(f"rendered volume identity is invalid: {service}")
        expected_keys = {"source", "target", "type", volume_type}
        if read_only:
            expected_keys.add("read_only")
        if (
            set(volume) != expected_keys
            or not isinstance(source, str)
            or not source
            or volume_type == "bind"
            and not Path(source).is_absolute()
            or not isinstance(target, str)
            or not target.startswith("/")
            or target in targets
            or not isinstance(read_only, bool)
        ):
            raise AdmissionError(f"rendered volume identity is invalid: {service}")
        options = volume.get(volume_type)
        if (
            volume_type == "bind"
            and options not in ({}, {"create_host_path": False})
            or volume_type == "volume"
            and options != {}
        ):
            raise AdmissionError(f"rendered volume options differ: {service}")
        targets.add(target)
        result.append((volume_type, source, target, read_only))
    return tuple(sorted(result))


def _tmpfs_model(
    model: Mapping[str, Any], *, service: str
) -> dict[str, frozenset[str]]:
    entries = model.get("tmpfs")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        raise AdmissionError(f"rendered tmpfs is invalid: {service}")
    result: dict[str, frozenset[str]] = {}
    for entry in entries:
        if not isinstance(entry, str):
            raise AdmissionError(f"rendered tmpfs entry is invalid: {service}")
        target, separator, raw_options = entry.partition(":")
        options = raw_options.split(",") if separator else []
        if (
            not target.startswith("/")
            or target in result
            or not options
            or any(not option for option in options)
            or len(options) != len(set(options))
        ):
            raise AdmissionError(f"rendered tmpfs entry is invalid: {service}")
        result[target] = frozenset(options)
    return result


def _mount_shadows_image_path(service: str, target: str) -> bool:
    normalized = target.rstrip("/") or "/"
    return any(
        protected == normalized
        or protected.startswith(normalized + "/")
        or normalized.startswith(protected + "/")
        for protected in _CRITICAL_IMAGE_PATHS[service]
    )


def _duration_nanoseconds(value: object, *, label: str) -> int:
    if not isinstance(value, str) or not value:
        raise AdmissionError(f"{label} is not a duration")
    units = {
        "h": 3_600_000_000_000,
        "m": 60_000_000_000,
        "s": 1_000_000_000,
        "ms": 1_000_000,
        "us": 1_000,
        "ns": 1,
    }
    position = 0
    total = 0
    pattern = re.compile(r"([0-9]+)(h|ms|us|ns|m|s)")
    for match in pattern.finditer(value):
        if match.start() != position:
            raise AdmissionError(f"{label} is not a canonical duration")
        total += int(match.group(1)) * units[match.group(2)]
        position = match.end()
    if position != len(value) or total <= 0:
        raise AdmissionError(f"{label} is not a canonical duration")
    return total


def _healthcheck_projection(
    model: Mapping[str, Any], *, service: str
) -> dict[str, Any]:
    healthcheck = model.get("healthcheck")
    if not isinstance(healthcheck, dict) or set(healthcheck) != {
        "interval",
        "retries",
        "start_period",
        "test",
        "timeout",
    }:
        raise AdmissionError(f"rendered healthcheck schema differs: {service}")
    test = _string_sequence(
        healthcheck.get("test"), label=f"rendered healthcheck test for {service}"
    )
    retries = healthcheck.get("retries")
    if not isinstance(retries, int) or isinstance(retries, bool) or retries <= 0:
        raise AdmissionError(f"rendered healthcheck retries differ: {service}")
    projection = {
        "Interval": _duration_nanoseconds(
            healthcheck.get("interval"), label=f"healthcheck interval for {service}"
        ),
        "Retries": retries,
        "StartPeriod": _duration_nanoseconds(
            healthcheck.get("start_period"),
            label=f"healthcheck start period for {service}",
        ),
        "Test": test,
        "Timeout": _duration_nanoseconds(
            healthcheck.get("timeout"), label=f"healthcheck timeout for {service}"
        ),
    }
    if projection != _EXPECTED_HEALTHCHECKS[service]:
        raise AdmissionError(f"rendered healthcheck policy differs: {service}")
    return {
        **projection,
        "Test": tuple(item.replace("$$", "$") for item in projection["Test"] or ()),
    }


def _positive_capped_decimal(value: str, *, flag: str, maximum: int) -> None:
    if re.fullmatch(r"[1-9][0-9]*", value) is None or int(value) > maximum:
        raise AdmissionError(f"rendered {flag} exceeds admission policy")


def _command_projection(
    command: tuple[str, ...] | None, *, service: str
) -> tuple[str, ...]:
    if service in {"flaresolverr", "flaresolverr_whoscored_paid"}:
        if command is not None:
            raise AdmissionError(
                "rendered FlareSolverr command bypasses baked preflight"
            )
        return _EXPECTED_COMMANDS[service]
    if service == "airflow-scheduler":
        if command != _EXPECTED_COMMANDS[service]:
            raise AdmissionError("rendered scheduler command differs")
        return command
    if service == "whoscored_paid_gateway":
        if command != _EXPECTED_COMMANDS[service]:
            raise AdmissionError("rendered WhoScored paid-gateway command differs")
        return command
    template = tuple(provenance.WHOSCORED_PROXY_COMMAND)
    if command is None or len(command) != len(template):
        raise AdmissionError("rendered WhoScored proxy command differs")
    variable_limits = {
        "${WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES:?set exact provider-policy daily cap in decimal bytes}": (
            "--daily-budget-bytes",
            300_000_000,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES:-2000000}": (
            "--max-lease-bytes",
            2_000_000,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_TTL_SECONDS:-3600}": (
            "--max-lease-ttl-seconds",
            3_600,
        ),
        "${WHOSCORED_PROXY_FILTER_DAGRUN_BUDGET_BYTES:-1000000000}": (
            "--dagrun-budget-bytes",
            1_000_000_000,
        ),
        "${WHOSCORED_PROXY_FILTER_URL_BUDGET_BYTES:-2000000}": (
            "--url-budget-bytes",
            2_000_000,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_ACTIVE_LEASES:-2}": (
            "--max-active-leases",
            2,
        ),
    }
    for index, expected in enumerate(template):
        variable = variable_limits.get(expected)
        if variable is None:
            if command[index] != expected:
                raise AdmissionError("rendered WhoScored proxy command differs")
            continue
        flag, maximum = variable
        if index == 0 or template[index - 1] != flag:
            raise AdmissionError(
                "WhoScored proxy command template is internally invalid"
            )
        _positive_capped_decimal(command[index], flag=flag, maximum=maximum)
    return command


def verify_rendered_compose(
    rendered: Mapping[str, Any], bindings: Mapping[str, str]
) -> dict[str, dict[str, Any]]:
    services = rendered.get("services")
    if not isinstance(services, dict):
        raise AdmissionError("rendered Compose model has no services mapping")
    networks = rendered.get("networks")
    if not isinstance(networks, dict) or any(
        networks.get(name) != policy
        for name, policy in _EXPECTED_NETWORK_DEFINITIONS.items()
    ):
        raise AdmissionError("rendered protected network definitions differ")
    volumes = rendered.get("volumes")
    if not isinstance(volumes, dict) or volumes.get("soccerdata_cache") != {
        "name": "soccerdata_cache"
    }:
        raise AdmissionError("rendered protected volume definition differs")
    projections: dict[str, dict[str, Any]] = {}
    for service in PROTECTED_SERVICES:
        model = services.get(service)
        if not isinstance(model, dict):
            raise AdmissionError(
                f"rendered Compose model omits protected service: {service}"
            )
        if model.get("post_start") is not None or model.get("pre_stop") is not None:
            raise AdmissionError(
                f"rendered protected service has lifecycle hooks: {service}"
            )
        if model.get("build") is not None:
            raise AdmissionError(
                f"rendered protected service still has a build: {service}"
            )
        modeled_keys = set(model)
        if model.get("profiles") == ["whoscored-paid"]:
            modeled_keys.discard("profiles")
        if (
            service == "whoscored_proxy_filter"
            and model.get("depends_on")
            == {
                "airflow-log-init": {
                    "condition": "service_completed_successfully",
                    "required": True,
                }
            }
        ):
            modeled_keys.discard("depends_on")
        if modeled_keys != _ALLOWED_RENDERED_KEYS[service]:
            raise AdmissionError(
                f"rendered protected service has unmodeled fields: {service}"
            )
        if model.get("image") != bindings[service]:
            raise AdmissionError(
                f"rendered Compose image differs from deployment attestation: {service}"
            )
        if model.get("entrypoint") is not None:
            raise AdmissionError(
                f"rendered protected service overrides its image entrypoint: {service}"
            )
        if model.get("container_name") != service:
            raise AdmissionError(f"rendered container name differs: {service}")
        observed_depends_on = model.get("depends_on")
        legacy_filter_depends_on = {
            "airflow-log-init": {
                "condition": "service_completed_successfully",
                "required": True,
            }
        }
        if observed_depends_on != _EXPECTED_DEPENDS_ON[service] and not (
            service == "whoscored_proxy_filter"
            and observed_depends_on == legacy_filter_depends_on
        ):
            raise AdmissionError(f"rendered dependency policy differs: {service}")
        if model.get("deploy") != _EXPECTED_DEPLOY[service]:
            raise AdmissionError(f"rendered resource policy differs: {service}")
        if model.get("networks") != _EXPECTED_NETWORKS[service]:
            raise AdmissionError(f"rendered network policy differs: {service}")
        if model.get("restart") != "unless-stopped":
            raise AdmissionError(f"rendered restart policy differs: {service}")
        environment = model.get("environment")
        if not isinstance(environment, dict):
            raise AdmissionError(f"rendered environment policy differs: {service}")
        if any(
            not isinstance(name, str) or not isinstance(value, str)
            for name, value in environment.items()
        ):
            raise AdmissionError(f"rendered environment values differ: {service}")
        if _forbidden_environment_names(environment, include_empty=True):
            raise AdmissionError(f"rendered environment has loader controls: {service}")
        _validate_rendered_environment(environment, service=service)
        if model.get("profiles") not in (None, ["whoscored-paid"]):
            raise AdmissionError(f"rendered protected profile differs: {service}")
        raw_shm_size = model.get("shm_size")
        if service in {"whoscored_paid_gateway", "whoscored_proxy_filter"}:
            if raw_shm_size is not None:
                raise AdmissionError(
                    f"rendered paid boundary shm_size differs: {service}"
                )
        elif raw_shm_size != str(_EXPECTED_SHM_SIZE[service]):
            raise AdmissionError(f"rendered shm_size differs: {service}")
        if service == "airflow-scheduler":
            if model.get("memswap_limit") != "17179869184":
                raise AdmissionError("rendered scheduler memswap limit differs")
        elif model.get("memswap_limit") is not None:
            raise AdmissionError(f"rendered memswap limit differs: {service}")
        rendered_ports = model.get("ports")
        if service == "flaresolverr":
            if rendered_ports != [
                {
                    "host_ip": "127.0.0.1",
                    "mode": "ingress",
                    "protocol": "tcp",
                    "published": "8191",
                    "target": 8191,
                }
            ]:
                raise AdmissionError("rendered FlareSolverr port policy differs")
        elif rendered_ports is not None:
            raise AdmissionError(f"rendered protected service adds ports: {service}")
        command = _string_sequence(
            model.get("command"), label=f"rendered command for {service}"
        )
        effective_command = _command_projection(command, service=service)
        policy = _SECURITY_POLICY[service]
        privileged = model.get("privileged")
        if privileged is not None and privileged is not False:
            raise AdmissionError(f"rendered protected service is privileged: {service}")
        if bool(model.get("read_only", False)) is not policy["read_only"]:
            raise AdmissionError(f"rendered root filesystem policy differs: {service}")
        cap_add = _normal_capabilities(
            model.get("cap_add"), label=f"rendered cap_add for {service}"
        )
        cap_drop = _normal_capabilities(
            model.get("cap_drop"), label=f"rendered cap_drop for {service}"
        )
        security_opt = (
            _string_sequence(
                model.get("security_opt"), label=f"rendered security_opt for {service}"
            )
            or ()
        )
        if cap_add != policy["cap_add"] or cap_drop != frozenset({"ALL"}):
            raise AdmissionError(f"rendered capability policy differs: {service}")
        if security_opt != _EXPECTED_SECURITY_OPT:
            raise AdmissionError(f"rendered security options differ: {service}")
        if model.get("configs") or model.get("secrets"):
            raise AdmissionError(
                f"rendered protected service adds configs/secrets: {service}"
            )
        volumes = _rendered_volumes(model, service=service)
        tmpfs = _tmpfs_model(model, service=service)
        for _kind, _source, target, _read_only in volumes:
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"rendered mount shadows image trust path: {service}"
                )
        for target in tmpfs:
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"rendered tmpfs shadows image trust path: {service}"
                )
        volume_policy = {
            target: (kind, read_only) for kind, _source, target, read_only in volumes
        }
        expected_volume_policy = _ALLOWED_VOLUME_TARGETS[service]
        legacy_volume_policy = dict(expected_volume_policy)
        if service == "airflow-scheduler":
            legacy_volume_policy.pop(
                "/opt/airflow/secure/whoscored-scheduled-pointers", None
            )
        if volume_policy != expected_volume_policy and volume_policy != legacy_volume_policy:
            raise AdmissionError(f"rendered mount-target policy differs: {service}")
        if tmpfs != _ALLOWED_TMPFS[service]:
            raise AdmissionError(f"rendered tmpfs policy differs: {service}")
        projections[service] = {
            "cap_add": cap_add,
            "cap_drop": cap_drop,
            "command": effective_command,
            "environment": dict(environment),
            "healthcheck": _healthcheck_projection(model, service=service),
            "port_bindings": _EXPECTED_PORT_BINDINGS[service],
            "network_names": tuple(
                _EXPECTED_NETWORK_DEFINITIONS[name]["name"]
                for name in _EXPECTED_NETWORKS[service]
            ),
            "read_only": policy["read_only"],
            "security_opt": security_opt,
            "tmpfs": tmpfs,
            "volumes": volumes,
            "shm_size": _EXPECTED_SHM_SIZE[service],
        }
    scheduler_environment = projections["airflow-scheduler"]["environment"]
    gateway_environment = projections["whoscored_paid_gateway"]["environment"]
    paid_browser_environment = projections["flaresolverr_whoscored_paid"]["environment"]
    filter_environment = projections["whoscored_proxy_filter"]["environment"]
    if (
        scheduler_environment["WHOSCORED_PAID_GATEWAY_TOKEN"]
        != gateway_environment["WHOSCORED_PAID_GATEWAY_TOKEN"]
        or scheduler_environment["WHOSCORED_PAID_BATCH_ENABLED"]
        != gateway_environment["WHOSCORED_PAID_BATCH_ENABLED"]
        or gateway_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"]
        != paid_browser_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"]
        or gateway_environment["WHOSCORED_PROXY_CONTROL_TOKEN"]
        != filter_environment["PROXY_FILTER_CONTROL_TOKEN"]
        or gateway_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"]
        != filter_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"]
    ):
        raise AdmissionError("rendered paid-boundary credentials differ")
    paid_secrets = tuple(
        value.encode("utf-8")
        for value in (
            scheduler_environment["WHOSCORED_PAID_GATEWAY_TOKEN"],
            gateway_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"],
            gateway_environment["WHOSCORED_PROXY_CONTROL_TOKEN"],
            gateway_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"],
            gateway_environment["WHOSCORED_PAID_ALERT_HMAC_SECRET"],
            filter_environment["WHOSCORED_PROXY_LEDGER_HMAC_SECRET"],
        )
    )
    generic_scheduler_secrets = tuple(
        scheduler_environment[name].encode("utf-8")
        for name in (
            "PROXY_FILTER_CONTROL_TOKEN",
            "SOFASCORE_PROXY_CONTROL_TOKEN",
        )
    )
    if any(
        hmac.compare_digest(left, right)
        for index, left in enumerate(paid_secrets)
        for right in paid_secrets[index + 1 :]
    ) or any(
        hmac.compare_digest(paid, generic)
        for paid in paid_secrets
        for generic in generic_scheduler_secrets
    ):
        raise AdmissionError("rendered paid-boundary secrets are not distinct")
    return projections


DockerRunner = Callable[[Sequence[str]], bytes]


def _assert_clean_control_environment() -> None:
    supplied = sorted(
        name
        for name in os.environ
        if name in _FORBIDDEN_CONTROL_ENV or name.startswith(("LD_", "DYLD_"))
    )
    if supplied:
        raise AdmissionError(
            "host control environment must be unset: " + ", ".join(supplied)
        )


def _trusted_docker_environment() -> dict[str, str]:
    _assert_clean_control_environment()
    try:
        binary = _DOCKER_CLI.lstat()
        socket = _DOCKER_SOCKET.lstat()
    except OSError as exc:
        raise AdmissionError(
            "trusted Docker CLI or daemon socket is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(binary.st_mode)
        or binary.st_uid != 0
        or binary.st_mode & 0o022
        or not binary.st_mode & 0o111
    ):
        raise AdmissionError("trusted Docker CLI identity is invalid")
    if (
        not stat.S_ISSOCK(socket.st_mode)
        or socket.st_uid != 0
        or socket.st_mode & 0o002
    ):
        raise AdmissionError("trusted Docker daemon socket identity is invalid")
    return {
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }


def _run_docker(arguments: Sequence[str]) -> bytes:
    environment = _trusted_docker_environment()
    try:
        result = subprocess.run(
            (str(_DOCKER_CLI), *arguments),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=environment,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdmissionError(
            f"Docker inspection failed: {' '.join(arguments)}"
        ) from exc
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise AdmissionError(
            f"Docker inspection failed ({result.returncode}): {message or arguments[0]}"
        )
    if len(result.stdout) > MAX_JSON_BYTES:
        raise AdmissionError("Docker inspection output is unreasonably large")
    return result.stdout


def _compose_arguments(
    *,
    root: Path,
    override_path: Path,
    env_files: Sequence[Path],
    project: str,
) -> tuple[str, ...]:
    if _PROJECT_NAME.fullmatch(project) is None:
        raise AdmissionError("Compose project name is invalid")
    config_files = (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        override_path,
    )
    all_paths = (*config_files, *env_files)
    if any(not path.is_absolute() or "," in str(path) for path in all_paths):
        raise AdmissionError("Compose evidence paths must be absolute and comma-free")
    if (
        not env_files
        or len(env_files) != len(set(env_files))
        or len(all_paths) != len(set(all_paths))
    ):
        raise AdmissionError("Compose env files must be a non-empty unique sequence")
    for path in all_paths:
        _read_regular_file(path, label="Compose admission input")
    arguments: list[str] = ["compose", "--project-name", project]
    for env_file in env_files:
        arguments.extend(("--env-file", str(env_file)))
    arguments.extend(("--profile", "whoscored-paid"))
    for config_file in config_files:
        arguments.extend(("--file", str(config_file)))
    return tuple(arguments)


def render_attested_compose(
    bindings: Mapping[str, str],
    *,
    root: Path,
    override_path: Path,
    env_files: Sequence[Path],
    project: str,
    runner: DockerRunner = _run_docker,
    protected_inputs: Mapping[Path, bytes] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], tuple[Path, ...], dict[str, Any]]:
    """Render only the fixed production file set and capture Compose hashes."""

    verify_override(override_path, bindings)
    prefix = _compose_arguments(
        root=root,
        override_path=override_path,
        env_files=env_files,
        project=project,
    )
    config_files = (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        override_path,
    )
    input_paths = (*config_files, *env_files)
    snapshots = {
        path: _read_regular_file(path, label="Compose admission input")
        for path in input_paths
    }
    if protected_inputs is not None and (
        set(protected_inputs) != set(input_paths)
        or any(
            not hmac.compare_digest(protected_inputs[path], snapshots[path][0])
            for path in input_paths
        )
    ):
        raise AdmissionError("protected Compose inputs differ from render snapshots")

    def assert_inputs_unchanged() -> None:
        for path, (expected_raw, expected_identity) in snapshots.items():
            actual_raw, actual_identity = _read_regular_file(
                path, label="Compose admission input"
            )
            if actual_identity != expected_identity or not hmac.compare_digest(
                actual_raw, expected_raw
            ):
                raise AdmissionError(f"Compose admission input changed: {path}")

    raw = runner((*prefix, "config", "--format", "json"))
    assert_inputs_unchanged()
    try:
        rendered = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("Docker Compose returned ambiguous rendered JSON") from exc
    if not isinstance(rendered, dict):
        raise AdmissionError("Docker Compose rendered model is not an object")
    projections = verify_rendered_compose(rendered, bindings)
    if protected_inputs is not None:
        _validate_bind_source_policy(projections, root=root)
    config_hashes: dict[str, str] = {}
    for service in PROTECTED_SERVICES:
        raw_hash = runner((*prefix, "config", "--hash", service))
        assert_inputs_unchanged()
        try:
            line = raw_hash.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"Compose config hash is non-ASCII: {service}"
            ) from exc
        fields = line.split()
        if (
            len(fields) != 2
            or fields[0] != service
            or _CONFIG_HASH.fullmatch(fields[1]) is None
        ):
            raise AdmissionError(f"Compose config hash is invalid: {service}")
        config_hashes[service] = fields[1]
    return projections, config_hashes, config_files, rendered


def _fixed_project_arguments(
    *,
    root: Path,
    project: str,
    override_path: Path,
    env_files: Sequence[Path],
) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    if project == COMMON_PROJECT:
        config_files = (
            root / "compose.yaml",
            root / "compose.seaweedfs-supervised.yaml",
            override_path,
        )
    elif project == GATEWAY_PROJECT:
        config_files = (
            root / "deploy/whoscored/gateway.compose.yaml",
            override_path,
        )
    else:
        raise AdmissionError("Compose project is not a fixed WhoScored project")
    all_paths = (*config_files, *env_files)
    if any(not path.is_absolute() or "," in str(path) for path in all_paths):
        raise AdmissionError("Compose evidence paths must be absolute and comma-free")
    if (
        not env_files
        or len(env_files) != len(set(env_files))
        or len(all_paths) != len(set(all_paths))
    ):
        raise AdmissionError("Compose env files must be a non-empty unique sequence")
    for path in all_paths:
        _read_regular_file(path, label="Compose admission input")
    arguments: list[str] = [
        "compose",
        "--project-name",
        project,
        "--project-directory",
        str(root),
    ]
    for env_file in env_files:
        arguments.extend(("--env-file", str(env_file)))
    for config_file in config_files:
        arguments.extend(("--file", str(config_file)))
    return tuple(arguments), config_files


def render_attested_projects(
    bindings: Mapping[str, str],
    *,
    root: Path,
    common_override_path: Path,
    gateway_override_path: Path,
    env_files: Sequence[Path],
    provider_authority: Mapping[str, object],
    runner: DockerRunner = _run_docker,
    protected_inputs: Mapping[Path, bytes] | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str],
    dict[str, tuple[Path, ...]],
    dict[str, dict[str, Any]],
]:
    """Render and cross-check the fixed common and paid-gateway projects."""

    verify_override(common_override_path, bindings, COMMON_PROTECTED_SERVICES)
    verify_override(gateway_override_path, bindings, GATEWAY_PROTECTED_SERVICES)
    project_specs = {
        COMMON_PROJECT: (common_override_path, COMMON_PROTECTED_SERVICES),
        GATEWAY_PROJECT: (gateway_override_path, GATEWAY_PROTECTED_SERVICES),
    }
    rendered_projects: dict[str, dict[str, Any]] = {}
    config_hashes: dict[str, str] = {}
    config_files_by_project: dict[str, tuple[Path, ...]] = {}
    snapshots: dict[Path, tuple[bytes, tuple[int, ...]]] = {}
    for project, (override_path, services) in project_specs.items():
        prefix, config_files = _fixed_project_arguments(
            root=root,
            project=project,
            override_path=override_path,
            env_files=env_files,
        )
        config_files_by_project[project] = config_files
        for path in (*config_files, *env_files):
            snapshots.setdefault(
                path, _read_regular_file(path, label="Compose admission input")
            )
        raw = runner((*prefix, "config", "--format", "json"))
        try:
            rendered = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdmissionError(
                f"Docker Compose returned ambiguous rendered JSON: {project}"
            ) from exc
        if not isinstance(rendered, dict) or rendered.get("name") != project:
            raise AdmissionError(f"rendered Compose project identity differs: {project}")
        rendered_projects[project] = rendered
        for service in services:
            raw_hash = runner((*prefix, "config", "--hash", service))
            try:
                line = raw_hash.decode("ascii").strip()
            except UnicodeDecodeError as exc:
                raise AdmissionError(
                    f"Compose config hash is non-ASCII: {service}"
                ) from exc
            fields = line.split()
            if (
                len(fields) != 2
                or fields[0] != service
                or _CONFIG_HASH.fullmatch(fields[1]) is None
            ):
                raise AdmissionError(f"Compose config hash is invalid: {service}")
            config_hashes[service] = fields[1]
    if protected_inputs is not None and (
        set(protected_inputs) != set(snapshots)
        or any(
            not hmac.compare_digest(protected_inputs[path], snapshot[0])
            for path, snapshot in snapshots.items()
        )
    ):
        raise AdmissionError("protected Compose inputs differ from render snapshots")
    for path, (expected_raw, expected_identity) in snapshots.items():
        actual_raw, actual_identity = _read_regular_file(
            path, label="Compose admission input"
        )
        if actual_identity != expected_identity or not hmac.compare_digest(
            actual_raw, expected_raw
        ):
            raise AdmissionError(f"Compose admission input changed: {path}")

    common = rendered_projects[COMMON_PROJECT]
    gateway = rendered_projects[GATEWAY_PROJECT]
    common_services = common.get("services")
    gateway_services = gateway.get("services")
    if not isinstance(common_services, dict) or not isinstance(gateway_services, dict):
        raise AdmissionError("rendered split Compose service models differ")
    if any(service in common_services for service in GATEWAY_PROTECTED_SERVICES):
        raise AdmissionError("common project owns a paid-gateway service")
    if set(gateway_services) != set(GATEWAY_PROTECTED_SERVICES):
        raise AdmissionError("paid-gateway project service boundary differs")
    if any(service not in common_services for service in COMMON_PROTECTED_SERVICES):
        raise AdmissionError("common project omits a protected common service")
    if any(
        gateway_services[service].get("profiles") is not None
        for service in GATEWAY_PROTECTED_SERVICES
    ):
        raise AdmissionError("paid-gateway project retains an opt-in profile")
    if gateway_services["whoscored_proxy_filter"].get("depends_on") is not None:
        raise AdmissionError("paid filter depends on a common-project service")
    filter_environment = gateway_services["whoscored_proxy_filter"].get("environment")
    if not isinstance(filter_environment, dict) or any(
        filter_environment.get(name) != str(provider_authority.get(authority_name))
        for name, authority_name in (
            ("WHOSCORED_PROVIDER_ORDER_ID", "order_id"),
            ("WHOSCORED_PROVIDER_POLICY_SHA256", "provider_policy_sha256"),
            ("WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES", "daily_cap_bytes"),
        )
    ):
        raise AdmissionError("paid filter does not bind the admitted provider policy")
    scheduler_model = common_services["airflow-scheduler"]
    scheduler_environment = scheduler_model.get("environment")
    if (
        not isinstance(scheduler_environment, dict)
        or set(scheduler_environment) != _EXPECTED_ENVIRONMENT_NAMES["airflow-scheduler"]
        or scheduler_environment.get("WHOSCORED_SCHEDULED_PAID_MODE") != "required"
        or "WHOSCORED_PROXY_APPROVAL_PATH" in scheduler_environment
    ):
        raise AdmissionError("common scheduler paid authority differs")
    scheduler_volumes = scheduler_model.get("volumes")
    if not isinstance(scheduler_volumes, list) or not any(
        isinstance(volume, dict)
        and volume.get("target")
        == "/opt/airflow/secure/whoscored-scheduled-pointers"
        and volume.get("read_only") is True
        for volume in scheduler_volumes
    ):
        raise AdmissionError("common scheduler pointer authority mount differs")
    common_networks = common.get("networks")
    gateway_networks = gateway.get("networks")
    if not isinstance(common_networks, dict) or not isinstance(gateway_networks, dict):
        raise AdmissionError("rendered split Compose network models differ")
    if common_networks.get("whoscored-paid-api") != _COMMON_EXTERNAL_NETWORKS[
        "whoscored-paid-api"
    ]:
        raise AdmissionError("common project does not use external paid API network")
    if set(gateway_networks) != {
        "whoscored-paid-api",
        "whoscored-paid-browser",
        "whoscored-paid-direct-egress",
        "whoscored-paid-provider-egress",
    } or any(
        gateway_networks.get(name) != _EXPECTED_NETWORK_DEFINITIONS[name]
        for name in gateway_networks
    ):
        raise AdmissionError("paid-gateway project network ownership differs")
    combined = {
        "services": {
            **{
                service: common_services[service]
                for service in COMMON_PROTECTED_SERVICES
            },
            **{
                service: gateway_services[service]
                for service in GATEWAY_PROTECTED_SERVICES
            },
        },
        "networks": {
            **{
                name: common_networks[name]
                for name in ("backend", "frontend", "storage")
            },
            **{name: gateway_networks[name] for name in gateway_networks},
        },
        "volumes": common.get("volumes"),
    }
    projections = verify_rendered_compose(combined, bindings)
    if protected_inputs is not None:
        _validate_bind_source_policy(projections, root=root)
    return projections, config_hashes, config_files_by_project, rendered_projects


def _docker_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(f"{label} returned ambiguous Docker JSON") from exc
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise AdmissionError(f"{label} did not resolve to exactly one Docker object")
    return value[0]


def _verify_docker_security_options(*, runner: DockerRunner) -> tuple[str, ...]:
    """Prove the daemon applies its built-in seccomp and AppArmor defaults."""

    raw = runner(("info", "--format", "{{json .SecurityOptions}}"))
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("Docker daemon security options are invalid") from exc
    if (
        not isinstance(value, list)
        or not all(isinstance(option, str) and option for option in value)
        or len(value) != len(set(value))
    ):
        raise AdmissionError("Docker daemon security options are invalid")
    options = frozenset(value)
    security_families = {
        option
        for option in options
        if option.startswith(("name=apparmor", "name=seccomp"))
    }
    if (
        not _REQUIRED_DOCKER_SECURITY_OPTIONS.issubset(options)
        or security_families != _REQUIRED_DOCKER_SECURITY_OPTIONS
    ):
        raise AdmissionError(
            "Docker daemon must enable AppArmor and the built-in seccomp profile"
        )
    return tuple(sorted(options))


def _apparmor_probe_arguments(image: str) -> tuple[str, ...]:
    if _PINNED_IMAGE.fullmatch(image) is None:
        raise AdmissionError("AppArmor probe image is not digest-pinned")
    return (
        "run",
        "--rm",
        "--pull=never",
        "--network=none",
        "--read-only",
        "--user=50000:0",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges=true",
        "--security-opt=apparmor=docker-default",
        "--security-opt=seccomp=builtin",
        "--entrypoint=/bin/cat",
        image,
        "/proc/self/attr/current",
    )


def _verify_apparmor_enforcement(*, runner: DockerRunner, image: str) -> str:
    """Prove enforcement inside a constrained digest-attested container."""

    try:
        lines = runner(_apparmor_probe_arguments(image)).decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise AdmissionError(
            "AppArmor enforcement probe returned invalid output"
        ) from exc
    if lines != ["docker-default (enforce)"]:
        raise AdmissionError(
            "docker-default AppArmor probe must report enforce mode exactly once"
        )
    return lines[0]


def _environment_mapping(value: object, *, label: str) -> dict[str, str]:
    sequence = _string_sequence(value, label=label) or ()
    result: dict[str, str] = {}
    for item in sequence:
        name, separator, content = item.partition("=")
        if not separator or not name or name in result:
            raise AdmissionError(f"{label} contains duplicate or invalid entries")
        result[name] = content
    return result


def _verify_container_labels(
    labels: Mapping[str, Any],
    image_labels_value: object,
    *,
    service: str,
    project: str,
    config_hash: str,
    image_id: str,
    config_files: str,
    env_files: str,
    working_dir: str,
) -> None:
    if image_labels_value is None:
        image_labels: dict[str, str] = {}
    elif isinstance(image_labels_value, dict) and all(
        isinstance(name, str) and isinstance(value, str)
        for name, value in image_labels_value.items()
    ):
        image_labels = dict(image_labels_value)
    else:
        raise AdmissionError(f"image labels are invalid: {service}")
    if any(not name.startswith(_SAFE_IMAGE_LABEL_PREFIXES) for name in image_labels):
        raise AdmissionError(f"image has an unsafe integration label: {service}")
    compose_version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(compose_version)
        if isinstance(compose_version, str)
        else None
    )
    if version_match is None or tuple(
        int(version_match.group(index)) for index in (1, 2, 3)
    ) < (2, 24, 4):
        raise AdmissionError(f"container Compose version label differs: {service}")
    compose_labels = {
        "com.docker.compose.config-hash": config_hash,
        "com.docker.compose.container-number": "1",
        "com.docker.compose.depends_on": "",
        "com.docker.compose.image": image_id,
        "com.docker.compose.oneoff": "False",
        "com.docker.compose.project": project,
        "com.docker.compose.project.config_files": config_files,
        "com.docker.compose.project.environment_file": env_files,
        "com.docker.compose.project.working_dir": working_dir,
        "com.docker.compose.service": service,
        "com.docker.compose.version": compose_version,
    }
    replacement = labels.get("com.docker.compose.replace")
    if replacement is not None:
        if replacement != service:
            raise AdmissionError(f"container replacement label differs: {service}")
        compose_labels["com.docker.compose.replace"] = service
    expected = {**image_labels, **compose_labels}
    if labels != expected:
        raise AdmissionError(f"container has unexpected integration labels: {service}")


def _port_bindings(
    value: object, *, label: str
) -> dict[str, tuple[dict[str, str], ...]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise AdmissionError(f"{label} is invalid")
    result: dict[str, tuple[dict[str, str], ...]] = {}
    for port, bindings in value.items():
        if not isinstance(port, str) or not isinstance(bindings, list):
            raise AdmissionError(f"{label} is invalid")
        normalized: list[dict[str, str]] = []
        for binding in bindings:
            if not isinstance(binding, dict) or set(binding) != {"HostIp", "HostPort"}:
                raise AdmissionError(f"{label} is invalid")
            host_ip = binding.get("HostIp")
            host_port = binding.get("HostPort")
            if not isinstance(host_ip, str) or not isinstance(host_port, str):
                raise AdmissionError(f"{label} is invalid")
            normalized.append({"HostIp": host_ip, "HostPort": host_port})
        result[port] = tuple(normalized)
    return result


def _verify_docker_network(
    *, logical_name: str, project: str, runner: DockerRunner
) -> dict[str, str]:
    definition = _EXPECTED_NETWORK_DEFINITIONS[logical_name]
    expected_name = definition["name"]
    expected_internal = bool(definition.get("internal", False))
    network = _docker_object(
        runner(("network", "inspect", expected_name)),
        label=f"network inspect for {expected_name}",
    )
    network_id = network.get("Id")
    if (
        network_id is None
        or not isinstance(network_id, str)
        or _CONTAINER_ID.fullmatch(network_id) is None
        or network.get("Name") != expected_name
        or network.get("Driver") != "bridge"
        or network.get("Scope") != "local"
        or network.get("Internal") is not expected_internal
        or network.get("Attachable") is not False
        or network.get("Ingress") is not False
        or network.get("EnableIPv4") is not True
        or network.get("EnableIPv6") is not False
        or network.get("Options") != {}
    ):
        raise AdmissionError(f"Docker network policy differs: {expected_name}")
    labels = network.get("Labels")
    if not isinstance(labels, dict) or set(labels) != {
        "com.docker.compose.config-hash",
        "com.docker.compose.network",
        "com.docker.compose.project",
        "com.docker.compose.version",
    }:
        raise AdmissionError(f"Docker network labels differ: {expected_name}")
    version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(version) if isinstance(version, str) else None
    )
    if (
        labels.get("com.docker.compose.network") != logical_name
        or labels.get("com.docker.compose.project") != project
        or not isinstance(labels.get("com.docker.compose.config-hash"), str)
        or _CONFIG_HASH.fullmatch(labels["com.docker.compose.config-hash"]) is None
        or version_match is None
        or tuple(int(version_match.group(index)) for index in (1, 2, 3)) < (2, 24, 4)
    ):
        raise AdmissionError(f"Docker network identity differs: {expected_name}")
    ipam = network.get("IPAM")
    if (
        not isinstance(ipam, dict)
        or set(ipam) != {"Config", "Driver", "Options"}
        or ipam.get("Driver") != "default"
        or ipam.get("Options") is not None
    ):
        raise AdmissionError(f"Docker network IPAM differs: {expected_name}")
    configurations = ipam.get("Config")
    if not isinstance(configurations, list) or len(configurations) != 1:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    configuration = configurations[0]
    if not isinstance(configuration, dict) or set(configuration) != {
        "Gateway",
        "IPRange",
        "Subnet",
    }:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    try:
        subnet = ipaddress.ip_network(configuration["Subnet"], strict=True)
        gateway = ipaddress.ip_address(configuration["Gateway"])
        ip_range = configuration["IPRange"]
        selected_range = (
            ipaddress.ip_network(ip_range, strict=True) if ip_range else subnet
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}") from exc
    if (
        subnet.version != 4
        or not subnet.is_private
        or subnet.is_loopback
        or subnet.prefixlen < 16
        or gateway not in subnet
        or selected_range.version != 4
        or not selected_range.subnet_of(subnet)
    ):
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    return {
        "id": network_id,
        "logical_name": logical_name,
        "name": expected_name,
        "subnet": str(subnet),
    }


def _verify_soccerdata_volume(*, project: str, runner: DockerRunner) -> dict[str, str]:
    name = "soccerdata_cache"
    volume = _docker_object(
        runner(("volume", "inspect", name)),
        label="volume inspect for soccerdata_cache",
    )
    mountpoint = "/var/lib/docker/volumes/soccerdata_cache/_data"
    if (
        volume.get("Name") != name
        or volume.get("Driver") != "local"
        or volume.get("Scope") != "local"
        or volume.get("Options") is not None
        or volume.get("Mountpoint") != mountpoint
    ):
        raise AdmissionError("Docker soccerdata volume policy differs")
    labels = volume.get("Labels")
    if not isinstance(labels, dict) or set(labels) != {
        "com.docker.compose.config-hash",
        "com.docker.compose.project",
        "com.docker.compose.version",
        "com.docker.compose.volume",
    }:
        raise AdmissionError("Docker soccerdata volume labels differ")
    version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(version) if isinstance(version, str) else None
    )
    config_hash = labels.get("com.docker.compose.config-hash")
    if (
        labels.get("com.docker.compose.project") != project
        or labels.get("com.docker.compose.volume") != name
        or not isinstance(config_hash, str)
        or _CONFIG_HASH.fullmatch(config_hash) is None
        or version_match is None
        or tuple(int(version_match.group(index)) for index in (1, 2, 3)) < (2, 24, 4)
    ):
        raise AdmissionError("Docker soccerdata volume identity differs")
    return {"driver": "local", "mountpoint": mountpoint, "name": name}


def verify_created_containers(
    bindings: Mapping[str, str],
    *,
    project: str | Mapping[str, str],
    selected_services: Sequence[str],
    projections: Mapping[str, Mapping[str, Any]],
    config_hashes: Mapping[str, str],
    config_files: Sequence[Path] | Mapping[str, Sequence[Path]],
    env_files: Sequence[Path],
    runner: DockerRunner = _run_docker,
    expected_state: str = "created",
) -> dict[str, Any]:
    if set(bindings) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("post-create bindings omit a protected service")
    split_projects = isinstance(project, Mapping)
    if split_projects:
        service_projects = dict(project)
        if service_projects != _SERVICE_PROJECT:
            raise AdmissionError("split Compose project ownership differs")
        if not isinstance(config_files, Mapping) or set(config_files) != {
            COMMON_PROJECT,
            GATEWAY_PROJECT,
        }:
            raise AdmissionError("split Compose file ownership differs")
        project_config_files = {
            name: tuple(paths) for name, paths in config_files.items()
        }
        project_directory = project_config_files[COMMON_PROJECT][0].parent
    else:
        if not isinstance(project, str) or _PROJECT_NAME.fullmatch(project) is None:
            raise AdmissionError("Compose project name is invalid")
        if isinstance(config_files, Mapping):
            raise AdmissionError("single-project Compose files are invalid")
        service_projects = {service: project for service in PROTECTED_SERVICES}
        project_config_files = {project: tuple(config_files)}
        project_directory = tuple(config_files)[0].parent
    if expected_state not in {"created", "running"}:
        raise AdmissionError("container admission state must be created or running")
    selected = tuple(selected_services)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(service not in _PROTECTED_SERVICE_SET for service in selected)
    ):
        raise AdmissionError(
            "post-create services must be a non-empty unique protected subset"
        )
    if (
        set(projections) != _PROTECTED_SERVICE_SET
        or set(config_hashes) != _PROTECTED_SERVICE_SET
    ):
        raise AdmissionError("post-create model does not bind all protected services")
    docker_security_options = _verify_docker_security_options(runner=runner)
    apparmor_profile = _verify_apparmor_enforcement(
        runner=runner,
        image=bindings["airflow-scheduler"],
    )
    expected_env_files = ",".join(str(path) for path in env_files)
    logical_networks = sorted(
        {
            logical_name
            for service in selected
            for logical_name in _EXPECTED_NETWORKS[service]
        }
    )
    verified_networks = {
        record["name"]: record
        for logical_name in logical_networks
        for record in (
            _verify_docker_network(
                logical_name=logical_name,
                project=(
                    _NETWORK_PROJECT[logical_name]
                    if split_projects
                    else service_projects[selected[0]]
                ),
                runner=runner,
            ),
        )
    }
    verified_volumes = (
        [_verify_soccerdata_volume(project=COMMON_PROJECT, runner=runner)]
        if "airflow-scheduler" in selected
        else []
    )
    verified: list[dict[str, str]] = []
    for service in selected:
        service_project = service_projects[service]
        service_config_files = project_config_files[service_project]
        expected_config_files = ",".join(str(path) for path in service_config_files)
        raw_ids = runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={service_project}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            )
        )
        try:
            ids = raw_ids.decode("ascii").splitlines()
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"Docker returned a non-ASCII container ID: {service}"
            ) from exc
        if len(ids) != 1 or _CONTAINER_ID.fullmatch(ids[0]) is None:
            raise AdmissionError(
                f"protected service must have exactly one full container ID: {service}"
            )
        container_id = ids[0]
        container = _docker_object(
            runner(("container", "inspect", container_id)),
            label=f"container inspect for {service}",
        )
        config = container.get("Config")
        state = container.get("State")
        image_id = container.get("Image")
        if not isinstance(config, dict) or not isinstance(state, dict):
            raise AdmissionError(f"container metadata is incomplete: {service}")
        if container.get("AppArmorProfile") != "docker-default":
            raise AdmissionError(f"container AppArmor profile differs: {service}")
        labels = config.get("Labels")
        if (
            not isinstance(labels, dict)
            or labels.get("com.docker.compose.project") != service_project
            or labels.get("com.docker.compose.service") != service
        ):
            raise AdmissionError(f"container Compose identity differs: {service}")
        if labels.get("com.docker.compose.oneoff") != "False":
            raise AdmissionError(f"container is a one-off Compose bypass: {service}")
        if labels.get("com.docker.compose.container-number") != "1":
            raise AdmissionError(f"container ordinal differs from policy: {service}")
        if (
            labels.get("com.docker.compose.project.config_files")
            != expected_config_files
        ):
            raise AdmissionError(f"container Compose file set differs: {service}")
        if (
            labels.get("com.docker.compose.project.environment_file")
            != expected_env_files
        ):
            raise AdmissionError(f"container Compose env-file set differs: {service}")
        if labels.get("com.docker.compose.project.working_dir") != str(
            project_directory
        ):
            raise AdmissionError(
                f"container Compose working directory differs: {service}"
            )
        if labels.get("com.docker.compose.config-hash") != config_hashes[service]:
            raise AdmissionError(f"container Compose config hash differs: {service}")
        if config.get("Image") != bindings[service]:
            raise AdmissionError(f"container Config.Image is not attested: {service}")
        if tuple(config.get("Entrypoint") or ()) != _EXPECTED_ENTRYPOINTS[service]:
            raise AdmissionError(
                f"container Config.Entrypoint bypasses gate: {service}"
            )
        if tuple(config.get("Cmd") or ()) != projections[service]["command"]:
            raise AdmissionError(f"container Config.Cmd differs from policy: {service}")
        healthcheck = config.get("Healthcheck")
        if not isinstance(healthcheck, dict):
            raise AdmissionError(f"container healthcheck is missing: {service}")
        actual_healthcheck = {
            "Interval": healthcheck.get("Interval"),
            "Retries": healthcheck.get("Retries"),
            "StartPeriod": healthcheck.get("StartPeriod"),
            "Test": tuple(healthcheck.get("Test") or ()),
            "Timeout": healthcheck.get("Timeout"),
        }
        unexpected_healthcheck = set(healthcheck) - {
            *actual_healthcheck,
            "StartInterval",
        }
        if (
            unexpected_healthcheck
            or healthcheck.get("StartInterval", 0) != 0
            or actual_healthcheck != projections[service]["healthcheck"]
        ):
            raise AdmissionError(f"container healthcheck policy differs: {service}")
        if not isinstance(image_id, str) or _IMAGE_ID.fullmatch(image_id) is None:
            raise AdmissionError(
                f"container .Image is not an immutable image ID: {service}"
            )
        if expected_state == "created":
            if state.get("Status") != "created" or state.get("Running") is not False:
                raise AdmissionError(
                    "protected container was started before post-create admission: "
                    f"{service}"
                )
        else:
            unhealthy_flags = ("Paused", "Restarting", "Dead", "OOMKilled")
            health = state.get("Health")
            if (
                state.get("Status") != "running"
                or state.get("Running") is not True
                or any(state.get(field) is not False for field in unhealthy_flags)
                or not isinstance(health, dict)
                or health.get("Status") != "healthy"
            ):
                raise AdmissionError(
                    f"protected container is not healthy and running: {service}"
                )
        image = _docker_object(
            runner(("image", "inspect", bindings[service])),
            label=f"image inspect for {service}",
        )
        if image.get("Id") != image_id:
            raise AdmissionError(
                f"container .Image differs from digest-selected image .Id: {service}"
            )
        repo_digests = image.get("RepoDigests")
        if not isinstance(repo_digests, list) or bindings[service] not in repo_digests:
            raise AdmissionError(
                f"digest-selected image lacks its attested RepoDigest: {service}"
            )
        image_config = image.get("Config")
        if (
            not isinstance(image_config, dict)
            or tuple(image_config.get("Entrypoint") or ())
            != _EXPECTED_ENTRYPOINTS[service]
        ):
            raise AdmissionError(f"digest-selected image entrypoint differs: {service}")
        image_user = image_config.get("User")
        if (
            image_user != _EXPECTED_IMAGE_USER[service]
            or config.get("User") != image_user
        ):
            raise AdmissionError(
                f"container user differs from non-root image policy: {service}"
            )
        image_working_dir = image_config.get("WorkingDir")
        if (
            image_working_dir != _EXPECTED_WORKING_DIR[service]
            or config.get("WorkingDir") != image_working_dir
        ):
            raise AdmissionError(f"container working directory differs: {service}")
        _verify_container_labels(
            labels,
            image_config.get("Labels"),
            service=service,
            project=service_project,
            config_hash=config_hashes[service],
            image_id=image_id,
            config_files=expected_config_files,
            env_files=expected_env_files,
            working_dir=str(project_directory),
        )
        expected_environment = _environment_mapping(
            image_config.get("Env"), label=f"image environment for {service}"
        )
        if any(
            expected_environment.get(name) != value
            for name, value in _EXPECTED_IMAGE_ENVIRONMENT[service].items()
        ):
            raise AdmissionError(f"image hardening environment differs: {service}")
        rendered_environment = projections[service]["environment"]
        if any(
            name in expected_environment and expected_environment[name] != value
            for name, value in rendered_environment.items()
        ):
            raise AdmissionError(
                f"rendered environment overrides image policy: {service}"
            )
        expected_environment.update(rendered_environment)
        if _forbidden_environment_names(expected_environment, include_empty=False):
            raise AdmissionError(f"image environment has loader controls: {service}")
        actual_environment = _environment_mapping(
            config.get("Env"), label=f"container environment for {service}"
        )
        if actual_environment != expected_environment:
            raise AdmissionError(f"container environment projection differs: {service}")
        if config.get("OpenStdin") is not False or config.get("Tty") is not False:
            raise AdmissionError(f"container interactive mode differs: {service}")
        host_config = container.get("HostConfig")
        if not isinstance(host_config, dict):
            raise AdmissionError(f"container HostConfig is incomplete: {service}")
        projection = projections[service]
        if host_config.get("Privileged") is not False:
            raise AdmissionError(f"container is privileged: {service}")
        if host_config.get("ReadonlyRootfs") is not projection["read_only"]:
            raise AdmissionError(f"container root filesystem policy differs: {service}")
        cap_add = _normal_capabilities(
            host_config.get("CapAdd"), label=f"container CapAdd for {service}"
        )
        cap_drop = _normal_capabilities(
            host_config.get("CapDrop"), label=f"container CapDrop for {service}"
        )
        security_opt = (
            _string_sequence(
                host_config.get("SecurityOpt"),
                label=f"container SecurityOpt for {service}",
            )
            or ()
        )
        if cap_add != projection["cap_add"] or cap_drop != projection["cap_drop"]:
            raise AdmissionError(f"container capability policy differs: {service}")
        if security_opt != projection["security_opt"]:
            raise AdmissionError(f"container security options differ: {service}")
        for field in (
            "DeviceCgroupRules",
            "Devices",
            "DeviceRequests",
            "GroupAdd",
            "StorageOpt",
            "Sysctls",
            "Ulimits",
            "VolumesFrom",
        ):
            if host_config.get(field):
                raise AdmissionError(f"container adds forbidden {field}: {service}")
        for field in ("PidMode", "UTSMode", "UsernsMode", "CgroupParent"):
            if host_config.get(field) not in (None, ""):
                raise AdmissionError(f"container adds forbidden {field}: {service}")
        if host_config.get("IpcMode") not in (None, "", "private"):
            raise AdmissionError(f"container IPC mode differs: {service}")
        if host_config.get("CgroupnsMode") not in (None, "", "private"):
            raise AdmissionError(f"container cgroup namespace differs: {service}")
        network_mode = host_config.get("NetworkMode")
        if network_mode != _EXPECTED_NETWORK_MODE[service]:
            raise AdmissionError(f"container network mode differs: {service}")
        network_settings = container.get("NetworkSettings")
        attached_networks = (
            network_settings.get("Networks")
            if isinstance(network_settings, dict)
            else None
        )
        if not isinstance(attached_networks, dict) or set(attached_networks) != set(
            projection["network_names"]
        ):
            raise AdmissionError(f"container network attachments differ: {service}")
        for endpoint in attached_networks.values():
            aliases = endpoint.get("Aliases") if isinstance(endpoint, dict) else None
            if (
                not isinstance(aliases, list)
                or not aliases
                or any(alias != service for alias in aliases)
            ):
                raise AdmissionError(f"container network aliases differ: {service}")
        for network_name, endpoint in attached_networks.items():
            assert isinstance(endpoint, dict)
            expected_network = verified_networks[network_name]
            address = endpoint.get("IPAddress")
            try:
                address_in_subnet = (
                    address == ""
                    or isinstance(address, str)
                    and ipaddress.ip_address(address)
                    in ipaddress.ip_network(expected_network["subnet"])
                )
            except ValueError:
                address_in_subnet = False
            if (
                endpoint.get("NetworkID") not in ("", expected_network["id"])
                or bool(address)
                and endpoint.get("NetworkID") != expected_network["id"]
                or endpoint.get("IPAMConfig") is not None
                or endpoint.get("Links") is not None
                or endpoint.get("DriverOpts") is not None
                or endpoint.get("GwPriority") != 0
                or not isinstance(endpoint.get("MacAddress"), str)
                or endpoint["MacAddress"]
                and _MAC_ADDRESS.fullmatch(endpoint["MacAddress"]) is None
                or endpoint.get("GlobalIPv6Address") not in (None, "")
                or not address_in_subnet
            ):
                raise AdmissionError(
                    f"container network endpoint policy differs: {service}"
                )
        if host_config.get("Runtime") not in (None, "", "runc"):
            raise AdmissionError(f"container runtime differs: {service}")
        if host_config.get("AutoRemove") is not False:
            raise AdmissionError(f"container auto-remove policy differs: {service}")
        if host_config.get("PublishAllPorts") is not False:
            raise AdmissionError(f"container publishes unmodeled ports: {service}")
        if (
            any(
                host_config.get(field) is not None
                for field in ("Dns", "DnsOptions", "DnsSearch", "Links")
            )
            or host_config.get("ExtraHosts") != []
        ):
            raise AdmissionError(f"container DNS/host-link policy differs: {service}")
        if host_config.get("LogConfig") != {"Config": {}, "Type": "json-file"}:
            raise AdmissionError(f"container logging policy differs: {service}")
        if tuple(host_config.get("MaskedPaths") or ()) != _EXPECTED_MASKED_PATHS:
            raise AdmissionError(f"container masked-path policy differs: {service}")
        if tuple(host_config.get("ReadonlyPaths") or ()) != _EXPECTED_READONLY_PATHS:
            raise AdmissionError(f"container readonly-path policy differs: {service}")
        if host_config.get("Init") is not None:
            raise AdmissionError(f"container init policy differs: {service}")
        oom_kill_disable = host_config.get("OomKillDisable")
        if (
            oom_kill_disable is not None and oom_kill_disable is not False
        ) or host_config.get("OomScoreAdj", 0) != 0:
            raise AdmissionError(f"container OOM policy differs: {service}")
        if host_config.get("ShmSize") != projection["shm_size"]:
            raise AdmissionError(f"container shm_size differs: {service}")
        if any(
            host_config.get(field) != expected
            for field, expected in _EXPECTED_CONTAINER_RESOURCES[service].items()
        ):
            raise AdmissionError(f"container resource policy differs: {service}")
        if host_config.get("RestartPolicy") != {
            "MaximumRetryCount": 0,
            "Name": "unless-stopped",
        }:
            raise AdmissionError(f"container restart policy differs: {service}")
        actual_ports = _port_bindings(
            host_config.get("PortBindings"), label=f"container ports for {service}"
        )
        if actual_ports != projection["port_bindings"]:
            raise AdmissionError(f"container port bindings differ: {service}")
        actual_mounts = container.get("Mounts")
        if not isinstance(actual_mounts, list):
            raise AdmissionError(f"container mount metadata is invalid: {service}")
        requested_mounts = host_config.get("Mounts")
        if requested_mounts is None:
            requested_mounts = []
        if not isinstance(requested_mounts, list) or any(
            not isinstance(item, dict) for item in requested_mounts
        ):
            raise AdmissionError(
                f"container requested-mount metadata is invalid: {service}"
            )
        requested_by_target: dict[str, dict[str, Any]] = {}
        for requested in requested_mounts:
            requested_target = requested.get("Target")
            if (
                not isinstance(requested_target, str)
                or not requested_target.startswith("/")
                or requested_target in requested_by_target
            ):
                raise AdmissionError(
                    f"container requested-mount identity is invalid: {service}"
                )
            requested_by_target[requested_target] = requested
        effective: list[tuple[Any, ...]] = []
        actual_tmpfs_targets: set[str] = set()
        seen_targets: set[str] = set()
        for mount in actual_mounts:
            if not isinstance(mount, dict):
                raise AdmissionError(f"container mount record is invalid: {service}")
            mount_type = mount.get("Type")
            target = mount.get("Destination")
            if (
                mount_type not in ("bind", "volume", "tmpfs")
                or not isinstance(target, str)
                or not target.startswith("/")
                or target in seen_targets
            ):
                raise AdmissionError(f"container mount identity is invalid: {service}")
            seen_targets.add(target)
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"container mount shadows image trust path: {service}"
                )
            if mount_type == "tmpfs":
                actual_tmpfs_targets.add(target)
                continue
            source_field = "Source" if mount_type == "bind" else "Name"
            source = mount.get(source_field)
            read_only = mount.get("RW") is False
            if (
                not isinstance(source, str)
                or not source
                or not isinstance(mount.get("RW"), bool)
            ):
                raise AdmissionError(f"container mount identity is invalid: {service}")
            expected_mode = "ro" if read_only else "rw"
            mount_mode = mount.get("Mode")
            if mount_mode != expected_mode:
                requested = requested_by_target.get(target)
                requested_read_only = (
                    requested.get("ReadOnly", False)
                    if isinstance(requested, dict)
                    else None
                )
                if (
                    mount_mode != ""
                    or mount_type != "bind"
                    or not isinstance(requested, dict)
                    or requested.get("Type") != "bind"
                    or requested.get("Source") != source
                    or requested.get("Target") != target
                    or not isinstance(requested_read_only, bool)
                    or requested_read_only is not read_only
                    or requested.get("BindOptions") != {}
                ):
                    raise AdmissionError(f"container mount mode differs: {service}")
            if mount_type == "bind":
                if mount.get("Propagation") != "rprivate":
                    raise AdmissionError(
                        f"container bind propagation differs: {service}"
                    )
            elif (
                mount.get("Driver") != "local"
                or mount.get("Propagation") != ""
                or not verified_volumes
                or mount.get("Source") != verified_volumes[0]["mountpoint"]
            ):
                raise AdmissionError(f"container volume driver differs: {service}")
            effective.append((mount_type, source, target, read_only))
        if tuple(sorted(effective)) != projection["volumes"]:
            raise AdmissionError(f"container mount projection differs: {service}")
        expected_tmpfs = projection["tmpfs"]
        actual_tmpfs = host_config.get("Tmpfs")
        if expected_tmpfs:
            if not isinstance(actual_tmpfs, dict) or set(actual_tmpfs) != set(
                expected_tmpfs
            ):
                raise AdmissionError(f"container tmpfs projection differs: {service}")
            for target, raw_options in actual_tmpfs.items():
                if (
                    not isinstance(raw_options, str)
                    or frozenset(raw_options.split(",")) != expected_tmpfs[target]
                ):
                    raise AdmissionError(f"container tmpfs options differ: {service}")
            if actual_tmpfs_targets and actual_tmpfs_targets != set(expected_tmpfs):
                raise AdmissionError(f"container tmpfs mounts differ: {service}")
        elif (actual_tmpfs is not None and actual_tmpfs != {}) or actual_tmpfs_targets:
            raise AdmissionError(f"container adds an unexpected tmpfs: {service}")
        verified.append(
            {
                "container_id": container_id,
                "final_image": bindings[service],
                "image_id": image_id,
                "service": service,
            }
        )
    return {
        "apparmor_profile": apparmor_profile,
        "docker_security_options": list(docker_security_options),
        "images": verified,
        "networks": list(verified_networks.values()),
        "projects": (
            {COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES), GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES)}
            if split_projects
            else {str(project): list(selected)}
        ),
        "schema_version": 2 if split_projects else 1,
        "status": (
            "admitted-running-v1" if expected_state == "running" else "admitted-v1"
        ),
        "volumes": verified_volumes,
    }


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(path))


def _assert_canonical_release(root: Path) -> None:
    if os.geteuid() != 0:
        raise AdmissionError("production admission requires effective UID 0")
    if Path(sys.executable) != _SYSTEM_PYTHON:
        raise AdmissionError("production admission requires exact /usr/bin/python3")
    if root != _REPOSITORY_ROOT:
        raise AdmissionError(
            "--root must be the canonical protected release containing this helper"
        )
    validator_path = root / "scripts/validate_whoscored_build_provenance.py"
    try:
        helper_source = provenance.read_protected_regular_file(
            _SCRIPT_PATH, label="production admission helper"
        )
        validator_source = provenance.read_protected_regular_file(
            validator_path, label="production provenance validator"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not helper_source:
        raise AdmissionError("production admission helper is empty")
    loaded_validator_digest = getattr(
        provenance, "_whoscored_loaded_source_sha256", None
    )
    if not isinstance(loaded_validator_digest, str) or not hmac.compare_digest(
        hashlib.sha256(validator_source).hexdigest(), loaded_validator_digest
    ):
        raise AdmissionError("production provenance validator changed after loading")


def _assert_protected_compose_inputs(paths: Sequence[Path]) -> dict[Path, bytes]:
    captured: dict[Path, bytes] = {}
    for path in paths:
        try:
            captured[path] = provenance.read_protected_regular_file(
                path, label="production Compose admission input"
            )
        except provenance.ProvenanceError as exc:
            raise AdmissionError(str(exc)) from exc
    return captured


def _assert_protected_directory(path: Path, *, label: str) -> os.stat_result:
    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = ("st_dev", "st_ino", "st_mode", "st_uid", "st_mtime_ns", "st_ctime_ns")
    if (
        before.st_uid != 0
        or before.st_mode & 0o022
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(f"{label} is not a protected directory: {path}")
    return before


def _assert_protected_regular_file(path: Path, *, label: str) -> os.stat_result:
    try:
        provenance.read_protected_regular_file(path, label=label)
        metadata = path.lstat()
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise AdmissionError(f"{label} is not a protected regular file: {path}")
    return metadata


def _assert_airflow_authority_directory(
    path: Path, *, label: str
) -> os.stat_result:
    """Require one UID-50000 directory writable only by its container owner."""

    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        before.st_uid != _AIRFLOW_RUNTIME_UID
        or before.st_gid != 0
        or stat.S_IMODE(before.st_mode) not in {0o700, 0o750}
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(
            f"{label} must be owned by {_AIRFLOW_RUNTIME_UID}:0 with mode "
            f"0700 or 0750: {path}"
        )
    return before


def _assert_fbref_geoip_database(path: Path) -> None:
    """Require the one reviewed external GeoLite byte identity."""

    try:
        raw, identity = provenance.read_protected_regular_file_snapshot(
            path, label="FBref Camoufox GeoLite database"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(
            "FBref Camoufox GeoLite database is missing or unprotected"
        ) from exc
    (
        _device,
        _inode,
        mode,
        uid,
        gid,
        link_count,
        size,
        _modified_ns,
        _changed_ns,
    ) = identity
    if (
        not stat.S_ISREG(mode)
        or uid != 0
        or gid != 0
        or link_count != 1
        or stat.S_IMODE(mode) != 0o444
        or size != FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE
        or len(raw) != FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE
        or not hmac.compare_digest(
            hashlib.sha256(raw).hexdigest(),
            FBREF_CAMOUFOX_GEOIP_DATABASE_SHA256,
        )
    ):
        raise AdmissionError(
            "FBref Camoufox GeoLite database differs from the reviewed bytes"
        )


def _provider_receipt_now() -> datetime:
    return datetime.now(timezone.utc)


def _provider_policy_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise AdmissionError(f"provider policy {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AdmissionError(f"provider policy {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise AdmissionError(f"provider policy {field} is not UTC")
    return parsed.astimezone(timezone.utc)


def validate_provider_policy(
    path: Path, *, owner_secret_path: Path
) -> dict[str, str | int]:
    """Verify one owner-signed provider-policy-v1 without projecting its key."""

    _canonical_existing_path(path, label="provider policy")
    _canonical_existing_path(owner_secret_path, label="provider-policy owner key")
    try:
        raw = provenance.read_protected_regular_file(path, label="provider policy")
        secret_raw = provenance.read_protected_regular_file(
            owner_secret_path, label="provider-policy owner key"
        )
        secret = secret_raw.decode("utf-8").strip()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError("provider policy/key is not protected strict data") from exc
    if len(secret.encode("utf-8")) < 32:
        raise AdmissionError("provider-policy owner key is too short")
    if (
        not isinstance(value, dict)
        or frozenset(value) != _PROVIDER_POLICY_FIELDS
        or raw != _canonical_bytes(value)
    ):
        raise AdmissionError("provider policy is not canonical provider-policy-v1")
    unsigned = {field: value[field] for field in _PROVIDER_POLICY_UNSIGNED_FIELDS}
    digest = hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
    signed_body = {**unsigned, "document_sha256": digest}
    signature = hmac.new(
        secret.encode("utf-8"), _canonical_bytes(signed_body), hashlib.sha256
    ).hexdigest()
    if (
        value.get("schema_version") != 1
        or value.get("source") != "whoscored"
        or value.get("signature_algorithm") != "hmac-sha256"
        or not isinstance(value.get("document_sha256"), str)
        or not hmac.compare_digest(str(value["document_sha256"]), digest)
        or not isinstance(value.get("signature"), str)
        or not hmac.compare_digest(str(value["signature"]), signature)
    ):
        raise AdmissionError("provider policy digest/signature is invalid")
    token = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
    for field in ("provider_id", "order_id", "plan_id"):
        if not isinstance(value.get(field), str) or token.fullmatch(str(value[field])) is None:
            raise AdmissionError(f"provider policy {field} is invalid")
    valid_from = _provider_policy_utc(value.get("valid_from"), field="valid_from")
    valid_until = _provider_policy_utc(value.get("valid_until"), field="valid_until")
    now = _provider_receipt_now()
    if not valid_from <= now < valid_until:
        raise AdmissionError("provider policy is not active")
    caps: list[int] = []
    for field in (
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "safety_cap_bytes",
        "provider_quota_bytes",
    ):
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise AdmissionError(f"provider policy {field} is invalid")
        caps.append(item)
    if caps != sorted(caps):
        raise AdmissionError("provider policy quota/safety caps are inconsistent")
    receipt_sha256 = value.get("receipt_sha256")
    if not isinstance(receipt_sha256, str) or _DIGEST.fullmatch(receipt_sha256) is None:
        raise AdmissionError("provider policy receipt digest is invalid")
    return {
        "daily_cap_bytes": int(value["daily_cap_bytes"]),
        "document_sha256": digest,
        "monthly_cap_bytes": int(value["monthly_cap_bytes"]),
        "order_cap_bytes": int(value["order_cap_bytes"]),
        "order_id": str(value["order_id"]),
        "plan_id": str(value["plan_id"]),
        "policy_path": str(path),
        "provider_id": str(value["provider_id"]),
        "provider_quota_bytes": int(value["provider_quota_bytes"]),
        "receipt_sha256": receipt_sha256,
        "safety_cap_bytes": int(value["safety_cap_bytes"]),
    }


def _decimal_gigabytes_to_bytes(value: object, *, field: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+\.[0-9]{2}", value) is None:
        raise AdmissionError(f"provider quota receipt {field} is invalid")
    try:
        result = Decimal(value) * Decimal(1_000_000_000)
    except InvalidOperation as exc:
        raise AdmissionError(f"provider quota receipt {field} is invalid") from exc
    if result != result.to_integral_value() or result < 0:
        raise AdmissionError(f"provider quota receipt {field} is invalid")
    return int(result)


def validate_provider_quota_receipt(
    path: Path,
    *,
    provider_policy_path: Path | None = None,
    owner_secret_path: Path | None = None,
) -> dict[str, str | int]:
    """Bind admission to fresh, credential-free provider quota evidence."""

    _canonical_existing_path(path, label="provider quota receipt")
    try:
        raw = provenance.read_protected_regular_file(
            path, label="provider quota receipt"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not 0 < len(raw) <= MAX_PROVIDER_QUOTA_RECEIPT_BYTES:
        raise AdmissionError("provider quota receipt size is invalid")
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("provider quota receipt is not strict JSON") from exc
    if (
        not isinstance(document, dict)
        or frozenset(document) != _PROVIDER_QUOTA_RECEIPT_FIELDS
        or raw != _canonical_bytes(document)
    ):
        raise AdmissionError("provider quota receipt is not canonical JSON")
    if (provider_policy_path is None) != (owner_secret_path is None):
        raise AdmissionError("provider policy and owner key must be supplied together")
    policy = (
        validate_provider_policy(
            provider_policy_path, owner_secret_path=owner_secret_path
        )
        if provider_policy_path is not None and owner_secret_path is not None
        else None
    )
    if policy is None:
        expected = {
            "schema_version": 1,
            "status": "active",
            "provider": "PROXYS.IO",
            "order_id": "38950",
            "plan": "Bronze",
            "quota_decimal_gb": "1.00",
            "remaining_decimal_gb": "1.00",
        }
    else:
        expected = {
            "schema_version": 1,
            "status": "active",
            "provider": policy["provider_id"],
            "order_id": policy["order_id"],
            "plan": policy["plan_id"],
        }
    if any(document.get(name) != value for name, value in expected.items()):
        raise AdmissionError("provider quota receipt does not prove the exact order")
    receipt_digest = hashlib.sha256(raw).hexdigest()
    if policy is not None:
        quota_bytes = _decimal_gigabytes_to_bytes(
            document.get("quota_decimal_gb"), field="quota_decimal_gb"
        )
        remaining_bytes = _decimal_gigabytes_to_bytes(
            document.get("remaining_decimal_gb"), field="remaining_decimal_gb"
        )
        if (
            not hmac.compare_digest(str(policy["receipt_sha256"]), receipt_digest)
            or quota_bytes != policy["provider_quota_bytes"]
            or remaining_bytes > quota_bytes
            or remaining_bytes < policy["safety_cap_bytes"]
        ):
            raise AdmissionError("provider quota receipt differs from signed policy")
    observed_raw = document.get("observed_at")
    if (
        type(observed_raw) is not str
        or re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", observed_raw
        )
        is None
    ):
        raise AdmissionError("provider quota receipt observed_at is invalid")
    observed_at = datetime.fromisoformat(observed_raw.replace("Z", "+00:00"))
    age = _provider_receipt_now() - observed_at
    if age < -timedelta(minutes=5) or age > MAX_PROVIDER_QUOTA_RECEIPT_AGE:
        raise AdmissionError("provider quota receipt is stale or future-dated")
    screenshot_raw = document.get("screenshot_path")
    screenshot_sha256 = document.get("screenshot_sha256")
    if (
        type(screenshot_raw) is not str
        or not screenshot_raw
        or type(screenshot_sha256) is not str
        or _DIGEST.fullmatch(screenshot_sha256) is None
    ):
        raise AdmissionError("provider quota screenshot identity is invalid")
    screenshot = Path(screenshot_raw)
    _canonical_existing_path(screenshot, label="provider quota screenshot")
    try:
        screenshot_bytes = provenance.read_protected_regular_file(
            screenshot, label="provider quota screenshot"
        )
        screenshot_stat = screenshot.lstat()
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError("provider quota screenshot is not protected") from exc
    if not hmac.compare_digest(
        hashlib.sha256(screenshot_bytes).hexdigest(), screenshot_sha256
    ):
        raise AdmissionError("provider quota screenshot digest differs")
    screenshot_time = datetime.fromtimestamp(
        screenshot_stat.st_mtime, tz=timezone.utc
    ).replace(microsecond=0)
    if screenshot_time != observed_at:
        raise AdmissionError("provider quota screenshot time differs from receipt")
    projection: dict[str, str | int] = {
        **expected,
        "observed_at": observed_raw,
        "receipt_path": str(path),
        "receipt_sha256": receipt_digest,
        "screenshot_path": str(screenshot),
        "screenshot_sha256": screenshot_sha256,
    }
    if policy is not None:
        projection.update(
            {
                "provider_policy_path": str(policy["policy_path"]),
                "provider_policy_sha256": str(policy["document_sha256"]),
                "daily_cap_bytes": int(policy["daily_cap_bytes"]),
                "monthly_cap_bytes": int(policy["monthly_cap_bytes"]),
                "order_cap_bytes": int(policy["order_cap_bytes"]),
                "provider_quota_bytes": int(policy["provider_quota_bytes"]),
                "safety_cap_bytes": int(policy["safety_cap_bytes"]),
            }
        )
    return projection


def validate_deployment_admission_receipt(
    path: Path,
    *,
    deployment_attestation_path: Path,
    provider_policy: Mapping[str, str | int],
) -> dict[str, str]:
    """Bind unattended running checks to the deploy-time fresh-receipt gate."""

    _canonical_existing_path(path, label="deployment admission receipt")
    try:
        raw = provenance.read_protected_regular_file(
            path, label="deployment admission receipt"
        )
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        deployment_raw = provenance.read_protected_regular_file(
            deployment_attestation_path, label="deployment attestation"
        )
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError(
            "deployment admission receipt is not protected strict data"
        ) from exc
    if not isinstance(value, dict) or raw != _canonical_bytes(value):
        raise AdmissionError("deployment admission receipt is not canonical JSON")
    attestation = value.get("deployment_attestation")
    quota = value.get("provider_quota_receipt")
    if (
        value.get("schema_version") != 2
        or value.get("status") != "rendered-admitted-v2"
        or value.get("projects")
        != {
            COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
            GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
        }
        or not isinstance(attestation, dict)
        or attestation
        != {
            "path": str(deployment_attestation_path),
            "sha256": hashlib.sha256(deployment_raw).hexdigest(),
        }
        or not isinstance(quota, dict)
        or quota.get("provider_policy_sha256")
        != provider_policy.get("document_sha256")
        or quota.get("receipt_sha256") != provider_policy.get("receipt_sha256")
        or quota.get("order_id") != provider_policy.get("order_id")
        or quota.get("daily_cap_bytes") != provider_policy.get("daily_cap_bytes")
    ):
        raise AdmissionError(
            "deployment admission receipt differs from attestation/provider policy"
        )
    return {
        "path": str(path),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _assert_writable_runtime_directory(path: Path, *, label: str) -> os.stat_result:
    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    mode = stat.S_IMODE(before.st_mode)
    if (
        before.st_uid not in {0, 50_000, 65_534}
        or before.st_gid != 0
        or mode & 0o070 != 0o070
        or mode & 0o002
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(f"{label} is not a protected writable directory: {path}")
    return before


def _canonical_existing_path(path: Path, *, label: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AdmissionError(f"{label} is missing: {path}") from exc
    if path != resolved:
        raise AdmissionError(f"{label} is not a canonical non-symlink path: {path}")
    return resolved


def _assert_separate_mounts(paths: Mapping[str, Path], *, label: str) -> None:
    resolved = {
        name: _canonical_existing_path(path, label=f"{label} {name}")
        for name, path in paths.items()
    }
    items = tuple(resolved.items())
    for index, (left_name, left) in enumerate(items):
        left_stat = left.stat()
        for right_name, right in items[index + 1 :]:
            right_stat = right.stat()
            if (
                (left_stat.st_dev, left_stat.st_ino)
                == (right_stat.st_dev, right_stat.st_ino)
                or left in right.parents
                or right in left.parents
            ):
                raise AdmissionError(
                    f"{label} mounts alias or nest: {left_name}, {right_name}"
                )


def _validate_bind_source_policy(
    projections: Mapping[str, Mapping[str, Any]], *, root: Path
) -> None:
    sources: dict[tuple[str, str], Path] = {}
    for service in PROTECTED_SERVICES:
        for kind, raw_source, target, _read_only in projections[service]["volumes"]:
            if kind == "bind":
                sources[(service, target)] = Path(raw_source)
    expected = {
        (service, target)
        for service, targets in _RELEASE_BIND_TARGETS.items()
        for target in targets
    } | set(_RUNTIME_HOST_BIND_TARGETS)
    pointer_identity = (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-scheduled-pointers",
    )
    if pointer_identity not in sources:
        expected.discard(pointer_identity)
    if set(sources) != expected:
        raise AdmissionError("rendered bind-source policy differs")
    for service, targets in _RELEASE_BIND_TARGETS.items():
        for target, relative in targets.items():
            source = sources[(service, target)]
            expected_source = root / relative
            if source != expected_source:
                raise AdmissionError(
                    f"rendered release bind source differs: {service} {target}"
                )
            if expected_source.is_dir():
                _assert_protected_directory(
                    source, label=f"release code directory for {service} {target}"
                )
            else:
                _assert_protected_regular_file(
                    source, label=f"release code file for {service} {target}"
                )
    for identity, policy in _RUNTIME_HOST_BIND_TARGETS.items():
        if identity == pointer_identity and identity not in sources:
            continue
        source = sources[identity]
        _canonical_existing_path(
            source, label=f"runtime bind {identity[0]} {identity[1]}"
        )
        if policy == "writable-directory":
            _assert_writable_runtime_directory(
                source, label=f"runtime state directory for {identity[0]} {identity[1]}"
            )
        elif policy == "airflow-authority-directory":
            _assert_airflow_authority_directory(
                source,
                label=f"Airflow authority directory for {identity[0]} {identity[1]}",
            )
        elif policy == "protected-directory":
            _assert_protected_directory(
                source, label=f"authority directory for {identity[0]} {identity[1]}"
            )
        elif policy == "fbref-geoip-database":
            _assert_fbref_geoip_database(source)
        else:
            _assert_protected_regular_file(
                source, label=f"protected input for {identity[0]} {identity[1]}"
            )
    scheduler_filter_state = sources[
        ("airflow-scheduler", "/opt/airflow/state/whoscored-proxy-filter")
    ]
    filter_state = sources[
        ("whoscored_proxy_filter", "/opt/airflow/state/whoscored-proxy-filter")
    ]
    if scheduler_filter_state != filter_state:
        raise AdmissionError(
            "scheduler read-only provider evidence does not bind filter-owned state"
        )
    protected_mounts = {
        "fotmob-admission": sources[
            ("airflow-scheduler", "/opt/airflow/fotmob-admission")
        ],
        "fbref-geoip-database": sources[
            (
                "airflow-scheduler",
                FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH,
            )
        ],
        "scheduler-logs": sources[("airflow-scheduler", "/opt/airflow/logs")],
        "gateway-state": sources[
            (
                "whoscored_paid_gateway",
                "/opt/airflow/state/whoscored-paid-gateway",
            )
        ],
        "filter-state": sources[
            (
                "whoscored_proxy_filter",
                "/opt/airflow/state/whoscored-proxy-filter",
            )
        ],
        "scheduler-approvals": sources[
            ("airflow-scheduler", "/opt/airflow/secure/whoscored-approvals")
        ],
        "gateway-alert-authority": sources[
            (
                "whoscored_paid_gateway",
                "/opt/airflow/secure/whoscored-alert-authority",
            )
        ],
    }
    if pointer_identity in sources:
        protected_mounts["scheduler-pointers"] = sources[pointer_identity]
    _assert_separate_mounts(protected_mounts, label="protected runtime")


def _common_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", type=Path, default=Path.cwd())
    common.add_argument("--attestation", type=Path)
    common.add_argument("--manifest", type=Path)
    common.add_argument("--deployment-attestation", type=Path, required=True)
    return common


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    common = _common_parser()
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate-override", parents=[common])
    generate.add_argument("--common-output", type=Path, required=True)
    generate.add_argument("--gateway-output", type=Path, required=True)
    rendered = commands.add_parser("verify-rendered", parents=[common])
    rendered.add_argument("--common-override", type=Path, required=True)
    rendered.add_argument("--gateway-override", type=Path, required=True)
    rendered.add_argument("--env-file", type=Path, action="append", required=True)
    rendered.add_argument("--provider-policy", type=Path, required=True)
    rendered.add_argument("--owner-secret-file", type=Path, required=True)
    rendered.add_argument("--provider-quota-receipt", type=Path, required=True)
    rendered.add_argument("--output", type=Path, required=True)
    created = commands.add_parser("post-create", parents=[common])
    created.add_argument("--common-override", type=Path, required=True)
    created.add_argument("--gateway-override", type=Path, required=True)
    created.add_argument("--env-file", type=Path, action="append", required=True)
    created.add_argument("--provider-policy", type=Path, required=True)
    created.add_argument("--owner-secret-file", type=Path, required=True)
    created.add_argument("--provider-quota-receipt", type=Path, required=True)
    created.add_argument("--service", action="append", required=True)
    running = commands.add_parser("verify-running", parents=[common])
    running.add_argument("--common-override", type=Path, required=True)
    running.add_argument("--gateway-override", type=Path, required=True)
    running.add_argument("--env-file", type=Path, action="append", required=True)
    running.add_argument("--provider-policy", type=Path, required=True)
    running.add_argument("--owner-secret-file", type=Path, required=True)
    running.add_argument(
        "--deployment-admission-receipt", type=Path, required=True
    )
    running.add_argument("--service", action="append", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if (
        not sys.flags.isolated
        or not sys.flags.no_site
        or not sys.flags.ignore_environment
    ):
        print(
            "WhoScored production admission blocked: invoke exact Python with -I -S",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    try:
        _assert_clean_control_environment()
    except AdmissionError as exc:
        print(f"WhoScored production admission blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    args = _parser().parse_args(argv)
    root = _absolute(args.root)
    canonical_attestation = root / provenance.ATTESTATION_RELATIVE
    canonical_manifest = root / provenance.MANIFEST_RELATIVE
    attestation = _absolute(args.attestation or canonical_attestation)
    manifest = _absolute(args.manifest or canonical_manifest)
    deployment = _absolute(args.deployment_attestation)
    try:
        _assert_canonical_release(root)
        if attestation != canonical_attestation or manifest != canonical_manifest:
            raise AdmissionError(
                "provenance manifest and attestation must be canonical release paths"
            )
        bindings = validate_bindings(
            root=root,
            attestation_path=attestation,
            manifest_path=manifest,
            deployment_attestation_path=deployment,
        )
        if args.command == "generate-override":
            common_output = _absolute(args.common_output)
            gateway_output = _absolute(args.gateway_output)
            if common_output == gateway_output:
                raise AdmissionError("split Compose overrides must be distinct")
            write_new_regular_file(
                common_output,
                compose_override_bytes(bindings, COMMON_PROTECTED_SERVICES),
            )
            write_new_regular_file(
                gateway_output,
                compose_override_bytes(bindings, GATEWAY_PROTECTED_SERVICES),
            )
            report: dict[str, Any] = {
                "outputs": {
                    COMMON_PROJECT: str(common_output),
                    GATEWAY_PROJECT: str(gateway_output),
                },
                "projects": {
                    COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                    GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
                },
                "schema_version": 2,
                "status": "overrides-created-v2",
            }
        else:
            provider_policy = validate_provider_policy(
                _absolute(args.provider_policy),
                owner_secret_path=_absolute(args.owner_secret_file),
            )
            provider_authority: dict[str, str | int] = {
                **provider_policy,
                "provider_policy_sha256": str(provider_policy["document_sha256"]),
            }
            provider_quota_receipt: dict[str, str | int] | None = None
            deployment_admission_receipt: dict[str, str] | None = None
            if args.command == "verify-running":
                deployment_admission_receipt = (
                    validate_deployment_admission_receipt(
                        _absolute(args.deployment_admission_receipt),
                        deployment_attestation_path=deployment,
                        provider_policy=provider_policy,
                    )
                )
            else:
                provider_quota_receipt = validate_provider_quota_receipt(
                    _absolute(args.provider_quota_receipt),
                    provider_policy_path=_absolute(args.provider_policy),
                    owner_secret_path=_absolute(args.owner_secret_file),
                )
                provider_authority = provider_quota_receipt
            common_override = _absolute(args.common_override)
            gateway_override = _absolute(args.gateway_override)
            if common_override == gateway_override:
                raise AdmissionError("split Compose overrides must be distinct")
            env_files = tuple(_absolute(path) for path in args.env_file)
            protected_inputs = _assert_protected_compose_inputs(
                (
                    root / "compose.yaml",
                    root / "compose.seaweedfs-supervised.yaml",
                    root / "deploy/whoscored/gateway.compose.yaml",
                    common_override,
                    gateway_override,
                    *env_files,
                )
            )
            projections, config_hashes, config_files, rendered = (
                render_attested_projects(
                    bindings,
                    root=root,
                    common_override_path=common_override,
                    gateway_override_path=gateway_override,
                    env_files=env_files,
                    provider_authority=provider_authority,
                    protected_inputs=protected_inputs,
                )
            )
            if args.command == "verify-rendered":
                assert provider_quota_receipt is not None
                output = _absolute(args.output)
                write_new_regular_file(output, _canonical_bytes(rendered))
                report = {
                    "config_hashes": config_hashes,
                    "deployment_attestation": {
                        "path": str(deployment),
                        "sha256": hashlib.sha256(
                            provenance.read_protected_regular_file(
                                deployment, label="deployment attestation"
                            )
                        ).hexdigest(),
                    },
                    "output": str(output),
                    "provider_quota_receipt": provider_quota_receipt,
                    "projects": {
                        COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                        GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
                    },
                    "schema_version": 2,
                    "status": "rendered-admitted-v2",
                }
            else:
                report = verify_created_containers(
                    bindings,
                    project=_SERVICE_PROJECT,
                    selected_services=args.service,
                    projections=projections,
                    config_hashes=config_hashes,
                    config_files=config_files,
                    env_files=env_files,
                    expected_state=(
                        "running" if args.command == "verify-running" else "created"
                    ),
                )
                if args.command == "verify-running":
                    assert deployment_admission_receipt is not None
                    report["deployment_admission_receipt"] = (
                        deployment_admission_receipt
                    )
                    report["provider_policy"] = provider_policy
                else:
                    assert provider_quota_receipt is not None
                    report["provider_quota_receipt"] = provider_quota_receipt
    except AdmissionError as exc:
        print(f"WhoScored production admission blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    sys.stdout.buffer.write(_canonical_bytes(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
