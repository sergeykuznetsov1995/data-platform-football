"""Публичный лендинг football.<домен> (#1570): проводка Caddy/compose/статика
и отсутствие адреса VM в отслеживаемых файлах.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
CADDYFILE = ROOT / "configs" / "caddy" / "Caddyfile"
LANDING = ROOT / "configs" / "caddy" / "landing" / "index.html"
BOOTSTRAP = ROOT / "configs" / "superset" / "bootstrap.sh"
CONFIG = ROOT / "configs" / "superset" / "superset_config.py"

# На лендинге два дашборда (владелец: без чемпионата мира); роль Public — см. test_create_public_role.
LANDING_SLUGS = ("league-overview", "player-overview-league")

pytestmark = pytest.mark.unit


def _service(name: str) -> dict:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    return compose["services"][name]


def test_caddyfile_serves_landing_as_static_site():
    text = CADDYFILE.read_text(encoding="utf-8")
    block = re.search(r"https://football\.\{\$PLATFORM_DOMAIN\} \{(.*?)\n\}", text, re.S)
    assert block, "нет блока football.{$PLATFORM_DOMAIN}"
    body = block.group(1)
    assert "root * /srv/landing" in body and "file_server" in body
    assert "reverse_proxy" not in body, "лендинг — статика, не прокси"


def test_compose_mounts_landing_into_caddy_read_only():
    volumes = _service("caddy")["volumes"]
    assert "./configs/caddy/landing:/srv/landing:ro" in volumes


def test_compose_mounts_public_role_script_into_superset_services():
    mount = "./configs/superset/create_public_role.py:/app/pythonpath/create_public_role.py:ro"
    for name in ("superset", "superset-worker", "superset-beat"):
        volumes = _service(name)["volumes"]
        assert volumes.count(mount) == 1, name


def test_bootstrap_runs_public_role_after_dashboards():
    text = BOOTSTRAP.read_text(encoding="utf-8")
    assert text.index("python import_dashboards.py") < text.index(
        'python "${PYTHONPATH_DIR}/create_public_role.py"'
    )


def test_superset_config_sets_public_role_without_gamma_copy():
    text = CONFIG.read_text(encoding="utf-8")
    assert re.search(r'^AUTH_ROLE_PUBLIC = "Public"$', text, re.M)
    assert not re.search(r"^PUBLIC_ROLE_LIKE\s*=", text, re.M), "Gamma даёт гостю запись"


def test_landing_links_dashboards_github_and_telegram():
    html = LANDING.read_text(encoding="utf-8")
    for slug in LANDING_SLUGS:
        assert f"/superset/dashboard/{slug}/" in html, slug
    assert "https://github.com/sergeykuznetsov1995/data-platform-football" in html
    assert "https://t.me/Sergeykuznetsov1995" in html
    assert "world-cup" not in html, "владелец: чемпионат мира на лендинге не нужен"
    assert '<meta name="viewport"' in html
    assert "prefers-color-scheme: dark" in html
    assert "<script src=" not in html and "<link rel=\"stylesheet\"" not in html, "без внешних зависимостей"


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout
    return [ROOT / p for p in out.decode().split("\0") if p]


def test_tracked_files_do_not_leak_vm_address():
    """Адрес VM в репозитории не хранится — сравниваем по хешу, чтобы не хранить
    его и в этом тесте. Плюс ни одного ``ssh … root@<ip>``."""
    vm_ip_sha256 = "17ee8084e74a1544ddff2fe2e1c9243759b7437b2953f2b067df9426f9ed5c54"
    # IPv6 VM: хеш первых двух хекстетов (в доке был только префикс).
    vm_ip6_prefix_sha256 = "1c06f9bdaa0c9f982c4f488a2fba545edd534629725602f6c4243bfdf3cfc9be"
    ipv4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
    ipv6_prefix = re.compile(r"\b([0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}):")
    ssh_root_ip = re.compile(r"ssh\b[^\n]*\broot@(?:\d{1,3}\.){3}\d{1,3}")
    offenders = []
    for path in _tracked_files():
        if path.suffix in {".png", ".jpg", ".jpeg", ".gif", ".ico", ".zip", ".parquet", ".pdf", ".woff", ".woff2"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        if ssh_root_ip.search(text):
            offenders.append(f"{path.relative_to(ROOT)}: ssh root@<ip>")
        for token in set(ipv4.findall(text)):
            if hashlib.sha256(token.encode()).hexdigest() == vm_ip_sha256:
                offenders.append(f"{path.relative_to(ROOT)}: адрес VM (IPv4)")
                break
        for token in set(ipv6_prefix.findall(text)):
            if hashlib.sha256(token.lower().encode()).hexdigest() == vm_ip6_prefix_sha256:
                offenders.append(f"{path.relative_to(ROOT)}: адрес VM (IPv6)")
                break
    assert not offenders, offenders
