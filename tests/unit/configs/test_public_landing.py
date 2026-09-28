"""Публичный лендинг football.<домен> (#1570): проводка Caddy/compose/статика
и отсутствие адреса VM в отслеживаемых файлах.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
CADDYFILE = ROOT / "configs" / "caddy" / "Caddyfile"
LANDING = ROOT / "configs" / "caddy" / "landing" / "index.html"
BOOTSTRAP = ROOT / "configs" / "superset" / "bootstrap.sh"
CONFIG = ROOT / "configs" / "superset" / "superset_config.py"

DASHBOARD_SLUGS = ("league-overview", "player-overview-league", "world-cup")

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


def test_landing_links_three_dashboards_github_and_telegram():
    html = LANDING.read_text(encoding="utf-8")
    for slug in DASHBOARD_SLUGS:
        assert f"/superset/dashboard/{slug}/" in html, slug
    assert "https://github.com/sergeykuznetsov1995/data-platform-football" in html
    assert "https://t.me/Sergeykuznetsov1995" in html
    assert '<meta name="viewport"' in html
    assert "prefers-color-scheme: dark" in html
    assert "<script src=" not in html and "<link rel=\"stylesheet\"" not in html, "без внешних зависимостей"


def test_tracked_docs_and_configs_do_not_leak_vm_address():
    pattern = re.compile(r"159\.195\.193\.250|ssh\s+\S*\s*root@\d|2a0a:4cc0")
    offenders = []
    for base in ("docs", "configs"):
        for path in (ROOT / base).rglob("*"):
            if path.is_file() and path.suffix in {".md", ".py", ".sh", ".local", ".yaml", ".yml", ".html", ".example", ""}:
                try:
                    text = path.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    continue
                if pattern.search(text):
                    offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, offenders
