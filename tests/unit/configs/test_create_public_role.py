"""Роль Public для гостя (#1570): ``configs/superset/create_public_role.py``.

Скрипт живёт в контейнере Superset; здесь его чистая часть ``run`` гоняется
на фейках security_manager/session. Проверяем: набор прав ровно ожидаемый
(литеральный список здесь, независимый от скрипта — любое новое право
ломает тест), лишние права снимаются, повтор идемпотентен, отсутствующий
дашборд или право — ошибка, а не тихий успех.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "configs" / "superset" / "create_public_role.py"

pytestmark = pytest.mark.unit

# Ровно то, что нужно гостю для просмотра дашборда с нативными фильтрами.
# Ничего с записью в metadata DB (can_log, can_write on Chart/Dashboard/…),
# Explore, SQL Lab, экспортом, списками БД.
EXPECTED_VIEW_PERMISSIONS = {
    ("can_dashboard", "Superset"),
    ("can_dashboard_permalink", "Superset"),
    ("can_read", "Dashboard"),
    ("can_read", "Chart"),
    ("can_read", "Dataset"),
    ("can_read", "CssTemplate"),
    ("can_read", "DashboardFilterStateRestApi"),
    ("can_write", "DashboardFilterStateRestApi"),
    ("can_read", "DashboardPermalinkRestApi"),
    ("can_read", "SecurityRestApi"),
    ("can_get", "MenuApi"),
    ("can_time_range", "Api"),
    ("menu_access", "Dashboards"),
}


def _load_module():
    spec = importlib.util.spec_from_file_location("create_public_role", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Pvm:
    def __init__(self, permission: str, view_menu: str) -> None:
        self.permission, self.view_menu = permission, view_menu

    def __repr__(self) -> str:
        return f"{self.permission} on {self.view_menu}"


class _Role:
    def __init__(self, name: str) -> None:
        self.name, self.permissions = name, []


class _SecurityManager:
    def __init__(self, known: set[tuple[str, str]], role: _Role | None = None) -> None:
        self.pvms = {k: _Pvm(*k) for k in known}
        self.roles = {role.name: role} if role else {}

    def find_role(self, name):
        return self.roles.get(name)

    def add_role(self, name):
        self.roles[name] = _Role(name)
        return self.roles[name]

    def find_permission_view_menu(self, permission, view_menu):
        return self.pvms.get((permission, view_menu))

    def add_permission_view_menu(self, permission, view_menu):
        return self.pvms.setdefault((permission, view_menu), _Pvm(permission, view_menu))

    def add_permission_role(self, role, pvm):
        role.permissions.append(pvm)

    def del_permission_role(self, role, pvm):
        role.permissions.remove(pvm)


class _Datasource:
    def __init__(self, perm: str) -> None:
        self.perm = perm


class _Dashboard:
    def __init__(self, datasources: list[str]) -> None:
        self.datasources = [_Datasource(p) for p in datasources]


class _Session:
    def __init__(self, dashboards: dict[str, _Dashboard]) -> None:
        self.dashboards, self.committed = dashboards, False

    def query(self, _model):
        return self

    def filter_by(self, slug):
        self._slug = slug
        return self

    def one_or_none(self):
        return self.dashboards.get(self._slug)

    def commit(self):
        self.committed = True


_DASHBOARDS = {
    "league-overview": _Dashboard(["[trino_iceberg].[v_lo_team_season](id:19)"]),
    "player-overview-league": _Dashboard(["[trino_iceberg].[v_player_xg_2025](id:40)"]),
    "world-cup": _Dashboard(
        ["[trino_iceberg].[v_wc_match](id:29)", "[trino_iceberg].[v_lo_team_season](id:19)"]
    ),
}
_EXPECTED_DATASOURCES = {
    ("datasource_access", "[trino_iceberg].[v_lo_team_season](id:19)"),
    ("datasource_access", "[trino_iceberg].[v_player_xg_2025](id:40)"),
    ("datasource_access", "[trino_iceberg].[v_wc_match](id:29)"),
}


def _granted(sm: _SecurityManager) -> set[tuple[str, str]]:
    return {(p.permission, p.view_menu) for p in sm.roles["Public"].permissions}


def test_whitelist_is_exactly_the_expected_read_only_set():
    mod = _load_module()
    assert set(mod.VIEW_PERMISSIONS) == EXPECTED_VIEW_PERMISSIONS
    assert len(mod.VIEW_PERMISSIONS) == len(EXPECTED_VIEW_PERMISSIONS), "дубли в списке"
    assert set(mod.DASHBOARD_SLUGS) == {"league-overview", "player-overview-league", "world-cup"}


def test_role_gets_exactly_whitelist_plus_dashboard_datasources():
    mod = _load_module()
    # Gamma-подобное окружение: есть и «опасные» права, скрипт их брать не должен.
    known = EXPECTED_VIEW_PERMISSIONS | {
        ("can_write", "Chart"), ("can_log", "Superset"), ("can_read", "Database"),
        ("can_explore", "Superset"), ("all_datasource_access", "all_datasource_access"),
    }
    sm = _SecurityManager(known)
    session = _Session(_DASHBOARDS)

    result = mod.run(sm, session, object)

    assert _granted(sm) == EXPECTED_VIEW_PERMISSIONS | _EXPECTED_DATASOURCES
    assert len(sm.roles["Public"].permissions) == len(EXPECTED_VIEW_PERMISSIONS) + 3
    assert result["removed"] == []
    assert len(result["added"]) == len(EXPECTED_VIEW_PERMISSIONS) + 3
    assert session.committed


def test_extra_permissions_are_revoked_and_rerun_is_idempotent():
    mod = _load_module()
    role = _Role("Public")
    sm = _SecurityManager(EXPECTED_VIEW_PERMISSIONS, role)
    leaked = [("can_write", "Chart"), ("can_log", "Superset"), ("can_explore", "Superset")]
    for perm in leaked:
        sm.add_permission_role(role, sm.add_permission_view_menu(*perm))
    session = _Session(_DASHBOARDS)

    first = mod.run(sm, session, object)
    assert sorted(first["removed"]) == sorted(f"{p} on {v}" for p, v in leaked)
    assert _granted(sm) == EXPECTED_VIEW_PERMISSIONS | _EXPECTED_DATASOURCES

    second = mod.run(sm, session, object)
    assert second == {"added": [], "removed": []}
    assert len(sm.roles["Public"].permissions) == len(EXPECTED_VIEW_PERMISSIONS) + 3


def test_missing_dashboard_is_an_error_after_syncing_what_exists():
    mod = _load_module()
    sm = _SecurityManager(EXPECTED_VIEW_PERMISSIONS)
    session = _Session({"league-overview": _DASHBOARDS["league-overview"]})

    with pytest.raises(SystemExit) as exc:
        mod.run(sm, session, object)

    assert "player-overview-league" in str(exc.value) and "world-cup" in str(exc.value)
    assert session.committed, "что нашлось — выдано, чтобы повтор после импорта был инкрементальным"
    assert ("datasource_access", "[trino_iceberg].[v_lo_team_season](id:19)") in _granted(sm)


def test_missing_permission_is_an_error():
    mod = _load_module()
    sm = _SecurityManager(EXPECTED_VIEW_PERMISSIONS - {("can_time_range", "Api")})
    session = _Session(_DASHBOARDS)

    with pytest.raises(SystemExit) as exc:
        mod.run(sm, session, object)

    assert "can_time_range on Api" in str(exc.value)
    assert ("can_time_range", "Api") not in _granted(sm)
