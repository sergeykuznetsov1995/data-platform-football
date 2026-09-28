"""Роль Public для гостя (#1570): ``configs/superset/create_public_role.py``.

Скрипт живёт в контейнере Superset; здесь его чистая часть ``run`` гоняется
на фейках security_manager/session. Проверяем: набор прав ровно ожидаемый
(только чтение и датасеты трёх дашбордов), лишние права снимаются,
отсутствующий дашборд не роняет скрипт.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "configs" / "superset" / "create_public_role.py"

pytestmark = pytest.mark.unit


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


def test_role_gets_exactly_view_permissions_and_dashboard_datasources():
    mod = _load_module()
    sm = _SecurityManager(set(mod.VIEW_PERMISSIONS) | {("can_write", "Chart")})
    session = _Session(_DASHBOARDS)

    result = mod.run(sm, session, object)

    got = {(p.permission, p.view_menu) for p in sm.roles["Public"].permissions}
    expected = set(mod.VIEW_PERMISSIONS) | {
        ("datasource_access", "[trino_iceberg].[v_lo_team_season](id:19)"),
        ("datasource_access", "[trino_iceberg].[v_player_xg_2025](id:40)"),
        ("datasource_access", "[trino_iceberg].[v_wc_match](id:29)"),
    }
    assert got == expected
    assert len(sm.roles["Public"].permissions) == len(expected), "права не дублируются"
    assert result["removed"] == [] and len(result["added"]) == len(expected)
    assert session.committed


def test_extra_permissions_are_revoked_and_rerun_is_idempotent():
    mod = _load_module()
    role = _Role("Public")
    sm = _SecurityManager(set(mod.VIEW_PERMISSIONS), role)
    for leaked in (("can_write", "Chart"), ("can_explore", "Superset"), ("all_datasource_access", "all_datasource_access")):
        sm.add_permission_role(role, sm.add_permission_view_menu(*leaked))
    session = _Session(_DASHBOARDS)

    first = mod.run(sm, session, object)
    assert sorted(first["removed"]) == sorted(
        ["can_write on Chart", "can_explore on Superset", "all_datasource_access on all_datasource_access"]
    )
    second = mod.run(sm, session, object)
    assert second == {"added": [], "removed": []}


def test_missing_dashboard_or_permission_is_skipped_not_fatal(caplog):
    mod = _load_module()
    known = set(mod.VIEW_PERMISSIONS) - {("can_time_range", "Api")}
    sm = _SecurityManager(known)
    session = _Session({"league-overview": _DASHBOARDS["league-overview"]})

    with caplog.at_level("WARNING"):
        mod.run(sm, session, object)

    got = {(p.permission, p.view_menu) for p in sm.roles["Public"].permissions}
    assert ("can_time_range", "Api") not in got
    assert ("datasource_access", "[trino_iceberg].[v_lo_team_season](id:19)") in got
    assert "player-overview-league" in caplog.text and "world-cup" in caplog.text


def test_no_write_or_sql_lab_permissions_in_whitelist():
    mod = _load_module()
    forbidden = {"can_write", "can_explore", "can_explore_json", "can_csv", "can_export"}
    allowed_writes = {("can_write", "DashboardFilterStateRestApi")}
    for permission, view_menu in mod.VIEW_PERMISSIONS:
        if (permission, view_menu) in allowed_writes:
            continue
        assert permission not in forbidden, f"{permission} on {view_menu}"
        assert "SQLLab" not in view_menu and "SavedQuery" not in view_menu
    assert set(mod.DASHBOARD_SLUGS) == {"league-overview", "player-overview-league", "world-cup"}
