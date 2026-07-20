"""
Unit tests for scripts/inventory_bronze_orphans.py — KEEP-set + orphan diff.

Strategy
--------
``inventory_bronze_orphans`` is a top-level script (not a package) that imports
``utils.silver_tasks`` and ``audit_bronze_columns``. The root ``conftest.py``
exposes ``dags/``, so the real utility can load without opening a connection.
We load the sibling script explicitly before the inventory module. No Trino,
no network — only pure diff logic.

What we cover
-------------
- ``build_keep_set`` includes the parser contract (``EXTRA_PRODUCED`` is empty
  after #354 removed the dead ``espn_standings`` producer).
- ``find_orphans`` flags a retired FBref stat_type table and a renamed table,
  but NOT a contract table.
- empty live list -> no orphans, no error.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'inventory_bronze_orphans.py'


def _load_module():
    audit_path = REPO_ROOT / 'scripts' / 'audit_bronze_columns.py'
    audit_spec = importlib.util.spec_from_file_location(
        'audit_bronze_columns', audit_path
    )
    assert audit_spec is not None and audit_spec.loader is not None
    audit = importlib.util.module_from_spec(audit_spec)
    sys.modules['audit_bronze_columns'] = audit
    audit_spec.loader.exec_module(audit)

    spec = importlib.util.spec_from_file_location(
        'inventory_bronze_orphans', _SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['inventory_bronze_orphans'] = mod
    spec.loader.exec_module(mod)
    return mod


def test_keep_set_covers_contract_and_producer_only_tables():
    mod = _load_module()
    keep = mod.build_keep_set()

    # EXTRA_PRODUCED is empty after #354 removed the dead espn_standings producer.
    assert mod.EXTRA_PRODUCED == set()
    # Representative contract tables across sources.
    assert 'fbref_player_stats' in keep
    assert 'capology_player_salaries' in keep
    assert 'understat_shots' in keep
    # The contract alone has 60+ tables; KEEP must be at least that big.
    assert len(keep) >= 60


def test_find_orphans_flags_retired_and_renamed_only():
    mod = _load_module()
    keep = mod.build_keep_set()

    live = [
        'fbref_player_stats',        # contract -> KEEP
        'fbref_player_passing',      # retired stat_type -> ORPHAN
        'fbref_team_defense',        # retired stat_type -> ORPHAN
        'fbref_team_match_stats',    # renamed (canon: fbref_match_team_stats) -> ORPHAN
    ]
    orphans = mod.find_orphans(live, keep)

    assert orphans == [
        'fbref_player_passing',
        'fbref_team_defense',
        'fbref_team_match_stats',
    ]
    assert 'fbref_player_stats' not in orphans


def test_find_orphans_empty_live_returns_empty():
    mod = _load_module()
    assert mod.find_orphans([], mod.build_keep_set()) == []


def test_classify_orphans_matchhistory_games_now_droppable():
    # #307 migrated all Silver readers off matchhistory_games → it is no longer
    # in BLOCKED_ORPHANS and classifies as a plain droppable orphan.
    mod = _load_module()
    droppable, blocked = mod.classify_orphans(
        ['fbref_player_passing', 'matchhistory_games', 'fbref_team_defense']
    )
    assert droppable == ['fbref_player_passing', 'matchhistory_games', 'fbref_team_defense']
    assert blocked == []
