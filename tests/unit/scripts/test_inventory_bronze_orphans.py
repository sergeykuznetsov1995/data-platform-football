"""
Unit tests for scripts/inventory_bronze_orphans.py — KEEP-set + orphan diff.

Strategy
--------
``inventory_bronze_orphans`` is a top-level script (not a package) that imports
``utils.silver_tasks`` (container-only path) and ``audit_bronze_columns`` (same
scripts/ dir). We stub ``utils.silver_tasks`` into ``sys.modules`` and put
``scripts/`` on ``sys.path`` so both imports resolve on the host, then load the
script via ``importlib.util``. No Trino, no network — only pure diff logic.

What we cover
-------------
- ``build_keep_set`` includes the parser contract AND the producer-only
  ``espn_standings`` (must never be flagged as an orphan).
- ``find_orphans`` flags a retired FBref stat_type table and a renamed table,
  but NOT a contract table and NOT ``espn_standings``.
- empty live list -> no orphans, no error.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'inventory_bronze_orphans.py'


def _load_module():
    # Stub the container-only import so exec_module succeeds on the host, and
    # expose scripts/ so ``import audit_bronze_columns`` resolves. Restore
    # sys.modules / sys.path in finally so loading is side-effect-free (the real
    # utils.silver_tasks must not be shadowed for other tests).
    stub = types.ModuleType('utils.silver_tasks')
    stub._get_trino_connection = lambda *a, **k: None  # noqa: E731 — never called here
    stub._execute = lambda *a, **k: None  # noqa: E731 — never called here

    _sentinel = object()
    _prev_utils = sys.modules.get('utils', _sentinel)
    _prev_silver = sys.modules.get('utils.silver_tasks', _sentinel)
    sys.modules.setdefault('utils', types.ModuleType('utils'))
    sys.modules['utils.silver_tasks'] = stub

    _scripts_dir = str(REPO_ROOT / 'scripts')
    _added_path = _scripts_dir not in sys.path
    if _added_path:
        sys.path.insert(0, _scripts_dir)

    try:
        spec = importlib.util.spec_from_file_location('inventory_bronze_orphans', _SCRIPT_PATH)
        assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
        mod = importlib.util.module_from_spec(spec)
        sys.modules['inventory_bronze_orphans'] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if _added_path:
            sys.path.remove(_scripts_dir)
        for _name, _prev in (('utils.silver_tasks', _prev_silver), ('utils', _prev_utils)):
            if _prev is _sentinel:
                sys.modules.pop(_name, None)
            else:
                sys.modules[_name] = _prev


def test_keep_set_covers_contract_and_producer_only_tables():
    mod = _load_module()
    keep = mod.build_keep_set()

    # Producer-only table outside the parser contract — must be kept.
    assert 'espn_standings' in keep
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
        'espn_standings',            # producer-only -> KEEP
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
    assert 'espn_standings' not in orphans


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
