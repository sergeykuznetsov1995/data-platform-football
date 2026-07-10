"""
ensure_league_dict — the zero-manual-step install of the repo's soccerdata
league_dict fragment (#920 Phase 3).

The WC ESPN ingest used to depend on a HAND-PATCHED, unversioned
league_dict.json inside the soccerdata_cache docker volume. These tests pin
the replacement mechanism: repo fragment authoritative for its keys, foreign
keys preserved (the prod patch's club entries and the documented Understat
RUS-Premier League path), idempotent, atomic, loud on the import-cache edge.
"""

from __future__ import annotations

import json
import sys
import types

import pytest

from scrapers.base.soccerdata_config import (
    FRAGMENT_PATH,
    ensure_league_dict,
    soccerdata_config_dir,
)


def _target(tmp_path):
    return tmp_path / 'soccerdata' / 'config' / 'league_dict.json'


@pytest.mark.unit
class TestInstall:
    def test_creates_file_from_fragment(self, tmp_path):
        # isolated_soccerdata_dir (autouse) points SOCCERDATA_DIR at tmp_path.
        ensure_league_dict()
        target = _target(tmp_path)
        assert target.is_file()
        installed = json.loads(target.read_text(encoding='utf-8'))
        fragment = json.loads(FRAGMENT_PATH.read_text(encoding='utf-8'))
        assert installed == fragment

    def test_idempotent_no_rewrite(self, tmp_path):
        ensure_league_dict()
        target = _target(tmp_path)
        before = target.stat().st_mtime_ns
        ensure_league_dict()
        assert target.stat().st_mtime_ns == before

    def test_merge_preserves_foreign_keys(self, tmp_path):
        # The prod VM's hand-patched file carries full club entries; the
        # documented Understat extension path (utils/config.py) adds
        # RUS-Premier League the same way. Both must survive the install.
        target = _target(tmp_path)
        target.parent.mkdir(parents=True)
        target.write_text(json.dumps({
            'RUS-Premier League': {'Understat': 'RFPL'},
            'INT-World Cup': {'stale': 'hand-patched'},
        }))
        ensure_league_dict()
        installed = json.loads(target.read_text(encoding='utf-8'))
        assert installed['RUS-Premier League'] == {'Understat': 'RFPL'}
        # ...but the fragment is authoritative for ITS keys (per-key replace).
        assert installed['INT-World Cup']['ESPN'] == 'fifa.world'
        assert 'stale' not in installed['INT-World Cup']

    def test_corrupt_target_moved_aside_and_rebuilt(self, tmp_path):
        target = _target(tmp_path)
        target.parent.mkdir(parents=True)
        target.write_text('{not json')
        ensure_league_dict()
        assert json.loads(target.read_text(encoding='utf-8'))
        assert target.with_suffix('.json.corrupt').is_file()

    def test_missing_fragment_is_loud_noop(self, tmp_path, monkeypatch, caplog):
        import scrapers.base.soccerdata_config as mod
        monkeypatch.setattr(
            mod, 'FRAGMENT_PATH', tmp_path / 'nope' / 'league_dict.json')
        ensure_league_dict()
        assert not _target(tmp_path).exists()
        assert any('fragment missing' in r.message for r in caplog.records)

    def test_config_dir_mirrors_soccerdata(self, tmp_path):
        assert soccerdata_config_dir() == tmp_path / 'soccerdata' / 'config'


@pytest.mark.unit
class TestImportCacheGuard:
    def test_raises_when_soccerdata_imported_without_required_league(
            self, monkeypatch):
        # A module-like stub whose LEAGUE_DICT is a REAL dict missing the
        # fragment leagues — the merged file can't take effect in-process.
        stub = types.ModuleType('soccerdata')
        stub._config = types.SimpleNamespace(
            LEAGUE_DICT={'ENG-Premier League': {}})
        monkeypatch.setitem(sys.modules, 'soccerdata', stub)
        with pytest.raises(RuntimeError, match='imported before'):
            ensure_league_dict(
                required_leagues=['INT-Africa Cup of Nations'])

    def test_present_league_passes(self, monkeypatch):
        stub = types.ModuleType('soccerdata')
        stub._config = types.SimpleNamespace(
            LEAGUE_DICT={'INT-World Cup': {}})
        monkeypatch.setitem(sys.modules, 'soccerdata', stub)
        ensure_league_dict(required_leagues=['INT-World Cup'])

    def test_non_fragment_league_never_trips_guard(self, monkeypatch):
        stub = types.ModuleType('soccerdata')
        stub._config = types.SimpleNamespace(LEAGUE_DICT={})
        monkeypatch.setitem(sys.modules, 'soccerdata', stub)
        ensure_league_dict(required_leagues=['ENG-Premier League'])

    def test_magicmock_stub_does_not_explode(self, monkeypatch):
        # Unit tests patch sys.modules['soccerdata'] with MagicMock — its
        # LEAGUE_DICT attr is not a dict, so the guard must skip silently.
        from unittest.mock import MagicMock
        monkeypatch.setitem(sys.modules, 'soccerdata', MagicMock())
        ensure_league_dict(required_leagues=['INT-World Cup'])


@pytest.mark.unit
class TestFragmentContent:
    """soccerdata merges custom entries with per-key FULL REPLACE — every
    committed entry must be complete, or the built-in source names it shadows
    silently vanish."""

    def _fragment(self):
        return json.loads(FRAGMENT_PATH.read_text(encoding='utf-8'))

    def test_every_entry_complete(self):
        for key, entry in self._fragment().items():
            assert entry.get('ESPN'), key            # the reason the file exists
            assert entry.get('WhoScored'), key
            assert entry.get('FBref'), key
            assert entry.get('season_code') == 'single-year', key

    def test_wc_restates_builtin_and_adds_espn(self):
        # Byte-faithful to soccerdata 1.8.8 builtin + the prod patch's ESPN
        # key (T0 recon dumped the hand-patched file).
        wc = self._fragment()['INT-World Cup']
        assert wc == {
            'FBref': 'FIFA World Cup',
            'FotMob': 'INT-World Cup',
            'WhoScored': 'International - FIFA World Cup',
            'ESPN': 'fifa.world',
            'season_code': 'single-year',
        }

    def test_euro_restates_builtin_sofascore_name(self):
        euro = self._fragment()['INT-European Championship']
        assert euro['FBref'] == 'UEFA European Football Championship'
        assert euro['Sofascore'] == 'EURO'
        assert euro['WhoScored'] == 'International - European Championship'
        assert euro['ESPN'] == 'uefa.euro'

    def test_new_tournaments_present(self):
        frag = self._fragment()
        assert frag['INT-Africa Cup of Nations']['ESPN'] == 'caf.nations'
        assert frag['INT-Africa Cup of Nations']['WhoScored'] == (
            'International - Africa Cup of Nations')
        assert frag['INT-Copa America']['ESPN'] == 'conmebol.america'
        # WhoScored's own string is unaccented Latin (T0 recon: t_id=94).
        assert frag['INT-Copa America']['WhoScored'] == (
            'International - Copa America')
