"""
Unit tests for ``dags/scripts/run_sofifa_scraper.py`` exit-code logic.

Issue #466: the runner previously returned ``0`` unconditionally — failed
``read_*()`` steps left ``results['errors']`` populated but the BashOperator
still saw exit 0 → green DAG while sofifa_team_ratings / versions / leagues /
player_ratings silently went stale for weeks. The fix returns ``1`` whenever
``results['errors']`` is non-empty.

The runner does ``from scrapers.sofifa import SoFIFAScraper`` lazily inside
``main()``; we install a stub via ``patch.dict('sys.modules', ...)``.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


_READ_METHODS = (
    'read_players',
    'read_teams',
    'read_team_ratings',
    'read_versions',
    'read_leagues',
    'read_player_ratings',
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_scraper(*, errors: bool):
    """Build a stub SoFIFAScraper context-manager.

    Successful path: every ``read_*()`` returns an EMPTY DataFrame so the
    runner skips ``save_to_iceberg()`` but does NOT append to errors.
    """
    scraper = MagicMock()
    # #616: runner calls scraper.get_traffic_stats(); stub so json.dump stays serializable.
    scraper.get_traffic_stats.return_value = {}

    for method in _READ_METHODS:
        if errors:
            getattr(scraper, method).side_effect = RuntimeError("forced failure")
        else:
            getattr(scraper, method).return_value = pd.DataFrame()

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_sofifa_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.SoFIFAScraper = scraper_cls

    sys.argv = ["run_sofifa_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.sofifa": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_sofifa_scraper", None)
        mod = importlib.import_module("dags.scripts.run_sofifa_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunSofifaExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """Empty DataFrames → nothing saved, no errors → exit 0."""
        scraper_cls = _build_scraper(errors=False)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 0
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_exit_one_when_errors(self, temp_output):
        """Every read_* raises → 6 errors → exit MUST be 1.

        Direct regression on issue #466: previously the runner returned 0
        unconditionally.
        """
        scraper_cls = _build_scraper(errors=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 1, (
            "Expected exit 1 when results.errors is populated — "
            "green-DAG-on-total-failure regression (#466)."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == len(_READ_METHODS)

    @pytest.mark.unit
    def test_exit_one_with_partial_failure(self, temp_output):
        """One step fails (player_ratings), the rest are empty — exit MUST
        be 1 (any non-empty ``errors`` ⇒ failure)."""
        scraper = MagicMock()
        scraper.get_traffic_stats.return_value = {}
        empty = pd.DataFrame()
        for method in _READ_METHODS:
            getattr(scraper, method).return_value = empty
        scraper.read_player_ratings.side_effect = RuntimeError("Turnstile")
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper_cls = MagicMock(return_value=scraper)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == 1
        assert "Player ratings" in payload["errors"][0]


class TestRunSofifaVersionsParsing:
    """Cover ``--versions`` parsing (#665).

    ``soccerdata.SoFIFA`` accepts ``'latest'|'all'|int|list[int]`` and raises
    ``ValueError`` on a raw digit string, so the runner must turn explicit
    comma-separated version IDs into ``list[int]`` before constructing the
    scraper — while leaving ``'latest'``/``'all'`` as pass-through strings.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_versions_comma_separated_parsed_to_int_list(self, temp_output):
        """``--versions "180084,190075"`` → SoFIFAScraper(versions=[180084, 190075])."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "180084,190075", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == [180084, 190075]

    @pytest.mark.unit
    def test_versions_single_id_parsed_to_int_list(self, temp_output):
        """A lone version ID still becomes a list[int] (soccerdata ``.loc[[id]]``)."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "180084", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == [180084]

    @pytest.mark.unit
    def test_versions_latest_passthrough(self, temp_output):
        """``--versions "latest"`` (the weekly path) stays a string, untouched."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "latest", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == "latest"

    @pytest.mark.unit
    def test_versions_all_passthrough(self, temp_output):
        """``--versions "all"`` stays a string, untouched."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "all", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == "all"


# ---------------------------------------------------------------------------
# Incremental version_id skip
# ---------------------------------------------------------------------------
_HEAVY_METHODS = (
    'read_players', 'read_teams', 'read_team_ratings', 'read_player_ratings',
)
_LIGHT_METHODS = ('read_versions', 'read_leagues')


def _build_versioned_scraper():
    """Stub scraper whose reader exposes a real ``versions`` frame, so the
    incremental check resolves latest version_id = 260035 ('FC 26')."""
    scraper = _build_scraper(errors=False).return_value
    reader = MagicMock()
    reader.versions = pd.DataFrame(
        [{'fifa_edition': 'FC 26', 'update': 'Jun 24, 2026'}],
        index=pd.Index([260035], name='version_id'),
    )
    scraper._get_reader.return_value = reader
    return scraper


def _run_main_with_probe(args, scraper, *, bronze_current):
    """Like ``_run_main`` but stubs the Bronze probe on the reloaded module.

    Returns ``(exit_code, probe_mock)``.
    """
    stub_pkg = MagicMock()
    stub_pkg.SoFIFAScraper = MagicMock(return_value=scraper)
    sys.argv = ["run_sofifa_scraper.py"] + args
    with patch.dict(sys.modules, {"scrapers.sofifa": stub_pkg}):
        sys.modules.pop("dags.scripts.run_sofifa_scraper", None)
        mod = importlib.import_module("dags.scripts.run_sofifa_scraper")
        importlib.reload(mod)
        probe = MagicMock(return_value=bronze_current)
        mod._bronze_up_to_date = probe
        return mod.main(), probe


class TestSofifaIncrementalSkip:
    """When Bronze already carries the latest sofifa version_id, the heavy
    steps (players/teams/team_ratings/player_ratings) must not run at all —
    only the two 1-request lookups (versions/leagues) refresh."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_skip_when_bronze_current(self, temp_output):
        scraper = _build_versioned_scraper()

        rc, probe = _run_main_with_probe(
            ["--output", temp_output], scraper, bronze_current=True,
        )

        assert rc == 0
        probe.assert_called_once_with(260035, 'FC 26', 'Jun 24, 2026')
        for m in _HEAVY_METHODS:
            getattr(scraper, m).assert_not_called()
        for m in _LIGHT_METHODS:
            getattr(scraper, m).assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["skipped"] == {
            "reason": "version_unchanged", "version_id": 260035,
        }
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_full_run_when_bronze_stale(self, temp_output):
        scraper = _build_versioned_scraper()

        rc, probe = _run_main_with_probe(
            ["--output", temp_output], scraper, bronze_current=False,
        )

        assert rc == 0
        probe.assert_called_once()
        for m in _HEAVY_METHODS:
            getattr(scraper, m).assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert "skipped" not in payload

    @pytest.mark.unit
    def test_force_full_bypasses_probe(self, temp_output):
        scraper = _build_versioned_scraper()

        rc, probe = _run_main_with_probe(
            ["--force-full", "--output", temp_output],
            scraper, bronze_current=True,
        )

        assert rc == 0
        probe.assert_not_called()
        for m in _HEAVY_METHODS:
            getattr(scraper, m).assert_called_once()

    @pytest.mark.unit
    def test_explicit_versions_bypass_probe(self, temp_output):
        """An explicit version list is a deliberate backfill — never skipped."""
        scraper = _build_versioned_scraper()

        rc, probe = _run_main_with_probe(
            ["--versions", "180084", "--output", temp_output],
            scraper, bronze_current=True,
        )

        assert rc == 0
        probe.assert_not_called()
        for m in _HEAVY_METHODS:
            getattr(scraper, m).assert_called_once()

    @pytest.mark.unit
    def test_probe_error_falls_back_to_full_run(self, temp_output):
        """Fail-open: a broken probe (Trino down etc.) must never block the
        scrape — and must not fail the run either."""
        scraper = _build_versioned_scraper()
        stub_pkg = MagicMock()
        stub_pkg.SoFIFAScraper = MagicMock(return_value=scraper)
        sys.argv = ["run_sofifa_scraper.py", "--output", temp_output]
        with patch.dict(sys.modules, {"scrapers.sofifa": stub_pkg}):
            sys.modules.pop("dags.scripts.run_sofifa_scraper", None)
            mod = importlib.import_module("dags.scripts.run_sofifa_scraper")
            importlib.reload(mod)
            mod._bronze_up_to_date = MagicMock(side_effect=RuntimeError("boom"))
            rc = mod.main()

        assert rc == 0
        for m in _HEAVY_METHODS:
            getattr(scraper, m).assert_called_once()


# ---------------------------------------------------------------------------
# #583: completeness-guard wiring
# ---------------------------------------------------------------------------
def _build_guard_scraper(*, guard_blocks: bool = False, with_edition: bool = True):
    """Stub SoFIFAScraper whose ``read_teams`` returns a non-empty frame (every
    other read stays empty) so exactly one ``save_to_iceberg`` is reached.
    ``teams`` takes the direct save path (no ``_process_*`` step), keeping the
    guard assertion isolated.

    ``with_edition=False`` drops the ``fifa_edition`` column → the dynamic
    ``part`` is ``None`` → the guard must NOT be armed (passing
    ``min_replace_ratio`` without ``replace_partitions`` would raise
    ``ValueError``). ``guard_blocks=True`` simulates the BaseScraper-level guard
    refusing the save (``ReplaceGuardError``).
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    cols = {
        'team': [f'Team {i}' for i in range(20)],
        'overall': [70 + i for i in range(20)],
    }
    if with_edition:
        cols = {'fifa_edition': ['24'] * 20, **cols}
    df = pd.DataFrame(cols)

    scraper = MagicMock()
    scraper.get_traffic_stats.return_value = {}
    for method in _READ_METHODS:
        getattr(scraper, method).return_value = pd.DataFrame()
    scraper.read_teams.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            "new=2 rows < 90% of existing=20 for bronze.sofifa_teams "
            "— refusing replace_partitions save (would shrink the partition)"
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.sofifa_teams'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestSofifaReplaceGuard:
    """#583: completeness-guard wiring in the SoFIFA runner.

    Arms the guard per-table, maps ``ReplaceGuardError`` to exit 3, lets
    ``--force-replace`` disarm it, and — uniquely for SoFIFA — keeps the guard
    OFF when the dynamic ``part`` is ``None`` (``fifa_edition`` absent).
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """save_to_iceberg raises ReplaceGuardError → exit 3 +
        SOFIFA_REPLACE_GUARD marker (distinct from the exit-1 path)."""
        scraper = _build_guard_scraper(guard_blocks=True)

        rc = _run_main(["--output", temp_output], MagicMock(return_value=scraper))

        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFIFA_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run with fifa_edition present passes min_replace_ratio=0.9
        (raw COUNT(*), no key)."""
        scraper = _build_guard_scraper()

        rc = _run_main(["--output", temp_output], MagicMock(return_value=scraper))

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["fifa_edition"]
        # full-state per fifa_edition → raw COUNT(*), no replace_guard_key
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the save."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        assert scraper.save_to_iceberg.call_args.kwargs["min_replace_ratio"] is None

    @pytest.mark.unit
    def test_missing_partition_col_does_not_arm_guard(self, temp_output):
        """No fifa_edition → dynamic part is None → guard stays OFF
        (min_replace_ratio=None, replace_partitions=None) with NO ValueError."""
        scraper = _build_guard_scraper(with_edition=False)

        rc = _run_main(["--output", temp_output], MagicMock(return_value=scraper))

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["replace_partitions"] is None
        assert kwargs["min_replace_ratio"] is None
