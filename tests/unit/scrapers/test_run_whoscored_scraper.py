"""
Unit tests for ``dags/scripts/run_whoscored_scraper.py`` exit-code logic.

Regression target: previously the runner unconditionally returned ``0``,
even when individual ``scrape_*`` methods raised and were captured into
``results['errors']``. That made Airflow tasks succeed silently while
the underlying scrape was partially or wholly broken.

Fix under test (line 209): ::

    return 1 if results.get('errors') else 0

These tests stub the heavy ``scrapers.whoscored.WhoScoredScraper`` import
so the runner can be exercised without real browser / Trino.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_scraper_class(*, errors: bool):
    """Return a class whose instances act as a context-manager scraper.

    If ``errors`` is True, every ``scrape_*`` method raises so the runner
    appends to ``results['errors']``. Otherwise the methods return empty
    dicts (success, no tables).
    """
    scraper = MagicMock()
    # #616: runner calls scraper.get_traffic_stats(); stub so json.dump stays serializable.
    scraper.get_traffic_stats.return_value = {}

    if errors:
        def _boom(*a, **k):
            raise RuntimeError("forced failure")

        scraper.scrape_schedule.side_effect = _boom
        scraper.scrape_missing_players.side_effect = _boom
        scraper.scrape_season_stages.side_effect = _boom
        scraper.scrape_events.side_effect = _boom
    else:
        scraper.scrape_schedule.return_value = {}
        scraper.scrape_missing_players.return_value = {}
        scraper.scrape_season_stages.return_value = {}
        scraper.scrape_events.return_value = {}

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls, pre_main=None) -> int:
    """Call ``run_whoscored_scraper.main()`` with sys.argv replaced.

    The runner does ``from scrapers.whoscored import WhoScoredScraper``
    inside ``main()`` (lazy import to avoid heavy ``scrapers/__init__.py``
    side-effects), so we need to install the stub via ``sys.modules``
    *before* main() runs. ``patch.dict('sys.modules', ...)`` handles
    that and cleans up after.

    ``pre_main`` (optional): callable receiving the freshly reloaded module,
    used to patch module attributes (e.g. the #878 skip-existing probe)
    before ``main()`` executes.
    """
    # Build a minimal fake ``scrapers.whoscored`` package surface.
    stub_pkg = MagicMock()
    stub_pkg.WhoScoredScraper = scraper_cls

    sys.argv = ["run_whoscored_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.whoscored": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_whoscored_scraper", None)
        mod = importlib.import_module("dags.scripts.run_whoscored_scraper")
        importlib.reload(mod)
        if pre_main is not None:
            pre_main(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunWhoscoredExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="whoscored_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """All scrape_* methods succeed → empty errors list → exit 0."""
        scraper_cls = _build_scraper_class(errors=False)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 0, "Expected exit 0 when results.errors is empty"

        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_exit_one_when_scrape_methods_raise(self, temp_output):
        """Every scrape_* method raises → results.errors populated → exit 1.

        This is the regression: previously the runner returned 0 here.
        """
        scraper_cls = _build_scraper_class(errors=True)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1, (
            "Expected exit 1 when results.errors is populated — "
            "this is the bug fix being verified."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        # 3 cheap tasks ran and each raised once
        assert len(payload["errors"]) == 3
        assert any("schedule" in e for e in payload["errors"])
        assert any("missing_players" in e for e in payload["errors"])
        assert any("season_stages" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_exit_one_with_partial_failure(self, temp_output):
        """Mixed success/failure also yields exit 1 — partial-error must not
        be reported as success."""
        scraper = MagicMock()
        scraper.get_traffic_stats.return_value = {}
        scraper.scrape_schedule.return_value = {
            "schedule": "iceberg.bronze.whoscored_schedule"
        }
        scraper.scrape_missing_players.side_effect = RuntimeError("502 BAD")
        scraper.scrape_season_stages.return_value = {}
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper_cls = MagicMock(return_value=scraper)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("missing_players" in e for e in payload["errors"])
        # schedule succeeded — should still be listed as a written table
        assert "iceberg.bronze.whoscored_schedule" in payload["tables"]


# ---------------------------------------------------------------------------
# #878 — fast schedule path + skip-existing probe
# ---------------------------------------------------------------------------
def _import_runner():
    """Import the runner module fresh, without stubbing scrapers.whoscored.

    Safe for helper-level tests: the heavy ``from scrapers.whoscored import
    WhoScoredScraper`` lives inside ``main()``, not at module level.
    """
    sys.modules.pop("dags.scripts.run_whoscored_scraper", None)
    return importlib.import_module("dags.scripts.run_whoscored_scraper")


class TestSeasonHelpers:
    """#878: local season-token converters (kept in sync with scraper.py)."""

    @pytest.mark.unit
    def test_year_start_converts_to_slug(self):
        mod = _import_runner()
        assert mod._season_to_bronze_str(2016) == "1617"

    @pytest.mark.unit
    def test_short_token_passes_through(self):
        mod = _import_runner()
        assert mod._season_to_bronze_str(1718) == "1718"
        assert mod._season_to_bronze_str("2526") == "2526"

    @pytest.mark.unit
    def test_9900_edge(self):
        mod = _import_runner()
        assert mod._season_to_bronze_str(1999) == "9900"
        assert mod._season_start_year("9900") == 1999

    @pytest.mark.unit
    def test_start_year_both_forms(self):
        mod = _import_runner()
        assert mod._season_start_year(2016) == 2016
        assert mod._season_start_year("2526") == 2025

    @pytest.mark.unit
    def test_garbage_raises(self):
        mod = _import_runner()
        with pytest.raises(ValueError):
            mod._season_to_bronze_str("20xx")
        with pytest.raises(ValueError):
            mod._season_start_year("20256")


class TestCompletedSchedulePairs:
    """#878: the bronze completeness probe (schedule AND season_stages)."""

    @staticmethod
    def _fake_conn(sched_rows, stages_rows):
        """Connection whose cursor returns sched_rows then stages_rows."""
        cur = MagicMock()
        cur.fetchall.side_effect = [sched_rows, stages_rows]
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    @pytest.mark.unit
    def test_pair_complete_when_both_tables_pass_floor(self):
        mod = _import_runner()
        conn = self._fake_conn(
            sched_rows=[("GER-Bundesliga", "1617", 306)],
            stages_rows=[("GER-Bundesliga", "1617", 2)],
        )
        with patch.object(mod, "_trino_connect", return_value=conn):
            done = mod._completed_schedule_pairs(["GER-Bundesliga"], ["1617"])
        assert done == {("GER-Bundesliga", "1617")}

    @pytest.mark.unit
    def test_schedule_full_but_stages_empty_is_not_complete(self):
        """Regression for the rc=124 tail: the backfill timeout kills the
        unit inside missing_players, AFTER schedule is written but BEFORE
        season_stages runs. A schedule-only probe would no-op forever and
        stages would never backfill."""
        mod = _import_runner()
        conn = self._fake_conn(
            sched_rows=[("ESP-La Liga", "1617", 380)],
            stages_rows=[],
        )
        with patch.object(mod, "_trino_connect", return_value=conn):
            done = mod._completed_schedule_pairs(["ESP-La Liga"], ["1617"])
        assert done == set()

    @pytest.mark.unit
    def test_below_schedule_floor_is_not_complete(self):
        mod = _import_runner()
        conn = self._fake_conn(
            sched_rows=[("GER-Bundesliga", "1617", 100)],
            stages_rows=[("GER-Bundesliga", "1617", 2)],
        )
        with patch.object(mod, "_trino_connect", return_value=conn):
            done = mod._completed_schedule_pairs(["GER-Bundesliga"], ["1617"])
        assert done == set()

    @pytest.mark.unit
    def test_trino_error_fails_open(self):
        mod = _import_runner()
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError("trino down")
        with patch.object(mod, "_trino_connect", return_value=conn):
            done = mod._completed_schedule_pairs(["GER-Bundesliga"], ["1617"])
        assert done == set()

    @pytest.mark.unit
    def test_no_connection_fails_open(self):
        mod = _import_runner()
        with patch.object(mod, "_trino_connect", return_value=None):
            done = mod._completed_schedule_pairs(["GER-Bundesliga"], ["1617"])
        assert done == set()


class TestSkipMissingPlayersFlag:
    """#878: --skip-missing-players removes the per-match Preview crawl."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="whoscored_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_flag_skips_only_missing_players(self, temp_output):
        scraper_cls = _build_scraper_class(errors=False)
        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga",
                "--seasons", "2016",
                "--skip-events",
                "--skip-missing-players",
                "--output", temp_output,
            ],
            scraper_cls,
        )
        assert rc == 0
        scraper = scraper_cls.return_value
        scraper.scrape_schedule.assert_called_once()
        scraper.scrape_season_stages.assert_called_once()
        scraper.scrape_missing_players.assert_not_called()
        scraper.scrape_events.assert_not_called()

    @pytest.mark.unit
    def test_default_path_still_runs_everything(self, temp_output):
        """No new flags → prod-DAG behavior unchanged: all subtasks run."""
        scraper_cls = _build_scraper_class(errors=False)
        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga",
                "--seasons", "2016",
                "--output", temp_output,
            ],
            scraper_cls,
        )
        assert rc == 0
        scraper = scraper_cls.return_value
        scraper.scrape_schedule.assert_called_once()
        scraper.scrape_missing_players.assert_called_once()
        scraper.scrape_season_stages.assert_called_once()
        scraper.scrape_events.assert_called_once()


class TestSkipExistingGate:
    """#878: --skip-existing full no-op never constructs the scraper."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="whoscored_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    FAST_PATH_ARGS = ["--skip-events", "--skip-missing-players", "--skip-existing"]

    @pytest.mark.unit
    def test_full_noop_skips_scraper_entirely(self, temp_output):
        scraper_cls = _build_scraper_class(errors=False)

        def _patch_probe(mod):
            mod._completed_schedule_pairs = MagicMock(
                return_value={("GER-Bundesliga", "1617")}
            )

        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga",
                "--seasons", "2016",
                *self.FAST_PATH_ARGS,
                "--output", temp_output,
            ],
            scraper_cls,
            pre_main=_patch_probe,
        )
        assert rc == 0
        scraper_cls.assert_not_called()

        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["skip_existing"] is True
        assert payload["skipped_pairs"] == [["GER-Bundesliga", "1617"]]
        assert payload["errors"] == []
        sched_traffic = payload["traffic"]["schedule"]
        assert sched_traffic["requests"] == 0
        assert sched_traffic["sessions_created"] == 0
        assert sched_traffic["fs_response_bytes"] == 0

    @pytest.mark.unit
    def test_partial_pairs_scrape_remaining(self, temp_output):
        """One of two leagues complete → only the other is scraped."""
        scraper_cls = _build_scraper_class(errors=False)

        def _patch_probe(mod):
            mod._completed_schedule_pairs = MagicMock(
                return_value={("GER-Bundesliga", "1617")}
            )

        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga,ITA-Serie A",
                "--seasons", "2016",
                *self.FAST_PATH_ARGS,
                "--output", temp_output,
            ],
            scraper_cls,
            pre_main=_patch_probe,
        )
        assert rc == 0
        assert scraper_cls.call_count == 1
        assert scraper_cls.call_args.kwargs["leagues"] == ["ITA-Serie A"]
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["skipped_pairs"] == [["GER-Bundesliga", "1617"]]

    @pytest.mark.unit
    def test_current_season_never_skipped(self, temp_output):
        """A pair whose season is current must scrape even if the probe
        would call it complete (it keeps growing until season end)."""
        scraper_cls = _build_scraper_class(errors=False)
        probe = MagicMock(return_value=set())

        def _patch_probe(mod):
            mod._completed_schedule_pairs = probe
            # Freeze "now" so the test doesn't rot: pretend it's 2017-01-15,
            # making season 2016 (16/17) the CURRENT season.
            mod.datetime = MagicMock()
            mod.datetime.now.return_value = MagicMock(year=2017, month=1)

        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga",
                "--seasons", "2016",
                *self.FAST_PATH_ARGS,
                "--output", temp_output,
            ],
            scraper_cls,
            pre_main=_patch_probe,
        )
        assert rc == 0
        # Season 2016 is current → not "past" → probe has nothing to check.
        probe.assert_not_called()
        scraper_cls.assert_called_once()

    @pytest.mark.unit
    def test_ignored_without_fast_path_flags(self, temp_output):
        """--skip-existing without --skip-events --skip-missing-players is
        ignored: a bronze schedule probe says nothing about events."""
        scraper_cls = _build_scraper_class(errors=False)
        probe = MagicMock(return_value={("GER-Bundesliga", "1617")})

        def _patch_probe(mod):
            mod._completed_schedule_pairs = probe

        rc = _run_main(
            [
                "--leagues", "GER-Bundesliga",
                "--seasons", "2016",
                "--skip-events",
                "--skip-existing",
                "--output", temp_output,
            ],
            scraper_cls,
            pre_main=_patch_probe,
        )
        assert rc == 0
        probe.assert_not_called()
        scraper_cls.assert_called_once()
        scraper = scraper_cls.return_value
        scraper.scrape_missing_players.assert_called_once()
