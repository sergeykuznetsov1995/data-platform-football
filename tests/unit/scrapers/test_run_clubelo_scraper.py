"""
Unit tests for ``dags/scripts/run_clubelo_scraper.py``.

The runner has two modes (#1463): ``daily`` (/Ranking + /Results snapshot,
``scrapers.clubelo.daily``) and ``history`` (club pages, #1462). Both write the
result JSON even on failure and exit non-zero on any incompleteness. The
collection modules are stubbed via ``patch.dict('sys.modules', ...)``; the
real ``exit_code`` functions decide the exit.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def temp_output():
    fd, path = tempfile.mkstemp(suffix=".json", prefix="clubelo_")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestDailyMode:
    """#1463: --mode daily runs the /Ranking + /Results snapshot."""

    @staticmethod
    def _run_daily(args, result=None, error=None):
        from scrapers.clubelo import daily as real_daily

        stub = MagicMock()
        if error is not None:
            stub.run_default.side_effect = error
        else:
            stub.run_default.return_value = result
        stub.exit_code = real_daily.exit_code
        sys.argv = ["run_clubelo_scraper.py"] + args
        with patch.dict(sys.modules, {"scrapers.clubelo.daily": stub}):
            sys.modules.pop("dags.scripts.run_clubelo_scraper", None)
            mod = importlib.import_module("dags.scripts.run_clubelo_scraper")
            importlib.reload(mod)
            return mod.main(), stub

    @staticmethod
    def _result(**overrides):
        result = {"rating_date": "2026-09-22", "fetched_at": "2026-09-25T09:30:05",
                  "rows": 1741, "provisional": 53, "written": True, "check": None,
                  "error": None, "blocked": None, "history_new_failed": 0}
        result.update(overrides)
        return result

    @pytest.mark.unit
    def test_daily_is_the_default_mode(self, temp_output):
        rc, stub = self._run_daily(["--output", temp_output], self._result())
        assert rc == 0
        stub.run_default.assert_called_once_with()
        with open(temp_output) as f:
            assert json.load(f)["rows"] == 1741

    @pytest.mark.unit
    @pytest.mark.parametrize("overrides", [
        {"written": False, "check": "C5 eloData has 900 rows, expected >= 1500"},
        {"written": False, "check": "G2 1600 clubs < 95% of 1741 of the previous rating date"},
        {"written": False, "check": "M-09 /Results h1 date 2026-09-21 != /Ranking 2026-09-22"},
        {"written": False, "blocked": "/Ranking: HTTP 403"},
        {"history_new_failed": 1},  # snapshot written, a new slug page failed
        {"written": True, "error": "TrinoError: boom"},
    ])
    def test_incomplete_daily_exits_non_zero(self, temp_output, overrides):
        rc, _ = self._run_daily(["--mode", "daily", "--output", temp_output],
                                self._result(**overrides))
        assert rc == 1

    @pytest.mark.unit
    def test_crash_exits_non_zero_with_error_in_result(self, temp_output):
        rc, _ = self._run_daily(["--output", temp_output], error=RuntimeError("boom"))
        assert rc == 1
        with open(temp_output) as f:
            assert "boom" in json.load(f)["error"]

    @pytest.mark.unit
    @pytest.mark.parametrize("old_flag", [
        ["--leagues", "ENG-Premier League"], ["--mode", "full"], ["--days-back", "30"],
        ["--force-replace"],
    ])
    def test_old_api_flags_are_gone(self, temp_output, old_flag):
        with pytest.raises(SystemExit) as exc:
            self._run_daily(["--output", temp_output] + old_flag, self._result())
        assert exc.value.code == 2

    @pytest.mark.unit
    def test_output_is_required(self):
        with pytest.raises(SystemExit) as exc:
            self._run_daily([], self._result())
        assert exc.value.code == 2


class TestHistoryMode:
    """#1462: --mode history runs the club-page history; any gap → exit != 0."""

    @staticmethod
    def _run_history(args, result=None, error=None):
        from scrapers.clubelo import history as real_history

        stub = MagicMock()
        if error is not None:
            stub.run_default.side_effect = error
        else:
            stub.run_default.return_value = result
        stub.exit_code = real_history.exit_code
        sys.argv = ["run_clubelo_scraper.py", "--mode", "history"] + args
        with patch.dict(sys.modules, {"scrapers.clubelo.history": stub}):
            sys.modules.pop("dags.scripts.run_clubelo_scraper", None)
            mod = importlib.import_module("dags.scripts.run_clubelo_scraper")
            importlib.reload(mod)
            return mod.main(), stub

    @staticmethod
    def _result(**overrides):
        result = {"rating_date": "2026-09-22", "queue": 498, "pending_before": 4,
                  "pending_after": 0, "pages_ok": 4, "no_page": 0, "pages_failed": 0,
                  "redirects": 0, "blocked": None, "error": None, "wire_bytes": 1,
                  "requests": 5}
        result.update(overrides)
        return result

    @pytest.mark.unit
    def test_complete_run_exits_0_and_writes_result(self, temp_output):
        rc, stub = self._run_history(["--batch-size", "50", "--output", temp_output],
                                     self._result())
        assert rc == 0
        stub.run_default.assert_called_once_with(batch_size=50)
        with open(temp_output) as f:
            assert json.load(f)["pages_ok"] == 4

    @pytest.mark.unit
    def test_default_batch_size_is_200(self, temp_output):
        _, stub = self._run_history(["--output", temp_output], self._result())
        stub.run_default.assert_called_once_with(batch_size=200)

    @pytest.mark.unit
    @pytest.mark.parametrize("overrides", [
        {"pages_failed": 1, "pending_after": 1},
        {"redirects": 1, "no_page": 1},
        {"blocked": "/riverplate: HTTP 403", "pending_after": 2},
        {"pending_after": 3},
        {"error": "ClubEloFetchError: /Ranking: HTTP 500"},
    ])
    def test_incomplete_run_exits_non_zero(self, temp_output, overrides):
        rc, _ = self._run_history(["--output", temp_output], self._result(**overrides))
        assert rc == 1

    @pytest.mark.unit
    def test_crash_exits_non_zero_with_error_in_result(self, temp_output):
        rc, _ = self._run_history(["--output", temp_output], error=RuntimeError("boom"))
        assert rc == 1
        with open(temp_output) as f:
            assert "boom" in json.load(f)["error"]

    @pytest.mark.unit
    def test_help_lists_history_mode(self, capsys):
        sys.argv = ["run_clubelo_scraper.py", "--help"]
        sys.modules.pop("dags.scripts.run_clubelo_scraper", None)
        mod = importlib.import_module("dags.scripts.run_clubelo_scraper")
        with pytest.raises(SystemExit) as exc:
            mod.main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "history" in out and "--batch-size" in out and "daily" in out
