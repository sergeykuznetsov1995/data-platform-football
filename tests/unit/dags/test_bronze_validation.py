"""
Unit tests for ``dags/utils/bronze_validation.py`` — fail-closed validator.

Moved from ``test_dag_ingest_whoscored.py`` when the validator was extracted
to a shared util (issue #466). Regression coverage for issue #110:
``validate_table`` previously called ``MIN_ROW_THRESHOLDS.get(threshold_key,
0)`` which silently passed any empty Bronze table when the key was missing
(root cause amplifier of #102). The function must raise ``AirflowException``
on a missing threshold key instead of falling back to 0.

These tests run on the host using the lightweight Airflow stubs from
``tests/unit/dags/conftest.py``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


def _load_module():
    sys.modules.pop("utils.bronze_validation", None)
    return importlib.import_module("utils.bronze_validation")


class TestValidateTableFailClosed:
    @pytest.mark.unit
    def test_known_key_passes_when_rows_above_threshold(self, monkeypatch):
        mod = _load_module()
        # Derive the passing row count from the configured threshold so the
        # test survives threshold re-scaling (e.g. × len(WHOSCORED_LEAGUES)).
        threshold = mod.MIN_ROW_THRESHOLDS["whoscored_schedule"]
        rows = threshold + 60
        monkeypatch.setattr(mod, "bronze_count", lambda _t: rows)

        result = mod.validate_table("whoscored_schedule", "whoscored_schedule")

        assert result["rows"] == rows
        assert result["threshold"] == threshold

    @pytest.mark.unit
    def test_missing_key_raises_instead_of_silent_pass(self, monkeypatch):
        """Regression: missing MIN_ROW_THRESHOLDS key must NOT silently
        fall back to 0. This is the hidden-enabler from issue #102 that
        let an empty ``bronze.whoscored_schedule`` scrape pass as success.
        """
        mod = _load_module()
        from airflow.exceptions import AirflowException

        # bronze_count must not be reached — the missing-key guard fires first
        def _should_not_be_called(_t):
            raise AssertionError("bronze_count must not be called when key missing")

        monkeypatch.setattr(mod, "bronze_count", _should_not_be_called)
        monkeypatch.setattr(
            mod,
            "MIN_ROW_THRESHOLDS",
            {k: v for k, v in mod.MIN_ROW_THRESHOLDS.items() if k != "whoscored_schedule"},
        )

        with pytest.raises(AirflowException, match="missing key 'whoscored_schedule'"):
            mod.validate_table("whoscored_schedule", "whoscored_schedule")

    @pytest.mark.unit
    def test_rows_below_threshold_raises(self, monkeypatch):
        """An empty/wiped Bronze table must hard-fail the validation task."""
        mod = _load_module()
        from airflow.exceptions import AirflowException

        monkeypatch.setattr(mod, "bronze_count", lambda _t: 0)

        with pytest.raises(AirflowException, match="0 rows < threshold"):
            mod.validate_table("whoscored_schedule", "whoscored_schedule")


class TestValidateTablePerLeague:
    """#920 Phase 2: with a ``leagues`` scope each competition is compared
    against its own floor — a league missing from the table can no longer
    hide behind the whole-table aggregate."""

    _LEAGUES = ["ENG-Premier League", "INT-World Cup"]

    def _patched(self, monkeypatch, counts, out_of_window=False):
        mod = _load_module()
        # Floors: EPL 340, WC 93 — the real competitions.yaml-derived values,
        # pinned here so the test doesn't need MEDALLION_CONFIG_DIR. The
        # window predicate is pinned too — the real one flips when the WC
        # window closes (2026-08-02) and would make these tests date-bombs.
        floors = {"ENG-Premier League": 340, "INT-World Cup": 93}
        monkeypatch.setattr(
            mod, "get_min_row_threshold", lambda key, lg: floors[lg]
        )
        monkeypatch.setattr(
            mod, "_out_of_window_tournament",
            lambda lg: out_of_window and lg.startswith("INT-"),
        )
        monkeypatch.setattr(mod, "bronze_count_by_league", lambda _t: counts)
        return mod

    @pytest.mark.unit
    def test_all_leagues_above_floor_pass(self, monkeypatch):
        mod = self._patched(
            monkeypatch, {"ENG-Premier League": 380, "INT-World Cup": 104}
        )
        result = mod.validate_table(
            "whoscored_schedule", "whoscored_schedule", leagues=self._LEAGUES
        )
        assert result["rows"] == 484
        assert result["per_league"]["INT-World Cup"] == {
            "rows": 104, "threshold": 93,
        }

    @pytest.mark.unit
    def test_missing_league_counts_as_zero_and_fails(self, monkeypatch):
        """The core #920 Phase 2 guarantee: a league entirely absent from the
        table fails ITS floor even when the aggregate looks healthy."""
        mod = self._patched(monkeypatch, {"ENG-Premier League": 9_500})
        from airflow.exceptions import AirflowException

        with pytest.raises(
            AirflowException, match="INT-World Cup: 0 rows < 93"
        ):
            mod.validate_table(
                "whoscored_schedule", "whoscored_schedule",
                leagues=self._LEAGUES,
            )

    @pytest.mark.unit
    def test_failure_reports_every_league_below_floor(self, monkeypatch):
        mod = self._patched(
            monkeypatch, {"ENG-Premier League": 12, "INT-World Cup": 5}
        )
        from airflow.exceptions import AirflowException

        with pytest.raises(AirflowException) as exc:
            mod.validate_table(
                "whoscored_schedule", "whoscored_schedule",
                leagues=self._LEAGUES,
            )
        msg = str(exc.value)
        assert "ENG-Premier League: 12 rows < 340" in msg
        assert "INT-World Cup: 5 rows < 93" in msg

    @pytest.mark.unit
    def test_unregistered_key_with_leagues_refuses_downgrade(self, monkeypatch):
        """Requesting per-league scope for a key without a per-league base
        must raise, not silently fall back to the whole-table aggregate —
        the silent downgrade would reintroduce the masking this API exists
        to remove (a dev wires leagues= for a new table, forgets the
        PER_LEAGUE_FLOOR_BASES entry, gets green aggregate checks)."""
        mod = _load_module()
        from airflow.exceptions import AirflowException

        monkeypatch.setattr(
            mod, "bronze_count",
            lambda _t: (_ for _ in ()).throw(AssertionError("unreachable")),
        )
        with pytest.raises(
            AirflowException, match="no PER_LEAGUE_FLOOR_BASES entry"
        ):
            mod.validate_table(
                "espn_lineup", "espn_lineup", leagues=self._LEAGUES
            )

    @pytest.mark.unit
    def test_empty_league_scope_falls_back_to_whole_table(self, monkeypatch):
        """leagues=[] must NOT take the per-league path (zero floors, zero
        checks, silent pass) — a derived scope that empties out falls back
        to the whole-table floor (the sofifa NON_INTERNATIONAL_LEAGUES list
        empties if LEAGUES ever goes tournament-only)."""
        mod = _load_module()

        def _no_group_by(_t):
            raise AssertionError("per-league count must not run for []")

        monkeypatch.setattr(mod, "bronze_count_by_league", _no_group_by)
        monkeypatch.setattr(
            mod, "bronze_count",
            lambda _t: mod.MIN_ROW_THRESHOLDS["whoscored_schedule"],
        )
        result = mod.validate_table(
            "whoscored_schedule", "whoscored_schedule", leagues=[]
        )
        assert result["threshold"] == mod.MIN_ROW_THRESHOLDS["whoscored_schedule"]

    @pytest.mark.unit
    def test_out_of_window_tournament_with_zero_rows_passes(self, monkeypatch):
        """Pre-activation grace: a single-year tournament league with an
        EMPTY partition outside its window is the healthy no-op state (the
        yaml activation recipe adds the league weeks before the window
        opens) — not a failure."""
        mod = self._patched(
            monkeypatch, {"ENG-Premier League": 9_500}, out_of_window=True
        )
        result = mod.validate_table(
            "whoscored_schedule", "whoscored_schedule", leagues=self._LEAGUES
        )
        assert result["per_league"]["INT-World Cup"]["rows"] == 0

    @pytest.mark.unit
    def test_out_of_window_partial_partition_still_fails(self, monkeypatch):
        """The grace covers ONLY rows == 0: a partially wiped tournament
        partition below its floor fails, in or out of window."""
        mod = self._patched(
            monkeypatch,
            {"ENG-Premier League": 9_500, "INT-World Cup": 40},
            out_of_window=True,
        )
        from airflow.exceptions import AirflowException

        with pytest.raises(AirflowException, match="INT-World Cup: 40 rows < 93"):
            mod.validate_table(
                "whoscored_schedule", "whoscored_schedule",
                leagues=self._LEAGUES,
            )

    @pytest.mark.unit
    def test_unknown_key_with_leagues_still_fails_closed(self, monkeypatch):
        mod = _load_module()
        from airflow.exceptions import AirflowException

        monkeypatch.setattr(
            mod, "bronze_count",
            lambda _t: (_ for _ in ()).throw(AssertionError("unreachable")),
        )
        with pytest.raises(
            AirflowException, match="no PER_LEAGUE_FLOOR_BASES entry"
        ):
            mod.validate_table("nope", "nope_table", leagues=self._LEAGUES)

    @pytest.mark.unit
    def test_floor_derivation_error_wrapped_fail_closed(self, monkeypatch):
        """A stub/unknown competition in the scope must fail the task, not
        default to floor 0 (the #102/#110 silent-pass class)."""
        mod = _load_module()
        from airflow.exceptions import AirflowException
        from utils.medallion_config import MedallionConfigError

        def _raise(key, lg):
            raise MedallionConfigError(f"competition not found: {lg!r}")

        monkeypatch.setattr(mod, "get_min_row_threshold", _raise)
        with pytest.raises(
            AirflowException, match="Cannot derive per-league floor"
        ):
            mod.validate_table(
                "whoscored_schedule", "whoscored_schedule",
                leagues=["XX-Nope"],
            )
