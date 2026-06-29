"""Unit tests for FotMob keep-last-good merge (issue #544).

Regression: a FotMob re-scrape returned empty content for 10 matches whose
Bronze identity rows existed; under ``replace_partitions`` this overwrote
previously-good ``stats_json`` with NULL, so they dropped out of
``silver.fotmob_team_match`` and went NULL in ``gold.fct_team_match``.

``_backfill_empty_json`` backfills empty content-JSON columns from the existing
Bronze partition so a failed re-fetch can never wipe good data. These tests lock
the pure merge logic (the I/O wiring in ``read_match_details`` is defensive and
falls back to the fresh frame on any read error).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Repo root on path so ``scrapers.*`` resolves on the host.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scrapers.fotmob.scraper import (  # noqa: E402
    _PRESERVE_JSON_COLS,
    _backfill_empty_json,
    _is_empty_json_value,
)

pytestmark = pytest.mark.unit


class TestIsEmptyJsonValue:
    @pytest.mark.parametrize("value", [None, float("nan"), "", "   ", "null", "{}", "[]"])
    def test_empty_values(self, value):
        assert _is_empty_json_value(value) is True

    @pytest.mark.parametrize("value", ['{"a": 1}', "[1,2]", '{"Periods": {}}', "0", 0])
    def test_non_empty_values(self, value):
        assert _is_empty_json_value(value) is False


class TestBackfillEmptyJson:
    def test_empty_new_filled_from_existing(self):
        # Arrange: new scrape lost stats for match 4813570; Bronze still has it.
        new = pd.DataFrame([
            {"match_id": 4813570, "stats_json": None, "player_stats_json": None},
            {"match_id": 4813528, "stats_json": '{"ok": 1}', "player_stats_json": '{"p": 1}'},
        ])
        existing = pd.DataFrame([
            {"match_id": "4813570", "stats_json": '{"good": 1}', "player_stats_json": '{"x": 2}'},
            {"match_id": "4813528", "stats_json": '{"old": 9}', "player_stats_json": '{"y": 9}'},
        ])

        # Act
        out = _backfill_empty_json(new, existing)

        # Assert: empty row backfilled (int<->str key match); non-empty row untouched.
        r0 = out[out.match_id == 4813570].iloc[0]
        assert r0.stats_json == '{"good": 1}'
        assert r0.player_stats_json == '{"x": 2}'
        r1 = out[out.match_id == 4813528].iloc[0]
        assert r1.stats_json == '{"ok": 1}'  # fresh value kept, NOT overwritten

    def test_non_empty_new_never_overwritten(self):
        new = pd.DataFrame([{"match_id": 1, "stats_json": '{"fresh": 1}'}])
        existing = pd.DataFrame([{"match_id": "1", "stats_json": '{"stale": 1}'}])
        out = _backfill_empty_json(new, existing)
        assert out.iloc[0].stats_json == '{"fresh": 1}'

    def test_empty_new_and_empty_existing_stays_empty(self):
        new = pd.DataFrame([{"match_id": 1, "stats_json": None}])
        existing = pd.DataFrame([{"match_id": "1", "stats_json": "null"}])
        out = _backfill_empty_json(new, existing)
        assert _is_empty_json_value(out.iloc[0].stats_json)

    def test_match_absent_from_existing_stays_empty(self):
        new = pd.DataFrame([{"match_id": 999, "stats_json": None}])
        existing = pd.DataFrame([{"match_id": "1", "stats_json": '{"good": 1}'}])
        out = _backfill_empty_json(new, existing)
        assert _is_empty_json_value(out.iloc[0].stats_json)

    @pytest.mark.parametrize("existing", [None, pd.DataFrame()])
    def test_missing_existing_returns_new_unchanged(self, existing):
        new = pd.DataFrame([{"match_id": 1, "stats_json": None}])
        out = _backfill_empty_json(new, existing)
        assert out.equals(new)

    def test_does_not_mutate_input(self):
        new = pd.DataFrame([{"match_id": 1, "stats_json": None}])
        existing = pd.DataFrame([{"match_id": "1", "stats_json": '{"good": 1}'}])
        _backfill_empty_json(new, existing)
        assert new.iloc[0].stats_json is None  # original untouched

    def test_all_preserve_cols_backfilled(self):
        new = pd.DataFrame([{"match_id": 1, **{c: None for c in _PRESERVE_JSON_COLS}}])
        existing = pd.DataFrame([{"match_id": "1", **{c: f'{{"{c}": 1}}' for c in _PRESERVE_JSON_COLS}}])
        out = _backfill_empty_json(new, existing)
        for c in _PRESERVE_JSON_COLS:
            assert out.iloc[0][c] == f'{{"{c}": 1}}', f"{c} not backfilled"
