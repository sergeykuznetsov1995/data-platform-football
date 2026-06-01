"""Unit tests for ``dags.utils.e3_dq`` (E3.8 DQ builders).

Strategy
--------
Pure-Python attribute inspection — verify the structure of the ``Check``
lists returned by the builders. No Trino, no DuckDB; the runners
themselves are exercised by the universal ``data_quality`` test suite.

Coverage
--------
* Per-table check counts (silver / gold) match the contract.
* SPADL_ACTION_ENUM has exactly 24 values and matches the documented set.
* Critical ERROR-severity checks are present (PK, schema-version drift,
  enum violation, ref_integrity).
* All check names are unique (the runner relies on uniqueness for the
  Telegram summary table).
* Schema-version literals match the SQL header convention.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Wire dags/ onto sys.path so ``utils.e3_dq`` resolves.
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import e3_dq  # noqa: E402


pytestmark = pytest.mark.unit


# ===========================================================================
# Builder counts
# ===========================================================================


class TestCheckCounts:
    """Per-table check counts must match the contract documented in e3_dq.py."""

    def test_silver_e3_total_check_count(self):
        """Silver builders compose 9 sub-builders = 44 checks total.

        whoscored_events_spadl (9) + whoscored_team_match + whoscored_team_season
        (T6.3) + understat_team_match + understat_team_season (T6.2) +
        espn_lineup (4) + sofascore_player_profile + sofascore_team_match +
        sofascore_team_season (T6.4). Bump this number whenever a builder is
        added to ``build_silver_e3_checks``.
        """
        checks = e3_dq.build_silver_e3_checks()
        assert len(checks) == 44, (
            f"Silver E3 expected 44 checks, got {len(checks)}: "
            f"{[c.name for c in checks]}"
        )

    def test_silver_whoscored_events_spadl_count(self):
        """whoscored_events_spadl has 9 checks. Some have custom names
        (``spadl_coverage_unknown_rate``, ``spadl_action_enum_violation``,
        ``schema_version_literal_drift``) so we count by the params['table']
        attribute rather than by name substring.
        """
        checks = e3_dq.build_silver_e3_checks()
        spadl = [
            c for c in checks
            if c.params.get("table") == "iceberg.silver.whoscored_events_spadl"
        ]
        assert len(spadl) == 9

    def test_silver_espn_lineup_count(self):
        """espn_lineup has 4 checks (PK + nulls + row_count + freshness)."""
        checks = e3_dq.build_silver_e3_checks()
        espn = [
            c for c in checks
            if c.params.get("table") == "iceberg.silver.espn_lineup"
        ]
        assert len(espn) == 4

    def test_gold_e3_total_check_count(self):
        """Gold builders total: fct_event (11) + fct_shot (7) + fct_lineup (8) = 26.

        fct_event grew by 1 check in Task 2.1 — Phase B re-enabled the
        ``ref_integrity[fct_event.match_id_canonical -> silver.xref_match]``
        gate that was disabled during the v0_unbridged interim.
        """
        checks = e3_dq.build_gold_e3_checks()
        assert len(checks) == 26, (
            f"Gold E3 expected 26 checks, got {len(checks)}: "
            f"{[c.name for c in checks]}"
        )

    def test_gold_fct_event_count(self):
        """Gold fct_event has 11 checks: 10 standard + ref_integrity (Phase B).
        Some custom-named ones address ``orphan_team_rate``,
        ``orphan_player_rate_non_meta``, ``schema_version_literal_drift[fct_event]``.
        Match by table OR child fragment (ref_integrity uses 'child' param).
        """
        checks = e3_dq.build_gold_e3_checks()
        fct_event = [
            c for c in checks
            if c.params.get("table") == "iceberg.gold.fct_event"
            or c.params.get("child") == "gold.fct_event"
        ]
        assert len(fct_event) == 11

    def test_gold_fct_shot_count(self):
        """fct_shot has 7 checks: no_duplicates, no_nulls, ref_integrity (uses
        'child' param, not 'table'), value_range[xg], row_count,
        shot_orphan_player_rate, freshness. Match by table OR child fragment."""
        checks = e3_dq.build_gold_e3_checks()
        fct_shot = [
            c for c in checks
            if c.params.get("table") == "iceberg.gold.fct_shot"
            or c.params.get("child") == "gold.fct_shot"
        ]
        assert len(fct_shot) == 7

    def test_gold_fct_lineup_count(self):
        checks = e3_dq.build_gold_e3_checks()
        fct_lineup = [
            c for c in checks
            if c.params.get("table") == "iceberg.gold.fct_lineup"
            or c.params.get("child") == "gold.fct_lineup"
        ]
        assert len(fct_lineup) == 8

    def test_build_all_e3_checks_total(self):
        """44 silver + 26 gold = 70 total E3 standard DQ checks.

        Bump when either ``build_silver_e3_checks`` (44) or
        ``build_gold_e3_checks`` (26) gains a builder.
        """
        all_checks = e3_dq.build_all_e3_checks()
        assert len(all_checks) == 70


# ===========================================================================
# SPADL_ACTION_ENUM contract
# ===========================================================================


class TestSpadlActionEnum:
    """The 24-value enum is the contract between Silver SQL and DQ."""

    def test_enum_size_is_24(self):
        assert len(e3_dq.SPADL_ACTION_ENUM) == 24

    def test_enum_includes_all_canonicals(self):
        expected = {
            "pass", "cross", "throw_in",
            "corner_crossed", "corner_short",
            "freekick_crossed", "freekick_short",
            "take_on", "foul", "tackle", "interception",
            "shot", "shot_penalty", "shot_freekick",
            "keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up",
            "clearance", "bad_touch", "dribble", "goalkick",
            "ball_recovery", "unknown",
        }
        assert set(e3_dq.SPADL_ACTION_ENUM) == expected

    def test_enum_values_are_lowercase_snake(self):
        """Defensive — no surprise capitalisation that would break the
        SQL CASE branches downstream.
        """
        import re
        for v in e3_dq.SPADL_ACTION_ENUM:
            assert re.fullmatch(r"[a-z][a-z_]*", v), (
                f"non-snake-case enum value: {v!r}"
            )

    def test_enum_no_duplicates(self):
        assert len(e3_dq.SPADL_ACTION_ENUM) == len(set(e3_dq.SPADL_ACTION_ENUM))

    def test_proprietary_supplement_present(self):
        """ball_recovery is the single non-SPADL proprietary value."""
        assert "ball_recovery" in e3_dq.SPADL_ACTION_ENUM

    def test_unknown_sentinel_present(self):
        """'unknown' sentinel is required for meta-events."""
        assert "unknown" in e3_dq.SPADL_ACTION_ENUM


# ===========================================================================
# Schema-version literals
# ===========================================================================


class TestSchemaVersionLiterals:
    """The literals are pinned by R0.4 contract."""

    def test_action_source_literal(self):
        assert e3_dq.SPADL_ACTION_SOURCE == "whoscored_spadl_proprietary_v1"

    def test_action_version_literal(self):
        assert e3_dq.SPADL_ACTION_VERSION == "v1"


# ===========================================================================
# Severity / criticality checks
# ===========================================================================


class TestErrorSeverityChecks:
    """Critical invariants must be ERROR severity (raise AirflowException)."""

    def test_silver_pk_uniqueness_is_error(self):
        checks = e3_dq.build_silver_e3_checks()
        pk = next(
            c for c in checks
            if c.kind == "no_duplicates"
            and c.params.get("table") == "iceberg.silver.whoscored_events_spadl"
        )
        assert pk.severity == "ERROR"

    def test_silver_no_nulls_is_error(self):
        checks = e3_dq.build_silver_e3_checks()
        nn = next(
            c for c in checks
            if c.kind == "no_nulls"
            and c.params.get("table") == "iceberg.silver.whoscored_events_spadl"
        )
        assert nn.severity == "ERROR"

    def test_spadl_enum_violation_is_error(self):
        """Enum-violation guard must be ERROR — it's the contract gate."""
        checks = e3_dq.build_silver_e3_checks()
        enum_violations = [
            c for c in checks if c.name == "spadl_action_enum_violation"
        ]
        assert len(enum_violations) == 1
        assert enum_violations[0].severity == "ERROR"

    def test_silver_schema_version_drift_is_error(self):
        checks = e3_dq.build_silver_e3_checks()
        drift = [c for c in checks if c.name == "schema_version_literal_drift"]
        assert len(drift) == 1
        assert drift[0].severity == "ERROR"

    def test_gold_fct_event_schema_drift_is_error(self):
        checks = e3_dq.build_gold_e3_checks()
        drift = [
            c for c in checks
            if c.name == "schema_version_literal_drift[fct_event]"
        ]
        assert len(drift) == 1
        assert drift[0].severity == "ERROR"

    def test_fct_shot_ref_integrity_is_error(self):
        """fct_shot uses INNER JOIN bridge — orphans = upstream regression."""
        checks = e3_dq.build_gold_e3_checks()
        ref = [
            c for c in checks
            if c.kind == "ref_integrity"
            and c.params.get("child") == "gold.fct_shot"
        ]
        assert len(ref) == 1
        assert ref[0].severity == "ERROR"

    def test_fct_event_ref_integrity_is_error(self):
        """fct_event → xref_match bridging is COMPLETE (#40): every WhoScored
        game resolves to a canonical_id row, so an orphan = a regression in
        the schedule⊇events invariant. Re-enabled at ERROR (was WARNING)."""
        checks = e3_dq.build_gold_e3_checks()
        ref = [
            c for c in checks
            if c.kind == "ref_integrity"
            and c.params.get("child") == "gold.fct_event"
            and c.params.get("parent") == "silver.xref_match"
        ]
        assert len(ref) == 1
        assert ref[0].severity == "ERROR"

    def test_fct_lineup_ref_integrity_is_warning_until_e15_cutover(self):
        """fct_lineup ref_integrity → silver.xref_match is WARNING (not ERROR)
        until E1.5 cutover bridges fct_event.match_id_canonical from
        'whoscored_raw' v0_unbridged to canonical IDs (E3 postmortem)."""
        checks = e3_dq.build_gold_e3_checks()
        ref = [
            c for c in checks
            if c.kind == "ref_integrity"
            and c.params.get("child") == "gold.fct_lineup"
        ]
        assert len(ref) == 1
        assert ref[0].severity == "WARNING"

    def test_fct_lineup_fbref_coverage_dominant_is_error(self):
        checks = e3_dq.build_gold_e3_checks()
        coverage = [c for c in checks if c.name == "fbref_coverage_dominant"]
        assert len(coverage) == 1
        assert coverage[0].severity == "ERROR"

    def test_xg_value_range_present_with_zero_one_bounds(self):
        """xG bounds [0, 1] — out-of-bounds = upstream model regression.

        Severity is value_range default (WARNING) in the current builder; if
        it is ever escalated to ERROR per ML-poisoning concerns, update this
        test to match. We DO assert the [0, 1] bounds because those numbers
        are the load-bearing ones. xa is not materialized in fct_shot —
        Understat assist xG lives on fct_player_match instead.
        """
        checks = e3_dq.build_gold_e3_checks()
        xg_check = next(
            c for c in checks
            if c.kind == "value_range"
            and c.params.get("table") == "iceberg.gold.fct_shot"
            and c.params.get("column") == "xg"
        )
        assert xg_check.params["min"] == 0
        assert xg_check.params["max"] == 1


class TestWarningSeverityChecks:
    """Coverage / boundary checks intentionally degrade to WARNING."""

    def test_pitch_coords_are_warning(self):
        """Opta x/y boundary quirks (100.1, -0.1) are WARNING only."""
        checks = e3_dq.build_silver_e3_checks()
        x_range = next(
            c for c in checks
            if c.kind == "value_range"
            and c.params.get("table") == "iceberg.silver.whoscored_events_spadl"
            and c.params.get("column") == "x"
        )
        assert x_range.severity == "WARNING"

    # Understat team aggregates intentionally run freshness at ERROR: Understat
    # is the xG-primary source (RX2, ~99% coverage), so a stale understat feed
    # poisons every downstream xG feature/prediction — worth failing the DAG.
    # Every other E3 table keeps freshness at WARNING (single missed run is OK).
    _ERROR_FRESHNESS_TABLES = {
        "iceberg.silver.understat_team_match",
        "iceberg.silver.understat_team_season",
    }

    def test_freshness_is_warning(self):
        """Freshness is WARNING for every E3 table except the Understat
        team aggregates (those are ERROR — see _ERROR_FRESHNESS_TABLES)."""
        for c in e3_dq.build_all_e3_checks():
            if c.kind == "freshness":
                if c.params.get("table") in self._ERROR_FRESHNESS_TABLES:
                    continue
                assert c.severity == "WARNING", (
                    f"freshness should be WARNING, got {c.severity} on {c.name}"
                )

    def test_understat_freshness_is_error(self):
        """Guard the other direction: Understat (xG-primary) freshness MUST
        stay ERROR so a stale feed fails the DAG instead of silently poisoning
        downstream xG features."""
        checks = e3_dq.build_all_e3_checks()
        understat_fresh = [
            c for c in checks
            if c.kind == "freshness"
            and c.params.get("table") in self._ERROR_FRESHNESS_TABLES
        ]
        assert len(understat_fresh) == 2, (
            f"expected 2 Understat freshness checks, got {len(understat_fresh)}"
        )
        for c in understat_fresh:
            assert c.severity == "ERROR", (
                f"Understat freshness must be ERROR, got {c.severity} on {c.name}"
            )


# ===========================================================================
# Check kind distribution
# ===========================================================================


class TestCheckKindDistribution:
    """Each table touches expected check kinds — defence-in-depth."""

    def test_silver_spadl_has_all_required_kinds(self):
        checks = [
            c for c in e3_dq.build_silver_e3_checks()
            if c.params.get("table") == "iceberg.silver.whoscored_events_spadl"
        ]
        kinds = {c.kind for c in checks}
        assert "no_duplicates" in kinds
        assert "no_nulls" in kinds
        assert "row_count" in kinds
        assert "value_range" in kinds
        assert "freshness" in kinds

    def test_fct_shot_has_ref_integrity(self):
        checks = [
            c for c in e3_dq.build_gold_e3_checks()
            if c.params.get("table") == "iceberg.gold.fct_shot"
            or c.params.get("child") == "gold.fct_shot"
        ]
        kinds = {c.kind for c in checks}
        assert "ref_integrity" in kinds

    def test_fct_lineup_has_ref_integrity(self):
        checks = [
            c for c in e3_dq.build_gold_e3_checks()
            if c.params.get("table") == "iceberg.gold.fct_lineup"
            or c.params.get("child") == "gold.fct_lineup"
        ]
        kinds = {c.kind for c in checks}
        assert "ref_integrity" in kinds


# ===========================================================================
# Check naming uniqueness
# ===========================================================================


class TestCheckNameUniqueness:
    """The DQ runner / Telegram formatter relies on unique names."""

    def test_silver_e3_unique_names(self):
        names = [c.name for c in e3_dq.build_silver_e3_checks()]
        dups = [n for n in names if names.count(n) > 1]
        assert not dups, f"duplicate Silver check names: {sorted(set(dups))}"

    def test_gold_e3_unique_names(self):
        names = [c.name for c in e3_dq.build_gold_e3_checks()]
        dups = [n for n in names if names.count(n) > 1]
        assert not dups, f"duplicate Gold check names: {sorted(set(dups))}"

    def test_all_e3_unique_names(self):
        """No name collisions across silver+gold combined."""
        names = [c.name for c in e3_dq.build_all_e3_checks()]
        assert len(names) == len(set(names)), (
            f"duplicate names across silver+gold: "
            f"{sorted({n for n in names if names.count(n) > 1})}"
        )


# ===========================================================================
# Enum-violation predicate
# ===========================================================================


class TestEnumViolationPredicate:
    """The NOT-IN predicate must contain every enum value, single-quoted."""

    def test_predicate_contains_all_24_values(self):
        checks = e3_dq.build_silver_e3_checks()
        enum_check = next(
            c for c in checks if c.name == "spadl_action_enum_violation"
        )
        where = enum_check.params["where"]
        for v in e3_dq.SPADL_ACTION_ENUM:
            assert f"'{v}'" in where, (
                f"enum value {v!r} missing from NOT IN predicate"
            )
        assert "NOT IN" in where

    def test_predicate_rejects_unsafe_values(self):
        """Internal helper guards against SQL injection in enum values."""
        with pytest.raises(ValueError):
            e3_dq._enum_violation_where("col", ["evil'); DROP TABLE x; --"])

    def test_predicate_rejects_double_dash(self):
        with pytest.raises(ValueError):
            e3_dq._enum_violation_where("col", ["a--b"])


# ===========================================================================
# Orphan-rate / coverage threshold checks
# ===========================================================================


class TestOrphanAndCoverageGuards:
    """Specific named guards must exist with the documented thresholds."""

    def test_orphan_team_rate_present(self):
        checks = e3_dq.build_gold_e3_checks()
        orphan = [c for c in checks if c.name == "orphan_team_rate"]
        assert len(orphan) == 1
        assert orphan[0].severity == "WARNING"

    def test_orphan_player_rate_non_meta_present(self):
        checks = e3_dq.build_gold_e3_checks()
        orphan = [c for c in checks if c.name == "orphan_player_rate_non_meta"]
        assert len(orphan) == 1
        # WARNING because the 4.89% baseline is expected post-resolver
        assert orphan[0].severity == "WARNING"
        # The where clause excludes 'unmappable' confidence events.
        assert "unmappable" in orphan[0].params["where"]

    def test_shot_orphan_player_rate_present(self):
        checks = e3_dq.build_gold_e3_checks()
        sh = [c for c in checks if c.name == "shot_orphan_player_rate"]
        assert len(sh) == 1

    def test_lineup_orphan_player_rate_present(self):
        checks = e3_dq.build_gold_e3_checks()
        ln = [c for c in checks if c.name == "lineup_orphan_player_rate"]
        assert len(ln) == 1

    def test_spadl_unknown_rate_capped(self):
        """spadl_coverage_unknown_rate must cap at 40K rows (R3 baseline 17.5K
        with headroom for 5-season backfill)."""
        checks = e3_dq.build_silver_e3_checks()
        unk = next(c for c in checks if c.name == "spadl_coverage_unknown_rate")
        assert unk.severity == "ERROR"
        assert unk.params["max_rows"] == 40_000


# ===========================================================================
# Parity check
# ===========================================================================


class TestParityCheckExportedAndShape:
    """The parity check is the sole 'CheckResult' (not Check) export."""

    def test_parity_function_exported(self):
        assert callable(e3_dq.parity_check_event_counts)

    def test_append_helper_exported(self):
        assert callable(e3_dq.append_parity_check_to_report)

    def test_module_exposes_public_api(self):
        """__all__ pins the public surface."""
        assert "build_silver_e3_checks" in e3_dq.__all__
        assert "build_gold_e3_checks" in e3_dq.__all__
        assert "build_all_e3_checks" in e3_dq.__all__
        assert "parity_check_event_counts" in e3_dq.__all__
        assert "SPADL_ACTION_ENUM" in e3_dq.__all__
        assert "SPADL_ACTION_SOURCE" in e3_dq.__all__
        assert "SPADL_ACTION_VERSION" in e3_dq.__all__
