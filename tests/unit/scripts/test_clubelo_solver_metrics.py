"""Offline pytest checks for scripts/research/clubelo_solver_metrics.py."""

import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "research"))

from clubelo_solver_metrics import (
    build_report,
    main,
    percentage_point_change,
    pool_report,
    summarize_failure_census,
)

FIXTURE = ROOT / "tests" / "fixtures" / "clubelo" / "solver_metrics_20261106_12.json"
EXPECTED_DS = {
    "2026-11-06": (6, 6),
    "2026-11-07": (5, 4),
    "2026-11-08": (6, 6),
    "2026-11-09": (7, 7),
    "2026-11-10": (8, 8),
    "2026-11-11": (6, 4),
    "2026-11-12": (4, 2),
}
FUNNEL_NULLS = ("attempts", "tunnel_failed", "validated")


def load_payload():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def report():
    return build_report(load_payload())


def day_payload(**overrides):
    payload = load_payload()
    payload["days"] = [dict(payload["days"][0], **overrides)]
    return payload


def test_report_shape_and_seven_day_counts(report):
    assert report["day_count"] == 7
    assert (report["first_day"], report["last_day"]) == ("2026-11-06", "2026-11-12")
    assert report["population"]["source"] == "clubelo"
    assert report["population"]["counting_unit"] == "challenge_events"
    assert "no attempt/session join" in report["population"]["coverage"]
    assert [day["day_utc"] for day in report["days"]] == sorted(EXPECTED_DS)
    for day in report["days"]:
        detected, solved = EXPECTED_DS[day["day_utc"]]
        assert (day["detected"], day["solved"]) == (detected, solved)
        assert (day["solve_numerator"], day["solve_denominator"]) == (solved, detected)
        assert day["solve_fraction"] == pytest.approx(solved / detected)
        for field in FUNNEL_NULLS:
            assert day[field] is None
        assert day["validated_over_attempts"] is None
        assert day["tunnel_failed_over_attempts"] is None
        assert day["status"] == "partial"


def test_reported_percent_rounding(report):
    nov11, nov12 = report["days"][-2], report["days"][-1]
    assert nov11["solve_fraction"] == pytest.approx(4 / 6)
    assert (nov11["solve_percent_precise"], nov11["solve_percent"]) == (66.67, 67)
    assert nov12["solve_fraction"] == pytest.approx(0.5)
    assert (nov12["solve_percent_precise"], nov12["solve_percent"]) == (50.0, 50)


def test_pooling_sums_counts_not_fractions(report):
    early = pool_report(report, "2026-11-09", "2026-11-10")
    late = pool_report(report, "2026-11-11", "2026-11-12")
    assert (early["detected"], early["solved"]) == (15, 15)
    assert (early["solve_fraction"], early["solve_percent"]) == (1.0, 100)
    assert (late["detected"], late["solved"]) == (10, 6)
    assert late["solve_fraction"] == pytest.approx(0.6)
    assert late["solve_percent"] == 60
    assert late["attempts"] is None and late["status"] == "partial"
    assert percentage_point_change(late, early) == -40
    two_days = pool_report(report, "2026-11-06", "2026-11-07")
    assert two_days["solve_fraction"] == pytest.approx(10 / 11)
    assert two_days["solve_fraction"] != pytest.approx((1.0 + 0.8) / 2)


def test_pool_rejects_gaps_and_reversed_ranges(report):
    payload = load_payload()
    payload["days"] = [payload["days"][0]] + payload["days"][2:4]
    gappy = build_report(payload)
    with pytest.raises(ValueError, match="missing"):
        pool_report(gappy, "2026-11-06", "2026-11-08")
    with pytest.raises(ValueError, match="not be after"):
        pool_report(report, "2026-11-12", "2026-11-09")


def test_failure_census_on_fixture(report):
    census = report["failure_census"]
    assert "total_failed_runs" not in census
    assert census["total_failed_run_attempts"] == 21
    assert census["counts"] == {"solver_timeout": 10, "tunnel": 8, "unknown": 3}
    timeout = census["categories"]["solver_timeout"]
    assert timeout["count"] == 10
    assert timeout["fraction"] == pytest.approx(10 / 21)
    assert timeout["percent"] == 48
    assert census["largest_category"] == "solver_timeout"
    assert census["largest_tie"] is False
    assert census["is_majority"] is False
    assert census["excluded_successful_run_invalid_clearance"] == 2


def test_census_tie_majority_and_zero_total():
    tied = summarize_failure_census({"solver_timeout": 5, "tunnel": 5, "unknown": 0})
    assert tied["largest_category"] is None
    assert tied["largest_tie"] is True
    assert tied["is_majority"] is False
    majority = summarize_failure_census({"solver_timeout": 11, "tunnel": 5, "unknown": 5})
    assert majority["largest_category"] == "solver_timeout"
    assert majority["largest_tie"] is False
    assert majority["is_majority"] is True
    zero = summarize_failure_census({"solver_timeout": 0, "tunnel": 0, "unknown": 0})
    assert zero["total_failed_run_attempts"] == 0
    assert zero["largest_category"] is None
    assert zero["largest_tie"] is False
    assert zero["is_majority"] is False
    assert all(cat["fraction"] is None and cat["percent"] is None for cat in zero["categories"].values())


def test_null_versus_explicit_zero_and_known_ratios():
    null_funnel = build_report(day_payload())["days"][0]
    assert null_funnel["status"] == "partial"
    assert null_funnel["validated_over_attempts"] is None
    known = build_report(day_payload(attempts=6, tunnel_failed=2, validated=3))["days"][0]
    assert known["status"] == "complete"
    assert known["validated_over_attempts"] == pytest.approx(0.5)
    assert known["tunnel_failed_over_attempts"] == pytest.approx(1 / 3)
    zeroed = build_report(
        day_payload(detected=0, solved=0, attempts=0, tunnel_failed=0, validated=0)
    )["days"][0]
    assert zeroed["status"] == "complete"
    assert zeroed["solve_fraction"] is None
    assert zeroed["solve_percent_precise"] is None
    assert zeroed["solve_percent"] is None
    assert zeroed["validated_over_attempts"] is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempts": 3, "detected": 4, "solved": 4},
        {"attempts": 2, "tunnel_failed": 3},
        {"attempts": 2, "validated": 3},
        {"attempts": 4, "tunnel_failed": 2, "validated": 3},
    ],
)
def test_complete_population_count_invariants(overrides):
    with pytest.raises(ValueError, match="attempts"):
        build_report(day_payload(**overrides))


def test_validated_not_constrained_to_solved():
    day = build_report(day_payload(validated=50))["days"][0]
    assert (day["validated"], day["solved"]) == (50, 6)


@pytest.mark.parametrize(
    "overrides",
    [
        {"solved": 3, "detected": 2},
        {"detected": -1},
        {"solved": True},
        {"detected": 1.5},
        {"attempts": -2},
        {"validated": False},
        {"day_utc": "2026-02-30"},
        {"day_utc": "2026-11-6"},
    ],
)
def test_day_input_rejections(overrides):
    with pytest.raises(ValueError):
        build_report(day_payload(**overrides))


def test_duplicate_day_rejected():
    payload = load_payload()
    payload["days"].append(dict(payload["days"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        build_report(payload)


def test_population_metadata_validation():
    payload = load_payload()
    payload["population"].pop("source")
    with pytest.raises(ValueError, match="population.source"):
        build_report(payload)
    payload = load_payload()
    payload["population"]["coverage"] = "   "
    with pytest.raises(ValueError, match="population.coverage"):
        build_report(payload)
    payload = load_payload()
    payload["mode"] = "raw_events"
    with pytest.raises(ValueError, match="mode"):
        build_report(payload)


def test_cli_prints_build_report_json_only(tmp_path, capsys):
    source = tmp_path / "input.json"
    source.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    assert main([str(source)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == build_report(load_payload())
    assert captured.err == ""
    assert [entry.name for entry in tmp_path.iterdir()] == ["input.json"]


def test_cli_bad_input_returns_nonzero_without_traceback(tmp_path, capsys):
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema_version": 99}', encoding="utf-8")
    assert main([str(bad)]) == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("error:")
    assert "Traceback" not in captured.err
    assert main([]) == 2


def test_deterministic_ordering_without_input_mutation():
    payload = load_payload()
    payload["days"].reverse()
    snapshot = copy.deepcopy(payload)
    first = build_report(payload)
    assert [day["day_utc"] for day in first["days"]] == sorted(EXPECTED_DS)
    assert payload == snapshot
    assert build_report(payload) == first
