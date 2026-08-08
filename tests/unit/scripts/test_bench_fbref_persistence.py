"""Contract coverage for the offline FBref persistence benchmark."""

import pytest

from scripts.research.bench_fbref_persistence import evaluate_gate


@pytest.mark.unit
def test_gate_fails_for_slow_or_non_equivalent_candidate():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=30.0,
        matches=2,
        equivalent=False,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.speedup == pytest.approx(80.0 / 30.0)
    assert report.passed is False


@pytest.mark.unit
def test_gate_requires_zero_network_and_four_x_speedup():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=16.0,
        matches=2,
        equivalent=True,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.seconds_per_match == pytest.approx(8.0)
    assert report.passed is True
