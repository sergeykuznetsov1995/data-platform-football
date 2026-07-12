import json

import pytest

from scripts.research import bench_sofascore_offline_replay


@pytest.mark.unit
def test_fixed_cohort_replay_and_noop_are_complete_and_network_free(capsys):
    assert bench_sofascore_offline_replay.main() == 0
    report = json.loads(capsys.readouterr().out)

    assert report["budget_eligible"] is False
    assert report["cohort"] == "25_matches_50_players"
    for mode in ("offline_replay", "no_op"):
        metrics = report[mode]
        assert metrics["paid_proxy_bytes"] == 0
        assert metrics["browser_sessions"] == 0
        assert metrics["navigations"] == 0
        assert metrics["request_count"] == 0
        assert metrics["completed_matches"] == 25
        assert metrics["completed_players"] == 50
        assert metrics["endpoint_completeness"] == 1.0
        assert metrics["status_counts"]["schema_error"] == 0
    assert report["offline_replay"]["replay_hit_rate"] == 1.0
    assert report["no_op"]["cache_hit_rate"] == 1.0
