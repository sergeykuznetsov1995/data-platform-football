from unittest.mock import patch

import pytest

from scripts import backfill_sofascore


@pytest.mark.unit
def test_backfill_runs_season_then_matches_through_same_runner(tmp_path):
    calls = []

    def run(argv):
        calls.append(argv)
        return 0

    with patch.object(backfill_sofascore, "run_capture", side_effect=run):
        rc = backfill_sofascore.main(
            [
                "--league",
                "ENG-Premier League",
                "--seasons",
                "2014,2015",
                "--output-dir",
                str(tmp_path),
                "--offline-replay",
                "--raw-store-uri",
                f"file://{tmp_path / 'raw'}",
            ]
        )

    assert rc == 0
    assert [call[call.index("--entity") + 1] for call in calls] == [
        "all",
        "match_capture",
        "all",
        "match_capture",
    ]
    assert all("--offline-replay" in call for call in calls)
    assert all("--raw-store-uri" in call for call in calls)


@pytest.mark.unit
def test_backfill_stops_before_match_capture_when_season_fails(tmp_path):
    with patch.object(backfill_sofascore, "run_capture", return_value=1) as run:
        rc = backfill_sofascore.main(
            ["--seasons", "2014", "--output-dir", str(tmp_path)]
        )

    assert rc == 1
    assert run.call_count == 1
    assert "all" in run.call_args.args[0]


@pytest.mark.unit
def test_backfill_passes_named_registry_season_without_numeric_coercion(tmp_path):
    with patch.object(backfill_sofascore, "run_capture", return_value=0) as run:
        rc = backfill_sofascore.main(
            [
                "--seasons",
                "Apertura 2025",
                "--output-dir",
                str(tmp_path),
            ]
        )

    assert rc == 0
    first = run.call_args_list[0].args[0]
    assert first[first.index("--season") + 1] == "Apertura 2025"
    assert "--allow-inactive-season" in first
    assert first[first.index("--output") + 1].endswith(
        "Apertura-2025-season.json"
    )
