"""Runtime admission and the source-specific Airflow plugin recipe."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from dags.utils.transfermarkt_current_timetable import (
    IDLE_POLL_SECONDS,
    available_work_seconds,
    next_admitted_time,
    remaining_work_seconds,
    work_deadline,
)


def _at(clock: str) -> datetime:
    return datetime.fromisoformat(f"2026-10-09T{clock}+00:00")


@pytest.mark.parametrize("clock", ["00:15:00", "00:59:59", "01:00:00", "02:59:59"])
def test_quiet_window_admission_never_sleeps(clock: str) -> None:
    now = _at(clock)
    assert available_work_seconds(now) == 0
    assert work_deadline(now) == now
    assert next_admitted_time(now) == _at("03:00:00")


@pytest.mark.parametrize("clock", ["00:00:00", "00:14:59", "03:00:00", "23:59:59"])
def test_full_runtime_budget_outside_admission_window(clock: str) -> None:
    now = _at(clock)
    assert next_admitted_time(now) == now
    assert available_work_seconds(now) == 2700
    assert work_deadline(now) == now + timedelta(minutes=45)


def test_deadline_bounds_late_start_retries_and_delivery() -> None:
    # A queued portion arriving after cutoff does no work; already admitted work
    # can finish before delivery, including its commit.
    deadline = work_deadline(_at("00:14:59"))
    assert deadline < _at("01:00:00")
    assert remaining_work_seconds(deadline, _at("00:30:00")) == 1799
    assert remaining_work_seconds(deadline, deadline) == 0
    assert remaining_work_seconds(_at("04:00:00"), _at("01:00:00")) == 0
    assert remaining_work_seconds(deadline, _at("03:00:00")) == 0
    assert available_work_seconds(_at("00:14:59"), 30) == 30


@pytest.mark.parametrize(('last_end', 'admitted'), [
    ('2026-10-09T00:10:00+00:00', '2026-10-09T03:00:00+00:00'),
    ('2026-10-09T03:00:00+00:00', '2026-10-09T03:05:00+00:00'),
    ('2026-10-09T23:59:00+00:00', '2026-10-10T00:04:00+00:00'),
])
def test_idle_poll_candidate_obeys_delivery_and_midnight(last_end, admitted) -> None:
    candidate = datetime.fromisoformat(last_end) + timedelta(seconds=IDLE_POLL_SECONDS)
    assert next_admitted_time(candidate) == datetime.fromisoformat(admitted)


def test_timezone_normalization_and_naive_rejection() -> None:
    moscow = timezone(timedelta(hours=3))
    assert available_work_seconds(_at("00:15:00").astimezone(moscow)) == 0
    assert work_deadline(_at("03:00:00").astimezone(moscow)).tzinfo == timezone.utc
    naive = datetime(2026, 10, 9)
    for call in (next_admitted_time, work_deadline, available_work_seconds):
        with pytest.raises(ValueError, match="timezone aware"):
            call(naive)


@pytest.mark.parametrize("budget", [0, -1, 2701, float("inf"), float("nan")])
def test_invalid_runtime_budget_fails_closed(budget: float) -> None:
    with pytest.raises(ValueError, match="portion budget"):
        available_work_seconds(_at("03:00:00"), budget)


def test_source_plugin_recipe_is_isolated() -> None:
    root = Path(__file__).resolve().parents[3]
    recipe = yaml.safe_load((root / "deploy/transfermarkt/airflow.compose.yaml").read_text())
    for service in ("airflow-scheduler", "airflow-webserver", "airflow-init"):
        assert recipe["services"][service]["environment"]["AIRFLOW__CORE__PLUGINS_FOLDER"] == (
            "/opt/airflow/dags/utils/transfermarkt_plugins"
        )
    plugins = root / "dags/utils/transfermarkt_plugins"
    files = list(plugins.glob("*.py"))
    assert [file.name for file in files] == ["current_timetable.py"]
    assert "whoscored" not in files[0].read_text().lower()
