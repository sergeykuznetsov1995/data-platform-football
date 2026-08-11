from __future__ import annotations

from datetime import date, datetime, time, timezone

import pytest

from utils.fotmob_orchestration import (
    FotMobLane,
    FotMobSchedulerState,
    advance_after_success,
    build_child_conf,
    choose_lane,
)


UTC = timezone.utc


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 8, hour, minute, tzinfo=UTC)


def _state(
    next_lane: FotMobLane = FotMobLane.REFRESH,
    *,
    daily_date: date | None = None,
) -> FotMobSchedulerState:
    return FotMobSchedulerState(
        next_background_lane=next_lane,
        daily_date=daily_date,
        generation=7,
        updated_at=_at(0),
    )


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (_at(13, 29), FotMobLane.REFRESH),
        (_at(13, 30), None),
        (_at(13, 59), None),
        (_at(14, 0), FotMobLane.DAILY),
        (_at(14, 59), FotMobLane.DAILY),
        (_at(15, 0), None),
        (_at(23, 59), None),
    ],
)
def test_lane_windows(now, expected):
    assert choose_lane(now, _state(), child_running=False).lane is expected


def test_daily_is_selected_only_once_after_its_success():
    state = _state()
    selected = choose_lane(_at(14, 5), state, child_running=False)
    assert selected.lane is FotMobLane.DAILY

    advanced = advance_after_success(state, selected.lane, _at(14, 40))
    assert advanced.daily_date == date(2026, 8, 8)
    assert choose_lane(_at(14, 45), advanced, child_running=False).lane is None


def test_child_running_never_selects_another_lane():
    assert choose_lane(_at(9), _state(), child_running=True).lane is None
    assert choose_lane(_at(14), _state(), child_running=True).lane is None


def test_successful_background_runs_alternate_and_restart_round_trips():
    state = _state()
    assert choose_lane(_at(8), state, child_running=False).lane is FotMobLane.REFRESH

    state = advance_after_success(state, FotMobLane.REFRESH, _at(8, 30))
    restored = FotMobSchedulerState.from_dict(state.to_dict())
    assert restored == state
    assert choose_lane(_at(9), restored, child_running=False).lane is FotMobLane.BACKFILL

    state = advance_after_success(restored, FotMobLane.BACKFILL, _at(10))
    assert choose_lane(_at(11), state, child_running=False).lane is FotMobLane.REFRESH


def test_failed_child_repeats_lane_because_state_is_not_advanced():
    state = _state(FotMobLane.BACKFILL)
    first = choose_lane(_at(8), state, child_running=False)
    second = choose_lane(_at(9), state, child_running=False)
    assert first.lane is second.lane is FotMobLane.BACKFILL
    assert state.generation == 7


@pytest.mark.parametrize("raw_daily_date", [False, 0, ""])
def test_persisted_daily_date_rejects_non_null_non_date_values(raw_daily_date):
    payload = _state().to_dict()
    payload["daily_date"] = raw_daily_date
    with pytest.raises(ValueError, match="daily date"):
        FotMobSchedulerState.from_dict(payload)


@pytest.mark.parametrize(
    ("lane", "max_requests", "max_direct_mib", "rpm"),
    [
        (FotMobLane.DAILY, 24_000, 1_536, 60),
        (FotMobLane.REFRESH, 27_000, 1_536, 60),
        (FotMobLane.BACKFILL, 20_000, 1_024, 45),
    ],
)
def test_child_conf_uses_dynamic_contract_and_exact_caps(
    lane, max_requests, max_direct_mib, rpm
):
    conf = build_child_conf(lane, _at(9))
    assert conf["mode"] == lane.value
    assert conf["catalog_contract"] == "fotmob-catalog-v1"
    assert conf["max_requests"] == max_requests
    assert conf["max_direct_mib"] == max_direct_mib
    assert conf["requests_per_minute"] == rpm
    assert conf["max_proxy_mib"] == 0
    if lane is FotMobLane.DAILY:
        assert conf["deadline"] == "2026-08-08T21:00:00+00:00"
    else:
        assert conf["deadline"] == "2026-08-08T13:45:00+00:00"


def test_every_lane_gets_a_cooperative_deadline():
    """Полоса без дедлайна останавливается только жёстким execution_timeout.

    Это SIGTERM, status=incomplete и красный ран вместо мягкой отсрочки. Дневная
    полоса жила без дедлайна, пока была прибита к 21 турниру; под автоматическим
    каталогом она обходит весь каталог, и отсутствие дедлайна стало дефектом.
    """
    for lane in FotMobLane:
        now = _at(14) if lane is FotMobLane.DAILY else _at(9)
        deadline = build_child_conf(lane, now)["deadline"]
        assert deadline, f"полоса {lane.value} осталась без кооперативного дедлайна"
        assert datetime.fromisoformat(deadline) > now


def test_lane_request_caps_are_reachable():
    """Недостижимый потолок запросов — декоративный: остановить ран станет нечему.

    Регрессия 2026-08-11: потолок 80_000 при 60 rpm недостижим ни в одном окне
    (за 8 часов физически выдаётся 28_800), поэтому вместо бюджетной отсрочки ран
    убивался по execution_timeout. Потолок обязан срабатывать РАНЬШЕ жёсткого
    таймаута и раньше конца окна полосы.
    """
    from utils.fotmob_orchestration import (
        BACKGROUND_DEADLINE,
        CHILD_TIMEOUT_MINUTES,
        DAILY_DEADLINE,
        DAILY_WINDOW_START,
        _LANE_CAPS,
    )

    def _minutes(start, end) -> int:
        return (end.hour - start.hour) * 60 + (end.minute - start.minute)

    windows = {
        # фоновые полосы стартуют с полуночи, дневная — не раньше своего окна
        FotMobLane.DAILY: _minutes(DAILY_WINDOW_START, DAILY_DEADLINE),
        FotMobLane.REFRESH: _minutes(time(0, 0), BACKGROUND_DEADLINE),
        FotMobLane.BACKFILL: _minutes(time(0, 0), BACKGROUND_DEADLINE),
    }
    for lane, (max_requests, _max_direct_mib, rpm) in _LANE_CAPS.items():
        bound_minutes = min(windows[lane], CHILD_TIMEOUT_MINUTES)
        reachable = rpm * bound_minutes
        assert max_requests < reachable, (
            f"потолок полосы {lane.value} ({max_requests}) недостижим: "
            f"{rpm} rpm × {bound_minutes} мин = {reachable}"
        )


def test_child_timeout_copy_matches_the_ingest_dag():
    """CHILD_TIMEOUT_MINUTES — копия execution_timeout из dag_ingest_fotmob.

    От неё считается достижимость потолков, поэтому расхождение копии с оригиналом
    молча вернёт недостижимые потолки.
    """
    import re
    from pathlib import Path

    from utils.fotmob_orchestration import CHILD_TIMEOUT_MINUTES

    source = (
        Path(__file__).resolve().parents[3] / "dags" / "dag_ingest_fotmob.py"
    ).read_text(encoding="utf-8")
    # между task_id и execution_timeout лежит вся bash-команда скрапера (~40 строк)
    scraper_task = source.split('task_id="scrape_fotmob_data"', 1)[1].split(
        "PythonOperator(", 1
    )[0]
    hours = re.search(r"execution_timeout=timedelta\(hours=(\d+)\)", scraper_task)
    assert hours is not None, "не найден execution_timeout у scrape_fotmob_data"
    assert int(hours.group(1)) * 60 == CHILD_TIMEOUT_MINUTES


@pytest.mark.parametrize("now", [_at(13, 30), _at(15, 1), _at(23, 59)])
def test_background_conf_fails_closed_after_daily_cutoff(now):
    with pytest.raises(ValueError, match="13:30 UTC cutoff"):
        build_child_conf(FotMobLane.REFRESH, now)
