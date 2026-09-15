from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest

from utils.fotmob_orchestration import (
    BACKGROUND_DEADLINE,
    BACKGROUND_HOLD_START,
    BACKGROUND_TAIL_ALLOWANCE,
    DAILY_DEADLINE,
    DAILY_WINDOW_END,
    DAILY_WINDOW_START,
    FAILURE_BACKOFF,
    MIN_BACKGROUND_RUNWAY,
    MIN_DAILY_RUNWAY,
    FotMobLane,
    FotMobSchedulerState,
    advance_after_success,
    build_child_conf,
    choose_lane,
)


UTC = timezone.utc


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 8, hour, minute, tzinfo=UTC)


def _on(moment: time, shift: timedelta = timedelta(0)) -> datetime:
    """Границу берём из константы, а не из литерала: окна двигаются вместе."""

    return datetime(2026, 8, 8, moment.hour, moment.minute, tzinfo=UTC) + shift


def _just_before_hold() -> datetime:
    return _on(BACKGROUND_HOLD_START, -timedelta(minutes=1))


def _at_hold() -> datetime:
    return _on(BACKGROUND_HOLD_START)


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
        (_just_before_hold(), FotMobLane.REFRESH),
        (_at_hold(), None),
        (_on(DAILY_WINDOW_START, -timedelta(minutes=1)), None),
        (_on(DAILY_WINDOW_START), FotMobLane.DAILY),
        (_on(DAILY_WINDOW_END, -timedelta(minutes=1)), FotMobLane.DAILY),
        (_on(DAILY_WINDOW_END), None),
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
    # Чередование курсора живёт внутри суток, где refresh уже сделан: до него полосу
    # выбирает суточная гарантия, а не курсор.
    state = _state()
    assert choose_lane(_at(8), state, child_running=False).lane is FotMobLane.REFRESH

    state = advance_after_success(state, FotMobLane.REFRESH, _at(8, 30))
    restored = FotMobSchedulerState.from_dict(state.to_dict())
    assert restored == state
    assert (
        choose_lane(
            _at(9), restored, child_running=False, refresh_done_today=True
        ).lane
        is FotMobLane.BACKFILL
    )

    state = advance_after_success(restored, FotMobLane.BACKFILL, _at(10))
    assert (
        choose_lane(_at(11), state, child_running=False, refresh_done_today=True).lane
        is FotMobLane.REFRESH
    )


def test_failed_child_repeats_lane_because_state_is_not_advanced():
    state = _state(FotMobLane.BACKFILL)
    # Курсор двигает только успех, поэтому повтор обязан вернуть ту же полосу.
    # Суточную гарантию refresh здесь закрываем явно, иначе полосу выбирает она.
    first = choose_lane(
        _at(8), state, child_running=False, refresh_done_today=True
    )
    second = choose_lane(
        _at(9), state, child_running=False, refresh_done_today=True
    )
    assert first.lane is second.lane is FotMobLane.BACKFILL
    assert state.next_background_lane is FotMobLane.BACKFILL
    assert state.generation == 7


def test_scheduler_state_shape_is_pinned_because_rollback_reads_it_back():
    """Форма Variable fotmob.scheduler.state.v1 — контракт отката пином.

    Автомат доставки откатывает дерево ``git checkout -f`` и не трогает Variable,
    а четыре внешних валидатора (deploy/fotmob/deploy.py, scripts/fotmob_runtime.py,
    scripts/fotmob_recover.py) сверяют множество ключей на равенство. Пятый ключ
    сделал бы откат невозможным: старый код отверг бы состояние, и планировщик
    краснел бы каждые пять минут.
    """
    expected = {
        "next_background_lane",
        "daily_date",
        "generation",
        "updated_at",
    }
    payload = FotMobSchedulerState.initial().to_dict()
    assert set(payload) == expected
    assert FotMobSchedulerState.from_dict(payload) == (
        FotMobSchedulerState.initial()
    )
    with pytest.raises(ValueError, match="unexpected shape"):
        FotMobSchedulerState.from_dict({**payload, "refresh_done_today": True})


def test_refresh_is_guaranteed_every_day_regardless_of_the_cursor():
    """Замер 08-15.09: refresh не заводился в 3 сутках из 8 — курсор стоял на backfill."""

    state = _state(FotMobLane.BACKFILL)
    owed = choose_lane(
        _at(0), state, child_running=False, refresh_done_today=False
    )
    assert owed.lane is FotMobLane.REFRESH
    assert owed.reason == "refresh_daily_guarantee"

    settled = choose_lane(
        _at(0), state, child_running=False, refresh_done_today=True
    )
    assert settled.lane is FotMobLane.BACKFILL
    assert settled.reason == "background_fair_turn"


def test_red_refresh_is_retried_after_the_backoff_because_the_day_still_owes_one():
    state = _state(FotMobLane.BACKFILL)
    failed_at = _at(1, 0)
    assert (
        choose_lane(
            failed_at + FAILURE_BACKOFF,
            state,
            child_running=False,
            refresh_done_today=False,
            last_failure_ended_at=failed_at,
        ).lane
        is FotMobLane.REFRESH
    )


def test_successful_refresh_hands_the_rest_of_the_day_to_history():
    state = _state(FotMobLane.REFRESH)
    advanced = advance_after_success(state, FotMobLane.REFRESH, _at(2))
    assert advanced.next_background_lane is FotMobLane.BACKFILL
    assert (
        choose_lane(
            _at(3), advanced, child_running=False, refresh_done_today=True
        ).lane
        is FotMobLane.BACKFILL
    )


@pytest.mark.parametrize(
    "now",
    [_at(14, 45), _on(DAILY_WINDOW_END, -timedelta(minutes=1))],
)
def test_daily_starts_even_after_a_long_background_tail(now):
    """Замер 08-15.09: фон кончался в 14:03-14:23, а окно старта было 14:00-15:00."""

    decision = choose_lane(now, _state(), child_running=False)
    assert decision.lane is FotMobLane.DAILY
    assert decision.reason == "daily_window"


def test_daily_start_window_closes_before_the_nightly_delivery_window():
    closed = choose_lane(_on(DAILY_WINDOW_END), _state(), child_running=False)
    assert closed.lane is None
    assert closed.reason == "background_window_closed"


def test_daily_is_not_repeated_after_its_success_in_the_same_day():
    done = _state(daily_date=date(2026, 8, 8))
    decision = choose_lane(_at(16), done, child_running=False)
    assert decision.lane is None
    assert decision.reason == "daily_already_completed"


def test_red_wave_pauses_every_lane_for_the_failure_backoff():
    """До #1282 следующая волна стартовала через 17-70 с после красной."""

    state = _state()
    failed_at = _at(0, 30)
    paused = choose_lane(
        failed_at + timedelta(minutes=1),
        state,
        child_running=False,
        last_failure_ended_at=failed_at,
    )
    assert paused.lane is None
    assert paused.reason == "failure_backoff"
    assert (
        choose_lane(
            failed_at + FAILURE_BACKOFF,
            state,
            child_running=False,
            last_failure_ended_at=failed_at,
        ).lane
        is FotMobLane.REFRESH
    )


def test_red_daily_is_retried_in_the_same_day_after_the_backoff():
    state = _state()
    failed_at = _at(15, 0)
    assert (
        choose_lane(
            failed_at + FAILURE_BACKOFF,
            state,
            child_running=False,
            last_failure_ended_at=failed_at,
        ).lane
        is FotMobLane.DAILY
    )


def test_background_cannot_outlive_its_deadline():
    """Жёсткий стоп фона — это дедлайн и разбег, а не убийство живой волны."""

    tail = datetime.combine(date(2026, 8, 8), BACKGROUND_DEADLINE, tzinfo=UTC)
    assert (tail + BACKGROUND_TAIL_ALLOWANCE).time() == DAILY_WINDOW_START
    assert _at_hold() + MIN_BACKGROUND_RUNWAY == tail

    conf = build_child_conf(FotMobLane.REFRESH, _just_before_hold())
    assert datetime.fromisoformat(conf["deadline"]) <= datetime.combine(
        date(2026, 8, 8), DAILY_WINDOW_START, tzinfo=UTC
    )

    assert choose_lane(_at_hold(), _state(), child_running=False).lane is None
    with pytest.raises(ValueError):
        build_child_conf(FotMobLane.REFRESH, _at_hold())
    assert (
        choose_lane(_just_before_hold(), _state(), child_running=False).lane
        is FotMobLane.REFRESH
    )
    assert build_child_conf(FotMobLane.REFRESH, _just_before_hold())["deadline"]


def test_late_daily_still_has_runway_before_its_deadline():
    latest_start = datetime.combine(
        date(2026, 8, 8), DAILY_WINDOW_END, tzinfo=UTC
    ) - timedelta(minutes=1)
    deadline = datetime.combine(date(2026, 8, 8), DAILY_DEADLINE, tzinfo=UTC)
    assert latest_start + MIN_DAILY_RUNWAY <= deadline


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
    expected = DAILY_DEADLINE if lane is FotMobLane.DAILY else BACKGROUND_DEADLINE
    assert conf["deadline"] == datetime.combine(
        date(2026, 8, 8), expected, tzinfo=UTC
    ).isoformat()


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


def test_hard_timeout_outlasts_every_lane_window():
    """Жёсткий таймаут обязан пережить окно полосы, иначе дедлайн недостижим.

    Регрессия 2026-08-12: при 8 часах фоновая волна, стартующая в 00:00, умирала по
    AirflowTaskTimeout в 08:00 — за 5 ч 45 мин до собственного дедлайна 13:45. Цена не
    только в потерянном окне: SIGTERM вместо мягкой отсрочки красит ран, поэтому
    публикация не закрывается, silver не триггерится, а advance_after_success не
    выполняется — курсор полос стоит и BACKFILL не наступает никогда.
    """
    from utils.fotmob_orchestration import (
        BACKGROUND_DEADLINE,
        CHILD_TIMEOUT_MINUTES,
        DAILY_DEADLINE,
        DAILY_WINDOW_START,
    )

    def _minutes(start, end) -> int:
        return (end.hour - start.hour) * 60 + (end.minute - start.minute)

    windows = {
        # фоновая полоса допускается к старту с полуночи, дневная — со своего окна
        FotMobLane.REFRESH: _minutes(time(0, 0), BACKGROUND_DEADLINE),
        FotMobLane.BACKFILL: _minutes(time(0, 0), BACKGROUND_DEADLINE),
        FotMobLane.DAILY: _minutes(DAILY_WINDOW_START, DAILY_DEADLINE),
    }
    for lane, window in windows.items():
        assert CHILD_TIMEOUT_MINUTES > window, (
            f"полоса {lane.value}: жёсткий таймаут {CHILD_TIMEOUT_MINUTES} мин не "
            f"переживает окно {window} мин — дедлайн недостижим, ран умрёт красным"
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


@pytest.mark.parametrize(
    "now", [_at_hold(), _on(DAILY_WINDOW_END, timedelta(minutes=1)), _at(23, 59)]
)
def test_background_conf_fails_closed_after_daily_cutoff(now):
    cutoff = BACKGROUND_HOLD_START.strftime("%H:%M")
    with pytest.raises(ValueError, match=f"{cutoff} UTC cutoff"):
        build_child_conf(FotMobLane.REFRESH, now)
