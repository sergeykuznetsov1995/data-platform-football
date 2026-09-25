"""Сторож простоя ESPN (#1496): «DAG на паузе → тревога», «36 ч без новых матчей → тревога»;
#1505: «турнир красный 3 волны подряд → тревога» по журналу волн;
#1506: «downgrade_rejected за 24 ч → тревога раз в сутки» по журналу перепроверок.

Заглушки не отвечают на всё одинаково: SQL сторожа ИСПОЛНЯЕТСЯ — запрос к метабазе в sqlite
над записанной таблицей `dag` (формат `psql -At`: `dag_id|t`), запрос к Trino в duckdb над
строками bronze (формат trino-ro.sh: `"a","b"`). Telegram и gh — записывающие подделки.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "deploy/espn/espn_stall_watch.py"
MODULE_NAME = "espn_stall_watch"


def _load_module():
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


watch = _load_module()

NOW = datetime(2026, 9, 24, 20, 0, tzinfo=timezone.utc)
# Записано 24.09.2026 (премиса-чек #1496): docker exec espn-airflow-airflow-metadb-1
# psql -U airflow -d airflow -At -c "SELECT dag_id, is_paused FROM dag ORDER BY 1"
DAG_TABLE_2409 = """dag_backfill_espn|t
dag_discover_espn_registry|t
dag_ingest_espn|t
dag_monitor_espn|t
dag_repair_espn|t
dag_replay_espn|t
dag_trigger_espn_daily|t
"""
# Записано 24.09.2026: max(_source_fetched_at) espn_lineup_generation_v2 через trino-ro.sh.
LAST_FETCH_2409 = "2026-08-13 11:38:28.616770"
EXPECTED = ("dag_ingest_espn", "dag_trigger_espn_daily", "dag_monitor_espn",
            "dag_discover_espn_registry")


def _ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


class World:
    """Метабаза, bronze, Telegram и gh, которые видит сторож."""

    def __init__(self, dag_table=DAG_TABLE_2409, bronze=None):
        self.dags = [tuple(line.split("|")) for line in dag_table.splitlines() if line]
        self.metadb_up = True
        self.trino_up = True
        self.bronze = bronze if bronze is not None else {
            "espn_lineup_generation_v2": [(401, LAST_FETCH_2409)],
            "espn_matchsheet_generation_v2": [(401, "2026-08-13 10:02:11.000001")],
        }
        # Журнал волн (#1505): None — таблицы ещё нет (до #1507).
        self.wave_log: list[tuple] | None = None
        # Журнал перепроверок (#1506): None — таблицы ещё нет (до #1507).
        self.recheck_log: list[tuple] | None = None
        self.sent: list[str] = []
        self.gh_calls: list[list[str]] = []
        self.open_issues: list[dict] = []
        self.board_failures = 0   # сколько раз подряд смена Status на доске упадёт

    def set_dags(self, **paused):
        self.dags = [(d, paused.get(d, p)) for d, p in self.dags]

    def psql(self, sql, timeout=40):
        if not self.metadb_up:
            return False, ""
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE dag (dag_id TEXT, is_paused TEXT)")
        db.executemany("INSERT INTO dag VALUES (?, ?)", self.dags)
        rows = db.execute(sql).fetchall()
        return True, "".join("|".join(str(c) for c in row) + "\n" for row in rows)

    def trino(self, sql, timeout=240):
        if not self.trino_up:
            return None
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS iceberg")
        con.execute("CREATE SCHEMA iceberg.bronze")
        for table in ("espn_lineup_generation_v2", "espn_matchsheet_generation_v2"):
            con.execute(f"CREATE TABLE iceberg.bronze.{table} "
                        "(event_id BIGINT, _source_fetched_at TIMESTAMP, _ingested_at TIMESTAMP)")
            for event_id, fetched in self.bronze.get(table, []):
                # _ingested_at врёт (= execution_date) и всегда «свежее» — сторож не должен его читать
                con.execute(f"INSERT INTO iceberg.bronze.{table} VALUES (?, ?, ?)",
                            [event_id, fetched, _ts(NOW)])
        if self.wave_log is not None or self.recheck_log is not None:
            con.execute("CREATE SCHEMA iceberg.ops")
        if self.recheck_log is not None:
            con.execute("CREATE TABLE iceberg.ops.espn_recheck_v1 (checked_at TIMESTAMP, "
                        "run_id VARCHAR, slug VARCHAR, season_year INTEGER, event_id BIGINT, "
                        "kind VARCHAR, before_parts VARCHAR, after_parts VARCHAR, outcome VARCHAR)")
            con.executemany("INSERT INTO iceberg.ops.espn_recheck_v1 VALUES "
                            "(?, 'r', ?, 2026, ?, 'recheck', '{}', '{}', ?)", self.recheck_log)
        if self.wave_log is not None:
            con.execute("CREATE TABLE iceberg.ops.espn_wave_tournament_v1 (run_id VARCHAR, "
                        "wave_started_at TIMESTAMP, wave_finished_at TIMESTAMP, slug VARCHAR, "
                        "season_year INTEGER, state VARCHAR, matches INTEGER, first_error VARCHAR)")
            con.executemany("INSERT INTO iceberg.ops.espn_wave_tournament_v1 VALUES "
                            "(?, ?, ?, ?, ?, ?, ?, ?)", self.wave_log)
        try:
            rows = con.execute(sql).fetchall()
        except duckdb.Error:   # как trino-ro.sh: запрос упал (нет таблицы) — ненулевой код
            return None
        return "".join(",".join(f'"{c}"' for c in row) + "\n" for row in rows)

    def add_wave(self, started, **states):
        """Волна: строка '(wave)' и по строке на турнир (state или (state, first_error))."""
        run_id = f"scheduled__{started.isoformat()}"
        rows = [(run_id, _ts(started), _ts(started + timedelta(minutes=20)), "(wave)", None,
                 "green", 0, None)]
        for slug, value in states.items():
            state, error = value if isinstance(value, tuple) else (value, None)
            rows.append((run_id, _ts(started), _ts(started + timedelta(minutes=20)),
                         slug.replace("_", "."), 2026, state, 1, error))
        self.wave_log = (self.wave_log or []) + rows

    def tg_send(self, text):
        self.sent.append(text)
        return True

    def gh(self, args, timeout=60):
        self.gh_calls.append(list(args))
        if args[:2] == ["issue", "list"]:
            return json.dumps(self.open_issues)
        if args[:2] == ["issue", "create"]:
            return "https://github.com/sergeykuznetsov1995/data-platform-football/issues/9001"
        query = next((a for a in args if a.startswith("query=")), "")
        if "issue(number" in query:
            return "I_kwNODE9001"
        if "addProjectV2ItemById" in query:
            return "PVTI_item9001"
        if "updateProjectV2ItemFieldValue" in query:
            if self.board_failures:
                self.board_failures -= 1
                return None
            return '{"data":{}}'
        raise AssertionError(f"неожиданный вызов gh: {args}")


@pytest.fixture
def world(monkeypatch):
    w = World()
    for name in ("psql", "trino", "tg_send", "gh"):
        monkeypatch.setattr(watch, name, getattr(w, name))
    return w


def _run(tmp_path, now, *extra):
    state = tmp_path / "espn_stall_state.json"
    assert watch.main(["--state", str(state), "--now", now.isoformat(), *extra]) == 0
    return json.loads(state.read_text())


# 1. «DAG на паузе → тревога»

def test_recorded_2409_metadb_all_expected_dags_paused_raise_alarm(world):
    alerts = watch.evaluate(NOW, watch.read_dags(), None)
    for dag in EXPECTED:
        assert f"{dag} (пауза)" in alerts["paused"]
    # backfill/repair/replay — по требованию, их пауза не тревога
    assert "dag_backfill_espn" not in alerts["paused"]
    assert "stall" not in alerts


def test_one_paused_dag_raises_alarm_with_its_name(world):
    world.set_dags(**{d: "f" for d in EXPECTED})
    world.set_dags(dag_ingest_espn="t")
    alerts = watch.evaluate(NOW, watch.read_dags(), None)
    assert alerts["paused"].startswith("⏸ ESPN: DAG на паузе/нет: dag_ingest_espn (пауза)")
    assert "dag_monitor_espn" not in alerts["paused"]


def test_all_expected_dags_running_no_alarm_even_if_backfill_paused(world):
    world.set_dags(**{d: "f" for d in EXPECTED})
    assert watch.evaluate(NOW, watch.read_dags(), None) == {"paused": None}


def test_missing_dag_raises_alarm(world):
    world.set_dags(**{d: "f" for d in EXPECTED})
    world.dags = [row for row in world.dags if row[0] != "dag_monitor_espn"]
    alerts = watch.evaluate(NOW, watch.read_dags(), None)
    assert "dag_monitor_espn (нет в metadb)" in alerts["paused"]


def test_metadb_unavailable_raises_alarm_under_the_same_rule(world):
    world.metadb_up = False
    assert watch.read_dags() is None
    alerts = watch.evaluate(NOW, None, None)
    assert "espn-airflow-airflow-metadb-1" in alerts["paused"]
    assert "недоступен" in alerts["paused"]


# 2. «36 ч без новых матчей → тревога»

def test_no_new_matches_for_37h_raises_alarm(world):
    last = NOW - timedelta(hours=37)
    world.bronze = {"espn_lineup_generation_v2": [(1, _ts(last)), (2, _ts(last - timedelta(hours=5)))],
                    "espn_matchsheet_generation_v2": [(1, _ts(last - timedelta(minutes=3)))]}
    bronze = watch.read_bronze(NOW)
    assert bronze[0] == 0
    alerts = watch.evaluate(NOW, None, bronze)
    assert "нет новых матчей в bronze 37 ч" in alerts["stall"]
    assert last.strftime("%Y-%m-%d %H:%M") in alerts["stall"]


def test_recorded_2409_bronze_gives_about_a_thousand_hours(world):
    alerts = watch.evaluate(NOW, None, watch.read_bronze(NOW))
    assert "нет новых матчей в bronze 1016 ч" in alerts["stall"]
    assert "2026-08-13 11:38" in alerts["stall"]


def test_three_fresh_matches_clear_the_stall(world):
    fresh = NOW - timedelta(hours=1)
    world.bronze = {"espn_lineup_generation_v2": [(1, _ts(fresh)), (2, _ts(fresh)), (2, _ts(fresh))],
                    "espn_matchsheet_generation_v2": [(3, _ts(fresh)), (1, _ts(fresh))]}
    bronze = watch.read_bronze(NOW)
    assert bronze[0] == 3
    assert bronze[1].startswith(fresh.strftime("%Y-%m-%d %H:%M:%S"))
    assert watch.evaluate(NOW, None, bronze)["stall"] is None


def test_rows_after_now_do_not_count(world):
    """--now в прошлом: загрузки позже «сейчас» не делают прошлое свежим."""
    world.bronze = {"espn_lineup_generation_v2": [(1, _ts(NOW - timedelta(hours=40))),
                                                  (2, _ts(NOW + timedelta(hours=2)))]}
    assert watch.read_bronze(NOW)[0] == 0


# 3. Серия: тишина, эскалация, отбой

def test_series_alarm_once_then_escalate_then_all_clear(world, tmp_path):
    state = _run(tmp_path, NOW)
    assert len(world.sent) == 2
    assert any(t.startswith("⏸ ESPN: DAG на паузе") for t in world.sent)
    assert any(t.startswith("🔴 ESPN: нет новых матчей в bronze 1016 ч") for t in world.sent)
    assert set(state["episodes"]) == {"paused", "stall"}

    world.sent.clear()
    _run(tmp_path, NOW + timedelta(minutes=15))
    assert world.sent == []
    assert world.gh_calls == []

    state = _run(tmp_path, NOW + timedelta(hours=25))
    creates = [c for c in world.gh_calls if c[:2] == ["issue", "create"]]
    titles = [c[c.index("--title") + 1] for c in creates]
    assert titles == ["ESPN: сторож [paused] — DAG ESPN на паузе или metadb недоступен, с 24.09.2026",
                      "ESPN: сторож [stall] — нет новых матчей в bronze 36 ч, с 24.09.2026"]
    assert all(c[c.index("--label") + 1] == "source:espn,area:bronze,type:bug" for c in creates)
    blocked = [c for c in world.gh_calls if any("updateProjectV2ItemFieldValue" in a for a in c)]
    assert len(blocked) == 2 and all("o=aedb5014" in c for c in blocked)
    assert state["episodes"]["paused"]["issue"] == 9001
    assert sum(t.startswith("📌 ESPN:") for t in world.sent) == 2

    world.sent.clear()
    world.set_dags(**{d: "f" for d in EXPECTED})
    world.bronze = {"espn_lineup_generation_v2": [(7, _ts(NOW + timedelta(hours=25)))]}
    state = _run(tmp_path, NOW + timedelta(hours=26))
    assert sorted(world.sent) == sorted([
        "✅ ESPN: отбой — DAG ESPN на паузе или metadb недоступен (эпизод с 2026-09-24T20:00Z, issue #9001)",
        "✅ ESPN: отбой — нет новых матчей в bronze 36 ч (эпизод с 2026-09-24T20:00Z, issue #9001)",
    ])
    assert state["episodes"] == {}


def test_daily_reminder_after_issue(world, tmp_path):
    _run(tmp_path, NOW)
    _run(tmp_path, NOW + timedelta(hours=25))
    world.sent.clear()
    _run(tmp_path, NOW + timedelta(hours=30))
    assert world.sent == []
    _run(tmp_path, NOW + timedelta(hours=49))
    assert len(world.sent) == 2
    assert any(t.startswith("⏳ ESPN: продолжается (49 ч) — ⏸ ESPN: DAG на паузе") for t in world.sent)
    assert any(t.startswith("⏳ ESPN: продолжается (49 ч) — 🔴 ESPN: нет новых матчей") for t in world.sent)


def test_open_issue_with_same_title_is_reused(world, tmp_path):
    _run(tmp_path, NOW)
    world.open_issues = [{"number": 1456,
                          "title": "ESPN: сторож [stall] — нет новых матчей в bronze 36 ч, с 24.09.2026"}]
    state = _run(tmp_path, NOW + timedelta(hours=25))
    assert state["episodes"]["stall"]["issue"] == 1456
    assert state["episodes"]["paused"]["issue"] == 9001
    assert sum(c[:2] == ["issue", "create"] for c in world.gh_calls) == 1
    # карточка Blocked ставится и найденной открытой issue (Astra р1 п.2)
    assert ["n=1456"] == [a for c in world.gh_calls for a in c if a == "n=1456"]
    assert state["episodes"]["stall"]["blocked"] is True
    assert any("issue #1456 уже была открыта, карточка Blocked" in t for t in world.sent)


def _updates(world):
    return [c for c in world.gh_calls if any("updateProjectV2ItemFieldValue" in a for a in c)]


def test_failed_board_card_is_retried_without_a_second_issue(world, tmp_path):
    world.set_dags(**{d: "f" for d in EXPECTED})   # только stall — одна issue
    _run(tmp_path, NOW)
    world.board_failures = 2
    state = _run(tmp_path, NOW + timedelta(hours=25))
    ep = state["episodes"]["stall"]
    assert ep["issue"] == 9001 and ep["blocked"] is False
    assert any(t.startswith("📌 ESPN:") and "карточку Blocked поставить не вышло — повторю" in t
               for t in world.sent)

    world.sent.clear()
    state = _run(tmp_path, NOW + timedelta(hours=25, minutes=15))   # вторая неудача
    assert state["episodes"]["stall"]["blocked"] is False
    state = _run(tmp_path, NOW + timedelta(hours=25, minutes=30))   # встала
    assert state["episodes"]["stall"]["blocked"] is True
    assert sum(c[:2] == ["issue", "create"] for c in world.gh_calls) == 1
    assert len(_updates(world)) == 3
    assert world.sent == []                                          # повторы карточки молча

    _run(tmp_path, NOW + timedelta(hours=26))
    assert len(_updates(world)) == 3                                 # после успеха доску не дёргаем


def test_seeded_issue_with_blocked_true_is_not_touched(world, tmp_path):
    """Шаг установки: эпизоды первого запуска помечаются issue #1456 + blocked — эпик на доске
    не трогаем и новую issue не заводим."""
    state = _run(tmp_path, NOW)
    for ep in state["episodes"].values():
        ep.update(issue=1456, blocked=True)
    (tmp_path / "espn_stall_state.json").write_text(json.dumps(state))
    world.sent.clear()
    _run(tmp_path, NOW + timedelta(hours=25))
    assert world.gh_calls == []
    assert len(world.sent) == 2 and all(t.startswith("⏳ ESPN: продолжается (25 ч)") for t in world.sent)


# 4. Trino недоступен — stall не трогаем

def test_trino_unavailable_leaves_stall_episode_untouched(world, tmp_path):
    before = _run(tmp_path, NOW)
    world.sent.clear()
    world.trino_up = False
    world.set_dags(**{d: "f" for d in EXPECTED})
    after = _run(tmp_path, NOW + timedelta(hours=30))
    assert after["episodes"]["stall"] == before["episodes"]["stall"]
    assert "paused" not in after["episodes"]
    assert world.sent == ["✅ ESPN: отбой — DAG ESPN на паузе или metadb недоступен (эпизод с 2026-09-24T20:00Z)"]
    assert world.gh_calls == []


# 5. --dry-run

def test_dry_run_sends_nothing_and_opens_no_issue(world, tmp_path, capsys):
    _run(tmp_path, NOW, "--dry-run")
    _run(tmp_path, NOW + timedelta(hours=25), "--dry-run")
    assert world.sent == []
    assert world.gh_calls == []
    out = capsys.readouterr().out
    assert "DRY: ⏸ ESPN: DAG на паузе" in out
    assert "DRY: завёл бы issue «ESPN: сторож [stall]" in out


def test_dry_run_refuses_the_production_state(world):
    with pytest.raises(SystemExit):
        watch.main(["--dry-run"])


# 4. #1505: турнир красный 3 волны подряд

def _quiet(world):
    """Старый контур в порядке — остаются только тревоги по турнирам."""
    world.set_dags(**{d: "f" for d in EXPECTED})
    world.bronze = {"espn_lineup_generation_v2": [(1, _ts(NOW + timedelta(hours=h)))
                                                  for h in range(-24, 48, 6)]}


def test_no_wave_log_is_silently_skipped(world, tmp_path, capsys):
    _quiet(world)
    state = _run(tmp_path, NOW)
    assert world.sent == [] and state["episodes"] == {}
    assert "red=no_wave_log" in capsys.readouterr().out


def test_tournament_red_three_waves_in_a_row_alerts_once_then_clears(world, tmp_path):
    _quiet(world)
    err = ("WavePlanError", "2026-09-24: HttpStatusError: down")
    world.add_wave(NOW - timedelta(hours=12), eng_1=("red", ": ".join(err)), ger_2="green")
    world.add_wave(NOW - timedelta(hours=6), eng_1=("red", ": ".join(err)), ger_2="red")
    state = _run(tmp_path, NOW - timedelta(hours=5))
    assert world.sent == []   # два красных подряд — ещё не тревога

    world.add_wave(NOW, eng_1=("red", ": ".join(err)), ger_2="red")
    state = _run(tmp_path, NOW + timedelta(minutes=15))
    assert world.sent == ["🔴 ESPN: турнир eng.1 красный 3 волны подряд (последняя "
                          "2026-09-24 20:00 UTC): WavePlanError: 2026-09-24: HttpStatusError: down. #1505"]
    assert set(state["episodes"]) == {"red:eng.1"}

    world.sent.clear()
    world.add_wave(NOW + timedelta(hours=6), eng_1="red", ger_2="green")
    _run(tmp_path, NOW + timedelta(hours=6, minutes=15))
    assert world.sent == []   # та же серия — тишина, ger.2 красный лишь 2 волны
    _run(tmp_path, NOW + timedelta(hours=25))
    assert len(world.sent) == 1
    assert world.sent[0].startswith("⏳ ESPN: продолжается (25 ч) — 🔴 ESPN: турнир eng.1 ")
    assert world.gh_calls == []   # без issue

    world.sent.clear()
    world.add_wave(NOW + timedelta(hours=30), eng_1="green")
    state = _run(tmp_path, NOW + timedelta(hours=30, minutes=15))
    assert world.sent == ["✅ ESPN: отбой — турнир eng.1 красный 3 волны подряд "
                          "(эпизод с 2026-09-24T20:15Z)"]
    assert state["episodes"] == {}


def test_red_rule_left_alone_when_trino_is_down(world, tmp_path):
    _quiet(world)
    for hours in (12, 6, 0):
        world.add_wave(NOW - timedelta(hours=hours), eng_1="red")
    _run(tmp_path, NOW)
    world.trino_up = False
    world.sent.clear()
    state = _run(tmp_path, NOW + timedelta(hours=1))
    assert world.sent == [] and set(state["episodes"]) == {"red:eng.1"}


def test_one_red_season_makes_the_tournament_red_in_any_row_order(world, tmp_path):
    """Astra 1505 р1 п.3: строка журнала — турнир-сезон; зелёный сезон не затирает красный."""
    _quiet(world)
    world.wave_log = []
    for hours, first in ((12, "green"), (6, "red"), (0, "green")):
        started = NOW - timedelta(hours=hours)
        run_id = f"r{hours}"
        end = started + timedelta(minutes=20)
        world.wave_log += [(run_id, _ts(started), _ts(end), "(wave)", None, "green", 0, None)]
        seasons = [(2025, "red", "E: old"), (2026, "green", None)]
        if first == "green":
            seasons.reverse()
        world.wave_log += [(run_id, _ts(started), _ts(end), "eng.1", year, state, 1, error)
                           for year, state, error in seasons]
    _run(tmp_path, NOW + timedelta(minutes=15))
    assert world.sent == ["🔴 ESPN: турнир eng.1 красный 3 волны подряд (последняя "
                          "2026-09-24 20:00 UTC): E: old. #1505"]


# 4. #1506: «downgrade_rejected за 24 ч → тревога раз в сутки, без issue»

def test_no_recheck_log_is_silently_skipped(world, tmp_path, capsys):
    _quiet(world)
    state = _run(tmp_path, NOW)
    assert world.sent == [] and state["episodes"] == {}
    assert "downgrade=no_recheck_log" in capsys.readouterr().out


def test_downgrade_alerts_once_a_day_with_leagues_then_clears(world, tmp_path):
    _quiet(world)
    world.recheck_log = [
        (_ts(NOW - timedelta(hours=3)), "bra.copa_do_brazil", 401866354, "downgrade_rejected"),
        (_ts(NOW - timedelta(hours=2)), "bra.copa_do_brazil", 401866355, "downgrade_rejected"),
        (_ts(NOW - timedelta(hours=1)), "eng.1", 578281, "downgrade_rejected"),
        (_ts(NOW - timedelta(hours=1)), "eng.1", 578282, "filled"),
        # Older than 24 h: outside.
        (_ts(NOW - timedelta(hours=30)), "ger.2", 456996, "downgrade_rejected"),
    ]
    state = _run(tmp_path, NOW)
    assert world.sent == ["🟠 ESPN: downgrade_rejected за 24 ч — 3 (ESPN прислал беднее, "
                          "оставлено старое): bra.copa_do_brazil 2, eng.1 1. #1506"]
    assert set(state["episodes"]) == {"downgrade"}

    world.sent.clear()
    _run(tmp_path, NOW + timedelta(minutes=15))
    _run(tmp_path, NOW + timedelta(hours=6))
    assert world.sent == []   # одно сообщение в сутки
    world.recheck_log.append((_ts(NOW + timedelta(hours=23)), "eng.1", 578283, "downgrade_rejected"))
    _run(tmp_path, NOW + timedelta(hours=24))
    assert len(world.sent) == 1
    assert world.sent[0].startswith("⏳ ESPN: продолжается (24 ч) — 🟠 ESPN: downgrade_rejected")
    assert world.gh_calls == []   # без issue даже после суток

    world.sent.clear()
    state = _run(tmp_path, NOW + timedelta(hours=48))
    assert world.sent == ["✅ ESPN: отбой — downgrade_rejected за 24 ч (эпизод с 2026-09-24T20:00Z)"]
    assert state["episodes"] == {}


def test_empty_recheck_log_is_quiet(world, tmp_path):
    _quiet(world)
    world.recheck_log = [(_ts(NOW - timedelta(hours=1)), "eng.1", 1, "same")]
    state = _run(tmp_path, NOW)
    assert world.sent == [] and state["episodes"] == {}
