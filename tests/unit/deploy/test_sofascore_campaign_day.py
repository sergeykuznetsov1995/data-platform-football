"""«Ночь без данных — красная» (#1376): кампания SofaScore за сутки судится кодом релиза."""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone

import pytest

from deploy.sofascore import campaign_day
from deploy.sofascore import pipeline_metrics as metrics
from tests.unit.utils.test_sofascore_red_share import _slot_report

DAY = date(2026, 10, 7)
HISTORY = metrics.red_share.HISTORY_DAG_ID
REFRESH = metrics.red_share.REFRESH_DAG_ID


def _history_scope(runtime, name, rows):
    results = runtime / "results"
    (results / name).mkdir(parents=True)
    (results / name / "matches.json").write_text(json.dumps({"capture_status_rows": rows}))
    report = results / f"{name}.json"
    report.write_text(json.dumps({"status": "success"}))
    noon = datetime(DAY.year, DAY.month, DAY.day, 12, tzinfo=timezone.utc).timestamp()
    os.utime(report, (noon, noon))


def _metabase(slot_runs=""):
    def query(sql):
        if sql.startswith("SELECT DISTINCT d.run_id"):
            return slot_runs
        if "FILTER (WHERE state = 'failed')" not in sql:
            raise RuntimeError("pool metric not stubbed")
        assert "-1, -1" not in sql, "legacy red share without scope reports"
        return f"{REFRESH}|0|10"
    return query


def _trino(tmp_path, value):
    script = tmp_path / "trino-ro.sh"
    script.write_text(f"#!/bin/sh\necho '\"{value}\"'\necho '\"__sentinel__\"'\n")
    script.chmod(0o755)
    return str(script)


@pytest.mark.unit
def test_campaign_day_is_red_when_the_night_has_no_matches(tmp_path, capsys):
    out, bang = campaign_day.lines(DAY, matches=17784, scopes=155, rows=21002,
                                   red_share={HISTORY: (0, 4), REFRESH: (0, 182)}, pool_wait=None)
    assert not bang
    assert out[0] == ("• кампания SofaScore за 07.10 (UTC): матчей 17784✅, история 155 скоупов/21002 строк✅, "
                      "красных попыток скоупов за 07.10 (UTC): 0/186 = 0,0 % ✅ "
                      "(порог < 20 %; история 0/4, актуалка 0/182)")
    out, bang = campaign_day.lines(DAY, matches=0, scopes=0, rows=0,
                                   red_share={REFRESH: (0, 10)}, pool_wait=None)
    assert bang and "матчей 0‼️ (<1000)" in out[0] and "история 0 скоупов‼️" in out[0]

    _history_scope(tmp_path, "a", 50)
    rc = campaign_day.main(["--day", DAY.isoformat(), "--trino", _trino(tmp_path, 0),
                            "--runtime", str(tmp_path)], query=_metabase())
    printed = capsys.readouterr().out
    assert rc == 3, printed
    assert "матчей 0‼️ (<1000), история 1 скоупов/50 строк✅" in printed
    assert "ожидание пула актуалкой не посчитано ⚠️ (metadb)" in printed

    rc = campaign_day.main(["--day", DAY.isoformat(), "--trino", _trino(tmp_path, 1500),
                            "--runtime", str(tmp_path)], query=_metabase())
    assert rc == 0, capsys.readouterr().out
    rc = campaign_day.main(["--day", DAY.isoformat(), "--trino", str(tmp_path / "absent"),
                            "--runtime", str(tmp_path)], query=_metabase())
    assert rc == 0 and "матчей не посчитаны ⚠️ (trino)" in capsys.readouterr().out


@pytest.mark.unit
def test_campaign_day_history_red_share_uses_scope_reports(tmp_path):
    results = tmp_path / "results"
    _slot_report(results, "slots-run", 0)
    _slot_report(results, "slots-run", 1, status="failed")
    per_dag = campaign_day.red_share_day(tmp_path, DAY, _metabase("slots-run"))
    assert per_dag == {HISTORY: (1, 2), REFRESH: (0, 10)}
    out, bang = campaign_day.lines(DAY, matches=1500, scopes=1, rows=5, red_share=per_dag, pool_wait=None)
    assert "история 1/2, актуалка 0/10" in out[0] and "недоступна" not in out[0]
    assert not bang  # 1/12 < 20 %
    # Квитанции слотов ещё нет (прогон идёт) — история недоступна: ⚠️, не красный.
    per_dag = campaign_day.red_share_day(tmp_path, DAY, _metabase("other-run"))
    out, bang = campaign_day.lines(DAY, matches=1500, scopes=1, rows=5, red_share=per_dag, pool_wait=None)
    assert "⚠️ история недоступна" in out[0] and not bang


@pytest.mark.unit
def test_campaign_day_metabase_failure_is_a_warning_not_red(tmp_path, capsys):
    _history_scope(tmp_path, "a", 50)

    def down(sql):
        raise KeyError("unexpected metabase failure")
    rc = campaign_day.main(["--day", DAY.isoformat(), "--trino", _trino(tmp_path, 1500),
                            "--runtime", str(tmp_path)], query=down)
    printed = capsys.readouterr().out
    assert rc == 0, printed
    assert "красных run_*_scope TI не посчитаны ⚠️ (metadb)" in printed
    assert "ожидание пула актуалкой не посчитано ⚠️ (metadb)" in printed


@pytest.mark.unit
def test_campaign_day_runtime_comes_from_the_contour_env_file(tmp_path, monkeypatch):
    env = tmp_path / "sofascore.env"
    env.write_text("SOFASCORE_RELEASE_ROOT=/x\nSOFASCORE_ALL_MENS_RUNTIME_HOST_DIR='/data/all-men'\n")
    assert campaign_day.runtime_from_env_file(env) == campaign_day.Path("/data/all-men")
    text = (campaign_day.ROOT / "deploy/sofascore/campaign_day.py").read_text()
    assert "/root/" not in text
