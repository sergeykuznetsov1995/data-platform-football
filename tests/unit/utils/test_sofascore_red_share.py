"""#1356: one red-share threshold for the SofaScore scope lanes.

The module is imported by the host morning report and the stall watch
straight from the release tree, so it must not need Airflow.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[3] / "dags" / "utils" / "sofascore_red_share.py"
)


@pytest.fixture
def red_share():
    spec = importlib.util.spec_from_file_location("sofascore_red_share", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_threshold_is_twenty_percent(red_share):
    assert red_share.RED_SHARE_THRESHOLD_PCT == 20


def test_exactly_twenty_percent_is_red(red_share):
    share = red_share.RedShare.of(failed=20, total=100)
    assert share.verdict == "red"


def test_just_below_twenty_percent_is_ok(red_share):
    share = red_share.RedShare.of(failed=199, total=1000)
    assert share.pct == pytest.approx(19.9)
    assert share.verdict == "ok"


def test_no_terminal_task_instances_is_not_ok(red_share):
    share = red_share.RedShare.of(failed=0, total=0)
    assert share.verdict == "no_data"


def test_sql_counts_terminal_scope_task_instances_in_the_window(red_share):
    sql = red_share.red_share_sql("2026-09-22T00:00:00Z", "2026-09-23T00:00:00Z")
    assert "FROM task_instance" in sql
    assert "dag_backfill_sofascore_all_mens" in sql
    assert "dag_refresh_sofascore_all_mens" in sql
    assert "task_id LIKE 'run\\_%\\_scope' ESCAPE '\\'" in sql
    assert "map_index >= 0" in sql
    assert "state IN ('success', 'failed')" in sql
    assert "start_date >= '2026-09-22T00:00:00Z'::timestamptz" in sql
    assert "start_date < '2026-09-23T00:00:00Z'::timestamptz" in sql


def test_sql_rejects_a_non_utc_literal(red_share):
    with pytest.raises(ValueError):
        red_share.red_share_sql("2026-09-22 00:00", "2026-09-23T00:00:00Z")


def test_pattern_matches_scope_tasks_only(red_share):
    import re

    like = red_share.SCOPE_TASK_PATTERN.replace("\\_", "_").replace("%", ".*")
    regex = re.compile("^" + like + "$")
    assert regex.match("run_historical_scope")
    assert regex.match("run_refresh_scope")
    assert not regex.match("run_sofascore_dq")


def test_parse_psql_rows(red_share):
    per_dag = red_share.parse_rows(
        "dag_backfill_sofascore_all_mens|70|121\n"
        "dag_refresh_sofascore_all_mens|3|40\n"
    )
    assert per_dag == {
        "dag_backfill_sofascore_all_mens": (70, 121),
        "dag_refresh_sofascore_all_mens": (3, 40),
    }


def test_format_line_red(red_share):
    line = red_share.format_line(
        "2026-09-22",
        {
            "dag_backfill_sofascore_all_mens": (70, 121),
            "dag_refresh_sofascore_all_mens": (3, 40),
        },
    )
    assert line == (
        "красных попыток скоупов за 22.09 (UTC): 73/161 = 45,3 % ⛔ "
        "(порог < 20 %; история 70/121, актуалка 3/40)"
    )


def test_format_line_ok_has_the_check_mark(red_share):
    line = red_share.format_line(
        "2026-09-22", {"dag_backfill_sofascore_all_mens": (1, 10)}
    )
    assert line == (
        "красных попыток скоупов за 22.09 (UTC): 1/10 = 10,0 % ✅ "
        "(порог < 20 %; история 1/10, актуалка 0/0)"
    )


def test_format_line_no_data_has_no_check_mark(red_share):
    line = red_share.format_line("2026-09-22", {})
    assert "✅" not in line
    assert "⚠️ нет терминальных попыток" in line


def test_module_imports_without_airflow():
    code = (
        "import sys, importlib.util\n"
        "sys.modules['airflow'] = None\n"
        f"spec = importlib.util.spec_from_file_location('m', {str(MODULE_PATH)!r})\n"
        "m = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(m)\n"
        "assert not any(k == 'airflow' or k.startswith('airflow.') "
        "for k, v in sys.modules.items() if v is not None)\n"
        "print(m.RED_SHARE_THRESHOLD_PCT)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "20"


def _slot_report(directory, run, index, *, status="success", start="2026-10-07T12:00:00+00:00"):
    import hashlib
    import json
    directory.mkdir(exist_ok=True)
    finished = start[:11] + "13:00:00+00:00"
    env = {"SOFASCORE_CAMPAIGN_ACTION": "capture", "SOFASCORE_SCOPE_RESULT_PATH": f"/container/results/{index}.json",
           "SOFASCORE_SCOPE_RUN_ID": f"scope-{index}", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "camp",
           "SOFASCORE_TOURNAMENT_ID": "1", "SOFASCORE_SOURCE_SEASON_ID": str(index)}
    item = {"slot": str(index % 3), "started_at": start, "finished_at": finished,
            "accounted": True, "outcome": {"status": status}, "environment": env}
    payload = {"campaign_id": "camp", "tournament_id": 1, "source_season_id": index, "run_id": f"scope-{index}", "scope_digest": f"scope-{index}",
               "status": "success", "history_scope_attempt": True,
               "history_slot": {"slot": index % 3, "dag_run_id": run, "started_at": start,
                                "finished_at": finished, "terminal_state": status}}
    (directory / f"{index}.json").write_text(json.dumps(payload))
    path = directory / ("history-slots-" + hashlib.sha256(run.encode()).hexdigest()[:20] + ".json")
    receipt = json.loads(path.read_text()) if path.exists() else {"history_slots_receipt": True, "finalized": True, "dag_run_id": run, "items": []}
    item["index"] = len(receipt["items"])
    item["plan_index"] = index * 2
    receipt["items"].append(item)
    receipt["claimed_count"] = len(receipt["items"])
    receipt["plan_length"] = max(item["plan_index"] for item in receipt["items"]) + 2
    receipt["plan_digest"] = "a" * 64
    receipt["receipt_digest"] = hashlib.sha256(json.dumps({key: value for key, value in receipt.items() if key != "receipt_digest"}, sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(receipt))
    return path


def test_sql_executes_and_distinguishes_slot_workers_from_legacy_scopes(red_share):
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.create_function("convert_from", 2, lambda value, encoding: bytes(value).decode(encoding))
    db.executescript("CREATE TABLE task_instance (dag_id,run_id,task_id,map_index,state,start_date,end_date); CREATE TABLE xcom(dag_id,run_id,task_id,key,value); CREATE TABLE dag_run(dag_id,run_id,start_date,end_date);")
    history, refresh = red_share.SCOPE_DAG_IDS
    db.execute("INSERT INTO xcom VALUES(?,?,?,?,?)", (history, "slots-run", "plan_historical_batch", "history_mode", b'"slots"'))
    db.execute("INSERT INTO dag_run VALUES(?,?,?,?)", (history, "slots-run", "2026-10-06T23:00:00Z", "2026-10-07T01:00:00Z"))
    for dag, run, state, started, ended in (
        (history, "old", "failed", "2026-10-07T11:00:00Z", "2026-10-07T12:00:00Z"),
        (history, "slots-run", "success", "2026-10-06T23:00:00Z", "2026-10-07T01:00:00Z"),
        (refresh, "refresh", "success", "2026-10-07T11:00:00Z", "2026-10-07T12:00:00Z"),
    ):
        task = "run_historical_scope" if dag == history else "run_refresh_scope"
        db.execute("INSERT INTO task_instance VALUES(?,?,?,?,?,?,?)", (dag, run, task, 0, state, started, ended))
    def rows(sql):
        return db.execute(sql.replace("::timestamptz", "")).fetchall()
    bounds = ("2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z")
    assert dict((dag, (f, t)) for dag, f, t in rows(red_share.red_share_sql(*bounds))) == {history: (-1, -1), refresh: (0, 1)}
    assert rows(red_share.red_share_sql(*bounds, scope_reports=True)) == [(history, 1, 1), (refresh, 0, 1)]
    assert rows(red_share.history_slot_runs_sql(*bounds)) == [("slots-run",)]
    db.close()


def test_scope_receipt_counts_attempts_and_authoritative_validation_failure(red_share, tmp_path):
    _slot_report(tmp_path, "run", 0)
    _slot_report(tmp_path, "run", 1, status="failed")
    _slot_report(tmp_path, "run", 2, start="2026-10-06T12:00:00+00:00")
    assert red_share.history_scope_counts(tmp_path, "2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z", {"run"}) == (1, 2)


@pytest.mark.parametrize("damage", ["missing", "unaccounted", "mismatch", "duplicate", "shape", "digest"])
def test_incomplete_slot_evidence_never_produces_green(red_share, tmp_path, damage):
    import json
    import hashlib
    receipt = _slot_report(tmp_path, "run", 0)
    if damage == "missing":
        receipt.unlink()
    else:
        document = json.loads(receipt.read_text())
        if damage == "unaccounted":
            document["items"][0]["accounted"] = False
        elif damage == "mismatch":
            document["items"][0]["outcome"]["status"] = "failed"
        elif damage == "duplicate":
            document["items"].append(document["items"][0])
        elif damage == "shape":
            document = []
        else:
            document["receipt_digest"] = "f" * 64
        if isinstance(document, dict) and damage != "digest":
            document["receipt_digest"] = hashlib.sha256(json.dumps({key: value for key, value in document.items() if key != "receipt_digest"}, sort_keys=True).encode()).hexdigest()
        receipt.write_text(json.dumps(document))
    with pytest.raises((OSError, ValueError)):
        red_share.history_scope_counts(tmp_path, "2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z", {"run"})


def test_legacy_host_without_scope_adapter_shows_unavailable(red_share):
    rows = {red_share.HISTORY_DAG_ID: (-1, -1), red_share.REFRESH_DAG_ID: (0, 100)}
    assert red_share.total_share(rows).verdict == "unavailable"
    line = red_share.format_line("2026-10-07", rows)
    assert "история недоступна" in line
    assert "✅" not in line


@pytest.mark.parametrize("damage", ["negative", "duplicate", "out_of_bounds", "boolean"])
def test_slot_receipt_rejects_invalid_immutable_plan_indices(red_share, tmp_path, damage):
    import hashlib
    import json
    _slot_report(tmp_path, "run", 0)
    path = _slot_report(tmp_path, "run", 1)
    receipt = json.loads(path.read_text())
    indexes = {"negative": -1, "duplicate": receipt["items"][0]["plan_index"],
               "out_of_bounds": receipt["plan_length"], "boolean": True}
    receipt["items"][1]["plan_index"] = indexes[damage]
    receipt["receipt_digest"] = hashlib.sha256(json.dumps({key: value for key, value in receipt.items() if key != "receipt_digest"}, sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="plan indexes"):
        red_share.history_scope_counts(tmp_path, "2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z", {"run"})


@pytest.mark.parametrize('attempts',[0,2])
def test_unstarted_reservation_is_not_a_capture_attempt(red_share,tmp_path,attempts):
    import json,hashlib
    path=_slot_report(tmp_path,'run',0)
    receipt=json.loads(path.read_text())
    receipt['items'][0]['attempts']=attempts
    receipt['items'][0]['outcome']={'status':'not_started'}
    receipt['receipt_digest']=hashlib.sha256(json.dumps({k:v for k,v in receipt.items() if k!='receipt_digest'},sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(receipt))
    args=(tmp_path,'2026-10-07T00:00:00Z','2026-10-08T00:00:00Z',{'run'})
    if attempts==0:
        assert red_share.history_scope_counts(*args)==(0,0)
    else:
        with pytest.raises(ValueError,match='unstarted scope'):
            red_share.history_scope_counts(*args)


def _partial_slot_report(directory):
    import json
    import hashlib
    from tests.unit.scrapers.test_sofascore_history_controller import continuation_report
    path = _slot_report(directory, 'run', 1, status='partial')
    report_path = directory/'1.json'
    payload = json.loads(report_path.read_text())
    payload.update(continuation_report(1, 1, 'camp', 'scope-1'))
    report_path.write_text(json.dumps(payload))
    receipt = json.loads(path.read_text())
    receipt['items'][0]['outcome']['partial'] = payload['phases'][0]['partial']
    receipt['receipt_digest'] = hashlib.sha256(json.dumps(
        {k:v for k,v in receipt.items() if k != 'receipt_digest'}, sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(receipt))
    return path, report_path


def test_durable_season_continuation_counts_attempt_without_failure(red_share, tmp_path):
    _partial_slot_report(tmp_path)
    assert red_share.history_scope_counts(tmp_path, '2026-10-07T00:00:00Z',
                                         '2026-10-08T00:00:00Z', {'run'}) == (0, 1)


@pytest.mark.parametrize('damage', ['marker', 'proof', 'frontier', 'raw', 'errors', 'phase'])
def test_partial_receipt_requires_exact_healthy_report_proof(red_share, tmp_path, damage):
    import json
    import hashlib
    path, report_path = _partial_slot_report(tmp_path)
    payload = json.loads(report_path.read_text())
    receipt = json.loads(path.read_text())
    if damage == 'proof': receipt['items'][0]['outcome']['partial'] = {}
    elif damage == 'marker': payload['phases'][0]['partial']['kind'] = 'time_budget'
    elif damage == 'frontier': payload['phases'][0]['partial']['continuation']['last'] = 48
    elif damage == 'raw': payload['phases'][0]['partial']['raw_manifest_committed'] = False
    elif damage == 'errors': payload['errors'] = ['failed']
    elif damage == 'phase': payload['phases'][0]['phase'] = 'matches'
    report_path.write_text(json.dumps(payload))
    receipt['receipt_digest'] = hashlib.sha256(json.dumps(
        {k:v for k,v in receipt.items() if k != 'receipt_digest'}, sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        red_share.history_scope_counts(tmp_path, '2026-10-07T00:00:00Z',
                                       '2026-10-08T00:00:00Z', {'run'})


def test_partial_receipt_works_as_standalone_stdlib_host_adapter(tmp_path):
    import subprocess
    import sys
    from pathlib import Path
    _partial_slot_report(tmp_path)
    module_path = Path(__file__).resolve().parents[3]/'dags/utils/sofascore_red_share.py'
    code = (
        'import importlib.util,sys\n'
        f'spec=importlib.util.spec_from_file_location("standalone", {str(module_path)!r})\n'
        'module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n'
        f'assert module.history_scope_counts(module.Path({str(tmp_path)!r}), '
        '"2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z", {"run"}) == (0,1)\n'
        'assert not any(k == "scrapers" or k.startswith("scrapers.") for k in sys.modules)\n'
    )
    result = subprocess.run([sys.executable, '-I', '-c', code], cwd=tmp_path,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('damage', ['bool_range', 'negative', 'oversize', 'zero', 'extra',
                                   'identity', 'digest', 'noncanonical', 'duplicate'])
def test_continuation_token_evidence_fails_closed(red_share, tmp_path, damage):
    import json
    import base64
    _, report_path = _partial_slot_report(tmp_path)
    partial = json.loads(report_path.read_text())['phases'][0]['partial']
    encoded = partial['chunk_unit'].split('.', 1)[1]
    chunk = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
    if damage == 'bool_range': chunk['last'][0] = False
    elif damage == 'negative': chunk['last'][0] = -1
    elif damage == 'oversize': chunk['last'][1] = 50
    elif damage == 'zero': chunk['last'] = chunk['next'] = [0, 0]
    elif damage == 'extra': chunk['hint'] = 'unsigned'
    elif damage == 'identity': chunk['season_id'] = '01'
    elif damage == 'digest': chunk['evidence_digest'] = 'A' * 64
    body = json.dumps(chunk, sort_keys=True, separators=(',', ':'))
    if damage == 'noncanonical': body = json.dumps(chunk, indent=2)
    elif damage == 'duplicate': body = body[:-1] + ',"season_id":"1"}'
    partial['chunk_unit'] = 'season-chunk-v1.' + base64.urlsafe_b64encode(body.encode()).decode().rstrip('=')
    with pytest.raises(ValueError):
        red_share.validate_season_continuation(partial, tournament_id='1', season_id='1')


def test_continuation_allows_inactive_direction_frontier_at_start(red_share):
    import json
    import base64
    from tests.unit.scrapers.test_sofascore_history_controller import continuation_report
    partial = continuation_report()['phases'][0]['partial']
    chunk = dict(tournament_id='8', season_id='826', last=[0, 50], next=[5, 5],
                 evidence_digest='a' * 64)
    body = json.dumps(chunk, sort_keys=True, separators=(',', ':'))
    partial['chunk_unit'] = 'season-chunk-v1.' + base64.urlsafe_b64encode(body.encode()).decode().rstrip('=')
    partial['continuation'] = {'last': None, 'next': 5}
    assert red_share.validate_season_continuation(partial, tournament_id=8, season_id=826) == partial
