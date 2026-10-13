from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import acceptance_miro as miro


TABLE = "https://miro.com/app/board/uXjVEfZMz1U=/?moveToWidget=3458764685876111831"
SCRIPT = Path(miro.__file__)


def snapshot(revision=1, keys=("fotmob-1288",)):
    return {
        "schema": 1,
        "revision": revision,
        "sources": {"fotmob": {"name": "FotMob", "miro_row_id": "source-row-not-acceptance"}},
        "acceptances": [{
            "id": key, "source": "fotmob", "issue": 1288, "title": "Полнота матчей",
            "issue_url": "https://github.com/example/project/issues/1288",
            "status": "observing", "revision": revision, "series": 2,
            "deployed_revision": "a1b2c3", "criteria": [{
                "id": "daily", "title": "Дни подряд", "kind": "streak",
                "target": 7, "progress": 2, "status": "waiting",
            }],
            "days": [{"label": "08.10", "status": "pass"}, {"label": "09.10", "status": "unknown"}],
            "reason": "Не хватает подтверждения 09.10",
            "checked_at": "2026-10-09T12:00:00+00:00",
            "next_check_at": "2026-10-10T12:00:00+00:00",
        } for key in keys],
    }


def remote(rows=()):
    return {
        "miro_url": TABLE, "columns": [{"column_title": title, "column_type": "text"}
                                          for title in miro.COLUMNS] + [
            {"column_title": "Чужая заметка", "column_type": "text"}],
        "rows": list(rows), "total": len(rows), "cursor": None,
    }


def row(key, row_id, values=None):
    all_values = {"Задача": key, **(values or {})}
    return {"rowId": row_id, "cells": [
        {"columnTitle": title, "valueType": "text", "content": value}
        for title, value in all_values.items()
    ]}


def apply_plan(readback, plan):
    """A local table model preserves foreign cells and unrelated rows."""
    changed = deepcopy(readback)
    for operation in plan["rows"]:
        if "rowId" in operation:
            target = next(r for r in changed["rows"] if r["rowId"] == operation["rowId"])
        else:
            target = {"rowId": "created-" + str(len(changed["rows"])), "cells": []}
            changed["rows"].append(target)
        cells = {c["columnTitle"]: c for c in target["cells"]}
        for cell in operation["cells"]:
            column = next(c for c in changed["columns"] if c["column_title"] == cell["columnTitle"])
            if column["column_type"] == "select":
                cells[cell["columnTitle"]] = {"columnTitle": cell["columnTitle"], "valueType": "select",
                                               "options": [{"displayValue": cell["value"]}]}
            else:
                cells[cell["columnTitle"]] = {"columnTitle": cell["columnTitle"],
                                               "valueType": "text", "content": cell["value"]}
        target["cells"] = list(cells.values())
    changed["total"] = len(changed["rows"])
    return changed


def test_prepare_saves_lease_before_return_and_ack_requires_values(tmp_path):
    state = tmp_path / "publish.json"
    initial = remote()
    plan = miro.prepare(snapshot(), initial, TABLE, state)
    saved = json.loads(state.read_text())
    assert saved["pending"]["plan_id"] == plan["plan_id"]
    assert saved["pending"]["rows"] == plan["rows"]
    assert "rowId" not in plan["rows"][0]
    miro.dispatch(plan["plan_id"], state)
    with pytest.raises(miro.SyncError, match="Readback does not confirm"):
        miro.ack(plan["plan_id"], initial, state)
    assert json.loads(state.read_text())["pending"]["plan_id"] == plan["plan_id"]
    after = apply_plan(initial, plan)
    confirmed = miro.ack(plan["plan_id"], after, state)
    assert confirmed["revision"] == 1
    assert confirmed["row_ids"] == {"fotmob-1288": "created-0"}
    assert json.loads(state.read_text())["pending"] is None
    acknowledged_bytes = state.read_bytes()
    assert miro.ack(plan["plan_id"], after, state) == confirmed
    assert state.read_bytes() == acknowledged_bytes


def test_lost_insert_response_reconciles_by_stable_key_without_second_insert(tmp_path):
    state = tmp_path / "publish.json"
    initial = remote()
    plan = miro.prepare(snapshot(), initial, TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    # Miro committed, but the caller never received the write response.
    after = apply_plan(initial, plan)
    retry = miro.reconcile(plan["plan_id"], after, state)
    assert retry["rows"] == []
    assert retry["plan_id"] != plan["plan_id"]
    with pytest.raises(miro.SyncError, match="plan_id"):
        miro.ack(plan["plan_id"], after, state)
    miro.ack(retry["plan_id"], after, state)
    repeated = miro.prepare(snapshot(), after, TABLE, state)
    assert repeated["rows"] == []
    assert len(after["rows"]) == 1


def test_ambiguous_absent_insert_keeps_pending_and_forbids_blind_retry(tmp_path):
    state = tmp_path / "publish.json"
    plan = miro.prepare(snapshot(), remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    before = state.read_bytes()
    with pytest.raises(miro.SyncError, match="Insert outcome unresolved"):
        miro.reconcile(plan["plan_id"], remote(), state)
    assert state.read_bytes() == before
    with pytest.raises(miro.SyncError, match="pending"):
        miro.prepare(snapshot(2), remote(), TABLE, state)


def test_partial_write_retry_contains_only_undelivered_cells(tmp_path):
    state = tmp_path / "publish.json"
    initial = remote([row("fotmob-1288", "r1"), row("fotmob-1293", "r2")])
    plan = miro.prepare(snapshot(keys=("fotmob-1288", "fotmob-1293")), initial, TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(initial, {"rows": plan["rows"][:1]})
    retry = miro.reconcile(plan["plan_id"], after, state)
    assert len(retry["rows"]) == 1
    assert retry["rows"][0]["rowId"] == "r2"
    assert "Задача" not in {c["columnTitle"] for c in retry["rows"][0]["cells"]}
    miro.dispatch(retry["plan_id"], state)
    miro.ack(retry["plan_id"], apply_plan(after, retry), state)


def test_foreign_rows_and_columns_unchanged_with_sorting(tmp_path):
    state = tmp_path / "publish.json"
    foreign = row("owners-other-key", "other", {"Чужая заметка": "keep all"})
    initial = remote([foreign, row("fotmob-1288", "own", {"Чужая заметка": "keep cell"})])
    plan = miro.prepare(snapshot(), initial, TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    assert [r["rowId"] for r in plan["rows"]] == ["own"]
    assert all(c["columnTitle"] in miro.COLUMNS for c in plan["rows"][0]["cells"])
    after = apply_plan(initial, plan)
    after["rows"].reverse()
    miro.ack(plan["plan_id"], after, state)
    assert after["rows"][1] == foreign
    assert next(c["content"] for c in after["rows"][0]["cells"]
                if c["columnTitle"] == "Чужая заметка") == "keep cell"


@pytest.mark.parametrize("kind", ["remote_key", "remote_row_id", "input_key", "column"])
def test_duplicate_identity_is_refused(tmp_path, kind):
    data = snapshot()
    readback = remote([row("fotmob-1288", "r1")])
    if kind == "remote_key":
        readback["rows"].append(row("fotmob-1288", "r2"))
    elif kind == "remote_row_id":
        readback["rows"].append(row("another", "r1"))
    elif kind == "input_key":
        data["acceptances"].append(deepcopy(data["acceptances"][0]))
    else:
        readback["columns"].append(readback["columns"][0])
    readback["total"] = len(readback["rows"])
    with pytest.raises(miro.SyncError, match="Duplicate"):
        miro.prepare(data, readback, TABLE, tmp_path / "publish.json")


@pytest.mark.parametrize("change", ["cursor", "count", "table", "filter", "type"])
def test_incomplete_wrong_or_unsupported_table_readback_is_refused(tmp_path, change):
    readback = remote()
    if change == "cursor":
        readback["cursor"] = "next-page"
    elif change == "count":
        readback["total"] = 5
    elif change == "table":
        readback["miro_url"] = TABLE.replace("3458764685876111831", "999")
    elif change == "filter":
        readback["filter_by"] = {"Status": ["Complete"]}
    else:
        readback["columns"][0]["column_type"] = "select"
    with pytest.raises(miro.SyncError):
        miro.prepare(snapshot(), readback, TABLE, tmp_path / "publish.json")


@pytest.mark.parametrize("change", ["missing", "row_id", "key"])
def test_known_row_cannot_be_reinserted_or_reassigned(tmp_path, change):
    state = tmp_path / "publish.json"
    plan = miro.prepare(snapshot(), remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    confirmed = apply_plan(remote(), plan)
    miro.ack(plan["plan_id"], confirmed, state)
    if change == "missing":
        changed = remote()
    else:
        changed = deepcopy(confirmed)
        if change == "row_id":
            changed["rows"][0]["rowId"] = "replacement"
        else:
            next(c for c in changed["rows"][0]["cells"]
                 if c["columnTitle"] == "Задача")["content"] = "foreign-key"
    with pytest.raises(miro.SyncError, match="row|rowId"):
        miro.prepare(snapshot(2), changed, TABLE, state)


def test_no_lease_timeout_and_stale_revision_cannot_overwrite_newer_state(tmp_path):
    state = tmp_path / "publish.json"
    plan = miro.prepare(snapshot(3), remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    saved = json.loads(state.read_text())
    saved["pending"]["prepared_at"] = "2000-01-01T00:00:00Z"
    state.write_text(json.dumps(saved))
    with pytest.raises(miro.SyncError, match="pending"):
        miro.prepare(snapshot(4), remote(), TABLE, state)
    after = apply_plan(remote(), plan)
    miro.ack(plan["plan_id"], after, state)
    with pytest.raises(miro.SyncError, match="Stale snapshot"):
        miro.prepare(snapshot(2), after, TABLE, state)
    modified = snapshot(3)
    modified["acceptances"][0]["reason"] = "new reason with same revision"
    with pytest.raises(miro.SyncError, match="without a new revision"):
        miro.prepare(modified, after, TABLE, state)


def test_counter_does_not_derive_acceptance_and_fact_time_converts_to_msk():
    data = snapshot()
    data["acceptances"][0]["status"] = "unknown"
    data["acceptances"][0]["criteria"][0]["progress"] = 7
    values = miro.render_targets(data)["fotmob-1288"]["values"]
    assert values["Статус"] == "Нет подтверждения"
    assert "7/7" in values["Прогресс"]
    assert "09.10: Нет подтверждения" in values["Дни и проверки"]
    assert values["Проверено, МСК"] == "09.10.2026 15:00:00 МСК"
    assert "Полнота матчей" in values["Причина / следующий шаг"]
    assert values["Задача"] == "fotmob-1288"
    data["acceptances"][0]["checked_at"] = None
    assert miro.render_targets(data)["fotmob-1288"]["values"]["Проверено, МСК"] == "нет подтверждения"


@pytest.mark.parametrize("change", ["naive_time", "bad_status", "negative_progress", "series_zero"])
def test_bad_export_fails_closed(change):
    data = snapshot()
    record = data["acceptances"][0]
    if change == "naive_time":
        record["checked_at"] = "2026-10-09T12:00:00"
    elif change == "bad_status":
        record["status"] = "success"
    elif change == "negative_progress":
        record["criteria"][0]["progress"] = -1
    else:
        record["series"] = 0
    with pytest.raises(miro.SyncError):
        miro.render_targets(data)


def test_readback_wrapped_by_connector_and_link_url_normalization(tmp_path):
    state = tmp_path / "publish.json"
    readback = remote()
    plan = miro.prepare(snapshot(), {"structuredContent": readback}, TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(readback, plan)
    next(c for c in after["columns"] if c["column_title"] == "GitHub")["column_type"] = "link"
    cell = next(c for c in after["rows"][0]["cells"] if c["columnTitle"] == "GitHub")
    cell["content"] = [{"url": cell["content"], "text": "issue"}]
    miro.ack(plan["plan_id"], {"structuredContent": after}, state)


def test_two_processes_cannot_both_prepare_publication(tmp_path):
    source = tmp_path / "export.json"
    readback = tmp_path / "remote.json"
    state = tmp_path / "state.json"
    source.write_text(json.dumps(snapshot()))
    readback.write_text(json.dumps(remote()))
    command = [sys.executable, "-B", str(SCRIPT), "prepare", "--snapshot", str(source),
               "--remote", str(readback), "--table-url", TABLE, "--state", str(state)]
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for _ in range(2)]
    results = [(p, *p.communicate(timeout=10)) for p in processes]
    assert sorted(p.returncode for p, _, _ in results) == [0, 2]
    winner = next(json.loads(out) for p, out, _ in results if p.returncode == 0)
    assert json.loads(state.read_text())["pending"]["plan_id"] == winner["plan_id"]
    assert "pending" in next(err for p, _, err in results if p.returncode == 2)


def test_cli_prepare_readback_ack_cycle(tmp_path, capsys):
    source = tmp_path / "export.json"
    readback = tmp_path / "remote.json"
    state = tmp_path / "state.json"
    source.write_text(json.dumps(snapshot()))
    readback.write_text(json.dumps(remote()))
    assert miro.main(["prepare", "--snapshot", str(source), "--remote", str(readback),
                      "--table-url", TABLE, "--state", str(state)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert miro.main(["dispatch", "--plan-id", plan["plan_id"], "--state", str(state)]) == 0
    assert json.loads(capsys.readouterr().out) == plan
    readback.write_text(json.dumps(apply_plan(remote(), plan)))
    assert miro.main(["ack", "--plan-id", plan["plan_id"], "--remote", str(readback),
                      "--state", str(state)]) == 0
    assert json.loads(capsys.readouterr().out)["row_ids"] == {"fotmob-1288": "created-0"}


def test_corrupt_state_is_not_reset(tmp_path):
    state = tmp_path / "state.json"
    state.write_text("{not JSON")
    with pytest.raises(miro.SyncError, match="do not reset"):
        miro.prepare(snapshot(), remote(), TABLE, state)
    assert state.read_text() == "{not JSON"


def test_dispatch_is_one_attempt_and_unsubmitted_plan_can_be_abandoned(tmp_path):
    state = tmp_path / "state.json"
    first = miro.prepare(snapshot(), remote(), TABLE, state)
    with pytest.raises(miro.SyncError, match="dispatched"):
        miro.ack(first["plan_id"], apply_plan(remote(), first), state)
    with pytest.raises(miro.SyncError, match="dispatched"):
        miro.reconcile(first["plan_id"], remote(), state)
    miro.abandon(first["plan_id"], state)
    second = miro.prepare(snapshot(), remote(), TABLE, state)
    with pytest.raises(miro.SyncError, match="plan_id"):
        miro.dispatch(first["plan_id"], state)
    assert miro.dispatch(second["plan_id"], state) == second
    assert json.loads(state.read_text())["pending"]["phase"] == "dispatched"
    with pytest.raises(miro.SyncError, match="already dispatched"):
        miro.dispatch(second["plan_id"], state)
    with pytest.raises(miro.SyncError, match="Cannot abandon"):
        miro.abandon(second["plan_id"], state)


def test_resolution_requires_definite_failure_for_exact_dispatched_plan(tmp_path):
    state = tmp_path / "state.json"
    evidence_file = tmp_path / "definite-rejection.json"
    plan = miro.prepare(snapshot(), remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    proof = {"plan_id": plan["plan_id"], "outcome": "not_applied", "request_finished": True,
             "summary": "Connector rejected the request before dispatch because the schema was invalid"}
    evidence_file.write_text(json.dumps(proof))
    with pytest.raises(miro.SyncError, match="explicit"):
        miro.resolve(plan["plan_id"], remote(), state, evidence_file)
    for changes in ({"request_finished": False}, {"outcome": "unknown"}, {"plan_id": "wrong"}):
        evidence_file.write_text(json.dumps({**proof, **changes}))
        with pytest.raises(miro.SyncError, match="Evidence must prove"):
            miro.resolve(plan["plan_id"], remote(), state, evidence_file, retry_missing_inserts=True)
    evidence_file.write_text(json.dumps(proof))
    retry = miro.resolve(plan["plan_id"], remote(), state, evidence_file, retry_missing_inserts=True)
    assert retry["plan_id"] != plan["plan_id"]
    assert retry["rows"] == plan["rows"]
    with pytest.raises(miro.SyncError, match="plan_id"):
        miro.dispatch(plan["plan_id"], state)
    miro.dispatch(retry["plan_id"], state)
    miro.ack(retry["plan_id"], apply_plan(remote(), retry), state)
    saved = json.loads(state.read_text())
    assert saved["resolutions"][0]["path"] == str(evidence_file)
    assert len(saved["resolutions"][0]["sha256"]) == 64


def test_later_daily_projection_can_advance_same_event_revision(tmp_path):
    state = tmp_path / "state.json"
    original = snapshot()
    original["generated_at"] = "2026-10-09T12:01:00Z"
    plan = miro.prepare(original, remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(remote(), plan)
    miro.ack(plan["plan_id"], after, state)
    later = deepcopy(original)
    later["generated_at"] = "2026-10-10T12:01:00Z"
    later["acceptances"][0]["days"].append({"label": "10.10", "status": "unknown"})
    later["acceptances"][0]["status"] = "unknown"
    next_plan = miro.prepare(later, after, TABLE, state)
    miro.dispatch(next_plan["plan_id"], state)
    after_next = apply_plan(after, next_plan)
    miro.ack(next_plan["plan_id"], after_next, state)
    with pytest.raises(miro.SyncError, match="generated_at"):
        miro.prepare(original, after_next, TABLE, state)


def test_store_snapshot_round_trip_across_day_without_new_event(tmp_path):
    from scripts.acceptance import Store

    store = Store(tmp_path / "ledger", sources={"fotmob": {"name": "FotMob"}})
    store.register({
        "id": "fotmob-1288", "source": "fotmob", "issue": 1288,
        "title": "Дни приёмки", "issue_body_sha256": "a" * 64, "definition_confirmed": True,
        "started_at": "2026-10-01T00:00:00Z", "deployed_revision": "a1b2c3",
        "criteria": [{"id": "daily", "title": "Данные", "kind": "days",
                      "required": 3, "min_samples": 1, "period_seconds": 86400}],
    }, now="2026-10-01T00:00:00Z")
    initial = store.snapshot(now="2026-10-01T12:00:00Z")
    state = tmp_path / "state.json"
    plan = miro.prepare(initial, remote(), TABLE, state)
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(remote(), plan)
    miro.ack(plan["plan_id"], after, state)
    later = store.snapshot(now="2026-10-02T12:00:00Z")
    assert later["revision"] == initial["revision"]
    retry = miro.prepare(later, after, TABLE, state)
    values = {c["columnTitle"]: c["value"] for c in retry["rows"][0]["cells"]}
    assert values["Статус"] == "Нет подтверждения"
    assert "Нет подтверждения" in values["Дни и проверки"]


def source_snapshot(revision=1):
    return {
        "schema": 1, "revision": revision, "generated_at": "2026-10-09T12:01:00Z",
        "sources": {
            "fotmob": {"name": "FotMob", "miro_row_id": "fotmob-row"},
            "espn": {"name": "ESPN", "miro_row_id": "espn-row"},
            "fbref": {"name": "FBref", "miro_row_id": "fbref-not-in-table"},
        },
        "handoffs": {"fotmob": {
            "path": "/root/handoffs/fotmob-latest.md", "sha256": "a" * 64,
            "checked_at": "2026-10-08T10:00:00Z", "recorded_at": "2026-10-09T12:00:00Z",
            "summary": "Код доставлен; данные ожидают проверки", "done": "",
            "next_step": "Проверить полное окно", "status": "Приёмка",
        }},
    }


def source_remote():
    return {
        "miro_url": TABLE,
        "columns": [{"column_title": title, "column_type": "text"}
                    for title in miro.SOURCE_COLUMNS if title != "Сейчас"] + [{
                        "column_title": "Сейчас", "column_type": "select",
                        "selectOptions": [{"displayValue": "Приёмка"}, {"displayValue": "На паузе"}],
                    }, {"column_title": "Чужая заметка", "column_type": "text"}],
        "rows": [
            row("", "fotmob-row", {"Источник": "FotMob", "Чужая заметка": "keep"}),
            row("", "espn-row", {"Источник": "ESPN", "Уже сделано": "do not refresh"}),
        ], "total": 2, "cursor": None,
    }


def test_source_mode_updates_only_handoff_source_and_preserves_fact_time(tmp_path):
    state = tmp_path / "source-publication.json"
    initial = source_remote()
    plan = miro.prepare(source_snapshot(), initial, TABLE, state, mode="sources")
    assert len(plan["rows"]) == 1
    assert plan["rows"][0]["rowId"] == "fotmob-row"
    cells = {c["columnTitle"]: c["value"] for c in plan["rows"][0]["cells"]}
    assert set(cells) == {"Уже сделано", "Следующий шаг", "Проверено, МСК", "Сейчас"}
    assert cells["Уже сделано"] == "Код доставлен; данные ожидают проверки"
    assert cells["Проверено, МСК"] == ("Проверено: 08.10.2026 13:00:00 МСК\n"
                                     "handoff сохранён: 09.10.2026 15:00:00 МСК")
    saved = json.loads(state.read_text())
    assert saved["pending"]["mode"] == "sources"
    assert saved["pending"]["key_column"] == "Источник"
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(initial, plan)
    assert after["rows"][1] == initial["rows"][1]
    miro.ack(plan["plan_id"], after, state)
    assert miro.prepare(source_snapshot(), after, TABLE, state, mode="sources")["rows"] == []


@pytest.mark.parametrize("change", ["missing", "wrong_row_id", "wrong_config_id", "unsupported_status", "empty_next"])
def test_source_mode_requires_exact_existing_identity_and_supported_content(tmp_path, change):
    data = source_snapshot()
    readback = source_remote()
    if change == "missing":
        readback["rows"] = readback["rows"][1:]
        readback["total"] = len(readback["rows"])
    elif change == "wrong_row_id":
        readback["rows"][0]["rowId"] = "replacement"
    elif change == "wrong_config_id":
        data["sources"]["fotmob"]["miro_row_id"] = "wrong"
    elif change == "unsupported_status":
        data["handoffs"]["fotmob"]["status"] = "Принято без проверки"
    else:
        data["handoffs"]["fotmob"]["next_step"] = ""
    with pytest.raises(miro.SyncError):
        miro.prepare(data, readback, TABLE, tmp_path / "state.json", mode="sources")


def test_source_mode_fences_stale_handoff_even_with_new_global_revision(tmp_path):
    state = tmp_path / "state.json"
    initial = source_remote()
    plan = miro.prepare(source_snapshot(3), initial, TABLE, state, mode="sources")
    miro.dispatch(plan["plan_id"], state)
    after = apply_plan(initial, plan)
    miro.ack(plan["plan_id"], after, state)
    with pytest.raises(miro.SyncError, match="Stale snapshot"):
        miro.prepare(source_snapshot(2), after, TABLE, state, mode="sources")
    stale = source_snapshot(4)
    stale["handoffs"]["fotmob"]["checked_at"] = "2026-10-07T00:00:00Z"
    with pytest.raises(miro.SyncError, match="Stale handoff"):
        miro.prepare(stale, after, TABLE, state, mode="sources")


def test_source_reconciliation_uses_same_durable_lifecycle(tmp_path):
    state = tmp_path / "state.json"
    initial = source_remote()
    plan = miro.prepare(source_snapshot(), initial, TABLE, state, mode="sources")
    miro.dispatch(plan["plan_id"], state)
    retry = miro.reconcile(plan["plan_id"], initial, state)
    assert retry["rows"] == plan["rows"]
    miro.dispatch(retry["plan_id"], state)
    after = apply_plan(initial, retry)
    miro.ack(retry["plan_id"], after, state)
    with pytest.raises(miro.SyncError, match="another mode"):
        miro.prepare(snapshot(2), remote(), TABLE, state)
