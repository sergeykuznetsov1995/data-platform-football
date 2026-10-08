from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from scrapers.sofascore import history_controller as controller
from scrapers.sofascore import history_inventory as inventory
from dags.utils.sofascore_all_mens_state import CampaignPlanningError, _snapshot_digest
from scrapers.sofascore.denominator import Denominator, DenominatorRow
from tests.unit.utils.test_sofascore_all_mens_state import _season

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)


def snapshot(years=(2026, 2025, 2024, 2023), ids=(8, 17)):
    value = {"campaign_id": "history-test", "tournaments": [
        {"unique_tournament_id": tid, "capture_key": f"SS-{tid}", "metadata_status": "ready",
         "seasons": [_season(tid, year, "ready") for year in years]} for tid in ids]}
    value["snapshot_id"] = _snapshot_digest(value)
    return value


def denominator(ids=(8, 17), disputed=(), excluded=()):
    return Denominator({tid: DenominatorRow(tid, f"SS-{tid}", str(tid),
                                          "student" if tid in excluded else "amateur" if tid in disputed else "core",
                                          0 if tid in excluded else 9 if tid in disputed else 1, "test") for tid in ids})


def evidence(snap, totals=100, closed=0):
    return {"observed_at": NOW.isoformat(), "scopes": [
        {"tournament_id": t["unique_tournament_id"], "season_id": s["source_season_id"],
         "finished": totals, "closed": closed, "ongoing": i == 0, "schedule_complete": True}
        for t in snap["tournaments"] for i, s in enumerate(t["seasons"])]}


def run(tmp_path, snap, ev, *, run_id="run1", **kwargs):
    return controller.plan(snap, inventory=ev, checkpoint_path=tmp_path / "controller.json",
                           denominator=kwargs.pop("denominator", denominator(tuple(t["unique_tournament_id"] for t in snap["tournaments"]))),
                           dag_run_id=run_id, moment=NOW, release="test", **kwargs)


def group_closed(ev, season_suffix, closed):
    for row in ev["scopes"]:
        if row["season_id"] % 100 == season_suffix:
            row["closed"] = closed


def finish(tmp_path, run_id="run1"):
    controller.finalize(tmp_path / "controller.json", campaign_id="history-test", run_id=run_id)


def test_new_current_seasons_precede_old_completed_memory(tmp_path):
    snap = snapshot()
    plan = run(tmp_path, snap, evidence(snap), completed={"history-test:8:826"}, batch_size=10)
    assert [p["SOFASCORE_CANONICAL_SEASON"] for p in plan] == ["2627", "2627"]
    assert {p["SOFASCORE_HISTORY_GROUP"] for p in plan} == {"1"}


@pytest.mark.parametrize("one,two,expected", [(9899, 10000, "1"), (9900, 9899, "2"), (9900, 9900, "3")])
def test_99_percent_gate_checks_each_group_independently(tmp_path, one, two, expected):
    snap = snapshot()
    ev = evidence(snap, totals=10000)
    group_closed(ev, 26, one)
    group_closed(ev, 25, two)
    plan = run(tmp_path, snap, ev)
    assert plan[0]["SOFASCORE_HISTORY_GROUP"] == expected


def test_unknown_coverage_blocks_gate_even_when_known_matches_are_closed(tmp_path):
    snap = snapshot()
    ev = evidence(snap, closed=100)
    ev["scopes"][0]["schedule_complete"] = False
    plan = run(tmp_path, snap, ev)
    assert plan[0]["SOFASCORE_HISTORY_GROUP"] == "1"
    assert controller.read_summary(tmp_path / "controller.json", "history-test")["groups"]["1"]["unknown_scopes"] == 1


def test_new_early_debt_preempts_history_after_restart(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    group_closed(ev, 26, 100)
    group_closed(ev, 25, 100)
    assert run(tmp_path, snap, ev)[0]["SOFASCORE_HISTORY_GROUP"] == "3"
    finish(tmp_path)
    group_closed(ev, 26, 98)
    assert run(tmp_path, snap, ev, run_id="run2")[0]["SOFASCORE_HISTORY_GROUP"] == "1"


def test_each_tournament_has_ten_previous_seasons_not_ten_years():
    snap = snapshot(years=(2026, 2025, 2023, 2021, 2019, 2017, 2015, 2013, 2011, 2009, 2007, 2005, 2003))
    scopes = controller.classify(snap, evidence(snap), denominator())
    assert sum(s["group"] == "3" for s in scopes) == 20
    assert sum(s["group"] == "4" for s in scopes) == 2


def test_disputed_last_students_never_planned():
    snap = snapshot(ids=(8, 17, 22))
    scopes = controller.classify(snap, evidence(snap), denominator((8, 17, 22), disputed=(17,), excluded=(22,)))
    assert not any(s["tournament"]["unique_tournament_id"] == 22 for s in scopes)
    assert {s["group"] for s in scopes if s["tournament"]["unique_tournament_id"] == 17} == {"5"}


def test_restart_returns_same_plan_and_does_not_skip_wave(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    first = run(tmp_path, snap, ev)
    assert run(tmp_path, snap, evidence(snap, closed=100)) == first
    finish(tmp_path)
    second = run(tmp_path, snap, ev, run_id="run2")
    assert first[0]["SOFASCORE_TOURNAMENT_ID"] == "8"
    assert second[0]["SOFASCORE_TOURNAMENT_ID"] == "17"
    finish(tmp_path, "run2")
    assert run(tmp_path, snap, ev, run_id="run3")[0]["SOFASCORE_TOURNAMENT_ID"] == "8"


def test_outstanding_reservation_cannot_be_replaced(tmp_path):
    snap = snapshot()
    run(tmp_path, snap, evidence(snap))
    with pytest.raises(CampaignPlanningError, match="not finalized"):
        run(tmp_path, snap, evidence(snap), run_id="run2")


def test_finalize_is_idempotent_but_requires_exact_identity(tmp_path):
    snap = snapshot()
    run(tmp_path, snap, evidence(snap))
    finish(tmp_path)
    finish(tmp_path)
    with pytest.raises(CampaignPlanningError, match="mismatch"):
        finish(tmp_path, "wrong")


def test_corrupt_or_foreign_checkpoint_fails(tmp_path):
    path = tmp_path / "controller.json"
    path.write_text('{')
    with pytest.raises(CampaignPlanningError, match="checkpoint"):
        run(tmp_path, snapshot(), evidence(snapshot()))
    path.write_text(json.dumps({"schema_version": 1, "campaign_id": "other", "positions": {}}))
    with pytest.raises(CampaignPlanningError, match="campaign"):
        run(tmp_path, snapshot(), evidence(snapshot()))


def test_july_raw_first_bounded_and_separate_group(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    group_closed(ev, 26, 100)
    group_closed(ev, 25, 100)
    ev["scopes"][0]["july_match_ids"] = [str(i) for i in range(1, 31)]
    ev["scopes"][0]["july_raw_match_ids"] = ["30"]
    plan = run(tmp_path, snap, ev)
    assert plan[0]["SOFASCORE_HISTORY_GROUP"] == "july"
    ids = json.loads(plan[0]["SOFASCORE_HISTORY_MATCH_IDS_JSON"])
    assert len(ids) == 25 and ids[0] == "30"
    assert plan[0]["SOFASCORE_HISTORY_PHASE"] == "matches"
    assert plan[0]["SOFASCORE_HISTORY_SEASON_EVIDENCE"] == "bronze"


def test_missing_shape_is_configuration_error_not_permanent_deferred(tmp_path):
    snap = snapshot()
    with pytest.raises(CampaignPlanningError, match="static policy"):
        run(tmp_path, snap, evidence(snap), authorized_season_classes=[])


def test_missing_canonical_mapping_is_visible_and_not_closed(tmp_path):
    snap = snapshot()
    snap["tournaments"][0]["seasons"][0].update(canonical_season=None, season_format="unknown")
    snap["snapshot_id"] = _snapshot_digest(snap)
    run(tmp_path, snap, evidence(snap))
    groups = controller.read_summary(tmp_path / "controller.json", "history-test")["groups"]
    assert groups["1"]["mapping_required"] == 1
    assert groups["1"]["deferred"] == 0
    assert not groups["1"]["gate_passed"]


def test_manifest_terminal_alone_does_not_prove_publication():
    records = [{"key": {"source_tournament_id": "8", "source_season_id": "826", "target_type": "event",
                        "target_id": "1", "freshness_key": "final", "endpoint": ep}, "status": "not_supported"}
               for ep in inventory.ENDPOINTS]
    assert inventory.publication_complete(records, tournament_id=8, season_id=826, match_id="1", capture_complete=True)
    assert not inventory.publication_complete(records, tournament_id=8, season_id=826, match_id="1", capture_complete=False)
    records[0]["status"] = "schema_error"
    assert not inventory.publication_complete(records, tournament_id=8, season_id=826, match_id="1", capture_complete=True)
    assert not inventory.publication_complete(records, tournament_id=8, season_id=825, match_id="1", capture_complete=True)


def test_july_cohort_survives_partial_events_and_checks_exact_manifest_season():
    snap = snapshot()
    old = inventory.from_rows(snap, [], [(8, "2627", "7", False, False, 826, None)])
    assert old["scopes"][0]["july_match_ids"] == ["7"]
    partial = inventory.from_rows(snap, [], [(8, "2627", "7", True, True, 826, 825)], july_cohort=old["july_cohort"])
    assert partial["scopes"][0]["july_match_ids"] == ["7"]
    closed = inventory.from_rows(snap, [], [(8, "2627", "7", True, True, 826, 826)], july_cohort=old["july_cohort"])
    assert closed["july_closed"] == 1
    assert not closed["scopes"]


def test_scope_alias_collision_is_unresolved_not_arbitrary_source_id():
    snap = snapshot()
    duplicate = dict(snap["tournaments"][0]["seasons"][0], source_season_id=999)
    snap["tournaments"][0]["seasons"].append(duplicate)
    ev = inventory.from_rows(snap, [], [(8, "2627", "7", False, False, 826, 826)])
    assert ev["july_unresolved"] == 1 and ev["scopes"] == []


def test_inventory_uses_successful_season_phase_not_legacy_completed(tmp_path):
    (tmp_path / "bad.json").write_text('{')
    (tmp_path / "match.json").write_text(json.dumps({"tournament_id": 8, "source_season_id": 826, "phases": [{"phase": "matches", "status": "success"}]}))
    assert not inventory.verified_schedules(tmp_path)
    (tmp_path / "season.json").write_text(json.dumps({"tournament_id": 8, "source_season_id": 826, "phases": [{"phase": "season", "status": "success"}]}))
    assert inventory.verified_schedules(tmp_path) == {(8, 826)}


def test_read_adapter_performs_two_selects_and_always_closes(tmp_path):
    snap = snapshot()
    registry = tmp_path / "registry.json"
    registry.write_text('{"tournaments": []}')
    class Cursor:
        def __init__(self):
            self.sql = []
        def execute(self, sql, *parameters):
            self.sql.append(sql)
        def fetchall(self):
            return [(8, 826, 100, 50, 3)] if len(self.sql) == 1 else []
    class Connection:
        def __init__(self):
            self.reader, self.closed = Cursor(), False
        def cursor(self):
            return self.reader
        def close(self):
            self.closed = True
    conn = Connection()
    ev = inventory.collect(snap, result_dir=tmp_path, checkpoint_path=tmp_path / "state", registry_path=registry, connect=lambda: conn)
    assert conn.closed and len(conn.reader.sql) == 2
    assert ev["scopes"][0]["closed"] == 50
    assert all(sql.startswith('WITH') for sql in conn.reader.sql)


def test_retired_run_cannot_mint_another_plan_after_later_run(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    run(tmp_path, snap, ev)
    finish(tmp_path)
    run(tmp_path, snap, ev, run_id="run2")
    finish(tmp_path, "run2")
    with pytest.raises(CampaignPlanningError, match="retired"):
        run(tmp_path, snap, ev)


def test_checkpoint_json_corruption_cannot_change_reserved_targets(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    run(tmp_path, snap, ev)
    path = tmp_path / "controller.json"
    value = json.loads(path.read_text())
    value["run"]["plan"][0]["SOFASCORE_TOURNAMENT_ID"] = "99"
    path.write_text(json.dumps(value))
    with pytest.raises(CampaignPlanningError, match="digest"):
        run(tmp_path, snap, ev)


def test_parked_core_scope_remains_debt_and_blocks_gate(tmp_path):
    snap = snapshot(ids=(8,))
    ev = evidence(snap)
    failures = {"history-test:8:826": {"count": 3, "last_at": NOW.isoformat()}}
    planned = run(tmp_path, snap, ev, failures=failures)
    assert not planned
    groups = controller.read_summary(tmp_path / "controller.json", "history-test")["groups"]
    assert groups["1"]["parked"] == 1 and not groups["1"]["gate_passed"]


def test_quarantined_historical_scope_does_not_hold_deeper_group(tmp_path):
    snap = snapshot(years=tuple(range(2026, 2012, -1)), ids=(8,))
    ev = evidence(snap, closed=100)
    for row in ev["scopes"]:
        if row["season_id"] % 100 == 24:
            row["closed"] = 0
        if row["season_id"] % 100 < 15:
            row["closed"] = 0
    failures = {"history-test:8:824": {"count": 3, "streak_no_traffic": 3, "last_release": "test"}}
    planned = run(tmp_path, snap, ev, failures=failures)
    assert planned[0]["SOFASCORE_HISTORY_GROUP"] == "4"
    report = controller.read_summary(tmp_path / "controller.json", "history-test")
    assert report["groups"]["3"]["quarantined"] == 1
    assert report["groups"]["3"]["remaining"] == 100


def test_future_metadata_missing_core_cannot_vanish_from_99_denominator(tmp_path):
    snap = snapshot(ids=(8,))
    ev = evidence(snap, closed=100)
    planned = run(tmp_path, snap, ev, denominator=denominator())
    assert not planned
    report = controller.read_summary(tmp_path / "controller.json", "history-test")
    assert report["groups"]["1"]["unknown_scopes"] == 1


def test_summary_reports_actual_match_fraction_not_completed_scopes(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    group_closed(ev, 26, 60)
    run(tmp_path, snap, ev, completed={"history-test:8:826", "history-test:17:1726"})
    report = controller.read_summary(tmp_path / "controller.json", "history-test")
    assert report["groups"]["1"]["percent"] == 60
    assert "120/200 (60.00%)" in report["summary"]


def test_green_scope_with_held_rows_waits_for_parser_release(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    failures = {"history-test:8:826": {"completed_rejected_endpoints": 3, "last_release": "test"}}
    first = run(tmp_path, snap, ev, failures=failures, batch_size=10)
    assert [p["SOFASCORE_TOURNAMENT_ID"] for p in first] == ["17"]
    finish(tmp_path)
    failures["history-test:8:826"]["last_release"] = "old"
    second = run(tmp_path, snap, ev, failures=failures, batch_size=10, run_id="run2")
    assert {p["SOFASCORE_TOURNAMENT_ID"] for p in second} == {"8", "17"}


def test_saved_canonical_manifest_keeps_deferred_materialization_as_debt():
    from pathlib import Path
    path = Path(__file__).resolve().parents[2] / "fixtures/sofascore_scope_767_69577/manifest.json"
    records = json.loads(path.read_text())
    matches = {r["key"]["target_id"] for r in records}
    assert len(matches) == 3
    for mid in matches:
        assert not inventory.publication_complete(records, tournament_id=767, season_id=69577,
                                                  match_id=mid, capture_complete=True)


def test_offline_cli_runs_with_no_network_and_same_process_restart(tmp_path, monkeypatch):
    import socket
    from scripts.research.replay_sofascore_history import main
    snap = snapshot()
    inputs = {"snapshot": snap, "inventory": evidence(snap),
              "state": {"schema_version": 1, "campaign_id": "history-test", "completed": []}}
    args = []
    for name, value in inputs.items():
        path = tmp_path / (name + '.json')
        path.write_text(json.dumps(value))
        args.extend(['--' + name, str(path)])
    args.extend(['--checkpoint', str(tmp_path / 'checkpoint.json'), '--output', str(tmp_path / 'result.json')])
    def deny(*args, **kwargs):
        raise AssertionError("offline rehearsal attempted network")
    monkeypatch.setattr(socket.socket, 'connect', deny)
    assert main(args) == 0
    first = (tmp_path / 'result.json').read_bytes()
    assert main(args) == 0
    assert (tmp_path / 'result.json').read_bytes() == first
    assert json.loads(first)['source_request_count'] == 0


def test_verified_page_chains_survive_report_retention_in_checkpoint(tmp_path):
    snap = snapshot()
    ev = evidence(snap)
    ev["verified_schedules"] = [[8, 826]]
    run(tmp_path, snap, ev)
    checkpoint = controller.read_summary(tmp_path / "controller.json", "history-test")
    assert checkpoint["verified_schedules"] == [[8, 826]]
    rebuilt = inventory.from_rows(snap, [(8, 826, 100, 99, 0)], [],
                                  schedule_verified={tuple(pair) for pair in checkpoint["verified_schedules"]})
    assert rebuilt["scopes"][0]["schedule_complete"] is True
    assert rebuilt["scopes"][0]["ongoing"] is False


@pytest.mark.parametrize('metadata_status', ['ready', 'excluded'])
def test_empty_or_excluded_core_tournament_remains_unknown(tmp_path, metadata_status):
    snap = snapshot()
    snap['tournaments'][1].update(metadata_status=metadata_status, seasons=[])
    snap['snapshot_id'] = _snapshot_digest(snap)
    ev = evidence(snap, closed=100)
    for row in ev['scopes']:
        if row['season_id'] % 100 == 24:
            row['closed'] = 0
    planned = run(tmp_path, snap, ev)
    assert not planned
    report = controller.read_summary(tmp_path / 'controller.json', 'history-test')
    assert report['groups']['1']['unknown_scopes'] == 1
    assert report['groups']['2']['unknown_scopes'] == 1


def test_verified_empty_schedule_is_known_zero_not_an_endless_bootstrap(tmp_path):
    snap = snapshot(ids=(8,))
    rows = [(8, 825, 100, 100, 0), (8, 824, 100, 0, 0)]
    ev = inventory.from_rows(snap, rows, [], schedule_verified={(8, 826), (8, 825), (8, 824)})
    empty = next(row for row in ev['scopes'] if row['season_id'] == 826)
    assert empty['finished'] == empty['closed'] == 0
    assert empty['schedule_complete'] is True and empty['ongoing'] is False
    planned = run(tmp_path, snap, ev)
    assert all(p['SOFASCORE_SOURCE_SEASON_ID'] != '826' for p in planned)


def test_saved_july_cohort_absent_from_query_cannot_disappear_from_gate(tmp_path):
    snap = snapshot()
    ev = inventory.from_rows(snap, [], [], july_cohort=[('8', '826', '7')])
    assert ev['july_unresolved'] == 1
    ev['scopes'] = evidence(snap, closed=100)['scopes']
    for row in ev['scopes']:
        if row['season_id'] % 100 == 24:
            row['closed'] = 0
    assert run(tmp_path, snap, ev) == []
    report = controller.read_summary(tmp_path / 'controller.json', 'history-test')
    assert report['active_group'] == 'july'


def test_july_query_rereads_cohort_without_stats_timestamp_filter():
    _, sql = inventory.queries("('SS-8',8)", "(8,826,'7','2627')")
    assert 'saved_cohort' in sql
    assert 'UNION' in sql
    assert "(8,826,'7','2627')" in sql
    assert 'JOIN terminal' in sql
