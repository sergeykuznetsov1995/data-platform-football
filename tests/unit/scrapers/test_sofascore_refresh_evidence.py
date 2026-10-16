from datetime import datetime, timedelta

import pytest

from scrapers.sofascore import refresh_evidence as evidence


def bundle():
    identity = dict(run_id="scheduled__night", code_revision="a" * 40,
                    artifact_id="b" * 64, denominator_sha256="c" * 64,
                    snapshot_id="d" * 64, campaign_id="e" * 64,
                    logical_date="2026-10-15T00:30:00+00:00",
                    started_at="2026-10-15T00:30:01+00:00")
    rows = [[f"SS-{n}", "2627", 2, 1792024800, 1, n + 100] for n in range(1, 22)]
    selected = [dict(scope_key=f"scope-{n}", tournament_id=n, source_season_id=n + 100)
                for n in range(1, 9)]
    initial = evidence.planning_snapshot(identity, rows, selected,
                                        core_ids=list(range(1, 22)), input_complete=True,
                                        observed_at=identity["started_at"])
    final = evidence.planning_snapshot(identity, [], [], core_ids=list(range(1, 22)),
                                      input_complete=True, observed_at="2026-10-15T02:03:21+00:00")
    start = datetime.fromisoformat(identity["started_at"])
    outcomes = [dict(scope_key=f"scope-{n}", status="success", remaining_matches=0,
                     run_id=identity["run_id"],
                     started_at=(start + timedelta(seconds=(n - 1) * 700)).isoformat(),
                     finished_at=(start + timedelta(seconds=n * 700)).isoformat())
                for n in range(1, 9)]
    tail = dict(evidence=complete_tail())
    return dict(state="success", initial=initial, final=final, refills=[], outcomes=outcomes,
                attempt_history_complete=True,
                capture_window=dict(started_at=identity["started_at"],
                                    deadline_at="2026-10-15T04:30:01+00:00"),
                refill_result=dict(status="success", stop_reason="empty_queue"),
                tail=tail, expected_tail_leagues=sorted(complete_tail()["child"]["conf"]["competition_ids"]),
                expected_revision="a" * 40, window_budget_s=7200)


def test_complete_planning_census_keeps_21_due_despite_8_selected():
    item = bundle()
    assert item["initial"]["due_core_scopes"] == 21
    assert item["initial"]["due_core_matches"] == 42
    assert item["initial"]["overdue_core_matches"] == 21
    assert len(item["initial"]["selected"]) == 8
    assert evidence.assess_run(item)["status"] == "pass"


@pytest.mark.parametrize("field", ["initial", "final", "refill_result", "tail"])
def test_missing_evidence_never_passes(field):
    item = bundle()
    item.pop(field)
    assert evidence.assess_run(item)["status"] == "unknown"


def test_partial_demand_may_finish_only_with_accounted_time_budget():
    item = bundle()
    item["final"] = evidence.planning_snapshot(item["initial"]["identity"],
        [[f"SS-{n}", "2627", 2, 1792024800, 1, n + 100] for n in range(1, 22)], [],
        core_ids=list(range(1, 22)), input_complete=True, observed_at=item["final"]["observed_at"])
    item["refill_result"] = dict(status="partial", stop_reason="time_budget")
    result = evidence.assess_run(item)
    assert result["status"] == "pass"
    assert result["remaining_core_matches"] == 42
    item["refill_result"]["stop_reason"] = "match_cap"
    assert evidence.assess_run(item)["status"] == "fail"


def test_parallel_elapsed_times_are_unioned_not_summed():
    item = bundle()
    for row in item["outcomes"]:
        row["started_at"] = item["outcomes"][0]["started_at"]
        row["finished_at"] = "2026-10-15T00:40:01+00:00"
    result = evidence.assess_run(item)
    assert result["window_used_s"] == 600
    assert result["status"] == "fail"


@pytest.mark.parametrize("mutation", ["tail_missing", "tail_failed", "players", "fotmob", "revision",
                                      "partial_no_remaining", "no_timing", "failed_scope", "unknown_partition"])
def test_incomplete_or_failed_contract(mutation):
    item = bundle()
    if mutation == "tail_missing":
        item["tail"]["evidence"]["reports"].pop()
    elif mutation == "tail_failed":
        item["tail"]["evidence"]["reports"][0]["errors"] = ["writer failed"]
    elif mutation == "players":
        item["tail"]["evidence"]["child"]["conf"]["run_players"] = True
    elif mutation == "fotmob":
        item["tail"]["evidence"]["import_errors"]["errors"] = ["bad import"]
    elif mutation == "revision":
        item["final"]["identity"]["code_revision"] = "f" * 40
    elif mutation == "partial_no_remaining":
        item["outcomes"][0].update(status="partial", stop_reason="time_budget")
        item["outcomes"][0].pop("remaining_matches")
    elif mutation == "no_timing":
        item["outcomes"][0].pop("finished_at")
    elif mutation == "failed_scope":
        item["outcomes"][0]["status"] = "failed"
    else:
        item["initial"]["unclassified_partitions"] = 1
    assert evidence.assess_run(item)["status"] != "pass"


def test_empty_daily_lane_is_unknown_even_if_ss_high():
    assert evidence.assess_third_day(dict(delivered_at="2026-10-12T09:00:00Z",
        checked_at="2026-10-15T09:00:00Z", code_revision="a" * 40,
        lanes={"SS": dict(closed=100, total=100), "daily": dict(closed=0, total=0)}), "a" * 40)["status"] == "unknown"


def test_third_day_uses_original_calendar_and_both_lane_denominators():
    data = original_day_data()
    assert evidence.assess_third_day(data, "a" * 40)["status"] == "pass"
    data["day"] = "2026-09-28"
    assert evidence.assess_third_day(data, "a" * 40)["status"] == "unknown"


def test_immutable_record_replay_and_conflict(tmp_path):
    doc = bundle()["initial"]
    path = tmp_path / "plan.json"
    evidence.write_immutable(path, doc)
    evidence.write_immutable(path, doc)
    doc["due_core_matches"] += 1
    with pytest.raises(ValueError, match="immutable"):
        evidence.write_immutable(path, doc)


def test_historical_selected_plan_is_not_a_full_census():
    old = dict(run_id="old", planned_count=8, scopes=[{}] * 8,
               unplanned_due_core_denominator=None)
    assert evidence.assess_run(old)["status"] == "unknown"


def test_missing_expected_run_is_retained():
    result = evidence.assess_nights(["a", "b", "c"], {"a": bundle(), "c": bundle()})
    assert result["runs"][1]["run_id"] == "b"
    assert result["runs"][1]["status"] == "unknown"
    assert result["accepted_nights"] == 0


def night_bundle(day):
    item = bundle()
    run_id = f"scheduled__2026-10-{day:02}T00:30:00+00:00"
    for stage in ("initial", "final"):
        old = item[stage]
        identity = old["identity"].copy()
        identity.update(run_id=run_id, logical_date=f"2026-10-{day:02}T00:30:00+00:00",
                        started_at=f"2026-10-{day:02}T00:30:01+00:00")
        rows = [[f"SS-{r['tournament_id']}", r['canonical_season'], r['pending_matches'],
                 r['nearest_open_deadline'], r['open_matches'], r['source_season_id']]
                for r in old["partitions"]]
        item[stage] = evidence.planning_snapshot(identity, rows, old["selected"],
                    core_ids=list(range(1, 22)), input_complete=True,
                    observed_at=old["observed_at"].replace("10-15", f"10-{day:02}"))
    for row in item["outcomes"]:
        row.update(run_id=run_id, started_at=row["started_at"].replace("10-15", f"10-{day:02}"),
                   finished_at=row["finished_at"].replace("10-15", f"10-{day:02}"))
    item["tail"] = dict(evidence=complete_tail(day))
    item["capture_window"] = {k: v.replace("10-15", f"10-{day:02}")
                              for k, v in item["capture_window"].items()}
    return run_id, item


def test_three_complete_consecutive_nights_pass_and_gap_breaks_streak():
    rows = dict(night_bundle(day) for day in (15, 16, 17))
    assert evidence.assess_nights(list(rows), rows)["accepted_nights"] == 3
    assert evidence.assess_nights(list(rows), rows)["status"] == "pass"
    rows = dict(night_bundle(day) for day in (15, 17, 18))
    assert evidence.assess_nights(list(rows), rows)["accepted_nights"] == 2


def test_known_failed_scope_remains_failed_when_tail_is_missing():
    item = bundle()
    item["outcomes"][0]["status"] = "failed"
    item.pop("tail")
    assert evidence.assess_run(item)["status"] == "fail"


def test_full_input_deadline_order_is_checked_before_acceptance():
    item = bundle()
    rows = [[f"SS-{n}", "2627", 2, 1792024800 + n, 1, n + 100] for n in range(1, 22)]
    item["initial"] = evidence.planning_snapshot(item["initial"]["identity"], rows,
        list(reversed(item["initial"]["selected"])), core_ids=list(range(1, 22)), input_complete=True,
        observed_at=item["initial"]["observed_at"])
    assert evidence.assess_run(item)["status"] == "unknown"


def test_changed_or_incomplete_census_and_duplicate_native_scope_are_rejected():
    item = bundle()
    item["initial"]["input_complete"] = False
    assert evidence.assess_run(item)["status"] == "unknown"
    with pytest.raises(ValueError, match="duplicate native"):
        evidence.planning_snapshot({}, [["SS-1", "2627", 2, None, 0, 11]] * 2, [], core_ids=[1])


def test_cli_evaluates_saved_nights_without_network_or_production(tmp_path):
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    rows = dict(night_bundle(day) for day in (15, 16, 17))
    source, output = tmp_path / "input.json", tmp_path / "output.json"
    source.write_text(json.dumps(dict(expected_runs=list(rows), bundles=rows)))
    code = """import runpy,sys
def audit(event,args):
    if event.startswith(('socket.','subprocess.')): raise RuntimeError('network/process forbidden')
sys.addaudithook(audit)
sys.argv=sys.argv[1:]
runpy.run_path(sys.argv[0],run_name='__main__')
"""
    script = Path(__file__).resolve().parents[3] / "scripts/sofascore_refresh_evidence.py"
    result = subprocess.run([sys.executable, "-B", "-c", code, str(script),
                            "--input", str(source), "--output", str(output)],
                            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True)
    assert result.returncode == 0, result.stderr.decode()
    report = json.loads(output.read_text())
    assert report["accepted_nights"] == 3 and report["source_requests"] == 0
    assert report["acceptance_started"] is False


@pytest.mark.parametrize("state,status", [("failed", "fail"), ("running", "unknown"), (None, "unknown")])
def test_dag_run_must_be_terminal_success_even_when_all_scope_results_are_green(state, status):
    item = bundle()
    item["state"] = state
    assert evidence.assess_run(item)["status"] == status
    item.pop("initial")
    assert evidence.assess_run(item)["status"] == status


def test_different_release_nights_cannot_form_one_accepted_series():
    rows = dict(night_bundle(day) for day in (15, 16, 17))
    for revision, item in zip(("a", "b", "c"), rows.values()):
        item["expected_revision"] = revision * 40
        item["tail"] = dict(evidence=complete_tail(15, revision * 40))
        for name in ("initial", "final"):
            item[name]["identity"]["code_revision"] = revision * 40
            item[name]["digest"] = evidence._digest({k: v for k, v in item[name].items() if k != "digest"})
    result = evidence.assess_nights(list(rows), rows)
    assert result["status"] == "unknown" and result["accepted_nights"] == 0


def test_capture_window_starts_after_schedule_sweep_and_keeps_original_wall_deadline():
    item = bundle()
    for name in ("initial", "final"):
        doc = item[name]
        doc["observed_at"] = (datetime.fromisoformat(doc["observed_at"]) + timedelta(hours=2)).isoformat()
        doc["digest"] = evidence._digest({k: v for k, v in doc.items() if k != "digest"})
    for row in item["outcomes"]:
        for name in ("started_at", "finished_at"):
            row[name] = (datetime.fromisoformat(row[name]) + timedelta(hours=2)).isoformat()
    item["capture_window"] = dict(started_at=item["initial"]["observed_at"],
                                  deadline_at="2026-10-15T06:30:01+00:00")
    assert evidence.assess_run(item)["status"] == "pass"
    item["capture_window"]["deadline_at"] = "2026-10-15T03:00:00+00:00"
    assert evidence.assess_run(item)["status"] == "unknown"


def test_physical_retry_failures_are_retained_and_terminal_outcome_is_checked():
    item = bundle()
    first = item["outcomes"][0]
    failed = {**first, "status": "failed", "finished_at": "2026-10-15T00:31:01+00:00"}
    first["started_at"] = failed["finished_at"]
    item["outcomes"].insert(0, failed)
    result = evidence.assess_run(item)
    assert result["status"] == "pass"
    assert result["physical_attempts"] == 9 and result["failed_physical_attempts"] == 1
    assert result["physical_outcomes"][0]["status"] == "failed"
    first["status"] = "failed"
    assert evidence.assess_run(item)["status"] == "fail"


def test_duplicate_physical_attempt_or_incomplete_history_does_not_pass():
    item = bundle()
    item["outcomes"].append(dict(item["outcomes"][0]))
    assert evidence.assess_run(item)["status"] == "unknown"
    item = bundle()
    item["attempt_history_complete"] = False
    assert evidence.assess_run(item)["status"] == "unknown"


def original_day_data():
    return dict(day="2026-09-29", measured_at="2026-09-30T05:04:49+00:00",
                checked_at="2026-10-16T10:00:00Z", code_revision="a" * 40,
                delivered_at="2026-09-27T04:20:10Z",
                original_delivery=dict(code_revision="70446986046371e2adecd034dba66a83afdecf99",
                    delivered_at="2026-09-27T04:20:10Z",
                    artifact_id="fe05605ff876b99a41d7bae740913f41b9432c57737a6cc24ee24912688aec11"),
                lanes={"SS": dict(closed=95, total=100), "daily": dict(closed=9, total=10)})


def test_original_calendar_day_accepts_later_actual_measurement():
    result = evidence.assess_third_day(original_day_data(), "a" * 40)
    assert result["status"] == "pass"
    assert result["day"] == "2026-09-29"
    assert result["measured_at"] == "2026-09-30T05:04:49+00:00"


def test_new_recorder_delivery_cannot_reanchor_historical_deadline():
    data = original_day_data()
    data.update(day="2026-10-18", delivered_at="2026-10-15T20:04:01Z",
                measured_at="2026-10-18T20:04:01Z", checked_at="2026-10-18T21:00:00Z")
    assert evidence.assess_third_day(data, "a" * 40)["status"] == "unknown"


def test_seven_self_declared_complete_leagues_do_not_prove_publication():
    item = bundle()
    item["tail"] = dict(run_id="tail__20261015", night="2026-10-15", code_revision="a" * 40,
                        run_players=False, fotmob_import_errors=0,
                        leagues=[dict(league=name, status="success", capture_complete=True)
                                 for name in item["expected_tail_leagues"]])
    assert evidence.assess_run(item)["status"] == "unknown"



def complete_tail(day=15, revision="a" * 40):
    from scrapers.sofascore.tail_evidence import configured_tournaments, _task

    configured = configured_tournaments()
    run_id = f"tail__202610{day:02}T002000"
    start, finish = f"2026-10-{day:02}T00:20:01Z", f"2026-10-{day:02}T00:30:00Z"
    child = dict(dag_id="dag_ingest_sofascore", run_id=run_id, code_revision=revision,
                 state="success", start_date=start, end_date=finish, logical_date=start,
                 conf=dict(run_players=False, competition_ids=sorted(configured)))
    parent = dict(dag_id="dag_trigger_sofascore_daily_tail", run_id="scheduled__" + start,
                  child_run_id=run_id, code_revision=revision, state="success")
    tasks, reports = [], []
    for league, tournament in configured.items():
        for prefix in ("scrape_sofascore_data", "scrape_match_capture", "scrape_player_capture"):
            tasks.append(dict(task_id=_task(prefix, league), dag_id=child["dag_id"], run_id=run_id,
                              state="skipped" if "player" in prefix else "success", try_number=1))
        for phase in ("season", "match"):
            identity = dict(dag_id=child["dag_id"], run_id=run_id, code_revision=revision,
                            logical_date=start, started_at=start, finished_at=finish,
                            league=league, source_tournament_id=tournament,
                            source_season_id=tournament + 100, season="2627", try_number=1)
            report = dict(evidence_identity=identity, errors=[], endpoint_completeness=1.0,
                          manifest_committed=True, publication_receipts=[])
            if phase == "season":
                report.update(entity="all", schedule_rows=10, league_table_rows=0)
                table, rows = "sofascore_schedule", 10
            else:
                report.update(entity="match_capture", capture_status_rows=5, matches_total=5,
                              matches_complete=5, matches_skipped_existing=0)
                table, rows = "sofascore_match_capture_status", 5
            report["tables"] = ["iceberg.bronze." + table]
            report["publication_receipts"].append(dict(table=table, rows=rows, location="iceberg.bronze." + table))
            reports.append(report)
    return dict(parent=parent, child=child, task_instances=tasks, reports=reports,
                import_errors=dict(isolate="fotmob", code_revision=revision,
                                   checked_at=finish, errors=[]))


def test_null_final_snapshot_retains_failed_run_instead_of_crashing():
    item = bundle()
    item.update(state="failed", final=None)
    result = evidence.assess_run(item)
    assert result["status"] == "fail"
    assert result["reasons"]
