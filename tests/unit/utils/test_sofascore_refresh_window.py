from __future__ import annotations

from utils.sofascore_refresh_window import refill_window


def _harness(counts, durations, *, clock_start=1000):
    clock = [float(clock_start)]
    index = [0]
    calls = []
    def pending():
        return [("SS-8", "2026", counts[index[0]], None, 0, 825)] if counts[index[0]] else []
    def plan(rows, budget, round_number):
        calls.append((rows, budget, round_number))
        return {"SOFASCORE_SCOPE_KEY": "c:8:825", "SOFASCORE_TOURNAMENT_ID": "8",
                "SOFASCORE_CANONICAL_SEASON": "2026", "SOFASCORE_SOURCE_SEASON_ID": "825",
                "SOFASCORE_SCOPE_RUN_ID": f"run:refill:{round_number}"}
    def execute(env):
        elapsed = durations[index[0]]
        clock[0] += elapsed
        index[0] += 1
        return {"status": "partial", "elapsed_s": elapsed, "stop_reason": "match_cap",
                "scope_key": env["SOFASCORE_SCOPE_KEY"]}
    return clock, calls, pending, plan, execute


def test_fast_replay_refills_actual_remaining_budget_and_replans_progress():
    clock, calls, pending, plan, execute = _harness([593, 400, 0], [1700, 1600])
    result = refill_window(initial_elapsed_s=2539.3, deadline_epoch=15000,
                           pending=pending, plan=plan, execute=execute, now=lambda: clock[0])
    assert [round(c[1], 1) for c in calls] == [4660.7, 2960.7]
    assert [c[2] for c in calls] == [1, 2]
    assert result['stop_reason'] == 'empty_queue'
    assert result['elapsed_s'] == 3300
    assert len(result['outcomes']) == 2


def test_no_progress_blocks_scope_instead_of_new_paid_identity_loop():
    clock, calls, pending, plan, execute = _harness([593, 593], [50])
    result = refill_window(initial_elapsed_s=0, deadline_epoch=15000,
                           pending=pending, plan=plan, execute=execute, now=lambda: clock[0])
    assert len(calls) == 1
    assert result['stop_reason'] == 'no_progress'
    assert result['blocked_scopes'] == ['c:8:825']


def test_wall_deadline_limits_budget_even_after_long_pool_wait():
    clock, calls, pending, plan, execute = _harness([593, 0], [50], clock_start=14400)
    refill_window(initial_elapsed_s=2539.3, deadline_epoch=15000,
                  pending=pending, plan=plan, execute=execute, now=lambda: clock[0])
    assert calls[0][1] == 600


def test_expired_deadline_or_spent_budget_performs_no_work():
    for elapsed, deadline in [(0, 900), (7200, 15000)]:
        clock, calls, pending, plan, execute = _harness([593], [])
        result = refill_window(initial_elapsed_s=elapsed, deadline_epoch=deadline,
                               pending=pending, plan=plan, execute=execute, now=lambda: clock[0])
        assert calls == []
        assert result['stop_reason'] in ('wall_deadline', 'time_budget')


def test_round_limit_is_finite_even_if_source_keeps_growing():
    clock, calls, pending, plan, execute = _harness([900, 800, 700], [1, 1])
    result = refill_window(initial_elapsed_s=0, deadline_epoch=15000, max_rounds=2,
                           pending=pending, plan=plan, execute=execute, now=lambda: clock[0])
    assert len(calls) == 2
    assert result['stop_reason'] == 'round_cap'


def test_failed_scope_remains_failed_and_stops_refill():
    clock, calls, pending, plan, _ = _harness([593], [])
    result = refill_window(initial_elapsed_s=0, deadline_epoch=15000,
                           pending=pending, plan=plan,
                           execute=lambda env: {'status': 'failed', 'errors': ['bad season']},
                           now=lambda: clock[0])
    assert result['status'] == 'failed'
    assert result['stop_reason'] == 'scope_failure'
    assert result['outcomes'][0]['errors'] == ['bad season']


def test_real_planner_preserves_urgent_order_and_new_signed_plan_identity():
    import hashlib
    import json
    from utils.sofascore_all_mens_state import plan_refresh_batch

    snapshot = {'campaign_id':'c', 'tournaments': [
        {'unique_tournament_id':tid,'capture_key':f'SS-{tid}', 'metadata_status':'ready',
         'seasons':[{'source_season_id':tid*100+25,'canonical_season':'2026',
                     'start_year':2026,'metadata_status':'ready'}]}
        for tid in (8, 17)]}
    snapshot['snapshot_id'] = hashlib.sha256(json.dumps(
        snapshot,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    rows = [[('SS-8','2026',593,None,0,825), ('SS-17','2026',2,2000,2,1725)],
            [('SS-8','2026',593,None,0,825)], []]
    envs = []
    def pending():
        return rows.pop(0)
    def plan(current, budget, round_number):
        env = plan_refresh_batch(snapshot,current,batch_size=1,queue_mode="deadline",scope_budget_s=int(budget),
                                 dag_run_id=f'run:refill:{round_number}')[0]
        envs.append(env)
        return env
    result = refill_window(initial_elapsed_s=2539.3,deadline_epoch=15000,pending=pending,
                           plan=plan,now=lambda:1000,
                           execute=lambda env:{'status':'refreshed','elapsed_s':100})
    assert [e['SOFASCORE_TOURNAMENT_ID'] for e in envs] == ['17','8']
    original = plan_refresh_batch(snapshot,[('SS-8','2026',593,None,0,825)],
                                 batch_size=1,queue_mode="deadline",scope_budget_s=7200,dag_run_id='run')[0]
    assert envs[1]['SOFASCORE_SCOPE_RESULT_PATH'] != original['SOFASCORE_SCOPE_RESULT_PATH']
    assert envs[1]['SOFASCORE_SCOPE_RUN_ID'] != original['SOFASCORE_SCOPE_RUN_ID']
    assert result['stop_reason'] == 'empty_queue'


def test_night_refill_preserves_delivery_reserve_and_utc_boundary():
    from datetime import datetime, timezone, timedelta
    from utils.sofascore_refresh_window import refill_deadline
    start = datetime(2026,10,2,0,30,tzinfo=timezone.utc)
    four = start.replace(hour=4,minute=0).timestamp()
    assert refill_deadline(four+3600, start) == four
    assert refill_deadline(four-60, start) == four-60
    assert refill_deadline(four+3600, start.astimezone(timezone(timedelta(hours=2)))) == four
    daytime = start.replace(hour=8)
    assert refill_deadline(daytime.timestamp()+14400,daytime) == daytime.timestamp()+14400


def test_stalled_scope_does_not_block_another_due_partition():
    rows = [('SS-8','2026',2,2000,2,825), ('SS-17','2026',1,3000,1,1725)]
    visited = []
    def plan(current, budget, round_number):
        row = current[0]
        tid = row[0].split('-')[1]
        return {'SOFASCORE_SCOPE_KEY':f'c:{tid}:{row[5]}',
                'SOFASCORE_TOURNAMENT_ID':tid,'SOFASCORE_CANONICAL_SEASON':'2026',
                'SOFASCORE_SOURCE_SEASON_ID':str(row[5])}
    def execute(env):
        tid = env['SOFASCORE_TOURNAMENT_ID']
        visited.append(tid)
        if tid == '17':
            rows.pop()
        return {'status':'partial','elapsed_s':1}
    result = refill_window(initial_elapsed_s=0,deadline_epoch=15000,pending=lambda:list(rows),
                           plan=plan,execute=execute,now=lambda:1000)
    assert visited == ['8','17']
    assert result['stop_reason'] == 'no_progress'
    assert result['blocked_scopes'] == ['c:8:825']
