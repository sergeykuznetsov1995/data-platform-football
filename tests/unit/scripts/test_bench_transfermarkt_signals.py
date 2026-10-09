"""Meaningful guards for the bounded source-only #1393 experiment."""
from argparse import Namespace
from datetime import date, datetime, timezone
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from dags.scripts import bench_transfermarkt_signals as bench


def test_absolute_cli_deadline_interrupts_a_stuck_call_in_real_process():
    code = '''import signal, time
from dags.scripts.bench_transfermarkt_signals import _cli_portion_alarm, ProbeGateError
old = signal.getsignal(signal.SIGALRM)
try:
    with _cli_portion_alarm(.02):
        time.sleep(2)
    raise AssertionError('blocking call escaped the absolute alarm')
except ProbeGateError as exc:
    assert 'wall clock deadline' in str(exc)
assert signal.getsignal(signal.SIGALRM) == old
assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
'''
    completed = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=4)
    assert completed.returncode == 0, completed.stderr


@pytest.fixture(autouse=True)
def outside_delivery_window(monkeypatch):
    monkeypatch.setattr(bench, "_now_utc", lambda: datetime(2026, 10, 8, 12, tzinfo=timezone.utc))


def test_player_batches_are_bounded_and_encoded():
    assert bench.player_url(["40104", "8198"]).endswith("ids%5B%5D=40104&ids%5B%5D=8198")
    for ids in ([], ["1"] * 2, [str(i) for i in range(1, 302)], ["0"], ["1&a=2"]):
        with pytest.raises(bench.ProbeGateError):
            bench.player_url(ids)


def tmapi_row(player="1", *, value=None, contract=None, club="281", value_date=None):
    result = {"id": player, "attributes": {"contractUntil": contract,
               "lastContractRenewal": {"year": None, "month": None, "day": None}},
              "clubAssignments": [{"clubId": club, "type": "current", "shirtNumber": None, "isCaptain": False}]}
    if value is not None:
        result["marketValueDetails"] = {"current": {"value": value, "currency": "EUR", "determined": value_date}}
    return result


def probe_body(url):
    from urllib.parse import urlsplit, parse_qs
    ids = parse_qs(urlsplit(url).query)["ids[]"]
    return json.dumps({"success": True, "data": [tmapi_row(player) for player in ids]}).encode()


def test_tmapi_batch_requires_identity_and_explicit_required_fields():
    rows = [tmapi_row()]
    result = bench.parse_players({"success": True, "data": rows}, ["1"])
    assert result["1"].market_value_eur is None
    for payload in ({"success": False, "data": rows}, {"success": True, "data": []},
                    {"success": True, "data": rows * 2},
                    {"success": True, "data": [{"id": 1, "clubAssignments": []}]}):
        with pytest.raises(bench.ProbeGateError):
            bench.parse_players(payload, ["1"])


def test_task_planner_reuses_careers_for_multiclub_player_and_batch300():
    sample = {"clubs": [{"club_id": "281", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"], "player_ids": [str(i) for i in range(1, 302)]},
                         {"club_id": "583", "saison_id": 2026, "scope": "FR1/2026", "player_ids": ["1"]}]}
    tasks, players, groups = bench.plan_tasks(sample)
    assert len(players) == 301
    assert "top_league" in groups
    assert [len(task["ids"]) for task in tasks if task["kind"] == "players"] == [300, 1]
    assert len([task for task in tasks if task["key"] == "mv/1"]) == 1
    assert all("/plus/1" in task["url"] for task in tasks if task["kind"] == "squad")


class FakeClient:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.calls = []
        self._raw_store = SimpleNamespace(load_capture=lambda capture: (probe_body(self.calls[-1][0]), SimpleNamespace(url=self.calls[-1][0], status_code=200)))
        self.closed = False

    def fetch(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.outcomes)

    def close(self):
        self.closed = True

    def get_traffic_stats(self):
        return {"request_attempts": len(self.calls), "provider_metered_bytes": len(self.calls) * 10,
                "decoded_response_body_bytes": len(self.calls) * 20, "provider_metering_available": True}

    def get_raw_attempt_records(self):
        return [{"capture_id": "raw", "status_code": 405} for _ in self.calls]


def args(tmp_path, **changes):
    values = dict(mode="probe", ids="40104,8198", sample=None, field_map=None,
                  evidence_dir=str(tmp_path / "day1"), experiment_dir=str(tmp_path),
                  max_attempts=4600, max_bytes=256 * bench.MIB, portion_seconds=2700, endpoint_attempts=2)
    values.update(changes)
    return Namespace(**values)


def test_probe_is_one_get_raw_only_and_keeps_gate_unconfirmed(tmp_path):
    client = FakeClient([FetchOutcome(status=FetchStatus.OK, raw_capture_id="raw", attempts=1)])
    assert bench.run(args(tmp_path), client_factory=lambda *a: client) == 0
    assert client.closed and len(client.calls) == 1
    assert client.calls[0][1]["max_attempts"] == 1
    report = json.loads((tmp_path / "day1/report.json").read_text())
    assert report["detector_gate"] is False
    assert report["experiment_totals"]["request_attempts"] == 1
    assert report["shape"]["data"]["type"] == "list"
    assert json.loads((tmp_path / "day1/state.json").read_text())["attempt_records"]


def test_attempts_and_paid_bytes_remain_global_across_days(tmp_path):
    for day in ("day1", "day2"):
        client = FakeClient([FetchOutcome(status=FetchStatus.OK, raw_capture_id="raw", attempts=1)])
        bench.run(args(tmp_path, evidence_dir=str(tmp_path / day)), client_factory=lambda *a: client)
    totals = json.loads((tmp_path / "signal-probe-ledger.json").read_text())["totals"]
    assert totals == {"request_attempts": 2, "provider_metered_bytes": 20, "decoded_response_body_bytes": 40}


def test_interruption_refuses_unmetered_resume_without_any_fetch(tmp_path):
    (tmp_path / "signal-probe-ledger.json").write_text(json.dumps({"active": True}))
    with pytest.raises(bench.ProbeGateError, match="reconcile"):
        bench.run(args(tmp_path), client_factory=lambda *a: pytest.fail("must not construct network client"))


def test_failed_attempt_retains_envelope_and_can_retry_within_global_limit(tmp_path):
    first = FakeClient([FetchOutcome(status=FetchStatus.BLOCKED, status_code=405, attempts=1)])
    assert bench.run(args(tmp_path), client_factory=lambda *a: first) == 3
    second = FakeClient([FetchOutcome(status=FetchStatus.OK, raw_capture_id="raw", attempts=1)])
    assert bench.run(args(tmp_path), client_factory=lambda *a: second) == 0
    state = json.loads((tmp_path / "day1/state.json").read_text())
    assert state["tries"]["probe"] == 2 and len(state["attempt_records"]) == 2
    third = FakeClient([])
    with pytest.raises(bench.ProbeGateError, match="budget exhausted"):
        bench.run(args(tmp_path, evidence_dir=str(tmp_path / "day2"), max_attempts=2), client_factory=lambda *a: third)
    assert not third.calls


def test_parity_gate_reports_contract_value_club_mismatch(monkeypatch):
    import scrapers.transfermarkt.scraper as scraper
    monkeypatch.setattr(scraper, "_parse_squad_page", lambda *a: [{"player_id": "1", "contract_until": date(2028, 6, 30), "market_value_eur": 100}])
    tasks = [{"key": "s", "kind": "squad", "ids": ["1"], "club_id": "281", "scope": "GB1/2026", "url": "s", "json": False},
             {"key": "p", "kind": "players", "ids": ["1"], "url": "p", "json": True},
             {"key": "m", "kind": "mv", "player_id": "1", "url": "m", "json": True},
             {"key": "t", "kind": "transfers", "player_id": "1", "url": "t", "json": True}]
    bodies = {"s": b'<table class="items"><thead><th>Contract</th></thead></table>', "p": json.dumps({"success": True, "data": [tmapi_row(value=200, contract="2027-06-30")]}).encode(),
              "m": b'{"list":[]}', "t": b'{"transfers":[]}'}
    store = SimpleNamespace(load_capture=lambda key: (bodies[key], SimpleNamespace(url=key, status_code=200, fetched_at="2026-10-09T00:00:00Z")))
    report = bench.parity_report(tasks, {key: {"raw_capture_id": key} for key in bodies}, store, ["1"], bench.GROUPS)
    assert report["field_parity_gate"] is False
    assert report["detector_gate"] is False
    assert report["mismatches"][0]["issues"] == ["tmapi_value_vs_squad", "tmapi_contract_vs_squad"]


SQUAD = b'''<table class="items"><thead><tr><th>#</th><th>Player</th>
<th>Date of birth/Age</th><th>Nat.</th><th>Height</th><th>Foot</th>
<th>Joined</th><th>Signed from</th><th>Contract</th><th>Market value</th>
</tr></thead><tbody><tr><td class="zentriert">1</td>
<td class="posrela"><table class="inline-table"><tr><td class="hauptlink">
<a href="/david-raya/profil/spieler/262749">David Raya</a></td></tr>
<tr><td>Goalkeeper</td></tr></table></td><td class="zentriert">Sep 15, 1995 (30)</td>
<td class="zentriert"><img title="Spain"/></td><td class="zentriert">1,86m</td>
<td class="zentriert">right</td><td class="zentriert">Jul 4, 2024</td>
<td class="zentriert"><img title="Brentford FC"/></td>
<td class="zentriert">Jun 30, 2028</td><td class="rechts hauptlink">\xe2\x82\xac30.00m</td>
</tr></tbody></table>'''


def real_parity_evidence(tmp_path):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    sample = {"clubs": [{"club_id": "11", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"], "player_ids": ["262749"]}]}
    tasks, players, groups = bench.plan_tasks(sample)
    payloads = {"squad": SQUAD, "players": json.dumps({"success": True, "data": [tmapi_row("262749", value=30000000, contract="2028-06-30", club="11", value_date="2026-10-09")]}).encode(),
                "mv": b'{"list":[{"datum_mw":"Oct 9, 2026","y":30000000}]}',
                "transfers": b'{"transfers":[{"date":"Jul 4, 2024","season":"24/25","upcoming":false,"from":{"clubName":"Brentford","href":"/brentford/startseite/verein/1148"},"to":{"clubName":"Arsenal","href":"/arsenal/startseite/verein/11"}}, {"date":"Oct 9, 2026","season":"26/27","upcoming":true,"from":{"clubName":"Arsenal","href":"/arsenal/startseite/verein/11"},"to":{"clubName":"Other","href":"/other/startseite/verein/22"}}]}'}
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    results = {}
    for index, task in enumerate(tasks):
        capture = store.store_attempt(url=task["url"], body=payloads[task["kind"]], status_code=200,
                                      headers={"Content-Type": "application/json" if task["json"] else "text/html"},
                                      fetched_at="2026-10-08T23:10:00+00:00", cycle_id="parity-test", scope_id=task["scope"], endpoint=task["kind"], attempt=index + 1)
        store.store_response_envelope(capture)
        results[task["key"]] = {"raw_capture_id": capture.capture_id}
    return tasks, players, groups, store, results


def test_real_raw_store_and_source_parsers_roundtrip(tmp_path):
    tasks, players, groups, store, results = real_parity_evidence(tmp_path)
    report = bench.parity_report(tasks, results, store, players, groups)
    assert report["mismatches"] == []
    assert report["comparisons"][0]["mv_rows"] == 1
    assert report["comparisons"][0]["transfer_rows"] == 2
    assert report["comparisons"][0]["fields"] == {"value": 30000000, "value_date": "2026-10-09", "value_present": True, "contract": "2028-06-30", "clubs": ("11",)}
    assert not report["field_parity_gate"]  # One player proves parsing, not cohort acceptance.
    assert len(list((tmp_path / "raw/attempts").rglob("*.json"))) == 4


def test_real_raw_corruption_does_not_establish_parity(tmp_path):
    from scrapers.transfermarkt.raw_store import RawCaptureCorrupt
    tasks, players, groups, store, results = real_parity_evidence(tmp_path)
    _, record = store.load_capture(results[tasks[0]["key"]]["raw_capture_id"])
    Path(store.root, record.blob_key).write_bytes(b"corrupt")
    with pytest.raises(RawCaptureCorrupt):
        bench.parity_report(tasks, results, store, players, groups)


def test_past_roster_without_contract_column_does_not_prove_null_parity(tmp_path):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    tasks, players, groups, store, results = real_parity_evidence(tmp_path)
    task = tasks[0]
    record = store.store_attempt(url=task["url"], body=SQUAD.replace(b"<th>Contract</th>", b"<th>Current club</th>"), status_code=200,
                                headers={}, fetched_at="2026-10-08T23:11:00+00:00", cycle_id="bad-header", scope_id=task["scope"], endpoint=task["kind"], attempt=1)
    results[task["key"]] = {"raw_capture_id": record.capture_id}
    with pytest.raises(bench.ProbeGateError, match="contract header"):
        bench.parity_report(tasks, results, store, players, groups)


def transport_factory(tmp_path, *, status=200, close_error=False, responses=None):
    from dataclasses import replace
    from scrapers.transfermarkt.client import TransfermarktHttpClient
    from scrapers.transfermarkt.models import LeaseTrafficSnapshot, ProxyLease, SharedTrafficLedger
    from scrapers.transfermarkt.raw_store import RawResponseStore
    factory = SimpleNamespace(calls=[], bytes=0, leases=[])

    class Provider:
        def acquire(self, *, max_bytes, ttl_seconds, metadata):
            lease = ProxyLease("offline-lease", "offline-token", "http://offline-proxy:8900", max_bytes, 9999999999)
            factory.leases.append(lease)
            return lease
        def stats(self, lease):
            return LeaseTrafficSnapshot(up_bytes=256 * len(factory.calls), down_bytes=factory.bytes)
        def close(self, lease):
            if close_error:
                raise RuntimeError("unavailable accounting")
            return replace(self.stats(lease), closed=True)
        def acquire_request_permit(self, *, metadata, request_id):
            return "offline-permit"
        def authenticated_proxy_url(self, lease):
            return "http://offline-proxy:8900"

    class TLS:
        def get(self, url, **kwargs):
            response_status, body = responses[url] if responses is not None else (status, probe_body(url))
            factory.calls.append(url)
            factory.bytes += len(body)
            return SimpleNamespace(status_code=response_status, content=body, headers={"Content-Length": str(len(body))})
        def close(self):
            pass

    def build(evidence, remaining, portion_id):
        client = TransfermarktHttpClient(lease_provider=Provider(), raw_store=RawResponseStore.from_uri((evidence / "raw").as_uri()),
                                         require_raw_store=True, traffic_ledger=SharedTrafficLedger(hard_provider_bytes=remaining["provider_metered_bytes"], soft_provider_bytes=remaining["provider_metered_bytes"] - bench.MIB),
                                         client_factory=lambda **kwargs: TLS(), sleep_fn=lambda seconds: None)
        client.begin_request_scope(request_attempt_budget=remaining["request_attempts"])
        client.set_cycle_decoded_body_budget(remaining["decoded_response_body_bytes"])
        return client
    return build, factory


def test_real_transport_proxy_raw_probe_chain_with_exact_paid_attempt(tmp_path):
    build, factory = transport_factory(tmp_path)
    assert bench.run(args(tmp_path, ids="8198", max_attempts=1), client_factory=build) == 0
    assert len(factory.calls) == 1 and len(factory.leases) == 1
    report = json.loads((tmp_path / "day1/report.json").read_text())
    assert report["traffic"]["request_attempts"] == 1
    assert report["experiment_totals"]["provider_metered_bytes"] == factory.bytes + 256
    assert report["shape"]["data"]["item"]["id"] == "str"
    assert report["packet_schema_gate"] is True
    state = json.loads((tmp_path / "day1/state.json").read_text())
    assert state["attempt_records"][0]["status_code"] == 200
    assert state["attempt_records"][0]["url"] == factory.calls[0]


def test_real_transport_blocked_attempt_keeps_raw_and_counts_bytes(tmp_path):
    build, factory = transport_factory(tmp_path, status=405)
    assert bench.run(args(tmp_path, ids="8198", max_attempts=1), client_factory=build) == 3
    state = json.loads((tmp_path / "day1/state.json").read_text())
    report = json.loads((tmp_path / "day1/report.json").read_text())
    assert len(factory.calls) == 1
    assert not state["results"] and state["attempt_records"][0]["status_code"] == 405
    assert report["experiment_totals"]["request_attempts"] == 1
    with pytest.raises(bench.ProbeGateError, match="budget exhausted"):
        bench.run(args(tmp_path, ids="8198", max_attempts=1), client_factory=build)


def test_real_transport_failed_close_leaves_interrupted_marker(tmp_path):
    build, factory = transport_factory(tmp_path, close_error=True)
    with pytest.raises(RuntimeError, match="accounting"):
        bench.run(args(tmp_path, ids="8198", max_attempts=1), client_factory=build)
    assert json.loads((tmp_path / "signal-probe-ledger.json").read_text())["active"]
    with pytest.raises(bench.ProbeGateError, match="reconcile"):
        bench.run(args(tmp_path), client_factory=lambda *a: pytest.fail("must not resume"))


def test_portion_stops_before_45minutes_without_starting_paid_request(tmp_path):
    build, factory = transport_factory(tmp_path)
    clock = iter([0.0, 2600.0])
    assert bench.run(args(tmp_path), client_factory=build, monotonic=lambda: next(clock)) == 3
    assert not factory.calls
    assert not json.loads((tmp_path / "signal-probe-ledger.json").read_text())["active"]


def test_cohort_templates_need_current_club_page_and_do_not_reuse_old_player_ids():
    sample = {"clubs": [{"club_id": "11", "saison_id": 2025, "scope": "ARG2/2025", "groups": ["top_league"], "player_ids": ["obsolete"]}]}
    tasks, players, _ = bench.plan_tasks(sample, roster_only=True)
    assert len(tasks) == 2 and not players and not tasks[1]["ids"]
    assert tasks[0]["kind"] == "participants"
    assert tasks[1]["url"].endswith("/kader/verein/11/plus/1")
    sample["clubs"][0]["squad_url"] = "https://www.transfermarkt.com/arsenal/kader/verein/11/saison_id/2025/plus/1"
    with pytest.raises(bench.ProbeGateError, match="current club URL"):
        bench.plan_tasks(sample, roster_only=True)


def test_bootstrap_uses_fresh_raw_roster_and_declares_scope_vs_club_season(tmp_path):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    sample = {"clubs": [{"club_id": "11", "saison_id": 2025, "scope": "ARG2/2025", "groups": ["top_league"]}]}
    tasks, _, _ = bench.plan_tasks(sample, roster_only=True)
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    body = b'<select name="saison_id"><option value="2026" selected>26/27</option></select>' + SQUAD
    record = store.store_attempt(url=tasks[1]["url"], body=body, status_code=200, headers={}, fetched_at="2026-10-08T23:10:00+00:00", cycle_id="boot", scope_id="ARG2/2025", endpoint="squad", attempt=1)
    report = bench.cohort_report(tasks, bootstrap_results(tasks, store, record), store)
    assert report["clubs"][0]["player_ids"] == ["262749"]
    assert report["clubs"][0]["saison_id"] == 2025
    assert report["clubs"][0]["club_selected_saison_id"] == "2026"
    assert report["cohort_notes"][1]["scope_season_matches_club"] is False
    assert report["unique_players"] == 1 and not report["cohort_gate"]
    assert report["missing_groups"] == ["calendar", "cup", "lower_league"]
    assert report["scope_membership_freshness_proven"] is True


def test_bootstrap_does_not_silently_count_unusable_contract_roster(tmp_path):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    sample = {"clubs": [{"club_id": "11", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"]}]}
    tasks, _, _ = bench.plan_tasks(sample, roster_only=True)
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    body = b'<select name="saison_id"><option value="2026" selected>26/27</option></select>' + SQUAD.replace(b"<th>Contract</th>", b"<th>Current club</th>")
    record = store.store_attempt(url=tasks[1]["url"], body=body, status_code=200, headers={}, fetched_at="2026-10-08T23:10:00+00:00", cycle_id="boot", scope_id="GB1/2026", endpoint="squad", attempt=1)
    report = bench.cohort_report(tasks, bootstrap_results(tasks, store, record), store)
    assert report["unique_players"] == 0 and not report["clubs"] and not report["cohort_gate"]
    assert len(report["cohort_notes"]) == 2 and report["cohort_notes"][1]["contract_header"] is False


def test_grounded_three_player_source_fixture_uses_public_parser():
    fixture = Path(__file__).resolve().parents[2] / "fixtures/transfermarkt/tmapi_signal_packet_real.json"
    packet = json.loads(fixture.read_text())
    expected = [row["id"] for row in packet["data"]]
    signals = bench.parse_players(packet, expected)
    assert len(signals) == 3
    assert any(not signal.market_value_present for signal in signals.values())
    assert any(signal.contract_until for signal in signals.values())
    national_row = next(row for row in packet["data"] if any(a["type"] == "nationalTeam" for a in row["clubAssignments"]))
    national_ids = {a["clubId"] for a in national_row["clubAssignments"] if a["type"] == "nationalTeam"}
    assert not national_ids.intersection(signals[national_row["id"]].club_ids)
    assert packet["_fixture_provenance"]["source_rows"] == 300


def bootstrap_results(tasks, store, roster_record, club_ids=None):
    task = tasks[0]
    participants = {"success": True, "data": {"competitionId": task["competition_id"], "seasonId": task["saison_id"], "clubIds": club_ids or ["11"]}}
    capture = store.store_attempt(url=task["url"], body=json.dumps(participants).encode(), status_code=200, headers={}, fetched_at="2026-10-08T23:09:00+00:00", cycle_id="boot-participants", scope_id=task["scope"], endpoint="participants", attempt=1)
    return {task["key"]: {"raw_capture_id": capture.capture_id}, tasks[1]["key"]: {"raw_capture_id": roster_record.capture_id}}


def test_template_scope_must_be_live_current_denominator():
    for scope, season in (("GB1/2025", 2025), ("UNKNOWN/2026", 2026), ("GB1/2026", 2025)):
        with pytest.raises(bench.ProbeGateError, match="denominator"):
            bench.plan_tasks({"clubs": [{"club_id": "11", "scope": scope, "saison_id": season}]}, roster_only=True)


def test_cohort_keeps_a_nonparticipant_as_qualification_failure(tmp_path):
    from scrapers.transfermarkt.raw_store import RawResponseStore
    sample = {"clubs": [{"club_id": "11", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"]}]}
    tasks, _, _ = bench.plan_tasks(sample, roster_only=True)
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    body = b'<select name="saison_id"><option value="2026" selected>26/27</option></select>' + SQUAD
    capture = store.store_attempt(url=tasks[1]["url"], body=body, status_code=200, headers={}, fetched_at="2026-10-08T23:10:00+00:00", cycle_id="boot", scope_id="GB1/2026", endpoint="squad", attempt=1)
    report = bench.cohort_report(tasks, bootstrap_results(tasks, store, capture, club_ids=["999"]), store)
    assert not report["cohort_gate"] and not report["scope_membership_freshness_proven"]
    assert report["membership_errors"] == [{"scope": "GB1/2026", "club_id": "11", "reason": "template_club_not_a_proven_current_participant"}]
    assert not report["clubs"] and report["cohort_notes"][1]["scope_membership_confirmed"] is False


def test_failed_scope_bootstrap_continues_other_clubs_and_retries_only_failed_task(tmp_path):
    sample = {"clubs": [{"club_id": "11", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"]},
                        {"club_id": "12", "saison_id": 2026, "scope": "GB1/2026", "groups": ["top_league"]}]}
    sample_path = tmp_path / "templates.json"
    sample_path.write_text(json.dumps(sample))
    tasks, _, _ = bench.plan_tasks(sample, roster_only=True)
    roster = b'<select name="saison_id"><option value="2026" selected>26/27</option></select>' + SQUAD
    responses = {task["url"]: (405, b"blocked") if task["kind"] == "participants" else (200, roster) for task in tasks}
    build, factory = transport_factory(tmp_path, responses=responses)
    options = args(tmp_path, mode="cohort", sample=str(sample_path), player_target=1000, min_scopes=20)
    initial, _ = transport_factory(tmp_path)
    assert bench.run(args(tmp_path, ids="8198", evidence_dir=str(tmp_path / "probe")), client_factory=initial) == 0
    assert bench.run(options, client_factory=build) == 3
    assert len(factory.calls) == 3  # A bad scope does not cancel the two paid successful full pages.
    state = json.loads((tmp_path / "day1/state.json").read_text())
    assert len(state["results"]) == 2 and len(state["failures"]) == 1
    participant = tasks[0]
    responses[participant["url"]] = (200, json.dumps({"success": True, "data": {"competitionId": "GB1", "seasonId": 2026, "clubIds": ["11", "12"]}}).encode())
    second, calls = transport_factory(tmp_path, responses=responses)
    assert bench.run(options, client_factory=second) == 2  # Valid membership, but not a 1000-player / 20-scope cohort.
    assert calls.calls == [participant["url"]]
    report = json.loads((tmp_path / "day1/report.json").read_text())
    assert report["scope_membership_freshness_proven"] is True
    assert report["task_failures"] == {} and report["experiment_totals"]["request_attempts"] == 5
    state = json.loads((tmp_path / "day1/state.json").read_text())
    assert sorted(row["status_code"] for row in state["attempt_records"]) == [200, 200, 200, 405]


def test_expensive_measurement_cannot_skip_initial_raw_packet_schema_probe(tmp_path):
    with pytest.raises(bench.ProbeGateError, match="initial player packet"):
        bench.run(args(tmp_path, mode="cohort", sample="unread-sample.json"), client_factory=lambda *a: pytest.fail("must not open a gateway lease"))
    with pytest.raises(bench.ProbeGateError, match="initial player packet"):
        bench.run(args(tmp_path, mode="parity", sample="unread-sample.json"), client_factory=lambda *a: pytest.fail("must not open a gateway lease"))


@pytest.mark.parametrize("hour,minute", [(0,15),(1,0),(2,59)])
def test_live_measurement_waits_delivery_without_claiming_ledger_or_opening_client(tmp_path, hour, minute):
    result = bench.run(args(tmp_path), now_fn=lambda: datetime(2026,10,9,hour,minute,tzinfo=timezone.utc), client_factory=lambda *a: pytest.fail("quiet window must not open a lease"))
    assert result == 3 and not (tmp_path / "signal-probe-ledger.json").exists()
    assert not (tmp_path / "signal-probe.lock").exists()
    assert json.loads((tmp_path / "day1/report.json").read_text())["resume_after_utc"] == "2026-10-09T03:00:00+00:00"


def test_recheck_selects_actual_changed_ids_current_club_without_fake_change():
    cohort = {"player_ids": ["1", "2"], "groups": sorted(bench.GROUPS), "scopes": ["GB1/2026"]}
    before = {"cohort": cohort, "qualification_observation": {"players": [{"player_id": "1", "tmapi": {"value": 1000, "clubs": ["11"]}}, {"player_id": "2", "tmapi": {"value": 1000, "clubs": ["12"]}}]}}
    after = json.loads(json.dumps(before))
    after["qualification_observation"]["players"][0]["tmapi"].update(value=2000, clubs=["13"])
    sample = bench.recheck_sample(before, after)
    assert sample["selected_changed_ids"] == ["1"]
    assert sample["clubs"][0]["club_id"] == "13" and "/saison_id/" not in sample["clubs"][0]["squad_url"]
    assert bench.recheck_sample(before, before)["clubs"] == []
    after["cohort"] = {**cohort, "player_ids": ["1"]}
    with pytest.raises(bench.ProbeGateError, match="exact measured cohort"):
        bench.recheck_sample(before, after)


def test_fresh_script_run_reaches_client_boundary_without_pytest_dags_path(tmp_path):
    """A shallow import missed this: run() imports dags.utils lazily."""
    import subprocess
    import sys
    import os
    script = tmp_path / "fresh_start.py"
    source = f'''from argparse import Namespace
import json
import runpy
from datetime import datetime, timezone
scope=runpy.run_path({str(bench.ROOT / 'dags/scripts/bench_transfermarkt_signals.py')!r},run_name='fresh_benchmark')
class ReachedBoundary(Exception): pass
def sentinel(*args): raise ReachedBoundary()
args=Namespace(mode='probe', ids='8198', evidence_dir={str(tmp_path / 'smoke')!r}, experiment_dir=None,max_attempts=1,max_bytes=268435456,portion_seconds=2700,endpoint_attempts=1)
try:
 scope['run'](args,client_factory=sentinel,now_fn=lambda:datetime(2026,10,8,12,tzinfo=timezone.utc))
except ReachedBoundary:
 print('CLIENT_BOUNDARY_REACHED')
'''
    script.write_text(source)
    result = subprocess.run([sys.executable, str(script)], cwd=tmp_path,
                            env={**os.environ, "PYTHONPATH": str(bench.ROOT), "PYTHONDONTWRITEBYTECODE": "1"},
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "CLIENT_BOUNDARY_REACHED"


def test_measurement_namespace_matches_existing_gateway_closed_classifier():
    """Execute the shipped classifier AST without importing the gateway runtime."""
    import ast
    from scrapers.transfermarkt.client import ProxyFilterLeaseProvider
    source = bench.ROOT / "scripts/proxy_filter/filter_proxy.py"
    tree = ast.parse(source.read_text())
    allowed = next(node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "TRANSFERMARKT_DAG_IDS" for target in node.targets))
    classifier = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_source_for_dag")
    names = {"SOFASCORE_DISCOVERY_DAG_IDS": frozenset(), "SOFASCORE_DAG_IDS": frozenset(), "TRANSFERMARKT_BACKFILL_DAG_IDS": frozenset(),
             "FBREF_DAG_IDS": frozenset(), "WHOSCORED_PAID_DAG_IDS": frozenset()}
    exec(compile(ast.Module(body=[allowed, classifier], type_ignores=[]), str(source), "exec"), names)
    assert names["_source_for_dag"](bench.MEASUREMENT_DAG_ID) == "transfermarkt"
    assert names["_source_for_dag"]("tm-1393-signal-experiment") == ""
    assert names["_source_for_dag"]("dag_measure_transfermarkt_signals_1393") == ""


def test_proxy_rejection_details_are_redacted_without_charging_source_attempt(tmp_path):
    from scrapers.transfermarkt.models import ProxyRequiredError
    client = FakeClient([])
    def reject(*args, **kwargs):
        raise ProxyRequiredError("proxy lease API rejected POST /v1/leases (HTTP 400): closed source allowlist http://user:private@proxy.example:8900")
    client.fetch = reject
    assert bench.run(args(tmp_path), client_factory=lambda *a: client) == 2
    report = json.loads((tmp_path / "day1/report.json").read_text())
    assert "closed source allowlist" in report["error_detail"]
    assert "private" not in report["error_detail"] and "user:" not in report["error_detail"]
    assert report["experiment_totals"]["request_attempts"] == 0
