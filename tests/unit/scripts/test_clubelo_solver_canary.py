"""Offline canary evidence tests: no real HTTP, browser or Docker."""

from copy import deepcopy
import builtins
import io
import json
import logging
import os
from pathlib import Path
import re
import socket
import time

import pytest
import requests

from scripts.research import clubelo_solver_canary as canary
from scripts.research import clubelo_canary_solver_instrumentation as observer


PROOF = dict(close_complete=True, closed=True, accounting_uncertain=False,
             active_tunnels=0, reserved_bytes=0, total_bytes=123)
# Intentionally short, non-secret placeholders, including all redaction probes.
CONTROL_TOKEN = "c" * 32
LEASE_TOKEN = "lease-demo"
COOKIE_VALUE = "cookie-demo"


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network forbidden in offline tests")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.fixture(autouse=True)
def forbid_budget_counter_access(monkeypatch):
    accesses = []
    def guard(original):
        def opened(file, *args, **kwargs):
            if not isinstance(file, int):
                path = Path(os.fsdecode(file))
                if (path.name in {"budget.json", "counter.json"}
                        or "clubelo-canary-budget" in path.parts):
                    accesses.append(str(path))
                    raise AssertionError("budget/counter access forbidden")
            return original(file, *args, **kwargs)
        return opened
    for module in (builtins, io, os):
        monkeypatch.setattr(module, "open", guard(module.open))
    yield
    # run_once catches exceptions; a swallowed access must still fail the test.
    assert accesses == []


def attempt(ordinal=1, *, outcome="clearance_accepted", latency=50000):
    return dict(schema=1, event=canary.PREFIX, attempt=ordinal, outcome=outcome,
                retry=outcome in {"solver_timeout", "tunnel", "solver_error", "invalid_clearance", "challenge_not_solved"},
                lease_id="lease" + str(ordinal), upstream_fingerprint="a" * 16,
                latency_ms=latency, failed_validation_predicate=None)


def event(sequence, kind, epoch, *, run_id="run1", arm="candidate"):
    return dict(schema=observer.EVENT_SCHEMA, sequence=sequence, run_id=run_id,
                arm=arm, kind=kind, observed_epoch=epoch)


def report(index=0, arm="candidate", *, solved=True, validated=True, latency=50000, direct=False):
    run_id = arm + str(index)
    row = dict(attempt(latency=latency, outcome="clearance_accepted" if validated else "challenge_not_solved"),
               run_id=run_id, started_epoch=10, finished_epoch=70,
               detection="detected", solved=solved, validated=validated)
    lease = dict(lease_id="lease1", acquisition="acquired", constructed=True,
                 close_attempted=True, close_failed=False, close_proof=dict(PROOF), accounting_complete=True)
    return dict(schema=canary.SCHEMA, run_id=run_id, arm=arm, started_epoch=0, finished_epoch=100,
                actual_start_utc=canary.utc(0), actual_end_utc=canary.utc(100),
                attempts=[] if direct else [row], complete=True, errors=[],
                detection=dict(detected=0 if direct else 1, solved=0 if direct else int(solved), unknown=0),
                page_validated=validated, page_outcome="acceptable_200" if validated else "blocked",
                validated_clearance=validated and not direct, no_challenge=direct and validated,
                wire_bytes=50, invalid_solutions_forwarded=0,
                accounting=dict(complete=True, total_bytes=0 if direct else 123,
                                leases=[] if direct else [lease]))


def population():
    return ([report(i, "control", solved=i < 24) for i in range(40)]
            + [report(i) for i in range(40)])


class Response:
    def __init__(self, data=None, *, status=201, headers=None, wire=b"<html>ranking</html>", fail=False):
        self.data = data
        self.status_code = status
        self.headers = headers or {}
        self.wire = wire
        self.raw = self
        self.fail = fail

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def read(self, *, decode_content):
        assert decode_content is False
        return self.wire

    def json(self):
        if self.fail == "json":
            raise ValueError("sensitive response")
        return self.data

    def raise_for_status(self):
        if self.fail == "http":
            raise requests.HTTPError("sensitive response")

    def close(self):
        pass


class GatewaySession:
    def __init__(self, *, refusal=False, proxy="http://clubelo-canary-gateway:8900", close_fail=False):
        self.headers = {}
        self.trust_env = True
        self.refusal = refusal
        self.proxy = proxy
        self.close_fail = close_fail
        self.deleted = []
        self.closed = False
        self.acquisitions = 0

    def post(self, url, **kwargs):
        assert url == canary.GATEWAY + "/v1/leases"
        assert kwargs["json"]["canonical_url"] == "https://clubelo.com/Ranking"
        self.acquisitions += 1
        return Response(dict(id="lease" + str(self.acquisitions), token=LEASE_TOKEN, proxy_url=self.proxy,
                             upstream_fingerprint="a" * 16), status=403 if self.refusal else 201)

    def delete(self, url, **kwargs):
        self.deleted.append((url, kwargs))
        ordinal = int(url.rsplit("lease", 1)[1])
        return Response(dict(PROOF, total_bytes=122 + ordinal, token=LEASE_TOKEN, proxy_url=self.proxy),
                        status=200, fail=self.close_fail)

    def close(self):
        self.closed = True


class HttpSession:
    def __init__(self, *, challenge=True, status=200):
        self.responses = ([Response(status=403, headers={"cf-mitigated": "challenge"})] if challenge else [])
        self.responses.append(Response(status=status))
        self.cookies = requests.cookies.RequestsCookieJar()
        self.headers = {}
        self.urls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.urls.append(url)
        assert url == "https://clubelo.com/Ranking"
        assert kwargs["allow_redirects"] is False
        return self.responses.pop(0)

    def close(self):
        self.closed = True


@pytest.fixture
def context(monkeypatch):
    values = dict(AIRFLOW_CTX_DAG_ID="dag_ingest_clubelo", AIRFLOW_CTX_TASK_ID="check_watch",
                  AIRFLOW_CTX_DAG_RUN_ID="run1", CLUBELO_CANARY_RUN_ID="run1",
                  CLUBELO_CANARY_ARM="candidate", CLUBELO_PROXY_CONTROL_URL=canary.GATEWAY,
                  CLUBELO_PROXY_CONTROL_TOKEN_FILE="/canary/secrets/control_token",
                  CLUBELO_PROXY_MAX_BYTES="33554432")
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CLUBELO_PROXY_CONTROL_TOKEN", raising=False)
    return values


def test_attempt_capture_validates_exact_schema_and_order():
    capture = canary.AttemptCapture()
    def emit(value):
        capture.emit(logging.LogRecord("test", logging.WARNING, "", 0,
                                      canary.PREFIX + " " + json.dumps(value), (), None))
    emit(attempt(outcome="solver_timeout"))
    emit(attempt(2))
    assert not capture.invalid
    assert len(capture.records) == 2
    emit(dict(attempt(3), token="secret"))
    assert capture.invalid
    assert "secret" not in json.dumps(capture.records)


@pytest.mark.parametrize("field,value", [("latency_ms", True), ("latency_ms", -1),
    ("lease_id", "http://secret"), ("upstream_fingerprint", "invalid"),
    ("outcome", "anything"), ("retry", True), ("failed_validation_predicate", "raw text")])
def test_attempt_rejects_unapproved_values(field, value):
    with pytest.raises(ValueError):
        canary.validate_attempt(dict(attempt(), **{field: value}), 1)


def test_real_intervals_are_not_reconstructed_from_latency(monkeypatch):
    from scrapers.clubelo.transport import ClubEloTransport
    times = iter([12.25, 19.75])
    _, transport_type = canary.observer_classes([], epoch=lambda: next(times))
    def failure(self):
        raise RuntimeError("synthetic timeout")
    monkeypatch.setattr(ClubEloTransport, "_bootstrap_once", failure)
    transport = transport_type(HttpSession())
    with pytest.raises(RuntimeError):
        transport._bootstrap_once()
    joined = canary.match_attempts([attempt(outcome="solver_timeout", latency=99999)],
                                  transport.attempt_intervals, "run1")
    assert joined[0]["started_epoch"] == 12.25
    assert joined[0]["finished_epoch"] == 19.75
    assert joined[0]["latency_ms"] == 99999


def test_detection_timeout_unknown_and_direct():
    rows = canary.match_attempts([attempt(outcome="solver_timeout")],
                                [dict(started_epoch=10, finished_epoch=20)], "run1")
    joined, totals = canary.join_observations(rows, [event(1, "detected", 11)], "run1", "candidate", False)
    assert totals == dict(detected=1, solved=0, unknown=0)
    assert joined[0]["solved"] is False
    unknown, totals = canary.join_observations(rows, [], "run1", "candidate", False)
    assert unknown[0]["solved"] is None and totals["unknown"] == 1
    assert canary.join_observations([], [], "run1", "candidate", True) == ([], dict(detected=0, solved=0, unknown=0))


@pytest.mark.parametrize("events", [
    [event(1, "solved", 11)],
    [event(1, "detected", 9)],
    [event(1, "detected", 11), event(2, "detected", 12)],
    [event(1, "detected", 11), event(2, "solved", 12), event(3, "solved", 13)],
    [event(1, "detected", 11, run_id="other")],
    [event(1, "detected", 11, arm="control")],
])
def test_invalid_detection_join(events):
    rows = canary.match_attempts([attempt()], [dict(started_epoch=10, finished_epoch=20)], "run1")
    with pytest.raises(ValueError):
        canary.join_observations(rows, events, "run1", "candidate", True)


def test_ambiguous_boundary_and_no_synthetic_direct_detection():
    rows = canary.match_attempts([attempt(outcome="solver_timeout"), attempt(2)],
        [dict(started_epoch=10, finished_epoch=20), dict(started_epoch=20, finished_epoch=30)], "run1")
    with pytest.raises(ValueError):
        canary.join_observations(rows, [event(1, "detected", 20)], "run1", "candidate", False)
    with pytest.raises(ValueError):
        canary.join_observations([], [event(1, "not_detected", 20)], "run1", "candidate", True)


@pytest.mark.parametrize("field,value", [("close_complete", False), ("closed", False),
    ("accounting_uncertain", True), ("active_tunnels", 1), ("reserved_bytes", 1),
    ("total_bytes", -1), ("total_bytes", True), ("total_bytes", 1.5)])
def test_finality_requires_exact_close_proof(field, value):
    with pytest.raises(ValueError):
        canary.close_proof(dict(PROOF, **{field: value}))


@pytest.mark.parametrize("close_fail", [False, "http", "json"])
def test_observed_close_preserves_cleanup_and_raise(monkeypatch, close_fail):
    gateway = GatewaySession(close_fail=close_fail)
    monkeypatch.setattr(requests, "Session", lambda: gateway)
    leases = []
    lease_type, _ = canary.observer_classes(leases)
    lease = lease_type()
    assert leases == [lease]
    lease.url, lease.lease_id, lease.token = canary.GATEWAY, "lease1", LEASE_TOKEN
    if close_fail == "http":
        with pytest.raises(requests.HTTPError):
            lease.close()
    else:
        lease.close()
    assert gateway.closed and len(gateway.deleted) == 1
    assert lease.lease_id is None and lease.identity_before_close == "lease1"
    assert lease.evidence()["accounting_complete"] == (not close_fail)
    lease.close()
    assert len(gateway.deleted) == 1  # No second DELETE and no stats request.
    assert LEASE_TOKEN not in json.dumps(lease.evidence())


@pytest.mark.parametrize("refusal,proxy,outcome", [
    (True, "http://clubelo-canary-gateway:8900", "lease_refusal"),
    (False, "invalid", "lease_error"),
])
def test_partial_acquisition_and_refusal(monkeypatch, context, refusal, proxy, outcome):
    monkeypatch.setenv("CLUBELO_PROXY_CONTROL_TOKEN", CONTROL_TOKEN)
    gateway = GatewaySession(refusal=refusal, proxy=proxy)
    monkeypatch.setattr(requests, "Session", lambda: gateway)
    leases = []
    lease_type, _ = canary.observer_classes(leases)
    lease = lease_type()
    with pytest.raises(ValueError):
        lease.acquire()
    lease.close()
    row = attempt(outcome=outcome)
    row["lease_id"] = None if refusal else "lease1"
    accounting = canary.accounting_evidence(leases, [row])
    assert accounting["complete"]
    assert accounting["leases"][0]["acquisition"] == ("no_lease" if refusal else "partial_acquisition")
    assert accounting["total_bytes"] == (0 if refusal else 123)
    if refusal:
        assert not canary.accounting_evidence(leases, [dict(row, outcome="lease_error")])["complete"]
    for placeholder in (CONTROL_TOKEN, LEASE_TOKEN):
        assert placeholder not in json.dumps(accounting)


def setup_run(tmp_path, monkeypatch, *, challenge=True, status=200, close_fail=False, solver_failure=False):
    from scrapers.base import flaresolverr_client
    state = tmp_path / "observer"
    state.mkdir(mode=0o700)
    observer.write_active_run(state, "run1", "candidate", time.time() - 1, time.time() + 600)
    http = HttpSession(challenge=challenge, status=status)
    gateway = GatewaySession(close_fail=close_fail)
    sessions = iter([http, gateway, gateway, gateway])
    monkeypatch.setattr(requests, "Session", lambda: next(sessions))
    original_read = Path.read_text
    def read_text(path, *args, **kwargs):
        if str(path) == "/canary/secrets/control_token":
            return CONTROL_TOKEN
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read_text)
    # Preserve 3/10/90/120 configuration; substitute only the sleep dependency.
    original_classes = canary.observer_classes
    def classes(*args, **kwargs):
        lease_type, transport_type = original_classes(*args, **kwargs)
        original_init = transport_type.__init__
        def initialize(self, *args, **kwargs):
            original_init(self, *args, sleep=lambda _: None, **kwargs)
        monkeypatch.setattr(transport_type, "__init__", initialize)
        return lease_type, transport_type
    monkeypatch.setattr(canary, "observer_classes", classes)
    class Solver:
        def __init__(self, **kwargs):
            assert kwargs == dict(url=canary.SOLVERS["candidate"], default_timeout=120, default_max_timeout_ms=90000)
        def create_session(self, session_id, *, proxy_url):
            pass
        def get(self, url, session_id):
            logger = logging.Logger("fake_solver")
            handler = observer.install(state, "candidate", logger=logger)
            try:
                logger.warning("Challenge detected. Title found: sensitive raw title")
                if solver_failure:
                    raise flaresolverr_client.FlareSolverrTimeout("sensitive exception")
                logger.warning("Challenge solved!")
            finally:
                observer.uninstall(handler, logger=logger)
            return dict(status=200, userAgent="synthetic agent", cookies=[dict(name="cf_clearance", value=COOKIE_VALUE)])
        def destroy_session(self, session_id):
            pass
        def close(self):
            pass
    monkeypatch.setattr(flaresolverr_client, "FlareSolverrClient", Solver)
    return state, http, gateway


def run_fixture(tmp_path, state):
    return canary.run_once(output=tmp_path / "report.json", observer_dir=state)


@pytest.mark.parametrize("challenge,status", [(True, 200), (False, 200), (True, 403)])
def test_run_once_real_transport_offline(tmp_path, monkeypatch, context, challenge, status):
    state, http, gateway = setup_run(tmp_path, monkeypatch, challenge=challenge, status=status)
    output = tmp_path / "report.json"
    result = run_fixture(tmp_path, state)
    assert result["complete"]
    assert result["page_validated"] == (status == 200)
    assert result["validated_clearance"] == (challenge and status == 200)
    assert result["no_challenge"] == (not challenge and status == 200)
    assert len(result["attempts"]) == int(challenge)
    assert len(http.urls) == (2 if challenge else 1)
    assert result["accounting"]["total_bytes"] == (123 if challenge else 0)
    assert set(result) == set(report())
    assert result["page_outcome"] == ("acceptable_200" if status == 200 else "blocked")
    assert result["detection"] == dict(detected=int(challenge), solved=int(challenge), unknown=0)
    assert len(gateway.deleted) == int(challenge)
    if challenge:
        row = result["attempts"][0]
        assert row["run_id"] == result["run_id"] == "run1"
        assert result["started_epoch"] <= row["started_epoch"] <= row["finished_epoch"] <= result["finished_epoch"]
        assert result["accounting"]["leases"][0]["lease_id"] == row["lease_id"]
        assert result["accounting"]["leases"][0]["close_proof"] == PROOF
    assert json.loads((state / observer.ACTIVE_FILE).read_text())["active"] is True
    assert canary.decode(output.read_text()) == result
    assert not re.search(r"synthetic_|sensitive|cf_clearance|Bearer|https?://|userAgent|<html", output.read_text())
    for placeholder in (CONTROL_TOKEN, LEASE_TOKEN, COOKIE_VALUE):
        assert placeholder not in output.read_text()
    assert canary.arm_metrics([result])["counts"]["runs"] == 1


def test_cleanup_failure_does_not_change_solver_or_page_result(tmp_path, monkeypatch, context):
    state, _, _ = setup_run(tmp_path, monkeypatch, close_fail="http")
    result = run_fixture(tmp_path, state)
    assert result["page_validated"] and result["attempts"][0]["solved"]
    assert not result["complete"] and not result["accounting"]["complete"]
    assert result["accounting"]["total_bytes"] is None
    assert "cleanup_failed" in result["errors"]


def test_solver_timeout_after_detection_stays_failure(tmp_path, monkeypatch, context):
    state, _, gateway = setup_run(tmp_path, monkeypatch, solver_failure=True)
    result = run_fixture(tmp_path, state)
    assert result["page_outcome"] == "blocked" and not result["page_validated"]
    assert len(result["attempts"]) == 3
    assert result["detection"] == dict(detected=3, solved=0, unknown=0)
    assert all(a["outcome"] == "solver_timeout" for a in result["attempts"])
    assert result["complete"] and result["errors"] == []
    assert result["accounting"]["complete"]
    assert result["accounting"]["total_bytes"] == 123 + 124 + 125
    assert [lease["close_proof"]["total_bytes"] for lease in result["accounting"]["leases"]] == [123, 124, 125]
    assert [lease["lease_id"] for lease in result["accounting"]["leases"]] == [a["lease_id"] for a in result["attempts"]]
    assert len(gateway.deleted) == 3
    assert canary.arm_metrics([result])["counts"]["validated_runs"] == 0


@pytest.mark.parametrize("fault", ["observer_error", "missing_active", "wrong_arm", "wrong_run"])
def test_missing_or_invalid_evidence_fails_before_request(tmp_path, monkeypatch, context, fault):
    state, http, _ = setup_run(tmp_path, monkeypatch)
    if fault == "observer_error":
        (state / observer.ERROR_FILE).write_text('{}')
    elif fault == "missing_active":
        (state / observer.ACTIVE_FILE).unlink()
    elif fault in ("wrong_arm", "wrong_run"):
        active_path = state / observer.ACTIVE_FILE
        active = json.loads(active_path.read_text())
        active["arm" if fault == "wrong_arm" else "run_id"] = "control" if fault == "wrong_arm" else "other"
        active_path.write_text(json.dumps(active))
    result = run_fixture(tmp_path, state)
    assert not result["complete"] and http.urls == []
    assert canary.evaluate([result])["status"] == "INCONCLUSIVE"


@pytest.mark.parametrize("fault", ["missing_detection", "wrong_run", "outside_interval",
                                  "observer_error", "changed_active"])
def test_post_fetch_evidence_failures_remain_incomplete(tmp_path, monkeypatch, context, fault):
    state, http, _ = setup_run(tmp_path, monkeypatch)
    original_read = observer.read_events
    def read_events(directory, run_id):
        events = original_read(directory, run_id)
        if events:
            if fault == "missing_detection":
                return []
            if fault == "wrong_run":
                return [dict(item, run_id="other") for item in events]
            if fault == "outside_interval":
                return [dict(item, observed_epoch=0) for item in events]
            if fault == "observer_error":
                (state / observer.ERROR_FILE).write_text('{}')
            if fault == "changed_active":
                active_path = state / observer.ACTIVE_FILE
                active = json.loads(active_path.read_text())
                active["run_id"] = "other"
                active_path.write_text(json.dumps(active))
        return events
    monkeypatch.setattr(observer, "read_events", read_events)
    result = run_fixture(tmp_path, state)
    assert len(http.urls) == 2 and result["page_validated"]
    assert result["accounting"]["complete"] and result["accounting"]["total_bytes"] == 123
    assert not result["complete"] and result["errors"]
    assert canary.evaluate([result])["status"] == "INCONCLUSIVE"


def test_context_is_verified_never_fabricated(context):
    assert canary.airflow_context(context) == ("candidate", "run1")
    for key in ("AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_TASK_ID", "AIRFLOW_CTX_DAG_RUN_ID", "CLUBELO_CANARY_ARM"):
        values = dict(context)
        values.pop(key)
        with pytest.raises(ValueError):
            canary.airflow_context(values)


def test_bad_context_still_publishes_sanitized_failure(tmp_path, monkeypatch, context):
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "http://sensitive")
    output = tmp_path / "report.json"
    result = canary.run_once(output=output)
    assert result["complete"] is False and result["run_id"] is None
    assert "sensitive" not in output.read_text()


def test_output_claim_prevents_retry_even_without_a_report(tmp_path, monkeypatch, context):
    output = tmp_path / "report.json"
    canary.claim_output(output)
    with pytest.raises(FileExistsError):
        canary.run_once(output=output)
    assert not output.exists()


def test_atomic_report_is_exclusive_and_private(tmp_path):
    target = tmp_path / "report.json"
    canary.atomic_report(target, {"schema": "test"})
    with pytest.raises(ValueError):
        canary.atomic_report(target, {"schema": "replacement"})
    assert target.stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob(".canary-*"))


@pytest.mark.parametrize("payload", ['{"schema":1,"schema":2}', '{"value":NaN}',
                                   '{"value":Infinity}', '{"value":-Infinity}'])
def test_decode_rejects_duplicate_fields_and_nonfinite_values(payload):
    with pytest.raises(ValueError):
        canary.decode(payload)


def test_denominators_and_no_challenge_are_separate():
    runs = [report(0), report(1, solved=False, validated=False), report(2, direct=True)]
    metrics = canary.arm_metrics(runs)
    counts, fractions = metrics["counts"], metrics["fractions"]
    assert counts["runs"] == 3 and counts["attempts"] == 2
    assert counts["detected"] == 2 and counts["solved"] == 1
    assert counts["validated_runs"] == 2 and counts["validated_attempts"] == 1
    assert counts["no_challenge_runs"] == 1
    assert fractions["solved_per_detected"] == dict(numerator=1, denominator=2, value=0.5)
    assert fractions["validated_clearance_per_run"]["value"] == 1 / 3
    assert fractions["validated_solved_per_solved"]["value"] == 1
    assert canary.arm_metrics([])["fractions"]["solved_per_detected"]["value"] is None


def test_nearest_rank_includes_failures_and_handles_empty():
    assert canary.nearest_rank([]) is None
    assert canary.nearest_rank(range(1, 21)) == 19
    assert canary.nearest_rank([50, 120, 60]) == 120
    rows = [report(0), report(1, solved=False, validated=False, latency=120001)]
    metrics = canary.arm_metrics(rows)
    assert metrics["p95_all_attempt_ms"] == 120001
    assert metrics["p95_validated_attempt_ms"] == 50000
    with pytest.raises(ValueError):
        canary.nearest_rank([float("nan")])


def test_bootstrap_cluster_determinism_and_degenerate():
    control, candidate = population()[:40], population()[40:]
    first = canary.cluster_bootstrap(control, candidate)
    assert first == canary.cluster_bootstrap(control, candidate)
    assert first["replicates"] == 10000 and first["seed"] == 2251
    assert first["lower_one_sided_95_pp"] > 0
    assert canary.cluster_bootstrap([], candidate)["lower_one_sided_95_pp"] is None
    same = canary.cluster_bootstrap([report(0, "control")], [report(0)])
    assert same["degenerate"] and same["lower_one_sided_95_pp"] == 0
    # A whole run with no D is kept; undefined resamples must not be dropped.
    partial = canary.cluster_bootstrap([report(0, "control"), report(1, "control", direct=True)], [report(0)])
    assert partial["undefined_replicates"] > 0 and partial["lower_one_sided_95_pp"] is None


def test_thresholds_pass_and_insufficient_is_inconclusive():
    result = canary.evaluate(population())
    assert result["status"] == "PASS"
    assert result["delta_solved_per_detected_pp"] == 40
    assert all(result["gates"].values())
    assert canary.evaluate(population()[:39] + population()[40:])["status"] == "INCONCLUSIVE"


@pytest.mark.parametrize("mutate,gate", [
    ("slow_all", "all_latency_absolute"),
    ("slow_relative", "all_latency_relative"),
    ("slow_validation", "validated_latency"),
    ("forwarded", "no_invalid_forwarded"),
    ("no_advantage", "improvement_pp"),
    ("solver_rate", "candidate_solved_per_detected"),
    ("validation_rate", "solved_validation"),
])
def test_threshold_gate_failures(mutate, gate):
    rows = population()
    if mutate.startswith("slow"):
        value = {"slow_all": 120001, "slow_relative": 60001, "slow_validation": 100001}[mutate]
        for run in rows[40:]:
            run["attempts"][0]["latency_ms"] = value
    elif mutate == "forwarded":
        rows[-1]["invalid_solutions_forwarded"] = 1
    elif mutate == "no_advantage":
        for run in rows[:40]:
            run["attempts"][0]["solved"] = True
            run["detection"]["solved"] = 1
    elif mutate == "solver_rate":
        for run in rows[40:45]:
            run["attempts"][0]["solved"] = False
            run["detection"]["solved"] = 0
    elif mutate == "validation_rate":
        for run in rows[40:43]:
            run["page_validated"] = run["validated_clearance"] = False
            run["page_outcome"] = "blocked"
            run["attempts"][0]["validated"] = False
    result = canary.evaluate(rows)
    assert result["status"] == "FAIL" and result["gates"][gate] is False


@pytest.mark.parametrize("corruption", ["duplicate", "unknown", "missing", "accounting", "false_total",
                                      "lease_join", "run_join", "interval", "detection_total"])
def test_missing_evidence_is_never_zero_or_success(corruption):
    rows = population()
    if corruption == "duplicate":
        rows.append(deepcopy(rows[0]))
    elif corruption == "unknown":
        rows[0]["attempts"][0].update(detection="unknown", solved=None)
    elif corruption == "missing":
        del rows[0]["attempts"][0]["latency_ms"]
    elif corruption == "accounting":
        rows[0]["accounting"]["leases"][0]["close_proof"]["closed"] = False
    elif corruption == "false_total":
        rows[0]["accounting"]["total_bytes"] = 0
    elif corruption == "lease_join":
        rows[0]["accounting"]["leases"][0]["lease_id"] = "other"
    elif corruption == "run_join":
        rows[0]["attempts"][0]["run_id"] = "other"
    elif corruption == "interval":
        rows[0]["attempts"][0]["finished_epoch"] = 101
    else:
        rows[0]["detection"]["detected"] = 0
    assert canary.evaluate(rows)["status"] == "INCONCLUSIVE"


def test_detection_minimum_is_independent_of_attempt_minimum():
    rows = population()
    for run in rows[:11]:
        run["attempts"][0].update(detection="not_detected", solved=False)
        run["detection"] = dict(detected=0, solved=0, unknown=0)
    result = canary.evaluate(rows)
    assert result["status"] == "INCONCLUSIVE"
    assert result["arms"]["control"]["counts"]["attempts"] == 40
    assert result["arms"]["control"]["counts"]["detected"] == 29


def test_run_minimum_is_independent_of_attempt_minimum():
    rows = []
    for arm in canary.ARMS:
        for index in range(15):
            run = report(index, arm)
            run["attempts"] = []
            run["accounting"]["leases"] = []
            for ordinal in range(1, 4):
                row = dict(attempt(ordinal, outcome="clearance_accepted" if ordinal == 3 else "challenge_not_solved"),
                           run_id=run["run_id"], started_epoch=ordinal * 20 - 10,
                           finished_epoch=ordinal * 20, detection="detected",
                           solved=ordinal == 3, validated=ordinal == 3)
                run["attempts"].append(row)
                lease = deepcopy(report()["accounting"]["leases"][0])
                lease["lease_id"] = row["lease_id"]
                run["accounting"]["leases"].append(lease)
            run["detection"] = dict(detected=3, solved=1, unknown=0)
            run["accounting"]["total_bytes"] = 369
            rows.append(run)
    result = canary.evaluate(rows)
    assert result["status"] == "INCONCLUSIVE"
    assert result["arms"]["candidate"]["counts"]["attempts"] == 45
    assert result["arms"]["candidate"]["counts"]["runs"] == 15


def test_validation_regression_gates_and_boundary():
    rows = population()
    for run in rows[40:47]:
        run["page_validated"] = run["validated_clearance"] = False
        run["page_outcome"] = "blocked"
        run["attempts"][0]["validated"] = False
    result = canary.evaluate(rows)
    assert result["status"] == "FAIL"
    for gate in ("attempt_validation", "run_validation_relative", "run_validation_absolute"):
        assert result["gates"][gate] is False
    rows = population()
    for run in rows[40:42]:
        run["page_validated"] = run["validated_clearance"] = False
        run["page_outcome"] = "blocked"
        run["attempts"][0]["validated"] = False
    result = canary.evaluate(rows)
    assert result["status"] == "PASS"  # Exactly 95% validation and -5 pp.


def test_solver_rate_and_improvement_inclusive_boundaries():
    rows = population()
    for index, run in enumerate(rows):
        solved = index < 28 if index < 40 else index < 76
        run["attempts"][0]["solved"] = solved
        run["detection"]["solved"] = int(solved)
    result = canary.evaluate(rows)
    assert result["gates"]["candidate_solved_per_detected"]
    assert result["gates"]["improvement_pp"]


def test_measured_zero_control_success_is_not_missing_evidence():
    rows = ([report(i, "control", solved=False, validated=False) for i in range(40)]
            + [report(i) for i in range(40)])
    result = canary.evaluate(rows)
    assert result["status"] == "PASS"
    assert result["arms"]["control"]["p95_validated_attempt_ms"] is None
    assert result["bootstrap"]["degenerate"]
    assert result["bootstrap"]["lower_one_sided_95_pp"] == 100
