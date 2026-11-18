"""Isolated ClubElo research runner and offline evaluator (#2251).

Nothing here schedules runs, changes production, or authorizes a preflight.
The outer controller owns randomization, global ceilings, immutable image
verification, fresh observer directories, and provider-side quota evidence.
Publish active_run.json BEFORE starting the selected solver; keep it active
until this process has atomically published its complete report.

Only run-once makes a site request, through the existing transport. The
airflow-task entry point creates an isolated SQLite DagRun, then uses the
supported Airflow tasks test command; it never manufactures task context.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote


SCHEMA = "clubelo.canary.run.v1"
OBSERVER_DIR = "/var/lib/clubelo-canary-run"
ARMS = ("control", "candidate")
SOLVERS = {arm: "http://solver-" + arm + ":8191" for arm in ARMS}
GATEWAY = "http://clubelo-canary-gateway:8899"
PREFIX = "clubelo_bootstrap_attempt"
ATTEMPT_KEYS = {"schema", "event", "attempt", "outcome", "retry", "lease_id",
                "upstream_fingerprint", "latency_ms", "failed_validation_predicate"}
OUTCOMES = {"invalid_origin", "lease_refusal", "lease_error", "invalid_clearance",
            "challenge_not_solved", "solver_timeout", "tunnel", "solver_error",
            "clearance_accepted"}
PREDICATES = {"solution_not_dict", "status_not_200", "user_agent_not_string",
              "user_agent_blank", "user_agent_not_printable_ascii", "cookies_not_list",
              "cookies_empty", "cookie_not_mapping", "cookie_name_not_string",
              "cookie_name_empty", "cookie_value_not_string", "cookie_value_empty",
              "cookie_domain_not_clubelo", "cookie_path_not_root",
              "cookie_characters_forbidden", "missing_cf_clearance"}
CLOSE_KEYS = ("close_complete", "closed", "accounting_uncertain", "active_tunnels",
              "reserved_bytes", "total_bytes")
BOOTSTRAP_REPLICATES = 10000
BOOTSTRAP_SEED = 2251


def _require(condition):
    if not condition:
        raise ValueError("invalid canary evidence")


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _integer(value):
    return type(value) is int and value >= 0


def _identity(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is not None


def _run_id(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is not None


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=_pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("invalid canary evidence")))


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def validate_attempt(record, ordinal):
    _require(type(record) is dict and set(record) == ATTEMPT_KEYS)
    _require(type(record["schema"]) is int and record["schema"] == 1)
    _require(record["event"] == PREFIX and type(record["attempt"]) is int
             and record["attempt"] == ordinal and 1 <= ordinal <= 3)
    _require(record["outcome"] in OUTCOMES and type(record["retry"]) is bool)
    _require(record["lease_id"] is None or _identity(record["lease_id"]))
    fingerprint = record["upstream_fingerprint"]
    _require(fingerprint is None or (isinstance(fingerprint, str)
             and re.fullmatch(r"[0-9a-f]{16}", fingerprint) is not None))
    _require(_integer(record["latency_ms"]))
    predicate = record["failed_validation_predicate"]
    _require(predicate is None or predicate in PREDICATES)
    _require(predicate is None or record["outcome"] == "invalid_clearance")
    _require(record["retry"] == (record["outcome"] in {
        "invalid_clearance", "challenge_not_solved", "solver_timeout", "tunnel", "solver_error"}))
    return dict(record)


class AttemptCapture(logging.Handler):
    """Retain only the exact #2246 allowlist; never retain formatted errors."""

    def __init__(self):
        super().__init__()
        self.records = []
        self.invalid = False
        self.cleanup_failed = False

    def emit(self, record):
        try:
            message = record.getMessage()
            if message.startswith("ClubElo transport cleanup failed:"):
                self.cleanup_failed = True
            if message.startswith(PREFIX):
                _require(message.startswith(PREFIX + " "))
                parsed = validate_attempt(decode(message[len(PREFIX) + 1:]), len(self.records) + 1)
                if self.records:
                    _require(self.records[-1]["retry"])
                self.records.append(parsed)
        except Exception:
            self.invalid = True


@contextmanager
def capture_logs(capture):
    # A dedicated process: suppress library diagnostics which can contain URLs
    # or credentials. Restore logger state for offline test callers.
    saved = []
    for name in ("scrapers.clubelo.transport", "scrapers.clubelo.proxy_lease",
                 "scrapers.base.flaresolverr_client", "requests", "urllib3"):
        logger = logging.getLogger(name)
        saved.append((logger, logger.handlers[:], logger.level, logger.propagate, logger.disabled))
        logger.handlers = [capture if name == "scrapers.clubelo.transport" else logging.NullHandler()]
        logger.setLevel(logging.WARNING)
        logger.propagate = False
        logger.disabled = False
    try:
        yield
    finally:
        for logger, handlers, level, propagate, disabled in saved:
            logger.handlers, logger.propagate, logger.disabled = handlers, propagate, disabled
            logger.setLevel(level)


def close_proof(data):
    """Whitelist the existing DELETE response, never fetch a stats endpoint."""
    _require(type(data) is dict)
    proof = {key: data.get(key) for key in CLOSE_KEYS}
    _require(proof["close_complete"] is True and proof["closed"] is True
             and proof["accounting_uncertain"] is False)
    _require(_integer(proof["active_tunnels"]) and proof["active_tunnels"] == 0)
    _require(_integer(proof["reserved_bytes"]) and proof["reserved_bytes"] == 0)
    _require(_integer(proof["total_bytes"]))
    return proof


def observer_classes(leases, *, epoch=time.time):
    """Create research subclasses without modifying the source transport.

    The lease class exists before importing the transport. observe_leases
    installs it before the transport's lazy lease import on every attempt.
    Registration precedes base initialization, covering partial construction.
    """
    from scrapers.clubelo.proxy_lease import ClubEloLease

    class ObservedLease(ClubEloLease):
        def __init__(self):
            leases.append(self)
            self.identity_before_close = None
            self.close_response = None
            self.close_attempted = False
            self.close_failed = False
            self.acquire_completed = False
            self.constructed = False
            super().__init__()
            self.constructed = True

        def acquire(self):
            result = super().acquire()
            self.acquire_completed = True
            return result

        def close(self):
            # Same identity clearing, DELETE, raise_for_status and unconditional
            # pool cleanup as ClubEloLease.close. Response observation must not
            # alter a successful HTTP close or hide an original exception.
            try:
                if self.identity_before_close is None:
                    self.identity_before_close = self.lease_id
                if self.lease_id and self.token:
                    lease_id, self.lease_id = self.lease_id, None
                    self.identity_before_close = lease_id
                    self.close_attempted = True
                    with self.session.delete(
                        self.url + "/v1/leases/" + quote(lease_id, safe=""),
                        headers={"Authorization": "Bearer " + self.token},
                        timeout=15, allow_redirects=False,
                    ) as response:
                        response.raise_for_status()
                        try:
                            self.close_response = response.json()
                        except Exception:
                            self.close_failed = True
            except Exception:
                self.close_failed = True
                raise
            finally:
                try:
                    self.session.close()
                except Exception:
                    self.close_failed = True
                    raise

        def evidence(self):
            identity = self.identity_before_close or getattr(self, "lease_id", None)
            valid_id = _identity(identity)
            # A constructed lease with no identity can be an explicit refusal;
            # it is not a zero-byte closed lease.
            kind = ("acquired" if self.acquire_completed else "partial_acquisition") if identity else "no_lease"
            proof = None
            if valid_id and not self.close_failed:
                try:
                    proof = close_proof(self.close_response)
                except Exception:
                    pass
            return dict(lease_id=identity if valid_id else None, acquisition=kind,
                        constructed=self.constructed, close_attempted=self.close_attempted,
                        close_failed=self.close_failed, close_proof=proof,
                        accounting_complete=bool(valid_id and proof is not None))

    from scrapers.clubelo.transport import ClubEloTransport

    class ObservedTransport(ClubEloTransport):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.attempt_intervals = []

        def _bootstrap_once(self):
            interval = {"started_epoch": epoch()}
            self.attempt_intervals.append(interval)
            try:
                return super()._bootstrap_once()
            finally:
                interval["finished_epoch"] = epoch()

    return ObservedLease, ObservedTransport


@contextmanager
def observe_leases(observed):
    from scrapers.clubelo import proxy_lease
    original = proxy_lease.ClubEloLease
    proxy_lease.ClubEloLease = observed
    try:
        yield
    finally:
        proxy_lease.ClubEloLease = original


def match_attempts(records, intervals, run_id):
    _require(_run_id(run_id) and len(records) == len(intervals))
    result = []
    for ordinal, (record, interval) in enumerate(zip(records, intervals), 1):
        record = validate_attempt(record, ordinal)
        _require(set(interval) == {"started_epoch", "finished_epoch"})
        start, finish = interval["started_epoch"], interval["finished_epoch"]
        _require(_number(start) and _number(finish) and start <= finish)
        if result:
            _require(result[-1]["retry"] and result[-1]["finished_epoch"] <= start)
        result.append(dict(record, run_id=run_id, **interval))
    return result


def accounting_evidence(leases, attempts):
    evidence = [lease.evidence() for lease in leases]
    complete = len(evidence) == len(attempts)
    ids = set()
    for lease, attempt in zip(evidence, attempts):
        if lease["acquisition"] == "no_lease":
            # A network/acquisition error without an identity cannot prove the
            # server did not allocate a lease. Only explicit refusals qualify.
            complete &= (lease["constructed"] and not lease["close_failed"]
                         and attempt["outcome"] == "lease_refusal"
                         and attempt["lease_id"] is None)
        else:
            identity = lease["lease_id"]
            complete &= (lease["accounting_complete"] and identity not in ids
                         and identity == attempt["lease_id"])
            ids.add(identity)
    total = sum(item["close_proof"]["total_bytes"] for item in evidence
                if item["close_proof"] is not None) if complete else None
    return dict(complete=bool(complete), total_bytes=total, leases=evidence)


def join_observations(attempts, events, run_id, arm, page_validated):
    from scripts.research.clubelo_canary_solver_instrumentation import join_attempt_events
    _require(all(event.get("arm") == arm for event in events))
    joined = join_attempt_events(attempts, events, run_id)
    result = []
    for item in joined["attempts"]:
        attempt = item["attempt"]
        result.append(dict(attempt, detection=item["detection"], solved=item["solved"],
                           validated=bool(page_validated and attempt["outcome"] == "clearance_accepted")))
    return result, joined["totals"]


def atomic_report(path, report):
    """Publish in the output directory on the same filesystem, mode 0600."""
    path = Path(path)
    _require(path.parent.is_dir() and not path.is_symlink() and not path.exists())
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=".canary-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # Exclusive publication: retries must not replace a prior run report.
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        os.unlink(temporary)


def claim_output(path):
    """A durable exclusive marker prevents a retry from making another fetch."""
    path = Path(path)
    _require(path.parent.is_dir() and not path.exists() and not path.is_symlink())
    marker = path.with_name("." + path.name + ".started")
    fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def airflow_context(environ):
    arm = environ.get("CLUBELO_CANARY_ARM")
    run_id = environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    _require(arm in ARMS and _run_id(run_id))
    _require(environ.get("AIRFLOW_CTX_DAG_ID") == "dag_ingest_clubelo"
             and environ.get("AIRFLOW_CTX_TASK_ID") == "check_watch")
    _require(run_id == environ.get("CLUBELO_CANARY_RUN_ID"))
    return arm, run_id


def run_once(*, output="/canary/output/report.json", observer_dir=OBSERVER_DIR,
             epoch=time.time):
    """One logical /Ranking fetch; internal bootstrap retains 3/10/90/120."""
    claim_output(output)
    started = epoch()
    try:
        arm, run_id = airflow_context(os.environ)
    except Exception:
        finished = epoch()
        failed = dict(schema=SCHEMA, arm=None, run_id=None, complete=False,
                      errors=["invalid_airflow_context"], attempts=[],
                      started_epoch=started, finished_epoch=finished,
                      actual_start_utc=utc(started), actual_end_utc=utc(finished))
        atomic_report(output, failed)
        return failed
    report = dict(schema=SCHEMA, run_id=run_id, arm=arm, started_epoch=started,
                  actual_start_utc=utc(started), attempts=[], detection=None,
                  page_outcome="not_requested", page_validated=False,
                  validated_clearance=False, no_challenge=False, wire_bytes=None,
                  accounting=dict(complete=False, total_bytes=None, leases=[]),
                  invalid_solutions_forwarded=0,
                  complete=False, errors=[])
    capture = AttemptCapture()
    transport = None
    leases = []
    old_solver = os.environ.get("CLUBELO_FLARESOLVERR_URL")
    try:
        from scripts.research import clubelo_canary_solver_instrumentation as observation
        _require(os.environ.get("CLUBELO_PROXY_CONTROL_URL") == GATEWAY)
        _require(not os.environ.get("CLUBELO_PROXY_CONTROL_TOKEN"))
        _require(os.environ.get("CLUBELO_PROXY_CONTROL_TOKEN_FILE") == "/canary/secrets/control_token")
        _require(os.environ.get("CLUBELO_PROXY_MAX_BYTES") == str(32 * 1024 * 1024))
        # A fresh active record must already exist before the solver starts.
        active = decode((Path(observer_dir) / observation.ACTIVE_FILE).read_text())
        observation._active(active, arm, started)
        _require(active["run_id"] == run_id)
        _require(observation.read_events(observer_dir, run_id) == [])
        ObservedLease, ObservedTransport = observer_classes(leases, epoch=epoch)
        import requests
        from scrapers.clubelo.transport import (
            BOOTSTRAP_ATTEMPTS, BOOTSTRAP_RETRY_PAUSE, ClubEloBlocked, ClubEloFetchError,
        )
        _require(BOOTSTRAP_ATTEMPTS == 3 and BOOTSTRAP_RETRY_PAUSE == 10.0)
        os.environ["CLUBELO_FLARESOLVERR_URL"] = SOLVERS[arm]
        with capture_logs(capture), observe_leases(ObservedLease):
            try:
                transport = ObservedTransport(requests.Session(), retries=0)
                page = transport.get("/Ranking")
                report["page_validated"] = page.status == 200 and bool(page.body.strip())
                report["page_outcome"] = "acceptable_200" if report["page_validated"] else "unacceptable_page"
                del page
            except ClubEloBlocked:
                report["page_outcome"] = "blocked"
            except ClubEloFetchError:
                report["page_outcome"] = "fetch_failed"
            except Exception:
                report["errors"].append("internal_transport_error")
            finally:
                if transport is not None:
                    transport.close()
        _require(transport is not None)
        report["wire_bytes"] = transport.wire_bytes
        if capture.invalid:
            report["errors"].append("invalid_attempt_records")
        if capture.cleanup_failed:
            report["errors"].append("cleanup_failed")
        attempts = match_attempts(capture.records, transport.attempt_intervals, run_id)
        report["accounting"] = accounting_evidence(leases, attempts)
        if capture.cleanup_failed:
            report["accounting"]["complete"] = False
            report["accounting"]["total_bytes"] = None
        if not report["accounting"]["complete"]:
            report["errors"].append("accounting_incomplete")
        # Read while active, including events from a detected-then-timeout call.
        report["attempts"] = [dict(attempt, detection="unknown", solved=None,
                                   validated=bool(report["page_validated"] and
                                                  attempt["outcome"] == "clearance_accepted"))
                              for attempt in attempts]
        events = observation.read_events(observer_dir, run_id)
        report["attempts"], report["detection"] = join_observations(
            attempts, events, run_id, arm, report["page_validated"])
        if report["detection"]["unknown"]:
            report["errors"].append("detection_unknown")
        report["no_challenge"] = not attempts and report["page_validated"]
        report["validated_clearance"] = any(a["validated"] for a in report["attempts"])
        # Recheck observer finality while the same run is still active.
        _require(observation.read_events(observer_dir, run_id) == events)
        _require(decode((Path(observer_dir) / observation.ACTIVE_FILE).read_text()) == active)
        observation._active(active, arm, epoch())
    except Exception:
        report["errors"].append("invalid_or_missing_evidence")
    finally:
        # Keep every constructed lease's sanitized evidence even when matching
        # records or reading the observer failed before the normal snapshot.
        if leases and not report["accounting"]["leases"]:
            report["accounting"] = accounting_evidence(leases, [])
        if old_solver is None:
            os.environ.pop("CLUBELO_FLARESOLVERR_URL", None)
        else:
            os.environ["CLUBELO_FLARESOLVERR_URL"] = old_solver
    finished = epoch()
    report.update(finished_epoch=finished, actual_end_utc=utc(finished))
    if finished < started:
        report["errors"].append("clock_reversed")
    report["complete"] = not report["errors"]
    atomic_report(output, report)
    return report


def nearest_rank(values, percentile=0.95):
    values = list(values)
    _require(_number(percentile) and 0 < percentile <= 1)
    _require(all(_number(value) and value >= 0 for value in values))
    return sorted(values)[math.ceil(percentile * len(values)) - 1] if values else None


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def _population(reports):
    """Validate joined evidence before any numerator is computed."""
    seen = set()
    for run in reports:
        _require(run["schema"] == SCHEMA and run["arm"] in ARMS and _run_id(run["run_id"]))
        _require(run["run_id"] not in seen)
        seen.add(run["run_id"])
        _require(run["complete"] is True and run["errors"] == [])
        _require(_number(run["started_epoch"]) and _number(run["finished_epoch"])
                 and run["started_epoch"] <= run["finished_epoch"])
        _require(run["accounting"]["complete"] is True and _integer(run["accounting"]["total_bytes"]))
        _require(_integer(run["wire_bytes"]) and _integer(run["invalid_solutions_forwarded"]))
        _require(type(run["page_validated"]) is bool and type(run["validated_clearance"]) is bool
                 and type(run["no_challenge"]) is bool)
        _require(run["page_validated"] == (run["page_outcome"] == "acceptable_200"))
        attempts = run["attempts"]
        _require(type(attempts) is list and len(attempts) <= 3)
        for ordinal, attempt in enumerate(attempts, 1):
            validate_attempt({key: attempt[key] for key in ATTEMPT_KEYS}, ordinal)
            _require(attempt["run_id"] == run["run_id"])
            _require(_number(attempt["started_epoch"]) and _number(attempt["finished_epoch"])
                     and run["started_epoch"] <= attempt["started_epoch"] <= attempt["finished_epoch"] <= run["finished_epoch"])
            if ordinal > 1:
                _require(attempts[ordinal - 2]["retry"]
                         and attempts[ordinal - 2]["finished_epoch"] <= attempt["started_epoch"])
            _require(attempt["detection"] in ("detected", "not_detected"))
            _require(type(attempt["solved"]) is bool and type(attempt["validated"]) is bool)
            _require(not attempt["solved"] or attempt["detection"] == "detected")
            _require(attempt["validated"] == (run["page_validated"] and attempt["outcome"] == "clearance_accepted"))
        _require(run["detection"] == dict(
            detected=sum(a["detection"] == "detected" for a in attempts),
            solved=sum(a["solved"] for a in attempts), unknown=0))
        _require(run["validated_clearance"] == any(a["validated"] for a in attempts))
        _require(run["no_challenge"] == (not attempts and run["page_validated"]))
        # Recheck byte proof rather than trusting a report's summary flag.
        lease_rows = run["accounting"]["leases"]
        _require(len(lease_rows) == len(attempts))
        lease_ids = set()
        byte_sum = 0
        for lease, attempt in zip(lease_rows, attempts):
            _require(lease["constructed"] is True and lease["close_failed"] is False)
            if lease["acquisition"] == "no_lease":
                _require(attempt["outcome"] == "lease_refusal" and attempt["lease_id"] is None
                         and lease["lease_id"] is None and lease["close_proof"] is None)
            else:
                _require(lease["acquisition"] in ("acquired", "partial_acquisition"))
                _require(_identity(lease["lease_id"]) and lease["lease_id"] not in lease_ids
                         and lease["lease_id"] == attempt["lease_id"]
                         and lease["close_attempted"] is True and lease["accounting_complete"] is True)
                lease_ids.add(lease["lease_id"])
                byte_sum += close_proof(lease["close_proof"])["total_bytes"]
        _require(byte_sum == run["accounting"]["total_bytes"])
    return reports


def arm_metrics(reports):
    reports = list(reports)
    _population(reports)
    _require(len({r["arm"] for r in reports}) <= 1)
    attempts = [a for run in reports for a in run["attempts"]]
    counts = dict(runs=len(reports), attempts=len(attempts),
                  detected=sum(a["detection"] == "detected" for a in attempts),
                  solved=sum(a["solved"] for a in attempts), unknown=0,
                  validated_attempts=sum(a["validated"] for a in attempts),
                  validated_and_solved=sum(a["validated"] and a["solved"] for a in attempts),
                  validated_runs=sum(r["page_validated"] for r in reports),
                  validated_clearance_runs=sum(r["validated_clearance"] for r in reports),
                  no_challenge_runs=sum(r["no_challenge"] for r in reports),
                  invalid_solutions_forwarded=sum(r["invalid_solutions_forwarded"] for r in reports))
    fractions = {}
    for name, numerator, denominator in (
        ("solved_per_detected", "solved", "detected"),
        ("validated_solved_per_solved", "validated_and_solved", "solved"),
        ("validated_per_attempt", "validated_attempts", "attempts"),
        ("validated_clearance_per_run", "validated_clearance_runs", "runs"),
        ("validated_per_run", "validated_runs", "runs"),
    ):
        fractions[name] = dict(numerator=counts[numerator], denominator=counts[denominator],
                               value=ratio(counts[numerator], counts[denominator]))
    return dict(counts=counts, fractions=fractions,
                p95_all_attempt_ms=nearest_rank(a["latency_ms"] for a in attempts),
                p95_validated_attempt_ms=nearest_rank(a["latency_ms"] for a in attempts if a["validated"]))


def cluster_bootstrap(control, candidate):
    """Resample whole runs independently within each arm, including empty runs.

    One-sided 95% lower bound: sorted difference at ceil(.05 * 10000),
    one-based (nearest rank). A replicate with D=0 makes the bound unknown;
    it is never discarded/replaced by zero. Constant empirical distributions
    yield a point interval, explicitly labelled degenerate.
    """
    rng = random.Random(BOOTSTRAP_SEED)
    samples = []
    for arm, runs in zip(ARMS, (control, candidate)):
        runs = list(runs)
        _population(runs)
        _require(all(run["arm"] == arm for run in runs))
        samples.append([(sum(a["solved"] for a in r["attempts"]),
                         sum(a["detection"] == "detected" for a in r["attempts"])) for r in runs])
    result = dict(replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED,
                  lower_one_sided_95_pp=None, undefined_replicates=0,
                  degenerate=None, rule="nearest_rank_0.05_candidate_minus_control")
    if any(not sample or sum(d for _, d in sample) == 0 for sample in samples):
        result["undefined_replicates"] = BOOTSTRAP_REPLICATES
        return result
    differences = []
    for _ in range(BOOTSTRAP_REPLICATES):
        rates = []
        for sample in samples:
            selected = [sample[rng.randrange(len(sample))] for _ in sample]
            rates.append(ratio(sum(s for s, _ in selected), sum(d for _, d in selected)))
        if None in rates:
            result["undefined_replicates"] += 1
        else:
            differences.append(100 * (rates[1] - rates[0]))
    if not result["undefined_replicates"]:
        differences.sort()
        result["lower_one_sided_95_pp"] = differences[math.ceil(0.05 * BOOTSTRAP_REPLICATES) - 1]
        result["degenerate"] = differences[0] == differences[-1]
    return result


def evaluate(reports):
    """Statistical gates only; PASS is not deployment or preflight approval."""
    result = dict(status="INCONCLUSIVE", reasons=[], arms={}, bootstrap=None,
                  delta_solved_per_detected_pp=None, gates={})
    try:
        reports = list(reports)
        _population(reports)
        arms = {arm: [run for run in reports if run["arm"] == arm] for arm in ARMS}
        result["arms"] = {arm: arm_metrics(runs) for arm, runs in arms.items()}
        for arm, metrics in result["arms"].items():
            counts = metrics["counts"]
            if counts["runs"] < 20 or counts["attempts"] < 40 or counts["detected"] < 30:
                result["reasons"].append(arm + "_insufficient_sample")
        if result["reasons"]:
            return result
        control, candidate = (result["arms"][arm] for arm in ARMS)
        c = {key: value["value"] for key, value in control["fractions"].items()}
        n = {key: value["value"] for key, value in candidate["fractions"].items()}
        delta = 100 * (n["solved_per_detected"] - c["solved_per_detected"])
        result["delta_solved_per_detected_pp"] = delta
        result["bootstrap"] = cluster_bootstrap(arms["control"], arms["candidate"])
        lower = result["bootstrap"]["lower_one_sided_95_pp"]
        required_rates = (
            c["solved_per_detected"], c["validated_per_attempt"], c["validated_clearance_per_run"],
            n["solved_per_detected"], n["validated_solved_per_solved"],
            n["validated_per_attempt"], n["validated_clearance_per_run"],
        )
        # Control S=0 or V=0 is measured failure, not missing evidence. Its
        # undefined conditional validation latency/rate is published as null
        # but is not a denominator in any of the candidate gates.
        if (lower is None or any(value is None for value in required_rates)
                or control["p95_all_attempt_ms"] is None
                or candidate["p95_all_attempt_ms"] is None
                or candidate["p95_validated_attempt_ms"] is None):
            result["reasons"].append("undefined_metric")
            return result
        result["gates"] = dict(
            candidate_solved_per_detected=n["solved_per_detected"] >= 0.90,
            improvement_pp=delta >= 20 - 1e-12,
            positive_lower_bound=lower > 0,
            solved_validation=n["validated_solved_per_solved"] >= 0.95,
            attempt_validation=n["validated_per_attempt"] + 1e-12 >= c["validated_per_attempt"] - 0.05,
            run_validation_relative=n["validated_clearance_per_run"] + 1e-12 >= c["validated_clearance_per_run"] - 0.05,
            run_validation_absolute=n["validated_clearance_per_run"] >= 0.85,
            no_invalid_forwarded=all(m["counts"]["invalid_solutions_forwarded"] == 0 for m in (control, candidate)),
            all_latency_absolute=candidate["p95_all_attempt_ms"] <= 120000,
            all_latency_relative=candidate["p95_all_attempt_ms"] <= 1.20 * control["p95_all_attempt_ms"],
            validated_latency=candidate["p95_validated_attempt_ms"] <= 100000)
        result["status"] = "PASS" if all(result["gates"].values()) else "FAIL"
        result["reasons"] = [name for name, passed in result["gates"].items() if not passed]
    except Exception:
        result["reasons"].append("invalid_or_missing_evidence")
    return result


def airflow_task():
    """Bootstrap the disposable SQLite metadata DB, not the shared scheduler."""
    _require(os.environ.get("AIRFLOW_HOME") == "/canary/airflow")
    _require(os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN") == "sqlite:////canary/airflow/airflow.db")
    _require(not any(key.startswith("AIRFLOW_CTX_") for key in os.environ))
    run_id = os.environ.get("CLUBELO_CANARY_RUN_ID")
    _require(_run_id(run_id) and os.environ.get("CLUBELO_CANARY_ARM") in ARMS)
    import airflow
    _require(airflow.__version__ == "2.11.2")
    from airflow.models import DagBag
    from airflow.utils import db, timezone as airflow_timezone
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType
    db.initdb()
    bag = DagBag(dag_folder="/canary/dags", include_examples=False)
    _require(not bag.import_errors and set(bag.dags) == {"dag_ingest_clubelo"})
    dag = bag.get_dag("dag_ingest_clubelo")
    _require(dag.task_ids == ["check_watch"])
    now = airflow_timezone.utcnow()
    dag.create_dagrun(run_id=run_id, run_type=DagRunType.MANUAL, execution_date=now,
                      data_interval=(now, now), state=DagRunState.RUNNING,
                      external_trigger=True, conf={})
    return subprocess.run([sys.executable, "-m", "airflow", "tasks", "test",
                           "dag_ingest_clubelo", "check_watch", now.isoformat(),
                           "--subdir", "/canary/dags/solver_canary_dag.py"], check=False).returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run-once")
    sub.add_parser("airflow-task")
    evaluate_parser = sub.add_parser("evaluate")
    evaluate_parser.add_argument("reports", nargs="+")
    args = parser.parse_args(argv)
    try:
        if args.command == "run-once":
            report = run_once()
            return 0 if report["complete"] and report["page_validated"] else 1
        if args.command == "airflow-task":
            return airflow_task()
        result = evaluate([decode(Path(path).read_text()) for path in args.reports])
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0 if result["status"] == "PASS" else 1
    except Exception:
        # No traceback/exception message can carry a credential or page body.
        print('{"status":"INCONCLUSIVE","reasons":["internal_error"]}')
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
