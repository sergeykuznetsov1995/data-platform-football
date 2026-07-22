from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from dags.scripts import whoscored_production_issuance as issuance


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
ROLLOUT_ID = "rollout-954"
RELEASE = {
    "parser_version": "whoscored-parser-v8",
    "manifest_sha256": SHA_A,
    "code_tree_sha256": SHA_C,
}


def _idempotency(scope_count: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "green",
        "scope": {
            "scope_count": scope_count,
            "exact_manifest_pair_count": scope_count * 5,
            "duplicate_counter_count": scope_count * 4,
            "physical_current_pair_count": scope_count * 11,
            "zero_mismatch_counter_count": scope_count * 6,
            "violation_count": 0,
            "evidence_sha256": SHA_A,
        },
        "profile": {
            "exact_manifest_pair_count": 2,
            "duplicate_counter_count": 1,
            "physical_current_pair_count": 2,
            "zero_mismatch_counter_count": 5,
            "violation_count": 0,
            "evidence_sha256": SHA_B,
        },
    }


class _OpsStore:
    def __init__(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        self.records = records
        self.prefixes: list[str] = []

    def iter_content_addressed_json(self, prefix: str):
        self.prefixes.append(prefix)
        return iter(self.records)


def _live_promotion(wave_id: str) -> dict[str, Any]:
    run_count = 2 if wave_id == "wave-70" else 4
    terminal_runs = [
        {
            "run_id": f"scheduled__2026-07-{10 + index:02d}T10:00:00+00:00",
            "logical_date": f"2026-07-{10 + index:02d}T10:00:00Z",
            "scope_plan_sha256": SHA_A,
            "evidence_sha256": SHA_B,
            "idempotency": _idempotency(20 if index < 2 else 70),
            "scope_dq": {
                "count": 20 if index < 2 else 70,
                "sha256": SHA_C,
                "scopes_sha256": SHA_D,
            },
            "task_states": {"count": 40, "sha256": SHA_A},
        }
        for index in range(run_count)
    ]
    return {
        "schema_version": 1,
        "rollout_id": ROLLOUT_ID,
        "source_wave_id": "wave-20" if wave_id == "wave-70" else "wave-70",
        "source_cohort_sha256": SHA_D,
        "runtime_sha256": SHA_C,
        "classifier_sha256": SHA_D,
        "release": dict(RELEASE),
        "receipt_sha256s": [SHA_A, SHA_B]
        if wave_id == "wave-70"
        else [SHA_A, SHA_B, SHA_C, SHA_D],
        "terminal_receipt_sha256": SHA_B,
        "promotion_acceptance_sha256": SHA_A,
        "terminal_runs": terminal_runs,
    }


def _scope_authority(wave_id: str) -> dict[str, Any]:
    max_scopes, require_full_active = issuance._WAVE_CONTRACTS[wave_id]
    genesis = issuance.ROLLOUT_GENESIS_PROOF_SHA256
    return {
        "catalog_active_scope_count": 71,
        "catalog_active_scopes_sha256": SHA_A,
        "classifier_sha256": SHA_D,
        "cohort_sha256": SHA_B,
        "max_scopes": max_scopes,
        "promotion_acceptance_sha256": genesis if wave_id == "wave-20" else SHA_A,
        "promotion_terminal_receipt_sha256": genesis if wave_id == "wave-20" else SHA_B,
        "ranked_scope_ids_sha256": SHA_C,
        "require_full_active": require_full_active,
        "rollout_id": ROLLOUT_ID,
        "runtime_sha256": SHA_C,
        "wave_id": wave_id,
    }


@pytest.mark.parametrize(
    ("wave_id", "expected_runs"),
    (("wave-20", 0), ("wave-70", 2), ("wave-all", 4)),
)
def test_live_issuance_replays_exact_predecessor_chain(
    monkeypatch: pytest.MonkeyPatch, wave_id: str, expected_runs: int
) -> None:
    store = _OpsStore([("receipt.json", {})])
    replayed: list[list[dict[str, Any]]] = []
    monkeypatch.setattr(
        issuance, "validate_runtime_contract", lambda **_kwargs: dict(RELEASE)
    )
    monkeypatch.setattr(
        issuance.WhoScoredOpsStore,
        "from_env",
        lambda **_kwargs: store,
    )
    if wave_id != "wave-20":
        monkeypatch.setattr(
            issuance,
            "promotion_acceptance_evidence",
            lambda *_args, **_kwargs: _live_promotion(wave_id),
        )
    else:
        monkeypatch.setattr(
            issuance,
            "promotion_acceptance_evidence",
            lambda *_args, **_kwargs: pytest.fail(
                "genesis must not replay a predecessor"
            ),
        )

    def verify_runs(value):
        replayed.append(list(value))
        return [dict(item, completed_at=item["logical_date"]) for item in value]

    monkeypatch.setattr(issuance, "_verify_terminal_runs", verify_runs)
    newer_checks: list[str] = []
    monkeypatch.setattr(
        issuance,
        "_verify_newer_runs_belong_to_current_wave",
        lambda _runs, **kwargs: newer_checks.append(
            kwargs["expected_authority"]["wave_id"]
        ),
    )
    result = issuance.verify_daily_issuance_rollout(
        rollout_id=ROLLOUT_ID,
        expected_scope_authority=_scope_authority(wave_id),
    )

    assert result["wave_id"] == wave_id
    assert len(result["terminal_runs"]) == expected_runs
    assert len(replayed[0]) == expected_runs if expected_runs else replayed == []
    assert store.prefixes == [f"production/whoscored-rollout/v1/{ROLLOUT_ID}/receipts"]
    assert newer_checks == [wave_id]


def test_live_issuance_fails_when_ops_terminal_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        issuance, "validate_runtime_contract", lambda **_kwargs: dict(RELEASE)
    )
    monkeypatch.setattr(
        issuance.WhoScoredOpsStore,
        "from_env",
        lambda **_kwargs: _OpsStore([]),
    )

    def absent(*_args, **_kwargs):
        raise RuntimeError("promotion terminal receipt is absent")

    monkeypatch.setattr(issuance, "promotion_acceptance_evidence", absent)
    with pytest.raises(RuntimeError, match="terminal receipt is absent"):
        issuance.verify_daily_issuance_rollout(
            rollout_id=ROLLOUT_ID,
            expected_scope_authority=_scope_authority("wave-70"),
        )


class _Query:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def all(self):
        return self.rows


class _Session:
    def __init__(self, results: list[list[Any]]) -> None:
        self.results = iter(results)

    def query(self, *_args):
        return _Query(next(self.results))


def _session_factory(results: list[list[Any]]):
    @contextmanager
    def create_session():
        yield _Session(results)

    return create_session


class _Field:
    def __eq__(self, _other):
        return self

    def in_(self, _other):
        return self

    def like(self, _other):
        return self

    def is_(self, _other):
        return self

    def __gt__(self, _other):
        return self

    def asc(self):
        return self


class _DagRunModel:
    dag_id = _Field()
    execution_date = _Field()
    external_trigger = _Field()
    run_id = _Field()
    run_type = _Field()


class _TaskInstanceModel:
    dag_id = _Field()
    run_id = _Field()
    task_id = _Field()
    map_index = _Field()
    state = _Field()


class _XComModel:
    dag_id = _Field()
    run_id = _Field()
    task_id = _Field()
    map_index = _Field()
    key = _Field()
    value = _Field()

    @staticmethod
    def deserialize_value(row):
        return row.value


def _patch_airflow(monkeypatch: pytest.MonkeyPatch, results: list[list[Any]]) -> None:
    monkeypatch.setattr(
        issuance,
        "_airflow_components",
        lambda: (
            _DagRunModel,
            _TaskInstanceModel,
            _XComModel,
            _session_factory(results),
            "return_value",
        ),
    )


def _terminal_witness() -> tuple[dict[str, Any], SimpleNamespace]:
    logical = datetime(2026, 7, 10, 10, tzinfo=timezone.utc)
    witness = {
        "run_id": f"scheduled__{logical.isoformat()}",
        "logical_date": logical.isoformat().replace("+00:00", "Z"),
        "scope_plan_sha256": SHA_A,
        "evidence_sha256": SHA_B,
        "idempotency": _idempotency(1),
        "scope_dq": {"count": 1, "sha256": SHA_C, "scopes_sha256": SHA_D},
        "task_states": {"count": 20, "sha256": SHA_A},
    }
    dag_run = SimpleNamespace(
        run_id=witness["run_id"],
        state="success",
        execution_date=logical,
        end_date=logical + timedelta(hours=1),
        run_type="scheduled",
        external_trigger=False,
        conf={},
    )
    return witness, dag_run


def test_terminal_replay_fails_when_metadata_db_run_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    witness, _dag_run = _terminal_witness()
    _patch_airflow(monkeypatch, [[]])

    with pytest.raises(RuntimeError, match="missing from metadata DB"):
        issuance._verify_terminal_runs([witness])


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("task-state", "TaskInstance states have drifted"),
        ("mapped-xcom", "mapped DQ XCom set is not exact"),
        ("singleton-xcom", "singleton XCom set is not exact"),
        ("evidence-xcom", "green XCom evidence has drifted"),
        ("idempotency", "idempotency evidence has drifted"),
    ),
)
def test_terminal_replay_fails_on_ti_or_xcom_drift(
    monkeypatch: pytest.MonkeyPatch, mutation: str, message: str
) -> None:
    witness, dag_run = _terminal_witness()
    task_rows = [
        SimpleNamespace(
            task_id="freeze_daily_scope_plan", map_index=-1, state="success"
        )
    ]
    mapped_rows = [SimpleNamespace(map_index=0, value={"scope": "a"})]
    singleton_task_ids = (
        "validate_whoscored_paid_alert_delivery",
        "validate_whoscored_catalog",
        "validate_whoscored_daily_slo",
        "validate_profile_refresh",
        "validate_whoscored_runtime",
        "freeze_daily_scope_plan",
        "report_whoscored_traffic",
    )
    singleton_rows = [
        SimpleNamespace(task_id=task_id, map_index=-1, value={})
        for task_id in singleton_task_ids
    ]
    if mutation == "mapped-xcom":
        mapped_rows = []
    elif mutation == "singleton-xcom":
        singleton_rows.pop()
    _patch_airflow(monkeypatch, [[dag_run], task_rows, mapped_rows, singleton_rows])
    monkeypatch.setattr(issuance, "is_countable_scheduled_run", lambda **_kwargs: True)
    monkeypatch.setattr(
        issuance,
        "terminal_task_states_evidence",
        lambda _rows: (
            {"count": 1, "sha256": SHA_D}
            if mutation == "task-state"
            else witness["task_states"]
        ),
    )
    monkeypatch.setattr(
        issuance, "mapped_scope_dq_evidence", lambda _rows: witness["scope_dq"]
    )
    monkeypatch.setattr(
        issuance, "scope_plan_sha256", lambda _value: witness["scope_plan_sha256"]
    )
    monkeypatch.setattr(
        issuance,
        "idempotency_evidence",
        lambda **_kwargs: (
            _idempotency(2) if mutation == "idempotency" else witness["idempotency"]
        ),
    )
    monkeypatch.setattr(
        issuance,
        "run_evidence_sha256",
        lambda **_kwargs: SHA_D if mutation == "evidence-xcom" else SHA_B,
    )

    with pytest.raises(RuntimeError, match=message):
        issuance._verify_terminal_runs([witness])


@pytest.mark.parametrize(
    ("state", "scope_mutation", "message"),
    (
        ("queued", "none", "lacks terminal current-wave proof"),
        ("running", "none", "lacks terminal current-wave proof"),
        ("failed", "wave", "belongs to a stale rollout wave"),
        ("failed", "cohort", "belongs to a stale rollout wave"),
        ("failed", "ranking", "belongs to a stale rollout wave"),
    ),
)
def test_newer_scheduled_run_cannot_hide_stale_predecessor_acceptance(
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    scope_mutation: str,
    message: str,
) -> None:
    witness, terminal = _terminal_witness()
    newer = SimpleNamespace(
        run_id="scheduled__2026-07-11T10:00:00+00:00",
        state=state,
        execution_date=terminal.execution_date + timedelta(days=1),
        end_date=(
            None
            if state in {"queued", "running"}
            else terminal.execution_date + timedelta(days=1, hours=1)
        ),
        run_type="scheduled",
        external_trigger=False,
        conf={},
    )
    scope_row = SimpleNamespace(map_index=-1, value={})
    _patch_airflow(
        monkeypatch,
        [[newer]] if state in {"queued", "running"} else [[newer], [scope_row]],
    )
    monkeypatch.setattr(issuance, "is_countable_scheduled_run", lambda **_kwargs: True)
    observed_scope = _scope_authority("wave-70")
    if scope_mutation == "wave":
        observed_scope["wave_id"] = "wave-20"
    elif scope_mutation == "cohort":
        observed_scope["cohort_sha256"] = SHA_D
    elif scope_mutation == "ranking":
        observed_scope["ranked_scope_ids_sha256"] = SHA_D
    monkeypatch.setattr(
        issuance,
        "normalized_scope_plan_authority",
        lambda _value: observed_scope,
    )

    with pytest.raises(RuntimeError, match=message):
        issuance._verify_newer_runs_belong_to_current_wave(
            [witness],
            expected_authority=_scope_authority("wave-70"),
        )


def test_newer_terminal_current_wave_run_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    witness, terminal = _terminal_witness()
    newer = SimpleNamespace(
        run_id="scheduled__2026-07-11T10:00:00+00:00",
        state="failed",
        execution_date=terminal.execution_date + timedelta(days=1),
        end_date=terminal.execution_date + timedelta(days=1, hours=1),
        run_type="scheduled",
        external_trigger=False,
        conf={},
    )
    _patch_airflow(
        monkeypatch,
        [[newer], [SimpleNamespace(map_index=-1, value={})]],
    )
    monkeypatch.setattr(issuance, "is_countable_scheduled_run", lambda **_kwargs: True)
    monkeypatch.setattr(
        issuance,
        "normalized_scope_plan_authority",
        lambda _value: _scope_authority("wave-70"),
    )

    issuance._verify_newer_runs_belong_to_current_wave(
        [witness],
        expected_authority=_scope_authority("wave-70"),
    )
