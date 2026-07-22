"""Production shape and ownership tests for Transfermarkt historical backfill."""

from __future__ import annotations

import importlib
import json
import sys
from types import SimpleNamespace

import pytest

from utils import transfermarkt_backfill_state as state


def _reload():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dag_backfill_transfermarkt", None)
    sys.modules.pop("dags.dag_backfill_transfermarkt", None)
    return importlib.import_module("dag_backfill_transfermarkt")


@pytest.fixture
def module():
    return _reload()


def _bash(task_id: str):
    from airflow.operators.bash import BashOperator

    return next(item for item in BashOperator._instances if item.task_id == task_id)


def _python(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(item for item in PythonOperator._instances if item.task_id == task_id)


def test_dag_is_continuous_single_run_and_has_no_scope_selectors(module):
    assert module.dag.dag_id == "dag_backfill_transfermarkt"
    assert module.dag.schedule == "@continuous"
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["catchup"] is False
    params = module.dag._dag_kwargs["params"]
    assert set(params) == {"max_batch", "resume_platform_block"}
    assert params["max_batch"].default == 8
    assert params["max_batch"]._kw["maximum"] == 8
    assert params["resume_platform_block"].default is False


def test_paid_task_is_native_only_raw_first_and_uses_dedicated_pool(module):
    task = _bash("run_historical_scope")
    assert task.is_mapped is True
    assert task._expand_kwargs["env"].operator.task_id == "plan_historical_batch"
    assert task._init_kwargs["pool"] == "transfermarkt_backfill_proxy"
    assert task._init_kwargs["pool_slots"] == 1
    assert task._init_kwargs["priority_weight"] == 10
    assert task._init_kwargs["max_active_tis_per_dag"] == 1
    assert task._init_kwargs["do_xcom_push"] is False
    command = task.bash_command
    assert "--write-mode native-only" in command
    assert "--refresh-mode historical" in command
    assert "--standing-policy" in command
    assert "exit 0" in command
    assert "trigger" not in command.lower()


def test_strict_preflight_precedes_paid_task_and_finalizer_is_all_done(module):
    preflight = _python("strict_cutover_preflight")
    planner = _python("plan_historical_batch")
    paid = _bash("run_historical_scope")
    finalizer = _python("finalize_historical_batch")
    cooldown = _python("wait_before_next_continuous_run")
    assert planner.task_id in preflight.downstream_task_ids
    assert paid.task_id in planner.downstream_task_ids
    assert finalizer.task_id in paid.downstream_task_ids
    assert finalizer._init_kwargs["trigger_rule"] == "all_done"
    assert cooldown.task_id in finalizer.downstream_task_ids
    assert cooldown._init_kwargs["mode"] == "reschedule"
    assert cooldown._init_kwargs["trigger_rule"] == "all_done"
    assert {
        task._init_kwargs["pool"]
        for task in (preflight, planner, finalizer, cooldown)
    } == {module.BACKFILL_CONTROL_POOL}


def test_mapped_environment_carries_frozen_campaign_not_proxy_secrets(module):
    payload = {
        "scope_id": "GB1__2020",
        "resume_cycle_id": "a" * 64,
        "result_paths": {},
    }
    environment = module._environment_for_scope(
        payload=payload,
        preflight={"revision": 9, "candidate_slot": "b"},
        policy_hash="b" * 64,
        run_id="scheduled__one",
        batch_id="c" * 64,
        lease_id="d" * 64,
        claim_generation=1,
    )

    assert environment["TM_DAG_ID"] == "dag_backfill_transfermarkt"
    assert environment["TM_WRITE_MODE"] == "native-only"
    assert environment["TM_REFRESH_MODE"] == "historical"
    assert environment["TRANSFERMARKT_REQUIRE_RAW_STORE"] == "true"
    assert environment["TM_BACKFILL_CLAIM_GENERATION"] == "1"
    assert environment["TM_BACKFILL_ATTEMPT_SEQUENCE"] == "1"
    assert json.loads(environment["TM_SCOPE_PAYLOAD_JSON"]) == payload
    assert "TM_PROXY_CONTROL_TOKEN" not in environment
    assert "TM_BACKFILL_PROXY_CONTROL_TOKEN" not in environment


def test_daily_task_has_strictly_higher_airflow_priority(module):
    sys.modules.pop("dag_ingest_transfermarkt", None)
    sys.modules.pop("dags.dag_ingest_transfermarkt", None)
    dag_ingest_transfermarkt = importlib.import_module("dag_ingest_transfermarkt")

    daily = next(
        item
        for item in __import__(
            "airflow.operators.bash", fromlist=["BashOperator"]
        ).BashOperator._instances
        if item.task_id == "run_exact_child_cycle"
    )
    assert (
        daily._init_kwargs["priority_weight"]
        > _bash("run_historical_scope")._init_kwargs["priority_weight"]
    )
    assert dag_ingest_transfermarkt.dag.dag_id == "dag_ingest_transfermarkt"


def test_finalizer_can_never_authorize_silver(module, monkeypatch):
    from utils import transfermarkt_backfill_finalize as finalize

    monkeypatch.setattr(
        finalize,
        "reconcile_campaign_completion",
        lambda: {"status": "idle", "silver_trigger_allowed": False},
    )

    class _Ti:
        @staticmethod
        def xcom_pull(task_ids):
            assert task_ids == "plan_historical_batch"
            return []

    result = module._finalize_historical_batch(ti=_Ti())
    assert result == {"status": "idle", "silver_trigger_allowed": False}


class _PreflightTi:
    @staticmethod
    def xcom_pull(task_ids):
        assert task_ids == "strict_cutover_preflight"
        return {
            "paid_io_allowed": True,
            "revision": 7,
            "candidate_slot": "b",
        }


def _planner_context(module, *, resume_platform_block):
    return {
        "ti": _PreflightTi(),
        "dag": module.dag,
        "run_id": "manual__incident-resume",
        "params": {
            "max_batch": 8,
            "resume_platform_block": resume_platform_block,
        },
    }


def test_planner_converges_active_open_incident_before_refusing_source_work(
    module,
    monkeypatch,
):
    campaign = SimpleNamespace(
        campaign_id="a" * 64,
        policy_sha256="c" * 64,
        status=state.CampaignStatus.ACTIVE,
    )
    blocked = SimpleNamespace(
        campaign_id=campaign.campaign_id,
        status=state.CampaignStatus.BLOCKED_PLATFORM,
    )
    incident_batch = SimpleNamespace(batch_id="b" * 64)

    class _Repository:
        def __init__(self):
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ensure_schema(self):
            pass

        def open_campaign(self):
            return campaign

        def load_scopes(self, campaign_id):
            assert campaign_id == campaign.campaign_id
            return ()

        def reconcile_open_platform_incident(self, current, *, now):
            assert current is campaign
            self.calls.append("incident_converged")
            return blocked, incident_batch

        def reconcile_platform_block(self, current, *, now):
            assert current is blocked
            self.calls.append("blocked_reconciled")

    repository = _Repository()
    monkeypatch.setattr(
        module.BackfillStateRepository,
        "connect",
        lambda: repository,
    )
    monkeypatch.setattr(
        module,
        "_load_backfill_policy",
        lambda: SimpleNamespace(policy_hash="c" * 64),
    )

    with pytest.raises(
        module.AirflowException,
        match="resume_platform_block=true",
    ):
        module._plan_historical_batch(
            **_planner_context(module, resume_platform_block=False)
        )

    assert repository.calls == ["incident_converged", "blocked_reconciled"]


def test_explicit_scope_platform_resume_maps_one_new_source_attempt(
    module,
    monkeypatch,
):
    campaign_id = "a" * 64
    batch_id = "b" * 64
    scope_id = "GB1__2020"
    campaign = SimpleNamespace(
        campaign_id=campaign_id,
        registry_snapshot_id="registry-1",
        policy_sha256="c" * 64,
        status=state.CampaignStatus.ACTIVE,
    )
    blocked = SimpleNamespace(
        campaign_id=campaign_id,
        registry_snapshot_id="registry-1",
        status=state.CampaignStatus.BLOCKED_PLATFORM,
    )
    scope = SimpleNamespace(
        target=SimpleNamespace(scope_id=scope_id),
        status=state.ScopeStatus.RUNNING,
        lease_id="lease-1",
        attempt_count=1,
    )
    batch = SimpleNamespace(
        batch_id=batch_id,
        scope_ids=(scope_id,),
        scope_claim_generations=(1,),
        status=state.BatchStatus.RUNNING,
    )

    class _Repository:
        def __init__(self):
            self.recover_called = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ensure_schema(self):
            pass

        def open_campaign(self):
            return campaign

        def load_scopes(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return (scope,)

        def reconcile_open_platform_incident(self, current, *, now):
            assert current is campaign
            return blocked, batch

        def resume_platform_campaign(self, current, *, lease_owner, now):
            assert current is blocked
            return campaign, (scope,), batch

        def load_batches(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return (batch,)

        def load_attempts(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return ()

        def recover_batch_claim(self, *_args, **_kwargs):
            self.recover_called = True
            raise AssertionError("incident resume must not reopen scope work")

    repository = _Repository()
    monkeypatch.setattr(
        module.BackfillStateRepository,
        "connect",
        lambda: repository,
    )
    monkeypatch.setattr(
        module,
        "_load_backfill_policy",
        lambda: SimpleNamespace(policy_hash="c" * 64),
    )
    monkeypatch.setattr(module, "read_promoted_registry", lambda **_kwargs: ({},))
    monkeypatch.setattr(
        module,
        "select_recoverable_batch",
        lambda *_args: batch,
    )
    monkeypatch.setattr(
        module,
        "plan_existing_batch",
        lambda *_args, **_kwargs: (
            {
                "scope_id": scope_id,
                "resume_cycle_id": campaign_id,
                "child_cycle_id": "tm-child-one",
                "result_paths": {
                    "base_dir": "/tmp/tm-backfill-test",
                    "entity_staging_dir": "/tmp/tm-backfill-test/entities",
                },
            },
        ),
    )
    monkeypatch.setattr(
        module, "has_matching_scope_attempt_result", lambda **_kwargs: False
    )

    planned = module._plan_historical_batch(
        **_planner_context(module, resume_platform_block=True)
    )

    assert repository.recover_called is False
    assert len(planned) == 1
    assert planned[0]["TM_BACKFILL_FINALIZE_ONLY"] == "false"


def test_policy_rotation_blocks_before_claim_or_registry_read(module, monkeypatch):
    campaign = SimpleNamespace(
        campaign_id="a" * 64,
        policy_sha256="b" * 64,
        status=state.CampaignStatus.ACTIVE,
    )

    class _Repository:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ensure_schema(self):
            pass

        def open_campaign(self):
            return campaign

        def load_scopes(self, campaign_id):
            assert campaign_id == campaign.campaign_id
            return ()

        def reconcile_open_platform_incident(self, *_args, **_kwargs):
            raise AssertionError("policy drift must stop before reconciliation")

    monkeypatch.setattr(
        module.BackfillStateRepository, "connect", lambda: _Repository()
    )
    monkeypatch.setattr(
        module,
        "_load_backfill_policy",
        lambda: SimpleNamespace(policy_hash="c" * 64),
    )
    monkeypatch.setattr(
        module,
        "read_promoted_registry",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("policy drift must stop before registry planning")
        ),
    )

    with pytest.raises(module.AirflowException, match="policy changed"):
        module._plan_historical_batch(
            **_planner_context(module, resume_platform_block=False)
        )


def test_post_claim_environment_failure_is_bound_to_exact_durable_batch(
    module,
    monkeypatch,
):
    campaign_id = "a" * 64
    batch_id = "b" * 64
    scope_id = "GB1__2020"
    campaign = SimpleNamespace(
        campaign_id=campaign_id,
        registry_snapshot_id="registry-1",
        policy_sha256="c" * 64,
        status=state.CampaignStatus.ACTIVE,
    )
    scope = SimpleNamespace(
        target=SimpleNamespace(scope_id=scope_id),
        status=state.ScopeStatus.RUNNING,
        lease_id="lease-1",
        attempt_count=0,
    )
    batch = SimpleNamespace(
        batch_id=batch_id,
        scope_ids=(scope_id,),
        scope_claim_generations=(1,),
        status=state.BatchStatus.RUNNING,
    )

    class _Repository:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ensure_schema(self):
            pass

        def open_campaign(self):
            return campaign

        def load_scopes(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return (scope,)

        def reconcile_open_platform_incident(self, current, *, now):
            assert current is campaign
            return campaign, None

        def load_batches(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return (batch,)

        def recover_batch_claim(self, current, scopes, **_kwargs):
            assert current is batch
            return scopes

        def load_attempts(self, current_campaign_id):
            assert current_campaign_id == campaign_id
            return ()

    monkeypatch.setattr(
        module.BackfillStateRepository, "connect", lambda: _Repository()
    )
    monkeypatch.setattr(
        module,
        "_load_backfill_policy",
        lambda: SimpleNamespace(policy_hash="c" * 64),
    )
    monkeypatch.setattr(module, "read_promoted_registry", lambda **_kwargs: ({},))
    monkeypatch.setattr(module, "select_recoverable_batch", lambda *_args: batch)
    monkeypatch.setattr(
        module,
        "plan_existing_batch",
        lambda *_args, **_kwargs: ({
            "scope_id": scope_id,
            "resume_cycle_id": campaign_id,
            "child_cycle_id": "tm-child-one",
            "result_paths": {
                "base_dir": "/tmp/tm-backfill-test",
                "entity_staging_dir": "/tmp/tm-backfill-test/entities",
            },
        },),
    )
    monkeypatch.setattr(
        module,
        "has_matching_scope_attempt_result",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("filesystem drift")),
    )
    incidents = []
    monkeypatch.setattr(
        module,
        "_persist_planner_platform_incident",
        lambda **kwargs: incidents.append(kwargs),
    )

    with pytest.raises(module.AirflowException, match="attempt fence"):
        module._plan_historical_batch(
            **_planner_context(module, resume_platform_block=False)
        )

    assert len(incidents) == 1
    assert incidents[0]["campaign_id"] == campaign_id
    assert incidents[0]["batch_id"] == batch_id
    assert isinstance(incidents[0]["exc"], module.AirflowException)


def test_cooldown_leaf_preserves_upstream_failure(module):
    class _Ti:
        @staticmethod
        def xcom_pull(*, task_ids, key):
            assert task_ids == "plan_historical_batch"
            assert key == "next_poll_at"
            return "2000-01-01T00:00:00+00:00"

    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id="plan_historical_batch", state="failed")
        ]
    )
    with pytest.raises(module.AirflowException, match="upstream task failure"):
        module._backfill_poll_ready(ti=_Ti(), dag_run=dag_run)
