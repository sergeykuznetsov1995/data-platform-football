from datetime import timedelta
import importlib
import sys


def test_achievement_dag_is_bounded_paused_and_single_stream():
    sys.modules.pop("dag_ingest_transfermarkt_achievements", None)
    module = importlib.import_module("dag_ingest_transfermarkt_achievements")
    task = module.run_achievement_portion
    assert task.task_id == "run_achievement_portion"
    assert "run_transfermarkt_achievements.py" in task.bash_command
    assert task._init_kwargs["pool"] == "transfermarkt_proxy"
    assert task._init_kwargs["pool_slots"] == 1
    assert task._init_kwargs["max_active_tis_per_dag"] == 1
    assert task._init_kwargs["execution_timeout"] == timedelta(minutes=45)
    assert task.env["TM_RUN_ID"] == "{{ run_id }}"
    assert task.env["TM_REQUIRE_METERED_PROXY"] == "true"
    assert task.env["TRANSFERMARKT_REQUIRE_RAW_STORE"] == "true"
    assert task.env["TM_ACHIEVEMENT_MAX_RECIPIENTS"] == "180"
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
