"""Static contracts for the dedicated WhoScored production CI workflow."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "whoscored-ci.yml"


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_every_pull_request_runs_the_cross_boundary_contract():
    text = _workflow_text()
    trigger = text.split("permissions:", 1)[0]

    assert "pull_request:" in trigger
    assert "paths:" not in trigger


def test_real_airflow_211_import_gate_is_not_a_stub_only_test():
    text = _workflow_text()

    assert '"apache-airflow==2.11.2"' in text
    assert "constraints-2.11.2/constraints-3.11.txt" in text
    assert "airflow dags list-import-errors" in text
    assert "from airflow.models import DagBag" in text
    for dag_id in (
        "dag_ingest_whoscored",
        "dag_backfill_whoscored",
        "dag_backup_whoscored_storage",
    ):
        assert dag_id in text


def test_ci_runs_public_writer_and_capacity_contracts():
    text = _workflow_text()

    assert "tests/unit/scrapers/test_iceberg_writer.py" in text
    assert "bench_whoscored_capacity.py" in text
    assert "rg --files tests" in text
