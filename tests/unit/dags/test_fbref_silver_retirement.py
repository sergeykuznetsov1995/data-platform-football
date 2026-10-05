"""Repository contract for the retired legacy FBref Silver producer."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
PARENTS = (
    "dags/utils/fbref_current_dag_factory.py",
    "dags/dag_backfill_fbref.py",
    "dags/dag_replay_fbref.py",
)


def test_legacy_fbref_silver_dag_and_sql_are_absent():
    legacy_sql = tuple(
        (ROOT / "dags" / "sql" / "silver").glob("fbref_*.sql")
    )

    assert not (ROOT / "dags" / "dag_transform_fbref_silver.py").exists()
    assert legacy_sql == ()


def test_legacy_fbref_silver_openmetadata_descriptions_are_absent():
    legacy_descriptions = tuple(
        (ROOT / "configs" / "openmetadata" / "descriptions").glob(
            "silver_fbref_*.yaml"
        )
    )

    assert legacy_descriptions == ()


@pytest.mark.parametrize("relative_path", PARENTS)
def test_fbref_bronze_parents_do_not_trigger_legacy_silver(relative_path):
    source = (ROOT / relative_path).read_text(encoding="utf-8")

    assert "dag_transform_fbref_silver" not in source
    assert "fbref_silver__" not in source
    assert "trigger_silver_transform" not in source
