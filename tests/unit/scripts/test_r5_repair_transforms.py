from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(ROOT / "dags"))


def _load_repair_script():
    spec = importlib.util.spec_from_file_location(
        "r5_repair_transforms_under_test",
        ROOT / "dags/scripts/r5_repair_transforms.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
def test_repair_phase_validates_scope_and_holds_global_publication_lock(
    monkeypatch,
):
    repair = _load_repair_script()
    from scrapers.fbref.control import ControlStore
    from utils import fbref_pipeline_tasks

    source_run_id = "11111111-1111-4111-8111-111111111111"
    validator = MagicMock(return_value={"status": "ready"})
    monkeypatch.setattr(
        fbref_pipeline_tasks, "validate_fbref_publication_scope", validator
    )
    control = MagicMock()
    monkeypatch.setattr(ControlStore, "from_env", lambda: control)
    phase = MagicMock()
    monkeypatch.setattr(repair, "PHASES", {"xref": phase})

    repair.run_locked_phase("xref", source_run_id)

    validator.assert_called_once_with(control_run_id=source_run_id)
    control.migrate.assert_called_once_with()
    repair_run_id = control.create_run.call_args.kwargs["run_id"]
    control.start_run.assert_called_once_with(repair_run_id)
    control.acquire_publication_lock.assert_called_once_with(
        repair_run_id,
        dag_id="r5_repair_transforms",
        ttl_seconds=(
            fbref_pipeline_tasks.FBREF_PUBLICATION_LOCK_TTL_SECONDS
        ),
    )
    phase.assert_called_once_with(source_run_id)
    control.release_publication_lock.assert_called_once_with(repair_run_id)
    control.finish_run.assert_called_once_with(
        repair_run_id, succeeded=True
    )


@pytest.mark.unit
def test_repair_phase_stops_before_any_write_for_unknown_scope(monkeypatch):
    repair = _load_repair_script()
    from scrapers.fbref.control import ControlStore
    from utils import fbref_pipeline_tasks

    validator = MagicMock(side_effect=RuntimeError("scope absent"))
    monkeypatch.setattr(
        fbref_pipeline_tasks, "validate_fbref_publication_scope", validator
    )
    control_factory = MagicMock()
    monkeypatch.setattr(ControlStore, "from_env", control_factory)

    with pytest.raises(RuntimeError, match="scope absent"):
        repair.run_locked_phase(
            "xref", "11111111-1111-4111-8111-111111111111"
        )

    control_factory.assert_not_called()
