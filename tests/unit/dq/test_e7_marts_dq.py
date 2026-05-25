"""Unit tests for E7 / T7 — DQ checks registered for the 3 BI marts.

Why
---
``validate_gold_quality()`` materialises a static list of ``CHECK.*``
objects before calling ``run_checks(...)`` against Trino. This test
intercepts ``run_checks`` so we can introspect the registered Check
list **without** any DB / Airflow runtime. We assert that:

* every E7 mart (``mart_scouting_radar`` / ``mart_referee_dashboard`` /
  ``mart_event_heatmap``) has at least one DQ check registered;
* PK uniqueness checks (``no_duplicates``) are ERROR-severity;
* the leakage guard for ``mart_scouting_radar.xg_l5``
  (``point_in_time``) is ERROR-severity.

If any of these contracts regress (someone deletes a check, flips
ERROR→WARNING, etc.) the test fails immediately — long before the DAG
actually runs against Trino.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import gold_tasks  # noqa: E402
from utils.data_quality import RunReport  # noqa: E402

pytestmark = pytest.mark.unit


MART_TABLES = (
    'gold.mart_scouting_radar',
    'gold.mart_referee_dashboard',
    'gold.mart_event_heatmap',
)


def _capture_checks(monkeypatch) -> list:
    """Run validate_gold_quality with run_checks stubbed; return the list
    of Check objects that would have been executed."""
    captured: list = []

    def fake_run(checks, raise_on_error=False):
        captured.extend(checks)
        return RunReport(results=[])

    # Patch BOTH the source module symbol AND the alias re-imported inside
    # validate_gold_quality (`from utils.data_quality import ... run_checks`).
    # The function does the import at call time so patching the source
    # module is sufficient — the late binding picks up the stub.
    monkeypatch.setattr('utils.data_quality.run_checks', fake_run)
    # Inline-helpers (_append_train_test_disjointness_check,
    # _append_dim_standings_coverage_check) issue their own Trino queries —
    # neutralise so the test stays offline.
    monkeypatch.setattr(gold_tasks,
                        '_append_train_test_disjointness_check',
                        lambda report: None)
    monkeypatch.setattr(gold_tasks,
                        '_append_dim_standings_coverage_check',
                        lambda report: None)
    # telegram alerts make HTTP calls — stub.
    monkeypatch.setattr('utils.alerts.telegram_dq_summary',
                        lambda *a, **kw: None)

    gold_tasks.validate_gold_quality()
    return captured


def test_marts_dq_registered(monkeypatch):
    """Every E7 mart must have ≥1 DQ check registered."""
    checks = _capture_checks(monkeypatch)
    tables_seen = {c.params.get('table') for c in checks if 'table' in c.params}
    for mart in MART_TABLES:
        assert mart in tables_seen, (
            f"No DQ check registered for {mart} in validate_gold_quality(); "
            f"observed mart-namespace tables: "
            f"{sorted(t for t in tables_seen if t and 'mart_' in t)}"
        )


def test_dq_severity_levels(monkeypatch):
    """no_duplicates on every mart + point_in_time on mart_scouting_radar
    must be ERROR-severity (data integrity / leakage guards)."""
    checks = _capture_checks(monkeypatch)

    # no_duplicates per mart — all ERROR.
    for mart in MART_TABLES:
        dups = [c for c in checks
                if c.kind == 'no_duplicates' and c.params.get('table') == mart]
        assert dups, f"no_duplicates check missing for {mart}"
        for c in dups:
            assert c.severity == 'ERROR', (
                f"{c.name}: expected ERROR severity, got {c.severity}"
            )

    # point_in_time on mart_scouting_radar.xg_l5 — ERROR (leakage guard).
    pit = [c for c in checks
           if c.kind == 'point_in_time'
           and c.params.get('table') == 'gold.mart_scouting_radar']
    assert pit, "point_in_time leakage guard missing for mart_scouting_radar"
    for c in pit:
        assert c.severity == 'ERROR', (
            f"{c.name}: leakage guard must be ERROR, got {c.severity}"
        )
