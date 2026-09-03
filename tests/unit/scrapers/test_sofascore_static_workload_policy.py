"""#1245: the SofaScore paid budget comes from git, not from a paid canary.

The retired v3 artifact bound every class to a four-hour paid measurement *and*
to the exact fingerprint of the runtime tree, so any scraper edit invalidated
the budget and stopped the whole source until a new canary was run.  The static
v4 policy declares the same class shapes with fixed caps; the spend is bounded
by the gateway ceilings and by the signed allocation of each task.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.sofascore import workload_plan as module
from scrapers.sofascore.workload_plan import (
    MATCH_BATCH_SIZE,
    PLAYER_BATCH_SIZE,
    WORKLOAD_POLICY_SCHEMA_VERSION,
    WORKLOAD_STATIC_BUDGET_DERIVATION,
    WorkloadPolicyUnavailable,
    load_static_workload_policy,
    match_workload_class,
    player_workload_class,
    production_season_shape,
    season_workload_class,
    workload_shape_digest,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SHIPPED_POLICY = REPO_ROOT / "configs" / "sofascore" / "workload_policy.json"
# The static DagRun ceiling the three gateway lanes are started with
# (``--dagrun-budget-bytes`` in deploy/sofascore/gateway.compose.yaml).
GATEWAY_DAGRUN_BUDGET_BYTES = 8_000_000


def _payload() -> dict:
    return json.loads(SHIPPED_POLICY.read_text(encoding="utf-8"))


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


@pytest.mark.unit
def test_shipped_policy_loads_without_any_measurement_evidence():
    policy = load_static_workload_policy(SHIPPED_POLICY)

    assert policy.artifact_id == hashlib.sha256(
        SHIPPED_POLICY.read_bytes()
    ).hexdigest()
    assert match_workload_class() in policy.classes
    assert player_workload_class() in policy.classes
    for measured in policy.classes.values():
        assert measured.hard_task_bytes > 0
        assert measured.sample_count == 0
        assert measured.distinct_proxy_exits == 0
        assert measured.measured_tournament_ids == ()
    assert policy.classes[match_workload_class()].max_units == MATCH_BATCH_SIZE
    assert policy.classes[player_workload_class()].max_units == PLAYER_BATCH_SIZE

    payload = _payload()
    assert payload["schema_version"] == WORKLOAD_POLICY_SCHEMA_VERSION
    assert payload["budget_derivation"] == WORKLOAD_STATIC_BUDGET_DERIVATION
    assert "runtime_fingerprint" not in payload
    assert "verified" not in payload


@pytest.mark.unit
def test_editing_scraper_code_cannot_invalidate_the_budget(monkeypatch):
    """The loader must never consult the runtime fingerprint of the tree."""

    def explode(*args, **kwargs):  # pragma: no cover - must not be reached
        raise AssertionError("static policy consulted the runtime fingerprint")

    monkeypatch.setattr(module, "validate_runtime_fingerprint", explode)

    assert load_static_workload_policy(SHIPPED_POLICY).classes


@pytest.mark.unit
def test_every_class_cap_fits_inside_the_static_gateway_dagrun_ceiling():
    policy = load_static_workload_policy(SHIPPED_POLICY)

    assert max(
        measured.hard_task_bytes for measured in policy.classes.values()
    ) <= GATEWAY_DAGRUN_BUDGET_BYTES


@pytest.mark.unit
def test_every_declared_season_shape_is_reachable_from_the_production_builder():
    policy = load_static_workload_policy(SHIPPED_POLICY)
    season_classes = {
        name
        for name, measured in policy.classes.items()
        if measured.scope == "season"
    }

    reachable = set()
    for season_format in ("calendar_year", "split_year"):
        for band in ("8_15", "16_20", "21_32", "33_48"):
            shape = production_season_shape(
                season_format=season_format,
                team_count_band=band,
                max_pages_per_direction=50,
            )
            name = season_workload_class(shape)
            if name in season_classes:
                reachable.add(name)
                assert policy.classes[name].shape_digest == workload_shape_digest(
                    shape
                )

    assert reachable == season_classes


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(schema_version=3), "schema_version must be"),
        (lambda value: value.update(source="fbref"), "source must be sofascore"),
        (lambda value: value.update(meter="hand-counted"), "untrusted meter"),
        (
            lambda value: value.update(budget_derivation="max_observed"),
            "budget_derivation must be",
        ),
        (lambda value: value.update(verified=True), "cannot carry 'verified'"),
        (
            lambda value: value.update(runtime_fingerprint={"digest": "0" * 64}),
            "cannot carry 'runtime_fingerprint'",
        ),
        (
            lambda value: value.update(budget_multiplier=2),
            "cannot carry 'budget_multiplier'",
        ),
        (lambda value: value.update(workload_classes={}), "non-empty object"),
    ],
)
def test_static_policy_rejects_a_measurement_artifact(tmp_path, mutate, message):
    payload = _payload()
    mutate(payload)

    with pytest.raises(WorkloadPolicyUnavailable, match=message):
        load_static_workload_policy(_write(tmp_path / "bad.json", payload))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("samples", []),
        ("measured_tournament_ids", [16, 17]),
        ("budget_multiplier", 2),
    ],
)
def test_static_policy_class_rejects_measurement_fields(tmp_path, key, value):
    payload = _payload()
    payload["workload_classes"][match_workload_class()][key] = value

    with pytest.raises(WorkloadPolicyUnavailable, match=f"cannot carry {key!r}"):
        load_static_workload_policy(_write(tmp_path / "bad.json", payload))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["workload_classes"][match_workload_class()].update(
                max_units=24
            ),
            "must declare exactly 25 units",
        ),
        (
            lambda value: value["workload_classes"][match_workload_class()].update(
                hard_task_bytes=0
            ),
            "hard_task_bytes must be a positive integer",
        ),
        (
            lambda value: value["workload_classes"][match_workload_class()].update(
                shape_digest="0" * 64
            ),
            "shape_digest does not match its shape",
        ),
        (
            lambda value: value["workload_classes"][match_workload_class()].update(
                required_endpoints=["event"]
            ),
            "must equal the endpoints of its shape",
        ),
    ],
)
def test_static_policy_still_pins_every_class_to_its_shape(tmp_path, mutate, message):
    payload = _payload()
    mutate(payload)

    with pytest.raises(WorkloadPolicyUnavailable, match=message):
        load_static_workload_policy(_write(tmp_path / "bad.json", payload))


@pytest.mark.unit
def test_static_policy_rejects_a_renamed_or_duplicated_class(tmp_path):
    payload = _payload()
    match = payload["workload_classes"].pop(match_workload_class())
    payload["workload_classes"]["match_batch_25_deadbeefdeadbeef"] = match

    with pytest.raises(WorkloadPolicyUnavailable, match="named after its scope"):
        load_static_workload_policy(_write(tmp_path / "renamed.json", payload))

    payload = _payload()
    payload["workload_classes"]["match_batch_25_copy"] = copy.deepcopy(
        payload["workload_classes"][match_workload_class()]
    )

    with pytest.raises(WorkloadPolicyUnavailable, match="duplicate shape_digest"):
        load_static_workload_policy(_write(tmp_path / "dup.json", payload))
