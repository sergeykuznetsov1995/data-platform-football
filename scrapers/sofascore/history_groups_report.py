"""Read-only, offline host summary of a pinned history checkpoint contract.

The checkpoint schema has no release field. Release pins identify the renderer,
not the provenance of checkpoint observations. Nothing here runs a collector.
"""
from __future__ import annotations

import ast
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

GROUPS = ("1", "2", "july", "3", "4", "5")
LABELS = {"1": "текущие", "2": "последние завершённые", "july": "июльский хвост",
          "3": "10 предыдущих", "4": "глубже", "5": "спорные"}
COUNTERS = ("finished", "closed", "remaining", "unknown_scopes", "scopes",
            "pending", "mapping_required", "parked", "quarantined", "deferred")


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _time(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    _require(isinstance(value, datetime) and value.tzinfo is not None and
             value.utcoffset() is not None, "timestamp_requires_timezone")
    return value.astimezone(timezone.utc)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("nonfinite_json_number")


def _renderer(root, release_id, controller_sha256):
    _require(isinstance(release_id, str) and re.fullmatch(r"[0-9a-f]{40}", release_id),
             "invalid_release_id")
    root = Path(root)
    _require(root.is_absolute(), "release_root_not_absolute")
    root = root.resolve(strict=True)
    name = root.name.removeprefix("release-")
    _require(root.name.startswith("release-") and 8 <= len(name) <= 40 and
             release_id.startswith(name), "release_root_identity_mismatch")
    # freeze_release.sh produces detached immutable release trees. Reading HEAD
    # avoids git subprocesses and proves the full ID beyond the directory prefix.
    _require((root / ".git/HEAD").read_text().strip() == release_id,
             "release_head_identity_mismatch")
    path = (root / "scrapers/sofascore/history_controller.py").resolve(strict=True)
    _require(path.is_relative_to(root), "controller_outside_release_root")
    source = path.read_bytes()
    _require(isinstance(controller_sha256, str) and
             re.fullmatch(r"[0-9a-f]{64}", controller_sha256) and
             hashlib.sha256(source).hexdigest() == controller_sha256,
             "controller_sha256_mismatch")
    # Execute only the reviewed renderer and its two literal constants. Importing
    # the controller would load state/inventory dependencies into the host process.
    tree = ast.parse(source, filename=str(path))
    selected = []
    found = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id in ("GROUPS", "LABELS"):
                found[target.id] = ast.literal_eval(node.value)
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "render_summary":
            selected.append(node)
    _require(found == {"GROUPS": GROUPS, "LABELS": LABELS} and len(selected) == 3,
             "renderer_contract_mismatch")
    namespace = {"Mapping": dict}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["render_summary"], str(root)


def _validate(value, campaign_id, checked_at, max_age_seconds):
    _require(isinstance(campaign_id, str) and bool(campaign_id), "missing_campaign_pin")
    _require(isinstance(value, dict) and type(value.get("schema_version")) is int and
             value["schema_version"] == 1, "checkpoint_schema_mismatch")
    _require(value.get("campaign_id") == campaign_id and
             isinstance(value.get("positions"), dict), "checkpoint_campaign_mismatch")
    run = value.get("run")
    _require(isinstance(run, dict) and isinstance(run.get("plan"), list) and
             isinstance(run.get("run_id"), str) and bool(run["run_id"]), "missing_run_plan")
    _require(run.get("plan_digest") == _digest(run["plan"]), "plan_digest_mismatch")
    mode = run.get("mode", "batch")
    _require(mode in ("batch", "slots"), "unsupported_run_mode")
    if mode == "slots":
        _require(run.get("slot_digest") == _digest({k: v for k, v in run.items()
                                                    if k != "slot_digest"}),
                 "slot_digest_mismatch")
    observed = _time(value.get("observed_at"))
    age = (checked_at - observed).total_seconds()
    _require(age >= 0, "checkpoint_observation_in_future")
    _require(age <= max_age_seconds, "checkpoint_stale")
    groups = value.get("groups")
    _require(isinstance(groups, dict) and set(groups) == set(GROUPS), "six_groups_required")
    for group in GROUPS:
        row = groups[group]
        _require(isinstance(row, dict), "invalid_group_row")
        _require(all(type(row.get(k)) is int and row[k] >= 0 for k in COUNTERS),
                 "invalid_group_counters")
        total, closed = row["finished"], row["closed"]
        _require(closed <= total and row["remaining"] == total - closed,
                 "group_coverage_mismatch")
        _require(sum(row[k] for k in ("pending", "mapping_required", "parked",
                                     "quarantined", "deferred")) <= row["scopes"],
                 "group_scope_counters_mismatch")
        expected = round(100 * closed / total, 4) if total else None
        percent = row.get("percent")
        _require((expected is None and percent is None) or
                 (expected is not None and type(percent) in (int, float) and
                  math.isfinite(percent) and math.isclose(percent, expected, abs_tol=0.00005)),
                 "group_percent_mismatch")
        gate = row["unknown_scopes"] == 0 and (total == 0 or closed * 100 >= total * 99)
        _require(type(row.get("gate_passed")) is bool and row["gate_passed"] == gate,
                 "group_gate_mismatch")
    return groups, mode, observed


def _unknown(result, reason):
    result.update(status="UNKNOWN", reason=reason,
                  groups={g: {"status": "UNKNOWN"} for g in GROUPS})
    result["summary"] = "• очередь истории SofaScore: UNKNOWN (" + reason + "); " + "; ".join(
        f"{g} {LABELS[g]}: UNKNOWN, закрыто/всего ?, процент ?, долг ?, "
        "неизвестно ?, pending+mapping ?, parked ?, quarantine ?" for g in GROUPS)
    return result


def build_report(*, checkpoint_path, campaign_id, release_root, release_id,
                 controller_sha256, checked_at=None, max_age_seconds=86400):
    """Return schema1 evidence; invalid/missing/stale inputs always yield UNKNOWN.

    max_age_seconds is a report freshness limit, not an acceptance criterion.
    checked_at and observed_at are distinct UTC timestamps. No checkpoint writes.
    """
    result = {"schema_version": 1, "campaign_id": campaign_id, "release_id": release_id,
              "controller_sha256": controller_sha256, "observed_at": None,
              "checked_at": None, "checkpoint_sha256": None,
              "release_identity_scope": "renderer_only_checkpoint_has_no_release_field"}
    try:
        checked = _time(checked_at if checked_at is not None else datetime.now(timezone.utc))
        result["checked_at"] = checked.isoformat()
        _require(type(max_age_seconds) in (int, float) and math.isfinite(max_age_seconds) and
                 max_age_seconds > 0, "invalid_freshness_limit")
        renderer, resolved_root = _renderer(release_root, release_id, controller_sha256)
        result["release_root"] = resolved_root
        raw = Path(checkpoint_path).read_bytes()
        result["checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_reject_constant)
        if isinstance(value, dict) and isinstance(value.get("observed_at"), str):
            result["observed_at"] = value["observed_at"]
        groups, mode, observed = _validate(value, campaign_id, checked, max_age_seconds)
        output = {}
        for group in GROUPS:
            row = dict(groups[group])
            row["status"] = ("UNKNOWN" if row["unknown_scopes"] else "BLOCKED"
                             if not row["gate_passed"] else "EMPTY"
                             if row["finished"] == 0 else "PASS")
            output[group] = row
        status = ("UNKNOWN" if any(r["status"] == "UNKNOWN" for r in output.values()) else
                  "BLOCKED" if any(r["status"] == "BLOCKED" for r in output.values()) else "PASS")
        status_line = ", ".join(f"{g}={output[g]['status']}" for g in GROUPS)
        result.update(status=status, reason=None, groups=output, mode=mode,
                      observed_at=observed.isoformat(),
                      plan_digest=value["run"]["plan_digest"],
                      slot_digest=value["run"].get("slot_digest"),
                      summary=f"{renderer(groups)}; {status_line}; observed_at={observed.isoformat()}, "
                              f"checked_at={checked.isoformat()}, release={release_id}")
        return result
    except (OSError, ValueError, TypeError, KeyError, OverflowError, SyntaxError, RecursionError) as error:
        # Never print file contents, paths or exception messages from the host env.
        # Validation errors use our fixed tokens; external errors use their type.
        reason = str(error) if type(error) is ValueError and re.fullmatch(r"[a-z_0-9]+", str(error)) else type(error).__name__
        return _unknown(result, reason)


def host_history_groups(*, campaign_id, release_id, controller_sha256,
                        checkpoint_path="/root/sofascore-runtime/all-men/history-controller.json",
                        env_path="/etc/data-platform/sofascore.env", checked_at=None,
                        max_age_seconds=86400):
    """Host insertion point: return one line without collecting any other metric."""
    roots = []
    try:
        with Path(env_path).open() as stream:
            for line in stream:
                if line.startswith("SOFASCORE_RELEASE_ROOT="):
                    roots.append(line.split("=", 1)[1].strip().strip("'\""))
    except (OSError, UnicodeError):
        pass
    root = roots[0] if len(roots) == 1 else ""
    result = build_report(checkpoint_path=checkpoint_path, campaign_id=campaign_id,
                          release_root=root, release_id=release_id,
                          controller_sha256=controller_sha256, checked_at=checked_at,
                          max_age_seconds=max_age_seconds)
    return [result["summary"]]
