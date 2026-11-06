from __future__ import annotations

import importlib.util
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "dags" / "scripts" / "run_transfermarkt_discovery.py"


class _Stub:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("stubbed dependency is not used in these tests")


def _install_dependency_stubs() -> None:
    """Stub heavy packages that do not exist in this trimmed worktree.

    Only the names imported at module load time are provided; none of the
    atomic-JSON behavior under test touches them.
    """
    import types
    import unittest.mock as mock

    def mod(name: str, **attrs) -> types.ModuleType:
        module = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(module, attr, value)
        sys.modules.setdefault(name, module)
        return module

    if "dags.utils" not in sys.modules:
        mod("dags")
        mod(
            "dags.utils",
            transfermarkt_registry_publish=mod(
                "dags.utils.transfermarkt_registry_publish",
                REGISTRY_STATE_TABLE="state",
                COMPETITIONS_TABLE="competitions",
                EDITIONS_TABLE="editions",
            ),
        )
        mod(
            "dags.utils.transfermarkt_approval",
            ApprovalJournal=_Stub,
            ApprovalPacket=_Stub,
            ApprovalStateError=RuntimeError,
            ApprovalValidationError=ValueError,
            StandingPolicy=_Stub,
            StandingPolicyScaling=_Stub,
            load_standing_policy=mock.Mock(),
        )
        mod(
            "dags.utils.transfermarkt_discovery_continuation",
            DiscoveryContinuation=_Stub,
            DiscoveryPortionComplete=type("DiscoveryPortionComplete", (Exception,), {}),
            identity_for=mock.Mock(),
            read_job=mock.Mock(),
            WORK_SECONDS=1,
            ensure_manual_delivery_admission=mock.Mock(),
            MANUAL_DAG_ID="manual",
        )
    if "scrapers" not in sys.modules:
        mod("scrapers")
        mod("scrapers.utils")
        mod("scrapers.utils.rate_limiter", RateLimiter=_Stub)
        mod("scrapers.transfermarkt")
        mod(
            "scrapers.transfermarkt.client",
            ProxyFilterLeaseProvider=_Stub,
            CurrentPortionDeadlineExceeded=type(
                "CurrentPortionDeadlineExceeded", (Exception,), {}
            ),
            TransfermarktHttpClient=_Stub,
            redact_sensitive=lambda value: value,
        )
        mod(
            "scrapers.transfermarkt.discovery",
            EXTRA_COMPETITIONS=(),
            MODES=("full", "daily", "auto"),
            PreviousRegistry=_Stub,
            discover_competition_registry=mock.Mock(),
            load_countries=mock.Mock(),
        )
        mod(
            "scrapers.transfermarkt.models",
            FetchOutcome=_Stub,
            FetchStatus=_Stub,
            HARD_PROVIDER_BYTE_BUDGET=0,
            SOFT_PROVIDER_BYTE_STOP=0,
            SharedTrafficLedger=_Stub,
            stable_payload_hash=mock.Mock(),
        )
        mod(
            "scrapers.transfermarkt.registry",
            ClassificationStatus=_Stub,
            RegistryPage=_Stub,
            RegistrySnapshot=_Stub,
            deterministic_scope_id=mock.Mock(),
            reconcile_registry_pages=mock.Mock(),
        )


def _load():
    _install_dependency_stubs()
    name = "run_transfermarkt_discovery_atomic_json_test_module"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def module():
    return _load()


@pytest.fixture()
def state_path(tmp_path: Path) -> Path:
    return tmp_path / "state.json"


def _tmp_files(directory: Path) -> list[Path]:
    return [p for p in directory.iterdir() if p.name.endswith(".tmp")]


def test_roundtrip_reopen_update_delete(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["alpha"] = {"n": 1, "tags": ["a", "b"]}
    mapping["beta"] = "value"
    reopened = module.AtomicJsonMapping(state_path)
    assert reopened["alpha"] == {"n": 1, "tags": ["a", "b"]}
    reopened["alpha"] = {"n": 2}
    del reopened["beta"]
    final = module.AtomicJsonMapping(state_path)
    assert dict(final) == {"alpha": {"n": 2}}
    assert len(final) == 1
    assert list(final) == ["alpha"]


def test_legacy_json_file_is_loaded(module, state_path):
    state_path.write_text(
        json.dumps({"z": 1, "a": [1, 2.5, None, True]}), encoding="utf-8"
    )
    mapping = module.AtomicJsonMapping(state_path)
    assert mapping["z"] == 1
    assert mapping["a"] == [1, 2.5, None, True]
    mapping["new"] = "entry"
    reopened = module.AtomicJsonMapping(state_path)
    assert reopened["a"] == [1, 2.5, None, True]


def test_unicode_html_control_chars_sorted_and_byte_identity(module, state_path):
    payload = {
        "zebra": "ümläut <b>html</b> & 'quotes' \"dq\"",
        "alpha": "line\nbreak\ttab\x01control",
        "mid": {"nested": ["€", "日本語", {"deep": "v"}]},
    }
    mapping = module.AtomicJsonMapping(state_path)
    for key, value in payload.items():
        mapping[key] = value
    raw = state_path.read_bytes()
    expected = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    # Byte identity for ordinary string/int checkpoint values.
    assert raw == expected
    assert list(json.loads(raw.decode("utf-8"))) == sorted(payload)
    reopened = module.AtomicJsonMapping(state_path)
    assert dict(reopened) == payload


def test_finite_floats_semantic_roundtrip(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["f"] = [0.1, 1e300, -2.5, 3.0]
    loaded = json.loads(state_path.read_text(encoding="utf-8"))
    assert loaded["f"] == [0.1, 1e300, -2.5, 3.0]
    assert module.AtomicJsonMapping(state_path)["f"] == [0.1, 1e300, -2.5, 3.0]


def test_nonfinite_floats_fall_back_to_stdlib(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["nan"] = float("nan")
    mapping["inf"] = float("inf")
    raw = state_path.read_text(encoding="utf-8")
    assert "NaN" in raw and "Infinity" in raw
    loaded = json.loads(raw)
    assert math.isnan(loaded["nan"]) and loaded["inf"] == float("inf")


def test_huge_ints_fall_back_to_stdlib(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["big"] = 2**70
    assert json.loads(state_path.read_text(encoding="utf-8"))["big"] == 2**70


def test_unsupported_objects_still_rejected(module, state_path):
    @dataclass
    class Payload:
        x: int

    for bad in (datetime(2026, 1, 1, tzinfo=timezone.utc), Payload(1), {1, 2}):
        mapping = module.AtomicJsonMapping(state_path)
        with pytest.raises(TypeError):
            mapping["bad"] = bad
        assert not state_path.exists() or "bad" not in json.loads(
            state_path.read_text(encoding="utf-8")
        )
        state_path.unlink(missing_ok=True)


def test_nonstring_root_keys_keep_previous_behavior(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    # Stdlib coerces int keys; must not regress via the fast path.
    mapping[1] = "one"  # type: ignore[index]
    assert json.loads(state_path.read_text(encoding="utf-8")) == {"1": "one"}
    # Mixed keys still fail the same way stdlib sorting fails.
    with pytest.raises(TypeError):
        mapping["s"] = "x"


def test_failed_persist_keeps_old_file_and_no_tmp(module, state_path, monkeypatch):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["good"] = "stable"
    before = state_path.read_bytes()

    original_replace = os.replace

    def failing_replace(src, dst):
        if Path(src).suffix == ".tmp" and Path(dst) == state_path:
            raise OSError("simulated replace failure")
        return original_replace(src, dst)

    monkeypatch.setattr(module.os, "replace", failing_replace)
    with pytest.raises(OSError):
        mapping["bad"] = "x"
    assert state_path.read_bytes() == before
    assert _tmp_files(state_path.parent) == []


def test_serialization_failure_keeps_old_file_and_no_tmp(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["good"] = "stable"
    before = state_path.read_bytes()
    with pytest.raises(TypeError):
        mapping["bad"] = object()
    assert state_path.read_bytes() == before
    assert _tmp_files(state_path.parent) == []


def test_fsync_before_replace(module, state_path, monkeypatch):
    events: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def fsync_spy(fd):
        events.append("fsync")
        return real_fsync(fd)

    def replace_spy(src, dst):
        events.append("replace")
        return real_replace(src, dst)

    monkeypatch.setattr(module.os, "fsync", fsync_spy)
    monkeypatch.setattr(module.os, "replace", replace_spy)
    mapping = module.AtomicJsonMapping(state_path)
    mapping["k"] = "v"
    assert events.index("fsync") < events.index("replace")


def test_no_whole_map_encoder_allocation(module, state_path, monkeypatch):
    calls: list[object] = []
    real_encode = module._encode_json_entry

    def spy(value):
        calls.append(value)
        return real_encode(value)

    monkeypatch.setattr(module, "_encode_json_entry", spy)
    mapping = module.AtomicJsonMapping(state_path)
    payload = {f"key{i}": {"v": i} for i in range(5)}
    for key, value in payload.items():
        mapping[key] = value
    calls.clear()
    mapping["extra"] = "x"
    data = mapping._data
    # Every encoder call handles a single key or value, never the whole map.
    assert calls
    assert all(call is not data and call != data for call in calls)
    assert len(calls) == 2 * len(data)


def test_nested_mutation_then_set_no_stale_cache(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    nested = {"items": [1, 2], "meta": {"a": "x"}}
    mapping["root"] = nested
    # Mutate the stored object in place, then persist via a later set.
    nested["items"].append(3)
    nested["meta"]["a"] = "y"
    mapping["other"] = True
    reopened = module.AtomicJsonMapping(state_path)
    assert reopened["root"] == {"items": [1, 2, 3], "meta": {"a": "y"}}
    assert reopened["other"] is True


def test_nested_reverse_order_keys_canonical_bytes(module, state_path):
    # Build nested dicts in reverse order at several depths; the persisted
    # bytes must be byte-identical to the canonical stdlib encoding.
    value = {
        "zeta": {"b": 2, "a": {"y": 1, "x": {"d": 4, "c": 3}}},
        "alpha": [{"b": 2, "a": 1}, {"z": 0, "m": {"n": 2, "l": 1}}],
        "mid": "plain",
    }
    mapping = module.AtomicJsonMapping(state_path)
    mapping["entry"] = value
    expected = json.dumps(
        {"entry": value},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert state_path.read_bytes() == expected
    assert module.AtomicJsonMapping(state_path)["entry"] == value


def test_deep_nesting_falls_back_and_preserves_semantics(module, state_path):
    deep = json.loads("[" * 300 + "1" + "]" * 300)
    mapping = module.AtomicJsonMapping(state_path)
    mapping["deep"] = deep
    mapping["flat"] = {"k": "v"}
    reopened = json.loads(state_path.read_text(encoding="utf-8"))
    assert reopened["deep"] == deep
    assert reopened["flat"] == {"k": "v"}
    assert dict(module.AtomicJsonMapping(state_path)) == reopened


def test_circular_value_rejected_and_original_preserved(
    module, state_path, monkeypatch
):
    state_path.write_text(json.dumps({"safe": 1}), encoding="utf-8")
    mapping = module.AtomicJsonMapping(state_path)
    circular: list = []
    circular.append(circular)
    with pytest.raises(ValueError, match="Circular"):
        mapping["bad"] = circular
    assert json.loads(state_path.read_text(encoding="utf-8")) == {"safe": 1}
    assert _tmp_files(state_path.parent) == []


def test_fsync_failure_preserves_old_file_and_no_temps(
    module, state_path, monkeypatch
):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["old"] = 1
    original_bytes = state_path.read_bytes()

    def boom(_fd):
        raise OSError("fsync failed")

    monkeypatch.setattr(module.os, "fsync", boom)
    with pytest.raises(OSError, match="fsync failed"):
        mapping["new"] = 2
    assert state_path.read_bytes() == original_bytes
    assert _tmp_files(state_path.parent) == []


def test_persisted_file_mode_is_0600(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["k"] = "v"
    mode = state_path.stat().st_mode & 0o777
    assert mode == 0o600


def test_surrogate_key_raises_unicode_error_and_preserves_file(module, state_path):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["good"] = "stable"
    before = state_path.read_bytes()
    with pytest.raises(UnicodeEncodeError):
        mapping["\ud800bad"] = "x"
    assert state_path.read_bytes() == before
    assert _tmp_files(state_path.parent) == []


def test_surrogate_nested_value_raises_unicode_error_and_preserves_file(
    module, state_path
):
    mapping = module.AtomicJsonMapping(state_path)
    mapping["good"] = "stable"
    before = state_path.read_bytes()
    with pytest.raises(UnicodeEncodeError):
        mapping["bad"] = {"nested": ["ok", "\udfff"]}
    assert state_path.read_bytes() == before
    assert _tmp_files(state_path.parent) == []


def test_fast_encoder_failure_falls_back_to_stdlib_bytes(
    module, state_path, monkeypatch
):
    real_dumps = module.orjson.dumps

    def failing_dumps(value, option=0):
        if isinstance(value, dict):
            raise module.orjson.JSONEncodeError("injected fast-encoder failure")
        return real_dumps(value, option=option)

    monkeypatch.setattr(module.orjson, "dumps", failing_dumps)
    mapping = module.AtomicJsonMapping(state_path)
    payload = {"b": [1, "x"], "a": {"z": 1, "y": 2}}
    mapping["entry"] = payload
    expected = json.dumps(
        {"entry": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert state_path.read_bytes() == expected
    assert module.AtomicJsonMapping(state_path)["entry"] == payload
    assert _tmp_files(state_path.parent) == []
