"""Contracts for the bounded, non-FBref Decodo sticky-session canary."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import scripts.research.bench_fbref_decodo_session as canary


@pytest.mark.unit
def test_probe_schedule_includes_local_lease_and_provider_boundary():
    assert canary.PROBE_OFFSETS_SECONDS == (
        0,
        600,
        1800,
        3600,
        5400,
        6900,
        7200,
    )
    assert canary.PROVIDER_STICKY_MINUTES == 120
    assert canary.LOCAL_LEASE_MINUTES == 115


@pytest.mark.unit
def test_exit_identity_is_hashed_and_raw_ip_never_enters_report():
    observed_ip = "203.0.113.10"
    observations = tuple(
        canary.ProbeObservation(
            offset_seconds=offset,
            elapsed_seconds=float(offset),
            observed_at="2026-08-08T00:00:00+00:00",
            exit_hash=canary.hash_exit_identity(observed_ip),
        )
        for offset in canary.PROBE_OFFSETS_SECONDS
    )

    report = canary.build_probe_report(observations)
    rendered = json.dumps(report, sort_keys=True)

    assert observed_ip not in rendered
    assert (
        report["observations"][0]["exit_hash"]
        == hashlib.sha256(observed_ip.encode("ascii")).hexdigest()
    )
    assert report["sticky_passed"] is True
    assert report["recommended_provider_sticky_minutes"] == 120
    assert report["recommended_local_lease_minutes"] == 115


@pytest.mark.unit
def test_report_fails_closed_when_probe_is_more_than_60_seconds_late():
    observations = []
    exit_hash = canary.hash_exit_identity("203.0.113.10")
    for offset in canary.PROBE_OFFSETS_SECONDS:
        observations.append(
            canary.ProbeObservation(
                offset_seconds=offset,
                elapsed_seconds=float(offset + (61 if offset == 1800 else 0)),
                observed_at="2026-08-08T00:00:00+00:00",
                exit_hash=exit_hash,
            )
        )

    report = canary.build_probe_report(tuple(observations))

    assert report["schedule_within_tolerance"] is False
    assert report["sticky_passed"] is False
    assert report["recommended_provider_sticky_minutes"] is None
    assert "probe_schedule_late" in report["failures"]


@pytest.mark.unit
def test_provider_boundary_rotation_passes_when_local_lease_stayed_stable():
    stable_hash = canary.hash_exit_identity("203.0.113.10")
    boundary_hash = canary.hash_exit_identity("203.0.113.11")
    observations = tuple(
        canary.ProbeObservation(
            offset_seconds=offset,
            elapsed_seconds=float(offset),
            observed_at="2026-08-08T00:00:00+00:00",
            exit_hash=(boundary_hash if offset == 7200 else stable_hash),
        )
        for offset in canary.PROBE_OFFSETS_SECONDS
    )

    report = canary.build_probe_report(observations)

    assert report["local_lease_stable"] is True
    assert report["provider_boundary_changed"] is True
    assert report["sticky_passed"] is True
    assert report["failures"] == []
    assert report["recommended_provider_sticky_minutes"] == 120
    assert report["recommended_local_lease_minutes"] == 115


@pytest.mark.unit
def test_rotation_by_local_lease_boundary_fails_closed():
    stable_hash = canary.hash_exit_identity("203.0.113.10")
    early_hash = canary.hash_exit_identity("203.0.113.11")
    observations = tuple(
        canary.ProbeObservation(
            offset_seconds=offset,
            elapsed_seconds=float(offset),
            observed_at="2026-08-08T00:00:00+00:00",
            exit_hash=(early_hash if offset >= 6900 else stable_hash),
        )
        for offset in canary.PROBE_OFFSETS_SECONDS
    )

    report = canary.build_probe_report(observations)

    assert report["local_lease_stable"] is False
    assert report["provider_boundary_changed"] is False
    assert report["sticky_passed"] is False
    assert "exit_changed_before_local_lease_end" in report["failures"]


@pytest.mark.unit
def test_provider_counter_lag_is_explicit_and_not_misreported_as_zero_usage():
    exit_hash = canary.hash_exit_identity("2001:db8::1")
    observations = tuple(
        canary.ProbeObservation(
            offset_seconds=offset,
            elapsed_seconds=float(offset),
            observed_at="2026-08-08T00:00:00+00:00",
            exit_hash=exit_hash,
        )
        for offset in canary.PROBE_OFFSETS_SECONDS
    )

    report = canary.build_probe_report(
        observations,
        provider_bytes_before=1000,
        provider_bytes_after=1000,
    )

    assert report["provider_counter"]["status"] == "pending"
    assert report["provider_counter"]["delta_bytes"] is None
    assert report["sticky_passed"] is True


@pytest.mark.unit
def test_current_decodo_ip_check_response_extracts_nested_proxy_ip():
    assert (
        canary.extract_ip_check_identity(
            {
                "browser": {"name": "Firefox"},
                "proxy": {"ip": "203.0.113.24"},
                "country": {"code": "US"},
            }
        )
        == "203.0.113.24"
    )


@pytest.mark.unit
def test_proxy_file_requires_exactly_one_entry_and_never_echoes_credentials(
    tmp_path,
):
    proxy_file = tmp_path / "decodo.txt"
    secret = "do-not-print-this"
    proxy_file.write_text(
        f"http://session-user:{secret}@gate.decodo.com:7000\n",
        encoding="utf-8",
    )
    proxy_file.chmod(0o640)

    proxy_url = canary.read_single_proxy_url(proxy_file)

    assert secret in proxy_url
    proxy_file.write_text(
        "host-one:7000:user:pass\nhost-two:7000:user:pass\n",
        encoding="utf-8",
    )
    with pytest.raises(canary.CanaryConfigurationError) as raised:
        canary.read_single_proxy_url(proxy_file)
    assert "pass" not in str(raised.value)
    assert "host-one" not in str(raised.value)


@pytest.mark.unit
def test_canary_runs_only_scheduled_ip_check_probes_without_leaking_proxy(
    tmp_path,
):
    proxy_file = tmp_path / "decodo.txt"
    secret = "runtime-secret"
    proxy_file.write_text(
        f"gate.decodo.com:7000:session-user:{secret}\n",
        encoding="utf-8",
    )
    proxy_file.chmod(0o640)
    clock_value = [1000.0]
    calls = []

    def clock():
        return clock_value[0]

    def sleep(seconds):
        clock_value[0] += seconds

    def probe(*, proxy_url, check_url):
        calls.append((proxy_url, check_url))
        return "203.0.113.20"

    report = canary.run_canary(
        proxy_file=proxy_file,
        clock=clock,
        sleep=sleep,
        probe=probe,
        now=lambda: datetime(2026, 8, 8, tzinfo=timezone.utc),
    )
    rendered = json.dumps(report, sort_keys=True)

    assert len(calls) == len(canary.PROBE_OFFSETS_SECONDS)
    assert {url for _, url in calls} == {canary.DECODO_IP_CHECK_URL}
    assert all("fbref" not in url.casefold() for _, url in calls)
    assert secret not in rendered
    assert "203.0.113.20" not in rendered
    assert report["sticky_passed"] is True


@pytest.mark.unit
def test_canary_checkpoints_completed_probes_and_redacted_failure_offset(tmp_path):
    proxy_file = tmp_path / "decodo.txt"
    proxy_file.write_text(
        "gate.decodo.com:7000:session-user:runtime-secret\n",
        encoding="utf-8",
    )
    proxy_file.chmod(0o600)
    clock_value = [1000.0]
    probe_calls = [0]
    checkpoints = []

    def clock():
        return clock_value[0]

    def sleep(seconds):
        clock_value[0] += seconds

    def probe(**_kwargs):
        probe_calls[0] += 1
        if probe_calls[0] == 3:
            raise canary.CanaryProbeError("redacted")
        return "203.0.113.20"

    def checkpoint(observations, failed_offset_seconds):
        checkpoints.append(
            (
                tuple(item.offset_seconds for item in observations),
                failed_offset_seconds,
            )
        )

    with pytest.raises(canary.CanaryProbeError):
        canary.run_canary(
            proxy_file=proxy_file,
            clock=clock,
            sleep=sleep,
            probe=probe,
            checkpoint=checkpoint,
        )

    assert checkpoints == [
        ((0,), None),
        ((0, 600), None),
        ((0, 600), 1800),
    ]


@pytest.mark.unit
def test_probe_checkpoint_contains_hashes_only_and_is_owner_readable(tmp_path):
    output = tmp_path / "checkpoint.json"
    raw_ip = "203.0.113.20"
    observation = canary.ProbeObservation(
        offset_seconds=0,
        elapsed_seconds=1.0,
        observed_at="2026-08-08T00:00:00+00:00",
        exit_hash=canary.hash_exit_identity(raw_ip),
    )

    canary.write_probe_checkpoint(
        output,
        (observation,),
        failed_offset_seconds=600,
    )

    rendered = output.read_text(encoding="utf-8")
    payload = json.loads(rendered)
    assert raw_ip not in rendered
    assert payload["status"] == "failed"
    assert payload["completed_offsets_seconds"] == [0]
    assert payload["failed_offset_seconds"] == 600
    assert output.stat().st_mode & 0o777 == 0o600


@pytest.mark.unit
def test_probe_failure_is_redacted(tmp_path):
    proxy_file = tmp_path / "decodo.txt"
    secret = "runtime-secret"
    proxy_file.write_text(
        f"http://user:{secret}@gate.decodo.com:7000\n", encoding="utf-8"
    )
    proxy_file.chmod(0o640)

    def fail(**_kwargs):
        raise RuntimeError(f"failed via {secret}")

    with pytest.raises(canary.CanaryProbeError) as raised:
        canary.run_canary(
            proxy_file=proxy_file,
            clock=lambda: 0.0,
            sleep=lambda _seconds: None,
            probe=fail,
        )

    assert secret not in str(raised.value)


@pytest.mark.unit
def test_rollout_runbook_has_staged_go_and_rollback_contract():
    path = (
        Path(__file__).resolve().parents[3] / "docs/operations/fbref-decodo-rollout.md"
    )
    text = path.read_text(encoding="utf-8")
    lowered = text.casefold()
    normalized = " ".join(lowered.split())

    assert "rotate" in lowered and "screenshot" in lowered
    assert "dedicated decodo sub-user" in lowered
    assert all(f"{minutes} minute" in normalized for minutes in (60, 90, 120))
    assert "120-minute provider" in normalized
    assert "115-minute local" in normalized
    assert "0640" in text
    assert "0/10/30/60/90/115/120" in text
    assert "fbref_batch_persist=0" in lowered
    assert "fbref_batch_persist=1" in lowered
    assert "fbref_persistent_http_session=0" in lowered
    assert "fbref_persistent_http_session=1" in lowered
    assert "persistent session is default-off" in normalized
    assert "persistent-session" in lowered and "observe" in lowered
    assert "except all" in lowered
    assert "12" in text and "sentinel" in lowered and "snapshot" in lowered
    assert "dag_replay_fbref_bronze" in lowered
    assert "persistence_mode=sequential" in lowered
    assert "persistence_mode=batch" in lowered
    assert "atomically stores" in lowered
    assert "--sequential-metrics" not in lowered
    assert "--batch-metrics" not in lowered
    assert "--sequential-seconds" not in lowered
    assert "match-key digest" in lowered
    assert "latest page manifest" in lowered
    assert "accepted run set" in lowered
    assert "not an isolated replay" in normalized
    assert "4×" in text and "20 seconds" in lowered
    assert "1,500" in text
    assert "100 requests / 50 mib" in lowered
    assert "4096 requests / 2048 mib" in lowered
    assert "bytes per durable match" in lowered
    assert "enable daily" in lowered and "enable drain" in lowered
    assert "previous proxy file" in lowered and "previous image digest" in lowered
    assert "gate.decodo.com" not in lowered
    assert "http://" not in lowered and "https://" not in lowered
