"""Offline contract tests for the scheduled Transfermarkt morning adapter."""

import hashlib
import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from dags.scripts import transfermarkt_morning_report as morning


DAY = date(2026, 11, 18)
AS_OF = datetime(2026, 11, 19, 4, tzinfo=timezone.utc)
CYCLE = "scheduled__2026-11-18T06:00:00+00:00"
FINISH = "2026-11-18T20:59:00+00:00"
HASH = "a" * 64
TSV_VERSION = "b" * 64
SNAPSHOT = "example-snapshot"


def publish(root, *, cycle=CYCLE, finish=FINISH, published=None, **fields):
    portion = {
        "cycle_id": cycle,
        "finished_at": finish,
        "denominator_source": "registry",
        "universe_version": SNAPSHOT,
        "denominator_hash": HASH,
        "current_targets": [
            {"queue_rank": 0, "registry_snapshot_id": SNAPSHOT},
            {"queue_rank": 1, "registry_snapshot_id": "other-snapshot"},
        ],
        **fields,
    }
    key = hashlib.sha256(cycle.encode()).hexdigest()[:24]
    path = root / f"portion-{key}.json"
    path.write_text(json.dumps(portion))
    stamp = datetime.fromisoformat(published or finish).timestamp()
    os.utime(path, (stamp, stamp))
    return path


def test_default_clock_reads_once_at_moscow_rollover(monkeypatch):
    calls = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz):
            calls.append(tz)
            return datetime(2026, 11, 18, 21, tzinfo=tz)

    monkeypatch.setattr(morning, "datetime", Clock)
    assert morning.report_clock(None, None) == (
        DAY,
        datetime(2026, 11, 18, 21, tzinfo=timezone.utc),
    )
    assert calls == [timezone.utc]
    before = morning.aware_timestamp("2026-11-18T23:59:59+03:00")
    assert morning.report_clock(None, before)[0] == date(2026, 11, 17)
    with pytest.raises(ValueError):
        morning.aware_timestamp("2026-11-18T21:00:00")


def test_embedded_finish_selects_across_midnight_and_as_of(tmp_path):
    old = "scheduled__2026-11-18T05:00:00Z"
    publish(
        tmp_path,
        cycle=old,
        finish="2026-11-18T19:00:00Z",
        published="2026-11-18T19:30:00Z",
        denominator_hash="c" * 64,
    )
    publish(tmp_path, published="2026-11-18T21:05:00Z")
    publish(
        tmp_path,
        cycle="scheduled__2026-11-18T20:00:00Z",
        finish="2026-11-19T00:00:00+03:00",
    )
    publish(
        tmp_path, cycle="manual__2026-11-18T20:00:00Z", finish="2026-11-18T20:59:59Z"
    )
    for cutoff, expected in [
        (AS_OF, CYCLE),
        (morning.aware_timestamp(FINISH), old),
        (morning.aware_timestamp("2026-11-18T20:58:59Z"), old),
    ]:
        assert morning.latest_pin(tmp_path, DAY, cutoff).cycle_id == expected
    with pytest.raises(morning.Unavailable):
        morning.latest_pin(
            tmp_path, DAY, morning.aware_timestamp("2026-11-18T18:59:59Z")
        )


def test_valid_ties_have_deterministic_winner(tmp_path):
    publish(tmp_path)
    later_cycle = "scheduled__2026-11-18T07:00:00+00:00"
    publish(tmp_path, cycle=later_cycle, denominator_hash="c" * 64)
    pin = morning.latest_pin(tmp_path, DAY, AS_OF)
    assert (pin.cycle_id, pin.denominator_hash) == (later_cycle, "c" * 64)


@pytest.mark.parametrize(
    "fields",
    [
        {"denominator_source": None},
        {"denominator_hash": "unverified"},
        {"universe_version": "other-snapshot"},
        {"denominator_source": "tsv", "universe_version": "not-a-file-digest"},
        {"current_targets": []},
        {"current_targets": [{"queue_rank": 0}]},
        {"current_targets": [{"queue_rank": 1, "registry_snapshot_id": SNAPSHOT}]},
        {
            "current_targets": [
                {"queue_rank": 0, "registry_snapshot_id": name}
                for name in (SNAPSHOT, "second")
            ]
        },
        {"cycle_id": "synthetic"},
    ],
)
def test_bad_newest_provenance_never_uses_older_receipt(tmp_path, monkeypatch, fields):
    publish(
        tmp_path,
        cycle="scheduled__2026-11-18T05:00:00Z",
        finish="2026-11-18T07:00:00Z",
        published="2026-11-19T01:00:00Z",
    )
    publish(tmp_path, **fields)
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: pytest.fail("unbound query")
    )
    output = morning.morning_report(DAY, AS_OF, tmp_path, tmp_path)
    assert output["report"] is None and output["status"] == "not_measured"
    assert (
        "не измерено" in output["summary"] and "Bronze DQ unknown" in output["summary"]
    )


def test_unreadable_candidate_and_irrelevant_old_file(tmp_path):
    publish(tmp_path)
    broken = tmp_path / "portion-broken.json"
    broken.write_text("{")
    past = datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp()
    os.utime(broken, (past, past))
    assert morning.latest_pin(tmp_path, DAY, AS_OF).cycle_id == CYCLE
    os.utime(broken, (AS_OF.timestamp(), AS_OF.timestamp()))
    assert morning.morning_report(DAY, AS_OF, tmp_path, tmp_path)["report"] is None


def test_old_replay_ignores_receipts_published_after_cutoff(tmp_path):
    publish(tmp_path)
    publish(
        tmp_path, cycle="scheduled__2026-11-19T06:00:00Z", finish="2026-11-19T07:00:00Z"
    )
    publish(
        tmp_path,
        cycle="scheduled__2026-11-18T20:00:00Z",
        finish="2026-11-18T20:59:30Z",
        published="2026-11-19T04:00:01Z",
    )
    assert morning.latest_pin(tmp_path, DAY, AS_OF).cycle_id == CYCLE


@pytest.mark.parametrize("source", ["registry", "tsv"])
def test_main_delegates_exact_measurement_and_preserves_output(
    tmp_path, monkeypatch, capsys, source
):
    version = TSV_VERSION if source == "tsv" else SNAPSHOT
    publish(tmp_path, denominator_source=source, universe_version=version)
    dq = tmp_path / "dq"
    dq.mkdir()
    receipt = {
        "version": morning.daily_dq.VERSION,
        "day_msk": str(DAY),
        "completed": False,
        "status": "RED",
        "cutoff_at": FINISH,
        "errors": ["tm_test[a]", "tm_test[b]"],
    }
    (dq / f"{DAY}.json").write_text(json.dumps(receipt))
    tsv = tmp_path / "evidence.tsv"
    sample_path = tmp_path / "samples.json"
    sample = {"checked": 3} if source == "tsv" else None
    args = [
        "--day",
        str(DAY),
        "--as-of",
        "2026-11-19T07:00:00+03:00",
        "--current-root",
        str(tmp_path),
        "--dq-state-root",
        str(dq),
    ]
    expected = [
        sys.executable,
        "-B",
        str(Path(morning.__file__).with_name("transfermarkt_daily_report.py")),
        "--execute",
        "--day",
        str(DAY),
        "--as-of",
        AS_OF.isoformat(),
        "--denominator-source",
        source,
        "--registry-snapshot-id",
        SNAPSHOT,
        "--dq-json",
        "/dev/stdin",
    ]
    if source == "tsv":
        evidence = ["--denominator", str(tsv), "--samples-json", str(sample_path)]
        args.extend(evidence)
        expected.extend(evidence)
    report = {
        "measurement_version": "test-version",
        "report_day": str(DAY),
        "day_start": "2026-11-17T21:00:00Z",
        "day_end": "2026-11-18T21:00:00Z",
        "as_of": AS_OF.isoformat(),
        "denominator_version": version,
        "denominator_hash": HASH,
        "within_24h_pct": None,
    }
    first = (
        f"Transfermarkt {DAY} МСК; изменения ≤24ч от обнаружения: незрелое окно; "
        f"контроль сайта: {'3' if sample else 'нет выборки'}; Bronze DQ RED (неполная проверка)"
    )
    warning = "ТРЕВОГА: standing_registry_policy.json: истекла"
    payload = {
        "report": report,
        "site_sample": sample,
        "summary": first + "\n" + warning,
    }
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert json.loads(kwargs["input"]) == receipt
        assert kwargs.get("shell", False) is False
        assert (
            kwargs["stdout"] == subprocess.PIPE
            and kwargs["stderr"] == subprocess.DEVNULL
        )
        assert 0 < kwargs["timeout"] <= 145
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload))

    monkeypatch.setattr(subprocess, "run", run)
    assert morning.main(args) == 0
    assert calls == [expected]
    output = json.loads(capsys.readouterr().out)
    assert output["summary"].splitlines() == [
        f"{first}, ошибок 2 (test); as_of={AS_OF.isoformat()}; знаменатель {source}; "
        f"версия={version}; хеш={HASH}; снимок={SNAPSHOT}",
        warning,
    ]
    assert output["report"] == report and output["site_sample"] == sample
    assert output["provenance"] == {
        "day": str(DAY),
        "as_of": AS_OF.isoformat(),
        "cycle_id": CYCLE,
        "finished_at": FINISH,
        "denominator_source": source,
        "universe_version": version,
        "denominator_hash": HASH,
        "registry_snapshot_id": SNAPSHOT,
    }


@pytest.mark.parametrize(
    "failure", ["hash", "version", "no report", "nonzero", "timeout", "json"]
)
def test_failure_discards_numbers_and_diagnostics_preserves_partial_red(
    tmp_path, monkeypatch, failure
):
    publish(tmp_path)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if failure == "timeout":
            raise subprocess.TimeoutExpired("/secret/path", 140)
        report = {
            "report_day": str(DAY),
            "denominator_hash": HASH,
            "denominator_version": SNAPSHOT,
        }
        if failure == "hash":
            report["denominator_hash"] = "d" * 64
        if failure == "version":
            report["denominator_version"] = "other-snapshot"
        raw = json.dumps(
            {
                "report": None if failure == "no report" else report,
                "site_sample": None,
                "summary": "100% /secret/path",
            }
        )
        return subprocess.CompletedProcess(
            argv,
            int(failure == "nonzero"),
            "/secret/path" if failure == "json" else raw,
        )

    def read(*, state_root, now):
        assert state_root == tmp_path and now.astimezone(morning.MOSCOW).date() == DAY
        return {
            "day_msk": str(DAY),
            "version": morning.daily_dq.VERSION,
            "status": "RED",
            "completed": False,
            "cutoff_at": FINISH,
        }

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(morning.daily_dq, "read_daily_result", read)
    output = morning.morning_report(DAY, AS_OF, tmp_path, tmp_path)
    assert len(calls) == 1 and output["report"] is None
    assert "Bronze DQ RED (неполная проверка)" in output["summary"]
    assert "не измерено" in output["summary"]
    assert all(text not in output["summary"] for text in ["%", "/secret", "\n"])


@pytest.mark.parametrize(
    ("overrides", "label"),
    [
        ({}, "RED"),
        ({"day_msk": "2026-11-19"}, "unknown"),
        ({"version": "old"}, "unknown"),
        ({"stale": True}, "unknown"),
        ({"cutoff_at": "2026-11-19T04:00:01Z"}, "unknown"),
        ({"cutoff_at": "not a timestamp"}, "unknown"),
        ({"status": "GREEN", "completed": False}, "unknown"),
    ],
)
def test_only_eligible_daily_dq_is_visible_without_a_portion(
    tmp_path, monkeypatch, overrides, label
):
    dq = {
        "day_msk": str(DAY),
        "version": morning.daily_dq.VERSION,
        "status": "RED",
        "completed": True,
        "cutoff_at": AS_OF.isoformat(),
        **overrides,
    }
    monkeypatch.setattr(morning.daily_dq, "read_daily_result", lambda **kwargs: dq)
    output = morning.morning_report(DAY, AS_OF, tmp_path, tmp_path)
    assert output["report"] is None and output["summary"].endswith("Bronze DQ " + label)


def test_script_entrypoint_without_evidence_is_read_only(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            morning.__file__,
            "--current-root",
            str(tmp_path),
            "--dq-state-root",
            str(tmp_path),
            "--day",
            str(DAY),
            "--as-of",
            AS_OF.isoformat(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 1
    output = json.loads(result.stdout)
    assert output["report"] is None and "Bronze DQ unknown" in output["summary"]
    assert list(tmp_path.iterdir()) == []


def test_refusal_rate_is_added_only_when_the_gateway_counters_are_named(tmp_path, monkeypatch, capsys):
    import json

    from dags.scripts import transfermarkt_morning_report as report

    monkeypatch.setattr(report, "morning_report", lambda *args: {
        "summary": "Transfermarkt 2026-11-28 МСК: сводка", "report": {"report_day": "2026-11-28"},
        "provenance": {}})
    common = ["--day", "2026-11-28", "--as-of", "2026-11-28T12:00:00+03:00"]
    assert report.main(common) == 0
    assert "source_refusals" not in json.loads(capsys.readouterr().out)
    state = tmp_path / "permits.json.refusals"
    empty = {"day": "2026-11-28", "requests": None, "refusals": None, "per_100": None}
    assert report.main([*common, "--refusals-state", str(state)]) == 0
    # No counters, or damaged ones, leave the rate unknown instead of inventing a zero.
    assert json.loads(capsys.readouterr().out)["source_refusals"] == empty
    state.write_text('{"schema_version": 1, "days": {')
    assert report.main([*common, "--refusals-state", str(state)]) == 0
    assert json.loads(capsys.readouterr().out)["source_refusals"] == empty
    state.write_text(json.dumps({
        "schema_version": 1,
        "days": {"2026-11-27": {"requests": 4, "refusals": 4},
                 "2026-11-28": {"requests": 8, "refusals": 2}}}))
    assert report.main([*common, "--refusals-state", str(state)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["source_refusals"] == {
        "day": "2026-11-28", "requests": 8, "refusals": 2, "per_100": 25.0}
    assert output["summary"] == "Transfermarkt 2026-11-28 МСК: сводка"   # the rest is untouched
