from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts.espn_watchdog_adapter_v1 import (
    ADAPTER_KIND,
    AdapterError,
    DEFAULT_PROBE_PYTHON,
    EXPECTED_HEALTH_URL,
    EXPECTED_METADB_CONTAINER,
    EXPECTED_PROBE_SHA256,
    EXPECTED_RESULT_CODES,
    EXPECTED_SCHEDULER_CONTAINER,
    RUNTIME_READ_METHODS,
    SchedulerRuntimeReaders,
    _TrinoReadClient,
    _adapter_sha256,
    collect_runtime_snapshot,
    main,
    observe,
    render_lines,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 8, 9, 14, 0, tzinfo=UTC)


def _runtime_observations() -> dict[str, object]:
    return {
        key: {"reader": key}
        for _method_name, key in RUNTIME_READ_METHODS
    }


def _runtime_envelope(observations=None, errors=None) -> dict[str, object]:
    return {
        "kind": "espn-watchdog-runtime-snapshot-v1",
        "schema_version": 1,
        "adapter_sha256": _adapter_sha256(),
        "observations": observations or _runtime_observations(),
        "errors": errors or {},
    }


def _probe_report(*, statuses=None) -> dict[str, object]:
    statuses = statuses or {code: "ok" for code in EXPECTED_RESULT_CODES}
    results = [
        {
            "code": code,
            "status": statuses[code],
            "severity": "hard",
            "summary": f"summary for {code}",
            "details": {"code": code},
        }
        for code in EXPECTED_RESULT_CODES
    ]
    counts = {
        status: sum(item["status"] == status for item in results)
        for status in ("ok", "fail", "unknown")
    }
    return {
        "kind": "espn-rollout-probe-v1",
        "schema_version": 1,
        "observed_at": OBSERVED_AT.isoformat(timespec="seconds"),
        "status": "ok" if counts == {"ok": 14, "fail": 0, "unknown": 0} else "fail",
        "counts": counts,
        "results": results,
    }


def _install_probe_fixture(root: Path) -> Path:
    probe = root / "scripts" / "espn_rollout_probe_v1.py"
    probe.parent.mkdir(parents=True, exist_ok=True)
    probe.write_bytes(Path("scripts/espn_rollout_probe_v1.py").read_bytes())
    return probe


class FakeHost:
    def __init__(
        self,
        probe_report=None,
        *,
        probe_returncode=0,
        probe_stdout: str | None = None,
        runtime_stdout: str | None = None,
    ):
        self.probe_report = probe_report or _probe_report()
        self.probe_returncode = probe_returncode
        self.probe_stdout = probe_stdout
        self.runtime_stdout = runtime_stdout
        self.commands: list[tuple[tuple[str, ...], str | None, float]] = []
        self.health_calls: list[tuple[str, float]] = []
        self.probe_snapshot = None

    def run_command(self, argv, *, input_text=None, timeout):
        command = tuple(argv)
        self.commands.append((command, input_text, timeout))
        if command[1:4] == (
            "inspect",
            "--type",
            "container",
        ):
            payload = [
                {
                    "Name": f"/{EXPECTED_METADB_CONTAINER}",
                    "State": {
                        "Status": "running",
                        "Health": {"Status": "healthy"},
                    },
                }
            ]
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "--collect-runtime" in command:
            return subprocess.CompletedProcess(
                command,
                0,
                self.runtime_stdout or json.dumps(_runtime_envelope()),
                "",
            )
        if "espn_rollout_probe_v1.py" in " ".join(command):
            self.probe_snapshot = json.loads(input_text)
            return subprocess.CompletedProcess(
                command,
                self.probe_returncode,
                self.probe_stdout or json.dumps(self.probe_report),
                "probe failed" if self.probe_returncode else "",
            )
        raise AssertionError(f"unexpected command: {command!r}")

    def read_health(self, url, *, timeout):
        self.health_calls.append((url, timeout))
        return {
            "url": url,
            "status_code": 200,
            "body": {
                "metadatabase": {"status": "healthy"},
                "scheduler": {"status": "healthy"},
            },
        }


@pytest.mark.unit
def test_host_adapter_uses_exact_boundaries_and_streams_snapshot(tmp_path):
    release = tmp_path / "immutable-release"
    probe = _install_probe_fixture(release)
    host = FakeHost()

    report = observe(
        release,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["kind"] == ADAPTER_KIND
    assert report["status"] == "ok"
    assert report["observer"] == "hourly"
    assert report["phase"] == "arm-window"
    assert host.health_calls == [(EXPECTED_HEALTH_URL, 10.0)]
    assert host.commands[0][0] == (
        "/usr/bin/docker",
        "inspect",
        "--type",
        "container",
        EXPECTED_METADB_CONTAINER,
    )
    assert host.commands[1][0] == (
        "/usr/bin/docker",
        "exec",
        EXPECTED_SCHEDULER_CONTAINER,
        "python",
        "-B",
        "/opt/airflow/scripts/espn_watchdog_adapter_v1.py",
        "--collect-runtime",
        "--observed-at",
        OBSERVED_AT.isoformat(timespec="seconds"),
    )
    assert host.commands[2][0] == (
        sys.executable,
        "-B",
        str(probe.resolve()),
        "--snapshot",
        "-",
        "--observed-at",
        OBSERVED_AT.isoformat(timespec="seconds"),
    )
    assert host.probe_snapshot["container"] == {
        "name": EXPECTED_METADB_CONTAINER,
        "status": "running",
        "health": "healthy",
    }
    assert host.probe_snapshot["ui_health"]["url"] == EXPECTED_HEALTH_URL
    assert set(host.probe_snapshot) == {
        "container",
        "ui_health",
        *(key for _method, key in RUNTIME_READ_METHODS),
    }


@pytest.mark.unit
def test_nonzero_versioned_probe_is_parsed_and_all_results_are_exposed(tmp_path):
    _install_probe_fixture(tmp_path)
    statuses = {code: "ok" for code in EXPECTED_RESULT_CODES}
    statuses[EXPECTED_RESULT_CODES[4]] = "fail"
    statuses[EXPECTED_RESULT_CODES[9]] = "unknown"
    host = FakeHost(_probe_report(statuses=statuses), probe_returncode=1)

    report = observe(
        tmp_path,
        observer="morning",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["status"] == "fail"
    assert [item["code"] for item in report["results"]] == list(
        EXPECTED_RESULT_CODES
    )
    assert [item["status"] for item in report["results"]].count("fail") == 1
    assert [item["status"] for item in report["results"]].count("unknown") == 1


@pytest.mark.unit
def test_morning_system_python_still_invokes_probe_with_reviewed_venv(tmp_path):
    _install_probe_fixture(tmp_path)
    host = FakeHost()

    observe(
        tmp_path,
        observer="morning",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
    )

    assert host.commands[-1][0][0] == DEFAULT_PROBE_PYTHON


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda report: {**report, "kind": "espn-rollout-probe-v2"},
        lambda report: {**report, "schema_version": 2},
        lambda report: {**report, "observed_at": "2026-08-09T13:00:00+00:00"},
        lambda report: {**report, "results": report["results"][:-1]},
        lambda report: {
            **report,
            "results": [report["results"][0], *report["results"]],
        },
        lambda report: {
            **report,
            "results": [*reversed(report["results"])],
        },
    ],
)
def test_invalid_probe_envelope_fails_closed_with_every_result(tmp_path, mutate):
    _install_probe_fixture(tmp_path)
    host = FakeHost(mutate(_probe_report()), probe_returncode=0)

    report = observe(
        tmp_path,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["status"] == "fail"
    assert [item["code"] for item in report["results"]] == list(
        EXPECTED_RESULT_CODES
    )
    assert {item["status"] for item in report["results"]} == {"unknown"}
    assert all("adapter_error_type" in item["details"] for item in report["results"])


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformed_stdout",
    [
        lambda raw: raw.replace(
            '{"counts":', '{"counts":{},"counts":', 1
        ),
        lambda raw: raw.replace(
            '"details": {', '"details": {"non_finite": NaN, ', 1
        ),
        lambda raw: raw.replace('"schema_version": 1', '"schema_version": true', 1),
        lambda raw: raw.replace('"schema_version": 1', '"schema_version": 1.0', 1),
        lambda raw: raw.replace('"ok": 14', '"ok": true', 1),
        lambda raw: raw.replace('"ok": 14', '"ok": 14.0', 1),
    ],
)
def test_noncanonical_probe_json_fails_closed_with_every_result(
    tmp_path, malformed_stdout
):
    _install_probe_fixture(tmp_path)
    raw = malformed_stdout(json.dumps(_probe_report(), sort_keys=True))
    host = FakeHost(probe_stdout=raw)

    report = observe(
        tmp_path,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["status"] == "fail"
    assert [item["code"] for item in report["results"]] == list(
        EXPECTED_RESULT_CODES
    )
    assert {item["status"] for item in report["results"]} == {"unknown"}
    assert json.dumps(report, allow_nan=False)


@pytest.mark.unit
def test_probe_bytes_are_pinned_before_invocation(tmp_path):
    probe = _install_probe_fixture(tmp_path)
    assert EXPECTED_PROBE_SHA256 == "040c79abf7f6757f5dbbe1541b53711d44f2ef74578d0bbbda99dbcce278ed64"
    probe.write_bytes(b"# post-review replacement\n")
    host = FakeHost()

    report = observe(
        tmp_path,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["status"] == "fail"
    assert {item["status"] for item in report["results"]} == {"unknown"}
    assert not any("espn_rollout_probe_v1.py" in " ".join(call[0]) for call in host.commands)


@pytest.mark.unit
def test_runtime_adapter_identity_drift_is_not_accepted(tmp_path):
    _install_probe_fixture(tmp_path)
    envelope = _runtime_envelope()
    envelope["adapter_sha256"] = "0" * 64
    host = FakeHost(runtime_stdout=json.dumps(envelope))

    report = observe(
        tmp_path,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=host.read_health,
        python_executable=sys.executable,
    )

    assert report["collection_errors"]["runtime_snapshot"] == "AdapterError"


@pytest.mark.unit
def test_failed_host_reader_does_not_skip_runtime_collection_or_probe(tmp_path):
    _install_probe_fixture(tmp_path)
    host = FakeHost()

    def failed_health(url, *, timeout):
        host.health_calls.append((url, timeout))
        raise TimeoutError("health endpoint did not answer")

    observe(
        tmp_path,
        observer="hourly",
        observed_at=OBSERVED_AT,
        run_command=host.run_command,
        read_health=failed_health,
        python_executable=sys.executable,
    )

    assert len(host.commands) == 3
    assert "ui_health" not in host.probe_snapshot
    assert "container" in host.probe_snapshot
    assert all(key in host.probe_snapshot for _method, key in RUNTIME_READ_METHODS)


@pytest.mark.unit
def test_runtime_snapshot_calls_every_reader_and_keeps_independent_results(capsys):
    calls = []

    class Readers:
        pass

    readers = Readers()
    failed_method = RUNTIME_READ_METHODS[3][0]
    for method_name, key in RUNTIME_READ_METHODS:
        def read(name=method_name, value=key):
            calls.append(name)
            print(f"reader-noise:{name}")
            if name == failed_method:
                raise LookupError("missing observation")
            return {"value": value}

        setattr(readers, method_name, read)

    envelope = collect_runtime_snapshot(readers)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "reader-noise" in captured.err
    assert calls == [method for method, _key in RUNTIME_READ_METHODS]
    assert failed_method.removeprefix("read_") not in envelope["observations"]
    assert envelope["errors"] == {
        failed_method.removeprefix("read_"): "LookupError"
    }
    assert len(envelope["observations"]) == len(RUNTIME_READ_METHODS) - 1


@pytest.mark.unit
def test_renderer_always_emits_one_line_per_independent_result():
    report = {
        "kind": ADAPTER_KIND,
        "schema_version": 1,
        "observer": "hourly",
        "phase": "arm-window",
        "observed_at": OBSERVED_AT.isoformat(timespec="seconds"),
        "status": "ok",
        "results": _probe_report()["results"],
    }

    lines = render_lines(report)

    assert len(lines) == len(EXPECTED_RESULT_CODES) + 1
    assert "hourly" in lines[0]
    assert "arm-window" in lines[0]
    for code, line in zip(EXPECTED_RESULT_CODES, lines[1:], strict=True):
        assert code in line


@pytest.mark.unit
def test_renderer_synthesizes_all_results_when_called_with_partial_report():
    lines = render_lines(
        {
            "observer": "morning",
            "phase": "rest",
            "status": "fail",
            "results": _probe_report()["results"][:-1],
        }
    )

    assert len(lines) == len(EXPECTED_RESULT_CODES) + 1
    assert all("unknown" in line for line in lines[1:])


@pytest.mark.unit
def test_cli_json_preserves_hourly_pause_window_timestamp(monkeypatch, capsys, tmp_path):
    expected = {
        "kind": ADAPTER_KIND,
        "schema_version": 1,
        "observer": "hourly",
        "phase": "arm-window",
        "observed_at": OBSERVED_AT.isoformat(timespec="seconds"),
        "status": "ok",
        "results": _probe_report()["results"],
    }
    seen = {}

    def fake_observe(release_root, **kwargs):
        seen["release_root"] = release_root
        seen.update(kwargs)
        return expected

    monkeypatch.setattr("scripts.espn_watchdog_adapter_v1.observe", fake_observe)

    assert main(
        [
            "--release-root",
            str(tmp_path),
            "--observer",
            "hourly",
            "--observed-at",
            OBSERVED_AT.isoformat(),
            "--format",
            "json",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out) == expected
    assert seen["release_root"] == Path(tmp_path)
    assert seen["observer"] == "hourly"
    assert seen["observed_at"] == OBSERVED_AT


@pytest.mark.unit
def test_runtime_source_contains_no_platform_mutation_or_file_writer():
    source = Path("scripts/espn_watchdog_adapter_v1.py").read_text(encoding="utf-8")
    forbidden_sql = (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "MERGE ",
        "CREATE ",
        "ALTER ",
        "DROP ",
        "TRUNCATE ",
        "CALL ",
    )

    assert all(token not in source for token in forbidden_sql)
    assert "shell=True" not in source
    assert "NamedTemporaryFile" not in source
    assert "mkstemp" not in source
    assert ".write_text(" not in source
    assert ".write_bytes(" not in source
    assert "tg-send" not in source
    assert '"restart"' not in source
    assert '"start"' not in source
    assert '"stop"' not in source
    assert '"kill"' not in source


@pytest.mark.unit
def test_dag_inventory_is_exact_dagbag_plus_readonly_pause_rows(monkeypatch):
    class Bag:
        def __init__(self, **kwargs):
            assert kwargs == {
                "dag_folder": "/opt/airflow/dags",
                "include_examples": False,
                "safe_mode": True,
                "read_dags_from_db": False,
            }
            self.import_errors = {}
            self.dags = {"dag_ingest_espn": object(), "dag_monitor_espn": object()}

    monkeypatch.setitem(sys.modules, "airflow.models", SimpleNamespace(DagBag=Bag))
    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)
    seen = {}

    def metadata_rows(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return [("dag_ingest_espn", True), ("dag_monitor_espn", False)]

    monkeypatch.setattr(readers, "_metadata_rows", metadata_rows)

    assert readers.read_dags() == {
        "dag_ingest_espn": True,
        "dag_monitor_espn": False,
    }
    assert seen["sql"].startswith("SELECT ")
    assert seen["params"] == (
        ["dag_ingest_espn", "dag_monitor_espn"],
    )


@pytest.mark.unit
def test_parent_child_reader_derives_only_the_exact_scheduled_child(monkeypatch):
    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)
    interval_start = OBSERVED_AT - timedelta(days=1)
    interval_end = OBSERVED_AT
    parent_run_id = f"scheduled__{interval_start.isoformat()}"
    calls = []

    def metadata_rows(sql, params=()):
        calls.append((sql, params))
        assert sql.startswith("SELECT ")
        if "run_type = 'scheduled'" in sql:
            return [
                (
                    "dag_trigger_espn_daily",
                    parent_run_id,
                    "scheduled",
                    interval_start,
                    interval_start,
                    interval_end,
                    "success",
                )
            ]
        assert params == (
            "dag_ingest_espn",
            f"espn_daily__dag_trigger_espn_daily__{parent_run_id}",
        )
        return [("running", None)]

    monkeypatch.setattr(readers, "_metadata_rows", metadata_rows)

    result = readers.read_parent_child()

    assert result["parent_run_type"] == "scheduled"
    assert result["child_run_id"] == (
        "espn_daily__dag_trigger_espn_daily__" + parent_run_id
    )
    assert result["child_state"] == "running"
    assert calls[0][1] == (
        "dag_trigger_espn_daily",
        interval_start,
        interval_start,
        interval_end,
    )
    assert len(calls) == 2


@pytest.mark.unit
def test_day_two_pre_parent_arm_ignores_prior_terminal_cycle(monkeypatch):
    observed_at = datetime(2026, 8, 10, 13, 55, tzinfo=UTC)
    expected_start = datetime(2026, 8, 9, 14, 0, tzinfo=UTC)
    expected_end = datetime(2026, 8, 10, 14, 0, tzinfo=UTC)
    readers = SchedulerRuntimeReaders(observed_at=observed_at)
    calls = []

    def metadata_rows(sql, params=()):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(readers, "_metadata_rows", metadata_rows)

    result = readers.read_parent_child()

    assert result["parent_created"] is False
    assert result["parent_run_id"] is None
    assert calls == [
        (
            calls[0][0],
            (
                "dag_trigger_espn_daily",
                expected_start,
                expected_start,
                expected_end,
            ),
        )
    ]
    assert "data_interval_end = %s" in calls[0][0]


@pytest.mark.unit
def test_parent_reader_rejects_stale_row_even_if_metadata_violates_filter(monkeypatch):
    observed_at = datetime(2026, 8, 10, 13, 55, tzinfo=UTC)
    stale_start = datetime(2026, 8, 8, 14, 0, tzinfo=UTC)
    stale_end = datetime(2026, 8, 9, 14, 0, tzinfo=UTC)
    readers = SchedulerRuntimeReaders(observed_at=observed_at)
    monkeypatch.setattr(
        readers,
        "_metadata_rows",
        lambda _sql, _params=(): [
            (
                "dag_trigger_espn_daily",
                f"scheduled__{stale_start.isoformat()}",
                "scheduled",
                stale_start,
                stale_start,
                stale_end,
                "success",
            )
        ],
    )

    with pytest.raises(AdapterError, match="current UTC daily cycle"):
        readers.read_parent_child()


@pytest.mark.unit
def test_active_lease_reader_enforces_readonly_session_before_select(monkeypatch):
    events = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql):
            events.append(("execute", sql))

        def fetchall(self):
            return [("19425:2026", "dag/run/1")]

    class Connection:
        def set_session(self, **kwargs):
            events.append(("set_session", kwargs))

        def cursor(self):
            return Cursor()

        def rollback(self):
            events.append(("rollback", None))

        def close(self):
            events.append(("close", None))

    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)
    monkeypatch.setattr(
        readers, "_store", lambda: SimpleNamespace(_connect=lambda: Connection())
    )

    assert readers.read_active_leases() == [
        {"scope_id": "19425:2026", "owner_id": "dag/run/1"}
    ]
    assert events[0] == (
        "set_session",
        {"readonly": True, "autocommit": False},
    )
    assert events[1][0] == "execute"
    assert events[-2:] == [("rollback", None), ("close", None)]


@pytest.mark.unit
def test_repository_constructor_has_query_only_client_and_no_writer_capability(
    monkeypatch,
):
    from scrapers.espn import layout, repository

    seen = {}
    sentinel = object()

    class FakeRepository:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(repository, "EspnBronzeRepository", FakeRepository)
    monkeypatch.setattr(layout, "require_layout_mode", lambda: "legacy14")
    monkeypatch.setattr(
        _TrinoReadClient,
        "from_env",
        classmethod(lambda cls: sentinel),
    )
    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)

    result = readers._repository()

    assert isinstance(result, FakeRepository)
    assert type(seen["writer"]) is object
    assert not hasattr(seen["writer"], "write_dataframe")
    assert seen == {
        "writer": seen["writer"],
        "query": sentinel,
        "layout_mode": "legacy14",
        "ensure_objects_on_write": False,
        "validate_catalog_layout_on_write": False,
    }


@pytest.mark.unit
def test_trino_boundary_rejects_non_query_before_cursor_execution():
    class Connection:
        def cursor(self):
            raise AssertionError("rejected statement must not open a cursor")

    client = _TrinoReadClient(Connection())

    with pytest.raises(RuntimeError, match="permits queries only"):
        client.execute_query("DE" + "LETE FROM iceberg.bronze.anything")


@pytest.mark.unit
def test_control_store_factory_makes_every_connection_readonly(monkeypatch):
    from scrapers.espn.operations import PostgresEspnControlStore

    events = []

    class Connection:
        def set_session(self, **kwargs):
            events.append(kwargs)

    monkeypatch.setenv(
        "ESPN_CONTROL_DATABASE_URL",
        "postgresql+psycopg2://observer:secret@control/espn",
    )
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        SimpleNamespace(
            connect=lambda dsn: (
                events.append(dsn),
                Connection(),
            )[1]
        ),
    )
    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)

    store = readers._store()
    connection = store._connect()

    assert isinstance(store, PostgresEspnControlStore)
    assert isinstance(connection, Connection)
    assert events == [
        "postgresql://observer:secret@control/espn",
        {"readonly": True, "autocommit": False},
    ]


@pytest.mark.unit
def test_scope_freshness_uses_existing_identity_bound_readonly_validator(monkeypatch):
    from dags.utils import espn_native_tasks as tasks

    head = SimpleNamespace(
        registry_signature="a" * 64,
    )
    evidence = object()
    registry = SimpleNamespace(signature=lambda: "a" * 64)
    state = {"male_registry_ref": {"uri": "s3://registry", "sha256": "b" * 64}}
    store = SimpleNamespace(
        read_scope_heads=lambda scopes: {"19425:2026": head},
        read_latest_run_evidence_by_scope=lambda scopes, dag_id: {
            "19425:2026": evidence
        },
    )
    generation = SimpleNamespace(parser_version="parser-v", runtime_version="runtime-v")
    readonly_repository = object()
    calls = []

    monkeypatch.setattr(
        tasks,
        "_load_exact_scope_head_snapshot",
        lambda value: generation,
    )
    monkeypatch.setattr(
        tasks,
        "_qualified_freshness_at",
        lambda value, latest, **kwargs: (
            calls.append((value, latest, kwargs)),
            (value, "complete", OBSERVED_AT),
        )[1],
    )
    readers = SchedulerRuntimeReaders(observed_at=OBSERVED_AT)
    monkeypatch.setattr(
        readers,
        "_frozen_target",
        lambda: (registry, state, ("19425:2026",)),
    )
    monkeypatch.setattr(readers, "_store", lambda: store)
    monkeypatch.setattr(readers, "_repository", lambda: readonly_repository)

    rows = readers.read_scope_heads()

    assert rows[0]["last_complete_at"] == OBSERVED_AT.isoformat(timespec="seconds")
    assert calls == [
        (
            head,
            evidence,
            {
                "expected_registry_ref": state["male_registry_ref"],
                "expected_registry_signature": "a" * 64,
                "observed_at": OBSERVED_AT,
                "repository": readonly_repository,
            },
        )
    ]


@pytest.mark.unit
def test_qualified_freshness_injects_repository_into_physical_verifier(monkeypatch):
    from dags.utils import espn_native_tasks as tasks

    repository = object()
    head = SimpleNamespace(published_at=OBSERVED_AT)
    seen = []
    monkeypatch.setattr(
        tasks,
        "_verified_complete_generation",
        lambda value, *, repository=None: seen.append((value, repository)),
    )

    assert tasks._qualified_freshness_at(
        head,
        None,
        expected_registry_ref={"uri": "s3://registry", "sha256": "a" * 64},
        expected_registry_signature="b" * 64,
        observed_at=OBSERVED_AT,
        repository=repository,
    ) == (head, "complete", OBSERVED_AT)
    assert seen == [(head, repository)]


@pytest.mark.unit
def test_install_artifacts_are_inactive_exact_and_hourly_arm_window_aware(tmp_path):
    patch_path = Path("deploy/espn/watchdog/morning_report_espn_v1.patch")
    cron_path = Path("deploy/espn/watchdog/espn_rollout_observer_v1.cron")
    install_path = Path(
        "deploy/espn/watchdog/install_espn_watchdog_adapter_v1.md"
    )
    patch_text = patch_path.read_text(encoding="utf-8")
    cron_text = cron_path.read_text(encoding="utf-8")
    install_text = install_path.read_text(encoding="utf-8")
    immutable_release = (
        "/root/watchdog/releases/"
        "espn-release-c2225657d354ee2a21f8c2e22daf5e233a25a3c0"
    )

    assert "from espn_watchdog_adapter_v1 import" in patch_text
    assert immutable_release.replace("/espn-release", '/"\n+                "espn-release') in patch_text
    assert "/root/dpf-espn-release" not in patch_text
    assert "CRON_TZ=UTC" in cron_text
    assert re.search(r"^10 \* \* \* \* root ", cron_text, re.MULTILINE)
    assert "--observer hourly --format lines" in cron_text
    assert immutable_release in cron_text
    assert "/root/dpf-espn-release" not in cron_text
    assert "tg-send" not in cron_text
    assert all(
        verb not in cron_text
        for verb in ("docker start", "docker restart", "docker stop", "docker kill")
    )
    assert "1dee57096b30d6362dd7d542aa664ec3758b07e684a8344700db917ce2974e91" in install_text
    assert "a94b6c9fd82a4fd4a5faf2040fe4da93cb768d78f02a51c3082405a1f1b747a9" in install_text
    assert "19198db13821f50db844a90dc5e916d06b8dc69fc8d3ba82c94d0c88a1bd773d" in install_text
    assert "811cf35601cb3a7210005da50b75c1b6db87b3f25852123fe2ce1c2ddbb4af70" in install_text
    assert EXPECTED_PROBE_SHA256 in install_text
    for artifact_path in (
        Path("scripts/espn_watchdog_adapter_v1.py"),
        patch_path,
        cron_path,
    ):
        assert hashlib.sha256(artifact_path.read_bytes()).hexdigest() in install_text
    assert immutable_release in install_text
    assert "morning_report.py.pre-espn-watchdog-v1" in install_text
    assert "espn-rollout-observer-v1.disabled" in install_text
    prepare, activation = install_text.split("## Activate only after review", 1)
    assert 'patch --forward --fuzz=0 -p0 -d "$watchdog_root"' not in prepare
    assert "\n  /etc/cron.d/espn-rollout-observer-v1\n" not in prepare
    patch_use = prepare.index("patch --dry-run")
    for staged_input in (
        "$stage_root/morning_report.py",
        "$stage_root/espn_watchdog_adapter_v1.py",
        "$stage_root/morning_report_espn_v1.patch",
        "$stage_root/espn_rollout_observer_v1.cron",
        "$stage_root/release/scripts/espn_rollout_probe_v1.py",
        "$stage_root/release/scrapers/__init__.py",
        "$stage_root/release/scrapers/espn/__init__.py",
        "$stage_root/release/scrapers/espn/layout.py",
    ):
        assert prepare.index(f'sha256sum "{staged_input}"') < patch_use
    assert '< "$stage_root/morning_report_espn_v1.patch"' in prepare
    assert activation.index('"$watchdog_root/morning_report.py"') < activation.index(
        "/etc/cron.d/espn-rollout-observer-v1"
    )

    fixture = tmp_path / "morning_report.py"
    filler = "".join(f"# filler {index}\n" for index in range(410))
    fixture.write_text(
        "#!/usr/bin/env python3\n"
        "import glob\nimport os\nimport re\nimport subprocess\nimport sys\n"
        "from datetime import datetime, timezone\n\n"
        "TRINO = \"/root/.claude/bin/trino-ro.sh\"\n"
        "TG_SEND = \"/root/.claude/hooks/tg-send.sh\"\n"
        "BRONZE_WARN_H = 36\n"
        + filler
        + "def main():\n"
        "    now = datetime.now().strftime(\"%d.%m %H:%M\")\n"
        "    fresh_lines, _ = freshness()\n"
        "    msg_parts = [\n"
        "        f\"🌅 Сводка платформы {now}\",\n"
        "        \"🖥 Хост и хранилище:\", *host_health(),\n"
        "        \"📦 Дозагрузки:\", *runners(),\n"
        "        \"🥉 Свежесть сырья (часов с последней загрузки):\", *fresh_lines,\n"
        "        gold_age(),\n"
        "        \"📊 Полнота (собрано / в афише источника):\", *completeness(),\n"
        "        *sofascore_player_age(),\n"
        "        \"🔴 Красные раны за 24ч:\", *red_runs(),\n"
        "        \"🎭 Зелёное врёт:\", *green_lies(),\n"
        "        \"⏸ На паузе:\", *paused(),\n"
        "        \"🌐 Прокси:\", *proxy_incidents(),\n"
        "        \"☠️ Потерянные страницы (fbref):\", *fbref_dead_letters(),\n"
        "    ]\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            "patch",
            "--dry-run",
            "--forward",
            "--fuzz=0",
            "-p0",
            "-d",
            str(tmp_path),
        ],
        input=patch_text,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
