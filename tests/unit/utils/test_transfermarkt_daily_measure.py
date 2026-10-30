"""Execute the daily SELECT on fixture rows, with Trino-to-DuckDB translation."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

import duckdb
import pytest
import sqlglot

from dags.utils import transfermarkt_daily_measure as measure
from dags.utils import transfermarkt_current_state as state
from dags.scripts import transfermarkt_daily_report as cli
from tests.unit.dags.test_transfermarkt_scope_planner import (
    _competition,
    _edition,
    _joined_row,
)
from tests.unit.dags.test_transfermarkt_current_state import observation
from tests.unit.scripts.test_run_transfermarkt_current import squad

DAY = "2026-10-08"
START = datetime(2026, 10, 7, 21, tzinfo=timezone.utc)
AS_OF = START + timedelta(days=2)


def denominator(*extra):
    rows = (
        {
            "competition_id": "GB1",
            "live": True,
            "competition_class": "core_club",
            "tier": 1,
        },
        *extra,
    )
    registry = [_joined_row(_competition("GB1"), _edition("GB1", "2026"))]
    return measure.daily_denominator(rows, registry, denominator_version="file-sha256")


def execute(checks=(), signals=(), *, snapshot=None, as_of=AS_OF):
    snapshot = snapshot or denominator()
    sql, params = measure.build_daily_measure_sql(snapshot, DAY, as_of=as_of)
    con = duckdb.connect()
    con.execute("CREATE SCHEMA ops")
    con.execute("CREATE MACRO at_timezone(t, z) AS timezone(z, t)")
    for ddl in state.build_current_state_tables():
        con.execute(
            sqlglot.transpile(
                ddl.split(" WITH (format")[0].replace("iceberg.ops.", "ops."),
                read="trino",
                write="duckdb",
            )[0]
        )
    for check in checks:
        values = check.as_dict()
        values["checked_at"] = state._time(values["checked_at"]).replace(tzinfo=None)
        values["raw_capture_ids_json"] = json.dumps(values.pop("raw_capture_ids"))
        con.execute(
            "INSERT INTO ops.transfermarkt_current_checks_v1 ("
            + ",".join(values)
            + ") VALUES ("
            + ",".join("?" for _ in values)
            + ")",
            tuple(values.values()),
        )
    for signal in signals:
        values = state._signal_values(signal)
        values = {
            key: value.replace(tzinfo=None) if isinstance(value, datetime) else value
            for key, value in values.items()
        }
        con.execute(
            "INSERT INTO ops.transfermarkt_current_signals_v1 ("
            + ",".join(values)
            + ") VALUES ("
            + ",".join("?" for _ in values)
            + ")",
            tuple(values.values()),
        )
    executable = sqlglot.transpile(
        sql.replace("iceberg.ops.", "ops."), read="trino", write="duckdb"
    )[0]
    try:
        cursor = con.execute(executable, params)
        return dict(zip([item[0] for item in cursor.description], cursor.fetchone()))
    finally:
        con.close()


def checks(snapshot, *, at=START + timedelta(hours=1)):
    return [
        state.EditionCheck(
            snapshot.rows[0][1],
            kind,
            "cycle",
            "registry-1",
            snapshot.denominator_hash,
            at,
            "complete",
            "checked",
            ("raw",),
            2,
            2,
        )
        for kind in ("listing", "players", "injury")
    ]


def event(
    signature,
    *,
    at=START + timedelta(hours=1),
    kind="change",
    status="pending",
    commit=None,
):
    obs = replace(observation(signature, at=at), scope_id=denominator().rows[0][1])
    signal = state.mark_seen(None, obs, measurement_kind=kind)
    if status == "failed":
        return state.mark_failed(signal, "failed collector")
    if status == "applied":
        return state.mark_applied(
            signal,
            signature=obs.signature,
            bronze_manifest="manifest",
            committed_at=commit or at + timedelta(hours=1),
        )
    return signal


def test_moscow_day_boundaries_and_time_validation():
    assert measure.day_bounds(DAY) == (START, START + timedelta(days=1))
    with pytest.raises(measure.MeasurementError, match="timezone"):
        measure.utc("2026-10-08T12:00:00")
    with pytest.raises(measure.MeasurementError, match="predates"):
        measure.build_daily_measure_sql(
            denominator(), DAY, as_of=START - timedelta(seconds=1)
        )


def test_coverage_keeps_missing_core_excludes_tail_and_requires_all_checks():
    snap = denominator(
        {
            "competition_id": "MISSING",
            "live": True,
            "competition_class": "core_club",
            "tier": 2,
        },
        {
            "competition_id": "YOUTH",
            "live": True,
            "competition_class": "youth",
            "tier": 2,
        },
    )
    report = execute(checks(snap), snapshot=snap)
    assert (
        report["denominator"],
        report["checked_editions"],
        report["coverage_pct"],
    ) == (2, 1, 50)
    assert snap.blocked_competitions == (
        ("MISSING", "missing current edition in promoted registry"),
    )
    assert execute(checks(snap)[:2], snapshot=snap)["checked_editions"] == 0
    assert report["denominator_version"] == "file-sha256"


@pytest.mark.parametrize(
    "change", ["stale", "future", "wrong_hash", "wrong_registry", "failed"]
)
def test_errors_and_stale_or_foreign_provenance_do_not_refresh(change):
    snap = denominator()
    rows = checks(snap)
    alterations = {
        "stale": {"checked_at": START - timedelta(seconds=1)},
        "future": {"checked_at": START + timedelta(days=1)},
        "wrong_hash": {"denominator_hash": "f" * 64},
        "wrong_registry": {"registry_snapshot_id": "stale"},
        "failed": {"status": "failed", "result": "timeout"},
    }
    rows[1] = replace(rows[1], **alterations[change])
    assert execute(rows, snapshot=snap)["checked_editions"] == 0


def test_freshness_retains_superseded_failures_excludes_cold_baseline_unknown_and_raw_only():
    failed = event("a", status="failed")
    pending = state.mark_seen(
        failed,
        replace(
            failed.observation,
            signature="b" * 64,
            checked_at=START + timedelta(hours=2),
        ),
        measurement_kind="change",
    )
    applied = state.mark_applied(
        pending,
        signature="b" * 64,
        bronze_manifest="real-manifest",
        committed_at=START + timedelta(hours=3),
    )
    others = [
        replace(
            event("c", kind="cold", status="applied"),
            observation=replace(event("c").observation, entity_id="2"),
        ),
        replace(
            event("d", kind="baseline", status="applied"),
            observation=replace(event("d").observation, entity_id="3"),
        ),
        replace(
            event("e", kind="unknown", status="applied"),
            observation=replace(event("e").observation, entity_id="4"),
        ),
        replace(
            event("f", status="applied"),
            observation=replace(event("f").observation, entity="injury"),
            bronze_manifest="raw:1",
            result="raw_durable",
        ),
    ]
    report = execute(signals=[applied, *others])
    assert (
        report["detected_changes"],
        report["mature_changes"],
        report["timely_changes"],
        report["failed_changes"],
    ) == (2, 2, 1, 1)
    assert report["detection_freshness_pct"] == 50
    assert (
        report["cold_events"],
        report["baseline_events"],
        report["unknown_events"],
    ) == (1, 1, 1)


def test_immature_and_late_changes_cannot_create_success():
    report = execute(signals=[event("a")], as_of=START + timedelta(hours=5))
    assert (report["mature_changes"], report["immature_pending"]) == (0, 1)
    assert report["detection_freshness_pct"] is None
    late = event("b", status="applied", commit=START + timedelta(hours=26))
    assert execute(signals=[late])["detection_freshness_pct"] == 0
    assert execute(signals=[])["detection_freshness_pct"] is None


def test_signal_metadata_roundtrip_legacy_unknown_and_supersession():
    cold = event("a", kind="cold")
    legacy = cold.as_dict()
    legacy.pop("measurement_kind")
    assert state.SignalState.from_mapping(legacy).measurement_kind == "unknown"
    rows = state._signal_values(cold)
    rows.pop("measurement_kind")
    assert state.SignalState.from_ops_row(rows).measurement_kind == "unknown"
    failed = state.mark_failed(cold, "crash")
    again = state.mark_seen(
        failed,
        replace(
            failed.observation,
            checked_at=failed.observation.checked_at + timedelta(hours=1),
        ),
        measurement_kind="change",
    )
    assert again.measurement_kind == "cold"
    changed = state.mark_seen(
        failed,
        replace(
            failed.observation,
            signature="b" * 64,
            checked_at=failed.observation.checked_at + timedelta(hours=1),
        ),
        measurement_kind="change",
    )
    assert json.loads(changed.superseded_evidence[0])["measurement_kind"] == "cold"
    assert (
        "ADD COLUMN IF NOT EXISTS measurement_kind"
        in state.build_measurement_kind_migration_sql()
    )


def sample_materials(tmp_path, *, with_bronze=True):
    scope = denominator().rows[0][1]
    url = "https://www.transfermarkt.com/club/kader/verein/10/saison_id/2026/plus/1"
    sample = {
        "scope_id": scope,
        "player_id": "110",
        "club_id": "10",
        "field": "market_value_eur",
    }
    raw_records = {}
    for name, value, stamp in [
        ("previous", "€1.0m", START),
        ("current", "€2.0m", START + timedelta(hours=1)),
    ]:
        body = squad("10", value=value).encode()
        record = {
            "capture_id": name,
            "scope_id": scope,
            "source": "transfermarkt",
            "status_code": 200,
            "hash_algorithm": "sha256",
            "url": url,
            "content_hash": hashlib.sha256(body).hexdigest(),
            "decoded_bytes": len(body),
            "fetched_at": stamp.isoformat(),
        }
        (tmp_path / (name + ".html")).write_bytes(body)
        (tmp_path / (name + ".json")).write_text(json.dumps(record))
        sample[name] = {"body_path": name + ".html", "receipt_path": name + ".json"}
        raw_records[name] = record
    if with_bronze:
        from dags.utils.transfermarkt_current_write import _rows
        import pandas as pd

        current = raw_records["current"]
        rows = [
            {
                "player_id": "110",
                "club_id": "10",
                "scope_id": scope,
                "market_value_eur": 2000000,
                "source_url": url,
                "source_body_hash": current["content_hash"],
                "fetched_at": current["fetched_at"],
                "_batch_id": "batch",
            }
        ]
        columns = list(rows[0])
        table = "iceberg.bronze.transfermarkt_player_attribute_observations"
        outputs = {
            "attributes": {
                "table": table,
                "batch_id": "batch",
                "rows": 1,
                "physical_hash": hashlib.sha256(
                    json.dumps(
                        _rows(pd.DataFrame(rows, columns=columns)), sort_keys=True
                    ).encode()
                ).hexdigest(),
            }
        }
        manifests = [
            {
                "status": "success",
                "rows": [
                    {
                        "status": "success",
                        "native_table": table.removeprefix("iceberg.bronze."),
                        "native_batch_id": "batch",
                        "native_rows": 1,
                        "native_hash": "a" * 64,
                    }
                ],
            }
        ]
        receipt = {
            "verified": True,
            "cycle_id": "write-cycle",
            "outputs": outputs,
            "manifests": manifests,
            "committed_at": (START + timedelta(hours=2)).isoformat(),
        }
        receipt["bronze_manifest"] = hashlib.sha256(
            json.dumps(
                {key: receipt[key] for key in ("cycle_id", "outputs", "manifests")},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        (tmp_path / "bronze.json").write_text(json.dumps(receipt))
        (tmp_path / "readback.json").write_text(
            json.dumps(
                {"table": table, "batch_id": "batch", "columns": columns, "rows": rows}
            )
        )
        sample.update(
            bronze_receipt_path="bronze.json",
            readback_path="readback.json",
            output_key="attributes",
        )
    return sample


def test_sample_validates_raw_pair_and_physical_readback(tmp_path):
    sample = sample_materials(tmp_path)
    report = measure.import_site_sample(
        [sample], evidence_dir=tmp_path, day=DAY, as_of=AS_OF
    )
    assert report["freshness_pct"] == 100
    assert (
        report["samples"][0]["site_first_seen_at"]
        == (START + timedelta(hours=1)).isoformat()
    )
    assert "publication time unknown" in report["clock_definition"]


def test_samples_missing_unknown_and_pending_not_green(tmp_path):
    assert (
        measure.import_site_sample([], evidence_dir=tmp_path, day=DAY, as_of=AS_OF)[
            "freshness_pct"
        ]
        is None
    )
    sample = sample_materials(tmp_path, with_bronze=False)
    assert (
        measure.import_site_sample(
            [sample], evidence_dir=tmp_path, day=DAY, as_of=AS_OF
        )["freshness_pct"]
        == 0
    )
    assert (
        measure.import_site_sample(
            [sample], evidence_dir=tmp_path, day=DAY, as_of=START + timedelta(hours=2)
        )["freshness_pct"]
        is None
    )


@pytest.mark.parametrize(
    "fault", ["body", "readback", "receipt", "duplicate", "old_pair", "path"]
)
def test_forged_and_mismatched_samples_rejected(tmp_path, fault):
    sample = sample_materials(tmp_path)
    samples = [sample]
    if fault == "body":
        (tmp_path / "current.html").write_text("altered")
    elif fault == "readback":
        value = json.loads((tmp_path / "readback.json").read_text())
        value["rows"][0]["market_value_eur"] = 0
        (tmp_path / "readback.json").write_text(json.dumps(value))
    elif fault == "receipt":
        value = json.loads((tmp_path / "bronze.json").read_text())
        value["bronze_manifest"] = "fake"
        (tmp_path / "bronze.json").write_text(json.dumps(value))
    elif fault == "duplicate":
        samples.append(sample)
    elif fault == "old_pair":
        value = json.loads((tmp_path / "previous.json").read_text())
        value["fetched_at"] = (START - timedelta(days=2)).isoformat()
        (tmp_path / "previous.json").write_text(json.dumps(value))
    else:
        sample["current"]["body_path"] = "../outside.html"
    with pytest.raises(measure.MeasurementError):
        measure.import_site_sample(samples, evidence_dir=tmp_path, day=DAY, as_of=AS_OF)


def test_formatter_and_offline_cli_no_samples_unknown(tmp_path, capsys):
    report = execute()
    assert "контроль сайта не доказано" in measure.format_daily_summary(
        report, dq_status="pass"
    )
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report, default=str))
    assert (
        cli.main(
            ["--day", DAY, "--as-of", AS_OF.isoformat(), "--report-json", str(path)]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["site_sample"] is None
    assert "Bronze DQ unknown" in result["summary"]


def test_current_hook_labels_cold_then_change_and_preserves_raw_clocks(
    tmp_path, monkeypatch
):
    from tests.unit.scripts.test_run_transfermarkt_current import Offline

    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "writer.lock"))
    offline = Offline(tmp_path, monkeypatch)
    offline.run("cold")
    data = next(iter(offline.state()["scopes"].values()))
    assert all(
        signal["measurement_kind"] == "cold" for signal in data["signals"].values()
    )
    assert any(check["status"] == "complete" for check in data["checks"].values())
    for check in data["checks"].values():
        if check["status"] != "complete":
            continue
        original = [
            offline.store.load_capture(capture)[1].fetched_at
            for capture in check["raw_capture_ids"]
        ]
        assert measure.utc(check["checked_at"]) == min(
            measure.utc(stamp) for stamp in original
        )
    offline.clock.advance(21 * 3600)
    offline.values["10"] = "€2.0m"
    offline.run("change")
    data = next(iter(offline.state()["scopes"].values()))
    club = data["signals"]["club:10"]
    assert club["measurement_kind"] == "change"
    superseded = json.loads(club["superseded_evidence"][0])
    assert superseded["measurement_kind"] == "cold"
    assert superseded["raw_capture_id"]


@pytest.mark.parametrize("fault", ["day", "as_of", "version"])
def test_cli_rejects_stale_or_mismatched_sql_result(tmp_path, fault):
    report = execute()
    if fault == "day":
        report["day_start"] = START - timedelta(days=1)
    elif fault == "as_of":
        report["as_of"] = AS_OF - timedelta(days=1)
    else:
        report["measurement_version"] = "old"
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report, default=str))
    with pytest.raises(measure.MeasurementError):
        cli.main(
            ["--day", DAY, "--as-of", AS_OF.isoformat(), "--report-json", str(path)]
        )


@pytest.mark.parametrize(
    "completed,day,status,expected,override",
    [
        (True, DAY, "GREEN", "GREEN", {}),
        (False, DAY, "GREEN", "unknown", {}),
        (True, "2026-10-07", "GREEN", "unknown", {}),
        (True, DAY, "RED", "RED", {}),
        (False, DAY, "RED", "RED (неполная проверка)", {}),
        (False, "2026-10-07", "RED", "unknown", {}),
        (False, DAY, "RED", "unknown", {"version": "old"}),
        (False, DAY, "RED", "unknown", {"stale": True}),
        (False, DAY, "RED", "unknown", {"cutoff_at": (AS_OF + timedelta(seconds=1)).isoformat()}),
    ],
)
def test_cli_reads_dq_status_from_valid_same_day_evidence(
    tmp_path, capsys, completed, day, status, expected, override
):
    from dags.utils.transfermarkt_daily_dq import VERSION

    report = execute()
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report, default=str))
    dq = {
        "day_msk": day,
        "version": VERSION,
        "completed": completed,
        "status": status,
        "cutoff_at": START.isoformat(),
    }
    dq.update(override)
    dqpath = tmp_path / "dq.json"
    dqpath.write_text(json.dumps(dq))
    cli.main(
        [
            "--day",
            DAY,
            "--as-of",
            AS_OF.isoformat(),
            "--report-json",
            str(path),
            "--dq-json",
            str(dqpath),
        ]
    )
    assert f"Bronze DQ {expected}" in json.loads(capsys.readouterr().out)["summary"]


def test_site_sampling_does_not_equate_observation_to_publication_clock(tmp_path):
    sample = sample_materials(tmp_path)
    value = json.loads((tmp_path / "bronze.json").read_text())
    value["committed_at"] = (START + timedelta(hours=24, minutes=30)).isoformat()
    (tmp_path / "bronze.json").write_text(json.dumps(value))
    report = measure.import_site_sample(
        [sample], evidence_dir=tmp_path, day=DAY, as_of=AS_OF
    )
    assert report["freshness_pct"] == 0
    assert report["appearance_interval_unproven"] == 1
    assert report["samples"][0]["observed_timely"] is True
    assert report["samples"][0]["timely"] is False


def test_daily_promotion_preserves_equivalent_edition_coverage_but_invalidates_drift():
    old = denominator()
    proved = [
        replace(
            check,
            edition_fingerprint=old.rows[0][5],
            denominator_definition_hash=old.denominator_definition_hash,
        )
        for check in checks(old)
    ]
    core = [
        {
            "competition_id": "GB1",
            "live": True,
            "competition_class": "core_club",
            "tier": 1,
        }
    ]
    row = _joined_row(_competition("GB1"), _edition("GB1", "2026"))
    row["registry_snapshot_id"] = "registry-daily-promotion"
    promoted = measure.daily_denominator(
        core, [row], denominator_version="new-file-proof"
    )
    assert old.denominator_hash != promoted.denominator_hash
    assert old.rows[0][5] == promoted.rows[0][5]
    assert execute(proved, snapshot=promoted)["checked_editions"] == 1
    assert (
        execute(checks(old), snapshot=promoted)["checked_editions"] == 0
    )  # Old missing fingerprints are not waived.
    changed = {**row, "participant_hash": "new-participants"}
    drift = measure.daily_denominator(core, [changed], denominator_version="v2")
    assert execute(proved, snapshot=drift)["checked_editions"] == 0
    new_edition = _joined_row(_competition("GB1"), _edition("GB1", "2027"))
    replacement = measure.daily_denominator(
        core, [new_edition], denominator_version="v3"
    )
    assert execute(proved, snapshot=replacement)["checked_editions"] == 0
    incompatible_core = [{**core[0], "competition_class": "core_national"}]
    replacement = measure.daily_denominator(
        incompatible_core, [row], denominator_version="v4"
    )
    assert execute(proved, snapshot=replacement)["checked_editions"] == 0


def test_multiple_current_editions_require_all_and_national_injury_requires_rule():
    snap = denominator()
    another = (snap.rows[0][0], "another-scope", "2027", "registry-1", "club", "f" * 64)
    both = replace(snap, rows=(*snap.rows, another))
    assert execute(checks(snap), snapshot=both)["checked_editions"] == 0
    extra = [replace(check, scope_id="another-scope") for check in checks(snap)]
    assert execute([*checks(snap), *extra], snapshot=both)["checked_editions"] == 1
    national = replace(
        snap, rows=(tuple([*snap.rows[0][:4], "national_team", snap.rows[0][5]]),)
    )
    assert execute(checks(snap), snapshot=national)["checked_editions"] == 0
    passed = checks(snap)
    passed[2] = replace(
        passed[2],
        result=json.dumps(
            {
                "applicability_status": "not_applicable",
                "rule": "league_injury_listing:club_competitions",
                "registry_snapshot_id": "registry-1",
            }
        ),
    )
    assert execute(passed, snapshot=national)["checked_editions"] == 1


def endpoint_sample(tmp_path, kind):
    from scrapers.transfermarkt.scraper import (
        _parse_transfers,
        _parse_mv_history,
        _parse_coach_history,
        _parse_squad_page,
    )
    from tests.unit.scrapers.test_transfermarkt_scraper import (
        _history_html,
        _history_row,
    )
    from dags.utils.transfermarkt_current_write import _rows
    import pandas as pd

    sample = sample_materials(tmp_path)
    sample["kind"] = kind
    sample.pop("field")
    if kind == "roster":
        bodies = [squad("10", players=["111"]), squad("10", players=["110", "111"])]
        def parser(body):
            return _parse_squad_page(body, "10")
        key = {"player_id": "110"}
        route = "/club/kader/verein/10/saison_id/2026/plus/1"
        table = "transfermarkt_squad_memberships"
    elif kind == "transfer":
        move = {
            "date": "Oct 8, 2026",
            "season": "26/27",
            "upcoming": False,
            "from": {"clubName": "A", "href": "/a/transfers/verein/20"},
            "to": {"clubName": "B", "href": "/b/transfers/verein/10"},
            "fee": "€1m",
            "marketValue": "€2m",
        }
        bodies = [json.dumps({"transfers": []}), json.dumps({"transfers": [move]})]
        def parser(body):
            return _parse_transfers(json.loads(body), "110")
        key = {"transfer_id": parser(bodies[1])[0]["transfer_id"]}
        route = "/ceapi/transferHistory/list/110"
        table = "transfermarkt_transfer_events"
    elif kind == "valuation":
        bodies = [
            json.dumps({"list": []}),
            json.dumps(
                {"list": [{"datum_mw": "Oct 8, 2026", "y": 2000000, "verein": "B"}]}
            ),
        ]
        def parser(body):
            return _parse_mv_history(json.loads(body), "110")
        key = {"mv_date": "2026-10-08"}
        route = "/ceapi/marketValueDevelopment/graph/110"
        table = "transfermarkt_market_value_points"
    else:
        bodies = [
            _history_html(""),
            _history_html(_history_row("coach", "42", "Coach", "Oct 8, 2026", "-")),
        ]
        def parser(body):
            return _parse_coach_history(body, "10")
        key = {"coach_id": "42", "appointed_date": "2026-10-08", "role": "Manager"}
        route = "/club/mitarbeiterhistorie/verein/10/plus/1"
        table = "transfermarkt_coach_stints"
    url = "https://www.transfermarkt.com" + route
    for name, body in zip(("previous", "current"), bodies):
        raw = json.loads((tmp_path / (name + ".json")).read_text())
        raw.update(
            url=url,
            content_hash=hashlib.sha256(body.encode()).hexdigest(),
            decoded_bytes=len(body.encode()),
        )
        (tmp_path / (name + ".html")).write_text(body)
        (tmp_path / (name + ".json")).write_text(json.dumps(raw))
    current = json.loads((tmp_path / "current.json").read_text())
    sample["row_key"] = key if kind != "roster" else {}
    parsed = [
        row
        for row in parser(bodies[1])
        if all(str(row.get(k)) == str(v) for k, v in key.items())
    ][0]
    row = {
        k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in parsed.items()
    }
    row.update(
        scope_id=sample["scope_id"],
        source_url=url,
        source_body_hash=current["content_hash"],
        fetched_at=current["fetched_at"],
        _batch_id="batch",
    )
    columns = list(row)
    table = "iceberg.bronze." + table
    physical = hashlib.sha256(
        json.dumps(_rows(pd.DataFrame([row], columns=columns)), sort_keys=True).encode()
    ).hexdigest()
    output = {"table": table, "batch_id": "batch", "rows": 1, "physical_hash": physical}
    receipt = json.loads((tmp_path / "bronze.json").read_text())
    receipt["outputs"] = {"attributes": output}
    receipt["manifests"] = [
        {
            "status": "success",
            "rows": [
                {
                    "status": "success",
                    "native_table": table.removeprefix("iceberg.bronze."),
                    "native_batch_id": "batch",
                    "native_rows": 1,
                    "native_hash": "a" * 64,
                }
            ],
        }
    ]
    receipt["bronze_manifest"] = hashlib.sha256(
        json.dumps(
            {k: receipt[k] for k in ("cycle_id", "outputs", "manifests")},
            sort_keys=True,
        ).encode()
    ).hexdigest()
    (tmp_path / "bronze.json").write_text(json.dumps(receipt))
    (tmp_path / "readback.json").write_text(
        json.dumps(
            {"table": table, "batch_id": "batch", "columns": columns, "rows": [row]}
        )
    )
    return sample


@pytest.mark.parametrize("kind", ["roster", "transfer", "valuation", "coach"])
def test_endpoint_specific_raw_and_native_readback_samples(tmp_path, kind):
    sample = endpoint_sample(tmp_path, kind)
    report = measure.import_site_sample(
        [sample], evidence_dir=tmp_path, day=DAY, as_of=AS_OF
    )
    assert report["freshness_pct"] == 100
    assert report["samples"][0]["kind"] == kind


def test_unsupported_sample_is_unproven_instead_of_synthetic_row(tmp_path):
    sample = sample_materials(tmp_path)
    sample["kind"] = "unimplemented"
    report = measure.import_site_sample(
        [sample], evidence_dir=tmp_path, day=DAY, as_of=AS_OF
    )
    assert report["freshness_pct"] == 0
    assert report["unsupported_samples"] == 1
    assert report["samples"][0]["unproven_reason"] == "unsupported_change_kind"


@pytest.mark.parametrize('name', [
    'standing_approval_policy.json', 'standing_registry_policy.json', 'standing_backfill_policy.json',
])
@pytest.mark.parametrize('days', [29, 30, 31, 0, -1])
def test_cli_policy_expiry_uses_current_clock(tmp_path, monkeypatch, capsys, name, days):
    now = datetime(2026, 12, 1, tzinfo=timezone.utc)
    policy_dir = tmp_path / 'dags' / 'configs' / 'transfermarkt'
    policy_dir.mkdir(parents=True)
    for source in (cli.ROOT / 'dags' / 'configs' / 'transfermarkt').glob('standing*policy.json'):
        body = json.loads(source.read_text())
        expires = now + timedelta(days=days if source.name == name else 60)
        body['expires_at'] = expires.isoformat()
        (policy_dir / source.name).write_text(json.dumps(body))
    monkeypatch.setattr(cli, 'ROOT', tmp_path)
    monkeypatch.setattr(cli, '_utc_now', lambda: now, raising=False)
    path = tmp_path / 'report.json'
    path.write_text(json.dumps(execute(), default=str))
    assert cli.main(['--day', DAY, '--as-of', AS_OF.isoformat(), '--report-json', str(path)]) == 0
    summary = json.loads(capsys.readouterr().out)['summary']
    assert ('ТРЕВОГА' in summary) == (days < 30)
    if days < 30:
        assert name in summary
        assert (now + timedelta(days=days)).isoformat() in summary
        assert ('истекла' in summary) == (days <= 0)


@pytest.mark.parametrize('fault', ['missing', 'json', 'schema', 'encoding'])
def test_cli_policy_read_error_is_visible(tmp_path, monkeypatch, capsys, fault):
    policy_dir = tmp_path / 'dags' / 'configs' / 'transfermarkt'
    policy_dir.mkdir(parents=True)
    for source in (cli.ROOT / 'dags' / 'configs' / 'transfermarkt').glob('standing*policy.json'):
        (policy_dir / source.name).write_bytes(source.read_bytes())
    name = 'standing_approval_policy.json'
    path = policy_dir / name
    if fault == 'missing':
        path.unlink()
    else:
        path.write_bytes({'json': b'{', 'schema': b'{}', 'encoding': b'\xff'}[fault])
    monkeypatch.setattr(cli, 'ROOT', tmp_path)
    monkeypatch.setattr(cli, '_utc_now', lambda: datetime(2026, 10, 28, tzinfo=timezone.utc), raising=False)
    report = tmp_path / 'report.json'
    report.write_text(json.dumps(execute(), default=str))
    assert cli.main(['--day', DAY, '--as-of', AS_OF.isoformat(), '--report-json', str(report)]) == 0
    summary = json.loads(capsys.readouterr().out)['summary']
    assert f'ТРЕВОГА: {name}: ошибка чтения политики' in summary


@pytest.mark.parametrize("dq_input", ["--dq-json", "--dq-state-root"])
@pytest.mark.parametrize(
    "completed,status,override,expected",
    [
        (True, "RED", {}, "RED"),
        (False, "RED", {}, "RED (неполная проверка)"),
        (False, "GREEN", {}, "unknown"),
        (True, "RED", {"day_msk": "2026-10-07"}, "unknown"),
        (True, "RED", {"version": "old"}, "unknown"),
        (True, "RED", {"cutoff_at": (AS_OF + timedelta(seconds=1)).isoformat()}, "unknown"),
    ],
)
def test_cli_sql_failure_preserves_independent_dq(
    tmp_path, monkeypatch, capsys, dq_input, completed, status, override, expected
):
    from dags.utils.transfermarkt_daily_dq import VERSION

    def fail(*args):
        raise RuntimeError("query failed: private connection details")

    monkeypatch.setattr(cli, "execute_measure", fail)
    path = tmp_path / f"{DAY}.json"
    dq = {"day_msk": DAY, "version": VERSION, "completed": completed,
          "status": status, "cutoff_at": START.isoformat()}
    dq.update(override)
    path.write_text(json.dumps(dq))
    result = cli.main(["--day", DAY, "--as-of", AS_OF.isoformat(), "--execute",
                       dq_input, str(path if dq_input == "--dq-json" else tmp_path)])
    output = json.loads(capsys.readouterr().out)
    assert result == 1
    assert output["report"] is None
    assert output["status"] == "not_measured"
    assert "не измерено" in output["summary"]
    assert "охват 0/" not in output["summary"]
    assert "private connection details" not in output["summary"]
    assert output["summary"].endswith(f"Bronze DQ {expected}")


@pytest.mark.parametrize("fault", [None, "journal", "sql", "zero", "two"])
def test_cli_executes_promoted_registry_daily_select(tmp_path, monkeypatch, capsys, fault):
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_scope_planner import build_promoted_registry_query
    from scrapers.transfermarkt.denominator import COLUMNS, load_denominator

    path = tmp_path / "denominator.tsv"
    path.write_text("\t".join(COLUMNS) + "\n" + "".join(
        "\t".join([cid, "League", "England", "UEFA", "1", "core_club",
                   "wettbewerb", "1", "2026", "", "", "", "", "rule:default"]) + "\n"
        for cid in ("GB1", "MISSING")
    ))
    registry = [_joined_row(_competition("GB1"), _edition("GB1", "2026"))]
    snapshot = measure.daily_denominator(
        load_denominator(path).rows.values(), registry,
        denominator_version=hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    expected_sql, expected_params = measure.build_daily_measure_sql(snapshot, DAY, as_of=AS_OF)
    report = execute(checks(snapshot), snapshot=snapshot)

    class Cursor:
        closed = False

        def execute(self, sql, params=()):
            statements.append((sql, params))
            if len(statements) == 1:
                assert sql == build_promoted_registry_query(current_only=True)
                values = registry
            else:
                assert (sql, params) == (expected_sql, expected_params)
                if fault in {"journal", "sql"}:
                    raise RuntimeError("TABLE_NOT_FOUND" if fault == "journal" else "query failed")
                values = [] if fault == "zero" else [report] * (2 if fault == "two" else 1)
            self.description = [(key,) for key in (values[0] if values else report)]
            self.rows = [tuple(value.values()) for value in values]

        def fetchall(self):
            return self.rows

        def close(self):
            self.closed = True

    class Connection:
        closed = False

        def cursor(self):
            return cursor

        def close(self):
            self.closed = True

    statements = []
    cursor, connection = Cursor(), Connection()
    monkeypatch.setattr(native, "connect", lambda: connection)
    result = cli.main(["--day", DAY, "--as-of", AS_OF.isoformat(),
                       "--execute", "--denominator", str(path)])
    output = json.loads(capsys.readouterr().out)
    assert len(statements) == 2
    assert cursor.closed and connection.closed
    if fault:
        assert result == 1
        assert output["report"] is None
        assert output["status"] == "not_measured"
        assert "не измерено" in output["summary"]
        assert "охват 0/" not in output["summary"]
    else:
        assert result == 0
        assert output["report"]["denominator"] == 2
        assert output["report"]["checked_editions"] == 1
        assert output["report"]["coverage_pct"] == 50
        assert snapshot.blocked_competitions[0][0] == "MISSING"
        assert output["summary"] == measure.format_daily_summary(report)


def execute_history(scopes=(), attempts=(), batches=(), *, day=DAY):
    from dags.utils import transfermarkt_backfill_state as backfill

    sql, params = measure.build_history_day_sql(day, as_of=AS_OF)
    con = duckdb.connect()
    con.execute("CREATE SCHEMA ops")
    for ddl in backfill.ddl_statements():
        con.execute(sqlglot.transpile(ddl.split(" WITH (format")[0].replace("iceberg.ops.", "ops."),
                                      read="trino", write="duckdb")[0])
    for status, updated_at in scopes:
        con.execute("INSERT INTO ops.transfermarkt_backfill_scope_v1 (status, updated_at) VALUES (?, ?)",
                    (status, updated_at.replace(tzinfo=None)))
    for evidence, observed_at, finished_at in attempts:
        con.execute("INSERT INTO ops.transfermarkt_backfill_attempt_v1 "
                    "(raw_evidence_ids_json, source_observed_at, finished_at) VALUES (?, ?, ?)",
                    (json.dumps(evidence), observed_at and observed_at.replace(tzinfo=None),
                     finished_at.replace(tzinfo=None)))
    for streams, claimed_at in batches:
        record = {"scope_stream_ids": streams} if streams else {}
        con.execute("INSERT INTO ops.transfermarkt_backfill_batch_v1 (record_json, claimed_at) VALUES (?, ?)",
                    (json.dumps(record), claimed_at.replace(tzinfo=None)))
    executable = sqlglot.transpile(sql.replace("iceberg.ops.", "ops."), read="trino", write="duckdb")[0]
    try:
        cursor = con.execute(executable, params)
        return dict(zip([item[0] for item in cursor.description], cursor.fetchone()))
    finally:
        con.close()


def test_history_day_counts_completed_editions_new_requests_and_streams():
    inside, before, after = START + timedelta(hours=2), START - timedelta(hours=1), START + timedelta(days=1, hours=1)
    report = execute_history(
        scopes=[("complete", inside), ("complete", inside), ("complete", before), ("running", inside),
                ("complete", after)],
        # "e1" was already sent yesterday; a continuation re-references it.  "e5" was
        # sent before Moscow midnight but finalised after it; "e6" has no observed time.
        attempts=[(["e1"], before, before), (["e1", "e2", "e3"], inside, inside), (["e4"], after, after),
                  (["e5"], START + timedelta(days=1) - timedelta(minutes=5), START + timedelta(days=1, minutes=10)),
                  (["e6"], None, inside)],
        batches=[({"s1": "history-0", "s2": "history-1"}, inside), ({"s3": "history-0"}, inside),
                 ({"s4": "history-2"}, before), (None, inside)],
    )
    assert (report["editions"], report["requests"], report["streams"]) == (2, 4, 2)
    summary = measure.format_history_summary({**report, "report_day": DAY})
    assert summary == (f"Transfermarkt история {DAY} МСК: изданий 2 (цель ≥ 57), "
                       "запросов 4 (ориентир 16,5 тыс.; ±1 попытка на границе суток), потоков активных 2")


def test_history_day_dates_a_midnight_straddling_attempt_by_its_last_request():
    # Known bound: "m1" was sent at 23:55 МСК, "m2" at 00:05; the attempt carries only
    # its latest request time, so both count on the next day.
    midnight = START + timedelta(days=1)
    attempt = (["m1", "m2"], midnight + timedelta(minutes=5), midnight + timedelta(minutes=10))
    assert execute_history(attempts=[attempt])["requests"] == 0
    assert execute_history(attempts=[attempt], day="2026-10-09")["requests"] == 2


@pytest.mark.parametrize("fault", [None, "sql", "two"])
def test_cli_history_reads_journal_and_hides_errors(monkeypatch, capsys, fault):
    from dags.utils import transfermarkt_native_v2 as native

    expected = measure.build_history_day_sql(DAY, as_of=AS_OF.isoformat())
    row = {"editions": 3, "requests": 900, "streams": 2}

    class Cursor:
        def execute(self, sql, params=()):
            assert (sql, params) == expected
            if fault == "sql":
                raise RuntimeError("private connection details")
            self.description = [(key,) for key in row]
            self.rows = [tuple(row.values())] * (2 if fault == "two" else 1)

        def fetchall(self):
            return self.rows

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(native, "connect", lambda: Connection())
    result = cli.main(["--day", DAY, "--as-of", AS_OF.isoformat(), "--history"])
    output = json.loads(capsys.readouterr().out)
    if fault:
        assert result == 1 and output["report"] is None
        assert output["summary"] == f"Transfermarkt история {DAY} МСК: не измерено — ошибка чтения журнала истории"
    else:
        assert result == 0
        assert output["summary"] == measure.format_history_summary({**row, "report_day": DAY})
        assert "цель ≥ 57" in output["summary"] and "16,5 тыс." in output["summary"]
