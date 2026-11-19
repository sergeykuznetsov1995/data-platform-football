#!/usr/bin/env python3
"""Execute/render a daily SELECT or import exported evidence.

--execute/--history read Trino using the native connection; no site requests or writes.
SQL mode needs promoted-registry rows; report mode needs the SELECT result.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.append(str(ROOT / "dags"))


def _utc_now():
    return datetime.now(timezone.utc)


def policy_expiry_warnings():
    from dags.utils.transfermarkt_approval import ApprovalError, load_standing_policy

    now = _utc_now()
    warnings = []
    for name in (
        "standing_approval_policy.json",
        "standing_registry_policy.json",
        "standing_backfill_policy.json",
    ):
        path = ROOT / "dags" / "configs" / "transfermarkt" / name
        try:
            policy = load_standing_policy(path)
        except (ApprovalError, UnicodeError) as exc:
            warnings.append(
                f"ТРЕВОГА: {name}: ошибка чтения политики ({type(exc).__name__})"
            )
            continue
        if policy.expires_at - now < timedelta(days=30):
            status = (
                "истекла"
                if policy.expires_at <= now
                else "истекает менее чем через 30 дней"
            )
            warnings.append(
                f"ТРЕВОГА: {name}: {status}; expires_at={policy.expires_at.isoformat()}"
            )
    return warnings


def _read(cur):
    from dags.utils.transfermarkt_daily_measure import MeasurementError

    rows = list(cur.fetchall())
    columns = [item[0] for item in (cur.description or ())]
    if not columns or len(set(columns)) != len(columns):
        raise MeasurementError("invalid SQL result columns")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def execute_history(day, as_of):
    from dags.utils.transfermarkt_daily_measure import (
        MeasurementError,
        build_history_day_sql,
    )
    from dags.utils import transfermarkt_native_v2 as native

    sql, params = build_history_day_sql(day, as_of=as_of)
    connection = native.connect()
    try:
        cur = connection.cursor()
        try:
            cur.execute(sql, params)
            rows = _read(cur)
            if len(rows) != 1:
                raise MeasurementError("history measure must return exactly one row")
            return rows[0]
        finally:
            cur.close()
    finally:
        connection.close()


def _unique_object(pairs):
    from dags.utils.transfermarkt_daily_measure import MeasurementError

    result = {}
    for key, value in pairs:
        if key in result:
            raise MeasurementError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _json_document(raw):
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)


def _valid_id(value):
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def _registry_snapshot(registry, expected):
    from dags.utils.transfermarkt_daily_measure import MeasurementError
    from dags.utils.transfermarkt_denominator_registry import registry_snapshot_id

    actual = registry_snapshot_id(registry)
    if expected is not None and actual != expected:
        raise MeasurementError(
            f"expected registry snapshot {expected!r}, got {actual!r}"
        )
    return actual


def _manifest_ids(section, key):
    from dags.utils.transfermarkt_daily_measure import MeasurementError

    values = section.get(key)
    if (
        not isinstance(values, list)
        or not all(_valid_id(value) for value in values)
        or len(values) != len(set(values))
    ):
        raise MeasurementError(f"{key} must be a list of unique nonempty string IDs")
    return values


def _manifest_metadata(path, registry, expected):
    from dags.utils.transfermarkt_daily_measure import MeasurementError
    from dags.utils.transfermarkt_registry_publish import stable_hash

    actual = _registry_snapshot(registry, expected)
    if path is None:
        raise MeasurementError("dual-run requires discovery manifest metadata")
    raw = Path(path).read_bytes()
    document = _json_document(raw)
    if not isinstance(document, dict):
        raise MeasurementError("discovery manifest document must be an object")
    manifest = document.get("manifest", document)
    if not isinstance(manifest, dict):
        raise MeasurementError("manifest must be an object")
    snapshot = manifest.get("snapshot_id")
    if not _valid_id(snapshot) or snapshot != actual:
        raise MeasurementError(
            f"manifest snapshot_id {snapshot!r} differs from registry {actual!r}"
        )
    section = manifest.get("discovery_report")
    if not isinstance(section, dict):
        raise MeasurementError("discovery_report must be an object")
    quarantine = set(_manifest_ids(manifest, "quarantined_competition_ids"))
    title_only = _manifest_ids(section, "title_only_competition_ids")
    reasons = section.get("discovery_quarantined")
    if not isinstance(reasons, dict):
        raise MeasurementError("discovery_quarantined must be an object")
    if set(reasons) != quarantine:
        raise MeasurementError(
            f"discovery_quarantined ID mismatch: {sorted(set(reasons) ^ quarantine)}"
        )
    for cid, reason in reasons.items():
        if not isinstance(reason, str) or not reason.strip():
            raise MeasurementError(
                f"discovery_quarantined requires a nonblank reason for {cid}"
            )
    collisions = quarantine.intersection(
        str(row.get("competition_id", "")).strip() for row in registry
    )
    if collisions:
        raise MeasurementError(f"published/quarantine collision: {sorted(collisions)}")

    content = {key: value for key, value in manifest.items() if key != "manifest_hash"}
    digest = stable_hash(content)
    for obj in (document, manifest):
        if "manifest_hash" in obj and obj["manifest_hash"] != digest:
            raise MeasurementError(
                "declared manifest_hash differs from discovery manifest content"
            )
    identity = {
        "manifest_snapshot_id": snapshot,
        "manifest_hash": digest,
        "manifest_file_sha256": hashlib.sha256(raw).hexdigest(),
    }
    return reasons, title_only, identity


def _selected_denominator(source, registry, path, *, metadata=None):
    from dags.utils.transfermarkt_daily_measure import daily_denominator
    from dags.utils.transfermarkt_denominator_registry import build_registry_denominator
    from scrapers.transfermarkt.denominator import load_versioned_denominator

    if source == "registry":
        quarantined, title_only = metadata[:2] if metadata is not None else ({}, ())
        selected = build_registry_denominator(
            registry,
            quarantined=quarantined,
            title_only_ids=title_only,
        )
        return daily_denominator(
            selected.denominator.rows.values(),
            registry,
            denominator_version=selected.universe_version,
        ), selected
    legacy, version = load_versioned_denominator(path)
    return daily_denominator(
        legacy.rows.values(), registry, denominator_version=version
    ), legacy


def _dual_denominators(registry, path, manifest_path, expected=None):
    from dags.utils.transfermarkt_daily_measure import daily_denominator
    from dags.utils.transfermarkt_denominator_registry import (
        build_registry_denominator,
        compare_denominators,
    )
    from scrapers.transfermarkt.denominator import load_versioned_denominator

    quarantine, titles, identity = _manifest_metadata(manifest_path, registry, expected)
    current_definition = build_registry_denominator(
        registry, quarantined=quarantine, title_only_ids=titles
    )
    legacy_definition, version = load_versioned_denominator(path)
    diff = compare_denominators(
        legacy_definition, current_definition, legacy_version=version
    )
    diff.update(identity)
    if not diff["migration_clean"]:
        return None, None, diff
    legacy = daily_denominator(
        legacy_definition.rows.values(), registry, denominator_version=version
    )
    current = daily_denominator(
        current_definition.denominator.rows.values(),
        registry,
        denominator_version=current_definition.universe_version,
    )
    return legacy, current, diff


def _measure_payload(denominator, day, as_of, journal_snapshots=None):
    from dags.utils.transfermarkt_daily_measure import build_daily_measure_sql, utc

    sql, params = build_daily_measure_sql(
        denominator, day, as_of=as_of, journal_snapshots=journal_snapshots
    )
    return {
        "sql": sql,
        "params": params,
        "report_day": day,
        "as_of": utc(as_of).isoformat(),
        "journal_snapshots": journal_snapshots,
        "denominator_hash": denominator.denominator_hash,
        "denominator_version": denominator.denominator_version,
        "blocked_competitions": dict(denominator.blocked_competitions),
        "current_editions": denominator.rows,
        "denominator_definition_hash": denominator.denominator_definition_hash,
    }


def _main_journal_snapshots(cur):
    from dags.utils.transfermarkt_current_state import CHECKS_TABLE, SIGNALS_TABLE
    from dags.utils.transfermarkt_daily_measure import (
        MeasurementError,
        validate_journal_snapshots,
    )

    selects = []
    for table in (CHECKS_TABLE, SIGNALS_TABLE):
        schema, name = table.rsplit(".", 1)
        selects.append(
            f"SELECT '{table}' AS journal, snapshot_id "
            f'FROM {schema}."{name}$refs" '
            "WHERE name = 'main'"
        )
    cur.execute(" UNION ALL ".join(selects))
    pins = {}
    for row in _read(cur):
        table = row["journal"]
        if table in pins:
            raise MeasurementError(f"multiple main refs for {table}")
        pins[table] = row["snapshot_id"]
    return validate_journal_snapshots(pins)


def _read_measure(cur, payload):
    from dags.utils.transfermarkt_daily_measure import MeasurementError

    cur.execute(payload["sql"], payload["params"])
    rows = _read(cur)
    if len(rows) != 1:
        raise MeasurementError("daily measure must return exactly one row")
    if rows[0].get("denominator_version") != payload["denominator_version"]:
        raise MeasurementError(
            "measured denominator version differs from the selected version"
        )
    return rows[0]


def _measure_error(exc):
    from dags.utils.transfermarkt_daily_measure import MeasurementError
    from scrapers.transfermarkt.denominator import DenominatorError

    if isinstance(exc, (MeasurementError, DenominatorError, json.JSONDecodeError)):
        return str(exc)
    return "daily measure input or query failed"


def _dual_output(
    registry, path, manifest_path, expected, day, as_of, pins, *, cur=None
):
    from dags.utils.transfermarkt_daily_measure import utc

    normalized = utc(as_of).isoformat()
    legacy, current, diff = _dual_denominators(registry, path, manifest_path, expected)
    if not diff["migration_clean"]:
        return {
            "status": "blocked",
            "error": "denominator comparison failed",
            "diff": diff,
        }
    if cur is not None and pins is None:
        pins = _main_journal_snapshots(cur)
    result = {
        "status": "sql_only" if cur is None else "measured",
        "diff": diff,
        "journal_snapshots": pins,
        "report_day": day,
        "as_of": normalized,
    }
    for name, denominator in (("legacy", legacy), ("registry", current)):
        payload = _measure_payload(denominator, day, normalized, pins)
        payload["denominator_source"] = "tsv" if name == "legacy" else "registry"
        if cur is not None:
            payload["report"] = _read_measure(cur, payload)
        result[name] = payload
    return result


def _single_payload(source, registry, path, expected, manifest_path, day, as_of, pins):
    from dags.utils.transfermarkt_denominator_registry import denominator_source

    metadata = None
    if manifest_path is not None:
        metadata = _manifest_metadata(manifest_path, registry, expected)
    elif expected is not None:
        _registry_snapshot(registry, expected)
    selected_source = denominator_source(source)
    denominator, _ = _selected_denominator(
        selected_source, registry, path, metadata=metadata
    )
    payload = _measure_payload(denominator, day, as_of, pins)
    payload["denominator_source"] = selected_source
    return payload


def execute_measure(
    day,
    as_of,
    path,
    *,
    source=None,
    registry_snapshot_id=None,
    discovery_manifest_path=None,
    dual_run=False,
    journal_snapshots=None,
):
    from dags.utils.transfermarkt_daily_measure import MeasurementError
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_scope_planner import build_promoted_registry_query

    connection = native.connect()
    try:
        cur = connection.cursor()
        try:
            cur.execute(
                build_promoted_registry_query(
                    registry_snapshot_id=registry_snapshot_id,
                    current_only=True,
                )
            )
            registry = _read(cur)
            if not registry:
                raise MeasurementError("no promoted registry available")
            if dual_run:
                return _dual_output(
                    registry,
                    path,
                    discovery_manifest_path,
                    registry_snapshot_id,
                    day,
                    as_of,
                    journal_snapshots,
                    cur=cur,
                )
            payload = _single_payload(
                source,
                registry,
                path,
                registry_snapshot_id,
                discovery_manifest_path,
                day,
                as_of,
                journal_snapshots,
            )
            report = _read_measure(cur, payload)
            report["denominator_source"] = payload["denominator_source"]
            if journal_snapshots is not None:
                report["journal_snapshots"] = journal_snapshots
            return report
        finally:
            cur.close()
    finally:
        connection.close()


def main(argv=None) -> int:
    from dags.utils.transfermarkt_daily_measure import (
        VERSION,
        MeasurementError,
        day_bounds,
        format_daily_summary,
        import_site_sample,
        utc,
        validate_journal_snapshots,
    )
    from scrapers.transfermarkt.denominator import denominator_path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, help="YYYY-MM-DD, Moscow calendar day")
    parser.add_argument("--as-of", required=True, help="ISO8601 including timezone")
    parser.add_argument(
        "--registry-json", type=Path, help="promoted scope query rows; SQL mode"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="read promoted registry and execute daily SELECT in Trino",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="read the history ops journal for --day in Trino",
    )
    parser.add_argument("--denominator", type=Path, default=None)
    parser.add_argument(
        "--denominator-source", choices=("tsv", "registry"), default=None
    )
    parser.add_argument("--denominator-dual-run", action="store_true")
    parser.add_argument("--discovery-manifest-json", type=Path, default=None)
    parser.add_argument("--registry-snapshot-id", default=None)
    parser.add_argument(
        "--journal-snapshots-json",
        type=Path,
        help="Iceberg journal snapshot IDs for replay",
    )
    parser.add_argument(
        "--report-json", type=Path, help="one daily SELECT result object"
    )
    parser.add_argument(
        "--samples-json",
        type=Path,
        help="offline sample list; paths relative to its directory",
    )
    parser.add_argument("--dq-json", type=Path, help="same-day daily Bronze DQ result")
    parser.add_argument(
        "--dq-state-root",
        type=Path,
        help="read existing daily DQ result; does not run DQ",
    )
    args = parser.parse_args(argv)
    if (
        sum(
            (
                bool(args.registry_json),
                bool(args.report_json),
                args.execute,
                args.history,
            )
        )
        != 1
    ):
        parser.error(
            "choose exactly one of --registry-json, --report-json, --execute and --history"
        )
    if args.denominator_dual_run and not (args.registry_json or args.execute):
        parser.error("dual-run requires --registry-json or --execute")
    if args.denominator_dual_run and args.discovery_manifest_json is None:
        print(
            json.dumps(
                {
                    "status": "not_measured",
                    "error": "dual-run requires --discovery-manifest-json",
                }
            )
        )
        return 1
    pins = None
    if args.journal_snapshots_json is not None:
        if not (args.registry_json or args.execute):
            parser.error(
                "--journal-snapshots-json requires --registry-json or --execute"
            )
        try:
            pins = validate_journal_snapshots(
                _json_document(args.journal_snapshots_json.read_bytes())
            )
        except (ValueError, OSError) as exc:
            print(json.dumps({"status": "not_measured", "error": str(exc)}))
            return 1
    if args.history:
        from dags.utils.transfermarkt_daily_measure import format_history_summary

        try:
            report = execute_history(args.day, args.as_of)
        except Exception:
            print(
                json.dumps(
                    {
                        "report": None,
                        "status": "not_measured",
                        "summary": f"Transfermarkt история {args.day} МСК: не измерено — ошибка чтения журнала истории",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        report["report_day"] = args.day
        print(
            json.dumps(
                {"report": report, "summary": format_history_summary(report)},
                default=str,
                ensure_ascii=False,
            )
        )
        return 0
    path = args.denominator or denominator_path()
    if args.registry_json:
        try:
            registry = _json_document(args.registry_json.read_bytes())
            if args.denominator_dual_run:
                output = _dual_output(
                    registry,
                    path,
                    args.discovery_manifest_json,
                    args.registry_snapshot_id,
                    args.day,
                    args.as_of,
                    pins,
                )
            else:
                output = _single_payload(
                    args.denominator_source,
                    registry,
                    path,
                    args.registry_snapshot_id,
                    args.discovery_manifest_json,
                    args.day,
                    args.as_of,
                    pins,
                )
                output["status"] = "sql_only"
        except Exception as exc:
            print(
                json.dumps(
                    {"status": "not_measured", "error": str(exc)}, ensure_ascii=False
                )
            )
            return 1
        print(json.dumps(output, default=str, ensure_ascii=False))
        return int(output.get("status") == "blocked")
    if args.execute and args.denominator_dual_run:
        try:
            output = execute_measure(
                args.day,
                args.as_of,
                path,
                registry_snapshot_id=args.registry_snapshot_id,
                discovery_manifest_path=args.discovery_manifest_json,
                dual_run=True,
                journal_snapshots=pins,
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "report": None,
                        "status": "not_measured",
                        "error": _measure_error(exc),
                        "summary": f"Transfermarkt {args.day} МСК: не измерено — ошибка чтения измерителя; Bronze DQ unknown",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(output, default=str, ensure_ascii=False))
        return int(output.get("status") == "blocked")

    def ops_clock(value):
        stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
        return utc(stamp.replace(tzinfo=stamp.tzinfo or timezone.utc))

    start, end = day_bounds(args.day)
    dq_status = "unknown"
    if args.dq_json and args.dq_state_root:
        parser.error("choose one DQ input")
    if args.dq_json or args.dq_state_root:
        from dags.utils.transfermarkt_daily_dq import (
            VERSION as DQ_VERSION,
            read_daily_result,
        )

        dq = (
            json.loads(args.dq_json.read_text())
            if args.dq_json
            else read_daily_result(state_root=args.dq_state_root, now=start)
        )
        if (
            dq.get("day_msk") == args.day
            and dq.get("version") == DQ_VERSION
            and (dq.get("completed") is True or dq.get("status") == "RED")
            and not dq.get("stale")
            and utc(dq["cutoff_at"]) <= utc(args.as_of)
        ):
            dq_status = dq.get("status", "unknown")
            if dq_status == "RED" and dq.get("completed") is not True:
                dq_status += " (неполная проверка)"
    if args.execute:
        try:
            report = execute_measure(
                args.day,
                args.as_of,
                path,
                source=args.denominator_source,
                registry_snapshot_id=args.registry_snapshot_id,
                discovery_manifest_path=args.discovery_manifest_json,
                journal_snapshots=pins,
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "report": None,
                        "status": "not_measured",
                        "error": _measure_error(exc),
                        "summary": (
                            f"Transfermarkt {args.day} МСК: не измерено — ошибка чтения измерителя; "
                            f"Bronze DQ {dq_status}"
                        ),
                    },
                    ensure_ascii=False,
                )
            )
            return 1
    else:
        report = json.loads(args.report_json.read_text())
    if (
        report.get("measurement_version") != VERSION
        or ops_clock(report["day_start"]) != start
        or ops_clock(report["day_end"]) != end
        or ops_clock(report["as_of"]) != utc(args.as_of)
    ):
        raise MeasurementError(
            "daily SQL report version/day/as_of differs from requested evidence"
        )
    report["report_day"] = args.day
    samples = (
        import_site_sample(
            json.loads(args.samples_json.read_text()),
            evidence_dir=args.samples_json.parent,
            day=args.day,
            as_of=args.as_of,
        )
        if args.samples_json
        else None
    )
    summary = format_daily_summary(report, dq_status=dq_status, site_sample=samples)
    summary = "\n".join([summary, *policy_expiry_warnings()])
    print(
        json.dumps(
            {"report": report, "site_sample": samples, "summary": summary},
            default=str,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
