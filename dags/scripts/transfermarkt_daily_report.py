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


def _manifest_metadata(path):
    from dags.utils.transfermarkt_daily_measure import MeasurementError

    if path is None:
        raise MeasurementError("dual-run requires discovery manifest metadata")
    document = json.loads(Path(path).read_text())
    manifest = document.get("manifest", document)
    report = manifest.get("discovery_report") or {}
    reasons = report.get("discovery_quarantined") or {}
    quarantined = {
        str(competition_id): str(
            reasons.get(competition_id)
            or "registry quarantine without detailed discovery reason"
        )
        for competition_id in manifest.get("quarantined_competition_ids", ())
    }
    title_only = report.get("title_only_competition_ids", ())
    return quarantined, title_only


def _selected_denominator(source, registry, path, *, metadata=None):
    from dags.utils.transfermarkt_daily_measure import daily_denominator
    from dags.utils.transfermarkt_denominator_registry import build_registry_denominator
    from scrapers.transfermarkt.denominator import load_denominator

    if source == "registry":
        quarantined, title_only = metadata or ({}, ())
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
    legacy = load_denominator(path)
    version = hashlib.sha256(path.read_bytes()).hexdigest()
    return daily_denominator(
        legacy.rows.values(), registry, denominator_version=version
    ), legacy


def _dual_denominators(registry, path, manifest_path):
    from dags.utils.transfermarkt_denominator_registry import compare_denominators

    metadata = _manifest_metadata(manifest_path)
    legacy, legacy_definition = _selected_denominator(
        "tsv", registry, path, metadata=metadata
    )
    current, current_definition = _selected_denominator(
        "registry", registry, path, metadata=metadata
    )
    version = hashlib.sha256(path.read_bytes()).hexdigest()
    diff = compare_denominators(
        legacy_definition, current_definition, legacy_version=version
    )
    return legacy, current, diff


def _measure_payload(denominator, day, as_of):
    from dags.utils.transfermarkt_daily_measure import build_daily_measure_sql

    sql, params = build_daily_measure_sql(denominator, day, as_of=as_of)
    return {
        "sql": sql,
        "params": params,
        "report_day": day,
        "denominator_hash": denominator.denominator_hash,
        "denominator_version": denominator.denominator_version,
        "blocked_competitions": dict(denominator.blocked_competitions),
        "current_editions": denominator.rows,
        "denominator_definition_hash": denominator.denominator_definition_hash,
    }


def execute_measure(
    day,
    as_of,
    path,
    *,
    source=None,
    registry_snapshot_id=None,
    discovery_manifest_path=None,
    dual_run=False,
):
    from dags.utils.transfermarkt_daily_measure import (
        MeasurementError,
        build_daily_measure_sql,
    )
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_denominator_registry import denominator_source
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
                legacy, current, diff = _dual_denominators(
                    registry, path, discovery_manifest_path
                )
                result = {}
                for name, denominator in (("legacy", legacy), ("registry", current)):
                    payload = _measure_payload(denominator, day, as_of)
                    cur.execute(payload["sql"], payload["params"])
                    rows = _read(cur)
                    if len(rows) != 1:
                        raise MeasurementError(
                            "daily measure must return exactly one row"
                        )
                    payload["report"] = rows[0]
                    result[name] = payload
                result["diff"] = diff
                return result
            denominator, _ = _selected_denominator(
                denominator_source(source),
                registry,
                path,
            )
            sql, params = build_daily_measure_sql(denominator, day, as_of=as_of)
            cur.execute(sql, params)
            rows = _read(cur)
            if len(rows) != 1:
                raise MeasurementError("daily measure must return exactly one row")
            return rows[0]
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
    )
    from dags.utils.transfermarkt_denominator_registry import denominator_source
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
    if args.denominator_dual_run and not args.discovery_manifest_json:
        parser.error("dual-run requires --discovery-manifest-json")
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
        registry = json.loads(args.registry_json.read_text())
        if args.denominator_dual_run:
            legacy, current, diff = _dual_denominators(
                registry, path, args.discovery_manifest_json
            )
            output = {
                "legacy": _measure_payload(legacy, args.day, args.as_of),
                "registry": _measure_payload(current, args.day, args.as_of),
                "diff": diff,
            }
        else:
            denominator, _ = _selected_denominator(
                denominator_source(args.denominator_source),
                registry,
                path,
            )
            output = _measure_payload(denominator, args.day, args.as_of)
        print(json.dumps(output, default=str, ensure_ascii=False))
        return 0
    if args.execute and args.denominator_dual_run:
        try:
            output = execute_measure(
                args.day,
                args.as_of,
                path,
                registry_snapshot_id=args.registry_snapshot_id,
                discovery_manifest_path=args.discovery_manifest_json,
                dual_run=True,
            )
        except Exception:
            print(
                json.dumps(
                    {
                        "report": None,
                        "status": "not_measured",
                        "summary": f"Transfermarkt {args.day} МСК: не измерено — ошибка чтения измерителя; Bronze DQ unknown",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(output, default=str, ensure_ascii=False))
        return 0

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
            )
        except Exception:
            print(
                json.dumps(
                    {
                        "report": None,
                        "status": "not_measured",
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
