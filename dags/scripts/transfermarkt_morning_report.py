"""Read-only morning summary of a scheduled Transfermarkt current run."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.append(str(ROOT / "dags"))

from dags.utils import transfermarkt_daily_dq as daily_dq  # noqa: E402 (script entry point)
from dags.utils.transfermarkt_daily_measure import utc as aware_timestamp  # noqa: E402

MOSCOW = ZoneInfo("Europe/Moscow")
CURRENT_ROOT = Path("/opt/airflow/logs/transfermarkt-native-v2/current")
SHA256 = re.compile(r"[0-9a-fA-F]{64}")


class Unavailable(ValueError):
    """The selected run or measurement cannot be verified."""


def report_clock(day: date | None, as_of: datetime | None) -> tuple[date, datetime]:
    instant = as_of if as_of is not None else datetime.now(timezone.utc)
    if instant.utcoffset() is None:
        raise ValueError("timezone required")
    if day is None:
        day = instant.astimezone(MOSCOW).date() - timedelta(days=1)
    return day, instant.astimezone(timezone.utc)


@dataclass(frozen=True)
class Pin:
    cycle_id: str
    finished_at: datetime
    denominator_source: str
    universe_version: str
    denominator_hash: str
    registry_snapshot_id: str


def portion_pin(portion: dict, cycle_id: str, finished_at: datetime) -> Pin:
    source = portion.get("denominator_source")
    version = portion.get("universe_version")
    digest = portion.get("denominator_hash")
    targets = portion.get("current_targets")
    if not isinstance(digest, str) or not SHA256.fullmatch(digest):
        raise Unavailable("denominator_hash")
    if not isinstance(targets, list):
        raise Unavailable("current_targets")
    snapshots = set()
    for target in targets:
        if not isinstance(target, dict) or type(target.get("queue_rank")) is not int:
            raise Unavailable("queue_rank")
        if target["queue_rank"] == 0:
            snapshot = target.get("registry_snapshot_id")
            if (
                not isinstance(snapshot, str)
                or not snapshot.strip()
                or snapshot != snapshot.strip()
            ):
                raise Unavailable("registry_snapshot_id")
            snapshots.add(snapshot)
    if len(snapshots) != 1:
        raise Unavailable("registry_snapshot_id")
    snapshot = snapshots.pop()
    if source == "tsv":
        if not isinstance(version, str) or not SHA256.fullmatch(version):
            raise Unavailable("universe_version")
    elif source != "registry" or version != snapshot:
        raise Unavailable("universe_version")
    return Pin(cycle_id, finished_at, source, version, digest, snapshot)


def latest_pin(root: Path, day: date, as_of: datetime) -> Pin:
    start = datetime.combine(day, time.min, MOSCOW)
    # The writer sets finished_at before writing the immutable receipt. Its mtime
    # bounds finish from above. Replay only receipts that were available by as_of.
    publications = sorted(
        ((path.stat().st_mtime_ns, path) for path in root.glob("portion-*.json")),
        reverse=True,
    )
    latest: datetime | None = None
    pin: Pin | None = None
    lower_bound_ns = int(start.timestamp()) * 1_000_000_000
    upper_bound_ns = (
        int(as_of.replace(microsecond=0).timestamp()) * 1_000_000_000
        + as_of.microsecond * 1000
    )
    for mtime_ns, path in publications:
        if mtime_ns > upper_bound_ns:
            continue
        if mtime_ns < lower_bound_ns:
            break
        with path.open(encoding="utf-8") as stream:
            portion = json.load(stream)
        if not isinstance(portion, dict):
            raise Unavailable("portion")
        cycle = portion.get("cycle_id")
        if isinstance(cycle, str) and cycle.startswith("manual__"):
            continue
        finished = aware_timestamp(portion.get("finished_at"))
        if finished > as_of or finished.astimezone(MOSCOW).date() != day:
            continue
        if latest is not None and finished < latest:
            continue
        try:
            if not isinstance(cycle, str) or not cycle.startswith("scheduled__"):
                raise Unavailable("cycle_id")
            if aware_timestamp(cycle[len("scheduled__") :]) > finished:
                raise Unavailable("cycle_id")
            key = hashlib.sha256(cycle.encode()).hexdigest()[:24]
            if path.name != f"portion-{key}.json":
                raise Unavailable("cycle filename")
            candidate = portion_pin(portion, cycle, finished)
        except ValueError:
            candidate = None
        if finished == latest:
            # All tied receipts must be valid. Break valid ties by cycle ID.
            if candidate is None or pin is None:
                pin = None
            elif candidate.cycle_id > pin.cycle_id:
                pin = candidate
        else:
            pin = candidate
        latest = finished
        seconds = int(finished.replace(microsecond=0).timestamp())
        lower_bound_ns = seconds * 1_000_000_000 + finished.microsecond * 1000
    if pin is None:
        raise Unavailable("scheduled portion")
    return pin


def report_dq(day: date, as_of: datetime, root: Path) -> dict:
    """Freeze one DQ receipt for both the canonical CLI and the host diagnostics."""
    try:
        dq = daily_dq.read_daily_result(
            state_root=root, now=datetime.combine(day, time.min, MOSCOW)
        )
        if (
            isinstance(dq, dict)
            and dq.get("day_msk") == day.isoformat()
            and dq.get("version") == daily_dq.VERSION
            and not dq.get("stale")
            and aware_timestamp(dq.get("cutoff_at")) <= as_of
        ):
            return {
                key: dq.get(key)
                for key in (
                    "version",
                    "day_msk",
                    "cutoff_at",
                    "completed",
                    "status",
                    "errors",
                )
            }
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
        pass
    return {"day_msk": day.isoformat(), "stale": True, "status": "UNKNOWN"}


def dq_errors(dq: dict) -> str:
    errors = dq.get("errors")
    if not isinstance(errors, list) or not errors:
        return ""
    names = sorted(
        {
            name.partition("[")[0].removeprefix("tm_")
            for name in errors
            if isinstance(name, str)
        }
    )
    labels = ", ".join(names[:5]) + (", …" if len(names) > 5 else "")
    return f", ошибок {len(errors)} ({labels})"


def morning_report(
    day: date,
    as_of: datetime,
    current_root: Path,
    dq_root: Path,
    denominator: Path | None = None,
    samples_json: Path | None = None,
) -> dict:
    provenance = {"day": day.isoformat(), "as_of": as_of.isoformat()}
    dq = report_dq(day, as_of, dq_root)
    errors = dq_errors(dq)
    source = "неизвестен"
    reason = "нет подтверждённого происхождения порции"
    try:
        pin = latest_pin(current_root, day, as_of)
        provenance.update(asdict(pin), finished_at=pin.finished_at.isoformat())
        source = pin.denominator_source
        reason = "ошибка чтения измерителя"
        argv = [
            sys.executable,
            "-B",
            str(Path(__file__).with_name("transfermarkt_daily_report.py")),
            "--execute",
            "--day",
            day.isoformat(),
            "--as-of",
            as_of.isoformat(),
            "--denominator-source",
            source,
            "--registry-snapshot-id",
            pin.registry_snapshot_id,
            "--dq-json",
            "/dev/stdin",
        ]
        if source == "tsv" and denominator is not None:
            argv.extend(["--denominator", str(denominator)])
        if samples_json is not None:
            argv.extend(["--samples-json", str(samples_json)])
        result = subprocess.run(
            argv,
            input=json.dumps(dq).encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=140,
        )
        if result.returncode != 0:
            raise Unavailable("execution")
        payload = json.loads(result.stdout)
        report = payload.get("report")
        if not isinstance(report, dict):
            raise Unavailable("report")
        reason = "знаменатель измерения расходится с порцией"
        if (
            report.get("denominator_version") != pin.universe_version
            or report.get("denominator_hash") != pin.denominator_hash
        ):
            raise Unavailable("denominator")
        reason = "некорректный ответ измерителя"
        summary = payload.get("summary")
        if (
            report.get("report_day") != day.isoformat()
            or not isinstance(summary, str)
            or "site_sample" not in payload
            or (
                payload["site_sample"] is not None
                and not isinstance(payload["site_sample"], dict)
            )
        ):
            raise Unavailable("summary")
        first, newline, warnings = summary.partition("\n")
        if not first.strip():
            raise Unavailable("summary")
        replay = (
            f"as_of={as_of.isoformat()}; знаменатель {source}; версия={pin.universe_version}; "
            f"хеш={pin.denominator_hash}; снимок={pin.registry_snapshot_id}"
        )
        return {
            "summary": f"{first}{errors}; {replay}{newline}{warnings}",
            "report": report,
            "site_sample": payload["site_sample"],
            "provenance": provenance,
        }
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        OverflowError,
        subprocess.SubprocessError,
    ):
        status = dq.get("status", "UNKNOWN")
        if dq.get("stale") or (dq.get("completed") is not True and status != "RED"):
            status = "unknown"
        elif status == "RED" and dq.get("completed") is not True:
            status = "RED (неполная проверка)"
        return {
            "summary": (
                f"Transfermarkt {day} МСК: не измерено — {reason}; "
                f"as_of={as_of.isoformat()}; знаменатель {source}; Bronze DQ {status}{errors}"
            ),
            "status": "not_measured",
            "report": None,
            "provenance": provenance,
        }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat)
    parser.add_argument("--as-of", type=aware_timestamp)
    parser.add_argument("--current-root", type=Path, default=CURRENT_ROOT)
    parser.add_argument(
        "--dq-state-root", type=Path, default=daily_dq.DEFAULT_STATE_ROOT
    )
    parser.add_argument("--denominator", type=Path)
    parser.add_argument("--samples-json", type=Path)
    parser.add_argument(
        "--refusals-state",
        type=Path,
        help="gateway refusal counters; adds the day's refusals per 100 requests",
    )
    args = parser.parse_args(argv)
    day, as_of = report_clock(args.day, args.as_of)
    output = morning_report(
        day,
        as_of,
        args.current_root,
        args.dq_state_root,
        args.denominator,
        args.samples_json,
    )
    if args.refusals_state is not None:
        from scrapers.transfermarkt.streams import refusal_rate

        # None for a day the gateway counted no requests: no rate is invented.
        counted = refusal_rate(args.refusals_state, day.isoformat()) or {}
        output["source_refusals"] = {
            "day": day.isoformat(),
            "requests": counted.get("requests"),
            "refusals": counted.get("refusals"),
            "per_100": counted.get("per_100"),
        }
    print(json.dumps(output, ensure_ascii=False))
    return int(output["report"] is None)


if __name__ == "__main__":
    raise SystemExit(main())
