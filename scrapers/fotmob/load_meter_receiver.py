"""HTTP receiver for Trino QueryCompleted events (FotMob load meter, #2126 variant (b)).

Receives one JSON QueryCompletedEvent per POST from the coordinator's
http-event-listener, keeps queries with FotMob SQL or physical table evidence,
and appends them as load_meter snapshot records. Stdlib only; no Trino client,
no credentials, no network egress. A minute heartbeat is written by cron
(load_meter_receiver heartbeat), not by this server.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scrapers.fotmob import load_meter as meter
except ImportError:  # installed as sibling single-file copies
    import load_meter as meter


MAX_BODY_BYTES = 10 * 1024 * 1024
RECEIVED_VIA = "http_event_listener"


def _event_time(value: Any) -> str | None:
    """Normalize an event timestamp; naive text is UTC, other offsets invalid."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=meter.UTC)
        return meter.utc_text(parsed)
    except ValueError:
        return None


def _io_tables(event: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """Fully qualified physical names, with the input and output roles preserved."""
    io = event.get("ioMetadata")
    if not isinstance(io, dict):
        return [], []

    def names(entry: Any) -> list[str]:
        result = set()
        entries = entry if isinstance(entry, list) else [entry]
        for item in entries:
            if isinstance(item, dict):
                parts = (item.get("catalogName"), item.get("schema"), item.get("table", item.get("tableName")))
                if all(isinstance(part, str) and part for part in parts):
                    result.add(".".join(parts))
        return sorted(result)

    return names(io.get("inputs")), names(io.get("output"))


def keep_event(event: Mapping[str, Any]) -> bool:
    """Keep SQL candidates and queries with physical FotMob I/O."""
    metadata = event.get("metadata")
    query = metadata.get("query") if isinstance(metadata, dict) else event.get("query")
    if "fotmob_" in str(query or "").lower():
        return True
    inputs, outputs = _io_tables(event)
    return any(meter.is_fotmob_io_table(name) for name in inputs + outputs)


def event_to_snapshot(event: Mapping[str, Any], *, tables: Sequence[str], received_at: str) -> dict[str, Any] | None:
    """Map a QueryCompletedEvent to a grain=snapshot load_meter record.

    Trino 484 posts a nested event: metadata{queryId, query, queryState},
    context{user, source}, ioMetadata{inputs, output}, failureInfo, and the
    three top-level timestamps (createTime, executionStartTime, endTime).
    """
    metadata = event.get("metadata")
    context = event.get("context")
    query_id = metadata.get("queryId") if isinstance(metadata, dict) else None
    query = metadata.get("query") if isinstance(metadata, dict) else None
    state = metadata.get("queryState") if isinstance(metadata, dict) else None
    user = context.get("user") if isinstance(context, dict) else None
    source = context.get("source") if isinstance(context, dict) else None
    failure = event.get("failureInfo")
    error_code = failure.get("errorCode") if isinstance(failure, dict) else None
    if not isinstance(query_id, str) or not query_id or not meter.QUERY_ID_RE.fullmatch(query_id):
        return None
    if not isinstance(query, str) or not query:
        return None
    created = _event_time(event.get("createTime"))
    started = _event_time(event.get("executionStartTime"))
    ended = _event_time(event.get("endTime"))
    if created is None and started is None and ended is None:
        return None
    inputs, outputs = _io_tables(event)
    return {
        "grain": "snapshot", "poll_id": "recv", "query_id": query_id,
        "user": user, "source": source, "query": query,
        "state": state, "created": created, "started": started,
        "ended": ended, "stmt_type": meter.parse_sql(query, tables).stmt_type,
        "error_code": error_code, "observed_at": received_at,
        "io_tables": sorted(set(inputs + outputs)),
        "io_input_tables": sorted({name.lower() for name in inputs}),
        "io_output_tables": sorted({name.lower() for name in outputs}), "received_via": RECEIVED_VIA,
    }


def _decode(body: bytes) -> tuple[dict | None, str]:
    if len(body) > MAX_BODY_BYTES:
        return None, "too_large"
    try:
        event = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None, "malformed"
    if not isinstance(event, dict):
        return None, "malformed"
    return event, ""


def _primary(event, error, tables, received_at):
    if event is None:
        return [], error
    if not keep_event(event):
        return [], "filtered"
    record = event_to_snapshot(event, tables=tables, received_at=received_at)
    return ([record] if record is not None else []), "kept"


def handle_post_body(body: bytes, *, tables: Sequence[str], received_at: str) -> tuple[list[dict[str, Any]], str]:
    """Return kept snapshot records plus one of kept/filtered/malformed/too_large."""
    event, error = _decode(body)
    return _primary(event, error, tables, received_at)


def _clubelo_module():
    try:
        from deploy.clubelo import trino_load_meter
    except ImportError:  # standalone host installation
        import trino_load_meter
    return trino_load_meter


def append_records(root: Path, records: Sequence[Mapping[str, Any]], *, now: datetime, tag: str = "recv") -> Path:
    """Atomically append records as one raw JSONL artifact, then rotate."""
    directory = root / "raw"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%d-%H%M%S")
    suffix = ""
    while True:
        path = directory / f"{stamp}-{tag}{suffix}.jsonl"
        if not path.exists():
            break
        suffix = str(int(suffix or "0") + 1)
    content = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for record in records)
    meter._atomic_write(path, content)
    meter._rotate(root, now)
    return path


def build_heartbeat(*, root_tables: Sequence[str], poll_interval_s: int = 60,
                     window_start: str, window_end: str, status: str = "ok",
                     producer_variant: str = "event_listener_http",
                     discovered_tables: Sequence[str]) -> dict[str, Any]:
    """Minute heartbeat for the event-listener variant; windows are half-open."""
    return {
        "grain": "heartbeat",
        "poll_id": window_end[:16].replace("-", "").replace("T", "_").replace(":", ""),
        "polled_at": window_end, "window_start": window_start, "window_end": window_end,
        "poll_interval_s": poll_interval_s, "producer_variant": producer_variant,
        "watermark_ok": True, "n5_measurable": True, "status": status,
        "gap_reason": None if status == "ok" else "producer_down",
        "discovered_tables": meter.discovered_tables([*root_tables, *discovered_tables]),
    }


class EventReceiver:
    """Counter state and POST processing; the HTTP layer is a thin adapter."""

    def __init__(self, root: Path, tables_cache: Path | None = None, *, clubelo_root: Path | None = None,
                 clubelo_monitor_user: str | None = None):
        self.root = Path(root)
        self.tables_cache = Path(tables_cache) if tables_cache is not None else None
        self._lock = threading.Lock()
        self._cache_mtime: int | None = None
        self._cache_tables: list[str] = []
        self.kept_total = 0
        self.filtered_total = 0
        self.malformed_total = 0
        self.last_event_at: str | None = None
        self.files: list[str] = []
        self.clubelo_enabled = clubelo_root is not None
        self._clubelo_lock = threading.Lock()
        self._clubelo_pending = Counter()
        self._clubelo = None
        if self.clubelo_enabled:
            try:
                self._clubelo = _clubelo_module().Sink(clubelo_root, clubelo_monitor_user)
            except Exception as exc:
                print(f"clubelo_meter: {type(exc).__name__}", file=sys.stderr)

    def tables(self) -> list[str]:
        if self.tables_cache is None:
            return []
        try:
            mtime = self.tables_cache.stat().st_mtime_ns
            if mtime != self._cache_mtime:
                payload = json.loads(self.tables_cache.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("expected a JSON object")
                self._cache_tables = [str(name) for name in payload.get("discovered_tables", [])]
                self._cache_mtime = mtime
        except (OSError, ValueError, AttributeError, TypeError):
            self._cache_mtime = None  # retry on the next POST; keep the last good list
        return list(self._cache_tables)

    def _primary_post(self, body, received_at):
        event = None
        with self._lock:
            tables = self.tables()
        if self._clubelo is None:
            records, outcome = handle_post_body(body, tables=tables, received_at=received_at)
        else:
            event, error = _decode(body)
            records, outcome = _primary(event, error, tables, received_at)
        with self._lock:
            if outcome == "kept":
                self.kept_total += 1
                self.last_event_at = received_at
                if records:
                    path = append_records(self.root, records, now=datetime.now(meter.UTC))
                    self.files.append(path.name)
            elif outcome == "filtered":
                self.filtered_total += 1
            else:
                self.malformed_total += 1
        return outcome, event

    @contextmanager
    def _delivery(self, body, truncated=False):
        received = datetime.now(meter.UTC)
        received_at = meter.utc_text(received)
        sink = self._clubelo
        if sink is None:
            outcome, _ = self._primary_post(body, received_at)
            yield outcome, 201
            return
        minute = int(received.replace(second=0, microsecond=0).timestamp())
        with self._clubelo_lock:
            self._clubelo_pending[minute] += 1
        try:
            outcome, event = self._primary_post(body, received_at)
            try:
                yield outcome, 201  # HTTP acknowledges the persisted primary result here.
            finally:
                if truncated:
                    self._clubelo_gap(received_at)
                else:
                    try:
                        sink.capture(event, received.isoformat())
                    except Exception:
                        self._clubelo_gap(received_at)
        finally:
            with self._clubelo_lock:
                self._clubelo_pending[minute] -= 1
                if not self._clubelo_pending[minute]:
                    del self._clubelo_pending[minute]

    def post(self, body: bytes) -> tuple[str, int]:
        with self._delivery(body) as result:
            return result

    def _clubelo_gap(self, received_at):
        if self._clubelo is not None:
            try:
                self._clubelo.gap(received_at)
            except Exception:
                self._clubelo.disable()

    def dropped(self):
        self._clubelo_gap(meter.utc_text(datetime.now(meter.UTC)))

    def clubelo_health(self):
        with self._clubelo_lock:
            if self._clubelo is None:
                return {"enabled": self.clubelo_enabled, "ready": False}
            health = self._clubelo.health()
            # A request in the new minute can overlap the heartbeat for the
            # previous one. Withhold only minutes still being processed.
            health["event_minutes"] = [m for m in health["event_minutes"] if m not in self._clubelo_pending]
            return health

    def health(self) -> dict[str, Any]:
        with self._lock:
            return {"status": "ok", "kept_total": self.kept_total, "filtered_total": self.filtered_total,
                    "malformed_total": self.malformed_total, "last_event_at": self.last_event_at,
                    "files": list(self.files)}


class _PostHandler(BaseHTTPRequestHandler):
    receiver: EventReceiver

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        shared = getattr(self.receiver, "clubelo_enabled", False)
        with ExitStack() as delivery:
            try:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(min(length, MAX_BODY_BYTES + 1)) if length > 0 else b""
                if shared:
                    truncated = len(body) < min(length, MAX_BODY_BYTES + 1)
                    _, code = delivery.enter_context(self.receiver._delivery(body, truncated))
                else:
                    _, code = self.receiver.post(body)
            except Exception:
                if shared:
                    delivery.callback(self.receiver.dropped)
                code = 500
            self.send_response(code)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def do_GET(self) -> None:
        if self.path == "/clubelo-healthz" and getattr(self.receiver, "clubelo_enabled", False):
            health = self.receiver.clubelo_health()
        elif self.path == "/healthz":
            health = self.receiver.health()
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        payload = json.dumps(health, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _serve(receiver: EventReceiver, host: str, port: int) -> None:
    handler = type("ReceiverHandler", (_PostHandler,), {"receiver": receiver})
    server = ThreadingHTTPServer((host, port), handler)
    if receiver._clubelo is not None:
        receiver._clubelo.activate()
    print(f"fotmob_load_meter_receiver: {host}:{port} root={receiver.root}", file=sys.stderr)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--root", type=Path, default=meter.DEFAULT_ROOT)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=4849)
    serve.add_argument("--tables-cache", type=Path)
    serve.add_argument("--clubelo-root", type=Path)
    serve.add_argument("--clubelo-monitor-user", help="verified dedicated read-only monitor principal")
    beat = commands.add_parser("heartbeat")
    beat.add_argument("--root", type=Path, default=meter.DEFAULT_ROOT)
    beat.add_argument("--discovered", type=Path, required=True)
    beat.add_argument("--status", choices=("ok", "down"), default="ok")
    beat.add_argument("--window-end")
    beat.add_argument("--clubelo-root", type=Path)
    beat.add_argument("--clubelo-health-url", default="http://127.0.0.1:4849/clubelo-healthz")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "serve":
            _serve(EventReceiver(args.root, args.tables_cache, clubelo_root=args.clubelo_root,
                                 clubelo_monitor_user=args.clubelo_monitor_user), args.host, args.port)
            return 0
        end = meter.utc_datetime(args.window_end) if args.window_end else datetime.now(meter.UTC).replace(microsecond=0)
        payload = json.loads(args.discovered.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("expected a JSON object")
        beat = build_heartbeat(root_tables=[], window_start=meter.utc_text(end - timedelta(seconds=60)),
                               window_end=meter.utc_text(end), status=args.status,
                               discovered_tables=payload.get("discovered_tables", []))
        path = append_records(args.root, [beat], now=end, tag="hb")
        print(meter.json_text({"raw_file": str(path), "status": beat["status"]}), end="")
        if args.clubelo_root is not None:
            try:
                _clubelo_module().heartbeat(args.clubelo_root, end, args.status, args.clubelo_health_url)
            except Exception as exc:
                print(f"clubelo_heartbeat: {type(exc).__name__}", file=sys.stderr)
        return 0
    except Exception as exc:
        # Raw exception text can contain HTTP headers, credentials or SQL.
        print(f"fotmob_load_meter_receiver: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
