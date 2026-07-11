"""No-write live benchmark for a fixed FotMob sentinel matrix.

The harness intentionally does not import the production FotMob scraper.  It
can therefore capture a reproducible baseline before a refactor, even when the
production module is temporarily broken.  It writes only benchmark artifacts
under ``--artifact-dir`` (``/tmp/fotmob-benchmark/<label>`` by default); it
never opens Trino, S3, Airflow, or any medallion table.

Response bodies are read before urllib3 content decoding so both encoded
(on-the-wire HTTP body) and decoded byte counts are available.  Header, request
and TLS overhead is deliberately excluded and this limitation is recorded in
the report.

Example::

    python scripts/research/bench_fotmob_fetch.py --label baseline
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import sys
import threading
import time
import zlib
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


API_URL = "https://www.fotmob.com/api/data/leagues"
RETRYABLE_STATUSES = frozenset({408, 425, 429, *range(500, 600)})
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class Sentinel:
    """One immutable competition/season benchmark target."""

    key: str
    name: str
    competition_id: int
    source_season_key: str

    @property
    def params(self) -> dict[str, str]:
        return {
            "id": str(self.competition_id),
            "season": self.source_season_key,
        }


# Exact API season keys use slashes.  The display names in the audit may use
# hyphens, but these strings must never be derived from an integer year.
SENTINELS: tuple[Sentinel, ...] = (
    Sentinel("epl", "Premier League", 47, "2025/2026"),
    Sentinel("ucl", "Champions League", 42, "2025/2026"),
    Sentinel(
        "ucl_qualification",
        "Champions League Qualification",
        10611,
        "2025/2026",
    ),
    Sentinel("nations_a", "Nations League A", 9806, "2024/2025"),
    Sentinel("rpl", "Russian Premier League", 63, "2025/2026"),
    Sentinel("afcon", "Africa Cup of Nations", 289, "2025"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json_bytes(value: Any) -> bytes:
    """Render stable JSON for hashes and deterministic comparisons."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _walk_json(value: Any, path: str = "$") -> Iterable[tuple[str, Any]]:
    yield path, value
    if isinstance(value, Mapping):
        for key, child in value.items():
            rendered = str(key).replace("~", "~0").replace("/", "~1")
            yield from _walk_json(child, f"{path}/{rendered}")
    elif isinstance(value, list):
        for child in value:
            # Array indexes are intentionally collapsed.  The resulting field
            # inventory detects schema drift without exploding by row count.
            yield from _walk_json(child, f"{path}/[]")


def json_schema_paths(value: Any) -> list[str]:
    return sorted({path for path, _ in _walk_json(value)})


def _stable_identity(value: Any) -> str:
    if isinstance(value, Mapping) and value.get("id") is not None:
        return f"id:{value['id']}"
    return f"sha256:{sha256_bytes(canonical_json_bytes(value))}"


def summarize_payload(payload: Any) -> dict[str, int]:
    """Best-effort, source-shaped counts that work without refactored parsers.

    Payload hashes remain the authoritative equality signal.  These counts are
    diagnostic and deliberately cover only structures FotMob advertises in a
    league response; unknown fields still appear in ``json_schema_paths``.
    """

    matches: list[Any] = []
    standings: list[Any] = []
    stat_preview_rows: list[Any] = []
    tables = 0
    playoff_rows = 0

    for path, value in _walk_json(payload):
        final = path.rsplit("/", 1)[-1]
        if final == "allMatches" and isinstance(value, list):
            matches.extend(value)
        if final == "all" and "/table/" in path and isinstance(value, list):
            standings.extend(value)
        if final == "StatList" and isinstance(value, list):
            stat_preview_rows.extend(value)
        if final == "tables" and isinstance(value, list):
            tables += len(value)
        if (
            final in {"matches", "allMatches"}
            and "playoff" in path.lower()
            and isinstance(value, list)
        ):
            playoff_rows += len(value)

    unique_matches = {_stable_identity(row) for row in matches}
    unique_standing_rows = {_stable_identity(row) for row in standings}

    stats = payload.get("stats") if isinstance(payload, Mapping) else None
    stats = stats if isinstance(stats, Mapping) else {}
    player_categories = stats.get("players")
    team_categories = stats.get("teams")

    seasons: list[Any] = []
    if isinstance(payload, Mapping):
        candidate = payload.get("allAvailableSeasons")
        if isinstance(candidate, list):
            seasons = candidate
        else:
            details = payload.get("details")
            if isinstance(details, Mapping):
                candidate = details.get("allAvailableSeasons")
                if isinstance(candidate, list):
                    seasons = candidate

    node_count = sum(1 for _ in _walk_json(payload))
    return {
        "matches_rows": len(matches),
        "matches_unique": len(unique_matches),
        "standings_rows": len(standings),
        "standings_unique_rows": len(unique_standing_rows),
        "standings_tables": tables,
        "playoff_match_rows": playoff_rows,
        "player_categories": (
            len(player_categories) if isinstance(player_categories, list) else 0
        ),
        "team_categories": (
            len(team_categories) if isinstance(team_categories, list) else 0
        ),
        "stat_preview_rows": len(stat_preview_rows),
        "available_seasons": len(seasons),
        "json_nodes": node_count,
    }


def selected_season(payload: Any) -> Optional[str]:
    if not isinstance(payload, Mapping):
        return None
    details = payload.get("details")
    if isinstance(details, Mapping) and details.get("selectedSeason") is not None:
        return str(details["selectedSeason"])
    if payload.get("selectedSeason") is not None:
        return str(payload["selectedSeason"])
    return None


def observed_competition_id(payload: Any) -> Optional[int]:
    if not isinstance(payload, Mapping):
        return None
    details = payload.get("details")
    candidates: list[Any] = []
    if isinstance(details, Mapping):
        candidates.extend(
            details.get(key) for key in ("id", "leagueId", "competitionId")
        )
    candidates.extend(payload.get(key) for key in ("id", "leagueId"))
    for candidate in candidates:
        try:
            if candidate is not None:
                return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def decode_http_body(body: bytes, content_encoding: Optional[str]) -> bytes:
    """Decode an exact HTTP body captured with ``decode_content=False``."""

    encodings = [
        item.strip().lower()
        for item in (content_encoding or "").split(",")
        if item.strip()
    ]
    decoded = body
    # Content-Encoding values are applied in order and decoded in reverse.
    for encoding in reversed(encodings):
        if encoding in {"identity", ""}:
            continue
        if encoding in {"gzip", "x-gzip"}:
            decoded = gzip.decompress(decoded)
        elif encoding == "deflate":
            try:
                decoded = zlib.decompress(decoded)
            except zlib.error:
                decoded = zlib.decompress(decoded, -zlib.MAX_WBITS)
        else:
            raise ValueError(f"unsupported Content-Encoding: {encoding}")
    return decoded


def read_encoded_response(response: Any) -> bytes:
    """Read a streamed response without transparent decompression.

    The fallback to ``response.content`` keeps this helper friendly to simple
    fake responses and old requests adapters used in unit tests.
    """

    raw = getattr(response, "raw", None)
    if raw is not None and hasattr(raw, "read"):
        try:
            return raw.read(decode_content=False)
        except TypeError:
            return raw.read()
    return bytes(getattr(response, "content", b"") or b"")


def parse_retry_after(
    value: Optional[str],
    *,
    now: Optional[datetime] = None,
) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (parsed - current).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


class RateLimiter:
    """Small thread-safe monotonic limiter shared by all benchmark targets."""

    def __init__(self, requests_per_minute: float):
        self._interval = 0.0
        if requests_per_minute > 0:
            self._interval = 60.0 / requests_per_minute
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            if delay:
                time.sleep(delay)
                now = time.monotonic()
            self._next = max(now, self._next) + self._interval


@dataclass
class FetchCapture:
    record: dict[str, Any]
    payload: Any = None
    decoded_body: Optional[bytes] = None


def make_session(proxy_url: Optional[str] = None) -> requests.Session:
    session = requests.Session()
    # Environment proxies would make a supposedly direct benchmark depend on
    # ambient container state.  Proxy use must always be explicit.
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fotmob.com/",
            "Origin": "https://www.fotmob.com",
        }
    )
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
    return session


def _attempt_delay(
    attempt_number: int,
    retry_after: Optional[float],
    max_retry_wait: float,
) -> float:
    base = retry_after if retry_after is not None else 2 ** (attempt_number - 1)
    base = min(max_retry_wait, max(0.0, base))
    if base == 0:
        return 0.0
    return min(max_retry_wait, base + random.uniform(0.0, min(1.0, base * 0.25)))


def fetch_scope(
    scope: Sentinel,
    *,
    session: Any,
    limiter: Optional[RateLimiter] = None,
    api_url: str = API_URL,
    timeout: float = 30.0,
    max_attempts: int = 4,
    max_retry_wait: float = 60.0,
    proxy_enabled: bool = False,
    strict_selected_season: bool = True,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchCapture:
    """Fetch and validate one target without touching production storage."""

    attempts: list[dict[str, Any]] = []
    started = time.perf_counter()
    payload: Any = None
    decoded_body: Optional[bytes] = None
    final_status: Optional[int] = None
    final_url = api_url
    final_headers: dict[str, Any] = {}
    error: Optional[str] = None

    for attempt_number in range(1, max(1, max_attempts) + 1):
        if limiter is not None:
            limiter.acquire()
        attempt_started = time.perf_counter()
        status: Optional[int] = None
        encoded = b""
        decoded = b""
        attempt_error: Optional[str] = None
        retry_after: Optional[float] = None
        response: Any = None
        try:
            response = session.get(
                api_url,
                params=scope.params,
                timeout=timeout,
                stream=True,
            )
            status = int(response.status_code)
            final_status = status
            final_url = str(getattr(response, "url", api_url))
            final_headers = {
                "content_type": response.headers.get("Content-Type"),
                "content_encoding": response.headers.get("Content-Encoding"),
                "content_length": response.headers.get("Content-Length"),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
            }
            encoded = read_encoded_response(response)
            decoded = decode_http_body(
                encoded,
                response.headers.get("Content-Encoding"),
            )
            retry_after = parse_retry_after(response.headers.get("Retry-After"))

            if status == 200:
                try:
                    payload = json.loads(decoded.decode("utf-8-sig"))
                    decoded_body = decoded
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    attempt_error = f"invalid_json: {type(exc).__name__}: {exc}"
                # JSON/decode failures are intentionally terminal.  The live
                # policy retries only retryable HTTP statuses.
            elif status not in RETRYABLE_STATUSES:
                attempt_error = f"terminal_http_status:{status}"
        except requests.RequestException as exc:
            attempt_error = f"request_error: {type(exc).__name__}: {exc}"
        except (OSError, ValueError) as exc:
            attempt_error = f"body_error: {type(exc).__name__}: {exc}"
        finally:
            if response is not None:
                response.close()

        elapsed = time.perf_counter() - attempt_started
        attempts.append(
            {
                "attempt": attempt_number,
                "status": status,
                "seconds": round(elapsed, 6),
                "encoded_bytes": len(encoded),
                "decoded_bytes": len(decoded),
                "transport": "proxy" if proxy_enabled else "direct",
                "retry_after_seconds": retry_after,
                "error": attempt_error,
            }
        )

        if status == 200:
            error = attempt_error
            break
        retryable_attempt = status in RETRYABLE_STATUSES or (
            status is None
            and attempt_error is not None
            and attempt_error.startswith("request_error:")
        )
        if not retryable_attempt or attempt_number >= max_attempts:
            error = attempt_error or (
                f"request_error_after_{attempt_number}_attempts"
                if status is None
                else f"http_status:{status}"
            )
            break

        delay = _attempt_delay(attempt_number, retry_after, max_retry_wait)
        attempts[-1]["retry_sleep_seconds"] = round(delay, 6)
        sleep(delay)

    errors: list[str] = []
    warnings: list[str] = []
    if error:
        errors.append(error)

    observed_season = selected_season(payload)
    observed_id = observed_competition_id(payload)
    if payload is not None:
        if observed_season is None:
            message = "details.selectedSeason is missing"
            (errors if strict_selected_season else warnings).append(message)
        elif observed_season != scope.source_season_key:
            errors.append(
                "selected season mismatch: "
                f"requested={scope.source_season_key!r}, observed={observed_season!r}"
            )
        if observed_id is not None and observed_id != scope.competition_id:
            errors.append(
                "competition id mismatch: "
                f"requested={scope.competition_id}, observed={observed_id}"
            )

    encoded_total = sum(int(item["encoded_bytes"]) for item in attempts)
    decoded_total = sum(int(item["decoded_bytes"]) for item in attempts)
    paths = json_schema_paths(payload) if payload is not None else []
    payload_hash = (
        sha256_bytes(canonical_json_bytes(payload)) if payload is not None else None
    )
    decoded_hash = sha256_bytes(decoded_body) if decoded_body is not None else None
    record: dict[str, Any] = {
        **asdict(scope),
        "request_params": scope.params,
        "url": final_url,
        "ok": payload is not None and not errors,
        "http_status": final_status,
        "attempt_count": len(attempts),
        "retry_count": max(0, len(attempts) - 1),
        "seconds": round(time.perf_counter() - started, 6),
        "attempts": attempts,
        "status_counts": dict(
            Counter(
                str(item["status"]) if item["status"] is not None else "exception"
                for item in attempts
            )
        ),
        "encoded_direct_bytes": 0 if proxy_enabled else encoded_total,
        "decoded_direct_bytes": 0 if proxy_enabled else decoded_total,
        "encoded_proxy_bytes": encoded_total if proxy_enabled else 0,
        "decoded_proxy_bytes": decoded_total if proxy_enabled else 0,
        "response_headers": final_headers,
        "selected_season_observed": observed_season,
        "competition_id_observed": observed_id,
        "payload_sha256": payload_hash,
        "decoded_body_sha256": decoded_hash,
        "row_counts": summarize_payload(payload) if payload is not None else {},
        "field_path_count": len(paths),
        "field_paths": paths,
        "validation_errors": errors,
        "validation_warnings": warnings,
        "artifact_path": None,
        "raw_cache_hit": False,
    }
    return FetchCapture(record=record, payload=payload, decoded_body=decoded_body)


def _counter_delta(
    after: Mapping[str, int], before: Mapping[str, int]
) -> dict[str, int]:
    keys = set(after) | set(before)
    return {
        str(key): int(after.get(key, 0)) - int(before.get(key, 0))
        for key in keys
        if int(after.get(key, 0)) - int(before.get(key, 0))
    }


def fetch_scope_canonical(
    scope: Sentinel,
    *,
    transport: Any,
    strict_selected_season: bool = True,
) -> FetchCapture:
    """Use the refactored transport through its small public result contract.

    Imports and construction deliberately happen outside this function, so a
    baseline checkout without ``scrapers.fotmob.transport`` still imports and
    runs the standalone harness.
    """

    before = transport.snapshot_stats()
    started = time.perf_counter()
    result = transport.fetch_json(
        "leagues",
        scope.params,
        allow_stale_on_error=False,
    )
    seconds = time.perf_counter() - started
    after = transport.snapshot_stats()
    before_statuses = dict(getattr(before, "status_counts", {}) or {})
    after_statuses = dict(getattr(after, "status_counts", {}) or {})
    statuses = _counter_delta(after_statuses, before_statuses)

    payload = getattr(result, "json_data", None)
    body = getattr(result, "body", None)
    if body is not None and not isinstance(body, bytes):
        body = bytes(body)
    errors: list[str] = []
    warnings: list[str] = []
    if not bool(getattr(result, "ok", False)):
        errors.append(str(getattr(result, "error", None) or "transport fetch failed"))

    observed_season = selected_season(payload)
    observed_id = observed_competition_id(payload)
    if payload is not None:
        if observed_season is None:
            message = "details.selectedSeason is missing"
            (errors if strict_selected_season else warnings).append(message)
        elif observed_season != scope.source_season_key:
            errors.append(
                "selected season mismatch: "
                f"requested={scope.source_season_key!r}, observed={observed_season!r}"
            )
        if observed_id is not None and observed_id != scope.competition_id:
            errors.append(
                "competition id mismatch: "
                f"requested={scope.competition_id}, observed={observed_id}"
            )

    paths = json_schema_paths(payload) if payload is not None else []
    attempts = int(getattr(result, "attempts", 0) or 0)
    encoded = int(getattr(result, "encoded_bytes", 0) or 0)
    decoded = int(getattr(result, "decoded_bytes", 0) or 0)
    direct = int(getattr(result, "direct_bytes", encoded) or 0)
    proxy = int(getattr(result, "proxy_bytes", 0) or 0)
    payload_hash = (
        sha256_bytes(canonical_json_bytes(payload)) if payload is not None else None
    )
    body_hash = sha256_bytes(body) if body is not None else None
    record: dict[str, Any] = {
        **asdict(scope),
        "request_params": scope.params,
        "url": str(getattr(result, "url", API_URL)),
        "target_key": getattr(result, "target_key", None),
        "ok": payload is not None and not errors,
        "http_status": getattr(result, "http_status", None),
        "transport_outcome": str(getattr(result, "status", "unknown")),
        "transport_cache_hit": bool(getattr(result, "cache_hit", False)),
        "transport_stale": bool(getattr(result, "stale", False)),
        "attempt_count": attempts,
        "retry_count": int(getattr(result, "retries", max(0, attempts - 1)) or 0),
        "seconds": round(seconds, 6),
        # Canonical transport intentionally exposes aggregate per-result
        # counters rather than response objects for every retry.
        "attempts": [],
        "status_counts": statuses,
        "encoded_direct_bytes": direct,
        "decoded_direct_bytes": decoded if proxy == 0 else 0,
        "encoded_proxy_bytes": proxy,
        "decoded_proxy_bytes": decoded if proxy else 0,
        "response_headers": {
            "etag": getattr(result, "etag", None),
            "last_modified": getattr(result, "last_modified", None),
        },
        "selected_season_observed": observed_season,
        "competition_id_observed": observed_id,
        "payload_sha256": payload_hash,
        "decoded_body_sha256": body_hash,
        "transport_content_hash": getattr(result, "content_hash", None),
        "transport_raw_uri": getattr(result, "raw_uri", None),
        "transport_fetched_at": getattr(result, "fetched_at", None),
        "row_counts": summarize_payload(payload) if payload is not None else {},
        "field_path_count": len(paths),
        "field_paths": paths,
        "validation_errors": errors,
        "validation_warnings": warnings,
        "artifact_path": None,
        "raw_cache_hit": False,
    }
    return FetchCapture(record=record, payload=payload, decoded_body=body)


def make_canonical_transport(
    artifact_dir: Path,
    *,
    timeout: float,
    max_attempts: int,
    max_retry_wait: float,
    requests_per_minute: float,
) -> Any:
    """Lazily construct the post-refactor direct transport and local raw store."""

    try:
        from scrapers.fotmob.raw_store import FotMobRawStore
        from scrapers.fotmob.transport import FotMobTransport
    except ImportError as exc:  # pragma: no cover - baseline checkout path
        raise RuntimeError(
            "canonical FotMob transport is unavailable; use --transport standalone"
        ) from exc

    raw_root = (artifact_dir.resolve() / "canonical-raw").as_uri()
    store = FotMobRawStore.from_uri(raw_root)
    return FotMobTransport(
        store,
        timeout=timeout,
        max_attempts=max_attempts,
        max_retry_delay=max_retry_wait,
        rate_limiter=RateLimiter(requests_per_minute),
    )


def write_raw_artifact(
    artifact_dir: Path,
    scope: Sentinel,
    decoded_body: bytes,
    decoded_hash: str,
) -> tuple[str, bool]:
    """Write exact decoded JSON as deterministic content-addressed gzip."""

    relative = Path("raw") / scope.key / f"{decoded_hash}.json.gz"
    destination = artifact_dir / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    existed = destination.exists()
    if not existed:
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(gzip.compress(decoded_body, compresslevel=6, mtime=0))
        temporary.replace(destination)
    return relative.as_posix(), existed


def failed_capture(scope: Sentinel, exc: Exception) -> FetchCapture:
    """Represent an unexpected target failure instead of dropping its scope."""

    message = f"unexpected_error: {type(exc).__name__}: {exc}"
    return FetchCapture(
        record={
            **asdict(scope),
            "request_params": scope.params,
            "url": None,
            "ok": False,
            "http_status": None,
            "attempt_count": 1,
            "retry_count": 0,
            "seconds": 0.0,
            "attempts": [],
            "status_counts": {"exception": 1},
            "encoded_direct_bytes": 0,
            "decoded_direct_bytes": 0,
            "encoded_proxy_bytes": 0,
            "decoded_proxy_bytes": 0,
            "response_headers": {},
            "selected_season_observed": None,
            "competition_id_observed": None,
            "payload_sha256": None,
            "decoded_body_sha256": None,
            "row_counts": {},
            "field_path_count": 0,
            "field_paths": [],
            "validation_errors": [message],
            "validation_warnings": [],
            "artifact_path": None,
            "raw_cache_hit": False,
        }
    )


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _sum_target_metric(records: Sequence[Mapping[str, Any]], key: str) -> int:
    return sum(int(record.get(key) or 0) for record in records)


def build_report(
    *,
    label: str,
    api_url: str,
    artifact_dir: Path,
    records: Sequence[dict[str, Any]],
    started_at: str,
    total_seconds: float,
    proxy_enabled: bool,
    requests_per_minute: float,
    transport_mode: str,
) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    row_counts: Counter[str] = Counter()
    path_targets: dict[str, list[str]] = {}
    for record in records:
        status_counts.update(record.get("status_counts") or {})
        row_counts.update(record.get("row_counts") or {})
        for path in record.get("field_paths") or []:
            path_targets.setdefault(path, []).append(str(record["key"]))

    direct_encoded = _sum_target_metric(records, "encoded_direct_bytes")
    direct_decoded = _sum_target_metric(records, "decoded_direct_bytes")
    proxy_encoded = _sum_target_metric(records, "encoded_proxy_bytes")
    proxy_decoded = _sum_target_metric(records, "decoded_proxy_bytes")
    failed = [str(record["key"]) for record in records if not record.get("ok")]
    mismatches = [
        str(record["key"])
        for record in records
        if record.get("selected_season_observed") is not None
        and record.get("selected_season_observed") != record.get("source_season_key")
    ]
    unconfirmed = [
        str(record["key"])
        for record in records
        if record.get("selected_season_observed") is None
    ]

    return {
        "schema_version": "fotmob.fetch-benchmark.v1",
        "label": label,
        "mode": "live_no_production_writes",
        "transport": transport_mode,
        "started_at": started_at,
        "finished_at": utc_now(),
        "artifact_dir": str(artifact_dir.resolve()),
        "api_url": api_url,
        "requests_per_minute": requests_per_minute,
        "proxy_enabled": proxy_enabled,
        "traffic_measurement": (
            "encoded bytes are HTTP response bodies on the wire; decoded bytes "
            "are JSON payload bytes processed (and may come from a 304 cache "
            "replay in canonical mode); request/response headers, connection "
            "and TLS overhead are excluded"
        ),
        "sentinel_matrix": [
            {
                "key": record["key"],
                "competition_id": record["competition_id"],
                "source_season_key": record["source_season_key"],
            }
            for record in records
        ],
        "metrics": {
            "wall_seconds": round(total_seconds, 6),
            "logical_targets": len(records),
            "attempts": sum(int(record["attempt_count"]) for record in records),
            "retries": sum(int(record["retry_count"]) for record in records),
            "status_counts": dict(sorted(status_counts.items())),
            "targets_succeeded": len(records) - len(failed),
            "targets_failed": len(failed),
            "encoded_direct_bytes": direct_encoded,
            "decoded_direct_bytes": direct_decoded,
            "encoded_proxy_bytes": proxy_encoded,
            "decoded_proxy_bytes": proxy_decoded,
            "encoded_direct_mb": round(direct_encoded / 1024 / 1024, 6),
            "decoded_direct_mb": round(direct_decoded / 1024 / 1024, 6),
            "encoded_proxy_mb": round(proxy_encoded / 1024 / 1024, 6),
            "decoded_proxy_mb": round(proxy_decoded / 1024 / 1024, 6),
            "raw_artifact_writes": sum(
                bool(record.get("artifact_path"))
                and not bool(record.get("raw_cache_hit"))
                for record in records
            ),
            "raw_artifact_hits": sum(
                bool(record.get("artifact_path")) and bool(record.get("raw_cache_hit"))
                for record in records
            ),
            "row_counts": dict(sorted(row_counts.items())),
        },
        "completeness": {
            "complete": not failed and not mismatches,
            "failed_targets": failed,
            "season_mismatch_targets": mismatches,
            "season_unconfirmed_targets": unconfirmed,
            "payload_hashes": {
                str(record["key"]): record.get("payload_sha256") for record in records
            },
        },
        "field_inventory": {
            "union_path_count": len(path_targets),
            "paths": {
                path: sorted(targets) for path, targets in sorted(path_targets.items())
            },
        },
        "targets": list(records),
    }


def run_benchmark(
    *,
    scopes: Sequence[Sentinel],
    artifact_dir: Path,
    label: str,
    api_url: str = API_URL,
    timeout: float = 30.0,
    max_attempts: int = 4,
    max_retry_wait: float = 60.0,
    requests_per_minute: float = 30.0,
    proxy_url: Optional[str] = None,
    strict_selected_season: bool = True,
    session: Any = None,
    transport_mode: str = "standalone",
    canonical_transport: Any = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if transport_mode not in {"standalone", "canonical"}:
        raise ValueError("transport_mode must be 'standalone' or 'canonical'")
    if transport_mode == "canonical" and proxy_url:
        raise ValueError("canonical transport is direct-only; --proxy-url is invalid")
    owns_session = transport_mode == "standalone" and session is None
    active_session = session or (
        make_session(proxy_url) if transport_mode == "standalone" else None
    )
    owns_transport = transport_mode == "canonical" and canonical_transport is None
    active_transport = canonical_transport
    if transport_mode == "canonical" and active_transport is None:
        active_transport = make_canonical_transport(
            artifact_dir,
            timeout=timeout,
            max_attempts=max_attempts,
            max_retry_wait=max_retry_wait,
            requests_per_minute=requests_per_minute,
        )
    limiter = RateLimiter(requests_per_minute)
    started_at = utc_now()
    started = time.perf_counter()
    records: list[dict[str, Any]] = []
    try:
        for scope in scopes:
            try:
                if transport_mode == "canonical":
                    capture = fetch_scope_canonical(
                        scope,
                        transport=active_transport,
                        strict_selected_season=strict_selected_season,
                    )
                else:
                    capture = fetch_scope(
                        scope,
                        session=active_session,
                        limiter=limiter,
                        api_url=api_url,
                        timeout=timeout,
                        max_attempts=max_attempts,
                        max_retry_wait=max_retry_wait,
                        proxy_enabled=bool(proxy_url),
                        strict_selected_season=strict_selected_season,
                        sleep=sleep,
                    )
            except Exception as exc:  # noqa: BLE001 - preserve full matrix report
                capture = failed_capture(scope, exc)
            if capture.decoded_body is not None:
                decoded_hash = str(capture.record["decoded_body_sha256"])
                relative, cache_hit = write_raw_artifact(
                    artifact_dir,
                    scope,
                    capture.decoded_body,
                    decoded_hash,
                )
                capture.record["artifact_path"] = relative
                capture.record["raw_cache_hit"] = cache_hit
            records.append(capture.record)
    finally:
        if owns_session:
            active_session.close()
        if owns_transport:
            active_transport.session.close()

    return build_report(
        label=label,
        api_url=api_url,
        artifact_dir=artifact_dir,
        records=records,
        started_at=started_at,
        total_seconds=time.perf_counter() - started,
        proxy_enabled=bool(proxy_url),
        requests_per_minute=requests_per_minute,
        transport_mode=transport_mode,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--label", default=os.environ.get("FOTMOB_BENCH_LABEL", "baseline")
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Local-only artifact root (default: /tmp/fotmob-benchmark/<label>)",
    )
    parser.add_argument(
        "--report", type=Path, help="Default: <artifact-dir>/fetch-report.json"
    )
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--max-retry-wait", type=float, default=60.0)
    parser.add_argument("--rpm", type=float, default=30.0)
    parser.add_argument(
        "--transport",
        choices=("standalone", "canonical"),
        default="standalone",
        help="Standalone baseline or lazily imported refactored transport",
    )
    parser.add_argument(
        "--proxy-url",
        default=os.environ.get("FOTMOB_BENCH_PROXY_URL"),
        help="Explicit proxy URL; environment proxies are always ignored",
    )
    parser.add_argument(
        "--target",
        action="append",
        choices=[scope.key for scope in SENTINELS],
        help="Repeat to select a subset; default is the complete fixed matrix",
    )
    parser.add_argument(
        "--allow-missing-selected-season",
        action="store_true",
        help="Record a warning instead of failing when selectedSeason is absent",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    artifact_dir = args.artifact_dir or Path(f"/tmp/fotmob-benchmark/{args.label}")
    report_path = args.report or artifact_dir / "fetch-report.json"
    selected_keys = set(args.target or [])
    scopes = [
        scope for scope in SENTINELS if not selected_keys or scope.key in selected_keys
    ]
    report = run_benchmark(
        scopes=scopes,
        artifact_dir=artifact_dir,
        label=args.label,
        api_url=args.api_url,
        timeout=args.timeout,
        max_attempts=max(1, args.max_attempts),
        max_retry_wait=max(0.0, args.max_retry_wait),
        requests_per_minute=max(0.0, args.rpm),
        proxy_url=args.proxy_url,
        strict_selected_season=not args.allow_missing_selected_season,
        transport_mode=args.transport,
    )
    atomic_write_json(report_path, report)
    summary = {
        "schema_version": report["schema_version"],
        "label": report["label"],
        "metrics": report["metrics"],
        "completeness": report["completeness"],
        "report_path": str(report_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["completeness"]["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
