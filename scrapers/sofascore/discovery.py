"""Direct-JSON discovery and deterministic registry merge for SofaScore."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from scrapers.sofascore.catalog import SCHEMA_VERSION, SofaScoreCatalog


DEFAULT_BASE_URL = "https://api.sofascore.com/api/v1"
CATALOG_PATH = "/config/unique-tournaments/EN/football"
CATEGORIES_PATH = "/sport/football/categories/all"
CATEGORIES_FALLBACK_PATH = "/sport/football/categories"
_SPLIT_SHORT_RE = re.compile(r"^(\d{2})/(\d{2})$")
_SPLIT_LONG_RE = re.compile(r"^(\d{4})/(\d{2}|\d{4})$")
_SINGLE_YEAR_RE = re.compile(r"^\d{4}$")
_CURLOPT_PROXY = 10004


class DiscoveryError(RuntimeError):
    """Base error for a discovery run that must not update the registry."""


class DiscoverySchemaError(DiscoveryError):
    """A SofaScore JSON response violates the expected public contract."""


class DiscoveryConcurrentUpdate(DiscoveryError):
    """The activation registry changed after discovery started."""


class DiscoveryHTTPError(DiscoveryError):
    """A direct request failed without any proxy/browser fallback."""

    def __init__(self, message: str, *, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise DiscoverySchemaError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise DiscoverySchemaError(
            f"{field} must be a positive integer"
        ) from exc
    if parsed <= 0:
        raise DiscoverySchemaError(f"{field} must be a positive integer")
    return parsed


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoverySchemaError(f"{field} must be a non-empty string")
    return value.strip()


def classify_season_year(year: Any) -> tuple[str, Optional[str]]:
    """Return ``(season_format, canonical_season)`` without guessing.

    SofaScore uses slash-separated consecutive years for ordinary seasons and
    a literal four-digit year for single-year competitions. Unknown labels are
    retained in the registry but are deliberately non-activatable.
    """

    token = str(year).strip()
    short = _SPLIT_SHORT_RE.fullmatch(token)
    if short:
        start, end = (int(part) for part in short.groups())
        if (start + 1) % 100 == end:
            return "split_year", f"{start:02d}{end:02d}"
        return "unknown", None

    long = _SPLIT_LONG_RE.fullmatch(token)
    if long:
        start = int(long.group(1))
        raw_end = long.group(2)
        end = int(raw_end) if len(raw_end) == 4 else start // 100 * 100 + int(raw_end)
        if end < start:
            end += 100
        if end == start + 1:
            return "split_year", f"{start % 100:02d}{end % 100:02d}"
        return "unknown", None

    if _SINGLE_YEAR_RE.fullmatch(token):
        return "single_year", token
    return "unknown", None


def _response_bytes(response: Any) -> int:
    try:
        content = response.content
    except Exception:
        return 0
    if isinstance(content, str):
        return len(content.encode("utf-8"))
    try:
        return len(content or b"")
    except Exception:
        return 0


class DirectSofaScoreClient:
    """Bounded direct HTTP client with an invariant zero paid-proxy route."""

    def __init__(
        self,
        *,
        session_factory: Optional[Callable[[], Any]] = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30,
        max_attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not base_url.startswith("https://"):
            raise ValueError("SofaScore discovery base_url must use https")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self._session_factory = session_factory
        self._sleeper = sleeper
        self._session: Any = None
        self._requests = 0
        self._direct_response_bytes = 0

    def _new_session(self) -> Any:
        proxy_option: Any = _CURLOPT_PROXY
        if self._session_factory is not None:
            session = self._session_factory()
        else:
            try:
                from curl_cffi.const import CurlOpt
                from curl_cffi.requests import Session
            except ImportError as exc:  # pragma: no cover - production image
                raise DiscoveryHTTPError(
                    "curl_cffi is required for direct SofaScore discovery"
                ) from exc
            proxy_option = CurlOpt.PROXY
            session = Session(
                impersonate="chrome120",
                trust_env=False,
                proxies={},
                # curl_cffi 0.15 stores ``trust_env`` but does not thread it
                # into libcurl. This option is the transport-level guarantee
                # that HTTPS_PROXY/ALL_PROXY can never be used.
                curl_options={proxy_option: ""},
            )

        # Two independent guards: ignore proxy environment variables and clear
        # any proxy mapping a supplied session might carry. Every request also
        # receives an explicit empty mapping below.
        try:
            session.trust_env = False
        except Exception as exc:
            raise DiscoveryHTTPError(
                "direct session cannot disable environment proxies"
            ) from exc
        try:
            session.proxies = {}
        except Exception as exc:
            raise DiscoveryHTTPError(
                "direct session cannot clear proxy configuration"
            ) from exc
        try:
            curl_options = dict(getattr(session, "curl_options", {}) or {})
            curl_options[proxy_option] = ""
            session.curl_options = curl_options
        except Exception as exc:
            raise DiscoveryHTTPError(
                "direct session cannot force CURLOPT_PROXY off"
            ) from exc

        headers = getattr(session, "headers", None)
        if headers is not None:
            headers.update({
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.sofascore.com/",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                ),
            })
        return session

    @property
    def session(self) -> Any:
        if self._session is None:
            self._session = self._new_session()
        return self._session

    @property
    def stats(self) -> dict[str, int]:
        return {
            "requests": self._requests,
            "direct_response_bytes": self._direct_response_bytes,
            "paid_proxy_bytes": 0,
            "browser_sessions": 0,
        }

    def get_json(self, path: str) -> Mapping[str, Any]:
        clean_path = "/" + str(path).lstrip("/")
        if "://" in clean_path or ".." in clean_path.split("/"):
            raise ValueError("discovery path must be a relative API path")
        url = f"{self.base_url}{clean_path}"
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            self._requests += 1
            try:
                response = self.session.get(
                    url,
                    timeout=self.timeout,
                    proxies={},
                )
            except Exception as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                self._sleeper(min(2 ** (attempt - 1), 4))
                continue

            self._direct_response_bytes += _response_bytes(response)
            try:
                status = int(response.status_code)
            except (AttributeError, TypeError, ValueError) as exc:
                raise DiscoveryHTTPError(
                    f"direct SofaScore response has no valid status: {url}"
                ) from exc

            if status == 200:
                try:
                    payload = response.json()
                except Exception:
                    try:
                        content = response.content
                        if isinstance(content, bytes):
                            content = content.decode("utf-8")
                        payload = json.loads(content)
                    except Exception as exc:
                        raise DiscoverySchemaError(
                            f"invalid JSON from {clean_path}"
                        ) from exc
                if not isinstance(payload, Mapping):
                    raise DiscoverySchemaError(
                        f"JSON root from {clean_path} must be an object"
                    )
                return payload

            error = DiscoveryHTTPError(
                f"direct SofaScore request failed: HTTP {status} {clean_path}",
                status_code=status,
            )
            if status == 403:
                raise error
            if status == 429 or 500 <= status <= 599:
                last_error = error
                if attempt < self.max_attempts:
                    self._sleeper(min(2 ** (attempt - 1), 4))
                    continue
            raise error

        raise DiscoveryHTTPError(
            f"direct SofaScore request failed after {self.max_attempts} "
            f"attempts: {clean_path}: {last_error}",
            status_code=getattr(last_error, "status_code", None),
        ) from last_error

    def close(self) -> None:
        if self._session is not None:
            close = getattr(self._session, "close", None)
            if callable(close):
                close()

    def __enter__(self) -> "DirectSofaScoreClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


def parse_categories_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse the complete football category index used for the full scan."""

    raw_categories = payload.get("categories")
    if not isinstance(raw_categories, list):
        raise DiscoverySchemaError("categories payload must contain categories")

    by_id: dict[int, dict[str, Any]] = {}
    for index, raw in enumerate(raw_categories):
        if not isinstance(raw, Mapping):
            raise DiscoverySchemaError(f"categories[{index}] must be an object")
        prefix = f"categories[{index}]"
        category_id = _positive_int(raw.get("id"), f"{prefix}.id")
        sport = raw.get("sport") or {}
        if not isinstance(sport, Mapping):
            raise DiscoverySchemaError(f"{prefix}.sport must be an object")
        sport_slug = str(sport.get("slug") or "football").strip()
        if sport_slug != "football":
            raise DiscoverySchemaError(f"{prefix} is not a football category")
        record = {
            "id": category_id,
            "name": _required_string(raw.get("name"), f"{prefix}.name"),
            "slug": _required_string(raw.get("slug"), f"{prefix}.slug"),
        }
        previous = by_id.get(category_id)
        if previous is not None and previous != record:
            raise DiscoverySchemaError(
                f"conflicting duplicate category id {category_id}"
            )
        by_id[category_id] = record
    return [by_id[category_id] for category_id in sorted(by_id)]


def _raw_tournament_items(payload: Mapping[str, Any]) -> list[Any]:
    """Read both curated and category-group SofaScore response shapes."""

    containers: list[list[Any]] = []
    recognized_shape = False
    if "uniqueTournaments" in payload:
        recognized_shape = True
        raw_tournaments = payload["uniqueTournaments"]
        if not isinstance(raw_tournaments, list):
            raise DiscoverySchemaError("uniqueTournaments must be a list")
        containers.append(raw_tournaments)

    if "groups" in payload:
        recognized_shape = True
        groups = payload["groups"]
        if not isinstance(groups, list):
            raise DiscoverySchemaError("groups must be a list")
        for index, group in enumerate(groups):
            if not isinstance(group, Mapping):
                raise DiscoverySchemaError(f"groups[{index}] must be an object")
            raw_tournaments = group.get("uniqueTournaments")
            if not isinstance(raw_tournaments, list):
                raise DiscoverySchemaError(
                    f"groups[{index}] must contain uniqueTournaments"
                )
            containers.append(raw_tournaments)

    if not recognized_shape:
        raise DiscoverySchemaError(
            "tournament payload must contain uniqueTournaments or groups"
        )
    return [item for container in containers for item in container]


def parse_catalog_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_tournaments = _raw_tournament_items(payload)

    by_id: dict[int, dict[str, Any]] = {}
    for index, raw in enumerate(raw_tournaments):
        if not isinstance(raw, Mapping):
            raise DiscoverySchemaError(
                f"uniqueTournaments[{index}] must be an object"
            )
        prefix = f"uniqueTournaments[{index}]"
        source_id = _positive_int(raw.get("id"), f"{prefix}.id")
        category = raw.get("category")
        if not isinstance(category, Mapping):
            raise DiscoverySchemaError(f"{prefix}.category must be an object")
        sport = category.get("sport") or {}
        if not isinstance(sport, Mapping):
            raise DiscoverySchemaError(f"{prefix}.category.sport must be an object")
        sport_slug = str(sport.get("slug") or "football").strip()
        if sport_slug != "football":
            raise DiscoverySchemaError(
                f"{prefix} is not a football tournament"
            )
        slug = _required_string(raw.get("slug"), f"{prefix}.slug")
        category_slug = _required_string(
            category.get("slug"), f"{prefix}.category.slug"
        )
        category_id = category.get("id")
        if category_id is not None:
            category_id = _positive_int(category_id, f"{prefix}.category.id")
        record = {
            "unique_tournament_id": source_id,
            "name": _required_string(raw.get("name"), f"{prefix}.name"),
            "slug": slug,
            "category": {
                "id": category_id,
                "name": _required_string(
                    category.get("name"), f"{prefix}.category.name"
                ),
                "slug": category_slug,
            },
            "sport_slug": sport_slug,
            "page_path": f"{sport_slug}/{category_slug}/{slug}",
            "canonical_id": None,
            "enabled": False,
            "seasons": [],
        }
        previous = by_id.get(source_id)
        if previous is not None and previous != record:
            raise DiscoverySchemaError(
                f"conflicting duplicate unique_tournament_id {source_id}"
            )
        by_id[source_id] = record
    return [by_id[source_id] for source_id in sorted(by_id)]


def _merge_tournament_sources(
    *sources: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Union source snapshots by SofaScore id and reject inconsistent data."""

    by_id: dict[int, dict[str, Any]] = {}
    for source in sources:
        for tournament in source:
            source_id = int(tournament["unique_tournament_id"])
            previous = by_id.get(source_id)
            if previous is not None and previous != tournament:
                raise DiscoverySchemaError(
                    "conflicting tournament data across discovery endpoints "
                    f"for unique_tournament_id {source_id}"
                )
            by_id[source_id] = tournament
    return [by_id[source_id] for source_id in sorted(by_id)]


def parse_seasons_payload(
    payload: Mapping[str, Any],
    unique_tournament_id: int,
) -> list[dict[str, Any]]:
    raw_seasons = payload.get("seasons")
    if not isinstance(raw_seasons, list):
        raise DiscoverySchemaError(
            f"tournament {unique_tournament_id} payload must contain seasons"
        )
    by_id: dict[int, dict[str, Any]] = {}
    years: dict[str, int] = {}
    canonical_seasons: dict[str, int] = {}
    for index, raw in enumerate(raw_seasons):
        if not isinstance(raw, Mapping):
            raise DiscoverySchemaError(
                f"tournament {unique_tournament_id} seasons[{index}] "
                "must be an object"
            )
        prefix = f"tournament {unique_tournament_id} seasons[{index}]"
        season_id = _positive_int(raw.get("id"), f"{prefix}.id")
        year = _required_string(raw.get("year"), f"{prefix}.year")
        season_format, canonical_season = classify_season_year(year)
        record = {
            "season_id": season_id,
            "name": _required_string(raw.get("name"), f"{prefix}.name"),
            "year": year,
            "season_format": season_format,
            "canonical_season": canonical_season,
        }
        previous = by_id.get(season_id)
        if previous is not None and previous != record:
            raise DiscoverySchemaError(
                f"tournament {unique_tournament_id} has conflicting "
                f"season_id {season_id}"
            )
        previous_year = years.get(year)
        if previous_year is not None and previous_year != season_id:
            raise DiscoverySchemaError(
                f"tournament {unique_tournament_id} has ambiguous year {year!r}"
            )
        years[year] = season_id
        if canonical_season is not None:
            previous_canonical = canonical_seasons.get(canonical_season)
            if (
                previous_canonical is not None
                and previous_canonical != season_id
            ):
                raise DiscoverySchemaError(
                    f"tournament {unique_tournament_id} has ambiguous "
                    f"canonical_season {canonical_season!r}"
                )
            canonical_seasons[canonical_season] = season_id
        by_id[season_id] = record
    return [by_id[season_id] for season_id in sorted(by_id)]


def merge_registry(
    existing: Mapping[str, Any],
    discovered_tournaments: list[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, int]]:
    """Merge source-owned fields while preserving activation and mappings."""

    SofaScoreCatalog.from_mapping(existing)
    old_records = {
        int(raw["unique_tournament_id"]): deepcopy(raw)
        for raw in existing.get("tournaments", [])
    }

    merged = dict(old_records)
    new_count = 0
    updated_count = 0
    unchanged_count = 0
    seen_discovered: set[int] = set()

    for raw_discovered in discovered_tournaments:
        discovered = deepcopy(dict(raw_discovered))
        source_id = _positive_int(
            discovered.get("unique_tournament_id"),
            "discovered.unique_tournament_id",
        )
        if source_id in seen_discovered:
            raise DiscoverySchemaError(
                f"duplicate discovered unique_tournament_id {source_id}"
            )
        seen_discovered.add(source_id)
        previous = old_records.get(source_id)
        if previous is None:
            discovered["canonical_id"] = None
            discovered["enabled"] = False
            merged[source_id] = discovered
            new_count += 1
            continue

        discovered["canonical_id"] = previous.get("canonical_id")
        discovered["enabled"] = previous.get("enabled", False)
        previous_seasons = {
            int(season["season_id"]): deepcopy(season)
            for season in previous.get("seasons", [])
        }
        for season in discovered.get("seasons", []):
            # SofaScore can replace a source season id while keeping the same
            # logical year. The current snapshot is authoritative for that
            # canonical season; retaining the superseded id would create an
            # ambiguous registry entry on the next lookup.
            for previous_id, previous_season in list(previous_seasons.items()):
                same_year = previous_season.get("year") == season.get("year")
                canonical = season.get("canonical_season")
                same_canonical = (
                    canonical is not None
                    and previous_season.get("canonical_season") == canonical
                )
                if int(previous_id) != int(season["season_id"]) and (
                    same_year or same_canonical
                ):
                    del previous_seasons[previous_id]
            previous_seasons[int(season["season_id"])] = deepcopy(season)
        discovered["seasons"] = [
            previous_seasons[season_id]
            for season_id in sorted(previous_seasons)
        ]
        merged[source_id] = discovered
        if discovered == previous:
            unchanged_count += 1
        else:
            updated_count += 1

    # Existing tournaments absent from the latest upstream catalog are kept.
    unchanged_count += len(set(old_records) - seen_discovered)
    document = {
        "schema_version": SCHEMA_VERSION,
        "tournaments": [merged[source_id] for source_id in sorted(merged)],
    }
    SofaScoreCatalog.from_mapping(document)
    return document, {
        "new_tournaments": new_count,
        "updated_tournaments": updated_count,
        "unchanged_tournaments": unchanged_count,
        "total_tournaments": len(merged),
        "total_seasons": sum(
            len(item.get("seasons", [])) for item in document["tournaments"]
        ),
    }


def discover_registry(
    existing: Mapping[str, Any],
    client: DirectSofaScoreClient,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fetch the complete catalog and every season list before merging."""

    existing_catalog = SofaScoreCatalog.from_mapping(existing)
    enabled_source_ids = {
        tournament.unique_tournament_id
        for tournament in existing_catalog.tournaments
        if tournament.enabled
    }

    # The config endpoint contains only SofaScore's curated competitions. It
    # remains useful for global/special entries, but the category fan-out is
    # what turns this into a complete tournament scan.
    curated = parse_catalog_payload(client.get_json(CATALOG_PATH))
    try:
        categories_payload = client.get_json(CATEGORIES_PATH)
    except DiscoveryHTTPError as exc:
        if exc.status_code != 404:
            raise
        categories_payload = client.get_json(CATEGORIES_FALLBACK_PATH)
    categories = parse_categories_payload(categories_payload)
    if not categories:
        raise DiscoverySchemaError(
            "complete football category index must not be empty"
        )

    category_tournaments: list[dict[str, Any]] = []
    empty_categories = 0
    for category in categories:
        try:
            category_payload = client.get_json(
                f"/category/{category['id']}/unique-tournaments"
            )
        except DiscoveryHTTPError as exc:
            if exc.status_code != 404:
                raise
            # The category came from the same source snapshot, so a missing
            # fan-out route makes this run incomplete. Preserve the previous
            # registry and let a later run retry instead of reporting success
            # from a curated-only subset.
            raise DiscoveryHTTPError(
                "incomplete category scan: SofaScore returned HTTP 404 for "
                f"category {category['id']}",
                status_code=404,
            ) from exc
        parsed = parse_catalog_payload(category_payload)
        if not parsed:
            empty_categories += 1
        category_tournaments.extend(parsed)

    category_tournaments = _merge_tournament_sources(category_tournaments)
    if not category_tournaments:
        raise DiscoverySchemaError(
            "complete category scan returned no football tournaments"
        )
    tournaments = _merge_tournament_sources(curated, category_tournaments)
    discovered_source_ids = {
        int(tournament["unique_tournament_id"])
        for tournament in tournaments
    }
    missing_enabled = sorted(enabled_source_ids - discovered_source_ids)
    if missing_enabled:
        raise DiscoverySchemaError(
            "complete discovery omitted enabled tournaments: "
            + ", ".join(str(source_id) for source_id in missing_enabled)
        )

    for tournament in tournaments:
        source_id = tournament["unique_tournament_id"]
        seasons_missing = False
        try:
            seasons_payload = client.get_json(
                f"/unique-tournament/{source_id}/seasons"
            )
        except DiscoveryHTTPError as exc:
            if exc.status_code != 404:
                raise
            seasons_payload = {"seasons": []}
            seasons_missing = True
        seasons = parse_seasons_payload(
            seasons_payload,
            source_id,
        )
        if source_id in enabled_source_ids and (
            seasons_missing
            or not seasons
            or not any(season["canonical_season"] for season in seasons)
        ):
            raise DiscoverySchemaError(
                f"enabled tournament {source_id} has no usable seasons in "
                "the current source response"
            )
        tournament["seasons"] = seasons

    merged, counts = merge_registry(existing, tournaments)
    report: dict[str, Any] = {
        **counts,
        "catalog_tournaments": len(tournaments),
        "curated_tournaments": len(curated),
        "category_tournaments": len(category_tournaments),
        "categories": len(categories),
        "empty_categories": empty_categories,
        "changed": merged != existing,
        "traffic": client.stats,
    }
    return merged, report


def render_registry(document: Mapping[str, Any]) -> bytes:
    SofaScoreCatalog.from_mapping(document)
    return (
        json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")


def _read_registry_snapshot(path: Path) -> tuple[Mapping[str, Any], int]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            current = json.load(handle)
            mode = stat.S_IMODE(os.fstat(handle.fileno()).st_mode)
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoveryError(f"cannot compare existing registry {path}: {exc}") from exc
    if not isinstance(current, Mapping):
        raise DiscoveryError(f"existing registry {path} must be a JSON object")
    return current, mode


def _assert_expected_registry(
    destination: Path,
    expected_current: Mapping[str, Any],
) -> tuple[Mapping[str, Any], int]:
    if not destination.exists():
        raise DiscoveryConcurrentUpdate(
            f"registry changed during discovery: {destination} was removed; rerun"
        )
    current, mode = _read_registry_snapshot(destination)
    if current != expected_current:
        raise DiscoveryConcurrentUpdate(
            f"registry changed during discovery: {destination}; rerun"
        )
    return current, mode


def write_registry_atomic(
    path: str | Path,
    document: Mapping[str, Any],
    *,
    expected_current: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Atomically replace a valid registry with optional optimistic CAS."""

    destination = Path(path)
    payload = render_registry(document)
    destination_mode = 0o644
    if destination.exists():
        current, destination_mode = _read_registry_snapshot(destination)
        if expected_current is not None and current != expected_current:
            raise DiscoveryConcurrentUpdate(
                f"registry changed during discovery: {destination}; rerun"
            )
        if current == document:
            return False
    elif expected_current is not None:
        raise DiscoveryConcurrentUpdate(
            f"registry changed during discovery: {destination} was removed; rerun"
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        dir=str(destination.parent),
    )
    try:
        os.fchmod(fd, destination_mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        # Check again immediately before replace so an activation/canonical-id
        # edit made while the long network scan was running is never silently
        # overwritten by the stale snapshot.
        if expected_current is not None:
            _, destination_mode = _assert_expected_registry(
                destination,
                expected_current,
            )
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return True


__all__ = [
    "CATALOG_PATH",
    "CATEGORIES_FALLBACK_PATH",
    "CATEGORIES_PATH",
    "DEFAULT_BASE_URL",
    "DirectSofaScoreClient",
    "DiscoveryError",
    "DiscoveryConcurrentUpdate",
    "DiscoveryHTTPError",
    "DiscoverySchemaError",
    "classify_season_year",
    "discover_registry",
    "merge_registry",
    "parse_catalog_payload",
    "parse_categories_payload",
    "parse_seasons_payload",
    "render_registry",
    "write_registry_atomic",
]
