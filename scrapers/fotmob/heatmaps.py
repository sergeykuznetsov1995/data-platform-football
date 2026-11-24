"""Pure, offline FotMob heatmap contracts and Bronze projections.

The public source URL is an identity attribute, never an HTTP destination here.
Transport, persistence, budgets and publication belong to the service adapter.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from urllib.parse import parse_qs, urlsplit
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Protocol

from scrapers.fotmob.transport import CanonicalTarget, canonicalize_target


COORDINATE_SYSTEM = "svg_view_box_user_units"
ORIENTATION = "unknown"
RADIUS_SEMANTICS = "unknown"


class HeatmapOutcome(str, Enum):
    URL_MISSING = "url_missing"
    RESOURCE_UNAVAILABLE = "resource_unavailable"
    UNEXPECTED_FORMAT = "unexpected_format"
    NOT_APPLICABLE = "not_applicable"
    SUCCESS = "success"
    PARSE_FAILED = "parse_failed"
    UNKNOWN = "unknown"


class FetchOrigin(str, Enum):
    NETWORK = "network"
    REPLAY = "replay"


HEATMAP_IDENTITY_RESOLVER_VERSION = "player_stats_opta_id_v1"


class HeatmapMatchStatus(str, Enum):
    VERIFIED = "verified"
    MISMATCH = "mismatch"
    UNVERIFIED = "unverified"


class HeatmapIdentityStatus(str, Enum):
    MATCHED = "matched"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"
    NOT_ATTEMPTED = "not_attempted"


@dataclass(frozen=True)
class HeatmapPlayerIdentityRow:
    competition_id: int | None
    source_season_key: str | None
    match_id: int
    player_key: str
    player_key_suffix: int | None
    match_status: HeatmapMatchStatus
    identity_status: HeatmapIdentityStatus
    candidate_count: int | None
    fotmob_player_id: int | None
    identity_basis: str
    resolver_version: str = field(default=HEATMAP_IDENTITY_RESOLVER_VERSION, init=False)
    candidate_fotmob_player_ids_json: tuple[int, ...] | None
    heatmap_batch_id: str
    parent_content_hash: str | None
    parent_raw_uri: str | None
    asset_content_hash: str


@dataclass(frozen=True)
class HeatmapReference:
    match_id: int
    heatmap_url: str
    target: CanonicalTarget


@dataclass(frozen=True)
class ReferenceResult:
    outcome: HeatmapOutcome | None
    reference: HeatmapReference | None = None


def _positive_id(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not a match identity")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        result = int(value)
    else:
        raise ValueError("invalid match identity")
    if result <= 0:
        raise ValueError("match identity must be positive")
    return result


def build_heatmap_reference(match_id: int, source_url: str) -> HeatmapReference:
    """Validate a direct source URL or its exact relative canonical wrapper."""
    match_id = _positive_id(match_id)
    if not isinstance(source_url, str):
        raise ValueError("heatmap URL must be a string")
    pattern = (
        r"https://pub\.fotmob\.com(?::443)?/prod/db/api/heatmap/match/"
        + str(match_id)
    )
    if re.fullmatch(pattern, source_url) is not None:
        heatmap_url = source_url
        target = canonicalize_target(
            f"heatmap/match/{match_id}/heatmaps", {"heatmapUrl": heatmap_url}
        )
    else:
        wrapper = urlsplit(source_url)
        if (
            wrapper.scheme
            or wrapper.netloc
            or wrapper.fragment
            or wrapper.path != f"/api/data/heatmap/match/{match_id}/heatmaps"
        ):
            raise ValueError("unsupported heatmap URL or match identity")
        params = parse_qs(wrapper.query, keep_blank_values=True, strict_parsing=True)
        if set(params) != {"heatmapUrl"} or len(params["heatmapUrl"]) != 1:
            raise ValueError("unsupported heatmap wrapper parameters")
        heatmap_url = params["heatmapUrl"][0]
        if re.fullmatch(pattern, heatmap_url) is None:
            raise ValueError("unsupported heatmap URL or match identity")
        target = canonicalize_target(source_url)
    return HeatmapReference(match_id, heatmap_url, target)


def extract_heatmap_reference(
    parent: object, match_id: int
) -> ReferenceResult:
    """Check parent identity before interpreting absence of heatmapUrl."""
    try:
        expected = _positive_id(match_id)
        if not isinstance(parent, Mapping):
            raise ValueError("parent must be an object")
        general = parent.get("general")
        if not isinstance(general, Mapping):
            raise ValueError("missing parent identity")
        if _positive_id(general.get("matchId")) != expected:
            raise ValueError("parent identity mismatch")
        content = parent.get("content", {})
        if not isinstance(content, Mapping):
            raise ValueError("content must be an object")
        source_url = content.get("heatmapUrl")
        if source_url is None or source_url == "":
            return ReferenceResult(HeatmapOutcome.URL_MISSING)
        return ReferenceResult(None, build_heatmap_reference(expected, source_url))
    except (ValueError, TypeError):
        return ReferenceResult(HeatmapOutcome.UNEXPECTED_FORMAT)


class FetchResultLike(Protocol):
    outcome: object
    http_status: int | None
    body: bytes | None
    stale: bool
    error: object
    content_hash: object
    raw_uri: object


@dataclass(frozen=True)
class FetchClassification:
    parse_body: bool
    outcome: HeatmapOutcome | None


def _json_value(body: bytes) -> object:
    # Duplicate object keys would otherwise silently discard source evidence.
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> object:
        raise ValueError(f"invalid JSON constant: {value}")

    return json.loads(
        body, object_pairs_hook=unique_object, parse_constant=invalid_constant
    )


def classify_heatmap_fetch(fetch: FetchResultLike) -> FetchClassification:
    """Classify a wrapper fetch without treating uncertain failures as absence.

    Call only for the validated, match-scoped wrapper. A terminal HTTP 200 is
    classified as malformed only with invalid body/decode evidence. An exact,
    raw-backed transport null result also proves absence; a missing body alone
    can be a raw-store failure and remains unknown.
    """
    name = getattr(fetch.outcome, "name", None)
    if name is None:
        name = str(getattr(fetch.outcome, "value", fetch.outcome)).upper()
    unknown = FetchClassification(False, HeatmapOutcome.UNKNOWN)
    if fetch.stale or name in {"STALE_REPLAY", "RETRYABLE_FAILURE"}:
        return unknown
    if name in {"SUCCESS", "NOT_MODIFIED"}:
        if isinstance(fetch.body, bytes) and fetch.body:
            return FetchClassification(True, None)
        return unknown
    if name == "NOT_AVAILABLE":
        if fetch.http_status in {204, 404}:
            return FetchClassification(False, HeatmapOutcome.RESOURCE_UNAVAILABLE)
        if (
            fetch.http_status in {200, 304}
            and fetch.error == "FotMob returned a null JSON body"
            and getattr(fetch, "content_hash", None)
            and getattr(fetch, "raw_uri", None)
        ):
            return FetchClassification(False, HeatmapOutcome.RESOURCE_UNAVAILABLE)
        if fetch.http_status == 200 and isinstance(fetch.body, bytes):
            try:
                if _json_value(fetch.body) is None:
                    return FetchClassification(
                        False, HeatmapOutcome.RESOURCE_UNAVAILABLE
                    )
            except (ValueError, UnicodeError, RecursionError):
                pass
        return unknown
    if name == "TERMINAL_FAILURE" and fetch.http_status == 200:
        if isinstance(fetch.error, str) and fetch.error.startswith(
            ("invalid JSON response", "response decode failed")
        ):
            return FetchClassification(False, HeatmapOutcome.UNEXPECTED_FORMAT)
        if isinstance(fetch.body, bytes):
            try:
                _json_value(fetch.body)
            except (ValueError, UnicodeError, RecursionError):
                return FetchClassification(False, HeatmapOutcome.UNEXPECTED_FORMAT)
        elif isinstance(fetch.error, (UnicodeError, json.JSONDecodeError)):
            return FetchClassification(False, HeatmapOutcome.UNEXPECTED_FORMAT)
    return unknown


@dataclass(frozen=True)
class AssetMetadata:
    reference: HeatmapReference
    observed_at: str
    content_hash: str
    raw_uri: str
    competition_id: int | None = None
    source_season_key: str | None = None
    http_last_modified: str | None = None


@dataclass(frozen=True)
class HeatmapAssetRow:
    competition_id: int | None
    source_season_key: str | None
    match_id: int
    heatmap_url: str
    canonical_url: str
    target_key: str
    observed_at: str
    source_last_modified: str
    http_last_modified: str | None
    content_hash: str
    raw_uri: str
    view_box: str
    min_x: float
    min_y: float
    width: float
    height: float
    player_count: int
    circle_count: int
    outcome: HeatmapOutcome
    coordinate_system: str = COORDINATE_SYSTEM
    orientation: str = ORIENTATION
    radius_semantics: str = RADIUS_SEMANTICS


@dataclass(frozen=True)
class HeatmapPointRow:
    competition_id: int | None
    source_season_key: str | None
    match_id: int
    player_key: str
    player_id: int
    point_index: int
    cx: float
    cy: float
    r: float
    view_box: str
    asset_content_hash: str
    coordinate_system: str = COORDINATE_SYSTEM


@dataclass(frozen=True)
class ParseResult:
    outcome: HeatmapOutcome
    asset: HeatmapAssetRow | None = None
    points: tuple[HeatmapPointRow, ...] = ()
    player_keys: tuple[str, ...] = field(default=(), kw_only=True)
    hash_verified: bool = False
    decoded: bool = False


_NUMBER = r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
_SEPARATOR = r"(?:[ \t\r\n]+(?:,[ \t\r\n]*)?|,[ \t\r\n]*)"
_VIEW_BOX = re.compile(
    r"[ \t\r\n]*(" + _NUMBER + r")"
    + (_SEPARATOR + r"(" + _NUMBER + r")") * 3
    + r"[ \t\r\n]*"
)


def _xml(text: str) -> ET.Element:
    # Keep comments/PIs visible, and do not permit DTD/entity expansion.
    if "<!DOCTYPE" in text or "<!ENTITY" in text:
        raise ValueError("DTD and entities are not supported")
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True, insert_pis=True))
    return ET.fromstring(text, parser=parser)


def _has_text(text: str | None) -> bool:
    return bool(text and text.strip())


def _literal_view_box(template: str) -> str:
    """Recover the root attribute spelling after ElementTree validates XML.

    ElementTree normalizes literal tabs/newlines and expands character refs in
    attributes. Use its value for numeric validation, but retain source spelling
    in rows. Tokenizing whole quoted attributes avoids matches inside other
    attribute values; comments and processing instructions cannot supply a root.
    """
    source = re.sub(r"<!--.*?-->|<\?.*?\?>", "", template, flags=re.DOTALL)
    opening = re.match(
        r'''[\s\ufeff]*<[^\s/>]+((?:[^<>"']|"[^"]*"|'[^']*')*)>''', source
    )
    if opening is None:
        raise ValueError("cannot locate SVG root opening tag")
    attributes = re.finditer(
        r'''([^\s=<>/]+)\s*=\s*(?:"([^"]*)"|'([^']*)')''', opening.group(1)
    )
    for attribute in attributes:
        if attribute.group(1) == "viewBox":
            return attribute.group(2) if attribute.group(2) is not None else attribute.group(3)
    raise ValueError("cannot locate source viewBox")


def _structure(
    body: bytes,
) -> tuple[str, str, tuple[float, ...], int, list[tuple[str, int, int, str, str, str]]]:
    payload = _json_value(body)
    if not isinstance(payload, dict) or set(payload) != {
        "template", "players", "lastModified"
    }:
        raise ValueError("unexpected top-level shape")
    template = payload["template"]
    modified = payload["lastModified"]
    players = payload["players"]
    if not isinstance(template, str):
        raise ValueError("template must be SVG text")
    if not isinstance(modified, str) or not modified.strip():
        raise ValueError("lastModified must be nonempty text")
    if not isinstance(players, dict):
        raise ValueError("players must be an object")
    svg = _xml(template)
    if svg.tag not in {"svg", "{http://www.w3.org/2000/svg}svg"}:
        raise ValueError("template root must be SVG")
    view_box = svg.get("viewBox")
    if not isinstance(view_box, str):
        raise ValueError("missing viewBox")
    match = _VIEW_BOX.fullmatch(view_box)
    if match is None:
        raise ValueError("invalid viewBox")
    dimensions = tuple(float(value) for value in match.groups())
    if not all(math.isfinite(value) for value in dimensions):
        raise ValueError("nonfinite viewBox")
    if dimensions[2] <= 0 or dimensions[3] <= 0:
        raise ValueError("viewBox dimensions must be positive")
    circles: list[tuple[str, int, int, str, str, str]] = []
    for player_key, fragment in players.items():
        if not isinstance(player_key, str) or not re.fullmatch(r"p[0-9]+", player_key):
            raise ValueError("invalid player key")
        player_id = int(player_key[1:])
        if player_id <= 0 or not isinstance(fragment, str):
            raise ValueError("invalid player identity or fragment")
        root = _xml("<heatmap-fragment>" + fragment + "</heatmap-fragment>")
        if _has_text(root.text):
            raise ValueError("text residue in fragment")
        for index, circle in enumerate(root):
            if circle.tag != "circle" or set(circle.attrib) != {"cx", "cy", "r"}:
                raise ValueError("expected circle with exactly cx, cy, r")
            if len(circle) or _has_text(circle.text) or _has_text(circle.tail):
                raise ValueError("nested elements or text residue")
            circles.append(
                (player_key, player_id, index,
                 circle.attrib["cx"], circle.attrib["cy"], circle.attrib["r"])
            )
    return modified, _literal_view_box(template), dimensions, len(players), circles


def parse_heatmap(body: bytes, metadata: AssetMetadata) -> ParseResult:
    """Keep the source keys in payload order after validating the complete asset."""
    result = _parse_heatmap(body, metadata)
    if result.outcome not in (HeatmapOutcome.SUCCESS, HeatmapOutcome.NOT_APPLICABLE):
        return result
    if result.asset is None or not result.asset.player_count:
        return result
    return replace(result, player_keys=tuple(json.loads(body)["players"]))


def _parse_heatmap(body: bytes, metadata: AssetMetadata) -> ParseResult:
    """Project a hash-verified decoded raw body atomically, without I/O.

    Structural validation precedes numeric projection across *all* players.
    Hash mismatch is unknown (raw identity uncertainty), not a source defect.
    """
    if not isinstance(body, bytes) or hashlib.sha256(body).hexdigest() != metadata.content_hash:
        return ParseResult(HeatmapOutcome.UNKNOWN)
    try:
        reference = metadata.reference
        expected = build_heatmap_reference(reference.match_id, reference.heatmap_url)
        if expected != reference:
            raise ValueError("wrapper identity mismatch")
        modified, view_box, dimensions, player_count, circles = _structure(body)
    except (ValueError, TypeError, ET.ParseError, UnicodeError, OverflowError, RecursionError):
        return ParseResult(HeatmapOutcome.UNEXPECTED_FORMAT, hash_verified=True)
    points: list[HeatmapPointRow] = []
    try:
        for player_key, player_id, index, x, y, radius in circles:
            cx, cy, r = float(x), float(y), float(radius)
            if not all(math.isfinite(value) for value in (cx, cy, r)):
                raise ValueError("nonfinite circle coordinate")
            points.append(HeatmapPointRow(
                competition_id=metadata.competition_id,
                source_season_key=metadata.source_season_key,
                match_id=reference.match_id,
                player_key=player_key,
                player_id=player_id,
                point_index=index,
                cx=cx, cy=cy, r=r,
                view_box=view_box,
                asset_content_hash=metadata.content_hash,
            ))
    except (ValueError, OverflowError):
        return ParseResult(
            HeatmapOutcome.PARSE_FAILED, hash_verified=True, decoded=True
        )
    outcome = HeatmapOutcome.SUCCESS if points else HeatmapOutcome.NOT_APPLICABLE
    asset = HeatmapAssetRow(
        competition_id=metadata.competition_id,
        source_season_key=metadata.source_season_key,
        match_id=reference.match_id,
        heatmap_url=reference.heatmap_url,
        canonical_url=reference.target.canonical_url,
        target_key=reference.target.target_key,
        observed_at=metadata.observed_at,
        source_last_modified=modified,
        http_last_modified=metadata.http_last_modified,
        content_hash=metadata.content_hash,
        raw_uri=metadata.raw_uri,
        view_box=view_box,
        min_x=dimensions[0], min_y=dimensions[1],
        width=dimensions[2], height=dimensions[3],
        player_count=player_count,
        circle_count=len(points),
        outcome=outcome,
    )
    return ParseResult(outcome, asset, tuple(points), hash_verified=True, decoded=True)


@dataclass(frozen=True)
class HeatmapObservation:
    """One terminal match observation in one run; flags are evidence, not guesses.

    attempted means the wrapper was actually submitted to transport/CAS.
    fetch_origin describes a hash-verified body, not merely a network attempt.
    A URL marker can have has_url=True without target_key or attempted=True.
    """
    run_id: str
    match_id: int
    outcome: HeatmapOutcome
    has_url: bool
    target_key: str | None = None
    attempted: bool = False
    content_hash: str | None = None
    hash_verified: bool = False
    fetch_origin: FetchOrigin | None = None
    decoded: bool = False
    typed_rows: int = 0


def observation_from_parse(
    run_id: str, metadata: AssetMetadata, parsed: ParseResult, origin: FetchOrigin
) -> HeatmapObservation:
    """Make report evidence from a parser result and explicit acquisition origin."""
    return HeatmapObservation(
        run_id=run_id,
        match_id=metadata.reference.match_id,
        outcome=parsed.outcome,
        has_url=True,
        target_key=metadata.reference.target.target_key,
        attempted=True,
        content_hash=metadata.content_hash,
        hash_verified=parsed.hash_verified,
        fetch_origin=origin if parsed.hash_verified else None,
        decoded=parsed.decoded,
        typed_rows=len(parsed.points),
    )


def _validate_observation(item: HeatmapObservation) -> None:
    if not isinstance(item, HeatmapObservation):
        raise ValueError("report requires typed observations")
    if not isinstance(item.run_id, str) or not item.run_id.strip():
        raise ValueError("run_id must be nonempty")
    if type(item.match_id) is not int or item.match_id <= 0:
        raise ValueError("invalid report match identity")
    if not isinstance(item.outcome, HeatmapOutcome):
        raise ValueError("invalid outcome")
    for flag in (item.has_url, item.attempted, item.hash_verified, item.decoded):
        if type(flag) is not bool:
            raise ValueError("evidence flags must be boolean")
    for digest in (item.target_key, item.content_hash):
        if digest is not None and (
            not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise ValueError("invalid SHA-256 identity")
    if type(item.typed_rows) is not int or item.typed_rows < 0:
        raise ValueError("invalid typed row count")
    if item.target_key is not None and not item.has_url:
        raise ValueError("wrapper target requires a URL")
    if item.attempted and (not item.has_url or item.target_key is None):
        raise ValueError("attempt requires a validated wrapper target")
    if item.hash_verified:
        if not item.attempted or item.content_hash is None:
            raise ValueError("verified body requires an attempt and content hash")
        if not isinstance(item.fetch_origin, FetchOrigin):
            raise ValueError("verified body requires a network/replay origin")
    elif item.fetch_origin is not None:
        raise ValueError("acquisition origin requires a verified body")
    projected = {
        HeatmapOutcome.SUCCESS, HeatmapOutcome.NOT_APPLICABLE,
        HeatmapOutcome.PARSE_FAILED,
    }
    if item.decoded != (item.outcome in projected):
        raise ValueError("decoded evidence contradicts terminal outcome")
    if item.decoded and not item.hash_verified:
        raise ValueError("decoded structure requires a verified body")
    if (item.typed_rows > 0) != (item.outcome == HeatmapOutcome.SUCCESS):
        raise ValueError("only successful nonempty assets have typed rows")
    if item.outcome == HeatmapOutcome.URL_MISSING and (
        item.has_url or item.attempted or item.target_key or item.content_hash
    ):
        raise ValueError("url_missing contradicts reference evidence")
    if item.outcome == HeatmapOutcome.RESOURCE_UNAVAILABLE and not item.attempted:
        raise ValueError("source absence requires a scoped attempt")


def aggregate_heatmap_report(
    observations: Iterable[HeatmapObservation],
) -> dict[str, object]:
    """Return deterministic counters; reject mixed runs and conflicting evidence.

    Exact duplicates collapse by match. A second body/origin/outcome for that
    match is contradictory even when the terminal outcome happens to agree.
    """
    matches: dict[int, HeatmapObservation] = {}
    run_id: str | None = None
    target_owners: dict[str, int] = {}
    for item in observations:
        _validate_observation(item)
        if run_id is None:
            run_id = item.run_id
        elif item.run_id != run_id:
            raise ValueError("report must contain exactly one run")
        previous = matches.get(item.match_id)
        if previous is not None and previous != item:
            raise ValueError("contradictory duplicate match observation")
        matches[item.match_id] = item
        if item.target_key is not None:
            owner = target_owners.setdefault(item.target_key, item.match_id)
            if owner != item.match_id:
                raise ValueError("one wrapper target cannot belong to two matches")
    records = tuple(matches[key] for key in sorted(matches))
    outcomes = {outcome.value: 0 for outcome in HeatmapOutcome}
    for item in records:
        outcomes[item.outcome.value] += 1
    attempted = {item.target_key for item in records if item.attempted}
    fetched = {item.target_key for item in records if item.hash_verified}
    network = {
        item.target_key for item in records
        if item.hash_verified and item.fetch_origin == FetchOrigin.NETWORK
    }
    replayed = {
        item.target_key for item in records
        if item.hash_verified and item.fetch_origin == FetchOrigin.REPLAY
    }
    decoded = {item.target_key for item in records if item.decoded}
    typed = {
        item.target_key for item in records if item.outcome == HeatmapOutcome.SUCCESS
    }
    with_url = sum(item.has_url for item in records)
    if not (len(typed) <= len(decoded) <= len(fetched) <= len(attempted)
            <= with_url <= len(records)):
        raise ValueError("nonmonotonic report denominators")
    if network & replayed or network | replayed != fetched:
        raise ValueError("network/replay must partition fetched bodies")
    report: dict[str, object] = {
        "matches": len(records),
        "matches_with_url": with_url,
        "unique_assets": len(attempted),
        "fetched": len(fetched),
        "network_fetched": len(network),
        "replayed": len(replayed),
        "decoded": len(decoded),
        "typed": len(typed),
        "typed_rows": sum(item.typed_rows for item in records),
        "unavailable": outcomes[HeatmapOutcome.RESOURCE_UNAVAILABLE.value],
        **outcomes,
        "outcomes": outcomes,
    }
    return report


def _positive_identity_id(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        digits = str(value)
        if digits.isascii() and digits.isdecimal():
            return int(digits) or None
    except ValueError:
        # Python limits conversions between very long decimal strings and integers.
        pass
    return None


def _heatmap_player_index(
    parent: object,
    match_id: int,
    parent_content_hash: str | None,
    parent_raw_uri: str | None,
) -> tuple[HeatmapMatchStatus, str | None, dict[str, list[int | None]]]:
    if not all(isinstance(value, str) and value.strip() for value in (parent_content_hash, parent_raw_uri)):
        return HeatmapMatchStatus.UNVERIFIED, "parent.raw_provenance.missing", {}

    parent_data = parent if isinstance(parent, Mapping) else {}
    general = parent_data.get("general")
    parent_id = _positive_identity_id(general.get("matchId")) if isinstance(general, Mapping) else None
    expected_id = _positive_identity_id(match_id)
    if parent_id is None or expected_id is None:
        return HeatmapMatchStatus.UNVERIFIED, "parent.general.matchId.unverified", {}
    if parent_id != expected_id:
        return HeatmapMatchStatus.MISMATCH, "parent.general.matchId.mismatch", {}

    content = parent_data.get("content")
    stats = content.get("playerStats") if isinstance(content, Mapping) else None
    if not isinstance(stats, Mapping):
        return HeatmapMatchStatus.VERIFIED, "parent.playerStats.unexpected_format", {}

    index: dict[str, list[int | None]] = {}
    for record in stats.values():
        if not isinstance(record, Mapping):
            return HeatmapMatchStatus.VERIFIED, "parent.playerStats.unexpected_format", {}
        if "optaId" not in record:
            continue
        opta_id = record["optaId"]
        if _positive_identity_id(opta_id) is None:
            return HeatmapMatchStatus.VERIFIED, "parent.playerStats.unexpected_format", {}
        index.setdefault(str(opta_id), []).append(_positive_identity_id(record.get("id")))
    return HeatmapMatchStatus.VERIFIED, None, index


def resolve_heatmap_players(
    parent: object,
    match_id: int,
    player_keys: Iterable[str],
    metadata: AssetMetadata,
    *,
    heatmap_batch_id: str,
    parent_content_hash: str | None,
    parent_raw_uri: str | None,
) -> tuple[HeatmapPlayerIdentityRow, ...]:
    """Resolve source player keys against the Opta IDs in this match's parent snapshot."""
    match_status, parent_basis, index = _heatmap_player_index(
        parent, match_id, parent_content_hash, parent_raw_uri
    )
    rows: list[HeatmapPlayerIdentityRow] = []
    for player_key in player_keys:
        digits = player_key[1:] if isinstance(player_key, str) and player_key.startswith("p") else ""
        suffix = _positive_identity_id(digits)
        candidates = index.get(digits, [])
        ids = tuple(sorted(value for value in candidates if value is not None))

        status = HeatmapIdentityStatus.NOT_ATTEMPTED
        candidate_ids = None
        fotmob_id = None
        if parent_basis is not None:
            basis = parent_basis
        elif suffix is None:
            basis = "heatmap.player_key.invalid"
        elif len(ids) != len(candidates):
            basis = "playerStats.fotmobId.unverified"
        else:
            candidate_ids = ids
            if not ids:
                status, basis = HeatmapIdentityStatus.MISSING, "playerStats.optaId.absent"
            elif len(ids) == 1:
                status, basis = HeatmapIdentityStatus.MATCHED, "playerStats.optaId.unique"
                fotmob_id = ids[0]
            else:
                status, basis = HeatmapIdentityStatus.AMBIGUOUS, "playerStats.optaId.duplicate"

        rows.append(
            HeatmapPlayerIdentityRow(
                competition_id=metadata.competition_id,
                source_season_key=metadata.source_season_key,
                match_id=match_id,
                player_key=player_key,
                player_key_suffix=suffix,
                match_status=match_status,
                identity_status=status,
                candidate_count=len(candidate_ids) if candidate_ids is not None else None,
                fotmob_player_id=fotmob_id,
                identity_basis=basis,
                candidate_fotmob_player_ids_json=candidate_ids,
                heatmap_batch_id=heatmap_batch_id,
                parent_content_hash=parent_content_hash,
                parent_raw_uri=parent_raw_uri,
                asset_content_hash=metadata.content_hash,
            )
        )
    return tuple(rows)
