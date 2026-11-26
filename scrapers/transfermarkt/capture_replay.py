"""Replay a bounded set of original Transfermarkt responses without fetching the site."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from .client import TransfermarktHttpClient
from .models import FetchOutcome, FetchStatus
from .raw_store import RawAttemptEnvelopeRecord, RawCaptureRecord, RawResponseStore
from .sections import parse_json_integer


_DENIED_HEADING = re.compile(
    r"<(?:title|h1)\b[^>]*>\s*(?:(?:403\s*[-:]?\s*)?(?:forbidden|access denied)|"
    r"human verification|verify (?:that )?you are human|captcha|request (?:blocked|rejected)|just a moment)\b",
    re.IGNORECASE,
)
_DENIED_MESSAGE = re.compile(
    r"\b(?:access denied|forbidden|human verification|captcha|challenge required|"
    r"verify (?:that )?you are human|too many requests|rate limited|request blocked)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _Response:
    envelope: RawAttemptEnvelopeRecord
    record: RawCaptureRecord
    body: bytes


def _check_body(body, record):
    text = body.decode("utf-8-sig")
    detector = TransfermarktHttpClient
    if (detector._looks_like_html_challenge(text) or detector._looks_like_aws_waf_challenge(text)
            or _DENIED_HEADING.search(text) or text.strip().lower() in {"forbidden", "access denied"}):
        raise ValueError("capture replay contains a denied or challenge page")
    if not text.strip() and record.status_code == 200:
        raise ValueError("capture replay contains an empty HTTP 200 response")
    if text.lstrip().startswith("{"):
        value = json.loads(text, parse_int=parse_json_integer)
        errors = [value[key] for key in ("error", "errors", "message", "detail") if value.get(key)]
        if (detector._looks_like_json_challenge(value) or _DENIED_MESSAGE.search(json.dumps(errors))
                or any(str(value.get(key)) in {"403", "429"} for key in ("code", "status", "statusCode"))):
            raise ValueError("capture replay contains a denied or challenge response")


class CaptureReplay:
    """A single CoreRun's verified response set. Only fetch consumes a target."""

    def __init__(self, pages, scope_id, cycle_id):
        self._pages = pages
        self._by_capture = {page.record.capture_id: page for page in pages.values()}
        self._scope_id = scope_id
        self._cycle_id = cycle_id
        self._used = set()
        self._stopped = False

    @property
    def fetched_at(self) -> tuple[str, ...]:
        """Return original timezone-aware ISO clocks in envelope input order."""
        return tuple(page.record.fetched_at for page in self._pages.values())

    def _stop(self, reason):
        self._stopped = True
        raise ValueError(reason)

    def load(self, capture_id, url):
        """Raw loader for CoreRun. The ID and exact requested URL must both match."""
        if self._stopped:
            raise ValueError("capture replay stopped")
        page = self._by_capture.get(capture_id)
        if page is None or page.record.url != url:
            self._stop("capture replay ID and URL do not match")
        return page.body, page.record

    def fetch(self, url, *, label, as_json, context=None, validator=None):
        if self._stopped:
            raise ValueError("capture replay stopped")
        if (not isinstance(context, Mapping) or context.get("scope_id") != self._scope_id
                or context.get("cycle_id") != self._cycle_id):
            self._stop("capture replay request scope or cycle differs")
        key = (label, url)
        page = self._pages.get(key)
        if page is None or key in self._used:
            self._stop("capture replay request is missing or repeated")
        record = page.record
        evidence = dict(
            label=label, context=dict(context), attempts=1, cache_hit=True, decoded_body_bytes=len(page.body),
            raw_capture_id=record.capture_id, raw_body_hash=record.content_hash, raw_fetched_at=record.fetched_at,
            raw_uri=record.raw_uri, raw_attempt_envelope_id=page.envelope.envelope_id,
            raw_attempt_envelope_ids=(page.envelope.envelope_id,), status_code=record.status_code,
        )
        if record.status_code == 404:
            # CoreRun requires the matching HTTP 200 club profile to prove an empty stadium.
            result = FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error="http_404", **evidence)
        else:
            try:
                value = TransfermarktHttpClient._decode_body(page.body, as_json=as_json)
            except ValueError:
                self._stop("capture replay response cannot be decoded")
            if TransfermarktHttpClient._validator_problem(validator, value):
                self._stop("capture replay endpoint validation failed")
            result = FetchOutcome(status=FetchStatus.OK, value=value, **evidence)
        self._used.add(key)
        return result

    def assert_consumed(self):
        """Require all original responses to have served the declared run."""
        if self._stopped or self._used != self._pages.keys():
            raise ValueError(f"capture replay incomplete ({len(self._used)}/{len(self._pages)} targets)")


def capture_replay(store: RawResponseStore, envelope_ids: Sequence[str], *, scope_id: str, cycle_id: str) -> CaptureReplay:
    """Validate every original before exposing a fetch or accepting any writes."""
    if not all(isinstance(value, str) and value and value.strip() == value for value in (scope_id, cycle_id)):
        raise ValueError("capture replay requires an exact scope and cycle")
    if not isinstance(envelope_ids, (list, tuple)) or not envelope_ids or not all(
        isinstance(item, str) for item in envelope_ids
    ):
        raise ValueError("capture replay requires a nonempty list of envelope IDs")
    if len(set(envelope_ids)) != len(envelope_ids):
        raise ValueError("capture replay contains a repeated envelope ID")
    pages = {}
    for envelope_id in envelope_ids:
        envelope = store.load_attempt_envelope(envelope_id)
        if (envelope.envelope_id != envelope_id or envelope.scope_id != scope_id or envelope.cycle_id != cycle_id
                or envelope.outcome_kind != "response" or envelope.capture_id is None):
            raise ValueError("capture replay requires a response envelope for the requested scope and cycle")
        if envelope.status_code != 200 and not (envelope.endpoint == "club_stadium" and envelope.status_code == 404):
            raise ValueError(f"capture replay stopped on HTTP {envelope.status_code}")
        body, record = store.load_capture(envelope.capture_id)
        # Both loaders verify immutable IDs and body SHA. Bind their independently read records as well.
        common = ("source", "capture_id", "url", "status_code", "scope_id", "cycle_id", "endpoint", "attempt")
        if (record.content_hash != envelope.raw_body_hash or record.fetched_at != envelope.observed_at
                or any(getattr(record, name) != getattr(envelope, name) for name in common)):
            raise ValueError("capture replay envelope and source evidence differ")
        key = (record.endpoint, record.url)
        if key in pages:
            raise ValueError("capture replay has multiple originals for one target")
        _check_body(body, record)
        pages[key] = _Response(envelope, record, body)
    for page in pages.values():
        if page.record.status_code == 404:
            profile = page.record.url.removesuffix("/stadium")
            if profile == page.record.url or ("club_profile", profile) not in pages:
                raise ValueError("capture replay stadium 404 requires its club profile")
    return CaptureReplay(pages, scope_id, cycle_id)


def read_input_json(data):
    def unique_keys(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError('Duplicate JSON key in Transfermarkt cycle input')
            obj[key] = value
        return obj

    return json.loads(data, object_pairs_hook=unique_keys)


def _attempt_entries(document):
    if isinstance(document, dict) and "raw_attempts" in document:
        entries = document["raw_attempts"]
        if not isinstance(entries, list):
            raise ValueError("Invalid raw_attempts journal")
        if any(document.get(field) for field in (
            "error", "errors", "failures", "failed_attempts", "failure_kind", "failure_stage", "check_failures", "cache_sources",
        )) or ("status" in document and document["status"] not in ("complete", "completed", "success", "ok")):
            raise ValueError("Bounded collection reported a failure or used prior-cycle evidence")
        if "network_fetches" in document and (
            type(document["network_fetches"]) is not int or document["network_fetches"] != len(entries)
        ):
            raise ValueError("Journal does not account for every reported network attempt")
    else:
        entries = document if isinstance(document, list) else [document]
    if not entries:
        raise ValueError("Empty bounded attempt journal")
    return entries


def verify_attempt_journals(store, cycle_id, scope_ids, journals, traffic):
    """Reconstruct all captured attempts from the originals and an independent run-specific counter."""
    if (not isinstance(traffic, dict) or not (traffic.keys() & {"cycle_id", "run_id"})
            or any(traffic[key] != cycle_id for key in ("cycle_id", "run_id") if key in traffic)
            or "raw_attempts" in traffic):
        raise ValueError("Traffic receipt must identify this exact Airflow run and come from its separate counter")
    expected_count = traffic.get("request_attempts")
    if type(expected_count) is not int or expected_count <= 0:
        raise ValueError("Traffic receipt requires a positive integer request_attempts")
    by_scope = {scope_id: set() for scope_id in scope_ids}
    ordinals = Counter()
    for journal in journals:
        for entry in _attempt_entries(journal):
            envelope_id = entry.get("envelope_id") if isinstance(entry, dict) else entry
            if not isinstance(envelope_id, str):
                raise ValueError("Expected a raw attempt record or envelope ID")
            envelope = store.load_attempt_envelope(envelope_id)
            if isinstance(entry, dict) and json.dumps(entry, sort_keys=True) != json.dumps(asdict(envelope), sort_keys=True):
                raise ValueError("Journal record differs from its immutable raw envelope")
            if envelope.cycle_id != cycle_id or envelope.scope_id not in by_scope:
                raise ValueError("Original attempt belongs to another cycle or an unplanned scope")
            if envelope.outcome_kind != "response":
                raise ValueError(f"Original attempt recorded a transport error: {envelope.error_kind}")
            ids = by_scope[envelope.scope_id]
            if envelope_id in ids:
                raise ValueError("Repeated raw attempt in the submitted journal")
            ids.add(envelope_id)
            ordinals[envelope.attempt] += 1
    if sum(ordinals.values()) != expected_count:
        raise ValueError("Raw journal does not account for every request_attempts entry in the traffic receipt")
    # Concurrent clients may each start at one. Every later ordinal needs a distinct predecessor.
    if any(i < 1 or (i > 1 and count > ordinals[i - 1]) for i, count in ordinals.items()):
        raise ValueError("Missing earlier raw attempt ordinal")
    envelopes = {key: sorted(ids) for key, ids in by_scope.items()}
    if not all(envelopes.values()):
        raise ValueError("Missing originals for a planned scope")
    return envelopes
