"""Replay a bounded set of original Transfermarkt responses without fetching the site."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

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
