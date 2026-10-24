from __future__ import annotations

import base64
import json
from dataclasses import FrozenInstanceError

import pytest

from scrapers.sofascore.season_chunks import (
    SeasonChunkError,
    SeasonScheduleChunk,
    decode_season_chunk_unit,
    encode_season_chunk_unit,
)


def _chunk(**changes):
    values = dict(
        tournament_id="26947", season_id="92699", last=(50, 99), next=(0, 1),
        evidence_digest="a" * 64,
    )
    values.update(changes)
    return SeasonScheduleChunk(**values)


def _token(payload, *, canonical=True):
    body = json.dumps(payload, sort_keys=canonical, separators=(",", ":"))
    return "season-chunk-v1." + base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


def test_chunk_roundtrips_canonically_and_is_frozen():
    chunk = _chunk()
    token = encode_season_chunk_unit(chunk)
    assert decode_season_chunk_unit(token) == chunk
    assert encode_season_chunk_unit(decode_season_chunk_unit(token)) == token
    with pytest.raises(FrozenInstanceError):
        chunk.last = (100, 150)


@pytest.mark.parametrize("unit", ["17:76986", "26947:92699"])
def test_legacy_canonical_season_identity_is_explicitly_distinguished(unit):
    assert decode_season_chunk_unit(unit) is None


@pytest.mark.parametrize("changes", [
    {"last": (0, 51)}, {"last": (0, 50), "next": (0, 1)},
    {"last": (-1, 3)}, {"last": (2, 1)}, {"last": (False, 2)},
    {"last": (0, 1.0)}, {"last": [0, 2]}, {"next": (0,)},
    {"tournament_id": "017"}, {"season_id": "0"}, {"season_id": 92699},
    {"evidence_digest": "A" * 64}, {"evidence_digest": "a" * 63},
])
def test_invalid_chunk_values_fail_before_signing(changes):
    with pytest.raises(SeasonChunkError):
        _chunk(**changes)


@pytest.mark.parametrize("unit", [
    "", "17", "017:76986", "17:0", "17:76986:50", " 17:76986",
    "season-chunk-v2.abc", "season-chunk-v1.@@@", "season-chunk-v1.e30",
])
def test_malformed_and_unknown_units_fail_closed(unit):
    with pytest.raises(SeasonChunkError):
        decode_season_chunk_unit(unit)


@pytest.mark.parametrize("mutation", ["extra", "missing", "bool", "oversize", "duplicate"])
def test_decoder_checks_exact_payload_before_authorizing_a_chunk(mutation):
    payload = dict(tournament_id="17", season_id="76986", last=[0, 49], next=[0, 1],
                   evidence_digest="a" * 64)
    if mutation == "extra":
        payload["unsigned_hint"] = True
    elif mutation == "missing":
        del payload["next"]
    elif mutation == "bool":
        payload["last"][0] = False
    elif mutation == "oversize":
        payload["last"][1] = 50
    token = _token(payload)
    if mutation == "duplicate":
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        body = body[:-1] + ',"season_id":"76986"}'
        token = "season-chunk-v1." + base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")
    with pytest.raises(SeasonChunkError):
        decode_season_chunk_unit(token)


def test_decoder_rejects_noncanonical_wire_encoding():
    token = encode_season_chunk_unit(_chunk())
    with pytest.raises(SeasonChunkError):
        decode_season_chunk_unit(token + "=")


def test_closed_schedule_chunk_roundtrips_without_authorizing_any_page():
    chunk = _chunk(last=(51, 51), next=(0, 0))
    assert decode_season_chunk_unit(encode_season_chunk_unit(chunk)) == chunk
