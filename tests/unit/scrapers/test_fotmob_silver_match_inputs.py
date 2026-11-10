"""Committed input selection must fail closed before any Silver writer runs."""
import csv
import hashlib
import io
import json
from dataclasses import FrozenInstanceError

import pytest

from scrapers.fotmob.silver_matches.inputs import (
    InputValidationError,
    load_input_set,
    validate_input_bytes,
)


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    csv.writer(stream).writerows(rows)
    return stream.getvalue().encode()


def sample(match_id="101"):
    payload = [
        "42", "2026/2027", match_id, "batch-one", "2026-10-21 12:00:00.000000",
        "a" * 64, "s3://football/raw/test.json.gz", "fotmob-native-v2",
        '{"matchFacts":true}', "[]", '{"general":{"matchId":' + match_id + '}}',
    ]
    manifest = [
        "match", payload[0], payload[1], payload[2], payload[3], "success", "success", "false",
        payload[4], "2026-10-21 12:00:01.000000", payload[5], payload[6], payload[7],
        '{"match_payloads":1}',
        '{"available_sections":{"matchFacts":true},"missing_expected_sections":[]}',
        "", "observation-one",
    ]
    return payload, manifest


def test_receipt_pins_exact_bytes_and_distinguishes_raw_claim_from_text_hash():
    payload, manifest = sample()
    p, m = csv_bytes([payload]), csv_bytes([manifest])
    selected = validate_input_bytes(p, m)
    assert selected.records == (tuple(payload),)
    item = selected.inputs[0]
    assert item.context == (42, "2026/2027", 101)
    assert item.context_key == '[42,"2026/2027",101]'
    assert item.raw_payload_sha256_claim == "a" * 64
    assert item.payload_text_sha256 == hashlib.sha256(payload[10].encode()).hexdigest()
    assert item.raw_payload_sha256_claim != item.payload_text_sha256
    assert item.observed_at_raw == payload[4]
    assert selected.payload_csv_sha256 == hashlib.sha256(p).hexdigest()
    assert selected.manifest_csv_sha256 == hashlib.sha256(m).hexdigest()
    assert selected.fingerprint == validate_input_bytes(p, m).fingerprint
    assert selected.receipt()["matches"][0]["manifest_observation_ids"] == ["observation-one"]
    with pytest.raises(FrozenInstanceError):
        item.record = tuple()


def test_loader_reads_once_and_does_not_follow_mutated_file(tmp_path):
    payload, manifest = sample()
    p, m = tmp_path / "payload.csv", tmp_path / "manifest.csv"
    p.write_bytes(csv_bytes([payload]))
    m.write_bytes(csv_bytes([manifest]))
    selected = load_input_set(p, m)
    payload[10] = '{"changed":true}'
    p.write_bytes(csv_bytes([payload]))
    assert "changed" not in selected.inputs[0].payload_json
    assert load_input_set(p, m).fingerprint != selected.fingerprint


@pytest.mark.parametrize("column,value", [
    (5, "failed"), (6, "not_available"), (7, "true"), (7, "False"), (7, ""),
    (10, "b" * 64), (11, "s3://football/another.json.gz"), (12, "other-parser"),
    (8, "2026-10-20 12:00:00.000000"), (9, ""), (15, "parse_error"),
    (13, '{"match_payloads":0}'), (13, '{"match_payloads":2}'),
    (13, '{"match_payloads":true}'), (13, "[]"),
    (14, '{"available_sections":{"matchFacts":true}}'),
    (14, '{"available_sections":{"matchFacts":false},"missing_expected_sections":[]}'),
    (14, '{"available_sections":{"matchFacts":1},"missing_expected_sections":[]}'),
])
def test_rejects_noncommitted_or_conflicting_manifest(column, value):
    payload, manifest = sample()
    manifest[column] = value
    with pytest.raises(InputValidationError):
        validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest]))


@pytest.mark.parametrize("column,value", [(1, "43"), (2, "2025/2026"), (3, "102"), (4, "other-batch"), (0, "player")])
def test_exact_context_and_batch_required(column, value):
    payload, manifest = sample()
    manifest[column] = value
    with pytest.raises(InputValidationError, match="Orphan"):
        validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest]))


def test_success_observation_cannot_hide_conflicting_duplicate():
    payload, manifest = sample()
    conflicting = manifest.copy()
    conflicting[10] = "b" * 64
    with pytest.raises(InputValidationError, match="Conflicting manifest"):
        validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest, conflicting]))


def test_repeated_equivalent_observations_preserve_evidence_without_duplicate_input():
    payload, manifest = sample()
    repeated = manifest.copy()
    repeated[16] = "observation-two"
    selected = validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest, repeated, manifest]))
    assert len(selected.inputs) == 1
    assert len(selected.inputs[0].manifest_records) == 2


def test_rejects_two_selected_payloads_even_if_byte_identical():
    payload, manifest = sample()
    with pytest.raises(InputValidationError, match="Duplicate selected match"):
        validate_input_bytes(csv_bytes([payload, payload]), csv_bytes([manifest]))


def test_extra_historical_manifests_do_not_select_a_different_version():
    payload, manifest = sample()
    older = manifest.copy()
    older[4] = "older-batch"
    older[5] = "failed"
    selected = validate_input_bytes(csv_bytes([payload]), csv_bytes([older, manifest]))
    assert selected.inputs[0].source_batch_id == "batch-one"


@pytest.mark.parametrize("column,value", [
    (0, "0"), (2, "1.0"), (1, " 2026/2027"), (3, ""), (7, ""),
    (5, "not-a-hash"), (6, "https://example.com/raw.json"),
    (10, "null"), (10, "{}"), (10, '{"x":NaN}'), (10, '{"x":1e999}'), (10, '{"x":1,"x":2}'),
    (8, '{"matchFacts":1}'), (9, '["matchFacts"]'),
])
def test_rejects_malformed_or_incomplete_payload_metadata(column, value):
    payload, manifest = sample()
    payload[column] = value
    with pytest.raises(InputValidationError):
        validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest]))


def test_large_json_fields_are_read_and_csv_global_limit_is_restored():
    payload, manifest = sample()
    payload[10] = json.dumps({"value": "x" * 200_000})
    previous = csv.field_size_limit()
    selected = validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest]))
    assert len(selected.inputs[0].payload_json) > 200_000
    assert csv.field_size_limit() == previous


@pytest.mark.parametrize("payload_bytes", [b"", b"x,y\n", b'"unterminated', b"\xff"])
def test_malformed_csv_rejected(payload_bytes):
    _, manifest = sample()
    with pytest.raises(InputValidationError):
        validate_input_bytes(payload_bytes, csv_bytes([manifest]))


def test_conflicting_counts_in_duplicate_manifest_are_not_ignored():
    payload, manifest = sample()
    other = manifest.copy()
    manifest[13] = '{"match_payloads":1,"field_inventory":2}'
    other[13] = '{"match_payloads":1,"field_inventory":3}'
    with pytest.raises(InputValidationError, match="Conflicting manifest batch evidence"):
        validate_input_bytes(csv_bytes([payload]), csv_bytes([manifest, other]))
