"""Synthetic contract tests: no source requests or production fixtures."""

import hashlib
import json
import math
from dataclasses import replace

import pytest
import requests

from scrapers.fotmob.heatmaps import (
    AssetMetadata,
    COORDINATE_SYSTEM,
    HEATMAP_IDENTITY_RESOLVER_VERSION,
    FetchOrigin,
    HeatmapIdentityStatus,
    HeatmapMatchStatus,
    HeatmapObservation,
    HeatmapOutcome as Outcome,
    aggregate_heatmap_report,
    build_heatmap_reference,
    classify_heatmap_fetch,
    extract_heatmap_reference,
    observation_from_parse,
    parse_heatmap,
    resolve_heatmap_players,
)
from scrapers.fotmob.raw_store import FotMobRawStore
from scrapers.fotmob.transport import FetchOutcome, FotMobTransport, canonicalize_target
from tests.unit.scrapers.test_fotmob_transport import FakeResponse, _transport


SOURCE = "https://pub.fotmob.com/prod/db/api/heatmap/match/101"
OBSERVED = "2026-01-02T03:04:05+00:00"
MODIFIED = "synthetic-source-version-1"
CIRCLE = '<circle cx="52.6" cy="33.9" r="7.5"/>'


def payload(players=None, view_box="0 0 105 68"):
    return {
        "template": (
            '<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="{view_box}"><path d="M0 0 L105 68"/></svg>'
        ),
        "players": {"p20": CIRCLE} if players is None else players,
        "lastModified": MODIFIED,
    }


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def metadata(body, match_id=101, **changes):
    result = AssetMetadata(
        reference=build_heatmap_reference(
            match_id, f"https://pub.fotmob.com/prod/db/api/heatmap/match/{match_id}"
        ),
        observed_at=OBSERVED,
        content_hash=hashlib.sha256(body).hexdigest(),
        raw_uri="file:///synthetic/immutable-body.json",
        competition_id=7,
        source_season_key="2025-2026",
        http_last_modified="Fri, 02 Jan 2026 02:00:00 GMT",
    )
    return replace(result, **changes)


def parse(value):
    body = encode(value)
    return parse_heatmap(body, metadata(body))


def assert_no_rows(result, outcome):
    assert result.outcome == outcome
    assert result.asset is None
    assert result.points == ()


def identity_parent(players, match_id=101):
    return {
        "general": {"matchId": match_id},
        "content": {"playerStats": players},
    }


def identity_rows(parent, player_keys=("p20",), match_id=101, **changes):
    body = encode(payload())
    provenance = {
        "heatmap_batch_id": "fm1-heatmap-test",
        "parent_content_hash": "a" * 64,
        "parent_raw_uri": "file:///synthetic/parent.json",
    }
    provenance.update(changes)
    return resolve_heatmap_players(
        parent,
        match_id,
        player_keys,
        metadata(body, match_id=match_id),
        **provenance,
    )


def test_happy_path_preserves_metadata_scope_and_view_box():
    body = encode(payload(view_box=" -5.5,  2,105,68 "))
    meta = metadata(body)
    result = parse_heatmap(body, meta)
    assert result.outcome == Outcome.SUCCESS
    assert result.hash_verified and result.decoded
    asset = result.asset
    assert asset is not None
    assert asset.match_id == 101
    assert asset.heatmap_url == SOURCE
    assert asset.canonical_url == meta.reference.target.canonical_url
    assert asset.target_key == meta.reference.target.target_key
    assert asset.observed_at == OBSERVED
    assert asset.source_last_modified == MODIFIED
    assert asset.http_last_modified == meta.http_last_modified
    assert asset.raw_uri == meta.raw_uri
    assert asset.content_hash == hashlib.sha256(body).hexdigest()
    assert asset.view_box == " -5.5,  2,105,68 "
    assert (asset.min_x, asset.min_y, asset.width, asset.height) == (-5.5, 2, 105, 68)
    assert (asset.player_count, asset.circle_count) == (1, 1)
    assert asset.outcome == Outcome.SUCCESS
    assert asset.coordinate_system == "svg_view_box_user_units"
    assert asset.orientation == asset.radius_semantics == "unknown"
    point, = result.points
    assert (point.match_id, point.player_key, point.player_id, point.point_index) == (
        101, "p20", 20, 0
    )
    assert (point.cx, point.cy, point.r) == (52.6, 33.9, 7.5)
    assert all(math.isfinite(value) for value in (point.cx, point.cy, point.r))
    assert point.view_box == asset.view_box
    assert point.coordinate_system == COORDINATE_SYSTEM
    assert point.asset_content_hash == asset.content_hash
    for row in (asset, point):
        assert row.competition_id == 7
        assert row.source_season_key == "2025-2026"


def test_source_order_duplicate_circles_and_unbounded_coordinates():
    outside = '<circle cx="-100" cy="999" r="-2"/>'
    result = parse(payload({"p20": CIRCLE + CIRCLE, "p3": outside + CIRCLE}))
    assert [(p.player_key, p.point_index) for p in result.points] == [
        ("p20", 0), ("p20", 1), ("p3", 0), ("p3", 1)
    ]
    assert replace(result.points[0], point_index=1) == result.points[1]
    assert (result.points[2].cx, result.points[2].cy, result.points[2].r) == (-100, 999, -2)
    assert result.asset.player_count == 2
    assert result.asset.circle_count == 4


@pytest.mark.parametrize("view_box", [" 0\t0\r\n105 68 ", "0&#32;0 105 68"])
def test_view_box_preserves_source_spelling_before_xml_normalization(view_box):
    data = payload()
    data["template"] = (
        '<?xml version="1.0"?><!-- <svg viewBox="9 9 9 9"/> -->'
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'data-note=\'viewBox="1 2 3 4"\' '
        f"viewBox='{view_box}'/>"
    )
    result = parse(data)
    assert result.outcome == Outcome.SUCCESS
    assert result.asset.view_box == result.points[0].view_box == view_box
    assert (result.asset.min_x, result.asset.min_y, result.asset.width, result.asset.height) == (
        0, 0, 105, 68
    )


def test_optional_scope_is_consistent_when_absent():
    body = encode(payload())
    result = parse_heatmap(body, metadata(body, competition_id=None, source_season_key=None))
    for row in (result.asset, *result.points):
        assert row.competition_id is None
        assert row.source_season_key is None


def test_identity_match_uses_opta_id_not_fotmob_id():
    parent = identity_parent({
        "200": {"id": 200, "optaId": "20", "name": "Opta match"},
        "20": {"id": 20, "optaId": "999", "name": "FotMob collision"},
    })
    row, = identity_rows(parent)
    assert row.match_status == HeatmapMatchStatus.VERIFIED
    assert row.identity_status == HeatmapIdentityStatus.MATCHED
    assert row.player_key == "p20"
    assert row.player_key_suffix == 20
    assert row.candidate_count == 1
    assert row.fotmob_player_id == 200
    assert row.identity_basis == "playerStats.optaId.unique"
    assert row.resolver_version == HEATMAP_IDENTITY_RESOLVER_VERSION
    assert row.candidate_fotmob_player_ids_json == (200,)
    assert row.heatmap_batch_id == "fm1-heatmap-test"
    assert row.parent_content_hash == "a" * 64
    assert row.parent_raw_uri == "file:///synthetic/parent.json"


def test_identity_missing_does_not_fallback_to_fotmob_id():
    row, = identity_rows(identity_parent({"20": {"id": 20, "optaId": "999"}}))
    assert row.match_status == HeatmapMatchStatus.VERIFIED
    assert row.identity_status == HeatmapIdentityStatus.MISSING
    assert row.candidate_count == 0
    assert row.candidate_fotmob_player_ids_json == ()
    assert row.fotmob_player_id is None
    assert row.identity_basis == "playerStats.optaId.absent"


def test_identity_duplicate_opta_id_is_ambiguous_and_deterministic():
    first = identity_parent({
        "200": {"id": 200, "optaId": "20"},
        "201": {"id": 201, "optaId": "20"},
    })
    second = identity_parent(dict(reversed(list(first["content"]["playerStats"].items()))))
    expected = identity_rows(first)
    assert identity_rows(second) == expected
    row, = expected
    assert row.identity_status == HeatmapIdentityStatus.AMBIGUOUS
    assert row.candidate_count == 2
    assert row.candidate_fotmob_player_ids_json == (200, 201)
    assert row.fotmob_player_id is None
    assert row.identity_basis == "playerStats.optaId.duplicate"


@pytest.mark.parametrize("parent_id", [102, None, "invalid"])
def test_identity_match_mismatch_or_unverified_is_not_attempted(parent_id):
    row, = identity_rows(identity_parent({"200": {"id": 200, "optaId": "20"}}, parent_id))
    expected = (
        HeatmapMatchStatus.MISMATCH if parent_id == 102
        else HeatmapMatchStatus.UNVERIFIED
    )
    assert row.match_status == expected
    assert row.identity_status == HeatmapIdentityStatus.NOT_ATTEMPTED
    assert row.candidate_count is None
    assert row.candidate_fotmob_player_ids_json is None
    assert row.fotmob_player_id is None


def test_identity_malformed_stats_and_provenance_are_not_attempted():
    malformed, = identity_rows({"general": {"matchId": 101}, "content": {"playerStats": []}})
    assert malformed.match_status == HeatmapMatchStatus.VERIFIED
    assert malformed.identity_status == HeatmapIdentityStatus.NOT_ATTEMPTED
    assert malformed.identity_basis == "parent.playerStats.unexpected_format"
    missing, = identity_rows(
        identity_parent({"200": {"id": 200, "optaId": "20"}}),
        parent_content_hash=None,
    )
    assert missing.match_status == HeatmapMatchStatus.UNVERIFIED
    assert missing.identity_status == HeatmapIdentityStatus.NOT_ATTEMPTED
    assert missing.identity_basis == "parent.raw_provenance.missing"
    assert missing.candidate_fotmob_player_ids_json is None


def test_identity_missing_opta_and_invalid_fotmob_id_are_not_guessed():
    missing, = identity_rows(identity_parent({"200": {"id": 200}}))
    assert missing.identity_status == HeatmapIdentityStatus.MISSING
    invalid, = identity_rows(identity_parent({"200": {"id": None, "optaId": "20"}}))
    assert invalid.identity_status == HeatmapIdentityStatus.NOT_ATTEMPTED
    assert invalid.identity_basis == "playerStats.fotmobId.unverified"
    assert invalid.candidate_count is None
    assert invalid.candidate_fotmob_player_ids_json is None
    assert invalid.fotmob_player_id is None


def test_identity_leading_zeroes_are_significant():
    matched, = identity_rows(
        identity_parent({"200": {"id": 200, "optaId": "00020"}}),
        player_keys=("p00020",),
    )
    assert matched.player_key == "p00020"
    assert matched.player_key_suffix == 20
    assert matched.identity_status == HeatmapIdentityStatus.MATCHED
    missing, = identity_rows(
        identity_parent({"200": {"id": 200, "optaId": "20"}}),
        player_keys=("p00020",),
    )
    assert missing.player_key == "p00020"
    assert missing.identity_status == HeatmapIdentityStatus.MISSING


def test_player_keys_include_zero_circle_players_without_changing_points():
    result = parse(payload({"p20": "", "p3": CIRCLE}))
    assert result.outcome == Outcome.SUCCESS
    assert result.player_keys == ("p20", "p3")
    assert [point.player_key for point in result.points] == ["p3"]
    empty = parse(payload({"p20": "", "p3": " \n\t "}))
    assert empty.outcome == Outcome.NOT_APPLICABLE
    assert empty.player_keys == ("p20", "p3")
    assert empty.points == ()


@pytest.mark.parametrize("content", [{}, {"heatmapUrl": None}, {"heatmapUrl": ""}])
def test_missing_reference(content):
    result = extract_heatmap_reference({"general": {"matchId": "101"}, "content": content}, 101)
    assert result.outcome == Outcome.URL_MISSING
    assert result.reference is None


def test_reference_checks_identity_and_uses_verbatim_wrapper_parameter():
    for source in (SOURCE, SOURCE.replace(".com/", ".com:443/")):
        result = extract_heatmap_reference(
            {"general": {"matchId": 101}, "content": {"heatmapUrl": source}}, 101
        )
        assert result.outcome is None
        assert result.reference.heatmap_url == source
        assert result.reference.target == canonicalize_target(
            "heatmap/match/101/heatmaps", {"heatmapUrl": source}
        )
    assert extract_heatmap_reference({"general": {"matchId": 101}}, 101).outcome == Outcome.URL_MISSING


def test_reference_accepts_exact_relative_wrapper():
    source = (
        "/api/data/heatmap/match/101/heatmaps?heatmapUrl="
        + SOURCE.replace("https://", "https%3A%2F%2F")
    )
    result = extract_heatmap_reference(
        {"general": {"matchId": "101"}, "content": {"heatmapUrl": source}}, 101
    )
    assert result.outcome is None
    assert result.reference.heatmap_url == SOURCE
    assert result.reference.target == canonicalize_target(source)
    assert result.reference.target == canonicalize_target(
        "heatmap/match/101/heatmaps", {"heatmapUrl": SOURCE}
    )
    for bad in (
        source.replace("/match/101/", "/match/102/"),
        source.replace(
            "/prod/db/api/heatmap/match/101", "/prod/db/api/heatmap/match/102"
        ),
        source + "&x=1",
        source + "#part",
        source.replace("?heatmapUrl=", "?x="),
        source + "&heatmapUrl=" + SOURCE,
        "https://www.fotmob.com" + source,
    ):
        invalid = extract_heatmap_reference(
            {"general": {"matchId": 101}, "content": {"heatmapUrl": bad}}, 101
        )
        assert invalid.outcome == Outcome.UNEXPECTED_FORMAT
        assert invalid.reference is None


@pytest.mark.parametrize("source", [
    123, [], {}, " ", SOURCE.replace("https:", "http:"),
    SOURCE.replace("pub.", "www."), SOURCE.replace(".com/", ".com:444/"),
    SOURCE.replace("pub.", "user@pub."), SOURCE + "?x=1", SOURCE + "#part",
    SOURCE + "?", SOURCE + "#", SOURCE + "/", SOURCE + "\n",
    SOURCE.replace("101", "102"), SOURCE.replace("101", "0"),
    SOURCE.replace("101", "0101"), SOURCE.replace("101", "%31%30%31"),
    SOURCE.replace("pub.", "pub.evil."), SOURCE.replace("/prod/", "/prod/../prod/"),
])
def test_bad_source_url(source):
    result = extract_heatmap_reference(
        {"general": {"matchId": 101}, "content": {"heatmapUrl": source}}, 101
    )
    assert result.outcome == Outcome.UNEXPECTED_FORMAT
    assert result.reference is None


@pytest.mark.parametrize("parent", [
    None, [], {}, {"general": None}, {"general": {"matchId": 102}},
    {"general": {"matchId": True}}, {"general": {"matchId": 101.0}},
    {"general": {"matchId": 101}, "content": []},
    {"general": {"matchId": 101}, "content": None},
])
def test_invalid_parent_and_mismatch_fail_before_missing_url(parent):
    assert extract_heatmap_reference(parent, 101).outcome == Outcome.UNEXPECTED_FORMAT


@pytest.mark.parametrize("value", [None, [], {}, {"template": "<svg/>"}])
def test_unexpected_top_level(value):
    assert_no_rows(parse(value), Outcome.UNEXPECTED_FORMAT)


@pytest.mark.parametrize("field,value", [
    ("extra", 1), ("template", {}), ("template", "<svg>"),
    ("template", '<html viewBox="0 0 105 68"/>'),
    ("template", "<svg/>"),
    ("template", '<!DOCTYPE svg [<!ENTITY x "text">]><svg viewBox="0 0 105 68"/>'),
    ("players", []), ("players", None), ("players", {"p0": CIRCLE}),
    ("players", {"p-1": CIRCLE}), ("players", {"20": CIRCLE}),
    ("players", {"p١": CIRCLE}), ("players", {"p20": None}),
    ("players", {"p20": [CIRCLE]}),
    ("lastModified", None), ("lastModified", ""), ("lastModified", "   "),
])
def test_invalid_shape_fields(field, value):
    data = payload()
    data[field] = value
    result = parse(data)
    assert_no_rows(result, Outcome.UNEXPECTED_FORMAT)
    assert result.hash_verified and not result.decoded


@pytest.mark.parametrize("field", ["template", "players", "lastModified"])
def test_missing_required_top_level_field(field):
    data = payload()
    del data[field]
    assert_no_rows(parse(data), Outcome.UNEXPECTED_FORMAT)


@pytest.mark.parametrize("view_box", [
    "", "0 0 105", "0 0 105 68 1", "0 0 0 68", "0 0 105 -1",
    "NaN 0 105 68", "0 0 inf 68", "0 0 1e999 68", "0 0 wide 68",
    ",0 0 105 68", "0,,0,105,68", "0 0 105 68,",
])
def test_bad_view_box_is_structural_failure(view_box):
    assert_no_rows(parse(payload(view_box=view_box)), Outcome.UNEXPECTED_FORMAT)


@pytest.mark.parametrize("fragment", [
    "<circle", "<rect/>", CIRCLE + "residue", "residue" + CIRCLE,
    '<circle cx="1" cy="2" r="3">text</circle>',
    '<circle cx="1" cy="2" r="3"><circle cx="1" cy="2" r="3"/></circle>',
    '<circle cx="1" cy="2"/>', '<circle cx="1" cy="2" r="3" fill="red"/>',
    '<circle cx="1" cx="2" cy="2" r="3"/>',
    CIRCLE + "<!-- residue -->", CIRCLE + "<?residue yes?>",
    '<g>' + CIRCLE + '</g>', '<circle xmlns="urn:foreign" cx="1" cy="2" r="3"/>',
    '<!DOCTYPE x>' + CIRCLE, '</heatmap-fragment>' + CIRCLE,
])
def test_fragment_residue_is_rejected(fragment):
    assert_no_rows(parse(payload({"p20": CIRCLE, "p3": fragment})), Outcome.UNEXPECTED_FORMAT)


@pytest.mark.parametrize("body", [
    b"not-json", b"\xff", b'{"template": NaN}',
    b'{"template":"x","template":"y","players":{},"lastModified":"z"}',
])
def test_invalid_json_and_duplicate_keys(body):
    assert_no_rows(parse_heatmap(body, metadata(body)), Outcome.UNEXPECTED_FORMAT)


@pytest.mark.parametrize("players", [{}, {"p20": "", "p3": " \n\t "}])
def test_empty_projection_is_not_applicable(players):
    result = parse(payload(players))
    assert result.outcome == Outcome.NOT_APPLICABLE
    assert result.asset is not None
    assert result.asset.player_count == len(players)
    assert result.asset.circle_count == 0
    assert result.points == ()
    assert result.decoded and result.hash_verified


@pytest.mark.parametrize("number", ["NaN", "nan", "Infinity", "-inf", "1e999", "", "oops"])
@pytest.mark.parametrize("attribute", ["cx", "cy", "r"])
def test_bad_numeric_value_discards_all_rows(number, attribute):
    attributes = {"cx": "1", "cy": "2", "r": "3"}
    attributes[attribute] = number
    circle = "<circle " + " ".join(f'{key}="{value}"' for key, value in attributes.items()) + "/>"
    result = parse(payload({"p20": CIRCLE, "p3": circle}))
    assert_no_rows(result, Outcome.PARSE_FAILED)
    assert result.hash_verified and result.decoded


def test_all_structure_is_checked_before_numeric_projection():
    result = parse(payload({"p20": '<circle cx="NaN" cy="1" r="1"/>', "p3": "<rect/>"}))
    assert_no_rows(result, Outcome.UNEXPECTED_FORMAT)
    assert not result.decoded


def test_hash_mismatch_and_wrapper_identity_fail_closed():
    body = encode(payload())
    meta = metadata(body)
    mismatch = parse_heatmap(body, replace(meta, content_hash="0" * 64))
    assert_no_rows(mismatch, Outcome.UNKNOWN)
    assert not mismatch.hash_verified and not mismatch.decoded
    bad_target = canonicalize_target("heatmap/match/102/heatmaps", {"heatmapUrl": SOURCE})
    bad_reference = replace(meta.reference, target=bad_target)
    assert_no_rows(parse_heatmap(body, replace(meta, reference=bad_reference)), Outcome.UNEXPECTED_FORMAT)


def observation(record):
    return {
        "target_key": record.target_key,
        "content_hash": record.content_hash,
        "raw_uri": record.raw_uri,
        "fetched_at": record.fetched_at,
        "validated_at": record.validated_at,
        "capabilities_json": json.dumps({"canonical_url": record.canonical_url}),
    }


@pytest.fixture
def offline(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    session = requests.Session()
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("HTTP forbidden in offline replay")

    session.get = forbidden
    transport = FotMobTransport(raw_store=store, session=session, max_attempts=1)
    yield store, transport, calls
    session.close()
    assert calls == []


def stored_replay(offline):
    store, transport, _ = offline
    body = encode(payload())
    target = build_heatmap_reference(101, SOURCE).target
    record = store.store(target, body)
    return transport.replay_target_key(target.target_key, observation=observation(record))


@pytest.mark.parametrize("outcome", [FetchOutcome.SUCCESS, FetchOutcome.NOT_MODIFIED])
def test_fresh_and_not_modified_bodies_are_parseable(offline, outcome):
    replay = stored_replay(offline)
    status = 304 if outcome == FetchOutcome.NOT_MODIFIED else 200
    result = classify_heatmap_fetch(
        replace(replay, outcome=outcome, http_status=status, stale=False)
    )
    assert result.parse_body and result.outcome is None


def test_not_modified_is_network_acquisition_for_report(offline):
    replay = stored_replay(offline)
    response = replace(
        replay, outcome=FetchOutcome.NOT_MODIFIED, http_status=304, stale=False
    )
    assert classify_heatmap_fetch(response).parse_body
    meta = metadata(response.body)
    parsed = parse_heatmap(response.body, meta)
    item = observation_from_parse("304-run", meta, parsed, FetchOrigin.NETWORK)
    report = aggregate_heatmap_report([item])
    assert report["network_fetched"] == report["fetched"] == 1
    assert report["replayed"] == 0


@pytest.mark.parametrize("outcome,status,body,stale,expected", [
    (FetchOutcome.NOT_AVAILABLE, 204, b"", False, Outcome.RESOURCE_UNAVAILABLE),
    (FetchOutcome.NOT_AVAILABLE, 404, None, False, Outcome.RESOURCE_UNAVAILABLE),
    (FetchOutcome.NOT_AVAILABLE, 200, b" null \n", False, Outcome.RESOURCE_UNAVAILABLE),
    (FetchOutcome.NOT_AVAILABLE, 200, b"{}", False, Outcome.UNKNOWN),
    (FetchOutcome.NOT_AVAILABLE, 200, b"invalid", False, Outcome.UNKNOWN),
    (FetchOutcome.NOT_AVAILABLE, 200, None, False, Outcome.UNKNOWN),
    (FetchOutcome.NOT_AVAILABLE, 503, b"null", False, Outcome.UNKNOWN),
    (FetchOutcome.RETRYABLE_FAILURE, 503, None, False, Outcome.UNKNOWN),
    (FetchOutcome.STALE_REPLAY, 200, b"{}", True, Outcome.UNKNOWN),
    (FetchOutcome.SUCCESS, 200, b"{}", True, Outcome.UNKNOWN),
    (FetchOutcome.SUCCESS, 200, None, False, Outcome.UNKNOWN),
    (FetchOutcome.TERMINAL_FAILURE, 200, b"invalid", False, Outcome.UNEXPECTED_FORMAT),
    (FetchOutcome.TERMINAL_FAILURE, 200, b"\xff", False, Outcome.UNEXPECTED_FORMAT),
    (FetchOutcome.TERMINAL_FAILURE, 200, None, False, Outcome.UNKNOWN),
    (FetchOutcome.TERMINAL_FAILURE, 200, b"{}", False, Outcome.UNKNOWN),
    (FetchOutcome.TERMINAL_FAILURE, 403, None, False, Outcome.UNKNOWN),
    (FetchOutcome.TERMINAL_FAILURE, None, None, False, Outcome.UNKNOWN),
])
def test_fetch_classification(offline, outcome, status, body, stale, expected):
    replay = stored_replay(offline)
    variant = replace(
        replay, outcome=outcome, http_status=status, body=body, stale=stale,
        error="synthetic failure", json_data=None,
    )
    result = classify_heatmap_fetch(variant)
    assert result.outcome == expected
    assert not result.parse_body


def test_selected_hash_replay_survives_same_url_update(offline):
    store, transport, calls = offline
    target = build_heatmap_reference(101, SOURCE).target
    v1 = encode(payload())
    second_payload = payload({"p20": CIRCLE + CIRCLE})
    second_payload["lastModified"] = "synthetic-source-version-2"
    v2 = encode(second_payload)
    first = store.store(target, v1)
    second = store.store(target, v2)
    assert first.content_hash != second.content_hash
    assert first.blob_key != second.blob_key
    latest_body, latest_record = store.load(target)
    assert latest_body == v2 and latest_record.content_hash == second.content_hash
    # Both immutable versions remain accessible, independently of the pointer.
    assert store.load_observation(observation(first), canonical_url=target.canonical_url)[0] == v1
    assert store.load_observation(observation(second), canonical_url=target.canonical_url)[0] == v2
    reports = []
    projections = []
    for record, expected_body, count in ((first, v1, 1), (second, v2, 2), (first, v1, 1)):
        replay = transport.replay_target_key(target.target_key, observation=observation(record))
        assert replay.outcome == FetchOutcome.SUCCESS
        assert not replay.stale
        assert replay.body == expected_body
        assert replay.content_hash == record.content_hash
        assert replay.fetched_at == record.fetched_at
        assert replay.last_modified is None
        assert classify_heatmap_fetch(replay).parse_body
        meta = metadata(
            replay.body, observed_at=record.fetched_at, raw_uri=record.raw_uri,
            http_last_modified=replay.last_modified,
        )
        result = parse_heatmap(replay.body, meta)
        assert result.asset.observed_at == record.fetched_at
        assert result.asset.http_last_modified is None
        assert result.asset.source_last_modified == json.loads(expected_body)["lastModified"]
        assert result.asset.circle_count == count
        item = observation_from_parse("offline-run", meta, result, FetchOrigin.REPLAY)
        reports.append(aggregate_heatmap_report([item, item]))
        projections.append(result)
    assert projections[0] == projections[2]
    assert reports[0] == reports[2]
    assert reports[0]["replayed"] == reports[0]["fetched"] == 1
    assert reports[0]["network_fetched"] == 0
    assert calls == []


def parsed_observation(match_id, data, origin=FetchOrigin.REPLAY):
    body = encode(data)
    meta = metadata(body, match_id=match_id)
    return observation_from_parse("run-1", meta, parse_heatmap(body, meta), origin)


def attempted_observation(match_id, outcome):
    reference = build_heatmap_reference(
        match_id, f"https://pub.fotmob.com/prod/db/api/heatmap/match/{match_id}"
    )
    return HeatmapObservation(
        "run-1", match_id, outcome, True,
        target_key=reference.target.target_key, attempted=True,
    )


def test_report_denominators_deduplication_and_origin_partition():
    network = parsed_observation(101, payload(), FetchOrigin.NETWORK)
    replay = parsed_observation(102, payload({"p20": CIRCLE + CIRCLE}))
    numeric = parsed_observation(103, payload({"p20": '<circle cx="1e999" cy="1" r="1"/>'}))
    empty = parsed_observation(104, payload({}))
    unavailable = attempted_observation(105, Outcome.RESOURCE_UNAVAILABLE)
    transient = attempted_observation(106, Outcome.UNKNOWN)
    marker = HeatmapObservation("run-1", 107, Outcome.UNKNOWN, True)
    missing = HeatmapObservation("run-1", 108, Outcome.URL_MISSING, False)
    malformed = parsed_observation(109, {"unexpected": "shape"})
    items = [network, replay, numeric, empty, unavailable, transient, marker, missing, malformed]
    report = aggregate_heatmap_report(items + [replay, replay, marker])
    assert report == aggregate_heatmap_report(reversed(items))
    assert report["matches"] == 9
    assert report["matches_with_url"] == 8
    assert report["unique_assets"] == 7
    assert report["fetched"] == 5
    assert report["network_fetched"] == 1
    assert report["replayed"] == 4
    assert report["decoded"] == 4
    assert report["typed"] == 2
    assert report["typed_rows"] == 3
    assert report["unavailable"] == report["resource_unavailable"] == 1
    assert report["unknown"] == 2
    assert report["not_applicable"] == report["parse_failed"] == report["unexpected_format"] == 1
    assert report["url_missing"] == 1
    assert report["success"] == 2
    assert set(report["outcomes"]) == {item.value for item in Outcome}
    assert sum(report["outcomes"].values()) == report["matches"]
    for key, count in report["outcomes"].items():
        assert report[key] == count


def test_known_wrapper_marker_alone_is_not_an_attempt_or_fetch():
    marker = replace(attempted_observation(101, Outcome.UNKNOWN), attempted=False)
    report = aggregate_heatmap_report([marker])
    assert report["matches_with_url"] == 1
    assert report["unique_assets"] == report["fetched"] == 0


def test_empty_report_has_every_outcome_and_zero_counters():
    report = aggregate_heatmap_report([])
    assert all(value == 0 for key, value in report.items() if key != "outcomes")
    assert report["outcomes"] == {item.value: 0 for item in Outcome}


@pytest.mark.parametrize("change", [
    {"outcome": Outcome.NOT_APPLICABLE, "typed_rows": 0},
    {"fetch_origin": FetchOrigin.NETWORK},
    {"content_hash": "0" * 64},
])
def test_contradictory_duplicate_records_are_rejected(change):
    item = parsed_observation(101, payload())
    with pytest.raises(ValueError, match="contradictory"):
        aggregate_heatmap_report([item, replace(item, **change)])


@pytest.mark.parametrize("change", [
    {"attempted": False}, {"has_url": False}, {"target_key": None},
    {"content_hash": None}, {"hash_verified": False}, {"fetch_origin": None},
    {"decoded": False}, {"typed_rows": 0}, {"typed_rows": -1},
    {"typed_rows": True}, {"outcome": Outcome.UNKNOWN}, {"target_key": "bad"},
    {"outcome": "success"}, {"has_url": 1},
])
def test_report_rejects_inconsistent_evidence(change):
    item = parsed_observation(101, payload())
    with pytest.raises(ValueError):
        aggregate_heatmap_report([replace(item, **change)])


def test_report_rejects_mixed_runs_and_cross_match_target_reuse():
    first = parsed_observation(101, payload())
    second = parsed_observation(102, payload())
    with pytest.raises(ValueError, match="one run"):
        aggregate_heatmap_report([first, replace(second, run_id="run-2")])
    with pytest.raises(ValueError, match="two matches"):
        aggregate_heatmap_report([first, replace(second, target_key=first.target_key)])
    with pytest.raises(ValueError):
        aggregate_heatmap_report([HeatmapObservation("run-1", 103, Outcome.RESOURCE_UNAVAILABLE, True)])
    with pytest.raises(ValueError):
        aggregate_heatmap_report([HeatmapObservation("run-1", 104, Outcome.URL_MISSING, True)])


def test_real_transport_null_and_invalid_json_classification(tmp_path):
    cases = (
        (901, b"null", Outcome.RESOURCE_UNAVAILABLE),
        (902, b"not-json", Outcome.UNEXPECTED_FORMAT),
    )
    for match_id, body, expected in cases:
        source = f"https://pub.fotmob.com/prod/db/api/heatmap/match/{match_id}"
        target = build_heatmap_reference(match_id, source).target
        store = FotMobRawStore.from_uri((tmp_path / str(match_id)).as_uri())
        transport, _ = _transport(
            [FakeResponse(200, body)], raw_store=store, max_attempts=1,
        )
        fetch = transport.fetch_json(target.canonical_url)
        assert fetch.body is None
        assert classify_heatmap_fetch(fetch).outcome == expected


def test_real_transport_null_and_invalid_json_evidence(tmp_path):
    cases = (
        (903, b"null", FetchOutcome.NOT_AVAILABLE, Outcome.RESOURCE_UNAVAILABLE),
        (904, b"not-json", FetchOutcome.TERMINAL_FAILURE, Outcome.UNEXPECTED_FORMAT),
    )
    for match_id, body, transport_outcome, expected in cases:
        source = f"https://pub.fotmob.com/prod/db/api/heatmap/match/{match_id}"
        target = build_heatmap_reference(match_id, source).target
        store = FotMobRawStore.from_uri((tmp_path / str(match_id)).as_uri())
        transport, _ = _transport(
            [FakeResponse(200, body)], raw_store=store, max_attempts=1,
        )
        fetch = transport.fetch_json(target.canonical_url)
        assert fetch.http_status == 200
        assert fetch.outcome == transport_outcome
        assert fetch.body is None
        if body == b"null":
            assert fetch.content_hash
            assert fetch.raw_uri
            assert fetch.error == "FotMob returned a null JSON body"
        else:
            assert fetch.content_hash is None
            assert fetch.raw_uri is None
            assert isinstance(fetch.error, str)
            assert fetch.error.startswith("invalid JSON response")
        assert classify_heatmap_fetch(fetch).outcome == expected
